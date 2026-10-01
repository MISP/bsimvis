"""N-way file diff: rows of functions shared across N files.

`stored` mode takes edges from each pair's stored bin_sim doc (`diff.matched`),
so the rows agree with the pair diff view by construction. A pair with no stored
doc falls back to `cached_pair_from_stored_sims` and is counted in
`fallback_pairs`.

`virtual` mode (R1) rebuilds the edges from the files' tf vectors as a fresh
collection holding exactly these files would: BSim discovery over functions at
or above `min_features`, exact FunctionID-hash matches below it, pairwise greedy,
then the bin-level discovery pass. It works for any set, whatever collections the
files live in. Nothing is written to Kvrocks in either mode.

`compute` is pure (no Flask, JSON-serialisable result) so a worker job can call
it unchanged later; only the cache in front of it would move to Redis.
"""

import hashlib
import json
import re
import threading
from collections import Counter, OrderedDict, defaultdict
from itertools import combinations

from bsimvis.app.services import bsim_profiles, container_sim_service, lineage_service
from bsimvis.app.services.bin_sim_service import (
    _origin_coll,
    bin_sim_service,
    discover_edges,
    load_vectors,
    read_bin_sim_rev,
    stored_discovery,
)
from bsimvis.app.services.bin_sim_tags import (
    greedy_match,
    is_library_tag,
    merge_tag_fields,
    read_tags_rev,
)
from bsimvis.app.services.collection_config import (
    assert_signature_settings_match,
    get_signature_settings,
    resolve_collection_algo,
)
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.nway_match import nway_match

_DEFAULT_NAME = re.compile(r"^(FUN_|sub_|thunk_)", re.I)


class TooLarge(Exception):
    def __init__(self, members, functions=None):
        super().__init__(f"{members} members, {functions} functions")
        self.members = members
        self.functions = functions


class BadParams(ValueError):
    pass


class MemberMissing(Exception):
    def __init__(self, missing):
        super().__init__(f"unknown files: {missing}")
        self.missing = missing


def caps():
    return (
        int(config_service.get("nway.max_members", 12)),
        int(config_service.get("nway.max_functions", 60000)),
    )


def check_caps(n_members, n_functions=None, max_members=None, max_functions=None):
    """Raise TooLarge when the set is over either cap (functions may be unknown yet)."""
    cap_m, cap_f = caps()
    max_members = cap_m if max_members is None else max_members
    max_functions = cap_f if max_functions is None else max_functions
    if n_members > max_members or (n_functions or 0) > max_functions:
        raise TooLarge(n_members, n_functions)


class _LRU:
    """Tiny thread-safe cache; the app is one threaded process (see _DIFF_CACHE)."""

    def __init__(self, size):
        self.size, self.data, self.lock = size, OrderedDict(), threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.data:
                self.data.move_to_end(key)
                return self.data[key]

    def put(self, key, value):
        with self.lock:
            self.data[key] = value
            self.data.move_to_end(key)
            while len(self.data) > self.size:
                self.data.popitem(last=False)


# ponytail: counted by entries, not bytes. Edges + metadata of a 12-file set are
# tens of MB at most under the caps; the Redis-backed job cache replaces this.
_BASE_CACHE = _LRU(2)
_DOC_CACHE = _LRU(8)


def default_min_edge():
    """The read-time floor the UI applies to file diffs (`similarity.file_min_score`)."""
    return float(config_service.get("similarity.file_min_score", 0.0))


def _decode(raw):
    if not raw:
        return {}
    value = json.loads(raw) if not isinstance(raw, dict) else raw
    return json.loads(value) if isinstance(value, str) else value


def _rev_scopes(scope, members):
    """Where the stored-data revisions live: the pool, or each member collection."""
    if scope.get("pool"):
        return [f"global:pool:{scope['pool']}"]
    return sorted({c for c, _ in members})


def _algo(scope, members):
    if scope.get("pool"):
        from bsimvis.app.services.pool_service import pool_service

        return pool_service.similarity_algo(pool_service.get_pool(scope["pool"]))
    return resolve_collection_algo(members[0][0])


def _col_id(coll, md5):
    return f"{coll}:{md5}"


def _file_names(r, members):
    pipe = r.pipeline(transaction=False)
    for coll, md5 in members:
        pipe.get(f"{coll}:file:{md5}:meta")
    return {
        _col_id(*m): _decode(raw).get("file_name") or m[1]
        for m, raw in zip(members, pipe.execute())
    }


def expand_members(r, members):
    """Replace each container by its code-bearing leaf children.

    Returns `(members, labels, warnings)`; `labels` maps a leaf's column id to
    `container › child` so the UI can show where it came from.
    """
    out, labels, warnings, containers = [], {}, [], {}
    for coll, md5 in members:
        if coll not in containers:
            containers[coll] = lineage_service.container_md5s(coll, r)
        if md5 not in containers[coll]:
            out.append((coll, md5))
            continue
        leaves, _ = container_sim_service.leaf_info(coll, md5, r, containers[coll])
        names = _file_names(r, [(coll, x) for x in [md5, *leaves]])
        empty = 0
        for leaf, count in sorted(leaves.items()):
            if not count:
                empty += 1
                continue
            out.append((coll, leaf))
            labels[_col_id(coll, leaf)] = (
                f"{names[_col_id(coll, md5)]} › {names[_col_id(coll, leaf)]}"
            )
        if empty:
            warnings.append(f"{md5}: {empty} child file(s) without functions skipped.")
    return list(dict.fromkeys(out)), labels, warnings


def _pool_params(scope):
    from bsimvis.app.services.pool_service import pool_service

    pool = pool_service.get_pool(scope["pool"])
    fsp, fisp = pool.get("func_sim_params") or {}, pool.get("file_sim_params") or {}
    cfg = config_service.get
    base = {
        "algo": pool_service.similarity_algo(pool),
        "min_score": fsp.get("min_score", cfg("similarity.min_score", 0.9)),
        "min_features": fsp.get("min_features", cfg("similarity.min_features", 10)),
    }
    discovery = None
    if fisp.get("discovery", stored_discovery() is not None):
        discovery = (
            float(
                fisp.get(
                    "discovery_min_score", cfg("similarity.discovery_min_score", 0.5)
                )
            ),
            float(
                fisp.get("discovery_max_df", cfg("similarity.discovery_max_df", 1.0))
            ),
        )
    return base, discovery


def virtual_params(scope, members, params):
    """Parameters a fresh collection of these files would run with, plus warnings.

    A caller-supplied value wins; otherwise a pool uses its own function-sim
    params and a collection scope (or a mixed set) uses the first member
    collection's locked values, the way scan does.
    """
    from bsimvis.app.services.scan_service import get_scan_service

    warnings = []
    colls = list(dict.fromkeys(c for c, _ in members))
    if scope.get("pool"):
        base, discovery = _pool_params(scope)
    else:
        scan = get_scan_service()
        base, discovery = scan._params_for(colls[0], {}), stored_discovery()
        for coll in colls[1:]:
            theirs = scan._params_for(coll, {})
            diff = [
                k for k in ("algo", "min_score", "min_features") if theirs[k] != base[k]
            ]
            if diff:
                warnings.append(
                    f"{coll}: locked {', '.join(diff)} differ from {colls[0]}; "
                    f"using {colls[0]}'s."
                )
    for key in ("algo", "min_score", "min_features"):
        if params.get(key) is not None:
            base[key] = params[key]
    out = {
        "algo": base["algo"],
        "min_score": float(base["min_score"]),
        "min_features": int(base["min_features"]),
        "discovery": discovery,
    }
    # ponytail: top_k is ignored (it never binds at the caps); minhash_lsh and
    # milvus_sparse have no in-memory scorer.
    if bsim_profiles.parse_algo(out["algo"])[0] not in (
        "unweighted_cosine",
        "binary_cosine",
        "jaccard",
        "weighted_cosine",
    ):
        raise BadParams(f"mode=virtual cannot score with algo {out['algo']!r}")

    masks = set()
    for coll in colls:
        mask = get_signature_settings(coll)
        if mask is not None:
            masks.add(int(mask))
        try:
            assert_signature_settings_match(coll)
        except ValueError as e:
            warnings.append(f"{coll}: {e}")
    if len(masks) > 1:
        warnings.append(
            "Member collections were extracted with different signature settings; "
            "their feature hashes do not intersect."
        )
    return out, warnings


def _virtual_edges(r, columns, fid_sets, p):
    """Pairwise-greedy edges per column pair, as a fresh collection would build them."""
    all_fids = set().union(*fid_sets.values())
    vectors = load_vectors(r, all_fids)
    small = {f for f in all_fids if len(vectors.get(f, ())) < p["min_features"]}

    # Sub-floor functions match on their exact FunctionID hash, never on BSim.
    ordered = sorted(small)
    pipe = r.pipeline(transaction=False)
    for fid in ordered:
        pipe.get(f"{fid}:funcid")
    owner = {f: cid for cid, fs in fid_sets.items() for f in fs}
    by_hash = defaultdict(lambda: defaultdict(list))
    for fid, h in zip(ordered, pipe.execute()):
        if h:
            by_hash[h.decode() if isinstance(h, bytes) else h][owner[fid]].append(fid)

    edges = {}
    for a, b in combinations(columns, 2):
        fa, fb = fid_sets[a["id"]], fid_sets[b["id"]]
        found = discover_edges(
            vectors, fa - small, fb - small, p["algo"], p["min_score"]
        )
        for cols in by_hash.values():
            found += [
                (x, y, 1.0)
                for x in cols.get(a["id"], ())
                for y in cols.get(b["id"], ())
            ]
        if p["discovery"]:
            min_score, max_df = p["discovery"]
            _, done_a, done_b = greedy_match(found)
            found += discover_edges(
                vectors, fa - done_a, fb - done_b, p["algo"], min_score, max_df=max_df
            )
        edges[(a["id"], b["id"])] = [
            (x, y, float(s)) for x, y, s in greedy_match(found)[0]
        ]
    return edges


def _stored_edges(r, scope, members, columns, algo):
    pool_id = scope.get("pool")
    # ponytail: fallback pairs use a fixed discovery floor, so lowering min_edge
    # below it cannot recover their weaker edges; pairs with a stored doc are exact.
    fallback_floor = float(config_service.get("similarity.discovery_min_score", 0.5))
    edges, fallback = {}, 0
    for (i, a), (j, b) in combinations(enumerate(members), 2):
        (coll_a, md5_a), (coll_b, md5_b) = a, b
        _, doc = bin_sim_service.load_pair(
            coll_a, md5_a, md5_b, coll_b if pool_id else None, pool_id, algo
        )
        if not doc:
            fallback += 1
            doc = bin_sim_service.cached_pair_from_stored_sims(
                coll_a,
                md5_a,
                md5_b,
                algo,
                coll_b if pool_id else None,
                pool_id,
                min_score=fallback_floor,
            )
        matched = ((doc or {}).get("diff") or {}).get("matched") or []
        edges[(columns[i]["id"], columns[j]["id"])] = [
            (m["func_a"], m["func_b"], float(m["similarity"])) for m in matched
        ]
    return edges, fallback


def _load_base(r, scope, members, vparams, labels, warnings):
    """Everything min_edge-independent: columns, function weights, metadata, edges.

    `vparams` is None for stored mode, else the virtual-mode parameters.
    """
    algo = vparams["algo"] if vparams else _algo(scope, members)

    pipe = r.pipeline(transaction=False)
    for coll, md5 in members:
        pipe.get(f"{coll}:file:{md5}:meta")
        pipe.smembers(f"{coll}:idx:file:functions:{md5}")
    res = pipe.execute()
    file_meta = [_decode(x) for x in res[0::2]]
    missing = [_col_id(*m) for m, meta in zip(members, file_meta) if not meta]
    if missing:
        raise MemberMissing(missing)
    fid_sets = [
        {
            (f.decode() if isinstance(f, bytes) else str(f)).removesuffix(":meta")
            for f in s
        }
        for s in res[1::2]
    ]
    check_caps(len(members), sum(len(s) for s in fid_sets))

    all_fids = sorted(set().union(*fid_sets))
    pipe = r.pipeline(transaction=False)
    for fid in all_fids:
        pipe.get(f"{fid}:meta")
    fmeta, library = {}, {}
    weights = {}
    for fid, raw in zip(all_fids, pipe.execute()):
        meta = _decode(raw)
        weights[fid] = float(meta.get("bsim_features_count") or 0) or 1.0
        library[fid] = any(is_library_tag(t) for t in merge_tag_fields(meta))
        fmeta[fid] = {
            k: meta[k]
            for k in ("name", "namespace", "entrypoint_address", "tags", "user_tags")
            if k in meta
        }

    columns = [
        {
            "id": _col_id(coll, md5),
            "collection": coll,
            "md5": md5,
            "file_name": labels.get(_col_id(coll, md5)) or meta.get("file_name") or md5,
            "functions": len(fids),
        }
        for (coll, md5), meta, fids in zip(members, file_meta, fid_sets)
    ]
    fid_sets = {col["id"]: fids for col, fids in zip(columns, fid_sets)}

    if vparams:
        edges, fallback = _virtual_edges(r, columns, fid_sets, vparams), 0
    else:
        edges, fallback = _stored_edges(r, scope, members, columns, algo)

    return {
        "algo": algo,
        "columns": columns,
        "funcs": {
            cid: {fid: weights[fid] for fid in fids} for cid, fids in fid_sets.items()
        },
        "fmeta": fmeta,
        "library": library,
        "edges": edges,
        "fallback_pairs": fallback,
        "warnings": warnings,
    }


def _label(cells, fmeta, weights):
    """Most frequent non-default name, else the heaviest function's address."""
    names = Counter(
        n
        for fid in cells
        if (n := (fmeta.get(fid) or {}).get("name")) and not _DEFAULT_NAME.match(n)
    )
    if names:
        return names.most_common(1)[0][0], len(names)
    heaviest = max(cells, key=lambda f: (weights[f], f))
    addr = (fmeta.get(heaviest) or {}).get("entrypoint_address") or heaviest.rsplit(
        ":", 1
    )[-1]
    return str(addr), 0


def _match(base, min_edge):
    weights = {f: w for funcs in base["funcs"].values() for f, w in funcs.items()}
    rows = nway_match(base["edges"], base["funcs"], min_edge)
    for row in rows:
        fids = list(row["cells"].values())
        row["name"], row["names"] = _label(fids, base["fmeta"], weights)
        row["library"] = any(base["library"].get(f) for f in fids)
    return rows


def project_rows(rows, groups, presence):
    """Re-express file-level rows over child-cluster / direct-file groups.

    `groups` is `[{"id", "members": [file column id, ...]}]`. A group is `on`
    for a row when at least `presence` of its files hold a function in it; the
    row's mask and span count the `on` groups. Rows are not merged: one function
    group stays one row. `file_span` keeps the file-level span (the Unique tab
    still means "one file"), `files` keeps the file-level cells.
    """
    out = []
    for row in rows:
        cells, mask, span = {}, 0, 0
        for i, g in enumerate(groups):
            present = sum(1 for m in g["members"] if m in row["cells"])
            on = present > 0 and present / len(g["members"]) >= presence
            cells[g["id"]] = {"present": present, "total": len(g["members"]), "on": on}
            if on:
                mask |= 1 << i
                span += 1
        out.append(
            {
                **row,
                "mask": mask,
                "span": span,
                "cells": cells,
                "file_span": row["span"],
                "files": row["cells"],
            }
        )
    return out


def demo():
    groups = [
        {"id": "A", "members": ["f1", "f2", "f3", "f4"]},
        {"id": "B", "members": ["f5", "f6"]},
    ]

    def row(cells):
        return {"cells": cells, "span": len(cells), "mask": 0}

    full = row({f"f{i}": f"x{i}" for i in range(1, 7)})
    half = row({"f1": "a", "f2": "b", "f5": "c"})
    lone = row({"f3": "z"})
    got = project_rows([full, half, lone], groups, 0.5)
    assert [(r["span"], r["mask"]) for r in got] == [(2, 3), (2, 3), (0, 0)], got
    assert got[1]["cells"]["A"] == {"present": 2, "total": 4, "on": True}
    assert got[1]["cells"]["B"] == {"present": 1, "total": 2, "on": True}
    assert (got[2]["file_span"], got[2]["cells"]["A"]["on"]) == (1, False)
    assert got[2]["files"] == {"f3": "z"} and len(got) == 3
    strict = project_rows([half], groups, 0.75)[0]
    assert (strict["span"], strict["mask"]) == (0, 0), strict
    assert project_rows([half], groups, 0.25)[0]["span"] == 2
    print("nway_diff_service demo ok")


def compute(r, scope, members, params=None):
    """Rows for `members` (list of `(collection, md5)`) under `scope`.

    `scope` is `{"collection": c}`, `{"collections": [...]}` or `{"pool": id}`.
    `params`: `min_edge` (default `similarity.file_min_score`); `mode`
    (`stored` | `virtual`; a set spanning collections with no pool is always
    virtual); `algo` / `min_score` / `min_features` override virtual-mode values.
    `groups` (+ `child_presence`, default 0.5) re-expresses the rows over
    child-cluster columns; see `project_rows`.
    """
    params = params or {}
    mode = params.get("mode") or "stored"
    if mode not in ("stored", "virtual"):
        raise BadParams("mode must be stored or virtual")
    members = list(dict.fromkeys((_origin_coll(c), m) for c, m in members))
    members, labels, warnings = expand_members(r, members)
    check_caps(len(members))
    if not scope.get("pool") and len({c for c, _ in members}) > 1:
        mode = "virtual"
    min_edge = params.get("min_edge")
    min_edge = default_min_edge() if min_edge is None else float(min_edge)

    vparams = None
    if mode == "virtual":
        vparams, vwarnings = virtual_params(scope, members, params)
        warnings += vwarnings
    revs = [
        (read_bin_sim_rev(r, s), read_tags_rev(r, s))
        for s in _rev_scopes(scope, members)
    ]
    base_key = (
        json.dumps(scope, sort_keys=True),
        hashlib.md5(json.dumps(members).encode()).hexdigest(),
        json.dumps(revs),
        mode,
        json.dumps(vparams, sort_keys=True),
    )
    groups = params.get("groups")
    presence = float(params.get("child_presence", 0.5))
    doc_key = base_key + (
        min_edge,
        presence,
        hashlib.md5(json.dumps(groups, sort_keys=True).encode()).hexdigest(),
    )
    doc = _DOC_CACHE.get(doc_key)
    if doc is not None:
        return doc

    base = _BASE_CACHE.get(base_key)
    if base is None:
        base = _load_base(r, scope, members, vparams, labels, warnings)
        _BASE_CACHE.put(base_key, base)
    rows = _match(base, min_edge)
    if groups:
        rows = project_rows(rows, groups, presence)
    doc = {
        "mode": mode,
        "algo": base["algo"],
        "min_edge": min_edge,
        "columns": groups or base["columns"],
        "columns_mode": "children" if groups else "files",
        "file_columns": base["columns"] if groups else None,
        "rows": rows,
        "fmeta": base["fmeta"],
        "fallback_pairs": base["fallback_pairs"],
        "warnings": base["warnings"],
    }
    _DOC_CACHE.put(doc_key, doc)
    return doc


if __name__ == "__main__":
    demo()
