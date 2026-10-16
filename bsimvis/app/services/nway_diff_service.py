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
import statistics
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

_MD5 = re.compile(r"^[0-9a-f]{32}$")
_DEFAULT_NAME = re.compile(r"^(FUN_|sub_|thunk_)", re.I)
MAX_COLUMNS = 400  # a column tree stops expanding past this many columns


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
        int(config_service.get("nway.max_members", 500)),
        int(config_service.get("nway.max_functions", 1000000)),
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


def invalidate():
    """Drop cached docs: function names are not part of their revision key."""
    for c in (_BASE_CACHE, _DOC_CACHE):
        with c.lock:
            c.data.clear()


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


def _fn_meta(meta):
    """The function fields the N-way views show."""
    out = {
        k: meta[k]
        for k in (
            "name",
            "user_name",
            "user_name_source",
            "namespace",
            "entrypoint_address",
            "tags",
            "user_tags",
            "parameters",
            "return_type",
            "bsim_features_count",
        )
        if k in meta
    }
    if "function_name" in meta:
        out["name"] = meta["function_name"]
    return out


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
        fmeta[fid] = _fn_meta(meta)

    columns = [
        {
            "id": _col_id(coll, md5),
            "collection": coll,
            "md5": md5,
            "file_name": labels.get(_col_id(coll, md5)) or meta.get("file_name") or md5,
            "functions": len(fids),
            "language_id": meta.get("language_id"),
            "file_size": meta.get("file_size"),
            "format": (meta.get("file_format") or {}).get("Executable Format"),
            "batch_name": meta.get("batch_name"),
            "tags": list(meta.get("tags") or [])[:40],
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
    """Most frequent non-default name, else the heaviest function's own name (FUN_addr)."""
    names = Counter(
        n
        for fid in cells
        if (n := (fmeta.get(fid) or {}).get("name")) and not _DEFAULT_NAME.match(n)
    )
    if names:
        return names.most_common(1)[0][0], len(names)
    heaviest = max(cells, key=lambda f: (weights[f], f))
    meta = fmeta.get(heaviest) or {}
    addr = meta.get("entrypoint_address") or heaviest.rsplit(":", 1)[-1]
    return meta.get("name") or f"FUN_{addr}", 0


def _tag_name(tag):
    """A function name carried by a tag: `fid:libc:1.2.6#__toread` -> `__toread`,
    `yara:botnet:generic:Rule` -> `Rule`. Bare `fid:libc` names no function."""
    if "#" in tag:
        return tag.rsplit("#", 1)[1] or None
    parts = tag.split(":")
    return parts[-1] if len(parts) >= 3 and parts[0] != "user" else None


def _row_tags(fids, fmeta, limit=8):
    """Tags over the row's functions, most shared first, library tags ahead on a tie."""
    count = Counter(t for f in fids for t in merge_tag_fields(fmeta.get(f)))
    ranked = sorted(count, key=lambda t: (-count[t], not is_library_tag(t), t))
    return ranked[:limit]


def _name_row(fids, fmeta, weights):
    """Name + tags of a set of functions: the rule N-way rows and the function hero share.

    Analyst name > most frequent non-default name > tag-carried name > heaviest FUN_addr.
    """
    out = {"tags": _row_tags(fids, fmeta)}
    out["name"], out["names"] = _label(fids, fmeta, weights)
    if not out["names"] and _DEFAULT_NAME.match(out["name"]):
        tagged = next((n for t in out["tags"] if (n := _tag_name(t))), None)
        if tagged:
            out["name"], out["name_from"] = tagged, "tag"
    # An analyst name wins; the heaviest named function decides, others show as aliases.
    named = sorted(
        (f for f in fids if (fmeta.get(f) or {}).get("user_name")),
        key=lambda f: (-weights[f], f),
    )
    if named:
        meta = fmeta[named[0]]
        out["name"] = meta["user_name"]
        out["user_named"] = meta.get("user_name_source", "user")
        out.pop("name_from", None)
        out["aliases"] = sorted({fmeta[f]["user_name"] for f in named} - {out["name"]})
    return out


def _match(base, min_edge):
    weights = {f: w for funcs in base["funcs"].values() for f, w in funcs.items()}
    rows = nway_match(base["edges"], base["funcs"], min_edge)
    for row in rows:
        fids = list(row["cells"].values())
        row.update(_name_row(fids, base["fmeta"], weights))
        row["library"] = any(base["library"].get(f) for f in fids)
    return rows


def project_rows(rows, groups, presence):
    """Re-express file-level rows over child-cluster / direct-file groups.

    `groups` is `[{"id", "members": [file column id, ...]}]`. A group is `on`
    for a row when at least `presence` of its files hold a function in it; the
    row's mask and span count the `on` groups. Rows are not merged: one function
    group stays one row. `file_span` keeps the file-level span (the Unique tab
    still means "one file"), `files` keeps the file-level cells. A group's own
    `presence` overrides the shared one.
    """
    out = []
    for row in rows:
        cells, mask, span = {}, 0, 0
        for i, g in enumerate(groups):
            present = sum(1 for m in g["members"] if m in row["cells"])
            need = g.get("presence", presence)
            on = present > 0 and present / len(g["members"]) >= need
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


def file_groups(group, names):
    """One column per file of a group: what an expanded twin or cluster leaf shows."""
    return [
        {
            "id": f"{group['id']}/{m}",
            "kind": "file",
            "label": names.get(m, m),
            "members": [m],
            "member_count": 1,
            "presence": 1e-9,
        }
        for m in group["members"]
    ]


def grow_columns(groups, open_ids, children_of, limit=MAX_COLUMNS):
    """Replace every open group by its sub-groups, recursively -> the columns to show.

    `children_of(group)` gives a group's sub-groups (`[]` for a leaf). Each column
    comes back with `path`, the groups it was expanded out of (outermost first,
    each `{id, label, member_count}`, so a header can draw a collapse band over
    its children), and `expandable`: more than one file. A group is left closed
    when expanding would pass `limit` columns.
    """
    count = [len(groups)]

    def grow(g, path):
        g = {**g, "path": path, "expandable": g["member_count"] > 1}
        if g["id"] not in open_ids or not g["expandable"]:
            return [g]
        sub = children_of(g)
        if not sub or count[0] + len(sub) - 1 > limit:
            return [g]
        count[0] += len(sub) - 1
        here = path + [
            {"id": g["id"], "label": g["label"], "member_count": g["member_count"]}
        ]
        return [x for child in sub for x in grow(child, here)]

    return [x for g in groups for x in grow(g, [])]


def twin_groups(rows, columns):
    """Files that hold a function in exactly the same shared rows (span >= 2): one column each.

    -> groups for `project_rows` (a group of one file for a file with no twin),
    or None when no two files are twins. A group counts as holding a function
    when any of its files does, so a function only one twin has shows as partial.
    """
    held = {c["id"]: 0 for c in columns}
    k = 0
    for row in rows:
        if row["span"] < 2:
            continue
        for cid in row["cells"]:
            held[cid] |= 1 << k
        k += 1
    by = {}
    for c in columns:
        by.setdefault(held[c["id"]], []).append(c)
    if len(by) == len(columns):
        return None
    groups = []
    for files in by.values():
        ids = [f["id"] for f in files]
        groups.append(
            {
                "id": "twin:" + hashlib.md5("|".join(ids).encode()).hexdigest()[:8],
                "kind": "twin",
                "label": files[0].get("file_name") or files[0]["md5"],
                "members": ids,
                "member_count": len(files),
                "presence": 1e-9,
            }
        )
    return groups


def coverage(rows, column_ids):
    """{column id: share of the shared weight that file holds}.

    Shared weight = sum of `weight` over rows spanning at least two files
    (Core + Partial, whatever `k` is); a file's share counts the same rows it
    has a function in. None for every file when nothing is shared.
    """
    shared = [r for r in rows if r["span"] >= 2]
    total = sum(r["weight"] for r in shared)
    if total <= 0:
        return {c: None for c in column_ids}
    return {
        c: sum(r["weight"] for r in shared if c in r["cells"]) / total
        for c in column_ids
    }


def demo():
    cov_rows = [
        {"span": 3, "weight": 6.0, "cells": {"a": 1, "b": 2, "c": 3}},
        {"span": 2, "weight": 2.0, "cells": {"a": 4, "b": 5}},
        {"span": 1, "weight": 50.0, "cells": {"c": 6}},
    ]
    cov = coverage(cov_rows, ["a", "b", "c"])
    assert cov == {"a": 1.0, "b": 1.0, "c": 0.75}, cov
    assert coverage(cov_rows[2:], ["c"]) == {"c": None}

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
    own = [dict(groups[0], presence=1.0), groups[1]]
    assert project_rows([half], own, 0.25)[0]["cells"]["A"]["on"] is False
    # twin files: same shared rows -> one column; a file with other rows stays alone
    cols = [{"id": i, "md5": i, "file_name": i.lower()} for i in "ABC"]
    shared = [
        {"span": 3, "cells": {"A": 1, "B": 2, "C": 3}},
        {"span": 2, "cells": {"A": 4, "B": 5}},
        {"span": 1, "cells": {"C": 6}},
    ]
    tw = twin_groups(shared, cols)
    assert [g["members"] for g in tw] == [["A", "B"], ["C"]], tw
    assert tw[0]["label"] == "a" and tw[1]["label"] == "c"
    assert tw[0]["id"].startswith("twin:") and tw[0]["id"] != tw[1]["id"]

    # column tree: open groups give way to their children, recursively, within the limit
    g0 = [
        {
            "id": "a",
            "label": "A",
            "kind": "child",
            "members": ["f1", "f2", "f3"],
            "member_count": 3,
        },
        {
            "id": "b",
            "label": "B",
            "kind": "child",
            "members": ["f4"],
            "member_count": 1,
        },
    ]
    a1 = {
        "id": "a1",
        "label": "A1",
        "kind": "child",
        "members": ["f1", "f2"],
        "member_count": 2,
    }
    a_direct = {
        "id": "a:direct",
        "label": "direct",
        "kind": "direct",
        "members": ["f3"],
        "member_count": 1,
    }
    kids = {"a": [a1, a_direct], "a1": file_groups(a1, {"f1": "one", "f2": "two"})}
    of = lambda g: kids.get(g["id"], [])
    ids = lambda gs: [g["id"] for g in gs]
    assert ids(grow_columns(g0, set(), of)) == ["a", "b"]
    assert [g["expandable"] for g in grow_columns(g0, set(), of)] == [True, False]
    one = grow_columns(g0, {"a"}, of)
    assert ids(one) == ["a1", "a:direct", "b"] and one[0]["path"][0]["id"] == "a"
    assert one[0]["expandable"] and not one[1]["expandable"] and one[2]["path"] == []
    deep = grow_columns(g0, {"a", "a1"}, of)
    assert ids(deep) == ["a1/f1", "a1/f2", "a:direct", "b"], ids(deep)
    assert [p["id"] for p in deep[0]["path"]] == ["a", "a1"] and deep[0][
        "label"
    ] == "one"
    assert ids(grow_columns(g0, {"a"}, of, limit=2)) == [
        "a",
        "b",
    ]  # would pass the limit
    assert ids(grow_columns(g0, {"b", "zzz"}, of)) == [
        "a",
        "b",
    ]  # one file: not expandable
    assert twin_groups(shared[:1], cols) is not None
    assert (
        twin_groups(
            [
                {"span": 2, "cells": {"A": 1, "B": 2}},
                {"span": 2, "cells": {"B": 3, "C": 4}},
            ],
            cols,
        )
        is None
    )
    print("nway_diff_service demo ok")


def compute(r, scope, members, params=None):
    """Rows for `members` (list of `(collection, md5)`) under `scope`.

    `scope` is `{"collection": c}`, `{"collections": [...]}` or `{"pool": id}`.
    `params`: `min_edge` (default `similarity.file_min_score`); `mode`
    (`stored` | `virtual`; a set spanning collections with no pool is always
    virtual); `algo` / `min_score` / `min_features` override virtual-mode values.
    `groups` (+ `child_presence`, default 0.5) re-expresses the rows over
    child-cluster columns; see `project_rows`. `focus` (column ids) +
    `focus_rule` (`any` | `all`) instead split the files into Focus vs Rest
    groups: Rest uses `child_presence`, Focus needs any / all of its files.
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
    focus = list(dict.fromkeys(params.get("focus") or []))
    # twin columns come from the matched rows, so they replace any child-cluster groups
    twins = bool(params.get("twins")) and not focus
    if twins:
        groups = None
    open_ids = set(params.get("open") or [])
    if focus:
        ids = [_col_id(c, m) for c, m in members]
        if [f for f in focus if f not in ids]:
            raise BadParams("focus must name files of the set")
        rest = [i for i in ids if i not in focus]
        if not rest:
            raise BadParams("focus must leave at least one file out")
        need = 1.0 if params.get("focus_rule") == "all" else 1e-9
        groups = [
            {
                "id": "focus",
                "kind": "focus",
                "label": "Focus",
                "members": focus,
                "presence": need,
            },
            {"id": "rest", "kind": "rest", "label": "Rest", "members": rest},
        ]
        for g in groups:
            g["member_count"] = len(g["members"])
    doc_key = base_key + (
        min_edge,
        presence,
        twins,
        tuple(sorted(open_ids)) if twins else (),
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
    cov = coverage(rows, [c["id"] for c in base["columns"]])
    file_columns = [{**c, "coverage": cov[c["id"]]} for c in base["columns"]]
    names = {c["id"]: c.get("file_name") or c["md5"] for c in base["columns"]}
    if twins:
        groups = twin_groups(rows, base["columns"])
        twins = groups is not None
        if twins:
            groups = grow_columns(groups, open_ids, lambda g: file_groups(g, names))
    for g in groups or []:
        if g.get("kind") == "file":
            g["label"] = names.get(g["members"][0], g["label"])
    if groups:
        rows = project_rows(rows, groups, presence)
    doc = {
        "mode": mode,
        "algo": base["algo"],
        "min_edge": min_edge,
        "columns": groups or file_columns,
        "columns_mode": (
            "focus"
            if focus
            else "twins" if twins else "children" if groups else "files"
        ),
        "file_columns": file_columns if groups else None,
        "rows": rows,
        "fmeta": base["fmeta"],
        "fallback_pairs": base["fallback_pairs"],
        "warnings": base["warnings"],
        # Read by nway_facets_service (cache key, pair scores); never sent to clients.
        "key": hashlib.md5(repr(doc_key).encode()).hexdigest(),
        "edges": base["edges"],
    }
    _DOC_CACHE.put(doc_key, doc)
    return doc


def compute_functions(r, fids, params=None):
    """Doc for a free set of functions (any collections), nothing written.

    Same shape `nway_facets_service.compute` reads from a file-level doc: one
    synthetic row holding every function, `fscore` pairwise scores instead of
    file-pair edges. Scores come from the tf vectors with the same
    `discover_edges` + `virtual_params` the virtual mode uses.
    """
    params = params or {}
    fids = list(dict.fromkeys(fids))
    check_caps(len(fids))
    parsed = []
    for f in fids:
        coll, _, rest = f.partition(":func:")
        md5 = rest.partition(":")[0]
        if not coll or not _MD5.match(md5):
            raise BadParams(f"bad function id {f!r}")
        parsed.append((_origin_coll(coll), md5))
    colls = sorted({c for c, _ in parsed})
    members = list(dict.fromkeys(parsed))
    vparams, warnings = virtual_params({"collections": colls}, members, params)
    revs = [(read_bin_sim_rev(r, c), read_tags_rev(r, c)) for c in colls]
    key = hashlib.md5(
        repr((sorted(fids), vparams, revs, warnings)).encode()
    ).hexdigest()
    doc = _DOC_CACHE.get(("fns", key))
    if doc is not None:
        return doc

    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{f}:meta")
    metas = [_decode(x) for x in pipe.execute()]
    missing = [f for f, m in zip(fids, metas) if not m]
    if missing:
        raise MemberMissing(missing)
    fmeta = {f: _fn_meta(m) for f, m in zip(fids, metas)}

    vectors = load_vectors(r, fids)
    fscore = {
        frozenset((a, b)): round(float(s), 4)
        for a, b, s in discover_edges(
            vectors, set(fids), set(fids), vparams["algo"], 0.0
        )
        if a < b
    }
    names = _file_names(r, members)
    columns = [
        {
            "id": _col_id(c, m),
            "collection": c,
            "md5": m,
            "file_name": names[_col_id(c, m)],
        }
        for c, m in members
    ]
    weights = {f: float(fmeta[f].get("bsim_features_count") or 0) or 1.0 for f in fids}
    name, n_names = _label(fids, fmeta, weights)
    doc = {
        "mode": "functions",
        "algo": vparams["algo"],
        "columns": columns,
        "file_columns": None,
        "rows": [{"cells": {f: f for f in fids}, "name": name, "names": n_names}],
        "fmeta": fmeta,
        "warnings": warnings,
        "key": key,
        "edges": {},
        "fscore": fscore,
    }
    _DOC_CACHE.put(("fns", key), doc)
    return doc


def _signature(fids, fmeta):
    """Consensus return type + parameter list (the vote of nway_panel `_rowSig`), with variants."""
    items = []
    for f in fids:
        m = fmeta.get(f) or {}
        params = tuple(
            (
                (p.get("type") or p.get("name") or "...")
                if isinstance(p, dict)
                else p or "..."
            )
            for p in m.get("parameters") or []
        )
        items.append((m.get("return_type") or "void", params))
    # Ghidra's undefined/undefined4 return types are placeholders: vote among real ones when any exist.
    real = [i for i in items if not i[0].startswith("undefined")]
    ret = Counter(i[0] for i in real or items).most_common(1)[0][0]
    params = Counter(i[1] for i in items if i[0] == ret).most_common(1)[0][0]
    variants = Counter(items).most_common()
    return {
        "return_type": ret,
        "parameters": list(params),
        "return_agree": sum(1 for i in items if i[0] == ret),
        "variants": [
            {"return_type": v[0], "parameters": list(v[1]), "count": n}
            for v, n in variants
        ],
    }


def _sample(fids, weights, files, cap):
    """At most `cap` functions: the heaviest of every file first, then the heaviest rest."""
    by_file = {}
    for f in sorted(fids, key=lambda f: (-weights[f], f)):
        by_file.setdefault(files[f], []).append(f)
    firsts = sorted((v[0] for v in by_file.values()), key=lambda f: (-weights[f], f))
    rest = sorted(
        (f for v in by_file.values() for f in v[1:]), key=lambda f: (-weights[f], f)
    )
    return sorted((firsts + rest)[:cap])


def _cohesion(r, fids, params):
    """Pairwise score spread over `fids`; pairs the engine did not score count 0."""
    if len(fids) < 2:
        return None
    try:
        fscore = compute_functions(r, fids, params)["fscore"]
    except (BadParams, MemberMissing) as e:
        return {"error": str(e)}
    total = len(fids) * (len(fids) - 1) // 2
    scores = list(fscore.values()) + [0.0] * (total - len(fscore))
    return {
        "min": min(scores),
        "median": statistics.median(scores),
        "max": max(scores),
        "scored_pairs": len(fscore),
        "total_pairs": total,
        "functions": len(fids),
    }


def function_meta(r, fids, ref=None, sample=None):
    """What the function-set hero shows, for any set of functions (nothing written).

    `ref` is the resolved reference (`kind`: nway | cluster | pool | None, plus
    `ns`, `pool`, `cluster_id`, `row`). Naming reuses `_name_row`. Counts cover
    every member; names, signature and the sample the comparison tabs take
    (`sample` functions, default the nway cap) follow the whole set too.
    """
    ref = ref or {}
    fids = list(dict.fromkeys(fids))
    files = {}
    for f in fids:
        coll, _, rest = f.partition(":func:")
        md5 = rest.partition(":")[0]
        if not coll or not _MD5.match(md5):
            raise BadParams(f"bad function id {f!r}")
        files[f] = (_origin_coll(coll), md5)
    if not fids:
        raise BadParams("no functions")
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{f}:meta")
        pipe.hkeys(
            f"global:pool:{ref['pool']}:{f}:cluster_scores"
            if ref.get("pool")
            else f"{f}:cluster_scores"
        )
    res = pipe.execute()
    metas = [_decode(x) for x in res[0::2]]
    missing = [f for f, m in zip(fids, metas) if not m]
    if missing:
        raise MemberMissing(missing)
    fmeta = {f: _fn_meta(m) for f, m in zip(fids, metas)}
    weights = {f: float(fmeta[f].get("bsim_features_count") or 0) or 1.0 for f in fids}
    cids = {
        f: {c.decode() if isinstance(c, bytes) else str(c) for c in cs}
        for f, cs in zip(fids, res[1::2])
    }

    named = _name_row(fids, fmeta, weights)
    source = (
        "analyst"
        if named.get("user_named")
        else (
            "tag"
            if named.get("name_from") == "tag"
            else "majority" if named["names"] else "fallback"
        )
    )

    members = sorted(set(files.values()))
    pipe = r.pipeline(transaction=False)
    for coll, md5 in members:
        pipe.get(f"{coll}:file:{md5}:meta")
    fmetas = [_decode(x) for x in pipe.execute()]
    arch = {(m.get("language_id") or "").split(":")[0] for m in fmetas} - {""}
    batches = {m.get("batch_uuid") or m.get("batch_name") for m in fmetas} - {None, ""}

    ns_of = lambda f: f"global:pool:{ref['pool']}" if ref.get("pool") else files[f][0]
    all_clusters = {(ns_of(f), c) for f in fids for c in cids[f]}
    shared = set.intersection(*(cids[f] for f in fids))
    one_ns = len({ns_of(f) for f in fids}) == 1
    cluster = None
    stored = ref.get("cluster")  # a stored cluster's id or uuid: always linked
    if one_ns and shared and (stored or len(shared) == 1):
        ns = ns_of(fids[0])
        algo = resolve_collection_algo(ns)
        found = {
            c: _decode(r.get(f"{ns}:cluster:{algo}:{c}:meta")) or {}
            for c in sorted(shared)
        }
        cid = next(
            (
                c
                for c, m in found.items()
                if stored in (c, (m.get("cluster_uuid") or "").lower())
            ),
            next(iter(found)),
        )
        cluster = {
            "namespace": ns,
            "collection": files[fids[0]][0],
            "pool": ref.get("pool"),
            "cluster_id": cid,
            "cluster_uuid": found[cid].get("cluster_uuid"),
            "name": found[cid].get("cluster_name"),
        }

    cap = min(max(int(sample or 0), 2), caps()[0]) if sample else caps()[0]
    sampled = len(fids) > cap or len(members) > caps()[0]
    shown = _sample(fids, weights, files, cap) if sampled else sorted(fids)
    shown_files = sorted({files[f] for f in shown})
    return {
        "name": named["name"],
        "name_source": source,
        "named_by": named.get("user_named"),
        "aliases": named.get("aliases", []),
        "prototype": _signature(fids, fmeta),
        "counts": {
            "functions": len(fids),
            "files": len(members),
            "collections": len({c for c, _ in members}),
            "batches": len(batches),
            "clusters": len(all_clusters),
            "arch": sorted(arch),
        },
        "cohesion": _cohesion(r, shown, {}),
        "sample": {"shown": len(shown), "total": len(fids), "cap": cap},
        "fids": shown,
        "links": {
            "kind": ref.get("kind") or "set",
            "nway": {
                "md5s": [f"{c}:{m}" for c, m in shown_files],
                "files": len(members),
                "pool": ref.get("pool"),
            },
            "cluster": cluster,
        },
    }


if __name__ == "__main__":
    demo()
