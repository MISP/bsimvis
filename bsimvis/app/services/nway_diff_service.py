"""N-way file diff: rows of functions shared across N files (stored path).

Edges come from each pair's stored bin_sim doc (`diff.matched`), so the rows
agree with the pair diff view by construction. A pair with no stored doc falls
back to `cached_pair_from_stored_sims` and is counted in `fallback_pairs`.
Nothing is written to Kvrocks.

`compute` is pure (no Flask, JSON-serialisable result) so a worker job can call
it unchanged later; only the cache in front of it would move to Redis.
"""

import hashlib
import json
import re
import threading
from collections import Counter, OrderedDict
from itertools import combinations

from bsimvis.app.services.bin_sim_service import (
    _origin_coll,
    bin_sim_service,
    read_bin_sim_rev,
)
from bsimvis.app.services.bin_sim_tags import (
    is_library_tag,
    merge_tag_fields,
    read_tags_rev,
)
from bsimvis.app.services.collection_config import resolve_collection_algo
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.nway_match import nway_match

_DEFAULT_NAME = re.compile(r"^(FUN_|sub_|thunk_)", re.I)


class TooLarge(Exception):
    def __init__(self, members, functions=None):
        super().__init__(f"{members} members, {functions} functions")
        self.members = members
        self.functions = functions


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


def _rev_scope(scope):
    return f"global:pool:{scope['pool']}" if scope.get("pool") else scope["collection"]


def _algo(scope):
    if scope.get("pool"):
        from bsimvis.app.services.pool_service import pool_service

        return pool_service.similarity_algo(pool_service.get_pool(scope["pool"]))
    return resolve_collection_algo(scope["collection"])


def _col_id(coll, md5):
    return f"{coll}:{md5}"


def _load_base(r, scope, members):
    """Everything min_edge-independent: columns, function weights, metadata, edges."""
    pool_id = scope.get("pool")
    algo = _algo(scope)

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
            "file_name": meta.get("file_name") or md5,
            "functions": len(fids),
        }
        for (coll, md5), meta, fids in zip(members, file_meta, fid_sets)
    ]

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

    return {
        "algo": algo,
        "columns": columns,
        "funcs": {
            col["id"]: {fid: weights[fid] for fid in fids}
            for col, fids in zip(columns, fid_sets)
        },
        "fmeta": fmeta,
        "library": library,
        "edges": edges,
        "fallback_pairs": fallback,
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


def compute(r, scope, members, params=None):
    """Rows for `members` (list of `(collection, md5)`) under `scope`.

    `scope` is `{"collection": c}` or `{"pool": id}`. `params`: `min_edge`
    (default `similarity.file_min_score`), `mode` (only `stored` for now).
    """
    params = params or {}
    if (params.get("mode") or "stored") != "stored":
        raise NotImplementedError("only mode=stored is available")
    members = list(dict.fromkeys((_origin_coll(c), m) for c, m in members))
    check_caps(len(members))
    min_edge = params.get("min_edge")
    min_edge = default_min_edge() if min_edge is None else float(min_edge)

    rev_scope = _rev_scope(scope)
    base_key = (
        json.dumps(scope, sort_keys=True),
        hashlib.md5(json.dumps(members).encode()).hexdigest(),
        read_bin_sim_rev(r, rev_scope),
        read_tags_rev(r, rev_scope),
        "stored",
    )
    doc_key = base_key + (min_edge,)
    doc = _DOC_CACHE.get(doc_key)
    if doc is not None:
        return doc

    base = _BASE_CACHE.get(base_key)
    if base is None:
        base = _load_base(r, scope, members)
        _BASE_CACHE.put(base_key, base)
    doc = {
        "mode": "stored",
        "algo": base["algo"],
        "min_edge": min_edge,
        "columns": base["columns"],
        "rows": _match(base, min_edge),
        "fmeta": base["fmeta"],
        "fallback_pairs": base["fallback_pairs"],
        "warnings": [],
    }
    _DOC_CACHE.put(doc_key, doc)
    return doc
