"""File search grouped by cluster: the page is made of clusters, not of files.

`/api/file/search` pages over files, so a cluster is cut wherever the page ends
and a filter drops the rest of its cluster. Here the same filters
(`filters_from_args` + `query_files_advanced`) pick the matching files, then the
display tree (`cluster_display`, kept clusters only) says which cluster each
one is shown under: the deepest kept cluster that holds it. Roots are ranked
and paged; each carries its nested kept clusters and the first files of each.

The cost is the filter pass, one SMEMBERS per kept cluster (kept clusters nest,
so a file is in at most `depth` of them) and one hydrate of the page's files.
"""

import logging
import time

from flask import request

from bsimvis.app.routes.bin_cluster import bin_cluster_algo
from bsimvis.app.routes.search_file import (
    filters_from_args,
    hydrate_files,
    query_files_advanced,
    sort_doc_ids,
)
from bsimvis.app.services.cluster_display import (
    DEFAULTS,
    _load,
    display_assignment,
    display_counts,
    display_direct,
    score,
)
from bsimvis.app.services.cluster_utils import get_cluster_metas, name_fields
from bsimvis.app.services.redis_client import get_queue_redis, get_redis

DEFAULT_ROOTS = 20
DEFAULT_PER_CLUSTER = 12
_META_FIELDS = (
    "cluster_id",
    "cluster_uuid",
    "cluster_name",
    "cohesion_score",
    "member_count",
    "architecture_distribution",
    "inferred_tags",
)
_SORTS = ("relevance", "size", "cohesion")


def _s(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def _display_opts():
    o = {}
    for arg, key, cast in [
        ("cluster_min_members", "min_members", int),
        ("cluster_min_cohesion", "min_cohesion", float),
        ("cluster_jump", "jump", float),
        ("cluster_depth", "depth", int),
    ]:
        if request.args.get(arg):
            o[key] = cast(request.args[arg])
    return {k: v for k, v in o.items() if k in DEFAULTS}


def _assign_stored(r, col, algo, tree, tag, cand):
    """Same answer as _assign, read from the precomputed hash (default opts)."""
    where = display_assignment(r, col, algo, tree, tag, only=cand) if tree else {}
    matching = dict.fromkeys(tree, 0)
    direct = {}
    for f, c in where.items():
        direct.setdefault(c, []).append(f)
        while c in matching:  # count the file in the cluster and its kept ancestors
            matching[c] += 1
            c = tree[c]["parent"]
    return where, matching, direct


def _assign(r, col, algo, tree, cand):
    """Each matching file -> the deepest kept cluster holding it, plus counts."""
    order = sorted(tree, key=lambda c: tree[c]["depth"])
    pipe = r.pipeline(transaction=False)
    for c in order:
        pipe.smembers(f"{col}:bin_cluster:{algo}:{c}:members")
    where, matching = {}, {}
    for c, mem in zip(order, pipe.execute()):
        hit = {_s(m) for m in mem or ()} & cand
        matching[c] = len(hit)
        for f in hit:  # deeper clusters come later and win
            where[f] = c
    direct = {}
    for f, c in where.items():
        direct.setdefault(c, []).append(f)
    return where, matching, direct


def _page_of(ids, r, col, pool_id, offset, limit):
    """Sorted page of file docs for `ids` (one sort walk, one hydrate)."""
    by = request.args.get("sort_by", "file_name")
    order = request.args.get("sort_order", "asc").lower()
    ordered = sort_doc_ids(r, col, ids, by, order, slice_end=offset + limit)
    page = ordered[offset : offset + limit]
    files, cmap, _, _ = hydrate_files(r, col, pool_id, page)
    return files, cmap


def search_files_by_cluster():
    try:
        t0 = time.perf_counter()
        col = request.args.get("collection")
        if not col:
            return {"error": "No collection specified"}, 400
        if request.args.get("pool"):
            return {"error": "Cluster grouping is not available for pools yet"}, 400

        r = get_redis()
        axis = request.args.get("axis", "overall").strip().lower()
        node_type = request.args.get("node_type", "file").strip().lower()
        algo = bin_cluster_algo(col, request.args.get("algo"), axis, node_type)
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_ROOTS))
        per = int(request.args.get("per_cluster", DEFAULT_PER_CLUSTER))

        flt = filters_from_args()
        opts = _display_opts()
        tree, tag = _load(r, col, algo, get_queue_redis(), **opts)
        cid, rest = request.args.get("cluster"), request.args.get("unclustered")
        # No filter and default display: counts come straight from the stored
        # sets and only the shown clusters' files are read, never all of them.
        # ponytail: a deleted file counts until the next recluster rebuilds the sets.
        fast = tree and not opts and not rest and not flt["fields"]
        fast = fast and not any(flt["tags"].values())
        if fast:
            display_assignment(r, col, algo, tree, tag, only=set())  # build if stale
            direct_n = display_counts(r, col, algo, tree, tag)
            matching = dict.fromkeys(tree, 0)
            for c, n in direct_n.items():
                while c in matching:
                    matching[c] += n
                    c = tree[c]["parent"]
            total = r.scard(f"{col}:all_files")
            loose = ()
            n_loose = max(total - sum(direct_n.values()), 0)
            direct = display_direct(r, col, algo, tag, [str(cid)] if cid else [])
        else:
            cand = {_s(i) for i in query_files_advanced(r, col, flt)}
            if opts:  # custom display params: no stored sets, walk the members
                where, matching, direct = _assign(r, col, algo, tree, cand)
            else:
                where, matching, direct = _assign_stored(r, col, algo, tree, tag, cand)
            loose = cand - where.keys()
            total, n_loose = len(cand), len(loose)
        out = {
            "collection": col,
            "algo": algo,
            "total": total,
            "has_clusters": bool(tree),
            "unclustered_total": n_loose,
        }

        # More files of one cluster, or of the unclustered rest.
        cid, rest = request.args.get("cluster"), request.args.get("unclustered")
        if cid or rest:
            ids = loose if rest else set(direct.get(str(cid), ()))
            files, cmap = _page_of(ids, r, col, None, offset, limit)
            return {
                **out,
                "files_total": len(ids),
                "files": files,
                "bin_cluster_map": cmap,
            }

        kids = {}
        for c, v in tree.items():
            if v["parent"] in tree and matching.get(c):
                kids.setdefault(v["parent"], []).append(c)
        rank = lambda c: score(matching[c], tree[c]["coh"])
        sort = request.args.get("cluster_sort", "relevance")
        key = {
            "size": lambda c: matching[c],
            "cohesion": lambda c: tree[c]["coh"],
        }.get(sort, rank)
        roots = sorted(
            (c for c, v in tree.items() if v["parent"] is None and matching.get(c)),
            key=key,
            reverse=True,
        )
        page = roots[offset : offset + limit]

        nodes, stack = [], list(page)
        while stack:
            c = stack.pop()
            nodes.append(c)
            kids[c] = sorted(kids.get(c, []), key=rank, reverse=True)
            stack.extend(kids[c])
        metas = get_cluster_metas(r, col, algo, nodes, "bin_cluster")

        # One sort over every file of the page's clusters, then only the first
        # `per` of each cluster are hydrated; `cluster=` loads the rest.
        if fast:
            direct = display_direct(r, col, algo, tag, nodes)
        by_node = {c: direct.get(c, []) for c in nodes}
        by = request.args.get("sort_by", "file_name")
        way = request.args.get("sort_order", "asc").lower()
        every = {f for fs in by_node.values() for f in fs}
        rank_of = {f: i for i, f in enumerate(sort_doc_ids(r, col, every, by, way))}
        shown = {c: sorted(fs, key=rank_of.get)[:per] for c, fs in by_node.items()}
        files, cmap, _, _ = hydrate_files(
            r, col, None, [f for fs in shown.values() for f in fs]
        )
        doc = {d["file_id"]: d for d in files}

        def build(c):
            meta = {k: metas.get(c, {}).get(k) for k in _META_FIELDS}
            meta.update(name_fields(metas.get(c)))
            return {
                **meta,
                "cid": c,
                "depth": tree[c]["depth"],
                "n": tree[c]["n"],
                "matching": matching[c],
                "files_total": len(by_node[c]),
                "files": [doc[f] for f in shown[c] if f in doc],
                "children": [build(k) for k in kids[c]],
            }

        out.update(
            roots_total=len(roots),
            offset=offset,
            limit=limit,
            clusters=[build(c) for c in page],
            bin_cluster_map=cmap,
        )
        logging.info(
            f"FILE SEARCH BY CLUSTER | {time.perf_counter()-t0:.3f}s | files={total} kept={len(tree)} roots={len(roots)}"
        )
        return out
    except Exception as e:
        logging.error(f"Error in search_files_by_cluster: {e}", exc_info=True)
        return {"error": str(e)}, 500
