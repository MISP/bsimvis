import re

from flask import request
from flask_restx import abort

from bsimvis.app.routes._list_query import fnum, in_bounds
from bsimvis.app.routes.bin_sim import _fids_tags, _fn_haystack
from bsimvis.app.services import nway_diff_service
from bsimvis.app.services.bin_sim_service import _origin_coll
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_taxonomy import tag_in_scope

TABS = ("core", "partial", "unique")
_MD5 = re.compile(r"^[0-9a-f]{32}$")


def _members(collection, pool_id):
    """`md5s=` tokens (`coll:md5`, or bare md5 with `collection=`) -> [(coll, md5)]."""
    tokens = [t.strip() for t in (request.args.get("md5s") or "").split(",")]
    members = []
    for token in filter(None, tokens):
        coll, _, md5 = token.rpartition(":")
        coll = _origin_coll(coll or collection)
        if not coll or not _MD5.match(md5.lower()):
            abort(400, f"bad md5s entry {token!r}: want <collection>:<md5> or an md5")
        members.append((coll, md5.lower()))
    return list(dict.fromkeys(members))


def _scope(members, pool_id):
    if pool_id:
        from bsimvis.app.services.pool_service import pool_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            abort(404, "Pool not found")
        outside = {c for c, _ in members} - set(pool.get("collections", []))
        if outside:
            abort(400, f"not in pool {pool_id}: {sorted(outside)}")
        return {"pool": pool_id}
    colls = sorted({c for c, _ in members})
    # Several collections and no pool: compute() runs them in virtual mode.
    return {"collection": colls[0]} if len(colls) == 1 else {"collections": colls}


def _in_tab(row, tab, k, n):
    span = row["span"]
    return span == n if tab == "core" else span >= k if tab == "partial" else span == 1


def _page(doc, args):
    """Tab counts, then filter + sort + slice one tab of the rows."""
    rows, fmeta = doc["rows"], doc["fmeta"]
    n = len(doc["columns"])
    tab = args.get("tab", "core")
    if tab not in TABS:
        abort(400, f"tab must be one of {TABS}")
    try:
        k = max(2, int(args.get("k", 2)))
    except ValueError:
        abort(400, "k must be an integer")
    scope = args.get("scope", "code")
    if scope not in ("code", "library", "all"):
        abort(400, "scope must be code, library or all")

    counts = {
        t: {
            "code": sum(1 for r in rows if _in_tab(r, t, k, n) and not r["library"]),
            "library": sum(1 for r in rows if _in_tab(r, t, k, n) and r["library"]),
        }
        for t in TABS
    }

    q = (args.get("q") or "").strip().lower()
    column = args.get("column")
    tag_scope = [t.strip() for t in (args.get("tags") or "").split(",") if t.strip()]
    feat_min, feat_max = fnum("feat_min", args), fnum("feat_max", args)
    sup_min = fnum("support_min", args)

    def keep(row):
        if not _in_tab(row, tab, k, n):
            return False
        if scope != "all" and row["library"] != (scope == "library"):
            return False
        if column and column not in row["cells"]:
            return False
        if not in_bounds(row["weight"], feat_min, feat_max):
            return False
        if sup_min is not None and row["support"] < sup_min:
            return False
        fids = list(row["cells"].values())
        if q and not any(q in _fn_haystack(f, fmeta) for f in fids):
            return False
        if tag_scope:
            tags = _fids_tags(fids, fmeta)
            if not any(tag_in_scope(t, p) for t in tags for p in tag_scope):
                return False
        return True

    filtered = [r for r in rows if keep(r)]

    sort_col = args.get("sort_col")
    if sort_col in ("span", "weight", "support", "cohesion", "name"):
        filtered = sorted(
            filtered,
            key=lambda r: (
                str(r[sort_col]).lower() if sort_col == "name" else r[sort_col]
            ),
            reverse=args.get("sort_dir", "desc") != "asc",
        )

    try:
        offset = max(0, int(args.get("offset", 0)))
        limit = int(args.get("limit", 100))
    except ValueError:
        abort(400, "offset and limit must be integers")
    page = filtered[offset : offset + limit] if limit > 0 else filtered[offset:]
    page_fids = {f for r in page for f in r["cells"].values()}
    return {
        "items": page,
        "total": len(filtered),
        "offset": offset,
        "limit": limit,
        "tab": tab,
        "k": k,
        "scope": scope,
        "counts": counts,
        "columns": doc["columns"],
        "functions_metadata": {f: fmeta[f] for f in page_fids if f in fmeta},
        "fallback_pairs": doc["fallback_pairs"],
        "min_edge": doc["min_edge"],
        "mode": doc["mode"],
        "algo": doc["algo"],
        "warnings": doc["warnings"],
    }


def get_nway():
    """N-way file diff over `md5s=`: rows of functions shared across the files."""
    pool_id = request.args.get("pool")
    collection = request.args.get("collection")
    members = _members(collection, pool_id)
    if len(members) < 2:
        abort(400, "md5s needs at least two distinct files")
    mode = request.args.get("mode", "stored")
    if mode not in ("stored", "virtual"):
        abort(400, "mode must be stored or virtual")
    try:
        min_edge = request.args.get("min_edge")
        min_edge = None if min_edge is None else max(0.0, min(1.0, float(min_edge)))
        min_score = request.args.get("min_score", type=float)
        min_features = request.args.get("min_features", type=int)
    except ValueError:
        abort(400, "min_edge must be a number between 0 and 1")

    scope = _scope(members, pool_id)
    try:
        doc = nway_diff_service.compute(
            get_redis(),
            scope,
            members,
            {
                "min_edge": min_edge,
                "mode": mode,
                "algo": request.args.get("algo"),
                "min_score": min_score,
                "min_features": min_features,
            },
        )
    except nway_diff_service.BadParams as e:
        abort(400, str(e))
    except nway_diff_service.TooLarge as e:
        return {"error": "too_large", "members": e.members, "children": []}, 413
    except nway_diff_service.MemberMissing as e:
        return {"error": "unknown_files", "missing": e.missing}, 404
    return _page(doc, request.args)
