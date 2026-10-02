import re

from flask import request
from flask_restx import abort

from bsimvis.app.routes._list_query import fnum, in_bounds
from bsimvis.app.routes.bin_sim import _fids_tags, _fn_haystack
from bsimvis.app.services import nway_diff_service
from bsimvis.app.services.bin_sim_service import _origin_coll
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_taxonomy import tag_in_scope

TABS = ("all", "core", "partial", "unique")
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


class _UnknownBatch(Exception):
    pass


def _batch_members(r, collection, batch_uuid):
    """A batch's files as [(coll, md5)]; unknown or empty batch -> 404."""
    from bsimvis.app.routes.file import _resolve_transfer_batch_sources

    found = [
        (_origin_coll(c), m.lower())
        for c, m in _resolve_transfer_batch_sources(r, collection, batch_uuid)
    ]
    if not found:
        raise _UnknownBatch()
    return found


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


AUTO_FILE_COLUMNS = 12


def _cluster_node(r, pool_id, collection, uuid):
    """A binary cluster node's files and child groups, read-only.

    Mirrors the namespace rules of routes/bin_cluster.py (file nodes only).
    Returns `(scope, members, groups, children)`: `groups` is one entry per
    child cluster plus a `direct` pseudo-group for files in no child, `children`
    is the same child list shaped for the 413 body.
    """
    from bsimvis.app.routes.bin_cluster import _pool_bin_cluster_namespace
    from bsimvis.app.services.cluster_utils import (
        get_cluster_metas,
        get_tree_links,
        resolve_cluster_id_by_uuid,
    )
    from bsimvis.app.services.collection_config import resolve_collection_algo
    from bsimvis.app.services.config_service import config_service

    algo = request.args.get("algo")
    axis = request.args.get("axis", "overall").strip().lower()
    if pool_id:
        from bsimvis.app.services.pool_service import pool_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            abort(404, "Pool not found")
        algo, collection_style = _pool_bin_cluster_namespace(
            pool, pool_service.similarity_algo(pool), axis
        )
        if not collection_style:
            abort(400, "this pool's cluster engine has no per-node member sets")
        ns, scope = f"global:pool:{pool_id}", {"pool": pool_id}
    else:
        if not collection:
            abort(400, "collection or pool required with cluster_uuid")
        algo = resolve_collection_algo(collection, algo)
        algo = f"{algo}:{axis}" if axis != "overall" else algo
        if config_service.get("clustering.bin_engine") == "hierarchical_snn":
            algo = f"{algo}:snn"
        ns, scope = collection, {"collection": collection}

    cid = resolve_cluster_id_by_uuid(r, ns, algo, uuid, "bin_cluster")
    if cid is None:
        abort(404, "unknown cluster_uuid")
    base = f"{ns}:bin_cluster:{algo}"
    _, parent_to_children = get_tree_links(r, ns, algo, "bin_cluster")
    child_ids = parent_to_children.get(str(cid), [])
    metas = get_cluster_metas(r, ns, algo, child_ids, "bin_cluster")
    child_ids = sorted(
        metas, key=lambda c: metas[c].get("member_count", 0), reverse=True
    )
    children = [
        {
            "cluster_uuid": metas[c].get("cluster_uuid"),
            "cluster_name": metas[c].get("cluster_name"),
            "member_count": metas[c].get("member_count", 0),
        }
        for c in child_ids
    ]

    n = r.scard(f"{base}:{cid}:members")
    if n > nway_diff_service.caps()[0]:
        err = nway_diff_service.TooLarge(n)
        err.children = children
        raise err
    pipe = r.pipeline(transaction=False)
    for key in [f"{base}:{cid}:members", *(f"{base}:{c}:members" for c in child_ids)]:
        pipe.smembers(key)

    def ident(raw):
        # member ids are `{coll}:file:{md5}`; the service's column id is `{coll}:{md5}`
        parts = (raw.decode() if isinstance(raw, bytes) else str(raw)).split(":")
        return f"{_origin_coll(parts[0])}:{parts[-1].lower()}"

    sets = [{ident(m) for m in raw} for raw in pipe.execute()]
    all_ids = sorted(sets[0])
    members = [tuple(i.rsplit(":", 1)) for i in all_ids]
    groups, taken = [], set()
    for c, ids in zip(child_ids, sets[1:]):
        ids = sorted(ids & sets[0])
        if ids:
            groups.append(
                {
                    "id": metas[c].get("cluster_uuid") or str(c),
                    "kind": "child",
                    "label": metas[c].get("cluster_name") or str(c),
                    "cluster_uuid": metas[c].get("cluster_uuid"),
                    "member_count": len(ids),
                    "members": ids,
                }
            )
            taken.update(ids)
    direct = [i for i in all_ids if i not in taken]
    if direct and groups:
        groups.append(
            {
                "id": "direct",
                "kind": "direct",
                "label": "direct files",
                "member_count": len(direct),
                "members": direct,
            }
        )
    return scope, members, groups, children


def _in_tab(row, tab, k, n, focus=False):
    span = row["span"]
    if tab == "all":
        return span >= 1 or not focus
    if tab == "unique" and focus:  # one side only
        return span == 1
    if tab == "unique":  # always one file, even when columns are child groups
        return row.get("file_span", span) == 1
    return span == n if tab == "core" else span >= k


def _row_fids(row):
    """Function ids of a row; child-group rows keep them under `files`."""
    return (row.get("files") or row["cells"]).values()


def _row_clusters(r, rows, fmeta, pool_id, args):
    """Function clusters of the paged rows: {row index: [uuid]}, plus the uuid -> card map.

    A row lists the clusters its functions sit in, most shared first.
    """
    from bsimvis.app.services.cluster_utils import get_cluster_metas
    from bsimvis.app.services.collection_config import resolve_collection_algo

    fids = list(dict.fromkeys(f for row in rows for f in _row_fids(row)))
    pipe = r.pipeline(transaction=False)
    for fid in fids:
        pipe.hkeys(
            f"global:pool:{pool_id}:{fid}:cluster_scores"
            if pool_id
            else f"{fid}:cluster_scores"
        )
    by_fid = {
        f: [c.decode() if isinstance(c, bytes) else c for c in cs]
        for f, cs in zip(fids, pipe.execute())
    }
    ns = (
        f"global:pool:{pool_id}" if pool_id else (fids[0].split(":")[0] if fids else "")
    )
    cids = {c for cs in by_fid.values() for c in cs}
    metas = (
        get_cluster_metas(r, ns, resolve_collection_algo(ns, args.get("algo")), cids)
        if cids
        else {}
    )
    cards, per_row = {}, []
    for row in rows:
        n = {}
        for f in _row_fids(row):
            for c in by_fid.get(f, []):
                if c in metas:
                    n[c] = n.get(c, 0) + 1
        top = sorted(n, key=lambda c: -n[c])[:3]
        per_row.append([metas[c].get("cluster_uuid") for c in top])
        for c in top:
            m = metas[c]
            cards[m.get("cluster_uuid")] = {
                k: m.get(k)
                for k in (
                    "cluster_id",
                    "cluster_uuid",
                    "cluster_name",
                    "cohesion_score",
                    "member_count",
                    "cluster_stability",
                    "avg_features",
                )
            }
    return per_row, cards


def _page(doc, args, r=None, pool_id=None):
    """Tab counts, then filter + sort + slice one tab of the rows."""
    rows, fmeta = doc["rows"], doc["fmeta"]
    focus = doc["columns_mode"] == "focus"
    n = len(doc["columns"])
    tab = args.get("tab", "core")
    if tab not in TABS:
        abort(400, f"tab must be one of {TABS}")
    try:
        k = max(2, int(args.get("k", 2)))
    except ValueError:
        abort(400, "k must be an integer")
    side = args.get("side", "")
    if side not in ("", "focus", "rest"):
        abort(400, "side must be focus or rest")
    scope = args.get("scope", "code")
    if scope not in ("code", "library", "all"):
        abort(400, "scope must be code, library or all")

    counts = {
        t: {
            "code": sum(
                1 for r in rows if _in_tab(r, t, k, n, focus) and not r["library"]
            ),
            "library": sum(
                1 for r in rows if _in_tab(r, t, k, n, focus) and r["library"]
            ),
        }
        for t in TABS
    }

    q = (args.get("q") or "").strip().lower()
    column = args.get("column")
    tag_scope = [t.strip() for t in (args.get("tags") or "").split(",") if t.strip()]
    feat_min, feat_max = fnum("feat_min", args), fnum("feat_max", args)
    sup_min = fnum("support_min", args)

    def keep(row):
        if not _in_tab(row, tab, k, n, focus):
            return False
        if focus and side and not row["mask"] & (1 if side == "focus" else 2):
            return False
        if scope != "all" and row["library"] != (scope == "library"):
            return False
        if column:
            cell = row["cells"].get(column)
            if not cell or (isinstance(cell, dict) and not cell["present"]):
                return False
        if not in_bounds(row["weight"], feat_min, feat_max):
            return False
        if sup_min is not None and row["support"] < sup_min:
            return False
        fids = list(_row_fids(row))
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
    page_fids = {f for row in page for f in _row_fids(row)}
    per_row, cards = _row_clusters(r, page, fmeta, pool_id, args)
    for row, uuids in zip(page, per_row):
        row["clusters"] = uuids
    return {
        "items": page,
        "clusters": cards,
        "total": len(filtered),
        "offset": offset,
        "limit": limit,
        "tab": tab,
        "k": k,
        "scope": scope,
        "counts": counts,
        "columns": doc["columns"],
        "columns_mode": doc["columns_mode"],
        "file_columns": doc["file_columns"],
        "functions_metadata": {f: fmeta[f] for f in page_fids if f in fmeta},
        "fallback_pairs": doc["fallback_pairs"],
        "min_edge": doc["min_edge"],
        "mode": doc["mode"],
        "algo": doc["algo"],
        "warnings": doc["warnings"],
    }


class _Reply(Exception):
    """An early (body, status) answer from _load_doc."""


def _load_doc():
    """Parse the shared N-way params and compute the doc: (doc, redis, pool_id)."""
    pool_id = request.args.get("pool")
    collection = request.args.get("collection")
    cluster_uuid = (request.args.get("cluster_uuid") or "").strip().lower()
    columns = request.args.get("columns", "auto")
    if columns not in ("auto", "files", "children"):
        abort(400, "columns must be auto, files or children")
    if columns == "children" and not cluster_uuid:
        abort(400, "columns=children needs cluster_uuid")
    focus = [
        t.strip() for t in (request.args.get("focus") or "").split(",") if t.strip()
    ]
    focus_rule = request.args.get("focus_rule", "any")
    if focus_rule not in ("any", "all"):
        abort(400, "focus_rule must be any or all")
    mode = request.args.get("mode", "stored")
    if mode not in ("stored", "virtual"):
        abort(400, "mode must be stored or virtual")
    try:
        min_edge = request.args.get("min_edge")
        min_edge = None if min_edge is None else max(0.0, min(1.0, float(min_edge)))
        min_score = request.args.get("min_score", type=float)
        min_features = request.args.get("min_features", type=int)
        presence = float(request.args.get("child_presence", 0.5))
    except ValueError:
        abort(400, "min_edge and child_presence must be numbers between 0 and 1")

    r = get_redis()
    children, groups = [], None
    try:
        if cluster_uuid:
            scope, members, node_groups, children = _cluster_node(
                r, pool_id, collection, cluster_uuid
            )
            if columns == "children" or (
                columns == "auto" and len(members) > AUTO_FILE_COLUMNS
            ):
                groups = node_groups if len(node_groups) > 1 else None
        else:
            members = _members(collection, pool_id)
            batch_uuid = (request.args.get("batch_uuid") or "").strip()
            if batch_uuid:
                members = list(
                    dict.fromkeys(members + _batch_members(r, collection, batch_uuid))
                )
            scope = _scope(members, pool_id)
        if len(members) < 2:
            abort(400, "need at least two distinct files")
        doc = nway_diff_service.compute(
            r,
            scope,
            members,
            {
                "min_edge": min_edge,
                "mode": mode,
                "algo": request.args.get("algo"),
                "min_score": min_score,
                "min_features": min_features,
                "groups": groups,
                "focus": focus,
                "focus_rule": focus_rule,
                "child_presence": max(0.0, min(1.0, presence)),
            },
        )
    except nway_diff_service.BadParams as e:
        abort(400, str(e))
    except nway_diff_service.TooLarge as e:
        raise _Reply(
            (
                {
                    "error": "too_large",
                    "message": "too many files: narrow the set",
                    "members": e.members,
                    "children": getattr(e, "children", children),
                },
                413,
            )
        )
    except _UnknownBatch:
        raise _Reply(({"error": "unknown_batch"}, 404))
    except nway_diff_service.MemberMissing as e:
        raise _Reply(({"error": "unknown_files", "missing": e.missing}, 404))
    return doc, r, pool_id


def get_nway():
    """N-way file diff over `md5s=` or a binary cluster's files (`cluster_uuid=`)."""
    try:
        doc, r, pool_id = _load_doc()
    except _Reply as e:
        return e.args[0]
    return _page(doc, request.args, r, pool_id)


def get_nway_neighbors():
    """Call-graph neighbors of one N-way row, regrouped into the doc's rows.

    `fids` are the row's functions; each neighbor is mapped to the row that
    holds it, so a callee present in 5 of 6 files reads as one conserved edge.
    """
    role = request.args.get("role", "callees")
    if role not in ("callers", "callees"):
        abort(400, "role must be callers or callees")
    fids = [t for t in (request.args.get("fids") or "").split(",") if t]
    if not fids:
        abort(400, "fids is required")
    try:
        doc, r, _ = _load_doc()
    except _Reply as e:
        return e.args[0]
    fmeta = doc["fmeta"]
    row_of = {f: i for i, row in enumerate(doc["rows"]) for f in _row_fids(row)}
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:{role}")
    groups = {}
    for src, members in zip(fids, pipe.execute()):
        for n in members:
            n = n.decode() if isinstance(n, bytes) else n
            if n.startswith("ext:"):
                key = ("ext", n)
            elif n in row_of:
                key = ("row", row_of[n])
            else:
                key = ("none", n)
            g = groups.setdefault(key, {"kind": key[0], "sources": {}})
            g["sources"].setdefault(src, []).append(n)
    items = []
    for (kind, ref), g in groups.items():
        item = {"kind": kind, "support": len(g["sources"]), "of": len(fids)}
        item["sources"] = g["sources"]
        if kind == "row":
            row = doc["rows"][ref]
            item["cells"] = dict(row.get("files") or row["cells"])
            first = next(iter(item["cells"].values()))
            item["name"] = (fmeta.get(first) or {}).get("name")
        elif kind == "ext":
            item["name"] = ref[4:]
        else:
            item["name"] = (fmeta.get(ref) or {}).get("name") or ref.rsplit(":", 1)[-1]
        items.append(item)
    items.sort(key=lambda i: (-i["support"], i["name"] or ""))
    shown = set(fids)
    for item in items:
        shown.update(f for f in item.get("cells", {}).values() if isinstance(f, str))
    return {
        "role": role,
        "of": len(fids),
        "items": items,
        "functions_metadata": {f: fmeta[f] for f in shown if f in fmeta},
    }
