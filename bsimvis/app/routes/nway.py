import hashlib
import json
import re

from flask import request
from flask_restx import abort

from bsimvis.app.routes._list_query import fnum, in_bounds
from bsimvis.app.routes.bin_sim import _fids_tags, _fn_haystack
from bsimvis.app.services import (
    nway_diff_service,
    nway_facets_service,
    nway_group_service,
)
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME
from bsimvis.app.services.bin_sim_service import _origin_coll
from bsimvis.app.services.redis_client import get_queue_redis, get_redis
from bsimvis.app.services.tag_taxonomy import tag_in_scope

# A stored cluster only names a row when its members agree this much.
CLUSTER_NAME_MIN_COHESION = 0.8
TABS = ("all", "core", "partial", "unique")
_MD5 = re.compile(r"^[0-9a-f]{32}$")
_PROGRESS_ID = re.compile(r"^[\w-]{8,40}$")


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


def _cluster_node(r, pool_id, collection, uuid, open_ids=()):
    """A binary cluster node's files and child groups, read-only.

    Mirrors the namespace rules of routes/bin_cluster.py (file nodes only).
    Returns `(scope, members, groups, children)`: `groups` is one entry per
    child cluster plus a `direct` pseudo-group for files in no child, `children`
    is the same child list shaped for the 413 body. A group named in `open_ids`
    is replaced by its own child clusters (and direct files), recursively, and a
    leaf by one column per file; see `nway_diff_service.grow_columns`.
    """
    from bsimvis.app.routes.bin_cluster import _pool_bin_cluster_namespace
    from bsimvis.app.services.cluster_utils import (
        get_cluster_metas,
        Tree,
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
    child_ids, _ = Tree(r, ns, algo, "bin_cluster").children(cid)
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
                    "cid": c,
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
    tree = Tree(r, ns, algo, "bin_cluster")

    def children_of(g):
        """Sub-groups of one group: its child clusters plus direct files, else one column per file."""
        if g.get("cid") is not None:
            kid_ids, _ = tree.children(g["cid"])
            kid_metas = get_cluster_metas(r, ns, algo, kid_ids, "bin_cluster")
            order = sorted(
                kid_metas,
                key=lambda c: kid_metas[c].get("member_count", 0),
                reverse=True,
            )
            pipe = r.pipeline(transaction=False)
            for c in order:
                pipe.smembers(f"{base}:{c}:members")
            have, subs, took = set(g["members"]), [], set()
            for c, raw in zip(order, pipe.execute()):
                ids = sorted({ident(m) for m in raw} & have)
                if ids:
                    subs.append(
                        {
                            "id": kid_metas[c].get("cluster_uuid") or str(c),
                            "kind": "child",
                            "label": kid_metas[c].get("cluster_name") or str(c),
                            "cluster_uuid": kid_metas[c].get("cluster_uuid"),
                            "member_count": len(ids),
                            "members": ids,
                            "cid": c,
                        }
                    )
                    took.update(ids)
            rest = [m for m in g["members"] if m not in took]
            if subs and rest:
                subs.append(
                    {
                        "id": f"{g['id']}:direct",
                        "kind": "direct",
                        "label": "direct files",
                        "member_count": len(rest),
                        "members": rest,
                    }
                )
            if subs:
                return subs
        return nway_diff_service.file_groups(g, {})

    groups = nway_diff_service.grow_columns(groups, set(open_ids), children_of)
    return scope, members, groups, children


def _in_tab(row, tab, k, n, focus=False, twin=False):
    # twin columns hold several files each: tabs count the files, not the columns
    span = row.get("file_span", row["span"]) if twin else row["span"]
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


def _attach_clusters(r, rows, fmeta, pool_id, args):
    """Set row clusters, rename default-named rows after a cohesive named cluster."""
    per_row, cards = _row_clusters(r, rows, fmeta, pool_id, args)
    for row, uuids in zip(rows, per_row):
        row["clusters"] = uuids
        if _DEFAULT_NAME.match(row["name"]):
            for u in uuids:
                c = cards[u]
                n = c.get("cluster_name") or ""
                if (
                    (c.get("cohesion_score") or 0) >= CLUSTER_NAME_MIN_COHESION
                    and not _DEFAULT_NAME.match(n)
                    and not re.fullmatch(r"Cluster \d+", n)
                ):
                    row["name"], row["name_from"] = n, "cluster"
                    break
    return cards


def _page(doc, args, r=None, pool_id=None):
    """Tab counts, then filter + sort + slice one tab of the rows."""
    rows, fmeta = doc["rows"], doc["fmeta"]
    focus = doc["columns_mode"] == "focus"
    twin = doc["columns_mode"] == "twins"
    n = len(doc["file_columns"] if twin else doc["columns"])
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
                1 for r in rows if _in_tab(r, t, k, n, focus, twin) and not r["library"]
            ),
            "library": sum(
                1 for r in rows if _in_tab(r, t, k, n, focus, twin) and r["library"]
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
        if not _in_tab(row, tab, k, n, focus, twin):
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
        if tag_scope:
            tags = _fids_tags(fids, fmeta)
            if not any(tag_in_scope(t, p) for t in tags for p in tag_scope):
                return False
        return True

    filtered = [r for r in rows if keep(r)]
    page_clusters = True
    if q:
        # Full-text over the whole row: name, cluster names, and every function's text/tags.
        cards = _attach_clusters(r, filtered, fmeta, pool_id, args)

        def hit(row):
            text = [row["name"]] + [
                (cards[u].get("cluster_name") or "") for u in row["clusters"]
            ]
            text += [_fn_haystack(f, fmeta) for f in _row_fids(row)]
            return q in " ".join(text).lower()

        filtered = [row for row in filtered if hit(row)]
        page_clusters = False

    sort_col = args.get("sort_col")
    if sort_col in ("span", "weight", "support", "cohesion", "name"):
        filtered = sorted(
            filtered,
            key=lambda r: (
                str(r[sort_col]).lower()
                if sort_col == "name"
                else (
                    r.get("file_span", r["span"]) if sort_col == "span" else r[sort_col]
                )
            ),
            reverse=args.get("sort_dir", "desc") != "asc",
        )

    try:
        offset = max(0, int(args.get("offset", 0)))
        limit = int(args.get("limit", 100))
    except ValueError:
        abort(400, "offset and limit must be integers")
    total, shown_groups, note = len(filtered), {}, ""
    if args.get("group") or args.get("group_merge"):
        # Rows come back in group order; a tag row can repeat under several groups.
        # A closed group is one `{"g", "header": true}` item with no row.
        try:
            entries, groups, note = nway_group_service.group_rows(
                filtered, doc, r, args.get("group", ""), args
            )
        except nway_diff_service.BadParams as e:
            abort(400, str(e))
        part = entries[offset : offset + limit] if limit > 0 else entries[offset:]
        page = [
            {**row, "g": gid, **extra} if row else {"g": gid, "header": True}
            for gid, row, extra in part
        ]
        needed = {p for gid, _, _ in part if gid in groups for p in groups[gid]["path"]}
        shown_groups = {g: m for g, m in groups.items() if g in needed}
        total = len(entries)
    else:
        page = filtered[offset : offset + limit] if limit > 0 else filtered[offset:]
    real = [row for row in page if not row.get("header")]
    page_fids = {f for row in real for f in _row_fids(row)}
    if page_clusters:
        cards = _attach_clusters(r, real, fmeta, pool_id, args)
    return {
        "items": page,
        "clusters": cards,
        "total": total,
        "groups": shown_groups,
        "group_note": note,
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


class _Cancelled(Exception):
    pass


class _Reply(Exception):
    """An early (body, status) answer from _load_doc."""


def _load_doc():
    """Parse the shared N-way params and compute the doc: (doc, redis, pool_id)."""
    pool_id = request.args.get("pool")
    collection = request.args.get("collection")
    cluster_uuid = (request.args.get("cluster_uuid") or "").strip().lower()
    columns = request.args.get("columns", "auto")
    if columns not in ("auto", "files", "children", "twins"):
        abort(400, "columns must be auto, files, children or twins")
    if columns == "children" and not cluster_uuid:
        abort(400, "columns=children needs cluster_uuid")
    focus = [
        t.strip() for t in (request.args.get("focus") or "").split(",") if t.strip()
    ]
    try:
        open_ids = json.loads(request.args.get("col_open") or "[]")
    except ValueError:
        open_ids = None
    if not isinstance(open_ids, list) or not all(isinstance(i, str) for i in open_ids):
        abort(400, "col_open must be a JSON list of column ids")
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
        pair_k = request.args.get("pair_k", type=int)
    except ValueError:
        abort(400, "min_edge and child_presence must be numbers between 0 and 1")

    r = get_redis()
    children, groups = [], None
    pid = request.args.get("progress_id", "")
    if _PROGRESS_ID.match(pid):
        q, last = get_queue_redis(), [-1]

        def report(frac, label):
            if int(frac * 100) != last[0]:
                last[0] = int(frac * 100)
                if q.exists(f"nway:cancel:{pid}"):
                    raise _Cancelled()
                q.setex(f"nway:progress:{pid}", 120, json.dumps([frac, label]))

        nway_diff_service.set_progress(report)
    try:
        if cluster_uuid:
            scope, members, node_groups, children = _cluster_node(
                r, pool_id, collection, cluster_uuid, open_ids
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
        # `solo` lets the function view graph a set that sits in one file.
        if len(members) < (1 if request.args.get("solo") else 2):
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
                "twins": columns == "twins",
                "open": open_ids,
                "focus": focus,
                "focus_rule": focus_rule,
                "child_presence": max(0.0, min(1.0, presence)),
                "pair_k": None if pair_k is None else max(0, pair_k),
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
                    "functions": e.functions,
                    "caps": dict(
                        zip(("members", "functions"), nway_diff_service.caps())
                    ),
                    "children": getattr(e, "children", children),
                },
                413,
            )
        )
    except _UnknownBatch:
        raise _Reply(({"error": "unknown_batch"}, 404))
    except _Cancelled:
        raise _Reply(({"error": "cancelled"}, 409))
    except nway_diff_service.MemberMissing as e:
        raise _Reply(({"error": "unknown_files", "missing": e.missing}, 404))
    finally:
        nway_diff_service.set_progress(None)
    return doc, r, pool_id


def get_nway_progress():
    """Fraction and label of a running `/nway` call that was sent with `progress_id`."""
    pid = request.args.get("id", "")
    raw = (
        get_queue_redis().get(f"nway:progress:{pid}")
        if _PROGRESS_ID.match(pid)
        else None
    )
    if not raw:
        return {}
    frac, label = json.loads(raw)
    return {"fraction": frac, "label": label}


def cancel_nway():
    """Stop a running `/nway` call at its next progress step (it answers 409)."""
    pid = request.args.get("id", "")
    if not _PROGRESS_ID.match(pid):
        abort(400, "id must be the progress_id of the call")
    get_queue_redis().setex(f"nway:cancel:{pid}", 120, 1)
    return {"status": "cancelling"}


def _shared_cluster(r, members, args):
    """The smallest binary cluster holding every file of a set, or None.

    One collection only (pools keep their clusters in another namespace).
    """
    from bsimvis.app.services.cluster_utils import get_cluster_metas
    from bsimvis.app.services.collection_config import resolve_collection_algo
    from bsimvis.app.services.config_service import config_service

    colls = {c for c, _ in members}
    if len(colls) != 1:
        return None
    coll = colls.pop()
    axis = args.get("axis", "overall").strip().lower()
    algo = resolve_collection_algo(coll, args.get("algo"))
    algo = f"{algo}:{axis}" if axis != "overall" else algo
    if config_service.get("clustering.bin_engine") == "hierarchical_snn":
        algo = f"{algo}:snn"
    pipe = r.pipeline(transaction=False)
    for c, md5 in members:
        pipe.smembers(f"{c}:file:{md5}:bin_clusters:{algo}")
    sets = [
        {x.decode() if isinstance(x, bytes) else str(x) for x in s}
        for s in pipe.execute()
    ]
    common = set.intersection(*sets) if sets else set()
    if not common:
        return None
    metas = get_cluster_metas(r, coll, algo, list(common), "bin_cluster")
    if not metas:
        return None
    m = min(metas.values(), key=lambda m: m.get("member_count", 0))
    return {
        "cluster_uuid": m.get("cluster_uuid"),
        "cluster_name": m.get("cluster_name"),
        "member_count": m.get("member_count", 0),
        "collection": coll,
    }


def get_nway():
    """N-way file diff over `md5s=` or a binary cluster's files (`cluster_uuid=`)."""
    try:
        doc, r, pool_id = _load_doc()
    except _Reply as e:
        return e.args[0]
    page = _page(doc, request.args, r, pool_id)
    if not request.args.get("cluster_uuid") and not pool_id:
        page["shared_cluster"] = _shared_cluster(
            r,
            [
                tuple(c["id"].rsplit(":", 1))
                for c in doc["file_columns"] or doc["columns"]
            ],
            request.args,
        )
    return page


NEIGHBOR_MAX_DEPTH = 5
DEEP_CAP = 8  # neighbors kept per expanded row, from depth 2 on
MAX_NODES = 60  # all levels together


def _group_neighbors(r, doc, fids, role, row_of):
    """Callers/callees of `fids` regrouped into the doc's rows.

    Each item is one neighbor row (or an imported symbol, or a function outside
    every row) with the source functions that reach it.
    """
    fmeta = doc["fmeta"]
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
        item["key"] = f"{kind}:{ref}"
        if kind == "row":
            row = doc["rows"][ref]
            item["cells"] = dict(row.get("files") or row["cells"])
            item["library"] = bool(row.get("library"))
            item["name"] = row["name"]
            if row.get("name_from"):
                item["name_from"] = row["name_from"]
        elif kind == "ext":
            item["name"] = ref[4:]
            item["library"] = True  # imported symbols are library calls
        else:
            item["name"] = (fmeta.get(ref) or {}).get("name") or ref.rsplit(":", 1)[-1]
        items.append(item)
    items.sort(key=lambda i: (-i["support"], i["name"] or ""))
    return items


HUB_CALLERS = 25  # a callee with more callers than this is skipped by `cocallers`
CALLEE_CAP = 20  # callee rows `cocallers` looks through


def _walk(r, doc, fids, role, row_of, scope, depth):
    """Rows reached from `fids` hop by hop: (levels, truncated, visited keys, last frontier)."""
    first = _group_neighbors(r, doc, fids, role, row_of)
    for item in first:
        item["parents"] = ["focus"]
    levels = [first]
    visited = {i["key"] for i in first}
    if fids[0] in row_of:
        visited.add(f"row:{row_of[fids[0]]}")

    def expandable(item):
        return item["kind"] == "row" and (
            scope == "all" or (scope == "library") == item["library"]
        )

    total, truncated = len(first), False
    frontier = [i for i in first if expandable(i)]
    for _ in range(depth - 1):
        nxt = {}
        for parent in frontier:
            src = [f for f in parent["cells"].values() if isinstance(f, str)]
            for item in _group_neighbors(r, doc, src, role, row_of)[:DEEP_CAP]:
                if item["key"] in visited:
                    continue
                if item["key"] in nxt:
                    nxt[item["key"]]["parents"].append(parent["key"])
                else:
                    item["parents"] = [parent["key"]]
                    nxt[item["key"]] = item
        level = list(nxt.values())
        if total + len(level) > MAX_NODES:
            level = level[: max(0, MAX_NODES - total)]
            truncated = True
        if not level:
            break
        levels.append(level)
        total += len(level)
        visited.update(i["key"] for i in level)
        frontier = [i for i in level if expandable(i)]
    return levels, truncated, visited, frontier


def _cocallers(r, doc, fids, row_of, scope, depth):
    """The other callers of the callees, one list per level: functions that call what they call.

    `levels[s]` are the other callers of the callees `s + 1` hops from the focus, so
    the UI can set them in the column just left of those callees (the focus column
    for s = 0) and every edge still runs caller -> callee. Ranked by how many of
    those callee rows they share. A callee called by more than HUB_CALLERS functions
    (malloc, csum...) says nothing about similarity and would flood the answer, so
    it is skipped and reported in `hubs`. Functions outside every row come back too
    (kind `none`): they may be an unmatched sibling.
    """
    fmeta = doc["fmeta"]
    walk, _, _, _ = _walk(r, doc, fids, "callees", row_of, scope, depth)
    focus_keys = {f"row:{row_of[f]}" for f in fids if f in row_of}
    out, counts, hubs, hub_keys, truncated = [], [], [], {}, False
    shown = set(fids)
    for s, level in enumerate(walk):
        callees = [
            i
            for i in level
            if i["kind"] == "row"
            and (scope == "all" or (scope == "library") == i["library"])
        ]
        counts.append(len(callees))
        skip = focus_keys | {i["key"] for i in level}
        if s:
            skip |= {i["key"] for i in walk[s - 1]}
        found = {}
        for callee in callees[:CALLEE_CAP]:
            src = [f for f in callee["cells"].values() if isinstance(f, str)]
            callers = [
                i
                for i in _group_neighbors(r, doc, src, "callers", row_of)
                if i["kind"] in ("row", "none") and i["key"] not in skip
            ]
            if len(callers) > HUB_CALLERS:
                hubs.append(callee["name"])
                hub_keys[callee["key"]] = len(callers)
                continue
            for item in callers:
                if item["key"] in found:
                    found[item["key"]]["parents"].append(callee["key"])
                    found[item["key"]]["sources"].update(item["sources"])
                else:
                    item["parents"] = [callee["key"]]
                    found[item["key"]] = item
        items = sorted(
            found.values(),
            key=lambda i: (-len(i["parents"]), -i["support"], i["name"] or ""),
        )
        if len(items) > MAX_NODES:
            truncated = True
        items = items[:MAX_NODES]
        for item in items:
            item["shared"] = len(item["parents"])
            shown.update(
                f for f in item.get("cells", {}).values() if isinstance(f, str)
            )
            if item["kind"] == "none":
                shown.add(item["key"].split(":", 1)[1])
        out.append(items)
    return {
        "role": "cocallers",
        "of": len(fids),
        "depth": len(out),
        "items": out[0] if out else [],
        "levels": out,
        "callees": counts[0] if counts else 0,
        "counts": counts,
        "hubs": hubs,
        "hub_keys": hub_keys,
        "truncated": truncated,
        "functions_metadata": {f: fmeta[f] for f in shown if f in fmeta},
    }


def get_nway_neighbors():
    """Call-graph neighbors of one N-way row, regrouped into the doc's rows.

    `fids` are the row's functions; each neighbor is mapped to the row that
    holds it, so a callee present in 5 of 6 files reads as one conserved edge.
    `role=cocallers` is the other direction round a callee: the other functions
    that call what the focus calls (see `_cocallers`).
    `depth` > 1 keeps walking from each neighbor row (in `scope` only, so library
    code is not expanded): `levels[d]` are the rows first reached at distance d+1,
    each listing its `parents` (keys of the previous level, `focus` for the first).
    A row already placed is not reached again, so cycles and shared helpers show
    up once, at their shortest distance.
    """
    role = request.args.get("role", "callees")
    if role not in ("callers", "callees", "cocallers"):
        abort(400, "role must be callers, callees or cocallers")
    fids = [t for t in (request.args.get("fids") or "").split(",") if t]
    if not fids:
        abort(400, "fids is required")
    try:
        depth = int(request.args.get("depth", 1))
    except ValueError:
        abort(400, "depth must be an integer")
    depth = max(1, min(NEIGHBOR_MAX_DEPTH, depth))
    scope = request.args.get("scope", "all")
    if scope not in ("code", "library", "all"):
        abort(400, "scope must be code, library or all")
    try:
        doc, r, _ = _load_doc()
    except _Reply as e:
        return e.args[0]
    fmeta = doc["fmeta"]
    row_of = {f: i for i, row in enumerate(doc["rows"]) for f in _row_fids(row)}

    if role == "cocallers":
        return _cocallers(r, doc, fids, row_of, scope, depth)

    levels, truncated, visited, frontier = _walk(
        r, doc, fids, role, row_of, scope, depth
    )

    # Every expandable row says how many neighbors are not on screen: past the
    # depth limit, or dropped by DEEP_CAP / MAX_NODES. `other` counts the
    # opposite direction (what a caller also calls, what a callee is also called by).
    focus_key = f"row:{row_of[fids[0]]}" if fids[0] in row_of else None
    opposite = "callees" if role == "callers" else "callers"
    # `back`: neighbors already placed in an earlier hop (cycles, shared helpers),
    # which the walk never draws as a child.
    lvl = {i["key"]: n for n, level in enumerate(levels) for i in level}
    for n, level in enumerate(levels):
        for parent in level:
            if parent["kind"] == "ext":
                continue
            if parent["kind"] == "row":
                src = [f for f in parent["cells"].values() if isinstance(f, str)]
                expands = scope == "all" or (scope == "library") == parent["library"]
            else:
                # a function outside every row is never expanded: all its edges are hidden
                src, expands = [parent["key"].split(":", 1)[1]], True
            if expands:
                nbrs = _group_neighbors(r, doc, src, role, row_of)
                parent["beyond"] = sum(1 for i in nbrs if i["key"] not in visited)
                parent["back"] = [i["key"] for i in nbrs if lvl.get(i["key"], n) < n]
            parent["other"] = sum(
                1
                for i in _group_neighbors(r, doc, src, opposite, row_of)
                if i["key"] != focus_key
            )

    shown = set(fids)
    for level in levels:
        for item in level:
            shown.update(
                f for f in item.get("cells", {}).values() if isinstance(f, str)
            )
    return {
        "role": role,
        "of": len(fids),
        "depth": depth,
        "items": levels[0],
        "levels": levels,
        "truncated": truncated,
        "functions_metadata": {f: fmeta[f] for f in shown if f in fmeta},
    }


def get_nway_facets():
    """Inline function diff of one N-way row: facets, similarity matrix, consensus code.

    Takes the member params of `/nway` plus `fids`, the row's function ids (the
    `functions_metadata` keys of a row's cells). Over `nway.code_max_functions`
    functions the consensus code is dropped, over `nway.facet_max_functions` the
    source-derived facets go too (see `level` in the answer).

    `scope=functions` takes no file set: `fids` alone (any collections) are scored
    pairwise from their feature vectors, and the reference is the medoid.
    """
    fids = list(
        dict.fromkeys(t for t in (request.args.get("fids") or "").split(",") if t)
    )
    if len(fids) < 2:
        abort(400, "fids needs at least two function ids")
    if len(fids) > nway_diff_service.caps()[0]:
        abort(
            400,
            f"too many functions: at most {nway_diff_service.caps()[0]} per N-way diff",
        )
    scope = request.args.get("scope", "files")
    if scope not in ("files", "functions"):
        abort(400, "scope must be files or functions")
    if scope == "functions":
        r = get_redis()
        try:
            doc = nway_diff_service.compute_functions(
                r,
                fids,
                {
                    "algo": request.args.get("algo"),
                    "min_score": request.args.get("min_score", type=float),
                    "min_features": request.args.get("min_features", type=int),
                },
            )
        except nway_diff_service.BadParams as e:
            abort(400, str(e))
        except nway_diff_service.MemberMissing as e:
            return {"error": "unknown_functions", "missing": e.missing}, 404
    else:
        try:
            doc, r, _ = _load_doc()
        except _Reply as e:
            return e.args[0]
    unknown = [f for f in fids if f not in doc["fmeta"]]
    if unknown:
        return {"error": "unknown_functions", "missing": unknown}, 404
    mode = request.args.get("blocks", "hybrid")
    if mode not in nway_facets_service.BLOCK_MODES:
        abort(400, "blocks must be hybrid, cfg or text")
    return nway_facets_service.compute(r, doc, fids, mode)


def get_nway_block_titles():
    """LLM titles for the reference blocks of a row's code tab (same params as `/nway/facets`).

    Never waits for the model: `status` is off (no LLM configured), pending
    (poll again), ready or failed; `titles` maps block id to title.
    """
    out = get_nway_facets()
    if isinstance(out, tuple):
        return out
    if not out.get("consensus"):
        return {"status": "off", "titles": {}}
    return nway_facets_service.block_titles(out["consensus"])


def get_nway_block_explain():
    """LLM explanation of one block's change in one function (`/nway/facets` params plus `block` and `fn`).

    `fn` is the function's index in `fids`. Never waits for the model: `status`
    is off, pending (poll again), ready (with `verdict`, `summary`) or failed.
    """
    out = get_nway_facets()
    if isinstance(out, tuple):
        return out
    j = request.args.get("fn", type=int)
    if not out.get("consensus") or j is None:
        return {"status": "off"}
    fs = out["functions"]
    return nway_facets_service.explain_block(
        out["consensus"],
        request.args.get("block", ""),
        j,
        [fs[out["reference"]]["id"], fs[j]["id"]] if 0 <= j < len(fs) else None,
    )


def get_nway_summaries():
    """One-line LLM summaries (from LLM-owned function notes) for `fids`; no model call."""
    fids = list(
        dict.fromkeys(t for t in (request.args.get("fids") or "").split(",") if t)
    )
    if len(fids) > nway_diff_service.caps()[0]:
        abort(400, "too many fids")
    return {"summaries": nway_facets_service.function_summaries(fids)}


SET_TTL = 24 * 3600
MAX_SET = 20000


def _ref_members(r, ns, cid):
    """Function ids of a stored cluster, by cluster id or by cluster uuid."""
    from bsimvis.app.services.cluster_utils import resolve_cluster_id_by_uuid
    from bsimvis.app.services.collection_config import resolve_collection_algo

    algo = resolve_collection_algo(ns)
    members = r.smembers(f"{ns}:cluster:{algo}:{cid}:members")
    if not members:
        real = resolve_cluster_id_by_uuid(r, ns, algo, cid)
        if real:
            members = r.smembers(f"{ns}:cluster:{algo}:{real}:members")
    return [
        (m.decode() if isinstance(m, bytes) else str(m)).removesuffix(":meta")
        for m in members
    ]


def post_function_set():
    """Keep a long function list under a short id (Redis, one day): the URL carries `fx_set`."""
    fids = list(
        dict.fromkeys(
            f for f in ((request.json or {}).get("fids") or []) if isinstance(f, str)
        )
    )
    if not fids or len(fids) > MAX_SET or any(":func:" not in f for f in fids):
        abort(400, f"fids: 1 to {MAX_SET} function ids")
    sid = hashlib.sha1(",".join(sorted(fids)).encode()).hexdigest()[:12]
    get_queue_redis().setex(f"nway:fnset:{sid}", SET_TTL, ",".join(fids))
    return {"id": sid, "ttl": SET_TTL}


def get_function_meta():
    """Hero data of a function set: name, prototype, counts, cohesion, links.

    The set is `ref` (`cluster:{coll}:{id}`, `pool:{id}:{cluster}`, `nway:{doc}:{row}`),
    `fids` or `set` (see `post_function_set`); a stored-cluster ref names its own members.
    """
    ref_arg = request.args.get("ref") or ""
    kind, _, rest = ref_arg.partition(":")
    ref = {"kind": kind or None}
    r = get_redis()
    fids = [t for t in (request.args.get("fids") or "").split(",") if t]
    if request.args.get("set"):
        raw = get_queue_redis().get(f"nway:fnset:{request.args['set']}")
        if not raw:
            return {"error": "set_expired"}, 404
        fids += (raw.decode() if isinstance(raw, bytes) else raw).split(",")
    if kind in ("cluster", "pool"):
        if kind == "cluster":
            ns, _, cid = rest.rpartition(":")
        else:
            pool, _, cid = rest.rpartition(":")
            ns, ref["pool"] = f"global:pool:{pool}", pool
        if not ns or not cid:
            abort(400, "ref: cluster:<collection>:<id> or pool:<id>:<cluster>")
        fids = _ref_members(r, ns, cid)
        if not fids:
            return {"error": "unknown_cluster"}, 404
        ref["cluster"] = cid.lower()  # a cluster id or its uuid
    elif kind == "nway":
        _, _, row = rest.rpartition(":")
        ref["row"] = row
    elif kind:
        abort(400, "ref must start with cluster:, pool: or nway:")
    try:
        out = nway_diff_service.function_meta(
            r, fids, ref, request.args.get("sample", type=int)
        )
    except nway_diff_service.BadParams as e:
        abort(400, str(e))
    except nway_diff_service.MemberMissing as e:
        return {"error": "unknown_functions", "missing": e.missing}, 404
    out["ref"] = ref_arg
    return out
