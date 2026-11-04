import hashlib
import json
import re
from collections import Counter, defaultdict

from flask import current_app, request
from flask_restx import abort

from bsimvis.app.routes._list_query import fnum, in_bounds
from bsimvis.app.routes.bin_sim import _fids_tags, _fn_haystack
from bsimvis.app.services import (
    nway_column_service,
    nway_diff_service,
    nway_facets_service,
    nway_group_service,
    nway_indicator_service,
    nway_lineage_service,
    nway_region_service,
    nway_role_service,
)
from bsimvis.app.services.nway_diff_service import _DEFAULT_NAME
from bsimvis.app.services.bin_sim_service import _origin_coll
from bsimvis.app.services.bin_sim_tags import merge_tag_fields, tag_axis
from bsimvis.app.services.redis_client import get_queue_redis, get_redis
from bsimvis.app.services.tag_taxonomy import tag_body, tag_in_scope, tag_prefixes

# A stored cluster only names a row when its members agree this much.
CLUSTER_NAME_MIN_COHESION = 0.8
TABS = ("all", "core", "partial", "unique")
_MD5 = re.compile(r"^[0-9a-f]{32}$")
_PROGRESS_ID = re.compile(r"^[\w-]{8,40}$")


def _members(collection, pool_id, tokens=None):
    """`md5s=` tokens (`coll:md5`, or bare md5 with `collection=`) -> [(coll, md5)]."""
    tokens = tokens or [t.strip() for t in (request.args.get("md5s") or "").split(",")]
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


def _cluster_node(r, pool_id, collection, uuid, open_ids=(), restrict=None):
    """A binary cluster node's files and child groups, read-only.

    Mirrors the namespace rules of routes/bin_cluster.py (file nodes only).
    Returns `(scope, members, groups, children)`: `groups` is one entry per
    child cluster plus a `direct` pseudo-group for files in no child, `children`
    is the same child list shaped for the 413 body. A group named in `open_ids`
    is replaced by its own child clusters (and direct files), recursively, and a
    leaf by one column per file; see `nway_diff_service.grow_columns`.
    `restrict` ({`coll:md5`}) keeps only those files of the node, so a file set
    can be grouped by the clusters it spans.
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
        if (
            config_service.get("clustering.bin_engine", "hierarchical_snn")
            == "hierarchical_snn"
        ):
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

    n = len(restrict) if restrict else r.scard(f"{base}:{cid}:members")
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
    if restrict:
        sets[0] &= restrict
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


def _tag_axis_counts(fmeta):
    """Distinct function tags available to this N-way comparison, by axis."""
    tags = {}
    for meta in fmeta.values():
        for tag in merge_tag_fields(meta):
            tags.setdefault(tag_axis(tag), set()).add(tag)
    return {axis: len(values) for axis, values in tags.items()}


# ponytail: tag levels per axis, page the facet tree if an axis outgrows it
FACET_CAP = 400


def _tag_facets(tagged, passes):
    """{axis: [[level id, parent id, rows]]}, every tag level the rows carry, busiest first.

    An axis is counted over the rows that pass the *other* axes' scope, so a count
    reads as what checking that level adds.
    """
    count = {}
    for tags, ok in zip(tagged, passes):
        levels = {}
        for t in tags:
            chain = tag_prefixes(t) + [tag_body(t)[0]]
            for i, c in enumerate(chain):
                levels[c] = (tag_axis(t), chain[i - 1] if i else "")
        for c, (axis, parent) in levels.items():
            if all(v for a, v in ok.items() if a != axis):
                count.setdefault(axis, {}).setdefault(c, [parent, 0])[1] += 1
    # a parent holds at least its child's rows, so the cap never orphans a kept level
    return {
        axis: [
            [c, p, n]
            for c, (p, n) in sorted(
                levels.items(), key=lambda x: (-x[1][1], len(x[0]))
            )[:FACET_CAP]
        ]
        for axis, levels in count.items()
    }


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
        # a tag-carried name is only a fallback: a real cluster name beats it
        if _DEFAULT_NAME.match(row["name"]) or row.get("name_from") in (
            "tag",
            "string",
        ):
            for u in uuids:
                c = cards[u]
                n = c.get("cluster_name") or ""
                if (
                    (c.get("cohesion_score") or 0) >= CLUSTER_NAME_MIN_COHESION
                    and not _DEFAULT_NAME.match(n)
                    and not re.fullmatch(r"Cluster \d+", n)
                ):
                    row["name"], row["name_from"] = n, "cluster"
                    row.pop("variant", None)  # named by its cluster now
                    break
    return cards


STAT_SORTS = {"cover": "cover", "leak": "leak_rate", "score": "score", "reach": "reach"}


def _lane_ctx(doc, args):
    """The family / contrast file sets of `family=` / `ignore=` (column ids), or None."""
    family = nway_indicator_service.ids(args.get("family"))
    if not family:
        return None
    try:
        return nway_indicator_service.context(
            doc, family, nway_indicator_service.ids(args.get("ignore"))
        )
    except nway_diff_service.BadParams as e:
        abort(400, str(e))


def _lane_info(ctx):
    return ctx and {k: sorted(v) for k, v in ctx.items()}


def _stat_ok(st, args):
    """`cover_min` (0-1) and `leak_max` (share of contrast files, 0-1) filters."""
    cmin, lmax = fnum("cover_min", args), fnum("leak_max", args)
    return in_bounds(st["cover"], cmin, None) and in_bounds(st["leak_rate"], None, lmax)


def _tab_args(doc, args):
    """The tab params of a request, checked: focus, twin, n, tab, k, side, scope."""
    cols = doc["columns"]
    # two top-level `by` groups compare like Focus / Rest (side a = first group)
    focus = doc["columns_mode"] == "focus" or (
        doc["columns_mode"] == "by"
        and len(cols) == 2
        and not any(c.get("path") for c in cols)
    )
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
    side = {"a": "focus", "b": "rest"}.get(side, side)
    if side not in ("", "focus", "rest"):
        abort(400, "side must be focus, rest, a or b")
    scope = args.get("scope", "code")
    if scope not in ("code", "library", "all"):
        abort(400, "scope must be code, library or all")
    return {
        "focus": focus,
        "twin": twin,
        "n": n,
        "tab": tab,
        "k": k,
        "side": side,
        "scope": scope,
    }


def _tab_counts(rows, ta):
    """{tab: {code, library}}: the rows of each tab, before any filter."""
    k, n, focus, twin = ta["k"], ta["n"], ta["focus"], ta["twin"]
    return {
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


def _filter_rows(doc, args, rows, ctx, ta, r=None, pool_id=None):
    """The /nway row filter (tab, side, scope, column, bounds, family, q, tags).

    -> (kept rows, cluster cards when `q` attached them else None, tag facets or None).
    """
    fmeta = doc["fmeta"]
    tab, k, n = ta["tab"], ta["k"], ta["n"]
    focus, twin, side, scope = ta["focus"], ta["twin"], ta["side"], ta["scope"]
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
        if ctx and not _stat_ok(row["stats"], args):
            return False
        if sup_min is not None and row["support"] < sup_min:
            return False
        return True

    filtered = [r for r in rows if keep(r)]
    cards = None
    if q:
        # Full-text over the whole row: name, cluster names, and every function's text/tags.
        cards = _attach_clusters(r, filtered, fmeta, pool_id, args)
        # a function also matches when it references a string containing the text
        by_string = nway_indicator_service.fids_with_string(r, doc, q) if r else set()

        def hit(row):
            var = row.get("variant")
            # the chip is searchable too: "name #2"
            text = [
                row["name"],
                f"{row['name']} #{var['n']}" if var else "",
            ] + [(cards[u].get("cluster_name") or "") for u in row["clusters"]]
            text += [_fn_haystack(f, fmeta) for f in _row_fids(row)]
            return q in " ".join(text).lower() or any(
                f in by_string for f in _row_fids(row)
            )

        filtered = [row for row in filtered if hit(row)]

    # tags: OR inside one axis, AND across axes; facets are counted before the cut
    facets = None
    if tag_scope or args.get("facets") == "1":
        by_axis = {}
        for p in tag_scope:
            by_axis.setdefault(tag_axis(p), []).append(p)
        tagged = [_fids_tags(list(_row_fids(row)), fmeta) for row in filtered]
        passes = [
            {
                a: any(tag_in_scope(t, p) for t in tags for p in ps)
                for a, ps in by_axis.items()
            }
            for tags in tagged
        ]
        if args.get("facets") == "1":
            facets = _tag_facets(tagged, passes)
        filtered = [row for row, ok in zip(filtered, passes) if all(ok.values())]
    return filtered, cards, facets


def _page(doc, args, r=None, pool_id=None):
    """Tab counts, then filter + sort + slice one tab of the rows."""
    nway_role_service.attach(r, doc, nway_group_service._fids)
    if "region" in (args.get("group") or "").split(","):
        nway_region_service.attach(doc, nway_group_service._fids)
    if args.get("order") == "lineage":
        doc = _lineage_order(doc, args)
    rows, fmeta = doc["rows"], doc["fmeta"]
    ctx = _lane_ctx(doc, args)
    if ctx:
        rows = nway_indicator_service.annotate(rows, ctx)
    ta = _tab_args(doc, args)
    tab, k, scope = ta["tab"], ta["k"], ta["scope"]
    counts = _tab_counts(rows, ta)
    filtered, cards, facets = _filter_rows(doc, args, rows, ctx, ta, r, pool_id)
    page_clusters = cards is None

    sort_col = args.get("sort_col")
    if ctx and sort_col in STAT_SORTS:
        filtered = sorted(
            filtered,
            key=lambda r: r["stats"][STAT_SORTS[sort_col]] or 0,
            reverse=args.get("sort_dir", "desc") != "asc",
        )
    elif sort_col in ("span", "weight", "support", "cohesion", "name"):
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
        # a sub-lane that left its parent's band names the parent, which may be closed
        needed |= {groups[gid].get("under_id") for gid, _, _ in part if gid in groups}
        shown_groups = {g: m for g, m in groups.items() if g in needed}
        total = len(entries)
    else:
        page = filtered[offset : offset + limit] if limit > 0 else filtered[offset:]
    if "1" in (args.get("strings"), args.get("calls"), args.get("consts")):
        page = [dict(p) for p in page]
        shown = [p for p in page if not p.get("header")]
        if args.get("strings") == "1":
            for p, st in zip(shown, nway_indicator_service.inline_strings(r, shown)):
                p["strings"] = st
        if args.get("calls") == "1":
            for p, st in zip(shown, nway_indicator_service.inline_links(r, doc, shown)):
                p["calls"] = st
        if args.get("consts") == "1":
            for p, st in zip(shown, nway_indicator_service.inline_consts(r, shown)):
                p["consts"] = st
    real = [row for row in page if not row.get("header")]
    page_fids = {f for row in real for f in _row_fids(row)}
    # a merged group draws its signature from its fids, even while collapsed
    page_fids |= {
        f
        for g in shown_groups.values()
        if g.get("kind") == "same"
        for f in g.get("fids", [])
    }
    if page_clusters:
        cards = _attach_clusters(r, real, fmeta, pool_id, args)
    # a search must not change a row's name: string names go on whatever rows are shown
    nway_indicator_service.name_by_strings(r, real)
    if "tag_axis_counts" not in doc:
        doc["tag_axis_counts"] = _tag_axis_counts(fmeta)
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
        "tag_axis_counts": doc["tag_axis_counts"],
        "tag_facets": facets,
        "columns": doc["columns"],
        "columns_mode": doc["columns_mode"],
        "file_columns": doc["file_columns"],
        "functions_metadata": {f: fmeta[f] for f in page_fids if f in fmeta},
        "fallback_pairs": doc["fallback_pairs"],
        "pair_stats": doc.get("pair_stats"),
        "min_edge": doc["min_edge"],
        "mode": doc["mode"],
        "algo": doc["algo"],
        "warnings": doc["warnings"],
        "family": _lane_info(ctx),
        **(
            {"order": "lineage", "column_parents": doc["column_parents"]}
            if "column_parents" in doc
            else {}
        ),
    }


def post_nway_role():
    """Set the analyst's triage role of functions (`role:<role>` user tag); an empty role clears it."""
    from bsimvis.app.services.tag_service import tag_service

    data = request.json or {}
    fids = [
        f for f in data.get("func_ids") or [] if isinstance(f, str) and ":func:" in f
    ]
    role = data.get("role") or ""
    if not fids:
        return {"error": "func_ids is required"}, 400
    if role and role not in nway_role_service.RANK:
        return {
            "error": f"role must be one of {nway_role_service.ROLE_IDS} or empty"
        }, 400
    r = get_redis()
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{f}:meta")
    drop = defaultdict(list)  # (collection, old tag) -> fids carrying it
    for f, raw in zip(fids, pipe.execute()):
        meta = json.loads(raw) if raw else {}
        meta = meta[0] if isinstance(meta, list) and meta else meta
        for t in merge_tag_fields(meta):
            if (
                t.startswith(nway_role_service.TAG)
                and t != nway_role_service.TAG + role
            ):
                drop[(f.partition(":func:")[0], t)].append(f)
    for (coll, tag), ids in drop.items():
        tag_service.bulk_remove_user_tag(coll, "function", ids, tag)
    if role:
        by_coll = defaultdict(list)
        for f in fids:
            by_coll[f.partition(":func:")[0]].append(f)
        for coll, ids in by_coll.items():
            if not tag_service.bulk_add_user_tag(
                coll, "function", ids, nway_role_service.TAG + role
            ):
                return {"error": "Could not tag the functions"}, 500
    nway_diff_service.invalidate()
    return {"status": "success", "role": role or None, "count": len(fids)}


class _Cancelled(Exception):
    pass


class _Reply(Exception):
    """An early (body, status) answer from _load_doc."""


COLSET_TTL = 3600
COLSET_MAX_GROUPS = 8


def _load_colset():
    raw = get_queue_redis().get(f"nway:colset:{request.args['col_set']}")
    if not raw:
        raise _Reply(({"error": "col_set_expired"}, 404))
    colset = json.loads(raw)
    colset["members"] = [tuple(m) for m in colset["members"]]
    return colset


def _col_pick(args):
    try:
        pick = json.loads(args.get("col_pick") or "[]")
    except ValueError:
        pick = None
    if not isinstance(pick, list) or not all(isinstance(v, str) for v in pick):
        abort(400, "col_pick must be a JSON list of values")
    return pick


def _by_groups(r, members, colset, args, open_ids):
    """Metadata or saved-query column groups, with `col_open` groups opened to files."""
    names = nway_diff_service._file_names(r, members)
    try:
        if colset:
            groups = nway_column_service.query_groups(colset, names)
        else:
            groups = nway_column_service.meta_groups(
                members,
                nway_column_service.read_metas(r, members),
                names,
                args["col_by"],
                args.get("col_by_multi", "first"),
                _col_pick(args),
                args.get("col_rest") == "drop",
            )
    except nway_diff_service.BadParams as e:
        abort(400, str(e))
    # unpicked files left out of every group leave the set
    kept = {m for g in groups for m in g["members"]}
    members = [m for m in members if nway_diff_service._col_id(*m) in kept]
    groups = nway_diff_service.grow_columns(
        groups, set(open_ids), lambda g: nway_diff_service.file_groups(g, names)
    )
    return groups, members


def _group_queries(spec):
    """A group's searches: `queries` (a list) or the single `query`, as dicts of search_files params."""
    qs = spec.get("queries") if "queries" in spec else [spec.get("query")]
    if not isinstance(qs, list) or not all(isinstance(q, dict) for q in qs):
        return None
    return qs


PREVIEW_SAMPLE = 200


def _preview_sample(groups, meta):
    """The files the groups matched, for the Compare panel's live list: one row per
    file with the indexes of the groups holding it. Capped; `shown` < `files` means cut.
    """
    held = {}
    for gi, g in enumerate(groups):
        for m in g["members"]:
            held.setdefault(m, []).append(gi)
    rows = []
    for m, gs in list(held.items())[:PREVIEW_SAMPLE]:
        f = meta.get(m) or {}
        rows.append(
            {
                "id": m,
                "name": f.get("file_name") or f.get("name") or m.rsplit(":", 1)[-1],
                "language": f.get("language_id") or "",
                "batch": f.get("batch_uuid") or "",
                "tags": list(f.get("tags") or [])[:6],
                "groups": gs,
            }
        )
    return rows


# file index fields the Compare filters can suggest values for
VALUE_FIELDS = {
    "tag": ("tags", "user_tags"),
    "file_name": ("file_name",),
    "batch_uuid": ("batch_uuid",),
    "language_id": ("language_id",),
    "filetype": ("filetype",),
}


def get_filter_values():
    """Type-ahead for the Compare filters: `{field, values: [{value, files}]}` from the
    file index registries of one collection, most used first."""
    collection = request.args.get("collection")
    fields = VALUE_FIELDS.get(request.args.get("field", ""))
    if not collection or not fields:
        abort(400, "collection and field (%s) required" % ", ".join(VALUE_FIELDS))
    q = request.args.get("q", "").strip().lower()
    limit = min(int(request.args.get("limit", 12)), 50)
    r = get_redis()
    buckets = {}
    for field in fields:
        prefix = f"{collection}:idx:file:{field}:"
        for b in r.sscan_iter(
            f"{collection}:reg:file:{field}", match=f"*{q}*" if q else "*", count=1000
        ):
            b = b.decode() if isinstance(b, bytes) else b
            if b.startswith(prefix) and q in b[len(prefix) :].lower():
                buckets[b[len(prefix) :]] = b
            if len(buckets) >= 400:
                break
    pipe = r.pipeline(transaction=False)
    for b in buckets.values():
        pipe.scard(b)
    rows = sorted(zip(buckets, pipe.execute()), key=lambda x: (-x[1], x[0]))
    return {
        "field": request.args["field"],
        "values": [{"value": v, "files": n} for v, n in rows[:limit]],
    }


def _cluster_files(scope, uuid, axis):
    """The files of one binary cluster node, read from its member set (the same
    namespace `_cluster_node` uses), so a parent cluster resolves as well as a leaf."""
    from bsimvis.app.services.cluster_utils import resolve_cluster_id_by_uuid
    from bsimvis.app.services.collection_config import resolve_collection_algo
    from bsimvis.app.services.config_service import config_service

    coll = scope.get("collection")
    if not coll:
        abort(400, "cluster_uuid needs a collection")
    axis = (axis or "overall").strip().lower()
    algo = resolve_collection_algo(coll, None)
    algo = f"{algo}:{axis}" if axis != "overall" else algo
    if (
        config_service.get("clustering.bin_engine", "hierarchical_snn")
        == "hierarchical_snn"
    ):
        algo = f"{algo}:snn"
    r = get_redis()
    cid = resolve_cluster_id_by_uuid(
        r, coll, algo, str(uuid).strip().lower(), "bin_cluster"
    )
    if cid is None:
        abort(404, "unknown cluster_uuid")
    members = r.smembers(f"{coll}:bin_cluster:{algo}:{cid}:members")
    return [
        (
            _origin_coll(coll),
            (m.decode() if isinstance(m, bytes) else m).rsplit(":", 1)[-1],
        )
        for m in members
    ]


def resolve_group(scope, spec, label, cap, meta):
    """A group spec's searches -> [(origin coll, md5)], filling `meta` (id -> search row)."""
    from bsimvis.app.routes.home import _call
    from bsimvis.app.routes.search_file import search_files

    found = []
    # an empty search would list the whole collection: a group needs a filter
    for query in filter(None, _group_queries(spec)):
        if query.get("cluster_uuid"):
            found += _cluster_files(scope, query["cluster_uuid"], query.get("axis"))
            continue
        try:
            res = _call(search_files, **{**scope, **query, "limit": cap + 1})
        except (TypeError, ValueError):
            abort(400, f"{label}: query must be search_files parameters")
        if res.get("error"):
            abort(400, f"{label}: {res['error']}")
        for f in res.get("files") or []:
            key = (
                _origin_coll(f["file_id"].split(":file:")[0]),
                f["file_id"].rsplit(":", 1)[1],
            )
            found.append(key)
            meta[nway_diff_service._col_id(*key)] = f
    return found


def post_column_set():
    """Resolve file queries into column groups: one saved set, read back by `col_set`.

    A group holds one or more searches and is their union. `within` (file ids,
    `<collection>:<md5>`) keeps only matches inside that set. With `preview` nothing
    is saved and empty groups are fine: it only reports the counts.
    """
    body = request.get_json(silent=True) or {}
    specs, preview = body.get("groups"), bool(body.get("preview"))
    low = 1 if preview else 2
    if (
        not isinstance(specs, list)
        or not low <= len(specs) <= COLSET_MAX_GROUPS
        or not all(isinstance(g, dict) and _group_queries(g) is not None for g in specs)
    ):
        abort(400, f"groups: {low} to {COLSET_MAX_GROUPS} of {{label, queries}}")
    scope = {k: body[k] for k in ("collection", "pool") if body.get(k)}
    within = body.get("within")
    if within is not None:
        within = {
            (_origin_coll(c), m.lower())
            for c, m in (str(t).rsplit(":", 1) for t in within if ":" in str(t))
        }
    cap = nway_diff_service.caps()[0]
    groups, members, meta = [], [], {}
    for i, spec in enumerate(specs):
        label = str(spec.get("label") or f"Group {i + 1}")[:60]
        found = list(dict.fromkeys(resolve_group(scope, spec, label, cap, meta)))
        if within is not None:
            found = [m for m in found if m in within]
        if not found and not preview:
            abort(400, f"{label}: the query matches no file")
        if len(found) > cap:
            abort(400, f"{label}: matches more than {cap} files: narrow the query")
        members += found
        groups.append(
            {"label": label, "members": [nway_diff_service._col_id(*m) for m in found]}
        )
    members = list(dict.fromkeys(members))
    seen = Counter(m for g in groups for m in g["members"])
    out = {
        "files": len(members),
        "overlap": sum(1 for n in seen.values() if n > 1),
        "groups": [{"label": g["label"], "files": len(g["members"])} for g in groups],
    }
    if preview:
        out["sample"] = _preview_sample(groups, meta)
        return out
    colset = {**scope, "members": members, "groups": groups}
    raw = json.dumps(colset)
    sid = hashlib.sha1(raw.encode()).hexdigest()[:12]
    get_queue_redis().setex(f"nway:colset:{sid}", COLSET_TTL, raw)
    return {"id": sid, "ttl": COLSET_TTL, **out}


def get_column_axes():
    """Keys `col_by` can split the given files on, with their value counts."""
    # a POST body carries a set too long for a URL
    body = request.get_json(silent=True) or {}
    tokens = [str(t) for t in body.get("md5s") or []]
    members = _members(request.args.get("collection"), request.args.get("pool"), tokens)
    if len(members) < 2:
        abort(400, "need at least two distinct files")
    r = get_redis()
    return {
        "axes": nway_column_service.axes(
            members, nway_column_service.read_metas(r, members)
        )
    }


def _load_doc(build=False, on_progress=None):
    """Parse the shared N-way params and compute the doc: (doc, redis, pool_id).

    `build` lets a large set build (the NWAY_BUILD job); a request for one that
    is not built answers 428 `needs_build`. `on_progress(frac, label)` also gets
    each progress step.
    """
    pool_id = request.args.get("pool")
    collection = request.args.get("collection")
    cluster_uuid = (request.args.get("cluster_uuid") or "").strip().lower()
    columns = request.args.get("columns", "auto")
    colset = _load_colset() if request.args.get("col_set") else None
    col_by = request.args.get("col_by", "")
    if colset or col_by:
        columns = "by"
    if columns not in ("auto", "files", "children", "twins", "by"):
        abort(400, "columns must be auto, files, children, twins or by")
    if columns == "by" and not (colset or col_by):
        abort(400, "columns=by needs col_by or col_set")
    col_presence = request.args.get("col_presence")
    if col_presence and col_presence not in nway_column_service.PRESENCE:
        abort(400, "col_presence must be any, half or all")
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
        sample_above = request.args.get("sample_above", type=int)
        twin_depth = request.args.get("twin_depth", 1, type=int)
        twin_link = float(request.args.get("twin_link", 0.5))
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
                if on_progress:
                    on_progress(frac, label)

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
            if colset:
                members = colset["members"]
                pool_id = pool_id or colset.get("pool")
            else:
                members = _members(collection, pool_id)
            if columns == "children":
                # a file set grouped by the clusters it spans, under their smallest shared one
                sc = not pool_id and _shared_cluster(r, members, request.args)
                if not sc or not sc["cluster_uuid"]:
                    abort(400, "these files share no stored cluster")
                scope, members, node_groups, children = _cluster_node(
                    r,
                    pool_id,
                    sc["collection"],
                    sc["cluster_uuid"],
                    open_ids,
                    {f"{c}:{m}" for c, m in members},
                )
                groups = node_groups if len(node_groups) > 1 else None
            batch_uuid = (request.args.get("batch_uuid") or "").strip()
            if batch_uuid:
                members = list(
                    dict.fromkeys(members + _batch_members(r, collection, batch_uuid))
                )
            if columns != "children":
                scope = _scope(members, pool_id)
        # `solo` lets the function view graph a set that sits in one file.
        if len(members) < (1 if request.args.get("solo") else 2):
            abort(400, "need at least two distinct files")
        if columns == "by":
            groups, members = _by_groups(r, members, colset, request.args, open_ids)
            if col_presence:
                presence = nway_column_service.PRESENCE[col_presence]
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
                "twin_depth": twin_depth,
                "twin_link": twin_link,
                "open": open_ids,
                "focus": focus,
                "focus_rule": focus_rule,
                "child_presence": max(0.0, min(1.0, presence)),
                "pair_k": None if pair_k is None else max(0, pair_k),
                "sample_above": None if sample_above is None else max(1, sample_above),
                "bridges": request.args.get("bridges"),
                "groups_mode": "by" if columns == "by" else "children",
                "build": build,
            },
        )
    except nway_diff_service.NeedsBuild as e:
        raise _Reply(
            (
                {
                    "error": "needs_build",
                    "message": f"{e.members} files: build this N-way as a job",
                    "members": e.members,
                    "key": e.key,
                    "limit": nway_diff_service.auto_build_max(),
                    "job": _build_job(e.key),
                    "functions": e.functions,
                    "pair_k": e.pair_k,
                    "depths": nway_diff_service.build_depths(e.members, e.functions),
                },
                428,
            )
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
    if columns == "children" and not cluster_uuid and doc["columns_mode"] != "children":
        doc["warnings"].append(
            "No sub-cluster splits these files: their smallest shared cluster holds them directly."
        )
    return doc, r, pool_id


def _build_job(key):
    """The live NWAY_BUILD job of a set (`NeedsBuild.key`), or None."""
    from bsimvis.app.routes.bin_sim import job_service
    from bsimvis.app.services.job_service import JobStatus

    jid = get_queue_redis().get(f"nway:build_job:{key}")
    job = job_service.r.hgetall(f"job:{jid}") if jid else {}
    if job.get("status") not in (JobStatus.PENDING.value, JobStatus.RUNNING.value):
        return None
    payload = json.loads(job.get("payload") or "{}")
    return {
        "id": jid,
        "status": job["status"],
        "progress_id": payload.get("args", {}).get("progress_id"),
    }


def post_nway_build():
    """Queue the NWAY_BUILD job of a large set (same params as `/nway`)."""
    from bsimvis.app.routes.bin_sim import job_service
    from bsimvis.app.services.job_service import JobType

    try:
        _load_doc()
        return {"status": "ready"}
    except _Reply as e:
        body, status = e.args[0]
        if body.get("error") != "needs_build":
            return body, status
    if body["job"]:
        return {"status": "queued", **body["job"]}
    args = request.args.to_dict()
    # one progress id per set, so a reload re-attaches to the running build
    args["progress_id"] = hashlib.md5(body["key"].encode()).hexdigest()[:20]
    payload = {
        "args": args,
        "key": body["key"],
        "members": body["members"],
        "priority": "high",
    }
    if args.get("pool"):
        payload["pool_id"] = args["pool"]
    elif args.get("collection"):
        payload["collection"] = args["collection"]
    # ponytail: check-then-set, two clicks in the same instant can queue twice
    jid = job_service.create_job(JobType.NWAY_BUILD, payload, enqueue=False)
    get_queue_redis().setex(
        f"nway:build_job:{body['key']}", nway_diff_service.BUILT_TTL, jid
    )
    get_queue_redis().delete(f"nway:cancel:{args['progress_id']}")
    job_service.enqueue_job(jid)
    return {
        "status": "queued",
        "id": jid,
        "progress_id": args["progress_id"],
    }, 202


def run_build_job(payload, job_service, job_id):
    """Worker side of NWAY_BUILD: compute the doc once, `compute` keeps it in Redis."""
    from flask import Flask

    def progress(frac, label):
        if job_service.is_cancelled(job_id):
            raise _Cancelled()
        job_service.update_progress(job_id, int(frac * 100))

    job_service.add_log(job_id, f"Building the N-way of {payload['members']} files")
    try:
        with Flask(__name__).test_request_context(query_string=payload["args"]):
            _load_doc(build=True, on_progress=progress)
    except _Cancelled:
        job_service.cancel_job(job_id)
        return False
    except _Reply as e:
        body, status = e.args[0]
        raise RuntimeError(f"{status}: {body.get('message') or body.get('error')}")
    finally:
        # the doc now lives in Redis; the worker has no use for its copy
        nway_diff_service.invalidate()
    return True


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
    if (
        config_service.get("clustering.bin_engine", "hierarchical_snn")
        == "hierarchical_snn"
    ):
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


def _strings_page(doc, args, r):
    """Filter, sort and slice the string rows; `reach` is read for the shown page only."""
    ctx = _lane_ctx(doc, args)
    try:
        min_len = max(
            1, int(args.get("min_length", nway_indicator_service.MIN_STRING_LEN))
        )
        offset = max(0, int(args.get("offset", 0)))
        limit = max(1, min(int(args.get("limit", 100)), 1000))
    except ValueError:
        abort(400, "min_length, offset and limit must be integers")
    scope = args.get("scope", "code")
    if scope not in ("code", "library", "all"):
        abort(400, "scope must be code, library or all")
    try:
        rows = nway_indicator_service.string_rows(r, doc, ctx, min_len)
    except nway_diff_service.BadParams as e:
        abort(400, str(e))
    q = (args.get("q") or "").strip().lower()
    only_fam = ctx and args.get("family_only", "1") != "0"
    rows = [
        row
        for row in rows
        if (scope == "all" or row["library"] == (scope == "library"))
        and (not q or q in row["name"].lower())
        and (not only_fam or row["stats"]["tp"])
        and (not ctx or _stat_ok(row["stats"], args))
    ]
    sort_col = args.get("sort_col") or ("score" if ctx else "span")
    rev = args.get("sort_dir", "desc") != "asc"
    if sort_col in STAT_SORTS and ctx:
        key = lambda x: (x["stats"][STAT_SORTS[sort_col]] or 0, x["span"])
    elif sort_col in ("span", "weight"):
        key = lambda x: x[sort_col]
    else:
        key = lambda x: x["name"].lower()
    rows.sort(key=key, reverse=rev)
    groups, note = {}, ""
    if args.get("group"):
        if set(args["group"].split(",")) - {"lane", "prefix"}:
            abort(400, "strings group by lane and/or prefix")
        try:
            entries, groups, note = nway_group_service.group_rows(
                rows,
                {**doc, "key": None},
                r,
                args["group"],
                {**args, "group_merge": ""},
            )
        except nway_diff_service.BadParams as e:
            abort(400, str(e))
        part = entries[offset : offset + limit]
        page = [
            {**row, "g": gid, **extra} if row else {"g": gid, "header": True}
            for gid, row, extra in part
        ]
        needed = {p for gid, _, _ in part if gid in groups for p in groups[gid]["path"]}
        needed |= {groups[gid].get("under_id") for gid, _, _ in part if gid in groups}
        groups = {g: m for g, m in groups.items() if g in needed}
        nway_indicator_service.lane_fids(r, doc, rows, groups)
        total = len(entries)
    else:
        page = rows[offset : offset + limit]
        total = len(rows)
    real = [x for x in page if not x.get("header")]
    meta = {}
    if args.get("refs") == "1":
        meta = nway_indicator_service.string_refs(r, doc, real)
    if ctx:
        nway_indicator_service.page_reach(r, doc, real, ctx)
    return {
        "items": page,
        "total": total,
        "groups": groups,
        "group_note": note,
        "functions_metadata": meta,
        "offset": offset,
        "limit": limit,
        "columns": doc["columns"],
        "columns_mode": doc["columns_mode"],
        "file_columns": doc["file_columns"],
        "family": _lane_info(ctx),
        "warnings": doc["warnings"],
    }


def get_nway_expand():
    """Sub-rows of one row, with presence over the table's columns: `kind=function`
    takes `cells` (JSON `{file id: function id}`) and gives the strings its functions
    reference; `kind=callers` / `callees` take `cells` and give the rows that call it / it calls;
    `kind=consts` takes `cells` and gives its constants; `kind=string` takes `id` and gives
    the function rows that reference it.
    """
    kind = request.args.get("kind", "")
    if kind not in ("function", "string", "callers", "callees", "consts"):
        abort(400, "kind must be function, string, callers, callees or consts")
    try:
        cells = json.loads(request.args.get("cells") or "{}")
    except ValueError:
        cells = None
    if kind != "string" and not (isinstance(cells, dict) and cells):
        abort(400, f"kind={kind} needs cells, a JSON object of file id -> function id")
    if kind == "string" and not request.args.get("id"):
        abort(400, "kind=string needs id")
    try:
        doc, r, _ = _load_doc()
    except _Reply as e:
        return e.args[0]
    if kind == "function":
        return nway_indicator_service.expand_function(r, doc, cells)
    if kind == "consts":
        return nway_indicator_service.expand_consts(r, doc, cells)
    if kind in ("callers", "callees"):
        return nway_indicator_service.expand_links(r, doc, cells, kind)
    return nway_indicator_service.expand_string(r, doc, request.args["id"])


def get_nway_strings():
    """The N-way table of strings: rows are string values, columns the same files or
    groups as /nway, with Cover / Leak / Reach / Score for a `family`."""
    try:
        doc, r, _ = _load_doc()
    except _Reply as e:
        return e.args[0]
    return _strings_page(doc, request.args, r)


def get_nway_indicators():
    """Strings and function blocks ranked by how well they single out the `family`."""
    if not request.args.get("family"):
        abort(400, "family is required")
    kinds = nway_indicator_service.ids(request.args.get("kinds", "string,function"))
    if not set(kinds) <= {"string", "function"}:
        abort(400, "kinds must be string and/or function")
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 500))
    except ValueError:
        abort(400, "limit must be an integer")
    try:
        doc, r, pool_id = _load_doc()
    except _Reply as e:
        return e.args[0]
    ctx = _lane_ctx(doc, request.args)
    out = []
    if "function" in kinds:
        for row in nway_indicator_service.annotate(doc["rows"], ctx):
            if (
                request.args.get("scope", "code") == "all"
                or row["library"] == (request.args.get("scope") == "library")
            ) and row["stats"]["tp"]:
                fids = list(_row_fids(row))
                out.append(
                    {
                        "kind": "function",
                        "id": fids[0] if fids else row["name"],
                        "preview": row["name"],
                        "files": len(fids),
                        "fids": fids,
                        "stats": row["stats"],
                    }
                )
    if "string" in kinds:
        a = request.args.to_dict()
        a.update(limit="1000", sort_col="score", sort_dir="desc")
        for row in _strings_page(doc, a, r)["items"]:
            out.append(
                {
                    "kind": "string",
                    "id": row["id"],
                    "preview": row["name"][:200],
                    "files": row["span"],
                    "stats": row["stats"],
                }
            )
    out = [x for x in out if _stat_ok(x["stats"], request.args)]
    out.sort(key=lambda x: (-x["stats"]["score"], -x["stats"]["tp"]))
    return {"items": out[:limit], "total": len(out), "family": _lane_info(ctx)}


NEIGHBOR_MAX_DEPTH = 5
DEEP_CAP = 8  # neighbors kept per expanded row, from depth 2 on
MAX_NODES = 60  # all levels together


def _group_neighbors(r, doc, fids, role, row_of, skip=()):
    """Callers/callees of `fids` regrouped into the doc's rows.

    Each item is one neighbor row (or an imported symbol, or a function outside
    every row) with the source functions that reach it. `skip`: function ids
    left out (the other members of a lane, which the graph draws itself).
    """
    fmeta = doc["fmeta"]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:{role}")
    groups = {}
    for src, members in zip(fids, pipe.execute()):
        for n in members:
            n = n.decode() if isinstance(n, bytes) else n
            if n in skip:
                continue
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


def _walk(r, doc, fids, role, row_of, scope, depth, skip=()):
    """Rows reached from `fids` hop by hop: (levels, truncated, visited keys, last frontier)."""
    first = _group_neighbors(r, doc, fids, role, row_of, skip)
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
            for item in _group_neighbors(r, doc, src, role, row_of, skip)[:DEEP_CAP]:
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


def _lane_rows(group_id):
    """The function ids of each row in one group of the table (a lane with its sub-lanes), [[fid, ...]].

    Returns `(rows, info)`; `info` is each row's name and file cells, for drawing it.

    Runs the list with the request's own table params, every group open and no
    page limit, so the lane is the one the user sees. Aborts when the group is unknown.
    """
    args = {k: v for k, v in request.args.items() if k not in ("lane", "fids")}
    args.update(group_open="all", group_flip="[]", limit=0, offset=0)
    # `scope` of a neighbors call is the graph's; the lane belongs to the table's
    if "lane_scope" in args:
        args["scope"] = args.pop("lane_scope")
    with current_app.test_request_context(query_string=args):
        out = get_nway()
    if isinstance(out, tuple):
        abort(out[1] if len(out) > 1 else 400, "the table could not be listed")
    groups = out.get("groups") or {}
    if group_id not in groups:
        abort(404, "unknown group")
    rows, info = [], []
    for it in out["items"]:
        g = groups.get(it.get("g"))
        if it.get("header") or not g or group_id not in g["path"]:
            continue
        rows.append([f for f in _row_fids(it) if isinstance(f, str)])
        info.append(
            {k: it.get(k) for k in ("name", "name_from")}
            | {"cells": dict(it.get("files") or it["cells"])}
        )
    return rows, info


def _lane_edges(r, rows):
    """Calls between the members of a lane: [[caller row, callee row, n functions], ...]."""
    member = {f: i for i, row in enumerate(rows) for f in row}
    fids = list(member)
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.smembers(f"{f}:callees")
    count = {}
    for f, callees in zip(fids, pipe.execute()):
        for n in callees:
            n = n.decode() if isinstance(n, bytes) else n
            if n in member and member[n] != member[f]:
                k = (member[f], member[n])
                count[k] = count.get(k, 0) + 1
    return [[a, b, n] for (a, b), n in sorted(count.items())]


def get_nway_lane_fids():
    """Function ids of every row of one lane (`lane` = its group id) of the table described by the same params as `/nway`."""
    if not request.args.get("lane"):
        abort(400, "lane is required")
    rows, _ = _lane_rows(request.args["lane"])
    return {"rows": rows, "fids": [f for row in rows for f in row]}


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
    lane = []
    if not fids and request.args.get("lane"):
        lane, lane_info = _lane_rows(request.args["lane"])
        fids = [f for row in lane for f in row]
    if not fids:
        abort(400, "fids or lane is required")
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

    skip = frozenset(fids) if lane else frozenset()
    levels, truncated, visited, frontier = _walk(
        r, doc, fids, role, row_of, scope, depth, skip
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
                nbrs = _group_neighbors(r, doc, src, role, row_of, skip)
                parent["beyond"] = sum(1 for i in nbrs if i["key"] not in visited)
                parent["back"] = [i["key"] for i in nbrs if lvl.get(i["key"], n) < n]
            parent["other"] = sum(
                1
                for i in _group_neighbors(r, doc, src, opposite, row_of, skip)
                if i["key"] != focus_key
            )

    shown = set(fids)
    for level in levels:
        for item in level:
            shown.update(
                f for f in item.get("cells", {}).values() if isinstance(f, str)
            )
    out = {
        "role": role,
        "of": len(fids),
        "depth": depth,
        "items": levels[0],
        "levels": levels,
        "truncated": truncated,
        "functions_metadata": {f: fmeta[f] for f in shown if f in fmeta},
    }
    if lane and role == "callees":
        out["lane_edges"] = _lane_edges(r, lane)
        out["lane_rows"] = lane_info
    return out


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


def post_nway_lane_names():
    """LLM names for lane groups (JSON body `{"lanes": [...]}`, each as the lane group meta plus `total`).

    Never waits for the model: `status` is off (no LLM configured), pending
    (poll again), ready or failed; `names` maps lane id to name.
    """
    if not nway_facets_service.llm_configured():
        return {"status": "off", "names": {}}
    lanes = (request.get_json(silent=True) or {}).get("lanes")
    if not isinstance(lanes, list) or not all(
        isinstance(l, dict) and {"id", "files", "total", "rows"} <= set(l)
        for l in lanes
    ):
        abort(400, "lanes must be a list of lane groups (id, files, total, rows, ...)")
    return nway_group_service.lane_names(lanes)


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


def _lineage_nodes(doc):
    """(node columns, rows with fid cells, pair edges) of a files / twins doc, else None."""
    if doc["columns_mode"] == "files":
        return doc["columns"], doc["rows"], doc["edges"]
    if doc["columns_mode"] == "twins":
        return nway_lineage_service.group_nodes(
            doc["columns"], doc["file_columns"], doc["rows"], doc["edges"]
        )
    return None


def _node_of(cols, raw):
    """The node id a `root` / `file` param names: a node id, a member file id or an md5."""
    raw = (raw or "").strip().lower()
    if not raw:
        return None
    for c in cols:
        for m in [c["id"]] + (c.get("members") or []):
            m = m.lower()
            if raw == m or m.endswith(":" + raw):
                return c["id"]
    return raw


def _lineage_order(doc, args):
    """`order=lineage`: a copy of a files / twins doc, its columns in lineage DFS order.

    Same tree as /nway/lineage's default (`root` honoured); the copy carries
    `column_parents`. Other column modes come back unchanged.
    """
    nodes = _lineage_nodes(doc)
    if nodes is None or len(doc["columns"]) < 2:
        return doc
    cols, _, edges = nodes
    try:
        w, root, pairs = nway_lineage_service.tree(
            cols, edges, doc["min_edge"], _node_of(cols, args.get("root"))
        )
    except nway_lineage_service.BadParams as e:
        abort(400, str(e))
    masks = Counter(row["mask"] for row in doc["rows"])
    order, parent = nway_lineage_service.dfs(w, root, pairs, masks)
    moved = {m: sum(1 << j for j, o in enumerate(order) if m >> o & 1) for m in masks}
    ids = [c["id"] for c in doc["columns"]]
    return {
        **doc,
        "columns": [doc["columns"][o] for o in order],
        "rows": [{**row, "mask": moved[row["mask"]]} for row in doc["rows"]],
        "column_parents": {
            ids[o]: None if parent[o] is None else ids[parent[o]] for o in order
        },
        # the group cache keys on it: a reordered doc must not share entries
        "key": doc.get("key") and f"{doc['key']}:lineage:{ids[root]}",
    }


# ponytail: past this many function ids `rows.fids` is left out, the client keeps `fid`
LINEAGE_FIDS_MAX = 200_000


def _lineage_rows(doc, args, r, pool_id):
    """Every row of the doc, compact and column-wise, flagged by the /nway row filter.

    -> {rows, regions, counts}; `rows.mask` bit i = node i (the doc's column order).
    """
    nway_role_service.attach(r, doc, nway_group_service._fids)
    nway_region_service.attach(doc, nway_group_service._fids)
    rows = doc["rows"]
    ctx = _lane_ctx(doc, args)
    if ctx:
        rows = nway_indicator_service.annotate(rows, ctx)
    ta = _tab_args(doc, args)
    kept = {id(x) for x in _filter_rows(doc, args, rows, ctx, ta, r, pool_id)[0]}
    fids = [nway_group_service._fids(x) for x in rows]
    out = {
        "n": len(rows),
        "fid": [f[0] if f else None for f in fids],
        "fids": fids,
        "name": [x["name"] for x in rows],
        "mask": [format(x["mask"], "x") for x in rows],
        "role": [x.get("role") for x in rows],
        "region": [x.get("region") for x in rows],
        "library": [bool(x["library"]) for x in rows],
        "weight": [x["weight"] for x in rows],
        "match": [id(x) in kept for x in rows],
    }
    if sum(map(len, fids)) > LINEAGE_FIDS_MAX:
        del out["fids"]
    special = {k: label for k, label, _ in nway_region_service.SPECIAL}
    size = Counter(x["region"] for x in rows if x.get("region"))
    keys = sorted((k for k in size if k not in special), key=lambda k: (-size[k], k))
    keys += [k for k in special if k in size]  # region_groups' order
    return {
        "rows": out,
        "regions": {
            k: {"label": special.get(k, ""), "kind": k if k in special else ""}
            for k in keys
        },
        "counts": _tab_counts(rows, ta),
    }


def get_nway_lineage():
    """Lineage tree of the files (or twin columns) of a set (same params as `/nway`, plus `root`, `order`).

    Read-only: it annotates the rows and pair edges `/nway` already computes.
    The tree is built from every row; `rows.match` marks the rows the /nway
    filter params keep.
    """
    order = request.args.get("order", "tree")
    if order == "lineage":  # the table's order param, carried over by a pivot
        order = "tree"
    try:
        doc, r, pool_id = _load_doc()
    except _Reply as e:
        return e.args[0]
    nodes = _lineage_nodes(doc)
    if nodes is None:
        abort(400, "lineage needs file or twin columns: use columns=files or twins")
    cols, rows, edges = nodes
    fmeta = doc["fmeta"]

    def name_of(fid):
        n = (fmeta.get(fid) or {}).get("name")
        return n if n and not _DEFAULT_NAME.match(n) else None

    try:
        out = nway_lineage_service.build(
            cols,
            rows,
            edges,
            doc["min_edge"],
            name_of,
            _node_of(cols, request.args.get("root")),
            order,
            _node_of(cols, request.args.get("file")),
        )
    except nway_lineage_service.BadParams as e:
        abort(400, str(e))
    out.update(_lineage_rows(doc, request.args, r, pool_id))
    out["columns_mode"] = doc["columns_mode"]
    out["min_edge"] = doc["min_edge"]
    out["warnings"] = doc["warnings"]
    return out
