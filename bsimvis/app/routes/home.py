"""Instance-wide homepage: counts, job health, insights, and unified search.

Everything here fans out over the existing per-entity search routes rather than
maintaining a second index. `_call` invokes a route function inside a synthetic
request context so the existing `request.args` parsing is reused verbatim.
"""

import json
import logging
import time
from urllib.parse import quote

from flask import current_app, request

from bsimvis.app.services.cluster_utils import _AXES as BIN_AXES, name_fields
from bsimvis.app.services.job_service import JobService
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_service import tag_service

job_service = JobService()

# ponytail: process-local dict cache, no eviction (a handful of keys).
# Swap for redis if the API ever runs multi-process and staleness matters.
_CACHE = {}
INSIGHTS_TTL = 120


def _cached(key, ttl, fn):
    hit = _CACHE.get(key)
    now = time.time()
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _CACHE[key] = (now, val)
    return val


def _call(fn, **args):
    """Calls a route function with a synthetic query string."""
    with current_app.test_request_context(query_string=args):
        res = fn()
    # Route functions return either a dict or a (dict, status) tuple.
    if isinstance(res, tuple):
        res = res[0]
    return res if isinstance(res, dict) else {}


def _collections():
    from bsimvis.app.routes.search_collection import search_collections

    return _call(search_collections, limit=100000).get("collections", [])


def get_home_stats():
    """Cheap instance-wide counters plus job queue health."""
    from bsimvis.app.routes.pools import list_pools

    cols = _collections()
    pools = _call(list_pools, limit=100000)
    pool_items = pools.get("items") or pools.get("pools") or []

    jobs = job_service.get_global_stats()
    recent_jobs, _ = job_service.list_jobs(limit=8, offset=0)

    return {
        "totals": {
            "collections": len(cols),
            "pools": len(pool_items),
            "files": sum(c.get("total_files", 0) for c in cols),
            "functions": sum(c.get("total_functions", 0) for c in cols),
            "batches": sum(c.get("total_batches", 0) for c in cols),
        },
        "jobs": jobs,
        "recent_jobs": recent_jobs,
        "recent_collections": sorted(
            cols, key=lambda c: c.get("last_updated", 0), reverse=True
        )[:5],
        "pools": pool_items[:5],
    }


def _top_tags(cols, limit=20):
    """Aggregates file-level tag cardinality across every collection."""
    r = get_redis()
    counts = {}
    for c in cols:
        name = c["name"]
        tags = list(tag_service.get_collection_tags(name).keys())
        if not tags:
            continue
        pipe = r.pipeline(transaction=False)
        for t in tags:
            pipe.scard(f"{name}:idx:file:tags:{t.lower()}")
            pipe.scard(f"{name}:idx:file:user_tags:{t.lower()}")
        res = pipe.execute()
        for i, t in enumerate(tags):
            n = (res[2 * i] or 0) + (res[2 * i + 1] or 0)
            if n:
                counts[t] = counts.get(t, 0) + n

    top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    ns = {}
    for tag, n in counts.items():
        prefix = (
            tag.split(":", 1)[0]
            if ":" in tag
            else (tag.split("/", 1)[0] if "/" in tag else "other")
        )
        ns[prefix] = ns.get(prefix, 0) + n
    return {
        "top": [{"tag": t, "count": n} for t, n in top],
        "namespaces": [
            {"namespace": k, "count": v}
            for k, v in sorted(ns.items(), key=lambda kv: kv[1], reverse=True)[:10]
        ],
    }


def _biggest_clusters(cols, limit=10):
    from bsimvis.app.routes.bin_cluster import list_bin_clusters

    out = []
    for c in cols:
        if not c.get("total_files"):
            continue
        res = _call(
            list_bin_clusters,
            collection=c["name"],
            limit=limit,
            sort_by="count",
            sort_order="desc",
        )
        for cl in res.get("results", [])[:limit]:
            out.append(
                {
                    "collection": c["name"],
                    "cluster_id": cl.get("cluster_id"),
                    "cluster_uuid": cl.get("cluster_uuid"),
                    "cluster_name": cl.get("cluster_name"),
                    **name_fields(cl),
                    "count": cl.get("count", 0),
                    "cohesion": cl.get("cohesion_score"),
                }
            )
    return sorted(out, key=lambda x: x["count"], reverse=True)[:limit]


def _recent_batches(cols, limit=10):
    from bsimvis.app.routes.search_collection import search_batches

    out = []
    for c in sorted(cols, key=lambda c: c.get("last_updated", 0), reverse=True)[:10]:
        res = _call(search_batches, collection=c["name"], limit=limit)
        for b in res.get("batches", res.get("items", [])):
            b = dict(b)
            b["collection"] = c["name"]
            out.append(b)
    out.sort(key=lambda b: b.get("last_updated") or 0, reverse=True)
    return out[:limit]


def get_home_insights():
    """Heavier panels: top tags, biggest clusters, recent batches. Cached."""

    def build():
        cols = _collections()
        try:
            tags = _top_tags(cols)
        except Exception as e:
            logging.warning(f"home insights: top tags failed: {e}")
            tags = {"top": [], "namespaces": []}
        try:
            clusters = _biggest_clusters(cols)
        except Exception as e:
            logging.warning(f"home insights: clusters failed: {e}")
            clusters = []
        try:
            batches = _recent_batches(cols)
        except Exception as e:
            logging.warning(f"home insights: batches failed: {e}")
            batches = []
        return {
            "tags": tags,
            "biggest_clusters": clusters,
            "recent_batches": batches,
            "generated_at": int(time.time() * 1000),
        }

    if request.args.get("refresh") == "true":
        _CACHE.pop("insights", None)
    return _cached("insights", INSIGHTS_TTL, build)


# ponytail: fixed floors, move to bsimvis_config.toml if a collection needs other ones.
OVERVIEW_MIN_AVG_FUNCS = 10  # packer stubs and tiny droppers hold 2-5 functions
OVERVIEW_MAX_PACKED = 0.5  # share of members tagged packer:* / runtime-packer:*


def _packed_share(meta):
    """Highest coverage of a packer tag in the cluster's file tag trees."""
    best = 0.0

    def visit(nodes):
        nonlocal best
        for n in nodes or []:
            tid = str(n.get("tag_id") or "").lower()
            if tid.startswith(("packer", "runtime-packer")):
                best = max(best, float(n.get("coverage") or 0))
            visit(n.get("children"))

    for key in ("file_tag_distribution", "tag_distribution"):
        for roots in (meta.get(key) or {}).values():
            visit(roots)
    return best


def is_noise_cluster(meta):
    """A cluster of packed stubs or near-empty files: not a family worth showing first."""
    avg = (meta.get("function_count_stats") or {}).get("avg")
    if avg is not None and avg < OVERVIEW_MIN_AVG_FUNCS:
        return True
    return _packed_share(meta) >= OVERVIEW_MAX_PACKED


FAMILY_MIN_COH = 0.2  # a looser cluster is always an umbrella, whatever its size
_FAM_KEYS = ("cid", "cluster_uuid", "cluster_name", "cohesion_score", "matching")
_FAM_KEYS += ("is_custom_name", "name_source", "name_basis", "name_ref", "auto_name")


def pick_families(roots, metas, min_coh=FAMILY_MIN_COH):
    """Families = the most relevant level of the display tree, ranked like the file search.

    A cluster is a family unless a cluster inside it ranks higher (cluster_display.score)
    or its cohesion is under min_coh. Then it is replaced by the families inside it and
    kept as a group in the map; such a cluster with none inside stays a family itself.
    -> (map tree, ranked families, hidden count). Pure: `metas` is {cid: stored meta}.
    """
    from bsimvis.app.services.cluster_display import score

    families, hidden = [], 0
    rank = lambda c: score(c.get("matching") or 0, c.get("cohesion_score") or 0)

    def best(c):
        return max((max(rank(k), best(k)) for k in c.get("children") or []), default=-1)

    def node(c):
        nonlocal hidden
        base = {k: c.get(k) for k in _FAM_KEYS}
        kids = []
        if (c.get("cohesion_score") or 0) < min_coh or best(c) > rank(c):
            kids = [n for n in map(node, c.get("children") or []) if n]
        if kids:
            return {**base, "family": False, "children": kids}
        if is_noise_cluster(metas.get(str(c.get("cid")), {})):
            hidden += 1
            return None
        fam = {**base, "family": True, "subs": len(c.get("children") or [])}
        families.append(fam)
        return fam

    tree = [n for n in map(node, roots) if n]
    families.sort(key=rank, reverse=True)
    return tree, families, hidden


ACTIVE_DAYS = 30
ACTIVE_WINDOWS = (ACTIVE_DAYS, 90, 365)
ACTIVE_TOP = 7


def rank_activity(families, metas, now_ms=None, days=ACTIVE_DAYS, top=ACTIVE_TOP):
    """Most active families (members uploaded in the last `days`) and the most
    recently changed ones (latest member upload), from the stored timelines.
    Plus the families seen in the wild most recently (newest member last_seen).
    -> (active, changed, seen, families with no timeline). Pure."""
    from bsimvis.app.services.cluster_utils import timeline_summary

    rows = [
        {
            **f,
            **timeline_summary(
                (metas.get(str(f.get("cid"))) or {}).get("timeline"), now_ms, days
            ),
        }
        for f in families
    ]
    timed = [f for f in rows if f["last_added"]]
    seen = sorted(
        (f for f in rows if f["last_seen"]), key=lambda f: f["last_seen"], reverse=True
    )
    active = sorted(
        (f for f in timed if f["new_members"]),
        key=lambda f: (f["new_members"], f["last_added"]),
        reverse=True,
    )
    changed = sorted(timed, key=lambda f: f["last_added"], reverse=True)
    return active[:top], changed[:top], seen[:top], len(rows) - len(timed)


def _all_cids(nodes):
    for c in nodes:
        yield c["cid"]
        yield from _all_cids(c.get("children") or [])


# Histograms the overview's Uploads card can switch between.
DATE_FIELDS = ("entry_date", "first_seen", "last_seen")


def _date_hists(r, col, bins=40):
    """Files over time per date field, all on one span and the same `bins` bins
    (first to last date over every field), so the Uploads card compares them."""
    keys = {f: f"{col}:idx:file:{f}" for f in DATE_FIELDS}
    ends = {}
    for f, key in keys.items():
        # ponytail: scores below 1e12 are legacy seconds (pre normalize-time); they
        # would stretch the span back to 1970, so the chart starts at the first ms one.
        lo = r.zrangebyscore(key, 1e12, "+inf", start=0, num=1, withscores=True)
        hi = r.zrange(key, -1, -1, withscores=True)
        if lo and hi:
            ends[f] = (float(lo[0][1]), float(hi[0][1]))
    if not ends:
        return {f: None for f in DATE_FIELDS}
    a = min(lo for lo, _ in ends.values())
    step = max((max(hi for _, hi in ends.values()) - a) / bins, 1.0)
    pipe = r.pipeline(transaction=False)
    for f in ends:
        for i in range(bins):
            left = a + i * step
            pipe.zcount(keys[f], left if i == 0 else f"({left}", a + (i + 1) * step)
    counts = pipe.execute()
    out = {f: None for f in DATE_FIELDS}
    for n, f in enumerate(ends):
        out[f] = {"start": a, "step": step, "counts": counts[n * bins : (n + 1) * bins]}
    return out


def _buckets(r, col, field, limit=8):
    """Largest values of an indexed file field, by bucket size (registry + SCARD)."""
    keys = [
        k.decode() if isinstance(k, bytes) else k
        for k in r.smembers(f"{col}:reg:file:{field}")
    ]
    if not keys or len(keys) > 5000:
        return []
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.scard(k)
    prefix = f"{col}:idx:file:{field}:"
    sized = [(k[len(prefix) :], n) for k, n in zip(keys, pipe.execute()) if n]
    sized.sort(key=lambda kv: -kv[1])
    return [{"value": v, "count": n} for v, n in sized[:limit]]


def get_collection_overview():
    """One collection's landing cards: top families, recent and rare files (cached)."""
    from bsimvis.app.routes.search_file import search_files
    from bsimvis.app.routes.search_file_clusters import search_files_by_cluster

    col = request.args.get("collection")
    if not col:
        return {"error": "No collection specified"}, 400
    axis = request.args.get("axis", "code").strip().lower()
    if axis not in BIN_AXES:
        return {"error": f"axis must be one of {', '.join(BIN_AXES)}"}, 400

    def build():
        from bsimvis.app.services.cluster_utils import get_cluster_metas

        fams = _call(
            search_files_by_cluster, collection=col, axis=axis, limit=40, per_cluster=0
        )
        roots = fams.get("clusters", [])
        r = get_redis()
        metas = get_cluster_metas(
            r, col, fams.get("algo"), list(_all_cids(roots)), "bin_cluster"
        )
        loose_total = fams.get("unclustered_total", 0)
        rare = None
        if not fams.get("has_clusters"):
            # No stored display tree (clusters from an older build): the flat root list
            # still names them; its rows carry the meta the noise filter reads.
            from bsimvis.app.routes.bin_cluster import list_bin_clusters

            flat = _call(
                list_bin_clusters,
                collection=col,
                axis=axis,
                limit=40,
                sort_by="count",
                sort_order="desc",
            ).get("results", [])
            roots = [
                {
                    "cid": c.get("cluster_id"),
                    "cluster_uuid": c.get("cluster_uuid"),
                    "cluster_name": c.get("cluster_name"),
                    **name_fields(c),
                    "cohesion_score": c.get("cohesion_score"),
                    "matching": c.get("count", 0),
                    "children": [],
                }
                for c in flat
            ]
            metas = {str(c.get("cluster_id")): c for c in flat}
            if roots:
                loose_total = max(
                    0, fams.get("total", 0) - sum(c["matching"] for c in roots)
                )
                rare = []  # which files are outside a flat cluster is not indexed
        tree, kept, hidden = pick_families(roots, metas)
        # Widen the window until a family grew in it: an idle collection still ranks.
        for days in ACTIVE_WINDOWS:
            active, changed, seen, untimed = rank_activity(kept, metas, days=days)
            if active:
                break
        recent = _call(
            search_files,
            collection=col,
            limit=8,
            sort_by="entry_date",
            sort_order="desc",
        )
        dates = _date_hists(r, col)
        if rare is None:
            rare = _call(
                search_files_by_cluster,
                collection=col,
                axis=axis,
                unclustered=1,
                limit=8,
            ).get("files", [])
        return {
            "axis": axis,
            "has_clusters": bool(roots),
            "families": kept,
            "family_map": tree,
            "families_hidden": hidden,
            "families_total": len(kept),
            "active": active,
            "changed": changed,
            "seen": seen,
            "active_days": days,
            "families_untimed": untimed,
            "unclustered_total": loose_total,
            "recent": recent.get("files", []),
            "rare": rare,
            "uploads": dates["entry_date"],
            "dates": dates,
            "arch": _buckets(r, col, "language_id"),
            "filetype": _buckets(r, col, "filetype", 20),
            "tags": _top_tags([{"name": col}], limit=12)["top"],
        }

    key = f"overview:{col}:{axis}"
    if request.args.get("refresh") == "true":
        _CACHE.pop(key, None)
    return _cached(key, INSIGHTS_TTL, build)


# --- Unified search -------------------------------------------------------


def _group(kind, items, mapper, limit):
    return {"kind": kind, "items": [mapper(i) for i in items[:limit]]}


def _search_params():
    """Shared parsing for both the batch and streaming search endpoints."""
    q = (request.args.get("q") or "").strip()
    limit = request.args.get("limit", 5, type=int)
    scope = request.args.getlist("collection") or [c["name"] for c in _collections()]
    scope_total = len(scope)
    max_cols = request.args.get("max_collections", type=int)
    truncated = bool(max_cols) and scope_total > max_cols
    if max_cols:
        scope = scope[:max_cols]
    return q, limit, scope, scope_total, truncated


def _search_groups(q, limit, scope):
    """Yields one result group at a time, cheapest entity types first.

    A generator rather than a list so the streaming endpoint can flush each
    group the moment it is found; `unified_search` just drains it.
    """
    from bsimvis.app.routes.bin_cluster import list_bin_clusters
    from bsimvis.app.routes.cluster import list_clusters
    from bsimvis.app.routes.pools import list_pools
    from bsimvis.app.routes.search_collection import search_batches, search_collections
    from bsimvis.app.routes.search_feature import search_features
    from bsimvis.app.routes.search_file import search_files
    from bsimvis.app.routes.search_function import search_functions
    from bsimvis.app.routes.strings import search_strings

    cols = _call(search_collections, q=q, limit=limit).get("collections", [])
    if cols:
        yield _group(
            "collections",
            cols,
            lambda c: {
                "title": c["name"],
                "subtitle": f"{c.get('total_files', 0)} files",
                "url": f"/collections/{quote(c['name'])}",
            },
            limit,
        )

    pools = _call(list_pools, limit=100000)
    pool_items = [
        p
        for p in (pools.get("items") or pools.get("pools") or [])
        if q.lower() in str(p.get("name", "")).lower()
    ]
    if pool_items:
        yield _group(
            "pools",
            pool_items,
            lambda p: {
                "title": p.get("name"),
                "subtitle": f"{len(p.get('collections', []))} collections",
                "url": f"/pools/{quote(str(p.get('id') or p.get('pool_id')))}",
            },
            limit,
        )

    # Tags have no search route: match names straight off the tag index.
    tag_hits = []
    for name in scope:
        for t in tag_service.get_collection_tags(name):
            if q.lower() in t.lower():
                tag_hits.append({"tag": t, "collection": name})
        if len(tag_hits) >= limit:
            break
    if tag_hits:
        # Counts only for the handful actually shown -- a tag lives on files or
        # on functions, and linking to the wrong list view shows nothing.
        for t in tag_hits[:limit]:
            t["stats"] = tag_service.get_tag_stats(t["collection"], t["tag"])
        yield _group(
            "tags",
            tag_hits,
            lambda t: {
                "title": t["tag"],
                "subtitle": (
                    f"{t['collection']} \u00b7 {t['stats']['file']} files, "
                    f"{t['stats']['function']} functions"
                ),
                "url": (
                    f"/collections/{quote(t['collection'])}"
                    f"/{'files' if t['stats']['file'] or not t['stats']['function'] else 'functions'}"
                    f"?tag={quote(t['tag'])}"
                ),
            },
            limit,
        )

    def fan(kind, fn, key, mapper, q_param="q", **extra):
        items = []
        for name in scope:
            res = _call(fn, **{q_param: q}, collection=name, limit=limit, **extra)
            for it in res.get(key, res.get("items", []))[:limit]:
                it = dict(it)
                it["_collection"] = name
                items.append(it)
            if len(items) >= limit:
                break
        return _group(kind, items, mapper, limit) if items else None

    fans = [
        (
            "files",
            search_files,
            "files",
            lambda f: {
                "title": f.get("file_name") or f.get("file_md5"),
                "subtitle": f"{f['_collection']} · {f.get('file_md5', '')[:12]}",
                "url": f"/collections/{quote(f['_collection'])}/files/{f.get('file_md5')}",
            },
        ),
        (
            "functions",
            search_functions,
            "functions",
            lambda f: {
                "title": f.get("user_name") or f.get("function_name") or f.get("name"),
                "subtitle": f"{f['_collection']} · {f.get('file_name', '')}",
                "url": (
                    f"/collections/{quote(f['_collection'])}/files/{f.get('file_md5')}"
                    f"/functions/{f.get('entrypoint_address')}"
                ),
            },
        ),
        (
            "strings",
            search_strings,
            "items",
            lambda s: {
                "title": s["value"][:120],
                "subtitle": f"{s['_collection']} · {s['files']} files, {s['functions']} functions",
                "url": f"/collections/{quote(s['_collection'])}/strings?q={quote(s['value'][:120])}",
            },
        ),
        (
            "function_strings",
            search_functions,
            "functions",
            lambda f: {
                "title": f.get("function_name") or f.get("name"),
                "subtitle": f"{f['_collection']} · uses string \u201c{q}\u201d",
                "url": (
                    f"/collections/{quote(f['_collection'])}/files/{f.get('file_md5')}"
                    f"/functions/{f.get('entrypoint_address')}"
                ),
            },
            "strings",
        ),
        (
            "batches",
            search_batches,
            "batches",
            lambda b: {
                "title": b.get("name") or b.get("batch_uuid"),
                "subtitle": b["_collection"],
                "url": f"/collections/{quote(b['_collection'])}/batches?q={b.get('batch_uuid')}",
            },
        ),
        (
            "function_clusters",
            list_clusters,
            "results",
            lambda c: {
                "title": c.get("cluster_name") or f"cluster {c.get('cluster_id')}",
                "subtitle": f"{c['_collection']} · {c.get('count', 0)} members",
                "url": (
                    f"/collections/{quote(c['_collection'])}/functions/clusters"
                    f"?cluster_uuid={c.get('cluster_uuid')}"
                ),
            },
        ),
        (
            "binary_clusters",
            list_bin_clusters,
            "results",
            lambda c: {
                "title": c.get("cluster_name") or f"cluster {c.get('cluster_id')}",
                **name_fields(c),
                "subtitle": f"{c['_collection']} · {c.get('count', 0)} files",
                "url": (
                    f"/collections/{quote(c['_collection'])}/files/clusters"
                    f"?cluster_uuid={c.get('cluster_uuid')}"
                ),
            },
        ),
        (
            "features",
            search_features,
            "features",
            lambda f: {
                "title": f.get("feature") or f.get("hash"),
                "subtitle": f"{f['_collection']} · {f.get('frequency', 0)} functions",
                "url": f"/collections/{quote(f['_collection'])}/features/{f.get('hash')}",
            },
        ),
    ]
    for kind, fn, key, mapper, *q_param in fans:
        g = fan(kind, fn, key, mapper, *q_param)
        if g:
            yield g


def unified_search():
    """Fans a free-text query out over every searchable entity type.

    Params: q (required), limit (per type, default 5),
            collection (repeatable; default = every collection).
    """
    q, limit, scope, scope_total, truncated = _search_params()
    if not q:
        return {"query": "", "groups": []}

    return {
        "query": q,
        "scope": len(scope),
        "scope_total": scope_total,
        "truncated": truncated,
        "groups": list(_search_groups(q, limit, scope)),
    }


def unified_search_stream():
    """Same fan-out as `unified_search`, streamed as NDJSON, group by group.

    One JSON object per line: a `meta` line first, then one line per group as
    it is found, then a `done` line. Lets the palette paint the cheap hits
    (collections, pools, tags) without waiting on the per-collection scans.
    """
    from flask import Response, stream_with_context

    q, limit, scope, scope_total, truncated = _search_params()

    def lines():
        head = {
            "type": "meta",
            "query": q,
            "scope": len(scope),
            "scope_total": scope_total,
            "truncated": truncated,
        }
        yield json.dumps(head) + "\n"
        if q:
            for g in _search_groups(q, limit, scope):
                yield json.dumps({"type": "group", **g}) + "\n"
        yield json.dumps({"type": "done"}) + "\n"

    return Response(
        stream_with_context(lines()),
        mimetype="application/x-ndjson",
        # Chunks are useless if a proxy buffers them into one response.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    # Self-check: uv run python -I bsimvis/app/routes/home.py
    big = {"function_count_stats": {"avg": 200}}
    leaf = lambda cid, coh, n, kids=(): {
        "cid": cid,
        "cohesion_score": coh,
        "matching": n,
        "children": list(kids),
    }
    roots = [
        leaf(
            "A",
            0.13,
            300,
            [
                leaf("A1", 0.85, 13, [leaf("A1a", 0.97, 4)]),
                leaf("A2", 0.3, 10, [leaf("A2a", 0.9, 4)]),
                leaf("A3", 0.9, 6),
            ],
        ),
        leaf("B", 0.85, 35, [leaf("B1", 1, 16)]),
        leaf("C", 0.14, 8),
        leaf("D", 0.25, 6, [leaf("D1", 1, 5)]),
    ]
    metas = {k: big for k in ("A", "A1", "A1a", "A2", "A2a", "B", "B1", "C", "D", "D1")}
    metas["A3"] = {"function_count_stats": {"avg": 2}}
    tree, fams, hidden = pick_families(roots, metas)
    # A is under the cohesion floor: a group of its children. A2 (10 files, 0.3)
    # outranks its 4-file 0.9 child, so it stays the family; D1 outranks D.
    assert [f["cid"] for f in fams] == ["B", "A1", "D1", "A2", "C"], fams
    assert not tree[3]["family"] and tree[3]["children"][0]["cid"] == "D1"
    assert hidden == 1  # A3: 2 functions per file
    assert not tree[0]["family"]
    assert [k["cid"] for k in tree[0]["children"]] == ["A1", "A2"]
    assert tree[2]["family"] and fams[0]["subs"] == 1  # C: loose, nothing inside
    packed = {"packer": [{"tag_id": "packer:upx", "coverage": 0.8, "children": []}]}
    assert is_noise_cluster({"file_tag_distribution": packed})
    # A pinned (analyst-named) 2-file node inside a loose group is a family, and
    # family rows carry the name fields (a near name keeps its reference).
    ref = {"cluster_id": 7, "cluster_name": "FAM", "delta": 1}
    named = {**leaf("P", 0.9, 2), "name_source": "analyst", "cluster_name": "FAM"}
    near = {**leaf("N", 0.9, 3), "name_source": "near", "name_ref": ref}
    tree, fams, _ = pick_families([leaf("G", 0.1, 50, [named, near])], {})
    assert [f["cid"] for f in fams] == ["N", "P"], fams
    assert fams[1]["name_source"] == "analyst" and fams[0]["name_ref"] == ref
    print("home self-check OK")
