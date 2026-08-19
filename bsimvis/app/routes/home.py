"""Instance-wide homepage: counts, job health, insights, and unified search.

Everything here fans out over the existing per-entity search routes rather than
maintaining a second index. `_call` invokes a route function inside a synthetic
request context so the existing `request.args` parsing is reused verbatim.
"""

import json
import logging
import time

from flask import current_app, request

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
        prefix = tag.split(":", 1)[0] if ":" in tag else (
            tag.split("/", 1)[0] if "/" in tag else "other"
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
                    "cluster_name": cl.get("cluster_name"),
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

    cols = _call(search_collections, q=q, limit=limit).get("collections", [])
    if cols:
        yield _group(
            "collections",
            cols,
            lambda c: {
                "title": c["name"],
                "subtitle": f"{c.get('total_files', 0)} files",
                "url": f"/collections/{c['name']}",
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
                "url": f"/pools/{p.get('id') or p.get('pool_id')}",
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
        yield _group(
            "tags",
            tag_hits,
            lambda t: {
                "title": t["tag"],
                "subtitle": t["collection"],
                "url": f"/collections/{t['collection']}/files?tag={t['tag']}",
            },
            limit,
        )

    def fan(kind, fn, key, mapper, **extra):
        items = []
        for name in scope:
            res = _call(fn, q=q, collection=name, limit=limit, **extra)
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
                "title": f.get("file_name") or f.get("md5"),
                "subtitle": f"{f['_collection']} · {f.get('md5', '')[:12]}",
                "url": f"/collections/{f['_collection']}/files/{f.get('md5')}",
            },
        ),
        (
            "functions",
            search_functions,
            "functions",
            lambda f: {
                "title": f.get("function_name") or f.get("name"),
                "subtitle": f"{f['_collection']} · {f.get('file_name', '')}",
                "url": (
                    f"/collections/{f['_collection']}/files/{f.get('file_md5') or f.get('md5')}"
                    f"/functions/{f.get('address') or f.get('addr')}"
                ),
            },
        ),
        (
            "batches",
            search_batches,
            "batches",
            lambda b: {
                "title": b.get("batch_name") or b.get("batch_uuid"),
                "subtitle": b["_collection"],
                "url": f"/collections/{b['_collection']}/batches",
            },
        ),
        (
            "function_clusters",
            list_clusters,
            "results",
            lambda c: {
                "title": c.get("cluster_name") or f"cluster {c.get('cluster_id')}",
                "subtitle": f"{c['_collection']} · {c.get('member_count', 0)} members",
                "url": f"/collections/{c['_collection']}/functions/clusters",
            },
        ),
        (
            "binary_clusters",
            list_bin_clusters,
            "results",
            lambda c: {
                "title": c.get("cluster_name") or f"cluster {c.get('cluster_id')}",
                "subtitle": f"{c['_collection']} · {c.get('member_count', 0)} files",
                "url": f"/collections/{c['_collection']}/files/clusters",
            },
        ),
        (
            "features",
            search_features,
            "features",
            lambda f: {
                "title": f.get("feature") or f.get("hash"),
                "subtitle": f"{f['_collection']} · {f.get('count', '')}",
                "url": f"/collections/{f['_collection']}/features/{f.get('hash') or f.get('feature_hash')}",
            },
        ),
    ]
    for kind, fn, key, mapper in fans:
        g = fan(kind, fn, key, mapper)
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
