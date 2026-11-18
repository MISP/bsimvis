"""Analyst capabilities (plan G): free-text name, MBC / ATT&CK ids, evidence pointers.

The store is capability_service (definition global, evidence per collection or
pool). Suggestions from the model live in the queue Redis (`capsugg:`), never in
Kvrocks, until the analyst accepts them.
"""

import json

from flask import request
from flask_restx import abort

from bsimvis.app.services import capability_service, nway_sem_service
from bsimvis.app.services.redis_client import get_redis


def _ns(src):
    ns = nway_sem_service.ns_of(src.get("collection"), src.get("pool"))
    if not ns:
        abort(400, "collection or pool is required")
    return ns


def cluster_files(r, ns, cluster_uuid):
    """Member md5s of a bin cluster's stored N-way set, or None when it has none."""
    h = r.hget(f"{ns}:nway_sem:by_cluster", (cluster_uuid or "").strip().lower())
    raw = h and r.get(f"{ns}:nway_sem:set:{capability_service._decode(h)}")
    if not raw:
        return None
    return list(json.loads(capability_service._decode(raw)).get("members") or [])


def get_list():
    """Capabilities accepted in a collection / pool, with their evidence there."""
    a = request.args
    ns, r = _ns(a), get_redis()
    md5s = None
    if a.get("cluster_uuid"):
        md5s = cluster_files(r, ns, a.get("cluster_uuid"))
        if md5s is None:
            return {"capabilities": [], "total": 0, "no_set": True}
    caps = capability_service.listing(
        r, ns, uid=a.get("uuid"), label=a.get("id"), q=a.get("q"), md5s=md5s
    )
    return {"capabilities": caps, "total": len(caps)}


def _put(r, ns, item, owner, replace):
    try:
        rec, ev = capability_service.put(r, ns, item, owner=owner, replace=replace)
    except LookupError as e:
        abort(404, str(e))
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError as e:
        abort(409, str(e))
    return {**rec, "evidence": ev, "names": capability_service.names_of(rec)}


def post_accept():
    """Accepts capabilities (multi-select): each item creates one, or with `uuid`
    adds its ids and evidence to that capability (an attach suggestion)."""
    body = request.get_json(silent=True) or {}
    ns, r = _ns(body), get_redis()
    items = body.get("items")
    if not isinstance(items, list) or not items or len(items) > 50:
        abort(400, "items must be a list of 1 to 50 capabilities")
    owner = body.get("owner")
    for it in items:
        if not isinstance(it, dict):
            abort(400, "each item must be an object")
    return {
        "status": "success",
        "capabilities": [_put(r, ns, it, owner, replace=False) for it in items],
    }


def post_save():
    """Saves one capability in full from the editor (ids and evidence replaced)."""
    body = request.get_json(silent=True) or {}
    ns, r = _ns(body), get_redis()
    cap = body.get("capability")
    if not isinstance(cap, dict):
        abort(400, "capability must be an object")
    return {
        "status": "success",
        "capability": _put(r, ns, cap, body.get("owner"), replace=True),
    }


def delete_one(uid):
    """Drops a capability's evidence in a collection / pool (the definition goes
    with its last one)."""
    ns = _ns(request.args)
    if not capability_service.remove(get_redis(), ns, uid):
        abort(404, f"no capability {uid}")
    return {"status": "success"}


def _set_groups(r, ns, cluster_uuid):
    rec = nway_sem_service.load_set(r, ns, None, (cluster_uuid or "").strip().lower())
    if not rec:
        abort(404, "no stored N-way set for this cluster: open its N-way or Build it")
    return rec


def _pick(rec, wanted):
    """Stored groups matching `wanted` (gid or name_id), with their cache keys."""
    want = set(wanted)
    out = []
    for g in rec["groups"]:
        if not g.get("anchors"):
            continue
        if g.get("gid") in want or (g.get("name_id") and g["name_id"] in want):
            out.append(
                {**g, "key": capability_service.gkey(g.get("name_id"), g["anchors"])}
            )
    return out


def _answer(red, ns, groups):
    """Each group's cached suggestions and the accepted capabilities that point at
    it; a suggestion that one of them already covers carries `accepted`."""
    got, pending = capability_service.cache_get(red, ns, [g["key"] for g in groups])
    caps = capability_service.listing(get_redis(), ns)
    out = []
    for g in groups:
        acc = capability_service.accepted_for_group(
            caps, g.get("name_id"), g["anchors"]
        )
        sugg = (got.get(g["key"]) or {}).get("suggestions")
        for s in sugg or []:
            s["accepted"] = capability_service.covered_by(s, acc)
        out.append(
            {
                "gid": g.get("gid"),
                "name_id": g.get("name_id"),
                "name": g.get("name"),
                "key": g["key"],
                "suggestions": sugg,
                "at": (got.get(g["key"]) or {}).get("at"),
                "accepted": [{"uuid": c["uuid"], "name": c["name"]} for c in acc],
            }
        )
    return {"groups": out, "pending": pending}


def get_functions():
    """Names and files of up to 300 functions (`fids`: comma list of md5:addr), for
    the evidence lists."""
    a = request.args
    if a.get("pool"):
        return {"functions": []}
    ns, r = _ns(a), get_redis()
    fids = [f for f in (a.get("fids") or "").split(",") if f][:300]
    pipe = r.pipeline(transaction=False)
    for f in fids:
        pipe.get(f"{ns}:func:{f}:meta")
    out = []
    for f, raw in zip(fids, pipe.execute()):
        m = capability_service._loads(raw) if raw else None
        m = m[0] if isinstance(m, list) and m else m
        if isinstance(m, dict):
            out.append(
                {
                    "fid": f,
                    "name": m.get("function_name"),
                    "file_name": m.get("file_name"),
                }
            )
    return {"functions": out}


def post_suggest():
    """Asks the model for capability suggestions on selected groups of a cluster's
    stored N-way set (`groups`: gids or name ids). Cached ones come back as they
    are unless `refresh`; the others start in the background (poll suggestions)."""
    from bsimvis.app.services import nway_group_service
    from bsimvis.app.services.redis_client import get_queue_redis

    body = request.get_json(silent=True) or {}
    if body.get("pool"):
        abort(400, "suggestions read function code: open the member collection")
    ns, r = _ns(body), get_redis()
    wanted = body.get("groups")
    if not isinstance(wanted, list) or not wanted or len(wanted) > 60:
        abort(400, "groups must be a list of 1 to 60 group ids")
    if not capability_service.enabled():
        abort(409, "capabilities are disabled ([capabilities] enabled = false)")
    rec = _set_groups(r, ns, body.get("cluster_uuid"))
    groups = _pick(rec, wanted)
    red = get_queue_redis()
    got, _ = capability_service.cache_get(red, ns, [g["key"] for g in groups])
    todo = [g for g in groups if body.get("refresh") or not got.get(g["key"])]
    if body.get("refresh") and todo:
        red.delete(*[f"capsugg:{ns}:{g['key']}" for g in todo])
    nway_group_service.request_suggestions(ns, ns, todo, len(rec.get("members") or []))
    return _answer(red, ns, groups)


def get_suggestions():
    """Cached suggestions of a cluster's groups (`groups`: comma list, empty = all)."""
    from bsimvis.app.services.redis_client import get_queue_redis

    a = request.args
    ns, r = _ns(a), get_redis()
    rec = _set_groups(r, ns, a.get("cluster_uuid"))
    wanted = [x for x in (a.get("groups") or "").split(",") if x]
    groups = (
        _pick(rec, wanted)
        if wanted
        else [
            {**g, "key": capability_service.gkey(g.get("name_id"), g["anchors"])}
            for g in rec["groups"]
            if g.get("anchors")
        ]
    )
    return _answer(get_queue_redis(), ns, groups)


def get_group_functions():
    """Every function of one group of a cluster's stored N-way set, row by row:
    `rows` = [[md5:addr of each file's copy], ...] and `row_names`. The table is
    listed with the params the set was stored with (a set too big to build on a
    request answers 428, as /nway does)."""
    from flask import current_app

    from bsimvis.app.routes import nway as nway_routes

    a = request.args
    if a.get("pool"):
        abort(400, "pools: open the member collection")
    ns, r = _ns(a), get_redis()
    cu = (a.get("cluster_uuid") or "").strip().lower()
    rec = _set_groups(r, ns, cu)
    gid = a.get("gid")
    if not gid and a.get("name_id"):
        gid = next(
            (g["gid"] for g in rec["groups"] if g.get("name_id") == a["name_id"]), None
        )
    if not gid:
        abort(400, "gid (or a stored name_id) is required")
    from werkzeug.exceptions import HTTPException

    q = {"collection": ns, "tab": "all", "lane": gid, **(rec.get("params") or {})}
    # the cluster's own table; its stored member files when the cluster is gone or
    # was never one (a set stored from an ad hoc file list)
    tries = [{"cluster_uuid": cu}, {"md5s": ",".join(rec.get("members") or [])}]
    for k, extra in enumerate(tries):
        try:
            with current_app.test_request_context(query_string={**q, **extra}):
                rows, info = nway_routes._lane_rows(gid)
            break
        except HTTPException as e:
            if k == len(tries) - 1 or e.code not in (400, 404):
                raise
    cap = capability_service.GROUP_ROWS_MAX
    return {
        "gid": gid,
        "rows": [[nway_sem_service.short(f) for f in row] for row in rows[:cap]],
        "row_names": [i.get("name") for i in info[:cap]],
        "total_rows": len(rows),
    }


def get_catalog():
    """MBC / ATT&CK ids whose id or name holds `q` (the editor's id picker)."""
    q = (request.args.get("q") or "").lower().strip()
    cat = capability_service.catalog()
    out = [
        {"id": i, "name": v.get("name"), "kind": k}
        for k in ("mbc", "attack")
        for i, v in cat[k].items()
        if not q or q in i.lower() or q in (v.get("name") or "").lower()
    ]
    return {"ids": out[:40], "total": len(out)}
