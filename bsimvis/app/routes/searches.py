"""HTTP surface for persisted fast-relevance searches (`search_service.py`).

Scope resolution reuses what pair/file/contextual LLM analysis already use --
`_resolve_filters_to_ids` (`routes/llm.py`) for whole-collection/one-file/an
arbitrary filter selection, `AnalysisOrchestrator.pair_candidates` for a
bin_sim pair's diff -- so a search's candidate set is computed the same way
those endpoints already compute theirs, not by new logic.
"""

from flask import request

from bsimvis.app.services.search_service import search_service

VALID_SCOPE_TYPES = ("collection", "file", "filter", "pair")


def _resolve_scope(collection, scope):
    """Scope dict -> (func_ids, error). `error` is a user-facing string."""
    from bsimvis.app.services.analysis_orchestrator import (
        analysis_orchestrator,
        max_batch_size,
    )
    from bsimvis.app.routes.llm import _resolve_filters_to_ids

    scope_type = (scope or {}).get("type")
    if scope_type not in VALID_SCOPE_TYPES:
        return None, f"scope.type must be one of {VALID_SCOPE_TYPES}"

    if scope_type == "collection":
        return _resolve_filters_to_ids(collection, "", max_batch_size())

    if scope_type == "file":
        md5 = scope.get("md5")
        if not md5:
            return None, "scope.md5 is required for scope.type=file"
        filters_qs = f"md5={md5}"
        if scope.get("skip_fid_tagged", True):
            filters_qs += "&exclude_tag=fid"
        func_ids, error = _resolve_filters_to_ids(
            collection, filters_qs, max_batch_size()
        )
        if error:
            return None, error
        marker = f":func:{md5}:"
        return [fid for fid in func_ids if marker in fid], None

    if scope_type == "filter":
        filters = scope.get("filters")
        if not filters:
            return None, "scope.filters is required for scope.type=filter"
        return _resolve_filters_to_ids(collection, filters, max_batch_size())

    # scope_type == "pair"
    from bsimvis.app.services.bin_sim_service import bin_sim_service

    md5_a, md5_b = scope.get("md5_a"), scope.get("md5_b")
    if not md5_a or not md5_b:
        return None, "scope.md5_a and scope.md5_b are required for scope.type=pair"
    coll_b = scope.get("coll_b") or collection
    pool_id = scope.get("pool_id")
    algo = scope.get("algo", "unweighted_cosine")
    state = scope.get("state")
    if state not in (None, "all", "matched", "unique", "changed"):
        return None, "scope.state must be one of all, matched, unique, changed"
    try:
        threshold = float(scope.get("threshold", 0.9))
    except (TypeError, ValueError) as error:
        return None, str(error)
    include_unique = scope.get("include_unique", True) is not False
    include_unchanged = bool(scope.get("include_unchanged", True))
    if state:
        include_unique = state in ("all", "unique")
        include_unchanged = state in ("all", "matched")
        if state == "unique":
            threshold = 0
    # Unlike deep pair analysis, search defaults to covering matched functions
    # too (include_unchanged=True) -- fast triage is cheap, and the whole
    # point of this feature is not silently excluding a candidate.
    _sid, pair = bin_sim_service.load_pair(
        collection, md5_a, md5_b, coll_b, pool_id, algo
    )
    if not pair:
        return None, "Similarity not calculated for this pair"
    try:
        candidates = analysis_orchestrator.pair_candidates(
            pair,
            threshold,
            include_unique,
            include_unchanged,
            scope.get("skip_fid_tagged", True) is not False,
            int(scope.get("min_complexity") or 0),
            int(scope.get("max_functions") or 0),
        )
    except ValueError as error:
        return None, str(error)
    return [row["func_id"] for row in candidates], None


def create_search():
    from bsimvis.app.services.analysis_orchestrator import max_batch_size

    data = request.json or {}
    collection = data.get("collection")
    pool_id = data.get("pool") or data.get("pool_id")
    if pool_id and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool_id}"
    if not collection:
        return {"error": "Missing collection"}, 400

    query = (data.get("query") or "").strip()
    if not query:
        return {"error": "Missing query"}, 400

    scope = data.get("scope") or {}
    func_ids, error = _resolve_scope(collection, scope)
    if error:
        return {"error": error}, 400
    func_ids = [f for f in dict.fromkeys(func_ids) if f]
    if not func_ids:
        return {"error": "Selection resolved to zero functions"}, 400

    search_id, job_id, total = search_service.create_search(
        collection, scope, query, func_ids, name=data.get("name")
    )

    cap = max_batch_size()
    result = {"search_id": search_id, "job_id": job_id, "total": total}
    if cap > 0 and total > cap:
        result["warning"] = (
            f"Search selection has {total} functions, over the batch cap of "
            f"{cap}. This will run anyway; raise llm.batch_max if that's not "
            f"what you want."
        )
    return result, 201


def list_searches():
    try:
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return {"error": "limit and offset must be integers"}, 400
    searches, total = search_service.list_searches(limit, offset)
    for s in searches:
        s["verdict_counts"] = search_service.get_verdict_counts(s["id"])
    return {"searches": searches, "total": total, "limit": limit, "offset": offset}


def get_search(search_id):
    from bsimvis.app.services.job_service import JobService

    meta = search_service.get_search(search_id)
    if not meta:
        return {"error": "Search not found"}, 404
    if meta.get("status") == "running" and meta.get("job_id"):
        job = JobService().get_job_status(meta["job_id"])
        if job:
            meta["job_status"] = job.get("status")
            meta["progress"] = job.get("progress")
    return meta


def delete_search(search_id):
    ok, message = search_service.delete_search(search_id)
    if not ok:
        return {"error": message}, 404
    return {"status": "success", "message": message}


def _enrich_with_function_meta(rows):
    """Attach name/namespace/tags/etc to each result row so the UI can reuse
    EntityRenderer.renderFunction/renderTag instead of showing a bare func_id."""
    from bsimvis.app.services.function_service import fetch_function_data
    from bsimvis.app.services.llm_tools import parse_func_id

    for row in rows:
        try:
            collection, md5, addr = parse_func_id(row["func_id"])
        except ValueError:
            continue
        _, _, meta, _ = fetch_function_data(collection, md5, addr, meta_only=True)
        meta = meta or {}
        row["collection"] = collection
        row["file_md5"] = md5
        row["entrypoint_address"] = addr
        row["function_name"] = meta.get("function_name")
        row["namespace"] = meta.get("namespace")
        row["parameters"] = meta.get("parameters")
        row["return_type"] = meta.get("return_type")
        row["bsim_features_count"] = meta.get("bsim_features_count")
        row["tags"] = meta.get("tags") or []
        row["user_tags"] = meta.get("user_tags") or []
        row["note_owners"] = meta.get("note_owners") or []
    return rows


def get_search_results(search_id):
    if not search_service.get_search(search_id):
        return {"error": "Search not found"}, 404
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return {"error": "limit and offset must be integers"}, 400
    verdict = request.args.getlist("verdict") or None
    rows, total = search_service.get_results(search_id, offset, limit, verdict)
    rows = _enrich_with_function_meta(rows)
    return {"results": rows, "total": total, "limit": limit, "offset": offset}


def apply_tag(search_id):
    from bsimvis.app.services import tag_taxonomy
    from bsimvis.app.services.tag_service import tag_service

    meta = search_service.get_search(search_id)
    if not meta:
        return {"error": "Search not found"}, 404

    data = request.json or {}
    func_ids = [f for f in dict.fromkeys(data.get("func_ids") or []) if f]
    tag = data.get("tag")
    if not func_ids or not tag:
        return {"error": "func_ids and tag are required"}, 400

    marked = tag_taxonomy.namespaced(tag)
    tag_service.create_tag(meta["collection"], marked, llm=True)
    applied = [
        fid
        for fid in func_ids
        if tag_service.add_user_tag(meta["collection"], "function", fid, marked)
    ]
    return {"tag": marked, "applied": applied, "total": len(func_ids)}


def analyze_selection(search_id):
    from bsimvis.app.services.job_service import JobService, JobType

    meta = search_service.get_search(search_id)
    if not meta:
        return {"error": "Search not found"}, 404

    data = request.json or {}
    func_ids = [f for f in dict.fromkeys(data.get("func_ids") or []) if f]
    if not func_ids:
        return {"error": "func_ids is required"}, 400

    actions = data.get("actions") or ["notes", "tags"]
    invalid = [a for a in actions if a not in ("notes", "tags")]
    if invalid:
        return {"error": f"Invalid actions: {', '.join(invalid)}"}, 400

    payload = {
        "collection": meta["collection"],
        "func_ids": func_ids,
        "actions": actions,
        "overwrite": bool(data.get("overwrite")),
        "custom_prompt": data.get("custom_prompt") or meta.get("query"),
    }
    job_id = JobService().create_job(JobType.LLM_CONTEXTUAL_BATCH, payload)
    return {"job_id": job_id, "total": len(func_ids)}
