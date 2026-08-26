"""HTTP surface for the agentic LLM analysis module: the interactive,
tool-using analyst chat, and the context-aware batch tagging orchestrator.

Kept separate from `routes/llm.py` (the single-shot summarize/batch
endpoints) since this module's two features share `llm_tools`/
`analysis_orchestrator` rather than `llm_batch_service`.
"""

from flask import request


def start_chat_session():
    from bsimvis.app.services.llm_chat_service import llm_chat_service

    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"
    if not collection:
        return {"error": "Missing collection"}, 400

    session_id = llm_chat_service.start_session(
        collection,
        custom_system_prompt=data.get("system_prompt"),
        context=data.get("context"),
    )
    return {"session_id": session_id}


def chat_message(session_id):
    from bsimvis.app.services.llm_chat_service import llm_chat_service

    data = request.json or {}
    message = data.get("message")
    if not message:
        return {"error": "Missing message"}, 400

    result = llm_chat_service.send_message(session_id, message)
    if "error" in result:
        return result, 404 if result["error"] == "Unknown or expired session" else 500
    return result


def get_chat_session(session_id):
    from bsimvis.app.services.llm_chat_service import llm_chat_service

    history = llm_chat_service.get_session(session_id)
    if history is None:
        return {"error": "Unknown or expired session"}, 404
    return {"session_id": session_id, "messages": history}


def contextual_batch():
    """Starts a background context-aware LLM tagging job: partitions the
    given functions by call-graph locality and runs a bottom-up pass so
    tightly-calling functions are judged with each other's summaries in
    context, instead of `llm/batch`'s per-function-blind pass."""
    from bsimvis.app.services.analysis_orchestrator import (
        max_batch_size,
        analysis_orchestrator,
    )
    from bsimvis.app.services.job_service import JobService, JobType
    from bsimvis.app.routes.llm import _resolve_filters_to_ids

    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"
    if not collection:
        return {"error": "Missing collection"}, 400

    cap = max_batch_size()
    func_ids = data.get("func_ids")
    explicit = bool(func_ids)

    if not func_ids:
        filters = data.get("filters")
        if not filters:
            return {"error": "Provide either func_ids or filters"}, 400
        func_ids, error = _resolve_filters_to_ids(collection, filters, cap)
        if error:
            return {"error": error}, 400

    func_ids = [f for f in dict.fromkeys(func_ids) if f]
    if not func_ids:
        return {"error": "Selection resolved to zero functions"}, 400

    if not explicit and cap > 0 and len(func_ids) > cap:
        return {
            "error": (
                f"Filter matched {len(func_ids)} functions, over the batch cap "
                f"of {cap}. Narrow the filter, raise llm.batch_max, or set it "
                f"to 0 for no cap."
            ),
            "count": len(func_ids),
            "cap": cap,
        }, 413

    job_service = JobService()
    job_id = job_service.create_job(
        JobType.LLM_CONTEXTUAL_BATCH,
        {
            "collection": collection,
            "func_ids": func_ids,
            "actions": data.get("actions") or ["notes", "tags"],
            "overwrite": bool(data.get("overwrite")),
            "custom_prompt": data.get("custom_prompt"),
            "unit_max_size": data.get("unit_max_size"),
        },
    )
    return {"job_id": job_id, "total": len(func_ids)}


def contextual_batch_status(job_id):
    from bsimvis.app.services.job_service import JobService
    from bsimvis.app.services.analysis_orchestrator import analysis_orchestrator
    import json as _json

    job_service = JobService()
    job = job_service.r.hgetall(f"job:{job_id}")
    if not job:
        return {"error": "Job not found"}, 404

    results = analysis_orchestrator.get_results(job_id)
    counts = {"done": 0, "skipped": 0, "failed": 0}
    errors = []
    for fid, res in results.items():
        state = res.get("state")
        counts[state] = counts.get(state, 0) + 1
        if state == "failed":
            errors.append({"func_id": fid, "error": res.get("detail")})

    payload = {}
    try:
        payload = _json.loads(job.get("payload") or "{}")
    except Exception:
        pass

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "progress": int(job.get("progress") or 0),
        "total": len(payload.get("func_ids") or []),
        "processed": sum(counts.values()),
        "counts": counts,
        "errors": errors,
        "results": results,
    }


def contextual_batch_cancel(job_id):
    from bsimvis.app.services.job_service import JobService

    if not JobService().cancel_job(job_id):
        return {"error": "Job not found"}, 404
    return {"status": "cancelled", "job_id": job_id}


def file_analysis():
    """Starts agentic LLM analysis for one file or every file in a collection.

    Every function in each file
    (minus configurable pre-filters) gets the same context-aware tagging/notes
    pass as `contextual_batch`, escalating to a tool-using pass when a
    function's purpose isn't clear from context alone, then folds every
    function's finding into one whole-file report saved as a file note.

    Status and cancellation reuse `contextual_batch_status`/
    `contextual_batch_cancel` -- both key off the job hash and
    `analysis_orchestrator`'s per-job result set, neither of which cares which
    route created the job.
    """
    from bsimvis.app.services.analysis_orchestrator import max_batch_size
    from bsimvis.app.services.job_service import JobService, JobType
    from bsimvis.app.services.redis_client import get_redis
    from bsimvis.app.routes.llm import _resolve_filters_to_ids

    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        collection = f"global:pool:{pool}"
    file_md5 = data.get("file_md5")
    if not collection:
        return {"error": "Missing collection"}, 400

    actions = data.get("actions") or ["notes", "tags"]
    invalid = [action for action in actions if action not in ("notes", "tags")]
    if invalid:
        return {"error": f"Invalid actions: {', '.join(invalid)}"}, 400
    try:
        min_complexity = int(data.get("min_complexity") or 0)
    except (TypeError, ValueError):
        return {"error": "min_complexity must be an integer"}, 400
    if min_complexity < 0:
        return {"error": "min_complexity must be zero or greater"}, 400

    cap = max_batch_size()

    def payload_for(md5):
        filters_qs = f"md5={md5}"
        if data.get("skip_fid_tagged", True):
            filters_qs += "&exclude_tag=fid"
        if min_complexity:
            filters_qs += f"&min_features={min_complexity}"
        func_ids, error = _resolve_filters_to_ids(collection, filters_qs, cap)
        if error:
            return None, error
        marker = f":func:{md5}:"
        func_ids = [fid for fid in dict.fromkeys(func_ids) if fid and marker in fid]
        if cap > 0 and len(func_ids) > cap:
            return None, (
                f"File {md5} has {len(func_ids)} functions after filters, over "
                f"the batch cap of {cap}. Raise llm.batch_max, or set it to 0 "
                "for no cap."
            )
        if not func_ids:
            return None, None
        return {
            "collection": collection,
            "file_md5": md5,
            "func_ids": func_ids,
            "actions": actions,
            "overwrite": bool(data.get("overwrite")),
            "custom_prompt": data.get("custom_prompt"),
        }, None

    job_service = JobService()
    if file_md5:
        payload, error = payload_for(file_md5)
        if error:
            return {"error": error}, 413 if "batch cap" in error else 400
        if not payload:
            return {
                "error": "Selection resolved to zero functions (after filters)"
            }, 400
        job_id = job_service.create_job(JobType.LLM_FILE_ANALYSIS, payload)
        return {"job_id": job_id, "total": len(payload["func_ids"]), "files": 1}

    file_ids = get_redis().sscan_iter(f"{collection}:all_files")
    # ponytail: group creation still holds one small task payload per file;
    # switch to a discovery/continuation job only if huge collections make setup slow.
    md5s = sorted(
        (raw.decode() if isinstance(raw, bytes) else str(raw)).rsplit(":file:", 1)[-1]
        for raw in file_ids
    )
    tasks = []
    total = 0
    for md5 in md5s:
        payload, error = payload_for(md5)
        if error:
            return {"error": error}, 413 if "batch cap" in error else 400
        if payload:
            tasks.append((JobType.LLM_FILE_ANALYSIS, payload))
            total += len(payload["func_ids"])
    if not tasks:
        return {"error": "Collection resolved to zero functions (after filters)"}, 400

    job_id = job_service.create_group(tasks)
    return {"job_id": job_id, "total": total, "files": len(tasks)}
