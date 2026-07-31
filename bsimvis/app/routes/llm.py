from flask import request, Response, stream_with_context
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.function_service import fetch_function_data
import json
import logging


def get_code_for_llm(func_id):
    """Helper to fetch raw code for a function ID."""
    try:
        parts = func_id.split(":")
        if len(parts) < 4:
            return None, "Invalid ID format"

        if parts[0] == "idx":
            collection = parts[1]
            md5 = parts[3]
            addr = parts[4]
        else:
            collection = parts[0]
            md5 = parts[2]
            addr = parts[3]

        source, _, meta, _ = fetch_function_data(collection, md5, addr)
        if not source:
            return None, "Function not found"

        # Use raw decompiled lines if available
        c_lines = source.get("c_lines")
        if c_lines:
            code = "\n".join(c_lines)
        else:
            # Fallback to reconstructing from tokens if c_lines is missing
            tokens = source.get("c_tokens", [])
            if not tokens:
                return None, "No tokens or lines found"

            # Group tokens by line
            max_line = max(t["line"] for t in tokens)
            lines = [[] for _ in range(max_line + 1)]
            for t in tokens:
                lines[t["line"]].append(t["t"])
            code = "\n".join(["".join(line_tokens) for line_tokens in lines])

        func_name = meta.get("function_name", "unknown") if meta else "unknown"

        return {"code": code, "func_name": func_name}, None
    except Exception as e:
        return None, str(e)


def summarize():
    data = request.json
    func_id = data.get("func_id")
    custom_prompt = data.get("prompt")
    code = data.get("code")
    func_name = data.get("func_name")

    if not code and func_id:
        res, error = get_code_for_llm(func_id)
        if error:
            return {"error": error}, 400
        code = res["code"]
        func_name = res["func_name"]

    if not code:
        return {"error": "Missing code or func_id"}, 400

    @stream_with_context
    def generate():
        logging.info("Starting LLM stream generator...")
        for chunk in llm_service.stream_summarize_function(
            func_name or "unknown", code, custom_prompt
        ):
            logging.info(f"Yielding chunk to response: {len(chunk)} chars")
            yield chunk
        logging.info("LLM stream generator finished.")

    resp = Response(generate(), mimetype="text/plain")
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


def chat():
    data = request.json
    messages = data.get("messages", [])
    if not messages:
        return {"error": "Missing messages"}, 400

    @stream_with_context
    def generate():
        logging.info("Starting LLM chat stream generator...")
        for chunk in llm_service.stream_chat(messages):
            logging.info(f"Yielding chat chunk to response: {len(chunk)} chars")
            yield chunk
        logging.info("LLM chat stream generator finished.")

    resp = Response(generate(), mimetype="text/plain")
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


def _resolve_filters_to_ids(collection, filters, cap):
    """Resolves function-search filters to a list of function ids.

    `filters` is the same query string the function search page uses, so any
    filtered result set is directly batchable without duplicating the ~600
    lines of filter parsing in search_function.py.
    """
    from flask import current_app
    from werkzeug.datastructures import MultiDict
    from bsimvis.app.routes.search_function import search_functions

    if isinstance(filters, str):
        args = MultiDict(url_decode(filters))
    else:
        args = MultiDict()
        for k, v in (filters or {}).items():
            if isinstance(v, (list, tuple)):
                for item in v:
                    args.add(k, item)
            else:
                args.add(k, v)

    args.setlist("collection", [collection])
    # One page, capped: a filter matching 200k functions must be refused up
    # front, so ask for cap+1 and let the caller detect the overflow.
    args.setlist("limit", [str(cap + 1)])
    args.setlist("offset", ["0"])
    args.setlist("format", [""])

    with current_app.test_request_context(
        "/api/function/search", query_string=args.to_dict(flat=False)
    ):
        result = search_functions()

    if isinstance(result, tuple):
        return None, result[0].get("error", "Function search failed")
    if not isinstance(result, dict) or "functions" not in result:
        return None, "Function search returned no result set"

    ids = []
    for f in result["functions"]:
        fid = f.get("function_id") or f.get("id")
        if fid:
            ids.append(fid)
    return ids, None


def url_decode(qs):
    from urllib.parse import parse_qs

    return parse_qs(qs, keep_blank_values=True)


def batch():
    """Starts a background LLM enrichment job over a set of functions."""
    from bsimvis.app.services.job_service import JobService, JobType
    from bsimvis.app.services.llm_batch_service import max_batch_size

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

    actions = data.get("actions") or ["notes"]
    invalid = [a for a in actions if a not in ("notes", "tags")]
    if invalid:
        return {"error": f"Invalid actions: {', '.join(invalid)}"}, 400

    cap = max_batch_size()
    func_ids = data.get("func_ids")

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

    if len(func_ids) > cap:
        return {
            "error": (
                f"Selection of {len(func_ids)} functions exceeds the batch cap "
                f"of {cap}. Narrow the filter or raise llm.batch_max."
            ),
            "count": len(func_ids),
            "cap": cap,
        }, 413

    job_service = JobService()
    job_id = job_service.create_job(
        JobType.LLM_BATCH,
        {
            "collection": collection,
            "func_ids": func_ids,
            "actions": actions,
            "overwrite": bool(data.get("overwrite")),
            "custom_prompt": data.get("custom_prompt") or data.get("prompt_template"),
            "tag_vocabulary": data.get("tag_vocabulary"),
        },
    )

    return {"job_id": job_id, "total": len(func_ids), "actions": actions}


def batch_status(job_id):
    """Progress, per-function state and errors for an LLM batch job."""
    from bsimvis.app.services.job_service import JobService
    from bsimvis.app.services.llm_batch_service import llm_batch_service

    job_service = JobService()
    job = job_service.r.hgetall(f"job:{job_id}")
    if not job:
        return {"error": "Job not found"}, 404

    results = llm_batch_service.get_results(job_id)
    counts = {"done": 0, "skipped": 0, "failed": 0}
    errors = []
    for fid, res in results.items():
        state = res.get("state")
        counts[state] = counts.get(state, 0) + 1
        if state == "failed":
            errors.append({"func_id": fid, "error": res.get("detail")})

    payload = {}
    try:
        payload = json.loads(job.get("payload") or "{}")
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


def batch_cancel(job_id):
    """Cancels a running or pending LLM batch job."""
    from bsimvis.app.services.job_service import JobService

    if not JobService().cancel_job(job_id):
        return {"error": "Job not found"}, 404
    return {"status": "cancelled", "job_id": job_id}


def summarize_file():
    """Streams an LLM threat-intel summary for a binary file."""
    data = request.json
    file_id = data.get("file_id")
    if not file_id:
        return {"error": "Missing file_id"}, 400

    # Parse collection and md5 from file_id ({col}:file:{md5})
    parts = file_id.split(":")
    if len(parts) < 3 or parts[1] != "file":
        return {"error": "Invalid file_id format, expected {col}:file:{md5}"}, 400

    collection = parts[0]
    md5 = parts[2]

    from bsimvis.app.services.redis_client import get_redis
    from bsimvis.app.services.config_service import config_service
    import json

    r = get_redis()

    # Fetch file meta
    raw = r.get(f"{collection}:file:{md5}:meta")
    if not raw:
        return {"error": "File not found"}, 404
    file_meta = json.loads(raw) if not isinstance(raw, dict) else raw
    if isinstance(file_meta, str):
        file_meta = json.loads(file_meta)

    # Fetch cluster membership and metadata
    cluster_ids_raw = r.smembers(f"{collection}:file:{md5}:bin_clusters")
    cluster_ids = [
        c.decode() if isinstance(c, bytes) else c for c in (cluster_ids_raw or [])
    ]

    algo = "unweighted_cosine"
    min_cohesion = float(config_service.get("clustering.min_cohesion", 0.5))
    clusters = []

    if cluster_ids:
        pipe = r.pipeline(transaction=False)
        for cid in cluster_ids:
            pipe.get(f"{collection}:bin_cluster:{algo}:{cid}:meta")
        results = pipe.execute()
        for cid, res in zip(cluster_ids, results):
            cm = json.loads(res) if res and not isinstance(res, dict) else (res or {})
            if isinstance(cm, str):
                cm = json.loads(cm)
            if (cm.get("cohesion_score") or 0) >= min_cohesion:
                clusters.append(cm)

    def _to_set(val):
        """Normalizes a field that may be a list, string, or None into a flat set of strings."""
        if not val:
            return set()
        if isinstance(val, list):
            return set(v for v in val if v and isinstance(v, str))
        return {val}

    inferred_meta = {k: {} for k in ["yara", "avtype", "filetype", "ccip", "filename"]}
    existing = {
        "yara": _to_set(file_meta.get("yara")),
        "avtype": _to_set(file_meta.get("avtype")),
        "filetype": _to_set(file_meta.get("filetype")),
        "ccip": _to_set(file_meta.get("cc_ip")),
        "filename": _to_set(file_meta.get("file_names"))
        | _to_set(file_meta.get("file_name")),
    }

    for cm in clusters:
        cohesion_pct = round((cm.get("cohesion_score") or 0) * 100)
        dist_map = {
            "yara_distribution": "yara",
            "avtype_distribution": "avtype",
            "filetype_distribution": "filetype",
            "ccip_distribution": "ccip",
            "filename_distribution": "filename",
        }
        for dist_key, meta_key in dist_map.items():
            for item in cm.get(dist_key) or []:
                val = item.get("value")
                if not val or val in existing[meta_key]:
                    continue
                if (
                    val not in inferred_meta[meta_key]
                    or inferred_meta[meta_key][val]["percent"] < cohesion_pct
                ):
                    inferred_meta[meta_key][val] = {"percent": cohesion_pct}

    @stream_with_context
    def generate():
        logging.info(f"Starting LLM file summary stream for {file_id}...")
        for chunk in llm_service.stream_summarize_file(
            file_meta, clusters, inferred_meta
        ):
            yield chunk
        logging.info("LLM file summary stream finished.")

    resp = Response(generate(), mimetype="text/plain")
    resp.headers["X-Accel-Buffering"] = "no"
    return resp
