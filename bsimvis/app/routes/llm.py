from flask import request, Response, stream_with_context
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.function_service import fetch_function_data
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
        for chunk in llm_service.stream_summarize_function(func_name or "unknown", code, custom_prompt):
            logging.info(f"Yielding chunk to response: {len(chunk)} chars")
            yield chunk
        logging.info("LLM stream generator finished.")

    resp = Response(generate(), mimetype='text/plain')
    resp.headers['X-Accel-Buffering'] = 'no'
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

    resp = Response(generate(), mimetype='text/plain')
    resp.headers['X-Accel-Buffering'] = 'no'
    return resp


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
    raw = r.json().get(f"{collection}:file:{md5}:meta", "$")
    if not raw:
        return {"error": "File not found"}, 404
    file_meta = raw[0] if isinstance(raw, list) else raw
    if isinstance(file_meta, str):
        file_meta = json.loads(file_meta)

    # Fetch cluster membership and metadata
    cluster_ids_raw = r.smembers(f"{collection}:file:{md5}:bin_clusters")
    cluster_ids = [c.decode() if isinstance(c, bytes) else c for c in (cluster_ids_raw or [])]

    algo = "unweighted_cosine"
    min_cohesion = float(config_service.get("clustering.min_cohesion", 0.5))
    clusters = []

    if cluster_ids:
        pipe = r.pipeline()
        for cid in cluster_ids:
            pipe.json().get(f"{collection}:bin_cluster:{algo}:{cid}:meta", "$")
        results = pipe.execute()
        for cid, res in zip(cluster_ids, results):
            cm = (res[0] if isinstance(res, list) and res else res) or {}
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
        "filename": _to_set(file_meta.get("file_names")) | _to_set(file_meta.get("file_name")),
    }

    for cm in clusters:
        cohesion_pct = round((cm.get("cohesion_score") or 0) * 100)
        dist_map = {
            "yara_distribution": "yara", "avtype_distribution": "avtype",
            "filetype_distribution": "filetype", "ccip_distribution": "ccip",
            "filename_distribution": "filename",
        }
        for dist_key, meta_key in dist_map.items():
            for item in (cm.get(dist_key) or []):
                val = item.get("value")
                if not val or val in existing[meta_key]:
                    continue
                if val not in inferred_meta[meta_key] or inferred_meta[meta_key][val]["percent"] < cohesion_pct:
                    inferred_meta[meta_key][val] = {"percent": cohesion_pct}

    @stream_with_context
    def generate():
        logging.info(f"Starting LLM file summary stream for {file_id}...")
        for chunk in llm_service.stream_summarize_file(file_meta, clusters, inferred_meta):
            yield chunk
        logging.info("LLM file summary stream finished.")

    resp = Response(generate(), mimetype='text/plain')
    resp.headers['X-Accel-Buffering'] = 'no'
    return resp

