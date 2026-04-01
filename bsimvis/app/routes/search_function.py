import json
import logging

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids, parse_timestamp

search_function_bp = Blueprint("search_function", __name__)

DEFAULT_LIMIT = 100


def normalize_tags(data):
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [t.strip() for t in tags.split(",")] if tags else []
    elif tags is None:
        data["tags"] = []
    return data


def get_true_total_functions(r, collection):
    return r.scard(f"idx:{collection}:indexed:functions")


@search_function_bp.route("/api/function/search")
def search_functions():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return jsonify({"error": "offset and limit must be integers"}), 400

    collection = request.args.get("collection")
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    # Build tag filters – collection is implicit in the key namespace
    tag_filters = {}
    for field in [
        "batch_uuid",
        "language_id",
        "file_md5",
        "file_name",
        "decompiler_id",
        "function_name",
        "return_type",
        "calling_convention",
        "entrypoint_address",
    ]:
        val = request.args.get(field)
        if val:
            tag_filters[field] = val

    tags = request.args.getlist("tag")
    if tags:
        tag_filters["tags"] = tags[0]

    doc_ids, total = query_ids(
        r, collection, "function", tag_filters=tag_filters, offset=offset, limit=limit
    )

    # Fetch full JSON for the page
    pipe = r.pipeline()
    for doc_id in doc_ids:
        pipe.json().get(doc_id, "$")
    raw_results = pipe.execute()

    functions_list = []
    for doc_id, raw in zip(doc_ids, raw_results):
        if not raw:
            continue
        data = raw[0] if isinstance(raw, list) and raw else raw

        # Extra tag filtering (multi-tag)
        if len(tags) > 1:
            doc_tags = data.get("tags", [])
            if isinstance(doc_tags, str):
                doc_tags = [t.strip() for t in doc_tags.split(",")]
            if not all(t in doc_tags for t in tags):
                continue

        col = data.get("collection", collection)
        md5 = data.get("file_md5")
        addr = data.get("entrypoint_address")
        b_uuid = data.get("batch_uuid")

        if col and md5 and addr and "function_id" not in data:
            data["function_id"] = f"{col}:function:{md5}:{addr}"
        if col and md5 and "file_id" not in data:
            data["file_id"] = f"{col}:file:{md5}"
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        normalize_tags(data)

        # Enforce Unix timestamps for UI
        for field in ["entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        functions_list.append(data)

    # Fall back to global counter if index not built yet
    if total == 0:
        total = get_true_total_functions(r, collection)

    return jsonify(
        {"total": total, "offset": offset, "limit": limit, "functions": functions_list}
    )
