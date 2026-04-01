import json
import logging
import re

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids, parse_timestamp

search_file_bp = Blueprint("search_file", __name__)

DEFAULT_LIMIT = 100


def normalize_tags(data):
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [t.strip() for t in tags.split(",")] if tags else []
    elif tags is None:
        data["tags"] = []
    return data


def get_true_total_files(r, collection):
    total = r.hget(f"global:collection:{collection}:meta", "total_files")
    return int(total) if total else 0


@search_file_bp.route("/api/file/search")
def search_files():
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
    for field in ["batch_uuid", "language_id", "file_md5"]:
        val = request.args.get(field)
        if val:
            tag_filters[field] = val

    file_name = request.args.get("file_name")
    if file_name:
        tag_filters["file_name"] = file_name

    tags = request.args.getlist("tag")
    if tags:
        tag_filters["tags"] = tags[0]

    doc_ids, total = query_ids(
        r, collection, "file", tag_filters=tag_filters, offset=offset, limit=limit
    )

    # Fetch full JSON for the page
    pipe = r.pipeline()
    for doc_id in doc_ids:
        pipe.json().get(doc_id, "$")
    raw_results = pipe.execute()

    files_list = []
    for doc_id, raw in zip(doc_ids, raw_results):
        if not raw:
            continue
        data = raw[0] if isinstance(raw, list) and raw else raw

        # Extra tag filtering (for multi-tag)
        if len(tags) > 1:
            doc_tags = data.get("tags", [])
            if isinstance(doc_tags, str):
                doc_tags = [t.strip() for t in doc_tags.split(",")]
            if not all(t in doc_tags for t in tags):
                continue

        col = data.get("collection", collection)
        md5 = data.get("file_md5")
        b_uuid = data.get("batch_uuid")
        if col and md5 and "file_id" not in data:
            data["file_id"] = f"{col}:file:{md5}"
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        normalize_tags(data)

        # Enforce Unix timestamps for UI
        for field in ["entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        files_list.append(data)

    # If total is 0 from index (index may not be built yet) fall back to global meta
    if total == 0 and not any(tag_filters.values()):
        total = get_true_total_files(r, collection)

    return jsonify(
        {"total": total, "offset": offset, "limit": limit, "files": files_list}
    )
