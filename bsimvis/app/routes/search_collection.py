from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
import json

search_collection_bp = Blueprint("search_collection", __name__)


@search_collection_bp.route("/api/collection/search")
def search_collections():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return jsonify({"error": "offset and limit must be integers"}), 400

    collection_names = sorted(list(r.smembers("global:collections")))
    total = len(collection_names)
    page_names = collection_names[offset : offset + limit]

    pipe = r.pipeline()
    for name in page_names:
        pipe.hgetall(f"global:collection:{name}:meta")
    metas = pipe.execute()

    results = []
    for name, meta in zip(page_names, metas):
        results.append(
            {
                "name": name,
                "total_files": int(meta.get("total_files", 0)),
                "total_functions": int(meta.get("total_functions", 0)),
                "last_updated": meta.get("last_updated", "N/A"),
            }
        )

    return jsonify(
        {"collections": results, "total": total, "offset": offset, "limit": limit}
    )


@search_collection_bp.route("/api/batch/search")
def search_batches():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return jsonify({"error": "offset and limit must be integers"}), 400

    target_collection = request.args.get("collection")
    if not target_collection:
        return jsonify({"error": "No collection specified"}), 400

    batch_uuids = list(r.smembers("global:batches"))

    pipe = r.pipeline()
    for uuid in batch_uuids:
        pipe.json().get(f"{target_collection}:batch:{uuid}", "$")
    raw_data = pipe.execute()

    all_results = []
    for item in raw_data:
        if not item:
            continue
        data = item[0] if isinstance(item, list) and item else item
        data = json.loads(data) if isinstance(data, str) else data
        col = data.get("collection") or target_collection
        b_uuid = data.get("batch_uuid")
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"
        all_results.append(data)

    all_results.sort(key=lambda x: x.get("last_updated", ""), reverse=True)

    total = len(all_results)
    page = all_results[offset : offset + limit]

    return jsonify({"batches": page, "total": total, "offset": offset, "limit": limit})
