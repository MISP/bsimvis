from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
import json

search_collection_bp = Blueprint("search_collection", __name__)


@search_collection_bp.route("/api/collection/search")
def search_collections():
    r = get_redis()
    collection_names = r.smembers("global:collections")

    pipe = r.pipeline()
    for name in collection_names:
        pipe.hgetall(f"global:collection:{name}:meta")
    metas = pipe.execute()

    results = []
    for name, meta in zip(collection_names, metas):
        results.append(
            {
                "name": name,
                "total_files": int(meta.get("total_files", 0)),
                "total_functions": int(meta.get("total_functions", 0)),
                "last_updated": meta.get("last_updated", "N/A"),
            }
        )

    return jsonify(results)


@search_collection_bp.route("/api/batch/search")
def search_batches():
    r = get_redis()
    target_collection = request.args.get("collection")
    if not target_collection:
        return jsonify({"error": "No collection specified"})

    batch_uuids = r.smembers("global:batches")

    pipe = r.pipeline()
    for uuid in batch_uuids:
        pipe.json().get(f"{target_collection}:batch:{uuid}", "$")
    raw_data = pipe.execute()

    results = []
    for item in raw_data:
        if not item:
            continue
        data = item[0] if isinstance(item, list) and item else item
        data = json.loads(data) if isinstance(data, str) else data
        col = data.get("collection") or target_collection
        b_uuid = data.get("batch_uuid")
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"
        results.append(data)

    results.sort(key=lambda x: x.get("last_updated", ""), reverse=True)
    return jsonify(results)
