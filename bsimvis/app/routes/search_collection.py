from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp
import json


def search_collections():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return {"error": "offset and limit must be integers"}, 400

    q = request.args.get("q", "").lower().strip()
    format_arg = request.args.get("format")
    if format_arg in ("csv", "json"):
        offset = 0
        limit = 100000

    collection_names = sorted(
        [
            n.decode() if isinstance(n, bytes) else str(n)
            for n in r.smembers("global:collections")
        ]
    )

    if q:
        keywords = [k for k in q.split() if k]
        filtered_names = []
        for name in collection_names:
            name_lower = name.lower()
            if all(kw in name_lower for kw in keywords):
                filtered_names.append(name)
        collection_names = filtered_names

    total = len(collection_names)
    page_names = collection_names[offset : offset + limit]

    pipe = r.pipeline()
    for name in page_names:
        pipe.scard(f"{name}:all_files")
        pipe.scard(f"{name}:all_functions")
        pipe.zrange(f"{name}:idx:file:entry_date", -1, -1, withscores=True)
        pipe.hgetall(f"global:collection:{name}:meta")
    pipe_results = pipe.execute()

    results = []
    for i, name in enumerate(page_names):
        idx = i * 4
        total_files = pipe_results[idx]
        total_functions = pipe_results[idx + 1]
        zrange_res = pipe_results[idx + 2]
        meta = pipe_results[idx + 3] or {}

        # Determine last_updated from latest file entry_date
        last_updated = 0
        if zrange_res:
            try:
                # zrange_res is a list of tuples: [(member, score)]
                last_updated = int(zrange_res[0][1])
            except (ValueError, TypeError, IndexError):
                pass

        # Fallback to meta hash last_updated if zrange_res was empty
        if not last_updated:
            last_updated_raw = meta.get("last_updated")
            if last_updated_raw:
                last_updated = parse_timestamp(last_updated_raw)

        results.append(
            {
                "name": name,
                "total_files": int(total_files) if total_files else 0,
                "total_functions": int(total_functions) if total_functions else 0,
                "last_updated": last_updated,
            }
        )

    response_data = {
        "collections": results,
        "total": total,
        "offset": offset,
        "limit": limit,
    }
    if format_arg == "csv":
        from bsimvis.app.services.export_service import export_to_csv

        return export_to_csv(results, "collections")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "collections")
    else:
        return response_data


def search_batches():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return {"error": "offset and limit must be integers"}, 400

    target_collection = request.args.get("collection")
    if not target_collection:
        return {"error": "No collection specified"}, 400

    q = request.args.get("q", "").lower().strip()
    format_arg = request.args.get("format")
    if format_arg in ("csv", "json"):
        offset = 0
        limit = 100000

    batch_uuids = list(r.smembers("global:batches"))

    pipe = r.pipeline()
    for uuid in batch_uuids:
        pipe.json().get(f"{target_collection}:batch:{uuid}", "$")
    raw_data = pipe.execute()

    all_results = []
    keywords = [k for k in q.split() if k] if q else []

    for item in raw_data:
        if not item:
            continue
        data = item[0] if isinstance(item, list) and item else item
        data = json.loads(data) if isinstance(data, str) else data

        # Apply q filter
        if keywords:
            b_uuid = str(data.get("batch_uuid", "")).lower()
            b_name = str(data.get("batch_name", "")).lower()
            b_col = str(data.get("collection", "")).lower()

            match = True
            for kw in keywords:
                if not (kw in b_uuid or kw in b_name or kw in b_col):
                    match = False
                    break
            if not match:
                continue

        col = data.get("collection") or target_collection
        b_uuid = data.get("batch_uuid")
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        # Ensure Unix timestamps for UI
        for field in ["last_updated", "created_at", "entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        all_results.append(data)

    all_results.sort(key=lambda x: x.get("last_updated", 0), reverse=True)

    total = len(all_results)
    page = all_results[offset : offset + limit]

    response_data = {
        "batches": page,
        "total": total,
        "offset": offset,
        "limit": limit,
    }
    if format_arg == "csv":
        from bsimvis.app.services.export_service import export_to_csv

        return export_to_csv(page, "batches")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "batches")
    else:
        return response_data
