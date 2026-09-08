import json
import logging
from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.routes import _list_query as lq


def search_collections():
    try:
        r = get_redis()

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", 100))
        except ValueError:
            return {"error": "offset and limit must be integers"}, 400

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

        # Hydrate ALL collection metas up front so we can filter/sort/paginate
        # in-memory (bounded to hundreds/low-thousands of collections).
        page_names = collection_names

        # 1. Fetch metadata hashes first
        pipe = r.pipeline(transaction=False)
        for name in page_names:
            pipe.hgetall(f"global:collection:{name}:meta")
        meta_results = pipe.execute()

        # 2. Identify collections that have missing cached statistics
        fetch_pipe = r.pipeline(transaction=False)
        fetch_jobs = (
            []
        )  # list of tuples: (collection_name, meta_decoded_dict, field_name)

        results_temp = []
        for i, name in enumerate(page_names):
            meta = meta_results[i] or {}
            meta_decoded = {}
            for k, v in meta.items():
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                v_str = v.decode() if isinstance(v, bytes) else str(v)
                meta_decoded[k_str] = v_str

            need_files = "total_files" not in meta_decoded
            need_functions = "total_functions" not in meta_decoded
            need_batches = "total_batches" not in meta_decoded
            need_last_updated = "last_updated" not in meta_decoded

            if need_files:
                fetch_pipe.scard(f"{name}:all_files")
                fetch_jobs.append((name, meta_decoded, "total_files"))
            if need_functions:
                fetch_pipe.scard(f"{name}:all_functions")
                fetch_jobs.append((name, meta_decoded, "total_functions"))
            if need_batches:
                fetch_pipe.scard(f"{name}:all_batches")
                fetch_jobs.append((name, meta_decoded, "total_batches"))
            if need_last_updated:
                fetch_pipe.zrange(
                    f"{name}:idx:file:entry_date", -1, -1, withscores=True
                )
                fetch_jobs.append((name, meta_decoded, "last_updated"))

            results_temp.append((name, meta_decoded))

        # 3. Fetch any missing fields and cache them back
        if fetch_jobs:
            fetched_vals = fetch_pipe.execute()
            updates_pipe = r.pipeline(transaction=False)
            for job_idx, (name, meta_decoded, field) in enumerate(fetch_jobs):
                val = fetched_vals[job_idx]
                if field == "last_updated":
                    last_updated = 0
                    if val:
                        try:
                            last_updated = int(val[0][1])
                        except (ValueError, TypeError, IndexError):
                            pass
                    if not last_updated:
                        raw_updated = meta_decoded.get("last_updated")
                        if raw_updated:
                            last_updated = parse_timestamp(raw_updated)
                    meta_decoded[field] = last_updated
                else:
                    meta_decoded[field] = int(val) if val else 0

                updates_pipe.hset(
                    f"global:collection:{name}:meta", field, meta_decoded[field]
                )
            updates_pipe.execute()

        # 4. Construct final results lists
        results = []
        for name, meta_decoded in results_temp:
            results.append(
                {
                    "name": name,
                    "total_files": int(meta_decoded.get("total_files", 0)),
                    "total_functions": int(meta_decoded.get("total_functions", 0)),
                    "total_batches": int(meta_decoded.get("total_batches", 0)),
                    "last_updated": int(meta_decoded.get("last_updated", 0)),
                }
            )

        # 5. Filter (q over name, plus specific-field filters), sort, paginate
        kws = lq.keywords()
        name_filter = request.args.get("name", "").lower().strip()
        ranges = {
            "total_files": lq.num_range("files"),
            "total_functions": lq.num_range("functions"),
            "total_batches": lq.num_range("batches"),
            "last_updated": lq.num_range("last_updated"),
        }
        results = [
            c
            for c in results
            if lq.matches_keywords(kws, c["name"])
            and (not name_filter or name_filter in c["name"].lower())
            and all(lq.in_range(c[f], rng) for f, rng in ranges.items())
        ]
        results, total = lq.sort_and_paginate(
            results,
            offset,
            limit,
            default_key="last_updated",
            default_reverse=True,
            key_fns={
                "name": lambda c: c["name"].lower(),
                "total_files": lambda c: c["total_files"],
                "total_functions": lambda c: c["total_functions"],
                "total_batches": lambda c: c["total_batches"],
                "last_updated": lambda c: c["last_updated"],
            },
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
    except Exception as e:
        logging.error(f"Error in search_collections: {e}", exc_info=True)
        return {"error": str(e)}, 500


def search_batches():
    try:
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

        pipe = r.pipeline(transaction=False)
        for uuid in batch_uuids:
            pipe.get(f"{target_collection}:batch:{uuid}")
        raw_data = pipe.execute()

        all_results = []
        keywords = [k for k in q.split() if k] if q else []

        for item in raw_data:
            if not item:
                continue
            data = json.loads(item) if not isinstance(item, dict) else item

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
    except Exception as e:
        logging.error(f"Error in search_batches: {e}", exc_info=True)
        return {"error": str(e)}, 500


def delete_collection():
    try:
        from flask import request
        from bsimvis.app.services.job_service import JobService, JobType

        # Accept parameters from request body (JSON) or query parameters
        data = request.json or {}
        collection = data.get("collection") or request.args.get("collection")
        if not collection:
            return {"error": "collection parameter is required"}, 400

        job_service = JobService()
        job_id = job_service.create_job(
            JobType.DELETE_COLLECTION, {"collection": collection}
        )
        return {"job_id": job_id, "status": "enqueued"}
    except Exception as e:
        logging.error(f"Error in delete_collection route: {e}", exc_info=True)
        return {"error": str(e)}, 500


def clean_collection():
    try:
        from flask import request
        from bsimvis.app.services.job_service import JobService, JobType

        # Accept parameters from request body (JSON) or query parameters
        data = request.json or {}
        collection = data.get("collection") or request.args.get("collection")
        if not collection:
            return {"error": "collection parameter is required"}, 400

        job_service = JobService()
        job_id = job_service.create_job(
            JobType.CLEAN_COLLECTION, {"collection": collection}
        )
        return {"job_id": job_id, "status": "enqueued"}
    except Exception as e:
        logging.error(f"Error in clean_collection route: {e}", exc_info=True)
        return {"error": str(e)}, 500
