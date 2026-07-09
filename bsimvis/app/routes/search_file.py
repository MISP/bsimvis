import json
import logging
import re
import time

from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import (
    query_ids,
    parse_timestamp,
    normalize_tags,
    enrich_pool_data,
    get_pool_id,
)

DEFAULT_LIMIT = 100


def search_files():
    try:
        t_start = time.perf_counter()
        pool_id = request.args.get("pool")
        col = request.args.get("collection")
        if not col and not pool_id:
            return {"error": "No collection or pool specified"}, 400

        if pool_id:
            col = f"global:pool:{pool_id}"
        else:
            pool_id = get_pool_id(col)

        r = get_redis()
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
        format_arg = request.args.get("format")

        if format_arg in ("csv", "json"):
            offset = 0
            limit = 100000

        # Filtering parameters
        filters = {
            "fields": {},
            "tags": {
                "include": [],
                "exclude": [],
                "static": [],
                "exclude_static": [],
                "user": [],
                "exclude_user": [],
            },
        }

        for arg, field in [
            ("q", "q"),
            ("file_name", "file_name"),
            ("file_md5", "file_md5"),
            ("md5", "file_md5"),
            ("language_id", "language_id"),
            ("language", "language_id"),
            ("batch_uuid", "batch_uuid"),
            ("bin_cluster_uuid", "bin_cluster_uuid"),
            ("bin_cluster_name", "bin_cluster_name"),
            ("first_seen", "first_seen"),
            ("last_seen", "last_seen"),
            ("filetype", "filetype"),
            ("avtype", "avtype"),
            ("yara", "yara"),
            ("cc_ip", "cc_ip"),
            ("file_names", "file_names"),
            ("inferred_yara", "inferred_yara"),
            ("inferred_avtype", "inferred_avtype"),
            ("inferred_filetype", "inferred_filetype"),
            ("inferred_ccip", "inferred_ccip"),
            ("inferred_filename", "inferred_filename"),
            ("inferred_md5", "inferred_md5"),
            ("note_owner", "note_owners"),
            ("note_owners", "note_owners"),
        ]:
            val = request.args.get(arg)
            if val:
                filters["fields"][field] = val.strip()

        # Range fields
        for arg, field in [
            ("min_function_count", "function_count"),
            ("max_function_count", "function_count"),
            ("min_bsim_features", "bsim_features_count"),
            ("max_bsim_features", "bsim_features_count"),
            ("min_cohesion", "cohesion_score"),
            ("max_cohesion", "cohesion_score"),
            ("min_entry_date", "entry_date"),
            ("max_entry_date", "entry_date"),
        ]:
            val = request.args.get(arg)
            if val:
                filters["fields"][arg] = val.strip()

        # Tags
        filters["tags"]["include"] = [
            t.strip()
            for t in request.args.getlist("tag") + request.args.getlist("file_tag")
            if t.strip()
        ]
        filters["tags"]["static"] = [
            t.strip()
            for t in request.args.getlist("static_tag")
            + request.args.getlist("file_static_tag")
            if t.strip()
        ]
        filters["tags"]["user"] = [
            t.strip()
            for t in request.args.getlist("user_tag")
            + request.args.getlist("file_user_tag")
            if t.strip()
        ]
        filters["tags"]["exclude"] = [
            t.strip()
            for t in request.args.getlist("exclude_tag")
            + request.args.getlist("exclude_file_tag")
            if t.strip()
        ]
        filters["tags"]["exclude_static"] = [
            t.strip()
            for t in request.args.getlist("exclude_static_tag")
            + request.args.getlist("exclude_file_static_tag")
            if t.strip()
        ]
        filters["tags"]["exclude_user"] = [
            t.strip()
            for t in request.args.getlist("exclude_user_tag")
            + request.args.getlist("exclude_file_user_tag")
            if t.strip()
        ]

        sort_by = request.args.get("sort_by", "file_name")
        sort_order = request.args.get("sort_order", "asc").lower()

        # 1. Fetch filtered IDs
        t0 = time.perf_counter()
        doc_ids = query_files_advanced(r, col, filters)
        t1 = time.perf_counter()

        # 2. Sort — walk the pre-built numeric ZSET keeping only candidates
        # (same idiom as search_bin_sim) for global order across pages.
        total = len(doc_ids)
        ordered_ids = sort_doc_ids(r, col, doc_ids, sort_by, sort_order)

        # 3. Paginate
        paged_ids = ordered_ids[offset : offset + limit]

        # 4. Fetch full JSON, function counts, and cluster assignments for the page
        pipe = r.pipeline(transaction=False)
        for doc_id in paged_ids:
            pipe.get(f"{doc_id}:meta")
            actual_col = doc_id.split(":")[0]
            md5 = doc_id.split(":")[-1]
            pipe.scard(f"{actual_col}:idx:file:functions:{md5}")
            if pool_id:
                pipe.smembers(f"pool:{pool_id}:file:{md5}:bin_clusters")
            else:
                pipe.smembers(f"{doc_id}:bin_clusters")

        results = pipe.execute()
        t2 = time.perf_counter()
        files_list = []
        unique_cluster_ids = set()

        # First pass: collect results and unique cluster IDs
        raw_files_data = []
        for i, doc_id in enumerate(paged_ids):
            res = results[3 * i]
            func_count = results[3 * i + 1]
            cluster_res = results[3 * i + 2]

            if not res:
                continue

            data = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(data, str):
                data = json.loads(data)

            data["function_count"] = func_count
            data["file_id"] = doc_id
            cluster_ids = (
                list(cluster_res) if isinstance(cluster_res, (list, set)) else []
            )
            data["bin_clusters"] = cluster_ids
            for cid in cluster_ids:
                unique_cluster_ids.add(cid)

            raw_files_data.append(data)

        # Second pass: fetch cluster metadata
        cluster_meta_map = {}
        from bsimvis.app.services.config_service import config_service

        min_cohesion = float(
            request.args.get(
                "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
            )
        )
        t3 = t2  # default: no cluster fetch
        if unique_cluster_ids:
            is_pool = pool_id is not None
            algo = "unweighted_cosine"  # Assuming default algo
            c_pipe = r.pipeline(transaction=False)
            c_list = list(unique_cluster_ids)
            for cid in c_list:
                if is_pool:
                    c_pipe.get(f"global:pool:{pool_id}:bin_cluster:{cid}:meta")
                else:
                    c_pipe.get(f"{col}:bin_cluster:{algo}:{cid}:meta")
            c_results = c_pipe.execute()
            t3 = time.perf_counter()
            for cid, res in zip(c_list, c_results):
                cm = (
                    json.loads(res)
                    if res and not isinstance(res, dict)
                    else (res or {})
                )
                if isinstance(cm, str):
                    cm = json.loads(cm)

                # Apply cohesion filter
                if (cm.get("cohesion_score") or 0) >= min_cohesion:
                    cluster_meta_map[cid] = cm

        # Third pass: finalize files list
        for data in raw_files_data:
            # Map IDs to metadata
            # We don't map it here anymore, we send the map separately
            normalize_tags(data)

            if pool_id:
                enrich_pool_data(data, pool_id)
            # Ensure dates are Unix timestamps
            for date_field in ["entry_date", "file_date"]:
                if date_field in data:
                    data[date_field] = parse_timestamp(data[date_field])

            files_list.append(data)

        response_data = {
            "total": total,
            "offset": offset,
            "limit": limit,
            "files": files_list,
            "bin_cluster_map": cluster_meta_map,
            "collection": col,
            "total_files_in_collection": get_true_total_files(r, col),
        }

        if format_arg == "csv":
            from bsimvis.app.services.export_service import export_to_csv

            return export_to_csv(files_list, "files")
        elif format_arg == "json":
            from bsimvis.app.services.export_service import export_to_json

            return export_to_json(response_data, "files")
        else:
            logging.info(
                f"FILE SEARCH | filter:{t1-t0:.3f}s | meta_fetch:{t2-t1:.3f}s | cluster_fetch:{t3-t2:.3f}s | TOTAL:{time.perf_counter()-t_start:.3f}s | count={total}"
            )
            return response_data
    except Exception as e:
        logging.error(f"Error in search_files: {e}", exc_info=True)
        return {"error": str(e)}, 500


def get_true_total_files(r, collection):
    total = r.hget(f"global:collection:{collection}:meta", "total_files")
    return int(total) if total else 0


# Numeric file fields with a pre-built ZSET `{col}:idx:file:{field}` keyed by doc_id.
SORTABLE_ZSET_FIELDS = {
    "function_count",
    "bsim_features_count",
    "cohesion_score",
    "entry_date",
    "file_date",
}


def sort_doc_ids(r, collection, doc_ids, sort_by, sort_order):
    """Order a candidate set by a numeric ZSET field, keeping only candidates.

    Non-numeric sorts (e.g. file_name) have no ZSET, so fall back to arbitrary
    set order. ponytail: walks the full ZSET once (O(N)); fine at current sizes,
    switch to per-id pipelined ZSCORE if the ZSET dwarfs the candidate set.
    """
    if sort_by not in SORTABLE_ZSET_FIELDS:
        return list(doc_ids)

    zset_key = f"{collection}:idx:file:{sort_by}"
    desc = sort_order == "desc"
    ranked = r.zrange(zset_key, 0, -1, desc=desc)
    ranked = [d.decode() if isinstance(d, bytes) else str(d) for d in ranked]

    candidates = set(doc_ids)
    ordered = [d for d in ranked if d in candidates]
    # Candidates missing from the ZSET (no score) go last, arbitrary order.
    ordered.extend(candidates - set(ordered))
    return ordered


RANGE_FIELD_MAP = {
    "min_function_count": "function_count",
    "max_function_count": "function_count",
    "min_bsim_features": "bsim_features_count",
    "max_bsim_features": "bsim_features_count",
    "min_entry_date": "entry_date",
    "max_entry_date": "entry_date",
}


def query_files_advanced(r, collection, filters):
    fields = filters.get("fields", {})

    # Seed candidates from a numeric range ZSET when a range filter is present,
    # avoiding the full all_files load. The range filters below still intersect
    # (harmless: seeding from one bound, they refine the rest).
    # ponytail: O(N) full-set load only on the unfiltered path; add more indexes
    # or a Lua path if broad queries get slow.
    seed_field = next((f for f in fields if f in RANGE_FIELD_MAP), None)
    if seed_field:
        zset_key = f"{collection}:idx:file:{RANGE_FIELD_MAP[seed_field]}"
        is_min = seed_field.startswith("min_")
        lo, hi = (fields[seed_field], "+inf") if is_min else ("-inf", fields[seed_field])
        candidates = {
            d.decode() if isinstance(d, bytes) else str(d)
            for d in r.zrange(zset_key, lo, hi, byscore=True)
        }
    else:
        candidates = {
            d.decode() if isinstance(d, bytes) else str(d)
            for d in r.smembers(f"{collection}:all_files")
        }

    # Helper: Get all doc IDs matching a substring in a specific field registry
    def get_field_matches(field_name, search_val, field_level="file"):
        registry_key = f"{collection}:reg:{field_level}:{field_name}"
        val_lower = search_val.lower()
        matching_buckets = []
        try:
            for bucket in r.sscan_iter(
                registry_key, match=f"*{val_lower}*", count=1000
            ):
                bucket_str = (
                    bucket.decode() if isinstance(bucket, bytes) else str(bucket)
                )
                if val_lower in bucket_str.lower():
                    matching_buckets.append(bucket_str)
        except Exception as e:
            logging.warning(f"Registry SSCAN failed for {registry_key}: {e}")

        field_candidates = set()
        if matching_buckets:
            if len(matching_buckets) == 1:
                field_candidates = {
                    t.decode() if isinstance(t, bytes) else str(t)
                    for t in r.smembers(matching_buckets[0])
                }
            else:
                field_candidates = {
                    t.decode() if isinstance(t, bytes) else str(t)
                    for t in r.sunion(*matching_buckets)
                }
        return field_candidates

    # Apply Metadata Filters
    for field, val in fields.items():
        if field == "q":
            q_matches = set()
            # Search across all indexed file fields
            from bsimvis.app.services.index_config import INDEX_CONFIG

            for f_name, targets in INDEX_CONFIG.get("file", {}).items():
                if "file" in targets:
                    q_matches.update(get_field_matches(f_name, val))
            candidates &= q_matches
        elif field in [
            "file_name",
            "file_md5",
            "language_id",
            "batch_uuid",
            "bin_cluster_name",
            "bin_cluster_uuid",
            "first_seen",
            "last_seen",
            "filetype",
            "avtype",
            "yara",
            "cc_ip",
            "file_names",
            "inferred_yara",
            "inferred_avtype",
            "inferred_filetype",
            "inferred_ccip",
            "inferred_filename",
            "inferred_md5",
            "note_owners",
        ]:
            candidates &= get_field_matches(field, val)

    # Apply Numeric Range Filters
    for field, val in fields.items():
        if field.startswith("min_") or field.startswith("max_"):
            try:
                zset_field = RANGE_FIELD_MAP.get(field)
                if not zset_field:
                    continue

                zset_key = f"{collection}:idx:file:{zset_field}"
                is_min = field.startswith("min_")

                # We need to find the intersection of current candidates and the range
                # Redis doesn't have a direct "ZSET range intersect SET" but we can fetch the IDs
                if is_min:
                    range_ids = {
                        d.decode() if isinstance(d, bytes) else str(d)
                        for d in r.zrange(zset_key, val, "+inf", byscore=True)
                    }
                else:
                    range_ids = {
                        d.decode() if isinstance(d, bytes) else str(d)
                        for d in r.zrange(zset_key, "-inf", val, byscore=True)
                    }
                candidates &= range_ids
            except Exception as e:
                logging.warning(f"Range filter failed for {field}={val}: {e}")

    # Apply Tag Filters
    tags = filters.get("tags", {})
    for t in tags.get("include", []):
        candidates &= get_field_matches("tags", t) | get_field_matches("user_tags", t)
    for t in tags.get("static", []):
        candidates &= get_field_matches("tags", t)
    for t in tags.get("user", []):
        candidates &= get_field_matches("user_tags", t)

    for t in tags.get("exclude", []):
        candidates -= get_field_matches("tags", t) | get_field_matches("user_tags", t)
    for t in tags.get("exclude_static", []):
        candidates -= get_field_matches("tags", t)
    for t in tags.get("exclude_user", []):
        candidates -= get_field_matches("user_tags", t)

    return candidates


def get_file_details(collection, file_md5):
    try:
        pool_id = request.args.get("pool") or get_pool_id(collection)
        sub_collection = collection
        if collection:
            if collection.startswith("global:pool:"):
                parts = collection.split(":")
                if len(parts) >= 5 and parts[3] == "col":
                    sub_collection = parts[4]
                elif len(parts) >= 3:
                    sub_collection = parts[2]
            elif collection.startswith("pool:"):
                parts = collection.split(":")
                if len(parts) >= 4 and parts[2] == "col":
                    sub_collection = parts[3]
                elif len(parts) >= 2:
                    sub_collection = parts[1]

        r = get_redis()
        file_id = f"{sub_collection}:file:{file_md5}"

        if pool_id:
            clusters_key = f"pool:{pool_id}:file:{file_md5}:bin_clusters"
        else:
            clusters_key = f"{collection}:file:{file_md5}:bin_clusters"

        # 1. Fetch full JSON, function counts, and cluster assignments
        pipe = r.pipeline(transaction=False)
        pipe.get(f"{file_id}:meta")
        pipe.scard(f"{sub_collection}:idx:file:functions:{file_md5}")
        pipe.smembers(clusters_key)
        results = pipe.execute()

        res = results[0]
        func_count = results[1]
        cluster_res = results[2]

        if not res:
            return {"error": "File not found"}, 404

        data = json.loads(res) if not isinstance(res, dict) else res
        if isinstance(data, str):
            data = json.loads(data)

        data["function_count"] = func_count
        data["file_id"] = f"{collection}:file:{file_md5}"

        if pool_id:
            enrich_pool_data(data, pool_id)

        cluster_ids = list(cluster_res) if isinstance(cluster_res, (list, set)) else []

        # Ensure array fields are set to actual arrays instead of strings, etc.
        data["bin_clusters"] = [
            c.decode() if isinstance(c, bytes) else str(c) for c in cluster_ids
        ]

        # 2. Fetch cluster metadata
        cluster_meta_map = {}
        if cluster_ids:
            algo = request.args.get("algo", "unweighted_cosine")
            c_pipe = r.pipeline(transaction=False)
            c_list = data["bin_clusters"]
            is_pool = pool_id is not None
            for cid in c_list:
                if is_pool:
                    # ponytail: Pool clusters do not use algo prefix in metadata keys
                    c_pipe.get(f"global:pool:{pool_id}:bin_cluster:{cid}:meta")
                else:
                    c_pipe.get(f"{collection}:bin_cluster:{algo}:{cid}:meta")
            c_results = c_pipe.execute()
            for cid, c_res in zip(c_list, c_results):
                cm = (
                    json.loads(c_res)
                    if c_res and not isinstance(c_res, dict)
                    else (c_res or {})
                )
                if isinstance(cm, str):
                    cm = json.loads(cm)
                cluster_meta_map[cid] = cm

        # 3. Compute inferred metadata (server-side)
        from bsimvis.app.services.config_service import config_service

        min_cohesion = float(
            request.args.get(
                "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
            )
        )

        inferred_meta = {
            "yara": {},
            "avtype": {},
            "filetype": {},
            "ccip": {},
            "filename": {},
            "md5": {},
        }

        # Collect existing values to exclude
        def to_list(v):
            if not v:
                return []
            if isinstance(v, list):
                return v
            return [v]

        existing = {
            "yara": set(to_list(data.get("yara"))),
            "avtype": set(to_list(data.get("avtype"))),
            "filetype": set(to_list(data.get("filetype"))),
            "ccip": set(to_list(data.get("cc_ip"))),
            "filename": set(
                to_list(data.get("file_names")) + to_list(data.get("file_name"))
            ),
            "md5": set(to_list(data.get("file_md5"))),
        }

        for cid, cm in cluster_meta_map.items():
            cohesion_score = cm.get("cohesion_score") or 0
            if cohesion_score >= min_cohesion:
                cohesion_pct = round(cohesion_score * 100)
                mapping = {
                    "yara_distribution": "yara",
                    "avtype_distribution": "avtype",
                    "filetype_distribution": "filetype",
                    "ccip_distribution": "ccip",
                    "filename_distribution": "filename",
                    "md5_distribution": "md5",
                }
                for dist_key, meta_key in mapping.items():
                    dist = cm.get(dist_key) or []
                    for item in dist:
                        val = item.get("value")
                        if not val:
                            continue

                        # Exclude if already in binary's own metadata
                        if val in existing[meta_key]:
                            continue

                        if (
                            val not in inferred_meta[meta_key]
                            or inferred_meta[meta_key][val]["percent"] < cohesion_pct
                        ):
                            inferred_meta[meta_key][val] = {
                                "percent": cohesion_pct,
                                "cluster_uuid": cm.get("cluster_uuid"),
                            }

        normalize_tags(data)
        for date_field in ["entry_date", "file_date"]:
            if date_field in data:
                data[date_field] = parse_timestamp(data[date_field])

        return {
            "file": data,
            "bin_cluster_map": cluster_meta_map,
            "inferred_meta": inferred_meta,
            "collection": collection,
        }
    except Exception as e:
        logging.error(f"Error in get_file_details: {e}", exc_info=True)
        return {"error": str(e)}, 500
