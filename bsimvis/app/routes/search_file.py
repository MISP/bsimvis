import json
import logging
import re

from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids, parse_timestamp

DEFAULT_LIMIT = 100


def normalize_tags(data):
    # Normalize legacy analysis tags
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [t.strip() for t in tags.split(",")] if tags else []
    elif tags is None:
        data["tags"] = []

    # Normalize new user tags
    user_tags = data.get("user_tags")
    if isinstance(user_tags, str):
        data["user_tags"] = (
            [t.strip() for t in user_tags.split(",")] if user_tags else []
        )
    elif user_tags is None:
        data["user_tags"] = []

    return data


def get_true_total_files(r, collection):
    total = r.hget(f"global:collection:{collection}:meta", "total_files")
    return int(total) if total else 0


def query_files_advanced(r, collection, filters):
    # Start with all files as candidates
    all_files_key = f"{collection}:all_files"
    candidates = {
        d.decode() if isinstance(d, bytes) else str(d)
        for d in r.smembers(all_files_key)
    }

    fields = filters.get("fields", {})

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
                pipe = r.pipeline()
                for b in matching_buckets:
                    pipe.smembers(b)
                for res in pipe.execute():
                    if res:
                        field_candidates.update(
                            t.decode() if isinstance(t, bytes) else str(t) for t in res
                        )
        return field_candidates

    # 0. Apply global search q (keyword search across all standard fields)
    search_q = filters.get("q", "").lower().strip()
    if search_q:
        search_fields = [
            ("file", "file_name"),
            ("file", "file_md5"),
            ("file", "language_id"),
            ("file", "batch_uuid"),
            ("idx", "tags"),
            ("idx", "user_tags"),
        ]
        for word in [w for w in search_q.split() if w.strip()]:
            word_matches = set()
            for lvl, field in search_fields:
                word_matches.update(get_field_matches(field, word, field_level=lvl))
            candidates.intersection_update(word_matches)
            if not candidates:
                return []

    # 1. Apply substring filters for string fields
    for field in [
        "file_name",
        "file_md5",
        "language_id",
        "batch_uuid",
        "bin_cluster_uuid",
    ]:
        val = fields.get(field)
        if val:
            field_matches = get_field_matches(field, val, field_level="file")
            candidates.intersection_update(field_matches)
            if not candidates:
                return []

    # Helper: get members of key/keys
    def get_members(keys):
        res_set = set()
        if not keys:
            return res_set
        if isinstance(keys, str):
            keys = [keys]
        for k in keys:
            members = r.smembers(k)
            if members:
                res_set.update(
                    m.decode() if isinstance(m, bytes) else str(m) for m in members
                )
        return res_set

    # Helper: get union of tag keys (tags + user_tags)
    def get_tag_union(tag_val):
        tags_key = f"{collection}:idx:file:tags:{tag_val.lower()}"
        user_tags_key = f"{collection}:idx:file:user_tags:{tag_val.lower()}"
        res = set()
        for m in r.sunion(tags_key, user_tags_key):
            res.add(m.decode() if isinstance(m, bytes) else str(m))
        return res

    # 2. Apply tag filters (intersections)
    for tag in filters.get("tags", []):
        match_set = get_tag_union(tag)
        if not match_set:
            # Fallback to substring search in both tag registries
            match_set = get_field_matches("tags", tag, field_level="idx")
            match_set.update(get_field_matches("user_tags", tag, field_level="idx"))

        candidates.intersection_update(match_set)
        if not candidates:
            return []

    for tag in filters.get("static_tags", []):
        tags_key = f"{collection}:idx:file:tags:{tag.lower()}"
        match_set = get_members(tags_key)
        if not match_set:
            match_set = get_field_matches("tags", tag, field_level="idx")

        candidates.intersection_update(match_set)
        if not candidates:
            return []

    for tag in filters.get("user_tags", []):
        user_tags_key = f"{collection}:idx:file:user_tags:{tag.lower()}"
        match_set = get_members(user_tags_key)
        if not match_set:
            match_set = get_field_matches("user_tags", tag, field_level="idx")

        candidates.intersection_update(match_set)
        if not candidates:
            return []

    # 3. Apply exclusion filters (subtractions)
    for tag in filters.get("exclude_tags", []):
        exclude_set = get_tag_union(tag)
        candidates.difference_update(exclude_set)

    for tag in filters.get("exclude_static_tags", []):
        tags_key = f"{collection}:idx:file:tags:{tag.lower()}"
        exclude_set = get_members(tags_key)
        candidates.difference_update(exclude_set)

    for tag in filters.get("exclude_user_tags", []):
        user_tags_key = f"{collection}:idx:file:user_tags:{tag.lower()}"
        exclude_set = get_members(user_tags_key)
        candidates.difference_update(exclude_set)

    # 4. Apply numerical date filters using ZSETs
    num_filters = {}
    min_entry_ts = filters.get("min_entry_ts")
    max_entry_ts = filters.get("max_entry_ts")
    min_file_ts = filters.get("min_file_ts")
    max_file_ts = filters.get("max_file_ts")

    if min_entry_ts is not None or max_entry_ts is not None:
        fmin = min_entry_ts if min_entry_ts is not None else "-inf"
        fmax = max_entry_ts if max_entry_ts is not None else "+inf"
        num_filters["entry_date"] = (fmin, fmax)

    if min_file_ts is not None or max_file_ts is not None:
        fmin = min_file_ts if min_file_ts is not None else "-inf"
        fmax = max_file_ts if max_file_ts is not None else "+inf"
        num_filters["file_date"] = (fmin, fmax)

    for field, (fmin, fmax) in num_filters.items():
        zset_key = f"{collection}:idx:file:{field}"
        matched_members = {
            m.decode() if isinstance(m, bytes) else str(m)
            for m in r.zrangebyscore(zset_key, fmin, fmax)
        }
        candidates.intersection_update(matched_members)
        if not candidates:
            return []

    # 5. Apply function count filter and sorting
    min_funcs = filters.get("min_funcs")
    max_funcs = filters.get("max_funcs")
    sort_by = filters.get("sort_by")
    sort_order = filters.get("sort_order", "desc")

    if (
        min_funcs is not None or max_funcs is not None or sort_by == "function_count"
    ) and candidates:
        pipe = r.pipeline()
        candidates_list = list(candidates)
        for cid in candidates_list:
            parts = cid.split(":")
            md5 = parts[-1] if len(parts) >= 3 else ""
            pipe.scard(f"{collection}:idx:file:functions:{md5}")
        func_counts = pipe.execute()

        candidate_counts = {}
        filtered_candidates = []
        for cid, count in zip(candidates_list, func_counts):
            count_val = int(count) if count is not None else 0
            if min_funcs is not None and count_val < min_funcs:
                continue
            if max_funcs is not None and count_val > max_funcs:
                continue
            candidate_counts[cid] = count_val
            filtered_candidates.append(cid)

        candidates = set(filtered_candidates)

        if sort_by == "function_count":
            reverse = sort_order == "desc"
            filtered_candidates.sort(key=lambda x: candidate_counts[x], reverse=reverse)
            return filtered_candidates

    # 6. Apply sorting by date or default
    if sort_by in ["entry_date", "file_date"]:
        zset_key = f"{collection}:idx:file:{sort_by}"
        pipe = r.pipeline()
        candidates_list = list(candidates)
        for cid in candidates_list:
            pipe.zscore(zset_key, cid)
        scores = pipe.execute()

        scored_candidates = []
        for cid, score in zip(candidates_list, scores):
            score_val = float(score) if score is not None else 0.0
            scored_candidates.append((cid, score_val))

        reverse = sort_order == "desc"
        scored_candidates.sort(key=lambda x: x[1], reverse=reverse)
        return [x[0] for x in scored_candidates]
    else:
        return sorted(list(candidates))


def search_files():
    r = get_redis()

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return {"error": "offset and limit must be integers"}, 400

    format_arg = request.args.get("format")
    if format_arg in ("csv", "json"):
        offset = 0
        limit = 100000

    collection = request.args.get("collection")
    if not collection:
        return {"error": "No collection specified"}, 400

    q = request.args.get("q", "").strip()

    # Build tag/field filters
    fields = {}
    for field in [
        "batch_uuid",
        "language_id",
        "file_md5",
        "file_name",
        "bin_cluster_uuid",
    ]:
        val = request.args.get(field)
        if val:
            fields[field] = val.strip()

    # Align standard and file-specific tag parameters
    tags = [
        t.strip()
        for t in request.args.getlist("tag") + request.args.getlist("file_tag")
        if t.strip()
    ]
    static_tags = [
        t.strip()
        for t in request.args.getlist("static_tag")
        + request.args.getlist("file_static_tag")
        if t.strip()
    ]
    user_tags = [
        t.strip()
        for t in request.args.getlist("user_tag")
        + request.args.getlist("file_user_tag")
        if t.strip()
    ]

    exclude_tags = [
        t.strip()
        for t in request.args.getlist("exclude_tag")
        + request.args.getlist("exclude_file_tag")
        if t.strip()
    ]
    exclude_static_tags = [
        t.strip()
        for t in request.args.getlist("exclude_static_tag")
        + request.args.getlist("exclude_file_static_tag")
        if t.strip()
    ]
    exclude_user_tags = [
        t.strip()
        for t in request.args.getlist("exclude_user_tag")
        + request.args.getlist("exclude_file_user_tag")
        if t.strip()
    ]

    def parse_date_filter(val):
        if not val:
            return None
        ts = parse_timestamp(val)
        return ts if ts > 0 else None

    min_entry_ts = parse_date_filter(request.args.get("min_entry_date"))
    max_entry_ts = parse_date_filter(request.args.get("max_entry_date"))
    min_file_ts = parse_date_filter(request.args.get("min_file_date"))
    max_file_ts = parse_date_filter(request.args.get("max_file_date"))

    def parse_int_filter(val):
        if val is None or val.strip() == "":
            return None
        try:
            return int(val)
        except ValueError:
            return None

    min_funcs = parse_int_filter(request.args.get("min_function_count"))
    max_funcs = parse_int_filter(request.args.get("max_function_count"))

    sort_by = request.args.get("sort_by")
    sort_order = request.args.get("sort_order", "desc")

    filters = {
        "q": q,
        "fields": fields,
        "tags": tags,
        "static_tags": static_tags,
        "user_tags": user_tags,
        "exclude_tags": exclude_tags,
        "exclude_static_tags": exclude_static_tags,
        "exclude_user_tags": exclude_user_tags,
        "min_entry_ts": min_entry_ts,
        "max_entry_ts": max_entry_ts,
        "min_file_ts": min_file_ts,
        "max_file_ts": max_file_ts,
        "min_funcs": min_funcs,
        "max_funcs": max_funcs,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }

    # Execute advanced search
    all_matched_ids = query_files_advanced(r, collection, filters)
    total = len(all_matched_ids)
    doc_ids = all_matched_ids[offset : offset + limit]

    # Fetch full JSON, function counts, and cluster assignments for the page
    pipe = r.pipeline()
    for doc_id in doc_ids:
        pipe.json().get(f"{doc_id}:meta", "$")
        parts = doc_id.split(":")
        md5 = parts[-1] if len(parts) >= 3 else ""
        pipe.scard(f"{collection}:idx:file:functions:{md5}")
        pipe.smembers(f"{doc_id}:bin_clusters")
    raw_results = pipe.execute()

    files_list = []
    unique_cluster_ids = set()
    for i, doc_id in enumerate(doc_ids):
        raw = raw_results[3 * i]
        func_count = raw_results[3 * i + 1]
        cluster_ids_raw = raw_results[3 * i + 2]
        if not raw:
            continue
        data = raw[0] if isinstance(raw, list) and raw else raw

        col = data.get("collection", collection)
        md5 = data.get("file_md5")
        b_uuid = data.get("batch_uuid")
        if col and md5 and "file_id" not in data:
            data["file_id"] = f"{col}:file:{md5}"
        if col and b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        data["function_count"] = func_count

        # Cluster assignments
        cluster_ids = [
            cid.decode() if isinstance(cid, bytes) else str(cid)
            for cid in (cluster_ids_raw or [])
        ]
        data["bin_clusters"] = cluster_ids
        unique_cluster_ids.update(cluster_ids)

        normalize_tags(data)

        # Enforce Unix timestamps for UI
        for field in ["entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        files_list.append(data)

    # Fetch Cluster Metadata
    bin_cluster_meta_map = {}
    algo = "unweighted_cosine"
    if unique_cluster_ids:
        c_pipe = r.pipeline()
        c_list = list(unique_cluster_ids)
        for cid in c_list:
            c_pipe.json().get(f"{collection}:bin_cluster:{algo}:{cid}:meta", "$")
        c_results = c_pipe.execute()
        for cid, res in zip(c_list, c_results):
            cm = (res[0] if isinstance(res, list) and res else res) or {}
            if isinstance(cm, str):
                cm = json.loads(cm)
            if cm:
                bin_cluster_meta_map[cid] = {
                    "cluster_id": cm.get("cluster_id"),
                    "cluster_uuid": cm.get("cluster_uuid"),
                    "cluster_name": cm.get("cluster_name"),
                    "cohesion_score": cm.get("cohesion_score", 0),
                    "member_count": cm.get("member_count", 0),
                    "cluster_stability": cm.get("cluster_stability", 0.0),
                }

    # If total is 0 and no filters were specified, fall back to global total_files
    has_filters = (
        q
        or any(fields.values())
        or any(
            [
                tags,
                static_tags,
                user_tags,
                exclude_tags,
                exclude_static_tags,
                exclude_user_tags,
                min_entry_ts,
                max_entry_ts,
                min_file_ts,
                max_file_ts,
                min_funcs,
                max_funcs,
            ]
        )
    )
    if total == 0 and not has_filters:
        total = get_true_total_files(r, collection)

    response_data = {
        "total": total,
        "offset": offset,
        "limit": limit,
        "files": files_list,
        "bin_clusters": bin_cluster_meta_map,
    }
    if format_arg == "csv":
        from bsimvis.app.services.export_service import export_to_csv

        return export_to_csv(files_list, "files")
    elif format_arg == "json":
        from bsimvis.app.services.export_service import export_to_json

        return export_to_json(response_data, "files")
    else:
        return response_data
