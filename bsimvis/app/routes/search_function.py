import json
import logging
import redis
import hashlib
import time
import uuid
from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.services.lua_manager import lua_manager

search_function_bp = Blueprint("search_function", __name__)

DEFAULT_LIMIT = 100
DEFAULT_POOL_LIMIT = 1000000
MAX_POOL_LIMIT = 1000000


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


@search_function_bp.route("/api/function/search")
def search_functions():
    t_req_start = time.perf_counter()
    col = request.args.get("collection")
    if not col:
        return jsonify({"error": "No collection specified"}), 400

    session_id = str(uuid.uuid4())[:8]

    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
        pool_limit = int(request.args.get("pool_limit", DEFAULT_POOL_LIMIT))
    except ValueError:
        return jsonify({"error": "Invalid numeric parameter"}), 400

    pool_limit = max(1, min(pool_limit, MAX_POOL_LIMIT))

    # Search parameters
    search_q = request.args.get("q", "").lower().strip()
    name_filter = (
        request.args.get("function_name", request.args.get("name", "")).lower().strip()
    )

    tag_filters = request.args.getlist("tag")
    static_tag_filters = request.args.getlist("static_tag")
    user_tag_filters = request.args.getlist("user_tag")

    lang_filter = (
        request.args.get("language_id", request.args.get("language", ""))
        .lower()
        .strip()
    )
    namespace_filter = request.args.get("namespace", "").lower().strip()
    ret_type_filter = (
        request.args.get("return_type", request.args.get("ret_type", ""))
        .lower()
        .strip()
    )
    address_filter = (
        request.args.get("entrypoint_address", request.args.get("address", ""))
        .lower()
        .strip()
    )
    md5_filter = (
        request.args.get("file_md5", request.args.get("md5", "")).lower().strip()
    )
    file_name_filter = request.args.get("file_name", "").lower().strip()

    # Exclusion filters (list)
    ex_tag_filters = request.args.getlist("exclude_tag")
    ex_static_tag_filters = request.args.getlist("exclude_static_tag")
    ex_user_tag_filters = request.args.getlist("exclude_user_tag")

    func_tag_filters = request.args.getlist("func_tag")
    func_static_tag_filters = request.args.getlist("func_static_tag")
    func_user_tag_filters = request.args.getlist("func_user_tag")

    file_tag_filters = request.args.getlist("file_tag")
    file_static_tag_filters = request.args.getlist("file_static_tag")
    file_user_tag_filters = request.args.getlist("file_user_tag")

    ex_func_tag_filters = request.args.getlist("exclude_func_tag")
    ex_func_static_tag_filters = request.args.getlist("exclude_func_static_tag")
    ex_func_user_tag_filters = request.args.getlist("exclude_func_user_tag")

    ex_file_tag_filters = request.args.getlist("exclude_file_tag")
    ex_file_static_tag_filters = request.args.getlist("exclude_file_static_tag")
    ex_file_user_tag_filters = request.args.getlist("exclude_file_user_tag")

    try:
        min_features = int(request.args.get("min_features", 0))
    except (ValueError, TypeError):
        min_features = 0

    sort_by = request.args.get("sort_by", "id")
    sort_order = request.args.get("sort_order", "desc").lower()

    r = get_redis()

    def get_group_targets(lvl, val, allowed_fields=None):
        from bsimvis.app.services.index_config import INDEX_CONFIG, EXACT_FIELDS

        if not allowed_fields:
            # Dynamically discover all fields allowed at this level from config
            allowed_fields = []
            for src_lvl, fields in INDEX_CONFIG.items():
                for field, targets in fields.items():
                    if lvl in targets:
                        from bsimvis.app.services.index_config import (
                            resolve_target_field,
                        )

                        allowed_fields.append(resolve_target_field(src_lvl, lvl, field))
            # Deduplicate
            allowed_fields = list(set(allowed_fields))

        val_lower = val.lower()
        matches = []

        for field in allowed_fields:
            registry_key = f"{col}:reg:{lvl}:{field}"
            if r.exists(registry_key):
                matching_buckets = []

                # Check for perfect match fields
                if field in EXACT_FIELDS:
                    target_bucket = f"{col}:idx:{lvl}:{field}:{val_lower}"
                    if r.sismember(registry_key, target_bucket):
                        matching_buckets = [target_bucket]
                else:
                    try:
                        for bucket in r.sscan_iter(
                            registry_key, match=f"*{val_lower}*"
                        ):
                            bucket_str = (
                                bucket.decode()
                                if isinstance(bucket, bytes)
                                else str(bucket)
                            )
                            if val_lower in bucket_str.lower():
                                matching_buckets.append(bucket_str)
                    except Exception as e:
                        logging.warning(f"SSCAN failed for {registry_key}: {e}")

                targets = []
                if matching_buckets:
                    prefix = f"{col}:idx:{lvl}:{field}:"
                    targets = [
                        b[len(prefix) :]
                        for b in matching_buckets
                        if b.startswith(prefix)
                    ]

                if targets:
                    matches.append((lvl, targets, field))
        return matches

    groups_raw = []

    def add_group(sub_matches, field_name="q", exclude=False):
        if not sub_matches:
            return

        normalized_subs = []
        total_weight = 0

        for lvl, targets, field in sub_matches:
            weight = 0
            # Estimate weight
            prefix = f"{col}:idx:{lvl}:{field}:"
            for t in targets:
                try:
                    weight += r.scard(f"{prefix}{t}")
                except:
                    pass

            total_weight += weight
            normalized_subs.append(
                {
                    "level": lvl,
                    "targets": targets[:1000],
                    "field": field,
                }
            )

        groups_raw.append(
            {
                "type": "metadata",
                "field": field_name,
                "sub_groups": normalized_subs,
                "weight": total_weight if not exclude else 99999999,
                "exclude": exclude,
            }
        )

    # Core Filters — config-driven
    from bsimvis.app.services.index_config import (
        get_search_paths_for_field,
        INDEX_CONFIG,
        resolve_target_field,
    )

    def _paths(field):
        return get_search_paths_for_field(field, "func")

    def _paths_for_source(source_lvl, field):
        targets = INDEX_CONFIG.get(source_lvl, {}).get(field, [])
        path = []
        for lvl in ["func", "file"]:
            if lvl in targets:
                path.append((lvl, resolve_target_field(source_lvl, lvl, field)))
        return [path] if path else []

    # Core Filters — fully config-driven
    filter_configs = []

    # 1. Native/Propagated fields from INDEX_CONFIG
    # We want to iterate over all fields that could end up at the 'func' level
    for src_lvl, fields in INDEX_CONFIG.items():
        for field, targets in fields.items():
            if "func" in targets:
                target_field = resolve_target_field(src_lvl, "func", field)
                # Check if this field is in request args
                # We handle both the target name (e.g. file_tags) and common aliases (md5 -> file_md5)
                val = request.args.get(target_field)

                # Alias handling (for backward compat or convenience)
                if not val:
                    aliases = {
                        "file_md5": ["md5"],
                        "entrypoint_address": ["address"],
                        "function_name": ["name"],
                        "language_id": ["language"],
                        "return_type": ["ret_type"],
                    }
                    if target_field in aliases:
                        for alias in aliases[target_field]:
                            val = request.args.get(alias)
                            if val:
                                break

                if val:
                    filter_configs.append((val, target_field, _paths(field)))

    # 2. Tag-specific logic (already handled above if they are in INDEX_CONFIG, but sometimes we want union)
    # The existing code had some manual additions for unions, keeping them for now if not redundant.
    # Actually, the logic below handles tag lists which are distinct from single request.args.get()
    tag_filter_configs = [
        (tag_filters, "tag", _paths("tags") + _paths("user_tags")),
        (static_tag_filters, "static_tag", _paths("tags")),
        (user_tag_filters, "user_tag", _paths("user_tags")),
    ]
    for tfc in tag_filter_configs:
        if tfc[0]:
            filter_configs.append(tfc)

    for f_v, label, paths in filter_configs:
        if not f_v:
            continue
        vals = f_v if isinstance(f_v, list) else [f_v]
        for val in vals:
            if not val:
                continue
            all_matches = []
            for path in paths:
                for i, (lvl, physical_field) in enumerate(path):
                    matches = get_group_targets(
                        lvl, val, allowed_fields=[physical_field]
                    )
                    if matches:
                        if i > 0:
                            logging.info(
                                f"FUNC SEARCH | {session_id} | Fallback triggered! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                            )
                        all_matches.extend(matches)
                        break  # stop at first level that returns results

            if not all_matches:
                logging.info(
                    f"FUNC SEARCH | {session_id} | Filter '{label}={val}' matched 0."
                )
                return jsonify(
                    {"total": 0, "functions": [], "offset": offset, "limit": limit}
                )

            add_group(all_matches, field_name=f"{label}:{val}")

    # Exclusions — config-driven
    exclude_configs = [
        (ex_tag_filters, "ex_tag", _paths("tags") + _paths("user_tags")),
        (ex_static_tag_filters, "ex_static_tag", _paths("tags")),
        (ex_user_tag_filters, "ex_user_tag", _paths("user_tags")),
        (
            ex_func_tag_filters,
            "ex_func_tag",
            _paths_for_source("func", "tags") + _paths_for_source("func", "user_tags"),
        ),
        (
            ex_func_static_tag_filters,
            "ex_func_static_tag",
            _paths_for_source("func", "tags"),
        ),
        (
            ex_func_user_tag_filters,
            "ex_func_user_tag",
            _paths_for_source("func", "user_tags"),
        ),
        (
            ex_file_tag_filters,
            "ex_file_tag",
            _paths_for_source("file", "tags") + _paths_for_source("file", "user_tags"),
        ),
        (
            ex_file_static_tag_filters,
            "ex_file_static_tag",
            _paths_for_source("file", "tags"),
        ),
        (
            ex_file_user_tag_filters,
            "ex_file_user_tag",
            _paths_for_source("file", "user_tags"),
        ),
    ]

    for ex_v, label, paths in exclude_configs:
        if not ex_v:
            continue
        vals = ex_v if isinstance(ex_v, list) else [ex_v]
        for val in vals:
            if not val:
                continue
            all_matches = []
            for path in paths:
                for i, (lvl, physical_field) in enumerate(path):
                    matches = get_group_targets(
                        lvl, val, allowed_fields=[physical_field]
                    )
                    if matches:
                        if i > 0:
                            logging.info(
                                f"FUNC SEARCH | {session_id} | Fallback triggered (Exclude)! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                            )
                        all_matches.extend(matches)
                        break
            if all_matches:
                add_group(all_matches, field_name=f"{label}:{val}", exclude=True)

    # Global search q
    if search_q:
        for word in [w for w in search_q.split() if w.strip()]:
            all_matches = []
            for lvl in ["func", "file"]:
                matches = get_group_targets(lvl, word)
                if matches:
                    all_matches.extend(matches)

            if not all_matches:
                return jsonify(
                    {
                        "total": 0,
                        "functions": [],
                        "offset": offset,
                        "limit": limit,
                        "q": search_q,
                    }
                )

            add_group(all_matches, field_name=f"q({word})")

    # Numeric range filters
    if min_features > 0:
        feat_key = f"{col}:idx:func:bsim_features_count"
        weight = r.zcount(feat_key, min_features, "+inf") or 1000
        groups_raw.append(
            {
                "type": "numeric_range",
                "key": feat_key,
                "min": min_features,
                "weight": weight,
            }
        )

    # Lua Exec
    search_script = lua_manager.get_script("search_function")
    if not search_script:
        # Fallback to manual reload if script not found (first time)
        lua_manager.register_all()
        search_script = lua_manager.get_script("search_function")

    lua_config = {
        "collection": col,
        "pool_limit": pool_limit,
        "groups": sorted(groups_raw, key=lambda x: x["weight"]),
        "offset": offset,
        "limit": limit,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }

    try:
        res = search_script(keys=[], args=[json.dumps(lua_config)])
        total = res[0]
        pool_truncated = bool(res[1])
        doc_ids = res[2]
    except Exception as e:
        logging.error(f"FUNC LUA SEARCH CRASH: {e}")
        return jsonify({"error": str(e)}), 500

    # Enrichment
    pipe = r.pipeline()
    for doc_id in doc_ids:
        pipe.json().get(f"{doc_id}:meta", "$")

    # Also fetch file-level metadata for tags
    for doc_id in doc_ids:
        try:
            # doc_id is {col}:func:{md5}:{addr}
            parts = doc_id.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                pipe.json().get(f"{col}:file:{md5}:meta", "$")
            else:
                pipe.json().get("nonexistent", "$")
        except:
            pipe.json().get("nonexistent", "$")

    # Fetch clusters SET and scores
    for doc_id in doc_ids:
        pipe.smembers(f"{doc_id}:clusters")
        pipe.hgetall(f"{doc_id}:cluster_scores")

    raw_results_all = pipe.execute()
    raw_meta_results = raw_results_all[: len(doc_ids)]
    raw_file_results = raw_results_all[len(doc_ids) : 2 * len(doc_ids)]
    raw_cluster_sets = raw_results_all[2 * len(doc_ids) : 4 * len(doc_ids) : 2]
    raw_cluster_scores = raw_results_all[2 * len(doc_ids) + 1 : 4 * len(doc_ids) : 2]

    functions_list = []
    parsed_data_list = []

    for i, (doc_id, raw) in enumerate(zip(doc_ids, raw_meta_results)):
        if not raw:
            parsed_data_list.append(None)
            continue
        data = raw[0] if isinstance(raw, list) and raw else raw
        if isinstance(data, str):
            data = json.loads(data)
        parsed_data_list.append(data)

    # Secondary pipeline for cluster metadata
    algo = "unweighted_cosine"  # Default algo used by backend
    cluster_pipe = r.pipeline()

    # We will track which index in the pipeline corresponds to which function and cluster
    # cluster_queries = [(function_index, cluster_id), ...]
    cluster_queries = []

    for i, data in enumerate(parsed_data_list):
        if data:
            c_set = raw_cluster_sets[i]
            if c_set:
                for c_bytes in c_set:
                    cid = c_bytes.decode() if isinstance(c_bytes, bytes) else c_bytes
                    cluster_pipe.json().get(f"{col}:cluster:{algo}:{cid}:meta", "$")
                    cluster_queries.append((i, cid))

    if cluster_queries:
        raw_cluster_meta_results = cluster_pipe.execute()
    else:
        raw_cluster_meta_results = []

    # Map back the cluster metadata to the respective functions
    func_clusters_map = {i: [] for i in range(len(parsed_data_list))}
    for (func_idx, cid), raw_cm in zip(cluster_queries, raw_cluster_meta_results):
        if raw_cm:
            cm = raw_cm[0] if isinstance(raw_cm, list) else raw_cm
            if isinstance(cm, str):
                cm = json.loads(cm)
            if cm:
                func_clusters_map[func_idx].append(cm)

    for i, data in enumerate(parsed_data_list):
        if not data:
            continue

        # File metadata
        file_raw = raw_file_results[i]
        file_data = (
            file_raw[0] if isinstance(file_raw, list) and file_raw else file_raw
        ) or {}
        if isinstance(file_data, str):
            file_data = json.loads(file_data)

        data["file_tags"] = file_data.get("tags", [])
        data["file_user_tags"] = file_data.get("user_tags", [])

        # ID construction if missing
        md5 = data.get("file_md5")
        addr = data.get("entrypoint_address")
        b_uuid = data.get("batch_uuid")

        if md5 and addr and "function_id" not in data:
            data["function_id"] = f"{col}:func:{md5}:{addr}"
        if md5 and "file_id" not in data:
            data["file_id"] = f"{col}:file:{md5}"
        if b_uuid and "batch_id" not in data:
            data["batch_id"] = f"{col}:batch:{b_uuid}"

        normalize_tags(data)

        # Enforce Unix timestamps
        for field in ["entry_date", "file_date"]:
            if field in data:
                data[field] = parse_timestamp(data[field])

        # Cluster metadata enrichment
        c_metas = func_clusters_map[i]

        # Sort clusters by member_count or cohesion descending, so UI can just pick the first
        # We can sort by member_count descending
        c_metas.sort(key=lambda x: x.get("member_count", 0), reverse=True)

        clusters = []
        scores = raw_cluster_scores[i] or {}
        for cm in c_metas:
            cid = str(cm.get("cluster_id"))
            # The user wants 'cluster_stability' to be the per-function score
            score = float(
                scores.get(cid.encode() if isinstance(cid, str) else cid, 0.0)
            )
            if not score and isinstance(scores, dict):
                # Try decoding keys
                for k, v in scores.items():
                    k_str = k.decode() if isinstance(k, bytes) else k
                    if k_str == cid:
                        score = float(v)
                        break

            clusters.append(
                {
                    "cluster_id": cm.get("cluster_id"),
                    "cluster_uuid": cm.get("cluster_uuid"),
                    "cluster_name": cm.get("cluster_name"),
                    "cohesion_score": cm.get("cohesion_score", 0),
                    "member_count": cm.get("member_count", 0),
                    "cluster_stability": score or cm.get("cluster_stability", 0.0),
                    "avg_features": cm.get("avg_features", 0),
                }
            )
        data["clusters"] = clusters

        for field in [
            "cluster_id",
            "cluster_name",
            "cluster_uuid",
            "cluster_stability",
        ]:
            data.pop(field, None)
        functions_list.append(data)

    total_time = time.perf_counter() - t_req_start
    logging.info(
        f"FUNC SEARCH | {session_id} | Total: {total} | Time: {total_time:.3f}s"
    )

    return jsonify(
        {
            "total": total,
            "offset": offset,
            "limit": limit,
            "pool_truncated": pool_truncated,
            "functions": functions_list,
            "collection": col,
            "q": search_q,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
    )
