import json
import logging
import redis
import hashlib
import os
from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import (
    query_ids,
    parse_timestamp,
    normalize_tags,
    enrich_pool_data,
    get_pool_id,
)
from bsimvis.app.services.config_service import config_service

DEFAULT_LIMIT = 100  # API RESULT LIMIT
DEFAULT_POOL_LIMIT = 1000000  # DATABASE FILTERING LIMIT
MAX_POOL_LIMIT = 1000000
CACHE_TIME_THRESHOLD = 0.1  # Only cache requests that take more than X seconds
MAX_CACHED_RESULTS = 10000


def autocomplete():
    try:
        col = request.args.get("collection")
        pool_id = request.args.get("pool")
        level = request.args.get("level", "func")
        field = request.args.get("field")
        query = request.args.get("q", "").lower().strip()
        limit = int(request.args.get("limit", 50))

        if pool_id:
            col = f"global:pool:{pool_id}"
        elif col and (col.startswith("pool:") or col.startswith("global:pool:")):
            pool_id = get_pool_id(col)
            col = f"global:pool:{pool_id}"

        if not all([col, level, field]):
            return {"error": "Missing parameters"}, 400

        r = get_redis()

        from bsimvis.app.services.index_config import get_search_paths_for_field

        paths = get_search_paths_for_field(field, level)

        registry_key = None
        prefix = None

        # Gracefully search down the path until we find a populated registry
        for path in paths:
            for lvl, physical_field in path:
                candidate_reg = f"{col}:reg:{lvl}:{physical_field}"
                if r.exists(candidate_reg):
                    registry_key = candidate_reg
                    prefix = f"{col}:idx:{lvl}:{physical_field}:"
                    break
            if registry_key:
                break

        results = []
        if registry_key:
            keywords = [k for k in query.split() if k]

            # Search pattern: use the first keyword for Redis-side filtering, or * if empty
            match_pat = f"*{keywords[0]}*" if keywords else "*"

            try:
                # Parse key to get the suffix
                count_found = 0
                pipe = r.pipeline(transaction=False)
                candidate_buckets = []

                for bucket in r.sscan_iter(registry_key, match=match_pat, count=1000):
                    bucket_str = (
                        bucket.decode() if isinstance(bucket, bytes) else str(bucket)
                    )

                    if bucket_str.startswith(prefix):
                        val = bucket_str[len(prefix) :]
                        val_lower = val.lower()
                        if not keywords or all(kw in val_lower for kw in keywords):
                            candidate_buckets.append((val, bucket_str))
                            count_found += 1

                    if count_found >= 100:
                        break

                # Batch fetch cardinalities
                for val, b_key in candidate_buckets:
                    pipe.scard(b_key)

                counts = pipe.execute()
                for (val, b_key), count in zip(candidate_buckets, counts):
                    results.append({"value": val, "count": count})

            except Exception as e:
                logging.warning(f"Autocomplete SSCAN failed for {registry_key}: {e}")

        # Deduplicate, sort and limit
        unique_results = {}
        for item in results:
            v = item["value"]
            if v not in unique_results or item["count"] > unique_results[v]["count"]:
                unique_results[v] = item

        final_results = sorted(
            unique_results.values(), key=lambda x: (len(x["value"]), x["value"])
        )[:limit]

        return {
            "results": final_results,
            "cardinality": (
                r.scard(registry_key) if registry_key and r.exists(registry_key) else 0
            ),
        }
    except Exception as e:
        logging.error(f"Error in autocomplete: {e}", exc_info=True)
        return {"error": str(e)}, 500


def get_field_stats():
    try:
        col = request.args.get("collection")
        level = request.args.get("level", "func")
        fields = request.args.getlist("field")

        if not col or not fields:
            return {"error": "Missing parameters"}, 400

        r = get_redis()
        stats = {}
        from bsimvis.app.services.index_config import get_search_paths_for_field

        for f in fields:
            paths = get_search_paths_for_field(f, level)
            reg_key = None

            for path in paths:
                for lvl, physical_field in path:
                    candidate_reg = f"{col}:reg:{lvl}:{physical_field}"
                    if r.exists(candidate_reg):
                        reg_key = candidate_reg
                        break
                if reg_key:
                    break

            if reg_key:
                stats[f] = r.scard(reg_key)
            else:
                stats[f] = 0

        return stats
    except Exception as e:
        logging.error(f"Error in get_field_stats: {e}", exc_info=True)
        return {"error": str(e)}, 500


def similarity_search():
    import time
    import uuid

    try:
        t_req_all_start = time.perf_counter()
        pool_id = request.args.get("pool")
        col = request.args.get("collection")
        algo = request.args.get("algo", "unweighted_cosine")

        metrics = {
            "cache_lookup": 0,
            "filter_resolve": 0,
            "prep_time": 0,
            "inter_time": 0,
            "mask_time": 0,
            "cache_write": 0,
            "enrich_time": 0,
        }
        session_id = str(uuid.uuid4())[:8]
        filter_keys_found = 0
        intersection_configs = []

        try:
            min_score = float(
                request.args.get(
                    "min_score", config_service.get("similarity.min_score", 0.9)
                )
            )
            max_score = float(request.args.get("max_score", 1.0))
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", DEFAULT_LIMIT))
            min_features = int(request.args.get("min_features", 0))
            min_cohesion = float(request.args.get("min_cohesion", 0.95))
        except ValueError:
            return {"error": "Invalid numeric parameter"}, 400

        format_arg = request.args.get("format")
        if format_arg in ("csv", "json"):
            offset = 0
            limit = 100000

        # Filtering parameters
        search_q = request.args.get("q", "").lower().strip()
        name_filter = request.args.get("name", "").lower().strip()

        # Tag related filters (now lists)
        tag_filters = request.args.getlist("tag")
        static_tag_filters = request.args.getlist("static_tag")
        user_tag_filters = request.args.getlist("user_tag")

        sim_tag_filters = request.args.getlist("sim_tag")
        sim_static_tag_filters = request.args.getlist("sim_static_tag")
        sim_user_tag_filters = request.args.getlist("sim_user_tag")

        func_tag_filters = request.args.getlist("func_tag")
        func_static_tag_filters = request.args.getlist("func_static_tag")
        func_user_tag_filters = request.args.getlist("func_user_tag")

        lang_filter = request.args.get("language", "").lower().strip()
        namespace_filter = request.args.get("namespace", "").lower().strip()
        ret_type_filter = request.args.get("ret_type", "").lower().strip()
        address_filter = request.args.get("address", "").lower().strip()
        file_tag_filters = request.args.getlist("file_tag")
        file_static_tag_filters = request.args.getlist("file_static_tag")
        file_user_tag_filters = request.args.getlist("file_user_tag")
        file_name_filter = request.args.get("file_name", "").lower().strip()
        md5_filters = request.args.getlist("md5")
        cross_binary_val = request.args.get("cross_binary")
        match_mode = request.args.get("match_mode", "any").lower().strip()
        if match_mode not in ("any", "both"):
            match_mode = "any"

        # Tag Exclusion filters (now lists)
        ex_tag_filters = request.args.getlist("exclude_tag")
        ex_static_tag_filters = request.args.getlist("exclude_static_tag")
        ex_user_tag_filters = request.args.getlist("exclude_user_tag")

        ex_sim_tag_filters = request.args.getlist("exclude_sim_tag")
        ex_sim_static_tag_filters = request.args.getlist("exclude_sim_static_tag")
        ex_sim_user_tag_filters = request.args.getlist("exclude_sim_user_tag")

        ex_func_tag_filters = request.args.getlist("exclude_func_tag")
        ex_func_static_tag_filters = request.args.getlist("exclude_func_static_tag")
        ex_func_user_tag_filters = request.args.getlist("exclude_func_user_tag")

        ex_file_tag_filters = request.args.getlist("exclude_file_tag")
        ex_file_static_tag_filters = request.args.getlist("exclude_file_static_tag")
        ex_file_user_tag_filters = request.args.getlist("exclude_file_user_tag")

        try:
            pool_limit = int(request.args.get("pool_limit", DEFAULT_POOL_LIMIT))
        except (ValueError, TypeError):
            pool_limit = DEFAULT_POOL_LIMIT
        pool_limit = max(1, min(pool_limit, MAX_POOL_LIMIT))

        sort_by = request.args.get("sort_by", "score")
        sort_order = request.args.get("sort_order", "desc").lower()

        if not col and not pool_id:
            return {"error": "Missing collection or pool"}, 400

        r = get_redis()

        is_pool = pool_id is not None
        if is_pool:
            col = f"global:pool:{pool_id}"
            algo_zset = f"global:pool:{pool_id}:sim:score"
            min_features_zset = f"global:pool:{pool_id}:sim:min_features"
        else:
            # Algorithm Name Standardizer
            # Map frontend 'milvus_sparse' to the ZSET name used in Redis
            # Support legacy 'milvus_inverted' if it exists and milvus_sparse doesn't
            if algo == "milvus_sparse":
                if not r.exists(f"{col}:sim:score:milvus_sparse") and r.exists(
                    f"{col}:sim:score:milvus_inverted"
                ):
                    algo = "milvus_inverted"
                    logging.info(
                        f"[*] Mapping 'milvus_sparse' to legacy 'milvus_inverted' for collection {col}"
                    )

            algo_zset = f"{col}:sim:score:{algo}"
            min_features_zset = f"{col}:sim:min_features"
        pool_truncated = False
        total = 0

        has_min_features = min_features > 0

        # --- CACHING LOGIC ---
        t_hash_start = time.perf_counter()
        cache_params = {
            "col": col,
            "algo": algo,
            "min_score": min_score,
            "max_score": max_score,
            "min_features": min_features,
            "pool_limit": pool_limit,  # CRITICAL: Include pool_limit in cache hash
            "cross_binary": cross_binary_val,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        # Include all other filters & exclusions
        for f in [
            "md5",
            "id",
            "language",
            "batch_uuid",
            "namespace",
            "ret_type",
            "address",
            "q",
            "name",
            "file_name",
            "match_mode",
        ]:
            v = request.args.get(f)
            if v:
                cache_params[f] = v.strip().lower()

        # Multi-value tag filters
        for f in [
            "tag",
            "static_tag",
            "user_tag",
            "sim_tag",
            "sim_static_tag",
            "sim_user_tag",
            "func_tag",
            "func_static_tag",
            "func_user_tag",
            "file_tag",
            "file_static_tag",
            "file_user_tag",
            "exclude_tag",
            "exclude_static_tag",
            "exclude_user_tag",
            "exclude_sim_tag",
            "exclude_sim_static_tag",
            "exclude_sim_user_tag",
            "exclude_func_tag",
            "exclude_func_static_tag",
            "exclude_func_user_tag",
            "exclude_file_tag",
            "exclude_file_static_tag",
            "exclude_file_user_tag",
        ]:
            v = request.args.getlist(f)
            if v:
                cache_params[f] = [x.strip().lower() for x in v if x.strip()]

        cache_hash = hashlib.md5(
            json.dumps(cache_params, sort_keys=True).encode()
        ).hexdigest()
        m_hash_prep_time = time.perf_counter() - t_hash_start
        metrics["hash_prep"] = m_hash_prep_time

        cache_key = f"cache:search:sim:{cache_hash}"

        t_cache_lookup_start = time.perf_counter()
        use_cache = request.args.get("use_cache", "false").lower() == "true"
        cached_res = r.get(cache_key) if use_cache else None
        m_cache_lookup_time = time.perf_counter() - t_cache_lookup_start
        metrics["cache_lookup"] = m_cache_lookup_time

        page_results = []
        cache_hit = False

        if cached_res:
            try:
                c_obj = json.loads(cached_res)
                total = c_obj.get("total", 0)
                pool_truncated = c_obj.get("pool_truncated", False)
                c_ids = c_obj.get("ids", [])
                c_scores = c_obj.get("scores", [])

                if (
                    offset + limit <= len(c_ids) or len(c_ids) == total
                ):  # Either we have enough results or the cache is exact
                    page_results = list(
                        zip(
                            c_ids[offset : offset + limit],
                            c_scores[offset : offset + limit],
                        )
                    )
                    cache_hit = True
                    r.expire(cache_key, 300)

                    logging.info(
                        f"SIM SEARCH [@CACHE] HIT {cache_key}: lookup={m_cache_lookup_time:.3f}s, total={total}"
                    )
            except Exception as e:
                logging.warning(f"Cache parse failed for {cache_key}: {e}")

        if not cache_hit:
            logging.info(
                f"SIM SEARCH [@CACHE] MISS {cache_key}: lookup={m_cache_lookup_time:.3f}s, total={total} VS looking for {offset + limit}"
            )
            start_time = time.perf_counter()
            # Collect all ZSET keys for the final intersection
            metrics["cache_lookup"] = m_cache_lookup_time

            # --- LUA SCRIPT SETUP ---
            from bsimvis.app.services.lua_manager import lua_manager

            lua_manager.register_all()
            search_script = lua_manager.get_script("search_similarity")

            t_lua_prep = time.perf_counter()
            groups_raw = []

            def get_group_targets(lvl, val, allowed_fields=None):
                """
                Resolves a filter into raw identity base-keys
                using the standardized registry->bucket hierarchy.
                Returns a list of (lvl, targets, field) tuples for all matches.
                """
                from bsimvis.app.services.index_config import (
                    INDEX_CONFIG,
                    EXACT_FIELDS,
                )

                if not allowed_fields:
                    # Dynamically discover all fields allowed at this level from config
                    allowed_fields = []
                    for src_lvl, fields in INDEX_CONFIG.items():
                        for field, targets in fields.items():
                            if lvl in targets:
                                from bsimvis.app.services.index_config import (
                                    resolve_target_field,
                                )

                                allowed_fields.append(
                                    resolve_target_field(src_lvl, lvl, field)
                                )
                    # Deduplicate
                    allowed_fields = list(set(allowed_fields))

                val_lower = val.lower()
                matches = []

                # Determine correct registry and index prefixes
                if is_pool:
                    reg_prefix = f"global:pool:{pool_id}:reg"
                    idx_prefix = f"global:pool:{pool_id}:idx"
                else:
                    reg_prefix = f"{col}:reg"
                    idx_prefix = f"{col}:idx"

                for field in allowed_fields:
                    registry_key = f"{reg_prefix}:{lvl}:{field}"
                    if r.exists(registry_key):
                        matching_buckets = []

                        # NEW: Perfect match fields
                        if field in EXACT_FIELDS:
                            target_bucket = f"{idx_prefix}:{lvl}:{field}:{val_lower}"
                            if r.sismember(registry_key, target_bucket):
                                matching_buckets = [target_bucket]
                        else:
                            try:
                                for bucket in r.sscan_iter(
                                    registry_key, match=f"*{val_lower}*", count=1000
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
                                pass

                        if matching_buckets:
                            targets = []
                            if lvl == "sim":
                                # Fix: Don't use split(":")[-1] as tags can contain colons
                                sim_idx_prefix = f"{idx_prefix}:sim:{field}:"
                                targets = [
                                    b[len(sim_idx_prefix) :]
                                    for b in matching_buckets
                                    if b.startswith(sim_idx_prefix)
                                ]
                            else:
                                if len(matching_buckets) == 1:
                                    targets = [
                                        (t.decode() if isinstance(t, bytes) else str(t))
                                        for t in r.smembers(matching_buckets[0])
                                    ]
                                else:
                                    # Use SUNION for multiple buckets
                                    targets = [
                                        (t.decode() if isinstance(t, bytes) else str(t))
                                        for t in r.sunion(*matching_buckets)
                                    ]

                            logging.info(
                                f"SIM SEARCH | {session_id} | Resolved {len(targets)} partial {lvl}-level targets across {len(matching_buckets)} buckets for '{field}:{val}'"
                            )
                            matches.append((lvl, targets, field))

                return matches

            def add_group(sub_matches, field_name="q", exclude=False):
                """
                Adds a metadata group to the Lua config.
                Supports sub_groups for OR logic within a single search term.
                """
                if not sub_matches:
                    return

                # Group normalization for Lua
                normalized_subs = []
                total_weight = 0

                for lvl, targets, field in sub_matches:
                    if lvl == "sim":
                        l_name = "similarity"
                        # Config-driven sim prefix: always {col}:idx:sim:{field}:
                        p = f"{col}:idx:sim:{field}:"
                    elif lvl == "func":
                        l_name = "function"
                        p = f"{col}:sim:involves:func:"
                    else:  # file
                        l_name = "binary"
                        p = f"{col}:sim:involves:file:"

                    weight = 0
                    clean_targets = []
                    if p:
                        for t in targets:
                            # Clean target ID to remove redundant collection:type prefixes
                            clean_t = t
                            if l_name == "function" and t.startswith(f"{col}:func:"):
                                clean_t = t[len(f"{col}:func:") :]
                            elif l_name == "binary" and t.startswith(f"{col}:file:"):
                                clean_t = t[len(f"{col}:file:") :]

                            clean_targets.append(clean_t)
                            try:
                                weight += r.scard(f"{p}{clean_t}")
                            except:
                                pass
                    else:
                        clean_targets = targets[:1000]

                    # Only these 3 fields are truly native to the sim entity itself.
                    # file_tags, func_tags, file_user_tags, func_user_tags etc. are
                    # propagated FROM file/func and need the "both" entity check.
                    sim_native_fields = {"tags", "user_tags", "is_cross_binary"}
                    is_propagated = (
                        l_name == "similarity" and field not in sim_native_fields
                    )

                    # func_index_prefix: tells Lua which Redis key prefix to use when
                    # building the entity map (function IDs) for "both" mode.
                    # - sim-level propagated: idx:func:{field}:  (e.g. idx:func:function_name:)
                    # - binary-level:         idx:func:file_md5: (targets are md5 values)
                    # - function-level:       func:              (targets are clean func IDs)
                    if l_name == "similarity" and is_propagated:
                        func_index_prefix = f"{col}:idx:func:{field}:"
                    elif l_name == "binary":
                        func_index_prefix = f"{col}:idx:func:file_md5:"
                    elif l_name == "function":
                        func_index_prefix = f"{col}:func:"
                    else:
                        func_index_prefix = ""

                    total_weight += weight
                    normalized_subs.append(
                        {
                            "level": l_name,
                            "targets": clean_targets[:1000],
                            "field": field,
                            "propagated": is_propagated,
                            "func_index_prefix": func_index_prefix,
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

            # --------------------------------------------------------------------------
            # Filter Configuration — config-driven
            #
            # Each entry: (param_value, label, paths)
            # where paths is a list of lists of (level, target_field_name).
            #
            # The search engine evaluates each path independently. It tries the levels
            # in a path in order, and stops at the first one that returns results.
            # --------------------------------------------------------------------------
            from bsimvis.app.services.index_config import (
                get_search_paths_for_field,
                INDEX_CONFIG,
                resolve_target_field,
            )

            def _paths(field):
                return get_search_paths_for_field(field, "sim")

            def _paths_for_source(source_lvl, field):
                """Explicit scoped searches (e.g. ?file_tag=) only look for tags originating from that specific source."""
                targets = INDEX_CONFIG.get(source_lvl, {}).get(field, [])
                path = []
                for lvl in ["sim", "func", "file"]:
                    if lvl in targets:
                        path.append((lvl, resolve_target_field(source_lvl, lvl, field)))
                return [path] if path else []

            # Core Filters — fully config-driven
            filter_configs = []

            # 1. Native/Propagated fields from INDEX_CONFIG
            # We want to iterate over all fields that could end up at the 'sim' level
            for src_lvl, fields in INDEX_CONFIG.items():
                for field, targets in fields.items():
                    # We allow filtering by ANY field in INDEX_CONFIG.
                    # If not natively at 'sim' level, it will trigger a join/fallback via _paths_for_source.
                    target_field = resolve_target_field(src_lvl, "sim", field)

                    # Check if this field is in request args
                    # We handle both the target name (e.g. func_tags) and common aliases
                    val = request.args.get(target_field)

                    if target_field in ["file_md5", "parent_md5", "related_md5", "file_name", "parent_file_name", "related_file_name"]:
                        continue

                    # Alias handling
                    if not val:
                        aliases = {
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
                        paths = _paths_for_source(src_lvl, field)
                        if paths:
                            filter_configs.append((val, target_field, paths))

            # 2. Tag-specific logic (for unions)
            md5_val = request.args.get("md5") or request.args.get("file_md5")
            md5_configs = []
            if md5_val:
                md5_paths = _paths_for_source("file", "file_md5") + _paths_for_source("file", "parent_md5") + _paths_for_source("file", "related_md5")
                md5_configs = [([md5_val], "any_md5", md5_paths)]

            file_name_val = request.args.get("file_name")
            file_name_configs = []
            if file_name_val:
                file_name_paths = _paths_for_source("file", "file_name") + _paths_for_source("file", "parent_file_name") + _paths_for_source("file", "related_file_name")
                file_name_configs = [([file_name_val], "any_file_name", file_name_paths)]

            tag_filter_configs = md5_configs + file_name_configs + [
                (tag_filters, "tag", _paths("tags") + _paths("user_tags")),
                (static_tag_filters, "static_tag", _paths("tags")),
                (user_tag_filters, "user_tag", _paths("user_tags")),
                (
                    sim_tag_filters,
                    "sim_tag",
                    _paths_for_source("sim", "tags")
                    + _paths_for_source("sim", "user_tags"),
                ),
                (
                    sim_static_tag_filters,
                    "sim_static_tag",
                    _paths_for_source("sim", "tags"),
                ),
                (
                    sim_user_tag_filters,
                    "sim_user_tag",
                    _paths_for_source("sim", "user_tags"),
                ),
                (
                    func_tag_filters,
                    "func_tag",
                    _paths_for_source("func", "tags")
                    + _paths_for_source("func", "user_tags"),
                ),
                (
                    func_static_tag_filters,
                    "func_static_tag",
                    _paths_for_source("func", "tags"),
                ),
                (
                    func_user_tag_filters,
                    "func_user_tag",
                    _paths_for_source("func", "user_tags"),
                ),
                (
                    file_tag_filters,
                    "file_tag",
                    _paths_for_source("file", "tags")
                    + _paths_for_source("file", "user_tags"),
                ),
                (
                    file_static_tag_filters,
                    "file_static_tag",
                    _paths_for_source("file", "tags"),
                ),
                (
                    file_user_tag_filters,
                    "file_user_tag",
                    _paths_for_source("file", "user_tags"),
                ),
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
                        # Evaluate this specific source's propagation path
                        for i, (lvl, physical_field) in enumerate(path):
                            matches = get_group_targets(
                                lvl, val, allowed_fields=[physical_field]
                            )
                            if matches:
                                if i > 0:
                                    logging.info(
                                        f"SIM SEARCH | {session_id} | Fallback triggered! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                                    )
                                all_matches.extend(matches)
                                # We found it! No need to fall back to joins for THIS specific source
                                break

                    if not all_matches:
                        logging.info(
                            f"SIM SEARCH | {session_id} | Filter '{label}={val}' matched 0 targets. Empty."
                        )
                        return {
                            "total": 0,
                            "pairs": [],
                            "algo": algo,
                            "collection": col,
                            "pool_truncated": False,
                        }

                    add_group(all_matches, field_name=f"{label}:{val}")

            # --- Exclusion Configuration (config-driven, same routing logic) ---
            exclude_configs = [
                (ex_tag_filters, "ex_tag", _paths("tags") + _paths("user_tags")),
                (ex_static_tag_filters, "ex_static_tag", _paths("tags")),
                (ex_user_tag_filters, "ex_user_tag", _paths("user_tags")),
                (
                    ex_sim_tag_filters,
                    "ex_sim_tag",
                    _paths_for_source("sim", "tags")
                    + _paths_for_source("sim", "user_tags"),
                ),
                (
                    ex_sim_static_tag_filters,
                    "ex_sim_static_tag",
                    _paths_for_source("sim", "tags"),
                ),
                (
                    ex_sim_user_tag_filters,
                    "ex_sim_user_tag",
                    _paths_for_source("sim", "user_tags"),
                ),
                (
                    ex_func_tag_filters,
                    "ex_func_tag",
                    _paths_for_source("func", "tags")
                    + _paths_for_source("func", "user_tags"),
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
                    _paths_for_source("file", "tags")
                    + _paths_for_source("file", "user_tags"),
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
                                        f"SIM SEARCH | {session_id} | Fallback triggered (Exclude)! '{physical_field}={val}' wasn't found natively at '{path[0][0]}', successfully joined via '{lvl}'."
                                    )
                                all_matches.extend(matches)
                                break

                    if all_matches:
                        add_group(
                            all_matches, field_name=f"{label}:{val}", exclude=True
                        )

            if search_q:
                from bsimvis.app.services.index_config import INDEX_CONFIG

                all_levels = list(INDEX_CONFIG.keys())

                for word in [w for w in search_q.split() if w.strip()]:
                    all_matches = []
                    # q search always checks all levels and does an OR between them
                    for lvl in all_levels:
                        matches = get_group_targets(lvl, word)
                        if matches:
                            all_matches.extend(matches)

                    if not all_matches:
                        return {
                            "total": 0,
                            "pairs": [],
                            "algo": algo,
                            "collection": col,
                            "pool_truncated": False,
                            "q": search_q,
                        }

                    add_group(all_matches, field_name=f"q({word})")

            if cross_binary_val is not None:
                cb_bool = cross_binary_val.lower() == "true"
                cb_key = f"{col}:sim:is_cross_binary:{'true' if cb_bool else 'false'}"
                if r.exists(cb_key):
                    groups_raw.append(
                        {
                            "type": "direct_zset",
                            "field": "cross_binary",
                            "key": cb_key,
                            "weight": r.zcard(cb_key),
                        }
                    )
                else:
                    # Essential: If user filters by cross_binary but no such pairs exist, return empty
                    logging.info(
                        f"SIM SEARCH | {session_id} | Cross-Binary Filter '{cross_binary_val}' matched 0 pairs (Key {cb_key} missing)"
                    )
                    return {
                        "total": 0,
                        "pairs": [],
                        "algo": algo,
                        "collection": col,
                        "pool_truncated": False,
                        "offset": offset,
                        "limit": limit,
                    }

            # Similarity Score Group
            sim_weight = r.zcount(algo_zset, min_score, max_score)
            groups_raw.append(
                {
                    "type": "score_range",
                    "field": "similarity",
                    "weight": sim_weight,
                    "min": min_score,
                    "max": max_score,
                    "key": algo_zset,
                }
            )

            # Feature Count Group
            if min_features > 0 or sort_by in ["feat_count", "min_features"]:
                feat_weight = (
                    r.zcount(min_features_zset, min_features, "+inf")
                    if min_features > 0
                    else r.zcard(min_features_zset)
                )
                groups_raw.append(
                    {
                        "type": "feature_range",
                        "field": "min_feature_count",
                        "weight": feat_weight,
                        "min": min_features,
                        "key": min_features_zset,
                    }
                )

            # Sort all groups by weight to find the best Producer (Step 2 of Lua)
            # Boost priority of the group that matches our sort_by metric
            for g in groups_raw:
                if sort_by == "score" and g["type"] == "score_range":
                    g["weight"] = max(0, g["weight"] - 5000)
                elif (sort_by == "feat_count" or sort_by == "min_features") and g[
                    "type"
                ] == "feature_range":
                    g["weight"] = max(0, g["weight"] - 5000)

            groups = sorted(groups_raw, key=lambda x: x["weight"])

            # --- LUA CONFIG ---
            lua_config = {
                "collection": f"global:pool:{pool_id}" if is_pool else col,
                "algo": algo,
                "pool_limit": pool_limit,
                "groups": groups,  # Use the sorted groups
                "offset": 0,  # Always fetch from 0 on miss to ensure cache consistency
                "limit": max(
                    offset + limit, 1000
                ),  # Fetch enough to seed cache for Switch View / Load More
                "min_score": min_score,
                "max_score": max_score,
                "sort_by": sort_by,
                "sort_order": sort_order,
                "match_mode": match_mode,
            }

            # Exec Lua Search (Unified Involves Architecture)
            t_lua_start = time.perf_counter()
            # We only pass keys that need direct ZSET access or global metric access
            keys = [algo_zset, min_features_zset]
            for g in groups_raw:
                if g["type"] == "direct_zset":
                    keys.append(g["key"])

            try:
                import json as std_json

                logging.info(
                    f"SIM SEARCH LUA_CONFIG: {std_json.dumps(lua_config, indent=2)}"
                )
                res = search_script(keys=keys, args=[json.dumps(lua_config)])
                total = res[0]
                pool_truncated = bool(res[1])
                all_ids = res[2]
                all_scores = res[3]
                if all_ids:
                    logging.info(
                        f"SIM SEARCH | {session_id} | Lua First Result: {all_ids[0]} -> {all_scores[0]}"
                    )

                if all_ids or total == 0:
                    cache_data = {
                        "total": total,
                        "pool_truncated": pool_truncated,
                        "ids": all_ids,
                        "scores": all_scores,
                    }
                    r.setex(cache_key, 3600, json.dumps(cache_data))

                # Python handles pagination for the final response when seeding from offset 0
                page_results = list(
                    zip(
                        all_ids[offset : offset + limit],
                        all_scores[offset : offset + limit],
                    )
                )
            except Exception as lua_err:
                logging.error(f"LUA SEARCH CRASH: {lua_err}")
                return {"error": f"Search engine error: {lua_err}"}, 500

            metrics["inter_time"] = time.perf_counter() - t_lua_start

        # --- PER-PAGE ENRICHMENT (Dynamic for Deep Selection Architecture) ---
        t_enrich_start = time.perf_counter()
        enriched_pairs = []
        cluster_meta_map = {}
        if page_results:
            # Phase 1: Fetch Similarity Metrics & Identity
            pipe = r.pipeline(transaction=False)
            for sid, sort_sc in page_results:
                pipe.get(sid)
                if sort_by == "score":
                    pipe.zscore(min_features_zset, sid)
                else:
                    pipe.zscore(algo_zset, sid)

            sim_raw = pipe.execute()

            # Extract unique function IDs and map sim data
            unique_fids = set()
            sim_data_map = {}  # sid -> {id1, id2, sim_doc, other_metric, sort_sc}

            def extract_from_sid(sid):
                if is_pool:
                    # global:pool:{pool_id}:sim:{fid1}::{fid2}
                    sim_prefix = f"global:pool:{pool_id}:sim:"
                    if not sid.startswith(sim_prefix):
                        return None, None
                    parts = sid[len(sim_prefix) :].split("::")
                    if len(parts) != 2:
                        return None, None
                    return parts[0], parts[1]
                else:
                    sim_prefix = f"{col}:sim:{algo}:"
                    if not sid.startswith(sim_prefix):
                        return None, None
                    parts = sid[len(sim_prefix) :].split("::")
                    if len(parts) != 2:
                        return None, None
                    return f"{col}:func:{parts[0]}", f"{col}:func:{parts[1]}"

            for i, (sid, sort_sc) in enumerate(page_results):
                raw_json = sim_raw[i * 2]
                if not raw_json:
                    continue
                data = (
                    json.loads(raw_json) if not isinstance(raw_json, dict) else raw_json
                )
                if isinstance(data, str):
                    data = json.loads(data)

                id1 = data.get("id1")
                id2 = data.get("id2")
                if not id1 or not id2:
                    id1, id2 = extract_from_sid(sid)

                if id1 and id2:
                    unique_fids.add(id1)
                    unique_fids.add(id2)
                    sim_data_map[sid] = {
                        "id1": id1,
                        "id2": id2,
                        "sim_doc": data,
                        "other_metric": sim_raw[i * 2 + 1],
                        "sort_sc": sort_sc,
                    }

            # Phase 2: Fetch Function Metadata & Cluster Scores (DEDUPLICATED)
            f_meta_map = {}  # fid -> {meta, scores}
            unique_fids_list = list(unique_fids)
            f_pipe = r.pipeline(transaction=False)
            for fid in unique_fids_list:
                f_pipe.get(f"{fid}:meta")
                if is_pool:
                    f_pipe.hgetall(f"global:pool:{pool_id}:{fid}:cluster_scores")
                else:
                    f_pipe.hgetall(f"{fid}:cluster_scores")

            f_results = f_pipe.execute()

            unique_cluster_ids = set()
            unique_md5s = set()

            for i, fid in enumerate(unique_fids_list):
                m_json = f_results[i * 2]
                scores_raw = f_results[i * 2 + 1] or {}

                meta = (
                    json.loads(m_json)
                    if m_json and not isinstance(m_json, dict)
                    else (m_json or {})
                )
                if isinstance(meta, str):
                    meta = json.loads(meta)

                meta["collection"] = fid.split(":")[0]

                scores = {}
                for k, v in scores_raw.items():
                    k_str = k.decode() if isinstance(k, bytes) else k
                    scores[k_str] = float(v)
                    c_coll = f"global:pool:{pool_id}" if is_pool else meta["collection"]
                    unique_cluster_ids.add((c_coll, k_str))

                f_meta_map[fid] = {"meta": meta, "scores": scores}
                if meta.get("file_md5"):
                    coll = fid.split(":")[0]
                    unique_md5s.add((coll, meta["file_md5"]))

            # Phase 3: Fetch File Metadata (DEDUPLICATED)
            file_meta_map = {}
            if unique_md5s:
                file_pipe = r.pipeline(transaction=False)
                md5_list = list(unique_md5s)
                for f_coll, md5 in md5_list:
                    file_pipe.get(f"{f_coll}:file:{md5}:meta")
                file_results = file_pipe.execute()
                for (f_coll, md5), res in zip(md5_list, file_results):
                    fm = (
                        json.loads(res)
                        if res and not isinstance(res, dict)
                        else (res or {})
                    )
                    if isinstance(fm, str):
                        fm = json.loads(fm)
                    file_meta_map[f"{f_coll}:{md5}"] = fm

            # Phase 4: Fetch Cluster Metadata (DEDUPLICATED & ALGO-AWARE)
            cluster_meta_map = {}
            if unique_cluster_ids:
                c_pipe = r.pipeline(transaction=False)
                c_list = list(unique_cluster_ids)
                # Use the requested algo for clusters if it matches a known clustering algo
                c_algo = (
                    algo
                    if algo in ["unweighted_cosine", "weighted_cosine"]
                    else "unweighted_cosine"
                )
                for c_coll, cid in c_list:
                    c_pipe.get(f"{c_coll}:cluster:{c_algo}:{cid}:meta")
                c_results = c_pipe.execute()
                for (c_coll, cid), res in zip(c_list, c_results):
                    cm = (
                        json.loads(res)
                        if res and not isinstance(res, dict)
                        else (res or {})
                    )
                    if isinstance(cm, str):
                        cm = json.loads(cm)
                    # Apply cohesion threshold server-side
                    if (cm.get("cohesion_score") or 0) >= min_cohesion:
                        cluster_meta_map[cid] = cm

            # Phase 5: Reconstruct Enriched Pairs
            for sid, sort_sc in page_results:
                s_data = sim_data_map.get(sid)
                if not s_data:
                    continue

                id1, id2 = s_data["id1"], s_data["id2"]
                f1_data = f_meta_map.get(id1, {"meta": {}, "scores": {}})
                f2_data = f_meta_map.get(id2, {"meta": {}, "scores": {}})

                m1, s1 = f1_data["meta"], f1_data["scores"]
                m2, s2 = f2_data["meta"], f2_data["scores"]

                f1 = file_meta_map.get(
                    f"{m1.get('collection')}:{m1.get('file_md5')}", {}
                )
                f2 = file_meta_map.get(
                    f"{m2.get('collection')}:{m2.get('file_md5')}", {}
                )

                if is_pool:
                    enrich_pool_data(f1, pool_id)
                    enrich_pool_data(f2, pool_id)
                    enrich_pool_data(m1, pool_id)
                    enrich_pool_data(m2, pool_id)
                    if s_data.get("sim_doc"):
                        enrich_pool_data(s_data["sim_doc"], pool_id)

                m1["file_tags"] = f1.get("tags", [])
                m1["file_user_tags"] = f1.get("user_tags", [])
                m2["file_tags"] = f2.get("tags", [])
                m2["file_user_tags"] = f2.get("user_tags", [])

                sim_score = (
                    float(s_data["sort_sc"])
                    if sort_by == "score"
                    else float(s_data["other_metric"] or 0)
                )
                feat_count = (
                    float(s_data["sort_sc"])
                    if sort_by in ["feat_count", "min_features"]
                    else float(s_data["other_metric"] or 0)
                )

                # Cluster references — plain list of UUIDs (metadata is in top-level map)
                clusters1 = [cid for cid in s1 if cid in cluster_meta_map]
                clusters2 = [cid for cid in s2 if cid in cluster_meta_map]

                # Cleanup function meta before embedding
                for field in [
                    "cluster_id",
                    "cluster_name",
                    "cluster_uuid",
                    "cluster_stability",
                ]:
                    m1.pop(field, None)
                    m2.pop(field, None)

                normalize_tags(m1)
                normalize_tags(m1, tag_fields=["file_tags", "file_user_tags"])
                normalize_tags(m2)
                normalize_tags(m2, tag_fields=["file_tags", "file_user_tags"])

                enriched_pair = {
                    "id1": id1,
                    "id2": id2,
                    "name1": m1.get(
                        "function_name", id1.split(":")[-1] if id1 else "N/A"
                    ),
                    "name2": m2.get(
                        "function_name", id2.split(":")[-1] if id2 else "N/A"
                    ),
                    "score": sim_score,
                    "feat_count": int(feat_count),
                    "sid": sid,
                    "entry_date": parse_timestamp(s_data["sim_doc"].get("entry_date")),
                    "meta1": {
                        "file_md5": m1.get("file_md5"),
                        "parent_md5": m1.get("parent_md5"),
                        "related_md5": m1.get("related_md5"),
                        "file_name": m1.get("file_name"),
                        "parent_file_name": m1.get("parent_file_name"),
                        "related_file_name": m1.get("related_file_name"),
                        "entrypoint_address": m1.get("entrypoint_address"),
                        "tags": m1.get("tags"),
                        "user_tags": m1.get("user_tags"),
                        "batch_uuid": m1.get("batch_uuid"),
                        "language_id": m1.get("language_id"),
                        "return_type": m1.get("return_type", "N/A"),
                        "namespace": m1.get("namespace", ""),
                        "parameters": m1.get("parameters", []),
                        "bsim_features_count": m1.get("bsim_features_count"),
                        "clusters": clusters1,
                        "file_tags": m1.get("file_tags"),
                        "file_user_tags": m1.get("file_user_tags"),
                        "entry_date": parse_timestamp(
                            m1.get("entry_date") or m1.get("file_date")
                        ),
                    },
                    "meta2": {
                        "file_md5": m2.get("file_md5"),
                        "parent_md5": m2.get("parent_md5"),
                        "related_md5": m2.get("related_md5"),
                        "file_name": m2.get("file_name"),
                        "parent_file_name": m2.get("parent_file_name"),
                        "related_file_name": m2.get("related_file_name"),
                        "entrypoint_address": m2.get("entrypoint_address"),
                        "tags": m2.get("tags"),
                        "user_tags": m2.get("user_tags"),
                        "batch_uuid": m2.get("batch_uuid"),
                        "language_id": m2.get("language_id"),
                        "return_type": m2.get("return_type", "N/A"),
                        "namespace": m2.get("namespace", ""),
                        "parameters": m2.get("parameters", []),
                        "bsim_features_count": m2.get("bsim_features_count"),
                        "clusters": clusters2,
                        "file_tags": m2.get("file_tags"),
                        "file_user_tags": m2.get("file_user_tags"),
                        "entry_date": parse_timestamp(
                            m2.get("entry_date") or m2.get("file_date")
                        ),
                    },
                    "tags": s_data["sim_doc"].get("tags", []),
                    "user_tags": s_data["sim_doc"].get("user_tags", []),
                    "algo": algo,
                }
                normalize_tags(enriched_pair)
                enriched_pairs.append(enriched_pair)

        metrics["enrich_time"] = time.perf_counter() - t_enrich_start

        total_time = time.perf_counter() - t_req_all_start

        # FINAL CONSOLIDATED PERFORMANCE LOGGING (CLEAN VERSION)
        cache_status = "HIT" if cache_hit else "MISS"
        is_fast_path = (
            " [FastPath]" if not cache_hit and len(intersection_configs) == 1 else ""
        )

        logging.info(
            f"SIM SEARCH | {session_id} | {cache_status}{is_fast_path} | "
            f"Total: {total} | Filters: {filter_keys_found} (in {metrics['filter_resolve']:.3f}s) | "
            f"Inter: {len(intersection_configs)} (in {metrics['inter_time']:.3f}s) | "
            f"Prep: {metrics['prep_time']:.3f}s | "
            f"CW: {metrics.get('cache_write', 0):.3f}s | "
            f"Mask: {abs(metrics['mask_time']):.3f}s {'[Lean]' if metrics['mask_time'] < 0 else '[Full]'} | "
            f"Enrich: {metrics['enrich_time']:.3f}s | Total: {total_time:.3f}s"
        )

        # Build top-level cluster metadata map (keyed by cid, sent once)
        clusters_response = {
            cid: {
                "cluster_id": cm.get("cluster_id"),
                "cluster_uuid": cm.get("cluster_uuid"),
                "cluster_name": cm.get("cluster_name"),
                "cohesion_score": cm.get("cohesion_score", 0),
                "member_count": cm.get("member_count", 0),
                "cluster_stability": cm.get("cluster_stability", 0.0),
                "avg_features": cm.get("avg_features", 0),
            }
            for cid, cm in cluster_meta_map.items()
        }

        response_data = {
            "collection": col,
            "algo": algo,
            "min_score": min_score,
            "max_score": max_score,
            "min_features": min_features,
            "q": request.args.get("q", ""),
            "name": request.args.get("name", ""),
            "tag": request.args.get("tag", ""),
            "static_tag": request.args.get("static_tag", ""),
            "user_tag": request.args.get("user_tag", ""),
            "sim_tag": request.args.get("sim_tag", ""),
            "sim_static_tag": request.args.get("sim_static_tag", ""),
            "sim_user_tag": request.args.get("sim_user_tag", ""),
            "func_tag": request.args.get("func_tag", ""),
            "func_static_tag": request.args.get("func_static_tag", ""),
            "func_user_tag": request.args.get("func_user_tag", ""),
            "language": request.args.get("language", ""),
            "md5": md5_filters,
            "cross_binary": cross_binary_val,
            "total": total,
            "offset": offset,
            "limit": limit,
            "pool_limit": pool_limit,
            "pool_truncated": pool_truncated,
            "clusters": clusters_response,
            "pairs": enriched_pairs,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "cached_response": cache_hit,
        }
        if format_arg == "csv":
            from bsimvis.app.services.export_service import export_to_csv

            return export_to_csv(enriched_pairs, "similarity")
        elif format_arg == "json":
            from bsimvis.app.services.export_service import export_to_json

            return export_to_json(response_data, "similarity")
        else:
            return response_data

    except Exception as e:
        logging.error(f"Error in similarity_search: {e}", exc_info=True)
        return {"error": str(e)}, 500
