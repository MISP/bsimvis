import json
import logging
import redis
import hashlib
import os
from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids, parse_timestamp

search_similarity_bp = Blueprint("search_similarity", __name__)

DEFAULT_LIMIT = 100  # API RESULT LIMIT
DEFAULT_POOL_LIMIT = 1000000  # DATABASE FILTERING LIMIT
MAX_POOL_LIMIT = 1000000
CACHE_TIME_THRESHOLD = 0.1  # Only cache requests that take more than X seconds
MAX_CACHED_RESULTS = 10000


@search_similarity_bp.route("/api/search/autocomplete", methods=["GET"])
def autocomplete():
    col = request.args.get("collection")
    level = request.args.get("level", "func")
    field = request.args.get("field")
    query = request.args.get("q", "").lower().strip()
    limit = int(request.args.get("limit", 50))

    if not all([col, level, field]):
        return jsonify({"error": "Missing parameters"}), 400

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
        # Search pattern: for better performance we match the whole bucket key
        match_pat = f"*{query}*"

        try:
            # Parse key to get the suffix
            count_found = 0
            pipe = r.pipeline()
            candidate_buckets = []

            for bucket in r.sscan_iter(registry_key, match=match_pat, count=1000):
                bucket_str = (
                    bucket.decode() if isinstance(bucket, bytes) else str(bucket)
                )

                if bucket_str.startswith(prefix):
                    val = bucket_str[len(prefix) :]
                    if query in val.lower():
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

    return jsonify(
        {
            "results": final_results,
            "cardinality": (
                r.scard(registry_key) if registry_key and r.exists(registry_key) else 0
            ),
        }
    )


@search_similarity_bp.route("/api/search/fields", methods=["GET"])
def get_field_stats():
    col = request.args.get("collection")
    level = request.args.get("level", "func")
    fields = request.args.getlist("field")

    if not col or not fields:
        return jsonify({"error": "Missing parameters"}), 400

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

    return jsonify(stats)


@search_similarity_bp.route("/api/similarity/search", methods=["GET"])
def similarity_search():
    import time
    import uuid

    t_req_all_start = time.perf_counter()
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
        min_score = float(request.args.get("min_score", 0.95))
        max_score = float(request.args.get("max_score", 1.0))
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
        min_features = int(request.args.get("min_features", 0))
    except ValueError:
        return jsonify({"detail": "Invalid numeric parameter"}), 400

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

    if not col:
        return jsonify({"detail": "Missing collection"}), 400

    try:
        r = get_redis()

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

            try:
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

                    for field in allowed_fields:
                        # # 1. Try Exact Match (O(1))
                        # exact_key = f"{col}:idx:{lvl}:{field}:{val_lower}"
                        # if r.exists(exact_key):
                        #     if lvl == "sim":
                        #         matches.append((lvl, [exact_key.split(":")[-1]], field))
                        #     else:
                        #         targets = [t.decode() if isinstance(t, bytes) else str(t) for t in r.smembers(exact_key)]
                        #         logging.info(f"SIM SEARCH | {session_id} | Resolved {len(targets)} exact {lvl}-level targets for '{field}:{val}'")
                        #         matches.append((lvl, targets, field))

                        # 2. Try Registry-Based Match
                        registry_key = f"{col}:reg:{lvl}:{field}"
                        if r.exists(registry_key):
                            matching_buckets = []

                            # NEW: Perfect match fields
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
                                    logging.warning(
                                        f"SSCAN failed for {registry_key}: {e}"
                                    )
                                    pass

                            if matching_buckets:
                                targets = []
                                if lvl == "sim":
                                    # Fix: Don't use split(":")[-1] as tags can contain colons
                                    prefix = f"{col}:idx:{lvl}:{field}:"
                                    targets = [
                                        b[len(prefix) :]
                                        for b in matching_buckets
                                        if b.startswith(prefix)
                                    ]
                                else:
                                    if len(matching_buckets) == 1:
                                        targets = [
                                            (
                                                t.decode()
                                                if isinstance(t, bytes)
                                                else str(t)
                                            )
                                            for t in r.smembers(matching_buckets[0])
                                        ]
                                    else:
                                        # Use SUNION for multiple buckets
                                        targets = [
                                            (
                                                t.decode()
                                                if isinstance(t, bytes)
                                                else str(t)
                                            )
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
                                if l_name == "function" and t.startswith(
                                    f"{col}:func:"
                                ):
                                    clean_t = t[len(f"{col}:func:") :]
                                elif l_name == "binary" and t.startswith(
                                    f"{col}:file:"
                                ):
                                    clean_t = t[len(f"{col}:file:") :]

                                clean_targets.append(clean_t)
                                try:
                                    weight += r.scard(f"{p}{clean_t}")
                                except:
                                    pass
                        else:
                            clean_targets = targets[:1000]

                        total_weight += weight
                        normalized_subs.append(
                            {
                                "level": l_name,
                                "targets": clean_targets[:1000],
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
                            path.append(
                                (lvl, resolve_target_field(source_lvl, lvl, field))
                            )
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

                        # Alias handling
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
                            filter_configs.append(
                                (val, target_field, _paths_for_source(src_lvl, field))
                            )

                # 2. Tag-specific logic (for unions)
                tag_filter_configs = [
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
                            return jsonify(
                                {
                                    "total": 0,
                                    "pairs": [],
                                    "algo": algo,
                                    "collection": col,
                                    "pool_truncated": False,
                                }
                            )

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
                    for word in [w for w in search_q.split() if w.strip()]:
                        all_matches = []
                        # q search always checks all levels and does an OR between them
                        for lvl in ["sim", "func", "file"]:
                            matches = get_group_targets(lvl, word)
                            if matches:
                                all_matches.extend(matches)

                        if not all_matches:
                            return jsonify(
                                {
                                    "total": 0,
                                    "pairs": [],
                                    "algo": algo,
                                    "collection": col,
                                    "pool_truncated": False,
                                    "q": search_q,
                                }
                            )

                        add_group(all_matches, field_name=f"q({word})")

                if cross_binary_val is not None:
                    cb_bool = cross_binary_val.lower() == "true"
                    cb_key = (
                        f"{col}:sim:is_cross_binary:{'true' if cb_bool else 'false'}"
                    )
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
                        return jsonify(
                            {
                                "total": 0,
                                "pairs": [],
                                "algo": algo,
                                "collection": col,
                                "pool_truncated": False,
                                "offset": offset,
                                "limit": limit,
                            }
                        )

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
                    "collection": col,
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
                    return jsonify({"detail": f"Search engine error: {lua_err}"}), 500

                metrics["inter_time"] = time.perf_counter() - t_lua_start
            except Exception as e:
                logging.error(f"Search preparation error: {e}")
                raise e

        # --- PER-PAGE ENRICHMENT (Dynamic for Deep Selection Architecture) ---
        t_enrich_start = time.perf_counter()
        enriched_pairs = []
        if page_results:
            # Phase 1: Fetch Similarity Metrics & identity
            pipe = r.pipeline()
            for sid, sort_sc in page_results:
                pipe.json().get(sid, "$")
                # Fetch cross-metric (e.g. if sorting by score, fetch features count)
                if sort_by == "score":
                    pipe.zscore(min_features_zset, sid)
                else:
                    pipe.zscore(algo_zset, sid)

            enrichment_raw = pipe.execute()

            # Helper to extract IDs from SID (same logic as Lua)
            def extract_from_sid(sid):
                # sid is {col}:sim:{algo}:{clean_id1}::{clean_id2}
                sim_prefix = f"{col}:sim:{algo}:"
                if not sid.startswith(sim_prefix):
                    return None, None

                rest = sid[len(sim_prefix) :]
                parts = rest.split("::")
                if len(parts) != 2:
                    return None, None

                # Reconstruct full IDs
                id1 = f"{col}:func:{parts[0]}"
                id2 = f"{col}:func:{parts[1]}"
                return id1, id2

            # Phase 2: Pipeline fetch for function-specific metadata
            meta_pipe = r.pipeline()
            f_id_map = {}  # Maps sid to (id1, id2)

            for i, (sid, sort_sc) in enumerate(page_results):
                raw_json = enrichment_raw[i * 2]
                if not raw_json:
                    continue
                data = raw_json[0] if isinstance(raw_json, list) else raw_json
                if isinstance(data, str):
                    data = json.loads(data)

                id1 = data.get("id1")
                id2 = data.get("id2")
                if not id1 or not id2:
                    id1, id2 = extract_from_sid(sid)

                if id1 and id2:
                    f_id_map[sid] = (id1, id2, data, enrichment_raw[i * 2 + 1], sort_sc)
                    meta_pipe.json().get(f"{id1}:meta", "$")
                    meta_pipe.json().get(f"{id2}:meta", "$")
                    meta_pipe.smembers(f"{id1}:clusters")
                    meta_pipe.smembers(f"{id2}:clusters")
                    meta_pipe.hgetall(f"{id1}:cluster_scores")
                    meta_pipe.hgetall(f"{id2}:cluster_scores")

                    # Also fetch file-level metadata for tags
                    try:
                        md5_1 = id1.split(":")[2]
                        md5_2 = id2.split(":")[2]
                        meta_pipe.json().get(f"{col}:file:{md5_1}:meta", "$")
                        meta_pipe.json().get(f"{col}:file:{md5_2}:meta", "$")
                    except:
                        meta_pipe.json().get("nonexistent", "$")
                        meta_pipe.json().get("nonexistent", "$")

            meta_results = meta_pipe.execute()

            # Collect all cluster IDs to fetch metadata for
            cluster_id_to_fetch = set()
            func_clusters_ids = {}  # maps sid -> (list1, list2)

            for i, sid in enumerate(f_id_map.keys()):
                c1_res = meta_results[i * 8 + 2]
                c2_res = meta_results[i * 8 + 3]

                c1_ids = (
                    [c.decode() if isinstance(c, bytes) else c for c in c1_res]
                    if c1_res
                    else []
                )
                c2_ids = (
                    [c.decode() if isinstance(c, bytes) else c for c in c2_res]
                    if c2_res
                    else []
                )

                func_clusters_ids[sid] = (c1_ids, c2_ids)
                for cid in c1_ids + c2_ids:
                    cluster_id_to_fetch.add(cid)

            # Fetch all cluster metadata in one go
            cluster_meta_map = {}
            if cluster_id_to_fetch:
                c_pipe = r.pipeline()
                algo = "unweighted_cosine"
                c_list = list(cluster_id_to_fetch)
                for cid in c_list:
                    c_pipe.json().get(f"{col}:cluster:{algo}:{cid}:meta", "$")
                c_results = c_pipe.execute()
                for cid, res in zip(c_list, c_results):
                    if res:
                        cm = res[0] if isinstance(res, list) else res
                        if isinstance(cm, str):
                            cm = json.loads(cm)
                        cluster_meta_map[cid] = cm

            # Map meta results back
            for i, sid in enumerate(f_id_map.keys()):
                id1, id2, sim_data, other_metric, sid_sort_sc = f_id_map[sid]

                id1 = sim_data.get("id1")
                id2 = sim_data.get("id2")

                m1_json = meta_results[i * 8]
                m2_json = meta_results[i * 8 + 1]
                f1_json = meta_results[i * 8 + 6]
                f2_json = meta_results[i * 8 + 7]
                s1_res = meta_results[i * 8 + 4] or {}
                s2_res = meta_results[i * 8 + 5] or {}

                m1 = (m1_json[0] if isinstance(m1_json, list) else m1_json) or {}
                m2 = (m2_json[0] if isinstance(m2_json, list) else m2_json) or {}
                f1 = (f1_json[0] if isinstance(f1_json, list) else f1_json) or {}
                f2 = (f2_json[0] if isinstance(f2_json, list) else f2_json) or {}

                if isinstance(m1, str):
                    m1 = json.loads(m1)
                if isinstance(m2, str):
                    m2 = json.loads(m2)
                if isinstance(f1, str):
                    f1 = json.loads(f1)
                if isinstance(f2, str):
                    f2 = json.loads(f2)

                sim_score = (
                    float(sid_sort_sc)
                    if sort_by == "score"
                    else float(other_metric or 0)
                )
                feat_count = (
                    float(sid_sort_sc)
                    if sort_by in ["feat_count", "min_features"]
                    else float(other_metric or 0)
                )

                # Construct 'clusters' list for UI consistency
                c1_ids, c2_ids = func_clusters_ids[sid]

                clusters1 = []
                for cid in c1_ids:
                    cm = cluster_meta_map.get(cid)
                    if cm:
                        cid_str = str(cid)
                        score = float(
                            s1_res.get(
                                (
                                    cid_str.encode()
                                    if isinstance(cid_str, str)
                                    else cid_str
                                ),
                                0.0,
                            )
                        )
                        if not score and isinstance(s1_res, dict):
                            for k, v in s1_res.items():
                                k_str = k.decode() if isinstance(k, bytes) else k
                                if k_str == cid_str:
                                    score = float(v)
                                    break
                        clusters1.append(
                            {
                                "cluster_id": cm.get("cluster_id"),
                                "cluster_uuid": cm.get("cluster_uuid"),
                                "cluster_name": cm.get("cluster_name"),
                                "cohesion_score": cm.get("cohesion_score", 0),
                                "member_count": cm.get("member_count", 0),
                                "cluster_stability": score
                                or cm.get("cluster_stability", 0.0),
                                "avg_features": cm.get("avg_features", 0),
                            }
                        )
                clusters1.sort(key=lambda x: x.get("member_count", 0), reverse=True)

                clusters2 = []
                for cid in c2_ids:
                    cm = cluster_meta_map.get(cid)
                    if cm:
                        cid_str = str(cid)
                        score = float(
                            s2_res.get(
                                (
                                    cid_str.encode()
                                    if isinstance(cid_str, str)
                                    else cid_str
                                ),
                                0.0,
                            )
                        )
                        if not score and isinstance(s2_res, dict):
                            for k, v in s2_res.items():
                                k_str = k.decode() if isinstance(k, bytes) else k
                                if k_str == cid_str:
                                    score = float(v)
                                    break
                        clusters2.append(
                            {
                                "cluster_id": cm.get("cluster_id"),
                                "cluster_uuid": cm.get("cluster_uuid"),
                                "cluster_name": cm.get("cluster_name"),
                                "cohesion_score": cm.get("cohesion_score", 0),
                                "member_count": cm.get("member_count", 0),
                                "cluster_stability": score
                                or cm.get("cluster_stability", 0.0),
                                "avg_features": cm.get("avg_features", 0),
                            }
                        )
                clusters2.sort(key=lambda x: x.get("member_count", 0), reverse=True)

                for field in [
                    "cluster_id",
                    "cluster_name",
                    "cluster_uuid",
                    "cluster_stability",
                ]:
                    m1.pop(field, None)
                    m2.pop(field, None)
                enriched_pairs.append(
                    {
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
                        "entry_date": parse_timestamp(sim_data.get("entry_date")),
                        "meta1": {
                            "file_md5": m1.get("file_md5"),
                            "file_name": m1.get("file_name"),
                            "tags": m1.get("tags", []),
                            "user_tags": m1.get("user_tags", []),
                            "batch_uuid": m1.get("batch_uuid"),
                            "language_id": m1.get("language_id"),
                            "return_type": m1.get("return_type", "N/A"),
                            "namespace": m1.get("namespace", ""),
                            "parameters": m1.get("parameters", []),
                            "bsim_features_count": m1.get("bsim_features_count"),
                            "clusters": clusters1,
                            "file_tags": f1.get("tags", []),
                            "file_user_tags": f1.get("user_tags", []),
                        },
                        "meta2": {
                            "file_md5": m2.get("file_md5"),
                            "file_name": m2.get("file_name"),
                            "tags": m2.get("tags", []),
                            "user_tags": m2.get("user_tags", []),
                            "batch_uuid": m2.get("batch_uuid"),
                            "language_id": m2.get("language_id"),
                            "return_type": m2.get("return_type", "N/A"),
                            "namespace": m2.get("namespace", ""),
                            "parameters": m2.get("parameters", []),
                            "bsim_features_count": m2.get("bsim_features_count"),
                            "clusters": clusters2,
                            "file_tags": f2.get("tags", []),
                            "file_user_tags": f2.get("user_tags", []),
                        },
                        "tags": sim_data.get("tags", []),
                        "user_tags": sim_data.get("user_tags", []),
                        "algo": algo,
                    }
                )

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

        resp = jsonify(
            {
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
                "pairs": enriched_pairs,
                "sort_by": sort_by,
                "sort_order": sort_order,
                "cached_response": cache_hit,
            }
        )
        return resp

    except Exception as e:
        import traceback

        logging.error(f"Similarity search error: {e}")
        traceback.print_exc()
        return jsonify({"detail": str(e)}), 500
