import json
import logging
import hashlib
import os
from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids, parse_timestamp

search_similarity_bp = Blueprint("search_similarity", __name__)

DEFAULT_LIMIT = 100  # API RESULT LIMIT
DEFAULT_POOL_LIMIT = 1000000  # DATABASE FILTERING LIMIT
MAX_POOL_LIMIT = 1000000
CACHE_TIME_THRESHOLD = 0.1 # Only cache requests that take more than X seconds
MAX_CACHED_RESULTS = 10000

@search_similarity_bp.route("/api/similarity/search", methods=["GET"])
def similarity_search():
    import time
    import uuid
    import threading
    
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
        "enrich_time": 0
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
    search_q = request.args.get("q", "").lower()
    name_filter = request.args.get("name", "").lower()
    tag_filter = request.args.get("tag", "").lower()
    lang_filter = request.args.get("language", "").lower()
    md5_filters = request.args.getlist("md5")
    is_cross_binary = request.args.get("cross_binary", "false").lower() == "true"

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

        algo_zset = f"{col}:all_sim:{algo}"
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
            "pool_limit": pool_limit, # CRITICAL: Include pool_limit in cache hash
            "cross_binary": is_cross_binary,
            "q": (request.args.get("q") or "").strip().lower(),
            "name": (request.args.get("name") or "").strip().lower(),
            "tag": (request.args.get("tag") or "").strip().lower(),
            "md5": (request.args.get("md5") or "").strip().lower(),
            "id": (request.args.get("id") or "").strip().lower(),
            "language_id": (request.args.get("language_id") or "").strip().lower(),
            "batch_uuid": (request.args.get("batch_uuid") or "").strip().lower(),
            "sort_by": (request.args.get("sort_by") or "").strip().lower(),
            "sort_order": (request.args.get("sort_order") or "").strip().lower(),
        }
        # Include all other filters
        for f in ["md5_", "id", "language_id", "batch_uuid"]:
            v = request.args.get(f)
            if v:
                cache_params[f] = v.strip().lower()

        cache_hash = hashlib.md5(json.dumps(cache_params, sort_keys=True).encode()).hexdigest()
        m_hash_prep_time = time.perf_counter() - t_hash_start
        metrics["hash_prep"] = m_hash_prep_time
        
        cache_key = f"cache:search:sim:{cache_hash}"

        t_cache_lookup_start = time.perf_counter()
        use_cache = request.args.get("use_cache", "true").lower() == "true"
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
                
                if offset + limit <= len(c_ids) or len(c_ids) == total: # Either we have enough results or the cache is exact
                    page_results = list(zip(c_ids[offset : offset + limit], c_scores[offset : offset + limit]))
                    cache_hit = True
                    r.expire(cache_key, 300)
                    
                    logging.info(f"SIM SEARCH [@CACHE] HIT {cache_key}: lookup={m_cache_lookup_time:.3f}s, total={total}")
            except Exception as e:
                logging.warning(f"Cache parse failed for {cache_key}: {e}")

        if not cache_hit:
            logging.info(f"SIM SEARCH [@CACHE] MISS {cache_key}: lookup={m_cache_lookup_time:.3f}s, total={total} VS looking for {offset + limit}")
            start_time = time.perf_counter()
            # Collect all ZSET keys for the final intersection
            temp_keys = []
            metrics["cache_lookup"] = m_cache_lookup_time

            try:
                # 1. Load and Register Lua Search Engine
                lua_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lua", "search_similarity.lua")
                with open(lua_path, 'r') as f:
                    lua_script_content = f.read()
                
                search_script = r.register_script(lua_script_content)

                t_lua_prep = time.perf_counter()
                bucket_keys = []
                groups = []

                def get_bucket_idx(prefix, val):
                    # Helper to find buckets and return their index in the bucket_keys list
                    # We check both suffix 1 and 2 for categorical fields
                    prefixes = [f"{prefix}1", f"{prefix}2"] if prefix in ["name", "tags", "id", "batch_uuid", "language_id", "md5"] else [prefix]
                    found_for_filter = []
                    target_lower = val.lower().replace("*", "")

                    for p in prefixes:
                        registry_key = f"idx:{col}:reg:{p}"
                        if not r.exists(registry_key): continue
                        
                        all_buckets = r.smembers(registry_key)
                        for b_key in all_buckets:
                            b_key_str = b_key.decode() if isinstance(b_key, bytes) else str(b_key)
                            b_key_lower = b_key_str.lower()
                            
                            # Match against original target
                            if target_lower in b_key_lower:
                                if b_key_str not in bucket_keys:
                                    bucket_keys.append(b_key_str)
                                
                                b_idx = bucket_keys.index(b_key_str) + 1
                                if b_idx not in found_for_filter:
                                    found_for_filter.append(b_idx) # 1-indexed for Lua
                                
                                if len(found_for_filter) >= 1000: break
                        if len(found_for_filter) >= 1000: break
                    
                    if found_for_filter:
                        logging.info(f"SIM SEARCH | {session_id} | Resolved {len(found_for_filter)} buckets for '{val}'")
                    return found_for_filter

                # Build Logical AND Groups
                # Each group is an OR-collection of buckets
                
                # Group: Language
                if lang_filter:
                    g = get_bucket_idx("language_id", lang_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    groups.append(g)

                # Group: Name
                if name_filter:
                    g = get_bucket_idx("name", name_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    groups.append(g)

                # Group: Tag
                if tag_filter:
                    g = get_bucket_idx("tags", tag_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    groups.append(g)

                # Group: MD5s (Unions of all MD5s)
                if md5_filters:
                    g = []
                    for m in md5_filters:
                        g.extend(get_bucket_idx("md5", m))
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    groups.append(g)

                # Group: Search Query (Each word is an AND group containing multiple OR buckets)
                if search_q:
                    for word in [w for w in search_q.split() if w.strip()]:
                        g = []
                        for p in ["name", "tags", "id", "language_id"]:
                            g.extend(get_bucket_idx(p, word))
                        if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                        groups.append(g)

                # Group: Cross Binary
                if is_cross_binary:
                    cb_key = f"idx:{col}:sim:is_cross_binary:true"
                    if r.exists(cb_key):
                        if cb_key not in bucket_keys: bucket_keys.append(cb_key)
                        groups.append([bucket_keys.index(cb_key) + 1])
                    else: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})

                # Sort groups by estimated size (smallest first) to optimize Lua iteration
                # This is a crude optimization but helps
                groups.sort(key=lambda x: len(x))

                lua_config = {
                    "groups": groups,
                    "pool_limit": pool_limit,
                    "min_score": min_score,
                    "max_score": max_score,
                    "min_features": min_features,
                    "offset": offset,
                    "limit": limit,
                    "sort_by": sort_by,
                    "sort_order": sort_order
                }

                logging.info(f"SIM SEARCH | {session_id} | Buckets: {len(bucket_keys)}, Groups: {len(groups)}")
                
                metrics["filter_resolve"] = time.perf_counter() - t_lua_prep
                
                # 2. Execute Lua Search
                t_lua_start = time.perf_counter()
                keys = [algo_zset, f"idx:{col}:sim:feat_count1" if sort_by == "feat_count" else f"idx:{col}:sim:min_features"] + bucket_keys
                try:
                    # Execute pre-registered script
                    res = search_script(keys=keys, args=[json.dumps(lua_config)])
                    total = res[0]
                    pool_truncated = bool(res[1])
                    all_ids = res[2]
                    all_scores = res[3]
                    
                    # 3. Cache results (Top-1000)
                    t_cache_write_start = time.perf_counter()
                    if use_cache and (all_ids or total == 0):
                        cache_data = {
                            "total": total,
                            "pool_truncated": pool_truncated,
                            "ids": all_ids,
                            "scores": all_scores
                        }
                        r.setex(cache_key, 3600, json.dumps(cache_data))
                    metrics["cache_write"] = time.perf_counter() - t_cache_write_start

                    # 4. Slice for current page
                    page_results = list(zip(all_ids[offset : offset + limit], all_scores[offset : offset + limit]))

                except Exception as lua_err:
                    logging.error(f"LUA SEARCH CRASH: {lua_err}")
                    return jsonify({"detail": f"Search engine error: {lua_err}"}), 500

                metrics["inter_time"] = time.perf_counter() - t_lua_start
                metrics["prep_time"] = 0
                metrics["mask_time"] = 0
                
            finally:
                pass

        # --- PER-PAGE ENRICHMENT ---
        t_enrich_start = time.perf_counter()
        enriched_pairs = []
        if page_results:
            pipe = r.pipeline()
            for sid, sort_sc in page_results:
                # Fetch missing metrics and metadata only for this page
                if sort_by == "score":
                    # We have the score, fetch feature count
                    pipe.zscore(f"idx:{col}:sim:feat_count1", sid)
                else:
                    # We have the feature count, fetch similarity score
                    pipe.zscore(algo_zset, sid)
                pipe.json().get(sid, "$")
            
            enrichment_raw = pipe.execute()

            for i, (sid, sort_sc) in enumerate(page_results):
                other_metric = float(enrichment_raw[i*2] or 0)
                raw_json = enrichment_raw[i*2 + 1]
                
                if not raw_json:
                    continue
                
                data = raw_json[0] if isinstance(raw_json, list) and raw_json else raw_json
                if isinstance(data, str):
                    data = json.loads(data)

                # Assign metrics correctly based on sort_by
                if sort_by == "score":
                    sim_score = float(sort_sc)
                    feat_count = float(other_metric)
                else:
                    sim_score = float(other_metric)
                    feat_count = float(sort_sc)
                
                id1 = data.get("id1")
                id2 = data.get("id2")
                tags1 = data.get("tags1", "").split(",") if data.get("tags1") else []
                tags2 = data.get("tags2", "").split(",") if data.get("tags2") else []

                enriched_pairs.append(
                    {
                        "id1": id1,
                        "id2": id2,
                        "name1": data.get("name1", (id1 or ":").split(":")[-1]),
                        "name2": data.get("name2", (id2 or ":").split(":")[-1]),
                        "score": sim_score,
                        "feat_count": int(feat_count),
                        "sid": sid,
                        "entry_date": parse_timestamp(data.get("entry_date")),

                        "meta1": {
                            "file_md5": data.get("md5_1"),
                            "tags": tags1,
                            "batch_uuid": data.get("batch_uuid1"),
                            "language_id": data.get("language_id1"),
                            "return_type": data.get("return_type1", "N/A"),
                            "bsim_features_count": data.get("feat_count1"),
                        },
                        "meta2": {
                            "file_md5": data.get("md5_2"),
                            "tags": tags2,
                            "batch_uuid": data.get("batch_uuid2"),
                            "language_id": data.get("language_id2"),
                            "return_type": data.get("return_type2", "N/A"),
                            "bsim_features_count": data.get("feat_count2"),
                        },
                    }
                )
        
        metrics["enrich_time"] = time.perf_counter() - t_enrich_start
        total_time = time.perf_counter() - t_req_all_start

        # FINAL CONSOLIDATED PERFORMANCE LOGGING (CLEAN VERSION)
        cache_status = "HIT" if cache_hit else "MISS"
        is_fast_path = " [FastPath]" if not cache_hit and len(intersection_configs) == 1 else ""
        
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
                "language": request.args.get("language", ""),
                "md5": md5_filters,
                "cross_binary": is_cross_binary,
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
