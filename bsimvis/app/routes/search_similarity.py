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
CACHE_TIME_THRESHOLD = 0.1 # Only cache requests that take more than X seconds
MAX_CACHED_RESULTS = 10000

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
    user_tag_filter = request.args.get("user_tag", "").lower()
    sim_tag_filter = request.args.get("sim_tag", "").lower()
    lang_filter = request.args.get("language", "").lower()
    md5_filters = request.args.getlist("md5")
    cross_binary_val = request.args.get("cross_binary")

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
            "pool_limit": pool_limit, # CRITICAL: Include pool_limit in cache hash
            "cross_binary": cross_binary_val,
            "q": (request.args.get("q") or "").strip().lower(),
            "name": (request.args.get("name") or "").strip().lower(),
            "tag": (request.args.get("tag") or "").strip().lower(),
            "user_tag": (request.args.get("user_tag") or "").strip().lower(),
            "md5": (request.args.get("md5") or "").strip().lower(),
            "id": (request.args.get("id") or "").strip().lower(),
            "language_id": (request.args.get("language_id") or "").strip().lower(),
            "batch_uuid": (request.args.get("batch_uuid") or "").strip().lower(),
            "sort_by": (request.args.get("sort_by") or "").strip().lower(),
            "sort_order": (request.args.get("sort_order") or "").strip().lower(),
        }
        # Include all other filters
        for f in ["md5", "id", "language_id", "batch_uuid"]:
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
            metrics["cache_lookup"] = m_cache_lookup_time

            try:
                # --- LUA SCRIPT SETUP ---
                from bsimvis.app.services.lua_manager import lua_manager
                search_script = lua_manager.get_script("search_similarity")

                t_lua_prep = time.perf_counter()
                groups_raw = []

                def get_group_targets(lvl, val, allowed_fields=None):
                    """
                    Resolves a filter into raw identity base-keys 
                    using the standardized registry->bucket hierarchy.
                    Returns a list of (lvl, targets, field) tuples for all matches.
                    """
                    if not allowed_fields:
                        allowed_fields = ["tags", "user_tags", "function_name", "file_name", "file_md5", "language_id"]
                    
                    val_lower = val.lower()
                    matches = []
                    
                    for field in allowed_fields:
                        # # 1. Try Exact Match (O(1))
                        # exact_key = f"idx:{col}:idx:{lvl}:{field}:{val_lower}"
                        # if r.exists(exact_key):
                        #     if lvl == "sim":
                        #         matches.append((lvl, [exact_key.split(":")[-1]], field))
                        #     else:
                        #         targets = [t.decode() if isinstance(t, bytes) else str(t) for t in r.smembers(exact_key)]
                        #         logging.info(f"SIM SEARCH | {session_id} | Resolved {len(targets)} exact {lvl}-level targets for '{field}:{val}'")
                        #         matches.append((lvl, targets, field))
                        
                        # 2. Try Registry-Based Partial Match (SSCAN)
                        registry_key = f"{col}:reg:{lvl}:{field}"
                        if r.exists(registry_key):
                            matching_buckets = []
                            try:
                                for bucket in r.sscan_iter(registry_key, match=f"*{val_lower}*"):
                                    bucket_str = bucket.decode() if isinstance(bucket, bytes) else str(bucket)
                                    if val_lower in bucket_str.lower():
                                        matching_buckets.append(bucket_str)
                            except Exception as e:
                                logging.warning(f"SSCAN failed for {registry_key}: {e}")
                                pass
                            
                            if matching_buckets:
                                targets = []
                                if lvl == "sim":
                                    targets = [b.split(":")[-1] for b in matching_buckets]
                                else:
                                    if len(matching_buckets) == 1:
                                        targets = [t.decode() if isinstance(t, bytes) else str(t) for t in r.smembers(matching_buckets[0])]
                                    else:
                                        # Use SUNION for multiple buckets
                                        targets = [t.decode() if isinstance(t, bytes) else str(t) for t in r.sunion(*matching_buckets)]
                                
                                logging.info(f"SIM SEARCH | {session_id} | Resolved {len(targets)} partial {lvl}-level targets across {len(matching_buckets)} buckets for '{field}:{val}'")
                                matches.append((lvl, targets, field))
                    
                    return matches

                def add_group(sub_matches, field_name="q"):
                    """
                    Adds a metadata group to the Lua config.
                    Supports sub_groups for OR logic within a single search term.
                    """
                    if not sub_matches: return
                    
                    # Group normalization for Lua
                    normalized_subs = []
                    total_weight = 0
                    
                    prefix_map = {
                        "binary": f"{col}:sim:involves:file:",
                        "function": f"{col}:sim:involves:func:",
                        "similarity": f"{col}:idx:sim:tags:" 
                    }
                    
                    for lvl, targets, field in sub_matches:
                        l_name = "binary" if lvl == "file" else "function" if lvl == "func" else "similarity"
                        p = prefix_map.get(l_name)
                        if l_name == "similarity" and field == "user_tags":
                            p = f"{col}:idx:sim:user_tags:"
                        
                        weight = 0
                        if p:
                            for t in targets:
                                try:
                                    weight += r.scard(f"{p}{t}")
                                except: pass
                        
                        total_weight += weight
                        normalized_subs.append({
                            "level": l_name,
                            "targets": targets[:1000],
                            "field": field
                        })
                    
                    groups_raw.append({
                        "type": "metadata",
                        "field": field_name,
                        "sub_groups": normalized_subs,
                        "weight": total_weight
                    })

                # Group resolution (Unified Involves Architecture)
                field_map = {
                    "language_id": ["language_id"],
                    "name": ["function_name", "file_name"],
                    "tags": ["tags", "user_tags"],
                    "user_tags": ["user_tags"]
                }
                for f_name, f_val in [
                    ("language_id", lang_filter), 
                    ("name", name_filter), 
                    ("tags", tag_filter), 
                    ("user_tags", user_tag_filter),
                    ("tags", sim_tag_filter) 
                ]:
                    if f_val:
                        levels = ["sim", "func", "file"]
                        if f_val == sim_tag_filter and f_name == f_name == "tags":
                            levels = ["sim"]
                        
                        all_matches = []
                        allowed = field_map.get(f_name)
                        for lvl in levels:
                            matches = get_group_targets(lvl, f_val, allowed_fields=allowed)
                            if matches:
                                all_matches.extend(matches)
                        
                        if not all_matches:
                            logging.info(f"SIM SEARCH | {session_id} | Filter '{f_name}={f_val}' matched 0 targets. Returning empty.")
                            return jsonify({"total": 0, "pairs": [], "algo": algo, "collection": col, "pool_truncated": False})
                        
                        add_group(all_matches, field_name=f_name)

                if md5_filters:
                    all_md5_base_ids = []
                    for m in md5_filters:
                        if not m: continue
                        matches = get_group_targets("file", m, allowed_fields=["file_md5"])
                        for _, tgts, _ in matches:
                            all_md5_base_ids.extend(tgts)
                    
                    if not all_md5_base_ids:
                        return jsonify({"total": 0, "pairs": [], "algo": algo, "collection": col, "pool_truncated": False})
                        
                    # MD5 is binary-only, but we wrap in a sub-group format for add_group
                    md5_submatches = [("file", list(set(all_md5_base_ids)), "md5")]
                    add_group(md5_submatches, field_name="md5")

                if search_q:
                    for word in [w for w in search_q.split() if w.strip()]:
                        all_matches = []
                        # q search always checks all levels and does an OR between them
                        for lvl in ["sim", "func", "file"]:
                            matches = get_group_targets(lvl, word)
                            if matches:
                                all_matches.extend(matches)
                        
                        if not all_matches:
                            return jsonify({"total": 0, "pairs": [], "algo": algo, "collection": col, "pool_truncated": False, "q": search_q})
                        
                        add_group(all_matches, field_name=f"q({word})")

                if cross_binary_val is not None:
                    cb_bool = cross_binary_val.lower() == "true"
                    cb_key = f"{col}:sim:is_cross_binary:{'true' if cb_bool else 'false'}"
                    if r.exists(cb_key):
                        groups_raw.append({
                            "type": "direct_zset",
                            "field": "cross_binary",
                            "key": cb_key,
                            "weight": r.zcard(cb_key)
                        })
                    else:
                        # Essential: If user filters by cross_binary but no such pairs exist, return empty
                        logging.info(f"SIM SEARCH | {session_id} | Cross-Binary Filter '{cross_binary_val}' matched 0 pairs (Key {cb_key} missing)")
                        return jsonify({
                            "total": 0, 
                            "pairs": [], 
                            "algo": algo, 
                            "collection": col,
                            "pool_truncated": False,
                            "offset": offset,
                            "limit": limit
                        })

                # Similarity Score Group
                sim_weight = r.zcount(algo_zset, min_score, max_score)
                groups_raw.append({
                    "type": "score_range",
                    "field": "similarity",
                    "weight": sim_weight,
                    "min": min_score,
                    "max": max_score,
                    "key": algo_zset
                })

                # Feature Count Group
                if min_features > 0 or sort_by in ["feat_count", "min_features"]:
                    feat_weight = r.zcount(min_features_zset, min_features, "+inf") if min_features > 0 else r.zcard(min_features_zset)
                    groups_raw.append({
                        "type": "feature_range",
                        "field": "min_feature_count",
                        "weight": feat_weight,
                        "min": min_features,
                        "key": min_features_zset
                    })

                # Sort all groups by weight to find the best Producer (Step 2 of Lua)
                # Boost priority of the group that matches our sort_by metric
                for g in groups_raw:
                    if sort_by == "score" and g["type"] == "score_range":
                        g["weight"] = max(0, g["weight"] - 5000)
                    elif (sort_by == "feat_count" or sort_by == "min_features") and g["type"] == "feature_range":
                        g["weight"] = max(0, g["weight"] - 5000)

                groups = sorted(groups_raw, key=lambda x: x["weight"])

                # --- LUA CONFIG ---
                lua_config = {
                    "collection": col,
                    "algo": algo,
                    "pool_limit": pool_limit,
                    "groups": groups,  # Use the sorted groups
                    "offset": offset,  # Lua performs pagination
                    "limit": limit,
                    "min_score": min_score,
                    "max_score": max_score,
                    "sort_by": sort_by,
                    "sort_order": sort_order
                }

                # Exec Lua Search (Unified Involves Architecture)
                t_lua_start = time.perf_counter()
                # We only pass keys that need direct ZSET access or global metric access
                keys = [algo_zset, min_features_zset]
                for g in groups_raw:
                    if g["type"] == "direct_zset": keys.append(g["key"])
                
                try:
                    import json as std_json
                    logging.info(f"SIM SEARCH LUA_CONFIG: {std_json.dumps(lua_config, indent=2)}")
                    res = search_script(keys=keys, args=[json.dumps(lua_config)])
                    total = res[0]
                    pool_truncated = bool(res[1])
                    all_ids = res[2]
                    all_scores = res[3]
                    if all_ids:
                        logging.info(f"SIM SEARCH | {session_id} | Lua First Result: {all_ids[0]} -> {all_scores[0]}")
                    
                    if use_cache and (all_ids or total == 0):
                        cache_data = {"total": total, "pool_truncated": pool_truncated, "ids": all_ids, "scores": all_scores}
                        r.setex(cache_key, 3600, json.dumps(cache_data))
                    
                    # Direct assignment, as Lua already handled pagination
                    page_results = list(zip(all_ids, all_scores))
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
                # sid is idx:coll:sim:algo:idx:coll:func:md5:addr:idx:coll:func:md5:addr
                sim_prefix = f"idx:{col}:sim:{algo}:"
                if not sid.startswith(sim_prefix): return None, None
                
                rest = sid[len(sim_prefix):]
                # We look for the start of the second ID: ':idx:{col}:func:'
                sep = f":idx:{col}:func:"
                pivot = rest.find(sep, 2)
                if pivot == -1: return None, None
                
                id1 = rest[:pivot]
                id2 = rest[pivot+1:]
                return id1, id2

            # Phase 2: Pipeline fetch for function-specific metadata
            meta_pipe = r.pipeline()
            f_id_map = {} # Maps sid to (id1, id2)
            
            for i, (sid, sort_sc) in enumerate(page_results):
                raw_json = enrichment_raw[i*2]
                if not raw_json: continue
                data = raw_json[0] if isinstance(raw_json, list) else raw_json
                if isinstance(data, str): data = json.loads(data)
                
                id1 = data.get("id1")
                id2 = data.get("id2")
                if not id1 or not id2:
                    id1, id2 = extract_from_sid(sid)
                
                if id1 and id2:
                    f_id_map[sid] = (id1, id2, data, enrichment_raw[i*2 + 1], sort_sc)
                    meta_pipe.json().get(f"{id1}:meta", "$")
                    meta_pipe.json().get(f"{id2}:meta", "$")
            
            meta_results = meta_pipe.execute()
            
            # Map meta results back
            for i, sid in enumerate(f_id_map.keys()):
                id1, id2, sim_data, other_metric, sid_sort_sc = f_id_map[sid]
                
                # Standard ID extraction: idx:col:sim:algo:id1:id2
                # id1 is idx:col:func:md5:addr (6 parts)
                # Standard Key: idx:col:sim:algo:idx:col:func:md5:addr:idx:col:func:md5:addr 
                # (Actually, let's keep it robust by getting them from sim_data)
                id1 = sim_data.get("id1")
                id2 = sim_data.get("id2")
                
                m1_json = meta_results[i*2]
                m2_json = meta_results[i*2 + 1]
                
                m1 = (m1_json[0] if isinstance(m1_json, list) else m1_json) or {}
                m2 = (m2_json[0] if isinstance(m2_json, list) else m2_json) or {}
                if isinstance(m1, str): m1 = json.loads(m1)
                if isinstance(m2, str): m2 = json.loads(m2)

                sim_score = float(sid_sort_sc) if sort_by == "score" else float(other_metric or 0)
                feat_count = float(sid_sort_sc) if sort_by in ["feat_count", "min_features"] else float(other_metric or 0)

                enriched_pairs.append({
                    "id1": id1,
                    "id2": id2,
                    "name1": m1.get("function_name", id1.split(":")[-1] if id1 else "N/A"),
                    "name2": m2.get("function_name", id2.split(":")[-1] if id2 else "N/A"),
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
                        "bsim_features_count": m1.get("bsim_features_count"),
                    },
                    "meta2": {
                        "file_md5": m2.get("file_md5"),
                        "file_name": m2.get("file_name"),
                        "tags": m2.get("tags", []),
                        "user_tags": m2.get("user_tags", []),
                        "batch_uuid": m2.get("batch_uuid"),
                        "language_id": m2.get("language_id"),
                        "return_type": m2.get("return_type", "N/A"),
                        "bsim_features_count": m2.get("bsim_features_count"),
                    },
                    "tags": sim_data.get("tags", []),
                    "user_tags": sim_data.get("user_tags", []),
                    "algo": algo,
                })

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
