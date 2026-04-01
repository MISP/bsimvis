import json
import logging
import hashlib

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids

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
                # 1. Resolve categorical filters (Substrings/Tags/MD5s)
                def resolve_to_temp_zset(tag_fields_and_vals):
                    """
                    Resolves multiple tag fields/values into a single temp ZSET.
                    Returns (temp_key_name or None)
                    """
                    nonlocal pool_truncated
                    found_keys = []
                    for field_prefix, val in tag_fields_and_vals:
                        if not val:
                            continue
                        
                        # Determine resolution strategy: use registry SSCAN if available
                        registry_key = f"idx:{col}:reg:{field_prefix}"
                        # For compound prefixes like tags1, we might need to check multiple or handle wildcards
                        # but typically we have specific registries: name1, name2, tags1, tags2
                        has_registry = r.exists(registry_key)
                        
                        target_sub = val if "*" in val else f"*{val}*"
                        patterns = [f"idx:{col}:sim:{field_prefix}*:{target_sub}"]
                        if val.lower() != val:
                            patterns.append(f"idx:{col}:sim:{field_prefix}*:{val.lower() if '*' in val else f'*{val.lower()}*'}")

                        for pat in patterns:
                            cursor = 0
                            while True:
                                if has_registry:
                                    # FAST PATH: Scan the registry Set
                                    cursor, sub_keys = r.sscan(registry_key, cursor=cursor, match=pat, count=5000)
                                else:
                                    # SLOW FALLBACK: Global Scan
                                    cursor, sub_keys = r.scan(cursor=cursor, match=pat, count=5000)
                                    
                                if sub_keys:
                                    found_keys.extend(sub_keys)
                                if cursor == 0 or len(found_keys) >= pool_limit:
                                    break
                            if len(found_keys) >= pool_limit:
                                break
                    
                    if found_keys:
                        nonlocal filter_keys_found
                        filter_keys_found += len(found_keys)
                        if len(found_keys) >= pool_limit:
                            found_keys = found_keys[:pool_limit]
                            pool_truncated = True
                            
                        found_keys = list(set(found_keys))
                        friendly_f = tag_fields_and_vals[0][0] # use first field for the name
                        u_key = f"tmp:{col}:filter:{session_id}:union:{friendly_f}"
                        
                        # Process each found key
                        batch_size = 500
                        for i in range(0, len(found_keys), batch_size):
                            batch = found_keys[i: i + batch_size]
                            if i == 0:
                                r.zunionstore(u_key, batch)
                            else:
                                r.zunionstore(u_key, [u_key] + batch)
                        
                        if r.zcard(u_key) > pool_limit:
                            if sort_order == "desc":
                                r.zremrangebyrank(u_key, 0, -(pool_limit + 1))
                            else:
                                r.zremrangebyrank(u_key, pool_limit, -1)
                            pool_truncated = True
                        
                        r.expire(u_key, 60)
                        temp_keys.append(u_key)
                        return u_key
                    return None

                t_cat_start = time.perf_counter()

                # 1.1 Apply explicit filters
                for f_p, val in [
                    ("language_id", lang_filter),
                    ("name", name_filter),
                    ("tags", tag_filter),
                ]:
                    if val:
                        sk = resolve_to_temp_zset([(f_p, val)])
                        if sk:
                            intersection_configs.append((sk, 0))
                        else:
                            return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})

                # 1.2 MD5 Filters
                if md5_filters:
                    md5_pairs = [("md5_1", m) for m in md5_filters] + [("md5_2", m) for m in md5_filters]
                    mk = resolve_to_temp_zset(md5_pairs)
                    if mk:
                        intersection_configs.append((mk, 0))
                    else:
                        return jsonify({"total": 0, "pairs": [], "algo": algo, "truncated": False})

                # 1.3 Global Search Query
                if search_q:
                    words = [w for w in search_q.split() if w.strip()]
                    for word in words:
                        wk = resolve_to_temp_zset([("name", word), ("tags", word), ("id", word), ("language_id", word)])
                        if wk:
                            intersection_configs.append((wk, 0))
                        else:
                            return jsonify({"total": 0, "pairs": [], "algo": algo, "truncated": False})

                # 1.4 Cross Binary
                if is_cross_binary:
                    cb_key = f"idx:{col}:sim:is_cross_binary:true"
                    if r.exists(cb_key):
                        intersection_configs.append((cb_key, 0))
                    else:
                        return jsonify({"total": 0, "pairs": [], "algo": algo, "truncated": False})

                metrics["filter_resolve"] = time.perf_counter() - t_cat_start
                
                # 2. Add Sort/Range ZSETs
                sim_weight = 1 if sort_by == "score" else 0
                feat_weight = 1 if sort_by == "feat_count" else 0
                
                # LAZY CANDIDATE RESOLUTION: If we have filters (names, tags), use them as the base.
                # Only use the pool_limit (algo_cap_key) if we don't have any other filters!
                algo_cap_key = None
                if not [c for c in intersection_configs if c[0] != f"idx:{col}:sim:is_cross_binary:true"]:
                    t_prep_start = time.perf_counter()
                    algo_cap_key = f"tmp:{col}:filter:{session_id}:algo_cap"
                    
                    # ALWAYS take the top-scoring similarity matches, regardless of final sort_order
                    if pool_limit <= 100000:
                        top_n = r.zrevrange(algo_zset, 0, pool_limit - 1, withscores=True)
                        
                        if not top_n:
                            return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                        
                        r.zadd(algo_cap_key, {p[0]: p[1] for p in top_n})
                        if r.zcard(algo_zset) > pool_limit:
                            pool_truncated = True
                    else:
                        # For huge pools (>100k), server-side ZINTERSTORE avoids massive data round-trips
                        r.zinterstore(algo_cap_key, {algo_zset: 1})
                        # ALWAYS keep the highest scores (rank 0 is lowest, so remove from 0 up to card - limit - 1)
                        # r.zremrangebyrank(algo_cap_key, 0, -(pool_limit + 1)) correctly keeps TOP pool_limit
                        r.zremrangebyrank(algo_cap_key, 0, -(pool_limit + 1))
                        
                        # Already knows it's truncated if we're here and zcard > limit
                        if r.zcard(algo_zset) > pool_limit:
                            pool_truncated = True

                    r.expire(algo_cap_key, 60)
                    temp_keys.append(algo_cap_key)
                    intersection_configs.append((algo_cap_key, sim_weight))
                    metrics["prep_time"] = time.perf_counter() - t_prep_start
                else:
                    # We have restrictive categorical filters! Join them with the FULL similarity index.
                    intersection_configs.append((algo_zset, sim_weight))
                    metrics["prep_time"] = 0

                if sort_by == "feat_count" or has_min_features:
                    f_idx = f"idx:{col}:sim:feat_count1" if sort_by == "feat_count" else f"idx:{col}:sim:min_features"
                    if r.exists(f_idx):
                        intersection_configs.append((f_idx, feat_weight))

                # 3. Perform Intersection
                inter_key = f"tmp:{col}:filter:{session_id}:inter"
                t_inter_start = time.perf_counter()
                
                # OPTIMIZATION: If we only have the similarity index (no filters), avoid the ZINTERSTORE entirely.
                if len(intersection_configs) == 1 and intersection_configs[0][0] == algo_cap_key:
                    inter_key = algo_cap_key
                    metrics["inter_time"] = 0
                else:
                    if len(intersection_configs) > 1:
                        pipe = r.pipeline()
                        for k, _ in intersection_configs:
                            pipe.zcard(k)
                        sizes = pipe.execute()
                        sorted_plan = sorted(zip(intersection_configs, sizes), key=lambda x: x[1])
                        intersection_configs = [item[0] for item in sorted_plan]
                        
                        # New Debug Log: Show the optimized intersection order and sizes
                        plan_str = ", ".join([f"{c[0].split(':')[-1]} ({sz})" for c, sz in sorted_plan])
                        logging.info(f"SIM SEARCH | {session_id} | Intersection Plan (Sorted): {plan_str}")

                    configs = {k: w for k, w in intersection_configs}
                    r.zinterstore(inter_key, configs)
                    metrics["inter_time"] = time.perf_counter() - t_inter_start
                
                # Result is already capped by prep and filters, final check if needed
                if inter_key != algo_cap_key and r.zcard(inter_key) > pool_limit:
                    if sort_order == "desc":
                        r.zremrangebyrank(inter_key, 0, -(pool_limit + 1))
                    else:
                        r.zremrangebyrank(inter_key, pool_limit, -1)
                    pool_truncated = True
                
                r.expire(inter_key, 60)
                temp_keys.append(inter_key)

                # 4. Apply Ranges (min_score / max_score)
                if sort_by == "score":
                    # Scores in inter_key are similarity scores, can filter directly
                    r.zremrangebyscore(inter_key, "-inf", f"({min_score}")
                    r.zremrangebyscore(inter_key, f"({max_score}", "inf")
                else:
                    # SCORE-RANGE MASK: Scores in inter_key are not similarity scores (e.g., feat_count)
                    if min_score > 0 or max_score < 1.0:
                        s_mask = f"tmp:{col}:filter:{session_id}:score_mask"
                        # Fast intersection with the full algo_zset because inter_key is small
                        r.zinterstore(s_mask, {inter_key: 0, algo_zset: 1})
                        r.zremrangebyscore(s_mask, "-inf", f"({min_score}")
                        r.zremrangebyscore(s_mask, f"({max_score}", "inf")
                        r.expire(s_mask, 60)
                        temp_keys.append(s_mask)
                        # Re-intersect to apply the similarity-range mask
                        r.zinterstore(inter_key, {inter_key: 1, s_mask: 0})

                if has_min_features:
                    # OPTIMIZED RANGE FILTER: Avoid 8M-intersection if result set is small
                    t_mask_start = time.perf_counter()
                    inter_size = r.zcard(inter_key)
                    lean_mask = False
                    
                    if inter_size < 10000:
                        # LEAN MASKING: Fetch feature counts only for candidates
                        lean_mask = True
                        ids_to_check = r.zrange(inter_key, 0, -1)
                        if ids_to_check:
                            # Use pipeline to fetch feature scores efficiently
                            pipe = r.pipeline()
                            for sid in ids_to_check:
                                pipe.zscore(f"idx:{col}:sim:min_features", sid)
                            f_scores = pipe.execute()
                            
                            to_remove = []
                            for i, fs in enumerate(f_scores):
                                if fs is None or float(fs) < min_features:
                                    to_remove.append(ids_to_check[i])
                            
                            if to_remove:
                                r.zrem(inter_key, *to_remove)
                    else:
                        # STANDARD MASKING: Full database intersection
                        mf_mask = f"tmp:{col}:filter:{session_id}:mf_mask"
                        r.zinterstore(mf_mask, {f"idx:{col}:sim:min_features": 1, inter_key: 0})
                        r.zremrangebyscore(mf_mask, "-inf", f"({min_features}")
                        r.expire(mf_mask, 60)
                        temp_keys.append(mf_mask)
                        r.zinterstore(inter_key, {inter_key: 1, mf_mask: 0})
                        
                    metrics["mask_time"] = time.perf_counter() - t_mask_start
                    if lean_mask:
                        metrics["mask_time"] = -(metrics["mask_time"]) # Mark Negative for Lean path in logs

                # 5. Handle Final Sorting (Sorting is now handled by ZINTERSTORE weights)
                final_zset = inter_key

                # 6. Fetch Total
                total = r.zcard(final_zset)
                
                # Top-K Caching
                cache_count = min(total, MAX_CACHED_RESULTS)
                if sort_order == "desc":
                    top_k = r.zrevrange(final_zset, 0, cache_count - 1, withscores=True)
                else:
                    top_k = r.zrange(final_zset, 0, cache_count - 1, withscores=True)
                
                if offset + limit <= len(top_k):
                    page_results = top_k[offset : offset + limit]
                else:
                    if sort_order == "desc":
                        page_results = r.zrevrange(final_zset, offset, offset + limit - 1, withscores=True)
                    else:
                        page_results = r.zrange(final_zset, offset, offset + limit - 1, withscores=True)

                elapsed_time = time.perf_counter() - start_time
                if elapsed_time >= CACHE_TIME_THRESHOLD and top_k:
                    # Async Top-K caching to avoid blocking the response
                    def save_cache_task(key, data_map):
                        try:
                            t_cw_start = time.perf_counter()
                            r.setex(key, 300, json.dumps(data_map))
                            cw_time = time.perf_counter() - t_cw_start
                            logging.info(f"Top-K Cache saved ASYNC (write={cw_time:.3f}s, {len(data_map['ids'])} items): {key}")
                        except Exception as exc:
                            logging.error(f"Async cache save failed: {exc}")

                    cache_payload = {
                        "total": total,
                        "ids": [p[0] for p in top_k],
                        "scores": [p[1] for p in top_k],
                        "pool_truncated": pool_truncated,
                        "collection": col,
                    }
                    import threading
                    threading.Thread(target=save_cache_task, args=(cache_key, cache_payload), daemon=True).start()
                
                # (No immediate breakdown logging here as we consolidate to the end)

            finally:
                if temp_keys:
                    r.delete(*temp_keys)

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
