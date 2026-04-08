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
            "cross_binary": cross_binary_val,
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
                # --- LUA SCRIPT SETUP (Hardened for Multi-worker Sync) ---
                from bsimvis.app.services.lua_manager import lua_manager
                
                # In development/debugging, we bypass the singleton to ensure all workers are synced
                lua_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lua", "search_similarity.lua")
                with open(lua_path, 'r') as f:
                    script_content = f.read()
                
                # Support dynamic reloading trigger locally
                force_reload = request.args.get("force_reload", "").lower() == "true"
                if force_reload:
                    logging.warning(f"[*] FORCED LUA SCRIPT RELOAD for session {session_id}")
                    # We also update the manager for other scripts
                    lua_manager.register_all()

                # Directly use EVAL to ensure the current worker is running our fresh script_content
                def search_script(keys=(), args=()):
                    return r.eval(script_content, len(keys), *(list(keys) + list(args)))

                t_lua_prep = time.perf_counter()
                bucket_keys = []
                groups = []

                def get_bucket_idx(prefix, val):
                    # Deep Selection: metadata filters resolve directly to FUNCTION-level indices
                    # Special Case: MD5 uses the file_funcs specialized index
                    if prefix == "md5":
                        b_key = f"idx:{col}:file_funcs:{val}"
                        if r.exists(b_key):
                            if b_key not in bucket_keys: bucket_keys.append(b_key)
                            return [bucket_keys.index(b_key) + 1]
                        return []

                    # Prefix mapping: user-facing filter to index_service FUNC_TAG_FIELDS
                    mapping = {"name": "function_name", "tags": "tags", "batch_uuid": "batch_uuid",
                               "language_id": "language_id", "id": "id"}
                    field = mapping.get(prefix, prefix)
                    
                    # Deep Selection: metadata filters resolve via the safe Metadata Registry
                    # We check both function-level and sim-level tags if applicable
                    registries = [f"idx:{col}:reg:function:{field}"]
                    if field == "tags":
                         registries.append(f"idx:{col}:reg:sim:{field}")

                    all_matching_buckets = []
                    
                    # Robust Union Strategy: If no wildcard is present, check direct existence first
                    # (This supports legacy data indexed before the Registry was added)
                    if "*" not in val:
                        prefixes = [f"idx:{col}:function:{field}", f"idx:{col}:sim:{field}"] if field == "tags" else [f"idx:{col}:function:{field}"]
                        for p in prefixes:
                            direct_key = f"{p}:{val.lower()}"
                            if r.exists(direct_key):
                                all_matching_buckets.append(direct_key)
                    
                    # Wildcard support / Registry search
                    if not all_matching_buckets or "*" in val:
                        for registry_key in registries:
                            if r.exists(registry_key):
                                target_lower = val.lower().replace("*", "")
                                all_buckets = [b.decode() if isinstance(b, bytes) else str(b) 
                                               for b in r.smembers(registry_key)]
                                for b_key_str in all_buckets:
                                    if target_lower in b_key_str.lower().split(":")[-1]:
                                        if b_key_str not in all_matching_buckets:
                                            all_matching_buckets.append(b_key_str)
                    
                    found_for_filter = []
                    for b_key_str in all_matching_buckets:
                        if b_key_str not in bucket_keys:
                            bucket_keys.append(b_key_str)
                        b_idx = bucket_keys.index(b_key_str) + 1
                        if b_idx not in found_for_filter:
                            found_for_filter.append(b_idx)
                        if len(found_for_filter) >= 1000: break
                    
                    if found_for_filter:
                        logging.info(f"SIM SEARCH | {session_id} | Resolved {len(found_for_filter)} function-indices for '{field}:{val}'")
                    return found_for_filter

                # Helper to add a group and calculate its weight
                groups_raw = []
                def add_group(bucket_indices, field=None):
                    weight = 0
                    names = []
                    for b_idx in bucket_indices:
                        b_key = bucket_keys[b_idx-1]
                        try:
                            weight += r.zcard(b_key)
                        except redis.exceptions.ResponseError:
                            weight += r.scard(b_key)
                        names.append(b_key.split(":")[-1])
                    
                    groups_raw.append({
                        "type": "metadata",
                        "field": field,
                        "buckets": bucket_indices,
                        "bucket_names": names,
                        "weight": weight,
                        "is_large": True # Deep Selection: Always use the robust verification path
                    })

                # Group resolution (Simplified for Deep Selection)
                if lang_filter:
                    g = get_bucket_idx("language_id", lang_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    add_group(g, field="language")

                if name_filter:
                    g = get_bucket_idx("name", name_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    add_group(g, field="name")

                if tag_filter:
                    g = get_bucket_idx("tags", tag_filter)
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    add_group(g, field="tag")

                if md5_filters:
                    g = []
                    for m in md5_filters:
                        g.extend(get_bucket_idx("md5", m))
                    if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                    add_group(g, field="md5")

                if search_q:
                    for word in [w for w in search_q.split() if w.strip()]:
                        g = []
                        for p in ["name", "tags", "id", "language_id"]:
                            g.extend(get_bucket_idx(p, word))
                        if not g: return jsonify({"total": 0, "pairs": [], "algo": algo, "pool_truncated": False})
                        add_group(g, field=f"q({word})")

                # Cross-binary filtering also uses function-level MD5s now?
                # Actually, cross_binary is a PROPERTY of the similarity, so we keep using the sim-index.
                if cross_binary_val is not None:
                    cb_bool = cross_binary_val.lower() == "true"
                    cb_key = f"idx:{col}:sim:is_cross_binary:{'true' if cb_bool else 'false'}"
                    if r.exists(cb_key):
                        if cb_key not in bucket_keys: bucket_keys.append(cb_key)
                        add_group([bucket_keys.index(cb_key) + 1], field="cross_binary")

                # Similarity Score Group
                if min_score <= 0.0 and max_score >= 1.0:
                    sim_weight = r.zcard(algo_zset)
                else:
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
                if min_features > 0 or sort_by == "feat_count":
                    feat_key = f"idx:{col}:sim:min_features"
                    if min_features <= 0:
                        feat_weight = r.zcard(feat_key)
                    else:
                        feat_weight = r.zcount(feat_key, min_features, "+inf")
                    
                    groups_raw.append({
                        "type": "feature_range",
                        "field": "feat_count",
                        "weight": feat_weight,
                        "min": min_features,
                        "key": feat_key
                    })

                # Sort all groups by weight (Selective Order)
                groups = sorted(groups_raw, key=lambda x: x["weight"])

                lua_config = {
                    "groups": groups,
                    "pool_limit": pool_limit,
                    "offset": offset,
                    "limit": limit,
                    "sort_by": sort_by,
                    "sort_order": sort_order,
                    "collection": col,
                    "algo": algo
                }

                # Exec Lua Search (Deep Selection Architecture)
                t_lua_start = time.perf_counter()
                keys = [algo_zset, f"idx:{col}:sim:min_features"] + bucket_keys
                try:
                    res = search_script(keys=keys, args=[json.dumps(lua_config)])
                    total = res[0]
                    pool_truncated = bool(res[1])
                    all_ids = res[2]
                    all_scores = res[3]
                    
                    if use_cache and (all_ids or total == 0):
                        cache_data = {"total": total, "pool_truncated": pool_truncated, "ids": all_ids, "scores": all_scores}
                        r.setex(cache_key, 3600, json.dumps(cache_data))
                    
                    page_results = list(zip(all_ids[offset : offset + limit], all_scores[offset : offset + limit]))
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
                    pipe.zscore(f"idx:{col}:sim:feat_count1", sid)
                else:
                    pipe.zscore(algo_zset, sid)
            
            enrichment_raw = pipe.execute()

            # Helper to extract IDs from SID (same logic as Lua)
            def extract_from_sid(sid):
                prefix = f"{col}:sim_meta:{algo}:"
                rest = sid[len(prefix):]
                f_prefix = f"{col}:function:"
                start_id2 = rest.find(f":{f_prefix}", len(f_prefix))
                if start_id2 == -1: return None, None
                return rest[:start_id2], rest[start_id2+1:]

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
                    f_id_map[sid] = (id1, id2, data, enrichment_raw[i*2 + 1])
                    meta_pipe.json().get(f"{id1}:meta", "$")
                    meta_pipe.json().get(f"{id2}:meta", "$")
            
            meta_results = meta_pipe.execute()
            
            # Map meta results back
            for i, sid in enumerate(f_id_map.keys()):
                id1, id2, sim_data, other_metric = f_id_map[sid]
                m1_json = meta_results[i*2]
                m2_json = meta_results[i*2 + 1]
                
                m1 = (m1_json[0] if isinstance(m1_json, list) else m1_json) or {}
                m2 = (m2_json[0] if isinstance(m2_json, list) else m2_json) or {}
                if isinstance(m1, str): m1 = json.loads(m1)
                if isinstance(m2, str): m2 = json.loads(m2)

                sim_score = float(sort_sc) if sort_by == "score" else float(other_metric or 0)
                feat_count = float(sort_sc) if sort_by == "feat_count" else float(other_metric or 0)

                enriched_pairs.append({
                    "id1": id1,
                    "id2": id2,
                    "name1": m1.get("function_name", id1.split(":")[-1]),
                    "name2": m2.get("function_name", id2.split(":")[-1]),
                    "score": sim_score,
                    "feat_count": int(feat_count),
                    "sid": sid,
                    "entry_date": parse_timestamp(sim_data.get("entry_date")),
                    "meta1": {
                        "file_md5": m1.get("file_md5"),
                        "file_name": m1.get("file_name"),
                        "tags": m1.get("tags", []),
                        "batch_uuid": m1.get("batch_uuid"),
                        "language_id": m1.get("language_id"),
                        "return_type": m1.get("return_type", "N/A"),
                        "bsim_features_count": m1.get("bsim_features_count"),
                    },
                    "meta2": {
                        "file_md5": m2.get("file_md5"),
                        "file_name": m2.get("file_name"),
                        "tags": m2.get("tags", []),
                        "batch_uuid": m2.get("batch_uuid"),
                        "language_id": m2.get("language_id"),
                        "return_type": m2.get("return_type", "N/A"),
                        "bsim_features_count": m2.get("bsim_features_count"),
                    },
                    "tags": sim_data.get("tags", []),
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
