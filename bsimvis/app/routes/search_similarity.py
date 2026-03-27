import json
import logging

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids

search_similarity_bp = Blueprint('search_similarity', __name__)

DEFAULT_LIMIT = 100


@search_similarity_bp.route("/api/similarity/search", methods=["GET"])
def similarity_search():
    col  = request.args.get('collection')
    algo = request.args.get('algo', 'unweighted_cosine')

    try:
        threshold   = float(request.args.get('threshold', 0.1))
        max_score   = float(request.args.get('max_score', 1.0))
        offset      = int(request.args.get('offset', 0))
        limit       = int(request.args.get('limit', DEFAULT_LIMIT))
        min_features = int(request.args.get('min_features', 0))
    except ValueError:
        return jsonify({"detail": "Invalid numeric parameter"}), 400

    # Filtering parameters
    search_q = request.args.get('q', '').lower()
    name_filter = request.args.get('name', '').lower()
    tag_filter = request.args.get('tag', '').lower()
    lang_filter = request.args.get('language', '').lower()
    md5_filters = request.args.getlist('md5')
    is_cross_binary = request.args.get('cross_binary', 'false').lower() == 'true'

    # Sorting
    sort_by = request.args.get('sort_by', 'score') # score, name, feat_count
    sort_order = request.args.get('sort_order', 'desc').lower() # asc, desc

    if not col:
        return jsonify({"detail": "Missing collection"}), 400

    try:
        r = get_redis()

        # Resolve candidate sim IDs via score ZSET
        algo_zset = f"{col}:all_sim:{algo}"
        
        # Performance optimization: if no filters, use a small window
        has_filters = any([name_filter, tag_filter, lang_filter, md5_filters, search_q, is_cross_binary, min_features > 0])
        
        # Decide how many candidates to fetch
        if has_filters or sort_by != 'score':
            fetch_num = 30000 
            fetch_start = 0
        else:
            # Fast path: no filter + score sort = fetch only the page we need
            fetch_num = limit
            fetch_start = offset

        # Optimized candidate fetch
        if sort_by == 'score' and sort_order == 'desc':
            candidate_ids = r.zrevrangebyscore(algo_zset, max_score, threshold, start=fetch_start, num=fetch_num)
        elif sort_by == 'score' and sort_order == 'asc':
            candidate_ids = r.zrangebyscore(algo_zset, threshold, max_score, start=fetch_start, num=fetch_num)
        else:
            # Fallback for name/feat_count sort: take top scores first
            candidate_ids = r.zrevrangebyscore(algo_zset, max_score, threshold, start=fetch_start, num=fetch_num)

        # Fetch full JSON docs via pipeline
        pipe = r.pipeline()
        for sim_member in candidate_ids:
            parts = sim_member.split("::", 1)
            meta_key = f"{col}:sim_meta:{algo}:{parts[0]}:{parts[1]}" if len(parts) == 2 else f"{col}:sim_meta:{algo}:{sim_member}"
            pipe.json().get(meta_key, "$")
        raw_results = pipe.execute()

        # Filter and collect
        enriched_pairs = []
        for sim_id, raw in zip(candidate_ids, raw_results):
            if not raw: continue
            data = raw[0] if isinstance(raw, list) and raw else raw
            if isinstance(data, str): data = json.loads(data)

            # --- Filtering ---
            if is_cross_binary and not data.get("is_cross_binary"): continue
            if min_features > 0:
                if data.get("feat_count1", 0) < min_features or data.get("feat_count2", 0) < min_features: continue
            
            if md5_filters:
                if not any(m in (data.get("md5_1"), data.get("md5_2")) for m in md5_filters): continue

            # Text filters
            n1, n2 = data.get("name1", "").lower(), data.get("name2", "").lower()
            if name_filter and name_filter not in n1 and name_filter not in n2: continue
            
            t1, t2 = data.get("tags1", "").lower(), data.get("tags2", "").lower()
            if tag_filter and tag_filter not in t1 and tag_filter not in t2: continue
            
            l1, l2 = data.get("language_id1", "").lower(), data.get("language_id2", "").lower()
            if lang_filter and lang_filter not in l1 and lang_filter not in l2: continue

            if search_q and not any(search_q in str(data.get(k, "")).lower() for k in ["name1", "name2", "id1", "id2", "tags1", "tags2"]):
                continue

            # Standardize tags
            tags1 = data.get("tags1", "").split(",") if data.get("tags1") else []
            tags2 = data.get("tags2", "").split(",") if data.get("tags2") else []

            enriched_pairs.append({
                "id1":   data.get("id1"),
                "id2":   data.get("id2"),
                "name1": data.get("name1", (data.get("id1") or ":").split(':')[-1]),
                "name2": data.get("name2", (data.get("id2") or ":").split(':')[-1]),
                "score": float(data.get("score", 0)),
                "meta1": {
                    "file_md5":           data.get("md5_1"),
                    "tags":               tags1,
                    "batch_uuid":         data.get("batch_uuid1"),
                    "language_id":        data.get("language_id1"),
                    "return_type":        data.get("return_type1", "N/A"),
                    "bsim_features_count": data.get("feat_count1")
                },
                "meta2": {
                    "file_md5":           data.get("md5_2"),
                    "tags":               tags2,
                    "batch_uuid":         data.get("batch_uuid2"),
                    "language_id":        data.get("language_id2"),
                    "return_type":        data.get("return_type2", "N/A"),
                    "bsim_features_count": data.get("feat_count2")
                }
            })

        # --- Sorting (Manual for non-score fields) ---
        if sort_by != "score" or (sort_by == "score" and sort_order != "desc" and sort_order != "asc"):
            reverse = (sort_order == 'desc')
            if sort_by == 'name':
                enriched_pairs.sort(key=lambda x: x['name1'].lower(), reverse=reverse)
            elif sort_by == 'feat_count':
                enriched_pairs.sort(key=lambda x: x['meta1']['bsim_features_count'], reverse=reverse)
            elif sort_by == 'score':
                enriched_pairs.sort(key=lambda x: x['score'], reverse=reverse)

        if has_filters or sort_by != 'score':
            total = len(enriched_pairs)
            page  = enriched_pairs[offset: offset + limit]
        else:
            # Fast path: the size of current page is what we fetched,
            # but 'total' is the ZSET size in score range (estimated).
            total = r.zcount(algo_zset, threshold, max_score)
            page  = enriched_pairs

        return jsonify({
            "collection": col,
            "algo":       algo,
            "threshold":  threshold,
            "total":      total,
            "offset":     offset,
            "limit":      limit,
            "pairs":      page,
            "sort_by":    sort_by,
            "sort_order": sort_order
        })

    except Exception as e:
        logging.error(f"Similarity search error: {e}")
        return jsonify({"detail": str(e)}), 500
