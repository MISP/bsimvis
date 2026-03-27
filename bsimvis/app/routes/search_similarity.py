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

    md5_filters    = request.args.getlist('md5')
    is_cross_binary = request.args.get('cross_binary', 'false').lower() == 'true'

    if not col:
        return jsonify({"detail": "Missing collection"}), 400

    try:
        r = get_redis()

        # Resolve candidate sim IDs via score ZSET
        # We use the collection-wide ZSET keyed by algo
        algo_zset = f"{col}:all_sim:{algo}"
        total_zset = r.zcard(algo_zset)

        # Get IDs in the score range
        candidate_ids = r.zrangebyscore(algo_zset, threshold, max_score, start=0, num=5000)

        # Fetch full JSON docs via pipeline.
        # ZSET member format: "funcid1::funcid2"
        # sim_meta key format: {coll}:sim_meta:{algo}:{funcid1}:{funcid2}
        pipe = r.pipeline()
        for sim_member in candidate_ids:
            parts = sim_member.split("::", 1)
            if len(parts) == 2:
                meta_key = f"{col}:sim_meta:{algo}:{parts[0]}:{parts[1]}"
            else:
                meta_key = f"{col}:sim_meta:{algo}:{sim_member}"
            pipe.json().get(meta_key, "$")
        raw_results = pipe.execute()

        # Filter and collect
        enriched_pairs = []
        for sim_id, raw in zip(candidate_ids, raw_results):
            if not raw:
                continue
            data = raw[0] if isinstance(raw, list) and raw else raw
            if isinstance(data, str):
                data = json.loads(data)

            # Apply filters
            if is_cross_binary and not data.get("is_cross_binary"):
                continue
            if min_features > 0:
                if data.get("feat_count1", 0) < min_features or data.get("feat_count2", 0) < min_features:
                    continue
            if md5_filters:
                m1 = data.get("md5_1", "")
                m2 = data.get("md5_2", "")
                if not any(m in (m1, m2) for m in md5_filters):
                    continue

            enriched_pairs.append({
                "id1":   data.get("id1"),
                "id2":   data.get("id2"),
                "name1": data.get("name1", (data.get("id1") or ":").split(':')[-1]),
                "name2": data.get("name2", (data.get("id2") or ":").split(':')[-1]),
                "score": float(data.get("score", 0)),
                "meta1": {
                    "file_md5":           data.get("md5_1"),
                    "tags":               data.get("tags1", []),
                    "batch_uuid":         data.get("batch_uuid1"),
                    "language_id":        data.get("language_id1"),
                    "bsim_features_count": data.get("feat_count1")
                },
                "meta2": {
                    "file_md5":           data.get("md5_2"),
                    "tags":               data.get("tags2", []),
                    "batch_uuid":         data.get("batch_uuid2"),
                    "language_id":        data.get("language_id2"),
                    "bsim_features_count": data.get("feat_count2")
                }
            })

        total = len(enriched_pairs)
        page  = enriched_pairs[offset: offset + limit]

        return jsonify({
            "collection": col,
            "algo":       algo,
            "threshold":  threshold,
            "total":      total,
            "offset":     offset,
            "limit":      limit,
            "pairs":      page
        })

    except Exception as e:
        logging.error(f"Similarity search error: {e}")
        return jsonify({"detail": str(e)}), 500
