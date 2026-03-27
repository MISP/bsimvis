import json
import logging

from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import query_ids

search_similarity_bp = Blueprint("search_similarity", __name__)

DEFAULT_LIMIT = 100  # API RESULT LIMIT
DEFAULT_POOL_LIMIT = 1000000  # DATABASE FILTERING LIMIT
MAX_POOL_LIMIT = 1000000


@search_similarity_bp.route("/api/similarity/search", methods=["GET"])
def similarity_search():
    col = request.args.get("collection")
    algo = request.args.get("algo", "unweighted_cosine")

    try:
        threshold = float(request.args.get("threshold", 0.95))
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
        truncated = False
        algo_zset = f"{col}:all_sim:{algo}"
        final_list = []
        candidate_list = []
        total = 0

        has_set_filters = any(
            [name_filter, tag_filter, lang_filter, md5_filters, search_q]
        )
        has_min_features = min_features > 0

        if has_set_filters:
            # --- DATABASE-SIDE INTERSECTION STRATEGY ---
            filter_keys = []
            temp_keys = []
            import uuid

            session_id = str(uuid.uuid4())[:8]

            try:
                # 1. MD5 Filters (Union if multiple)
                if md5_filters:
                    m_key = f"tmp:{col}:filter:{session_id}:md5"
                    m_keys = [f"idx:{col}:sim:md5_1:{m}" for m in md5_filters] + [
                        f"idx:{col}:sim:md5_2:{m}" for m in md5_filters
                    ]
                    r.sunionstore(m_key, *m_keys)
                    r.expire(m_key, 60)
                    filter_keys.append(m_key)
                    temp_keys.append(m_key)

                # 2. Substring/Search Resolution Helper
                def resolve_db_substring(field_prefix, substring):
                    if not substring:
                        return None
                    patterns = []
                    # Try original case first (default to *substring* if no * provided)
                    target_sub = substring if "*" in substring else f"*{substring}*"
                    pat1 = f"idx:{col}:sim:{field_prefix}*:{target_sub}"
                    patterns.append(pat1)
                    # Try lowercase if different
                    if substring.lower() != substring:
                        sub_low = substring.lower()
                        target_low = sub_low if "*" in sub_low else f"*{sub_low}*"
                        pat_low = f"idx:{col}:sim:{field_prefix}*:{target_low}"
                        patterns.append(pat_low)

                    found_keys = []
                    for pat in patterns:
                        cursor = 0
                        while True:
                            cursor, sub_keys = r.scan(
                                cursor=cursor, match=pat, count=5000
                            )
                            if sub_keys:
                                found_keys.extend(sub_keys)
                            if cursor == 0:
                                break

                    if found_keys:
                        # Dedup keys if both original and lower case matched some same keys
                        found_keys = list(set(found_keys))
                        u_key = f"tmp:{col}:filter:{session_id}:sub:{field_prefix}"
                        # BATCH sunionstore to avoid argument limits or memory spikes
                        batch_size = 500
                        for i in range(0, len(found_keys), batch_size):
                            batch = found_keys[i : i + batch_size]
                            if i == 0:
                                r.sunionstore(u_key, *batch)
                            else:
                                r.sunionstore(u_key, u_key, *batch)
                        r.expire(u_key, 60)
                        return u_key
                    return None

                # 3. Apply Substring Filters (Name, Tags, Language)
                for f_p, val in [
                    ("language_id", lang_filter),
                    ("name", name_filter),
                    ("tags", tag_filter),
                ]:
                    if val:
                        sk = resolve_db_substring(f_p, val)
                        if sk:
                            filter_keys.append(sk)
                            temp_keys.append(sk)
                        else:
                            return jsonify(
                                {
                                    "total": 0,
                                    "pairs": [],
                                    "algo": algo,
                                    "truncated": False,
                                }
                            )

                # 4. Search Query (Global keyword search)
                if search_q:
                    q_key = f"tmp:{col}:filter:{session_id}:search"
                    sub_components = []
                    for f_p in ["name", "tags", "id", "language_id"]:
                        sk = resolve_db_substring(f_p, search_q)
                        if sk:
                            sub_components.append(sk)
                            temp_keys.append(sk)
                    if sub_components:
                        r.sunionstore(q_key, *sub_components)
                        r.expire(q_key, 60)
                        filter_keys.append(q_key)
                        temp_keys.append(q_key)
                    else:
                        return jsonify(
                            {"total": 0, "pairs": [], "algo": algo, "truncated": False}
                        )

                # 5. Final Intersection
                if filter_keys:
                    inter_key = f"tmp:{col}:filter:{session_id}:final"
                    r.sinterstore(inter_key, *filter_keys)
                    r.expire(inter_key, 60)
                    temp_keys.append(inter_key)

                    candidate_list = []
                    cursor = 0
                    while len(candidate_list) < pool_limit:
                        cursor, batch = r.sscan(inter_key, cursor=cursor, count=5000)
                        candidate_list.extend(batch)
                        if cursor == 0 or len(candidate_list) >= pool_limit:
                            break

                    if len(candidate_list) > pool_limit:
                        candidate_list = candidate_list[:pool_limit]
                        truncated = True

            finally:
                if temp_keys:
                    pipe = r.pipeline()
                    for k in temp_keys:
                        pipe.delete(k)
                    pipe.execute()

        elif has_min_features:
            # Optimized path for ONLY min_features filter: Use ZSET range with LIMIT
            ids1 = r.zrangebyscore(
                f"idx:{col}:sim:feat_count1",
                min_features,
                float("inf"),
                start=0,
                num=pool_limit,
            )
            ids2 = r.zrangebyscore(
                f"idx:{col}:sim:feat_count2",
                min_features,
                float("inf"),
                start=0,
                num=pool_limit,
            )
            candidate_list = list(set(ids1) | set(ids2))[:pool_limit]
            # Since we can't easily get the true total of the Union without fetching, we estimate
            # Or just check if we hit the limit
            if len(candidate_list) >= pool_limit:
                truncated = True

        # --- SCORING PHASE for Filtered Matches ---
        if (has_set_filters or has_min_features) and candidate_list:
            pipe = r.pipeline()
            feat1_zset = f"idx:{col}:sim:feat_count1"
            feat2_zset = f"idx:{col}:sim:feat_count2"
            for sid in candidate_list:
                pipe.zscore(algo_zset, sid)
                if sort_by == "feat_count" or has_min_features:
                    pipe.zscore(feat1_zset, sid)
                    pipe.zscore(feat2_zset, sid)

            results = pipe.execute()
            res_idx = 0
            for sid in candidate_list:
                sc = results[res_idx]
                res_idx += 1

                f1_count = 0
                f2_count = 0
                if sort_by == "feat_count" or has_min_features:
                    f1_count = float(results[res_idx] or 0)
                    res_idx += 1
                    f2_count = float(results[res_idx] or 0)
                    res_idx += 1

                if sc is not None:
                    s_val = float(sc)
                    if threshold <= s_val <= max_score:
                        if has_min_features and max(f1_count, f2_count) < min_features:
                            continue
                        if is_cross_binary:
                            parts = sid.split(":")
                            if len(parts) >= 11 and parts[5] == parts[9]:
                                continue
                        final_list.append((sid, s_val, f1_count))

        elif not has_set_filters and not has_min_features:
            # NO filters: fetch all scores in range (up to the pool limit)
            score_tuples = r.zrevrangebyscore(
                algo_zset,
                max_score,
                threshold,
                withscores=True,
                start=0,
                num=pool_limit,
            )

            if sort_by == "feat_count" or is_cross_binary:
                candidate_list = [t[0] for t in score_tuples]
                pipe = r.pipeline()
                feat_zset = f"idx:{col}:sim:feat_count1"
                for sid in candidate_list:
                    pipe.zscore(feat_zset, sid)
                f_counts = pipe.execute()

                for (sid, sc), fc in zip(score_tuples, f_counts):
                    if is_cross_binary:
                        parts = sid.split(":")
                        if len(parts) >= 11 and parts[5] == parts[9]:
                            continue
                    final_list.append((sid, float(sc), float(fc or 0)))
            else:
                final_list = [(sid, float(sc), 0) for sid, sc in score_tuples]

            if len(final_list) >= pool_limit:
                truncated = True

        total = len(final_list)

        # --- GLOBAL SORTING & PAGINATION ---
        reverse = sort_order == "desc"
        if sort_by == "feat_count":
            final_list.sort(key=lambda x: x[2], reverse=reverse)
        else:
            final_list.sort(key=lambda x: x[1], reverse=reverse)

        page_ids = final_list[offset : offset + limit]

        # --- METADATA FETCH ---
        enriched_pairs = []
        if page_ids:
            pipe = r.pipeline()
            for sid, score, feat_count in page_ids:
                pipe.json().get(sid, "$")
            page_raw = pipe.execute()

            for (sid, score, feat_count), raw in zip(page_ids, page_raw):
                if not raw:
                    continue
                data = raw[0] if isinstance(raw, list) and raw else raw
                if isinstance(data, str):
                    data = json.loads(data)

                tags1 = data.get("tags1", "").split(",") if data.get("tags1") else []
                tags2 = data.get("tags2", "").split(",") if data.get("tags2") else []

                enriched_pairs.append(
                    {
                        "id1": data.get("id1"),
                        "id2": data.get("id2"),
                        "name1": data.get(
                            "name1", (data.get("id1") or ":").split(":")[-1]
                        ),
                        "name2": data.get(
                            "name2", (data.get("id2") or ":").split(":")[-1]
                        ),
                        "score": score,
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

        if sort_by == "name":
            enriched_pairs.sort(
                key=lambda x: x["name1"].lower(), reverse=(sort_order == "desc")
            )

        return jsonify(
            {
                "collection": col,
                "algo": algo,
                "threshold": threshold,
                "total": total,
                "offset": offset,
                "limit": limit,
                "pool_limit": pool_limit,
                "truncated": truncated,
                "pairs": enriched_pairs,
                "sort_by": sort_by,
                "sort_order": sort_order,
            }
        )

    except Exception as e:
        import traceback

        logging.error(f"Similarity search error: {e}")
        traceback.print_exc()
        return jsonify({"detail": str(e)}), 500
