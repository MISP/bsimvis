import json
import logging
import time

from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import normalize_tags, enrich_pool_data

DEFAULT_LIMIT = 50


def search_bin_sims():
    """
    Search binary similarity pairs with in-memory filtering and sorting.
    Uses involves:{md5} if md5 filter is provided, else falls back to built:{algo}.
    """
    try:
        t_start = time.perf_counter()
        r = get_redis()

        pool_id = request.args.get("pool")
        collection = request.args.get("collection")
        if not collection and not pool_id:
            return {"error": "No collection or pool specified"}, 400

        algo = request.args.get("algo", "unweighted_cosine")

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", DEFAULT_LIMIT))
        except ValueError:
            return {"error": "offset and limit must be integers"}, 400

        def parse_float(v):
            try:
                return float(v) if v is not None and v.strip() != "" else None
            except (ValueError, AttributeError):
                return None

        min_score = parse_float(request.args.get("min_score"))
        max_score = parse_float(request.args.get("max_score"))
        min_cov = parse_float(request.args.get("min_coverage"))
        max_cov = parse_float(request.args.get("max_coverage"))
        min_shared = parse_float(request.args.get("min_shared"))
        max_shared = parse_float(request.args.get("max_shared"))

        arch_filter = request.args.get("arch", "").strip().lower()
        min_funcs = parse_float(request.args.get("min_funcs"))
        max_funcs = parse_float(request.args.get("max_funcs"))

        sort_by = (
            request.args.get("sort_by") or request.args.get("sort") or "score"
        ).strip()
        sort_order = request.args.get("sort_order", "desc")

        # The UI now sends unified filters instead of A/B specific ones.
        md5_filter = request.args.get("md5", "").strip().lower()
        file_name_filter = request.args.get("file_name", "").strip().lower()
        file_tag_filters = [
            t.strip().lower() for t in request.args.getlist("file_tag") if t.strip()
        ]
        exclude_file_tag_filters = [
            t.strip().lower()
            for t in request.args.getlist("exclude_file_tag")
            if t.strip()
        ]
        exclude_file_static_tag_filters = [
            t.strip().lower()
            for t in request.args.getlist("exclude_file_static_tag")
            if t.strip()
        ]
        exclude_file_user_tag_filters = [
            t.strip().lower()
            for t in request.args.getlist("exclude_file_user_tag")
            if t.strip()
        ]

        # Similarity-level tag filters
        sim_tag_filters = [
            t.strip().lower() for t in request.args.getlist("tag") if t.strip()
        ]
        exclude_sim_tag_filters = [
            t.strip().lower() for t in request.args.getlist("exclude_tag") if t.strip()
        ]
        exclude_sim_static_tag_filters = [
            t.strip().lower()
            for t in request.args.getlist("exclude_static_tag")
            if t.strip()
        ]
        exclude_sim_user_tag_filters = [
            t.strip().lower()
            for t in request.args.getlist("exclude_user_tag")
            if t.strip()
        ]

        # 1. Fetch Candidate SIDs
        is_pool = pool_id is not None

        t0 = time.perf_counter()
        if md5_filter:
            if is_pool:
                cursor = 0
                matching_keys = []
                while True:
                    cursor, found_keys = r.scan(
                        cursor=cursor,
                        match=f"global:pool:{pool_id}:bin_sim:involves:*:{md5_filter}",
                        count=1000,
                    )
                    matching_keys.extend(found_keys)
                    if cursor == 0:
                        break
                sids = set()
                if matching_keys:
                    pipe = r.pipeline()
                    for k in matching_keys:
                        pipe.smembers(k)
                    res = pipe.execute()
                    for s_set in res:
                        sids.update(s_set)
            else:
                sids = r.smembers(f"{collection}:bin_sim:involves:{md5_filter}")
        else:
            if is_pool:
                sids = r.smembers(f"global:pool:{pool_id}:bin_sim:built:{algo}")
            else:
                sids = r.smembers(f"{collection}:bin_sim:built:{algo}")
        t1 = time.perf_counter()

        if not sids:
            return {"total": 0, "offset": offset, "limit": limit, "results": []}

        candidates = [s.decode() if isinstance(s, bytes) else s for s in sids]
        candidates = [s for s in candidates if f":bin_sim:{algo}:" in s]

        if not candidates:
            return {"total": 0, "offset": offset, "limit": limit, "results": []}

        # 2. Extract MD5s and Fetch File Meta
        t2 = time.perf_counter()
        light_docs = []
        unique_md5s = set()

        # Pipeline fetch JSON for ALL candidates to run in-memory filters
        pipe = r.pipeline()
        for sid in candidates:
            pipe.json().get(sid, "$")
        raw_json_docs = pipe.execute()

        for sid, res in zip(candidates, raw_json_docs):
            if not res:
                continue
            doc = res[0] if isinstance(res, list) else res
            if isinstance(doc, str):
                try:
                    doc = json.loads(doc)
                except Exception:
                    continue

            m_a = doc.get("md5_a") or doc.get("md5_1", "")
            m_b = doc.get("md5_b") or doc.get("md5_2", "")
            coll_a = doc.get("coll_1") or (collection[5:] if is_pool else collection)
            coll_b = doc.get("coll_2") or (collection[5:] if is_pool else collection)

            unique_md5s.add((coll_a, m_a))
            unique_md5s.add((coll_b, m_b))

            ld = {
                "sid": sid,
                "m_a": m_a,
                "m_b": m_b,
                "coll_a": coll_a,
                "coll_b": coll_b,
                "score": doc.get("score", 0.0),
                "score_sim_weighted": doc.get("sim_weighted_score")
                or doc.get("score", 0.0),
                "score_collection_weighted": doc.get("collection_weighted_score")
                or doc.get("score", 0.0),
                "coverage_a": doc.get("coverage_a")
                or (
                    doc.get("matched_clusters_count", 0)
                    / max(1, doc.get("matched_clusters_count", 1))
                ),
                "coverage_b": doc.get("coverage_b")
                or (
                    doc.get("matched_clusters_count", 0)
                    / max(1, doc.get("matched_clusters_count", 1))
                ),
                "shared_clusters": doc.get("shared_clusters")
                or doc.get("matched_clusters_count", 0),
                "doc": doc,
            }
            light_docs.append(ld)

        file_meta_cache = {}
        file_funcs_count = {}
        if unique_md5s:
            md5_list = list(unique_md5s)
            pipe = r.pipeline()
            for coll, md5 in md5_list:
                pipe.json().get(f"{coll}:file:{md5}:meta", "$")
                pipe.scard(f"{coll}:idx:file:functions:{md5}")
            results = pipe.execute()
            for i, (coll, md5) in enumerate(md5_list):
                res = results[2 * i]
                func_count = results[2 * i + 1]
                if res:
                    m = res[0] if isinstance(res, list) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    file_meta_cache[(coll, md5)] = m if isinstance(m, dict) else {}
                else:
                    file_meta_cache[(coll, md5)] = {}
                file_funcs_count[(coll, md5)] = func_count or 0
        t3 = time.perf_counter()

        if not is_pool:
            numeric_fields_to_fetch = set()
            sort_field_map = {
                "score": "score",
                "score_sim_weighted": "score_sim_weighted",
                "score_collection_weighted": "score_collection_weighted",
                "coverage": "coverage_a",
                "shared_clusters": "shared_clusters",
                "architecture": None,
                "functions_count": None,
                "computed_at": "computed_at",
            }
            sort_zset_field = sort_field_map.get(sort_by, "score")
            if sort_zset_field:
                numeric_fields_to_fetch.add(sort_zset_field)

            score_filter_field = "score_collection_weighted"
            if sort_by in ["score", "score_sim_weighted", "score_collection_weighted"]:
                score_filter_field = sort_by

            if min_score is not None or max_score is not None:
                numeric_fields_to_fetch.add(score_filter_field)
            if min_cov is not None or max_cov is not None:
                numeric_fields_to_fetch.add("coverage_a")
                numeric_fields_to_fetch.add("coverage_b")
            if min_shared is not None or max_shared is not None:
                numeric_fields_to_fetch.add("shared_clusters")

            if numeric_fields_to_fetch:
                pipe = r.pipeline()
                fields_list = list(numeric_fields_to_fetch)
                for ld in light_docs:
                    sid = ld["sid"]
                    for field in fields_list:
                        pipe.zscore(f"{collection}:idx:bin_sim:{field}", sid)

                zscore_res = pipe.execute()

                idx = 0
                for ld in light_docs:
                    for field in fields_list:
                        val = zscore_res[idx]
                        ld[field] = float(val) if val is not None else 0.0
                        idx += 1
        else:
            sort_field_map = {
                "score": "score",
                "score_sim_weighted": "score_sim_weighted",
                "score_collection_weighted": "score_collection_weighted",
                "coverage": "coverage_a",
                "shared_clusters": "shared_clusters",
            }
            sort_zset_field = sort_field_map.get(sort_by, "score")
            score_filter_field = (
                sort_by
                if sort_by
                in ["score", "score_sim_weighted", "score_collection_weighted"]
                else "score_collection_weighted"
            )

        t4 = time.perf_counter()

        # 4. Filter
        filtered_docs = []
        q_lower = request.args.get("q", "").strip().lower()

        for ld in light_docs:
            m_a = ld["m_a"]
            m_b = ld["m_b"]
            coll_a = ld["coll_a"]
            coll_b = ld["coll_b"]
            meta_a = file_meta_cache.get((coll_a, m_a), {})
            meta_b = file_meta_cache.get((coll_b, m_b), {})

            if pool_id:
                enrich_pool_data(meta_a, pool_id)
                enrich_pool_data(meta_b, pool_id)
                enrich_pool_data(ld, pool_id)

            ld["file_name_a"] = meta_a.get("file_name", m_a)
            ld["file_name_b"] = meta_b.get("file_name", m_b)
            ld["file_tags_a"] = meta_a.get("tags", [])
            ld["file_tags_b"] = meta_b.get("tags", [])
            ld["file_user_tags_a"] = meta_a.get("user_tags", [])
            ld["file_user_tags_b"] = meta_b.get("user_tags", [])
            ld["architecture_a"] = meta_a.get("language_id", "")
            ld["architecture_b"] = meta_b.get("language_id", "")
            ld["functions_count_a"] = file_funcs_count.get((coll_a, m_a), 0)
            ld["functions_count_b"] = file_funcs_count.get((coll_b, m_b), 0)

            # Filters
            if file_name_filter:
                if (
                    file_name_filter not in ld["file_name_a"].lower()
                    and file_name_filter not in ld["file_name_b"].lower()
                ):
                    continue

            if md5_filter:
                if md5_filter not in m_a.lower() and md5_filter not in m_b.lower():
                    continue

            if q_lower:
                if (
                    q_lower not in ld["file_name_a"].lower()
                    and q_lower not in ld["file_name_b"].lower()
                    and q_lower not in m_a.lower()
                    and q_lower not in m_b.lower()
                    and not any(q_lower in t.lower() for t in ld["file_tags_a"])
                    and not any(q_lower in t.lower() for t in ld["file_tags_b"])
                    and not any(q_lower in t.lower() for t in ld["file_user_tags_a"])
                    and not any(q_lower in t.lower() for t in ld["file_user_tags_b"])
                ):
                    continue

            if min_score is not None and ld.get(score_filter_field, 0) < min_score:
                continue
            if max_score is not None and ld.get(score_filter_field, 0) > max_score:
                continue

            cov_a = ld.get("coverage_a", 0)
            cov_b = ld.get("coverage_b", 0)
            if min_cov is not None and max(cov_a, cov_b) < min_cov:
                continue
            if max_cov is not None and min(cov_a, cov_b) > max_cov:
                continue

            shared = ld.get("shared_clusters", 0)
            if min_shared is not None and shared < min_shared:
                continue
            if max_shared is not None and shared > max_shared:
                continue

            if arch_filter:
                if (
                    arch_filter not in ld["architecture_a"].lower()
                    and arch_filter not in ld["architecture_b"].lower()
                ):
                    continue

            funcs_a = ld["functions_count_a"]
            funcs_b = ld["functions_count_b"]
            if min_funcs is not None and max(funcs_a, funcs_b) < min_funcs:
                continue
            if max_funcs is not None and min(funcs_a, funcs_b) > max_funcs:
                continue

            if file_tag_filters:
                combined_tags = set(
                    t.lower()
                    for t in ld["file_tags_a"]
                    + ld["file_user_tags_a"]
                    + ld["file_tags_b"]
                    + ld["file_user_tags_b"]
                )
                if not all(tf in combined_tags for tf in file_tag_filters):
                    continue

            if exclude_file_tag_filters:
                combined_tags = set(
                    t.lower()
                    for t in ld["file_tags_a"]
                    + ld["file_user_tags_a"]
                    + ld["file_tags_b"]
                    + ld["file_user_tags_b"]
                )
                if any(tf in combined_tags for tf in exclude_file_tag_filters):
                    continue
            if exclude_file_static_tag_filters:
                combined_static_tags = set(
                    t.lower() for t in ld["file_tags_a"] + ld["file_tags_b"]
                )
                if any(
                    tf in combined_static_tags for tf in exclude_file_static_tag_filters
                ):
                    continue
            if exclude_file_user_tag_filters:
                combined_user_tags = set(
                    t.lower() for t in ld["file_user_tags_a"] + ld["file_user_tags_b"]
                )
                if any(
                    tf in combined_user_tags for tf in exclude_file_user_tag_filters
                ):
                    continue

            filtered_docs.append(ld)

        total = len(filtered_docs)
        t5 = time.perf_counter()

        # 5. Sort
        def sort_key(d):
            if sort_zset_field:
                return d.get(sort_zset_field, 0)
            elif sort_by == "architecture":
                return d.get("architecture_a", "")
            elif sort_by == "functions_count":
                return d.get("functions_count_a", 0)
            return 0

        filtered_docs.sort(key=sort_key, reverse=(sort_order != "asc"))
        t6 = time.perf_counter()

        # 6. Paginate SIDs
        paged_light = filtered_docs[offset : offset + limit]

        # 7. Format FULL response
        final_docs = []
        for ld in paged_light:
            sid = ld["sid"]
            doc = ld["doc"] if is_pool else None

            if not doc:
                res = r.json().get(sid, "$")
                if res:
                    doc = res[0] if isinstance(res, list) else res
                    if isinstance(doc, str):
                        doc = json.loads(doc)

            if not doc:
                continue

            doc["_id"] = sid
            doc.pop("diff", None)

            m_a = doc.get("md5_a") or doc.get("md5_1", "")
            m_b = doc.get("md5_b") or doc.get("md5_2", "")
            coll_a = ld["coll_a"]
            coll_b = ld["coll_b"]

            # Normalize field names so frontend always receives md5_a/md5_b/coll_a/coll_b
            doc["md5_a"] = m_a
            doc["md5_b"] = m_b
            doc["coll_a"] = coll_a
            doc["coll_b"] = coll_b

            meta_a = file_meta_cache.get((coll_a, m_a), {})
            meta_b = file_meta_cache.get((coll_b, m_b), {})

            if pool_id:
                enrich_pool_data(meta_a, pool_id)
                enrich_pool_data(meta_b, pool_id)
                enrich_pool_data(doc, pool_id)

            doc["file_name_a"] = meta_a.get("file_name", m_a)
            doc["file_name_b"] = meta_b.get("file_name", m_b)
            doc["file_tags_a"] = meta_a.get("tags", [])
            doc["file_tags_b"] = meta_b.get("tags", [])
            doc["file_user_tags_a"] = meta_a.get("user_tags", [])
            doc["file_user_tags_b"] = meta_b.get("user_tags", [])

            doc["architecture_a"] = doc.get("architecture_a") or meta_a.get(
                "language_id", ""
            )
            doc["architecture_b"] = doc.get("architecture_b") or meta_b.get(
                "language_id", ""
            )
            doc["functions_count_a"] = doc.get(
                "functions_count_a"
            ) or file_funcs_count.get((coll_a, m_a), 0)
            doc["functions_count_b"] = doc.get(
                "functions_count_b"
            ) or file_funcs_count.get((coll_b, m_b), 0)

            doc["compiler_a"] = meta_a.get("compiler") or meta_a.get("compiler_id", "")
            doc["compiler_b"] = meta_b.get("compiler") or meta_b.get("compiler_id", "")
            doc["entry_date_a"] = meta_a.get("entry_date", 0)
            doc["entry_date_b"] = meta_b.get("entry_date", 0)

            normalize_tags(doc)
            normalize_tags(
                doc,
                tag_fields=[
                    "file_tags_a",
                    "file_user_tags_a",
                    "file_tags_b",
                    "file_user_tags_b",
                ],
            )

            # Re-apply sim_tag_filters if provided
            if sim_tag_filters:
                combined_sim_tags = set(
                    t.lower() for t in doc.get("tags", []) + doc.get("user_tags", [])
                )
                if not all(tf in combined_sim_tags for tf in sim_tag_filters):
                    total -= 1
                    continue

            if exclude_sim_tag_filters:
                combined_sim_tags = set(
                    t.lower() for t in doc.get("tags", []) + doc.get("user_tags", [])
                )
                if any(tf in combined_sim_tags for tf in exclude_sim_tag_filters):
                    total -= 1
                    continue
            if exclude_sim_static_tag_filters:
                combined_sim_static_tags = set(t.lower() for t in doc.get("tags", []))
                if any(
                    tf in combined_sim_static_tags
                    for tf in exclude_sim_static_tag_filters
                ):
                    total -= 1
                    continue
            if exclude_sim_user_tag_filters:
                combined_sim_user_tags = set(
                    t.lower() for t in doc.get("user_tags", [])
                )
                if any(
                    tf in combined_sim_user_tags for tf in exclude_sim_user_tag_filters
                ):
                    total -= 1
                    continue

            final_docs.append(doc)

        t7 = time.perf_counter()
        logging.info(
            f"BIN_SIM SEARCH | Fetch SIDs:{t1-t0:.3f}s | Meta:{t3-t2:.3f}s | ZSCORE:{t4-t3:.3f}s | Filter:{t5-t4:.3f}s | Sort:{t6-t5:.3f}s | FinalPage:{t7-t6:.3f}s | TOTAL:{t7-t_start:.3f}s | count={len(candidates)}"
        )

        return {"total": total, "offset": offset, "limit": limit, "results": final_docs}
    except Exception as e:
        logging.error(f"Error in search_bin_sims: {e}", exc_info=True)
        return {"error": str(e)}, 500
