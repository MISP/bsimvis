import json
import logging
import time

from flask import request
from bsimvis.app.services.redis_client import get_redis

DEFAULT_LIMIT = 50


def search_bin_sims():
    """
    Search binary similarity pairs with in-memory filtering and sorting.
    Uses involves:{md5} if md5 filter is provided, else falls back to built:{algo}.
    """
    t_start = time.perf_counter()
    r = get_redis()

    collection = request.args.get("collection")
    if not collection:
        return {"error": "No collection specified"}, 400

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

    sort_by = request.args.get("sort_by", "score")
    sort_order = request.args.get("sort_order", "desc")

    # The UI now sends unified filters instead of A/B specific ones.
    md5_filter = request.args.get("md5", "").strip().lower()
    file_name_filter = request.args.get("file_name", "").strip().lower()
    file_tag_filters = [t.strip().lower() for t in request.args.getlist("file_tag") if t.strip()]
    exclude_file_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_file_tag") if t.strip()]
    exclude_file_static_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_file_static_tag") if t.strip()]
    exclude_file_user_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_file_user_tag") if t.strip()]

    # Similarity-level tag filters
    sim_tag_filters = [t.strip().lower() for t in request.args.getlist("tag") if t.strip()]
    exclude_sim_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_tag") if t.strip()]
    exclude_sim_static_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_static_tag") if t.strip()]
    exclude_sim_user_tag_filters = [t.strip().lower() for t in request.args.getlist("exclude_user_tag") if t.strip()]

    # 1. Fetch Candidate SIDs
    t0 = time.perf_counter()
    if md5_filter:
        sids = r.smembers(f"{collection}:bin_sim:involves:{md5_filter}")
    else:
        sids = r.smembers(f"{collection}:bin_sim:built:{algo}")
    t1 = time.perf_counter()

    if not sids:
        return {"total": 0, "offset": offset, "limit": limit, "results": []}

    candidates = [s.decode() if isinstance(s, bytes) else s for s in sids]
    # Filter out mismatching algorithms just in case
    candidates = [s for s in candidates if f":bin_sim:{algo}:" in s]

    if not candidates:
        return {"total": 0, "offset": offset, "limit": limit, "results": []}

    # 2. Extract MD5s and Fetch File Meta
    t2 = time.perf_counter()
    light_docs = []
    unique_md5s = set()
    for sid in candidates:
        try:
            parts = sid.split(f"{collection}:bin_sim:{algo}:")[1].split("::")
            m_a, m_b = parts[0], parts[1]
            unique_md5s.add(m_a)
            unique_md5s.add(m_b)
            light_docs.append({"sid": sid, "m_a": m_a, "m_b": m_b})
        except Exception:
            continue

    file_meta_cache = {}
    if unique_md5s:
        md5_list = list(unique_md5s)
        pipe = r.pipeline()
        for md5 in md5_list:
            pipe.json().get(f"{collection}:file:{md5}:meta", "$")
        for md5, res in zip(md5_list, pipe.execute()):
            if res:
                m = res[0] if isinstance(res, list) else res
                if isinstance(m, str):
                    m = json.loads(m)
                file_meta_cache[md5] = m if isinstance(m, dict) else {}
            else:
                file_meta_cache[md5] = {}
    t3 = time.perf_counter()

    # 3. Fetch Required Numeric Fields via Pipeline
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
        
    if min_score is not None or max_score is not None:
        numeric_fields_to_fetch.add("score_collection_weighted")
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
    t4 = time.perf_counter()

    # 4. Filter
    filtered_docs = []
    q_lower = request.args.get("q", "").strip().lower()

    for ld in light_docs:
        m_a = ld["m_a"]
        m_b = ld["m_b"]
        meta_a = file_meta_cache.get(m_a, {})
        meta_b = file_meta_cache.get(m_b, {})

        ld["file_name_a"] = meta_a.get("file_name", m_a)
        ld["file_name_b"] = meta_b.get("file_name", m_b)
        ld["file_tags_a"] = meta_a.get("tags", [])
        ld["file_tags_b"] = meta_b.get("tags", [])
        ld["file_user_tags_a"] = meta_a.get("user_tags", [])
        ld["file_user_tags_b"] = meta_b.get("user_tags", [])
        ld["architecture_a"] = meta_a.get("language_id", "")
        ld["architecture_b"] = meta_b.get("language_id", "")
        ld["functions_count_a"] = meta_a.get("functions_count", 0)
        ld["functions_count_b"] = meta_b.get("functions_count", 0)

        # Filters
        if file_name_filter:
            if file_name_filter not in ld["file_name_a"].lower() and file_name_filter not in ld["file_name_b"].lower():
                continue

        if md5_filter:
            if md5_filter not in m_a.lower() and md5_filter not in m_b.lower():
                continue

        if q_lower:
            if q_lower not in ld["file_name_a"].lower() and \
               q_lower not in ld["file_name_b"].lower() and \
               q_lower not in m_a.lower() and \
               q_lower not in m_b.lower() and \
               not any(q_lower in t.lower() for t in ld["file_tags_a"]) and \
               not any(q_lower in t.lower() for t in ld["file_tags_b"]) and \
               not any(q_lower in t.lower() for t in ld["file_user_tags_a"]) and \
               not any(q_lower in t.lower() for t in ld["file_user_tags_b"]):
                continue

        if min_score is not None and ld.get("score_collection_weighted", 0) < min_score: continue
        if max_score is not None and ld.get("score_collection_weighted", 0) > max_score: continue

        cov_a = ld.get("coverage_a", 0)
        cov_b = ld.get("coverage_b", 0)
        if min_cov is not None and max(cov_a, cov_b) < min_cov: continue
        if max_cov is not None and min(cov_a, cov_b) > max_cov: continue

        shared = ld.get("shared_clusters", 0)
        if min_shared is not None and shared < min_shared: continue
        if max_shared is not None and shared > max_shared: continue

        if arch_filter:
            if arch_filter not in ld["architecture_a"].lower() and arch_filter not in ld["architecture_b"].lower():
                continue

        funcs_a = ld["functions_count_a"]
        funcs_b = ld["functions_count_b"]
        if min_funcs is not None and max(funcs_a, funcs_b) < min_funcs: continue
        if max_funcs is not None and min(funcs_a, funcs_b) > max_funcs: continue

        if file_tag_filters:
            combined_tags = set(t.lower() for t in ld["file_tags_a"] + ld["file_user_tags_a"] + ld["file_tags_b"] + ld["file_user_tags_b"])
            if not all(tf in combined_tags for tf in file_tag_filters):
                continue

        if exclude_file_tag_filters:
            combined_tags = set(t.lower() for t in ld["file_tags_a"] + ld["file_user_tags_a"] + ld["file_tags_b"] + ld["file_user_tags_b"])
            if any(tf in combined_tags for tf in exclude_file_tag_filters):
                continue
        if exclude_file_static_tag_filters:
            combined_static_tags = set(t.lower() for t in ld["file_tags_a"] + ld["file_tags_b"])
            if any(tf in combined_static_tags for tf in exclude_file_static_tag_filters):
                continue
        if exclude_file_user_tag_filters:
            combined_user_tags = set(t.lower() for t in ld["file_user_tags_a"] + ld["file_user_tags_b"])
            if any(tf in combined_user_tags for tf in exclude_file_user_tag_filters):
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
    paged_sids = [d["sid"] for d in paged_light]

    # 7. Fetch FULL JSON for the page
    final_docs = []
    if paged_sids:
        pipe = r.pipeline()
        for sid in paged_sids:
            pipe.json().get(sid, "$")
        page_raw = pipe.execute()
        
        for sid, res in zip(paged_sids, page_raw):
            if not res: continue
            doc = res[0] if isinstance(res, list) else res
            if isinstance(doc, str): doc = json.loads(doc)
            
            doc["_id"] = sid
            doc.pop("diff", None)
            
            m_a = doc.get("md5_a", "")
            m_b = doc.get("md5_b", "")
            meta_a = file_meta_cache.get(m_a, {})
            meta_b = file_meta_cache.get(m_b, {})
            
            doc["file_name_a"] = meta_a.get("file_name", m_a)
            doc["file_name_b"] = meta_b.get("file_name", m_b)
            doc["file_tags_a"] = meta_a.get("tags", [])
            doc["file_tags_b"] = meta_b.get("tags", [])
            doc["file_user_tags_a"] = meta_a.get("user_tags", [])
            doc["file_user_tags_b"] = meta_b.get("user_tags", [])
            
            doc["architecture_a"] = doc.get("architecture_a") or meta_a.get("language_id", "")
            doc["architecture_b"] = doc.get("architecture_b") or meta_b.get("language_id", "")
            doc["functions_count_a"] = doc.get("functions_count_a") or meta_a.get("functions_count", 0)
            doc["functions_count_b"] = doc.get("functions_count_b") or meta_b.get("functions_count", 0)
            
            doc["compiler_a"] = meta_a.get("compiler") or meta_a.get("compiler_id", "")
            doc["compiler_b"] = meta_b.get("compiler") or meta_b.get("compiler_id", "")
            doc["entry_date_a"] = meta_a.get("entry_date", 0)
            doc["entry_date_b"] = meta_b.get("entry_date", 0)

            # Re-apply sim_tag_filters if provided
            if sim_tag_filters:
                combined_sim_tags = set(t.lower() for t in doc.get("tags", []) + doc.get("user_tags", []))
                if not all(tf in combined_sim_tags for tf in sim_tag_filters):
                    total -= 1
                    continue
                    
            if exclude_sim_tag_filters:
                combined_sim_tags = set(t.lower() for t in doc.get("tags", []) + doc.get("user_tags", []))
                if any(tf in combined_sim_tags for tf in exclude_sim_tag_filters):
                    total -= 1
                    continue
            if exclude_sim_static_tag_filters:
                combined_sim_static_tags = set(t.lower() for t in doc.get("tags", []))
                if any(tf in combined_sim_static_tags for tf in exclude_sim_static_tag_filters):
                    total -= 1
                    continue
            if exclude_sim_user_tag_filters:
                combined_sim_user_tags = set(t.lower() for t in doc.get("user_tags", []))
                if any(tf in combined_sim_user_tags for tf in exclude_sim_user_tag_filters):
                    total -= 1
                    continue

            final_docs.append(doc)

    t7 = time.perf_counter()
    logging.info(f"BIN_SIM SEARCH | Fetch SIDs:{t1-t0:.3f}s | Meta:{t3-t2:.3f}s | ZSCORE:{t4-t3:.3f}s | Filter:{t5-t4:.3f}s | Sort:{t6-t5:.3f}s | FinalPage:{t7-t6:.3f}s | TOTAL:{t7-t_start:.3f}s | count={len(candidates)}")

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": final_docs
    }


def search_umap():
    """Retrieve UMAP projection coordinates and file metadata for all binaries in a collection."""
    r = get_redis()
    collection = request.args.get("collection")
    if not collection:
        return {"error": "No collection specified"}, 400
    
    algo = request.args.get("algo", "unweighted_cosine")
    
    # 1. Fetch UMAP coords
    umap_key = f"{collection}:bin_sim:umap:{algo}"
    umap_data_raw = r.json().get(umap_key, "$")
    
    if not umap_data_raw:
        return {"nodes": []}
        
    umap_data = umap_data_raw[0] if isinstance(umap_data_raw, list) else umap_data_raw
    if not isinstance(umap_data, dict):
        return {"nodes": []}

    md5_list = list(umap_data.keys())
    
    # 2. Fetch all file meta and function counts for nodes
    pipe = r.pipeline()
    for md5 in md5_list:
        pipe.json().get(f"{collection}:file:{md5}:meta", "$")
        pipe.scard(f"{collection}:idx:file:functions:{md5}")
    results = pipe.execute()
    
    nodes = []
    for i, md5 in enumerate(md5_list):
        coords = umap_data.get(md5)
        if not coords or len(coords) < 2:
            continue
            
        res = results[2 * i]
        func_count = results[2 * i + 1]
        
        node = {
            "id": md5,
            "x": coords[0],
            "y": coords[1],
            "functions_count": func_count or 0
        }
        
        if res:
            m = res[0] if isinstance(res, list) else res
            if isinstance(m, str):
                try:
                    m = json.loads(m)
                except:
                    pass
            
            if isinstance(m, dict):
                node["file_name"] = m.get("file_name", md5)
                node["architecture"] = m.get("language_id", m.get("architecture", ""))
                node["tags"] = m.get("tags", [])
                node["user_tags"] = m.get("user_tags", [])
                node["compiler"] = m.get("compiler") or m.get("compiler_id", "")
                node["entry_date"] = m.get("entry_date", 0)
        
        if not node.get("file_name"):
            node["file_name"] = md5
            
        nodes.append(node)

    return {"nodes": nodes}
