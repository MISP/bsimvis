import json
import logging

from flask import request
from bsimvis.app.services.redis_client import get_redis

DEFAULT_LIMIT = 50


def search_bin_sims():
    """
    Search binary similarity pairs with in-memory filtering and sorting.
    Uses involves:{md5} if md5 filter is provided, else falls back to built:{algo}.
    """
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
    if md5_filter:
        sids = r.smembers(f"{collection}:bin_sim:involves:{md5_filter}")
    else:
        sids = r.smembers(f"{collection}:bin_sim:built:{algo}")

    if not sids:
        return {"total": 0, "offset": offset, "limit": limit, "results": []}

    candidates = [s.decode() if isinstance(s, bytes) else s for s in sids]
    
    # Filter out mismatching algorithms just in case
    candidates = [s for s in candidates if f":bin_sim:{algo}:" in s]

    if not candidates:
        return {"total": 0, "offset": offset, "limit": limit, "results": []}

    # 2. Fetch all JSON documents
    pipe = r.pipeline()
    for sid in candidates:
        pipe.json().get(sid, "$")
    docs_raw = pipe.execute()

    docs = []
    md5s_needed = set()
    for sid, res in zip(candidates, docs_raw):
        if not res:
            continue
        doc = res[0] if isinstance(res, list) else res
        if isinstance(doc, str):
            doc = json.loads(doc)
        if not isinstance(doc, dict):
            continue
        doc["_id"] = sid
        # We don't need the heavy diff payload in listing
        doc.pop("diff", None)
        
        md5s_needed.add(doc.get("md5_a"))
        md5s_needed.add(doc.get("md5_b"))
        docs.append(doc)

    # 3. Fetch File Meta to enrich
    md5s_needed.discard(None)
    md5s_needed.discard("")
    file_meta_cache = {}
    if md5s_needed:
        md5_list = list(md5s_needed)
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

    # 4. Enrich and Filter
    filtered_docs = []
    for doc in docs:
        m_a = doc.get("md5_a", "")
        m_b = doc.get("md5_b", "")
        meta_a = file_meta_cache.get(m_a, {})
        meta_b = file_meta_cache.get(m_b, {})

        # Enrich
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

        # Filter: Name
        if file_name_filter:
            if file_name_filter not in doc["file_name_a"].lower() and file_name_filter not in doc["file_name_b"].lower():
                continue

        # Filter: MD5
        if md5_filter:
            if md5_filter not in m_a.lower() and md5_filter not in m_b.lower():
                continue
                
        # Filter: Global full-text q
        q = request.args.get("q", "").strip().lower()
        if q:
            if q not in doc["file_name_a"].lower() and \
               q not in doc["file_name_b"].lower() and \
               q not in m_a.lower() and \
               q not in m_b.lower() and \
               not any(q in t.lower() for t in doc["file_tags_a"]) and \
               not any(q in t.lower() for t in doc["file_tags_b"]) and \
               not any(q in t.lower() for t in doc["file_user_tags_a"]) and \
               not any(q in t.lower() for t in doc["file_user_tags_b"]):
                continue

        # Filter: Score
        score = doc.get("score_collection_weighted", doc.get("score", 0))
        if min_score is not None and score < min_score: continue
        if max_score is not None and score > max_score: continue

        # Filter: Coverage
        cov_a = doc.get("coverage_a", 0)
        cov_b = doc.get("coverage_b", 0)
        if min_cov is not None and max(cov_a, cov_b) < min_cov: continue
        if max_cov is not None and min(cov_a, cov_b) > max_cov: continue

        # Filter: Shared Clusters
        shared = doc.get("shared_clusters", 0)
        if min_shared is not None and shared < min_shared: continue
        if max_shared is not None and shared > max_shared: continue

        # Filter: Architecture
        if arch_filter:
            if arch_filter not in doc["architecture_a"].lower() and arch_filter not in doc["architecture_b"].lower():
                continue

        # Filter: Function Count
        funcs_a = doc.get("functions_count_a", 0)
        funcs_b = doc.get("functions_count_b", 0)
        if min_funcs is not None and max(funcs_a, funcs_b) < min_funcs: continue
        if max_funcs is not None and min(funcs_a, funcs_b) > max_funcs: continue

        # Filter: Tags
        if file_tag_filters:
            combined_tags = set(t.lower() for t in doc["file_tags_a"] + doc["file_user_tags_a"] + doc["file_tags_b"] + doc["file_user_tags_b"])
            if not all(tf in combined_tags for tf in file_tag_filters):
                continue

        if sim_tag_filters:
            combined_sim_tags = set(t.lower() for t in doc.get("tags", []) + doc.get("user_tags", []))
            if not all(tf in combined_sim_tags for tf in sim_tag_filters):
                continue

        # Filter: Exclude Tags
        if exclude_file_tag_filters:
            combined_tags = set(t.lower() for t in doc["file_tags_a"] + doc["file_user_tags_a"] + doc["file_tags_b"] + doc["file_user_tags_b"])
            if any(tf in combined_tags for tf in exclude_file_tag_filters):
                continue
        if exclude_file_static_tag_filters:
            combined_static_tags = set(t.lower() for t in doc["file_tags_a"] + doc["file_tags_b"])
            if any(tf in combined_static_tags for tf in exclude_file_static_tag_filters):
                continue
        if exclude_file_user_tag_filters:
            combined_user_tags = set(t.lower() for t in doc["file_user_tags_a"] + doc["file_user_tags_b"])
            if any(tf in combined_user_tags for tf in exclude_file_user_tag_filters):
                continue

        if exclude_sim_tag_filters:
            combined_sim_tags = set(t.lower() for t in doc.get("tags", []) + doc.get("user_tags", []))
            if any(tf in combined_sim_tags for tf in exclude_sim_tag_filters):
                continue
        if exclude_sim_static_tag_filters:
            combined_sim_static_tags = set(t.lower() for t in doc.get("tags", []))
            if any(tf in combined_sim_static_tags for tf in exclude_sim_static_tag_filters):
                continue
        if exclude_sim_user_tag_filters:
            combined_sim_user_tags = set(t.lower() for t in doc.get("user_tags", []))
            if any(tf in combined_sim_user_tags for tf in exclude_sim_user_tag_filters):
                continue

        filtered_docs.append(doc)

    total = len(filtered_docs)

    # 5. Sort
    sort_field_map = {
        "score": "score",
        "score_sim_weighted": "score_sim_weighted",
        "score_collection_weighted": "score_collection_weighted",
        "coverage": "coverage_a",
        "shared_clusters": "shared_clusters",
        "architecture": "architecture_a",
        "functions_count": "functions_count_a",
        "computed_at": "computed_at",
    }
    sort_field = sort_field_map.get(sort_by, "score")

    def sort_key(d):
        val = d.get(sort_field)
        if val is None:
            # Fallbacks
            if sort_field in ["score", "score_sim_weighted", "score_collection_weighted"]:
                val = d.get("score", 0)
            else:
                val = 0
        return val

    filtered_docs.sort(key=sort_key, reverse=(sort_order != "asc"))

    # 6. Paginate
    paged = filtered_docs[offset : offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": paged
    }


def search_umap():
    """Retrieve UMAP projection coordinates and filtered links for a collection."""
    r = get_redis()
    collection = request.args.get("collection")
    if not collection:
        return {"error": "No collection specified"}, 400
    
    algo = request.args.get("algo", "unweighted_cosine")
    
    # 1. Fetch UMAP coords
    umap_key = f"{collection}:bin_sim:umap:{algo}"
    umap_data_raw = r.json().get(umap_key, "$")
    
    if not umap_data_raw:
        return {"nodes": [], "links": []}
        
    umap_data = umap_data_raw[0] if isinstance(umap_data_raw, list) else umap_data_raw
    if not isinstance(umap_data, dict):
        return {"nodes": [], "links": []}

    md5_list = list(umap_data.keys())
    
    # 2. Fetch all file meta and function counts for nodes
    pipe = r.pipeline()
    for md5 in md5_list:
        pipe.json().get(f"{collection}:file:{md5}:meta", "$")
        pipe.scard(f"{collection}:idx:file:functions:{md5}")
    results = pipe.execute()
    
    nodes = []
    node_map = {}
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
                try: m = json.loads(m)
                except: pass
            
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
        node_map[md5] = node

    # 3. Fetch and filter links (similarities)
    # We use similar filtering logic as search_bin_sims but without pagination limit
    # because we want to see all links in the graph view (within reason)
    
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
    
    arch_filter = request.args.get("arch", "").strip().lower()
    md5_filter = request.args.get("md5", "").strip().lower()
    file_name_filter = request.args.get("file_name", "").strip().lower()
    
    # Get all candidate links
    if md5_filter:
        sids = r.smembers(f"{collection}:bin_sim:involves:{md5_filter}")
    else:
        sids = r.smembers(f"{collection}:bin_sim:built:{algo}")
        
    links = []
    if sids:
        candidates = [s.decode() if isinstance(s, bytes) else s for s in sids]
        candidates = [s for s in candidates if f":bin_sim:{algo}:" in s]
        
        # Batch fetch docs
        pipe = r.pipeline()
        for sid in candidates:
            pipe.json().get(sid, "$")
        docs_raw = pipe.execute()
        
        for sid, res in zip(candidates, docs_raw):
            if not res: continue
            doc = res[0] if isinstance(res, list) else res
            if isinstance(doc, str): doc = json.loads(doc)
            if not isinstance(doc, dict): continue
            
            m_a = doc.get("md5_a")
            m_b = doc.get("md5_b")
            
            # Basic existence check in UMAP
            if m_a not in node_map or m_b not in node_map:
                continue
                
            # Score filter
            score = doc.get("score_collection_weighted", doc.get("score", 0))
            if min_score is not None and score < min_score: continue
            if max_score is not None and score > max_score: continue
            
            # Coverage filter
            cov_a = doc.get("coverage_a", 0)
            cov_b = doc.get("coverage_b", 0)
            if min_cov is not None and max(cov_a, cov_b) < min_cov: continue
            if min_shared is not None and doc.get("shared_clusters", 0) < min_shared: continue

            # Name/MD5 filters (already handled if md5_filter was used for initial set, 
            # but we need to check both sides if file_name_filter is active)
            if file_name_filter:
                name_a = node_map[m_a].get("file_name", "").lower()
                name_b = node_map[m_b].get("file_name", "").lower()
                if file_name_filter not in name_a and file_name_filter not in name_b:
                    continue

            if arch_filter:
                arch_a = node_map[m_a].get("architecture", "").lower()
                arch_b = node_map[m_b].get("architecture", "").lower()
                if arch_filter not in arch_a and arch_filter not in arch_b:
                    continue

            links.append({
                "source": m_a,
                "target": m_b,
                "value": score
            })

    # Limit links to avoid crashing browser (e.g. 5000 links)
    if len(links) > 5000:
        links.sort(key=lambda x: x["value"], reverse=True)
        links = links[:5000]

    return {"nodes": nodes, "links": links}
