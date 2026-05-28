import time
import json
import logging
import math
from collections import defaultdict
from bsimvis.app.services.redis_client import get_redis


class BinSimService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def build_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5_a=None,
        md5_b=None,
        min_cohesion=0.0,
        job_service=None,
        job_id=None,
    ):
        """
        Builds binary similarity diff docs and scores for pairs of binaries.
        Uses a cluster-first greedy sweep algorithm.
        """
        r = self.r
        start_time = time.time()

        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Starting Binary Similarity Build for collection {collection} (algo: {algo})")

        # 1. Fetch all files (binaries)
        file_keys = []
        if md5_a and md5_b:
            binaries = [md5_a, md5_b]
        else:
            # Get all md5s
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=f"{collection}:file:*", count=1000)
                file_keys.extend([k.decode() if isinstance(k, bytes) else k for k in keys])
                if cursor == 0:
                    break
            
            binaries = []
            for k in file_keys:
                if k.endswith(":meta"):
                    continue
                parts = k.split(":")
                if len(parts) >= 3:
                    binaries.append(parts[2])
            binaries = list(set(binaries))

        num_binaries = len(binaries)
        if num_binaries < 2:
            msg = "Not enough binaries to compare."
            if job_service and job_id:
                job_service.add_log(job_id, msg)
                job_service.update_progress(job_id, 100)
            return True

        # 2. Build cluster frequency map for rarity
        # We need to know for each cluster, how many distinct binaries have it.
        if job_service and job_id:
            job_service.add_log(job_id, "[*] Precomputing cluster rarities...")

        binary_cluster_maps = {}
        cluster_binary_count_job = defaultdict(int)
        
        # We need to load all functions for all binaries in the job
        for i, md5 in enumerate(binaries):
            func_set_key = f"{collection}:idx:file:functions:{md5}"
            raw_ids = r.smembers(func_set_key)
            fids = [fid.decode().replace(":meta", "") if isinstance(fid, bytes) else str(fid).replace(":meta", "") for fid in raw_ids]
            
            # Map of cid -> set of function IDs for this binary
            b_cluster_map = defaultdict(set)
            
            if fids:
                pipe = r.pipeline()
                for fid in fids:
                    pipe.smembers(f"{fid}:clusters")
                
                results = pipe.execute()
                
                for idx, fid in enumerate(fids):
                    clusters_res = results[idx]
                    if clusters_res:
                        for c_raw in clusters_res:
                            cid = c_raw.decode() if isinstance(c_raw, bytes) else str(c_raw)
                            b_cluster_map[cid].add(fid)
            
            binary_cluster_maps[md5] = b_cluster_map
            for cid in b_cluster_map.keys():
                cluster_binary_count_job[cid] += 1
                
            if job_service and job_id and (i + 1) % 50 == 0:
                job_service.update_progress(job_id, int((i + 1) / num_binaries * 10), f"Loading cluster maps: {i+1}/{num_binaries}")

        def get_col_rarity(cid):
            # Try to get the true collection count from cluster meta (set during HDBSCAN)
            # Fallback to local job count if missing
            global_count = cluster_meta.get(cid, {}).get("unique_files_count", cluster_binary_count_job.get(cid, 0))
            return 1.0 / math.log(1 + global_count + 1)

        # 3. Load cluster meta (cohesion)
        all_cids = set()
        for cmap in binary_cluster_maps.values():
            all_cids.update(cmap.keys())
            
        cluster_meta = {}
        if all_cids:
            if job_service and job_id:
                job_service.add_log(job_id, f"[*] Loading metadata for {len(all_cids)} clusters...")
            
            cids_list = list(all_cids)
            pipe = r.pipeline()
            for cid in cids_list:
                pipe.json().get(f"{collection}:cluster:{algo}:{cid}:meta", "$")
                
            meta_results = pipe.execute()
            for i, cid in enumerate(cids_list):
                res = meta_results[i]
                if res:
                    m = res[0] if isinstance(res, list) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    cluster_meta[cid] = m
                else:
                    cluster_meta[cid] = {"cohesion_score": 0.0, "cluster_name": f"Cluster {cid}"}

        # 4. Generate Pairs
        pairs = []
        if md5_a and md5_b:
            if md5_a < md5_b:
                pairs.append((md5_a, md5_b))
            else:
                pairs.append((md5_b, md5_a))
        else:
            for i in range(len(binaries)):
                for j in range(i + 1, len(binaries)):
                    b1, b2 = binaries[i], binaries[j]
                    if b1 < b2:
                        pairs.append((b1, b2))
                    else:
                        pairs.append((b2, b1))

        num_pairs = len(pairs)
        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Computing similarities for {num_pairs} pairs...")

        # 5. Process Pairs (Greedy Sweep)
        processed = 0
        pipe = r.pipeline()
        
        for m_a, m_b in pairs:
            cmap_a = binary_cluster_maps[m_a]
            cmap_b = binary_cluster_maps[m_b]
            
            def get_pair_sim_rarity(cid):
                count_in_pair = len(cmap_a.get(cid, [])) + len(cmap_b.get(cid, []))
                return 1.0 / math.log(1 + count_in_pair + 1)
            
            shared_cids = set(cmap_a.keys()).intersection(set(cmap_b.keys()))
            
            # Sort shared clusters by cohesion descending
            shared_cids_sorted = sorted(
                list(shared_cids),
                key=lambda c: float(cluster_meta.get(c, {}).get("cohesion_score", 0.0)),
                reverse=True
            )
            
            assigned_a = set()
            assigned_b = set()
            diff_matched = []
            
            # Weighted score accumulators
            sum_weighted_cohesion_sim = 0.0
            sum_weights_sim = 0.0
            
            sum_weighted_cohesion_col = 0.0
            sum_weights_col = 0.0
            
            sum_weighted_cohesion_unweighted = 0.0
            sum_weights_unweighted = 0.0
            
            for cid in shared_cids_sorted:
                pool_a = cmap_a[cid] - assigned_a
                pool_b = cmap_b[cid] - assigned_b
                
                if pool_a and pool_b:
                    cohesion = float(cluster_meta.get(cid, {}).get("cohesion_score", 0.0))
                    if cohesion < min_cohesion:
                        continue
                        
                    s_rarity = get_pair_sim_rarity(cid)
                    c_rarity = get_col_rarity(cid)
                    cluster_feat = float(cluster_meta.get(cid, {}).get("avg_features", 1.0))
                    # Avoid zero weight if avg_features is 0 or missing
                    if cluster_feat <= 0:
                        cluster_feat = 1.0
                    
                    count_a = len(pool_a)
                    count_b = len(pool_b)
                    match_ratio = min(count_a, count_b) / max(count_a, count_b)
                    
                    diff_matched.append({
                        "cluster_id": cid,
                        "cluster_uuid": cluster_meta.get(cid, {}).get("cluster_uuid", ""),
                        "cluster_name": cluster_meta.get(cid, {}).get("cluster_name", str(cid)),
                        "cohesion": cohesion,
                        "sim_rarity": s_rarity,
                        "collection_rarity": c_rarity,
                        "avg_features": cluster_feat,
                        "funcs_a": list(pool_a),
                        "funcs_b": list(pool_b),
                        "count_a": count_a,
                        "count_b": count_b,
                        "match_ratio": match_ratio
                    })
                    
                    assigned_a.update(pool_a)
                    assigned_b.update(pool_b)
                    
                    sum_weighted_cohesion_sim += cohesion * s_rarity * match_ratio * cluster_feat
                    sum_weights_sim += s_rarity * cluster_feat
                    
                    sum_weighted_cohesion_col += cohesion * c_rarity * match_ratio * cluster_feat
                    sum_weights_col += c_rarity * cluster_feat
                    
                    sum_weighted_cohesion_unweighted += cohesion * match_ratio * cluster_feat
                    sum_weights_unweighted += 1.0 * cluster_feat
            
            # Unique clusters logic (grouping unassigned funcs by their tightest cluster)
            all_funcs_a = set()
            for funcs in cmap_a.values():
                all_funcs_a.update(funcs)
            
            all_funcs_b = set()
            for funcs in cmap_b.values():
                all_funcs_b.update(funcs)
                
            unassigned_a = all_funcs_a - assigned_a
            unassigned_b = all_funcs_b - assigned_b
            
            unique_to_a = []
            unclustered_a = []
            if unassigned_a:
                grouped_a = defaultdict(list)
                for fid in unassigned_a:
                    cids = []
                    for cid, funcs in cmap_a.items():
                        if fid in funcs:
                            cids.append(cid)
                    
                    if cids:
                        best_cid = max(cids, key=lambda c: float(cluster_meta.get(c, {}).get("cohesion_score", 0.0)))
                        grouped_a[best_cid].append(fid)
                    else:
                        unclustered_a.append(fid)
                        
                for cid, funcs in grouped_a.items():
                    cluster_feat = float(cluster_meta.get(cid, {}).get("avg_features", 1.0))
                    if cluster_feat <= 0:
                        cluster_feat = 1.0
                    
                    s_rarity = get_pair_sim_rarity(cid)
                    c_rarity = get_col_rarity(cid)
                    unique_to_a.append({
                        "cluster_id": cid,
                        "cluster_uuid": cluster_meta.get(cid, {}).get("cluster_uuid", ""),
                        "cluster_name": cluster_meta.get(cid, {}).get("cluster_name", str(cid)),
                        "cohesion": float(cluster_meta.get(cid, {}).get("cohesion_score", 0.0)),
                        "sim_rarity": s_rarity,
                        "collection_rarity": c_rarity,
                        "avg_features": cluster_feat,
                        "funcs": funcs
                    })
                    sum_weights_sim += s_rarity * cluster_feat
                    sum_weights_col += c_rarity * cluster_feat
                    sum_weights_unweighted += 1.0 * cluster_feat

            unique_to_b = []
            unclustered_b = []
            if unassigned_b:
                grouped_b = defaultdict(list)
                for fid in unassigned_b:
                    cids = []
                    for cid, funcs in cmap_b.items():
                        if fid in funcs:
                            cids.append(cid)
                    if cids:
                        best_cid = max(cids, key=lambda c: float(cluster_meta.get(c, {}).get("cohesion_score", 0.0)))
                        grouped_b[best_cid].append(fid)
                    else:
                        unclustered_b.append(fid)
                for cid, funcs in grouped_b.items():
                    cluster_feat = float(cluster_meta.get(cid, {}).get("avg_features", 1.0))
                    if cluster_feat <= 0:
                        cluster_feat = 1.0
                    
                    s_rarity = get_pair_sim_rarity(cid)
                    c_rarity = get_col_rarity(cid)
                    unique_to_b.append({
                        "cluster_id": cid,
                        "cluster_uuid": cluster_meta.get(cid, {}).get("cluster_uuid", ""),
                        "cluster_name": cluster_meta.get(cid, {}).get("cluster_name", str(cid)),
                        "cohesion": float(cluster_meta.get(cid, {}).get("cohesion_score", 0.0)),
                        "sim_rarity": s_rarity,
                        "collection_rarity": c_rarity,
                        "avg_features": cluster_feat,
                        "funcs": funcs
                    })
                    sum_weights_sim += s_rarity * cluster_feat
                    sum_weights_col += c_rarity * cluster_feat
                    sum_weights_unweighted += 1.0 * cluster_feat

            score_sim_weighted = sum_weighted_cohesion_sim / sum_weights_sim if sum_weights_sim > 0 else 0.0
            score_collection_weighted = sum_weighted_cohesion_col / sum_weights_col if sum_weights_col > 0 else 0.0
            score_unweighted = sum_weighted_cohesion_unweighted / sum_weights_unweighted if sum_weights_unweighted > 0 else 0.0
            
            cov_a = len(assigned_a) / len(all_funcs_a) if all_funcs_a else 0.0
            cov_b = len(assigned_b) / len(all_funcs_b) if all_funcs_b else 0.0

            sid = f"{collection}:bin_sim:{algo}:{m_a}::{m_b}"
            
            doc = {
                "md5_a": m_a,
                "md5_b": m_b,
                "algo": algo,
                "score": score_unweighted,
                "score_sim_weighted": score_sim_weighted,
                "score_collection_weighted": score_collection_weighted,
                "coverage_a": cov_a,
                "coverage_b": cov_b,
                "shared_clusters": len(diff_matched),
                "unique_clusters_a": len(unique_to_a),
                "unique_clusters_b": len(unique_to_b),
                "unclustered_a": len(unclustered_a),
                "unclustered_b": len(unclustered_b),
                "computed_at": int(time.time() * 1000),
                "diff": {
                    "matched": diff_matched,
                    "unique_to_a": unique_to_a,
                    "unique_to_b": unique_to_b,
                    "unclustered_a": unclustered_a,
                    "unclustered_b": unclustered_b
                }
            }
            
            pipe.json().set(sid, "$", doc)
            pipe.zadd(f"{collection}:bin_sim:score:{algo}", {sid: score_collection_weighted})
            pipe.sadd(f"{collection}:bin_sim:involves:{m_a}", sid)
            pipe.sadd(f"{collection}:bin_sim:involves:{m_b}", sid)
            pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)
            
            processed += 1
            
            if processed % 100 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = 10 + int(processed / num_pairs * 90)
                    job_service.update_progress(job_id, pct, f"Processed {processed}/{num_pairs} pairs")

        pipe.execute()
        
        if job_service and job_id:
            job_service.update_progress(job_id, 100, f"Completed binary similarity build for {processed} pairs.")
            
        return True

    def clear_bin_sim(self, collection, algo="unweighted_cosine", md5=None, job_service=None, job_id=None):
        """
        Clears binary similarity scores.
        If md5 is provided, clears only pairs involving that md5.
        """
        r = self.r
        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Clearing binary similarities (md5: {md5 or 'ALL'})")

        if md5:
            involves_key = f"{collection}:bin_sim:involves:{md5}"
            sids = r.smembers(involves_key)
            if sids:
                pipe = r.pipeline()
                for sid_raw in sids:
                    sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
                    pipe.delete(sid)
                    pipe.zrem(f"{collection}:bin_sim:score:{algo}", sid)
                    pipe.srem(f"{collection}:bin_sim:built:{algo}", sid)
                    
                    parts = sid.split(":")
                    if len(parts) >= 5:
                        m_a, m_b = parts[4].split("::") if "::" in parts[4] else (parts[3], parts[4])
                        # Let's cleanly extract it
                        try:
                            keys_split = sid.split(f"{collection}:bin_sim:{algo}:")[1].split("::")
                            m_a, m_b = keys_split[0], keys_split[1]
                            other_md5 = m_b if m_a == md5 else m_a
                            pipe.srem(f"{collection}:bin_sim:involves:{other_md5}", sid)
                        except:
                            pass
                
                pipe.delete(involves_key)
                pipe.execute()
        else:
            patterns = [
                f"{collection}:bin_sim:{algo}:*",
                f"{collection}:bin_sim:involves:*",
            ]
            
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                    if keys:
                        r.delete(*keys)
                    if cursor == 0:
                        break
            
            r.delete(f"{collection}:bin_sim:score:{algo}")
            r.delete(f"{collection}:bin_sim:built:{algo}")

        if job_service and job_id:
            job_service.update_progress(job_id, 100, "Cleared binary similarities.")
            
        return True

bin_sim_service = BinSimService()
