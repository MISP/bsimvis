import re

with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()

replacement = """        backend = config_service.get("similarity.discovery_backend", "rust_cpu")
        if backend in ["wgpu", "rust_cpu"] and algo == "unweighted_cosine":
            logging.info(f"[*] Running LCA projection for {total} functions in {batch_uuid or md5}")
            
            self.build_lca_snapshot(collection, algo=algo)
            
            vclass_keys = r.keys(f"{collection}:vclass:*:functions")
            vclass_funcs = {}
            for k in vclass_keys:
                k_str = k.decode() if isinstance(k, bytes) else k
                v_id = int(k_str.split(":")[2])
                vclass_funcs[v_id] = [f.decode() if isinstance(f, bytes) else f for f in r.smembers(k)]
                
            func_feat_counts = {}
            target_func_set = set(f.decode() if isinstance(f, bytes) else f for f in function_ids)
            all_funcs = [f.decode() if isinstance(f, bytes) else f for f in r.smembers(f"{collection}:indexed:functions")]
            
            pipe = r.pipeline(transaction=False)
            for f in all_funcs:
                pipe.zscore(f"{collection}:idx:func:bsim_features_count", f)
            res = pipe.execute()
            for f, count in zip(all_funcs, res):
                func_feat_counts[f] = float(count or 0)
            
            discovery_results_map = {} 
            def add_candidate(fid_target, fid_cand, score):
                t_total = func_feat_counts.get(fid_target, 0)
                c_total = func_feat_counts.get(fid_cand, 0)
                if t_total < min_features or c_total < min_features:
                    return
                if fid_target not in discovery_results_map:
                    discovery_results_map[fid_target] = []
                discovery_results_map[fid_target].append({"id": fid_cand, "score": score, "c_total": c_total})

            for v_id, funcs in vclass_funcs.items():
                if len(funcs) > 1:
                    for f1 in funcs:
                        for f2 in funcs:
                            if f1 != f2:
                                if f1 in target_func_set:
                                    add_candidate(f1, f2, 1.0)
            
            if self._base_snapshot:
                for u_id, v_id, score in self._base_snapshot:
                    if score >= min_score:
                        u_funcs = vclass_funcs.get(u_id, [])
                        v_funcs = vclass_funcs.get(v_id, [])
                        for f_u in u_funcs:
                            for f_v in v_funcs:
                                if f_u in target_func_set:
                                    add_candidate(f_u, f_v, score)
                                if f_v in target_func_set:
                                    add_candidate(f_v, f_u, score)
            
            discovery_results = []
            for fid, candidates in discovery_results_map.items():
                if fid in target_func_set:
                    candidates.sort(key=lambda x: x["score"], reverse=True)
                    if top_k > 0:
                        candidates = candidates[:top_k]
                    parts = fid.split(":")
                    md5_val = parts[2] if len(parts) >= 3 else "unknown"
                    addr_val = parts[3] if len(parts) >= 4 else ""
                    t_total = func_feat_counts.get(fid, 0)
                    discovery_results.append((fid, md5_val, addr_val, t_total, candidates))

            if discovery_results:
                written = self._persist_and_index_batch(
                    collection,
                    algo,
                    discovery_results,
                    min_features=min_features,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
                total_sims = written or 0
                
            if job_service and job_id:
                job_service.update_progress(job_id, 100, f"Completed LCA building {total_sims} similarities.")
            
            # 3. Mark all target functions as built so they aren't processed again
            if not skip_write:
                r.sadd(f"{collection}:built:functions:{algo}", *target_func_set)
                
            return True

        start_time = time.time()"""

content = re.sub(r'        start_time = time\.time\(\)', replacement, content, count=1)
with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)

