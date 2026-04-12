import redis
import json
import math
import time
import logging

# --- Shared Lua Scripts ---

class SimilarityService:
    def __init__(self, r=None):
        if r:
            self.r = r
        else:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
        
        from bsimvis.app.services.lua_manager import lua_manager
        self._find_script = lua_manager.get_script("find_candidates")
        self._clear_script = lua_manager.get_script("clear_similarity")
        
        from bsimvis.app.services.tag_service import tag_service
        self.tag_service = tag_service

    def build_batch(self, collection, batch_uuid=None, md5=None, algo="unweighted_cosine", top_k=1000, min_score=0.3, job_service=None, job_id=None, sleep_time=0):
        """
        Builds similarities for all functions in a batch or for a specific file.
        Uses chunked pipelining for O(N/100) performance and throttling.
        """
        r = self.r
        function_ids = []
        
        if batch_uuid:
            batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
            function_ids = list(r.smembers(batch_func_set))
        elif md5:
            # Find all functions for this MD5
            raw_ids = list(r.smembers(f"{collection}:file_funcs:{md5}"))
            function_ids = [fid.replace(":meta", "") if fid.endswith(":meta") else fid for fid in raw_ids]
            if not function_ids:
                pattern = f"idx:{collection}:func:{md5}:*:vec:tf"
                keys = r.scan_iter(pattern)
                function_ids = [k.replace(":vec:tf", "") for k in keys]

        total = len(function_ids)
        if total == 0:
            logging.warning(f"No functions found to build similarities for {batch_uuid or md5}")
            return True

        logging.info(f"[*] Building similarities for {total} functions in {batch_uuid or md5} (chunk_size=20, yield={sleep_time}s)...")
        
        start_time = time.time()
        chunk_size = 5
        
        for i in range(0, total, chunk_size):
            chunk = function_ids[i:i + chunk_size]
            
            # 1. Update Progress & Metrics
            if job_service and job_id:
                elapsed = time.time() - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                remaining = total - done
                eta = remaining / speed if speed > 0 else 0
                
                pct = int((i) / total * 100)
                job_service.update_progress(job_id, pct, f"Building similarities: {i}/{total} ({speed:.1f} fn/s, ETA: {int(eta)}s)")
                
                # Store metrics in job hash for global visibility
                r_queue = job_service.r
                r_queue.hset(f"job:{job_id}", mapping={
                    "speed": f"{speed:.2f}",
                    "eta": str(int(eta)),
                    "total_items": str(total),
                    "processed_items": str(i)
                })

            # 2. Process Chunk with Pipelining
            self._process_chunk(collection, chunk, algo, top_k, min_score)
            
            # 3. Dashboard Protection: Yield
            if sleep_time > 0 and i + chunk_size < total:
                time.sleep(sleep_time)
        
        # Final update
        if job_service and job_id:
            job_service.update_progress(job_id, 100, f"Completed building {total} similarities.")

        return True

    def _process_chunk(self, collection, chunk, algo, top_k, min_score):
        """Processes a chunk of functions using Redis pipelining."""
        r = self.r
        built_set_key = f"{collection}:built:functions:{algo}"
        
        # Phase 1: Bulk fetch built status and feature vectors
        pipe = r.pipeline()
        for fid in chunk:
            pipe.sismember(built_set_key, fid)
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        
        results = pipe.execute()
        
        # Phase 2: Filter and prepare Lua bursts
        targets_to_build = []
        for idx, fid in enumerate(chunk):
            is_built = results[idx * 2]
            features = results[idx * 2 + 1]
            
            if is_built:
                continue
            
            if not features:
                # Shortcut: Zero features = Mark as built immediately
                r.sadd(built_set_key, fid)
                continue
            
            targets_to_build.append((fid, features))
            
        if not targets_to_build:
            return

        # Phase 3: Execute Discovery (Lua)
        discovery_results = []
        for fid, features_raw in targets_to_build:
            parts = fid.split(":")
            if len(parts) < 5: continue
            md5, addr = parts[3], parts[4]
            
            target_feat_total = 0
            target_feat_norm_sq = 0
            lua_features_args = []
            
            for f_hash, f_tf_raw in features_raw:
                f_tf = float(f_tf_raw)
                target_feat_total += f_tf
                target_feat_norm_sq += (f_tf * f_tf)
                lua_features_args.extend([f_hash, str(f_tf)])
            
            target_feat_norm = math.sqrt(target_feat_norm_sq)
            
            # Lua ARGV: [id, collection, algo, threshold, total, norm, limit, features...]
            lua_args = [
                fid, collection, algo, min_score,
                target_feat_total, target_feat_norm, top_k
            ] + lua_features_args
            
            # Execute Discovery Script
            try:
                candidates_raw = self._find_script(args=lua_args)
                logging.debug(f"Discovery for {fid}: {len(candidates_raw) if candidates_raw else 0} raw results")
                if candidates_raw:
                    # Parse flat array return into triples (id, score, c_total)
                    candidates = []
                    for k in range(0, len(candidates_raw), 3):
                        candidates.append({
                            "id": candidates_raw[k],
                            "score": float(candidates_raw[k+1]),
                            "c_total": float(candidates_raw[k+2])
                        })
                    discovery_results.append((fid, md5, addr, target_feat_total, candidates))
                
                # Mark as built regardless of candidates found
                r.sadd(built_set_key, fid)
            except Exception as e:
                logging.error(f"Discovery Error for {fid}: {e}")

        # Phase 4: Persistence (Python)
        if not discovery_results:
            return

        persist_pipe = r.pipeline()
        now = int(time.time() * 1000)
        
        # We need to collect MD5s for the candidates to generate correct sim_docs
        # However, extracting them from the ID is faster than fetching from Redis
        def extract_md5(fid, coll):
            # doc: idx:{coll}:func:{md5}:{addr}
            prefix = f"idx:{coll}:func:"
            if fid.startswith(prefix):
                parts = fid[len(prefix):].split(":")
                if parts: return parts[0]
            return "unknown"

        for fid, t_md5, t_addr, t_total, candidates in discovery_results:
            for item in candidates:
                # Canonical Order (id_a > id_b)
                if fid > item["id"]:
                    id_a, id_b = fid, item["id"]
                    md5_a = t_md5
                    md5_b = extract_md5(item["id"], collection)
                    fc_a, fc_b = t_total, item["c_total"]
                else:
                    id_a, id_b = item["id"], fid
                    md5_a = extract_md5(item["id"], collection)
                    md5_b = t_md5
                    fc_a, fc_b = item["c_total"], t_total

                score_rounded = round(item["score"], 4)
                sid = f"idx:{collection}:sim:{algo}:{id_a}:{id_b}"
                
                sim_doc = {
                    "type": "sim",
                    "collection": collection,
                    "algo": algo,
                    "score": score_rounded,
                    "id1": id_a,
                    "id2": id_b,
                    "md5_1": md5_a,
                    "md5_2": md5_b,
                    "feat_count1": int(fc_a),
                    "feat_count2": int(fc_b),
                    "min_features": int(min(fc_a, fc_b)),
                    "entry_date": now,
                    "is_cross_binary": "true" if md5_a != md5_b else "false"
                }
                
                # Persistence
                persist_pipe.json().set(sid, "$", sim_doc)
                persist_pipe.zadd(f"{collection}:sim:score:{algo}", {sid: score_rounded})
                persist_pipe.zadd(f"{collection}:sim:all", {sid: 0})
                
                # Involves
                persist_pipe.sadd(f"{collection}:sim:involves:func:{id_a}", sid)
                persist_pipe.sadd(f"{collection}:sim:involves:func:{id_b}", sid)
                persist_pipe.sadd(f"{collection}:sim:involves:file:idx:{collection}:file:{md5_a}", sid)
                persist_pipe.sadd(f"{collection}:sim:involves:file:idx:{collection}:file:{md5_b}", sid)
                
                # Range filters
                persist_pipe.zadd(f"{collection}:sim:min_features", {sid: sim_doc["min_features"]})
                persist_pipe.zadd(f"{collection}:sim:is_cross_binary:{sim_doc['is_cross_binary']}", {sid: 0})

        res = persist_pipe.execute()
        logging.debug(f"Persistence executed: {len(res)} commands in pipe")

    def build_function(self, collection, base_id, algo="unweighted_cosine", top_k=1000, min_score=0.3, sleep_time=0):
        """
        Builds similarities for a single function against the collection.
        base_id: coll:function:md5:addr
        """
        parts = base_id.split(":")
        if len(parts) < 5:
            return False
        
        md5, addr = parts[3], parts[4]
        vec_key = f"{base_id}:vec:tf"
        built_set_key = f"{collection}:built:functions:{algo}"

        # Incremental Skip: Check if already built
        if self.r.sismember(built_set_key, base_id):
            return True

        features_raw = self.r.zrange(vec_key, 0, -1, withscores=True)
        if not features_raw:
            return False

        target_feat_total = 0
        target_feat_norm_sq = 0
        lua_features_args = []
        for f_hash, f_tf_raw in features_raw:
            f_tf = float(f_tf_raw)
            target_feat_total += f_tf
            target_feat_norm_sq += (f_tf * f_tf)
            lua_features_args.extend([f_hash, str(f_tf)])
        
        target_feat_norm = math.sqrt(target_feat_norm_sq)

        try:
            # Stage 1: Discovery (Lua)
            lua_args = [
                base_id, collection, algo, min_score,
                target_feat_total, target_feat_norm, top_k
            ] + lua_features_args
            
            candidates_raw = self._find_script(args=lua_args)
            
            # Mark as built
            self.r.sadd(built_set_key, base_id)
            
            if not candidates_raw:
                return True
                
            # Stage 2: Persistence (Python)
            now = int(time.time() * 1000)
            pipe = self.r.pipeline()
            
            def extract_md5(fid, coll):
                prefix = f"idx:{coll}:func:"
                if fid.startswith(prefix):
                    parts = fid[len(prefix):].split(":")
                    if parts: return parts[0]
                return "unknown"

            for k in range(0, len(candidates_raw), 3):
                item_id = candidates_raw[k]
                item_score = float(candidates_raw[k+1])
                item_fc = float(candidates_raw[k+2])
                
                # Canonical Order
                if base_id > item_id:
                    id_a, id_b = base_id, item_id
                    md5_a, md5_b = md5, extract_md5(item_id, collection)
                    fc_a, fc_b = target_feat_total, item_fc
                else:
                    id_a, id_b = item_id, base_id
                    md5_a, md5_b = extract_md5(item_id, collection), md5
                    fc_a, fc_b = item_fc, target_feat_total

                score_rounded = round(item_score, 4)
                sid = f"idx:{collection}:sim:{algo}:{id_a}:{id_b}"
                
                sim_doc = {
                    "type": "sim",
                    "collection": collection,
                    "algo": algo,
                    "score": score_rounded,
                    "id1": id_a,
                    "id2": id_b,
                    "md5_1": md5_a,
                    "md5_2": md5_b,
                    "feat_count1": int(fc_a),
                    "feat_count2": int(fc_b),
                    "min_features": int(min(fc_a, fc_b)),
                    "entry_date": now,
                    "is_cross_binary": "true" if md5_a != md5_b else "false"
                }

                pipe.json().set(sid, "$", sim_doc)
                pipe.zadd(f"{collection}:sim:score:{algo}", {sid: score_rounded})
                pipe.zadd(f"{collection}:sim:all", {sid: 0})
                pipe.sadd(f"{collection}:sim:involves:func:{id_a}", sid)
                pipe.sadd(f"{collection}:sim:involves:func:{id_b}", sid)
                pipe.sadd(f"{collection}:sim:involves:file:idx:{collection}:file:{md5_a}", sid)
                pipe.sadd(f"{collection}:sim:involves:file:idx:{collection}:file:{md5_b}", sid)
                pipe.zadd(f"{collection}:sim:min_features", {sid: sim_doc["min_features"]})
                pipe.zadd(f"{collection}:sim:is_cross_binary:{sim_doc['is_cross_binary']}", {sid: 0})

            pipe.execute()
            return True
        except Exception as e:
            logging.error(f"SimilarityService: Error for {base_id}: {e}")
            return False

    def clear_filtered(self, collection, field, value, algo=None):
        """
        Targeted similarity deletion.
        field: 'batch_uuid' or 'md5'
        """
        return self._clear_script(args=[collection, field, value, algo or ""])

    def get_pair_score(self, id1, id2, algo="unweighted_cosine"):
        """
        Returns the score for a specific pair.
        Uses cache if already built, otherwise performs direct calculation in Python.
        Ensures no on-demand building occurs to prevent index pollution.
        """
        try:
            parts1 = id1.split(":")
            parts2 = id2.split(":")
            if len(parts1) < 1 or len(parts2) < 1:
                return None
            
            coll1 = parts1[0]
            coll2 = parts2[0]

            # 1. Cross-collection diff: Always direct calculation (No Cache, No Build)
            if coll1 != coll2:
                return self.calculate_exact_score(id1, id2, algo=algo)

            # 2. Same collection: Check Cache first
            score = self.check_cache(id1, id2, coll1, algo)
            if score is not None:
                return score

            # 3. Fallback: Direct Calculation (No on-demand baking)
            return self.calculate_exact_score(id1, id2, algo=algo)

        except Exception as e:
            logging.error(f"SimilarityService: Error getting pair score: {e}")
            return None

    def calculate_exact_score(self, id1, id2, algo="unweighted_cosine"):
        """Fetches feature vectors and calculates similarity directly in Python."""
        try:
            vec1_raw = self.r.zrange(f"{id1}:vec:tf", 0, -1, withscores=True)
            vec2_raw = self.r.zrange(f"{id2}:vec:tf", 0, -1, withscores=True)

            if not vec1_raw or not vec2_raw:
                return None

            d1 = {h: float(s) for h, s in vec1_raw}
            d2 = {h: float(s) for h, s in vec2_raw}
            
            common = set(d1.keys()).intersection(set(d2.keys()))

            if algo == "jaccard":
                # Generalized Jaccard (Tanimoto): sum(min(a,b)) / sum(max(a,b))
                # sum(max(a,b)) = sum(a) + sum(b) - sum(min(a,b))
                sum_min = sum(min(d1[h], d2[h]) for h in common)
                sum_a = sum(d1.values())
                sum_b = sum(d2.values())
                union = sum_a + sum_b - sum_min
                return float(sum_min / union) if union > 0 else 0.0
            
            elif algo == "unweighted_cosine":
                # TF-weighted Cosine: sum(a*b) / (sqrt(sum(a^2)) * sqrt(sum(b^2)))
                dot_product = sum(d1[h] * d2[h] for h in common)
                norm1 = math.sqrt(sum(v**2 for v in d1.values()))
                norm2 = math.sqrt(sum(v**2 for v in d2.values()))
                return float(dot_product / (norm1 * norm2)) if (norm1 > 0 and norm2 > 0) else 0.0
            
            return None
        except Exception as e:
            logging.error(f"SimilarityService: Error calculating exact score for {id1}, {id2}: {e}")
            return None

    def check_cache(self, id1, id2, collection, algo):
        """Checks if a similarity pair is already built."""
        key1 = f"idx:{collection}:sim:{algo}:{id1}:{id2}"
        key2 = f"idx:{collection}:sim:{algo}:{id2}:{id1}"
        zset_key = f"{collection}:sim:score:{algo}"
        
        score = self.r.zscore(zset_key, key1)
        if score is None:
            score = self.r.zscore(zset_key, key2)
        
        return float(score) if score is not None else None

    def get_build_status(self, collection, batch_uuid=None, md5=None, algo="unweighted_cosine"):
        """Returns total vs built counts for a target."""
        r = self.r
        built_set = f"{collection}:built:functions:{algo}"
        
        total = 0
        built = 0
        
        if batch_uuid:
            batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
            total = r.scard(batch_func_set)
            try:
                built = r.execute_command("SINTERCARD", "2", batch_func_set, built_set)
            except:
                built = len(r.sinter(batch_func_set, built_set))
        elif md5:
            file_func_set = f"{collection}:file_funcs:{md5}"
            total = r.scard(file_func_set)
            try:
                built = r.execute_command("SINTERCARD", "2", file_func_set, built_set)
            except:
                built = len(r.sinter(file_func_set, built_set))
        else:
            # Full collection status
            total = r.scard(f"{collection}:indexed:functions")
            built = r.scard(built_set)
            
        return {
            "total": total,
            "built": built,
            "unbuilt": max(0, total - built),
            "ratio": (built / total * 100) if total > 0 else 0,
            "algo": algo
        }

    def list_batches_build_status(self, collection, algo="unweighted_cosine"):
        """Returns detailed build status for all batches in a collection."""
        r = self.r
        batch_uuids = r.smembers("global:batches")
        built_set = f"{collection}:built:functions:{algo}"

        results = []
        for uuid in sorted(list(batch_uuids)):
            batch_func_set = f"{collection}:batch:{uuid}:functions"
            if not r.exists(batch_func_set):
                continue

            meta_key = f"{collection}:batch:{uuid}"
            name_raw = r.json().get(meta_key, "$")
            name = "N/A"
            if name_raw:
                if isinstance(name_raw, list):
                    name_raw = name_raw[0]
                name = name_raw.get("name", "N/A")

            total = r.scard(batch_func_set)
            try:
                built = r.execute_command("SINTERCARD", "2", batch_func_set, built_set)
            except:
                built = len(r.sinter(batch_func_set, built_set))

            results.append({
                "batch_uuid": uuid,
                "name": name,
                "total": total,
                "built": built,
                "ratio": (built / total * 100) if total > 0 else 0
            })
            
        return results

    def list_files_build_status(self, collection, algo="unweighted_cosine"):
        """Returns detailed build status for all files in a collection."""
        r = self.r
        file_keys = r.smembers(f"{collection}:all_files")
        built_set = f"{collection}:built:functions:{algo}"

        results = []
        for f_key in sorted(list(file_keys)):
            parts = f_key.split(":")
            if len(parts) < 3: continue
            md5 = parts[2]

            meta = r.json().get(f_key, "$")
            if meta and isinstance(meta, list):
                meta = meta[0]
            name = meta.get("file_name", "N/A") if meta else "N/A"

            # Get functions for this file
            file_func_set = f"{collection}:file_funcs:{md5}"
            total = r.scard(file_func_set)
            
            try:
                built = r.execute_command("SINTERCARD", "2", file_func_set, built_set)
            except:
                built = len(r.sinter(file_func_set, built_set))

            results.append({
                "file_md5": md5,
                "name": name,
                "total": total,
                "built": built,
                "ratio": (built / total * 100) if total > 0 else 0
            })
            
        return results

    def _canonicalize_sid(self, collection: str, id1: str, id2: str, algo: str) -> str:
        """Returns the canonical key for a similarity pair."""
        if id1 > id2:
            return f"idx:{collection}:sim:{algo}:{id1}:{id2}"
        else:
            return f"idx:{collection}:sim:{algo}:{id2}:{id1}"

    def tag_similarity(self, collection: str, id1: str, id2: str, algo: str, tag: str) -> bool:
        """Adds a user tag to a similarity pair (delegates to TagService)."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        return self.tag_service.add_user_tag(collection, "similarity", sid, tag)

    def _ensure_tag_metadata(self, collection: str, tag: str):
        """Ensures a tag has metadata (color) in the global index."""
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        if not r.hexists(meta_key, tag):
            palette = [
                "#FF5555", "#50FA7B", "#F1FA8C", "#BD93F9", "#FF79C6", 
                "#8BE9FD", "#FFB86C", "#A6E22E", "#66D9EF"
            ]
            import random
            color = random.choice(palette)
            import json
            r.hset(meta_key, tag, json.dumps({"color": color, "priority": 0}))

    def get_tags(self, collection: str) -> dict:
        """Returns all tags and their metadata for a collection (delegates to TagService)."""
        return self.tag_service.get_collection_tags(collection)

    def set_tag_color(self, collection: str, tag: str, color: str) -> bool:
        """Updates the color for a tag."""
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        import json
        raw = r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"priority": 0}
        meta["color"] = color
        r.hset(meta_key, tag, json.dumps(meta))
        return True

    def set_tag_priority(self, collection: str, tag: str, priority: int) -> bool:
        """Updates the priority for a tag."""
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        import json
        raw = r.hget(meta_key, tag)
        meta = json.loads(raw) if raw else {"color": "#66d9ef"}
        meta["priority"] = int(priority)
        r.hset(meta_key, tag, json.dumps(meta))
        return True

    def untag_similarity(self, collection: str, id1: str, id2: str, algo: str, tag: str) -> bool:
        """Removes a user tag from a similarity pair (delegates to TagService)."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        return self.tag_service.remove_user_tag(collection, "similarity", sid, tag)
