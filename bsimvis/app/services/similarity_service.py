import redis
import json
import math
import time
import logging
from bsimvis.app.services.index_service import save_similarity
from bsimvis.app.services.milvus_service import milvus_service
from bsimvis.app.services.index_config import get_propagated_fields

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

    def build_batch(
        self,
        collection,
        batch_uuid=None,
        md5=None,
        algo=None,
        top_k=None,
        min_score=None,
        min_features=None,
        job_service=None,
        job_id=None,
        sleep_time=0,
    ):
        """
        Builds similarities for all functions in a batch or for a specific file.
        Uses chunked pipelining for O(N/100) performance and throttling.
        """
        from bsimvis.app.services.config_service import config_service

        if algo is None:
            algo = config_service.get("similarity.algo", "unweighted_cosine")
        if top_k is None:
            top_k = config_service.get("similarity.top_k", 1000)
        if min_score is None:
            min_score = config_service.get("similarity.min_score", 0.3)
        if min_features is None:
            min_features = config_service.get("similarity.min_features", 0)
        r = self.r
        function_ids = []

        if batch_uuid:
            batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
            function_ids = list(r.smembers(batch_func_set))
        elif md5:
            # Find all functions for this MD5
            raw_ids = list(r.smembers(f"{collection}:idx:file:functions:{md5}"))
            function_ids = [
                fid.replace(":meta", "") if fid.endswith(":meta") else fid
                for fid in raw_ids
            ]
            if not function_ids:
                pattern = f"{collection}:func:{md5}:*:vec:tf"
                keys = r.scan_iter(pattern)
                function_ids = [k.replace(":vec:tf", "") for k in keys]
        else:
            # Build for ALL functions in the collection
            function_ids = list(r.smembers(f"{collection}:indexed:functions"))

        total = len(function_ids)
        if total == 0:
            logging.warning(
                f"No functions found to build similarities for {batch_uuid or md5}"
            )
            return True

        logging.info(
            f"[*] Building similarities for {total} functions in {batch_uuid or md5} (chunk_size=20, yield={sleep_time}s)..."
        )

        start_time = time.time()
        chunk_size = 5

        for i in range(0, total, chunk_size):
            chunk = function_ids[i : i + chunk_size]

            # 1. Update Progress & Metrics
            if job_service and job_id:
                elapsed = time.time() - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                remaining = total - done
                eta = remaining / speed if speed > 0 else 0

                pct = int((i) / total * 100)
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Building similarities: {i}/{total} ({speed:.1f} fn/s, ETA: {int(eta)}s)",
                )

                # Store metrics in job hash for global visibility
                r_queue = job_service.r
                r_queue.hset(
                    f"job:{job_id}",
                    mapping={
                        "speed": f"{speed:.2f}",
                        "eta": str(int(eta)),
                        "total_items": str(total),
                        "processed_items": str(i),
                    },
                )

            # 2. Process Chunk with Pipelining
            self._process_chunk(collection, chunk, algo, top_k, min_score, min_features)

            # 3. Dashboard Protection: Yield
            if sleep_time > 0 and i + chunk_size < total:
                time.sleep(sleep_time)

        # Final update
        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Completed building {total} similarities."
            )

        return True

    def _process_chunk(self, collection, chunk, algo, top_k, min_score, min_features=0):
        """Processes a chunk of functions using Redis pipelining."""
        if algo in ["milvus_sparse"]:
            return self._process_chunk_milvus(
                collection, chunk, algo, top_k, min_score, min_features
            )

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

            if not features or len(features) < min_features:
                # Shortcut: Too few features = Mark as built immediately
                r.sadd(built_set_key, fid)
                continue

            targets_to_build.append((fid, features))

        if not targets_to_build:
            return

        # Phase 3: Execute Discovery (Lua)
        discovery_results = []
        for fid, features_raw in targets_to_build:
            parts = fid.split(":")
            if len(parts) < 4:
                continue
            md5, addr = parts[-2], parts[-1]

            target_feat_total = 0
            target_feat_norm_sq = 0
            lua_features_args = []

            for f_hash, f_tf_raw in features_raw:
                f_tf = float(f_tf_raw)
                target_feat_total += f_tf
                target_feat_norm_sq += f_tf * f_tf
                lua_features_args.extend([f_hash, str(f_tf)])

            target_feat_norm = math.sqrt(target_feat_norm_sq)

            # Lua ARGV: [id, collection, algo, threshold, total, norm, limit, min_features, features...]
            lua_args = [
                fid,
                collection,
                algo,
                min_score,
                target_feat_total,
                target_feat_norm,
                top_k,
                min_features,
            ] + lua_features_args

            # Execute Discovery Script
            try:
                candidates_raw = self._find_script(args=lua_args)
                logging.debug(
                    f"Discovery for {fid}: {len(candidates_raw) if candidates_raw else 0} raw results"
                )
                if candidates_raw:
                    # Parse flat array return into triples (id, score, c_total)
                    candidates = []
                    for k in range(0, len(candidates_raw), 3):
                        candidates.append(
                            {
                                "id": candidates_raw[k],
                                "score": float(candidates_raw[k + 1]),
                                "c_total": float(candidates_raw[k + 2]),
                            }
                        )
                    discovery_results.append(
                        (fid, md5, addr, target_feat_total, candidates)
                    )

                # Mark as built regardless of candidates found
                r.sadd(built_set_key, fid)
            except Exception as e:
                logging.error(f"Discovery Error for {fid}: {e}")

        # Phase 4: Persistence and Indexing
        if discovery_results:
            self._persist_and_index_batch(collection, algo, discovery_results)

    def _process_chunk_milvus(
        self, collection, chunk, algo, top_k, min_score, min_features=0
    ):
        """Processes a chunk using Milvus for discovery."""
        if not milvus_service.enabled:
            logging.error(
                "[!] Attempted to use milvus_sparse algorithm while Milvus is disabled."
            )
            return

        r = self.r
        built_set_key = f"{collection}:built:functions:{algo}"
        index_type = "SPARSE_INVERTED_INDEX"

        # Phase 1: Bulk fetch built status and feature vectors from Kvrocks
        pipe = r.pipeline()
        for fid in chunk:
            pipe.sismember(built_set_key, fid)
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        results = pipe.execute()

        targets_to_build = []
        for idx, fid in enumerate(chunk):
            is_built = results[idx * 2]
            features_raw = results[idx * 2 + 1]
            if is_built:
                continue
            if not features_raw or len(features_raw) < min_features:
                r.sadd(built_set_key, fid)
                continue

            tf_dict = {h: float(s) for h, s in features_raw}
            total_feat = sum(tf_dict.values())
            targets_to_build.append((fid, tf_dict, total_feat))

        if not targets_to_build:
            return

        # Phase 2: Milvus Discovery
        discovery_results = []
        for fid, tf_dict, t_total in targets_to_build:
            parts = fid.split(":")
            if len(parts) < 4:
                continue
            md5 = parts[-2]

            # Query Milvus
            candidates = milvus_service.search_similar(
                collection,
                tf_dict,
                top_k=top_k,
                min_score=min_score,
                index_type=index_type,
            )

            if candidates is not None:
                # 1. Enrichment and Filtering
                enriched = []
                if candidates:
                    count_idx = f"{collection}:idx:func:bsim_features_count"
                    for cand in candidates:
                        cand_id = cand["id"]
                        if cand_id == fid:
                            continue
                        c_total = float(r.zscore(count_idx, cand_id) or 0)
                        if c_total >= min_features:
                            enriched.append(
                                {
                                    "id": cand_id,
                                    "score": cand["score"],
                                    "c_total": c_total,
                                }
                            )

                # 2. Protection and Persistence Prep
                if not candidates:
                    # PROTECTION: If Milvus returned 0 results, check if the collection is empty.
                    # If it's empty, we likely have a sync issue, so don't mark as built yet.
                    col = milvus_service.ensure_collection(
                        collection, index_type=index_type
                    )
                    if col and col.num_entities == 0:
                        logging.warning(
                            f"[!] Skipping built status for {fid} because Milvus collection {col.name} is EMPTY. Sync required."
                        )
                        continue

                if enriched:
                    discovery_results.append((fid, md5, "", t_total, enriched))

                # 3. Mark as built
                r.sadd(built_set_key, fid)

        # Phase 3: Persistence and Indexing
        if discovery_results:
            self._persist_and_index_batch(collection, algo, discovery_results)

    def _persist_and_index_batch(self, collection, algo, discovery_results, pool_id=None):
        """Unified helper to persist similarity results and propagate metadata to search indexes."""
        r = self.r
        persist_pipe = r.pipeline()
        now = int(time.time() * 1000)

        def extract_md5(fid):
            # FID is {coll}:func:{md5}:{addr}
            parts = fid.split(":")
            if len(parts) >= 3:
                return parts[2]
            return "unknown"

        def extract_coll(fid):
            # FID is {coll}:func:{md5}:{addr}
            parts = fid.split(":")
            if len(parts) >= 1:
                return parts[0]
            return "unknown"

        # 1. Canonical Persistence
        for fid, t_md5, t_addr, t_total, candidates in discovery_results:
            for item in candidates:
                if fid > item["id"]:
                    id_a, id_b = fid, item["id"]
                    md5_a, md5_b = t_md5, extract_md5(item["id"])
                    fc_a, fc_b = t_total, item["c_total"]
                    coll_a, coll_b = extract_coll(fid), extract_coll(item["id"])
                else:
                    id_a, id_b = item["id"], fid
                    md5_a, md5_b = extract_md5(item["id"]), t_md5
                    fc_a, fc_b = item["c_total"], t_total
                    coll_a, coll_b = extract_coll(item["id"]), extract_coll(fid)

                score_rounded = round(item["score"], 4)
                
                if pool_id:
                    # Pool Namespace: global:pool:{pool_id}:sim:{fid1}::{fid2}
                    sid = f"global:pool:{pool_id}:sim:{id_a}::{id_b}"
                    score_key = f"global:pool:{pool_id}:sim:score"
                    all_key = f"global:pool:{pool_id}:sim:all"
                    involves_func_prefix = f"global:pool:{pool_id}:sim:involves:func:"
                    involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"
                    min_feat_key = f"global:pool:{pool_id}:sim:min_features"
                    cross_binary_prefix = f"global:pool:{pool_id}:sim:is_cross_binary:"
                else:
                    # Collection Namespace
                    func_prefix = f"{collection}:func:"
                    clean_id_a = id_a[len(func_prefix):] if id_a.startswith(func_prefix) else id_a
                    clean_id_b = id_b[len(func_prefix):] if id_b.startswith(func_prefix) else id_b
                    sid = f"{collection}:sim:{algo}:{clean_id_a}::{clean_id_b}"
                    score_key = f"{collection}:sim:score:{algo}"
                    all_key = f"{collection}:sim:all"
                    involves_func_prefix = f"{collection}:sim:involves:func:"
                    involves_file_prefix = f"{collection}:sim:involves:file:"
                    min_feat_key = f"{collection}:sim:min_features"
                    cross_binary_prefix = f"{collection}:sim:is_cross_binary:"

                sim_doc = {
                    "type": "sim",
                    "collection": collection if not pool_id else f"pool:{pool_id}",
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
                    "is_cross_binary": "true" if md5_a != md5_b else "false",
                }
                
                if pool_id:
                    sim_doc["coll_1"] = coll_a
                    sim_doc["coll_2"] = coll_b

                persist_pipe.json().set(sid, "$", sim_doc)
                persist_pipe.zadd(score_key, {sid: score_rounded})
                persist_pipe.zadd(all_key, {sid: 0})
                
                # For involves, we use the full FID if it's a pool
                inv_id_a = id_a if pool_id else clean_id_a
                inv_id_b = id_b if pool_id else clean_id_b
                persist_pipe.sadd(f"{involves_func_prefix}{inv_id_a}", sid)
                persist_pipe.sadd(f"{involves_func_prefix}{inv_id_b}", sid)
                
                # File involves
                inv_file_a = f"{coll_a}:{md5_a}" if pool_id else md5_a
                inv_file_b = f"{coll_b}:{md5_b}" if pool_id else md5_b
                persist_pipe.sadd(f"{involves_file_prefix}{inv_file_a}", sid)
                persist_pipe.sadd(f"{involves_file_prefix}{inv_file_b}", sid)
                
                persist_pipe.zadd(min_feat_key, {sid: sim_doc["min_features"]})
                persist_pipe.zadd(f"{cross_binary_prefix}{sim_doc['is_cross_binary']}", {sid: 0})

        persist_pipe.execute()

        # 2. Metadata Propagation (Search Index)
        propagated = get_propagated_fields("sim")
        # Optimization: use pool_id for coll param in save_similarity if provided
        target_coll = f"global:pool:{pool_id}" if pool_id else collection
        needs_func_meta = len(propagated.get("func", [])) > 0
        needs_file_meta = (
            len([f for f, t in propagated.get("file", []) if f != "file_md5"]) > 0
        )

        func_meta_cache = {}
        file_meta_cache = {}

        if needs_func_meta or needs_file_meta:
            func_ids_needed = set()
            file_ids_needed = set()

            for fid, t_md5, t_addr, t_total, candidates in discovery_results:
                func_ids_needed.add(fid)
                if needs_file_meta:
                    md5 = fid.split(":")[2] if ":" in fid else "unknown"
                    file_ids_needed.add(f"{collection}:file:{md5}")

                for item in candidates:
                    func_ids_needed.add(item["id"])
                    if needs_file_meta:
                        md5 = (
                            item["id"].split(":")[2] if ":" in item["id"] else "unknown"
                        )
                        file_ids_needed.add(f"{collection}:file:{md5}")

            meta_pipe = r.pipeline()
            func_ids_list = list(func_ids_needed)
            for fid in func_ids_list:
                meta_pipe.json().get(f"{fid}:meta", "$")

            file_ids_list = list(file_ids_needed)
            for fid in file_ids_list:
                meta_pipe.json().get(f"{fid}:meta", "$")

            raw_metas = meta_pipe.execute()

            # Unpack func metas
            raw_func = raw_metas[: len(func_ids_list)]
            for fid, raw in zip(func_ids_list, raw_func):
                if raw:
                    m = raw[0] if isinstance(raw, list) else raw
                    if isinstance(m, str):
                        m = json.loads(m)
                    func_meta_cache[fid] = m

            # Unpack file metas
            raw_file = raw_metas[len(func_ids_list) :]
            for fid, raw in zip(file_ids_list, raw_file):
                if raw:
                    m = raw[0] if isinstance(raw, list) else raw
                    if isinstance(m, str):
                        m = json.loads(m)
                    file_meta_cache[fid] = m

        idx_pipe = r.pipeline()
        for fid, t_md5, t_addr, t_total, candidates in discovery_results:
            for item in candidates:
                if fid > item["id"]:
                    id_a, id_b = fid, item["id"]
                else:
                    id_a, id_b = item["id"], fid

                func_prefix = f"{collection}:func:"
                clean_id_a = (
                    id_a[len(func_prefix) :] if id_a.startswith(func_prefix) else id_a
                )
                clean_id_b = (
                    id_b[len(func_prefix) :] if id_b.startswith(func_prefix) else id_b
                )
                sid = f"{collection}:sim:{algo}:{clean_id_a}::{clean_id_b}"

                md5_a = clean_id_a.split(":")[0]
                md5_b = clean_id_b.split(":")[0]

                # We pass a minimal sim_doc to save_similarity because it only needs md5s and tags
                # Native sim tags are handled by save_similarity internally
                sim_doc_for_idx = {
                    "md5_1": md5_a,
                    "md5_2": md5_b,
                    "tags": [],
                    "user_tags": [],
                }

                save_similarity(
                    idx_pipe,
                    target_coll,
                    sid,
                    sim_doc_for_idx,
                    func_meta1=func_meta_cache.get(id_a),
                    func_meta2=func_meta_cache.get(id_b),
                    file_meta1=file_meta_cache.get(f"{coll_a}:file:{md5_a}"),
                    file_meta2=file_meta_cache.get(f"{coll_b}:file:{md5_b}"),
                )

        idx_pipe.execute()

    def build_function(
        self,
        collection,
        base_id,
        algo=None,
        top_k=None,
        min_score=None,
        min_features=None,
        sleep_time=0,
    ):
        """
        Builds similarities for a single function against the collection.
        base_id: coll:function:md5:addr
        """
        from bsimvis.app.services.config_service import config_service

        if algo is None:
            algo = config_service.get("similarity.algo", "unweighted_cosine")
        if top_k is None:
            top_k = config_service.get("similarity.top_k", 1000)
        if min_score is None:
            min_score = config_service.get("similarity.min_score", 0.3)
        if min_features is None:
            min_features = config_service.get("similarity.min_features", 0)
        parts = base_id.split(":")
        if len(parts) < 4:
            return False

        md5, addr = parts[-2], parts[-1]
        vec_key = f"{base_id}:vec:tf"
        built_set_key = f"{collection}:built:functions:{algo}"

        # Incremental Skip: Check if already built
        if self.r.sismember(built_set_key, base_id):
            return True

        features_raw = self.r.zrange(vec_key, 0, -1, withscores=True)
        if not features_raw or len(features_raw) < min_features:
            # Skip if missing or too few features (mark as built to avoid retries)
            self.r.sadd(built_set_key, base_id)
            return True

        target_feat_total = 0
        target_feat_norm_sq = 0
        lua_features_args = []
        for f_hash, f_tf_raw in features_raw:
            f_tf = float(f_tf_raw)
            target_feat_total += f_tf
            target_feat_norm_sq += f_tf * f_tf
            lua_features_args.extend([f_hash, str(f_tf)])

        target_feat_norm = math.sqrt(target_feat_norm_sq)

        try:
            # Stage 1: Discovery (Lua)
            lua_args = [
                base_id,
                collection,
                algo,
                min_score,
                target_feat_total,
                target_feat_norm,
                top_k,
                min_features,
            ] + lua_features_args

            candidates_raw = self._find_script(args=lua_args)

            # Mark as built
            self.r.sadd(built_set_key, base_id)

            if not candidates_raw:
                return True

            # Stage 2: Persistence and Indexing
            # Format candidates_raw (Lua flat list) into unified discovery_results format
            enriched_candidates = []
            for k in range(0, len(candidates_raw), 3):
                enriched_candidates.append(
                    {
                        "id": candidates_raw[k],
                        "score": float(candidates_raw[k + 1]),
                        "c_total": float(candidates_raw[k + 2]),
                    }
                )

            discovery_results = [
                (base_id, md5, addr, target_feat_total, enriched_candidates)
            ]
            self._persist_and_index_batch(collection, algo, discovery_results)

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

    def clear_all(self, collection, algo=None):
        """Clears ALL similarities in the collection safely using SCAN."""
        r = self.r
        algos = [algo] if algo else ["jaccard", "unweighted_cosine", "milvus_sparse"]

        logging.info(f"[*] Clearing ALL similarities for collection: {collection}")

        # 1. Clear global ZSETs and SETs
        for a in algos:
            r.delete(f"{collection}:sim:score:{a}")
            r.delete(f"{collection}:built:functions:{a}")

        r.delete(f"{collection}:sim:all")
        r.delete(f"{collection}:sim:min_features")

        # 2. Scan and delete involves, tag indexes, and similarity docs
        from bsimvis.app.services.index_config import get_propagated_fields

        patterns = [
            f"{collection}:sim:involves:func:*",
            f"{collection}:sim:involves:file:*",
            f"{collection}:sim:is_cross_binary:*",
        ]

        # Sim-level secondary indexes (from IndexConfig propagation)
        propagated = get_propagated_fields("sim")
        for src_level, fields in propagated.items():
            for orig_field, target_field in fields:
                patterns.append(f"{collection}:idx:sim:{target_field}:*")
                r.delete(f"{collection}:reg:sim:{target_field}")
        for a in algos:
            patterns.append(f"{collection}:sim:{a}:*")

        for pattern in patterns:
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    r.delete(*keys)
                if cursor == 0:
                    break

        return True

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
                return (
                    float(dot_product / (norm1 * norm2))
                    if (norm1 > 0 and norm2 > 0)
                    else 0.0
                )

            elif algo in ["milvus_sparse"]:
                # Cosine Similarity: sum(a*b) / (norm1 * norm2)
                dot_product = sum(d1[h] * d2[h] for h in common)
                norm1 = math.sqrt(sum(v**2 for v in d1.values()))
                norm2 = math.sqrt(sum(v**2 for v in d2.values()))
                return (
                    float(dot_product / (norm1 * norm2))
                    if (norm1 > 0 and norm2 > 0)
                    else 0.0
                )

            return None
        except Exception as e:
            logging.error(
                f"SimilarityService: Error calculating exact score for {id1}, {id2}: {e}"
            )
            return None

    def check_cache(self, id1, id2, collection, algo):
        """Checks if a similarity pair is already built."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        zset_key = f"{collection}:sim:score:{algo}"

        score = self.r.zscore(zset_key, sid)
        return float(score) if score is not None else None

    def get_build_status(
        self, collection, batch_uuid=None, md5=None, algo="unweighted_cosine"
    ):
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
            file_func_set = f"{collection}:idx:file:functions:{md5}"
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
            "algo": algo,
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

            results.append(
                {
                    "batch_uuid": uuid,
                    "name": name,
                    "total": total,
                    "built": built,
                    "ratio": (built / total * 100) if total > 0 else 0,
                }
            )

        return results

    def list_files_build_status(self, collection, algo="unweighted_cosine"):
        """Returns detailed build status for all files in a collection."""
        r = self.r
        file_keys = r.smembers(f"{collection}:all_files")
        built_set = f"{collection}:built:functions:{algo}"

        results = []
        for f_key in sorted(list(file_keys)):
            parts = f_key.split(":")
            if len(parts) < 3:
                continue
            md5 = parts[2]

            meta = r.json().get(f_key, "$")
            if meta and isinstance(meta, list):
                meta = meta[0]
            name = meta.get("file_name", "N/A") if meta else "N/A"

            # Get functions for this file
            file_func_set = f"{collection}:idx:file:functions:{md5}"
            total = r.scard(file_func_set)

            try:
                built = r.execute_command("SINTERCARD", "2", file_func_set, built_set)
            except:
                built = len(r.sinter(file_func_set, built_set))

            results.append(
                {
                    "file_md5": md5,
                    "name": name,
                    "total": total,
                    "built": built,
                    "ratio": (built / total * 100) if total > 0 else 0,
                }
            )

        return results

    def _canonicalize_sid(self, collection: str, id1: str, id2: str, algo: str) -> str:
        """Returns the canonical key for a similarity pair."""
        # Clean identifiers for SID construction (strip collection:func:)
        func_prefix = f"{collection}:func:"
        c1 = id1[len(func_prefix) :] if id1.startswith(func_prefix) else id1
        c2 = id2[len(func_prefix) :] if id2.startswith(func_prefix) else id2

        if c1 > c2:
            return f"{collection}:sim:{algo}:{c1}::{c2}"
        else:
            return f"{collection}:sim:{algo}:{c2}::{c1}"

    def tag_similarity(
        self, collection: str, id1: str, id2: str, algo: str, tag: str
    ) -> bool:
        """Adds a user tag to a similarity pair (delegates to TagService)."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        return self.tag_service.add_user_tag(collection, "similarity", sid, tag)

    def _ensure_tag_metadata(self, collection: str, tag: str):
        """Ensures a tag has metadata (color) in the global index."""
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        if not r.hexists(meta_key, tag):
            palette = [
                "#FF5555",
                "#50FA7B",
                "#F1FA8C",
                "#BD93F9",
                "#FF79C6",
                "#8BE9FD",
                "#FFB86C",
                "#A6E22E",
                "#66D9EF",
            ]
            import random

            color = random.choice(palette)
            import json

            r.hset(meta_key, tag, json.dumps({"color": color, "priority": 0}))

    def untag_similarity(
        self, collection: str, id1: str, id2: str, algo: str, tag: str
    ) -> bool:
        """Removes a user tag from a similarity pair (delegates to TagService)."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        return self.tag_service.remove_user_tag(collection, "similarity", sid, tag)

    def build_pool(
        self,
        pool_id,
        job_service=None,
        job_id=None,
    ):
        """
        Orchestrates cross-collection similarity discovery for a pool.
        """
        from bsimvis.app.services.pool_service import pool_service
        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        collections = pool.get("collections", [])
        
        # New structured config handling
        only_cross_collection = pool.get("only_cross_collection", False)
        func_sim_params = pool.get("func_sim_params", {})
        
        algo = func_sim_params.get("algo", pool.get("algo", "unweighted_cosine"))
        top_k = int(func_sim_params.get("top_k", pool.get("top_k", 1000)))
        min_score = float(func_sim_params.get("min_score", pool.get("min_score", 0.3)))
        
        r = self.r
        
        # 1. Collect all functions from all collections
        all_function_ids = []
        for coll in collections:
            all_function_ids.extend([f.decode() if isinstance(f, bytes) else f for f in r.smembers(f"{coll}:all_functions")])
            
        total = len(all_function_ids)
        if total == 0:
            logging.warning(f"No functions found in collections {collections}")
            return True

        logging.info(f"[*] Building pool {pool_id} for {total} functions...")
        if job_service and job_id:
            job_service.add_log(job_id, f"Building pool {pool_id} for {total} functions...")

        start_time = time.time()
        chunk_size = 5
        
        for i in range(0, total, chunk_size):
            chunk = all_function_ids[i : i + chunk_size]
            
            # Update Progress
            if job_service and job_id:
                elapsed = time.time() - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                job_service.update_progress(job_id, int(done / total * 100), f"Building pool: {done}/{total} functions ({speed:.1f} fn/s)")

            discovery_results = []
            for fid in chunk:
                # Get vector
                vec_key = f"{fid}:vec:tf"
                features_raw = r.zrange(vec_key, 0, -1, withscores=True)
                if not features_raw:
                    continue
                
                target_feat_total = 0
                target_feat_norm_sq = 0
                lua_features_args = []
                for f_hash, f_tf_raw in features_raw:
                    f_tf = float(f_tf_raw)
                    target_feat_total += f_tf
                    target_feat_norm_sq += f_tf * f_tf
                    lua_features_args.extend([f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash), str(f_tf)])
                
                target_feat_norm = math.sqrt(target_feat_norm_sq)
                
                # Search in EACH collection of the pool
                candidates = []
                for search_coll in collections:
                    # ONLY_CROSS_COLLECTION FILTER: Skip Lua if query FID is from search_coll
                    if only_cross_collection and fid.startswith(f"{search_coll}:"):
                        continue

                    # LUA discovery for each collection
                    lua_args = [
                        fid,
                        search_coll,
                        algo,
                        min_score,
                        target_feat_total,
                        target_feat_norm,
                        top_k,
                        0, # min_features
                    ] + lua_features_args
                    
                    try:
                        coll_candidates_raw = self._find_script(args=lua_args)
                        if coll_candidates_raw:
                            for k in range(0, len(coll_candidates_raw), 3):
                                candidates.append({
                                    "id": coll_candidates_raw[k].decode() if isinstance(coll_candidates_raw[k], bytes) else coll_candidates_raw[k],
                                    "score": float(coll_candidates_raw[k+1]),
                                    "c_total": float(coll_candidates_raw[k+2])
                                })
                    except Exception as e:
                        logging.error(f"LUA search failed for {fid} in {search_coll}: {e}")

                if candidates:
                    # Sort and limit combined candidates
                    candidates.sort(key=lambda x: x["score"], reverse=True)
                    candidates = candidates[:top_k]
                    
                    parts = fid.split(":")
                    md5 = parts[2] if len(parts) >= 3 else "unknown"
                    discovery_results.append((fid, md5, "", target_feat_total, candidates))

            if discovery_results:
                self._persist_and_index_batch("", algo, discovery_results, pool_id=pool_id)

        # 2. Update Sync Snapshots
        pool_service.update_sync_snapshots(pool_id)
        
        # Automatically trigger pool clustering (which includes function clustering, bin_sim, and bin_clustering)
        from bsimvis.app.services.cluster_service import cluster_service
        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Triggering Pool Clustering automatically at the end of build...")
        cluster_service.run_pool_clustering(pool_id, job_service=job_service, job_id=job_id)

        if job_service and job_id:
            job_service.add_log(job_id, f"Pool build {pool_id} completed in {time.time() - start_time:.2f}s")
        
        return True

    def build_pool_bin_sim(self, pool_id, job_service=None, job_id=None):
        """
        Orchestrates cross-collection binary similarity calculations for a pool.
        """
        from bsimvis.app.services.pool_service import pool_service
        import math
        import time
        from collections import defaultdict
        
        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False
            
        # New structured config handling
        file_sim_params = pool.get("file_sim_params", {})
        if not file_sim_params.get("enabled", True):
            if job_service and job_id:
                job_service.add_log(job_id, f"[*] File similarity disabled for pool {pool_id}, skipping build_pool_bin_sim")
            return True

        collections = pool.get("collections", [])
        algo = pool.get("algo", "unweighted_cosine")
        cluster_params = pool.get("cluster_params", {})
        
        min_cohesion = float(file_sim_params.get("min_cohesion", cluster_params.get("min_cohesion", 0.5)))

        r = self.r
        start_time = time.time()

        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Starting Pool Binary Similarity Build for pool {pool_id}")

        # 1. Fetch function-level pool clusters and map function ID -> cluster UUID
        cluster_list_key = f"global:pool:{pool_id}:cluster:list"
        cluster_uuids = [c.decode() if isinstance(c, bytes) else c for c in r.smembers(cluster_list_key)]

        fid_to_cid = {}
        cluster_meta = {}
        if cluster_uuids:
            pipe = r.pipeline()
            for c_uuid in cluster_uuids:
                pipe.smembers(f"global:pool:{pool_id}:cluster:{c_uuid}:members")
                pipe.json().get(f"global:pool:{pool_id}:cluster:{c_uuid}:meta", "$")
            results = pipe.execute()
            for idx, c_uuid in enumerate(cluster_uuids):
                members = results[idx * 2]
                meta_raw = results[idx * 2 + 1]
                meta = (meta_raw[0] if isinstance(meta_raw, list) else meta_raw) if meta_raw else {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        pass
                cluster_meta[c_uuid] = meta
                for m in members:
                    fid = m.decode() if isinstance(m, bytes) else m
                    fid_to_cid[fid] = c_uuid

        # 2. Fetch all binaries across all collections in the pool
        binaries = []  # List of tuples (collection, md5)
        binary_func_counts = {}
        binary_fids = {}
        binary_cluster_maps = {}
        cluster_binary_count_job = defaultdict(int)

        for coll in collections:
            all_files_key = f"{coll}:all_files"
            file_keys = [d.decode() if isinstance(d, bytes) else str(d) for d in r.smembers(all_files_key)]
            for k in file_keys:
                if k.endswith(":meta"):
                    continue
                parts = k.split(":")
                if len(parts) >= 3:
                    md5 = parts[2]
                    binaries.append((coll, md5))

        num_binaries = len(binaries)
        if num_binaries < 2:
            msg = "Not enough binaries to compare."
            if job_service and job_id:
                job_service.add_log(job_id, msg)
            return True

        # Precompute file function sets and map to pool clusters
        for coll, md5 in binaries:
            func_set_key = f"{coll}:idx:file:functions:{md5}"
            raw_ids = r.smembers(func_set_key)
            fids = [
                (fid.decode().replace(":meta", "") if isinstance(fid, bytes) else str(fid).replace(":meta", ""))
                for fid in raw_ids
            ]
            
            b_cluster_map = defaultdict(set)
            binary_fids[(coll, md5)] = set(fids)
            binary_func_counts[(coll, md5)] = len(fids)

            for fid in fids:
                full_fid = fid if fid.startswith(f"{coll}:func:") else f"{coll}:func:{fid}"
                if full_fid in fid_to_cid:
                    cid = fid_to_cid[full_fid]
                    b_cluster_map[cid].add(full_fid)

            binary_cluster_maps[(coll, md5)] = b_cluster_map
            for cid in b_cluster_map.keys():
                cluster_binary_count_job[cid] += 1

        def get_col_rarity(cid):
            global_count = cluster_meta.get(cid, {}).get(
                "unique_files_count", cluster_binary_count_job.get(cid, 0)
            )
            return 1.0 / math.log(1 + global_count + 1)

        # Pre-fetch file metadata
        file_meta_cache = {}
        pipe_meta = r.pipeline()
        for coll, md5 in binaries:
            pipe_meta.json().get(f"{coll}:file:{md5}:meta", "$")
        meta_results = pipe_meta.execute()
        for (coll, md5), res in zip(binaries, meta_results):
            if res:
                m = res[0] if isinstance(res, list) else res
                if isinstance(m, str):
                    try:
                        m = json.loads(m)
                    except Exception:
                        pass
                file_meta_cache[(coll, md5)] = m if isinstance(m, dict) else {}
            else:
                file_meta_cache[(coll, md5)] = {}

        # 3. Generate Pairs (all combinations cross-collection/in pool)
        pairs = []
        for i in range(len(binaries)):
            for j in range(i + 1, len(binaries)):
                b1, b2 = binaries[i], binaries[j]
                if b1 < b2:
                    pairs.append((b1, b2))
                else:
                    pairs.append((b2, b1))

        # 4. Process Pairs (Greedy Sweep)
        persist_pipe = r.pipeline()
        now = int(time.time() * 1000)

        for b1, b2 in pairs:
            coll_a, md5_a = b1
            coll_b, md5_b = b2

            cmap_a = binary_cluster_maps[b1]
            cmap_b = binary_cluster_maps[b2]

            def get_pair_sim_rarity(cid):
                count_in_pair = len(cmap_a.get(cid, [])) + len(cmap_b.get(cid, []))
                return 1.0 / math.log(1 + count_in_pair + 1)

            shared_cids = set(cmap_a.keys()).intersection(set(cmap_b.keys()))
            shared_cids_sorted = sorted(
                list(shared_cids),
                key=lambda c: float(cluster_meta.get(c, {}).get("cohesion_score", 0.0)),
                reverse=True,
            )

            assigned_a = set()
            assigned_b = set()
            diff_matched = []

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
                        "match_ratio": match_ratio,
                    })

                    assigned_a.update(pool_a)
                    assigned_b.update(pool_b)

                    num_matched = min(count_a, count_b)
                    num_total = count_a + count_b

                    sum_weighted_cohesion_sim += 2.0 * num_matched * cohesion * s_rarity * cluster_feat
                    sum_weights_sim += num_total * s_rarity * cluster_feat

                    sum_weighted_cohesion_col += 2.0 * num_matched * cohesion * c_rarity * cluster_feat
                    sum_weights_col += num_total * c_rarity * cluster_feat

                    sum_weighted_cohesion_unweighted += 2.0 * num_matched * cohesion * cluster_feat
                    sum_weights_unweighted += num_total * cluster_feat

            sim_score = (sum_weighted_cohesion_sim / sum_weights_sim) if sum_weights_sim > 0 else 0.0
            col_weighted_score = (sum_weighted_cohesion_col / sum_weights_col) if sum_weights_col > 0 else 0.0
            unweighted_score = (sum_weighted_cohesion_unweighted / sum_weights_unweighted) if sum_weights_unweighted > 0 else 0.0

            final_score = sim_score
            if algo == "unweighted_cosine":
                final_score = unweighted_score
            elif algo == "weighted_cosine":
                final_score = col_weighted_score

            final_score = round(final_score, 4)

            # Persist pool bin_sim
            sid = f"global:pool:{pool_id}:bin_sim:{algo}:{coll_a}:{md5_a}::{coll_b}:{md5_b}"
            doc = {
                "type": "bin_sim",
                "pool_id": pool_id,
                "algo": algo,
                "score": final_score,
                "md5_1": md5_a,
                "md5_2": md5_b,
                "coll_1": coll_a,
                "coll_2": coll_b,
                "sim_weighted_score": round(sim_score, 4),
                "collection_weighted_score": round(col_weighted_score, 4),
                "unweighted_score": round(unweighted_score, 4),
                "matched_clusters_count": len(diff_matched),
                "matched_clusters": diff_matched,
                "entry_date": now,
            }

            persist_pipe.json().set(sid, "$", doc)
            persist_pipe.zadd(f"global:pool:{pool_id}:bin_sim:score:{algo}", {sid: final_score})
            persist_pipe.sadd(f"global:pool:{pool_id}:bin_sim:involves:{coll_a}:{md5_a}", sid)
            persist_pipe.sadd(f"global:pool:{pool_id}:bin_sim:involves:{coll_b}:{md5_b}", sid)
            persist_pipe.sadd(f"global:pool:{pool_id}:bin_sim:built:{algo}", sid)

        persist_pipe.execute()
        if job_service and job_id:
            job_service.add_log(job_id, f"Pool binary similarity build finished. Found {len(pairs)} comparisons.")

        return True
