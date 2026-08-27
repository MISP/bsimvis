import redis
import json
import math
import time
import logging
import hashlib
from bsimvis.app.services.index_service import save_similarity
from bsimvis.app.services.milvus_service import milvus_service
from bsimvis.app.services.index_config import get_propagated_fields

# LCA acceleration (rust_cpu/wgpu) needs the compiled extension from
# native/bsimvis_similarity/ (`maturin develop --release`), which is a
# manual build step, not part of `uv sync`/pip install -- an environment
# that skipped it must not crash every file's analysis job on the very
# first similarity build. Checked once at import time; build_batch uses it
# to fall back to the plain per-function Python path automatically instead
# of blowing up on an unguarded `import bsimvis_similarity_native`.
try:
    import bsimvis_similarity_native as _native_probe  # noqa: F401

    NATIVE_AVAILABLE = True
except ImportError:
    NATIVE_AVAILABLE = False
    logging.warning(
        "bsimvis_similarity_native not installed -- LCA discovery "
        "(rust_cpu/wgpu) is unavailable, falling back to the plain "
        "per-function Python path. Build it with `maturin develop --release "
        "--manifest-path native/bsimvis_similarity/Cargo.toml` to enable."
    )

# --- Shared Lua Scripts ---


class SimilarityService:
    def __init__(self, r=None):
        if r:
            self.r = r
        else:
            from bsimvis.app.services.redis_client import get_redis

            self.r = get_redis()

        from bsimvis.app.services.lua_manager import lua_manager

        # Build-sim discovery is pure Python now (see _discover) to avoid the
        # kvrocks global EVAL lock that serialized concurrent workers.
        self._clear_script = lua_manager.get_script("clear_similarity")

        from bsimvis.app.services.tag_service import tag_service

        self.tag_service = tag_service

        # Per-build read caches. Feature posting lists, vector norms and feature
        # counts are all static during a build (they only change at ingestion),
        # so memoizing them turns the repeated cross-target reads the old Lua did
        # per target into one fetch each. Reset at every top-level build entry.
        from collections import OrderedDict

        self._pl_cache = OrderedDict()  # feature key -> [(func_id, tf_float), ...]
        self._pl_pairs = 0
        # ponytail: LRU-bounded by total cached pairs (~hundreds of MB) so a huge
        # collection can't OOM the cache. Raise/lower if RAM vs hit-rate needs it.
        self._pl_budget = 5_000_000
        self._norm_cache = {}  # v_id -> vector norm (float)
        self._count_cache = {}  # (count_idx_key, v_id) -> feature count (float)
        
        # LCA Acceleration Graph Cache
        self._base_snapshot = None
        self._delta_snapshots = []
        self._snapshot_budget_bytes = 1024 * 1024 * 500 # 500MB


    def build_lca_snapshot(self, collection, algo="unweighted_cosine", workers=4):
        # Reset on every call, including an early return below -- otherwise a
        # collection/build where this bails (native missing, no vclasses yet)
        # would silently reuse whatever _base_snapshot a PRIOR call (possibly
        # for a different collection) last set, feeding stale cross-vclass
        # edges into build_batch's discovery.
        self._base_snapshot = None
        if not NATIVE_AVAILABLE:
            return
        import bsimvis_similarity_native as sn
        from bsimvis.app.services.config_service import config_service
        import logging

        r = self.r
        vclass_keys = r.keys(f"{collection}:vclass:*:vec:tf")
        if not vclass_keys:
            return
        
        vectors = []
        self.vclass_map = []
        pipe = r.pipeline(transaction=False)
        for key in vclass_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            v_id = key_str.split(":")[2]
            self.vclass_map.append(v_id)
            pipe.zrange(key, 0, -1, withscores=True)
            
        results = pipe.execute()
        for vec in results:
            parsed = [(h.decode() if isinstance(h, bytes) else h, float(tf)) for h, tf in vec]
            vectors.append(parsed)
            
        scorer = sn.ExactScorer(vectors)
        indices = list(range(len(vectors)))
        
        backend = config_service.get("similarity.discovery_backend", "rust_cpu")
        min_score = config_service.get("similarity.min_score", 0.9)
        top_k = 0 # No top_k for discovery
        
        edges_raw = None
        if backend == "wgpu" and hasattr(scorer, "select_target_block_wgpu"):
            try:
                edges_raw = scorer.select_target_block_wgpu(indices, indices, algo, workers, top_k, min_score, 0.05)
                # Recompute and threshold in Rust f64 is done by the backend/we can also pass it to CPU scorer to be safe, but select_target_block_wgpu returns accurate scores.
            except Exception as e:
                logging.error(f"WGPU fallback on GPU failure with telemetry: {e}")
                edges_raw = None
                
        if edges_raw is None:
            edges_raw = scorer.select_target_block(indices, indices, algo, workers, top_k, min_score)
            
        from bsimvis.app.services.graph_service import graph_service
        gen = graph_service.get_active_generation(collection)
        mapped_edges = []
        # vclass_map gives stable ids, edges_raw uses indices 0...len(vectors)-1
        for u, targets in enumerate(edges_raw):
            for v, s in targets:
                mapped_edges.append((int(self.vclass_map[u]), int(self.vclass_map[v]), s))
        
        next_gen = gen + 1
        graph_service.write_base_partitions(collection, next_gen, mapped_edges)
        graph_service.set_active_generation(collection, next_gen)
        
        self._base_snapshot = mapped_edges
    def _reset_read_caches(self):
        """Drop per-build read caches (call at each top-level build entry so a
        later build never sees posting lists/norms stale from a prior ingestion)."""
        self._pl_cache.clear()
        self._pl_pairs = 0
        self._norm_cache.clear()
        self._count_cache.clear()

    def _pl_warm(self, keys):
        """Pipeline-fetch any uncached feature posting lists in `keys` in one RTT
        and memoize them. Order within a list is irrelevant to the callers (they
        sum over it), so a plain ZRANGE is fine."""
        c = self._pl_cache
        miss = [k for k in keys if k not in c]
        if not miss:
            return
        pipe = self.r.pipeline(transaction=False)
        for k in miss:
            pipe.zrange(k, 0, -1, withscores=True)
        for k, raw in zip(miss, pipe.execute()):
            pl = [(fid, float(tf)) for fid, tf in raw]
            c[k] = pl
            c.move_to_end(k)
            self._pl_pairs += len(pl)
        while self._pl_pairs > self._pl_budget and len(c) > 1:
            _, ev = c.popitem(last=False)
            self._pl_pairs -= len(ev)

    def _pl(self, key):
        """Cached feature posting list [(func_id, tf_float), ...] for one feature."""
        c = self._pl_cache
        pl = c.get(key)
        if pl is not None:
            c.move_to_end(key)
            return pl
        self._pl_warm([key])
        return c.get(key, [])

    def _counts(self, count_idx, ids):
        """Cached feature counts (ZSCORE count_idx) for ids; pipeline the misses."""
        cache = self._count_cache
        miss = [i for i in ids if (count_idx, i) not in cache]
        if miss:
            pipe = self.r.pipeline(transaction=False)
            for i in miss:
                pipe.zscore(count_idx, i)
            for i, v in zip(miss, pipe.execute()):
                cache[(count_idx, i)] = float(v or 0)
        return [cache[(count_idx, i)] for i in ids]

    def _norms(self, ids):
        """Cached vector norms (GET {id}:vec:norm) for ids; pipeline the misses."""
        cache = self._norm_cache
        miss = [i for i in ids if i not in cache]
        if miss:
            pipe = self.r.pipeline(transaction=False)
            for i in miss:
                pipe.get(f"{i}:vec:norm")
            for i, v in zip(miss, pipe.execute()):
                cache[i] = float(v or 0)
        return [cache[i] for i in ids]

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
        index_depth="none",
        skip_write=False,
    ):
        """
        Builds similarities for all functions in a batch or for a specific file.
        Uses chunked pipelining for O(N/100) performance and throttling.
        """
        self._reset_read_caches()
        self._func_meta_cache = {}
        self._file_meta_cache = {}
        self._sim_registry_seen = set()
        from bsimvis.app.services.config_service import config_service

        if algo is None:
            algo = config_service.get("similarity.algo", "unweighted_cosine")
        if top_k is None:
            top_k = config_service.get("similarity.top_k", 1000)
        if min_score is None:
            min_score = config_service.get("similarity.min_score", 0.9)
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
            # Build for ALL functions in the collection.
            # Force a complete rebuild by clearing the per-function skip-set first:
            # concurrent per-file ingestion can mark a binary's functions "built"
            # against a partially-populated collection (before the other binaries'
            # functions are visible), so their cross-binary similarities are never
            # computed. An all-build must be authoritative, so recompute every
            # function's candidates (idempotent for already-correct pairs).
            r.delete(f"{collection}:built:functions:{algo}")
            function_ids = list(r.smembers(f"{collection}:indexed:functions"))

        total = len(function_ids)
        if total == 0:
            logging.warning(
                f"No functions found to build similarities for {batch_uuid or md5}"
            )
            return True

        logging.info(
            f"[*] Building similarities for {total} functions in {batch_uuid or md5} (chunk_size=100, yield={sleep_time}s)..."
        )

        backend = config_service.get("similarity.discovery_backend", "rust_cpu")
        if backend in ["wgpu", "rust_cpu"] and algo == "unweighted_cosine":
            logging.info(f"[*] Running LCA projection for {total} functions in {batch_uuid or md5}")

            # Same-vector-class exact matching (below) needs no native code --
            # it's a plain Redis set lookup. Only the cross-class fuzzy
            # matching inside build_lca_snapshot needs bsimvis_similarity_native;
            # that call is a no-op (self._base_snapshot stays None) when it's
            # missing, so this still finds every byte-identical function
            # across files even without the native extension built.
            self.build_lca_snapshot(collection, algo=algo)
            
            vclass_keys = r.keys(f"{collection}:vclass:*:functions")
            vclass_funcs = {}
            for k in vclass_keys:
                k_str = k.decode() if isinstance(k, bytes) else k
                try:
                    v_id = int(k_str.split(":")[2])
                except ValueError:
                    continue
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
            else:
                total_sims = 0
                
            small_fids = [f for f, count in func_feat_counts.items() if count < min_features and f in target_func_set]
            if small_fids and not skip_write:
                written_small = self._hash_match_small(collection, algo, small_fids, index_depth)
                total_sims += written_small or 0
                
            if job_service and job_id:
                job_service.update_progress(job_id, 100, f"Completed LCA building {total_sims} similarities.")
            
            # 3. Mark all target functions as built so they aren't processed again
            if not skip_write:
                r.sadd(f"{collection}:built:functions:{algo}", *target_func_set)
                
            return True

        start_time = time.time()
        # Count total functions in the collection to size chunks dynamically
        db_func_count = r.scard(f"{collection}:indexed:functions") or total
        chunk_size = max(1, min(100, int(100000 / max(1, db_func_count))))
        total_sims = 0
        last_t, last_done, last_sims = start_time, 0, 0

        for i in range(0, total, chunk_size):
            chunk = function_ids[i : i + chunk_size]

            # 1. Update Progress & Metrics
            if job_service and job_id:
                now = time.time()
                elapsed = now - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                sim_speed = total_sims / elapsed if elapsed > 0 else 0
                # Instantaneous speed over the last chunk (isolates real slowdowns
                # from the cumulative average, which lags after one slow chunk)
                d_t = now - last_t
                cur_speed = (done - last_done) / d_t if d_t > 0 else 0
                cur_sim_speed = (total_sims - last_sims) / d_t if d_t > 0 else 0
                last_t, last_done, last_sims = now, done, total_sims
                remaining = total - done
                eta = remaining / speed if speed > 0 else 0

                pct = int((i) / total * 100)
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Building similarities: {i}/{total} ({speed:.1f} fn/s, {sim_speed:.1f} sim/s, cur {cur_speed:.1f} fn/s, {cur_sim_speed:.1f} sim/s, ETA: {int(eta)}s)",
                )

                # Store metrics in job hash for global visibility
                r_queue = job_service.r
                r_queue.hset(
                    f"job:{job_id}",
                    mapping={
                        "speed": f"{speed:.2f}",
                        "sim_speed": f"{sim_speed:.2f}",
                        "eta": str(int(eta)),
                        "total_items": str(total),
                        "processed_items": str(i),
                    },
                )

            # 2. Process Chunk with Pipelining
            written = self._process_chunk(
                collection,
                chunk,
                algo,
                top_k,
                min_score,
                min_features,
                index_depth=index_depth,
                skip_write=skip_write,
            )
            total_sims += written or 0

            # Clear caches to prevent unbounded memory growth and GC stalls
            self._func_meta_cache.clear()
            self._file_meta_cache.clear()

            # 3. Dashboard Protection: Yield
            if sleep_time > 0 and i + chunk_size < total:
                time.sleep(sleep_time)

        # Final update
        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Completed building {total} similarities."
            )

        return True

    def _compute_lsh_buckets(self, features_raw, num_bands=30, rows_per_band=4):
        """Generates SimHash LSH buckets for a set of features."""
        # Signature size M = num_bands * rows_per_band (e.g. 8 * 16 = 128 bits)
        # We project features using deterministically seeded weights:
        # random_weight = ((hash(feat + plane) % 2) * 2) - 1  --> yields +1 or -1
        num_features = num_bands * rows_per_band
        if not features_raw:
            return []

        projections = [0.0] * num_features
        for f_hash, f_tf_raw in features_raw:
            f = f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash)
            tf = float(f_tf_raw)
            for j in range(num_features):
                # Deterministic projection weight hash
                h = int(hashlib.md5(f"{f}:{j}".encode()).hexdigest(), 16)
                weight = 1.0 if (h % 2 == 1) else -1.0
                projections[j] += tf * weight

        # Generate binary SimHash signature
        sig = [1 if val >= 0 else 0 for val in projections]

        # Group signature into bands to get LSH bucket strings
        buckets = []
        for band in range(num_bands):
            start = band * rows_per_band
            band_sig = sig[start : start + rows_per_band]
            band_str = "".join(map(str, band_sig))
            bucket_hash = hashlib.md5(band_str.encode()).hexdigest()
            buckets.append((band, bucket_hash))
        return buckets

    def _discover(self, args):
        """Python reimplementation of the find_candidates.lua / minhash_lsh.lua
        discovery step. Takes the same flat ARGV list the Lua scripts took and
        returns the same flat result list [id, score, c_total, ...].

        Why: kvrocks runs every EVAL under a single global interpreter lock, so
        concurrent workers building similarity serialize on it. Plain read
        commands (ZCARD/ZRANGE/ZSCORE/GET) take fine-grained RocksDB locks
        instead, so workers no longer contend. Reads are pipelined to keep the
        round-trip count close to the old single-EVAL cost.
        """
        if args[2] == "minhash_lsh":
            return self._discover_minhash(args)
        return self._discover_find(args)

    def _discover_find(self, args):
        """jaccard / unweighted_cosine discovery (mirrors find_candidates.lua)."""
        r = self.r
        target_id = args[0]
        collection = args[1]
        algo = args[2]
        threshold = float(args[3])
        target_total = float(args[4])
        target_norm = float(args[5])
        limit = int(args[6])
        min_features = float(args[7] or 0)

        target_features = {}
        for i in range(8, len(args), 2):
            target_features[args[i]] = float(args[i + 1])
        if not target_features:
            return []

        min_shared_norm_sq = 0.0
        if algo == "unweighted_cosine":
            min_shared_norm_sq = (threshold * target_norm) ** 2

        # 1. Size each feature's posting list, rarest-first (pipelined ZCARDs)
        feats = list(target_features.items())
        pipe = r.pipeline(transaction=False)
        for f_hash, _ in feats:
            pipe.zcard(f"{collection}:feature:{f_hash}:functions")
        sizes = pipe.execute()
        features_sorted = sorted(
            (
                {
                    "hash": h,
                    "tf": tf,
                    "key": f"{collection}:feature:{h}:functions",
                    "size": sz,
                }
                for (h, tf), sz in zip(feats, sizes)
            ),
            key=lambda x: x["size"],
        )

        # 2. Accumulate intersection (dot product / sum-min) with pruning bounds
        intersection_counts = {}
        shared_target_norm_sq = {}
        target_norm_sq = target_norm * target_norm
        processed_norm_sq = 0.0
        processed_total = 0.0
        num_candidates = 0

        for feat in features_sorted:
            remaining_norm_sq = target_norm_sq - processed_norm_sq
            remaining_total = target_total - processed_total
            can_add_new = True
            if algo == "unweighted_cosine":
                if remaining_norm_sq < min_shared_norm_sq:
                    can_add_new = False
            elif algo == "jaccard":
                if remaining_total < threshold * target_total:
                    can_add_new = False
            if not can_add_new and num_candidates == 0:
                break

            target_tf_sq = feat["tf"] * feat["tf"] if algo == "unweighted_cosine" else 0
            # Cached across targets in this build (feature posting lists are static
            # during a build). Order is irrelevant — we sum over the whole list.
            for func_id, cand_tf in self._pl(feat["key"]):
                if func_id == target_id:
                    continue
                is_existing = func_id in intersection_counts
                if is_existing or can_add_new:
                    if not is_existing:
                        intersection_counts[func_id] = 0.0
                        if algo == "unweighted_cosine":
                            shared_target_norm_sq[func_id] = 0.0
                        num_candidates += 1
                    if algo == "jaccard":
                        intersection_counts[func_id] += min(feat["tf"], cand_tf)
                    elif algo == "unweighted_cosine":
                        intersection_counts[func_id] += feat["tf"] * cand_tf
                        shared_target_norm_sq[func_id] += target_tf_sq

            processed_norm_sq += feat["tf"] * feat["tf"]
            processed_total += feat["tf"]

        # 3. Phase-1 bound filter
        kept = []
        for cid, intersect in intersection_counts.items():
            if algo == "jaccard":
                if intersect < threshold * target_total:
                    continue
            elif algo == "unweighted_cosine":
                if shared_target_norm_sq.get(cid, 0) < min_shared_norm_sq:
                    continue
            kept.append(cid)
        if not kept:
            return []

        # 4. Fetch candidate feature counts (cached; pipeline the misses)
        count_idx = f"{collection}:idx:func:bsim_features_count"
        totals = self._counts(count_idx, kept)

        candidate_list = []
        if algo == "jaccard":
            for cid, cand_total in zip(kept, totals):
                if cand_total < min_features or cand_total <= 0:
                    continue
                intersect = intersection_counts[cid]
                union = target_total + cand_total - intersect
                score = intersect / union if union > 0 else 0
                if score >= threshold and score > 0:
                    candidate_list.append((cid, score, cand_total))
        else:  # unweighted_cosine — norm only fetched for phase-2 survivors
            need_norm = []
            for cid, cand_total in zip(kept, totals):
                if cand_total < min_features or cand_total <= 0:
                    continue
                intersect = intersection_counts[cid]
                denom = threshold * target_norm
                max_cand_total = (intersect / denom) ** 2 if denom > 0 else 0
                if cand_total <= max_cand_total:
                    need_norm.append((cid, intersect, cand_total))
            if need_norm:
                norms = self._norms([cid for cid, _, _ in need_norm])
                for (cid, intersect, cand_total), cand_norm in zip(need_norm, norms):
                    score = (
                        intersect / (target_norm * cand_norm)
                        if (target_norm > 0 and cand_norm > 0)
                        else 0
                    )
                    if score >= threshold and score > 0:
                        candidate_list.append((cid, score, cand_total))

        candidate_list.sort(key=lambda x: x[1], reverse=True)
        result = []
        for cid, score, cand_total in candidate_list[:limit]:
            result.extend([cid, str(score), str(cand_total)])
        return result

    def _discover_minhash(self, args):
        """minhash_lsh discovery (mirrors minhash_lsh.lua)."""
        r = self.r
        target_id = args[0]
        collection = args[1]
        threshold = float(args[3])
        target_norm = float(args[5])
        limit = int(args[6])
        min_features = float(args[7] or 0)
        num_bands = int(args[8] or 10)

        # 1. Candidate set = SUNION of the target's LSH buckets
        bucket_keys = [
            f"{collection}:lsh:bucket:{band}:{args[9 + band]}"
            for band in range(num_bands)
        ]
        candidate_set = set()
        if bucket_keys:
            for cid in r.sunion(bucket_keys):
                if cid != target_id:
                    candidate_set.add(cid)
        if not candidate_set:
            return []

        # 2. Features start after the bucket hashes
        target_features = {}
        for i in range(9 + num_bands, len(args), 2):
            target_features[args[i]] = float(args[i + 1])

        # 3. Dot product against candidates only. Posting lists cached across
        # targets (warm all misses for this target in one RTT first).
        intersection_counts = {}
        keys = [f"{collection}:feature:{h}:functions" for h in target_features]
        self._pl_warm(keys)
        for f_hash, key in zip(target_features, keys):
            target_tf = target_features[f_hash]
            for func_id, cand_tf in self._pl(key):
                if func_id in candidate_set:
                    intersection_counts[func_id] = (
                        intersection_counts.get(func_id, 0.0) + target_tf * cand_tf
                    )
        if not intersection_counts:
            return []

        # 4. Score: fetch count + norm per candidate (cached; pipeline misses)
        count_idx = f"{collection}:idx:func:bsim_features_count"
        ids = list(intersection_counts.keys())
        totals = self._counts(count_idx, ids)
        norms = self._norms(ids)

        candidate_list = []
        for cid, cand_total, cand_norm in zip(ids, totals, norms):
            if cand_total < min_features or cand_total <= 0:
                continue
            intersect = intersection_counts[cid]
            score = (
                intersect / (target_norm * cand_norm)
                if (target_norm > 0 and cand_norm > 0)
                else 0
            )
            if score >= threshold:
                candidate_list.append((cid, score, cand_total))

        candidate_list.sort(key=lambda x: x[1], reverse=True)
        result = []
        for cid, score, cand_total in candidate_list[:limit]:
            result.extend([cid, str(score), str(cand_total)])
        return result

    def _process_chunk(
        self,
        collection,
        chunk,
        algo,
        top_k,
        min_score,
        min_features=0,
        index_depth="full",
        skip_write=False,
    ):
        """Processes a chunk of functions using Redis pipelining."""
        if algo in ["milvus_sparse"]:
            return self._process_chunk_milvus(
                collection,
                chunk,
                algo,
                top_k,
                min_score,
                min_features,
                index_depth=index_depth,
            )

        r = self.r
        built_set_key = f"{collection}:built:functions:{algo}"

        # ponytail: Check built status first to avoid fetching large vector objects for already built functions
        pipe = r.pipeline(transaction=False)
        for fid in chunk:
            pipe.sismember(built_set_key, fid)
        built_statuses = pipe.execute()

        # Phase 1: Only fetch feature vectors for functions not yet built
        targets_needing_vectors = [
            fid for fid, is_built in zip(chunk, built_statuses) if not is_built
        ]

        if not targets_needing_vectors:
            return

        pipe = r.pipeline(transaction=False)
        for fid in targets_needing_vectors:
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        vectors = pipe.execute()

        # Phase 2: Filter and prepare Lua bursts
        targets_to_build = []
        small_fids = []
        for fid, features in zip(targets_needing_vectors, vectors):
            if not features or len(features) < min_features:
                # Shortcut: Too few features = Mark as built immediately.
                # BSim is false-positive-prone here; these get exact FunctionID-hash
                # matches instead (see _hash_match_small).
                if not skip_write:
                    r.sadd(built_set_key, fid)
                    small_fids.append(fid)
                continue

            targets_to_build.append((fid, features))

        # Small functions: deterministic 1.0 / 0 similarity via exact FunctionID hash.
        # ponytail: skip_write (bench dry-run) skips this — it measures BSim perf only.
        # Pools go through build_pool (crosses every member's buckets), not this path.
        if small_fids and not collection.startswith("global:pool:"):
            self._hash_match_small(collection, algo, small_fids, index_depth)

        if not targets_to_build:
            return

        # Pre-populate LSH buckets if algorithm is minhash_lsh
        if algo == "minhash_lsh" and not skip_write:
            num_bands = 30
            lsh_pipe = r.pipeline(transaction=False)
            for fid, features in targets_to_build:
                buckets = self._compute_lsh_buckets(features, num_bands=num_bands)
                for band, b_hash in buckets:
                    bucket_key = f"{collection}:lsh:bucket:{band}:{b_hash}"
                    # Associate func to bucket
                    lsh_pipe.sadd(bucket_key, fid)
                    # Associate func to bucket key for quick lookup
                    lsh_pipe.set(f"{fid}:lsh:bucket_key:{band}", bucket_key)
            lsh_pipe.execute()

        # Phase 3: Execute Discovery (Lua Pipelining)
        prepared_targets = []
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
                lua_features_args.extend(
                    [
                        f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash),
                        str(f_tf),
                    ]
                )

            target_feat_norm = math.sqrt(target_feat_norm_sq)

            if algo == "minhash_lsh":
                # Lua ARGV: [id, collection, algo, threshold, total, norm, limit, min_features, num_bands, bucket_hashes..., features...]
                num_bands = 30
                buckets = self._compute_lsh_buckets(features_raw, num_bands=num_bands)
                hashes = [b_hash for band, b_hash in buckets]
                lua_args = (
                    [
                        fid,
                        collection,
                        algo,
                        min_score,
                        target_feat_total,
                        target_feat_norm,
                        top_k,
                        min_features,
                        num_bands,
                    ]
                    + hashes
                    + lua_features_args
                )
            else:
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

            prepared_targets.append((fid, md5, addr, target_feat_total, lua_args))

        discovery_results = []
        if prepared_targets:
            # Discover in Python (no EVAL lock); mark built in one pipelined batch.
            per_target_raw = [
                self._discover(lua_args)
                for fid, md5, addr, target_feat_total, lua_args in prepared_targets
            ]
            if not skip_write:
                built_pipe = r.pipeline(transaction=False)
                for fid, md5, addr, target_feat_total, lua_args in prepared_targets:
                    built_pipe.sadd(built_set_key, fid)
                built_pipe.execute()

            for idx, (fid, md5, addr, target_feat_total, lua_args) in enumerate(
                prepared_targets
            ):
                candidates_raw = per_target_raw[idx]
                if candidates_raw:
                    # Parse flat array return into triples (id, score, c_total)
                    candidates = []
                    for k in range(0, len(candidates_raw), 3):
                        candidates.append(
                            {
                                "id": (
                                    candidates_raw[k].decode()
                                    if isinstance(candidates_raw[k], bytes)
                                    else candidates_raw[k]
                                ),
                                "score": float(candidates_raw[k + 1]),
                                "c_total": float(candidates_raw[k + 2]),
                            }
                        )
                    discovery_results.append(
                        (fid, md5, addr, target_feat_total, candidates)
                    )

        # Phase 4: Persistence and Indexing
        if discovery_results:
            if skip_write:
                # ponytail: just count similarities found without writing to DB/disk
                total_sims_found = 0
                for fid, t_md5, t_addr, t_total, candidates in discovery_results:
                    if t_total < min_features:
                        continue
                    for item in candidates:
                        if item["c_total"] < min_features:
                            continue
                        total_sims_found += 1
                return total_sims_found
            return self._persist_and_index_batch(
                collection,
                algo,
                discovery_results,
                min_features=min_features,
                index_depth=index_depth,
            )
        return 0

    def _process_chunk_milvus(
        self,
        collection,
        chunk,
        algo,
        top_k,
        min_score,
        min_features=0,
        index_depth="full",
    ):
        """Processes a chunk using Milvus for discovery."""
        if not milvus_service.enabled:
            logging.error(
                "[!] Attempted to use milvus_sparse algorithm while Milvus is disabled."
            )
            return 0

        r = self.r
        built_set_key = f"{collection}:built:functions:{algo}"
        index_type = "SPARSE_INVERTED_INDEX"

        # Phase 1: Bulk fetch built status and feature vectors from Kvrocks
        pipe = r.pipeline(transaction=False)
        for fid in chunk:
            pipe.sismember(built_set_key, fid)
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        results = pipe.execute()

        targets_to_build = []
        small_fids = []
        for idx, fid in enumerate(chunk):
            is_built = results[idx * 2]
            features_raw = results[idx * 2 + 1]
            if is_built:
                continue
            if not features_raw or len(features_raw) < min_features:
                r.sadd(built_set_key, fid)
                small_fids.append(fid)
                continue

            tf_dict = {h: float(s) for h, s in features_raw}
            total_feat = sum(tf_dict.values())
            targets_to_build.append((fid, tf_dict, total_feat))

        # Small functions: exact FunctionID-hash matches (see _hash_match_small).
        # Pools go through build_pool, not this path.
        if small_fids and not collection.startswith("global:pool:"):
            self._hash_match_small(collection, algo, small_fids, index_depth)

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
            return self._persist_and_index_batch(
                collection,
                algo,
                discovery_results,
                min_features=min_features,
                index_depth=index_depth,
            )
        return 0

    def _hash_match_small(
        self,
        collection,
        algo,
        small_fids,
        index_depth,
        search_collections=None,
        pool_id=None,
        only_cross_collection=False,
    ):
        """Give small functions a deterministic similarity from exact FunctionID-hash
        matches: score 1.0 against cross-binary functions sharing the same hash, no
        edge otherwise. Reuses _persist_and_index_batch with min_features=0 so its
        BSim feature-floor guards don't drop these (that's the whole point).

        For pools, pass search_collections (the member collections) + pool_id: the
        buckets are per-collection, so matching across a pool means crossing every
        member's {coll}:funcid:{hash} set. only_cross_collection mirrors the pool's
        same-name filter.
        """
        r = self.r
        # Non-pool: match within the one collection. Pool: cross every member bucket.
        colls = search_collections or [collection]

        # Each small func's hash (cheap {fid}:funcid pointer written at ingestion)
        pipe = r.pipeline(transaction=False)
        for fid in small_fids:
            pipe.get(f"{fid}:funcid")
        hashes = pipe.execute()

        # Group fids by hash so each bucket is read once (small funcs sharing a hash
        # are exactly the ones that match each other)
        fids_by_hash = {}
        for fid, h in zip(small_fids, hashes):
            if not h or len(fid.split(":")) < 4:
                continue
            h = h.decode() if isinstance(h, bytes) else h
            fids_by_hash.setdefault(h, []).append(fid)
        if not fids_by_hash:
            return

        # Read each hash bucket once (union across searched collections). No size cap:
        # an (A,B) match is intrinsic to the two functions, so it must not depend on how
        # many other binaries share the hash — that's what keeps the file score canonical.
        buckets = {}
        for h in fids_by_hash:
            members = set()
            for c in colls:
                for m in r.smembers(f"{c}:funcid:{h}"):
                    members.add(m.decode() if isinstance(m, bytes) else m)
            buckets[h] = members

        synth = []  # (fid, md5, addr, [mate_fid, ...])
        for h, fids in fids_by_hash.items():
            bucket = buckets.get(h)
            if not bucket:
                continue
            for fid in fids:
                parts = fid.split(":")
                my_coll, my_md5 = parts[0], parts[2]
                mates = []
                for m in bucket:
                    mp = m.split(":")
                    # ponytail: cross-binary only (same-binary dupes aren't a file-diff signal)
                    if m == fid or len(mp) < 3 or mp[2] == my_md5:
                        continue
                    if only_cross_collection and mp[0] == my_coll:
                        continue
                    mates.append(m)
                if mates:
                    synth.append((fid, my_md5, parts[3], mates))

        if not synth:
            return

        # Feature counts (unique features = zcard of the tf vector) for the sim_doc
        all_fids = set()
        for fid, md5, addr, mates in synth:
            all_fids.add(fid)
            all_fids.update(mates)
        fid_list = list(all_fids)
        pipe = r.pipeline(transaction=False)
        for f in fid_list:
            pipe.zcard(f"{f}:vec:tf")
        counts = {f: float(c or 0) for f, c in zip(fid_list, pipe.execute())}

        discovery = []
        for fid, md5, addr, mates in synth:
            items = [
                {"id": m, "score": 1.0, "c_total": counts.get(m, 0.0)} for m in mates
            ]
            discovery.append((fid, md5, addr, counts.get(fid, 0.0), items))

        # index_depth="none": skip search-index propagation (save_similarity) — it's the
        # per-doc bottleneck and hash dupes flood it. involves:file + the sim doc (all
        # build_bin_sim reads) are written regardless. Exact tiny-func dupes needn't be searchable.
        self._persist_and_index_batch(
            collection,
            algo,
            discovery,
            pool_id=pool_id,
            min_features=0,
            index_depth="none",
        )

    def _persist_and_index_batch(
        self,
        collection,
        algo,
        discovery_results,
        pool_id=None,
        min_features=0,
        index_depth="full",
        skip_write=False,
    ):
        """Unified helper to persist similarity results and propagate metadata to search indexes."""
        r = self.r
        now = int(time.time() * 1000)
        total_written = 0

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

        # Pre-fetch metadata if indexing is enabled
        needs_func_meta = False
        needs_file_meta = False
        target_coll = f"global:pool:{pool_id}" if pool_id else collection

        if index_depth != "none":
            propagated = get_propagated_fields("sim")
            if index_depth == "minimal":
                needs_func_meta = False
                needs_file_meta = False
            else:
                needs_func_meta = len(propagated.get("func", [])) > 0
                needs_file_meta = (
                    len([f for f, t in propagated.get("file", []) if f != "file_md5"])
                    > 0
                )

            if not hasattr(self, "_func_meta_cache"):
                self._func_meta_cache = {}
            if not hasattr(self, "_file_meta_cache"):
                self._file_meta_cache = {}

            if needs_func_meta or needs_file_meta:
                func_ids_needed = set()
                file_ids_needed = set()

                for fid, t_md5, t_addr, t_total, candidates in discovery_results:
                    if t_total < min_features:
                        continue
                    if fid not in self._func_meta_cache:
                        func_ids_needed.add(fid)
                    if needs_file_meta:
                        t_md5_key = f"{extract_coll(fid)}:file:{t_md5}"
                        if t_md5_key not in self._file_meta_cache:
                            file_ids_needed.add(t_md5_key)

                    for item in candidates:
                        if item["c_total"] < min_features:
                            continue
                        if item["id"] not in self._func_meta_cache:
                            func_ids_needed.add(item["id"])
                        if needs_file_meta:
                            md5 = (
                                item["id"].split(":")[2]
                                if ":" in item["id"]
                                else "unknown"
                            )
                            file_key = f"{extract_coll(item['id'])}:file:{md5}"
                            if file_key not in self._file_meta_cache:
                                file_ids_needed.add(file_key)

                if func_ids_needed:
                    func_ids_list = list(func_ids_needed)
                    raw_func_metas = r.json().mget(
                        [f"{fid}:meta" for fid in func_ids_list], "$"
                    )
                    for fid, raw in zip(func_ids_list, raw_func_metas):
                        if raw:
                            m = raw[0] if isinstance(raw, list) else raw
                            if isinstance(m, str):
                                m = json.loads(m)
                            self._func_meta_cache[fid] = m
                        else:
                            self._func_meta_cache[fid] = None

                if file_ids_needed:
                    file_ids_list = list(file_ids_needed)
                    raw_file_metas = r.json().mget(
                        [f"{fid}:meta" for fid in file_ids_list], "$"
                    )
                    for fid, raw in zip(file_ids_list, raw_file_metas):
                        if raw:
                            m = raw[0] if isinstance(raw, list) else raw
                            if isinstance(m, str):
                                m = json.loads(m)
                            self._file_meta_cache[fid] = m
                        else:
                            self._file_meta_cache[fid] = None

        if skip_write:
            return 0

        # Execute persistence and indexing in chunked batches
        # ponytail: batch size set to 2000 to balance serialization latency and worker contention
        batch_size = 200
        persist_pipe = r.pipeline(transaction=False)
        sim_count = 0

        for fid, t_md5, t_addr, t_total, candidates in discovery_results:
            # Skip if target function has too few features
            if t_total < min_features:
                continue

            for item in candidates:
                # Skip if candidate function has too few features
                if item["c_total"] < min_features:
                    continue

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
                    involves_func_prefix = f"global:pool:{pool_id}:sim:involves:func:"
                    involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"
                    min_feat_key = f"global:pool:{pool_id}:sim:min_features"
                    cross_binary_prefix = f"global:pool:{pool_id}:sim:is_cross_binary:"
                else:
                    # Collection Namespace
                    func_prefix = f"{collection}:func:"
                    clean_id_a = (
                        id_a[len(func_prefix) :]
                        if id_a.startswith(func_prefix)
                        else id_a
                    )
                    clean_id_b = (
                        id_b[len(func_prefix) :]
                        if id_b.startswith(func_prefix)
                        else id_b
                    )
                    sid = f"{collection}:sim:{algo}:{clean_id_a}::{clean_id_b}"
                    score_key = f"{collection}:sim:score:{algo}"
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

                persist_pipe.set(sid, json.dumps(sim_doc))
                persist_pipe.zadd(score_key, {sid: score_rounded})

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
                persist_pipe.zadd(
                    f"{cross_binary_prefix}{sim_doc['is_cross_binary']}", {sid: 0}
                )

                # 2. Metadata Propagation (Search Index)
                if index_depth != "none":
                    sim_doc_for_idx = {
                        "md5_1": md5_a,
                        "md5_2": md5_b,
                        "tags": [],
                        "user_tags": [],
                    }

                    save_similarity(
                        persist_pipe,
                        target_coll,
                        sid,
                        sim_doc_for_idx,
                        func_meta1=self._func_meta_cache.get(id_a),
                        func_meta2=self._func_meta_cache.get(id_b),
                        file_meta1=self._file_meta_cache.get(f"{coll_a}:file:{md5_a}"),
                        file_meta2=self._file_meta_cache.get(f"{coll_b}:file:{md5_b}"),
                        index_depth=index_depth,
                        seen=getattr(self, "_sim_registry_seen", None),
                    )

                sim_count += 1
                total_written += 1
                if sim_count >= batch_size:
                    persist_pipe.execute()
                    persist_pipe = r.pipeline(transaction=False)
                    sim_count = 0

        if sim_count > 0:
            persist_pipe.execute()

        return total_written

    def build_function(
        self,
        collection,
        base_id,
        algo=None,
        top_k=None,
        min_score=None,
        min_features=None,
        sleep_time=0,
        index_depth="none",
    ):
        """
        Builds similarities for a single function against the collection.
        base_id: coll:function:md5:addr
        """
        # On-demand single target: drop any caches left from a prior build so we
        # never read a posting list/norm that ingestion has since changed.
        self._reset_read_caches()
        from bsimvis.app.services.config_service import config_service

        if algo is None:
            algo = config_service.get("similarity.algo", "unweighted_cosine")
        if top_k is None:
            top_k = config_service.get("similarity.top_k", 1000)
        if min_score is None:
            min_score = config_service.get("similarity.min_score", 0.9)
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

            candidates_raw = self._discover(lua_args)

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
            self._persist_and_index_batch(
                collection,
                algo,
                discovery_results,
                min_features=min_features,
                index_depth=index_depth,
            )

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

    def get_pair_score(self, id1, id2, algo="unweighted_cosine", collection=None):
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

            # 1. Use the provided collection parameter if specified
            if collection:
                score = self.check_cache(id1, id2, collection, algo)
                if score is not None:
                    return score

            # 2. Same collection: Check Cache first
            if coll1 == coll2:
                score = self.check_cache(id1, id2, coll1, algo)
                if score is not None:
                    return score

            # 3. Check cache under pool collection if IDs differ but pool is active
            if id1.startswith("global:pool:"):
                pool_prefix = ":".join(parts1[:3])
                score = self.check_cache(id1, id2, pool_prefix, algo)
                if score is not None:
                    return score

            # 4. Fallback: Direct Calculation (No on-demand baking)
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
        is_pool = collection.startswith("global:pool:") or collection.startswith(
            "pool:"
        )

        # 1. Try directly with collection
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        zset_key = (
            f"{collection}:sim:score" if is_pool else f"{collection}:sim:score:{algo}"
        )
        score = self.r.zscore(zset_key, sid)
        if score is not None:
            return float(score)

        # 2. If collection is sub-collection of pool (e.g. global:pool:UUID:col:coll), try base pool namespace
        if collection.startswith("global:pool:") and ":col:" in collection:
            base_pool = collection.split(":col:")[0]
            sid_base = self._canonicalize_sid(base_pool, id1, id2, algo)
            zset_key_base = f"{base_pool}:sim:score"
            score = self.r.zscore(zset_key_base, sid_base)
            if score is not None:
                return float(score)

        return None

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
            name_raw = r.get(meta_key)
            name = "N/A"
            if name_raw:
                val = name_raw.decode() if isinstance(name_raw, bytes) else name_raw
                try:
                    meta_dict = json.loads(val)
                    name = meta_dict.get("name", "N/A")
                except:
                    pass

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

            meta_raw = r.get(f_key)
            meta = {}
            if meta_raw:
                val = meta_raw.decode() if isinstance(meta_raw, bytes) else meta_raw
                try:
                    meta = json.loads(val)
                except:
                    pass
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

        is_pool = collection.startswith("global:pool:") or collection.startswith(
            "pool:"
        )

        if c1 > c2:
            if is_pool:
                return f"{collection}:sim:{c1}::{c2}"
            return f"{collection}:sim:{algo}:{c1}::{c2}"
        else:
            if is_pool:
                return f"{collection}:sim:{c2}::{c1}"
            return f"{collection}:sim:{algo}:{c2}::{c1}"

    def tag_similarity(
        self, collection: str, id1: str, id2: str, algo: str, tag: str
    ) -> bool:
        """Adds a user tag to a similarity pair (delegates to TagService)."""
        sid = self._canonicalize_sid(collection, id1, id2, algo)
        return self.tag_service.add_user_tag(collection, "similarity", sid, tag)

    def _ensure_tag_metadata(self, collection: str, tag: str):
        """Ensures a tag has a metadata row in the global index.

        No colour, for the reason `tag_service._ensure_tag_metadata` gives: a
        colour is derived from the tag id, and a stored one wins over it, so
        rolling a palette entry here silently disabled the rule.
        """
        r = self.r
        meta_key = f"{collection}:tags_metadata"
        if not r.hexists(meta_key, tag):
            import json

            r.hset(meta_key, tag, json.dumps({"priority": 0}))

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
        index_depth="none",
        skip_write=False,
    ):
        """
        Orchestrates cross-collection similarity discovery for a pool.
        """
        self._reset_read_caches()
        self._func_meta_cache = {}
        self._file_meta_cache = {}
        self._sim_registry_seen = set()
        from bsimvis.app.services.pool_service import pool_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        collections = pool.get("collections", [])

        # New structured config handling. Fall back to the same config defaults the
        # collection path uses, so an unset pool param == the collection default.
        from bsimvis.app.services.config_service import config_service

        only_cross_collection = pool.get("only_cross_collection", False)
        func_sim_params = pool.get("func_sim_params", {})

        algo = func_sim_params.get(
            "algo", config_service.get("similarity.algo", "unweighted_cosine")
        )
        top_k = int(
            func_sim_params.get("top_k", config_service.get("similarity.top_k", 1000))
        )
        min_score = float(
            func_sim_params.get(
                "min_score", config_service.get("similarity.min_score", 0.9)
            )
        )
        min_features = int(
            func_sim_params.get(
                "min_features", config_service.get("similarity.min_features", 10)
            )
        )

        r = self.r

        # 1. Collect all functions from all collections
        all_function_ids = []
        for coll in collections:
            all_function_ids.extend(
                [
                    f.decode() if isinstance(f, bytes) else f
                    for f in r.smembers(f"{coll}:all_functions")
                ]
            )

        total = len(all_function_ids)
        if total == 0:
            logging.warning(f"No functions found in collections {collections}")
            return True

        logging.info(f"[*] Building pool {pool_id} for {total} functions...")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Building pool {pool_id} for {total} functions..."
            )

        start_time = time.time()
        # Count total functions in all pool collections to size chunks dynamically
        chunk_size = max(1, min(100, int(100000 / max(1, total))))
        total_sims = 0
        last_t, last_done, last_sims = start_time, 0, 0
        pool_small_fids = (
            []
        )  # below min_features -> exact FunctionID-hash match instead
        for i in range(0, total, chunk_size):
            chunk = all_function_ids[i : i + chunk_size]

            # Update Progress
            if job_service and job_id:
                now = time.time()
                elapsed = now - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                sim_speed = total_sims / elapsed if elapsed > 0 else 0
                d_t = now - last_t
                cur_speed = (done - last_done) / d_t if d_t > 0 else 0
                cur_sim_speed = (total_sims - last_sims) / d_t if d_t > 0 else 0
                last_t, last_done, last_sims = now, done, total_sims
                job_service.update_progress(
                    job_id,
                    int(done / total * 100),
                    f"Building pool: {done}/{total} functions ({speed:.1f} fn/s, {sim_speed:.1f} sim/s, cur {cur_speed:.1f} fn/s, {cur_sim_speed:.1f} sim/s)",
                )

            # Bulk fetch feature vectors for the chunk
            vec_pipe = r.pipeline(transaction=False)
            for fid in chunk:
                vec_pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            chunk_features = vec_pipe.execute()

            # Use a pipeline to batch any LSH setup writes for this chunk
            lsh_pipe = r.pipeline(transaction=False)
            has_lsh_writes = False
            targets_with_lua = []

            for idx, fid in enumerate(chunk):
                features_raw = chunk_features[idx]
                if not features_raw or len(features_raw) < min_features:
                    # Small: skip BSim, match by exact FunctionID hash after the loop
                    pool_small_fids.append(fid)
                    continue

                target_feat_total = 0
                target_feat_norm_sq = 0
                lua_features_args = []
                for f_hash, f_tf_raw in features_raw:
                    f_tf = float(f_tf_raw)
                    target_feat_total += f_tf
                    target_feat_norm_sq += f_tf * f_tf
                    lua_features_args.extend(
                        [
                            (
                                f_hash.decode()
                                if isinstance(f_hash, bytes)
                                else str(f_hash)
                            ),
                            str(f_tf),
                        ]
                    )

                target_feat_norm = math.sqrt(target_feat_norm_sq)

                buckets = None
                if algo == "minhash_lsh":
                    num_bands = 30
                    buckets = self._compute_lsh_buckets(
                        features_raw, num_bands=num_bands
                    )
                    parts_fid = fid.split(":")
                    if parts_fid:
                        src_coll = parts_fid[0]
                        for band, b_hash in buckets:
                            bucket_key = f"{src_coll}:lsh:bucket:{band}:{b_hash}"
                            lsh_pipe.sadd(bucket_key, fid)
                            lsh_pipe.set(f"{fid}:lsh:bucket_key:{band}", bucket_key)
                        has_lsh_writes = True

                for search_coll in collections:
                    # ONLY_CROSS_COLLECTION FILTER: Skip Lua if query FID is from search_coll
                    if only_cross_collection and fid.startswith(f"{search_coll}:"):
                        continue

                    if algo == "minhash_lsh":
                        num_bands = 30
                        hashes = [b_hash for band, b_hash in buckets]
                        lua_args = (
                            [
                                fid,
                                search_coll,
                                algo,
                                min_score,
                                target_feat_total,
                                target_feat_norm,
                                top_k,
                                min_features,
                                num_bands,
                            ]
                            + hashes
                            + lua_features_args
                        )
                    else:
                        lua_args = [
                            fid,
                            search_coll,
                            algo,
                            min_score,
                            target_feat_total,
                            target_feat_norm,
                            top_k,
                            min_features,
                        ] + lua_features_args

                    targets_with_lua.append((fid, target_feat_total, lua_args))

            if has_lsh_writes:
                lsh_pipe.execute()

            if not targets_with_lua:
                continue

            # Python discovery per (function × collection) — no EVAL lock.
            raw_results = [
                self._discover(lua_args) for fid, _, lua_args in targets_with_lua
            ]

            # Parse pipeline results back into per-function groups
            # pipeline.execute() returns results in same order as commands were queued
            candidates_by_fid = []
            for (fid, t_total, lua_args), raw in zip(targets_with_lua, raw_results):
                if not raw:
                    continue
                fid_candidates = []
                for k in range(0, len(raw), 3):
                    fid_candidates.append(
                        {
                            "id": (
                                raw[k].decode() if isinstance(raw[k], bytes) else raw[k]
                            ),
                            "score": float(raw[k + 1]),
                            "c_total": float(raw[k + 2]),
                        }
                    )
                candidates_by_fid.append((fid, t_total, fid_candidates))

            discovery_results = []
            for fid, t_total, candidates in candidates_by_fid:
                if candidates:
                    # Sort and limit combined candidates (from all collections)
                    candidates.sort(key=lambda x: x["score"], reverse=True)
                    # 
                    parts = fid.split(":")
                    md5 = parts[2] if len(parts) >= 3 else "unknown"
                    discovery_results.append((fid, md5, "", t_total, candidates))

            if discovery_results:
                written = self._persist_and_index_batch(
                    "",
                    algo,
                    discovery_results,
                    pool_id=pool_id,
                    min_features=min_features,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
                total_sims += written or 0

            # Clear caches to prevent unbounded memory growth and GC stalls
            self._func_meta_cache.clear()
            self._file_meta_cache.clear()

        # Cross every member collection's FunctionID-hash buckets for the small funcs
        if pool_small_fids and not skip_write:
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] FunctionID-hash matching {len(pool_small_fids)} small functions (<{min_features} features)...",
                )
            self._hash_match_small(
                "",
                algo,
                pool_small_fids,
                index_depth,
                search_collections=collections,
                pool_id=pool_id,
                only_cross_collection=only_cross_collection,
            )

        # 2. Update Sync Snapshots and Indexes
        pool_service.update_sync_snapshots(pool_id)
        pool_service.build_pool_indexes(pool_id)

        # Automatically trigger pool clustering (which includes function clustering, bin_sim, and bin_clustering)
        from bsimvis.app.services.cluster_service import cluster_service

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Triggering Pool Clustering automatically at the end of build...",
            )
        cluster_service.run_pool_clustering(
            pool_id, job_service=job_service, job_id=job_id
        )

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Pool build {pool_id} completed in {time.time() - start_time:.2f}s",
            )

        return True

    def build_pool_file(
        self,
        pool_id,
        file_md5,
        job_service=None,
        job_id=None,
        index_depth="none",
        skip_write=False,
    ):
        """
        Orchestrates cross-collection similarity discovery for a single file in the pool.
        """
        self._reset_read_caches()
        self._func_meta_cache = {}
        self._file_meta_cache = {}
        self._sim_registry_seen = set()
        from bsimvis.app.services.pool_service import pool_service

        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        collections = pool.get("collections", [])

        # New structured config handling. Fall back to the same config defaults the
        # collection path uses, so an unset pool param == the collection default.
        from bsimvis.app.services.config_service import config_service

        only_cross_collection = pool.get("only_cross_collection", False)
        func_sim_params = pool.get("func_sim_params", {})

        algo = func_sim_params.get(
            "algo", config_service.get("similarity.algo", "unweighted_cosine")
        )
        top_k = int(
            func_sim_params.get("top_k", config_service.get("similarity.top_k", 1000))
        )
        min_score = float(
            func_sim_params.get(
                "min_score", config_service.get("similarity.min_score", 0.9)
            )
        )
        min_features = int(
            func_sim_params.get(
                "min_features", config_service.get("similarity.min_features", 10)
            )
        )

        r = self.r

        # 1. Collect all functions for this file md5 from all member collections
        all_function_ids = []
        for coll in collections:
            funcs_key = f"{coll}:idx:file:functions:{file_md5}"
            all_function_ids.extend(
                [
                    f.decode() if isinstance(f, bytes) else f
                    for f in r.smembers(funcs_key)
                ]
            )

        total = len(all_function_ids)
        if total == 0:
            logging.warning(
                f"No functions found for file {file_md5} in collections {collections}"
            )
            return True

        logging.info(
            f"[*] Building pool similarities for file {file_md5} ({total} functions)..."
        )
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Building pool similarities for file {file_md5} ({total} functions)...",
            )

        start_time = time.time()
        # Count total functions in all pool collections to size chunks dynamically
        db_func_count = sum(r.scard(f"{coll}:all_functions") for coll in collections)
        chunk_size = max(1, min(5, int(100000 / max(1, db_func_count))))
        total_sims = 0
        last_t, last_done, last_sims = start_time, 0, 0
        pool_small_fids = (
            []
        )  # below min_features -> exact FunctionID-hash match instead
        for i in range(0, total, chunk_size):
            chunk = all_function_ids[i : i + chunk_size]

            # Update Progress
            if job_service and job_id:
                now = time.time()
                elapsed = now - start_time
                done = i
                speed = done / elapsed if elapsed > 0 else 0
                sim_speed = total_sims / elapsed if elapsed > 0 else 0
                d_t = now - last_t
                cur_speed = (done - last_done) / d_t if d_t > 0 else 0
                cur_sim_speed = (total_sims - last_sims) / d_t if d_t > 0 else 0
                last_t, last_done, last_sims = now, done, total_sims
                job_service.update_progress(
                    job_id,
                    int(done / total * 100),
                    f"Building file {file_md5} pool sim: {done}/{total} functions ({speed:.1f} fn/s, {sim_speed:.1f} sim/s, cur {cur_speed:.1f} fn/s, {cur_sim_speed:.1f} sim/s)",
                )

            # Use a pipeline to batch any LSH setup writes for this chunk
            lsh_pipe = r.pipeline(transaction=False)
            has_lsh_writes = False
            targets_with_lua = []

            for fid in chunk:
                vec_key = f"{fid}:vec:tf"
                features_raw = r.zrange(vec_key, 0, -1, withscores=True)
                if not features_raw or len(features_raw) < min_features:
                    # Small: skip BSim, match by exact FunctionID hash after the loop
                    pool_small_fids.append(fid)
                    continue

                target_feat_total = 0
                target_feat_norm_sq = 0
                lua_features_args = []
                for f_hash, f_tf_raw in features_raw:
                    f_tf = float(f_tf_raw)
                    target_feat_total += f_tf
                    target_feat_norm_sq += f_tf * f_tf
                    lua_features_args.extend(
                        [
                            (
                                f_hash.decode()
                                if isinstance(f_hash, bytes)
                                else str(f_hash)
                            ),
                            str(f_tf),
                        ]
                    )

                target_feat_norm = math.sqrt(target_feat_norm_sq)

                buckets = None
                if algo == "minhash_lsh":
                    num_bands = 30
                    buckets = self._compute_lsh_buckets(
                        features_raw, num_bands=num_bands
                    )
                    parts_fid = fid.split(":")
                    if parts_fid:
                        src_coll = parts_fid[0]
                        for band, b_hash in buckets:
                            bucket_key = f"{src_coll}:lsh:bucket:{band}:{b_hash}"
                            lsh_pipe.sadd(bucket_key, fid)
                            lsh_pipe.set(f"{fid}:lsh:bucket_key:{band}", bucket_key)
                        has_lsh_writes = True

                for search_coll in collections:
                    # ONLY_CROSS_COLLECTION FILTER: Skip Lua if query FID is from search_coll
                    if only_cross_collection and fid.startswith(f"{search_coll}:"):
                        continue

                    if algo == "minhash_lsh":
                        num_bands = 30
                        hashes = [b_hash for band, b_hash in buckets]
                        lua_args = (
                            [
                                fid,
                                search_coll,
                                algo,
                                min_score,
                                target_feat_total,
                                target_feat_norm,
                                top_k,
                                min_features,
                                num_bands,
                            ]
                            + hashes
                            + lua_features_args
                        )
                    else:
                        lua_args = [
                            fid,
                            search_coll,
                            algo,
                            min_score,
                            target_feat_total,
                            target_feat_norm,
                            top_k,
                            min_features,
                        ] + lua_features_args

                    targets_with_lua.append((fid, target_feat_total, lua_args))

            if has_lsh_writes:
                lsh_pipe.execute()

            if not targets_with_lua:
                continue

            # Python discovery per (function × collection) — no EVAL lock.
            raw_results = [
                self._discover(lua_args) for fid, _, lua_args in targets_with_lua
            ]

            # Parse pipeline results back into per-function groups
            candidates_by_fid = []
            for (fid, t_total, lua_args), raw in zip(targets_with_lua, raw_results):
                if not raw:
                    continue
                fid_candidates = []
                for k in range(0, len(raw), 3):
                    fid_candidates.append(
                        {
                            "id": (
                                raw[k].decode() if isinstance(raw[k], bytes) else raw[k]
                            ),
                            "score": float(raw[k + 1]),
                            "c_total": float(raw[k + 2]),
                        }
                    )
                candidates_by_fid.append((fid, t_total, fid_candidates))

            discovery_results = []
            for fid, t_total, candidates in candidates_by_fid:
                if candidates:
                    candidates.sort(key=lambda x: x["score"], reverse=True)
                    # 
                    parts = fid.split(":")
                    md5 = parts[2] if len(parts) >= 3 else "unknown"
                    discovery_results.append((fid, md5, "", t_total, candidates))

            if discovery_results:
                written = self._persist_and_index_batch(
                    "",
                    algo,
                    discovery_results,
                    pool_id=pool_id,
                    min_features=min_features,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
                total_sims += written or 0

            # Clear caches to prevent unbounded memory growth and GC stalls
            self._func_meta_cache.clear()
            self._file_meta_cache.clear()

        # Cross every member collection's FunctionID-hash buckets for this file's small funcs
        if pool_small_fids and not skip_write:
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] FunctionID-hash matching {len(pool_small_fids)} small functions (<{min_features} features)...",
                )
            self._hash_match_small(
                "",
                algo,
                pool_small_fids,
                index_depth,
                search_collections=collections,
                pool_id=pool_id,
                only_cross_collection=only_cross_collection,
            )

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"File {file_md5} pool sim completed in {time.time() - start_time:.2f}s",
            )

        return True

    def build_pool_bin_sim(self, pool_id, job_service=None, job_id=None):
        """
        Orchestrates cross-collection binary similarity calculations for a pool.
        """
        from bsimvis.app.services.pool_service import pool_service
        import math
        import time
        from collections import defaultdict
        from bsimvis.app.services.bin_sim_tags import (
            AxisSplit,
            EMPTY_SUMMARIES,
            merge_tag_fields,
            load_tag_meta,
            read_tags_rev,
        )

        pool = pool_service.get_pool(pool_id)
        if not pool:
            logging.error(f"Pool {pool_id} not found")
            return False

        # New structured config handling
        file_sim_params = pool.get("file_sim_params", {})
        if not file_sim_params.get("enabled", True):
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] File similarity disabled for pool {pool_id}, skipping build_pool_bin_sim",
                )
            return True

        collections = pool.get("collections", [])
        algo = pool.get("algo", "unweighted_cosine")
        cluster_params = pool.get("cluster_params", {})

        from bsimvis.app.services.config_service import config_service

        min_cohesion = file_sim_params.get("min_cohesion")
        if min_cohesion is None:
            min_cohesion = cluster_params.get("min_cohesion")
        if min_cohesion is None:
            min_cohesion = config_service.get("clustering.min_cohesion", 0.5)
        min_cohesion = float(min_cohesion)

        r = self.r
        start_time = time.time()

        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Starting Pool Binary Similarity Build for pool {pool_id}"
            )

        # 1. Fetch function-level pool clusters and map function ID -> cluster UUID
        cluster_list_key = f"global:pool:{pool_id}:cluster:list"
        cluster_labels = [
            c.decode() if isinstance(c, bytes) else c
            for c in r.smembers(cluster_list_key)
        ]

        fid_to_cids = defaultdict(set)
        cluster_meta = {}
        if cluster_labels:
            pipe = r.pipeline(transaction=False)
            for label in cluster_labels:
                pipe.smembers(f"global:pool:{pool_id}:cluster:{algo}:{label}:members")
                pipe.get(f"global:pool:{pool_id}:cluster:{algo}:{label}:meta")
            results = pipe.execute()
            for idx, label in enumerate(cluster_labels):
                members = results[idx * 2] or []
                meta_raw = results[idx * 2 + 1]
                meta = {}
                if meta_raw:
                    val = meta_raw.decode() if isinstance(meta_raw, bytes) else meta_raw
                    try:
                        meta = json.loads(val)
                    except Exception:
                        pass
                c_uuid = meta.get("cluster_uuid", str(label))
                cluster_meta[c_uuid] = meta
                for m in members:
                    fid = m.decode() if isinstance(m, bytes) else m
                    fid_to_cids[fid].add(c_uuid)

        # 2. Fetch all binaries across all collections in the pool
        binaries = []  # List of tuples (collection, md5)
        binary_func_counts = {}
        binary_fids = {}
        binary_cluster_maps = {}
        cluster_binary_count_job = defaultdict(int)

        for coll in collections:
            all_files_key = f"{coll}:all_files"
            file_keys = [
                d.decode() if isinstance(d, bytes) else str(d)
                for d in r.smembers(all_files_key)
            ]
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
                (
                    fid.decode().replace(":meta", "")
                    if isinstance(fid, bytes)
                    else str(fid).replace(":meta", "")
                )
                for fid in raw_ids
            ]

            b_cluster_map = defaultdict(set)
            binary_fids[(coll, md5)] = set(fids)
            binary_func_counts[(coll, md5)] = len(fids)

            for fid in fids:
                full_fid = (
                    fid if fid.startswith(f"{coll}:func:") else f"{coll}:func:{fid}"
                )
                if full_fid in fid_to_cids:
                    for cid in fid_to_cids[full_fid]:
                        b_cluster_map[cid].add(full_fid)

            binary_cluster_maps[(coll, md5)] = b_cluster_map
            for cid in b_cluster_map.keys():
                cluster_binary_count_job[cid] += 1

        def get_col_rarity(cid):
            global_count = cluster_meta.get(cid, {}).get(
                "unique_files_count", cluster_binary_count_job.get(cid, 0)
            )
            return 1.0 / math.log(1 + global_count + 1)

        def pick_cluster(full_a, full_b):
            """Best function cluster for a matched pair (mirrors bin_sim_service):
            prefer a cluster both share, else any either belongs to; tightest cohesion wins.
            """
            la = fid_to_cids.get(full_a, set())
            lb = fid_to_cids.get(full_b, set())
            shared = la & lb
            candidates = shared if shared else (la | lb)
            best = None
            best_coh = -1.0
            for cid in candidates:
                meta = cluster_meta.get(cid)
                if not meta:
                    continue
                coh = float(meta.get("cohesion_score", 0.0))
                if coh > best_coh:
                    best_coh = coh
                    best = meta
            return best

        # Pre-fetch file metadata
        file_meta_cache = {}
        pipe_meta = r.pipeline(transaction=False)
        for coll, md5 in binaries:
            pipe_meta.get(f"{coll}:file:{md5}:meta")
        meta_results = pipe_meta.execute()
        for (coll, md5), res in zip(binaries, meta_results):
            if res:
                m = res.decode() if isinstance(res, bytes) else res
                if isinstance(m, str):
                    try:
                        m = json.loads(m)
                    except Exception:
                        pass
                file_meta_cache[(coll, md5)] = m if isinstance(m, dict) else {}
            else:
                file_meta_cache[(coll, md5)] = {}

        # Load function metadata (for bsim_features_count of functions)
        func_meta_cache = {}
        all_unique_fids = set()
        for fids_set in binary_fids.values():
            all_unique_fids.update(fids_set)

        if all_unique_fids:
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] Loading metadata for {len(all_unique_fids)} functions...",
                )
            fids_list = list(all_unique_fids)
            pipe = r.pipeline(transaction=False)
            for fid in fids_list:
                pipe.get(f"{fid}:meta")
            meta_results = pipe.execute()
            for fid, res in zip(fids_list, meta_results):
                if res:
                    m = res.decode() if isinstance(res, bytes) else res
                    if isinstance(m, str):
                        try:
                            m = json.loads(m)
                        except ValueError:
                            pass
                    func_meta_cache[fid] = m if isinstance(m, dict) else {}

        # Normalize each function's tags once here, not once per matched edge.
        fid_tags = {}
        for fid, m in func_meta_cache.items():
            tags = merge_tag_fields(m)
            if tags:
                fid_tags[fid] = tags

        tag_meta_cache = load_tag_meta(r, f"global:pool:{pool_id}") if fid_tags else {}
        tags_rev = read_tags_rev(r, f"global:pool:{pool_id}")

        # 3. Generate Pairs (all combinations cross-collection/in pool)
        pairs = []
        for i in range(len(binaries)):
            for j in range(i + 1, len(binaries)):
                b1, b2 = binaries[i], binaries[j]
                if b1 < b2:
                    pairs.append((b1, b2))
                else:
                    pairs.append((b2, b1))

        def log(msg):
            if job_service and job_id:
                job_service.add_log(job_id, msg)

        # 4. Process Pairs (Direct Similarity Matching with Bipartite Greedy Selection)
        persist_pipe = r.pipeline(transaction=False)
        now = int(time.time() * 1000)

        involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"

        log(f"[*] {num_binaries} binaries -> {len(pairs)} pairs to compare")

        # (coll, md5.lower()) -> canonical binary tuple. Pools are multi-collection and
        # the SAME md5 can appear in two collections, so partner MUST be resolved by
        # (collection, md5), never md5 alone. Pool sim docs carry both endpoints
        # (coll_1/md5_1, coll_2/md5_2), so read them straight from the doc.
        bin_by_norm = {(coll, md5.lower()): (coll, md5) for coll, md5 in binaries}

        # ponytail: stream one source-binary at a time instead of loading all ~26M
        # edges up front. For binary b_src we SMEMBERS+MGET only ITS sim docs (bounded
        # by a single binary), bucket them by partner, and yield each pair (b_src,
        # b_par) with b_par > b_src so every pair is emitted exactly once (at its lower
        # binary). Peak RAM = one binary's docs, not the whole pool. Cost: each doc is
        # read ~twice (once per endpoint) -- the RAM/IO trade that keeps big pools off
        # swap. Edges are pre-oriented fid_a -> b_src (== b1), so the consumer is O(1).
        def stream_pair_edges():
            for b_src in binaries:
                coll_i, md5_i = b_src
                member_sids = [
                    s.decode() if isinstance(s, bytes) else str(s)
                    for s in r.smembers(f"{involves_file_prefix}{coll_i}:{md5_i}")
                ]
                buckets = defaultdict(list)
                for k in range(0, len(member_sids), 10000):
                    chunk = member_sids[k : k + 10000]
                    for res in r.mget(chunk):
                        if not res:
                            continue
                        try:
                            doc = json.loads(
                                res.decode() if isinstance(res, bytes) else res
                            )
                        except Exception:
                            continue
                        f1, f2 = doc.get("id1"), doc.get("id2")
                        if not f1 or not f2:
                            continue
                        e1 = bin_by_norm.get(
                            (doc.get("coll_1"), (doc.get("md5_1") or "").lower())
                        )
                        e2 = bin_by_norm.get(
                            (doc.get("coll_2"), (doc.get("md5_2") or "").lower())
                        )
                        score = doc.get("score", 0.0)
                        if e1 == b_src:
                            b_par, edge = e2, (f1, f2, score)
                        elif e2 == b_src:
                            b_par, edge = e1, (f2, f1, score)
                        else:
                            continue
                        # only partners above b_src -> pair emitted once, and this
                        # also drops intra-binary docs (partner resolves to b_src).
                        if not b_par or b_par <= b_src:
                            continue
                        buckets[b_par].append(edge)
                for b_par, edges in buckets.items():
                    yield b_src, b_par, edges
                # buckets dropped here -> one binary's edges reclaimed before the next

        # ponytail: precompute per-function "unique" entry + weight once.
        # An unmatched function's diff entry + cluster scan depend only on (coll, fid),
        # so they're identical in every one of the ~N pairs the fn stays unmatched.
        # Building once turns O(pairs * funcs) dict/cluster work into O(funcs). Entries
        # are read-only downstream (json.dumps), so sharing the dict by reference is safe.
        unique_entry = {}
        unique_feat = {}
        for coll, md5 in binaries:
            for fid in binary_fids[(coll, md5)]:
                key = (coll, fid)
                if key in unique_entry:
                    continue
                f_features = float(
                    func_meta_cache.get(fid, {}).get("bsim_features_count", 1.0)
                )
                if f_features <= 0:
                    f_features = 1.0
                full_fid = (
                    fid if fid.startswith(f"{coll}:func:") else f"{coll}:func:{fid}"
                )
                # Slim doc (matches collection path); cluster tag derived at read.
                unique_entry[key] = {
                    "func_id": fid,
                    "avg_features": f_features,
                }
                unique_feat[key] = f_features

        loop_t = time.time()
        total_pairs = len(pairs)
        log(f"[*] Streaming {total_pairs} pairs (computed + saved incrementally)...")

        for pair_idx, (b1, b2, edges) in enumerate(stream_pair_edges()):
            if pair_idx and pair_idx % 2000 == 0:
                elapsed = time.time() - loop_t
                rate = pair_idx / elapsed if elapsed else 0
                eta = (total_pairs - pair_idx) / rate if rate else 0
                log(
                    f"[*] {pair_idx}/{total_pairs} pairs computed + saved "
                    f"({rate:.0f}/s, ETA {eta:.0f}s)"
                )
            coll_a, md5_a = b1
            coll_b, md5_b = b2

            # Edges streamed pre-oriented (fid_a -> b1) for this source binary; b1 < b2
            # holds (generator only emits partners above the source).
            # Sort edges by score descending (greedy match prioritizes best matches), using function IDs as deterministic tie-breakers
            edges.sort(key=lambda x: (-x[2], x[0], x[1]))

            assigned_a = set()
            assigned_b = set()
            diff_matched = []

            sum_weighted_cohesion = 0.0
            sum_weights = 0.0

            tag_split = AxisSplit(fid_tags, tag_meta_cache)

            for fid_a, fid_b, score in edges:
                if fid_a not in assigned_a and fid_b not in assigned_b:
                    assigned_a.add(fid_a)
                    assigned_b.add(fid_b)

                    f_features_a = float(
                        func_meta_cache.get(fid_a, {}).get("bsim_features_count", 1.0)
                    )
                    f_features_b = float(
                        func_meta_cache.get(fid_b, {}).get("bsim_features_count", 1.0)
                    )
                    f_features = max(f_features_a, f_features_b)

                    # Slim doc: persist only the stable triple (+ avg_features),
                    # matching the collection bin_sim path. Cluster tag / cohesion /
                    # rarity are derived live at read (get_bin_sim ->
                    # _enrich_diff_clusters, which handles pools) so a cluster
                    # rebuild can't leave them stale.
                    diff_matched.append(
                        {
                            "similarity": score,
                            "avg_features": f_features,
                            "func_a": fid_a,
                            "func_b": fid_b,
                        }
                    )

                    sum_weighted_cohesion += score * f_features
                    sum_weights += f_features

                    tag_split.add_match(fid_a, fid_b, score, f_features_a, f_features_b)

            # Unique/Unmatched functions logic
            all_funcs_a_total = binary_fids[b1]
            all_funcs_b_total = binary_fids[b2]

            unassigned_a = all_funcs_a_total - assigned_a
            unassigned_b = all_funcs_b_total - assigned_b

            unique_to_a = [unique_entry[(coll_a, fid)] for fid in sorted(unassigned_a)]
            sum_weights += sum(unique_feat[(coll_a, fid)] for fid in unassigned_a)
            for fid in unassigned_a:
                tag_split.add_unique(fid, unique_feat[(coll_a, fid)], "a")

            unique_to_b = [unique_entry[(coll_b, fid)] for fid in sorted(unassigned_b)]
            sum_weights += sum(unique_feat[(coll_b, fid)] for fid in unassigned_b)
            for fid in unassigned_b:
                tag_split.add_unique(fid, unique_feat[(coll_b, fid)], "b")

            total_weight_a = sum(unique_feat[(coll_a, f)] for f in all_funcs_a_total)
            total_weight_b = sum(unique_feat[(coll_b, f)] for f in all_funcs_b_total)

            tag_fields = (
                tag_split.summaries(total_weight_a, total_weight_b, tag_meta_cache)
                if fid_tags
                else dict(EMPTY_SUMMARIES)
            )

            # `algo` is a provenance tag, not a choice of file score: the score is
            # always the feature-weighted cohesion mean, as at collection level.
            final_score = (
                (sum_weighted_cohesion / sum_weights) if sum_weights > 0 else 0.0
            )

            # Persist pool bin_sim
            sid = f"global:pool:{pool_id}:bin_sim:{algo}:{coll_a}:{md5_a}::{coll_b}:{md5_b}"
            # Same field names as the collection bin_sim doc, plus the pool-only
            # endpoints (coll_a/coll_b), so readers need no translation layer.
            doc = {
                "type": "bin_sim",
                "pool_id": pool_id,
                "md5_a": md5_a,
                "md5_b": md5_b,
                "coll_a": coll_a,
                "coll_b": coll_b,
                "algo": algo,
                "functions_count_a": len(all_funcs_a_total),
                "functions_count_b": len(all_funcs_b_total),
                "score": final_score,
                "coverage_a": (
                    len(assigned_a) / len(all_funcs_a_total)
                    if all_funcs_a_total
                    else 0.0
                ),
                "coverage_b": (
                    len(assigned_b) / len(all_funcs_b_total)
                    if all_funcs_b_total
                    else 0.0
                ),
                "shared_clusters": len(diff_matched),
                "unique_clusters_a": len(unique_to_a),
                "unique_clusters_b": len(unique_to_b),
                "unclustered_a": len(unique_to_a),
                "unclustered_b": len(unique_to_b),
                "computed_at": now,
                "tags_rev": tags_rev,
                **tag_fields,
                "diff": {
                    "matched": diff_matched,
                    "unique_to_a": unique_to_a,
                    "unique_to_b": unique_to_b,
                    "unclustered_a": [],
                    "unclustered_b": [],
                },
            }

            persist_pipe.set(sid, json.dumps(doc))
            persist_pipe.zadd(
                f"global:pool:{pool_id}:bin_sim:score:{algo}", {sid: final_score}
            )
            persist_pipe.sadd(
                f"global:pool:{pool_id}:bin_sim:involves:{coll_a}:{md5_a}", sid
            )
            persist_pipe.sadd(
                f"global:pool:{pool_id}:bin_sim:involves:{coll_b}:{md5_b}", sid
            )
            persist_pipe.sadd(f"global:pool:{pool_id}:bin_sim:built:{algo}", sid)

            # ponytail: flush periodically so fat docs save over time and the client
            # buffer stays bounded, instead of holding all ~45k docs to one final
            # execute (re-held the whole pool in RAM + all-or-nothing on crash).
            if (pair_idx + 1) % 500 == 0:
                persist_pipe.execute()
                persist_pipe = r.pipeline(transaction=False)

        log(
            f"[*] All pairs computed + saved in {time.time() - loop_t:.1f}s; flushing final batch..."
        )
        persist_pipe.execute()
        log(
            f"Pool binary similarity build finished. Found {len(pairs)} comparisons in {time.time() - start_time:.1f}s."
        )

        self.r.hdel(f"global:pool:{pool_id}:meta", "total_file_similarities")
        self.reindex_pool_bin_sim(
            pool_id, algo=algo, job_service=job_service, job_id=job_id
        )
        return True

    def reindex_pool_bin_sim(
        self, pool_id, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """Build the same secondary indexes collections have for a pool's bin_sim
        pairs, so pool search can filter/sort/paginate server-side instead of
        materializing every pair. Idempotent; runs in-place on an already-built
        pool (no rebuild needed)."""
        from bsimvis.app.services.bin_sim_service import _index_bin_sim_pair

        r = self.r
        prefix = f"global:pool:{pool_id}"
        sids = [
            s.decode() if isinstance(s, bytes) else s
            for s in r.smembers(f"{prefix}:bin_sim:built:{algo}")
        ]
        if not sids:
            if job_service and job_id:
                job_service.add_log(job_id, "No pool bin_sim docs to reindex.")
                job_service.update_progress(job_id, 100)
            return True
        total = len(sids)
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Reindexing {total} pool bin_sim pairs for pool {pool_id}"
            )

        # Stream docs in chunks, keeping ONLY the scalar fields indexing needs.
        # Pool bin_sim docs carry the full per-pair diff blob (diff.matched +
        # unique lists) which can be hundreds of KB-MB each. GETting all of them
        # at once and retaining every parsed dict held ~2x the whole payload
        # resident (raw strings + dicts) -> swap thrash. Drop the fat fields as we
        # go so memory stays bounded to one chunk.
        # ponytail: 5000/chunk bounds transient RAM; lower it if docs are huge.
        docs = []
        md5set = set()
        for i in range(0, total, 5000):
            chunk = sids[i : i + 5000]
            for sid, raw in zip(chunk, r.mget(chunk)):
                if not raw:
                    continue
                d = json.loads(raw) if not isinstance(raw, dict) else raw
                if isinstance(d, str):
                    d = json.loads(d)
                c1, m1 = d.get("coll_a", ""), d.get("md5_a", "")
                c2, m2 = d.get("coll_b", ""), d.get("md5_b", "")
                slim = {
                    k: d.get(k)
                    for k in (
                        "coll_a",
                        "md5_a",
                        "coll_b",
                        "md5_b",
                        "score",
                        "coverage_a",
                        "coverage_b",
                        "shared_clusters",
                        "computed_at",
                    )
                }
                docs.append((sid, slim))
                md5set.add((c1, m1))
                md5set.add((c2, m2))

        # File meta (arch/tags/name) + function counts for every referenced binary.
        md5list = list(md5set)
        pipe = r.pipeline(transaction=False)
        for c, m in md5list:
            pipe.get(f"{c}:file:{m}:meta")
            pipe.scard(f"{c}:idx:file:functions:{m}")
        mres = pipe.execute()
        meta_map, func_map = {}, {}
        for i, (c, m) in enumerate(md5list):
            raw = mres[2 * i]
            mm = {}
            if raw:
                mm = json.loads(raw) if not isinstance(raw, dict) else raw
                if isinstance(mm, str):
                    mm = json.loads(mm)
            meta_map[(c, m)] = mm if isinstance(mm, dict) else {}
            func_map[(c, m)] = mres[2 * i + 1] or 0

        pipe = r.pipeline(transaction=False)
        for i, (sid, d) in enumerate(docs):
            c1, m1 = d.get("coll_a", ""), d.get("md5_a", "")
            c2, m2 = d.get("coll_b", ""), d.get("md5_b", "")
            # Pool docs already carry the collection field names; only the
            # denormalized file metadata has to be filled in here.
            norm = dict(
                d,
                algo=algo,
                architecture_a=meta_map.get((c1, m1), {}).get("language_id", ""),
                architecture_b=meta_map.get((c2, m2), {}).get("language_id", ""),
                functions_count_a=func_map.get((c1, m1), 0),
                functions_count_b=func_map.get((c2, m2), 0),
            )
            _index_bin_sim_pair(
                pipe, prefix, sid, norm, meta_map.get((c1, m1)), meta_map.get((c2, m2))
            )
            if (i + 1) % 200 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                if job_service and job_id:
                    job_service.update_progress(job_id, int((i + 1) / total * 100))
        pipe.execute()
        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Reindexed {total} pool bin_sim pairs."
            )
        return True

    def index_similarities(
        self,
        collection,
        algo="unweighted_cosine",
        pool_id=None,
        md5=None,
        batch_uuid=None,
        job_service=None,
        job_id=None,
    ):
        """
        Reads existing similarity JSON documents and builds/updates full secondary indexes.
        Used to perform deferred indexing after running build_sim with index_depth='none' or 'minimal'.
        Can run incrementally for a specific file md5 or batch_uuid.
        """
        r = self.r
        if pool_id:
            # score zset holds every sid; :sim:all removed as redundant
            all_key = f"global:pool:{pool_id}:sim:score"
            target_coll = f"global:pool:{pool_id}"
            involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"
            involves_func_prefix = f"global:pool:{pool_id}:sim:involves:func:"
        else:
            all_key = f"{collection}:sim:score:{algo}"
            target_coll = collection
            involves_file_prefix = f"{collection}:sim:involves:file:"
            involves_func_prefix = f"{collection}:sim:involves:func:"

        # Prep caches
        self._func_meta_cache = {}
        self._file_meta_cache = {}
        self._sim_registry_seen = set()

        if md5:
            if pool_id:
                keys = r.keys(f"{involves_file_prefix}*:{md5}")
                similarity_ids = []
                for k in keys:
                    similarity_ids.extend(
                        [
                            sid.decode() if isinstance(sid, bytes) else sid
                            for sid in r.smembers(k)
                        ]
                    )
            else:
                involves_key = f"{involves_file_prefix}{md5}"
                similarity_ids = [
                    sid.decode() if isinstance(sid, bytes) else sid
                    for sid in r.smembers(involves_key)
                ]

            self._index_similarity_ids_batch(
                r, similarity_ids, target_coll, pool_id, job_service, job_id
            )
        elif batch_uuid:
            batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
            func_ids = [
                fid.decode() if isinstance(fid, bytes) else fid
                for fid in r.smembers(batch_func_set)
            ]
            total_funcs = len(func_ids)
            if total_funcs == 0:
                if job_service and job_id:
                    job_service.update_progress(
                        job_id, 100, "No functions found in batch to index."
                    )
                return True

            similarity_ids_set = set()
            for fid in func_ids:
                clean_fid = fid
                if not pool_id:
                    func_prefix = f"{collection}:func:"
                    if fid.startswith(func_prefix):
                        clean_fid = fid[len(func_prefix) :]
                involves_key = f"{involves_func_prefix}{clean_fid}"
                for sid in r.smembers(involves_key):
                    similarity_ids_set.add(
                        sid.decode() if isinstance(sid, bytes) else sid
                    )

            similarity_ids = list(similarity_ids_set)
            self._index_similarity_ids_batch(
                r, similarity_ids, target_coll, pool_id, job_service, job_id
            )
        else:
            similarity_ids = [
                k.decode() if isinstance(k, bytes) else k
                for k in r.zrange(all_key, 0, -1)
            ]
            self._index_similarity_ids_batch(
                r, similarity_ids, target_coll, pool_id, job_service, job_id
            )

        return True

    def _index_similarity_ids_batch(
        self,
        r,
        similarity_ids,
        target_coll,
        pool_id,
        job_service,
        job_id,
    ):
        total = len(similarity_ids)
        if total == 0:
            return

        def extract_coll(fid):
            parts = fid.split(":")
            if len(parts) >= 1:
                return parts[0]
            return "unknown"

        batch_size = 200
        start_time = time.time()
        last_t, last_done = start_time, 0

        for i in range(0, total, batch_size):
            batch_ids = similarity_ids[i : i + batch_size]

            # 1. Fetch similarity documents
            sim_docs_raw = r.json().mget(batch_ids, "$")
            valid_docs = []
            func_ids_needed = set()
            file_ids_needed = set()

            for sid, doc_raw in zip(batch_ids, sim_docs_raw):
                if not doc_raw:
                    continue
                doc = doc_raw[0] if isinstance(doc_raw, list) else doc_raw
                if isinstance(doc, str):
                    doc = json.loads(doc)
                if not doc:
                    continue
                valid_docs.append((sid, doc))

                # Identify meta needed
                id_a, id_b = doc.get("id1"), doc.get("id2")
                md5_a, md5_b = doc.get("md5_1"), doc.get("md5_2")
                if id_a and id_a not in self._func_meta_cache:
                    func_ids_needed.add(id_a)
                if id_b and id_b not in self._func_meta_cache:
                    func_ids_needed.add(id_b)

                coll_a = extract_coll(id_a) if id_a else "unknown"
                coll_b = extract_coll(id_b) if id_b else "unknown"
                if md5_a:
                    file_key = f"{coll_a}:file:{md5_a}"
                    if file_key not in self._file_meta_cache:
                        file_ids_needed.add(file_key)
                if md5_b:
                    file_key = f"{coll_b}:file:{md5_b}"
                    if file_key not in self._file_meta_cache:
                        file_ids_needed.add(file_key)

            # 2. Fetch required function metadata
            if func_ids_needed:
                func_ids_list = list(func_ids_needed)
                raw_func_metas = r.json().mget(
                    [f"{fid}:meta" for fid in func_ids_list], "$"
                )
                for fid, raw in zip(func_ids_list, raw_func_metas):
                    if raw:
                        m = raw[0] if isinstance(raw, list) else raw
                        if isinstance(m, str):
                            m = json.loads(m)
                        self._func_meta_cache[fid] = m
                    else:
                        self._func_meta_cache[fid] = None

            # 3. Fetch required file metadata
            if file_ids_needed:
                file_ids_list = list(file_ids_needed)
                raw_file_metas = r.json().mget(
                    [f"{fid}:meta" for fid in file_ids_list], "$"
                )
                for fid, raw in zip(file_ids_list, raw_file_metas):
                    if raw:
                        m = raw[0] if isinstance(raw, list) else raw
                        if isinstance(m, str):
                            m = json.loads(m)
                        self._file_meta_cache[fid] = m
                    else:
                        self._file_meta_cache[fid] = None

            # 4. Write indexes
            idx_pipe = r.pipeline(transaction=False)
            for sid, doc in valid_docs:
                id_a, id_b = doc.get("id1"), doc.get("id2")
                md5_a, md5_b = doc.get("md5_1"), doc.get("md5_2")
                coll_a = extract_coll(id_a) if id_a else "unknown"
                coll_b = extract_coll(id_b) if id_b else "unknown"

                sim_doc_for_idx = {
                    "md5_1": md5_a,
                    "md5_2": md5_b,
                    "tags": doc.get("tags", []),
                    "user_tags": doc.get("user_tags", []),
                }

                save_similarity(
                    idx_pipe,
                    target_coll,
                    sid,
                    sim_doc_for_idx,
                    func_meta1=self._func_meta_cache.get(id_a),
                    func_meta2=self._func_meta_cache.get(id_b),
                    file_meta1=self._file_meta_cache.get(f"{coll_a}:file:{md5_a}"),
                    file_meta2=self._file_meta_cache.get(f"{coll_b}:file:{md5_b}"),
                    index_depth="full",
                    seen=getattr(self, "_sim_registry_seen", None),
                )
            idx_pipe.execute()

            # 5. Progress updates
            if job_service and job_id:
                now = time.time()
                elapsed = now - start_time
                done = min(i + batch_size, total)
                speed = done / elapsed if elapsed > 0 else 0
                d_t = now - last_t
                cur_speed = (done - last_done) / d_t if d_t > 0 else 0
                last_t, last_done = now, done
                pct = int(done / total * 100)
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Indexing similarities: {done}/{total} ({speed:.1f} sim/s, cur {cur_speed:.1f} sim/s)",
                )

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Completed indexing {total} similarities."
            )
        return True
