import math
import os
from collections import defaultdict
import logging
import json
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.milvus_service import milvus_service

# Field separator for the per-feature stats hash. Chars in type/op are a small
# enum-like set (DATA_FLOW, COPY_SIG, ...) so one byte outside the Latin-1
# alphabet is a safe delimiter.
PAIR_SEP = "\u241f"
OCC_FIELD = "occ\u241f"


def _pair_key(feat):
    """(type, op) pair-key as stored on the stats hash fields."""
    return (
        f"{str(feat.get('type', 'N/A'))}{PAIR_SEP}"
        f"{str(feat.get('pcode_op', 'N/A'))}"
    )


def _compact_rep(occ, func_id):
    """One representative occurrence for a (type, op) pair.

    Only what enrich needs to build context; the full occurrence is still in
    :feature:{fh}:meta under the function's field name. This is a derived
    cache enrich consumes when present."""
    return json.dumps(
        {
            "function_id": func_id,
            "line_idx": occ.get("line_idx", []),
            "seq": occ.get("seq"),
            "function_name": occ.get("function_name"),
            "pcode_op_full": occ.get("pcode_op_full"),
        },
        separators=(",", ":"),
    )


def _safe_json(raw):
    """Parse a JSON field value; returns None on any failure."""
    if raw is None:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return json.loads(raw)
    except Exception:
        return None


class _WriteBuffer:
    """Merges one window of index_functions writes, keyed by feature hash."""

    def __init__(self):
        self.norms = {}  # norm key -> value
        self.zadds = defaultdict(dict)  # f_hash -> {func_id: tf}
        self.incrs = defaultdict(float)  # f_hash -> summed tf
        self.metas = defaultdict(dict)  # f_hash -> {func_id: json}
        # (type, op) pair counts and reps. Argmax crosses functions/windows,
        # so we persist the full table and let enrich compute the winner.
        # Distinct pairs per feature is bounded (it is one pcode family), so
        # each :stats hash stays small (4-10 fields in real data).
        self.pair_counts = defaultdict(dict)  # f_hash -> {pair_key: int}
        self.pair_reps = defaultdict(dict)  # f_hash -> {pair_key: json-rep}
        self.indexed = []  # func_ids

    def flush(self, pipe, collection):
        if self.norms:
            pipe.mset(self.norms)
        for f_hash, members in self.zadds.items():
            pipe.zadd(f"{collection}:feature:{f_hash}:functions", members)
        for f_hash, amount in self.incrs.items():
            pipe.zincrby(f"{collection}:features:by_tf", amount, f_hash)
        for f_hash, fields in self.metas.items():
            pipe.hset(f"{collection}:feature:{f_hash}:meta", mapping=fields)
        # :stats — HINCRBY accumulates pair counts across the 100-func
        # windows (a window that saw 0 occurrences is absent and adds 0).
        # HSETNX keeps the first rep per pair: arbitrary but deterministic
        # within a feature, which matches the legacy path's non-determinism
        # on ties. One HINCRBY + one HSETNX per pair per window, in the same
        # pipeline that already writes the per-feature :meta/:by_tf.
        for f_hash in set(self.pair_counts) | set(self.pair_reps):
            stats_key = f"{collection}:feature:{f_hash}:stats"
            for k, v in self.pair_counts.get(f_hash, {}).items():
                pipe.hincrby(stats_key, k, v)
            for pair_key, rep in self.pair_reps.get(f_hash, {}).items():
                pipe.hsetnx(stats_key, f"{OCC_FIELD}{pair_key}", rep)
        if self.indexed:
            pipe.sadd(f"{collection}:indexed:functions", *self.indexed)
        self.__init__()


class FeatureService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    READ_BATCH = 100

    def _fetch_vec_batch(self, func_ids):
        """GET :vec:meta + ZRANGE :vec:tf for a batch of functions in one round-trip."""
        pipe = self.r.pipeline(transaction=False)
        for fid in func_ids:
            pipe.get(f"{fid}:vec:meta")
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
        res = pipe.execute()

        out = {}
        for idx, fid in enumerate(func_ids):
            raw_meta = res[idx * 2]
            if raw_meta:
                raw_meta = json.loads(raw_meta)
                if isinstance(raw_meta, list) and len(raw_meta) == 1:
                    raw_meta = raw_meta[0]
            out[fid] = (raw_meta, res[idx * 2 + 1])
        return out

    def index_functions(self, collection, function_ids, job_service=None, job_id=None):
        """
        Reverse feature indexing for a list of functions.
        Extracted and adapted from bsimvis_features.py.
        """
        total = len(function_ids)
        logging.info(f"[*] Indexing {total} functions for collection: {collection}")

        milvus_data = []
        milvus_chunk_size = 100
        indexed_features = set()

        pipe = self.r.pipeline(transaction=False)
        batch = {}  # func_id -> (raw_meta, tf_data), refilled every READ_BATCH funcs
        last_pct = -1
        # The write fan-out (one ZADD/ZINCRBY/HSET per function *per feature*) is
        # what actually dominates this job. Features repeat heavily across the
        # functions of one file, so merge a window's writes per feature hash:
        # ~40 commands per function collapse to ~1 per distinct feature.
        acc = _WriteBuffer()

        for i, func_id in enumerate(function_ids):
            # Update job progress if applicable. update_progress is expensive (it
            # re-aggregates the parent pipeline with one HGET per sibling task), so
            # only fire it when the whole percent actually moves.
            if job_service and job_id:
                pct = int((i + 1) / total * 100)
                if pct != last_pct or i == total - 1:
                    last_pct = pct
                    job_service.update_progress(
                        job_id, pct, f"Indexing features: {i+1}/{total}"
                    )

            # 1. Fetch metadata and vector data, one round-trip per READ_BATCH
            # functions instead of two per function.
            if func_id not in batch:
                batch = self._fetch_vec_batch(function_ids[i : i + self.READ_BATCH])

            raw_meta, new_tf_data = batch.pop(func_id)
            if not raw_meta or not new_tf_data:
                logging.warning(
                    f"  [!] Skipping {func_id}: Missing metadata or vector data."
                )
                continue

            # A. Recalculate L2 Norm
            sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
            acc.norms[f"{func_id}:vec:norm"] = math.sqrt(sum_sq)

            # B. Build Reverse Index (ZSETs)
            tf_dict = {
                h.decode() if isinstance(h, bytes) else str(h): float(score)
                for h, score in new_tf_data
            }

            for f_hash, new_tf in tf_dict.items():
                indexed_features.add(f_hash)
                # Update function mapping for this feature
                acc.zadds[f_hash][func_id] = new_tf
                # Update global TF counter for this feature
                acc.incrs[f_hash] += float(new_tf)

            # Store feature metadata as a JSON string in a HASH keyed by function_id
            for feat_item in raw_meta:
                f_hash = feat_item.get("hash")
                if not f_hash:
                    continue
                meta_entry = dict(feat_item)
                meta_entry["function_id"] = func_id
                # Convention: {coll}:feature:{hash}:meta -> HASH (field=func_id, value=JSON)
                acc.metas[f_hash][func_id] = json.dumps(meta_entry)
                # :stats tally — one HINCRBY per (type, op) pair per window.
                # HSETNX on the rep keeps the first occurrence (arbitrary but
                # deterministic; the legacy sampling path was already non-
                # deterministic on ties).
                pk = _pair_key(feat_item)
                pc = acc.pair_counts[f_hash]
                pc[pk] = pc.get(pk, 0) + 1
                if pk not in acc.pair_reps[f_hash]:
                    acc.pair_reps[f_hash][pk] = _compact_rep(feat_item, func_id)

            # Mark as indexed (Base ID)
            acc.indexed.append(func_id)

            # Execute pipeline in chunks to reduce memory footprint and network overhead
            if (i + 1) % 100 == 0:
                acc.flush(pipe, collection)
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)

            # Milvus Buffer
            if milvus_service.enabled:
                milvus_data.append({"fid": func_id, "tf_dict": tf_dict})
                if len(milvus_data) >= milvus_chunk_size:
                    for itype in ["SPARSE_INVERTED_INDEX"]:
                        milvus_service.upsert_functions(
                            collection, milvus_data, index_type=itype
                        )
                    milvus_data = []

        acc.flush(pipe, collection)
        pipe.execute()

        # Final Milvus Flush
        if milvus_service.enabled and milvus_data:
            for itype in ["SPARSE_INVERTED_INDEX"]:
                milvus_service.upsert_functions(
                    collection, milvus_data, index_type=itype
                )

        if indexed_features:
            self.r.sadd(
                f"{collection}:features:pending_enrichment", *list(indexed_features)
            )
            # Similarity posting-list caches are valid only for one completed
            # reverse-index generation. Increment after every write is committed.
            self.r.incr(f"{collection}:features:generation")

        return True

    def clear_features(self, collection, batch_uuid=None, file_md5=None):
        """Clears feature indexing data for a specific batch or file."""
        r = self.r

        # 1. Resolve function IDs to clear
        function_ids = []
        if batch_uuid:
            batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
            function_ids = list(r.smembers(batch_func_set))
        elif file_md5:
            # Look up base ids in the standard set
            raw_ids = list(r.smembers(f"{collection}:idx:file:functions:{file_md5}"))
            function_ids = [
                fid.replace(":meta", "") if fid.endswith(":meta") else fid
                for fid in raw_ids
            ]
            if not function_ids:
                # Fallback scan
                pattern = f"{collection}:function:{file_md5}:*:vec:tf"
                keys = r.scan_iter(pattern)
                function_ids = [k.replace(":vec:tf", "") for k in keys]

        if not function_ids:
            # Full collection clear if no filters and no specific functions found
            if not batch_uuid and not file_md5:
                patterns = [
                    f"{collection}:feature:*:functions",
                    f"{collection}:feature:*:meta",
                    # :stats is a derived cache of :meta; invalidate it the same way
                    # so the next enrich falls back to sampling surviving :meta.
                    f"{collection}:feature:*:stats",
                    f"{collection}:features:by_tf",
                    f"{collection}:feature:*:global_meta",
                    f"{collection}:idx:feature:*",
                    f"{collection}:reg:feature:*",
                    f"{collection}:all_features",
                ]
                for pattern in patterns:
                    cursor = 0
                    while True:
                        cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                        if keys:
                            r.delete(*keys)
                        if cursor == 0:
                            break
                # Also reset indexed flag for all functions in this collection
                cursor = 0
                while True:
                    cursor, keys = r.scan(
                        cursor=cursor, match=f"{collection}:function:*:meta", count=1000
                    )
                    if keys:
                        fids = [k.replace(":meta", "") for k in keys]
                        r.srem(f"{collection}:indexed:functions", *fids)
                    if cursor == 0:
                        break
                r.incr(f"{collection}:features:generation")
                return True
            return False

        # 2. Targeted clear
        logging.info(
            f"[*] Clearing features for {len(function_ids)} functions in {collection}..."
        )
        affected_features = set()
        for start in range(0, len(function_ids), 100):
            fids = function_ids[start : start + 100]
            reader = r.pipeline(transaction=False)
            for fid in fids:
                reader.get(f"{fid}:vec:meta")

            pipe = r.pipeline(transaction=False)
            for fid, raw_meta in zip(fids, reader.execute()):
                if raw_meta:
                    raw_meta = json.loads(raw_meta)
                    if isinstance(raw_meta, list) and len(raw_meta) == 1:
                        raw_meta = raw_meta[0]

                if not raw_meta:
                    continue

                for feat in raw_meta:
                    f_hash = feat.get("hash")
                    tf = feat.get("tf", 1)
                    if not f_hash:
                        continue

                    affected_features.add(f_hash)
                    pipe.zrem(f"{collection}:feature:{f_hash}:functions", fid)
                    pipe.zincrby(f"{collection}:features:by_tf", -float(tf), f_hash)
                    pipe.hdel(f"{collection}:feature:{f_hash}:meta", fid)
                    # :stats is a derived cache; subtracting counts would drift,
                    # so invalidate it. The affected_features reindex right after
                    # falls back to sampling the surviving :meta, which is exact.
                    pipe.delete(f"{collection}:feature:{f_hash}:stats")

                pipe.delete(f"{fid}:vec:norm")
                pipe.srem(f"{collection}:indexed:functions", fid)
            pipe.execute()

        # Re-index remaining occurrences for affected features
        if affected_features:
            self.index_global_features(collection, list(affected_features))

        r.incr(f"{collection}:features:generation")

        return True

    def get_indexing_status(self, collection, batch_uuid=None, file_md5=None):
        """Returns high-level indexing stats (collection, batch, or file level)."""
        r = self.r
        indexed_set = f"{collection}:indexed:functions"

        if file_md5:
            file_func_set = f"{collection}:idx:file:functions:{file_md5}"
            total = r.scard(file_func_set)
            try:
                indexed = r.execute_command(
                    "SINTERCARD", "2", file_func_set, indexed_set
                )
            except:
                indexed = len(r.sinter(file_func_set, indexed_set))
            return {
                "total": total,
                "indexed": indexed,
                "unindexed": max(0, total - indexed),
                "ratio": (indexed / total * 100) if total > 0 else 0,
            }

        total = 0
        indexed = 0
        batch_uuids = [batch_uuid] if batch_uuid else list(r.smembers("global:batches"))
        for b_uuid in batch_uuids:
            batch_func_set = f"{collection}:batch:{b_uuid}:functions"
            if not r.exists(batch_func_set):
                continue

            b_total = r.scard(batch_func_set)
            total += b_total
            try:
                b_indexed = r.execute_command(
                    "SINTERCARD", "2", batch_func_set, indexed_set
                )
            except:
                # Fallback for environments without SINTERCARD
                b_indexed = len(r.sinter(batch_func_set, indexed_set))
            indexed += b_indexed

        return {
            "total": total,
            "indexed": indexed,
            "unindexed": max(0, total - indexed),
            "ratio": (indexed / total * 100) if total > 0 else 0,
        }

    def list_batches_status(self, collection, batch_filter=None):
        """Returns detailed indexing status for all batches in a collection."""
        r = self.r
        batch_uuids = r.smembers("global:batches")
        indexed_set = f"{collection}:indexed:functions"

        results = []
        for uuid in sorted(list(batch_uuids)):
            if batch_filter and uuid != batch_filter:
                continue

            batch_func_set = f"{collection}:batch:{uuid}:functions"
            if not r.exists(batch_func_set):
                continue

            meta_key = f"{collection}:batch:{uuid}"
            name_raw = r.get(meta_key)
            name = "N/A"
            if name_raw:
                name_raw = json.loads(name_raw)
                if isinstance(name_raw, list):
                    name_raw = name_raw[0]
                name = (
                    name_raw.get("name", "N/A") if isinstance(name_raw, dict) else "N/A"
                )

            total = r.scard(batch_func_set)
            try:
                indexed = r.execute_command(
                    "SINTERCARD", "2", batch_func_set, indexed_set
                )
            except:
                indexed = len(r.sinter(batch_func_set, indexed_set))

            results.append(
                {
                    "batch_uuid": uuid,
                    "name": name,
                    "total": total,
                    "indexed": indexed,
                    "ratio": (indexed / total * 100) if total > 0 else 0,
                }
            )

        return results

    def list_files_status(self, collection):
        """Returns detailed indexing status for all files in a collection."""
        r = self.r
        file_keys = r.smembers(f"{collection}:all_files")
        indexed_set = f"{collection}:indexed:functions"

        results = []
        for f_key in sorted(list(file_keys)):
            # doc_id is {coll}:file:{md5}:meta
            parts = f_key.split(":")
            if len(parts) < 3:
                continue
            md5 = parts[2]

            meta = r.get(f_key)
            if meta:
                meta = json.loads(meta)
                if isinstance(meta, list):
                    meta = meta[0]

            name = meta.get("file_name", "N/A") if isinstance(meta, dict) else "N/A"

            # Get functions for this file (Base IDs now)
            file_func_set = f"{collection}:idx:file:functions:{md5}"
            total = r.scard(file_func_set)

            try:
                indexed = r.execute_command(
                    "SINTERCARD", "2", file_func_set, indexed_set
                )
            except:
                indexed = len(r.sinter(file_func_set, indexed_set))

            results.append(
                {
                    "file_md5": md5,
                    "name": name,
                    "total": total,
                    "indexed": indexed,
                    "ratio": (indexed / total * 100) if total > 0 else 0,
                }
            )

        return results

    def index_global_features(
        self,
        collection,
        feature_hashes,
        job_service=None,
        job_id=None,
        progress_offset=0,
        progress_total=None,
    ):
        """
        Computes global metadata (most common type/op pair, frequency, tf_score, decompiled context)
        for a list of feature hashes, and saves them to KV / secondary indexes.

        `progress_offset`/`progress_total` let enrich_features call this once per
        streamed batch while still reporting progress across the whole run.
        When progress_total is set the caller owns the final 100% update.
        """
        if not feature_hashes:
            return

        from bsimvis.app.services.index_service import save_feature, delete_feature

        logging.info(
            f"[*] Starting global indexing for {len(feature_hashes)} features in {collection}"
        )

        # Process in chunks to avoid blocking Kvrocks / Redis.
        #
        # This is the allocation that OOM-killed the fleet, found by measuring
        # against stdlib-ref (419,617 pending features). Each chunk pipelines
        # one HRANDFIELD-100-withvalues per feature and holds every response in
        # memory at once, so peak scales with chunk_size -- NOT with the size of
        # the pending set. At 500 the worker peaked over 3 GiB and was killed
        # three times before the first batch ever committed. Streaming the
        # pending set bounded the input list; this bounds the actual work.
        # 50, not 100. Measured on 10k real stdlib-ref features, three runs per
        # setting: peak tracks chunk size almost linearly (100 -> 2.07/2.40 GiB,
        # 50 -> 1.60 GiB, 25 -> 1.38 GiB) while throughput stays flat inside
        # noise (44-49 features/s at every setting). The round-trip cost the
        # smaller chunk was expected to pay does not show up, so this is ~30%
        # of the peak for free. 25 buys much less and is not worth the extra
        # round-trips.
        chunk_size = int(os.getenv("ENRICH_CHUNK_SIZE", 50))
        denom = progress_total or len(feature_hashes)
        for i in range(0, len(feature_hashes), chunk_size):
            if job_service and job_id:
                done = progress_offset + i
                # Clamped: features can be queued while the job runs, so `done`
                # may overshoot the count we started with.
                pct = min(99, int(done / denom * 100)) if denom else 0
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Enriching global features: {done}/{denom}",
                    processed=done,
                    total=denom,
                )

            chunk = feature_hashes[i : i + chunk_size]

            # Stats fast path. Pair counts and one representative occurrence per
            # (type, op) pair are persisted at write time on
            # {coll}:feature:{fh}:stats. A feature is covered when :stats has
            # at least one pair-count field; anything else takes the old
            # HRANDFIELD sampling path below (unchanged).
            #
            # `frequency` is still HLEN :feature:{fh}:meta — number of distinct
            # functions with the feature (what the legacy path stored). That is
            # a per-function count, so it is independent of how many (type, op)
            # pairs a function contributes to :stats. Adding one HLEN per
            # feature per chunk (50 extra cmds at chunk_size=50) keeps the
            # stored field byte-identical to the pre-optimization output.
            sp = self.r.pipeline(transaction=False)
            for fh in chunk:
                sp.hgetall(f"{collection}:feature:{fh}:stats")
                sp.hlen(f"{collection}:feature:{fh}:meta")
                sp.zscore(f"{collection}:features:by_tf", fh)
            stats_res = sp.execute()

            # --- STAGE 2: Pick winner per feature; dispatch legacy for missing ---
            save_pipe = self.r.pipeline(transaction=False)

            # Phase A: parse pair counts, build `results`. Legacy features are
            # collected in slow_hashes and handled in the block below.
            results = []
            pending_funcs = {}  # func_id → {func_id, line_idxs list}
            slow_hashes = []

            for idx, fh in enumerate(chunk):
                stats_map = stats_res[idx * 3] or {}
                meta_len = stats_res[idx * 3 + 1]
                tf_score_val = stats_res[idx * 3 + 2]

                counts = {}
                reps = {}
                for f_name, val in stats_map.items():
                    if isinstance(f_name, bytes):
                        f_name = f_name.decode()
                    if f_name.startswith(OCC_FIELD):
                        reps[f_name[len(OCC_FIELD) :]] = val
                    else:
                        try:
                            counts[f_name] = int(val)
                        except (TypeError, ValueError):
                            pass

                if not counts:
                    slow_hashes.append(fh)
                    continue

                # Winner = max(counts, key=(count, pair)) — byte-identical
                # tie-break semantics to the legacy sampling path.
                best_key, _best_count = max(counts.items(), key=lambda x: (x[1], x[0]))
                best_type, best_op = best_key.split(PAIR_SEP, 1)
                best_rep = _safe_json(reps.get(best_key)) or {}
                func_id = best_rep.get("function_id")
                line_idxs = best_rep.get("line_idx") or []
                if func_id and func_id not in pending_funcs:
                    pending_funcs[func_id] = func_id
                fid = func_id or "N/A"
                parts = fid.split(":")
                if len(parts) >= 4:
                    md5, addr = parts[-2], parts[-1]
                else:
                    md5 = addr = "N/A"
                pcode_full = best_rep.get("pcode_op_full")
                if pcode_full is None:
                    # pcode_op_full may live on a non-winner pair's rep; the
                    # legacy path scanned all occurrences and took the first
                    # non-None. Reps are stored in pair order, so this is
                    # equivalent up to the (irrelevant) order of pairs.
                    for rk, rv in reps.items():
                        if rk == best_key:
                            continue
                        r = _safe_json(rv) or {}
                        if r.get("pcode_op_full") is not None:
                            pcode_full = r["pcode_op_full"]
                            break
                if pcode_full is None:
                    pcode_full = "N/A"

                results.append(
                    {
                        "fh": fh,
                        "best_type": best_type,
                        "best_op": best_op,
                        "func_id": fid,
                        "frequency": meta_len,
                        "tf_score": (
                            float(tf_score_val) if tf_score_val is not None else 0.0
                        ),
                        "line_idxs": line_idxs,
                        "c_code": None,
                        "pcode_full": pcode_full,
                        "context": {
                            "type": best_type,
                            "op": best_op,
                            "pcode_full": pcode_full,
                            "func_id": fid,
                            "seq": best_rep.get("seq"),
                            "line_idxs": line_idxs,
                            "md5": md5,
                            "addr": addr,
                            "name": best_rep.get("function_name") or addr,
                            "c_code": None,
                        },
                    }
                )

            if slow_hashes:
                # --- LEGACY PATH: HRANDFIELD + HGETALL sampling (unchanged) ---
                # Only features with no :stats reach this — legacy collections,
                # or features cleared since their last write. Cost is bounded by
                # the number of stats-missing features in the chunk, usually 0.
                pipe1 = self.r.pipeline(transaction=False)
                for fh in slow_hashes:
                    # HRANDFIELD withvalues → 100 random entries across ALL functions
                    # Covers every function proportionally, avoids first-N clustering bias
                    pipe1.hrandfield(
                        f"{collection}:feature:{fh}:meta", count=100, withvalues=True
                    )
                    pipe1.hlen(f"{collection}:feature:{fh}:meta")
                    pipe1.zscore(f"{collection}:features:by_tf", fh)

                res1 = pipe1.execute()

                # Features with < 100 occurrences need a full HGETALL (HRANDFIELD may
                # dedup). That is the common case, so batch them into one round-trip.
                small_pipe = self.r.pipeline(transaction=False)
                small_hashes = [
                    fh
                    for idx, fh in enumerate(slow_hashes)
                    if 0 < res1[idx * 3 + 1] <= 100
                ]
                for fh in small_hashes:
                    small_pipe.hgetall(f"{collection}:feature:{fh}:meta")
                small_full = dict(zip(small_hashes, small_pipe.execute()))

                for idx, fh in enumerate(slow_hashes):
                    hr = res1[idx * 3]
                    # HRANDFIELD withvalues returns flat list [key1, val1, key2, val2, ...]
                    data_batch = dict(zip(hr[0::2], hr[1::2])) if hr else {}
                    total_freq = res1[idx * 3 + 1]
                    tf_score_val = res1[idx * 3 + 2]

                    if fh in small_full:
                        data_batch = small_full[fh]

                    # parse each occ, include function_id from the hash field
                    parsed = {}
                    for func_key, occ_str in data_batch.items():
                        try:
                            occ = json.loads(occ_str)
                            occ["function_id"] = (
                                func_key.decode()
                                if isinstance(func_key, bytes)
                                else func_key
                            )
                            parsed[func_key] = occ
                        except Exception:
                            pass

                    # find most common (type, op) pair
                    best_type, best_op = "N/A", "N/A"
                    func_id, line_idxs, best_occ = None, [], {}
                    if parsed:
                        counts = {}
                        for key, occ in parsed.items():
                            pair = (
                                str(occ.get("type", "N/A")),
                                str(occ.get("pcode_op", "N/A")),
                            )
                            counts[pair] = counts.get(pair, 0) + 1
                        if counts:
                            best_pair = max(counts.items(), key=lambda x: (x[1], x[0]))[
                                0
                            ]
                            best_type, best_op = best_pair
                            for key, occ in parsed.items():
                                if (
                                    str(occ.get("type", "N/A")) == best_type
                                    and str(occ.get("pcode_op", "N/A")) == best_op
                                ):
                                    best_occ = occ
                                    bk = key
                                    if isinstance(bk, bytes):
                                        bk = bk.decode()
                                    func_id = bk  # func_id from hash field name
                                    line_idxs = best_occ.get("line_idx", [])
                                    break
                            # pcode_op_full may be in a non-mode-matching entry — check all
                            pcode_full = best_occ.get("pcode_op_full") or "lazy"
                            if pcode_full == "lazy":
                                for occ in parsed.values():
                                    if occ.get("pcode_op_full"):
                                        pcode_full = occ["pcode_op_full"]
                                        break
                        else:
                            delete_feature(self.r, collection, fh)
                            continue
                    else:
                        delete_feature(self.r, collection, fh)
                        continue

                    # Queue source fetch for this func if not already queued (dedup across chunk)
                    if func_id and func_id not in pending_funcs:
                        pending_funcs[func_id] = func_id

                    # Function parts from func_id (hash key format: {coll}:func:{md5}:{addr})
                    fid = func_id or "N/A"
                    parts = fid.split(":")
                    md5, addr = "N/A", "N/A"
                    if len(parts) >= 4:
                        md5 = parts[-2]
                        addr = parts[-1]

                    pcode_full = pcode_full  # from Phase A loop
                    # Function parts from func_id (hash key format: {coll}:func:{md5}:{addr})
                    fid = func_id or "N/A"
                    parts = fid.split(":")
                    md5, addr = "N/A", "N/A"
                    if len(parts) >= 4:
                        md5 = parts[-2]
                        addr = parts[-1]

                    # pcode_full will be filled from source fetch below
                    results.append(
                        {
                            "fh": fh,
                            "best_type": best_type,
                            "best_op": best_op,
                            "func_id": fid,
                            "frequency": total_freq,
                            "tf_score": (
                                float(tf_score_val) if tf_score_val is not None else 0.0
                            ),
                            "line_idxs": line_idxs,
                            "c_code": None,
                            "pcode_full": pcode_full,
                            "context": {
                                "type": best_type,
                                "op": best_op,
                                "pcode_full": pcode_full,
                                "func_id": fid,
                                "seq": best_occ.get("seq"),
                                "line_idxs": line_idxs,
                                "md5": md5,
                                "addr": addr,
                                "name": best_occ.get("function_name", addr),
                                "c_code": None,
                            },
                        }
                    )

            # Phase B: 1 GET per unique func (2 per func: source + vec:meta)
            source_lookup = {}
            vec_meta_lookup = {}
            if pending_funcs:
                ctx_pipe = self.r.pipeline(transaction=False)
                for func_id in pending_funcs:
                    ctx_pipe.get(f"{func_id}:source")
                ctx_res = ctx_pipe.execute()
                for func_id, ctx_entry in zip(pending_funcs, ctx_res):
                    if ctx_entry:
                        entry_decoded = json.loads(ctx_entry)
                        # An empty list is stored for functions with no source.
                        # `isinstance([], list)` is true, so the old form did
                        # [][0] and raised -- one such record aborted the entire
                        # enrichment run, which at 419k features meant the job
                        # could never finish no matter how often it was retried.
                        if isinstance(entry_decoded, list):
                            entry_decoded = entry_decoded[0] if entry_decoded else {}
                        source_lookup[func_id] = entry_decoded
                    else:
                        source_lookup[func_id] = {}

                ctx_pipe2 = self.r.pipeline(transaction=False)
                for func_id in pending_funcs:
                    ctx_pipe2.get(f"{func_id}:vec:meta")
                ctx_res2 = ctx_pipe2.execute()
                func_vec = list(pending_funcs)
                for func_id, ctx_entry in zip(func_vec, ctx_res2):
                    if ctx_entry:
                        entry_decoded = json.loads(ctx_entry)
                        # Same empty-list guard as :source above.
                        if isinstance(entry_decoded, list):
                            entry_decoded = entry_decoded[0] if entry_decoded else []
                        vec_meta_lookup[func_id] = entry_decoded
                    else:
                        vec_meta_lookup[func_id] = []

            # Phase C: fill in c_code and pcode_full from fetched data
            for result in results:
                if result["func_id"] not in pending_funcs:
                    continue
                source_data = source_lookup.get(result["func_id"], {})
                if isinstance(source_data, dict):
                    # Extract C tokens from source
                    if result["line_idxs"]:
                        try:
                            c_tokens = source_data.get("c_tokens", [])
                            target_line = (
                                int(result["line_idxs"][0])
                                if result["line_idxs"]
                                else -1
                            )
                            if target_line != -1:
                                line_tokens = [
                                    t
                                    for t in c_tokens
                                    if int(t.get("line", -1)) == target_line
                                ]
                                result["c_code"] = [
                                    {
                                        "type": t.get("type"),
                                        "text": str(t.get("t", t.get("text", ""))),
                                    }
                                    for t in line_tokens
                                ]
                        except Exception:
                            pass
                # Fill pcode_full from vec:meta when lazy
                func_id_for_lookup = result["func_id"]
                if result["pcode_full"] == "lazy":
                    vec_entries = vec_meta_lookup.get(func_id_for_lookup, [])
                    if vec_entries and isinstance(vec_entries, list):
                        for feat in vec_entries:
                            if feat.get("hash") == result["fh"]:
                                result["pcode_full"] = feat.get("pcode_op_full", "N/A")
                                break
                result["context"]["pcode_full"] = result["pcode_full"]

                # Fill c_code
                result["context"]["c_code"] = result["c_code"]

            # Phase D: build save_pipe
            for result in results:
                context = result["context"]
                context["c_code"] = result["c_code"]
                global_meta = {
                    "hash": result["fh"],
                    "feature_id": f"{collection}:feature:{result['fh']}",
                    "type": result["best_type"],
                    "op": result["best_op"],
                    "frequency": result["frequency"],
                    "tf_score": result["tf_score"],
                    "context": context,
                }
                save_pipe.set(
                    f"{collection}:feature:{result['fh']}:global_meta",
                    json.dumps(global_meta),
                )
                save_feature(save_pipe, collection, result["fh"], global_meta)

            save_pipe.execute()

        if job_service and job_id and progress_total is None:
            job_service.update_progress(job_id, 100, "Completed feature enrichment.")

    def enrich_features(
        self, collection, job_service=None, job_id=None, batch_size=None
    ):
        """
        Enriches all features that were added to the pending enrichment set.

        Streamed and resumable. This handler OOM-killed ten workers in one
        evening, and each kill restarted it from zero because the pending set
        was only deleted at the very end -- five kills over two fleets meant the
        same work was attempted five times and never finished.

        Two changes: SSCAN a batch at a time instead of materialising the whole
        set (stdlib-ref holds 375k hashes), and SREM each batch once it has been
        indexed, so the set itself is the checkpoint. A kill now costs at most
        one batch, and a rerun picks up exactly where the last one stopped.
        Removing only what we have already processed is safe under SSCAN.
        """
        # Kept modest deliberately. The batch is only a checkpoint interval --
        # peak memory is governed by ENRICH_CHUNK_SIZE inside
        # index_global_features -- but a smaller batch also means a kill costs
        # less work. 5000 checkpointed too rarely to be useful at 419k features.
        batch_size = batch_size or int(os.getenv("ENRICH_BATCH_SIZE", 1000))
        pending_key = f"{collection}:features:pending_enrichment"
        total = self.r.scard(pending_key)
        if not total:
            logging.info(
                f"[*] No pending features to enrich in collection: {collection}"
            )
            if job_service and job_id:
                job_service.update_progress(
                    job_id, 100, "No pending features to enrich."
                )
            return True

        logging.info(
            f"[*] Enriching {total} global features for collection: {collection}"
        )

        processed = 0
        while True:
            # Cursor 0 every time on purpose: the batch we just finished is gone
            # from the set, so the next scan returns the next slice of work.
            _, batch = self.r.sscan(pending_key, 0, count=batch_size)
            if not batch:
                break
            batch = [fh.decode() if isinstance(fh, bytes) else fh for fh in batch]

            self.index_global_features(
                collection,
                batch,
                job_service,
                job_id,
                progress_offset=processed,
                progress_total=total,
            )
            # Checkpoint AFTER the batch is indexed, never before -- the same
            # ordering rule as the INDEX_FUNCTIONS chunk delete.
            self.r.srem(pending_key, *batch)
            processed += len(batch)
            logging.info(f"[*] Enriched {processed}/{total} features in {collection}")

        if job_service and job_id:
            job_service.update_progress(
                job_id,
                100,
                f"Completed feature enrichment ({processed} features).",
                processed=processed,
                total=total,
            )
        return True
