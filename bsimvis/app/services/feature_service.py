import math
import logging
import json
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.milvus_service import milvus_service


class FeatureService:
    def __init__(self, r=None):
        self.r = r or get_redis()

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

        for i, func_id in enumerate(function_ids):
            # Update job progress if applicable
            if job_service and job_id and (i % 10 == 0 or i == total - 1):
                pct = int((i + 1) / total * 100)
                job_service.update_progress(
                    job_id, pct, f"Indexing features: {i+1}/{total}"
                )

            meta_key = f"{func_id}:vec:meta"
            tf_key = f"{func_id}:vec:tf"

            # 1. Fetch metadata and vector data
            raw_meta = self.r.json().get(meta_key, "$")
            if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1:
                raw_meta = raw_meta[0]

            new_tf_data = self.r.zrange(tf_key, 0, -1, withscores=True)
            if not raw_meta or not new_tf_data:
                logging.warning(
                    f"  [!] Skipping {func_id}: Missing metadata or vector data."
                )
                continue

            pipe = self.r.pipeline()

            # A. Recalculate L2 Norm
            sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
            pipe.set(f"{func_id}:vec:norm", math.sqrt(sum_sq))

            # B. Build Reverse Index (ZSETs)
            tf_dict = {h: float(score) for h, score in new_tf_data}

            for feat_item in raw_meta:
                f_hash = feat_item.get("hash")
                if not f_hash:
                    continue

                indexed_features.add(f_hash)
                new_tf = tf_dict.get(f_hash, 0)

                # Update function mapping for this feature
                pipe.zadd(f"{collection}:feature:{f_hash}:functions", {func_id: new_tf})

                # Update global TF counter for this feature
                pipe.zincrby(f"{collection}:features:by_tf", float(new_tf), f_hash)

                # Store feature metadata as a JSON string in a HASH keyed by function_id
                # This allows the API to pick any function's context for a feature.
                meta_entry = dict(feat_item)
                meta_entry["function_id"] = func_id

                # Convention: {coll}:feature:{hash}:meta -> HASH (field=func_id, value=JSON)
                pipe.hset(
                    f"{collection}:feature:{f_hash}:meta",
                    func_id,
                    json.dumps(meta_entry),
                )

            # Mark as indexed (Base ID)
            pipe.sadd(f"{collection}:indexed:functions", func_id)
            pipe.execute()

            # Milvus Buffer
            if milvus_service.enabled:
                milvus_data.append({"fid": func_id, "tf_dict": tf_dict})
                if len(milvus_data) >= milvus_chunk_size:
                    for itype in ["SPARSE_INVERTED_INDEX"]:
                        milvus_service.upsert_functions(
                            collection, milvus_data, index_type=itype
                        )
                    milvus_data = []

        # Final Milvus Flush
        if milvus_service.enabled and milvus_data:
            for itype in ["SPARSE_INVERTED_INDEX"]:
                milvus_service.upsert_functions(
                    collection, milvus_data, index_type=itype
                )

        if indexed_features:
            self.r.sadd(f"{collection}:features:pending_enrichment", *list(indexed_features))

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
                return True
            return False

        # 2. Targeted clear
        logging.info(
            f"[*] Clearing features for {len(function_ids)} functions in {collection}..."
        )
        affected_features = set()
        for fid in function_ids:
            meta_key = f"{fid}:vec:meta"
            raw_meta = r.json().get(meta_key, "$")
            if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1:
                raw_meta = raw_meta[0]

            if not raw_meta:
                continue

            pipe = r.pipeline()
            for feat in raw_meta:
                f_hash = feat.get("hash")
                tf = feat.get("tf", 1)
                if not f_hash:
                    continue

                affected_features.add(f_hash)
                # Remove from inverted index and subtract from global rank
                pipe.zrem(f"{collection}:feature:{f_hash}:functions", fid)
                pipe.zincrby(f"{collection}:features:by_tf", -float(tf), f_hash)
                # Remove from feature details HASH
                pipe.hdel(f"{collection}:feature:{f_hash}:meta", fid)

            pipe.delete(f"{fid}:vec:norm")
            pipe.srem(f"{collection}:indexed:functions", fid)
            pipe.execute()

        # Re-index remaining occurrences for affected features
        if affected_features:
            self.index_global_features(collection, list(affected_features))

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
            name_raw = r.json().get(meta_key, "$")
            name = "N/A"
            if name_raw:
                if isinstance(name_raw, list):
                    name_raw = name_raw[0]
                name = name_raw.get("name", "N/A")

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

            meta = r.json().get(f_key, "$")
            if meta and isinstance(meta, list):
                meta = meta[0]

            name = meta.get("file_name", "N/A") if meta else "N/A"

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

    def index_global_features(self, collection, feature_hashes, job_service=None, job_id=None):
        """
        Computes global metadata (most common type/op pair, frequency, tf_score, decompiled context)
        for a list of feature hashes, and saves them to KV / secondary indexes.
        """
        if not feature_hashes:
            return

        from bsimvis.app.services.index_service import save_feature, delete_feature

        logging.info(
            f"[*] Starting global indexing for {len(feature_hashes)} features in {collection}"
        )

        # Process in chunks to avoid blocking Kvrocks / Redis
        chunk_size = 200
        for i in range(0, len(feature_hashes), chunk_size):
            if job_service and job_id:
                pct = int(i / len(feature_hashes) * 100)
                job_service.update_progress(
                    job_id, pct, f"Enriching global features: {i}/{len(feature_hashes)}"
                )

            chunk = feature_hashes[i : i + chunk_size]

            # --- STAGE 1: Batch fetch samples, frequencies, and scores ---
            pipe1 = self.r.pipeline()
            for fh in chunk:
                # Use a single HSCAN call as a representative sample (1000 items)
                pipe1.hscan(f"{collection}:feature:{fh}:meta", cursor=0, count=1000)
                pipe1.hlen(f"{collection}:feature:{fh}:meta")
                pipe1.zscore(f"{collection}:features:by_tf", fh)

            res1 = pipe1.execute()

            # --- STAGE 2: Identify best occurrences and batch fetch contexts ---
            context_fetch_pipe = self.r.pipeline()
            fetch_map = []

            for idx, fh in enumerate(chunk):
                # res1 indices: HSCAN=idx*3, HLEN=idx*3+1, ZSCORE=idx*3+2
                hscan_res = res1[idx * 3]
                total_freq = res1[idx * 3 + 1]
                tf_score_val = res1[idx * 3 + 2]

                # hscan_res is [cursor, {func_id: occ_str, ...}]
                data_batch = (
                    hscan_res[1]
                    if (isinstance(hscan_res, (list, tuple)) and len(hscan_res) > 1)
                    else {}
                )

                occurrences = []
                for func_id, occ_str in data_batch.items():
                    try:
                        occ = json.loads(occ_str)
                        occ["function_id"] = func_id
                        occurrences.append(occ)
                    except Exception:
                        pass

                if not occurrences:
                    delete_feature(self.r, collection, fh)
                    context_fetch_pipe.execute_command("ECHO", "delete_dummy")
                    context_fetch_pipe.execute_command("ECHO", "delete_dummy")
                    fetch_map.append(None)
                    continue

                # Group by (type, pcode_op) to find the most common pair
                counts = {}
                for occ in occurrences:
                    pair = (
                        str(occ.get("type", "N/A")),
                        str(occ.get("pcode_op", "N/A")),
                    )
                    counts[pair] = counts.get(pair, 0) + 1

                if not counts:
                    delete_feature(self.r, collection, fh)
                    context_fetch_pipe.execute_command("ECHO", "delete_dummy")
                    context_fetch_pipe.execute_command("ECHO", "delete_dummy")
                    fetch_map.append(None)
                    continue

                # Find most common
                best_pair = max(counts.items(), key=lambda x: (x[1], x[0]))[0]
                best_type, best_op = best_pair

                # Filter occurrences to this best pair, and pick the first one
                matching_occs = [
                    occ
                    for occ in occurrences
                    if str(occ.get("type", "N/A")) == best_type
                    and str(occ.get("pcode_op", "N/A")) == best_op
                ]
                best_occ = matching_occs[0]

                func_id = best_occ.get("function_id")
                line_idxs = best_occ.get("line_idx", [])

                # Queue context fetches (Source Code and Full Vector Meta)
                if func_id:
                    context_fetch_pipe.json().get(f"{func_id}:source", "$")
                else:
                    context_fetch_pipe.execute_command("ECHO", "no_source")

                if not best_occ.get("pcode_op_full") and func_id:
                    context_fetch_pipe.json().get(f"{func_id}:vec:meta", "$")
                else:
                    context_fetch_pipe.execute_command("ECHO", "no_fallback_meta")

                fetch_map.append(
                    {
                        "fh": fh,
                        "best_type": best_type,
                        "best_op": best_op,
                        "best_occ": best_occ,
                        "line_idxs": line_idxs,
                        "frequency": total_freq,
                        "tf_score": (
                            float(tf_score_val) if tf_score_val is not None else 0.0
                        ),
                    }
                )

            context_res = context_fetch_pipe.execute()

            # --- STAGE 3: Final assembly and batch save ---
            save_pipe = self.r.pipeline()
            for idx, info in enumerate(fetch_map):
                if not info:
                    continue

                fh = info["fh"]
                best_occ = info["best_occ"]
                best_type = info["best_type"]
                best_op = info["best_op"]
                line_idxs = info["line_idxs"]

                # Each fetch_map entry consumed 2 pipeline calls
                source_data = context_res[idx * 2]
                if isinstance(source_data, list) and source_data:
                    source_data = source_data[0]

                vec_meta = context_res[idx * 2 + 1]
                if isinstance(vec_meta, list) and vec_meta:
                    vec_meta = vec_meta[0]

                # Extract C tokens
                c_code = None
                target_line = (
                    int(line_idxs[0]) if (line_idxs and len(line_idxs) > 0) else -1
                )
                if source_data and isinstance(source_data, dict) and target_line != -1:
                    try:
                        c_tokens = source_data.get("c_tokens", [])
                        line_tokens = [
                            t for t in c_tokens if int(t.get("line", -1)) == target_line
                        ]
                        if line_tokens:
                            c_code = [
                                {
                                    "type": t.get("type"),
                                    "text": str(t.get("t", t.get("text", ""))),
                                }
                                for t in line_tokens
                            ]
                    except Exception as e:
                        logging.error(
                            f"Error extracting global tokens for feature {fh}: {e}"
                        )

                # Extract Pcode fallback
                pcode_full = best_occ.get("pcode_op_full")
                if not pcode_full and isinstance(vec_meta, list):
                    for feat in vec_meta:
                        if feat.get("hash") == fh:
                            pcode_full = feat.get("pcode_op_full")
                            break
                if not pcode_full:
                    pcode_full = "N/A"

                # Function parts
                fid = best_occ.get("function_id", "")
                parts = fid.split(":")
                md5, addr = "N/A", "N/A"
                if len(parts) >= 4:
                    md5 = parts[-2]
                    addr = parts[-1]

                context = {
                    "type": best_type,
                    "op": best_op,
                    "pcode_full": pcode_full,
                    "func_id": fid,
                    "seq": best_occ.get("seq"),
                    "line_idxs": line_idxs,
                    "md5": md5,
                    "addr": addr,
                    "name": best_occ.get("function_name", addr),
                    "c_code": c_code,
                }

                global_meta = {
                    "hash": fh,
                    "feature_id": f"{collection}:feature:{fh}",
                    "type": best_type,
                    "op": best_op,
                    "frequency": info["frequency"],
                    "tf_score": info["tf_score"],
                    "context": context,
                }

                # Write to Kvrocks JSON and Update Secondary Indexes
                save_pipe.json().set(
                    f"{collection}:feature:{fh}:global_meta", "$", global_meta
                )
                save_feature(save_pipe, collection, fh, global_meta)

            save_pipe.execute()

        if job_service and job_id:
            job_service.update_progress(job_id, 100, "Completed feature enrichment.")

    def enrich_features(self, collection, job_service=None, job_id=None):
        """
        Enriches all features that were added to the pending enrichment set.
        """
        pending_key = f"{collection}:features:pending_enrichment"
        feature_hashes = list(self.r.smembers(pending_key))
        if not feature_hashes:
            logging.info(f"[*] No pending features to enrich in collection: {collection}")
            if job_service and job_id:
                job_service.update_progress(job_id, 100, "No pending features to enrich.")
            return True

        feature_hashes = [
            fh.decode() if isinstance(fh, bytes) else fh for fh in feature_hashes
        ]

        total = len(feature_hashes)
        logging.info(f"[*] Enriching {total} global features for collection: {collection}")
        self.index_global_features(collection, feature_hashes, job_service, job_id)
        self.r.delete(pending_key)
        return True
