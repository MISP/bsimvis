import math
import logging
import json
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.milvus_service import milvus_service


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
                batch = self._fetch_vec_batch(
                    function_ids[i : i + self.READ_BATCH]
                )

            raw_meta, new_tf_data = batch.pop(func_id)
            if not raw_meta or not new_tf_data:
                logging.warning(
                    f"  [!] Skipping {func_id}: Missing metadata or vector data."
                )
                continue

            # A. Recalculate L2 Norm
            sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
            pipe.set(f"{func_id}:vec:norm", math.sqrt(sum_sq))

            # B. Build Reverse Index (ZSETs)
            tf_dict = {
                h.decode() if isinstance(h, bytes) else str(h): float(score)
                for h, score in new_tf_data
            }

            for f_hash, new_tf in tf_dict.items():
                indexed_features.add(f_hash)
                # Update function mapping for this feature
                pipe.zadd(f"{collection}:feature:{f_hash}:functions", {func_id: new_tf})
                # Update global TF counter for this feature
                pipe.zincrby(f"{collection}:features:by_tf", float(new_tf), f_hash)

            # Store feature metadata as a JSON string in a HASH keyed by function_id
            for feat_item in raw_meta:
                f_hash = feat_item.get("hash")
                if not f_hash:
                    continue
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

            # Execute pipeline in chunks to reduce memory footprint and network overhead
            if (i + 1) % 100 == 0:
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
            raw_meta = r.get(meta_key)
            if raw_meta:
                raw_meta = json.loads(raw_meta)
                if isinstance(raw_meta, list) and len(raw_meta) == 1:
                    raw_meta = raw_meta[0]

            if not raw_meta:
                continue

            pipe = r.pipeline(transaction=False)
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
        self, collection, feature_hashes, job_service=None, job_id=None
    ):
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
        chunk_size = 500
        for i in range(0, len(feature_hashes), chunk_size):
            if job_service and job_id:
                pct = int(i / len(feature_hashes) * 100)
                job_service.update_progress(
                    job_id, pct, f"Enriching global features: {i}/{len(feature_hashes)}"
                )

            chunk = feature_hashes[i : i + chunk_size]

            # --- STAGE 1: Batch fetch sparse samples, frequencies, and scores ---
            pipe1 = self.r.pipeline(transaction=False)
            for fh in chunk:
                # HRANDFIELD withvalues → 100 random entries across ALL functions
                # Covers every function proportionally, avoids first-N clustering bias
                pipe1.hrandfield(
                    f"{collection}:feature:{fh}:meta", count=100, withvalues=True
                )
                pipe1.hlen(f"{collection}:feature:{fh}:meta")
                pipe1.zscore(f"{collection}:features:by_tf", fh)

            res1 = pipe1.execute()

            # --- STAGE 2: Find best (type, op) per feature, assemble into save_pipe ---
            save_pipe = self.r.pipeline(transaction=False)

            # Phase A: parse HRANDFIELD, collect unique func_ids for context fetch
            results = []
            pending_funcs = {}  # func_id → {func_id, line_idxs list}

            # Features with < 100 occurrences need a full HGETALL (HRANDFIELD may
            # dedup). That is the common case, so batch them into one round-trip.
            small_pipe = self.r.pipeline(transaction=False)
            small_hashes = [
                fh
                for idx, fh in enumerate(chunk)
                if 0 < res1[idx * 3 + 1] <= 100
            ]
            for fh in small_hashes:
                small_pipe.hgetall(f"{collection}:feature:{fh}:meta")
            small_full = dict(zip(small_hashes, small_pipe.execute()))

            for idx, fh in enumerate(chunk):
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
                        best_pair = max(counts.items(), key=lambda x: (x[1], x[0]))[0]
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
                        source_lookup[func_id] = (
                            entry_decoded[0]
                            if isinstance(entry_decoded, list)
                            else entry_decoded
                        )
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
                        vec_meta_lookup[func_id] = (
                            entry_decoded[0]
                            if isinstance(entry_decoded, list)
                            else entry_decoded
                        )
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

        if job_service and job_id:
            job_service.update_progress(job_id, 100, "Completed feature enrichment.")

    def enrich_features(self, collection, job_service=None, job_id=None):
        """
        Enriches all features that were added to the pending enrichment set.
        """
        pending_key = f"{collection}:features:pending_enrichment"
        feature_hashes = list(self.r.smembers(pending_key))
        if not feature_hashes:
            logging.info(
                f"[*] No pending features to enrich in collection: {collection}"
            )
            if job_service and job_id:
                job_service.update_progress(
                    job_id, 100, "No pending features to enrich."
                )
            return True

        feature_hashes = [
            fh.decode() if isinstance(fh, bytes) else fh for fh in feature_hashes
        ]

        total = len(feature_hashes)
        logging.info(
            f"[*] Enriching {total} global features for collection: {collection}"
        )
        self.index_global_features(collection, feature_hashes, job_service, job_id)
        self.r.delete(pending_key)
        return True
