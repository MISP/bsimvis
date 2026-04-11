import math
import logging
import json
from bsimvis.app.services.redis_client import get_redis

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

        for i, func_id in enumerate(function_ids):
            # Update job progress if applicable
            if job_service and job_id and (i % 10 == 0 or i == total - 1):
                pct = int((i + 1) / total * 100)
                job_service.update_progress(job_id, pct, f"Indexing features: {i+1}/{total}")

            meta_key = f"{func_id}:vec:meta"
            tf_key = f"{func_id}:vec:tf"

            # 1. Fetch metadata and vector data
            raw_meta = self.r.json().get(meta_key, "$")
            if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1:
                raw_meta = raw_meta[0]

            new_tf_data = self.r.zrange(tf_key, 0, -1, withscores=True)
            if not raw_meta or not new_tf_data:
                logging.warning(f"  [!] Skipping {func_id}: Missing metadata or vector data.")
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

                new_tf = tf_dict.get(f_hash, 0)
                
                # Update function mapping for this feature
                pipe.zadd(f"idx:{collection}:feature:{f_hash}:functions", {func_id: new_tf})
                
                # Update global TF counter for this feature
                pipe.zincrby(f"idx:{collection}:features:by_tf", float(new_tf), f_hash)
                
                # Store feature metadata as a JSON string in a HASH keyed by function_id
                # This allows the API to pick any function's context for a feature.
                meta_entry = dict(feat_item)
                meta_entry["function_id"] = func_id
                
                # Convention: idx:{coll}:feature:{hash}:meta -> HASH (field=func_id, value=JSON)
                pipe.hset(f"idx:{collection}:feature:{f_hash}:meta", func_id, json.dumps(meta_entry))

            # Mark as indexed (Base ID)
            pipe.sadd(f"idx:{collection}:indexed:functions", func_id)
            pipe.execute()

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
            # Assuming idx:{coll}:file_funcs:{md5} exists
            raw_ids = list(r.smembers(f"idx:{collection}:file_funcs:{file_md5}"))
            function_ids = [fid.replace(":meta", "") if fid.endswith(":meta") else fid for fid in raw_ids]
            if not function_ids:
                # Fallback scan
                pattern = f"{collection}:function:{file_md5}:*:vec:tf"
                keys = r.scan_iter(pattern)
                function_ids = [k.replace(":vec:tf", "") for k in keys]
        
        if not function_ids:
            # Full collection clear if no filters and no specific functions found
            if not batch_uuid and not file_md5:
                patterns = [
                    f"idx:{collection}:feature:*:functions",
                    f"idx:{collection}:feature:*:meta",
                    f"idx:{collection}:features:by_tf",
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
                    cursor, keys = r.scan(cursor=cursor, match=f"{collection}:function:*:meta", count=1000)
                    if keys:
                        fids = [k.replace(":meta", "") for k in keys]
                        r.srem(f"idx:{collection}:indexed:functions", *fids)
                    if cursor == 0:
                        break
                return True
            return False

        # 2. Targeted clear
        logging.info(f"[*] Clearing features for {len(function_ids)} functions in {collection}...")
        for fid in function_ids:
            meta_key = f"{fid}:vec:meta"
            raw_meta = r.json().get(meta_key, "$")
            if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1:
                raw_meta = raw_meta[0]
            
            if not raw_meta: continue

            pipe = r.pipeline()
            for feat in raw_meta:
                f_hash = feat.get("hash")
                tf = feat.get("tf", 1)
                if not f_hash: continue

                # Remove from inverted index and subtract from global rank
                pipe.zrem(f"idx:{collection}:feature:{f_hash}:functions", fid)
                pipe.zincrby(f"idx:{collection}:features:by_tf", -float(tf), f_hash)
                # Remove from feature details HASH
                pipe.hdel(f"idx:{collection}:feature:{f_hash}:meta", fid)
            
            pipe.delete(f"{fid}:vec:norm")
            pipe.srem(f"idx:{collection}:indexed:functions", fid)
            pipe.execute()
            
        return True

    def get_indexing_status(self, collection, batch_uuid=None, file_md5=None):
        """Returns high-level indexing stats (collection, batch, or file level)."""
        r = self.r
        indexed_set = f"idx:{collection}:indexed:functions"

        if file_md5:
            file_func_set = f"idx:{collection}:file_funcs:{file_md5}"
            total = r.scard(file_func_set)
            try:
                indexed = r.execute_command("SINTERCARD", "2", file_func_set, indexed_set)
            except:
                indexed = len(r.sinter(file_func_set, indexed_set))
            return {
                "total": total,
                "indexed": indexed,
                "unindexed": max(0, total - indexed),
                "ratio": (indexed / total * 100) if total > 0 else 0
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
            "ratio": (indexed / total * 100) if total > 0 else 0
        }

    def list_batches_status(self, collection, batch_filter=None):
        """Returns detailed indexing status for all batches in a collection."""
        r = self.r
        batch_uuids = r.smembers("global:batches")
        indexed_set = f"idx:{collection}:indexed:functions"

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

            results.append({
                "batch_uuid": uuid,
                "name": name,
                "total": total,
                "indexed": indexed,
                "ratio": (indexed / total * 100) if total > 0 else 0
            })
            
        return results

    def list_files_status(self, collection):
        """Returns detailed indexing status for all files in a collection."""
        r = self.r
        file_keys = r.smembers(f"idx:{collection}:all_files")
        indexed_set = f"idx:{collection}:indexed:functions"

        results = []
        for f_key in sorted(list(file_keys)):
            # doc_id is {coll}:file:{md5}:meta
            parts = f_key.split(":")
            if len(parts) < 3: continue
            md5 = parts[2]

            meta = r.json().get(f_key, "$")
            if meta and isinstance(meta, list):
                meta = meta[0]
            
            name = meta.get("file_name", "N/A") if meta else "N/A"
            
            # Get functions for this file (Base IDs now)
            file_func_set = f"idx:{collection}:file_funcs:{md5}"
            total = r.scard(file_func_set)
            
            try:
                indexed = r.execute_command(
                    "SINTERCARD", "2", file_func_set, indexed_set
                )
            except:
                indexed = len(r.sinter(file_func_set, indexed_set))

            results.append({
                "file_md5": md5,
                "name": name,
                "total": total,
                "indexed": indexed,
                "ratio": (indexed / total * 100) if total > 0 else 0
            })
            
        return results
