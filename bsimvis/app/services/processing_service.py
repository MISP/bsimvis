import logging
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import save_file, save_function


class ProcessingService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def index_metadata(self, collection, file_id, job_service=None, job_id=None):
        """Indexes file, batch, and collection metadata globals and explodes file-level meta."""
        logging.info(f"[*] Indexing metadata for {file_id} in {collection}...")

        # Since we use SET instead of JSON.SET for the monolith, we load the whole string.
        raw_data = self.r.get(file_id)
        if not raw_data:
            logging.error(f"Data not found for {file_id}")
            return False

        import json

        data = json.loads(raw_data)

        if not data:
            logging.error(f"Data not found for {file_id}")
            return False

        file_meta = data.get("file_metadata", {})
        file_md5 = file_meta.get("file_md5") or data.get("file_md5") or "unknown_md5"
        batch_uuid = (
            file_meta.get("batch_uuid")
            or data.get("batch_uuid")
            or "unknown_batch_uuid"
        )
        batch_name = (
            file_meta.get("batch_name")
            or data.get("batch_name")
            or "unknown_batch_name"
        )
        num_functions = len(data.get("functions", []))
        timestamp = file_meta.get("entry_date") or data.get("entry_date") or 0

        # Create the standalone file metadata key (exploded from the main blob)
        file_base_id = f"{collection}:file:{file_md5}"
        file_meta_key = f"{file_base_id}:meta"
        coll_file_meta = dict(file_meta)
        coll_file_meta["collection"] = collection
        coll_file_meta["type"] = "file"
        coll_file_meta["file_id"] = file_base_id
        coll_file_meta["function_count"] = num_functions

        # Calculate total bsim features
        total_features = 0
        for f in data.get("functions", []):
            total_features += f.get("function_metadata", {}).get(
                "bsim_features_count", 0
            )
        coll_file_meta["bsim_features_count"] = total_features

        pipe = self.r.pipeline()

        # 0. Store exploded file meta
        pipe.json().set(file_meta_key, "$", coll_file_meta)

        # 1. Standard file-level indexing (secondary search)
        save_file(pipe, collection, file_md5, coll_file_meta)

        # 2. Global Batch & Collection Registry
        pipe.sadd("global:batches", batch_uuid)
        pipe.sadd("global:collections", collection)

        # 3. Global Batch Metadata
        global_batch_key = f"global:batch:{batch_uuid}"
        exists = self.r.exists(global_batch_key)
        if not exists:
            initial_global_batch = {
                "name": batch_name,
                "batch_uuid": batch_uuid,
                "batch_id": global_batch_key,
                "created_at": timestamp,
                "last_updated": timestamp,
                "collections": {collection: True},
            }
            self.r.json().set(global_batch_key, "$", initial_global_batch)
        else:
            pipe.json().set(global_batch_key, f'$["collections"]["{collection}"]', True)
            pipe.json().set(global_batch_key, '$["last_updated"]', timestamp)

        # 4. Collection Stats
        coll_meta_key = f"global:collection:{collection}:meta"
        pipe.hincrby(coll_meta_key, "total_files", 1)
        pipe.hincrby(coll_meta_key, "total_functions", num_functions)
        pipe.hset(coll_meta_key, "last_updated", timestamp)
        pipe.sadd(f"{collection}:all_batches", batch_uuid)

        # 5. Collection Batch Metadata
        batch_key = f"{collection}:batch:{batch_uuid}"
        exists_batch = self.r.exists(batch_key)
        if not exists_batch:
            initial_batch_data = {
                "name": batch_name,
                "batch_uuid": batch_uuid,
                "batch_id": batch_key,
                "created_at": timestamp,
                "last_updated": timestamp,
                "total_files": 0,
                "total_functions": 0,
                "collection": collection,
            }
            self.r.json().set(batch_key, "$", initial_batch_data)

        pipe.json().numincrby(batch_key, '$["total_files"]', 1)
        pipe.json().numincrby(batch_key, '$["total_functions"]', num_functions)
        pipe.json().set(batch_key, '$["last_updated"]', timestamp)

        pipe.execute()

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, "Metadata and registry indexing complete."
            )

        return True

    def index_functions(self, collection, file_id, job_service=None, job_id=None):
        """Explodes and indexes all functions in a file."""
        logging.info(f"[*] Exploding and indexing functions for {file_id}...")

        # Load monolith from SET
        raw_data = self.r.get(file_id)
        if not raw_data:
            return False

        import json

        data = json.loads(raw_data)

        if not data:
            return False

        functions = data.get("functions", [])
        total = len(functions)
        file_meta = data.get("file_metadata", {})
        file_md5 = file_meta.get("file_md5") or data.get("file_md5")
        batch_uuid = file_meta.get("batch_uuid") or data.get("batch_uuid")

        if total == 0:
            return True

        for i, func_data in enumerate(functions):
            if job_service and job_id and (i % 50 == 0 or i == total - 1):
                pct = int((i + 1) / total * 100)
                job_service.update_progress(
                    job_id, pct, f"Exploding functions: {i+1}/{total}"
                )

            # --- Extract parts ---
            func_meta = dict(func_data.get("function_metadata", {}))
            func_meta["collection"] = collection

            # Copy file-level metadata to function metadata
            fields_to_copy = [
                "first_seen",
                "last_seen",
                "filetype",
                "avtype",
                "yara",
                "cc_ip",
                "file_names",
            ]
            for f in fields_to_copy:
                if f in file_meta:
                    func_meta[f] = file_meta[f]

            func_features = func_data.get("function_features", {})
            func_source = func_data.get("function_source", {})

            full_id = func_meta.get("full_id", "")
            addr = full_id.split(":@")[-1] if ":@" in full_id else "unknown_addr"

            base_func_key = f"{collection}:func:{file_md5}:{addr}"
            func_meta["function_id"] = base_func_key

            # --- Store exploded data ---
            pipe = self.r.pipeline()
            pipe.json().set(f"{base_func_key}:meta", "$", func_meta)
            pipe.json().set(f"{base_func_key}:source", "$", func_source)

            vec_meta = func_features.get("bsim_features_meta", [])
            pipe.json().set(f"{base_func_key}:vec:meta", "$", vec_meta)

            vec_raw = func_features.get("bsim_features_raw", [])
            pipe.json().set(f"{base_func_key}:vec:raw", "$", vec_raw)

            # --- Store Call Graph Sets ---
            callees_key = f"{base_func_key}:callees"
            callers_key = f"{base_func_key}:callers"
            pipe.delete(callees_key)
            pipe.delete(callers_key)

            callees = func_meta.get("callees", [])
            for callee in callees:
                callee_entry = callee.get("entrypoint")
                callee_name = callee.get("name")
                is_ext = callee.get("is_external", False)
                if is_ext or not callee_entry:
                    callee_id = f"ext:{callee_name}"
                else:
                    callee_id = f"{collection}:func:{file_md5}:{callee_entry}"
                pipe.sadd(callees_key, callee_id)

            callers = func_meta.get("callers", [])
            for caller in callers:
                caller_entry = caller.get("entrypoint")
                caller_name = caller.get("name")
                is_ext = caller.get("is_external", False)
                if is_ext or not caller_entry:
                    caller_id = f"ext:{caller_name}"
                else:
                    caller_id = f"{collection}:func:{file_md5}:{caller_entry}"
                pipe.sadd(callers_key, caller_id)

            # Add to batch-to-functions mapping SET (using base key)
            if batch_uuid:
                pipe.sadd(f"{collection}:batch:{batch_uuid}:functions", base_func_key)

            vec_tf_list = func_features.get("bsim_features_tf", [])
            if vec_tf_list:
                zset_mapping = {item["hash"]: item["tf"] for item in vec_tf_list}
                pipe.zadd(f"{base_func_key}:vec:tf", zset_mapping)

            # --- Secondary Indexing ---
            save_function(pipe, collection, file_md5, addr, func_meta)

            pipe.execute()

        return True

    def delete_collection(self, collection, job_service=None, job_id=None):
        """Deletes everything about a collection from Redis/Kvrocks and Milvus."""
        r = self.r
        logging.info(f"[*] Starting full deletion of collection: {collection}")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Starting full deletion of collection: {collection}"
            )

        # 1. Find all batch UUIDs associated with this collection
        batch_uuids = set()
        batch_prefix = f"{collection}:batch:"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{batch_prefix}*", count=1000)
            for k in keys:
                k_str = k.decode() if isinstance(k, bytes) else k
                # Key format: {collection}:batch:{batch_uuid} or {collection}:batch:{batch_uuid}:functions
                parts = k_str.split(":")
                if len(parts) >= 3:
                    batch_uuid = parts[2]
                    batch_uuids.add(batch_uuid)
            if cursor == 0:
                break

        total_batches = len(batch_uuids)
        logging.info(
            f"[*] Found {total_batches} batches associated with collection {collection}"
        )
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Found {total_batches} batches to update/remove."
            )

        # 2. Update global batch metadata
        pipe = r.pipeline()
        batches_removed = []
        for idx, batch_uuid in enumerate(batch_uuids):
            global_batch_key = f"global:batch:{batch_uuid}"
            # Fetch global batch metadata
            raw_meta = r.json().get(global_batch_key, "$")
            if raw_meta:
                meta = raw_meta[0] if isinstance(raw_meta, list) else raw_meta
                if isinstance(meta, str):
                    import json

                    meta = json.loads(meta)

                # Remove collection from collections dict
                collections = meta.get("collections", {})
                if collection in collections:
                    del collections[collection]

                if not collections:
                    # No other collections use this batch -> delete global batch and remove from global set
                    pipe.delete(global_batch_key)
                    batches_removed.append(batch_uuid)
                else:
                    # Save updated collections dict
                    meta["collections"] = collections
                    pipe.json().set(global_batch_key, "$", meta)

            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        # Remove deleted batches from global:batches
        if batches_removed:
            r.srem("global:batches", *batches_removed)

        if job_service and job_id:
            job_service.add_log(job_id, "Global batch metadata updated.")
            job_service.update_progress(job_id, 20)

        # 3. Clean up Milvus collection if enabled
        try:
            from bsimvis.app.services.milvus_service import milvus_service

            if milvus_service.enabled:
                logging.info(f"[*] Dropping Milvus collection: {collection}")
                if job_service and job_id:
                    job_service.add_log(
                        job_id, f"Dropping Milvus collection: {collection}"
                    )
                milvus_service.drop_collection(collection)
        except Exception as e:
            logging.error(f"[!] Error dropping Milvus collection: {e}")
            if job_service and job_id:
                job_service.add_log(job_id, f"Error dropping Milvus collection: {e}")

        if job_service and job_id:
            job_service.update_progress(job_id, 30)

        # 4. Find and delete all collection-specific keys (chunked to prevent freeze)
        logging.info(f"[*] Scanning and deleting keys matching '{collection}:*'...")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Scanning and deleting all database keys matching '{collection}:*'...",
            )

        cursor = 0
        deleted_count = 0
        pipe = r.pipeline()
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{collection}:*", count=1000)
            if keys:
                pipe.delete(*keys)
                deleted_count += len(keys)

                # Periodic execution to avoid huge memory/network payloads
                if len(pipe) >= 1000:
                    pipe.execute()
                    if job_service and job_id:
                        job_service.add_log(job_id, f"Deleted {deleted_count} keys...")
            if cursor == 0:
                break
        pipe.execute()

        logging.info(f"[+] Deleted {deleted_count} keys for collection {collection}")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Completed database keys cleanup. Deleted {deleted_count} keys.",
            )
            job_service.update_progress(job_id, 80)

        # 5. Clean up remaining global collection registries
        logging.info(f"[*] Removing collection {collection} from global registries...")
        r.srem("global:collections", collection)
        r.delete(f"global:collection:{collection}:meta")

        if job_service and job_id:
            job_service.add_log(
                job_id, f"Collection {collection} deleted successfully."
            )
            job_service.update_progress(job_id, 100)

        return True

    def clean_collection(self, collection, job_service=None, job_id=None):
        """Cleans up temporary raw and JSON upload keys in a collection to save space."""
        r = self.r
        logging.info(
            f"[*] Starting cleanup of temporary keys for collection: {collection}"
        )
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Starting cleanup of temporary keys for collection: {collection}",
            )

        patterns = [f"{collection}:file:*:data", f"{collection}:file:*:raw"]

        pipe = r.pipeline()
        total_deleted = 0

        for pattern in patterns:
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    pipe.delete(*keys)
                    total_deleted += len(keys)
                    if len(pipe) >= 1000:
                        pipe.execute()
                        if job_service and job_id:
                            job_service.add_log(
                                job_id, f"Deleted {total_deleted} temporary keys..."
                            )
                if cursor == 0:
                    break

        pipe.execute()

        logging.info(
            f"[+] Wiped {total_deleted} temporary upload keys for collection {collection}"
        )
        if job_service and job_id:
            job_service.add_log(job_id, f"Wiped {total_deleted} temporary upload keys.")
            job_service.update_progress(job_id, 100)

        return True
