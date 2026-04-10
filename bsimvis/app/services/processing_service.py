import logging
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import save_file, save_function

class ProcessingService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def index_metadata(self, collection, file_id, job_service=None, job_id=None):
        """Indexes file, batch, and collection metadata globals and explodes file-level meta."""
        logging.info(f"[*] Indexing metadata for {file_id} in {collection}...")
        
        # OPTIMIZATION: Only fetch metadata and function list (to get count), skip heavy source/features
        # Depending on Kvrocks implementation, fetching multiple paths is more efficient than the whole blob
        data_list = self.r.json().get(file_id, "$.file_metadata", "$.file_md5", "$.batch_uuid", "$.batch_name", "$.entry_date", "$.functions")
        
        if not data_list or not isinstance(data_list, dict):
             # Fallback if the path-based fetch fails or returns unexpected format
             data = self.r.json().get(file_id, "$")
             if isinstance(data, list) and data: data = data[0]
        else:
             # Helper for safe path extraction (Kvrocks can return [] for missing paths)
             def get_path(p, default=None):
                 val = data_list.get(p)
                 return val[0] if (isinstance(val, list) and len(val) > 0) else default

             file_meta = get_path("$.file_metadata", {})
             file_md5 = get_path("$.file_md5")
             batch_uuid = get_path("$.batch_uuid")
             batch_name = get_path("$.batch_name")
             timestamp = get_path("$.entry_date", 0)
             
             funcs = get_path("$.functions", [])
             num_functions = len(funcs) if isinstance(funcs, list) else 0
             
             data = {
                 "file_metadata": file_meta,
                 "file_md5": file_md5,
                 "batch_uuid": batch_uuid,
                 "batch_name": batch_name,
                 "entry_date": timestamp,
                 "functions": [] # We only needed the count for this step
             }
             # For indexing count, we don't need the actual function list, just len
        
        if not data:
            logging.error(f"Data not found for {file_id}")
            return False

        file_meta = data.get("file_metadata", {})
        file_md5 = file_meta.get("file_md5") or data.get("file_md5") or "unknown_md5"
        batch_uuid = file_meta.get("batch_uuid") or data.get("batch_uuid") or "unknown_batch_uuid"
        batch_name = file_meta.get("batch_name") or data.get("batch_name") or "unknown_batch_name"
        num_functions = len(data.get("functions", []))
        timestamp = file_meta.get("entry_date") or data.get("entry_date") or 0

        # Create the standalone file metadata key (exploded from the main blob)
        file_base_id = f"idx:{collection}:file:{file_md5}"
        file_meta_key = f"{file_base_id}:meta"
        coll_file_meta = dict(file_meta)
        coll_file_meta["collection"] = collection
        coll_file_meta["type"] = "file"
        coll_file_meta["file_id"] = file_base_id
        
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
            job_service.update_progress(job_id, 100, "Metadata and registry indexing complete.")
            
        return True

    def index_functions(self, collection, file_id, job_service=None, job_id=None):
        """Explodes and indexes all functions in a file."""
        logging.info(f"[*] Exploding and indexing functions for {file_id}...")
        
        # OPTIMIZATION: Get all functions but exclude heavy fields (source and raw features)
        # We fetch the function metadata, bsim meta, and TF vectors.
        # Note: Kvrocks/Redis-py handles this by returning the subset.
        p_meta = "$.file_metadata"
        p_md5 = "$.file_md5"
        p_batch = "$.batch_uuid"
        p_funcs = "$.functions"
        
        # For simplicity and reliability, we can fetch exactly what we need for each function
        # But fetching the whole blob without source/raw is usually better.
        # We'll use a single GET but with exclusion if Kvrocks supported it, 
        # but since it doesn't, we fetch the skeleton.
        data_list = self.r.json().get(file_id, p_meta, p_md5, p_batch, p_funcs)
        
        if not data_list or not isinstance(data_list, dict):
            # Fallback
            data = self.r.json().get(file_id, "$")
            if isinstance(data, list) and data: data = data[0]
        else:
            # Reconstruct a lean data object safely
            def get_path(p, default=None):
                val = data_list.get(p)
                return val[0] if (isinstance(val, list) and len(val) > 0) else default

            file_meta = get_path(p_meta, {})
            file_md5 = get_path(p_md5)
            batch_uuid = get_path(p_batch)
            functions_raw = get_path(p_funcs, [])
            
            data = {
                "file_metadata": file_meta,
                "file_md5": file_md5,
                "batch_uuid": batch_uuid,
                "functions": functions_raw
            }
            
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
                job_service.update_progress(job_id, pct, f"Exploding functions: {i+1}/{total}")
            
            # --- Extract parts ---
            func_meta = dict(func_data.get("function_metadata", {}))
            func_meta["collection"] = collection
            func_features = func_data.get("function_features", {})
            func_source = func_data.get("function_source", {})

            full_id = func_meta.get("full_id", "")
            addr = (
                full_id.split(":@")[-1] if ":@" in full_id else "unknown_addr"
            )

            base_func_key = f"idx:{collection}:func:{file_md5}:{addr}"
            func_meta["function_id"] = base_func_key

            # --- Store exploded data ---
            pipe = self.r.pipeline()
            pipe.json().set(f"{base_func_key}:meta", "$", func_meta)
            pipe.json().set(f"{base_func_key}:source", "$", func_source)
            
            vec_meta = func_features.get("bsim_features_meta", [])
            pipe.json().set(f"{base_func_key}:vec:meta", "$", vec_meta)

            vec_raw = func_features.get("bsim_features_raw", [])
            pipe.json().set(f"{base_func_key}:vec:raw", "$", vec_raw)
            
            # Add to batch-to-functions mapping SET (using base key)
            if batch_uuid:
                pipe.sadd(f"idx:{collection}:batch:{batch_uuid}:functions", base_func_key)

            vec_tf_list = func_features.get("bsim_features_tf", [])
            if vec_tf_list:
                zset_mapping = {item["hash"]: item["tf"] for item in vec_tf_list}
                pipe.zadd(f"{base_func_key}:vec:tf", zset_mapping)

            # --- Secondary Indexing ---
            save_function(pipe, collection, file_md5, addr, func_meta)
            
            pipe.execute()

        return True
