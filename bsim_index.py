import redis
import json
import time
import math
# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6666
BATCH_SIZE = 1000

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def clear_collection_index(collection):
    print(f"[*] Clearing all index data for: {collection}...")
    
    patterns = [
        f'{collection}:feature:*:functions',
        f'{collection}:features:by_tf',
        f'{collection}:feature:*:meta',
        f'{collection}:indexed:functions',
        f'{collection}:function:*:vec:norm'
    ]

    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break

def update_functions_incremental(collection, function_ids, label=None):
    """
    Precisely updates specific functions in the index, handling overwrites and cleanup.
    """

    label = label or collection
    total = len(function_ids)
    
    for i, func_id in enumerate(function_ids):
        # Progress bar (updates every 10 items or at completion)
        if (i + 1) % 10 == 0 or (i + 1) == total:
            pct = (i + 1) / total * 100
            bar_len = 30
            filled = int(bar_len * (i + 1) // total)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"\r    {label:<40}: [{bar}] {pct:>5.1f}% ({i+1}/{total})", end="", flush=True)

        # 1. Fetch current (pre-index) meta and features (these are the 'new' targets)
        meta_key = f"{func_id}:vec:meta"
        tf_key = f"{func_id}:vec:tf"
        
        raw_meta = r.json().get(meta_key, "$")
        if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1: raw_meta = raw_meta[0]
        
        new_tf_data = r.zrange(tf_key, 0, -1, withscores=True)
        if not raw_meta or not new_tf_data:
            print(f"  [!] Skipping {func_id}: Missing metadata or vector data.")
            continue

        # Note: We no longer track stale features individually per-function.
        # This reduces memory but means removed features in a re-bake leave ghost TF scores
        # in the global rank until a full rebuild.
        
        pipe = r.pipeline()
        
        # A. Recalculate L2 Norm
        sum_sq = sum(float(tf)**2 for _, tf in new_tf_data)
        pipe.set(f"{func_id}:vec:norm", math.sqrt(sum_sq))

        # B. Update Features
        for feat_item in raw_meta:
            f_hash = feat_item.get("hash")
            if not f_hash: continue
            
            new_tf = int(next((score for h, score in new_tf_data if h == f_hash), 0))
            
            # Get current TF in index to calculate DIFF for by_tf
            pipe.zscore(f"{collection}:feature:{f_hash}:functions", func_id)
            pipe.zadd(f"{collection}:feature:{f_hash}:functions", {func_id: new_tf})
            
            meta_entry = {
                "function_id": func_id,
                "type": feat_item.get("type"),
                "pcode_op": feat_item.get("pcode_op"),
                "pcode_op_full": feat_item.get("pcode_op_full"),
                "tf": new_tf,
                "seq": feat_item.get("seq"),
                "line_idx": feat_item.get("line_idx"),
                "addr_to_token_idx": feat_item.get("addr_to_token_idx"),
                "pcode_block": feat_item.get("pcode_block")
            }
            pipe.hset(f"{collection}:feature:{f_hash}:meta", func_id, json.dumps(meta_entry))

        # C. Execute and Handle ZINCRBY (Global TF adjustment)
        results = pipe.execute()
        
        pipe = r.pipeline()
        res_idx = 1 # skip norm set res
        
        # Handle updated by_tf with precise deltas
        for feat_item in raw_meta:
            f_hash = feat_item.get("hash")
            if not f_hash: continue
            new_tf = int(next((score for h, score in new_tf_data if h == f_hash), 0))
            
            old_tf = results[res_idx]
            diff = new_tf - (float(old_tf) if old_tf is not None else 0)
            if diff != 0:
                pipe.zincrby(f"{collection}:features:by_tf", diff, f_hash)
            res_idx += 3 # zscore + zadd + hset
        
        # 3. Mark function as indexed and map to batch
        pipe.sadd(f"{collection}:indexed:functions", func_id)
        pipe.json().set(f"{func_id}:vec:meta", "$.indexed", True)
        
        batch_uuid = raw_meta[0].get('batch_uuid') if isinstance(raw_meta, list) and raw_meta else raw_meta.get('batch_uuid')
        if batch_uuid:
            pipe.sadd(f"{collection}:batch:{batch_uuid}:functions", func_id)
        
        pipe.execute()
    
    print() # Newline after progress bar

def rebuild_single_collection(collection):
    print(f"[*] Rebuilding index for: {collection} batch-by-batch")
    
    batch_uuids = sorted(list(r.smembers("global:batches")))
    for uuid in batch_uuids:
        # Check if batch exists in this collection
        meta_key = f"{collection}:batch:{uuid}"
        name_raw = r.json().get(meta_key, "$.name")
        if not name_raw: continue
        
        name = name_raw[0] if isinstance(name_raw, list) else name_raw
        label = f"{str(name)[:20]} ({uuid[:8]}...)"
        
        func_ids = resolve_functions(collection, batch_uuid=uuid)
        if func_ids:
            update_functions_incremental(collection, func_ids, label=label)
    
    print(f"\n[+] {collection} indexed complete.")

def rebuild_all_collections():
    collections = list(r.smembers("global:collections"))
    if not collections:
        print("[!] No collections found in 'global:collections'")
        return

    print(f"[*] Found {len(collections)} collections: {collections}")
    
    overall_start = time.time()
    for coll in collections:
        clear_collection_index(coll)
        rebuild_single_collection(coll)
    
    print(f"\n[+---] ALL COLLECTIONS COMPLETE [---+]")
    print(f"Total processing time: {time.time() - overall_start:.2f}s")

def resolve_functions(collection, batch_uuid=None, md5=None, func_id=None):
    """Resolves CLI filters to a list of function IDs."""
    if func_id:
        return [f"{collection}:function:{func_id}"]
    
    if batch_uuid:
        # 1. Try robust cached SET
        batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
        if r.exists(batch_func_set):
            return list(r.smembers(batch_func_set))
        else:
            print(f"[*] Batch mapping missing for: {batch_uuid}")
            print(f"    (Use 'build {collection} --sync' to rebuild mappings for old data)")
            return []

    if md5:
        # 1. Direct key pattern (fast)
        cursor, keys = r.scan(0, match=f"{collection}:function:{md5}:*:meta", count=1000)
        found = [k.removesuffix(":meta") for k in keys]
        if found: return found
        
        # 2. Search fallback (for SHA256 in file_name or file_md5)
        try:
            from redis.commands.search.query import Query
            # Match any field (TEXT search)
            q = Query(f'"{md5}"').dialect(2).paging(0, 1000)
            res = r.ft(f"{collection}:idx:functions").search(q)
            if res.docs: return [d.id for d in res.docs]
        except: pass
        
        # 3. Slow scan fallback (check every meta)
        print(f"[*] Advanced scan for hash '{md5}'...")
        cursor = 0
        found = []
        while True:
            cursor, keys = r.scan(cursor, match=f"{collection}:function:*:meta", count=1000)
            if keys:
                pipe = r.pipeline()
                for k in keys: pipe.json().get(k, "$")
                contents = pipe.execute()
                for i, c_list in enumerate(contents):
                    # Flatten nested lists
                    c = c_list
                    while isinstance(c, list) and c: c = c[0]
                    
                    if not isinstance(c, dict): continue
                    # Check common fields for the match
                    if md5 in [c.get('file_md5'), c.get('file_name'), c.get('file_sha256')]:
                        found.append(keys[i].removesuffix(":meta"))
            if cursor == 0: break
            return found
    return []

def clear_functions_index(collection, func_ids):
    if not func_ids:
        print("[!] No functions specified for clear.")
        return
    print(f"[*] Clearing indexing data for {len(func_ids)} functions in {collection}...")
    
    # Process in batches to handle large clear operations
    for i in range(0, len(func_ids), 100):
        chunk = func_ids[i:i+100]
        # 1. Fetch current features and TFs for rank adjustment
        pipe = r.pipeline()
        for fid in chunk:
            pipe.json().get(f"{fid}:vec:meta", "$")
        results = pipe.execute()
        
        pipe = r.pipeline()
        for j, res_list in enumerate(results):
            fid = chunk[j]
            # Flatten Kvrocks list
            item = res_list
            while isinstance(item, list) and item: item = item[0]
            if not isinstance(item, dict): continue
            
            features = item.get('features', [])
            for feat in features:
                f_hash = feat.get('hash')
                tf = feat.get('tf', 1)
                if not f_hash: continue
                
                # Remove from inverted index and subtract from global rank
                pipe.zrem(f"{collection}:feature:{f_hash}:functions", fid)
                pipe.zincrby(f"{collection}:features:by_tf", -float(tf), f_hash)
                
                # Remove from feature details HASH
                pipe.hdel(f"{collection}:feature:{f_hash}:meta", fid)
            
            pipe.delete(f"{fid}:vec:norm")
            
            # Reset status flags
            pipe.srem(f"{collection}:indexed:functions", fid)
            pipe.json().set(f"{fid}:vec:meta", "$.indexed", False)
            
        pipe.execute()
    print(f"[+] Clearing complete.")
            
    return []

def rebuild_all_batch_mappings(collection):
    print(f"[*] Rebuilding batch-to-function mappings for {collection} (via scan)...")
    cursor = 0
    match_pattern = f"{collection}:function:*:*:meta"
    count = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=match_pattern, count=1000)
        if keys:
            pipe = r.pipeline()
            for k in keys: pipe.json().get(k, "$")
            res_batch = pipe.execute()
            
            pipe = r.pipeline()
            # Clear old mappings first if starting fresh? No, we'll just add.
            # Clear old mappings first if starting fresh? No, we'll just add.
            for i, res_list in enumerate(res_batch):
                # Flatten nested lists
                item = res_list
                while isinstance(item, list) and item: item = item[0]
                
                if isinstance(item, dict):
                    uuid = item.get("batch_uuid")
                    if uuid:
                        func_id = keys[i].removesuffix(":meta")
                        pipe.sadd(f"{collection}:batch:{uuid}:functions", func_id)
            pipe.execute()
            count += len(keys)
        if cursor == 0: break
    print(f"[+] Mapping complete: {count} functions processed.")

def list_missing_batches(collection, batch_filter=None):
    print(f"\n[*] Indexing Status for Collection: {collection}")
    if batch_filter:
        print(f"[*] Filtering for Batch: {batch_filter}")
    print(f"{'Batch UUID':<40} | {'Name':<30} | {'Src Funcs':<10} | {'Indexed':<8} | {'Ratio':<6}")
    print("-" * 105)
    
    batch_uuids = r.smembers("global:batches")
    indexed_set = f"{collection}:indexed:functions"
    
    found_any = False
    for uuid in sorted(list(batch_uuids)):
        if batch_filter and uuid != batch_filter:
            continue
            
        # 1. Key for the set of all function IDs belonging to this batch
        batch_func_set = f"{collection}:batch:{uuid}:functions"
        
        # 1. Start with metadata for instant reporting
        meta_key = f"{collection}:batch:{uuid}"
        name_raw = r.json().get(meta_key, "$")
        name = "N/A"
        total = 0
        if name_raw:
            if isinstance(name_raw, list): name_raw = name_raw[0]
            name = name_raw.get('name', 'N/A')
            total = name_raw.get('total_functions', 0)

        # 2. Try to get precise counts from SETs if they exist
        indexed = 0
        if r.exists(batch_func_set):
            found_any = True
            total = r.scard(batch_func_set) # Prefer precise SET count if mapped
            try:
                indexed = r.execute_command("SINTERCARD", "2", batch_func_set, indexed_set)
            except:
                indexed = len(r.sinter(batch_func_set, indexed_set))
        elif total > 0:
            found_any = True
            # If SET is missing but total is > 0, we can't calculate intersection easily without scan
            # We'll just show 0 indexed for now unless the user builds mappings
            pass
        else:
            # Batch not in this collection
            continue
        
        ratio = (indexed / total * 100) if total > 0 else 0
        print(f"{uuid:<40} | {str(name)[:30]:<30} | {total:<10} | {indexed:<8} | {ratio:>5.1f}%")

    if not found_any:
        print(f"[!] No batches found for collection {collection}.")

def bake_missing(collection):
    print(f"[*] Checking for missing functions in {collection}...")
    batch_uuids = sorted(list(r.smembers("global:batches")))
    indexed_set = f"{collection}:indexed:functions"
    
    for uuid in batch_uuids:
        batch_func_set = f"{collection}:batch:{uuid}:functions"
        if not r.exists(batch_func_set): continue
        
        # Get missing for THIS batch
        missing_ids = []
        try:
            missing_ids = list(r.sdiff(batch_func_set, indexed_set))
        except:
            pass
            
        if missing_ids:
            meta_key = f"{collection}:batch:{uuid}"
            name_raw = r.json().get(meta_key, "$.name")
            name = name_raw[0] if (isinstance(name_raw, list) and name_raw) else (name_raw or "Unknown")
            label = f"{str(name)[:30]}" # Just the name for clarity
            
            update_functions_incremental(collection, missing_ids, label=f"{label} ({uuid[:8]}...)")
    
    print(f"\n[+] Bake complete.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BSimVis Index Management Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Status command (Instant)
    status_parser = subparsers.add_parser("status", help="Show indexing status (instant)")
    status_parser.add_argument("collection", help="Collection name")
    status_parser.add_argument("--batch", help="Filter for a specific batch UUID")

    # Build command
    build_parser = subparsers.add_parser("build", help="Index functions (defaults to missing)")
    build_parser.add_argument("collection", help="Collection name")
    build_parser.add_argument("--batch", help="Index a specific batch UUID")
    build_parser.add_argument("--all", action="store_true", help="Clear and rebuild everything")
    build_parser.add_argument("--sync", action="store_true", help="Sync batch mappings (scan)")
    build_parser.add_argument("--md5", help="Index functions for a specific file (MD5 or SHA256)")

    # Rebuild command (Shortcut for clear + build)
    rebuild_parser = subparsers.add_parser("rebuild", help="Clear and rebuild (all or targeted)")
    rebuild_parser.add_argument("collection", help="Collection name")
    rebuild_parser.add_argument("--batch", help="Rebuild a specific batch UUID")
    rebuild_parser.add_argument("--md5", help="Rebuild a specific file (MD5 or SHA256)")

    # Clear command
    clear_parser = subparsers.add_parser("clear", help="Remove indexing data")
    clear_parser.add_argument("collection", help="Collection name")
    clear_group = clear_parser.add_mutually_exclusive_group(required=True)
    clear_group.add_argument("--batch", help="Clear a specific batch UUID")
    clear_group.add_argument("--all", action="store_true", help="Clear everything in the collection")
    clear_parser.add_argument("--md5", help="Clear functions for a specific file (MD5 or SHA256)")

    args = parser.parse_args()
    coll = args.collection

    if args.command == "status":
        list_missing_batches(coll, batch_filter=args.batch)
        
    elif args.command == "build":
        if args.all:
            clear_collection_index(coll)
            rebuild_single_collection(coll)
        elif args.sync:
            rebuild_all_batch_mappings(coll)
        elif args.batch or args.md5:
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5)
            if func_ids:
                update_functions_incremental(coll, func_ids)
            else:
                print(f"[!] No functions found to build.")
        else:
            # Default: Index missing
            bake_missing(coll)
            
    elif args.command == "rebuild":
        if args.batch or args.md5:
            # Targeted rebuild: clear then build
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5)
            if func_ids:
                clear_functions_index(coll, func_ids)
                update_functions_incremental(coll, func_ids)
            else:
                print(f"[!] No functions found to rebuild.")
        else:
            # Full rebuild
            clear_collection_index(coll)
            rebuild_single_collection(coll)

    elif args.command == "clear":
        if args.all:
            clear_collection_index(coll)
        elif args.batch or args.md5:
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5)
            if func_ids:
                clear_functions_index(coll, func_ids)
            else:
                print(f"[!] No functions found to clear.")