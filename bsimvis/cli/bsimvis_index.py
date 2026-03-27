import redis
import json
import time
import math
import sys
import os

# Allow running this file directly from CLI
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from bsimvis.app.services.index_service import save_file, save_function, save_similarity

_r = None

def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r

def clear_collection_index(collection, r=None):
    r = r or get_redis()
    print(f"[*] Clearing all index data for: {collection}...")
    
    patterns = [
        f'idx:{collection}:feature:*:functions',
        f'idx:{collection}:features:by_tf',
        f'idx:{collection}:feature:*:meta',
        f'idx:{collection}:indexed:functions',
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

def update_functions_incremental(collection, function_ids, label=None, r=None):
    r = r or get_redis()
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
            r.sadd(f"idx:{collection}:indexed:functions", func_id)
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
            pipe.zscore(f"idx:{collection}:feature:{f_hash}:functions", func_id)
            pipe.zadd(f"idx:{collection}:feature:{f_hash}:functions", {func_id: new_tf})
            
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
            pipe.hset(f"idx:{collection}:feature:{f_hash}:meta", func_id, json.dumps(meta_entry))

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
                pipe.zincrby(f"idx:{collection}:features:by_tf", diff, f_hash)
            res_idx += 3 # zscore + zadd + hset
        
        # 3. Mark function as indexed and map to batch
        pipe.sadd(f"idx:{collection}:indexed:functions", func_id)
        #pipe.json().set(f"{func_id}:vec:meta", "$.indexed", True)
        
        #batch_uuid = raw_meta[0].get('batch_uuid') if isinstance(raw_meta, list) and raw_meta else raw_meta.get('batch_uuid')
        #if batch_uuid:
        #    pipe.sadd(f"{collection}:batch:{batch_uuid}:functions", func_id)
        
        pipe.execute()
    
    print() # Newline after progress bar

def rebuild_single_collection(collection, r=None):
    r = r or get_redis()
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

def resolve_functions(collection, batch_uuid=None, md5=None, func_id=None, r=None):
    r = r or get_redis()
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
            res = r.ft(f"idx:{collection}:idx:functions").search(q)
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

def clear_functions_index(collection, func_ids, r=None):
    r = r or get_redis()
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
                pipe.zrem(f"idx:{collection}:feature:{f_hash}:functions", fid)
                pipe.zincrby(f"idx:{collection}:features:by_tf", -float(tf), f_hash)
                
                # Remove from feature details HASH
                pipe.hdel(f"idx:{collection}:feature:{f_hash}:meta", fid)
            
            pipe.delete(f"{fid}:vec:norm")
            
            # Reset status flags
            pipe.srem(f"idx:{collection}:indexed:functions", fid)
            #pipe.json().set(f"{fid}:vec:meta", "$.indexed", False)
            
        pipe.execute()
    print(f"[+] Clearing complete.")
            
    return []

def rebuild_all_batch_mappings(collection, r=None):
    r = r or get_redis()
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

def list_missing_batches(collection, batch_filter=None, r=None):
    r = r or get_redis()
    print(f"\n[*] Indexing Status for Collection: {collection}")
    if batch_filter:
        print(f"[*] Filtering for Batch: {batch_filter}")
    print(f"{'Batch UUID':<40} | {'Name':<30} | {'Src Funcs':<10} | {'Indexed':<8} | {'Ratio':<6}")
    print("-" * 105)
    
    batch_uuids = r.smembers("global:batches")
    indexed_set = f"idx:{collection}:indexed:functions"
    
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

def bake_missing(collection, r=None):
    r = r or get_redis()
    print(f"[*] Checking for missing functions in {collection}...")
    batch_uuids = sorted(list(r.smembers("global:batches")))
    indexed_set = f"idx:{collection}:indexed:functions"
    
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

def index_quick_status(collection, batch_uuid=None, r=None):
    r = r or get_redis()
    indexed_set = f"idx:{collection}:indexed:functions"
    
    total = 0
    indexed = 0
    
    batch_uuids = [batch_uuid] if batch_uuid else list(r.smembers("global:batches"))
    for b_uuid in batch_uuids:
        batch_func_set = f"{collection}:batch:{b_uuid}:functions"
        if not r.exists(batch_func_set): continue
        
        b_total = r.scard(batch_func_set)
        total += b_total
        try:
            b_indexed = r.execute_command("SINTERCARD", "2", batch_func_set, indexed_set)
        except:
            b_indexed = len(r.sinter(batch_func_set, indexed_set))
        indexed += b_indexed
        
    if total == 0:
        print("[!] No data found for this collection.")
    elif indexed == total:
        print(f"OK : 0 unindexed / {total} total")
    else:
        print(f"{total - indexed} unindexed / {total} total")

def reindex_secondary(collection, r=None):
    """
    Scan all existing :meta JSON keys and rebuild the secondary
    (Set/ZSet) indexes using index_service.  Run this once after
    migrating from FT.SEARCH to the manual index system.
    """
    r = r or get_redis()
    print(f"[*] Rebuilding secondary indexes for: {collection}")

    # --- Files ---
    file_count = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"{collection}:file:*:meta", count=500)
        if keys:
            pipe = r.pipeline()
            for k in keys:
                pipe.json().get(k, "$")
            results = pipe.execute()

            pipe = r.pipeline()
            for k, res in zip(keys, results):
                if not res:
                    continue
                data = res[0] if isinstance(res, list) and res else res
                if not isinstance(data, dict):
                    continue
                md5 = data.get("file_md5")
                if md5:
                    save_file(pipe, collection, md5, data)
                    file_count += 1
            pipe.execute()
        if cursor == 0:
            break
    print(f"    Files reindexed: {file_count}")

    # --- Functions ---
    func_count = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"{collection}:function:*:*:meta", count=500)
        if keys:
            pipe = r.pipeline()
            for k in keys:
                pipe.json().get(k, "$")
            results = pipe.execute()

            pipe = r.pipeline()
            for k, res in zip(keys, results):
                if not res:
                    continue
                data = res[0] if isinstance(res, list) and res else res
                if not isinstance(data, dict):
                    continue
                md5  = data.get("file_md5")
                addr = data.get("entrypoint_address")
                if md5 and addr:
                    save_function(pipe, collection, md5, addr, data)
                    func_count += 1
            pipe.execute()
        if cursor == 0:
            break
    print(f"    Functions reindexed: {func_count}")

    # --- Similarities ---
    sim_count = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"{collection}:sim_meta:*:*:*", count=500)
        if keys:
            pipe = r.pipeline()
            # Keys are typically {collection}:sim_meta:{algo}:{id1}:{id2}
            for k in keys:
                pipe.json().get(k, "$")
            results = pipe.execute()

            pipe = r.pipeline()
            for k, res in zip(keys, results):
                if not res:
                    continue
                data = res[0] if isinstance(res, list) and res else res
                if not isinstance(data, dict):
                    continue
                
                # Use a dummy sim_id or extract it from key
                # The save_similarity needs a sim_id
                # Key: {collection}:sim_meta:{algo}:{id1}:{id2}
                parts = k.split(':')
                if len(parts) >= 4:
                    algo = parts[2]
                    # id1 and id2 might contain colons too, but they are at the end
                    # Actually id1 is {coll}:function:...
                    # So it's easier to just use the data
                    id1 = data.get("id1")
                    id2 = data.get("id2")
                    if id1 and id2:
                        sim_id = f"{algo}:{id1}:{id2}"
                        save_similarity(pipe, collection, sim_id, data)
                        sim_count += 1
            pipe.execute()
        if cursor == 0:
            break
    print(f"    Similarities reindexed: {sim_count}")
    print(f"[+] Secondary index rebuild complete for {collection}.")


def run_features(host, port, args):
    r = get_redis(host, port)
    coll = args.collection
    
    if args.action == "status":
        index_quick_status(coll, batch_uuid=args.batch, r=r)
    elif args.action == "list":
        list_missing_batches(coll, batch_filter=args.batch, r=r)
    elif args.action == "reindex":
        reindex_secondary(coll, r=r)
    elif args.action == "build":
        if args.all:
            clear_collection_index(coll, r=r)
            rebuild_single_collection(coll, r=r)
        elif args.sync:
            rebuild_all_batch_mappings(coll, r=r)
        elif args.batch or args.md5:
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5, r=r)
            if func_ids:
                update_functions_incremental(coll, func_ids, r=r)
            else:
                print(f"[!] No functions found to build.")
        else:
            bake_missing(coll, r=r)
    elif args.action == "rebuild":
        if args.batch or args.md5:
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5, r=r)
            if func_ids:
                clear_functions_index(coll, func_ids, r=r)
                update_functions_incremental(coll, func_ids, r=r)
            else:
                print(f"[!] No functions found to rebuild.")
        else:
            clear_collection_index(coll, r=r)
            rebuild_single_collection(coll, r=r)
    elif args.action == "clear":
        if args.all:
            clear_collection_index(coll, r=r)
        elif args.batch or args.md5:
            func_ids = resolve_functions(coll, batch_uuid=args.batch, md5=args.md5, r=r)
            if func_ids:
                clear_functions_index(coll, func_ids, r=r)
            else:
                print(f"[!] No functions found to clear.")

def run_index_status(host, port, args):
    r = get_redis(host, port)
    coll = args.collection
    
    print(f"\n[*] Index Status for Collection: {coll}")
    
    # 1. Core Counts
    num_files = r.scard(f"idx:{coll}:all_files")
    num_funcs = r.scard(f"idx:{coll}:all_functions")
    num_indexed = r.scard(f"idx:{coll}:indexed:functions")
    num_unique_features = r.zcard(f"idx:{coll}:features:by_tf")
    
    print(f"    - Files           : {num_files}")
    print(f"    - Functions       : {num_funcs} ({num_indexed} indexed / {num_funcs - num_indexed} missing)")
    print(f"    - Unique Features : {num_unique_features}")
    
    if not args.details:
        print(f"\n[i] Use --details for comprehensive space and size analysis.")
        return

    print(f"\n[+] Detailed Index Analysis (Approximate):")
    
    # 2. Component Breakdown
    patterns = [
        ("File Meta", f"{coll}:file:*:meta"),
        ("Func Meta", f"{coll}:function:*:*:meta"),
        ("Func Source", f"{coll}:function:*:*:source"),
        ("Func Vector (TF)", f"{coll}:function:*:*:vec:tf"),
        ("Inverted Index", f"idx:{coll}:feature:*:functions"),
        ("Feature Meta", f"idx:{coll}:feature:*:meta"),
        ("Tag Index", f"idx:{coll}:*:*:*"),
    ]
    
    def estimate_total_keys(pattern):
        # We try to avoid a full SCAN if possible.
        if "file:*:meta" in pattern: return num_files
        if "function:*:*:meta" in pattern: return num_funcs
        if "function:*:*:source" in pattern: return num_funcs
        if "function:*:*:vec:tf" in pattern: return num_funcs
        if "feature:*:functions" in pattern: return num_unique_features
        if "feature:*:meta" in pattern: return num_unique_features
        
        # For tags, we might need a quick scan to estimate.
        cursor = 0
        count = 0
        # Quick sample scan of 1000
        cursor, keys = r.scan(0, match=pattern, count=1000)
        return len(keys) # Very rough estimate if it's > 1000

    print(f"    {'Component':<20} | {'Key Pattern':<40} | {'Count':<10}")
    print("-" * 80)
    for name, pat in patterns:
        count = estimate_total_keys(pat)
        if count > 0:
            print(f"    {name:<20} | {pat:<40} | {count:<10}")

    # 3. Space Estimation (Deep Sampling)
    print(f"\n[+] Space Estimation (Deep Sampling):")
    
    def get_key_count(k):
        """Unified cardinality check."""
        try:
            rtype = r.type(k).lower()
            if "zset" in rtype: return r.zcard(k)
            if "set" in rtype: return r.scard(k)
            if "list" in rtype: return r.llen(k)
            if "hash" in rtype: return r.hlen(k)
        except: pass
        return 0

    def get_key_size(k):
        """Unified size estimator for different redis types in Kvrocks."""
        try:
            # 1. Try MEMORY USAGE (Best)
            size = r.execute_command("MEMORY", "USAGE", k)
            if size: return size
        except: pass
        
        try:
            # 2. Fallback to Type-specific estimation
            rtype = r.type(k).lower()
            if rtype == "string": return r.strlen(k)
            if rtype == "list": return r.llen(k) * 100 # Approx
            if rtype == "set": return r.scard(k) * 40 # Approx
            if rtype == "zset": return r.zcard(k) * 50 # Approx
            if rtype == "hash": return r.hlen(k) * 150 # Approx
            if "rejson" in rtype or "json" in rtype:
                 val = r.execute_command("JSON.GET", k)
                 return len(str(val)) if val is not None else 0
        except Exception: pass
        return 0

    def estimate_group_size(pattern, count_total, tracking_set=None, key_formatter=None):
        sample_size = 10
        if count_total == 0: return 0
        
        found_keys = []
        if tracking_set:
            try:
                tset_type = r.type(tracking_set).lower()
                if "zset" in tset_type:
                    items = r.zrandmember(tracking_set, sample_size)
                else:
                    items = r.srandmember(tracking_set, sample_size)
                
                if items:
                    if key_formatter:
                        found_keys = [key_formatter(i) for i in items]
                    else:
                        found_keys = items
            except Exception: pass
        
        if not found_keys:
            # Fallback to SCAN for keys without a tracking set
            cursor = 0
            for _ in range(30): 
                cursor, keys = r.scan(cursor, match=pattern, count=2000)
                found_keys.extend([k for k in keys if k not in found_keys])
                if len(found_keys) >= sample_size or cursor == 0:
                    break
        
        if not found_keys: return 0
        sample = found_keys[:sample_size]
        total_size = 0
        actual_samples = 0
        for k in sample:
            sz = get_key_size(k)
            if sz > 0:
                total_size += sz
                actual_samples += 1
        return (total_size / actual_samples) if actual_samples > 0 else 0

    stats_config = [
        ("File Meta", f"{coll}:file:*:meta", num_files, f"idx:{coll}:all_files", None),
        ("Func Meta", f"{coll}:function:*:*:meta", num_funcs, f"idx:{coll}:all_functions", None),
        ("Func Source", f"{coll}:function:*:*:source", num_funcs, f"idx:{coll}:all_functions", lambda x: str(x).replace(":meta", ":source")),
        ("Inverted Index", f"idx:{coll}:feature:*:functions", num_unique_features, f"idx:{coll}:features:by_tf", lambda x: f"idx:{coll}:feature:{x}:functions"),
        ("Feature Meta", f"idx:{coll}:feature:*:meta", num_unique_features, f"idx:{coll}:features:by_tf", lambda x: f"idx:{coll}:feature:{x}:meta"),
    ]

    total_est = 0
    print(f"    {'Component':<20} | {'Avg Size':<12} | {'Total Est.':<12}")
    print("-" * 60)
    
    def format_bytes(b):
        if b < 0.1: return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB']:
            if b < 1024: return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    for name, pat, count, tset, kform in stats_config:
        avg = estimate_group_size(pat, count, tracking_set=tset, key_formatter=kform)
        comp_total = avg * count
        total_est += comp_total
        print(f"    {name:<20} | {format_bytes(avg):<12} | {format_bytes(comp_total):<12}")

    print("-" * 60)
    print(f"    {'ESTIMATED TOTAL':<20} | {'':<12} | {format_bytes(total_est):<12}")
    
    # 4. Secondary Index Analysis (Detailed breakdown of tags, names, etc.)
    print(f"\n[+] Secondary Index Breakdown (Tags, Fields):")
    
    sec_indexes = {} # (doc_type, field) -> list of keys
    # Targeted scans to avoid being drowned out by high-cardinality features
    scan_patterns = [
        f"idx:{coll}:file:*:*",
        f"idx:{coll}:function:*:*",
        f"idx:{coll}:feature:*:functions",
        f"idx:{coll}:feature:*:meta",
        f"idx:{coll}:sim:*:*",
        f"idx:{coll}:baked:*:*",
    ]
    
    for sec_pattern in scan_patterns:
        cursor = 0
        for _ in range(50): 
            cursor, keys = r.scan(cursor, match=sec_pattern, count=1000)
            for k in keys:
                parts = k.split(':')
                if len(parts) >= 5:
                    if parts[2] == "feature": group = (parts[2], parts[4])
                    else: group = (parts[2], parts[3])
                    
                    if group not in sec_indexes: sec_indexes[group] = []
                    if len(sec_indexes[group]) < 50:
                        sec_indexes[group].append(k)
            if cursor == 0: break

    print(f"    {'Doc':<10} | {'Field':<18} | {'Type':<8} | {'Unique Vals':<12} | {'Avg Docs':<10} | {'Est. Space':<12} | {'Pattern'}")
    print("-" * 140)
    
    total_sec_est = 0
    for (dtype, field), samples in sec_indexes.items():
        # Get actual redis type for one of the keys
        sample_key = samples[0]
        rtype = r.type(sample_key).lower()
        
        # Representative pattern
        if dtype == "feature":
            rep_pat = f"idx:{coll}:feature:*:{field}"
        else:
            rep_pat = f"idx:{coll}:{dtype}:{field}:*"

        # 1. Use known counts for features if available
        real_val_count = len(samples)
        if dtype == "feature" and (field == "functions" or field == "meta"):
            real_val_count = num_unique_features
            val_count_str = str(real_val_count)
        elif dtype == "function" and field == "entrypoint_address":
            real_val_count = num_funcs
            val_count_str = str(real_val_count)
        elif dtype == "sim" and field == "scoreboard":
            val_count_str = f"~{num_funcs}"
            real_val_count = num_funcs
        else:
            if len(samples) >= 50:
                inner_cursor = 0
                count_acc = 0
                for _ in range(20):
                    inner_cursor, inner_keys = r.scan(inner_cursor, match=f"idx:{coll}:{dtype}:{field}:*", count=2000)
                    count_acc += len(inner_keys)
                    if inner_cursor == 0: break
                
                real_val_count = count_acc
                val_count_str = str(real_val_count) if inner_cursor == 0 else f">{real_val_count}"
            else:
                val_count_str = str(real_val_count)

        # Average docs per value
        total_docs = 0
        total_bytes = 0
        for sk in samples[:10]:
            total_docs += get_key_count(sk)
            total_bytes += get_key_size(sk)
        
        avg_docs = total_docs / min(len(samples), 10)
        avg_bytes = total_bytes / min(len(samples), 10)
        est_total = avg_bytes * real_val_count
        total_sec_est += est_total

        print(f"    {dtype:<10} | {field:<18} | {rtype:<8} | {val_count_str:<12} | {avg_docs:<10.1f} | {format_bytes(est_total):<12} | {rep_pat}")

    print("-" * 140)
    print(f"    {'TOTAL SECONDARY INDEX':<54} | {format_bytes(total_sec_est):<12}")

    # 5. Collection Efficiency
    if num_indexed > 0 and num_unique_features > 0:
        avg_feats = r.zcard(f"idx:{coll}:features:by_tf") # Already have it
        # Total TF / Num Funcs
        # This is harder to get without ZSCORE on everything.
        print(f"\n[+] Index Density:")
        print(f"    - Unique Features per Function : {num_unique_features / num_indexed:.2f}")

    print(f"\n[i] Note: Calculations are estimates based on sampling 10 keys per component.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="BSimVis Features Management Tool (Indexing)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Status command (Instant)
    status_parser = subparsers.add_parser("status", help="Show indexing status (instant)")
    status_parser.add_argument("collection", help="Collection name")
    status_parser.add_argument("--batch", help="Filter for a specific batch UUID")

    # Reindex command – rebuild secondary (Set/ZSet) indexes from existing data
    reindex_parser = subparsers.add_parser("reindex", help="Rebuild secondary indexes from existing data")
    reindex_parser.add_argument("collection", help="Collection name")

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

    elif args.command == "reindex":
        reindex_secondary(coll)

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

if __name__ == "__main__":
    main()