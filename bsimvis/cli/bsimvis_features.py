import redis
import json
import time
import math
import sys
import os

# Allow running this file directly from CLI
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
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
        f"idx:{collection}:feature:*:functions",
        f"idx:{collection}:features:by_tf",
        f"idx:{collection}:feature:*:meta",
        f"idx:{collection}:indexed:functions",
        f"{collection}:function:*:vec:norm",
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
            print(
                f"\r    {label:<40}: [{bar}] {pct:>5.1f}% ({i+1}/{total})",
                end="",
                flush=True,
            )

        # 1. Fetch current (pre-index) meta and features (these are the 'new' targets)
        meta_key = f"{func_id}:vec:meta"
        tf_key = f"{func_id}:vec:tf"

        raw_meta = r.json().get(meta_key, "$")
        if isinstance(raw_meta, list) and raw_meta and len(raw_meta) == 1:
            raw_meta = raw_meta[0]

        new_tf_data = r.zrange(tf_key, 0, -1, withscores=True)
        if not raw_meta or not new_tf_data:
            print(f"  [!] Skipping {func_id}: Missing metadata or vector data.")
            r.sadd(f"idx:{collection}:indexed:functions", func_id)
            continue

        pipe = r.pipeline()

        # A. Recalculate L2 Norm
        sum_sq = sum(float(tf) ** 2 for _, tf in new_tf_data)
        pipe.set(f"{func_id}:vec:norm", math.sqrt(sum_sq))

        # B. Update Features
        for feat_item in raw_meta:
            f_hash = feat_item.get("hash")
            if not f_hash:
                continue

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
                "pcode_block": feat_item.get("pcode_block"),
            }
            pipe.hset(
                f"idx:{collection}:feature:{f_hash}:meta",
                func_id,
                json.dumps(meta_entry),
            )

        # C. Execute and Handle ZINCRBY (Global TF adjustment)
        results = pipe.execute()

        pipe = r.pipeline()
        res_idx = 1  # skip norm set res

        # Handle updated by_tf with precise deltas
        for feat_item in raw_meta:
            f_hash = feat_item.get("hash")
            if not f_hash:
                continue
            new_tf = int(next((score for h, score in new_tf_data if h == f_hash), 0))

            old_tf = results[res_idx]
            diff = new_tf - (float(old_tf) if old_tf is not None else 0)
            if diff != 0:
                pipe.zincrby(f"idx:{collection}:features:by_tf", diff, f_hash)
            res_idx += 3  # zscore + zadd + hset

        # 3. Mark function as indexed and map to batch
        pipe.sadd(f"idx:{collection}:indexed:functions", func_id)
        pipe.execute()

    print()  # Newline after progress bar


def rebuild_single_collection(collection, r=None):
    r = r or get_redis()
    print(f"[*] Rebuilding index for: {collection} batch-by-batch")

    batch_uuids = sorted(list(r.smembers("global:batches")))
    for uuid in batch_uuids:
        # Check if batch exists in this collection
        meta_key = f"{collection}:batch:{uuid}"
        name_raw = r.json().get(meta_key, "$.name")
        if not name_raw:
            continue

        name = name_raw[0] if isinstance(name_raw, list) else name_raw
        label = f"{str(name)[:20]} ({uuid[:8]}...)"

        func_ids = resolve_functions(collection, batch_uuid=uuid)
        if func_ids:
            update_functions_incremental(collection, func_ids, label=label)

    print(f"\n[+] {collection} indexed complete.")


def rebuild_all_collections():
    r = get_redis()
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
            print(
                f"    (Use 'build {collection} --sync' to rebuild mappings for old data)"
            )
            return []

    if md5:
        # 1. Direct key pattern (fast)
        cursor, keys = r.scan(
            0, match=f"{collection}:function:{md5}:*:meta", count=1000
        )
        found = [k.removesuffix(":meta") for k in keys]
        if found:
            return found

        # 2. Search fallback (for SHA256 in file_name or file_md5)
        try:
            from redis.commands.search.query import Query

            # Match any field (TEXT search)
            q = Query(f'"{md5}"').dialect(2).paging(0, 1000)
            res = r.ft(f"idx:{collection}:idx:functions").search(q)
            if res.docs:
                return [d.id for d in res.docs]
        except:
            pass

        # 3. Slow scan fallback (check every meta)
        print(f"[*] Advanced scan for hash '{md5}'...")
        cursor = 0
        found = []
        while True:
            cursor, keys = r.scan(
                cursor, match=f"{collection}:function:*:meta", count=1000
            )
            if keys:
                pipe = r.pipeline()
                for k in keys:
                    pipe.json().get(k, "$")
                contents = pipe.execute()
                for i, c_list in enumerate(contents):
                    # Flatten nested lists
                    c = c_list
                    while isinstance(c, list) and c:
                        c = c[0]

                    if not isinstance(c, dict):
                        continue
                    # Check common fields for the match
                    if md5 in [
                        c.get("file_md5"),
                        c.get("file_name"),
                        c.get("file_sha256"),
                    ]:
                        found.append(keys[i].removesuffix(":meta"))
            if cursor == 0:
                break
        return found
    return []


def clear_functions_index(collection, func_ids, r=None):
    r = r or get_redis()
    if not func_ids:
        print("[!] No functions specified for clear.")
        return
    print(
        f"[*] Clearing indexing data for {len(func_ids)} functions in {collection}..."
    )

    # Process in batches to handle large clear operations
    for i in range(0, len(func_ids), 100):
        chunk = func_ids[i : i + 100]
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
            while isinstance(item, list) and item:
                item = item[0]
            if not isinstance(item, dict):
                continue

            features = item.get("features", [])
            for feat in features:
                f_hash = feat.get("hash")
                tf = feat.get("tf", 1)
                if not f_hash:
                    continue

                # Remove from inverted index and subtract from global rank
                pipe.zrem(f"idx:{collection}:feature:{f_hash}:functions", fid)
                pipe.zincrby(f"idx:{collection}:features:by_tf", -float(tf), f_hash)

                # Remove from feature details HASH
                pipe.hdel(f"idx:{collection}:feature:{f_hash}:meta", fid)

            pipe.delete(f"{fid}:vec:norm")

            # Reset status flags
            pipe.srem(f"idx:{collection}:indexed:functions", fid)

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
            for k in keys:
                pipe.json().get(k, "$")
            res_batch = pipe.execute()

            pipe = r.pipeline()
            for i, res_list in enumerate(res_batch):
                # Flatten nested lists
                item = res_list
                while isinstance(item, list) and item:
                    item = item[0]

                if isinstance(item, dict):
                    uuid = item.get("batch_uuid")
                    if uuid:
                        func_id = keys[i].removesuffix(":meta")
                        pipe.sadd(f"{collection}:batch:{uuid}:functions", func_id)
            pipe.execute()
            count += len(keys)
        if cursor == 0:
            break
    print(f"[+] Mapping complete: {count} functions processed.")


def list_missing_batches(collection, batch_filter=None, r=None):
    r = r or get_redis()
    print(f"\n[*] Indexing Status for Collection: {collection}")
    if batch_filter:
        print(f"[*] Filtering for Batch: {batch_filter}")
    print(
        f"{'Batch UUID':<40} | {'Name':<30} | {'Src Funcs':<10} | {'Indexed':<8} | {'Ratio':<6}"
    )
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
            if isinstance(name_raw, list):
                name_raw = name_raw[0]
            name = name_raw.get("name", "N/A")
            total = name_raw.get("total_functions", 0)

        # 2. Try to get precise counts from SETs if they exist
        indexed = 0
        if r.exists(batch_func_set):
            found_any = True
            total = r.scard(batch_func_set)  # Prefer precise SET count if mapped
            try:
                indexed = r.execute_command(
                    "SINTERCARD", "2", batch_func_set, indexed_set
                )
            except:
                indexed = len(r.sinter(batch_func_set, indexed_set))
        elif total > 0:
            found_any = True
            pass
        else:
            # Batch not in this collection
            continue

        ratio = (indexed / total * 100) if total > 0 else 0
        print(
            f"{uuid:<40} | {str(name)[:30]:<30} | {total:<10} | {indexed:<8} | {ratio:>5.1f}%"
        )

    if not found_any:
        print(f"[!] No batches found for collection {collection}.")


def bake_missing(collection, r=None):
    r = r or get_redis()
    print(f"[*] Checking for missing functions in {collection}...")
    batch_uuids = sorted(list(r.smembers("global:batches")))
    indexed_set = f"idx:{collection}:indexed:functions"

    for uuid in batch_uuids:
        batch_func_set = f"{collection}:batch:{uuid}:functions"
        if not r.exists(batch_func_set):
            continue

        # Get missing for THIS batch
        missing_ids = []
        try:
            missing_ids = list(r.sdiff(batch_func_set, indexed_set))
        except:
            pass

        if missing_ids:
            meta_key = f"{collection}:batch:{uuid}"
            name_raw = r.json().get(meta_key, "$.name")
            name = (
                name_raw[0]
                if (isinstance(name_raw, list) and name_raw)
                else (name_raw or "Unknown")
            )
            label = f"{str(name)[:30]}"  # Just the name for clarity

            update_functions_incremental(
                collection, missing_ids, label=f"{label} ({uuid[:8]}...)"
            )

    print(f"\n[+] Bake complete.")


def index_quick_status(collection, batch_uuid=None, r=None):
    r = r or get_redis()
    indexed_set = f"idx:{collection}:indexed:functions"

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
    (Set/ZSet) indexes using index_service.
    """
    r = r or get_redis()
    print(f"[*] Rebuilding secondary indexes for: {collection}")

    # --- Files ---
    file_count = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"{collection}:file:*:meta", count=500
        )
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
        cursor, keys = r.scan(
            cursor=cursor, match=f"{collection}:function:*:*:meta", count=500
        )
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
        cursor, keys = r.scan(
            cursor=cursor, match=f"{collection}:sim_meta:*:*:*", count=500
        )
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

                # Key: {collection}:sim_meta:{algo}:{id1}:{id2}
                parts = k.split(":")
                if len(parts) >= 4:
                    algo = parts[2]
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
