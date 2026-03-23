import redis
import json
import time
import math
# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6667
BATCH_SIZE = 1000

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def clear_collection_index(collection):
    print(f"[*] Clearing index, norms, and feature-meta for: {collection}...")
    patterns = [
        f'{collection}:feature:*:functions', 
        f'{collection}:feature:*:meta', 
        f'{collection}:features:by_tf',
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

def rebuild_single_collection(collection):
    print(f"[*] Rebuilding index and feature metadata for: {collection}")
    cursor = 0
    processed_count = 0
    start_time = time.time()

    # Pattern targets the metadata JSON for each function
    match_pattern = f"{collection}:function:*:*:vec:meta"

    while True:
        cursor, keys = r.scan(cursor=cursor, match=match_pattern, count=BATCH_SIZE)
        if not keys:
            if cursor == 0: break
            continue

        pipe = r.pipeline()
        for key in keys:
            # key: "coll:function:md5:addr:vec:meta"
            base_func_id = key.removesuffix(":vec:meta")
            
            # 1. Fetch Metadata and TF ZSet
            # We need both: Meta for the feature details, TF for the L2 Norm
            raw_meta = r.json().get(key)
            tf_data = r.zrange(f"{base_func_id}:vec:tf", 0, -1, withscores=True)
            
            if not raw_meta or not tf_data:
                continue

            # 2. Calculate L2 Norm (Similarity math)
            sum_sq = sum(float(tf)**2 for _, tf in tf_data)
            pipe.set(f"{base_func_id}:vec:norm", math.sqrt(sum_sq))

            # 3. Process Features for Inverted Index and Metadata Store
            for feat_item in raw_meta:
                f_hash = feat_item.get("hash")
                if not f_hash: continue

                # A. Inverted Index (Which functions have this feature?)
                pipe.sadd(f"{collection}:feature:{f_hash}:functions", base_func_id)

                # B. Feature Metadata (What does this feature look like in this collection?)
                # We use JSON.ARRAPPEND to store every occurrence of this feature
                # We also inject the base_func_id so we know which function this specific meta belongs to
                feat_meta_key = f"{collection}:feature:{f_hash}:meta"
                
                # Ensure the JSON array exists before appending
                pipe.json().set(feat_meta_key, '$', [], nx=True)
                
                # Prepare the meta object for storage
                meta_entry = {
                    "function-id": base_func_id,
                    "type": feat_item.get("type"),
                    "pcode-op": feat_item.get("pcode-op"),
                    "pcode-op-full": feat_item.get("pcode-op-full"),
                    "tf": int(next((score for h, score in tf_data if h == f_hash), 0)),
                    "line-idx": feat_item.get("line-idx"),
                    "addr-to-token-idx": feat_item.get("addr-to-token-idx"),
                    "pcode-block": feat_item.get("pcode-block")
                }
                pipe.json().arrappend(feat_meta_key, '$', meta_entry)
                
                # C. Global Feature Ranking (by TF)
                pipe.zincrby(f"{collection}:features:by_tf", meta_entry["tf"], f_hash)

            processed_count += 1
            if processed_count % 200 == 0:
                pipe.execute()
                print(f"  [i] {collection}: Processed {processed_count} functions...")
        
        pipe.execute()
        if cursor == 0: break

    print(f"[+] {collection} indexed in {time.time() - start_time:.2f}s")

def rebuild_all_collections():
    """
    Fetches the list of collections from Redis and iterates through them.
    """
    collections = r.smembers("global:collections")
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build BSimVis Index")
    parser.add_argument("-c", "--collection", help="Specific collection to rebuild", type=str)
    parser.add_argument("--all", action="store_true", help="Rebuild all collections")
    args = parser.parse_args()

    if args.collection:
        clear_collection_index(args.collection)
        rebuild_single_collection(args.collection)
    elif args.all:
        rebuild_all_collections()
    else:
        parser.print_help()