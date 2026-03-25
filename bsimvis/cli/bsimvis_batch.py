import redis
import json

_r = None

def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r

def run_batch(host, port, args):
    r = get_redis(host, port)
    coll = args.collection
    
    if args.action == "list":
        list_batches(coll, r=r)
    elif args.action == "remove":
        remove_batch(coll, args.batch, r=r)

def list_batches(collection, r=None):
    r = r or get_redis()
    print(f"\n[*] List of Batches in Collection: {collection}")
    print(f"{'Batch UUID':<40} | {'Name':<35} | {'Functions':<10}")
    print("-" * 90)
    
    batch_uuids = sorted(list(r.smembers("global:batches")))
    found = 0
    for uuid in batch_uuids:
        # Check if batch has functions in this collection
        batch_func_set = f"{collection}:batch:{uuid}:functions"
        meta_key = f"{collection}:batch:{uuid}"
        
        if not r.exists(batch_func_set) and not r.exists(meta_key):
            continue
            
        name_raw = r.json().get(meta_key, "$.name")
        name = "Unknown"
        if name_raw:
            name = name_raw[0] if isinstance(name_raw, list) else name_raw
            
        count = r.scard(batch_func_set)
        print(f"{uuid:<40} | {str(name)[:35]:<35} | {count:<10}")
        found += 1
        
    if found == 0:
        print(f"[!] No batches found for collection {collection}.")

def remove_batch(collection, batch_uuid, r=None):
    r = r or get_redis()
    print(f"[*] Removing batch {batch_uuid} from collection {collection}...")
    
    # 1. Clear indexing data for all functions in this batch
    from bsimvis.cli.bsimvis_index import resolve_functions, clear_functions_index
    func_ids = resolve_functions(collection, batch_uuid=batch_uuid, r=r)
    if func_ids:
        clear_functions_index(collection, func_ids, r=r)
    
    # 2. Clear batch-specific keys
    r.delete(f"{collection}:batch:{batch_uuid}:functions")
    r.delete(f"{collection}:batch:{batch_uuid}")
    
    # Note: We don't remove from 'global:batches' because it might be shared across collections
    
    print(f"[+] Batch {batch_uuid} and its indexing data removed from {collection}.")
