#!/usr/bin/env python3
import sys
import hashlib
from bsimvis.app.services.redis_client import get_redis

def main():
    if len(sys.argv) < 2:
        print("Usage: uv run scripts/backfill_bsimhash.py <collection_name>")
        sys.exit(1)
        
    collection = sys.argv[1]
    r = get_redis()
    
    print(f"[*] Backfilling bsimhash for collection: {collection}")
    
    # Get all files
    files = r.smembers(f"{collection}:all_files")
    if not files:
        print("No files found.")
        return
        
    total_funcs = 0
    updated_funcs = 0
    
    for file_id in files:
        file_id = file_id.decode() if isinstance(file_id, bytes) else str(file_id)
        md5 = file_id.split(":")[-1]
        
        funcs = r.smembers(f"{collection}:idx:file:functions:{md5}")
        if not funcs:
            continue
            
        pipe = r.pipeline(transaction=False)
        for fid in funcs:
            fid = fid.decode() if isinstance(fid, bytes) else str(fid)
            
            # Fetch vec:tf
            # In Redis, ZRANGE withscores returns a list of tuples (member, score)
            vec_tf = r.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            if not vec_tf:
                continue
                
            total_funcs += 1
            
            # Convert to list of dicts or just sort by feature hash directly
            # ZRANGE returns sorted by score, we need to sort by feature hash!
            # vec_tf is [(b'feature_hash', 1.0), ...]
            feats = []
            for f_hash, tf in vec_tf:
                f_hash = f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash)
                feats.append({"hash": f_hash, "tf": tf})
                
            sorted_feats = sorted(feats, key=lambda x: x["hash"])
            canon_str = ",".join(f"{item['hash']}:{item['tf']}" for item in sorted_feats)
            bsim_hash = hashlib.sha256(canon_str.encode()).hexdigest()
            
            pipe.sadd(f"{collection}:bsimhash:{bsim_hash}", fid)
            pipe.set(f"{fid}:bsimhash", bsim_hash)
            updated_funcs += 1
            
        pipe.execute()
        
    print(f"[*] Done. Backfilled {updated_funcs}/{total_funcs} functions with BSim vectors.")

if __name__ == "__main__":
    main()
