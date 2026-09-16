import os
import sys
from dotenv import load_dotenv

load_dotenv()
from bsimvis.app.services.similarity_service import SimilarityService

def main():
    source = "rapperbot"
    dest = "rapperbot_fixed"
    s = SimilarityService()
    r = s.r
    
    r.sadd("registry:collections", dest)
    
    print(f"Finding keys for {source}...")
    keys = r.keys(f"{source}:*")
    
    exclude_prefixes = {
        "sim",
        "bin_sim",
        "cluster",
        "bin_cluster",
        "all_bin_sims",
        "bin_sim_rev",
        "built"
    }
    
    keys_to_copy = []
    for k in keys:
        if isinstance(k, bytes): k = k.decode()
        parts = k.split(':')
        if len(parts) > 1 and parts[1] in exclude_prefixes:
            continue
        keys_to_copy.append(k)
        
    print(f"Smart-copying {len(keys_to_copy)} keys to {dest}...")
    
    def replace_str(val):
        if isinstance(val, bytes):
            val = val.decode(errors='ignore')
        if isinstance(val, str):
            return val.replace(f"{source}:", f"{dest}:")
        return val

    for k in keys_to_copy:
        new_k = k.replace(f"{source}:", f"{dest}:", 1)
        k_type = r.type(k)
        if isinstance(k_type, bytes):
            k_type = k_type.decode()
            
        if k_type == 'string':
            val = r.get(k)
            r.set(new_k, replace_str(val))
        elif k_type == 'zset':
            vals = r.zrange(k, 0, -1, withscores=True)
            if vals:
                new_vals = {replace_str(member): score for member, score in vals}
                r.zadd(new_k, new_vals)
        elif k_type == 'set':
            vals = r.smembers(k)
            if vals:
                new_vals = [replace_str(member) for member in vals]
                r.sadd(new_k, *new_vals)
        elif k_type == 'hash':
            vals = r.hgetall(k)
            if vals:
                new_vals = {replace_str(hk): replace_str(hv) for hk, hv in vals.items()}
                r.hset(new_k, mapping=new_vals)
        elif k_type == 'list':
            vals = r.lrange(k, 0, -1)
            if vals:
                new_vals = [replace_str(v) for v in vals]
                r.rpush(new_k, *new_vals)

    print("Copy complete!")

if __name__ == '__main__':
    main()
