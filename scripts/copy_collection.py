import os
import sys
import redis
from dotenv import load_dotenv

load_dotenv()
from bsimvis.app.services.config_service import config

def main():
    source = "rapperbot"
    dest = "rapperbot_fixed"

    host = config.get("database.kvrocks_host", "localhost")
    port = config.get("database.kvrocks_port", 6666)
    r = redis.Redis(host=host, port=port, decode_responses=False)
    
    r.sadd("registry:collections", dest)
    
    print(f"Finding keys for {source}...")
    keys = r.keys(f"{source}:*".encode())
    
    exclude_prefixes = {
        b"sim",
        b"bin_sim",
        b"cluster",
        b"bin_cluster",
        b"all_bin_sims",
        b"bin_sim_rev",
        b"built"
    }
    
    keys_to_copy = []
    for k in keys:
        parts = k.split(b':')
        if len(parts) > 1 and parts[1] in exclude_prefixes:
            continue
        keys_to_copy.append(k)

    print(f"Smart-copying {len(keys_to_copy)} keys to {dest}...")
    
    def replace_bytes(val):
        if isinstance(val, bytes):
            return val.replace(f"{source}:".encode(), f"{dest}:".encode())
        return val

    count = 0
    pipe = r.pipeline(transaction=False)
    for k in keys_to_copy:
        new_k = k.replace(f"{source}:".encode(), f"{dest}:".encode(), 1)
        if r.exists(new_k):
            count += 1
            continue
            
        k_type = r.type(k).decode()

        if k_type == 'string':
            val = r.get(k)
            pipe.set(new_k, replace_bytes(val))
        elif k_type == 'zset':
            vals = r.zrange(k, 0, -1, withscores=True)
            if vals:
                new_vals = {replace_bytes(member): score for member, score in vals}
                pipe.zadd(new_k, new_vals)
        elif k_type == 'set':
            vals = r.smembers(k)
            if vals:
                new_vals = [replace_bytes(member) for member in vals]
                pipe.sadd(new_k, *new_vals)
        elif k_type == 'hash':
            vals = r.hgetall(k)
            if vals:
                new_vals = {replace_bytes(hk): replace_bytes(hv) for hk, hv in vals.items()}
                pipe.hset(new_k, mapping=new_vals)
        elif k_type == 'list':
            vals = r.lrange(k, 0, -1)
            if vals:
                new_vals = [replace_bytes(v) for v in vals]
                pipe.rpush(new_k, *new_vals)

        count += 1
        if count % 2000 == 0:
            pipe.execute()
            print(f"Copied {count}")

    pipe.execute()
    print("Copy complete!")

if __name__ == '__main__':
    main()
