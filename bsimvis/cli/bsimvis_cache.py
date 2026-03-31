import redis
import json
import logging
from bsimvis.app.services.redis_client import get_redis

def run_cache(host, port, args):
    r = get_redis()
    
    if args.action == "clear":
        clear_cache(r, args.collection)

def clear_cache(r, collection=None):
    print(f"[*] Clearing similarity search cache" + (f" for collection '{collection}'" if collection else " (global)") + "...")
    
    count = 0
    # Use scan_iter for safe iteration over keys
    for key in r.scan_iter("cache:search:sim:*"):
        if collection:
            try:
                # Need to check collection inside the value
                val = r.get(key)
                if val:
                    data = json.loads(val)
                    if data.get("collection") == collection:
                        r.delete(key)
                        count += 1
            except Exception as e:
                logging.warning(f"Failed to parse cache key {key}: {e}")
                # If we can't parse it, better safe than sorry? 
                # No, if filtering by collection, we must be sure.
                pass
        else:
            # Global clear
            r.delete(key)
            count += 1
            
    print(f"[+] Successfully cleared {count} cache entries.")
