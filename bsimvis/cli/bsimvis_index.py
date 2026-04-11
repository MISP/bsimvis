import requests
import json
import os
import sys

def format_bytes(b):
    if b < 1:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def run_index_status(host, port, args):
    """
    Directly queries the database for exact index cardinalities.
    """
    import redis

    # Connect to the Kvrocks data port (6666) instead of the Flask API port (5000)
    kv_port = 6666 if port == 5000 else port
    try:
        r = redis.Redis(host=host, port=kv_port, decode_responses=True)
        col = args.collection

        files = r.scard(f"{col}:all_files") or 0
        funcs = r.scard(f"{col}:all_functions") or 0
        indexed = r.scard(f"{col}:indexed:functions") or 0
        features = r.zcard(f"{col}:features:by_tf") or 0
        sim_cos = r.zcard(f"{col}:sim:score:unweighted_cosine") or 0
        sim_jac = r.zcard(f"{col}:sim:score:jaccard") or 0
        
        print(f"\n[*] Exact Index Status for {col.upper()}")
        print("-" * 45)
        print(f"    {'Files':<22} | {files}")
        print(f"    {'Functions Total':<22} | {funcs}")
        print(f"    {'Functions Indexed':<22} | {indexed}")
        print(f"    {'Functions Missing':<22} | {max(0, funcs - indexed)}")
        print(f"    {'Unique Features':<22} | {features}")
        print(f"    {'Sims (Cosine)':<22} | {sim_cos}")
        print(f"    {'Sims (Jaccard)':<22} | {sim_jac}")
        print("-" * 45 + "\n")

    except Exception as e:
        print(f"[!] Error reading Exact Index components: {e}")

def run_index_reg(host, port, args):
    """
    Directly queries the database for all registries and prints their cardinality.
    """
    import redis
    
    # Connect to the Kvrocks data port (6666) instead of the Flask API port (5000)
    # For simplicity, if standard API port 5000 is given, we assume Kvrocks is on 6666
    kv_port = 6666 if port == 5000 else port
    try:
        r = redis.Redis(host=host, port=kv_port, decode_responses=True)
        
        # Determine the pattern based on the optional collection argument
        if getattr(args, "collection", None):
            pattern = f"{args.collection}:reg:*"
        else:
            pattern = "*:reg:*"
            
        keys = list(r.scan_iter(pattern))
        
        if not keys:
            col_str = f" for '{args.collection}'" if getattr(args, "collection", None) else ""
            print(f"\n[*] No registries found{col_str}.")
            return

        print(f"\n[*] Registry Efficiency (Sampled):")
        print(f"    {'Registry Key':<42} | {'Template':<38} | {'Buckets':<8} | {'Avg':<8} | {'Sample Bucket'}")
        print("-" * 145)
        
        total_buckets = 0
        for k in sorted(keys):
            try:
                card = r.scard(k)
                total_buckets += card
                
                prefix = k.replace(":reg:", ":idx:") + ":"
                if ":reg:tags" in k and ":sim:tags" not in k and ":function:tags" not in k:
                    prefix = k.replace(":reg:tags", ":idx:sim:tags:")
                    if ":reg:user_tags" in k:
                        prefix = k.replace(":reg:user_tags", ":idx:sim:user_tags:")
                template = prefix + "*"
                
                # Fast Estimate: Sample up to 50 random buckets from the registry
                sample = r.srandmember(k, 50)
                avg_size = 0
                if sample:
                    # Registry items might be bytes or strings depending on redis-py decode_responses
                    sample = [b.decode() if isinstance(b, bytes) else b for b in sample]
                    
                    pipe = r.pipeline()
                    for bucket in sample:
                        pipe.scard(bucket)
                    sizes = pipe.execute()
                    
                    valid_sizes = [s for s in sizes if s > 0]
                    if not valid_sizes:
                        # Fallback to ZCARD if they are zsets
                        pipe = r.pipeline()
                        for bucket in sample:
                            pipe.zcard(bucket)
                        sizes = pipe.execute()
                        valid_sizes = [s for s in sizes if s > 0]
                        
                    if valid_sizes:
                        avg_size = sum(valid_sizes) / len(valid_sizes)
                
                sample_bucket = sample[0] if sample else "N/A"
                print(f"    {k:<42} | {template:<38} | {card:<8} | {avg_size:<8.1f} | {sample_bucket}")
            except Exception:
                pass
                
        print("-" * 145)
        print(f"    {'TOTAL':<42} | {'':<38} | {total_buckets:<8} | {'-':<8} | {'-'}\n")
    except Exception as e:
        print(f"[!] Error connecting to database for registry status: {e}")
