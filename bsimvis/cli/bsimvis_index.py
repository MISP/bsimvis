import redis
import json
import time
import math
import sys
import os

# Allow running this file directly from CLI
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

_r = None

def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r

def get_key_count(r, k):
    """Unified cardinality check."""
    try:
        rtype = r.type(k).lower()
        if "zset" in rtype: return r.zcard(k)
        if "set" in rtype: return r.scard(k)
        if "list" in rtype: return r.llen(k)
        if "hash" in rtype: return r.hlen(k)
    except: pass
    return 0

def get_key_size(r, k):
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

def format_bytes(b):
    if b < 1: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024: return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def estimate_total_keys(r, pattern, num_files, num_funcs, num_unique_features):
    # We try to avoid a full SCAN if possible.
    if "file:*:meta" in pattern: return num_files
    if "function:*:*:meta" in pattern: return num_funcs
    if "function:*:*:source" in pattern: return num_funcs
    if "function:*:*:vec:tf" in pattern: return num_funcs
    if "feature:*:functions" in pattern: return num_unique_features
    if "feature:*:meta" in pattern: return num_unique_features
    
    # For sim_meta and tags, we might need a quick scan to estimate.
    cursor = 0
    count_acc = 0
    # Sample scan up to 100 iterations of 5000 for better accuracy on large sets
    for _ in range(100):
        cursor, keys = r.scan(cursor, match=pattern, count=5000)
        count_acc += len(keys)
        if cursor == 0: break
    
    if cursor == 0: return count_acc
    # If still scanning, we have at least count_acc. 
    return count_acc

def estimate_group_size(r, pattern, count_total, tracking_set=None, key_formatter=None):
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
        sz = get_key_size(r, k)
        if sz > 0:
            total_size += sz
            actual_samples += 1
    return (total_size / actual_samples) if actual_samples > 0 else 0

def run_index_status(host, port, args):
    r = get_redis(host, port)
    coll = args.collection
    
    print(f"\n[*] Index Status for Collection: {coll}")
    
    # 1. Core Counts
    num_files = r.scard(f"idx:{coll}:all_files")
    num_funcs = r.scard(f"idx:{coll}:all_functions")
    num_indexed = r.scard(f"idx:{coll}:indexed:functions")
    num_unique_features = r.zcard(f"idx:{coll}:features:by_tf")
    
    # Estimate sim_meta accurately (deep scan if needed)
    num_sim_meta = estimate_total_keys(r, f"{coll}:sim_meta:*:*:*", num_files, num_funcs, num_unique_features)
    
    print(f"    - Files           : {num_files}")
    print(f"    - Functions       : {num_funcs} ({num_indexed} indexed / {num_funcs - num_indexed} missing)")
    print(f"    - Unique Features : {num_unique_features}")
    if num_sim_meta > 0:
        print(f"    - Sim Metadata    : {num_sim_meta}")
    
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
        ("Sim Meta", f"{coll}:sim_meta:*:*:*"),
        ("Inverted Index", f"idx:{coll}:feature:*:functions"),
        ("Feature Meta", f"idx:{coll}:feature:*:meta"),
        ("Tag Index", f"idx:{coll}:*:*:*"),
    ]

    print(f"    {'Component':<20} | {'Key Pattern':<40} | {'Count':<10}")
    print("-" * 80)
    for name, pat in patterns:
        count = estimate_total_keys(r, pat, num_files, num_funcs, num_unique_features)
        if count > 0:
            print(f"    {name:<20} | {pat:<40} | {count:<10}")

    # 3. Space Estimation (Deep Sampling)
    print(f"\n[+] Space Estimation (Deep Sampling):")
    
    # Recalculate counts for stats_config
    num_sim_meta_est = estimate_total_keys(r, f"{coll}:sim_meta:*:*:*", num_files, num_funcs, num_unique_features)

    stats_config = [
        ("File Meta", f"{coll}:file:*:meta", num_files, f"idx:{coll}:all_files", None),
        ("Func Meta", f"{coll}:function:*:*:meta", num_funcs, f"idx:{coll}:all_functions", None),
        ("Func Source", f"{coll}:function:*:*:source", num_funcs, f"idx:{coll}:all_functions", lambda x: str(x).replace(":meta", ":source")),
        ("Sim Meta", f"{coll}:sim_meta:*:*:*", num_sim_meta_est, None, None),
        ("Inverted Index", f"idx:{coll}:feature:*:functions", num_unique_features, f"idx:{coll}:features:by_tf", lambda x: f"idx:{coll}:feature:{x}:functions"),
        ("Feature Meta", f"idx:{coll}:feature:*:meta", num_unique_features, f"idx:{coll}:features:by_tf", lambda x: f"idx:{coll}:feature:{x}:meta"),
    ]

    total_est = 0
    print(f"    {'Component':<20} | {'Avg Size':<12} | {'Total Est.':<12}")
    print("-" * 60)
    
    for name, pat, count, tset, kform in stats_config:
        avg = estimate_group_size(r, pat, count, tracking_set=tset, key_formatter=kform)
        comp_total = avg * count
        total_est += comp_total
        print(f"    {name:<20} | {format_bytes(avg):<12} | {format_bytes(comp_total):<12}")

    print("-" * 60)
    print(f"    {'ESTIMATED TOTAL INFRA':<20} | {'':<12} | {format_bytes(total_est):<12}")
    
    # 4. Secondary Index Analysis (Detailed breakdown of tags, names, etc.)
    print(f"\n[+] Secondary Index Breakdown (Tags, Fields):")
    
    sec_indexes = {} # (doc_type, field) -> list of keys
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
        sample_key = samples[0]
        rtype = r.type(sample_key).lower()
        if dtype == "feature": rep_pat = f"idx:{coll}:feature:*:{field}"
        else: rep_pat = f"idx:{coll}:{dtype}:{field}:*"

        real_val_count = len(samples)
        if dtype == "feature" and (field == "functions" or field == "meta"): real_val_count = num_unique_features
        elif dtype == "function" and field == "entrypoint_address": real_val_count = num_funcs
        elif dtype == "sim" and field == "scoreboard": real_val_count = num_funcs
        else:
            if len(samples) >= 50:
                inner_cursor = 0
                count_acc = 0
                for _ in range(20):
                    inner_cursor, inner_keys = r.scan(inner_cursor, match=f"idx:{coll}:{dtype}:{field}:*", count=2000)
                    count_acc += len(inner_keys)
                    if inner_cursor == 0: break
                real_val_count = count_acc
        
        total_docs = 0
        total_bytes = 0
        for sk in samples[:10]:
            total_docs += get_key_count(r, sk)
            total_bytes += get_key_size(r, sk)
        
        n_samples = min(len(samples), 10)
        avg_docs = total_docs / n_samples if n_samples > 0 else 0
        avg_bytes = total_bytes / n_samples if n_samples > 0 else 0
        est_total = avg_bytes * real_val_count
        total_sec_est += est_total

        val_count_str = str(real_val_count)
        if dtype == "sim" and field == "scoreboard": val_count_str = f"~{num_funcs}"

        print(f"    {dtype:<10} | {field:<18} | {rtype:<8} | {val_count_str:<12} | {avg_docs:<10.1f} | {format_bytes(est_total):<12} | {rep_pat}")

    print("-" * 140)
    print(f"    {'TOTAL SECONDARY INDEX':<54} | {format_bytes(total_sec_est):<12}")
    print("-" * 60)
    print(f"    {'ESTIMATED GRAND TOTAL':<20} | {'':<12} | {format_bytes(total_est + total_sec_est):<12}")

    if num_indexed > 0 and num_unique_features > 0:
        print(f"\n[+] Index Density:")
        print(f"    - Unique Features per Function : {num_unique_features / num_indexed:.2f}")

    print(f"\n[i] Note: Calculations are estimates based on sampling 10 keys per component.")