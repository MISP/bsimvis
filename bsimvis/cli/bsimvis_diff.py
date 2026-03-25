import redis
import json
import time
import math
import argparse
import logging
import datetime

_r = None

def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r

# --- Turbo Similarity Bake Lua Script (Resource Optimized) ---
# Supports Jaccard and Unweighted Cosine algorithms.
# Stores results in per-function keys and a global collection-wide ZSET.
# Thresholding is applied early to prevent memory and CPU bloat.
LUA_ENHANCED_SIM_SCRIPT = """
local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local limit = tonumber(ARGV[4])
local target_md5 = ARGV[5]
local target_addr = ARGV[6]
local threshold = tonumber(ARGV[7])

local target_meta_raw = redis.call('JSON.GET', target_id .. ':meta')
local target_meta = {}
if target_meta_raw then target_meta = cjson.decode(target_meta_raw) end

-- ARGV[8...] are feature hashes
local target_features = {}
for i = 8, #ARGV do
    target_features[#target_features + 1] = ARGV[i]
end

local target_count = #target_features
local intersection_counts = {}
local candidates_seen = {}

-- 1. Identify all candidates and count intersections
for _, f_hash in ipairs(target_features) do
    local functions = redis.call('ZRANGE', collection .. ':feature:' .. f_hash .. ':functions', 0, -1)
    for _, func_id in ipairs(functions) do
        if func_id ~= target_id then
            if not candidates_seen[func_id] then
                candidates_seen[func_id] = true
                intersection_counts[func_id] = 0
            end
            intersection_counts[func_id] = intersection_counts[func_id] + 1
        end
    end
end

-- 2. Scored Candidates (Filtered by Threshold)
local candidate_list = {}
for id, intersect in pairs(intersection_counts) do
    local cand_count = redis.call('ZCARD', id .. ':vec:tf')
    if cand_count and cand_count > 0 then
        local score = 0
        if algo == 'jaccard' then
            score = intersect / (target_count + cand_count - intersect)
        elseif algo == 'unweighted_cosine' then
            score = intersect / math.sqrt(target_count * cand_count)
        end
        
        -- ONLY keep if above threshold
        if score >= threshold then
            table.insert(candidate_list, {id = id, score = score})
        end
    end
end

-- 3. Sort by score
table.sort(candidate_list, function(a, b) return a.score > b.score end)

-- 4. Enrich Top K and Prepare for Storage
local matches = {}
local limit_val = math.min(limit, #candidate_list)
local global_zset_key = collection .. ':all_sim:' .. algo

for i = 1, limit_val do
    local item = candidate_list[i]
    local meta_raw = redis.call('JSON.GET', item.id .. ':meta')
    local meta = {}
    if meta_raw then
        meta = cjson.decode(meta_raw)
    end
    
    local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
    
    table.insert(matches, {
        id = item.id,
        name = meta["function_name"] or item.id,
        score = score_rounded,
        md5 = meta["file_md5"] or ""
    })
    
    -- 5. Add to collection-wide similarity scoreboard (ZSET)
    local pair_id = target_id .. '::' .. item.id
    redis.call('ZADD', global_zset_key, score_rounded, pair_id)

    -- 6. Store similarity metadata for RediSearch
    local sim_meta_key = collection .. ':sim_meta:' .. algo .. ':' .. target_id .. ':' .. item.id
    local sim_doc = {
        type = "sim",
        collection = collection,
        algo = algo,
        score = score_rounded,
        id1 = target_id,
        id2 = item.id,
        name1 = target_meta["function_name"] or target_id,
        name2 = meta["function_name"] or item.id,
        md5_1 = target_md5,
        md5_2 = meta["file_md5"] or "",
        tags1 = table.concat(target_meta["tags"] or {}, ","),
        tags2 = table.concat(meta["tags"] or {}, ","),
        batch_uuid1 = target_meta["batch_uuid"] or "",
        batch_uuid2 = meta["batch_uuid"] or "",
        language_id1 = target_meta["language_id"] or "",
        language_id2 = meta["language_id"] or "",
        feat_count1 = target_meta["bsim_features_count"] or 0,
        feat_count2 = meta["bsim_features_count"] or 0,
        is_cross_binary = (target_md5 ~= meta["file_md5"]) and "true" or "false"
    }
    redis.call('JSON.SET', sim_meta_key, '$', cjson.encode(sim_doc))
end

-- 7. Store result directly in Redis
local result_key = collection .. ':function:' .. target_md5 .. ':' .. target_addr .. ':sim:' .. algo
redis.call('JSON.SET', result_key, '$', cjson.encode(matches))

return #matches
"""

def bake_enhanced_similarities(args, r=None):
    r = r or get_redis()
    sim_script = r.register_script(LUA_ENHANCED_SIM_SCRIPT)
    
    collection = args.collection
    algo = args.algo
    top_k = args.top_k
    threshold = args.threshold
    
    print(f"[*] Enhanced Sim Bake | Collection: {collection} | Algo: {algo} | Threshold: {threshold}")
    
    # Selecting targets based on filters
    keys_to_process = []
    
    # 1. Binary Filters (MD5s)
    if args.md5:
        for md5 in args.md5:
            keys_to_process.extend(r.keys(f"{collection}:function:{md5}:*:vec:tf"))
    
    # 2. Function Filters
    if args.func:
        for f in args.func:
            if ":" in f:
                keys_to_process.append(f"{collection}:function:{f}:vec:tf")
            else:
                pattern = f"{collection}:function:*:{f}:vec:tf"
                keys_to_process.extend(r.keys(pattern))

    # If no filters, keys_to_process remains empty but we skip to SCAN
    if not args.md5 and not args.func:
        keys_to_process = None

    start_time = time.time()
    processed = 0

    if keys_to_process is not None:
        # Use set to avoid duplicates if multiple filters overlap
        unique_keys = sorted(list(set(keys_to_process)))
        print(f"[*] Found {len(unique_keys)} keys in batch")
        for key in unique_keys:
            process_single_key(r, sim_script, key, collection, algo, top_k, threshold)
            processed += 1
            if processed % 10 == 0: print_progress(processed, start_time, collection)
            if args.delay > 0: time.sleep(args.delay)
    else:
        # Full SCAN mode
        cursor = 0
        match_pattern = f"{collection}:function:*:*:vec:tf"
        while True:
            cursor, keys = r.scan(cursor=cursor, match=match_pattern, count=args.batch_size)
            print(f"[*] Found {len(keys)} keys in batch")
            for key in keys:
                process_single_key(r, sim_script, key, collection, algo, top_k, threshold)
                processed += 1
                if processed % 50 == 0: print_progress(processed, start_time, collection)
                if args.delay > 0: time.sleep(args.delay)
            if cursor == 0: break

    print(f"[+] DONE: {processed} functions baked in {time.time() - start_time:.2f}s")

def process_single_key(r, script, key, collection, algo, top_k, threshold):
    parts = key.split(':')
    if len(parts) < 4: return
    md5, addr = parts[2], parts[3]
    base_id = f"{collection}:function:{md5}:{addr}"
    
    features = r.zrange(key, 0, -1)
    if not features: return
    
    # Lua ARGV: [id, collection, algo, top_k, md5, addr, threshold, features...]
    lua_args = [base_id, collection, algo, top_k, md5, addr, threshold] + features
    
    try:
        script(args=lua_args)
    except Exception as e:
        logging.error(f"Lua Error for {base_id}: {e}")

def print_progress(count, start, collection):
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"  [i] {collection}: {count} functions baked ({rate:.1f} func/s)...")

def run_diff(host, port, args):
    r = get_redis(host, port)
    coll = args.collection
    
    if args.action == "status":
        print(f"[*] Checking similarity bake status for: {coll}")
        # Placeholder: Check for existence of any similarity keys
        cursor, keys = r.scan(0, match=f"{coll}:all_sim:*", count=1)
        if keys:
            print("OK")
        else:
            print("No similarity data found.")
            
    elif args.action == "build":
        bake_enhanced_similarities(args, r=r)
        
    elif args.action == "rebuild":
        print(f"[*] Clearing and rebuilding similarities for: {coll}")
        diff_clear(coll, r=r)
        bake_enhanced_similarities(args, r=r)
        
    elif args.action == "clear":
        diff_clear(coll, r=r)

def diff_clear(collection, r=None):
    r = r or get_redis()
    print(f"[*] Clearing similarity data for: {collection}...")
    patterns = [
        f"{collection}:all_sim:*",
        f"{collection}:sim_meta:*",
        f"{collection}:function:*:sim:*"
    ]
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                r.delete(*keys)
            if cursor == 0: break
    print("[+] Similarity data cleared.")

def main():
    parser = argparse.ArgumentParser(description="BSim Turbo Similarity Baker with Resource Guards")
    parser.add_argument("-c", "--collection", required=True, help="Collection name")
    parser.add_argument("--algo", default="unweighted_cosine", choices=["jaccard", "unweighted_cosine"], help="Similarity algorithm")
    parser.add_argument("--md5", action="append", help="Filter by specific binary MD5 (can be used multiple times)")
    parser.add_argument("--func", action="append", help="Filter by specific function (ID or MD5:ADDR, can be used multiple times)")
    parser.add_argument("-k", "--top-k", type=int, default=DEFAULT_TOP_K, help="Top K matches per function")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="Minimum similarity score (e.g., 0.1)")
    parser.add_argument("--delay", type=float, default=0.0, help="Artificial delay (seconds) between calculations")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Internal SCAN batch size")
    
    args = parser.parse_args()
    bake_enhanced_similarities(args)

if __name__ == "__main__":
    main()


