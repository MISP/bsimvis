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
    local functions = redis.call('ZRANGE', 'idx:' .. collection .. ':feature:' .. f_hash .. ':functions', 0, -1)
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
    
    -- --- Canonical Check: Only store if target_id > item.id ---
    -- target_id format: collection:function:target_md5:target_addr
    -- item.id format:   collection:function:item_md5:item_addr
    
    local parts2 = {}
    for p in string.gmatch(item.id, "([^:]+)") do
        table.insert(parts2, p)
    end
    
    local item_md5 = parts2[3]
    local item_addr = parts2[4] or "0"
    
    local should_store = false
    if target_addr > item_addr then
        should_store = true
    elseif target_addr == item_addr then
        if target_md5 > item_md5 then
            should_store = true
        end
    end
    
    if should_store then
        local meta_raw = redis.call('JSON.GET', item.id .. ':meta')
        local meta = {}
        if meta_raw then
            meta = cjson.decode(meta_raw)
        end
        
        local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
        
        -- 5. Store similarity metadata for RediSearch
        local sim_meta_key = collection .. ':sim_meta:' .. algo .. ':' .. target_id .. ':' .. item.id
        
        -- 6. Add to collection-wide similarity scoreboard (ZSET)
        redis.call('ZADD', global_zset_key, score_rounded, sim_meta_key)
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
            return_type1 = target_meta["return_type"] or "",
            return_type2 = meta["return_type"] or "",
            decompiler1 = target_meta["decompiler_id"] or "",
            decompiler2 = meta["decompiler_id"] or "",
            feat_count1 = target_meta["bsim_features_count"] or 0,
            feat_count2 = meta["bsim_features_count"] or 0,
            min_features = math.min(target_meta["bsim_features_count"] or 0, meta["bsim_features_count"] or 0),
            is_cross_binary = (target_md5 ~= meta["file_md5"]) and "true" or "false"
        }
        redis.call('JSON.SET', sim_meta_key, '$', cjson.encode(sim_doc))

        -- 6.1 Secondary Indexing (Kvrocks-optimized ZSETs)
        local all_key = 'idx:' .. collection .. ':all_similarities'
        local all_type = redis.call('TYPE', all_key)
        if type(all_type) == 'table' then all_type = all_type['ok'] or all_type[1] end
        if all_type == 'set' then redis.call('DEL', all_key) end
        redis.call('ZADD', all_key, 0, sim_meta_key)
        
        -- Index individual fields for filtering (using ZSETs with score 0) and maintain a key registry
        local function index_tag(field, value)
            if value and value ~= "" then
                local k = 'idx:' .. collection .. ':sim:' .. field .. ':' .. string.lower(tostring(value))
                local kt = redis.call('TYPE', k)
                if type(kt) == 'table' then kt = kt['ok'] or kt[1] end
                if kt == 'set' then redis.call('DEL', k) end
                redis.call('ZADD', k, 0, sim_meta_key)
                
                -- ADD TO REGISTRY for fast SSCAN
                redis.call('SADD', 'idx:' .. collection .. ':reg:' .. field, k)
            end
        end

        index_tag('name1', sim_doc.name1)
        index_tag('name2', sim_doc.name2)
        index_tag('md5_1', sim_doc.md5_1)
        index_tag('md5_2', sim_doc.md5_2)
        index_tag('batch_uuid1', sim_doc.batch_uuid1)
        index_tag('batch_uuid2', sim_doc.batch_uuid2)
        index_tag('language_id1', sim_doc.language_id1)
        index_tag('language_id2', sim_doc.language_id2)
        index_tag('is_cross_binary', sim_doc.is_cross_binary)
        
        -- Special handling for IDs (which are md5:addr)
        index_tag('id1', target_md5 .. ':' .. target_addr)
        index_tag('id2', meta["file_md5"] .. ':' .. (meta["entrypoint_address"] or "0"))

        -- Numeric Indexes
        if sim_doc.feat_count1 then
            redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count1', sim_doc.feat_count1, sim_meta_key)
        end
        if sim_doc.feat_count2 then
            redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count2', sim_doc.feat_count2, sim_meta_key)
        end
        if sim_doc.min_features then
            redis.call('ZADD', 'idx:' .. collection .. ':sim:min_features', sim_doc.min_features, sim_meta_key)
        end

        -- Tags (split by comma if needed)
        if target_meta["tags"] then
            for _, t in ipairs(target_meta["tags"]) do
                index_tag('tags1', t)
            end
        end
        if meta["tags"] then
            for _, t in ipairs(meta["tags"]) do
                index_tag('tags2', t)
            end
        end
    end
end

return 1
"""


def is_fully_indexed(collection, r):
    indexed_set = f"idx:{collection}:indexed:functions"
    total = 0
    indexed = 0

    batch_uuids = list(r.smembers("global:batches"))
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
        return False, 0, 0
    return indexed == total, indexed, total


def bake_enhanced_similarities(args, r=None):
    r = r or get_redis()

    # --- Index Status Check ---
    if not getattr(args, "ignore_indexing", False):
        is_ok, indexed, total = is_fully_indexed(args.collection, r)
        if not is_ok:
            if total == 0:
                print(f"[!] ERROR: No data found for collection '{args.collection}'.")
            else:
                print(
                    f"[!] ERROR: Collection '{args.collection}' is not fully indexed ({indexed}/{total})."
                )
                print(
                    f"    Please run 'bsimvis index build -c {args.collection}' first, or use --ignore-indexing."
                )
            return

    sim_script = r.register_script(LUA_ENHANCED_SIM_SCRIPT)

    collection = args.collection
    algo = args.algo
    top_k = args.top_k
    min_score = args.min_score

    print(
        f"[*] Enhanced Sim Bake | Collection: {collection} | Algo: {algo} | Min Score: {min_score}"
    )

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
            if process_single_key(
                r, sim_script, key, collection, algo, top_k, min_score
            ):
                processed += 1
            if processed % 10 == 0:
                print_progress(processed, start_time, collection)
            if args.delay > 0:
                time.sleep(args.delay)
    else:
        # Full SCAN mode
        cursor = 0
        match_pattern = f"{collection}:function:*:*:vec:tf"
        while True:
            cursor, keys = r.scan(
                cursor=cursor, match=match_pattern, count=args.batch_size
            )
            print(f"[*] Found {len(keys)} keys in batch")
            for key in keys:
                if process_single_key(
                    r, sim_script, key, collection, algo, top_k, min_score
                ):
                    processed += 1
                if processed % 50 == 0:
                    print_progress(processed, start_time, collection)
                if args.delay > 0:
                    time.sleep(args.delay)
            if cursor == 0:
                break

    print(f"[+] DONE: {processed} functions baked in {time.time() - start_time:.2f}s")


def process_single_key(r, script, key, collection, algo, top_k, min_score):
    parts = key.split(":")
    if len(parts) < 4:
        return
    md5, addr = parts[2], parts[3]
    base_id = f"{collection}:function:{md5}:{addr}"

    features = r.zrange(key, 0, -1)
    if not features:
        return

    # Lua ARGV: [id, collection, algo, top_k, md5, addr, threshold, features...]
    lua_args = [base_id, collection, algo, top_k, md5, addr, min_score] + features

    try:
        script(args=lua_args)
        r.sadd(f"idx:{collection}:baked:functions:{algo}", base_id)
        return True
    except Exception as e:
        logging.error(f"Lua Error for {base_id}: {e}")
        return False


def print_progress(count, start, collection):
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"  [i] {collection}: {count} functions baked ({rate:.1f} func/s)...")


def run_sim(host, port, args):
    r = get_redis(host, port)
    coll = args.collection

    if args.action == "status":
        sim_quick_status(coll, r=r)
    elif args.action == "list":
        list_sim_batches(coll, batch_filter=args.batch, r=r)
    elif args.action == "build":
        bake_enhanced_similarities(args, r=r)

    elif args.action == "rebuild":
        print(f"[*] Clearing and rebuilding similarities for: {coll}")
        sim_clear(coll, r=r)
        bake_enhanced_similarities(args, r=r)

    elif args.action == "clear":
        sim_clear(coll, r=r)


def sim_clear(collection, r=None):
    r = r or get_redis()
    print(f"[*] Clearing similarity data for: {collection}...")
    patterns = [
        f"{collection}:all_sim:*",
        f"{collection}:sim_meta:*",
        f"{collection}:function:*:sim:*",
        f"idx:{collection}:baked:functions:*",
        f"idx:{collection}:sim:*",
        f"idx:{collection}:all_similarities",
    ]
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break
    print("[+] Similarity data cleared.")


def sim_quick_status(collection, r=None):
    r = r or get_redis()
    total_set = f"idx:{collection}:indexed:functions"
    total_count = r.scard(total_set)

    algos = ["jaccard", "unweighted_cosine"]
    print(f"[*] Similarity Bake Status for: {collection}")
    for algo in algos:
        baked_set = f"idx:{collection}:baked:functions:{algo}"
        baked_count = r.scard(baked_set)
        unbaked = max(0, total_count - baked_count)

        if unbaked == 0 and total_count > 0:
            print(f"  {algo:<18}: OK : 0 unbaked / {total_count} total")
        else:
            print(f"  {algo:<18}: {unbaked} unbaked / {total_count} total")


def list_sim_batches(collection, batch_filter=None, r=None):
    r = r or get_redis()
    print(f"\n[*] Similarity Bake Status for Collection: {collection}")
    if batch_filter:
        print(f"[*] Filtering for Batch: {batch_filter}")

    print(
        f"{'Batch UUID':<40} | {'Name':<20} | {'Src Funcs':<10} | {'Jaccard':<10} | {'Cosine':<10} | {'Ratio':<6}"
    )
    print("-" * 115)

    batch_uuids = r.smembers("global:batches")
    algos = ["jaccard", "unweighted_cosine"]

    found_any = False
    for uuid in sorted(list(batch_uuids)):
        if batch_filter and batch_filter != uuid:
            continue

        batch_func_set = f"{collection}:batch:{uuid}:functions"
        meta_key = f"{collection}:batch:{uuid}"

        if not r.exists(batch_func_set):
            continue
        found_any = True

        name_raw = r.json().get(meta_key, "$.name")
        name = (
            name_raw[0]
            if (isinstance(name_raw, list) and name_raw)
            else (name_raw or "Unknown")
        )
        total = r.scard(batch_func_set)

        baked_counts = {}
        for algo in algos:
            baked_set = f"idx:{collection}:baked:functions:{algo}"
            try:
                baked_counts[algo] = r.execute_command(
                    "SINTERCARD", "2", batch_func_set, baked_set
                )
            except:
                baked_counts[algo] = len(r.sinter(batch_func_set, baked_set))

        # We use Jaccard as the primary ratio for display, or average them if needed.
        # Here we show the Jaccard ratio.
        ratio = (baked_counts["jaccard"] / total * 100) if total > 0 else 0

        print(
            f"{uuid:<40} | {str(name)[:20]:<20} | {total:<10} | {baked_counts['jaccard']:<10} | {baked_counts['unweighted_cosine']:<10} | {ratio:>5.1f}%"
        )

    if not found_any:
        print(f"[!] No batches found for collection {collection}.")


def main():
    parser = argparse.ArgumentParser(
        description="BSim Turbo Similarity Baker with Resource Guards"
    )
    parser.add_argument("-c", "--collection", required=True, help="Collection name")
    parser.add_argument(
        "--algo",
        default="unweighted_cosine",
        choices=["jaccard", "unweighted_cosine"],
        help="Similarity algorithm",
    )
    parser.add_argument(
        "--md5",
        action="append",
        help="Filter by specific binary MD5 (can be used multiple times)",
    )
    parser.add_argument(
        "--func",
        action="append",
        help="Filter by specific function (ID or MD5:ADDR, can be used multiple times)",
    )
    parser.add_argument(
        "-k", "--top-k", type=int, default=20, help="Top K matches per function"
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.1,
        help="Minimum similarity score (e.g., 0.1)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Artificial delay (seconds) between calculations",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Internal SCAN batch size"
    )
    parser.add_argument(
        "--ignore-indexing",
        action="store_true",
        help="Skip the full-index check before baking",
    )

    args = parser.parse_args()
    bake_enhanced_similarities(args)


if __name__ == "__main__":
    main()
