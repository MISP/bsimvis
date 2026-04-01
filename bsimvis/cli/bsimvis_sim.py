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
local baked_set_key = ARGV[8]

local target_meta_raw = redis.call('JSON.GET', target_id .. ':meta')
local target_meta = {}
if target_meta_raw then target_meta = cjson.decode(target_meta_raw) end

-- ARGV[9...] are feature hashes
local target_features = {}
for i = 9, #ARGV do
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
    
    -- --- Robust Canonical Order (Incremental-Safe) ---
    local id_a, id_b
    local meta_a, meta_b
    local md5_a, md5_b
    local addr_a, addr_b
    
    local is_target_greater = (target_id > item.id)
    local is_item_baked = (redis.call('SISMEMBER', baked_set_key, item.id) == 1)
    
    local should_store = false
    if is_target_greater then
        should_store = true
    elseif is_item_baked then
        should_store = true
    end
    
    if should_store then
        local item_meta_raw = redis.call('JSON.GET', item.id .. ':meta')
        local item_meta = {}
        if item_meta_raw then item_meta = cjson.decode(item_meta_raw) end
        
        -- Parse item parts for indexing
        local parts2 = {}
        for p in string.gmatch(item.id, "([^:]+)") do table.insert(parts2, p) end
        local item_md5 = parts2[3] or ""
        local item_addr = parts2[4] or "0"

        if is_target_greater then
            id_a, id_b = target_id, item.id
            meta_a, meta_b = target_meta, item_meta
            md5_a, md5_b = target_md5, item_md5
            addr_a, addr_b = target_addr, item_addr
        else
            id_a, id_b = item.id, target_id
            meta_a, meta_b = item_meta, target_meta
            md5_a, md5_b = item_md5, target_md5
            addr_a, addr_b = item_addr, target_addr
        end

        local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
        
        -- 5. Store similarity metadata for RediSearch
        local sim_meta_key = collection .. ':sim_meta:' .. algo .. ':' .. id_a .. ':' .. id_b
        
        -- 6. Add to collection-wide similarity scoreboard (ZSET)
        redis.call('ZADD', global_zset_key, score_rounded, sim_meta_key)
        local sim_doc = {
            type = "sim",
            collection = collection,
            algo = algo,
            score = score_rounded,
            id1 = id_a,
            id2 = id_b,
            name1 = meta_a["function_name"] or id_a,
            name2 = meta_b["function_name"] or id_b,
            md5_1 = md5_a,
            md5_2 = md5_b,
            tags1 = table.concat(meta_a["tags"] or {}, ","),
            tags2 = table.concat(meta_b["tags"] or {}, ","),
            batch_uuid1 = meta_a["batch_uuid"] or "",
            batch_uuid2 = meta_b["batch_uuid"] or "",
            language_id1 = meta_a["language_id"] or "",
            language_id2 = meta_b["language_id"] or "",
            return_type1 = meta_a["return_type"] or "",
            return_type2 = meta_b["return_type"] or "",
            decompiler1 = meta_a["decompiler_id"] or "",
            decompiler2 = meta_b["decompiler_id"] or "",
            feat_count1 = meta_a["bsim_features_count"] or 0,
            feat_count2 = meta_b["bsim_features_count"] or 0,
            min_features = math.min(meta_a["bsim_features_count"] or 0, meta_b["bsim_features_count"] or 0),
            is_cross_binary = (md5_a ~= md5_b) and "true" or "false"
        }
        redis.call('JSON.SET', sim_meta_key, '$', cjson.encode(sim_doc))

        -- 6.1 Secondary Indexing (Kvrocks-optimized ZSETs)
        local all_key = 'idx:' .. collection .. ':all_similarities'
        local all_type = redis.call('TYPE', all_key)
        if type(all_type) == 'table' then all_type = all_type['ok'] or all_type[1] end
        if all_type == 'set' then redis.call('DEL', all_key) end
        redis.call('ZADD', all_key, 0, sim_meta_key)
        
        -- Unified indexing helper
        local function index_tag(field, value)
            if value and value ~= "" then
                local k = 'idx:' .. collection .. ':sim:' .. field .. ':' .. string.lower(tostring(value))
                local kt = redis.call('TYPE', k)
                if type(kt) == 'table' then kt = kt['ok'] or kt[1] end
                if kt == 'set' then redis.call('DEL', k) end
                redis.call('ZADD', k, 0, sim_meta_key)
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
        index_tag('id1', md5_a .. ':' .. addr_a)
        index_tag('id2', md5_b .. ':' .. addr_b)

        if sim_doc.feat_count1 then redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count1', sim_doc.feat_count1, sim_meta_key) end
        if sim_doc.feat_count2 then redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count2', sim_doc.feat_count2, sim_meta_key) end
        if sim_doc.min_features then redis.call('ZADD', 'idx:' .. collection .. ':sim:min_features', sim_doc.min_features, sim_meta_key) end

        if meta_a["tags"] then for _, t in ipairs(meta_a["tags"]) do index_tag('tags1', t) end end
        if meta_b["tags"] then for _, t in ipairs(meta_b["tags"]) do index_tag('tags2', t) end end
    end
end

return 1
"""


LUA_CLEAR_FILTERED_SIM_SCRIPT = """
local collection = ARGV[1]
local filter_field = ARGV[2] -- 'batch_uuid' or 'md5'
local filter_value = ARGV[3]

local function get_items(k)
    local t_raw = redis.call('TYPE', k)
    local t = (type(t_raw) == 'table') and (t_raw['ok'] or t_raw[1]) or t_raw
    if t == 'zset' then return redis.call('ZRANGE', k, 0, -1) end
    if t == 'set' then return redis.call('SMEMBERS', k) end
    return {}
end

local function rem_item(k, m)
    local t_raw = redis.call('TYPE', k)
    local t = (type(t_raw) == 'table') and (t_raw['ok'] or t_raw[1]) or t_raw
    if t == 'zset' then redis.call('ZREM', k, m) end
    if t == 'set' then redis.call('SREM', k, m) end
end

local function cleanup_key(sm_key)
    local doc_raw = redis.call('JSON.GET', sm_key)
    if not doc_raw then return end
    local doc = cjson.decode(doc_raw)
    
    -- 1. Remove from global ZSETs
    local algo = doc.algo or "unweighted_cosine"
    rem_item(collection .. ':all_sim:' .. algo, sm_key)
    rem_item('idx:' .. collection .. ':all_similarities', sm_key)
    
    -- 2. Remove from field indexes
    local fields = {"name1", "name2", "md5_1", "md5_2", "batch_uuid1", "batch_uuid2", "language_id1", "language_id2", "is_cross_binary", "id1", "id2"}
    for _, f in ipairs(fields) do
        if doc[f] then
            local val = string.lower(tostring(doc[f]))
            rem_item('idx:' .. collection .. ':sim:' .. f .. ':' .. val, sm_key)
        end
    end
    
    -- 3. Numeric Indexes
    rem_item('idx:' .. collection .. ':sim:feat_count1', sm_key)
    rem_item('idx:' .. collection .. ':sim:feat_count2', sm_key)
    rem_item('idx:' .. collection .. ':sim:min_features', sm_key)
    
    -- 4. Tags
    if doc.tags1 then
        for t in string.gmatch(doc.tags1, "([^,]+)") do
            rem_item('idx:' .. collection .. ':sim:tags1:' .. string.lower(t), sm_key)
        end
    end
    if doc.tags2 then
        for t in string.gmatch(doc.tags2, "([^,]+)") do
            rem_item('idx:' .. collection .. ':sim:tags2:' .. string.lower(t), sm_key)
        end
    end
    
    -- 5. Delete the doc itself
    redis.call('DEL', sm_key)
end

-- Find keys via filter indexes
local sep = (filter_field == 'md5') and '_' or ''
local keys1 = get_items('idx:' .. collection .. ':sim:' .. filter_field .. sep .. '1:' .. filter_value)
local keys2 = get_items('idx:' .. collection .. ':sim:' .. filter_field .. sep .. '2:' .. filter_value)

local seen = {}
for _, k in ipairs(keys1) do
    if not seen[k] then
        cleanup_key(k)
        seen[k] = true
    end
end
for _, k in ipairs(keys2) do
    if not seen[k] then
        cleanup_key(k)
        seen[k] = true
    end
end

-- Cleanup the filter indexes themselves
redis.call('DEL', 'idx:' .. collection .. ':sim:' .. filter_field .. sep .. '1:' .. filter_value)
redis.call('DEL', 'idx:' .. collection .. ':sim:' .. filter_field .. sep .. '2:' .. filter_value)

-- Also un-bake the functions associated with this filter
local target_funcs = {}
if filter_field == 'batch_uuid' then
    target_funcs = redis.call('SMEMBERS', collection .. ':batch:' .. filter_value .. ':functions')
elseif filter_field == 'md5' then
    local p = collection .. ':function:' .. filter_value .. ':*:vec:tf'
    local k_raw = redis.call('KEYS', p)
    for i, k in ipairs(k_raw) do
        target_funcs[i] = string.gsub(k, ":vec:tf", "")
    end
end

local algos = {"jaccard", "unweighted_cosine"}
for _, f_id in ipairs(target_funcs) do
    for _, algo in ipairs(algos) do
        rem_item('idx:' .. collection .. ':baked:functions:' .. algo, f_id)
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
    baked_set_key = f"idx:{collection}:baked:functions:{algo}"

    print(
        f"[*] Enhanced Sim Bake | Collection: {collection} | Algo: {algo} | Min Score: {min_score}"
    )

    # Selecting targets based on filters
    candidate_keys = []

    # 1. Binary Filters (MD5s)
    if args.md5:
        for md5 in args.md5:
            candidate_keys.extend(r.keys(f"{collection}:function:{md5}:*:vec:tf"))

    # 2. Function Filters
    if args.func:
        for f in args.func:
            if ":" in f:
                candidate_keys.append(f"{collection}:function:{f}:vec:tf")
            else:
                pattern = f"{collection}:function:*:{f}:vec:tf"
                candidate_keys.extend(r.keys(pattern))

    # 3. Batch Filters
    if getattr(args, "batch", None):
        batch_func_set = f"{collection}:batch:{args.batch}:functions"
        funcs = r.smembers(batch_func_set)
        for f_id in funcs:
            candidate_keys.append(f"{f_id}:vec:tf")

    # If no filters, candidate_keys remains empty but we skip to SCAN
    if not args.md5 and not args.func and not getattr(args, "batch", None):
        candidate_keys = None

    start_time = time.time()
    processed = 0
    skipped = 0

    if candidate_keys is not None:
        # Use set to avoid duplicates if multiple filters overlap
        unique_keys = sorted(list(set(candidate_keys)))
        print(f"[*] Found {len(unique_keys)} keys in batch")
        for key in unique_keys:
            if process_single_key(
                r, sim_script, key, collection, algo, top_k, min_score, baked_set_key
            ):
                processed += 1
            else:
                skipped += 1
            if processed > 0 and processed % 10 == 0:
                print_progress(processed, start_time, collection, skipped)
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
            for key in keys:
                if process_single_key(
                    r, sim_script, key, collection, algo, top_k, min_score, baked_set_key
                ):
                    processed += 1
                else:
                    skipped += 1
                if processed > 0 and processed % 50 == 0:
                    print_progress(processed, start_time, collection, skipped)
                if args.delay > 0:
                    time.sleep(args.delay)
            if cursor == 0:
                break

    print(
        f"[+] DONE: {processed} new functions baked ({skipped} skipped) in {time.time() - start_time:.2f}s"
    )


def process_single_key(r, script, key, collection, algo, top_k, min_score, baked_set_key):
    parts = key.split(":")
    if len(parts) < 4:
        return False
    md5, addr = parts[2], parts[3]
    base_id = f"{collection}:function:{md5}:{addr}"

    # Incremental Skip: Check if already baked
    if r.sismember(baked_set_key, base_id):
        return False

    features = r.zrange(key, 0, -1)
    if not features:
        return False

    # Lua ARGV: [id, collection, algo, top_k, md5, addr, threshold, baked_set, features...]
    lua_args = [base_id, collection, algo, top_k, md5, addr, min_score, baked_set_key] + features

    try:
        script(args=lua_args)
        r.sadd(baked_set_key, base_id)
        return True
    except Exception as e:
        logging.error(f"Lua Error for {base_id}: {e}")
        return False


def print_progress(count, start, collection, skipped=0):
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    skip_str = f" ({skipped} skipped)" if skipped > 0 else ""
    print(
        f"  [i] {collection}: {count} functions baked{skip_str} ({rate:.1f} func/s)..."
    )


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
        sim_clear(coll, batch=args.batch, md5s=args.md5, r=r)
        bake_enhanced_similarities(args, r=r)

    elif args.action == "clear":
        sim_clear(coll, batch=args.batch, md5s=args.md5, r=r)


def sim_clear(collection, batch=None, md5s=None, r=None):
    r = r or get_redis()
    clear_script = r.register_script(LUA_CLEAR_FILTERED_SIM_SCRIPT)

    # 1. Clear by Batch
    if batch:
        print(
            f"[*] Clearing similarity data for collection: {collection}, batch: {batch}..."
        )
        try:
            clear_script(args=[collection, "batch_uuid", batch])
            print(f"[+] Similarity data for batch '{batch}' cleared.")
        except Exception as e:
            print(f"[!] Error clearing batch similarity: {e}")
        if not md5s:
            return

    # 2. Clear by MD5s
    if md5s:
        for md5 in md5s:
            print(
                f"[*] Clearing similarity data for collection: {collection}, md5: {md5}..."
            )
            try:
                clear_script(args=[collection, "md5", md5])
                print(f"[+] Similarity data for md5 '{md5}' cleared.")
            except Exception as e:
                print(f"[!] Error clearing md5 similarity: {e}")
        return

    # 3. Global Clear
    print(f"[*] Clearing ALL similarity data for: {collection}...")
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
    print("[+] ALL similarity data cleared.")


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
        f"{'Batch UUID':<40} | {'Name':<20} | {'Src Funcs':<10} | {'Jac. Count':<10} | {'Jac %':<7} | {'Cos. Count':<10} | {'Cos %':<7}"
    )
    print("-" * 125)

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

        j_ratio = (baked_counts["jaccard"] / total * 100) if total > 0 else 0
        c_ratio = (baked_counts["unweighted_cosine"] / total * 100) if total > 0 else 0

        print(
            f"{uuid:<40} | {str(name)[:20]:<20} | {total:<10} | {baked_counts['jaccard']:<10} | {j_ratio:>6.1f}% | {baked_counts['unweighted_cosine']:<10} | {c_ratio:>6.1f}%"
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
