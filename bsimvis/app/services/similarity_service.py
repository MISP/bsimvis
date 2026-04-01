import redis
import json
import math
import time
import logging

# --- Shared Lua Scripts ---
LUA_SIM_SCRIPT = """
local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local timestamp = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local target_md5 = ARGV[6]
local target_addr = ARGV[7]
local threshold = tonumber(ARGV[8])
local baked_set_key = ARGV[9]

local target_meta_raw = redis.call('JSON.GET', target_id .. ':meta')
local target_meta = {}
if target_meta_raw then target_meta = cjson.decode(target_meta_raw) end

-- ARGV[10...] are feature hashes
local target_features = {}
for i = 10, #ARGV do
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
            entry_date = timestamp,
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
        if sim_doc.entry_date then redis.call('ZADD', 'idx:' .. collection .. ':sim:entry_date', sim_doc.entry_date, sim_meta_key) end

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
local algo_filter = ARGV[4] or ""

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
    if algo_filter ~= "" and algo ~= algo_filter then return end

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
    rem_item('idx:' .. collection .. ':sim:entry_date', sm_key)
    
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
        if algo_filter == "" or algo == algo_filter then
            rem_item('idx:' .. collection .. ':baked:functions:' .. algo, f_id)
        end
    end
end

return 1
"""

class SimilarityService:
    def __init__(self, r=None):
        if r:
            self.r = r
        else:
            from bsimvis.app.services.redis_client import get_redis
            self.r = get_redis()
        
        self._sim_script = self.r.register_script(LUA_SIM_SCRIPT)
        self._clear_script = self.r.register_script(LUA_CLEAR_FILTERED_SIM_SCRIPT)

    def bake_function(self, collection, base_id, algo="unweighted_cosine", top_k=20, min_score=0.1):
        """
        Bakes a single function against the collection.
        base_id: coll:function:md5:addr
        """
        parts = base_id.split(":")
        if len(parts) < 4:
            return False
        
        md5, addr = parts[2], parts[3]
        vec_key = f"{base_id}:vec:tf"
        baked_set_key = f"idx:{collection}:baked:functions:{algo}"

        # Incremental Skip: Check if already baked
        if self.r.sismember(baked_set_key, base_id):
            return True

        features = self.r.zrange(vec_key, 0, -1)
        if not features:
            return False

        # Lua ARGV: [id, collection, algo, timestamp, top_k, md5, addr, threshold, baked_set, features...]
        lua_args = [
            base_id,
            collection,
            algo,
            int(time.time() * 1000),
            top_k,
            md5,
            addr,
            min_score,
            baked_set_key,
        ] + features

        try:
            self._sim_script(args=lua_args)
            self.r.sadd(baked_set_key, base_id)
            return True
        except Exception as e:
            logging.error(f"SimilarityService: Lua Error for {base_id}: {e}")
            return False

    def clear_filtered(self, collection, field, value, algo=None):
        """
        Targeted similarity deletion.
        field: 'batch_uuid' or 'md5'
        """
        return self._clear_script(args=[collection, field, value, algo or ""])

    def get_pair_score(self, id1, id2, algo="unweighted_cosine"):
        """
        Returns the score for a specific pair, triggering a bake if missing.
        Fallbacks to direct calculation if missing from cache (e.g. below threshold).
        """
        try:
            coll = id1.split(":")[0]

            # 1. Check Cache
            score = self.check_cache(id1, id2, coll, algo)
            if score is not None:
                return score

            # 2. Trigger on-demand bake
            self.bake_function(coll, id1, algo=algo)
            self.bake_function(coll, id2, algo=algo)

            # 3. Re-check Cache
            score = self.check_cache(id1, id2, coll, algo)
            if score is not None:
                return score

            # 4. Final Fallback: Direct Calculation (Sub-threshold or out of Top-K)
            return self.calculate_exact_score(id1, id2, algo=algo)

        except Exception as e:
            logging.error(f"SimilarityService: Error getting pair score: {e}")
            return None

    def calculate_exact_score(self, id1, id2, algo="unweighted_cosine"):
        """Fetches feature vectors and calculates similarity directly in Python."""
        try:
            vec1 = self.r.zrange(f"{id1}:vec:tf", 0, -1)
            vec2 = self.r.zrange(f"{id2}:vec:tf", 0, -1)

            if not vec1 or not vec2:
                return None

            s1 = set(vec1)
            s2 = set(vec2)
            intersect = len(s1.intersection(s2))

            if algo == "jaccard":
                union_len = len(s1) + len(s2) - intersect
                return float(intersect / union_len) if union_len > 0 else 0.0
            elif algo == "unweighted_cosine":
                # Cosine = intersect / sqrt(count1 * count2)
                return float(intersect / math.sqrt(len(s1) * len(s2))) if (len(s1) > 0 and len(s2) > 0) else 0.0
            return None
        except Exception as e:
            logging.error(f"SimilarityService: Error calculating exact score for {id1}, {id2}: {e}")
            return None

    def check_cache(self, id1, id2, collection, algo):
        """Checks if a similarity pair is already baked."""
        key1 = f"{collection}:sim_meta:{algo}:{id1}:{id2}"
        key2 = f"{collection}:sim_meta:{algo}:{id2}:{id1}"
        zset_key = f"{collection}:all_sim:{algo}"
        
        score = self.r.zscore(zset_key, key1)
        if score is None:
            score = self.r.zscore(zset_key, key2)
        
        return float(score) if score is not None else None
