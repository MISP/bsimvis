local algo_zset = KEYS[1]
local feat_zset = KEYS[2]
local buckets = {}
for i=3, #KEYS do buckets[i-2] = KEYS[i] end

local config = cjson.decode(ARGV[1])
local groups = config.groups or {}
local pool_limit = tonumber(config.pool_limit) or 1000000
local offset = tonumber(config.offset) or 0
local limit = tonumber(config.limit) or 100
local sort_by = config.sort_by or "score"
local sort_order = config.sort_order or "desc"

local refined = {}
local total_found = 0
local pool_count = 0
local pool_truncated = false

-- Helper: Get Union of Multiple Buckets in Safe Chunks (max 500 keys per ZUNION)
local function get_union_lookup(group_indices)
    local lookup = {}
    local b_keys = {}
    for _, idx in ipairs(group_indices) do table.insert(b_keys, buckets[idx]) end
    
    for i=1, #b_keys, 500 do
        local chunk = {}
        for j=i, math.min(i+499, #b_keys) do table.insert(chunk, b_keys[j]) end
        -- native ZUNION is much faster than ZRANGE loop in Lua
        local ids = redis.call('ZUNION', #chunk, unpack(chunk))
        for _, id in ipairs(ids) do
            lookup[id] = true
        end
    end
    return lookup
end

-- 1. Pre-build Lookup Tables for Metadata Groups
local group_lookups = {}
for i, g in ipairs(groups) do
    if g.type == "metadata" and not g.is_large then
        group_lookups[i] = get_union_lookup(g.buckets)
    end
end

-- 2. Unified Candidate Extraction & Filtering
local seen = {}
local target_group = groups[1]
local consumers = {}
for i=2, #groups do table.insert(consumers, {idx=i, g=groups[i]}) end

-- Helper: Verify a candidate against all consumer groups
local function verify_match(sid, score_in, f_sc_in)
    local match = true
    local current_score = score_in
    local current_f_sc = f_sc_in
    
    for _, item in ipairs(consumers) do
        local g = item.g
        local g_idx = item.idx
        
        if g.type == "score_range" then
            if not current_score then
                current_score = tonumber(redis.call('ZSCORE', algo_zset, sid) or 0)
            end
            if current_score < g.min or current_score > g.max then match = false; break end
            
        elseif g.type == "feature_range" then
            if not current_f_sc then
                current_f_sc = tonumber(redis.call('ZSCORE', feat_zset, sid) or 0)
            end
            if current_f_sc < g.min then match = false; break end
            
        elseif g.type == "metadata" then
            if g.is_large then
                local bucket_found = false
                for _, b_idx in ipairs(g.buckets) do
                    if redis.call('ZSCORE', buckets[b_idx], sid) then
                        bucket_found = true; break
                    end
                end
                if not bucket_found then match = false; break end
            else
                if not group_lookups[g_idx][sid] then
                    match = false; break
                end
            end
        end
    end
    return match, current_score, current_f_sc
end

if target_group then
    if target_group.type == "score_range" or target_group.type == "feature_range" then
        local range_cmd, first, second
        local base_key = target_group.key
        
        -- Determine if we are sorting on the producer's field
        local producer_prefix = target_group.type:sub(1,5)
        local sorting_on_producer = (sort_by == producer_prefix or (sort_by == "feat_count" and producer_prefix == "featu"))
        
        if sorting_on_producer then
            if sort_order == "desc" then
                range_cmd = "ZREVRANGEBYSCORE"
                first = "+inf"
                second = target_group.min or 0
            else
                range_cmd = "ZRANGEBYSCORE"
                first = target_group.min or 0
                second = "+inf"
            end
        else
            -- Producer is just a filter; use its full range efficiently
            range_cmd = "ZRANGEBYSCORE"
            first = target_group.min or 0
            second = target_group.max or "+inf"
        end

        local raw = redis.call(range_cmd, base_key, first, second, 'WITHSCORES', 'LIMIT', 0, pool_limit)
        for i=1, #raw, 2 do
            local sid = raw[i]
            local val = tonumber(raw[i+1])
            local is_match, score, f_sc
            if target_group.type == "score_range" then
                is_match, score, f_sc = verify_match(sid, val, nil)
            else
                is_match, score, f_sc = verify_match(sid, nil, val)
            end
            
            if is_match then
                total_found = total_found + 1
                -- Use available metrics to populate the sort value
                local s_val = (sort_by == "score") and (score or val) or (f_sc or val)
                table.insert(refined, {sid, s_val})
            end
        end
        if #raw / 2 >= pool_limit then pool_truncated = true end

    elseif target_group.type == "metadata" then
        for _, b_idx in ipairs(target_group.buckets) do
            local b_key = buckets[b_idx]
            local fetch_max = pool_limit - pool_count - 1
            if fetch_max < 0 then break end
            
            local b_ids = redis.call('ZRANGE', b_key, 0, fetch_max) 
            for _, sid in ipairs(b_ids) do
                if not seen[sid] then
                    seen[sid] = true
                    pool_count = pool_count + 1
                    
                    local is_match, score, f_sc = verify_match(sid, nil, nil)
                    if is_match then
                        total_found = total_found + 1
                        -- Enforce score/feature lookup if they weren't checked in consumers
                        if not score then score = tonumber(redis.call('ZSCORE', algo_zset, sid) or 0) end
                        if not f_sc then f_sc = tonumber(redis.call('ZSCORE', feat_zset, sid) or 0) end
                        
                        local s_val = (sort_by == "score") and score or f_sc
                        table.insert(refined, {sid, s_val})
                    end
                    
                    if pool_count >= pool_limit then 
                        pool_truncated = true; break 
                    end
                end
            end
            if pool_truncated then break end
        end
    end
end

-- 3. Final Sort
-- Required if:
-- 1. There are multiple filters.
-- 2. We used metadata buckets (unordered).
-- 3. The producer field wasn't what we are sorting by.
local producer_prefix = target_group and target_group.type:sub(1,5) or ""
local sorted_on_producer = (sort_by == producer_prefix or (sort_by == "feat_count" and producer_prefix == "featu"))

if #groups > 1 or (target_group and target_group.type == "metadata") or not sorted_on_producer then
    table.sort(refined, function(a, b)
        if sort_order == "desc" then return a[2] > b[2] else return a[2] < b[2] end
    end)
end

-- 4. Return Top-1000 for caching + global total
local result_ids = {}
local result_scores = {}
for i=1, math.min(#refined, 1000) do
    table.insert(result_ids, refined[i][1])
    table.insert(result_scores, tostring(refined[i][2]))
end

return {total_found, pool_truncated and 1 or 0, result_ids, result_scores}
