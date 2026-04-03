local algo_zset = KEYS[1]
local feat_zset = KEYS[2]
local buckets = {}
for i=3, #KEYS do buckets[i-2] = KEYS[i] end

local config = cjson.decode(ARGV[1])
local groups = config.groups or {}
local pool_limit = tonumber(config.pool_limit) or 1000000
local min_sc = tonumber(config.min_score) or 0
local max_sc = tonumber(config.max_score) or 1.0
local min_feat = tonumber(config.min_features) or 0
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

-- 1. Pre-build Lookup Tables for Secondary Groups (i >= 2)
-- Optimization: If a group is "large", we use ZSCORE in the loop instead of a Lua table
local group_lookups = {}
if #groups > 1 then
    for i=2, #groups do
        local g = groups[i]
        if not g.is_large then
            group_lookups[i] = get_union_lookup(g.buckets)
        end
    end
end

-- 2. Candidate Extraction & Filtering
local seen = {}
local use_algo_base = (#groups == 0)

if use_algo_base then
    local raw = redis.call('ZREVRANGE', algo_zset, 0, pool_limit - 1, 'WITHSCORES')
    for i=1, #raw, 2 do
        local sid = raw[i]
        local score = tonumber(raw[i+1])
        if score >= min_sc and score <= max_sc then
            local pass_feat = true
            local f_sc = 0
            if min_feat > 0 or sort_by == "feat_count" then
                f_sc = redis.call('ZSCORE', feat_zset, sid)
                if f_sc then f_sc = tonumber(f_sc) else f_sc = 0 end
                if f_sc < min_feat then pass_feat = false end
            end
            if pass_feat then
                total_found = total_found + 1
                local sort_val = (sort_by == "score") and score or f_sc
                table.insert(refined, {sid, sort_val})
            end
        end
    end
    if #raw / 2 >= pool_limit then pool_truncated = true end
else
    -- Filtered search: Fetch Base Group candidates (Group 1)
    for _, b_idx in ipairs(groups[1].buckets) do
        local b_key = buckets[b_idx]
        -- Optimization: only fetch what we need from the bucket
        local fetch_max = pool_limit - pool_count - 1
        if fetch_max < 0 then break end
        
        local b_ids = redis.call('ZRANGE', b_key, 0, fetch_max) 
        for _, sid in ipairs(b_ids) do
            if not seen[sid] then
                seen[sid] = true
                pool_count = pool_count + 1
                
                -- Intersection check against secondary lookup tables or via ZSCORE
                local match = true
                for i=2, #groups do
                    local g = groups[i]
                    if g.is_large then
                        -- Large group: check each bucket with ZSCORE (usually just one bucket)
                        local bucket_found = false
                        for _, sub_b_idx in ipairs(g.buckets) do
                            if redis.call('ZSCORE', buckets[sub_b_idx], sid) then
                                bucket_found = true
                                break
                            end
                        end
                        if not bucket_found then match = false; break end
                    else
                        -- Small group: use pre-built lookup table
                        if not group_lookups[i][sid] then
                            match = false
                            break
                        end
                    end
                end
                
                if match then
                    local score = redis.call('ZSCORE', algo_zset, sid)
                    if score then
                        score = tonumber(score)
                        if score >= min_sc and score <= max_sc then
                            local pass_feat = true
                            local f_sc = 0
                            if min_feat > 0 or sort_by == "feat_count" then
                                f_sc = redis.call('ZSCORE', feat_zset, sid)
                                if f_sc then f_sc = tonumber(f_sc) else f_sc = 0 end
                                if f_sc < min_feat then pass_feat = false end
                            end
                            if pass_feat then
                                total_found = total_found + 1
                                local sort_val = (sort_by == "score") and score or f_sc
                                table.insert(refined, {sid, sort_val})
                            end
                        end
                    end
                end
                
                if pool_count >= pool_limit then 
                    pool_truncated = true
                    break 
                end
            end
        end
        if pool_truncated then break end
    end
end

-- 3. Sort Results
table.sort(refined, function(a, b)
    if sort_order == "desc" then return a[2] > b[2] else return a[2] < b[2] end
end)

-- 4. Return Top-1000 for caching + global total
local result_ids = {}
local result_scores = {}
for i=1, math.min(#refined, 1000) do
    table.insert(result_ids, refined[i][1])
    table.insert(result_scores, tostring(refined[i][2]))
end

return {total_found, pool_truncated and 1 or 0, result_ids, result_scores}
