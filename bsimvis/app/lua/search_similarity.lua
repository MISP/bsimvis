-- BSimVis Search Similarity Core (Unified Involves Architecture)
local config = cjson.decode(ARGV[1])
local collection = config.collection
local algo = config.algo
local sort_by = config.sort_by or "score"
local sort_order = config.sort_order or "desc"
local pool_limit = tonumber(config.pool_limit or 1000000)
local groups = config.groups or {}
local offset = tonumber(config.offset or 0)
local limit = tonumber(config.limit or 100)

-- Global Score range bounds (passed from Python for map loading)
local min_score = tonumber(config.min_score or 0.95)
local max_score = tonumber(config.max_score or 1.0)

local algo_zset = KEYS[1]
local feat_zset = KEYS[2]

-- Helper: Reconstruct FIDs from Lean SID (idx:coll:sim:algo:id1:id2)
local function extract_ids(sid)
    -- sid is idx:coll:sim:algo:idx:coll:func:md5:addr:idx:coll:func:md5:addr
    local sim_prefix = 'idx:' .. collection .. ':sim:' .. algo .. ':'
    if sid:sub(1, #sim_prefix) ~= sim_prefix then return nil, nil end
    
    local rest = sid:sub(#sim_prefix + 1)
    -- We look for the start of the second ID: ':idx:{coll}:func:'
    local sep = ':idx:' .. collection .. ':func:'
    local pivot = rest:find(sep, 2, true) 
    if not pivot then return nil, nil end
    
    local id1 = rest:sub(1, pivot - 1)
    local id2 = rest:sub(pivot + 1)
    return id1, id2
end

-- 1. Pre-resolve Filter Maps
local producer = nil
local filter_maps = {} -- index -> Map of { member = true }
local score_map = nil  -- sid -> score (if narrow range)

-- Identify Producer and handle pre-loading
for idx, g in ipairs(groups) do
    if not producer and (g.type == "score_range" or g.type == "feature_range" or g.type == "metadata" or g.type == "direct_zset") then
        producer = g
        producer.idx = idx
    end

    if g.type == "metadata" and g.targets then
        local allowed = {}
        local prefix = ""
        if g.level == "binary" then prefix = "idx:" .. collection .. ":sim:involves:file:"
        elseif g.level == "function" then prefix = "idx:" .. collection .. ":sim:involves:func:"
        elseif g.level == "similarity" then prefix = "idx:" .. collection .. ":sim:tags:" end
        
        if prefix ~= "" then
            -- Chunked UNION to resolve all SIDs for these targets
            local targets = g.targets
            for j=1, #targets, 5000 do
                local keys = {}
                for k=j, math.min(j+4999, #targets) do table.insert(keys, prefix .. targets[k]) end
                local members = redis.call('SUNION', unpack(keys)) or {}
                for _, m in ipairs(members) do allowed[m] = true end
            end
        end
        filter_maps[idx] = allowed
    elseif g.type == "score_range" and g.weight < 50000 then
        -- KILLER FEATURE: Pre-load small score ranges to eliminate ZSCORE in loops
        local s_items = redis.call('ZRANGEBYSCORE', algo_zset, min_score, max_score, 'WITHSCORES')
        local smap = {}
        for j=1, #s_items, 2 do smap[s_items[j]] = tonumber(s_items[j+1]) end
        score_map = smap
    end
end

if not producer then
    producer = {type="score_range", key=algo_zset, min=min_score, max=max_score, idx=0}
end

local refined = {}
local total_found = 0
local pool_truncated = false
local raw = {} -- sid, score pairs
local sorting_on_producer = false

-- 2. Producer Phase: Generate candidate pool
if producer.type == "metadata" then
    local seen_sids = {}
    local unsorted_raw = {}
    local targets = producer.targets
    local prefix = ""
    if producer.level == "binary" then prefix = "idx:" .. collection .. ":sim:involves:file:"
    elseif producer.level == "function" then prefix = "idx:" .. collection .. ":sim:involves:func:"
    elseif producer.level == "similarity" then prefix = "idx:" .. collection .. ":sim:tags:" end
    
    for j=1, #targets, 5000 do
        local keys = {}
        for k=j, math.min(j+4999, #targets) do table.insert(keys, prefix .. targets[k]) end
        local sids = redis.call('SUNION', unpack(keys)) or {}
        
        for _, sid in ipairs(sids) do
            if not seen_sids[sid] then
                seen_sids[sid] = true
                local score = (score_map and score_map[sid]) or tonumber(redis.call('ZSCORE', algo_zset, sid) or 0)
                if score >= min_score and score <= max_score then
                    table.insert(unsorted_raw, sid)
                    table.insert(unsorted_raw, tostring(score))
                end
                if (#unsorted_raw / 2) >= pool_limit then break end
            end
        end
        if (#unsorted_raw / 2) >= pool_limit then break end
    end
    raw = unsorted_raw
    sorting_on_producer = (sort_by == "score")
elseif producer.type == "score_range" or producer.type == "feature_range" or producer.type == "direct_zset" then
    sorting_on_producer = (sort_by == "score" and producer.type == "score_range") or ((sort_by == "min_features" or sort_by == "feat_count") and producer.type == "feature_range")
    local first, second, range_cmd
    if sorting_on_producer and sort_order == "desc" then
        range_cmd = "ZREVRANGEBYSCORE"; first = producer.max or "+inf"; second = producer.min or "-inf"
    else
        range_cmd = "ZRANGEBYSCORE"; first = producer.min or "-inf"; second = producer.max or "+inf"
    end
    raw = redis.call(range_cmd, producer.key, first, second, 'WITHSCORES', 'LIMIT', 0, pool_limit)
end

if #raw / 2 >= pool_limit then pool_truncated = true end

-- 3. Main Loop: Fast In-Memory Filtering
for i=1, #raw, 2 do
    local sid = raw[i]
    local score = tonumber(raw[i+1])
    local id1, id2 = nil, nil
    local match = true
    
    for idx, g in ipairs(groups) do
        if idx == producer.idx then
            -- Redundant
        elseif g.type == "score_range" then
            local s = score -- We always have score in 'raw' (either from producer or manual expansion)
            if s < min_score or s > max_score then match = false; break end
        elseif g.type == "feature_range" then
            local f = tonumber(redis.call('ZSCORE', g.key, sid) or 0)
            if f < (g.min or 0) then match = false; break end
        elseif g.type == "metadata" then
            -- In-Memory Map Check (Fast Intersect)
            local map = filter_maps[idx]
            if not map[sid] then
                -- Check constituent FIDs (Deep Join)
                if not id1 then id1, id2 = extract_ids(sid) end
                if not id1 or (not map[id1] and not map[id2]) then match = false; break end
            end
        elseif g.type == "direct_zset" then
            if not redis.call('ZSCORE', g.key, sid) then match = false; break end
        end
    end
    
    if match then
        total_found = total_found + 1
        if #refined < 1000 then
            local final_metric = score
            if sort_by == "feat_count" or sort_by == "min_features" then
                final_metric = tonumber(redis.call('ZSCORE', feat_zset, sid) or 0)
            end
            table.insert(refined, {sid, final_metric})
        end
    end
end

-- 4. Result Finalization
table.sort(refined, function(a, b)
    if a[2] ~= b[2] then
        if sort_order == "desc" then return a[2] > b[2] else return a[2] < b[2] end
    end
    return a[1] < b[1]
end)

local res_ids, res_scores = {}, {}
for i=1, math.min(#refined, limit+offset) do
    if i > offset then
        table.insert(res_ids, refined[i][1])
        table.insert(res_scores, tostring(refined[i][2]))
    end
end

return {total_found, pool_truncated and 1 or 0, res_ids, res_scores}
