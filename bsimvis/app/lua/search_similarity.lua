-- BSimVis Search Similarity Core (Batch-Optimized Arch)
local config = cjson.decode(ARGV[1])
local collection = config.collection
local algo = config.algo
local sort_by = config.sort_by or "score"
local sort_order = config.sort_order or "desc"
local pool_limit = tonumber(config.pool_limit or 1000000)
local groups = config.groups or {}
local offset = tonumber(config.offset or 0)
local limit = tonumber(config.limit or 100)

local buckets = {}
for i=3, #KEYS do buckets[i-2] = KEYS[i] end

-- Helper: Reconstruct FIDs from Lean SID (coll:sim_meta:algo:id1:id2)
local function extract_ids(sid)
    local sim_prefix = collection .. ":sim_meta:" .. algo .. ":"
    if sid:sub(1, #sim_prefix) ~= sim_prefix then return nil, nil end
    local rest = sid:sub(#sim_prefix + 1)
    
    local func_prefix = ":" .. collection .. ":function:"
    local pivot = rest:find(func_prefix, 2, true) 
    if not pivot then return nil, nil end
    
    local id1 = rest:sub(1, pivot - 1)
    local id2 = rest:sub(pivot + 1)
    return id1, id2
end

-- 1. Identify Producer & Load Filters
local producer = nil
local filter_maps = {} -- index -> Map of { member = true }

for idx, g in ipairs(groups) do
    if not producer then
        if g.type == "score_range" or g.type == "feature_range" or g.type == "metadata" then
            producer = g
            producer.idx = idx
        end
    end
    
    -- Pre-load metadata filters into Lua memory
    if g.type == "metadata" then
        local allowed = {}
        local keys = {}
        for _, b_idx in ipairs(g.buckets) do table.insert(keys, buckets[b_idx]) end
        
        if #keys > 0 then
            -- SUNION supports many keys (chunking if needed, but 8000 is common limit)
            local members = redis.call('SUNION', unpack(keys))
            for _, m in ipairs(members) do allowed[m] = true end
        end
        filter_maps[idx] = allowed
    end
end

if not producer then
    producer = {type="score_range", key=KEYS[1], min="-inf", max="+inf", idx=0}
end

local refined = {}
local total_found = 0
local pool_truncated = false
local raw = {}
local sorting_on_producer = false

-- 2. Producer Phase: Generate candidate pool
if producer.type == "metadata" then
    local seen_sids = {}
    local unsorted_raw = {}
    
    -- Global bounds for early truncation
    local min_score, max_score = -9999999, 9999999
    local min_features = 0
    for _, grp in ipairs(groups) do
        if grp.type == "score_range" then
            min_score = grp.min or min_score
            max_score = grp.max or max_score
        elseif grp.type == "feature_range" then
            min_features = grp.min or min_features
        end
    end

    for i, b_idx in ipairs(producer.buckets) do
        local b_key = buckets[b_idx]
        local is_func = (string.find(b_key, ":function:") ~= nil)
        
        local members = redis.call('SMEMBERS', b_key)
        local sids = {}
        
        if is_func then
            -- Mass bridge expansion
            local bridge_keys = {}
            for _, fid in ipairs(members) do table.insert(bridge_keys, 'idx:' .. collection .. ':function_to_sim:' .. fid) end
            
            -- Chunked expansion to respect unpack limit (~7000)
            local expanded = {}
            for j=1, #bridge_keys, 5000 do
                local chunk = {}
                for k=j, math.min(j+4999, #bridge_keys) do table.insert(chunk, bridge_keys[k]) end
                local res = redis.call('SUNION', unpack(chunk))
                for _, s in ipairs(res) do table.insert(expanded, s) end
            end
            sids = expanded
        else
            sids = members
        end
        
        for _, sid in ipairs(sids) do
            if not seen_sids[sid] then
                seen_sids[sid] = true
                local val = tonumber(redis.call('ZSCORE', (sort_by == "score" and KEYS[1] or KEYS[2]), sid) or 0)
                
                local in_range = false
                if sort_by == "score" then
                    if val >= min_score and val <= max_score then in_range = true end
                else
                    if val >= min_features then in_range = true end
                end
                
                if in_range then
                    table.insert(unsorted_raw, sid)
                    table.insert(unsorted_raw, tostring(val))
                end
                if (#unsorted_raw / 2) >= pool_limit then break end
            end
        end
        if (#unsorted_raw / 2) >= pool_limit then break end
    end
    raw = unsorted_raw
    sorting_on_producer = true
else
    local prefix = producer.type:sub(1,5)
    sorting_on_producer = (sort_by == prefix or (sort_by == "feat_count" and producer.type == "feature_range"))
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
        if idx == producer.idx and producer.type == "metadata" then
            -- Producer metadata check is redundant, we already have the SID from its bucket
        elseif g.type == "score_range" then
            local s = ((producer.idx == idx) and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
            if s < (g.min or -9999999) or s > (g.max or 9999999) then match = false; break end
        elseif g.type == "feature_range" then
            local f = ((producer.idx == idx) and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
            if f < (g.min or 0) then match = false; break end
        elseif g.type == "metadata" then
            local map = filter_maps[idx]
            if not map[sid] then
                if not id1 then id1, id2 = extract_ids(sid) end
                if not id1 or (not map[id1] and not map[id2]) then match = false; break end
            end
        end
    end
    
    if match then
        total_found = total_found + 1
        if #refined < 1000 then
            local final_score = score
            if not sorting_on_producer then
                final_score = tonumber(redis.call('ZSCORE', (sort_by == "score" and KEYS[1] or KEYS[2]), sid) or 0)
            end
            table.insert(refined, {sid, final_score})
        end
    end
end

-- 4. Result Finalization
table.sort(refined, function(a, b)
    local s1, s2 = a[2], b[2]
    if s1 ~= s2 then
        if sort_order == "desc" then return s1 > s2 else return s1 < s2 end
    end
    return a[1] < b[1]
end)

local res_ids, res_scores = {}, {}
for i=1, math.min(#refined, limit + offset) do
    if i > offset then
        table.insert(res_ids, refined[i][1])
        table.insert(res_scores, tostring(refined[i][2]))
    end
end

return {total_found, pool_truncated and 1 or 0, res_ids, res_scores}
