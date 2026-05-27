-- BSimVis Search Function Core
local config = cjson.decode(ARGV[1])
local collection = config.collection
local sort_by = config.sort_by or "id"
local sort_order = config.sort_order or "desc"
local pool_limit = tonumber(config.pool_limit or 1000000)
local groups = config.groups or {}
local offset = tonumber(config.offset or 0)
local limit = tonumber(config.limit or 100)

-- 1. Pre-resolve Filter Maps
local producer = nil
local filter_maps = {} -- index -> Map of { member = true }

-- Identify Producer
for idx, g in ipairs(groups) do
    if not producer and not g.exclude and (g.type == "metadata" or g.type == "numeric_range") then
        producer = g
        producer.idx = idx
    end

    if g.type == "metadata" then
        local allowed = {}
        local sub_groups = g.sub_groups or { {level=g.level, targets=g.targets, field=g.field} }
        
        for _, sub in ipairs(sub_groups) do
            local targets = sub.targets or {}
            if sub.level == "file" then 
                local file_prefix = collection .. ":idx:file:" .. sub.field .. ":"
                local file_ids = {}
                for j=1, #targets, 5000 do
                    local keys = {}
                    for k=j, math.min(j+4999, #targets) do 
                        table.insert(keys, file_prefix .. targets[k]) 
                    end
                    local f_ids = redis.call('SUNION', unpack(keys)) or {}
                    for _, f_id in ipairs(f_ids) do table.insert(file_ids, f_id) end
                end
                
                if #file_ids > 0 then
                    local func_keys = {}
                    for _, f_id in ipairs(file_ids) do
                        local md5 = string.match(f_id, ":file:([^:]+)")
                        if md5 then
                            table.insert(func_keys, collection .. ":idx:file:functions:" .. md5)
                        end
                    end
                    for j=1, #func_keys, 5000 do
                        local chunk = {}
                        for k=j, math.min(j+4999, #func_keys) do table.insert(chunk, func_keys[k]) end
                        local members = redis.call('SUNION', unpack(chunk)) or {}
                        for _, m in ipairs(members) do allowed[m] = true end
                    end
                end
            elseif sub.level == "func" then 
                local prefix = collection .. ":idx:func:" .. sub.field .. ":"
                if sub.targets then
                    local targets = sub.targets
                    for j=1, #targets, 5000 do
                        local keys = {}
                        for k=j, math.min(j+4999, #targets) do 
                            table.insert(keys, prefix .. targets[k]) 
                        end
                        local members = redis.call('SUNION', unpack(keys)) or {}
                        for _, m in ipairs(members) do allowed[m] = true end
                    end
                end
            end
        end
        filter_maps[idx] = allowed
    end
end

if not producer then
    producer = {type="all", key=collection .. ":all_functions", idx=0}
end

local refined = {}
local total_found = 0
local pool_truncated = false
local raw_ids = {} 

-- 2. Producer Phase: Generate candidate pool
if producer.type == "metadata" then
    local allowed = filter_maps[producer.idx]
    if allowed then
        for fid, _ in pairs(allowed) do
            table.insert(raw_ids, fid)
            if #raw_ids >= pool_limit then break end
        end
    end
elseif producer.type == "all" then
    raw_ids = redis.call('SMEMBERS', producer.key)
elseif producer.type == "numeric_range" then
    local range_cmd = (sort_order == "desc") and "ZREVRANGEBYSCORE" or "ZRANGEBYSCORE"
    local first = (sort_order == "desc") and (producer.max or "+inf") or (producer.min or "-inf")
    local second = (sort_order == "desc") and (producer.min or "-inf") or (producer.max or "+inf")
    raw_ids = redis.call(range_cmd, producer.key, first, second, 'LIMIT', 0, pool_limit)
end

if #raw_ids >= pool_limit then pool_truncated = true end

-- 3. Main Loop: Fast In-Memory Filtering
for i=1, #raw_ids do
    local fid = raw_ids[i]
    local match = true
    
    for idx, g in ipairs(groups) do
        if idx == producer.idx then
            -- Already handled
        elseif g.type == "metadata" then
            local map = filter_maps[idx]
            if map then
                if g.exclude then
                    if map[fid] then match = false; break end
                else
                    if not map[fid] then match = false; break end
                end
            end
        elseif g.type == "numeric_range" then
            local score = tonumber(redis.call('ZSCORE', g.key, fid) or 0)
            if score < (g.min or -math.huge) or score > (g.max or math.huge) then 
                match = false; break 
            end
        end
    end
    
    if match then
        total_found = total_found + 1
        -- For simplicity, we just store the ID. Sorting might be added later if needed.
        -- If sort_by is numeric, we should fetch it.
        local sort_val = fid
        if sort_by ~= "id" then
            local sort_key = collection .. ":idx:func:" .. sort_by
            sort_val = tonumber(redis.call('ZSCORE', sort_key, fid) or 0)
        end
        table.insert(refined, {fid, sort_val})
    end
end

-- 4. Result Finalization
table.sort(refined, function(a, b)
    if type(a[2]) == "number" and type(b[2]) == "number" then
        if a[2] ~= b[2] then
            if sort_order == "desc" then return a[2] > b[2] else return a[2] < b[2] end
        end
    end
    return a[1] < b[1]
end)

local res_ids = {}
for i=1, math.min(#refined, limit+offset) do
    if i > offset then
        table.insert(res_ids, refined[i][1])
    end
end

return {total_found, pool_truncated and 1 or 0, res_ids}
