-- BSimVis Search Similarity Core (Deep Selection Architecture)
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

-- Preprocess metadata groups for O(1) Lua-local matching
for _, g in ipairs(groups) do
    if g.type == "metadata" and g.members then
        local lookup = {}
        for _, m in ipairs(g.members) do
            lookup[m] = true
        end
        g.lookup = lookup
    end
end

-- Helper: Robust Token Splitter
local function g_split(inputstr, sep)
    if sep == nil then sep = ":" end
    local t = {}
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
        table.insert(t, str)
    end
    return t
end

-- Helper: Reconstruct FIDs from Lean SID (coll:sim_meta:algo:id1:id2)
-- where id is coll:function:md5:addr
local function extract_ids(sid)
    local sim_prefix = collection .. ":sim_meta:" .. algo .. ":"
    if sid:sub(1, #sim_prefix) ~= sim_prefix then return nil, nil end
    local rest = sid:sub(#sim_prefix + 1)
    
    local func_prefix = ":" .. collection .. ":function:"
    local pivot = rest:find(func_prefix, 2, true) -- Look for the second occurrence start
    if not pivot then return nil, nil end
    
    local id1 = rest:sub(1, pivot - 1)
    local id2 = rest:sub(pivot + 1)
    return id1, id2
end

local producer = nil
for _, g in ipairs(groups) do
    if g.type == "score_range" or g.type == "feature_range" then producer = g; break end
end
if not producer then
    producer = {type="score_range", key=KEYS[1], min="-inf", max="+inf"}
end

local refined = {}
local total_found = 0
local pool_truncated = false

local range_cmd, first, second
if producer.type == "score_range" or producer.type == "feature_range" then
    local prefix = producer.type:sub(1,5)
    local sorting_on_producer = (sort_by == prefix or (sort_by == "feat_count" and producer.type == "feature_range"))
    if sorting_on_producer then
        if sort_order == "desc" then 
            range_cmd = "ZREVRANGEBYSCORE"; 
            first = producer.max or "+inf"; 
            second = producer.min or "-inf"
        else 
            range_cmd = "ZRANGEBYSCORE"; 
            first = producer.min or "-inf"; 
            second = producer.max or "+inf"
        end
    else
        range_cmd = "ZRANGEBYSCORE"; 
        first = producer.min or "-inf"; 
        second = producer.max or "+inf"
    end

    local raw = redis.call(range_cmd, producer.key, first, second, 'WITHSCORES', 'LIMIT', 0, pool_limit)
    if #raw / 2 >= pool_limit then pool_truncated = true end

    for i=1, #raw, 2 do
        local sid = raw[i]
        local score = tonumber(raw[i+1])
        local id1, id2 = nil, nil
        local match = true

        for _, g in ipairs(groups) do
            if g.type == "score_range" then
                local s = ((producer.type == g.type and producer.key == g.key) and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
                if s < (g.min or -9999999) or s > (g.max or 9999999) then match = false; break end
            elseif g.type == "feature_range" then
                local f = ((producer.type == g.type and producer.key == g.key) and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
                if f < (g.min or 0) then match = false; break end
            elseif g.type == "metadata" then
                if not id1 then id1, id2 = extract_ids(sid) end
                if not id1 then match = false; break end
                
                local found = false
                if g.lookup then
                    -- High-performance Lua local match
                    if g.lookup[id1] or g.lookup[id2] or g.lookup[sid] then
                        found = true
                    end
                else
                    -- Fallback: Regular SISMEMBER check
                    for _, b_idx in ipairs(g.buckets) do
                        local b_key = buckets[b_idx]
                        if b_key and (redis.call('SISMEMBER', b_key, id1) == 1 or 
                                      redis.call('SISMEMBER', b_key, id2) == 1 or
                                      redis.call('SISMEMBER', b_key, sid) == 1) then
                            found = true; break
                        end
                    end
                end
                if not found then match = false; break end
            end
        end

        if match then
            total_found = total_found + 1
            if #refined < 1000 then
                local final_score = tonumber(score) or 0
                local is_prod = (producer.type == "score_range" and producer.key == KEYS[1]) or (producer.type == "feature_range" and producer.key == KEYS[2])
                
                if not sorting_on_producer then
                    local skey = (sort_by == "score") and KEYS[1] or KEYS[2]
                    final_score = tonumber(redis.call('ZSCORE', skey, sid) or 0)
                end
                table.insert(refined, {sid, final_score})
            end
        end
    end
end

table.sort(refined, function(a, b)
    local s1, s2 = tonumber(a[2]) or 0, tonumber(b[2]) or 0
    if s1 ~= s2 then
        if sort_order == "desc" then return s1 > s2 else return s1 < s2 end
    end
    -- Fallback to stable sort on SID if scores are identical
    return a[1] < b[1]
end)

local res_ids, res_scores = {}, {}
for i=1, #refined do
    table.insert(res_ids, refined[i][1])
    table.insert(res_scores, tostring(refined[i][2]))
end

return {total_found, pool_truncated and 1 or 0, res_ids, res_scores}
