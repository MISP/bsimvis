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

-- Helper: Robust Token Splitter
local function g_split(inputstr, sep)
    if sep == nil then sep = ":" end
    local t = {}
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
        table.insert(t, str)
    end
    return t
end

-- Helper: Reconstruct FIDs from Lean SID (coll:sim_meta:algo:md5_1:addr_1:md5_2:addr_2)
local function extract_ids(sid)
    local p = g_split(sid, ":")
    if #p < 7 then return nil, nil end
    local fid1 = collection .. ":function:" .. p[4] .. ":" .. p[5]
    local fid2 = collection .. ":function:" .. p[6] .. ":" .. p[7]
    return fid1, fid2
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
        if sort_order == "desc" then range_cmd = "ZREVRANGEBYSCORE"; first = "+inf"; second = producer.min or 0
        else range_cmd = "ZRANGEBYSCORE"; first = producer.min or 0; second = "+inf" end
    else
        range_cmd = "ZRANGEBYSCORE"; first = producer.min or "-inf"; second = producer.max or "+inf"
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
                local s = (producer == g and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
                if s < (g.min or -9999999) or s > (g.max or 9999999) then match = false; break end
            elseif g.type == "feature_range" then
                local f = (producer == g and score) or tonumber(redis.call('ZSCORE', g.key, sid) or 0)
                if f < (g.min or 0) then match = false; break end
            elseif g.type == "metadata" then
                if not id1 then id1, id2 = extract_ids(sid) end
                if not id1 then match = false; break end
                
                local found = false
                for _, b_idx in ipairs(g.buckets) do
                    local b_key = buckets[b_idx]
                    if b_key and (redis.call('SISMEMBER', b_key, id1) == 1 or 
                                  redis.call('SISMEMBER', b_key, id2) == 1 or
                                  redis.call('SISMEMBER', b_key, sid) == 1) then
                        found = true; break
                    end
                end
                if not found then match = false; break end
            end
        end

        if match then
            total_found = total_found + 1
            if #refined < 1000 then
                local final_score = score
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
    if sort_order == "desc" then return a[2] > b[2] else return a[2] < b[2] end
end)

local res_ids, res_scores = {}, {}
for i=1, #refined do
    table.insert(res_ids, refined[i][1])
    table.insert(res_scores, tostring(refined[i][2]))
end

return {total_found, pool_truncated and 1 or 0, res_ids, res_scores}
