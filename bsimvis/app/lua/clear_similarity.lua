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
    local doc_raw = redis.call('GET', sm_key)
    if not doc_raw then return end
    local doc = cjson.decode(doc_raw)
    
    local algo = doc.algo or "unweighted_cosine"
    if algo_filter ~= "" and algo ~= algo_filter then return end

    -- 1. Remove from global ZSETs
    redis.call('ZREM', collection .. ':sim:score:' .. algo, sm_key)
    redis.call('ZREM', collection .. ':sim:all', sm_key)
    redis.call('ZREM', collection .. ':sim:min_features', sm_key)
    
    -- 2. Remove from involves indexes
    local clean_id1 = doc.id1:gsub("^" .. collection .. ":func:", "")
    local clean_id2 = doc.id2:gsub("^" .. collection .. ":func:", "")
    
    redis.call('SREM', collection .. ':sim:involves:func:' .. clean_id1, sm_key)
    redis.call('SREM', collection .. ':sim:involves:func:' .. clean_id2, sm_key)
    redis.call('SREM', collection .. ':sim:involves:file:' .. doc.md5_1, sm_key)
    redis.call('SREM', collection .. ':sim:involves:file:' .. doc.md5_2, sm_key)
    
    -- 3. Remove from secondary tag indexes
    if doc.user_tags then
        for _, t in ipairs(doc.user_tags) do
             redis.call('SREM', collection .. ':idx:sim:user_tags:' .. string.lower(t), sm_key)
        end
    end
    if doc.tags then
        for _, t in ipairs(doc.tags) do
             redis.call('SREM', collection .. ':idx:sim:tags:' .. string.lower(t), sm_key)
        end
    end
    
    -- 4. Delete the doc itself
    redis.call('DEL', sm_key)
end

-- Find keys via involves index
local index_key = ""
if filter_field == 'md5' then
    index_key = collection .. ':sim:involves:file:' .. filter_value
elseif filter_field == 'batch_uuid' then
    -- Batch involves is currently mapped via the batch:functions set -> individual involves:func
    local b_funcs = redis.call('SMEMBERS', collection .. ':batch:' .. filter_value .. ':functions')
    for _, f_id in ipairs(b_funcs) do
        local clean_fid = f_id:gsub("^" .. collection .. ":func:", "")
        local f_sims = get_items(collection .. ':sim:involves:func:' .. clean_fid)
        for _, sm_key in ipairs(f_sims) do
            cleanup_key(sm_key)
        end
    end
    -- No central batch involves key yet, so we return here after per-function cleanup
end

if index_key ~= "" then
    local keys = get_items(index_key)
    for _, k in ipairs(keys) do
        cleanup_key(k)
    end
    -- Cleanup the filter index itself
    redis.call('DEL', index_key)
end

-- Also un-build the functions associated with this filter to allow re-building
local target_funcs = {}
if filter_field == 'batch_uuid' then
    target_funcs = redis.call('SMEMBERS', collection .. ':batch:' .. filter_value .. ':functions')
elseif filter_field == 'md5' then
    target_funcs = redis.call('SMEMBERS', collection .. ':idx:file:functions:' .. filter_value)
end

local algos = {"jaccard", "unweighted_cosine"}
for _, f_id in ipairs(target_funcs) do
    for _, algo in ipairs(algos) do
        if algo_filter == "" or algo == algo_filter then
            redis.call('SREM', collection .. ':built:functions:' .. algo, f_id)
        end
    end
end

return true
