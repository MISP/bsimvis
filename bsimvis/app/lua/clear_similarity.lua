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
    rem_item(collection .. ':sim:all_similarities', sm_key)
    
    -- 2. Remove from field indexes
    local fields = {"name1", "name2", "md5_1", "md5_2", "batch_uuid1", "batch_uuid2", "language_id1", "language_id2", "is_cross_binary", "id1", "id2"}
    for _, f in ipairs(fields) do
        if doc[f] then
            local val = string.lower(tostring(doc[f]))
            rem_item(collection .. ':sim:' .. f .. ':' .. val, sm_key)
        end
    end
    
    -- 3. Numeric Indexes
    rem_item(collection .. ':sim:feat_count', sm_key)
    rem_item(collection .. ':sim:min_features', sm_key)
    rem_item(collection .. ':sim:entry_date', sm_key)
    
    -- 4. Tags
    if doc.tags1 then
        for t in string.gmatch(doc.tags1, "([^,]+)") do
            rem_item(collection .. ':sim:tags1:' .. string.lower(t), sm_key)
        end
    end
    if doc.tags2 then
        for t in string.gmatch(doc.tags2, "([^,]+)") do
            rem_item(collection .. ':sim:tags2:' .. string.lower(t), sm_key)
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
redis.call('DEL', collection .. ':sim:' .. filter_field .. sep .. '1:' .. filter_value)
redis.call('DEL', collection .. ':sim:' .. filter_field .. sep .. '2:' .. filter_value)

-- Also un-build the functions associated with this filter
local target_funcs = {}
if filter_field == 'batch_uuid' then
    target_funcs = redis.call('SMEMBERS', collection .. ':batch:' .. filter_value .. ':functions')
elseif filter_field == 'md5' then
    local p = 'idx:' .. collection .. ':file_funcs:' .. filter_value
    target_funcs = redis.call('SMEMBERS', p)
end

local algos = {"jaccard", "unweighted_cosine"}
for _, f_id in ipairs(target_funcs) do
    for _, algo in ipairs(algos) do
        if algo_filter == "" or algo == algo_filter then
            redis.call('SREM', collection .. ':built:functions:' .. algo, f_id)
        end
    end
end

return 1
