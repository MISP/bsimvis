local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local timestamp = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local target_md5 = ARGV[6]
local target_addr = ARGV[7]
local threshold = tonumber(ARGV[8])
local built_set_key = ARGV[9]
local target_count = tonumber(ARGV[10])

-- Features start from ARGV[11]
local target_features = {}
for i = 11, #ARGV do
    target_features[#target_features + 1] = ARGV[i]
end

local intersection_counts = {}
local candidates_seen = {}

-- 1. Identify all candidates and count intersections (Manual increment for 0-1 scores)
for _, f_hash in ipairs(target_features) do
    local f_key = 'idx:' .. collection .. ':feature:' .. f_hash .. ':functions'
    local functions = redis.call('ZRANGE', f_key, 0, -1)
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
        
        if score >= threshold and score > 0 then
            table.insert(candidate_list, {id = id, score = score, c_count = cand_count})
        end
    end
end

-- 3. Sort by score
table.sort(candidate_list, function(a, b) return a.score > b.score end)

-- 4. Deep Selection Architecture: Minimal Storage
local limit_val = math.min(limit, #candidate_list)
local global_zset_key = collection .. ':all_sim:' .. algo
local all_key = 'idx:' .. collection .. ':all_similarities'

for i = 1, limit_val do
    local item = candidate_list[i]
    
    local id_a, id_b
    local feat_count_a, feat_count_b
    local md5_a, md5_b
    
    local is_target_greater = (target_id > item.id)
    local is_item_built = (redis.call('SISMEMBER', built_set_key, item.id) == 1)
    
    -- Robust Canonical Order
    if is_target_greater or is_item_built then
        -- Parse item MD5 from ID (id is collection:function:md5:addr)
        local func_tag = collection .. ":function:"
        local rest = item.id:sub(#func_tag + 1)
        local next_sep = rest:find(":")
        local item_md5 = rest:sub(1, next_sep and (next_sep - 1) or #rest)

        if is_target_greater then
            id_a, id_b = target_id, item.id
            feat_count_a, feat_count_b = target_count, item.c_count
            md5_a, md5_b = target_md5, item_md5
        else
            id_a, id_b = item.id, target_id
            feat_count_a, feat_count_b = item.c_count, target_count
            md5_a, md5_b = item_md5, target_md5
        end

        local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
        local sim_meta_key = collection .. ':sim_meta:' .. algo .. ':' .. id_a .. ':' .. id_b
        
        -- Deep Selection: Store ONLY identity and metrics
        local sim_doc = {
            type = "sim",
            collection = collection,
            algo = algo,
            score = score_rounded,
            id1 = id_a,
            id2 = id_b,
            md5_1 = md5_a,
            md5_2 = md5_b,
            feat_count1 = feat_count_a,
            feat_count2 = feat_count_b,
            min_features = math.min(feat_count_a, feat_count_b),
            entry_date = timestamp,
            is_cross_binary = (md5_a ~= md5_b) and "true" or "false"
        }
        redis.call('JSON.SET', sim_meta_key, '$', cjson.encode(sim_doc))

        -- Essential Metrics Indexing
        redis.call('ZADD', global_zset_key, score_rounded, sim_meta_key)
        redis.call('ZADD', all_key, 0, sim_meta_key)
        
        -- Similarity-Specific Range Filters
        redis.call('ZADD', 'idx:' .. collection .. ':sim:min_features', sim_doc.min_features, sim_meta_key)
        redis.call('ZADD', 'idx:' .. collection .. ':sim:is_cross_binary:' .. sim_doc.is_cross_binary, 0, sim_meta_key)
        
        -- MD5 indexing is kept as it's a primary similarity filter
        redis.call('ZADD', 'idx:' .. collection .. ':sim:md5_1:' .. md5_a, 0, sim_meta_key)
        redis.call('ZADD', 'idx:' .. collection .. ':sim:md5_2:' .. md5_b, 0, sim_meta_key)
        
        -- NOTE: name1, name2, tags1, tags2, etc. are NOT indexed here anymore.
        -- They are resolved via Function Registries during search.
    end
end

return 1
