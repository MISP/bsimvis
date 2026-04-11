local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local timestamp = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local target_md5 = ARGV[6]
local target_addr = ARGV[7]
local threshold = tonumber(ARGV[8])
local built_set_key = ARGV[9]
local target_total = tonumber(ARGV[10])
local target_norm = tonumber(ARGV[11])

-- Features start from ARGV[12] as (hash, tf) pairs
local target_features = {}
for i = 12, #ARGV, 2 do
    target_features[ARGV[i]] = tonumber(ARGV[i+1])
end

local intersection_counts = {}
local candidates_seen = {}

-- 1. Identify all candidates and calculate dot product / sum(min(tf))
for f_hash, target_tf in pairs(target_features) do
    local f_key = 'idx:' .. collection .. ':feature:' .. f_hash .. ':functions'
    -- Fetch (func_id, tf) pairs from the inverted index
    local functions = redis.call('ZRANGE', f_key, 0, -1, 'WITHSCORES')
    for i = 1, #functions, 2 do
        local func_id = functions[i]
        local cand_tf = tonumber(functions[i+1])
        
        if func_id ~= target_id then
            if not candidates_seen[func_id] then
                candidates_seen[func_id] = true
                intersection_counts[func_id] = 0
            end
            
            if algo == 'jaccard' then
                -- sum(min(target_tf, cand_tf))
                intersection_counts[func_id] = intersection_counts[func_id] + math.min(target_tf, cand_tf)
            elseif algo == 'unweighted_cosine' then
                -- Dot Product: sum(target_tf * cand_tf)
                intersection_counts[func_id] = intersection_counts[func_id] + (target_tf * cand_tf)
            end
        end
    end
end

-- 2. Scored Candidates (Filtered by Threshold)
local candidate_list = {}
local count_idx = 'idx:' .. collection .. ':idx:func:bsim_features_count'

for id, intersect in pairs(intersection_counts) do
    -- Fetch candidate metrics
    local cand_total = tonumber(redis.call('ZSCORE', count_idx, id) or 0)
    
    if cand_total > 0 then
        local score = 0
        if algo == 'jaccard' then
            -- Generalized Jaccard: intersect / (target_total + cand_total - intersect)
            score = intersect / (target_total + cand_total - intersect)
        elseif algo == 'unweighted_cosine' then
            -- TF-weighted Cosine: intersect / (target_norm * cand_norm)
            local cand_norm = tonumber(redis.call('GET', id .. ':vec:norm') or 0)
            score = intersect / (target_norm * cand_norm)
        end
        
        if score >= threshold and score > 0 then
            table.insert(candidate_list, {id = id, score = score, c_total = cand_total})
        end
    end
end

-- 3. Sort by score
table.sort(candidate_list, function(a, b) return a.score > b.score end)

-- 4. Deep Selection Architecture: Minimal Storage
local limit_val = math.min(limit, #candidate_list)
local global_zset_key = 'idx:' .. collection .. ':sim:score:' .. algo
local all_key = 'idx:' .. collection .. ':sim:all'

for i = 1, limit_val do
    local item = candidate_list[i]
    
    local id_a, id_b
    local feat_count_a, feat_count_b
    local md5_a, md5_b
    
    local is_target_greater = (target_id > item.id)
    local is_item_built = (redis.call('SISMEMBER', built_set_key, item.id) == 1)
    
    -- Robust Canonical Order
    if is_target_greater or is_item_built then
        -- Parse item MD5 from ID (id is idx:collection:func:md5:addr)
        local func_pref = "idx:" .. collection .. ":func:"
        local rest = item.id:sub(#func_pref + 1)
        local next_sep = rest:find(":")
        local item_md5 = rest:sub(1, next_sep and (next_sep - 1) or #rest)

        if is_target_greater then
            id_a, id_b = target_id, item.id
            feat_count_a, feat_count_b = target_total, item.c_total
            md5_a, md5_b = target_md5, item_md5
        else
            id_a, id_b = item.id, target_id
            feat_count_a, feat_count_b = item.c_total, target_total
            md5_a, md5_b = item_md5, target_md5
        end

        local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
        local sim_meta_key = 'idx:' .. collection .. ':sim:' .. algo .. ':' .. id_a .. ':' .. id_b
        
        -- Deep Selection: Store identity and TF-weighted total count
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
        
        -- Unified Involves Indexing
        redis.call('SADD', 'idx:' .. collection .. ':sim:involves:func:' .. id_a, sim_meta_key)
        redis.call('SADD', 'idx:' .. collection .. ':sim:involves:func:' .. id_b, sim_meta_key)
        
        local file_id_a = 'idx:' .. collection .. ':file:' .. md5_a
        local file_id_b = 'idx:' .. collection .. ':file:' .. md5_b
        redis.call('SADD', 'idx:' .. collection .. ':sim:involves:file:' .. file_id_a, sim_meta_key)
        redis.call('SADD', 'idx:' .. collection .. ':sim:involves:file:' .. file_id_b, sim_meta_key)
        
        -- Similarity-Specific Range Filters (Using Total Feature Counts)
        redis.call('ZADD', 'idx:' .. collection .. ':sim:min_features', sim_doc.min_features, sim_meta_key)
        redis.call('ZADD', 'idx:' .. collection .. ':sim:is_cross_binary:' .. sim_doc.is_cross_binary, 0, sim_meta_key)
    end
end

return 1
