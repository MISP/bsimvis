local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local threshold = tonumber(ARGV[4])
local target_total = tonumber(ARGV[5])
local target_norm = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local min_features = tonumber(ARGV[8] or 0)

-- Features start from ARGV[9] as (hash, tf) pairs
local target_features = {}
for i = 9, #ARGV, 2 do
    target_features[ARGV[i]] = tonumber(ARGV[i+1])
end

local intersection_counts = {}
local shared_target_norm_sq = {}
local min_shared_norm_sq = 0
if algo == 'unweighted_cosine' then
    min_shared_norm_sq = (threshold * target_norm) * (threshold * target_norm)
end

-- 1. Sort features by bucket size (rarest first) to find candidates efficiently
local features_sorted = {}
for f_hash, target_tf in pairs(target_features) do
    local f_key = collection .. ':feature:' .. f_hash .. ':functions'
    local size = redis.call('ZCARD', f_key)
    table.insert(features_sorted, {hash = f_hash, tf = target_tf, key = f_key, size = size})
end
table.sort(features_sorted, function(a, b) return a.size < b.size end)

-- 2. Identify all candidates and calculate dot product / sum(min(tf))
for idx, feat in ipairs(features_sorted) do
    -- ponytail: Scan all buckets fully to ensure absolutely no matches are lost
    local scan_limit = feat.size
    
    local functions = redis.call('ZREVRANGE', feat.key, 0, scan_limit - 1, 'WITHSCORES')
    
    local target_tf_sq = 0
    if algo == 'unweighted_cosine' then
        target_tf_sq = feat.tf * feat.tf
    end
    
    for i = 1, #functions, 2 do
        local func_id = functions[i]
        local cand_tf = tonumber(functions[i+1])
        
        if func_id ~= target_id then
            if algo == 'jaccard' then
                intersection_counts[func_id] = (intersection_counts[func_id] or 0) + math.min(feat.tf, cand_tf)
            elseif algo == 'unweighted_cosine' then
                intersection_counts[func_id] = (intersection_counts[func_id] or 0) + (feat.tf * cand_tf)
                shared_target_norm_sq[func_id] = (shared_target_norm_sq[func_id] or 0) + target_tf_sq
            end
        end
    end
end

-- 2. Scored Candidates (Filtered by Threshold)
local candidate_list = {}
local count_idx = collection .. ':idx:func:bsim_features_count'

for id, intersect in pairs(intersection_counts) do
    local keep = true
    
    -- Phase 1 Bounds
    if algo == 'jaccard' then
        if intersect < threshold * target_total then
            keep = false
        end
    elseif algo == 'unweighted_cosine' then
        if (shared_target_norm_sq[id] or 0) < min_shared_norm_sq then
            keep = false
        end
    end
    
    if keep then
        -- Fetch candidate metrics
        local cand_total = tonumber(redis.call('ZSCORE', count_idx, id) or 0)
        
        if cand_total >= min_features and cand_total > 0 then
            local score = 0
            if algo == 'jaccard' then
                -- Generalized Jaccard: intersect / (target_total + cand_total - intersect)
                score = intersect / (target_total + cand_total - intersect)
            elseif algo == 'unweighted_cosine' then
                -- Cosine Pruning (Phase 2 Bound)
                local max_cand_total = (intersect / (threshold * target_norm)) ^ 2
                if cand_total <= max_cand_total then
                    -- TF-weighted Cosine: intersect / (target_norm * cand_norm)
                    local cand_norm = tonumber(redis.call('GET', id .. ':vec:norm') or 0)
                    score = intersect / (target_norm * cand_norm)
                end
            end
            
            if score >= threshold and score > 0 then
                table.insert(candidate_list, {id = id, score = score, c_total = cand_total})
            end
        end
    end
end

-- 3. Sort by score
table.sort(candidate_list, function(a, b) return a.score > b.score end)

-- 4. Limit and Format Return
local limit_val = math.min(limit, #candidate_list)
local result = {}

for i = 1, limit_val do
    local item = candidate_list[i]
    -- Pack as flat array: id1, score1, c_total1, id2, score2, c_total2 ...
    table.insert(result, item.id)
    table.insert(result, tostring(item.score))
    table.insert(result, tostring(item.c_total))
end

return result
