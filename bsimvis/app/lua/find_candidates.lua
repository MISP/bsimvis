local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local threshold = tonumber(ARGV[4])
local target_total = tonumber(ARGV[5])
local target_norm = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])

-- Features start from ARGV[8] as (hash, tf) pairs
local target_features = {}
for i = 8, #ARGV, 2 do
    target_features[ARGV[i]] = tonumber(ARGV[i+1])
end

local intersection_counts = {}
local candidates_seen = {}

-- 1. Identify all candidates and calculate dot product / sum(min(tf))
for f_hash, target_tf in pairs(target_features) do
    local f_key = collection .. ':feature:' .. f_hash .. ':functions'
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
local count_idx = collection .. ':idx:func:bsim_features_count'

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
