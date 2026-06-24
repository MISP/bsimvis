local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3] -- minhash_lsh
local threshold = tonumber(ARGV[4])
local target_total = tonumber(ARGV[5])
local target_norm = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local min_features = tonumber(ARGV[8] or 0)
local num_bands = tonumber(ARGV[9] or 10)

-- Features start from ARGV[10] as (hash, tf) pairs
local target_features = {}
for i = 10, #ARGV, 2 do
    target_features[ARGV[i]] = tonumber(ARGV[i+1])
end

-- Helper to extract collection prefix from a string (swaps coll1:lsh:bucket... -> coll2:lsh:bucket...)
local function get_search_bucket_key(stored_key, target_coll)
    if not stored_key then return nil end
    -- stored_key format: "source_collection:lsh:bucket:band:hash"
    -- Find the index of ':lsh:bucket:'
    local pivot = string.find(stored_key, ":lsh:bucket:")
    if not pivot then return nil end
    local suffix = string.sub(stored_key, pivot)
    return target_coll .. suffix
end

-- 1. Get query function's own LSH buckets
-- Find candidate function IDs by union of members in matching LSH buckets
local candidate_set = {}
for band = 0, num_bands - 1 do
    local stored_bucket_key = redis.call('GET', target_id .. ':lsh:bucket_key:' .. band)
    local bucket_key = get_search_bucket_key(stored_bucket_key, collection)
    if bucket_key then
        local cands = redis.call('SMEMBERS', bucket_key)
        for _, cand_id in ipairs(cands) do
            if cand_id ~= target_id then
                candidate_set[cand_id] = true
            end
        end
    end
end

-- 2. Compute similarity for the candidate set
local intersection_counts = {}
for f_hash, target_tf in pairs(target_features) do
    local f_key = collection .. ':feature:' .. f_hash .. ':functions'
    local functions = redis.call('ZRANGE', f_key, 0, -1, 'WITHSCORES')
    for i = 1, #functions, 2 do
        local func_id = functions[i]
        local cand_tf = tonumber(functions[i+1])
        if candidate_set[func_id] then
            intersection_counts[func_id] = (intersection_counts[func_id] or 0) + (target_tf * cand_tf)
        end
    end
end

local candidate_list = {}
local count_idx = collection .. ':idx:func:bsim_features_count'

for id, intersect in pairs(intersection_counts) do
    local cand_total = tonumber(redis.call('ZSCORE', count_idx, id) or 0)
    if cand_total >= min_features and cand_total > 0 then
        local cand_norm = tonumber(redis.call('GET', id .. ':vec:norm') or 0)
        local score = 0
        if target_norm > 0 and cand_norm > 0 then
            score = intersect / (target_norm * cand_norm)
        end

        if score >= threshold then
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
    table.insert(result, item.id)
    table.insert(result, tostring(item.score))
    table.insert(result, tostring(item.c_total))
end

return result
