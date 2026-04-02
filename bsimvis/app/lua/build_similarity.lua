local target_id = ARGV[1]
local collection = ARGV[2]
local algo = ARGV[3]
local timestamp = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local target_md5 = ARGV[6]
local target_addr = ARGV[7]
local threshold = tonumber(ARGV[8])
local built_set_key = ARGV[9]

local target_meta_raw = redis.call('JSON.GET', target_id .. ':meta')
local target_meta = {}
if target_meta_raw then target_meta = cjson.decode(target_meta_raw) end

-- ARGV[10...] are feature hashes
local target_features = {}
for i = 10, #ARGV do
    target_features[#target_features + 1] = ARGV[i]
end

local target_count = #target_features
local intersection_counts = {}
local candidates_seen = {}

-- 1. Identify all candidates and count intersections
for _, f_hash in ipairs(target_features) do
    local functions = redis.call('ZRANGE', 'idx:' .. collection .. ':feature:' .. f_hash .. ':functions', 0, -1)
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
        
        -- ONLY keep if above threshold
        if score >= threshold and score > 0 then
            table.insert(candidate_list, {id = id, score = score})
        end
    end
end

-- 3. Sort by score
table.sort(candidate_list, function(a, b) return a.score > b.score end)

-- 4. Enrich Top K and Prepare for Storage
local matches = {}
local limit_val = math.min(limit, #candidate_list)
local global_zset_key = collection .. ':all_sim:' .. algo

for i = 1, limit_val do
    local item = candidate_list[i]
    
    -- --- Robust Canonical Order (Incremental-Safe) ---
    local id_a, id_b
    local meta_a, meta_b
    local md5_a, md5_b
    local addr_a, addr_b
    
    local is_target_greater = (target_id > item.id)
    local is_item_built = (redis.call('SISMEMBER', built_set_key, item.id) == 1)
    
    local should_store = false
    if is_target_greater then
        should_store = true
    elseif is_item_built then
        should_store = true
    end
    
    if should_store then
        local item_meta_raw = redis.call('JSON.GET', item.id .. ':meta')
        local item_meta = {}
        if item_meta_raw then item_meta = cjson.decode(item_meta_raw) end
        
        -- Parse item parts for indexing
        local parts2 = {}
        for p in string.gmatch(item.id, "([^:]+)") do table.insert(parts2, p) end
        local item_md5 = parts2[3] or ""
        local item_addr = parts2[4] or "0"

        if is_target_greater then
            id_a, id_b = target_id, item.id
            meta_a, meta_b = target_meta, item_meta
            md5_a, md5_b = target_md5, item_md5
            addr_a, addr_b = target_addr, item_addr
        else
            id_a, id_b = item.id, target_id
            meta_a, meta_b = item_meta, target_meta
            md5_a, md5_b = item_md5, target_md5
            addr_a, addr_b = item_addr, target_addr
        end

        local score_rounded = math.floor(item.score * 10000 + 0.5) / 10000
        
        -- 5. Store similarity metadata for RediSearch
        local sim_meta_key = collection .. ':sim_meta:' .. algo .. ':' .. id_a .. ':' .. id_b
        
        -- 6. Add to collection-wide similarity scoreboard (ZSET)
        redis.call('ZADD', global_zset_key, score_rounded, sim_meta_key)
        local sim_doc = {
            type = "sim",
            collection = collection,
            algo = algo,
            score = score_rounded,
            id1 = id_a,
            id2 = id_b,
            name1 = meta_a["function_name"] or id_a,
            name2 = meta_b["function_name"] or id_b,
            file_name1 = meta_a["file_name"] or "",
            file_name2 = meta_b["file_name"] or "",
            md5_1 = md5_a,
            md5_2 = md5_b,
            tags1 = table.concat(meta_a["tags"] or {}, ","),
            tags2 = table.concat(meta_b["tags"] or {}, ","),
            batch_uuid1 = meta_a["batch_uuid"] or "",
            batch_uuid2 = meta_b["batch_uuid"] or "",
            language_id1 = meta_a["language_id"] or "",
            language_id2 = meta_b["language_id"] or "",
            return_type1 = meta_a["return_type"] or "",
            return_type2 = meta_b["return_type"] or "",
            decompiler1 = meta_a["decompiler_id"] or "",
            decompiler2 = meta_b["decompiler_id"] or "",
            feat_count1 = meta_a["bsim_features_count"] or 0,
            feat_count2 = meta_b["bsim_features_count"] or 0,
            min_features = math.min(meta_a["bsim_features_count"] or 0, meta_b["bsim_features_count"] or 0),
            entry_date = timestamp,
            is_cross_binary = (md5_a ~= md5_b) and "true" or "false"
        }
        redis.call('JSON.SET', sim_meta_key, '$', cjson.encode(sim_doc))

        -- 6.1 Secondary Indexing (Kvrocks-optimized ZSETs)
        local all_key = 'idx:' .. collection .. ':all_similarities'
        local all_type = redis.call('TYPE', all_key)
        if type(all_type) == 'table' then all_type = all_type['ok'] or all_type[1] end
        if all_type == 'set' then redis.call('DEL', all_key) end
        redis.call('ZADD', all_key, 0, sim_meta_key)
        
        -- Unified indexing helper
        local function index_tag(field, value)
            if value and value ~= "" then
                local k = 'idx:' .. collection .. ':sim:' .. field .. ':' .. string.lower(tostring(value))
                local kt = redis.call('TYPE', k)
                if type(kt) == 'table' then kt = kt['ok'] or kt[1] end
                if kt == 'set' then redis.call('DEL', k) end
                redis.call('ZADD', k, 0, sim_meta_key)
                redis.call('SADD', 'idx:' .. collection .. ':reg:' .. field, k)
            end
        end

        index_tag('name1', sim_doc.name1)
        index_tag('name2', sim_doc.name2)
        index_tag('md5_1', sim_doc.md5_1)
        index_tag('md5_2', sim_doc.md5_2)
        index_tag('batch_uuid1', sim_doc.batch_uuid1)
        index_tag('batch_uuid2', sim_doc.batch_uuid2)
        index_tag('language_id1', sim_doc.language_id1)
        index_tag('language_id2', sim_doc.language_id2)
        index_tag('is_cross_binary', sim_doc.is_cross_binary)
        index_tag('id1', md5_a .. ':' .. addr_a)
        index_tag('id2', md5_b .. ':' .. addr_b)

        if sim_doc.feat_count1 then redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count1', sim_doc.feat_count1, sim_meta_key) end
        if sim_doc.feat_count2 then redis.call('ZADD', 'idx:' .. collection .. ':sim:feat_count2', sim_doc.feat_count2, sim_meta_key) end
        if sim_doc.min_features then redis.call('ZADD', 'idx:' .. collection .. ':sim:min_features', sim_doc.min_features, sim_meta_key) end
        if sim_doc.entry_date then redis.call('ZADD', 'idx:' .. collection .. ':sim:entry_date', sim_doc.entry_date, sim_meta_key) end

        if meta_a["tags"] then for _, t in ipairs(meta_a["tags"]) do index_tag('tags1', t) end end
        if meta_b["tags"] then for _, t in ipairs(meta_b["tags"]) do index_tag('tags2', t) end end
    end
end

return 1
