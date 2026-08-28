local v_hash = KEYS[1]
local coll = ARGV[1]
local v_id_key = coll .. ":vclass_hash_to_id"
local v_id = redis.call("HGET", v_id_key, v_hash)
if not v_id then
    v_id = redis.call("INCR", coll .. ":vclass_counter")
    redis.call("HSET", v_id_key, v_hash, v_id)
    return {tostring(v_id), 1}
end
return {tostring(v_id), 0}
