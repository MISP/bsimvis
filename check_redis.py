import redis
r = redis.Redis(port=7220)
print(r.eval("""
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
""", 1, "00e0df533b35e04dc9de0f1bb38de9a891c96c1ef387ab9ce8c341954ead476b", "test_bench"))
