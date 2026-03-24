import redis

# Hardcoded for your POC, but easy to move to env vars later
REDIS_CONFIG = {"host": "localhost", "port": 6666, "decode_responses": True}

def get_redis():
    return redis.Redis(**REDIS_CONFIG)

def get_batch_meta(collection, batch_uuid):
    r = get_redis()
    data = r.json().get(f"{collection}:batch:{batch_uuid}", "$")
    if isinstance(data, list) and data and len(data) == 1: data = data[0]
    return data