import redis

# Hardcoded for your POC, but easy to move to env vars later
REDIS_CONFIG = {"host": "localhost", "port": 6667, "decode_responses": True}

def get_redis():
    return redis.Redis(**REDIS_CONFIG)

def get_batch_meta(collection, batch_uuid):
    r = get_redis()
    return r.json().get(f"{collection}:batch:{batch_uuid}")