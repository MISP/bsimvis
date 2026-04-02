import redis

# Kvrocks is on 6666 for data
KV_CONFIG = {"host": "localhost", "port": 6666, "decode_responses": True}
# Standard Redis is on 6379 for jobs
REDIS_CONFIG = {"host": "localhost", "port": 6379, "decode_responses": True}


def init_redis(host=None, kv_port=None, redis_port=None):
    if host:
        KV_CONFIG["host"] = host
        REDIS_CONFIG["host"] = host
    if kv_port:
        KV_CONFIG["port"] = kv_port
    if redis_port:
        REDIS_CONFIG["port"] = redis_port


def get_redis():
    """Returns the Kvrocks connection for data."""
    return redis.Redis(**KV_CONFIG)


def get_queue_redis():
    """Returns the standard Redis connection for job queue."""
    return redis.Redis(**REDIS_CONFIG)


def get_batch_meta(collection, batch_uuid):
    r = get_redis()
    data = r.json().get(f"{collection}:batch:{batch_uuid}", "$")
    if isinstance(data, list) and data and len(data) == 1:
        data = data[0]
    return data
