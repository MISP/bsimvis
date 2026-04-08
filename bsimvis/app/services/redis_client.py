import redis
import time
from .timer_service import get_active_timer

class TimedRedis(redis.Redis):
    """A wrapper for Redis client that records performance timings."""
    def execute_command(self, *args, **options):
        timer = get_active_timer()
        if not timer:
            return super().execute_command(*args, **options)
        
        cmd = args[0] if args else "UNKNOWN"
        # Categorize: EVAL and EVALSHA are Lua scripts
        category = "lua_scripts" if cmd in ("EVAL", "EVALSHA") else "db_queries"
        
        start = time.time()
        try:
            return super().execute_command(*args, **options)
        finally:
            duration = time.time() - start
            timer.record(cmd, duration, category)

    def pipeline(self, transaction=True, shard_hint=None):
        return TimedPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )

class TimedPipeline(redis.client.Pipeline):
    """A wrapper for Redis Pipeline that records performance timings."""
    def execute(self, raise_on_error=True):
        timer = get_active_timer()
        if not timer:
            return super().execute(raise_on_error)
        
        # Pipelines can be complex; we categorize as lua if any lua inside
        has_lua = any(cmd[0] in ("EVAL", "EVALSHA") for cmd in self.command_stack)
        category = "lua_scripts" if has_lua else "db_queries"
        desc = f"PIPELINE({len(self.command_stack)} cmds)"
        
        start = time.time()
        try:
            return super().execute(raise_on_error)
        finally:
            duration = time.time() - start
            timer.record(desc, duration, category)

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
    return TimedRedis(**KV_CONFIG)


def get_queue_redis():
    """Returns the standard Redis connection for job queue."""
    return TimedRedis(**REDIS_CONFIG)


def get_batch_meta(collection, batch_uuid):
    r = get_redis()
    data = r.json().get(f"{collection}:batch:{batch_uuid}", "$")
    if isinstance(data, list) and data and len(data) == 1:
        data = data[0]
    return data
