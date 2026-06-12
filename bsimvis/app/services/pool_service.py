import json
import logging
import time
from bsimvis.app.services.redis_client import get_redis

class PoolService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def create_pool(self, pool_id, name, collections, config):
        """
        Creates a new pool definition.
        config: {algo, top_k, min_score, cluster_algo, cluster_params}
        """
        r = self.r
        pool_meta_key = f"global:pool:{pool_id}:meta"
        
        if r.exists(pool_meta_key):
            return False, "Pool ID already exists"

        now = int(time.time() * 1000)
        meta = {
            "name": name,
            "created_at": now,
            "status": "created",
            "last_built_at": 0,
            "sync_status": "outdated",
            "algo": config.get("algo", "unweighted_cosine"),
            "top_k": config.get("top_k", 1000),
            "min_score": config.get("min_score", 0.3),
            "cluster_algo": config.get("cluster_algo", "hdbscan"),
            "cluster_params": json.dumps(config.get("cluster_params", {}))
        }

        pipe = r.pipeline()
        pipe.hset(pool_meta_key, mapping=meta)
        pipe.sadd("global:pools", pool_id)
        
        for coll in collections:
            pipe.sadd(f"global:pool:{pool_id}:collections_list", coll)
            pipe.sadd(f"{coll}:pools", pool_id)
            
        pipe.execute()
        return True, "Pool created successfully"

    def get_pool(self, pool_id):
        r = self.r
        meta = r.hgetall(f"global:pool:{pool_id}:meta")
        if not meta:
            return None
            
        # Convert bytes to string if necessary
        meta = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in meta.items()}
        meta["collections"] = [c.decode() if isinstance(c, bytes) else c for c in r.smembers(f"global:pool:{pool_id}:collections_list")]
        
        # Get sync state snapshots
        sync_snapshots = r.hgetall(f"global:pool:{pool_id}:collections")
        meta["sync_snapshots"] = {k.decode() if isinstance(k, bytes) else k: json.loads(v) for k, v in sync_snapshots.items()}
        
        return meta

    def list_pools(self, collection=None):
        r = self.r
        if collection:
            pool_ids = [p.decode() if isinstance(p, bytes) else p for p in r.smembers(f"{collection}:pools")]
        else:
            pool_ids = [p.decode() if isinstance(p, bytes) else p for p in r.smembers("global:pools")]
            
        pools = []
        for pid in pool_ids:
            meta = self.get_pool(pid)
            if meta:
                meta["id"] = pid
                pools.append(meta)
        return pools

    def delete_pool(self, pool_id):
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False, "Pool not found"

        pipe = r.pipeline()
        # Remove from collections reverse index
        for coll in meta.get("collections", []):
            pipe.srem(f"{coll}:pools", pool_id)
            
        pipe.srem("global:pools", pool_id)
        
        # Cleanup all keys in pool namespace
        cursor = 0
        pattern = f"global:pool:{pool_id}:*"
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                pipe.delete(*keys)
            if cursor == 0:
                break
                
        pipe.execute()
        return True, "Pool deleted successfully"

    def check_sync_status(self, pool_id):
        """
        Compares current collection state with stored snapshots.
        """
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return None

        collections = meta.get("collections", [])
        snapshots = meta.get("sync_snapshots", {})
        
        is_outdated = False
        details = {}

        for coll in collections:
            # Get current state
            current_count = r.scard(f"{coll}:all_files")
            zrange_res = r.zrange(f"{coll}:idx:file:entry_date", -1, -1, withscores=True)
            current_last_entry = int(zrange_res[0][1]) if zrange_res else 0
            
            snap = snapshots.get(coll, {})
            snap_count = snap.get("file_count", -1)
            snap_last_entry = snap.get("last_entry_date", -1)
            
            coll_outdated = (current_count != snap_count) or (current_last_entry > snap_last_entry)
            if coll_outdated:
                is_outdated = True
                
            details[coll] = {
                "outdated": coll_outdated,
                "current": {"file_count": current_count, "last_entry_date": current_last_entry},
                "snapshot": snap
            }

        new_status = "outdated" if is_outdated else "current"
        r.hset(f"global:pool:{pool_id}:meta", "sync_status", new_status)
        
        return {
            "sync_status": new_status,
            "details": details
        }

    def update_sync_snapshots(self, pool_id):
        """
        Called after build to snapshot current collection states.
        """
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False

        collections = meta.get("collections", [])
        pipe = r.pipeline()
        
        for coll in collections:
            current_count = r.scard(f"{coll}:all_files")
            zrange_res = r.zrange(f"{coll}:idx:file:entry_date", -1, -1, withscores=True)
            current_last_entry = int(zrange_res[0][1]) if zrange_res else 0
            
            snapshot = {
                "file_count": current_count,
                "last_entry_date": current_last_entry
            }
            pipe.hset(f"global:pool:{pool_id}:collections", coll, json.dumps(snapshot))
            
        pipe.hset(f"global:pool:{pool_id}:meta", "sync_status", "current")
        pipe.hset(f"global:pool:{pool_id}:meta", "last_built_at", int(time.time() * 1000))
        pipe.execute()
        return True

pool_service = PoolService()
