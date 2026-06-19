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
        config: {only_cross_collection, func_sim_params, func_cluster_params, file_sim_params, file_cluster_params, ...}
        """
        r = self.r
        pool_meta_key = f"global:pool:{pool_id}:meta"

        if r.exists(pool_meta_key):
            return False, "Pool ID already exists"

        now = int(time.time() * 1000)

        # New structured config storage
        only_cross_collection = (
            "1" if config.get("only_cross_collection", False) else "0"
        )
        func_sim_params = config.get("func_sim_params", {})
        func_cluster_params = config.get("func_cluster_params", {})
        file_sim_params = config.get("file_sim_params", {})
        file_cluster_params = config.get("file_cluster_params", {})

        meta = {
            "name": name,
            "created_at": now,
            "status": "created",
            "last_built_at": 0,
            "sync_status": "outdated",
            "only_cross_collection": only_cross_collection,
            "func_sim_params": json.dumps(func_sim_params),
            "func_cluster_params": json.dumps(func_cluster_params),
            "file_sim_params": json.dumps(file_sim_params),
            "file_cluster_params": json.dumps(file_cluster_params),
            # Keep old fields as fallback for backward compatibility
            "algo": config.get("algo", "unweighted_cosine"),
            "top_k": config.get("top_k", 1000),
            "min_score": config.get("min_score", 0.3),
            "cluster_algo": config.get("cluster_algo", "hdbscan"),
            "cluster_params": json.dumps(config.get("cluster_params", {})),
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
        meta = {
            k.decode() if isinstance(k, bytes) else k: (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in meta.items()
        }
        meta["collections"] = [
            c.decode() if isinstance(c, bytes) else c
            for c in r.smembers(f"global:pool:{pool_id}:collections_list")
        ]

        if "only_cross_collection" in meta:
            meta["only_cross_collection"] = meta["only_cross_collection"] == "1"

        # Parse nested JSON fields
        json_fields = [
            "cluster_params",
            "func_sim_params",
            "func_cluster_params",
            "file_sim_params",
            "file_cluster_params",
        ]
        for field in json_fields:
            if field not in meta:
                meta[field] = {}
            elif isinstance(meta[field], str):
                try:
                    meta[field] = json.loads(meta[field])
                except Exception:
                    meta[field] = {}

        # Get sync state snapshots
        sync_snapshots = r.hgetall(f"global:pool:{pool_id}:collections")
        meta["sync_snapshots"] = {
            k.decode() if isinstance(k, bytes) else k: json.loads(v)
            for k, v in sync_snapshots.items()
        }

        # Retrieve pool-wide similarities & clusters counts
        # Function similarities
        meta["total_func_similarities"] = r.zcard(f"global:pool:{pool_id}:sim:score")

        # Function clusters
        meta["total_func_clusters"] = r.scard(f"global:pool:{pool_id}:cluster:list")

        # File similarities
        file_algo = meta.get("file_sim_params", {}).get("algo", "unweighted_cosine")
        meta["total_file_similarities"] = r.zcard(
            f"global:pool:{pool_id}:bin_sim:score:{file_algo}"
        )

        # File clusters (if any list exists, otherwise default 0 or check algorithm clusters)
        meta["total_file_clusters"] = r.scard(f"global:pool:{pool_id}:bin_cluster:list")

        return meta

    def list_pools(self, collection=None):
        r = self.r
        if collection:
            pool_ids = [
                p.decode() if isinstance(p, bytes) else p
                for p in r.smembers(f"{collection}:pools")
            ]
        else:
            pool_ids = [
                p.decode() if isinstance(p, bytes) else p
                for p in r.smembers("global:pools")
            ]

        pools = []
        for pid in pool_ids:
            # Dynamically verify sync status on list to show accurate state
            self.check_sync_status(pid)
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

    def wipe_pool_data(self, pool_id):
        """Wipes all generated similarity & clustering data but preserves pool configuration."""
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False, "Pool not found"

        pipe = r.pipeline()

        # Cleanup keys in pool namespace except meta and collections_list keys
        cursor = 0
        pattern = f"global:pool:{pool_id}:*"
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                keys_to_delete = []
                for k in keys:
                    k_str = k.decode() if isinstance(k, bytes) else k
                    if k_str not in [
                        f"global:pool:{pool_id}:meta",
                        f"global:pool:{pool_id}:collections_list",
                    ]:
                        keys_to_delete.append(k)
                if keys_to_delete:
                    pipe.delete(*keys_to_delete)
            if cursor == 0:
                break

        # Reset sync status and last built timestamps in metadata
        pipe.hset(
            f"global:pool:{pool_id}:meta",
            mapping={"sync_status": "outdated", "last_built_at": 0},
        )
        pipe.execute()
        return True, "Pool data wiped successfully"

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
            zrange_res = r.zrange(
                f"{coll}:idx:file:entry_date", -1, -1, withscores=True
            )
            current_last_entry = int(zrange_res[0][1]) if zrange_res else 0

            snap = snapshots.get(coll, {})
            snap_count = snap.get("file_count", -1)
            snap_last_entry = snap.get("last_entry_date", -1)

            coll_outdated = (current_count != snap_count) or (
                current_last_entry > snap_last_entry
            )
            if coll_outdated:
                is_outdated = True

            details[coll] = {
                "outdated": coll_outdated,
                "current": {
                    "file_count": current_count,
                    "last_entry_date": current_last_entry,
                },
                "snapshot": snap,
            }

        new_status = "outdated" if is_outdated else "current"
        r.hset(f"global:pool:{pool_id}:meta", "sync_status", new_status)

        return {"sync_status": new_status, "details": details}

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
            zrange_res = r.zrange(
                f"{coll}:idx:file:entry_date", -1, -1, withscores=True
            )
            current_last_entry = int(zrange_res[0][1]) if zrange_res else 0

            snapshot = {
                "file_count": current_count,
                "last_entry_date": current_last_entry,
            }
            pipe.hset(f"global:pool:{pool_id}:collections", coll, json.dumps(snapshot))

        pipe.hset(f"global:pool:{pool_id}:meta", "sync_status", "current")
        pipe.hset(
            f"global:pool:{pool_id}:meta", "last_built_at", int(time.time() * 1000)
        )
        pipe.execute()
        return True

    def build_pool_indexes(self, pool_id):
        """
        Merges global registries and indexes from all member collections
        into the global:pool:{pool_id} namespace so that search and filtering work correctly.
        """
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False

        collections = meta.get("collections", [])
        if not collections:
            return True

        from bsimvis.app.services.index_service import (
            FILE_TAG_FIELDS,
            FUNC_TAG_FIELDS,
            FILE_NUM_FIELDS,
            FUNC_NUM_FIELDS,
        )
        from bsimvis.app.services.index_config import get_fields_targeting_level

        SIM_TAG_FIELDS = get_fields_targeting_level("sim", is_num=False)
        SIM_NUM_FIELDS = get_fields_targeting_level("sim", is_num=True)

        pool_coll = f"global:pool:{pool_id}"

        # 1. Merge TAG registries and buckets
        for level, fields in [
            ("file", FILE_TAG_FIELDS),
            ("func", FUNC_TAG_FIELDS),
            ("sim", SIM_TAG_FIELDS),
        ]:
            for field in fields:
                # Skip pool-specific user annotations and their propagations
                if field in [
                    "user_tags",
                    "file_user_tags",
                    "func_user_tags",
                    "note_owners",
                ]:
                    continue

                # Get all buckets from member collections
                bucket_values = set()
                for coll in collections:
                    reg_key = f"{coll}:reg:{level}:{field}"
                    buckets = r.smembers(reg_key)
                    for b in buckets:
                        b_str = b.decode() if isinstance(b, bytes) else str(b)
                        # Extract the value part: {coll}:idx:{level}:{field}:{value}
                        prefix = f"{coll}:idx:{level}:{field}:"
                        if b_str.startswith(prefix):
                            bucket_values.add(b_str[len(prefix) :])

                # Now merge buckets and populate pool registry
                if bucket_values:
                    pool_reg_key = f"{pool_coll}:reg:{level}:{field}"
                    pipe = r.pipeline()
                    # Clear old pool registry
                    pipe.delete(pool_reg_key)
                    for val in bucket_values:
                        pool_bucket_key = f"{pool_coll}:idx:{level}:{field}:{val}"
                        if level == "sim":
                            # Fetch and translate SIDs
                            all_sids = set()
                            for coll in collections:
                                sb = f"{coll}:idx:{level}:{field}:{val}"
                                for sid in r.smembers(sb):
                                    sid_str = (
                                        sid.decode()
                                        if isinstance(sid, bytes)
                                        else str(sid)
                                    )
                                    # Translate to pool SID format
                                    parts = sid_str.split(":")
                                    if len(parts) >= 4:
                                        coll_name = parts[0]
                                        rest = ":".join(parts[3:])
                                        pivot = rest.find("::")
                                        if pivot != -1:
                                            clean_id1 = rest[:pivot]
                                            clean_id2 = rest[pivot + 2 :]
                                            pool_sid = f"global:pool:{pool_id}:sim:{coll_name}:func:{clean_id1}::{coll_name}:func:{clean_id2}"
                                            all_sids.add(pool_sid)
                            if all_sids:
                                pipe.delete(pool_bucket_key)
                                pipe.sadd(pool_bucket_key, *all_sids)
                                pipe.sadd(pool_reg_key, pool_bucket_key)
                        else:
                            source_buckets = [
                                f"{coll}:idx:{level}:{field}:{val}"
                                for coll in collections
                            ]
                            # Verify sources exist before sunionstore to avoid errors
                            existing_sources = [
                                sb for sb in source_buckets if r.exists(sb)
                            ]
                            if existing_sources:
                                pipe.sunionstore(pool_bucket_key, *existing_sources)
                                pipe.sadd(pool_reg_key, pool_bucket_key)
                    pipe.execute()

        # 2. Merge NUM ZSets
        for level, fields in [
            ("file", FILE_NUM_FIELDS),
            ("func", FUNC_NUM_FIELDS),
            ("sim", SIM_NUM_FIELDS),
        ]:
            for field in fields:
                source_zsets = [f"{coll}:idx:{level}:{field}" for coll in collections]
                existing_zsets = [sz for sz in source_zsets if r.exists(sz)]
                if existing_zsets:
                    pool_zset_key = f"{pool_coll}:idx:{level}:{field}"
                    r.zunionstore(pool_zset_key, existing_zsets)

        # 3. Merge all_files and all_functions
        pipe = r.pipeline()
        pipe.delete(f"{pool_coll}:all_files")
        pipe.delete(f"{pool_coll}:all_functions")
        all_files_sources = [
            f"{coll}:all_files" for coll in collections if r.exists(f"{coll}:all_files")
        ]
        if all_files_sources:
            pipe.sunionstore(f"{pool_coll}:all_files", *all_files_sources)

        all_funcs_sources = [
            f"{coll}:all_functions"
            for coll in collections
            if r.exists(f"{coll}:all_functions")
        ]
        if all_funcs_sources:
            pipe.sunionstore(f"{pool_coll}:all_functions", *all_funcs_sources)

        # 4. Merge idx:file:functions:*
        md5_set = set()
        for coll in collections:
            for key in r.scan_iter(match=f"{coll}:idx:file:functions:*", count=1000):
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                md5 = key_str.split(":")[-1]
                md5_set.add(md5)

        for md5 in md5_set:
            sources = [
                f"{coll}:idx:file:functions:{md5}"
                for coll in collections
                if r.exists(f"{coll}:idx:file:functions:{md5}")
            ]
            if sources:
                pipe.sunionstore(f"{pool_coll}:idx:file:functions:{md5}", *sources)

        pipe.execute()

        # 5. Merge tags_metadata
        pool_tags_meta_key = f"{pool_coll}:tags_metadata"
        r.delete(pool_tags_meta_key)
        for coll in collections:
            coll_tags_meta = r.hgetall(f"{coll}:tags_metadata")
            if coll_tags_meta:
                r.hset(
                    pool_tags_meta_key,
                    mapping={
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in coll_tags_meta.items()
                    },
                )

        return True

    def init_pool_build(self, pool_id):
        """
        Wipes pool data, updates snapshots, and merges base file/func indexes.
        """
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False

        collections = meta.get("collections", [])
        if not collections:
            return True

        # Wipe old data
        self.wipe_pool_data(pool_id)
        # Update snapshots
        self.update_sync_snapshots(pool_id)

        from bsimvis.app.services.index_service import (
            FILE_TAG_FIELDS,
            FUNC_TAG_FIELDS,
            FILE_NUM_FIELDS,
            FUNC_NUM_FIELDS,
        )

        pool_coll = f"global:pool:{pool_id}"

        # 1. Merge TAG registries and buckets (excluding 'sim')
        for level, fields in [
            ("file", FILE_TAG_FIELDS),
            ("func", FUNC_TAG_FIELDS),
        ]:
            for field in fields:
                # Skip pool-specific user annotations and their propagations
                if field in [
                    "user_tags",
                    "file_user_tags",
                    "func_user_tags",
                    "note_owners",
                ]:
                    continue

                bucket_values = set()
                for coll in collections:
                    reg_key = f"{coll}:reg:{level}:{field}"
                    buckets = r.smembers(reg_key)
                    for b in buckets:
                        b_str = b.decode() if isinstance(b, bytes) else str(b)
                        prefix = f"{coll}:idx:{level}:{field}:"
                        if b_str.startswith(prefix):
                            bucket_values.add(b_str[len(prefix) :])

                if bucket_values:
                    pool_reg_key = f"{pool_coll}:reg:{level}:{field}"
                    pipe = r.pipeline()
                    pipe.delete(pool_reg_key)
                    for val in bucket_values:
                        pool_bucket_key = f"{pool_coll}:idx:{level}:{field}:{val}"
                        source_buckets = [
                            f"{coll}:idx:{level}:{field}:{val}" for coll in collections
                        ]
                        existing_sources = [sb for sb in source_buckets if r.exists(sb)]
                        if existing_sources:
                            pipe.sunionstore(pool_bucket_key, *existing_sources)
                            pipe.sadd(pool_reg_key, pool_bucket_key)
                    pipe.execute()

        # 2. Merge NUM ZSets (excluding 'sim')
        for level, fields in [
            ("file", FILE_NUM_FIELDS),
            ("func", FUNC_NUM_FIELDS),
        ]:
            for field in fields:
                source_zsets = [f"{coll}:idx:{level}:{field}" for coll in collections]
                existing_zsets = [sz for sz in source_zsets if r.exists(sz)]
                if existing_zsets:
                    pool_zset_key = f"{pool_coll}:idx:{level}:{field}"
                    r.zunionstore(pool_zset_key, existing_zsets)

        # 3. Merge all_files and all_functions
        pipe = r.pipeline()
        pipe.delete(f"{pool_coll}:all_files")
        pipe.delete(f"{pool_coll}:all_functions")
        all_files_sources = [
            f"{coll}:all_files" for coll in collections if r.exists(f"{coll}:all_files")
        ]
        if all_files_sources:
            pipe.sunionstore(f"{pool_coll}:all_files", *all_files_sources)

        all_funcs_sources = [
            f"{coll}:all_functions"
            for coll in collections
            if r.exists(f"{coll}:all_functions")
        ]
        if all_funcs_sources:
            pipe.sunionstore(f"{pool_coll}:all_functions", *all_funcs_sources)

        # 4. Merge idx:file:functions:*
        md5_set = set()
        for coll in collections:
            for key in r.scan_iter(match=f"{coll}:idx:file:functions:*", count=1000):
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                md5 = key_str.split(":")[-1]
                md5_set.add(md5)

        for md5 in md5_set:
            sources = [
                f"{coll}:idx:file:functions:{md5}"
                for coll in collections
                if r.exists(f"{coll}:idx:file:functions:{md5}")
            ]
            if sources:
                pipe.sunionstore(f"{pool_coll}:idx:file:functions:{md5}", *sources)

        pipe.execute()

        # 5. Merge tags_metadata
        pool_tags_meta_key = f"{pool_coll}:tags_metadata"
        r.delete(pool_tags_meta_key)
        for coll in collections:
            coll_tags_meta = r.hgetall(f"{coll}:tags_metadata")
            if coll_tags_meta:
                r.hset(
                    pool_tags_meta_key,
                    mapping={
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in coll_tags_meta.items()
                    },
                )

        return True

    def finalize_pool_build(self, pool_id):
        """
        Merges similarity-level tag registries and index buckets.
        """
        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False

        collections = meta.get("collections", [])
        if not collections:
            return True

        from bsimvis.app.services.index_config import get_fields_targeting_level

        SIM_TAG_FIELDS = get_fields_targeting_level("sim", is_num=False)
        SIM_NUM_FIELDS = get_fields_targeting_level("sim", is_num=True)

        pool_coll = f"global:pool:{pool_id}"

        # 1. Merge TAG registries and buckets for level 'sim'
        for field in SIM_TAG_FIELDS:
            bucket_values = set()
            for coll in collections:
                reg_key = f"{coll}:reg:sim:{field}"
                buckets = r.smembers(reg_key)
                for b in buckets:
                    b_str = b.decode() if isinstance(b, bytes) else str(b)
                    prefix = f"{coll}:idx:sim:{field}:"
                    if b_str.startswith(prefix):
                        bucket_values.add(b_str[len(prefix) :])

            if bucket_values:
                pool_reg_key = f"{pool_coll}:reg:sim:{field}"
                pipe = r.pipeline()
                pipe.delete(pool_reg_key)
                for val in bucket_values:
                    pool_bucket_key = f"{pool_coll}:idx:sim:{field}:{val}"
                    all_sids = set()
                    for coll in collections:
                        sb = f"{coll}:idx:sim:{field}:{val}"
                        for sid in r.smembers(sb):
                            sid_str = (
                                sid.decode() if isinstance(sid, bytes) else str(sid)
                            )
                            parts = sid_str.split(":")
                            if len(parts) >= 4:
                                coll_name = parts[0]
                                rest = ":".join(parts[3:])
                                pivot = rest.find("::")
                                if pivot != -1:
                                    clean_id1 = rest[:pivot]
                                    clean_id2 = rest[pivot + 2 :]
                                    pool_sid = f"global:pool:{pool_id}:sim:{coll_name}:func:{clean_id1}::{coll_name}:func:{clean_id2}"
                                    all_sids.add(pool_sid)
                    if all_sids:
                        pipe.delete(pool_bucket_key)
                        pipe.sadd(pool_bucket_key, *all_sids)
                        pipe.sadd(pool_reg_key, pool_bucket_key)
                pipe.execute()

        # 2. Merge NUM ZSets for level 'sim'
        for field in SIM_NUM_FIELDS:
            source_zsets = [f"{coll}:idx:sim:{field}" for coll in collections]
            existing_zsets = [sz for sz in source_zsets if r.exists(sz)]
            if existing_zsets:
                pool_zset_key = f"{pool_coll}:idx:sim:{field}"
                r.zunionstore(pool_zset_key, existing_zsets)

        return True


pool_service = PoolService()
