import json
import logging
import time
from bsimvis.app.services.redis_client import get_redis


class PoolService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    @staticmethod
    def similarity_algo(pool):
        """Return the algorithm selected for every pool similarity stage."""
        params = pool.get("func_sim_params") or {}
        from bsimvis.app.services.config_service import config_service

        return (
            params.get("algo")
            or pool.get("algo")
            or config_service.get("similarity.algo", "unweighted_cosine")
        )

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
        # Pool function algorithm is canonical for all pool stages.
        algo = self.similarity_algo(
            {"func_sim_params": func_sim_params, "algo": config.get("algo")}
        )

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
            "algo": algo,
            "top_k": config.get("top_k", 1000),
            "min_score": config.get("min_score", 0.9),
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

    def remove_collection(self, pool_id, collection):
        """Detach a member, remove its pair edges, and rebuild affected clusters."""
        from bsimvis.app.services.index_service import delete_similarity
        from bsimvis.app.services.bin_sim_service import _unindex_bin_sim_pair

        r = self.r
        meta = self.get_pool(pool_id)
        if not meta:
            return False, "Pool not found"
        if collection not in meta.get("collections", []):
            return False, "Collection is not a pool member"

        def txt(value):
            return value.decode() if isinstance(value, bytes) else str(value)

        pool_coll = f"global:pool:{pool_id}"
        file_ids = [txt(v) for v in r.smembers(f"{collection}:all_files")]
        func_ids = [txt(v) for v in r.smembers(f"{collection}:all_functions")]
        sim_ids, bin_ids = set(), set()
        for file_id in file_ids:
            md5 = file_id.split(":file:", 1)[-1]
            sim_ids.update(
                txt(v)
                for v in r.smembers(f"{pool_coll}:sim:involves:file:{collection}:{md5}")
            )
            bin_ids.update(
                txt(v)
                for v in r.smembers(f"{pool_coll}:bin_sim:involves:{collection}:{md5}")
            )
        for fid in func_ids:
            sim_ids.update(
                txt(v) for v in r.smembers(f"{pool_coll}:sim:involves:func:{fid}")
            )

        for sid in sim_ids:
            raw = r.get(sid)
            try:
                doc = json.loads(raw) if raw else {}
            except (ValueError, TypeError):
                doc = {}
            if not doc:
                continue
            pipe = r.pipeline(transaction=False)
            for fid in (doc.get("id1"), doc.get("id2")):
                if fid:
                    pipe.srem(f"{pool_coll}:sim:involves:func:{fid}", sid)
            for coll, md5 in (
                (doc.get("coll_1"), doc.get("md5_1")),
                (doc.get("coll_2"), doc.get("md5_2")),
            ):
                if coll and md5:
                    pipe.srem(f"{pool_coll}:sim:involves:file:{coll}:{md5}", sid)
            pipe.zrem(f"{pool_coll}:sim:score", sid)
            pipe.zrem(f"{pool_coll}:sim:min_features", sid)
            pipe.zrem(
                f"{pool_coll}:sim:is_cross_binary:{doc.get('is_cross_binary', 'false')}",
                sid,
            )
            func_meta, file_meta = [], []
            for fid in (doc.get("id1"), doc.get("id2")):
                try:
                    func_meta.append(json.loads(r.get(f"{fid}:meta") or "{}"))
                except (ValueError, TypeError):
                    func_meta.append({})
            for coll, md5 in (
                (doc.get("coll_1"), doc.get("md5_1")),
                (doc.get("coll_2"), doc.get("md5_2")),
            ):
                try:
                    file_meta.append(
                        json.loads(r.get(f"{coll}:file:{md5}:meta") or "{}")
                    )
                except (ValueError, TypeError):
                    file_meta.append({})
            delete_similarity(pipe, pool_coll, sid, doc, *func_meta, *file_meta)
            pipe.delete(sid)
            pipe.execute()

        for sid in bin_ids:
            raw = r.get(sid)
            try:
                doc = json.loads(raw) if raw else {}
            except (ValueError, TypeError):
                doc = {}
            if not doc:
                continue
            pipe = r.pipeline(transaction=False)
            for coll, md5 in (
                (doc.get("coll_a"), doc.get("md5_a")),
                (doc.get("coll_b"), doc.get("md5_b")),
            ):
                if coll and md5:
                    pipe.srem(f"{pool_coll}:bin_sim:involves:{coll}:{md5}", sid)
            algo = doc.get("algo", meta.get("algo", "unweighted_cosine"))
            for axis in ("", "_code", "_library", "_content"):
                pipe.zrem(f"{pool_coll}:bin_sim:score{axis}:{algo}", sid)
            pipe.srem(f"{pool_coll}:bin_sim:built:{algo}", sid)
            file_meta = []
            for coll, md5 in (
                (doc.get("coll_a"), doc.get("md5_a")),
                (doc.get("coll_b"), doc.get("md5_b")),
            ):
                try:
                    file_meta.append(
                        json.loads(r.get(f"{coll}:file:{md5}:meta") or "{}")
                    )
                except (ValueError, TypeError):
                    file_meta.append({})
            _unindex_bin_sim_pair(pipe, pool_coll, sid, doc, *file_meta)
            pipe.delete(sid)
            pipe.execute()

        pipe = r.pipeline()
        pipe.srem(f"global:pool:{pool_id}:collections_list", collection)
        pipe.srem(f"{collection}:pools", pool_id)
        pipe.hdel(f"global:pool:{pool_id}:collections", collection)
        pipe.hset(
            f"global:pool:{pool_id}:meta",
            mapping={"sync_status": "outdated", "last_built_at": 0},
        )
        pipe.hdel(
            f"global:pool:{pool_id}:meta",
            "total_func_similarities",
            "total_func_clusters",
            "total_file_similarities",
            "total_file_clusters",
        )
        pipe.execute()

        # Rebuild merged member indexes after deleting the old merged buckets.
        for pattern in (
            f"{pool_coll}:idx:file:*",
            f"{pool_coll}:idx:func:*",
            f"{pool_coll}:reg:file:*",
            f"{pool_coll}:reg:func:*",
            f"{pool_coll}:idx:file:functions:*",
        ):
            for key in r.scan_iter(match=pattern, count=1000):
                r.delete(key)
        r.delete(
            f"{pool_coll}:all_files",
            f"{pool_coll}:all_functions",
            f"{pool_coll}:tags_metadata",
        )
        self.build_pool_indexes(pool_id)
        self.update_sync_snapshots(pool_id)

        if self.get_pool(pool_id).get("collections"):
            from bsimvis.app.services.cluster_service import cluster_service
            from bsimvis.app.services.bin_cluster_service import bin_cluster_service

            algo = self.similarity_algo(self.get_pool(pool_id))
            cluster_service.clear_clustering(pool_coll, algo)
            cluster_service.run_pool_clustering(pool_id)
            for axis in ("overall", "code", "library", "content"):
                bin_cluster_service.clear_clusters(pool_coll, algo, axis=axis)
                bin_cluster_service.run_pool_bin_clustering(pool_id, axis=axis)
        return True, "Collection removed from pool"

    def edit_pool_name(self, pool_id, name):
        """Edits the name of a pool."""
        r = self.r
        pool_meta_key = f"global:pool:{pool_id}:meta"
        if not r.exists(pool_meta_key):
            return False, "Pool not found"
        r.hset(pool_meta_key, "name", name)
        return True, "Pool name updated successfully"

    def update_pool_config(self, pool_id, config):
        """Update stored pool parameters without touching generated data."""
        if not self.r.exists(f"global:pool:{pool_id}:meta"):
            return False, "Pool not found"
        current = self.get_pool(pool_id) or {}
        fields = {}
        if "only_cross_collection" in config:
            fields["only_cross_collection"] = (
                "1" if config["only_cross_collection"] else "0"
            )
        for name in (
            "func_sim_params",
            "func_cluster_params",
            "file_sim_params",
            "file_cluster_params",
        ):
            if name in config:
                merged = dict(current.get(name) or {})
                merged.update(config[name] or {})
                fields[name] = json.dumps(merged)
        if fields:
            self.r.hset(f"global:pool:{pool_id}:meta", mapping=fields)
        return True, "Pool parameters updated successfully"

    def add_collection(self, pool_id, collection):
        if not self.r.exists(f"global:pool:{pool_id}:meta"):
            return False, "Pool not found"
        if not self.r.sismember("global:collections", collection):
            return False, "Collection not found"
        pipe = self.r.pipeline()
        pipe.sadd(f"global:pool:{pool_id}:collections_list", collection)
        pipe.sadd(f"{collection}:pools", pool_id)
        pipe.hset(
            f"global:pool:{pool_id}:meta",
            mapping={"sync_status": "outdated", "last_built_at": 0},
        )
        pipe.execute()
        return True, "Collection added to pool"

    def clear_pool_targets(self, pool_id, targets):
        """Clear generated pool keys for selected maintenance targets."""
        suffixes = {
            "function_similarity": ":sim:",
            "function_cluster": ":cluster:",
            "binary_similarity": ":bin_sim:",
            "binary_cluster": ":bin_cluster:",
        }
        pipe = self.r.pipeline()
        for target in targets:
            cursor = 0
            while True:
                cursor, keys = self.r.scan(
                    cursor, match=f"global:pool:{pool_id}{suffixes[target]}*"
                )
                if keys:
                    pipe.delete(*keys)
                if cursor == 0:
                    break
        pipe.hset(f"global:pool:{pool_id}:meta", "sync_status", "outdated")
        pipe.execute()

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

        # Nested function config is canonical for all pool stages, including legacy pools.
        meta["algo"] = self.similarity_algo(meta)

        # Get sync state snapshots
        sync_snapshots = r.hgetall(f"global:pool:{pool_id}:collections")
        meta["sync_snapshots"] = {
            k.decode() if isinstance(k, bytes) else k: json.loads(v)
            for k, v in sync_snapshots.items()
        }

        # Retrieve pool-wide similarities & clusters counts
        updated_meta = {}

        # Function similarities
        if "total_func_similarities" not in meta:
            total_func_sim = r.zcard(f"global:pool:{pool_id}:sim:score")
            meta["total_func_similarities"] = total_func_sim
            updated_meta["total_func_similarities"] = total_func_sim
        else:
            meta["total_func_similarities"] = int(meta["total_func_similarities"])

        # Function clusters
        if "total_func_clusters" not in meta:
            total_func_clust = r.scard(f"global:pool:{pool_id}:cluster:list")
            meta["total_func_clusters"] = total_func_clust
            updated_meta["total_func_clusters"] = total_func_clust
        else:
            meta["total_func_clusters"] = int(meta["total_func_clusters"])

        # File similarities
        # File bin_sim lives in the namespace of the function algo its clusters came
        # from; there is no separate file algo.
        file_algo = self.similarity_algo(meta)
        if "total_file_similarities" not in meta:
            total_file_sim = r.zcard(f"global:pool:{pool_id}:bin_sim:score:{file_algo}")
            meta["total_file_similarities"] = total_file_sim
            updated_meta["total_file_similarities"] = total_file_sim
        else:
            meta["total_file_similarities"] = int(meta["total_file_similarities"])

        # File clusters (if any list exists, otherwise default 0 or check algorithm clusters)
        if "total_file_clusters" not in meta:
            total_file_clust = r.scard(
                f"global:pool:{pool_id}:bin_cluster:list:{file_algo}"
            )
            meta["total_file_clusters"] = total_file_clust
            updated_meta["total_file_clusters"] = total_file_clust
        else:
            meta["total_file_clusters"] = int(meta["total_file_clusters"])

        # Real files count
        total_files = r.scard(f"global:pool:{pool_id}:all_files")
        meta["total_files"] = total_files

        # Real functions count
        total_functions = r.scard(f"global:pool:{pool_id}:all_functions")
        meta["total_functions"] = total_functions

        if updated_meta:
            r.hset(f"global:pool:{pool_id}:meta", mapping=updated_meta)

        return meta

    def list_pools(self, collection=None, refresh_sync=False):
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
            # ponytail: refresh_sync recomputes live sync state per pool (extra
            # per-collection scans + a 2nd get_pool). Off by default so the list
            # stays cheap at ~1k pools; sync_status is read from cached meta.
            # Upgrade path if this ever gets hot: pipeline get_pool reads / ZSET
            # date index + page-before-hydrate.
            if refresh_sync:
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
        pipe.hdel(
            f"global:pool:{pool_id}:meta",
            "total_func_similarities",
            "total_func_clusters",
            "total_file_similarities",
            "total_file_clusters",
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

            current_generation = int(r.get(f"{coll}:features:generation") or 0)

            snap = snapshots.get(coll, {})
            snap_count = snap.get("file_count", -1)
            snap_last_entry = snap.get("last_entry_date", -1)

            coll_outdated = (
                (current_count != snap_count)
                or (current_last_entry > snap_last_entry)
                # A snapshot taken before generations were tracked has no baseline;
                # don't read its absence as drift.
                or current_generation
                != snap.get("feature_generation", current_generation)
            )
            if coll_outdated:
                is_outdated = True

            details[coll] = {
                "outdated": coll_outdated,
                "current": {
                    "file_count": current_count,
                    "last_entry_date": current_last_entry,
                    "feature_generation": current_generation,
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
                "feature_generation": int(r.get(f"{coll}:features:generation") or 0),
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
            to_pool_indexed_id,
        )
        from bsimvis.app.services.index_config import (
            get_fields_targeting_level,
            POOL_LOCAL_FIELDS,
        )

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
                # Cluster artifacts belong to the namespace that computed them.
                if field in POOL_LOCAL_FIELDS:
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
                            # A pool owns its own sim docs, so member sids must be
                            # rewritten into the pool namespace before indexing.
                            all_sids = set()
                            for coll in collections:
                                sb = f"{coll}:idx:{level}:{field}:{val}"
                                for sid in r.smembers(sb):
                                    sid_str = (
                                        sid.decode()
                                        if isinstance(sid, bytes)
                                        else str(sid)
                                    )
                                    pool_sid = to_pool_indexed_id(
                                        sid_str, "sim", pool_id
                                    )
                                    if pool_sid:
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
                if field in POOL_LOCAL_FIELDS:
                    continue
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
        r.hset(
            f"global:pool:{pool_id}:meta",
            "build_generations",
            json.dumps(
                {
                    coll: int(r.get(f"{coll}:features:generation") or 0)
                    for coll in collections
                }
            ),
        )

        from bsimvis.app.services.index_service import (
            FILE_TAG_FIELDS,
            FUNC_TAG_FIELDS,
            FILE_NUM_FIELDS,
            FUNC_NUM_FIELDS,
        )
        from bsimvis.app.services.index_config import POOL_LOCAL_FIELDS

        pool_coll = f"global:pool:{pool_id}"

        # 1. Merge TAG registries and buckets (excluding 'sim')
        for level, fields in [
            ("file", FILE_TAG_FIELDS),
            ("func", FUNC_TAG_FIELDS),
        ]:
            for field in fields:
                # Cluster artifacts belong to the namespace that computed them.
                if field in POOL_LOCAL_FIELDS:
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
                    bucket_list = list(bucket_values)

                    # Chunk to avoid holding lock / freezing redis
                    chunk_size = 500
                    for i in range(0, len(bucket_list), chunk_size):
                        chunk = bucket_list[i : i + chunk_size]

                        exists_pipe = r.pipeline(transaction=False)
                        for val in chunk:
                            for coll in collections:
                                exists_pipe.exists(f"{coll}:idx:{level}:{field}:{val}")
                        exists_results = exists_pipe.execute()

                        pipe = r.pipeline()
                        if i == 0:
                            pipe.delete(pool_reg_key)

                        idx = 0
                        for val in chunk:
                            pool_bucket_key = f"{pool_coll}:idx:{level}:{field}:{val}"
                            existing_sources = []
                            for coll in collections:
                                if exists_results[idx]:
                                    existing_sources.append(
                                        f"{coll}:idx:{level}:{field}:{val}"
                                    )
                                idx += 1

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
                if field in POOL_LOCAL_FIELDS:
                    continue
                source_zsets = [f"{coll}:idx:{level}:{field}" for coll in collections]
                # Check existences in a single pipeline
                exists_pipe = r.pipeline(transaction=False)
                for sz in source_zsets:
                    exists_pipe.exists(sz)
                exists_results = exists_pipe.execute()

                existing_zsets = [
                    sz for sz, exists in zip(source_zsets, exists_results) if exists
                ]
                if existing_zsets:
                    pool_zset_key = f"{pool_coll}:idx:{level}:{field}"
                    r.zunionstore(pool_zset_key, existing_zsets)

        # 3. Merge all_files and all_functions
        pipe = r.pipeline()
        pipe.delete(f"{pool_coll}:all_files")
        pipe.delete(f"{pool_coll}:all_functions")

        # Check existence in a pipeline
        all_files_sources = [f"{coll}:all_files" for coll in collections]
        all_funcs_sources = [f"{coll}:all_functions" for coll in collections]

        exists_pipe = r.pipeline(transaction=False)
        for path in all_files_sources + all_funcs_sources:
            exists_pipe.exists(path)
        exists_results = exists_pipe.execute()

        files_exists = exists_results[: len(all_files_sources)]
        funcs_exists = exists_results[len(all_files_sources) :]

        existing_files = [
            path for path, exists in zip(all_files_sources, files_exists) if exists
        ]
        existing_funcs = [
            path for path, exists in zip(all_funcs_sources, funcs_exists) if exists
        ]

        if existing_files:
            pipe.sunionstore(f"{pool_coll}:all_files", *existing_files)
        if existing_funcs:
            pipe.sunionstore(f"{pool_coll}:all_functions", *existing_funcs)
        pipe.execute()  # section 4 reassigns `pipe`, so flush section 3 first

        # 4. Merge idx:file:functions:*
        md5_set = set()
        for coll in collections:
            for key in r.scan_iter(match=f"{coll}:idx:file:functions:*", count=1000):
                key_str = key.decode() if isinstance(key, bytes) else str(key)
                md5 = key_str.split(":")[-1]
                md5_set.add(md5)

        if md5_set:
            md5_list = list(md5_set)
            chunk_size = 500
            for i in range(0, len(md5_list), chunk_size):
                chunk = md5_list[i : i + chunk_size]

                exists_pipe = r.pipeline(transaction=False)
                for md5 in chunk:
                    for coll in collections:
                        exists_pipe.exists(f"{coll}:idx:file:functions:{md5}")
                exists_results = exists_pipe.execute()

                pipe = r.pipeline()
                idx = 0
                for md5 in chunk:
                    sources = []
                    for coll in collections:
                        if exists_results[idx]:
                            sources.append(f"{coll}:idx:file:functions:{md5}")
                        idx += 1
                    if sources:
                        pipe.sunionstore(
                            f"{pool_coll}:idx:file:functions:{md5}", *sources
                        )
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
            logging.error(f"Pool {pool_id} not found")
            return False

        collections = meta.get("collections", [])
        if not collections:
            logging.info(f"No collections defined for pool {pool_id}")
            return True

        # No baseline recorded (pool built before generations were tracked, or a
        # finalize already consumed it) means there is nothing to compare against.
        # Only an actual mismatch is a mid-build ingest.
        expected_raw = r.hget(f"global:pool:{pool_id}:meta", "build_generations")
        if expected_raw:
            expected = json.loads(expected_raw)
            current = {
                coll: int(r.get(f"{coll}:features:generation") or 0)
                for coll in collections
            }
            if current != expected:
                logging.warning(
                    "Pool %s feature generations changed during build: %s -> %s",
                    pool_id,
                    expected,
                    current,
                )
                r.hset(f"global:pool:{pool_id}:meta", "sync_status", "outdated")
                return False

        from bsimvis.app.services.index_config import get_fields_targeting_level
        from bsimvis.app.services.index_service import to_pool_indexed_id

        SIM_TAG_FIELDS = get_fields_targeting_level("sim", is_num=False)
        SIM_NUM_FIELDS = get_fields_targeting_level("sim", is_num=True)

        pool_coll = f"global:pool:{pool_id}"
        logging.info(
            f"Starting finalize_pool_build for pool {pool_id} (collections: {collections})"
        )

        # 1. Merge TAG registries and buckets for level 'sim'
        for field in SIM_TAG_FIELDS:
            logging.info(f"Merging TAG registry & buckets for field '{field}'")
            bucket_values = set()
            for coll in collections:
                reg_key = f"{coll}:reg:sim:{field}"
                buckets = r.smembers(reg_key)
                for b in buckets:
                    b_str = b.decode() if isinstance(b, bytes) else str(b)
                    prefix = f"{coll}:idx:sim:{field}:"
                    if b_str.startswith(prefix):
                        bucket_values.add(b_str[len(prefix) :])

            logging.info(
                f"Field '{field}' has {len(bucket_values)} unique bucket values across collections"
            )

            if bucket_values:
                pool_reg_key = f"{pool_coll}:reg:sim:{field}"
                logging.info(f"Deleting pool registry key {pool_reg_key}")
                r.delete(pool_reg_key)

                bucket_list = list(bucket_values)
                chunk_size = 100
                total_chunks = (len(bucket_list) + chunk_size - 1) // chunk_size

                for i in range(0, len(bucket_list), chunk_size):
                    chunk = bucket_list[i : i + chunk_size]
                    chunk_idx = i // chunk_size + 1
                    logging.info(
                        f"Field '{field}': processing chunk {chunk_idx}/{total_chunks} (size {len(chunk)})"
                    )

                    # Phase 1: Pipeline read smembers for all collections and bucket values in the chunk
                    read_pipe = r.pipeline(transaction=False)
                    key_mapping = []
                    for val in chunk:
                        for coll in collections:
                            sb = f"{coll}:idx:sim:{field}:{val}"
                            read_pipe.smembers(sb)
                            key_mapping.append((val, coll))

                    logging.info(
                        f"Executing read pipeline for chunk {chunk_idx} ({len(key_mapping)} commands)"
                    )
                    read_results = read_pipe.execute()
                    logging.info(f"Read pipeline for chunk {chunk_idx} completed")

                    # Group similarity IDs by bucket value
                    val_to_sids = {}
                    for (val, coll), sids in zip(key_mapping, read_results):
                        if not sids:
                            continue
                        if val not in val_to_sids:
                            val_to_sids[val] = set()
                        for sid in sids:
                            sid_str = (
                                sid.decode() if isinstance(sid, bytes) else str(sid)
                            )
                            pool_sid = to_pool_indexed_id(sid_str, "sim", pool_id)
                            if pool_sid:
                                val_to_sids[val].add(pool_sid)

                    # Phase 2: Pipeline write results back
                    if val_to_sids:
                        write_pipe = r.pipeline(transaction=False)
                        sadd_cmd_count = 0
                        for val, all_sids in val_to_sids.items():
                            if all_sids:
                                pool_bucket_key = f"{pool_coll}:idx:sim:{field}:{val}"
                                write_pipe.delete(pool_bucket_key)

                                # Chunk SADD calls to prevent excessively large Redis commands
                                sids_list = list(all_sids)
                                for k in range(0, len(sids_list), 2000):
                                    write_pipe.sadd(
                                        pool_bucket_key, *sids_list[k : k + 2000]
                                    )
                                    sadd_cmd_count += 1

                                write_pipe.sadd(pool_reg_key, pool_bucket_key)

                        logging.info(
                            f"Executing write pipeline for chunk {chunk_idx} (with {sadd_cmd_count} SADD chunks)"
                        )
                        write_pipe.execute()
                        logging.info(f"Write pipeline for chunk {chunk_idx} completed")

        # 2. Merge NUM ZSets for level 'sim'
        for field in SIM_NUM_FIELDS:
            logging.info(f"Merging NUM ZSets for field '{field}'")
            source_zsets = [f"{coll}:idx:sim:{field}" for coll in collections]
            existing_zsets = [sz for sz in source_zsets if r.exists(sz)]
            if existing_zsets:
                pool_zset_key = f"{pool_coll}:idx:sim:{field}"
                logging.info(
                    f"Running zunionstore on {pool_zset_key} with {existing_zsets}"
                )
                r.zunionstore(pool_zset_key, existing_zsets)
                logging.info("zunionstore completed")

        r.hdel(f"global:pool:{pool_id}:meta", "total_func_similarities")

        self.update_sync_snapshots(pool_id)
        r.hdel(f"global:pool:{pool_id}:meta", "build_generations")
        from bsimvis.app.services.similarity_axis_index import ready_key

        r.set(ready_key(pool_coll), "1")

        return True


pool_service = PoolService()
