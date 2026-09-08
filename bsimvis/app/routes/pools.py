from flask import request, jsonify
from bsimvis.app.services.pool_service import pool_service
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.routes import _list_query as lq

job_service = JobService()


def create_pool():
    import uuid

    data = request.json
    pool_id = data.get("pool_id")
    if not pool_id:
        pool_id = str(uuid.uuid4())
    name = data.get("name")
    collections = data.get("collections", [])
    config = data.get("config", {})

    if not name or not collections:
        return {"error": "Missing required fields (name, collections)"}, 400

    success, message = pool_service.create_pool(pool_id, name, collections, config)
    if not success:
        return {"error": message}, 400

    # Fetch all files in the member collections to build parallel build_pool_sim tasks
    redis_client = pool_service.r
    file_tasks = []
    for coll in collections:
        all_files_key = f"{coll}:all_files"
        file_keys = [
            d.decode() if isinstance(d, bytes) else str(d)
            for d in redis_client.smembers(all_files_key)
        ]
        for k in file_keys:
            if k.endswith(":meta"):
                continue
            parts = k.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                skip_write = config.get("skip_write", False)
                file_tasks.append(
                    (
                        JobType.BUILD_POOL_SIM,
                        {"pool_id": pool_id, "file_md5": md5, "skip_write": skip_write},
                    )
                )

    tasks = [(JobType.INIT_POOL_BUILD, {"pool_id": pool_id})]
    if file_tasks:
        group_id = job_service.create_group(file_tasks, enqueue=False)
        tasks.append(group_id)
    else:
        tasks.append((JobType.BUILD_POOL_SIM, {"pool_id": pool_id}))

    tasks.extend(
        [
            (JobType.FINALIZE_POOL_BUILD, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL, {"pool_id": pool_id}),
            (JobType.BUILD_POOL_BIN_SIM, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL_BINARIES, {"pool_id": pool_id}),
            (JobType.INDEX_SIM, {"collection": "", "pool_id": pool_id}),
        ]
    )

    pipeline_id = job_service.create_pipeline(tasks)

    return {"message": message, "pool_id": pool_id, "job_id": pipeline_id}, 201


def get_pool(pool_id):
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404
    return pool


def list_pools():
    """Lists pools with keyword search (q), specific-field filters, and sorting.

    Params: q, name, sync_status, collection (membership), sort_by, sort_order,
    offset, limit, refresh_sync, min_/max_ ranges on created_at/last_built_at
    and the count fields.
    """
    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return {"error": "offset and limit must be integers"}, 400

    collection = request.args.get("collection")
    refresh_sync = request.args.get("refresh_sync") in ("1", "true", "True")
    pools = pool_service.list_pools(collection=collection, refresh_sync=refresh_sync)

    count_fields = [
        "total_func_similarities",
        "total_func_clusters",
        "total_file_similarities",
        "total_file_clusters",
        "total_files",
        "total_functions",
    ]

    # Filter: q over name/id/collections/sync_status, plus specific-field filters
    kws = lq.keywords()
    name_filter = request.args.get("name", "").lower().strip()
    id_filter = request.args.get("id", "").lower().strip()
    status_filter = request.args.get("sync_status", "").lower().strip()
    ranges = {
        f: lq.num_range(f) for f in ["created_at", "last_built_at"] + count_fields
    }

    def keep(p):
        colls = " ".join(p.get("collections", []))
        if not lq.matches_keywords(
            kws, p.get("name"), p.get("id"), colls, p.get("sync_status")
        ):
            return False
        if name_filter and name_filter not in str(p.get("name", "")).lower():
            return False
        if id_filter and id_filter not in str(p.get("id", "")).lower():
            return False
        if status_filter and status_filter != str(p.get("sync_status", "")).lower():
            return False
        return all(lq.in_range(p.get(f, 0), rng) for f, rng in ranges.items())

    pools = [p for p in pools if keep(p)]

    key_fns = {
        "name": lambda p: str(p.get("name", "")).lower(),
        "id": lambda p: str(p.get("id", "")).lower(),
        "sync_status": lambda p: str(p.get("sync_status", "")).lower(),
        "created_at": lambda p: int(p.get("created_at", 0) or 0),
        "last_built_at": lambda p: int(p.get("last_built_at", 0) or 0),
    }
    key_fns.update(
        {f: (lambda f: lambda p: int(p.get(f, 0) or 0))(f) for f in count_fields}
    )

    page, total = lq.sort_and_paginate(
        pools, offset, limit, "last_built_at", True, key_fns
    )
    return {"pools": page, "total": total, "offset": offset, "limit": limit}


def delete_pool(pool_id):
    success, message = pool_service.delete_pool(pool_id)
    if not success:
        return {"error": message}, 404
    return {"message": message}


def edit_pool(pool_id):
    data = request.json
    name = data.get("name") if data else None
    if not name:
        return {"error": "Missing name parameter"}, 400
    success, message = pool_service.edit_pool_name(pool_id, name)
    if not success:
        return {"error": message}, 404
    return {"message": message}


def build_pool(pool_id):
    """Enqueues a job to build pool similarities."""
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404

    collections = pool.get("collections", [])
    redis_client = pool_service.r
    file_tasks = []
    for coll in collections:
        all_files_key = f"{coll}:all_files"
        file_keys = [
            d.decode() if isinstance(d, bytes) else str(d)
            for d in redis_client.smembers(all_files_key)
        ]
        for k in file_keys:
            if k.endswith(":meta"):
                continue
            parts = k.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                func_sim_params = pool.get("func_sim_params", {})
                skip_write = func_sim_params.get(
                    "skip_write", pool.get("skip_write", False)
                )
                file_tasks.append(
                    (
                        JobType.BUILD_POOL_SIM,
                        {"pool_id": pool_id, "file_md5": md5, "skip_write": skip_write},
                    )
                )

    tasks = [(JobType.INIT_POOL_BUILD, {"pool_id": pool_id})]
    if file_tasks:
        group_id = job_service.create_group(file_tasks, enqueue=False)
        tasks.append(group_id)
    else:
        tasks.append((JobType.BUILD_POOL_SIM, {"pool_id": pool_id}))

    tasks.extend(
        [
            (JobType.FINALIZE_POOL_BUILD, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL, {"pool_id": pool_id}),
            (JobType.BUILD_POOL_BIN_SIM, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL_BINARIES, {"pool_id": pool_id}),
            (JobType.INDEX_SIM, {"collection": "", "pool_id": pool_id}),
        ]
    )

    pipeline_id = job_service.create_pipeline(tasks)
    return {"job_id": pipeline_id, "message": "Pool build pipeline enqueued"}


def cluster_pool(pool_id):
    """Enqueues a job to cluster pool similarities, cleaning previous clusters and binary similarities first."""
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404

    # ponytail: Clean previous function clusters, bin sim, and bin clusters before rebuilding
    r = pool_service.r
    pipe = r.pipeline()

    # 1. Clear function clusters
    pipe.delete(f"global:pool:{pool_id}:cluster:list")

    # 2. Clear binary similarities and scores
    algo = pool.get("algo", "unweighted_cosine")
    pipe.delete(f"global:pool:{pool_id}:bin_sim:score:{algo}")
    pipe.delete(f"global:pool:{pool_id}:bin_sim:built:{algo}")

    # Clean binary similarity documents and involves indexes
    cursor = 0
    pattern_sim = f"global:pool:{pool_id}:bin_sim:*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern_sim, count=1000)
        if keys:
            pipe.delete(*keys)
        if cursor == 0:
            break

    # 3. Clear binary clusters
    pipe.delete(f"global:pool:{pool_id}:bin_cluster:list")

    pipe.execute()

    pipeline_id = job_service.create_pipeline(
        [
            (JobType.CLUSTER_POOL, {"pool_id": pool_id}),
            (JobType.BUILD_POOL_BIN_SIM, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL_BINARIES, {"pool_id": pool_id}),
            (JobType.INDEX_SIM, {"collection": "", "pool_id": pool_id}),
        ]
    )
    return {"job_id": pipeline_id, "message": "Pool clustering pipeline enqueued"}


def sync_check(pool_id):
    status = pool_service.check_sync_status(pool_id)
    if not status:
        return {"error": "Pool not found"}, 404
    return status


def rebuild_pool(pool_id):
    """Wipes all computed pool similarity & cluster data, then triggers rebuilding pipeline."""
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404

    pool_service.wipe_pool_data(pool_id)

    collections = pool.get("collections", [])
    redis_client = pool_service.r
    file_tasks = []
    for coll in collections:
        all_files_key = f"{coll}:all_files"
        file_keys = [
            d.decode() if isinstance(d, bytes) else str(d)
            for d in redis_client.smembers(all_files_key)
        ]
        for k in file_keys:
            if k.endswith(":meta"):
                continue
            parts = k.split(":")
            if len(parts) >= 3:
                md5 = parts[2]
                file_tasks.append(
                    (JobType.BUILD_POOL_SIM, {"pool_id": pool_id, "file_md5": md5})
                )

    tasks = [(JobType.INIT_POOL_BUILD, {"pool_id": pool_id})]
    if file_tasks:
        group_id = job_service.create_group(file_tasks, enqueue=False)
        tasks.append(group_id)
    else:
        tasks.append((JobType.BUILD_POOL_SIM, {"pool_id": pool_id}))

    tasks.extend(
        [
            (JobType.FINALIZE_POOL_BUILD, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL, {"pool_id": pool_id}),
            (JobType.BUILD_POOL_BIN_SIM, {"pool_id": pool_id}),
            (JobType.CLUSTER_POOL_BINARIES, {"pool_id": pool_id}),
            (JobType.INDEX_SIM, {"collection": "", "pool_id": pool_id}),
        ]
    )

    pipeline_id = job_service.create_pipeline(tasks)
    return {
        "message": "Pool data wiped and rebuild pipeline enqueued",
        "pool_id": pool_id,
        "job_id": pipeline_id,
    }
