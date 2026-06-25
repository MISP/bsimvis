from flask import request, jsonify
from bsimvis.app.services.pool_service import pool_service
from bsimvis.app.services.job_service import JobService, JobType

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

    return {"message": message, "pool_id": pool_id, "job_id": pipeline_id}, 201


def get_pool(pool_id):
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404
    return pool


def list_pools():
    collection = request.args.get("collection")
    pools = pool_service.list_pools(collection=collection)
    return {"pools": pools}


def delete_pool(pool_id):
    success, message = pool_service.delete_pool(pool_id)
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
    return {"job_id": pipeline_id, "message": "Pool build pipeline enqueued"}


def cluster_pool(pool_id):
    """Enqueues a job to cluster pool similarities."""
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404

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
