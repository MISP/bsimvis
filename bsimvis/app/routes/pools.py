from flask import request, jsonify
from bsimvis.app.services.pool_service import pool_service
from bsimvis.app.services.job_service import JobService, JobType

job_service = JobService()

def create_pool():
    data = request.json
    pool_id = data.get("pool_id")
    name = data.get("name")
    collections = data.get("collections", [])
    config = data.get("config", {})
    
    if not pool_id or not name or not collections:
        return {"error": "Missing required fields (pool_id, name, collections)"}, 400
        
    success, message = pool_service.create_pool(pool_id, name, collections, config)
    if not success:
        return {"error": message}, 400
        
    # Schedule similarity building followed by clustering in a unified pipeline
    pipeline_id = job_service.create_pipeline([
        (JobType.BUILD_POOL_SIM, {"pool_id": pool_id}),
        (JobType.CLUSTER_POOL, {"pool_id": pool_id})
    ])
    
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
        
    # We will implement SimilarityService.build_pool next
    job_id = job_service.enqueue_job(
        JobType.BUILD_POOL_SIM,
        {"pool_id": pool_id},
        pool_id=pool_id
    )
    return {"job_id": job_id, "message": "Pool build job enqueued"}

def cluster_pool(pool_id):
    """Enqueues a job to cluster pool similarities."""
    pool = pool_service.get_pool(pool_id)
    if not pool:
        return {"error": "Pool not found"}, 404
        
    # We will implement ClusterService.run_pool_clustering later
    job_id = job_service.enqueue_job(
        JobType.CLUSTER_POOL,
        {"pool_id": pool_id},
        pool_id=pool_id
    )
    return {"job_id": job_id, "message": "Pool clustering job enqueued"}

def sync_check(pool_id):
    status = pool_service.check_sync_status(pool_id)
    if not status:
        return {"error": "Pool not found"}, 404
    return status
