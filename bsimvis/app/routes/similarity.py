from flask import Blueprint, jsonify, request
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.redis_client import get_redis

similarity_bp = Blueprint("similarity", __name__)
job_service = JobService()
similarity_service = SimilarityService()

@similarity_bp.route("/api/similarity/list", methods=["GET"])
def list_similarities():
    """Lists similarities (scores) for a given md5 or batch_uuid."""
    collection = request.args.get("collection", "main")
    md5 = request.args.get("md5")
    batch_uuid = request.args.get("batch")
    algo = request.args.get("algo", "unweighted_cosine")
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    r = get_redis()
    
    # We use the existing idx:coll:sim:md5_1:VAL or batch_uuid1:VAL indices
    index_key = None
    if md5:
        index_key = f"idx:{collection}:sim:md5_1:{md5}"
    elif batch_uuid:
        index_key = f"idx:{collection}:sim:batch_uuid1:{batch_uuid}"
    
    if not index_key:
        return jsonify({"error": "md5 or batch parameter required"}), 400

    # Get total and slice
    total = r.zcard(index_key)
    sim_keys = r.zrange(index_key, offset, offset + limit - 1)
    
    results = []
    if sim_keys:
        pipe = r.pipeline()
        for k in sim_keys:
            pipe.json().get(k, "$")
        raw_docs = pipe.execute()
        
        for doc in raw_docs:
            if doc:
                results.append(doc[0] if isinstance(doc, list) else doc)

    return jsonify({
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results
    })

@similarity_bp.route("/api/similarity/status", methods=["GET"])
def similarity_status():
    """Returns build status (counts) for a target."""
    collection = request.args.get("collection", "main")
    md5 = request.args.get("md5")
    batch_uuid = request.args.get("batch")
    algo = request.args.get("algo", "unweighted_cosine")
    
    status = similarity_service.get_build_status(collection, batch_uuid=batch_uuid, md5=md5, algo=algo)
    return jsonify(status)

@similarity_bp.route("/api/similarity/batches", methods=["GET"])
def list_batches():
    """Returns detailed build status (for batches or files)."""
    collection = request.args.get("collection", "main")
    algo = request.args.get("algo", "unweighted_cosine")
    by_type = request.args.get("by", "batch")
    
    if by_type == "md5":
        results = similarity_service.list_files_build_status(collection, algo=algo)
    else:
        results = similarity_service.list_batches_build_status(collection, algo=algo)
    return jsonify({"results": results})

@similarity_bp.route("/api/similarity/build", methods=["POST"])
def build_similarity():
    # ... (remains as is)
    """Enqueues a similarity build job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    algo = data.get("algo", "unweighted_cosine")
    
    if not md5 and not batch_uuid:
        return jsonify({"error": "md5 or batch required"}), 400
    
    payload = {
        "collection": collection,
        "md5": md5,
        "batch_uuid": batch_uuid,
        "algo": algo,
        "min_score": data.get("min_score", 0.95),
        "top_k": data.get("top_k", 20)
    }
    
    job_id = job_service.create_job(JobType.BUILD_SIM, payload)
    return jsonify({"job_id": job_id, "status": "enqueued"})

@similarity_bp.route("/api/similarity/rebuild", methods=["POST"])
def rebuild_similarity():
    """Enqueues a clear + build pipeline."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    algo = data.get("algo", "unweighted_cosine")
    
    if not md5 and not batch_uuid:
        return jsonify({"error": "md5 or batch required"}), 400
    
    tasks = [
        (JobType.CLEAR_SIM, {"collection": collection, "md5": md5, "batch_uuid": batch_uuid, "algo": algo}),
        (JobType.BUILD_SIM, {
            "collection": collection, 
            "md5": md5, 
            "batch_uuid": batch_uuid, 
            "algo": algo,
            "min_score": data.get("min_score", 0.95),
            "top_k": data.get("top_k", 20)
        })
    ]
    
    pipeline_id = job_service.create_pipeline(tasks)
    return jsonify({"pipeline_id": pipeline_id, "status": "enqueued"})

@similarity_bp.route("/api/similarity/clear", methods=["POST"])
def clear_similarity():
    """Enqueues a similarity clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    algo = data.get("algo", "unweighted_cosine")
    
    if not md5 and not batch_uuid:
        return jsonify({"error": "md5 or batch required"}), 400

    job_id = job_service.create_job(JobType.CLEAR_SIM, {
        "collection": collection, "md5": md5, "batch_uuid": batch_uuid, "algo": algo
    })
    return jsonify({"job_id": job_id, "status": "enqueued"})
