from flask import Blueprint, request, jsonify
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.feature_service import FeatureService

features_bp = Blueprint("features", __name__)
job_service = JobService()
feature_service = FeatureService()

@features_bp.route("/api/features/status", methods=["GET"])
def get_status():
    """Returns indexing status for a collection."""
    collection = request.args.get("collection", "main")
    batch_uuid = request.args.get("batch")
    md5 = request.args.get("md5")
    
    if request.args.get("details") == "true":
        results = feature_service.list_batches_status(collection, batch_filter=batch_uuid)
        return jsonify({"results": results})
    
    status = feature_service.get_indexing_status(collection, batch_uuid=batch_uuid, file_md5=md5)
    return jsonify(status)

@features_bp.route("/api/features/files", methods=["GET"])
def get_file_status():
    """Returns indexing status for all files."""
    collection = request.args.get("collection", "main")
    results = feature_service.list_files_status(collection)
    return jsonify({"results": results})

@features_bp.route("/api/features/index", methods=["POST"])
def index_features():
    """Enqueues a feature indexing job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    
    if not md5 and not batch_uuid:
        return jsonify({"error": "md5 or batch required"}), 400
    
    payload = {
        "collection": collection,
        "md5": md5,
        "batch_uuid": batch_uuid
    }
    
    job_id = job_service.create_job(JobType.INDEX_FEATURES, payload)
    return jsonify({"job_id": job_id, "status": "enqueued"})

@features_bp.route("/api/features/clear", methods=["POST"])
def clear_features():
    """Enqueues a feature clear job."""
    data = request.json or {}
    collection = data.get("collection", "main")
    md5 = data.get("md5")
    batch_uuid = data.get("batch")
    
    payload = {
        "collection": collection,
        "md5": md5,
        "batch_uuid": batch_uuid
    }
    
    job_id = job_service.create_job(JobType.CLEAR_FEATURES, payload)
    return jsonify({"job_id": job_id, "status": "enqueued"})
