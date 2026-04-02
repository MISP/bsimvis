from flask import Blueprint, jsonify, request
import json
import hashlib
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.job_service import JobService, JobType

file_bp = Blueprint("file", __name__)
job_service = JobService()

@file_bp.route("/api/file/upload/file_data", methods=["POST"])
def upload_file_data():
    """
    Receives JSON data from local Ghidra analysis.
    Stores it in Kvrocks and triggers the processing pipeline.
    """
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Extract basic info
        collection = data.get("collection", "main")
        file_md5 = data.get("file_md5")
        if not file_md5:
            # Fallback MD5 if not provided
            content = json.dumps(data, sort_keys=True).encode()
            file_md5 = hashlib.md5(content).hexdigest()
        
        # 1. Store in Kvrocks (JSON.SET)
        r_data = get_redis()
        file_id = f"{collection}:file:{file_md5}"
        r_data.json().set(file_id, "$", data)
        
        # 2. Trigger Pipeline
        # Steps: Meta indexing, Function indexing, Feature indexing, Sim bake
        pipeline_tasks = [
            (JobType.INDEX_META, {"collection": collection, "file_id": file_id}),
            (JobType.INDEX_FUNCTIONS, {"collection": collection, "file_id": file_id}),
            (JobType.INDEX_FEATURES, {"collection": collection, "file_id": file_id}),
            (JobType.BUILD_SIM, {"collection": collection, "file_id": file_id, "algo": "unweighted_cosine"}),
        ]
        
        pipeline_id = job_service.create_pipeline(pipeline_tasks)
        
        return jsonify({
            "status": "processing",
            "file_id": file_id,
            "pipeline_id": pipeline_id,
            "message": "Data stored. Processing pipeline started."
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
