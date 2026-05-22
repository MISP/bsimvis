from flask import Blueprint, jsonify, request
import json
import hashlib
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.milvus_service import milvus_service
import logging

file_bp = Blueprint("file", __name__)
job_service = JobService()


@file_bp.route("/api/file/upload/file_data", methods=["POST"])
def upload_file_data():
    """
    Receives JSON data from local Ghidra analysis.
    Stores it in Kvrocks and triggers the processing pipeline.
    """
    try:
        # Get raw bytes to avoid double parsing/serialization
        raw_bytes = request.get_data()
        if not raw_bytes:
            return jsonify({"error": "No data provided"}), 400

        # Parse once to get meta info
        # Note: For very huge files, this still holds the dict in memory,
        # but we avoid multiple json.dumps() later.
        data = json.loads(raw_bytes)
        collection = data.get("collection", "main")
        file_md5 = data.get("file_md5")

        if not file_md5:
            # Efficient MD5 from raw bytes instead of re-serializing the dict
            file_md5 = hashlib.md5(raw_bytes).hexdigest()

        # 1. Store in Kvrocks (JSON.SET)
        r_data = get_redis()
        file_id = f"idx:{collection}:file:{file_md5}"

        # Store as a standard string (SET) instead of a JSON object.
        # This is much faster and avoids server-side parsing of large files.
        r_data.set(file_id, raw_bytes)

        build_sim_payload = {
            "collection": collection,
            "file_id": file_id,
            "md5": file_md5,
            "algo": "unweighted_cosine",
        }
        if "top_k" in data:
            build_sim_payload["top_k"] = data["top_k"]
        if "min_score" in data:
            build_sim_payload["min_score"] = data["min_score"]
        if "min_features" in data:
            build_sim_payload["min_features"] = data["min_features"]
        if "algo" in data:
            build_sim_payload["algo"] = data["algo"]

        if build_sim_payload.get("algo") == "milvus_sparse" and not milvus_service.enabled:
            return jsonify({"error": "Milvus is disabled. Cannot use milvus_sparse algorithm."}), 400

        skip_sim = data.get("skip_sim", False)

        # 2. Trigger Pipeline
        # Steps: Meta indexing, Function indexing, Feature indexing, Sim bake
        pipeline_tasks = [
            (
                JobType.INDEX_META,
                {"collection": collection, "file_id": file_id, "md5": file_md5},
            ),
            (
                JobType.INDEX_FUNCTIONS,
                {"collection": collection, "file_id": file_id, "md5": file_md5},
            ),
            (
                JobType.INDEX_FEATURES,
                {"collection": collection, "file_id": file_id, "md5": file_md5},
            ),
        ]

        if not skip_sim:
            if milvus_service.enabled and build_sim_payload.get("algo") == "milvus_sparse":
                pipeline_tasks.append((JobType.SYNC_MILVUS, {"collection": collection}))
            pipeline_tasks.append((JobType.BUILD_SIM, build_sim_payload))

        pipeline_id = job_service.create_pipeline(pipeline_tasks)

        return jsonify(
            {
                "status": "processing",
                "file_id": file_id,
                "pipeline_id": pipeline_id,
                "message": "Data stored. Processing pipeline started.",
            }
        )

    except Exception as e:
        import traceback

        logging.error(f"Upload failed: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e), "detail": traceback.format_exc()}), 500
