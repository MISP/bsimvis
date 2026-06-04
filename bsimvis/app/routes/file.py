from flask import request
import json
import hashlib
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.milvus_service import milvus_service
import logging
import uuid

job_service = JobService()


def upload_file_data():
    """
    Receives JSON data from local Ghidra analysis.
    Stores it in Kvrocks and triggers the processing pipeline.
    """
    try:
        # Get raw bytes to avoid double parsing/serialization
        raw_bytes = request.get_data()
        if not raw_bytes:
            return {"error": "No data provided"}, 400

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
        file_id = f"{collection}:file:{file_md5}:data"

        # Store as a standard string (SET) instead of a JSON object.
        # This is much faster and avoids server-side parsing of large files.
        r_data.set(file_id, raw_bytes)

        from bsimvis.app.services.config_service import config_service

        algo = data.get("algo")
        if algo is None:
            algo = config_service.get("similarity.algo", "unweighted_cosine")

        top_k = data.get("top_k")
        if top_k is None:
            top_k = config_service.get("similarity.top_k", 1000)

        min_score = data.get("min_score")
        if min_score is None:
            min_score = config_service.get("similarity.min_score", 0.3)

        min_features = data.get("min_features")
        if min_features is None:
            min_features = config_service.get("similarity.min_features", 0)

        build_sim_payload = {
            "collection": collection,
            "file_id": file_id,
            "md5": file_md5,
            "algo": algo,
            "top_k": top_k,
            "min_score": min_score,
            "min_features": min_features,
        }

        if (
            build_sim_payload.get("algo") == "milvus_sparse"
            and not milvus_service.enabled
        ):
            return {
                "error": "Milvus is disabled. Cannot use milvus_sparse algorithm."
            }, 400

        skip_sim = data.get("skip_sim", False)

        enqueue_val = request.args.get("enqueue")
        if enqueue_val is not None:
            enqueue = enqueue_val.lower() == "true"
        else:
            enqueue = data.get("enqueue", True)
            if isinstance(enqueue, str):
                enqueue = enqueue.lower() == "true"

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
            if (
                milvus_service.enabled
                and build_sim_payload.get("algo") == "milvus_sparse"
            ):
                pipeline_tasks.append((JobType.SYNC_MILVUS, {"collection": collection}))
            pipeline_tasks.append((JobType.BUILD_SIM, build_sim_payload))

        if enqueue:
            pipeline_tasks.append((JobType.ENRICH_FEATURES, {"collection": collection}))

        pipeline_id = job_service.create_pipeline(pipeline_tasks, enqueue=enqueue)

        return {
            "status": "processing" if enqueue else "queued",
            "file_id": file_id,
            "pipeline_id": pipeline_id,
            "message": (
                "Data stored. Processing pipeline started."
                if enqueue
                else "Data stored. Pipeline queued."
            ),
        }

    except Exception as e:
        import traceback

        logging.error(f"Upload failed: {str(e)}")
        logging.error(traceback.format_exc())
        return {"error": str(e), "detail": traceback.format_exc()}, 500


def upload_raw_binary():
    """
    Receives a raw binary file.
    Stores it in Kvrocks and triggers the Ghidra analysis job.
    """
    try:
        logging.info(f"[*] Raw upload request received. Args: {request.args}")
        raw_bytes = request.get_data()
        if not raw_bytes:
            logging.warning("[-] No data provided in raw upload request")
            return {"error": "No data provided"}, 400

        logging.info(f"[*] Received {len(raw_bytes)} bytes for raw upload")

        # Get metadata from headers or query params
        collection = request.args.get("collection", "main")
        file_name = request.args.get("file_name", "unknown")
        batch_uuid = request.args.get("batch_uuid")
        # If client did not provide a batch UUID, generate one server‑side so that all files uploaded in this request share the same identifier
        if not batch_uuid:
            batch_uuid = uuid.uuid4().hex
        batch_name = request.args.get("batch_name", "Ghidra Batch")

        # Compute MD5
        file_md5 = hashlib.md5(raw_bytes).hexdigest()

        # Store raw binary in Kvrocks
        r_data = get_redis()
        raw_file_id = f"{collection}:file:{file_md5}:raw"
        r_data.set(raw_file_id, raw_bytes)

        # Build analysis payload
        analysis_payload = {
            "collection": collection,
            "raw_file_id": raw_file_id,
            "file_md5": file_md5,
            "file_name": file_name,
            "batch_uuid": batch_uuid,
            "batch_name": batch_name,
            "tags": request.args.getlist("tags"),
            "profile": request.args.get("profile", "fast"),
            "min_func_len": int(request.args.get("min_func_len", 10)),
        }

        # Mirror compiler/processor options if provided
        for opt in ["processor", "cspec", "algo"]:
            if opt in request.args:
                analysis_payload[opt] = request.args.get(opt)

        if "top_k" in request.args:
            analysis_payload["top_k"] = int(request.args.get("top_k"))
        if "min_score" in request.args:
            analysis_payload["min_score"] = float(request.args.get("min_score"))
        if "min_features" in request.args:
            analysis_payload["min_features"] = int(request.args.get("min_features"))
        if "skip_sim" in request.args:
            val = request.args.get("skip_sim")
            analysis_payload["skip_sim"] = (
                val.lower() in ("true", "1") if isinstance(val, str) else bool(val)
            )

        # Trigger Pipeline: Analysis -> Indexing -> Similarity
        pipeline_tasks = [(JobType.GHIDRA_ANALYZE, analysis_payload)]

        enqueue = request.args.get("enqueue", "true").lower() == "true"
        pipeline_id = job_service.create_pipeline(pipeline_tasks, enqueue=enqueue)

        return {
            "status": "processing" if enqueue else "queued",
            "file_md5": file_md5,
            "pipeline_id": pipeline_id,
            "batch_uuid": batch_uuid,
            "message": "Binary uploaded. Analysis pipeline started.",
        }

    except Exception as e:
        import traceback

        logging.error(f"Raw upload failed: {str(e)}")
        return {"error": str(e), "detail": traceback.format_exc()}, 500


def finalize_batch_upload():
    """
    Finalizes a batch upload by wrapping all file pipelines in a group,
    and appending clustering/bin_sim at the end.
    """
    data = request.json
    if not data:
        return {"error": "Missing JSON payload"}, 400

    pipeline_ids = data.get("pipeline_ids", [])
    batch_uuid = data.get("batch_uuid")
    collection = data.get("collection", "main")
    algo = data.get("algo", "unweighted_cosine")
    skip_sim = data.get("skip_sim", False)

    if not pipeline_ids:
        return {"error": "No pipelines provided"}, 400

    group_id = job_service.create_group(pipeline_ids, enqueue=False)

    master_tasks = [group_id]

    # After the group finishes, we do clustering:
    master_tasks.append(
        (
            JobType.CLUSTER_FUNCTIONS.value,
            {"collection": collection, "algo": algo, "batch_uuid": batch_uuid},
        )
    )

    # After clustering, we do binary similarity:
    if not skip_sim:
        master_tasks.append(
            (JobType.CLEAR_BIN_CLUSTER.value, {"collection": collection, "algo": algo})
        )
        master_tasks.append(
            (
                JobType.BUILD_BIN_SIM.value,
                {"collection": collection, "algo": algo, "batch_uuid": batch_uuid},
            )
        )
        master_tasks.append(
            (JobType.CLUSTER_BINARIES.value, {"collection": collection, "algo": algo})
        )

    # Enrich features must be the absolute last job to run:
    master_tasks.append(
        (
            JobType.ENRICH_FEATURES.value,
            {"collection": collection, "batch_uuid": batch_uuid},
        )
    )

    master_id = job_service.create_pipeline(master_tasks, enqueue=True)

    return {
        "status": "success",
        "master_pipeline_id": master_id,
        "batch_uuid": batch_uuid,
    }
