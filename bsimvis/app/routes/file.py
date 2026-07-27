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
            file_meta = data.get("file_metadata", {})
            file_md5 = file_meta.get("file_md5")

        if not file_md5:
            # Efficient MD5 from raw bytes instead of re-serializing the dict
            file_md5 = hashlib.md5(raw_bytes).hexdigest()

        # 1. Store in Kvrocks (JSON.SET)
        r_data = get_redis()

        # Check if file is already in the collection
        if r_data.sismember(f"{collection}:all_files", f"{collection}:file:{file_md5}"):
            return {
                "error": f"File with MD5 {file_md5} already exists in collection '{collection}'"
            }, 400

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
            min_score = config_service.get("similarity.min_score", 0.9)

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
            "skip_write": data.get("skip_write", False),  # ponytail
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
        batch_uuid = data.get("batch_uuid") or (data.get("file_metadata") or {}).get(
            "batch_uuid"
        )
        if enqueue_val is not None:
            enqueue = enqueue_val.lower() == "true"
        else:
            default_enqueue = False if batch_uuid else True
            enqueue = data.get("enqueue", default_enqueue)
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
            if not data.get("skip_write", False):
                pipeline_tasks.append(
                    (
                        JobType.INDEX_SIM,
                        {
                            "collection": collection,
                            "md5": file_md5,
                            "algo": build_sim_payload.get("algo"),
                        },
                    )
                )

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


def upload_chunk():
    """
    Receives a chunk of function data.
    Stores chunks in Redis and, on the final chunk, builds a single sequential pipeline:
    INDEX_META -> INDEX_FUNCTIONS (per chunk) -> INDEX_FEATURES -> BUILD_SIM -> INDEX_SIM.
    This guarantees BUILD_SIM never runs before all indexing is complete.
    """
    try:
        data = request.json
        if not data:
            return {"error": "No chunk data provided"}, 400

        collection = data.get("collection", "main")
        file_md5 = data.get("file_md5")
        chunk_index = data.get("chunk_index", 0)
        is_final = data.get("is_final", False)
        skip_sim = data.get("skip_sim", False)
        file_metadata = data.get("file_metadata")
        functions = data.get("functions", [])
        parent_job_id = data.get("parent_job_id")

        if not file_md5:
            return {"error": "Missing file_md5 in chunk"}, 400

        r_data = get_redis()
        r_queue = job_service.r  # Queue Redis - where job metadata lives

        parent_pipeline_id = None
        if parent_job_id:
            val = r_queue.hget(f"job:{parent_job_id}", "parent_id")
            if val:
                parent_pipeline_id = val.decode() if isinstance(val, bytes) else val

        suffix = f":{parent_job_id}" if parent_job_id else ""
        meta_store_key = f"{collection}:file:{file_md5}:chunk_meta{suffix}"
        chunk_jobs_key = f"{collection}:file:{file_md5}:chunk_jobs{suffix}"
        features_counter_key = f"{collection}:file:{file_md5}:total_features{suffix}"
        functions_counter_key = f"{collection}:file:{file_md5}:total_functions{suffix}"

        # 1. Save file metadata once (chunk 0)
        stored_meta = None
        if chunk_index == 0 and file_metadata:
            r_data.set(meta_store_key, json.dumps(file_metadata))
            stored_meta = file_metadata
        else:
            stored_meta_raw = r_data.get(meta_store_key)
            stored_meta = json.loads(stored_meta_raw) if stored_meta_raw else {}

        # 2. Immediately index functions for this chunk
        if functions:
            batch_uuid = stored_meta.get("batch_uuid") if stored_meta else None
            # ponytail: Offload heavy functions list to Kvrocks. Save job RAM.
            chunk_id = f"{collection}:file:{file_md5}:chunk_data:{chunk_index}"
            r_data.set(chunk_id, json.dumps(functions))

            job_payload = {
                "collection": collection,
                "chunk_id": chunk_id,
                "file_meta": stored_meta,
                "file_md5": file_md5,
                "batch_uuid": batch_uuid,
            }
            # is_subtask defers enqueueing so we can push as a continuation:
            # chunk indexing lands on the tail of jobs:pending, which workers pop
            # first. Without this the chunk jobs queue up behind every
            # already-pending GHIDRA_ANALYZE, so a 30-file batch finishes all
            # analysis before a single function is navigable.
            chunk_job_id = job_service.create_job(
                JobType.INDEX_FUNCTIONS,
                job_payload,
                parent_id=parent_job_id,
                is_subtask=bool(parent_job_id),
            )
            job_service.enqueue_job(chunk_job_id, is_continuation=True)

            r_data.sadd(chunk_jobs_key, chunk_job_id)

            # Update feature/function counters
            chunk_features_count = sum(
                f.get("function_metadata", {}).get("bsim_features_count", 0)
                for f in functions
            )
            r_data.incrby(features_counter_key, chunk_features_count)
            r_data.incrby(functions_counter_key, len(functions))

        # 3. On final chunk, collect jobs and build pipeline
        if is_final:
            stored_meta_raw = r_data.get(meta_store_key)
            stored_meta = (
                json.loads(stored_meta_raw) if stored_meta_raw else file_metadata or {}
            )
            batch_uuid = stored_meta.get("batch_uuid")

            # Collect child job IDs
            chunk_jobs_bytes = r_data.smembers(chunk_jobs_key)
            chunk_jobs = [
                jid.decode() if isinstance(jid, bytes) else jid
                for jid in chunk_jobs_bytes
            ]

            total_features = int(r_data.get(features_counter_key) or 0)
            num_functions = int(r_data.get(functions_counter_key) or 0)

            # Clean up Redis keys
            r_data.delete(
                chunk_jobs_key,
                meta_store_key,
                features_counter_key,
                functions_counter_key,
            )

            from bsimvis.app.services.config_service import config_service

            algo = data.get("algo") or config_service.get(
                "similarity.algo", "unweighted_cosine"
            )
            top_k = data.get("top_k") or config_service.get("similarity.top_k", 1000)
            min_score = data.get("min_score") or config_service.get(
                "similarity.min_score", 0.9
            )
            min_features = data.get("min_features") or config_service.get(
                "similarity.min_features", 0
            )

            # Build strictly ordered pipeline
            pipeline_tasks = [
                (
                    JobType.INDEX_META,
                    {
                        "collection": collection,
                        "file_id": None,
                        "file_meta": stored_meta,
                        "num_functions": num_functions,
                        "total_features": total_features,
                    },
                )
            ]

            if chunk_jobs:
                # Group wraps existing jobs (enqueue=False since already active)
                group_id = job_service.create_group(chunk_jobs, enqueue=False)
                pipeline_tasks.append(group_id)

            pipeline_tasks.append(
                (JobType.INDEX_FEATURES, {"collection": collection, "md5": file_md5})
            )

            if not skip_sim:
                build_sim_payload = {
                    "collection": collection,
                    "file_id": None,
                    "md5": file_md5,
                    "algo": algo,
                    "top_k": top_k,
                    "min_score": min_score,
                    "min_features": min_features,
                    "skip_write": data.get("skip_write", False),  # ponytail
                }
                if algo == "milvus_sparse" and milvus_service.enabled:
                    pipeline_tasks.append(
                        (JobType.SYNC_MILVUS, {"collection": collection})
                    )
                pipeline_tasks.append((JobType.BUILD_SIM, build_sim_payload))
                if not data.get("skip_write", False):
                    pipeline_tasks.append(
                        (
                            JobType.INDEX_SIM,
                            {"collection": collection, "md5": file_md5, "algo": algo},
                        )
                    )

            if parent_pipeline_id:
                # Splice tasks into parent pipeline
                new_tids = [
                    job_service._resolve_task(task, parent_pipeline_id)
                    for task in pipeline_tasks
                ]

                pipe_data = r_queue.hgetall(f"job:{parent_pipeline_id}")
                if pipe_data and "task_ids" in pipe_data:
                    existing_tids = json.loads(pipe_data["task_ids"])
                    try:
                        idx = existing_tids.index(parent_job_id)
                        updated_tids = (
                            existing_tids[: idx + 1]
                            + new_tids
                            + existing_tids[idx + 1 :]
                        )
                    except ValueError:
                        updated_tids = existing_tids + new_tids
                    r_queue.hset(
                        f"job:{parent_pipeline_id}",
                        "task_ids",
                        json.dumps(updated_tids),
                    )
                    job_service.add_log(
                        parent_pipeline_id,
                        f"Spliced {len(new_tids)} ordered indexing tasks into pipeline.",
                    )
            else:
                pipeline_id = job_service.create_pipeline(pipeline_tasks, enqueue=True)
                return {
                    "status": "success",
                    "chunk_index": chunk_index,
                    "pipeline_id": pipeline_id,
                }

        return {"status": "success", "chunk_index": chunk_index}
    except Exception as e:
        import traceback

        logging.error(f"Chunk upload failed: {str(e)}")
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

        # Check if file is already in the collection
        r_data = get_redis()
        if r_data.sismember(f"{collection}:all_files", f"{collection}:file:{file_md5}"):
            return {
                "error": f"File with MD5 {file_md5} already exists in collection '{collection}'"
            }, 400

        # Store raw binary in Kvrocks
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
            "related_md5": request.args.getlist("related_md5"),
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

        if "file_metadata_extra" in request.args:
            analysis_payload["file_metadata_extra"] = json.loads(
                request.args.get("file_metadata_extra")
            )

        # Trigger Pipeline: Analysis -> Indexing -> Similarity
        pipeline_tasks = [(JobType.GHIDRA_ANALYZE, analysis_payload)]

        # Pre-register similarity jobs so the pipeline doesn't finish early
        is_gpr_zip = file_name.endswith(".gpr.zip")
        if not analysis_payload.get("skip_sim") and not is_gpr_zip:
            algo = analysis_payload.get("algo")
            build_sim_payload = {
                "collection": collection,
                "file_id": None,
                "md5": file_md5,
                "algo": algo,
                "top_k": analysis_payload.get("top_k"),
                "min_score": analysis_payload.get("min_score"),
                "min_features": analysis_payload.get("min_features"),
            }
            if algo == "milvus_sparse" and milvus_service.enabled:
                pipeline_tasks.append((JobType.SYNC_MILVUS, {"collection": collection}))
            pipeline_tasks.append((JobType.BUILD_SIM, build_sim_payload))
            pipeline_tasks.append(
                (
                    JobType.INDEX_SIM,
                    {"collection": collection, "md5": file_md5, "algo": algo},
                )
            )

        # Default enqueue to true unless explicitly disabled
        enqueue_arg = request.args.get("enqueue")
        if enqueue_arg is not None:
            enqueue = enqueue_arg.lower() == "true"
        else:
            enqueue = True
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
    min_cohesion = data.get("min_cohesion")

    if not pipeline_ids:
        return {"error": "No pipelines provided"}, 400

    group_id = job_service.create_group(pipeline_ids, enqueue=False)

    # 1. Clear old results in parallel before rebuilding
    clear_tasks = [
        (JobType.CLEAR_CLUSTER.value, {"collection": collection, "algo": algo}),
    ]
    if not skip_sim:
        clear_tasks.append(
            (JobType.CLEAR_BIN_SIM.value, {"collection": collection, "algo": algo})
        )
        clear_tasks.append(
            (JobType.CLEAR_BIN_CLUSTER.value, {"collection": collection, "algo": algo})
        )

    clear_group_id = job_service.create_group(clear_tasks, enqueue=False)

    master_tasks = [group_id, clear_group_id]

    # After the clears, we do clustering:
    master_tasks.append(
        (
            JobType.CLUSTER_FUNCTIONS.value,
            {"collection": collection, "algo": algo, "batch_uuid": batch_uuid},
        )
    )

    # After clustering, we do binary similarity:
    if not skip_sim:
        build_payload = {
            "collection": collection,
            "algo": algo,
            "batch_uuid": batch_uuid,
        }
        if min_cohesion is not None:
            build_payload["min_cohesion"] = min_cohesion
        master_tasks.append(
            (
                JobType.BUILD_BIN_SIM.value,
                build_payload,
            )
        )
        cluster_payload = {
            "collection": collection,
            "algo": algo,
        }
        if min_cohesion is not None:
            cluster_payload["min_cohesion"] = min_cohesion
        master_tasks.append((JobType.CLUSTER_BINARIES.value, cluster_payload))
        master_tasks.append(
            (
                JobType.INDEX_SIM.value,
                {
                    "collection": collection,
                    "algo": algo,
                    "batch_uuid": batch_uuid,
                },
            )
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


def update_file_metadata(file_md5):
    """
    Partially updates metadata for a single file and enqueues propagation.
    """
    try:
        data = request.json or {}
        collection = data.get("collection", "main")
        metadata = data.get("metadata", {})

        if not metadata:
            return {"error": "Missing metadata to update"}, 400

        payload = {"collection": collection, "updates": {file_md5: metadata}}

        job_id = job_service.create_job(JobType.PROPAGATE_METADATA, payload)

        return {
            "status": "processing",
            "job_id": job_id,
            "message": "Metadata propagation job enqueued.",
        }

    except Exception as e:
        logging.error(f"Failed to update file metadata: {e}")
        return {"error": str(e)}, 500


def bulk_propagate_metadata():
    """
    Updates metadata for multiple files in bulk and enqueues propagation.
    """
    try:
        data = request.json or {}
        collection = data.get("collection", "main")
        updates = data.get("updates", {})

        if not updates:
            return {"error": "Missing updates mapping"}, 400

        payload = {"collection": collection, "updates": updates}

        job_id = job_service.create_job(JobType.PROPAGATE_METADATA, payload)

        return {
            "status": "processing",
            "job_id": job_id,
            "message": "Bulk metadata propagation job enqueued.",
        }

    except Exception as e:
        logging.error(f"Failed bulk metadata propagation: {e}")
        return {"error": str(e)}, 500
