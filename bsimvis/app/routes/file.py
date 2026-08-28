from flask import request
import json
import hashlib
from bsimvis.app.services import archive_service, lineage_service, unpack_service
from bsimvis.app.services.index_service import normalize_tags, save_file
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.milvus_service import milvus_service
from bsimvis.app.services.metadata_service import stage_metadata, staged_metadata
from bsimvis.app.services.processing_service import ProcessingService
from bsimvis.app.services.config_service import config_service
import logging
import time
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

        if enqueue:
            pipeline_id = job_service.submit_to_lane(collection, pipeline_tasks)
        else:
            pipeline_id = job_service.create_pipeline(pipeline_tasks, enqueue=False)

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
        if not file_md5:
            return {"error": "Missing file_md5 in chunk"}, 400

        r_data = get_redis()
        meta_store_key = f"{collection}:file:{file_md5}:chunk_meta"
        chunk_jobs_key = f"{collection}:file:{file_md5}:chunk_jobs"
        features_counter_key = f"{collection}:file:{file_md5}:total_features"
        functions_counter_key = f"{collection}:file:{file_md5}:total_functions"

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
            # enqueue=False defers enqueueing so we can push as a continuation.
            chunk_job_id = job_service.create_job(
                JobType.INDEX_FUNCTIONS, job_payload, enqueue=False
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

            pipeline_id = job_service.submit_to_lane(collection, pipeline_tasks)
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


def _ingest_raw_binary(
    raw_bytes,
    file_name,
    collection,
    batch_uuid,
    batch_name,
    parent_md5=None,
    parent_file_name=None,
    root_md5=None,
    extra_tags=(),
):
    """Stores one binary and queues its analysis job.

    Returns the per-file result dict, or ({"error": ...}, status) on failure.
    Shared by plain uploads and by every binary unpacking produced.
    """
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

    # Default enqueue to true unless explicitly disabled. Computed early: an
    # enqueue=false upload (used for unpacked container members that never
    # reach a worker) must NOT get a document registered here either, or it
    # stops being distinguishable from an actually-processed file -- see the
    # lineage "child with no document yet" test.
    enqueue_arg = request.args.get("enqueue")
    if enqueue_arg is not None:
        enqueue = enqueue_arg.lower() == "true"
    else:
        enqueue = True

    # Register a pending stub immediately so the file shows up (with a status)
    # in searches/listings right away, instead of only after the whole
    # analysis+indexing pipeline finishes. index_metadata overwrites this with
    # the full record once GHIDRA_ANALYZE completes. Skipped when enqueue is
    # false: those files (unpacked container members) never reach a worker,
    # so they must stay invisible/non-existent, same as before this stub
    # existed.
    if enqueue:
        file_base_id = f"{collection}:file:{file_md5}"
        stub_meta = {
            "file_md5": file_md5,
            "file_name": file_name,
            "batch_uuid": batch_uuid,
            "batch_name": batch_name,
            "collection": collection,
            "type": "file",
            "file_id": file_base_id,
            "status": "pending",
            "function_count": 0,
            "bsim_features_count": 0,
            "entry_date": int(time.time()),
        }
        stub_pipe = r_data.pipeline(transaction=False)
        stub_pipe.set(f"{file_base_id}:meta", json.dumps(stub_meta))
        save_file(stub_pipe, collection, file_md5, stub_meta)
        stub_pipe.execute()

        # Same idea for the batch: register it (if new) so it shows up in
        # /api/batch/search right away. Per-item state doesn't apply here --
        # the existing job-status badge covers "is something running for
        # this batch".
        global_batch_key = f"global:batch:{batch_uuid}"
        if not r_data.exists(global_batch_key):
            now_ms = int(time.time() * 1000)
            r_data.sadd("global:batches", batch_uuid)
            r_data.set(
                global_batch_key,
                json.dumps(
                    {
                        "name": batch_name,
                        "batch_uuid": batch_uuid,
                        "batch_id": global_batch_key,
                        "created_at": now_ms,
                        "last_updated": now_ms,
                        "collections": {collection: True},
                    }
                ),
            )
        batch_key = f"{collection}:batch:{batch_uuid}"
        if not r_data.exists(batch_key):
            now_ms = int(time.time() * 1000)
            r_data.sadd(f"{collection}:all_batches", batch_uuid)
            r_data.set(
                batch_key,
                json.dumps(
                    {
                        "name": batch_name,
                        "batch_uuid": batch_uuid,
                        "batch_id": batch_key,
                        "created_at": now_ms,
                        "last_updated": now_ms,
                        "total_files": 0,
                        "total_functions": 0,
                        "collection": collection,
                    }
                ),
            )

    # Build analysis payload
    analysis_payload = {
        "collection": collection,
        "raw_file_id": raw_file_id,
        "file_md5": file_md5,
        "file_name": file_name,
        "batch_uuid": batch_uuid,
        "batch_name": batch_name,
        "tags": request.args.getlist("tags") + list(extra_tags),
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

    # Analysis modules default to `[analysis_modules].enabled` in the instance
    # config (doc/bench-fid-cost.md has the per-module cost that motivated
    # making them opt-in in the first place). A request can widen or narrow
    # that default per-upload via `enable`/`disable` without touching the
    # config. The worker still speaks `skip_*`, so the inversion happens here
    # and nowhere else.
    from bsimvis.app.services.config_service import config_service

    enabled = set(config_service.get("analysis_modules.enabled", []))
    enabled |= set(request.args.getlist("enable"))
    enabled -= set(request.args.getlist("disable"))
    analysis_payload["skip_function_id"] = "FunctionID" not in enabled
    analysis_payload["skip_capa"] = "capa" not in enabled
    analysis_payload["skip_yara"] = "yara" not in enabled
    analysis_payload["skip_rulezet"] = "rulezet" not in enabled

    extra_meta = {}
    if "file_metadata_extra" in request.args:
        extra_meta = json.loads(request.args.get("file_metadata_extra"))
        if parent_md5:
            # `upload --metadata` matched that CSV row against the md5 of the
            # *upload*, so on an unpacked child it is inherited, not matched.
            # Its facts still describe the sample; its name does not -- without
            # this every member of an archive is stored under the container's
            # name and they become indistinguishable.
            extra_meta.pop("file_name", None)

    # This blob's own CSV row, if the batch staged one. Unpacking happens here,
    # so a member's md5 is only knowable now -- an exact match beats whatever
    # was inherited from the container, name included, because it was matched
    # against this file rather than the thing it arrived in.
    own_meta = staged_metadata(batch_uuid, file_md5)
    if own_meta:
        extra_meta.update(own_meta)
        if "file_name" in own_meta:
            file_name = own_meta["file_name"]
            analysis_payload["file_name"] = file_name

    # parent_md5 / parent_file_name are already declared index fields at the
    # file, func and sim levels, so lineage rides the existing metadata merge
    # in ghidra_job._index_streamed_program -- no schema change needed.
    if parent_md5:
        extra_meta["parent_md5"] = parent_md5
        extra_meta["parent_file_name"] = parent_file_name
        extra_meta["path_in_parent"] = file_name
    if root_md5:
        extra_meta["root_md5"] = root_md5
    if extra_meta:
        analysis_payload["file_metadata_extra"] = extra_meta

    priority = request.args.get("priority", "").lower() == "high"
    if priority:
        analysis_payload["priority"] = "high"

    job_id = job_service.create_job(
        JobType.GHIDRA_ANALYZE, analysis_payload, enqueue=enqueue
    )
    if enqueue:
        job_service.open_or_extend_wave(
            collection,
            job_id,
            config_service.get("clustering.idle_debounce_seconds", 30),
        )

    return {
        "status": "processing" if enqueue else "queued",
        "file_md5": file_md5,
        "file_name": file_name,
        "pipeline_id": job_id,
        "batch_uuid": batch_uuid,
        "message": "Binary uploaded. Analysis started.",
    }


def _ingest_container(
    file_md5,
    raw_bytes,
    file_name,
    collection,
    batch_uuid,
    batch_name,
    handler,
    tags,
    parent_md5,
    parent_file_name,
    root_md5,
):
    """Give a container an identity-only file document (issue #32 section 2).

    An APK or a zip is not code: it gets a document holding what it is, so the
    lineage links its children carry resolve to something and it can be found
    by name, but no functions and no similarity document. Its function count is
    the rolled-up count of everything below it, restated by lineage_service as
    each child finishes indexing.
    """
    r_data = get_redis()
    if r_data.sismember(f"{collection}:all_files", f"{collection}:file:{file_md5}"):
        lineage_service.mark_container(collection, file_md5, r_data)
        return  # re-uploaded container: the edges below are refreshed regardless

    now_unix = int(time.time() * 1000)
    file_meta = {
        "entry_date": now_unix,
        "file_date": now_unix,
        "file_md5": file_md5,
        "file_name": file_name,
        "batch_uuid": batch_uuid,
        "batch_name": batch_name,
        "tags": list(tags),
        "filetype": handler.name,
        "file_size": len(raw_bytes),
        "is_container": True,
        "language_id": "",
    }
    if parent_md5:
        file_meta["parent_md5"] = parent_md5
        file_meta["parent_file_name"] = parent_file_name
    if root_md5:
        file_meta["root_md5"] = root_md5

    # ponytail: the container's own bytes are not stored. Nothing reads them --
    # its children are already extracted -- and keeping them doubles the corpus
    # on disk. Store them here if re-extraction without a re-upload is wanted.
    ProcessingService().index_metadata(
        collection, None, file_meta=file_meta, num_functions=0, total_features=0
    )
    lineage_service.mark_container(collection, file_md5, r_data)


def _ingest_tree(
    raw_bytes,
    file_name,
    collection,
    batch_uuid,
    batch_name,
    parent_md5=None,
    parent_file_name=None,
    path_in_parent="",
    root_md5=None,
    inherited_tags=(),
    depth=0,
):
    """Ingest one upload plus everything unpacking it produces.

    Returns (results, errors). Which files get analyzed is decided by the
    handler that matched (see unpack_service): a packed executable is analyzed
    both packed and unpacked, a container only through its children.
    """
    options = {
        "password": request.args.get(
            "archive_password", archive_service.DEFAULT_PASSWORD
        )
    }

    handler, children = None, []
    if (
        request.args.get("unpack", "true").lower() != "false"
        and depth < unpack_service.MAX_DEPTH
    ):
        try:
            handler, children = unpack_service.unpack(raw_bytes, file_name, options)
        except unpack_service.UnpackError as e:
            handler = unpack_service.find_handler(raw_bytes, file_name)
            if handler is None or not handler.parent_is_code:
                # A container that will not open yields nothing at all.
                return [], [
                    {"file_name": file_name, "error": f"Could not extract: {e}"}
                ]
            # A packed binary that will not unpack is still a real sample, and
            # the detector is a heuristic that may simply have been wrong.
            logging.warning(f"[-] {file_name}: {handler.name} unpack failed: {e}")
            handler, children = None, []

    tags = list(inherited_tags) + ([handler.tag] if handler else [])
    self_md5 = hashlib.md5(raw_bytes).hexdigest()

    # A declared parent is an edge too, even when we unpacked nothing ourselves:
    # it is how out-of-band unpacking gets its lineage in.
    if parent_md5:
        lineage_service.record(
            collection, parent_md5, self_md5, path_in_parent or file_name
        )

    results, errors = [], []
    if handler is None or handler.parent_is_code:
        outcome = _ingest_raw_binary(
            raw_bytes,
            file_name,
            collection,
            batch_uuid,
            batch_name,
            parent_md5=parent_md5,
            parent_file_name=parent_file_name,
            root_md5=root_md5,
            extra_tags=tags,
        )
        if isinstance(outcome, tuple):
            errors.append({"file_name": file_name, **outcome[0]})
        else:
            results.append(outcome)
    else:
        _ingest_container(
            self_md5,
            raw_bytes,
            file_name,
            collection,
            batch_uuid,
            batch_name,
            handler,
            tags,
            parent_md5,
            parent_file_name,
            root_md5,
        )

    for child_name, child_bytes in children:
        sub_results, sub_errors = _ingest_tree(
            child_bytes,
            child_name,
            collection,
            batch_uuid,
            batch_name,
            parent_md5=self_md5,
            parent_file_name=file_name,
            path_in_parent=child_name,
            # The root is the upload itself, so every descendant answers
            # "everything under this upload" with one indexed field.
            root_md5=root_md5 or self_md5,
            inherited_tags=tags,
            depth=depth + 1,
        )
        results.extend(sub_results)
        errors.extend(sub_errors)

    return results, errors


def upload_raw_binary():
    """
    Receives a raw binary file, an archive (zip/tar/APK) of binaries, or a
    packed executable. Stores each resulting binary in Kvrocks and triggers its
    Ghidra analysis job.
    """
    try:
        logging.info(f"[*] Raw upload request received. Args: {request.args}")
        raw_bytes = request.get_data()
        if not raw_bytes:
            logging.warning("[-] No data provided in raw upload request")
            return {"error": "No data provided"}, 400

        logging.info(f"[*] Received {len(raw_bytes)} bytes for raw upload")

        # Reject a bad language/cspec pair here: otherwise it only fails deep
        # inside the Ghidra import, after the job has been queued.
        from bsimvis.app.services.ghidra_lang_service import validate as validate_lang

        lang_error = validate_lang(
            request.args.get("processor"), request.args.get("cspec")
        )
        if lang_error:
            return {"error": lang_error}, 400

        # Get metadata from headers or query params
        collection = request.args.get("collection", "main")
        file_name = request.args.get("file_name", "unknown")
        batch_uuid = request.args.get("batch_uuid")
        # If client did not provide a batch UUID, generate one server‑side so that all files uploaded in this request share the same identifier
        if not batch_uuid:
            batch_uuid = uuid.uuid4().hex
        batch_name = request.args.get("batch_name", "Ghidra Batch")

        # A declared parent (issue #32: users who unpack out-of-band with their
        # own tooling) is honoured whether or not we unpack anything ourselves.
        results, errors = _ingest_tree(
            raw_bytes,
            file_name,
            collection,
            batch_uuid,
            batch_name,
            parent_md5=request.args.get("parent_md5"),
            parent_file_name=request.args.get("parent_file_name"),
            path_in_parent=request.args.get("path_in_parent", ""),
        )

        # A plain binary keeps the flat single-file response it always had.
        # Keyed on the md5, not the name: a staged CSV row may rename the
        # upload, and that must not turn it into an archive response.
        if (
            len(results) == 1
            and not errors
            and results[0]["file_md5"] == hashlib.md5(raw_bytes).hexdigest()
        ):
            return results[0]

        if not results:
            if len(errors) == 1:
                return {"error": errors[0]["error"]}, 400
            return {
                "error": "No file in this upload could be queued",
                "errors": errors,
            }, 400

        return {
            "status": results[0]["status"],
            "archive": file_name,
            "batch_uuid": batch_uuid,
            "file_count": len(results),
            "files": results,
            "errors": errors,
            "pipeline_id": results[0]["pipeline_id"],
            "pipeline_ids": [r["pipeline_id"] for r in results],
            "message": f"Unpacked: {len(results)} binaries queued"
            + (f", {len(errors)} skipped" if errors else ""),
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

    # 1. Clear old results in parallel before rebuilding.
    # Function clustering (CLUSTER_FUNCTIONS below) is NOT cleared here: under
    # the default threshold_uf engine it updates incrementally in place, keyed
    # off this batch_uuid, instead of wiping and rebuilding every cluster in
    # the collection on every single upload.
    clear_tasks = []

    master_tasks = [group_id]
    if clear_tasks:
        master_tasks.append(job_service.create_group(clear_tasks, enqueue=False))

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

    priority = str(data.get("priority", "")).lower() == "high"
    master_id = job_service.submit_to_lane(collection, master_tasks, priority=priority)

    return {
        "status": "queued",
        "master_pipeline_id": master_id,
        "batch_uuid": batch_uuid,
    }


def _lineage_nodes(collection, edges, r):
    """Resolve lineage edges into displayable nodes.

    `exists` is what lets the UI stay honest about a container it was told
    about but never given -- a declared parent, or anything ingested before
    containers got documents of their own.
    """
    if not edges:
        return []
    pipe = r.pipeline(transaction=False)
    for edge in edges:
        pipe.get(f"{collection}:file:{edge['md5']}:meta")
        # Lets a tree row know whether it expands without a round trip per node.
        # SMEMBERS, not SCARD: the set holds more than one spelling per edge.
        pipe.smembers(f"{collection}:lineage:children:{edge['md5']}")
    containers = lineage_service.container_md5s(collection, r)

    results = pipe.execute()
    nodes = []
    for i, edge in enumerate(edges):
        raw = results[2 * i]
        child_count = lineage_service.count_members(results[2 * i + 1])
        meta = {}
        if raw:
            try:
                meta = json.loads(raw)
            except (ValueError, TypeError):
                meta = {}
        # The whole stored document, not a hand-picked subset: a lineage row is
        # rendered by the same row renderer as a search hit, so it needs the
        # same fields (language, yara/avtype/filetype, batch, dates, tags...).
        # ponytail: bin_clusters live in their own set and are left out — the
        # cluster cell stays empty on lineage rows until someone needs it.
        node = dict(meta)
        node.update(
            {
                "file_md5": edge["md5"],
                "file_id": f"{collection}:file:{edge['md5']}",
                "collection": collection,
                "path_in_parent": edge["path"],
                # Falling back to the path keeps a dangling node labelled with
                # something a human recognises instead of a bare hash.
                "file_name": meta.get("file_name") or edge["path"] or edge["md5"],
                "exists": bool(raw),
                "is_container": edge["md5"] in containers,
                "child_count": child_count or 0,
                "function_count": meta.get("function_count", 0),
                "filetype": meta.get("filetype", ""),
            }
        )
        # `tags` is a comma-separated string on older documents; the tag
        # renderer only understands lists.
        normalize_tags(node)
        nodes.append(node)
    return nodes


def get_file_lineage(file_md5):
    """Returns the containment lineage of one file: ancestors and children.

    `ancestors` is ordered nearest-first, so a breadcrumb is that list reversed
    plus the file itself.
    """
    try:
        collection = request.args.get("collection", "main")
        r = get_redis()

        raw = r.get(f"{collection}:file:{file_md5}:meta")
        meta = json.loads(raw) if raw else {}

        children_nodes = _lineage_nodes(
            collection, lineage_service.children(collection, file_md5, r), r
        )
        descendants = lineage_service.descendants(collection, file_md5, r)

        return {
            "collection": collection,
            "file": {
                "file_md5": file_md5,
                "file_name": meta.get("file_name", ""),
                "exists": bool(raw),
                "is_container": lineage_service.is_container(collection, file_md5, r),
                "function_count": meta.get("function_count", 0),
                "root_md5": meta.get("root_md5", ""),
            },
            "parents": _lineage_nodes(
                collection, lineage_service.parents(collection, file_md5, r), r
            ),
            "ancestors": _lineage_nodes(
                collection, lineage_service.ancestors(collection, file_md5, r), r
            ),
            "children": children_nodes,
            "child_count": len(children_nodes),
            "descendant_count": len(descendants),
        }

    except Exception as e:
        logging.error(f"Failed to fetch lineage for {file_md5}: {e}")
        return {"error": str(e)}, 500


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


def stage_batch_metadata():
    """
    Stages a batch's md5 -> metadata map so the ingest path can resolve each
    binary -- including ones that only exist after server-side unpacking -- by
    its own hash rather than the uploaded container's.
    """
    try:
        data = request.json or {}
        batch_uuid = data.get("batch_uuid")
        updates = data.get("updates", {})

        if not batch_uuid:
            return {"error": "Missing batch_uuid"}, 400
        if not isinstance(updates, dict) or not updates:
            return {"error": "Missing updates mapping"}, 400

        count = stage_metadata(batch_uuid, updates)
        return {
            "status": "ok",
            "batch_uuid": batch_uuid,
            "staged": count,
            "message": f"Staged metadata for {count} hashes.",
        }

    except Exception as e:
        logging.error(f"Failed to stage batch metadata: {e}")
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
