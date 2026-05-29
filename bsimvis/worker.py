import time
import json
import logging
import signal
import sys
import os
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from bsimvis.app.services.redis_client import get_queue_redis, get_redis, get_raw_redis
from bsimvis.app.services.job_service import JobService, JobStatus, JobType
from bsimvis.app.services.processing_service import ProcessingService
from bsimvis.app.services.feature_service import FeatureService
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.bin_sim_service import bin_sim_service
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.timer_service import job_timer
from bsimvis.app.services.ghidra_service import ghidra_service

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


class Worker:
    def __init__(self, name="worker-1"):
        self.name = name
        self.r_queue = get_queue_redis()
        self.r_data = get_redis()
        self.r_raw = get_raw_redis()
        self.job_service = JobService()
        self.processing_service = ProcessingService(self.r_data)
        self.feature_service = FeatureService(self.r_data)

        # Initialize Lua scripts for this process
        lua_manager.init_app()

        # Ensure Ghidra is ready
        ghidra_service.ensure_launcher()

        self.similarity_service = SimilarityService(self.r_data)
        self.running = True

    def stop(self, signum, frame):
        logging.info(f"[*] Worker {self.name} received stop signal...")
        self.running = False

    def run(self):
        logging.info(f"[*] Worker {self.name} started. Waiting for jobs...")

        while self.running:
            try:
                # Reliable Priority Queue Pattern
                # 1. First check High-Priority Queue (Non-blocking)
                job_id = self.r_queue.execute_command(
                    "LMOVE", "jobs:pending:high", "jobs:processing", "RIGHT", "LEFT"
                )

                # 2. If empty, fall back to Default Queue (Blocking for 2s)
                if not job_id:
                    job_id = self.r_queue.execute_command(
                        "BLMOVE", "jobs:pending", "jobs:processing", "RIGHT", "LEFT", 2
                    )

                if not job_id:
                    continue

                # Fetch job metadata
                job_id = job_id.decode() if isinstance(job_id, bytes) else job_id
                job_data = self.r_queue.hgetall(f"job:{job_id}")

                if not job_data:
                    logging.warning(f"[!] Job {job_id} metadata missing. Skipping.")
                    self.r_queue.lrem("jobs:processing", 1, job_id)
                    continue

                if job_data.get("status") == JobStatus.CANCELLED.value:
                    logging.info(f"[-] Job {job_id} was cancelled. Skipping.")
                    self.r_queue.lrem("jobs:processing", 1, job_id)
                    continue

                # Execute Job
                self._execute_job(job_id, job_data)

                # Success: Remove from processing
                self.r_queue.lrem("jobs:processing", 1, job_id)

            except Exception as e:
                logging.error(f"[!] Worker loop error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(1)

    def _execute_job(self, job_id, job_data):
        jtype = job_data.get("type")
        payload = json.loads(job_data.get("payload", "{}"))
        parent_id = job_data.get("parent_id")

        logging.info(f"[+] Executing Job {job_id} ({jtype})...")
        self.job_service.add_log(
            job_id, f"Worker {self.name} started processing {jtype}."
        )
        self.r_queue.hset(f"job:{job_id}", "status", JobStatus.RUNNING.value)

        # Execute Job within a timer context
        with job_timer(job_id) as timer:
            try:
                # Dispatch
                success = self._dispatch(jtype, payload, job_id)

                if success:
                    self.job_service.add_log(
                        job_id, f"Job {jtype} completed successfully."
                    )
                    self.job_service.update_progress(job_id, 100)
                    self.r_queue.hset(
                        f"job:{job_id}", "status", JobStatus.COMPLETED.value
                    )

                    # Pipeline Chaining: Check if we need to enqueue the next task
                    if parent_id:
                        self._handle_pipeline_next(parent_id, job_id)
                else:
                    self.r_queue.hset(f"job:{job_id}", "status", JobStatus.FAILED.value)
                    self.job_service.add_log(
                        job_id, "Job failed (returned False from dispatcher)."
                    )

            except Exception as e:
                logging.error(f"[!] Job {job_id} failed with error: {e}")
                import traceback

                traceback.print_exc()
                self.r_queue.hset(f"job:{job_id}", "status", JobStatus.FAILED.value)
                self.r_queue.hset(f"job:{job_id}", "error", str(e))
                self.job_service.add_log(job_id, f"Execution error: {e}")

                # If sub-task fails, mark parent pipeline as failed
                if parent_id:
                    self.r_queue.hset(
                        f"job:{parent_id}", "status", JobStatus.FAILED.value
                    )
                    self.job_service.add_log(
                        parent_id, f"Pipeline failed because sub-task {job_id} failed."
                    )
            finally:
                # Finalize and save performance stats
                stats = timer.finalize()
                self.job_service.save_performance_stats(job_id, stats)
                perf_summary = f"Perf: Total {stats['total_time']}s | Python {stats['python_time']}s | DB {stats['db_time']}s | Lua {stats['lua_time']}s"
                self.job_service.add_log(job_id, perf_summary)
                logging.info(f"[#] Job {job_id} {perf_summary}")

    def _dispatch(self, jtype, payload, job_id):
        """Dispatcher for background jobs."""
        collection = payload.get("collection", "main")
        file_id = payload.get("file_id")
        md5 = payload.get("md5")
        batch_uuid = payload.get("batch_uuid")

        if jtype == JobType.GHIDRA_ANALYZE.value:
            raw_file_id = payload.get("raw_file_id")
            file_md5 = payload.get("file_md5")

            # 1. Fetch raw binary from Kvrocks
            raw_bytes = self.r_raw.get(raw_file_id)
            if not raw_bytes:
                self.job_service.add_log(
                    job_id, f"Error: Raw file {raw_file_id} not found."
                )
                return False

            temp_dir = None
            temp_path = None
            try:
                # 2. Save to temp file with original name to preserve name in Ghidra/DB
                orig_name = payload.get("file_name", "unknown")
                orig_name = os.path.basename(orig_name)
                if not orig_name:
                    orig_name = "unknown"

                temp_dir = tempfile.mkdtemp(prefix="bsim_worker_")
                temp_path = os.path.join(temp_dir, orig_name)
                with open(temp_path, "wb") as f:
                    f.write(raw_bytes)

                # 3. Run Analysis
                all_analysis_data = []

                if temp_path.endswith(".gpr.zip"):
                    self.job_service.add_log(
                        job_id, f"Extracting Ghidra project archive {orig_name}..."
                    )
                    import zipfile

                    with zipfile.ZipFile(temp_path, "r") as zip_ref:
                        zip_ref.extractall(temp_dir)

                    # Find .gpr file
                    gpr_path = None
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            if file.endswith(".gpr"):
                                gpr_path = os.path.join(root, file)
                                break
                        if gpr_path:
                            break

                    if not gpr_path:
                        self.job_service.add_log(
                            job_id, "Error: No .gpr file found in archive."
                        )
                        return False

                    self.job_service.add_log(
                        job_id, f"Starting Ghidra project analysis for {orig_name}..."
                    )
                    all_analysis_data = ghidra_service.analyze_project(
                        gpr_path, payload
                    )
                else:
                    self.job_service.add_log(
                        job_id, f"Starting Ghidra analysis for {orig_name}..."
                    )
                    all_analysis_data = [ghidra_service.analyze_file(temp_path, payload)]

                # 4. Store JSON results and chain indexing
                parent_id = self.r_queue.hget(f"job:{job_id}", "parent_id")
                if parent_id:
                    parent_id = (
                        parent_id.decode()
                        if isinstance(parent_id, bytes)
                        else parent_id
                    )

                for analysis_data in all_analysis_data:
                    real_md5 = analysis_data.get("file_metadata", {}).get("file_md5")
                    if not real_md5:
                        continue

                    file_id = f"{collection}:file:{real_md5}:data"
                    self.r_data.set(file_id, json.dumps(analysis_data))

                    self.job_service.add_log(
                        job_id,
                        f"Analysis complete for {analysis_data['file_metadata'].get('file_name')}. Result stored.",
                    )

                    # 5. Chain next tasks (if this is part of a pipeline)
                    if parent_id:
                        pipe_data = self.r_queue.hgetall(f"job:{parent_id}")
                        if pipe_data and "task_ids" in pipe_data:
                            task_ids = json.loads(pipe_data["task_ids"])

                            # Define remaining tasks for this file
                            next_tasks = [
                                (
                                    JobType.INDEX_META,
                                    {
                                        "collection": collection,
                                        "file_id": file_id,
                                        "md5": real_md5,
                                    },
                                ),
                                (
                                    JobType.INDEX_FUNCTIONS,
                                    {
                                        "collection": collection,
                                        "file_id": file_id,
                                        "md5": real_md5,
                                    },
                                ),
                                (
                                    JobType.INDEX_FEATURES,
                                    {
                                        "collection": collection,
                                        "file_id": file_id,
                                        "md5": real_md5,
                                    },
                                ),
                            ]

                            if not payload.get("skip_sim"):
                                from bsimvis.app.services.milvus_service import (
                                    milvus_service,
                                )

                                build_sim_payload = {
                                    "collection": collection,
                                    "file_id": file_id,
                                    "md5": real_md5,
                                }
                                # Copy similarity options
                                for opt in [
                                    "top_k",
                                    "min_score",
                                    "min_features",
                                    "algo",
                                ]:
                                    if opt in payload:
                                        build_sim_payload[opt] = payload[opt]

                                if (
                                    milvus_service.enabled
                                    and build_sim_payload.get("algo") == "milvus_sparse"
                                ):
                                    next_tasks.append(
                                        (
                                            JobType.SYNC_MILVUS,
                                            {"collection": collection},
                                        )
                                    )
                                next_tasks.append((JobType.BUILD_SIM, build_sim_payload))

                            # Create these jobs and append to task_ids
                            new_tids = []
                            for jt, pl in next_tasks:
                                tid = self.job_service.create_job(
                                    jt, pl, parent_id=parent_id, is_subtask=True
                                )
                                new_tids.append(tid)

                            task_ids.extend(new_tids)
                            self.r_queue.hset(
                                f"job:{parent_id}", "task_ids", json.dumps(task_ids)
                            )
                            self.job_service.add_log(
                                parent_id,
                                f"Appended {len(new_tids)} indexing tasks for {analysis_data['file_metadata'].get('file_name')} to pipeline.",
                            )

                return True
            except Exception as e:
                self.job_service.add_log(job_id, f"Analysis failed: {str(e)}")
                import traceback

                logging.error(traceback.format_exc())
                return False
            finally:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        os.rmdir(temp_dir)
                    except Exception:
                        pass

        elif jtype == JobType.INDEX_META.value:
            return self.processing_service.index_metadata(
                collection, file_id, self.job_service, job_id
            )

        elif jtype == JobType.INDEX_FUNCTIONS.value:
            return self.processing_service.index_functions(
                collection, file_id, self.job_service, job_id
            )

        elif jtype == JobType.INDEX_FEATURES.value:
            # For INDEX_FEATURES, we need a list of function IDs
            if md5:
                # OPTIMIZATION: Use md5 from payload directly to find functions
                batch_func_set = f"{collection}:idx:file:functions:{md5}"
                raw_ids = list(self.r_data.smembers(batch_func_set))
                function_ids = [
                    fid.replace(":meta", "") if fid.endswith(":meta") else fid
                    for fid in raw_ids
                ]
            elif file_id:
                # Fallback: Fetch monolith if MD5 is missing (legacy/direct call)
                raw_data = self.r_data.get(file_id)
                data = json.loads(raw_data) if raw_data else {}
                md5 = data.get("file_md5")
                batch_func_set = f"{collection}:idx:file:functions:{md5}"
                raw_ids = list(self.r_data.smembers(batch_func_set))
                function_ids = [
                    fid.replace(":meta", "") if fid.endswith(":meta") else fid
                    for fid in raw_ids
                ]
            elif batch_uuid:
                batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
                function_ids = list(self.r_data.smembers(batch_func_set))
            else:
                return False

            return self.feature_service.index_functions(
                collection, function_ids, self.job_service, job_id
            )

        elif jtype == JobType.SYNC_MILVUS.value:
            from bsimvis.app.services.milvus_service import milvus_service

            return milvus_service.sync_collection(
                collection, self.r_data, self.job_service, job_id
            )

        elif jtype == JobType.BUILD_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            top_k = payload.get("top_k", 1000)
            min_score = payload.get("min_score", 0.3)
            min_features = payload.get("min_features", 0)

            if not md5 and file_id:
                # Fallback: Fetch monolith if MD5 is missing
                data = self.r_data.json().get(file_id, "$")
                if isinstance(data, list) and data:
                    data = data[0]
                md5 = data.get("file_md5")

            return self.similarity_service.build_batch(
                collection,
                batch_uuid=batch_uuid,
                md5=md5,
                algo=algo,
                top_k=top_k,
                min_score=min_score,
                min_features=min_features,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.CLEAR_SIM.value:
            if payload.get("all"):
                return self.similarity_service.clear_all(
                    collection, algo=payload.get("algo")
                )
            else:
                field = "batch_uuid" if batch_uuid else "md5"
                value = batch_uuid or md5
                return self.similarity_service.clear_filtered(
                    collection, field, value, algo=payload.get("algo")
                )

        elif jtype == JobType.CLEAR_FEATURES.value:
            return self.feature_service.clear_features(
                collection, batch_uuid=batch_uuid, file_md5=md5
            )

        elif jtype == JobType.CLUSTER_FUNCTIONS.value:
            from bsimvis.app.services.cluster_service import cluster_service

            algo = payload.get("algo", "unweighted_cosine")
            min_cluster_size = payload.get("min_cluster_size", 2)
            min_samples = payload.get("min_samples", 1)
            epsilon = payload.get("epsilon", 0.0)
            selection_method = payload.get("selection_method", "eom")
            min_sim = payload.get("min_sim", 0.0)
            min_features = payload.get("min_features", 0)

            return cluster_service.run_clustering(
                collection,
                algo=algo,
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                cluster_selection_epsilon=epsilon,
                selection_method=selection_method,
                min_sim=min_sim,
                min_features=min_features,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.CLEAR_CLUSTER.value:
            from bsimvis.app.services.cluster_service import cluster_service

            algo = payload.get("algo", "unweighted_cosine")
            return cluster_service.clear_clustering(
                collection, algo=algo, job_service=self.job_service, job_id=job_id
            )

        elif jtype == JobType.BUILD_BIN_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            md5_a = payload.get("md5_a")
            md5_b = payload.get("md5_b")
            min_cohesion = payload.get("min_cohesion", 0.5)

            return bin_sim_service.build_bin_sim(
                collection,
                algo=algo,
                md5_a=md5_a,
                md5_b=md5_b,
                min_cohesion=min_cohesion,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.CLEAR_BIN_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            md5 = payload.get("md5")
            return bin_sim_service.clear_bin_sim(
                collection,
                algo=algo,
                md5=md5,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.REINDEX_BIN_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            return bin_sim_service.reindex_bin_sim(
                collection,
                algo=algo,
                job_service=self.job_service,
                job_id=job_id,
            )

        return False

    def _handle_pipeline_next(self, pipeline_id, finished_job_id):
        """Finds and enqueues the next task in the pipeline."""
        pipe_data = self.r_queue.hgetall(f"job:{pipeline_id}")
        if not pipe_data or "task_ids" not in pipe_data:
            return

        if pipe_data.get("status") == JobStatus.CANCELLED.value:
            return

        tids = json.loads(pipe_data["task_ids"])
        try:
            current_idx = tids.index(finished_job_id)
            if current_idx + 1 < len(tids):
                next_tid = tids[current_idx + 1]
                logging.info(
                    f"[*] Pipeline {pipeline_id}: Enqueueing next sub-task {next_tid}"
                )
                self.job_service.add_log(
                    pipeline_id,
                    f"Sub-task {finished_job_id} done. Enqueueing next: {next_tid}",
                )
                self.job_service.enqueue_job(next_tid)
            else:
                logging.info(f"[+] Pipeline {pipeline_id} complete!")
                self.r_queue.hset(
                    f"job:{pipeline_id}", "status", JobStatus.COMPLETED.value
                )
                self.job_service.add_log(
                    pipeline_id, "All tasks in pipeline completed."
                )
                self.job_service.update_progress(pipeline_id, 100)
        except ValueError:
            logging.error(
                f"[!] Job {finished_job_id} not found in pipeline {pipeline_id} task list."
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="worker-1")
    args = parser.parse_args()

    worker = Worker(name=args.name)
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)

    worker.run()

# To launch worker : uv run bsimvis/worker.py --name worker-1
