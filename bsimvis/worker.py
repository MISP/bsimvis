import time
import json
import logging
import signal
import sys
from bsimvis.app.services.redis_client import get_queue_redis, get_redis
from bsimvis.app.services.job_service import JobService, JobStatus, JobType
from bsimvis.app.services.processing_service import ProcessingService
from bsimvis.app.services.feature_service import FeatureService
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.timer_service import job_timer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class Worker:
    def __init__(self, name="worker-1"):
        self.name = name
        self.r_queue = get_queue_redis()
        self.r_data = get_redis()
        self.job_service = JobService()
        self.processing_service = ProcessingService(self.r_data)
        self.feature_service = FeatureService(self.r_data)
        
        # Initialize Lua scripts for this process
        lua_manager.init_app()
        
        self.similarity_service = SimilarityService(self.r_data)
        self.running = True

    def stop(self, signum, frame):
        logging.info(f"[*] Worker {self.name} received stop signal...")
        self.running = False

    def run(self):
        logging.info(f"[*] Worker {self.name} started. Waiting for jobs...")
        
        while self.running:
            try:
                # Reliable Queue Pattern: BLMOVE from pending to processing
                # BLMOVE <source> <destination> <LEFT|RIGHT> <LEFT|RIGHT> <timeout>
                # We move from the right of pending to the left of processing (FIFO)
                job_id = self.r_queue.execute_command("BLMOVE", "jobs:pending", "jobs:processing", "RIGHT", "LEFT", 2)
                
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
        self.job_service.add_log(job_id, f"Worker {self.name} started processing {jtype}.")
        self.r_queue.hset(f"job:{job_id}", "status", JobStatus.RUNNING.value)
        
        # Execute Job within a timer context
        with job_timer(job_id) as timer:
            try:
                # Dispatch
                success = self._dispatch(jtype, payload, job_id)
                
                if success:
                    self.job_service.add_log(job_id, f"Job {jtype} completed successfully.")
                    self.job_service.update_progress(job_id, 100)
                    self.r_queue.hset(f"job:{job_id}", "status", JobStatus.COMPLETED.value)
                    
                    # Pipeline Chaining: Check if we need to enqueue the next task
                    if parent_id:
                        self._handle_pipeline_next(parent_id, job_id)
                else:
                    self.r_queue.hset(f"job:{job_id}", "status", JobStatus.FAILED.value)
                    self.job_service.add_log(job_id, "Job failed (returned False from dispatcher).")
                    
            except Exception as e:
                logging.error(f"[!] Job {job_id} failed with error: {e}")
                import traceback
                traceback.print_exc()
                self.r_queue.hset(f"job:{job_id}", "status", JobStatus.FAILED.value)
                self.r_queue.hset(f"job:{job_id}", "error", str(e))
                self.job_service.add_log(job_id, f"Execution error: {e}")
                
                # If sub-task fails, mark parent pipeline as failed
                if parent_id:
                    self.r_queue.hset(f"job:{parent_id}", "status", JobStatus.FAILED.value)
                    self.job_service.add_log(parent_id, f"Pipeline failed because sub-task {job_id} failed.")
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
        
        if jtype == JobType.INDEX_META.value:
            return self.processing_service.index_metadata(collection, file_id, self.job_service, job_id)
            
        elif jtype == JobType.INDEX_FUNCTIONS.value:
            return self.processing_service.index_functions(collection, file_id, self.job_service, job_id)
            
        elif jtype == JobType.INDEX_FEATURES.value:
            # For INDEX_FEATURES, we need a list of function IDs
            if md5:
                # OPTIMIZATION: Use md5 from payload directly to find functions
                batch_func_set = f"idx:{collection}:file_funcs:{md5}"
                raw_ids = list(self.r_data.smembers(batch_func_set))
                function_ids = [fid.replace(":meta", "") if fid.endswith(":meta") else fid for fid in raw_ids]
            elif file_id:
                # Fallback: Fetch monolith if MD5 is missing (legacy/direct call)
                data = self.r_data.json().get(file_id, "$")
                if isinstance(data, list) and data: data = data[0]
                md5 = data.get("file_md5")
                batch_func_set = f"idx:{collection}:file_funcs:{md5}"
                raw_ids = list(self.r_data.smembers(batch_func_set))
                function_ids = [fid.replace(":meta", "") if fid.endswith(":meta") else fid for fid in raw_ids]
            elif batch_uuid:
                batch_func_set = f"{collection}:batch:{batch_uuid}:functions"
                function_ids = list(self.r_data.smembers(batch_func_set))
            else:
                return False
                
            return self.feature_service.index_functions(collection, function_ids, self.job_service, job_id)
            
        elif jtype == JobType.BUILD_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            top_k = payload.get("top_k", 1000)
            min_score = payload.get("min_score", 0.3)
            
            if not md5 and file_id:
                # Fallback: Fetch monolith if MD5 is missing
                data = self.r_data.json().get(file_id, "$")
                if isinstance(data, list) and data: data = data[0]
                md5 = data.get("file_md5")
            
            return self.similarity_service.build_batch(
                collection, batch_uuid=batch_uuid, md5=md5, 
                algo=algo, top_k=top_k, min_score=min_score,
                job_service=self.job_service, job_id=job_id
            )
            
        elif jtype == JobType.CLEAR_SIM.value:
            field = "batch_uuid" if batch_uuid else "md5"
            value = batch_uuid or md5
            return self.similarity_service.clear_filtered(collection, field, value, algo=payload.get("algo"))
            
        elif jtype == JobType.CLEAR_FEATURES.value:
            return self.feature_service.clear_features(collection, batch_uuid=batch_uuid, file_md5=md5)
            
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
                logging.info(f"[*] Pipeline {pipeline_id}: Enqueueing next sub-task {next_tid}")
                self.job_service.add_log(pipeline_id, f"Sub-task {finished_job_id} done. Enqueueing next: {next_tid}")
                self.job_service.enqueue_job(next_tid)
            else:
                logging.info(f"[+] Pipeline {pipeline_id} complete!")
                self.r_queue.hset(f"job:{pipeline_id}", "status", JobStatus.COMPLETED.value)
                self.job_service.add_log(pipeline_id, "All tasks in pipeline completed.")
                self.job_service.update_progress(pipeline_id, 100)
        except ValueError:
            logging.error(f"[!] Job {finished_job_id} not found in pipeline {pipeline_id} task list.")

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