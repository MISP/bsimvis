import uuid
import time
import json
from enum import Enum
from .redis_client import get_queue_redis


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(Enum):
    FILE_DATA_INGEST = "file_data_ingest"
    GHIDRA_ANALYZE = "ghidra_analyze"
    INDEX_META = "idx_meta"
    INDEX_FUNCTIONS = "idx_functions"
    INDEX_FEATURES = "idx_features"
    BUILD_SIM = "build_sim"
    CLEAR_SIM = "clear_sim"
    CLEAR_FEATURES = "clear_features"
    SYNC_MILVUS = "sync_milvus"
    CLUSTER_FUNCTIONS = "cluster_functions"
    CLEAR_CLUSTER = "clear_cluster"
    CLUSTER_BINARIES = "cluster_binaries"
    CLEAR_BIN_CLUSTER = "clear_bin_cluster"
    BUILD_BIN_SIM = "build_bin_sim"
    CLEAR_BIN_SIM = "clear_bin_sim"
    REINDEX_BIN_SIM = "reindex_bin_sim"
    ENRICH_FEATURES = "enrich_features"


class JobService:
    def __init__(self):
        self.r = get_queue_redis()

    def create_job(self, job_type, payload, parent_id=None, is_subtask=False):
        """Creates a job record and returns the job_id."""
        job_id = str(uuid.uuid4())
        timestamp = int(time.time() * 1000)

        job_data = {
            "id": job_id,
            "type": job_type.value if isinstance(job_type, JobType) else job_type,
            "status": JobStatus.PENDING.value,
            "payload": json.dumps(payload),
            "created_at": timestamp,
            "updated_at": timestamp,
            "progress": 0,
            "parent_id": parent_id or "",
            "error": "",
        }

        # Store job metadata as a Hash
        self.r.hset(f"job:{job_id}", mapping=job_data)

        # Add to global list of jobs for tracking if not a subtask
        if not is_subtask:
            self.r.lpush("jobs:global", job_id)
            # Keep only the last 1000 jobs in the global list
            self.r.ltrim("jobs:global", 0, 999)

        # If it's not a subtask of a pipeline (or it's the first subtask), enqueue it
        if not is_subtask:
            self.enqueue_job(job_id)

        return job_id

    def _resolve_task(self, task, parent_id):
        """Resolves a task definition into a job_id."""
        if isinstance(task, str):
            # Existing job_id
            self.r.hset(f"job:{task}", "parent_id", parent_id)
            # Remove from jobs:global since it now has a parent
            self.r.lrem("jobs:global", 0, task)
            return task
        elif isinstance(task, (list, tuple)) and len(task) >= 2:
            jtype, payload = task[0], task[1]
            return self.create_job(jtype, payload, parent_id=parent_id, is_subtask=True)
        else:
            raise ValueError(f"Invalid task format: {task}")

    def create_pipeline(self, tasks, parent_id=None, enqueue=True):
        """
        Creates a pipeline with a list of tasks.
        tasks: list of (JobType, payload) or job_id strings
        """
        pipeline_id = f"pipe_{str(uuid.uuid4())[:18]}"
        timestamp = int(time.time() * 1000)

        task_ids = []
        for task in tasks:
            tid = self._resolve_task(task, pipeline_id)
            task_ids.append(tid)

        pipeline_data = {
            "id": pipeline_id,
            "type": "pipeline",
            "status": JobStatus.PENDING.value,
            "task_ids": json.dumps(task_ids),
            "created_at": timestamp,
            "updated_at": timestamp,
            "progress": 0,
            "parent_id": parent_id or "",
            "error": "",
        }

        self.r.hset(f"job:{pipeline_id}", mapping=pipeline_data)
        self.r.lpush("jobs:global", pipeline_id)
        self.r.ltrim("jobs:global", 0, 999)

        if enqueue:
            self.start_job(pipeline_id)

        return pipeline_id

    def create_group(self, tasks, parent_id=None, enqueue=True):
        """
        Creates a group with a list of tasks to run in parallel.
        tasks: list of (JobType, payload) or job_id strings
        """
        group_id = f"group_{str(uuid.uuid4())[:18]}"
        timestamp = int(time.time() * 1000)

        task_ids = []
        for task in tasks:
            tid = self._resolve_task(task, group_id)
            task_ids.append(tid)

        group_data = {
            "id": group_id,
            "type": "group",
            "status": JobStatus.PENDING.value,
            "task_ids": json.dumps(task_ids),
            "created_at": timestamp,
            "updated_at": timestamp,
            "progress": 0,
            "parent_id": parent_id or "",
            "error": "",
        }

        self.r.hset(f"job:{group_id}", mapping=group_data)
        self.r.lpush("jobs:global", group_id)
        self.r.ltrim("jobs:global", 0, 999)

        if enqueue:
            self.start_job(group_id)

        return group_id

    def enqueue_job(self, job_id):
        """Pushes a job ID onto the appropriate priority queue."""
        job = self.r.hgetall(f"job:{job_id}")
        jtype = job.get("type") if job else None

        high_priority_types = [
            JobType.CLEAR_SIM.value,
            JobType.CLEAR_FEATURES.value,
            JobType.CLEAR_CLUSTER.value,
            JobType.CLEAR_BIN_SIM.value,
            JobType.SYNC_MILVUS.value,
        ]

        if jtype in high_priority_types:
            self.r.lpush("jobs:pending:high", job_id)
        else:
            self.r.lpush("jobs:pending", job_id)

    def start_job(self, job_id):
        """Starts a job. If it's a group or pipeline, it resolves down to leaf jobs."""
        job = self.r.hgetall(f"job:{job_id}")
        if not job:
            return
            
        jtype = job.get("type")
        
        if job.get("status") == JobStatus.CANCELLED.value:
            return

        if jtype in ["pipeline", "group"]:
            self.r.hset(f"job:{job_id}", "status", JobStatus.RUNNING.value)
        
        if jtype == "pipeline":
            tids = json.loads(job.get("task_ids", "[]"))
            if tids:
                self.start_job(tids[0])
            else:
                self.complete_job(job_id)
        elif jtype == "group":
            tids = json.loads(job.get("task_ids", "[]"))
            if tids:
                for tid in tids:
                    self.start_job(tid)
            else:
                self.complete_job(job_id)
        else:
            self.enqueue_job(job_id)

    def complete_job(self, job_id):
        """Marks a job as completed and advances its parent if applicable."""
        self.r.hset(f"job:{job_id}", "status", JobStatus.COMPLETED.value)
        self.update_progress(job_id, 100)
        
        parent_id = self.r.hget(f"job:{job_id}", "parent_id")
        if parent_id:
            self.advance_parent(parent_id, job_id)

    def advance_parent(self, parent_id, finished_job_id):
        """Advances the parent job based on its type (pipeline sequence or group barrier)."""
        parent = self.r.hgetall(f"job:{parent_id}")
        if not parent or parent.get("status") == JobStatus.CANCELLED.value:
            return
            
        ptype = parent.get("type")
        tids = json.loads(parent.get("task_ids", "[]"))
        
        if ptype == "pipeline":
            try:
                current_idx = tids.index(finished_job_id)
                if current_idx + 1 < len(tids):
                    next_tid = tids[current_idx + 1]
                    self.add_log(parent_id, f"Sub-task {finished_job_id} done. Starting next: {next_tid}")
                    self.start_job(next_tid)
                else:
                    self.add_log(parent_id, "All tasks in pipeline completed.")
                    self.complete_job(parent_id)
            except ValueError:
                pass
                
        elif ptype == "group":
            all_done = True
            any_failed = False
            for tid in tids:
                status = self.r.hget(f"job:{tid}", "status")
                if status not in [JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
                    all_done = False
                    break
                if status == JobStatus.FAILED.value:
                    any_failed = True
                    
            if all_done:
                if any_failed:
                    self.add_log(parent_id, "All tasks in group finished, but some failed.")
                else:
                    self.add_log(parent_id, "All tasks in group completed.")
                self.complete_job(parent_id)

    def fail_job(self, job_id, error_msg):
        """Marks a job as failed and cascades failure to its parent."""
        self.r.hset(f"job:{job_id}", "status", JobStatus.FAILED.value)
        self.r.hset(f"job:{job_id}", "error", error_msg)
        self.add_log(job_id, f"Execution error: {error_msg}")
        
        parent_id = self.r.hget(f"job:{job_id}", "parent_id")
        if parent_id:
            parent_type = self.r.hget(f"job:{parent_id}", "type")
            if parent_type == "group":
                # For groups, we don't fail the group instantly. We wait for other tasks.
                self.advance_parent(parent_id, job_id)
            else:
                self.fail_job(parent_id, f"Failed because sub-task {job_id} failed.")

    def get_job_status(self, job_id):
        """Returns the full job or pipeline status."""
        data = self.r.hgetall(f"job:{job_id}")
        if not data:
            return None

        # Decode JSON fields
        if "payload" in data:
            data["payload"] = json.loads(data["payload"])
        if "task_ids" in data:
            tids = json.loads(data["task_ids"])
            data["task_ids"] = tids
            # Enrich with sub-task statuses
            sub_tasks = []
            for tid in tids:
                st = self.r.hgetall(f"job:{tid}")
                if st:
                    sub_tasks.append(
                        {
                            "id": tid,
                            "type": st.get("type"),
                            "status": st.get("status"),
                            "progress": int(st.get("progress", 0)),
                            "perf_total": float(st.get("perf_total", 0)),
                            "perf_python": float(st.get("perf_python", 0)),
                            "perf_db": float(st.get("perf_db", 0)),
                            "perf_lua": float(st.get("perf_lua", 0)),
                        }
                    )
            data["sub_tasks"] = sub_tasks

        # Fetch logs
        logs = self.r.lrange(f"job_log:{job_id}", 0, -1)
        data["logs"] = [log for log in logs]

        # Fetch performance details if available
        perf_details = self.r.get(f"job_perf_details:{job_id}")
        if perf_details:
            data["perf_details"] = json.loads(perf_details)

        return data

    def save_performance_stats(self, job_id, stats):
        """Saves performance stats for a job."""
        # Update main job hash with core metrics
        self.r.hset(
            f"job:{job_id}",
            mapping={
                "perf_total": str(stats["total_time"]),
                "perf_python": str(stats["python_time"]),
                "perf_db": str(stats["db_time"]),
                "perf_lua": str(stats["lua_time"]),
                "perf_ops": str(stats["ops_count"]),
            },
        )

        # Store full details separately to avoid hash bloat
        details = stats["details"]
        # Limit to last 500 ops for detail view if it's huge
        if len(details) > 500:
            details = (
                details[:200]
                + [{"op": "...", "time": 0, "cat": "skipped", "ts": 0}]
                + details[-300:]
            )

        self.r.set(
            f"job_perf_details:{job_id}", json.dumps(details), ex=86400
        )  # 24h retention

    def cancel_job(self, job_id):
        """Marks a job or pipeline as cancelled."""
        data = self.r.hgetall(f"job:{job_id}")
        if not data:
            return False

        self.r.hset(f"job:{job_id}", "status", JobStatus.CANCELLED.value)

        # Remove from pending queues to update stats immediately
        self.r.lrem("jobs:pending", 0, job_id)
        self.r.lrem("jobs:pending:high", 0, job_id)

        self.r.lpush(
            f"job_log:{job_id}", f"[{int(time.time()*1000)}] Job cancelled by user."
        )

        # Cancel all subtasks recursively
        if "task_ids" in data:
            tids = json.loads(data["task_ids"])
            for tid in tids:
                self.cancel_job(tid)

        return True

    def cancel_all_jobs(self):
        """Cancels all pending/running jobs and pipelines."""
        cancelled = 0
        job_ids = self.r.lrange("jobs:global", 0, -1)
        for jid in job_ids:
            status = self.r.hget(f"job:{jid}", "status")
            if status in [JobStatus.PENDING.value, JobStatus.RUNNING.value]:
                self.cancel_job(jid)
                cancelled += 1
        return cancelled

    def add_log(self, job_id, message):
        """Adds a log entry for a job."""
        timestamp = int(time.time() * 1000)
        log_entry = f"[{timestamp}] {message}"
        self.r.lpush(f"job_log:{job_id}", log_entry)
        self.r.ltrim(f"job_log:{job_id}", 0, 100)  # Keep last 100 logs

        # Also update updated_at
        self.r.hset(f"job:{job_id}", "updated_at", timestamp)

    def update_progress(self, job_id, progress, message=None):
        """Updates progress (0-100) and optionally adds a log entry."""
        self.r.hset(f"job:{job_id}", "progress", progress)
        if message:
            self.add_log(job_id, message)

        # If it has a parent pipeline, update the pipeline's overall progress
        parent_id = self.r.hget(f"job:{job_id}", "parent_id")
        if parent_id:
            self._update_pipeline_aggregate_progress(parent_id)

    def _update_pipeline_aggregate_progress(self, pipeline_id):
        """Recalculates pipeline progress based on subtasks."""
        pipe_data = self.r.hgetall(f"job:{pipeline_id}")
        if not pipe_data or "task_ids" not in pipe_data:
            return

        tids = json.loads(pipe_data["task_ids"])
        if not tids:
            return

        total_p = 0
        for tid in tids:
            p = self.r.hget(f"job:{tid}", "progress")
            total_p += int(p or 0)

        agg_progress = total_p // len(tids)
        self.r.hset(f"job:{pipeline_id}", "progress", agg_progress)
        self.r.hset(f"job:{pipeline_id}", "updated_at", int(time.time() * 1000))

    def get_global_stats(self):
        """Returns aggregate stats across all active and pending jobs."""
        processing_ids = self.r.lrange("jobs:processing", 0, -1)
        pending_count = self.r.llen("jobs:pending") + self.r.llen("jobs:pending:high")

        total_speed = 0.0
        active_jobs_count = 0
        remaining_items = 0

        for jid in processing_ids:
            job = self.r.hgetall(f"job:{jid}")
            if not job:
                continue

            # Only count if NOT cancelled, failed, or completed
            status = job.get("status")
            if status in [
                JobStatus.CANCELLED.value,
                JobStatus.FAILED.value,
                JobStatus.COMPLETED.value,
            ]:
                continue

            active_jobs_count += 1

            speed = float(job.get("speed", 0))
            if speed > 0:
                total_speed += speed

            total = int(job.get("total_items", 0))
            done = int(job.get("processed_items", 0))
            remaining_items += max(0, total - done)

        # Average speed
        avg_speed = total_speed / active_jobs_count if active_jobs_count > 0 else 0

        global_eta = remaining_items / total_speed if total_speed > 0 else 0

        # Collect active collections
        active_collections = set()
        # Check processing jobs
        all_processing_ids = self.r.lrange("jobs:processing", 0, -1)
        # Also check pending jobs (last 100 for efficiency)
        pending_ids = self.r.lrange("jobs:pending", 0, 100) + self.r.lrange(
            "jobs:pending:high", 0, 100
        )

        for jid in set(all_processing_ids + pending_ids):
            job = self.r.hgetall(f"job:{jid}")
            if not job:
                continue
            if job.get("status") in [
                JobStatus.CANCELLED.value,
                JobStatus.FAILED.value,
                JobStatus.COMPLETED.value,
            ]:
                continue

            payload_raw = job.get("payload")
            if payload_raw:
                try:
                    payload = json.loads(payload_raw)
                    coll = payload.get("collection")
                    if coll:
                        active_collections.add(coll)
                except:
                    pass

        return {
            "active_workers": active_jobs_count,
            "pending_jobs": pending_count,
            "avg_speed": round(avg_speed, 2),
            "total_speed": round(total_speed, 2),
            "remaining_items": remaining_items,
            "global_eta": int(global_eta),
            "active_collections": list(active_collections),
        }

    def list_jobs(self, limit=100, offset=0):
        """Returns a paged list of jobs and the total count."""
        total = self.r.llen("jobs:global")
        job_ids = self.r.lrange("jobs:global", offset, offset + limit - 1)
        # Fetch summary info for each job in a pipeline (MGET equivalent for hashes not possible, but we can do a quick loop)
        results = []
        for jid in job_ids:
            job = self.r.hgetall(f"job:{jid}")
            if job:
                # Extract target from payload
                target = ""
                collection = ""
                payload_raw = job.get("payload")
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
                        collection = payload.get("collection", "")
                        target = (
                            payload.get("md5")
                            or payload.get("file_id")
                            or payload.get("batch_uuid")
                            or ""
                        )
                        # Truncate long targets
                        if target and len(target) > 20:
                            target = target[:8] + "..." + target[-8:]
                    except:
                        pass

                # Basic summary info
                parent_id = job.get("parent_id", "")
                task_ids = []
                if "task_ids" in job:
                    try:
                        task_ids = json.loads(job["task_ids"])
                    except:
                        pass

                results.append(
                    {
                        "id": jid,
                        "type": job.get("type"),
                        "status": job.get("status"),
                        "progress": int(job.get("progress", 0)),
                        "collection": collection,
                        "target": target,
                        "created_at": int(job.get("created_at", 0)),
                        "updated_at": int(job.get("updated_at", 0)),
                        "parent_id": parent_id,
                        "task_ids": task_ids,
                    }
                )

        # Recursively fetch and append all descendant subtasks to ensure
        # that nested trees are fully represented in a single response.
        results_map = {res["id"]: res for res in results}
        to_process = [res for res in results]

        while to_process:
            current = to_process.pop(0)
            if current["type"] in ["pipeline", "group"] and current.get("task_ids"):
                for tid in current["task_ids"]:
                    if tid not in results_map:
                        sub_job = self.r.hgetall(f"job:{tid}")
                        if sub_job:
                            # Extract target from payload
                            target = ""
                            collection = ""
                            payload_raw = sub_job.get("payload")
                            if payload_raw:
                                try:
                                    payload = json.loads(payload_raw)
                                    collection = payload.get("collection", "")
                                    target = (
                                        payload.get("md5")
                                        or payload.get("file_id")
                                        or payload.get("batch_uuid")
                                        or ""
                                    )
                                    if target and len(target) > 20:
                                        target = target[:8] + "..." + target[-8:]
                                except:
                                    pass

                            # Decode task_ids if they exist
                            task_ids = []
                            if "task_ids" in sub_job:
                                try:
                                    task_ids = json.loads(sub_job["task_ids"])
                                except:
                                    pass

                            sub_job_dict = {
                                "id": tid,
                                "type": sub_job.get("type"),
                                "status": sub_job.get("status"),
                                "progress": int(sub_job.get("progress", 0)),
                                "collection": collection,
                                "target": target,
                                "created_at": int(sub_job.get("created_at", 0)),
                                "updated_at": int(sub_job.get("updated_at", 0)),
                                "parent_id": current["id"],
                                "task_ids": task_ids,
                            }
                            results_map[tid] = sub_job_dict
                            results.append(sub_job_dict)
                            to_process.append(sub_job_dict)

        return results, total
