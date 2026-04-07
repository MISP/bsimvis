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
    INDEX_META = "idx_meta"
    INDEX_FUNCTIONS = "idx_functions"
    INDEX_FEATURES = "idx_features"
    BUILD_SIM = "build_sim"
    CLEAR_SIM = "clear_sim"
    CLEAR_FEATURES = "clear_features"

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
        
        # Add to global list of jobs for tracking
        self.r.lpush("jobs:global", job_id)
        # Keep only the last 1000 jobs in the global list
        self.r.ltrim("jobs:global", 0, 999)

        # If it's not a subtask of a pipeline (or it's the first subtask), enqueue it
        if not is_subtask:
            self.enqueue_job(job_id)

        return job_id

    def create_pipeline(self, tasks):
        """
        Creates a pipeline with a list of tasks.
        tasks: list of (JobType, payload)
        """
        pipeline_id = f"pipe_{str(uuid.uuid4())[:18]}"
        timestamp = int(time.time() * 1000)

        task_ids = []
        for i, (jtype, payload) in enumerate(tasks):
            # Create subtasks but don't enqueue them independently (is_subtask=True)
            tid = self.create_job(jtype, payload, parent_id=pipeline_id, is_subtask=True)
            task_ids.append(tid)

        pipeline_data = {
            "id": pipeline_id,
            "type": "pipeline",
            "status": JobStatus.PENDING.value,
            "task_ids": json.dumps(task_ids),
            "current_task_idx": 0,
            "created_at": timestamp,
            "updated_at": timestamp,
            "progress": 0,
            "error": "",
        }

        self.r.hset(f"job:{pipeline_id}", mapping=pipeline_data)
        self.r.lpush("jobs:global", pipeline_id)
        self.r.ltrim("jobs:global", 0, 999)

        # Enqueue only the first task of the pipeline
        if task_ids:
            self.enqueue_job(task_ids[0])

        return pipeline_id

    def enqueue_job(self, job_id):
        """Pushes a job ID onto the pending queue."""
        self.r.lpush("jobs:pending", job_id)

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
                    sub_tasks.append({
                        "id": tid,
                        "type": st.get("type"),
                        "status": st.get("status"),
                        "progress": int(st.get("progress", 0))
                    })
            data["sub_tasks"] = sub_tasks

        # Fetch logs
        logs = self.r.lrange(f"job_log:{job_id}", 0, -1)
        data["logs"] = [log for log in logs]
        
        return data

    def cancel_job(self, job_id):
        """Marks a job or pipeline as cancelled."""
        data = self.r.hgetall(f"job:{job_id}")
        if not data:
            return False
        
        self.r.hset(f"job:{job_id}", "status", JobStatus.CANCELLED.value)
        self.r.lpush(f"job_log:{job_id}", f"[{int(time.time()*1000)}] Job cancelled by user.")
        
        # If it's a pipeline, cancel all subtasks
        if "task_ids" in data:
            tids = json.loads(data["task_ids"])
            for tid in tids:
                self.r.hset(f"job:{tid}", "status", JobStatus.CANCELLED.value)
        
        return True

    def add_log(self, job_id, message):
        """Adds a log entry for a job."""
        timestamp = int(time.time() * 1000)
        log_entry = f"[{timestamp}] {message}"
        self.r.lpush(f"job_log:{job_id}", log_entry)
        self.r.ltrim(f"job_log:{job_id}", 0, 100) # Keep last 100 logs
        
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
        pending_count = self.r.llen("jobs:pending")
        
        total_speed = 0.0
        active_jobs_count = 0
        remaining_items = 0
        
        for jid in processing_ids:
            job = self.r.hgetall(f"job:{jid}")
            if not job: continue
            
            speed = float(job.get("speed", 0))
            if speed > 0:
                total_speed += speed
                active_jobs_count += 1
            
            total = int(job.get("total_items", 0))
            done = int(job.get("processed_items", 0))
            remaining_items += max(0, total - done)

        # Average speed (weighted by number of workers if we have them)
        avg_speed = total_speed / active_jobs_count if active_jobs_count > 0 else 0
        
        global_eta = remaining_items / total_speed if total_speed > 0 else 0
        
        return {
            "active_workers": len(processing_ids),
            "pending_jobs": pending_count,
            "avg_speed": round(avg_speed, 2),
            "total_speed": round(total_speed, 2),
            "remaining_items": remaining_items,
            "global_eta": int(global_eta)
        }

    def list_jobs(self, limit=50):
        """Returns a list of the most recent jobs."""
        job_ids = self.r.lrange("jobs:global", 0, limit - 1)
        # Fetch summary info for each job in a pipeline (MGET equivalent for hashes not possible, but we can do a quick loop)
        results = []
        for jid in job_ids:
            job = self.r.hgetall(f"job:{jid}")
            if job:
                # Basic summary info
                results.append({
                    "id": jid,
                    "type": job.get("type"),
                    "status": job.get("status"),
                    "progress": int(job.get("progress", 0)),
                    "created_at": int(job.get("created_at", 0)),
                    "updated_at": int(job.get("updated_at", 0))
                })
        return results
