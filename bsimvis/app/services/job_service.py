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
    DELETE_COLLECTION = "delete_collection"
    CLEAN_COLLECTION = "clean_collection"
    PROPAGATE_METADATA = "propagate_metadata"
    BUILD_POOL_SIM = "build_pool_sim"
    CLUSTER_POOL = "cluster_pool"
    INIT_POOL_BUILD = "init_pool_build"
    FINALIZE_POOL_BUILD = "finalize_pool_build"
    BUILD_POOL_BIN_SIM = "build_pool_bin_sim"
    CLUSTER_POOL_BINARIES = "cluster_pool_binaries"


def safe_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, bytes):
        try:
            val = val.decode("utf-8")
        except:
            return default
    try:
        return int(val)
    except (ValueError, TypeError):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return default


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
        if isinstance(payload, dict):
            if "collection" in payload:
                job_data["collection"] = payload["collection"]
            elif "pool_id" in payload:
                job_data["collection"] = f"pool:{payload['pool_id']}"

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

        # Determine collection from tasks if possible
        collection = None
        for task in tasks:
            if isinstance(task, (list, tuple)) and len(task) >= 2:
                payload = task[1]
                if isinstance(payload, dict):
                    if "collection" in payload:
                        collection = payload["collection"]
                        break
                    elif "pool_id" in payload:
                        collection = f"pool:{payload['pool_id']}"
                        break
            elif isinstance(task, str):
                st = self.r.hgetall(f"job:{task}")
                if st:
                    st_decoded = {
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in st.items()
                    }
                    collection = st_decoded.get("collection")
                    if not collection and "payload" in st_decoded:
                        try:
                            pl = json.loads(st_decoded["payload"])
                            collection = pl.get("collection")
                            if not collection and "pool_id" in pl:
                                collection = f"pool:{pl['pool_id']}"
                        except:
                            pass
                    if collection:
                        break

        if collection:
            pipeline_data["collection"] = collection

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

        # Determine collection from tasks if possible
        collection = None
        for task in tasks:
            if isinstance(task, (list, tuple)) and len(task) >= 2:
                payload = task[1]
                if isinstance(payload, dict):
                    if "collection" in payload:
                        collection = payload["collection"]
                        break
                    elif "pool_id" in payload:
                        collection = f"pool:{payload['pool_id']}"
                        break
            elif isinstance(task, str):
                st = self.r.hgetall(f"job:{task}")
                if st:
                    st_decoded = {
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in st.items()
                    }
                    collection = st_decoded.get("collection")
                    if not collection and "payload" in st_decoded:
                        try:
                            pl = json.loads(st_decoded["payload"])
                            collection = pl.get("collection")
                            if not collection and "pool_id" in pl:
                                collection = f"pool:{pl['pool_id']}"
                        except:
                            pass
                    if collection:
                        break

        if collection:
            group_data["collection"] = collection

        self.r.hset(f"job:{group_id}", mapping=group_data)
        self.r.lpush("jobs:global", group_id)
        self.r.ltrim("jobs:global", 0, 999)

        if enqueue:
            self.start_job(group_id)

        return group_id

    def enqueue_job(self, job_id, is_continuation=False):
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
            if is_continuation:
                # Push to tail/RIGHT so workers pulling from tail (LMOVE/BLMOVE ... RIGHT LEFT) pick it up immediately
                self.r.rpush("jobs:pending", job_id)
            else:
                # Push to head/LEFT (normal queueing order)
                self.r.lpush("jobs:pending", job_id)

    def start_job(self, job_id, is_continuation=False):
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
                self.start_job(tids[0], is_continuation=is_continuation)
            else:
                self.complete_job(job_id)
        elif jtype == "group":
            tids = json.loads(job.get("task_ids", "[]"))
            if tids:
                for tid in tids:
                    self.start_job(tid, is_continuation=is_continuation)
            else:
                self.complete_job(job_id)
        else:
            self.enqueue_job(job_id, is_continuation=is_continuation)

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
                    self.add_log(
                        parent_id,
                        f"Sub-task {finished_job_id} done. Starting next: {next_tid}",
                    )
                    self.start_job(next_tid, is_continuation=True)
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
                if status not in [
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.CANCELLED.value,
                ]:
                    all_done = False
                    break
                if status == JobStatus.FAILED.value:
                    any_failed = True

            if all_done:
                if any_failed:
                    self.add_log(
                        parent_id, "All tasks in group finished, but some failed."
                    )
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
                            "progress": safe_int(st.get("progress", 0)),
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
            total_p += safe_int(p, 0)

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

            total = safe_int(job.get("total_items", 0))
            done = safe_int(job.get("processed_items", 0))
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

        active_jobs = []
        for jid in set(all_processing_ids + pending_ids):
            job = self.r.hgetall(f"job:{jid}")
            if not job:
                continue

            # Decode key-value pairs of job if they are bytes
            job_decoded = {}
            for k, v in job.items():
                k_str = k.decode() if isinstance(k, bytes) else k
                v_str = v.decode() if isinstance(v, bytes) else v
                job_decoded[k_str] = v_str
            job = job_decoded

            status = job.get("status")
            if status in [
                JobStatus.CANCELLED.value,
                JobStatus.FAILED.value,
                JobStatus.COMPLETED.value,
            ]:
                continue

            jtype = job.get("type", "")
            coll = job.get("collection", "")
            pool_id = ""
            payload_raw = job.get("payload")
            if payload_raw:
                try:
                    payload = json.loads(payload_raw)
                    if not coll:
                        coll = payload.get("collection", "")
                    pool_id = payload.get("pool_id", "")
                except:
                    pass

            if not coll and pool_id:
                coll = f"pool:{pool_id}"

            if coll:
                active_collections.add(coll)

            jid_str = jid.decode() if isinstance(jid, bytes) else jid
            active_jobs.append(
                {
                    "id": jid_str,
                    "type": jtype,
                    "status": status,
                    "collection": coll,
                    "pool_id": pool_id,
                    "progress": safe_int(job.get("progress", 0)),
                }
            )

        return {
            "active_workers": active_jobs_count,
            "pending_jobs": pending_count,
            "avg_speed": round(avg_speed, 2),
            "total_speed": round(total_speed, 2),
            "remaining_items": remaining_items,
            "global_eta": int(global_eta),
            "active_collections": list(active_collections),
            "active_jobs": active_jobs,
        }

    def list_jobs(
        self, limit=100, offset=0, collection=None, pool=None, status=None, jtype=None
    ):
        """Returns a paged list of jobs and the total count."""
        # Fetch all top-level job IDs (at most 1000)
        all_job_ids = self.r.lrange("jobs:global", 0, -1)

        pool_cols = set()
        if pool:
            pool_cols = {
                c.decode() if isinstance(c, bytes) else c
                for c in self.r.smembers(f"global:pool:{pool}:collections_list")
            }
            pool_cols.add(f"pool:{pool}")

        # Batch fetch all top-level job hashes using pipeline
        pipe = self.r.pipeline()
        for jid in all_job_ids:
            pipe.hgetall(f"job:{jid}")
        raw_jobs = pipe.execute()

        # Parse payloads and build initial list of top-level job dicts
        top_level_jobs = []
        for jid, job in zip(all_job_ids, raw_jobs):
            if not job:
                continue

            # Decode key-value pairs of job if they are bytes
            job_decoded = {}
            for k, v in job.items():
                k_str = k.decode() if isinstance(k, bytes) else k
                v_str = v.decode() if isinstance(v, bytes) else v
                job_decoded[k_str] = v_str
            job = job_decoded

            # Extract target and collection from payload
            target = ""
            coll = job.get("collection", "")
            if not coll:
                payload_raw = job.get("payload")
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
                        coll = payload.get("collection", "")
                        if not coll and "pool_id" in payload:
                            coll = f"pool:{payload['pool_id']}"
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
            else:
                payload_raw = job.get("payload")
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
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

            parent_id = job.get("parent_id", "")
            task_ids = []
            if "task_ids" in job:
                try:
                    task_ids = json.loads(job["task_ids"])
                except:
                    pass

            if not coll and job.get("type") in ["pipeline", "group"] and task_ids:
                first_task_id = task_ids[0]
                first_task = self.r.hgetall(f"job:{first_task_id}")
                if first_task:
                    first_task = {
                        k.decode() if isinstance(k, bytes) else k: (
                            v.decode() if isinstance(v, bytes) else v
                        )
                        for k, v in first_task.items()
                    }
                    coll = first_task.get("collection", "")
                    if not coll:
                        payload_raw = first_task.get("payload")
                        if payload_raw:
                            try:
                                payload = json.loads(payload_raw)
                                coll = payload.get("collection", "")
                                if not coll and "pool_id" in payload:
                                    coll = f"pool:{payload['pool_id']}"
                            except:
                                pass

            # Apply filters early to top-level jobs
            if collection and coll != collection:
                # If pool is specified, allow pool-level jobs to bypass the collection filter
                if not (pool and coll == f"pool:{pool}"):
                    continue
            if pool and coll not in pool_cols:
                # Also check if payload explicitly has pool_id
                payload_pool = ""
                payload_raw = job.get("payload")
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
                        payload_pool = payload.get("pool_id", "")
                    except:
                        pass
                if payload_pool != pool:
                    continue
            if status and job.get("status") != status:
                continue
            if jtype and job.get("type") != jtype:
                continue

            top_level_jobs.append(
                {
                    "id": jid,
                    "type": job.get("type"),
                    "status": job.get("status"),
                    "progress": safe_int(job.get("progress", 0)),
                    "collection": coll,
                    "target": target,
                    "created_at": safe_int(job.get("created_at", 0)),
                    "updated_at": safe_int(job.get("updated_at", 0)),
                    "parent_id": parent_id,
                    "task_ids": task_ids,
                }
            )

        # Total is the count of filtered top-level jobs
        total = len(top_level_jobs)

        # Paginate top-level jobs
        paginated_top_level = top_level_jobs[offset : offset + limit]

        # Now, recursively fetch all subtask descendants for the paginated top-level jobs.
        results = list(paginated_top_level)
        results_map = {res["id"]: res for res in results}

        to_fetch = []
        for job in paginated_top_level:
            if job["type"] in ["pipeline", "group"] and job.get("task_ids"):
                to_fetch.extend(job["task_ids"])

        while to_fetch:
            ids_to_fetch = list(set(tid for tid in to_fetch if tid not in results_map))
            to_fetch = []

            if not ids_to_fetch:
                break

            pipe = self.r.pipeline()
            for tid in ids_to_fetch:
                pipe.hgetall(f"job:{tid}")
            raw_sub_jobs = pipe.execute()

            for tid, sub_job in zip(ids_to_fetch, raw_sub_jobs):
                if not sub_job:
                    continue

                target = ""
                coll = sub_job.get("collection", "")
                if not coll:
                    payload_raw = sub_job.get("payload")
                    if payload_raw:
                        try:
                            payload = json.loads(payload_raw)
                            coll = payload.get("collection", "")
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
                else:
                    payload_raw = sub_job.get("payload")
                    if payload_raw:
                        try:
                            payload = json.loads(payload_raw)
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
                    "progress": safe_int(sub_job.get("progress", 0)),
                    "collection": coll,
                    "target": target,
                    "created_at": safe_int(sub_job.get("created_at", 0)),
                    "updated_at": safe_int(sub_job.get("updated_at", 0)),
                    "parent_id": sub_job.get("parent_id", ""),
                    "task_ids": task_ids,
                }
                results_map[tid] = sub_job_dict
                results.append(sub_job_dict)

                if sub_job_dict["type"] in ["pipeline", "group"] and task_ids:
                    to_fetch.extend(task_ids)

        # Group subtasks by their top-level ancestor
        ancestor_map = {job["id"]: [] for job in paginated_top_level}
        sub_tasks_list = results[len(paginated_top_level):]

        def get_top_level_ancestor(sub_job):
            curr = sub_job
            visited = set()
            while curr.get("parent_id") and curr["id"] not in visited:
                visited.add(curr["id"])
                p_id = curr["parent_id"]
                if p_id in ancestor_map:
                    return p_id
                parent = results_map.get(p_id)
                if not parent:
                    break
                curr = parent
            return None

        for sub_job in sub_tasks_list:
            ancestor = get_top_level_ancestor(sub_job)
            if ancestor:
                ancestor_map[ancestor].append(sub_job)

        final_results = []
        for job in paginated_top_level:
            job_descendants = ancestor_map.get(job["id"], [])
            job_total_count = 1 + len(job_descendants)
            if not final_results or len(final_results) + job_total_count <= limit:
                final_results.append(job)
                final_results.extend(job_descendants)
            else:
                break

        return final_results, total
