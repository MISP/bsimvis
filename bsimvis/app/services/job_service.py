import uuid
import time
import json
from enum import Enum
from .redis_client import get_queue_redis, get_redis


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
    INDEX_SIM = "index_sim"
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
    LLM_BATCH = "llm_batch"


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

    def _get_pool_name(self, pool_id):
        if not pool_id:
            return None
        try:
            r_kv = get_redis()
            name = r_kv.hget(f"global:pool:{pool_id}:meta", "name")
            if name:
                return name.decode("utf-8") if isinstance(name, bytes) else name
        except Exception:
            pass
        return None

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

            # Index by collection if present
            coll = job_data.get("collection")
            if coll:
                self.r.lpush(f"jobs:collection:{coll}", job_id)
                self.r.ltrim(f"jobs:collection:{coll}", 0, 999)

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

        if collection:
            self.r.lpush(f"jobs:collection:{collection}", pipeline_id)
            self.r.ltrim(f"jobs:collection:{collection}", 0, 999)

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

        if collection:
            self.r.lpush(f"jobs:collection:{collection}", group_id)
            self.r.ltrim(f"jobs:collection:{collection}", 0, 999)

        if enqueue:
            self.start_job(group_id)

        return group_id

    def enqueue_job(self, job_id, is_continuation=False):
        """Pushes a job ID onto the appropriate priority queue.

        Idempotent: a job can be enqueued from several paths (create_job, an
        explicit enqueue, and start_job re-visiting a group member). Without a
        guard the same pending job lands on the queue twice and two workers run
        it concurrently -- one indexes while the other sees the chunk data
        already consumed and returns early, releasing the pipeline barrier
        before indexing finished. The `queued` latch (cleared by the worker when
        it pops the job) ensures each pending job is enqueued at most once.
        """
        job = self.r.hgetall(f"job:{job_id}")
        jtype = job.get("type") if job else None

        # Atomic latch: hset returns 1 only when it creates the field. If it was
        # already set, skip when the job is still pending or running (already
        # queued / executing); only fall through for a terminal job being retried.
        if self.r.hset(f"job:{job_id}", "queued", "1") == 0:
            status = job.get("status")
            if status in (JobStatus.PENDING.value, JobStatus.RUNNING.value):
                return

        high_priority_types = [
            JobType.CLEAR_SIM.value,
            JobType.CLEAR_FEATURES.value,
            JobType.CLEAR_CLUSTER.value,
            JobType.CLEAR_BIN_SIM.value,
            JobType.SYNC_MILVUS.value,
            # User-initiated LLM batches are interactive work: they must not sit
            # behind a Ghidra analysis or a sim build for hours.
            JobType.LLM_BATCH.value,
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

        jtype = job.get("jtype")
        if isinstance(jtype, bytes):
            jtype = jtype.decode()
        if not jtype:
            jtype = job.get("type")
            if isinstance(jtype, bytes):
                jtype = jtype.decode()

        status = job.get("status")
        if isinstance(status, bytes):
            status = status.decode()

        if status == JobStatus.CANCELLED.value:
            return

        if status in [
            JobStatus.RUNNING.value,
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
        ]:
            if status in [JobStatus.COMPLETED.value, JobStatus.FAILED.value]:
                parent_id = job.get("parent_id")
                if parent_id:
                    if isinstance(parent_id, bytes):
                        parent_id = parent_id.decode()
                    self.advance_parent(parent_id, job_id)
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
                for i in range(current_idx):
                    prev_status = self.r.hget(f"job:{tids[i]}", "status")
                    if isinstance(prev_status, bytes):
                        prev_status = prev_status.decode()
                    if prev_status != JobStatus.COMPLETED.value:
                        return
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
                # Atomic one-shot latch: multiple workers can finish the last
                # group members concurrently and each observe all_done. HSET
                # returns 1 only for the first caller that creates the field, so
                # exactly one advances the parent. Without this the pipeline is
                # advanced twice and downstream steps (e.g. CLUSTER_POOL then
                # BUILD_POOL_BIN_SIM) run concurrently and race each other.
                if self.r.hset(f"job:{parent_id}", "barrier_fired", "1") != 1:
                    return
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

        # Extract pool name for top-level job
        pool_name = None
        coll = data.get("collection", "")
        payload = data.get("payload")
        pool_id = None
        if payload and isinstance(payload, dict):
            pool_id = payload.get("pool_id")
        if not pool_id and coll and coll.startswith("pool:"):
            pool_id = coll.split(":", 1)[1]
        if pool_id:
            pool_name = self._get_pool_name(pool_id)
        if pool_name:
            data["pool_name"] = pool_name

        if "task_ids" in data:
            tids = json.loads(data["task_ids"])
            data["task_ids"] = tids
            # Enrich with sub-task statuses
            sub_tasks = []
            for tid in tids:
                st = self.r.hgetall(f"job:{tid}")
                if st:
                    # Extract target and collection from payload
                    target = ""
                    coll = st.get("collection", "")
                    payload_raw = st.get("payload", "")
                    sub_pool_name = None
                    if payload_raw:
                        try:
                            payload = json.loads(payload_raw)
                            if not coll:
                                coll = payload.get("collection", "")
                                if not coll and "pool_id" in payload:
                                    coll = f"pool:{payload['pool_id']}"

                            sub_pool_id = payload.get("pool_id")
                            if not sub_pool_id and coll and coll.startswith("pool:"):
                                sub_pool_id = coll.split(":", 1)[1]
                            if sub_pool_id:
                                sub_pool_name = self._get_pool_name(sub_pool_id)

                            target = (
                                payload.get("md5")
                                or payload.get("file_id")
                                or payload.get("batch_uuid")
                                or ""
                            )
                        except:
                            pass

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
                            "created_at": safe_int(st.get("created_at", 0)),
                            "updated_at": safe_int(st.get("updated_at", 0)),
                            "started_at": safe_int(st.get("started_at", 0)),
                            "collection": coll,
                            "pool_name": sub_pool_name,
                            "target": target,
                            "payload": payload_raw,
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

    def is_cancelled(self, job_id):
        """True if the job was cancelled -- for long jobs to poll mid-run."""
        return self.r.hget(f"job:{job_id}", "status") == JobStatus.CANCELLED.value

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

        # ponytail: the UI polls this every 3s; fetch each job hash once, pipelined.
        pending_ids = self.r.lrange("jobs:pending", 0, 100) + self.r.lrange(
            "jobs:pending:high", 0, 100
        )
        wanted_ids = list(dict.fromkeys(list(processing_ids) + list(pending_ids)))
        pipe = self.r.pipeline(transaction=False)
        for jid in wanted_ids:
            pipe.hgetall(f"job:{jid}")
        job_hashes = {}
        for jid, job in zip(wanted_ids, pipe.execute()):
            job_hashes[jid] = {
                (k.decode() if isinstance(k, bytes) else k): (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in (job or {}).items()
            }

        total_speed = 0.0
        active_jobs_count = 0
        remaining_items = 0

        for jid in processing_ids:
            job = job_hashes.get(jid)
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

        active_jobs = []
        for jid in wanted_ids:
            job = job_hashes.get(jid)
            if not job:
                continue

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
        # Use collection index key if collection filter is passed
        if collection:
            all_job_ids = self.r.lrange(f"jobs:collection:{collection}", 0, -1)
        elif pool:
            all_job_ids = self.r.lrange(f"jobs:collection:pool:{pool}", 0, -1)
        else:
            all_job_ids = self.r.lrange("jobs:global", 0, -1)

        pool_cols = set()
        if pool:
            pool_cols = {
                c.decode() if isinstance(c, bytes) else c
                for c in self.r.smembers(f"global:pool:{pool}:collections_list")
            }
            pool_cols.add(f"pool:{pool}")

        # Optimize: if no filter is active, we can slice top-level job IDs early
        # to avoid fetching and parsing all 1000 jobs.
        has_filters = any([collection, pool, status, jtype])
        sliced_job_ids = all_job_ids
        if not has_filters:
            sliced_job_ids = all_job_ids[offset : offset + limit]

        # Batch fetch all top-level job hashes using pipeline
        pipe = self.r.pipeline(transaction=False)
        for jid in sliced_job_ids:
            pipe.hgetall(f"job:{jid}")
        raw_jobs = pipe.execute()

        # Parse payloads and build initial list of top-level job dicts
        top_level_jobs = []
        for jid, job in zip(sliced_job_ids, raw_jobs):
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
                    "started_at": safe_int(job.get("started_at", 0)),
                    "parent_id": parent_id,
                    "task_ids": task_ids,
                    "payload": job.get("payload", ""),
                }
            )

        # Collect first_task_ids to resolve collection info in a single batch pipeline
        pipeline_job_map = {}
        for job_dict in top_level_jobs:
            if (
                not job_dict["collection"]
                and job_dict["type"] in ["pipeline", "group"]
                and job_dict["task_ids"]
            ):
                pipeline_job_map[job_dict["id"]] = job_dict["task_ids"][0]

        if pipeline_job_map:
            p_ids = list(pipeline_job_map.keys())
            t_ids = [pipeline_job_map[pid] for pid in p_ids]
            pipe = self.r.pipeline(transaction=False)
            for tid in t_ids:
                pipe.hgetall(f"job:{tid}")
            raw_first_tasks = pipe.execute()

            for pid, tid, raw_task in zip(p_ids, t_ids, raw_first_tasks):
                if not raw_task:
                    continue
                first_task = {
                    k.decode() if isinstance(k, bytes) else k: (
                        v.decode() if isinstance(v, bytes) else v
                    )
                    for k, v in raw_task.items()
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

                # Update the corresponding job object
                for job_dict in top_level_jobs:
                    if job_dict["id"] == pid:
                        job_dict["collection"] = coll
                        break

        # Filter the top-level jobs post-pipeline resolution
        filtered_top_level_jobs = []
        for job_dict in top_level_jobs:
            coll = job_dict["collection"]
            # Apply filters early to top-level jobs
            if collection and coll != collection:
                # If pool is specified, allow pool-level jobs to bypass the collection filter
                if not (pool and coll == f"pool:{pool}"):
                    continue
            if pool and coll not in pool_cols:
                # Also check if payload explicitly has pool_id
                payload_pool = ""
                payload_raw = job_dict.get("payload")
                if payload_raw:
                    try:
                        payload = json.loads(payload_raw)
                        payload_pool = payload.get("pool_id", "")
                    except:
                        pass
                if payload_pool != pool:
                    continue

            if status and job_dict.get("status") != status:
                continue
            if jtype and job_dict.get("type") != jtype:
                continue

            filtered_top_level_jobs.append(job_dict)

        top_level_jobs = filtered_top_level_jobs

        # Total is the count of filtered top-level jobs
        total = len(all_job_ids) if not has_filters else len(top_level_jobs)

        # Paginate top-level jobs (skip slicing if already sliced early)
        paginated_top_level = (
            top_level_jobs
            if not has_filters
            else top_level_jobs[offset : offset + limit]
        )

        for r in paginated_top_level:
            # Try to resolve pool_name
            pool_id = None
            payload_raw = r.get("payload", "")
            coll = r.get("collection", "")

            if payload_raw:
                try:
                    payload = (
                        json.loads(payload_raw)
                        if isinstance(payload_raw, str)
                        else payload_raw
                    )
                    if isinstance(payload, dict):
                        pool_id = payload.get("pool_id")
                except:
                    pass
            if not pool_id and coll and coll.startswith("pool:"):
                pool_id = coll.split(":", 1)[1]

            if pool_id:
                r["pool_name"] = self._get_pool_name(pool_id)
            else:
                r["pool_name"] = None

            r.pop("payload", None)

        return paginated_top_level, total
