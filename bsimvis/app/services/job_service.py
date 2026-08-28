import uuid
import time
import json
import os
from enum import Enum
from redis.exceptions import WatchError
from .redis_client import get_queue_redis, get_redis
from .config_service import config_service


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
    RESPLIT_BIN_SIM = "resplit_bin_sim"
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
    LLM_CONTEXTUAL_BATCH = "llm_contextual_batch"
    LLM_FILE_ANALYSIS = "llm_file_analysis"
    LLM_PAIR_ANALYSIS = "llm_pair_analysis"


# Lease-based claims. A worker refreshes its lease while it holds a job; if the
# process dies (SIGKILL, OOM, host reset) nothing refreshes it, the lease expires
# and the reaper requeues the job. This replaces the old "sweep jobs:processing on
# startup" remedy, which could not tell a dead claim from a live one.
LEASE_TTL = 60  # seconds a claim stays valid without a refresh
LEASE_KEY = "jobs:leased"  # ZSET job_id -> expiry timestamp
WORKERS_KEY = "workers:alive"  # ZSET worker_id -> registration expiry
# Same shape as the lease: refreshed from the worker heartbeat, so a killed
# worker ages out on its own. Generous enough that one slow heartbeat (the
# worker is mid-job and the loop only ticks every LEASE_TTL/3) never drops a
# live worker off the dashboard.
WORKER_TTL = LEASE_TTL

# --- memory admission control ---------------------------------------------
# Weights are MEASURED, not hand-picked. The draft version of this listed
# cluster_*, build_bin_sim and big binaries as "heavy" -- and enrich_features,
# the job that actually killed ten workers, was not on the list. So instead each
# worker records the peak RSS it actually saw for a job type and admission uses
# that. Unmeasured types get a modest default and calibrate themselves after
# one run.
MEM_PEAK_KEY = "jobs:mem:peak"  # HASH jtype -> largest RSS observed, bytes
MEM_RESERVED_KEY = "jobs:mem:reserved"  # HASH job_id -> bytes reserved
MEM_USED_KEY = "jobs:mem:used"  # INT sum of live reservations
MEM_DEFAULT_COST = 512 * 1024**2

# Caveat worth keeping in view: tokens bound CONCURRENCY, not per-job
# footprint. Each worker has its own cgroup, so serialising five
# enrich_features jobs would not have saved any single one of them. This stops
# the fleet from collectively overcommitting the host; it is necessary, not
# sufficient.
MAX_ATTEMPTS = 3  # requeue this many times before failing the job for good
REAPER_LOCK_KEY = "jobs:reaper:lock"
PAUSE_KEY = "jobs:paused"

# --- job_log stream ---------------------------------------------------------
# job_log:<id> is a Redis Stream, not a capped LIST. A pasted-log job doing
# ~1 line/1-2s (a 20M-similarity run) blew through the old 100-line LTRIM cap
# in under 3 minutes and lost everything before that. XADD trims by count
# (approximate MAXLEN, O(1) amortized) instead of a hard cutoff, and the key
# expires on its own after LOG_STREAM_TTL of inactivity instead of living
# forever (job-system-rework-plan.md §5).
LOG_STREAM_MAXLEN = 5000
LOG_STREAM_TTL = 7 * 86400


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
    # Centralizes what used to be ~6 separate hset(job, "status", ...) call
    # sites, each of which needs to also move the job between the
    # jobs:idx:status:<status> index sets (§2.1) -- doing that as two
    # separate round trips per call site is how index/hash drift happens.
    _SET_STATUS_LUA = """
    local job_key = KEYS[1]
    local job_id = ARGV[1]
    local new_status = ARGV[2]
    local old_status = redis.call('hget', job_key, 'status')
    redis.call('hset', job_key, 'status', new_status)
    if old_status and old_status ~= new_status then
        redis.call('srem', 'jobs:idx:status:' .. old_status, job_id)
    end
    redis.call('sadd', 'jobs:idx:status:' .. new_status, job_id)
    return old_status
    """

    def __init__(self):
        self.r = get_queue_redis()

    def _set_status(self, job_id, new_status):
        """Sets job status and keeps jobs:idx:status:<status> in sync."""
        val = new_status.value if isinstance(new_status, JobStatus) else new_status
        return self.r.eval(self._SET_STATUS_LUA, 1, f"job:{job_id}", job_id, val)

    def _canonical_md5(self, payload):
        """Normalizes whichever of md5/file_md5/file_id (top-level or nested
        under file_meta) a payload used, into one name."""
        if not isinstance(payload, dict):
            return None
        md5 = payload.get("file_md5") or payload.get("md5") or payload.get("file_id")
        if not md5:
            file_meta = payload.get("file_meta") or {}
            md5 = file_meta.get("file_md5") or file_meta.get("md5")
        return md5

    def _index_job(self, job_id, job_type, status, payload):
        """Writes the §2.1 secondary indexes for a newly-created job. Called
        for every job (leaf or subtask), not just top-level units, so 'every
        job that touched this file' is answerable regardless of nesting."""
        jtype_val = job_type.value if isinstance(job_type, JobType) else job_type
        status_val = status.value if isinstance(status, JobStatus) else status
        self.r.sadd(f"jobs:idx:status:{status_val}", job_id)
        self.r.sadd(f"jobs:idx:type:{jtype_val}", job_id)
        md5 = self._canonical_md5(payload)
        if md5:
            self.r.hset(f"job:{job_id}", "file_md5", md5)
            self.r.sadd(f"jobs:idx:md5:{md5}", job_id)
        if isinstance(payload, dict) and payload.get("pool_id"):
            self.r.sadd(f"jobs:idx:pool:{payload['pool_id']}", job_id)

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

    def create_job(
        self, job_type, payload, parent_id=None, is_subtask=False, enqueue=True
    ):
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
            if "priority" in payload:
                job_data["priority"] = payload["priority"]

        # Store job metadata as a Hash
        self.r.hset(f"job:{job_id}", mapping=job_data)
        self._index_job(job_id, job_type, JobStatus.PENDING, payload)

        # Add to global list of jobs for tracking if not a subtask
        if not is_subtask:
            self.r.lpush("jobs:global", job_id)
            # Keep only the last 1000 jobs in the global list
            self.r.ltrim("jobs:global", 0, 999)
            # Uncapped recency index (§2.1) -- jobs:global's LTRIM-to-1000
            # means old jobs fall off it; the timeline never loses one.
            self.r.zadd("jobs:timeline", {job_id: timestamp})

            # Index by collection if present
            coll = job_data.get("collection")
            if coll:
                self.r.lpush(f"jobs:collection:{coll}", job_id)
                self.r.ltrim(f"jobs:collection:{coll}", 0, 999)

        # If it's not a subtask of a pipeline (or it's the first subtask), enqueue it
        if not is_subtask and enqueue:
            self.enqueue_job(job_id)

        return job_id

    def _resolve_task(self, task, parent_id):
        """Resolves a task definition into a job_id."""
        if isinstance(task, str):
            # Existing job_id
            self.r.hset(f"job:{task}", "parent_id", parent_id)
            # Remove from jobs:global since it now has a parent
            self.r.lrem("jobs:global", 0, task)
            self.r.zrem("jobs:timeline", task)
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
        self._index_job(pipeline_id, "pipeline", JobStatus.PENDING, None)
        self.r.lpush("jobs:global", pipeline_id)
        self.r.ltrim("jobs:global", 0, 999)
        self.r.zadd("jobs:timeline", {pipeline_id: timestamp})

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
        self._index_job(group_id, "group", JobStatus.PENDING, None)
        self.r.lpush("jobs:global", group_id)
        self.r.ltrim("jobs:global", 0, 999)
        self.r.zadd("jobs:timeline", {group_id: timestamp})

        if collection:
            self.r.lpush(f"jobs:collection:{collection}", group_id)
            self.r.ltrim(f"jobs:collection:{collection}", 0, 999)

        if enqueue:
            self.start_job(group_id)

        return group_id

    # --- Per-collection lane -------------------------------------------------
    # One rule, no job-type classification: for a given collection, exactly one
    # top-level unit (job/pipeline/group with no parent) runs at a time.
    # Concurrency only exists inside a group's own members. See
    # /home/thomas/.claude/plans/synthetic-dancing-harbor.md.

    _ADVANCE_LANE_LUA = """
    local nxt = redis.call('lpop', KEYS[2])
    if nxt then
      redis.call('set', KEYS[1], nxt)
    else
      redis.call('del', KEYS[1])
    end
    return nxt
    """

    def _lane_key(self, collection, suffix):
        return f"lane:{collection}:{suffix}"

    def _touch_active_lanes(self, collection):
        self.r.sadd("active_lanes", collection)

    def _maybe_clear_active_lanes(self, collection):
        if (
            not self.r.exists(self._lane_key(collection, "active"))
            and self.r.llen(self._lane_key(collection, "pending")) == 0
            and not self.r.exists(self._lane_key(collection, "wave_deadline"))
        ):
            self.r.srem("active_lanes", collection)

    def submit_to_lane(self, collection, tasks_or_id, priority=False):
        """Submits a top-level unit (a task list, or an already-created job/
        pipeline/group id) to run for this collection. Dispatches immediately
        if the lane is idle; otherwise queues (front if priority) and runs
        once the currently active unit finishes. Always returns a pollable
        id -- nothing is rejected, dropped, or silently merged."""
        if isinstance(tasks_or_id, str):
            unit_id = tasks_or_id
        else:
            unit_id = self.create_pipeline(tasks_or_id, enqueue=False)

        self.r.hset(f"job:{unit_id}", "lane_collection", collection)
        self._touch_active_lanes(collection)
        active_key = self._lane_key(collection, "active")
        if self.r.set(active_key, unit_id, nx=True):
            self.start_job(unit_id)
        else:
            pending_key = self._lane_key(collection, "pending")
            if priority:
                self.r.lpush(pending_key, unit_id)
            else:
                self.r.rpush(pending_key, unit_id)
        return unit_id

    def advance_lane(self, collection):
        """Called whenever a collection's active lane unit reaches a terminal
        state. Promotes the next pending unit, if any. The single
        serialization point -- no job-type awareness at all."""
        active_key = self._lane_key(collection, "active")
        pending_key = self._lane_key(collection, "pending")
        next_id = self.r.eval(self._ADVANCE_LANE_LUA, 2, active_key, pending_key)
        if next_id:
            next_id = next_id.decode() if isinstance(next_id, bytes) else next_id
            self.start_job(next_id)
        self._maybe_clear_active_lanes(collection)

    def open_or_extend_wave(self, collection, job_id, debounce_seconds):
        """Records an upload's job_id into the collection's currently-open
        debounce window, purely for later grouping -- it does NOT delay the
        job itself. The caller enqueues it normally (analysis starts
        immediately, fully parallel across workers, same as today); this only
        controls when the collection's next auto-cluster fires. Deliberately
        a fixed deadline from the first arrival (SETNX), not a sliding one --
        otherwise a collection under steady upload traffic would never seal."""
        self._touch_active_lanes(collection)
        wave_key = self._lane_key(collection, "wave")
        deadline_key = self._lane_key(collection, "wave_deadline")
        self.r.rpush(wave_key, job_id)
        self.r.setnx(deadline_key, int(time.time() * 1000) + debounce_seconds * 1000)

    def seal_wave(self, collection):
        """Seals the open wave (if any) into a group, wraps it with the
        standard cluster/bin_sim rebuild steps, and submits that pipeline to
        the lane. This *is* automatic clustering-after-batch -- no separate
        finalize call needed.

        Members were already enqueued and may have started, or even finished,
        running before this fires (open_or_extend_wave never delays them) --
        create_group(..., enqueue=True) is required here, not enqueue=False:
        for an already-terminal member, start_job's own status recheck
        retroactively fires advance_parent now that parent_id is set, exactly
        as if it had just completed. Without enqueue=True a fast file that
        finishes before the debounce window closes would leave the group's
        barrier permanently unfired."""
        wave_key = self._lane_key(collection, "wave")
        deadline_key = self._lane_key(collection, "wave_deadline")
        members = self.r.lrange(wave_key, 0, -1)
        self.r.delete(wave_key, deadline_key)
        if not members:
            self._maybe_clear_active_lanes(collection)
            return None
        members = [m.decode() if isinstance(m, bytes) else m for m in members]
        group_id = self.create_group(members, enqueue=True)
        # Lazy import: cluster.py imports JobService, so a module-level import
        # here would be circular.
        from bsimvis.app.routes.cluster import build_rebuild_all_tasks

        algo = config_service.get("similarity.algo", "unweighted_cosine")
        tasks = [group_id] + build_rebuild_all_tasks(collection, algo, skip_sim=False)
        return self.submit_to_lane(collection, tasks)

    def tick_lanes(self):
        """Idle-loop sweep (called from Worker.run()'s idle branch): seals any
        wave past its deadline, and self-heals a lane whose active unit has
        gone stale (crashed worker) instead of relying on a fixed lock TTL."""
        lane_stale_ms = config_service.get("clustering.lane_stale_seconds", 1800) * 1000
        now_ms = int(time.time() * 1000)

        for collection in self.r.smembers("active_lanes"):
            collection = (
                collection.decode() if isinstance(collection, bytes) else collection
            )
            deadline_raw = self.r.get(self._lane_key(collection, "wave_deadline"))
            if deadline_raw and now_ms - safe_int(deadline_raw) >= 0:
                self.seal_wave(collection)

            active_id = self.r.get(self._lane_key(collection, "active"))
            if not active_id:
                self._maybe_clear_active_lanes(collection)
                continue
            active_id = (
                active_id.decode() if isinstance(active_id, bytes) else active_id
            )
            job = self.r.hgetall(f"job:{active_id}")
            status = job.get("status")
            updated_at = job.get("updated_at")
            if (
                status in (JobStatus.PENDING.value, JobStatus.RUNNING.value)
                and updated_at
                and now_ms - safe_int(updated_at) > lane_stale_ms
            ):
                self.add_log(
                    active_id,
                    "Lane self-heal: active unit stale (worker likely crashed), promoting next.",
                )
                self.advance_lane(collection)

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

        # Held by its own pause or an ancestor's: record that it wants to be
        # queued and stop here. Catches the continuation case -- a paused
        # pipeline whose running stage finishes and tries to start the next one.
        if self.is_job_paused(job_id):
            self.r.hdel(f"job:{job_id}", "queued")
            self.r.hset(f"job:{job_id}", "paused_queued", "1")
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
            JobType.LLM_CONTEXTUAL_BATCH.value,
            JobType.LLM_FILE_ANALYSIS.value,
            JobType.LLM_PAIR_ANALYSIS.value,
        ]

        if jtype in high_priority_types or job.get("priority") == "high":
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
            self._set_status(job_id, JobStatus.RUNNING)
            # Composite units never went through worker._execute_job, so
            # unlike leaf jobs they never got a started_at at all -- that's
            # why jobs.js fell back to created_at (enqueue time, not
            # execution time) for duration (§3.7).
            self.r.hset(f"job:{job_id}", "started_at", str(int(time.time() * 1000)))

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
        self._set_status(job_id, JobStatus.COMPLETED)
        self.r.hset(f"job:{job_id}", "completed_at", str(int(time.time() * 1000)))
        self.update_progress(job_id, 100)

        data = self.r.hgetall(f"job:{job_id}")
        parent_id = data.get("parent_id")
        if parent_id:
            self.advance_parent(parent_id, job_id)
        else:
            collection = data.get("lane_collection")
            if collection:
                self.advance_lane(collection)

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
        self._set_status(job_id, JobStatus.FAILED)
        self.r.hset(
            f"job:{job_id}",
            mapping={"error": error_msg, "completed_at": str(int(time.time() * 1000))},
        )
        self.add_log(job_id, f"Execution error: {error_msg}")

        # Failure cascades down as well as up: without this the remaining children
        # of a failed pipeline stay queued and still run.
        raw_tids = self.r.hget(f"job:{job_id}", "task_ids")
        if raw_tids:
            for tid in json.loads(raw_tids):
                child_status = self.r.hget(f"job:{tid}", "status")
                if child_status in (
                    JobStatus.PENDING.value,
                    JobStatus.RUNNING.value,
                ):
                    self.cancel_job(tid)

        parent_id = self.r.hget(f"job:{job_id}", "parent_id")
        if parent_id:
            parent_type = self.r.hget(f"job:{parent_id}", "type")
            if parent_type == "group":
                # For groups, we don't fail the group instantly. We wait for other tasks.
                self.advance_parent(parent_id, job_id)
            else:
                self.fail_job(parent_id, f"Failed because sub-task {job_id} failed.")
        else:
            collection = self.r.hget(f"job:{job_id}", "lane_collection")
            if collection:
                self.advance_lane(collection)

    # ------------------------------------------------------------------
    # Leases: crash recovery for claimed jobs
    # ------------------------------------------------------------------

    def claim_lease(self, job_id, owner, ttl=LEASE_TTL):
        """Records a lease for a claimed job. Called right after the queue pop."""
        self.r.zadd(LEASE_KEY, {job_id: time.time() + ttl})
        self.r.hset(f"job:{job_id}", "lease_owner", owner)

    def refresh_lease(self, job_id, ttl=LEASE_TTL):
        """Extends a live lease. No-op if the reaper already took the job away."""
        # XX: never resurrect a lease the reaper has already released, or the job
        # would run twice -- once here, once from the requeue.
        return self.r.zadd(LEASE_KEY, {job_id: time.time() + ttl}, xx=True)

    def release_lease(self, job_id):
        """Drops the lease and the in-flight marker. Safe to call twice."""
        self.r.zrem(LEASE_KEY, job_id)
        # count 0: a job enqueued twice would otherwise leave a permanent orphan.
        self.r.lrem("jobs:processing", 0, job_id)
        # The memory reservation has the same lifetime as the claim.
        self.release_admission(job_id)

    # ------------------------------------------------------------------
    # Worker registry: an honest count of live worker processes
    # ------------------------------------------------------------------

    def register_worker(self, worker_id, ttl=WORKER_TTL):
        """Marks a worker alive until `ttl` seconds from now.

        Called from the worker's heartbeat, so a worker that is OOM-killed
        simply stops refreshing and ages out. Same trick as the leases: nothing
        here relies on a `finally` block that SIGKILL never runs.
        """
        self.r.zadd(WORKERS_KEY, {worker_id: time.time() + ttl})

    def unregister_worker(self, worker_id):
        """Drops a worker from the registry on a clean shutdown."""
        self.r.zrem(WORKERS_KEY, worker_id)

    def count_active_workers(self, now=None):
        """Number of workers whose registration has not expired.

        This is the real fleet size. The dashboard used to show the count of
        active *jobs* instead, so a dead fleet holding stale `running` jobs
        looked like a busy system -- which is why the outage went unnoticed.
        """
        now = time.time() if now is None else now
        # Trim first so an unnoticed dead fleet cannot inflate the count forever.
        self.r.zremrangebyscore(WORKERS_KEY, 0, now)
        return self.r.zcard(WORKERS_KEY)

    def list_active_workers(self, now=None):
        """Live worker ids, soonest-to-expire first."""
        now = time.time() if now is None else now
        self.r.zremrangebyscore(WORKERS_KEY, 0, now)
        return [
            w.decode() if isinstance(w, bytes) else w
            for w in self.r.zrange(WORKERS_KEY, 0, -1)
        ]

    # ------------------------------------------------------------------
    # Memory admission control
    # ------------------------------------------------------------------

    def memory_budget(self):
        """Bytes of worker memory the fleet may collectively reserve."""
        env = os.getenv("JOB_MEMORY_BUDGET_MB")
        if env:
            return int(float(env) * 1024**2)
        # Same reservation as launch_tmux.sh: leave 8 GB for kvrocks, redis and
        # the desktop, offer the rest to jobs.
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) * 1024
                        return max(total - 8 * 1024**3, 1024**3)
        except OSError:
            pass
        return 8 * 1024**3

    def record_job_peak(self, jtype, peak_bytes):
        """Remembers the largest RSS ever seen for a job type."""
        if not jtype or not peak_bytes:
            return
        prev = safe_int(self.r.hget(MEM_PEAK_KEY, jtype))
        if peak_bytes > prev:
            self.r.hset(MEM_PEAK_KEY, jtype, int(peak_bytes))

    def job_cost(self, jtype):
        """Measured peak for this job type, or a default until one exists."""
        return safe_int(self.r.hget(MEM_PEAK_KEY, jtype), MEM_DEFAULT_COST) or (
            MEM_DEFAULT_COST
        )

    def try_admit(self, job_id, jtype):
        """Reserves this job's measured cost, or refuses if the fleet is full.

        INCRBY-then-roll-back: two workers racing can never both slip past the
        budget, because the increment is what decides.
        """
        cost = self.job_cost(jtype)
        used = self.r.incrby(MEM_USED_KEY, cost)
        # `used == cost` means nothing else was reserved. Always admit then,
        # even if the job alone exceeds the budget -- otherwise a single
        # expensive job type would deadlock the queue forever.
        if used <= self.memory_budget() or used == cost:
            self.r.hset(MEM_RESERVED_KEY, job_id, cost)
            return True
        self.r.incrby(MEM_USED_KEY, -cost)
        return False

    def release_admission(self, job_id):
        """Gives a job's reservation back. Safe to call twice."""
        cost = safe_int(self.r.hget(MEM_RESERVED_KEY, job_id))
        if cost:
            self.r.hdel(MEM_RESERVED_KEY, job_id)
            self.r.incrby(MEM_USED_KEY, -cost)

    def resync_admissions(self):
        """Rebuilds the used counter from the live reservations.

        A worker killed between INCRBY and its release leaks its reservation,
        and enough leaks would starve the fleet into a permanent standstill.
        The reaper calls this, so the budget self-heals the same way leases do.
        """
        total = sum(safe_int(v) for v in (self.r.hvals(MEM_RESERVED_KEY) or []))
        self.r.set(MEM_USED_KEY, total)
        return total

    def _classify_worker_death(self, worker_name):
        """Reads what scripts/worker-supervisor.sh recorded about a dead worker.

        The supervisor is the only thing that ever sees an exit code -- a
        worker killed by SIGKILL/OOM never runs its `finally`, and the reaper
        only observes a silently-expired lease. Without this, an OOM kill and
        a genuinely frozen process look identical from Redis alone. Only
        classify what the supervisor actually recorded (worker_exit:<name>,
        written after every worker exit); don't guess at a cause with no
        signal behind it.
        """
        if not worker_name:
            return "no lease owner recorded (worker died before claiming)"
        info = self.r.hgetall(f"worker_exit:{worker_name}")
        if not info:
            return "no exit signal recorded (frozen process, or supervisor not running)"
        rc = info.get("rc")
        peak = safe_int(info.get("peak"))
        peak_str = f", peak={peak / 1024**3:.2f} GiB" if peak else ""
        if rc == "137":
            return f"OOM-killed by supervisor (rc=137{peak_str})"
        return f"worker exited rc={rc}{peak_str}"

    def reap_expired(self, now=None):
        """Requeues jobs whose worker died, and clears stale in-flight entries.

        Returns (requeued, failed, cleaned). Held under a short lock so a fleet
        starting together does not requeue the same job several times.
        """
        if not self.r.set(REAPER_LOCK_KEY, "1", nx=True, ex=30):
            return (0, 0, 0)

        now = time.time() if now is None else now
        try:
            expired = list(self.r.zrangebyscore(LEASE_KEY, 0, now))

            # Entries sitting in jobs:processing with no lease at all: either a
            # worker died between the queue pop and claim_lease, or they predate
            # leases entirely (the historical jobs:processing leak).
            leased = set(self.r.zrange(LEASE_KEY, 0, -1))
            for job_id in self.r.lrange("jobs:processing", 0, -1):
                if job_id not in leased and job_id not in expired:
                    expired.append(job_id)

            requeued = failed = cleaned = 0
            for job_id in dict.fromkeys(expired):
                job = self.r.hgetall(f"job:{job_id}")
                status = job.get("status") if job else None

                if not job or status in (
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.CANCELLED.value,
                ):
                    # Already resolved -- the list entry is just stale bookkeeping.
                    self.release_lease(job_id)
                    cleaned += 1
                    continue

                death = self._classify_worker_death(job.get("lease_owner"))
                self.r.hset(f"job:{job_id}", "failure_detail", death)
                self.release_lease(job_id)

                # MAX_ATTEMPTS predates jobs being resumable. enrich_features
                # now checkpoints every batch, so a job could be OOM-killed
                # three times while permanently enriching thousands of features
                # each time and still be abandoned -- which is what happened.
                #
                # A job that advanced since its last claim is slow, not poison.
                # The watermark only ever moves forward, so a job that stops
                # advancing still fails after MAX_ATTEMPTS: the counter now
                # targets jobs that make no progress rather than jobs that need
                # more than three goes.
                processed = safe_int(job.get("processed_items"))
                watermark = safe_int(job.get("attempts_progress"))
                if processed > watermark:
                    self.r.hset(
                        f"job:{job_id}",
                        mapping={"attempts_progress": processed, "attempts": 0},
                    )
                    self.add_log(
                        job_id,
                        f"Progressed to {processed} items since the last attempt; "
                        "retry counter reset.",
                    )
                    attempts = 0
                else:
                    attempts = self.r.hincrby(f"job:{job_id}", "attempts", 1)
                if attempts > MAX_ATTEMPTS:
                    self.add_log(
                        job_id,
                        f"Abandoned after {attempts - 1} attempts (worker kept dying). "
                        f"Last death: {death}",
                    )
                    self.fail_job(
                        job_id,
                        f"Lease expired {attempts - 1} times; giving up. Last death: {death}",
                    )
                    failed += 1
                    continue

                # The `queued` latch is what stops a re-enqueue, and the status is
                # still `running` from the dead worker. Both must be reset first.
                self.r.hdel(f"job:{job_id}", "queued", "lease_owner")
                self._set_status(job_id, JobStatus.PENDING)
                self.add_log(
                    job_id,
                    f"Lease expired; requeued (attempt {attempts + 1}). "
                    f"Cause: {death}",
                )
                self.enqueue_job(job_id)
                requeued += 1

            # Drop memory reservations for jobs that are no longer in flight.
            # A worker killed between INCRBY and its release would otherwise
            # leak budget permanently, and enough leaks starve the whole fleet
            # into a standstill that looks exactly like the outage we are
            # fixing. Then rebuild the counter from what is actually held.
            in_flight = set(self.r.lrange("jobs:processing", 0, -1))
            for job_id in list(self.r.hkeys(MEM_RESERVED_KEY) or []):
                job_id = job_id.decode() if isinstance(job_id, bytes) else job_id
                if job_id not in in_flight:
                    self.r.hdel(MEM_RESERVED_KEY, job_id)
            self.resync_admissions()

            return (requeued, failed, cleaned)
        finally:
            self.r.delete(REAPER_LOCK_KEY)

    # ------------------------------------------------------------------
    # Pause / resume
    # ------------------------------------------------------------------

    def is_paused(self):
        """True when workers should finish their current job and stop claiming."""
        return bool(self.r.exists(PAUSE_KEY))

    def set_paused(self, paused):
        if paused:
            self.r.set(PAUSE_KEY, "1")
        else:
            self.r.delete(PAUSE_KEY)
        return self.is_paused()

    def set_job_paused(self, job_id, paused):
        """Holds one job/group/pipeline out of scheduling, leaving the rest alone.

        Stored as a field rather than a JobStatus: a paused job is still
        pending/running and must keep aggregating into its parent's progress
        exactly as before. A new terminal-looking status would have to be taught
        to every status filter and rollup in the app.

        Pausing pulls the paused subtree's queued work off the pending queues so
        the freed capacity goes to other jobs immediately. Leaving it queued
        would make workers pop it, notice the flag and push it back, burning
        claim cycles on work that cannot run.
        """
        if not self.r.exists(f"job:{job_id}"):
            return None
        if paused:
            self.r.hset(f"job:{job_id}", "paused", "1")
            self._dequeue_paused_subtree(job_id)
        else:
            self.r.hdel(f"job:{job_id}", "paused")
            self._requeue_paused_subtree(job_id)
        return bool(paused)

    def _dequeue_paused_subtree(self, job_id, seen=None):
        """Takes the subtree's queued-but-not-started work off the pending queues.

        Each removed job is tagged `paused_queued` so resume can put back exactly
        what was taken. That tag is the whole point: a pipeline has many pending
        stages but only the current one is queued, so resuming by "enqueue every
        pending descendant" would fire the entire pipeline in parallel.
        """
        seen = seen if seen is not None else set()
        if job_id in seen:
            return
        seen.add(job_id)

        job = self.r.hgetall(f"job:{job_id}")
        if not job:
            return

        removed = self.r.lrem("jobs:pending", 0, job_id) or 0
        removed += self.r.lrem("jobs:pending:high", 0, job_id) or 0
        if removed:
            # The `queued` latch must go too, or the resume enqueue is treated as
            # a duplicate and silently dropped -- the same trap worker._requeue hits.
            self.r.hdel(f"job:{job_id}", "queued")
            self.r.hset(f"job:{job_id}", "paused_queued", "1")

        for tid in self._task_ids(job):
            self._dequeue_paused_subtree(tid, seen)

    def _requeue_paused_subtree(self, job_id, seen=None):
        """Puts back exactly the work that pausing took off the queues."""
        seen = seen if seen is not None else set()
        if job_id in seen:
            return
        seen.add(job_id)

        job = self.r.hgetall(f"job:{job_id}")
        if not job:
            return

        # A job still held by an ancestor's pause stays down; the outer resume
        # is what will release it.
        if job.get("paused_queued") and not self.is_job_paused(job_id):
            self.r.hdel(f"job:{job_id}", "paused_queued")
            if job.get("status") == JobStatus.PENDING.value:
                self.enqueue_job(job_id)

        for tid in self._task_ids(job):
            self._requeue_paused_subtree(tid, seen)

    @staticmethod
    def _task_ids(job):
        raw = job.get("task_ids")
        if not raw:
            return []
        try:
            tids = json.loads(raw)
            return tids if isinstance(tids, list) else []
        except Exception:
            return []

    def is_job_paused(self, job_id):
        """True when this job or any ancestor is paused.

        Walking up is what makes pausing a group meaningful: the group itself is
        never claimed by a worker, only its leaves are, so the leaves are where
        the flag has to be observed. Depth is bounded by the pipeline/group
        nesting (a handful), and the walk is capped in case parent_id ever loops.
        """
        seen = set()
        while job_id and job_id not in seen:
            seen.add(job_id)
            job = self.r.hgetall(f"job:{job_id}")
            if not job:
                return False
            if job.get("paused"):
                return True
            job_id = job.get("parent_id")
        return False

    # ------------------------------------------------------------------
    # Task splicing
    # ------------------------------------------------------------------

    def splice_tasks(self, parent_id, after_id, new_tids, retries=10):
        """Inserts task ids into a parent's task_ids after `after_id`, atomically.

        task_ids is a JSON blob, so a plain read-modify-write loses one of two
        concurrent splices (chunks arrive in parallel). WATCH makes the write
        fail instead of silently dropping tasks.
        """
        if not new_tids:
            return True
        # new_tids is (jtype, payload) task defs, same shape create_pipeline/
        # create_group take -- task_ids stores resolved job-id strings, so these
        # need the same _resolve_task() pass (creates the child job, points its
        # parent_id at parent_id) before they're spliced in. Resolve once, up
        # front: doing it inside the WATCH retry loop would create a duplicate
        # orphaned child job on every WatchError retry.
        resolved_tids = [self._resolve_task(t, parent_id) for t in new_tids]
        key = f"job:{parent_id}"
        for _ in range(retries):
            try:
                with self.r.pipeline() as pipe:
                    pipe.watch(key)
                    raw = pipe.hget(key, "task_ids")
                    if raw is None:
                        pipe.unwatch()
                        return False
                    existing = json.loads(raw)
                    try:
                        idx = existing.index(after_id)
                        updated = (
                            existing[: idx + 1] + resolved_tids + existing[idx + 1 :]
                        )
                    except ValueError:
                        updated = existing + resolved_tids
                    pipe.multi()
                    pipe.hset(key, "task_ids", json.dumps(updated))
                    pipe.execute()
                return True
            except WatchError:
                continue
        return False

    def get_job_status(self, job_id):
        """Returns the full job or pipeline status."""
        data = self.r.hgetall(f"job:{job_id}")
        if not data:
            return None

        data["tier"] = 2 if data.get("parent_id") else 1

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
                                or payload.get("file_md5")
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

        # Fetch logs. The stream also carries bare progress ticks (no
        # `message` field) for the future SSE tail -- the log view only
        # wants the message-bearing entries, same as the old capped LIST.
        entries = self.r.xrange(f"job_log:{job_id}")
        logs = [
            f"[{fields.get('ts', '')}] {fields['message']}"
            for _id, fields in entries
            if fields.get("message")
        ]
        logs.reverse()  # newest-first, matching the old LPUSH/lrange order
        data["logs"] = logs

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

        self._set_status(job_id, JobStatus.CANCELLED)
        self.r.hset(f"job:{job_id}", "completed_at", str(int(time.time() * 1000)))

        # Remove from pending queues to update stats immediately
        self.r.lrem("jobs:pending", 0, job_id)
        self.r.lrem("jobs:pending:high", 0, job_id)

        self.add_log(job_id, "Job cancelled by user.")

        if not data.get("parent_id"):
            collection = data.get("lane_collection")
            if collection:
                self.advance_lane(collection)

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

    def add_log(self, job_id, message, level="info"):
        """Adds a structured log entry for a job. See LOG_STREAM_* above."""
        timestamp = int(time.time() * 1000)
        key = f"job_log:{job_id}"
        self.r.xadd(
            key,
            {"ts": timestamp, "level": level, "message": message},
            maxlen=LOG_STREAM_MAXLEN,
            approximate=True,
        )
        self.r.expire(key, LOG_STREAM_TTL)
        self.r.hset(f"job:{job_id}", "updated_at", timestamp)

    def update_progress(
        self,
        job_id,
        progress=None,
        message=None,
        processed=None,
        total=None,
        phase=None,
        speed_current=None,
        rss_current=None,
        rss_peak=None,
        level="info",
    ):
        """Updates progress (0-100) and/or phase, and appends a stream entry.

        `processed`/`total` are what make the throughput fields on
        /api/jobs/stats real. Only similarity_service ever wrote them, so during
        an enrich_features drain every speed/ETA field on the dashboard read
        zero and the only way to tell the queue had stalled was polling
        pending_jobs by hand. Handlers that know their item counts should pass
        them; `speed_avg` is derived here so no caller has to time itself --
        `speed_current` is an optional instantaneous rate a caller can supply
        if it's already tracking one.

        The progress fields ride the same job_log stream as log lines
        (job-system-rework-plan.md §5): mem_util.phase() is the shared
        checkpoint API built on this -- one call updates phase, progress,
        item counts and RSS in a single write, instead of a separate hset
        and a separate add_log the way handlers used to do it by hand.
        """
        fields = {}
        if progress is not None:
            fields["progress"] = progress
        if total is not None:
            fields["total_items"] = str(total)
        speed_avg = None
        if processed is not None:
            fields["processed_items"] = str(processed)
            started = safe_int(self.r.hget(f"job:{job_id}", "started_at"))
            elapsed = time.time() - started / 1000.0 if started else 0
            if elapsed > 0:
                speed_avg = processed / elapsed
                fields["speed"] = f"{speed_avg:.2f}"
        if phase is not None:
            fields["phase"] = phase
        timestamp = int(time.time() * 1000)
        fields["updated_at"] = timestamp
        self.r.hset(f"job:{job_id}", mapping=fields)

        entry = {"ts": timestamp, "level": level}
        if progress is not None:
            entry["progress"] = progress
        if message:
            entry["message"] = message
        if processed is not None:
            entry["processed"] = processed
        if total is not None:
            entry["total"] = total
        if speed_avg is not None:
            entry["speed_avg"] = f"{speed_avg:.2f}"
        if speed_current is not None:
            entry["speed_current"] = f"{speed_current:.2f}"
        if phase is not None:
            entry["phase"] = phase
        if rss_current is not None:
            entry["rss_current"] = rss_current
        if rss_peak is not None:
            entry["rss_peak"] = rss_peak
        key = f"job_log:{job_id}"
        self.r.xadd(key, entry, maxlen=LOG_STREAM_MAXLEN, approximate=True)
        self.r.expire(key, LOG_STREAM_TTL)

        # If it has a parent pipeline, update the pipeline's overall progress
        parent_id = self.r.hget(f"job:{job_id}", "parent_id")
        if parent_id:
            self._update_pipeline_aggregate_progress(parent_id)

    def _update_pipeline_aggregate_progress(self, pipeline_id):
        """Recalculates pipeline progress based on subtasks.

        Weighted by each child's total_items when children report sizes
        (a huge cluster_pool job and three quick idx_* jobs don't count
        equally), falling back to an equal-weight average only when none
        do. Then walks up to the grandparent too, not just one hop --
        without this a change three levels down never reaches the
        top-level unit's progress bar (job-system-rework-plan.md §3.7).
        """
        pipe_data = self.r.hgetall(f"job:{pipeline_id}")
        if not pipe_data or "task_ids" not in pipe_data:
            return

        tids = json.loads(pipe_data["task_ids"])
        if not tids:
            return

        pipe = self.r.pipeline(transaction=False)
        for tid in tids:
            pipe.hmget(f"job:{tid}", "progress", "total_items")
        rows = pipe.execute()

        total_p = 0
        weighted_num = 0
        weighted_den = 0
        for progress_raw, total_items_raw in rows:
            p = safe_int(progress_raw, 0)
            total_p += p
            sz = safe_int(total_items_raw, 0)
            if sz > 0:
                weighted_num += sz * p
                weighted_den += sz

        if weighted_den > 0:
            agg_progress = weighted_num // weighted_den
        else:
            agg_progress = total_p // len(tids)

        self.r.hset(f"job:{pipeline_id}", "progress", agg_progress)
        self.r.hset(f"job:{pipeline_id}", "updated_at", int(time.time() * 1000))

        parent_id = pipe_data.get("parent_id")
        if parent_id:
            self._update_pipeline_aggregate_progress(parent_id)

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
        # Longest per-job ETA derived from elapsed time vs progress percent, for
        # handlers that report no item counts. Without it the queue could be
        # visibly draining while global_eta sat at 0.
        progress_eta = 0.0
        now_s = time.time()

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

            pct = safe_int(job.get("progress", 0))
            started = safe_int(job.get("started_at", 0))
            if 0 < pct < 100 and started:
                elapsed = now_s - started / 1000.0
                if elapsed > 0:
                    progress_eta = max(progress_eta, elapsed * (100 - pct) / pct)

        # Average speed
        avg_speed = total_speed / active_jobs_count if active_jobs_count > 0 else 0

        # Item counts when we have them, elapsed-vs-percent otherwise.
        if total_speed > 0 and remaining_items > 0:
            global_eta = remaining_items / total_speed
        else:
            global_eta = progress_eta

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
            # A count of live worker processes, not of active jobs. The two
            # differ exactly when it matters: a dead fleet still holding
            # `running` jobs now reads 0 workers instead of looking busy.
            "active_workers": self.count_active_workers(),
            "active_jobs_count": active_jobs_count,
            "pending_jobs": pending_count,
            "avg_speed": round(avg_speed, 2),
            "total_speed": round(total_speed, 2),
            "remaining_items": remaining_items,
            "global_eta": int(global_eta),
            "active_collections": list(active_collections),
            "active_jobs": active_jobs,
        }

    def _root_job_id(self, job_id):
        """Walks parent_id up to the top-level unit -- list_jobs is unit-
        oriented (rows are chain/group/chord roots), so an md5 hit on a
        leaf/subtask still needs to surface as its containing unit."""
        current = job_id
        seen = set()
        while current not in seen:
            seen.add(current)
            parent = self.r.hget(f"job:{current}", "parent_id")
            if isinstance(parent, bytes):
                parent = parent.decode()
            if not parent:
                return current
            current = parent
        return current

    def list_jobs(
        self,
        limit=100,
        offset=0,
        collection=None,
        pool=None,
        status=None,
        jtype=None,
        tier=None,
        md5=None,
    ):
        """Returns a paged list of jobs and the total count."""
        # md5 -- "every job that ever touched this file" -- resolves via the
        # jobs:idx:md5 index (§2.1) instead of the full-scan-and-filter every
        # other branch here still does; there was no way to answer this query
        # at all before that index existed.
        if md5:
            raw_ids = {
                i.decode() if isinstance(i, bytes) else i
                for i in self.r.smembers(f"jobs:idx:md5:{md5}")
            }
            root_ids = {self._root_job_id(i) for i in raw_ids}
            pipe = self.r.pipeline(transaction=False)
            for rid in root_ids:
                pipe.hget(f"job:{rid}", "created_at")
            created_ats = pipe.execute()
            all_job_ids = [
                rid
                for rid, _ in sorted(
                    zip(root_ids, created_ats),
                    key=lambda pair: safe_int(pair[1], 0),
                    reverse=True,
                )
            ]
        # Use collection index key if collection filter is passed
        elif collection:
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
        has_filters = any([collection, pool, status, jtype, tier, md5])
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
                    "completed_at": safe_int(job.get("completed_at", 0)),
                    "parent_id": parent_id,
                    "task_ids": task_ids,
                    "payload": job.get("payload", ""),
                    # Visibility tier, derived from root-ness rather than a type
                    # table: a job the user started has no parent run (tier 1),
                    # anything spawned underneath one is internal (tier 2). The
                    # same type is legitimately both depending on context.
                    "tier": 2 if parent_id else 1,
                    "attempts": safe_int(job.get("attempts", 0)),
                    "paused": bool(job.get("paused")),
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
            if tier and job_dict.get("tier") != safe_int(tier):
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
