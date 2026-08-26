import time
import json
import logging
import signal
import subprocess
import sys
import os
import threading
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from bsimvis.app.services.redis_client import get_queue_redis, get_redis, get_raw_redis
from bsimvis.app.services.index_service import update_file_status
from bsimvis.app.services.job_service import JobService, JobStatus, JobType, LEASE_TTL
from bsimvis.app.services.processing_service import ProcessingService
from bsimvis.app.services.feature_service import FeatureService
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.bin_sim_service import bin_sim_service
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.timer_service import job_timer
from bsimvis.app.services.cluster_service import cluster_service
from bsimvis.app.services.bin_cluster_service import bin_cluster_service
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.metadata_service import MetadataService

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def _current_rss():
    """Peak RSS in bytes since the last reset, from the kernel's own counter."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def _make_preferred_oom_victim():
    """Raises this process's oom_score_adj so the kernel picks it before kvrocks.

    Every process here inherits oom_score_adj=+200, and kvrocks is the largest
    RSS in the session, so under real host pressure the kernel killed the
    datastore first. An unprivileged process may only RAISE its own score, so
    kvrocks cannot protect itself -- the worker volunteers instead. A killed
    worker is recoverable (the reaper requeues its job); kvrocks is not.

    Note this cannot be done with `systemd-run --scope -p OOMScoreAdjust=`:
    that is an exec property and systemd rejects it on a scope unit.
    """
    adj = os.getenv("WORKER_OOM_SCORE_ADJ", "1000")
    try:
        with open("/proc/self/oom_score_adj", "w") as f:
            f.write(str(adj))
        return True
    except OSError as e:
        logging.warning(f"[!] Could not set oom_score_adj={adj}: {e}")
        return False


def _reset_peak_rss():
    """Clears VmHWM so the next reading is this job's peak, not the worker's.

    Linux exposes exactly this via clear_refs, which beats sampling: a job that
    balloons and frees between two heartbeats would otherwise be recorded as
    cheap. Best-effort -- sampling in the heartbeat covers kernels that refuse.
    """
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
        return True
    except OSError:
        return False


class Worker:
    def __init__(self, name="worker-1"):
        self.name = name
        # launch_tmux.sh used to start every worker without --name, so a whole
        # fleet registered as "worker-1". The pid keeps ids unique even if that
        # regresses, and makes lease_owner point at a process you can actually
        # find.
        self.id = f"{name}-{os.getpid()}"
        _make_preferred_oom_victim()
        self.r_queue = get_queue_redis()
        self.r_data = get_redis()
        self.r_raw = get_raw_redis()
        self.job_service = JobService()
        self.processing_service = ProcessingService(self.r_data)
        self.feature_service = FeatureService(self.r_data)

        # Initialize Lua scripts for this process
        lua_manager.init_app()

        # No JVM here on purpose. Ghidra analysis runs in a child process
        # (bsimvis/ghidra_job.py), so the worker does not carry a 1.3-2.4 GB
        # JVM floor through every job that never touches Ghidra. Under a 3 GB
        # cgroup cap that floor left enrich_features roughly 0.6 GB to work in.

        self.similarity_service = SimilarityService(self.r_data)
        self.metadata_service = MetadataService(self.r_data)
        self.running = True
        self.current_job_id = None
        self._last_reap = 0.0
        self._job_peak_rss = 0

    def _reap(self, interval=30):
        """Runs the lease reaper, at most once per `interval` per worker."""
        now = time.time()
        if now - self._last_reap < interval:
            return
        self._last_reap = now
        try:
            requeued, failed, cleaned = self.job_service.reap_expired()
            if requeued or failed or cleaned:
                logging.info(
                    f"[*] Reaper: {requeued} requeued, {failed} failed, {cleaned} stale entries cleared."
                )
        except Exception as e:
            logging.warning(f"[!] Reaper error: {e}")

    def stop(self, signum, frame):
        logging.info(f"[*] Worker {self.name} received stop signal...")
        self.running = False

    def _heartbeat_loop(self):
        """Refreshes the lease of the job this worker currently holds.

        A dead process stops refreshing, its lease expires, and the reaper
        requeues the job -- which is the whole point: a `finally` block cannot
        run after SIGKILL or an OOM kill.
        """
        while self.running:
            # Registration rides the same heartbeat as the lease: one dead
            # process, one thing that stops refreshing, both age out together.
            try:
                self.job_service.register_worker(self.id)
            except Exception as e:
                logging.warning(f"[!] Worker registration failed: {e}")
            job_id = self.current_job_id
            if job_id:
                try:
                    self.job_service.refresh_lease(job_id)
                except Exception as e:
                    logging.warning(f"[!] Lease refresh failed for {job_id}: {e}")
                # Sample RSS while a job runs. This is what makes admission
                # weights measured rather than guessed -- the draft heavy-job
                # list did not include enrich_features, the one that actually
                # killed the fleet.
                rss = _current_rss()
                if rss > self._job_peak_rss:
                    self._job_peak_rss = rss
            time.sleep(LEASE_TTL / 3)

    def run(self):
        logging.info(f"[*] Worker {self.id} started. Waiting for jobs...")

        # Register before the first heartbeat tick so a fleet shows its true
        # size immediately rather than 20s in.
        self.job_service.register_worker(self.id)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        # Recover anything stranded by a previous crash before taking new work.
        self._reap()

        try:
            self._run_loop()
        finally:
            # Best-effort only. A SIGKILL never gets here, which is exactly why
            # registrations expire on their own.
            try:
                self.job_service.unregister_worker(self.id)
            except Exception:
                pass

    def _run_loop(self):
        while self.running:
            try:
                self._reap()

                if self.job_service.is_paused():
                    # Pause is a flag read between claims: the current job always
                    # finishes, nothing new is claimed. With leases in place this
                    # makes stop/restart safe by construction.
                    time.sleep(1)
                    continue

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
                    self.job_service.tick_lanes()
                    continue

                # Fetch job metadata
                job_id = job_id.decode() if isinstance(job_id, bytes) else job_id

                # The claim is now held. The lease is what makes it recoverable:
                # the finally below covers a clean exit, the lease expiring covers
                # SIGKILL / OOM / power loss, where no finally ever runs.
                self.job_service.claim_lease(job_id, self.id)
                self.current_job_id = job_id
                try:
                    job_data = self.r_queue.hgetall(f"job:{job_id}")

                    if not job_data:
                        logging.warning(f"[!] Job {job_id} metadata missing. Skipping.")
                        continue

                    if job_data.get("status") == JobStatus.CANCELLED.value:
                        logging.info(f"[-] Job {job_id} was cancelled. Skipping.")
                        continue

                    # Per-job pause: the job (or a group/pipeline above it) is
                    # held back, so put it down untouched and take the next one.
                    # Checked here rather than at enqueue time because work
                    # arrives on several paths (continuations, retries, spliced
                    # chunks); the pop is the one place they all meet.
                    if self.job_service.is_job_paused(job_id):
                        # Raced with the pause, or was enqueued by a continuation
                        # after it. Drop the claim without requeueing and move
                        # straight to the next job -- no sleep, because other work
                        # can run right now. Resume puts this back on the queue.
                        self.r_queue.hdel(f"job:{job_id}", "queued")
                        self.r_queue.hset(f"job:{job_id}", "paused_queued", "1")
                        logging.info(f"[~] Job {job_id} is paused; leaving it held.")
                        continue

                    # Admission control: only start if the fleet can still
                    # afford this job type's measured peak. Refused jobs go back
                    # on the queue rather than being failed.
                    jtype = job_data.get("type")
                    if not self.job_service.try_admit(job_id, jtype):
                        logging.info(
                            f"[~] Job {job_id} ({jtype}) deferred: fleet memory budget full."
                        )
                        self._requeue(job_id)
                        time.sleep(2)
                        continue

                    # Execute Job
                    self._execute_job(job_id, job_data)
                finally:
                    self.current_job_id = None
                    self.job_service.release_lease(job_id)

            except Exception as e:
                logging.error(f"[!] Worker loop error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(1)

    def _requeue(self, job_id):
        """Puts a claimed job back on the queue untouched.

        The `queued` latch must be cleared first: enqueue_job is idempotent and
        would otherwise treat this as an already-queued pending job and drop it
        on the floor, losing the job entirely.
        """
        self.r_queue.hdel(f"job:{job_id}", "queued")
        self.job_service.enqueue_job(job_id)

    def _mark_file_status(self, payload, status, only_if_not=None):
        """Mirrors a per-file job's outcome onto its file record's `status`.

        payload key names vary by job type (analysis_payload uses
        `file_md5`, build_sim/index_sim payloads use `md5`), so both are
        checked. No-ops for jobs that aren't scoped to a single file
        (batch/collection/pool-wide jobs carry neither key).
        """
        collection = payload.get("collection")
        file_md5 = payload.get("file_md5") or payload.get("md5")
        if not file_md5:
            # INDEX_META's payload nests it under file_meta instead of
            # carrying it at the top level like every other file-scoped job.
            file_meta = payload.get("file_meta") or {}
            file_md5 = file_meta.get("file_md5")
            collection = collection or file_meta.get("collection")
        if not collection or not file_md5:
            return
        try:
            update_file_status(
                self.r_data, collection, file_md5, status, only_if_not=only_if_not
            )
        except Exception as e:
            logging.warning(f"[!] Could not update file status for {file_md5}: {e}")

    def _execute_job(self, job_id, job_data):
        jtype = job_data.get("type")
        payload = json.loads(job_data.get("payload", "{}"))
        parent_id = job_data.get("parent_id")

        logging.info(f"[+] Executing Job {job_id} ({jtype})...")
        _reset_peak_rss()
        self._job_peak_rss = 0
        self.job_service.add_log(
            job_id, f"Worker {self.id} started processing {jtype}."
        )
        self.r_queue.hset(
            f"job:{job_id}",
            mapping={
                "status": JobStatus.RUNNING.value,
                "started_at": str(int(time.time() * 1000)),
            },
        )

        if jtype == JobType.GHIDRA_ANALYZE.value:
            self._mark_file_status(payload, "analyzing")

        # Execute Job within a timer context
        with job_timer(job_id) as timer:
            try:
                # Dispatch
                success = self._dispatch(jtype, payload, job_id)

                if success:
                    self.job_service.add_log(
                        job_id, f"Job {jtype} completed successfully."
                    )
                    self.job_service.complete_job(job_id)
                else:
                    self._mark_file_status(payload, "failed", only_if_not="analyzed")
                    self.job_service.fail_job(
                        job_id, "Job failed (returned False from dispatcher)."
                    )

            except Exception as e:
                logging.error(f"[!] Job {job_id} failed with error: {e}")
                import traceback

                traceback.print_exc()
                self._mark_file_status(payload, "failed", only_if_not="analyzed")
                self.job_service.fail_job(job_id, str(e))
            finally:
                # Record what this job actually cost, so admission weights come
                # from observation instead of a hand-picked list of suspects.
                peak = max(_current_rss(), self._job_peak_rss)
                if peak:
                    try:
                        self.job_service.record_job_peak(jtype, peak)
                    except Exception as e:
                        logging.warning(f"[!] Could not record memory peak: {e}")
                    logging.info(f"[#] Job {job_id} peak RSS {peak / 1024**3:.2f} GiB")

                # Finalize and save performance stats
                stats = timer.finalize()
                self.job_service.save_performance_stats(job_id, stats)
                perf_summary = f"Perf: Total {stats['total_time']}s | Python {stats['python_time']}s | DB {stats['db_time']}s | Lua {stats['lua_time']}s"
                self.job_service.add_log(job_id, perf_summary)
                logging.info(f"[#] Job {job_id} {perf_summary}")

    def _run_ghidra_out_of_process(self, job_id, payload):
        """Runs one Ghidra analysis in a child process, with retries.

        Ghidra's JVM crashes -- there are 27 hs_err_pid*.log dumps in the repo
        root. In-process, a crash took the worker down with it and whatever else
        it was doing. Here a crash costs one child: we retry, and if the binary
        crashes the JVM every time we flag it unanalyzed and move on rather than
        letting one bad file kill workers forever.
        """
        attempts = int(os.getenv("GHIDRA_ANALYZE_ATTEMPTS", 3))
        cmd = [
            sys.executable,
            "-m",
            "bsimvis.ghidra_job",
            "--job-id",
            job_id,
            "--name",
            self.id,
        ]

        for attempt in range(1, attempts + 1):
            self.job_service.add_log(
                job_id, f"Ghidra analysis attempt {attempt}/{attempts} (subprocess)."
            )
            try:
                proc = subprocess.run(cmd, cwd=os.getcwd())
                rc = proc.returncode
            except Exception as e:
                logging.error(f"[!] Could not launch Ghidra subprocess: {e}")
                self.job_service.add_log(job_id, f"Could not launch analysis: {e}")
                return False

            if rc == 0:
                return True

            # Negative rc means a signal: SIGSEGV/SIGABRT is the JVM dying,
            # which is exactly the case retrying is for.
            reason = f"signal {-rc}" if rc < 0 else f"exit {rc}"
            logging.warning(
                f"[!] Ghidra analysis for {job_id} failed ({reason}), attempt {attempt}/{attempts}"
            )
            self.job_service.add_log(
                job_id, f"Analysis attempt {attempt} failed ({reason})."
            )

        # Out of retries. Record it against the file so it is visible as
        # unanalyzed rather than silently missing from the collection.
        collection = payload.get("collection", "main")
        file_md5 = payload.get("file_md5") or payload.get("md5")
        if file_md5:
            try:
                self.r_data.sadd(f"{collection}:files:unanalyzed", file_md5)
            except Exception as e:
                logging.warning(f"[!] Could not flag {file_md5} unanalyzed: {e}")
        self.job_service.add_log(
            job_id,
            f"Giving up after {attempts} attempts; flagged unanalyzed.",
        )
        return False

    def _dispatch(self, jtype, payload, job_id):
        """Dispatcher for background jobs."""
        collection = payload.get("collection", "main")
        md5 = payload.get("md5")
        file_id = payload.get("file_id")
        batch_uuid = payload.get("batch_uuid")

        if jtype == JobType.GHIDRA_ANALYZE.value:
            return self._run_ghidra_out_of_process(job_id, payload)

        elif jtype == JobType.INDEX_META.value:
            file_meta = payload.get("file_meta")
            num_functions = payload.get("num_functions")
            total_features = payload.get("total_features")
            return self.processing_service.index_metadata(
                collection,
                file_id,
                self.job_service,
                job_id,
                file_meta=file_meta,
                num_functions=num_functions,
                total_features=total_features,
            )

        elif jtype == JobType.INDEX_FUNCTIONS.value:
            functions_list = payload.get("functions_list")
            chunk_id = payload.get("chunk_id")
            if chunk_id:
                raw_funcs = self.r_data.get(chunk_id)
                if raw_funcs:
                    functions_list = json.loads(raw_funcs)
                else:
                    # Deleted only after a successful commit below, so an empty
                    # chunk here means the work is already done.
                    self.job_service.add_log(
                        job_id, "Chunk data empty or already processed. Skipping."
                    )
                    return True

            file_meta = payload.get("file_meta")
            file_md5 = payload.get("file_md5")
            batch_uuid = payload.get("batch_uuid")
            ok = self.processing_service.index_functions(
                collection,
                file_id,
                self.job_service,
                job_id,
                functions_list=functions_list,
                file_meta=file_meta,
                file_md5=file_md5,
                batch_uuid=batch_uuid,
            )
            # The chunk is the ONLY copy of these functions. Deleting it before
            # the commit meant a crash in between lost them silently, and made
            # the reaper's requeue destructive: the retry found no data and
            # "succeeded" with nothing indexed. Delete last, so a retry always
            # has something to retry with. index_functions writes keyed by
            # function id, so replaying a chunk overwrites rather than
            # duplicates.
            if ok and chunk_id:
                self.r_data.delete(chunk_id)
            return ok

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

            ok = self.feature_service.index_functions(
                collection, function_ids, self.job_service, job_id
            )
            # Last stage that is guaranteed to run after every INDEX_FUNCTIONS
            # chunk, so it is where the functions' library tags become file tags.
            if ok and md5:
                self.processing_service.rollup_lib_tags(collection, md5)
                # Also the last file-scoped stage of core analysis (before the
                # optional similarity stages), so this is where a file's
                # status stops being "analyzing" -- not GHIDRA_ANALYZE
                # dispatch or INDEX_META, both of which fire while functions
                # are still being indexed.
                update_file_status(self.r_data, collection, md5, "analyzed")
            return ok

        elif jtype == JobType.SYNC_MILVUS.value:
            from bsimvis.app.services.milvus_service import milvus_service

            return milvus_service.sync_collection(
                collection, self.r_data, self.job_service, job_id
            )

        elif jtype == JobType.BUILD_SIM.value:
            algo = payload.get(
                "algo", config_service.get("similarity.algo", "unweighted_cosine")
            )
            top_k = payload.get("top_k", config_service.get("similarity.top_k", 1000))
            # Collection-sticky: first build locks these; later payload values ignored
            # so the BSim-vs-hash split (and canonical file score) stays stable.
            from bsimvis.app.services.collection_config import resolve_and_lock

            min_score = resolve_and_lock(
                collection, "min_score", payload.get("min_score")
            )
            min_features = resolve_and_lock(
                collection, "min_features", payload.get("min_features")
            )
            # ponytail: Default to 'minimal' index depth to save indexing writes during build_sim
            index_depth = payload.get("index_depth", "minimal")

            if not md5 and file_id:
                # Fallback: Fetch monolith if MD5 is missing
                raw = self.r_data.get(file_id)
                data = {}
                if raw:
                    val = raw.decode() if isinstance(raw, bytes) else raw
                    try:
                        data = json.loads(val)
                    except Exception:
                        pass
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
                index_depth=index_depth,
                skip_write=payload.get("skip_write", False),  # ponytail
            )

        elif jtype == JobType.INDEX_SIM.value:
            algo = payload.get(
                "algo", config_service.get("similarity.algo", "unweighted_cosine")
            )
            pool_id = payload.get("pool_id")
            if not md5 and file_id:
                raw = self.r_data.get(file_id)
                data = {}
                if raw:
                    val = raw.decode() if isinstance(raw, bytes) else raw
                    try:
                        data = json.loads(val)
                    except Exception:
                        pass
                md5 = data.get("file_md5")
            return self.similarity_service.index_similarities(
                collection,
                algo=algo,
                pool_id=pool_id,
                md5=md5,
                batch_uuid=batch_uuid,
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
            algo = payload.get("algo", "unweighted_cosine")
            min_cluster_size = payload.get(
                "min_cluster_size", config_service.get("clustering.min_cluster_size", 2)
            )
            min_samples = payload.get(
                "min_samples", config_service.get("clustering.min_samples", 1)
            )
            epsilon = payload.get(
                "epsilon", config_service.get("clustering.epsilon", 0.1)
            )
            selection_method = payload.get(
                "selection_method",
                config_service.get("clustering.selection_method", "eom"),
            )
            min_sim = payload.get(
                "min_sim", config_service.get("clustering.min_sim", 0.0)
            )
            min_features = payload.get(
                "min_features", config_service.get("clustering.min_features", 0)
            )

            return cluster_service.run_clustering(
                collection,
                algo=algo,
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                cluster_selection_epsilon=epsilon,
                selection_method=selection_method,
                min_sim=min_sim,
                min_features=min_features,
                batch_uuid=batch_uuid,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.CLEAR_CLUSTER.value:
            algo = payload.get("algo", "unweighted_cosine")
            return cluster_service.clear_clustering(
                collection, algo=algo, job_service=self.job_service, job_id=job_id
            )

        elif jtype == JobType.CLUSTER_BINARIES.value:
            algo = payload.get("algo", "unweighted_cosine")
            min_cluster_size = payload.get(
                "min_cluster_size", config_service.get("clustering.min_cluster_size", 2)
            )
            min_samples = payload.get(
                "min_samples", config_service.get("clustering.min_samples", 1)
            )
            epsilon = payload.get(
                "epsilon", config_service.get("clustering.epsilon", 0.1)
            )
            selection_method = payload.get(
                "selection_method",
                config_service.get("clustering.selection_method", "eom"),
            )
            min_sim = payload.get(
                "min_sim", config_service.get("clustering.min_sim", 0.0)
            )
            min_cohesion = payload.get(
                "min_cohesion", config_service.get("clustering.min_cohesion", 0.5)
            )

            return bin_cluster_service.run_clustering(
                collection,
                algo=algo,
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                cluster_selection_epsilon=epsilon,
                selection_method=selection_method,
                min_sim=min_sim,
                job_service=self.job_service,
                job_id=job_id,
                min_cohesion=min_cohesion,
            )

        elif jtype == JobType.CLEAR_BIN_CLUSTER.value:
            algo = payload.get("algo", "unweighted_cosine")
            return bin_cluster_service.clear_clusters(
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
                batch_uuid=payload.get("batch_uuid"),
                pairs_key=payload.get("pairs_key"),
                offset=payload.get("offset", 0),
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

        elif jtype == JobType.RESPLIT_BIN_SIM.value:
            return bin_sim_service.resplit_bin_sim(
                collection,
                algo=payload.get("algo", "unweighted_cosine"),
                md5=payload.get("md5"),
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.REINDEX_BIN_SIM.value:
            algo = payload.get("algo", "unweighted_cosine")
            pool_id = payload.get("pool_id")
            if pool_id:
                return self.similarity_service.reindex_pool_bin_sim(
                    pool_id,
                    algo=algo,
                    job_service=self.job_service,
                    job_id=job_id,
                )
            return bin_sim_service.reindex_bin_sim(
                collection,
                algo=algo,
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.ENRICH_FEATURES.value:
            return self.feature_service.enrich_features(
                collection, self.job_service, job_id
            )

        elif jtype == JobType.LLM_BATCH.value:
            from bsimvis.app.services.llm_batch_service import llm_batch_service

            return llm_batch_service.run_batch(
                collection,
                payload.get("func_ids") or [],
                payload.get("actions") or ["notes"],
                overwrite=payload.get("overwrite", False),
                custom_prompt=payload.get("custom_prompt"),
                vocabulary=payload.get("tag_vocabulary"),
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.LLM_CONTEXTUAL_BATCH.value:
            from bsimvis.app.services.analysis_orchestrator import analysis_orchestrator

            return analysis_orchestrator.run_contextual_batch(
                collection,
                payload.get("func_ids") or [],
                actions=payload.get("actions") or ["notes", "tags"],
                overwrite=payload.get("overwrite", False),
                custom_prompt=payload.get("custom_prompt"),
                job_service=self.job_service,
                job_id=job_id,
                unit_max_size=payload.get("unit_max_size"),
            )

        elif jtype == JobType.LLM_FILE_ANALYSIS.value:
            from bsimvis.app.services.analysis_orchestrator import analysis_orchestrator

            return analysis_orchestrator.run_file_analysis(
                collection,
                payload.get("file_md5"),
                payload.get("func_ids") or [],
                actions=payload.get("actions") or ["notes", "tags"],
                overwrite=payload.get("overwrite", False),
                custom_prompt=payload.get("custom_prompt"),
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.DELETE_COLLECTION.value:
            return self.processing_service.delete_collection(
                collection, self.job_service, job_id
            )

        elif jtype == JobType.CLEAN_COLLECTION.value:
            return self.processing_service.clean_collection(
                collection, self.job_service, job_id
            )

        elif jtype == JobType.PROPAGATE_METADATA.value:
            return self.metadata_service.propagate_metadata(
                collection,
                payload.get("updates"),
                job_service=self.job_service,
                job_id=job_id,
            )

        elif jtype == JobType.INIT_POOL_BUILD.value:
            pool_id = payload.get("pool_id")
            from bsimvis.app.services.pool_service import pool_service

            return pool_service.init_pool_build(pool_id)

        elif jtype == JobType.FINALIZE_POOL_BUILD.value:
            pool_id = payload.get("pool_id")
            from bsimvis.app.services.pool_service import pool_service

            return pool_service.finalize_pool_build(pool_id)

        elif jtype == JobType.BUILD_POOL_SIM.value:
            pool_id = payload.get("pool_id")
            file_md5 = payload.get("file_md5")
            index_depth = payload.get("index_depth", "none")
            skip_write = payload.get("skip_write", False)
            if file_md5:
                return self.similarity_service.build_pool_file(
                    pool_id,
                    file_md5,
                    job_service=self.job_service,
                    job_id=job_id,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
            else:
                return self.similarity_service.build_pool(
                    pool_id,
                    job_service=self.job_service,
                    job_id=job_id,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )

        elif jtype == JobType.CLUSTER_POOL.value:
            pool_id = payload.get("pool_id")
            return cluster_service.run_pool_clustering(
                pool_id, job_service=self.job_service, job_id=job_id
            )

        elif jtype == JobType.BUILD_POOL_BIN_SIM.value:
            pool_id = payload.get("pool_id")
            return self.similarity_service.build_pool_bin_sim(
                pool_id, job_service=self.job_service, job_id=job_id
            )

        elif jtype == JobType.CLUSTER_POOL_BINARIES.value:
            pool_id = payload.get("pool_id")
            return cluster_service.run_pool_bin_clustering(
                pool_id, job_service=self.job_service, job_id=job_id
            )

        return False


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
