import time
import json
import logging
import signal
import sys
import os
import tempfile
from pathlib import Path
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
        max_heap_mb = config_service.get("ghidra.max_heap_mb", 1536)
        jvm_args = config_service.get("ghidra.jvm_args", [])
        ghidra_service.ensure_launcher(max_heap_mb=max_heap_mb, jvm_args=jvm_args)

        self.similarity_service = SimilarityService(self.r_data)
        self.metadata_service = MetadataService(self.r_data)
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
                    self.job_service.tick_lanes()
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
        self.r_queue.hset(
            f"job:{job_id}",
            mapping={
                "status": JobStatus.RUNNING.value,
                "started_at": str(int(time.time() * 1000)),
            },
        )

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
                    self.job_service.fail_job(
                        job_id, "Job failed (returned False from dispatcher)."
                    )

            except Exception as e:
                logging.error(f"[!] Job {job_id} failed with error: {e}")
                import traceback

                traceback.print_exc()
                self.job_service.fail_job(job_id, str(e))
            finally:
                # Finalize and save performance stats
                stats = timer.finalize()
                self.job_service.save_performance_stats(job_id, stats)
                perf_summary = f"Perf: Total {stats['total_time']}s | Python {stats['python_time']}s | DB {stats['db_time']}s | Lua {stats['lua_time']}s"
                self.job_service.add_log(job_id, perf_summary)
                logging.info(f"[#] Job {job_id} {perf_summary}")

    def _collect_project_files(self, folder):
        """Recursively collect all DomainFiles from a Ghidra project folder."""
        files = list(folder.getFiles())
        for sub in folder.getFolders():
            files.extend(self._collect_project_files(sub))
        return files

    def _index_streamed_program(self, program, payload, job_id):
        """Analyzes and indexes one program's functions in-process, chunk by
        chunk -- bounding RAM the same way the old chunked-upload endpoint
        did, but without spawning a job or HTTP call per chunk. Replaces the
        old _post_chunk/self-HTTP-call design, which spliced dynamically
        created jobs into this job's own running pipeline and, as a
        consequence, ran BUILD_SIM/INDEX_SIM twice per upload. See Item D in
        /home/thomas/.claude/plans/synthetic-dancing-harbor.md."""
        collection = payload.get("collection", "main")
        skip_sim = payload.get("skip_sim", False)
        batch_uuid = payload.get("batch_uuid")

        generator = ghidra_service.stream_bsim_data(
            program,
            payload,
            chunk_size=100,
            job_service=self.job_service,
            job_id=job_id,
        )
        file_meta = next(generator)

        # Merge CLI-provided metadata (upload --metadata) into the file metadata so
        # it gets stored just like the `metadata propagate` path. The raw-upload
        # route forwards it as file_metadata_extra; without this the streaming
        # analysis path silently drops it.
        extra_meta = payload.get("file_metadata_extra")
        if extra_meta:
            if isinstance(extra_meta, str):
                extra_meta = json.loads(extra_meta)
            file_meta.update(extra_meta)
            if "file_name" in extra_meta:
                file_meta["file_name"] = extra_meta["file_name"]

        file_md5 = file_meta.get("file_md5")

        num_functions = 0
        total_features = 0
        for chunk in generator:
            if not chunk:
                continue
            num_functions += len(chunk)
            total_features += sum(
                f.get("function_metadata", {}).get("bsim_features_count", 0)
                for f in chunk
            )
            self.processing_service.index_functions(
                collection,
                None,
                self.job_service,
                job_id,
                functions_list=chunk,
                file_meta=file_meta,
                file_md5=file_md5,
                batch_uuid=batch_uuid,
            )
            self.job_service.update_progress(
                job_id, 80, f"Indexed {num_functions} functions for {file_md5}"
            )

        self.processing_service.index_metadata(
            collection,
            None,
            self.job_service,
            job_id,
            file_meta=file_meta,
            num_functions=num_functions,
            total_features=total_features,
        )

        # Same function-id resolution the INDEX_FEATURES dispatch used.
        batch_func_set = f"{collection}:idx:file:functions:{file_md5}"
        raw_ids = list(self.r_data.smembers(batch_func_set))
        function_ids = [
            fid.replace(":meta", "") if fid.endswith(":meta") else fid
            for fid in raw_ids
        ]
        self.feature_service.index_functions(
            collection, function_ids, self.job_service, job_id
        )

        if not skip_sim:
            algo = payload.get(
                "algo", config_service.get("similarity.algo", "unweighted_cosine")
            )
            top_k = payload.get(
                "top_k", config_service.get("similarity.top_k", 1000)
            )
            # Collection-sticky: first build locks these; later payload values
            # ignored so the BSim-vs-hash split stays stable.
            from bsimvis.app.services.collection_config import resolve_and_lock

            min_score = resolve_and_lock(
                collection, "min_score", payload.get("min_score")
            )
            min_features = resolve_and_lock(
                collection, "min_features", payload.get("min_features")
            )
            index_depth = payload.get("index_depth", "minimal")

            if algo == "milvus_sparse":
                from bsimvis.app.services.milvus_service import milvus_service

                if milvus_service.enabled:
                    milvus_service.sync_collection(
                        collection, self.r_data, self.job_service, job_id
                    )

            self.similarity_service.build_batch(
                collection,
                batch_uuid=batch_uuid,
                md5=file_md5,
                algo=algo,
                top_k=top_k,
                min_score=min_score,
                min_features=min_features,
                job_service=self.job_service,
                job_id=job_id,
                index_depth=index_depth,
                skip_write=payload.get("skip_write", False),
            )
            self.similarity_service.index_similarities(
                collection,
                algo=algo,
                pool_id=payload.get("pool_id"),
                md5=file_md5,
                batch_uuid=batch_uuid,
                job_service=self.job_service,
                job_id=job_id,
            )

        self.job_service.update_progress(
            job_id, 100, f"Indexing complete for {file_md5}"
        )
        return file_md5

    def _dispatch(self, jtype, payload, job_id):
        """Dispatcher for background jobs."""
        collection = payload.get("collection", "main")
        md5 = payload.get("md5")
        file_id = payload.get("file_id")
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

                # 3. Run analysis, then index in-process (see
                # _index_streamed_program)
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
                    from ghidra.base.project import GhidraProject

                    project = GhidraProject.openProject(
                        Path(gpr_path).parent, Path(gpr_path).stem
                    )
                    try:
                        root_folder = project.getProjectData().getRootFolder()
                        files = self._collect_project_files(root_folder)
                        for file in files:
                            from ghidra.util.task import ConsoleTaskMonitor

                            program = file.getDomainObject(
                                project, True, False, ConsoleTaskMonitor()
                            )
                            try:
                                binary_md5 = program.getExecutableMD5()
                                if binary_md5:
                                    # Check if already exists in collection
                                    if self.r_data.sismember(
                                        f"{collection}:all_files",
                                        f"{collection}:file:{binary_md5}",
                                    ):
                                        self.job_service.add_log(
                                            job_id,
                                            f"Skipping {program.getName()} (MD5 {binary_md5} already exists in collection).",
                                        )
                                        continue

                                ghidra_service.run_profile_analysis(
                                    program,
                                    payload.get("profile", "fast"),
                                    force_reanalysis=False,
                                )
                                # Indexed in-process, one program at a time --
                                # each program still gets its own file entry
                                # (own md5), just no per-program job/pipeline.
                                self._index_streamed_program(program, payload, job_id)
                            finally:
                                if program:
                                    program.release(project)
                    finally:
                        project.close()
                else:
                    self.job_service.add_log(
                        job_id, f"Starting Ghidra analysis for {orig_name}..."
                    )
                    # For a single file
                    from ghidra.base.project import GhidraProject

                    with tempfile.TemporaryDirectory(
                        prefix="bsim_"
                    ) as project_temp_dir:
                        project = GhidraProject.createProject(
                            project_temp_dir, "TempGhidraProject", False
                        )
                        try:
                            if payload.get("processor"):
                                from ghidra.program.model.lang import (
                                    LanguageID,
                                    CompilerSpecID,
                                )
                                from ghidra.program.util import DefaultLanguageService

                                lang_service = (
                                    DefaultLanguageService.getLanguageService()
                                )
                                lang_id = LanguageID(payload.get("processor"))
                                lang = lang_service.getLanguage(lang_id)
                                if payload.get("cspec"):
                                    cspec_id = CompilerSpecID(payload.get("cspec"))
                                    cspec = lang.getCompilerSpecByID(cspec_id)
                                else:
                                    cspec = lang.getDefaultCompilerSpec()
                                program = project.importProgram(
                                    Path(temp_path), lang, cspec
                                )
                            else:
                                program = project.importProgram(Path(temp_path))

                            ghidra_service.run_profile_analysis(
                                program,
                                payload.get("profile", "fast"),
                                force_reanalysis=True,
                            )
                            self._index_streamed_program(program, payload, job_id)
                        finally:
                            if "program" in locals() and program:
                                program.release(project)

                self.job_service.add_log(
                    job_id, f"Analysis and streaming complete for {orig_name}."
                )

                return True
            except Exception as e:
                self.job_service.add_log(job_id, f"Analysis failed: {str(e)}")
                raise
            finally:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                if temp_dir and os.path.exists(temp_dir):
                    import shutil

                    try:
                        shutil.rmtree(temp_dir)
                    except Exception:
                        pass

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
                # ponytail: retrieve from Kvrocks & delete right away
                raw_funcs = self.r_data.get(chunk_id)
                if raw_funcs:
                    functions_list = json.loads(raw_funcs)
                    self.r_data.delete(chunk_id)
                else:
                    # ponytail: Duplicate run or missing data, finish gracefully
                    self.job_service.add_log(
                        job_id, "Chunk data empty or already processed. Skipping."
                    )
                    return True

            file_meta = payload.get("file_meta")
            file_md5 = payload.get("file_md5")
            batch_uuid = payload.get("batch_uuid")
            return self.processing_service.index_functions(
                collection,
                file_id,
                self.job_service,
                job_id,
                functions_list=functions_list,
                file_meta=file_meta,
                file_md5=file_md5,
                batch_uuid=batch_uuid,
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
