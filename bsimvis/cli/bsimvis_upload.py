import tomllib, json, uuid, csv, hashlib

import time, logging, argparse, os, tempfile, zipfile
from pathlib import Path
from collections import Counter
from typing import Optional
import concurrent.futures, threading

from tqdm import tqdm
import requests

from bsimvis.app.services.ghidra_service import ghidra_service

DEFAULT_CONFIG_NAME = "bsimvis_config.toml"
DEFAULT_BATCH_NAME = "Ghidra Batch"

# Per-target outcomes. "Already present by MD5" is a third state, not a failure:
# folding it into failure made a fully-deduplicated group report 0/19 and look
# like a total wipeout. Callers should treat only UPLOAD_FAILED as an error.
UPLOAD_FAILED = 0
UPLOAD_OK = 1
UPLOAD_DUPLICATE = 2


def archive_ghidra_project(gpr_path: Path) -> Optional[str]:
    """
    Bundles a .gpr file and its associated .rep directory into a zip for remote analysis.
    """
    rep_dir = gpr_path.with_suffix(".rep")
    if not rep_dir.exists():
        logging.error(f"[!] Associated .rep directory not found for {gpr_path.name}")
        return None

    # Create temp zip
    fd, tmp_zip = tempfile.mkstemp(suffix=".gpr.zip")
    os.close(fd)

    try:
        with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add .gpr
            zipf.write(gpr_path, gpr_path.name)
            # Add .rep content
            for root, dirs, files in os.walk(rep_dir):
                for file in files:
                    full_path = Path(root) / file
                    # arcname should be <project>.rep/...
                    arcname = full_path.relative_to(gpr_path.parent)
                    zipf.write(full_path, arcname)
        return tmp_zip
    except Exception as e:
        logging.error(f"[!] Failed to archive Ghidra project: {e}")
        if os.path.exists(tmp_zip):
            os.remove(tmp_zip)
        return None


def upload_bsim_data(data, args, config):
    """
    Submits analyzed BSim data to the BSimVis API instead of direct Redis writes.
    This triggers the background job pipeline.
    """
    if not data or not data.get("functions"):
        logging.warning("[!] No data to upload.")
        return []

    file_meta = data.get("file_metadata", {})
    file_md5 = file_meta.get("file_md5", "unknown_md5")

    if getattr(args, "metadata_dict", None) and file_md5 in args.metadata_dict:
        extra_meta = args.metadata_dict[file_md5]
        data.setdefault("file_metadata", {}).update(extra_meta)
        if "file_name" in extra_meta:
            data["file_metadata"]["file_name"] = extra_meta["file_name"]

    # Ensure collection is at the root for the API
    collections = args.collections if args.collections else ["main"]

    # NEW: Handle saving JSON to file
    save_path = getattr(args, "save_json", None)
    if save_path:
        # If multiple collections, we still only need to save the data once
        # (collection field will be set by the bench script during replay)
        dump_data = {"collection": collections[0], "file_md5": file_md5, **data}

        target_file = save_path
        # If it's an existing dir, or ends in slash, or doesn't have .json extension, treat as dir
        if (
            os.path.isdir(save_path)
            or save_path.endswith(("/", "\\"))
            or not save_path.lower().endswith(".json")
        ):
            os.makedirs(save_path, exist_ok=True)
            target_file = os.path.join(save_path, f"{file_md5}.json")

        try:
            # For pure file paths, ensure parent exists
            parent_dir = os.path.dirname(os.path.abspath(target_file))
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            with open(target_file, "w") as f:
                json.dump(dump_data, f, indent=2)
            logging.info(f"[+] Data saved to {target_file}")
        except Exception as e:
            logging.error(f"[!] Failed to save JSON to {target_file}: {e}")

    pipeline_details = []
    # We trigger the API for each collection
    for collection in collections:
        # Prepare the payload for the API
        payload = {
            "collection": collection,
            "file_md5": file_md5,
            "enqueue": False,
            **data,
        }

        if getattr(args, "top_k", None) is not None:
            payload["top_k"] = args.top_k
        if getattr(args, "min_score", None) is not None:
            payload["min_score"] = args.min_score
        if getattr(args, "min_features", None) is not None:
            payload["min_features"] = args.min_features
        if getattr(args, "algo", None) is not None:
            payload["algo"] = args.algo
        if getattr(args, "skip_sim", False):
            payload["skip_sim"] = True

        # Submit to API
        hosts = getattr(args, "hosts", [])
        if not hosts:
            # Fallback to single host if plural not set
            hosts = [getattr(args, "host", "localhost:5000")]

        for api_host in hosts:
            api_url = f"http://{api_host}/api/file/upload_file_data"

            try:
                logging.info(
                    f"[*] Submitting {file_md5} to API at {api_url} (collection: {collection})..."
                )
                resp = requests.post(api_url, json=payload, timeout=300)
                resp.raise_for_status()

                result = resp.json()
                pipeline_id = result.get("pipeline_id")
                logging.info(
                    f"[+] Upload Success on {api_host}! Pipeline ID: {pipeline_id}"
                )
                if pipeline_id:
                    pipeline_details.append(
                        {
                            "host": api_host,
                            "collection": collection,
                            "pipeline_id": pipeline_id,
                        }
                    )
            except Exception as e:
                is_duplicate = False
                err_msg = ""
                if (
                    isinstance(e, requests.exceptions.HTTPError)
                    and e.response is not None
                ):
                    try:
                        err_json = e.response.json()
                        err_msg = err_json.get("error") or err_json.get("message") or ""
                        if (
                            e.response.status_code == 400
                            and "already exists" in err_msg
                        ):
                            logging.warning(f"[!] Skipped: {file_md5} - {err_msg}")
                            is_duplicate = True
                    except Exception:
                        err_msg = e.response.text[:200]

                if not is_duplicate:
                    err_suffix = f" (Details: {err_msg})" if err_msg else ""
                    logging.error(
                        f"[!] API Submission failed for {api_url}: {e}{err_suffix}"
                    )

    return pipeline_details


def upload_raw_binary(target_path, args):
    """
    Uploads a raw binary file or a Ghidra project to the server for analysis.
    """
    target_path = Path(target_path).resolve()
    if not target_path.exists():
        logging.error(f"[!] Target path does not exist: {target_path}")
        return 0, []

    is_gpr = target_path.suffix == ".gpr"

    if is_gpr:
        logging.info(
            f"[*] Archiving Ghidra project {target_path.name} for remote analysis..."
        )
        archive_path = archive_ghidra_project(target_path)
        if not archive_path:
            return 0, []
        try:
            with open(archive_path, "rb") as f:
                raw_bytes = f.read()
            # Override file_name for the API
            file_name = target_path.name + ".zip"
            return _perform_raw_upload(raw_bytes, file_name, args)
        finally:
            if os.path.exists(archive_path):
                os.remove(archive_path)

    if not target_path.is_file():
        logging.error(f"[!] Target path is not a file: {target_path}")
        return 0, []

    try:
        with open(target_path, "rb") as f:
            raw_bytes = f.read()

        if not raw_bytes:
            logging.warning(f"[!] File {target_path.name} is empty. Skipping upload.")
            return 0, []

        return _perform_raw_upload(raw_bytes, target_path.name, args)
    except Exception as e:
        logging.error(f"[!] Failed to read {target_path}: {e}")
        return 0, []


def _perform_raw_upload(raw_bytes, file_name, args):
    hosts = getattr(args, "hosts", [])
    if not hosts:
        hosts = [getattr(args, "host", "localhost:5000")]

    collections = args.collections if args.collections else ["main"]

    success = True
    duplicate = False  # every rejection so far was "already exists"
    failed = False  # at least one genuine error
    pipeline_details = []
    for collection in collections:
        for api_host in hosts:
            params = {
                "collection": collection,
                "file_name": file_name,
                "batch_uuid": args.batch_uuid,
                "batch_name": args.batch_name,
                "profile": args.profile,
                "min_func_len": args.min_func_len,
                "enqueue": "false",
            }

            if getattr(args, "metadata_dict", None):
                file_md5 = hashlib.md5(raw_bytes).hexdigest()
                extra_meta = args.metadata_dict.get(file_md5)
                if extra_meta:
                    params["file_metadata_extra"] = json.dumps(extra_meta)
                    if "file_name" in extra_meta:
                        params["file_name"] = extra_meta["file_name"]

            # Ghidra Import options
            if getattr(args, "processor", None) is not None:
                params["processor"] = args.processor
            if getattr(args, "cspec", None) is not None:
                params["cspec"] = args.cspec

            # Add tags as multiple params
            if getattr(args, "tags", None):
                params["tags"] = args.tags

            # Similarity options
            if getattr(args, "top_k", None) is not None:
                params["top_k"] = args.top_k
            if getattr(args, "min_score", None) is not None:
                params["min_score"] = args.min_score
            if getattr(args, "min_features", None) is not None:
                params["min_features"] = args.min_features
            if getattr(args, "algo", None) is not None:
                params["algo"] = args.algo
            if getattr(args, "skip_sim", False):
                params["skip_sim"] = True

            api_url = f"http://{api_host}/api/file/upload"
            try:
                logging.info(f"[*] Uploading {file_name} to {api_url}...")
                resp = requests.post(
                    api_url, params=params, data=raw_bytes, timeout=600
                )
                resp.raise_for_status()
                result = resp.json()
                pipeline_id = result.get("pipeline_id")
                logging.info(
                    f"[+] Upload Success on {api_host}! Pipeline ID: {pipeline_id}"
                )
                if pipeline_id:
                    pipeline_details.append(
                        {
                            "host": api_host,
                            "collection": collection,
                            "pipeline_id": pipeline_id,
                        }
                    )
            except Exception as e:
                is_duplicate = False
                err_msg = ""
                if (
                    isinstance(e, requests.exceptions.HTTPError)
                    and e.response is not None
                ):
                    try:
                        err_json = e.response.json()
                        err_msg = err_json.get("error") or err_json.get("message") or ""
                        if (
                            e.response.status_code == 400
                            and "already exists" in err_msg
                        ):
                            logging.warning(f"[!] Skipped: {file_name} - {err_msg}")
                            is_duplicate = True
                    except Exception:
                        err_msg = e.response.text[:200]

                if is_duplicate:
                    duplicate = True
                else:
                    err_suffix = f" (Details: {err_msg})" if err_msg else ""
                    logging.error(f"[!] Upload failed for {api_url}: {e}{err_suffix}")
                    failed = True
                success = False

    # A file already present by MD5 is not a failure. Collapsing the two meant a
    # group where every file was already indexed reported "0.00% (0/19)" and
    # looked like a total wipeout -- while a run where everything genuinely
    # failed reported the same thing and still exited 0.
    if failed:
        return UPLOAD_FAILED, pipeline_details
    if duplicate:
        return UPLOAD_DUPLICATE, pipeline_details
    return (UPLOAD_OK if success else UPLOAD_FAILED), pipeline_details


def process_target(target, args, config, batch_order) -> tuple[int, list]:
    target_path = Path(target).resolve()

    # CASE 1: Local Analysis forced
    if getattr(args, "local_analysis", False):
        try:
            t0 = time.time()
            options = vars(args).copy()
            options["batch_order"] = batch_order

            pipeline_details = []

            # Setup hosts
            hosts = getattr(args, "hosts", [])
            if not hosts:
                hosts = [getattr(args, "host", "localhost:5000")]

            collections = args.collections if args.collections else ["main"]

            # Initialize Ghidra VM
            ghidra_service.ensure_launcher()

            if target_path.suffix == ".gpr":
                project = ghidra_service.openProject(
                    target_path.parent, target_path.stem
                )
                try:
                    root_folder = project.getProjectData().getRootFolder()
                    files = root_folder.getFiles()
                    for file in files:
                        from ghidra.util.task import ConsoleTaskMonitor

                        program = file.getDomainObject(
                            project, True, False, ConsoleTaskMonitor()
                        )
                        try:
                            ghidra_service.run_profile_analysis(
                                program,
                                options.get("profile", "fast"),
                                force_reanalysis=False,
                            )
                            stream_generator = ghidra_service.stream_bsim_data(
                                program, options, chunk_size=100
                            )
                            file_meta = next(stream_generator)

                            all_chunks = list(stream_generator)
                            if not all_chunks:
                                all_chunks = [[]]

                            for collection in collections:
                                for idx, chunk in enumerate(all_chunks):
                                    chunk_payload = {
                                        "collection": collection,
                                        "file_md5": file_meta.get("file_md5"),
                                        "chunk_index": idx,
                                        "is_final": (idx == len(all_chunks) - 1),
                                        "skip_sim": getattr(args, "skip_sim", False),
                                        "file_metadata": (
                                            file_meta if idx == 0 else None
                                        ),
                                        "functions": chunk,
                                    }
                                    # Copy other variables
                                    for opt in [
                                        "top_k",
                                        "min_score",
                                        "min_features",
                                        "algo",
                                    ]:
                                        if getattr(args, opt, None) is not None:
                                            chunk_payload[opt] = getattr(args, opt)

                                    for api_host in hosts:
                                        url = f"http://{api_host}/api/file/upload_chunk"
                                        resp = requests.post(
                                            url, json=chunk_payload, timeout=300
                                        )
                                        resp.raise_for_status()
                        finally:
                            if program:
                                program.release(project)
                finally:
                    project.close()
            else:
                from ghidra.base.project import GhidraProject

                with tempfile.TemporaryDirectory(prefix="bsim_") as project_temp_dir:
                    project = GhidraProject.createProject(
                        project_temp_dir, "TempGhidraProject", False
                    )
                    try:
                        if options.get("processor"):
                            from ghidra.program.model.lang import (
                                LanguageID,
                                CompilerSpecID,
                            )
                            from ghidra.program.util import DefaultLanguageService

                            lang_service = DefaultLanguageService.getLanguageService()
                            lang_id = LanguageID(options.get("processor"))
                            lang = lang_service.getLanguage(lang_id)
                            if options.get("cspec"):
                                cspec_id = CompilerSpecID(options.get("cspec"))
                                cspec = lang.getCompilerSpecByID(cspec_id)
                            else:
                                cspec = lang.getDefaultCompilerSpec()
                            program = project.importProgram(target_path, lang, cspec)
                        else:
                            program = project.importProgram(target_path)

                        ghidra_service.run_profile_analysis(
                            program,
                            options.get("profile", "fast"),
                            force_reanalysis=True,
                        )
                        stream_generator = ghidra_service.stream_bsim_data(
                            program, options, chunk_size=100
                        )
                        file_meta = next(stream_generator)

                        all_chunks = list(stream_generator)
                        if not all_chunks:
                            all_chunks = [[]]

                        for collection in collections:
                            for idx, chunk in enumerate(all_chunks):
                                chunk_payload = {
                                    "collection": collection,
                                    "file_md5": file_meta.get("file_md5"),
                                    "chunk_index": idx,
                                    "is_final": (idx == len(all_chunks) - 1),
                                    "skip_sim": getattr(args, "skip_sim", False),
                                    "file_metadata": file_meta if idx == 0 else None,
                                    "functions": chunk,
                                }
                                for opt in [
                                    "top_k",
                                    "min_score",
                                    "min_features",
                                    "algo",
                                ]:
                                    if getattr(args, opt, None) is not None:
                                        chunk_payload[opt] = getattr(args, opt)

                                for api_host in hosts:
                                    url = f"http://{api_host}/api/file/upload_chunk"
                                    resp = requests.post(
                                        url, json=chunk_payload, timeout=300
                                    )
                                    resp.raise_for_status()
                    finally:
                        # close() releases every program importProgram() registered and
                        # disposes the project's LocalFileSystem, stopping its
                        # "File System Listener" thread. Releasing the program first
                        # would make close() throw "unknown consumer". See worker.py.
                        project.close()

            t_total = time.time() - t0
            logging.info(
                f"[+] Local processing and chunked streaming finished for {target_path.name} in {t_total:.3f}s"
            )
            return 1, pipeline_details
        except Exception as e:
            logging.error(f"[!] Local processing failed for {target_path.name}: {e}")
            import traceback

            logging.error(traceback.format_exc())
            return 0, []

    # CASE 2: Raw Binary - Remote Analysis
    else:
        return upload_raw_binary(target, args)


def worker(target, args, config, batch_order):
    """Thread entry point."""
    logging.info(f"[+] Job {batch_order} started for {target}")
    result = process_target(target, args, config, batch_order)

    return result


def run_upload(host, port, args):
    if args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(level=level, force=True)

    # Map back to what main(args) expects. Returning it matters: the failure
    # count is the exit code, and dropping it here would keep `upload` exiting
    # 0 no matter how many files failed.
    return main(args)


def main(args):

    # Check if we need local Ghidra
    needs_local_ghidra = getattr(args, "local_analysis", False)

    if needs_local_ghidra:
        ghidra_service.ensure_launcher(
            verbose=args.verbose_analysis,
            max_ram_percent=args.max_ram_percent,
            jvm_args=args.jvm_args,
        )
    else:
        logging.info("[i] Remote analysis selected. Skipping local Ghidra JVM start.")

    logging.info(f"[i] Loading config {args.config}")
    config = load_config(args.config)

    if len(args.collections) == 0:
        args.collections = ["main"]

    if not args.batch_uuid:
        args.batch_uuid = str(uuid.uuid4())

    args.metadata_dict = {}
    if getattr(args, "metadata", None):
        try:
            with open(args.metadata, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="|")
                reader.fieldnames = [n.strip() for n in reader.fieldnames]
                for row in reader:
                    hash_val = row.get("HASH", "").strip()
                    if not hash_val:
                        continue

                    def parse_list(val):
                        if not val or val.strip() == "-":
                            return []
                        return [v.strip() for v in val.split(",")]

                    names = parse_list(row.get("names", ""))
                    extra = {
                        "first_seen": parse_list(row.get("first_seen", "")),
                        "last_seen": parse_list(row.get("last_seen", "")),
                        "filetype": parse_list(row.get("filetype", "")),
                        "avtype": parse_list(row.get("avtype", "")),
                        "yara": parse_list(row.get("yara", "")),
                        "file_names": names,
                        "cc_ip": parse_list(row.get("CC ip", "")),
                    }
                    if names:
                        extra["file_name"] = names[0]
                    args.metadata_dict[hash_val] = extra
            logging.info(
                f"[i] Parsed metadata for {len(args.metadata_dict)} hashes from {args.metadata}"
            )
        except Exception as e:
            logging.error(f"[!] Failed to parse metadata file {args.metadata}: {e}")

    logging.info(f"[i] Processing targets using profile: {args.profile}")
    print(
        f"[i] Uploading to collections {args.collections} on hosts {args.hosts} with batch uuid {args.batch_uuid}"
    )

    if getattr(args, "limit", 0) > 0:
        args.targets = args.targets[: args.limit]
        logging.info(f"[i] Capping upload targets to strictly {args.limit} binaries.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_target = {
            executor.submit(worker, target, args, config, batch_order): target
            for batch_order, target in enumerate(args.targets)
        }

        success_count = 0
        duplicate_count = 0
        failed_count = 0
        total = len(args.targets)
        pipeline_details = []

        # Progress bar setup
        # unit="bin" makes it say "10bin/s"
        with tqdm(
            total=total, desc="Analyzing", unit="bin", dynamic_ncols=True
        ) as pbar:
            for future in concurrent.futures.as_completed(future_to_target):
                target_name = future_to_target[future]
                try:
                    result, p_details = future.result()
                    if result == UPLOAD_OK:
                        success_count += 1
                        pipeline_details.extend(p_details)
                    elif result == UPLOAD_DUPLICATE:
                        duplicate_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    # tqdm.write ensures the progress bar stays at the bottom
                    # while the error message is printed above it
                    pbar.write(f"[!] Exception in job for {target_name}: {e}")
                    failed_count += 1

                pbar.update(1)

        # Report the three outcomes separately. The SightHouse bridge used to
        # regex-scrape this success-rate line because it was the only signal
        # available; the counts and the exit code are the interface now.
        rate = (success_count / total * 100) if total > 0 else 0
        print(f"[i] Success rate : {rate:.2f}% ({success_count}/{total})")
        print(f"[i] Uploaded     : {success_count}")
        print(f"[i] Skipped (dup): {duplicate_count}")
        print(f"[i] Failed       : {failed_count}")

        if pipeline_details:
            # Group pipeline IDs by (host, collection)
            groups = {}
            for detail in pipeline_details:
                key = (detail["host"], detail["collection"])
                if key not in groups:
                    groups[key] = []
                groups[key].append(detail["pipeline_id"])

            print(
                f"[i] Grouping and finalizing batch uploads for {len(groups)} host/collection pairs..."
            )

            for (host, collection), pipeline_ids in groups.items():
                api_url = f"http://{host}/api/file/upload/batch_finalize"
                payload = {
                    "pipeline_ids": pipeline_ids,
                    "batch_uuid": args.batch_uuid,
                    "collection": collection,
                    "algo": getattr(args, "algo", "unweighted_cosine")
                    or "unweighted_cosine",
                    "skip_sim": getattr(args, "skip_sim", False),
                }
                try:
                    logging.info(
                        f"[*] Calling batch finalize on {host} (collection: {collection})..."
                    )
                    resp = requests.post(api_url, json=payload, timeout=300)
                    resp.raise_for_status()
                    result = resp.json()
                    print(
                        f"[+] Batch finalize success on {host}! Master Pipeline ID: {result.get('master_pipeline_id')}"
                    )
                except Exception as e:
                    logging.error(f"[!] Batch finalize failed for {api_url}: {e}")
                    failed_count += 1

        # Exit code reflects real failures only. It used to be 0 unconditionally,
        # so a run where every single file failed still looked like a success to
        # any caller or CI step.
        return 1 if failed_count else 0


def load_config(path=DEFAULT_CONFIG_NAME):
    with open(path, "rb") as f:
        return tomllib.load(f)


def cli_main():
    start = time.time()

    parser = argparse.ArgumentParser(prog="BSimVis", description="...", epilog="...")

    parser.add_argument(
        "targets",
        nargs="+",
        help="Path to Ghidra project (.gpr), a specific binary, or a directory/*",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        default=0,
        help="Increase output verbosity (e.g., -v, -vv, -vvv)",
        action="count",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of targets processed (useful with *)",
    )

    parser.add_argument(
        "-H",
        "--host",
        dest="hosts",
        action="append",
        metavar="HOST",
        required=True,
        default=[],
        help="Host address (can be specified multiple times)",
    )

    parser.add_argument(
        "-n",
        "--threads",
        type=int,
        default=1,
        help="Number of threads to use (default: 1)",
    )

    parser.add_argument(
        "-t",
        "--tag",
        dest="tags",
        action="append",
        metavar="TAG",
        default=[],
        help="Tag to filter by (can be specified multiple times)",
    )

    parser.add_argument(
        "-c",
        "--collection",
        dest="collections",
        action="append",
        metavar="NAME",
        default=[],
        help="Collections to include (default: 'main' if none provided)",
    )

    parser.add_argument(
        "-C",
        "--config",
        dest="config",
        default=DEFAULT_CONFIG_NAME,
        metavar="FILE",
        help="Config file",
    )

    parser.add_argument(
        "--metadata",
        metavar="FILE",
        help="Path to a metadata CSV file to enrich uploaded binaries",
    )

    decomp_args = parser.add_argument_group("Decompilation options")

    decomp_args.add_argument(
        "--va",
        "--verbose-analysis",
        dest="verbose_analysis",
        help="Verbose logging for analysis step.",
        action="store_true",
        default=False,
    )
    # decomp_args.add_argument('-d', '--decompilers', dest="decompilers", help='Number of parallel decompilers', type=int,default=1)
    decomp_args.add_argument("--temp-dir", metavar="DIR", default=None)
    decomp_args.add_argument(
        "-p",
        "--profile",
        dest="profile",
        default="fast",
        help="Profile for ghidra analysis options",
    )
    decomp_args.add_argument(
        "--min-func-len",
        help="Minimum function length to be considered",
        type=int,
        default=10,
    )

    ghidra_import = parser.add_argument_group("Ghidra Import Options")
    ghidra_import.add_argument(
        "--processor",
        dest="processor",
        help="Force a specific Ghidra Language ID (e.g., 'x86:LE:64:default')",
        default=None,
    )
    ghidra_import.add_argument(
        "--cspec",
        dest="cspec",
        help="Force a specific Ghidra Compiler Spec ID (e.g., 'gcc')",
        default=None,
    )

    jvm_options = parser.add_argument_group("JVM Options")
    jvm_options.add_argument(
        "--max-ram-percent", help="Set JVM Max Ram %% of host RAM", default=60.0
    )
    jvm_options.add_argument(
        "--print-flags",
        help="Print JVM flags at start",
        action="store_true",
        default=False,
    )
    jvm_options.add_argument(
        "--jvm-args", nargs="?", help="JVM args to add at start", default=None
    )

    batch_options = parser.add_argument_group("Batch Options")
    batch_options.add_argument("--batch-uuid", help="Batch uuid", default=None)
    batch_options.add_argument(
        "--batch-name", help="Batch name", default=DEFAULT_BATCH_NAME
    )

    sim_options = parser.add_argument_group("Similarity Options")
    sim_options.add_argument(
        "-k", "--top-k", type=int, default=None, help="Top K matches per function"
    )
    sim_options.add_argument(
        "--min-score", type=float, default=None, help="Minimum similarity score"
    )
    sim_options.add_argument(
        "--min-features", type=int, default=None, help="Minimum number of features"
    )
    sim_options.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        help="Similarity algorithm to use (default: unweighted_cosine)",
    )
    sim_options.add_argument(
        "--skip-sim",
        action="store_true",
        default=False,
        help="Skip building similarities after upload",
    )

    args = parser.parse_args()

    if args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(level=level)

    main(args)

    end = time.time()

    print(f"[i] Total time : {end - start:.6f} seconds")


if __name__ == "__main__":
    cli_main()
