import tomllib, json, uuid

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
        return

    file_meta = data.get("file_metadata", {})
    file_md5 = file_meta.get("file_md5", "unknown_md5")

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

    # We trigger the API for each collection
    for collection in collections:
        # Prepare the payload for the API
        payload = {"collection": collection, "file_md5": file_md5, **data}

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
                logging.info(
                    f"[+] Upload Success on {api_host}! Pipeline ID: {result.get('pipeline_id')}"
                )
            except Exception as e:
                logging.error(f"[!] API Submission failed for {api_url}: {e}")


def upload_raw_binary(target_path, args):
    """
    Uploads a raw binary file or a Ghidra project to the server for analysis.
    """
    target_path = Path(target_path).resolve()
    if not target_path.exists():
        logging.error(f"[!] Target path does not exist: {target_path}")
        return 0

    is_gpr = target_path.suffix == ".gpr"

    if is_gpr:
        logging.info(
            f"[*] Archiving Ghidra project {target_path.name} for remote analysis..."
        )
        archive_path = archive_ghidra_project(target_path)
        if not archive_path:
            return 0
        try:
            with open(archive_path, "rb") as f:
                raw_bytes = f.read()
            # Override file_name for the API
            file_name = target_path.name + ".zip"
            result = _perform_raw_upload(raw_bytes, file_name, args)
            return result
        finally:
            if os.path.exists(archive_path):
                os.remove(archive_path)

    if not target_path.is_file():
        logging.error(f"[!] Target path is not a file: {target_path}")
        return 0

    try:
        with open(target_path, "rb") as f:
            raw_bytes = f.read()

        if not raw_bytes:
            logging.warning(f"[!] File {target_path.name} is empty. Skipping upload.")
            return 0

        return _perform_raw_upload(raw_bytes, target_path.name, args)
    except Exception as e:
        logging.error(f"[!] Failed to read {target_path}: {e}")
        return 0


def _perform_raw_upload(raw_bytes, file_name, args):
    hosts = getattr(args, "hosts", [])
    if not hosts:
        hosts = [getattr(args, "host", "localhost:5000")]

    collections = args.collections if args.collections else ["main"]

    success = True
    for collection in collections:
        for api_host in hosts:
            params = {
                "collection": collection,
                "file_name": file_name,
                "batch_uuid": args.batch_uuid,
                "batch_name": args.batch_name,
                "profile": args.profile,
                "min_func_len": args.min_func_len,
            }
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
                logging.info(
                    f"[+] Upload Success on {api_host}! Pipeline ID: {result.get('pipeline_id')}"
                )
            except Exception as e:
                logging.error(f"[!] Upload failed for {api_url}: {e}")
                success = False

    return 1 if success else 0


def process_target(target, args, config, batch_order) -> int:
    target_path = Path(target).resolve()

    # CASE 1: Local Analysis forced
    if getattr(args, "local_analysis", False):
        try:
            t0 = time.time()
            options = vars(args).copy()
            options["batch_order"] = batch_order

            if target_path.suffix == ".gpr":
                all_data = ghidra_service.analyze_project(target_path, options)
                for data in all_data:
                    upload_bsim_data(data, args, config)
            else:
                data = ghidra_service.analyze_file(target_path, options)
                upload_bsim_data(data, args, config)

            t_total = time.time() - t0
            logging.info(
                f"[+] Local processing finished for {target_path.name} in {t_total:.3f}s"
            )
            return 1
        except Exception as e:
            logging.error(f"[!] Local processing failed for {target_path.name}: {e}")
            import traceback

            logging.error(traceback.format_exc())
            return 0

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

    # Map back to what main(args) expects
    main(args)


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
        logging.info(
            "[i] Remote analysis selected. Skipping local Ghidra JVM start."
        )

    logging.info(f"[i] Loading config {args.config}")
    config = load_config(args.config)

    if len(args.collections) == 0:
        args.collections = ["main"]

    if not args.batch_uuid:
        args.batch_uuid = str(uuid.uuid4())

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
        total = len(args.targets)

        # Progress bar setup
        # unit="bin" makes it say "10bin/s"
        with tqdm(
            total=total, desc="Analyzing", unit="bin", dynamic_ncols=True
        ) as pbar:
            for future in concurrent.futures.as_completed(future_to_target):
                target_name = future_to_target[future]
                try:
                    result = future.result()
                    if result == 1:
                        success_count += 1
                except Exception as e:
                    # tqdm.write ensures the progress bar stays at the bottom
                    # while the error message is printed above it
                    pbar.write(f"[!] Exception in job for {target_name}: {e}")

                pbar.update(1)

        rate = (success_count / total * 100) if total > 0 else 0
        print(f"[i] Success rate : {rate:.2f}% ({success_count}/{total})")


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
