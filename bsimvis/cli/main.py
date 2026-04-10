import argparse
import sys
import logging
import time
import tomllib
import os
from bsimvis.cli import (
    bsimvis_index,
    bsimvis_sim,
    bsimvis_upload,
    bsimvis_features,
    bsimvis_job,
    bsimvis_worker,
)


def main():
    parser = argparse.ArgumentParser(prog="bsimvis", description="Unified BSimVis CLI")
    parser.add_argument(
        "-H",
        "--host",
        default=None,
        help="API host:port (default: localhost:5000 or from bsimvis_config.toml)",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # --- FEATURES (formerly Index) ---
    features_parser = subparsers.add_parser(
        "features", help="BSim Feature management (Indexing)"
    )
    features_actions = features_parser.add_subparsers(dest="action", required=True)

    # features status
    feat_status = features_actions.add_parser(
        "status", help="Quick features indexing check"
    )
    feat_status.add_argument(
        "-c", "--collection", required=True, help="Collection name"
    )
    feat_status.add_argument("--batch", help="Filter by batch UUID")
    feat_status.add_argument("--md5", help="Filter by binary MD5")

    # features list
    feat_list = features_actions.add_parser("list", help="Show batch table and ratios")
    feat_list.add_argument("-c", "--collection", required=True, help="Collection name")
    feat_list.add_argument("--batch", help="Filter by batch UUID")
    feat_list.add_argument("--md5", action="store_true", help="List status by file (MD5)")

    # features build
    feat_build = features_actions.add_parser("build", help="Index missing functions")
    feat_build.add_argument("-c", "--collection", required=True, help="Collection name")
    feat_build.add_argument("--batch", help="Index a specific batch UUID")
    feat_build.add_argument(
        "--all", action="store_true", help="Clear and rebuild everything"
    )
    feat_build.add_argument(
        "--sync", action="store_true", help="Sync batch mappings (scan)"
    )
    feat_build.add_argument("--md5", help="Index functions for a specific file")

    # features rebuild
    feat_rebuild = features_actions.add_parser("rebuild", help="Clear and rebuild")
    feat_rebuild.add_argument(
        "-c", "--collection", required=True, help="Collection name"
    )
    feat_rebuild.add_argument("--batch", help="Rebuild a specific batch UUID")
    feat_rebuild.add_argument("--md5", help="Rebuild a specific file")

    # features clear
    feat_clear = features_actions.add_parser("clear", help="Remove indexing data")
    feat_clear.add_argument("-c", "--collection", required=True, help="Collection name")
    clear_group_feat = feat_clear.add_mutually_exclusive_group(required=True)
    clear_group_feat.add_argument("--batch", help="Clear a specific batch UUID")
    clear_group_feat.add_argument(
        "--all", action="store_true", help="Clear everything in the collection"
    )
    feat_clear.add_argument("--md5", help="Clear functions for a specific file")

    # features reindex
    feat_reindex = features_actions.add_parser(
        "reindex", help="Rebuild secondary indexes from JSON meta"
    )
    feat_reindex.add_argument(
        "-c", "--collection", required=True, help="Collection name"
    )

    # --- INDEX (Stats & Health) ---
    index_parser = subparsers.add_parser("index", help="Index health and statistics")
    index_actions = index_parser.add_subparsers(dest="action", required=True)
    index_status = index_actions.add_parser(
        "status", help="Show database index statistics"
    )
    index_status.add_argument(
        "-c", "--collection", required=True, help="Collection name"
    )
    index_reg = index_actions.add_parser(
        "reg", help="Show cardinality of all metadata registries"
    )
    index_reg.add_argument(
        "-c", "--collection", required=False, help="Filter by specific collection"
    )

    # --- SIM ---
    sim_parser = subparsers.add_parser("sim", help="Similarity management")
    sim_actions = sim_parser.add_subparsers(dest="action", required=True)

    for action in ["status", "scores", "build", "rebuild", "clear"]:
        dp = sim_actions.add_parser(
            action, help=f"{action.capitalize()} similarity scores"
        )
        dp.add_argument("-c", "--collection", required=True, help="Collection name")
        dp.add_argument("--batch", help="Target specific batch UUID")
        dp.add_argument("--md5", action="append", help="Filter by binary MD5")
        dp.add_argument(
            "--func", action="append", help="Filter by specific function ID/pattern"
        )
        dp.add_argument(
            "--algo",
            choices=["jaccard", "unweighted_cosine"],
            help="Algorithm to target",
        )

        if action in ["build", "rebuild"]:
            # Set default for build/rebuild if not provided
            dp.set_defaults(algo="unweighted_cosine")
            dp.add_argument(
                "-k", "--top-k", type=int, default=1000, help="Top K matches per function"
            )
            dp.add_argument("--min-score", type=float, default=0)
            dp.add_argument("--delay", type=float, default=0.0)
            dp.add_argument(
                "--batch-size", type=int, default=100, help="Internal SCAN batch size"
            )
            dp.add_argument(
                "--ignore-indexing",
                action="store_true",
                help="Build even for functions not in indexed:functions set",
            )

    # sim list
    sim_list = sim_actions.add_parser("list", help="List similarity builds")
    sim_list.add_argument("-c", "--collection", required=True, help="Collection name")
    sim_list.add_argument("--batch", help="Target specific batch UUID")
    sim_list.add_argument("--md5", action="store_true", help="List status by file (MD5)")
    sim_list.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine"],
        help="Algorithm to filter",
    )
    # --- JOB ---
    job_parser = subparsers.add_parser("job", help="Job & Pipeline management")
    job_actions = job_parser.add_subparsers(dest="action", required=True)
    
    j_list = job_actions.add_parser("list", help="List recent jobs")
    j_list.add_argument("--limit", type=int, default=20)
    
    j_status = job_actions.add_parser("status", help="Get job status & logs")
    j_status.add_argument("job_id", nargs="?", help="Job or Pipeline ID (optional for global stats)")
    j_status.add_argument("--watch", action="store_true", help="Watch progress")
    j_status.add_argument("--logs", action="store_true", help="Show logs")
    
    j_perf = job_actions.add_parser("perf", help="Display performance statistics for a job or pipeline")
    j_perf.add_argument("job_id", help="Job or Pipeline ID")
    j_perf.add_argument("--top", type=int, default=10, help="Show top N most demanding DB commands (default: 10)")
    
    j_cancel = job_actions.add_parser("cancel", help="Cancel a job")
    j_cancel.add_argument("job_id", help="Job or Pipeline ID")

    # --- WORKER ---
    worker_parser = subparsers.add_parser("worker", help="Worker management")
    worker_actions = worker_parser.add_subparsers(dest="action", required=True)
    w_start = worker_actions.add_parser("start", help="Start background workers")
    w_start.add_argument("-n", "--count", type=int, default=1, help="Number of workers to start")

    # --- UPLOAD ---
    upload_parser = subparsers.add_parser(
        "upload", help="Upload binaries to redis/kvrocks"
    )

    # Mirroring EXACT arguments from bsimvis_upload.py
    upload_parser.add_argument(
        "--save-json",
        metavar="PATH",
        help="Save analyzed JSON data to a file instead of (or in addition to) uploading",
    )
    upload_parser.add_argument(
        "targets",
        nargs="+",
        help="Path to Ghidra project (.gpr), a specific binary, or a directory/*",
    )
    upload_parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase output verbosity"
    )
    upload_parser.add_argument(
        "--limit", type=int, default=0, help="Limit the number of targets processed (useful with *)"
    )
    upload_parser.add_argument(
        "-H",
        "--host",
        dest="hosts",
        action="append",
        metavar="HOST",
        help="Host address (can be specified multiple times)",
    )
    upload_parser.add_argument(
        "-n",
        "--threads",
        type=int,
        default=1,
        help="Number of threads to use (default: 1)",
    )
    upload_parser.add_argument(
        "-t",
        "--tag",
        dest="tags",
        action="append",
        metavar="TAG",
        default=[],
        help="Tag to filter by",
    )
    upload_parser.add_argument(
        "-c",
        "--collection",
        dest="collections",
        action="append",
        metavar="NAME",
        default=[],
        help="Collections to include",
    )
    upload_parser.add_argument(
        "-C",
        "--config",
        dest="config",
        default="bsimvis_config.toml",
        metavar="FILE",
        help="Config file",
    )

    decomp_args = upload_parser.add_argument_group("Decompilation options")
    decomp_args.add_argument(
        "--va",
        "--verbose-analysis",
        dest="verbose_analysis",
        action="store_true",
        default=False,
    )
    # decomp_args.add_argument('-d', '--decompilers', dest="decompilers", type=int, default=1)
    decomp_args.add_argument("--temp-dir", metavar="DIR", default=None)
    decomp_args.add_argument(
        "-p",
        "--profile",
        dest="profile",
        default="fast",
        help="Profile for ghidra analysis options",
    )
    decomp_args.add_argument("--min-func-len", type=int, default=10)
    decomp_args.add_argument(
        "--processor",
        dest="processor",
        help="Force a specific Ghidra Language ID (e.g., 'x86:LE:64:default')",
        default=None,
    )
    decomp_args.add_argument(
        "--cspec",
        dest="cspec",
        help="Force a specific Ghidra Compiler Spec ID (e.g., 'gcc')",
        default=None,
    )

    jvm_options = upload_parser.add_argument_group("JVM Options")
    jvm_options.add_argument("--max-ram-percent", type=float, default=60.0)
    jvm_options.add_argument("--print-flags", action="store_true", default=False)
    jvm_options.add_argument(
        "--jvm-args", nargs="?", help="JVM args to add at start", default=None
    )

    batch_options = upload_parser.add_argument_group("Batch Options")
    batch_options.add_argument("--batch-uuid", help="Batch uuid", default=None)
    batch_options.add_argument(
        "--batch-name", help="Batch name", default="Ghidra Batch"
    )

    # Parse and Resolve Host
    args = parser.parse_args()
    
    def resolve_api_host(cli_host):
        if cli_host:
            return cli_host
        
        config_path = "bsimvis_config.toml"
        if os.path.exists(config_path):
            try:
                with open(config_path, "rb") as f:
                    config = tomllib.load(f)
                    return config.get("bsimvis", {}).get("host", "localhost:5000")
            except Exception:
                pass
        return "localhost:5000"

    api_host_str = resolve_api_host(args.host)
    if ":" in api_host_str:
        g_host, g_port = api_host_str.split(":")
    else:
        g_host, g_port = api_host_str, 5000
    
    # For backward compatibility with things that still talk directly to Redis/Kvrocks (like setup)
    # we reuse the same host but we might need a different port if redirected.
    # For now, we assume the API host is what we use.

    try:
        if args.subcommand == "features":
            bsimvis_features.run_features(g_host, int(g_port), args)
        elif args.subcommand == "index":
            if args.action == "status":
                bsimvis_index.run_index_status(g_host, int(g_port), args)
            elif args.action == "reg":
                bsimvis_index.run_index_reg(g_host, int(g_port), args)
        elif args.subcommand == "sim":
            bsimvis_sim.run_sim(g_host, int(g_port), args)
        elif args.subcommand == "upload":
            # Pass the resolved API host to upload
            args.host = api_host_str
            if not args.hosts:
                args.hosts = [api_host_str]
            bsimvis_upload.run_upload(None, None, args)
        elif args.subcommand == "job":
            bsimvis_job.run_job(g_host, int(g_port), args)
        elif args.subcommand == "worker":
            bsimvis_worker.run_worker(g_host, int(g_port), args)

    except Exception as e:
        import traceback

        logging.error(f"Execution failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
