import argparse
import sys
import logging
import time
import tomllib
import os
from dotenv import load_dotenv
from bsimvis.cli import (
    bsimvis_index,
    bsimvis_sim,
    bsimvis_upload,
    bsimvis_features,
    bsimvis_job,
    bsimvis_worker,
    bsimvis_cluster,
    bsimvis_binsim,
    bsimvis_collection,
    bsimvis_metadata,
)


def main():
    # Load environment variables from .env if present
    load_dotenv()

    parser = argparse.ArgumentParser(prog="bsimvis", description="Unified BSimVis CLI")
    parser.add_argument(
        "-H",
        "--host",
        default=None,
        help="API host:port (default: localhost:5000, or from .env, or from bsimvis_config.toml)",
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
    feat_list.add_argument(
        "--md5", action="store_true", help="List status by file (MD5)"
    )

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
            choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
            help="Algorithm to target",
        )

        if action in ["build", "rebuild"]:
            from bsimvis.app.services.config_service import config_service

            # Set default for build/rebuild if not provided
            dp.set_defaults(
                algo=config_service.get("similarity.algo", "unweighted_cosine")
            )
            dp.add_argument(
                "-k",
                "--top-k",
                type=int,
                default=config_service.get("similarity.top_k", 1000),
                help="Top K matches per function",
            )
            dp.add_argument(
                "--min-score",
                type=float,
                default=config_service.get("similarity.min_score", 0.9),
            )
            dp.add_argument(
                "--min-feature",
                "--min-features",
                dest="min_features",
                type=int,
                default=config_service.get("similarity.min_features", 0),
                help="Minimum number of features",
            )
            dp.add_argument("--delay", type=float, default=0.0)
            dp.add_argument(
                "--all",
                action="store_true",
                help="Build/Rebuild for all functions in the collection",
            )
            dp.add_argument(
                "--batch-size", type=int, default=100, help="Internal SCAN batch size"
            )
            dp.add_argument(
                "--ignore-indexing",
                action="store_true",
                help="Build even for functions not in indexed:functions set",
            )

    # --- CLUSTER ---
    cluster_parser = subparsers.add_parser(
        "cluster", help="Unsupervised clustering management"
    )
    cluster_actions = cluster_parser.add_subparsers(dest="action", required=True)

    # cluster build
    c_build = cluster_actions.add_parser(
        "build", help="Run HDBSCAN clustering discovery"
    )
    c_build.add_argument("-c", "--collection", required=True, help="Collection name")
    c_build.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        default="unweighted_cosine",
        help="Algorithm to cluster",
    )
    c_build.add_argument(
        "--min-cluster-size",
        type=int,
        default=5,
        help="Minimum cluster size (default: 5)",
    )
    from bsimvis.app.services.config_service import config_service

    c_build.add_argument(
        "--min-samples",
        type=int,
        help="Min samples for HDBSCAN core points",
        default=config_service.get("clustering.min_samples", 1),
    )
    c_build.add_argument(
        "--epsilon",
        type=float,
        help="HDBSCAN epsilon threshold",
        default=config_service.get("clustering.epsilon", 0.1),
    )
    c_build.add_argument(
        "--leaf-method", action="store_true", help="Use 'leaf' selection method"
    )
    c_build.add_argument(
        "--min-sim", type=float, default=0.0, help="Minimum similarity threshold"
    )
    c_build.add_argument(
        "--min-features",
        type=int,
        default=0,
        help="Minimum number of features to include a function in clustering",
    )

    # cluster rebuild
    c_rebuild = cluster_actions.add_parser(
        "rebuild", help="Clear and run HDBSCAN clustering discovery"
    )
    c_rebuild.add_argument("-c", "--collection", required=True, help="Collection name")
    c_rebuild.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        default="unweighted_cosine",
        help="Algorithm to cluster",
    )
    c_rebuild.add_argument(
        "--min-cluster-size",
        type=int,
        default=config_service.get("clustering.min_cluster_size", 2),
        help="Minimum cluster size",
    )
    c_rebuild.add_argument(
        "--min-samples",
        type=int,
        help="Min samples for HDBSCAN core points",
        default=config_service.get("clustering.min_samples", 1),
    )
    c_rebuild.add_argument(
        "--epsilon",
        type=float,
        help="HDBSCAN epsilon threshold",
        default=config_service.get("clustering.epsilon", 0.1),
    )
    c_rebuild.add_argument(
        "--leaf-method", action="store_true", help="Use 'leaf' selection method"
    )
    c_rebuild.add_argument(
        "--min-sim", type=float, default=0.0, help="Minimum similarity threshold"
    )
    c_rebuild.add_argument(
        "--min-features",
        type=int,
        default=0,
        help="Minimum number of features to include a function in clustering",
    )

    # cluster clear
    c_clear = cluster_actions.add_parser("clear", help="Remove clustering data")
    c_clear.add_argument("-c", "--collection", required=True, help="Collection name")
    c_clear.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        default="unweighted_cosine",
        help="Algorithm to target",
    )

    # cluster list
    c_list = cluster_actions.add_parser(
        "list", help="List discovered clusters or members"
    )
    c_list.add_argument("-c", "--collection", required=True, help="Collection name")
    c_list.add_argument("--cluster-id", help="See members of a specific cluster")
    c_list.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        default="unweighted_cosine",
    )
    c_list.add_argument("--limit", type=int, default=100)
    c_list.add_argument("--offset", type=int, default=0)

    # --- BINSIM ---
    binsim_parser = subparsers.add_parser(
        "binsim", help="Binary-level similarity management"
    )
    binsim_actions = binsim_parser.add_subparsers(dest="action", required=True)

    # binsim build
    bs_build = binsim_actions.add_parser("build", help="Build binary similarities")
    bs_build.add_argument("-c", "--collection", required=True, help="Collection name")
    bs_build.add_argument("--algo", default="unweighted_cosine", help="Algorithm")
    bs_build.add_argument("--md5-a", help="First binary MD5 (optional)")
    bs_build.add_argument("--md5-b", help="Second binary MD5 (optional)")
    bs_build.add_argument(
        "--min-cohesion", type=float, default=0.0, help="Min cohesion"
    )

    # binsim rebuild
    bs_rebuild = binsim_actions.add_parser(
        "rebuild", help="Clear and build binary similarities"
    )
    bs_rebuild.add_argument("-c", "--collection", required=True, help="Collection name")
    bs_rebuild.add_argument("--algo", default="unweighted_cosine", help="Algorithm")
    bs_rebuild.add_argument("--md5-a", help="First binary MD5 (optional)")
    bs_rebuild.add_argument("--md5-b", help="Second binary MD5 (optional)")
    bs_rebuild.add_argument(
        "--min-cohesion", type=float, default=0.0, help="Min cohesion"
    )

    # binsim clear
    bs_clear = binsim_actions.add_parser("clear", help="Clear binary similarities")
    bs_clear.add_argument("-c", "--collection", required=True, help="Collection name")
    bs_clear.add_argument("--algo", default="unweighted_cosine", help="Algorithm")
    bs_clear.add_argument("--md5", help="Target specific MD5")

    # binsim list
    bs_list = binsim_actions.add_parser("list", help="List similar binaries")
    bs_list.add_argument("-c", "--collection", required=True, help="Collection name")
    bs_list.add_argument("--algo", default="unweighted_cosine", help="Algorithm")
    bs_list.add_argument("--md5", required=True, help="Target specific MD5")
    bs_list.add_argument("--limit", type=int, default=20)
    bs_list.add_argument("--offset", type=int, default=0)

    # binsim diff
    bs_diff = binsim_actions.add_parser("diff", help="Get binary similarity diff")
    bs_diff.add_argument("-c", "--collection", required=True, help="Collection name")
    bs_diff.add_argument("--algo", default="unweighted_cosine", help="Algorithm")
    bs_diff.add_argument("--md5-a", required=True, help="First binary MD5")
    bs_diff.add_argument("--md5-b", required=True, help="Second binary MD5")

    # sim list
    sim_list = sim_actions.add_parser("list", help="List similarity builds")
    sim_list.add_argument("-c", "--collection", required=True, help="Collection name")
    sim_list.add_argument("--batch", help="Target specific batch UUID")
    sim_list.add_argument(
        "--md5", action="store_true", help="List status by file (MD5)"
    )
    sim_list.add_argument(
        "--algo",
        choices=["jaccard", "unweighted_cosine", "milvus_sparse"],
        help="Algorithm to filter",
    )
    # --- JOB ---
    job_parser = subparsers.add_parser("job", help="Job & Pipeline management")
    job_actions = job_parser.add_subparsers(dest="action", required=True)

    j_list = job_actions.add_parser("list", help="List recent jobs")
    j_list.add_argument("--limit", type=int, default=20)
    j_list.add_argument(
        "-t", "--tree", action="store_true", help="Show hierarchy as a tree"
    )
    j_list.add_argument(
        "-d",
        "--depth",
        type=int,
        default=2,
        help="Max depth in tree mode (0 = unlimited, default: 2)",
    )
    j_list.add_argument(
        "--follow", action="store_true", help="Keep refreshing the output every 2s"
    )
    j_list.add_argument(
        "-p", "--parent", help="Filter: show children of this job/pipeline ID"
    )
    j_list.add_argument("-c", "--collection", help="Filter: jobs for this collection")
    j_list.add_argument("--pool", help="Filter: jobs for this pool")

    j_status = job_actions.add_parser("status", help="Get job status & logs")
    j_status.add_argument(
        "job_id", nargs="?", help="Job or Pipeline ID (optional for global stats)"
    )
    j_status.add_argument("--watch", action="store_true", help="Watch progress")
    j_status.add_argument("--logs", action="store_true", help="Show logs")

    j_perf = job_actions.add_parser(
        "perf", help="Display performance statistics for a job or pipeline"
    )
    j_perf.add_argument("job_id", help="Job or Pipeline ID")
    j_perf.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N most demanding DB commands (default: 10)",
    )

    j_cancel = job_actions.add_parser("cancel", help="Cancel a job")
    j_cancel.add_argument("job_id", help="Job or Pipeline ID")

    j_retry = job_actions.add_parser("retry", help="Retry a failed or cancelled job")
    j_retry.add_argument("job_id", help="Job or Pipeline ID")

    # --- WORKER ---
    worker_parser = subparsers.add_parser("worker", help="Worker management")
    worker_actions = worker_parser.add_subparsers(dest="action", required=True)
    w_start = worker_actions.add_parser("start", help="Start background workers")
    w_start.add_argument(
        "-n",
        "--count",
        type=int,
        default=int(os.getenv("WORKERS_COUNT", 1)),
        help="Number of workers to start (default: from .env WORKERS_COUNT or 1)",
    )

    # --- UPLOAD ---
    upload_parser = subparsers.add_parser(
        "upload", help="Upload binaries to redis/kvrocks"
    )

    # Mirroring EXACT arguments from bsimvis_upload.py
    upload_parser.add_argument(
        "--local-analysis",
        action="store_true",
        default=False,
        help="Perform Ghidra analysis locally instead of on the server",
    )

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
        "--limit",
        type=int,
        default=0,
        help="Limit the number of targets processed (useful with *)",
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

    upload_parser.add_argument(
        "--metadata",
        metavar="FILE",
        help="Path to a metadata CSV file to enrich uploaded binaries",
    )

    upload_parser.add_argument(
        "--archive-password",
        dest="archive_password",
        metavar="PASSWORD",
        default=None,
        help="Password for uploaded zip archives (server default: infected). "
        "Archives are unpacked server-side and every member analyzed.",
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

    sim_options = upload_parser.add_argument_group("Similarity Options")
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

    # --- COLLECTION ---
    collection_parser = subparsers.add_parser(
        "collection", help="Collection management"
    )
    collection_actions = collection_parser.add_subparsers(dest="action", required=True)

    collection_delete = collection_actions.add_parser(
        "delete", help="Wipe and delete a collection completely"
    )
    collection_delete.add_argument(
        "-c", "--collection", required=True, help="Collection name to delete"
    )

    collection_clean = collection_actions.add_parser(
        "clean", help="Clean up temporary raw/JSON upload keys in a collection"
    )
    collection_clean.add_argument(
        "-c", "--collection", required=True, help="Collection name to clean"
    )

    # --- METADATA ---
    metadata_parser = subparsers.add_parser(
        "metadata", help="Metadata management and propagation"
    )
    metadata_actions = metadata_parser.add_subparsers(dest="action", required=True)

    metadata_propagate = metadata_actions.add_parser(
        "propagate", help="Propagate metadata from a CSV file"
    )
    metadata_propagate.add_argument(
        "-m",
        "--metadata",
        required=True,
        help="Path to pipe-delimited metadata CSV file",
    )
    metadata_propagate.add_argument(
        "-c", "--collection", required=True, help="Target collection name"
    )

    # Parse and Resolve Host
    args = parser.parse_args()

    def resolve_api_host(cli_host):
        if cli_host:
            return cli_host

        env_host = os.getenv("APP_HOST")
        env_port = os.getenv("APP_PORT")
        if env_host and env_port:
            return f"{env_host}:{env_port}"
        elif env_host:
            return f"{env_host}:5000"
        elif env_port:
            return f"localhost:{env_port}"

        config_path = "bsimvis_config.toml"
        if os.path.exists(config_path):
            try:
                with open(config_path, "rb") as f:
                    config = tomllib.load(f)
                    return config.get("bsimvis", {}).get("host", "localhost:5000")
            except Exception:
                pass
        return "localhost:5000"

    # Use the first host from args.hosts if it exists and args.host is None
    effective_host = args.host
    if effective_host is None and hasattr(args, "hosts") and args.hosts:
        effective_host = args.hosts[0]

    api_host_str = resolve_api_host(effective_host)
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
            # Propagate the failure count as an exit code: upload used to exit 0
            # even when every file failed.
            rc = bsimvis_upload.run_upload(None, None, args)
            if rc:
                sys.exit(rc)
        elif args.subcommand == "job":
            bsimvis_job.run_job(g_host, int(g_port), args)
        elif args.subcommand == "worker":
            bsimvis_worker.run_worker(g_host, int(g_port), args)
        elif args.subcommand == "cluster":
            bsimvis_cluster.run_cluster(g_host, int(g_port), args)
        elif args.subcommand == "binsim":
            bsimvis_binsim.run_binsim(g_host, int(g_port), args)
        elif args.subcommand == "collection":
            bsimvis_collection.run_collection(g_host, int(g_port), args)
        elif args.subcommand == "metadata":
            bsimvis_metadata.run_metadata(g_host, int(g_port), args)

    except Exception as e:
        import traceback

        logging.error(f"Execution failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
