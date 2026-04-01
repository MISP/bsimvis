import argparse
import sys
import logging
import time
from bsimvis.cli import (
    bsimvis_setup,
    bsimvis_index,
    bsimvis_sim,
    bsimvis_upload,
    bsimvis_batch,
    bsimvis_features,
    bsimvis_cache,
)


def main():
    parser = argparse.ArgumentParser(prog="bsimvis", description="Unified BSimVis CLI")
    parser.add_argument(
        "-H",
        "--host",
        default="localhost:6666",
        help="Default Redis/Kvrocks host:port (default: localhost:6666)",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # --- SETUP ---
    setup_parser = subparsers.add_parser("setup", help="System setup")
    setup_actions = setup_parser.add_subparsers(dest="action", required=True)
    ft_parser = setup_actions.add_parser("ftsearch", help="Setup search indexes")
    ft_parser.add_argument("-c", "--collection", required=True, help="Collection name")
    ft_parser.add_argument(
        "-i",
        "--index",
        nargs="+",
        default=["functions", "files", "similarities"],
        help="Index types (default: all)",
    )

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

    # features list
    feat_list = features_actions.add_parser("list", help="Show batch table and ratios")
    feat_list.add_argument("-c", "--collection", required=True, help="Collection name")
    feat_list.add_argument("--batch", help="Filter by batch UUID")

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
    index_status.add_argument(
        "-v",
        "--verbose",
        "--details",
        dest="details",
        action="store_true",
        help="Show comprehensive space and size analysis",
    )

    # --- SIM ---
    sim_parser = subparsers.add_parser("sim", help="Similarity management")
    sim_actions = sim_parser.add_subparsers(dest="action", required=True)

    for action in ["status", "list", "build", "rebuild", "clear"]:
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
            help="Algorithm to target (default: both for clear/list, unweighted_cosine for build)",
        )

        if action in ["build", "rebuild"]:
            # Set default for build/rebuild if not provided
            dp.set_defaults(algo="unweighted_cosine")
            dp.add_argument(
                "-k", "--top-k", type=int, default=20, help="Top K matches per function"
            )
            dp.add_argument("--min-score", type=float, default=0.1)
            dp.add_argument("--delay", type=float, default=0.0)
            dp.add_argument(
                "--batch-size", type=int, default=100, help="Internal SCAN batch size"
            )
            dp.add_argument(
                "--ignore-indexing",
                action="store_true",
                help="Skip the full-index check before baking",
            )

    # --- BATCH ---
    batch_parser = subparsers.add_parser("batch", help="Batch management")
    batch_actions = batch_parser.add_subparsers(dest="action", required=True)

    b_list = batch_actions.add_parser("list", help="List all batches")
    b_list.add_argument("-c", "--collection", required=True, help="Collection name")

    b_remove = batch_actions.add_parser("remove", help="Remove a batch and its data")
    b_remove.add_argument("-c", "--collection", required=True, help="Collection name")
    b_remove.add_argument("--batch", required=True, help="Batch UUID to remove")

    # --- CACHE ---
    cache_parser = subparsers.add_parser("cache", help="Cache management")
    cache_actions = cache_parser.add_subparsers(dest="action", required=True)
    c_clear = cache_actions.add_parser("clear", help="Clear similarity search cache")
    c_clear.add_argument("-c", "--collection", help="Optional collection name filter")

    # --- UPLOAD ---
    upload_parser = subparsers.add_parser(
        "upload", help="Upload binaries to redis/kvrocks"
    )

    # Mirroring EXACT arguments from bsimvis_upload.py
    upload_parser.add_argument(
        "targets",
        nargs="+",
        help="Path to Ghidra project (.gpr), a specific binary, or a directory/*",
    )
    upload_parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase output verbosity"
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

    # Parse and Dispatch
    args = parser.parse_args()

    # Global Redis config extraction
    if ":" in args.host:
        g_host, g_port = args.host.split(":")
    else:
        g_host, g_port = args.host, 6666

    try:
        if args.subcommand == "setup":
            bsimvis_setup.run_setup(g_host, int(g_port), args)
        elif args.subcommand == "features":
            bsimvis_features.run_features(g_host, int(g_port), args)
        elif args.subcommand == "index":
            if args.action == "status":
                bsimvis_index.run_index_status(g_host, int(g_port), args)
        elif args.subcommand == "sim":
            bsimvis_sim.run_sim(g_host, int(g_port), args)
        elif args.subcommand == "upload":
            # If no hosts provided in subcommand, use the global one
            if not args.hosts:
                args.hosts = [args.host]
            # No need to inject g_host/g_port to run_upload because it uses args.hosts
            bsimvis_upload.run_upload(None, None, args)
        elif args.subcommand == "batch":
            bsimvis_batch.run_batch(g_host, int(g_port), args)
        elif args.subcommand == "cache":
            bsimvis_cache.run_cache(g_host, int(g_port), args)

    except Exception as e:
        import traceback

        logging.error(f"Execution failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
