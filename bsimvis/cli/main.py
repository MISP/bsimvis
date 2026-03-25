import argparse
import sys
import logging
import time
from bsimvis.cli import bsimvis_setup, bsimvis_index, bsimvis_diff, bsimvis_upload, bsimvis_batch

def main():
    parser = argparse.ArgumentParser(prog="bsimvis", description="Unified BSimVis CLI")
    parser.add_argument("-H", "--host", default="localhost:6666", help="Default Redis/Kvrocks host:port (default: localhost:6666)")
    
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    
    # --- SETUP ---
    setup_parser = subparsers.add_parser("setup", help="System setup")
    setup_actions = setup_parser.add_subparsers(dest="action", required=True)
    ft_parser = setup_actions.add_parser("ftsearch", help="Setup search indexes")
    ft_parser.add_argument("-c", "--collection", required=True, help="Collection name")
    ft_parser.add_argument("-i", "--index", nargs="+", default=["functions", "files", "similarities"], 
                          help="Index types (default: all)")

    # --- INDEX ---
    index_parser = subparsers.add_parser("index", help="Index management")
    index_actions = index_parser.add_subparsers(dest="action", required=True)
    
    # index status
    idx_status = index_actions.add_parser("status", help="Quick indexing check")
    idx_status.add_argument("-c", "--collection", required=True, help="Collection name")
    idx_status.add_argument("--batch", help="Filter by batch UUID")

    # index list
    idx_list = index_actions.add_parser("list", help="Show batch table and ratios")
    idx_list.add_argument("-c", "--collection", required=True, help="Collection name")
    idx_list.add_argument("--batch", help="Filter by batch UUID")

    # index build
    idx_build = index_actions.add_parser("build", help="Index missing functions")
    idx_build.add_argument("-c", "--collection", required=True, help="Collection name")
    idx_build.add_argument("--batch", help="Index a specific batch UUID")
    idx_build.add_argument("--all", action="store_true", help="Clear and rebuild everything")
    idx_build.add_argument("--sync", action="store_true", help="Sync batch mappings (scan)")
    idx_build.add_argument("--md5", help="Index functions for a specific file")

    # index rebuild
    idx_rebuild = index_actions.add_parser("rebuild", help="Clear and rebuild")
    idx_rebuild.add_argument("-c", "--collection", required=True, help="Collection name")
    idx_rebuild.add_argument("--batch", help="Rebuild a specific batch UUID")
    idx_rebuild.add_argument("--md5", help="Rebuild a specific file")

    # index clear
    idx_clear = index_actions.add_parser("clear", help="Remove indexing data")
    idx_clear.add_argument("-c", "--collection", required=True, help="Collection name")
    clear_group = idx_clear.add_mutually_exclusive_group(required=True)
    clear_group.add_argument("--batch", help="Clear a specific batch UUID")
    clear_group.add_argument("--all", action="store_true", help="Clear everything in the collection")
    idx_clear.add_argument("--md5", help="Clear functions for a specific file")

    # --- DIFF ---
    diff_parser = subparsers.add_parser("diff", help="Similarity analytics")
    diff_actions = diff_parser.add_subparsers(dest="action", required=True)
    
    for action in ["status", "build", "rebuild", "clear"]:
        dp = diff_actions.add_parser(action, help=f"{action.capitalize()} similarity scores")
        dp.add_argument("-c", "--collection", required=True, help="Collection name")
        dp.add_argument("--batch", help="Target specific batch UUID")
        if action in ["build", "rebuild"]:
            dp.add_argument("--algo", default="unweighted_cosine", choices=["jaccard", "unweighted_cosine"])
            dp.add_argument("-k", "--top-k", type=int, default=20)
            dp.add_argument("--threshold", type=float, default=0.1)
            dp.add_argument("--delay", type=float, default=0.0)

    # --- BATCH ---
    batch_parser = subparsers.add_parser("batch", help="Batch management")
    batch_actions = batch_parser.add_subparsers(dest="action", required=True)
    
    b_list = batch_actions.add_parser("list", help="List all batches")
    b_list.add_argument("-c", "--collection", required=True, help="Collection name")
    
    b_remove = batch_actions.add_parser("remove", help="Remove a batch and its data")
    b_remove.add_argument("-c", "--collection", required=True, help="Collection name")
    b_remove.add_argument("--batch", required=True, help="Batch UUID to remove")

    # --- UPLOAD ---
    upload_parser = subparsers.add_parser("upload", help="Push data to Redis")
    
    # Mirroring EXACT arguments from bsimvis_upload.py
    upload_parser.add_argument(
        "targets", nargs="+", help="Path to Ghidra project (.gpr), a specific binary, or a directory/*"
    )
    upload_parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase output verbosity"
    )
    upload_parser.add_argument(
        "-H", "--host", dest="hosts", action="append", metavar="HOST", help="Host address (can be specified multiple times)"
    )
    upload_parser.add_argument(
        "-n", "--threads", type=int, default=1, help="Number of threads to use (default: 1)"
    )
    upload_parser.add_argument(
        "-t", "--tag", dest="tags", action="append", metavar="TAG", default=[], help="Tag to filter by"
    )
    upload_parser.add_argument(
        "-c", "--collection", dest="collections", action="append", metavar="NAME", default=[], help="Collections to include"
    )
    upload_parser.add_argument(
        "-C", "--config", dest="config", default="bsimvis_config.toml", metavar="FILE", help="Config file"
    )

    decomp_args = upload_parser.add_argument_group("Decompilation options")
    decomp_args.add_argument('--va', '--verbose-analysis', dest="verbose_analysis", action='store_true', default=False)
    #decomp_args.add_argument('-d', '--decompilers', dest="decompilers", type=int, default=1)
    decomp_args.add_argument('--temp-dir', metavar="DIR", default=None)
    decomp_args.add_argument("-p", "--profile", dest="profile", default="fast", help="Profile for ghidra analysis options")
    decomp_args.add_argument('--min-func-len', type=int, default=10)
    
    jvm_options = upload_parser.add_argument_group('JVM Options')
    jvm_options.add_argument('--max-ram-percent', type=float, default=60.0)
    jvm_options.add_argument('--print-flags', action='store_true', default=False)
    jvm_options.add_argument('--jvm-args', nargs='?', help='JVM args to add at start', default=None)

    batch_options = upload_parser.add_argument_group('Batch Options')
    batch_options.add_argument("--batch-uuid", help="Batch uuid", default=None)
    batch_options.add_argument("--batch-name", help="Batch name", default="Ghidra Batch")

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
        elif args.subcommand == "index":
            bsimvis_index.run_index(g_host, int(g_port), args)
        elif args.subcommand == "diff":
            bsimvis_diff.run_diff(g_host, int(g_port), args)
        elif args.subcommand == "upload":
            # If no hosts provided in subcommand, use the global one
            if not args.hosts:
                args.hosts = [args.host]
            # No need to inject g_host/g_port to run_upload because it uses args.hosts
            bsimvis_upload.run_upload(None, None, args)
        elif args.subcommand == "batch":
            bsimvis_batch.run_batch(g_host, int(g_port), args)
            
    except Exception as e:
        import traceback
        logging.error(f"Execution failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
