import redis
import argparse

_r = None


def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r


def setup_collection_index(collection, r=None):
    """
    Manual secondary index setup for a collection.
    No FT.CREATE needed – indexes are built on write via index_service.
    This function initialises the bookkeeping sets if they do not exist.
    """
    r = r or get_redis()
    print(f"[*] Initialising index bookkeeping for collection '{collection}'...")

    # Ensure top-level membership sets exist (sadd is a no-op if already present)
    r.sadd("global:collections", collection)
    print(f"[+] Collection '{collection}' registered in global:collections.")
    print(f"[i] Secondary indexes are built automatically on upload via index_service.")
    print(f"[+] Done.")


def run_setup(host, port, args):
    r = get_redis(host, port)
    collections = []
    if args.collection:
        collections = (
            [args.collection] if isinstance(args.collection, str) else args.collection
        )
    if not collections:
        print("[-] No collections specified.")
        return
    setup_indices(collections, r_override=r)


def setup_indices(collections, index_types=None, r_override=None):
    # index_types kept for CLI compatibility but ignored (no FT indexes to create)
    print(
        f"[*] Setting up secondary index metadata for {len(collections)} collections..."
    )
    for coll in collections:
        print(f"\n--- Processing Collection: {coll} ---")
        setup_collection_index(coll, r_override)
    print("\n[+] All collections set up.")


def main():
    parser = argparse.ArgumentParser(description="Setup BSimVis secondary indexes.")
    parser.add_argument(
        "--all", action="store_true", help="Setup for all existing collections."
    )
    parser.add_argument(
        "-c", "--collection", nargs="+", help="Collection names to setup."
    )
    parser.add_argument(
        "-i",
        "--index",
        nargs="+",
        choices=["functions", "files", "similarities"],
        help="(Legacy) Ignored - no FT indexes used.",
    )

    args = parser.parse_args()

    if not args.all and not args.collection:
        parser.print_help()
        exit(0)

    r = get_redis()
    collections = []
    if args.all:
        collections = list(r.smembers("global:collections"))
        collections = [c.decode() if isinstance(c, bytes) else c for c in collections]
        print(f"[*] Found {len(collections)} existing collections: {collections}")

    if args.collection:
        for c in args.collection:
            if c not in collections:
                collections.append(c)

    if not collections:
        print("[-] No collections found or specified. Nothing to do.")
        exit(0)

    setup_indices(collections, index_types=args.index, r_override=r)


if __name__ == "__main__":
    main()
