import redis
import json
import time
import math
import argparse
import logging
import datetime
from bsimvis.app.services.similarity_service import SimilarityService

_r = None


def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r


# Refactored: Lua scripts moved to SimilarityService


def is_fully_indexed(collection, r):
    indexed_set = f"idx:{collection}:indexed:functions"
    total = 0
    indexed = 0

    batch_uuids = list(r.smembers("global:batches"))
    for b_uuid in batch_uuids:
        batch_func_set = f"{collection}:batch:{b_uuid}:functions"
        if not r.exists(batch_func_set):
            continue

        b_total = r.scard(batch_func_set)
        total += b_total
        try:
            b_indexed = r.execute_command(
                "SINTERCARD", "2", batch_func_set, indexed_set
            )
        except:
            b_indexed = len(r.sinter(batch_func_set, indexed_set))
        indexed += b_indexed

    if total == 0:
        return False, 0, 0
    return indexed == total, indexed, total


def bake_similarities(args, r=None):
    r = r or get_redis()

    # --- Index Status Check ---
    if not getattr(args, "ignore_indexing", False):
        is_ok, indexed, total = is_fully_indexed(args.collection, r)
        if not is_ok:
            if total == 0:
                print(f"[!] ERROR: No data found for collection '{args.collection}'.")
            else:
                print(
                    f"[!] ERROR: Collection '{args.collection}' is not fully indexed ({indexed}/{total})."
                )
                print(
                    f"    Please run 'bsimvis features build -c {args.collection}' first, or use --ignore-indexing."
                )
            return

    service = SimilarityService(r)

    collection = args.collection
    algo = args.algo
    top_k = args.top_k
    min_score = args.min_score
    baked_set_key = f"idx:{collection}:baked:functions:{algo}"

    print(
        f"[*] Sim Bake | Collection: {collection} | Algo: {algo} | Min Score: {min_score}"
    )

    # Selecting targets based on filters
    candidate_keys = []

    # 1. Binary Filters (MD5s)
    if args.md5:
        for md5 in args.md5:
            candidate_keys.extend(r.keys(f"{collection}:function:{md5}:*:vec:tf"))

    # 2. Function Filters
    if args.func:
        for f in args.func:
            if ":" in f:
                candidate_keys.append(f"{collection}:function:{f}:vec:tf")
            else:
                pattern = f"{collection}:function:*:{f}:vec:tf"
                candidate_keys.extend(r.keys(pattern))

    # 3. Batch Filters
    if getattr(args, "batch", None):
        batch_func_set = f"{collection}:batch:{args.batch}:functions"
        funcs = r.smembers(batch_func_set)
        for f_id in funcs:
            candidate_keys.append(f"{f_id}:vec:tf")

    # If no filters, candidate_keys remains empty but we skip to SCAN
    if not args.md5 and not args.func and not getattr(args, "batch", None):
        candidate_keys = None

    start_time = time.time()
    processed = 0
    skipped = 0

    if candidate_keys is not None:
        # Use set to avoid duplicates if multiple filters overlap
        unique_keys = sorted(list(set(candidate_keys)))
        print(f"[*] Found {len(unique_keys)} keys in batch")
        for key in unique_keys:
            if process_single_key(
                service, key, collection, algo, top_k, min_score, baked_set_key
            ):
                processed += 1
            else:
                skipped += 1
            if processed > 0 and processed % 10 == 0:
                print_progress(processed, start_time, collection, skipped)
            if args.delay > 0:
                time.sleep(args.delay)
    else:
        # Full SCAN mode
        cursor = 0
        match_pattern = f"{collection}:function:*:*:vec:tf"
        while True:
            cursor, keys = r.scan(
                cursor=cursor, match=match_pattern, count=args.batch_size
            )
            for key in keys:
                if process_single_key(
                    service, key, collection, algo, top_k, min_score, baked_set_key
                ):
                    processed += 1
                else:
                    skipped += 1
                if processed > 0 and processed % 50 == 0:
                    print_progress(processed, start_time, collection, skipped)
                if args.delay > 0:
                    time.sleep(args.delay)
            if cursor == 0:
                break

    print(
        f"[+] DONE: {processed} new functions baked ({skipped} skipped) in {time.time() - start_time:.2f}s"
    )


def process_single_key(service, key, collection, algo, top_k, min_score, baked_set_key):
    # key: coll:function:md5:addr:vec:tf
    base_id = key.replace(":vec:tf", "")
    return service.bake_function(collection, base_id, algo, top_k, min_score)


def print_progress(count, start, collection, skipped=0):
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    skip_str = f" ({skipped} skipped)" if skipped > 0 else ""
    print(
        f"  [i] {collection}: {count} functions baked{skip_str} ({rate:.1f} func/s)..."
    )


def run_sim(host, port, args):
    r = get_redis(host, port)
    coll = args.collection

    if args.action == "status":
        sim_quick_status(coll, r=r)
    elif args.action == "list":
        list_sim_batches(coll, batch_filter=args.batch, r=r)
    elif args.action == "build":
        bake_similarities(args, r=r)

    elif args.action == "rebuild":
        print(f"[*] Clearing and rebuilding similarities for: {coll}")
        sim_clear(coll, batch=args.batch, md5s=args.md5, algo=args.algo, r=r)
        bake_similarities(args, r=r)

    elif args.action == "clear":
        sim_clear(coll, batch=args.batch, md5s=args.md5, algo=args.algo, r=r)


def sim_clear(collection, batch=None, md5s=None, algo=None, r=None):
    r = r or get_redis()
    service = SimilarityService(r)
    algo_str = f", algorithm: {algo}" if algo else ""

    # 1. Clear by Batch
    if batch:
        print(
            f"[*] Clearing similarity data for collection: {collection}, batch: {batch}{algo_str}..."
        )
        try:
            service.clear_filtered(collection, "batch_uuid", batch, algo=algo)
            print(f"[+] Similarity data for batch '{batch}' cleared.")
        except Exception as e:
            print(f"[!] Error clearing batch similarity: {e}")
        if not md5s:
            return

    # 2. Clear by MD5s
    if md5s:
        for md5 in md5s:
            print(
                f"[*] Clearing similarity data for collection: {collection}, md5: {md5}{algo_str}..."
            )
            try:
                service.clear_filtered(collection, "md5", md5, algo=algo)
                print(f"[+] Similarity data for md5 '{md5}' cleared.")
            except Exception as e:
                print(f"[!] Error clearing md5 similarity: {e}")
        return

    # 3. Global Clear
    if algo:
        print(f"[*] Clearing {algo} similarity data for: {collection}...")
        patterns = [
            f"{collection}:all_sim:{algo}",
            f"{collection}:sim_meta:{algo}:*",
            f"idx:{collection}:baked:functions:{algo}",
        ]
    else:
        print(f"[*] Clearing ALL similarity data for: {collection}...")
        patterns = [
            f"{collection}:all_sim:*",
            f"{collection}:sim_meta:*",
            f"{collection}:function:*:sim:*",
            f"idx:{collection}:baked:functions:*",
            f"idx:{collection}:sim:*",
            f"idx:{collection}:all_similarities",
        ]
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break
    print("[+] ALL similarity data cleared.")


def sim_quick_status(collection, r=None):
    r = r or get_redis()
    total_set = f"idx:{collection}:indexed:functions"
    total_count = r.scard(total_set)

    algos = ["jaccard", "unweighted_cosine"]
    print(f"[*] Similarity Bake Status for: {collection}")
    for algo in algos:
        baked_set = f"idx:{collection}:baked:functions:{algo}"
        baked_count = r.scard(baked_set)
        unbaked = max(0, total_count - baked_count)

        if unbaked == 0 and total_count > 0:
            print(f"  {algo:<18}: OK : 0 unbaked / {total_count} total")
        else:
            print(f"  {algo:<18}: {unbaked} unbaked / {total_count} total")


def list_sim_batches(collection, batch_filter=None, r=None):
    r = r or get_redis()
    print(f"\n[*] Similarity Bake Status for Collection: {collection}")
    if batch_filter:
        print(f"[*] Filtering for Batch: {batch_filter}")

    print(
        f"{'Batch UUID':<40} | {'Name':<20} | {'Src Funcs':<10} | {'Jac. Count':<10} | {'Jac %':<7} | {'Cos. Count':<10} | {'Cos %':<7}"
    )
    print("-" * 125)

    batch_uuids = r.smembers("global:batches")
    algos = ["jaccard", "unweighted_cosine"]

    found_any = False
    for uuid in sorted(list(batch_uuids)):
        if batch_filter and batch_filter != uuid:
            continue

        batch_func_set = f"{collection}:batch:{uuid}:functions"
        meta_key = f"{collection}:batch:{uuid}"

        if not r.exists(batch_func_set):
            continue
        found_any = True

        name_raw = r.json().get(meta_key, "$.name")
        name = (
            name_raw[0]
            if (isinstance(name_raw, list) and name_raw)
            else (name_raw or "Unknown")
        )
        total = r.scard(batch_func_set)

        baked_counts = {}
        for algo in algos:
            baked_set = f"idx:{collection}:baked:functions:{algo}"
            try:
                baked_counts[algo] = r.execute_command(
                    "SINTERCARD", "2", batch_func_set, baked_set
                )
            except:
                baked_counts[algo] = len(r.sinter(batch_func_set, baked_set))

        j_ratio = (baked_counts["jaccard"] / total * 100) if total > 0 else 0
        c_ratio = (baked_counts["unweighted_cosine"] / total * 100) if total > 0 else 0

        print(
            f"{uuid:<40} | {str(name)[:20]:<20} | {total:<10} | {baked_counts['jaccard']:<10} | {j_ratio:>6.1f}% | {baked_counts['unweighted_cosine']:<10} | {c_ratio:>6.1f}%"
        )

    if not found_any:
        print(f"[!] No batches found for collection {collection}.")


def main():
    parser = argparse.ArgumentParser(
        description="BSim Similarity Baker"
    )
    parser.add_argument("-c", "--collection", required=True, help="Collection name")
    parser.add_argument(
        "--algo",
        default="unweighted_cosine",
        choices=["jaccard", "unweighted_cosine"],
        help="Similarity algorithm",
    )
    parser.add_argument(
        "--md5",
        action="append",
        help="Filter by specific binary MD5 (can be used multiple times)",
    )
    parser.add_argument(
        "--func",
        action="append",
        help="Filter by specific function (ID or MD5:ADDR, can be used multiple times)",
    )
    parser.add_argument(
        "-k", "--top-k", type=int, default=20, help="Top K matches per function"
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0,
        help="Minimum similarity score (default: 0)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Artificial delay (seconds) between calculations",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Internal SCAN batch size"
    )
    parser.add_argument(
        "--ignore-indexing",
        action="store_true",
        help="Skip the full-index check before baking",
    )

    args = parser.parse_args()
    bake_similarities(args)


if __name__ == "__main__":
    main()
