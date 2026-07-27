import time
import argparse
import redis
import math
import logging
import sys
from bsimvis.app.services.similarity_service import SimilarityService

# Configure logging to show info logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def copy_collection_subset(r, source_coll, target_coll, limit=300):
    print(
        f"[*] Copying subset of {limit} functions from '{source_coll}' to '{target_coll}'..."
    )

    # 1. Fetch functions
    source_funcs = [
        f.decode() if isinstance(f, bytes) else f
        for f in r.srandmember(f"{source_coll}:indexed:functions", limit)
    ]
    if not source_funcs:
        print(f"[-] No functions found in source collection '{source_coll}'")
        sys.exit(1)

    pipe = r.pipeline()
    copied_count = 0

    for sfid in source_funcs:
        # sfid is {source_coll}:func:{md5}:{addr}
        parts = sfid.split(":")
        if len(parts) < 4:
            continue
        md5, addr = parts[-2], parts[-1]
        tfid = f"{target_coll}:func:{md5}:{addr}"

        # 1. Copy vec:tf (ZSET)
        tf_key = f"{sfid}:vec:tf"
        tf_data = r.zrange(tf_key, 0, -1, withscores=True)
        if tf_data:
            pipe.zadd(f"{tfid}:vec:tf", dict(tf_data))

            # 2. Build target inverted index & copy features
            for feat, tf in tf_data:
                pipe.zadd(f"{target_coll}:feature:{feat}:functions", {tfid: tf})

        # 3. Copy vec:norm (string)
        norm_val = r.get(f"{sfid}:vec:norm")
        if norm_val:
            pipe.set(f"{tfid}:vec:norm", norm_val)

        # 4. Copy meta (JSON)
        meta_val_raw = r.json().get(f"{sfid}:meta", "$")
        if meta_val_raw:
            meta_val = (
                meta_val_raw[0] if isinstance(meta_val_raw, list) else meta_val_raw
            )
            if meta_val:
                pipe.json().set(f"{tfid}:meta", "$", meta_val)

        # 5. Copy file meta if exists
        file_meta_key = f"{source_coll}:file:{md5}:meta"
        target_file_meta_key = f"{target_coll}:file:{md5}:meta"
        file_meta_raw = r.json().get(file_meta_key, "$")
        if file_meta_raw:
            file_meta = (
                file_meta_raw[0] if isinstance(file_meta_raw, list) else file_meta_raw
            )
            if file_meta:
                pipe.json().set(target_file_meta_key, "$", file_meta)

        # 6. Add to indexed set
        pipe.sadd(f"{target_coll}:indexed:functions", tfid)

        # 7. Add feature count to count index
        feat_count = r.zscore(f"{source_coll}:idx:func:bsim_features_count", sfid)
        if feat_count is not None:
            pipe.zadd(f"{target_coll}:idx:func:bsim_features_count", {tfid: feat_count})

        copied_count += 1
        if copied_count % 100 == 0:
            pipe.execute()
            pipe = r.pipeline()

    pipe.execute()
    print(f"[+] Successfully copied {copied_count} functions to '{target_coll}'.")


def cleanup_benchmark_collection(r, coll):
    print(f"[*] Cleaning up benchmark collection '{coll}'...")
    keys = r.keys(f"{coll}:*")
    if keys:
        pipe = r.pipeline()
        for k in keys:
            pipe.delete(k)
        pipe.execute()
    # Clean up global registries if any
    reg_keys = r.keys("global:*")
    if reg_keys:
        pipe = r.pipeline()
        for k in reg_keys:
            pipe.delete(k)
        pipe.execute()
    print("[+] Benchmark collection cleaned up.")


def run_benchmark_mode(sim_service, coll, num_funcs, mode_name, index_depth):
    print(
        f"\n[*] Running Benchmark in mode: {mode_name} (index_depth={index_depth})..."
    )
    # Clear previous runs safely on benchmark collection
    sim_service.clear_all(coll, algo="unweighted_cosine")

    start_time = time.time()

    # Run the similarity build
    sim_service.build_batch(coll, algo="unweighted_cosine", index_depth=index_depth)

    build_time = time.time() - start_time
    indexing_time = 0.0

    if mode_name == "Split / Deferred":
        # Run indexing separately
        index_start = time.time()
        sim_service.index_similarities(coll, algo="unweighted_cosine")
        indexing_time = time.time() - index_start

    total_time = build_time + indexing_time
    num_sims = sim_service.r.zcard(f"{coll}:sim:all")
    print(
        f"[+] Completed {mode_name}: Build={build_time:.2f}s, Index={indexing_time:.2f}s, Total={total_time:.2f}s, Saved Sims={num_sims}"
    )
    return build_time, indexing_time, total_time


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Similarity calculation & Indexing speed on real collections."
    )
    parser.add_argument(
        "--collection",
        default="mirai7",
        help="Source collection to subset for benchmarking (default: mirai7).",
    )
    parser.add_argument("--port", type=int, default=6667, help="Kvrocks/Redis port.")
    parser.add_argument(
        "--limit",
        type=int,
        default=150,
        help="Number of functions to subset for the benchmark.",
    )
    args = parser.parse_args()

    r = redis.Redis(port=args.port, decode_responses=True)

    source_coll = args.collection
    bench_coll = "bench_temp_coll"

    # Clean up any leftover benchmark keys
    cleanup_benchmark_collection(r, bench_coll)

    # Copy subset of functions safely
    copy_collection_subset(r, source_coll, bench_coll, limit=args.limit)

    try:
        # Instantiate service
        sim_service = SimilarityService(r=r)

        # Warmup / Test check
        func_set = f"{bench_coll}:indexed:functions"
        funcs_count = r.scard(func_set)
        if funcs_count == 0:
            print("[-] No functions found in benchmark collection.")
            return

        print(f"[*] Commencing comparative benchmark on {funcs_count} functions...")

        results = {}

        # 1. Full (Current Baseline)
        results["Full (Current)"] = run_benchmark_mode(
            sim_service, bench_coll, funcs_count, "Full (Current)", "full"
        )

        # 2. Minimal (MD5 + cross_binary only)
        results["Minimal"] = run_benchmark_mode(
            sim_service, bench_coll, funcs_count, "Minimal", "minimal"
        )

        # 3. None (No indexing)
        results["None (No indexing)"] = run_benchmark_mode(
            sim_service, bench_coll, funcs_count, "None", "none"
        )

        # 4. Split / Deferred (Build None + Index separately)
        results["Split / Deferred"] = run_benchmark_mode(
            sim_service, bench_coll, funcs_count, "Split / Deferred", "none"
        )

        # Output comparison table
        print("\n" + "=" * 70)
        print(
            f"{'BENCHMARK RESULTS SUMMARY (Functions: ' + str(funcs_count) + ')':^70}"
        )
        print("=" * 70)
        print(
            f"{'Mode':<25} | {'Build Time':<12} | {'Index Time':<12} | {'Total Time':<12} | {'Speed (fn/s)':<12}"
        )
        print("-" * 70)
        for mode, (b_time, idx_time, tot_time) in results.items():
            speed = funcs_count / tot_time if tot_time > 0 else 0
            print(
                f"{mode:<25} | {b_time:>10.2f}s | {idx_time:>10.2f}s | {tot_time:>10.2f}s | {speed:>10.1f}"
            )
        print("=" * 70)

    finally:
        cleanup_benchmark_collection(r, bench_coll)


if __name__ == "__main__":
    main()
