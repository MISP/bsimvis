import os
import sys
import time
import requests
import argparse
import logging
from dotenv import load_dotenv
from bsimvis.app.services.redis_client import get_redis
from populate_lsh import compute_lsh_buckets

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Resolve API base URL
APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = os.getenv("APP_PORT", "5000")
API_BASE = f"http://{APP_HOST}:{APP_PORT}/api"


def poll_job(job_id, timeout=36000):
    """Wait for a pool build pipeline job to complete."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{API_BASE}/jobs/{job_id}")
            resp.raise_for_status()
            job = resp.json()
            status = job.get("status")
            if status in ["completed", "failed", "cancelled"]:
                return job
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error polling job {job_id}: {e}")
            return None
    logging.error(f"Timeout waiting for job {job_id}")
    return None


def delete_pool(pool_id):
    """Clean up pool definitions and similarities."""
    try:
        requests.delete(f"{API_BASE}/pool/{pool_id}")
    except Exception:
        pass


def run_pool_build(pool_id, collections, algo, min_score, min_features, timeout=36000):
    """Trigger pool creation and record execution duration and similarities count."""
    delete_pool(pool_id)

    payload = {
        "pool_id": pool_id,
        "name": f"Benchmark Pool: {algo}",
        "collections": collections,
        "config": {
            "skip_clustering": True,
            "func_sim_params": {
                "algo": algo,
                "min_score": min_score,
                "min_features": min_features,
            },
        },
    }

    start_time = time.time()
    resp = requests.post(f"{API_BASE}/pool", json=payload)
    resp.raise_for_status()
    job_id = resp.json().get("job_id")

    if not job_id:
        logging.error(f"[-] No job ID returned for pool build with {algo}")
        return None

    logging.info(
        f"[*] Started pool job {job_id} for {algo}. Waiting for completion (timeout {timeout}s)..."
    )
    finished_job = poll_job(job_id, timeout=timeout)
    duration = time.time() - start_time

    if not finished_job or finished_job.get("status") != "completed":
        logging.error(f"[-] Pool build failed or timed out for algorithm: {algo}")
        return None

    # Get pool metadata for similarity counts
    pool_resp = requests.get(f"{API_BASE}/pool/{pool_id}")
    pool_resp.raise_for_status()
    pool_data = pool_resp.json()

    return {
        "duration": duration,
        "similarities": pool_data.get("total_func_similarities", 0),
        "pipeline_id": job_id,
    }


def rebuild_lsh_hashes(collections, num_bands, rows_per_band):
    """Regenerates LSH buckets for all member collections in Kvrocks."""
    r = get_redis()
    start_time = time.time()

    for collection in collections:
        indexed_set_key = f"{collection}:indexed:functions"
        total_funcs = r.scard(indexed_set_key)
        if total_funcs == 0:
            logging.warning(
                f"No functions to index in collection '{collection}'. Skipping."
            )
            continue

        logging.info(
            f"[*] Re-populating LSH hashes for '{collection}' ({total_funcs} functions, b={num_bands}, r={rows_per_band})..."
        )

        # Clear existing LSH hashes for this collection
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{collection}:lsh:*", count=1000)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break

        function_ids = list(r.smembers(indexed_set_key))
        batch_size = 100
        for i in range(0, len(function_ids), batch_size):
            chunk = function_ids[i : i + batch_size]

            # Pipe delete keys & fetch tf vectors
            pipe = r.pipeline()
            for fid in chunk:
                for band in range(num_bands):
                    pipe.delete(f"{fid}:lsh:bucket_key:{band}")
                pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            pipe_results = pipe.execute()

            # Save new LSH buckets
            save_pipe = r.pipeline()
            for idx, fid in enumerate(chunk):
                features = pipe_results[idx * (num_bands + 1) + num_bands]
                if not features:
                    continue
                buckets = compute_lsh_buckets(features, num_bands, rows_per_band)
                for band, b_hash in buckets:
                    bucket_key = f"{collection}:lsh:bucket:{band}:{b_hash}"
                    save_pipe.sadd(bucket_key, fid)
                    save_pipe.set(f"{fid}:lsh:bucket_key:{band}", bucket_key)
            save_pipe.execute()

    return time.time() - start_time


def main():
    parser = argparse.ArgumentParser(
        description="Full Pool-level LSH benchmarking script."
    )
    parser.add_argument(
        "-c",
        "--collections",
        required=True,
        nargs="+",
        help="Space-separated member collections",
    )
    parser.add_argument(
        "--pool-id",
        default="bench_temp_pool",
        help="Temporary pool ID (default: bench_temp_pool)",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.70,
        help="Similarity threshold (default: 0.70)",
    )
    parser.add_argument(
        "--min-features", type=int, default=10, help="Min feature count (default: 10)"
    )
    parser.add_argument(
        "-b", "--bands", type=int, default=40, help="Number of LSH bands (default: 40)"
    )
    parser.add_argument(
        "-r", "--rows", type=int, default=8, help="Rows per band (default: 8)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=36000,
        help="Job completion timeout in seconds (default: 36000)",
    )

    args = parser.parse_args()

    # 1. Regenerate LSH Hashes
    logging.info("[*] Phase 1: Rebuilding LSH hashes for member collections...")
    lsh_generation_time = rebuild_lsh_hashes(args.collections, args.bands, args.rows)
    logging.info(f"[+] Rebuilt LSH hashes in {lsh_generation_time:.2f} seconds.")

    # 2. Run Baseline (unweighted_cosine)
    logging.info("[*] Phase 2: Running baseline pool build (unweighted_cosine)...")
    baseline = run_pool_build(
        args.pool_id,
        args.collections,
        "unweighted_cosine",
        args.min_score,
        args.min_features,
        timeout=args.timeout,
    )

    # 3. Run LSH Pool Build
    logging.info("[*] Phase 3: Running LSH pool build (minhash_lsh)...")
    current = run_pool_build(
        args.pool_id,
        args.collections,
        "minhash_lsh",
        args.min_score,
        args.min_features,
        timeout=args.timeout,
    )

    # 4. Clean up pool similarities
    delete_pool(args.pool_id)

    # 5. Output comparison results
    if not baseline or not current:
        logging.error(
            "Benchmarking failed. Baseline or LSH run did not complete successfully."
        )
        sys.exit(1)

    print("\n" + "=" * 80)
    print(" " * 22 + "POOL BENCHMARK PERFORMANCE RESULTS")
    print("=" * 80)

    # Format comparison table
    headers = f"{'Metric Name':<30} | {'Baseline (Cosine)':>18} | {'Current (LSH)':>15} | {'Diff / Change':>10}"
    print(headers)
    print("-" * len(headers))

    # Compare Build Time
    b_time, c_time = baseline["duration"], current["duration"]
    time_diff = c_time - b_time
    time_change = (time_diff / b_time * 100) if b_time > 0 else 0.0
    print(
        f"{'Pool Similarity Build Time':<30} | {b_time:>16.4f}s | {c_time:>13.4f}s | {time_diff:>+8.4f}s ({time_change:+.1f}%)"
    )

    # Compare Similarity Counts (Recall check)
    b_sims, c_sims = baseline["similarities"], current["similarities"]
    sims_diff = c_sims - b_sims
    sims_change = (sims_diff / b_sims * 100) if b_sims > 0 else 0.0
    print(
        f"{'Total Similarity Count':<30} | {b_sims:>17}  | {c_sims:>14}  | {sims_diff:>+9}  ({sims_change:+.1f}%)"
    )

    # Add LSH Generation overhead info
    print("-" * len(headers))
    print(
        f"LSH Hash Generation Overhead   : {lsh_generation_time:.2f}s (run once to index)"
    )
    print(f"Baseline Pipeline ID           : {baseline['pipeline_id']}")
    print(f"LSH Pipeline ID                : {current['pipeline_id']}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
