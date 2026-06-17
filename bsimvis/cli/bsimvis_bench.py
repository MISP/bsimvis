import os
import json
import time
import argparse
import requests
import sys
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Defaults ---
APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = os.getenv("APP_PORT", "5000")
API_BASE = f"http://{APP_HOST}:{APP_PORT}/api"
DEFAULT_TEST_DIR = "data/bench"
DEFAULT_COLLECTION = "test_bench"


def poll_job(job_id, timeout=300):
    """Wait for a job or pipeline to finish."""
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
            print(f"\n[!] Error polling job {job_id}: {e}")
            return None

    print(f"\n[!] Timeout waiting for job {job_id}")
    return None


def clear_collection(collection):
    """Wipe all data for a specific collection in Redis/Kvrocks."""
    # We use direct redis for the wipe as it's the fastest way for a test bench
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()

    # Patterns to wipe
    patterns = [
        f"{collection}:*",
        f"global:batches",  # We filter this one in memory to only remove test batches if needed,
        # but for a test bench, clearing it or leaving it is fine.
    ]

    print(f"[*] Wiping collection '{collection}'...")
    count = 0
    for pat in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=pat, count=1000)
            if keys:
                r.delete(*keys)
                count += len(keys)
            if cursor == 0:
                break
    print(f"[+] Removed {count} keys for collection '{collection}'.")


def get_file_stats(path):
    """Returns (size_mb, line_count)."""
    size_mb = os.path.getsize(path) / (1024 * 1024)
    with open(path, "rb") as f:
        lines = sum(1 for _ in f)
    return size_mb, lines


def get_collection_stats(collection):
    """Fetch summary stats from the API."""
    try:
        resp = requests.get(
            f"{API_BASE}/index/status", params={"collection": collection}
        )
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return {}


def run_bench(
    data_dir,
    collection,
    clear_first=False,
    limit=None,
    top_k=None,
    min_score=None,
    min_features=None,
):
    """Run benchmark by uploading all JSON files in a directory."""
    if not os.path.exists(data_dir):
        print(f"Error: Directory {data_dir} not found.")
        return

    json_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".json")])
    if limit:
        json_files = json_files[:limit]

    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return

    # 1. Optional Clear
    if clear_first:
        clear_collection(collection)

    print(f"[*] Starting Benchmark on collection: {collection}")
    print(f"[*] Found {len(json_files)} binaries to process.")

    results = []

    # 2. Sequential Upload
    for filename in json_files:
        path = os.path.join(data_dir, filename)
        size_mb, lines = get_file_stats(path)

        try:
            with open(path, "r") as f:
                data = json.load(f)

            # Rewrite collection and add parameters
            data["collection"] = collection
            if top_k:
                data["top_k"] = top_k
            if min_score:
                data["min_score"] = min_score
            if min_features:
                data["min_features"] = min_features
            num_funcs_in_file = len(data.get("functions", []))

            print(f"\n[*] Processing {filename}")
            print(
                f"    - Size: {size_mb:.2f} MB | Lines: {lines:,} | Functions: {num_funcs_in_file}"
            )

            # Post to API
            resp = requests.post(f"{API_BASE}/file/upload_file_data", json=data)
            resp.raise_for_status()
            res = resp.json()
            pipeline_id = res.get("pipeline_id")

            if not pipeline_id:
                print(f"[!] No pipeline ID returned for {filename}")
                continue

            # Wait for completion with a small progress bar
            print(f"[*] Waiting for pipeline {pipeline_id} to complete...")
            finished_job = poll_job(pipeline_id)

            if finished_job:
                status = finished_job.get("status")

                # Fetch collection stats AFTER completion
                stats = get_collection_stats(collection)

                results.append(
                    {
                        "filename": filename,
                        "pipeline_id": pipeline_id,
                        "status": status,
                        "size_mb": size_mb,
                        "lines": lines,
                        "funcs": stats.get("num_functions", 0),
                        "indexed": stats.get("num_indexed", 0),
                        "features": stats.get("num_features", 0),
                        "sims": stats.get("num_sim_meta", 0),
                    }
                )

                if status == "completed":
                    print(f"[+ ]{filename} finished successfully.")
                    # Automatically show performance report
                    os.system(f"uv run bsimvis job perf {pipeline_id}")

                    print(f"\n[i] Collection State after {filename}:")
                    print(
                        f"    - Total Functions: {stats.get('num_functions')} ({stats.get('num_indexed')} indexed)"
                    )
                    print(f"    - Total Features : {stats.get('num_features')}")
                    print(f"    - Total Similarities: {stats.get('num_sim_meta', 0)}")
                else:
                    print(f"[!] {filename} failed with status: {status}")
                print("-" * 60)

        except Exception as e:
            print(f"[!] Failed to process {filename}: {e}")

    print("\n=== BENCHMARK SUMMARY ===")
    header = f"{'Filename':<20} | {'Pipeline ID':<25} | {'Size':>7} | {'Funcs':>6} | {'Features':>8} | {'Sims':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['filename']:<20} | {r['pipeline_id']:<25} | {r['size_mb']:>6.1f}M | {r['funcs']:>6} | {r['features']:>8} | {r['sims']:>8}"
        )


def get_pipeline_perf_metrics(pipeline_id):
    """Retrieve detailed execution time metrics for a pipeline."""
    try:
        resp = requests.get(f"{API_BASE}/jobs/{pipeline_id}")
        if resp.status_code != 200:
            return None
        job = resp.json()
        metrics = {
            "grand_total": 0.0,
            "sub_tasks": {}
        }
        
        if "sub_tasks" in job and job["sub_tasks"]:
            for st in job["sub_tasks"]:
                t = float(st.get("perf_total", 0))
                metrics["sub_tasks"][st["type"]] = {
                    "total": t,
                    "python": float(st.get("perf_python", 0)),
                    "db": float(st.get("perf_db", 0)),
                    "lua": float(st.get("perf_lua", 0)),
                }
                metrics["grand_total"] += t
        return metrics
    except Exception as e:
        print(f"[!] Failed to fetch performance metrics: {e}")
        return None


def print_comparison(baseline, current):
    """Print a comparison table between baseline and current metrics."""
    print("\n=== PERFORMANCE COMPARISON ===")
    header = f"{'Sub-Task Type':<20} | {'Baseline (s)':>12} | {'Current (s)':>12} | {'Diff (s)':>10} | {'Change':>8}"
    print(header)
    print("-" * len(header))
    
    all_keys = sorted(list(set(baseline["sub_tasks"].keys()) | set(current["sub_tasks"].keys())))
    for k in all_keys:
        b_val = baseline["sub_tasks"].get(k, {}).get("total", 0.0)
        c_val = current["sub_tasks"].get(k, {}).get("total", 0.0)
        diff = c_val - b_val
        change_pct = (diff / b_val * 100) if b_val > 0 else 0.0
        change_str = f"{change_pct:+.1f}%" if b_val > 0 else "N/A"
        print(f"{k:<20} | {b_val:>12.4f} | {c_val:>12.4f} | {diff:>+10.4f} | {change_str:>8}")
        
    print("-" * len(header))
    b_tot = baseline.get("grand_total", 0.0)
    c_tot = current.get("grand_total", 0.0)
    diff_tot = c_tot - b_tot
    tot_change_pct = (diff_tot / b_tot * 100) if b_tot > 0 else 0.0
    tot_change_str = f"{tot_change_pct:+.1f}%" if b_tot > 0 else "N/A"
    print(f"{'GRAND TOTAL':<20} | {b_tot:>12.4f} | {c_tot:>12.4f} | {diff_tot:>+10.4f} | {tot_change_str:>8}\n")


def run_bench(
    data_dir,
    collection,
    clear_first=False,
    limit=None,
    top_k=None,
    min_score=None,
    min_features=None,
    save_path=None,
    compare_path=None,
):
    """Run benchmark by uploading all JSON files in a directory."""
    if not os.path.exists(data_dir):
        print(f"Error: Directory {data_dir} not found.")
        return

    json_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".json")])
    if limit:
        json_files = json_files[:limit]

    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return

    # 1. Optional Clear
    if clear_first:
        clear_collection(collection)

    print(f"[*] Starting Benchmark on collection: {collection}")
    print(f"[*] Found {len(json_files)} binaries to process.")

    results = []
    perf_metrics = None

    # 2. Sequential Upload
    for filename in json_files:
        path = os.path.join(data_dir, filename)
        size_mb, lines = get_file_stats(path)

        try:
            with open(path, "r") as f:
                data = json.load(f)

            # Rewrite collection and add parameters
            data["collection"] = collection
            if top_k:
                data["top_k"] = top_k
            if min_score:
                data["min_score"] = min_score
            if min_features:
                data["min_features"] = min_features
            num_funcs_in_file = len(data.get("functions", []))

            print(f"\n[*] Processing {filename}")
            print(
                f"    - Size: {size_mb:.2f} MB | Lines: {lines:,} | Functions: {num_funcs_in_file}"
            )

            # Post to API
            resp = requests.post(f"{API_BASE}/file/upload_file_data", json=data)
            resp.raise_for_status()
            res = resp.json()
            pipeline_id = res.get("pipeline_id")

            if not pipeline_id:
                print(f"[!] No pipeline ID returned for {filename}")
                continue

            # Wait for completion with a small progress bar
            print(f"[*] Waiting for pipeline {pipeline_id} to complete...")
            finished_job = poll_job(pipeline_id)

            if finished_job:
                status = finished_job.get("status")

                # Fetch collection stats AFTER completion
                stats = get_collection_stats(collection)
                
                # Retrieve structured performance metrics
                perf_metrics = get_pipeline_perf_metrics(pipeline_id)

                results.append(
                    {
                        "filename": filename,
                        "pipeline_id": pipeline_id,
                        "status": status,
                        "size_mb": size_mb,
                        "lines": lines,
                        "funcs": stats.get("num_functions", 0),
                        "indexed": stats.get("num_indexed", 0),
                        "features": stats.get("num_features", 0),
                        "sims": stats.get("num_sim_meta", 0),
                    }
                )

                if status == "completed":
                    print(f"[+ ]{filename} finished successfully.")
                    # Automatically show performance report
                    os.system(f"uv run bsimvis job perf {pipeline_id}")

                    print(f"\n[i] Collection State after {filename}:")
                    print(
                        f"    - Total Functions: {stats.get('num_functions')} ({stats.get('num_indexed')} indexed)"
                    )
                    print(f"    - Total Features : {stats.get('num_features')}")
                    print(f"    - Total Similarities: {stats.get('num_sim_meta', 0)}")
                else:
                    print(f"[!] {filename} failed with status: {status}")
                print("-" * 60)

        except Exception as e:
            print(f"[!] Failed to process {filename}: {e}")

    print("\n=== BENCHMARK SUMMARY ===")
    header = f"{'Filename':<20} | {'Pipeline ID':<25} | {'Size':>7} | {'Funcs':>6} | {'Features':>8} | {'Sims':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['filename']:<20} | {r['pipeline_id']:<25} | {r['size_mb']:>6.1f}M | {r['funcs']:>6} | {r['features']:>8} | {r['sims']:>8}"
        )

    # Handle Save
    if save_path and perf_metrics:
        try:
            with open(save_path, "w") as f:
                json.dump(perf_metrics, f, indent=2)
            print(f"\n[+] Saved performance metrics to {save_path}")
        except Exception as e:
            print(f"[!] Failed to save metrics: {e}")

    # Handle Compare
    if compare_path and perf_metrics:
        try:
            if os.path.exists(compare_path):
                with open(compare_path, "r") as f:
                    baseline = json.load(f)
                print_comparison(baseline, perf_metrics)
            else:
                print(f"[!] Baseline file not found: {compare_path}")
        except Exception as e:
            print(f"[!] Error comparing baseline: {e}")


def main():
    parser = argparse.ArgumentParser(description="BSimVis Pipeline Benchmarker")
    parser.add_argument(
        "--dir",
        default=DEFAULT_TEST_DIR,
        help=f"Directory containing test JSONs (default: {DEFAULT_TEST_DIR})",
    )
    parser.add_argument(
        "-c",
        "--collection",
        default=DEFAULT_COLLECTION,
        help=f"Collection to use (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--clear", action="store_true", help="Clear the collection before starting"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of binaries to process", default=None
    )
    parser.add_argument("--top-k", type=int, help="Top K candidates to keep")
    parser.add_argument(
        "--min-score", type=float, help="Minimum similarity score threshold"
    )
    parser.add_argument(
        "--min-features", type=int, help="Minimum feature count required"
    )
    parser.add_argument(
        "--save", type=str, help="Path to save performance metrics JSON"
    )
    parser.add_argument(
        "--compare", type=str, help="Path to baseline performance metrics JSON for comparison"
    )

    args = parser.parse_args()

    run_bench(
        args.dir,
        args.collection,
        args.clear,
        args.limit,
        args.top_k,
        args.min_score,
        args.min_features,
        args.save,
        args.compare,
    )


if __name__ == "__main__":
    main()
