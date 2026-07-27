import os
import json
import time
import uuid
import argparse
import requests
import sys
import concurrent.futures
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


def get_pipeline_perf_metrics(pipeline_id, collection=None, pool_id=None):
    """Retrieve detailed execution time metrics and stats for a pipeline."""
    try:
        resp = requests.get(f"{API_BASE}/jobs/{pipeline_id}")
        if resp.status_code != 200:
            return None
        job = resp.json()
        metrics = {"grand_total": 0.0, "sub_tasks": {}, "stats": {}}

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

        # Fetch counts (similarities and clusters)
        if pool_id:
            try:
                p_resp = requests.get(f"{API_BASE}/pool/{pool_id}")
                if p_resp.status_code == 200:
                    p_data = p_resp.json()
                    metrics["stats"] = {
                        "func_similarities": p_data.get("total_func_similarities", 0),
                        "func_clusters": p_data.get("total_func_clusters", 0),
                        "file_similarities": p_data.get("total_file_similarities", 0),
                        "file_clusters": p_data.get("total_file_clusters", 0),
                    }
            except Exception as e:
                print(f"[!] Failed to fetch pool stats: {e}")
        elif collection:
            stats = get_collection_stats(collection)
            if stats:
                metrics["stats"] = {
                    "func_similarities": stats.get("num_sim_meta", 0),
                    "func_clusters": stats.get("num_clusters", 0),
                }

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

    all_keys = sorted(
        list(
            set(baseline.get("sub_tasks", {}).keys())
            | set(current.get("sub_tasks", {}).keys())
        )
    )
    for k in all_keys:
        b_val = baseline.get("sub_tasks", {}).get(k, {}).get("total", 0.0)
        c_val = current.get("sub_tasks", {}).get(k, {}).get("total", 0.0)
        diff = c_val - b_val
        change_pct = (diff / b_val * 100) if b_val > 0 else 0.0
        change_str = f"{change_pct:+.1f}%" if b_val > 0 else "N/A"
        print(
            f"{k:<20} | {b_val:>12.4f} | {c_val:>12.4f} | {diff:>+10.4f} | {change_str:>8}"
        )

    print("-" * len(header))
    b_tot = baseline.get("grand_total", 0.0)
    c_tot = current.get("grand_total", 0.0)
    diff_tot = c_tot - b_tot
    tot_change_pct = (diff_tot / b_tot * 100) if b_tot > 0 else 0.0
    tot_change_str = f"{tot_change_pct:+.1f}%" if b_tot > 0 else "N/A"
    print(
        f"{'GRAND TOTAL':<20} | {b_tot:>12.4f} | {c_tot:>12.4f} | {diff_tot:>+10.4f} | {tot_change_str:>8}\n"
    )

    # Compare generated counts/stats if available
    b_stats = baseline.get("stats", {})
    c_stats = current.get("stats", {})
    if b_stats or c_stats:
        print("=== GENERATED DATA COMPARISON ===")
        stat_header = f"{'Metric Name':<20} | {'Baseline':>12} | {'Current':>12} | {'Diff':>10} | {'Change':>8}"
        print(stat_header)
        print("-" * len(stat_header))
        all_stat_keys = sorted(list(set(b_stats.keys()) | set(c_stats.keys())))
        for k in all_stat_keys:
            b_val = b_stats.get(k, 0)
            c_val = c_stats.get(k, 0)
            diff = c_val - b_val
            change_pct = (diff / b_val * 100) if b_val > 0 else 0.0
            change_str = f"{change_pct:+.1f}%" if b_val > 0 else "N/A"
            print(
                f"{k:<20} | {b_val:>12} | {c_val:>12} | {diff:>+10} | {change_str:>8}"
            )
        print("-" * len(stat_header) + "\n")


def run_single_file(
    data_dir,
    filename,
    collection,
    top_k,
    min_score,
    min_features,
    algo=None,
    skip_write=False,
):
    """Process a single file and return results. Thread-safe."""
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
        if algo:
            data["algo"] = algo
        if skip_write:
            data["skip_write"] = True
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
            return None

        # Wait for completion with a small progress bar
        print(f"[*] Waiting for pipeline {pipeline_id} to complete...")
        finished_job = poll_job(pipeline_id)

        if finished_job:
            status = finished_job.get("status")

            # Fetch collection stats AFTER completion
            stats = get_collection_stats(collection)

            # Retrieve structured performance metrics
            perf_metrics = get_pipeline_perf_metrics(pipeline_id, collection=collection)

            result = {
                "filename": filename,
                "pipeline_id": pipeline_id,
                "status": status,
                "size_mb": size_mb,
                "lines": lines,
                "funcs": stats.get("num_functions", 0),
                "indexed": stats.get("num_indexed", 0),
                "features": stats.get("num_features", 0),
                "sims": stats.get("num_sim_meta", 0),
                "perf_metrics": perf_metrics,
            }

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

            return result
    except Exception as e:
        print(f"[!] Failed to process {filename}: {e}")

    return None


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
    sequential=False,
    algo=None,
    skip_write=False,
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

    # 2. Upload using ThreadPoolExecutor or sequentially
    start_time = time.time()
    results = []
    all_perf_metrics = []

    if sequential:
        print(f"\n[*] Uploading {len(json_files)} files sequentially...")
        for f in json_files:
            result = run_single_file(
                data_dir,
                f,
                collection,
                top_k,
                min_score,
                min_features,
                algo=algo,
                skip_write=skip_write,
            )
            if result:
                results.append(result)
                all_perf_metrics.append(result["perf_metrics"])
    else:
        print(f"\n[*] Uploading {len(json_files)} files concurrently...")
        max_workers = len(json_files)  # One thread per file
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(
                    run_single_file,
                    data_dir,
                    f,
                    collection,
                    top_k,
                    min_score,
                    min_features,
                    algo,
                    skip_write,
                ): f
                for f in json_files
            }
            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                if result:
                    results.append(result)
                    all_perf_metrics.append(result["perf_metrics"])

    total_elapsed = time.time() - start_time

    # Use the last non-None perf metrics (they may differ per file in parallel mode)
    all_perfs = [m for m in all_perf_metrics if m is not None]
    perf_metrics = all_perfs[-1] if all_perfs else None

    print("\n=== BENCHMARK SUMMARY ===")
    header = f"{'Filename':<20} | {'Pipeline ID':<25} | {'Size':>7} | {'Funcs':>6} | {'Features':>8} | {'Sims':>8}"
    print(header)
    print("-" * len(header))
    for r in sorted(results, key=lambda x: x["filename"]):
        print(
            f"{r['filename']:<20} | {r['pipeline_id']:<25} | {r['size_mb']:>6.1f}M | {r['funcs']:>6} | {r['features']:>8} | {r['sims']:>8}"
        )
    print(f"\n[+] Total elapsed time: {total_elapsed:.2f}s")

    # Handle Save
    if save_path and perf_metrics:
        try:
            with open(save_path, "w") as f:
                json.dump(perf_metrics, f, indent=2)
            print(f"[+] Saved performance metrics to {save_path}")
        except Exception as e:
            print(f"[!] Failed to save metrics: {e}")

    # Handle Compare
    if compare_path and all_perfs:
        # For parallel runs, average the perf metrics across all completed pipelines
        avg_metrics = {
            "grand_total": sum(m["grand_total"] for m in all_perfs) / len(all_perfs),
            "sub_tasks": {},
            "stats": {},
        }
        for m in all_perfs:
            for k, v in m["sub_tasks"].items():
                if k not in avg_metrics["sub_tasks"]:
                    avg_metrics["sub_tasks"][k] = {
                        "total": 0,
                        "python": 0,
                        "db": 0,
                        "lua": 0,
                    }
                avg_metrics["sub_tasks"][k]["total"] += v["total"]
                avg_metrics["sub_tasks"][k]["python"] += v["python"]
                avg_metrics["sub_tasks"][k]["db"] += v["db"]
                avg_metrics["sub_tasks"][k]["lua"] += v["lua"]
            # Take the max for stats counts (similarities and clusters) across pipelines instead of summing
            for sk, sv in m.get("stats", {}).items():
                avg_metrics["stats"][sk] = max(avg_metrics["stats"].get(sk, 0), sv)

        for k in avg_metrics["sub_tasks"]:
            n = len(all_perfs)
            for vv in ("total", "python", "db", "lua"):
                avg_metrics["sub_tasks"][k][vv] /= n

        try:
            if os.path.exists(compare_path):
                with open(compare_path, "r") as f:
                    baseline = json.load(f)
                print_comparison(baseline, avg_metrics)
            else:
                print(f"[!] Baseline file not found: {compare_path}")
        except Exception as e:
            print(f"[!] Error comparing baseline: {e}")


def run_single_file_for_pool(
    data_dir, filename, collection, top_k, min_score, min_features
):
    """Process a single file for pool benchmark by uploading it with skip_sim=True."""
    path = os.path.join(data_dir, filename)
    size_mb, lines = get_file_stats(path)

    try:
        # Files are already-analyzed Ghidra JSON, so ingest via upload_file_data
        # (pre-analyzed path). Posting raw bytes to /file/upload triggers a Ghidra
        # analyze job that fails with "No load spec found".
        with open(path, "r") as fh:
            data = json.load(fh)

        data["collection"] = collection
        data["skip_sim"] = True  # pool handles similarity
        if top_k is not None:
            data["top_k"] = top_k
        if min_score is not None:
            data["min_score"] = min_score
        if min_features is not None:
            data["min_features"] = min_features

        print(f"\n[*] Processing {filename} -> Collection: {collection}")
        print(f"    - Size: {size_mb:.2f} MB | Lines: {lines:,}")

        resp = requests.post(
            f"{API_BASE}/file/upload_file_data",
            json=data,
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        pipeline_id = body.get("pipeline_id")

        if not pipeline_id:
            print(f"[!] No pipeline ID returned for {filename}")
            return None

        return {
            "filename": filename,
            "collection": collection,
            "pipeline_id": pipeline_id,
        }
    except Exception as e:
        print(f"[!] Failed to process {filename} into {collection}: {e}")

    return None


def run_bench_pools(
    data_dir,
    collection,
    clear_first=False,
    limit=None,
    top_k=None,
    min_score=None,
    min_features=None,
    save_path=None,
    compare_path=None,
    sequential=False,
    algo=None,
    skip_write=False,
):
    """Run benchmark by creating a pool across separate collections."""
    if not os.path.exists(data_dir):
        print(f"Error: Directory {data_dir} not found.")
        return

    json_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".json")])
    if limit:
        json_files = json_files[:limit]

    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return

    # Map filename -> target collection name
    file_collections = [f"{collection}_{idx + 1}" for idx, _ in enumerate(json_files)]
    pool_id = f"pool_{collection}"

    from bsimvis.app.services.redis_client import get_redis

    r_client = get_redis()

    # Check if all collections already have functions indexed
    collections_exist = False
    if not clear_first:
        collections_exist = True
        for col_name in file_collections:
            try:
                if r_client.scard(f"{col_name}:all_functions") == 0:
                    collections_exist = False
                    break
            except Exception:
                collections_exist = False
                break

    # 1. Unconditionally delete the pool definition so we can recreate it
    try:
        requests.delete(f"{API_BASE}/pool/{pool_id}")
    except Exception:
        pass

    # 2. Optional Clear for collections
    if clear_first:
        for col_name in file_collections:
            clear_collection(col_name)

    print(f"[*] Starting Pool Benchmark: {pool_id}")
    print(f"[*] Collections in pool: {file_collections}")

    # 2. Ingest files concurrently or sequentially (only if they don't exist)
    start_time = time.time()
    results = []

    if collections_exist:
        print(
            "\n[*] Target collections are already populated. Skipping ingestion/indexing phase."
        )
    else:
        if sequential:
            print(f"\n[*] Ingesting {len(json_files)} files sequentially...")
            for f, col_name in zip(json_files, file_collections):
                res = run_single_file_for_pool(
                    data_dir, f, col_name, top_k, min_score, min_features
                )
                if res:
                    results.append(res)
        else:
            print(f"\n[*] Ingesting {len(json_files)} files concurrently...")
            max_workers = len(json_files)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_file = {
                    executor.submit(
                        run_single_file_for_pool,
                        data_dir,
                        f,
                        col_name,
                        top_k,
                        min_score,
                        min_features,
                    ): f
                    for f, col_name in zip(json_files, file_collections)
                }
                for future in concurrent.futures.as_completed(future_to_file):
                    res = future.result()
                    if res:
                        results.append(res)

        if len(results) < len(json_files):
            print(
                "[!] Not all files completed ingestion. Aborting pool similarity benchmark."
            )
            return

        # Wait for all uploaded file ingestion pipelines to finish
        print("\n[*] Waiting for all file ingestion pipelines to complete...")
        pids = [res["pipeline_id"] for res in results if res.get("pipeline_id")]
        for pid in pids:
            poll_job(pid)

        # Call batch_finalize to complete the indexing (but skip binary similarity as pool handles it)
        batch_uuid = str(uuid.uuid4())
        print("\n[*] Finalizing batch uploads for collections...")
        for res in results:
            col_name = res["collection"]
            pid = res["pipeline_id"]
            api_url = f"{API_BASE}/file/upload/batch_finalize"
            payload = {
                "pipeline_ids": [pid],
                "batch_uuid": batch_uuid,
                "collection": col_name,
                "algo": algo or "unweighted_cosine",
                "skip_sim": True,
            }
            try:
                resp = requests.post(api_url, json=payload, timeout=300)
                resp.raise_for_status()
                master_pipeline_id = resp.json().get("master_pipeline_id")
                if master_pipeline_id:
                    print(
                        f"[*] Waiting for batch finalize pipeline {master_pipeline_id} to complete..."
                    )
                    poll_job(master_pipeline_id)
            except Exception as e:
                print(f"[!] Batch finalize failed for {col_name}: {e}")

    # 3. Create the pool and trigger building
    print(f"\n[*] Creating Pool '{pool_id}'...")
    func_sim_params = {}
    if algo:
        func_sim_params["algo"] = algo
    if top_k:
        func_sim_params["top_k"] = top_k
    if min_score:
        func_sim_params["min_score"] = min_score
    if min_features:
        func_sim_params["min_features"] = min_features

    pool_payload = {
        "pool_id": pool_id,
        "name": f"Benchmark Pool for {collection}",
        "collections": file_collections,
        "config": {
            "skip_clustering": True,
        },
    }
    if skip_write:
        func_sim_params["skip_write"] = True
        pool_payload["config"]["skip_write"] = True
    if func_sim_params:
        pool_payload["config"]["func_sim_params"] = func_sim_params

    try:
        resp = requests.post(f"{API_BASE}/pool", json=pool_payload)
        resp.raise_for_status()
        pool_res = resp.json()
        job_id = pool_res.get("job_id")

        if not job_id:
            print("[!] No job ID returned from pool creation API.")
            return

        print(f"[*] Waiting for pool build pipeline {job_id} to complete...")
        finished_job = poll_job(job_id)

        if finished_job:
            status = finished_job.get("status")
            total_elapsed = time.time() - start_time

            # Fetch performance metrics for this pipeline
            perf_metrics = get_pipeline_perf_metrics(job_id, pool_id=pool_id)

            print("\n=== POOL BENCHMARK SUMMARY ===")
            print(f"Pool ID:     {pool_id}")
            print(f"Status:      {status}")
            print(f"Total time:  {total_elapsed:.2f}s")

            if perf_metrics:
                # We can print the subtask details
                print("\n=== SUBTASK DETAILS ===")
                for task_type, vals in perf_metrics.get("sub_tasks", {}).items():
                    print(
                        f"  - {task_type:<20}: {vals['total']:.4f}s (Lua: {vals['lua']:.4f}s, DB: {vals['db']:.4f}s, Python: {vals['python']:.4f}s)"
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

    except Exception as e:
        print(f"[!] Failed to run pool benchmark: {e}")


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
        "--compare",
        type=str,
        help="Path to baseline performance metrics JSON for comparison",
    )
    parser.add_argument(
        "--algo",
        type=str,
        help="Similarity algorithm to use (e.g. unweighted_cosine, minhash_lsh)",
        default=None,
    )
    parser.add_argument(
        "--bench-pools",
        action="store_true",
        help="Benchmark pool-level similarities instead of standard pipeline",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Run file uploads sequentially rather than concurrently",
    )
    parser.add_argument(
        "--skip-write",
        action="store_true",
        help="Run benchmark in discovery-only mode without writing similarities to disk/DB",
    )

    args = parser.parse_args()

    if args.bench_pools:
        run_bench_pools(
            args.dir,
            args.collection,
            args.clear,
            args.limit,
            args.top_k,
            args.min_score,
            args.min_features,
            args.save,
            args.compare,
            args.sequential,
            args.algo,
            skip_write=args.skip_write,
        )
    else:
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
            args.sequential,
            args.algo,
            skip_write=args.skip_write,
        )


if __name__ == "__main__":
    main()
