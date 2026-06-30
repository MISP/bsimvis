import os
import sys
import json
import time
import argparse
import requests
import logging
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Resolve API base URL
APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = os.getenv("APP_PORT", "5000")
API_BASE = f"http://{APP_HOST}:{APP_PORT}/api"


def poll_job(job_id, timeout=36000):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{API_BASE}/jobs/{job_id}", timeout=10)
            resp.raise_for_status()
            job = resp.json()
            status = job.get("status")
            if status in ["completed", "failed", "cancelled"]:
                return job
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"Error polling job {job_id}: {e}")
            return None
    logging.error(f"Timeout waiting for job {job_id}")
    return None


def delete_pool(pool_id):
    try:
        requests.delete(f"{API_BASE}/pool/{pool_id}", timeout=10)
    except Exception:
        pass


def run_pool_benchmark(collection, json_path, graph_path, algo, min_scores, top_k):
    pool_id = "bench_min_score_pool"
    results = []

    print(f"[*] Starting pool benchmark on collection: {collection} using algo: {algo}")

    for score in min_scores:
        print(f"[*] Testing min-score = {score} (top_k = {top_k}) ...")
        delete_pool(pool_id)

        payload = {
            "pool_id": pool_id,
            "name": f"Benchmark Min-Score Pool: {score}",
            "collections": [collection],
            "config": {
                "skip_clustering": True,
                "func_sim_params": {
                    "algo": algo,
                    "min_score": score,
                    "min_features": 0,
                    "top_k": top_k,
                },
            },
        }

        start_time = time.time()
        try:
            resp = requests.post(f"{API_BASE}/pool", json=payload, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"[-] Failed to trigger pool build: {e}")
            continue

        job_id = resp.json().get("job_id")
        if not job_id:
            print("[-] No job_id returned from pool API")
            continue

        print(f"  [+] Pipeline created: {job_id}. Waiting for completion...")
        finished_job = poll_job(job_id)
        pipeline_total = time.time() - start_time

        if not finished_job or finished_job.get("status") != "completed":
            print(f"  [!] Pool build failed or timed out for min-score {score}")
            continue

        # Get pool metadata for similarity counts
        similarities = 0
        try:
            pool_resp = requests.get(f"{API_BASE}/pool/{pool_id}", timeout=10)
            pool_resp.raise_for_status()
            pool_data = pool_resp.json()
            similarities = pool_data.get("total_func_similarities", 0)
        except Exception as e:
            print(f"  [!] Error fetching pool details: {e}")

        # Success! Gather job times
        times = {"pipeline_total": pipeline_total}

        # Get individual sub_tasks
        sub_tasks = finished_job.get("sub_tasks", [])
        for st in sub_tasks:
            jtype = st.get("type")
            if jtype == "group":
                group_id = st.get("id")
                try:
                    g_resp = requests.get(f"{API_BASE}/jobs/{group_id}", timeout=10)
                    g_resp.raise_for_status()
                    g_job = g_resp.json()

                    # Store elapsed wall time of group
                    times["build_pool_sim_elapsed"] = float(
                        g_job.get("perf_total", 0.0)
                    )

                    # Sum individual tasks in group
                    group_tasks = g_job.get("sub_tasks", [])
                    sum_time = sum(
                        float(gt.get("perf_total", 0.0)) for gt in group_tasks
                    )
                    times["build_pool_sim_sum"] = sum_time
                except Exception as e:
                    print(f"    [!] Error reading group {group_id}: {e}")
            else:
                perf_total = float(st.get("perf_total", 0.0))
                times[jtype] = perf_total

        for jt, t in times.items():
            print(f"    - Job {jt}: {t:.4f}s")

        results.append(
            {
                "min_score": score,
                "pipeline_id": job_id,
                "similarities": similarities,
                "times": times,
            }
        )

    # Clean up pool at the end
    delete_pool(pool_id)

    # Save to JSON
    output_data = {
        "collection": collection,
        "algo": algo,
        "top_k": top_k,
        "results": results,
    }
    with open(json_path, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"[+] Saved results to {json_path}")

    # Generate graph
    generate_graph(output_data, graph_path)


def generate_graph(data, graph_path):
    results = data.get("results", [])
    if not results:
        print("[-] No results to plot.")
        return

    # Sort results by min_score
    results_sorted = sorted(results, key=lambda x: x["min_score"])
    x_vals = [res["min_score"] for res in results_sorted]

    # Find all job type keys except pipeline_total
    job_types = set()
    for res in results:
        job_types.update(res["times"].keys())
    job_types.discard("pipeline_total")
    job_types = sorted(list(job_types))

    # Set up 5 subplots
    fig, axs = plt.subplots(5, 1, figsize=(10, 24))

    algo_name = data.get("algo", "unweighted_cosine")
    top_k = data.get("top_k", 1000)

    # 1. Job Times (excluding pipeline_total) vs Min-Score
    for jt in job_types:
        y_vals = [res["times"].get(jt, 0.0) for res in results_sorted]
        axs[0].plot(x_vals, y_vals, marker="o", label=jt)
    axs[0].set_title(
        f"Sub-task Times vs Min-Score (Coll: {data.get('collection')}, Algo: {algo_name}, Top_K: {top_k})"
    )
    axs[0].set_ylabel("Execution Time (seconds)")
    axs[0].grid(True, linestyle="--", alpha=0.6)
    axs[0].legend()

    # 2. Time divided by similarities vs Min-Score (excluding pipeline_total)
    for jt in job_types:
        y_vals = []
        for res in results_sorted:
            sims = res.get("similarities", 0)
            t = res["times"].get(jt, 0.0)
            ratio = t / sims if sims > 0 else 0.0
            y_vals.append(ratio)
        axs[1].plot(x_vals, y_vals, marker="s", label=f"{jt} (per sim)")
    axs[1].set_title("Sub-task Time per Similarity vs Min-Score")
    axs[1].set_ylabel("Time / Similarities (seconds)")
    axs[1].grid(True, linestyle="--", alpha=0.6)
    axs[1].legend()

    # 3. Similarities vs Min-Score
    sims_vals = [res.get("similarities", 0) for res in results_sorted]
    axs[2].plot(x_vals, sims_vals, marker="^", color="red", label="Similarities")
    axs[2].set_title("Number of Similarities vs Min-Score")
    axs[2].set_ylabel("Total Function Similarities")
    axs[2].grid(True, linestyle="--", alpha=0.6)
    axs[2].legend()

    # 4. Pipeline Total Time vs Min-Score
    total_times = [res["times"].get("pipeline_total", 0.0) for res in results_sorted]
    axs[3].plot(x_vals, total_times, marker="o", color="purple", label="Pipeline Total")
    axs[3].set_title("Pipeline Total Time vs Min-Score")
    axs[3].set_ylabel("Total Time (seconds)")
    axs[3].grid(True, linestyle="--", alpha=0.6)
    axs[3].legend()

    # 5. Time vs Similarity Count
    results_by_sims = sorted(results, key=lambda x: x.get("similarities", 0))
    sims_sorted = [res.get("similarities", 0) for res in results_by_sims]
    total_times_by_sims = [
        res["times"].get("pipeline_total", 0.0) for res in results_by_sims
    ]

    axs[4].plot(
        sims_sorted,
        total_times_by_sims,
        marker="D",
        color="green",
        label="Pipeline Total",
    )
    for jt in job_types:
        y_vals_by_sims = [res["times"].get(jt, 0.0) for res in results_by_sims]
        axs[4].plot(sims_sorted, y_vals_by_sims, marker=".", linestyle=":", label=jt)

    axs[4].set_title("Execution Time vs Similarity Count")
    axs[4].set_xlabel("Similarity Count")
    axs[4].set_ylabel("Time (seconds)")
    axs[4].grid(True, linestyle="--", alpha=0.6)
    axs[4].legend()

    plt.tight_layout()
    plt.savefig(graph_path, dpi=300)
    plt.close()
    print(f"[+] Saved 5-in-1 graph to {graph_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark pool-level similarity build times depending on min-score."
    )
    parser.add_argument(
        "-c", "--collection", help="Collection name to benchmark via pool."
    )
    parser.add_argument(
        "--json",
        default="benchmark_results.json",
        help="Path to save benchmark results JSON.",
    )
    parser.add_argument(
        "--graph", default="graph.png", help="Path to save graph image."
    )
    parser.add_argument("--regraph", help="Path to JSON file to regenerate graph from.")
    parser.add_argument(
        "--algo",
        default="unweighted_cosine",
        help="Similarity algorithm to use (default: unweighted_cosine).",
    )
    parser.add_argument(
        "--min-scores",
        nargs="+",
        type=float,
        default=[0.1, 0.3, 0.5, 0.7, 0.9, 0.95],
        help="List of min-score thresholds to test (default: 0.1 0.3 0.5 0.7 0.9 0.95).",
    )
    parser.add_argument(
        "--top-k", type=int, default=1000, help="Top-K limit parameter (default: 1000)."
    )

    args = parser.parse_args()

    if args.regraph:
        if not os.path.exists(args.regraph):
            print(f"[-] Error: file {args.regraph} not found.")
            sys.exit(1)
        with open(args.regraph, "r") as f:
            data = json.load(f)
        generate_graph(data, args.graph)
    else:
        if not args.collection:
            parser.error(
                "the following arguments are required: -c/--collection (or specify --regraph)"
            )
        run_pool_benchmark(
            args.collection,
            args.json,
            args.graph,
            args.algo,
            args.min_scores,
            args.top_k,
        )


if __name__ == "__main__":
    main()
