import os
import time
import json
import requests
import subprocess
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = os.getenv("APP_PORT", "5000")
API_URL = f"http://{APP_HOST}:{APP_PORT}/api"

BIN_DIR = os.path.expanduser("~/data/malware/mirai/payloads")
COLLECTIONS = ["bench_10", "bench_20", "bench_30", "bench_40", "bench_50", "bench_60"]
BIN_COUNTS = [10, 20, 30, 40, 50, 60]

ALGOS = ["unweighted_cosine", "milvus_sparse"]
SCENARIOS = [{"name": "Realistic", "top_k": 100, "min_score": 0.8, "min_features": 10}]


def wait_for_pipeline(pipeline_id):
    if not pipeline_id:
        return None
    while True:
        try:
            resp = requests.get(f"{API_URL}/jobs/{pipeline_id}")
            if resp.status_code == 200:
                data = resp.json()
                if data["status"] in ["completed", "failed", "cancelled"]:
                    return data
        except Exception as e:
            print(f"[!] Error checking status for {pipeline_id}: {e}")
        time.sleep(2)


def benchmark():
    # --- PHASE 2: BUILD SIMS ---
    print("\n=== PHASE 2: BUILDING SIMILARITIES ===")
    build_results = []
    for coll, count in zip(COLLECTIONS, BIN_COUNTS):
        print(f"\n[*] Benchmarking Build: {coll}...")
        scenario_results = {}
        for scenario in SCENARIOS:
            scenario_results[scenario["name"]] = {}
            for algo in ALGOS:
                print(f"[*] Running Algo: {algo} | Scenario: {scenario['name']}")

                build_payload = {
                    "collection": coll,
                    "algo": algo,
                    "top_k": scenario["top_k"],
                    "min_score": scenario["min_score"],
                    "min_features": scenario["min_features"],
                    "all": True,
                }
                # Use rebuild API which handles CLEAR_SIM + (SYNC_MILVUS if needed) + BUILD_SIM
                resp = requests.post(
                    f"{API_URL}/similarity/rebuild", json=build_payload
                )
                data = resp.json()
                pipe_id = data.get("pipeline_id") or data.get("job_id")

                build_data = wait_for_pipeline(pipe_id)

                build_sim_time = 0
                if "sub_tasks" in build_data:
                    for st in build_data.get("sub_tasks", []):
                        if st["type"] == "build_sim":
                            build_sim_time = st.get("perf_total", 0)
                else:
                    build_sim_time = build_data.get("perf_total", 0)

                # Get Function Count for the collection (actual X-axis)
                func_resp = requests.get(
                    f"{API_URL}/function/search",
                    params={"collection": coll, "limit": 1},
                )
                actual_func_count = 0
                if func_resp.status_code == 200:
                    actual_func_count = func_resp.json().get("total", 0)

                # Get Sim Count using search API (as requested)
                sim_search_resp = requests.get(
                    f"{API_URL}/similarity/search",
                    params={
                        "collection": coll,
                        "algo": algo,
                        "limit": 1,
                        "min_score": scenario["min_score"],
                    },
                )
                sim_count = 0
                if sim_search_resp.status_code == 200:
                    sim_count = sim_search_resp.json().get("total", 0)

                scenario_results[scenario["name"]][algo] = {
                    "time": float(build_sim_time),
                    "sim_count": sim_count,
                    "func_count": actual_func_count,
                }

        build_results.append({"count": count, "scenarios": scenario_results})

    # Save and Plot Final
    with open("benchmark_final_results.json", "w") as f:
        json.dump({"build": build_results}, f, indent=2)

    plot_build_results(build_results)


def plot_build_results(build_results):
    plt.figure(figsize=(14, 8))
    ax1 = plt.gca()
    ax2 = ax1.twinx()  # Secondary axis for counts

    # Sort build_results by func_count just in case
    # We take func_count from the first algo of the first scenario
    first_algo = ALGOS[0]
    build_results.sort(
        key=lambda r: r["scenarios"]["Realistic"][first_algo]["func_count"]
    )

    for i, algo in enumerate(ALGOS):
        # Time curves on AX1
        times = [
            float(r["scenarios"]["Realistic"][algo]["time"]) for r in build_results
        ]
        func_counts = [
            r["scenarios"]["Realistic"][algo]["func_count"] for r in build_results
        ]
        sim_counts = [
            r["scenarios"]["Realistic"][algo]["sim_count"] for r in build_results
        ]

        color = plt.cm.tab10(i)

        # Plot Time (Solid)
        ax1.plot(
            func_counts,
            times,
            marker="o",
            linestyle="-",
            color=color,
            label=f"{algo} Time",
        )

        # Plot Sim Count (Dotted)
        ax2.plot(
            func_counts,
            sim_counts,
            marker="x",
            linestyle=":",
            color=color,
            alpha=0.6,
            label=f"{algo} Sim Count",
        )

        # Add time annotations
        for x, y in zip(func_counts, times):
            ax1.annotate(
                f"{y:.1f}s",
                (x, y),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=9,
                color=color,
            )

    ax1.set_xlabel("Number of Functions")
    ax1.set_ylabel("Building Sim Time (s)")
    ax2.set_ylabel("Number of Similarities Found")
    plt.title("BSim Performance Scaling: Lua vs Milvus (Inverted)")

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig("benchmark_comparison_scaling.png")
    print("[+] Saved benchmark_comparison_scaling.png")


if __name__ == "__main__":
    benchmark()
