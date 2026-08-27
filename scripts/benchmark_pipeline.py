#!/usr/bin/env python3
"""Benchmark the REAL pipeline (function similarity build -> function
clustering -> binary similarity -> binary clustering -> sim indexing) on one
or more already-migrated collections, driving it through the same HTTP API
the dashboard uses -- not a throwaway direct-service-call harness. Whatever
this leaves behind is real, browsable data in the running instance, not a
deleted bench_temp collection.

Does NOT switch the discovery backend itself -- run this once per backend,
with scripts/switch_backend.sh in between (that script edits
bsimvis_config.toml and restarts the stack, since the backend is only read
once at worker start). Pass --backend-label to tag each run's report row
with whichever backend is actually active right now.

Per collection this runs two pipelines end to end and polls each job to
completion via GET /api/jobs/<id>:
    1. POST /api/similarity/rebuild {"collection": X, "all": true}
       (clear + build + index -- the function-level BSim/LCA discovery step)
    2. POST /api/cluster/rebuild_all {"collection": X}
       (clear + cluster_functions + build_bin_sim + cluster_binaries + index_sim)

Usage:
    python3 scripts/benchmark_pipeline.py \\
        --base-url http://localhost:5000 \\
        --collection mycoll_a --collection mycoll_b \\
        --backend-label rust_cpu \\
        --out bench_rust_cpu.json

Run again after switch_backend.sh wgpu with --backend-label wgpu --out
bench_wgpu.json, then diff the two JSON reports (or eyeball the printed
tables) to compare.
"""
import argparse
import json
import sys
import time

import requests


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", default="http://localhost:5000", help="This deployment's APP_HOST:APP_PORT (its own .env), not any fixed value.")
    p.add_argument("--collection", action="append", required=True, dest="collections")
    p.add_argument("--backend-label", required=True, help="Just a report tag -- e.g. rust_cpu or wgpu. Does not switch anything.")
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--timeout", type=float, default=1800.0, help="Max seconds to wait for one pipeline.")
    p.add_argument("--out", default=None, help="Optional path to write the JSON report to.")
    return p.parse_args()


def poll_job(base_url, job_id, poll_interval, timeout):
    t0 = time.time()
    while True:
        r = requests.get(f"{base_url}/api/jobs/{job_id}")
        r.raise_for_status()
        job = r.json()
        status = job.get("status")
        if status in ("completed", "failed"):
            return job
        if time.time() - t0 > timeout:
            raise TimeoutError(f"job {job_id} did not finish within {timeout}s (last status={status})")
        time.sleep(poll_interval)


def run_pipeline(base_url, path, payload, poll_interval, timeout):
    r = requests.post(f"{base_url}{path}", json=payload)
    r.raise_for_status()
    resp = r.json()
    job_id = resp.get("pipeline_id") or resp.get("job_id")
    if not job_id:
        raise RuntimeError(f"POST {path} did not return a job id: {resp}")
    t0 = time.time()
    job = poll_job(base_url, job_id, poll_interval, timeout)
    wall_seconds = time.time() - t0
    return job, wall_seconds


def sum_subtask_perf(base_url, job):
    """Sum perf_total across a pipeline's sub-tasks (job['task_ids']), so the
    report has both wall time (includes queue wait behind other lane work)
    and pure compute time (sum of what each stage actually measured)."""
    task_ids = job.get("task_ids") or []
    total = 0.0
    per_type = {}
    for tid in task_ids:
        if not isinstance(tid, str) or tid.startswith("group_"):
            continue
        r = requests.get(f"{base_url}/api/jobs/{tid}")
        if r.status_code != 200:
            continue
        t = r.json()
        pt = float(t.get("perf_total") or 0)
        total += pt
        per_type[t.get("type", tid)] = pt
    return total, per_type


def fetch_counts(base_url, collection):
    out = {}
    for key, path in (
        ("func_clusters", f"/api/cluster/list?collection={collection}&limit=1"),
        ("bin_clusters", f"/api/bin_cluster/list?collection={collection}&limit=1"),
        ("sim_pairs", f"/api/similarity/status?collection={collection}"),
        ("bin_sim_pairs", f"/api/bin_sim/search?collection={collection}&limit=1"),
    ):
        try:
            r = requests.get(f"{base_url}{path}")
            r.raise_for_status()
            d = r.json()
            if key == "sim_pairs":
                out[key] = d.get("built", d.get("total"))
            else:
                out[key] = d.get("total")
        except Exception as e:
            out[key] = f"error: {e}"
    return out


def main():
    args = parse_args()
    report = {"backend_label": args.backend_label, "collections": {}}

    for coll in args.collections:
        print(f"\n{'=' * 70}\n{coll} ({args.backend_label})\n{'=' * 70}")
        coll_report = {}

        print("[*] similarity rebuild (all)...")
        job, wall = run_pipeline(
            args.base_url,
            "/api/similarity/rebuild",
            {"collection": coll, "all": True},
            args.poll_interval,
            args.timeout,
        )
        if job.get("status") != "completed":
            print(f"[-] similarity rebuild FAILED: {job.get('error')}")
        compute, per_type = sum_subtask_perf(args.base_url, job)
        coll_report["similarity_rebuild"] = {"wall_seconds": wall, "compute_seconds": compute, "per_stage": per_type, "status": job.get("status")}
        print(f"    wall={wall:.2f}s compute={compute:.2f}s status={job.get('status')}")

        print("[*] cluster rebuild_all (cluster_functions + build_bin_sim + cluster_binaries + index_sim)...")
        job, wall = run_pipeline(
            args.base_url,
            "/api/cluster/rebuild_all",
            {"collection": coll},
            args.poll_interval,
            args.timeout,
        )
        if job.get("status") != "completed":
            print(f"[-] cluster rebuild FAILED: {job.get('error')}")
        compute, per_type = sum_subtask_perf(args.base_url, job)
        coll_report["cluster_rebuild_all"] = {"wall_seconds": wall, "compute_seconds": compute, "per_stage": per_type, "status": job.get("status")}
        print(f"    wall={wall:.2f}s compute={compute:.2f}s status={job.get('status')}")

        coll_report["final_counts"] = fetch_counts(args.base_url, coll)
        print(f"    final: {coll_report['final_counts']}")

        report["collections"][coll] = coll_report

    print(f"\n{'=' * 90}")
    print(f"SUMMARY -- backend={args.backend_label}")
    print(f"{'=' * 90}")
    print(f"{'collection':<24} | {'sim wall':>9} | {'sim cpu':>9} | {'cluster wall':>12} | {'cluster cpu':>12} | {'func clu':>8} | {'bin clu':>7}")
    print("-" * 90)
    for coll, r in report["collections"].items():
        sr = r["similarity_rebuild"]
        cr = r["cluster_rebuild_all"]
        fc = r["final_counts"]
        print(
            f"{coll:<24} | {sr['wall_seconds']:>8.2f}s | {sr['compute_seconds']:>8.2f}s "
            f"| {cr['wall_seconds']:>11.2f}s | {cr['compute_seconds']:>11.2f}s "
            f"| {str(fc.get('func_clusters')):>8} | {str(fc.get('bin_clusters')):>7}"
        )
    print("=" * 90)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n[+] report written to {args.out}")


if __name__ == "__main__":
    main()
