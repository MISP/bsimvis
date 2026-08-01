#!/usr/bin/env python3
"""End-to-end pipeline cost of the corpus, and build-sim throughput per algorithm.

Two stages, measured separately because only one of them can change with a
scoring change:

  ingest      `bsimvis upload` against a running stack, with --skip-sim: Ghidra
              decompiles, features are extracted, stored and indexed. Algorithm
              independent, so it is the fixed cost of putting the corpus in.
  build_sim   one similarity build job per binary, per `--algo`. This is the
              O(n^2) candidate walk that a weighting change would slow down, so
              it is what an algorithm comparison is actually about.

Ingest goes through the real client rather than replaying a saved payload: the
dumps for a statically linked binary run to several GB, and a benchmark that
POSTs those in one request measures the JSON parser, not the pipeline.

Because build_sim runs against an index that is already built, the algorithms are
compared on identical input without re-ingesting between runs.

Requires a running stack (in a worktree: scripts/wt-setup.sh) and reads its
ports from the environment, like the rest of the CLI.

Usage:
    scripts/bench/pipeline_bench.py --match '-linux-x64-O2-' --report perf.json
    scripts/bench/pipeline_bench.py --skip-ingest --algo jaccard --algo unweighted_cosine
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

import requests

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, REPO)

API = f"http://{os.getenv('APP_HOST', 'localhost')}:{os.getenv('APP_PORT', '5000')}/api"


def wipe(collection):
    """Drop the collection so every run starts from the same empty state."""
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    n, cursor = 0, 0
    while True:
        cursor, keys = r.scan(cursor, match=f"{collection}:*", count=1000)
        if keys:
            r.delete(*keys)
            n += len(keys)
        if cursor == 0:
            break
    print(f"[*] wiped {n} keys from '{collection}'")


def poll(job_id, timeout=14400):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            job = requests.get(f"{API}/jobs/{job_id}", timeout=30).json()
            if job.get("status") in ("completed", "failed", "cancelled"):
                return job
        except requests.RequestException:
            pass
        time.sleep(2)
    return {"status": "timeout", "id": job_id}


def index_status(collection):
    try:
        return requests.get(f"{API}/index/status",
                            params={"collection": collection}, timeout=60).json()
    except requests.RequestException:
        return {}


def wait_for_queue_drain(collection, quiet_polls=5):
    """Ingest enqueues background work; the run is not over until it settles."""
    stable, last = 0, None
    while stable < quiet_polls:
        s = index_status(collection)
        now = (s.get("num_functions"), s.get("num_indexed"), s.get("num_features"))
        stable = stable + 1 if now == last else 0
        last = now
        time.sleep(3)
    return last


def ingest(binaries, collection, threads, host):
    print(f"[*] ingesting {len(binaries)} binaries via bsimvis upload "
          f"({threads} threads) -- Ghidra runs here")
    t0 = time.time()
    proc = subprocess.run(
        ["uv", "run", "bsimvis", "upload", "--local-analysis", "--skip-sim",
         "-c", collection, "-H", host, "-n", str(threads)]
        + [b["path"] for b in binaries],
        cwd=REPO, capture_output=True, text=True,
    )
    upload_wall = time.time() - t0
    if proc.returncode != 0:
        print(proc.stdout[-2000:])
        print(proc.stderr[-2000:], file=sys.stderr)
    counts = wait_for_queue_drain(collection)
    wall = time.time() - t0

    status = index_status(collection)
    funcs = int(status.get("num_functions", 0) or 0)
    print(f"[+] ingest: {wall:.1f}s wall ({upload_wall:.1f}s in the client), "
          f"{status.get('num_files')} files, {funcs} functions "
          f"({funcs / wall:.1f} funcs/s), {status.get('num_features')} features")
    return {"wall_seconds": wall, "client_seconds": upload_wall,
            "functions": funcs, "funcs_per_second": funcs / wall if wall else 0,
            "index_status": status, "settled_counts": counts,
            "returncode": proc.returncode}


def md5s_in(collection):
    r = requests.get(f"{API}/similarity/batches",
                     params={"collection": collection, "by": "md5"}, timeout=120)
    r.raise_for_status()
    return [x["file_md5"] for x in r.json().get("results", []) if "file_md5" in x]


def clear_sim(collection, algo, md5s):
    for md5 in md5s:
        try:
            requests.post(f"{API}/similarity/clear", json={
                "collection": collection, "algo": algo, "md5": md5}, timeout=120)
        except requests.RequestException:
            pass
    # the clears are queued work; let them land before timing the rebuild
    time.sleep(5)


def build_sim(collection, algo, md5s, top_k, min_score, skip_write=False):
    """One build job per binary, all enqueued up front: this measures the fleet."""
    print(f"[*] build_sim algo={algo} over {len(md5s)} binaries")
    before = int(index_status(collection).get("num_sim_meta", 0) or 0)
    ids = []
    for md5 in md5s:
        resp = requests.post(f"{API}/similarity/build", json={
            "collection": collection, "algo": algo, "md5": md5,
            "top_k": top_k, "min_score": min_score, "skip_write": skip_write,
        }, timeout=120)
        if resp.status_code == 400:
            # The build path refuses algorithms it cannot compute -- currently
            # weighted_cosine, which exists only on the exact-score path.
            reason = resp.json().get("error", "rejected")
            print(f"[!] {algo}: not supported by the build path ({reason})")
            return {"algo": algo, "unsupported": reason, "wall_seconds": 0,
                    "similarities": 0, "sims_per_second": 0,
                    "worker_seconds": {"total": 0.0}, "failed_jobs": 0}
        resp.raise_for_status()
        ids.append(resp.json().get("job_id"))

    t0 = time.time()
    perf = {"python": 0.0, "db": 0.0, "lua": 0.0, "total": 0.0}
    failed = 0
    for jid in ids:
        job = poll(jid)
        if job.get("status") != "completed":
            failed += 1
        for k, field in (("python", "perf_python"), ("db", "perf_db"),
                         ("lua", "perf_lua"), ("total", "perf_total")):
            perf[k] += float(job.get(field) or 0)
    wall = time.time() - t0

    sims = int(index_status(collection).get("num_sim_meta", 0) or 0) - before
    print(f"[+] {algo}: {wall:.1f}s wall, {sims} similarities "
          f"({sims / wall if wall else 0:.0f} sims/s), "
          f"worker-cpu {perf['total']:.1f}s, {failed} failed")
    return {"algo": algo, "wall_seconds": wall, "similarities": sims,
            "sims_per_second": sims / wall if wall else 0,
            "worker_seconds": perf, "failed_jobs": failed}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.environ.get(
        "CORPUS_ROOT", os.path.expanduser("~/data/bsim-bench-corpus")))
    ap.add_argument("--collection", default="bench_corpus")
    ap.add_argument("--algo", action="append", default=[],
                    help="repeatable; default: jaccard, unweighted_cosine, weighted_cosine")
    ap.add_argument("--match", default="-linux-x64-O2-",
                    help="regex selecting corpus binaries by filename")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--threads", type=int, default=4, help="parallel Ghidra in the client")
    ap.add_argument("--top-k", type=int, default=20)
    ap.add_argument("--min-score", type=float, default=0.5)
    ap.add_argument("--skip-ingest", action="store_true",
                    help="reuse an already-ingested collection")
    ap.add_argument("--skip-write", action="store_true",
                    help="score without persisting: isolates scoring from write cost")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    algos = args.algo or ["jaccard", "unweighted_cosine", "weighted_cosine"]
    host = f"{os.getenv('APP_HOST', 'localhost')}:{os.getenv('APP_PORT', '5000')}"
    try:
        requests.get(f"{API}/index/status", params={"collection": args.collection},
                     timeout=10).raise_for_status()
    except requests.RequestException:
        sys.exit(f"no API at {API} -- start the stack (scripts/wt-setup.sh)")

    manifest = json.load(open(os.path.join(args.out, "manifest.json")))
    pat = re.compile(args.match)
    binaries = [b for b in manifest["binaries"] if pat.search(b["file"])]
    if args.limit:
        binaries = binaries[: args.limit]
    if not binaries and not args.skip_ingest:
        sys.exit(f"no corpus binaries match {args.match!r}")

    ing = None
    if not args.skip_ingest:
        wipe(args.collection)
        ing = ingest(binaries, args.collection, args.threads, host)

    md5s = md5s_in(args.collection)
    if not md5s:
        sys.exit("collection has no binaries -- ingest first")
    print(f"[*] {len(md5s)} binaries in '{args.collection}'")

    runs = []
    for algo in algos:
        clear_sim(args.collection, algo, md5s)
        runs.append(build_sim(args.collection, algo, md5s, args.top_k,
                              args.min_score, args.skip_write))

    print(f"\n{'algo':<22}{'wall s':>10}{'sims':>12}{'sims/s':>10}{'worker cpu s':>14}")
    for r in runs:
        if r.get("unsupported"):
            print(f"{r['algo']:<22}   build path does not support it: {r['unsupported']}")
            continue
        print(f"{r['algo']:<22}{r['wall_seconds']:>10.1f}{r['similarities']:>12}"
              f"{r['sims_per_second']:>10.0f}{r['worker_seconds']['total']:>14.1f}")
    timed = [r for r in runs if not r.get("unsupported") and r["wall_seconds"]]
    if len(timed) > 1:
        base = timed[0]
        for r in timed[1:]:
            print(f"{r['algo']} vs {base['algo']}: "
                  f"{r['wall_seconds'] / base['wall_seconds']:.2f}x wall, "
                  f"{r['worker_seconds']['total'] / base['worker_seconds']['total']:.2f}x worker cpu"
                  if base["worker_seconds"]["total"] else "")

    if args.report:
        with open(args.report, "w") as fh:
            json.dump({"ingest": ing, "runs": runs, "binaries": len(md5s),
                       "config": vars(args),
                       "index_status": index_status(args.collection)}, fh, indent=1)
        print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
