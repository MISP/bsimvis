"""
Compare pool BUILD_SIM throughput: sequential (1 process) vs concurrent (N).

Builds per-file pool similarities (the same unit the worker pool runs as
BUILD_POOL_SIM jobs) over one collection wrapped in a throwaway pool. Runs the
SAME file set twice against the same Kvrocks, re-initializing the pool between
runs, while sampling the pool sim-score ZSET over wall-time. Answers:
  1. Does per-job speed decay as the run progresses (slowness building up)?
  2. What is concurrency actually buying vs its per-job slowdown?

Outputs a JSON timeline + a 4-panel PNG.

    # concurrent run uses your real tmux worker fleet (default --workers 0):
    uv run scripts/benchmark_build_concurrency.py            # collection newMirainew
    # or let the script spawn its own N processes instead of the fleet:
    uv run scripts/benchmark_build_concurrency.py --workers 10
    uv run scripts/benchmark_build_concurrency.py --regraph out.json --graph out.png
"""

import sys
import json
import time
import uuid
import logging
import argparse
import threading
import multiprocessing as mp

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()


def list_md5s(collection, limit=None):
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    md5s = []
    for m in r.smembers(f"{collection}:all_files"):
        m = m.decode() if isinstance(m, bytes) else m
        md5s.append(m.rsplit(":", 1)[-1])  # member is "{collection}:file:{md5}"
    md5s.sort()  # deterministic: same order for both runs
    return md5s[:limit] if limit else md5s


def count_functions(collections, md5):
    from bsimvis.app.services.redis_client import get_redis

    r = get_redis()
    return sum(r.scard(f"{c}:idx:file:functions:{md5}") for c in collections)


def pool_sim_zcard(pool_id):
    from bsimvis.app.services.redis_client import get_redis

    return get_redis().zcard(f"global:pool:{pool_id}:sim:score")


def ensure_pool(pool_id, collections, algo, top_k, min_score, min_features):
    from bsimvis.app.services.pool_service import pool_service

    if pool_service.get_pool(pool_id):
        pool_service.delete_pool(pool_id)
    ok, msg = pool_service.create_pool(
        pool_id,
        name="build-concurrency benchmark",
        collections=collections,
        config={
            "func_sim_params": {
                "algo": algo,
                "top_k": top_k,
                "min_score": min_score,
                "min_features": min_features,
            },
        },
    )
    print(f"[*] create_pool: {msg}")


# --- Worker: one BUILD_POOL_SIM unit (own process, own redis connection) ---
def build_one(args):
    pool_id, collections, md5, index_depth, job_id, verbose = args
    from bsimvis.app.services.similarity_service import SimilarityService

    if verbose:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s [%(processName)s] %(message)s"
        )
    svc = SimilarityService()
    # A job_id makes build_pool_file emit per-chunk progress (incl. cur speed) to job_log.
    js = None
    if job_id:
        from bsimvis.app.services.job_service import JobService

        js = JobService()
    n_funcs = count_functions(collections, md5)
    t0 = time.time()
    svc.build_pool_file(
        pool_id, md5, job_service=js, job_id=job_id, index_depth=index_depth
    )
    t1 = time.time()
    return {"md5": md5, "t_start": t0, "t_end": t1, "n_funcs": n_funcs}


class LogTailer(threading.Thread):
    """Prints new job_log entries for a set of job_ids (redis-backed worker logs)."""

    def __init__(self, job_ids, interval=0.5):
        super().__init__(daemon=True)
        self.job_ids, self.interval = list(job_ids), interval
        self.seen = {}
        self._stop = threading.Event()

    def _drain(self):
        from bsimvis.app.services.redis_client import get_queue_redis

        r = get_queue_redis()
        for jid in self.job_ids:
            last_id = self.seen.get(jid, "0")
            entries = r.xrange(f"job_log:{jid}", min=f"({last_id}", max="+")
            for entry_id, fields in entries:  # chronological
                msg = fields.get("message")
                if msg:
                    print(f"    [{jid[:8]}] [{fields.get('ts', '')}] {msg}", flush=True)
                self.seen[jid] = entry_id

    def run(self):
        while not self._stop.is_set():
            self._drain()
            self._stop.wait(self.interval)
        self._drain()

    def stop(self):
        self._stop.set()
        self.join()


def run_via_queue(
    pool_id,
    collections,
    md5s,
    index_depth,
    run_t0,
    verbose=False,
    poll=0.25,
    timeout=36000,
):
    """workers=0 path: enqueue real BUILD_POOL_SIM jobs; the tmux worker fleet runs them.
    Per-job timing comes from each job hash's started_at + our completion-detection time.
    """
    from bsimvis.app.services.job_service import JobService, JobStatus, JobType
    from bsimvis.app.services.redis_client import get_queue_redis

    js, rq = JobService(), get_queue_redis()
    pending = {}
    for md5 in md5s:
        jid = js.create_job(
            JobType.BUILD_POOL_SIM,
            {"pool_id": pool_id, "file_md5": md5, "index_depth": index_depth},
        )
        pending[jid] = md5
    print(
        f"  [+] enqueued {len(pending)} BUILD_POOL_SIM jobs; waiting for the fleet..."
    )

    tailer = None
    if verbose:
        tailer = LogTailer(list(pending))
        tailer.start()

    done, deadline = [], time.time() + timeout
    terminal = {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    }
    while pending and time.time() < deadline:
        for jid in list(pending):
            h = rq.hgetall(f"job:{jid}")
            status = (h.get(b"status") or h.get("status")) if h else None
            status = status.decode() if isinstance(status, bytes) else status
            if status in terminal:
                md5 = pending.pop(jid)
                started = h.get(b"started_at") or h.get("started_at")
                started = float(started) / 1000.0 if started else run_t0
                done.append(
                    {
                        "md5": md5,
                        "t_start": started,
                        "t_end": time.time(),
                        "n_funcs": count_functions(collections, md5),
                    }
                )
        if pending:
            time.sleep(poll)
    if tailer:
        tailer.stop()
    if pending:
        print(
            f"  [!] TIMEOUT: {len(pending)} jobs never finished (is the fleet running?)"
        )
    return done


class Sampler(threading.Thread):
    """Polls the pool sim-score ZSET on an interval; records (elapsed, sims)."""

    def __init__(self, pool_id, interval=0.25):
        super().__init__(daemon=True)
        self.pool_id, self.interval = pool_id, interval
        self.samples = []
        self._stop = threading.Event()

    def run(self):
        base = pool_sim_zcard(self.pool_id)  # ~0 after reset
        t0 = time.time()
        while not self._stop.is_set():
            self.samples.append((time.time() - t0, pool_sim_zcard(self.pool_id) - base))
            self._stop.wait(self.interval)
        self.samples.append((time.time() - t0, pool_sim_zcard(self.pool_id) - base))

    def stop(self):
        self._stop.set()
        self.join()


def run_mode(mode, collections, md5s, index_depth, workers, pool_params, verbose=False):
    # Fresh pool per run => starts empty, no clearing needed.
    pool_id = f"bench_{mode}_{uuid.uuid4().hex[:8]}"
    print(f"\n[*] {mode.upper()}: pool {pool_id}, building {len(md5s)} files...")
    ensure_pool(pool_id, collections, **pool_params)
    time.sleep(0.5)

    # In verbose in-process/spawn modes, give each build a job_id so it emits
    # per-chunk progress to job_log, and tail those logs live.
    job_ids = [f"bench_{uuid.uuid4().hex[:12]}" if verbose else None for _ in md5s]
    job_args = [
        (pool_id, collections, md5, index_depth, jid, verbose)
        for md5, jid in zip(md5s, job_ids)
    ]

    sampler = Sampler(pool_id)
    sampler.start()
    # Local tailer only for build_one paths (sequential or own spawned pool).
    # Fleet mode (workers==0) tails its own job_ids inside run_via_queue.
    tailer = None
    if verbose and (mode == "sequential" or workers > 0):
        tailer = LogTailer([j for j in job_ids if j])
        tailer.start()
    run_t0 = time.time()

    if mode == "sequential":
        jobs = [build_one(a) for a in job_args]
    elif workers == 0:
        # Concurrent via the real queue + tmux worker fleet
        jobs = run_via_queue(
            pool_id, collections, md5s, index_depth, run_t0, verbose=verbose
        )
    else:
        # Concurrent via our own spawned process pool
        ctx = mp.get_context("spawn")  # fresh interpreters: no forked redis pools
        with ctx.Pool(processes=workers) as pool:
            jobs = pool.map(build_one, job_args)

    wall = time.time() - run_t0
    sampler.stop()
    if tailer:
        tailer.stop()

    for j in jobs:
        j["rel_start"] = j["t_start"] - run_t0
        j["rel_end"] = j["t_end"] - run_t0
        dur = j["t_end"] - j["t_start"]
        j["fn_per_s"] = j["n_funcs"] / dur if dur > 0 else 0.0

    total_sims = sampler.samples[-1][1] if sampler.samples else 0
    print(f"  [+] {mode}: {wall:.1f}s wall, {total_sims} sims, {len(md5s)} jobs")
    return {
        "mode": mode,
        "pool_id": pool_id,
        "wall": wall,
        "total_sims": total_sims,
        "timeline": sampler.samples,  # [(elapsed, sims)]
        "jobs": jobs,
    }


def instantaneous(timeline):
    """(elapsed, sims) -> (mid_t, sims_per_s) via finite differences."""
    xs, ys = [], []
    for (t0, s0), (t1, s1) in zip(timeline, timeline[1:]):
        dt = t1 - t0
        if dt > 0:
            xs.append((t0 + t1) / 2)
            ys.append((s1 - s0) / dt)
    return xs, ys


def generate_graph(data, graph_path):
    runs = data["runs"]
    colors = {"sequential": "tab:blue", "concurrent": "tab:red"}

    fig, axs = plt.subplots(2, 2, figsize=(16, 11))
    wk = "fleet" if data["workers"] == 0 else str(data["workers"])
    meta = (
        f"Pool over: {data['collection']} | Algo: {data['algo']} | "
        f"workers: {wk} | files: {data['n_files']}"
    )
    fig.suptitle(f"Pool BUILD_SIM: sequential vs concurrent\n{meta}", fontsize=13)

    # 1. Similarities built vs wall time
    ax = axs[0][0]
    for r in runs:
        tl = r["timeline"]
        ax.plot(
            [t for t, _ in tl],
            [s for _, s in tl],
            label=f"{r['mode']} ({r['wall']:.0f}s)",
            color=colors.get(r["mode"]),
        )
    ax.set_title("Similarities built vs wall time")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("similarities")
    ax.grid(True, ls="--", alpha=0.5)
    ax.legend()

    # 2. Instantaneous throughput vs wall time (does aggregate decay?)
    ax = axs[0][1]
    for r in runs:
        xs, ys = instantaneous(r["timeline"])
        ax.plot(xs, ys, label=r["mode"], color=colors.get(r["mode"]), alpha=0.8)
    ax.set_title("Instantaneous throughput vs wall time")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("sims / s")
    ax.grid(True, ls="--", alpha=0.5)
    ax.legend()

    # 3. Per-job fn/s vs job START time (does starting later => slower job?)
    ax = axs[1][0]
    for r in runs:
        js = sorted(r["jobs"], key=lambda j: j["rel_start"])
        ax.scatter(
            [j["rel_start"] for j in js],
            [j["fn_per_s"] for j in js],
            label=r["mode"],
            color=colors.get(r["mode"]),
            alpha=0.6,
            s=20,
        )
    ax.set_title("Per-job speed vs job start time")
    ax.set_xlabel("job start (s into run)")
    ax.set_ylabel("fn / s (job avg)")
    ax.grid(True, ls="--", alpha=0.5)
    ax.legend()

    # 4. Per-job fn/s vs completion order
    ax = axs[1][1]
    for r in runs:
        js = sorted(r["jobs"], key=lambda j: j["rel_end"])
        ax.plot(
            range(len(js)),
            [j["fn_per_s"] for j in js],
            marker=".",
            label=r["mode"],
            color=colors.get(r["mode"]),
            alpha=0.7,
        )
    ax.set_title("Per-job speed by completion order")
    ax.set_xlabel("job # (completion order)")
    ax.set_ylabel("fn / s (job avg)")
    ax.grid(True, ls="--", alpha=0.5)
    ax.legend()

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(graph_path, dpi=150)
    plt.close()
    print(f"[+] Saved graph to {graph_path}")


def main():
    p = argparse.ArgumentParser(
        description="Sequential vs concurrent pool BUILD_SIM benchmark."
    )
    p.add_argument("-c", "--collection", default="newMirainew")
    p.add_argument("--algo", default="unweighted_cosine")
    p.add_argument(
        "--workers",
        type=int,
        default=0,
        help="0 = enqueue real jobs for the tmux worker fleet; N = spawn N own processes.",
    )
    p.add_argument("--min-score", type=float, default=0.9)
    p.add_argument("--min-features", type=int, default=0)
    p.add_argument("--top-k", type=int, default=1000)
    p.add_argument(
        "--index-depth", default="none", help="Match production BUILD_POOL_SIM (none)."
    )
    p.add_argument("--limit", type=int, help="Max files to test.")
    p.add_argument(
        "--keep-pool",
        action="store_true",
        help="Do not delete the throwaway pools at the end.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Stream worker/build logs (per-chunk progress incl. cur speed).",
    )
    p.add_argument("--json", default="build_concurrency.json")
    p.add_argument("--graph", default="build_concurrency.png")
    p.add_argument("--regraph", help="Regenerate graph from an existing JSON.")
    args = p.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s [%(processName)s] %(message)s"
        )

    if args.regraph:
        with open(args.regraph) as f:
            data = json.load(f)
        generate_graph(data, args.graph)
        return

    collections = [args.collection]
    md5s = list_md5s(args.collection, args.limit)
    if not md5s:
        print(f"[-] No files found in collection {args.collection}")
        sys.exit(1)
    print(f"[*] {len(md5s)} files in {args.collection}")

    pool_params = dict(
        algo=args.algo,
        top_k=args.top_k,
        min_score=args.min_score,
        min_features=args.min_features,
    )
    common = dict(
        collections=collections,
        md5s=md5s,
        index_depth=args.index_depth,
        workers=args.workers,
        pool_params=pool_params,
        verbose=args.verbose,
    )
    runs = [
        run_mode("sequential", **common),
        run_mode("concurrent", **common),
    ]

    if not args.keep_pool:
        from bsimvis.app.services.pool_service import pool_service

        for r in runs:
            pool_service.delete_pool(r["pool_id"])
            print(f"[*] Deleted pool {r['pool_id']}")

    data = {
        "collection": args.collection,
        "algo": args.algo,
        "workers": args.workers,
        "n_files": len(md5s),
        "min_score": args.min_score,
        "top_k": args.top_k,
        "runs": runs,
    }
    with open(args.json, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[+] Saved results to {args.json}")
    generate_graph(data, args.graph)


if __name__ == "__main__":
    main()
