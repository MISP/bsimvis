#!/usr/bin/env python3
"""Fast, throwaway-collection gut-check of discovery backends: legacy
pre-LCA Python discovery ("dev" proxy) vs rust_cpu vs wgpu, all run against
the SAME copy of one source collection in the SAME process, so hardware/data
noise is removed and only discovery_backend varies. Runs each backend
`--repeats` times (default 3) and reports mean/median -- a single run is
noisy at small N (first-call warmup can flip the ranking, see doc).

This is the quick single-machine version for "does this even move the
needle" before committing to the full doc/lca-remote-benchmark-walkthrough.md
flow (migrate real collections, benchmark_pipeline.py through the real API,
keep the result). It calls SimilarityService.build_batch() directly (no job
queue) against throwaway bench_<backend> collections, cleaned up after --
nothing it does persists.

The "dev" run uses this branch's own preserved compatibility-fallback code
path (build_batch's plain per-chunk Python discovery, untouched by the LCA
work) as a stand-in for the pre-LCA `dev` branch algorithm -- same algorithm
dev runs, just without checking out a second branch/stack.

Usage:
    python3 scripts/quick_bench_backends.py --source test_bench --port 7720
    python3 scripts/quick_bench_backends.py --source test_bench --port 7720 \\
        --backends rust_cpu wgpu --repeats 5
"""
import argparse
import statistics
import sys
import time
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.similarity_service import SimilarityService

LABELS = {
    "legacy_proxy": "dev (pre-LCA Python, fallback path)",
    "rust_cpu": "rust_cpu (LCA native CPU)",
    "wgpu": "wgpu (LCA native + GPU)",
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, help="Source collection to copy from (read-only).")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True, help="Kvrocks port.")
    p.add_argument("--backends", nargs="+", default=["legacy_proxy", "rust_cpu", "wgpu"], choices=list(LABELS))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--restore-backend", default="rust_cpu", help="discovery_backend to leave the process config on when done.")
    return p.parse_args()


def cleanup(r, coll):
    keys = r.keys(f"{coll}:*")
    if keys:
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.delete(k)
        pipe.execute()


def copy_collection(r, source_coll, target_coll):
    """Copies functions (plain-string :meta, matching this app's actual
    storage convention) + the LCA vector-class layer wholesale (not part of
    a per-function copy -- it's built once at ingestion, and the LCA
    backends key their whole discovery off it)."""
    source_funcs = list(r.smembers(f"{source_coll}:indexed:functions"))
    pipe = r.pipeline(transaction=False)
    n = 0
    for sfid in source_funcs:
        parts = sfid.split(":")
        if len(parts) < 4:
            continue
        md5, addr = parts[-2], parts[-1]
        tfid = f"{target_coll}:func:{md5}:{addr}"

        tf_data = r.zrange(f"{sfid}:vec:tf", 0, -1, withscores=True)
        if tf_data:
            pipe.zadd(f"{tfid}:vec:tf", dict(tf_data))
            for feat, tf in tf_data:
                pipe.zadd(f"{target_coll}:feature:{feat}:functions", {tfid: tf})

        norm_val = r.get(f"{sfid}:vec:norm")
        if norm_val:
            pipe.set(f"{tfid}:vec:norm", norm_val)

        meta_val = r.get(f"{sfid}:meta")
        if meta_val:
            pipe.set(f"{tfid}:meta", meta_val)

        file_meta_val = r.get(f"{source_coll}:file:{md5}:meta")
        if file_meta_val:
            pipe.set(f"{target_coll}:file:{md5}:meta", file_meta_val)

        pipe.sadd(f"{target_coll}:indexed:functions", tfid)

        feat_count = r.zscore(f"{source_coll}:idx:func:bsim_features_count", sfid)
        if feat_count is not None:
            pipe.zadd(f"{target_coll}:idx:func:bsim_features_count", {tfid: feat_count})

        n += 1
        if n % 200 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()

    cursor = 0
    vc_pipe = r.pipeline(transaction=False)
    vc_n = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"{source_coll}:vclass:*", count=1000)
        for k in keys:
            suffix = k[len(f"{source_coll}:vclass:") :]
            tk = f"{target_coll}:vclass:{suffix}"
            ktype = r.type(k)
            if ktype == "set":
                members = r.smembers(k)
                if members:
                    remapped = [
                        f"{target_coll}:{m[len(source_coll) + 1:]}" if m.startswith(f"{source_coll}:") else m
                        for m in members
                    ]
                    vc_pipe.sadd(tk, *remapped)
            elif ktype == "zset":
                members = r.zrange(k, 0, -1, withscores=True)
                if members:
                    vc_pipe.zadd(tk, dict(members))
            elif ktype == "string":
                val = r.get(k)
                if val is not None:
                    vc_pipe.set(tk, val)
            vc_n += 1
            if vc_n % 200 == 0:
                vc_pipe.execute()
                vc_pipe = r.pipeline(transaction=False)
        if cursor == 0:
            break
    vc_pipe.execute()
    return n


def run_once(r, source_coll, backend_value, run_idx):
    bench_coll = f"bench_{backend_value}_{run_idx}"
    cleanup(r, bench_coll)
    n_funcs = copy_collection(r, source_coll, bench_coll)

    config_service._config.setdefault("similarity", {})["discovery_backend"] = backend_value
    sim_service = SimilarityService(r=r)

    t0 = time.time()
    sim_service.build_batch(bench_coll, algo="unweighted_cosine", index_depth="full")
    elapsed = time.time() - t0

    n_pairs = r.zcard(f"{bench_coll}:sim:score:unweighted_cosine")
    cleanup(r, bench_coll)
    return elapsed, n_pairs, n_funcs


def main():
    args = parse_args()
    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    src_count = r.scard(f"{args.source}:indexed:functions")
    if not src_count:
        print(f"[-] source collection '{args.source}' not found or empty")
        sys.exit(1)
    print(f"[*] source={args.source} functions={src_count} repeats={args.repeats}")

    rows = []
    for backend in args.backends:
        times, pairs_seen, funcs_seen = [], set(), set()
        for i in range(args.repeats):
            elapsed, n_pairs, n_funcs = run_once(r, args.source, backend, i)
            times.append(elapsed)
            pairs_seen.add(n_pairs)
            funcs_seen.add(n_funcs)
            print(f"    {backend} run {i + 1}/{args.repeats}: {elapsed:.3f}s, {n_pairs} pairs")
        rows.append(
            {
                "backend": backend,
                "label": LABELS[backend],
                "mean": statistics.mean(times),
                "median": statistics.median(times),
                "min": min(times),
                "max": max(times),
                "pairs": sorted(pairs_seen),
                "funcs": sorted(funcs_seen),
            }
        )

    print("\n" + "=" * 92)
    print(f"{'Backend':<38} | {'mean':>8} | {'median':>8} | {'min':>8} | {'max':>8} | {'pairs':>10}")
    print("-" * 92)
    for row in rows:
        pairs_str = str(row["pairs"][0]) if len(row["pairs"]) == 1 else f"varies {row['pairs']}"
        print(
            f"{row['label']:<38} | {row['mean']:>7.3f}s | {row['median']:>7.3f}s "
            f"| {row['min']:>7.3f}s | {row['max']:>7.3f}s | {pairs_str:>10}"
        )
    print("=" * 92)

    config_service._config.setdefault("similarity", {})["discovery_backend"] = args.restore_backend


if __name__ == "__main__":
    main()
