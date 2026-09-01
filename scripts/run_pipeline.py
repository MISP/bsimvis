#!/usr/bin/env python3
"""Run the post-analysis pipeline on a collection while a batch is still ingesting.

    BinSim build -> binary clustering -> sim index -> function clustering

Safe to fire mid-batch with workers running:

* It goes through /api/cluster/rebuild_all with `incremental`, so the CLEAR_*
  steps are dropped -- nothing already built is wiped.
* rebuild_all submits to the collection's job *lane*, so it queues behind
  whatever unit is already active for that collection instead of racing it.
* Partial files are excluded by build_bin_sim's readiness gate: a binary whose
  own similarity discovery hasn't finished is skipped this round and paired on
  a later run. Nothing is computed against a half-analyzed file.

Re-runnable: every stage is idempotent, so run it again whenever more of the
batch has landed.

    uv run scripts/run_pipeline.py MiraiBench --url http://10.1.0.143:5001 --wait
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

TERMINAL = {"completed", "failed", "cancelled"}


def _call(url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("collection")
    ap.add_argument("--url", default=os.getenv("API_URL", "http://localhost:5000"),
                    help="BSimVis base URL (env API_URL)")
    ap.add_argument("--algo", default="unweighted_cosine")
    ap.add_argument("--min-cohesion", type=float, default=0.5)
    ap.add_argument("--priority", action="store_true",
                    help="jump the collection's lane queue (still waits for the active unit)")
    ap.add_argument("--wait", action="store_true", help="poll until the pipeline finishes")
    ap.add_argument("--interval", type=int, default=15, help="poll seconds (default 15)")
    args = ap.parse_args()

    base = args.url.rstrip("/")
    res = _call(
        f"{base}/api/cluster/rebuild_all",
        {
            "collection": args.collection,
            "algo": args.algo,
            "incremental": True,
            "min_cohesion": args.min_cohesion,
            "priority": "high" if args.priority else "",
        },
    )
    job_id = res.get("pipeline_id") or res.get("job_id")
    if not job_id:
        print(f"unexpected response: {res}", file=sys.stderr)
        return 2
    # Abort if the server predates the `incremental` flag: it would have built a
    # clear-then-rebuild pipeline, which wipes live clusters/bin_sim mid-batch.
    # Cancel before a worker claims it rather than trusting the deploy.
    job = _call(f"{base}/api/jobs/{job_id}")
    clears = [t["type"] for t in job.get("sub_tasks", []) if str(t["type"]).startswith("clear_")]
    if clears:
        _call(f"{base}/api/jobs/{job_id}/cancel", {})
        print(
            f"ABORTED: server built a destructive pipeline ({', '.join(clears)}) -- it "
            f"does not know the `incremental` flag yet. Pipeline {job_id} cancelled. "
            f"Deploy this branch and restart the app, then re-run.",
            file=sys.stderr,
        )
        return 2

    print(f"queued pipeline {job_id} on lane '{args.collection}'")
    print(f"  {base}/jobs")
    if not args.wait:
        return 0

    seen = {}
    while True:
        job = _call(f"{base}/api/jobs/{job_id}")
        for t in job.get("sub_tasks", []):
            key = (t["type"], t["status"], t["progress"] // 10)
            if seen.get(t["id"]) != key:
                seen[t["id"]] = key
                print(f"  {t['type']:<18} {t['status']:<10} {t['progress']:>3}%")
        status = job.get("status")
        if status in TERMINAL:
            print(f"pipeline {status}")
            for line in reversed(job.get("logs", [])[:20]):
                print(f"  | {line}")
            return 0 if status == "completed" else 1
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.URLError as e:
        print(f"cannot reach API: {e}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("\ndetached -- pipeline keeps running server-side")
        sys.exit(0)
