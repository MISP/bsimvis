#!/usr/bin/env python3
"""
Queue BUILD_SIM + INDEX_SIM for exactly the functions that never finished a
discovery pass, without recomputing already-built functions and without
touching CLUSTER_FUNCTIONS.

Mirrors the readiness check in bin_sim_service.py's "Skipping N binaries
whose similarity discovery hasn't finished yet" gate: a binary is ready once
every one of its function IDs is in {collection}:built:functions:{algo}.
This finds every not-ready binary (not just the first 10 the log line
prints), unions their missing function IDs into a fresh batch set, and
queues one BUILD_SIM+INDEX_SIM pipeline scoped to just that batch.

Run from the app's venv, with the same env the running app uses
(KVROCKS_HOST/PORT etc):
    python3 scripts/backfill_missing_discovery.py --collection AndroZooComAndroidSystem
    python3 scripts/backfill_missing_discovery.py --collection AndroZooComAndroidSystem --dry-run
"""
import argparse
import uuid

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services import lineage_service


def find_missing(r, collection, algo):
    file_keys = [
        d.decode() if isinstance(d, bytes) else str(d)
        for d in r.smembers(f"{collection}:all_files")
    ]
    binaries = set()
    for k in file_keys:
        if k.endswith(":meta"):
            continue
        parts = k.split(":")
        if len(parts) >= 3:
            binaries.add(parts[2])

    containers = lineage_service.container_md5s(collection, r)
    if containers:
        binaries -= set(containers)

    built_functions = set(
        f.decode() if isinstance(f, bytes) else f
        for f in r.smembers(f"{collection}:built:functions:{algo}")
    )

    not_ready = []
    missing_fids = set()
    for md5 in binaries:
        fids = r.smembers(f"{collection}:idx:file:functions:{md5}")
        fids = {f.decode() if isinstance(f, bytes) else f for f in fids}
        if fids and not fids.issubset(built_functions):
            not_ready.append(md5)
            missing_fids |= (fids - built_functions)

    return not_ready, missing_fids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--collection", required=True)
    ap.add_argument("--algo", default="unweighted_cosine")
    ap.add_argument("--dry-run", action="store_true", help="report counts, queue nothing")
    args = ap.parse_args()

    r = get_redis()
    not_ready, missing_fids = find_missing(r, args.collection, args.algo)

    print(f"{len(not_ready)} binaries not ready, {len(missing_fids)} functions missing discovery")
    if not not_ready:
        print("nothing to backfill")
        return
    if args.dry_run:
        print("not_ready md5s:", not_ready)
        return

    batch_uuid = str(uuid.uuid4())
    batch_key = f"{args.collection}:batch:{batch_uuid}:functions"
    pipe = r.pipeline(transaction=False)
    for fid in missing_fids:
        pipe.sadd(batch_key, fid)
    pipe.execute()

    job_service = JobService()
    tasks = [
        (JobType.BUILD_SIM, {
            "collection": args.collection, "batch_uuid": batch_uuid, "algo": args.algo,
        }),
        (JobType.INDEX_SIM, {
            "collection": args.collection, "batch_uuid": batch_uuid, "algo": args.algo,
        }),
    ]
    pipeline_id = job_service.create_pipeline(tasks)
    print(f"queued pipeline {pipeline_id} for batch {batch_uuid} ({len(missing_fids)} functions, "
          f"{len(not_ready)} binaries)")


if __name__ == "__main__":
    main()
