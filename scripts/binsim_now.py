#!/usr/bin/env python3
"""Run BinSim + clustering NOW on a collection whose batch is still ingesting.

`scripts/run_pipeline.py` is the polite path: it goes through the collection's
job lane, so it waits for the whole upload wave to finish (hours). This one
reaches straight into the Redis job queue and jumps the line. It is a personal
tool, not an API.

What it does, in order:

  1. Lifts this collection's not-yet-started `ghidra_analyze` jobs out of
     `jobs:pending` into a stash list. Workers stop starting new analyses for it;
     every other collection, and every job already in flight, is untouched.
  2. Waits until no worker is executing anything for this collection. Because
     only fresh analyses were held back, work already under way -- including the
     indexing chain behind a finished analysis -- runs to completion instead of
     freezing half-done, so those files are ready in time to be included.
  3. Creates build_bin_sim -> cluster_binaries -> index_sim -> cluster_functions
     OUTSIDE the lane, first stage marked priority=high so it lands on
     `jobs:pending:high`, which workers drain before `jobs:pending`.
  4. Puts the stash back, in its original order. The high-priority stage is
     already queued by then, so the next free worker takes BinSim rather than the
     analysis it was about to start.

Removing an entry is race-free against a worker claiming it: LREM either removes
it (no worker saw it) or reports 0 (a worker already has it, and we leave it
alone). Nothing is ever queued twice. Job UUIDs, parents and statuses are never
touched -- this only reorders `jobs:pending`.

`--pause` is the blunter alternative: pause the whole fleet for the drain
instead. Simpler, but it stalls every other collection, and it freezes in-flight
files mid-chain so they miss this pass.

If the run dies between stashing and restoring, the ids are still in
`binsim_now:stash:{collection}` and the next run refuses to start until you
recover them with `--restore`.

Nothing about default behavior changes: no CLEAR_* steps, no lane keys written,
no job re-parented, cancelled or dropped. The upload wave still runs its own
clustering pass when it ends -- this is an early preview, not a replacement.

Binaries still mid-analysis are left out of this pass: build_bin_sim's readiness
gate skips any binary whose similarity discovery hasn't run, and the wave's own
pass picks them up later. Re-run this as often as you like; every stage is
idempotent.

The queue Redis runs in protected mode (loopback only), so this has to talk to
it from the server's own host -- either run it there:

    uv run scripts/binsim_now.py MiraiBench --redis localhost:6380 \
        --api http://localhost:5001

or tunnel first, then point it at the local end:

    ssh -N -L 6380:localhost:6380 <host> &
    uv run scripts/binsim_now.py MiraiBench --redis localhost:6380 \
        --api http://10.1.0.143:5001
"""

import argparse
import json
import sys
import time
import urllib.request

# Stages we must never run twice at once: cluster_functions clears the
# collection's clustering before persisting the new one, so two overlapping
# runs orphan each other's state.
STAGES = ("build_bin_sim", "cluster_binaries", "index_sim", "cluster_functions")
TERMINAL = {"completed", "failed", "cancelled", "skipped"}


def job_collection(r, job_id):
    coll, payload = r.hmget(f"job:{job_id}", "collection", "payload")
    if coll:
        return coll
    if payload:
        try:
            return json.loads(payload).get("collection")
        except Exception:
            pass
    return None


def running_for(r, collection):
    """Jobs a worker is actually executing right now for this collection.

    Two filters, both load-bearing. Status, because jobs:processing keeps
    entries a worker already finished with. And a live lease, because a job
    whose worker was SIGKILLed stays "running" until reap_expired() gets to
    it -- without this the drain would wait on a corpse until it times out.
    """
    out = []
    ids = r.lrange("jobs:processing", 0, -1)
    if not ids:
        return out
    now = time.time()
    pipe = r.pipeline(transaction=False)
    for i in ids:
        pipe.hmget(f"job:{i}", "type", "status", "collection", "payload")
    leases = {i: r.zscore("jobs:leased", i) for i in ids}
    for job_id, (jtype, status, coll, payload) in zip(ids, pipe.execute()):
        if status != "running":
            continue
        lease = leases.get(job_id)
        if lease is None or lease < now:
            continue  # dead worker; the reaper owns this one, not us
        if not coll and payload:
            try:
                coll = json.loads(payload).get("collection")
            except Exception:
                coll = None
        if coll == collection:
            out.append((job_id, jtype))
    return out


def stage_conflict(r, collection):
    """A clustering/BinSim stage for this collection already running.

    Catches both the wave's own pass (once its group finishes) and a previous
    binsim_now that hasn't finished yet.
    """
    ids = list(r.smembers("jobs:idx:status:running"))
    if not ids:
        return []
    pipe = r.pipeline(transaction=False)
    for i in ids:
        pipe.hmget(f"job:{i}", "type", "collection", "payload")
    hits = []
    for job_id, (jtype, coll, payload) in zip(ids, pipe.execute()):
        if jtype not in STAGES:
            continue
        if not coll and payload:
            try:
                coll = json.loads(payload).get("collection")
            except Exception:
                coll = None
        if coll == collection:
            hits.append((job_id, jtype))
    return hits


def stash_key(collection):
    return f"binsim_now:stash:{collection}"


def stash_pending(r, collection, key):
    """Lift this collection's not-yet-started analyses out of jobs:pending.

    Only `ghidra_analyze`. The rest of the chain (index_*, build_sim ...) is a
    continuation of a file already being worked on -- holding those back would
    freeze that file half-indexed, which is exactly what we are trying to avoid.

    LREM is the whole safety argument: it removes the entry, or reports 0
    because a worker popped it first. In that case we simply do not stash it,
    so an id can never be both claimed and re-queued later.
    """
    ids = r.lrange("jobs:pending", 0, -1)
    if not ids:
        return 0
    # Batched: this re-runs every few seconds against a queue a live fleet is
    # already hammering, so it must not be one round trip per entry.
    pipe = r.pipeline(transaction=False)
    for job_id in ids:
        pipe.hmget(f"job:{job_id}", "type", "collection", "payload")
    mine = []
    for job_id, (jtype, coll, payload) in zip(ids, pipe.execute()):
        if jtype != "ghidra_analyze":
            continue
        if not coll and payload:
            try:
                coll = json.loads(payload).get("collection")
            except Exception:
                coll = None
        if coll == collection:
            mine.append(job_id)
    if not mine:
        return 0
    pipe = r.pipeline(transaction=False)
    for job_id in mine:
        pipe.lrem("jobs:pending", 0, job_id)
    taken = [j for j, removed in zip(mine, pipe.execute()) if removed]
    if taken:
        r.rpush(key, *taken)
    return len(taken)


def restore_stash(r, key):
    """Put the stash back in its original relative order, then drop it.

    LPUSH in reverse: the list is read head-first, so pushing the tail entry
    first leaves them in the order they came out.
    """
    ids = r.lrange(key, 0, -1)
    if ids:
        r.lpush("jobs:pending", *reversed(ids))
    r.delete(key)
    return len(ids)


def remote_algo(api):
    if not api:
        return "unweighted_cosine"
    try:
        with urllib.request.urlopen(f"{api.rstrip('/')}/api/index/config", timeout=10) as resp:
            cfg = json.loads(resp.read())
        return cfg.get("similarity", {}).get("algo") or "unweighted_cosine"
    except Exception as e:
        print(f"  (could not read remote config: {e}; using unweighted_cosine)")
        return "unweighted_cosine"


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("collection")
    ap.add_argument("--redis", default="localhost:6379", help="queue Redis host:port")
    ap.add_argument("--api", default="", help="base URL, for reading config + printing links")
    ap.add_argument("--algo", default="", help="override; default is the server's similarity.algo")
    ap.add_argument("--min-cohesion", type=float, default=None)
    ap.add_argument(
        "--pause",
        action="store_true",
        help="drain by pausing the whole fleet instead of stashing this "
        "collection's queued analyses (stalls every other collection too)",
    )
    ap.add_argument(
        "--no-wait",
        action="store_true",
        help="skip the drain entirely: queue BinSim immediately and let the "
        "readiness gate drop whatever is mid-analysis",
    )
    ap.add_argument("--restore", action="store_true",
                    help="put back a stash a previous run died holding, then exit")
    ap.add_argument("--drain-timeout", type=int, default=900,
                    help="give up (and put everything back) if the drain takes longer")
    args = ap.parse_args()

    host, _, port = args.redis.partition(":")
    from bsimvis.app.services.redis_client import init_redis

    init_redis(host=host, redis_port=int(port or 6379))
    from bsimvis.app.services.job_service import JobService, JobType

    js = JobService()
    r = js.r
    r.ping()
    key = stash_key(args.collection)

    if args.restore:
        n = restore_stash(r, key)
        print(f"restored {n} job(s) to jobs:pending")
        return 0

    held = r.llen(key)
    if held:
        print(f"refusing: {held} job(s) are still stashed in {key} from an earlier "
              f"run that did not finish. Recover them first:\n"
              f"  uv run scripts/binsim_now.py {args.collection} "
              f"--redis {args.redis} --restore", file=sys.stderr)
        return 1

    conflict = stage_conflict(r, args.collection)
    if conflict:
        print(f"refusing: {conflict[0][1]} already running for {args.collection} "
              f"({conflict[0][0]}). Wait for it -- two overlapping runs corrupt clustering.",
              file=sys.stderr)
        return 1

    algo = args.algo or remote_algo(args.api)
    busy = running_for(r, args.collection)
    print(f"collection {args.collection} | algo {algo} | {len(busy)} job(s) in flight")

    was_paused = bool(r.exists("jobs:paused"))
    paused_by_us = False
    stashed = 0
    try:
        if not args.no_wait:
            if args.pause:
                if not was_paused:
                    # TTL, so a SIGKILL of this script cannot leave the fleet
                    # paused forever -- is_paused() is just an existence check.
                    r.set("jobs:paused", "1", ex=args.drain_timeout + 60)
                    paused_by_us = True
                    print("paused the fleet (in-flight jobs finish, nothing new is claimed)")
            else:
                stashed = stash_pending(r, args.collection, key)
                print(f"held back {stashed} queued analysis job(s) -- other "
                      f"collections keep running")

            deadline = time.time() + args.drain_timeout
            while True:
                # Re-sweep every round: an analysis that finishes hands its file
                # to the next stage, and the wave can enqueue more behind us.
                if not args.pause:
                    more = stash_pending(r, args.collection, key)
                    stashed += more
                busy = running_for(r, args.collection)
                if not busy:
                    break
                if time.time() > deadline:
                    print(f"drain timed out with {len(busy)} still running; aborting "
                          f"(nothing was queued)", file=sys.stderr)
                    return 1
                print(f"  waiting on {len(busy)}: "
                      f"{', '.join(sorted({t for _, t in busy}))}")
                time.sleep(5)
            print("drained -- no worker is executing this collection any more")

        # Re-check now that we hold the field: the wave's own pass may have
        # started during the drain.
        conflict = stage_conflict(r, args.collection)
        if conflict:
            print(f"refusing: {conflict[0][1]} started during the drain "
                  f"({conflict[0][0]}).", file=sys.stderr)
            return 1

        # Payloads carry only what we mean to pin. Every other knob is left out
        # on purpose so the *worker* fills it from the server's own config
        # rather than from this checkout's bsimvis_config.toml.
        first = {"collection": args.collection, "algo": algo, "priority": "high"}
        if args.min_cohesion is not None:
            first["min_cohesion"] = args.min_cohesion
        rest = {"collection": args.collection, "algo": algo}
        tasks = [
            (JobType.BUILD_BIN_SIM, first),
            (JobType.CLUSTER_BINARIES, dict(rest)),
            (JobType.INDEX_SIM, dict(rest)),
            (JobType.CLUSTER_FUNCTIONS, dict(rest)),
        ]
        pipeline_id = js.create_pipeline(tasks, enqueue=False)
        js.start_job(pipeline_id)
        print(f"queued {pipeline_id} on jobs:pending:high -- next free worker takes it")
        if args.api:
            print(f"  {args.api.rstrip('/')}/jobs")
        return 0
    finally:
        # Order matters: BinSim is already on the high queue by now, so the
        # analyses we put back queue behind it rather than in front.
        if r.llen(key):
            print(f"restored {restore_stash(r, key)} held job(s) to jobs:pending")
        if paused_by_us:
            r.delete("jobs:paused")
            print("fleet resumed")
        elif was_paused:
            print("NOTE: the fleet was already paused before this run -- left it "
                  "paused, so nothing starts until you resume it.")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
