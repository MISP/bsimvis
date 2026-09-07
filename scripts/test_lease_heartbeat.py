"""A lease must not expire under a job that is still running.

Two ways that happened. The heartbeat died on SIGTERM while the job in flight
kept going -- a Ghidra subprocess retry loop can spend minutes there. And when
kvrocks stalls, every worker stops heartbeating at once, so after the freeze the
reaper judged expiry on wall clock and requeued jobs that never stopped. Each
false requeue also burns one of MAX_ATTEMPTS, so three of them kill the job.
"""

import sys
import time
import types
from pathlib import Path

import bsimvis.worker as worker_mod
from bsimvis.worker import Worker
from bsimvis.app.services.job_service import (
    JobService,
    JobStatus,
    JobType,
    LEASE_KEY,
    LEASE_TTL,
    REAPER_RUN_KEY,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_chunk_job_priority import StubRedis  # noqa: E402


class ZStubRedis(StubRedis):
    """StubRedis plus the sorted-set commands the lease path needs."""

    def zadd(self, key, mapping, xx=False):
        z = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if xx and member not in z:
                continue
            added += 0 if member in z else 1
            z[member] = float(score)
        return added

    def zrangebyscore(self, key, low, high):
        z = self.zsets.get(key, {})
        return [m for m, s in sorted(z.items(), key=lambda kv: kv[1]) if low <= s <= high]

    def zrange(self, key, start, end, withscores=False):
        return list(self.zsets.get(key, {}))

    def zrem(self, key, *members):
        z = self.zsets.get(key, {})
        return sum(1 for m in members if z.pop(m, None) is not None)

    def hkeys(self, key):
        return list(self.hashes.get(key, {}))

    def hvals(self, key):
        return list(self.hashes.get(key, {}).values())

    def hincrby(self, key, field, amount=1):
        h = self.hashes.setdefault(key, {})
        h[field] = str(int(h.get(field, 0)) + amount)
        return int(h[field])

    def incrby(self, key, amount):
        self.strings[key] = str(int(self.strings.get(key, 0)) + amount)
        return int(self.strings[key])


class StubJobService:
    def __init__(self):
        self.refreshes = []
        self.registrations = 0

    def register_worker(self, worker_id):
        self.registrations += 1

    def refresh_lease(self, job_id):
        self.refreshes.append(job_id)


def run_heartbeat(stub, ticks):
    """Runs the real loop with sleep stubbed out, for `ticks` iterations."""
    remaining = {"n": ticks}

    def fake_sleep(_seconds):
        remaining["n"] -= 1
        if remaining["n"] <= 0:
            # Whatever the loop condition says, stop the test from spinning.
            stub.running = False
            stub.current_job_id = None

    real_sleep = worker_mod.time.sleep
    worker_mod.time.sleep = fake_sleep
    try:
        Worker._heartbeat_loop(stub)
    finally:
        worker_mod.time.sleep = real_sleep


def make_reaper_case(last_sweep_age, now):
    """A JobService holding one running job whose lease just went stale."""
    js = JobService()
    js.r = ZStubRedis()
    js.r.hset(
        "job:job-1",
        mapping={
            "jtype": JobType.GHIDRA_ANALYZE.value,
            "status": JobStatus.RUNNING.value,
        },
    )
    js.r.zadd(LEASE_KEY, {"job-1": now - 10})
    js.r.set(REAPER_RUN_KEY, str(now - last_sweep_age))
    return js


def test_reaper_pause_grace():
    now = time.time()

    # The fleet was frozen: the sweep itself went missing for 10 minutes. The
    # lease is stale by wall clock, but that says nothing about the job.
    js = make_reaper_case(last_sweep_age=600, now=now)
    assert js.reap_expired(now=now) == (0, 0, 0)
    assert js.r.hgetall("job:job-1")["status"] == JobStatus.RUNNING.value
    assert js.r.hgetall("job:job-1").get("attempts") is None
    renewed = js.r.zsets[LEASE_KEY]["job-1"]
    assert abs(renewed - (now + LEASE_TTL)) < 1, renewed

    # A sweep that ran on schedule still reaps: this worker really is gone.
    js = make_reaper_case(last_sweep_age=30, now=now)
    requeued, failed, _ = js.reap_expired(now=now)
    assert (requeued, failed) == (1, 0), (requeued, failed)
    assert js.r.hgetall("job:job-1")["status"] == JobStatus.PENDING.value
    assert js.r.hgetall("job:job-1")["attempts"] == "1"

    print("OK: reaper skips a sweep it was frozen through, reaps otherwise")


def main():
    # A stopped worker still holding a job keeps refreshing its lease.
    svc = StubJobService()
    stub = types.SimpleNamespace(
        id="worker-test",
        running=False,
        current_job_id="job-1",
        job_service=svc,
        _job_peak_rss=0,
    )
    run_heartbeat(stub, ticks=3)
    assert svc.refreshes == ["job-1"] * 3, svc.refreshes
    assert svc.registrations == 3, svc.registrations

    # A stopped worker holding nothing exits immediately.
    svc = StubJobService()
    stub = types.SimpleNamespace(
        id="worker-test",
        running=False,
        current_job_id=None,
        job_service=svc,
        _job_peak_rss=0,
    )
    run_heartbeat(stub, ticks=3)
    assert svc.refreshes == [], svc.refreshes
    assert svc.registrations == 0, svc.registrations

    print("OK: heartbeat survives stop while a job is in flight")
    test_reaper_pause_grace()


if __name__ == "__main__":
    main()
