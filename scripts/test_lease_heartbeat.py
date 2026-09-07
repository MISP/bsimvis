"""The heartbeat must outlive a stop signal, until the job in flight is done.

SIGTERM sets `running = False`, but the current job keeps running -- a Ghidra
subprocess retry loop can spend minutes there. If the heartbeat stopped with the
signal, the lease expired underneath a live job and the reaper requeued it,
burning one of MAX_ATTEMPTS each time.
"""

import types

import bsimvis.worker as worker_mod
from bsimvis.worker import Worker


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


if __name__ == "__main__":
    main()
