"""The jobs:processing claim must be released on every exit path of the worker loop.

Regression test for orphaned claims: before the finally-block, any path that did not
reach the success LREM (exception, cancellation, missing metadata) leaked its entry in
jobs:processing forever, because nothing expires or sweeps that list.
"""

import logging

from bsimvis.worker import Worker
from bsimvis.app.services.job_service import JobStatus


class FakeQueue:
    """Stand-in for the redis handle, modelling only the jobs:processing list."""

    def __init__(self, worker, jobs):
        self.worker = worker
        self.jobs = jobs
        self.processing = []
        self._pending = list(jobs)

    def execute_command(self, cmd, *args):
        if self._pending:
            job_id = self._pending.pop(0)
            self.processing.append(job_id)
            return job_id
        # Queue drained: stop the loop instead of spinning on empty BLMOVEs.
        self.worker.running = False
        return None

    def hgetall(self, key):
        return self.jobs.get(key.removeprefix("job:"), {})

    def lrem(self, _key, count, job_id):
        assert count == 0, "count must be 0, or duplicate claims survive forever"
        self.processing = [j for j in self.processing if j != job_id]


class FakeJobService:
    """Lease bookkeeping only: claiming, releasing, and a reaper that does nothing."""

    def __init__(self, queue):
        self.queue = queue
        self.leases = {}

    def claim_lease(self, job_id, owner, ttl=None):
        self.leases[job_id] = owner

    def refresh_lease(self, job_id, ttl=None):
        pass

    def release_lease(self, job_id):
        self.leases.pop(job_id, None)
        self.queue.lrem("jobs:processing", 0, job_id)

    def reap_expired(self, now=None):
        return (0, 0, 0)

    def is_paused(self):
        return False

    def register_worker(self, worker_id, ttl=None):
        self.registered = worker_id

    def unregister_worker(self, worker_id):
        self.registered = None

    def try_admit(self, job_id, jtype):
        return True

    def release_admission(self, job_id):
        pass

    def record_job_peak(self, jtype, peak):
        pass


def make_worker(jobs, execute=lambda job_id, job_data: None):
    w = object.__new__(Worker)
    w.name = "test"
    w.id = "test-0"  # normally f"{name}-{pid}", set in __init__
    w.running = True
    w.current_job_id = None
    w._last_reap = 0.0
    w.r_queue = FakeQueue(w, jobs)
    w.job_service = FakeJobService(w.r_queue)
    w._execute_job = execute
    return w


def drain(jobs, execute=lambda job_id, job_data: None):
    """Run the real Worker.run over `jobs`; return claims still held afterwards."""
    w = make_worker(jobs, execute)
    w.run()
    assert w.job_service.leases == {}, f"lease leaked: {w.job_service.leases}"
    return w.r_queue.processing


def test_success_releases_claim():
    left = drain({"a": {"type": "t", "status": "pending"}})
    assert left == [], f"claim leaked after success: {left}"


def test_jobs_actually_reach_the_executor():
    """Guards the guard.

    Every assertion in this file is about claims being released, and a claim is
    released just as thoroughly when the loop throws before dispatching. When
    admission control was added and FakeJobService lacked try_admit, the whole
    file kept passing while executing nothing at all.
    """
    executed = []
    drain({"a": {"type": "t", "status": "pending"}}, lambda jid, data: executed.append(jid))
    assert executed == ["a"], f"the loop never dispatched the job: {executed}"


def test_exception_releases_claim():
    def boom(job_id, job_data):
        raise RuntimeError("job blew up")

    left = drain({"a": {"type": "t", "status": "pending"}}, boom)
    assert left == [], f"claim leaked after exception: {left}"


def test_cancelled_releases_claim():
    left = drain({"a": {"type": "t", "status": JobStatus.CANCELLED.value}})
    assert left == [], f"claim leaked after cancellation: {left}"


def test_missing_metadata_releases_claim():
    # Claimed id has no job:{id} hash -> loop skips it, must still release.
    w = make_worker({})
    w.r_queue._pending = ["ghost"]
    w.run()
    assert w.r_queue.processing == [], "claim leaked when metadata was missing"


def test_duplicate_claims_all_removed():
    w = object.__new__(Worker)
    q = FakeQueue(w, {})
    q.processing = ["dup", "dup", "dup", "other"]
    q.lrem("jobs:processing", 0, "dup")
    assert q.processing == ["other"], f"duplicate claims survived: {q.processing}"


def test_survivor_claim_is_untouched():
    # Releasing one job must not disturb another worker's in-flight claim.
    left = drain({"a": {"type": "t", "status": "pending"}})
    assert left == []
    w = object.__new__(Worker)
    q = FakeQueue(w, {})
    q.processing = ["mine", "theirs"]
    q.lrem("jobs:processing", 0, "mine")
    assert q.processing == ["theirs"]


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all passed")
