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


def drain(jobs, execute=lambda job_id, job_data: None):
    """Run the real Worker.run over `jobs`; return claims still held afterwards."""
    w = object.__new__(Worker)
    w.name = "test"
    w.running = True
    w.r_queue = FakeQueue(w, jobs)
    w._execute_job = execute
    w.run()
    return w.r_queue.processing


def test_success_releases_claim():
    left = drain({"a": {"type": "t", "status": "pending"}})
    assert left == [], f"claim leaked after success: {left}"


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
    w = object.__new__(Worker)
    w.name = "test"
    w.running = True
    w.r_queue = FakeQueue(w, {})
    w.r_queue._pending = ["ghost"]
    w._execute_job = lambda job_id, job_data: None
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
