"""Cooperative cancel must actually leave a job CANCELLED, not FAILED.

cancel_job (job_service.py) flips a job's status to CANCELLED out-of-band
while a handler is still running. The 3 handlers that cooperatively poll
is_cancelled() then exit early -- by returning False (analysis_orchestrator's
convention) or raising (ghidra_job.py's). Worker._execute_job used to react
to that exit by unconditionally calling complete_job/fail_job, stomping the
CANCELLED status a moment after cancel_job set it. These tests pin the fix:
a job already CANCELLED when its handler returns must be left alone.

Also covers feature_service.enrich_features, one of the loops newly wired to
check is_cancelled() so a stop takes effect between batches instead of only
at the handlers that already had this before job-system-rework-plan.md §4.

Run: uv run python test_job_cancellation.py
"""

from bsimvis.worker import Worker
from bsimvis.app.services.feature_service import FeatureService


class FakeJobService:
    def __init__(self, status="running"):
        self.status = status
        self.logs = []
        self.completed = False
        self.failed_msg = None

    def is_cancelled(self, job_id):
        return self.status == "cancelled"

    def add_log(self, job_id, msg):
        self.logs.append(msg)

    def complete_job(self, job_id):
        self.completed = True

    def fail_job(self, job_id, error_msg, failure_reason=None):
        self.failed_msg = error_msg

    def record_job_peak(self, jtype, peak):
        pass

    def save_performance_stats(self, job_id, stats):
        pass


class FakeQueueRedis:
    def hset(self, key, field=None, value=None, mapping=None):
        pass


def make_worker(job_service):
    w = object.__new__(Worker)
    w.id = "worker-test"
    w.job_service = job_service
    w.r_queue = FakeQueueRedis()
    w._job_peak_rss = 0
    return w


def run_execute(dispatch_result_or_exc, status="running"):
    svc = FakeJobService(status=status)
    w = make_worker(svc)
    if isinstance(dispatch_result_or_exc, Exception):
        w._dispatch = lambda jtype, payload, job_id: (_ for _ in ()).throw(
            dispatch_result_or_exc
        )
    else:
        w._dispatch = lambda jtype, payload, job_id: dispatch_result_or_exc
    w._execute_job("job1", {"type": "idx_features", "payload": "{}"})
    return svc


def test_cancelled_job_returning_false_is_not_marked_failed():
    svc = run_execute(False, status="cancelled")
    assert svc.failed_msg is None, svc.failed_msg
    assert svc.completed is False
    assert any("cancel" in m.lower() for m in svc.logs), svc.logs


def test_cancelled_job_that_raises_is_not_marked_failed():
    svc = run_execute(RuntimeError("cancelled during streaming"), status="cancelled")
    assert svc.failed_msg is None, svc.failed_msg
    assert svc.completed is False
    assert any("cancel" in m.lower() for m in svc.logs), svc.logs


def test_uncancelled_false_still_fails_the_job():
    # Regression guard: the CANCELLED check must not swallow ordinary failures.
    svc = run_execute(False, status="running")
    assert svc.failed_msg == "Job failed (returned False from dispatcher)."
    assert svc.completed is False


def test_uncancelled_true_still_completes_the_job():
    svc = run_execute(True, status="running")
    assert svc.completed is True
    assert svc.failed_msg is None


def test_uncancelled_exception_still_fails_the_job():
    svc = run_execute(RuntimeError("boom"), status="running")
    assert svc.failed_msg == "boom"
    assert svc.completed is False


# --------------------------------------------------------------------------
# enrich_features: cancellation checked between batches (job-system-rework-
# plan.md §4/§7.2's checkpoint-then-check ordering)
# --------------------------------------------------------------------------


class FakeSetRedis:
    def __init__(self, members):
        self.members = set(members)

    def scard(self, key):
        return len(self.members)

    def sscan(self, key, cursor=0, count=10):
        return 0, sorted(self.members)[:count]

    def srem(self, key, *vals):
        self.members.difference_update(vals)


class CancelAfter(FakeJobService):
    def __init__(self, cancel_after_calls):
        super().__init__(status="running")
        self.calls = 0
        self.cancel_after_calls = cancel_after_calls

    def is_cancelled(self, job_id):
        self.calls += 1
        return self.calls > self.cancel_after_calls


def test_enrich_features_stops_between_batches_when_cancelled():
    svc = object.__new__(FeatureService)
    svc.r = FakeSetRedis([f"fh{i:04d}" for i in range(25)])
    indexed = []
    svc.index_global_features = (
        lambda collection, hashes, *a, **kw: indexed.append(list(hashes))
    )
    job_service = CancelAfter(cancel_after_calls=1)

    result = svc.enrich_features(
        "c", batch_size=10, job_service=job_service, job_id="j1"
    )

    assert result is False
    # One batch got through before the cancellation took effect; the rest
    # stayed checkpointed in the pending set for a later resume.
    assert len(indexed) == 1, indexed
    assert len(svc.r.members) == 15, svc.r.members


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
