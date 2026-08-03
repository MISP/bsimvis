"""enrich_features must stream its pending set and survive being killed.

This is the handler that OOM-killed ten workers across two fleets in one
evening. It loaded the whole pending enrichment set into a Python list
(stdlib-ref holds 375,755 hashes) and only deleted the set after the very last
feature, so every kill restarted it from zero -- five kills meant the same work
was attempted five times and never finished.

These tests pin the two properties that fix that: the set is read in batches,
and each batch is removed once indexed so the set itself is the checkpoint.

Run: uv run python test_enrich_resumable.py
"""

from bsimvis.app.services.feature_service import FeatureService


class FakeSetRedis:
    """Just the set operations enrich_features uses."""

    def __init__(self, members):
        self.members = set(members)
        self.scans = 0

    def scard(self, key):
        return len(self.members)

    def sscan(self, key, cursor=0, count=10):
        self.scans += 1
        # Real SSCAN treats `count` as a hint; returning exactly `count` is a
        # fair stand-in and keeps the batching observable.
        batch = sorted(self.members)[:count]
        return 0, batch

    def srem(self, key, *vals):
        before = len(self.members)
        self.members.difference_update(vals)
        return before - len(self.members)

    def delete(self, key):
        raise AssertionError(
            "enrich_features must checkpoint with SREM per batch, not delete the "
            "whole pending set at the end"
        )


def make_service(members):
    svc = object.__new__(FeatureService)
    svc.r = FakeSetRedis(members)
    return svc


class RecordingService(FeatureService):
    pass


def patched(svc, fail_after_batches=None):
    """Replaces index_global_features with a recorder, optionally exploding."""
    calls = []

    def fake_index(collection, hashes, job_service=None, job_id=None,
                   progress_offset=0, progress_total=None):
        if fail_after_batches is not None and len(calls) >= fail_after_batches:
            raise MemoryError("simulated OOM kill mid-run")
        calls.append(list(hashes))

    svc.index_global_features = fake_index
    return calls


PENDING = [f"fh{i:04d}" for i in range(25)]


def test_pending_set_is_read_in_batches():
    svc = make_service(PENDING)
    calls = patched(svc)

    assert svc.enrich_features("c", batch_size=10) is True

    assert [len(c) for c in calls] == [10, 10, 5], [len(c) for c in calls]
    assert sum(len(c) for c in calls) == 25
    assert svc.r.members == set(), "every processed hash must be checkpointed away"


def test_batch_is_removed_only_after_it_is_indexed():
    svc = make_service(PENDING)
    seen_at_call_time = []

    def fake_index(collection, hashes, *a, **kw):
        # The batch must still be in the pending set while it is being indexed,
        # or a crash here would lose it -- the INDEX_FUNCTIONS bug in set form.
        seen_at_call_time.append(all(h in svc.r.members for h in hashes))

    svc.index_global_features = fake_index
    svc.enrich_features("c", batch_size=10)

    assert all(seen_at_call_time), "a batch was removed before it was indexed"


def test_a_kill_only_costs_the_unfinished_batch():
    svc = make_service(PENDING)
    patched(svc, fail_after_batches=2)

    try:
        svc.enrich_features("c", batch_size=10)
        raise AssertionError("expected the simulated kill to propagate")
    except MemoryError:
        pass

    # Two batches of 10 were indexed and checkpointed; only the rest survives.
    assert len(svc.r.members) == 5, svc.r.members

    # The rerun finishes the job instead of starting over.
    calls = patched(svc)
    assert svc.enrich_features("c", batch_size=10) is True
    assert [len(c) for c in calls] == [5]
    assert svc.r.members == set()


def test_empty_pending_set_is_a_no_op():
    svc = make_service([])
    calls = patched(svc)
    assert svc.enrich_features("c") is True
    assert calls == []


def test_progress_is_reported_across_the_whole_run_not_per_batch():
    svc = make_service(PENDING)
    offsets = []

    def fake_index(collection, hashes, job_service=None, job_id=None,
                   progress_offset=0, progress_total=None):
        offsets.append((progress_offset, progress_total))

    svc.index_global_features = fake_index
    svc.enrich_features("c", batch_size=10)

    assert offsets == [(0, 25), (10, 25), (20, 25)], offsets


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
            passed += 1
    print(f"\n{passed} passed")
