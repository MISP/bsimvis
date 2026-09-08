"""Lease-based claims and the reaper.

A worker that dies by SIGKILL / OOM / power loss never runs its `finally`, so the
claim it held used to sit in jobs:processing forever: not retried, not failed, not
pending. These tests pin the replacement -- a lease that expires when nobody
refreshes it, and a reaper that requeues exactly those jobs and leaves live ones
alone.

Run: uv run python test_job_leases.py
"""

import time

from bsimvis.app.services.job_service import (
    JobService,
    JobStatus,
    JobType,
    LEASE_KEY,
    MAX_ATTEMPTS,
    PAUSE_KEY,
)


class FakeRedis:
    """Enough of redis to exercise the lease/reaper paths, no server needed."""

    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.zsets = {}
        self.strings = {}

    # --- strings
    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    def get(self, key):
        return self.strings.get(key)

    def delete(self, key):
        self.strings.pop(key, None)
        return 1

    def exists(self, key):
        # Real EXISTS is type-agnostic; checking only strings would silently
        # report every job hash as missing.
        return 1 if (key in self.strings or key in self.hashes) else 0

    # --- hashes
    def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        created = 0
        items = dict(mapping) if mapping else {field: value}
        for f, v in items.items():
            if f not in h:
                created += 1
            h[f] = str(v)
        return created

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        return sum(1 for f in fields if h.pop(f, None) is not None)

    def hincrby(self, key, field, amount=1):
        h = self.hashes.setdefault(key, {})
        h[field] = str(int(h.get(field, 0)) + amount)
        return int(h[field])

    def hkeys(self, key):
        return list(self.hashes.get(key, {}).keys())

    def hvals(self, key):
        return list(self.hashes.get(key, {}).values())

    def incrby(self, key, amount):
        self.strings[key] = str(int(self.strings.get(key, 0)) + amount)
        return int(self.strings[key])

    # --- lists
    def lpush(self, key, *vals):
        lst = self.lists.setdefault(key, [])
        for v in vals:
            lst.insert(0, v)
        return len(lst)

    def rpush(self, key, *vals):
        self.lists.setdefault(key, []).extend(vals)
        return len(self.lists[key])

    def lrange(self, key, start, end):
        lst = self.lists.get(key, [])
        return lst[start:] if end == -1 else lst[start : end + 1]

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lrem(self, key, count, value):
        assert count == 0, "count must be 0, or duplicate claims survive the sweep"
        lst = self.lists.get(key, [])
        removed = lst.count(value)
        self.lists[key] = [v for v in lst if v != value]
        return removed

    def ltrim(self, key, start, end):
        self.lists[key] = self.lists.get(key, [])[start : end + 1]

    # --- zsets
    def zadd(self, key, mapping, xx=False):
        z = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if xx and member not in z:
                continue
            if member not in z:
                added += 1
            z[member] = score
        return added

    def zrem(self, key, member):
        return 1 if self.zsets.get(key, {}).pop(member, None) is not None else 0

    def zrange(self, key, start, end):
        members = sorted(self.zsets.get(key, {}), key=lambda m: self.zsets[key][m])
        return members[start:] if end == -1 else members[start : end + 1]

    def zrangebyscore(self, key, lo, hi):
        z = self.zsets.get(key, {})
        return sorted((m for m, s in z.items() if lo <= s <= hi), key=lambda m: z[m])

    def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    def zcard(self, key):
        return len(self.zsets.get(key, {}))

    def zremrangebyscore(self, key, lo, hi):
        z = self.zsets.get(key, {})
        doomed = [m for m, s in z.items() if lo <= s <= hi]
        for m in doomed:
            del z[m]
        return len(doomed)

    # --- pipeline (single-threaded, so WATCH never actually conflicts here)
    def pipeline(self, transaction=True):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis):
        self.r = redis
        self.queued = []
        self.buffering = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def watch(self, *keys):
        self.watched = keys

    def unwatch(self):
        pass

    def multi(self):
        self.buffering = True

    def hget(self, key, field):
        return self.r.hget(key, field)

    def hgetall(self, key):
        # Buffered so get_global_stats' read pipeline returns results in order.
        self.queued.append(("hgetall", key))
        return self

    def hset(self, key, field=None, value=None, mapping=None):
        if self.buffering:
            self.queued.append(("hset", key, field, value, mapping))
            return self
        return self.r.hset(key, field, value, mapping)

    def execute(self):
        out = []
        for op in self.queued:
            if op[0] == "hgetall":
                out.append(self.r.hgetall(op[1]))
            else:
                out.append(self.r.hset(op[1], op[2], op[3], op[4]))
        self.queued = []
        self.buffering = False
        return out


def make_service():
    svc = object.__new__(JobService)
    svc.r = FakeRedis()
    return svc


def claim(svc, job_id, jtype="idx_features", status=JobStatus.RUNNING.value, ttl=60):
    """Simulates a worker popping a job and taking its lease."""
    svc.r.hset(
        f"job:{job_id}",
        mapping={"id": job_id, "type": jtype, "status": status, "queued": "1"},
    )
    svc.r.lpush("jobs:processing", job_id)
    svc.claim_lease(job_id, "worker-1", ttl=ttl)


# --------------------------------------------------------------------------
# leases
# --------------------------------------------------------------------------


def test_dead_worker_job_is_requeued():
    svc = make_service()
    claim(svc, "dead", ttl=-1)  # lease already expired: nobody refreshed it

    requeued, failed, cleaned = svc.reap_expired()

    assert (requeued, failed, cleaned) == (1, 0, 0)
    assert svc.r.lrange("jobs:pending", 0, -1) == ["dead"]
    assert svc.r.lrange("jobs:processing", 0, -1) == []
    assert svc.r.zscore(LEASE_KEY, "dead") is None
    assert svc.r.hget("job:dead", "status") == JobStatus.PENDING.value
    # The queued latch must be cleared or enqueue_job would refuse the requeue.
    assert svc.r.hget("job:dead", "queued") == "1"  # re-set by enqueue_job
    assert svc.r.hget("job:dead", "attempts") == "1"


def test_live_worker_job_is_left_alone():
    svc = make_service()
    claim(svc, "live", ttl=60)  # heartbeat keeps this one in the future

    requeued, failed, cleaned = svc.reap_expired()

    assert (requeued, failed, cleaned) == (0, 0, 0)
    assert svc.r.lrange("jobs:pending", 0, -1) == []
    assert svc.r.lrange("jobs:processing", 0, -1) == ["live"]
    assert svc.r.hget("job:live", "status") == JobStatus.RUNNING.value


def test_refresh_extends_the_lease_and_defers_the_reaper():
    svc = make_service()
    claim(svc, "busy", ttl=-1)
    svc.refresh_lease("busy", ttl=60)

    assert svc.reap_expired() == (0, 0, 0)
    assert svc.r.lrange("jobs:processing", 0, -1) == ["busy"]


def test_refresh_never_resurrects_a_reaped_lease():
    # Otherwise the job runs twice: once from the requeue, once from the worker
    # that came back to life and kept refreshing.
    svc = make_service()
    claim(svc, "gone", ttl=-1)
    svc.reap_expired()

    svc.refresh_lease("gone", ttl=60)

    assert svc.r.zscore(LEASE_KEY, "gone") is None


def test_terminal_jobs_are_cleared_not_requeued():
    svc = make_service()
    claim(svc, "done", status=JobStatus.COMPLETED.value, ttl=-1)
    claim(svc, "gone", status=JobStatus.CANCELLED.value, ttl=-1)

    requeued, failed, cleaned = svc.reap_expired()

    assert (requeued, failed) == (0, 0)
    assert cleaned == 2
    assert svc.r.lrange("jobs:pending", 0, -1) == []
    assert svc.r.lrange("jobs:processing", 0, -1) == []


def test_unleased_processing_entries_are_swept():
    # The historical leak: entries that predate leases, plus a worker killed
    # between the queue pop and claim_lease.
    svc = make_service()
    svc.r.hset("job:orphan", mapping={"type": "idx_features", "status": "running"})
    svc.r.lpush("jobs:processing", "orphan")

    requeued, _, _ = svc.reap_expired()

    assert requeued == 1
    assert svc.r.lrange("jobs:processing", 0, -1) == []


def test_duplicate_processing_entries_all_disappear():
    svc = make_service()
    claim(svc, "dup", ttl=-1)
    svc.r.lpush("jobs:processing", "dup")  # same job claimed twice

    svc.reap_expired()

    assert svc.r.lrange("jobs:processing", 0, -1) == []


def test_poison_job_fails_instead_of_looping_forever():
    # A job that kills its worker every time must not be requeued indefinitely.
    svc = make_service()
    claim(svc, "poison", ttl=-1)

    for _ in range(MAX_ATTEMPTS):
        svc._last_reap = 0
        svc.r.delete("jobs:reaper:lock")
        svc.reap_expired()
        # worker picks it up again and dies again
        svc.r.hset("job:poison", "status", JobStatus.RUNNING.value)
        svc.r.lpush("jobs:processing", "poison")
        svc.claim_lease("poison", "worker-1", ttl=-1)

    svc.r.delete("jobs:reaper:lock")
    requeued, failed, _ = svc.reap_expired()

    assert (requeued, failed) == (0, 1)
    assert svc.r.hget("job:poison", "status") == JobStatus.FAILED.value


def test_a_job_making_progress_is_not_abandoned():
    # enrich_features checkpoints every batch. Being killed MAX_ATTEMPTS times
    # while permanently enriching features each time is slow, not poison.
    svc = make_service()
    claim(svc, "slow", ttl=-1)

    for i in range(MAX_ATTEMPTS + 3):
        svc.r.hset("job:slow", "processed_items", str((i + 1) * 1000))
        svc.r.delete("jobs:reaper:lock")
        requeued, failed, _ = svc.reap_expired()
        assert (requeued, failed) == (1, 0)
        svc.r.hset("job:slow", "status", JobStatus.RUNNING.value)
        svc.r.lpush("jobs:processing", "slow")
        svc.claim_lease("slow", "worker-1", ttl=-1)

    assert svc.r.hget("job:slow", "status") != JobStatus.FAILED.value


def test_a_job_that_stops_progressing_still_fails():
    # The counter targets jobs that make no progress -- a job that advances once
    # and then stalls must still hit MAX_ATTEMPTS from where it stalled.
    svc = make_service()
    claim(svc, "stalled", ttl=-1)
    svc.r.hset("job:stalled", "processed_items", "500")

    for _ in range(MAX_ATTEMPTS + 1):
        svc.r.delete("jobs:reaper:lock")
        svc.reap_expired()  # processed_items never moves again
        svc.r.hset("job:stalled", "status", JobStatus.RUNNING.value)
        svc.r.lpush("jobs:processing", "stalled")
        svc.claim_lease("stalled", "worker-1", ttl=-1)

    svc.r.delete("jobs:reaper:lock")
    _, failed, _ = svc.reap_expired()

    assert failed == 1
    assert svc.r.hget("job:stalled", "status") == JobStatus.FAILED.value


def test_reaper_lock_stops_a_starting_fleet_double_requeueing():
    svc = make_service()
    claim(svc, "dead", ttl=-1)

    svc.r.set("jobs:reaper:lock", "1", nx=True)  # another worker is mid-sweep
    assert svc.reap_expired() == (0, 0, 0)
    assert svc.r.lrange("jobs:pending", 0, -1) == []


def test_high_priority_job_is_requeued_to_the_high_queue():
    svc = make_service()
    claim(svc, "clear", jtype=JobType.CLEAR_SIM.value, ttl=-1)

    svc.reap_expired()

    assert svc.r.lrange("jobs:pending:high", 0, -1) == ["clear"]
    assert svc.r.lrange("jobs:pending", 0, -1) == []


# --------------------------------------------------------------------------
# pause
# --------------------------------------------------------------------------


def test_pause_flag_round_trips():
    svc = make_service()
    assert svc.is_paused() is False
    assert svc.set_paused(True) is True
    assert svc.r.exists(PAUSE_KEY) == 1
    assert svc.set_paused(False) is False
    assert svc.is_paused() is False


# --------------------------------------------------------------------------
# failure cascade
# --------------------------------------------------------------------------


def test_failure_cancels_the_remaining_children():
    # fail_job cascaded up but not down, so siblings of a failed task kept running.
    svc = make_service()
    svc.r.hset(
        "job:pipe",
        mapping={
            "type": "pipeline",
            "status": "running",
            "task_ids": '["a", "b", "c"]',
        },
    )
    svc.r.hset("job:a", mapping={"type": "t", "status": "completed"})
    svc.r.hset("job:b", mapping={"type": "t", "status": "running"})
    svc.r.hset("job:c", mapping={"type": "t", "status": "pending"})

    svc.fail_job("pipe", "boom")

    assert svc.r.hget("job:a", "status") == JobStatus.COMPLETED.value
    assert svc.r.hget("job:b", "status") == JobStatus.CANCELLED.value
    assert svc.r.hget("job:c", "status") == JobStatus.CANCELLED.value


# --------------------------------------------------------------------------
# task splicing
# --------------------------------------------------------------------------


def test_splice_inserts_after_the_anchor_task():
    svc = make_service()
    svc.r.hset("job:pipe", "task_ids", '["a", "b"]')

    assert svc.splice_tasks("pipe", "a", ["x", "y"]) is True
    assert svc.r.hget("job:pipe", "task_ids") == '["a", "x", "y", "b"]'


def test_splice_appends_when_the_anchor_is_gone():
    svc = make_service()
    svc.r.hset("job:pipe", "task_ids", '["a"]')

    assert svc.splice_tasks("pipe", "missing", ["x"]) is True
    assert svc.r.hget("job:pipe", "task_ids") == '["a", "x"]'


def test_concurrent_splices_both_survive():
    # Read-modify-write of the task_ids blob used to lose one of two parallel
    # chunk splices outright.
    svc = make_service()
    svc.r.hset("job:pipe", "task_ids", '["a"]')

    svc.splice_tasks("pipe", "a", ["chunk1"])
    svc.splice_tasks("pipe", "a", ["chunk2"])

    tids = svc.r.hget("job:pipe", "task_ids")
    assert "chunk1" in tids and "chunk2" in tids


def test_splice_on_a_missing_pipeline_is_a_no_op():
    svc = make_service()
    assert svc.splice_tasks("nope", "a", ["x"]) is False


def test_empty_splice_is_a_no_op():
    svc = make_service()
    svc.r.hset("job:pipe", "task_ids", '["a"]')
    assert svc.splice_tasks("pipe", "a", []) is True
    assert svc.r.hget("job:pipe", "task_ids") == '["a"]'


def test_pausing_a_group_holds_back_its_children():
    # The group is never claimed by a worker -- only its leaves are -- so the
    # flag has to be visible from the leaf that actually gets popped.
    svc = make_service()
    svc.r.hset("job:grp", mapping={"status": JobStatus.RUNNING.value})
    svc.r.hset(
        "job:leaf", mapping={"status": JobStatus.PENDING.value, "parent_id": "grp"}
    )

    assert svc.is_job_paused("leaf") is False

    svc.set_job_paused("grp", True)
    assert svc.is_job_paused("leaf") is True
    # An unrelated job must keep running.
    svc.r.hset("job:other", mapping={"status": JobStatus.PENDING.value})
    assert svc.is_job_paused("other") is False

    svc.set_job_paused("grp", False)
    assert svc.is_job_paused("leaf") is False


def test_pausing_frees_the_queue_for_other_jobs():
    # The point of a per-job pause: capacity goes to everything else at once,
    # so the paused work must leave the pending queue entirely.
    svc = make_service()
    svc.r.hset(
        "job:grp", mapping={"status": JobStatus.RUNNING.value, "task_ids": '["leaf"]'}
    )
    svc.r.hset(
        "job:leaf", mapping={"status": JobStatus.PENDING.value, "parent_id": "grp"}
    )
    svc.enqueue_job("leaf")
    svc.r.hset("job:other", mapping={"status": JobStatus.PENDING.value})
    svc.enqueue_job("other")

    svc.set_job_paused("grp", True)

    pending = svc.r.lrange("jobs:pending", 0, -1)
    assert "leaf" not in pending
    assert "other" in pending  # untouched, workers keep eating it

    svc.set_job_paused("grp", False)
    assert "leaf" in svc.r.lrange("jobs:pending", 0, -1)


def test_resume_requeues_only_what_pause_took():
    # A pipeline has many pending stages but only the current one is queued.
    # Resuming must not fire the whole pipeline in parallel.
    svc = make_service()
    svc.r.hset(
        "job:pipe",
        mapping={"status": JobStatus.RUNNING.value, "task_ids": '["s1", "s2"]'},
    )
    svc.r.hset(
        "job:s1", mapping={"status": JobStatus.PENDING.value, "parent_id": "pipe"}
    )
    svc.r.hset(
        "job:s2", mapping={"status": JobStatus.PENDING.value, "parent_id": "pipe"}
    )
    svc.enqueue_job("s1")  # only stage 1 is live

    svc.set_job_paused("pipe", True)
    assert svc.r.lrange("jobs:pending", 0, -1) == []

    svc.set_job_paused("pipe", False)
    assert svc.r.lrange("jobs:pending", 0, -1) == ["s1"]


def test_a_continuation_under_a_paused_pipeline_does_not_queue():
    # Stage 1 finishes while the pipeline is paused: stage 2 must not start.
    svc = make_service()
    svc.r.hset("job:pipe", mapping={"status": JobStatus.RUNNING.value, "paused": "1"})
    svc.r.hset(
        "job:s2", mapping={"status": JobStatus.PENDING.value, "parent_id": "pipe"}
    )

    svc.enqueue_job("s2")

    assert svc.r.lrange("jobs:pending", 0, -1) == []
    assert svc.r.hget("job:s2", "paused_queued") == "1"

    # ...and resuming the pipeline releases it.
    svc.r.hset("job:pipe", mapping={"task_ids": '["s2"]'})
    svc.set_job_paused("pipe", False)
    assert svc.r.lrange("jobs:pending", 0, -1) == ["s2"]


def test_a_child_still_held_by_an_ancestor_stays_down():
    # Resuming an inner group must not override the outer group's pause.
    svc = make_service()
    svc.r.hset("job:outer", mapping={"paused": "1", "task_ids": '["inner"]'})
    svc.r.hset(
        "job:inner",
        mapping={"parent_id": "outer", "paused": "1", "task_ids": '["leaf"]'},
    )
    svc.r.hset(
        "job:leaf",
        mapping={
            "status": JobStatus.PENDING.value,
            "parent_id": "inner",
            "paused_queued": "1",
        },
    )

    svc.set_job_paused("inner", False)

    assert svc.r.lrange("jobs:pending", 0, -1) == []
    assert svc.r.hget("job:leaf", "paused_queued") == "1"


def test_pause_on_a_missing_job_is_reported_not_invented():
    svc = make_service()
    assert svc.set_job_paused("ghost", True) is None


def test_ancestor_walk_survives_a_parent_cycle():
    svc = make_service()
    svc.r.hset("job:a", mapping={"parent_id": "b"})
    svc.r.hset("job:b", mapping={"parent_id": "a"})
    assert svc.is_job_paused("a") is False


def test_retry_resets_the_lease_attempt_counter():
    # A job that already burned MAX_ATTEMPTS must get a fresh budget when the
    # user retries it, or the next lease expiry fails it immediately.
    import bsimvis.app.routes.jobs as jobs_routes

    svc = make_service()
    svc.r.hset(
        "job:burnt",
        mapping={"status": JobStatus.FAILED.value, "attempts": str(MAX_ATTEMPTS)},
    )

    real = jobs_routes.job_service
    jobs_routes.job_service = svc
    try:
        jobs_routes._reset_job_recursive("burnt")
    finally:
        jobs_routes.job_service = real

    assert svc.r.hget("job:burnt", "status") == JobStatus.PENDING.value
    assert int(svc.r.hget("job:burnt", "attempts")) == 0


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all passed")
