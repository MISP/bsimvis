"""Chunk indexing jobs must be queued as continuations.

Workers pop jobs:pending from the RIGHT, so a continuation (RPUSH) runs before
any normal job (LPUSH) that is already pending. Guards the batch-upload
behaviour: functions get indexed while other files are still being analyzed.
"""

from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.similarity_service import SimilarityService


class StubRedis:
    """Minimal stand-in: only the commands the queueing path touches."""

    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.strings = {}
        self.sets = {}
        self.zsets = {}

    def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update({k: str(v) for k, v in mapping.items()})
            return len(mapping)
        created = 0 if field in h else 1
        h[field] = str(value)
        return created

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key, *fields):
        for field in fields:
            self.hashes.get(key, {}).pop(field, None)

    def lpush(self, key, *vals):
        self.lists.setdefault(key, [])[0:0] = list(vals)

    def rpush(self, key, *vals):
        self.lists.setdefault(key, []).extend(vals)

    def ltrim(self, key, start, end):
        pass

    def lrem(self, key, count, val):
        pass

    def lrange(self, key, start, end):
        return list(self.lists.get(key, []))

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lpop(self, key):
        lst = self.lists.get(key, [])
        return lst.pop(0) if lst else None

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = str(value)
        return True

    def setnx(self, key, value):
        if key in self.strings:
            return 0
        self.strings[key] = str(value)
        return 1

    def get(self, key):
        return self.strings.get(key)

    def exists(self, key):
        return 1 if key in self.strings else 0

    def delete(self, *keys):
        for k in keys:
            self.strings.pop(k, None)
            self.lists.pop(k, None)

    def sadd(self, key, *vals):
        s = self.sets.setdefault(key, set())
        added = sum(1 for v in vals if v not in s)
        s.update(vals)
        return added

    def srem(self, key, *vals):
        s = self.sets.get(key, set())
        removed = sum(1 for v in vals if v in s)
        s.difference_update(vals)
        return removed

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def zcard(self, key):
        return len(self.zsets.get(key, {}))

    def zrange(self, key, start, end, withscores=False):
        return []

    def pipeline(self, transaction=True):
        return self

    def execute(self):
        return []

    def eval(self, script, numkeys, *keys):
        # Only the lane's own advance script is ever run through this stub.
        active_key, pending_key = keys[0], keys[1]
        lst = self.lists.get(pending_key, [])
        if lst:
            nxt = lst.pop(0)
            self.strings[active_key] = nxt
            return nxt
        self.strings.pop(active_key, None)
        return None


def pop(r):
    """Same end as the worker's LMOVE/BLMOVE ... RIGHT LEFT."""
    return r.lists["jobs:pending"].pop()


def test_chunk_jobs_jump_pending_analysis():
    js = JobService()
    js.r = StubRedis()

    analyze_ids = [
        js.create_job(
            JobType.GHIDRA_ANALYZE, {"collection": "main", "file_md5": str(i)}
        )
        for i in range(3)
    ]

    chunk_id = js.create_job(
        JobType.INDEX_FUNCTIONS,
        {"collection": "main"},
        parent_id=analyze_ids[0],
        is_subtask=True,
    )
    js.enqueue_job(chunk_id, is_continuation=True)

    assert pop(js.r) == chunk_id, "indexing must not wait behind pending analysis"
    assert pop(js.r) == analyze_ids[0], "analysis order stays FIFO"


def test_lane_dispatches_immediately_when_idle():
    import json as _json

    js = JobService()
    js.r = StubRedis()

    unit_id = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    leaf_id = _json.loads(js.r.hgetall(f"job:{unit_id}")["task_ids"])[0]

    assert js.r.get("lane:main:active") == unit_id
    assert js.r.llen("lane:main:pending") == 0
    assert pop(js.r) == leaf_id, "the pipeline's leaf job must have been enqueued"


def test_lane_queues_behind_active_unit_fifo():
    js = JobService()
    js.r = StubRedis()

    first = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    second = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    third = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )

    assert js.r.get("lane:main:active") == first
    assert js.r.lrange("lane:main:pending", 0, -1) == [second, third]


def test_lane_priority_jumps_the_pending_queue():
    js = JobService()
    js.r = StubRedis()

    first = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    normal = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    urgent = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})], priority=True
    )

    assert js.r.get("lane:main:active") == first
    assert js.r.lrange("lane:main:pending", 0, -1) == [urgent, normal]


def test_advance_lane_promotes_next_pending_unit():
    js = JobService()
    js.r = StubRedis()

    first = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    second = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )

    js.advance_lane("main")

    assert js.r.get("lane:main:active") == second
    assert js.r.llen("lane:main:pending") == 0


def test_advance_lane_clears_when_nothing_pending():
    js = JobService()
    js.r = StubRedis()

    js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    js.advance_lane("main")

    assert js.r.exists("lane:main:active") == 0
    assert "main" not in js.r.smembers("active_lanes")


def test_complete_job_only_advances_lane_for_top_level_jobs():
    import json as _json

    js = JobService()
    js.r = StubRedis()

    # Two tasks so completing the first doesn't itself complete the pipeline
    # (a one-task pipeline's only leaf completing IS the pipeline completing,
    # which wouldn't exercise the leaf-vs-top-level distinction at all).
    two_task_pipeline = [
        (JobType.CLUSTER_FUNCTIONS, {"collection": "main"}),
        (JobType.INDEX_SIM, {"collection": "main"}),
    ]
    first = js.submit_to_lane("main", two_task_pipeline)
    second = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )

    tids = _json.loads(js.r.hgetall(f"job:{first}")["task_ids"])

    # A subtask completing must never touch the lane -- only its top-level
    # pipeline (parent_id == "") does, via advance_parent's own bookkeeping.
    js.complete_job(tids[0])
    assert (
        js.r.get("lane:main:active") == first
    ), "leaf completion must not advance the lane"

    js.complete_job(tids[1])  # last task -> completes the pipeline itself
    assert (
        js.r.get("lane:main:active") == second
    ), "pipeline completion must advance the lane"


def test_non_lane_job_does_not_advance_active_lane():
    js = JobService()
    js.r = StubRedis()

    first = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    direct = js.create_job(JobType.GHIDRA_ANALYZE, {"collection": "main"})

    js.complete_job(direct)

    assert js.r.get("lane:main:active") == first


def test_similarity_retries_when_feature_generation_changes():
    from types import MethodType

    r = StubRedis()
    fid = "main:func:a:1"
    r.strings["main:features:generation"] = "1"
    r.sets["main:batch:b:functions"] = {fid}
    r.sets["main:indexed:functions"] = {fid}

    service = SimilarityService.__new__(SimilarityService)
    service.r = r
    service._pl_cache = {}
    service._pl_pairs = 0
    service._norm_cache = {}
    service._count_cache = {}
    calls = []

    def process(self, collection, chunk, *args, **kwargs):
        calls.append(list(chunk))
        r.sadd("main:built:functions:unweighted_cosine", *chunk)
        if len(calls) == 1:
            r.strings["main:features:generation"] = "2"
        return 0

    service._process_chunk = MethodType(process, service)

    assert service.build_batch("main", batch_uuid="b") is True
    assert calls == [[fid], [fid]]
    assert fid in r.smembers("main:built:functions:unweighted_cosine")


def test_wave_reconciles_each_batch_once_before_clustering():
    import json as _json

    js = JobService()
    js.r = StubRedis()
    members = [
        js.create_job(
            JobType.GHIDRA_ANALYZE,
            {"collection": "main", "file_md5": str(i), "batch_uuid": "batch"},
        )
        for i in range(2)
    ]
    for member in members:
        js.open_or_extend_wave("main", member, debounce_seconds=30)

    pipeline_id = js.seal_wave("main")
    task_ids = _json.loads(js.r.hgetall(f"job:{pipeline_id}")["task_ids"])
    tasks = [js.r.hgetall(f"job:{tid}") for tid in task_ids]
    build_positions = [i for i, task in enumerate(tasks) if task["type"] == "build_sim"]

    assert build_positions == [1]
    payload = _json.loads(tasks[1]["payload"])
    assert payload["batch_uuid"] == "batch"
    assert payload["force"] is True
    assert tasks[2]["type"] == "clear_cluster"


def test_wave_seals_into_one_group_not_n_pipelines():
    js = JobService()
    js.r = StubRedis()

    members = [
        js.create_job(
            JobType.GHIDRA_ANALYZE, {"collection": "main", "file_md5": str(i)}
        )
        for i in range(3)
    ]
    for m in members:
        js.open_or_extend_wave("main", m, debounce_seconds=30)

    assert js.r.llen("lane:main:wave") == 3
    assert js.r.exists("lane:main:wave_deadline") == 1

    # Re-opening does not push the deadline back (fixed window, not sliding).
    deadline_before = js.r.get("lane:main:wave_deadline")
    js.open_or_extend_wave("main", "another-job", debounce_seconds=30)
    assert js.r.get("lane:main:wave_deadline") == deadline_before


def _pool_stub():
    """Pool with two member collections, both at feature generation 0."""
    r = StubRedis()
    pool_id = "p1"
    r.hashes[f"global:pool:{pool_id}:meta"] = {"name": "p", "status": "created"}
    r.sets[f"global:pool:{pool_id}:collections_list"] = {"c1", "c2"}
    return r, pool_id


def test_finalize_runs_when_no_build_generations_baseline():
    """A pool whose init predates generation tracking must still finalize.

    Regression: `json.loads(expected_raw or "{}")` read a missing baseline as
    `{}`, which never equals `{coll: 0}`, so finalize failed on every retry.
    """
    from bsimvis.app.services.pool_service import pool_service

    r, pool_id = _pool_stub()
    pool_service.r = r

    assert pool_service.finalize_pool_build(pool_id) is True
    assert r.hashes[f"global:pool:{pool_id}:meta"]["sync_status"] != "outdated"


def test_finalize_rejects_when_generations_changed_mid_build():
    import json as _json

    from bsimvis.app.services.pool_service import pool_service

    r, pool_id = _pool_stub()
    r.hashes[f"global:pool:{pool_id}:meta"]["build_generations"] = _json.dumps(
        {"c1": 0, "c2": 0}
    )
    r.strings["c2:features:generation"] = "1"  # reingest landed during the build
    pool_service.r = r

    assert pool_service.finalize_pool_build(pool_id) is False
    assert r.hashes[f"global:pool:{pool_id}:meta"]["sync_status"] == "outdated"


if __name__ == "__main__":
    test_chunk_jobs_jump_pending_analysis()
    test_lane_dispatches_immediately_when_idle()
    test_lane_queues_behind_active_unit_fifo()
    test_lane_priority_jumps_the_pending_queue()
    test_advance_lane_promotes_next_pending_unit()
    test_advance_lane_clears_when_nothing_pending()
    test_complete_job_only_advances_lane_for_top_level_jobs()
    test_non_lane_job_does_not_advance_active_lane()
    test_similarity_retries_when_feature_generation_changes()
    test_wave_reconciles_each_batch_once_before_clustering()
    test_wave_seals_into_one_group_not_n_pipelines()
    test_finalize_runs_when_no_build_generations_baseline()
    test_finalize_rejects_when_generations_changed_mid_build()
    print("ok")
