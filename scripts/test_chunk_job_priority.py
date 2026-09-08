"""Chunk indexing jobs must be queued as continuations.

Workers pop jobs:pending from the RIGHT, so a continuation (RPUSH) runs before
any normal job (LPUSH) that is already pending. Guards the batch-upload
behaviour: functions get indexed while other files are still being analyzed.
"""

import json
import time

from bsimvis.app.services.job_service import (
    LEASE_TTL,
    JobService,
    JobStatus,
    JobType,
)
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
        lst = self.lists.get(key, [])
        removed = lst.count(val)
        self.lists[key] = [v for v in lst if v != val]
        return removed

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

    def sismember(self, key, val):
        return val in self.sets.get(key, set())

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def zcard(self, key):
        return len(self.zsets.get(key, {}))

    def zadd(self, key, mapping, xx=False, nx=False):
        z = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if xx and member not in z:
                continue
            if nx and member in z:
                continue
            added += 0 if member in z else 1
            z[member] = score
        return added

    def zrem(self, key, *members):
        z = self.zsets.get(key, {})
        return sum(1 for m in members if z.pop(m, None) is not None)

    def zrangebyscore(self, key, low, high):
        z = self.zsets.get(key, {})
        return [m for m, score in sorted(z.items(), key=lambda kv: kv[1]) if low <= score <= high]

    def zremrangebyscore(self, key, low, high):
        z = self.zsets.get(key, {})
        doomed = [m for m, score in z.items() if low <= score <= high]
        for m in doomed:
            del z[m]
        return len(doomed)

    def hincrby(self, key, field, amount=1):
        h = self.hashes.setdefault(key, {})
        h[field] = str(int(h.get(field, 0)) + amount)
        return int(h[field])

    def incrby(self, key, amount):
        self.strings[key] = str(int(self.strings.get(key, 0)) + amount)
        return int(self.strings[key])

    def hvals(self, key):
        return list(self.hashes.get(key, {}).values())

    def hkeys(self, key):
        return list(self.hashes.get(key, {}).keys())

    def zrange(self, key, start, end, withscores=False):
        members = [m for m, _ in sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])]
        return members[start:] if end == -1 else members[start : end + 1]

    def pipeline(self, transaction=True):
        return StubPipeline(self)

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


class StubPipeline:
    """Queues commands and replays them on execute(), like the real client.

    Returning the connection itself (the old shortcut) made every pipelined
    read run immediately and execute() return nothing, which silently skipped
    the parts of seal_wave that read member payloads.
    """

    def __init__(self, r):
        self.r = r
        self.queued = []

    def __getattr__(self, name):
        method = getattr(self.r, name)

        def queue(*args, **kwargs):
            self.queued.append((method, args, kwargs))
            return self

        return queue

    def execute(self):
        queued, self.queued = self.queued, []
        return [method(*args, **kwargs) for method, args, kwargs in queued]


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


def test_running_group_does_not_advance_lane_queued_pipeline():
    import json as _json

    js = JobService()
    js.r = StubRedis()

    js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    member = js.create_job(JobType.GHIDRA_ANALYZE, {"collection": "main"})
    group = js.create_group([member], enqueue=True)
    queued = js.submit_to_lane(
        "main", [group, (JobType.INDEX_SIM, {"collection": "main"})]
    )
    next_task = _json.loads(js.r.hgetall(f"job:{queued}")["task_ids"])[1]

    js.complete_job(member)

    assert js.r.hgetall(f"job:{queued}")["status"] == "pending"
    assert js.r.hget(f"job:{next_task}", "queued") is None

    js.advance_lane("main")
    assert js.r.hgetall(f"job:{queued}")["status"] == "running"
    assert js.r.hget(f"job:{next_task}", "queued") == "1"


def test_sealed_wave_waits_for_lane_before_starting_analysis():
    js = JobService()
    js.r = StubRedis()

    js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    member = js.create_job(
        JobType.GHIDRA_ANALYZE, {"collection": "main"}, enqueue=False
    )
    queued = js.seal_wave("main", extra_members=[member], options={"skip_sim": True})

    assert js.r.hgetall(f"job:{queued}")["status"] == "pending"
    assert js.r.hget(f"job:{member}", "queued") is None

    js.advance_lane("main")
    assert js.r.hgetall(f"job:{queued}")["status"] == "running"
    assert js.r.hget(f"job:{member}", "queued") == "1"


def test_upload_api_scheduling_modes():
    from unittest.mock import MagicMock, patch

    from flask import Flask

    from bsimvis.app.routes import file as file_route

    app = Flask(__name__)

    def run(query):
        r = MagicMock()
        r.sismember.return_value = False
        r.exists.return_value = False
        jobs = MagicMock()
        jobs.create_job.return_value = "analysis"
        jobs.seal_wave.return_value = "master"

        with (
            app.test_request_context(f"/{query}", method="POST"),
            patch.object(file_route, "get_redis", return_value=r),
            patch.object(file_route, "save_file"),
            patch.object(file_route, "staged_metadata", return_value=None),
            patch.object(file_route, "job_service", jobs),
        ):
            result = file_route._ingest_raw_binary(
                b"x", "x.bin", "main", "batch", "Batch"
            )
        return result, jobs

    result, jobs = run("")
    assert result["pipeline_id"] == "master"
    assert jobs.create_job.call_args.kwargs["enqueue"] is False
    jobs.mark_tail_pending.assert_called_once_with("analysis")
    jobs.seal_wave.assert_called_once()
    jobs.open_or_extend_wave.assert_not_called()
    jobs.enqueue_job.assert_not_called()

    result, jobs = run("?debounce=true")
    assert result["pipeline_id"] == "analysis"
    assert jobs.create_job.call_args.kwargs["enqueue"] is False
    jobs.open_or_extend_wave.assert_called_once()
    jobs.enqueue_job.assert_called_once_with("analysis")
    jobs.seal_wave.assert_not_called()

    result, jobs = run("?enqueue=false")
    assert result["pipeline_id"] == "analysis"
    assert jobs.create_job.call_args.kwargs["enqueue"] is False
    jobs.mark_tail_pending.assert_called_once_with("analysis")
    jobs.seal_wave.assert_not_called()
    jobs.enqueue_job.assert_not_called()


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


def test_similarity_skips_functions_already_built():
    """The reconcile pass must not re-walk functions an earlier build covered."""
    from types import MethodType

    r = StubRedis()
    done, todo = "main:func:a:1", "main:func:b:1"
    r.strings["main:features:generation"] = "1"
    r.sets["main:batch:b:functions"] = {done, todo}
    r.sets["main:indexed:functions"] = {done, todo}
    r.sets["main:built:functions:unweighted_cosine"] = {done}

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
        return 0

    service._process_chunk = MethodType(process, service)

    assert service.build_batch("main", batch_uuid="b") is True
    assert calls == [[todo]]


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
    # Not force: build_batch's generation guard unmarks what went stale, so the
    # reconcile pass builds only that instead of the whole batch again.
    assert "force" not in payload
    assert tasks[-1]["type"] != "enrich_features", "automatic path must not enrich"


def test_waved_analysis_defers_its_own_similarity_build():
    """A file in a wave must not build sims in-line: the wave rebuilds them."""
    js = JobService()
    js.r = StubRedis()

    job_id = js.create_job(JobType.GHIDRA_ANALYZE, {"collection": "main"})
    assert js.r.hget(f"job:{job_id}", "tail_pending") is None

    js.open_or_extend_wave("main", job_id, debounce_seconds=30)
    assert js.r.hget(f"job:{job_id}", "tail_pending") == "1"


def test_enqueue_false_upload_also_defers_its_similarity_build():
    """The upload page uploads with enqueue=false, so it never opens a wave.

    Those jobs run from the group batch_finalize creates and that tail builds
    the batch, so building in-line as well is the same duplicate by a route the
    wave flag alone did not cover.
    """
    js = JobService()
    js.r = StubRedis()

    job_id = js.create_job(
        JobType.GHIDRA_ANALYZE, {"collection": "main"}, enqueue=False
    )
    js.mark_tail_pending(job_id)
    assert js.r.hget(f"job:{job_id}", "tail_pending") == "1"


def test_finalize_folds_pipelines_into_one_wave_without_duplicates():
    """batch_finalize seals the wave instead of submitting a second tail."""
    import json as _json

    js = JobService()
    js.r = StubRedis()

    waved = js.create_job(
        JobType.GHIDRA_ANALYZE, {"collection": "main", "batch_uuid": "batch"}
    )
    js.open_or_extend_wave("main", waved, debounce_seconds=30)

    # The CLI hands back the same id it already uploaded (plus one that never
    # reached a wave, e.g. a JSON-upload pipeline).
    other = js.create_job(JobType.BUILD_SIM, {"collection": "main"})
    pipeline_id = js.seal_wave(
        "main",
        extra_members=[waved, other],
        options={"algo": "unweighted_cosine", "batch_uuid": "batch", "enrich": True},
    )

    task_ids = _json.loads(js.r.hgetall(f"job:{pipeline_id}")["task_ids"])
    tasks = [js.r.hgetall(f"job:{tid}") for tid in task_ids]

    group_members = _json.loads(js.r.hgetall(f"job:{task_ids[0]}")["task_ids"])
    assert group_members == [waved, other], "a waved id must be folded in once"

    build_sims = [t for t in tasks if t["type"] == "build_sim"]
    assert len(build_sims) == 1, "one reconcile build, not one per entry point"
    assert _json.loads(build_sims[0]["payload"])["batch_uuid"] == "batch"
    assert [t["type"] for t in tasks].count("cluster_binaries") == 1
    # Enrichment is finalize's own step, and last.
    assert tasks[-1]["type"] == "enrich_features"


def test_finalize_after_the_wave_sealed_reuses_that_tail():
    """The debounce can expire mid-upload; finalize must not build a 2nd tail."""
    js = JobService()
    js.r = StubRedis()

    member = js.create_job(
        JobType.GHIDRA_ANALYZE, {"collection": "main", "batch_uuid": "batch"}
    )
    js.open_or_extend_wave("main", member, debounce_seconds=30)

    # tick_lanes gets there first, while the client is still uploading.
    auto_tail = js.seal_wave("main")
    assert auto_tail

    unsealed, sealed_into = js.split_sealed([member])
    assert unsealed == []
    assert sealed_into == auto_tail

    # A later upload the wave never saw is still uncovered.
    later = js.create_job(JobType.GHIDRA_ANALYZE, {"collection": "main"})
    unsealed, sealed_into = js.split_sealed([member, later])
    assert unsealed == [later], "only uncovered jobs may join a new group"
    assert sealed_into == auto_tail


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


def test_progress_touches_every_ancestor_not_just_the_parent():
    """A pipeline whose group is busy must not look untouched.

    tick_lanes judges a lane unit dead from `updated_at`, and a pipeline
    wrapping a multi-hour GHIDRA group never wrote its own hash -- so the lane
    was handed to the next unit while this one was still running.
    """
    js = JobService()
    js.r = StubRedis()

    group_id = js.create_group(
        [(JobType.GHIDRA_ANALYZE, {"collection": "main", "file_md5": "a"})],
        enqueue=False,
    )
    pipeline_id = js.create_pipeline(
        [group_id, (JobType.BUILD_SIM, {"collection": "main"})], enqueue=False
    )
    leaf_id = json.loads(js.r.hget(f"job:{group_id}", "task_ids"))[0]
    js.r.hashes[f"job:{pipeline_id}"]["updated_at"] = "0"
    js.r.hashes[f"job:{group_id}"]["updated_at"] = "0"

    js.update_progress(leaf_id, 50)

    assert int(js.r.hget(f"job:{group_id}", "updated_at")) > 0
    assert int(js.r.hget(f"job:{pipeline_id}", "updated_at")) > 0


def _lane_unit_over_a_group(js):
    """A lane unit shaped like a sealed wave: a GHIDRA group, then its tail."""
    group_id = js.create_group(
        [(JobType.GHIDRA_ANALYZE, {"collection": "main", "file_md5": "a"})],
        enqueue=False,
    )
    unit_id = js.submit_to_lane(
        "main", [group_id, (JobType.BUILD_SIM, {"collection": "main"})]
    )
    leaf_id = json.loads(js.r.hget(f"job:{group_id}", "task_ids"))[0]
    return unit_id, leaf_id


def test_stale_unit_with_a_claimed_leaf_keeps_its_lane():
    """The whole bug: five tails ran against one collection because a slow
    unit reads as a crashed one."""
    js = JobService()
    js.r = StubRedis()

    active, leaf_id = _lane_unit_over_a_group(js)
    queued = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )

    js.r.hashes[f"job:{active}"]["updated_at"] = "0"  # stale by any measure
    js.claim_lease(leaf_id, "worker-1")  # ...but its analysis is running

    js.tick_lanes()

    assert js.r.hget(f"job:{queued}", "status") == JobStatus.PENDING.value
    assert js.r.get("lane:main:active") == active
    assert int(js.r.hget(f"job:{active}", "updated_at")) > 0


def test_stale_unit_with_no_live_worker_still_hands_the_lane_over():
    """Crash recovery has to keep working: nothing claimed, so promote."""
    js = JobService()
    js.r = StubRedis()

    active, _leaf = _lane_unit_over_a_group(js)
    queued = js.submit_to_lane(
        "main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})]
    )
    js.r.hashes[f"job:{active}"]["updated_at"] = "0"

    js.tick_lanes()

    assert js.r.get("lane:main:active") == queued
    assert js.r.hget(f"job:{queued}", "status") == JobStatus.RUNNING.value


def test_only_the_active_unit_may_advance_the_lane():
    """A cancelled *queued* unit used to promote a second unit alongside the
    one still running."""
    js = JobService()
    js.r = StubRedis()

    active = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    second = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    third = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])

    js.cancel_job(third)

    assert js.r.get("lane:main:active") == active
    assert js.r.hget(f"job:{second}", "status") == JobStatus.PENDING.value
    assert third not in js.r.lists.get("lane:main:pending", [])


def test_duplicate_terminal_event_does_not_promote_twice():
    js = JobService()
    js.r = StubRedis()

    active = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    second = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])
    third = js.submit_to_lane("main", [(JobType.CLUSTER_FUNCTIONS, {"collection": "main"})])

    js.complete_job(active)
    js.complete_job(active)  # a requeued leaf reporting twice, a retry, a race

    assert js.r.get("lane:main:active") == second
    assert js.r.hget(f"job:{third}", "status") == JobStatus.PENDING.value


def test_expired_lease_fails_the_job_instead_of_requeuing_it():
    js = JobService()
    js.r = StubRedis()

    job_id = js.create_job(JobType.GHIDRA_ANALYZE, {"collection": "main", "file_md5": "a"})
    js.r.lists["jobs:pending"] = []
    js.r.hset(f"job:{job_id}", "status", JobStatus.RUNNING.value)
    js.claim_lease(job_id, "worker-1")
    js.r.rpush("jobs:processing", job_id)

    requeued, failed, _ = js.reap_expired(now=time.time() + LEASE_TTL + 1)

    assert (requeued, failed) == (0, 1)
    assert js.r.hget(f"job:{job_id}", "status") == JobStatus.FAILED.value
    assert js.r.lists.get("jobs:pending", []) == []


if __name__ == "__main__":
    test_chunk_jobs_jump_pending_analysis()
    test_lane_dispatches_immediately_when_idle()
    test_lane_queues_behind_active_unit_fifo()
    test_lane_priority_jumps_the_pending_queue()
    test_advance_lane_promotes_next_pending_unit()
    test_advance_lane_clears_when_nothing_pending()
    test_complete_job_only_advances_lane_for_top_level_jobs()
    test_non_lane_job_does_not_advance_active_lane()
    test_running_group_does_not_advance_lane_queued_pipeline()
    test_sealed_wave_waits_for_lane_before_starting_analysis()
    test_upload_api_scheduling_modes()
    test_similarity_retries_when_feature_generation_changes()
    test_wave_reconciles_each_batch_once_before_clustering()
    test_waved_analysis_defers_its_own_similarity_build()
    test_enqueue_false_upload_also_defers_its_similarity_build()
    test_finalize_folds_pipelines_into_one_wave_without_duplicates()
    test_similarity_skips_functions_already_built()
    test_finalize_after_the_wave_sealed_reuses_that_tail()
    test_wave_seals_into_one_group_not_n_pipelines()
    test_finalize_runs_when_no_build_generations_baseline()
    test_finalize_rejects_when_generations_changed_mid_build()
    test_progress_touches_every_ancestor_not_just_the_parent()
    test_stale_unit_with_a_claimed_leaf_keeps_its_lane()
    test_stale_unit_with_no_live_worker_still_hands_the_lane_over()
    test_only_the_active_unit_may_advance_the_lane()
    test_duplicate_terminal_event_does_not_promote_twice()
    test_expired_lease_fails_the_job_instead_of_requeuing_it()
    print("ok")
