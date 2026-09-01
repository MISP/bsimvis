"""Chunk indexing jobs must be queued as continuations.

Workers pop jobs:pending from the RIGHT, so a continuation (RPUSH) runs before
any normal job (LPUSH) that is already pending. Guards the batch-upload
behaviour: functions get indexed while other files are still being analyzed.
"""

from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.similarity_service import SimilarityService


class StubPipeline:
    """Queues calls and replays them on the stub. No transactions, no WATCH --
    the code under test only ever uses a pipeline to batch reads/writes."""

    def __init__(self, stub):
        self._stub = stub
        self._queued = []

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self._queued.append((name, args, kwargs))
            return self

        return queue

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self):
        results = [
            getattr(self._stub, name)(*args, **kwargs)
            for name, args, kwargs in self._queued
        ]
        self._queued = []
        return results


class StubRedis:
    """Minimal stand-in: only the commands the queueing path touches."""

    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.strings = {}
        self.sets = {}
        self.zsets = {}
        self.streams = {}

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

    def hmget(self, key, *fields):
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = fields[0]
        h = self.hashes.get(key, {})
        return [h.get(f) for f in fields]

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
            self.zsets.pop(k, None)

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

    def scan_iter(self, match):
        import fnmatch

        keys = set(self.hashes) | set(self.lists) | set(self.strings) | set(self.sets)
        return (k for k in keys if fnmatch.fnmatch(k, match))

    def scard(self, key):
        return len(self.sets.get(key, set()))

    # Sorted sets: this branch's job_service keeps a jobs:timeline zset.
    # ponytail: enough of the zset API for the lane paths under test.
    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zcard(self, key):
        return len(self.zsets.get(key, {}))

    def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    def zrem(self, key, *members):
        z = self.zsets.get(key, {})
        for m in members:
            z.pop(m, None)

    def zrange(self, key, start, end, withscores=False, desc=False):
        items = sorted(
            self.zsets.get(key, {}).items(), key=lambda kv: kv[1], reverse=desc
        )
        end = None if end == -1 else end + 1
        items = items[start:end]
        return items if withscores else [m for m, _ in items]

    def zrangebyscore(self, key, minv, maxv, **kwargs):
        lo = float("-inf") if minv in ("-inf", b"-inf") else float(minv)
        hi = float("inf") if maxv in ("+inf", b"+inf") else float(maxv)
        return [
            m
            for m, sc in sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
            if lo <= sc <= hi
        ]

    def zremrangebyscore(self, key, minv, maxv):
        for m in self.zrangebyscore(key, minv, maxv):
            self.zsets[key].pop(m, None)

    def incrby(self, key, amount=1):
        self.strings[key] = str(int(self.strings.get(key, 0)) + amount)
        return int(self.strings[key])

    def expire(self, key, seconds):
        return True

    def xadd(self, key, entry, **kwargs):
        self.streams.setdefault(key, []).append(entry)

    def xrevrange(self, key, *args, **kwargs):
        return []

    def pipeline(self, transaction=False):
        return StubPipeline(self)

    def eval(self, script, numkeys, *args):
        # Two scripts run through this stub: the status setter and the lane's
        # own advance script. Dispatch on the one distinguishing call.
        if "jobs:idx:status:" in script:
            job_key, job_id, new_status = args[0], args[1], args[2]
            old_status = self.hashes.get(job_key, {}).get("status")
            self.hset(job_key, "status", new_status)
            if old_status and old_status != new_status:
                self.srem(f"jobs:idx:status:{old_status}", job_id)
            self.sadd(f"jobs:idx:status:{new_status}", job_id)
            return old_status

        keys = args
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


def _sim_service(r):
    service = SimilarityService.__new__(SimilarityService)
    service.r = r
    service._pl_cache = {}
    service._pl_pairs = 0
    service._norm_cache = {}
    service._count_cache = {}
    return service


def test_similarity_retries_when_feature_generation_changes():
    """Chunked (non-LCA) path: a feature write mid-build must invalidate the
    built markers and rebuild, or the edges missed against the partial reverse
    index stay hidden forever."""
    from types import MethodType

    # algo != unweighted_cosine keeps build_batch on the chunked path
    # regardless of the configured discovery_backend.
    algo = "weighted_cosine"
    r = StubRedis()
    fid = "main:func:a:1"
    r.strings["main:features:generation"] = "1"
    r.sets["main:batch:b:functions"] = {fid}
    r.sets["main:indexed:functions"] = {fid}

    service = _sim_service(r)
    calls = []

    def process(self, collection, chunk, *args, **kwargs):
        calls.append(list(chunk))
        r.sadd(f"main:built:functions:{algo}", *chunk)
        if len(calls) == 1:
            r.strings["main:features:generation"] = "2"
        return 0

    service._process_chunk = MethodType(process, service)

    assert service.build_batch("main", batch_uuid="b", algo=algo) is True
    assert calls == [[fid], [fid]]
    assert fid in r.smembers(f"main:built:functions:{algo}")


def test_lca_path_retries_when_feature_generation_changes():
    """Same guard on the LCA path, which is the default backend here and
    returns before the chunked path's tail ever runs."""
    from types import MethodType

    r = StubRedis()
    fid = "main:func:a:1"
    r.strings["main:features:generation"] = "1"
    r.sets["main:batch:b:functions"] = {fid}
    r.sets["main:indexed:functions"] = {fid}

    service = _sim_service(r)
    snapshots = []

    def snapshot(self, collection, **kwargs):
        snapshots.append(collection)
        # What the real build_lca_snapshot leaves behind for the inline
        # same-class matching below it; None means "no native extension".
        self._base_snapshot = None
        self.vclass_map = {}
        if len(snapshots) == 1:
            r.strings["main:features:generation"] = "2"

    service.build_lca_snapshot = MethodType(snapshot, service)

    assert service.build_batch("main", batch_uuid="b") is True
    assert snapshots == ["main", "main"], "generation bump must force one rebuild"
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
    # The reconcile has to land before anything consumes function similarities.
    consumers = {"build_bin_sim", "cluster_binaries", "cluster_functions", "index_sim"}
    assert min(i for i, task in enumerate(tasks) if task["type"] in consumers) > 1


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
    test_lca_path_retries_when_feature_generation_changes()
    test_wave_reconciles_each_batch_once_before_clustering()
    test_wave_seals_into_one_group_not_n_pipelines()
    print("ok")
