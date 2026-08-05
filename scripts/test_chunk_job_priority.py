"""Chunk indexing jobs must be queued as continuations.

Workers pop jobs:pending from the RIGHT, so a continuation (RPUSH) runs before
any normal job (LPUSH) that is already pending. Guards the batch-upload
behaviour: functions get indexed while other files are still being analyzed.
"""

from bsimvis.app.services.job_service import JobService, JobType


class StubRedis:
    """Minimal stand-in: only the commands the queueing path touches."""

    def __init__(self):
        self.hashes = {}
        self.lists = {}

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

    def lpush(self, key, *vals):
        self.lists.setdefault(key, [])[0:0] = list(vals)

    def rpush(self, key, *vals):
        self.lists.setdefault(key, []).extend(vals)

    def ltrim(self, key, start, end):
        pass

    def lrem(self, key, count, val):
        pass


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


if __name__ == "__main__":
    test_chunk_jobs_jump_pending_analysis()
    print("ok")
