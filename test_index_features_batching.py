"""index_functions must read vec:meta / vec:tf in batched round-trips.

The old code did one blocking GET + one ZRANGE per function, which dominated
INDEX_FEATURES wall time. This guards both the batching and the writes it emits.
"""

import json

from bsimvis.app.services.feature_service import FeatureService


class StubPipe:
    def __init__(self, store, stats):
        self.store = store
        self.stats = stats
        self.ops = []

    def get(self, key):
        self.ops.append(("get", key))

    def zrange(self, key, start, end, withscores=False):
        self.ops.append(("zrange", key))

    def hgetall(self, key):
        self.ops.append(("hgetall", key))

    # write side: recorded, no result needed
    def set(self, key, value):
        self.ops.append(("w", "set", key))

    def zadd(self, key, mapping):
        self.ops.append(("w", "zadd", key))

    def zincrby(self, key, amount, member):
        self.ops.append(("w", "zincrby", key))

    def hset(self, key, field, value):
        self.ops.append(("w", "hset", key))

    def sadd(self, key, *values):
        self.ops.append(("w", "sadd", key))

    def execute(self):
        self.stats["executes"] += 1
        out = []
        for op in self.ops:
            if op[0] == "get":
                out.append(self.store.get(op[1]))
            elif op[0] == "zrange":
                out.append(self.store.get(op[1], []))
            elif op[0] == "hgetall":
                out.append(self.store.get(op[1], {}))
            else:
                out.append(True)
        self.ops = []
        return out


class StubRedis:
    def __init__(self, store):
        self.store = store
        self.stats = {"executes": 0, "direct": 0}

    def pipeline(self, transaction=False):
        return StubPipe(self.store, self.stats)

    def get(self, key):
        self.stats["direct"] += 1
        return self.store.get(key)

    def zrange(self, key, start, end, withscores=False):
        self.stats["direct"] += 1
        return self.store.get(key, [])

    def sadd(self, key, *values):
        self.store.setdefault(key, set()).update(values)


def test_batched_reads():
    n = 250
    store = {}
    fids = [f"main:function:abc:{i}" for i in range(n)]
    for fid in fids:
        store[f"{fid}:vec:meta"] = json.dumps([{"hash": f"h{fid}", "tf": 1}, {"hash": f"g{fid}", "tf": 1}])
        store[f"{fid}:vec:tf"] = [(f"h{fid}", 2.0)]

    r = StubRedis(store)
    svc = FeatureService(r)
    assert svc.index_functions("main", fids) is True

    # No per-function blocking reads outside the pipeline.
    assert r.stats["direct"] == 0, r.stats

    # 250 funcs / READ_BATCH 100 => 3 read flushes, plus write flushes.
    assert r.stats["executes"] <= 8, r.stats

    # Features still land in the pending-enrichment set.
    assert len(store["main:features:pending_enrichment"]) == n


def test_missing_data_skipped():
    store = {}
    fids = ["main:function:abc:0", "main:function:abc:1"]
    store[f"{fids[0]}:vec:meta"] = json.dumps([{"hash": "h0", "tf": 1}, {"hash": "g0", "tf": 1}])
    store[f"{fids[0]}:vec:tf"] = [("h0", 1.0)]
    # fids[1] has no data at all -> must be skipped, not crash

    r = StubRedis(store)
    assert FeatureService(r).index_functions("main", fids) is True
    assert store["main:features:pending_enrichment"] == {"h0"}


if __name__ == "__main__":
    test_batched_reads()
    test_missing_data_skipped()
    print("ok")
