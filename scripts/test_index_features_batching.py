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

    # write side: applied to the store so results can be asserted
    def _w(self, fn):
        self.stats["cmds"] += 1
        self.ops.append(("w", fn))

    def mset(self, mapping):
        self._w(lambda: self.store.update(mapping))

    def zadd(self, key, mapping):
        self._w(lambda: self.store.setdefault(key, {}).update(mapping))

    def zincrby(self, key, amount, member):
        def apply():
            z = self.store.setdefault(key, {})
            z[member] = z.get(member, 0.0) + amount

        self._w(apply)

    def hset(self, key, field=None, value=None, mapping=None):
        self._w(
            lambda: self.store.setdefault(key, {}).update(mapping or {field: value})
        )

    def sadd(self, key, *values):
        self._w(lambda: self.store.setdefault(key, set()).update(values))

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
                op[1]()
                out.append(True)
        self.ops = []
        return out


class StubRedis:
    def __init__(self, store):
        self.store = store
        self.stats = {"executes": 0, "direct": 0, "cmds": 0}

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


def test_batched_and_correct():
    """Realistic shape: many functions sharing a small pool of feature hashes."""
    n, per_func, pool = 250, 40, 60
    store = {}
    fids = [f"main:function:abc:{i}" for i in range(n)]
    hashes = {}  # fid -> {f_hash: tf}
    for k, fid in enumerate(fids):
        hs = {f"h{(k + j) % pool}": float(j + 1) for j in range(per_func)}
        hashes[fid] = hs
        store[f"{fid}:vec:meta"] = json.dumps(
            [{"hash": h, "tf": tf} for h, tf in hs.items()]
        )
        store[f"{fid}:vec:tf"] = list(hs.items())

    r = StubRedis(store)
    assert FeatureService(r).index_functions("main", fids) is True

    # No per-function blocking reads outside the pipeline.
    assert r.stats["direct"] == 0, r.stats

    # Naive fan-out would be n*per_func*3 = 30000 commands.
    assert r.stats["cmds"] < 3000, r.stats

    # --- writes must be identical to the un-merged version ---
    expected_by_tf = {}
    for fid, hs in hashes.items():
        for h, tf in hs.items():
            expected_by_tf[h] = expected_by_tf.get(h, 0.0) + tf
            assert store[f"main:feature:{h}:functions"][fid] == tf
            entry = json.loads(store[f"main:feature:{h}:meta"][fid])
            assert entry["function_id"] == fid and entry["hash"] == h
    for h, total in expected_by_tf.items():
        assert abs(store["main:features:by_tf"][h] - total) < 1e-9, h

    assert store["main:indexed:functions"] == set(fids)
    assert len(store["main:features:pending_enrichment"]) == pool
    # L2 norm per function
    fid = fids[0]
    assert (
        abs(store[f"{fid}:vec:norm"] ** 2 - sum(tf**2 for tf in hashes[fid].values()))
        < 1e-6
    )


def test_missing_data_skipped():
    store = {}
    fids = ["main:function:abc:0", "main:function:abc:1"]
    store[f"{fids[0]}:vec:meta"] = json.dumps(
        [{"hash": "h0", "tf": 1}, {"hash": "g0", "tf": 1}]
    )
    store[f"{fids[0]}:vec:tf"] = [("h0", 1.0)]
    # fids[1] has no data at all -> must be skipped, not crash

    r = StubRedis(store)
    assert FeatureService(r).index_functions("main", fids) is True
    assert store["main:features:pending_enrichment"] == {"h0"}


if __name__ == "__main__":
    test_batched_and_correct()
    test_missing_data_skipped()
    print("ok")
