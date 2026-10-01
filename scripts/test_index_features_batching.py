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

    def hincrby(self, key, field, amount):
        def apply():
            h = self.store.setdefault(key, {})
            h[field] = int(h.get(field, 0)) + amount

        self._w(apply)

    def hsetnx(self, key, field, value):
        def apply():
            h = self.store.setdefault(key, {})
            h.setdefault(field, value)

        self._w(apply)

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

    def incr(self, key, amount=1):
        self.store[key] = int(self.store.get(key, 0)) + amount
        return self.store[key]

    def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)


def test_batched_and_correct():
    """Realistic shape: many functions sharing a small pool of feature hashes."""
    n, per_func, pool = 250, 40, 60
    store = {}
    fids = [f"main:function:abc:{i}" for i in range(n)]
    hashes = {}  # fid -> {f_hash: tf}
    meta_features = {}  # f_hash -> [(type, pcode_op), ...] first-occurrence order
    for k, fid in enumerate(fids):
        hs = {f"h{(k + j) % pool}": float(j + 1) for j in range(per_func)}
        hashes[fid] = hs
        # Each occurrence has a type/pcode_op so the stats tally exercises
        # _pair_key and the HSETNX first-rep semantics.
        feats = [
            {
                "hash": h,
                "tf": tf,
                "type": f"T{i % 3}",
                "pcode_op": f"OP{i % 4}",
                "line_idx": [1, 2],
                "seq": f"seq{k}:{i}",
                "function_name": f"fn{k}",
                "pcode_op_full": f"full{k}:{i}",
            }
            for i, (h, tf) in enumerate(hs.items())
        ]
        for f in feats:
            meta_features.setdefault(f["hash"], []).append((f["type"], f["pcode_op"]))
        store[f"{fid}:vec:meta"] = json.dumps(feats)
        store[f"{fid}:vec:tf"] = list(hs.items())

    r = StubRedis(store)
    assert FeatureService(r).index_functions("main", fids) is True

    # --- stats table: pair-count fields + one rep per pair ---
    for h in set(fh for hs in hashes.values() for fh in hs):
        stats_key = f"main:feature:{h}:stats"
        assert stats_key in store, f"missing {stats_key}"
        stats = store[stats_key]
        # At least 1 pair-count and 1 rep per feature
        count_fields = [k for k in stats if not k.startswith("occ\u241f")]
        rep_fields = [k for k in stats if k.startswith("occ\u241f")]
        assert len(count_fields) >= 1
        assert len(rep_fields) >= 1
        # Reps are JSON objects
        for k in rep_fields:
            rep = json.loads(stats[k])
            assert "function_id" in rep

    # No per-function blocking reads outside the pipeline.
    assert r.stats["direct"] == 0, r.stats

    # Naive fan-out would be n*per_func*3 = 30000 commands.
    # The stats path (~2 HINCRBY+HSETNX per pair) adds a modest per-feature
    # overhead; both are bounded by the pool size, not the function count.
    assert r.stats["cmds"] < 8000, r.stats

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
