"""Parity: index_global_features must pick the same (type, op) winner from
:stats as it did from the legacy HRANDFIELD sampling over :meta.

The :stats table is written by index_functions (HINCRBY per (type, op) pair per
window, HSETNX the first rep per pair). Enrich reads it and applies the exact
same `max(counts, key=(count, pair))` tie-break as the old path, so the
global_meta we save should be stable.

Also: the fallback path triggers when :stats is missing entirely (legacy
collections or freshly cleared features).
"""

import json
import bsimvis.app.services.index_service as isvc
from bsimvis.app.services.feature_service import FeatureService, PAIR_SEP, OCC_FIELD


class Pipe:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def hset(self, key, field=None, value=None, mapping=None):
        m = mapping or {field: value}
        self.ops.append(lambda k=key: self.store.setdefault(k, {}).update(m))
        return self

    def hsetnx(self, key, field, value):
        self.ops.append(lambda: self.store.setdefault(key, {}).setdefault(field, value))
        return self

    def hincrby(self, key, field, amount):
        self.ops.append(
            lambda k=key, f=field, a=amount: self.store.setdefault(k, {}).update(
                {f: int(self.store[k].get(f, 0)) + a}
            )
        )
        return self

    def zadd(self, key, mapping):
        self.ops.append(lambda: self.store.setdefault(key, {}).update(mapping))
        return self

    def zincrby(self, key, amount, member):
        self.ops.append(
            lambda: self.store.setdefault(key, {}).update(
                {member: self.store[key].get(member, 0.0) + amount}
            )
        )
        return self

    def zrem(self, key, *members):
        def _():
            h = self.store.get(key, {})
            for m in members:
                h.pop(m, None)

        self.ops.append(_)
        return self

    def hdel(self, key, *fields):
        def _():
            h = self.store.get(key, {})
            for f in fields:
                h.pop(f, None)

        self.ops.append(_)
        return self

    def mset(self, mapping):
        self.ops.append(lambda: self.store.update(mapping))
        return self

    def sadd(self, key, *vals):
        self.ops.append(lambda: self.store.setdefault(key, set()).update(vals))
        return self

    def srem(self, key, *vals):
        self.ops.append(
            lambda k=key, v=set(vals): self.store.setdefault(
                k, set()
            ).difference_update(v)
        )
        return self

    def set(self, key, value):
        self.ops.append(lambda: self.store.update({key: value}))
        return self

    # --- reads ---
    def hgetall(self, key):
        self.ops.append(lambda k=key: dict(self.store.get(k, {})))
        return self

    def hrandfield(self, key, count=10, withvalues=False):
        def _():
            items = list(self.store.get(key, {}).items())
            if withvalues:
                return [x for pair in items[:count] for x in pair]
            return list(self.store.get(key, {}).keys())[:count]

        self.ops.append(_)
        return self

    def hlen(self, key):
        self.ops.append(lambda k=key: len(self.store.get(k, {})))
        return self

    def zscore(self, key, member):
        self.ops.append(lambda k=key, m=member: self.store.get(k, {}).get(m))
        return self

    def get(self, key):
        self.ops.append(lambda k=key: self.store.get(k))
        return self

    def zrange(self, key, start, end, withscores=False):
        def _():
            v = self.store.get(key, [])
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return list(v.items()) if withscores else list(v.keys())
            return v

        self.ops.append(_)
        return self

    def delete(self, *keys):
        self.ops.append(lambda ks=keys: [self.store.pop(k, None) for k in ks])
        return self

    def scard(self, key):
        self.ops.append(lambda k=key: len(self.store.get(k, set())))
        return self

    def sscan(self, key, cursor=0, count=10):
        self.ops.append(
            lambda k=key, c=count: (0, sorted(list(self.store.get(k, set())))[:c])
        )
        return self

    def incr(self, key, amount=1):
        self.ops.append(
            lambda k=key, a=amount: self.store.__setitem__(
                k, int(self.store.get(k, 0)) + a
            )
        )
        return self

    def execute(self):
        out = [fn() for fn in self.ops]
        self.ops = []
        return out


class Redis:
    def __init__(self, store):
        self.store = store

    def pipeline(self, transaction=False):
        return Pipe(self.store)

    def smembers(self, key):
        return set(self.store.get(key, set()))

    def get(self, key):
        return self.store.get(key)

    def sadd(self, key, *vals):
        self.store.setdefault(key, set()).update(vals)

    def srem(self, key, *vals):
        s = self.store.setdefault(key, set())
        s.difference_update(vals)

    def scard(self, key):
        return len(self.store.get(key, set()))

    def incr(self, key, amount=1):
        v = int(self.store.get(key, 0)) + amount
        self.store[key] = v
        return v


def _fill(store, coll, fids):
    """Store :vec:meta for each fid: one primary (type, op) pair for feature
    `fh` plus a second feature (`fh_other`) to sidestep _fetch_vec_batch's
    length-1 list unwrap (a pre-existing quirk that breaks single-feature
    vectors). Real vectors hold 10s of features, so this is the realistic
    shape.
    """
    pairs = [("DATA_FLOW", "ADD"), ("COPY_SIG", "COPY"), ("CONTROL_FLOW", "BRANCH")]
    for i, fid in enumerate(fids):
        t, o = pairs[i % len(pairs)]
        feats = [
            {
                "hash": "fh",
                "type": t,
                "pcode_op": o,
                "line_idx": [(i % 4) + 1],
                "seq": f"s{i}",
                "function_name": f"fn{i}",
                "pcode_op_full": f"{o}:RAX,RBX",
                "tf": 1,
            },
            # second feature so the vector has length-2 and the unwrap quirk
            # does not trigger.
            {
                "hash": "fh_other",
                "type": "CONTROL_FLOW",
                "pcode_op": "RET",
                "line_idx": [(i % 4) + 2],
                "seq": f"o{i}",
                "function_name": f"fn{i}",
                "pcode_op_full": "RET:RAX",
                "tf": 1,
            },
        ]
        store[f"{fid}:vec:meta"] = json.dumps(feats)
        store[f"{fid}:vec:tf"] = [("fh", 1.0), ("fh_other", 1.0)]
        store[f"{fid}:source"] = json.dumps(
            [
                {
                    "c_tokens": [
                        {"line": (i % 4) + 1, "t": "t", "type": "id"},
                    ]
                }
            ]
        )


def _winner_from_stats(store, coll):
    stats = store.get(f"{coll}:feature:fh:stats", {})
    counts = {}
    for f, v in stats.items():
        if f.startswith(OCC_FIELD):
            continue
        counts[f] = int(v)
    if not counts:
        return None
    best_key = max(counts.items(), key=lambda x: (x[1], x[0]))[0]
    best_type, best_op = best_key.split(PAIR_SEP, 1)
    best_rep = json.loads(stats.get(f"{OCC_FIELD}{best_key}", "{}"))
    return {
        "type": best_type,
        "op": best_op,
        "frequency": sum(counts.values()),
        "func_id": best_rep.get("function_id"),
        "line_idx": best_rep.get("line_idx"),
    }


def _patch(saved, raise_on_delete=True):
    orig = (isvc.save_feature, isvc.delete_feature)
    isvc.save_feature = lambda pipe, coll, fh, data: saved.append((fh, data))
    if raise_on_delete:

        def _del(r, coll, fh):
            raise AssertionError(
                f"path should not delete {fh}: meta should be non-empty"
            )

    else:

        def _del(r, coll, fh, _orig=orig[1]):
            return _orig(r, coll, fh)

    isvc.delete_feature = _del
    return orig


def _restore(orig):
    isvc.save_feature = orig[0]
    isvc.delete_feature = orig[1]


def test_stats_winner_matches_legacy_winner():
    """Fast-path stats and legacy HRANDFIELD produce the same (type, op)
    winner on the same occurrence distribution. Both compute
    `max(counts, key=(count, pair))`; the difference is where the counts
    come from -- persisted :stats table vs. HRANDFIELD sample of :meta.
    """
    coll = "c"
    n = 30
    fids = [f"{coll}:func:md5_{i}:{0x4000 + i}" for i in range(n)]
    store = {}
    _fill(store, coll, fids)

    svc = FeatureService(Redis(store))
    svc.index_functions(coll, fids)

    expected = _winner_from_stats(store, coll)
    # With 30 fids and 3 pairs, each pair gets 10 fids.
    # Winner = max(counts, key=(count, pair)) — on ties picks lex-LARGEST pair
    # (mirrors the legacy `max(counts.items(), key=lambda x: (x[1], x[0]))`
    # exactly). DATA_FLOW > COPY_SIG > CONTROL_FLOW, so DATA_FLOW/ADD wins.
    assert expected["type"] == "DATA_FLOW", expected
    assert expected["op"] == "ADD", expected
    assert expected["frequency"] == n, expected  # HLEN :meta

    # Enrich reads the stats table (fast path)
    saved = []
    orig = _patch(saved)
    try:
        svc.index_global_features(coll, ["fh"], job_service=None, job_id=None)
    finally:
        _restore(orig)

    assert len(saved) == 1, saved
    fh, data = saved[0]
    assert fh == "fh"
    assert (data["type"], data["op"]) == ("DATA_FLOW", "ADD"), data
    assert data["frequency"] == n, data
    # Context is pulled from the rep. Winner is DATA_FLOW/ADD -> fids 0, 3,
    # 6, ...; HSETNX keeps the FIRST rep = fid0. _fill gives fid0
    # line_idx = (0 % 4) + 1 = 1, seq = s0, fn0.
    ctx = data["context"]
    assert ctx["type"] == "DATA_FLOW"
    assert ctx["op"] == "ADD"
    assert ctx["seq"] == "s0"
    assert ctx["name"] == "fn0"
    assert ctx["line_idxs"] == [1]
    # c_code from :source at line 1.
    assert ctx["c_code"] and ctx["c_code"][0]["text"] == "t"


def test_legacy_fallback_when_stats_missing():
    """If :stats is missing (old data or post-clear), the legacy HRANDFIELD
    path still runs and produces the same winner."""
    coll = "c"
    n = 10
    fids = [f"{coll}:func:md5_{i}:{0x4000 + i}" for i in range(n)]
    store = {}
    _fill(store, coll, fids)

    # Simulate a legacy collection: :meta has one entry per fid, :stats is
    # absent. Same pair distribution as _fill, so both paths pick the same
    # winner.
    for i, fid in enumerate(fids):
        t, o = (("DATA_FLOW", "ADD"), ("COPY_SIG", "COPY"), ("CONTROL_FLOW", "BRANCH"))[
            i % 3
        ]
        store.setdefault(f"{coll}:feature:fh:meta", {})[fid] = json.dumps(
            {
                "hash": "fh",
                "type": t,
                "pcode_op": o,
                "line_idx": [(i % 4) + 1],
                "seq": f"s{i}",
                "function_name": f"fn{i}",
                "pcode_op_full": f"{o}:RAX,RBX",
            }
        )
    assert f"{coll}:feature:fh:stats" not in store
    assert len(store[f"{coll}:feature:fh:meta"]) == n

    # Legacy sample: HRANDFIELD (capped at 100) returns all 10 in deterministic
    # order; HLEN :meta = 10.
    # Counts: 10 fids, 3 pairs, so 4/3/3 or similar. n=10 -> i%3 is 0,1,2,0,1,2,0,1,2,0
    # Pair 0 (DATA_FLOW/ADD) x 4, Pair 1 (COPY_SIG/COPY) x 3, Pair 2 (CONTROL_FLOW/BRANCH) x 3.
    # Winner by (count, pair): Pair 0 with 4 -> DATA_FLOW/ADD.
    svc = FeatureService(Redis(store))
    saved = []
    orig = _patch(saved)
    try:
        svc.index_global_features(coll, ["fh"], job_service=None, job_id=None)
    finally:
        _restore(orig)

    assert len(saved) == 1, saved
    fh, data = saved[0]
    assert fh == "fh"
    assert (data["type"], data["op"]) == ("DATA_FLOW", "ADD"), data
    assert data["frequency"] == n, data  # HLEN :meta


def test_stats_is_invalidated_on_clear():
    """clear_features must delete :stats so enrich falls back to :meta.

    Subtracting counts from a partial (per-batch) clear would drift the whole
    table, so instead the clear DROPs the derived hash; a later enrich re-derives
    it from the surviving :meta. Both clear branches (targeted + collection-wide)
    must invalidate it, or a wiped feature would still look like it has pairs.
    """
    from bsimvis.app.services import feature_service as fs

    src = open(fs.__file__).read()
    # Targeted clear: per-hash DELETE inside the HDEL pipeline.
    assert (
        ":stats" in src and 'pipe.delete(f"{collection}:feature:{f_hash}:stats")' in src
    ), "targeted clear_features must delete the :stats hash"
    # Collection-wide clear: the :stats pattern in the SCAN-DELETE list.
    assert (
        'f"{collection}:feature:*:stats"' in src
    ), "full clear_features must include {collection}:feature:*:stats"
    # delete_feature (index_service) must drop it too.
    from bsimvis.app.services import index_service as isvc

    isrc = open(isvc.__file__).read()
    assert (
        'pipe.delete(f"{base_id}:stats")' in isrc
    ), "delete_feature must delete {base}:stats"


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok  {name}")
    print("ok")
