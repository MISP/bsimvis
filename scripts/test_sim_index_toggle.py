"""Checks for cluster -> similarity index propagation.

Covers the skip toggle and the streaming candidate scan (each similarity must be
indexed exactly once, without materializing every sim id in memory).
"""

import json
from unittest.mock import MagicMock, patch

from bsimvis.app.services.cluster_service import ClusterService
from bsimvis.app.services.config_service import config_service

ALGO = "unweighted_cosine"
COL = "col"


def _run(enabled):
    svc = ClusterService.__new__(ClusterService)
    svc.r = MagicMock()
    svc.r.smembers.return_value = set()
    svc.r.scan.return_value = (0, [])
    svc.r.zcard.return_value = 0
    svc.get_propagated_fields = lambda lvl: {"func": []}
    svc.get_native_fields = lambda lvl, native: []
    with patch.object(
        config_service,
        "get",
        side_effect=lambda k, d=None: (
            enabled if k == "clustering.propagate_sim_indexes" else d
        ),
    ):
        assert svc._update_similarity_indexing(COL, ALGO) is True
    return svc.r


def test_toggle():
    # Disabled: no writes at all beyond the (skipped) build path.
    r_off = _run(False)
    assert not r_off.delete.called, "disabled propagation must not touch indexes"

    # Enabled: build path runs and wipes the best_cluster hash.
    r_on = _run(True)
    assert "col:sim:best_cluster:unweighted_cosine" in [
        c.args[0] for c in r_on.delete.call_args_list
    ], "enabled propagation must wipe stale best_cluster index"


class FakePipeline:
    """Queues reads/writes like redis-py; execute() replays them against FakeRedis."""

    def __init__(self, r):
        self.r = r
        self.queue = []

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.queue.append((name, args, kwargs))
            return self

        return call

    def execute(self):
        out = []
        for name, args, kwargs in self.queue:
            out.append(getattr(self.r, name)(*args, **kwargs))
        self.queue = []
        return out


class FakeRedis:
    def __init__(self, sets, strings):
        self.sets = {k: set(v) for k, v in sets.items()}
        self.strings = dict(strings)
        self.hashes = {}

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    def smembers(self, k):
        return set(self.sets.get(k, ()))

    def get(self, k):
        return self.strings.get(k)

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def sadd(self, k, *vals):
        self.sets.setdefault(k, set()).update(vals)

    def zadd(self, k, mapping):
        pass

    def hset(self, k, mapping=None):
        # Count writes per field so a duplicated scan is detectable.
        h = self.hashes.setdefault(k, {})
        for f, v in (mapping or {}).items():
            h[f] = h.get(f, 0) + 1 if False else v
            self.hset_calls.append(f)

    def delete(self, k):
        self.sets.pop(k, None)
        self.strings.pop(k, None)
        self.hashes.pop(k, None)

    def zcard(self, k):
        return 0

    def scan(self, cursor=0, match=None, count=None):
        return (0, [])


def test_streaming_scan_indexes_each_sim_once():
    """Three clustered functions, three pairs among them: three hset writes, no dupes."""
    funcs = ["md5:100", "md5:200", "md5:300"]
    pairs = [("md5:100", "md5:200"), ("md5:100", "md5:300"), ("md5:200", "md5:300")]
    sid = lambda a, b: f"{COL}:sim:{ALGO}:{a}::{b}"

    meta = {
        "cluster_id": 1,
        "cluster_uuid": "u-1",
        "cluster_name": "cl",
        "cohesion_score": 0.9,
        "cluster_stability": 0.5,
    }
    sets = {
        f"{COL}:cluster:list:{ALGO}": {"1"},
        f"{COL}:cluster:{ALGO}:1:members": {f"{COL}:func:{f}" for f in funcs},
    }
    for f in funcs:
        sets[f"{COL}:func:{f}:clusters"] = {"1"}
        # Both endpoints list the sim, as the real involves index does.
        sets[f"{COL}:sim:involves:func:{f}"] = {
            sid(a, b) for a, b in pairs if f in (a, b)
        }
    strings = {f"{COL}:cluster:{ALGO}:1:meta": json.dumps(meta)}

    r = FakeRedis(sets, strings)
    r.hset_calls = []

    svc = ClusterService.__new__(ClusterService)
    svc.r = r
    svc.get_propagated_fields = lambda lvl: {"func": [("cluster_name", "cluster_name")]}
    svc.get_native_fields = lambda lvl, native: []

    # Count candidates as they are considered: the write paths are sets/dicts, so a
    # sim scanned twice would silently collapse instead of failing the assertions.
    import bsimvis.app.services.cluster_service as cs

    considered = []
    real_pick = cs.pick_best_shared_cluster

    def counting_pick(cids1, cids2, meta_map):
        considered.append((tuple(cids1), tuple(cids2)))
        return real_pick(cids1, cids2, meta_map)

    with (
        patch.object(config_service, "get", side_effect=lambda k, d=None: d),
        patch.object(cs, "pick_best_shared_cluster", counting_pick),
    ):
        assert svc._update_similarity_indexing(COL, ALGO) is True

    assert len(considered) == len(pairs), (
        f"each sim must be scanned exactly once, got {len(considered)} "
        f"scans for {len(pairs)} sims"
    )

    expected = {sid(a, b) for a, b in pairs}
    assert (
        set(r.hset_calls) == expected
    ), f"missing sims: {expected ^ set(r.hset_calls)}"
    assert len(r.hset_calls) == len(expected), f"duplicate scan: {r.hset_calls}"

    # Tag buckets carry the same sims, under the cluster name.
    bucket = r.sets.get(f"{COL}:idx:sim:cluster_name:cl")
    assert bucket == expected, f"tag bucket mismatch: {bucket}"


if __name__ == "__main__":
    test_toggle()
    test_streaming_scan_indexes_each_sim_once()
    print("ok")
