"""Check that unweighted_cosine discovery scores true binary cosine.

The candidate norm must be sqrt(unique feature count), not sqrt of the raw
`bsim_features_count` index (which counts repeated features). Runs offline
against a stub redis — no kvrocks needed:

    uv run python scripts/test_binary_cosine.py
"""

import math

from bsimvis.app.services.similarity_service import SimilarityService


class StubPipeline:
    def __init__(self, db):
        self.db = db
        self.ops = []

    def zcard(self, key):
        self.ops.append(("zcard", key))

    def zscore(self, key, member):
        self.ops.append(("zscore", key, member))

    def zrange(self, key, start, end, withscores=False):
        self.ops.append(("zrange", key))

    def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "zcard":
                out.append(len(self.db.get(op[1], {})))
            elif op[0] == "zscore":
                out.append(self.db.get(op[1], {}).get(op[2]))
            else:
                out.append(list(self.db.get(op[1], {}).items()))
        self.ops = []
        return out


class StubRedis:
    def __init__(self, db):
        self.db = db

    def pipeline(self, transaction=False):
        return StubPipeline(self.db)


def make_service(db):
    from collections import OrderedDict

    svc = SimilarityService.__new__(SimilarityService)
    svc.r = StubRedis(db)
    svc._pl_cache = OrderedDict()
    svc._pl_pairs = 0
    svc._pl_budget = 5_000_000
    svc._norm_cache = {}
    svc._count_cache = {}
    svc._unique_count_cache = {}
    return svc


def test_candidate_norm_uses_unique_features():
    coll = "c"
    cand = "c:func:cand"
    target = "c:func:target"
    # Target has 3 unique features (a, b, c); candidate shares a and b and has
    # one extra (d) — 3 unique features, but 10 raw ones (features repeat).
    db = {
        f"{coll}:feature:a:functions": {target: 1.0, cand: 4.0},
        f"{coll}:feature:b:functions": {target: 1.0, cand: 3.0},
        f"{coll}:feature:c:functions": {target: 1.0},
        f"{coll}:feature:d:functions": {cand: 3.0},
        f"{coll}:idx:func:bsim_features_count": {cand: 10.0},
        f"{cand}:vec:tf": {"a": 4.0, "b": 3.0, "d": 3.0},
    }
    svc = make_service(db)
    # ARGV layout: target, collection, algo, threshold, total, norm, limit,
    # min_features, then feature/tf pairs.
    args = [
        target,
        coll,
        "unweighted_cosine",
        "0.1",
        "3",
        "1.7320508",
        "10",
        "0",
        "a",
        "1",
        "b",
        "1",
        "c",
        "1",
    ]
    result = svc._discover_find(args)
    assert result[0] == cand, result
    score = float(result[1])
    expected = 2 / math.sqrt(3 * 3)  # intersection 2, both sides 3 unique
    assert abs(score - expected) < 1e-9, f"{score} != {expected}"
    # The raw-count norm is the bug this guards against.
    assert abs(score - 2 / math.sqrt(3 * 10)) > 0.1


def test_threshold_prunes():
    coll = "c"
    cand = "c:func:cand"
    target = "c:func:target"
    db = {
        f"{coll}:feature:a:functions": {target: 1.0, cand: 1.0},
        f"{coll}:feature:b:functions": {target: 1.0},
        f"{coll}:feature:c:functions": {target: 1.0},
        f"{coll}:idx:func:bsim_features_count": {cand: 1.0},
        f"{cand}:vec:tf": {"a": 1.0},
    }
    svc = make_service(db)
    # score = 1/sqrt(3*1) = 0.577 — above 0.5, below 0.8.
    base = [target, coll, "unweighted_cosine"]
    tail = ["3", "1.73", "10", "0", "a", "1", "b", "1", "c", "1"]
    assert svc._discover_find(base + ["0.5"] + tail)
    assert svc._discover_find(base + ["0.8"] + tail) == []


if __name__ == "__main__":
    test_candidate_norm_uses_unique_features()
    test_threshold_prunes()
    print("PASS")
