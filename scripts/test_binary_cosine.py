"""Check the two cosine algos stay distinct and self-consistent.

`unweighted_cosine` is cosine over the raw TF vectors (no IDF weighting);
`binary_cosine` is cosine over the feature sets. For each, the discovery score
must equal the exact score, in both directions. Runs offline against a stub
redis — no kvrocks needed:

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

    def get(self, key):
        self.ops.append(("get", key))

    def zrange(self, key, start, end, withscores=False):
        self.ops.append(("zrange", key))

    def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "zcard":
                out.append(len(self.db.get(op[1], {})))
            elif op[0] == "zscore":
                out.append(self.db.get(op[1], {}).get(op[2]))
            elif op[0] == "get":
                out.append(self.db.get(op[1]))
            else:
                out.append(list(self.db.get(op[1], {}).items()))
        self.ops = []
        return out


class StubRedis:
    def __init__(self, db):
        self.db = db

    def zrange(self, key, start, end, withscores=False):
        return list(self.db.get(key, {}).items())

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


def build_db(vectors, collection="c"):
    """{func_id: {feature: tf}} -> the redis keys discovery and scoring read."""
    db = {}
    features = {f for vec in vectors.values() for f in vec}
    for feature in features:
        db[f"{collection}:feature:{feature}:functions"] = {
            fid: tf for fid, vec in vectors.items() if (tf := vec.get(feature))
        }
    db[f"{collection}:idx:func:bsim_features_count"] = {
        fid: float(sum(vec.values())) for fid, vec in vectors.items()
    }
    for fid, vec in vectors.items():
        db[f"{fid}:vec:tf"] = dict(vec)
        db[f"{fid}:vec:norm"] = math.sqrt(sum(v * v for v in vec.values()))
    return db


def discover(svc, target, vectors, algo, collection="c", threshold="0.1"):
    vec = vectors[target]
    args = [
        target,
        collection,
        algo,
        threshold,
        str(sum(vec.values())),
        str(math.sqrt(sum(v * v for v in vec.values()))),
        "10",
        "0",
    ]
    for feature, tf in vec.items():
        args += [feature, str(tf)]
    raw = svc._discover_find(args)
    return {raw[i]: float(raw[i + 1]) for i in range(0, len(raw), 3)}


# X repeats feature A; Y does not. Same feature *set*, different frequencies.
VECTORS = {"c:func:X": {"A": 2.0, "B": 1.0}, "c:func:Y": {"A": 1.0, "B": 1.0}}
X, Y = "c:func:X", "c:func:Y"


def test_algos_disagree_on_repeats():
    db = build_db(VECTORS)
    tf_score = make_service(db).calculate_exact_score(X, Y, "unweighted_cosine")
    binary_score = make_service(db).calculate_exact_score(X, Y, "binary_cosine")
    # 3 / (sqrt(5) * sqrt(2)) — repeats count, and count linearly.
    assert abs(tf_score - 3 / math.sqrt(10)) < 1e-9, tf_score
    # Feature sets are identical, so binary cannot tell the two apart.
    assert abs(binary_score - 1.0) < 1e-9, binary_score


def test_discovery_matches_exact_both_directions():
    for algo in ("unweighted_cosine", "binary_cosine"):
        db = build_db(VECTORS)
        exact = make_service(db).calculate_exact_score(X, Y, algo)
        for target, other in ((X, Y), (Y, X)):
            found = discover(make_service(db), target, VECTORS, algo)
            assert other in found, (algo, target, found)
            assert abs(found[other] - exact) < 1e-9, (algo, target, found[other], exact)


def test_binary_norm_uses_unique_features():
    # Z shares 2 of X's 2 unique features but repeats a third one 8 times, so its
    # raw bsim_features_count (11) is far above its unique count (3).
    vectors = dict(VECTORS, **{"c:func:Z": {"A": 1.0, "B": 1.0, "D": 8.0}})
    db = build_db(vectors)
    found = discover(make_service(db), X, vectors, "binary_cosine")
    expected = 2 / math.sqrt(2 * 3)
    assert abs(found["c:func:Z"] - expected) < 1e-9, found
    # The raw count would give 2/sqrt(2*11) — the deflation this guards against.
    assert abs(found["c:func:Z"] - 2 / math.sqrt(2 * 11)) > 0.1


def test_threshold_prunes():
    vectors = {"c:func:T": {"A": 1.0, "B": 1.0, "C": 1.0}, "c:func:P": {"A": 1.0}}
    db = build_db(vectors)
    # binary score = 1/sqrt(3*1) = 0.577 — above 0.5, below 0.8.
    assert discover(
        make_service(db), "c:func:T", vectors, "binary_cosine", threshold="0.5"
    )
    assert not discover(
        make_service(build_db(vectors)),
        "c:func:T",
        vectors,
        "binary_cosine",
        threshold="0.8",
    )


if __name__ == "__main__":
    test_algos_disagree_on_repeats()
    test_discovery_matches_exact_both_directions()
    test_binary_norm_uses_unique_features()
    test_threshold_prunes()
    print("PASS")
