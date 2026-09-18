"""Check the bin_sim discovery sweep against the dense one it replaced.

`discover_edges` drives off a posting list over the partner's feature hashes
instead of walking every A/B pair. That is only a speedup if it returns the
same edges, so this reimplements the dense sweep inline and compares. Runs
offline -- no kvrocks needed:

    uv run python scripts/test_bin_sim_discovery.py
"""

import random

from bsimvis.app.services.bin_sim_service import discover_edges
from bsimvis.app.services.bin_sim_tags import greedy_match


def dense_edges(vectors, fids_a, fids_b, algo, min_score, skip_pairs=()):
    """The O(A x B) loop cached_pair_from_stored_sims used to run."""
    norms = {
        fid: sum(v * v for v in vec.values()) ** 0.5 for fid, vec in vectors.items()
    }
    edges = []
    for fid_a in sorted(fids_a):
        vec_a, norm_a = vectors.get(fid_a), norms.get(fid_a, 0)
        if not vec_a or not norm_a:
            continue
        for fid_b in sorted(fids_b):
            if frozenset((fid_a, fid_b)) in skip_pairs:
                continue
            vec_b, norm_b = vectors.get(fid_b), norms.get(fid_b, 0)
            if not vec_b or not norm_b:
                continue
            common = set(vec_a) & set(vec_b)
            if algo == "jaccard":
                shared = sum(min(vec_a[k], vec_b[k]) for k in common)
                score = shared / (sum(vec_a.values()) + sum(vec_b.values()) - shared)
            elif algo == "binary_cosine":
                score = len(common) / (len(vec_a) * len(vec_b)) ** 0.5
            else:
                score = sum(vec_a[k] * vec_b[k] for k in common) / (norm_a * norm_b)
            if score >= min_score and score > 0:
                edges.append((fid_a, fid_b, score))
    return edges


def make_corpus(seed, n_a=25, n_b=30, vocab=40):
    rng = random.Random(seed)
    fids_a = [f"a:{i}" for i in range(n_a)]
    fids_b = [f"b:{i}" for i in range(n_b)]
    vectors = {}
    for fid in fids_a + fids_b:
        size = rng.randint(1, 8)
        vectors[fid] = {
            f"h{rng.randrange(vocab)}": float(rng.randint(1, 3)) for _ in range(size)
        }
    # A function with no features at all: both sweeps must skip it.
    vectors["a:empty"] = {}
    fids_a.append("a:empty")
    return vectors, set(fids_a), set(fids_b)


def test_matches_dense_sweep():
    for algo in ("unweighted_cosine", "binary_cosine", "jaccard", "weighted_cosine"):
        for seed in range(5):
            vectors, fids_a, fids_b = make_corpus(seed)
            for min_score in (0.0, 0.3, 0.9):
                got = sorted(discover_edges(vectors, fids_a, fids_b, algo, min_score))
                want = sorted(dense_edges(vectors, fids_a, fids_b, algo, min_score))
                assert len(got) == len(want), (
                    algo,
                    seed,
                    min_score,
                    len(got),
                    len(want),
                )
                for (ga, gb, gs), (wa, wb, ws) in zip(got, want):
                    assert (ga, gb) == (wa, wb), (algo, seed, ga, gb, wa, wb)
                    assert abs(gs - ws) < 1e-9, (algo, seed, ga, gb, gs, ws)


def test_skip_pairs_respected():
    vectors, fids_a, fids_b = make_corpus(1)
    full = discover_edges(vectors, fids_a, fids_b, "unweighted_cosine", 0.0)
    assert full, "corpus produced no edges to skip"
    skip = {frozenset(full[0][:2])}
    kept = discover_edges(
        vectors, fids_a, fids_b, "unweighted_cosine", 0.0, skip_pairs=skip
    )
    assert len(kept) == len(full) - 1
    assert frozenset(full[0][:2]) not in {frozenset(e[:2]) for e in kept}


def test_max_df_drops_only_common_features():
    # "h_all" sits in every b function, "h_rare" in one. At max_df=0.5 the
    # common feature stops contributing; the rare one still does.
    vectors = {
        "a:0": {"h_all": 1.0, "h_rare": 1.0},
        "b:0": {"h_all": 1.0, "h_rare": 1.0},
        "b:1": {"h_all": 1.0},
        "b:2": {"h_all": 1.0},
    }
    fids_a, fids_b = {"a:0"}, {"b:0", "b:1", "b:2"}
    exact = {
        b: s
        for _, b, s in discover_edges(
            vectors, fids_a, fids_b, "unweighted_cosine", 0.0
        )
    }
    assert set(exact) == {"b:0", "b:1", "b:2"}
    capped = {
        b: s
        for _, b, s in discover_edges(
            vectors, fids_a, fids_b, "unweighted_cosine", 0.0, max_df=0.5
        )
    }
    assert set(capped) == {"b:0"}, capped
    assert abs(capped["b:0"] - 1 / (2 * 2) ** 0.5) < 1e-9


def test_discovery_only_fills_unmatched():
    # What build_bin_sim does: greedy over stored edges, then discovery over
    # the leftovers only. A function a stored edge already placed must not pick
    # up a second, higher-scoring candidate.
    vectors = {
        "a:0": {"h0": 1.0},
        "a:1": {"h0": 1.0},
        "b:0": {"h0": 1.0},
        "b:1": {"h0": 1.0},
    }
    stored = [("a:0", "b:0", 0.95)]
    _, matched_a, matched_b = greedy_match(stored)
    assert matched_a == {"a:0"} and matched_b == {"b:0"}
    found = discover_edges(
        vectors,
        {"a:0", "a:1"} - matched_a,
        {"b:0", "b:1"} - matched_b,
        "unweighted_cosine",
        0.5,
    )
    assert found == [("a:1", "b:1", 1.0)], found


if __name__ == "__main__":
    test_matches_dense_sweep()
    test_skip_pairs_respected()
    test_max_df_drops_only_common_features()
    test_discovery_only_fills_unmatched()
    print("PASS")
