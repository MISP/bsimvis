"""build_lca_snapshot's discovery cap must not drop candidates on ANY build of
bsimvis_similarity_native.

The snapshot used to pass top_k=0 meaning "no limit". Modules built before
d8e9de6 read that as truncate(0) and returned nothing, so _base_snapshot stayed
empty and build_batch wrote only same-vector-class pairs -- every similarity
exactly 1.0, never a 0.9-1.0 one. A venv that was not reinstalled after that
commit reproduces it silently, so the snapshot now passes v_total as the cap.

This pins the invariant that makes that safe: at top_k = len(vectors), every
candidate at or above min_score survives, on whatever module is installed.
"""

import sys


def main():
    try:
        import bsimvis_similarity_native as sn
    except ImportError:
        print("SKIP: bsimvis_similarity_native not installed")
        return 0

    # 0 and 1 are near-identical (cosine ~0.993), 2 shares nothing with them.
    vectors = [
        [("a", 1.0), ("b", 1.0), ("c", 1.0)],
        [("a", 1.0), ("b", 1.0), ("c", 1.0), ("d", 0.2)],
        [("x", 1.0)],
    ]
    scorer = sn.ExactScorer(vectors)
    indices = list(range(len(vectors)))
    cap = len(vectors)  # what build_lca_snapshot passes

    edges = scorer.select_target_block(indices, indices, "unweighted_cosine", 1, cap, 0.9)
    assert [len(t) for t in edges] == [1, 1, 0], edges
    assert 0.9 < edges[0][0][1] < 1.0, edges[0]

    if hasattr(scorer, "select_inverted_target_block"):
        inverted = [
            block
            for block, _count in scorer.select_inverted_target_block(
                indices, indices, "unweighted_cosine", 1, 1.0, cap, 0.9
            )
        ]
        assert inverted == edges, (inverted, edges)

    print(f"PASS: discovery cap {cap} keeps every >=0.9 candidate ({sn.__file__})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
