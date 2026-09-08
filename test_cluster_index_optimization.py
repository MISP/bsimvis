"""Focused checks for sampled cohesion used by hierarchical clustering."""

import numpy as np

from bsimvis.app.services.cluster_service import _score_cohesion
from bsimvis.app.services.sim_edges import EdgeSet, SimAdjacency


def _adjacency():
    src = []
    dst = []
    dist = []
    for left in range(60):
        for right in range(left + 1, 60):
            src.append(left)
            dst.append(right)
            dist.append(0.1 + ((left + right) % 5) * 0.01)
    edges = EdgeSet(
        np.asarray(src, dtype=np.int32),
        np.asarray(dst, dtype=np.int32),
        np.asarray(dist, dtype=np.float32),
        {},
        {},
        len(src),
    )
    return SimAdjacency(edges, 60)


def test_sampled_cohesion_reuses_and_clears_scratch():
    adjacency = _adjacency()
    members = list(range(60))
    scratch = np.zeros(60, dtype=bool)

    exact_total, exact_count = adjacency.cohesion(members, scratch)
    sample_total, sample_count = adjacency.cohesion(
        members, scratch, np.arange(20, dtype=np.int32)
    )

    assert not scratch.any()
    assert sample_count > 0
    assert abs(sample_total / sample_count - exact_total / exact_count) < 0.01


class FakeAdjacency:
    def __init__(self, sampled, exact):
        self.values = [sampled, exact]
        self.calls = []

    def cohesion(self, members, scratch, sources=None):
        self.calls.append(sources)
        return self.values[len(self.calls) - 1]


def test_boundary_sample_is_rescored_exactly():
    adjacency = FakeAdjacency(sampled=(90.0, 100), exact=(80.0, 100))
    score = _score_cohesion(
        adjacency,
        list(range(501)),
        0.9,
        np.zeros(501, dtype=bool),
        np.random.default_rng(0),
    )

    assert score == 0.8
    assert len(adjacency.calls) == 2
    assert adjacency.calls[0] is not None and adjacency.calls[1] is None


if __name__ == "__main__":
    test_sampled_cohesion_reuses_and_clears_scratch()
    test_boundary_sample_is_rescored_exactly()
