"""A bin_sim pair is stored once, in one order; the API must serve it in the
order the caller asked for. Guards the side-flip used by get_bin_sim()."""

from bsimvis.app.routes.bin_sim import _flip_diff_sides
from bsimvis.app.services.bin_sim_service import BinSimService


def test_flip_diff_sides():
    doc = {
        "md5_a": "x",
        "md5_b": "y",
        "coverage_a": 1,
        "coverage_b": 2,
        "score": 9,
        "diff": {
            "unique_to_a": ["ua"],
            "unique_to_b": ["ub1", "ub2"],
            "matched": [{"func_a": "fa", "func_b": "fb", "similarity": 0.5}],
        },
    }

    _flip_diff_sides(doc)
    assert (doc["md5_a"], doc["md5_b"]) == ("y", "x")
    assert (doc["coverage_a"], doc["coverage_b"]) == (2, 1)
    assert doc["score"] == 9
    assert doc["diff"]["unique_to_a"] == ["ub1", "ub2"]
    assert doc["diff"]["unique_to_b"] == ["ua"]
    assert doc["diff"]["matched"][0] == {
        "func_a": "fb",
        "func_b": "fa",
        "similarity": 0.5,
    }

    _flip_diff_sides(doc)
    assert doc["md5_a"] == "x"
    assert doc["diff"]["unique_to_b"] == ["ub1", "ub2"]


def test_flip_tags_summary():
    """The per-tag split carries its own sided fields; they must flip too, or a
    pool pair served in reverse order reports libc's coverage against the wrong
    binary. `bins` is positional, so the generic key swap cannot see it."""
    doc = {
        "md5_a": "x",
        "md5_b": "y",
        "tags_summary": [
            {
                "tag_id": "lib:libc:2.31",
                "weight_a": 40.0,
                "weight_b": 10.0,
                "unique_weight_a": 5.0,
                "unique_weight_b": 0.0,
                "unique_count_a": 1.0,
                "unique_count_b": 0.0,
                "coverage_pct_a": 90.0,
                "coverage_pct_b": 20.0,
                "contribution_pct": 25.0,
                "bins": {"16": [3.0, 40.0, 2.0, 10.0]},
                "children": [
                    {
                        "tag_id": "lib:libc:2.31:memcpy",
                        "weight_a": 40.0,
                        "weight_b": 10.0,
                        "bins": {"16": [3.0, 40.0, 2.0, 10.0]},
                    },
                ],
            }
        ],
    }

    _flip_diff_sides(doc)
    t = doc["tags_summary"][0]
    assert (t["weight_a"], t["weight_b"]) == (10.0, 40.0)
    assert (t["coverage_pct_a"], t["coverage_pct_b"]) == (20.0, 90.0)
    assert (t["unique_weight_a"], t["unique_weight_b"]) == (0.0, 5.0)
    assert (t["unique_count_a"], t["unique_count_b"]) == (0.0, 1.0)
    assert t["contribution_pct"] == 25.0, "side-agnostic fields must not move"
    assert t["bins"]["16"] == [2.0, 10.0, 3.0, 40.0], "both count and weight swap"
    assert t["children"][0]["bins"]["16"] == [2.0, 10.0, 3.0, 40.0]

    _flip_diff_sides(doc)
    assert doc["tags_summary"][0]["weight_a"] == 40.0
    assert doc["tags_summary"][0]["bins"]["16"] == [3.0, 40.0, 2.0, 10.0]


class _FakeRedis:
    """Only what find_pair_sid touches: a pipeline of smembers."""

    def __init__(self, data):
        self.data, self.queued = data, []

    def pipeline(self, transaction=False):
        return self

    def smembers(self, key):
        self.queued.append(key)
        return self.data.get(key, set())

    def execute(self):
        out = [self.data.get(k, set()) for k in self.queued]
        self.queued = []
        return out


def test_find_pair_sid_pool_namespaced_collection():
    """A pooled request arrives with `collection` rewritten to
    global:pool:{id}:col:{name}; the involves index is keyed by the origin
    name, so the lookup must map it back or every pooled pair reads as
    "Similarity not calculated for this pair"."""
    sid = "global:pool:7:bin_sim:unweighted_cosine:mirai:aa::rondo:bb"
    svc = BinSimService(
        _FakeRedis(
            {
                "global:pool:7:bin_sim:involves:mirai:aa": {sid},
                "global:pool:7:bin_sim:involves:rondo:bb": {sid},
            }
        )
    )

    assert svc.find_pair_sid("mirai", "aa", "bb", "rondo", "7") == sid
    assert (
        svc.find_pair_sid("global:pool:7:col:mirai", "aa", "bb", "rondo", "7") == sid
    )
    # both sides can arrive namespaced, and a wrong collection still misses
    assert (
        svc.find_pair_sid(
            "global:pool:7:col:mirai", "aa", "bb", "global:pool:7:col:rondo", "7"
        )
        == sid
    )
    assert svc.find_pair_sid("nope", "aa", "bb", "rondo", "7") is None


if __name__ == "__main__":
    test_flip_diff_sides()
    test_flip_tags_summary()
    test_find_pair_sid_pool_namespaced_collection()
    print("ok")
