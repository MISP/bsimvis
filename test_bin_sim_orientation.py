"""A bin_sim pair is stored once, in one order; the API must serve it in the
order the caller asked for. Guards the side-flip used by get_bin_sim()."""

from bsimvis.app.routes.bin_sim import _flip_diff_sides


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


if __name__ == "__main__":
    test_flip_diff_sides()
    print("ok")
