#!/usr/bin/env python3
"""Self-check for the per-tag similarity split (bsimvis/app/services/bin_sim_tags.py).

No redis, no fixtures: TagSplit is pure arithmetic over a fid -> tags map.
Run: python3 scripts/test_bin_sim_tag_split.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.bin_sim_tags import (  # noqa: E402
    TAG_MISMATCH,
    TAG_UNTAGGED,
    TagSplit,
    normalize_tags,
    parse_tag_id,
    tag_parent,
)


def by_id(rows):
    return {r["tag_id"]: r for r in rows}


def test_normalize_and_parse():
    assert normalize_tags(["lib:libc:2.31"]) == {"lib:libc:2.31": 1.0}
    assert normalize_tags({"lib:libc": 0.5}) == {"lib:libc": 0.5}
    assert normalize_tags(None) == {}
    assert normalize_tags("nonsense") == {}

    # Matching keeps the function name; only display rolls up.
    assert tag_parent("lib:libc:2.31:memcpy") == "lib:libc:2.31"
    assert tag_parent("lib:libc:2.31") == "lib:libc:2.31"
    assert tag_parent(TAG_UNTAGGED) == TAG_UNTAGGED
    assert parse_tag_id("lib:libc:2.31:memcpy") == ("lib", "libc", "2.31")
    assert parse_tag_id("mytag") == ("user", "mytag", "")


def test_shared_tag_split():
    fid_tags = {
        "a1": {"lib:libc:2.31:memcpy": 1.0},
        "b1": {"lib:libc:2.31:memcpy": 1.0},
        "a2": {"bundle:mirai_core": 1.0},
        "b2": {"bundle:mirai_core": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.5, 10.0, 10.0)
    ts.add_match("a2", "b2", 1.0, 30.0, 30.0)
    rows = by_id(ts.summary(40.0, 40.0))

    libc = rows["lib:libc:2.31"]
    mirai = rows["bundle:mirai_core"]
    # Per-tag score is that tag's own cohesion, not the pair's blended 0.875.
    assert abs(libc["score"] - 0.5) < 1e-9, libc["score"]
    assert abs(mirai["score"] - 1.0) < 1e-9
    # contribution_pct is 0-100, not a 0-1 fraction (the old bug rendered 25% as 0.2%).
    assert abs(libc["contribution_pct"] - 25.0) < 1e-9, libc["contribution_pct"]
    assert abs(mirai["contribution_pct"] - 75.0) < 1e-9
    assert abs(libc["coverage_pct_a"] - 25.0) < 1e-9
    # The func-level tag survives as a child of its lib:name:version parent.
    assert [c["tag_id"] for c in libc["children"]] == ["lib:libc:2.31:memcpy"]
    assert mirai["children"] == []


def test_disagreement_stays_on_its_own_tag():
    """A match whose two sides are tagged differently must still count toward each
    side's own tag. Re-bucketing it (the old behaviour) drained matched mass out of
    the real tag while its unmatched mass stayed, so every library rendered as an
    almost entirely unmatched flow."""
    fid_tags = {
        "a1": {"lib:libc:2.31": 1.0},
        "b1": {"lib:uclibc:0.9": 1.0},  # both tagged, nothing in common
        "a2": {"lib:libc:2.31": 1.0},
        "b2": {},  # one side has no evidence at all
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.9, 10.0, 10.0)
    ts.add_match("a2", "b2", 0.9, 10.0, 10.0)
    rows = by_id(ts.summary(20.0, 20.0))

    # A's libc mass is on libc, matched, both times -- not shunted elsewhere.
    assert abs(rows["lib:libc:2.31"]["weight_a"] - 20.0) < 1e-9
    assert abs(rows["lib:libc:2.31"]["weight_b"]) < 1e-9
    assert abs(rows["lib:uclibc:0.9"]["weight_b"] - 10.0) < 1e-9
    # B's untagged partner is reported as untagged, on B's side only.
    assert abs(rows[TAG_UNTAGGED]["weight_b"] - 10.0) < 1e-9
    # Disagreement is still visible, as a field rather than a stolen bucket.
    assert abs(rows["lib:libc:2.31"]["mismatch_weight_a"] - 20.0) < 1e-9
    assert TAG_MISMATCH not in rows


def test_one_sided_tag_does_not_inflate_unmatched():
    """The reported symptom: libc showing a huge unmatched flow. 3 of A's 4 libc
    functions matched, so at most a quarter of libc may read as unmatched."""
    fid_tags = {("a%d" % i): {"lib:libc": 1.0} for i in range(4)}
    fid_tags.update(
        {"b1": {}, "b2": {"lib:libc": 1.0}, "b3": {}}
    )  # FID missed 2 of B's
    ts = TagSplit(fid_tags)
    for a, b in (("a0", "b1"), ("a1", "b2"), ("a2", "b3")):
        ts.add_match(a, b, 0.9, 10.0, 10.0)
    ts.add_unique("a3", 10.0, "a")
    libc = by_id(ts.summary(40.0, 30.0))["lib:libc"]

    assert abs(libc["weight_a"] - 30.0) < 1e-9, "3 matched libc funcs, all on libc"
    assert abs(libc["unique_weight_a"] - 10.0) < 1e-9
    unmatched_share = libc["unique_weight_a"] / (
        libc["weight_a"] + libc["unique_weight_a"]
    )
    assert abs(unmatched_share - 0.25) < 1e-9, unmatched_share
    assert abs(libc["coverage_pct_a"] - 100.0) < 1e-9


def test_asymmetric_sides_and_unique_flow():
    fid_tags = {
        "a1": {"lib:libc": 1.0},
        "b1": {"lib:libc": 1.0},
        "a9": {"lib:libc": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 1.0, 40.0, 10.0)  # same function, different feature counts
    ts.add_unique("a9", 50.0, "a")  # libc mass present only in A
    rows = by_id(ts.summary(90.0, 10.0))
    libc = rows["lib:libc"]
    # Each side keeps its own weight instead of both being the max.
    assert abs(libc["weight_a"] - 40.0) < 1e-9
    assert abs(libc["weight_b"] - 10.0) < 1e-9
    assert abs(libc["unique_weight_a"] - 50.0) < 1e-9
    assert abs(libc["unique_weight_b"]) < 1e-9
    # A is entirely libc; B's libc is fully matched.
    assert abs(libc["coverage_pct_a"] - 100.0) < 1e-9
    assert abs(libc["coverage_pct_b"] - 100.0) < 1e-9


def test_fractional_allocation_and_bins():
    fid_tags = {
        "a1": {"lib:libc": 1.0, "bundle:utils": 1.0},
        "b1": {"lib:libc": 1.0, "bundle:utils": 0.5},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.8, 10.0, 10.0)
    rows = by_id(ts.summary(10.0, 10.0))
    # min-weight pairing, then split across the two shared tags. matched_weight is
    # two-sided (5.0 from each of A and B), since each side is attributed separately.
    assert abs(rows["lib:libc"]["matched_weight"] - 10.0) < 1e-9
    assert abs(rows["bundle:utils"]["matched_weight"] - 5.0) < 1e-9
    assert abs(rows["lib:libc"]["weight_a"] - 5.0) < 1e-9
    # score 0.8 lands in the 5%-wide bin 16 (0.80-0.85), which the UI re-aggregates.
    assert list(rows["lib:libc"]["bins"].keys()) == ["16"]
    # [count_a, weight_a, count_b, weight_b]
    assert rows["lib:libc"]["bins"]["16"] == [0.5, 5.0, 0.5, 5.0]


def test_bins_reconcile_with_side_weights():
    """The sankey sizes a tag's left node from its bins plus its unmatched mass, but
    labels it from weight_a/unique_weight_a. If the bins did not sum to weight_a the
    drawn height and the printed count would disagree."""
    fid_tags = {
        "a1": {"lib:libc": 1.0},
        "b1": {"lib:libc": 1.0},
        "a2": {"lib:libc": 1.0},
        "b2": {"lib:libc": 1.0},
        "a3": {"lib:libc": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.95, 12.0, 9.0)
    ts.add_match("a2", "b2", 0.30, 7.0, 5.0)  # a different bin
    ts.add_unique("a3", 4.0, "a")
    libc = by_id(ts.summary(23.0, 14.0))["lib:libc"]

    bins = libc["bins"].values()
    assert len(libc["bins"]) == 2, "two distinct similarity bins"
    assert abs(sum(b[1] for b in bins) - libc["weight_a"]) < 1e-9
    assert abs(sum(b[3] for b in bins) - libc["weight_b"]) < 1e-9
    assert abs(sum(b[0] for b in bins) - 2.0) < 1e-9, "two matched funcs on A"
    assert abs(sum(b[2] for b in bins) - 2.0) < 1e-9
    # What the left node draws vs what its label says.
    assert abs((libc["weight_a"] + libc["unique_weight_a"]) - 23.0) < 1e-9


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print("ok  %s" % fn.__name__)
    print("all passed")
