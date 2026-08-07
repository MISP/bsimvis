#!/usr/bin/env python3
"""Self-check for the per-tag similarity split (bsimvis/app/services/bin_sim_tags.py)
and for the File sim view's read path over it (_page_diff, routes/bin_sim.py).

No redis, no fixtures, no running server: TagSplit is pure arithmetic over a
fid -> tags map, and _page_diff is pure filtering over a diff dict + query args.
Run: python3 scripts/test_bin_sim_tag_split.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask  # noqa: E402

from bsimvis.app.services.bin_sim_tags import (  # noqa: E402
    TAG_MISMATCH,
    TAG_UNTAGGED,
    TagSplit,
    normalize_tags,
    parse_tag_id,
    tag_parent,
)
from bsimvis.app.routes.bin_sim import _page_diff, _sim_pair_sid  # noqa: E402


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


def test_drift_names_its_counterpart():
    """`mismatch_weight_*` says mass disagreed; `drift` says what it disagreed with.

    The tree draws a drift child under each library, and "libc 2.31 -> 2.35" is a
    version-drift finding while a bare count is not.
    """
    fid_tags = {
        "a1": {"lib:libc:2.31:memcpy": 1.0},
        "b1": {"lib:libc:2.35:memcpy": 1.0},  # same lib, drifted version
        "a2": {"lib:zlib:1.2:inflate": 1.0},
        "b2": {"lib:zlib:1.2:inflate": 1.0},  # clean match
        "a3": {"lib:libc:2.31:strlen": 1.0},
        "b3": {},  # no evidence: untagged, not drift
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.9, 10.0, 10.0)
    ts.add_match("a2", "b2", 0.95, 10.0, 10.0)
    ts.add_match("a3", "b3", 0.9, 10.0, 10.0)
    rows = by_id(ts.summary(30.0, 30.0))

    # Counterpart is rolled up to its display parent, not left per-function.
    assert rows["lib:libc:2.31"]["drift"] == {"lib:libc:2.35": 10.0}
    # A clean match drifts nowhere.
    assert rows["lib:zlib:1.2"]["drift"] == {}
    # An untagged partner is absence of evidence, not disagreement.
    assert TAG_UNTAGGED not in rows["lib:libc:2.31"]["drift"]
    # The counterpart's row records the drift symmetrically, from its own side.
    assert rows["lib:libc:2.35"]["drift"] == {"lib:libc:2.31": 10.0}
    # Drift never exceeds the mismatch mass it explains. Both sides, because a
    # tag's row is side-agnostic and B-side tags carry their disagreement in
    # mismatch_weight_b.
    for row in rows.values():
        mismatch = row["mismatch_weight_a"] + row["mismatch_weight_b"]
        assert sum(row["drift"].values()) <= mismatch + 1e-9


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


# ---------------------------------------------------------------------------
# File sim read path: one table with a `state` column, scoped by the tag tree.
# ---------------------------------------------------------------------------

_APP = Flask(__name__)

_DIFF = {
    "diff": {
        "matched": [
            {"func_a": "fa1", "func_b": "fb1", "similarity": 0.98},
            {"func_a": "fa2", "func_b": "fb2", "similarity": 0.80},
            {"func_a": "fa3", "func_b": "fb3", "similarity": 0.95},
        ],
        "unique_to_a": [
            {"func_id": "fa4"},  # a third memcpy, unmatched
            {"func_id": "fa5"},  # auto-named
            {"func_id": "fa6"},  # auto-named
        ],
        "unique_to_b": [{"func_id": "fb4"}],
    },
    "functions_metadata": {
        "fa1": {"name": "memcpy", "tags": ["lib:libc:2.31:memcpy"]},
        "fb1": {"name": "memcpy", "tags": ["lib:libc:2.31:memcpy"]},
        "fa2": {"name": "memcpy", "tags": ["lib:libc:2.31:memcpy"]},
        "fb2": {"name": "memcpy", "tags": ["lib:libc:2.31:memcpy"]},
        "fa3": {"name": "inflate", "tags": ["lib:zlib:1.2:inflate"]},
        "fb3": {"name": "inflate", "tags": ["lib:zlib:1.2:inflate"]},
        "fa4": {"name": "memcpy", "tags": ["lib:libc:2.31:memcpy"]},
        "fa5": {"name": "FUN_00401234", "tags": []},
        "fa6": {"name": "FUN_00401299", "tags": []},
        "fb4": {"name": "helper", "tags": []},
    },
}


def page(qs=""):
    with _APP.test_request_context("/?" + qs):
        return _page_diff(_DIFF, "all")


def test_union_table_carries_state():
    """All / Matched / Unmatched are one request with a different state filter."""
    r = page()
    assert r["total"] == 7
    assert {i["state"] for i in r["items"]} == {"matched", "uniq_a", "uniq_b"}
    assert page("state=uniq_a,uniq_b")["total"] == 4
    assert page("state=matched")["total"] == 3


def test_tag_scope_is_a_prefix_match():
    """Selecting a tree node catches everything under it, at any depth."""
    assert page("tags=lib:libc:2.31")["total"] == 3  # its per-function children
    assert page("tags=lib")["total"] == 4  # the whole namespace
    assert page("tags=original_code")["total"] == 3  # untagged is selectable
    # Prefix must respect the separator: `lib` must not match a `libfoo:` tag.
    assert page("tags=lib:libc:2")["total"] == 0


def test_fold_by_name_pages_over_names():
    """Copies of a name fold into one row so a page can never split them."""
    r = page("collapse=name")
    folds = {i["fold_name"]: i["n_copies"] for i in r["items"] if i.get("fold_name")}
    assert folds["memcpy"] == 3, folds
    assert r["total"] == 5  # memcpy, inflate, helper + 2 auto-named
    # Auto-generated names are not copies of each other.
    assert sum(1 for i in r["items"] if not i.get("fold_name")) == 2
    # The fold shows its strongest evidence, not an arbitrary member.
    rep = next(i for i in r["items"] if i.get("fold_name") == "memcpy")
    assert rep["state"] == "matched" and rep["similarity"] == 0.98
    # Expanding a fold returns its members, uncollapsed.
    expanded = page("collapse=name&name=memcpy")
    assert expanded["total"] == 3
    assert all("n_copies" not in i for i in expanded["items"])


def test_sort_applies_to_fold_representatives():
    r = page("collapse=name&sort_col=similarity&sort_dir=desc")
    sims = [i.get("similarity") or 0 for i in r["items"]]
    assert sims == sorted(sims, reverse=True), sims


# ---------------------------------------------------------------------------
# Similarity tags on the matched rows: a matched row IS a function-similarity
# pair, so it carries that pair's own tags and can be filtered by them.
# ---------------------------------------------------------------------------


class _FakePipe:
    def __init__(self, docs):
        self.docs, self.keys = docs, []

    def get(self, k):
        self.keys.append(k)

    def execute(self):
        out = [self.docs.get(k) for k in self.keys]
        self.keys = []
        return out


class _FakeRedis:
    """Just enough of the client for the pair lookups: pipelined GETs."""

    def __init__(self, docs):
        self.docs = docs

    def pipeline(self, transaction=False):
        return _FakePipe(self.docs)


# fa1 < fb1, and the key puts the larger fid first (similarity_service.py:1061).
_PAIR_DOCS = {
    "main:sim:uc:fb1::fa1": json.dumps({"tags": ["crypto"], "user_tags": ["bookmark"]}),
    "main:sim:uc:fb2::fa2": json.dumps({"tags": [], "user_tags": ["lib:libc:review"]}),
    # fb3::fa3 deliberately absent: a pair with no doc must still page fine.
}


def tag_page(qs=""):
    with _APP.test_request_context("/?" + qs):
        return _page_diff(_DIFF, "all", _FakeRedis(_PAIR_DOCS), "main", "uc", None)


def test_pair_sid_matches_how_similarity_writes_it():
    # Larger fid first, collection prefix stripped.
    assert _sim_pair_sid("main:func:aa", "main:func:bb", "main", "uc", None) == (
        "main:sim:uc:bb::aa"
    )
    assert _sim_pair_sid("main:func:bb", "main:func:aa", "main", "uc", None) == (
        "main:sim:uc:bb::aa"
    ), "order of the arguments must not matter"
    # Pools keep whole function ids under their own namespace.
    assert _sim_pair_sid("c:func:aa", "c:func:bb", "main", "uc", "p1") == (
        "global:pool:p1:sim:c:func:bb::c:func:aa"
    )
    assert _sim_pair_sid("fa1", None, "main", "uc", None) is None


def test_matched_rows_carry_the_pairs_tags():
    items = {i.get("sid"): i for i in tag_page("state=matched")["items"]}
    assert items["main:sim:uc:fb1::fa1"]["tags"] == ["crypto"]
    assert items["main:sim:uc:fb1::fa1"]["user_tags"] == ["bookmark"]
    # A pair with no stored doc reads as untagged rather than failing.
    assert items["main:sim:uc:fb3::fa3"]["tags"] == []


def test_similarity_tag_filter():
    assert tag_page("sim_tags=crypto")["total"] == 1
    assert tag_page("sim_tags=bookmark")["total"] == 1, "user tags count too"
    # Namespace prefix, same rule as the tree's scope.
    assert tag_page("sim_tags=lib")["total"] == 1
    assert tag_page("sim_tags=lib:libc:review")["total"] == 1
    assert tag_page("sim_tags=nope")["total"] == 0
    # Unmatched rows have no pair, so a similarity-tag filter excludes them.
    assert all(i["state"] == "matched" for i in tag_page("sim_tags=crypto")["items"])


def test_similarity_tag_exclude():
    # 7 rows total, one of which carries `crypto`.
    assert tag_page("sim_tags_not=crypto")["total"] == 6
    assert tag_page("sim_tags=crypto&sim_tags_not=bookmark")["total"] == 0


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print("ok  %s" % fn.__name__)
    print("all passed")
