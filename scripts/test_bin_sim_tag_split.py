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
    AXIS_CATEGORY,
    AXIS_ORIGIN,
    AXIS_SEVERITY,
    AXIS_USER,
    SPLIT_SCHEMA,
    AxisSplit,
    TagSplit,
    joint_marginal,
    merge_tag_fields,
    normalize_tags,
    parse_tag_id,
    split_axes,
    tag_axis,
    tag_parent,
)
from bsimvis.app.routes.bin_sim import _page_diff, _sim_pair_sid  # noqa: E402


def by_id(rows):
    return {r["tag_id"]: r for r in rows}


def test_normalize_and_parse():
    assert normalize_tags(["origin:lib:libc:2.31"]) == {"origin:lib:libc:2.31": 1.0}
    assert normalize_tags({"origin:lib:libc:unknown": 0.5}) == {"origin:lib:libc:unknown": 0.5}
    assert normalize_tags(None) == {}
    assert normalize_tags("nonsense") == {}

    # Matching keeps the function name; only display rolls up.
    assert tag_parent("origin:lib:libc:2.31:memcpy") == "origin:lib:libc:2.31"
    assert tag_parent("origin:lib:libc:2.31") == "origin:lib:libc:2.31"
    assert tag_parent(TAG_UNTAGGED) == TAG_UNTAGGED
    assert parse_tag_id("origin:lib:libc:2.31:memcpy") == ("lib", "libc", "2.31")
    assert parse_tag_id("mytag") == ("user", "mytag", "")


def test_shared_tag_split():
    fid_tags = {
        "a1": {"origin:lib:libc:2.31:memcpy": 1.0},
        "b1": {"origin:lib:libc:2.31:memcpy": 1.0},
        "a2": {"origin:bundle:mirai_core:unknown": 1.0},
        "b2": {"origin:bundle:mirai_core:unknown": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.5, 10.0, 10.0)
    ts.add_match("a2", "b2", 1.0, 30.0, 30.0)
    rows = by_id(ts.summary(40.0, 40.0))

    libc = rows["origin:lib:libc:2.31"]
    mirai = rows["origin:bundle:mirai_core:unknown"]
    # Per-tag score is that tag's own cohesion, not the pair's blended 0.875.
    assert abs(libc["score"] - 0.5) < 1e-9, libc["score"]
    assert abs(mirai["score"] - 1.0) < 1e-9
    # contribution_pct is 0-100, not a 0-1 fraction (the old bug rendered 25% as 0.2%).
    assert abs(libc["contribution_pct"] - 25.0) < 1e-9, libc["contribution_pct"]
    assert abs(mirai["contribution_pct"] - 75.0) < 1e-9
    assert abs(libc["coverage_pct_a"] - 25.0) < 1e-9
    # The func-level tag survives as a child of its lib:name:version parent.
    assert [c["tag_id"] for c in libc["children"]] == ["origin:lib:libc:2.31:memcpy"]
    assert mirai["children"] == []


def test_disagreement_stays_on_its_own_tag():
    """A match whose two sides are tagged differently must still count toward each
    side's own tag. Re-bucketing it (the old behaviour) drained matched mass out of
    the real tag while its unmatched mass stayed, so every library rendered as an
    almost entirely unmatched flow."""
    fid_tags = {
        "a1": {"origin:lib:libc:2.31": 1.0},
        "b1": {"origin:lib:uclibc:0.9": 1.0},  # both tagged, nothing in common
        "a2": {"origin:lib:libc:2.31": 1.0},
        "b2": {},  # one side has no evidence at all
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.9, 10.0, 10.0)
    ts.add_match("a2", "b2", 0.9, 10.0, 10.0)
    rows = by_id(ts.summary(20.0, 20.0))

    # A's libc mass is on libc, matched, both times -- not shunted elsewhere.
    assert abs(rows["origin:lib:libc:2.31"]["weight_a"] - 20.0) < 1e-9
    assert abs(rows["origin:lib:libc:2.31"]["weight_b"]) < 1e-9
    assert abs(rows["origin:lib:uclibc:0.9"]["weight_b"] - 10.0) < 1e-9
    # B's untagged partner is reported as untagged, on B's side only.
    assert abs(rows[TAG_UNTAGGED]["weight_b"] - 10.0) < 1e-9
    # Disagreement is still visible, as a field rather than a stolen bucket.
    assert abs(rows["origin:lib:libc:2.31"]["mismatch_weight_a"] - 20.0) < 1e-9
    assert TAG_MISMATCH not in rows


def test_drift_names_its_counterpart():
    """`mismatch_weight_*` says mass disagreed; `drift` says what it disagreed with.

    The tree draws a drift child under each library, and "libc 2.31 -> 2.35" is a
    version-drift finding while a bare count is not.
    """
    fid_tags = {
        "a1": {"origin:lib:libc:2.31:memcpy": 1.0},
        "b1": {"origin:lib:libc:2.35:memcpy": 1.0},  # same lib, drifted version
        "a2": {"origin:lib:zlib:1.2:inflate": 1.0},
        "b2": {"origin:lib:zlib:1.2:inflate": 1.0},  # clean match
        "a3": {"origin:lib:libc:2.31:strlen": 1.0},
        "b3": {},  # no evidence: untagged, not drift
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.9, 10.0, 10.0)
    ts.add_match("a2", "b2", 0.95, 10.0, 10.0)
    ts.add_match("a3", "b3", 0.9, 10.0, 10.0)
    rows = by_id(ts.summary(30.0, 30.0))

    # Counterpart is rolled up to its display parent, not left per-function.
    assert rows["origin:lib:libc:2.31"]["drift"] == {"origin:lib:libc:2.35": 10.0}
    # A clean match drifts nowhere.
    assert rows["origin:lib:zlib:1.2"]["drift"] == {}
    # An untagged partner is absence of evidence, not disagreement.
    assert TAG_UNTAGGED not in rows["origin:lib:libc:2.31"]["drift"]
    # The counterpart's row records the drift symmetrically, from its own side.
    assert rows["origin:lib:libc:2.35"]["drift"] == {"origin:lib:libc:2.31": 10.0}
    # Drift never exceeds the mismatch mass it explains. Both sides, because a
    # tag's row is side-agnostic and B-side tags carry their disagreement in
    # mismatch_weight_b.
    for row in rows.values():
        mismatch = row["mismatch_weight_a"] + row["mismatch_weight_b"]
        assert sum(row["drift"].values()) <= mismatch + 1e-9


def test_one_sided_tag_does_not_inflate_unmatched():
    """The reported symptom: libc showing a huge unmatched flow. 3 of A's 4 libc
    functions matched, so at most a quarter of libc may read as unmatched."""
    fid_tags = {("a%d" % i): {"origin:lib:libc:unknown": 1.0} for i in range(4)}
    fid_tags.update(
        {"b1": {}, "b2": {"origin:lib:libc:unknown": 1.0}, "b3": {}}
    )  # FID missed 2 of B's
    ts = TagSplit(fid_tags)
    for a, b in (("a0", "b1"), ("a1", "b2"), ("a2", "b3")):
        ts.add_match(a, b, 0.9, 10.0, 10.0)
    ts.add_unique("a3", 10.0, "a")
    libc = by_id(ts.summary(40.0, 30.0))["origin:lib:libc:unknown"]

    assert abs(libc["weight_a"] - 30.0) < 1e-9, "3 matched libc funcs, all on libc"
    assert abs(libc["unique_weight_a"] - 10.0) < 1e-9
    unmatched_share = libc["unique_weight_a"] / (
        libc["weight_a"] + libc["unique_weight_a"]
    )
    assert abs(unmatched_share - 0.25) < 1e-9, unmatched_share
    assert abs(libc["coverage_pct_a"] - 100.0) < 1e-9


def test_asymmetric_sides_and_unique_flow():
    fid_tags = {
        "a1": {"origin:lib:libc:unknown": 1.0},
        "b1": {"origin:lib:libc:unknown": 1.0},
        "a9": {"origin:lib:libc:unknown": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 1.0, 40.0, 10.0)  # same function, different feature counts
    ts.add_unique("a9", 50.0, "a")  # libc mass present only in A
    rows = by_id(ts.summary(90.0, 10.0))
    libc = rows["origin:lib:libc:unknown"]
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
        "a1": {"origin:lib:libc:unknown": 1.0, "origin:bundle:utils:unknown": 1.0},
        "b1": {"origin:lib:libc:unknown": 1.0, "origin:bundle:utils:unknown": 0.5},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.8, 10.0, 10.0)
    rows = by_id(ts.summary(10.0, 10.0))
    # min-weight pairing, then split across the two shared tags. matched_weight is
    # two-sided (5.0 from each of A and B), since each side is attributed separately.
    assert abs(rows["origin:lib:libc:unknown"]["matched_weight"] - 10.0) < 1e-9
    assert abs(rows["origin:bundle:utils:unknown"]["matched_weight"] - 5.0) < 1e-9
    assert abs(rows["origin:lib:libc:unknown"]["weight_a"] - 5.0) < 1e-9
    # score 0.8 lands in the 5%-wide bin 16 (0.80-0.85), which the UI re-aggregates.
    assert list(rows["origin:lib:libc:unknown"]["bins"].keys()) == ["16"]
    # [count_a, weight_a, count_b, weight_b]
    assert rows["origin:lib:libc:unknown"]["bins"]["16"] == [0.5, 5.0, 0.5, 5.0]


def test_bins_reconcile_with_side_weights():
    """The sankey sizes a tag's left node from its bins plus its unmatched mass, but
    labels it from weight_a/unique_weight_a. If the bins did not sum to weight_a the
    drawn height and the printed count would disagree."""
    fid_tags = {
        "a1": {"origin:lib:libc:unknown": 1.0},
        "b1": {"origin:lib:libc:unknown": 1.0},
        "a2": {"origin:lib:libc:unknown": 1.0},
        "b2": {"origin:lib:libc:unknown": 1.0},
        "a3": {"origin:lib:libc:unknown": 1.0},
    }
    ts = TagSplit(fid_tags)
    ts.add_match("a1", "b1", 0.95, 12.0, 9.0)
    ts.add_match("a2", "b2", 0.30, 7.0, 5.0)  # a different bin
    ts.add_unique("a3", 4.0, "a")
    libc = by_id(ts.summary(23.0, 14.0))["origin:lib:libc:unknown"]

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
        "fa1": {"name": "memcpy", "tags": ["origin:lib:libc:2.31:memcpy"]},
        "fb1": {"name": "memcpy", "tags": ["origin:lib:libc:2.31:memcpy"]},
        "fa2": {"name": "memcpy", "tags": ["origin:lib:libc:2.31:memcpy"]},
        "fb2": {"name": "memcpy", "tags": ["origin:lib:libc:2.31:memcpy"]},
        "fa3": {"name": "inflate", "tags": ["origin:lib:zlib:1.2:inflate"]},
        "fb3": {"name": "inflate", "tags": ["origin:lib:zlib:1.2:inflate"]},
        "fa4": {"name": "memcpy", "tags": ["origin:lib:libc:2.31:memcpy"]},
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
    assert page("tags=origin:lib:libc:2.31")["total"] == 3  # per-function children
    assert page("tags=origin:lib")["total"] == 4  # the whole library namespace
    assert page("tags=origin")["total"] == 4  # every origin, at the root
    assert page("tags=original_code")["total"] == 3  # untagged is selectable
    # Prefix must respect the separator: `lib` must not match a `libfoo:` tag.
    assert page("tags=origin:lib:libc:2")["total"] == 0


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
    "main:sim:uc:fb2::fa2": json.dumps({"tags": [], "user_tags": ["origin:lib:libc:review"]}),
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
    # Namespace prefix, same rule as the tree's scope. Origin ids lead with
    # `origin:` now, so a filter written against the old `lib` bucket no longer
    # matches -- a deliberate hard break, not a regression.
    assert tag_page("sim_tags=lib")["total"] == 0
    assert tag_page("sim_tags=origin")["total"] == 1
    assert tag_page("sim_tags=origin:lib:libc:review")["total"] == 1
    assert tag_page("sim_tags=nope")["total"] == 0
    # Unmatched rows have no pair, so a similarity-tag filter excludes them.
    assert all(i["state"] == "matched" for i in tag_page("sim_tags=crypto")["items"])


def test_similarity_tag_exclude():
    # 7 rows total, one of which carries `crypto`.
    assert tag_page("sim_tags_not=crypto")["total"] == 6
    assert tag_page("sim_tags=crypto&sim_tags_not=bookmark")["total"] == 0


# --- axes -----------------------------------------------------------------
# A tag answers one of two questions -- whose code is this, or what does it do
# -- and the two must not share one pool of mass. These pin that separation.


def test_axis_routing_and_parents():
    assert tag_axis("origin:lib:libc:2.31") == AXIS_ORIGIN
    assert tag_axis("origin:bundle:mirai:unknown") == AXIS_ORIGIN
    assert tag_axis("severity:high") == AXIS_SEVERITY
    assert tag_axis("category:network:c2") == AXIS_CATEGORY
    assert tag_axis("user:bookmark") == AXIS_USER
    # An unrecognised tag came from a human, and must not be able to empty
    # original_code or dilute the behaviour percentages.
    assert tag_axis("mirai") == AXIS_USER
    # Origin rolls up at version, category at the behaviour group it refines.
    assert tag_parent("category:network:c2") == "category:network"
    assert tag_parent("origin:lib:libc:2.31:memcpy") == "origin:lib:libc:2.31"
    # A bundle carries a placeholder version precisely so this depth is uniform.
    assert tag_parent("origin:bundle:mirai:unknown:scan") == "origin:bundle:mirai:unknown"
    # Severity is one segment deep and is already its own parent.
    assert tag_parent("severity:high") == "severity:high"


def test_origin_resolves_by_priority():
    tags = {
        "origin:lib:libc:2.31": 1.0,
        "origin:bundle:mirai:unknown": 1.0,
        "category:network:c2": 1.0,
    }
    axes = split_axes(tags)
    # Function ID matched actual bytes; the bundle tag labelled a whole binary.
    assert axes[AXIS_ORIGIN] == {"origin:lib:libc:2.31": 1.0}
    assert axes[AXIS_CATEGORY] == {"category:network:c2": 1.0}
    assert axes[AXIS_SEVERITY] == {} and axes[AXIS_USER] == {}

    # An explicit priority overrides the namespace default...
    meta = {"origin:bundle:mirai:unknown": {"priority": 500}}
    assert set(split_axes(tags, meta)[AXIS_ORIGIN]) == {"origin:bundle:mirai:unknown"}
    # ...and a genuine tie keeps the even split, which is the honest answer.
    meta = {"origin:bundle:mirai:unknown": {"priority": 100}}
    assert set(split_axes(tags, meta)[AXIS_ORIGIN]) == {
        "origin:lib:libc:2.31",
        "origin:bundle:mirai:unknown",
    }


def test_analysis_tag_does_not_dilute_origin():
    """The regression this whole split exists to prevent.

    Under one flat tag space, tagging a function's behaviour halved its
    library's mass and evicted genuinely original code from `original_code` --
    that bucket only fills when a function carries no tag at all.
    """
    plain = AxisSplit({"a1": {"origin:lib:libc:2.31": 1.0}, "b1": {"origin:lib:libc:2.31": 1.0}})
    plain.add_match("a1", "b1", 1.0, 10.0, 10.0)
    plain.add_unique("a2", 10.0, "a")
    base = by_id(plain.summaries(20.0, 10.0)["tags_summary"])

    tagged = AxisSplit(
        {
            "a1": {"origin:lib:libc:2.31": 1.0, "category:network:c2": 1.0},
            "b1": {"origin:lib:libc:2.31": 1.0},
            "a2": {"category:network:c2": 1.0},
        }
    )
    tagged.add_match("a1", "b1", 1.0, 10.0, 10.0)
    tagged.add_unique("a2", 10.0, "a")
    out = tagged.summaries(20.0, 10.0)
    rows = by_id(out["tags_summary"])

    assert rows["origin:lib:libc:2.31"]["weight_a"] == base["origin:lib:libc:2.31"]["weight_a"]
    # a2 has a behaviour but is still nobody's library code, so it stays original.
    assert rows[TAG_UNTAGGED]["unique_weight_a"] == 10.0

    cats = by_id(out["category_summary"])
    assert set(cats) == {"category:network"}
    # Behaviour overlays: the tag claims its functions whole, on top of origin.
    assert cats["category:network"]["weight_a"] == 10.0
    assert cats["category:network"]["unique_weight_a"] == 10.0
    # "No behaviour found" is the absence of a finding, not a row competing
    # with one -- only origin gets an untagged bucket.
    assert TAG_UNTAGGED not in cats
    assert out["severity_summary"] == [] and out["user_summary"] == []


def test_joint_crosses_origin_and_category():
    """The Sankey's third stage: which part of libc's match is network code."""
    split = AxisSplit(
        {
            "a1": {"origin:lib:libc:2.31": 1.0, "category:network:c2": 1.0},
            "b1": {"origin:lib:libc:2.31": 1.0},
            "a2": {"origin:lib:libc:2.31": 1.0},
            "b2": {"origin:lib:libc:2.31": 1.0},
        }
    )
    split.add_match("a1", "b1", 1.0, 10.0, 10.0)
    split.add_match("a2", "b2", 1.0, 30.0, 30.0)
    out = split.summaries(40.0, 40.0)

    crossed = joint_marginal(out["joint"], AXIS_ORIGIN, AXIS_CATEGORY)
    cell = crossed["origin:lib:libc:2.31"]["category:network"]
    assert cell[0] == 10.0, "A-side matched mass that is both libc and network"
    assert cell[1] == 0.0, "B never carried the behaviour"
    assert cell[4] == 1.0, "one function"

    # Untagged mass is the row minus its cells, so nothing has to be stored for
    # it and the two can never drift apart.
    row = by_id(out["tags_summary"])["origin:lib:libc:2.31"]
    assert row["weight_a"] - cell[0] == 30.0


def test_joint_serves_every_axis_mode():
    """Ten Sankey modes, one stored table: each view is a marginal of it."""
    split = AxisSplit(
        {
            "a1": {
                "origin:lib:libc:2.31": 1.0,
                "severity:high": 1.0,
                "category:network:c2": 1.0,
            },
            "b1": {"origin:lib:libc:2.31": 1.0, "severity:high": 1.0},
            "a2": {"severity:low": 1.0, "category:util:init": 1.0},
            "b2": {"severity:low": 1.0, "category:util:init": 1.0},
        }
    )
    split.add_match("a1", "b1", 1.0, 10.0, 10.0)
    split.add_match("a2", "b2", 1.0, 40.0, 40.0)
    out = split.summaries(50.0, 50.0)
    assert out["split_schema"] == SPLIT_SCHEMA

    # The cross the whole redesign was asked for: high-severity network mass.
    sev_cat = joint_marginal(out["joint"], AXIS_SEVERITY, AXIS_CATEGORY)
    assert sev_cat["severity:high"]["category:network"][0] == 10.0
    assert sev_cat["severity:low"]["category:util"][0] == 40.0
    # a2 carries no origin, so crossing severity with origin must attribute it
    # to original_code rather than dropping it.
    sev_origin = joint_marginal(out["joint"], AXIS_SEVERITY, AXIS_ORIGIN)
    assert sev_origin["severity:low"][TAG_UNTAGGED][0] == 40.0

    # A single-axis view is the same call with one axis, and must conserve the
    # same total however the joint is sliced.
    for axis in (AXIS_ORIGIN, AXIS_SEVERITY, AXIS_CATEGORY):
        single = joint_marginal(out["joint"], axis)
        assert sum(c[0] for row in single.values() for c in row.values()) == 50.0, axis


def test_merge_tag_fields_reads_both_fields():
    meta = {"tags": ["origin:lib:libc:2.31"], "user_tags": ["severity:high", "origin:lib:libc:2.31"]}
    merged = merge_tag_fields(meta)
    assert merged == {"origin:lib:libc:2.31": 1.0, "severity:high": 1.0}
    assert merge_tag_fields({}) == {}


class FakeRedis:
    """Just enough redis for the resplit path: string get/set, one set, pipelines."""

    def __init__(self, values=None, members=()):
        self.values = dict(values or {})
        self.members = set(members)

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def smembers(self, key):
        return set(self.members) if key.endswith(":built:uc") else set()

    def hgetall(self, key):
        return {}

    def pipeline(self, transaction=False):
        outer = self

        class Pipe:
            def __init__(self):
                self.ops = []

            def get(self, key):
                self.ops.append(("get", key))

            def set(self, key, value):
                self.ops.append(("set", key, value))

            def execute(self):
                out = []
                for op in self.ops:
                    if op[0] == "get":
                        out.append(outer.get(op[1]))
                    else:
                        outer.set(op[1], op[2])
                        out.append(True)
                self.ops = []
                return out

        return Pipe()


def test_resplit_replays_the_split_from_the_stored_diff():
    """Re-tagging must not need a rebuild.

    The score comes from the matched edges alone, so it has to survive the
    resplit untouched while the split under it changes.
    """
    from bsimvis.app.services.bin_sim_service import bin_sim_service

    sid = "main:bin_sim:uc:aaa::bbb"
    stored = {
        "md5_a": "aaa",
        "md5_b": "bbb",
        "score": 0.75,
        "tags_summary": [],
        "diff": {
            "matched": [{"func_a": "fa1", "func_b": "fb1", "similarity": 0.9}],
            "unique_to_a": [{"func_id": "fa2"}],
            "unique_to_b": [],
        },
    }
    meta = {
        "fa1": {"bsim_features_count": 10, "tags": ["origin:lib:libc:2.31"],
                "user_tags": ["severity:high", "category:network:c2"]},
        "fb1": {"bsim_features_count": 10, "tags": ["origin:lib:libc:2.31"]},
        "fa2": {"bsim_features_count": 5},
    }
    values = {sid: json.dumps(stored), "main:tags_rev": "7"}
    for fid, m in meta.items():
        values[f"{fid}:meta"] = json.dumps(m)

    fake = FakeRedis(values, members=[sid])
    old_r = bin_sim_service.r
    bin_sim_service.r = fake
    try:
        assert bin_sim_service.resplit_bin_sim("main", algo="uc") is True
    finally:
        bin_sim_service.r = old_r

    out = json.loads(fake.values[sid])
    assert out["score"] == 0.75, "resplitting must not touch the score"
    assert out["tags_rev"] == 7

    rows = by_id(out["tags_summary"])
    # The tagged function is still wholly libc's, and the untagged leftover is
    # still original code.
    assert rows["origin:lib:libc:2.31"]["weight_a"] == 10.0
    assert rows[TAG_UNTAGGED]["unique_weight_a"] == 5.0

    # A resplit must write every axis and stamp the schema, or the read path
    # cannot tell a fresh doc from one split by the old two-axis code.
    assert by_id(out["severity_summary"])["severity:high"]["weight_a"] == 10.0
    assert by_id(out["category_summary"])["category:network"]["weight_a"] == 10.0
    assert out["split_schema"] == SPLIT_SCHEMA
    crossed = joint_marginal(out["joint"], AXIS_SEVERITY, AXIS_CATEGORY)
    assert crossed["severity:high"]["category:network"][0] == 10.0


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print("ok  %s" % fn.__name__)
    print("all passed")
