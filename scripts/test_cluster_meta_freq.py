#!/usr/bin/env python3
"""Self-check for collect_member_values / build_freq (cluster_utils.py).

The block that turns member file metadata into a cluster's distributions was
copied into cluster_service, bin_cluster_service (twice) and metadata_service,
and the copies drifted: the recalc path divided each count by len(items) -- the
flattened value list -- while the three clustering paths divided by the member
count, so the same `{collection}:bin_cluster:{algo}:{cid}:meta` key reported
different percents depending on which writer ran last. This pins the
member-count denominator and the scalar-or-list coercion.

No redis, no fixtures: pure functions over a list of meta dicts.
Run: uv run python scripts/test_cluster_meta_freq.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.cluster_utils import (  # noqa: E402
    build_freq,
    collect_member_values,
    cluster_summary,
    function_count_stats,
)

# Two members, three yara hits between them: "rule_a" is carried by both files,
# so it describes 100% of the cluster. The old len(items) denominator divided by
# 3 and reported 67%.
MEMBERS = [
    {
        "file_names": ["dropper.exe", "dropper.exe"],
        "file_md5": "aaaa",
        "yara": ["rule_a", "rule_b"],
        "avtype": "Emotet",
        "filetype": "PE32",
        "cc_ip": ["10.0.0.1"],
    },
    {
        "file_name": "payload.bin",
        "file_md5": "bbbb",
        "yara": "rule_a",
        "avtype": ["Emotet"],
        "filetype": ["PE32"],
    },
]


def test_percent_is_a_share_of_members():
    _, _, yara_list, _, _, _ = collect_member_values(MEMBERS)
    freq = build_freq(yara_list, len(MEMBERS))
    assert freq[0] == {"value": "rule_a", "count": 2, "percent": 100}, freq
    assert freq[1] == {"value": "rule_b", "count": 1, "percent": 50}, freq


def test_flattened_lists_are_collected_once_per_value():
    names, md5s, yara, avtype, filetype, ccip = collect_member_values(MEMBERS)
    assert names == ["dropper.exe", "dropper.exe", "payload.bin"], names
    assert md5s == ["aaaa", "bbbb"], md5s
    assert yara == ["rule_a", "rule_b", "rule_a"], yara
    assert avtype == ["Emotet", "Emotet"], avtype
    assert filetype == ["PE32", "PE32"], filetype
    assert ccip == ["10.0.0.1"], ccip


def test_file_names_wins_over_file_name():
    meta = {"file_names": ["a.exe"], "file_name": "b.exe"}
    names = collect_member_values([meta])[0]
    assert names == ["a.exe"], names


def test_all_values_are_kept_unless_a_limit_is_given():
    items = [f"v{i}" for i in range(6)]
    freq = build_freq(items, 6)
    assert len(freq) == 6, freq
    limited = build_freq(items, 6, limit=5)
    assert len(limited) == 5, limited


def test_nothing_collected_gives_an_empty_distribution():
    assert build_freq([], 3) == []


def test_tag_distribution_is_per_member_and_policy_filtered():
    members = [
        {"tags": ["av:clamav:mirai#rule_a", "ip:1.2.3.4", "rulezet:uuid-a"]},
        {"tags": ["av:clamav:mirai#rule_b"], "user_tags": ["reviewed"]},
    ]
    result = cluster_summary(members)

    # Each axis is a tree of {tag_id, count, children} nodes.
    def flatten(nodes):
        for node in nodes:
            yield node
            yield from flatten(node.get("children", []))

    family = {
        row["tag_id"]: row for row in flatten(result["tag_distribution"]["family"])
    }
    assert "av:clamav:mirai" in family, family
    assert family["av:clamav:mirai"]["count"] == 2
    assert "ioc" not in result["tag_distribution"]
    assert "ruleset" not in result["tag_distribution"]
    assert result["tag_distribution"]["user"][-1]["tag_id"] == "user:reviewed"


def test_function_count_spread_over_members():
    stats = function_count_stats(
        [{"function_count": 10}, {"function_count": 200}, {"function_count": 90}]
    )
    assert stats == {"min": 10, "avg": 100.0, "max": 200, "files": 3}, stats


def test_members_without_a_function_count_do_not_become_zeroes():
    # A file whose meta predates function_count would otherwise drag every
    # cluster's minimum to 0 and halve its average.
    stats = function_count_stats([{"function_count": 40}, {"file_name": "x.elf"}, {}])
    assert stats == {"min": 40, "avg": 40.0, "max": 40, "files": 1}, stats


def test_no_member_reports_a_function_count():
    assert function_count_stats([{"file_name": "x.elf"}]) == {}
    assert function_count_stats([]) == {}


if __name__ == "__main__":
    test_percent_is_a_share_of_members()
    test_flattened_lists_are_collected_once_per_value()
    test_file_names_wins_over_file_name()
    test_all_values_are_kept_unless_a_limit_is_given()
    test_nothing_collected_gives_an_empty_distribution()
    test_tag_distribution_is_per_member_and_policy_filtered()
    test_function_count_spread_over_members()
    test_members_without_a_function_count_do_not_become_zeroes()
    test_no_member_reports_a_function_count()
    print("OK")
