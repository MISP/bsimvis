"""Self-check for filter value syntax and hierarchical tag buckets.

Covers the two rules that decide whether a filter returns the right set:
quoted values are literal (so a `DIR *` return type is not a wildcard), and
hierarchical tags index their ancestors (so `func_tag=lib` is one bucket
lookup rather than a scan that used to be silently capped).

Run: python3 scripts/test_query_syntax.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.index_config import tag_ancestors
from bsimvis.app.services.query_syntax import (
    MAX_UNION_KEYS,
    parse_filter_value,
    resolve_targets,
    union_buckets,
)


class FakeRedis:
    """Set-only Redis good enough for registry/bucket resolution."""

    def __init__(self, sets):
        self.sets = {k: set(v) for k, v in sets.items()}

    def exists(self, key):
        return 1 if key in self.sets else 0

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def sscan_iter(self, key, match=None, count=None):
        # Deliberately ignores `match`: MATCH is only ever a prefilter, so a
        # correct implementation must not depend on it narrowing anything.
        return iter(sorted(self.sets.get(key, set())))


def _index(sets, col, level, field, value, doc):
    """Mirror of index_service._index_tag: value plus every ancestor."""
    reg = f"{col}:reg:{level}:{field}"
    for bucket_value in [value] + tag_ancestors(field, value):
        key = f"{col}:idx:{level}:{field}:{bucket_value}"
        sets.setdefault(key, set()).add(doc)
        sets.setdefault(reg, set()).add(key)


def demo():
    # --- parsing -----------------------------------------------------------
    # Pointer types are real values, not patterns.
    spec = parse_filter_value("return_type", '"DIR *"')
    assert spec.kind == "exact" and spec.value == "dir *", spec.value
    assert spec.quoted is True

    # Unquoted star is a wildcard, anchored at both ends.
    spec = parse_filter_value("tags", "lib*")
    assert spec.kind == "glob"
    assert spec.matches("lib:uclibc:seekdir")
    assert not spec.matches("zlib:foo"), "prefix wildcard must not match mid-string"

    spec = parse_filter_value("tags", "*uclibc*")
    assert spec.kind == "glob"
    assert spec.matches("lib:uclibc:seekdir")
    assert not spec.matches("lib:musl:seekdir")

    # Glob metacharacters other than `*` stay literal — C++ names survive.
    spec = parse_filter_value("function_name", "operator[]")
    assert spec.kind == "substring"
    assert spec.matches("std::operator[]")

    # Wildcard-free value: exact on controlled vocabulary, contains on free
    # text, and `q` can ask for contains everywhere.
    assert parse_filter_value("tags", "lib").kind == "exact"
    assert parse_filter_value("function_name", "alloc").kind == "substring"
    assert parse_filter_value("tags", "lib", default_kind="substring").kind == (
        "substring"
    )

    # --- ancestors ---------------------------------------------------------
    assert tag_ancestors("tags", "lib:uclibc:0.9.30.1:seekdir") == [
        "lib",
        "lib:uclibc",
        "lib:uclibc:0.9.30.1",
    ]
    assert tag_ancestors("tags", "mirai") == []
    # Non-hierarchical fields are left alone.
    assert tag_ancestors("function_name", "a:b") == []

    # Function namespaces mix `::`, `/` and `.` inside a single value.
    assert tag_ancestors("namespace", "crypto/elliptic::crypto/elliptic.initP256") == [
        "crypto",
        "crypto/elliptic",
        "crypto/elliptic::crypto",
        "crypto/elliptic::crypto/elliptic",
    ]
    # `::` wins over a bare `:` — splitting must not produce empty segments.
    assert tag_ancestors("namespace", "std::vector") == ["std"]
    assert tag_ancestors("namespace", "main") == []
    # Every ancestor is a genuine prefix of the value, separators included.
    value = "a/b::c.d"
    for ancestor in tag_ancestors("namespace", value):
        assert value.startswith(ancestor), ancestor

    # --- resolution against an index --------------------------------------
    sets = {}
    for i, leaf in enumerate(
        [
            "lib:uclibc:0.9.30.1:seekdir",
            "lib:uclibc:0.9.30.1:telldir",
            "lib:musl:strlen",
            "malware:mirai",
        ]
    ):
        _index(sets, "main", "func", "tags", leaf, f"main:func:aa:{i}")
    r = FakeRedis(sets)

    # The whole point: a namespace filter is one exact bucket, no scan.
    targets, truncated, spec = resolve_targets(r, "main", "func", "tags", "lib")
    assert targets == ["lib"], targets
    assert spec.kind == "exact" and not truncated

    targets, _, _ = resolve_targets(r, "main", "func", "tags", "lib:uclibc")
    assert targets == ["lib:uclibc"], targets

    # A wildcard still covers every leaf under the namespace, but collapses to
    # the ancestor bucket that already holds all of them.
    targets, _, spec = resolve_targets(r, "main", "func", "tags", "lib:uclibc:*")
    assert spec.kind == "glob"
    assert targets == ["lib:uclibc:0.9.30.1"], targets

    # Collapsing must not lose a leaf that has no matched ancestor.
    targets, _, _ = resolve_targets(r, "main", "func", "tags", "lib:*:s*")
    assert sorted(targets) == [
        "lib:musl:strlen",
        "lib:uclibc:0.9.30.1:seekdir",
    ], targets

    # `lib` must not drag in a tag that merely contains the letters.
    _index(sets, "main", "func", "tags", "zlib:deflate", "main:func:aa:9")
    r = FakeRedis(sets)
    targets, _, _ = resolve_targets(r, "main", "func", "tags", "lib")
    assert targets == ["lib"], targets

    # Truncation is reported, never silent — this was the original bug.
    # (The returned count can be below the cap: covered descendants collapse
    # into their ancestor after the scan stops.)
    targets, truncated, _ = resolve_targets(
        r, "main", "func", "tags", "*", max_targets=2
    )
    assert truncated is True, "hitting the bucket cap must be reported"
    assert targets, targets

    targets, truncated, _ = resolve_targets(r, "main", "func", "tags", "*")
    assert truncated is False

    # An unknown value resolves to nothing rather than to everything.
    targets, _, _ = resolve_targets(r, "main", "func", "tags", "nosuchtag")
    assert targets == []

    # union_buckets must chunk, and must return the same set either way.
    class CountingRedis(FakeRedis):
        def __init__(self, sets):
            super().__init__(sets)
            self.union_calls = 0
            self.max_keys_per_call = 0

        def sunion(self, *keys):
            self.union_calls += 1
            self.max_keys_per_call = max(self.max_keys_per_call, len(keys))
            out = set()
            for k in keys:
                out |= self.sets.get(k, set())
            return out

    n = MAX_UNION_KEYS * 2 + 5
    big = {"k%d" % i: {"doc%d" % i} for i in range(n)}
    cr = CountingRedis(big)
    got = union_buckets(cr, list(big))
    assert len(got) == n, len(got)
    assert cr.union_calls == 3, cr.union_calls
    assert cr.max_keys_per_call <= MAX_UNION_KEYS, cr.max_keys_per_call
    assert union_buckets(cr, []) == set()

    print("query syntax self-check: ok")


if __name__ == "__main__":
    demo()
