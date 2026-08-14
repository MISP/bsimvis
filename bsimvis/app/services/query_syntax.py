"""Filter value syntax — one parser for every search route.

Search routes used to each carry their own copy of "SSCAN the registry with
`*value*`, then re-check with a Python `in`". Those copies drifted, and the
drift was silent: a bucket cap in two of them dropped documents out of filter
results with no error and no flag. This module is the single place a filter
value is turned into a match, so a fix lands everywhere at once.

Syntax
------
    func_tag=lib              exact bucket lookup (controlled-vocabulary field)
    func_tag=lib*             wildcard, anchored: starts with "lib"
    func_tag=*uclibc*         wildcard, anchored: contains "uclibc"
    return_type="DIR *"       quoted: the whole value is literal, `*` is data
    function_name=alloc       contains — free-text field, see SUBSTRING_FIELDS

The mode depends only on the field and on whether the value carries an
unquoted `*`. Nothing is inferred from what happens to exist in the index, so
the same query always resolves the same way and the chosen mode can be
reported back to the caller.

Users get `*` and nothing else. `?`, `[`, `]`, `<`, `>` are literal, so
`operator[]` and C++ template arguments need no escaping.
"""

import logging
import re

from bsimvis.app.services.index_config import (
    EXACT_FIELDS,
    SUBSTRING_FIELDS,
    tag_ancestors,
)

# Buckets returned for one filter value. Only wildcard/substring modes can
# reach it; an exact lookup returns a single bucket regardless of vocabulary
# size, which is what the hierarchical namespace buckets are for.
#
# Cost is linear and modest — 60k buckets measured at ~0.13s to scan and
# ~0.11s to union — so this is set high enough that a real corpus does not hit
# it, and truncation is reported when it does. Raise `search.max_filter_buckets`
# in bsimvis_config.toml if a vocabulary ever outgrows it.
#
# ponytail: the practical ceiling is not this number but SUNION blocking Redis
# for the duration; that is why the unions are chunked (see MAX_UNION_KEYS).
DEFAULT_MAX_TARGETS = 200000

# Keys per SUNION call. Redis is single-threaded, so one union over hundreds of
# thousands of keys stalls every other client for its whole duration. Chunking
# does not reduce the total work, it just lets other commands interleave. Match
# the chunk size the Lua search scripts already use.
MAX_UNION_KEYS = 5000


def _max_targets():
    from bsimvis.app.services.config_service import config_service

    try:
        return int(config_service.get("search.max_filter_buckets", DEFAULT_MAX_TARGETS))
    except (TypeError, ValueError):
        return DEFAULT_MAX_TARGETS


def union_buckets(r, keys):
    """SUNION over any number of keys, in chunks, as a set of decoded ids."""
    out = set()
    for i in range(0, len(keys), MAX_UNION_KEYS):
        chunk = keys[i : i + MAX_UNION_KEYS]
        if not chunk:
            continue
        for t in r.sunion(*chunk):
            out.add(t.decode() if isinstance(t, bytes) else str(t))
    return out


_GLOB_META = "\\*?[]^"


class MatchSpec:
    """A parsed filter value.

    kind:    "exact" | "glob" | "substring"
    value:   the literal value, quotes stripped and wildcards still in place
    glob:    pattern for SSCAN MATCH — a prefilter only, never the semantics
    regex:   authoritative matcher for "glob", None otherwise
    quoted:  whether the user quoted the value (reported back for the UI)
    """

    def __init__(self, kind, value, glob=None, regex=None, quoted=False):
        self.kind = kind
        self.value = value
        self.glob = glob
        self.regex = regex
        self.quoted = quoted

    def matches(self, bucket_value):
        if self.kind == "exact":
            return bucket_value == self.value
        if self.kind == "glob":
            return bool(self.regex.match(bucket_value))
        return self.value in bucket_value

    def as_dict(self):
        return {"mode": self.kind, "value": self.value, "quoted": self.quoted}


def _escape_glob(s):
    """Escape every Redis glob metacharacter.

    Needed even on the plain substring path: an unescaped `[` in a value makes
    SSCAN yield *fewer* buckets than the literal substring would match, and the
    Python re-check can only narrow the result, never recover a bucket SSCAN
    never handed back. That was a silent-miss bug of its own.
    """
    out = []
    for ch in s:
        if ch in _GLOB_META:
            out.append("\\")
        out.append(ch)
    return "".join(out)


def _unquote(raw):
    """Strip surrounding double quotes. Returns (value, was_quoted)."""
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        return raw[1:-1].replace('\\"', '"'), True
    return raw, False


def parse_filter_value(field, raw, default_kind="exact"):
    """Parse one filter value into a MatchSpec. `raw` is the value as typed.

    default_kind is what a wildcard-free value on a controlled-vocabulary field
    means. Named filters want "exact"; the free-text `q` box passes "substring"
    so that a bare word still searches broadly. Either way an explicit `*` wins,
    so `q=lib*` is a wildcard everywhere.
    """
    value, quoted = _unquote(raw.strip())
    value = value.lower()

    if not quoted and "*" in value:
        parts = value.split("*")
        return MatchSpec(
            kind="glob",
            value=value,
            glob="*".join(_escape_glob(p) for p in parts),
            regex=re.compile("^" + ".*".join(re.escape(p) for p in parts) + "$"),
        )

    kind = default_kind
    if field in EXACT_FIELDS:
        kind = "exact"
    elif field in SUBSTRING_FIELDS:
        kind = "substring"

    if kind == "substring":
        return MatchSpec(
            kind="substring",
            value=value,
            glob="*" + _escape_glob(value) + "*",
            quoted=quoted,
        )

    return MatchSpec(kind="exact", value=value, quoted=quoted)


def resolve_targets(r, col, level, field, raw, max_targets=None, default_kind="exact"):
    """Bucket values matching one filter value.

    Returns (targets, truncated, spec). `targets` are bucket suffixes — the part
    after `{col}:idx:{level}:{field}:` — which is what the Lua search scripts
    expect. `truncated` is True when the vocabulary scan hit `max_targets`, in
    which case the result is a subset and the caller must say so.
    """
    spec = parse_filter_value(field, raw, default_kind=default_kind)
    if max_targets is None:
        max_targets = _max_targets()
    registry_key = f"{col}:reg:{level}:{field}"
    prefix = f"{col}:idx:{level}:{field}:"

    if not r.exists(registry_key):
        return [], False, spec

    if spec.kind == "exact":
        # Hierarchical ancestors are indexed as real buckets at write time, so
        # `func_tag=lib` is this branch too: one lookup, no vocabulary scan.
        bucket = prefix + spec.value
        return ([spec.value] if r.sismember(registry_key, bucket) else []), False, spec

    targets = []
    truncated = False
    # Anchor the scan on the bucket prefix so a value can never match against
    # the collection or field part of the key. The prefix is data too (a
    # collection may be named with a `[`), so it gets escaped as well.
    match_pat = _escape_glob(prefix) + spec.glob
    try:
        for bucket in r.sscan_iter(registry_key, match=match_pat, count=1000):
            bucket_str = bucket.decode() if isinstance(bucket, bytes) else str(bucket)
            if not bucket_str.startswith(prefix):
                continue
            bucket_value = bucket_str[len(prefix) :]
            # SSCAN's glob is a prefilter; this is what actually decides.
            if not spec.matches(bucket_value.lower()):
                continue
            targets.append(bucket_value)
            if len(targets) >= max_targets:
                truncated = True
                logging.warning(
                    "FILTER TRUNCATED | %s:%s=%s hit %d buckets",
                    level,
                    field,
                    spec.value,
                    max_targets,
                )
                break
    except Exception as e:
        logging.warning(f"SSCAN failed for {registry_key}: {e}")

    return _drop_covered(field, targets), truncated, spec


def _drop_covered(field, targets):
    """Drop targets whose ancestor is also in the list.

    A wildcard over a hierarchical field matches the synthetic ancestor buckets
    as well as the leaves under them (`lib:uclibc:*` hits `lib:uclibc:0.9.30.1`
    and every symbol beneath it). An ancestor bucket holds every member of its
    descendants by construction, so the descendants add nothing to the union —
    dropping them keeps the result identical and cuts the key count handed to
    SUNION.
    """
    if len(targets) < 2:
        return targets
    present = set(targets)
    return [
        t for t in targets if not any(a in present for a in tag_ancestors(field, t))
    ]
