"""Shared in-memory filter/sort/paginate helpers for the small registry list
endpoints (collections, pools). These registries hold at most ~1k items, so we
hydrate all, then filter/sort/paginate in Python — no Redis secondary index.

ponytail: in-memory is correct at this cardinality. If pool/collection count
grows well past 1k and per-request hydration gets hot, add ZSET date indexes
and page-before-hydrate.

Each request-reading helper takes an optional `args` mapping (defaults to the
Flask request args) so the logic is testable without a request context.
"""


def _args(args):
    if args is not None:
        return args
    from flask import request

    return request.args


def keywords(args=None):
    """Whitespace-split lowercased tokens from the `q` query param."""
    q = _args(args).get("q", "").lower().strip()
    return [k for k in q.split() if k]


def matches_keywords(kws, *fields):
    """True if every keyword is a substring of at least one of `fields`."""
    hay = " ".join(str(f).lower() for f in fields if f is not None)
    return all(kw in hay for kw in kws)


def num_range(field, args=None):
    """(min, max) floats for `field` from ?min_<field>/?max_<field>, or None ends.
    Returns None if neither bound supplied."""
    a = _args(args)
    lo = a.get(f"min_{field}")
    hi = a.get(f"max_{field}")
    if lo is None and hi is None:
        return None
    try:
        lo_f = float(lo) if lo not in (None, "") else float("-inf")
        hi_f = float(hi) if hi not in (None, "") else float("inf")
    except ValueError:
        return None
    return (lo_f, hi_f)


def in_range(val, rng):
    if rng is None:
        return True
    try:
        v = float(val)
    except (ValueError, TypeError):
        return False
    return rng[0] <= v <= rng[1]


def sort_and_paginate(
    items, offset, limit, default_key, default_reverse, key_fns, args=None
):
    """Sort `items` (list of dicts) by ?sort_by/?sort_order then slice.

    key_fns: {field_name: callable(item)->comparable}. `sort_by` must be a key in
    key_fns to take effect, else falls back to default_key.
    Returns (page, total).
    """
    a = _args(args)
    total = len(items)
    sort_by = a.get("sort_by") or default_key
    order = a.get("sort_order")
    if order:
        reverse = order.lower() == "desc"
    else:
        reverse = default_reverse

    keyfn = key_fns.get(sort_by, key_fns.get(default_key))
    if keyfn:
        items = sorted(items, key=keyfn, reverse=reverse)
    return items[offset : offset + limit], total


def _selfcheck():
    # keywords + matches
    assert keywords({"q": " Foo  BAR "}) == ["foo", "bar"]
    assert matches_keywords(["ab"], "xxAByy", None)
    assert not matches_keywords(["zz"], "ab")

    # num_range + in_range
    assert num_range("files", {}) is None
    assert num_range("files", {"min_files": "10", "max_files": "20"}) == (10.0, 20.0)
    r = num_range("files", {"min_files": "10"})
    assert r == (10.0, float("inf"))
    assert in_range(15, r) and not in_range(5, r)
    assert in_range(1, None)  # no range = pass
    assert not in_range("x", (0, 5))  # non-numeric fails a real range

    # sort desc + paginate
    items = [{"n": v} for v in [3, 1, 2, 5, 4]]
    keyf = {"n": lambda d: d["n"]}
    page, total = sort_and_paginate(
        items, 0, 2, "n", False, keyf, {"sort_by": "n", "sort_order": "desc"}
    )
    assert total == 5
    assert [d["n"] for d in page] == [5, 4], page
    # offset + default order
    page2, _ = sort_and_paginate(items, 2, 2, "n", False, keyf, {})
    assert [d["n"] for d in page2] == [3, 4], page2
    print("ok")


if __name__ == "__main__":
    _selfcheck()
