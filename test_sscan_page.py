#!/usr/bin/env python3
"""Check _sscan_page returns stable, non-overlapping id-sorted pages.

Usage: uv run python test_sscan_page.py
"""

import random

from bsimvis.app.routes.search_function import SORT_SCAN_CAP, _sscan_page


class FakeRedis:
    """SSCAN that reshuffles every traversal, like a real unordered set."""

    def __init__(self, members):
        self.members = list(members)

    def sscan(self, key, cursor=0, count=10):
        if cursor == 0:
            self.order = random.sample(self.members, len(self.members))
        batch = self.order[cursor : cursor + count]
        nxt = cursor + count
        return (0 if nxt >= len(self.order) else nxt), batch


def test_pages_are_sorted_and_disjoint():
    ids = [f"main:func:{i:04d}" for i in range(250)]
    r = FakeRedis(ids)

    page1, trunc1 = _sscan_page(r, "main:all_functions", 0, 100)
    page2, _ = _sscan_page(r, "main:all_functions", 100, 100)

    assert not trunc1
    assert page1 == sorted(ids)[:100]
    assert page2 == sorted(ids)[100:200]
    assert not set(page1) & set(page2), "pages must not overlap"
    # same request twice -> same page, even though scan order changed
    assert _sscan_page(r, "main:all_functions", 0, 100)[0] == page1


def test_truncation_is_flagged():
    r = FakeRedis([f"main:func:{i:07d}" for i in range(SORT_SCAN_CAP + 500)])
    page, truncated = _sscan_page(r, "main:all_functions", 0, 10)
    assert truncated
    assert len(page) == 10


if __name__ == "__main__":
    test_pages_are_sorted_and_disjoint()
    test_truncation_is_flagged()
    print("ok")
