"""
Standalone check for the same-class matching fix in similarity_service.py
(bsimvis/app/services/similarity_service.py, build_batch's LCA discovery stage).

Mirrors the ring-slice + bounded-heap logic added there: a vector-class of n
byte-identical functions all tie at score 1.0, so generating all n*(n-1) pairs
before trimming to top_k is wasted work that OOM's the worker on large
duplicate-function groups. This reproduces just the algorithm shape (not the
Redis-backed build_batch) to prove candidate counts stay bounded by top_k
regardless of group size.

Run: python3 test_lca_same_class_cap.py
"""
import heapq


def same_class_matches(funcs, top_k):
    """Ring-slice version: each function gets up to top_k partners, never all n-1."""
    n = len(funcs)
    cap = n - 1 if top_k <= 0 else min(top_k, n - 1)
    heaps = {}
    max_heap_size = 0
    total_calls = 0
    for idx, f1 in enumerate(funcs):
        for offset in range(1, cap + 1):
            f2 = funcs[(idx + offset) % n]
            total_calls += 1
            heap = heaps.setdefault(f1, [])
            entry = (1.0, f2, 0)
            if top_k <= 0 or len(heap) < top_k:
                heapq.heappush(heap, entry)
            elif entry > heap[0]:
                heapq.heapreplace(heap, entry)
            max_heap_size = max(max_heap_size, len(heap))
    return heaps, total_calls, max_heap_size


def demo():
    n, top_k = 5527, 1000
    funcs = [f"func:{i}" for i in range(n)]

    heaps, total_calls, max_heap_size = same_class_matches(funcs, top_k)

    # Every function ends up with exactly top_k candidates, not n-1.
    assert all(len(h) == top_k for h in heaps.values()), "candidate count not capped at top_k"
    assert max_heap_size == top_k, f"a heap grew past top_k: {max_heap_size}"

    # add_candidate call volume is O(n * top_k), not O(n^2) -- for n=5527 that's
    # ~5.5M vs. ~30.5M the old unbounded pairwise loop generated.
    assert total_calls == n * top_k, f"expected {n * top_k} calls, got {total_calls}"
    assert total_calls < n * n, "still doing O(n^2) work"

    # Small groups (below top_k) are unaffected -- every member still gets all others.
    small_top_k = 1000
    small_funcs = [f"s:{i}" for i in range(5)]
    small_heaps, _, _ = same_class_matches(small_funcs, small_top_k)
    assert all(len(h) == 4 for h in small_heaps.values()), "small group should get all n-1 partners"

    print(f"OK: n={n} top_k={top_k} -> {total_calls} calls (was {n*(n-1)} unbounded), "
          f"max per-function candidates={max_heap_size}")


if __name__ == "__main__":
    demo()
