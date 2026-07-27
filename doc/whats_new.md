# Release 0.4.0 - Pools

## Pools

A pool correlates several collections at once, with two modes:

* **Full cross-correlation** — matches what happens in collections.
* **Cross-collection only** — set `only_cross_collection` in the pool config to
  keep just the pairs that span two different collections. Use this to identify
  known functions from a reference collection in another one, without paying for
  the intra-collection comparisons you already have.

Pool similarity, clustering, binary similarity and binary clustering all run as
one job pipeline, and pool results carry their own namespace so tags, notes and
clusters stay attached to the right level.

## Performance

* Significant throughput improvements for concurrent similarity builds (`BUILD_SIM`). This was optimized by addressing two root causes: an unbounded metadata cache that starved the GC, and a per-candidate `ZSCORE` loop in Lua. Candidate discovery is now pure Python, so it no longer serializes on the kvrocks global `EVAL` lock.
* **Concurrency scales now.** Multiple workers speed up the build rather than slowing it down, allowing builds to scale efficiently with the number of workers.
* Diff tables filter, sort and paginate server-side instead of shipping the
  whole table to the browser, plus a compact Sankey projection (`view=sankey`)
  for large diffs.
* File and function search order through the tag index; job statistics are
  fetched in a single pipeline instead of one round trip per job.
* Index jobs run as continuations so batches interleave rather than blocking
  each other.

## Stability

* Similarity scores are stable for a given set of parameters and no longer
  drift with collection size.
* Small functions fall back to Ghidra Function ID hash matching, where BSim
  produces false positives.
* Improvements to JVM memory management to prevent crashes.
* Job system improvements and fixes.

## Tooling

* `bsimvis-bench` CLI for reproducible similarity and build benchmarks, with
  test data under `data/bench` and results under `data/bench_results`.
* An API test suite covering every endpoint, including cross-level filtering
  and sorting.

## Thanks

New contributors this release: Alexandre Dulaunoy and SegmondFault.
