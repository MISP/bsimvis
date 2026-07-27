# Release 0.4.0 - Pools

## Pools

A pool correlates several collections at once, with two modes:

* **Full cross-correlation** — every function in the pool is compared against
  every other, across and within member collections.
* **Cross-collection only** — set `only_cross_collection` in the pool config to
  keep just the pairs that span two different collections. Use this to identify
  known functions from a reference collection in another one, without paying for
  the intra-collection comparisons you already have.

Pool similarity, clustering, binary similarity and binary clustering all run as
one job pipeline, and pool results carry their own namespace so tags, notes and
clusters stay attached to the right level.

## Performance

* BUILD_SIM concurrent throughput went from **1,714 to 3,764 sim/s (2.2x)** on
  the reference run (60 files, 58,997 sims). Two root causes: an unbounded
  metadata cache that starved the GC, and a per-candidate `ZSCORE` loop in Lua.
  Candidate discovery is now pure Python, so it no longer serializes on the
  kvrocks global `EVAL` lock.
* **Concurrency scales now.** 10 workers finish that build in ~16 s where 1
  worker takes ~155 s. Before, 10 workers were *slower* than 1.
* Diff tables filter, sort and paginate server-side instead of shipping the
  whole table to the browser, plus a compact Sankey projection (`view=sankey`)
  for large diffs.
* File and function search order through the tag index; job statistics are
  fetched in a single pipeline instead of one round trip per job.
* Index jobs run as continuations so batches interleave rather than blocking
  each other.

See `data/bench_results/BUILD_SIM_perf_report.md` for the full breakdown.

## Stability

* Similarity scores are stable for a given set of parameters and no longer
  drift with collection size.
* Small functions fall back to Ghidra Function ID hash matching, where BSim
  produces false positives.
* The Ghidra JVM is bounded per worker instead of per host, so a heavy binary
  can no longer take the machine down with it.
* Job enqueue is idempotent — no more duplicate pipeline execution or
  double-fired pool builds.

## Tooling

* `bsimvis-bench` CLI for reproducible similarity and build benchmarks, with
  test data under `data/bench` and results under `data/bench_results`.
* An API test suite covering every endpoint, including cross-level filtering
  and sorting.

## Thanks

New contributors this release: Alexandre Dulaunoy and SegmondFault.
