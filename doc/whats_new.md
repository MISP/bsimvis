BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

# New features

This new version focuses on cross-collection analysis via pools, significant performance and concurrency scaling improvements for similarity builds, stability enhancements, and robust testing tools.

# Screenshots

![binary similarity diffing view](/img/binary_diff.png)
*Interactive side-by-side binary diffing and function matching view.*


## Pools
* **Full cross-correlation** — matches what happens in collections, saving similarities across all collections and within them.
* **Cross-collection only** — set `only_cross_collection` in the pool config to keep just the pairs that span two different collections. Use this to identify known functions from a reference collection in another one, without paying for the intra-collection comparisons you already have.

## Performance

![BUILD_SIM Optimization Performance](/img/benchmark/master_build_sim_perf.png)
*Concurrent build throughput improvements across optimization stages (left) and build timeline comparing the baseline against the optimized run (right).*

* **Faster Similarity Builds** — improved concurrent throughput by optimizing candidate discovery to avoid database serialization.
* **Concurrency scaling** — multiple workers speed up builds rather than slowing them down, allowing builds to scale efficiently.
* **Binary Similarity** — server-side pagination, sorting, and filtering for diff tables, along with a Sankey visualization mode.
* **Search** — faster tag-indexed search sorting.

## Stability
* Similarity scores are stable for a given set of parameters and no longer drift with collection size.
* Small functions fall back to Ghidra Function ID hash matching, where BSim produced false positives.
* Improvements to JVM memory management to prevent crashes.
* Job system improvements and fixes.

## Tooling
* `bsimvis-bench` CLI for reproducible similarity and build benchmarks, with test data under `data/bench` and results under `data/bench_results`.
* An API test suite covering every endpoint, including cross-level filtering and sorting.

# New Contributor
* @SegmondFault made their first contribution
