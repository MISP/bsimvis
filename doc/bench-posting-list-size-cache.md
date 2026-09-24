# Posting-list size cache benchmark

Measured 2026-09-24 in the linked worktree on an isolated Kvrocks instance.
The corpus has three `versioned_c` Linux builds (`v01`, `v05`, `v10`) and one
existing benchmark fixture as unrelated noise: 338 functions total.

The benchmark called `_discover_find` for every indexed function at a 0.9
threshold, with top-k 1000. Each trial reset the per-build caches before
walking all targets, preserving cache reuse across targets within that build.
Two trials ran per algorithm for both baseline and cached-size code. Times are
median wall time; candidate matches are summed across targets.

## Results

| algorithm | baseline | cached sizes | improvement | candidate matches (both) |
|---|---:|---:|---:|---:|
| jaccard | 1.049 s | 0.932 s | 11% | 1,126 |
| unweighted cosine | 1.614 s | 1.174 s | 27% | 1,200 |
| binary cosine | 1.187 s | 0.881 s | 26% | 1,154 |
| weighted cosine | 3.344 s | 2.881 s | 14% | 1,152 |

The change reads a posting-list length from the existing posting-list cache.
It pipelines `ZCARD` only for features whose lists are not cached. Candidate
counts stayed identical in every comparison. This small corpus and two trials
show a positive signal; timings are not a production-scale guarantee.

Reproduce the fixture ingestion with:

```sh
uv run bsimvis-bench --dir data/bench/versioned_c --collection versioned_fixture_smoke \
  --algo unweighted_cosine --min-score 0.9 --sequential --skip-write
```

`versioned_c/` contains the three analyzed version fixtures and a symlink to
the existing noise fixture, avoiding another copy of its 41 MB JSON file.
