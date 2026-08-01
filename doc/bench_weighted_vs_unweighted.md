# Weighted vs unweighted BSim scoring, at corpus scale

Follow-up to the small comparison posted on MISP/bsimvis#50, which ran on 21 and
20 shared function names. At that size one comparison saturated at 100% recall
for all three algorithms and a single function moved recall by five points.

This one runs on the benchmark corpus described in [bench_corpus.md](bench_corpus.md):
180 cross-compiled open-source binaries, 313,685 defined functions, with
unrelated programs on the reference side so a wrong top-1 is a real false
positive rather than a same-program near-miss.

Everything below is reproducible from that document; nothing is hand-selected.

## Results at a glance

| Question | Answer |
|:--|:--|
| Does weighting retrieve better? | Yes: **86.4% vs 80.3%** recall@1 over unweighted, and **+11.7 points** on the hardest axis |
| Does it beat jaccard? | Roughly a tie overall (86.4% vs 86.2%), ahead where it matters (optimisation changes) |
| Does it cost more? | **1.16–1.19x** unweighted per pair, and ~parity on large vectors |
| Can it run in the pipeline? | **No** — the build path rejects it; exact-score path only |
| Is Ghidra's shipped weight table valid here? | Yes — **0/50** of the corpus's most common features are missing from it |
| How does Ghidra's own BSim compare? | **12.9%** of true matches never survive its LSH candidate selection |

## 1. Retrieval quality

`scripts/bench/quality.py`, 46 binaries extracted so far, 166 build pairs,
18,239 queried functions, median reference pool 1,624 functions. Each pair
differs in exactly one build coordinate, and the reference pool always contains
every other project's binary of the same variant.

| axis / algo | recall@1 | recall@5 | MRR | true min | false max | sep | xprog fp |
|:--|--:|--:|--:|--:|--:|--:|--:|
| **all** jaccard | 86.2% | 93.5% | 0.896 | 0.1408 | 0.8388 | 84.2% | 2.89% |
| **all** unweighted_cosine | 80.3% | 88.2% | 0.841 | 0.1871 | 0.9595 | 78.5% | 4.00% |
| **all** weighted_cosine | **86.4%** | **93.9%** | **0.899** | 0.1723 | 0.8853 | **84.6%** | **2.59%** |
| arch jaccard | 89.2% | 95.4% | 0.921 | 0.0706 | 0.7912 | 87.8% | 1.41% |
| arch unweighted_cosine | 84.0% | 90.9% | 0.872 | 0.1229 | 0.9451 | 82.8% | 2.25% |
| arch weighted_cosine | 89.0% | 95.4% | 0.920 | 0.1048 | 0.8477 | 87.7% | 1.38% |
| opt jaccard | 73.5% | 86.0% | 0.794 | 0.0113 | 0.8865 | 70.3% | 7.50% |
| opt unweighted_cosine | 63.3% | 76.3% | 0.696 | 0.0203 | 0.9779 | 60.3% | 9.75% |
| opt weighted_cosine | **75.0%** | **87.5%** | **0.810** | 0.0222 | 0.9260 | **71.9%** | **6.44%** |
| link jaccard | 97.8% | 99.5% | 0.986 | 0.7190 | 0.9597 | 95.8% | 0.42% |
| link unweighted_cosine | 97.7% | 99.5% | 0.986 | 0.8128 | 0.9885 | 95.8% | 0.42% |
| link weighted_cosine | 97.8% | 99.6% | 0.987 | 0.7795 | 0.9746 | 95.8% | 0.42% |

`sep` = the true match outscored every wrong candidate. `xprog fp` = the top-1
came from a different program entirely.

What the axes say, which an averaged number would have hidden:

- **Optimisation level is the hard case, and it is where weighting earns its
  keep.** O0 vs O2 costs unweighted cosine 20 points of recall (84.0% -> 63.3%);
  weighting recovers 11.7 of them. Different inlining and scheduling change which
  features survive, and down-weighting the common ones is exactly the right
  correction.
- **Linkage is not a discriminator.** Static vs dynamic scores ~97.8% for
  everything: the shared functions are byte-identical, only the libc around them
  differs. A benchmark reporting only this axis would conclude all three
  algorithms are equivalent.
- **Weighting cuts false positives.** Cross-program top-1 hits drop from 4.00% to
  2.59% overall, and 9.75% to 6.44% on the optimisation axis — a third fewer
  wrong-program matches. This is the number the small dataset could not produce
  at all, because it had no second program.
- **Jaccard is not the weak baseline it is often assumed to be.** It ties
  weighted cosine overall and beats unweighted everywhere. Any argument for
  switching the default has to clear jaccard, not just unweighted cosine.

`true min` moving up (0.1871 -> 0.1723 overall, but 0.0203 -> 0.0222 on the opt
axis) reproduces the effect reported in #50 at 20 functions: weighting lifts the
weakest true match. It is a small effect and it is not the reason recall improves.

## 2. Scoring cost

`scripts/bench/scoring_cost.py` on real corpus vectors (lua x64 vs arm64, 60
functions per side, 3,600 pairs). Per-function work — coefficients, norms,
lengths — is precomputed, because it is O(n) index-time work; charging it to the
O(n²) pair loop overstates the weighted penalty several times over.

| vector size | unweighted | weighted (sim) | weighted (sim + significance) |
|:--|--:|--:|--:|
| real (~20 feats) | 1.86 µs/pair | 2.22 µs (**1.19x**) | 2.40 µs (1.29x) |
| ~120 feats | 8.94 µs/pair | 10.37 µs (**1.16x**) | 10.64 µs (1.19x) |
| ~400 feats | 35.53 µs/pair | 34.14 µs (**0.96x**) | 33.80 µs (0.95x) |

Weighting is a constant-factor tax that vanishes on large vectors. The optimised
loop matches the reference `compare()` to `max|dsim| = 7.8e-16`, including the
min-tf branch.

## 3. Pipeline throughput

`scripts/bench/pipeline_bench.py` against a live stack (3 workers), 4 binaries /
5,709 functions ingested, one build job per binary.

| algo | wall | similarities written | sims/s |
|:--|--:|--:|--:|
| jaccard | 34.2 s | 6,359 | 186 |
| unweighted_cosine | 80.4 s | 34,568 | 430 |
| weighted_cosine | — | — | rejected by the build path |

The two are not doing equal work: at the same threshold cosine admits 5.4x more
pairs, so its higher wall time buys more output. Per similarity written, cosine
is the cheaper of the two.

Ingest, measured separately because no scoring algorithm changes it: **12
binaries in 4,991 s** with 2 parallel Ghidra processes — about 7 minutes per
binary, dominated entirely by decompilation. Any scoring-side change is noise
against that.

**`weighted_cosine` cannot be benchmarked end to end.** The build path
(`similarity_service.py`, the inverted-index candidate walk) branches only on
`jaccard` and `unweighted_cosine`; the API refuses the algorithm rather than
falling through and producing unfiltered results. So the quality numbers above
come from offline scoring, which is exactly why `quality.py` does not need the
stack. Landing weighting in the build path is a prerequisite for a real
throughput comparison.

## 4. Is Ghidra's weight table even applicable here?

`scripts/bench/idf_coverage.py bench_corpus` — the go/no-go gate from #50, run
for the first time against a real collection:

```
distinct hashes covered : 908/78654 (1.15%)
OCCURRENCES covered     : 81858/237201 (34.51%)
0/50 of this collection's most common features are ABSENT from Ghidra's table
```

A feature absent from the 1000-entry `idflookup` resolves to index 0, the
*largest* weight — so missing boilerplate would be amplified rather than
suppressed. Every one of this corpus's 50 most common features is present, and a
third of all feature occurrences are covered. For a GCC-compiled C corpus,
Ghidra's shipped table is a reasonable fit.

This does **not** transfer to the production corpus, which is largely Go on
MIPS/ARM/SH4/m68k. Run the same gate there before drawing any conclusion about
it; that was the open question in #50 and it stays open for that corpus.

## 5. Ghidra's own BSim, as a baseline

`scripts/bench/bsim_baseline.py` builds a real Ghidra BSim H2 database from the
x86-64 builds, commits signatures, and queries the ARM64 builds through Ghidra's
own client (`support/bsim`, `support/analyzeHeadless`, top 10 matches per
function). Same binaries, same symbol-name ground truth.

| query binary | true match in window | recall@1 | recall@5 | MRR |
|:--|--:|--:|--:|--:|
| lua-linux-arm64-O2-dyn | 84.7% | 77.3% | 83.4% | 0.799 |
| zlib-linux-arm64-O2-dyn | 89.6% | 73.4% | 85.7% | 0.787 |
| **mean** | **87.1%** | **75.3%** | 84.6% | 0.793 |

The interesting column is the first one. **12.9% of true matches never appear in
BSim's results at all** — they were eliminated by LSH binning before any score
was computed. That is the cost of Ghidra's candidate-selection stage, and it is
invisible to `oracle_compare.py`, which proves our arithmetic matches Ghidra's
but says nothing about which candidates a real BSim query returns.

BSimVis scores every candidate, so it has no such ceiling: on the cross-architecture
axis it reaches 89.0% recall@1 (weighted). The comparison is indicative rather
than exact — our figure averages all five projects and both linkages, this one is
two projects — but the direction is structural, not noise: you cannot rank a
candidate that was never retrieved.

Nothing here is wired into BSimVis. The database, the client and the query script
all live under `scripts/bench/`, driven from the Ghidra install already in `bin/`.

## 6. What this does not show

- **Weighting is measured offline.** The scores are computed by the same
  transcribed arithmetic the exact-score path uses (`bsim_weights.compare`,
  verified bit-exact against Ghidra by `oracle_compare.py`), but no weighted
  build has ever run. Candidate *selection* in the build path could change the
  picture.
- **46 of 180 binaries.** Extraction of the remainder is Ghidra-bound and still
  running; the arch axis is well covered, the opt axis less so. Numbers will move
  a little, not qualitatively.
- **One compiler family.** GCC 13 everywhere, mingw-w64 for Windows. Clang or
  MSVC would be a genuinely harder axis.
- **Ground truth is symbol names,** so inlined-away functions count as misses.
- **Throughput ran on 4 binaries,** enough to separate the algorithms but not to
  characterise scaling. The O(n²) build cost grows with collection size, which
  this does not sample.

## Reproducing

```bash
scripts/bench/corpus/build_corpus.sh && scripts/bench/corpus/manifest.py --symbols
scripts/bench/corpus/extract.py --jobs 4          # hours, Ghidra-bound, resumable
scripts/bench/quality.py --sample 120 --report quality.json
VECS=vecs.json scripts/bench/scoring_cost.py
scripts/bench/idf_coverage.py <collection>
scripts/bench/pipeline_bench.py --report perf.json   # needs a running stack
scripts/bench/bsim_baseline.py --report bsim.json    # Ghidra's own BSim, optional
```
