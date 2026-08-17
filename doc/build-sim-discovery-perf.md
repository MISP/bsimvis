# build_sim discovery: why it stalled, and why we vectorised instead of pruning

**Date:** 2026-08-14
**Measured against:** live `full_arbor` collection (1,510,333 indexed functions),
BSimVis instance at commit `a17b89d`, kvrocks on `:6666`.
**Code touched:** `bsimvis/app/services/similarity_service.py`
(`_discover_find`, `_discover_minhash`, posting-list cache).
**Equivalence check:** `scripts/test_discover_equivalence.py`.

> **These numbers are dataset-dependent.** `full_arbor` is a duplicate-heavy
> malware corpus. Every table below is reproducible with the scripts described
> at the end — re-run them before assuming the same conclusion holds for a
> differently-shaped collection. The *decision procedure* is the durable part,
> not the constants.

---

## 1. Symptom

A 4000-file upload appeared to freeze overnight. It had not frozen:

| signal | value |
|---|---|
| kvrocks throughput | 17,312 ops/s, 22 MB/s out |
| `estimate_pending_compaction_bytes` | 0 (no write stall) |
| workers | 14/14 busy, all `build_sim` / `index_sim` |
| jobs pending | 3,265 (`jobs:pending` list length confirmed 3,265) |
| oldest pending `ghidra_analyze` | queued 4 days |
| observed build rate | 0.1–0.4 fn/s |

The queue was healthy and nothing was deadlocked. All 14 workers were pinned on
long `build_sim` jobs (one had 91,872 items at 0.37 fn/s ≈ 64 h remaining), and
the FIFO queue has no per-type fairness, so `ghidra_analyze` never got a slot.
**The stall was a throughput problem in `build_sim`, not a scheduling bug.**

## 2. Where the cost is

`_discover_find` scans the full posting list of every feature of every target.
Cost per target = Σ |posting list| over the target's features.

Feature document-frequency is extremely skewed:

| features appearing in > X% of corpus | count | % of vocabulary | % of total scan cost |
|---|---|---|---|
| 0.1% | 2,426 | 9.08% | 99.8% |
| 0.5% | 520 | 1.95% | 98.9% |
| 1.0% | 253 | 0.95% | 97.8% |
| 5.0% | 49 | 0.18% | 90.1% |
| 10.0% | 16 | 0.06% | 76.9% |
| 50.0% | 1 | 0.00% | 32.0% |

**One feature appears in 948,734 of 1,510,333 functions (62.8%)** — a pure
stop-word. Sixteen features drive 77% of all work.

Average posting pairs scanned per function: **1,907,043** (sample of 500).

Feature-count distribution (`ZCOUNT` on `full_arbor:idx:func:bsim_features_count`):

| features per function | count | share |
|---|---|---|
| 0–4 | 241,597 | 16.0% |
| 5–9 | 248,449 | 16.4% |
| 10–19 | 300,679 | 19.9% |
| 20–49 | 380,042 | 25.2% |
| 50–99 | 179,655 | 11.9% |
| 100+ | 159,911 | 10.6% |

## 3. The rejected fix: `max_posting_fraction`

`bsimvis/similarity/backends.py:196` already has a `max_posting_fraction`
cutoff, so porting it into `_discover_find` looked obvious. **It is not
equivalent, and we rejected it.**

In `backends.py` the cutoff gates *candidate generation only*; scores are then
recomputed exactly by `select_target_block`. In `_discover_find`,
`intersection_counts` **is** the score numerator. Skipping a feature there
silently under-counts the dot product, so true matches sag below
`min_score` and disappear.

Worst-case score deficit for an identical twin equals the fraction of squared
norm carried by dropped features. Sample of 1,500 functions:

| cutoff | scan cost | fns with error > 0.10 | p95 error | blind fns |
|---|---|---|---|---|
| 0.50 | 68.8% | 48% | 0.65 | 0% |
| 0.20 | 42.3% | 57% | 0.80 | 0% |
| 0.10 | 23.1% | 80% | 0.77 | 0% |
| 0.05 | 9.9% | 93% | 0.89 | 3.1% |
| 0.01 | 2.2% | 99% | 1.00 | 11.9% |

At `min_score = 0.9`, an error above 0.10 can drop a perfect match. Even
removing *only* the single 63% stop-word (cutoff 0.50) breaks 48% of functions
for a 31% saving.

`min_features` does not rescue it — it converts blind functions into at-risk
ones rather than removing the problem:

| min_features | cutoff 0.10, fns over error bound |
|---|---|
| 0 | 79.9% |
| 5 | 81.3% |
| 8 | 80.3% |
| 16 | 79.2% |
| 32 | 79.9% |

### 3b. The safe version of the cutoff, and why it still lost

A candidate missing from the set shares no rare feature with the target, so by
Cauchy–Schwarz on the stop-word subspace:

```
score(T,C)  <=  |T_stop| / |T|  =  sqrt(dropped_norm_sq_frac)
```

A miss at `min_score` therefore requires `dropped_norm_sq_frac >= min_score²`.
That is computable per target from the `ZCARD`s `_discover_find` already
fetches, so targets can be routed to a fast path only when provably safe.
Measured at `min_features = 0`:

| cutoff | zero-loss (T=0.81) | relaxed (T=0.90, lossy) |
|---|---|---|
| 0.002 | 1.68x | 3.69x |
| 0.005 | **2.00x** | 3.98x |
| 0.01 | 1.97x | 3.49x |
| 0.02 | 2.06x | 2.68x |

**~2x for zero loss.** The relaxed variant reaches ~4x but drops true pairs
scoring between 0.90 and `sqrt(0.90)` = 0.949 — precisely the near-duplicates
the corpus exists to find. Not worth 2x.

## 4. What we shipped instead

Profiling the loop rather than the I/O changed the answer. The accumulate loop
runs at **849 ns/pair** → ~1.61 s for a 1.9M-pair function, against ~2.5 s
observed end to end. **`build_sim` was already CPU-bound in Python**, so
pruning I/O was attacking the smaller half.

### 4a. Vectorised accumulation

The loop is a sparse scatter-add. Rewritten with `np.bincount`:

| implementation | ns/pair | per 1.9M-pair function |
|---|---|---|
| Python `for` loop | 849 | 1.61 s |
| `np.add.at` | 7 | 0.013 s |
| fused `np.bincount` | 7 | 0.013 s |

**129x on the hot loop.** Sums stay `float64` and stay in feature order, so
results are unchanged — verified bit-identical, not argued.

Preserving semantics required one observation: the `can_add_new` pruning bound
reads only `tf` values, never posting-list contents, and `remaining_*` falls
monotonically. So the open/closed feature split is decidable **before** any
fetch. Open features may introduce candidates (vectorised scatter-add); closed
features may only top up candidates already found (masked scatter-add). This
reproduces the original loop exactly, including its early `break`.

### 4b. Compact posting-list cache

The LRU thrashed because of its *representation*, not its size:

| representation | bytes/pair | 27M-pair working set |
|---|---|---|
| `list[(str, float)]` | 186.7 | **5.09 GB** |
| `int32` ids + `float64` tfs | 12.0 | **0.33 GB** |

The 5.09 GB is why the budget was 5M pairs — and the working set is larger than
that, so it thrashed. Measured working set is stable at ~27M pairs regardless of
binary size:

| binary | functions | distinct features | working set | pairs fetched (5M LRU) | needed | waste |
|---|---|---|---|---|---|---|
| `34633f50…` | 2,394 | 14,559 | 26,292,139 | 1,259,696,010 | 26,292,139 | **48x** |
| `1a585ee3…` | 2,394 | 14,559 | 26,292,139 | 1,249,264,793 | 26,292,139 | **48x** |
| `8c87104f…` | 5,901 | 62,209 | 27,254,967 | 5,345,357,664 | 27,254,967 | **196x** |

A rejected variant, for the record: *"don't cache lists above N entries"* makes
it **worse** (0.29–0.43x), because the mega-lists are exactly what gets
refetched. Simulated before implementing.

Default budget is now 30M pairs (`similarity.posting_cache_pairs`), ≈0.36 GB of
arrays plus a fid→index intern table (~235 MB at 1.5M functions).
**Check total RAM against your worker count before raising it** — at 14 workers
the default is ~8 GB across the fleet. Lower it for small hosts; the code
degrades to the old thrashing behaviour rather than failing.

## 5. Measured effect

An earlier draft of this document projected 10–25x from the 129x microbenchmark.
**That projection was wrong and is corrected here by measurement.** The 129x
applies to the accumulate loop alone; `_discover_find` also does per-candidate
Python work (phase-1 filter, `kept` construction, `_counts`, the scoring loop)
that the change does not touch, and vectorising added a new per-pair cost of its
own — interning function ids into dense indices.

### 5a. Scaling A/B, CPU-side (in-memory store, no network)

Old vs new on synthetic corpora of the measured shape, 400 targets per corpus so
posting lists are reused across targets the way a real `build_batch` reuses them.
Results identical at every size.

| corpus (functions) | pairs/target | baseline | new | speedup |
|---|---|---|---|---|
| 1,000 | 1,894 | 0.06 s | 0.05 s | 1.11x |
| 5,000 | 9,342 | 0.32 s | 0.08 s | 3.96x |
| 20,000 | 37,615 | 1.85 s | 0.24 s | 7.70x |
| 60,000 | 113,259 | 6.93 s | 0.84 s | 8.23x |
| 150,000 | 282,339 | 20.81 s | 2.55 s | 8.17x |

**The win is scale-dependent and plateaus near 8x.** It is roughly nil on small
corpora, because numpy's fixed overhead and the interning loop cancel the saving.

### 5b. Pipeline benchmark on `data/bench` (5 binaries, 1,179 functions)

| sub_task | baseline | new |
|---|---|---|
| `build_sim` | 1.478 s | 1.250 s (**1.18x**) |
| `enrich_features` | 81.70 s | 75.74 s (untouched by this change) |
| `grand_total` | 86.26 s | 79.80 s |
| `func_similarities` | 2048 | 2048 (identical) |

1.18x matches the 1.11x the scaling table predicts at that corpus size — two
independent methods agreeing. At this scale the benchmark can only demonstrate
**no regression and identical output**; it cannot show the win.

### 5c. Read-only A/B against live `full_arbor` (1.5M functions)

12 targets sampled at random, cold caches: **1.08x**, identical result counts.

This is *not* a refutation of 5a — it measures a different bottleneck. Random
targets across 1.5M functions share almost no features, so the posting-list cache
never gets reused and the run is dominated by fetching ~95 MB per target from an
already-saturated kvrocks. The CPU saving is real but is a minority of that wall
clock. A per-binary sample (where a real build gets its reuse) is the meaningful
end-to-end measurement and had not completed when this was written.

### 5d. Production deploy, full_arbor (measured after rollout)

Deployed to the live instance and measured with `scripts/compare_build_speed.py`
over two independent windows (600 s and 1,507 s). Baseline is the pre-deploy
snapshot pair recorded in `doc/benchmark.md`.

**DB traffic collapsed as designed:**

| kvrocks | pre-deploy | post-deploy |
|---|---|---|
| output | 22,353 kbps | **382 kbps** (58x less) |
| ops/s | 17,312 | 6,827 |

That 58x is also the only reliable proof the new code was live — there is no
version endpoint, and the old representation could not produce it.

**Throughput did not follow.** Headline numbers are a mix artifact and must be
size-matched: the pre-deploy sample contained only large binaries (9 large, 0
small), the post-deploy sample was roughly half small ones.

| bucket | baseline | 600 s window | 1,507 s window |
|---|---|---|---|
| overall median (misleading) | 0.0073 | 0.0383 (5.24x) | 0.0391 (5.35x) |
| **large ≥1000 fn (like-for-like)** | **0.0073** | **0.0083 (1.14x)** | **0.0129 (1.77x)** |
| 2,394-function binaries | 0.0073 | 0.0050 (0.68x) | 0.0023 (0.32x) |
| 2,129-function binaries | 0.0073 | 0.0250 (3.41x) | 0.0219 (2.99x) |
| small <1000 fn | no baseline | 0.0650 | 0.0730 |

**Real like-for-like gain is ~1.1–1.8x, not the 8x the synthetic sweep
predicted**, with high variance and only 4–5 comparable jobs per window. One
2,394-function binary built a single function in 25 minutes, so a few
pathological functions dominate the tail.

The conclusion is sharper than the number: removing 58x of DB traffic bought
almost no throughput, which proves discovery on this host was **never
fetch-bound**. The time is in Python, and still is. §5c's 1.08x read-only A/B
predicted this and should have been weighted more heavily than the in-memory
sweep.

### 5e. What this means

- CPU-side, at production corpus size: **~8x on discovery**, results identical.
- End-to-end gain depends on whether fetch or CPU dominates on your host. On a
  saturated remote kvrocks with cold caches, expect much less than 8x.
- The remaining hot spots, from profiling the new code: `_intern` (a per-pair
  Python `dict.get` loop, 2.1M calls in the profiled run) and the per-candidate
  scoring loop. **`_intern` is the next thing to optimise** — vectorising it, or
  assigning stable integer function ids at ingest, would remove the one cost this
  change added. The production deploy in §5d confirms this: DB traffic fell 58x
  while throughput moved ~1.1–1.8x, so the residual cost is entirely CPU-side.
- **Was it worth shipping?** On the measured evidence: the cache half is a clear
  win (58x less DB traffic frees kvrocks for the whole fleet), the vectorisation
  half is currently cancelled out by `_intern`, and output is provably identical.
  Keep it, but do not claim a throughput win until `_intern` and the
  per-candidate loop are addressed.
- **`build_sim` throughput was never the fleet's real problem.** The original
  stall was 14 workers monopolised by `build_sim` while 3,265 `ghidra_analyze`
  jobs starved on a FIFO queue with no per-type fairness. That recurred within
  hours of the restart (12/15 workers on `build_sim`, ghidra back to 1 running
  and 86 pending). Worker fairness, not discovery speed, is the higher-leverage
  fix.

## 6. Known behavioural difference

Among candidates with **exactly equal scores**, the order fed to the `top_k`
truncation changes (old: posting-list encounter order; new: ascending intern
index). Scores and the pre-truncation candidate set are identical. On a corpus
with many exact 1.0 duplicates, *which* arbitrary 1000 of 950k tied candidates
survive `top_k` may differ between old and new. Both are arbitrary; neither is
more correct. The equivalence test compares full candidate sets with an
effectively unbounded limit for this reason.

## 7. Caveats

- Benchmarks ran on a dev CPU, not the server; ratios should carry, absolute
  timings will not.
- §5a uses an in-memory store, so it isolates CPU and excludes fetch entirely.
  Real hosts sit somewhere between §5a and §5c.
- The 8x plateau is specific to this feature-frequency shape and to 400 targets
  of reuse. Fewer targets per posting list means less amortisation of `_intern`
  and a lower number — at 25 targets the same corpora gave only 1.1–1.7x.
- Sampling used `ZRANDMEMBER` with 80–2000 functions depending on the table;
  cost figures ±20%.
- The Cauchy–Schwarz bound in §3b is derived for cosine. It does **not** carry
  over to `jaccard` unchanged.
- `full_arbor` contains many byte-level duplicate binaries (two sampled hashes
  had identical function counts and feature working sets). A deduplicated
  corpus will show a flatter feature-frequency curve and smaller wins.

## 8. Reproducing

Measurement scripts are not vendored — they are short and read-only against
kvrocks. Reconstruct from these primitives:

```bash
# feature-count distribution
redis-cli -h <host> -p 6666 zcount full_arbor:idx:func:bsim_features_count 0 4

# posting-list size for one feature
redis-cli -h <host> -p 6666 zcard full_arbor:feature:<hash>:functions

# a function's feature vector
redis-cli -h <host> -p 6666 zrange full_arbor:func:<md5>:<addr>:vec:tf 0 -1 withscores
```

Sample targets with `ZRANDMEMBER full_arbor:idx:func:bsim_features_count <n>`,
then for each target sum `ZCARD` over its features to get scan cost, and
`sum(tf²)` over dropped features / `sum(tf²)` to get the error bound in §3.

Equivalence after any change to discovery:

```bash
uv run python scripts/test_discover_equivalence.py
```
