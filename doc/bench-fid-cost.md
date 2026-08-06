# Function ID cost inside the ghidra analyze job

Measured 2026-08-06 on branch `feat/bin-sim-split`, Ghidra 12.1, profile `fast`,
one JVM (4 GB heap), warmed up before measuring.

Reproduce:

```
GHIDRA_INSTALL_DIR=$PWD/bin/ghidra_12.1_PUBLIC \
  .venv/bin/python scripts/benchmark_fid_cost.py <binaries...> [--rounds N]
```

The script runs the analyze job's two timed phases (auto-analysis, then the
decompile+extract stream) in four modes, so the FID cost splits into its parts:

| mode | what runs | isolates |
|---|---|---|
| `off` | Function ID analyzer disabled, no tag extraction | baseline |
| `analyzer` | analyzer on, no tag extraction | analyzer cost (analysis phase) |
| `bookmark` | + `_extract_fid_tags_for_function` bookmark parsing | parse cost (stream phase) |
| `full` | + `FidQueryService` hash matching (production) | hash-query cost (stream phase) |

## Results

| binary | funcs | baseline | analyzer | bookmark parse | hash query | FID total | overhead | lib: tags |
|---|---|---|---|---|---|---|---|---|
| v01_linux_x64 (17 KB) | 28 | 2.4s | +0.2s | −0.1s | +7.5s | +7.0s | **+298%** | 0 |
| curl (297 KB) | 97 | 52.3s | −0.2s | +0.1s | +53.4s | +54.8s | **+105%** | 0 |
| ssh (847 KB) | 712 | 133.0s | −3.3s | +2.5s | +851.3s | +866.0s | **+651%** | 0 |
| hello_static (785 KB, `gcc -static`) | 1005 | 314.3s | +0.1s | +11.4s | +2607.2s | +2590.3s | **+824%** | 20 |

First three are best-of-2, the static one is a single round. Sub-second deltas
are noise (analysis phase varies ±10% run to run).

## Reading

- **The Ghidra Function ID analyzer is free.** Every analyzer delta is inside
  run-to-run noise. It is threaded, threshold-gated and runs once per program.
- **Bookmark parsing is near-free** (~11 ms/function) — and produced **zero**
  tags in all four samples, so today it is pure overhead.
- **`FidQueryService` hash matching is the entire cost**: 0.26 s/function on the
  smallest binary up to 2.6 s/function on the static one, i.e. **2× to 9× the
  whole analyze job**. Worst observed single run: ssh at 2698s of stream time
  against a 133s baseline.
- Per-function cost *grows* with binary size rather than staying flat, which
  points at cache thrash over the 47 attached `.fidb` files (660 MB in
  `Ghidra/Features/FunctionID/data`), not at a fixed per-lookup price.
- Every function pays two misses (`findFunctionsByFullHash`, then the
  `findFunctionsBySpecificHash` fallback) across every attached database. On
  dynamically-linked ELF that is 100% miss — curl and ssh got 0 tags for +53s
  and +851s. Only the static binary got matches: 20 tags for 2607s.

## Cheap levers, roughly in order of payoff

1. Detach the FID databases that cannot match the program's language/toolchain.
   The cost is per attached DB per query; 47 are attached today.
2. Skip the `findFunctionsBySpecificHash` fallback (halves queries; it only
   fires when the full-hash lookup already missed).
3. Query only functions the analyzer already flagged, or raise the instruction
   threshold well above the current 10 — most of the 1005 functions in the
   static sample are tiny libc stubs that never match.
