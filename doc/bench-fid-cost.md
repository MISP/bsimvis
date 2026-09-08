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
- Every function pays two misses (`findFunctionsByFullHash`, then the
  `findFunctionsBySpecificHash` fallback) across every attached database. On
  dynamically-linked ELF that is 100% miss — curl and ssh got 0 tags for +53s
  and +851s. Only the static binary got matches: 20 tags for 2607s.

## Cause, from the FunctionID sources

The first reading of these numbers blamed cache thrash over the 47 attached
`.fidb` files. `Ghidra/Features/FunctionID/lib/FunctionID-src.zip` says
otherwise, on both counts:

- **`findFunctionsBySpecificHash` is an unindexed full table scan.**
  `FunctionsTable.getFunctionRecordsBySpecificHash` walks `table.iterator()`
  over *every function record in the database*, comparing the hash in a loop.
  The full-hash sibling right below it uses `table.indexKeyIterator` on an
  index. We called the scan for every function whose full-hash lookup missed —
  on dynamically-linked ELF, all of them. That is the 2-9x.
- Ghidra never calls it in a hot path. Its own matcher, `FidProgramSeeker`,
  queries the full hash only and then compares `getSpecificHash()` as a field
  on records it already holds. The only other callers are `FidDebugUtils` and
  `FidFunctionDebugPanel` — the human-driven debug UI, where one scan is fine.
- The fallback could not have matched anyway. The full hash covers the same
  code units with the operands masked out (`MessageDigestFidHasher`), so a
  full-hash miss means the instruction sequence differs, and the specific hash
  — the same sequence *plus* operand scalars — misses too, short of a
  collision.
- **The 47 databases were never all queried.** `FidQueryService` opens only
  files where `fidFile.canProcessLanguage(language)`, so an x86-64 program
  sees ~12 of them. Detaching databases by hand buys little.
- **Bookmark parsing produced zero tags because it read the wrong field.** The
  FID analyzer writes `Library: <family> <version> <variant>` into the
  function's *plate comment* (`ApplyFidEntriesCommand.generateComment`); the
  bookmark it also drops carries only `Library Function - Single Match,
  <name>` (`generateBookmark`). The regex looked for `Library:` in the
  bookmark, where it can never appear.

## What changed

`ghidra_service.py`, in `_extract_fid_tags_for_function` and its caller:

1. Dropped the `findFunctionsBySpecificHash` fallback. No tags lost — see the
   static row below.
2. Read `func.getComment()` instead of the bookmarks. The analyzer's own
   verdict, at the cost of a string parse, and the analyzer is free.
3. Took the instruction-count threshold from `hash_quad.getCodeUnitSize()`
   instead of walking the listing from Python — that loop crossed the JVM
   bridge twice per instruction per function.
4. Hash each function once and share the quad between the tag lookup and the
   `function_id_hash` we ship to the backend. Each `hashFunction` call walks
   the extent twice inside Ghidra, and it used to be called twice per function
   (plus a fresh `FidService()` per call).

Note that `skip_function_id` still emits `function_id_hash`: hashing touches no
database, only the query service does, so the identity hash stays cheap.

Parse behaviour is pinned by an assert self-check:
`python -m bsimvis.app.services.ghidra_service`.

## After

Re-measured 2026-08-11 on `worktree-fid-perf-fix`, same profile and heap. The
static sample is a local `gcc -static -O2` build (785 KB, 1005 functions), not
the same binary as the 2026-08-06 row, so compare the overhead column, not the
absolute seconds.

| binary | funcs | baseline | analyzer | comment parse | hash query | FID total | overhead |
|---|---|---|---|---|---|---|---|
| v01_linux_x64 (17 KB) | 28 | 2.2s | −0.0s | 0.0s | 0.0s | −0.2s | **−10%** |
| crypto_test (16 KB) | 13 | 1.8s | −0.1s | 0.0s | 0.0s | −0.1s | **−6%** |
| hello_static (785 KB) | 1005 | 303.7s | +15.5s | −11.3s | +28.4s | −29.7s | **−10%** |

Every delta is now inside run-to-run noise — the static sample's analysis phase
alone swung 25s to 50s across modes in a single round, which is why the totals
come out negative. The number that moved: hash query on the static binary,
**2607s → 28s**.

### Did dropping the fallback cost any matches?

Probed directly on the static binary — for each function, full-hash query
first, then the specific-hash scan on every miss (capped at 25 scans):

```
functions: 1079
analyzer plate comments mentioning a library: 17
-> tags our parser would emit: 4
below threshold / unhashable: 234
full-hash hits: 19
specific-hash-only hits: 0 (out of 25 misses scanned, 13.2s)
```

No match was reachable only through the scan, as the hash construction
predicts. The 13.2s/25 scans is 0.53 s per call, the same per-function price
the original table charged to `hashq`.

The 17 plate comments yield 4 tags because the other 13 are multi-library
(`Libraries: glibc-static 2.28 42.el8.1.x86_64, ...`), which the parser skips;
the full-hash query covers those functions instead.

One caveat on the old table's `lib: tags` column: it counted tags starting with
`lib:`, which the tag-taxonomy migration renamed to `origin:lib:`. On current
`dev` that column read 0 regardless of what was tagged. Fixed in the script.
