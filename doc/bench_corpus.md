# The BSim benchmark corpus

A reproducible, open-source binary corpus for measuring BSimVis: how fast the
pipeline ingests and scores, and how well it finds a function back.

It exists because the weighted-vs-unweighted comparison in MISP/bsimvis#50 ran on
21 and 20 shared function names. At that size every algorithm scored 100% on one
comparison and single-function differences moved recall by 5 points. Nothing
about a scoring change can be concluded from that. This corpus is three orders of
magnitude larger and, more importantly, contains *unrelated programs*, so a wrong
top-1 hit is a real false positive instead of a same-program near-miss.

## What it is

| | |
|:--|:--|
| Projects | 5 (sqlite, zlib, lua, zstd, mbedtls) |
| Binaries | 180 |
| Defined functions | 313,685 |
| On disk | 213 MB of binaries |
| Targets | 6 (x86-64, ARM64, PPC64LE, RISC-V 64, Windows x64, Windows x32) |
| Optimisation | O0, O2, Os |
| Linkage | dynamic and fully static (`-static`) |

180 = 5 projects x 6 targets x 3 optimisation levels x 2 linkages. Every cell of
the matrix built; there are no gaps to explain away.

## Sources

Everything is upstream open source, pinned by version **and sha256**, downloaded
at build time. No source or binary is vendored into this repository.

| Project | Version | License | Why it is here |
|:--|:--|:--|:--|
| SQLite | 3.45.3 | Public domain | Large single-TU amalgamation; ~2.8k functions of dense, branchy C |
| zlib | 1.3.1 | zlib | Small and ubiquitous; the "is this library present" case |
| Lua | 5.4.6 | MIT | Interpreter: dispatch loops and many small functions |
| Zstandard | 1.5.6 | BSD-3 / GPLv2 | Modern C with heavy inlining and SIMD-ish integer code |
| Mbed TLS | 3.6.0 | Apache-2.0 | Crypto: constant-time patterns, big-integer arithmetic |

The exact list, with hashes, is `scripts/bench/corpus/sources.txt`. A checksum
mismatch aborts the build rather than benchmarking against something else.

## How the binaries are made

One direct compiler invocation per binary — no autotools, no CMake, no container.
That is deliberate: a `./configure` run bakes in host details, so the same tarball
would produce different binaries on a different machine. A fixed command line
over a fixed source list does not.

```bash
# Debian/Ubuntu toolchains (this is the whole dependency list)
sudo apt install gcc gcc-aarch64-linux-gnu gcc-powerpc64le-linux-gnu \
                 gcc-riscv64-linux-gnu gcc-mingw-w64 unzip

scripts/bench/corpus/build_corpus.sh              # full: 180 binaries, ~3 min
scripts/bench/corpus/build_corpus.sh --tier quick #  60 binaries, 3 targets
scripts/bench/corpus/manifest.py --symbols        # manifest.json + symbols.json
```

Flags are identical across targets: `-O{0,2,s} -g0 -fno-stack-protector`, plus
`-static` for the static half.

Two choices matter for what the benchmark can measure:

**Binaries are not stripped.** The symbol table is the ground truth. Two builds of
one source share function names, so a same-name pair across builds is a known
match and a different-name pair is a known non-match — no human judgement, no
labelling pass. Nothing else in the pipeline reads those names.

**Half of the corpus is statically linked.** `-static` pulls the whole libc into
the binary — `memcpy`, `strlen`, `qsort`, printf's formatting machinery, and on
Windows the static CRT. That is exactly the boilerplate BSim feature weighting is
supposed to suppress, and it is also what makes two unrelated programs look alike.
A corpus of dynamically linked binaries only would leave that untested. It also
grows the corpus: static builds carry roughly 1,000 extra functions each.

`-g0` keeps DWARF out. With debug info Ghidra would recover types and names it
would never have in a real analysis, and the benchmark would flatter itself.

## What each script produces

```
scripts/bench/corpus/build_corpus.sh   # fetch + verify + cross-compile   -> bin/
scripts/bench/corpus/manifest.py       # md5, build coordinates, symbols  -> manifest.json
scripts/bench/corpus/extract.py        # Ghidra decompile + BSim features -> vectors/
```

`extract.py` runs Ghidra **once** for the whole corpus and writes:

- `vectors/<md5>.json` — `{function_name: {feature_hash: tf}}`, tens to hundreds
  of KB per binary, produced directly by `bsimvis upload --save-vectors`. Every
  accuracy number is computed from these, offline: no server, no kvrocks, no JVM.
  Reruns cost seconds.
- `dumps/<md5>.json` — the full upload payload, written only with `--keep-dumps`.
  A statically linked binary produces **several GB** of these, because every
  feature carries its pcode block; they exist for payload-level debugging, not for
  routine benchmarking. Nothing in the suite requires them.

Extraction is the expensive step — it is the Ghidra decompiler, and its per-binary
cost is itself a result, recorded in `extract_times.json`.

Requires `bin/` (the vendored Ghidra) and a `bsimvis_config.toml`; in a worktree,
symlink `bin/` and copy the example config, exactly as `scripts/wt-setup.sh` does.
On a headless box, run it with `DISPLAY=` — Ghidra's project API otherwise tries to
reach an X server and every binary fails instantly.

## Reproducing the whole thing

```bash
scripts/bench/corpus/build_corpus.sh
scripts/bench/corpus/manifest.py --symbols
scripts/bench/corpus/extract.py --jobs 4                  # hours; Ghidra-bound, resumable

scripts/bench/quality.py --report quality.json             # accuracy, offline
scripts/bench/idf_coverage.py <collection>                 # weighting go/no-go gate
scripts/bench/pipeline_bench.py --report perf.json         # throughput, needs the stack
scripts/bench/bsim_baseline.py --report bsim.json          # Ghidra's own BSim, optional
```

The corpus lives outside the repository, under `$CORPUS_ROOT`
(default `~/data/bsim-bench-corpus`), because 213 MB of binaries and gigabytes of
extracted features do not belong in git.

## The Ghidra BSim baseline

`bsim_baseline.py` builds a real Ghidra BSim H2 database from the reference half
of a corpus slice, commits signatures, and queries the other half with
`scripts/bench/bsim/BSimQueryAll.java`. It reports recall on the same binaries and
the same symbol-name ground truth as `quality.py`, plus one number our own
benchmark cannot produce: how often the true match survived BSim's **LSH candidate
selection** at all. BSimVis scores all candidates; Ghidra bins first and scores
second, and only a live database shows what that costs.

This is benchmark scaffolding, not a dependency. It runs `support/bsim` and
`support/analyzeHeadless` out of the Ghidra install already in `bin/`. BSimVis
itself never connects to a BSim database, and no package, driver or config was
added to the application for it.

## Limits worth stating

- **Ground truth is symbol names.** Inlining means a name can exist in one build
  and be dissolved into callers in another; those show up as unfindable, which is
  correct but harsher than a human would judge.
- **Static builds repeat libc across projects.** `memcpy` in static sqlite really
  is the same code as `memcpy` in static lua. Cross-project matches on libc
  functions are therefore true matches wearing a false-positive costume; the
  per-axis breakdown in `quality.py` keeps them from being read as errors.
- **One compiler family.** Everything is GCC 13 (mingw-w64 for Windows). Clang and
  MSVC would add a real cross-compiler axis and are not here.
- **`weighted_cosine` has no build path.** It works on the exact-score path only,
  so end-to-end throughput can be compared for `jaccard` and `unweighted_cosine`
  only. The weighted per-pair cost is measured directly by
  `scripts/bench/scoring_cost.py` instead, and its retrieval quality by
  `quality.py`, which scores offline and does not need the build path.
