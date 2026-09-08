# FunctionID Coverage Report — collection `main`

Date: 2026-08-06
Instance: `http://10.1.0.143:5000`
Scope: collection `main`, 257 files, ~107k functions.

## Method

In BSimVis, FunctionID output is exactly the `lib:*` tag namespace. `_extract_fid_tags_for_function`
(`bsimvis/app/services/ghidra_service.py:411`) is the only producer of these tags, and it emits
nothing else — it reads Ghidra's Function ID analyzer bookmarks and queries `FidQueryService` by
full/specific hash. So "function has a `lib:` tag" == "FunctionID identified it".

Counts pulled from `/api/function/search` (`func_tag=lib`, `language_id=…`, `min_cohesion=0`,
`format=json`) and `/api/file/search`. Cross-architecture evidence from `/api/similarity/search`.

Total lib-tagged functions: **33 719**.

## 1. Per-architecture FunctionID coverage

### Architectures with FunctionID hits (9 / 14)

| language_id | functions | lib-tagged | % | FID libraries hit |
|---|---|---|---|---|
| mips:le:32:default | 14 018 | 10 910 | 77.8% | uclibc, openssl-dev |
| 68000:be:32:coldfire | 5 318 | 3 422 | 64.3% | uclibc, gcc-m68k-linux-gnu |
| superh4:le:32:default | 4 198 | 2 463 | 58.7% | uclibc |
| x86:le:32:default | 6 807 | 3 775 | 55.5% | uclibc, libc6-dev, libgcc-5/7, glibc-static, boost-static, qt5-qtbase-static, libsodium |
| sparc:be:32:default | 1 068 | 476 | 44.6% | uclibc |
| mips:be:32:default | 18 274 | 6 132 | 33.6% | uclibc |
| x86:le:64:default | 10 267 | 2 057 | 20.0% | uclibc, glibc-static, libc6-dev, Visual Studio, gcc, openssl-dev, openssl-static, libgcc-7 |
| arm:le:32:v8 | 29 153 | 4 378 | 15.0% | uclibc, gcc-arm-linux-gnu, openssl-dev |
| aarch64:le:64:v8a | 1 844 | 106 | 5.7% | libc6-dev-arm64, glibc-static, libgcc-4.8/5/7-dev-arm64, openssl-dev, gcc-aarch64-linux-gnu |

### Architectures with ZERO FunctionID identification (5 / 14)

| language_id | functions |
|---|---|
| mips:le:64:64-32r6addr | 9 708 |
| powerpc:be:32:e500 | 4 643 |
| riscv:le:32:andestar_v5 | 592 |
| loongarch:le:64:lp64d | 590 |
| riscv:le:64:default | 570 |

**16 103 functions** with no FID coverage whatsoever. Cause: no FID database exists for those
language IDs. Stock Ghidra FID ships x86/ARM/AARCH64 only; the local uClibc and gcc-cross FIDBs
cover arm32, mips32 be/le, sh4, sparc, m68k and x86 32/64. Nothing exists for ppc-e500, either
RISC-V variant, LoongArch, or MIPS64 r6.

Notable secondary gap: `aarch64:le:64:v8a` has only glibc-side FIDBs — no uClibc coverage at all.

## 2. Per-binary FunctionID coverage

"Zero-FID" = binary in which not one function carries a `lib:` tag.

| arch | files | with FID | zero-FID | % zero | median per-file coverage | files <5% covered |
|---|---|---|---|---|---|---|
| PowerPC:BE:32:e500 | 17 | 0 | 17 | **100%** | 0.0% | 17 |
| MIPS:LE:64:64-32R6addr | 3 | 0 | 3 | **100%** | 0.0% | 3 |
| Loongarch:LE:64:lp64d | 2 | 0 | 2 | **100%** | 0.0% | 2 |
| RISCV:LE:64:default | 2 | 0 | 2 | **100%** | 0.0% | 2 |
| RISCV:LE:32:AndeStar_v5 | 2 | 0 | 2 | **100%** | 0.0% | 2 |
| AARCH64:LE:64:v8A | 4 | 2 | 2 | 50% | 1.1% | 3 |
| 68000:BE:32:Coldfire | 16 | 12 | 4 | 25% | 63.3% | 4 |
| sparc:BE:32:default | 8 | 6 | 2 | 25% | 47.3% | 2 |
| x86:LE:64:default | 16 | 13 | 3 | 19% | 57.9% | 6 |
| MIPS:BE:32:default | 34 | 28 | 6 | 18% | 78.8% | 9 |
| SuperH4:LE:32:default | 18 | 15 | 3 | 17% | 75.0% | 5 |
| MIPS:LE:32:default | 44 | 40 | 4 | 9% | 81.1% | 5 |
| ARM:LE:32:v8 | 62 | 58 | 4 | 6% | **4.7%** | 33 |
| x86:LE:32:default | 29 | 29 | 0 | 0% | 60.7% | 7 |

**203 / 257 files (79%)** have at least one FID hit; **54 have none**.

### Two distinct failure modes

- **Binary-level (no FIDB at all):** the 5 architectures at 100% zero — 26 binaries, no coverage.
- **Function-level (FIDB present but weak):** `ARM:LE:32:v8` looks healthy at binary level (6% zero)
  but is the worst real case — median per-file coverage 4.7%, and 33 of 62 files below 5%. Most ARM
  binaries get a handful of uClibc/gcc stub hits and nothing more. Biggest actionable gap by volume.
- `AARCH64` is effectively uncovered: 4 files, median 1.1%.
- MIPS le/be, SH4, m68k and x86 32/64 are genuinely well covered (median 57–81%).

### Zero-FID binaries inside otherwise-covered architectures

| arch | files |
|---|---|
| MIPS:BE:32:default | `pmips` ×2, `boatnet.mips`, `83866cc199…`, `bded860e92…`, `e609c77b01…` |
| 68000:BE:32:Coldfire | `pm68k` ×2, `xnxn…m68kxnxn` ×2 |
| ARM:LE:32:v8 | `parm5`, `4f80418188…`, `b14c44765e…`, `dc58bebcba…` |
| MIPS:LE:32:default | `pmpsl`, `pkf4m2`, `boatnet.mpsl`, `ee518fb241…` |
| SuperH4:LE:32:default | `psh4`, `xnxn…sh4xnxn` ×2 |
| x86:LE:64:default | `px86_64` ×2, `c57c374c39…` |
| sparc:BE:32:default | `nuclear.spc`, `676346035726…` |
| AARCH64:LE:64:v8A | `xnxn…aarch64xnxn` ×2 |

The `p<arch>` and `xnxn*` families fail on *every* architecture. That is one root cause, not seven:
a single build toolchain / static libc with no matching FIDB. Ties into the known static-ELF libc floor.

## 3. Case study — x86-64 zero-FID binary

### Target

`px86_64`, md5 `8c3dbb4c02edd6eef4c97f11dc609b50`, `x86:LE:64:default`.
183 functions, **all named `FUN_*`** (stripped, statically linked, gcc, 79 482 bytes, ELF exe). Zero FID tags.

Rejected candidate: `c57c374c39a3af9ece667639c2d3927e` (4 356 funcs, also zero FID) is a **Go 1.24
binary with `CGO_ENABLED=0`** — no libc present at all. Its unmatched functions are Go stdlib, a
separate gap (no Go FIDB exists).

### Standard library identified via BSim

**`FUN_00408f10` = `inet_addr`, uClibc 0.9.30.1.**

- 31 instructions, 10 BSim features, not a thunk
- BSim score **1.000** against **116** functions tagged `lib:uclibc:0.9.30.1:inet_addr`
- Those 116 span **6 architectures**: ARM32, MIPS BE/LE 32, SuperH4, x86-32, x86-64
- Shares cluster `84705` (`inet_addr`, cohesion 1.0, 126 members) with them

Further functions with cluster / similarity evidence:

| address | instr | evidence |
|---|---|---|
| `00408f10` | 31 | `lib:uclibc:0.9.30.1:inet_addr` — score 1.000, 116 matches, 6 arches |
| `00410646` | 90 | `lib:uclibc:0.9.30.1:pread64` — score 1.000 (via SH4) |
| `00410971` | 73 | `lib:uclibc:0.9.30.1:__xstat64_conv` — score 0.901 |
| `0040e58d` | 104 | printf/scanf family — `__init_scan_cookie` cluster |
| `00410342` | 406 | `_ppfs_prepargs` cluster |
| `00409b66` | 1 532 | `__scan_getc` cluster (vfprintf core) |
| `00406680` | 33 | `lib:openssl-static:1.0.2k` / `sk_free` — score 0.933 |

Conclusion: the binary is **uClibc 0.9.30.1 statically linked, plus statically linked OpenSSL ~1.0.2k**.

### Why FunctionID missed it

Not a FIDB coverage gap. A working uClibc x86-64 FIDB exists in this deployment: **a second binary
also named `px86_64`** (md5 `fd42521eceddf75baa6f1d411215c137`, 212 funcs) receives **171
`lib:uclibc:0.9.30.1:*` tags** — 81% coverage. Same architecture, same libc, same version, same
family name.

| comparison | pairs ≥ 0.9 |
|---|---|
| `8c3dbb4c` ↔ `fd42521e` (the FID-identified `px86_64`) | **9** |
| `8c3dbb4c` ↔ `844d438f` (its own sibling `px86_64`) | **181** |

The two `px86_64` builds are the same source compiled differently. FID hashes are
instruction-sequence exact (full hash) or operand-masked (specific hash); a different `-O` level,
gcc version, or `-static` variant changes both, and every lookup misses.

Instruction-count threshold is **not** the cause — `inet_addr` has 31 instructions, comfortably over
the 10-instruction floor applied at `bsimvis/app/services/ghidra_service.py:456`.

The same signature runs through the family: `bin.x86_64` (2 284 funcs → 3 FID tags) and both
`xnxn…x86_64` (257 funcs → 1 tag each) share ~100 functions ≥0.9 with the target and are equally
FID-blind. One toolchain, one FIDB miss, roughly 2 900 unidentified libc functions on x86-64 alone.

## 4. Recommendations

1. **Build FIDBs for the 5 uncovered architectures** — ppc-e500, mips64r6, loongarch, riscv32/64.
   26 binaries, 16 103 functions currently unreachable by FID.
2. **Build a uClibc FIDB from the `p*` / `xnxn*` toolchain**, not the stock one. Ground truth already
   exists via BSim cross-architecture matches — the tagged uClibc 0.9.30.1 functions can seed it.
3. **Backfill `lib:` tags by BSim cluster propagation** where FID structurally cannot reach. That is
   what surfaced `inet_addr` here, and it generalises across architectures.
4. **Add uClibc coverage for AARCH64** — currently glibc-only, median coverage 1.1%.
5. **Investigate ARM32 function-level coverage** (median 4.7% despite a present FIDB) — likely the
   same compile-flag mismatch as the x86-64 case, at 10× the volume.

## Caveats

- The similarity index only stores pairs scoring ≥0.9, so the 7 identified functions in §3 are a
  floor, not the full overlap. A direct BSim query of all 183 functions against the uClibc-tagged
  pool would name more.
- Per-file coverage % uses each file's `function_count` as denominator; Ghidra's own function count
  is slightly higher than the indexed count on some binaries, so coverage is marginally understated.
