# Investigation — why kuna reports more functions than BsimVis/Ghidra

Date: 2026-07-30. Corpus: `~/data/versioned_c/bin` (60 binaries, 6 architectures).
BsimVis collection `versioned_c_deep` on `:5001` / kvrocks `:6667`.
Reference Ghidra: `/opt/ghidra/ghidra_12.0.4_PUBLIC` headless, stock analysis.
Trigger: `~/github/kuna/NOTES-bsim-kuna.md`, which reported "kuna enumerates far more
functions than Ghidra (8785 vs 3766)" and listed reduced BsimVis analysis as
confounder #1.

## Summary

1. **BsimVis's analyzer profile is not the cause.** Its stored BSim vectors are
   *byte-identical* to stock full-analysis Ghidra output — 57/57 functions, mean
   Jaccard 1.000. Its ELF function sets match headless Ghidra exactly.
2. **kuna's 8785 is inflated to 5861** once the duplicate rows in the sig dump are
   removed.
3. **On ELF the remaining delta is exactly the PLT thunks** BsimVis filters on
   purpose. RISCV, which has no PLT thunks, matches 356 vs 356.
4. **PE is the only real gap**, and it splits three ways: ~59% import-table slots
   that are not functions at all (kuna wrong), ~28% real uncalled functions Ghidra
   never creates (kuna right, and BsimVis loses 8 of the corpus's own test
   functions per win_x32 binary), ~13% mid-body addresses (kuna wrong).
5. **No Ghidra analyzer recovers the genuine misses** — but a 20-line
   create-functions-from-code-symbols pass does, exactly and only where it should,
   on symbol-bearing binaries. A stripped control shows kuna finds the same
   functions with no symbols at all: it sweeps the code section, Ghidra follows
   references.
6. The win_x64 kuna panic is an un-ported opcode in a kuna shim table
   (`CPUI_INT_SRIGHT`), fixable in one line, with the correct line already present
   three times elsewhere in the same repo.

---

## 1. The analyzer confounder is dead

BsimVis runs `AutoAnalysisManager` directly with the `fast` profile
(`bsimvis_config.toml`), which disables Decompiler Parameter ID, Decompiler Switch
Analysis, Apply Data Archives, Function ID, Stack, and Call Convention ID. The
file metadata shows `"Analyzed": "false"`, which is what suggested a reduced
analysis tier.

`"Analyzed": "false"` is cosmetic. That property is set by `analyzeAll`; BsimVis
drives `AutoAnalysisManager` itself and never sets it. Analysis did run.

Test: dumped BSim vectors from stock `/opt` Ghidra headless (default full
analysis) via `DecompInterface.debugSignatures(f, 10, monitor)` with
`setSignatureSettings(0x4d)` — the same call and settings word BsimVis uses — and
compared against the `versioned_c_deep:func:{md5}:{addr}:vec:tf` ZSets in kvrocks.

| | v01_linux_x64 + v01_arm_x64 |
|---|---|
| function pairs compared | 57 (every eligible function in both files) |
| **exact vector match** (hash→tf identical) | **57 / 57** |
| mean Jaccard | **1.000** |

Not "close" — identical. Re-running the Ghidra side under full analysis, step 1 of
the NOTES next-steps list, would change nothing. The 8.5%-exact / 0.379-cosine
kuna-vs-Ghidra gap is entirely on kuna's side of the IR.

## 2. Function counts, all 60 binaries

`ghidra all` = `getFunctionsNoStubs()` under stock full headless analysis.
`ghidra eligible` = after BsimVis's filter (`ghidra_service.py:474`):
`not isExternal() and not isThunk() and body >= min_func_len (10)`.
`kuna unique` = distinct entry addresses in `scratch-bsim/sig/*.sig`.

| arch | ghidra all | ghidra eligible | bsimvis stored | kuna unique |
|---|---|---|---|---|
| arm_x64 | 416 | 306 | 306 | 416 |
| linux_x64 | 376 | 296 | 296 | 396 |
| ppc_x64 | 670 | 386 | 386 | 431 |
| riscv_x64 | 356 | 356 | **356** | **356** |
| win_x32 | 1280 | 900 | 1256 | 2136 |
| win_x64 | 1616 | 1106 | 1166 | 2126 |
| **total** | **4714** | **3350** | **3766** | **5861** |

ELF `eligible` equals `bsimvis stored` in every row. BsimVis stores exactly what
stock Ghidra finds, minus its documented filter.

### 2a. kuna's 8785 is a double count

Every function in the ELF sig dumps is emitted twice — once under its symbol name,
once as `sub_<addr>` — with byte-identical vectors:

```
00000780 _init    58e282ec:1,90790e2c:1,ca0bb8a0:1
00000780 sub_780  58e282ec:1,90790e2c:1,ca0bb8a0:1
```

9275 lines → **5861 unique addresses**. The `kuna signatures` driver resolves
symbol targets and address targets into one target list without deduping.

### 2b. The ELF delta is PLT thunks

arm: 416 − 306 = 110 = 11 thunks × 10 binaries. linux and ppc likewise. RISCV has
no PLT thunks in this corpus and the two sides agree exactly, 356 vs 356. Thunks
are one-instruction jump stubs; excluding them from a BSim index is correct.

On PE, BsimVis stores *more* than headless-eligible (1256 vs 900) because the
`fast` profile leaves PE thunks unidentified, so they pass the `isThunk()` filter.
That inflates BsimVis, it never shrinks it.

## 3. The PE gap, classified

Probed every kuna-only address against a stock-analyzed Ghidra program.

| class | win_x32 | win_x64 | who is right |
|---|---|---|---|
| `.idata` IAT pointer slots (`__imp_*`) | 53 | 47 | **Ghidra**. Not functions — 8-byte-stride import table entries. kuna decompiles them as functions. |
| real `.text` code, symbol'd, no function created | 24 | 0 | **kuna**. Genuine Ghidra misses. |
| mid-body of an existing Ghidra function | 9 | 5 | **Ghidra**. Bogus entry points. |
| symbol'd but Ghidra left it as undefined bytes | 1 | 0 | **kuna** (`_WinMainCRTStartup` @ `0x4013d0`). |
| **kuna-only total** | **85** | **51** | |
| ghidra-only | 0 | 1 (`___chkstk_ms`) | |

On **win_x64, 92% of kuna's extras are import-table slots** and the rest are
mid-body — kuna finds nothing real there that Ghidra missed. The genuine gap is
win_x32 only.

### The genuine misses matter

The 24 real win_x32 functions Ghidra skips include eight of the corpus's own test
functions:

```
00401788 _linear_search       0040194a _matrix_add       00401a48 _is_palindrome
00401b3b _calculate_std_dev   00401b75 _dot_product      00401bd0 _manual_pow
00401cc2 _selection_sort      00401d89 _matrix_transpose
```

plus MinGW CRT internals (`_pre_c_init`, `_pre_cpp_init`, `__FindPESectionByName`,
`___d2b_D2A`, `___mingw_enum_import_library_names`, …). BsimVis is therefore
missing eight of the actual comparison targets in every win_x32 binary — a real
data loss for cross-arch similarity work on this corpus.

### Why Ghidra misses them

Ghidra creates functions from **references**. These addresses have none:

```
REF 00401788 func=false refs=[]                              prev_instr=00401787:RET   <- linear_search
REF 0040194a func=false refs=[]                              prev_instr=00401949:RET   <- matrix_add
REF 0040151c func=true  refs=[UNCONDITIONAL_CALL@00401e4f]   prev_instr=0040151b:RET   <- array_sum
```

`array_sum` and `bubble_sort` are called from `main`, so Ghidra creates them.
`linear_search` and friends are compiled in but never called in the win_x32 build,
so nothing references them. The COFF symbol becomes a *label*, not a function. The
code is disassembled correctly and sits between two `RET`s at a real boundary
(`bubble_sort` ends at exactly `0x401788`) — Ghidra just never promotes it.

kuna finds them by **sweeping executable bytes**, not by reading symbols — see the
stripped-binary control below. Its extra coverage is genuine code discovery.

### Control: the same binary, stripped

`strip`ped `v01_win_x32.exe` (245737 → 47118 bytes, COFF symbol table gone,
import table necessarily retained):

| | unstripped | stripped |
|---|---|---|
| ghidra `nostub` | 126 | 106 |
| kuna unique | 212 | 201 |
| all 8 uncalled test functions found by kuna | yes | **yes** |
| same 8 found by ghidra | no | no |
| kuna addresses in `.idata` | 53 | 53 |

kuna finds `linear_search`, `matrix_add`, `is_palindrome`, `calculate_std_dev`,
`dot_product`, `manual_pow`, `selection_sort` and `matrix_transpose` at the same
addresses with **zero symbols present**. Only `_WinMainCRTStartup` @ `0x4013d0`
drops out when stripped — it is the one entry that was reachable from a symbol
alone (Ghidra had left it as undefined bytes).

So the mechanism is not symbol lookup on either side: Ghidra is
**reference-driven** and skips code nothing calls; kuna **sweeps the code section**
and picks up unreferenced function bodies. The 53 `.idata` false positives are
likewise not symbol-driven — kuna treats import-table pointer slots as entry
points whether or not `__imp_*` names exist.

### No analyzer fixes this

Re-ran headless with every plausible analyzer forced on — Aggressive Instruction
Finder, Function Start Search, Non-Returning Functions - Discovered, Decompiler
Switch Analysis, Create Address Tables, Demangler Microsoft, Decompiler Parameter
ID, Stack, Shared Return Calls, Subroutine References, Embedded Media:

| | default | aggressive |
|---|---|---|
| v01_win_x32.exe | 126 | **126** |
| v01_win_x64.exe | 160 | **160** |

Zero new functions. Function Start Search scans *undefined* bytes for prologue
patterns; these addresses are already disassembled code, so it skips them. There
is no stock analyzer that promotes a code label to a function.

### What does fix it

A post-analysis pass: for every `LABEL` symbol in an executable block that has an
instruction at it and no containing function, call `createFunction`.

| binary | before | created | after |
|---|---|---|---|
| v01_win_x32.exe | 126 | **24** | 150 |
| v01_win_x64.exe | 160 | 0 | 160 |
| v01_linux_x64 | 36 | 0 | 36 |

All 24 come back with sane bodies (`_linear_search` 80 bytes, `_selection_sort`
199, `___d2b_D2A` 230) and it fires on nothing where nothing is wrong. Three of
the 24 are named `.text` (the symbol is a section name) and should be filtered out;
skipping `LAB_*` is already in the probe. `_WinMainCRTStartup` @ `0x4013d0` needs a
`disassemble()` first — Ghidra left it as undefined bytes — and is not recovered by
the pass as written.

Suggested placement: `GhidraService.run_profile_analysis`, after
`mgr.waitForAnalysis`, guarded so it only creates functions at non-`LAB_`,
non-section-named code labels. Not implemented here.

**Limit of this fix:** it keys on symbols, so it only closes the gap on binaries
that still carry them. On the stripped control it would recover nothing, while
kuna still finds all 8 (see above). Matching kuna on stripped input needs a
gap-sweep — promote runs of disassembled-but-unclaimed code between function
bodies — which is a bigger change and not evaluated here.

## 4. The win_x64 kuna panic

```
thread 'main' panicked at crates/kuna-decomp/src/p3_dataflow/ruleaction_2.rs:96:18:
ruleaction_2 op_typeop: unhandled opcode CPUI_INT_SRIGHT (W6 seam)
```

Function: **`__lshift_D2A` @ `0x140007b40`** — the dtoa bignum left-shift helper,
the only function in the corpus that reaches this path. kuna catches the unwind per
function and emits `ERROR: decompile pipeline reached an un-ported seam (LOSS-131)`,
so the run still completes; that one function has no vector.

Cause: `op_typeop` in `ruleaction_2.rs` is a `// STUB(W6)` local resolver standing
in for C++ `glb->inst[opc]`. It enumerates only the opcodes the author expected this
rule batch to emit, and panics on anything else. `CPUI_INT_SRIGHT` is missing, but
two rules in the file can pass it through — `RuleAndCommute` (`:554`,
`op_set_opcode(data, op, opc)` with the caller's opcode) and the shift-equality arm
at `:1916` (`CPUI_INT_LEFT | CPUI_INT_RIGHT | CPUI_INT_SRIGHT`).

Fix is one line in the `op_typeop` match:

```rust
OpCode::CPUI_INT_SRIGHT => (f::binary, "s>>"),
```

The same line already exists, identical, in two sibling shims —
`p3_dataflow/ruleaction_6.rs:198` and `substrate/funcdata_op.rs:1760` — which
confirms the flag word. Not applied here (kuna checkout is the user's, and the
signatures work there is deliberately uncommitted).

Side finding while confirming: `p3_dataflow/ruleaction_7.rs:78` has
`CPUI_INT_SRIGHT => (f::binary, ">>")` — the mnemonic should be `"s>>"`. Cosmetic
(the flag word is right), but it will mislabel signed shifts in any debug dump.

## 5. Conclusions

- Nothing to change in BsimVis's analyzer profile. It reproduces stock Ghidra
  bit-for-bit.
- The NOTES "kuna finds way more functions" result should be restated: after
  dedup, kuna finds 5861 vs Ghidra's 4714, and the difference is thunks (correctly
  filtered), import slots (kuna false positives), and one real class of Ghidra
  miss confined to win_x32.
- The one actionable BsimVis improvement is the create-functions-from-code-symbols
  pass: +24 real functions per win_x32 binary, including eight of the corpus's own
  test functions, zero false positives elsewhere. It closes the gap only on
  symbol-bearing binaries.
- kuna is genuinely better at *unreferenced code*, symbols or not — the stripped
  control proves it sweeps rather than reads symbols — and genuinely worse at
  *distinguishing import-table slots from code*. Neither is a signature-algorithm
  difference.
- NOTES confounder #1 is resolved; steps 2 and 3 there (audit
  `collect_block_sigs`/`initialize_blocks` for the DUAL_FLOW 94.3% miss, and p-code
  diff `array_sum`) are now the only remaining explanations for the vector gap.

## Reproduction

Scripts used (headless Ghidra, `-scriptPath` alongside the binary list):
`CountFuncs.java` (per-class function counts), `DumpSigs.java` (BSim vectors via
`debugSignatures`), `DumpEntries.java` (entry addresses), `ProbeExtras.java`
(classify kuna-only addresses), `AggrOpts.java` (prescript forcing analyzers on),
`FuncsFromSyms.java` (the proposed fix), `Refs.java` (references to missed
entries). kuna side: `kuna signatures <binary>` from
`~/github/kuna/decompiler/target/release/kuna`. Stripped control:
`cp v01_win_x32.exe strip32.exe && strip strip32.exe`, then the same
`CountFuncs.java` / `DumpEntries.java` / `kuna signatures` pair.
