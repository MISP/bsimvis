# cap_anti_debug_ptrace_strace

Rule `Linux_Capability_AntiPtrace` (kind capability, confidence medium). MITRE ATT&CK T1622.

## What it detects
Aborts when traced ('[anti-debug] PTRACE DETECTED - EXITING'), checks for strace.

## Evidence
- `string` "[anti-debug] PTRACE DETECTED - EXITING\n" (7 files)
- `string` "strace" (17 files) - also libc? 17 files

Catalogue holders: 7. Spread in the catalogue: {"MD5": 3, "Josho": 2, "chrome": 1, "kkk": 1}. Spread of the rule: josho_flood (2), frosted (2), few MD5-named and chrome/kkk-named files.

## Rule
Anti-debug: exits when ptrace-traced, logging '[anti-debug] PTRACE DETECTED - EXITING'. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (string).

## Measured
- vs catalogue holders: tp 7, fn 0 (recall 1.0).
- hits outside the holder list: 2 (frosted 2). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.7778.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073886338 `FUN_0000ed10` (3 members), 3 hit files, ARM, x86, via $d1
- cluster 1073888035 `FUN_00406c60` (2 members), 2 hit files, PowerPC, SuperH4, via $d1
- cluster 1073842630 `anti_gdb_entry` (2 members), 2 hit files, ARM, via $d1

Implemented in: FUN_0000ed10 1073886338 (ARM, x86), FUN_00406c60 1073888035 (PowerPC, SuperH4), anti_gdb_entry 1073842630 (ARM).

## Gaps
Single anchor; the 2 frosted hits are true holders (the catalogue pivot missed them).
