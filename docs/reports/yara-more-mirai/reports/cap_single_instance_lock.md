# cap_single_instance_lock

Rule `Linux_Capability_SingleInstanceLock` (kind capability, confidence medium). MITRE ATT&CK T1480.

## What it detects
Lock file /tmp/.bot_lock and ensure_single_instance().

## Evidence
- `string` "/tmp/.bot_lock" (18 files)

Catalogue holders: 18. Spread in the catalogue: {"MD5.unpacked": 10, "xnxnxnxnxnxnxnxn": 6, "xnxnxnxnxnxnxnxnloon": 2}. Spread of the rule: xnxn (all arches incl. riscv/loongarch/m68k/sh4), one static_bot_started build.

## Rule
Single-instance lock file /tmp/.bot_lock (ensure_single_instance). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (string).

## Measured
- vs catalogue holders: tp 18, fn 0 (recall 1.0).
- hits outside the holder list: 5 (static_bot_started 1, xnxn 4). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.7826.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073887623 `FUN_ram_120000140` (4 members), 2 hit files, Loongarch, via $l1
- cluster 1073851558 `FUN_08049049` (2 members), 2 hit files, x86, via $l1
- cluster 1073779067 `FUN_ram_000100d4` (2 members), 2 hit files, RISCV, via $l1
- cluster 1073781316 `FUN_00400140` (2 members), 2 hit files, AARCH64, via $l1
- cluster 1073793041 `FUN_ram_00010158` (2 members), 2 hit files, RISCV, via $l1
- cluster 1073742271 `FUN_00400140` (2 members), 2 hit files, MIPS, via $l1

Implemented in: ensure_single_instance 1073888696; per-arch xrefs in FUN_ram_120000140 (Loongarch), FUN_08049049 (x86), FUN_00400140 (MIPS/AARCH64), FUN_ram_00010158 (RISCV).

## Gaps
Single anchor; the 4 xnxn hits with sh2/sh4 suffixes and static_bot_started are carriers of the same lock (merged strings), not unrelated families. Keep as a profile signal.
