# cap_self_copy_proc_hide

Rule `Linux_Capability_SelfCopyProcHide` (kind capability, confidence high). MITRE ATT&CK T1036,T1564.001.

## What it detects
'rm -rf %s;cp %s %s' relocation and 'mount --bind /tmp /proc/%d' to hide the process from /proc.

## Evidence
- `string` "mount --bind /tmp /proc/%d" (9 files)
- `string` "rm -rf %s;cp %s %s" (9 files)

Catalogue holders: 9. Spread in the catalogue: {"MD5": 9}. Spread of the rule: the 9 MD5-named TCP_MSGHEAD files.

## Rule
Self-relocation 'rm -rf %s;cp %s %s' with 'mount --bind /tmp /proc/%d' to hide the process from /proc. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 9, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073840899 `copy_file` (6 members), 4 hit files, ARM, PowerPC, via $h2
- cluster 1073886422 `FUN_00010b28` (3 members), 3 hit files, ARM, via $h1
- cluster 1073886920 `FUN_00013510` (2 members), 2 hit files, x86, via $h2
- cluster 1073887986 `init_persistence` (5 members), 1 hit files, MIPS, via $h2
- cluster 1073750758 `FUN_000122dc` (2 members), 1 hit files, MIPS, via $h2
- cluster 1073750757 `init_hidden` (2 members), 1 hit files, MIPS, via $h1

Implemented in: copy_file 1073840899 (ARM, PowerPC), FUN_00010b28 1073886422 (ARM), init_hidden 1073750757 (MIPS), init_persistence 1073887986 (MIPS).

## Gaps
none known
