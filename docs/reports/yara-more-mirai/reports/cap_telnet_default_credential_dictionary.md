# cap_telnet_default_credential_dictionary

Rule `Linux_Capability_TelnetDefaultCreds` (kind capability, confidence high). MITRE ATT&CK T1078.001,T1110.001.

## What it detects
Mirai/DEMONS-style credential list in clear: xc3511, vizxv, 7ujMko0vizxv, telnetadmin, telecomadmin, smcadmin, Zte521, dvr2580222, juantech ...

## Evidence
- `string` "xc3511" (14 files)
- `string` "vizxv" (14 files)
- `string` "7ujMko0vizxv" (13 files)
- `string` "telnetadmin" (16 files)
- `string` "telecomadmin" (16 files)
- `string` "smcadmin" (16 files)
- `string` "Zte521" (14 files)
- `string` "dvr2580222" (12 files)

Catalogue holders: 14. Spread in the catalogue: {"DEMONS": 12, "bin": 1, "MD5": 1}. Spread of the rule: demons, zbot_static, a few boatnet unpacked builds (the catalogue lists 14 DEMONS-named holders; the rule also reaches the zbot_static statics).

## Rule
Plaintext Mirai-style telnet default-credential dictionary (xc3511, 7ujMko0vizxv, telnetadmin, telecomadmin, Zte521 ...), 5 of 11 entries. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 14, fn 0 (recall 1.0).
- hits outside the holder list: 17 (demons 6, zbot_static 10, boatnet_xor22 1). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.4516.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073888319 `FUN_004083cc` (2 members), 2 hit files, MIPS, via $c1, $c10, $c11, $c2, $c3, $c4, $c5, $c6, $c7, $c8, $c9
- cluster 1073856098 `scanner_init` (2 members), 2 hit files, ARM, via $c1, $c10, $c11, $c2, $c3, $c4, $c5, $c6, $c7, $c8, $c9
- cluster 1073766605 `FUN_0000e3cc` (2 members), 2 hit files, ARM, via $c1, $c10, $c11, $c2, $c3, $c4, $c5, $c6, $c7, $c8, $c9
- cluster 1073845860 `FUN_00410c9c` (2 members), 2 hit files, MIPS, via $c1, $c10, $c11, $c2, $c3, $c4, $c5, $c6, $c8, $c9
- cluster 1073755735 `FUN_0000f9dc` (2 members), 2 hit files, ARM, via $c1, $c10, $c11, $c2, $c3, $c4, $c5, $c6, $c7, $c8, $c9
- cluster 1073883784 `FUN_800000f6` (92 members), 1 hit files, 68000, via $c1, $c9

Implemented in: scanner_init clusters 1073856098 (ARM), FUN_004083cc 1073888319 and FUN_00410c9c 1073845860 (MIPS), FUN_0000e3cc/FUN_0000f9dc (ARM): the credential table is referenced from the scanner init.

## Gaps
Hits in zbot_static and DEMONS files absent from the catalogue pivot are true holders (Ghidra merges adjacent table strings). XOR-encoded credential tables are not covered.
