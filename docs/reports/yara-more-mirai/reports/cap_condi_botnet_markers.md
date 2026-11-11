# cap_condi_botnet_markers

Rule `Linux_Capability_CondiMarkers` (kind capability, confidence high). MITRE ATT&CK T1036.

## What it detects
/var/condibot, /var/CondiBot, /var/condinet, /tmp/condinetwork.

## Evidence
- `string` "/var/condibot" (11 files)
- `string` "/var/CondiBot" (11 files)
- `string` "/var/condinet" (11 files)
- `string` "/tmp/condinetwork" (11 files)

Catalogue holders: 11. Spread in the catalogue: {"MD5.unpacked": 9, "MD5": 2}. Spread of the rule: 11 MD5-named files; 2 boatnet-lineage unpacked builds also carry the paths.

## Rule
Condi botnet path markers /var/condibot, /var/CondiBot, /var/condinet, /tmp/condinetwork (2 of 4). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 11, fn 0 (recall 1.0).
- hits outside the holder list: 2 (boatnet_xor22 2). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.8462.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073888220 `FUN_0040d080` (3 members), 3 hit files, x86, via $q1, $q2, $q3, $q4
- cluster 1073878036 `FUN_000166b0` (3 members), 3 hit files, ARM, via $q1, $q2, $q3, $q4
- cluster 1073887408 `killer_condi` (2 members), 2 hit files, ARM, SuperH4, via $q1, $q2, $q3, $q4

Implemented in: killer_condi 1073887408 (ARM, SuperH4), FUN_0040d080 1073888220 (x86), FUN_000166b0 1073878036 (ARM).

## Gaps
The 2 boatnet_xor22 hits carry all four paths, so they are Condi-derived rather than noise (check against Linux_Botnet_Mirai_Condi2_Top1hbt).
