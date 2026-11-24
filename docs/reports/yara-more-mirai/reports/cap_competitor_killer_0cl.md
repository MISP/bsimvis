# cap_competitor_killer_0cl

Rule `Linux_Capability_KillerProcScan0cl` (kind capability, confidence high). MITRE ATT&CK T1057,T1489,T1562.

## What it detects
Scans /proc/*/{maps,exe,stat} and kills processes (competing malware), logs '[0clKiller*] Killed Process'.

## Evidence
- `string` "[0clKillerMaps] Killed Process: %s, PID: %d\n" (48 files)
- `string` "[0clKillerKillerEXE] Killed process: %s, PID: %d\n" (48 files)
- `string` "[0clKillerStat] Killed Process: %s, PID: %d\n" (40 files)
- `string` "/proc/%d/maps" (53 files) - generic but co-located
- `string` "/proc/%d/stat" (51 files)

Catalogue holders: 48. Spread in the catalogue: {"p": 31, "MD5.unpacked": 16, "MD5": 1}. Spread of the rule: p_killermaps (31), unpacked boatnet-lineage builds (16), 1 stripped MD5 file.

## Rule
Rival-process killer that scans /proc/*/{maps,exe,stat} and logs '[0clKiller*] Killed Process': any 2 of 3 log formats. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 48, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073881003 `FUN_00407844` (7 members), 7 hit files, MIPS, via $k1
- cluster 1073881002 `FUN_004072e4` (7 members), 7 hit files, MIPS, via $k2
- cluster 1073844836 `killer_maps` (6 members), 6 hit files, ARM, via $k1
- cluster 1073844834 `killer_exe` (6 members), 6 hit files, ARM, via $k2
- cluster 1073844835 `killer_stat` (6 members), 6 hit files, ARM, via $k3
- cluster 1073889304 `FUN_0804cac0` (5 members), 5 hit files, x86, via $k2

Implemented in: killer_maps 1073844836, killer_exe 1073844834, killer_stat 1073844835 (ARM); FUN_00407844/FUN_004072e4 (MIPS); FUN_0804cac0 (x86); m68k FUN_800000f6.

## Gaps
Strongest cross-arch capability anchor in the collection. UPX originals carry no strings.
