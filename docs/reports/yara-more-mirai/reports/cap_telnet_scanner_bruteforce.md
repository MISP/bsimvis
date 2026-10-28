# cap_telnet_scanner_bruteforce

Rule `Linux_Capability_TelnetBruteScanner` (kind capability, confidence medium). MITRE ATT&CK T1021,T1110.001,T1595.001.

## What it detects
scanner_init/scanner_thread/scanner_kill (+ adb/huawei/realtek/dlink scanner_init variants); telnet-brute log tags.

## Evidence
- `string` "telnet-brute" (10 files)
- `string` "%s|%s|telnet-brute" (10 files) - result record format
- `string` "/usr/sbin/telnetd" (18 files)

Catalogue holders: 21. Spread in the catalogue: {"MD5MD5": 9, "MD5.unpacked": 6, "nuclear": 2, "DEMONS": 2, "": 1, "MD5MD5.elf": 1}. Spread of the rule: od_ kit (10 MD5MD5 files); byte windows add ARM/MIPS/AARCH64 builds with scanner_* symbols.

## Rule
Telnet brute-force scanner: the 'telnet-brute' result record format of the od_ kit, or two per-arch scanner_thread/scanner_init byte windows. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: string path arch-independent; byte windows for ARM, MIPS BE/LE and AARCH64.

## Measured
- vs catalogue holders: tp 10, fn 11 (recall 0.4762).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073886103 `adb_infect` (8 members), 8 hit files, ARM, MIPS, via $t4
- cluster 1073853774 `lateral_thread` (4 members), 4 hit files, ARM, via $t1, $t2
- cluster 1073853775 `scanner_thread` (4 members), 4 hit files, ARM, via $b1, $t2
- cluster 1073766843 `lateral_thread` (2 members), 2 hit files, MIPS, via $t1, $t2
- cluster 1073766844 `scanner_thread` (2 members), 2 hit files, MIPS, via $b5, $t2
- cluster 1073837947 `lateral_thread` (2 members), 2 hit files, AARCH64, via $t1, $t2

Implemented in: lateral_thread 1073853774 (ARM), 1073766843 (MIPS), 1073837947 (AARCH64) for the strings; scanner_thread 1073853775 (ARM), 1073766844 (MIPS), 1073837948 (AARCH64) and scanner_init 1073856098 (ARM) for the byte windows.

## Gaps
Byte windows are build-specific (ARM literal-pool offsets): they hit identical builds only. Scanners of DEMONS.arm7/nuclear/boatnet-unpacked ARM builds are NOT covered; use Linux_Capability_TelnetLoginPromptTable and Linux_Capability_TelnetDefaultCreds for those.
