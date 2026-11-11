# cap_hardware_watchdog_keepalive

Rule `Linux_Capability_WatchdogHandle` (kind capability, confidence low). MITRE ATT&CK T1562.

## What it detects
Opens /dev/watchdog and /dev/misc/watchdog (Mirai keeps it fed / disabled so the device does not reboot).

## Evidence
- `string` "/dev/watchdog" (63 files) - plain
- `string` "/dev/misc/watchdog" (63 files) - plain
- `string` "/bin/watchdog" (12 files)
- `xor22` "\rFGT\rUCVAJFME\"" (49 files) - xor22-encoded
- `xor54` "{01\"{#5 7<0;3T" (22 files) - xor54-encoded

Catalogue holders: 63. Spread in the catalogue: {"p": 26, "MD5.unpacked": 11, "MD5": 11, "MD5MD5": 10, "MD5MD5.elf": 3, "": 2}. Spread of the rule: 63 catalogue holders plus ~100 more across boatnet, josho, demons, p_killermaps (all Mirai-descended).

## Rule
Opens the hardware watchdog (/dev/watchdog together with /dev/misc/watchdog in clear, or /dev/watchdog XOR-encoded with 0x22 or 0x54) so the device is kept alive. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 63, fn 0 (recall 1.0).
- hits outside the holder list: 101 (boatnet_xor22 29, p_killermaps 3, josho_flood 9, demons 8, mtab_manji 1). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.3841.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073887903 `table_init` (33 members), 8 hit files, ARM, MIPS, sparc, x86, via $w3, $w5
- cluster 1073887030 `evade_bg` (4 members), 4 hit files, MIPS, via $w1, $w2
- cluster 1073853772 `evade_bg` (4 members), 4 hit files, ARM, via $w1, $w2
- cluster 1073882194 `FUN_0000e510` (3 members), 3 hit files, ARM, via $w1, $w2
- cluster 1073809455 `main` (3 members), 3 hit files, ARM, via $w1, $w2
- cluster 1073805632 `FUN_0804d120` (3 members), 3 hit files, x86, via $w1, $w2

Implemented in: table_init 1073887903 holds the encoded entry; evade_bg 1073853772 (ARM) / 1073887030 (MIPS) / 1073837945 (AARCH64) and main/FUN_0000e510/FUN_0804d120 hold the plain open() paths.

## Gaps
Weak alone by design: nearly every Mirai descendant opens the watchdog, so the hits outside the 63 catalogue holders are real holders, not noise. Use as one capability of a profile, never to classify.
