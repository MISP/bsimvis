# cap_competitor_killer_by_name_port

Rule `Linux_Capability_KillerByNameOrPort` (kind capability, confidence medium). MITRE ATT&CK T1489,T1562.001.

## What it detects
killer_kill_by_port, killer_mirai_exists/init, '[killer] killed mirai', pkill/killall command lines, auditctl/snoop kill.

## Evidence
- `string` "[killer] killed: %s (pid: %d) exe: %s\n" (2 files) - josho
- `string` "[killer] killed mirai (pid: %d)\n" (2 files)
- `string` "[killer] starting memory scan on (pid=%d)\n" (1 files)
- `string` "pkill -9 -x %s 2>/dev/null &" (10 files) - od_ family
- `string` "pkill -9 -f [%c]%s 2>/dev/null &" (10 files)
- `string` "auditctl -e 0 2>/dev/null &pkill -9 [a]uditd 2>/dev/null &pkill -9 -f [s]noop 2>/dev/null " (10 files) - disables auditd/snoop
- `string` "killall -9 telnetd utelnetd scfgmgr" (1 files)
- `string` "[watch_time] kill() pid: ('%d'), exit()\n" (11 files) - self-timeout watchdog

Catalogue holders: 33. Spread in the catalogue: {"MD5MD5": 10, "p": 8, "MD5.unpacked": 6, "DEMONS": 2, "nuclear": 2, "MD5MD5.elf": 2, "Josho": 2, "MD5": 1}. Spread of the rule: od_ kit (10), josho_flood (2), kushnet-style rival kill, plus ARM p_killermaps/nuclear/DEMONS via byte windows.

## Rule
Killer that matches rivals by name, port or exe path: '[killer] killed mirai', pkill -9 -x/-f command lines, auditctl/snoop kill, or ARM windows of killer_exe/maps/stat and killer_kill_by_port. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: strings arch-independent; byte windows ARM only.

## Measured
- vs catalogue holders: tp 18, fn 15 (recall 0.5455).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073882187 `killer_mirai_exists` (11 members), 6 hit files, ARM, via $b5
- cluster 1073844834 `killer_exe` (6 members), 6 hit files, ARM, via $b1
- cluster 1073844836 `killer_maps` (6 members), 6 hit files, ARM, via $b2
- cluster 1073844835 `killer_stat` (6 members), 6 hit files, ARM, via $b3
- cluster 1073887030 `evade_bg` (4 members), 4 hit files, MIPS, via $n4, $n5, $n6
- cluster 1073853772 `evade_bg` (4 members), 4 hit files, ARM, via $n4, $n5, $n6

Implemented in: evade_bg 1073853772 (ARM), 1073887030 (MIPS), 1073837945 (AARCH64) for pkill/auditctl; killer_mirai_exists 1073882187, killer_exe/maps/stat 1073844834-6, killer_kill_by_port 1073856086 (ARM) for byte windows.

## Gaps
Fragmented capability: 15/33 catalogue holders are ARM builds whose windows differ and are not matched. No false hit measured.
