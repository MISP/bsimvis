# cap_persistence_crontab_tmp_z

Rule `Linux_Capability_PersistCrontabTmpZ` (kind capability, confidence high). MITRE ATT&CK T1037.004,T1053.003.

## What it detects
crontab */5 * * * * {[ -x /tmp/.z ] || cp /var/.z ...}; also /etc/rc.local.

## Evidence
- `string` "(crontab -l 2>/dev/null; echo '*/5 * * * * { [ -x /tmp/.z ] || cp /var/.z /tmp/.z 2>/dev/n" (10 files)
- `string` "/tmp/.z" (12 files)
- `string` "/etc/rc.local" (10 files)

Catalogue holders: 12. Spread in the catalogue: {"MD5MD5": 11, "MD5MD5.elf": 1}. Spread of the rule: od_ kit (10 MD5MD5 files).

## Rule
Crontab persistence '*/5 * * * * { [ -x /tmp/.z ] || cp /var/.z /tmp/.z ...' or the /tmp/.z + /etc/rc.local + dropper chmod triple. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 10, fn 2 (recall 0.8333).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073886103 `adb_infect` (8 members), 8 hit files, ARM, MIPS, via $z2, $z4
- cluster 1073853716 `od_f5_infect.constprop.0` (8 members), 8 hit files, ARM, MIPS, via $z2
- cluster 1073887030 `evade_bg` (4 members), 4 hit files, MIPS, via $z1, $z2, $z3
- cluster 1073834082 `lol_priv_esc.isra.0` (4 members), 4 hit files, MIPS, via $z2
- cluster 1073853772 `evade_bg` (4 members), 4 hit files, ARM, via $z1, $z2, $z3
- cluster 1073853713 `payload_server` (4 members), 4 hit files, ARM, via $z2

Implemented in: evade_bg 1073853772 (ARM) / 1073887030 (MIPS) carry the full crontab line; adb_infect 1073886103 and od_*_infect clusters carry the /tmp/.z dropper.

## Gaps
Two static_sk_bot files from the catalogue were never hit; they hold neither the crontab line nor the triple.
