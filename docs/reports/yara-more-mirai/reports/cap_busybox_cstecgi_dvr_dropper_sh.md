# cap_busybox_cstecgi_dvr_dropper_sh

Rule `Linux_Capability_ExploitDvrCstecgi` (kind capability, confidence high). MITRE ATT&CK T1105,T1190.

## What it detects
Embedded /dvr script (rm fwr, busybox wget) and TOTOLINK cstecgi.cgi telnet-enable requests.

## Evidence
- `string` "#!/bin/sh\ncd /dvr\nrm -f /dvr/fwr /dvr/fwr_ver /dvr/fwr.tmp\n/bin/busybox wget -q http://%s:" (10 files)
- `string` "GET /cgi-bin/cstecgi.cgi?action=telnet&enable=1&password=%s&code=%s HTTP/1.1\r\nHost: %s\r\nCo" (10 files)
- `string` "/dvr/upgrade" (10 files)

Catalogue holders: 10. Spread in the catalogue: {"MD5MD5": 9, "MD5MD5.elf": 1}. Spread of the rule: od_ kit (10).

## Rule
TOTOLINK cstecgi.cgi telnet-enable request and the /dvr firmware-upgrade dropper script (2 of 3). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 10, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073834095 `od_totolink_infect.constprop.0` (4 members), 4 hit files, MIPS, via $d1
- cluster 1073853776 `od_totolink_infect.constprop.0` (4 members), 4 hit files, ARM, via $d1
- cluster 1073853780 `od_3r_infect.constprop.0` (4 members), 4 hit files, ARM, via $d2, $d3
- cluster 1073766849 `od_3r_infect.constprop.0` (2 members), 2 hit files, MIPS, via $d2, $d3
- cluster 1073837949 `od_totolink_infect.constprop.0` (2 members), 2 hit files, AARCH64, via $d1
- cluster 1073837953 `od_3r_infect.constprop.0` (2 members), 2 hit files, AARCH64, via $d2, $d3

Implemented in: od_totolink_infect 1073853776 (ARM), 1073834095 (MIPS), 1073837949 (AARCH64); od_3r_infect 1073853780 (ARM), 1073766849/1073834099 (MIPS), 1073837953 (AARCH64).

## Gaps
Overlaps ExploitKitOd by construction (same kit).
