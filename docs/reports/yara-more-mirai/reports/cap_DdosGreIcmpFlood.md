# Linux_Capability_DdosGreIcmpFlood

Capability: gre_flood,icmp_flood (gre_ip,gre_eth,icmp_flood). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

GRE and ICMP flood: '[icmp_flood]' worker logs, or ARM prologues of attack_gre_ip / attack_gre_eth / attack_method_icmpflood.

## Evidence

### ddos_gre_icmp_flood (25 holders)

attack_gre_ip / attack_gre_eth encapsulated floods and ICMP echo flood.

- [string] `"[icmp_flood] started: ('%d')\n"` in 5 files

## Spread (groups of every file the rule hits)

josho_flood:11, boatnet_xor22:8, p_killermaps:7, demons:4, nuclear:2, frosted:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_gre_ip[1073856790] hit=20/20 (ARM)
- attack_gre_eth[1073886374] hit=18/18 (ARM)
- FUN_0001784c[1073886182] hit=2/2 (ARM)
- FUN_00014f74[1073887469] hit=2/2 (ARM)
- attack_method_netbios_datagram[1073842619] hit=2/2 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_gre_icmp_flood`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 25, fn 0, fp 9, precision 0.7353, recall 1.0.
- GRE handlers carry no strings: ARM only. Non-ARM GRE builds are not covered.

## Known gaps

GRE on MIPS/x86 builds; ICMP only matched where the log strings exist.
