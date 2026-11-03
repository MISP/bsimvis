# Linux_Capability_C2GoogleDnsResolver

Capability: dns_resolve_google (resolve_cnc_addr,get_local_ip). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Hard-coded 8.8.8.8 resolver / local-IP lookup, plus ARM resolve_cnc_addr prologues.

## Evidence

### c2_dns_resolve_google (51 holders)

Hardcoded 8.8.8.8 resolver and resolve_cnc_addr().

- [string] `'8.8.8.8'` in 33 files

## Spread (groups of every file the rule hits)

tsun_net_series:12, boatnet_xor22:11, tsun_holocaust:10, zbot_static:10, cling_exploit_scanner:9, demons:4, unclassified:3, stun_probe:2, frosted:2, josho_flood:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- resolve_cnc_addr[1073856089] hit=12/12 (ARM)
- getOurIP[1073888658] hit=6/6 (ARM,PowerPC,SuperH4)
- getOurIP[1073886449] hit=5/5 (ARM,MIPS,MIPS)
- FUN_0001f190[1073887793] hit=2/4 (ARM)
- FUN_000119c0[1073888869] hit=2/2 (x86)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`c2_dns_resolve_google`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 47, fn 4, fp 19, precision 0.7121, recall 0.9216.
- Weakest rule of the set: 8.8.8.8 is a bare IP string. Hitmap shows most strings sit in getOurIP (connect to 8.8.8.8:53 to learn the local address), which is an IP-discovery step, and resolve_cnc_addr on ARM. 16 non-holder hits come from the string only; treat as medium-quality evidence of the pattern, not of a family.

## Known gaps

Single weak string; 4 holders (nuclear.arm6/7 and two UPX) unmatched.
