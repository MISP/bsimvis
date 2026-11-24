# Linux_Capability_DdosDnsFlood

Capability: dns_flood (dns_flood). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

DNS query flood handler (attack_udp_dns / attack_method_udpdns): ARM prologues only, the handler has no strings.

## Evidence

### ddos_dns_flood (12 holders)

attack_udp_dns / attack_method_udpdns handlers (random-label DNS queries). No unique strings: code-only.


## Spread (groups of every file the rule hits)

boatnet_xor22:6, demons:4, josho_flood:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- FUN_0000ed4c[1073886487] hit=10/10 (ARM)
- FUN_0000e28c[1073831554] hit=2/2 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_dns_flood`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 12, fn 0, fp 0, precision 1.0, recall 1.0.
- Code-only capability. Prologues encode two build families' stack sizes.

## Known gaps

No non-ARM coverage; no string anchor exists.
