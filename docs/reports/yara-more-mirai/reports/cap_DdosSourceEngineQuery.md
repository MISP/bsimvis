# Linux_Capability_DdosSourceEngineQuery

Capability: vse_flood (source_engine_query_flood,udp_flood). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Valve Source Engine query flood: plain 'TSource Engine Query' with its escaped /x54/x53.. twin (gafgyt), the Mirai table entry XOR 0x22, or the ARM attack_udp_vse prologue.

## Evidence

### ddos_vse_source_engine_query (12 holders)

Valve Source Engine query payload flood (attack_udp_vse / vseattack).

- [string] `'TSource Engine Query + /x54/x53/x6f/x75/x72/x63/x65/x20/x45/x6e/x67/x69/x6e/x65/x20/x51/x7'` in 12 files
- [string] `'TSource Engine Query + /x54/x53/x6f/x75/x72/x63/x65/x20/x45/x6e/x67/x69/x6e/x65/x20/x51/x7'` in 12 files
- [string] `'SNQUERY: 127.0.0.1:AAAAAA:xsvr'` in 55 files

## Spread (groups of every file the rule hits)

boatnet_xor22:28, p_killermaps:18, tsun_net_series:12, tsun_holocaust:2, josho_flood:1, demons:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- vseattack[1073796272] hit=3/3 (MIPS,MIPS)
- vseattack[1073837228] hit=2/2 (ARM)
- attack_udp_vse[1073856796] hit=1/20 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_vse_source_engine_query`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 12, fn 0, fp 126, precision 0.087, recall 1.0.
- The Mirai XOR-0x22 table entry is the bulk of hits (about 105 non-holder files); these are real VSE implementers, not false positives. Holder list in capabilities.json counts only the gafgyt text variant.

## Known gaps

Mirai builds with a different XOR key are missed.
