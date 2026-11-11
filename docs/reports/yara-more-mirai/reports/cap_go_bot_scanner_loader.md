# cap_go_bot_scanner_loader

Rule `Linux_Capability_GoBotScannerLoader` (kind capability, confidence low). MITRE ATT&CK T1090,T1498,T1595.

## What it detects
Go-built bot: 'Scanner started. Payload (Loader)', tcp-stomp, minecraft, xmrig-x86 in the Go string pool.

## Evidence
- `string` "struct contains unexported fieldsAuto-reconnecting (120s limit)...\nScanner started. Payloa" (1 files) - Go pooled string

Catalogue holders: 1. Spread in the catalogue: {"": 1}. Spread of the rule: 1 file (MIPS64) plus its go_bot_b sibling.

## Rule
Go-built bot: pooled strings 'Auto-reconnecting (120s limit)' and 'Scanner started. Payload' (Loader). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (Go string pool).

## Measured
- vs catalogue holders: tp 1, fn 0 (recall 1.0).
- hits outside the holder list: 1 (go_bot_b 1). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.5.

## Cluster landing (POST /api/yara/hitmap)
- no hit lands in a named or clustered function (strings sit outside clustered code)

Implemented in: Go binary, no function cluster (string pool only).

## Gaps
Single reference sample; the sibling hit in go_bot_b is a true holder of the same pool.
