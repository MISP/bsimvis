# Linux_Capability_DdosTcpFlood

Capability: tcp_flood (syn_flood,ack_flood,psh_ack_flood,tcp_bypass,stomp). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

TCP flood capability: '[syn_flood]/[ack_flood]/[tcpbypass_flood] started' worker logs, DOMINATE banner, STOMP stub text, or the ARM attack_tcp_syn prologue.

## Evidence

### ddos_tcp_flood_workers (46 holders)

attack_tcp_{syn,ack,stomp,bypass,legit,socket,xmas} handlers (raw TCP, handshake stomp, flag permutations).

- [string] `"[syn_flood] started: ('%d')\n"` in 5 files
- [string] `"[ack_flood] started: ('%d')\n"` in 5 files
- [string] `"[tcpbypass_flood] started: ('%d')\n"` in 10 files
- [string] `'[syn_flood] socket() failed'` in 4 files
- [string] `'DOMINATE Flooding %s for %d seconds.'` in 10 files
- [string] `'[INFO] STOMP attack is not implemented in this version.'` in 1 files

## Spread (groups of every file the rule hits)

josho_flood:11, tsun_net_series:11, boatnet_xor22:10, p_killermaps:6, demons:4, nuclear:2, frosted:2, sysd_dropper:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_tcp_syn[1073887222] hit=26/26 (ARM)
- processCmd[1073837257] hit=5/5 (ARM,MIPS,PowerPC)
- FUN_00402724[1073881295] hit=5/5 (MIPS)
- processCmd[1073887214] hit=3/3 (x86,x86)
- psh_ack_flood_wuhan[1073886510] hit=2/2 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_tcp_flood_workers`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 44, fn 2, fp 3, precision 0.9362, recall 0.9565.
- Three hits outside the holder list (sh4 josho, bin.x86_64 sysd_dropper, cock tsunami) carry the same strings and are true holders the string pivot missed.

## Known gaps

2 UPX originals (boatnet_xor22) are not matched; attack_tcp_ack/stomp clusters only reachable through the syn prologue on ARM.
