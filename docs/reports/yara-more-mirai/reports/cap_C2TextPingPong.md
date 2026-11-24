# Linux_Capability_C2TextPingPong

Capability: c2_ping_pong (c2_text_commands). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

C2 text-command handler: 'pong %s' reply together with a command-parser anchor ('!udpplain', 'srcport=', '/proc/%s/comm').

## Evidence

### c2_text_ping_pong (32 holders)

Replies 'pong %s' to a PING command.

- [string] `'pong %s'` in 32 files
- [string] `'[main] ping received from cnc\n'` in 1 files
- [string] `'[main] cnc initiating lockdown to start\n'` in 1 files

## Spread (groups of every file the rule hits)

xnxn:22, flylegit:16, iran_dg:2, iran_procscan:2, static_bot_started:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- FUN_ram_00012f94[1073886164] hit=4/4 (RISCV,RISCV)
- FUN_00403934[1073841768] hit=4/4 (SuperH4)
- FUN_0804c854[1073851589] hit=2/2 (x86)
- FUN_0804a7f8[1073851576] hit=2/2 (x86)
- FUN_004058c0[1073781348] hit=2/2 (AARCH64)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`c2_text_ping_pong`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 32, fn 0, fp 11, precision 0.7442, recall 1.0.
- 'pong %s' sits in the same function as '!udpplain' (processCmd). 11 hits outside the holder list also carry 'pong %s' and '/proc/%s/comm' (iran, boatnet): the same handler family.

## Known gaps

PING/PONG literals are under 6 bytes so they are not usable as anchors.
