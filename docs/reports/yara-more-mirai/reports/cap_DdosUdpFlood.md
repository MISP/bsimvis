# Linux_Capability_DdosUdpFlood

Capability: udp_flood (udp_plain,udp_generic,udp_bypass,udp_hex). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

UDP flood capability: gafgyt '!udpplain/!udpcustom' command pair, Mirai-style '[udp_flood] started' worker logs, or the per-arch ARM prologues of attack_udp_plain/generic/bypass.

## Evidence

### ddos_udp_gafgyt_bang_commands (22 holders)

C2 command names '!udpplain' / '!udpcustom' compared against the received line; the UDP flood handlers they dispatch to.

- [string] `'!udpplain'` in 22 files
- [string] `'!udpcustom'` in 22 files

### ddos_udp_flood_workers (36 holders)

Mirai-derived attack_udp_* handlers: raw/DGRAM sockets, random payload via rand_next/rand_str, per-method opts through attack_get_opt_*.

- [string] `"[udp_plain_flood] started: ('%d')\n"` in 10 files
- [string] `"[udp_flood] started: ('%d')\n"` in 10 files
- [string] `"[udpbypass_flood] started: ('%d')\n"` in 5 files
- [string] `'[udp_flood] socket() failed'` in 9 files

## Spread (groups of every file the rule hits)

xnxn:20, boatnet_xor22:11, josho_flood:9, p_killermaps:5, nuclear:2, demons:2, frosted:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_udp_plain[1073888704] hit=22/24 (ARM)
- attack_udp_generic[1073886304] hit=18/18 (ARM)
- attack_udp_bypass[1073888039] hit=12/12 (ARM)
- attack_method_udphex[1073831543] hit=8/8 (ARM)
- FUN_00402360[1073881294] hit=5/5 (MIPS)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_udp_gafgyt_bang_commands`, `ddos_udp_flood_workers`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 58, fn 0, fp 1, precision 0.9831, recall 1.0.
- Hits every group that implements UDP floods, not one variant. The ARM prologues carry the stack-frame size of one build family, so they are support, not primary evidence. UPX originals carry no strings.

## Known gaps

UPX-packed originals; non-ARM Mirai builds without log strings have no byte window yet.
