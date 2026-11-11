# Linux_Capability_DdosGameProtocol

Capability: game_protocol_flood (discord_flood,vse_flood,custom_udp,minecraft_flood,fivem_flood). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Game and service-specific floods: Go bot Methods.Start* table, tiny MIPS method table (UDP-VSE/TCP-FIVEM/MINECRAFT), or ARM attack_udp_vse/custom/discord prologues.

## Evidence

### ddos_game_protocol_floods (36 holders)

Specialised attack handlers named attack_tcp_minecraft, attack_raknet_burst, attack_udp_discord, attack_udp_openvpn, attack_udp_ragnarok.

- [string] `'MINECRAFT'` in 2 files

## Spread (groups of every file the rule hits)

boatnet_xor22:12, go_bot_a:8, p_killermaps:6, demons:4, frosted:2, udp_vse_tiny:2, josho_flood:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_udp_vse[1073856796] hit=20/20 (ARM)
- attack_udp_custom[1073887637] hit=10/10 (ARM)
- attack_udp_discord[1073831547] hit=4/4 (ARM)
- FUN_0001564c[1073887999] hit=2/2 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_game_protocol_floods`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 36, fn 0, fp 0, precision 1.0, recall 1.0.
- Overlaps UdpFlood and SourceEngineQuery on purpose: those ARM handlers are the game-protocol floods.

## Known gaps

Rare per-sample method names (raknet, openvpn, ragnarok) are not individually covered.
