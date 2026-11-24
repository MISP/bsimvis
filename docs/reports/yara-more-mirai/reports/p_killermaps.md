# p_killermaps

64 files, 9 arches (arm 20, x86 13, mips-le 8, mips-be 7, x86_64 4, ppc 4, m68k 4, sh4 3, sparc 1). 16 are UPX originals with no strings; 48 carry a string table.

## Evidence

- `[0clKillerMaps] Killed Process: %s, PID: %d`, `[0clKillerKillerEXE] Killed process: %s, PID: %d`, `[0clKillerStat] Killed Process: %s, PID: %d`: a /proc walker that kills by `/proc/%d/maps`, `/proc/%d/exe` and `/proc/%d/stat`. Zero hits in any other group or in the unclassified files.
- `im in deep sorrow.`: exclusive to this group. The strings API reports 38 files, a raw byte scan finds it in all 48 stringed files (Ghidra's table misses some).
- `/dev/watchdog` and `/dev/misc/watchdog` handling, the `abcdefghijklmnopqrstuvw012345678` alphabet (random name generator) and the XOR-0x22 table (`LAMPPGAV"`, `GLC@NG"`, `Q[QVGO"`). The table is shared with boatnet, so the rules do not use it.
- Kill log strings sit in one function each (e.g. `FUN_0000d98c` in the arm6 build, `FUN_80004a0c` on m68k).

## What it does

Stock Mirai with a /proc killer that logs each kill, watchdog handling, and in 27 of 48 builds an HTTP flood (Chrome-58 User-Agent, urlencoded POST).

## Sub-variants

Strings are the same across all stringed members except one axis, measured by raw byte scan:

- HTTP flood present (27): `HTTP/1.1\r\nHost: `, `application/x-www-form-urlencoded`, `Mozilla/5.0 (Windows NT 10.0; Win64; x64)`. Mostly arm, x86, mipsel.
- No HTTP flood (21): mips/mpsl, m68k, ppc, sh4 and some arm.
- `/proc/cpuinfo` is added in two arm builds (9de850.., d86825..): too small for a rule.
- No split by sorrow or by Stat: they are in every stringed file.

## Rules (rules/B.yar)

| rule | kind | hits in group | siblings (193) | unrelated (158) + unclassified (21) |
|---|---|---|---|---|
| Linux_Botnet_Mirai_PKillerMaps | family | 48/48 unpacked (48/64) | 0 | 0 |
| Linux_Botnet_Mirai_PKillerMaps_HttpFlood | variant | 27 | 0 | 0 |
| Linux_Botnet_Mirai_PKillerMaps_Lean | variant | 21 | 0 | 0 |

Siblings tested: boatnet, nuclear, xor_wbyy, demons, josho, flylegit, frosted, rival_killer. Lint is clean.

## Cross-group capabilities (for capability rules, not written here)

- Killer: p_killermaps' `[0clKiller*]` walker is exclusive; the XOR-0x22 killer words are shared with boatnet and nuclear.
- `HTTP/1.1\r\nHost` + Chrome-58 UA + urlencoded POST appears in 92 other files, so it is an HTTP-flood capability across families.
- The `deep sorrow` string never leaves this group.

## Gaps

- 16 UPX originals are not covered (no strings); use the unpacked sibling.
- The Lean rule is a negation-based complement, medium confidence.
- No byte-window seeds written: string recall is already 100% on unpacked members.
