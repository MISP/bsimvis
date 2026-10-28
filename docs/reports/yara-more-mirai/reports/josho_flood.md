# josho_flood

Rule: `Linux_Botnet_Mirai_Josho_Flood` (variant, high).

## Evidence
Verbose bracketed logging unusual for Mirai: `[watch_time] started: ('%d') | seconds: ('%d')`, `[watch_time] kill() pid: ('%d'), exit()`, `[udp_plain_flood] started`, `[tcpbypass_flood] started`, `[udp_flood] started`, the `socket() failed` twins, also `[psh_ack_flood]` and `[icmp_flood]`. Static uClibc builds with dl-error text. 12/14 files carry some `_flood]` string.

## Group composition (mixed)
- 11 real Josho builds (MIPS BE, ARM, PPC, SH4, m68k, x86, MIPS LE): matched.
- `Josho.ppc` and `Josho.sh4`: Frosted lineage (anti-debug + ROBLOXSPAM + `[locker]`); see the frosted report and `Linux_Botnet_Mirai_Frosted_Locker`.
- one `mpsl` (b1ef9c2a944280c1a2ec04f2c1be3c70): only uClibc loader strings and IP 176.65.139.124; probably a stripped or dropper variant. Not matched.

## Rule P/R
| set | files | hits |
|---|---|---|
| josho_flood | 14 | 11 (11/12 real Josho) |
| siblings (xor_wbyy, p_killermaps) | 80 | 0 |
| unrelated | 321 | 0 |

Condition: ELF, `filesize < 3MB`, one `[watch_time]` line and 2 of the flood/killer lines.

## Gaps
The stripped mpsl needs a byte seed or the IP anchor (176.65.139.124 hits only that file). No hitmap/seed work was done because string recall on the real members is complete.
