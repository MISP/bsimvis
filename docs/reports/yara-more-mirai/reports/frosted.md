# frosted

Rules: `Linux_Botnet_Mirai_Frosted` (variant, high) and `Linux_Botnet_Mirai_Frosted_Locker` (variant, medium).

## Evidence
- Banner `FROSTED IS HERE NIGGA` in 7/7 files; `[anti-debug] DEBUGGER DETECTED (%s) - EXITING` and `PTRACE DETECTED` in 5/7; `ROBLOXSPAM` attack name in 3/7.
- Two files filed under josho_flood (`Josho.ppc` bdb43140..., `Josho.sh4` feff91ef...) have the same anti-debug and `ROBLOXSPAM` code without the banner, plus `[locker] killed cmd: %s (pid: %d)` and `[killer] killed mirai (pid: %d)`, and hard-coded IPs (87.121.112.57 in sh4). They are a Frosted-lineage build (the `Locker` rule).

## What it does
Mirai with ptrace/debugger detection that exits, a `[killer]` and `[locker]` that log killed processes, and a ROBLOXSPAM flood.

## Rule P/R
| rule | set | files | hits |
|---|---|---|---|
| Frosted | frosted | 7 | 7 |
| Frosted | unrelated (incl. josho_flood) | 408 | 0 |
| Frosted_Locker | its 2 members | 2 | 2 |
| Frosted_Locker | frosted | 7 | 0 |
| Frosted_Locker | unrelated | 394 | 0 |

## Gaps
Frosted requires the banner, so a banner-stripped build with only anti-debug strings falls to Locker or to nothing.
