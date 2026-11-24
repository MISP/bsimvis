# flylegit

Verdict: coherent. Rule `Linux_Botnet_Mirai_Flylegit` (variant, medium).

## Evidence
- `t.me/flylegit` and the banner "Looks like you are a honeypot, this tool was made by t.me/flylegit!" sit in 12 of 13 files and in nothing outside the lineage.
- Shared by 8 of 13: `udpplain`, `%31s %31s %7s %11s`; 7 of 13: C2 `176.65.139.59`.
- One build carries a downloader: `http://94.154.43.201/iran.%s`, `/tmp/iran.%s`, systemd/init.d/cron persistence. The `iran.<arch>` name ties this family to the iran_dg group.
- The 13th member, boatnet.x86_64, is the UPX original of the unpacked 2269684e..; it has no strings and is a separate packer concern.
- The file names (boatnet.m68k, boatnet.mipsrouter, boatnet.powerpc) are misleading: the code is not the boatnet XOR build.

## What it does
Honeypot check message, a UDP flood called `udpplain`, a space-separated command parser and a `pong %s` reply. Persistence and download strings appear in one build only.

## Rule and measurements
`Linux_Botnet_Mirai_Flylegit`: ELF, under 3 MB, banner or `t.me/flylegit`, or 2 of udpplain / fmt / C2 / iran URL / iran path.
- flylegit: 12/12 non-packed (12/13 with the UPX original).
- Unrelated families: 0 hits.
- Sibling lineage, 4 hits: three iran_dg MIPS builds (999c20be boatnet.mips, ce750fab, df1b7bbc iran.mipsel) and c5f3f9cd boatnet.sh4 (filed in boatnet_xor22). All four carry the banner, so they are real flylegit builds filed elsewhere.

## Gaps
No byte seeds; the UPX original is not covered. The group set should absorb the four hits above.
