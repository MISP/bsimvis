# boatnet_xor22

107 files: 64 unpacked, 43 UPX originals with no strings (the unpacked sibling carries the rule). Mirai fork with the config table XOR-encoded with 0x22; the encoded entries are matchable directly (`\rQVCVWQ"` decodes to `stats`, the trailing `"` is the encoded NUL).

## Evidence
Per-file string signatures over the 64 unpacked files show three real sub-builds and a few strays:

| sub-build | files | markers |
|---|---|---|
| netis | ~27 | `Self Rep Fucking NeTiS and Thisity...`, `SNQUERY: 127.0.0.1:AAAAAA:xsvr`, SSDP M-SEARCH (dial-multiscreen), table entries `GLVGP"`, `NKLWZQJGNN"`, `LAMPPGAV"` |
| tftp | ~16 | loader grammar `tftp %s -c get`, `cd %s && tftp -g -r`, `ftpget -v -u anonymous`, table entries `FTPjGNRGP"`, `\rRPMA\rARWKLDM"` |
| glvgp, no banner | ~9 | netis table, no banner (a trimmed netis build) |
| strays | 3 | `bot.*` / `m-p.s-l.dick` with `UDP-GBPS`, `definitely-not.gay`; carry only part of the table |

Capabilities from strings: UDP/SSDP/Source-query floods, telnet scanner, killer words, netis backdoor spread (banner), tftp/ftpget/curl/wget loader (tftp build).

## Rules (all in rules/A.yar, saved to the workbench)
- `Linux_Botnet_Mirai_Boatnet` (family): 2 of 7 table entries, or SNQUERY plus banner. 58/62 scanned non-packed members; 2 hits in iran_dg (`bot.mips`, `bot.mpsl`: banner and SNQUERY, no 0x22 table; same netis lineage); 0 unrelated.
- `Linux_Botnet_Mirai_Boatnet_Netis` (variant): banner plus 2 of table/SNQUERY/SSDP. 39 hits, same 2 iran_dg files, 0 unrelated.
- `Linux_Botnet_Mirai_Boatnet_Tftp` (variant): 2 table entries plus a loader command. 15/15, 0 siblings.

## Gaps
- 43 UPX originals: packer concern, not covered.
- 4 unpacked members with no table or banner at all (sha256-named stripped ELFs and `boatnet.sh4`); strings cannot reach them. Byte seeds not attempted (the functions are libc-dominated).
- The split "netis vs tftp" is clean; "glvgp no banner" is covered by the family rule only.
- iran_dg `bot.*` is probably a boatnet descendant; the grouping may want to move them.
