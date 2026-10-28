# nuclear

Rule: `Linux_Botnet_Mirai_Nuclear` (kind variant, confidence high).

## Evidence
- Loader host `raw.flameblox.com` in 13/13 files, 0 outside the group.
- Shares boatnet's XOR-0x22 encoded attack table (`HWCLVGAJ`, `QOACFOKL`, `QWRRMPV`, `RCQQUMPF`; 12/13) and the Valve `TSource Engine Query` flood string (13/13, but also in 25 other files).

## What it does
Boatnet-lineage Mirai with a flameblox raw-paste loader (ftpget) and the TSource query flood.

## Rule P/R
Condition: ELF, `filesize < 3MB`, host AND (any XOR string OR TSource).

| set | files | hits |
|---|---|---|
| nuclear | 13 | 13 |
| siblings (boatnet_xor22) | 105 | 0 |
| unrelated | 297 | 0 |

## Gaps
The host is the only discriminator against boatnet; a rebuild with another loader would pass as boatnet. No byte windows: the code is identical to boatnet.
