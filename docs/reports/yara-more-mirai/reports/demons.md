# demons

Rule: `Linux_Botnet_Mirai_Demons` (kind variant, confidence high).

## Evidence
- 18 files, 11 architectures (`DEMONS.<arch>`). All 18 carry the plaintext D-Link model table (`wrgn39_dlob.hans_dir645_V1`, `wrgac01_dlob.hans_dir865`, `wapnd15_dlob_dap1522b`, `wrgn23_dlwbr_dir300b`, `wrgn28_dlob_dir412`) and the vendor credential list (`vertex25ektks123`, `$chwarzepumpe`, `CTLSupport12`, `Is$uper@dmin`, `AitbISP4eCiG`), plus the debug line `<<< %s(un='%s') = %u`. The suggest endpoint reports 14/18 because the string-table extraction misses four files; a raw byte match hits all 18.
- A second, XOR-0x54 encoded table (`{6=:{6!'-6;,t?=88tymtT`, terminated by `T`) is present in 16/18 files but also in 1-5 outside files, so it is not used as an anchor.

## What it does
Mirai scanner with a very large credential list drawn from router and CPE vendors, and a model-name table naming D-Link devices. Attack code is Mirai-standard (not examined further here).

## mushi: sub-variant or separate fork?
Separate fork. Mushi shares only two model names with DEMONS (`wrgnd08_dlob_dir815`, `wrgn49_dlob_dir600b`; both also appear in all DEMONS builds). It has none of the other model names, none of the credentials and not the `<<<` log line. Its code is a different bot (see the mushi report). Likely it copied a fragment of a public D-Link name list.

## Rule P/R
| set | files | hits |
|---|---|---|
| demons | 18 | 18 |
| siblings (mushi, nuclear) | 15 | 0 |
| unrelated | 382 | 0 |

Condition: ELF, `filesize < 3MB`, 2 of 5 models, 3 of 5 credentials, log line.

## Gaps
8 files were unreadable by the test endpoint (in the unrelated set). A future DEMONS build with a trimmed table would be missed.
