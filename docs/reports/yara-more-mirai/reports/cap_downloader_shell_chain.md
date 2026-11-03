# cap_downloader_shell_chain

Rule `Linux_Capability_DropperShellChain` (kind capability, confidence high). MITRE ATT&CK T1059.004,T1105.

## What it detects
Format-string command lines that try each fetcher: wget URL -O, tftp -c get, tftp -g -r, ftpget anonymous, curl -o.

## Evidence
- `string` "wget http://%s/%s/%s -O %s" (16 files)
- `string` "tftp %s -c get %s %s" (16 files)
- `string` "cd %s && tftp -g -r %s %s" (16 files)
- `string` "ftpget -v -u anonymous -p anonymous -P 21 %s %s %s" (16 files)
- `string` "curl -o %s http://%s/%s/%s" (16 files)
- `string` "/bin/busybox" (35 files) - 35 files, generic

Catalogue holders: 16. Spread in the catalogue: {"MD5.unpacked": 11, "MD5": 3, "miniiupnpd": 1, "upnnpd": 1}. Spread of the rule: unpacked MD5-named builds (11), miniiupnpd/upnnpd-named (2), 3 MD5.

## Rule
Dropper command chain trying wget, tftp (two syntaxes), ftpget anonymous and curl via format strings (3 of 5). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 16, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073879292 `FUN_0040c578` (5 members), 5 hit files, MIPS, via $f1, $f2, $f3, $f4, $f5

Implemented in: FUN_0040c578 1073879292 (MIPS) holds all five templates; other arches stripped, unclustered.

## Gaps
none known
