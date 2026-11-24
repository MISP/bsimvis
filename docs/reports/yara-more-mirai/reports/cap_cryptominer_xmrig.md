# cap_cryptominer_xmrig

Rule `Linux_Capability_CryptoMinerXmrig` (kind capability, confidence high). MITRE ATT&CK T1496.

## What it detects
XMRig config/usage strings, stratum URLs, cryptonight algo names, donate servers.

## Evidence
- `string` "no valid configuration found, try https://xmrig.com/wizard" (8 files)
- `string` "donate.v2.xmrig.com" (6 files)
- `string` "stratum+tcp://" (8 files)
- `string` "cryptonight-monerov8" (8 files)
- `string` "donate-over-proxy" (8 files)

Catalogue holders: 8. Spread in the catalogue: {"MD5.unpacked": 8}. Spread of the rule: 8 unpacked MD5-named files (ARM, AARCH64, x86).

## Rule
Bundled XMRig miner: config wizard message, donate servers, stratum+tcp, cryptonight-monerov8 algo name (3 of 5). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 8, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073888564 `FUN_0805b300` (4 members), 2 hit files, x86, via $m4
- cluster 1073873296 `FUN_00525a80` (2 members), 2 hit files, x86, via $m2
- cluster 1073872472 `FUN_00460930` (2 members), 2 hit files, x86, via $m3
- cluster 1073871993 `FUN_00412460` (2 members), 2 hit files, x86, via $m4
- cluster 1073886252 `FUN_004457c0` (2 members), 2 hit files, x86, via $m5
- cluster 1073888344 `FUN_081545c0` (2 members), 2 hit files, x86, via $m1

Implemented in: x86 library functions only (FUN_0805b300, FUN_00525a80, FUN_00460930, ...), no named clusters.

## Gaps
filesize bound is 16MB because the statically linked miners are large.
