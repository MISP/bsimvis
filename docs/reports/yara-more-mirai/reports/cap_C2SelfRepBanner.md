# Linux_Capability_C2SelfRepBanner

Capability: self_rep_banner (netis_backdoor_spread). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

'Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG FoReHeAd We BiG L33T HaxErS' banner printed by self-replicating netis-backdoor spreaders.

## Evidence

### self_rep_banner (32 holders)

Verbose gafgyt/Qbot-style 'Self Rep Fucking NeTiS and Thisity ...' propagation banner.

- [string] `'Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG FoReHeAd We BiG L33T HaxErS\n'` in 32 files

## Spread (groups of every file the rule hits)

boatnet_xor22:41, tsun_net_series:11, tsun_holocaust:10, unclassified:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_method_ovhdrop[1073888073] hit=6/6 (ARM)
- FUN_0000e9d4[1073887985] hit=6/6 (ARM)
- FUN_0804ca20[1073885226] hit=3/3 (x86)
- FUN_00015000[1073805457] hit=3/3 (sparc)
- FUN_1000517c[1073883025] hit=3/3 (PowerPC)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`self_rep_banner`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 32, fn 0, fp 31, precision 0.5079, recall 1.0.
- Single anchor (lint warns). It is a family marker in rules/A.yar (Boatnet); here it is the capability signal and is intentionally the only evidence. 31 hits outside the holder list carry the identical string.

## Known gaps

No second independent anchor exists that is not already a Boatnet marker.
