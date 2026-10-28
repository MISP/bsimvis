# Linux_Capability_DdosSsdpMsearch

Capability: ssdp_reflection (ssdp_msearch_amplification). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

SSDP M-SEARCH reflection payload (HOST 255.255.255.255:1900, ST urn:dial-multiscreen-org:service:dial:1).

## Evidence

### ssdp_msearch_amplification (45 holders)

Embedded 'M-SEARCH * HTTP/1.1 ... ssdp:discover' datagram (HOST 255.255.255.255:1900), SSDP reflection/flood payload.

- [string] `'M-SEARCH * HTTP/1.1\r\nHOST: 255.255.255.255:1900\r\nMAN: "ssdp:discover"\r\nMX: 1\r\nST: urn:dial'` in 45 files
- [string] `'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: "ssdp:discover"\r\nMX: 1\r\nST: ssdp:all'` in 1 files

## Spread (groups of every file the rule hits)

boatnet_xor22:56, nuclear:13, josho_flood:12, rival_killer:7, unclassified:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- attack_method_nudp[1073831300] hit=8/10 (ARM)
- FUN_0000e898[1073878033] hit=8/10 (ARM)
- FUN_0804e590[1073887464] hit=4/5 (x86)
- FUN_10005d04[1073883027] hit=4/5 (PowerPC)
- FUN_80005402[1073878195] hit=4/5 (68000)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ssdp_msearch_amplification`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 45, fn 0, fp 44, precision 0.5056, recall 1.0.
- Hits 44 files outside the holder list: they hold the identical string (Ghidra merged it into longer data), so tp+fp is the honest count (89).

## Known gaps

The multicast 239.255.255.250 spelling (one MIPS sample) is carried in the rule but not seen in this corpus by raw scan.
