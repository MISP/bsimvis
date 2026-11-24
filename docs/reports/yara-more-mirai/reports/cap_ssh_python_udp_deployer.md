# cap_ssh_python_udp_deployer

Rule `Linux_Capability_SshPythonSpread` (kind capability, confidence high). MITRE ATT&CK T1021.004,T1498.

## What it detects
sshpass -p ... ssh ... 'python udp.py' with ssh_server_ip config.

## Evidence
- `string` "sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s '%scd; python udp.py %s %s %s'" (11 files)
- `string` "ssh_server_ip" (11 files)

Catalogue holders: 11. Spread in the catalogue: {"cock": 1, "weednet": 1, "net": 1, "dicknet": 1, "fucknet": 1, "gaynet": 1, "swatnet": 1, "botnet": 1, "ballnet": 1, "unet": 1}. Spread of the rule: 'net'-named gafgyt (weednet, gaynet, dicknet, fucknet, cock, ballnet, swatnet, ...).

## Rule
Spread by ssh: sshpass -p ... ssh and 'python udp.py' with an ssh_server_ip config (2 of 3). Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 11, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073888522 `senditbudAMP` (7 members), 7 hit files, 68000, ARM, MIPS, PowerPC, via $s1, $s2, $s3
- cluster 1073812669 `senditbudAMP` (2 members), 2 hit files, x86, via $s1, $s2, $s3

Implemented in: senditbudAMP 1073888522 (m68k, ARM, MIPS, PowerPC) and 1073812669 (x86).

## Gaps
none known
