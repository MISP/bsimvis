# cap_telnet_login_prompt_xor54_table

Rule `Linux_Capability_TelnetLoginPromptTable` (kind capability, confidence high). MITRE ATT&CK T1027,T1059.004,T1110.001.

## What it detects
Same table idea, key xor 0x54: decodes to '.anime', '/bin/busybox kill -9 ', 'nameserver ', prompts and /proc paths.

## Evidence
- `xor54` ":7;&&17 T" (22 files) - -> 'ncorrect'
- `xor54` "1:5681T" (22 files) - -> 'enable'
- `xor54` "'<188T" (22 files) - -> 'shell'
- `xor54` "'-' 19T" (22 files) - -> 'system'
- `xor54` "z5:=91T" (22 files) - -> '.anime'
- `xor54` "{6=:{6!'-6;,t?=88tymtT" (22 files) - -> '/bin/busybox kill -9 '
- `xor54` ":591'1&\"1&tT" (22 files) - -> 'nameserver '
- `xor54` "{01\"{#5 7<0;3T" (22 files) - -> '/dev/watchdog'

Catalogue holders: 22. Spread in the catalogue: {"DEMONS": 16, "": 4, "chernobyl": 1, "Josho": 1}. Spread of the rule: boatnet_xor22, p_killermaps, xnxn, josho_flood, demons (0x54 side: demons, chernobyl, josho).

## Rule
Mirai telnet login-prompt table (ncorrect, system, enable, assword) stored XOR-encoded with key 0x22 or 0x54: the scanner's login state machine. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (XOR-encoded constants; matches bytes with xor(0x22) / xor(0x54)).

## Measured
- vs catalogue holders: tp 102, fn 0 (recall 1.0).
- hits outside the holder list: 20 (p_killermaps 3, josho_flood 9, demons 2, boatnet_xor22 6). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.8361.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073887903 `table_init` (33 members), 21 hit files, ARM, MIPS, sparc, x86, via $a1, $a2, $a3, $a4
- cluster 1073888733 `table_init` (14 members), 8 hit files, ARM, MIPS, SuperH4, x86, via $b1, $b2, $b3, $b4
- cluster 1073882205 `FUN_0000f76c` (5 members), 4 hit files, ARM, via $a1, $a2, $a3
- cluster 1073844850 `table_init` (6 members), 3 hit files, ARM, via $a1, $a2, $a3
- cluster 1073842423 `FUN_0804e400` (3 members), 3 hit files, x86, via $a1, $a2, $a3
- cluster 1073881015 `FUN_00409648` (4 members), 2 hit files, MIPS, via $a1, $a2, $a3

Implemented in: table_init clusters 1073887903 (key 0x22; ARM, MIPS, sparc, x86) and 1073888733 (key 0x54; ARM, MIPS, SuperH4, x86); scanner reads the entries via table_retrieve_val 1073880296.

## Gaps
Not specific to one variant: it is the table scheme shared by the Mirai-derived families. Rule only fires on the encoded form.
