# cap_telnet_login_prompt_xor22_table

Rule `Linux_Capability_TelnetLoginPromptTable` (kind capability, confidence high). MITRE ATT&CK T1027,T1059.004,T1110.001.

## What it detects
Mirai table_unlock_val strings stored XOR-encoded, key xor 0x22 (Mirai table_unlock_val key: 0xdeadbeef folded = 0x22). Decodes to login prompts and kill/persist paths. Several variants (Hakai/qtxbot) share the table.

## Evidence
- `xor22` "LAMPPGAV\"" (80 files) - -> 'ncorrect' (login failure prompt) key 0x22
- `xor22` "Q[QVGO\"" (80 files) - -> 'system'
- `xor22` "QJGNN\"" (80 files) - -> 'shell'
- `xor22` "GLC@NG\"" (80 files) - -> 'enable'
- `xor22` "\rRPMA\r\"" (51 files) - -> '/proc/'
- `xor22` "\rFGT\rUCVAJFME\"" (49 files) - -> '/dev/watchdog'
- `xor22` "\rFGT\rOKQA\rUCVAJFME\"" (49 files) - -> '/dev/misc/watchdog'
- `xor22` "\rRPMA\rLGV\rVAR\"" (47 files) - -> '/proc/net/tcp'

Catalogue holders: 80. Spread in the catalogue: {"MD5.unpacked": 42, "p": 29, "boatnet": 6, "m-p.s-l.dick": 1, "MD5MD5.elf": 1, "MD5": 1}. Spread of the rule: boatnet_xor22, p_killermaps, xnxn, josho_flood, demons (0x54 side: demons, chernobyl, josho).

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
