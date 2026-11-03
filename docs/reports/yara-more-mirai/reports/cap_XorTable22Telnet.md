# Linux_Capability_XorTable22Telnet

Capability: xor22_string_table (telnet_login_prompt,xor_table_decode). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Mirai string table obfuscated with 0x22 (deadbeef fold): encoded login prompts 'ncorrect', 'system', 'shell', 'enable' (3 of 6 entries).

## Evidence

### telnet_login_prompt_xor22_table (80 holders)

Mirai table_unlock_val strings stored XOR-encoded, key xor 0x22 (Mirai table_unlock_val key: 0xdeadbeef folded = 0x22). Decodes to login prompts and kill/persist paths. Several variants (Hakai/qtxbot) share the table.

- [xor22] `'LAMPPGAV"'` in 80 files
- [xor22] `'Q[QVGO"'` in 80 files
- [xor22] `'QJGNN"'` in 80 files
- [xor22] `'GLC@NG"'` in 80 files
- [xor22] `'\rRPMA\r"'` in 51 files
- [xor22] `'\rFGT\rUCVAJFME"'` in 49 files

## Spread (groups of every file the rule hits)

boatnet_xor22:30, p_killermaps:29

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- table_init[1073887903] hit=27/33 (ARM,MIPS,MIPS)
- table_init[1073844850] hit=5/6 (ARM)
- FUN_0000f76c[1073882205] hit=4/5 (ARM)
- FUN_00409648[1073881015] hit=3/4 (MIPS)
- FUN_0804e400[1073842423] hit=3/3 (x86)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`telnet_login_prompt_xor22_table`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 80, fn 0, fp 9, precision 0.8989, recall 1.0.
- Eight hits outside the holder list are real tables the Ghidra string index merged away. Family-wide marker for 'table_unlock_val with 0x22'.

## Known gaps

Tables with a different key (0x54 -> separate rule) or padded entries.
