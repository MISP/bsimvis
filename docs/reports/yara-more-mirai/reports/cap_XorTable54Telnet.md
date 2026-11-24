# Linux_Capability_XorTable54Telnet

Capability: xor54_string_table (telnet_login_prompt,xor_table_decode). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Mirai string table obfuscated with key 0x54, terminator 'T': encoded 'ncorrect', 'enable', 'shell', 'system', '.anime', busybox kill line (3 of 8).

## Evidence

### telnet_login_prompt_xor54_table (22 holders)

Same table idea, key xor 0x54: decodes to '.anime', '/bin/busybox kill -9 ', 'nameserver ', prompts and /proc paths.

- [xor54] `':7;&&17 T'` in 22 files
- [xor54] `'1:5681T'` in 22 files
- [xor54] `"'<188T"` in 22 files
- [xor54] `"'-' 19T"` in 22 files
- [xor54] `'z5:=91T'` in 22 files
- [xor54] `"{6=:{6!'-6;,t?=88tymtT"` in 22 files

## Spread (groups of every file the rule hits)

demons:18, josho_flood:12, frosted:2, unclassified:1

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- table_init[1073888733] hit=12/14 (ARM,MIPS,MIPS)
- FUN_00012154[1073790654] hit=3/3 (ARM)
- FUN_000192f8[1073831581] hit=2/2 (ARM)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`telnet_login_prompt_xor54_table`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 22, fn 0, fp 11, precision 0.6667, recall 1.0.
- 11 hits outside the holder list are real tables.

## Known gaps

None known.
