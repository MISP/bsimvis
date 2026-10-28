# Linux_Capability_C2IrcCommandsXor22

Capability: irc_commands (irc_privmsg,getlocalip,killattk). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

Gafgyt IRC-style command words (PRIVMSG, KILLATTK, GETLOCALIP, BOGOMIPS, dvrHelper) stored XOR 0x22.

## Evidence

### irc_gafgyt_commands_xor22 (18 holders)

XOR-0x22 strings decoding to PRIVMSG, KILLATTK, GETLOCALIP, BOGOMIPS, dvrHelper.

- [xor22] `'rpktoqe"'` in 16 files
- [xor22] `'iknncvvi"'` in 16 files
- [xor22] `'egvnmacnkr"'` in 18 files
- [xor22] `'`memokrq"'` in 16 files
- [xor22] `'FTPjGNRGP"'` in 16 files
- [xor22] `'AJWLIGF"'` in 18 files

## Spread (groups of every file the rule hits)

boatnet_xor22:17

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- FUN_00016df8[1073795588] hit=2/2 (ARM)
- FUN_0001890c[1073878047] hit=2/2 (ARM)
- do_dlvsym[1073889437] hit=1/38 (AARCH64)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`irc_gafgyt_commands_xor22`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 16, fn 2, fp 1, precision 0.9412, recall 0.8889.
- 3-of-5 xor(0x22) words. Uses the YARA xor modifier on the plain words; an encoded-byte form would be identical.

## Known gaps

2 holders (353851616f, m-p.s-l.dick) are missed: their words use another encoding or are stripped.
