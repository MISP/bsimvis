rule Linux_Capability_DdosUdpFlood
{
    meta:
        description = "UDP flood capability: gafgyt '!udpplain/!udpcustom' command pair, Mirai-style '[udp_flood] started' worker logs, or the per-arch ARM prologues of attack_udp_plain/generic/bypass"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "udp_flood"
        capabilities = "udp_plain,udp_generic,udp_bypass,udp_hex"
        mitre_attack = "T1498.001"
        spread = "xnxn:20, boatnet_xor22:11, josho_flood:9, p_killermaps:5, nuclear:2, demons:2, frosted:2"
        implemented_in = "attack_udp_plain[1073888704] hit=22/24 (ARM); attack_udp_generic[1073886304] hit=18/18 (ARM); attack_udp_bypass[1073888039] hit=12/12 (ARM); attack_method_udphex[1073831543] hit=8/8 (ARM); FUN_00402360[1073881294] hit=5/5 (MIPS)"
        confidence = "high"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,024c9c6ef641449947ed6c3a65e34c79"
        sample_count = "58 holder files; 59 files hit"
        tested = "recall 58/58 on capability holders, 1 hits on non-holders (string-confirmed true holders in most cases; tp+fp=59 files) (2026-10-28)"
        arch_note = "strings for MIPS/x86/PPC/m68k/RISC-V/SH4; ARM prologue windows for attack_udp_plain/generic/bypass"
        note = "Hits every group that implements UDP floods, not one variant. The ARM prologues carry the stack-frame size of one build family, so they are support, not primary evidence. UPX originals carry no strings."
    strings:
        $c1 = "!udpplain"
        $c2 = "!udpcustom"
        $w1 = "[udp_plain_flood] started"
        $w2 = "[udp_flood] started"
        $w3 = "[udpbypass_flood] started"
        $w4 = "[udp_flood] socket() failed"
        $w5 = "[udp_plain_flood] socket() failed"
        $b1 = { f0 4f 2d e9 2c d0 4d e2 04 10 8d e5 04 10 a0 e3 }
        $b2 = { f0 4f 2d e9 01 80 a0 e1 54 d0 4d e2 04 10 a0 e3 }
        $b3 = { f0 4f 2d e9 24 d0 4d e2 04 10 8d e5 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and
        (all of ($c*) or 2 of ($w*) or any of ($b*))
}

rule Linux_Capability_DdosTcpFlood
{
    meta:
        description = "TCP flood capability: '[syn_flood]/[ack_flood]/[tcpbypass_flood] started' worker logs, DOMINATE banner, STOMP stub text, or the ARM attack_tcp_syn prologue"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "tcp_flood"
        capabilities = "syn_flood,ack_flood,psh_ack_flood,tcp_bypass,stomp"
        mitre_attack = "T1498.001,T1499.002"
        spread = "josho_flood:11, tsun_net_series:11, boatnet_xor22:10, p_killermaps:6, demons:4, nuclear:2, frosted:2, sysd_dropper:1"
        implemented_in = "attack_tcp_syn[1073887222] hit=26/26 (ARM); processCmd[1073837257] hit=5/5 (ARM,MIPS,PowerPC); FUN_00402724[1073881295] hit=5/5 (MIPS); processCmd[1073887214] hit=3/3 (x86,x86); psh_ack_flood_wuhan[1073886510] hit=2/2 (ARM)"
        confidence = "high"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,0479a683c44c1834c6cbaae1cf7a3e29"
        sample_count = "46 holder files; 47 files hit"
        tested = "recall 44/46 on capability holders, 3 hits on non-holders (string-confirmed true holders in most cases; tp+fp=47 files) (2026-10-28)"
        arch_note = "strings (all arches); ARM attack_tcp_syn prologue"
        note = "Three hits outside the holder list (sh4 josho, bin.x86_64 sysd_dropper, cock tsunami) carry the same strings and are true holders the string pivot missed."
    strings:
        $w1 = "[syn_flood] started"
        $w2 = "[ack_flood] started"
        $w3 = "[tcpbypass_flood] started"
        $w4 = "[psh_ack_flood] started"
        $w5 = "[syn_flood] socket() failed"
        $w6 = "[ack_flood] setsockopt() failed"
        $d1 = "DOMINATE Flooding %s for %d seconds."
        $s1 = "STOMP attack is not implemented"
        $b1 = { f0 4f 2d e9 01 80 a0 e1 74 d0 4d e2 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and
        (2 of ($w*) or $d1 or $s1 or $b1)
}

rule Linux_Capability_DdosGreIcmpFlood
{
    meta:
        description = "GRE and ICMP flood: '[icmp_flood]' worker logs, or ARM prologues of attack_gre_ip / attack_gre_eth / attack_method_icmpflood"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "gre_flood,icmp_flood"
        capabilities = "gre_ip,gre_eth,icmp_flood"
        mitre_attack = "T1498.001"
        spread = "josho_flood:11, boatnet_xor22:8, p_killermaps:7, demons:4, nuclear:2, frosted:2"
        implemented_in = "attack_gre_ip[1073856790] hit=20/20 (ARM); attack_gre_eth[1073886374] hit=18/18 (ARM); FUN_0001784c[1073886182] hit=2/2 (ARM); FUN_00014f74[1073887469] hit=2/2 (ARM); attack_method_netbios_datagram[1073842619] hit=2/2 (ARM)"
        confidence = "medium"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,0479a683c44c1834c6cbaae1cf7a3e29"
        sample_count = "25 holder files; 34 files hit"
        tested = "recall 25/25 on capability holders, 9 hits on non-holders (string-confirmed true holders in most cases; tp+fp=34 files) (2026-10-28)"
        arch_note = "strings for icmp; ARM prologues for gre (code-only)"
        note = "GRE handlers carry no strings: ARM only. Non-ARM GRE builds are not covered."
    strings:
        $i1 = "[icmp_flood] started"
        $i2 = "[icmp_flood] socket() failed"
        $i3 = "[icmp_flood] setsockopt() failed"
        $g1 = { f0 4f 2d e9 01 90 a0 e1 64 d0 4d e2 04 10 a0 e3 }
        $g2 = { f0 4f 2d e9 01 90 a0 e1 74 d0 4d e2 04 10 a0 e3 }
        $m1 = { f0 4f 2d e9 3c d0 4d e2 08 10 8d e5 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and (any of ($i*) or any of ($g*) or $m1)
}

rule Linux_Capability_DdosSourceEngineQuery
{
    meta:
        description = "Valve Source Engine query flood: plain 'TSource Engine Query' with its escaped /x54/x53.. twin (gafgyt), the Mirai table entry XOR 0x22, or the ARM attack_udp_vse prologue"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "vse_flood"
        capabilities = "source_engine_query_flood,udp_flood"
        mitre_attack = "T1498.001"
        spread = "boatnet_xor22:28, p_killermaps:18, tsun_net_series:12, tsun_holocaust:2, josho_flood:1, demons:1"
        implemented_in = "vseattack[1073796272] hit=3/3 (MIPS,MIPS); vseattack[1073837228] hit=2/2 (ARM); attack_udp_vse[1073856796] hit=1/20 (ARM)"
        confidence = "high"
        reference_md5 = "168036e68ea46fd5dd2be5f70e248d9d,282898fac2e6a88c4108e9751cfc8ce4"
        sample_count = "12 holder files; 138 files hit"
        tested = "recall 12/12 on capability holders, 126 hits on non-holders (string-confirmed true holders in most cases; tp+fp=138 files) (2026-10-28)"
        arch_note = "strings + xor22 bytes (arch-independent); ARM prologue"
        note = "The Mirai XOR-0x22 table entry is the bulk of hits (about 105 non-holder files); these are real VSE implementers, not false positives. Holder list in capabilities.json counts only the gafgyt text variant."
    strings:
        $a = "TSource Engine Query"
        $h = "/x54/x53/x6f/x75/x72/x63/x65"
        $x22 = { 76 71 4d 57 50 41 47 02 67 4c 45 4b 4c 47 02 73 57 47 50 5b }
        $b1 = { f0 4f 2d e9 01 a0 a0 e1 34 d0 4d e2 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and (($a and $h) or $x22 or $b1)
}

rule Linux_Capability_DdosHttpCdnCgiBypass
{
    meta:
        description = "HTTP flood with Cloudflare captcha-path bypass: '/cdn-cgi/l/chk_captcha' request template, or ARM httpattack/SendHTTP prologues"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "http_flood"
        capabilities = "http_flood,cloudflare_bypass"
        mitre_attack = "T1499.002"
        spread = "tsun_net_series:11, tsun_holocaust:10, boatnet_xor22:6, nuclear:2"
        implemented_in = "FUN_00008d3c[1073887716] hit=8/8 (ARM); SendHTTPCloudflare[1073886354] hit=4/4 (ARM,MIPS,MIPS); httpattack[1073885976] hit=4/4 (ARM,MIPS,MIPS); sendHTTPtwo[1073889201] hit=3/3 (MIPS,MIPS,PowerPC); sendHTTPtwo[1073889010] hit=2/2 (x86)"
        confidence = "high"
        reference_md5 = "0bf5f494aaed28575c8dddc5439fabb7,0c35812d27bc574f232ecae092ee54c1"
        sample_count = "28 holder files; 29 files hit"
        tested = "recall 26/28 on capability holders, 3 hits on non-holders (string-confirmed true holders in most cases; tp+fp=29 files) (2026-10-28)"
        arch_note = "strings; ARM prologues for FUN_00008d3c and sendHTTPtwo"
        note = "The 3 hits outside the holder list also contain the cdn-cgi template."
    strings:
        $a = "/cdn-cgi/l/chk_captcha"
        $v = "Connection: close\r\n\r\n"
        $b1 = { f0 4f 2d e9 51 dc 4d e2 74 d0 4d e2 02 40 a0 e1 03 50 a0 e1 2c 00 8d e5 }
        $b2 = { 0d c0 a0 e1 10 dc 2d e9 04 b0 4c e2 4a dd 4d e2 18 d0 4d e2 b4 a5 9f e5 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and (($a and $v) or any of ($b*))
}

rule Linux_Capability_DdosDnsFlood
{
    meta:
        description = "DNS query flood handler (attack_udp_dns / attack_method_udpdns): ARM prologues only, the handler has no strings"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "dns_flood"
        capabilities = "dns_flood"
        mitre_attack = "T1498.001"
        spread = "boatnet_xor22:6, demons:4, josho_flood:2"
        implemented_in = "FUN_0000ed4c[1073886487] hit=10/10 (ARM); FUN_0000e28c[1073831554] hit=2/2 (ARM)"
        confidence = "medium"
        reference_md5 = "0bf5f494aaed28575c8dddc5439fabb7,0c35812d27bc574f232ecae092ee54c1"
        sample_count = "12 holder files; 12 files hit"
        tested = "recall 12/12 on capability holders, 0 hits on non-holders (string-confirmed true holders in most cases; tp+fp=12 files) (2026-10-28)"
        arch_note = "ARM only (byte windows)"
        note = "Code-only capability. Prologues encode two build families' stack sizes."
    strings:
        $b1 = { f0 4f 2d e9 89 de 4d e2 04 d0 4d e2 18 10 8d e5 }
        $b2 = { f0 4f 2d e9 6c d0 4d e2 18 10 8d e5 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and any of them
}

rule Linux_Capability_DdosGameProtocol
{
    meta:
        description = "Game and service-specific floods: Go bot Methods.Start* table, tiny MIPS method table (UDP-VSE/TCP-FIVEM/MINECRAFT), or ARM attack_udp_vse/custom/discord prologues"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "game_protocol_flood"
        capabilities = "discord_flood,vse_flood,custom_udp,minecraft_flood,fivem_flood"
        mitre_attack = "T1498.001"
        spread = "boatnet_xor22:12, go_bot_a:8, p_killermaps:6, demons:4, frosted:2, udp_vse_tiny:2, josho_flood:2"
        implemented_in = "attack_udp_vse[1073856796] hit=20/20 (ARM); attack_udp_custom[1073887637] hit=10/10 (ARM); attack_udp_discord[1073831547] hit=4/4 (ARM); FUN_0001564c[1073887999] hit=2/2 (ARM)"
        confidence = "medium"
        reference_md5 = "07aef2ea126c64fd555d58a906867e7b,0bf5f494aaed28575c8dddc5439fabb7"
        sample_count = "36 holder files; 36 files hit"
        tested = "recall 36/36 on capability holders, 0 hits on non-holders (string-confirmed true holders in most cases; tp+fp=36 files) (2026-10-28)"
        arch_note = "strings (Go, MIPS); ARM prologues"
        note = "Overlaps UdpFlood and SourceEngineQuery on purpose: those ARM handlers are the game-protocol floods."
    strings:
        $g1 = "Methods.StartPPS"
        $g2 = "Methods.StartDiscord"
        $g3 = "Methods.StartUDP"
        $g4 = "Methods.StartTCP"
        $m1 = "UDP-CUSTOM"
        $m2 = "UDP-VSE"
        $m3 = "TCP-FIVEM"
        $m4 = "TCP-HANDSHAKE"
        $m5 = "MINECRAFT"
        $b1 = { f0 4f 2d e9 01 b0 a0 e1 34 d0 4d e2 04 10 a0 e3 }
        $b2 = { f0 4f 2d e9 3c d0 4d e2 04 10 8d e5 04 10 a0 e3 02 50 a0 e1 03 60 a0 e1 }
        $b3 = { f0 4f 2d e9 01 a0 a0 e1 34 d0 4d e2 04 10 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 8MB and
        (2 of ($g*) or 4 of ($m*) or any of ($b*))
}

rule Linux_Capability_DdosSsdpMsearch
{
    meta:
        description = "SSDP M-SEARCH reflection payload (HOST 255.255.255.255:1900, ST urn:dial-multiscreen-org:service:dial:1)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "ssdp_reflection"
        capabilities = "ssdp_msearch_amplification"
        mitre_attack = "T1498.002"
        spread = "boatnet_xor22:56, nuclear:13, josho_flood:12, rival_killer:7, unclassified:1"
        implemented_in = "attack_method_nudp[1073831300] hit=8/10 (ARM); FUN_0000e898[1073878033] hit=8/10 (ARM); FUN_0804e590[1073887464] hit=4/5 (x86); FUN_10005d04[1073883027] hit=4/5 (PowerPC); FUN_80005402[1073878195] hit=4/5 (68000)"
        confidence = "high"
        reference_md5 = "00de94cb911eccf096e190ef7fe9cb08,0fac505e1c34ad2c2f108c8fa586d8a3"
        sample_count = "45 holder files; 89 files hit"
        tested = "recall 45/45 on capability holders, 44 hits on non-holders (string-confirmed true holders in most cases; tp+fp=89 files) (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Hits 44 files outside the holder list: they hold the identical string (Ghidra merged it into longer data), so tp+fp is the honest count (89)."
    strings:
        $a = "M-SEARCH * HTTP/1.1\r\nHOST: 255.255.255.255:1900\r\nMAN: \"ssdp:discover\""
        $b = "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\""
        $c = "ST: urn:dial-multiscreen-org:service:dial:1"
        $d = "ST: ssdp:all"
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and ($a or $b) and ($c or $d)
}

rule Linux_Capability_C2GoogleDnsResolver
{
    meta:
        description = "Hard-coded 8.8.8.8 resolver / local-IP lookup, plus ARM resolve_cnc_addr prologues"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "dns_resolve_google"
        capabilities = "resolve_cnc_addr,get_local_ip"
        mitre_attack = "T1071.004"
        spread = "tsun_net_series:12, boatnet_xor22:11, tsun_holocaust:10, zbot_static:10, cling_exploit_scanner:9, demons:4, unclassified:3, stun_probe:2, frosted:2, josho_flood:2"
        implemented_in = "resolve_cnc_addr[1073856089] hit=12/12 (ARM); getOurIP[1073888658] hit=6/6 (ARM,PowerPC,SuperH4); getOurIP[1073886449] hit=5/5 (ARM,MIPS,MIPS); FUN_0001f190[1073887793] hit=2/4 (ARM); FUN_000119c0[1073888869] hit=2/2 (x86)"
        confidence = "low"
        reference_md5 = "0c35812d27bc574f232ecae092ee54c1,117a13cd039de86a48c7ee1ee7899719"
        sample_count = "51 holder files; 66 files hit"
        tested = "recall 47/51 on capability holders, 19 hits on non-holders (string-confirmed true holders in most cases; tp+fp=66 files) (2026-10-28)"
        arch_note = "string '8.8.8.8' (all arches); ARM prologues"
        note = "Weakest rule of the set: 8.8.8.8 is a bare IP string. Hitmap shows most strings sit in getOurIP (connect to 8.8.8.8:53 to learn the local address), which is an IP-discovery step, and resolve_cnc_addr on ARM. 16 non-holder hits come from the string only; treat as medium-quality evidence of the pattern, not of a family."
    strings:
        $s = "8.8.8.8" fullword
        $a1 = { 00 eb 28 40 9f e5 00 10 a0 e3 04 00 84 e5 }
        $a2 = { 70 40 2d e9 f8 11 9f e5 28 d0 4d e2 00 60 a0 e1 f0 51 9f e5 11 00 a0 e3 }
        $a3 = { f0 45 2d e9 43 df 4d e2 01 0c 8d e2 02 00 80 e2 d4 12 9f e5 06 20 a0 e3 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and ($s or any of ($a*))
}

rule Linux_Capability_C2TextPingPong
{
    meta:
        description = "C2 text-command handler: 'pong %s' reply together with a command-parser anchor ('!udpplain', 'srcport=', '/proc/%s/comm')"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "c2_ping_pong"
        capabilities = "c2_text_commands"
        mitre_attack = "T1071"
        spread = "xnxn:22, flylegit:16, iran_dg:2, iran_procscan:2, static_bot_started:1"
        implemented_in = "FUN_ram_00012f94[1073886164] hit=4/4 (RISCV,RISCV); FUN_00403934[1073841768] hit=4/4 (SuperH4); FUN_0804c854[1073851589] hit=2/2 (x86); FUN_0804a7f8[1073851576] hit=2/2 (x86); FUN_004058c0[1073781348] hit=2/2 (AARCH64)"
        confidence = "medium"
        reference_md5 = "024c9c6ef641449947ed6c3a65e34c79,071cda1ed6338195c101e74f67b0773c"
        sample_count = "32 holder files; 43 files hit"
        tested = "recall 32/32 on capability holders, 11 hits on non-holders (string-confirmed true holders in most cases; tp+fp=43 files) (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "'pong %s' sits in the same function as '!udpplain' (processCmd). 11 hits outside the holder list also carry 'pong %s' and '/proc/%s/comm' (iran, boatnet): the same handler family."
    strings:
        $a = "pong %s"
        $b1 = "!udpplain"
        $b2 = "!udpcustom"
        $b3 = "srcport="
        $b4 = "payload="
        $b5 = "/proc/%s/comm"
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $a and any of ($b*)
}

rule Linux_Capability_C2IrcCommandsXor22
{
    meta:
        description = "Gafgyt IRC-style command words (PRIVMSG, KILLATTK, GETLOCALIP, BOGOMIPS, dvrHelper) stored XOR 0x22"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "irc_commands"
        capabilities = "irc_privmsg,getlocalip,killattk"
        mitre_attack = "T1071,T1027"
        spread = "boatnet_xor22:17"
        implemented_in = "FUN_00016df8[1073795588] hit=2/2 (ARM); FUN_0001890c[1073878047] hit=2/2 (ARM); do_dlvsym[1073889437] hit=1/38 (AARCH64)"
        confidence = "high"
        reference_md5 = "2abe9d933d6821e118fadbaadddfa44c,336057e0016e4cd35ac43ee31062f196"
        sample_count = "18 holder files; 17 files hit"
        tested = "recall 16/18 on capability holders, 1 hits on non-holders (string-confirmed true holders in most cases; tp+fp=17 files) (2026-10-28)"
        arch_note = "arch-independent (xor-aware strings)"
        note = "3-of-5 xor(0x22) words. Uses the YARA xor modifier on the plain words; an encoded-byte form would be identical."
    strings:
        $a = "PRIVMSG" xor(0x22)
        $b = "KILLATTK" xor(0x22)
        $c = "GETLOCALIP" xor(0x22)
        $d = "BOGOMIPS" xor(0x22)
        $e = "dvrHelper" xor(0x22)
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 3 of them
}

rule Linux_Capability_C2SelfRepBanner
{
    meta:
        description = "'Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG FoReHeAd We BiG L33T HaxErS' banner printed by self-replicating netis-backdoor spreaders"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "self_rep_banner"
        capabilities = "netis_backdoor_spread"
        mitre_attack = "T1210"
        spread = "boatnet_xor22:41, tsun_net_series:11, tsun_holocaust:10, unclassified:1"
        implemented_in = "attack_method_ovhdrop[1073888073] hit=6/6 (ARM); FUN_0000e9d4[1073887985] hit=6/6 (ARM); FUN_0804ca20[1073885226] hit=3/3 (x86); FUN_00015000[1073805457] hit=3/3 (sparc); FUN_1000517c[1073883025] hit=3/3 (PowerPC)"
        confidence = "medium"
        reference_md5 = "00de94cb911eccf096e190ef7fe9cb08,26ead3fb6e79060c84225282f3955ed4"
        sample_count = "32 holder files; 63 files hit"
        tested = "recall 32/32 on capability holders, 31 hits on non-holders (string-confirmed true holders in most cases; tp+fp=63 files) (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Single anchor (lint warns). It is a family marker in rules/A.yar (Boatnet); here it is the capability signal and is intentionally the only evidence. 31 hits outside the holder list carry the identical string."
    strings:
        $a = "Self Rep Fucking NeTiS and Thisity 0n Ur FuCkInG FoReHeAd We BiG L33T HaxErS"
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $a
}

rule Linux_Capability_XorTable22Telnet
{
    meta:
        description = "Mirai string table obfuscated with 0x22 (deadbeef fold): encoded login prompts 'ncorrect', 'system', 'shell', 'enable' (3 of 6 entries)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "xor22_string_table"
        capabilities = "telnet_login_prompt,xor_table_decode"
        mitre_attack = "T1027,T1110.001"
        spread = "boatnet_xor22:30, p_killermaps:29"
        implemented_in = "table_init[1073887903] hit=27/33 (ARM,MIPS,MIPS); table_init[1073844850] hit=5/6 (ARM); FUN_0000f76c[1073882205] hit=4/5 (ARM); FUN_00409648[1073881015] hit=3/4 (MIPS); FUN_0804e400[1073842423] hit=3/3 (x86)"
        confidence = "high"
        reference_md5 = "00de94cb911eccf096e190ef7fe9cb08,07aef2ea126c64fd555d58a906867e7b"
        sample_count = "80 holder files; 89 files hit"
        tested = "recall 80/80 on capability holders, 9 hits on non-holders (string-confirmed true holders in most cases; tp+fp=89 files) (2026-10-28)"
        arch_note = "arch-independent (encoded bytes)"
        note = "Eight hits outside the holder list are real tables the Ghidra string index merged away. Family-wide marker for 'table_unlock_val with 0x22'."
    strings:
        $n1 = "LAMPPGAV\""
        $n2 = "Q[QVGO\""
        $n3 = "QJGNN\""
        $n4 = "GLC@NG\""
        $n6 = "CQQUMPF\""
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 3 of ($n*)
}

rule Linux_Capability_XorTable54Telnet
{
    meta:
        description = "Mirai string table obfuscated with key 0x54, terminator 'T': encoded 'ncorrect', 'enable', 'shell', 'system', '.anime', busybox kill line (3 of 8)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "mirai,gafgyt,tsunami"
        capability = "xor54_string_table"
        capabilities = "telnet_login_prompt,xor_table_decode"
        mitre_attack = "T1027,T1110.001"
        spread = "demons:18, josho_flood:12, frosted:2, unclassified:1"
        implemented_in = "table_init[1073888733] hit=12/14 (ARM,MIPS,MIPS); FUN_00012154[1073790654] hit=3/3 (ARM); FUN_000192f8[1073831581] hit=2/2 (ARM)"
        confidence = "high"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,1d6e40f1e0f2624e4e31620d9b8123f5"
        sample_count = "22 holder files; 33 files hit"
        tested = "recall 22/22 on capability holders, 11 hits on non-holders (string-confirmed true holders in most cases; tp+fp=33 files) (2026-10-28)"
        arch_note = "arch-independent (encoded bytes)"
        note = "11 hits outside the holder list are real tables."
    strings:
        $n1 = ":7;&&17 T"
        $n2 = "1:5681T"
        $n3 = "'<188T"
        $n4 = "'-' 19T"
        $n5 = "z5:=91T"
        $n6 = "{6=:{6!'-6;,t?=88tymtT"
        $n7 = ":591'1&\"1&tT"
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 3 of ($n*)
}
