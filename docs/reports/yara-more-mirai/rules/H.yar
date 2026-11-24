rule Linux_Capability_TelnetDefaultCreds
{
    meta:
        description = "Plaintext Mirai-style telnet default-credential dictionary (xc3511, 7ujMko0vizxv, telnetadmin, telecomadmin, Zte521 ...), 5 of 11 entries"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "telnet_default_credential_dictionary"
        mitre_attack = "T1078.001,T1110.001"
        spread = "demons, zbot_static, a few boatnet unpacked builds (the catalogue lists 14 DEMONS-named holders; the rule also reaches the zbot_static statics)"
        implemented_in = "scanner_init clusters 1073856098 (ARM), FUN_004083cc 1073888319 and FUN_00410c9c 1073845860 (MIPS), FUN_0000e3cc/FUN_0000f9dc (ARM): the credential table is referenced from the scanner init"
        confidence = "high"
        reference_md5 = "1de0d0093f48ce33ae88ded5530830c6,20be272edd356638e4df9fcf77f9d33a"
        sample_count = "14 catalogue holder files"
        tested = "recall 14/14 catalogue holders, 17 further hits (zbot_static 10, demons 6, boatnet_xor22 1), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Hits in zbot_static and DEMONS files absent from the catalogue pivot are true holders (Ghidra merges adjacent table strings). XOR-encoded credential tables are not covered."
    strings:
        $c1 = "xc3511"
        $c2 = "7ujMko0vizxv"
        $c3 = "telnetadmin"
        $c4 = "telecomadmin"
        $c5 = "smcadmin"
        $c6 = "Zte521"
        $c7 = "dvr2580222"
        $c8 = "juantech"
        $c9 = "zhongxing"
        $c10 = "Alphanetworks"
        $c11 = "microbusiness"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 5 of ($c*)
}

rule Linux_Capability_TelnetLoginPromptTable
{
    meta:
        description = "Mirai telnet login-prompt table (ncorrect, system, enable, assword) stored XOR-encoded with key 0x22 or 0x54: the scanner's login state machine"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "telnet_login_prompt_xor22_table,telnet_login_prompt_xor54_table"
        mitre_attack = "T1027,T1059.004,T1110.001"
        spread = "boatnet_xor22, p_killermaps, xnxn, josho_flood, demons (0x54 side: demons, chernobyl, josho)"
        implemented_in = "table_init clusters 1073887903 (key 0x22; ARM, MIPS, sparc, x86) and 1073888733 (key 0x54; ARM, MIPS, SuperH4, x86); scanner reads the entries via table_retrieve_val 1073880296"
        confidence = "high"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,00de94cb911eccf096e190ef7fe9cb08"
        sample_count = "102 catalogue holder files"
        tested = "recall 102/102 catalogue holders, 20 further hits (josho_flood 9, boatnet_xor22 6, p_killermaps 3, demons 2), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (XOR-encoded constants; matches bytes with xor(0x22) / xor(0x54))"
        note = "Not specific to one variant: it is the table scheme shared by the Mirai-derived families. Rule only fires on the encoded form."
    strings:
        $a1 = "ncorrect" xor(0x22)
        $a2 = "system" xor(0x22)
        $a3 = "enable" xor(0x22)
        $a4 = "assword" xor(0x22)
        $b1 = "ncorrect" xor(0x54)
        $b2 = "/dev/watchdog" xor(0x54)
        $b3 = "nameserver " xor(0x54)
        $b4 = "/bin/busybox kill -9 " xor(0x54)

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and (3 of ($a*) or 3 of ($b*))
}

rule Linux_Capability_TelnetBruteScanner
{
    meta:
        description = "Telnet brute-force scanner: the 'telnet-brute' result record format of the od_ kit, or two per-arch scanner_thread/scanner_init byte windows"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "telnet_scanner_bruteforce"
        mitre_attack = "T1021,T1110.001,T1595.001"
        spread = "od_ kit (10 MD5MD5 files); byte windows add ARM/MIPS/AARCH64 builds with scanner_* symbols"
        implemented_in = "lateral_thread 1073853774 (ARM), 1073766843 (MIPS), 1073837947 (AARCH64) for the strings; scanner_thread 1073853775 (ARM), 1073766844 (MIPS), 1073837948 (AARCH64) and scanner_init 1073856098 (ARM) for the byte windows"
        confidence = "medium"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,0fac505e1c34ad2c2f108c8fa586d8a3"
        sample_count = "21 catalogue holder files"
        tested = "recall 10/21 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "string path arch-independent; byte windows for ARM, MIPS BE/LE and AARCH64"
        note = "Byte windows are build-specific (ARM literal-pool offsets): they hit identical builds only. Scanners of DEMONS.arm7/nuclear/boatnet-unpacked ARM builds are NOT covered; use Linux_Capability_TelnetLoginPromptTable and Linux_Capability_TelnetDefaultCreds for those."
    strings:
        $t1 = "%s|%s|telnet-brute"
        $t2 = "telnet-brute"
        $t3 = "/usr/sbin/telnetd"
        $t4 = "%s|adb|shell|shell"
        $b1 = { f0 4f 2d e9 84 a4 9f e5 df df 4d e2 0a a0 8f e0 00 30 9a e5 00 00 53 e3 74 00 00 0a 70 b4 9f e5 }
        $b2 = { f0 4f 2d e9 73 de 4d e2 0c d0 4d e2 e4 18 00 eb 01 00 70 e3 00 30 a0 13 01 30 a0 03 }
        $b3 = { 04 e0 2d e5 18 30 9f e5 04 d0 4d e2 00 00 93 e5 09 10 a0 e3 ?? ?? 00 eb 04 d0 8d e2 04 e0 9d e4 }
        $b4 = { 3c 1c 00 10 27 9c c2 88 03 99 e0 21 27 bd fc 38 af b6 03 b8 8f 96 80 a0 af bc 00 18 }
        $b5 = { 10 00 1c 3c b8 cd 9c 27 21 e0 99 03 38 fc bd 27 b8 03 b6 af a0 80 96 8f 18 00 bc af }
        $b6 = { ff 43 0f d1 fd 7b 00 a9 fd 03 00 91 f5 5b 02 a9 b6 07 00 b0 c0 42 40 b9 c0 16 00 34 }

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and (($t1 and ($t2 or $t3 or $t4)) or 2 of ($b*))
}

rule Linux_Capability_KillerProcScan0cl
{
    meta:
        description = "Rival-process killer that scans /proc/*/{maps,exe,stat} and logs '[0clKiller*] Killed Process': any 2 of 3 log formats"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "competitor_killer_0cl"
        mitre_attack = "T1057,T1489,T1562"
        spread = "p_killermaps (31), unpacked boatnet-lineage builds (16), 1 stripped MD5 file"
        implemented_in = "killer_maps 1073844836, killer_exe 1073844834, killer_stat 1073844835 (ARM); FUN_00407844/FUN_004072e4 (MIPS); FUN_0804cac0 (x86); m68k FUN_800000f6"
        confidence = "high"
        reference_md5 = "07aef2ea126c64fd555d58a906867e7b,0987f84e807a81c742e344c7cec0cb02"
        sample_count = "48 catalogue holder files"
        tested = "recall 48/48 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Strongest cross-arch capability anchor in the collection. UPX originals carry no strings."
    strings:
        $k1 = "[0clKillerMaps] Killed Process: %s, PID: %d"
        $k2 = "[0clKillerKillerEXE] Killed process: %s, PID: %d"
        $k3 = "[0clKillerStat] Killed Process: %s, PID: %d"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 2 of ($k*)
}

rule Linux_Capability_KillerByNameOrPort
{
    meta:
        description = "Killer that matches rivals by name, port or exe path: '[killer] killed mirai', pkill -9 -x/-f command lines, auditctl/snoop kill, or ARM windows of killer_exe/maps/stat and killer_kill_by_port"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "competitor_killer_by_name_port"
        mitre_attack = "T1489,T1562.001"
        spread = "od_ kit (10), josho_flood (2), kushnet-style rival kill, plus ARM p_killermaps/nuclear/DEMONS via byte windows"
        implemented_in = "evade_bg 1073853772 (ARM), 1073887030 (MIPS), 1073837945 (AARCH64) for pkill/auditctl; killer_mirai_exists 1073882187, killer_exe/maps/stat 1073844834-6, killer_kill_by_port 1073856086 (ARM) for byte windows"
        confidence = "medium"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,07aef2ea126c64fd555d58a906867e7b"
        sample_count = "33 catalogue holder files"
        tested = "recall 18/33 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "strings arch-independent; byte windows ARM only"
        note = "Fragmented capability: 15/33 catalogue holders are ARM builds whose windows differ and are not matched. No false hit measured."
    strings:
        $n1 = "[killer] killed mirai (pid: %d)"
        $n2 = "[killer] killed: %s (pid: %d) exe: %s"
        $n3 = "[killer] starting memory scan on (pid=%d)"
        $n4 = "pkill -9 -x %s 2>/dev/null &"
        $n5 = "pkill -9 -f [%c]%s 2>/dev/null &"
        $n6 = "auditctl -e 0 2>/dev/null &pkill -9 [a]uditd"
        $n7 = "[watch_time] kill() pid: ('%d'), exit()"
        $n8 = "killall -9 telnetd utelnetd scfgmgr"
        $n9 = "killed rival pid=%d exe=%s"
        $b1 = { 00 eb 05 00 50 e1 ee ff ff 0a 01 00 55 e3 00 00 55 13 00 80 a0 13 01 80 a0 03 e9 ff ff 0a }
        $b2 = { 00 eb 05 00 50 e1 ee ff ff 0a 00 30 94 e5 00 00 55 e3 03 00 55 11 ea ff ff 0a 01 00 55 e3 }
        $b3 = { 00 eb 05 00 50 e1 e7 ff ff 0a 00 30 94 e5 00 00 55 e3 03 00 55 11 00 70 a0 13 01 70 a0 03 }
        $b4 = { f0 4f 2d e9 22 dc 4d e2 2c d0 4d e2 02 6a 8d e2 08 60 86 e2 02 3a 8d e2 00 50 a0 e3 }
        $b5 = { 07 00 eb 00 00 50 e3 01 70 87 e2 f2 ff ff 1a 00 00 a0 e3 42 dd 8d e2 f0 41 bd e8 1e ff 2f e1 }

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and (2 of ($n*) or 2 of ($b*))
}

rule Linux_Capability_WatchdogHandle
{
    meta:
        description = "Opens the hardware watchdog (/dev/watchdog together with /dev/misc/watchdog in clear, or /dev/watchdog XOR-encoded with 0x22 or 0x54) so the device is kept alive"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "hardware_watchdog_keepalive"
        mitre_attack = "T1562"
        spread = "63 catalogue holders plus ~100 more across boatnet, josho, demons, p_killermaps (all Mirai-descended)"
        implemented_in = "table_init 1073887903 holds the encoded entry; evade_bg 1073853772 (ARM) / 1073887030 (MIPS) / 1073837945 (AARCH64) and main/FUN_0000e510/FUN_0804d120 hold the plain open() paths"
        confidence = "low"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,07aef2ea126c64fd555d58a906867e7b"
        sample_count = "63 catalogue holder files"
        tested = "recall 63/63 catalogue holders, 101 further hits (boatnet_xor22 29, josho_flood 9, demons 8, p_killermaps 3, mtab_manji 1), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Weak alone by design: nearly every Mirai descendant opens the watchdog, so the hits outside the 63 catalogue holders are real holders, not noise. Use as one capability of a profile, never to classify."
    strings:
        $w1 = "/dev/watchdog"
        $w2 = "/dev/misc/watchdog"
        $w3 = "/dev/watchdog" xor(0x22)
        $w4 = "/dev/watchdog" xor(0x54)
        $w5 = "/dev/misc/watchdog" xor(0x22)

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and (($w1 and $w2) or $w3 or $w4 or $w5)
}

rule Linux_Capability_SingleInstanceLock
{
    meta:
        description = "Single-instance lock file /tmp/.bot_lock (ensure_single_instance)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "gafgyt"
        capability = "single_instance_lock"
        mitre_attack = "T1480"
        spread = "xnxn (all arches incl. riscv/loongarch/m68k/sh4), one static_bot_started build"
        implemented_in = "ensure_single_instance 1073888696; per-arch xrefs in FUN_ram_120000140 (Loongarch), FUN_08049049 (x86), FUN_00400140 (MIPS/AARCH64), FUN_ram_00010158 (RISCV)"
        confidence = "medium"
        reference_md5 = "024c9c6ef641449947ed6c3a65e34c79,071cda1ed6338195c101e74f67b0773c"
        sample_count = "18 catalogue holder files"
        tested = "recall 18/18 catalogue holders, 5 further hits (xnxn 4, static_bot_started 1), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (string)"
        note = "Single anchor; the 4 xnxn hits with sh2/sh4 suffixes and static_bot_started are carriers of the same lock (merged strings), not unrelated families. Keep as a profile signal."
    strings:
        $l1 = "/tmp/.bot_lock"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and $l1
}

rule Linux_Capability_PersistKsync
{
    meta:
        description = "Boot persistence via /etc/init.d/S99ksync, /etc/cron.d/ksync, /data/local/.ksync.sh and update-rc.d: any 2 of 4"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "other"
        capability = "persistence_ksync_initd_cron"
        mitre_attack = "T1037,T1053.003,T1543"
        spread = "MD5MD5-named ksync family (6) and the kushnet rival_killer sample (shares the boot-hook set)"
        implemented_in = "string-only: no function cluster resolves (the paths live in unnamed init_persistence functions)"
        confidence = "high"
        reference_md5 = "169c706be8c5c49024c29704ec0e8ef5,4dcd0fffcf3ef5be03ca71f56954c514"
        sample_count = "6 catalogue holder files"
        tested = "recall 6/6 catalogue holders, 1 further hits (rival_killer 1), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "The one hit in rival_killer is expected: kushnet reuses the ksync boot hooks."
    strings:
        $p1 = "/etc/init.d/S99ksync"
        $p2 = "/etc/cron.d/ksync"
        $p3 = "/data/local/.ksync.sh"
        $p4 = "update-rc.d S99ksync defaults"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 2 of ($p*)
}

rule Linux_Capability_PersistCrontabTmpZ
{
    meta:
        description = "Crontab persistence '*/5 * * * * { [ -x /tmp/.z ] || cp /var/.z /tmp/.z ...' or the /tmp/.z + /etc/rc.local + dropper chmod triple"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "other"
        capability = "persistence_crontab_tmp_z"
        mitre_attack = "T1037.004,T1053.003"
        spread = "od_ kit (10 MD5MD5 files)"
        implemented_in = "evade_bg 1073853772 (ARM) / 1073887030 (MIPS) carry the full crontab line; adb_infect 1073886103 and od_*_infect clusters carry the /tmp/.z dropper"
        confidence = "high"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,1e90d958fd6ccef710ad17552fac894a"
        sample_count = "12 catalogue holder files"
        tested = "recall 10/12 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Two static_sk_bot files from the catalogue were never hit; they hold neither the crontab line nor the triple."
    strings:
        $z1 = "crontab -l 2>/dev/null; echo '*/5 * * * * { [ -x /tmp/.z ] || cp /var/.z /tmp/.z"
        $z2 = "/tmp/.z"
        $z3 = "/etc/rc.local"
        $z4 = "chmod 755 /tmp/.z;/tmp/.z %s &"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and ($z1 or ($z2 and $z3 and $z4))
}

rule Linux_Capability_AntiVmDmi
{
    meta:
        description = "Anti-VM check: reads /sys/class/dmi/id/product_name and compares to VMware; the od_ build adds /proc/driver/vmware, /sys/hypervisor/type, /proc/sys/qemu_version"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "other"
        capability = "anti_vm_dmi_hypervisor"
        mitre_attack = "T1497.001"
        spread = "xnxn (all arch variants), od_ kit (10), unpacked MD5MD5 files"
        implemented_in = "evade_detect 1073853705 (ARM), 1073834024 (MIPS), 1073837878 (AARCH64) for the full check; FUN_00400568 1073841746 (AARCH64, Loongarch, MIPS, SuperH4, x86), FUN_ram_00010446 (PowerPC, RISCV), FUN_800005d2 (m68k) for the short one"
        confidence = "high"
        reference_md5 = "024c9c6ef641449947ed6c3a65e34c79,064cfbdb66eea314aa505887b25f7530"
        sample_count = "32 catalogue holder files"
        tested = "recall 32/32 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "The condition needs product_name + VMware only; the extra od_ strings make it stronger but are not required."
    strings:
        $v1 = "/sys/class/dmi/id/product_name"
        $v2 = "VMware"
        $v3 = "/proc/driver/vmware"
        $v4 = "/sys/hypervisor/type"
        $v5 = "/proc/sys/qemu_version"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and $v1 and $v2 and any of ($v3, $v4, $v5, $v1)
}

rule Linux_Capability_AntiPtrace
{
    meta:
        description = "Anti-debug: exits when ptrace-traced, logging '[anti-debug] PTRACE DETECTED - EXITING'"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "anti_debug_ptrace_strace"
        mitre_attack = "T1622"
        spread = "josho_flood (2), frosted (2), few MD5-named and chrome/kkk-named files"
        implemented_in = "FUN_0000ed10 1073886338 (ARM, x86), FUN_00406c60 1073888035 (PowerPC, SuperH4), anti_gdb_entry 1073842630 (ARM)"
        confidence = "medium"
        reference_md5 = "069a963215886b09b570c345e46ba20a,17a373b6ef2c95394453a02d03540480"
        sample_count = "7 catalogue holder files"
        tested = "recall 7/7 catalogue holders, 2 further hits (frosted 2), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (string)"
        note = "Single anchor; the 2 frosted hits are true holders (the catalogue pivot missed them)."
    strings:
        $d1 = "[anti-debug] PTRACE DETECTED - EXITING"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and $d1
}

rule Linux_Capability_ExploitKitOd
{
    meta:
        description = "Router exploit kit of the od_ family: GPON diag_Form, Zyxel ztp handler, WebLogic wls-wsat requests and per-exploit 'fired at' log lines (3 of 6)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "router_exploit_kit_od"
        mitre_attack = "T1105,T1190,T1203"
        spread = "od_ kit (10 MD5MD5 files, 3 arches)"
        implemented_in = "od_zyxelnas_infect 1073853740, od_zygate_infect 1073853717, od_netgear_infect 1073853731, od_weblogic_infect 1073853719/1073834038, od_huawei_infect 1073853746/1073834065 (ARM, MIPS)"
        confidence = "high"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,20799844dd0d22718fea50e9dd97de81"
        sample_count = "10 catalogue holder files"
        tested = "recall 10/10 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Works on the capability (exploit templates), not on the od_ prefix, so a fork that renames the log lines is still caught by the request templates."
    strings:
        $e1 = "POST /GponForm/diag_Form?images/ HTTP/1.1"
        $e2 = "POST /ztp/cgi-bin/handler HTTP/1.1"
        $e3 = "POST /wls-wsat/CoordinatorPortType HTTP/1.1"
        $e4 = "HG532 upnp rce fired at"
        $e5 = "setup.cgi syscmd at"
        $e6 = "cve-2022-30525 fired at"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 3 of ($e*)
}

rule Linux_Capability_ExploitTendaRealtekLinksys
{
    meta:
        description = "Router exploit requests of the TCP_MSGHEAD family: Tenda set_LimitClient_cfg, Realtek miniigd picsdesc.xml, Linksys tmUnblock.cgi, JAWS /shell and shortcut_telnet.cgi (3 of 6)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "router_exploit_soap_tenda_realtek"
        mitre_attack = "T1105,T1190"
        spread = "the 9 MD5-named TCP_MSGHEAD files"
        implemented_in = "FUN_0001164c 1073840895 and FUN_00011378 1073840892 (ARM), FUN_000119c0 (x86), FUN_00011a10 1073750754 (MIPS), clean_system 1073772936 (MIPS)"
        confidence = "high"
        reference_md5 = "117a13cd039de86a48c7ee1ee7899719,1c78aba4749bd7dea183ea2fb9f966ef"
        sample_count = "9 catalogue holder files"
        tested = "recall 9/9 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "The %%-escaped URL templates are matched literally (as stored)."
    strings:
        $e1 = "POST /goform/set_LimitClient_cfg HTTP/1.1"
        $e2 = "POST /picsdesc.xml HTTP/1.1"
        $e3 = "POST /tmUnblock.cgi HTTP/1.1"
        $e4 = "GET /shell?cd%%20%%2Ftmp%%3Brm%%20-rf%%20wget.sh"
        $e5 = "GET /cgi-bin/shortcut_telnet.cgi?cd%%20%%2Ftmp"
        $e6 = "TCP_MSGHEAD_CMD cd /tmp/tmpfs/; wget http://"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 3 of ($e*)
}

rule Linux_Capability_ExploitDvrCstecgi
{
    meta:
        description = "TOTOLINK cstecgi.cgi telnet-enable request and the /dvr firmware-upgrade dropper script (2 of 3)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "busybox_cstecgi_dvr_dropper_sh"
        mitre_attack = "T1105,T1190"
        spread = "od_ kit (10)"
        implemented_in = "od_totolink_infect 1073853776 (ARM), 1073834095 (MIPS), 1073837949 (AARCH64); od_3r_infect 1073853780 (ARM), 1073766849/1073834099 (MIPS), 1073837953 (AARCH64)"
        confidence = "high"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,20799844dd0d22718fea50e9dd97de81"
        sample_count = "10 catalogue holder files"
        tested = "recall 10/10 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "Overlaps ExploitKitOd by construction (same kit)."
    strings:
        $d1 = "GET /cgi-bin/cstecgi.cgi?action=telnet&enable=1&password=%s&code=%s"
        $d2 = "cd /dvr\nrm -f /dvr/fwr /dvr/fwr_ver /dvr/fwr.tmp"
        $d3 = "/dvr/upgrade"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 2 of ($d*)
}

rule Linux_Capability_SelfCopyProcHide
{
    meta:
        description = "Self-relocation 'rm -rf %s;cp %s %s' with 'mount --bind /tmp /proc/%d' to hide the process from /proc"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "self_copy_proc_hide"
        mitre_attack = "T1036,T1564.001"
        spread = "the 9 MD5-named TCP_MSGHEAD files"
        implemented_in = "copy_file 1073840899 (ARM, PowerPC), FUN_00010b28 1073886422 (ARM), init_hidden 1073750757 (MIPS), init_persistence 1073887986 (MIPS)"
        confidence = "high"
        reference_md5 = "117a13cd039de86a48c7ee1ee7899719,1c78aba4749bd7dea183ea2fb9f966ef"
        sample_count = "9 catalogue holder files"
        tested = "recall 9/9 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = ""
    strings:
        $h1 = "mount --bind /tmp /proc/%d"
        $h2 = "rm -rf %s;cp %s %s"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and all of ($h*)
}

rule Linux_Capability_DropperShellChain
{
    meta:
        description = "Dropper command chain trying wget, tftp (two syntaxes), ftpget anonymous and curl via format strings (3 of 5)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "downloader_shell_chain"
        mitre_attack = "T1059.004,T1105"
        spread = "unpacked MD5-named builds (11), miniiupnpd/upnnpd-named (2), 3 MD5"
        implemented_in = "FUN_0040c578 1073879292 (MIPS) holds all five templates; other arches stripped, unclustered"
        confidence = "high"
        reference_md5 = "2abe9d933d6821e118fadbaadddfa44c,336057e0016e4cd35ac43ee31062f196"
        sample_count = "16 catalogue holder files"
        tested = "recall 16/16 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = ""
    strings:
        $f1 = "wget http://%s/%s/%s -O %s"
        $f2 = "tftp %s -c get %s %s"
        $f3 = "cd %s && tftp -g -r %s %s"
        $f4 = "ftpget -v -u anonymous -p anonymous -P 21 %s %s %s"
        $f5 = "curl -o %s http://%s/%s/%s"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 3 of ($f*)
}

rule Linux_Capability_SshPythonSpread
{
    meta:
        description = "Spread by ssh: sshpass -p ... ssh and 'python udp.py' with an ssh_server_ip config (2 of 3)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "gafgyt"
        capability = "ssh_python_udp_deployer"
        mitre_attack = "T1021.004,T1498"
        spread = "'net'-named gafgyt (weednet, gaynet, dicknet, fucknet, cock, ballnet, swatnet, ...)"
        implemented_in = "senditbudAMP 1073888522 (m68k, ARM, MIPS, PowerPC) and 1073812669 (x86)"
        confidence = "high"
        reference_md5 = "168036e68ea46fd5dd2be5f70e248d9d,282898fac2e6a88c4108e9751cfc8ce4"
        sample_count = "11 catalogue holder files"
        tested = "recall 11/11 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = ""
    strings:
        $s1 = "sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s"
        $s2 = "python udp.py %s %s %s"
        $s3 = "ssh_server_ip"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 2 of ($s*)
}

rule Linux_Capability_AdbSpread
{
    meta:
        description = "Android Debug Bridge spreader: '[adb] payload fired at' with the '%s|adb|shell|shell' record, or the /data/local/tmp wget dropper line"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "adb_android_spread"
        mitre_attack = "T1021,T1105"
        spread = "od_ kit (10) and one unnamed sample"
        implemented_in = "adb_infect 1073886103 (ARM, MIPS) and 1073837881 (AARCH64)"
        confidence = "high"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,20799844dd0d22718fea50e9dd97de81"
        sample_count = "11 catalogue holder files"
        tested = "recall 11/11 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "The xor22 'data/local' string is in 47 files and is deliberately not an anchor."
    strings:
        $a1 = "[adb] payload fired at %s"
        $a2 = "%s|adb|shell|shell"
        $a3 = "cd /data/local/tmp; wget http://"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and (($a1 and $a2) or $a3)
}

rule Linux_Capability_CryptoMinerXmrig
{
    meta:
        description = "Bundled XMRig miner: config wizard message, donate servers, stratum+tcp, cryptonight-monerov8 algo name (3 of 5)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "miner"
        capability = "cryptominer_xmrig"
        mitre_attack = "T1496"
        spread = "8 unpacked MD5-named files (ARM, AARCH64, x86)"
        implemented_in = "x86 library functions only (FUN_0805b300, FUN_00525a80, FUN_00460930, ...), no named clusters"
        confidence = "high"
        reference_md5 = "0ab5c6d452d9d8581923181a6e4de3a3,1a99ba4d3c42a0c75bcdc390fae345cc"
        sample_count = "8 catalogue holder files"
        tested = "recall 8/8 catalogue holders, 0 further hits (none), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "filesize bound is 16MB because the statically linked miners are large."
    strings:
        $m1 = "no valid configuration found, try https://xmrig.com/wizard"
        $m2 = "donate.v2.xmrig.com"
        $m3 = "stratum+tcp://"
        $m4 = "cryptonight-monerov8"
        $m5 = "donate-over-proxy"

    condition:
        uint32(0) == 0x464c457f and filesize < 16MB and 3 of ($m*)
}

rule Linux_Capability_CondiMarkers
{
    meta:
        description = "Condi botnet path markers /var/condibot, /var/CondiBot, /var/condinet, /tmp/condinetwork (2 of 4)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "mirai"
        capability = "condi_botnet_markers"
        mitre_attack = "T1036"
        spread = "11 MD5-named files; 2 boatnet-lineage unpacked builds also carry the paths"
        implemented_in = "killer_condi 1073887408 (ARM, SuperH4), FUN_0040d080 1073888220 (x86), FUN_000166b0 1073878036 (ARM)"
        confidence = "high"
        reference_md5 = "2abe9d933d6821e118fadbaadddfa44c,3ec5423cc6a8909a2d13cea49e8f65fa"
        sample_count = "11 catalogue holder files"
        tested = "recall 11/11 catalogue holders, 2 further hits (boatnet_xor22 2), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (strings)"
        note = "The 2 boatnet_xor22 hits carry all four paths, so they are Condi-derived rather than noise (check against Linux_Botnet_Mirai_Condi2_Top1hbt)."
    strings:
        $q1 = "/var/condibot"
        $q2 = "/var/CondiBot"
        $q3 = "/var/condinet"
        $q4 = "/tmp/condinetwork"

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and 2 of ($q*)
}

rule Linux_Capability_GoBotScannerLoader
{
    meta:
        description = "Go-built bot: pooled strings 'Auto-reconnecting (120s limit)' and 'Scanner started. Payload' (Loader)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-29"
        kind = "capability"
        family = "other"
        capability = "go_bot_scanner_loader"
        mitre_attack = "T1090,T1498,T1595"
        spread = "1 file (MIPS64) plus its go_bot_b sibling"
        implemented_in = "Go binary, no function cluster (string pool only)"
        confidence = "low"
        reference_md5 = "7315d6b3184da18833691c46c63e8ede"
        sample_count = "1 catalogue holder files"
        tested = "recall 1/1 catalogue holders, 1 further hits (go_bot_b 1), tp+fp truth judged by raw byte scan (2026-10-29)"
        arch_note = "arch-independent (Go string pool)"
        note = "Single reference sample; the sibling hit in go_bot_b is a true holder of the same pool."
    strings:
        $g1 = "Auto-reconnecting (120s limit)"
        $g2 = "Scanner started. Payload"

    condition:
        uint32(0) == 0x464c457f and filesize < 16MB and all of ($g*)
}
