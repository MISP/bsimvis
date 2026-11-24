rule Linux_Botnet_Mirai_Flylegit
{
    meta:
        description = "Mirai-derived bot carrying the t.me/flylegit honeypot-detection banner, the udpplain attack name, a four-field %31s command parser and the iran.<arch> dropper URL"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "flylegit"
        capabilities = "honeypot_check,udp_flood,c2_ping,downloader"
        confidence = "medium"
        reference_md5 = "1108e44314223ae8245932c239388a5a,e080c88f93ccb57569a4d6a3dd8fbd97,2269684e7950087b4bc1d1d698e52b92"
        sample_count = "13"
        tested = "recall 12/12 non-packed on flylegit, 0 hits on unrelated families, 4 hits in sibling-lineage files (3 iran_dg MIPS builds, 1 boatnet.sh4 filed under boatnet_xor22) that carry the same banner (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "The 4 extra hits are true flylegit builds filed in other groups. The one miss is boatnet.x86_64, the UPX original of an unpacked member."

    strings:
        $s_banner = "Looks like you are a honeypot, this tool was made by t.me/flylegit!" ascii
        $s_tme    = "t.me/flylegit" ascii
        $s_udp    = "udpplain" ascii
        $s_fmt    = "%31s %31s %7s %11s" ascii
        $s_c2     = "176.65.139.59" ascii
        $s_dl     = "http://94.154.43.201/iran." ascii
        $s_iran   = "/tmp/iran.%s" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and
        ( $s_banner or $s_tme or 2 of ($s_udp, $s_fmt, $s_c2, $s_dl, $s_iran) )
}

rule Linux_Botnet_Mirai_Iran_ProcScan
{
    meta:
        description = "Static MIPS bot (iran.* builds) that reads /proc/<pid>/status for SigCgt and VmSize and walks /proc/<pid>/task/<tid>/children and /proc/<pid>/fd/0 to find processes"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "iran_procscan"
        capabilities = "killer,process_scan"
        confidence = "medium"
        reference_md5 = "807787a0b175aec8ef4babf41f61b95f,05d802743047f1bd7771a4dce73eeb70"
        sample_count = "4"
        tested = "recall 4/4 on the SigCgt sub-set (4/13 of iran_dg), 0 hits elsewhere in 423 files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Covers only the 4 iran.* builds with the process-inspection strings. The other iran_dg members (dg.*, wife.*, the flylegit-banner MIPS trio, bot.*) do not carry these strings; see Linux_Botnet_Mirai_Flylegit and Linux_Botnet_Mirai_Condi2_Top1hbt."

    strings:
        $s_sig   = "SigCgt:" ascii
        $s_vm    = "VmSize:" ascii
        $s_child = "/proc/%d/task/%d/children" ascii
        $s_fd0   = "/proc/%s/fd/0" ascii
        $s_login = "/bin/login" ascii
        $s_scan  = "%*d %*s %*c %d" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 4 of them
}

rule Linux_Botnet_Mirai_Condi2_Top1hbt
{
    meta:
        description = "Small MIPS bot (bot.mips, bot.mpsl) that reports condi2 host:port and fetches per-arch top1hbt.* payloads over a hand-rolled HTTP/1.0 client"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "condi2_top1hbt"
        capabilities = "downloader,http_client"
        confidence = "medium"
        reference_md5 = "25535a645dd035412d06b3b1367b4a60,f2ba198dcea60ce07f2a0ded504784a1"
        sample_count = "2"
        tested = "recall 2/2 on its two files (2/13 of iran_dg), 0 hits elsewhere in 423 files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "These two files are not part of the iran lineage: non-library code similarity to other iran_dg members is 0.29-0.35."

    strings:
        $s_condi = "condi2 %s:%d" ascii
        $s_ua    = "GET /%s HTTP/1.0\r\nUser-Agent: Update v1.0\r\n\r\n" ascii
        $s_arm   = "top1hbt.arm7" ascii
        $s_mips  = "top1hbt.mpsl" ascii
        $s_x86   = "top1hbt.x86_64" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and
        ( $s_condi or 2 of ($s_arm, $s_mips, $s_x86, $s_ua) )
}

rule Linux_Botnet_Mirai_Cling_Scanner
{
    meta:
        description = "Mirai-derived dropper that appends a .cling copy of itself to inittab and rc scripts, remounts /proc over its own pid and scans with embedded router exploit requests"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "cling"
        capabilities = "persistence,exploit_scanner,hide_process"
        confidence = "high"
        reference_md5 = "2dca42cb8ffb7bebe4e7c4f79a8241d5,7a78f435d04ec2cfb5e37e56298a4175"
        sample_count = "9"
        tested = "recall 9/9 on group, 0 hits in 427 other files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Both sub-sets (builds with hard-coded server IPs, and builds with only the exploit requests) share all .cling persistence strings, so they are one codebase."

    strings:
        $p1 = "echo ::once:/usr/local/bin/.cling >> /etc/inittab" ascii
        $p2 = "echo /usr/local/bin/.cling >> /etc/rc.d/rc.boot" ascii
        $p3 = "echo /root/.cling >> /etc/init.d/rcS" ascii
        $p4 = "chmod +x /usr/local/bin/.cling" ascii
        $h1 = "mount --bind /tmp /proc/%d" ascii
        $h2 = "rm -rf %s;cp %s %s" ascii
        $x1 = "/goform/set_LimitClient_cfg" ascii
        $x2 = "TCP_MSGHEAD_CMD cd /tmp/tmpfs/" ascii
        $x3 = "/cgi-bin/shortcut_telnet.cgi?cd" ascii
        $x4 = "/device.rsp?opt=sys&cmd=" ascii
        $x5 = "/tmUnblock.cgi HTTP/1.1" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and
        ( 2 of ($p*) and ( 1 of ($h*) or 1 of ($x*) ) )
}

rule Linux_Botnet_Mirai_Kushnet
{
    meta:
        description = "Kushnet bot: rival-killing, boot-hook persistence (ksync, install-recovery, libkys.so) and a runtime C2 redirect file, identified by its own log and path strings"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "kushnet"
        capabilities = "killer,persistence,c2_redirect,android_persistence"
        confidence = "high"
        reference_md5 = "4dcd0fffcf3ef5be03ca71f56954c514,98adbf4b04578c5157c4631a94e816a6,a8b573631cbaa1e051eff645242ba5cb"
        sample_count = "7"
        tested = "recall 7/7 on group, 0 hits in 429 other files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Includes the m68k and sh4 members that carry the full string set; the group label rival_killer undersells it, the family names itself kushnet. Overlaps with the existing kushnet_google_firestick rule by design."

    strings:
        $k1 = "kushnet_boot_v1" ascii
        $k2 = "/tmp/.kushnet_pw.bak" ascii
        $k3 = "KUSHNET_GUARD" ascii
        $k4 = "libkys.so" ascii
        $k5 = "killed rival pid=%d exe=%s" ascii
        $k6 = "removed persist path=%s" ascii
        $k7 = "/tmp/.c2_redirect" ascii
        $k8 = "com.google.firestick.youtube" ascii
        $k9 = "boot hook install-recovery" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 3 of them
}

rule Linux_Botnet_Mirai_Sysd_Dropper
{
    meta:
        description = "Dropper that copies itself to /usr/bin/.sh, installs an /etc/init.d/sysd script with an rc2.d S99sysd link, and keeps a /tmp/.instance_lock single-instance lock while reading /proc/net/tcp6 socket inodes"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "sysd"
        capabilities = "persistence,single_instance,socket_inode_scan"
        confidence = "medium"
        reference_md5 = "38ea4d206fe17ad4889d0cfe43f5bd0f,468723b8a4a80d29f73811b3ec0b061b"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 hits in 434 other files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Two files only. The x86_64 member also embeds a toxcore (Tox DHT) stack, the MIPS one is the small dropper; the shared part is the persistence/lock code."

    strings:
        $s_init = "/etc/init.d/sysd" ascii
        $s_rc2  = "/etc/rc2.d/S99sysd" ascii
        $s_cp   = "cp %s /usr/bin/.sh" ascii
        $s_run  = "#!/bin/sh\n/usr/bin/.sh &\n" ascii
        $s_lock = "/tmp/.instance_lock" ascii
        $s_tcp6 = "%*d: %64[0-9A-Fa-f]:%*x %*s %*s %*s %*s %*s %*s %u" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 8MB and
        ( 2 of ($s_init, $s_rc2, $s_cp, $s_run) and ($s_lock or $s_tcp6) )
}
