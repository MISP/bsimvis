rule Linux_Botnet_Tsunami_NetSeries
{
    meta:
        description = "Tsunami/Kaiten-style IRC bot, *net series: OVH-STORM/HIPER-OVH/DOMINATE flood names, CFKILL captcha flood and sshpass udp.py spreader"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "tsunami"
        variant = "net_series"
        capabilities = "irc_c2,udp_flood,http_flood,raw_socket_flood,ssh_spread"
        confidence = "medium"
        reference_md5 = "168036e68ea46fd5dd2be5f70e248d9d"
        sample_count = "12"
        tested = "recall 12/12 on group, 0 hits in siblings (tsun_holocaust), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Sibling Holocaust shares the dayzddos.co note and key tables, so those are deliberately not anchors. Second path covers the NFODROP/OVHKILL/STDHEX flavour (7b70aceca81d038bcf859ea5a28f9fd9)."
    strings:
        $a1 = "DOMINATE Flooding %s for %d seconds." ascii
        $a2 = "Failed setting raw headers mode." ascii
        $a3 = "ssh_server_ip" ascii
        $a4 = "Hax0r@1337" ascii
        $a5 = "python udp.py %s %s %s" ascii
        $a6 = "CFKILL/1.1" ascii
        $f1 = "OVH-STORM" ascii
        $f2 = "HIPER-OVH" ascii
        $f3 = "HYDRA-KILL" ascii
        $f4 = "KILLALLV3" ascii
        $f5 = "OVH-PACKET" ascii
        $k1 = "d4mQasDSH6" ascii
        $g1 = "NFODROP" ascii
        $g2 = "OVHKILL" ascii
        $g3 = "STDHEX" ascii
        $g4 = "TSource Engine Query" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and (
            (2 of ($a*) and 1 of ($f*)) or
            ($k1 and $g4 and 2 of ($g1, $g2, $g3))
        )
}

rule Linux_Botnet_Tsunami_Holocaust
{
    meta:
        description = "Tsunami/Kaiten-style IRC bot, Holocaust fork: obfuscated key tables, dayzddos.co note, IHolocaust ANSI banner and OVHCRUSH/CPU-RAPE/HOME-SEC flood names"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "tsunami"
        variant = "holocaust"
        capabilities = "irc_c2,udp_flood,tcp_flood,http_flood,source_engine_query,game_ddos"
        confidence = "medium"
        reference_md5 = "318bf65afc7d06fbe762feac8bdfcaf3"
        sample_count = "9"
        tested = "recall 9/9 on group, 0 hits in siblings (tsun_net_series), 1 outside (sh4 9fa3e4cb407b4a15244191292efac441, filed under xor_wbyy, probably a Holocaust build) (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Stripped mips/mpsl members still match because the key strings survive in .rodata. The sh4 hit in xor_wbyy is likely a mis-grouped Holocaust sample, not a false positive."
    strings:
        $a1 = "xmiwdio MOQWI\"K (**(D8u9289d2892e9 NIOWJDiojwioWAJM DKLAM)" ascii
        $a2 = "9u123448u124au814d4x10" ascii
        $a3 = "IHolocaust" ascii
        $f1 = "OVHCRUSH" ascii
        $f2 = "OVHFREEZE" ascii
        $f3 = "CPU-RAPE" ascii
        $f4 = "HOME-SEC" ascii
        $f5 = "COD-CRASH" ascii
        $f6 = "MOLLY-NFO" ascii
        $f7 = "SLAP-OVH" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and (
            2 of ($a*) or
            4 of ($f*)
        )
}

rule Linux_Botnet_Gafgyt_BusyboxWgetCgi
{
    meta:
        description = "Bot that spreads by wget/nc of an arm7 payload through embedded CGI and UPnP exploit requests (D-Link/Netgear-style ping.cgi and login.cgi, Huawei DeviceUpgrade_1, picsdesc.xml SOAP)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "gafgyt"
        variant = "busybox_wget_cgi"
        capabilities = "exploit_scanner,wget_dropper,credential_list"
        confidence = "low"
        reference_md5 = "6f276fb35587a7fa723543ff6df97281"
        sample_count = "2 (of 3 in group; the third is an unrelated Android persistence bot)"
        tested = "recall 2/3 on group, 0 hits outside (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Group is not coherent: e2a883b3... (Android persistence, crontab, chattr, su paths) shares nothing with the two exploit builds. Rule covers mipsel and arm7 only. A boatnet sample (0c35812d...) also embeds POST /ctrlt and /picsdesc.xml exploit requests, hence the ($e1|$e2) gate."
    strings:
        $e1 = "GET /login.cgi?cli=aa%20aa%27;wget%20http://" ascii
        $e2 = "GET /ping.cgi?pingIpAddress=" ascii
        $e3 = "POST /ctrlt/DeviceUpgrade_1 HTTP/1.1" ascii
        $e4 = "POST /picsdesc.xml HTTP/1.1" ascii
        $e5 = "/bin/busybox wget http://" ascii
        $e6 = "nc 37.48.254.120 25565" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and 1 of ($e1, $e2) and 2 of ($e3, $e4, $e5, $e6)
}

rule Linux_Capability_UdpVseTinyFlood
{
    meta:
        description = "Tiny MIPS flood module with a method table UDP-RAW/UDP-CUSTOM/UDP-VSE/TCP-FIVEM/TCP-HANDSHAKE/TCP-ACK/MINECRAFT and a hardcoded controller IP"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "capability"
        family = "other"
        capability = "udp_flood,tcp_flood,game_server_flood"
        capabilities = "udp_flood,tcp_flood,vse_flood,minecraft_flood,fivem_flood"
        confidence = "low"
        reference_md5 = "41300cb20e5e3b13f2630603622a22d6,7a4879efbc7c38d4bca7e970bead8a33"
        sample_count = "2 unpacked (4 files with the UPX originals named boatnet.mips/mpsl)"
        tested = "recall 2/2 on unpacked members, 0 hits outside (2026-10-28)"
        arch_note = "arch-independent (strings); MIPS BE and LE builds, UPX originals carry no strings and are not covered"
        note = "The two UPX originals (boatnet.mips, boatnet.mpsl) carry no strings and are not matched; their unpacked siblings are. Only two distinct samples (MIPS BE and LE of one build)."
    strings:
        $m1 = "UDP-CUSTOM" ascii
        $m2 = "UDP-VSE" ascii
        $m3 = "TCP-FIVEM" ascii
        $m4 = "TCP-HANDSHAKE" ascii
        $m5 = "MINECRAFT" ascii
        $m6 = "TCP-ACK" ascii
        $ip = "212.43.149.186" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 100KB and 5 of ($m*) and $ip
}

rule Linux_Botnet_Other_MountFlagsHelper
{
    meta:
        description = "Small MIPS-LE bot (manji/nerv names) sharing a /proc/mounts + /etc/mtab mount-flag block (nosuid/noexec/nodev/noatime/nodiratime) next to /proc/<pid>/cmdline process scanning"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "other"
        variant = "mtab_manji"
        capabilities = "process_scan,mount_enumeration,http_flood"
        confidence = "low"
        reference_md5 = "a27302b04b1eab1a905f268218d389b2,5991b6c6b8e96e75fdb71ae25b68bc97"
        sample_count = "4 (3 share the mount block)"
        tested = "recall 3/4 on group, 0 hits outside (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Group is weakly coherent: 8ace49d1... (dvrHelper, systemd bot.service persistence) lacks the mount-flag block. The block looks like a shared helper, not a family marker; treat as a clustering aid."
    strings:
        $mt = "/etc/mtab" ascii
        $pm = "/proc/mounts" ascii
        $f1 = "nosuid" ascii
        $f2 = "noexec" ascii
        $f3 = "nodiratime" ascii
        $f4 = "noatime" ascii
        $cm = "/proc/%d/cmdline" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 200KB and $mt and $pm and $cm and all of ($f*)
}
