rule Linux_Botnet_Mirai_Demons
{
    meta:
        description = "Mirai DEMONS build: D-Link model-name table and vendor default-credential list"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "demons"
        capabilities = "telnet_scanner,udp_flood,killer"
        confidence = "high"
        reference_md5 = "1d6e40f1e0f2624e4e31620d9b8123f5,1de0d0093f48ce33ae88ded5530830c6,20be272edd356638e4df9fcf77f9d33a"
        sample_count = "18"
        tested = "recall 18/18 on group, 0 hits in siblings (mushi, nuclear), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Mushi shares only wrgnd08_dlob_dir815 and wrgn49_dlob_dir600b with this model list and is NOT matched; it is a separate fork (see Linux_Botnet_Mirai_Mushi). Model/credential tables are plaintext (the XOR-0x54 table is not used as an anchor)."
    strings:
        $m1 = "wrgn39_dlob.hans_dir645_V1" ascii
        $m2 = "wrgac01_dlob.hans_dir865" ascii
        $m3 = "wapnd15_dlob_dap1522b" ascii
        $m4 = "wrgn23_dlwbr_dir300b" ascii
        $m5 = "wrgn28_dlob_dir412" ascii
        $c1 = "vertex25ektks123" ascii
        $c2 = "$chwarzepumpe" ascii
        $c3 = "CTLSupport12" ascii
        $c4 = "Is$uper@dmin" ascii
        $c5 = "AitbISP4eCiG" ascii
        $l1 = "<<< %s(un='%s') = %u" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 2 of ($m*) and 3 of ($c*) and $l1
}

rule Linux_Botnet_Mirai_Mushi
{
    meta:
        description = "Mushi bot fork (Mirai-derived): mushi init.d/systemd persistence, self-restart script and cmdline/path kill strings"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "mushi"
        capabilities = "persistence,killer,downloader"
        confidence = "medium"
        reference_md5 = "6ab3f168d8a10c45c1c2914dcce68587,da8f55004e542890460736e660b9c424"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 hits in siblings (demons), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); only MIPS-LE seen"
        note = "Separate fork, not a DEMONS sub-variant: shares only wrgnd08_dlob_dir815 and wrgn49_dlob_dir600b with DEMONS, none of the other model names or credentials. Two samples only."
    strings:
        $p1 = "/etc/init.d/mushi_%ld" ascii
        $p2 = "/etc/systemd/system/mushi%ld.service" ascii
        $p3 = "nohup /bin/bash /tmp/self_restart.sh > /dev/null 2>&1 &" ascii
        $k1 = "because it matched kill string \"%s\" in cmdline_kill_strings." ascii
        $k2 = "because it matched kill string \"%s\" in path_kill_strings." ascii
        $k3 = "Deleted %s because it was a statically linked executable file" ascii
        $x1 = "Packet Type: %u\n" ascii
        $x2 = "hexakisoctahedron" ascii
        $x3 = "Description=The service of legends" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 2 of ($p*) and 2 of ($k*) and 1 of ($x*)
}

rule Linux_Botnet_Mirai_Nuclear
{
    meta:
        description = "Mirai nuclear fork: raw.flameblox.com loader host together with the boatnet-style XOR-0x22 attack table or the Valve TSource flood"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "nuclear"
        capabilities = "udp_flood,valve_source_flood,downloader"
        confidence = "high"
        reference_md5 = "0fac505e1c34ad2c2f108c8fa586d8a3,23d815d05f6901340b4e2ed399dcc65b,26fdc113cd46767e6fdc2dac14ad5508"
        sample_count = "13"
        tested = "recall 13/13 on group, 0 hits in siblings (boatnet), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Shares the boatnet XOR-0x22 table and TSource flood, so the loader host is the discriminator; boatnet files are not matched because they lack raw.flameblox.com. A rebuild with another loader host would be missed."
    strings:
        $host = "raw.flameblox.com" ascii
        $x1 = "HWCLVGAJ" ascii
        $x2 = "QOACFOKL" ascii
        $x3 = "QWRRMPV" ascii
        $x4 = "RCQQUMPF" ascii
        $s1 = "TSource Engine Query" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $host and (any of ($x*) or $s1)
}

rule Linux_Botnet_Mirai_Frosted
{
    meta:
        description = "Mirai FROSTED variant: FROSTED banner, ROBLOXSPAM attack name and ptrace/debugger anti-debug messages"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "frosted"
        capabilities = "udp_flood,anti_debug,roblox_spam"
        confidence = "high"
        reference_md5 = "069a963215886b09b570c345e46ba20a,02e9bb2a57a6c924818ff0883bf219e4,17a373b6ef2c95394453a02d03540480"
        sample_count = "7"
        tested = "recall 7/7 on group, 0 hits unrelated incl. josho_flood (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Banner is required. Two builds filed under josho_flood carry the same anti-debug and ROBLOXSPAM without the banner; see Linux_Botnet_Mirai_Frosted_Locker."
    strings:
        $b = "FROSTED IS HERE NIGGA" ascii
        $a1 = "[anti-debug] DEBUGGER DETECTED (%s) - EXITING" ascii
        $a2 = "[anti-debug] PTRACE DETECTED - EXITING" ascii
        $r = "ROBLOXSPAM" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $b and any of ($a*, $r)
}

rule Linux_Botnet_Mirai_Frosted_Locker
{
    meta:
        description = "Frosted-lineage Mirai build without the FROSTED banner: anti-debug messages, ROBLOXSPAM and the [locker]/[killer] killed-process logging"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "frosted_locker"
        capabilities = "anti_debug,killer,locker,roblox_spam"
        confidence = "medium"
        reference_md5 = "bdb431408ca37c09241892aaf05595b4,feff91effc49fba46a78ddc61cbb1ff4"
        sample_count = "2"
        tested = "recall 2/2 on its two members (filed in josho_flood), 0 hits in frosted, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); PowerPC and SuperH seen"
        note = "Overlaps Frosted code but lacks the banner and has [locker] logging; kept as its own rule instead of loosening Linux_Botnet_Mirai_Frosted."
    strings:
        $a1 = "[anti-debug] DEBUGGER DETECTED (%s) - EXITING" ascii
        $a2 = "[anti-debug] PTRACE DETECTED - EXITING" ascii
        $l = "[locker] killed cmd: %s (pid: %d)" ascii
        $k = "[killer] killed mirai (pid: %d)" ascii
        $r = "ROBLOXSPAM" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and all of ($a*) and $l and $k and $r
}

rule Linux_Botnet_Mirai_Josho_Flood
{
    meta:
        description = "Mirai Josho / udp_flood series: verbose bracketed attack logging ([udp_plain_flood], [watch_time], [tcpbypass_flood]) in a static uClibc build"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "josho"
        capabilities = "udp_flood,tcp_bypass_flood,psh_ack_flood,icmp_flood,killer"
        confidence = "high"
        reference_md5 = "0064bd45fdb7a8dc60d1d0a72590f8ab,085efcef06ea995885d7879fd3af2be9,09bad3b3149802d08a050f09f3c15b26"
        sample_count = "14"
        tested = "recall 11/14 on group (11/12 real Josho; the 2 misses are Frosted-lineage and 1 is a stripped dropper), 0 hits in siblings (xor_wbyy, p_killermaps), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Group josho_flood is mixed: Josho.ppc and Josho.sh4 are Frosted-lineage builds and one mpsl (b1ef9c2a...) holds only uClibc loader strings plus 176.65.139.124; none carry the flood log lines."
    strings:
        $w1 = "[watch_time] started: ('%d') | seconds: ('%d')" ascii
        $w2 = "[watch_time] kill() pid: ('%d'), exit()" ascii
        $f1 = "[udp_plain_flood] started: ('%d')" ascii
        $f2 = "[tcpbypass_flood] started: ('%d')" ascii
        $f3 = "[udp_flood] started: ('%d')" ascii
        $f4 = "[udp_plain_flood] socket() failed" ascii
        $f5 = "[udp_flood] socket() failed" ascii
        $f6 = "[psh_ack_flood] started" ascii
        $f7 = "[icmp_flood] setsockopt() failed" ascii
        $k = "killed mirai" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 1 of ($w*) and 2 of ($f*, $k)
}

rule Linux_Botnet_Mirai_SecureCore
{
    meta:
        description = "Mirai-derived MIPS-LE build carrying the embedded key A_2026_SECURE_CORE_BOT_KEY!"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "securecore"
        capabilities = "encrypted_c2"
        confidence = "medium"
        reference_md5 = "b1676efd14ae4d2b1350ce2eab07095d,f3f8af384317a201a9cc4167d3004912"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); only MIPS-LE seen"
        note = "Single anchor (the key literal); only two MIPS-LE samples known, so the rule identifies this build, not a code family."
    strings:
        $key = "A_2026_SECURE_CORE_BOT_KEY!" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $key
}
