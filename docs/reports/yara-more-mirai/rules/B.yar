rule Linux_Botnet_Mirai_PKillerMaps
{
    meta:
        description = "Mirai p<arch> series: /proc killer that logs each kill with a [0clKiller*] tag, plus the 'im in deep sorrow.' marker"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "mirai"
        variant = "pkillermaps"
        capability = "killer"
        capabilities = "killer,watchdog_handling,xor22_table"
        confidence = "high"
        reference_md5 = "9de8506ce2cc10ce450ab66e39548859,fd42521e6d8d0fa8c1ab85a82e0d3d32,4322e27e542e3a062725d2eda6595d33"
        sample_count = "64 files (48 with strings, 16 UPX originals)"
        tested = "recall 48/48 unpacked (48/64 incl. 16 UPX originals), 0 hits in 193 sibling files, 0 in 158+21 unrelated/unclassified (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "UPX-packed originals carry no strings and are not covered; scan the unpacked sibling. The XOR-0x22 table is shared with boatnet and is deliberately not used."

    strings:
        $sorrow = "im in deep sorrow." ascii
        $k_maps = "[0clKillerMaps] Killed Process: %s, PID: %d" ascii
        $k_exe  = "[0clKillerKillerEXE] Killed process: %s, PID: %d" ascii
        $k_stat = "[0clKillerStat] Killed Process: %s, PID: %d" ascii
        $wd     = "/dev/misc/watchdog" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and
        2 of ($k_*) and ($sorrow or $wd)
}


rule Linux_Botnet_Mirai_PKillerMaps_HttpFlood
{
    meta:
        description = "p<arch> killer build that also carries an HTTP flood (Chrome-58 User-Agent, urlencoded POST body)"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "pkillermaps_http"
        capabilities = "killer,http_flood"
        confidence = "medium"
        reference_md5 = "8fed03a0b4d68e015cf76ba4947c5697,0efee0938b784a2f4a1a6c30c677db7c,844d438f332bf8bb9bfabec5cac8f5a5"
        sample_count = "27 of 48 stringed files"
        tested = "27 hits in group, 0 in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Subset of Linux_Botnet_Mirai_PKillerMaps. The HTTP strings alone are generic (found in other families); the rule needs the killer strings too."

    strings:
        $k_maps = "[0clKillerMaps] Killed Process" ascii
        $k_exe  = "[0clKillerKillerEXE] Killed process" ascii
        $http   = " HTTP/1.1\x0d\x0aHost: " ascii
        $form   = "Content-Type: application/x-www-form-urlencoded" ascii
        $ua     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and
        $k_maps and $k_exe and $http and $form and $ua
}


rule Linux_Botnet_Mirai_PKillerMaps_Lean
{
    meta:
        description = "p<arch> killer build without the HTTP flood: killer strings present, no HTTP request template or Chrome User-Agent"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "pkillermaps_lean"
        capabilities = "killer"
        confidence = "medium"
        reference_md5 = "c526b3460c087358b047119e25b73e11,0987f84e807a81c742e344c7cec0cb02,07aef2ea126c64fd555d58a906867e7b"
        sample_count = "21 of 48 stringed files"
        tested = "21 hits in group (complement of HttpFlood), 0 in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); mostly mips, mpsl, m68k, ppc, sh4, some arm"
        note = "Defined by the absence of the HTTP strings, so it is the complement of the HttpFlood rule inside PKillerMaps and also hits the parent rule."

    strings:
        $sorrow = "im in deep sorrow." ascii
        $k_maps = "[0clKillerMaps] Killed Process" ascii
        $k_exe  = "[0clKillerKillerEXE] Killed process" ascii
        $form   = "Content-Type: application/x-www-form-urlencoded" ascii
        $ua     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and
        $sorrow and $k_maps and $k_exe and not $form and not $ua
}

rule Linux_Botnet_Mirai_Xnxn
{
    meta:
        description = "Mirai-derived xnxn bot: !-prefixed attack commands with key=value parameters, VMware check, embedded hex key blobs"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "mirai"
        variant = "xnxn"
        capability = "vm_check"
        capabilities = "udp_flood,http_flood,icmp_flood,vm_check,single_instance_lock,command_params"
        confidence = "high"
        reference_md5 = "3da829c5a17477e1c0487d1898d83765,605f69c6a57eacdd4f1775db0a3eef82,0ed905e85235efcfce5ba4c042375b63"
        sample_count = "32 files (22 with strings, 10 UPX originals)"
        tested = "recall 22/22 unpacked (22/32 incl. 10 UPX originals), 0 hits in 47 sibling files, 0 in 336+21 unrelated/unclassified (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "UPX-packed originals carry no strings and are not covered; scan the unpacked sibling. The VMware check and the pong/key=value parser also occur in other groups (zbot_static, flylegit), so those strings are not used alone."

    strings:
        $c_plain  = "!udpplain" ascii
        $c_custom = "!udpcustom" ascii
        $p_src    = "srcport=" ascii
        $p_pay    = "payload=" ascii
        $p_gport  = "gport=" ascii
        $lock     = "/tmp/.bot_lock" ascii
        $blob     = "fd00e82a0a3d86af73deacaa9df16432" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and
        $c_plain and $c_custom and 1 of ($p_*) and ($lock or $blob)
}


rule Linux_Botnet_Mirai_Xnxn_KeyA
{
    meta:
        description = "xnxn build carrying the bb63937d... 48-byte hex blob next to the shared 32-byte and 48-byte blobs"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "xnxn_key_a"
        capabilities = "udp_flood,http_flood,icmp_flood,vm_check"
        confidence = "medium"
        reference_md5 = "3da829c5a17477e1c0487d1898d83765,92d54dea46de76e881efe7be93a1e973,73ddc9560f56a45abc67d3ff34fe4493"
        sample_count = "11 stringed files (one per arch), 11 of 22"
        tested = "11 hits in group, 0 in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); mips, loongarch64, aarch64, riscv32/64, x86_64, i386, ppc, m68k, sh2, sh4"
        note = "One of two builds of the same bot (see KeyB); the blobs look like embedded keys (unverified). Hits the parent Xnxn rule."

    strings:
        $keyA   = "bb63937d0e510b0ba7204ed3cc2109945dcbcaf443c9437ff99283aff7f35510e7a47c1194aff24087b8ab31da220ae8" ascii
        $shared = "b4c4d0f3fad5c8ec76046ea95517e1412266c4e21a801dbb7d792102afcff1d8fe35284185de0ff65279f7887a91585f" ascii
        $c_plain = "!udpplain" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and
        $keyA and $shared and $c_plain
}


rule Linux_Botnet_Mirai_Xnxn_KeyB
{
    meta:
        description = "xnxn build carrying the ed74bacf... 48-byte hex blob next to the shared 32-byte and 48-byte blobs"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "xnxn_key_b"
        capabilities = "udp_flood,http_flood,icmp_flood,vm_check"
        confidence = "medium"
        reference_md5 = "605f69c6a57eacdd4f1775db0a3eef82,991c092a636801994f3b88aec45fe115,0ed905e85235efcfce5ba4c042375b63"
        sample_count = "11 stringed files (one per arch), 11 of 22"
        tested = "11 hits in group, 0 in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings); same 11 arches as KeyA"
        note = "Second build of the same bot (see KeyA); the blobs look like embedded keys (unverified). Hits the parent Xnxn rule."

    strings:
        $keyB   = "ed74bacffb15a80cad29dd0b278cae801c7c823acfed171cb761b2f0473536a06dd68b3a5923bd7e5fd1e597c1ba85a6" ascii
        $shared = "b4c4d0f3fad5c8ec76046ea95517e1412266c4e21a801dbb7d792102afcff1d8fe35284185de0ff65279f7887a91585f" ascii
        $c_plain = "!udpplain" ascii

    condition:
        uint32(0) == 0x464c457f and filesize < 2MB and
        $keyB and $shared and $c_plain
}
