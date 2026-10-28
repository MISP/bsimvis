rule Linux_Botnet_Mirai_Boatnet
{
    meta:
        description = "Mirai fork 'boatnet': table entries XOR-encoded with 0x22 (stats, killer and scanner words), or the SNQUERY probe together with the 'Self Rep NeTiS' banner"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "mirai"
        variant = "boatnet"
        capabilities = "udp_flood,ssdp_flood,source_query_flood,telnet_scanner,killer,netis_backdoor_spread"
        confidence = "high"
        reference_md5 = "26ead3fb6e79060c84225282f3955ed4,3ec5423cc6a8909a2d13cea49e8f65fa"
        sample_count = "107 files (64 unpacked, 43 UPX originals)"
        tested = "recall 58/62 scanned non-packed, 2 hits in iran_dg (bot.mips/bot.mpsl, netis-banner lineage), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "UPX originals carry no strings and are not covered (packer concern). 4 stripped members without any table or banner are missed. iran_dg bot.* hits come from the SNQUERY+banner path and share the netis lineage."

    strings:
        $x1 = "\rQVCVWQ\""
        $x2 = "\rFGT\rUCVAJFME\""
        $x3 = "\rAOFNKLG\""
        $x4 = "\rGVA\rFGDCWNV\rUCVAJFME\""
        $x5 = "\rRPMA\rLGV\rVAR\""
        $x6 = "SVZ@MV\""
        $x7 = "\rRPMA\r\""
        $s1 = "SNQUERY: 127.0.0.1:AAAAAA:xsvr"
        $s2 = "Self Rep Fucking NeTiS and Thisity"

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and (2 of ($x*) or all of ($s*))
}

rule Linux_Botnet_Mirai_Boatnet_Netis
{
    meta:
        description = "Boatnet build carrying the netis banner 'Self Rep Fucking NeTiS', the SNQUERY probe and the GLVGP/NKLWZQJGNN XOR-0x22 table entries"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "boatnet_netis"
        capabilities = "udp_flood,ssdp_flood,source_query_flood,netis_backdoor_spread"
        confidence = "high"
        reference_md5 = "26ead3fb6e79060c84225282f3955ed4,00de94cb911eccf096e190ef7fe9cb08"
        sample_count = "about 27 of the 107 group files"
        tested = "recall 39/39 stringed netis members, 2 hits in iran_dg, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Also hits the parent boatnet family (expected). Hits iran_dg bot.mips/bot.mpsl which share the banner; the netis banner is the discriminator against the tftp variant."

    strings:
        $x1 = "\rFGT\rUCVAJFME\""
        $x2 = "NKLWZQJGNN\""
        $x3 = "GLVGP\""
        $s1 = "SNQUERY: 127.0.0.1:AAAAAA:xsvr"
        $s2 = "Self Rep Fucking NeTiS and Thisity"
        $s3 = "urn:dial-multiscreen-org:service:dial:1"

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and $s2 and 2 of ($x*, $s1, $s3)
}

rule Linux_Botnet_Mirai_Boatnet_Tftp
{
    meta:
        description = "Boatnet build with a tftp/ftpget/curl loader grammar and the FTPjGNRGP/RPMA-ARWKLDM XOR-0x22 table entries, no netis banner"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "boatnet_tftp"
        capabilities = "udp_flood,tftp_ftp_curl_loader,telnet_scanner"
        confidence = "high"
        reference_md5 = "3ec5423cc6a8909a2d13cea49e8f65fa,2abe9d933d6821e118fadbaadddfa44c"
        sample_count = "about 16 of the 107 group files"
        tested = "recall 15/15 stringed members, 0 hits in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "Also hits the parent boatnet family (expected)."

    strings:
        $x1 = "FTPjGNRGP\""
        $x2 = "\rRPMA\rARWKLDM\""
        $x3 = "\rRPMA\rLGV\rPMWVG\""
        $x4 = "\rFGT\rLGVQNKLI\r\""
        $c1 = "tftp %s -c get %s %s"
        $c2 = "cd %s && tftp -g -r %s %s"
        $c3 = "ftpget -v -u anonymous -p anonymous -P 21 %s %s %s"

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and 2 of ($x*) and 1 of ($c*)
}

rule Linux_Botnet_Mirai_XorWbyy
{
    meta:
        description = "Mirai variant whose config table uses a different XOR key; decoded-table fragments such as wbYyC107:6;6: and kf{bhjpl`wbkfgqlm"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "xor_wbyy"
        capabilities = "udp_flood,telnet_scanner,killer"
        confidence = "medium"
        reference_md5 = "1a907dd4c92797996033348bf1be51fb,1d1b07065613cc12b70a8e11817e0aa5"
        sample_count = "16 files"
        tested = "recall 13/16, 0 hits in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "The 3 misses (two sh4, azsxd.mpsl) are the stripped members the grouping marked uncertain."

    strings:
        $a1 = "kf{bhjpl`wbkfgqlm"
        $a2 = "wbYyC107:6;6:"
        $a3 = "bgnjmwfof`ln"
        $a4 = "wfomfwbgnjm"
        $a5 = "wpdljmdlm"
        $a6 = "L{kotPD;"
        $a7 = "P1eDrMEp"
        $a8 = "woItsal5"
        $a9 = "rae44232"

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and 3 of them
}

rule Linux_Botnet_Mirai_MvChmodDlr
{
    meta:
        description = "Mirai dropper that relocates itself with 'mv %s /%s/%s && chmod 777' and 'mkdir /%s/ && >/%s/%s && cd /%s/'"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "mvchmod_dlr"
        capabilities = "self_relocation,dropper"
        confidence = "high"
        reference_md5 = "0c2fc1bdc7fb88e48e1db8d0248e2548,1d86140dab72cdfd3c4779a9c8e7d276"
        sample_count = "10 files"
        tested = "recall 10/10, 1 hit in xor_wbyy (same dropper), 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "One xor_wbyy member carries the same two commands; expected overlap."

    strings:
        $a = "mv %s /%s/%s && chmod 777 /%s/%s >/dev/null"
        $b = "mkdir /%s/ && >/%s/%s && cd /%s/ >/dev/null"

    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and all of them
}

rule Linux_Botnet_Mirai_StrippedSha
{
    meta:
        description = "Stripped uClibc Mirai-like build (sha256-named) found by byte windows of its hash routine: MIPS-BE block, ARM SHA-224 IV, x86 length-padding head"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "mirai"
        variant = "stripped_sha256_set"
        capabilities = "unknown"
        confidence = "low"
        reference_md5 = "5630ee34393ce22d317c3a11a91b5bb2,a5af70c231f652effcf5e1235b7891fd"
        sample_count = "4 ELFs (plus 4 .zip wrappers not matched)"
        tested = "recall 4/4 ELFs, 0 hits in siblings, 0 unrelated (2026-10-28)"
        arch_note = "per-arch byte windows for MIPS-BE, ARM, x86"
        note = "ARM and x86 windows come from one sample each so are overfitted; MIPS-BE block is stable across 2 builds. The program has no strings at all. .zip wrappers are containers and not matched."

    strings:
        $b_mipsbe = { 03 99 e0 21 27 bd ff d0 af bf 00 28 af b1 00 24 af b0 00 20 af bc 00 10 8c 83 00 00 8c 82 00 04 00 80 88 21 00 02 10 c0 00 03 27 42 00 82 20 25 30 6c 00 3f 00 03 18 c0 00 04 16 02 00 04 34 02 00 04 3a 02 }
        $b_arm = { d8 9e 05 c1 07 d5 7c 36 17 dd 70 30 39 59 0e f7 }
        $b_x86 = { 56 53 83 ec 24 8b 5c 24 30 8b 74 24 34 8b 0b 8b 43 04 89 ca c1 e0 03 c1 ea 1d 09 c2 89 d0 c1 e8 18 88 44 24 1c 89 d0 c1 e8 10 88 44 24 1d }

    condition:
        uint32(0) == 0x464c457f and filesize < 400KB and any of ($b_*)
}

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

rule Linux_Botnet_BotTx_HttpToken
{
    meta:
        description = "MIPS bot with a bot-auth/bot-tx/bot-rx HTTP token channel (getEndpoints, token/sid/nonce/seq form) and eth_call JSON-RPC lookups"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "bottx"
        capabilities = "http_c2,blockchain_endpoint_lookup,process_blocklist"
        confidence = "low"
        reference_md5 = "77c33bd78efb9858245d8d7731114eda,c3550a56574d34ed9f4c21b4c789a3e2"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 hits in 413 other files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "two MIPS builds (BE, LE) only; family identity unknown"
    strings:
        $a1 = "bot-auth-v1" ascii
        $a2 = "bot-tx-v1" ascii
        $a3 = "bot-rx-v1" ascii
        $b1 = "token=%s&sid=%s&nonce=%u&seq=%u" ascii
        $b2 = "method=getEndpoints&token=" ascii
        $b3 = "method=getConfiguration" ascii
        $b4 = "token=%s&guid=%s" ascii
        $c1 = "reason=var_run" ascii
        $c2 = "reason=blocklist" ascii
        $c3 = "eth_call" ascii
        $c4 = "CitizenFX/1" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 2 of ($a*) and 2 of ($b*) and 1 of ($c*)
}

rule Linux_Botnet_StunProbe_Cloudflare
{
    meta:
        description = "MIPS static binary that discovers its public address through stun.cloudflare.com / 74.125.250.129 and walks /proc/net/unix"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "stun_probe"
        capabilities = "stun_public_ip_discovery,socket_inventory"
        confidence = "low"
        reference_md5 = "284c5f8e950d870ad778e0f89cf37344,8a708e6ed102d24a8b8d7951e28937e7"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 hits in 413 other files (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "two MIPS builds; one also carries relay.mtdblock.st"
    strings:
        $s1 = "stun.cloudflare.com" ascii
        $s2 = "74.125.250.129" ascii
        $s3 = "/proc/net/unix" ascii
        $s4 = "relay.mtdblock.st" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 1MB and $s1 and ($s2 or $s4) and $s3
}

rule Linux_Botnet_DhtBep44_Peer
{
    meta:
        description = "ARM bot speaking bencoded BitTorrent DHT with BEP44 signed mutable put/get (seq/sig/k/v) and ChaCha constants"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "dht_bep44_bot"
        capabilities = "dht_peer,signed_config_distribution,p2p_c2"
        confidence = "low"
        reference_md5 = "3c89839c2fad66f657db73f71ce534c8,b681c2c52a71aa760fa1327b0d1ef834"
        sample_count = "4"
        tested = "recall 4/4 on group, 0 hits in 411 other files (2026-10-28)"
        arch_note = "arch-independent (strings); samples are ARM only"
        note = "generic DHT clients share bencode verbs; the rule needs the BEP44 seq/sig forms plus the base32 alphabet and the expand 32-byte k constant. Not verified as Mozi or Hajime"
    strings:
        $d1 = "e1:q9:get_peers1:t2:" ascii
        $d2 = "3:seqi%llde3:sig64:" ascii
        $d3 = "3:seqi%llde1:v%d:" ascii
        $d4 = "e1:q3:put1:t2:" ascii
        $d5 = "5:token%d:" ascii
        $d6 = "4:k32:" ascii
        $z32 = "ybndrfg8ejkmcpqxot1uwisza345h769" ascii
        $e1 = "expand 32-byte k" ascii
        $e2 = "|%s|%s|" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and $z32 and 1 of ($e*) and 2 of ($d2, $d3, $d6) and 3 of ($d*)
}

rule Linux_Botnet_StaticBot_BotStarted
{
    meta:
        description = "Static-glibc bot that prints BOT STARTED, reads CNC_HOST/CNC_PORT, drops to /tmp/.bot and persists via cron, rc.common, inittab respawn and a Network Manager systemd unit"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "static_bot_started"
        capabilities = "ssh_bruteforce,router_cmd_injection,persistence,http_proxy_connect"
        confidence = "medium"
        reference_md5 = "42a7f1075db95ad189001a7ad8edf971,6e8342657aa1b45918cf886eaec7250f"
        sample_count = "3 (2 coherent; 6e05a46adcfdb572705b6df690aad0b0 is a different binary, see Linux_Botnet_StaticBot_UpdaterDesktop)"
        tested = "recall 2/2 on coherent members, 0 hits in zbot_static and static_sk_bot siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "no packed originals; the MIPS-LE member of the original group does not carry these strings"
    strings:
        $a1 = "BOT STARTED\n" ascii
        $a2 = "CNC_HOST" ascii
        $a3 = "CNC_PORT" ascii
        $a4 = " -O /tmp/.bot || " ascii
        $b1 = "Description=Network Manager\n" ascii
        $b2 = "\nRestart=always\nRestartSec=30\n" ascii
        $b3 = "ping_addr=127.0.0.1%0a" ascii
        $b4 = "/etc/rc2.d/" ascii
        $b5 = "systemctl enable " ascii
        $b6 = "::respawn:" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 3 of ($a*) and 2 of ($b*)
}

rule Linux_Botnet_StaticBot_UpdaterDesktop
{
    meta:
        description = "Static-glibc MIPS bot with updater.desktop and .profile autostart, oom_score_adj -1000, a pong reply and psize/srcport/gport option parser"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "static_bot_started"
        variant = "updater_desktop"
        capabilities = "autostart_persistence,oom_protection,udp_flood_options"
        confidence = "low"
        reference_md5 = "6e05a46adcfdb572705b6df690aad0b0"
        sample_count = "1"
        tested = "recall 1/1, 0 hits in siblings, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "single sample split off the static_bot_started group because it shares only glibc with it"
    strings:
        $a1 = "updater.desktop" ascii
        $a2 = "X-GNOME-Autostart-enabled=true" ascii
        $a3 = "\n(nohup %s >/dev/null 2>&1 &)\n" ascii
        $a4 = "/proc/self/oom_score_adj" ascii
        $b1 = "pong %s" ascii
        $b2 = "psize=" ascii
        $b3 = "srcport=" ascii
        $b4 = "gport=" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and 2 of ($a*) and 2 of ($b*)
}

rule Linux_Botnet_Zbot_StaticDropper
{
    meta:
        description = "Static-glibc multi-exploit bot that pulls bot.$(uname -m) into /tmp/.z, stages /tmp/.st /tmp/.se /tmp/.sk and keeps a */5 crontab, identified by its router-exploit payload templates"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "zbot"
        variant = "static_dropper"
        capabilities = "router_exploit_scanner,cmd_injection,upnp_soap,cron_persistence,suid_hunt,kernel_module_load"
        confidence = "high"
        reference_md5 = "064cfbdb66eea314aa505887b25f7530,20799844dd0d22718fea50e9dd97de81"
        sample_count = "11"
        tested = "recall 10/10 scannable members (11th is a zip), 0 hits in static_sk_bot and static_bot_started, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (strings)"
        note = "the member without ELF is a zip container carrying the same ELF; static_sk_bot shares the /tmp/.sk /tmp/.se /tmp/.z paths but none of these templates"
    strings:
        $x1 = "cp /proc/self/exe %s 2>/dev/null && chmod +x %s 2>/dev/null" ascii
        $x2 = "[od_realtek] udp 9034 exec sent at %s:%d (resp=%d)" ascii
        $x3 = "find / -perm -4000 -type f -exec %s" ascii
        $x4 = "GET /device.rsp?opt=user&list=" ascii
        $x5 = "GET /ftptest.cgi?loginuse=%s&loginpas=%s HTTP/1.0" ascii
        $x6 = "todo=connectivity_test&ip_addr=" ascii
        $x7 = "StatusAndSupportPingIPAddr=%s&StatusAndSupportRunPing=1" ascii
        $w1 = "/bot.$(uname -m) -O /tmp/.z" ascii
        $w2 = "/tmp/.se" ascii
        $w3 = "/tmp/.sk" ascii
        $w4 = "POST /upnp/control/WANIPConn1 HTTP/1.0" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 8MB and 3 of ($x*) and 2 of ($w*)
}

rule Linux_Botnet_Zbot_Rc4Stub
{
    meta:
        description = "Static-glibc RC4 stub loader of the zbot ecosystem: decrypts /tmp/.se with the hex key in /tmp/.sk into /tmp/.z, deletes both and execs it; carries none of the zbot exploit templates"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "zbot"
        variant = "rc4_stub"
        capabilities = "payload_decrypt,self_delete,exec_dropped_payload"
        confidence = "medium"
        reference_md5 = "1e90d958fd6ccef710ad17552fac894a,652dec668945accfd4e50cee75e8ad9d"
        sample_count = "2"
        tested = "recall 2/2 on group, 0 hits in zbot_static and elsewhere (the three paths alone hit 10 zbot files, removed by the zbot negation) (2026-10-28)"
        arch_note = "per-arch byte windows for AARCH64 (RC4 KSA/PRGA loop) and ARM (RC4 PRGA)"
        note = "this is the stub fetched as stub.$m into /tmp/.st by the zbot_static exploit templates (bot.$m.se / bot.$m.sk are its payload and key); the three paths alone also hit zbot_static, hence the negation on the zbot markers. Other architectures are not sampled"
    strings:
        $s1 = "/tmp/.sk" ascii
        $s2 = "/tmp/.se" ascii
        $s3 = "/tmp/.z" ascii
        $z1 = "cp /proc/self/exe %s 2>/dev/null" ascii
        $z2 = "POST /upnp/control/WANIPConn1" ascii
        $b_a64 = { 40 08 d8 9a 41 68 64 38 23 00 03 0b 00 88 18 9b 20 6b 60 38 03 00 03 0b 63 1c 00 12 80 c8 63 38 40 68 24 38 }
        $b_arm = { ff 30 03 e2 03 20 d4 e7 04 31 84 e5 0c c0 82 e0 ff c0 0c e2 0c e0 d4 e7 06 10 a0 e1 03 e0 c4 e7 0c 20 c4 e7 }
    condition:
        uint32(0) == 0x464c457f and filesize < 3MB and all of ($s*) and not any of ($z*) and any of ($b_*)
}

rule Linux_Botnet_GoMethods_Family
{
    meta:
        description = "Go DDoS-bot codebase with a Methods package of attack functions (Priv7Flood, StartGREIP, StartDiscord...) and a Bot/bot.go entry point, matched on pclntab package and function names"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "go_methods_bot"
        capabilities = "udp_flood,tcp_flood,pps_flood,gre_flood,dns_flood,discord_flood,tcp_c2_line_protocol"
        confidence = "medium"
        reference_md5 = "c57c374c39a3af9ece667639c2d3927e,09681f16bcacf77b409dfd5cf177fafa,3692a2b0bab1bf3628ac024af4ff08f1"
        sample_count = "10 (go_bot_a 8, go_bot_b 2)"
        tested = "recall 10/10 on go_bot_a+go_bot_b, 0 hits in 405 other files, 0 in host Go binaries gh, go, docker (2026-10-28)"
        arch_note = "arch-independent (pclntab/symbol strings survive stripping)"
        note = "module name differs per build (vortax_server, Botnet, catstorm); the Methods package, the bot.go file and the flood names are the stable part. Checked absent from host Go binaries gh (go1.22), go (go1.22) and docker (go1.26); no benign Go sample is in the collection itself (benign_test is C)"
    strings:
        $m1 = "/Methods." ascii
        $b1 = "/bot.go" ascii
        $f1 = "Priv7Flood" ascii
        $f2 = "StartGREIP" ascii
        $f3 = "StartDiscord" ascii
        $f4 = "StartPPS" ascii
        $f5 = "FortAttack" ascii
        $f6 = "MoneroMiner" ascii
        $f7 = "StartDNS" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 20MB and $m1 and $b1 and 1 of ($f*)
}

rule Linux_Botnet_GoMethods_Vortax
{
    meta:
        description = "Go DDoS bot built from module vortax_server: dials a hard-coded C2:9111, sends REGISTER <os> <arch>, then dispatches dns/udp/tcp/pps/priv7/greip/discord attack commands"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "go_methods_bot"
        variant = "vortax"
        capabilities = "udp_flood,tcp_flood,pps_flood,gre_flood,dns_flood,discord_flood,c2_register,line_command_protocol"
        confidence = "high"
        reference_md5 = "c57c374c39a3af9ece667639c2d3927e,33acdb3da37be3b4a222e1410671dc89,4f80418188150bd81d5033a0876e3d8e"
        sample_count = "8"
        tested = "recall 8/8 on go_bot_a, 0 hits in go_bot_b, 0 unrelated (2026-10-28)"
        arch_note = "arch-independent (x86-64, ARM, MIPS BE/LE seen)"
        note = "build paths seen: /root/wichtig/bot.go (6), /root/ztrz/bot.go, /root/databsased/bot.go; C2 5.175.221.69:9111 present in 6 of 8 builds. Does not hit the two go_bot_b builds (other module names)"
    strings:
        $p1 = "vortax_server/Methods" ascii
        $p2 = "vortax_server" ascii
        $s1 = "StartDiscord" ascii
        $s2 = "StartGREIP" ascii
        $s3 = "Priv7Flood" ascii
        $s4 = "StartPPS" ascii
        $s5 = "StartDNS" ascii
        $c1 = "REGISTER %s %s" ascii
        $c2 = "5.175.221.69:9111" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 20MB and $p1 and $p2 and 2 of ($s*) and 1 of ($c*)
}

rule Linux_Botnet_GoMethods_Catstorm
{
    meta:
        description = "Go bot of the Methods/Bot codebase built as module catstorm that adds a MoneroMiner method with xmrig/stratum strings"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "go_methods_bot"
        variant = "catstorm"
        capabilities = "cryptominer,ddos_methods"
        confidence = "low"
        reference_md5 = "3692a2b0bab1bf3628ac024af4ff08f1"
        sample_count = "1"
        tested = "recall 1/1, 0 elsewhere (2026-10-28)"
        arch_note = "arch-independent (ARM sample)"
        note = "single sample; pclntab names for this build are unrecovered (FUN_ names) so the match is raw-byte only"
    strings:
        $a1 = "catstorm/Bot/bot.go" ascii
        $a2 = "Bot/Methods.MoneroMiner" ascii
        $a4 = "stratum" ascii nocase
    condition:
        uint32(0) == 0x464c457f and filesize < 20MB and 1 of ($a1, $a2) and $a4
}

rule Linux_Botnet_GoMethods_Botnet
{
    meta:
        description = "Go bot of the Methods/Bot codebase built as module Botnet, with FortAttack and Priv7Flood methods and a /tmp/lgerr error log"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "variant"
        family = "go_methods_bot"
        variant = "botnet_fortattack"
        capabilities = "ddos_methods,game_server_flood"
        confidence = "low"
        reference_md5 = "09681f16bcacf77b409dfd5cf177fafa"
        sample_count = "1"
        tested = "recall 1/1, 0 elsewhere (2026-10-28)"
        arch_note = "arch-independent (x86-32 sample)"
        note = "single sample; pclntab names for this build are unrecovered (FUN_ names) so the match is raw-byte only"
    strings:
        $a1 = "Botnet/Bot/bot.go" ascii
        $a2 = "Botnet/Methods.FortAttack" ascii
        $a3 = "Botnet/Methods.Priv7Flood" ascii
        $a4 = "/tmp/lgerr" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 20MB and 1 of ($a1, $a2, $a3) and $a4
}

rule Linux_Miner_Redtail_Xmrig
{
    meta:
        description = "Statically linked XMRig-based CPU miner built under /var/build/redtail (hwloc 2.14, libuv, snappy, libevent TLS), identified by the libredtail marker, build paths and XMRig config text"
        author = "BSimVis YARA workbench (Claude), human review pending"
        date = "2026-10-28"
        kind = "family"
        family = "miner"
        variant = "redtail"
        capability = "cryptomining"
        capabilities = "xmrig_stratum_miner,hugepages_tuning,ssl_pool_connection"
        confidence = "high"
        reference_md5 = "0ab5c6d452d9d8581923181a6e4de3a3,1a99ba4d3c42a0c75bcdc390fae345cc"
        sample_count = "16 (8 unpacked, 8 UPX originals)"
        tested = "recall 8/8 unpacked (8 UPX originals not covered by design), 0 hits in 399 other files (2026-10-28)"
        arch_note = "arch-independent (strings): x86-32, x86-64, ARM, AARCH64"
        note = "stock XMRig strings alone also match plain XMRig builds, hence the libredtail / build-path anchor; UPX originals carry no strings and are not covered (packer concern)"
    strings:
        $r1 = "libredtail evbuffer_tls" ascii
        $r2 = "/var/build/redtail/scripts/" ascii
        $x1 = "no valid configuration found, try https://xmrig.com/wizard" ascii
        $x2 = "stratum+ssl://%s" ascii
        $x3 = "COMMANDS     'h' hashrate, 'p' pause, 'r' resume, 's' results, 'c' connection" ascii
        $x4 = "donate.v2.xmrig.com" ascii
        $h1 = "hwloc_bitmap_isincluded(obj->nodeset, obj->complete_nodeset)" ascii
    condition:
        uint32(0) == 0x464c457f and filesize < 40MB and 1 of ($r*) and 2 of ($x*) and $h1
}

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
