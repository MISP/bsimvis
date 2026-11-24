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
