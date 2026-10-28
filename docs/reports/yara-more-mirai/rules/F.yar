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
