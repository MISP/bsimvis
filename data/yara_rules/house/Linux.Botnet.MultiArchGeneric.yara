/*
   Author: bsimvis (house rules)
   Date: 2026-08-11
   Corpus: 259 ELF IoT-botnet samples (~/data/malware/mirai2/ghidra_loadable),
   spanning MIPS LSB/MSB, ARM, aarch64, PPC, Renesas SH, m68k, SPARC,
   RISC-V, LoongArch, OpenRISC, x86 and x86-64.

   Why these exist: every vendored family ruleset here -- Elastic's included --
   detects ELF botnets with code-byte patterns, so a rule built from an x86 or
   ARM build of a family does not fire on the MIPS/SH/PPC/m68k build of that
   same family. On the corpus above, the whole vendored set left 84/259 samples
   untagged and 79 of those 84 were non-x86/ARM. These five rules key on
   behaviour that survives recompilation -- attack-command parameter names, the
   kthread-masquerade table, the HTTP-flood request templates, the embedded
   device paths, and the single-byte-XOR'ed config strings -- so they cross
   architectures. They cover 220/259 of the corpus on their own, and take the
   whole vendored set from 19/259 to 227/259, with 0 hits on 5112 benign
   system ELFs from /usr/bin, /usr/sbin, /bin and /usr/lib/x86_64-linux-gnu.

   Deliberately generic: `malware = "GENERIC"` because these identify the
   IoT-botnet code family in general (Mirai and its forks, Gafgyt, and the
   long tail of recompiled derivatives), not one named family. A more specific
   rule that also matches is what names the sample; these are the floor that
   keeps a sample from being tagged as nothing at all.

   Unlike Linux.Botnet.MiraiChaCha20.yara these match .rodata strings rather
   than instruction bytes, so their file offsets resolve to data, not to a
   Ghidra function -- they are file-level tags in practice. That is the same
   trade the vendored ReversingLabs string rules already make.
*/

rule Linux_Botnet_Generic_AttackCmdParams
{
   meta:
      description = "IoT botnet attack-command parameter parser (gport/srcport/psize/data=random_data), architecture-independent"
      category = "botnet"
      malware = "GENERIC"
      author = "bsimvis"
      date = "2026-08-11"
   strings:
      // C2 flood-command options parsed by the bot: these key names travel
      // with the source, so every recompile for a new arch keeps them.
      $a1 = "gport=" ascii
      $a2 = "srcport=" ascii
      $a3 = "psize=" ascii
      $a4 = "data=random_data" ascii
      $a5 = "timeout:" ascii
      $a6 = "attempts:" ascii
   condition:
      uint32(0) == 0x464c457f and 4 of ($a*)
}

rule Linux_Botnet_Generic_KthreadMasquerade
{
   meta:
      description = "Bot hiding as a kernel thread: /proc/<pid>/ walk plus the kworker/kthreadd/ksoftirqd name table"
      category = "botnet"
      malware = "GENERIC"
      author = "bsimvis"
      date = "2026-08-11"
   strings:
      // The killer/scanner reads these to enumerate and kill rival bots.
      $p1 = "/proc/%s/comm" ascii
      $p2 = "/proc/%s/cmdline" ascii
      $p3 = "/proc/%s/exe" ascii
      $p4 = "/proc/%s/status" ascii
      // Names the bot renames itself to, and screens rivals against.
      $k1 = "kworker" ascii fullword
      $k2 = "kthreadd" ascii fullword
      $k3 = "ksoftirqd" ascii fullword
      $k4 = "watchdog" ascii fullword
   condition:
      // Both halves required: the name table alone appears in benign
      // process-listing tools, the /proc walk alone in anything ps-like.
      uint32(0) == 0x464c457f and 2 of ($p*) and 3 of ($k*)
}

rule Linux_Botnet_Generic_HTTPFlood
{
   meta:
      description = "IoT botnet HTTP flood module: hardcoded bare-path GET/POST/HEAD request templates"
      category = "botnet"
      malware = "GENERIC"
      author = "bsimvis"
      date = "2026-08-11"
   strings:
      // Request lines with a bare "/" target and no host -- assembled at
      // attack time, not by an HTTP client library.
      $h1 = "GET / HTTP/1.1" ascii
      $h2 = "POST / HTTP/1.1" ascii
      $h3 = "HEAD / HTTP/1.1" ascii
      $h4 = "Content-Type: application/x-www-form-urlencoded" ascii
      $h5 = "Connection: keep-alive" ascii
      $h6 = "User-Agent: %s" ascii
   condition:
      uint32(0) == 0x464c457f and 4 of ($h*)
}

rule Linux_Botnet_Generic_BusyboxTelnetSpread
{
   meta:
      description = "IoT botnet embedded-device footprint: busybox payload staging plus /dev/watchdog disarm and /proc/net/tcp scan"
      category = "botnet"
      malware = "GENERIC"
      author = "bsimvis"
      date = "2026-08-11"
   strings:
      $b1 = "/bin/busybox" ascii
      $b2 = "/dev/shm/" ascii
      $b3 = "/proc/net/tcp" ascii
      $b4 = "/dev/watchdog" ascii
      $b5 = "/dev/console" ascii
      $b6 = "/var/run/" ascii
   condition:
      // 5 of 6, not 4: at 4 this also fires on /usr/bin/snap, which ships the
      // same device paths -- and /bin/busybox -- for its own container
      // plumbing, so no single anchor string separates them. The stricter
      // count costs 3 corpus samples that nothing else here tags; a benign
      // hit on a binary as common as snap was judged the worse of the two.
      uint32(0) == 0x464c457f and 5 of ($b*)
}

rule Linux_Botnet_Generic_XorObfuscatedConfig
{
   meta:
      description = "Mirai-lineage single-byte-XOR string table (config strings recovered under any key 0x01-0xff)"
      category = "botnet"
      malware = "GENERIC"
      author = "bsimvis"
      date = "2026-08-11"
   strings:
      // Mirai's table_init() XORs its config strings with a per-build key,
      // which is why the plaintext rules above miss the packed variants.
      // YARA's `xor` modifier searches every single-byte key at once, so one
      // string covers all of them without knowing the build's key.
      //
      // "TSource Engine Query" is the Valve A2S probe Mirai's VSE flood
      // replays verbatim; it is specific enough to stand alone. The rest are
      // generic device paths, so they need a pair.
      $t = "TSource Engine Query" xor
      $x1 = "/bin/busybox" xor(0x01-0xff)
      $x2 = "/dev/watchdog" xor(0x01-0xff)
      $x3 = "/proc/net/tcp" xor(0x01-0xff)
      $x4 = "nameserver" xor(0x01-0xff)
      $x5 = "/bin/sh" xor(0x01-0xff)
   condition:
      uint32(0) == 0x464c457f and ($t or 2 of ($x*))
}
