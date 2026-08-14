# YARA coverage on the multi-architecture botnet corpus

Measured 2026-08-11 against 259 ELF IoT-botnet samples in
`~/data/malware/mirai2/ghidra_loadable`, scanned with `yara-python` 4.5.4 using
the same recursive load `bsimvis.app.services.yara_service` performs.

A sample counts as *tagged* when at least one rule fires that names a family or
behaviour. File-format and packer rules (`SUSP_*`, `elf_*`, `ELF_anomal*`) are
excluded from the count -- they fire on benign ELFs too and say nothing about
what the sample is.

## Result

| Ruleset | Tagged | Share |
|---|---|---|
| Before (ReversingLabs + signature-base + house) | 19/259 | 7.3% |
| \+ `elastic/` (273 Linux/Multi rules) | 142/259 | 54.8% |
| \+ `house/Linux.Botnet.MultiArchGeneric.yara` | **227/259** | **87.6%** |

False positives on 5112 benign system ELFs (`/usr/bin`, `/usr/sbin`, `/bin`,
`/usr/lib/x86_64-linux-gnu`): **0**. Compiling all 588 rule files takes 0.58s,
so `yara_service`'s one-compile-per-worker cache still holds.

## Why the vendored rules alone cap out at 55%

The 84 samples Elastic misses break down by architecture as:

    28  MIPS 32 LSB        6  PowerPC          2  RISC-V 32
    15  MIPS 32 MSB        4  aarch64          2  SPARC
     7  Renesas SH         4  m68k             2  OpenRISC
     6  ARM 32             4  x86-64           2  LoongArch

79 of 84 are neither x86 nor ARM. Elastic's ELF rules are byte sequences
lifted from compiled code and tagged `arch_context = "x86"`; the same family
recompiled for MIPS shares no code bytes with them. This is a property of how
public ELF rules are written, not a gap in Elastic's specifically -- rules from
Yara-Rules/rules, ditekshen/detection and ESET/malware-ioc were each measured
against these 84 and added **zero** detections.

The fix is rules keyed on what survives recompilation, which is what
`Linux.Botnet.MultiArchGeneric.yara` does: attack-command parameter names, the
kthread-masquerade name table, HTTP-flood request templates, embedded-device
paths, and -- the single biggest one, 188 samples -- Mirai's XOR'ed config
string table, reachable across every single-byte key at once via YARA's `xor`
string modifier.

## The 32 still untagged

Mostly MIPS, and string-poor beyond a libc `strerror` table: their config
strings are neither plaintext nor single-byte-XOR'ed. Closing these needs
either per-family code patterns built from MIPS/SH/PPC builds (the corpus has
them; BSim function similarity is the cheaper way to find which samples share
code), or multi-byte-key config decryption, which YARA cannot express.

## Reproducing

    python scripts/yara_coverage.py ~/data/malware/mirai2/ghidra_loadable \
        --fp /usr/bin --fp /usr/sbin

`--rules DIR` points it at an alternative ruleset, which is how the per-row
numbers in the table above were taken.
