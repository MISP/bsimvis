# stripped_sha256_set

8 files = 4 ELFs (ARM 228 funcs, MIPS-BE 228 and 200, x86 200) and 4 `.zip` wrappers. No strings at all, so a strings rule is impossible. Shared yara hits in the set: Trojan_Linux_Mirai_Y_MTB, ELF_Toriilike_persist.

## Evidence
Function-cluster join over the 4 ELFs: most shared clusters are uClibc (sigaddset, malloc, sbrk, heap). Three clusters are not library code: 1073885865 (6-7 KB) and 1073885920 (11-17 KB), in ARM and both MIPS builds, and 1073888089 (419-648 B, MIPS pair plus x86). Their bodies look like SHA-224/256 style code (ARM holds the SHA-224 initial values, the x86 head is a bit-length padding sequence). The MIPS-BE pair share a 119-byte stable window at the start of the 641-byte function.

## Rule
`Linux_Botnet_Mirai_StrippedSha` (variant, low): any of three byte windows. 4/4 ELFs, 0 hits in the other 411 scanned files.

## Gaps
The ARM (SHA-224 IV, 16 bytes) and x86 (head) windows are single-sample seeds and may overfit; a new build may only match through the MIPS window. SHA-224 IVs are generic crypto, so precision leans on the other windows and the filesize bound. The zip wrappers are not matched (would need a zip rule). Mirai attribution rests on the AV and yara hits, not on code seen here.
