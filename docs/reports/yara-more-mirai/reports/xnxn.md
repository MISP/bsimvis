# xnxn

32 files named `xnxnxnxnxnxnxnxn<arch>xnxn` plus md5 siblings: 12 arch labels (aarch64 4, x86 4, x86_64 4, ppc 4, mips-be 4, sh4 4, loongarch 2, m68k 2, riscv 4). 10 are UPX originals without strings; 22 carry strings.

## Evidence

- Attack commands `!udpplain`, `!udpcustom`, `!http`, `!icmp` with key=value parameters `srcport=`, `payload=`, `psize=`, `gport=`, `proto=`. `!udpplain` and `!udpcustom` are in all 22 stringed files and in no other group.
- `/sys/class/dmi/id/product_name` compared with `VMware`: a VM check. Also present in zbot_static (10 files), so it is a cross-group capability, not an xnxn marker.
- `/tmp/.bot_lock` single-instance lock (18/22), `pong %s` (shared with flylegit), `%*s %31s %d %d %511[^\n]` /proc/net parsers.
- Hex blobs: `fd00e82a0a3d86af73deacaa9df16432` (32 chars), a 64-char blob `42480e8f...`, a 96-char blob `b4c4d0f3...` in every build, and a second 96-char blob that differs by build (see below). Purpose not verified; the lengths fit embedded keys or signatures, which is a guess.
- Two builds also carry kthread-name strings (`kworker`, `kthreadd`, `migration`) but these are common elsewhere (30 other files).

## Sub-variants

Same 11 architectures built twice, split cleanly by the architecture-independent second blob:

- KeyA, `bb63937d0e51...`: mips, loongarch64, aarch64, riscv32, riscv64, x86_64, i386, ppc, m68k, sh2, sh4 (11).
- KeyB, `ed74bacffb15...`: same 11 arches (11).

Beyond that blob the strings are identical, so these look like two campaigns or two key rotations of one codebase.

## Rules (rules/B.yar)

| rule | kind | hits in group | siblings (47) | unrelated (336) + unclassified (21) |
|---|---|---|---|---|
| Linux_Botnet_Mirai_Xnxn | family | 22/22 unpacked (22/32) | 0 | 0 |
| Linux_Botnet_Mirai_Xnxn_KeyA | variant | 11 | 0 | 0 |
| Linux_Botnet_Mirai_Xnxn_KeyB | variant | 11 | 0 | 0 |

Siblings tested: flylegit, zbot_static, static_bot_started, mvchmod_dlr, cling_exploit_scanner, sysd_dropper. Lint is clean after dropping the 5-char `!http` and `!icmp`.

## Cross-group capabilities (not written)

- VMware/DMI check: xnxn + zbot_static.
- `pong %s` + key=value command parser: xnxn + flylegit (+1 static_bot_started), suggests shared lineage with flylegit.
- `srcport=` also appears in one static_bot_started file.

## Gaps

- 10 UPX originals not covered.
- The key-blob meaning is unverified.
- The kthread masquerade tags that earlier YARA tagging reported were not turned into a rule; the strings are too common.
