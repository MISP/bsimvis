# miner_redtail

Rule: `Linux_Miner_Redtail_Xmrig` (kind family, family miner, variant redtail, high).

## Evidence
16 files = 8 unpacked statically linked builds + 8 UPX originals (confirmed with a `UPX!` probe: the 8 files the rule misses are exactly the UPX ones; they have no string table).
The unpacked builds are XMRig: `no valid configuration found, try https://xmrig.com/wizard`, the interactive help line ` * COMMANDS 'h' hashrate, 'p' pause, 'r' resume, 's' results, 'c' connection`, `stratum+ssl://%s`, `stratum+tcp://`, `donate.v2.xmrig.com` (6/8), hugepages sysfs paths. Statically linked libs: hwloc 2.14.0, libuv, snappy 1.2.2, libevent TLS and OpenSSL, plus a `libredtail evbuffer_tls` string and build paths `/var/build/redtail/scripts/{x86_64,i686,...}-build/...`. The `-----BEGIN OPENSSH PRIVATE KEY-----` strings come from the SSH code in the static OpenSSL/libssh part, not from a worm module; no scanner or attack strings in the group.

Capabilities seen: CPU mining over stratum (TLS), hugepage tuning, hwloc NUMA topology. No botnet capability.

## Rule and measurement
Anchors: `libredtail evbuffer_tls` or the build path, two of four XMRig strings, and a hwloc assert string. Recall 8/8 on the unpacked files, 0 hits in the 399 other files. Stock XMRig builds would match the XMRig strings but need the redtail anchor, so the rule is specific to this build chain.

## Gaps
UPX originals are not covered (packer concern, per the guide). Only the x86/ARM/AARCH64 builds seen; strings are arch-independent so other arches of the same build should match.
