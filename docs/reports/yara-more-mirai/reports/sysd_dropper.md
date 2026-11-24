# sysd_dropper

Verdict: coherent but tiny (2 files, medium). Rule `Linux_Botnet_Mirai_Sysd_Dropper`.

## Evidence
Both files (an MD5-named MIPS-LE binary with 39 strings, and bin.x86_64 with 1026) share and nothing else in the collection has:
`/etc/init.d/sysd`, `/etc/rc2.d/S99sysd`, `cp %s /usr/bin/.sh`, `#!/bin/sh\n/usr/bin/.sh &\n`, `/tmp/.instance_lock`, `/proc/net/tcp6`, the sscanf format `%*d: %64[0-9A-Fa-f]:%*x ... %u`, `socket:[%u]`, `/proc/%d/fd/0`.
The x86_64 file also embeds a toxcore (Tox DHT) stack, `!scan on/off/fast`, `!killapi`, `!spawnapi` and an exploit request to port 3000; those are not in the MIPS file and are not in the rule.

## What it does
Copies itself to `/usr/bin/.sh`, writes an init script and rc2.d link for boot, takes a lock file, and maps TCP6 sockets to process fds via inodes (the socket-inode walk is typically used to find and kill owners of a port).

## Rule
ELF, under 8 MB, 2 of the 4 persistence strings and (lock file or tcp6 format). 2/2 recall, 0 hits in 434 other files.

## Gaps
Two samples cannot show variation. The shared part is a persistence stub, so unrelated malware reusing it would match.
