# static_sk_bot

Rule: `Linux_Botnet_Zbot_Rc4Stub` (kind variant of zbot, variant rc4_stub, medium).

## Coherence check
The group looked like two bots with only temp paths. The earlier claim "related to zbot only at the glibc level" is wrong at the string level: the paths `/tmp/.sk`, `/tmp/.se`, `/tmp/.z` are the same ones zbot_static writes (10 of the 11 zbot files contain all three; my first path-only rule hit all 10, P=0.17). The explanation is in zbot's exploit templates: they download `stub.$m` to `/tmp/.st`, `bot.$m.se` to `/tmp/.se` and `bot.$m.sk` to `/tmp/.sk`, then run `/tmp/.st`. The two sk files are that stub.

## What the stub does (decompiled `main`, aarch64 and ARM)
`main(-t)` returns 0 (self-test). Otherwise it reads up to 0x80 hex chars from `/tmp/.sk` as an RC4 key, runs the RC4 KSA/PRGA, decrypts `/tmp/.se` (optionally skipping a 4-byte "SH" header) into `/tmp/.z`, unlinks `.se`, `.sk` and itself, `chmod 755 /tmp/.z` and `execl("/tmp/.z", ...)`, passing up to three argv values on. About 960-990 functions, nearly all static glibc. Payload encryption keeps the real bot out of the network capture.

## Rule and measurement
Three paths + negation of two zbot exploit strings + per-arch byte windows (aarch64 RC4 loop, ARM RC4 PRGA). Recall 2/2, 0 hits in zbot_static (13 siblings scanned), 0 in the other ~400 files. Each byte window is from one sample per arch; other arches (MIPS, x86) of the stub are not in the set.

## Gaps
Windows are single-sample. If the stub is rebuilt with another compiler, the three paths plus the missing zbot strings still match, but the byte anchor will not.
