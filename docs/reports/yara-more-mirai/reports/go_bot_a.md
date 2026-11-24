# go_bot_a (and go_bot_b): Go DDoS bot, "Methods" codebase

Rules: `Linux_Botnet_GoMethods_Family` (kind family, all 10 Go files), `Linux_Botnet_GoMethods_Vortax` (variant, go_bot_a).

## Coherence check
The group label said "purpose unknown". It is one codebase. Ghidra recovered pclntab names for all eight files, and the x86-64 build (c57c374c) decompiles cleanly:

- `main.main` dials a hard-coded `5.175.221.69:9111` in a loop (5 s retry), sends `REGISTER %s %s\n` (GOOS, GOARCH), then reads lines and dispatches `dns`, `udp`, `tcp`, `pps`, `priv7`, `greip`, `discord` (plus `ping` -> `pong\n`, `exit`) into goroutines `main.main.gowrap1..7`.
- Each goroutine calls into package `vortax_server/Methods`: `StartDNS`, `StartUDP`, `StartPPS`, `StartTCP`, `Priv7Flood`, `StartGREIP`, `StartDiscord`. DWARF source path: `/root/wichtig/bot.go`.
- Builds found: `/root/wichtig/bot.go` (6 files), `/root/ztrz/bot.go` (5de612e9), `/root/databsased/bot.go` (ab631273). C2 string present in 6 of 8 (the other two use another address, not recovered).
- The bin_sim "code" score is 0.999 only between the two identical-arch pairs; across arches it is 0.05-0.2 (Go compiler output differs). Library score is high everywhere. So the eight files are one program in 3-4 arch builds, not eight programs.

`go_bot_b` (2 files) is the same codebase under other module names: `Botnet/Bot/bot.go` with `Botnet/Methods.FortAttack` and `Priv7Flood` (09681f16, x86-32, also `/tmp/lgerr`), and `catstorm/Bot/bot.go` with `Methods.MoneroMiner`, xmrig and stratum strings (3692a2b0, ARM). Pclntab names were not recovered for them (`FUN_`), the match is raw bytes. Their BSim code score against go_bot_a is 0.04-0.07, so they are forks/rebuilds with different code, sharing only the package layout. They get their own low-confidence variant rules (see go_bot_b.md).

## Why the Ghidra string table is useless here
Go stores string literals as one length-sorted blob; the extractor sees fragments like `...STREETdiscord(PANIC=...`. "discord" and "info_hash" in that blob are Go-runtime/dependency neighbours, not meaningful. Only raw-byte matching of pclntab names and DWARF paths works, so the rules use those.

## Benign Go check
No Go program exists in `benign_test` (it is curl and small C programs). I scanned the host Go binaries `/usr/bin/gh` and `/usr/bin/go` (go1.22.2) and `/usr/bin/docker` (go1.26.10): none of the rule strings (`/Methods.`, `/bot.go`, `Priv7Flood`, `StartGREIP`, `vortax_server`, ...) hit. Generic markers (`main.main`, `go1.`, `9111`, `info_hash`, `sysd`) do appear in them and are not used as anchors.

## Rules and measured P/R
- Family: `$m1 "/Methods."` + `$b1 "/bot.go"` + one flood name. Hits 10/10 (go_bot_a 8/8, go_bot_b 2/2), 0 in the 366-405 other files, 0 in the host Go binaries.
- Vortax: `vortax_server/Methods` + `vortax_server` + 2 of 5 method names + (`REGISTER %s %s` or the C2 address). 8/8, 0 in go_bot_b, 0 elsewhere.

## Gaps
Other module names of the same codebase will only be caught by the family rule if they keep `Methods`/`bot.go` and a known flood name. Packed Go builds are not covered. The second C2 address is unknown.
