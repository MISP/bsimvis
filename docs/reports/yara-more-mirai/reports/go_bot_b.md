# go_bot_b: two forks of the Go "Methods" bot

Rules: `Linux_Botnet_GoMethods_Catstorm` (variant, low), `Linux_Botnet_GoMethods_Botnet` (variant, low), plus the shared `Linux_Botnet_GoMethods_Family` (see go_bot_a.md).

## Coherence check
The label "Go binary with crypto/tls, no malicious indicators" was wrong: both files are the same codebase as go_bot_a.

| file | arch | module / source path | extra markers |
|---|---|---|---|
| 09681f16 | x86-32, 2800 funcs | `Botnet/Bot/bot.go`, `Botnet/Methods.FortAttack`, `Botnet/Methods.Priv7Flood` | `/tmp/lgerr` error log, "sysd" |
| 3692a2b0 | ARM, 3499 funcs | `catstorm/Bot/bot.go`, `Bot/Methods.MoneroMiner` | `xmrig`, `stratum` strings |

Pclntab names are not recovered in Ghidra (all `FUN_`), so evidence comes from raw bytes via the YARA match offsets. Mutual code BSim is 0.07; versus go_bot_a 0.04, library part 0.4-0.5. They share the Methods/Bot package layout and the `Priv7Flood` method with go_bot_a, but are different builds; they are not one program with each other.

## Rules
Both variant rules require the module-specific source path or method plus a second marker (`/tmp/lgerr` for Botnet, `stratum` for Catstorm). Measured: 1/1 each, 0 hits on the other 9 Go files and 0 in the other 400+ files. Host Go binaries (gh, go, docker) do not match.

## Gaps
Single samples; no C2 or command grammar recovered because the names are missing. The family rule is the better detector for new forks.
