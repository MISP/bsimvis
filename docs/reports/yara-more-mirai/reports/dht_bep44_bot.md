# dht_bep44_bot

Rule: `Linux_Botnet_DhtBep44_Peer` (kind family, low).

## Coherence check
Coherent: four ARM builds (`armv4l..armv7l.bin`) share every marker at 4/4 and 0 hits in the other files.

## Evidence
- Bencoded KRPC verbs: `e1:q4:ping1:t2:`, `e1:q9:get_peers1:t2:`, `e1:q3:get1:t2:`, `e1:q3:put1:t2:`, `d1:ad2:id20:`, `9:info_hash20:`, `6:target20:`, `5:token%d:`.
- BEP44 mutable item fields: `3:seqi%llde3:sig64:`, `3:seqi%llde1:v%d:`, `4:k32:`, i.e. signed (ed25519, 32-byte key / 64-byte signature) puts and gets.
- `ybndrfg8ejkmcpqxot1uwisza345h769` is the z-base-32 alphabet; `expand 32-byte k` is the ChaCha/Salsa constant; `|%s|%s|` is a separator format. `/proc/self/exe` and `/proc/self/fd`.
- An RPC/XDR section from the static libc (`xdr_*`, `clntudp_create`) is just uClibc; not used as an anchor.

Purpose: a P2P node that distributes signed values over the BitTorrent DHT (config or peer lists). Design resembles Hajime, but I have not compared with Hajime samples; family identity is unverified.

## Rule and measurement
z-base-32 alphabet + (`expand 32-byte k` or `|%s|%s|`) + 2 of the BEP44 `seq/sig/k32` forms + 3 of the 6 bencode strings. 4/4, 0 hits in the 411 other files.

## Gaps
ARM only. Strings are arch-independent, so other arch builds should match. No signing key or bootstrap nodes extracted.
