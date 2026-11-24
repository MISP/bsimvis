# stun_probe

Rule: `Linux_Botnet_StunProbe_Cloudflare` (kind family, low).

## Coherence check
Two small MIPS builds (BE, LE) with an almost identical string table: `stun.cloudflare.com`, `74.125.250.129` (a Google STUN address), `8.8.8.8`, `/proc/net/unix`, `/proc/<pid>/fd/`. One build also has `relay.mtdblock.st`. They are the same program; nothing else is unique in either (rest is libc: `hlLjztqZ`, `Unknown error `, ...).

## Evidence and purpose
The only meaningful strings are STUN servers and a /proc socket walk (`/proc/net/unix`, fd inspection), so it discovers the public address and enumerates sockets. `relay.mtdblock.st` looks like a relay/C2 name (only in one file; unconfirmed). No attack strings. I did not decompile it; purpose beyond this is unknown.

## Rule and measurement
`stun.cloudflare.com` + (`74.125.250.129` or `relay.mtdblock.st`) + `/proc/net/unix`. 2/2; 0 hits in the 413 other files.

## Gaps
Only strings and libc; an arch/byte-level anchor is not available (the MIPS functions are tiny and the strings are the only distinguishing data). Identification as malware rests on the family assignment of the collection, not on a hostile capability seen in the strings.
