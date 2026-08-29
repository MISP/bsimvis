# Version diff — `7b70aceca8…` vs `botnet` (`5d2c1d74…`)

Deep dive on the pair behind
`/collections/mirai7/files/7b70aceca81d038bcf859ea5a28f9fd9/vs/mirai7/5d2c1d7436d3a989a6c3c580fb547525`.
Companion to [`mirai7_family_report.md`](mirai7_family_report.md) §7 — both
samples belong to the **Kaiten/STD lineage**, not to Mirai.

| | A | B |
|---|---|---|
| MD5 | `7b70aceca81d038bcf859ea5a28f9fd9` | `5d2c1d7436d3a989a6c3c580fb547525` |
| Name | *(hash-named)* | `botnet` |
| Arch | MIPS:LE:32 | **SuperH4:LE:32** |
| Functions | 213 | 628 |
| BSim features | 13 386 | 31 395 |
| First seen | **2025-04-04** | **2026-02-09** |
| AV | `Unix.Dropper.Mirai-7138865-0` | `Unix.Trojan.Tsunami-6981155-0` |
| Reported score | **0.107** — 96 matched, 117 unique to A, 532 unique to B | |

Verdict up front: **same code base, ~10 months apart, B is a strict superset of
A's malware functionality.** The 0.107 score is close to meaningless here — it is
mostly an architecture and libc-volume artefact. The evidence is below.

---

## 1. What the 10 % is actually made of

> **Pivot** — `diff?md5_a=&md5_b=&table=matched|unique_to_a|unique_to_b&limit=0`,
> then bucket every row's name (from `functions_metadata`) into malware vs
> library vocabulary. Three calls; everything else here follows from them.

Of the 96 matched functions, only **15 are malware code**. The rest is uClibc.
Of the 532 functions "unique to B", only **32** are malware code — 500 are
library. So the headline counts describe two different libc builds far more than
they describe two different bots.

Worse, **86 function names appear on *both* unique lists** — same name, same
role, but no match recorded. Splitting those by feature count:

* **24 have feature counts within 10 % of each other** (`csum` 54/54,
  `rand_cmwc` 32/32, `getRandomIP` 10/10, `SendUDP` 182/185, `vseattack`
  248/249, `atcp` 298/280 …). These are the *same source function*; the match
  failed because A is MIPS:LE and B is SuperH4, which is the cross-ISA ceiling
  documented in §5 of the family report.
* The remainder are mostly libc syscall stubs whose feature counts differ because
  of how each libc build inlines them (`kill` 11 → 3, `socket` 11 → 5, `fcntl`
  18 → 71). Noise.

**A better relatedness metric for this pair** — Jaccard over malware-authored
symbol names rather than over BSim clusters:

| | A | B | Shared |
|---|---|---|---|
| Malware-named functions | 28 | 47 | **28** |

**Jaccard = 0.60, and A's malware function set is a strict subset of B's
(A-only = ∅).** That is the number that describes this pair; 0.107 is not.

## 2. The shared core — what did not change

> **Pivot** — the 15 malware rows in `table=matched`, sorted by `avg_features`.

| Function | Features | Similarity |
|---|---|---|
| `recvLine` | 105 | 0.99 |
| `getOurIP` | 84 | 0.96 |
| `connectTimeout` | 77 | 0.93 |
| `initConnection` | 46 | **1.00** |
| `stdhexflood` | 42 | 0.97 |
| `astd` | 42 | 0.97 |
| `SendSTDHEX` / `SendSTD_HEX` | 42 | 0.96 |
| `SendSTD` | 40 | 0.97 |
| `makevsepacket` | 32 | 0.96 |
| `makeIPPacket` | 31 | 0.96 |
| `getPortz` | 28 | **1.00** |
| `tcpcsum` | 20 | 0.97 |
| `getHost` | 7 | **1.00** |
| `getArch` | 2 | **1.00** |

The whole C2 transport layer (`initConnection`, `recvLine`, `connectTimeout`,
`getOurIP`, `getHost`, `getArch`, `getPortz`) and the original STD flood
(`SendSTD`, `SendSTDHEX`, `stdhexflood`, `astd`) are **untouched across ten
months and two architectures** — several at similarity 1.00. This is the family
backbone, and it is why both samples cluster as Kaiten/STD.

## 3. What changed — the C2 command vocabulary

> **Pivot** — `function/code` on `processCmd` in each binary, then regex the
> joined token text for string literals. The command table *is* the version
> fingerprint.

`processCmd` is the C2 command dispatcher, and it grew from **967 to 1 664 BSim
features** (+72 %). Its literals:

| A (2025-04) | B (2026-02) |
|---|---|
| `STD`, `TCP`, `UDP`, `VSE` | `STD`, `TCP`, `UDP`, `VSE` |
| `STDHEX` | — |
| `XMAS`, `STOMP`, `CRUSH` | — |
| `OVHKILL` | `OVH-STORM`, `HIPER-OVH` |
| `NFODROP` | `NFO-COM` |
| `STOP` | `KILLALLV3`, `HYDRA-KILL` |
| — | `CF-KILL`, `NULL-CF` (Cloudflare) |
| — | `HTTP-KO`, `HTTPS-KTN` (L7 / TLS) |

Four commands survive verbatim (`STD`, `TCP`, `UDP`, `VSE`). The rest is
renamed or new, and the naming style changes from bare words to hyphenated
`X-Y` — an operator/branding shift on top of the code change.

## 4. What was added — 19 new malware functions, all in B

> **Pivot** — set-difference of malware-named functions between the two
> `function/search?file_md5=` listings. A-only came back empty, which is the
> whole finding.

```
DNSw                HIPER_OVH          Randhex            SendCloudflare
SendDOMINATE        SendHOME1          SendHOME2          SendHTTPCloudflare
SendHTTPHex         SendOVH_STORM      UDPRAW             competitiveKiller
httpattack          sendHLD            sendHTTPtwo        sendKILLALL
sendPkt             sendTLS            xtdcustom
```

Grouped by what they buy the operator:

* **Anti-mitigation / L7** — `SendCloudflare`, `SendHTTPCloudflare`,
  `SendHTTPHex`, `sendHTTPtwo`, `httpattack`, `sendTLS`. A had **no** HTTP or
  TLS attack at all; it was purely volumetric. This is the single biggest
  capability change: the bot moved from L3/L4 floods to application-layer and
  TLS attacks aimed at CDN-fronted targets.
* **Provider-specific bypasses** — `SendOVH_STORM`, `HIPER_OVH`,
  `SendDOMINATE`, `sendHLD`, `SendHOME1`/`SendHOME2`. `SendOVH_STORM` is the
  header-fragmented `"PGET "` flood described in §7.1 of the family report.
* **Competitor eviction** — `competitiveKiller` and `sendKILLALL`, absent from A.
  `competitiveKiller` walks `/proc/%d/fd`, parses `/proc/net/tcp`
  (`"%*d: %255s %255s %x"`) and SIGKILLs processes holding sockets — i.e. B
  actively clears other bots off the host, a feature A did not have.
* **Support** — `DNSw` (DNS-based resolution, 103 features), `Randhex`,
  `UDPRAW`, `xtdcustom`, `sendPkt`.

## 5. What was modified in place

> **Pivot** — feature-count delta on the same-name functions, filtered to those
> where the delta is too large to be a compilation artefact.

| Function | A | B | Δ | Reading |
|---|---|---|---|---|
| `processCmd` | 967 | 1664 | **+72 %** | command dispatcher — matches the vocabulary growth in §3 |
| `main` | 277 | 494 | **+78 %** | startup/persistence path |
| `sockprintf` | 18 | 75 | **+316 %** | C2 write path — B's is substantially more complex (formatting/buffering for the new reporting) |
| `print` | 145 | 191 | +32 % | its formatting helper, grown in step |

Everything else with a large delta is a libc stub (`kill` 11→3, `bcopy` 307→3,
`fopen` 150→3) and reflects the two libc builds, not the malware.

Note what is *not* in this list: the flood primitives. `atcp`, `audp`, `ftcp`,
`rtcp`, `vseattack`, `SendUDP`, `csum`, `makeIPPacket`, `rand_cmwc` all keep
near-identical feature counts. **The author added new attacks; they did not
rewrite the old ones.**

## 6. Conclusions

1. **Same code base, two generations.** A (2025-04) and B (2026-02) share an
   untouched C2 transport and STD flood core; B's malware function set is a
   strict superset of A's, 28 shared + 19 added, nothing removed.
2. **The evolution is anti-mitigation.** A is a volumetric flooder (UDP/TCP/STD/
   VSE/XMAS/STOMP). B keeps all of that and adds Cloudflare bypasses, HTTP and
   TLS attacks, OVH-specific floods, and a competitor killer. That is the
   trajectory of a booter service chasing targets that deployed DDoS protection.
3. **The 0.107 score is misleading and should not be reported as-is.** It is
   depressed by (a) the MIPS↔SuperH4 architecture gap, which alone blocks 24
   provably-identical functions from matching, and (b) B statically linking
   ~390 more library functions than A, which dominates the denominator. The
   name-level Jaccard (0.60, subset relation) is the honest figure.
4. **Method note for similar cases.** When both sides retain symbols, the fastest
   route to a version diff is not the similarity tables at all — it is the
   symbol-name set difference plus the string literals of the command
   dispatcher. Two `function/search` calls and two `function/code` calls answered
   "what changed between these versions" more precisely than the 745-row diff.

### Follow-ups

* Same comparison against `cock`/`net` (MIPS BE/LE, 591/596 functions) would
  place A and B on a proper timeline for the whole `*net` set — those two are
  same-ISA-family, so the scores would be usable.
* `DNSw` (B-only) is worth reversing: DNS-based C2 resolution would be an
  infrastructure change, not just an attack one.
* Neither `main` nor `initConnection` exposes the C2 host as a literal — it is
  held in globals. Extracting it needs data-flow, not string search.
