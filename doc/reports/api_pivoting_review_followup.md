# API review — addendum from the timeline follow-ups

Findings from two further passes over the BSimVis API:
[`mirai7_kaiten_timeline.md`](mirai7_kaiten_timeline.md) (§1–3, ~30 read calls,
symbol-set method) and [`mirai7_mirai_timeline.md`](mirai7_mirai_timeline.md)
(§4, ~380 read calls, cluster method). Extends
[`api_pivoting_review.md`](api_pivoting_review.md); only **new** findings are
here. Server `http://localhost:5001/api`, collection `mirai7`.

---

## 1. What worked well (new)

### 1.1 `function/search?function_name=` collection-wide is the single best pivot

One call answered "which files are this family":

```bash
curl -s "localhost:5001/api/function/search?collection=mirai7&function_name=DNSw&limit=50"
# total 8 — botnet, cock, cracknet, dicknet, fucknet, net, swatnet, unet
```

A rare malware symbol resolves an entire cross-compiled build set — 8
architectures — in one round trip. Faster and far more reliable than clustering
or `bin_sim/search` for this question, because a symbol name is exact and the
similarity score is not comparable across ISAs (§2.1).

This deserves to be recipe step 0 in the docs: **if any sample keeps symbols,
pick a distinctive one and query it collection-wide before touching any
similarity endpoint.**

### 1.2 Same-ISA diffs are precise, and worth engineering the pair for

`diff?table=matched&limit=0` on two MIPS:LE binaries returned 36 malware
functions at similarity **exactly 1.000** with identical feature counts. That is
a "these are the same object file" verdict, not a fuzzy score, and it let the
previous report's conclusions be corrected outright.

Practical rule the API rewards: **before diffing, list the family members and
choose the same-ISA pair**, even if it is not the pair you were asked about. The
cross-ISA diff of the same code answered 0.107; the same-ISA diff answered
"identical except one function".

### 1.3 `file_metadata_a` / `file_metadata_b` on every diff page

`language_id` rides along with each diff response, so the "is this score
comparable" check costs zero extra calls. Undocumented, but correct and useful.

### 1.4 `function/code` returning a token stream is right for scripted analysis

Joining `token['text']` gives grep-able C. Regexing string literals out of
`processCmd` was the highest-information operation of the whole analysis, and
comparing the *character length* of two decompilations turned out to be a
usable proxy for decompiler quality per ISA (63 344 chars on MIPS vs 33 048 on
SuperH4 for the same function).

---

## 2. New bugs and defects

### 2.1 **`avg_features` on matched diff rows is not an average**

`diff?table=matched` row for `processCmd`, A vs `net`:

```json
{"similarity": 0.9904, "avg_features": 2153.0,
 "func_a": "mirai7:func:7b70…:004069e4", "func_b": "mirai7:func:2828…:0040c1c4"}
```

`functions_metadata` gives `bsim_features_count` **967** for `func_a` and
**2153** for `func_b`. The mean is 1560; the field reports 2153.

Only 3 of the 209 matched rows have differing counts at all (the rest are
identical functions, where mean and max coincide). All three show
`avg_features == max(features_a, features_b)`, in both directions:

| Function | A | net | `avg_features` | true mean |
|---|---|---|---|---|
| `processCmd` | 967 | 2153 | **2153** | 1560 |
| `main` | 277 | 280 | **280** | 278.5 |
| `fopen` | 150 | 149 | **150** | 149.5 |

`fopen` rules out "it reports the B side" — it is `max()`. Which is the same
defect already found in `min_funcs` (§4.17 of the original review): a
pair-reducing statistic implemented as `max`.

Impact: `sort_col=avg_features` is documented and used as "sort by size", and it
silently sorts by one side only. Worse, anyone reading the field as an average
will overestimate the smaller side on exactly the rows that matter (the ones
that changed — `processCmd` reads as 2153/2153 rather than 967→2153, hiding the
+123 % growth entirely). Either fix the computation or rename the field, and expose
`features_a` / `features_b` on the row so the delta needs no
`functions_metadata` join.

### 2.2 `functions_metadata` values use `name`; every search endpoint uses `function_name`

```jsonc
// function/search  →  {"function_name": "processCmd", …}
// diff  functions_metadata  →  {"name": "processCmd", …}
```

Same entity, same collection, two key names, neither documented (§4.5 of the
original review listed the envelope keys but not the item shape). A `.get('function_name')`
against diff metadata returns `None` for every row and looks like "the diff has
no names", which is a silent wrong answer rather than an error. Pick one key.

### 2.3 Matched rows can pair functions with **different names**, unflagged

Real rows from A ↔ `net`, both at similarity 1.000:

```
SendSTDHEX   →  SendSTD_HEX      1.000
SendSTD_HEX  →  SendHOME1        1.000
```

A contains both `SendSTDHEX` and `SendSTD_HEX` with identical feature vectors
(41 each); `net` contains `SendSTD_HEX` and `SendHOME1`, also identical. The
matcher makes an arbitrary 1:1 assignment among interchangeable candidates, so
`SendSTD_HEX` "became" `SendHOME1` — which reads as a rename and is not one.

This is the mirror image of §4.18 in the original review (there: same name, no
match). Both are the same missing signal. A `name_match: true|false` flag on
matched rows, plus an `ambiguous: N` count when the feature vector matched more
than one candidate, would cost nothing and prevent a fabricated rename in a
report.

### 2.4 The similarity score punishes containment

A ⊂ `net`: 209 of A's 213 functions matched, and the 4 that did not are uClibc.
By any analyst's reading that is "A is fully contained in `net`". The score is
**0.485**, because the denominator is the union and `net` has 387 extra library
functions.

Two same-generation binaries (`cock`/`net`) score 0.973; a binary and its own
direct ancestor scores 0.485; an unrelated family fork scores 0.625. **The
ranking is wrong**: the fork outranks the ancestor.

The fix is cheap and does not touch the existing score — add directional
coverage to the diff response:

```jsonc
{"score": 0.485, "coverage_a": 0.981, "coverage_b": 0.351}   // matched/|A|, matched/|B|
```

`coverage_a = 0.98` says "A is a subset of B" immediately, which is the finding
that took a 209-row table and a name classification to reach. This is also the
generic answer to §4.19 and §4.12 of the original review: asymmetric coverage is
robust to one side statically linking more libc.

### 2.5 `diff?table=…` responses carry no score

`diff?...&view=sankey` returns `score` and `counts`; `diff?...&table=matched`
returns `items/total/offset/limit/table/functions_metadata/file_metadata_a/b`
and **no score at all**. Two calls to get the number and the rows for the same
pair. Add `score` and `counts` to the table responses — the data is already
computed.

### 2.6 Decompiler quality varies by ISA and nothing in the API says so

Same source function, same build set:

| Build | ISA | `processCmd` features | decompiled chars | string literals |
|---|---|---|---|---|
| `net` | MIPS:LE:32 | 2153 | 63 344 | **29** |
| `botnet` | SuperH4:LE:32 | 1664 | 33 048 | 13 |

Both binaries contain the same 59 malware symbols, so the capability is
identical — SuperH4 simply recovers less. Consumed naively this produces a false
finding ("this build lacks 16 attack commands"), and the previous report made
exactly that error.

`bsim_features_count` is treated throughout the UI and the docs as a proxy for
function complexity. Across architectures it is a proxy for **decompiler
success**. One line in `doc/similarity_filtering.md`: *feature counts and
literal recovery are comparable within an ISA only; when a family spans
architectures, read code from the best-decompiling member.*

### 2.7 No data-section access — the concrete blocker

Confirmed exhaustively for the C2 host of this family:

* `initConnection` does `strcpy(buf, (&commServer)[currentServer])` — the C2
  list is a `char *` array in `.data`.
* `main` has **zero** string literals and never names `commServer`.
* `cc_ip` is `[]` for all ten family files.
* `function/code` decompiles functions only; there is no endpoint that reads a
  data section, resolves a `DAT_xxxxxxxx` / `C.nnn.nnnn` symbol, or lists
  strings.

So the single most valuable IOC in the family is unreachable through the API,
while Ghidra on the server side has it. §4.14 of the original review asked for
string search; this sharpens it to a minimum viable version:

* `GET /api/file/strings?file_md5=&min_len=` — the `.rodata` string table;
* `GET /api/file/data?file_md5=&symbol=DAT_00435a98` (or `&addr=`) — resolve one
  referenced data symbol to its bytes/string.

The second is the smaller build and answers more: every interesting global in
this family (`commServer`, the check-in format string, `DNSw`'s nine packet
templates at `C.278.6738`) is reachable from a `DAT_`/`C.` name already present
in the decompilation. A one-symbol lookup turns `function/code` output into
complete analysis instead of a pointer to something the API will not give you.

---

## 3. Cost note

The family sweep in §1 of the timeline report was done the expensive way —
174 `function/search?file_md5=` calls, ~6 MB — before realising that
`function/search?function_name=DNSw` answers it in one. Keep both paths in the
docs but lead with the symbol query: the per-file sweep is only worth it when
you also want the *partial* matches (it is what surfaced `m-p.s-l.dick`, which
shares 12 helpers and no dispatcher, and which the single-symbol query cannot
see by construction).

A `function/search?collection=&function_name_in=a,b,c` (or repeated
`function_name=`) returning file → matched-name counts would give both answers
in one call, and is the natural companion to the facet/`distinct` request in
§4.11 of the original review.

---

## 4. Second pass — the Mirai capability timeline

Findings from [`mirai7_mirai_timeline.md`](mirai7_mirai_timeline.md), which ran
the same exercise over the 164 non-Kaiten files. That side of the corpus is
almost entirely stripped, so the analysis ran on **clusters** rather than symbol
names — a much better exercise for the API, and it surfaced different gaps.

### 4.1 The `clusters` map is keyed by `cluster_id`, but you need `cluster_uuid`

```jsonc
// function/search?function_name=table_init
"clusters": { "31836": { "cluster_id": 31836, "cluster_uuid": "8ec63aa85bd9", … } }
```

The key is the int id; the drill-down endpoint
(`function/search?cluster_uuid=`) wants the uuid from the value. Passing the key
returns `{"functions": [], "total": 0}` — an empty success, the same silent dead
end already noted for `cluster/functions` in §4.3 of the original review. Key the
map by uuid, or accept either.

### 4.2 `function_name` is a substring match, and the `clusters` map is a flat union

`function_name=attack_tcp_syn` also returns `attack_tcp_synr` and
`attack_tcp_syn_aisuru`. That is defensible on its own — but the `clusters` map
is a single union over **all** returned functions, with nothing tying a cluster
to the function it came from. Resolving one symbol to its cluster reliably
therefore takes: one name query, then one membership query per candidate cluster,
then an intersection against the exact-name function ids.

Two fixes, either is enough:

* an `exact=true` flag on `function_name` (also solves §4.8 of the original
  review, where substring matching on `file_name` produced a nonsense diff);
* `cluster_uuid` as a field on each **function row**, which is the natural place
  for it and removes the map lookup entirely.

For 100 symbols this was 201 calls instead of 100.

### 4.3 There is still no way to ask "which files kept their symbols"

§4.11 of the original review asked for this. The Mirai pass shows the cost
concretely: finding the 4 symbolised files out of 164 took **164
`function/search?file_md5=` calls and ~6 MB** of responses, to compute one ratio
per file (share of names not matching `FUN_*`).

Those 4 files are the anchors for the entire analysis — every capability in the
timeline is reached from them. So the *first* question in a stripped corpus is
the one the API answers least efficiently. A `has_symbols=true` filter, or a
`named_function_count` field on `file/search`, turns this step into one call.

### 4.4 Clusters know their members, but not when those members appeared

Building a timeline means joining, client-side, three things the server already
has: cluster → member functions → member files → `first_seen`. A `first_seen` /
`last_seen` pair on cluster responses (`cluster/list`, and the `clusters` map on
`function/search`) would make "when did this code first appear in the corpus"
a sort instead of a join. Same for `member_file_count`, which today means
fetching up to 500 member rows to count distinct md5s.

### 4.5 `cluster_name` cannot be used as a grouping key

Already flagged as "a hint, not a label" in §4.21 of the original review. The
Mirai pass shows the failure mode: the config-table routine's clusters are named
`FUN_0040becc` in one case and `xor_init` in another — the same code, two labels,
neither being the canonical `table_init`. Grouping by `cluster_name` silently
splits a capability in two. Grouping by the cluster's **dominant symbol name**
(computed client-side from the members) works; that computation belongs on the
server.

### 4.6 What the API got right here

Worth stating, because this pass leant on it heavily:

* **`function/search?cluster_uuid=` is the workhorse.** Full metadata per member —
  file name, md5, architecture, feature count — so a cluster resolves to "which
  samples carry this code" in one call. Everything in the Mirai timeline rests on
  it.
* **Clusters genuinely cross the naming conventions.** `xor_init` and
  `table_init` land in one cluster; `attack_tcp_syn` and `attack_tcp_syn_aisuru`
  land in one cluster. Symbol-set methods cannot see either, and the tool gets
  this right with no tuning.
* **Clusters are also honest about limits.** `mipsel`'s `flood_*` routines
  cluster only with themselves, correctly reporting a rewritten attack layer
  rather than forcing a match to `attack_*`.

One caveat for any automated view built on this: **clusters resolve modules, not
individual attack methods.** A single cluster holds `attack_tcp_syn`,
`attack_tcp_ack`, `attack_gre_ip`, `attack_tcp_null` and six more — they share a
packet loop. Counting capabilities from clusters undercounts, and a view that
reports "22 attack capabilities" should say it is counting clusters.
