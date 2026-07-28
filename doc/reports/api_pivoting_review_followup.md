# API review — addendum from the Kaiten timeline follow-up

Second pass over the BSimVis API, driven by
[`mirai7_kaiten_timeline.md`](mirai7_kaiten_timeline.md). Extends
[`api_pivoting_review.md`](api_pivoting_review.md); only **new** findings are
here. Server `http://localhost:5001/api`, collection `mirai7`. ~30 read calls.

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
