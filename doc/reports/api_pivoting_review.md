# BSimVis API — pivoting walkthrough, usability review and documentation gaps

Companion to [`mirai7_family_report.md`](mirai7_family_report.md). Everything in
that report was produced with the commands below, from a cold start (no prior
knowledge of the collection). Server: `http://localhost:5001/api`.

---

## 1. How I discovered the API

1. `doc/api_documentation.md` — endpoint list, params. This is good and was
   enough to work without reading source.
2. `ss -ltnp` to find the live port. **The docs and `AGENTS.md` say API default
   is 5000; the running instance is on 5001.** First `curl` to `:5000` failed
   silently (connection refused, empty body) with no hint.
3. From then on, pure `curl` + `python3 -m json.tool`.

---

## 2. The pivot chain, with the actual commands

### Step 0 — find the collection, size it up

```bash
curl -s "localhost:5001/api/collection/search?q=mirai&limit=20"
curl -s "localhost:5001/api/index/status?collection=mirai7"
# 174 files, 34503 functions, 99.25% indexed
```

### Step 1 — file inventory + threat-intel metadata

```bash
curl -s "localhost:5001/api/file/search?collection=mirai7&limit=200" -o files.json
```

One call returned everything needed for the corpus overview: `language_id`,
`filetype`, `avtype`, `yara`, `cc_ip`, `first_seen`, `file_names`,
`function_count`, `bsim_features_count`, `bin_clusters`. Aggregation was local
(`collections.Counter`). **This is the single best endpoint in the API** — it
collapses what would normally be five tools into one call.

C2 pivot, straight from the same endpoint:

```bash
curl -s "localhost:5001/api/file/search?collection=mirai7&cc_ip=143.20.185.245&limit=20"
# 7 files, 4 architectures, all hash-named
```

### Step 2 — binary clusters (which files belong together)

```bash
curl -s "localhost:5001/api/bin_cluster/list?collection=mirai7&limit=200&sort_by=count&sort_order=desc" -o bc.json
```

Trap (see §4.1): the response is a **dendrogram**. Root node = all 174 files at
cohesion 0.043. Leaves have to be derived client-side:

```python
par = {str(c['parent']) for c in results}
leaves = [c for c in results if str(c['cluster_id']) not in par]   # 50 of 100
```

### Step 3 — binary similarity pairs (how strongly, and across what)

```bash
# naive: useless — top 300 pairs are all 2-function UPX stubs at score 1.0
curl -s "localhost:5001/api/bin_sim/search?collection=mirai7&limit=300&sort_by=score&sort_order=desc"

# useful:
curl -s "localhost:5001/api/bin_sim/search?collection=mirai7&limit=500&min_funcs=50&min_score=0.5&sort_by=score&sort_order=desc"
# 15051 pairs -> 89
```

Then quantify one pair:

```bash
curl -s "localhost:5001/api/bin_sim/diff?collection=mirai7&md5_a=168036e68ea46fd5dd2be5f70e248d9d&md5_b=282898fac2e6a88c4108e9751cfc8ce4&view=sankey"
# {"score":0.973, "counts":{"matched":583,"unique_to_a":8,"unique_to_b":13}}
```

`view=sankey` is the right call here — the full diff document for a 590-function
binary is huge, the projection answers "how much is shared" in one line.

### Step 4 — function clusters (what code is reused)

Naive sort by count is again useless: the biggest clusters are dendrogram
internals (`entry`, n=30172, cohesion 0.003) and statically-linked uClibc
(`__stdio_wcommit`, `_ppfs_setargs`, `malloc`, `memchr`). Filtering helps:

```bash
curl -s "localhost:5001/api/cluster/list?collection=mirai7&limit=40&sort_by=count&sort_order=desc&min_cohesion=0.9&min_features=60"
```

…but the real breakthrough was going the other way — **from a name, not from a
cluster**:

```bash
curl -s "localhost:5001/api/function/search?collection=mirai7&function_name=table_init&limit=10&min_cohesion=0.9"
```

`function/search` returns a `clusters` map alongside the matched functions.
Because a handful of samples in this collection kept their symbols, one query
turns a known Mirai symbol into a cluster UUID, and the cluster UUID turns into
every stripped `FUN_xxxxxxxx` twin:

```bash
curl -s "localhost:5001/api/function/search?collection=mirai7&cluster_uuid=8ec63aa85bd9&limit=200"
# 40 members, 34 files, 8 architectures — all with full metadata (arch, file, features)
```

This "symbolised sample as Rosetta stone" workflow is the strongest thing the API
supports, and it is **not described anywhere in the docs**.

### Step 5 — down to source

```bash
curl -s "localhost:5001/api/function/code?id=idx:mirai7:func:0fac505e1c34ad2c2f108c8fa586d8a3:0001c164"   # ARM  table_init
curl -s "localhost:5001/api/function/code?id=idx:mirai7:func:069a963215886b09b570c345e46ba20a:00407780"   # x86-64 FUN_00407780
```

Rows → tokens → join `token['text']`. Side-by-side is also available via
`/api/diff?...&addr_a=..&addr_b=..`.

Grepping the joined token text for IPs/URLs is what surfaced the loader
`37.48.254.120` in the `mipsel` exploit scanners — there is no string-search
endpoint, so this has to be done client-side, function by function.

### Step 6 — annotate the results back into the collection

```bash
# whole cluster at once — every stripped twin inherits the label
curl -s -X POST localhost:5001/api/tags/bulk_add -H 'Content-Type: application/json' \
  -d '{"collection":"mirai7","entity_type":"function",
       "entity_ids":["mirai7:func:<md5>:<addr>", "..."],"tag":"mirai:config_table"}'

curl -s -X POST localhost:5001/api/tags/add -H 'Content-Type: application/json' \
  -d '{"collection":"mirai7","entity_type":"file","entity_id":"mirai7:file:<md5>","tag":"campaign:frosted"}'

curl -s -X POST localhost:5001/api/notes/file/add -H 'Content-Type: application/json' \
  -d '{"collection":"mirai7","file_id":"mirai7:file:<md5>","owner":"claude-report","text":"..."}'
```

`entity_id` is the `file_id` / `function_id` straight out of the search
endpoints — this composes perfectly with step 4 (search a cluster, pipe the
`function_id` list into `bulk_add`). 15 tag operations covered 88 functions and
124 file-tag assignments. This is the API at its best.

### Step 7 — LLM triage

```bash
curl -s -X POST localhost:5001/api/llm/summarize -H 'Content-Type: application/json' \
  -d '{"func_id":"mirai7:func:069a963215886b09b570c345e46ba20a:00407780","func_name":"FUN_00407780"}'
```

Returns **plain text** (SUMMARY / KEYWORDS / IMPACT / LOGIC sections), not JSON.
Genuinely useful: run on the stripped x86-64 `FUN_00407780` it described Mirai's
`table_init` semantics correctly without any symbol, independently confirming the
cluster match. It also surfaced the `37.48.254.120` loader — which I verified in
the raw decompiled strings before putting it in the report.

---

## 3. How good is the API for pivoting?

**Very good.** Score by axis:

| Axis | Verdict |
|---|---|
| Corpus triage (file → metadata) | Excellent. One call, all threat-intel fields, all filterable. |
| Infrastructure pivot (C2, YARA, AV, filename) | Excellent — `cc_ip`, `yara`, `avtype` are first-class filters at file *and* function level. |
| File→file similarity | Good, once `min_funcs` is applied. `bin_sim/diff?view=sankey` is exactly the right granularity for a report. |
| Cluster → member drill-down | Awkward. Two endpoints (`cluster/members`, `cluster/functions`) both underperform `function/search?cluster_uuid=`. |
| Name → cluster → stripped twins | Excellent, and the highest-value path. Undocumented. |
| Cluster → code | Excellent, but the ID format has to be reconstructed by hand (§4.4). |
| Cross-architecture reasoning | Works at function level (8 ISAs in one cluster), fails at binary level (MIPS BE↔LE only). Nothing in the docs sets that expectation. |
| Annotation write-back | Excellent. `tags/bulk_add` eats `function_id` lists from search verbatim; cluster-wide labelling is one call. |
| LLM assistance | Useful for stripped code, but see §4.11 (config points at an uninstalled model, response is plain text, no `model` override). |
| String / IOC search | **Missing.** No endpoint searches decompiled text or embedded strings; IOC extraction is a client-side loop over `function/code`. |

Round-trip cost: the analysis was ~30 read calls plus ~20 annotation writes. No pagination pain, no
rate limits, filters compose cleanly. The gap is not capability — it's that a
newcomer will burn their first hour on the four traps below.

---

## 4. Documentation gaps (in priority order)

### 4.1 Cluster lists return a dendrogram, not a partition — **not documented**

`cluster/list` and `bin_cluster/list` return HDBSCAN condensed-tree nodes,
parent-linked. For `mirai7`: `bin_cluster/list` root node `174` has `count: 174`
(the whole collection) at cohesion 0.043; `cluster/list` root has `count: 30172`.
Sorting by `count` desc — the obvious first move — returns meaningless nodes.

Docs only say *"Cluster membership excludes points shed as noise by HDBSCAN"*.
They should say:

* the list contains internal nodes **and** leaves, linked by `parent`;
* `cohesion_score` rises as you descend;
* the leaf-derivation snippet from §2 above;
* that `file['bin_clusters']` is the file's **path through the tree**, not a set
  of distinct memberships (a file listing 17 `bin_clusters` is in one branch).

### 4.2 `parent` is a **string** while `cluster_id` is an **int**

```json
{"cluster_id": 175, "parent": "174"}
```

Any `id in {parents}` comparison silently returns "everything is a leaf". Either
document it or normalise the type.

### 4.3 `cluster/functions` and `cluster/members` are effectively broken/misleading

* `cluster/functions?cluster_uuid=<uuid>` returns members **without** the
  `file_name`, `file_md5` and `language_id` keys at all (keys present:
  `function_id`, `function_name`, `entrypoint_address`, `namespace`,
  `return_type`, `parameters`, `bsim_features_count`) — the file/arch fields you
  actually pivot on are missing. `file_md5` is recoverable only by parsing
  `function_id`.
* Passing a **`cluster_id`** to `cluster_uuid` returns `{"functions": [], "total": 0}`
  — an empty success, not a 400. Easy silent dead end.
* `cluster/list?...&show_members=true` returns `sample_members` **with** full
  metadata — inconsistent with `cluster/functions`.

Docs should state plainly: **use `function/search?cluster_uuid=` for cluster
drill-down**; `cluster/functions` is a lightweight sample only.

### 4.4 Function ID format is documented, but its construction isn't

The doc says `function/code` wants `idx:coll:func:md5:addr`. Search endpoints
return `function_id: "mirai7:func:<md5>:<addr>"` — the same string **without**
the `idx:` prefix. Both forms work in practice (verified: identical 164-row
response), but the docs show only the prefixed form, so a reader assumes a
transformation is required. Say that `function_id` is directly usable. Also
`full_id` exists on function documents in a third, unrelated format
(`<md5>:#<md5>::<name>:@<addr>`) with no explanation of which endpoint eats it.

### 4.5 Response envelopes are undocumented across the board

The doc lists params and describes returns in prose, but never gives the
top-level keys. Observed, and inconsistent:

| Endpoint | Items key |
|---|---|
| `file/search` | `files` (+ undocumented `bin_cluster_map`, `total_files_in_collection`) |
| `function/search` | `functions` (+ undocumented `clusters` map, `pool_truncated`) |
| `cluster/list`, `bin_cluster/list`, `bin_sim/search` | `results` |
| `cluster/functions` | `functions` |
| `collection/search` | `collections` |

The `clusters` map on `function/search` is the key to the whole
name→cluster→twins workflow and it isn't mentioned at all.

### 4.6 Packed/tiny binaries dominate similarity — no guidance

15 051 bin_sim pairs; the top 300 by score are all 2-function UPX stubs at
score 1.0. `min_funcs` exists and fixes it, but the doc doesn't say the default
ranking is dominated by degenerate binaries, nor whether `min_funcs` applies to
one side or both. `doc/similarity_filtering.md` would be the right home for a
"filter degenerate binaries" section.

### 4.7 `min_cohesion` default of 0.95 on `function/search` is a silent filter

Documented (*"clusters below the threshold are dropped"*) but the effect is
invisible: you get functions back with an empty/partial `clusters` map and no
indication that clusters were dropped. A `clusters_filtered: N` counter would
save a lot of confusion. `cluster/list` has **no** default — the asymmetry
between the two endpoints should be called out.

### 4.8 `file_name` matching is fuzzy, with no exact-match option

`file/search?file_name=net` matched a different file than the sample literally
named `net`, producing a nonsense diff (0 matched, 591 unique) before I noticed.
The doc explains that `file_name` also matches parent/related names, but not that
matching is substring-based. There is no `exact=true`. Workaround: always resolve
to `file_md5` first.

### 4.9 Cross-architecture behaviour is undocumented

Empirically: function-level clusters cross ISAs freely (one cluster spans ARM,
MIPS BE/LE, x86 32/64, SuperH4, PowerPC, SPARC), while binary-level similarity
only crosses **endianness within the same ISA**. That is a fundamental property
of how the scores are built and every analyst will hit it. One paragraph in
`doc/similarity_filtering.md` would prevent wrong conclusions ("these samples are
unrelated" when they share 34-file function clusters).

### 4.11 LLM endpoints: undocumented failure mode and response type

* `bsimvis_config.toml` shipped pointing at `qwen3.6:35b`, which was not present
  in the local Ollama (`qwen3.5:4b`, `qwen3.5:9b`, `qwen3.6:latest` were). The
  API surfaced it as a bare `Error: model 'qwen3.6:35b' not found (status code:
  404)` with **HTTP 200**. Neither the docs nor `AGENTS.md` mention that the
  model name must match a pulled Ollama model, or where to change it.
* `/api/llm/summarize` returns **plain text**, not JSON — the doc says
  "Generates a summary" with no response type. `json.load()` on it fails with a
  confusing `Expecting value: line 1 column 1`.
* No `model` parameter in the request body — you cannot pick a smaller/faster
  model per call, so a wrong config value blocks all LLM use.
* Cost is not signalled: a single `summarize` on a 160-line function took
  ~2 minutes on a 9B local model. Worth a note that these calls are slow enough
  to need a long client timeout.

### 4.12 No string / IOC search

The highest-value IOC in this collection (the loader `37.48.254.120`, embedded
in five exploit scanner functions) is only reachable by fetching
`function/code` per function and regexing the joined token text. `features/*`
indexes BSim features, not literals. A documented "search decompiled text /
string literals" filter on `function/search` would remove a whole class of
client-side loops.

### 4.13 Smaller items

* `index/status` returns `num_sim_meta: 500000` — a suspiciously round number
  (cap? truncation?). Undocumented either way.
* `bin_cluster/list` returns `yara_distribution`, `avtype_distribution`,
  `filetype_distribution`, `ccip_distribution` — all empty here, and not
  mentioned in the docs. If they are populated only under some condition
  (a rebuild flag?), say which.
* `cluster_name` is derived from a majority member name, so clusters get named
  after uClibc functions (`__xstat64_conv`) or after one fork's private symbol
  (`xor_init` vs `table_init`). Worth one line: **cluster names are hints, not
  labels**.
* No documented way to list all distinct values of a metadata field
  (arch, yara, avtype) — every consumer re-implements the counting client-side.

---

## 5. Suggested additions to `AGENTS.md`

`AGENTS.md` documents the internals well (jobs, pools, Lua/Kvrocks constraints)
but says nothing about *consuming* the API, which is what every analyst-facing
agent task needs. Proposed sections:

```markdown
## Talking to the API

- Check the live port before assuming the default: `ss -ltnp | grep python`.
  `.env` (`APP_PORT`) overrides the 5000 default — the dev box runs 5001.
- Swagger UI: `/api/`. Endpoint reference: `doc/api_documentation.md`.
- Response envelopes differ per endpoint: `files` / `functions` / `results` /
  `collections`. Never assume `items`.

## Cluster model (read before any cluster query)

`cluster/list` and `bin_cluster/list` return an HDBSCAN **condensed tree**, not a
flat partition. The largest-`count` node is the root and is meaningless.
`parent` is a string, `cluster_id` an int. Derive leaves client-side, or descend
until `cohesion_score` is high enough for your purpose.
Drill down with `function/search?cluster_uuid=` (full metadata), not
`cluster/functions` (sample only, null `file_name`).

## Analyst pivot recipe (corpus -> reused code)

1. `file/search?collection=X&limit=200`            — inventory + threat-intel
2. `bin_cluster/list` -> leaves                    — which files group together
3. `bin_sim/search?min_funcs=50&min_score=0.5`     — how close, cross-arch or not
4. `bin_sim/diff?...&view=sankey`                  — matched/unique counts for a pair
5. `function/search?function_name=<known symbol>`  — read `clusters` from response
6. `function/search?cluster_uuid=<uuid>`           — stripped twins in other samples
7. `function/code?id=<function_id>`                — the code itself
   (`function_id` from search is usable as-is; the `idx:` prefix is optional)
8. `tags/bulk_add` + `notes/file/add`              — write findings back;
   tagging cluster members propagates a name to every stripped twin
9. `llm/summarize` (plain-text response, slow)     — triage stripped functions;
   treat output as a lead and verify against `function/code`

## LLM config

`bsimvis_config.toml` `[llm].model` must name a model actually pulled in Ollama
(`curl localhost:11434/api/tags`). A mismatch returns HTTP 200 with a plain-text
`Error: model '<x>' not found` body. There is no per-request model override.

## Known analysis traps

- Packed samples (UPX) decompile to ~2 functions and score 1.0 against each
  other. Always set `min_funcs` on `bin_sim/search`.
- `min_cohesion` defaults to 0.95 on `function/search`, 0 on `cluster/list`.
- `file_name` filters are substring matches over own/parent/related names —
  resolve to `file_md5` before diffing.
- Function clusters cross architectures; binary similarity effectively does not
  (only BE<->LE within one ISA).
- `cluster_name` is a majority-vote hint, often a libc name. Don't trust it.
```

---

## 6. Bottom line

The API is genuinely good for the corpus → cluster → binary pair → function
cluster → source pivot; I went from "never seen this collection" to identifying
a cross-architecture shared config-table routine and an operator branding string
in about twenty calls. The blockers are documentation, not capability: the
dendrogram shape of cluster lists (§4.1), the `function/search?cluster_uuid=`
drill-down path (§4.3/4.5), and the packed-binary ranking trap (§4.6) are the
three that cost the most time and would each be fixed by a paragraph.
