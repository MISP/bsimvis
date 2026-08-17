# BSimVis tag taxonomy

Status: **implemented**. The migration has *not* been run against any
collection — see [Running the migration](#running-the-migration).

The vocabulary and the old-to-new mapping live in one module,
`bsimvis/app/services/tag_taxonomy.py`, so the LLM prompt, the validator that
rejects invented tags, and the migration script cannot drift apart. Add a leaf
there and it reaches the model without a second edit.

## Why

Today every tag lives in one loosely-structured space and is routed onto two
axes at read time (`bsimvis/app/services/bin_sim_tags.py`, `TAG_NAMESPACES`):

- **provenance** — `lib:`, `stdlib:`, `bundle:`
- **flags** — `flag:`, `llm:`, and anything with an unrecognised namespace

Two problems follow from that.

**Severity is welded into the behaviour tag.** The LLM emits
`flag:<risk>:<capability>` (`bsimvis/app/services/llm_service.py:54`), so
"how bad is it" and "what does it do" share one id. You cannot ask *"how much
of the shared mass is high-severity network code"* without string surgery on
the tag id, and the Sankey cannot put severity on one side and behaviour on the
other.

**The capability list is flat and mixed.** 28 names with no grouping, where
`init`, `string` and `math` (compiler plumbing) sit at the same level as
`ransomware` and `c2` (the actual finding). Every graph node is a leaf, so 28
thin ribbons render instead of a readable handful.

**Human tags pollute the behaviour axis.** A bare `mirai` or `bookmark` typed
into the tag box has no namespace, so it falls to the flags axis by default
(`DEFAULT_NAMESPACE`) and competes for mass with LLM findings.

The fix is four independent namespaces, one per question, each its own axis.

## The four namespaces

| Namespace | Axis | Cardinality | Written by | Exclusive? |
|---|---|---|---|---|
| `origin:` | Origin | open | Function ID, bundle import | yes — priority-resolved |
| `severity:` | Severity | 4 fixed | LLM | yes — one per function |
| `category:` | Behavior | 9 parents / 51 leaves | LLM | no — overlap is the point |
| `user:` | User | open | human | no |

### Origin

Replaces provenance. Uniform depth so rollup is one rule for every kind:

```
origin:<kind>:<name>:<version>[:<func>]

origin:lib:libc:2.31:memcpy
origin:stdlib:musl:1.2.4
origin:bundle:mirai:unknown:scanner_init
```

Synthetic buckets are unchanged: `original_code` (no origin tag on at least one
side) and `tag_mismatch` (both sides tagged, disjoint).

Priority is unchanged — `lib`/`stdlib` 100 beats `bundle` 50, so a statically
linked `memcpy` inside a Mirai sample still counts as libc. That rule is what
keeps a libc floor from turning into a fake family attribution.

Bundles have no natural version, but carry one anyway so the rollup depth is a
constant 4 rather than a per-kind table. Unknown version is the literal
`unknown`; the display layer strips a trailing `:unknown` so the UI still reads
`bundle / mirai`.

### Severity

Exactly one per function. Ordinal, so the Sankey can colour-ramp it.

```
severity:none   severity:low   severity:medium   severity:high
```

### Behavior

Two levels. The Sankey node is the **parent**; the leaf stays available for
filtering and search. Grouping costs no new rollup code — the non-origin axes
roll up at depth 2 (`_PARENT_DEPTH` in `bin_sim_tags.py`), which is exactly
`category:<group>`.

```
category:network:      c2  download  upload  p2p  scan  proxy  dns  socket
category:crypto:       cipher  hash  key_exchange  encoding  random
category:file:         read_write  path  archive  tempfile
category:process:      exec  inject  thread  shell  ipc  privesc
category:persistence:  autostart  service  cron  bootloader  registry
category:evasion:      anti_debug  anti_vm  obfuscation  packer  rootkit  log_clear
category:recon:        sysinfo  proclist  filesearch  creds  env
category:impact:       ddos  ransom  wipe  exfil  keylog  screencap  spyware
category:util:         init  string  memory  math  parser  compression  wrapper
```

`category:util` is deliberate: it is the boring-mass bucket. Without it the
libc/compiler floor has nowhere to land on the behaviour axis and quietly
inflates whatever else the LLM picked.

`category:impact:spyware` overlaps `keylog`/`screencap` on purpose — it is the
fallback when the LLM cannot tell which, and it makes the migration of the old
`spyware` capability lossless. Prefer the specific leaf when it is knowable.

### User

Open vocabulary, never touches Behavior mass.

```
user:bookmark   user:ignore   user:suspicious   user:<free_slug>
```

## Reserved namespaces

Not implemented, no code now. Each becomes its own axis when added; the
namespace table already supports that shape.

```
capa:communication/http/client
mitre:defense-evasion:obfuscation
kill-chain:command-and-control
maec-malware-capabilities:anti-behavioral-analysis
```

These are externally standardised. They are recorded verbatim, not remapped
into `category:`, precisely because their value is that they match what other
tools emit.

## Migration

One-shot rewrite of every tag id plus a reindex. Old ids do not survive.

### Severity

| old risk | new |
|---|---|
| `benign` | `severity:none` |
| `suspicious` | `severity:medium` |
| `malicious` | `severity:high` |

`severity:low` is never produced by the migration — only by the LLM going
forward. That is expected, not a bug: the old 3-point scale has no cell for it.

### Capability

All 28 old capabilities, from both `flag:<risk>:<cap>` and the legacy
`llm:<risk>:<cap>`:

| old | new | | old | new |
|---|---|---|---|---|
| `init` | `category:util:init` | | `c2` | `category:network:c2` |
| `string` | `category:util:string` | | `download` | `category:network:download` |
| `memory` | `category:util:memory` | | `network_io` | `category:network:socket` |
| `math` | `category:util:math` | | `p2p` | `category:network:p2p` |
| `parser` | `category:util:parser` | | `persistence` | `category:persistence:autostart` |
| `compression` | `category:util:compression` | | `registry` | `category:persistence:registry` |
| `file_io` | `category:file:read_write` | | `privesc` | `category:process:privesc` |
| `crypto` | `category:crypto:cipher` | | `injection` | `category:process:inject` |
| `encoding` | `category:crypto:encoding` | | `shell` | `category:process:shell` |
| `anti_debug` | `category:evasion:anti_debug` | | `ransomware` | `category:impact:ransom` |
| `anti_vm` | `category:evasion:anti_vm` | | `ddos` | `category:impact:ddos` |
| `obfuscation` | `category:evasion:obfuscation` | | `exfil` | `category:impact:exfil` |
| `packer` | `category:evasion:packer` | | `destruction` | `category:impact:wipe` |
| | | | `spyware` | `category:impact:spyware` |

One old tag produces **two** new tags — a `severity:` and a `category:` — which
is the whole point of the split.

### Origin

```
lib:<name>[:<ver>[:<func>]]     -> origin:lib:<name>:<ver|unknown>[:<func>]
stdlib:<...>                    -> origin:stdlib:<...>
bundle:<name>[:<func>]          -> origin:bundle:<name>:unknown[:<func>]
```

### Bare tags

Anything with an unrecognised namespace (`mirai` typed into the tag box)
becomes `user:<tag>`. They came from a human, so they belong on the User axis;
keeping the Behavior axis pure LLM output is what makes its percentages mean
anything. Anyone who meant a bare tag as an origin re-tags it as
`origin:bundle:<name>:<version>`.

## Sankey axis picker

The bin-sim graph stops hard-coding provenance x flags. The user chooses:

- **one axis** — Origin, Severity, Behavior, or User
- **a cross of any two** — 6 ordered-insensitive pairs

10 modes total. A cross renders as `shared mass -> axisA node -> axisB node`:

```
View: [ Severity  x  Behavior  v ]

  shared mass ---> severity:high ---> category:network   12.4%
              \--> severity:high ---> category:crypto     3.1%
               \-> severity:low  ---> category:util      61.0%
```

Single-axis mode drops the second column.

The first axis is not the graph's alone: it is the axis the **function tag
tree** reads, so it also scopes the tables and the Summary rollup. Each axis
gets its own tree, and each is built from that axis's own summary rows:

- **Origin** — three levels the tree adds above the rows: group (Libraries,
  Bundles, Original, Other) / library / version.
- **Behavior** — the rollup the backend already stores: `category:network` over
  its leaves `c2`, `dns`, ....
- **Severity** — flat, ordered high → none rather than by mass.
- **User** — flat, or `user:<slug>` over its leaves.

A node id is a tag id, so selecting one scopes every pane by tag prefix exactly
the way selecting `libc` always did. The graph folds where the tree folds, but
only as deep as the joint is keyed (the display parent), which is why a
behaviour group is not drillable in the graph the way a library is.

**Empty axes are not offered.** A pair whose functions were never sent to the
LLM has no severity and no behaviour rows, and one nobody has tagged has no user
rows; those axes are dropped from both pickers, and with a single axis left the
pickers disappear. Selecting an axis that later turns out empty falls back to
one that has rows rather than blanking the view.

Re-tagging is still answered by a resplit rather than a rebuild: the pair score
is the matched edges alone, and tagging only changes how that score is broken
down. Switching axes in the UI is cheaper still — see below, it is a pure
re-render.

## How the ten modes are stored

Ten modes are not ten stored matrices. A bin_sim doc holds four per-axis
summaries plus **one** sparse joint table, and every view is a marginal of that
table, summed over the axes the view does not show:

```
doc = {
  tags_summary:     [rows]   # the origin axis, under its historical name
  severity_summary: [rows]
  category_summary: [rows]
  user_summary:     [rows]
  joint: { "<origin parent>": { "<severity>\x1f<category>\x1f<user>": [8 slots] } }
  split_schema: 2
}
```

`tags_summary` keeps its name because it has always *been* the origin axis;
renaming it would churn the tree, the table and the container-sim renderer that
already read it for exactly that.

The joint's cell layout is the old `flag_matrix`'s, unchanged:
`[w_shared_a, w_shared_b, w_uniq_a, w_uniq_b, n_shared_a, n_shared_b, n_uniq_a,
n_uniq_b]`. Origin is the outer key; the other three axes are packed into the
inner key, separated by `\x1f` so a user-typed tag can never forge a key
boundary. Within one axis, a function's several tags form one combo joined by
` + ` — a flow diagram can only stay countable in whole functions if a function
lands in one cell rather than half in two.

Marginalising is `joint_marginal(joint, axis_a, axis_b)` in Python and
`fileSimJointMarginal` in JS. Switching axes in the UI needs no fetch and no
resplit. Adding a fifth axis later costs one more key segment, not six more
matrices.

**One asymmetry worth knowing.** The A side expands combos back into individual
tags so its nodes line up with the axis summary rows the rest of the pane draws.
For origin and severity that is exact — a function has one of each. Category and
user overlap, so an A column over them can exceed the pair total, exactly as
their summary rows already do and already say they do.

`split_schema` is what makes an upgrade safe. `tags_rev` alone cannot tell a doc
split by the old two-axis code from one split here — the old doc carries whatever
revision was current when it was written, which can equal today's. A doc with an
older schema is unconditionally stale and the UI offers a resplit.

## Running the migration

```
uv run python scripts/migrate_tag_taxonomy.py --demo                    # rule checks, no DB
uv run python scripts/migrate_tag_taxonomy.py --collection main --dry-run
uv run python scripts/migrate_tag_taxonomy.py --collection main
```

Idempotent — an already-migrated tag maps to itself, so a re-run after an
interruption is safe. Per collection it rewrites the `tags`/`user_tags` fields on
every file and function `:meta` doc, the `lib_tags` staging sets, the
`tags_metadata` registry (merging colour/priority/`llm` where two old ids
converge), and the file- and func-level tag index buckets — rebuilt by
re-driving `index_service.save_file` / `save_function`, so the ancestor expansion
is the real one and cannot drift from it. `tags_rev` is bumped last, so an
interrupted run never claims the splits are current.

Afterwards, run a bin_sim resplit to rebuild the per-axis summaries.

**Known gap, deliberate.** The migration does not rewrite the **sim-level** tag
index buckets (`{coll}:idx:sim:{file,func}_tags:*`). Those are built by
`save_similarity` from per-pair function and file metadata, which the script
would have to reconstruct for every pair. They keep the old id form — harmless
but stale, so a sim-level tag filter matches old ids until similarities are next
rebuilt. Nothing else reads them.

## Breaking changes

* **Saved filters and URLs using the old ids stop matching.** `func_tag=lib`,
  `file_tag=lib:uclibc*` and friends need `origin:lib`, `origin:lib:uclibc*`.
  Substring and glob users typing `*uclibc*` are unaffected. This was chosen
  over a permanent compat shim in `query_syntax.py`: a clean id space is the
  point of migrating at all.
* **`flags_summary` and `flag_matrix` are gone** from the bin_sim doc, replaced
  by the three summaries and the joint above.
* **The LLM prompt and validator changed shape.** They previously disagreed with
  each other — `llm_service.py` asked the model for `llm:<risk>:<capability>`
  and validated `^llm:...`, while `llm_batch_service.py` wrote the result under
  `flag:`. Both now go through `tag_taxonomy`.
* **Free-form LLM vocabulary tags that carry no namespace are written under
  `user:`** rather than being pushed onto an analysis axis.
