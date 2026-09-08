# Design: File sim becomes the main binary-similarity view

## Context

The binary-similarity view (`binary_similarity.js`, 3001 lines) is a POC with six
peer tabs — Matched functions, Unmatched functions, Function graph, Metadata,
Clusters, File sim. The tag hierarchy work landed in File sim
(`fileSimTree` js:466, `fileSimRows` js:544) and it is the view that actually
answers the question the tool exists for: *what mass do these two binaries share,
and is any of it interesting.*

File sim's own table is now the crowded one. It recursively renders tag nodes,
synthetic `Shared (n)` / `Unique to A (n)` / `Unique to B (n)` group nodes, and
individual function rows, all in one arbitrary-depth table
(js:484-628), with dummy leaf nodes to make `original_code` render at all
(js:502-517). It has no sorting and no filtering.

This design promotes File sim to the main view and gives it the sort/filter
capability the other tables already have, without inheriting the crowding.

## Shape

```
┌─ FUNCTION TAG TREE ────┬─ [Summary][All][Matched][Unmatched] ─────────┐
│  ▼ All          68%    │ chips: [tag: libc 2.31 ✕] [state: matched ✕] │
│      Original   12%    │ ──────────────────────────────────────────── │
│    ▼ Bundles    77%    │ state │ sim │ func A │ func B │ rarity │ tag │
│        Mirai    91%    │ match  0.98  memcpy   memcpy    0.12   libc  │
│        Other    40%    │ match  0.95  strlen   strlen    0.31   libc  │
│    ▼ Libraries  72%    │ uniq A  —    rc4_init  —        0.91   libc  │
│      ▼ libc     91%    │                                              │
│          ⚠ 2.35 drift  │  ← per-column filter row under headers       │
│        thing    55%    │                                              │
│ ─────────────────────  │                                              │
│  Metadata              │                                              │
│  Clusters              │                                              │
└────────────────────────┴──────────────────────────────────────────────┘
```

Left pane is the **scope selector**. Right pane is the **detail**, and its four
tabs mean the same thing for every possible selection — that scope-invariance is
what makes the layout work.

## One folding state

The tree, the Summary rollup, the table's groups and the Sankey all read a
single expansion set keyed by tree node id. Folding libc anywhere folds it
everywhere, so no two panes can disagree about what is being looked at, and
`Expand all` / `Collapse all` act on all four at once.

That set replaced the Sankey's private namespace-depth frontier, which could not
express "drill into libc but leave zlib folded" — a single global depth applied
to every library at once. It also removed the Namespace / Library / Version
depth buttons: depth *is* the folding now.

`Expand all` opens leaves too. A leaf has nothing to unfold in the tree, but in
the table a leaf's open state is what loads its function rows, so stopping at
nodes-with-children left every leaf empty. Earlier drafts had Libraries and Original code as
*tabs*, which forced two different scoping rules into one tab bar; they are tree
nodes instead.

## Left pane: tag tree

Groups by the first namespace segment of the tag id, which `parse_tag_id`
(`bin_sim_tags.py:66`) already returns as `type`:

| tag id | tree location |
|---|---|
| `original_code` | Original *(a leaf: it has no library/version structure, so nesting one child called "Original code" under a node called "Original" is a level that says nothing)* |
| `bundle:mirai:v1:attack_udp` | Bundles > Mirai |
| `lib:libc:2.31:memcpy` | Libraries > libc |
| `stdlib:libstdc++:11` | Libraries > libstdc++ |
| anything else | Bundles > Other |

`bundle:` does not exist in any collection today. The node renders only when
`bundle:*` tags are present, so it starts absent and fills as tagging happens.
**No migration job, and no frontend prefix-mapping table** — the grouping stays a
pure prefix read, which is why it can't drift from the data.

**Selection**: multi-select. Click a node to select it; ctrl/shift-click to add.
Selecting a parent cascades to all descendants. Selections union, and each one
becomes a removable chip in the right pane's filter bar, so scope is visible and
dismissable from either side.

**Drift** is a child node under the library it drifted from, not a top-level
`tag_mismatch` bucket. `mismatch_w[side][tag_id]` (`bin_sim_tags.py:141`) is
already accumulated per tag, so "libc has 12 drifted functions" needs no backend
change. Naming the *counterpart* ("→ 2.35") does — see Backend below.

**Metadata / Clusters** sit below a hard divider, styled as navigation rather
than tree nodes. Selecting one greys the tree without collapsing it, so returning
to a scope is one click.

## Right pane: one table, four tabs

There is **one** table implementation with a `state` column
(`match` / `uniq A` / `uniq B`). The tabs are presets on it:

| tab | state chip |
|---|---|
| All | none |
| Matched | `state: matched` |
| Unmatched | `state: uniq A, uniq B` |

Summary is the exception — stats header, then a rollup that recurses to
library-version depth with the same folding as the tree, then the Sankey
underneath it. At the root `All` node the header is the whole-pair overview
(score, coverage, function counts, architectures) and the rollup starts at
Original / Bundles / Libraries.

The Sankey lives here rather than behind a Table/Sankey toggle: it is a picture
of the composition the rollup tabulates, so it belongs under it. The toggle is
gone.

Because the tabs are chip presets, any mix a user wants — "unmatched, but only
in B, only libc, rarity above 0.8" — is reachable from the chip bar without a
tab for it.

### Grouping

The table groups by tag **when the result spans more than one tag**, and is flat
otherwise. Drilling to `libc 2.31` gives a plain list with no pointless single
header; selecting `Libraries` gives collapsed per-library groups.
A `Group by: Tag / None` toggle overrides, plus `Collapse all`.

```
▼ libc 2.31                          80 A │ 78 B │ 91%
    match  0.98  memcpy      memcpy      0.12
    match  0.95  strlen      strlen      0.31
    ▶ 2 more copies of memcpy
▶ openssl 3.0                        40 A │ 20 B │ 40%
▶ zlib 1.2.11                        12 A │ 12 B │ 100%
```

**Two levels maximum**: tag group → functions, plus the duplicate fold. The
arbitrary-depth recursion is gone. The left tree and the group headers show the
same tag names, which is not redundancy — the tree shows every tag in the pair,
the headers show only tags surviving the current filters.

Group headers render from `tags_summary`, which is already in the payload with
per-tag A/B counts and score. **Collapsed groups cost zero requests**; expanding
one fetches its first page. The known ceiling: header counts are pre-filter, so a
header reads "80 A / 78 B" (total in tag) rather than "12 rows matching your
filter". A filtered group-count endpoint is the upgrade path if that proves
confusing in practice.

**Paging is per node.** Each leaf keeps its own page state and offers
`Load 100 more … showing 100 of 250 names` when there is more. The page limit
therefore applies to the tag you opened rather than truncating the whole view,
which is what a single global list would have done.

### Duplicate fold

The `And N unmatched duplicates` affordance survives, keyed on **function name**
rather than on tag. `▶ 2 more copies of memcpy` folds a matched `memcpy`'s
unmatched namesakes under it.

Consequence: **the paging unit is the distinct function name within a group, not
the row.** Paging over rows would let a name's copies straddle a page boundary,
and sorting by rarity would scatter them so they could never be adjacent.

```
GET ?tags=lib:libc:2.31&state=all&collapse=name&sort=rarity&offset=0&limit=100
  → 100 distinct names, each a representative row + n_copies
expand → GET ?tags=lib:libc:2.31&name=memcpy
```

The representative row is the highest-scoring matched row for that name, or the
first unmatched one if none matched.

Two rules keep this from degenerating:

- **Default names never fold.** `FUN_00401234`, `sub_*`, `thunk_*` are not
  duplicates of each other; folding 800 of them into one row is noise.
- **Fold key is the A-side name, falling back to B.** Handles the stripped-A /
  symbolized-B comparison, which is the common one.

### Filtering and sorting

Two scopes, visually distinct:

- **Global chip bar** above the tabs — tag selections (from the tree), state,
  and any cross-tab filter. Persists across tab switches.
- **Per-column filter row** under the headers — refines within the current tab
  only. This is the existing pattern (`filterHtml` js:2089, `searchHtml`
  js:2097), reused as-is.

Computed **server-side**. `_page_diff` (`bin_sim.py:505`) already does
filter + sort + paginate over the three tables; this extends it rather than
adding a path.

### Table / Graph

All / Matched / Unmatched each read either as rows or as the same rows drawn as
flow, behind a `View: Table / Graph` toggle. The graph is fed by the page the
table already fetched, so the tab's state filter and the tree's scope carry over
for free — select libc on Unmatched, see libc's unmatched functions.

The graph draws **function to function, with no cluster column**. The old
Function graph tab routed every match through a cluster node (and had Detailed /
Simplified / Tags modes on top of it), which said nothing the matched pair did
not already say; unmatched functions flow to a single `No match in <file>` node
instead. The sidebar's Function graph entry is gone with it — the graph is a way
of reading a tab, not a page of its own.

## Backend work

1. **`_page_diff` gains `tags=`, `state=`, `collapse=name`, `name=`.**
   `tags` is a comma list matched by prefix (`lib:libc:2.31` catches
   `lib:libc:2.31:memcpy`). `state` selects across the three tables, so one
   endpoint serves All/Matched/Unmatched. `collapse=name` pages over distinct
   names and returns `n_copies` per row.

   **Dependency confirmed available.** `functions_metadata` carries `tags` and
   `user_tags` per function (`bin_sim.py:364-365`) and `_page_diff` already reads
   them for free-text search (`haystack`, bin_sim.py:499). The tag filter is a
   new predicate over data the function already has — no new fetch. The
   frontend's client-side tag map (js:696-726) is redundant with this and goes
   away.

   **Prerequisite bug.** `haystack` does
   `m.get("tags", []) + m.get("user_tags", [])`, which assumes a list. Tags are
   stored passthrough (`bin_sim.py:364`) and `normalize_tags`
   (`bin_sim_tags.py:31`) documents two shapes in the wild — list, or dict of
   tag → confidence — while the frontend's `getTags` (js:697-705) additionally
   defends against a bare string. A dict or string reaches `list.__add__` and
   raises `TypeError`, 500-ing the search. Route both through `normalize_tags`
   before the new tag filter starts depending on that field.

2. **Drift counterpart tags.** `add_match` (`bin_sim_tags.py:110`) knows both
   sides' tags and records only that they disagree. Add
   `mismatch_pairs[tag_id][partner_tag] += weight` so the drift node can say
   "→ 2.35" instead of an anonymous count. O(1) per already-matched edge, no
   asymptotic cost, consistent with the module's existing guarantee.

## Frontend work

**New**: tree sidebar (tree build reuses `fileSimTree` js:466), chip bar, single
grouped table renderer, group/fold expansion state.

**Deleted**:

- Top-level Matched and Unmatched tabs and their two separate renderers
- `fileSimRows` inline function rows, the synthetic Shared/UniqueA/UniqueB group
  nodes, and the `original_code` dummy-leaf hack (js:484-628)
- The old tag-keyed `And N unmatched duplicates` expander (js:562-604), replaced
  by the name-keyed fold
- The `unique_to_a` / `unique_to_b` split table bodies, subsumed by the `state`
  column
- The cluster Sankey (`renderBinaryDiffSankey`, its Detailed / Simplified / Tags
  modes, the split control, and the cluster tooltip plumbing), replaced by the
  function-to-function graph behind the Table / Graph toggle

Net line count is expected to fall.

**Never built**: a Tree / Flat layout toggle. It was on the table until the
sidebar became the tree — at which point the right pane is always flat and the
toggle has nothing to switch.

## Open

Nothing blocking. One item to schedule: the `normalize_tags` fix in `haystack`
(Backend item 1) lands before or with the tag filter, since the filter makes that
field load-bearing.
