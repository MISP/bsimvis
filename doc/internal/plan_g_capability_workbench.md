# Plan G: capability workbench

> Since 2026-11-18, `NWAY_SEM_BUILD` is merged into `NWAY_BUILD` (one N-way Build; see the
> AGENTS.md Capabilities section). Mentions below are historical.

Status: design agreed on 2026-11-18. Nothing is built yet. UI sketches the user picked from:
https://claude.ai/artifact/K8GYtLGi6THYrid74wAhnZ (private).

Follows Plan F (`plan_f_nway_semantics_overview.md`), which built LLM-owned capability records
per N-way group. This plan replaces that model with capabilities the analyst owns. The LLM only
suggests them.

## What changes

Today a capability is written by the model. `capability_service.save()` merges any two answers
that share one id and half their anchors, so two different behaviours on one lane end up in one
record. The analyst cannot name, edit, accept or reject one.

After this plan:

- A capability is a free-text name ("Minecraft server DDoS"), optional MBC and ATT&CK ids, an
  optional description, and evidence.
- Evidence is a list of typed pointers that the UI can open:
  - functions (`md5:addr`);
  - an N-way group (its anchors);
  - a YARA, Rulezet or capa rule hit, with its function;
  - a string, with the functions that hold it.
- Nothing automatic writes to an accepted capability. The model suggests; the analyst edits and
  accepts.
- Every capability surface carries the Experimental badge (`.exp-badge`), as the YARA workbench
  does.

## Decisions

| Question | Decision |
|---|---|
| Scope of the definition | Global, like YARA rules. Evidence is per collection. |
| Storage | Own record, plus a per-file reverse index. No function tags: a tag write bumps `tags_rev` and drops every cached N-way build. |
| Where suggestions are reviewed | The bin cluster's Capabilities tab, and a drawer on the N-way page that shows the same list. There is no queue and no inbox. |
| When the LLM runs | A "Suggest" button, only for the groups the analyst selects. The result is cached per group. `NWAY_SEM_BUILD` pre-fills that cache for the TopN clusters and shows nothing on its own. |
| What the LLM may suggest | It attaches to an accepted capability first and proposes a new one only when none fits. A specific word in a name ("Minecraft") needs a pointer that holds it. Otherwise the name stays generic ("UDP flood"). |
| Accepting | One whole capability per click, after optional edits (rename, drop a pointer, add a function, fix an id). A multi-select accepts several at once. |
| Suggestions not accepted | Ephemeral. A re-suggest may bring them back. |
| Group headers | Accepted capabilities as solid chips, plus one "N suggested" badge. |
| Propagation to other files | None for now. BSim discovery of groups, regions and capability functions is future work. |
| Library page | Later. |
| Today's LLM records | Dropped after a migration check. The drop runs only after an explicit go from the user at that time. |

UI picks:

| Surface | Pick |
|---|---|
| Capabilities tab | 1A: two panes. Left: the cluster's stored groups by tier, each with a checkbox. Right: suggestions on top, then the accepted capabilities. |
| Editing a suggestion | 2A: the card expands in place. Hovering a pointer shows the code preview. |
| Selecting groups on N-way and lineage pages | 3B: a "Select groups" mode in the toolbar, off by default; Esc leaves it. |
| Results after Suggest on the N-way page | 4B: a right drawer with the same list component as the tab. |
| Tray | 5B: a bottom dock bar. It names the capability being built and its item counts, offers Accept, and opens upward into the same editor as 2A. |

## Data model

The definition is global, like `global:yara_rule:{uuid}`:

```json
{"uuid": "c7e1...", "name": "Minecraft server DDoS",
 "description": "Floods Minecraft handshake packets at port 25565",
 "mbc": ["B0033"], "attack": ["T1498.001"],
 "created": 1763460000000, "updated": 1763460000000, "user": "...",
 "history": [{"at": 1763460000000, "op": "rename", "from": "UDP flood"}]}
```

Evidence is per collection, one hash field per item:

```json
{"id": "e1", "type": "functions", "fids": ["<md5>:<addr>"], "why": "handshake builder + sendto loop", "src": "user|llm", "at": 0}
{"id": "e2", "type": "group", "anchors": ["<md5>:<addr>"], "name_id": "...", "label": "Attack floods"}
{"id": "e3", "type": "string", "value": "minecraft", "fids": ["..."]}
{"id": "e4", "type": "yara", "rule": "...", "rule_uuid": "...", "v": 3, "fids": ["..."]}
```

`type` can also be `rulezet` or `capa`. A function pointer never leaves its file (`md5:addr`),
so it survives every recluster, bin_sim rebuild and uuid change.

### Keys (Kvrocks)

| Key | Type | Holds |
|---|---|---|
| `global:capability:{uuid}` | String (JSON) | Definition |
| `global:capabilities` | ZSET | uuid -> updated (ms) |
| `global:idx:capability:label:{id}` | Set | uuids per MBC / ATT&CK id |
| `{ns}:capability:{uuid}:ev` | Hash | evidence item id -> JSON |
| `{ns}:capabilities:in` | Set | uuids with evidence in this collection |
| `{ns}:idx:capability:file:{md5}` | Set | uuids with a pointer into that file |

`{ns}` is the collection, or `global:pool:{id}` for a pool, as in `nway_sem_service`.

The file index takes the md5 of every fid in an item. For a group item, that means its anchors.
So a group pointer covers the files its anchors came from, not every file in the group. A file
that has the code only through the N-way row is shown as covered on the cluster page (from the
row model), but it is not indexed. The plan accepts that limit. The fix, later, is to index each
group's file list when the group is accepted.

Document every key in `doc/kvrocks_database_structure.md`.

### Suggestion cache (queue Redis, TTL)

| Key | Type | Holds |
|---|---|---|
| `capsugg:{ns}:{name_id}` | String (JSON) | One group's suggestions (list), `caps_v`, `at`. TTL 30 days. |

A suggestion is shaped like an accepted capability, plus `attach_to` (the uuid of an accepted
capability, or null) and `group` (the `name_id` it came from). The cache is keyed by the stored
group's name id, so a rebuild that finds the same group by its anchors finds its suggestions too.
Add the key to the AGENTS.md Redis table.

## API

New file `routes/capabilities.py`, namespace `/api/capability` in `swagger.py`:

| Route | Does |
|---|---|
| `GET /capability/list` | Accepted capabilities with evidence in `collection` / `pool`. Filters: `cluster_uuid` (files of that cluster, through the file index), `id`, `q` (name). |
| `POST /capability/accept` | `{collection\|pool, items: [...]}`. Each item either creates a capability or, with `uuid`, adds its evidence to one (attach). This is the multi-select accept. Writes are WATCH/MULTI, as in `note_service`. |
| `POST /capability/save` | One capability in full (definition plus this collection's evidence), from the editor. A rename keeps history. |
| `DELETE /capability/{uuid}` | Drops this collection's evidence. The definition goes when no collection has evidence left. |
| `POST /capability/suggest` | `{collection\|pool, cluster_uuid, groups: [name_id], refresh}`. Returns the cached suggestions. Groups with no cache, or with `refresh`, are started on the app's background naming thread (the `_name_lane` pattern, never a job). |
| `GET /capability/suggestions` | Polls the cache for `groups`; `pending` lists groups still running. |

Pointer checks on write:

- A `fid` must be `md5:addr`, and its function meta must exist (one pipelined `EXISTS` per item).
- An item holds at most 500 fids.
- An id must be in `data/capabilities/catalog.json`. The catalog is the vocabulary for analysts;
  the curated list only limits what the model sees.

## LLM suggestions (CAPS_V 5)

- Input: the structured evidence that `_lane_evidence` emits on branch `nway-evidence-regions`,
  one entry per read function: `{fid, strings, calls, findings, snippet}`. This slice waits for
  that branch to merge.
- The prompt also lists the accepted capabilities of the collection: name and ids, at most 40,
  most-used first. The model answers either `attach_to` with one of them, or a new name.
- Output: a list of up to 3 `{name, attach_to, mbc, attack, pointers, why}`, where `pointers`
  index into the structured evidence. A pointer the evidence does not hold is dropped. That rule
  replaces the quote rule in `clean()`. An id outside the curated list is dropped.
- Name check: a name word that is not a generic behaviour term must appear in a cited string or
  rule name. Otherwise the suggestion keeps its generic part and records a warning that the UI
  shows. The generic term list goes in `[capabilities]`.
- Calls stay one per group (the name call is separate and unchanged).
- `NWAY_SEM_BUILD`: `_lane_caps` writes to `capsugg:` instead of `capability_service.save()`.
- Eval: before tuning anything, the user makes about 20 capabilities by hand (slice C2). These
  become the truth set for the suggestion prompt.

## UI

All new UI goes through `escapeHtml` / `escapeAttr`; names, strings and rule names come off
samples.

- **Editor (one component, two places).** `static/js/capability_editor.js` provides the name
  input, id chips with x and "+ id" (catalog search), and the evidence list. Each pointer has an
  x, and hovering it opens `showCodePreview`. "+ add function" takes the tray's items or a pasted
  `md5:addr`. It renders inside an expanded card (2A) and inside the dock (5B).
- **Dock tray (5B).** `static/js/capability_tray.js`, mounted once in the dashboard shell. It holds
  one draft: a new capability, or an accepted one opened for edit. Its bar reads
  `⚗ Building <name> · 3 fn · 1 string · 1 id [Open ▴] [Accept]`. The draft is kept in
  `localStorage` (`cap.tray`) so a reload or a view switch keeps it. That is a per-viewer
  convenience; nothing is saved to the server before Accept.
- **"Capability ▸" submenu** in `context_menu.js` (`showGraphContextMenu`), for functions and
  groups:
  - New from this;
  - Add to "<tray draft>";
  - Suggest for this group.
  Group-header kebabs in `nway_panel.js` and the lineage Inspector call the same menu.
- **Capabilities tab (1A).** Rewrite `cluster_capabilities.js`'s `renderTab`:
  - Left pane: the stored groups from `GET /nway/semantics`, by tier, with a search and
    checkboxes, and a "Suggest (N)" bar. When the cluster has no stored set, it shows the
    existing Build button.
  - Right pane, top: suggestions, as dashed cards. Each card has a checkbox, Edit (2A), Dismiss
    and Accept, and an attach suggestion reads "+1 group → UDP flood". Above the cards: "Accept N
    selected" and "Re-suggest".
  - Right pane, below: the accepted capabilities with this cluster's files (`/capability/list`).
  The overview card (`card` / `loadCard`) shows the accepted capabilities instead of LLM ids.
- **Select groups mode (3B).** A `.lin-seg` toggle in the N-way table toolbar (`nway_panel.js`)
  and in the lineage page's toolbar. It shows checkboxes on group headers and pane rows, and a
  sticky bar: "N groups · Suggest capabilities · Done". Esc leaves the mode (the lineage page's
  `onEsc` order). The selection stays on its page; the tab has its own.
- **Suggestions drawer (4B).** On the N-way page, Suggest opens a right drawer. It renders the
  tab's right-pane component (`ClusterCapabilities.suggestPane(el, ctx)`), filtered to the
  selected groups, with the same bulk accept.
- **Header chips.** `nway_groups.js` drops the LLM id and trait chips (lines ~198-199). It shows
  accepted capabilities as solid chips (from evidence whose group `name_id` matches) and a
  "N suggested" badge from `capsugg:` that opens the drawer.

## Slices

1. **C1: store and routes.** Add the keys, the `routes/capabilities.py` endpoints except suggest,
   a service self-check (`uv run python -I bsimvis/app/services/capability_service.py`) and API
   checks in `test_api_endpoints.py`. The old LLM path stays as it is.
2. **C2: editor, dock tray, submenu, tab with hand-made capabilities.** UI only on top of C1.
   The user builds about 20 capabilities by hand.
3. **C3: suggestions.** Starts after `nway-evidence-regions` merges: rebase this branch first.
   Adds the suggestion cache, `POST /capability/suggest` and `GET /capability/suggestions`, the
   CAPS_V 5 prompt and clean rules, the `NWAY_SEM_BUILD` pre-cache, and the suggestions in the
   tab with multi-select accept. Eval against the C2 truth set.
4. **C4: N-way and lineage surfaces.** The Select groups mode, the suggestions drawer and the
   header chips.
5. **C5: retire the old records.**
   - A dry-run script prints, per collection, the number of `{ns}:capabilities` records,
     `idx:capability:label:{id}` sets and name records holding `caps`.
   - After the user's go: delete them. The label keys come from the catalog ids, so no `KEYS` or
     `SCAN` is needed.
   - Then remove `save()` / `unlink()` / `_attach_caps` and point `GET /nway/capabilities` at the
     new list.

Later, not planned in detail:

- the `/capabilities` library (search, rename, merge);
- BSim discovery of capability functions in other files;
- accepted capabilities on the function and file views;
- a files × capabilities matrix on the cluster tab;
- MISP export of accepted ids;
- "Write YARA for this", seeding the workbench with the capability's functions.

## Conflicts with parallel work

- `nway-evidence-regions` (handoff `handoff_nway_evidence_regions.md`) edits `_lane_evidence`,
  `_annotate` and `nway_region_service`. It does not touch `capability_service` or bump CAPS_V.
  C1, C2 and C4 do not touch those functions. C3 edits `_lane_caps`, next to them, so it waits for
  the merge.
- `nway_group_service.py` and `nway_groups.js` are hot files on the N-way branches. Keep each
  slice's diff there small and rebase before each slice.

## Tests

- `./scripts/wt-test.sh --only <step>` with `Failed : 0` before each commit of C1, C3 and C5.
- UI slices: `node --check` on each changed file, `node scripts/test_xss_escaping.js`, and
  `node scripts/test_nway_lineage.js` when lineage code changes.
- The test fixture never forms clusters. Tab and suggestion checks use a direct service
  self-check, or an N-way set over the fixture files passed by md5.
