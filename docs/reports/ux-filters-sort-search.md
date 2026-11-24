# UX report: sticky filters, sorting and search in the list views

Reviewed in the `nway-cfg` worktree on a live stack, then by reading the code. Examples use `$BSIMVIS` for the host and `$COLL` for a collection.

**Filters outlive the view that set them, and nothing on screen says so.** Sorting has a second, smaller problem: several column headers show a sort arrow for a sort the server does not do.

## What was checked, and how

Not everything the task asked for was covered. The machine rebooted mid-review, so the sweeps stopped.

| Area | How | Status |
|---|---|---|
| Filter persistence across tabs/navbar | live (browser) + code | done |
| Footer filter pill, Filters toggle, defaults | live + code | done |
| Functions: every sortable header | live, plus `/api/function/search` | done |
| Files: sortable headers | live, plus `/api/file/search` | done |
| Clusters / bin-clusters sorts | code only | not run |
| Features, strings, batches, similarities, bin-sim, jobs sorts | planned sweep | **not run** |
| Keyword search (`q`) and per-column filter semantics | probe script written, never ran | **not run** |

I won't call those views fine until a sweep has run on them.

## 1. Why filters stay

Every refresh of a list view writes its query string to `localStorage` under `savedFilters:{collection}:{view}` (`dashboard.js:1035`, and again on clear at `dashboard.js:3515`). The key never expires and nothing else deletes it.

Two places read it back and put it into links:

- the tab strip under the breadcrumb, `UI.EntityTabs.savedQuery` (`ui_components.js:135`)
- the older `updateNavbarLinks` (`dashboard.js:1292`)

So the Functions tab's `href` already carries last week's filters. Checked live: typed `init` in the function-name filter, went to Files, clicked Functions again. The tab link had become

```
/functions?min_cohesion=0.5&function_name=init&min_features=0&limit=100&score_axis=code
```

and the page opened filtered. The same collection typed straight into the address bar opens at bare `/functions`. Same view, two different results, depending on how you arrived.

**The global BSimVis menu and the tab strip disagree.** The menu (`Sidebar.buildNavUrl`, `ui_components.js`) always opens clean URLs. The tabs restore. A user who learns one behaviour gets surprised by the other.

### What makes it hard to notice

- **The only signal is a small pill at the bottom left of the table** ("4 filters"). The detail is a tooltip. On a table that fills the screen nobody looks there.
- **The pill counts defaults.** On a fresh browser profile the Functions view already says "2 filters": `min_cohesion=0.5` and `score_axis=code`, neither set by the user. `applyAdvancedFuncSearch` forces `params.set('min_cohesion', minCohesionFlt || '0.5')` on every function search. Files showed "1 filter" on a 2-file collection for the same reason. A count that is never zero stops carrying information.
- **The Filters toggle hides the filter row and remembers it globally** (`filtersCollapsed`). It has no badge. Collapse it once and every view can be filtered with nothing visible except the pill.
- **Filled inputs have no label.** The grey placeholder is the only label, and typing replaces it. A bare `0.5` in the Clusters column or a `0` with an × beside it is not readable as "min cohesion 0.5" or "min features 0".
- **"Scoring Metric: Code" is a filter that looks like a tab.** It is selected by default, saved per view, and carried through the tab links.
- **Empty results say "No data found"** and nothing about filters or how to clear them.

### The only way out is undiscoverable, and it is not a real clear

Clicking the tab you are already on calls `clearFilters()` (`dashboard.js:1360`). Nothing tells you that. And it does not clear everything: it keeps `algo` and `view`, sets `min_cohesion=0.95` on the cluster views, and falls back to the defaults above. The pill still shows a count afterwards.

### Context leaks, not filters but the same complaint

- `lastCollectionContext` / `lastPoolContext` in `localStorage` stand in when the URL has no scope. Scan, N-way diff and Jobs links then open with `?collection=` of whatever was visited last.
- `bsimvis_search_history` keeps every search forever.

Not confirmed visually: the footer pill on module views. Reading the DOM on Overview, Upload and Searches still returned the Functions pill text ("4 filters"). The footer may be hidden there; I did not screenshot it.

## 2. Sorting

Clicking a header sets `sort_by` and `sort_order`, cycles asc, desc, then unsorted, and the arrow follows. That part works. The problems are in what the server does with the key.

### Functions: four headers sort nothing

On a large collection, `Address`, `File Name`, `MD5` and `Language` give the same rows for asc and desc:

```bash
for o in asc desc; do
  curl -s "$BSIMVIS/api/function/search?collection=$COLL&sort_by=file_name&sort_order=$o&limit=4"
done
```

Both calls return the same first four functions. Same for `entrypoint_address` and `language_id`.

Cause: `search_function.py:606` only sorts on a numeric ZSET `{coll}:idx:func:{field}`. When that key is empty it falls back to id order without saying so. Numeric columns work: Feat, Callers, Callees and Date came back ordered in both directions. The text columns have no ZSET.

### Files: the Funcs sort does not match the Funcs column

```bash
curl -s "$BSIMVIS/api/file/search?collection=$COLL&sort_by=function_count&sort_order=desc&limit=8"
```

returns `function_count` 8148, 7879, 8007, 7726, 6363. The order follows one number and the column shows another. `sort_doc_ids` ranks off the `function_count` ZSET, while the row's value is a `SCARD` of the file's indexed functions (`search_file.py:259`). The ZSET is filled from the count recorded at ingest (`processing_service.py:120`), so any file whose indexed set differs from that count lands out of order. I have not checked which files differ. That is the most likely cause.

Also on Files: only `Funcs` and `Entry Date` are sortable in the table. The server already sorts `file_name`, `language_id`, `filetype`, `avtype`, `bsim_features_count`, `cohesion_score` and `file_date`. File name is the sort a user reaches for first.

### Clusters (from the code, not run)

`Name` and `UUID` on the cluster and bin-cluster lists fall into the final `else` of the sort (`cluster.py:888`, `bin_cluster.py:725`), which sorts by `cluster_id`. The arrow says name, the order is by internal id.

### A reading I got wrong first

The first Entry Date sweep showed non-monotonic dates. Re-run with a longer wait it was correct, so that was my harness reading too early. My 1.6 s wait was not enough for that sort to land, so it is slow on a large collection, and the only loading cue is a dimmed table.

## 3. Search

Checked: the function-name column filter returned the 2 expected rows for `init`. Not checked: what the keyword box (`q`) matches, case and wildcard handling, bad numeric input, `offset` past the end, `limit=0`. The probe script exists (`qprobe.py`, out of tree) and covers these on the API. It was queued when the machine went down.

Structural issues from reading the code:

- Every list view has three entry points: the keyword box, per-column filters, and the top-right Search button with its history. The keyword box and the column filters combine silently. A user who typed `q=foo` and then types in a column filter is filtering on both.
- `autofocus` is on every keyword box, so opening a view puts the cursor into search.
- Placeholders differ between views ("Search by keywords...", "Search strings (use * for glob)...", "Search similarities by keywords..."). The glob hint exists on one view only.

## 4. Plan

Ordered by value for effort. Each step is its own small commit.

**P0, make filters visible and stop the surprise**

1. **Open every tab clean.** Stop reading `savedFilters` in `savedQuery` and `updateNavbarLinks`. If keeping "back to where I was" matters, move it to `sessionStorage` so it dies with the tab, and show it (step 2).
2. **Active-filter strip under the tabs.** One chip per non-default filter, with a label and an ×, plus "Clear all". Hidden when there are none. This makes the Filters-toggle hiding harmless.
3. **Count only real filters.** One table of per-view defaults (`min_cohesion`, `score_axis`, `limit`, `min_features` lock) and one comparison function. The footer pill and the toggle badge both use it.
4. **Make Clear mean clear.** Drop `sort_by` / `sort_order` too, keep only `view`. Rename the active-tab trick or remove it; the strip's "Clear all" replaces it.
5. **Empty state names the filters.** "No results for 3 filters. Clear all."
6. **Decide `min_cohesion=0.5` on files and functions.** If the server ignores it without a cluster filter, stop sending it.

**P1, sorting**

7. **Functions text columns.** Smallest fix: drop `sort:` from Address, File Name, MD5 and Language in the `routes` table, so no arrow shows. Better, if the use is real: file name and language through the same tag-bucket walk `_sort_by_tag_index` already does for files.
8. **Files.** Take the displayed count and the sort from one source, then add File Name, Language and Cohesion headers, which the server already sorts.
9. **Clusters.** Add `cluster_name` and `cluster_uuid` branches to the sort.
10. **Say when the server ignores a sort key.** Return `sort_by` actually applied, or a 400, instead of falling back to id order.

**P2, keep it from coming back**

11. **Extend `test_api_endpoints.py`** (no new test file): for each list endpoint and each sortable header in the `routes` table, call asc and desc and assert the two orders are reversed and monotonic on the returned values. This is the test that would have caught steps 7 to 9.
12. **Run it on the git-tracked fixtures, not the seeded collection:** `./scripts/wt-test.sh --only sort`.
13. **Finish the sweeps listed as not run in the first table**, one view at a time, on a fixture collection.

**P3, polish**

14. Keep a visible label once a column filter holds a value.
15. One keyword-box placeholder style, and say what it matches.
16. Show a spinner during slow sorts, not only a dimmed table.

Suggested first commit: steps 1 and 2 together, since removing the restore without the strip would just lose the "where was I" for people who rely on it.

## Resource note

The review was not read-only on the machine: it ran a browser sweep over a 400k-function collection and several large `sort_by` calls (each walks a whole ZSET). The machine rebooted during it. I don't know the cause and did not check. For the follow-up, use the small fixture collection and a stack with one worker.
