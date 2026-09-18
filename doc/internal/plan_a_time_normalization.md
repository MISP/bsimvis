# Plan A: one timestamp representation, one timezone, real first/last seen

Part of [plan_tag_unification_overview.md](plan_tag_unification_overview.md).
Depends on nothing. Ships first because PR D's `seen` metadata depends on it.

## Context

Timestamps are written in three representations and read through a heuristic
that hides the difference.

**Unit is inconsistent at the write side.** `file.py:859` writes
`entry_date = int(time.time() * 1000)` — milliseconds, correct. The pending
stub a few lines earlier, `file.py:655`, writes `entry_date = int(time.time())`
— seconds. Both land in the same field of the same entity.

**The reader guesses.** `index_service.parse_timestamp()` (line 143) decides
seconds versus milliseconds by `val > 1e12`. That is what lets the two writers
coexist, and it is also what makes any real pre-2001 timestamp unrepresentable.

**Naive ISO strings are parsed as host local time.** The docstring says "UTC ISO
strings", but the implementation is
`datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()`. A string with
no offset — which is what most imported CSV columns carry — produces a naive
datetime, and `.timestamp()` interprets a naive datetime in the host's local
zone. The same CSV imported on two machines yields two different numbers.

**`first_seen` / `last_seen` are not timestamps at all.** They are declared
non-numeric in `INDEX_CONFIG["file"]` (`index_config.py:51-52`) with targets
`["file", "func"]`, and both CSV readers parse them with `parse_list`
(`bsimvis_upload.py:727`, `bsimvis_metadata.py:28`), producing a list of
strings. Consequences: they are indexed as tag buckets rather than numerically,
so no `min_`/`max_` range filter can reach them; and they are copied onto every
function of the file, which is a per-function rewrite for a fact that is only
true of the file.

## Decisions

1. **One representation: UTC epoch milliseconds, integer.** Not seconds, not
   ISO. Milliseconds because it is what the majority of the existing data and
   `parse_timestamp` already produce, so the backfill is smaller.
2. **One parse point, one write point.** `parse_timestamp` stays the only
   reader-side normaliser; add a `now_ms()` writer helper and use it everywhere
   a timestamp is minted.
3. **A naive ISO string is UTC.** Explicit, not inherited from the host. This is
   a behaviour change for anyone who imported naive local timestamps; it is the
   correct one, and the alternative is data that means something different per
   machine.
4. **Drop the `> 1e12` heuristic after backfill**, not before. While both units
   exist in the datastore, removing it corrupts reads.

## Changes

### A1 — writer helper and the seconds write

Add to `index_service`:

```python
def now_ms():
    """Current time as UTC epoch milliseconds — the one representation."""
    return int(time.time() * 1000)
```

Replace `int(time.time())` at `file.py:655` and every other bare
`time.time()` used as a stored timestamp. Grep target: `time.time()` assigned
to any of `entry_date`, `file_date`, `computed_at`, `created_at`.

### A2 — UTC-explicit ISO parsing

In `parse_timestamp`, after `fromisoformat`, attach UTC when the parsed value is
naive:

```python
dt = datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
if dt.tzinfo is None:
    dt = dt.replace(tzinfo=datetime.timezone.utc)
return int(dt.timestamp() * 1000)
```

### A3 — backfill

A maintenance job that walks `{collection}:all_files`, reads each `:meta`, and
rewrites any timestamp field below `1e12` by multiplying by 1000. Same pass
rewrites the numeric index buckets for those fields via the existing
`_unindex_num` / `_index_num` pair. Idempotent: a second run finds nothing
below the threshold.

Function-level metas carry `entry_date` / `file_date` too (propagated), so the
pass has to follow `{collection}:idx:file:functions:{md5}` the way
`metadata_service.propagate_metadata` does.

### A4 — promote first_seen / last_seen

- Add both to `NUM_FIELDS` (`index_config.py:163`).
- Change their `INDEX_CONFIG["file"]` targets from `["file", "func"]` to
  `["file"]`. They are facts about the file; propagating them to functions buys
  nothing and costs a rewrite per function.
- Both CSV readers stop calling `parse_list` on them and call `parse_timestamp`
  instead. A CSV that legitimately carries several sighting dates keeps the
  earliest for `first_seen` and the latest for `last_seen`, which is what the
  field names mean — a list was never the right shape.
- Add `min_first_seen` / `max_first_seen` / `min_last_seen` / `max_last_seen`
  to the filter map in `search_file.py:401`, alongside the existing
  `min_entry_date` pattern.
- Backfill: existing values are lists of strings; take min/max after
  `parse_timestamp`, drop the tag buckets for those fields via
  `_clear_indexes_via_registry`, write numeric buckets.

### A5 — remove the heuristic

Once A3 reports zero rewrites on a second run, delete the `> 1e12` branches.
`parse_timestamp` then accepts an int (already ms), or an ISO string, and
rejects anything else with 0.

## Out of scope

Display formatting and per-user timezone rendering. Storage is UTC; what the
browser shows is a separate question and does not block anything here.

## Verification

- Unit self-check on `parse_timestamp`: naive ISO, `Z`-suffixed ISO,
  `+02:00`-offset ISO and an int all round-trip to the same instant.
- `./scripts/wt-test.sh` — the file search endpoints exercise the date filters.
- Manual: upload a CSV with a naive `first_seen`, confirm
  `?min_first_seen=` filters on it.
