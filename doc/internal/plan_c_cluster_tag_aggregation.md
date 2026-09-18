# Plan C: cluster tag aggregation, and inferred tags that stay inferred

Part of [plan_tag_unification_overview.md](plan_tag_unification_overview.md).
Depends on PR B (the namespace policy table supplies the `aggregate` switch and
the axis of each namespace).

## Context

Two things are called "cluster tags" and neither is what is wanted.

**Cluster tags today are human annotation only.** `tag_service.add_user_tag`
with `entity_type="bin_cluster"` writes into a hash,
`{collection}:cluster_tags`, keyed by `"{entity_type}:{algo}:{cluster_uuid}"`.
Nothing derives a cluster tag from its members.

**Member summaries exist but only for four hardcoded metadata fields.** Each
cluster's `:meta` carries `yara_distribution`, `avtype_distribution`,
`filetype_distribution`, `ccip_distribution` (and filename/md5 on some paths),
built by `cluster_utils.build_freq` over values collected by
`cluster_utils.collect_member_values`.

`collect_member_values` is hardcoded to those six fields. The helpers were
unified; the call sites were not. They are:

- `bin_cluster_service.py:602` — threshold path, writes distributions and
  indexes the top value as `inferred_*`
- `bin_cluster_service.py:1625` — hierarchical path, unindexes on retire
- `bin_cluster_service.py:1912` — hierarchical path, writes
- `metadata_service.py:329` — recompute after a metadata propagation
- `search_file.py:641` — computes `inferred_meta` for one file at read time

Adding a fifth distribution means five edits. Turning the four into "whatever
tags the members carry" means five edits to code that must agree exactly or the
same cluster reports different percentages depending on which path wrote last.

**The cohesion gate already disagrees between paths.** `metadata_service.py:337-341`
blanks all four distributions when `cohesion_score < clustering.min_cohesion`
(default 0.5). `bin_cluster_service.py:482-492` deliberately does not gate, with
a comment explaining that at the default `bin_uf_threshold` of 0.1 a threshold
cut's average pair sits well under `min_cohesion`, so gating would blank almost
every inferred value. So today, whether a cluster shows inferred labels depends
on which code path last wrote its `:meta`. That has to be resolved before
anything is built on top of it.

## Decisions

1. **Collapse the five call sites to one function first, in its own commit.**
   It is a refactor with no behaviour change and it is what makes the rest of
   this PR a small diff.
2. **Aggregate on tag prefixes, not full tag ids.** `tag_prefixes` already
   knows where the instance tail begins; without it,
   `yara:trojan:mirai#rule_a` and `yara:trojan:mirai#rule_b` are two rows of
   one member each instead of one row of two.
3. **Top-5 per axis, not top-5 overall.** A binary carries far more origin tags
   (one per library function) than family tags. A single global top-5 is all
   libc, every time, and the family signal — the reason the feature exists —
   never appears.
4. **The denominator stays the member count.** `build_freq`'s docstring already
   fixes this: a file carrying two YARA hits contributes two entries to a list
   whose percentages are shares of *members*. That must not change, or two
   writers of the same cluster disagree.
5. **Resolve the cohesion gate one way: report, do not blank.** Every
   distribution row already carries its cluster's `cohesion_score` by virtue of
   living in that cluster's meta; the consumer decides. Blanking loses the
   information and, per the comment at `bin_cluster_service.py:482`, blanks
   almost everything at real thresholds. The UI applies `min_cohesion` at read
   time, where it is already a request parameter (`search_file.py:635`).
6. **Inferred tags get their own namespace and axis.** A label derived from a
   cluster is written as `inferred:<original namespace>:<body>`, for example
   `inferred:av:clamav:mirai`. It is never merged into `tags`.

Decision 6 is the load-bearing one. If a cluster-derived label is written as an
ordinary tag it is read back by:

- the next clustering run, which then partly clusters on its own previous output
- `bin_sim_tags`, which would let an inferred origin tag move mass in a pair
  score split
- the LLM prompt (`llm_tools.py:356`), where it is indistinguishable from
  evidence, against the explicit instruction in `analysis_rules()` to treat
  supplied tags as untrusted hints

The existing `inferred_*` fields are in `POOL_LOCAL_FIELDS`
(`index_config.py:354`) so that a cluster label computed in one collection
never enters a pool whose similarity graph is different. The `inferred:`
namespace inherits that: policy `aggregate=False` (an inferred tag must never
be re-aggregated into another cluster's distribution), `propagate_func=False`,
and excluded from pool merges.

## Changes

### C1 — collapse the call sites (no behaviour change)

One function in `cluster_utils`:

```python
def cluster_summary(metas, member_count, fields=DISTRIBUTION_FIELDS):
    """Every distribution for one cluster, from its members' metas."""
```

It returns the dict of `*_distribution` keys the callers write today. All five
sites call it. Verified by comparing a cluster's `:meta` before and after on a
test collection — the blobs should be byte-identical.

### C2 — tag distribution

Extend the same function to also produce `tag_distribution`, shaped per axis:

```json
"tag_distribution": {
  "family": [{"value": "av:clamav:mirai", "count": 47, "percent": 94}, ...],
  "yara":   [{"value": "yara:trojan:mirai", "count": 44, "percent": 88}, ...],
  "origin": [{"value": "origin:lib:uclibc", "count": 50, "percent": 100}, ...]
}
```

Values are prefixes from `tag_prefixes`, counted once per member per prefix — a
member with three `origin:lib:uclibc:*` tags contributes one, not three, to the
`origin:lib:uclibc` row. Namespaces with `aggregate=False` are skipped, which
is what keeps `rulezet:` uuids and `ip:` out.

Size bound: axes times five rows. Cluster `:meta` growth is bounded and small.

### C3 — write inferred tags

Where `bin_cluster_service` currently indexes the top value of each
distribution as `inferred_yara` / `inferred_avtype` / ..., it instead writes
`inferred:<ns>:<body>` tags into a dedicated `inferred_tags` field on the
member's meta. Separate field, not `tags`, so that every existing consumer of
`tags` is unaffected and the separation cannot be lost by a careless merge.

`inferred_tags` is added to `INDEX_CONFIG["file"]` with target `["file"]` and to
`POOL_LOCAL_FIELDS`.

### C4 — cluster-level user tags keep working

`{collection}:cluster_tags` is untouched. A human tag on a cluster and a derived
distribution over its members are different facts and stay in different places.

## What is deliberately not done

**Propagating a cluster's tags down to members as ordinary tags.** That is the
failure mode this plan is shaped around. A member with no YARA hit does not
acquire a YARA tag; it acquires an `inferred:` tag that every downstream
consumer can tell apart.

## Verification

- `cluster_utils.demo()` extended: a fixture of member metas with known tags
  produces a known `tag_distribution`, including the per-member-once rule and
  the per-axis top-5 cut.
- Before/after comparison of cluster `:meta` blobs across C1 on a real
  collection.
- `./scripts/wt-test.sh`. Note the fixture collection never forms clusters, so
  the suite proves the endpoints still answer, not that aggregation is correct
  — C2 needs the demo check and a manual run on a real collection.
