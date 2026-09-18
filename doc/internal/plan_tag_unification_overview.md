# Plan: unify metadata and tags — overview and sequencing

Four plans, one goal: retire the ad-hoc metadata fields (`avtype`, `yara`,
`cc_ip`, `filetype`, `inferred_*`) in favour of the namespaced tag vocabulary,
so that any analysis module, any external source and any analyst writes one
kind of fact through one write path.

They are separate PRs on purpose. Each one is shippable alone, and each later
one depends on the earlier ones landing.

| PR | Title | Depends on | Plan |
|:---|:---|:---|:---|
| A | Time normalization; real `first_seen` / `last_seen` | — | [plan_a_time_normalization.md](plan_a_time_normalization.md) |
| B | Namespace policy table; writer reservation | — | [plan_b_tag_namespace_policy.md](plan_b_tag_namespace_policy.md) |
| C | Cluster tag aggregation; inferred tags | B | [plan_c_cluster_tag_aggregation.md](plan_c_cluster_tag_aggregation.md) |
| D | Retire the metadata fields | B, C | [plan_d_retire_metadata_fields.md](plan_d_retire_metadata_fields.md) |

A and B are independent of each other and can run in parallel.

## What is already built

Half of the target already exists, which changes what the remaining work is.

- **`yara:` is already a namespace.** `tag_taxonomy.yara_tag()` mints
  `yara:<category>:<family>#<rule_name>`, and `ghidra_job.py:683` already
  writes those onto the file's `tags` on every analysis. YARA scan results are
  tags today.
- **The `yara` *metadata field* is not scan output.** It is a CSV column read at
  upload (`bsimvis_upload.py:731`), like `avtype` and `cc_ip`. It carries
  imported analyst or VT data. Two different facts share one name; only the
  imported one is being retired.
- **The tag id shape is settled.** `canonical_tag_id` folds case on levels,
  keeps the `#` detail tail intact, and gives an un-namespaced tag the `user:`
  namespace. `tag_prefixes` decides which prefixes become index buckets.
  Nothing new should parse tag ids.
- **`first_seen` / `last_seen` already exist** — as string lists, which is the
  problem PR A fixes.

## The rule that decides tag vs metadata

> If a value's cardinality grows with the size of the corpus and it is not a
> class you group by, it is metadata, not vocabulary.

Stays metadata: `type`, `status`, `language_id`, every count, every md5 and
filename field, `entry_date` / `file_date` / `first_seen` / `last_seen`.

Becomes a tag: AV family, YARA family, packer, container kind, MITRE technique,
capa capability, CVE.

The awkward case is `ip:`. A C2 address is a class you filter by, but its
cardinality grows with the corpus and it will never be a useful grouping level.
It is indexed and searchable, and excluded from the vocabulary listing, the LLM
prompt and cluster aggregation. That split is exactly why PR B replaces the
idea of a namespace allowlist with a per-namespace policy table: "may this be
written" is the wrong question, "what does writing it cost" is the right one.

## The failure mode being designed against

Inferred labels must never become ordinary tags. A cluster label written back
onto its own members as a plain tag is read by the next clustering run, by
`bin_sim_tags` when it splits a pair score, and by the LLM prompt
(`llm_tools.py:356`) — all of which cannot tell it apart from evidence. The
`inferred_*` fields are in `POOL_LOCAL_FIELDS` (`index_config.py:354`) today
precisely so a cluster label never crosses into a pool. PR C keeps that
property by giving inferred facts their own namespace and their own axis.
