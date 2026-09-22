# Plan B: one namespace policy table, and namespace reservation by writer

Part of [plan_tag_unification_overview.md](plan_tag_unification_overview.md).
Depends on nothing. PR C and PR D both depend on it.

## Context

The taxonomy in `tag_taxonomy.py` describes what a namespace *means*. Nothing
describes what writing into it *costs*, and nothing restricts who may write
into it. Both gaps are live problems today, before any new namespace is added.

**Every tag becomes a vocabulary row, automatically.** `_index_tag`
(`index_service.py:236`) ends with:

```python
if "tags" in field:
    meta_key = f"{coll}:tags_metadata"
    pipe.hsetnx(meta_key, str(v), json.dumps({"priority": 0}))
```

Any value of any field whose name contains `tags` is registered. That includes
`rulezet:<uuid>` — one row per mirrored rule, against a 129k-rule mirror — and
every `yara:<category>:<family>#<rule_name>`, where the detail tail makes the
id unique per rule.

**The vocabulary listing is linear in that hash.** `list_tags`
(`routes/tags.py:308`) iterates every row from `get_collection_tags` and calls
`get_tag_stats` for each, and `get_tag_stats` (`tag_service.py:763`) issues six
`SCARD`s — three levels times `tags` and `user_tags`. The endpoint is therefore
`O(vocabulary x 6)` round trips with no pagination before the loop. The `q`
filter helps only when one is passed.

**Nothing validates a namespace on write.** Two paths:

- `tag_service.add_user_tag` (line 204) strips whitespace and writes. An
  analyst can type `fid:libc:2.31`; it lands in `user_tags`, satisfies
  `bin_sim_tags.is_library_tag`, and shifts the code-versus-library split of
  every pair score that function participates in.
- `routes/file.py:711` builds the analysis `tags` field from
  `request.args.getlist("tags")`. An uploader can put anything, including an
  `origin:` or `fid:` tag, straight into the field reserved for analyser
  output.

The natural reaction — "keep an allowlist of namespaces a writer may use" — is
the wrong control, because the interesting namespaces need different answers to
different questions. `ip:` should be searchable but must not enter the
vocabulary listing. `rulezet:` should be searchable but must not be propagated
or aggregated. `fid:` must be propagated to functions. One flat list cannot say
any of that.

## Decisions

1. **One policy table, keyed by namespace, in `tag_taxonomy.py`.** It subsumes
   `TAG_AXES`, which becomes a derived view so there is one table rather than
   two that can disagree — the same reason the module exists at all.
2. **Four independent switches**, because they are four different costs:

   | switch | what turning it on costs |
   |:---|:---|
   | `index` | one bucket key per value plus one per ancestor, plus a registry entry |
   | `propagate_func` | a rewrite of every function meta of the file (`metadata_service.py:184-250`) |
   | `vocabulary` | a row in `{coll}:tags_metadata`, and six `SCARD`s per `list_tags` call |
   | `aggregate` | an entry in each cluster's distribution (PR C) |

3. **Writers are named in the table, not in the call sites.** A namespace
   declares which producers may write it. `analysis`, `user`, `import`,
   `rulezet`, `fid`.
4. **`user` is provenance, not a semantic namespace.** `user_tags` already
   records that a human or LLM wrote a value. New bare user values therefore
   stay bare (`reviewed`, not `user:reviewed`) and use the user axis only as a
   source-aware fallback. Do not add `human:`, `analyst:` or another writer
   namespace.
5. **An unknown namespace is not rejected.** It is accepted and indexed, but
   gets the neutral/default policy until a namespace policy is added. Unknown
   values must not be silently rewritten as `user:<value>` merely because their
   producer is a user. Rejecting them would lose data; treating them as user
   semantics would confuse provenance with meaning.

### The `user:` compatibility rule

`user:foo` is a legacy spelling of a bare user value, not a namespace to emit
for new data. Reads and filters continue to accept it. Display may strip the
prefix, and a later migration may convert it to `foo`, but no blind rewrite is
part of Plan B because a collection can contain both static `foo` and user
`user:foo` values.

The distinction must survive places that currently merge `tags` and
`user_tags` (notably binary-similarity scoring):

```text
tags:      ["yara:trojan:mirai"]
user_tags: ["reviewed", "category:network:c2"]

yara:trojan:mirai   -> yara axis
reviewed            -> user axis (bare-value fallback from user_tags)
category:network:c2 -> category axis
```

The minimal implementation is to preserve the source field while merging tag
values for axis/scoring decisions. A recognized namespace wins; a bare value
from `user_tags` uses the user axis. The stored fields remain the provenance
boundary, and a user-written namespaced value is still user-written even when
it participates in another semantic axis.

## Changes

### B1 — the table

```python
# namespace -> policy. One row per namespace; `axis` is the question it
# answers, the four flags are what writing into it costs.
NAMESPACE_POLICY = {
    "fid":       Policy(axis="origin",   index=True,  propagate_func=True,  vocabulary=True,  aggregate=True,  writers=("analysis",)),
    "bsim":      Policy(axis="origin",   index=True,  propagate_func=True,  vocabulary=True,  aggregate=True,  writers=("analysis",)),
    "yara":      Policy(axis="yara",     index=True,  propagate_func=False, vocabulary=True,  aggregate=True,  writers=("analysis",)),
    "rulezet":   Policy(axis="ruleset",  index=True,  propagate_func=False, vocabulary=False, aggregate=False, writers=("analysis",)),
    "capa":      Policy(axis="capa",     index=True,  propagate_func=False, vocabulary=True,  aggregate=True,  writers=("analysis",)),
    "mitre":     Policy(axis="mitre",    index=True,  propagate_func=False, vocabulary=True,  aggregate=True,  writers=("analysis",)),
    "av":        Policy(axis="family",   index=True,  propagate_func=False, vocabulary=True,  aggregate=True,  writers=("import", "analysis")),
    "ip":        Policy(axis="ioc",      index=True,  propagate_func=False, vocabulary=False, aggregate=False, writers=("import", "analysis", "user")),
    # No `user` namespace row: bare values from user_tags use the source-aware
    # fallback axis. Keep a read-only compatibility alias for legacy user: ids.
    ...
}
```

`TAG_AXES` becomes `{ns: p.axis for ns, p in NAMESPACE_POLICY.items()}` plus the
two synthetic ids (`original_code`, `tag_mismatch`) it already special-cases.

### B2 — enforcement, in two places only

- `_index_tag` consults `vocabulary` before the `hsetnx`. This alone stops the
  rulezet uuid flood and is worth shipping on its own.
- `index_config.get_propagated_fields` — or the tag branch of it — consults
  `propagate_func` per tag value rather than per field. A file's `tags` list
  can hold both `fid:` (propagate) and `yara:` (do not), so the decision is
  per value.
- Writer checks go in `tag_service.add_user_tag` / `bulk_add_user_tag`
  (producer `user`) and in whatever assembles the analysis `tags` field
  (`ghidra_job.py:683`, `routes/file.py:711`, producer `analysis`/`import`).
  A rejected tag is dropped with a log line, not an error: a client sending one
  bad tag among twenty should not lose the other nineteen.

### B3 — the two new namespaces

**`av:`** — the AV label as it arrives is a signature id, not a family:
`Unix.Trojan.Mirai-7100807-0` differs per signature revision, so used as a level
it mints a bucket per revision. Use the existing `TAG_DETAIL` convention:

```
av:clamav:mirai#Unix.Trojan.Mirai-7100807-0
```

Family groups and colours; the full label stays searchable and displayed and
never becomes a bucket. This mirrors exactly what `yara_tag` already does with
the rule name, and the reasoning is written out at `tag_taxonomy.py:650`.

Parsing vendor labels into `(vendor, family)` is a heuristic and will be wrong
sometimes. `unknown` is a valid family, the same way `yara:unknown:unknown:`
is expected rather than a parsing bug to chase.

**`ip:`** — flat, `ip:143.20.185.245`. Indexed, not in the vocabulary, not
aggregated into clusters, and excluded from the LLM prompt context. It is an
IOC list, not a grouping axis. Its own axis (`ioc`) exists so it can never
compete for mass on `family` or `origin` in a Sankey.

### B4 — backfill

One maintenance pass that deletes the `tags_metadata` rows whose namespace has
`vocabulary=False`. Index buckets are untouched: the tags stay searchable, they
just stop appearing in the tag list. Reversible by re-running a registration
pass, so this is not a destructive migration.

## Out of scope

Retiring any metadata field — that is PR D. Cluster aggregation — PR C. This PR
changes no existing tag id.

## Verification

- Self-check in `tag_taxonomy.demo()`: every namespace in `TAG_AXES` has a
  policy row and vice versa; every policy `axis` is a known axis.
- A unit check that `add_user_tag("fid:libc:2.31")` is refused and
  `add_user_tag("mirai")` still becomes `user:mirai`.
- `./scripts/wt-test.sh` — tag add/remove/list endpoints are in the suite.
- Manual: `list_tags` on a collection with the rulezet mirror loaded returns a
  vocabulary that no longer contains uuids.
