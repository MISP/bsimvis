# Plan D: retire the ad-hoc metadata fields

Part of [plan_tag_unification_overview.md](plan_tag_unification_overview.md).
Depends on PR B (namespaces for `av:` and `ip:`) and PR C (cluster aggregation
and the `inferred:` namespace). Ships last.

## Context

Six file-level fields hold facts that the tag vocabulary can hold better, and
six more hold the cluster-derived versions of them.

| field | written by | becomes |
|:---|:---|:---|
| `avtype` | CSV upload (`bsimvis_upload.py:730`, `bsimvis_metadata.py:31`) | `av:<vendor>:<family>#<label>` |
| `yara` | CSV upload (`bsimvis_upload.py:731`) | `yara:<category>:<family>#<rule>` |
| `cc_ip` | CSV upload | `ip:<address>` |
| `filetype` | CSV upload | `type` metadata, or a `format:` namespace |
| `file_names` | upload and unpack | stays metadata (identity, not vocabulary) |
| `inferred_yara`, `inferred_avtype`, `inferred_filetype`, `inferred_ccip` | `bin_cluster_service` | `inferred_tags` (PR C) |
| `inferred_filename`, `inferred_md5` | `bin_cluster_service` | stay as they are |

Note what is *not* here. The `yara` metadata field is not YARA scan output —
scan output has been written as `yara:` tags since `ghidra_job.py:683`. The
field carries imported analyst or VT data that happens to share the name. Only
the imported one is retired; the scan path is untouched.

`inferred_filename` and `inferred_md5` stay as fields. A filename and an md5
are identity, not a class you group by, and as tags they would mint one
vocabulary entry per file in the corpus. This is the cardinality rule from the
overview, applied.

## The size of the read surface

Roughly 52 references across seven JS files:

```
19  dashboard.js
10  binary_similarity.js
 9  bin_cluster_views.js
 6  previews.js
 4  similarity_graph.js
 2  table_renderers.js
 2  cluster_views.js
```

Plus, on the backend: `search_file.py` (filter map at line 401, projection at
485, inferred computation at 641), `bin_cluster.py` (515, 598, 916),
`llm_tools.py:356`, `llm_service.py`, `processing_service.py:286`,
`metadata_service.py` (the `list_fields` list at line 118 and the recompute at
329), `cluster_utils.collect_member_values`, `index_config.py` (`INDEX_CONFIG`,
`SUBSTRING_FIELDS`, `POOL_LOCAL_FIELDS`), the swagger model, and three scripts
in `scripts/`.

That is too much for one change, which is why this is staged rather than a
single cut.

## Decisions

1. **Dual write for one release.** The CSV readers write both the field and the
   tag. Nothing that reads the field breaks while the UI is migrated.
2. **Flip reads per view, not all at once.** Each JS view moves to reading tags
   in its own commit, and can be reverted alone.
3. **Delete the fields last**, once no reader remains, in one commit that also
   drops the index buckets via `_clear_indexes_via_registry`.
4. **The filter API keeps working by alias.** `?avtype=mirai` maps to a tag
   filter on `av:*:mirai` rather than 404-ing. Old permalinks and saved
   searches keep resolving; there is no reason to break them for a rename.

## Stages

### D1 — dual write

In both CSV readers, for each of `avtype`, `yara`, `cc_ip`, emit the tag
alongside the field. Producer is `import`, which PR B's policy table must
permit for those namespaces.

The vendor-label parse (`Unix.Trojan.Mirai-7100807-0` to `av:clamav:mirai` plus
the detail tail) lives in `tag_taxonomy` next to `yara_tag`, not in the CLI, so
the LLM prompt, the validator and the importer see one definition.

### D2 — backfill existing collections

A maintenance job that walks each collection's files, reads the three fields,
mints the tags, and indexes them. Idempotent — a tag already present is a
no-op. It does not delete the fields.

### D3 — flip the reads

Per view. The mechanical part is that a distribution row changes shape: today a
view reads `cluster.avtype_distribution`, after PR C it reads
`cluster.tag_distribution.family`. Same row shape (`value`, `count`,
`percent`), different path, so most of the diff is the path.

`llm_tools.py:356` is worth its own commit: what the model is shown changes,
and `analysis_rules()` already tells it to treat supplied tags as untrusted
hints. Confirm an `inferred:` tag is excluded from that context entirely rather
than merely marked.

### D4 — filter aliases

In `search_file.py`'s filter map, `avtype` / `yara` / `cc_ip` become aliases
onto a tag filter. Prefix matching already works as a single exact bucket
lookup because `tag_prefixes` indexes every ancestor, so `av:clamav:mirai` is
one lookup, not a scan.

### D5 — delete

Remove the fields from `INDEX_CONFIG`, `SUBSTRING_FIELDS`, `POOL_LOCAL_FIELDS`,
`metadata_service`'s `list_fields`, `collect_member_values`, the swagger model
and the CSV readers' field output. Clear the buckets via
`_clear_indexes_via_registry`, the way `bin_cluster_service.py:2049` already
clears the `inferred_*` indexes.

The CSV *columns* keep their names. The upload format is a user-facing
contract and there is no reason to churn it.

## Risk

The one-way step is D5. Everything before it is additive and revertible. Do not
start D5 until D3 has been in use on a real collection for a while and the tag
path has produced the same answers the fields did.

## Verification

- After D2, on a real collection: for a sample of files, the set of `av:` tags
  matches what `avtype` holds.
- `./scripts/wt-test.sh` at each stage. The known-failure set on
  `test_api_endpoints.py` drifts with `dev`, so compare the failure labels
  against this diff rather than against a remembered count.
- `scripts/test_cluster_meta_freq.py` and
  `scripts/test_default_bin_cluster_name.py` both read these fields and need
  updating in step with D3.
