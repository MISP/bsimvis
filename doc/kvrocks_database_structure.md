# Kvrocks Database Structure Documentation

BSimVis uses Kvrocks (Redis-compatible) for storing binary analysis data, features, and similarity results. It implements secondary indexes using Sets and ZSets.

## Key Naming Conventions

All fields in the database use `_` as a separator. Key patterns generally follow a hierarchical colon-separated format.

---

## 1. Global Metadata

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:collections` | **Set** | Names of all active collections. |
| `global:batches` | **Set** | UUIDs of all data upload batches. |
| `global:collection:{coll}:meta` | **Hash** | Cumulative stats for a collection (`total_files`, `total_functions`). |
| `job:{id}` | **Hash** | Status and metadata for a background job. |
| `pipeline:{id}:jobs` | **List** | Ordered list of job IDs for a multi-step pipeline. |

---

## 2. Document Storage

### Files & Functions
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:file:{md5}` | **JSON** | Full binary file data and metadata. |
| `{coll}:function:{md5}:{addr}:meta` | **JSON** | Core metadata for a function. |
| `{coll}:function:{md5}:{addr}:source` | **JSON** | Decompiled C code and semantic tokens. |
| `{coll}:function:{md5}:{addr}:vec:tf` | **ZSet** | BSim feature counts (Member: `hash`, Score: `TF`). |

### Similarities
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:sim_meta:{algo}:{id1}:{id2}` | **JSON** | Metadata for a similarity match (e.g., tags, date). |

---

## 3. Secondary Indexes

### Tag Indexes (Sets)
**Pattern:** `idx:{coll}:{doc_type}:{field}:{value}`
Used for exact matching on fields like `language_id`, `batch_uuid`, `function_name`, `tags`.
- `doc_type`: `file` or `function`.

### Numeric Indexes (ZSets)
**Pattern:** `idx:{coll}:{doc_type}:{field}`
Stores `doc_id` as member and the numeric value as score.
- **Fields:** `instruction_count`, `bsim_features_count`, `entry_date`.

### Registry Keys (Discovery)
**Pattern:** `idx:{coll}:reg:{field}` (**Set**)
Stores a list of all existing bucket keys for a specific field. Enables efficient "contains" search and key discovery without using expensive `KEYS` commands.
- Example: `idx:main:reg:function_name` contains `idx:main:function:function_name:main`, `idx:main:function:function_name:fun_123...`.

### Similarity Indexes
- `idx:{coll}:all_sim:{algo}` (**ZSet**): Global scoreboard (Member: `sim_id`, Score: `score`).
- `idx:{coll}:sim:feat_count`: (**ZSet**) Combined feature count for a pair.
- `idx:{coll}:sim:is_cross_binary:true` (**Set**): Pre-filtered cross-binary pairs.

---

## 4. Search Engine Internals

The similarity search engine in `search_similarity.lua` intersects these indices using a "Deep Selection" architecture:
1. Resolve metadata filters to Sets of function IDs.
2. Resolve score/feature ranges from ZSets.
3. Combine using optimized Set intersections in Lua.
