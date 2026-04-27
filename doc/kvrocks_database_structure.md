# Kvrocks Database Structure Documentation

BSimVis uses Kvrocks (Redis-compatible) for storing funcitons, vectors, and similarity results. Collections are isolated from each other.

## Key Naming Conventions

The database distinguishes between **Primary Documents** (large JSON/ZSets) and **Secondary Indices** (lookups/registries).

- **Primary Documents**: Start with `{collection}:` e.g `{collection}:func:{md5}:{addr}`
- **Secondary Indices & Registries**: Start directly with `{collection}:idx:` and `{collection}:reg:` respectively.

---

## 1. Global Metadata

These keys track system-wide state across all collections.

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:collections` | **Set** | Names of all active collections. |
| `global:batches` | **Set** | UUIDs of all data upload batches. |

---

Jobs are also stored in Redis queue as : 

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:job:{id}` | **Hash** | Status and metadata for a background job. |
| `global:pipeline:{id}:jobs` | **List** | Ordered list of job IDs for a multi-step pipeline. |

---

## 2. Document Storage (Primary Data)

These keys store the actual analysis results and decompiler output.

### Files & Functions
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:file:{md5}` | **JSON** | Full binary file metadata. |
| `{coll}:file:{md5}:meta` | **JSON** | Redundant metadata for fast enrichment. |
| `{coll}:func:{md5}:{addr}` | **JSON** | Comprehensive function data (name, convention, etc). |
| `{coll}:func:{md5}:{addr}:source` | **JSON** | Decompiled C code and semantic tokens. |
| `{coll}:func:{md5}:{addr}:vec:tf` | **ZSet** | BSim feature counts (Member: `hash`, Score: `TF`). |

### Similarities
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:sim:{algo}:{id1}:{id2}` | **JSON** | Metadata for a similarity match (e.g., user tags). |

---

Where id1 and id2 are {md5:addr}.

## 3. Secondary Indexes (Collection Scoped)

Indices are optimized for search and do not use the `idx:` prefix at the root level.

### Tag & Metadata Buckets
**Pattern:** `{coll}:idx:{level}:{field}:{value}` (**Set**)
Stores a set of document IDs (e.g. `{coll}:func:...`).
- `level`: `file`, `func`, or `sim`.
- `field`: `batch_uuid`, `language_id`, `function_name`, `tags`, etc.

### Numeric & Sorting Indexes
**Pattern:** `{coll}:idx:{level}:{field}` (**ZSet**)
Stores `doc_id` as member and the numeric value as score.
- **Fields:** `instruction_count`, `bsim_features_count`, `entry_date`.

### Structural Relationships
**Pattern:** `{coll}:idx:file:functions:{md5}` (**Set**)
- Stores a set of function IDs belonging to a specific file.

### Registry Keys (Discovery)
**Pattern:** `{coll}:reg:{level}:{field}` (**Set**)
Stores a list of all existing bucket keys for a specific field.
- Example: `bench2:reg:file:batch_uuid` contains `bench2:idx:file:batch_uuid:uuid_1`, `bench2:idx:file:batch_uuid:uuid_2`.

### Similarity Engine Indices
- `{coll}:sim:score:{algo}` (**ZSet**): Global scoreboard (Member: `sid`, Score: `similarity`).
- `{coll}:sim:built:{algo}` (**Set**): IDs of functions already processed for similarity.
- `{coll}:sim:involves:{level}:{doc_id}` (**ZSet**): Map of which similarities involve a specific file or function.

---

## 4. Search Engine Internals

The similarity search engine in `search_similarity.lua` leverages these indices:
1. Filters are resolved by intersecting bucket Sets from `{coll}:idx:...`.
2. Ranges are resolved using ZSets from `{coll}:idx:...`.
3. The resulting candidate Set is used to rank similarities from `{coll}:sim:score:{algo}`.
