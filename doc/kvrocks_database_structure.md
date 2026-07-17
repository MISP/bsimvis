# Kvrocks Database Structure Documentation

BSimVis uses Kvrocks (Redis-compatible) for storing functions, vectors, and similarity results. Collections are isolated from each other. Cross-collection **pools** mirror the same layout under a `global:pool:{pool_id}` prefix (see section 5).

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
| `{coll}:file:{md5}` | **JSON** | Full binary file metadata. Analyst **file notes** live inline in this document (`notes` field + `note_owners`). |
| `{coll}:file:{md5}:meta` | **JSON** | Redundant metadata for fast enrichment. |
| `{coll}:func:{md5}:{addr}` | **JSON** | Comprehensive function data (name, convention, etc). Analyst **function notes** live inline here (`notes` field + `note_owners`). |
| `{coll}:func:{md5}:{addr}:source` | **JSON** | Decompiled C code and semantic tokens. |
| `{coll}:func:{md5}:{addr}:vec:tf` | **ZSet** | BSim feature counts (Member: `hash`, Score: `TF`). |
| `{coll}:batch:{uuid}` | **Hash** | Ingestion batch metadata. |
| `{coll}:batch:{uuid}:functions` | **Set** | Function doc keys belonging to a batch (used to list a batch's functions). |

### Similarities
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:sim:{algo}:{id1}:{id2}` | **JSON** | Metadata for a similarity match (e.g., user tags). |

### Global Features
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:all_features` | **Set** | Master list of all feature IDs. |
| `{coll}:feature:{hash}:global_meta` | **JSON** | Global metadata for a specific feature. |
| `{coll}:feature:{hash}:meta` | **Hash** | Context occurrences (Field: `func_id`, Value: Context JSON). |
| `{coll}:feature:{hash}:functions` | **ZSet** | Functions containing this feature. |
| `{coll}:features:by_tf` | **ZSet** | Feature TF scores (Member: `hash`, Score: `TF`). |

### Clusters
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:cluster:list:{algo}` | **Set** | All cluster IDs discovered for the algorithm. |
| `{coll}:cluster:{algo}:{cluster_id}:meta` | **JSON** | Metadata for a specific cluster. |
| `{coll}:cluster:{algo}:{cluster_id}:members` | **Set** | Member function IDs of the cluster. |
| `{coll}:cluster:tree:{algo}` | **String** | Serialized dendrogram tree. |
| `{coll}:cluster:tree_links:{algo}` | **String** | Raw parent-child links for dynamic tree building. |

### Binary Similarities
File-level (binary-to-binary) similarity, derived from shared function clusters.
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:bin_sim:{algo}:{md5_a}::{md5_b}` | **JSON** | Binary similarity pair (score, coverage, shared clusters). |
| `{coll}:bin_sim:score:{algo}` | **ZSet** | Binary-pair scoreboard (Member: `md5_a::md5_b`, Score: similarity). |
| `{coll}:bin_sim:built:{algo}` | **Set** | Binaries already processed for binary similarity. |
| `{coll}:bin_sim:involves:{md5}` | **ZSet** | Binary-sim pairs involving a specific file. |

### Binary Clusters
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:bin_cluster:list:{algo}` | **Set** | All binary cluster IDs for the algorithm. |
| `{coll}:bin_cluster:{algo}:{cid}:meta` | **JSON** | Metadata for a binary cluster. |
| `{coll}:bin_cluster:{algo}:{cid}:members` | **Set** | Member file IDs (full subtree). |
| `{coll}:bin_cluster:{algo}:{cid}:direct_members` | **Set** | Direct member file IDs. |
| `{coll}:bin_cluster:tree:{algo}` | **String** | Serialized binary dendrogram tree. |
| `{coll}:bin_cluster:tree_links:{algo}` | **String** | Raw parent-child links. |

---

Where id1 and id2 are {md5:addr}.

## 3. Secondary Indexes (Collection Scoped)

Indices are optimized for search and do not use the `idx:` prefix at the root level.

### Tag & Metadata Buckets
**Pattern:** `{coll}:idx:{level}:{field}:{value}` (**Set**)
Stores a set of document IDs (e.g. `{coll}:func:...`).
- `level`: `file`, `func`, `sim`, or `feature`.
- `field`: `batch_uuid`, `language_id`, `function_name`, `tags`, `cluster_uuid`, `bin_cluster_uuid`, `note_owners`, etc.

`note_owners` buckets index files/functions by note author, e.g. `{coll}:idx:func:note_owners:{owner}` with its registry `{coll}:reg:func:note_owners`.

### Numeric & Sorting Indexes
**Pattern:** `{coll}:idx:{level}:{field}` (**ZSet**)
Stores `doc_id` as member and the numeric value as score.
- **Fields:** `instruction_count`, `bsim_features_count`, `entry_date`, `frequency`, `tf_score`.

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
- `{coll}:sim:min_features` (**ZSet**): Optimization for feature count filtering.

---

## 4. Search Engine Internals

The similarity search engine in `search_similarity.lua` leverages these indices:
1. Filters are resolved by intersecting bucket Sets from `{coll}:idx:...`.
2. Ranges are resolved using ZSets from `{coll}:idx:...`.
3. The resulting candidate Set is used to rank similarities from `{coll}:sim:score:{algo}`.

---

## 5. Pools (Cross-Collection)

A pool combines the functions/binaries of several collections into one searchable space. Pool data is stored under a `global:pool:{pool_id}` prefix that mirrors the per-collection layout, so search, similarity, and clustering reuse the same engine.

### Registry & Definition
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:pools` | **Set** | All pool IDs. |
| `global:pool:{pool_id}:meta` | **JSON** | Pool definition (name, config, timestamps, sync status). |
| `global:pool:{pool_id}:collections` | **Set** | Member collection names. |
| `{coll}:pools` | **Set** | Reverse index: pools a collection belongs to. |

### Pool Data (mirrors collection layout)
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:pool:{pool_id}:all_functions` | **Set** | All function IDs in the pool. |
| `global:pool:{pool_id}:all_files` | **Set** | All file IDs in the pool. |
| `global:pool:{pool_id}:sim:score` | **ZSet** | Function-pair scoreboard for the pool. |
| `global:pool:{pool_id}:sim:{coll}:func:{id1}::{coll}:func:{id2}` | **JSON** | Pool function similarity pair (IDs are collection-qualified). |
| `global:pool:{pool_id}:bin_sim:score:{algo}` | **ZSet** | Binary-pair scoreboard for the pool. |
| `global:pool:{pool_id}:cluster:list` | **Set** | Pool function cluster IDs. |
| `global:pool:{pool_id}:bin_cluster:list` | **Set** | Pool binary cluster IDs. |
| `global:pool:{pool_id}:idx:...` / `:reg:...` | **Set** | Same secondary index/registry layout as a collection. |

