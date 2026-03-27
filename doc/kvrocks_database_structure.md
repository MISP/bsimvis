# Kvrocks Database Structure Documentation

BSimVis uses Kvrocks (Redis-compatible) for storing binary analysis data, features, and similarity results. It implements secondary indexes using Sets and ZSets.

## Key Naming Conventions

All fields in the database use `_` as a separator (e.g., `file_md5`, `function_name`). Key patterns generally follow a hierarchical colon-separated format.

---

## 1. Global Metadata
These keys track system-wide information across all collections.

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:collections` | **Set** | Names of all active collections. |
| `global:batches` | **Set** | UUIDs of all data upload batches. |
| `global:batch:{uuid}` | **JSON** | Global metadata for an upload batch (name, timestamps, collections). |
| `global:collection:{coll}:meta` | **Hash** | Cumulative stats for a collection (`total_files`, `total_functions`). |

---

## 2. Document Storage
Primary data is stored as JSON objects or specific data types.

### Files
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:file:{md5}:meta` | **JSON** | Metadata for a binary file (name, hashes, language, etc.). |

### Functions
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:function:{md5}:{addr}:meta` | **JSON** | Core metadata for a function. |
| `{coll}:function:{md5}:{addr}:source` | **JSON** | Decompiled C code, tokens, and address-to-source mappings. |
| `{coll}:function:{md5}:{addr}:vec:meta` | **JSON** | Metadata/Context for each BSim feature (line numbers, P-Code ops). |
| `{coll}:function:{md5}:{addr}:vec:raw` | **JSON** | Flat list of BSim feature hashes. |
| `{coll}:function:{md5}:{addr}:vec:tf` | **ZSet** | Per-function feature counts (Member: `hash`, Score: `TF`). |
| `{coll}:function:{md5}:{addr}:vec:norm` | **String** | Pre-calculated L2 norm of the function's feature vector. |

### Similarities & Batches
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `{coll}:sim_meta:{sim_id}` | **JSON** | Details of a similarity match between two functions. |
| `{coll}:batch:{uuid}` | **JSON** | Collection-specific batch metadata (total counts for this collection). |
| `{coll}:batch:{uuid}:functions` | **Set** | Set of all function IDs belonging to this batch in the collection. |

---

## 3. Manual Secondary Indexes
Defined in [bsimvis/app/services/index_service.py](file:///bsimvis/app/services/index_service.py), these keys enable efficient querying without full scans.

### Tags (Exact Match)
**Pattern:** `idx:{coll}:{doc_type}:{field}:{value}` (Type: **Set**)
Used for fields like `file_md5`, `function_name`, `tags`, `language_id`, etc.

### Numeric (Range Query)
**Pattern:** `idx:{coll}:{doc_type}:{field}` (Type: **ZSet**)
Stores `doc_id` as member and the numeric value as score.
- **Fields:** `instruction_count`, `bsim_features_count`, `score`, `batch_order`.

### Similarity Secondary Indexes
Used for filtered searches in `bsimvis/app/routes/search_similarity.py`. These indexes refer to the "first" and "second" functions in a similarity pair.
- **Sets (Exact/Substring):** `idx:{coll}:sim:{field}{1|2}:{value}` (e.g., `idx:main:sim:name1:FUN_000080d4`).
    - **Fields:** `md5_`, `name`, `tag`, `language_id`.
- **ZSets (Range):** `idx:{coll}:sim:feat_count{1|2}`.
    - **Score:** Feature count of the respective function in the pair.

### Tracking & Relationships
| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `idx:{coll}:all_{type}s` | **Set** | Tracking all files, functions, or similarities (e.g., `idx:main:all_files`). |
| `idx:{coll}:file_funcs:{md5}` | **Set** | Maps a file MD5 to all its member function meta keys. |
| `idx:{coll}:sim:{md5_1}` | **ZSet** | Maps a file MD5 to its similarities (Member: `sim_id`, Score: `score`). |
| `{coll}:all_sim:{algo}` | **ZSet** | Global scoreboard for an algorithm. Member: `sim_id`, Score: `similarity_score`. |

---

## 4. Vector Search Engine
Defined in [bsimvis/cli/bsimvis_index.py](file:///bsimvis/cli/bsimvis_index.py), managed via `bsimvis features`.
Use `bsimvis index status -c {coll} --details` for a health and space overview.

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `idx:{coll}:feature:{f_hash}:functions` | **ZSet** | Inverted Index. Maps feature hash to Function IDs with TF as score. |
| `idx:{coll}:feature:{f_hash}:meta` | **Hash** | Function-specific context for a feature (JSON entries indexed by Func ID). |
| `idx:{coll}:features:by_tf` | **ZSet** | Global ranking of features by their total frequency in the collection. |
| `idx:{coll}:indexed:functions` | **Set** | Bookkeeping set of all functions that have been processed by the indexer. |
| `idx:{coll}:baked:functions:{algo}` | **Set** | Bookkeeping set of all functions that have had similarities baked for a specific algorithm. |
