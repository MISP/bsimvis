BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

# 1.0

BSimVis is now a malware family analysis tool. Go beyond comparing two samples: analyse an
entire malware family at once, understand how code is reused across variants, and track how
capabilities evolve over time. This release also gives file clusters a durable identity that
analysts can annotate, and reworks navigation around collections.

## N-way diff
* **Lanes and lenses** : rows are grouped into lanes (the functions a set of files shares)
  under Core / Common / Rare tiers. Nested sub-lanes and a split by region are options in
  More options. Lens tabs switch the grouping
  between Lanes, Modules, Core, Deltas and Triage. Lanes get LLM names from their strings, calls and
  tags, and nameless `FUN_` rows are named after a discriminating string.
* **Column groups** : files with the same shared rows merge into one twin column, files can be
  grouped by cluster, and columns are ordered so that files of the same lane sit together.
  Compare groups take several searches and can be sent from the Files page filters.
* **Lineage page** : the family drawn as a tree of files with gained and lost code, in five
  views. Tree and Tracks follow one group through the family. Ring shows clusters of files with
  the modules they share as ribbons. Timeline orders files by first seen date and flags dates
  that are probably late. Births shows where each group of functions first appears and how it
  is inherited across forks. Every view shares a group selector, an inspector, and a state bar
  with Back and Reset, and the N-way table can be shown under the chart.
* **Large sets** : sets above `nway.auto_build_max_members` files build as a background job
  with Fast / Balanced / Deep presets and a time estimate, instead of blocking a request.
* **Code views** : one Code tab (blocks, full function, unified), a CFG strip on a shared row
  grid, and callers, callees and constants with per-column presence.

## File clusters
* **Cluster overview** : an Overview tab with shared-code lanes, a lineage preview and metadata
  distributions, plus a medoid tab and secondary-axis cohesion.
* **Names, notes and bookmarks that survive rebuilds** : a cluster keeps its uuid when a rebuild
  gives back the same members. Analyst names, tags, notes and bookmarks stay on it, and a cluster
  that was merged or split proposes its notes to its successor.
* **Timeline and relevance** : clusters carry first / last upload and in-the-wild dates
  (`first_seen` / `last_seen` at upload), and the cluster list sorts by activity and by a
  family-level relevance score.

## Navigation and collections
* **Collection overview** : stats, a family map, uploads over time against first and last seen
  dates, architecture / format / tag mix, and the most active families.
* **Navigation** : a global menu in the breadcrumb row and a tab strip on every collection and
  pool page, with counts. Searches are anchored to a collection or pool.
* **File browser** : files shown as cards, list + detail or table, and drawn as a tree by
  archive or by cluster. Every paged list has a jump-to-page pager, with a pages-or-scroll
  setting.

## Analysis
* **Strings** : Ghidra strings are extracted at analysis, indexed and searchable, with a
  collection-wide Strings page, a function Strings tab and pivots to function and file search.
  YARA string hits are mapped to strings.
* **YARA workbench (experimental)** : a live rule editor at `/yara` that tests a rule against
  chosen files and reports precision, recall and where each hit lands in functions and
  strings. It suggests strings from a file set or an N-way family, and can apply a rule as
  `yara:custom:` file tags.
* **Function CFG** : a CFG tab on the function view, linked both ways with the code, and Block
  Diff and CFG Diff tabs in the 2-way diff.
* **Analyst function names** : a function renamed by an analyst or the LLM shows that name,
  with its origin, everywhere. LLM name suggestions use the cluster names of stripped callers
  and callees.
* **Entry point tag** : the program entry point is tagged `entry:process` (LIEF) and opens from
  the file view.

## New defaults
* **Weighted cosine by default** : function similarity weights BSim features by rarity. On 40
  Mirai samples of 8 known variants it placed each variant in one cluster more often than
  `unweighted_cosine` (best-cluster F1 0.88 against 0.75), and the benchmark suite ranks it first
  for separating projects. It costs about 1.6x the similarity build time of `unweighted_cosine`.
* **Clustering and similarity follow the production setup** : function clusters by
  `hierarchical_uf`, file clusters by `hierarchical_snn` (both with a cluster tree, which the
  overviews and family views need), every match counted in the file score
  (`unweighted_match`), and leftover functions re-matched down to 0.4 when two files are
  compared (`discovery`).
* **FunctionID, boilerplate and YARA on by default** : library and runtime code is tagged on every
  upload, so file scores and clusters follow the original code. capa and Rulezet stay opt-in.
* **A collection keeps its algorithm** : the first build locks it, like `min_score`. A collection built
  before this release keeps the algorithm its edges were built with, so a later change of
  `similarity.algo` does not empty its similarity views. Change it in the collection params, then rebuild.
* Existing `bsimvis_config.toml` files are not touched: copy the new keys from
  `bsimvis_config.toml.example` to adopt them.

## Operations
* **Install** : `./install.sh` creates `.env` and `bsimvis_config.toml` on first run, so there is
  nothing to copy by hand. It installs the prebuilt Kvrocks package (sha256-pinned) instead of
  compiling it.
* **Disk use** : Kvrocks keeps at most 1 GB / 10 min of old write-ahead logs (`kvrocks.conf`). Its
  defaults kept up to 16 GB for replicas BSimVis does not have, enough to fill a small disk.
* **Docker** : a Dockerfile and compose services for the app and workers (gunicorn, non-root,
  pinned base image and JDK), with optional Docker datastores for small machines.
* **Sample download** : download a stored sample as an `infected` password zip.
* **`scripts/fetch_samples.py`** : pull samples from MalwareBazaar or MWDB by hash list or tag
  and upload them.
* **Performance** : large `build_sim` batches split into parallel shard jobs, `redis[hiredis]`
  for reply parsing, gzip responses, a file search that stops sorting at the requested page,
  and an overview that serves its cached copy while it refreshes.
* **Security** : dependency bumps for every open Dependabot advisory (black, certifi, mcp, pillow,
  pyjwt, setuptools, urllib3, werkzeug).

# 0.6.0

This release focuses on scan-time triage, function-similarity precision, and maintenance tooling for running collections at scale.

## Scan / fast mode
* **Compare without ingest** : Scan API analyses a file against existing collections without writing indexes, so an analyst can compare a sample with the whole BsimVis instance before deciding to commit it in a collection. Multi-file upload support, container unpacking (archives,
  UPX packed files) and queue with high priority worker.  

![image-20260929104657241](../img/fast_scan_mode.png)

## Function similarity
* **Weighted cosine & binary cosine** : two new similarity algorithms alongside the existing BSim score, configurable per collection and pool. 
* **Discovery of low similarity functions** : function similarity search only stores high similarity scores. However, during the matching of functions between two binaries, a discovery pass now allows matches below `similarity.min_score` . This can also now be used at runtime with different parameters and algorithms, using **Runtime Greedy Matcher**.
* **Call graph similarity** : diffing view now provides matching of callers and callees between the two functions, as an extra signal alongside code similarity.

## N-way diff
* **Inline function diff** : expand a row of the N-way table to compare its functions in place: string / API / callee / constant facets, a function-similarity matrix, block-level code alignment (identical, modified, moved, restructured, missing) with a whole-function side-by-side view, and the existing 2-way diff inline. Block titles are deterministic, with optional LLM titles filled in on demand.

## Clustering
* **Code/library axis split for clustering** : Identified shared library don't score and cluster with original malware code anymore. This allows analyst to focus on unidentified code. Standard library and boilerplate code were also a source of low similarity scores between different architectures. This improved similarity scores and clustering of files with different architecture dependent boilerplate code, focusing on the actual malware code for scoring. 

![image-20260929104945543](../img/code_axis_split.png)

* **Cluster view** : Explore the file cluster dendrogram and get detailed metadata distribution of members (family classification, format, architecture. etc.) 

![image-20260929113223137](../img/detailed_cluster_metadata.png)

* **SNN clustering** : Based on Incremental Union Find, prevents over-merging of clusters. 

## Tags & metadata
* **New tag namespaces and automatic metadata inference** : Infers metadata from neighbor files

![image-20260929110953372](../img/inferred_tags.png)

* **Boilerplate tagging** : automatic recognition of common compiler/runtime boilerplate
  function names (mingw, uclibc/libc internals, pformat-style helpers) for fast triage.

## Maintenance tools
* **Collection / pool maintenance API** : delete file entries, and rebuild any step of
  the similarity pipeline for each file or batch, with different parameters and algorithms.
* **`bsimvis transfer`** : copy a collection between instances over the CLI.

## Security
* **XSS fixes** : Added a script to check for unescaped values from malware samples, XSS fixed by new contributor [elhoim](https://github.com/elhoim)
* Dependency bumps and offline support via vendored libraries.

**Next steps:** benchmarking BSimVis settings and algorithms for the storage/speed/precision
tradeoff across different datasets, to land on better default settings.
