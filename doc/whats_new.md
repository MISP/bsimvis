BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

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
