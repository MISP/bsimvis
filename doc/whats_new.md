BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

# New features

This release focuses on automated analysis, triage of functions, and file level similarities views and searches :
- Mandiant capa integration

- Yara rules, with a Rulezet.org API dump tool

- Ghidra FunctionID for standard library identification

- LLM analysis and tagging

The new binary similarity view allows to split the similarities based on these triage. 
Some optimisations for incremental build, and improved clustering. 

![image-20260907144949763](../img/tag_sources.png)

## Agentic LLM analysis
* **Integrated chat agent** — Interactive context aware assistant panel.

![alt text](../img/agent_view.png)

* **MCP server** — Exposes search, call graphs, tags, and similarities to external MCP clients.
* **Batch and pair analysis** — Whole-file reports, batch tagging, and comparative analysis of binary pairs.

![alt text](../img/C2_search.png)

## External analysis integrations
* **YARA preanalysis** tags functions for fast triage and inisights, with an automated Rulezet.org synchronization tool. 
* **capa rule metadata & axes** — ATT&CK/MBC rule metadata is recorded and inherited onto function tags, carrying family/vuln/MITRE/MBC axes through to bin-sim.
* **Provenance** — hover an analysis tag to see the rule that created it and its documentation. 

## Nested files and packed files
* **Auto-unpacking pipeline** — Seamless extraction of code from container formats such as Android APKs, fat/universal Mach-O binaries, and ZIP/TAR archives.
* **UPX packer support** — Automatic decompression of UPX-packed binaries, preserving both the original packed sample and the unpacked child executable for side-by-side diffing and analysis.

## Binary similarity & Pivotick
* **Hard File/Container split** — no more mixed edges between file-level and container-level clusters; containers score by rolling child similarity up the lineage.
* **Code/Library/Content score axes** on every bin_sim pair, with a matching axis picker in the UI.
* **Pivotick call graph** — recursive expansion with depth cap, similarity edges merged in, drag-and-drop/bulk-add, persistent side panel with locked side-by-side diff, and binary clustering with notes sync.
* **Neighbors tab**, resplit-exact-pair action, Sankey view, and tag-scoped server-side paging on the File sim view.
![alt text](../img/call_graph_pivotick.png)
## Clustering
* **New clustering backend** — Threshold union-find replaces HDBSCAN for function clustering with incremental update, lowering drastically clustering time.
* **Hierarchical binary clustering** — Groups binary families with Kruskal/union-find.


## Jobs and injesting
* **Reliable workflows** — Lease-based job scheduler, per-collection lanes, auto-unpacking of container files

## Search, UI & Security
* **Unified search** — Ctrl+K global search with streaming results.
* **Light theme** — Light mode support.
* **Security** — Hardened input escaping to prevent XSS.
