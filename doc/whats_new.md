BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

# New features

This cycle focuses on agentic LLM analysis, a tag taxonomy overhaul with YARA/capa provenance, a reworked binary-similarity UI (File/Container split, Pivotick call graphs), a new default clustering engine, a hardened job system, and a broad XSS-escaping security pass.

## Agentic LLM analysis
* **Tool-using chat agent** — per-function and per-collection chat panel, backed by an agent that can call the same search/graph/tag tools a human would.
* **MCP server** — the read-only analysis tools (search, call graph, tags, similar functions) are now exposed over stdio for external MCP clients.
* **Batch and pair analysis** — whole-file reports, cluster-wide batch tagging/notes, and binary-comparison (pair) analysis, with prompts hardened against over-claiming, double-prefixed tags, and unsupported C2/severity tags.

## Tag taxonomy
* **Four namespaced axes** with a canonical tag id, one hue rule per namespace, and colour assigned by identity rather than match quality.
* **Provenance popups** — hover a tag to see the YARA/rulezet/capa rule that minted it, with rule preview and permalinks where available.
* **ATT&CK/MBC axes** — capa and rulezet matches now carry family/vuln/MITRE/MBC axes through to bin-sim.

## YARA & capa
* Rulezet.org mirror with namespace-routed tags and a false-positive gate; vendored Elastic Linux rules plus multi-arch botnet/Mirai coverage.
* YARA preanalysis tags functions the same way capa does, propagating string hits via xrefs.
* capa rule metadata (ATT&CK/MBC) is now recorded and inherited onto function tags.

## Binary similarity & Pivotick
* **Hard File/Container split** — no more mixed edges between file-level and container-level clusters; containers score by rolling child similarity up the lineage.
* **Code/Library/Content score axes** on every bin_sim pair, with a matching axis picker in the UI.
* **Pivotick call graph** — recursive expansion with depth cap, similarity edges merged in, drag-and-drop/bulk-add, persistent side panel with locked side-by-side diff, and binary clustering with notes sync.
* **Neighbors tab**, resplit-exact-pair action, Sankey view, and tag-scoped server-side paging on the File sim view.

## Clustering
* **Threshold union-find is now the default** function-clustering engine, replacing HDBSCAN, with incremental updates on upload.
* **hierarchical_uf** adds a full single-linkage hierarchy (Kruskal + union-find) for binary clustering, cohesion-scored and named by AV family/YARA.
* Cohesion adjacency now held as CSR instead of a dict-of-dicts; sim-index candidates and propagation are streamed/chunked instead of buffered.

## Jobs, ingest & Ghidra
* Lease-based job claims with a reaper and pause/resume; per-collection job lanes; memory-bounded admission and worker supervision.
* Uploads auto-unpack packed/container files before analysis, with per-binary `--metadata` matching against unpacked children.
* Ghidra analysis runs out-of-process with retry, FID databases are no longer rescanned per function, and temporary GhidraProjects are closed (fixed a JVM thread leak).

## Search & UI
* Unified Ctrl+K homepage search with streaming results and hierarchical namespace tag buckets.
* Full light theme support across every view, plus a restyled homepage.

## Security
* Twelve-commit escaping pass closing XSS gaps across tooltips, tables, context menus, breadcrumbs, note rendering, and shared UI renderers — every user- and analysis-derived string reaching the DOM is now escaped.

# Upgrade notes
* Default clustering engine changed from HDBSCAN to threshold union-find (`hierarchical_uf`) — re-cluster existing collections to pick up the new engine.
* Tag ids are now canonical/namespaced; a one-time migration moves legacy double-prefixed and user-buried tag ids.
