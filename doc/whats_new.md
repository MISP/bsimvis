BSimVis is a tool to analyze similarities across a collection of binaries, based on [Ghidra](https://github.com/nationalsecurityagency/ghidra) analyzers and the BSim (Behavioral Similarity) plugin. It provides an API and Web interface to upload large quantities of decompiled binaries and BSim feature vectors to a Kvrocks database for similarity analysis, function diffing, and binary family clustering.

# New features

This release lands LCA-accelerated similarity discovery and hierarchical clustering, an agentic LLM analysis layer (chat + MCP server), a rebuilt call-graph/cluster UI (Pivotick), and a tag taxonomy overhaul — on top of continued stability and job-pipeline fixes.

## LCA Acceleration
* **Unique-vector-class discovery** — functions are hashed into shared vector classes instead of compared pairwise; discovery scores classes once and expands to member functions, cutting redundant work on large collections.
* **rust_cpu / wgpu backends** — `similarity.discovery_backend` selects the native Rust scorer or a WGPU-accelerated one; WGPU does a broad float32 pass and recomputes/thresholds candidates in Rust f64, falling back to CPU on GPU failure.
* **Compact incremental class graph** — edges are stored as delta-encoded partitions over a base snapshot, compacted after a few overlay generations instead of rebuilt from scratch on every change.
* **Hierarchical UF clustering** — `hierarchical_uf` is now the default clustering engine: a full single-linkage tree (Kruskal + union-find) over vector classes, cut by cohesion, projected back to functions and binaries.
* **Remote deploy/migrate/benchmark tooling** for standing up and comparing `rust_cpu` vs `wgpu` on remote hosts.
* Python discovery remains as a compatibility fallback when the native extension isn't available.

## Agentic LLM Analysis
* **Tool-using chat agent** wired into the per-function chat panel, with the MCP/chat tool surface broadened to full search coverage (functions, tags, call graphs, similar functions).
* **MCP server** exposing the read-only analysis tools over stdio, for driving BSimVis from external MCP clients.
* **Configurable full-file analysis** — agentic tagging, notes, and a whole-file report, scoped to the target architecture and grounded against actual evidence to curb speculative malware attribution.

## Binary Similarity & Call Graph UI
* **Pivotick rebuilt**: all three call-graph surfaces (function view, binary cluster, similarity diff) now share one controller, with tree/radial layout, cluster collapse, drag-and-drop, and a persistent side panel with lock mode for side-by-side diffing.
* **File/Container view split** for binary clustering, so file-level and container-level clusters no longer share one mixed graph.
* **Neighbors tab** added to File and Function views, reusing the bin-sim hero card/pill pattern.
* **Cluster bookmarks and tags.**
* **git-stash-alike** (`tag_stash.py`) — attach notes/tags to bin_sim, file, or function similarity pairs from the CLI, routed through `SimilarityService`.

## Tag Taxonomy
* Canonical tag IDs with detector-named origin namespaces and a single colour rule per namespace, applied consistently across tables, trees, and cards.
* Hoverable provenance popups with rule preview and permalinks.
* Rulezet and YARA tag routing fixes (mirrored rule UUID vs name, family fallback).

## Stability & Job Pipeline
* Per-collection job lanes replace mid-analysis job splicing.
* Live job-status badges on collections/batches/files tables; upload flow gets progress bars, a success screen, and a "Go to Collection" CTA.
* Assorted bin-sim crash fixes (unguarded `.decode()`, count-pill races, chunked build scheduling) and score-formula corrections (Code/Library/Content axes).
* Portable JDK 21 auto-install when no system Java is found.

## Search & Home
* Unified `Ctrl+K` search streams results as they're found, no longer capped at 25 collections.
* Homepage restyle with real tables and an upload CTA.

# New Contributor
* @SegmondFault made their first contribution
