# BSimVis: Scaling Binary Similarity and Clustering with Ghidra

## Abstract 

When analyzing malware campaigns, firmware variants, or large binary datasets, analysts often face a wall of files. Reviewing these binaries one by one is inefficient, and reverse engineers end up wasting countless hours analyzing the exact same shared library functions or reused code across different files.

Enter **BSimVis**, an open-source platform that supercharges Ghidra's BSim capabilities to let you analyze entire collections of binaries *at once*. In this 30-minute session, we will demonstrate how BSimVis shifts the paradigm from single-file analysis to bulk visualization and triage. By indexing BSim feature vectors, decompiled code, and metadata into a high-performance Kvrocks backend, BSimVis automatically correlates identical and similar functions across your entire dataset. 

Attendees will learn how to use BSimVis to immediately filter out known reused code, rapidly diff variations in modified functions, and use unsupervised HDBSCAN clustering to automatically group thousands of binaries into distinct malware families. Stop reverse engineering the same functions twice, and start visualizing the broader context of your binary collections.

---

## Presentation Outline (30 Minutes)

**Target Audience:** Malware Analysts, Reverse Engineers, Threat Intelligence Researchers, and Security Tooling Developers.
**Prerequisite Knowledge:** Basic understanding of reverse engineering, Ghidra, and the concept of binary similarity.

### 1. Introduction: The Bulk Analysis Bottleneck (5 mins)
*   The challenge of analyzing large collections of binaries (e.g., malware families, firmware variants).
*   The inefficiency of traditional single-file analysis: wasting time on shared libraries, statically linked code, and copy-pasted functions.
*   Brief overview of Ghidra's BSim (Behavioral Similarity) plugin and why it needs an external platform for fleet-wide analysis.

### 2. BSimVis Architecture & The Paradigm Shift (5 mins)
*   How BSimVis works under the hood: Ghidra headless analyzers + Redis (job queueing) + Kvrocks (high-performance similarity storage).
*   Shifting from single-binary reverse engineering to cross-binary querying and filtering.
*   Differences between the standard BSim database and the BSimVis database model (storing decompiled code, metadata, and cluster tracking).

### 3. Core Features & Live Demonstration (12 mins)
*   **Ingestion at Scale:** Uploading raw binaries or existing Ghidra projects via the CLI/API.
*   **Filtering the Noise:** Identifying and filtering out identical, reused code across the entire collection to isolate the unique logic.
*   **Cross-Binary Diffing:** Rapidly diffing variations in modified functions without switching contexts.
*   **Interactive Visualization:** Navigating the Similarity Graph and Call Graphs directly in the Web UI.

### 4. Automated Clustering & "Pools" (5 mins)
*   Unsupervised binary family clustering using HDBSCAN.
*   Exploring the interactive file dendrogram and packing diagrams to trace malware lineage and group families.
*   **Cross-Collection Pools:** Unioning multiple collections to find similarities across different datasets without mixing their origin tags.

### 5. Getting Started & Q&A (3 mins)
*   How to deploy BSimVis and integrate the REST API into your existing malware processing pipelines.
*   Open floor for questions.
