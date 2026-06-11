# Release 0.3.0 - Binary clustering, Notes and AI Insights 

This new version focuses on file-level similarities, hierarchical clustering visualizations. It also brings analyst notes, and local LLM insights to streamline binary analysis workflows.

# Screenshots

![Analyst Notes & AI Insights](/img/notes_and_ai_insights.png)
![File Dendrogram](/img/binary_dendrogram.png)
![Binary Diff](/img/binary_diff.png)

# New features

## Binary Similarity & Clustering
* Hierarchical binary clustering and interactive file dendrogram visualization
* Automated metadata propagation from similar files in clusters to infer attributes (Yara rules, AV classification, file type, C2 IPs)

## Analyst Notes & IA Insights
* Analyst notes system for files and functions
* Local LLM assistant for file and function summaries, supporting Ollama 
* Note-owner indexing and filtering in function and file search

## Navigation & SPA
* Single Page Application (SPA) architecture with full browser history support
* Contextual right-click menus for copying, tagging and navigating
* Unified breadcrumbs navigation

# Performance & Maintenance
* Fixed pipelining transaction performance issue
* CLI tools for deleting and cleaning up collections