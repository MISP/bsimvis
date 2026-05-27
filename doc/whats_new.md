# Release 0.2

# New features

## Clustering
* HDBSCAN clustering
* Cluster search view
* Dendrogram and Packing diagram

![alt text](/img/function_cluster_view.png)

## Search
* Full text file search, sorting and filtering
* Full text feature search, sorting and filtering
* Matching both function filters
* Indexing configuration
* Search history and caching

## Call graph
* Callees and callers navigation
* Call graph view


## API
* Extended upload API: analysis config params (processor/compiler, profiling, batch metadata, similarity params)
* Swagger UI API documentation

## UI Improvements
* Function code / diff selection and copy
* All dashboard tables selection and copy
* All search export to JSON and CSV
* Tag management panel and user settings panel
* Quick preview tooltips :
	* Cluster preview (scroll to view all functions code)
	* Diff preview (scroll to view all diffs)
* Job view

## Setup
* `install.sh`: automated install of Redis, Kvrocks, Ghidra and optional Milvus
* `launch.sh`: one-command service launcher with screen sessions and `--clear` flag
* Milvus support is now optional (`ENABLE_MILVUS=true` in `.env`)
* Configurable `DATA_BASE_DIR` for data storage paths

# Refactor
* New similarity graph using D3js, with more coloring options
* New window management, allowing multiple code preview in the same page
* Modular frontend JS

![alt text](/img/new_sim_view.png)

# Experimental

* Tests with Milvus vector database for building similarities