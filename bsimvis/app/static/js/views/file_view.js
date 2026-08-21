/**
 * File View Module
 * Extracted from file/index.html
 */

window.FileView = {
    container: null,
    params: null,
    functions: [],
    clusters: {},
    functionsLoaded: false,
    neighborsLoaded: false,
    neighborsDebounceTimer: null,
    sortState: { col: 'function_name', dir: 1 },
    funcClusters: {},
    funcPage: { total: null, loading: false, reqId: 0 },
    FUNC_PAGE_SIZE: 100,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        this.functions = [];
        this.clusters = {};
        this.funcClusters = {};
        this.file = null;
        this.funcPage = { total: null, loading: false, reqId: 0 };
        this.functionsLoaded = false;
        this.neighborsLoaded = false;
        this.sortState = { col: 'function_name', dir: 1 };
        this.fvAxis = '';
        this.fvSelectedTag = null;
        this.fvOpen = new Set();
        this.fvGroupBy = 'auto';
        this.fvTagIndex = null;

        const collection = params.collection || '';
        const file_md5 = params.md5 || params.file_md5;

        if (!file_md5) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No file MD5 provided.</div>';
            return;
        }

        // Build HTML structure
        this.container.innerHTML = `
            <style>
                .bsim-tabbar { display:flex; gap:4px; margin:0 0 16px 0; border-bottom:2px solid var(--border); }
                .bsim-tab {
                    background:none; border:none; border-bottom:3px solid transparent;
                    margin-bottom:-2px; padding:10px 20px; cursor:pointer;
                    color:var(--subtle); font-size:0.9rem; font-weight:600; letter-spacing:0.01em;
                    transition:color 0.15s, border-color 0.15s, background 0.15s;
                }
                .bsim-tab:hover { color:var(--text); background: var(--hover); }
                .bsim-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
                
                .file-func-table { width:100%; border-collapse:collapse; font-size:0.8rem; }
                .file-func-table th { text-align:left; padding:10px; border-bottom:1px solid var(--border); color:var(--subtle); text-transform:uppercase; font-size:0.75rem; letter-spacing:0.05em; }
                .file-func-table td { padding:10px; border-bottom: 1px solid var(--border); vertical-align:middle; }
                .file-func-table tr:hover { background: var(--hover); }
                
                .file-func-table th.sortable { cursor: pointer; user-select: none; }
                .file-func-table th.sortable:hover { color: var(--text); }
                .file-func-table tr.filter-row th { padding: 4px 10px; border-bottom: 1px solid var(--border); background: var(--border); }
                .file-func-table tr.filter-row input { background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 3px; font-size: 0.7rem; box-sizing: border-box; }

                .bin-sim-mc-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
                .bin-sim-mc-table th { text-align:left; padding:6px 12px; color:var(--subtle); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; border-bottom:1px solid var(--border); }
                .bin-sim-mc-table td { padding:6px 12px; border-bottom: 1px solid var(--border); vertical-align:top; font-family:'Consolas',monospace; word-break:break-word; }
                .bin-sim-mc-cat { padding:10px 12px 4px; font-weight:bold; color:var(--accent); font-size:0.78rem; }
                .bin-sim-mc-label { color:var(--subtle); font-family:'Inter',sans-serif; width:160px; }
                
                .bin-sim-strip { border:1px solid var(--border); border-radius:6px; padding:10px 12px; background:var(--card-bg); display:flex; align-items:center; gap:10px; min-height:24px; }

                /* Function tag tree sidebar -- same shape/classes as bin-sim's
                   #bsim-sidebar (binary_similarity.js), ported for a single file's
                   function list instead of a two-sided comparison. */
                #fv-tree-sidebar {
                    width:240px; flex-shrink:0; display:flex; flex-direction:column;
                    border:1px solid var(--border); border-radius:8px; background:var(--card-bg);
                    overflow:auto; padding:10px 0; max-height: calc(100vh - 260px); min-height: 300px;
                }
                .bsim-side-title {
                    font-size:0.68rem; text-transform:uppercase; letter-spacing:0.07em;
                    color:var(--subtle); font-weight:bold; padding:4px 12px 8px;
                    display:flex; align-items:baseline; justify-content:space-between; gap:8px;
                }
                .bsim-side-actions { display:flex; gap:8px; text-transform:none; letter-spacing:0; font-weight:normal; }
                .bsim-side-actions span { cursor:pointer; color:var(--dim); }
                .bsim-side-actions span:hover { color:var(--accent); }
                .bsim-axis-pick { display:flex; align-items:center; gap:6px; padding:0 12px 8px; }
                .bsim-axis-pick:empty { display:none; }
                .bsim-axis-pick select { flex:1; min-width:0; }
                .bsim-tree { flex:0 0 auto; }
                .bsim-node {
                    display:flex; align-items:center; gap:6px; padding:4px 12px; cursor:pointer;
                    font-size:0.8rem; font-family:'Inter',sans-serif; color:var(--text);
                    border-left:3px solid transparent; white-space:nowrap;
                }
                .bsim-node:hover { background:var(--hover); }
                .bsim-node.selected { background:var(--hover); border-left-color:var(--accent); }
                .bsim-node .bsim-caret { width:12px; color:var(--subtle); flex-shrink:0; user-select:none; }
                .bsim-node-dot {
                    display:inline-block; width:8px; height:8px; border-radius:50%; flex-shrink:0;
                    vertical-align:middle;
                }
                .bsim-node .bsim-node-label { flex:1; overflow:hidden; text-overflow:ellipsis; }
                .bsim-node .bsim-node-count { font-size:0.68rem; color:var(--dim); font-family:'Consolas',monospace; }
                .bsim-chips { display:flex; flex-wrap:wrap; gap:6px; padding:0 12px 8px; min-height:0; }
                .bsim-chip {
                    display:inline-flex; align-items:center; gap:6px; padding:3px 8px;
                    border:1px solid var(--border); border-radius:12px; background:var(--bg-alt);
                    font-size:0.72rem; font-family:'Inter',sans-serif; color:var(--subtle);
                }
                .bsim-chip b { color:var(--text); font-weight:600; }
                .bsim-chip .bsim-chip-x { cursor:pointer; color:var(--dim); }
                .bsim-chip .bsim-chip-x:hover { color:var(--token-instruction); }
                .bsim-ctl-label {
                    font-size:0.7rem; color:var(--subtle); margin-right:6px; font-weight:bold;
                    font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;
                }
                .bsim-grp-row td {
                    background:var(--bg-alt); border-top:1px solid var(--border);
                    border-bottom:1px solid var(--border); padding:7px 10px;
                    font-family:'Inter',sans-serif; font-size:0.78rem; cursor:pointer;
                }
                .bsim-grp-row:hover td { background:var(--hover); }
                .bsim-caret-btn { cursor:pointer; user-select:none; color:var(--subtle); display:inline-block; width:14px; }
            </style>
            <div id="file-view-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Loading Binary Details...
            </div>
            <div id="file-view-content" style="display: none; flex:1; overflow-y:auto; padding: 0 0 20px 0;">
                <div id="file-lineage-breadcrumb"></div>
                <div id="file-title-strip" class="bin-sim-strip" style="margin-bottom: 20px; cursor: context-menu;"
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
                    <span id="file-title-text" style="font-weight:bold; color:var(--accent); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:30%;">unknown</span>
                    <span id="file-md5-text" style="font-family: 'JetBrains Mono', 'Consolas', monospace; color: var(--dim); font-size: 0.8rem; margin-right: 10px;">(MD5: ---)</span>
                    <span id="file-tags-container" style="display: inline-flex; gap: 4px; flex-wrap: wrap; align-items: center; min-width: 0; flex: 1;"></span>
                    <span id="file-note-btn-container" style="margin-left:auto; display: inline-flex; align-items: center;"></span>
                </div>

                <div class="bsim-tabbar" id="file-view-tabs">
                    <button class="bsim-tab" id="file-tab-btn-files" onclick="FileView.switchTab('files')" style="display: none;">Files</button>
                    <button class="bsim-tab active" id="file-tab-btn-metadata" onclick="FileView.switchTab('metadata')">Metadata (<span id="metadata-count">0</span>)</button>
                    <button class="bsim-tab" id="file-tab-btn-functions" onclick="FileView.switchTab('functions')">Functions (<span id="functions-count">0</span>)</button>
                    <button class="bsim-tab" id="file-tab-btn-clusters" onclick="FileView.switchTab('clusters')">Clusters (<span id="cluster-count">0</span>)</button>
                    <button class="bsim-tab" id="file-tab-btn-extracted_from" onclick="FileView.switchTab('extracted_from')" style="display: none;">Extracted From</button>
                    <button class="bsim-tab" id="file-tab-btn-neighbors" onclick="FileView.switchTab('neighbors')">Similar<span id="nbr-count-wrap" style="display:none;"> (<span id="nbr-count">0</span>)</span></button>
                </div>

                <!-- Files Tab Panel -->
                <div id="file-panel-files" class="file-view-panel" style="display: none;">
                    <div id="file-lineage-children-panel"></div>
                </div>

                <!-- Extracted From Tab Panel -->
                <div id="file-panel-extracted_from" class="file-view-panel" style="display: none;">
                    <div id="file-lineage-parents-panel"></div>
                </div>

                <!-- Metadata Tab Panel (Default Active) -->
                <div id="file-panel-metadata" class="file-view-panel" style="display: block;">
                    <div style="display: flex; flex-direction: column; gap: 20px;">
                        <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; ">
                            <div id="file-meta-container">
                                <!-- Reused comparison table layout here -->
                            </div>
                        </div>

                        <div class="card" id="inferred-meta-card" style="display: none; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; ">
                            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid var(--border); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                                <i class="fa-solid fa-wand-magic-sparkles"></i> Inferred Metadata
                            </div>
                            <div class="meta-grid" id="inferred-meta" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;"></div>
                        </div>
                    </div>
                </div>

                <!-- Functions Tab Panel -->
                <div id="file-panel-functions" class="file-view-panel" style="display: none;">
                  <div style="display:flex; gap:16px; align-items:stretch; min-height:0;">
                    <div id="fv-tree-sidebar">
                        <div class="bsim-side-title">
                            Function tag tree
                            <span class="bsim-side-actions">
                                <span onclick="FileView.clearTreeSelection()" title="Clear the tag filter">clear</span>
                            </span>
                        </div>
                        <div id="fv-axis-pick" class="bsim-axis-pick"></div>
                        <div id="fv-tree" class="bsim-tree"></div>
                        <div id="fv-chips" class="bsim-chips"></div>
                    </div>
                    <div class="card" style="flex:1; min-width:0; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; display: flex; flex-direction: column; gap: 15px;">
                        <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                            <div class="view-toggle" style="margin:0; display:flex; align-items:center;">
                                <span class="bsim-ctl-label">Group by:</span>
                                <button class="view-btn active" id="fv-group-btn-auto" onclick="FileView.setGroupBy('auto')" title="Group by tag when more than one tag is in scope">Auto</button>
                                <button class="view-btn" id="fv-group-btn-tag" onclick="FileView.setGroupBy('tag')" title="Always group by tag">Tag</button>
                                <button class="view-btn" id="fv-group-btn-none" onclick="FileView.setGroupBy('none')" title="One flat list">None</button>
                            </div>
                        </div>
                        <!-- ponytail: viewport-relative instead of a flex chain; 260px is the title strip + tabbar + card padding above it -->
                        <div id="file-func-scroll" style="overflow-x: auto; max-height: calc(100vh - 260px); min-height: 300px; overflow-y: auto;">
                            <table class="file-func-table" id="file-func-table">
                                <thead>
                                    <tr>
                                        <th class="sortable" onclick="FileView.toggleSort('function_name')">Function <span id="sort-icon-function_name">↕</span></th>
                                        <th class="sortable" onclick="FileView.toggleSort('entrypoint_address')">Entrypoint <span id="sort-icon-entrypoint_address">↕</span></th>
                                        <th>Tags</th>
                                        <th>Clusters</th>
                                        <th class="sortable" onclick="FileView.toggleSort('bsim_features_count')">Features <span id="sort-icon-bsim_features_count">↕</span></th>
                                        <th>Notes</th>
                                    </tr>
                                    <tr class="filter-row">
                                        <th>
                                            <div style="display:flex; flex-direction:column; gap:4px;">
                                                <input type="text" id="flt-func-name" placeholder="Name..." style="width:100%;" onfocus="FileView.attachFieldAutocomplete(this, 'function_name')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <div style="display:flex; gap:2px;">
                                                    <input type="text" id="flt-func-namespace" placeholder="Namespace..." style="width:50%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'namespace')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                    <input type="text" id="flt-func-ret_type" placeholder="Return type..." style="width:50%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'return_type')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                </div>
                                            </div>
                                        </th>
                                        <th><input type="text" id="flt-func-address" placeholder="Addr..." style="width:100%;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th><input type="text" id="flt-func-tag" placeholder="Tag..." style="width:100%;" onfocus="FileView.attachTagFilterAutocomplete(this)" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th>
                                            <div style="display:flex; flex-direction:column; gap:2px;">
                                                <input type="text" id="flt-func-cluster" placeholder="UUID..." style="width:100%; font-size:0.6rem;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <input type="text" id="flt-func-cluster-name" placeholder="Cluster name..." style="width:100%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'cluster_name')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <input type="number" id="flt-func-min-cohesion" placeholder="Min cohesion..." value="0.95" step="0.05" min="0" max="1" title="Min Cluster Cohesion" style="width:100%; font-size:0.6rem;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                            </div>
                                        </th>
                                        <th><input type="number" id="flt-func-min-features" placeholder="Min" min="0" title="Min Features" style="width:100%;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th><input type="text" id="flt-func-note-owner" placeholder="Note owner..." style="width:100%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'note_owners')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                    </tr>
                                </thead>
                                <tbody id="file-functions-tbody">
                                    <tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading functions...</td></tr>
                                </tbody>
                            </table>
                        </div>
                        <div id="file-func-status" class="dim" style="font-size:0.7rem; text-align:center;"></div>
                    </div>
                  </div>
                </div>

                <!-- Clusters Tab Panel -->
                <div id="file-panel-clusters" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; ">
                        <div class="cluster-list" id="cluster-list" style="display: flex; flex-direction: column; gap: 10px;"></div>
                    </div>
                </div>

                <!-- Neighbors Tab Panel -->
                <div id="file-panel-neighbors" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3); display: flex; flex-direction: column; gap: 15px;">
                        <div class="filter-bar" style="gap:20px; padding:0;">
                            <div class="search-input-wrapper">
                                <input type="text" id="nbr-q" placeholder="Search similar files by keywords..." oninput="FileView.debounceNeighborsSearch()">
                                <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="FileView.searchNeighbors()" title="Search"></i>
                            </div>
                        </div>
                        <div style="display:flex; gap:20px; flex-wrap:wrap;">
                            <div class="home-card" style="padding:16px; min-width:180px;">
                                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Scope</h3>
                                <input type="hidden" id="nbr-scope" value="collection">
                                <div id="nbr-scope-pills" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                            </div>
                            <div class="home-card" style="padding:16px; min-width:300px;">
                                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Scoring Metric</h3>
                                <input type="hidden" id="nbr-score-type" value="score">
                                <div id="nbr-score-type-pills" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                            </div>
                            <div class="home-card" style="padding:16px; min-width:100px;">
                                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Limit</h3>
                                <input type="number" id="nbr-limit" value="50" min="1" max="1000" style="width:70px; font-size:0.8rem; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:5px;" oninput="FileView.debounceNeighborsSearch()">
                            </div>
                            <div class="home-card" style="padding:16px; min-width:160px;">
                                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Packer</h3>
                                <input type="hidden" id="nbr-hide-packed" value="">
                                <div id="nbr-hide-packed-pill" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                            </div>
                        </div>
                        <div style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
                            <table id="nbr-results-table">
                                <thead>
                                    <tr>
                                        <th>Score</th>
                                        <th>File</th>
                                        <th>MD5</th>
                                        <th>Arch</th>
                                        <th>Funcs</th>
                                        <th>Coverage</th>
                                        <th>Shared</th>
                                        <th>Tags</th>
                                    </tr>
                                    <tr class="filter-row">
                                        <th>
                                            <div style="display:flex; align-items:center; gap:2px;">
                                                <input type="number" id="nbr-min-score" placeholder="Min..." value="0.9" step="0.05" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                                <span class="dim" style="font-size:0.6rem">-</span>
                                                <input type="number" id="nbr-max-score" placeholder="Max..." step="0.05" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                            </div>
                                        </th>
                                        <th><input type="text" id="nbr-file-name" placeholder="File Name..." style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()"></th>
                                        <th></th>
                                        <th><input type="text" id="nbr-arch" placeholder="Arch..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()"></th>
                                        <th>
                                            <div style="display:flex; align-items:center; gap:2px;">
                                                <input type="number" id="nbr-min-funcs" placeholder="Min..." min="0" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                                <span class="dim" style="font-size:0.6rem">-</span>
                                                <input type="number" id="nbr-max-funcs" placeholder="Max..." min="0" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                            </div>
                                        </th>
                                        <th>
                                            <div style="display:flex; align-items:center; gap:2px;">
                                                <input type="number" id="nbr-min-cov" placeholder="Min..." step="0.1" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                                <span class="dim" style="font-size:0.6rem">-</span>
                                                <input type="number" id="nbr-max-cov" placeholder="Max..." step="0.1" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                            </div>
                                        </th>
                                        <th><input type="number" id="nbr-min-shared" placeholder="Min..." min="0" style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()"></th>
                                        <th>
                                            <div style="display:flex; flex-direction:column; gap:2px;">
                                                <input type="text" id="nbr-file-tag" placeholder="Tags..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                                <input type="text" id="nbr-exclude-file-tag" placeholder="Exclude..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FileView.debounceNeighborsSearch()">
                                            </div>
                                        </th>
                                    </tr>
                                </thead>
                                <tbody id="nbr-results-tbody">
                                    <tr><td colspan="8" style="text-align: center; color: var(--dim); padding: 20px;">Loading similar files...</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;

        try {
            if (window.fetchTagMetadata) await window.fetchTagMetadata(collection);

            const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
            const res = await fetch(`/api/file/details/${file_md5}?${apiParams}`);
            if (!res.ok) throw new Error("File not found");
            const data = await res.json();

            if (data.error) throw new Error(data.error);

            document.getElementById('file-view-loader').style.display = 'none';
            document.getElementById('file-view-content').style.display = 'block';

            const file = data.file;
            this.clusters = data.bin_cluster_map || {};
            const inferredMeta = data.inferred_meta || {};

            // Render Hero Header
            const fileName = file.file_name || file.file_names?.[0] || 'Unknown Binary';
            window.filenameCache = window.filenameCache || {};
            window.filenameCache[file.file_md5] = fileName;
            document.getElementById('file-title-text').innerText = fileName;
            document.getElementById('file-md5-text').innerText = `(MD5: ${file.file_md5})`;

            // buildFunctionsQuery and the lineage panel both need the raw doc.
            this.file = file;
            this.fvLoadTagIndex();
            if (file.is_container) {
                document.getElementById('file-md5-text').insertAdjacentHTML('beforebegin',
                    `<span class="badge" title="Container: holds code but is not code itself. Its function count is the total of everything below it."
                        style="font-size:0.7rem; margin-right:8px;"><i class="fa-solid fa-box-archive"></i> Container</span>`);
            }

            // Pre-populate functions count
            document.getElementById('functions-count').innerText = file.function_count || 0;

            if (typeof Breadcrumbs !== 'undefined') {
                Breadcrumbs.setFilename(file.file_md5, fileName);
                Breadcrumbs.refresh();
            }

            // Bind data-entity-data on the strip for context menu functionality
            const fileId = `${collection}:file:${file_md5}`;
            const strip = document.getElementById('file-title-strip');
            if (strip) {
                const entityData = {
                    id: file.file_id || fileId,
                    name: fileName,
                    md5: file.file_md5,
                    note_owners: file.note_owners || [],
                    user_tags: file.user_tags || [],
                    tags: file.tags || []
                };
                strip.setAttribute('data-entity-data', JSON.stringify(entityData).replace(/'/g, "&apos;"));
            }

            // Render Tags and Notes in Header (Inspired by File Strip in Sim view)
            if (window.renderTagEditor) {
                document.getElementById('file-tags-container').innerHTML = window.renderTagEditor(
                    'file', file.file_id || fileId, file.tags || [], file.user_tags || []
                );
            }
            if (window.EntityRenderer) {
                document.getElementById('file-note-btn-container').innerHTML = window.EntityRenderer.renderFileNoteButton(
                    file.file_id || fileId, file.note_owners || [], { raw_data: file }
                );
            }

            // Render Metadata Table (Reusing comparison table layout and styles)
            const fmt = (v) => {
                if (v === undefined || v === null || v === '') return '<span style="color:var(--subtle); opacity:0.5;">—</span>';
                if (Array.isArray(v)) return v.length ? v.join(', ') : '<span style="color:var(--subtle); opacity:0.5;">—</span>';
                return String(v);
            };
            const fmtDate = (timestamp) => {
                if (!timestamp) return '';
                const d = new Date(Number(timestamp) * 1000);
                return d.toLocaleString();
            };

            const iconMap = {
                'File Name': 'fa-solid fa-file',
                'Other Names': 'fa-solid fa-tags',
                'MD5': 'fa-solid fa-fingerprint',
                'Batch UUID': 'fa-solid fa-box',
                'Language': 'fa-solid fa-microchip',
                'AV Type': 'fa-solid fa-shield',
                'File Type': 'fa-solid fa-file-code',
                'Yara': 'fa-solid fa-biohazard',
                'CC IP': 'fa-solid fa-network-wired',
                'Functions': 'fa-solid fa-list-ol',
                'BSim Features': 'fa-solid fa-dna',
                'First Seen': 'fa-solid fa-clock',
                'Related MD5s': 'fa-solid fa-link'
            };

            const categories = [
                ['Identity', [
                    ['File Name', file.file_name],
                    ['Other Names', file.file_names],
                    ['MD5', file.file_md5],
                    ['Related MD5s', file.related_md5],
                    ['Batch UUID', file.batch_uuid],
                    ['First Seen', file.first_seen ? fmtDate(file.first_seen) : ''],
                ]],
                ['Classification', [
                    ['Language', file.language_id || file.language],
                    ['AV Type', file.avtype],
                    ['File Type', file.filetype],
                    ['Yara', file.yara],
                    ['CC IP', file.cc_ip],
                ]],
                ['Statistics', [
                    ['Functions', file.function_count],
                    ['BSim Features', file.bsim_features_count],
                ]]
            ];

            if (file.file_format && Object.keys(file.file_format).length > 0) {
                const formatFields = Object.entries(file.file_format).map(([k, v]) => [k, v]);
                categories.push(['File Format', formatFields]);
            }

            let rows = '';
            let metaCount = 0;
            for (const [cat, fields] of categories) {
                rows += `<tr><td class="bin-sim-mc-cat" colspan="2">${cat}</td></tr>`;
                for (const [label, val] of fields) {
                    const icon = iconMap[label] || 'fa-solid fa-circle-info';
                    rows += `<tr>
                        <td class="bin-sim-mc-label" style="display: flex; align-items: center; gap: 8px;"><i class="${icon}" style="width: 14px; text-align: center; color: var(--dim); opacity: 0.8;"></i>${label}</td>
                        <td>${fmt(val)}</td>
                    </tr>`;
                    if (val !== undefined && val !== null && val !== '') {
                        metaCount++;
                    }
                }
            }
            document.getElementById('metadata-count').innerText = metaCount;

            document.getElementById('file-meta-container').innerHTML = `
                <table class="bin-sim-mc-table">
                    <thead><tr><th>Field</th><th>Value</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            `;

            // Render Distributions for Clusters
            const self = this;
            function renderDist(title, icon, dist) {
                if (!dist || dist.length === 0) return '';
                
                const colors = ['#66d9ef', '#a6e22e', '#f92672', '#fd971f', '#ae81ff', '#e6db74', '#75715e'];
                
                let legendHtml = '';
                let totalPercent = 0;
                dist.forEach(d => totalPercent += (d.percent || 0));
                
                let pieData = dist.map((d, i) => {
                    const color = colors[i % colors.length];
                    legendHtml += `
                        <div style="display: flex; align-items: center; gap: 6px; font-size: 0.75rem; margin-bottom: 4px;">
                            <div style="width: 10px; height: 10px; background-color: ${color}; border-radius: 2px;"></div>
                            <span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 150px;" title="${d.value}">${d.value}</span>
                            <span style="color: var(--dim); margin-left: auto;">${d.percent || 0}%</span>
                        </div>
                    `;
                    return {...d, color: color, value: d.percent || 0};
                });
                
                if (totalPercent < 100) {
                    pieData.push({value: 100 - totalPercent, color: 'var(--border)', isDummy: true});
                }
                
                const width = 50;
                const height = 50;
                const radius = Math.min(width, height) / 2;
                
                const pie = d3.pie().value(d => d.value).sort(null);
                const arc = d3.arc().innerRadius(0).outerRadius(radius);
                
                const svg = d3.create("svg")
                    .attr("width", width)
                    .attr("height", height)
                    .attr("viewBox", `0 0 ${width} ${height}`)
                    .style("border-radius", "50%");
                    
                svg.append("g")
                    .attr("transform", `translate(${width/2},${height/2})`)
                    .selectAll("path")
                    .data(pie(pieData))
                    .join("path")
                    .attr("fill", d => d.data.color)
                    .attr("d", arc)
                    .append("title")
                    .text(d => d.data.isDummy ? "" : `${d.data.value}: ${d.value}%`);
                    
                const svgHtml = svg.node().outerHTML;
                
                return `
                    <div style="margin-top: 15px; padding: 10px; background: var(--border); border: 1px solid var(--border); border-radius: 6px;">
                        <div style="font-size: 0.75rem; color: var(--dim); margin-bottom: 10px; display: flex; align-items: center; gap: 6px;">
                            <i class="${icon}"></i> ${title}
                        </div>
                        <div style="display: flex; gap: 15px; align-items: center;">
                            <div style="flex-shrink: 0;">${svgHtml}</div>
                            <div style="display: flex; flex-direction: column; flex: 1; min-width: 0;">
                                  ${legendHtml}
                            </div>
                        </div>
                    </div>
                `;
            }

            // Render Clusters
            const clusterIds = file.bin_clusters || [];
            document.getElementById('cluster-count').innerText = clusterIds.length;
            let clustersHtml = '';
            
            if (clusterIds.length === 0) {
                clustersHtml = '<div class="dim" style="text-align:center; padding: 20px;">Binary does not belong to any clusters.</div>';
            } else {
                clusterIds.sort((a, b) => {
                    const cmA = this.clusters[a] || {};
                    const cmB = this.clusters[b] || {};
                    return (cmB.cohesion_score || 0) - (cmA.cohesion_score || 0);
                });

                clusterIds.forEach(cid => {
                    const cm = this.clusters[cid];
                    if (!cm) return;
                    
                    const name = cm.cluster_name || `Cluster ${cid}`;
                    const size = cm.size || cm.member_count || cm.members || cm.count || 0;
                    const cohesionScore = cm.cohesion_score || 0;
                    const cohesion = cohesionScore.toFixed(2);
                    const cohesionColor = d3.interpolateRdYlGn(cohesionScore);
                    
                    let distBadges = '';
                    distBadges += renderDist('Yara Distributions', 'fa-solid fa-biohazard', cm.yara_distribution);
                    distBadges += renderDist('AV Type Distributions', 'fa-solid fa-shield', cm.avtype_distribution);
                    distBadges += renderDist('File Type Distributions', 'fa-solid fa-file-code', cm.filetype_distribution);
                    distBadges += renderDist('CC IP Distributions', 'fa-solid fa-network-wired', cm.ccip_distribution);
                    distBadges += renderDist('File Name Distributions', 'fa-solid fa-file', cm.filename_distribution);
                    distBadges += renderDist('MD5 Distributions', 'fa-solid fa-fingerprint', cm.md5_distribution);

                    clustersHtml += `
                        <div class="cluster-item" style="background: var(--border); border: 1px solid var(--border); border-radius:6px; padding:12px; display:flex; flex-direction:column; gap:8px;">
                            <div class="cluster-item-header" style="margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center; font-weight:bold; font-size:0.95rem; color:var(--text);">
                                <span style="color: var(--accent);"><i class="fa-solid fa-bullseye" style="margin-right: 6px;"></i>${name}</span>
                                <a href="#" style="font-size:0.75rem; color:var(--dim); text-decoration:none;" onclick="FileView.openClusterFiles(event, ${escapeAttr(jsString(cm.cluster_uuid))})">View Binaries <i class="fa-solid fa-arrow-right"></i></a>
                            </div>
                            <div class="cluster-stat-badges" style="margin-bottom: 5px; display:flex; gap:10px; flex-wrap:wrap;">
                                <div class="stat-badge" style="background: var(--hover); border: 1px solid var(--border); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-users" style="color:var(--dim);"></i><span>Members: <span class="val" style="color:var(--accent); font-family: 'JetBrains Mono', 'Consolas', monospace;">${size}</span></span></div>
                                <div class="stat-badge" style="background: var(--hover); border: 1px solid var(--border); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-bullseye" style="color:var(--dim);"></i><span>Cohesion: <span class="val" style="color: ${cohesionColor}; font-family: 'JetBrains Mono', 'Consolas', monospace;">${cohesion}</span></span></div>
                            </div>
                            ${distBadges}
                        </div>
                    `;
                });
            }
            document.getElementById('cluster-list').innerHTML = clustersHtml;

            // Render Inferred Rows
            const renderInferredRow = (icon, label, mapObj) => {
                const keys = Object.keys(mapObj).sort((a,b) => mapObj[b].percent - mapObj[a].percent);
                if (keys.length === 0) return '';
                const badges = keys.map(k => {
                    const confObj = mapObj[k];
                    const confScore = confObj.percent;
                    const confColor = d3.interpolateRdYlGn(confScore / 100);
                    const clusterLink = Nav.buildUIUrl(collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(confObj.cluster_uuid)}`;
                    return `<a href="${clusterLink}" class="stat-badge" style="background: var(--hover); display: inline-flex; margin: 2px 4px 2px 0; text-decoration: none; transition: background 0.2s;" onclick="event.preventDefault(); Nav.openPath(${escapeAttr(jsString(clusterLink))}, event);"><span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace;">${k}</span> <span class="val" style="margin-left: 4px; color: ${confColor};">${confScore}%</span></a>`;
                }).join('');
                return `
                    <div class="meta-label" style="align-items: flex-start; margin-top: 4px; color: var(--dim); text-transform: uppercase; font-size: 0.75rem; display: flex; gap: 6px;"><i class="${icon}" style="width:14px; text-align:center;"></i> ${label}</div>
                    <div class="meta-value" style="display: flex; flex-wrap: wrap;">${badges}</div>
                `;
            };

            let inferredHtml = '';
            inferredHtml += renderInferredRow('fa-solid fa-file', 'File Name', inferredMeta.filename || {});
            inferredHtml += renderInferredRow('fa-solid fa-fingerprint', 'MD5', inferredMeta.md5 || {});
            inferredHtml += renderInferredRow('fa-solid fa-shield', 'AV Type', inferredMeta.avtype || {});
            inferredHtml += renderInferredRow('fa-solid fa-file-code', 'File Type', inferredMeta.filetype || {});
            inferredHtml += renderInferredRow('fa-solid fa-biohazard', 'Yara', inferredMeta.yara || {});
            inferredHtml += renderInferredRow('fa-solid fa-network-wired', 'CC IP', inferredMeta.ccip || {});

            if (inferredHtml) {
                document.getElementById('inferred-meta').innerHTML = inferredHtml;
                document.getElementById('inferred-meta-card').style.display = 'block';
            }

            // Unique-value counts appended to the filter placeholders
            if (typeof loadFieldCardinalities === 'function') {
                loadFieldCardinalities(collection, 'func', {
                    'function_name': 'flt-func-name',
                    'namespace': 'flt-func-namespace',
                    'return_type': 'flt-func-ret_type',
                    'cluster_name': 'flt-func-cluster-name',
                    'note_owners': 'flt-func-note-owner'
                });
            }

            // Breadcrumb and containment panel; not awaited, they only fill
            // their own containers and must not hold up the rest of the view.
            this.loadLineage(collection, file_md5);

            // Silently fetch functions so they're ready when switching tabs
            this.loadFunctionsTable();

            // Apply tab from URL hash
            this.applyTabFromHash();

            // Register hashchange listener
            if (!this._hashBound) {
                this._onHashChange = () => this.applyTabFromHash();
                window.addEventListener('hashchange', this._onHashChange);
                this._hashBound = true;
            }

            // Initialize Notes panel silently
            if (typeof window.showFileNotes === 'function') {
                window.showFileNotes(fileId, false);
            }

        } catch (err) {
            console.error(err);
            document.getElementById('file-view-loader').innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
        }
    },

    /**
     * Containment breadcrumb + "Extracted from / Contains" panel.
     *
     * Siblings need one extra lookup per parent: the lineage of a file lists
     * its parents, not its parents' other children. A file usually has one
     * parent, and multi-parent means the same child sitting in two archives,
     * so the neighbours are grouped per parent rather than merged.
     */
    async loadLineage(collection, file_md5) {
        const crumbEl = document.getElementById('file-lineage-breadcrumb');
        const parentsEl = document.getElementById('file-lineage-parents-panel');
        const childrenEl = document.getElementById('file-lineage-children-panel');
        if (!crumbEl) return;
        try {
            const lin = await Lineage.fetch(collection, file_md5);
            if (!this.container) return;   // view was destroyed mid-flight

            const siblingsByParent = {};
            const [, subtrees] = await Promise.all([
                Promise.all((lin.parents || []).map(async p => {
                    if (!p.exists) return;
                    try {
                        siblingsByParent[p.file_md5] = (await Lineage.fetch(collection, p.file_md5)).children || [];
                    } catch (e) {
                        console.error(e);
                    }
                })),
                // A container nested in this one is shown already expanded.
                Lineage.fetchSubtrees(collection, lin.children || []),
            ]);
            if (!this.container) return;

            crumbEl.innerHTML = Lineage.renderBreadcrumb(lin, collection);
            if (parentsEl) {
                parentsEl.innerHTML = Lineage.renderParents(lin, collection, siblingsByParent);
                if (window.TableSelection) {
                    parentsEl.querySelectorAll('.data-table').forEach(t => { if (t.id) new window.TableSelection(t.id); });
                }
            }
            if (childrenEl) {
                childrenEl.innerHTML = Lineage.renderChildren(lin, collection, subtrees);
                if (window.TableSelection) {
                    childrenEl.querySelectorAll('.data-table').forEach(t => { if (t.id) new window.TableSelection(t.id); });
                }
            }
        } catch (e) {
            console.error(e);
        }
    },

    switchTab(tabId, push = true) {
        document.querySelectorAll('#file-view-tabs .bsim-tab').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.file-view-panel').forEach(panel => panel.style.display = 'none');

        const btn = document.getElementById(`file-tab-btn-${tabId}`);
        if (btn) btn.classList.add('active');

        const panel = document.getElementById(`file-panel-${tabId}`);
        if (panel) panel.style.display = 'block';

        if (tabId === 'functions' && !this.functionsLoaded) {
            this.loadFunctionsTable();
        }
        if (tabId === 'neighbors') {
            this.loadNeighborsPanel();
        }

        if (push && location.hash.slice(1) !== tabId) {
            history.pushState(null, '', location.pathname + location.search + '#' + tabId);
        }
    },

    applyTabFromHash() {
        const allowedTabs = ['metadata', 'functions', 'clusters', 'extracted_from', 'files', 'neighbors'];
        let tab = location.hash.slice(1);

        const hasChildren = this.file && (this.file.child_count > 0 || this.file.is_container);
        const hasParents = this.file && !!this.file.parent_md5;

        const btnFiles = document.getElementById('file-tab-btn-files');
        if (btnFiles) btnFiles.style.display = hasChildren ? 'inline-block' : 'none';

        const btnExtracted = document.getElementById('file-tab-btn-extracted_from');
        if (btnExtracted) btnExtracted.style.display = hasParents ? 'inline-block' : 'none';

        if (!allowedTabs.includes(tab)) {
            if (hasChildren) tab = 'files';
            else tab = 'metadata';
        }

        if (tab === 'files' && !hasChildren) tab = 'metadata';
        if (tab === 'extracted_from' && !hasParents) tab = 'metadata';

        this.switchTab(tab, false);
    },

    // Filter inputs -> /api/function/search params. Same names the function
    // search view uses, so the server-side handling is shared.
    FUNC_FILTERS: {
        'flt-func-name': 'function_name',
        'flt-func-namespace': 'namespace',
        'flt-func-ret_type': 'return_type',
        'flt-func-address': 'entrypoint_address',
        'flt-func-tag': 'func_tag',
        'flt-func-cluster': 'cluster_uuid',
        'flt-func-cluster-name': 'cluster_name',
        'flt-func-min-cohesion': 'min_cohesion',
        'flt-func-min-features': 'min_features',
        'flt-func-note-owner': 'note_owner'
    },

    // attachAutocomplete rebinds focus/click/input on the element, so the inline
    // onfocus only ever runs once — same wiring as the function search view.
    attachFieldAutocomplete(input, field) {
        if (typeof attachAutocomplete !== 'function') return;
        attachAutocomplete(input, 'func', field, (val) => {
            input.value = val;
            this.applyFilters();
        });
    },

    attachTagFilterAutocomplete(input) {
        if (typeof attachTagAutocomplete !== 'function') return;
        attachTagAutocomplete(input, (val) => {
            input.value = val;
            this.applyFilters();
        });
    },

    applyFilters() {
        clearTimeout(this._filterTimer);
        this.loadFunctionsTable({ reset: true });
    },

    buildFunctionsQuery(offset) {
        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
        const p = new URLSearchParams(apiParams);
        // A container has no functions of its own, so file_md5= would show an
        // empty tab. Unpacking stops at MAX_DEPTH=2: root_md5 covers a whole
        // upload, and md5 (file_md5 OR parent_md5) covers a mid-tree subtree,
        // so between them every container's subtree is reachable.
        if (this.file && this.file.is_container) {
            p.set(this.file.root_md5 ? 'md5' : 'root_md5', file_md5);
        } else {
            p.set('file_md5', file_md5);
        }
        p.set('offset', offset);
        p.set('limit', this.FUNC_PAGE_SIZE);
        p.set('sort_by', this.sortState.col);
        p.set('sort_order', this.sortState.dir === 1 ? 'asc' : 'desc');
        for (const [id, param] of Object.entries(this.FUNC_FILTERS)) {
            const v = (document.getElementById(id)?.value || '').trim();
            if (v) p.set(param, v);
        }
        return p.toString();
    },

    async loadFunctionsTable({ reset = false } = {}) {
        if (this.funcPage.loading && !reset) return;
        // No `functionsLoaded` guard here: this is also the "load the next page"
        // entry point for the infinite scroll. Having everything already is what
        // the total check below covers; callers that only want the first page
        // check functionsLoaded themselves.
        if (!reset && this.funcPage.total !== null && this.functions.length >= this.funcPage.total) return;

        const tbody = document.getElementById('file-functions-tbody');
        if (reset) {
            this.functions = [];
            this.funcPage.total = null;
        }
        this.funcPage.loading = true;
        this.setFunctionsStatus('<i class="fa-solid fa-spinner fa-spin"></i> Loading...');

        // Bump on every request so a slow earlier page can't overwrite a newer filter's result
        const reqId = ++this.funcPage.reqId;

        try {
            const res = await fetch(`/api/function/search?${this.buildFunctionsQuery(this.functions.length)}`);
            if (!res.ok) throw new Error("Functions load failed");
            const data = await res.json();
            if (reqId !== this.funcPage.reqId) return;
            if (data.error) throw new Error(data.error);

            this.functions = this.functions.concat(data.functions || []);
            this.funcPage.total = data.total || 0;
            this.funcClusters = Object.assign(this.funcClusters || {}, data.clusters || {});
            document.getElementById('functions-count').innerText = this.funcPage.total;
            this.renderFunctionsTable();
            this.functionsLoaded = true;
        } catch (e) {
            console.error(e);
            if (reqId !== this.funcPage.reqId) return;
            if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color:#f92672; padding: 20px;"><i class="fa-solid fa-circle-exclamation"></i> Error loading functions: ${e.message}</td></tr>`;
            this.setFunctionsStatus('');
        } finally {
            if (reqId === this.funcPage.reqId) this.funcPage.loading = false;
        }
    },

    setFunctionsStatus(html) {
        const el = document.getElementById('file-func-status');
        if (el) el.innerHTML = html;
    },

    // Loads the next page whenever the table is scrolled near the bottom.
    bindFunctionsScroll() {
        const scroller = document.getElementById('file-func-scroll');
        if (!scroller || scroller._funcScrollBound) return;
        scroller._funcScrollBound = true;
        scroller.addEventListener('scroll', () => {
            if (scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 200) {
                this.loadFunctionsTable();
            }
        });
    },

    async loadNeighborsPanel() {
        if (this.neighborsLoaded) return;
        this.neighborsLoaded = true;

        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const scopeEl = document.getElementById('nbr-scope');
        if (scopeEl) scopeEl.value = poolId ? 'pool' : 'collection';
        this.renderScopePills();
        this.renderScoreTypePills();
        this.renderHidePackedPill();

        await this.searchNeighbors();
    },

    debounceNeighborsSearch() {
        if (this.neighborsDebounceTimer) clearTimeout(this.neighborsDebounceTimer);
        this.neighborsDebounceTimer = setTimeout(() => this.searchNeighbors(), 400);
    },

    async searchNeighbors() {
        const tbody = document.getElementById('nbr-results-tbody');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--dim); padding: 20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading similar files...</td></tr>';

        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const scope = document.getElementById('nbr-scope')?.value || (poolId ? 'pool' : 'collection');

        const qs = new URLSearchParams();
        qs.set('md5', file_md5);
        if (scope === 'pool' && poolId) qs.set('pool', poolId);
        else qs.set('collection', collection);

        qs.set('sort', document.getElementById('nbr-score-type')?.value || 'score');
        qs.set('min_score', document.getElementById('nbr-min-score')?.value || '0.9');

        const setIfVal = (id, key) => {
            const v = document.getElementById(id)?.value;
            if (v) qs.set(key, v);
        };
        setIfVal('nbr-q', 'q');
        setIfVal('nbr-max-score', 'max_score');
        setIfVal('nbr-file-name', 'file_name');
        setIfVal('nbr-arch', 'arch');
        setIfVal('nbr-min-funcs', 'min_funcs');
        setIfVal('nbr-max-funcs', 'max_funcs');
        setIfVal('nbr-min-cov', 'min_coverage');
        setIfVal('nbr-max-cov', 'max_coverage');
        setIfVal('nbr-min-shared', 'min_shared');
        qs.set('limit', document.getElementById('nbr-limit')?.value || '50');

        const tagList = (id) => (document.getElementById(id)?.value || '').split(',').map(s => s.trim()).filter(Boolean);
        tagList('nbr-file-tag').forEach(t => qs.append('file_tag', t));
        tagList('nbr-exclude-file-tag').forEach(t => qs.append('exclude_file_tag', t));
        // Anchored on this file's own md5 -- exclude_file_tag only drops the
        // *other* side of a pair (search_bin_sim.py), so this stays correct even
        // when file_md5 itself is UPX-packed.
        if (document.getElementById('nbr-hide-packed')?.value === 'true') {
            qs.append('exclude_file_tag', 'packer:upx');
        }

        try {
            const res = await fetch(`/api/bin_sim/search?${qs.toString()}`);
            if (!res.ok) throw new Error("Neighbors search failed");
            const data = await res.json();
            const items = data.items || data.results || [];
            const html = window.renderBinSimPairs ? window.renderBinSimPairs(items, 0, file_md5) : '';
            tbody.innerHTML = html || '<tr><td colspan="8" style="text-align: center; color: var(--dim); padding: 20px;">No similar files found.</td></tr>';
            const countEl = document.getElementById('nbr-count');
            if (countEl) countEl.innerText = data.total ?? items.length;
            const countWrap = document.getElementById('nbr-count-wrap');
            if (countWrap) countWrap.style.display = 'inline';
            if (window.TableSelection) new window.TableSelection('nbr-results-table');
            this.refreshNeighborPillCounts(qs);
        } catch (e) {
            console.error(e);
            tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color:#f92672; padding: 20px;"><i class="fa-solid fa-circle-exclamation"></i> Error loading similar files: ${e.message}</td></tr>`;
        }
    },

    renderScopePills() {
        const el = document.getElementById('nbr-scope-pills');
        if (!el || !window.binSimPillStyle) return;
        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const active = document.getElementById('nbr-scope')?.value || (poolId ? 'pool' : 'collection');
        const options = [
            { v: 'collection', label: 'Collection', icon: 'fa-solid fa-database', color: 'var(--info, #3b82f6)' },
            { v: 'pool', label: 'Pool', icon: 'fa-solid fa-layer-group', color: 'var(--warning, #d97706)', disabled: !poolId },
        ];
        el.innerHTML = options.map(o => `<span class="bsim-tag-pill" style="${window.binSimPillStyle(o.v === active, o.color)}${o.disabled ? ' opacity:0.4; cursor:not-allowed;' : ''}" title="${o.disabled ? 'No pool in this context' : escapeAttr(o.label)}" ${o.disabled ? '' : `onclick="FileView.setNeighborScope('${o.v}')"`}><i class="${o.icon}"></i>${o.label}</span>`).join('');
    },

    setNeighborScope(v) {
        const el = document.getElementById('nbr-scope');
        if (el) el.value = v;
        this.renderScopePills();
        this.searchNeighbors();
    },

    renderScoreTypePills() {
        const el = document.getElementById('nbr-score-type-pills');
        if (!el || !window.binSimPillStyle) return;
        const types = window.BinSimScoreTypes || { score: { label: 'Overall', icon: 'fa-solid fa-layer-group', color: 'var(--success)' } };
        const active = document.getElementById('nbr-score-type')?.value || 'score';
        el.innerHTML = Object.entries(types).map(([v, meta]) => `<span class="bsim-tag-pill" style="${window.binSimPillStyle(v === active, meta.color)}" title="${escapeAttr(meta.label)}" onclick="FileView.setNeighborScoreType('${v}')"><i class="${meta.icon}"></i>${meta.label} <span id="nbr-count-score-${v}" style="font-size:0.75rem; opacity:0.8; font-weight:normal;"></span></span>`).join('');
    },

    setNeighborScoreType(v) {
        const el = document.getElementById('nbr-score-type');
        if (el) el.value = v;
        this.renderScoreTypePills();
        this.searchNeighbors();
    },

    renderHidePackedPill() {
        const el = document.getElementById('nbr-hide-packed-pill');
        if (!el || !window.binSimPillStyle) return;
        const active = document.getElementById('nbr-hide-packed')?.value === 'true';
        el.innerHTML = `<span class="bsim-tag-pill" style="${window.binSimPillStyle(active, 'var(--danger, #dc2626)')}" title="Hide candidates that are themselves UPX-packed -- packer stub matches are nice for reference but drown out real payload similarity" onclick="FileView.toggleNeighborHidePacked()"><i class="fa-solid fa-box-archive"></i>Hide Packed</span>`;
    },

    toggleNeighborHidePacked() {
        const el = document.getElementById('nbr-hide-packed');
        if (!el) return;
        el.value = el.value === 'true' ? '' : 'true';
        this.renderHidePackedPill();
        this.searchNeighbors();
    },

    // Cheap limit=0 count-only requests per score type, so the pills show
    // "(N)" the same way the bin-sim search hero does -- one extra request
    // per type, fired after the real search so it never blocks results.
    refreshNeighborPillCounts(baseQs) {
        const types = window.BinSimScoreTypes || {};
        Object.keys(types).forEach(async (v) => {
            try {
                const u = new URLSearchParams(baseQs);
                u.set('sort', v);
                u.set('limit', '0');
                const res = await fetch(`/api/bin_sim/search?${u.toString()}`);
                if (!res.ok) return;
                const data = await res.json();
                const el = document.getElementById(`nbr-count-score-${v}`);
                if (el && data.total !== undefined) el.innerText = `(${data.total.toLocaleString()})`;
            } catch (e) {}
        });
    },

    toggleSort(col) {
        if (this.sortState.col === col) {
            this.sortState.dir = -this.sortState.dir;
        } else {
            this.sortState.col = col;
            this.sortState.dir = 1;
        }

        ['function_name', 'entrypoint_address', 'bsim_features_count'].forEach(c => {
            const el = document.getElementById(`sort-icon-${c}`);
            if (el) {
                el.innerText = this.sortState.col === c ? (this.sortState.dir === 1 ? '▲' : '▼') : '↕';
            }
        });

        this.loadFunctionsTable({ reset: true });
    },

    handleFilterChange() {
        clearTimeout(this._filterTimer);
        this._filterTimer = setTimeout(() => this.loadFunctionsTable({ reset: true }), 350);
    },

    handleFilterKey(e) {
        if (e.key === 'Enter') this.applyFilters();
    },

    renderFunctionsTable() {
        const tbody = document.getElementById('file-functions-tbody');
        if (!tbody) return;

        // Filtering, sorting and paging all happen server-side; render what we hold.
        if (this.functions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;">No functions found.</td></tr>';
            this.setFunctionsStatus('');
            return;
        }

        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;

        const rowHtml = (f) => {
            const entry = f.entrypoint_address || '';
            const funcName = f.function_name || 'unknown';
            const featCount = f.bsim_features_count || 0;
            const fColl = f.collection || collection;
            // A container's tab lists its whole subtree, so the owning file is
            // per row, not the file being viewed.
            const fMd5 = f.file_md5 || file_md5;
            const funcId = f.function_id || `${fColl}:func:${fMd5}:${entry}`;
            // renderFunction/context menu read these off the object; the search API may omit them
            f.collection = fColl;
            f.file_md5 = fMd5;
            f.function_id = funcId;

            // Notes
            const noteBtn = window.EntityRenderer ? window.EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true, raw_data: f }) : '';
            
            // Tags
            const tagsHtml = window.EntityRenderer ? window.EntityRenderer.renderTag('function', funcId, f.tags || [], f.user_tags || []) : '';
            
            // Clusters
            const cls = (f.clusters || []).map(uuid => (this.funcClusters || {})[uuid] || this.clusters[uuid]).filter(Boolean);
            const clusterCardHtml = window.EntityRenderer ? window.EntityRenderer.renderClusterCard(cls) : '';

            // Clickable details URL
            let poolId = null;
            if (window.getRoutingState && window.getRoutingState().pool) {
                poolId = window.getRoutingState().pool;
            }
            let detailUrl = `/collections/${encodeURIComponent(fColl)}/files/${fMd5}/functions/${entry}`;
            if (poolId) {
                detailUrl = `/pools/${encodeURIComponent(poolId)}` + detailUrl;
            }

            // Only meaningful for a container, where each row came out of a
            // different extracted file.
            const originHtml = fMd5 === file_md5 ? '' : `
                <div class="lineage-path" style="margin-top:2px;">
                    from <b class="lineage-link" onclick="event.stopPropagation(); openFileDetails(${escapeAttr(jsString(fColl))}, ${escapeAttr(jsString(fMd5))}, ${escapeAttr(jsString(f.file_name || ''))}, event)">${escapeHtml(middleTruncate(f.file_name || fMd5, 40))}</b>
                </div>`;

            return `
                <tr class="sim-row" style="font-size: 0.75rem;" data-id="${funcId}"
                    data-entity-data='${escapeAttr(JSON.stringify(f))}'
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                    <td class="sim-cell" style="min-width:300px;">
                        ${window.EntityRenderer ? window.EntityRenderer.renderFunction(f, { hideNote: true }) : funcName}
                        ${originHtml}
                    </td>
                    <td>
                        <a class="mono" href="${detailUrl}" onclick="event.preventDefault(); Nav.openPath(${escapeAttr(jsString(detailUrl))}, event);" style="color:var(--accent); text-decoration:none;">@ ${entry}</a>
                    </td>
                    <td>${tagsHtml}</td>
                    <td>${clusterCardHtml}</td>
                    <td>
                        <div style="display:inline-flex; align-items:center; gap:6px;">
                            <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                            <button class="btn-icon" onclick="showFeaturePanel(${escapeAttr(jsString(funcId))}, event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                        </div>
                    </td>
                    <td style="text-align:center;">${noteBtn}</td>
                </tr>
            `;
        };

        // Grouped: functions bucketed by their top-level tag node, folded with
        // the same fvOpen state the sidebar tree uses -- one dropdown tree,
        // shared between sidebar and table, exactly like bin-sim's bsim-grp-row.
        const nodes = this.fvTree();
        const grouped = this.fvGroupBy === 'tag'
            || (this.fvGroupBy === 'auto' && !this.fvSelectedTag && nodes.length > 1);

        if (!grouped) {
            tbody.innerHTML = this.functions.map(rowHtml).join('');
        } else {
            const dot = (id) => (typeof TagColor !== 'undefined')
                ? `<span class="bsim-node-dot" style="background:${TagColor.forTag(id)}; margin-right:6px;"></span>` : '';
            const out = [];
            const rest = new Set(this.functions);
            nodes.forEach(n => {
                const matches = this.functions.filter(f => (f.tags || []).some(t => t.startsWith(n.prefix)));
                matches.forEach(f => rest.delete(f));
                const open = this.fvOpen.has(n.id);
                out.push(`
                    <tr class="bsim-grp-row" onclick="FileView.toggleTreeNode(${escapeAttr(jsString(n.id))})">
                        <td colspan="6">
                            <div style="display:flex; align-items:center; gap:10px;">
                                <span class="bsim-caret-btn">${open ? '▼' : '▶'}</span>
                                ${dot(n.id)}
                                <span style="font-weight:600;">${escapeHtml(n.label)}</span>
                                <div style="flex:1;"></div>
                                <span style="color:var(--subtle); font-size:0.74rem;">${matches.length}</span>
                            </div>
                        </td>
                    </tr>`);
                if (open) matches.forEach(f => out.push(rowHtml(f)));
            });
            if (rest.size) {
                const open = this.fvOpen.has('__untagged__');
                out.push(`
                    <tr class="bsim-grp-row" onclick="FileView.toggleTreeNode('__untagged__')">
                        <td colspan="6">
                            <div style="display:flex; align-items:center; gap:10px;">
                                <span class="bsim-caret-btn">${open ? '▼' : '▶'}</span>
                                <span style="font-weight:600; color:var(--dim);">(untagged)</span>
                                <div style="flex:1;"></div>
                                <span style="color:var(--subtle); font-size:0.74rem;">${rest.size}</span>
                            </div>
                        </td>
                    </tr>`);
                if (open) [...rest].forEach(f => out.push(rowHtml(f)));
            }
            tbody.innerHTML = out.join('');
        }

        const shown = this.functions.length;
        const total = this.funcPage.total ?? shown;
        this.setFunctionsStatus(shown < total ? `Showing ${shown} of ${total} — scroll for more` : `${total} function${total === 1 ? '' : 's'}`);
        this.bindFunctionsScroll();
        this.renderTagTree();

        // TableSelection takes an element id, not an element (constructor is idempotent per table)
        if (window.TableSelection) {
            new window.TableSelection('file-func-table');
        }
    },

    // ---- Function tag tree sidebar --------------------------------------
    // Ported from bin-sim's tag tree (binary_similarity.js: fileSimTree /
    // fileSimTreeRoot / fileSimAxisNodes). That tree reads backend-precomputed
    // `tags_summary` rows shaped for a TWO-SIDED comparison (a/b counts per
    // tag, drift between sides) -- genuinely comparison-specific data, not a
    // flat `{tags:[...]}` shape, so it isn't reusable as-is for one file's
    // function list. What IS reused verbatim: the CSS classes/layout
    // (#fv-tree-sidebar mirrors #bsim-sidebar), `fileSimTagParts()` and
    // `TagColor.forTag()` from binary_similarity.js (both globals, already
    // loaded on this page) for tag decomposition/colour, and the same
    // click-to-filter interaction feel. The tree itself -- counting tags
    // across `this.functions` and grouping name -> version -- is new, small,
    // and single-sided by construction (no drift/sankey/cross-axis: those are
    // two-sided-only concepts with nothing to port).
    // ponytail: tree only reflects functions already paged into `this.functions`
    // (grows as you scroll, narrows with any active filter -- same "scope
    // narrows everything" feel bin-sim's chips have), not a full-file
    // aggregate. A dedicated backend summary endpoint would fix that; skip
    // until someone needs whole-file counts before scrolling/filtering.
    fvAxis: '',
    fvSelectedTag: null,
    // Which tree nodes are unfolded, keyed by node id -- which is a tag id.
    fvOpen: new Set(),
    // Tag counts across the WHOLE file, independent of whatever the table's
    // filter row currently has typed in. Scoping the table to one tag used to
    // rebuild the tree from `this.functions` -- which is the *filtered* page --
    // so the tree would collapse down to just the branch you clicked. This is
    // fetched once per file (fvLoadTagIndex) and never touched by table filters.
    fvTagIndex: null,

    // Pages through every function this file has, ignoring the table's filter
    // row, to build a stable tag count. Runs once per file load, in the
    // background; the tree/axis picker fall back to the filtered page's counts
    // until it lands.
    // ponytail: no backend tag-aggregate endpoint exists yet (bin-sim's
    // tags_summary is two-sided-comparison-shaped, not reusable here -- see the
    // note on fvTree below), so this pages the same search endpoint with a
    // bigger limit and no filters. Capped at 20k functions; add a real
    // aggregate endpoint if a file blows past that.
    async fvLoadTagIndex() {
        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
        const counts = {};
        let offset = 0;
        const PAGE = 500, CAP = 20000;
        try {
            while (offset < CAP) {
                const p = new URLSearchParams(apiParams);
                if (this.file && this.file.is_container) {
                    p.set(this.file.root_md5 ? 'md5' : 'root_md5', file_md5);
                } else {
                    p.set('file_md5', file_md5);
                }
                p.set('offset', offset);
                p.set('limit', PAGE);
                const res = await fetch(`/api/function/search?${p.toString()}`);
                if (!res.ok) break;
                const data = await res.json();
                (data.functions || []).forEach(f => (f.tags || []).forEach(t => { counts[t] = (counts[t] || 0) + 1; }));
                offset += PAGE;
                if (!data.functions || data.functions.length < PAGE || offset >= (data.total || 0)) break;
            }
        } catch (e) {
            console.error('tag index load failed', e);
        }
        this.fvTagIndex = counts;
        this.renderTagTree();
    },

    fvTagCounts() {
        if (this.fvTagIndex) return this.fvTagIndex;
        const counts = {};
        (this.functions || []).forEach(f => {
            (f.tags || []).forEach(t => { counts[t] = (counts[t] || 0) + 1; });
        });
        return counts;
    },

    // The axes this file actually carries mass on, named the way Bin Sim names
    // them. The namespace -> axis map comes from `/api/tags/colors`, so both
    // views put a tag on the same axis instead of each keeping its own table.
    fvAvailableAxes() {
        const counts = this.fvTagCounts();
        const axes = new Set();
        Object.keys(counts).forEach(tagId => axes.add(TagColor.axisOf(tagId)));
        return [...axes].sort();
    },

    // One trie over the tag ids, the same shape Bin Sim's tree has: a node id is
    // a real tag id and a literal prefix of everything beneath it, depth is
    // whatever the ids have, and a detail tail is never a level -- so the
    // function a library was matched on cannot become a category of its own.
    //
    // This used to read `fileSimTagParts`, which flattened every id to
    // name/version and could only ever draw two levels.
    fvTree() {
        const counts = this.fvTagCounts();
        const axis = this.fvAxis;
        const root = { children: new Map() };

        Object.entries(counts).forEach(([tagId, count]) => {
            if (TagColor.axisOf(tagId) !== axis) return;
            let node = root;
            TagColor.chain(tagId).forEach(prefix => {
                let next = node.children.get(prefix);
                if (!next) {
                    const segs = TagColor.levels(prefix).segs;
                    next = {
                        id: prefix, prefix,
                        label: segs[segs.length - 1] || prefix,
                        count: 0, children: new Map(),
                    };
                    node.children.set(prefix, next);
                }
                next.count += count;
                node = next;
            });
        });

        const finish = (node) => {
            const kids = [...node.children.values()].map(finish);
            kids.sort((a, b) => b.count - a.count);
            node.children = kids;
            return node;
        };
        let nodes = finish(root).children;
        // The picker already names the namespace, so a lone top node repeats it.
        // One level only, matching Bin Sim.
        if (nodes.length === 1 && nodes[0].children.length) nodes = nodes[0].children;
        return nodes;
    },

    fvRenderAxisPicker() {
        const host = document.getElementById('fv-axis-pick');
        if (!host) return;
        const avail = this.fvAvailableAxes();
        if (!avail.includes(this.fvAxis)) this.fvAxis = avail[0] || '';
        // Always shown -- every axis this file has tags on stays pickable
        // regardless of the current tag scope, not just while >1 exists.
        host.innerHTML = !avail.length ? '' : `
            <div class="view-toggle" style="margin:0; flex:1; min-width:0;">
                <span class="bsim-ctl-label" style="margin:4px 6px;">Axis:</span>
                <select class="view-btn" style="flex:1; min-width:0;" onchange="FileView.setTreeAxis(this.value)">
                    ${avail.map(a => `<option value="${escapeAttr(a)}"${a === this.fvAxis ? ' selected' : ''}>${escapeHtml(a)}</option>`).join('')}
                </select>
            </div>`;
    },

    fvRenderTree() {
        const host = document.getElementById('fv-tree');
        if (!host) return;
        const nodes = this.fvTree();
        if (!nodes.length) {
            host.innerHTML = '<div style="color:var(--dim); padding:10px 12px; font-size:0.78rem;">No tag data yet.</div>';
            return;
        }
        const dot = (id) => (typeof TagColor !== 'undefined')
            ? `<span class="bsim-node-dot" style="background:${TagColor.forTag(id)};"></span>` : '';
        const out = [];
        // Depth is whatever the ids have, so this recurses rather than
        // unrolling two levels the way the name/version tree did.
        const walk = (n, depth) => {
            const hasKids = n.children.length > 0;
            const open = this.fvOpen.has(n.id);
            const caret = hasKids
                ? `<span class="bsim-caret" onclick="event.stopPropagation(); FileView.toggleTreeNode(${escapeAttr(jsString(n.id))})">${open ? '▾' : '▸'}</span>`
                : '<span class="bsim-caret"></span>';
            out.push(`
            <div class="bsim-node${this.fvSelectedTag === n.id ? ' selected' : ''}" style="padding-left:${8 + depth * 14}px;"
                 onclick="FileView.selectTreeNode(${escapeAttr(jsString(n.id))})">
                ${caret}
                ${dot(n.id)}
                <span class="bsim-node-label">${escapeHtml(n.label)}</span>
                <span class="bsim-node-count">${n.count}</span>
            </div>`);
            if (open) n.children.forEach(c => walk(c, depth + 1));
        };
        nodes.forEach(n => walk(n, 0));
        host.innerHTML = out.join('');
    },

    renderTagTree() {
        this.fvRenderAxisPicker();
        this.fvRenderTree();
        this.fvRenderChips();
    },

    fvRenderChips() {
        const host = document.getElementById('fv-chips');
        if (!host) return;
        if (!this.fvSelectedTag) {
            host.innerHTML = '<span style="font-size:0.72rem; color:var(--dim); font-family:sans-serif;">Whole file — select a tag above to scope.</span>';
            return;
        }
        host.innerHTML = `
            <span class="bsim-chip">tag: <b>${escapeHtml(this.fvSelectedTag)}</b>
                <span class="bsim-chip-x" title="Remove this scope" onclick="FileView.clearTreeSelection()">✕</span>
            </span>`;
    },

    setGroupBy(mode) {
        this.fvGroupBy = mode;
        ['auto', 'tag', 'none'].forEach(m => {
            const btn = document.getElementById(`fv-group-btn-${m}`);
            if (btn) btn.classList.toggle('active', m === mode);
        });
        this.renderFunctionsTable();
    },

    setTreeAxis(axis) {
        this.fvAxis = axis;
        this.fvSelectedTag = null;
        this.renderTagTree();
    },

    toggleTreeNode(id) {
        if (this.fvOpen.has(id)) this.fvOpen.delete(id);
        else this.fvOpen.add(id);
        this.renderFunctionsTable();
    },

    // Every node is a real tag id, so selecting one filters by that id as a
    // prefix -- the wildcard syntax /api/function/search already supports
    // (query_syntax.py). It is a prefix even at a leaf: `fid:libc:2.31` has to
    // catch `fid:libc:2.31#memcpy`, because the functions carrying a library's
    // mass are tagged with the symbol they matched on.
    selectTreeNode(id) {
        this.fvSelectedTag = id;
        const input = document.getElementById('flt-func-tag');
        if (input) input.value = `${id}*`;
        this.fvRenderTree();
        this.applyFilters();
    },

    clearTreeSelection() {
        this.fvSelectedTag = null;
        const input = document.getElementById('flt-func-tag');
        if (input) input.value = '';
        this.fvRenderTree();
        this.applyFilters();
    },

    openFunctions(e) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['search', 'functions']) + `?file_md5=${encodeURIComponent(this.params.md5)}`;
        Nav.openPath(url, e, { title: 'Functions', type: 'functions' });
    },

    openCallGraph(e) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['call_graph', this.params.md5]);
        Nav.openPath(url, e, { title: `Call Graph: ${this.params.md5.substring(0, 8)}`, type: 'call_graph' });
    },

    openClusterFiles(e, clusterUuid) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(clusterUuid)}`;
        Nav.openPath(url, e, { title: 'Cluster Files', type: 'files' });
    },

    showNotes(e) {
        e.preventDefault();
        if (window.showFileNotes) window.showFileNotes(`${this.params.collection}:file:${this.params.md5}`, true);
    },

    destroy() {
        if (this._hashBound) {
            window.removeEventListener('hashchange', this._onHashChange);
            this._hashBound = false;
        }
        clearTimeout(this._filterTimer);
        this.funcPage.reqId++;   // orphan any request still in flight
        this.container = null;
        this.params = null;
        this.functions = [];
        this.clusters = {};
        this.funcClusters = {};
        this.file = null;
        this.funcPage = { total: null, loading: false, reqId: 0 };
        this.functionsLoaded = false;
        this.neighborsLoaded = false;
        if (this.neighborsDebounceTimer) clearTimeout(this.neighborsDebounceTimer);
    }
};
