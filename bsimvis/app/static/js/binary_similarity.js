// Binary Similarity View Logic

// Score types a bin_sim pair doc can carry, keyed by the `sort` param value.
// `field` is the doc key `renderBinSimPairs` reads for that type -- Overall
// stays `score` for backward-compat URLs. Order here is card display order.
window.BinSimScoreTypes = {
    score: { label: 'Overall', field: 'score', icon: 'fa-solid fa-layer-group', color: 'var(--success)' },
    score_code: { label: 'Code', field: 'score_code', icon: 'fa-solid fa-code', color: 'var(--info, #3b82f6)' },
    score_library: { label: 'Library', field: 'score_library', icon: 'fa-solid fa-cubes', color: 'var(--warning, #d97706)' },
    score_content: { label: 'Content', field: 'score_content', icon: 'fa-solid fa-file-image', color: 'var(--accent, #9333ea)' },
};

let binSimDataCache = null;
let binSimMetaCtx = null;
let binSimMetaCache = null;
let metaHighlightMode = 'different';
// The composition Sankey lives under the Summary rollup rather than behind a view toggle,
// and folds with the tree, so neither a view nor a depth setting is needed.
let fileSimScale = 'count';     // 'count' | 'features'
// One table now, so one sort state. Keyed `matched` for continuity with the
// filter-input ids the header still uses.
let binSimSortState = {
    matched: { col: 'similarity', dir: -1 },
};

// Change 4 (frontend): server-paged tables. The compact summary payload feeds the
// tree and the composition flow; rows are paged in on demand.
const BINSIM_LIMIT = 100;
let binSimCtx = null;        // {collection, md5a, md5b, collB, poolId}

function renderBinarySimilarityView(params) {
    const container = document.getElementById('binary-similarity-container');
    const collection = params.get('collection');
    if (!collection) {
        throw new Error("renderBinarySimilarityView: collection is required.");
    }
    let md5a = params.get('md5_a');
    let md5b = params.get('md5_b');
    let collB = params.get('coll_b');
    let poolId = params.get('pool_id');

    // Parse new RESTful URL using routing state or fallback
    if (!md5a || !md5b || !collB || !poolId) {
        if (window.getRoutingState) {
            const state = window.getRoutingState();
            md5a = md5a || state.md5;
            md5b = md5b || state.md5_b;
            collB = collB || state.coll_b;
            poolId = poolId || state.pool;
        }
    }

    // Index-based fallback for coll_b
    if (!collB) {
        const parts = window.location.pathname.split('/').filter(Boolean);
        const vsIdx = parts.indexOf('vs');
        if (vsIdx !== -1 && vsIdx + 1 < parts.length) {
            collB = decodeURIComponent(parts[vsIdx + 1]);
        }
    }

    
    // Set up layout: Header (Selection/Summary) + Body (Sankey / Tables)
    let html = `
        <div id="bin-sim-results" style="display:none; flex:1; flex-direction:column; padding:20px; min-height:0; overflow-y:auto;">
            <!-- Similarity Hero (prominent, score-colored) -->
            <div id="bin-sim-hero" style="border: 1px solid var(--border); border-radius: 8px; padding: 18px 20px; margin-bottom: 12px; display: flex; align-items: center; justify-content: center; gap: 16px; background: var(--card-bg);">
                <span style="color: var(--subtle); text-transform: uppercase; font-size: 0.8rem; font-weight: bold; letter-spacing: 0.08em;">Binary Similarity</span>
                <span id="bin-sim-score-val" style="font-family: 'Consolas', monospace; font-weight: 800; font-size: 2.4rem; line-height: 1; color: var(--accent);">--%</span>
            </div>

            <!-- Resplit pill: own row right under the hero, not inside it
                 (hero's innerHTML is fully replaced on every pair load, which
                 was silently wiping this out). Hidden unless stale. -->
            <div id="bin-sim-resplit-banner" style="display:none; margin-bottom:12px; justify-content:flex-end;"></div>

            <!-- Slim per-binary strip: user tags + notes only -->
            <div style="display: flex; gap: 20px; margin-bottom: 12px;">
                <div id="bin-sim-strip-a" class="bin-sim-strip" style="flex: 1; min-width: 0;"></div>
                <div id="bin-sim-strip-b" class="bin-sim-strip" style="flex: 1; min-width: 0;"></div>
            </div>

            <!-- Main view: the tag tree scopes the detail pane beside it. Matched
                 and Unmatched are states of the one detail table, not tables of
                 their own, so every tab means the same thing for any selection. -->
            <div id="bsim-main">
                <div id="bsim-sidebar">
                    <div class="bsim-side-title">
                        Function tag tree
                        <span class="bsim-side-actions">
                            <span onclick="expandAllFileSimNodes()" title="Expand every tag node">expand all</span>
                            <span onclick="collapseAllFileSimNodes()" title="Collapse every tag node">collapse all</span>
                        </span>
                    </div>
                    <!-- Which axis the tree reads. It lives here, not with the
                         flow controls, because the tree scopes every tab. -->
                    <div id="bsim-axis-pick" class="bsim-axis-pick"></div>
                    <div id="bsim-tree" class="bsim-tree"></div>
                    <div class="bsim-side-sep"></div>
                    <div class="bsim-side-nav">
                        <div class="bsim-nav-item" id="bsim-nav-metadata" onclick="switchBinSimTab('metadata')">Metadata</div>
                        <div class="bsim-nav-item" id="bsim-nav-inferred" onclick="switchBinSimTab('inferred')">Clusters</div>
                    </div>
                </div>

                <div id="bsim-detail">
                    <!-- Detail tabs: presets on one table, plus its own Summary -->
                    <div class="bsim-tabbar" id="bin-sim-tabs">
                        <button class="bsim-tab active" id="bin-sim-tab-btn-summary" onclick="switchBinSimTab('summary')">Summary</button>
                        <button class="bsim-tab" id="bin-sim-tab-btn-all" onclick="switchBinSimTab('all')">All</button>
                        <button class="bsim-tab" id="bin-sim-tab-btn-matched" onclick="switchBinSimTab('matched')">Matched</button>
                        <button class="bsim-tab" id="bin-sim-tab-btn-unique_a" onclick="switchBinSimTab('unique_a')">Unique to A</button>
                        <button class="bsim-tab" id="bin-sim-tab-btn-unique_b" onclick="switchBinSimTab('unique_b')">Unique to B</button>
                        <button class="bsim-tab" id="bin-sim-tab-btn-unmatched" onclick="switchBinSimTab('unmatched')">Unmatched</button>
                    </div>

                    <!-- Global scope chips: the tree selection, removable from here too -->
                    <div id="bsim-chips" class="bsim-chips"></div>

                    <!-- Summary: stats for the selection, a rollup that folds with the
                         tree, and the same composition drawn as flow underneath. -->
                    <div class="bsim-subtab-panel" id="bsim-panel-summary" style="flex:1; min-height:0; display:flex; flex-direction:column; overflow:auto; gap:12px;">
                        <div id="bsim-summary-head"></div>
                        <div id="bsim-summary-rollup"></div>
                        <div style="display:flex; align-items:center; gap:14px; flex-wrap:wrap; margin-top:4px;">
                            <span class="bsim-ctl-label" style="margin:0;">Composition flow</span>
                            <div class="view-toggle" id="bin-sim-filesim-scale-toggle" style="margin:0; align-items:center; display:flex;">
                                <span class="bsim-ctl-label">Scale:</span>
                                <button class="view-btn ${fileSimScale === 'count' ? 'active' : ''}" id="bsim-filesim-scale-btn-count" onclick="setFileSimScale('count')" title="Scale flow by function count">Count</button>
                                <button class="view-btn ${fileSimScale === 'features' ? 'active' : ''}" id="bsim-filesim-scale-btn-features" onclick="setFileSimScale('features')" title="Scale flow by BSim feature sum">Features</button>
                            </div>
                            <!-- Options are filled per pair: an axis the pair has
                                 no tags on is not offered. -->
                            <div class="view-toggle" id="bin-sim-filesim-axis-toggle" style="margin:0; align-items:center; display:none; gap:4px;">
                                <span class="bsim-ctl-label">Cross with:</span>
                                <select class="view-btn" id="bsim-filesim-axis-b" onchange="setFileSimAxis(null, this.value)"
                                        title="Cross the tree's axis with a second one, e.g. Severity × Behavior shows shared high-severity network code"></select>
                            </div>
                            <span style="font-size:0.68rem; color:var(--dim); font-family:sans-serif;">follows the tree's folding · click a node to drill in</span>
                        </div>
                        <div id="bin-sim-filesim-sankey-card" style="position:relative; width:100%; flex:0 0 auto; height:440px; border:1px solid var(--border); background:var(--bg); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                            <div id="bin-sim-filesim-sankey" style="flex:1; width:100%; min-height:0; overflow:auto; position:relative;"></div>
                        </div>
                    </div>

                    <!-- The one function table. All / Matched / Unmatched differ only
                         by the state filter they send, and each of them reads either
                         as rows or as the same rows drawn as flow. -->
                    <div class="bsim-subtab-panel" id="bsim-panel-table" style="flex:1; min-height:0; display:none; flex-direction:column;">
                        <div style="display:flex; align-items:center; gap:10px; padding:0 0 8px 0; flex-shrink:0; flex-wrap:wrap;">
                            <div class="view-toggle" style="margin:0; display:flex; align-items:center;">
                                <span class="bsim-ctl-label">View:</span>
                                <button class="view-btn active" id="bsim-view-btn-table" onclick="setFileSimView('table')" title="Function rows">Table</button>
                                <button class="view-btn" id="bsim-view-btn-graph" onclick="setFileSimView('graph')" title="The same rows drawn as flow, function to function">Graph</button>
                            </div>
                            <div id="bsim-table-controls" style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                                <div class="view-toggle" style="margin:0; display:flex; align-items:center;">
                                    <span class="bsim-ctl-label">Group by:</span>
                                    <button class="view-btn active" id="bsim-group-btn-auto" onclick="setFileSimGroupBy('auto')" title="Group by tag when the selection spans more than one">Auto</button>
                                    <button class="view-btn" id="bsim-group-btn-tag" onclick="setFileSimGroupBy('tag')" title="Always group by tag">Tag</button>
                                    <button class="view-btn" id="bsim-group-btn-none" onclick="setFileSimGroupBy('none')" title="One flat list">None</button>
                                </div>
                                <button class="view-btn" onclick="expandAllFileSimNodes()" title="Expand every tag group (also expands the tree)">Expand all</button>
                                <button class="view-btn" onclick="collapseAllFileSimNodes()" title="Collapse every tag group (also collapses the tree)">Collapse all</button>
                            </div>
                            <span id="bsim-table-count" style="font-size:0.72rem; color:var(--dim); font-family:sans-serif;"></span>
                        </div>
                        <div class="resizable-card" id="bsim-table-card" style="border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; flex:1; min-height:200px; overflow:hidden;">
                            <div class="bin-sim-table-scroll" style="flex:1; overflow:auto;">
                                <table id="bin-sim-table-matched-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                                    <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;"></thead>
                                    <tbody id="bin-sim-table-matched"></tbody>
                                </table>
                            </div>
                        </div>
                        <div class="resizable-card" id="bsim-fngraph-card" style="border:1px solid var(--border); border-radius:8px; background:var(--bg); display:none; flex-direction:column; flex:1; min-height:200px; overflow:hidden;">
                            <div id="bsim-fngraph" style="flex:1; width:100%; min-height:0; overflow:auto; position:relative;"></div>
                        </div>
                    </div>

            <!-- Metadata tab -->
            <div class="bsim-subtab-panel" id="bsim-panel-metadata" style="flex:1; min-height:0; display:none; flex-direction:column; overflow:auto; padding:5px 0 0 0; gap:10px;">
                <div style="display:flex; align-items:center; justify-content:flex-end; gap:8px; flex-shrink:0;">
                    <span style="font-size:0.7rem; color:var(--subtle); margin-right:6px; font-weight:bold; font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;">Highlight:</span>
                    <div class="view-toggle" style="margin:0; display:flex;">
                        <button class="view-btn active" id="meta-highlight-different" onclick="setMetaHighlightMode('different')" title="Highlight different metadata fields">Differences</button>
                        <button class="view-btn" id="meta-highlight-similar" onclick="setMetaHighlightMode('similar')" title="Highlight identical metadata fields">Similarities</button>
                        <button class="view-btn" id="meta-highlight-none" onclick="setMetaHighlightMode('none')" title="Do not highlight">None</button>
                    </div>
                </div>
                <div id="bin-sim-meta-compare" style="color:var(--dim); text-align:center; padding:40px;">Loading metadata…</div>
            </div>

            <!-- Clusters tab -->
            <div class="bsim-subtab-panel" id="bsim-panel-inferred" style="flex:1; min-height:0; display:none; flex-direction:column; overflow:auto; padding:5px 0 0 0; gap:10px;">
                <div id="bin-sim-inferred-meta-container" style="color:var(--dim); text-align:center; padding:40px;">Loading clusters…</div>
            </div>

                </div><!-- /#bsim-detail -->
            </div><!-- /#bsim-main -->
        </div>
        <style>
            #bsim-main { display:flex; flex:1; min-height:0; align-items:stretch; gap:16px; }
            #bsim-sidebar {
                width:280px; flex-shrink:0; display:flex; flex-direction:column;
                border:1px solid var(--border); border-radius:8px; background:var(--card-bg);
                overflow:auto; padding:10px 0;
            }
            #bsim-detail { flex:1; min-width:0; display:flex; flex-direction:column; min-height:0; }
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
            .bsim-sum-row td { padding:6px 10px; border-bottom:1px solid var(--border); }
            .bsim-sum-row:hover td { background:var(--hover); }
            .bsim-caret-btn { cursor:pointer; user-select:none; color:var(--subtle); display:inline-block; width:14px; }
            .bsim-tree { flex:0 0 auto; }
            /* Non-tree navigation: same column, deliberately not tree-shaped, so a
               page switch never reads as a scope change. */
            .bsim-side-sep { border-top:1px solid var(--border); margin:10px 12px; }
            .bsim-nav-item {
                padding:7px 14px; cursor:pointer; font-size:0.82rem; color:var(--subtle);
                font-family:'Inter',sans-serif; border-left:3px solid transparent;
            }
            .bsim-nav-item:hover { background:var(--hover); color:var(--text); }
            .bsim-nav-item.active { color:var(--accent); border-left-color:var(--accent); background:var(--hover); }
            /* The tree greys out while a non-scoping page is open, rather than
               collapsing, so returning to a scope stays one click. */
            #bsim-sidebar.nav-active .bsim-tree { opacity:0.45; }
            .bsim-node {
                display:flex; align-items:center; gap:6px; padding:4px 12px; cursor:pointer;
                font-size:0.8rem; font-family:'Inter',sans-serif; color:var(--text);
                border-left:3px solid transparent; white-space:nowrap;
            }
            .bsim-node:hover { background:var(--hover); }
            .bsim-node.selected { background:var(--hover); border-left-color:var(--accent); }
            .bsim-node .bsim-caret { width:12px; color:var(--subtle); flex-shrink:0; user-select:none; }
            /* A whole row in the tag's colour would drown the tree, so the list
               carries the colour as a dot and the graph carries it as area. */
            .bsim-node-dot {
                display:inline-block; width:8px; height:8px; border-radius:50%; flex-shrink:0;
                vertical-align:middle;
            }
            .bsim-node.bsim-drift .bsim-node-dot { display:none; }
            .bsim-node .bsim-node-label { flex:1; overflow:hidden; text-overflow:ellipsis; }
            .bsim-node .bsim-node-count { font-size:0.68rem; color:var(--dim); font-family:'Consolas',monospace; }
            .bsim-node .bsim-node-pct { font-size:0.72rem; color:var(--accent); font-family:'Consolas',monospace; width:40px; text-align:right; }
            .bsim-node.bsim-drift { color:var(--token-instruction); font-size:0.74rem; cursor:default; }
            .bsim-node.bsim-drift:hover { background:none; }
            .bsim-chips { display:flex; flex-wrap:wrap; gap:6px; margin:0 0 10px 0; min-height:0; }
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
            .bsim-fold-pill {
                display:inline-block; padding:2px 8px; border-radius:12px; background:var(--bg-alt);
                border:1px solid var(--border); font-size:0.72rem; color:var(--subtle);
                cursor:pointer; user-select:none; white-space:nowrap; margin-left:8px;
                font-family:'Inter',sans-serif;
            }
            .bsim-fold-pill:hover { border-color:var(--accent); color:var(--text); }
            .bsim-tabbar { display:flex; gap:4px; margin:0 0 16px 0; border-bottom:2px solid var(--border); }
            .bsim-tab {
                background:none; border:none; border-bottom:3px solid transparent;
                margin-bottom:-2px; padding:10px 20px; cursor:pointer;
                color:var(--subtle); font-size:0.9rem; font-weight:600; letter-spacing:0.01em;
                transition:color 0.15s, border-color 0.15s, background 0.15s;
            }
            .bsim-tab:hover { color:var(--text); background: var(--hover); }
            .bsim-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
            .bin-sim-strip { border:1px solid var(--border); border-radius:6px; padding:10px 12px; background:var(--card-bg); display:flex; align-items:center; gap:10px; min-height:24px; }
            .bin-sim-mc-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
            .bin-sim-mc-table th { text-align:left; padding:6px 12px; color:var(--subtle); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; border-bottom:1px solid var(--border); }
            .bin-sim-mc-table td { padding:6px 12px; border-bottom: 1px solid var(--border); vertical-align:top; font-family:'Consolas',monospace; word-break:break-word; }
            .bin-sim-mc-cat { padding:10px 12px 4px; font-weight:bold; color:var(--accent); font-size:0.78rem; }
            .bin-sim-mc-label { color:var(--subtle); font-family:'Inter',sans-serif; width:160px; }
            .bin-sim-mc-diff td { background:color-mix(in srgb, var(--token-instruction) 10%, transparent); }
            .bin-sim-mc-same td { background:color-mix(in srgb, var(--token-symbol) 10%, transparent); }
            .drag-handle-v:hover {
                background: var(--hover) !important;
            }
            .drag-handle-v:hover div {
                background: var(--accent) !important;
            }
            #bin-sim-table-matched-table td {
                position: relative;
                user-select: text !important;
            }
        </style>
    `;
    
    container.innerHTML = html;
    initResizableCards();
    
    if (md5a && md5b) {
        // pool_id is encoded as a query param in the diff URL
        const urlParams = new URLSearchParams(window.location.search);
        let poolId = params.get('pool_id') || urlParams.get('pool_id') || null;
        if (!poolId && window.getRoutingState) {
            poolId = window.getRoutingState().pool || null;
        }
        fetchAndRenderBinaryDiff(collection, md5a, md5b, collB, poolId);
    }
}

function initResizableCards() {
    const cards = document.querySelectorAll('.resizable-card');
    cards.forEach(card => {
        const handle = card.querySelector('.drag-handle-v');
        if (!handle) return;
        
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            const startHeight = card.offsetHeight;
            const startY = e.clientY;
            
            // Add a temporary overlay to prevent pointer events on layout children (like iframe/hover previews) during resize
            const overlay = document.createElement('div');
            overlay.style.position = 'fixed';
            overlay.style.top = '0';
            overlay.style.left = '0';
            overlay.style.width = '100vw';
            overlay.style.height = '100vh';
            overlay.style.cursor = 'ns-resize';
            overlay.style.zIndex = '99999';
            document.body.appendChild(overlay);
            
            const onMouseMove = (moveEvent) => {
                const deltaY = moveEvent.clientY - startY;
                card.style.height = `${Math.max(200, Math.min(1000, startHeight + deltaY))}px`;
                
                if (card.id === 'bsim-fngraph-card' && binSimDataCache) {
                    renderFileSimGraph();
                }
            };
            
            const onMouseUp = () => {
                document.body.removeChild(overlay);
                document.removeEventListener('mousemove', onMouseMove);
                document.removeEventListener('mouseup', onMouseUp);
            };
            
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
            });
            });
            }

            async function fetchAndRenderBinaryDiff(collection, md5a, md5b, collB, poolId) {

    const resultsEl = document.getElementById('bin-sim-results');

    // Navigating to a different pair abandons any resplit poll for the old one.
    if (binSimResplitPoll && !(binSimCtx && binSimCtx.md5a === md5a && binSimCtx.md5b === md5b && binSimCtx.collection === collection)) {
        clearInterval(binSimResplitPoll);
        binSimResplitPoll = null;
    }

    try {
        // Compact summary: scores, counts, file meta, and the Sankey projection — no rows.
        let url = `/api/diff?view=sankey&collection_a=${encodeURIComponent(collection)}&md5_a=${encodeURIComponent(md5a)}&md5_b=${encodeURIComponent(md5b)}`;
        if (collB) url += `&collection_b=${encodeURIComponent(collB)}`;
        if (poolId) url += `&pool=${encodeURIComponent(poolId)}`;
        const res = await fetch(url);
        if (!res.ok) {
            let errMsg = "Failed to fetch similarity comparison";
            try {
                const errData = await res.json();
                if (errData && errData.message) errMsg = errData.message;
            } catch (e) {}
            throw new Error(errMsg);
        }
        const data = await res.json();

        if (data.is_container_pair) {
            // Neither side has functions of its own, so none of the machinery
            // below (tag tree, sankey, function tables) has anything to draw.
            await renderContainerPairView(data, collection, md5a, md5b, collB, poolId);
            return;
        }

        window.filenameCache = window.filenameCache || {};
        if (data.file_metadata_a) window.filenameCache[md5a] = data.file_metadata_a.file_name || 'File';
        if (data.file_metadata_b) window.filenameCache[md5b] = data.file_metadata_b.file_name || 'File';

        const nameA = data.file_metadata_a?.file_name || 'Binary A';
        const nameB = data.file_metadata_b?.file_name || 'Binary B';


        Breadcrumbs.setFilename(md5a, data.file_metadata_a?.file_name || 'File');
        Breadcrumbs.setFilename(md5b, data.file_metadata_b?.file_name || 'File');
        Breadcrumbs.refresh();
        
        // Render Summary — prominent, score-colored
        const heroEl = document.getElementById('bin-sim-hero');
        if (heroEl) {
            const types = window.BinSimScoreTypes || {};
            const mainMeta = types.score;
            const mainPct = ((data.score || 0) * 100).toFixed(1) + '%';
            
            const others = Object.keys(types).filter(k => k !== 'score');
                
            const small = others.map(k => {
                const meta = types[k];
                const val = ((data[k] || 0) * 100).toFixed(0) + '%';
                return `<div style="display:flex; align-items:center; gap:6px; font-size:0.9rem; color:${meta.color}; font-weight:600;">
                    <i class="${meta.icon}"></i> <span>${meta.label}</span> <span>${val}</span>
                </div>`;
            }).join('');

            heroEl.innerHTML = `
                <div style="display:flex; align-items:center; gap:20px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <i class="${mainMeta.icon}" style="color:${mainMeta.color}; font-size:1.4rem;" title="${escapeAttr(mainMeta.label)}"></i>
                        <span style="color:var(--subtle); text-transform:uppercase; font-size:0.9rem; font-weight:bold; letter-spacing:0.05em;">${mainMeta.label}</span>
                        <span style="font-family:'Consolas', monospace; font-weight:800; font-size:2.4rem; line-height:1; color:${mainMeta.color};">${mainPct}</span>
                    </div>
                    <div style="display:flex; gap:16px; margin-left:8px; border-left:1px solid var(--border); padding-left:24px;">${small}</div>
                </div>
            `;
        }

        resultsEl.style.display = 'flex';

        // Slim per-binary strip: user tags + notes only
        renderBinSimStrip('bin-sim-strip-a', data.file_metadata_a, `${collection}:file:${md5a}`);
        renderBinSimStrip('bin-sim-strip-b', data.file_metadata_b, `${collB || collection}:file:${md5b}`);

        // Stash context for lazy Metadata tab load
        binSimMetaCtx = {
            collection, md5a, md5b, collB: collB || collection, poolId, loaded: false,
        };

        // Cache: the compact summary only; tables and the function graph load their
        // rows via paging, merging functions_metadata across pages.
        binSimCtx = { collection, md5a, md5b, collB: collB || collection, poolId };
        const counts = data.counts || { matched: 0, unique_to_a: 0, unique_to_b: 0 };
        binSimDataCache = {
            score: data.score,
            file_metadata_a: data.file_metadata_a,
            file_metadata_b: data.file_metadata_b,
            tags_summary: data.tags_summary || [],
            severity_summary: data.severity_summary || [],
            category_summary: data.category_summary || [],
            user_summary: data.user_summary || [],
            capa_summary: data.capa_summary || [],
            mitre_summary: data.mitre_summary || [],
            yara_summary: data.yara_summary || [],
            family_summary: data.family_summary || [],
            vuln_summary: data.vuln_summary || [],
            ruleset_summary: data.ruleset_summary || [],
            joint: data.joint || {},
            tags_stale: !!data.tags_stale,
            counts,
            functions_metadata: {},
        };
        // A pair that never went through the LLM has no severity or behaviour
        // axis, and one nobody has tagged has no user axis: snap both pickers to
        // axes this pair actually carries before anything reads them.
        fileSimAxisA = fileSimAxisKey();
        if (!fileSimAvailableAxes().includes(fileSimAxisB) || fileSimAxisB === fileSimAxisA) {
            fileSimAxisB = '';
        }
        // A fresh pair starts unscoped, at the root of the tree.
        fileSimSelection = new Set();
        fileSimTreeOpen = fileSimDefaultOpen();
        fileSimOpenFolds = new Set();
        fileSimRows = {};
        fileSimFoldRows = {};

        const btnAll = document.getElementById('bin-sim-tab-btn-all');
        const btnMatched = document.getElementById('bin-sim-tab-btn-matched');
        const btnUniqueA = document.getElementById('bin-sim-tab-btn-unique_a');
        const btnUniqueB = document.getElementById('bin-sim-tab-btn-unique_b');
        const btnUnmatched = document.getElementById('bin-sim-tab-btn-unmatched');
        // A matched row is one function on each side, so All counts both sides.
        const allCount = (counts.matched || 0) * 2 + (counts.unique_to_a || 0) + (counts.unique_to_b || 0);
        if (btnAll) btnAll.textContent = `All (${allCount})`;
        if (btnMatched) btnMatched.textContent = `Matched (${counts.matched})`;
        if (btnUniqueA) btnUniqueA.textContent = `Unique to A (${counts.unique_to_a})`;
        if (btnUniqueB) btnUniqueB.textContent = `Unique to B (${counts.unique_to_b})`;
        if (btnUnmatched) btnUnmatched.textContent = `Unmatched (${counts.unique_to_a} / ${counts.unique_to_b})`;

        // Tree + Summary render from the compact payload alone; the table and the
        // function graph page themselves once a tab that needs rows is shown.
        // Restore the tab from the URL hash (e.g. after a Back navigation).
        applyBinSimTabFromHash();

    } catch(err) {
        console.error(err);
        if (resultsEl) {
            resultsEl.style.display = 'flex';
            resultsEl.innerHTML = `
                <div style="text-align:center; padding:50px; color:var(--dim); font-size:1.1rem; flex:1; display:flex; flex-direction:column; justify-content:center; align-items:center; gap:15px; min-height:300px;">
                    <i class="fa-solid fa-triangle-exclamation" style="color:#f92672; font-size:3rem;"></i>
                    <div style="font-weight:bold; color:var(--text);">${err.message}</div>
                    <div style="font-size:0.85rem; opacity:0.7; max-width:400px; line-height:1.4;">This comparison has not been pre-calculated. You may need to trigger a collection rebuild or run the binary similarity analysis.</div>
                </div>
            `;
        }
    }
}


// ---- File sim: the main view --------------------------------------------
// A tag tree on the left scopes the detail pane on the right. The tree answers
// "what mass do these two binaries carry"; the table answers "which functions".
// Keeping those in separate panes is what stopped the single recursive table
// from having to be both at once.
//
// One expansion state (`fileSimTreeOpen`) drives all four surfaces: the tree,
// the Summary rollup, the table's groups, and the Sankey's frontier. Folding
// libc anywhere folds it everywhere, so the panes can never disagree about what
// is being looked at.
//
// Composition similarity, tag by tag, is independent of how well individual
// functions matched: a leaf tag scores min(count_a, count_b) / max(...) --
// "A has 2 libc funcs, B has 4" -> 50%. A group is the mean of its children, so
// a group with one perfect and one absent library reads 50%, not "mostly fine".
// ponytail: counts, not feature weights. Switch to weight_* if count proves noisy.


// Selected tag node ids. Empty = the root "All" node = the whole pair.
let fileSimSelection = new Set();
// The one expansion state, shared by tree / summary / table / sankey.
let fileSimTreeOpen = new Set(['root']);
// 'summary' | 'all' | 'matched' | 'unique_a' | 'unique_b' -- the right pane's tabs.
let fileSimTab = 'summary';
// How All / Matched / Unmatched draw the same rows: as a table or as flow.
let fileSimView = 'table';     // 'table' | 'graph'
let fileSimGroupBy = 'auto';   // 'auto' | 'tag' | 'none'
// Expanded duplicate folds, by key.
let fileSimOpenFolds = new Set();

// Functions carrying this tag on each side = matched (from the bins) + unique.
function tagSideCounts(row) {
    let a = row.unique_count_a || 0, b = row.unique_count_b || 0;
    Object.keys(row.bins || {}).forEach(k => {
        a += row.bins[k][0] || 0;
        b += row.bins[k][2] || 0;
    });
    return [a, b];
}

function fileSimSim(a, b) {
    return Math.max(a, b) > 0 ? Math.min(a, b) / Math.max(a, b) : 0;
}


// `category:network` -> `network`, `category:network:c2` -> `c2`. The parent is
// already on screen above the leaf, so repeating it in the leaf reads as noise.
function fileSimLeafLabel(tagId) {
    const segs = TagColor.levels(tagId).segs;
    return segs[segs.length - 1] || String(tagId);
}

// Severity is ordinal, so its tree reads worst-first rather than by mass.
const FILESIM_SEVERITY_ORDER = ['high', 'medium', 'low', 'none'];


// Family and vuln ids are a path, not a group refined by a leaf:
// `ms-caro-malware-full:malware-platform:linux` is taxonomy, predicate, value,
// and every one of those is a level someone wants to fold at
// -- reading it as one node labelled `linux` throws away that it is a platform
// rather than a malware type. So their tree is a trie over the id's segments,
// which is also why the backend leaves these axes unrolled (_PARENT_DEPTH).
// Depth is whatever the ids have, so `cve:` gets two levels and ms-caro three
// with no per-namespace table anywhere.
// The levels a tag id occupies, deepest last, every one of them a real tag id:
// `['origin', 'origin:lib', 'origin:lib:libc', 'origin:lib:libc:2.31']`. Split
// by `TagColor`, so the tree nests exactly where the search index buckets and
// where the colour rule changes hue. A detail tail contributes no level, which
// is what keeps a function name out of the sankey's columns.
function fileSimTagChain(tagId) {
    const chain = TagColor.prefixes(tagId);
    chain.push(TagColor.groupId(tagId));
    return chain;
}

function fileSimNestedNodes(rows) {
    const root = { children: new Map() };

    const add = (tagId, a, b, drift) => {
        // A tag with no functions on either side is not evidence of
        // dissimilarity, so it must not drag its parent's mean down.
        if (a === 0 && b === 0) return;
        let node = root;
        fileSimTagChain(tagId).forEach(prefix => {
            let next = node.children.get(prefix);
            if (!next) {
                next = {
                    id: prefix, label: fileSimLeafLabel(prefix), prefix,
                    a: 0, b: 0, children: new Map(), drift: {}, tagIds: [],
                };
                node.children.set(prefix, next);
            }
            // A branch carries the sum of the ids beneath it. A function tagged
            // both `...:malware-type:trojan` and `...:malware-platform:linux`
            // therefore counts once under each, and twice under the taxonomy
            // above them -- the same double-count the category axis already has
            // wherever one function carries two tags of one group.
            next.a += a; next.b += b;
            next.tagIds.push(tagId);
            Object.entries(drift || {}).forEach(([partner, w]) => {
                next.drift[partner] = (next.drift[partner] || 0) + w;
            });
            node = next;
        });
    };

    (rows || []).forEach(row => {
        // A parent row is the merge of its children (`bin_sim_tags.summary`),
        // so feeding both would count the same mass twice. Take the leaves and
        // let the trie rebuild every level above them.
        const kids = row.children || [];
        if (kids.length) {
            kids.forEach(child => {
                const [ca, cb] = tagSideCounts(child);
                add(child.tag_id, ca, cb, child.drift);
            });
            return;
        }
        const [a, b] = tagSideCounts(row);
        add(row.tag_id, a, b, row.drift);
    });

    return fileSimNestedFinish(root).children;
}

// Map of children -> sorted array, and a similarity for every node. Depth is
// unbounded, so this recurses rather than unrolling levels the way origin does.
function fileSimNestedFinish(node) {
    const kids = [...node.children.values()]
        .map(fileSimNestedFinish)
        .sort((x, y) => y.sim - x.sim);
    node.children = kids;
    // A leaf scores on its own counts; a branch is the mean of what is under it,
    // so one absent family still shows instead of being averaged away by the
    // mass of its siblings. Same rule the origin groups use.
    node.sim = kids.length
        ? kids.reduce((s, c) => s + c.sim, 0) / kids.length
        : fileSimSim(node.a, node.b);
    return node;
}


// One tree per axis, one builder for all of them: a node id is always a real
// tag id, so its colour, its index buckets and its backend scope are the same
// string. Origin used to have its own builder that minted ids like
// `libraries/Visual Studio` -- which no colour rule and no index had ever seen,
// so a library drew one colour in the tree and another on its own card.
function fileSimTree(rows, axis) {
    let live = fileSimNestedNodes(rows);
    // The tab already names the namespace, so a lone top node just repeats it:
    // an Origin tree that opens on "origin" before it says "libc" wastes the
    // level. One level only, so a pair carrying nothing but libraries still
    // opens on `lib` rather than being drilled down to a bare version number.
    if (live.length === 1 && (live[0].children || []).length) live = live[0].children;
    // Severity is ordinal: worst first reads better than biggest first.
    if (axis === 'severity') {
        const rank = (n) => {
            const i = FILESIM_SEVERITY_ORDER.indexOf(n.label);
            return i === -1 ? FILESIM_SEVERITY_ORDER.length : i;
        };
        live = live.slice().sort((x, y) => rank(x) - rank(y));
    }
    return {
        id: 'root', label: 'All', prefix: null, children: live, drift: {},
        a: live.reduce((s, g) => s + g.a, 0),
        b: live.reduce((s, g) => s + g.b, 0),
        sim: live.length ? live.reduce((s, g) => s + g.sim, 0) / live.length : 0,
    };
}

// Axes this pair actually carries mass on. Behaviour and severity are empty
// until the LLM has tagged something and user is empty until a human has, so
// offering them would be the picker promising a view that can only say "no
// data". Origin is not special-cased: it is empty for a pair with no functions.
function fileSimAvailableAxes() {
    const data = binSimDataCache || {};
    return Object.keys(FILESIM_AXES).filter(k => (data[FILESIM_AXES[k].field] || []).length);
}

// The axis the tree, the tables and the graph read. Falls back to whatever the
// pair has, so a stale selection from the previous pair cannot blank the view.
function fileSimAxisKey() {
    const have = fileSimAvailableAxes();
    return have.includes(fileSimAxisA) ? fileSimAxisA : (have[0] || 'origin');
}

// Origin needs its group level opened to say anything -- a closed tree that
// reads "lib" tells you nothing, "libc 40%" does. The ids are the tree's own,
// so this asks the tree rather than naming nodes it hopes exist.
function fileSimDefaultOpen() {
    const open = new Set(['root']);
    if (fileSimAxisKey() !== 'origin') return open;
    fileSimTreeRoot().children.forEach(n => open.add(n.id));
    return open;
}

// Rebuilt per render: cheap (tens of rows) and always consistent with the cache.
function fileSimTreeRoot() {
    const axis = fileSimAxisKey();
    const rows = (binSimDataCache && binSimDataCache[FILESIM_AXES[axis].field]) || [];
    return fileSimTree(rows, axis);
}

function fileSimNodes(node, out = []) {
    out.push(node);
    (node.children || []).forEach(c => fileSimNodes(c, out));
    return out;
}

function fileSimNodeById(id) {
    return fileSimNodes(fileSimTreeRoot()).find(n => n.id === id);
}

function fileSimNodePrefixes(node) {
    if (!node) return [];
    if (node.prefix) return [node.prefix];
    return node.prefixes || [];
}

// tags_summary rows inside the current scope. Same prefix rule the backend
// applies to functions, so every pane agrees on what is selected.
function fileSimScopeRows(rows) {
    const prefixes = fileSimScopePrefixes();
    if (!prefixes.length) return rows;
    // By levels, not by text: `startsWith(p + ':')` reads
    // `fid:uclibc:0.9.30.1#xdrmem_getint32` as outside `fid:uclibc:0.9.30.1`,
    // because the next character is the detail marker rather than a colon.
    return rows.filter(r => prefixes.some(p => fileSimTagChain(r.tag_id).includes(p)));
}

// The tag prefixes the current selection sends to the backend. A group node has
// several (Libraries is `lib` + `stdlib`); the root has none, meaning no filter.
function fileSimScopePrefixes() {
    if (!fileSimSelection.size) return [];
    const nodes = fileSimNodes(fileSimTreeRoot());
    const out = [];
    fileSimSelection.forEach(id => {
        const node = nodes.find(n => n.id === id);
        if (node) out.push(...fileSimNodePrefixes(node));
    });
    return out;
}

// The nodes the detail panes render at the top level: the selection's children
// when one node is selected, the selected nodes themselves otherwise.
function fileSimScopeNodes() {
    const root = fileSimTreeRoot();
    if (!fileSimSelection.size) return root.children;
    const nodes = fileSimNodes(root);
    const sel = [...fileSimSelection].map(id => nodes.find(n => n.id === id)).filter(Boolean);
    if (!sel.length) return root.children;
    if (sel.length === 1) return sel[0].children.length ? sel[0].children : sel;
    return sel;
}

window.expandAllFileSimNodes = function() {
    // Every node, including leaves: in the tree a leaf has nothing to unfold,
    // but in the table a leaf's open state is what loads its function rows.
    fileSimNodes(fileSimTreeRoot()).forEach(n => fileSimTreeOpen.add(n.id));
    onFileSimFoldChange();
};

window.collapseAllFileSimNodes = function() {
    fileSimTreeOpen = new Set(['root']);
    onFileSimFoldChange();
};

// Folding is shared, so a fold anywhere redraws everything that reads it.
function onFileSimFoldChange() {
    renderFileSimTree();
    if (fileSimTab === 'summary') renderFileSimSummary();
    else renderFileSimRows();
}

// ---- Tree rendering ------------------------------------------------------

// The tag's colour dot. The tree, the Summary rollup and the table's group rows
// all name the same node, so they all paint it from the same id.
function fileSimDotHtml(nodeId, style = '') {
    return `<span class="bsim-node-dot" style="background:${TagColor.forTag(nodeId)};${style}"></span>`;
}

function fileSimNodeHtml(node, depth, out) {
    const hasKids = (node.children || []).length > 0;
    const open = fileSimTreeOpen.has(node.id);
    const selected = fileSimSelection.has(node.id) || (node.id === 'root' && !fileSimSelection.size);
    const pct = (node.sim * 100).toFixed(node.sim === 1 || node.sim === 0 ? 0 : 1) + '%';
    const caret = hasKids
        ? `<span class="bsim-caret" onclick="event.stopPropagation(); toggleFileSimNode(${escapeAttr(jsString(node.id))})">${open ? '▼' : '▶'}</span>`
        : '<span class="bsim-caret"></span>';

    out.push(`
        <div class="bsim-node${selected ? ' selected' : ''}" style="padding-left:${8 + depth * 14}px;"
             title="A: ${Math.round(node.a)} · B: ${Math.round(node.b)}"
             onclick="selectFileSimNode(${escapeAttr(jsString(node.id))}, event)">
            ${caret}
            ${fileSimDotHtml(node.id)}
            <span class="bsim-node-label">${escapeHtml(node.label)}</span>
            <span class="bsim-node-count">${Math.round(node.a)}/${Math.round(node.b)}</span>
            <span class="bsim-node-pct">${pct}</span>
        </div>`);

    // Drift sits under the library it drifted from, where the version comparison
    // is legible, rather than in one anonymous global mismatch bucket.
    const drift = Object.entries(node.drift || {}).sort((x, y) => y[1] - x[1]);
    if (open && drift.length) {
        drift.forEach(([partner, w]) => {
            out.push(`
                <div class="bsim-node bsim-drift" style="padding-left:${8 + (depth + 1) * 14}px;"
                     title="${Math.round(w)} weight matched against ${escapeHtml(partner)} instead">
                    <span class="bsim-caret"></span>
                    <span class="bsim-node-label">⚠ drift → ${escapeHtml(fileSimDriftLabel(partner))}</span>
                </div>`);
        });
    }
    if (open) (node.children || []).forEach(c => fileSimNodeHtml(c, depth + 1, out));
}

function fileSimDriftLabel(tagId) {
    const parts = String(tagId).split(':');
    if (parts.length >= 3) return parts[1] + ' ' + parts[2];
    if (parts.length === 2) return parts[1];
    return tagId;
}

// Both axis pickers, offering only the axes this pair carries tags on. With one
// axis there is nothing to pick, so neither control is drawn at all.
function renderFileSimAxisPicker() {
    const avail = fileSimAvailableAxes();
    const axisA = fileSimAxisKey();
    const host = document.getElementById('bsim-axis-pick');
    if (host) {
        host.innerHTML = avail.length < 2 ? '' : `
            <span class="bsim-ctl-label">Axis:</span>
            <select class="view-btn" id="bsim-filesim-axis-a" onchange="setFileSimAxis(this.value, null)"
                    title="What the tree, the tables and the flow are grouped by">
                ${avail.map(k => `<option value="${k}"${axisA === k ? ' selected' : ''}>${FILESIM_AXES[k].label}</option>`).join('')}
            </select>`;
    }
    const selB = document.getElementById('bsim-filesim-axis-b');
    const wrap = document.getElementById('bin-sim-filesim-axis-toggle');
    const cross = avail.filter(k => k !== axisA);
    if (selB) {
        selB.innerHTML = `<option value="">(none)</option>`
            + cross.map(k => `<option value="${k}"${fileSimAxisB === k ? ' selected' : ''}>${FILESIM_AXES[k].label}</option>`).join('');
    }
    if (wrap) wrap.style.display = cross.length ? 'flex' : 'none';
}

function renderFileSimTree() {
    const el = document.getElementById('bsim-tree');
    if (!el) return;
    const root = fileSimTreeRoot();
    if (!root.children.length) {
        el.innerHTML = '<div style="color:var(--dim); padding:10px 12px; font-size:0.78rem;">No tag data for this pair.</div>';
        return;
    }
    const out = [];
    fileSimNodeHtml(root, 0, out);
    el.innerHTML = out.join('');
}

window.toggleFileSimNode = function(id) {
    if (fileSimTreeOpen.has(id)) fileSimTreeOpen.delete(id);
    else fileSimTreeOpen.add(id);
    onFileSimFoldChange();
};

// Click selects, ctrl/shift-click adds. Selecting the root clears the scope,
// which is the same thing as selecting everything.
window.selectFileSimNode = function(id, event) {
    const additive = event && (event.ctrlKey || event.metaKey || event.shiftKey);
    if (id === 'root') {
        fileSimSelection.clear();
    } else if (additive) {
        if (fileSimSelection.has(id)) fileSimSelection.delete(id);
        else fileSimSelection.add(id);
    } else {
        fileSimSelection = new Set([id]);
    }
    if (id !== 'root') fileSimTreeOpen.add(id);
    onFileSimScopeChange();
};

function onFileSimScopeChange() {
    fileSimOpenFolds.clear();
    fileSimRows = {};
    fileSimFoldRows = {};
    renderFileSimTree();
    renderFileSimChips();
    renderFileSim(binSimDataCache);
}

// ---- Scope chips ---------------------------------------------------------

function renderFileSimChips() {
    const el = document.getElementById('bsim-chips');
    if (!el) return;
    const nodes = fileSimNodes(fileSimTreeRoot());
    const chips = [];

    [...fileSimSelection].forEach(id => {
        const node = nodes.find(n => n.id === id);
        if (!node) return;
        chips.push(`
            <span class="bsim-chip">tag: <b>${escapeHtml(node.label)}</b>
                <span class="bsim-chip-x" title="Remove this scope"
                      onclick="removeFileSimScope(${escapeAttr(jsString(id))})">✕</span>
            </span>`);
    });

    const stateLabel = { matched: 'matched', unique_a: 'unique to A', unique_b: 'unique to B' }[fileSimTab];
    if (stateLabel) {
        chips.push(`
            <span class="bsim-chip">state: <b>${stateLabel}</b>
                <span class="bsim-chip-x" title="Show all states"
                      onclick="switchBinSimTab('all')">✕</span>
            </span>`);
    }

    el.innerHTML = chips.length
        ? chips.join('')
        : '<span style="font-size:0.72rem; color:var(--dim); font-family:sans-serif;">Whole pair — select a tag on the left to scope.</span>';
}

window.removeFileSimScope = function(id) {
    fileSimSelection.delete(id);
    onFileSimScopeChange();
};

// ---- Summary tab ---------------------------------------------------------

// Rollup rows recurse to library-version depth, folding with the same state the
// tree uses, so the two always show the same shape.
function fileSimSummaryRows(nodes, depth, out) {
    nodes.forEach(node => {
        const hasKids = (node.children || []).length > 0;
        const open = fileSimTreeOpen.has(node.id);
        const drift = Object.entries(node.drift || {}).sort((x, y) => y[1] - x[1]);
        const caret = hasKids
            ? `<span class="bsim-caret-btn">${open ? '▼' : '▶'}</span>`
            : '<span class="bsim-caret-btn"></span>';
        const rowClick = hasKids 
            ? `toggleFileSimNode(${escapeAttr(jsString(node.id))})`
            : `selectFileSimNode(${escapeAttr(jsString(node.id))}, event)`;
        out.push(`
            <tr class="bsim-sum-row" style="cursor:pointer;"
                onclick="${rowClick}">
                <td style="padding-left:${10 + depth * 18}px;">${caret}${fileSimDotHtml(node.id, 'margin-right:6px;')}${escapeHtml(node.label)}</td>
                <td style="text-align:right;">${Math.round(node.a)}</td>
                <td style="text-align:right;">${Math.round(node.b)}</td>
                <td style="text-align:right; color:var(--accent);">${(node.sim * 100).toFixed(0)}%</td>
                <td style="width:140px;">
                    <div style="background:var(--border); border-radius:3px; height:6px;">
                        <div style="width:${Math.round(node.sim * 100)}%; background:var(--accent); height:6px; border-radius:3px;"></div>
                    </div>
                </td>
                <td style="color:var(--token-instruction); font-size:0.75rem;">
                    ${drift.length ? '⚠ ' + escapeHtml(fileSimDriftLabel(drift[0][0])) : ''}
                </td>
            </tr>`);
        if (open && hasKids) fileSimSummaryRows(node.children, depth + 1, out);
    });
}

function renderFileSimSummary() {
    const headEl = document.getElementById('bsim-summary-head');
    const rollupEl = document.getElementById('bsim-summary-rollup');
    if (!headEl || !rollupEl) return;
    const data = binSimDataCache || {};
    const root = fileSimTreeRoot();
    const nodes = fileSimNodes(root);
    const sel = [...fileSimSelection].map(id => nodes.find(n => n.id === id)).filter(Boolean);
    const head = !fileSimSelection.size ? root : (sel.length === 1 ? sel[0] : null);
    const nameA = data.file_metadata_a?.file_name || 'Binary A';
    const nameB = data.file_metadata_b?.file_name || 'Binary B';
    const counts = data.counts || {};

    if (head && head.id === 'root') {
        headEl.innerHTML = '';
    } else if (head) {
        const drift = Object.entries(head.drift || {}).sort((x, y) => y[1] - x[1]);
        headEl.innerHTML = `
            <div style="border:1px solid var(--border); border-radius:8px; padding:14px 16px; background:var(--card-bg);">
                <div style="display:flex; align-items:baseline; justify-content:space-between; gap:12px;">
                    <div style="font-size:0.95rem; font-weight:600;">${escapeHtml(head.label)}</div>
                    <div style="font-size:1.1rem; color:var(--accent); font-family:'Consolas',monospace;">${(head.sim * 100).toFixed(1)}%</div>
                </div>
                <div style="display:flex; gap:24px; flex-wrap:wrap; font-size:0.8rem; color:var(--subtle); margin-top:8px;">
                    <div>${escapeHtml(nameA)} <b style="color:var(--text);">${Math.round(head.a)}</b> funcs</div>
                    <div>${escapeHtml(nameB)} <b style="color:var(--text);">${Math.round(head.b)}</b> funcs</div>
                </div>
                ${drift.length ? `<div style="margin-top:10px; font-size:0.78rem; color:var(--token-instruction);">
                    ⚠ version drift: ${drift.map(([p, w]) => `${escapeHtml(fileSimDriftLabel(p))} (${Math.round(w)})`).join(', ')}
                </div>` : ''}
            </div>`;
    } else {
        headEl.innerHTML = `<div style="font-size:0.82rem; color:var(--subtle); padding:4px 2px;">${fileSimSelection.size} tags selected</div>`;
    }

    const rows = fileSimScopeNodes();
    if (!rows.length) {
        rollupEl.innerHTML = '';
    } else {
        const body = [];
        fileSimSummaryRows(rows, 0, body);
        rollupEl.innerHTML = `
            <table class="bin-sim-mc-table">
                <thead>
                    <tr>
                        <th style="padding:8px 10px;">Tag</th>
                        <th style="padding:8px 10px; text-align:right; max-width:150px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${escapeHtml(nameA)}">${escapeHtml(nameA)}</th>
                        <th style="padding:8px 10px; text-align:right; max-width:150px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${escapeHtml(nameB)}">${escapeHtml(nameB)}</th>
                        <th style="padding:8px 10px; text-align:right;">Sim</th>
                        <th style="padding:8px 10px;"></th>
                        <th style="padding:8px 10px;">Drift</th>
                    </tr>
                </thead>
                <tbody>${body.join('')}</tbody>
            </table>`;
    }
    // The flow sits under the table and reads the same folding.
    renderFileSimSankey(data);
}

// ---- The one function table ---------------------------------------------

// Tabs are presets on the state filter, nothing more.
const FILESIM_TAB_STATES = {
    all: '',
    matched: 'matched',
    unmatched: 'uniq_a,uniq_b',
    unique_a: 'uniq_a',
    unique_b: 'uniq_b',
};

// Per-node row pages, keyed by tree node id ('' = the flat, ungrouped list).
let fileSimRows = {};
let fileSimFoldRows = {};

function fileSimRowState(key) {
    if (!fileSimRows[key]) {
        fileSimRows[key] = { items: [], offset: 0, total: 0, loading: false, loaded: false };
    }
    return fileSimRows[key];
}

function fileSimTableParams(prefixes, extra = {}) {
    const sort = binSimSortState.matched;
    const params = new URLSearchParams({
        table: 'all',
        collection_a: binSimCtx.collection,
        md5_a: binSimCtx.md5a,
        md5_b: binSimCtx.md5b,
        limit: BINSIM_LIMIT,
        sort_col: sort.col,
        sort_dir: sort.dir === -1 ? 'desc' : 'asc',
        collapse: 'name',
        ...binSimFilterParams('matched'),
        ...extra,
    });
    const state = FILESIM_TAB_STATES[fileSimTab] || '';
    if (state) params.set('state', state);
    if (prefixes && prefixes.length) params.set('tags', prefixes.join(','));
    if (binSimCtx.collB) params.set('collection_b', binSimCtx.collB);
    if (binSimCtx.poolId) params.set('pool', binSimCtx.poolId);
    return params;
}

async function fileSimFetchRows(prefixes, extra = {}) {
    const params = fileSimTableParams(prefixes, extra);
    const res = await fetch(`/api/diff?${params.toString()}`);
    const data = await res.json();
    Object.assign(binSimDataCache.functions_metadata, data.functions_metadata || {});
    return data;
}

// Rows are paged per node, so the 100-row page limit applies to the node you
// opened rather than truncating the whole view. `more` appends the next page.
async function loadFileSimRows(key, prefixes, { reset = false } = {}) {
    if (!binSimCtx) return;
    const st = fileSimRowState(key);
    if (st.loading) return;
    if (reset) { st.items = []; st.offset = 0; st.total = 0; st.loaded = false; }
    if (st.loaded && st.items.length >= st.total && st.total > 0) return;
    st.loading = true;
    renderFileSimRows();
    try {
        const data = await fileSimFetchRows(prefixes, { offset: st.offset });
        st.items = st.items.concat(data.items || []);
        st.offset = st.items.length;
        st.total = data.total || 0;
        st.loaded = true;
    } catch (e) {
        console.error('file sim page load failed', e);
        st.loaded = true;
    } finally {
        st.loading = false;
    }
    renderFileSimRows();
}

window.loadMoreFileSimRows = function(key) {
    const node = key ? fileSimNodeById(key) : null;
    loadFileSimRows(key, node ? fileSimNodePrefixes(node) : fileSimScopePrefixes());
};

window.collapseAllFileSimGroups = window.collapseAllFileSimNodes;

window.setFileSimGroupBy = function(mode) {
    fileSimGroupBy = mode;
    ['auto', 'tag', 'none'].forEach(m => {
        const btn = document.getElementById(`bsim-group-btn-${m}`);
        if (btn) btn.classList.toggle('active', m === mode);
    });
    renderFileSim(binSimDataCache);
};

// Duplicate fold: the server hands back one row per distinct name with
// `n_copies`, so expanding asks for that name specifically.
window.toggleFileSimFold = async function(key, name, nodeId) {
    if (fileSimOpenFolds.has(key)) {
        fileSimOpenFolds.delete(key);
        renderFileSimTable();
        return;
    }
    fileSimOpenFolds.add(key);
    if (!fileSimFoldRows[key]) {
        const node = nodeId ? fileSimNodeById(nodeId) : null;
        const prefixes = node ? fileSimNodePrefixes(node) : fileSimScopePrefixes();
        try {
            const data = await fileSimFetchRows(prefixes, { name, limit: 200 });
            fileSimFoldRows[key] = data.items || [];
        } catch (e) {
            console.error('file sim fold load failed', e);
            fileSimFoldRows[key] = [];
        }
    }
    renderFileSimTable();
};

function fileSimStateType(row) {
    if (row.state === 'matched') return 'matched';
    return row.state === 'uniq_b' ? 'uniqueB' : 'uniqueA';
}

// One row, plus the fold pill when the server says this name has copies. The
// pill is a peek past the current state filter: on the Matched tab it reveals
// the same name's unmatched leftovers without leaving the tab.
function fileSimRowHtml(row, depth, groupId) {
    const copies = (row.n_copies || 1) - 1;
    const out = [];
    let pill = '';
    let key = null;
    if (copies > 0 && row.fold_name) {
        key = (groupId || 'flat') + '/' + row.fold_name;
        const open = fileSimOpenFolds.has(key);
        pill = `<span class="bsim-fold-pill" onclick="event.stopPropagation(); toggleFileSimFold(${escapeAttr(jsString(key))}, ${escapeAttr(jsString(row.fold_name))}, ${escapeAttr(jsString(groupId || ''))})">${open ? '▼' : '▶'} ${copies} more ${copies === 1 ? 'copy' : 'copies'} of ${escapeHtml(row.fold_name)}</span>`;
    }
    out.push(renderMatchedFunctionRow(row, fileSimStateType(row), depth, pill));
    if (key && fileSimOpenFolds.has(key)) {
        const rows = fileSimFoldRows[key];
        if (!rows) {
            out.push(`<tr><td colspan="7" style="padding:8px 0 8px ${28 + depth * 22}px; color:var(--dim); font-size:0.75rem;">Loading copies…</td></tr>`);
        } else {
            rows.forEach(r => {
                // The representative is already shown above it.
                if (r.func_a === row.func_a && r.func_b === row.func_b && r.func_id === row.func_id) return;
                out.push(renderMatchedFunctionRow(r, fileSimStateType(r), depth + 1));
            });
        }
    }
    return out.join('');
}

// mode: 'both' (All/Matched -- 7 cols, both sides) or 'a'/'b' (Unique to A/B --
// rows are always one-sided there, so Similarity and the other side's two blank
// columns are pointless; drop them to 4 cols).
function fileSimColMode() {
    if (fileSimTab === 'unique_a') return 'a';
    if (fileSimTab === 'unique_b') return 'b';
    return 'both';
}

function fileSimTableHeadHtml(mode) {
    const data = binSimDataCache || {};
    const nameA = data.file_metadata_a?.file_name || 'Binary A';
    const nameB = data.file_metadata_b?.file_name || 'Binary B';
    const icon = (col) => binSimSortState.matched.col === col
        ? (binSimSortState.matched.dir === -1 ? '▼' : '▲') : '↕';
    const simTh = `<th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable" onclick="setBinSimSort('matched','similarity')">Similarity <small>${icon('similarity')}</small></th>`;
    const featTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width:80px;" class="sortable" onclick="setBinSimSort('matched','avg_features')" title="BSim feature count (A / B for a match)">Features <small>${icon('avg_features')}</small></th>`;
    const clusterTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width:150px;" class="sortable" onclick="setBinSimSort('matched','cluster_name')">Cluster <small>${icon('cluster_name')}</small></th>`;
    const aTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);">${escapeHtml(nameA)}</th>`;
    const aNotesTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width:50px;">Notes</th>`;
    const bTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);">${escapeHtml(nameB)}</th>`;
    const bNotesTh = `<th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width:50px;">Notes</th>`;

    const headCells = mode === 'a' ? [featTh, clusterTh, aTh, aNotesTh]
        : mode === 'b' ? [featTh, clusterTh, bTh, bNotesTh]
        : [simTh, featTh, clusterTh, aTh, aNotesTh, bTh, bNotesTh];

    const searchColspan = mode === 'both' ? 4 : 2;
    const filterCells = mode === 'both' ? `
            <th style="position:relative;"><div style="display:flex; flex-direction:column; gap:3px;" onclick="event.stopPropagation()">
                <div style="display:flex; align-items:center; gap:2px;">
                    <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-coh-min" placeholder="Min..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
                    <span class="dim" style="font-size:0.6rem">-</span>
                    <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-coh-max" placeholder="Max..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
                </div>
                <div class="tag-filter-container" id="tag-container-bsim-sim">
                    <input type="text" class="tag-filter-add" placeholder="+ Sim tag"
                           onkeydown="binSimSimTagAdd(event)"
                           onfocus="attachTagAutocomplete(this, (val) => { createTagCard('bsim-sim', 'sim_tag', val, false, false); this.value=''; binSimFilterChange(true); })">
                </div>
            </div></th>` : '';
    return `
        <tr>${headCells.join('')}</tr>
        <tr class="filter-row">
            ${filterCells}
            <th><div style="display:flex; align-items:center; gap:2px;" onclick="event.stopPropagation()">
                <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-feat-min" placeholder="Min..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
                <span class="dim" style="font-size:0.6rem">-</span>
                <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-feat-max" placeholder="Max..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
            </div></th>
            <th><div onclick="event.stopPropagation()">
                <input type="text" oninput="binSimFilterChange(true)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-cl-q" placeholder="Cluster name..." style="font-size:0.65rem; box-sizing:border-box; width:100%;">
            </div></th>
            <th colspan="${searchColspan}"><div onclick="event.stopPropagation()">
                <input type="text" oninput="binSimFilterChange(true)" onkeydown="if(event.key==='Enter') binSimFilterChange(true)" id="bsim-flt-matched-q" placeholder="Search name / tag / addr..." style="font-size:0.65rem; box-sizing:border-box; width:100%;">
            </div></th>
        </tr>`;
}

function fileSimMoreRowHtml(key, st, indent) {
    if (st.items.length >= st.total) return '';
    // Keyed so Enter can be pressed on it again and again: the row comes back at a
    // new index after each page, and the focus has to follow it there.
    return `
        <tr data-rowkey="more:${escapeAttr(key)}"><td colspan="7" style="padding:8px 10px 8px ${indent}px; background:var(--bg);">
            <button class="btn-primary" style="padding:6px 16px; font-size:0.78rem;" onclick="loadMoreFileSimRows(${escapeAttr(jsString(key))})">
                Load More Results (${st.total - st.items.length} remaining)
            </button>
            <span style="color:var(--dim); font-size:0.72rem; margin-left:8px;">showing ${st.items.length} of ${st.total} names</span>
        </td></tr>`;
}

// Groups recurse with the tree: opening a node with children shows its child
// groups, opening a leaf loads that leaf's functions.
function fileSimGroupRows(nodes, depth, out) {
    nodes.forEach(node => {
        const open = fileSimTreeOpen.has(node.id);
        const hasKids = (node.children || []).length > 0;
        out.push(`
            <tr class="bsim-grp-row" data-rowkey="${escapeAttr(node.id)}" onclick="toggleFileSimNode(${escapeAttr(jsString(node.id))})">
                <td colspan="7" style="padding-left:${10 + depth * 18}px;">
                    <div style="display:flex; align-items:center; gap:10px;">
                        <span style="color:var(--subtle); width:12px;">${open ? '▼' : '▶'}</span>
                        ${fileSimDotHtml(node.id)}
                        <span style="font-weight:600;">${escapeHtml(node.label)}</span>
                        <div style="flex:1;"></div>
                        <span style="color:var(--subtle); font-size:0.74rem;">${Math.round(node.a)} A</span>
                        <span style="color:var(--subtle); font-size:0.74rem;">${Math.round(node.b)} B</span>
                        <span style="color:var(--accent); font-size:0.74rem; width:44px; text-align:right;">${(node.sim * 100).toFixed(0)}%</span>
                    </div>
                </td>
            </tr>`);
        if (!open) return;
        if (hasKids) { fileSimGroupRows(node.children, depth + 1, out); return; }

        const st = fileSimRows[node.id];
        if (!st || (!st.loaded && !st.loading)) {
            // Opened but never fetched: kick it off, render a placeholder now.
            loadFileSimRows(node.id, fileSimNodePrefixes(node));
            out.push(`<tr><td colspan="7" style="padding:14px ${30 + depth * 18}px; color:var(--dim); font-size:0.78rem;">Loading…</td></tr>`);
            return;
        }
        if (st.loading && !st.items.length) {
            out.push(`<tr><td colspan="7" style="padding:14px ${30 + depth * 18}px; color:var(--dim); font-size:0.78rem;">Loading…</td></tr>`);
            return;
        }
        if (!st.items.length) {
            out.push(`<tr><td colspan="7" style="padding:14px ${30 + depth * 18}px; color:var(--dim); font-size:0.78rem;">No functions match this scope.</td></tr>`);
            return;
        }
        st.items.forEach(row => out.push(fileSimRowHtml(row, depth + 1, node.id)));
        out.push(fileSimMoreRowHtml(node.id, st, 30 + depth * 18));
    });
}

function renderFileSimTable() {
    const tbody = document.getElementById('bin-sim-table-matched');
    if (!tbody) return;
    const thead = tbody.previousElementSibling;
    const colMode = fileSimColMode();
    if (thead && thead.dataset.built !== colMode) {
        thead.innerHTML = fileSimTableHeadHtml(colMode);
        thead.dataset.built = colMode;
        restoreFileSimFilters();
    }
    const countEl = document.getElementById('bsim-table-count');
    const scope = fileSimScopeNodes();
    // Auto: a single tag needs no header of its own; several do.
    const grouped = fileSimGroupBy === 'tag'
        || (fileSimGroupBy === 'auto' && scope.length > 1);
    const out = [];

    if (!grouped) {
        const st = fileSimRowState('');
        if (!st.loaded && !st.loading) loadFileSimRows('', fileSimScopePrefixes());
        if (st.loading && !st.items.length) {
            out.push('<tr><td colspan="7" style="padding:30px; text-align:center; color:var(--dim);">Loading…</td></tr>');
        } else if (!st.items.length) {
            out.push('<tr><td colspan="7" style="padding:30px; text-align:center; color:var(--dim);">No functions match this scope.</td></tr>');
        } else {
            st.items.forEach(row => out.push(fileSimRowHtml(row, 0, null)));
            out.push(fileSimMoreRowHtml('', st, 20));
        }
        if (countEl) countEl.textContent = st.total ? `${st.items.length} of ${st.total} names` : '';
    } else {
        fileSimGroupRows(scope, 0, out);
        if (countEl) countEl.textContent = `${scope.length} tag groups`;
    }
    tbody.innerHTML = out.join('');
    // Same cell selection, arrow/shift navigation and copy as every other table.
    // The constructor is idempotent per table element.
    if (window.TableSelection) new window.TableSelection('bin-sim-table-matched-table');
}

// ---- The same rows as a graph -------------------------------------------
// Function to function, no cluster in between: a match is a pair, and routing it
// through a cluster node said nothing the pair did not already say. The rows are
// the table's rows, so the tab's state filter and the tree's scope carry over.

function fileSimFuncLabel(fid) {
    const meta = (binSimDataCache && binSimDataCache.functions_metadata) || {};
    const m = meta[fid];
    return (m && m.name) ? m.name : ('@' + String(fid).split(':').pop());
}

function renderFileSimGraph() {
    const host = document.getElementById('bsim-fngraph');
    if (!host) return;
    const st = fileSimRowState('');
    if (!st.loaded && !st.loading) loadFileSimRows('', fileSimScopePrefixes());
    const countEl = document.getElementById('bsim-table-count');
    if (countEl) countEl.textContent = st.total ? `${st.items.length} of ${st.total} names` : '';

    const msg = (text) => {
        host.innerHTML = `<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">${text}</div>`;
    };
    if (st.loading && !st.items.length) return msg('Loading…');
    if (!st.items.length) return msg('No functions match this scope.');

    const data = binSimDataCache || {};
    const nameA = data.file_metadata_a?.file_name || 'A';
    const nameB = data.file_metadata_b?.file_name || 'B';

    const nodes = [];
    const index = new Map();
    const addNode = (id, name, color, side) => {
        if (!index.has(id)) {
            index.set(id, nodes.length);
            nodes.push({ id, name, color, side });
        }
        return index.get(id);
    };
    const links = [];

    st.items.forEach(r => {
        // A folded row stands for every copy of that name, so it flows that thick.
        const value = r.n_copies || 1;
        if (r.state === 'matched' && r.func_a && r.func_b) {
            const sim = r.similarity || 0;
            const color = `hsl(${sim * 120}, var(--color-s-med), var(--color-l-dim))`;
            links.push({
                source: addNode('a_' + r.func_a, fileSimFuncLabel(r.func_a), color, 0),
                target: addNode('b_' + r.func_b, fileSimFuncLabel(r.func_b), color, 1),
                value, tip: `${fileSimFuncLabel(r.func_a)} → ${fileSimFuncLabel(r.func_b)}\nSimilarity: ${(sim * 100).toFixed(1)}%${value > 1 ? `\n${value} copies` : ''}`,
            });
        } else if (r.state === 'uniq_a' && r.func_id) {
            links.push({
                source: addNode('a_' + r.func_id, fileSimFuncLabel(r.func_id), '#f92672', 0),
                target: addNode('none_b', `No match in ${nameB}`, '#f92672', 1),
                value, tip: `${fileSimFuncLabel(r.func_id)}\nOnly in ${nameA}`,
            });
        } else if (r.state === 'uniq_b' && r.func_id) {
            links.push({
                source: addNode('none_a', `No match in ${nameA}`, '#66d9ef', 0),
                target: addNode('b_' + r.func_id, fileSimFuncLabel(r.func_id), '#66d9ef', 1),
                value, tip: `${fileSimFuncLabel(r.func_id)}\nOnly in ${nameB}`,
            });
        }
    });

    if (!links.length) return msg('Not enough data for graph');

    host.innerHTML = '';
    const perSide = [0, 1].map(s => nodes.filter(n => n.side === s).length);
    const maxNodes = Math.max(...perSide, 6);
    const width = host.clientWidth || 800;
    const padding = maxNodes > 40 ? 2 : 8;
    const height = Math.max(host.clientHeight || 400, maxNodes * (padding + 10) + 40);

    const svg = d3.select(host).append('svg').attr('width', width).attr('height', height);
    const g = svg.append('g');

    const sankey = d3.sankey()
        .nodeWidth(14)
        .nodePadding(padding)
        .nodeAlign(n => n.side)
        .extent([[25, 10], [width - 25, height - 10]]);

    let graph;
    try {
        graph = sankey({
            nodes: nodes.map(d => Object.assign({}, d)),
            links: links.map(d => Object.assign({}, d)),
        });
    } catch (e) {
        console.error('file sim function graph layout failed', e);
        return msg('Graph layout error');
    }

    g.append('g').selectAll('path')
        .data(graph.links)
        .enter().append('path')
        .attr('d', d => {
            const x0 = d.source.x1, x1 = d.target.x0;
            const x2 = x0 + (x1 - x0) * 0.4, x3 = x0 + (x1 - x0) * 0.6;
            return `M ${x0},${d.source.y0}
                    C ${x2},${d.source.y0} ${x3},${d.target.y0} ${x1},${d.target.y0}
                    L ${x1},${d.target.y1}
                    C ${x3},${d.target.y1} ${x2},${d.source.y1} ${x0},${d.source.y1}
                    Z`;
        })
        .attr('fill', d => d.target.color || 'var(--text)')
        .style('fill-opacity', 0.4)
        .on('mouseenter', function () { d3.select(this).style('fill-opacity', 0.75); })
        .on('mouseleave', function () { d3.select(this).style('fill-opacity', 0.4); })
        .append('title')
        .text(d => d.tip || `${d.source.name}\n  ↓\n${d.target.name}`);

    const node = g.append('g').selectAll('.node')
        .data(graph.nodes)
        .enter().append('g')
        .attr('class', 'node')
        .attr('transform', d => `translate(${d.x0},${d.y0})`);

    node.append('rect')
        .attr('height', d => Math.max(1, d.y1 - d.y0))
        .attr('width', sankey.nodeWidth())
        .attr('fill', d => d.color)
        .attr('stroke', 'var(--border)')
        .attr('stroke-width', '0.5px')
        .attr('opacity', 0.6)
        .append('title')
        .text(d => d.name);

    node.append('text')
        .attr('x', d => (d.side === 1 ? -6 : 6 + sankey.nodeWidth()))
        .attr('y', d => (d.y1 - d.y0) / 2)
        .attr('dy', '0.35em')
        .attr('text-anchor', d => (d.side === 1 ? 'end' : 'start'))
        .text(d => d.name)
        .attr('fill', 'var(--text)')
        .attr('font-size', '9px')
        .attr('opacity', 0.75)
        .attr('font-family', 'sans-serif');
}

// Table or graph: same rows, so anything that changes them renders through here
// instead of each caller knowing which of the two is on screen.
function renderFileSimRows() {
    if (fileSimView === 'graph') renderFileSimGraph();
    else renderFileSimTable();
}

// The graph draws the flat scope, so grouping controls have nothing to say in it.
// ponytail: graph pages with the table (100 names); Load more is the table's.
window.setFileSimView = function(view) {
    fileSimView = view;
    ['table', 'graph'].forEach(v => {
        const btn = document.getElementById(`bsim-view-btn-${v}`);
        if (btn) btn.classList.toggle('active', v === view);
        const card = document.getElementById(v === 'table' ? 'bsim-table-card' : 'bsim-fngraph-card');
        if (card) card.style.display = (v === view) ? 'flex' : 'none';
    });
    const ctl = document.getElementById('bsim-table-controls');
    if (ctl) ctl.style.display = view === 'graph' ? 'none' : 'flex';
    renderFileSimRows();
};

// ---- Entry point ---------------------------------------------------------

function renderFileSim(data) {
    if (!data) return;
    // Colours come from a config fetch; without it every dot is grey, so draw
    // again once it lands.
    if (!TagColor.config()) TagColor.ready.then(() => renderFileSim(data));
    renderFileSimAxisPicker();
    renderFileSimTree();
    renderFileSimChips();
    if (fileSimTab === 'summary') renderFileSimSummary();
    else renderFileSimRows();
}

// ---- File sim tab: sankey view ------------------------------------------
// Same tag composition as the table, drawn as flow. Deliberately stops at
// Shared / Unique to A / Unique to B per tag -- function-level detail is the
// Function graph tab's job. Depth is a namespace frontier over the tag ids
// (`lib:libc:2.31`), so a whole namespace or a whole library folds to one node.

// Colour here means "which tag", never "how well it matched". Match quality is
// a number in the tooltip; hue is spent on the one thing the eye can track
// across five columns and two axes, which is a tag's identity. `TagColor`
// derives it from the tag id, so a node keeps its colour between views.

// The axes the graph can draw, and the doc field each reads. `tags_summary` is
// the origin axis under its historical name. Must mirror bin_sim_tags.AXES.
const FILESIM_AXES = {
    origin: { field: 'tags_summary', label: 'Origin' },
    severity: { field: 'severity_summary', label: 'Severity' },
    category: { field: 'category_summary', label: 'Behavior' },
    user: { field: 'user_summary', label: 'User' },
    capa: { field: 'capa_summary', label: 'Capa' },
    mitre: { field: 'mitre_summary', label: 'ATT&CK' },
    yara: { field: 'yara_summary', label: 'YARA' },
    family: { field: 'family_summary', label: 'Family' },
    vuln: { field: 'vuln_summary', label: 'Vulnerability' },
    ruleset: { field: 'ruleset_summary', label: 'Ruleset' },
};
// Which axis is on each side. An empty B is a single-axis view.
let fileSimAxisA = 'origin';
let fileSimAxisB = 'category';

// Key layout of the stored joint table, mirroring bin_sim_tags.JOINT_INNER_AXES:
// origin is the outer key, the rest are packed into the inner key in this order.
const FILESIM_AXIS_SEP = '\u001f';
const FILESIM_COMBO_SEP = ' + ';
const FILESIM_JOINT_INNER = ['severity', 'category', 'user', 'capa', 'mitre',
                             'yara', 'family', 'vuln', 'ruleset'];

// `category:network:c2` reads as "network c2"; `severity:high` as "high". A
// combo of several keeps them joined.
function fileSimFlagLabel(flagId) {
    return String(flagId).split(FILESIM_COMBO_SEP).map(t => {
        const parts = String(t).split(':');
        return (parts.length > 1 ? parts.slice(1) : parts).join(' ');
    }).join(FILESIM_COMBO_SEP);
}

// Collapse the stored joint down to the two axes being drawn. Every one of the
// ten views is a marginal of that one table, which is why switching axes needs
// no backend call at all.
//
// The A side is expanded back out to individual tags so its nodes line up with
// the axis summary rows the rest of the pane draws. For origin and severity
// that is exact -- a function has one of each. Category and user overlap, so an
// A column over them can exceed the pair total, exactly as their summary rows
// already do. The B side stays a combo, because a flow diagram can only stay
// countable in whole functions if a function lands in one cell, not half in two.
function fileSimJointMarginal(joint, axisA, axisB) {
    const out = {};
    const idx = (ax) => FILESIM_JOINT_INNER.indexOf(ax);
    Object.entries(joint || {}).forEach(([outer, row]) => {
        Object.entries(row || {}).forEach(([inner, cell]) => {
            const parts = String(inner).split(FILESIM_AXIS_SEP);
            if (parts.length !== FILESIM_JOINT_INNER.length) return;
            const aRaw = axisA === 'origin' ? outer : parts[idx(axisA)];
            if (!aRaw) return;
            const bKey = !axisB ? '' : (axisB === 'origin' ? outer : parts[idx(axisB)]);
            const aKeys = axisA === 'origin' ? [outer] : aRaw.split(FILESIM_COMBO_SEP);
            aKeys.forEach(a => {
                if (!a) return;
                const dst = out[a] || (out[a] = {});
                const acc = dst[bKey] || (dst[bKey] = new Array(8).fill(0));
                for (let i = 0; i < 8; i++) acc[i] += cell[i] || 0;
            });
        });
    });
    return out;
}

// The chain of tree nodes a tag id belongs to, outermost first. The Sankey needs
// this to fold exactly where the tree folds instead of keeping its own frontier.
// Depth is whatever the axis's tree has, so origin's three levels and the other
// axes' one both work off the same walk.
function fileSimChainFor(tagId, node, chain = []) {
    for (const child of node.children || []) {
        if (!(child.tagIds || []).includes(tagId)) continue;
        return fileSimChainFor(tagId, child, chain.concat(child));
    }
    return chain;
}

// Descend the chain while each node is open; the node we stop at is what the
// graph draws, and `more` says whether unfolding it would still change the
// picture. Same rule the table's group rows follow.
function fileSimFrontier(tagId, root) {
    const chain = fileSimChainFor(tagId, root);
    if (!chain.length) return null;
    let i = 0;
    while (i < chain.length - 1 && fileSimTreeOpen.has(chain[i].id)) i++;
    return { node: chain[i], more: i < chain.length - 1 };
}

// A tag row's four masses, in whichever metric is selected. Shared mass is
// per-side on purpose: a match need not be tagged the same on both binaries,
// so A's shared count and B's shared count can differ.
function fileSimRowMass(row, scale) {
    if (scale === 'features') {
        return [row.weight_a || 0, row.weight_b || 0, row.unique_weight_a || 0, row.unique_weight_b || 0];
    }
    let sa = 0, sb = 0;
    Object.keys(row.bins || {}).forEach(k => {
        sa += row.bins[k][0] || 0;
        sb += row.bins[k][2] || 0;
    });
    return [sa, sb, row.unique_count_a || 0, row.unique_count_b || 0];
}

// Fold the tag rows onto the tree's current frontier. The Sankey used to keep
// its own namespace-depth override, which meant the graph and the tree could
// disagree about what "libc" currently means; it now reads the shared state.
function fileSimSankeyGroups(rows, scale, matrix) {
    const groups = new Map();
    // Every axis has a tree now, so the graph folds where that axis's tree folds.
    // The rows the graph gets are one per display parent, which on the non-origin
    // axes is the deepest level the stored joint is keyed at -- their trees can
    // drill one level further (into `category:network:c2`) to scope the table,
    // and `more` is what keeps the graph from advertising a fold it cannot draw.
    const root = fileSimTreeRoot();
    // Cells are [w_shared_a, w_shared_b, w_uniq_a, w_uniq_b, then the same four
    // as function counts], so the metric toggle is a 4-slot offset.
    const off = scale === 'features' ? 0 : 4;
    (rows || []).forEach(row => {
        const front = fileSimFrontier(row.tag_id, root);
        if (!front) return;
        const node = front.node;
        const key = node.id;
        let g = groups.get(key);
        if (!g) {
            g = {
                key, label: node.label, depth: 1,
                sharedA: 0, sharedB: 0, uniqA: 0, uniqB: 0,
                cohNum: 0, cohDen: 0, tags: 0,
                // flag id -> [shared A, shared B] of this provenance node's mass.
                flags: new Map(), flagA: 0, flagB: 0,
                expandable: false,
            };
            groups.set(key, g);
        }
        // Expandable means "this node still folds rows the graph has", not
        // "the tree has children here" -- on the non-origin axes the tree goes
        // one level deeper than the graph's rows do.
        if (front.more) g.expandable = true;
        const [sa, sb, ua, ub] = fileSimRowMass(row, scale);
        g.sharedA += sa; g.sharedB += sb; g.uniqA += ua; g.uniqB += ub;
        // score_weight (matched + this tag's own unmatched mass) is score's real
        // denominator now -- reconstructing against matched_weight alone would
        // drop the unique share and inflate the folded group's score.
        const scoreWeight = row.score_weight != null ? row.score_weight : row.matched_weight;
        g.cohNum += (row.score || 0) * (scoreWeight || 0);
        g.cohDen += scoreWeight || 0;
        g.tags += 1;
        // The other axis, folded onto the same node: how much of this tag's
        // matched mass someone flagged. Rows fold, so their cells fold with them.
        Object.entries((matrix || {})[row.tag_id] || {}).forEach(([flag, cell]) => {
            const cur = g.flags.get(flag) || [0, 0];
            cur[0] += cell[off] || 0;
            cur[1] += cell[off + 1] || 0;
            g.flags.set(flag, cur);
            g.flagA += cell[off] || 0;
            g.flagB += cell[off + 1] || 0;
        });
    });
    return [...groups.values()]
        .filter(g => g.sharedA + g.sharedB + g.uniqA + g.uniqB > 0)
        .sort((x, y) => (y.sharedA + y.sharedB + y.uniqA + y.uniqB) - (x.sharedA + x.sharedB + x.uniqA + x.uniqB));
}

// Click a graph node: fold or unfold the tree node it stands for. `key` is a
// tree node id, so this is the same toggle the sidebar and the table use --
// clicking libc in the graph collapses it everywhere.
window.toggleFileSimNs = function(key) {
    window.toggleFileSimNode(key);
};

// Tagging changes the split, never the score, so a stale split is an offer to
// recompute rather than a reason to invalidate the pair. Pinned to the hero
// corner as a small amber pill, not a full-width row, so it stands out by
// color instead of by size.
let binSimResplitPoll = null;
const BSIM_RESPLIT_AMBER = '#f0ad4e';

function renderFileSimResplit(stale) {
    const banner = document.getElementById('bin-sim-resplit-banner');
    if (!banner) return;
    // A poll already in flight is tracking this pair's job; don't stomp it
    // with the stale-button markup on every re-render of the sankey.
    if (binSimResplitPoll) return;
    banner.style.display = stale ? 'flex' : 'none';
    banner.innerHTML = stale
        ? `<button id="bin-sim-resplit-btn" onclick="resplitBinSimTags()"
             style="display:flex; align-items:center; gap:6px; font-size:0.78rem; font-weight:600; padding:5px 10px; border-radius:999px; border:1px solid ${BSIM_RESPLIT_AMBER}; color:#3a2400; background:${BSIM_RESPLIT_AMBER}; cursor:pointer; box-shadow:0 1px 3px rgba(0,0,0,0.3);"
             title="Tags changed since this pair was split. The score is unaffected; only its breakdown by tag is.">
             <i class="fa-solid fa-arrows-rotate"></i> Tags changed &mdash; refresh split</button>`
        : '';
}

function setBinSimResplitBanner(html) {
    const banner = document.getElementById('bin-sim-resplit-banner');
    if (!banner) return;
    banner.style.display = 'flex';
    banner.innerHTML = html;
}

window.resplitBinSimTags = async function() {
    if (!binSimCtx) return;
    const ctx = binSimCtx; // snapshot: user may navigate to another pair mid-poll
    setBinSimResplitBanner(`<span style="display:flex; align-items:center; gap:6px; font-size:0.78rem; font-weight:600; padding:5px 10px; border-radius:999px; border:1px solid ${BSIM_RESPLIT_AMBER}; color:#3a2400; background:${BSIM_RESPLIT_AMBER};">
        <i class="fa-solid fa-spinner fa-spin"></i> Resplitting&hellip;</span>`);
    try {
        const res = await fetch('/api/bin_sim/resplit', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                collection: ctx.collection,
                algo: new URLSearchParams(location.search).get('algo') || 'unweighted_cosine',
                // Only pairs naming these two can have changed. Resplitting the
                // whole collection would rewrite thousands of identical docs.
                md5: [ctx.md5a, ctx.md5b],
            }),
        });
        const out = await res.json();
        if (out.status !== 'success') {
            showToast(out.message || 'Resplit failed', 'error');
            renderFileSimResplit(true);
            return;
        }
        pollBinSimResplitJob(out.job_id, ctx);
    } catch (e) {
        showToast('Resplit failed: ' + e, 'error');
        renderFileSimResplit(true);
    }
};

function pollBinSimResplitJob(jobId, ctx) {
    if (binSimResplitPoll) clearInterval(binSimResplitPoll);
    binSimResplitPoll = setInterval(async () => {
        let job;
        try {
            const res = await fetch(`/api/jobs/${jobId}`);
            job = await res.json();
        } catch (e) {
            return; // transient fetch error, try again next tick
        }
        if (!job || job.status === 'pending' || job.status === 'running') return;

        clearInterval(binSimResplitPoll);
        binSimResplitPoll = null;

        if (job.status !== 'completed') {
            showToast('Tag resplit job failed', 'error');
            renderFileSimResplit(true);
            return;
        }
        showToast('Tag resplit complete — refreshing', 'success');
        // Only refresh if the user is still looking at the pair the resplit was for.
        if (binSimCtx && binSimCtx.md5a === ctx.md5a && binSimCtx.md5b === ctx.md5b
            && binSimCtx.collection === ctx.collection) {
            fetchAndRenderBinaryDiff(ctx.collection, ctx.md5a, ctx.md5b, ctx.collB, ctx.poolId);
        }
    }, 2000);
}

function renderFileSimSankey(data) {
    renderFileSimResplit(data.tags_stale);
    const container = document.getElementById('bin-sim-filesim-sankey');
    if (!container) return;
    // Colours come from a config the page fetches once. Drawing before it lands
    // would paint the whole graph grey, so the first render waits for it.
    if (!TagColor.config()) {
        TagColor.ready.then(() => renderFileSimSankey(data));
        return;
    }
    container.innerHTML = '';

    // Rows come from whichever axis is on the left, scoped by that axis's own
    // tree -- selecting libc, or `network`, narrows the flow the same way.
    const axisA = fileSimAxisKey();
    const axisB = fileSimAxisB === axisA ? '' : fileSimAxisB;
    const rows = data[FILESIM_AXES[axisA].field] || [];
    const groups = fileSimSankeyGroups(
        fileSimScopeRows(rows),
        fileSimScale,
        axisB ? fileSimJointMarginal(data.joint, axisA, axisB) : null
    );
    // The crossed stage is drawn only when the second axis actually has mass, so
    // a single-axis view keeps exactly the three columns it has always had.
    const hasFlags = groups.some(g => g.flagA + g.flagB > 0);
    const COL_A = 0;
    const COL_MID = hasFlags ? 2 : 1;
    const COL_B = hasFlags ? 4 : 2;
    const COLUMNS = hasFlags ? [0, 1, 2, 3, 4] : [0, 1, 2];
    if (!groups.length) {
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">No tag data for this scope.</div>';
        return;
    }

    const filenameA = data.file_metadata_a?.file_name || 'A';
    const filenameB = data.file_metadata_b?.file_name || 'B';
    const suffix = fileSimScale === 'features' ? 'feats' : 'funcs';
    const fmt = (v) => (v % 1 !== 0 ? v.toFixed(1) : String(Math.round(v)));

    const nodes = [];
    const links = [];
    const addNode = (id, name, color, extra) => {
        const n = Object.assign({ id, name, color, index: nodes.length }, extra || {});
        nodes.push(n);
        return n;
    };

    groups.forEach((g, i) => {
        const totalA = g.sharedA + g.uniqA;
        const totalB = g.sharedB + g.uniqB;
        // Composition similarity, same measure as the table: how evenly the tag's
        // mass is carried by both binaries.
        const comp = Math.max(totalA, totalB) > 0 ? Math.min(totalA, totalB) / Math.max(totalA, totalB) : 0;
        const score = g.cohDen > 0 ? g.cohNum / g.cohDen : 0;
        const tagColor = TagColor.forTag(g.key);
        // An outer node is always its tag's colour, matched or not -- the two
        // ends of the graph are the tag composition of each binary, and greying
        // half of it hides which tag the unmatched mass belongs to. Only the
        // middle greys: that column is about the pairing, and unmatched mass has
        // no pairing to report. The link between them does the fade, so the flow
        // reads as a tag draining into nothing.
        const unmatchedColor = TagColor.forTag(g.key, { gray: true });
        const marker = g.expandable ? ' ▸' : '';

        // Each side is split by category at the source: a tag's shared mass and its
        // unmatched mass are separate side nodes. One node fanning out into both
        // reads as "all of original_code is shared AND unique", which it is not.
        const stat = `Composition: ${(comp * 100).toFixed(0)}%  ·  Match quality: ${(score * 100).toFixed(0)}%\n${g.tags} tag${g.tags === 1 ? '' : 's'} folded`;
        // Single axis: the middle node is the same tag as the two ends, so it
        // takes their colour -- one unbroken band across the graph. Crossed: the
        // middle column IS the crossing, so it wears the crossed axis's colour
        // and the A / B ends stay the main axis's.

        if (g.sharedA > 0 || g.sharedB > 0) {
            const unionKeys = new Set([...g.flags.keys()]);
            let flaggedA = 0, flaggedB = 0;
            for (const v of g.flags.values()) {
                flaggedA += v[0];
                flaggedB += v[1];
            }
            const restA = Math.max(0, g.sharedA - flaggedA);
            const restB = Math.max(0, g.sharedB - flaggedB);
            if (restA > 0 || restB > 0) unionKeys.add('');

            // Sort so unflagged is at the end, else by max size
            const keys = [...unionKeys].sort((x, y) => {
                if (x === '') return 1;
                if (y === '') return -1;
                const vx = g.flags.get(x);
                const vy = g.flags.get(y);
                const max_x = Math.max(vx[0], vx[1]);
                const max_y = Math.max(vy[0], vy[1]);
                return max_y - max_x;
            });

            keys.forEach((flagId, k) => {
                const valA = flagId === '' ? restA : g.flags.get(flagId)[0];
                const valB = flagId === '' ? restB : g.flags.get(flagId)[1];
                if (valA <= 0 && valB <= 0) return;

                const lbl = flagId ? fileSimFlagLabel(flagId) : 'unflagged';
                const suffixLbl = hasFlags ? ` · ${lbl}` : '';
                // A crossed node belongs to the other axis, so it wears that
                // axis's tag colour -- the colour change mid-graph is the
                // crossing. A combo is drawn as its first tag: the node is one
                // rectangle and its label already names the rest.
                const flagColor = flagId
                    ? TagColor.forTag(String(flagId).split(FILESIM_COMBO_SEP)[0])
                    : 'var(--dim)';

                // A single mid node for THIS specific flag combination, separating the flows completely
                const mid = addNode(`fsk_s_${i}_${k}`, `${g.label}${suffixLbl} shared (${fmt(Math.max(valA, valB))} ${suffix})`, hasFlags ? flagColor : tagColor, {
                    align: COL_MID, tagIdx: i, sort: i * 10 + k * 0.01,
                    tip: `${g.label}${suffixLbl} · shared\n${filenameA}: ${fmt(valA)} ${suffix}\n${filenameB}: ${fmt(valB)} ${suffix}\nMatch quality: ${(score * 100).toFixed(0)}%`,
                });

                if (valA > 0) {
                    const outerA = addNode(
                        hasFlags ? `fsk_as_${i}_${k}` : `fsk_as_${i}`,
                        `${g.label}${marker}${suffixLbl} shared (${fmt(valA)} ${suffix})`,
                        tagColor,
                        {
                            align: COL_A, tagIdx: i, sort: i * 10 + k * 0.01,
                            tagKey: g.key, expandable: g.expandable,
                            tip: `${filenameA} · ${g.label}${suffixLbl} — matched\n${fmt(valA)} of ${fmt(totalA)} ${suffix}\n${stat}`,
                        }
                    );
                    if (!hasFlags) {
                        links.push({ source: outerA.index, target: mid.index, value: valA });
                    } else {
                        const flagA = addNode(`fsk_fl_a_${i}_${k}`, `${lbl} (${fmt(valA)} ${suffix})`,
                            flagColor, {
                                align: 1, tagIdx: i, sort: i * 10 + k * 0.01,
                                tip: flagId
                                    ? `${g.label} · ${lbl}\n${fmt(valA)} of ${fmt(g.sharedA)} matched ${suffix}`
                                    : `${g.label} · no flag raised\n${fmt(valA)} of ${fmt(g.sharedA)} matched ${suffix}`,
                            });
                        links.push({ source: outerA.index, target: flagA.index, value: valA });
                        links.push({ source: flagA.index, target: mid.index, value: valA });
                    }
                }

                if (valB > 0) {
                    const outerB = addNode(
                        hasFlags ? `fsk_bs_${i}_${k}` : `fsk_bs_${i}`,
                        `${g.label}${marker}${suffixLbl} shared (${fmt(valB)} ${suffix})`,
                        tagColor,
                        {
                            align: COL_B, tagIdx: i, sort: i * 10 + k * 0.01,
                            tagKey: g.key, expandable: g.expandable,
                            tip: `${filenameB} · ${g.label}${suffixLbl} — matched\n${fmt(valB)} of ${fmt(totalB)} ${suffix}\n${stat}`,
                        }
                    );
                    if (!hasFlags) {
                        links.push({ source: mid.index, target: outerB.index, value: valB });
                    } else {
                        const flagB = addNode(`fsk_fl_b_${i}_${k}`, `${lbl} (${fmt(valB)} ${suffix})`,
                            flagColor, {
                                align: 3, tagIdx: i, sort: i * 10 + k * 0.01,
                                tip: flagId
                                    ? `${g.label} · ${lbl}\n${fmt(valB)} of ${fmt(g.sharedB)} matched ${suffix}`
                                    : `${g.label} · no flag raised\n${fmt(valB)} of ${fmt(g.sharedB)} matched ${suffix}`,
                            });
                        links.push({ source: mid.index, target: flagB.index, value: valB });
                        links.push({ source: flagB.index, target: outerB.index, value: valB });
                    }
                }
            });
        }
        if (g.uniqA > 0) {
            // Unmatched mass has no crossed-axis cell to pass through, so it ends
            // in the column right next to its own side. Sending it to the middle
            // instead made its link span two columns and run underneath the
            // crossed nodes -- the superposition that made the crossed view unreadable.
            const mid = addNode(`fsk_ua_${i}`, `${g.label} only in ${filenameA} (${fmt(g.uniqA)} ${suffix})`, unmatchedColor, {
                align: COL_A + 1, tagIdx: i, sort: i * 10 + 1, tip: `${g.label}\nUnique to ${filenameA}: ${fmt(g.uniqA)} ${suffix}`,
            });
            const n = addNode(`fsk_au_${i}`, `${g.label}${marker} unmatched (${fmt(g.uniqA)} ${suffix})`, tagColor, {
                align: COL_A, tagIdx: i, sort: i * 10 + 1, tagKey: g.key, expandable: g.expandable,
                tip: `${filenameA} · ${g.label} — unmatched\n${fmt(g.uniqA)} of ${fmt(totalA)} ${suffix}\n${stat}`,
            });
            links.push({ source: n.index, target: mid.index, value: g.uniqA });
        }
        if (g.uniqB > 0) {
            const mid = addNode(`fsk_ub_${i}`, `${g.label} only in ${filenameB} (${fmt(g.uniqB)} ${suffix})`, unmatchedColor, {
                align: COL_B - 1, tagIdx: i, sort: i * 10 + 2, tip: `${g.label}\nUnique to ${filenameB}: ${fmt(g.uniqB)} ${suffix}`,
            });
            const n = addNode(`fsk_bu_${i}`, `${g.label}${marker} unmatched (${fmt(g.uniqB)} ${suffix})`, tagColor, {
                align: COL_B, tagIdx: i, sort: i * 10 + 2, tagKey: g.key, expandable: g.expandable,
                tip: `${filenameB} · ${g.label} — unmatched\n${fmt(g.uniqB)} of ${fmt(totalB)} ${suffix}\n${stat}`,
            });
            links.push({ source: mid.index, target: n.index, value: g.uniqB });
        }
    });

    if (!links.length) {
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">Not enough data for graph</div>';
        return;
    }

    // Every column now carries up to 2-3 nodes per tag, so the height budget comes
    // from the fullest column, not from the tag count.
    const perColumn = COLUMNS.map(a => nodes.filter(n => n.align === a).length);
    const maxNodesInColumn = Math.max(...perColumn, 6);
    const width = container.clientWidth || 800;
    const padding = maxNodesInColumn > 30 ? 3 : 10;
    const height = Math.max(container.clientHeight || 400, maxNodesInColumn * (padding + 12) + 40);

    const svg = d3.select(container).append('svg').attr('width', width).attr('height', height);
    const zoomG = svg.append('g');

    const gap = Math.max(3, Math.min(padding, Math.floor((height - 40) / (maxNodesInColumn + 1))));
    const sankey = d3.sankey()
        .nodeWidth(15)
        .nodePadding(gap)
        .nodeAlign(n => n.align)
        // Tag order first, then shared / unique-A / unique-B within a tag, in every
        // column -- so a tag's three rows sit at the same height on both sides.
        .nodeSort((a, b) => (a.sort || 0) - (b.sort || 0))
        .extent([[25, 10], [width - 25, height - 10]]);

    let graph;
    try {
        graph = sankey({
            nodes: nodes.map(d => Object.assign({}, d)),
            links: links.map(d => Object.assign({}, d)),
        });
    } catch (e) {
        console.error('file sim sankey layout failed', e);
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--danger);">Graph layout error</div>';
        return;
    }

    // d3-sankey pads every node equally, which makes a tag's shared and unmatched
    // rows look like two unrelated tags. Re-stack each column with no gap inside a
    // tag and the full gap between tags: one visually contiguous block per tag,
    // still split by category. Heights are untouched, and every link spans a whole
    // node face, so nothing needs re-linking. Dropping gaps only shrinks a column,
    // so this can never overflow the extent.
    COLUMNS.forEach(col => {
        const colNodes = graph.nodes.filter(n => n.align === col).sort((a, b) => a.y0 - b.y0);
        let y = 10;
        colNodes.forEach((n, k) => {
            if (k > 0 && n.tagIdx !== colNodes[k - 1].tagIdx) y += gap;
            const h = n.y1 - n.y0;
            n.y0 = y;
            n.y1 = y + h;
            y += h;
        });
    });

    // A link between two differently coloured nodes is a crossing between two
    // axes, so it fades from one tag's colour to the other's rather than picking
    // a side. Same colour at both ends needs no gradient.
    const defs = svg.append('defs');
    const linkFill = (d, i) => {
        const from = d.source.color || 'var(--text)';
        const to = d.target.color || 'var(--text)';
        if (from === to) return from;
        const id = `fsk-grad-${i}`;
        const grad = defs.append('linearGradient')
            .attr('id', id)
            .attr('gradientUnits', 'userSpaceOnUse')
            .attr('x1', d.source.x1).attr('x2', d.target.x0);
        grad.append('stop').attr('offset', '0%').attr('stop-color', from);
        grad.append('stop').attr('offset', '100%').attr('stop-color', to);
        return `url(#${id})`;
    };

    zoomG.append('g').selectAll('path')
        .data(graph.links)
        .enter().append('path')
        .attr('d', d => {
            const x0 = d.source.x1, x1 = d.target.x0;
            const x2 = x0 + (x1 - x0) * 0.4, x3 = x0 + (x1 - x0) * 0.6;
            return `M ${x0},${d.source.y0}
                    C ${x2},${d.source.y0} ${x3},${d.target.y0} ${x1},${d.target.y0}
                    L ${x1},${d.target.y1}
                    C ${x3},${d.target.y1} ${x2},${d.source.y1} ${x0},${d.source.y1}
                    Z`;
        })
        .attr('fill', linkFill)
        .style('fill-opacity', 0.4)
        .on('mouseenter', function () { d3.select(this).style('fill-opacity', 0.75); })
        .on('mouseleave', function () { d3.select(this).style('fill-opacity', 0.4); })
        .append('title')
        .text(d => `${d.source.name}\n  ↓\n${d.target.name}\n${fmt(d.value)} ${suffix}`);

    const node = zoomG.append('g').selectAll('.node')
        .data(graph.nodes)
        .enter().append('g')
        .attr('class', 'node')
        .attr('transform', d => `translate(${d.x0},${d.y0})`);

    node.append('rect')
        .attr('height', d => Math.max(1, d.y1 - d.y0))
        .attr('width', sankey.nodeWidth())
        .attr('fill', d => d.color)
        .attr('stroke', 'var(--border)')
        .attr('stroke-width', '0.5px')
        .attr('opacity', 0.6)
        .style('cursor', d => (d.tagKey ? 'pointer' : 'default'))
        .on('click', (event, d) => { if (d.tagKey) window.toggleFileSimNs(d.tagKey, d.expandable, event.shiftKey); })
        .append('title')
        .text(d => d.tip || d.name);

    node.append('text')
        .attr('x', d => (d.align === 2 ? -6 : 6 + sankey.nodeWidth()))
        .attr('y', d => (d.y1 - d.y0) / 2)
        .attr('dy', '0.35em')
        .attr('text-anchor', d => (d.align === 2 ? 'end' : 'start'))
        .text(d => d.name)
        .attr('fill', 'var(--text)')
        .attr('font-size', '9px')
        .attr('opacity', 0.75)
        .attr('font-family', 'sans-serif')
        .style('cursor', d => (d.tagKey ? 'pointer' : 'default'))
        .on('click', (event, d) => { if (d.tagKey) window.toggleFileSimNs(d.tagKey, d.expandable, event.shiftKey); });
}

window.setFileSimScale = function(scale) {
    fileSimScale = scale;
    ['count', 'features'].forEach(s => {
        const b = document.getElementById(`bsim-filesim-scale-btn-${s}`);
        if (b) b.classList.toggle('active', scale === s);
    });
    if (binSimDataCache) renderFileSimSankey(binSimDataCache);
};

// Switch what the graph is grouped by. Pass null for the side you are not
// changing. Every mode is a marginal of the one stored joint table, so this is
// a pure re-render -- no fetch, no resplit.
window.setFileSimAxis = function(a, b) {
    const prev = fileSimAxisKey();
    if (a !== null && a !== undefined) fileSimAxisA = a;
    if (b !== null && b !== undefined) fileSimAxisB = b;
    // Crossing an axis with itself has no meaning; read it as "single axis".
    if (fileSimAxisB === fileSimAxisA) fileSimAxisB = '';
    // Node ids are tag ids, so a scope taken on the old axis means nothing on
    // the new one: switching the tree's axis starts again at its root.
    if (fileSimAxisKey() !== prev) {
        fileSimSelection = new Set();
        fileSimTreeOpen = fileSimDefaultOpen();
        fileSimOpenFolds.clear();
        fileSimRows = {};
        fileSimFoldRows = {};
    }
    const selB = document.getElementById('bsim-filesim-axis-b');
    if (selB && selB.value !== fileSimAxisB) selB.value = fileSimAxisB;
    if (!binSimDataCache) return;
    renderFileSim(binSimDataCache);
    // The Summary tab draws the flow itself; the other tabs need it drawn once
    // more so a hidden-then-shown panel is never a stale axis.
    if (fileSimTab !== 'summary') renderFileSimSankey(binSimDataCache);
};

// The Namespace / Library / Version depth buttons are gone: depth is now the
// tree's folding, and Expand all / Collapse all cover what the presets did.

// There is one table now, so sorting always means the same thing. The header is
// rebuilt in place rather than re-rendered so the filter inputs keep focus.
function setBinSimSort(table, col) {
    const st = binSimSortState.matched;
    if (st.col === col) st.dir *= -1;
    else { st.col = col; st.dir = -1; }
    const tbody = document.getElementById('bin-sim-table-matched');
    const thead = tbody && tbody.previousElementSibling;
    if (thead) { thead.innerHTML = fileSimTableHeadHtml(); restoreFileSimFilters(); }
    reloadFileSimRows();
}

// Per-column filters refine the current tab; the chip bar carries the scope.
// Values are stashed so a re-render of the header does not lose what was typed.
function binSimFilterChange(shouldApply = false) {
    ['q', 'cl-q', 'coh-min', 'coh-max', 'feat-min', 'feat-max', 'rar-min', 'rar-max', 'note-a', 'note-b'].forEach(k => {
        const el = document.getElementById(`bsim-flt-matched-${k}`);
        if (el) window[`bsim-flt-matched-${k}-val`] = el.value;
    });
    readFileSimSimTags();
    if (window._binSimFilterTimer) clearTimeout(window._binSimFilterTimer);
    window._binSimFilterTimer = setTimeout(reloadFileSimRows, shouldApply ? 0 : 300);
}

function restoreFileSimFilters() {
    ['q', 'cl-q', 'coh-min', 'coh-max', 'feat-min', 'feat-max', 'rar-min', 'rar-max', 'note-a', 'note-b'].forEach(k => {
        const el = document.getElementById(`bsim-flt-matched-${k}`);
        const v = window[`bsim-flt-matched-${k}-val`];
        if (el && v) el.value = v;
    });
    fileSimSimTags.forEach(t => createTagCard('bsim-sim', 'sim_tag', t.value, t.exclude, false));
}

// Tree folding is untouched -- only the rows are stale. Dropping the page state
// makes every open node re-fetch on the next render.
function dropFileSimRowCache() {
    fileSimRows = {};
    fileSimFoldRows = {};
    fileSimOpenFolds.clear();
}

// Any change to sort or filters invalidates every already-fetched page: rows
// that were in a group may no longer belong there.
function reloadFileSimRows() {
    dropFileSimRowCache();
    renderFileSimRows();
}

function buildFuncObj(fid) {
    const parts = fid.split(':');
    const entry = parts.pop();
    const md5 = parts.pop();
    parts.pop(); // type segment (func/function/idx marker)
    const col = parts.join(':');

    const meta = (binSimDataCache && binSimDataCache.functions_metadata)
        ? binSimDataCache.functions_metadata[fid] : null;
    const params = meta && meta.parameters
        ? (Array.isArray(meta.parameters) ? meta.parameters : [meta.parameters]) : [];
    return {
        function_id: fid,
        function_name: (meta && meta.name) ? meta.name : ('sub_' + entry),
        return_type: (meta && meta.return_type) ? meta.return_type : 'void',
        parameters: params,
        namespace: (meta && meta.namespace) ? meta.namespace : '',
        entrypoint_address: (meta && meta.entrypoint_address) ? meta.entrypoint_address : entry,
        bsim_features_count: (meta && meta.bsim_features_count) ? meta.bsim_features_count : 0,
        file_md5: md5,
        collection: col,
        tags: (meta && meta.tags) || [],
        user_tags: (meta && meta.user_tags) || [],
        note_owners: (meta && meta.note_owners) || [],
        note_count: (meta && meta.note_count) || 0,
    };
}

function renderFuncBadge(fid) {
    const f = buildFuncObj(fid);
    const sig = (typeof EntityRenderer !== 'undefined')
        ? EntityRenderer.renderFunction(f, { isTable: true, hideNote: true, showActions: false })
        : (f.function_name || fid);
    const tagsHtml = (typeof EntityRenderer !== 'undefined')
        ? EntityRenderer.renderTag('function', fid, f.tags, f.user_tags, { maxTags: 4 }) : '';
    return `
        <div class="bsim-func-cell" style="display:flex; flex-direction:column; gap:2px; min-width:0; text-align:left; width:100%;">
            ${sig}
            <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                <span class="mono dim" style="font-size:0.65rem;">@ ${f.entrypoint_address}</span>
                ${tagsHtml}
            </div>
        </div>`;
}

// Feature counts come from the per-function metadata when it is on the page, so a
// match shows both sides; `avg_features` (what the sort and the min/max filter run
// on) is the fallback and the tooltip.
function fileSimFeatCell(m, fA, fB) {
    const avg = Math.round(m.avg_features || 0);
    const one = (f) => (f && f.bsim_features_count) || 0;
    let text = String(avg);
    if (fA && fB) text = `${one(fA)} / ${one(fB)}`;
    else if (fA || fB) text = String(one(fA || fB) || avg);
    return `<span class="mono dim" style="font-size:0.72rem;" title="avg ${avg} BSim features">${text}</span>`;
}

// The cluster the row's function(s) belong to, as the same card the cluster views
// use. Samples ride on the row so the tooltip works for cross-collection and pool
// diffs, where fetching members by collection would fail.
function fileSimClusterCell(m) {
    if (!m.cluster_uuid && !m.cluster_name) return '';
    if (window.clusterTooltipMockCache && m.cluster_uuid && (m.cluster_sample_functions || []).length) {
        window.clusterTooltipMockCache.set(m.cluster_uuid, { data: {
            uuid: m.cluster_uuid, name: m.cluster_name,
            size: Number(m.cluster_member_count || 0), stability: Number(m.cluster_stability || 0),
            cohesion: Number(m.cohesion || 0), avg_features: Number(m.cluster_avg_features || 0),
            runtime_members: m.cluster_sample_functions, scrollOffset: 0
        }});
    }
    const card = (typeof renderClusterCards === 'function') ? renderClusterCards([{
        cluster_id: m.cluster_id,
        cluster_uuid: m.cluster_uuid,
        cluster_name: m.cluster_name,
        cohesion_score: m.cohesion || 0,
        member_count: m.cluster_member_count || 0,
        cluster_stability: m.cluster_stability || 0,
        avg_features: m.cluster_avg_features || 0,
    }]) : '';
    // Below the cohesion threshold the card renderer draws nothing, but the row is
    // still in that cluster and the filter still matches it, so name it plainly.
    return card || `<span class="dim" style="font-size:0.68rem;">${escapeHtml(m.cluster_name || '')}</span>`;
}

function renderMatchedFunctionRow(m, type, depth, extraHtml = '') {
    const noteBtn = (fid) => {
        const fObj = buildFuncObj(fid);
        return `<div style="min-height:24px; display:flex; align-items:center; justify-content:center;">${typeof EntityRenderer !== 'undefined' ? EntityRenderer.renderNoteButton(fid, fObj.note_owners, { isTable: true, raw_data: fObj }) : ''}</div>`;
    };

    let similarityHtml = '';
    let fA = null, fB = null;
    let col2 = '', col3 = '', col4 = '', col5 = '';

    if (type === 'matched' && m.func_a && m.func_b) {
        fA = buildFuncObj(m.func_a);
        fB = buildFuncObj(m.func_b);
        let diffUrl = '';
        if (window.buildDiffUrl) {
            diffUrl = window.buildDiffUrl(fA.function_id, fB.function_id);
        } else {
            let poolId = null;
            if (window.getRoutingState && window.getRoutingState().pool) {
                poolId = window.getRoutingState().pool;
            } else {
                poolId = new URLSearchParams(window.location.search).get('pool_id');
            }
            diffUrl = `/collections/${encodeURIComponent(fA.collection)}/files/${fA.file_md5}/functions/${fA.entrypoint_address}/vs/${encodeURIComponent(fB.collection)}/${fB.file_md5}/${fB.entrypoint_address}`;
            if (poolId) {
                diffUrl = `/pools/${encodeURIComponent(poolId)}` + diffUrl;
            }
        }

        // The row IS a function-similarity pair, so it carries that pair's tags —
        // the same editor, on the same entity, as the function-similarity view.
        const simTags = (typeof EntityRenderer !== 'undefined' && m.sid)
            ? EntityRenderer.renderTag('similarity', m.sid, m.tags || [], m.user_tags || [], { maxTags: 4 })
            : '';
        similarityHtml = `
            <div style="display:flex; align-items:center; gap:8px;">
                <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;"
                    onmouseenter="showDiffPreview(${escapeAttr(jsString(fA.function_id))}, ${escapeAttr(jsString(fA.function_name || ''))}, ${escapeAttr(jsString(fB.function_id))}, ${escapeAttr(jsString(fB.function_name || ''))}, ${Number(m.similarity) || 0}, event)"
                    onmousemove="moveCodePreview(event)"
                    onmouseleave="hideDiffPreview(event)"
                    onclick="Nav.openPath(${escapeAttr(jsString(diffUrl))}, event, { title: ${escapeAttr(jsString(`Diff: ${fA.function_name} vs ${fB.function_name}`))}, type: 'diff' })"
                    title="Run Aligned Diff">${(m.similarity * 100).toFixed(1)}%</div>
                ${extraHtml}
            </div>
            ${simTags}`;
        col2 = renderFuncBadge(m.func_a);
        col3 = noteBtn(m.func_a);
        col4 = renderFuncBadge(m.func_b);
        col5 = noteBtn(m.func_b);
    } else if (type === 'uniqueA') {
        if (m.func_id) {
            fA = buildFuncObj(m.func_id);
            col2 = renderFuncBadge(m.func_id);
            col3 = noteBtn(m.func_id);
        }
    } else if (type === 'uniqueB') {
        if (m.func_id) {
            fB = buildFuncObj(m.func_id);
            col4 = renderFuncBadge(m.func_id);
            col5 = noteBtn(m.func_id);
        }
    }

    // data-id is what the selection re-finds a focused row by after a re-render,
    // and what bulk actions resolve the row to: the pair for a match, the lone
    // function otherwise.
    const rowId = (type === 'matched') ? (m.sid || '') : (m.func_id || '');
    const ctxData = (type === 'matched' && m.sid)
        ? { sid: m.sid, id1: m.func_a, id2: m.func_b, score: m.similarity }
        : null;
    const ctxAttr = ctxData
        ? ` data-entity-data='${escapeAttr(JSON.stringify(ctxData))}' oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'similarity', this)"`
        : '';
    const simTd = `<td style="padding:10px; padding-left:${12 + depth * 22}px;">${similarityHtml}</td>`;
    const featTd = `<td style="padding:8px; text-align:center; vertical-align:top;">${fileSimFeatCell(m, fA, fB)}</td>`;
    const clusterTd = `<td style="padding:6px; text-align:center; vertical-align:top;">${fileSimClusterCell(m)}</td>`;
    const aTd = `<td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">${col2}</td>`;
    const aNoteTd = `<td style="padding:4px; vertical-align:top;">${col3}</td>`;
    const bTd = `<td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">${col4}</td>`;
    const bNoteTd = `<td style="padding:4px; vertical-align:top;">${col5}</td>`;

    // Column count follows the TABLE's mode, not this row's type: the
    // Unique-A/B tabs are 100% one-sided so they drop to a 4-col layout, but
    // All/Matched/Unmatched are 7-col tables that can still mix matched and
    // one-sided rows (e.g. All shows everything, Unmatched shows both
    // uniqueA and uniqueB rows) — those must always emit all 7 cells so the
    // header and body stay aligned regardless of any individual row's type.
    const mode = fileSimColMode();
    let cells;
    if (mode === 'a') {
        cells = [featTd, clusterTd, aTd, aNoteTd];
        // Indent lives on the (now-first) Features cell for one-sided rows.
        cells[0] = `<td style="padding:8px; padding-left:${12 + depth * 22}px; text-align:center; vertical-align:top;">${fileSimFeatCell(m, fA, fB)}</td>`;
    } else if (mode === 'b') {
        cells = [featTd, clusterTd, bTd, bNoteTd];
        cells[0] = `<td style="padding:8px; padding-left:${12 + depth * 22}px; text-align:center; vertical-align:top;">${fileSimFeatCell(m, fA, fB)}</td>`;
    } else {
        cells = [simTd, featTd, clusterTd, aTd, aNoteTd, bTd, bNoteTd];
    }
    return `
        <tr style="border-bottom: 1px solid var(--border); background: var(--bg);" data-id="${escapeAttr(rowId)}"${ctxAttr}>
            ${cells.join('')}
        </tr>`;
}

// The similarity tag filter is the same tag-card widget the other views use. The
// cards live in the table header, which is rebuilt whenever the sort changes, so
// what they hold is mirrored here and put back afterwards.
let fileSimSimTags = [];

function readFileSimSimTags() {
    const c = document.getElementById('tag-container-bsim-sim');
    if (c) {
        fileSimSimTags = Array.from(c.querySelectorAll('.tag-filter-card')).map(el => ({
            value: el.dataset.value,
            exclude: el.dataset.exclude === 'true',
        }));
    }
    return fileSimSimTags;
}

window.binSimSimTagAdd = function(event) {
    if (event.key !== 'Enter' && event.key !== ',') return;
    event.preventDefault();
    const val = event.target.value.replace(',', '').trim();
    if (!val) return;
    createTagCard('bsim-sim', 'sim_tag', val, false, false);
    event.target.value = '';
    binSimFilterChange(true);
};

function binSimFilterParams(prefix) {
    const val = (id) => (document.getElementById(id)?.value || '').trim();
    const p = {};
    const q = val(`bsim-flt-${prefix}-q`);
    if (q) p.q = q;
    // Cluster membership is a row column on every state, so its filter is too.
    const clq = val(`bsim-flt-${prefix}-cl-q`);
    if (clq) p.cl_q = clq;
    if (prefix === 'matched') {
        const simTags = readFileSimSimTags();
        const inc = simTags.filter(t => !t.exclude).map(t => t.value);
        const exc = simTags.filter(t => t.exclude).map(t => t.value);
        if (inc.length) p.sim_tags = inc.join(',');
        if (exc.length) p.sim_tags_not = exc.join(',');
        const na = val('bsim-flt-matched-note-a'); if (na) p.note_a = na;
        const nb = val('bsim-flt-matched-note-b'); if (nb) p.note_b = nb;
        const smin = val('bsim-flt-matched-coh-min'); if (smin) p.sim_min = smin;
        const smax = val('bsim-flt-matched-coh-max'); if (smax) p.sim_max = smax;
    } else {
        const n = val(`bsim-flt-${prefix}-note`); if (n) p.note = n;
    }
    const fmin = val(`bsim-flt-${prefix}-feat-min`); if (fmin) p.feat_min = fmin;
    const fmax = val(`bsim-flt-${prefix}-feat-max`); if (fmax) p.feat_max = fmax;
    const rmin = val(`bsim-flt-${prefix}-rar-min`); if (rmin) p.rar_min = rmin;
    const rmax = val(`bsim-flt-${prefix}-rar-max`); if (rmax) p.rar_max = rmax;
    return p;
}

function applyBinSimSearch() {
    if (window.filterDebounceTimer) clearTimeout(window.filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

    const inputs = {
        'q': 'bsim-search-input',
        'sort': 'bsim-score-type',
        'min_score': 'bsim-min-score',
        'max_score': 'bsim-max-score',
        'file_name': 'bsim-file-name',
        'md5': 'bsim-md5',
        'arch': 'bsim-arch',
        'containers': 'bsim-containers',
        'min_funcs': 'bsim-min-funcs',
        'max_funcs': 'bsim-max-funcs',
        'min_coverage': 'bsim-min-cov',
        'max_coverage': 'bsim-max-cov',
        'min_shared': 'bsim-min-shared'
    };

    for (const [paramKey, elemId] of Object.entries(inputs)) {
        const val = document.getElementById(elemId)?.value;
        if (val) params.set(paramKey, val);
        else params.delete(paramKey);
    }

    const countLimit = document.getElementById('sim-limit')?.value;
    if (countLimit) params.set('limit', countLimit);

    const tagCols = ['bin-sim'];
    const allTagKeys = ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'];
    allTagKeys.forEach(k => params.delete(k));

    tagCols.forEach(colId => {
        const container = document.getElementById(`tag-container-${colId}`);
        if (!container) return;
        const cards = container.querySelectorAll('.tag-filter-card');
        cards.forEach(card => {
            const type = card.dataset.type;
            const val = card.dataset.value;
            const isEx = card.dataset.exclude === 'true';
            const key = (isEx ? 'exclude_' : '') + type;
            // Quote unless the user hand-typed a wildcard (see quoteFilterValue).
            params.append(key, quoteFilterValue(val, card.dataset.literal !== 'false'));
        });
    });

    if (typeof navigate === 'function') {
        navigate(viewKey, params);
    } else {
        const newUrl = window.location.pathname + '?' + params.toString();
        history.pushState(null, '', newUrl);
        if (window.refreshData) window.refreshData();
    }
}

function binSimBytes(n) {
    if (!n) return '';
    const units = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

/** The comparison view for a container pair: the same coverage bar and child
 *  rows the grouped list uses. The function tag tree, the sankey and the
 *  matched/unmatched function tabs all describe functions, and a container has
 *  none of its own -- its evidence is one level down, in the child pairs, each
 *  of which links to its own real function-level diff. */
async function renderContainerPairView(data, collection, md5a, md5b, collB, poolId) {
    const el = document.getElementById('binary-similarity-container');
    if (!el) return;
    const nameA = data.file_metadata_a?.file_name || md5a;
    const nameB = data.file_metadata_b?.file_name || md5b;
    const activeScoreType = (new URLSearchParams(location.search)).get('sort') || 'score';

    const side = (name, md5, coll, cov, analyzed, unanalyzed, childCount, funcs) => `
        <div style="flex:1; min-width:0;">
            <div style="display:flex; align-items:center; gap:6px; overflow:hidden;">
                <i class="fa-solid fa-box-archive" style="color:var(--subtle);"></i>
                ${EntityRenderer.renderFileName(name, md5, coll)}
            </div>
            <div style="font-family:'Consolas',monospace; font-size:0.9rem; margin-top:6px;">${((cov || 0) * 100).toFixed(1)}% covered</div>
            ${binSimCovBar(cov, analyzed, unanalyzed)}
            <div class="dim" style="font-size:0.72rem; margin-top:4px;">
                ${childCount || 0} files · ${funcs || 0} functions${unanalyzed ? ` · ${binSimBytes(unanalyzed)} not analyzed` : ''}
            </div>
        </div>`;

    el.innerHTML = `
        <div style="flex:1; display:flex; flex-direction:column; padding:20px; min-height:0; overflow-y:auto;">
            <div style="border:1px solid var(--border); border-radius:8px; padding:18px 20px; margin-bottom:12px; display:flex; align-items:center; justify-content:center; gap:16px; background:var(--card-bg);">
                <span style="color:var(--subtle); text-transform:uppercase; font-size:0.8rem; font-weight:bold; letter-spacing:0.08em;">Container Similarity</span>
                ${binSimScoreCards(data, activeScoreType)}
                <span class="dim" style="font-size:0.72rem; max-width:280px;">rolled up from the files inside, weighted by function count</span>
            </div>
            <div style="display:flex; gap:20px; margin-bottom:14px;">
                ${side(nameA, md5a, collection, data.coverage_a, data.analyzed_bytes_a, data.unanalyzed_bytes_a, data.child_count_a, data.functions_count_a)}
                ${side(nameB, md5b, collB || collection, data.coverage_b, data.analyzed_bytes_b, data.unanalyzed_bytes_b, data.child_count_b, data.functions_count_b)}
            </div>
            <div class="resizable-card" style="border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; flex:1; min-height:200px; overflow:hidden;">
                <div style="flex:1; overflow:auto;">
                    <table style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                        <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                            <tr>
                                <th style="text-align:left; padding:6px 10px;">Score</th>
                                <th style="text-align:left; padding:6px 10px;">${escapeHtml(nameA)}</th>
                                <th style="text-align:left; padding:6px 10px;">${escapeHtml(nameB)}</th>
                                <th style="text-align:right; padding:6px 10px;">Functions</th>
                            </tr>
                        </thead>
                        <tbody id="container-pair-rows"><tr><td colspan="4" class="dim" style="padding:20px; text-align:center;">Loading files…</td></tr></tbody>
                    </table>
                </div>
            </div>
        </div>`;

    let url = `/api/diff?table=all&limit=500&sort_col=similarity&sort_dir=desc&collection_a=${encodeURIComponent(collection)}&md5_a=${encodeURIComponent(md5a)}&md5_b=${encodeURIComponent(md5b)}`;
    if (collB) url += `&collection_b=${encodeURIComponent(collB)}`;
    if (poolId) url += `&pool=${encodeURIComponent(poolId)}`;

    const body = document.getElementById('container-pair-rows');
    try {
        const res = await fetch(url);
        const page = await res.json();
        const items = page.items || [];
        if (!items.length) {
            body.innerHTML = `<tr><td colspan="4" class="dim" style="padding:20px; text-align:center;">No files to compare.</td></tr>`;
            return;
        }
        body.innerHTML = items.map(it => containerPairRow(it, collection, collB || collection, poolId)).join('');
    } catch (e) {
        body.innerHTML = `<tr><td colspan="4" style="padding:20px; text-align:center; color:var(--token-instruction);">Failed to load files: ${escapeHtml(String(e.message || e))}</td></tr>`;
    }
}

function containerPairRow(it, collA, collB, poolId) {
    const matched = it.state === 'matched';
    const label = (md5, name, path, coll) => {
        if (!md5) return '<span class="dim">—</span>';
        const shown = name || md5;
        const sub = path ? `<div class="dim" style="font-size:0.68rem;">${escapeHtml(path)}</div>` : '';
        return `<div>${EntityRenderer.renderFileName(shown, md5, coll)}${sub}</div>`;
    };

    if (!matched) {
        const onA = it.state === 'unique_to_a';
        return `<tr class="sim-row" style="opacity:0.55;">
            <td style="padding:6px 10px;" class="dim">no match</td>
            <td style="padding:6px 10px;">${onA ? label(it.md5, it.file_name, it.path_in_parent, collA) : '<span class="dim">—</span>'}</td>
            <td style="padding:6px 10px;">${onA ? '<span class="dim">—</span>' : label(it.md5, it.file_name, it.path_in_parent, collB)}</td>
            <td style="padding:6px 10px; text-align:right;" class="dim">${it.functions_count || 0}${it.bytes ? ` · ${binSimBytes(it.bytes)}` : ''}</td>
        </tr>`;
    }

    let diffUrl = `/collections/${collA}/files/${it.md5_a}/vs/${collB}/${it.md5_b}`;
    if (poolId) diffUrl = `/pools/${encodeURIComponent(poolId)}/collections/${collA}/files/${it.md5_a}/vs/${collB}/${it.md5_b}`;
    const title = `Bin Diff: ${it.file_name_a || it.md5_a} vs ${it.file_name_b || it.md5_b}`;
    const open = `Nav.openPath(${jsString(diffUrl)}, event, { title: ${jsString(title)}, type: 'bin_sim' });`;
    const pct = ((it.similarity || 0) * 100).toFixed(1) + '%';

    return `<tr class="sim-row">
        <td style="padding:6px 10px;">
            <span style="font-weight:bold; color:var(--success); cursor:pointer;" onclick="${escapeAttr(open)}" title="Open the diff for these two files">${pct}</span>
            ${binSimCovBar(it.coverage_a, 1, 0)}
        </td>
        <td style="padding:6px 10px;">${label(it.md5_a, it.file_name_a, it.path_in_parent_a, collA)}</td>
        <td style="padding:6px 10px;">${label(it.md5_b, it.file_name_b, it.path_in_parent_b, collB)}</td>
        <td style="padding:6px 10px; text-align:right;" class="dim">${it.functions_count_a || 0} / ${it.functions_count_b || 0}</td>
    </tr>`;
}

/** Matched / unmatched / unjudged mass of one side, as one small bar.
 *  Grey is the honest part: bytes with no functions in them (a `classes.dex`,
 *  resources) are invisible to a function-count score, so they are shown rather
 *  than folded into it. */
function binSimCovBar(coverage, analyzedBytes, unanalyzedBytes) {
    const cov = Math.max(0, Math.min(1, coverage || 0));
    const analyzed = Math.max(0, analyzedBytes || 0);
    const unanalyzed = Math.max(0, unanalyzedBytes || 0);
    const total = analyzed + unanalyzed;
    const codeShare = total > 0 ? analyzed / total : 1;
    const matched = cov * codeShare * 100;
    const unmatched = (1 - cov) * codeShare * 100;
    const grey = (1 - codeShare) * 100;
    const title = total > 0 && unanalyzed > 0
        ? `${(cov * 100).toFixed(1)}% of analyzed code matched · ${(grey).toFixed(0)}% of bytes hold no analyzed code`
        : `${(cov * 100).toFixed(1)}% of analyzed code matched`;
    return `<div title="${escapeAttr(title)}" style="display:flex; height:4px; width:100%; max-width:120px; border-radius:2px; overflow:hidden; background:var(--border); margin-top:3px;">
        <div style="width:${matched}%; background:var(--success);"></div>
        <div style="width:${unmatched}%; background:var(--border);"></div>
        <div style="width:${grey}%; background:var(--subtle); opacity:0.45;"></div>
    </div>`;
}

/** Main score card (current sort key) + small side cards for every other
 *  score type the item actually has data for, high -> low, click-to-promote.
 *  A type with a null/undefined field on this item gets no card -- a
 *  container-only Content score never shows a dead 0% on a file pair. */
function binSimScoreCards(item, activeScoreType) {
    const types = window.BinSimScoreTypes || {};
    const active = types[activeScoreType] ? activeScoreType : 'score';
    const mainMeta = types[active] || types.score;
    const mainVal = item[mainMeta.field];
    const mainPct = ((mainVal || 0) * 100).toFixed(1) + '%';

    const others = Object.keys(types)
        .filter(k => k !== active && item[types[k].field] != null)
        .sort((a, b) => (item[types[b].field] || 0) - (item[types[a].field] || 0));

    const promote = (type) =>
        `event.stopPropagation(); const sel = document.getElementById('bsim-score-type'); if (sel) { sel.value = ${jsString(type)}; applyBinSimSearch(); }`;

    const small = others.map(k => {
        const meta = types[k];
        const val = ((item[meta.field] || 0) * 100).toFixed(0) + '%';
        return `<div class="bsim-score-card" title="${escapeAttr(meta.label + ': click to make this the main score')}"
            onclick="${escapeAttr(promote(k))}"
            style="display:flex; align-items:center; gap:4px; font-size:0.7rem; color:${meta.color}; cursor:pointer; opacity:0.85; font-weight:600;">
            <i class="${meta.icon}"></i> <span>${meta.label}</span> <span>${val}</span>
        </div>`;
    }).join('');

    return `<div style="display:flex; flex-direction:column; gap:4px;">
        <div style="display:flex; align-items:center; gap:6px;">
            <i class="${mainMeta.icon}" style="color:${mainMeta.color}; font-size:0.9rem;" title="${escapeAttr(mainMeta.label)}"></i>
            <span style="font-size:0.8rem; font-weight:bold; color:var(--subtle); text-transform:uppercase;">${mainMeta.label}</span>
            <span style="font-size:1.1rem; font-weight:bold; color:${mainMeta.color};">${mainPct}</span>
        </div>
        ${small ? `<div style="display:flex; gap:10px; flex-wrap:wrap;">${small}</div>` : ''}
    </div>`;
}

/** @param depth 0 for a normal row; deeper rows are the children folded under a
 *  container row, hidden until its caret is opened. */
function renderBinSimPairs(items, depth = 0) {
    if (!items || items.length === 0) return '';
    let html = '';
    const { collection, params } = getRoutingState();

    items.forEach(item => {
        const kids = Array.isArray(item.children) ? item.children : [];
        // Reuse the lineage tree's row mechanics rather than a second one: it
        // hides and shows by data-depth, which is all a folded row needs.
        const rowAttrs = `data-depth="${depth}"${depth > 0 ? ' style="display:none;"' : ''}`;
        const caret = kids.length
            ? `<span class="bsim-caret-btn" style="display:inline-block; width:14px; cursor:pointer; color:var(--subtle);" onclick="event.stopPropagation(); Lineage.toggleTreeRow(this.closest('tr'));">▶</span>`
            : (depth > 0 ? `<span style="display:inline-block; width:${14 + (depth - 1) * 12}px;"></span>` : '');
        const activeScoreType = params.get('sort') || 'score';
        const archA = item.architecture_a || '---';
        const archB = item.architecture_b || '---';
        const funcsA = item.functions_count_a || 0;
        const funcsB = item.functions_count_b || 0;
        const covA = (item.coverage_a || 0).toFixed(4);
        const covB = (item.coverage_b || 0).toFixed(4);
        const shared = item.shared_clusters || 0;
        
        let tagsA = Array.isArray(item.file_tags_a) ? item.file_tags_a : [];
        let userTagsA = Array.isArray(item.file_user_tags_a) ? item.file_user_tags_a : [];
        
        let tagsB = Array.isArray(item.file_tags_b) ? item.file_tags_b : [];
        let userTagsB = Array.isArray(item.file_user_tags_b) ? item.file_user_tags_b : [];
        
        const collA = item.coll_a || collection;
        const collB = item.coll_b || collA;
        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        let diffUrl = `/collections/${collA}/files/${item.md5_a}/vs/${collB}/${item.md5_b}`;
        if (poolId) diffUrl = `/pools/${encodeURIComponent(poolId)}/collections/${collA}/files/${item.md5_a}/vs/${collB}/${item.md5_b}`;

        const diffTitle = `Bin Diff: ${item.file_name_a || 'Unknown'} vs ${item.file_name_b || 'Unknown'}`;
        const onClickHandler = `Nav.openPath(${jsString(diffUrl)}, event, { title: ${jsString(diffTitle)}, type: 'bin_sim' });`;

        html += `
            <tr class="sim-row" ${rowAttrs}>
                <td>
                    <div style="display:flex; align-items:center; gap:8px; padding-left:${depth * 14}px;">
                        ${caret}
                        <div style="cursor:pointer;" onclick="${escapeAttr(onClickHandler)}" title="Open Diff">${binSimScoreCards(item, activeScoreType)}</div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="display:flex; align-items:center; gap:6px; overflow:hidden; min-height:24px;" title="${item.file_name_a || ''}">
                            ${item.is_container_pair ? '<i class="fa-solid fa-box-archive" style="color:var(--subtle); font-size:0.75rem;" title="Container pair: rolled up from the files inside"></i>' : ''}
                            ${EntityRenderer.renderFileName(item.file_name_a, item.md5_a, collA)}
                        </div>
                        <div style="display:flex; align-items:center; gap:6px; overflow:hidden; min-height:24px;" title="${item.file_name_b || ''}">
                            ${item.is_container_pair ? '<i class="fa-solid fa-box-archive" style="color:var(--subtle); font-size:0.75rem;" title="Container pair: rolled up from the files inside"></i>' : ''}
                            ${EntityRenderer.renderFileName(item.file_name_b, item.md5_b, collB)}
                        </div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderMd5(item.md5_a)}</div>
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderMd5(item.md5_b)}</div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.75rem;">${archA}</span></div>
                        <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.75rem;">${archB}</span></div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.8rem;">${funcsA}</span></div>
                        <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.8rem;">${funcsB}</span></div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; flex-direction:column; justify-content:center;">${covA}${binSimCovBar(item.coverage_a, item.analyzed_bytes_a, item.unanalyzed_bytes_a)}</div>
                        <div style="min-height:24px; display:flex; flex-direction:column; justify-content:center;">${covB}${binSimCovBar(item.coverage_b, item.analyzed_bytes_b, item.unanalyzed_bytes_b)}</div>
                    </div>
                </td>
                <td class="sim-cell" style="vertical-align:middle;">
                    <div style="display:flex; align-items:center; justify-content:center; height:100%; font-weight:bold;">${shared}</div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collA}:file:${item.md5_a}`, tagsA, userTagsA, { maxTags: 4 })}</div>
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collB}:file:${item.md5_b}`, tagsB, userTagsB, { maxTags: 4 })}</div>
                    </div>
                </td>
                ${window.renderCollectionCell ? window.renderCollectionCell(collA, collB) : ''}
            </tr>
        `;

        // A child is the same kind of row one level down, so it draws itself.
        if (kids.length) html += renderBinSimPairs(kids, depth + 1);
    });
    return html;
}

window.applyBinSimSearch = applyBinSimSearch;
window.renderBinSimPairs = renderBinSimPairs;
window.binSimCovBar = binSimCovBar;
window.renderContainerPairView = renderContainerPairView;

// Expose showFunctionCodeById if not already defined, supporting iframe/standalone/Ctrl-click
if (typeof window.showFunctionCodeById === 'undefined') {
    window.showFunctionCodeById = function(id, name, lineHash = '', e) {
        if (window.parent && window.parent !== window && window.parent.showFunctionCodeById) {
            window.parent.showFunctionCodeById(id, name, lineHash, e);
        } else {
            if (window.getSelection && window.getSelection().toString().trim()) {
                return;
            }
            const parts = id.split(':');
            const col = parts[0] || '';
            const md5 = parts[2];
            const addr = parts[3];
            const url = `/collection/${encodeURIComponent(col)}/function/${encodeURIComponent(md5)}/${encodeURIComponent(addr)}${lineHash}`;
            
            const Nav = window.Nav || (window.parent && window.parent.Nav);
            if (Nav) {
                Nav.openPath(url, e, { title: `Code: ${name}`, type: 'code' });
            } else {
                window.open(url, '_blank');
            }
        }
    };
}

// Refresh logic for function rows on note updates
window.refreshFunctionRow = async function(funcId) {
    if (!binSimDataCache) return;
    try {
        const parts = funcId.split(':');
        const collection = parts[0];
        const md5 = parts[2];
        const addr = parts[3];
        const res = await fetch(`/api/function/search?collection=${collection}&entrypoint_address=${addr}&file_md5=${md5}`);
        const data = await res.json();
        if (data.functions && data.functions.length > 0) {
            const f = data.functions[0];
            if (!binSimDataCache.functions_metadata) {
                binSimDataCache.functions_metadata = {};
            }
            binSimDataCache.functions_metadata[funcId] = {
                ...(binSimDataCache.functions_metadata[funcId] || {}),
                note_owners: f.note_owners || [],
                note_count: f.note_count || 0
            };
            renderFileSimRows();
        }
    } catch (e) {
        console.error("Failed to refresh function note badge in comparison view:", e);
    }
};


// ---- Slim per-binary strip (user tags + notes only) ----
function renderBinSimStrip(containerId, m, fileId) {
    const el = document.getElementById(containerId);
    if (!el) return;
    m = m || {};
    const parts = fileId.split(':');
    const col = parts[0];
    const md5 = m.file_md5 || parts[parts.length - 1];
    const name = m.file_name || md5 || 'File';
    const fileUrl = Nav.buildUIUrl(col, ['file', md5]);
    const safeName = name.replace(/'/g, "\\'");

    // user tags only (pass empty static list)
    const tags = (typeof EntityRenderer !== 'undefined')
        ? EntityRenderer.renderTag('file', fileId, [], m.user_tags || [])
        : '';
    const noteBtn = (typeof EntityRenderer !== 'undefined')
        ? EntityRenderer.renderFileNoteButton(fileId, m.note_owners || [], { raw_data: m })
        : '';
    const filenameEl = (typeof EntityRenderer !== 'undefined') ? EntityRenderer.renderFileName(name, md5, col) : escapeHtml(name);
    el.innerHTML = `
        <span style="max-width:40%; display:inline-flex; min-width:0; align-items:center;" title="${escapeAttr(name)}">${filenameEl}</span>
        <span style="display:inline-flex; gap:4px; flex:1; min-width:0; flex-wrap:wrap;">${tags}</span>
        <span style="margin-left:auto;">${noteBtn}</span>
    `;
}

// ---- Tab switching: Summary / All / Matched / Unique to A / Unique to B / Metadata / Clusters ----
// Detail tabs (scoped by the tree) and sidebar pages (not scoped). Both live in
// the same hash so Back/forward restores either.
const BIN_SIM_DETAIL_TABS = ['summary', 'all', 'matched', 'unique_a', 'unique_b', 'unmatched'];
const BIN_SIM_NAV_PAGES = ['metadata', 'inferred'];
const BIN_SIM_TABS = BIN_SIM_DETAIL_TABS.concat(BIN_SIM_NAV_PAGES);

// Which DOM panel backs each tab. All / Matched / Unique to A / Unique to B share
// one panel -- they differ only by the state filter they send.
function binSimPanelFor(tab) {
    if (tab === 'summary') return 'summary';
    if (BIN_SIM_NAV_PAGES.includes(tab)) return tab;
    return 'table';
}

// push=true (a real click) writes the tab into the URL hash so it lands in
// browser history; Back/forward then fires hashchange and re-selects the tab.
window.switchBinSimTab = function(tab, push = true) {
    if (!BIN_SIM_TABS.includes(tab)) tab = 'summary';
    const isNav = BIN_SIM_NAV_PAGES.includes(tab);
    // Pages are cached per tree node, not per tab, and each tab sends a different
    // `state` filter -- so a tab change makes every fetched page stale. Without
    // this the table kept showing whatever the previous tab had loaded.
    if (!isNav) {
        if (tab !== fileSimTab) dropFileSimRowCache();
        fileSimTab = tab;
    }

    const shown = binSimPanelFor(tab);
    ['summary', 'table', 'filesim'].concat(BIN_SIM_NAV_PAGES).forEach(p => {
        const panel = document.getElementById(`bsim-panel-${p}`);
        if (panel) panel.style.display = (p === shown) ? 'flex' : 'none';
    });
    BIN_SIM_DETAIL_TABS.forEach(t => {
        const btn = document.getElementById(`bin-sim-tab-btn-${t}`);
        if (btn) btn.classList.toggle('active', !isNav && t === tab);
    });
    BIN_SIM_NAV_PAGES.forEach(p => {
        const item = document.getElementById(`bsim-nav-${p}`);
        if (item) item.classList.toggle('active', p === tab);
    });
    // The tree greys out on a non-scoping page rather than disappearing, so the
    // scope you were reading is still there when you come back.
    const sidebar = document.getElementById('bsim-sidebar');
    if (sidebar) sidebar.classList.toggle('nav-active', isNav);
    if ((tab === 'metadata' || tab === 'inferred') && binSimMetaCtx && !binSimMetaCtx.loaded) loadBinSimMetadata();
    if (!isNav && binSimDataCache) renderFileSim(binSimDataCache);

    if (push && location.hash.slice(1) !== tab) {
        // pushState (not location.hash=) so the app's hashchange ROUTER doesn't
        // re-render the whole view on every tab click. Adds a history entry;
        // Back/forward fires popstate+hashchange -> the view re-renders and
        // applyBinSimTabFromHash() restores the tab.
        history.pushState(null, '', location.pathname + location.search + '#' + tab);
    }
};

// Select the tab named in the URL hash (default summary). Called on initial
// render and on Back/forward navigation.
function applyBinSimTabFromHash() {
    const tab = location.hash.slice(1);
    window.switchBinSimTab(BIN_SIM_TABS.includes(tab) ? tab : 'summary', false);
}
window.applyBinSimTabFromHash = applyBinSimTabFromHash;

if (!window.__binSimHashBound) {
    window.addEventListener('hashchange', applyBinSimTabFromHash);
    window.__binSimHashBound = true;
}

// ---- Lazy full-metadata load for the Metadata tab ----
async function loadBinSimMetadata() {
    const ctx = binSimMetaCtx;
    if (!ctx) return;
    const target = document.getElementById('bin-sim-meta-compare');
    try {
        const mkParams = (col) => {
            let p = `collection=${encodeURIComponent(col)}`;
            if (ctx.poolId) p += `&pool=${encodeURIComponent(ctx.poolId)}`;
            return p;
        };
        const [ra, rb] = await Promise.all([
            fetch(`/api/file/details/${encodeURIComponent(ctx.md5a)}?${mkParams(ctx.collection)}`),
            fetch(`/api/file/details/${encodeURIComponent(ctx.md5b)}?${mkParams(ctx.collB)}`),
        ]);
        const da = await ra.json();
        const db = await rb.json();
        ctx.loaded = true;
        binSimMetaCache = { da, db };
        if (target) target.outerHTML = buildMetaCompareTable(da, db, ctx.collection, ctx.collB);
        const inferredTarget = document.getElementById('bin-sim-inferred-meta-container');
        if (inferredTarget) inferredTarget.innerHTML = buildInferredMetaCards(da, db, ctx.collection, ctx.collB);
    } catch (e) {
        console.error('Failed to load comparison metadata:', e);
        if (target) target.innerHTML = '<div style="color:var(--dim); padding:20px;">Failed to load metadata.</div>';
        const inferredTarget = document.getElementById('bin-sim-inferred-meta-container');
        if (inferredTarget) inferredTarget.innerHTML = '<div style="color:var(--dim); padding:20px;">Failed to load clusters.</div>';
    }
}

window.setMetaHighlightMode = function(mode) {
    metaHighlightMode = mode;
    ['different', 'similar', 'none'].forEach(m => {
        const btn = document.getElementById(`meta-highlight-${m}`);
        if (btn) btn.classList.toggle('active', m === mode);
    });
    if (binSimMetaCache && binSimMetaCtx) {
        const target = document.getElementById('bin-sim-meta-compare');
        if (target) {
            target.outerHTML = buildMetaCompareTable(
                binSimMetaCache.da,
                binSimMetaCache.db,
                binSimMetaCtx.collection,
                binSimMetaCtx.collB
            );
        }
    }
};

// ---- Categorized side-by-side compare table with diff highlighting ----
function buildMetaCompareTable(da, db, colA, colB) {
    const fa = (da && da.file) || {};
    const fb = (db && db.file) || {};
    const ia = (da && da.inferred_meta) || {};
    const ib = (db && db.inferred_meta) || {};

    const filenameA = fa.file_name || 'Binary A';
    const filenameB = fb.file_name || 'Binary B';

    const fmt = (v) => {
        if (v === undefined || v === null || v === '') return '<span style="color:var(--dim)">—</span>';
        if (Array.isArray(v)) return v.length ? v.join(', ') : '<span style="color:var(--dim)">—</span>';
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
        'BSim Features': 'fa-solid fa-dna',
        'First Seen': 'fa-solid fa-clock',
        'Related MD5s': 'fa-solid fa-link'
    };

    const categories = [
        ['Identity', [
            ['File Name', fa.file_name, fb.file_name],
            ['Other Names', fa.file_names, fb.file_names],
            ['MD5', fa.file_md5, fb.file_md5],
            ['Related MD5s', fa.related_md5, fb.related_md5],
            ['Batch UUID', fa.batch_uuid, fb.batch_uuid],
            ['First Seen', fa.first_seen ? fmtDate(fa.first_seen) : '', fb.first_seen ? fmtDate(fb.first_seen) : ''],
        ]],
        ['Classification', [
            ['Language', fa.language_id || fa.language, fb.language_id || fb.language],
            ['AV Type', fa.avtype, fb.avtype],
            ['File Type', fa.filetype, fb.filetype],
            ['Yara', fa.yara, fb.yara],
            ['CC IP', fa.cc_ip, fb.cc_ip],
        ]],
        ['Statistics', [
            ['Functions', fa.function_count, fb.function_count],
            ['BSim Features', fa.bsim_features_count, fb.bsim_features_count],
        ]]
    ];

    if ((fa.file_format && Object.keys(fa.file_format).length > 0) || (fb.file_format && Object.keys(fb.file_format).length > 0)) {
        const allKeys = new Set([...Object.keys(fa.file_format || {}), ...Object.keys(fb.file_format || {})]);
        const formatFields = Array.from(allKeys).map(k => [k, (fa.file_format || {})[k], (fb.file_format || {})[k]]);
        categories.push(['File Format', formatFields]);
    }

    const norm = (v) => Array.isArray(v) ? v.slice().sort().join('|') : (v === undefined || v === null ? '' : String(v));

    let rows = '';
    for (const [cat, fields] of categories) {
        rows += `<tr><td class="bin-sim-mc-cat" colspan="3">${cat}</td></tr>`;
        for (const [label, va, vb] of fields) {
            const diff = norm(va) !== norm(vb);
            const icon = iconMap[label] || 'fa-solid fa-circle-info';
            
            const isEmptyA = va === undefined || va === null || va === '' || (Array.isArray(va) && va.length === 0);
            const isEmptyB = vb === undefined || vb === null || vb === '' || (Array.isArray(vb) && vb.length === 0);

            let highlightClass = '';
            if (!isEmptyA || !isEmptyB) {
                if (metaHighlightMode === 'different' && diff) {
                    highlightClass = 'bin-sim-mc-diff';
                } else if (metaHighlightMode === 'similar' && !diff) {
                    highlightClass = 'bin-sim-mc-same';
                }
            }

            rows += `<tr class="${highlightClass}">
                <td class="bin-sim-mc-label" style="display: flex; align-items: center; gap: 8px;"><i class="${icon}" style="width: 14px; text-align: center; color: var(--dim); opacity: 0.8;"></i>${label}</td>
                <td>${fmt(va)}</td>
                <td>${fmt(vb)}</td>
            </tr>`;
        }
    }

    // Render Inferred Rows helper for the side-by-side cards
    const renderInferredRow = (icon, label, mapObj, collection) => {
        if (!mapObj) return '';
        const keys = Object.keys(mapObj).sort((a,b) => mapObj[b].percent - mapObj[a].percent);
        if (keys.length === 0) return '';
        const badges = keys.map(k => {
            const confObj = mapObj[k];
            const confScore = confObj.percent;
            const confColor = d3.interpolateRdYlGn(confScore / 100);
            const clusterLink = Nav.buildUIUrl(collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(confObj.cluster_uuid)}`;
            return `<a href="${clusterLink}" class="stat-badge" style="background: var(--hover); display: inline-flex; margin: 2px 4px 2px 0; text-decoration: none; transition: background 0.2s;" onclick="event.preventDefault(); Nav.openPath(${escapeAttr(jsString(clusterLink))}, event);"><span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace;">${escapeHtml(k)}</span> <span class="val" style="margin-left: 4px; color: ${confColor};">${confScore}%</span></a>`;
        }).join('');
        return `
            <div class="meta-label" style="align-items: flex-start; margin-top: 4px; color: var(--dim); text-transform: uppercase; font-size: 0.75rem; display: flex; gap: 6px;"><i class="${icon}" style="width:14px; text-align:center;"></i> ${label}</div>
            <div class="meta-value" style="display: flex; flex-wrap: wrap;">${badges}</div>
        `;
    };

    const buildInferredHtml = (inferredMeta, collection) => {
        let html = '';
        html += renderInferredRow('fa-solid fa-file', 'File Name', inferredMeta.filename || {}, collection);
        html += renderInferredRow('fa-solid fa-fingerprint', 'MD5', inferredMeta.md5 || {}, collection);
        html += renderInferredRow('fa-solid fa-shield', 'AV Type', inferredMeta.avtype || {}, collection);
        html += renderInferredRow('fa-solid fa-file-code', 'File Type', inferredMeta.filetype || {}, collection);
        html += renderInferredRow('fa-solid fa-biohazard', 'Yara', inferredMeta.yara || {}, collection);
        html += renderInferredRow('fa-solid fa-network-wired', 'CC IP', inferredMeta.ccip || {}, collection);
        return html || '<div class="dim" style="grid-column: 1 / -1; padding: 10px 0;">No clusters available.</div>';
    };

    const inferredHtmlA = buildInferredHtml(ia, colA);
    const inferredHtmlB = buildInferredHtml(ib, colB);

    return `<div id="bin-sim-meta-compare" style="display: flex; flex-direction: column; gap: 20px; width: 100%;">
        <table class="bin-sim-mc-table">
            <thead><tr><th></th><th>${filenameA}</th><th>${filenameB}</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>
    </div>`;
}

// ---- Side-by-side inferred metadata cards for the Inferred Metadata tab ----
function buildInferredMetaCards(da, db, colA, colB) {
    const fa = (da && da.file) || {};
    const fb = (db && db.file) || {};
    const ia = (da && da.inferred_meta) || {};
    const ib = (db && db.inferred_meta) || {};

    const filenameA = fa.file_name || 'Binary A';
    const filenameB = fb.file_name || 'Binary B';

    // Render Inferred Rows helper for the side-by-side cards
    const renderInferredRow = (icon, label, mapObj, collection) => {
        if (!mapObj) return '';
        const keys = Object.keys(mapObj).sort((a,b) => mapObj[b].percent - mapObj[a].percent);
        if (keys.length === 0) return '';
        const badges = keys.map(k => {
            const confObj = mapObj[k];
            const confScore = confObj.percent;
            const confColor = d3.interpolateRdYlGn(confScore / 100);
            const clusterLink = Nav.buildUIUrl(collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(confObj.cluster_uuid)}`;
            return `<a href="${clusterLink}" class="stat-badge" style="background: var(--hover); display: inline-flex; margin: 2px 4px 2px 0; text-decoration: none; transition: background 0.2s;" onclick="event.preventDefault(); Nav.openPath(${escapeAttr(jsString(clusterLink))}, event);"><span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace;">${escapeHtml(k)}</span> <span class="val" style="margin-left: 4px; color: ${confColor};">${confScore}%</span></a>`;
        }).join('');
        return `
            <div class="meta-label" style="align-items: flex-start; margin-top: 4px; color: var(--dim); text-transform: uppercase; font-size: 0.75rem; display: flex; gap: 6px;"><i class="${icon}" style="width:14px; text-align:center;"></i> ${label}</div>
            <div class="meta-value" style="display: flex; flex-wrap: wrap;">${badges}</div>
        `;
    };

    const buildInferredHtml = (inferredMeta, collection) => {
        let html = '';
        html += renderInferredRow('fa-solid fa-file', 'File Name', inferredMeta.filename || {}, collection);
        html += renderInferredRow('fa-solid fa-fingerprint', 'MD5', inferredMeta.md5 || {}, collection);
        html += renderInferredRow('fa-solid fa-shield', 'AV Type', inferredMeta.avtype || {}, collection);
        html += renderInferredRow('fa-solid fa-file-code', 'File Type', inferredMeta.filetype || {}, collection);
        html += renderInferredRow('fa-solid fa-biohazard', 'Yara', inferredMeta.yara || {}, collection);
        html += renderInferredRow('fa-solid fa-network-wired', 'CC IP', inferredMeta.ccip || {}, collection);
        return html || '<div class="dim" style="grid-column: 1 / -1; padding: 10px 0;">No clusters available.</div>';
    };

    const inferredHtmlA = buildInferredHtml(ia, colA);
    const inferredHtmlB = buildInferredHtml(ib, colB);

    return `<div style="display: flex; gap: 20px; flex-wrap: wrap; width: 100%;">
        <div class="card" style="flex: 1; min-width: 300px; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; ">
            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid var(--border); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px; font-family: sans-serif;">
                <i class="fa-solid fa-wand-magic-sparkles"></i> ${filenameA}: Clusters
            </div>
            <div class="meta-grid" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;">
                ${inferredHtmlA}
            </div>
        </div>
        <div class="card" style="flex: 1; min-width: 300px; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; ">
            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid var(--border); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px; font-family: sans-serif;">
                <i class="fa-solid fa-wand-magic-sparkles"></i> ${filenameB}: Clusters
            </div>
            <div class="meta-grid" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;">
                ${inferredHtmlB}
            </div>
        </div>
    </div>`;
}



