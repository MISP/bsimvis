// Binary Similarity View Logic

let binSimDataCache = null;
let binSimMetaCtx = null;
let binSimMetaCache = null;
let metaHighlightMode = 'different';
let sankeyMode = 'simplified';
let sankeyScale = 'count';
let sankeySplit = 10;
let binSimSortState = {
    matched: { col: 'similarity', dir: -1 },
    uniqueA: { col: 'cohesion', dir: -1 },
    uniqueB: { col: 'cohesion', dir: -1 }
};

// Change 4 (frontend): server-paged tables. Sankey uses the compact view; tables load
// pages on demand. binSimFullDiff is lazily fetched only for the detailed Sankey.
const BINSIM_LIMIT = 100;
let binSimCtx = null;        // {collection, md5a, md5b, collB, poolId}
let binSimFullDiff = null;   // full diff, fetched only when detailed Sankey is opened
let binSimPage = {
    // keyed by sort-state key; maps to backend table + tbody id + filter prefix
    matched: { table: 'matched', tbody: 'bin-sim-table-matched', prefix: 'matched', items: [], offset: 0, total: 0, loading: false },
    uniqueA: { table: 'unique_to_a', tbody: 'bin-sim-table-unique-a', prefix: 'ua', items: [], offset: 0, total: 0, loading: false },
    uniqueB: { table: 'unique_to_b', tbody: 'bin-sim-table-unique-b', prefix: 'ub', items: [], offset: 0, total: 0, loading: false },
};
function openClusterView(uuid, name, event) {
    const { collection: col } = getRoutingState();
    const url = Nav.buildUIUrl(col, ['search', 'functions']) + `?cluster_uuid=${encodeURIComponent(uuid)}`;
    Nav.openPath(url, event, { title: `Cluster: ${name}`, type: 'cluster' });
}
window.openClusterView = openClusterView;

function handleIframeMouseLeave(event) {
    const relatedTarget = event.relatedTarget;
    const isIframe = window.parent && (window.parent !== window) && window.parent.showClusterTableTooltipFromIframe;
    const parentWin = isIframe ? window.parent : window;
    const tooltip = parentWin.document.getElementById('hierarchy-tooltip');
    if (tooltip) {
        if (tooltip === relatedTarget || tooltip.contains(relatedTarget)) {
            return;
        }
        const rect = tooltip.getBoundingClientRect();
        const iframeRect = (isIframe && window.frameElement) ? window.frameElement.getBoundingClientRect() : { left: 0, top: 0 };
        const parentX = event.clientX + iframeRect.left;
        const parentY = event.clientY + iframeRect.top;
        if (parentX >= rect.left - 5 && parentX <= rect.right + 5 && parentY >= rect.top - 5 && parentY <= rect.bottom + 5) {
            return;
        }
    }
    if (isIframe) {
        window.parent.hideClusterTableTooltipFromIframe();
    } else if (window.hideClusterTableTooltip) {
        window.hideClusterTableTooltip();
    }
}

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

            <!-- Slim per-binary strip: user tags + notes only -->
            <div style="display: flex; gap: 20px; margin-bottom: 12px;">
                <div id="bin-sim-strip-a" class="bin-sim-strip" style="flex: 1; min-width: 0;"></div>
                <div id="bin-sim-strip-b" class="bin-sim-strip" style="flex: 1; min-width: 0;"></div>
            </div>

            <!-- Tab bar -->
            <div class="bsim-tabbar" id="bin-sim-tabs">
                <button class="bsim-tab active" id="bin-sim-tab-btn-matched" onclick="switchBinSimTab('matched')">Matched functions</button>
                <button class="bsim-tab" id="bin-sim-tab-btn-unmatched" onclick="switchBinSimTab('unmatched')">Unmatched functions</button>
                <button class="bsim-tab" id="bin-sim-tab-btn-graph" onclick="switchBinSimTab('graph')">Function graph</button>
                <button class="bsim-tab" id="bin-sim-tab-btn-metadata" onclick="switchBinSimTab('metadata')">Metadata</button>
                <button class="bsim-tab" id="bin-sim-tab-btn-inferred" onclick="switchBinSimTab('inferred')">Clusters</button>
                <button class="bsim-tab" id="bin-sim-tab-btn-filesim" onclick="switchBinSimTab('filesim')">File sim</button>
            </div>

            <!-- Matched functions tab -->
            <div class="bsim-subtab-panel" id="bsim-panel-matched" style="flex:1; min-height:0; display:flex; flex-direction:column;">
                <div class="resizable-card" style="border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; flex:1; min-height:200px; overflow:hidden;">
                    <div style="flex:1; overflow:auto;">
                        <table id="bin-sim-table-matched-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                            <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;"></thead>
                            <tbody id="bin-sim-table-matched"></tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- Unmatched functions sub-tab -->
            <div class="bsim-subtab-panel" id="bsim-panel-unmatched" style="flex:1; min-height:0; display:none; gap:20px;">
                <div style="flex:1; border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                    <div style="flex:1; overflow:auto;">
                        <table id="bin-sim-table-unique-a-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                            <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;"></thead>
                            <tbody id="bin-sim-table-unique-a"></tbody>
                        </table>
                    </div>
                </div>
                <div style="flex:1; border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                    <div style="flex:1; overflow:auto;">
                        <table id="bin-sim-table-unique-b-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                            <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;"></thead>
                            <tbody id="bin-sim-table-unique-b"></tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- Graph sub-tab -->
            <div class="bsim-subtab-panel" id="bsim-panel-graph" style="flex:1; min-height:0; display:none; flex-direction:column;">
                <div id="bin-sim-sankey-card" style="position:relative; width:100%; flex:1; min-height:200px; border:1px solid var(--border); background:var(--bg); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                    <div class="view-toggle" id="bin-sim-sankey-mode-toggle" style="position:absolute; top:15px; left:15px; z-index:10; margin:0; align-items:center;">
                        <button class="view-btn ${sankeyMode === 'detailed' ? 'active' : ''}" id="bsim-sankey-btn-detailed" onclick="setSankeyMode('detailed')" title="Show detailed function-level similarities">Detailed</button>
                        <button class="view-btn ${sankeyMode === 'simplified' ? 'active' : ''}" id="bsim-sankey-btn-simplified" onclick="setSankeyMode('simplified')" title="Show simplified cluster-level summary">Simplified</button>
                        <button class="view-btn ${sankeyMode === 'tags' ? 'active' : ''}" id="bsim-sankey-btn-tags" onclick="setSankeyMode('tags')" title="Split the match by library/bundle tag, crossed with similarity">Tags</button>
                    </div>
                    <div class="view-toggle" id="bin-sim-sankey-scale-toggle" style="position:absolute; top:15px; left:210px; z-index:10; margin:0; align-items:center; padding-left:10px;">
                        <span style="font-size:0.7rem; color:var(--subtle); margin-right:6px; font-weight:bold; font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;">Scale:</span>
                        <button class="view-btn ${sankeyScale === 'count' ? 'active' : ''}" id="bsim-sankey-scale-btn-count" onclick="setSankeyScale('count')" title="Scale flow by function count">Count</button>
                        <button class="view-btn ${sankeyScale === 'features' ? 'active' : ''}" id="bsim-sankey-scale-btn-features" onclick="setSankeyScale('features')" title="Scale flow by BSim feature count">Features</button>
                    </div>
                    <div class="view-toggle" id="bin-sim-sankey-split-toggle" style="position:absolute; top:15px; left:410px; z-index:10; margin:0; align-items:center; padding-left:10px; display: ${sankeyMode === 'detailed' ? 'none' : 'flex'};">
                        <span style="font-size:0.7rem; color:var(--subtle); margin-right:6px; font-weight:bold; font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;">Split:</span>
                        <button class="view-btn ${sankeySplit === 5 ? 'active' : ''}" onclick="setSankeySplit(5)" title="5% granularity (20 bins)">5%</button>
                        <button class="view-btn ${sankeySplit === 10 ? 'active' : ''}" onclick="setSankeySplit(10)" title="10% granularity (10 bins)">10%</button>
                        <button class="view-btn ${sankeySplit === 20 ? 'active' : ''}" onclick="setSankeySplit(20)" title="20% granularity (5 bins)">20%</button>
                        <button class="view-btn ${sankeySplit === 25 ? 'active' : ''}" onclick="setSankeySplit(25)" title="25% granularity (4 bins)">25%</button>
                    </div>
                    <div id="bin-sim-sankey" style="flex:1; width:100%; min-height:0; overflow-y:auto; position:relative;"></div>
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

            <!-- File sim tab: tag-composition similarity, collapsible by depth -->
            <div class="bsim-subtab-panel" id="bsim-panel-filesim" style="flex:1; min-height:0; display:none; flex-direction:column; overflow:auto; padding:5px 0 0 0; gap:10px;">
                <div id="bin-sim-filesim" style="color:var(--dim); text-align:center; padding:40px;">No tag data for this pair.</div>
            </div>
        </div>
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
            #bin-sim-table-matched-table td,
            #bin-sim-table-unique-a-table td,
            #bin-sim-table-unique-b-table td {
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
                
                if (card.id === 'bin-sim-sankey-card' && binSimDataCache) {
                    renderBinaryDiffSankey(binSimDataCache);
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
        
        window.filenameCache = window.filenameCache || {};
        if (data.file_metadata_a) window.filenameCache[md5a] = data.file_metadata_a.file_name || 'File';
        if (data.file_metadata_b) window.filenameCache[md5b] = data.file_metadata_b.file_name || 'File';

        const nameA = data.file_metadata_a?.file_name || 'Binary A';
        const nameB = data.file_metadata_b?.file_name || 'Binary B';


        Breadcrumbs.setFilename(md5a, data.file_metadata_a?.file_name || 'File');
        Breadcrumbs.setFilename(md5b, data.file_metadata_b?.file_name || 'File');
        Breadcrumbs.refresh();
        
        // Render Summary — prominent, score-colored
        const scoreVal = document.getElementById('bin-sim-score-val');
        if (scoreVal) {
            scoreVal.textContent = (data.score * 100).toFixed(1) + '%';
            scoreVal.style.color = 'var(--success)';
        }

        resultsEl.style.display = 'flex';

        // Slim per-binary strip: user tags + notes only
        renderBinSimStrip('bin-sim-strip-a', data.file_metadata_a, `${collection}:file:${md5a}`);
        renderBinSimStrip('bin-sim-strip-b', data.file_metadata_b, `${collB || collection}:file:${md5b}`);

        // Stash context for lazy Metadata tab load
        binSimMetaCtx = {
            collection, md5a, md5b, collB: collB || collection, poolId, loaded: false,
        };

        // Cache: compact summary + Sankey; tables load their rows via paging. diff{} is
        // filled incrementally per table page; functions_metadata merged across pages.
        binSimCtx = { collection, md5a, md5b, collB: collB || collection, poolId };
        binSimFullDiff = null;
        const counts = data.counts || { matched: 0, unique_to_a: 0, unique_to_b: 0 };
        binSimDataCache = {
            score: data.score,
            file_metadata_a: data.file_metadata_a,
            file_metadata_b: data.file_metadata_b,
            sankey: data.sankey || { matched: [], unique_to_a: [], unique_to_b: [] },
            tags_summary: data.tags_summary || [],
            counts,
            functions_metadata: {},
            diff: { matched: [], unique_to_a: [], unique_to_b: [] },
        };
        ['matched', 'uniqueA', 'uniqueB'].forEach(k => {
            binSimPage[k].items = []; binSimPage[k].offset = 0; binSimPage[k].total = 0; binSimPage[k].loading = false;
        });

        const btnMatched = document.getElementById('bin-sim-tab-btn-matched');
        const btnUnmatched = document.getElementById('bin-sim-tab-btn-unmatched');
        if (btnMatched) btnMatched.textContent = `Matched functions (${counts.matched})`;
        if (btnUnmatched) btnUnmatched.textContent = `Unmatched functions (${counts.unique_to_a} / ${counts.unique_to_b})`;

        // Render headers, the Sankey (from compact data), then load first page of each table.
        renderBinSimTables();
        renderBinaryDiffSankey(binSimDataCache);
        loadBinSimTablePage('matched', { reset: true });
        loadBinSimTablePage('uniqueA', { reset: true });
        loadBinSimTablePage('uniqueB', { reset: true });

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


// Display name for a tag row: version-qualified, e.g. "libc 2.31" / "mirai_core".
function tagLabel(t) {
    const name = t.name || t.tag_id || 'untagged';
    return t.version ? `${name} ${t.version}` : name;
}

// UX guardrail: a real corpus has a long tail of one-function libraries. Keep the
// tags that carry the match and roll the rest into a single "other" row.
function foldSmallTags(rows, keep = 12) {
    if (rows.length <= keep) return rows;
    const head = rows.slice(0, keep);
    const tail = rows.slice(keep);
    const other = { tag_id: 'other', name: `other (${tail.length} tags)`, version: '', bins: {} };
    ['matched_weight','matched_count','unique_weight_a','unique_weight_b','unique_count_a','unique_count_b','contribution_pct','coverage_pct_a','coverage_pct_b'].forEach(k => {
        other[k] = tail.reduce((s, r) => s + (r[k] || 0), 0);
    });
    other.score = other.matched_weight > 0
        ? tail.reduce((s, r) => s + (r.score || 0) * (r.matched_weight || 0), 0) / other.matched_weight
        : 0;
    tail.forEach(r => Object.keys(r.bins || {}).forEach(k => {
        const acc = other.bins[k] || (other.bins[k] = [0, 0, 0, 0]);
        for (let j = 0; j < 4; j++) acc[j] += r.bins[k][j] || 0;
    }));
    head.push(other);
    return head;
}

// ---- File sim tab -------------------------------------------------------
// Composition similarity: how much of the same *stuff* the two binaries carry,
// tag by tag, independent of how well individual functions matched. A leaf tag
// scores min(count_a, count_b) / max(count_a, count_b) — "A has 2 libc funcs,
// B has 4" -> 50%. A category is the mean of its children, so a category with
// one perfect and one absent library reads 50%, not "mostly fine".
// ponytail: counts, not feature weights. Switch to weight_* if count proves noisy.

// Functions carrying this tag on each side = matched (from the bins) + unique.
function tagSideCounts(row) {
    let a = row.unique_count_a || 0, b = row.unique_count_b || 0;
    Object.keys(row.bins || {}).forEach(k => {
        a += row.bins[k][0] || 0;
        b += row.bins[k][2] || 0;
    });
    return [a, b];
}

function fileSimNode(name, children, a, b, forceSim, tagId, groupType) {
    let sim;
    if (forceSim !== undefined) {
        sim = forceSim;
    } else {
        sim = children.length
            ? children.reduce((s, c) => s + c.sim, 0) / children.length
            : (Math.max(a, b) > 0 ? Math.min(a, b) / Math.max(a, b) : 0);
    }
    return { name, children, a, b, sim, tagId, groupType };
}

function fileSimTree(rows) {
    const byType = new Map();
    (rows || []).forEach(row => {
        const [a, b] = tagSideCounts(row);
        const kids = (row.children || []).map(c => {
            const [ca, cb] = tagSideCounts(c);
            let childName = c.version || c.name;
            if (c.tag_id && c.tag_id.split(':').length > 3) {
                childName = c.tag_id.split(':').slice(3).join(':'); // Extract function name
            }
            return fileSimNode(childName, [], ca, cb, undefined, c.tag_id);
        });
        
        let nodeName = row.name;
        if (row.version) nodeName += ' ' + row.version;
        
        const trueSim = kids.length ? kids.reduce((s, c) => s + c.sim, 0) / kids.length : (Math.max(a, b) > 0 ? Math.min(a, b) / Math.max(a, b) : 0);
        
        const groupedKids = [];
        if (kids.length > 0) {
            const shared = kids.filter(k => k.a > 0 && k.b > 0);
            const uniqueA = kids.filter(k => k.a > 0 && k.b === 0);
            const uniqueB = kids.filter(k => k.a === 0 && k.b > 0);
            
            if (shared.length > 0) {
                shared.forEach(k => k.groupType = 'matched');
                groupedKids.push(fileSimNode(`Shared (${shared.length})`, shared, shared.reduce((s, k) => s + k.a, 0), shared.reduce((s, k) => s + k.b, 0), 1.0));
            }
            if (uniqueA.length > 0) {
                uniqueA.forEach(k => k.groupType = 'uniqueA');
                groupedKids.push(fileSimNode(`Unique to A (${uniqueA.length})`, uniqueA, uniqueA.reduce((s, k) => s + k.a, 0), 0, 0.0));
            }
            if (uniqueB.length > 0) {
                uniqueB.forEach(k => k.groupType = 'uniqueB');
                groupedKids.push(fileSimNode(`Unique to B (${uniqueB.length})`, uniqueB, 0, uniqueB.reduce((s, k) => s + k.b, 0), 0.0));
            }
        }
        
        const node = fileSimNode(nodeName, groupedKids, a, b, trueSim);
        const type = row.type || 'other';
        if (!byType.has(type)) byType.set(type, []);
        byType.get(type).push(node);
    });
    const cats = [...byType.entries()].map(([type, kids]) => fileSimNode(
        type,
        kids.sort((x, y) => y.sim - x.sim),
        kids.reduce((s, k) => s + k.a, 0),
        kids.reduce((s, k) => s + k.b, 0),
    ));
    return fileSimNode('Similarity', cats.sort((x, y) => y.sim - x.sim), 0, 0);
}

// Paths of expanded subtrees. Everything else is collapsed.
const fileSimExpanded = new Set();
// Auto-expand depth 0 once
let fileSimAutoExpanded = false;

window.toggleFileSimRow = function(path) {
    if (fileSimExpanded.has(path)) fileSimExpanded.delete(path);
    else fileSimExpanded.add(path);
    if (binSimDataCache) renderFileSimTable(binSimDataCache);
};

function fileSimRows(node, path, depth, out, tagToMatched, tagToUniqueA, tagToUniqueB) {
    if (node.name === 'untagged' && depth === 1 && node.children.length > 0 && node.children[0].name === 'Untagged') {
        // Skip redundant 'untagged' category wrapper for the 'Untagged' node
        node.children.forEach(c => fileSimRows(c, path + '/' + c.name, depth, out, tagToMatched, tagToUniqueA, tagToUniqueB));
        return;
    }

    if (node.tagId && node.groupType && tagToMatched) {
        const rowsToRender = [];
        if (node.groupType === 'matched') {
            const pairs = tagToMatched.get(node.tagId) || [];
            rowsToRender.push(...pairs.map(p => ({ type: 'matched', data: p })));
            const uniqA = tagToUniqueA.get(node.tagId) || [];
            rowsToRender.push(...uniqA.map(f => ({ type: 'uniqueA', data: f })));
            const uniqB = tagToUniqueB.get(node.tagId) || [];
            rowsToRender.push(...uniqB.map(f => ({ type: 'uniqueB', data: f })));
        } else if (node.groupType === 'uniqueA') {
            const funcs = tagToUniqueA.get(node.tagId) || [];
            rowsToRender.push(...funcs.map(f => ({ type: 'uniqueA', data: f })));
        } else if (node.groupType === 'uniqueB') {
            const funcs = tagToUniqueB.get(node.tagId) || [];
            rowsToRender.push(...funcs.map(f => ({ type: 'uniqueB', data: f })));
        }
        
        if (rowsToRender.length > 0) {
            rowsToRender.forEach((item) => {
                out.push(renderMatchedFunctionRow(item.data, item.type, depth));
            });
            return;
        }
    }

    const hasKids = node.children.length > 0;
    if (depth === 0 && !fileSimAutoExpanded) {
        fileSimExpanded.add(path);
    }
    const expanded = fileSimExpanded.has(path);
    const pct = (node.sim * 100).toFixed(node.sim === 1 || node.sim === 0 ? 0 : 1) + '%';
    const bar = Math.round(node.sim * 100);
    out.push(`
        <tr style="${depth === 0 ? 'font-weight:bold;' : ''}; border-bottom: 1px solid var(--border);">
            <td style="padding:8px; padding-left:${12 + depth * 22}px;">
                ${hasKids
                    ? `<span onclick="toggleFileSimRow(${escapeAttr(jsString(path))})" style="cursor:pointer; user-select:none; color:var(--subtle); margin-right:6px;">${expanded ? '▼' : '▶'}</span>`
                    : '<span style="display:inline-block; width:14px;"></span>'}
                ${escapeHtml(node.name)}
            </td>
            <td colspan="4" style="padding:8px;">
                <div style="display:flex; align-items:center; gap:20px; justify-content:flex-end; color:var(--subtle);">
                    <div title="Functions in A">A: <span style="color:var(--text);">${Math.round(node.a)}</span></div>
                    <div title="Functions in B">B: <span style="color:var(--text);">${Math.round(node.b)}</span></div>
                    <div style="width:160px;">
                        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:4px; font-size:0.7rem;">
                            <span>Similarity</span>
                            <span style="color:var(--accent); font-weight:bold;">${pct}</span>
                        </div>
                        <div style="background:var(--border); border-radius:3px; height:6px;">
                            <div style="width:${bar}%; background:var(--accent); height:6px; border-radius:3px;"></div>
                        </div>
                    </div>
                </div>
            </td>
        </tr>`);
    if (expanded) node.children.forEach(c => fileSimRows(c, path + '/' + c.name, depth + 1, out, tagToMatched, tagToUniqueA, tagToUniqueB));
}

async function renderFileSimTable(data) {
    const el = document.getElementById('bin-sim-filesim');
    if (!el) return;
    const rows = data.tags_summary || [];
    if (!rows.length) {
        el.innerHTML = '<div style="color:var(--dim); text-align:center; padding:40px;">No tag data for this pair.</div>';
        return;
    }

    if (!binSimFullDiff && binSimCtx) {
        el.innerHTML = '<div style="color:var(--dim); text-align:center; padding:40px;">Loading full function data…</div>';
        try {
            let u = `/api/diff?collection_a=${encodeURIComponent(binSimCtx.collection)}&md5_a=${encodeURIComponent(binSimCtx.md5a)}&md5_b=${encodeURIComponent(binSimCtx.md5b)}`;
            if (binSimCtx.collB) u += `&collection_b=${encodeURIComponent(binSimCtx.collB)}`;
            if (binSimCtx.poolId) u += `&pool=${encodeURIComponent(binSimCtx.poolId)}`;
            const r = await fetch(u);
            binSimFullDiff = await r.json();
            Object.assign(binSimDataCache.functions_metadata, binSimFullDiff.functions_metadata || {});
        } catch (e) { console.error('filesim full diff fetch failed', e); }
    }

    const tagToMatched = new Map();
    const tagToUniqueA = new Map();
    const tagToUniqueB = new Map();
    
    if (binSimFullDiff) {
        const getTags = (fid) => {
            const meta = binSimFullDiff.functions_metadata[fid];
            if (meta && meta.tags) {
                if (typeof meta.tags === 'string') return [meta.tags];
                if (Array.isArray(meta.tags) && meta.tags.length > 0) return meta.tags;
                if (typeof meta.tags === 'object' && Object.keys(meta.tags).length > 0) return Object.keys(meta.tags);
            }
            return ['untagged'];
        };

        (binSimFullDiff.diff.matched || []).forEach(m => {
            const tags = new Set([...(m.func_a ? getTags(m.func_a) : []), ...(m.func_b ? getTags(m.func_b) : [])]);
            tags.forEach(t => {
                if (!tagToMatched.has(t)) tagToMatched.set(t, []);
                tagToMatched.get(t).push(m);
            });
        });
        (binSimFullDiff.diff.unique_to_a || []).forEach(u => {
            getTags(u.func_id).forEach(t => {
                if (!tagToUniqueA.has(t)) tagToUniqueA.set(t, []);
                tagToUniqueA.get(t).push(u);
            });
        });
        (binSimFullDiff.diff.unique_to_b || []).forEach(u => {
            getTags(u.func_id).forEach(t => {
                if (!tagToUniqueB.has(t)) tagToUniqueB.set(t, []);
                tagToUniqueB.get(t).push(u);
            });
        });
    }

    const nameA = data.file_metadata_a?.file_name || 'Binary A';
    const nameB = data.file_metadata_b?.file_name || 'Binary B';
    const body = [];
    fileSimAutoExpanded = false;
    fileSimRows(fileSimTree(rows), 'root', 0, body, tagToMatched, tagToUniqueA, tagToUniqueB);
    fileSimAutoExpanded = true;
    el.innerHTML = `
        <table class="bin-sim-mc-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
            <thead>
                <tr>
                    <th style="padding:10px;">Tag / Similarity</th>
                    <th style="padding:10px; text-align:center;">${escapeHtml(nameA)}</th>
                    <th style="padding:10px; text-align:center; width:50px;">Notes</th>
                    <th style="padding:10px; text-align:center;">${escapeHtml(nameB)}</th>
                    <th style="padding:10px; text-align:center; width:50px;">Notes</th>
                </tr>
            </thead>
            <tbody>${body.join('')}</tbody>
        </table>`;
}

// Aggregate a tag's fixed 5% server bins up to the currently selected split.
// Returns Map<groupIdx, [count_a, weight_a, count_b, weight_b]>.
function tagBinGroups(t, perGroup) {
    const groups = new Map();
    Object.keys(t.bins || {}).forEach(k => {
        const g = Math.floor(parseInt(k, 10) / perGroup);
        const acc = groups.get(g) || [0, 0, 0, 0];
        const b = t.bins[k];
        for (let j = 0; j < 4; j++) acc[j] += b[j] || 0;
        groups.set(g, acc);
    });
    return groups;
}

async function renderBinaryDiffSankey(data) {
    const container = document.getElementById('bin-sim-sankey');
    if (!container) return;

    // Detailed mode needs per-function ids/names → lazy-fetch the full diff once.
    if (sankeyMode === 'detailed' && !binSimFullDiff && binSimCtx) {
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">Loading detailed graph…</div>';
        try {
            let u = `/api/diff?collection_a=${encodeURIComponent(binSimCtx.collection)}&md5_a=${encodeURIComponent(binSimCtx.md5a)}&md5_b=${encodeURIComponent(binSimCtx.md5b)}`;
            if (binSimCtx.collB) u += `&collection_b=${encodeURIComponent(binSimCtx.collB)}`;
            if (binSimCtx.poolId) u += `&pool=${encodeURIComponent(binSimCtx.poolId)}`;
            const r = await fetch(u);
            binSimFullDiff = await r.json();
        } catch (e) { console.error('detailed sankey fetch failed', e); }
    }
    container.innerHTML = '';

    // Data source: compact projection for simplified, full diff for detailed.
    const isDetailed = sankeyMode === 'detailed';
    const isTagMode = sankeyMode === 'tags' && (data.tags_summary || []).length > 0;
    const src = isDetailed
        ? (binSimFullDiff && binSimFullDiff.diff ? binSimFullDiff.diff : { matched: [], unique_to_a: [], unique_to_b: [] })
        : (data.sankey || { matched: [], unique_to_a: [], unique_to_b: [] });
    const funcsMeta = isDetailed ? (binSimFullDiff && binSimFullDiff.functions_metadata) : null;

    const detailedBtn = document.getElementById('bsim-sankey-btn-detailed');
    if (detailedBtn) {
        detailedBtn.disabled = false;
        detailedBtn.classList.remove('disabled');
        detailedBtn.title = "Show detailed function-level similarities";
        detailedBtn.style.opacity = 1.0;
        detailedBtn.style.cursor = 'pointer';
    }

    const filenameA = data.file_metadata_a?.file_name || 'A';
    const filenameB = data.file_metadata_b?.file_name || 'B';
    
    const width = container.clientWidth;
    
    const rawMatched = src.matched || [];
    const rawUniqueA = src.unique_to_a || [];
    const rawUniqueB = src.unique_to_b || [];
    
    // Tag mode's node count is driven by the tag rows, not the cluster rows, so the
    // canvas has to be measured from them or every column gets squeezed into the
    // height budget for 10 nodes.
    const tagRows = isTagMode ? foldSmallTags(data.tags_summary || []) : [];
    const tagPerGroup = Math.max(1, Math.round(sankeySplit / 5));   // server bins are 5% wide

    let maxNodesInColumn = 10;
    if (isTagMode) {
        let middle = 0;
        tagRows.forEach(t => {
            middle += tagBinGroups(t, tagPerGroup).size;
            if ((t.unique_weight_a || 0) > 0) middle += 1;   // its own unmatched node
            if ((t.unique_weight_b || 0) > 0) middle += 1;
        });
        maxNodesInColumn = Math.max(tagRows.length, middle, 10);
    } else if (isDetailed) {
        const groupA_count = rawMatched.filter(m => m.func_a).length +
                             rawUniqueA.filter(u => u.func_id).length;
        const groupB_count = rawMatched.filter(m => m.func_b).length +
                             rawUniqueB.filter(u => u.func_id).length;
        const cluster_count = rawMatched.length + rawUniqueA.length + rawUniqueB.length;
        maxNodesInColumn = Math.max(groupA_count, groupB_count, cluster_count, 10);
    }
    
    const padding = maxNodesInColumn > 30 ? 2 : 8;
    const minHeightNeeded = maxNodesInColumn * (padding + 10) + 50;
    const height = Math.max(container.clientHeight || 400, minHeightNeeded);
    
    const svg = d3.select('#bin-sim-sankey').append('svg')
        .attr('width', width)
        .attr('height', height);
        
    const zoomG = svg.append('g');
        
    // Build Nodes and Links
    const nodesMap = new Map();
    const links = [];
    const funcParentMap = new Map();
    
    const getNode = (id, name, color, funcs = []) => {
        if (!nodesMap.has(id)) {
            nodesMap.set(id, { id, name, color, funcs, index: nodesMap.size });
        }
        return nodesMap.get(id);
    };
    
    const getFuncValue = (fid) => {
        if (sankeyScale === 'features') {
            const meta = funcsMeta ? funcsMeta[fid] : null;
            return Math.max(1, (meta && meta.bsim_features_count) ? parseInt(meta.bsim_features_count) : 1);
        }
        return 1;
    };

    const sumFuncsValue = (funcs) => {
        return (funcs || []).reduce((sum, fid) => sum + getFuncValue(fid), 0);
    };

    const getFuncDisplayName = (fid) => {
        const meta = funcsMeta ? funcsMeta[fid] : null;
        if (meta && meta.name) {
            return meta.name;
        }
        const parts = fid.split(':');
        return '@' + parts.pop();
    };

    // Sankey shows the full aggregate (server-computed); table filters apply to tables only.
    const sortedMatched = rawMatched;
    const sortedUniqueA = rawUniqueA;
    const sortedUniqueB = rawUniqueB;

    const matchedRank = new Map(sortedMatched.map((m, idx) => [m.cluster_uuid, idx]));
    const uniqueARank = new Map(sortedUniqueA.map((u, idx) => [u.cluster_uuid, idx]));
    const uniqueBRank = new Map(sortedUniqueB.map((u, idx) => [u.cluster_uuid, idx]));

    if (!isDetailed) {
        const uVal = (u) => sankeyScale === 'features' ? Math.max(1, u.feat || 1) : 1;
        let totalUniqueA = 0;
        sortedUniqueA.forEach(u => { totalUniqueA += uVal(u); });

        let totalUniqueB = 0;
        sortedUniqueB.forEach(u => { totalUniqueB += uVal(u); });

        const metricSuffix = sankeyScale === 'features' ? 'feats' : 'funcs';

        if (isTagMode) {
            // Tag mode crosses both axes: one row per tag (libc, mirai_core, ...), each
            // split across similarity bins, so a library that matches poorly is visibly
            // different from one that matches perfectly.
            const perGroup = tagPerGroup;
            const fmt = (v) => (v % 1 !== 0 ? v.toFixed(1) : String(v));
            // Server bins are [count_a, weight_a, count_b, weight_b]; each side is
            // tracked separately because a match need not be tagged the same on both.
            const slot = { a: 0, b: 2 };
            const binVal = (b, side) => b[slot[side] + (sankeyScale === 'features' ? 1 : 0)] || 0;

            tagRows.forEach((t, i) => {
                const label = tagLabel(t);
                const score = t.score || 0;
                const tagColor = `hsl(${score * 120}, var(--color-s-med), var(--color-l-dim))`;

                const uniqA = sankeyScale === 'features' ? (t.unique_weight_a || 0) : (t.unique_count_a || 0);
                const uniqB = sankeyScale === 'features' ? (t.unique_weight_b || 0) : (t.unique_count_b || 0);

                const groups = tagBinGroups(t, perGroup);

                let totalA = uniqA, totalB = uniqB;
                groups.forEach(b => { totalA += binVal(b, 'a'); totalB += binVal(b, 'b'); });
                if (totalA <= 0 && totalB <= 0) return;

                let nodeA = null;
                if (totalA > 0) {
                    nodeA = getNode(`tag_a_${i}`, `${filenameA} · ${label} (${fmt(totalA)} ${metricSuffix}, ${(t.coverage_pct_a || 0).toFixed(0)}% of ${filenameA})`, tagColor);
                    nodeA.alignOverride = 0;
                    nodeA.cohesion = score;
                }
                let nodeB = null;
                if (totalB > 0) {
                    nodeB = getNode(`tag_b_${i}`, `${filenameB} · ${label} (${fmt(totalB)} ${metricSuffix}, ${(t.coverage_pct_b || 0).toFixed(0)}% of ${filenameB})`, tagColor);
                    nodeB.alignOverride = 2;
                    nodeB.cohesion = score;
                }

                Array.from(groups.keys()).sort((x, y) => y - x).forEach(g => {
                    const b = groups.get(g);
                    const vA = binVal(b, 'a');
                    const vB = binVal(b, 'b');
                    if (vA <= 0 && vB <= 0) return;
                    const lo = g * perGroup * 5;
                    const hi = Math.min(100, lo + perGroup * 5);
                    const mid = (lo + hi) / 200;
                    const mNode = getNode(
                        `tag_c_${i}_${g}`,
                        `${label} ${lo}%-${hi}% (${fmt(Math.max(b[0], b[2]))} funcs)`,
                        `hsl(${mid * 120}, var(--color-s-med), var(--color-l-dim))`
                    );
                    mNode.alignOverride = 1;
                    mNode.cohesion = mid;
                    if (nodeA && vA > 0) links.push({ source: nodeA.index, target: mNode.index, value: vA });
                    if (nodeB && vB > 0) links.push({ source: mNode.index, target: nodeB.index, value: vB });
                });

                // Unmatched mass gets its OWN node per tag, not one shared bucket:
                // the whole point of the row is to see how much of this tag matched
                // and how much did not, scaled against each other.
                if (nodeA && uniqA > 0) {
                    const n = getNode(`tag_ua_${i}`, `${label} unmatched in ${filenameA} (${fmt(uniqA)} ${metricSuffix})`, '#f92672');
                    n.alignOverride = 1;
                    links.push({ source: nodeA.index, target: n.index, value: uniqA });
                }
                if (nodeB && uniqB > 0) {
                    const n = getNode(`tag_ub_${i}`, `${label} unmatched in ${filenameB} (${fmt(uniqB)} ${metricSuffix})`, '#66d9ef');
                    n.alignOverride = 1;
                    links.push({ source: n.index, target: nodeB.index, value: uniqB });
                }
            });
        } else {
            const sortCol = (binSimSortState.matched && binSimSortState.matched.col) || 'similarity';
            const groupCol = sortCol === 'cluster_name' ? 'similarity' : sortCol;

            let minVal = 0.0;
            let maxVal = 1.0;
            if (groupCol === 'avg_features') {
                const vals = sortedMatched.map(m => m[groupCol] || 0);
                minVal = vals.length > 0 ? Math.min(...vals) : 0;
                maxVal = vals.length > 0 ? Math.max(...vals) : 100;
                if (minVal === maxVal) {
                    maxVal = minVal + 10;
                }
            }

            const step = sankeySplit;
            const numBins = Math.round(100 / step);
            const bins = Array.from({ length: numBins }, (_, i) => ({
                binIdx: i,
                clusters: [],
                totalA: 0,
                totalB: 0,
                sumCohesion: 0,
                sumWeights: 0
            }));

            sortedMatched.forEach(m => {
                const val = m[groupCol] || 0;
                let fraction = (val - minVal) / (maxVal - minVal);
                if (fraction < 0) fraction = 0;
                if (fraction > 1) fraction = 1;
                
                let binIdx = Math.floor(fraction * numBins);
                if (binIdx >= numBins) binIdx = numBins - 1;
                
                const wValMatch = (f) => sankeyScale === 'features' ? Math.max(1, f || 1) : 1;
                const wA = wValMatch(m.feat_a);
                const wB = wValMatch(m.feat_b);
                const similarity = m.similarity || 0;

                bins[binIdx].clusters.push(m);
                bins[binIdx].totalA += wA;
                bins[binIdx].totalB += wB;
                bins[binIdx].sumCohesion += similarity * (wA + wB);
                bins[binIdx].sumWeights += (wA + wB);
            });

            const getBinName = (binIdx, countText, prefix) => {
                const stepVal = (maxVal - minVal) / numBins;
                const low = minVal + binIdx * stepVal;
                const high = minVal + (binIdx + 1) * stepVal;
                
                let label = '';
                if (groupCol === 'similarity' || groupCol === 'cohesion' || groupCol === 'sim_rarity') {
                    const lowPct = Math.round(low * 100);
                    const highPct = Math.round(high * 100);
                    const colName = groupCol === 'similarity' ? 'Similarity' : (groupCol === 'cohesion' ? 'Cohesion' : 'Rarity');
                    if (prefix === 'a') {
                        label = `${filenameA} Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                    } else if (prefix === 'b') {
                        label = `${filenameB} Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                    } else {
                        label = `Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                    }
                } else {
                    const lowNum = Math.round(low);
                    const highNum = Math.round(high);
                    const colName = 'Avg Feat';
                    if (prefix === 'a') {
                        label = `${filenameA} Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                    } else if (prefix === 'b') {
                        label = `${filenameB} Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                    } else {
                        label = `Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                    }
                }
                return label;
            };

            bins.forEach(b => {
                if (b.clusters.length === 0) return;

                const binAvgCohesion = b.sumWeights > 0 ? (b.sumCohesion / b.sumWeights) : (b.binIdx * (step / 100) + (step / 200));
                const binColor = `hsl(${binAvgCohesion * 120}, var(--color-s-med), var(--color-l-dim))`;

                let binNodeA = null;
                if (b.totalA > 0) {
                    const binNodeAId = `simplified_a_matched_bin_${b.binIdx}`;
                    binNodeA = getNode(binNodeAId, getBinName(b.binIdx, `${b.totalA} ${metricSuffix}`, 'a'), binColor);
                    binNodeA.alignOverride = 0;
                    binNodeA.cohesion = binAvgCohesion;
                }

                const binNodeId = `simplified_c_matched_bin_${b.binIdx}`;
                const binNode = getNode(
                    binNodeId, 
                    getBinName(b.binIdx, `${b.clusters.length} clusters`, 'c'), 
                    binColor
                );
                binNode.alignOverride = 1;
                binNode.cohesion = binAvgCohesion;

                let binNodeB = null;
                if (b.totalB > 0) {
                    const binNodeBId = `simplified_b_matched_bin_${b.binIdx}`;
                    binNodeB = getNode(binNodeBId, getBinName(b.binIdx, `${b.totalB} ${metricSuffix}`, 'b'), binColor);
                    binNodeB.alignOverride = 2;
                    binNodeB.cohesion = binAvgCohesion;
                }

                if (binNodeA) {
                    links.push({ source: binNodeA.index, target: binNode.index, value: b.totalA });
                }
                if (binNodeB) {
                    links.push({ source: binNode.index, target: binNodeB.index, value: b.totalB });
                }
            });
        }

        let nodeA_unique, nodeC_uniqueA, nodeC_uniqueB, nodeB_unique;
        if (!isTagMode && totalUniqueA > 0) {
            nodeA_unique = getNode('simplified_a_unique', `${filenameA} Unmatched (${totalUniqueA} ${metricSuffix})`, '#f92672');
            nodeA_unique.alignOverride = 0;
            nodeC_uniqueA = getNode('simplified_c_uniqueA', `Unmatched to ${filenameA} (${sortedUniqueA.length})`, '#f92672');
            nodeC_uniqueA.alignOverride = 1;
            links.push({ source: nodeA_unique.index, target: nodeC_uniqueA.index, value: totalUniqueA });
        }
        if (!isTagMode && totalUniqueB > 0) {
            nodeC_uniqueB = getNode('simplified_c_uniqueB', `Unmatched to ${filenameB} (${sortedUniqueB.length})`, '#66d9ef');
            nodeC_uniqueB.alignOverride = 1;
            nodeB_unique = getNode('simplified_b_unique', `${filenameB} Unmatched (${totalUniqueB} ${metricSuffix})`, '#66d9ef');
            nodeB_unique.alignOverride = 2;
            links.push({ source: nodeC_uniqueB.index, target: nodeB_unique.index, value: totalUniqueB });
        }
    } else {
        // 1. Matched Clusters
        sortedMatched.forEach(m => {
            const similarity = m.similarity || 0;
            const cColor = `hsl(${similarity * 120}, var(--color-s-med), var(--color-l-dim))`;
            const cNode = getNode('cluster_' + m.cluster_uuid, m.cluster_name, cColor);
            cNode.cohesion = m.cohesion || 0;
            cNode.cluster_uuid = m.cluster_uuid;
            cNode.cluster_name = m.cluster_name;
            cNode.size = 2;
            cNode.stability = 1.0;
            cNode.avg_features = m.avg_features || 0;

            if (m.func_a) {
                const fNodeId = 'funcgroup_a_' + m.cluster_uuid;
                funcParentMap.set(fNodeId, m.cluster_uuid);
                const fNode = getNode(fNodeId, [getFuncDisplayName(m.func_a)], cColor, [m.func_a]);
                fNode.cohesion = m.cohesion || 0;
                links.push({ source: fNode.index, target: cNode.index, value: getFuncValue(m.func_a) });
            }

            if (m.func_b) {
                const fNodeId = 'funcgroup_b_' + m.cluster_uuid;
                funcParentMap.set(fNodeId, m.cluster_uuid);
                const fNode = getNode(fNodeId, [getFuncDisplayName(m.func_b)], cColor, [m.func_b]);
                fNode.cohesion = m.cohesion || 0;
                links.push({ source: cNode.index, target: fNode.index, value: getFuncValue(m.func_b) });
            }
        });
        
        // 2. Unmatched to A Functions
        sortedUniqueA.forEach(u => {
            const targetNodeId = u.is_clustered ? ('cluster_' + u.cluster_uuid) : 'unclustered_a_group';
            const targetNodeName = u.is_clustered ? u.cluster_name : 'Unclustered';
            const cNode = getNode(targetNodeId, targetNodeName, '#f92672');
            cNode.cluster_uuid = u.is_clustered ? u.cluster_uuid : '';
            cNode.cluster_name = targetNodeName;
            cNode.size = (cNode.size || 0) + 1;
            cNode.stability = 1.0;
            cNode.cohesion = u.cohesion || 0;
            cNode.avg_features = u.avg_features || 0;

            if (u.func_id) {
                const fNodeId = 'funcgroup_a_' + (u.is_clustered ? u.cluster_uuid : u.func_id);
                funcParentMap.set(fNodeId, u.is_clustered ? u.cluster_uuid : 'unclustered_a_group');
                const fNode = getNode(fNodeId, [getFuncDisplayName(u.func_id)], '#f92672', [u.func_id]);
                links.push({ source: fNode.index, target: cNode.index, value: getFuncValue(u.func_id) });
            }
        });
        
        // 3. Unmatched to B Functions
        sortedUniqueB.forEach(u => {
            const targetNodeId = u.is_clustered ? ('cluster_' + u.cluster_uuid) : 'unclustered_b_group';
            const targetNodeName = u.is_clustered ? u.cluster_name : 'Unclustered';
            const cNode = getNode(targetNodeId, targetNodeName, '#66d9ef');
            cNode.cluster_uuid = u.is_clustered ? u.cluster_uuid : '';
            cNode.cluster_name = targetNodeName;
            cNode.size = (cNode.size || 0) + 1;
            cNode.stability = 1.0;
            cNode.cohesion = u.cohesion || 0;
            cNode.avg_features = u.avg_features || 0;

            if (u.func_id) {
                const fNodeId = 'funcgroup_b_' + (u.is_clustered ? u.cluster_uuid : u.func_id);
                funcParentMap.set(fNodeId, u.is_clustered ? u.cluster_uuid : 'unclustered_b_group');
                const fNode = getNode(fNodeId, [getFuncDisplayName(u.func_id)], '#66d9ef', [u.func_id]);
                links.push({ source: cNode.index, target: fNode.index, value: getFuncValue(u.func_id) });
            }
        });
    }
    
    const nodes = Array.from(nodesMap.values());
    
    if (nodes.length === 0 || links.length === 0) {
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">Not enough data for graph</div>';
        return;
    }
    
    const maxPaddingLimit = maxNodesInColumn > 30 ? 2 : 8;
    const dynamicPadding = Math.max(2, Math.min(maxPaddingLimit, Math.floor((height - 50) / (maxNodesInColumn + 1))));

    const marginX = 25;

    const sankey = d3.sankey()
        .nodeWidth(15)
        .nodePadding(dynamicPadding)
        .nodeAlign((node) => {
            if (node.alignOverride !== undefined) return node.alignOverride;
            if (node.id.startsWith('funcgroup_a_')) return 0;
            if (node.id.startsWith('funcgroup_b_')) return 2;
            return 1; // cluster_
        })
        .extent([[marginX, 10], [width - marginX, height - 10]])
        .nodeSort((a, b) => {
            const getNodeSortRank = (n) => {
                if (n.id.startsWith('tag_')) {
                    // tag_<a|b|c|ua|ub>_<tagIdx>[_<binGroup>]
                    const m = n.id.match(/^tag_(a|b|c|ua|ub)_(\d+)(?:_(\d+))?$/);
                    if (m) {
                        const kind = m[1];
                        const tagIdx = parseInt(m[2], 10);
                        const type = kind === 'a' ? 0 : (kind === 'b' ? 2 : 1);
                        // Within a tag: best-matching bins first, unmatched last.
                        let sub = 0;
                        if (kind === 'c') sub = 100 - parseInt(m[3] || '0', 10);
                        else if (kind === 'ua' || kind === 'ub') sub = 500;
                        return { type: type, rank: tagIdx * 1000 + sub };
                    }
                }
                if (n.id.startsWith('simplified_')) {
                    const binMatch = n.id.match(/_bin_(\d+)/);
                    let rank = 0;
                    if (binMatch) {
                        const binIdx = parseInt(binMatch[1]);
                        const sortDir = (binSimSortState.matched && binSimSortState.matched.dir) !== undefined 
                            ? binSimSortState.matched.dir 
                            : -1;
                        const numBins = Math.round(100 / sankeySplit);
                        rank = sortDir === -1 ? ((numBins - 1) - binIdx) : binIdx;
                    } else {
                        const isMatched = n.id.includes('_matched');
                        rank = isMatched ? 0 : (n.id.includes('uniqueA') ? 100 : 200);
                    }
                    const type = n.id.includes('_a_') ? 0 : (n.id.includes('_c_') ? 1 : 2);
                    return { type: type, rank: rank };
                }
                
                if (n.id.startsWith('cluster_')) {
                    const uuid = n.id.replace('cluster_', '');
                    if (matchedRank.has(uuid)) return { type: 0, rank: matchedRank.get(uuid) };
                    if (uniqueARank.has(uuid)) return { type: 1, rank: uniqueARank.get(uuid) };
                    if (uniqueBRank.has(uuid)) return { type: 2, rank: uniqueBRank.get(uuid) };
                    return { type: 3, rank: 999 };
                }
                
                if (n.id.startsWith('funcgroup_a_')) {
                    const parentUuid = funcParentMap.get(n.id);
                    if (matchedRank.has(parentUuid)) return { type: 0, rank: matchedRank.get(parentUuid) };
                    if (uniqueARank.has(parentUuid)) return { type: 1, rank: uniqueARank.get(parentUuid) };
                    return { type: 3, rank: 999 };
                }
                
                if (n.id.startsWith('funcgroup_b_')) {
                    const parentUuid = funcParentMap.get(n.id);
                    if (matchedRank.has(parentUuid)) return { type: 0, rank: matchedRank.get(parentUuid) };
                    if (uniqueBRank.has(parentUuid)) return { type: 2, rank: uniqueBRank.get(parentUuid) };
                    return { type: 3, rank: 999 };
                }
                return { type: 3, rank: 999 };
            };
            
            const rA = getNodeSortRank(a);
            const rB = getNodeSortRank(b);
            if (rA.type !== rB.type) {
                return rA.type - rB.type;
            }
            return rA.rank - rB.rank;
        });
        
    let graph;
    try {
        graph = sankey({
            nodes: nodes.map(d => Object.assign({}, d)),
            links: links.map(d => Object.assign({}, d))
        });
    } catch(e) {
        console.error("Sankey layout failed", e);
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--danger);">Graph layout error</div>';
        return;
    }
    
    // Add Links
    zoomG.append("g")
        .selectAll("path")
        .data(graph.links)
        .enter().append("path")
        .attr("d", d => {
            const x0 = d.source.x1;
            const x1 = d.target.x0;
            const x2 = x0 + (x1 - x0) * 0.4;
            const x3 = x0 + (x1 - x0) * 0.6;
            
            const y0_top = d.source.y0;
            const y0_bot = d.source.y1;
            const y1_top = d.target.y0;
            const y1_bot = d.target.y1;
            
            return `M ${x0},${y0_top}
                    C ${x2},${y0_top} ${x3},${y1_top} ${x1},${y1_top}
                    L ${x1},${y1_bot}
                    C ${x3},${y1_bot} ${x2},${y0_bot} ${x0},${y0_bot}
                    Z`;
        })
        .attr("stroke", "none")
        .attr("stroke-width", 0)
        .attr("fill", d => d.target.color || 'var(--text)')
        .style("fill-opacity", 0.4)
        .style("cursor", d => (d.source.id.startsWith('funcgroup_') || d.target.id.startsWith('funcgroup_')) ? "pointer" : "default")
        .on("mouseenter", function(event, d) { 
            d3.select(this).style("fill-opacity", 0.8);
            const sourceIsFuncGroup = d.source.id.startsWith('funcgroup_');
            const targetIsFuncGroup = d.target.id.startsWith('funcgroup_');
            const isDetailedLink = sourceIsFuncGroup || targetIsFuncGroup;
            if (isDetailedLink) {
                const funcNode = sourceIsFuncGroup ? d.source : d.target;
                const clusterNode = sourceIsFuncGroup ? d.target : d.source;
                const funcsList = funcNode.funcs || [];
                
                const customMembers = funcsList.map(fid => {
                    const meta = (binSimDataCache && binSimDataCache.functions_metadata) ? binSimDataCache.functions_metadata[fid] : null;
                    const parts = fid.split(':');
                    const entry = parts.pop();
                    const md5 = parts.pop();
                    const function_name = meta && meta.name ? meta.name : ('sub_' + entry);
                    const return_type = meta && meta.return_type ? meta.return_type : 'void';
                    const parameters = meta && meta.parameters ? (Array.isArray(meta.parameters) ? meta.parameters : [meta.parameters]) : [];
                    const bsim_features_count = meta && meta.bsim_features_count ? parseInt(meta.bsim_features_count) : 0;
                    const namespace = meta && meta.namespace ? meta.namespace : '';
                    const entrypoint_address = meta && meta.entrypoint_address ? meta.entrypoint_address : ('0x' + entry);
                    
                    return {
                        function_id: fid,
                        function_name: function_name,
                        return_type: return_type,
                        parameters: parameters,
                        bsim_features_count: bsim_features_count,
                        namespace: namespace,
                        entrypoint_address: entrypoint_address
                    };
                });
                
                if (window.parent && window.parent.showClusterTableTooltipFromIframe && window.parent !== window) {
                    const cleanName = clusterNode.cluster_name.replace(/'/g, "\\'");
                    window.parent.showClusterTableTooltipFromIframe(
                        window.name, 
                        clusterNode.cluster_uuid + '_path_' + funcNode.id, 
                        cleanName + ' (Path Functions)', 
                        customMembers.length, 
                        1.0, 
                        clusterNode.cohesion || 0, 
                        clusterNode.avg_features || 0, 
                        event,
                        customMembers
                    );
                } else if (window.showClusterTableTooltip) {
                    const cleanName = clusterNode.cluster_name.replace(/'/g, "\\'");
                    window.showClusterTableTooltip(
                        event,
                        clusterNode.cluster_uuid + '_path_' + funcNode.id, 
                        cleanName + ' (Path Functions)', 
                        customMembers.length, 
                        1.0, 
                        clusterNode.cohesion || 0, 
                        clusterNode.avg_features || 0, 
                        customMembers
                    );
                }
            }
        })
        .on("mousemove", function(event) {
            if (window.parent && window.parent.moveClusterTableTooltipFromIframe && window.parent !== window) {
                window.parent.moveClusterTableTooltipFromIframe(window.name, event);
            } else if (window.moveClusterTableTooltip) {
                window.moveClusterTableTooltip(event);
            }
        })
        .on("mouseleave", function(event) { 
            d3.select(this).style("fill-opacity", 0.4); 
            handleIframeMouseLeave(event);
        })
        .append("title")
        .text(d => {
            const formatNodeName = (node) => {
                if (Array.isArray(node.name)) {
                    const V = sumFuncsValue(node.funcs);
                    const suffix = sankeyScale === 'features' ? 'feats' : 'funcs';
                    return `${node.funcs.length} Functions (Total: ${V} ${suffix}):\n${node.name.map((n, idx) => `  - ${n} (${getFuncValue(node.funcs[idx])} ${suffix})`).join('\n')}`;
                }
                const suffix = sankeyScale === 'features' ? 'feats' : 'funcs';
                return `${node.name} (${d.value} ${suffix})`;
            };
            return `${formatNodeName(d.source)}\n  ↓\n${formatNodeName(d.target)}`;
        });
        
    // Add Nodes
    const node = zoomG.append("g")
        .selectAll(".node")
        .data(graph.nodes)
        .enter().append("g")
        .attr("class", "node")
        .attr("transform", d => `translate(${d.x0},${d.y0})`);
        
    node.each(function(d) {
        const el = d3.select(this);
        const height = d.y1 - d.y0;
        const width = sankey.nodeWidth();
        
        if (d.id.startsWith('funcgroup_') && d.funcs && d.funcs.length > 0) {
            const V = sumFuncsValue(d.funcs);
            let currentY = 0;
            
            d.funcs.forEach((fid, idx) => {
                const val = getFuncValue(fid);
                const h = height * (val / V);
                
                el.append("rect")
                    .attr("y", currentY)
                    .attr("height", h)
                    .attr("width", width)
                    .attr("fill", d.color)
                    .attr("stroke", "var(--border)")
                    .attr("stroke-width", "0.5px")
                    .attr("opacity", 0.6);
                    
                const name = d.name[idx];
                el.append("text")
                    .attr("x", d.id.startsWith('funcgroup_b_') ? -6 : 6 + width)
                    .attr("y", currentY + h / 2)
                    .attr("dy", "0.35em")
                    .attr("text-anchor", d.id.startsWith('funcgroup_b_') ? "end" : "start")
                    .text(name)
                    .attr("fill", "var(--text)")
                    .attr("font-size", "8px")
                    .attr("font-weight", "normal")
                    .attr("opacity", 0.7)
                    .attr("font-family", "sans-serif");
                    
                currentY += h;
            });
            
            const metricSuffix = sankeyScale === 'features' ? 'feats' : 'funcs';
            const cohesionText = (d.cohesion !== undefined) ? ` [Cohesion: ${d.cohesion.toFixed(2)}]` : '';
            el.append("title")
                .text(`${d.funcs.length} Functions (Total: ${V} ${metricSuffix})${cohesionText}:\n` + 
                      d.funcs.map((fid, idx) => `  - ${d.name[idx]} (${getFuncValue(fid)} ${metricSuffix})`).join('\n'));
        } else {
            if (d.id.startsWith('cluster_')) {
                el.style("cursor", "pointer")
                  .on("mouseenter", function(event) {
                      if (window.parent && window.parent.showClusterTableTooltipFromIframe && window.parent !== window) {
                          const cleanName = d.cluster_name.replace(/'/g, "\\'");
                          window.parent.showClusterTableTooltipFromIframe(
                              window.name, 
                              d.cluster_uuid, 
                              cleanName, 
                              d.size, 
                              d.stability || 1.0, 
                              d.cohesion || 0, 
                              d.avg_features || 0, 
                              event
                          );
                      } else if (window.showClusterTableTooltip) {
                          const cleanName = d.cluster_name.replace(/'/g, "\\'");
                          window.showClusterTableTooltip(
                              event,
                              d.cluster_uuid, 
                              cleanName, 
                              d.size, 
                              d.stability || 1.0, 
                              d.cohesion || 0, 
                              d.avg_features || 0
                          );
                      }
                  })
                  .on("mousemove", function(event) {
                      if (window.parent && window.parent.moveClusterTableTooltipFromIframe && window.parent !== window) {
                          window.parent.moveClusterTableTooltipFromIframe(window.name, event);
                      } else if (window.moveClusterTableTooltip) {
                          window.moveClusterTableTooltip(event);
                      }
                  })
                  .on("mouseleave", handleIframeMouseLeave)
                  .on("click", function(event) {
                      const cleanName = d.cluster_name.replace(/'/g, "\\'");
                      openClusterView(d.cluster_uuid, cleanName, event);
                  });
            }

            el.append("rect")
                .attr("height", height)
                .attr("width", width)
                .attr("fill", d.color)
                .attr("stroke", "var(--border)")
                .attr("stroke-width", "0.5px")
                .attr("opacity", 0.6)
                .append("title")
                .text(`${d.name}${d.cohesion !== undefined ? `\nCohesion: ${d.cohesion.toFixed(2)}` : ''}\n${sankeyScale === 'features' ? 'Features' : 'Functions'}: ${d.value}`);
                
            el.append("text")
                .attr("x", d.id.startsWith('func_b_') || d.id.startsWith('funcgroup_b_') || d.id.startsWith('simplified_b_') ? -6 : 6 + width)
                .attr("y", height / 2)
                .attr("dy", "0.35em")
                .attr("text-anchor", d.id.startsWith('func_b_') || d.id.startsWith('funcgroup_b_') || d.id.startsWith('simplified_b_') ? "end" : "start")
                .text(d.name)
                .attr("fill", "var(--text)")
                .attr("font-size", "8px")
                .attr("font-weight", "normal")
                .attr("opacity", 0.7)
                .attr("font-family", "sans-serif");
        }
    });
}

function setBinSimSort(table, col) {
    if (binSimSortState[table].col === col) {
        binSimSortState[table].dir *= -1;
    } else {
        binSimSortState[table].col = col;
        binSimSortState[table].dir = -1;
    }
    // Re-render headers (sort arrows) then reload that table's first page from the server.
    renderBinSimTables();
    loadBinSimTablePage(table, { reset: true });
}

function binSimFilterChange(shouldApply = false) {
    const prefixes = ['matched', 'ua', 'ub'];
    prefixes.forEach(prefix => {
        const qEl = document.getElementById(`bsim-flt-${prefix}-q`);
        if (qEl) window[`bsim-flt-${prefix}-q-val`] = qEl.value;
        const suffixes = prefix === 'matched' ? ['feat', 'coh', 'rar'] : ['feat', 'rar'];
        suffixes.forEach(suffix => {
            const minEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-min`);
            const maxEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-max`);
            if (minEl) window[`bsim-flt-${prefix}-${suffix}-min-val`] = minEl.value;
            if (maxEl) window[`bsim-flt-${prefix}-${suffix}-max-val`] = maxEl.value;
        });
        const noteSuffixes = prefix === 'matched' ? ['note-a', 'note-b'] : ['note'];
        noteSuffixes.forEach(suffix => {
            const el = document.getElementById(`bsim-flt-${prefix}-${suffix}`);
            if (el) window[`bsim-flt-${prefix}-${suffix}-val`] = el.value;
        });
    });

    // Reload only the table whose filter input changed (derived from the focused input),
    // so a search in one table doesn't reset the others. Fallback: reload all.
    const el = document.activeElement;
    let targets = ['matched', 'uniqueA', 'uniqueB'];
    if (el && el.id) {
        if (el.id.startsWith('bsim-flt-matched-')) targets = ['matched'];
        else if (el.id.startsWith('bsim-flt-ua-')) targets = ['uniqueA'];
        else if (el.id.startsWith('bsim-flt-ub-')) targets = ['uniqueB'];
    }
    if (window._binSimFilterTimer) clearTimeout(window._binSimFilterTimer);
    window._binSimFilterTimer = setTimeout(() => {
        targets.forEach(k => loadBinSimTablePage(k, { reset: true }));
    }, shouldApply ? 0 : 300);
}

const funcSearchHaystack = (fid) => {
    const meta = (binSimDataCache && binSimDataCache.functions_metadata)
        ? (binSimDataCache.functions_metadata[fid] || {}) : {};
    const addr = meta.entrypoint_address || fid.split(':').pop();
    return [meta.name, meta.namespace, addr, ...(meta.tags || []), ...(meta.user_tags || [])]
        .filter(Boolean).join(' ').toLowerCase();
};

const applyFilters = (items, prefix) => {
    const q = (document.getElementById(`bsim-flt-${prefix}-q`)?.value || '').trim().toLowerCase();
    const checkNotes = (funcsList, searchOwner) => {
        if (!searchOwner) return true;
        return funcsList.some(fid => {
            const owners = (binSimDataCache.functions_metadata && binSimDataCache.functions_metadata[fid]?.note_owners) || [];
            return owners.some(o => o.toLowerCase().includes(searchOwner));
        });
    };

    return items.filter(item => {
        if (q) {
            const fids = (item.func_a || item.func_b)
                ? [item.func_a, item.func_b].filter(Boolean)
                : (item.func_id ? [item.func_id] : []);
            const hay = fids.map(funcSearchHaystack).join(' ');
            if (!hay.includes(q)) return false;
        }

        const noteA = (document.getElementById(`bsim-flt-${prefix}-note-a`)?.value || '').trim().toLowerCase();
        if (noteA && !checkNotes(item.func_a ? [item.func_a] : [], noteA)) return false;

        const noteB = (document.getElementById(`bsim-flt-${prefix}-note-b`)?.value || '').trim().toLowerCase();
        if (noteB && !checkNotes(item.func_b ? [item.func_b] : [], noteB)) return false;

        const noteU = (document.getElementById(`bsim-flt-${prefix}-note`)?.value || '').trim().toLowerCase();
        if (noteU) {
            if (!checkNotes(item.func_id ? [item.func_id] : [], noteU)) return false;
        }

        // Similarity filter (matched rows); unique rows have no similarity inputs so this is skipped.
        const simMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-coh-min`)?.value);
        const simMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-coh-max`)?.value);
        if (!isNaN(simMin) && (item.similarity || 0) < simMin) return false;
        if (!isNaN(simMax) && (item.similarity || 0) > simMax) return false;
        
        const featMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-feat-min`)?.value);
        const featMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-feat-max`)?.value);
        if (!isNaN(featMin) && (item.avg_features || 0) < featMin) return false;
        if (!isNaN(featMax) && (item.avg_features || 0) > featMax) return false;

        if (item.sim_rarity !== undefined) {
            const rarMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-rar-min`)?.value);
            const rarMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-rar-max`)?.value);
            if (!isNaN(rarMin) && item.sim_rarity < rarMin) return false;
            if (!isNaN(rarMax) && item.sim_rarity > rarMax) return false;
        }

        return true;
    });
};

const sortItems = (items, state) => {
    return items.sort((a, b) => {
        let valA = a[state.col];
        let valB = b[state.col];
        if (state.col === 'count' && a.funcs) { valA = a.funcs.length; valB = b.funcs.length; }
        if (typeof valA === 'string') return valA.localeCompare(valB) * state.dir;
        return ((valA || 0) - (valB || 0)) * state.dir;
    });
};

// Pre-seed the shared cluster tooltip cache with sample members shipped on the row, so the
// tooltip renders them directly instead of fetching by collection (which fails for
// cross-collection / pool bin-sim). No-op when the cluster has no samples. [[dynamic]]
function seedBinSimClusterSamples(cd) {
    if (!cd || !cd.cluster_uuid || !cd.sample_functions || !cd.sample_functions.length) return;
    if (!window.clusterTooltipMockCache) return;
    window.clusterTooltipMockCache.set(cd.cluster_uuid, { data: {
        uuid: cd.cluster_uuid, name: cd.cluster_name,
        size: Number(cd.member_count || 0), stability: Number(cd.cluster_stability || 0),
        cohesion: Number(cd.cohesion_score || 0), avg_features: Number(cd.avg_features || 0),
        runtime_members: cd.sample_functions, scrollOffset: 0
    }});
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
        ? EntityRenderer.renderTag('function', fid, f.tags, f.user_tags) : '';
    return `
        <div class="bsim-func-cell" style="display:flex; flex-direction:column; gap:2px; min-width:0; text-align:left; width:100%;">
            ${sig}
            <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                <span class="mono dim" style="font-size:0.65rem;">@ ${f.entrypoint_address}</span>
                ${tagsHtml}
            </div>
        </div>`;
}

function renderMatchedFunctionRow(m, type, depth) {
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

        similarityHtml = `
            <div style="display:flex; align-items:center; gap:8px;">
                <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;"
                    onmouseenter="showDiffPreview(${escapeAttr(jsString(fA.function_id))}, ${escapeAttr(jsString(fA.function_name || ''))}, ${escapeAttr(jsString(fB.function_id))}, ${escapeAttr(jsString(fB.function_name || ''))}, ${Number(m.similarity) || 0}, event)"
                    onmousemove="moveCodePreview(event)"
                    onmouseleave="hideDiffPreview(event)"
                    onclick="Nav.openPath(${escapeAttr(jsString(diffUrl))}, event, { title: ${escapeAttr(jsString(`Diff: ${fA.function_name} vs ${fB.function_name}`))}, type: 'diff' })"
                    title="Run Aligned Diff">${(m.similarity * 100).toFixed(1)}%</div>
            </div>`;
        col2 = renderFuncBadge(m.func_a);
        col3 = noteBtn(m.func_a);
        col4 = renderFuncBadge(m.func_b);
        col5 = noteBtn(m.func_b);
    } else if (type === 'uniqueA') {
        similarityHtml = `<span class="mono" style="color:#f92672; font-weight:bold;">0%</span>`;
        if (m.func_id) {
            fA = buildFuncObj(m.func_id);
            col2 = renderFuncBadge(m.func_id);
            col3 = noteBtn(m.func_id);
        }
    } else if (type === 'uniqueB') {
        similarityHtml = `<span class="mono" style="color:#66d9ef; font-weight:bold;">0%</span>`;
        if (m.func_id) {
            fB = buildFuncObj(m.func_id);
            col4 = renderFuncBadge(m.func_id);
            col5 = noteBtn(m.func_id);
        }
    }

    return `
        <tr style="border-bottom: 1px solid var(--border); background: var(--bg);">
            <td style="padding:10px; padding-left:${12 + depth * 22}px;">
                ${similarityHtml}
            </td>
            <td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">
                ${col2}
            </td>
            <td style="padding:4px; vertical-align:top;">
                ${col3}
            </td>
            <td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">
                ${col4}
            </td>
            <td style="padding:4px; vertical-align:top;">
                ${col5}
            </td>
        </tr>`;
}

function renderBinSimTables(isFilterChange = false) {
    if (!binSimDataCache) return;
    const data = binSimDataCache;



    const getSortIcon = (table, col) => {
        if (binSimSortState[table].col === col) {
            return binSimSortState[table].dir === -1 ? '▼' : '▲';
        }
        return '↕';
    };

    const filterHtml = (prefix, suffix) => `
        <div style="display:flex; align-items:center; gap:2px;" onclick="event.stopPropagation()">
            <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key === 'Enter') binSimFilterChange(true)" id="bsim-flt-${prefix}-${suffix}-min" placeholder="Min..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
            <span class="dim" style="font-size:0.6rem">-</span>
            <input type="number" step="any" oninput="binSimFilterChange(false)" onkeydown="if(event.key === 'Enter') binSimFilterChange(true)" id="bsim-flt-${prefix}-${suffix}-max" placeholder="Max..." style="font-size:0.65rem; box-sizing:border-box; width:45%;">
        </div>`;

    // Free-text search over function name / namespace / address / tags for a table.
    const searchHtml = (prefix) => `
        <div onclick="event.stopPropagation()">
            <input type="text" oninput="binSimFilterChange(true)" onkeydown="if(event.key === 'Enter') binSimFilterChange(true)" id="bsim-flt-${prefix}-q" placeholder="Search name / tag / addr..." style="font-size:0.65rem; box-sizing:border-box; width:100%;">
        </div>`;

    const noteFilterHtml = (prefix, suffix) => `
        <div onclick="event.stopPropagation()">
            <input type="text" oninput="binSimFilterChange(true)" onkeydown="if(event.key === 'Enter') binSimFilterChange(true)" id="bsim-flt-${prefix}-${suffix}" placeholder="Note Owner..." style="font-size:0.65rem; box-sizing:border-box; width:100%;">
        </div>`;

    const restoreFilters = (prefix, suffixes, noteSuffixes = []) => {
        const qEl = document.getElementById(`bsim-flt-${prefix}-q`);
        if (qEl && window[`bsim-flt-${prefix}-q-val`]) qEl.value = window[`bsim-flt-${prefix}-q-val`];
        suffixes.forEach(suffix => {
            const minEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-min`);
            const maxEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-max`);
            if (minEl && window[`bsim-flt-${prefix}-${suffix}-min-val`]) minEl.value = window[`bsim-flt-${prefix}-${suffix}-min-val`];
            if (maxEl && window[`bsim-flt-${prefix}-${suffix}-max-val`]) maxEl.value = window[`bsim-flt-${prefix}-${suffix}-max-val`];
        });
        noteSuffixes.forEach(suffix => {
            const el = document.getElementById(`bsim-flt-${prefix}-${suffix}`);
            if (el && window[`bsim-flt-${prefix}-${suffix}-val`]) el.value = window[`bsim-flt-${prefix}-${suffix}-val`];
        });
    };

    const nameA = (data && data.file_metadata_a && data.file_metadata_a.file_name) || 'A';
    const nameB = (data && data.file_metadata_b && data.file_metadata_b.file_name) || 'B';

    const tbodyMatched = document.getElementById('bin-sim-table-matched');
    if (tbodyMatched) {
        const thead = tbodyMatched.previousElementSibling;
        if (thead && !isFilterChange) {
            thead.innerHTML = `
                <tr>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'similarity')">Similarity <small>${getSortIcon('matched', 'similarity')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);">${nameA}</th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width: 50px;">Notes</th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);">${nameB}</th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width: 50px;">Notes</th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'sim_rarity')">Rarity <small>${getSortIcon('matched', 'sim_rarity')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'avg_features')">Avg Feat <small>${getSortIcon('matched', 'avg_features')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'cluster_name')">Cluster <small>${getSortIcon('matched', 'cluster_name')}</small><div class="resizer"></div></th>
                </tr>
                <tr class="filter-row">
                    <th>${filterHtml('matched', 'coh')}</th>
                    <th>${searchHtml('matched')}</th>
                    <th>${noteFilterHtml('matched', 'note-a')}</th>
                    <th></th>
                    <th>${noteFilterHtml('matched', 'note-b')}</th>
                    <th>${filterHtml('matched', 'rar')}</th>
                    <th>${filterHtml('matched', 'feat')}</th>
                    <th></th>
                </tr>
            `;
            restoreFilters('matched', ['feat', 'coh', 'rar'], ['note-a', 'note-b']);
        }
    }

    if (tbodyMatched) {
        // Server already filtered + sorted; render the accumulated pages as-is.
        const matched = binSimPage.matched.items;

        if (matched.length > 0) {
            tbodyMatched.innerHTML = matched.map(m => {
                const cleanName = m.cluster_name.replace(/'/g, "\\'");
                const escUuid = m.cluster_uuid;
                const noteBtn = (fid) => {
                    const fObj = buildFuncObj(fid);
                    return `<div style="min-height:24px; display:flex; align-items:center; justify-content:center;">${EntityRenderer.renderNoteButton(fid, fObj.note_owners, { isTable: true, raw_data: fObj })}</div>`;
                };
                const notesAHtml = m.func_a ? noteBtn(m.func_a) : '';
                const notesBHtml = m.func_b ? noteBtn(m.func_b) : '';

                let similarityHtml = '';
                if (m.func_a && m.func_b) {
                    const fA = buildFuncObj(m.func_a);
                    const fB = buildFuncObj(m.func_b);
                    let diffUrl = '';
                    if (window.buildDiffUrl) {
                        diffUrl = window.buildDiffUrl(fA.function_id, fB.function_id);
                    } else {
                        // Fallback just in case
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

                    const pairId = `${fA.function_id}|${fB.function_id}|unweighted_cosine`;

                    similarityHtml = `
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;"
                            onmouseenter="showDiffPreview(${escapeAttr(jsString(fA.function_id))}, ${escapeAttr(jsString(fA.function_name || ''))}, ${escapeAttr(jsString(fB.function_id))}, ${escapeAttr(jsString(fB.function_name || ''))}, ${Number(m.similarity) || 0}, event)"
                            onmousemove="moveCodePreview(event)"
                            onmouseleave="hideDiffPreview(event)"
                            onclick="Nav.openPath(${escapeAttr(jsString(diffUrl))}, event, { title: ${escapeAttr(jsString(`Diff: ${fA.function_name} vs ${fB.function_name}`))}, type: 'diff' })"
                            title="Run Aligned Diff">${(m.similarity * 100).toFixed(1)}%</div>
                    </div>
                    ${EntityRenderer.renderTag('similarity', pairId, m.tags || [], m.user_tags || [])}
                    `;
                } else {
                    similarityHtml = `<span class="mono" style="color:var(--accent); font-weight:bold;">${(m.similarity * 100).toFixed(1)}%</span>`;
                }

                const clusterData = (m.cluster_id || m.cluster_uuid) ? [{
                    cluster_id: m.cluster_id,
                    cluster_uuid: m.cluster_uuid,
                    cluster_name: m.cluster_name,
                    cohesion_score: m.cohesion || 0.0,
                    member_count: m.cluster_member_count || 0,
                    cluster_stability: m.cluster_stability || 0.0,
                    avg_features: m.cluster_avg_features || 0.0,
                    sample_functions: m.cluster_sample_functions || []
                }] : [];
                seedBinSimClusterSamples(clusterData[0]);

                return `
                <tr style="border-bottom: 1px solid var(--border);"
                    data-entity-data='${escapeAttr(JSON.stringify({
                        cluster_id: m.cluster_id,
                        cluster_uuid: m.cluster_uuid,
                        cluster_name: m.cluster_name
                    }))}'
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'bin_cluster', this)">
                    <td style="padding:10px;">
                        ${similarityHtml}
                    </td>
                    <td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">
                        <div style="display:flex; flex-direction:column; gap:6px; max-height:120px; overflow-y:auto;">
                            ${m.func_a ? renderFuncBadge(m.func_a) : ''}
                        </div>
                    </td>
                    <td style="padding:10px; text-align:center;">
                        <div style="display:flex; flex-direction:column; gap:6px; max-height:120px; overflow-y:auto;">
                            ${notesAHtml}
                        </div>
                    </td>
                    <td style="padding:8px; text-align:left; vertical-align:top; min-width:220px;">
                        <div style="display:flex; flex-direction:column; gap:6px; max-height:120px; overflow-y:auto;">
                            ${m.func_b ? renderFuncBadge(m.func_b) : ''}
                        </div>
                    </td>
                    <td style="padding:10px; text-align:center;">
                        <div style="display:flex; flex-direction:column; gap:6px; max-height:120px; overflow-y:auto;">
                            ${notesBHtml}
                        </div>
                    </td>
                    <td style="padding:10px;">
                        <div class="mono dim">${m.sim_rarity.toFixed(2)}</div>
                    </td>
                    <td style="padding:10px;">
                        <div class="mono dim">${(m.avg_features || 0).toFixed(1)}</div>
                    </td>
                    <td class="cluster-cards-cell" data-clusters='${escapeAttr(JSON.stringify(clusterData))}' style="padding:10px;">
                        ${EntityRenderer.renderClusterCard(clusterData)}
                    </td>
                </tr>
                `;
            }).join('');
        } else {
            tbodyMatched.innerHTML = '<tr><td colspan="8" style="text-align:center; padding:20px;">No matched functions</td></tr>';
        }
    }

    const renderUnique = (itemsRaw, tbody, state, prefix) => {
        if (!tbody) return;
        const stateKey = state === binSimSortState.uniqueA ? 'uniqueA' : 'uniqueB';
        const thead = tbody.previousElementSibling;
        if (thead && !isFilterChange) {
            thead.innerHTML = `
                <tr>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort(${escapeAttr(jsString(stateKey))}, 'func_name')">Function <small>${getSortIcon(stateKey, 'func_name')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border); width: 50px;">Notes</th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort(${escapeAttr(jsString(stateKey))}, 'sim_rarity')">Rarity <small>${getSortIcon(stateKey, 'sim_rarity')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort(${escapeAttr(jsString(stateKey))}, 'avg_features')">Features <small>${getSortIcon(stateKey, 'avg_features')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort(${escapeAttr(jsString(stateKey))}, 'cluster_name')">Cluster <small>${getSortIcon(stateKey, 'cluster_name')}</small><div class="resizer"></div></th>
                </tr>
                <tr class="filter-row">
                    <th>${searchHtml(prefix)}</th>
                    <th>${noteFilterHtml(prefix, 'note')}</th>
                    <th>${filterHtml(prefix, 'rar')}</th>
                    <th>${filterHtml(prefix, 'feat')}</th>
                    <th>${searchHtml(prefix + '-cl')}</th>
                </tr>
            `;
            restoreFilters(prefix, ['feat', 'rar'], ['note']);
        }

        // Server already filtered + sorted (incl. cluster-name via cl_q); render as-is.
        const items = itemsRaw || [];

        if (items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding:20px;">No unmatched functions</td></tr>';
            return;
        }
        tbody.innerHTML = items.map(u => {
            const rarity = (u.sim_rarity !== undefined) ? u.sim_rarity : 0;
            const funcs = u.funcs || (u.func_id ? [u.func_id] : []);
            const notesHtml = funcs.map(fid => {
                const fObj = buildFuncObj(fid);
                return `<div style="min-height:24px; display:flex; align-items:center; justify-content:center;">${EntityRenderer.renderNoteButton(fid, fObj.note_owners, { isTable: true, raw_data: fObj })}</div>`;
            }).join('');
            const clusterData = (u.cluster_id || u.cluster_uuid) ? [{
                cluster_id: u.cluster_id,
                cluster_uuid: u.cluster_uuid,
                cluster_name: u.cluster_name,
                cohesion_score: u.cohesion || 1.0,
                member_count: u.cluster_member_count || 0,
                cluster_stability: u.cluster_stability || 0.0,
                avg_features: u.cluster_avg_features || 0.0,
                sample_functions: u.cluster_sample_functions || []
            }] : [];
            seedBinSimClusterSamples(clusterData[0]);
            return `
            <tr style="border-bottom: 1px solid var(--border);"
                data-entity-data='${escapeAttr(JSON.stringify({
                    cluster_id: u.cluster_id,
                    cluster_uuid: u.cluster_uuid,
                    cluster_name: u.cluster_name
                }))}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'bin_cluster', this)">
                <td style="padding:8px;">
                    ${funcs.map(renderFuncBadge).join('')}
                </td>
                <td style="padding:10px; text-align:center;">
                    <div style="display:flex; flex-direction:column; gap:6px;">
                        ${notesHtml}
                    </div>
                </td>
                <td style="padding:10px;">
                    <span class="dim">${rarity.toFixed(2)}</span>
                </td>
                <td style="padding:10px;">
                    <span class="dim">${(u.avg_features || 0).toFixed(0)}</span>
                </td>
                <td class="cluster-cards-cell" data-clusters='${escapeAttr(JSON.stringify(clusterData))}' style="padding:10px;">
                    ${clusterData.length > 0 ? EntityRenderer.renderClusterCard(clusterData) : ''}
                </td>
            </tr>
            `;
        }).join('');
    };

    renderUnique(binSimPage.uniqueA.items, document.getElementById('bin-sim-table-unique-a'), binSimSortState.uniqueA, 'ua');
    renderUnique(binSimPage.uniqueB.items, document.getElementById('bin-sim-table-unique-b'), binSimSortState.uniqueB, 'ub');

    if (!isFilterChange && typeof TableSelection !== 'undefined') {
        new TableSelection('bin-sim-table-matched-table');
        new TableSelection('bin-sim-table-unique-a-table');
        new TableSelection('bin-sim-table-unique-b-table');
    }

    ['matched', 'uniqueA', 'uniqueB'].forEach(setupBinSimInfiniteScroll);
}

// ---- Change 4 (frontend): server paging + infinite scroll for the diff tables ----

function binSimFilterParams(prefix) {
    const val = (id) => (document.getElementById(id)?.value || '').trim();
    const p = {};
    const q = val(`bsim-flt-${prefix}-q`);
    if (q) p.q = q;
    if (prefix === 'matched') {
        const na = val('bsim-flt-matched-note-a'); if (na) p.note_a = na;
        const nb = val('bsim-flt-matched-note-b'); if (nb) p.note_b = nb;
        const smin = val('bsim-flt-matched-coh-min'); if (smin) p.sim_min = smin;
        const smax = val('bsim-flt-matched-coh-max'); if (smax) p.sim_max = smax;
    } else {
        const n = val(`bsim-flt-${prefix}-note`); if (n) p.note = n;
        const clq = val(`bsim-flt-${prefix}-cl-q`); if (clq) p.cl_q = clq;
    }
    const fmin = val(`bsim-flt-${prefix}-feat-min`); if (fmin) p.feat_min = fmin;
    const fmax = val(`bsim-flt-${prefix}-feat-max`); if (fmax) p.feat_max = fmax;
    const rmin = val(`bsim-flt-${prefix}-rar-min`); if (rmin) p.rar_min = rmin;
    const rmax = val(`bsim-flt-${prefix}-rar-max`); if (rmax) p.rar_max = rmax;
    return p;
}

async function loadBinSimTablePage(key, { reset = false } = {}) {
    if (!binSimCtx) return;
    const st = binSimPage[key];
    if (st.loading) return;
    if (!reset && st.items.length >= st.total && st.total > 0) return; // fully loaded
    st.loading = true;
    if (reset) { st.offset = 0; st.items = []; }

    const sort = binSimSortState[key];
    const params = new URLSearchParams({
        view: 'table',  // ignored by backend; documents intent
        table: st.table,
        collection_a: binSimCtx.collection,
        md5_a: binSimCtx.md5a,
        md5_b: binSimCtx.md5b,
        offset: st.offset,
        limit: BINSIM_LIMIT,
        sort_col: sort.col,
        sort_dir: sort.dir === -1 ? 'desc' : 'asc',
        ...binSimFilterParams(st.prefix),
    });
    if (binSimCtx.collB) params.set('collection_b', binSimCtx.collB);
    if (binSimCtx.poolId) params.set('pool', binSimCtx.poolId);

    try {
        const res = await fetch(`/api/diff?${params.toString()}`);
        const data = await res.json();
        Object.assign(binSimDataCache.functions_metadata, data.functions_metadata || {});
        st.items = st.items.concat(data.items || []);
        st.offset = st.items.length;
        st.total = data.total || 0;
        binSimDataCache.diff[st.table] = st.items;
    } catch (e) {
        console.error('bin-sim page load failed', e);
    } finally {
        st.loading = false;
    }
    renderBinSimTables(true);
}

function setupBinSimInfiniteScroll(key) {
    const st = binSimPage[key];
    const tbody = document.getElementById(st.tbody);
    if (!tbody) return;
    const scroller = tbody.closest('.bin-sim-table-scroll') || tbody.closest('[style*="overflow"]') || tbody.parentElement;
    if (!scroller || scroller._binSimScrollBound === key) return;
    scroller._binSimScrollBound = key;
    scroller.addEventListener('scroll', () => {
        if (st.loading || st.items.length >= st.total) return;
        if (scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 200) {
            loadBinSimTablePage(key);
        }
    });
}

window.setSankeyMode = function(mode) {
    sankeyMode = mode;
    const btnDet = document.getElementById('bsim-sankey-btn-detailed');
    const btnSimp = document.getElementById('bsim-sankey-btn-simplified');
    const btnTags = document.getElementById('bsim-sankey-btn-tags');
    if (btnDet && btnSimp) {
        btnDet.classList.toggle('active', mode === 'detailed');
        btnSimp.classList.toggle('active', mode === 'simplified');
    }
    if (btnTags) btnTags.classList.toggle('active', mode === 'tags');
    const splitToggle = document.getElementById('bin-sim-sankey-split-toggle');
    if (splitToggle) {
        splitToggle.style.display = mode === 'detailed' ? 'none' : 'flex';
    }
    if (binSimDataCache) {
        renderBinaryDiffSankey(binSimDataCache);
    }
};

window.setSankeySplit = function(split) {
    sankeySplit = split;
    const splitToggle = document.getElementById('bin-sim-sankey-split-toggle');
    if (splitToggle) {
        const buttons = splitToggle.querySelectorAll('.view-btn');
        buttons.forEach(btn => {
            const clickAttr = btn.getAttribute('onclick') || '';
            const isTarget = clickAttr.includes(`(${split})`);
            btn.classList.toggle('active', isTarget);
        });
    }
    if (binSimDataCache) {
        renderBinaryDiffSankey(binSimDataCache);
    }
};

window.setSankeyScale = function(scale) {
    sankeyScale = scale;
    const btnCount = document.getElementById('bsim-sankey-scale-btn-count');
    const btnFeat = document.getElementById('bsim-sankey-scale-btn-features');
    if (btnCount && btnFeat) {
        btnCount.classList.toggle('active', scale === 'count');
        btnFeat.classList.toggle('active', scale === 'features');
    }
    if (binSimDataCache) {
        renderBinaryDiffSankey(binSimDataCache);
    }
};

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
            params.append(key, val);
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

function renderBinSimPairs(items) {
    if (!items || items.length === 0) return '';
    let html = '';
    const { collection, params } = getRoutingState();

    items.forEach(item => {
        const activeScoreType = params.get('sort') || 'score';
        const score = item[activeScoreType] || item.score || 0;
        const scoreFormatted = (score * 100).toFixed(1) + '%';
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
            <tr class="sim-row">
                <td>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;" onclick="${escapeAttr(onClickHandler)}" title="Open Diff">${scoreFormatted}</div>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="display:flex; align-items:center; overflow:hidden; min-height:24px;" title="${item.file_name_a || ''}">
                            ${EntityRenderer.renderFileName(item.file_name_a, item.md5_a, collA)}
                        </div>
                        <div style="display:flex; align-items:center; overflow:hidden; min-height:24px;" title="${item.file_name_b || ''}">
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
                        <div style="min-height:24px; display:flex; align-items:center;">${covA}</div>
                        <div style="min-height:24px; display:flex; align-items:center;">${covB}</div>
                    </div>
                </td>
                <td class="sim-cell" style="vertical-align:middle;">
                    <div style="display:flex; align-items:center; justify-content:center; height:100%; font-weight:bold;">${shared}</div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collA}:file:${item.md5_a}`, tagsA, userTagsA)}</div>
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collB}:file:${item.md5_b}`, tagsB, userTagsB)}</div>
                    </div>
                </td>
                ${window.renderCollectionCell ? window.renderCollectionCell(collA, collB) : ''}
            </tr>
        `;
    });
    return html;
}

window.applyBinSimSearch = applyBinSimSearch;
window.renderBinSimPairs = renderBinSimPairs;

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
            renderBinSimTables();
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
    el.innerHTML = `
        <a href="${escapeAttr(fileUrl)}" onclick="event.preventDefault(); Nav.openPath(${escapeAttr(jsString(fileUrl))}, event, { title: ${escapeAttr(jsString('File: ' + name))}, type: 'file' });" style="font-weight:bold; color:var(--accent); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:40%; text-decoration:none;" title="${escapeAttr(name)}" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${escapeHtml(name)}</a>
        <span style="display:inline-flex; gap:4px; flex:1; min-width:0; flex-wrap:wrap;">${tags}</span>
        <span style="margin-left:auto;">${noteBtn}</span>
    `;
}

// ---- Tab switching: Matched / Unmatched / Graph / Metadata / Inferred ----
const BIN_SIM_TABS = ['matched', 'unmatched', 'graph', 'metadata', 'inferred', 'filesim'];

// push=true (a real click) writes the tab into the URL hash so it lands in
// browser history; Back/forward then fires hashchange and re-selects the tab.
window.switchBinSimTab = function(tab, push = true) {
    if (!BIN_SIM_TABS.includes(tab)) tab = 'matched';
    BIN_SIM_TABS.forEach(t => {
        const panel = document.getElementById(`bsim-panel-${t}`);
        const btn = document.getElementById(`bin-sim-tab-btn-${t}`);
        if (panel) panel.style.display = (t === tab) ? 'flex' : 'none';
        if (btn) btn.classList.toggle('active', t === tab);
    });
    // Sankey needs a visible (non-zero) container to size itself; render on show.
    if (tab === 'graph' && binSimDataCache) {
        renderBinaryDiffSankey(binSimDataCache);
    }
    if ((tab === 'metadata' || tab === 'inferred') && binSimMetaCtx && !binSimMetaCtx.loaded) loadBinSimMetadata();
    if (tab === 'filesim' && binSimDataCache) renderFileSimTable(binSimDataCache);

    if (push && location.hash.slice(1) !== tab) {
        // pushState (not location.hash=) so the app's hashchange ROUTER doesn't
        // re-render the whole view on every tab click. Adds a history entry;
        // Back/forward fires popstate+hashchange -> the view re-renders and
        // applyBinSimTabFromHash() restores the tab.
        history.pushState(null, '', location.pathname + location.search + '#' + tab);
    }
};

// Select the tab named in the URL hash (default matched). Called on initial
// render and on Back/forward navigation.
function applyBinSimTabFromHash() {
    const tab = location.hash.slice(1);
    window.switchBinSimTab(BIN_SIM_TABS.includes(tab) ? tab : 'matched', false);
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



