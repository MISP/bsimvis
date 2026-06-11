// Binary Similarity View Logic

let binSimDataCache = null;
let sankeyMode = 'simplified';
let sankeyScale = 'count';
let sankeySplit = 10;
let binSimSortState = {
    matched: { col: 'cohesion', dir: -1 },
    uniqueA: { col: 'cohesion', dir: -1 },
    uniqueB: { col: 'cohesion', dir: -1 }
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
    const collection = params.get('collection') || 'main';
    let md5a = params.get('md5_a');
    let md5b = params.get('md5_b');

    // Parse new RESTful URL: /collections/{coll}/files/{md5_a}/vs/{coll_b}/{md5_b}
    if (!md5a || !md5b) {
        const parts = window.location.pathname.split('/').filter(Boolean);
        const hasCol = parts[0] === 'collection' || parts[0] === 'collections';
        const hasFile = parts[2] === 'file' || parts[2] === 'files';
        if (hasCol && hasFile && parts[4] === 'vs') {
            md5a = md5a || decodeURIComponent(parts[3]);
            md5b = md5b || decodeURIComponent(parts[6]);
        }
    }
    
    // Set up layout: Header (Selection/Summary) + Body (Sankey / Tables)
    let html = `
        <div id="bin-sim-results" style="display:none; flex:1; flex-direction:column; padding:20px; min-height:0; overflow-y:auto;">
            <!-- File Metadata Cards -->
            <div id="bin-sim-meta-cards" style="display: flex; gap: 20px; margin-bottom: 5px;">
                <div id="bin-sim-meta-a" style="flex: 1; min-width: 0;"></div>
                <div id="bin-sim-meta-b" style="flex: 1; min-width: 0;"></div>
            </div>

            <!-- Similarity Bar (exactly like function diff) -->
            <div id="bin-sim-bar" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 4px; padding: 8px 15px; margin-bottom: 15px; display: flex; align-items: center; justify-content: center; gap: 20px; font-family: 'Inter', sans-serif; font-size: 0.9rem;">
                <div class="sim-info" style="display: flex; align-items: center; gap: 10px;">
                    <span class="sim-label" style="color: var(--subtle); text-transform: uppercase; font-size: 0.75rem; font-weight: bold; letter-spacing: 0.05em;">Binary Similarity</span>
                    <span id="bin-sim-score-val" class="sim-score" style="color: var(--accent); font-family: 'Consolas', monospace; font-weight: bold; font-size: 1.1rem; min-width: 60px; text-align: center;">--%</span>
                </div>
            </div>
            
            <!-- Sankey Graph Placeholder -->
            <div class="resizable-card" id="bin-sim-sankey-card" style="position:relative; width:100%; height:400px; min-height:200px; margin-bottom:20px; border:1px solid var(--border); background:#121212; border-radius:8px; flex-shrink:0; display:flex; flex-direction:column; overflow:hidden;">
                <div class="view-toggle" id="bin-sim-sankey-mode-toggle" style="position:absolute; top:15px; left:15px; z-index:10; margin:0; align-items:center;">
                    <button class="view-btn ${sankeyMode === 'detailed' ? 'active' : ''}" id="bsim-sankey-btn-detailed" onclick="setSankeyMode('detailed')" title="Show detailed function-level similarities">Detailed</button>
                    <button class="view-btn ${sankeyMode === 'simplified' ? 'active' : ''}" id="bsim-sankey-btn-simplified" onclick="setSankeyMode('simplified')" title="Show simplified cluster-level summary">Simplified</button>
                </div>
                <div class="view-toggle" id="bin-sim-sankey-scale-toggle" style="position:absolute; top:15px; left:210px; z-index:10; margin:0; align-items:center; padding-left:10px;">
                    <span style="font-size:0.7rem; color:#888; margin-right:6px; font-weight:bold; font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;">Scale:</span>
                    <button class="view-btn ${sankeyScale === 'count' ? 'active' : ''}" id="bsim-sankey-scale-btn-count" onclick="setSankeyScale('count')" title="Scale flow by function count">Count</button>
                    <button class="view-btn ${sankeyScale === 'features' ? 'active' : ''}" id="bsim-sankey-scale-btn-features" onclick="setSankeyScale('features')" title="Scale flow by BSim feature count">Features</button>
                </div>
                <div class="view-toggle" id="bin-sim-sankey-split-toggle" style="position:absolute; top:15px; left:410px; z-index:10; margin:0; align-items:center; padding-left:10px; display: ${sankeyMode === 'simplified' ? 'flex' : 'none'};">
                    <span style="font-size:0.7rem; color:#888; margin-right:6px; font-weight:bold; font-family:sans-serif; text-transform:uppercase; letter-spacing:0.5px;">Split:</span>
                    <button class="view-btn ${sankeySplit === 5 ? 'active' : ''}" onclick="setSankeySplit(5)" title="5% granularity (20 bins)">5%</button>
                    <button class="view-btn ${sankeySplit === 10 ? 'active' : ''}" onclick="setSankeySplit(10)" title="10% granularity (10 bins)">10%</button>
                    <button class="view-btn ${sankeySplit === 20 ? 'active' : ''}" onclick="setSankeySplit(20)" title="20% granularity (5 bins)">20%</button>
                    <button class="view-btn ${sankeySplit === 25 ? 'active' : ''}" onclick="setSankeySplit(25)" title="25% granularity (4 bins)">25%</button>
                </div>
                <div id="bin-sim-sankey" style="flex:1; width:100%; min-height:0; overflow-y:auto; position:relative;"></div>
                <div class="drag-handle-v" style="height:8px; background:rgba(255,255,255,0.02); border-top:1px solid var(--border); cursor:ns-resize; display:flex; align-items:center; justify-content:center; transition:background 0.2s;">
                    <div style="width:30px; height:2px; border-radius:1px; background:rgba(255,255,255,0.15); transition:background 0.2s;"></div>
                </div>
            </div>
            
            <div style="display:flex; flex-direction:column; gap:20px; flex:1; min-height:0;">
                <div class="resizable-card" style="border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; height:350px; min-height:200px; overflow:hidden; flex-shrink:0;">
                    <h3 style="margin:0; padding:15px; background:rgba(255,255,255,0.03); border-bottom:1px solid var(--border); color:var(--success);">Matched Clusters</h3>
                    <div style="flex:1; overflow:auto;">
                        <table style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                            <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                                <!-- Rendered dynamically -->
                            </thead>
                            <tbody id="bin-sim-table-matched"></tbody>
                        </table>
                    </div>
                    <div class="drag-handle-v" style="height:8px; background:rgba(255,255,255,0.02); border-top:1px solid var(--border); cursor:ns-resize; display:flex; align-items:center; justify-content:center; transition:background 0.2s;">
                        <div style="width:30px; height:2px; border-radius:1px; background:rgba(255,255,255,0.15); transition:background 0.2s;"></div>
                    </div>
                </div>
                
                <div class="resizable-card" style="display:flex; flex-direction:column; height:350px; min-height:200px; overflow:hidden; flex-shrink:0;">
                    <div style="display:flex; gap:20px; flex:1; overflow:hidden;">
                        <div style="flex:1; border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                            <h3 style="margin:0; padding:15px; background:rgba(255,255,255,0.03); border-bottom:1px solid var(--border); color:var(--accent);">Unique to Binary A</h3>
                            <div style="flex:1; overflow:auto;">
                                <table style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                                    <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                                        <!-- Rendered dynamically -->
                                    </thead>
                                    <tbody id="bin-sim-table-unique-a"></tbody>
                                </table>
                            </div>
                        </div>
                        
                        <div style="flex:1; border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; overflow:hidden;">
                            <h3 style="margin:0; padding:15px; background:rgba(255,255,255,0.03); border-bottom:1px solid var(--border); color:var(--accent);">Unique to Binary B</h3>
                            <div style="flex:1; overflow:auto;">
                                <table style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                                    <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                                        <!-- Rendered dynamically -->
                                    </thead>
                                    <tbody id="bin-sim-table-unique-b"></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                    <div class="drag-handle-v" style="height:8px; background:rgba(255,255,255,0.02); border:1px solid var(--border); border-radius:8px; margin-top:8px; cursor:ns-resize; display:flex; align-items:center; justify-content:center; transition:background 0.2s;">
                        <div style="width:30px; height:2px; border-radius:1px; background:rgba(255,255,255,0.15); transition:background 0.2s;"></div>
                    </div>
                </div>
            </div>
        </div>
        <style>
            .drag-handle-v:hover {
                background: rgba(255,255,255,0.08) !important;
            }
            .drag-handle-v:hover div {
                background: var(--accent) !important;
            }
        </style>
    `;
    
    container.innerHTML = html;
    initResizableCards();
    
    if (md5a && md5b) {
        fetchAndRenderBinaryDiff(collection, md5a, md5b);
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

            async function fetchAndRenderBinaryDiff(collection, md5a, md5b) {

    const resultsEl = document.getElementById('bin-sim-results');
    
    try {
        const res = await fetch(`/api/bin_sim/diff?collection=${encodeURIComponent(collection)}&md5_a=${encodeURIComponent(md5a)}&md5_b=${encodeURIComponent(md5b)}`);
        if (!res.ok) {
            let errMsg = "Failed to fetch similarity comparison";
            try {
                const errData = await res.json();
                if (errData && errData.message) errMsg = errData.message;
            } catch (e) {}
            throw new Error(errMsg);
        }
        const data = await res.json();
        
        // Render Summary
        const scoreVal = document.getElementById('bin-sim-score-val');
        if (scoreVal) scoreVal.textContent = (data.score * 100).toFixed(1) + '%';
        
        resultsEl.style.display = 'flex';
        
        // Render File Metadata Cards
        if (typeof renderFileMetadata === 'function') {
            const col = collection;
            if (data.file_metadata_a) {
                renderFileMetadata('bin-sim-meta-a', data.file_metadata_a, `${col}:file:${md5a}`, { side: 'l' });
            }
            if (data.file_metadata_b) {
                renderFileMetadata('bin-sim-meta-b', data.file_metadata_b, `${col}:file:${md5b}`, { side: 'r' });
            }
        }
        
        // Save comparison data to cache
        binSimDataCache = data;

        // Render Tables
        renderBinSimTables();
        
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


function renderBinaryDiffSankey(data) {
    const container = document.getElementById('bin-sim-sankey');
    container.innerHTML = '';
    
    if (!data.diff) {
        container.innerHTML = '<div style="display:flex; align-items:center; justify-content:center; height:100%; color:var(--dim);">No diff data available</div>';
        return;
    }

    const detailedBtn = document.getElementById('bsim-sankey-btn-detailed');
    if (detailedBtn) {
        detailedBtn.disabled = false;
        detailedBtn.classList.remove('disabled');
        detailedBtn.title = "Show detailed function-level similarities";
        detailedBtn.style.opacity = 1.0;
        detailedBtn.style.cursor = 'pointer';
    }
    
    const width = container.clientWidth;
    
    const rawMatched = data.diff.matched || [];
    const rawUniqueA = data.diff.unique_to_a || [];
    const rawUniqueB = data.diff.unique_to_b || [];
    
    let maxNodesInColumn = 10;
    if (sankeyMode !== 'simplified') {
        const groupA_count = rawMatched.filter(m => m.funcs_a && m.funcs_a.length > 0).length + 
                             rawUniqueA.filter(u => u.funcs && u.funcs.length > 0).length;
        const groupB_count = rawMatched.filter(m => m.funcs_b && m.funcs_b.length > 0).length + 
                             rawUniqueB.filter(u => u.funcs && u.funcs.length > 0).length;
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
            const meta = (data && data.functions_metadata) ? data.functions_metadata[fid] : null;
            return Math.max(1, (meta && meta.bsim_features_count) ? parseInt(meta.bsim_features_count) : 1);
        }
        return 1;
    };

    const sumFuncsValue = (funcs) => {
        return (funcs || []).reduce((sum, fid) => sum + getFuncValue(fid), 0);
    };
    
    const getFuncDisplayName = (fid) => {
        const meta = (data && data.functions_metadata) ? data.functions_metadata[fid] : null;
        if (meta && meta.name) {
            return meta.name;
        }
        const parts = fid.split(':');
        return '@' + parts.pop();
    };

    const filteredMatched = typeof applyFilters === 'function' ? applyFilters(data.diff.matched || [], 'matched') : (data.diff.matched || []);
    const filteredUniqueA = typeof applyFilters === 'function' ? applyFilters(data.diff.unique_to_a || [], 'ua') : (data.diff.unique_to_a || []);
    const filteredUniqueB = typeof applyFilters === 'function' ? applyFilters(data.diff.unique_to_b || [], 'ub') : (data.diff.unique_to_b || []);

    const sortedMatched = typeof sortItems === 'function' ? sortItems([...filteredMatched], binSimSortState.matched) : filteredMatched;
    const sortedUniqueA = typeof sortItems === 'function' ? sortItems([...filteredUniqueA], binSimSortState.uniqueA) : filteredUniqueA;
    const sortedUniqueB = typeof sortItems === 'function' ? sortItems([...filteredUniqueB], binSimSortState.uniqueB) : filteredUniqueB;

    const matchedRank = new Map(sortedMatched.map((m, idx) => [m.cluster_uuid, idx]));
    const uniqueARank = new Map(sortedUniqueA.map((u, idx) => [u.cluster_uuid, idx]));
    const uniqueBRank = new Map(sortedUniqueB.map((u, idx) => [u.cluster_uuid, idx]));

    if (sankeyMode === 'simplified') {
        const sortCol = (binSimSortState.matched && binSimSortState.matched.col) || 'cohesion';
        const groupCol = sortCol === 'cluster_name' ? 'cohesion' : sortCol;

        let minVal = 0.0;
        let maxVal = 1.0;
        if (groupCol === 'avg_features' || groupCol === 'count_a' || groupCol === 'count_b') {
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
            
            const wA = sumFuncsValue(m.funcs_a);
            const wB = sumFuncsValue(m.funcs_b);
            const cohesion = m.cohesion || 0;
            
            bins[binIdx].clusters.push(m);
            bins[binIdx].totalA += wA;
            bins[binIdx].totalB += wB;
            bins[binIdx].sumCohesion += cohesion * (wA + wB);
            bins[binIdx].sumWeights += (wA + wB);
        });

        let totalMatchedA = 0;
        let totalMatchedB = 0;
        bins.forEach(b => {
            totalMatchedA += b.totalA;
            totalMatchedB += b.totalB;
        });

        let totalUniqueA = 0;
        sortedUniqueA.forEach(u => {
            totalUniqueA += sumFuncsValue(u.funcs);
        });

        let totalUniqueB = 0;
        sortedUniqueB.forEach(u => {
            totalUniqueB += sumFuncsValue(u.funcs);
        });

        const metricSuffix = sankeyScale === 'features' ? 'feats' : 'funcs';

        const getBinName = (binIdx, countText, prefix) => {
            const stepVal = (maxVal - minVal) / numBins;
            const low = minVal + binIdx * stepVal;
            const high = minVal + (binIdx + 1) * stepVal;
            
            let label = '';
            if (groupCol === 'cohesion' || groupCol === 'sim_rarity') {
                const lowPct = Math.round(low * 100);
                const highPct = Math.round(high * 100);
                const colName = groupCol === 'cohesion' ? 'Cohesion' : 'Rarity';
                if (prefix === 'a') {
                    label = `A Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                } else if (prefix === 'b') {
                    label = `B Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                } else {
                    label = `Matched ${colName} ${lowPct}%-${highPct}% (${countText})`;
                }
            } else {
                const lowNum = Math.round(low);
                const highNum = Math.round(high);
                const colName = groupCol === 'avg_features' ? 'Avg Feat' : (groupCol === 'count_a' ? 'Funcs A' : 'Funcs B');
                if (prefix === 'a') {
                    label = `A Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                } else if (prefix === 'b') {
                    label = `B Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                } else {
                    label = `Matched ${colName} ${lowNum}-${highNum} (${countText})`;
                }
            }
            return label;
        };

        bins.forEach(b => {
            if (b.clusters.length === 0) return;

            const binAvgCohesion = b.sumWeights > 0 ? (b.sumCohesion / b.sumWeights) : (b.binIdx * (step / 100) + (step / 200));
            const binColor = `hsl(${binAvgCohesion * 120}, 70%, 55%)`;

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

        let nodeA_unique, nodeC_uniqueA, nodeC_uniqueB, nodeB_unique;
        if (totalUniqueA > 0) {
            nodeA_unique = getNode('simplified_a_unique', `A Unique (${totalUniqueA} ${metricSuffix})`, '#f92672');
            nodeA_unique.alignOverride = 0;
            nodeC_uniqueA = getNode('simplified_c_uniqueA', `Unique to A (${sortedUniqueA.length})`, '#f92672');
            nodeC_uniqueA.alignOverride = 1;
            links.push({ source: nodeA_unique.index, target: nodeC_uniqueA.index, value: totalUniqueA });
        }
        if (totalUniqueB > 0) {
            nodeC_uniqueB = getNode('simplified_c_uniqueB', `Unique to B (${sortedUniqueB.length})`, '#66d9ef');
            nodeC_uniqueB.alignOverride = 1;
            nodeB_unique = getNode('simplified_b_unique', `B Unique (${totalUniqueB} ${metricSuffix})`, '#66d9ef');
            nodeB_unique.alignOverride = 2;
            links.push({ source: nodeC_uniqueB.index, target: nodeB_unique.index, value: totalUniqueB });
        }
    } else {
        // 1. Matched Clusters
        sortedMatched.forEach(m => {
            const cohesion = m.cohesion || 0;
            const cColor = `hsl(${cohesion * 120}, 70%, 55%)`;
            const cNode = getNode('cluster_' + m.cluster_uuid, m.cluster_name, cColor);
            cNode.cohesion = cohesion;
            cNode.cluster_uuid = m.cluster_uuid;
            cNode.cluster_name = m.cluster_name;
            cNode.size = m.count_a + m.count_b;
            cNode.stability = 1.0;
            cNode.avg_features = m.avg_features || 0;
            
            if (m.funcs_a && m.funcs_a.length > 0) {
                const fNames = m.funcs_a.map(fa => getFuncDisplayName(fa));
                const fNodeId = 'funcgroup_a_' + m.cluster_uuid;
                funcParentMap.set(fNodeId, m.cluster_uuid);
                const fNode = getNode(fNodeId, fNames, cColor, m.funcs_a);
                fNode.cohesion = cohesion;
                links.push({ source: fNode.index, target: cNode.index, value: sumFuncsValue(m.funcs_a) });
            }
            
            if (m.funcs_b && m.funcs_b.length > 0) {
                const fNames = m.funcs_b.map(fb => getFuncDisplayName(fb));
                const fNodeId = 'funcgroup_b_' + m.cluster_uuid;
                funcParentMap.set(fNodeId, m.cluster_uuid);
                const fNode = getNode(fNodeId, fNames, cColor, m.funcs_b);
                fNode.cohesion = cohesion;
                links.push({ source: cNode.index, target: fNode.index, value: sumFuncsValue(m.funcs_b) });
            }
        });
        
        // 2. Unique to A Clusters
        sortedUniqueA.forEach(u => {
            const cNode = getNode('cluster_' + u.cluster_uuid, u.cluster_name, '#f92672');
            cNode.cluster_uuid = u.cluster_uuid;
            cNode.cluster_name = u.cluster_name;
            cNode.size = u.funcs.length;
            cNode.stability = 1.0;
            cNode.cohesion = u.cohesion || 0;
            cNode.avg_features = u.avg_features || 0;
            
            if (u.funcs && u.funcs.length > 0) {
                const fNames = u.funcs.map(fa => getFuncDisplayName(fa));
                const fNodeId = 'funcgroup_a_' + u.cluster_uuid;
                funcParentMap.set(fNodeId, u.cluster_uuid);
                const fNode = getNode(fNodeId, fNames, '#f92672', u.funcs);
                links.push({ source: fNode.index, target: cNode.index, value: sumFuncsValue(u.funcs) });
            }
        });
        
        // 3. Unique to B Clusters
        sortedUniqueB.forEach(u => {
            const cNode = getNode('cluster_' + u.cluster_uuid, u.cluster_name, '#66d9ef');
            cNode.cluster_uuid = u.cluster_uuid;
            cNode.cluster_name = u.cluster_name;
            cNode.size = u.funcs.length;
            cNode.stability = 1.0;
            cNode.cohesion = u.cohesion || 0;
            cNode.avg_features = u.avg_features || 0;
            
            if (u.funcs && u.funcs.length > 0) {
                const fNames = u.funcs.map(fb => getFuncDisplayName(fb));
                const fNodeId = 'funcgroup_b_' + u.cluster_uuid;
                funcParentMap.set(fNodeId, u.cluster_uuid);
                const fNode = getNode(fNodeId, fNames, '#66d9ef', u.funcs);
                links.push({ source: cNode.index, target: fNode.index, value: sumFuncsValue(u.funcs) });
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
        .attr("fill", d => d.target.color || '#fff')
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
                    .attr("stroke", "rgba(0,0,0,0.5)")
                    .attr("stroke-width", "0.5px")
                    .attr("opacity", 0.6);
                    
                const name = d.name[idx];
                el.append("text")
                    .attr("x", d.id.startsWith('funcgroup_b_') ? -6 : 6 + width)
                    .attr("y", currentY + h / 2)
                    .attr("dy", "0.35em")
                    .attr("text-anchor", d.id.startsWith('funcgroup_b_') ? "end" : "start")
                    .text(name)
                    .attr("fill", "#fff")
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
                .attr("stroke", "rgba(0,0,0,0.5)")
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
                .attr("fill", "#fff")
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
    renderBinSimTables();
}

function binSimFilterChange(shouldApply = false) {
    const prefixes = ['matched', 'ua', 'ub'];
    prefixes.forEach(prefix => {
        const suffixes = prefix === 'matched' ? ['feat', 'ca', 'cb', 'coh', 'rar'] : ['feat', 'c', 'coh'];
        suffixes.forEach(suffix => {
            const minEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-min`);
            const maxEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-max`);
            if (minEl) window[`bsim-flt-${prefix}-${suffix}-min-val`] = minEl.value;
            if (maxEl) window[`bsim-flt-${prefix}-${suffix}-max-val`] = maxEl.value;
        });
    });

    if (shouldApply) {
        renderBinSimTables(true);
    }
}

const applyFilters = (items, prefix) => {
    return items.filter(item => {
        const count = item.count_a !== undefined ? Math.max(item.count_a, item.count_b) : item.funcs.length;
        const caMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-ca-min`)?.value || document.getElementById(`bsim-flt-${prefix}-c-min`)?.value);
        const caMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-ca-max`)?.value || document.getElementById(`bsim-flt-${prefix}-c-max`)?.value);
        
        if (!isNaN(caMin) && count < caMin) return false;
        if (!isNaN(caMax) && count > caMax) return false;

        if (item.count_b !== undefined) {
            const cbMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-cb-min`)?.value);
            const cbMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-cb-max`)?.value);
            if (!isNaN(cbMin) && item.count_b < cbMin) return false;
            if (!isNaN(cbMax) && item.count_b > cbMax) return false;
        }

        const cohMin = parseFloat(document.getElementById(`bsim-flt-${prefix}-coh-min`)?.value);
        const cohMax = parseFloat(document.getElementById(`bsim-flt-${prefix}-coh-max`)?.value);
        if (!isNaN(cohMin) && item.cohesion < cohMin) return false;
        if (!isNaN(cohMax) && item.cohesion > cohMax) return false;
        
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

function renderBinSimTables(isFilterChange = false) {
    if (!binSimDataCache || !binSimDataCache.diff) return;
    const data = binSimDataCache;

    const renderFuncBadge = (fid) => {
        const parts = fid.split(':');
        const entry = parts.pop();
        const md5 = parts.pop();
        const type = parts.pop();
        const col = parts.join(':');
        
        const meta = (binSimDataCache && binSimDataCache.functions_metadata) ? binSimDataCache.functions_metadata[fid] : null;
        const name = meta && meta.name ? meta.name : ('sub_' + entry);
        const retType = meta && meta.return_type ? meta.return_type : 'void';
        const params = meta && meta.parameters ? (Array.isArray(meta.parameters) ? meta.parameters : [meta.parameters]).join(', ') : '';
        const titleStr = `${retType} ${name}(${params})`;
        
        const cleanName = name.replace(/'/g, "\\'");
        
        const fData = {
            function_id: fid,
            function_name: name,
            file_md5: md5,
            entrypoint_address: entry,
            collection: col
        };
        
        return `<span class="badge clickable" title="${titleStr}" style="background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1); margin:2px;" 
            data-entity-data='${JSON.stringify(fData).replace(/'/g, "&apos;")}'
            onmouseenter="showCodePreview('${fid}', '${cleanName}', '${cleanName}', '${md5}', 0, event)" 
            onmouseleave="hideCodePreview(event)" 
            onmousemove="moveCodePreview(event)"
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)"
            onclick="showFunctionCodeById('${fid}', '${cleanName}', '', event)">${name}</span>`;
    };



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

    const restoreFilters = (prefix, suffixes) => {
        suffixes.forEach(suffix => {
            const minEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-min`);
            const maxEl = document.getElementById(`bsim-flt-${prefix}-${suffix}-max`);
            if (minEl && window[`bsim-flt-${prefix}-${suffix}-min-val`]) minEl.value = window[`bsim-flt-${prefix}-${suffix}-min-val`];
            if (maxEl && window[`bsim-flt-${prefix}-${suffix}-max-val`]) maxEl.value = window[`bsim-flt-${prefix}-${suffix}-max-val`];
        });
    };

    const tbodyMatched = document.getElementById('bin-sim-table-matched');
    if (tbodyMatched) {
        const thead = tbodyMatched.previousElementSibling;
        if (thead && !isFilterChange) {
            thead.innerHTML = `
                <tr>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'cluster_name')">Cluster <small>${getSortIcon('matched', 'cluster_name')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'avg_features')">Avg Feat <small>${getSortIcon('matched', 'avg_features')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'count_a')">Funcs A <small>${getSortIcon('matched', 'count_a')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'count_b')">Funcs B <small>${getSortIcon('matched', 'count_b')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'cohesion')">Cohesion <small>${getSortIcon('matched', 'cohesion')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('matched', 'sim_rarity')">Sim Rarity <small>${getSortIcon('matched', 'sim_rarity')}</small><div class="resizer"></div></th>
                </tr>
                <tr class="filter-row">
                    <th></th>
                    <th>${filterHtml('matched', 'feat')}</th>
                    <th>${filterHtml('matched', 'ca')}</th>
                    <th>${filterHtml('matched', 'cb')}</th>
                    <th>${filterHtml('matched', 'coh')}</th>
                    <th>${filterHtml('matched', 'rar')}</th>
                </tr>
            `;
            restoreFilters('matched', ['feat', 'ca', 'cb', 'coh', 'rar']);
        }
    }

    if (data.diff.matched) {
        let matched = applyFilters(data.diff.matched, 'matched');
        matched = sortItems(matched, binSimSortState.matched);
        
        if (matched.length > 0) {
            tbodyMatched.innerHTML = matched.map(m => {
                const cleanName = m.cluster_name.replace(/'/g, "\\'");
                const escUuid = m.cluster_uuid;
                const size = m.count_a + m.count_b;
                const cohesion = m.cohesion || 0;
                const avgFeat = m.avg_features || 0;
                return `
                <tr style="border-bottom:1px solid rgba(255,255,255,0.05);"
                    data-entity-data='${JSON.stringify({
                        cluster_id: m.cluster_id,
                        cluster_uuid: m.cluster_uuid,
                        cluster_name: m.cluster_name
                    }).replace(/'/g, "&apos;")}'
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'bin_cluster', this)">
                    <td style="padding:10px;">
                        <div class="clickable" style="font-weight:bold; color:var(--accent);"
                             onmouseenter="if(window.parent && window.parent.showClusterTableTooltipFromIframe && window.parent !== window) { window.parent.showClusterTableTooltipFromIframe(window.name, '${escUuid}', '${cleanName}', ${size}, 1.0, ${cohesion}, ${avgFeat}, event); } else if(window.showClusterTableTooltip) { window.showClusterTableTooltip(event, '${escUuid}', '${cleanName}', ${size}, 1.0, ${cohesion}, ${avgFeat}); }"
                             onmousemove="if(window.parent && window.parent.moveClusterTableTooltipFromIframe && window.parent !== window) { window.parent.moveClusterTableTooltipFromIframe(window.name, event); } else if(window.moveClusterTableTooltip) { window.moveClusterTableTooltip(event); }"
                             onmouseleave="if(window.parent && window.parent.hideClusterTableTooltipFromIframe && window.parent !== window) { window.parent.hideClusterTableTooltipFromIframe(); } else if(window.hideClusterTableTooltip) { window.hideClusterTableTooltip(); }"
                             onclick="openClusterView('${escUuid}', '${cleanName}', event)">
                             ${m.cluster_name}
                        </div>
                        <div class="mono dim" style="font-size:0.65rem;">UUID: ${m.cluster_uuid}</div>
                    </td>
                    <td style="padding:10px; text-align:center;">
                        <div class="mono dim">${(m.avg_features || 0).toFixed(1)}</div>
                    </td>
                    <td style="padding:10px; text-align:center;">
                        <div style="margin-bottom:4px; font-weight:bold;">${m.count_a}</div>
                        <div style="display:flex; flex-wrap:wrap; justify-content:center; max-height:60px; overflow-y:auto;">
                            ${m.funcs_a.map(renderFuncBadge).join('')}
                        </div>
                    </td>
                    <td style="padding:10px; text-align:center;">
                        <div style="margin-bottom:4px; font-weight:bold;">${m.count_b}</div>
                        <div style="display:flex; flex-wrap:wrap; justify-content:center; max-height:60px; overflow-y:auto;">
                            ${m.funcs_b.map(renderFuncBadge).join('')}
                        </div>
                    </td>
                    <td style="padding:10px;">
                        <div style="display:flex; align-items:center; gap:8px;">
                            <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:40px;">
                                <div style="height:100%; background:var(--info); width:${(m.cohesion * 100).toFixed(0)}%"></div>
                            </div>
                            <span class="dim">${m.cohesion.toFixed(2)}</span>
                        </div>
                    </td>
                    <td style="padding:10px;">
                        <div class="mono dim">${m.sim_rarity.toFixed(2)}</div>
                    </td>
                </tr>
                `;
            }).join('');
        } else {
            tbodyMatched.innerHTML = '<tr><td colspan="6" style="text-align:center; padding:20px;">No matched clusters</td></tr>';
        }
    }

    const renderUnique = (itemsRaw, tbody, state, prefix) => {
        const stateKey = state === binSimSortState.uniqueA ? 'uniqueA' : 'uniqueB';
        const thead = tbody.previousElementSibling;
        if (thead && !isFilterChange) {
            thead.innerHTML = `
                <tr>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('${stateKey}', 'cluster_name')">Cluster <small>${getSortIcon(stateKey, 'cluster_name')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('${stateKey}', 'avg_features')">Avg Feat <small>${getSortIcon(stateKey, 'avg_features')}</small><div class="resizer"></div></th>
                    <th style="text-align:center; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('${stateKey}', 'count')">Functions <small>${getSortIcon(stateKey, 'count')}</small><div class="resizer"></div></th>
                    <th style="text-align:left; padding:10px; border-bottom:1px solid var(--border);" class="sortable resizable-th" onclick="setBinSimSort('${stateKey}', 'cohesion')">Cohesion <small>${getSortIcon(stateKey, 'cohesion')}</small><div class="resizer"></div></th>
                </tr>
                <tr class="filter-row">
                    <th></th>
                    <th>${filterHtml(prefix, 'feat')}</th>
                    <th>${filterHtml(prefix, 'c')}</th>
                    <th>${filterHtml(prefix, 'coh')}</th>
                </tr>
            `;
            restoreFilters(prefix, ['feat', 'c', 'coh']);
        }

        let items = applyFilters(itemsRaw || [], prefix);
        items = sortItems(items, state);
        
        if (items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding:20px;">No unique clusters</td></tr>';
            return;
        }
        tbody.innerHTML = items.map(u => {
            const cleanName = u.cluster_name.replace(/'/g, "\\'");
            const escUuid = u.cluster_uuid;
            const size = u.funcs.length;
            const cohesion = u.cohesion || 0;
            const avgFeat = u.avg_features || 0;
            return `
            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                <td style="padding:10px;">
                    <div class="clickable" style="font-weight:bold; color:var(--accent);"
                         onmouseenter="if(window.parent && window.parent.showClusterTableTooltipFromIframe && window.parent !== window) { window.parent.showClusterTableTooltipFromIframe(window.name, '${escUuid}', '${cleanName}', ${size}, 1.0, ${cohesion}, ${avgFeat}, event); } else if(window.showClusterTableTooltip) { window.showClusterTableTooltip(event, '${escUuid}', '${cleanName}', ${size}, 1.0, ${cohesion}, ${avgFeat}); }"
                         onmousemove="if(window.parent && window.parent.moveClusterTableTooltipFromIframe && window.parent !== window) { window.parent.moveClusterTableTooltipFromIframe(window.name, event); } else if(window.moveClusterTableTooltip) { window.moveClusterTableTooltip(event); }"
                         onmouseleave="if(window.parent && window.parent.hideClusterTableTooltipFromIframe && window.parent !== window) { window.parent.hideClusterTableTooltipFromIframe(); } else if(window.hideClusterTableTooltip) { window.hideClusterTableTooltip(); }"
                         onclick="openClusterView('${escUuid}', '${cleanName}', event)">
                         ${u.cluster_name}
                    </div>
                    <div class="mono dim" style="font-size:0.65rem;">UUID: ${u.cluster_uuid}</div>
                </td>
                <td style="padding:10px; text-align:center;">
                    <div class="mono dim">${(u.avg_features || 0).toFixed(1)}</div>
                </td>
                <td style="padding:10px; text-align:center;">
                    <div style="margin-bottom:4px; font-weight:bold;">${u.funcs.length}</div>
                    <div style="display:flex; flex-wrap:wrap; justify-content:center; max-height:60px; overflow-y:auto;">
                        ${u.funcs.map(renderFuncBadge).join('')}
                    </div>
                </td>
                <td style="padding:10px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:40px;">
                            <div style="height:100%; background:var(--info); width:${(u.cohesion * 100).toFixed(0)}%"></div>
                        </div>
                        <span class="dim">${u.cohesion.toFixed(2)}</span>
                    </div>
                </td>
            </tr>
            `;
        }).join('');
    };

    renderUnique(data.diff.unique_to_a, document.getElementById('bin-sim-table-unique-a'), binSimSortState.uniqueA, 'ua');
    renderUnique(data.diff.unique_to_b, document.getElementById('bin-sim-table-unique-b'), binSimSortState.uniqueB, 'ub');

    renderBinaryDiffSankey(data);
}

window.setSankeyMode = function(mode) {
    sankeyMode = mode;
    const btnDet = document.getElementById('bsim-sankey-btn-detailed');
    const btnSimp = document.getElementById('bsim-sankey-btn-simplified');
    if (btnDet && btnSimp) {
        btnDet.classList.toggle('active', mode === 'detailed');
        btnSimp.classList.toggle('active', mode === 'simplified');
    }
    const splitToggle = document.getElementById('bin-sim-sankey-split-toggle');
    if (splitToggle) {
        splitToggle.style.display = mode === 'simplified' ? 'flex' : 'none';
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
        
        const diffUrl = `/collection/${collection}/file/${item.md5_a}/vs/${collection}/${item.md5_b}`;
        const safeNameA = (item.file_name_a || 'Unknown').replace(/'/g, "\\'").replace(/"/g, "&quot;");
        const safeNameB = (item.file_name_b || 'Unknown').replace(/'/g, "\\'").replace(/"/g, "&quot;");
        const onClickHandler = `Nav.openPath('${diffUrl}', event, { title: 'Bin Diff: ${safeNameA} vs ${safeNameB}', type: 'bin_sim' });`;

        html += `
            <tr class="sim-row">
                <td>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="font-size:1.1rem; font-weight:bold; color:var(--success);">${scoreFormatted}</div>
                        <button class="btn-diff-action" onclick="${onClickHandler}" title="Open Diff" style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;">
                            <span>±</span>
                        </button>
                    </div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="display:flex; align-items:center; overflow:hidden; min-height:24px;" title="${item.file_name_a || ''}">
                            <b style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${item.file_name_a || 'Unknown'}</b>
                        </div>
                        <div style="display:flex; align-items:center; overflow:hidden; min-height:24px;" title="${item.file_name_b || ''}">
                            <b style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${item.file_name_b || 'Unknown'}</b>
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
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collection}:file:${item.md5_a}`, tagsA, userTagsA)}</div>
                        <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${collection}:file:${item.md5_b}`, tagsB, userTagsB)}</div>
                    </div>
                </td>
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
            const col = parts[0] || 'main';
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


