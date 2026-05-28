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

function renderBinarySimilarityView(params) {
    const container = document.getElementById('binary-similarity-container');
    const collection = params.get('collection') || 'main';
    const md5a = params.get('md5_a');
    const md5b = params.get('md5_b');
    
    // Set up layout: Header (Selection/Summary) + Body (Sankey / Tables)
    let html = `
        <div style="padding: 20px; border-bottom: 1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                <div>
                    <h2 style="margin:0 0 10px 0; color:var(--accent);">Binary Similarity</h2>
                    <div style="display:flex; gap: 15px; align-items:center;">
                        <div>
                            <label class="dim" style="font-size:0.8rem; display:block; margin-bottom:5px;">Binary A</label>
                            <select id="bin-sim-md5-a" class="form-input" style="width:250px; font-family:monospace; padding:8px;">
                                <option value="${md5a || ''}">${md5a || '-- Select Binary --'}</option>
                            </select>
                        </div>
                        <div style="font-size:1.5rem; color:var(--dim); padding-top:20px;">↔</div>
                        <div>
                            <label class="dim" style="font-size:0.8rem; display:block; margin-bottom:5px;">Binary B</label>
                            <select id="bin-sim-md5-b" class="form-input" style="width:250px; font-family:monospace; padding:8px;">
                                <option value="${md5b || ''}">${md5b || '-- Select Binary --'}</option>
                            </select>
                        </div>
                        <div style="padding-top:20px;">
                            <button class="btn-primary" onclick="triggerBinarySimilarityDiff('${collection}')">Compare</button>
                        </div>
                    </div>
                </div>
                <div id="bin-sim-summary" style="display:none; text-align:right; background:rgba(166,226,46,0.1); border:1px solid var(--success); padding:10px 15px; border-radius:8px;">
                    <!-- Filled dynamically -->
                </div>
            </div>
        </div>
        
        <div id="bin-sim-results" style="display:none; flex:1; flex-direction:column; padding:20px; min-height:0;">
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
                <button id="bin-sim-sankey-reset-zoom" class="clickable" style="position:absolute; top:15px; right:15px; padding:6px 12px; font-size:0.75rem; background:rgba(0,0,0,0.8); border:1px solid var(--border); color:#fff; border-radius:4px; z-index:10; display:none; transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.1)'" onmouseout="this.style.background='rgba(0,0,0,0.8)'" onclick="resetSankeyZoom()">Reset View</button>
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

function triggerBinarySimilarityDiff(collection) {
    const md5a = document.getElementById('bin-sim-md5-a').value.trim();
    const md5b = document.getElementById('bin-sim-md5-b').value.trim();
    
    if (!md5a || !md5b) {
        alert("Please provide both MD5 hashes.");
        return;
    }
    
    const [hashPath] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams();
    params.set('collection', collection);
    params.set('md5_a', md5a);
    params.set('md5_b', md5b);
    
    window.location.hash = `${hashPath}?${params.toString()}`;
}

async function fetchAndRenderBinaryDiff(collection, md5a, md5b) {
    const summaryEl = document.getElementById('bin-sim-summary');
    const resultsEl = document.getElementById('bin-sim-results');
    
    summaryEl.style.display = 'block';
    summaryEl.innerHTML = `<i class="fa-solid fa-circle-notch fa-spin"></i> Computing Diff...`;
    
    try {
        const res = await fetch(`/api/bin_sim/diff?collection=${encodeURIComponent(collection)}&md5_a=${encodeURIComponent(md5a)}&md5_b=${encodeURIComponent(md5b)}`);
        if (!res.ok) throw new Error("Failed to fetch diff");
        const data = await res.json();
        
        // Render Summary
        summaryEl.innerHTML = `
            <div style="font-size:0.8rem; color:var(--success); text-transform:uppercase; font-weight:bold; letter-spacing:1px; margin-bottom:5px;">Similarity Score</div>
            <div style="font-size:2rem; color:var(--success); font-weight:bold; line-height:1;">${(data.score * 100).toFixed(1)}%</div>
            <div style="font-size:0.75rem; color:var(--dim); margin-top:5px;">
                Weighted Sim: ${(data.score_sim_weighted * 100).toFixed(1)}% | Weighted Col: ${(data.score_collection_weighted * 100).toFixed(1)}%
            </div>
        `;
        
        resultsEl.style.display = 'flex';
        
        // Save comparison data to cache
        binSimDataCache = data;
        
        // Fetch files to populate dropdowns
        fetch(`/api/file/search?collection=${encodeURIComponent(collection)}&limit=1000`)
            .then(res => res.json())
            .then(fileData => {
                const files = fileData.files || [];
                const selA = document.getElementById('bin-sim-md5-a');
                const selB = document.getElementById('bin-sim-md5-b');
                
                let optionsHtml = '<option value="">-- Select Binary --</option>';
                files.forEach(f => {
                    optionsHtml += `<option value="${f.file_md5}">${f.file_name} (${f.file_md5.substring(0,8)})</option>`;
                });
                
                if(selA) selA.innerHTML = optionsHtml;
                if(selB) selB.innerHTML = optionsHtml;
                
                // Also add files from the queue if they aren't in the fetched list
                const queue = JSON.parse(localStorage.getItem('bsim_file_diff_queue') || '[]');
                queue.forEach(q => {
                    const md5 = q.id.split(':').pop();
                    if (selA && !selA.querySelector(`option[value="${md5}"]`)) {
                        selA.innerHTML += `<option value="${md5}">${q.name} (${md5.substring(0,8)})</option>`;
                    }
                    if (selB && !selB.querySelector(`option[value="${md5}"]`)) {
                        selB.innerHTML += `<option value="${md5}">${q.name} (${md5.substring(0,8)})</option>`;
                    }
                });

                if (md5a && selA) {
                    if (!selA.querySelector(`option[value="${md5a}"]`)) {
                        selA.innerHTML += `<option value="${md5a}">${md5a}</option>`;
                    }
                    selA.value = md5a;
                }
                if (md5b && selB) {
                    if (!selB.querySelector(`option[value="${md5b}"]`)) {
                        selB.innerHTML += `<option value="${md5b}">${md5b}</option>`;
                    }
                    selB.value = md5b;
                }
            });

        // Render Tables
        renderBinSimTables();
        
    } catch(err) {
        console.error(err);
        summaryEl.innerHTML = `<span style="color:var(--danger)">Error: ${err.message}</span>`;
    }
}

window.resetSankeyZoom = () => {
    const svg = d3.select('#bin-sim-sankey svg');
    if (!svg.empty() && window.sankeyZoomBehavior) {
        svg.transition().duration(500).call(
            window.sankeyZoomBehavior.transform,
            d3.zoomIdentity
        );
    }
};

function renderBinaryDiffSankey(data) {
    const container = document.getElementById('bin-sim-sankey');
    container.innerHTML = '';
    
    const resetBtn = document.getElementById('bin-sim-sankey-reset-zoom');
    if (resetBtn) resetBtn.style.display = 'none';
    
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
    
    if (resetBtn) resetBtn.style.display = 'block';
    
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
        .attr('height', height)
        .style('cursor', 'grab');
        
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
        .style("transition", "fill-opacity 0.2s")
        .on("mouseover", function() { 
            d3.select(this).style("fill-opacity", 0.8); 
        })
        .on("mouseout", function() { 
            d3.select(this).style("fill-opacity", 0.4); 
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
            el.append("rect")
                .attr("height", height)
                .attr("width", width)
                .attr("fill", d.color)
                .attr("stroke", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? "#fff" : "rgba(0,0,0,0.5)")
                .attr("stroke-width", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? "1.5px" : "0.5px")
                .attr("opacity", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? 1.0 : 0.6)
                .append("title")
                .text(`${d.name}${d.cohesion !== undefined ? `\nCohesion: ${d.cohesion.toFixed(2)}` : ''}\n${sankeyScale === 'features' ? 'Features' : 'Functions'}: ${d.value}`);
                
            el.append("text")
                .attr("x", d.id.startsWith('func_b_') || d.id.startsWith('funcgroup_b_') || d.id.startsWith('simplified_b_') ? -6 : 6 + width)
                .attr("y", height / 2)
                .attr("dy", "0.35em")
                .attr("text-anchor", d.id.startsWith('func_b_') || d.id.startsWith('funcgroup_b_') || d.id.startsWith('simplified_b_') ? "end" : "start")
                .text(d.name)
                .attr("fill", "#fff")
                .attr("font-size", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? "10px" : "8px")
                .attr("font-weight", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? "bold" : "normal")
                .attr("opacity", (d.id.startsWith('cluster_') || d.id.startsWith('simplified_')) ? 1.0 : 0.7)
                .attr("font-family", "sans-serif");
        }
    });

    // Initialize D3 Zoom Behavior
    const zoom = d3.zoom()
        .scaleExtent([0.3, 5])
        .on("zoom", (event) => {
            zoomG.attr("transform", event.transform);
        });
        
    svg.call(zoom);
    window.sankeyZoomBehavior = zoom;
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
        
        return `<span class="badge clickable" title="${titleStr}" style="background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1); margin:2px;" 
            onmouseenter="showCodePreview('${fid}', '${cleanName}', '${cleanName}', '${md5}', 0, event)" 
            onmouseleave="hideCodePreview(event)" 
            onmousemove="moveCodePreview(event)"
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
            tbodyMatched.innerHTML = matched.map(m => `
                <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                    <td style="padding:10px;">
                        <div style="font-weight:bold; color:var(--accent);">${m.cluster_name}</div>
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
            `).join('');
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
        tbody.innerHTML = items.map(u => `
            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                <td style="padding:10px;">
                    <div style="font-weight:bold; color:var(--accent);">${u.cluster_name}</div>
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
        `).join('');
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
