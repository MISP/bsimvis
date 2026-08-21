/**
 * Function Diff View Module
 * Extracted from diff/index.html
 */

window.DiffView = {
    container: null,
    params: null,
    scrollEl: null,
    vHeightEl: null,
    leftContent: null,
    rightContent: null,
    tooltipEl: null,
    bsimRows: [],
    tokenTipsL: {},
    tokenTipsR: {},
    lockedHashes: new Set(),
    rowH: 24,
    OVERSCAN: 20,
    lastStart: -1,
    lastEnd: -1,
    currentScores: {},
    currentSimId1: null,
    currentSimId2: null,
    currentSimTags: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);

        // Save original globals from code_renderer.js
        this._originalToggleLock = window.toggleLock;
        this._originalClearAllLocks = window.clearAllLocks;
        this._originalSetHighlight = window.setHighlight;
        this._originalSetChunkHighlight = window.setChunkHighlight;
        
        // Expose globals for HTML inline onclick handlers
        window.copyDiffCode = (side, btn) => this.copyDiffCode(side, btn);
        window.updateSimDisplay = () => this.updateSimDisplay();
        window.onManualInput = (side) => this.onManualInput(side);
        window.onCollChange = (side) => this.onCollChange(side);
        window.onFileChange = (side) => this.onFileChange(side);
        window.onFuncChange = (side) => this.onFuncChange(side);
        window.startComparison = () => this.startComparison();
        window.toggleBothDetail = () => this.toggleBothDetail();
        window.navigateToFunction = (funcId, e) => this.navigateToFunction(funcId, e);
        window.toggleLock = (hashString, target) => this.toggleLock(hashString, target);
        window.clearAllLocks = () => this.clearAllLocks();
        window.setHighlight = (hashString, state, target) => this.setHighlight(hashString, state, target);
        window.setChunkHighlight = (chunkId, state, target) => this.setChunkHighlight(chunkId, state, target);
        window.switchDiffMode = (mode) => this.switchDiffMode(mode);

        // Build HTML Layout
        this.container.innerHTML = `
            <div style="display:flex; flex-direction:column; flex:1; overflow:hidden; height:100%; width:100%;">
                <div id="meta-container" style="display:flex; flex-direction:row; gap:10px; margin-bottom:5px; flex-shrink:0;"></div>
                
                <div id="similarity-bar" style="display:none; padding:10px; border-bottom:1px solid var(--border); font-size:0.9rem; align-items:center;">
                    <div class="sim-info" style="display:flex; width:100%; justify-content:space-between; align-items:center;">
                        <div style="display:flex; align-items:center; gap:10px;">
                            <span class="sim-label" style="font-weight:bold;">Similarity:</span>
                            <span id="sim-score-val" class="sim-score" style="font-weight:bold; font-size:1.1rem; color:var(--success);">---</span>
                            <select id="sim-algo-select" class="sim-select" onchange="updateSimDisplay()" style="background:var(--card-bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:2px 5px; font-size:0.8rem;">
                                <option value="unweighted_cosine">Cosine</option>
                                <option value="jaccard">Jaccard</option>
                                <option value="milvus_sparse">Milvus Sparse</option>
                            </select>
                            <div style="display:flex; align-items:center; gap:5px; margin-left:15px; border-left:1px solid var(--border); padding-left:15px;">
                                <button id="btn-diff-mode-code" class="top-action-btn active" onclick="switchDiffMode('code')" style="font-size:0.8rem; padding:3px 8px;">
                                    <i class="fa-solid fa-code"></i> Code Diff
                                </button>
                                <button id="btn-diff-mode-graph" class="top-action-btn" onclick="switchDiffMode('graph')" style="font-size:0.8rem; padding:3px 8px;">
                                    <i class="fa-solid fa-diagram-project"></i> Call Graph Diff
                                </button>
                            </div>
                        </div>
                        <div style="display:flex; align-items:center; gap:15px;">
                            <div id="diff-queue-status" style="display:flex; align-items:center; gap:10px;"></div>
                            <div id="similarity-tags-container" style="display:flex; align-items:center;"></div>
                        </div>
                    </div>
                </div>

                <div id="diff-view-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                    <i class="fa-solid fa-spinner fa-spin"></i> Loading Function Diff...
                </div>

                <div id="selection-tool" style="display:none; flex-direction:column; padding:20px; gap:20px; overflow-y:auto; flex:1;">
                    <h2 style="margin-top:0; color:var(--accent);">Function Diff Selection</h2>
                    <div class="selection-columns" style="display:flex; gap:20px; flex-wrap:wrap;">
                        <div class="selection-col" id="col-left" style="flex:1; min-width:280px; background:var(--card-bg); padding:15px; border-radius:6px; border:1px solid var(--border);">
                            <h3 style="margin-top:0; color:var(--text-bright);">Left Side</h3>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Manual Function ID</label>
                                <input type="text" id="manual-id-l" placeholder="coll:func:md5:addr" oninput="onManualInput('l')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                            </div>
                            <div class="divider" style="text-align:center; font-size:0.75rem; color:var(--dim); margin:15px 0;">OR SELECT</div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Collection</label>
                                <select id="select-coll-l" onchange="onCollChange('l')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose Collection --</option>
                                </select>
                            </div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">File</label>
                                <select id="select-file-l" disabled onchange="onFileChange('l')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose File --</option>
                                </select>
                            </div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Function</label>
                                <select id="select-func-l" disabled onchange="onFuncChange('l')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose Function --</option>
                                </select>
                            </div>
                        </div>

                        <div class="selection-col" id="col-right" style="flex:1; min-width:280px; background:var(--card-bg); padding:15px; border-radius:6px; border:1px solid var(--border);">
                            <h3 style="margin-top:0; color:var(--text-bright);">Right Side</h3>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Manual Function ID</label>
                                <input type="text" id="manual-id-r" placeholder="coll:func:md5:addr" oninput="onManualInput('r')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                            </div>
                            <div class="divider" style="text-align:center; font-size:0.75rem; color:var(--dim); margin:15px 0;">OR SELECT</div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Collection</label>
                                <select id="select-coll-r" onchange="onCollChange('r')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose Collection --</option>
                                </select>
                            </div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">File</label>
                                <select id="select-file-r" disabled onchange="onFileChange('r')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose File --</option>
                                </select>
                            </div>
                            <div class="form-group" style="margin-bottom:15px;">
                                <label style="display:block; margin-bottom:5px; font-size:0.85rem; color:var(--dim);">Function</label>
                                <select id="select-func-r" disabled onchange="onFuncChange('r')" style="width:100%; padding:6px 10px; background:var(--bg); border:1px solid var(--border); border-radius:4px; color:var(--text); box-sizing:border-box;">
                                    <option value="">-- Choose Function --</option>
                                </select>
                            </div>
                        </div>
                    </div>
                    <div class="compare-btn-container" style="text-align:center; margin-top:20px;">
                        <button id="compare-btn" class="btn-compare" disabled onclick="startComparison()" style="padding:10px 25px; background:var(--accent); color:var(--window-tray); border:none; border-radius:4px; font-weight:bold; cursor:pointer; font-size:1rem;">Start Comparison</button>
                    </div>
                </div>

                <div id="bsim-scroll" style="display:none; flex:1; overflow-y:auto; overflow-x:hidden; position:relative;">
                    <div id="bsim-vheight" style="position:absolute; width:1px; top:0; left:0; z-index:-1;"></div>
                    <div id="bsim-sticky" style="display:flex; height:100%; width:100%; position:sticky; top:0;">
                        <div style="flex:1; position:relative; display:flex; flex-direction:column; overflow:hidden;">
                            <div class="diff-pane" style="height:100%; overflow-x:auto; overflow-y:hidden;">
                                <div id="bsim-left-content" class="bsim-vcontent c-code-container" style="position:relative;"></div>
                            </div>
                            <button class="floating-copy-btn" title="Copy left code with colors" onclick="copyDiffCode('l', this)">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                        <div style="flex:1; position:relative; display:flex; flex-direction:column; overflow:hidden; border-left:1px solid #3e3d32;">
                            <div class="diff-pane" style="height:100%; overflow-x:auto; overflow-y:hidden;">
                                <div id="bsim-right-content" class="bsim-vcontent c-code-container" style="position:relative;"></div>
                            </div>
                            <button class="floating-copy-btn" title="Copy right code with colors" onclick="copyDiffCode('r', this)">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                    </div>
                </div>

                <div id="bsim-graph-diff-wrap" style="display:none; flex:1; height:100%; width:100%; position:relative; min-height:500px;">
                    <div style="display:flex; height:100%; width:100%;">
                        <div style="flex:1; border-right:1px solid var(--border); display:flex; flex-direction:column; position:relative; background:var(--bg);">
                            <div style="padding:6px 12px; background:var(--meta-bg); border-bottom:1px solid var(--border); font-weight:bold; font-size:0.8rem; color:var(--accent); display:flex; justify-content:space-between; align-items:center;">
                                <span><i class="fa-solid fa-diagram-project"></i> Left Call Graph</span>
                                <span id="diff-cg-left-name" style="font-size:0.75rem; font-weight:normal; color:var(--subtle);"></span>
                            </div>
                            <div id="diff-cg-left-loader" style="text-align:center; padding:40px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin"></i> Loading Left Graph...</div>
                            <div id="diff-cg-left-container" style="display:none; width:100%; height:100%; flex:1; position:relative;"></div>
                        </div>
                        <div style="flex:1; display:flex; flex-direction:column; position:relative; background:var(--bg);">
                            <div style="padding:6px 12px; background:var(--meta-bg); border-bottom:1px solid var(--border); font-weight:bold; font-size:0.8rem; color:var(--accent); display:flex; justify-content:space-between; align-items:center;">
                                <span><i class="fa-solid fa-diagram-project"></i> Right Call Graph</span>
                                <span id="diff-cg-right-name" style="font-size:0.75rem; font-weight:normal; color:var(--subtle);"></span>
                            </div>
                            <div id="diff-cg-right-loader" style="text-align:center; padding:40px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin"></i> Loading Right Graph...</div>
                            <div id="diff-cg-right-container" style="display:none; width:100%; height:100%; flex:1; position:relative;"></div>
                        </div>
                    </div>
                </div>

                <div id="bsim-tooltip" class="tooltip" style="display:none; position:fixed; z-index:20000; background:var(--window-bg); padding:10px; border-radius:4px; border:1px solid var(--accent); color:var(--text); font-size:0.8rem; pointer-events:none;"></div>
            </div>
        `;

        this.scrollEl = document.getElementById('bsim-scroll');
        this.vHeightEl = document.getElementById('bsim-vheight');
        this.leftContent = document.getElementById('bsim-left-content');
        this.rightContent = document.getElementById('bsim-right-content');
        this.tooltipEl = document.getElementById('bsim-tooltip');

        document.getElementById('meta-container').addEventListener('click', (e) => {
            if (e.target.closest('#swap-btn')) {
                // Extract current params from URL or from parsed path
                const p = this._getCurrentP();
                if (p) {
                    const newP = { ...p };
                    const tmp = { collection_a: p.collection_a, md5_a: p.md5_a, addr_a: p.addr_a };
                    newP.collection_a = p.collection_b;
                    newP.md5_a = p.md5_b;
                    newP.addr_a = p.addr_b;
                    newP.collection_b = tmp.collection_a;
                    newP.md5_b = tmp.md5_a;
                    newP.addr_b = tmp.addr_a;
                    let newUrl = '';
                    if (window.buildDiffUrl) {
                        const id1 = `${newP.collection_a}:func:${newP.md5_a}:${newP.addr_a}`;
                        const id2 = `${newP.collection_b}:func:${newP.md5_b}:${newP.addr_b}`;
                        newUrl = window.buildDiffUrl(id1, id2);
                    } else {
                        newUrl = `/collections/${encodeURIComponent(newP.collection_a)}/files/${newP.md5_a}/functions/${newP.addr_a}/vs/${encodeURIComponent(newP.collection_b)}/${newP.md5_b}/${newP.addr_b}`;
                        const poolId = window.getRoutingState?.()?.pool || null;
                        if (poolId) {
                            newUrl = `/pools/${encodeURIComponent(poolId)}` + newUrl;
                        }
                    }
                    Nav.openPath(newUrl);
                }
            }
        });

        await this.fetchData();
    },

    _getCurrentP() {
        const path = window.location.pathname;
        const parts = path.split('/').filter(Boolean);
        let hasCol = parts[0] === 'collection' || parts[0] === 'collections';
        let offset = 0;
        let poolId = null;
        if (parts[0] === 'pool' || parts[0] === 'pools') {
            poolId = parts[1];
            if (parts[2] === 'collection' || parts[2] === 'collections') {
                hasCol = true;
                offset = 2;
            }
        }
        if (hasCol && (parts[offset + 2] === 'files' || parts[offset + 2] === 'file') && parts[offset + 4] === 'functions' && parts[offset + 6] === 'vs') {
            const rawCollA = decodeURIComponent(parts[offset + 1]);
            const collA = poolId ? `pool:${poolId}:col:${rawCollA}` : rawCollA;
            const md5A = decodeURIComponent(parts[offset + 3]);
            const addrA = decodeURIComponent(parts[offset + 5]);
            const rawCollB = decodeURIComponent(parts[offset + 7]);
            const collB = poolId ? `pool:${poolId}:col:${rawCollB}` : rawCollB;
            const md5B = decodeURIComponent(parts[offset + 8]);
            const addrB = decodeURIComponent(parts[offset + 9]);
            return { collection_a: collA, md5_a: md5A, addr_a: addrA, collection_b: collB, md5_b: md5B, addr_b: addrB };
        }
        return null;
    },

    _parsePathUrl() {
        const path = window.location.pathname;
        const parts = path.split('/').filter(Boolean);
        let hasCol = parts[0] === 'collection' || parts[0] === 'collections';
        let offset = 0;
        let poolId = null;
        if (parts[0] === 'pool' || parts[0] === 'pools') {
            poolId = parts[1];
            if (parts[2] === 'collection' || parts[2] === 'collections') {
                hasCol = true;
                offset = 2;
            }
        }
        if (hasCol && (parts[offset + 2] === 'files' || parts[offset + 2] === 'file') && parts[offset + 4] === 'functions' && parts[offset + 6] === 'vs') {
            const rawCollA = stripPoolPrefix(decodeURIComponent(parts[offset + 1]));
            const rawCollB = stripPoolPrefix(decodeURIComponent(parts[offset + 7]));
            return {
                collection_a: rawCollA || 'main',
                md5_a: decodeURIComponent(parts[offset + 3]),
                addr_a: decodeURIComponent(parts[offset + 5]),
                collection_b: rawCollB || rawCollA || 'main',
                md5_b: decodeURIComponent(parts[offset + 8]),
                addr_b: decodeURIComponent(parts[offset + 9]),
                pool: poolId
            };
        } else if (hasCol && parts[offset + 2] === 'function') {
            if (parts.length >= (offset + 9) && parts[offset + 5] === 'vs') {
                const rawCollA = stripPoolPrefix(decodeURIComponent(parts[offset + 1]));
                const rawCollB = stripPoolPrefix(decodeURIComponent(parts[offset + 6]));
                return {
                    collection_a: rawCollA || 'main',
                    md5_a: decodeURIComponent(parts[offset + 3]),
                    addr_a: decodeURIComponent(parts[offset + 4]),
                    collection_b: rawCollB || rawCollA || 'main',
                    md5_b: decodeURIComponent(parts[offset + 7]),
                    addr_b: decodeURIComponent(parts[offset + 8]),
                    pool: poolId
                };
            }
        }
        return {};
    },

    async fetchData() {
        let p;
        // Try URL query params first
        const urlParams = new URLSearchParams(window.location.search);
        const collection_a = urlParams.get('collection_a');
        const md5_a = urlParams.get('md5_a');
        const addr_a = urlParams.get('addr_a');
        const collection_b = urlParams.get('collection_b') || urlParams.get('coll_b');
        const md5_b = urlParams.get('md5_b');
        const addr_b = urlParams.get('addr_b');
        
        if (collection_a && md5_a && addr_a && collection_b && md5_b && addr_b) {
            p = { collection_a, md5_a, addr_a, collection_b, md5_b, addr_b };
        } else {
            p = this._parsePathUrl();
        }

        // If still no flat params, try legacy id1/id2
        if (!p || !p.collection_a || !p.md5_a) {
            const legacyId1 = this.params.id1 || urlParams.get('id1');
            const legacyId2 = this.params.id2 || urlParams.get('id2');
            if (legacyId1 && legacyId2) {
                function parseLegacyId(id) {
                    if (!id) return { collection: '', md5: '', addr: '' };
                    if (id.includes(':func:')) {
                        const [c, rest] = id.split(':func:');
                        const [md5, addr] = rest.split(':');
                        return { collection: c || '', md5, addr };
                    }
                    if (id.includes(':function:')) {
                        const [c, rest] = id.split(':function:');
                        const [md5, addr] = rest.split(':');
                        return { collection: c || '', md5, addr };
                    }
                    const parts = id.split(':');
                    if (parts[0] === 'idx') {
                        return { collection: parts[1] || '', md5: parts[3], addr: parts[4] };
                    }
                    return { collection: parts[0] || '', md5: parts[2], addr: parts[3] };
                }
                const a = parseLegacyId(legacyId1);
                const b = parseLegacyId(legacyId2);
                p = { collection_a: a.collection, md5_a: a.md5, addr_a: a.addr, collection_b: b.collection, md5_b: b.md5, addr_b: b.addr };
            }
        }

        if (!p) {
            p = this._parsePathUrl();
        }

        const loader = document.getElementById('diff-view-loader');
        const scrollContainer = document.getElementById('bsim-scroll');
        const metaContainer = document.getElementById('meta-container');
        const selectionTool = document.getElementById('selection-tool');
        const simBar = document.getElementById('similarity-bar');

        if (!p.collection_a || !p.md5_a || !p.addr_a || !p.collection_b || !p.md5_b || !p.addr_b) {
            if (loader) loader.style.display = 'none';
            if (scrollContainer) scrollContainer.style.display = 'none';
            if (metaContainer) metaContainer.style.display = 'none';
            if (simBar) simBar.style.display = 'none';
            if (selectionTool) {
                selectionTool.style.display = 'flex';
                this.initSelectionTool();
            }
            return;
        }

        if (selectionTool) selectionTool.style.display = 'none';
        if (loader) loader.style.display = 'block';
        if (scrollContainer) scrollContainer.style.display = 'none';
        if (metaContainer) metaContainer.style.display = 'flex';

        try {
            const pool = urlParams.get('pool') || this.params.pool || null;
            const apiUrl = `/api/diff?collection_a=${encodeURIComponent(p.collection_a)}&md5_a=${encodeURIComponent(p.md5_a)}&addr_a=${encodeURIComponent(p.addr_a)}&collection_b=${encodeURIComponent(p.collection_b)}&md5_b=${encodeURIComponent(p.md5_b)}&addr_b=${encodeURIComponent(p.addr_b)}${pool ? '&pool=' + encodeURIComponent(pool) : ''}`;
            
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error("API Network error");
            const data = await response.json();

            if (window.fetchTagMetadata) {
                await window.fetchTagMetadata(p.collection_a.split(':')[0]);
            }
            
            const id1 = `${p.collection_a}:func:${p.md5_a}:${p.addr_a}`;
            const id2 = `${p.collection_b}:func:${p.md5_b}:${p.addr_b}`;
            metaContainer.innerHTML = this.formatMetaCard(data.meta1, id1, true) + this.formatMetaCard(data.meta2, id2, false);
            this.fetchSimilarity(id1, id2);
            if (typeof window.updateDiffQueueUI === 'function') window.updateDiffQueueUI();

            if (data.meta1 && data.meta2) {
                const collA = p.collection_a.split(':')[0];
                const collB = p.collection_b.split(':')[0];
                Breadcrumbs.setFilename(p.md5_a, data.meta1.file_name || 'File');
                Breadcrumbs.setFilename(p.md5_b, data.meta2.file_name || 'File');
                Breadcrumbs.setFuncName(collA, p.md5_a, p.addr_a, data.meta1.function_name || 'Function');
                Breadcrumbs.setFuncName(collB, p.md5_b, p.addr_b, data.meta2.function_name || 'Function');
                Breadcrumbs.refresh();
            }

            this.bsimRows = data.rows || [];
            this.tokenTipsL = data.left_tips || {};
            this.tokenTipsR = data.right_tips || {};

            if (scrollContainer) scrollContainer.style.display = 'flex';
            this.initVirtualScroll();
        } catch (e) {
            console.error(e);
            metaContainer.innerHTML = `<div style="color:red; padding:20px;">Failed to fetch diff data format from API. Ensure server is running.</div>`;
        } finally {
            if (loader) loader.style.display = 'none';
        }
    },

    initVirtualScroll() {
        this.scrollEl.scrollTop = 0;
        this.lastStart = -1;
        this.lastEnd = -1;

        this.vHeightEl.style.height = (this.bsimRows.length * this.rowH) + 'px';

        if (!this.scrollEl.dataset.listenersAttached) {
            this.scrollEl.addEventListener('scroll', () => this.onScroll(), { passive: true });
            this.scrollEl.addEventListener('pointerover', e => this.handleHover(e, true), true);
            this.scrollEl.addEventListener('pointerout', e => this.handleHover(e, false), true);
            this.scrollEl.addEventListener('contextmenu', e => this.showTokenContextMenu(e), true);
            this.scrollEl.addEventListener('click', (event) => {
                const target = event.composedPath ? event.composedPath()[0] : event.target;
                const tokenTarget = target?.closest?.('.token');
                if (tokenTarget) {
                    const calledFuncId = tokenTarget.getAttribute('data-called-func-id');
                    if (calledFuncId) {
                        this.navigateToFunction(calledFuncId, event);
                        return;
                    }
                    if (tokenTarget.classList.contains('feature-highlight')) {
                        const hashes = this.getHashesForToken(tokenTarget);
                        if (hashes) this.toggleLock(hashes, tokenTarget);
                    } else {
                        this.clearAllLocks();
                    }
                } else {
                    this.clearAllLocks();
                }
            }, true);
            
            this.scrollEl.addEventListener('pointermove', e => {
                this.moveTooltip(e.clientX, e.clientY);
                if (window.parent && window.parent !== window && typeof window.parent.moveCodePreviewFromIframe === 'function') {
                    window.parent.moveCodePreviewFromIframe(window.name, e);
                }
            });
            this.scrollEl.dataset.listenersAttached = "true";
        }

        this.setupKeyboardSelection();
        if (window.setupRichCopyInterceptor) {
            window.setupRichCopyInterceptor(this.leftContent, () => this.bsimRows.map(r => r.l).filter(r => r && r.line_idx !== undefined), { showDiffs: true });
            window.setupRichCopyInterceptor(this.rightContent, () => this.bsimRows.map(r => r.r).filter(r => r && r.line_idx !== undefined), { showDiffs: true });
        }
        this.onScroll();
        
        // Actually place the cursor in the element so arrow keys work immediately
        this.leftContent.focus();
        setTimeout(() => {
            const sel = window.getSelection();
            if (!sel.rangeCount || !this.leftContent.contains(sel.focusNode)) {
                const range = document.createRange();
                range.selectNodeContents(this.leftContent);
                range.collapse(true);
                sel.removeAllRanges();
                sel.addRange(range);
            }
        }, 250);

        this._selectionChangeListener = () => {
            const activeEl = document.activeElement;
            if (activeEl !== this.leftContent && activeEl !== this.rightContent) {
                if (this._currentKbdToken) {
                    const els = this.getHoverElementsForToken(this._currentKbdToken);
                    this.handleHoverElements(els.token, els.tooltipTarget, els.funcCallToken, false);
                    this._currentKbdToken = null;
                }
                return;
            }
            const token = this.findTokenFromSelection();
            if (token !== this._currentKbdToken) {
                if (this._currentKbdToken) {
                    const els = this.getHoverElementsForToken(this._currentKbdToken);
                    this.handleHoverElements(els.token, els.tooltipTarget, els.funcCallToken, false);
                }
                this._currentKbdToken = token;
                if (this._currentKbdToken) {
                    const els = this.getHoverElementsForToken(this._currentKbdToken);
                    this.handleHoverElements(els.token, els.tooltipTarget, els.funcCallToken, true);
                }
            }
        };
        document.addEventListener('selectionchange', this._selectionChangeListener);

        requestAnimationFrame(() => {
            const sample = this.leftContent.querySelector('.code-line, .code-spacer');
            if (sample) {
                const measuredH = sample.getBoundingClientRect().height;
                if (measuredH > 0 && Math.abs(measuredH - this.rowH) > 1) {
                    this.rowH = measuredH;
                    this.vHeightEl.style.height = (this.bsimRows.length * this.rowH) + 'px';
                }
            }
            this.lastStart = -1; this.lastEnd = -1;
            this.onScroll();
        });
    },

    onScroll() {
        const rows = this.bsimRows || [];
        const st = this.scrollEl.scrollTop;
        const vh = this.scrollEl.clientHeight;
        const start = Math.max(0, Math.floor(st / this.rowH) - this.OVERSCAN);
        const end = Math.min(rows.length, Math.ceil((st + vh) / this.rowH) + this.OVERSCAN);
        this.renderRows(start, end);

        const offset = (start * this.rowH) - st;
        this.leftContent.style.transform = `translateY(${offset}px)`;
        this.rightContent.style.transform = `translateY(${offset}px)`;

        this.leftContent.scrollTop = 0;
        this.rightContent.scrollTop = 0;
        const lp = this.leftContent.closest('.diff-pane');
        const rp = this.rightContent.closest('.diff-pane');
        if (lp) lp.scrollTop = 0;
        if (rp) rp.scrollTop = 0;
    },

    renderRows(start, end) {
        if (start === this.lastStart && end === this.lastEnd) return;
        this.lastStart = start; this.lastEnd = end;

        const rows = this.bsimRows || [];
        
        const lMap = new Map(), rMap = new Map();
        Array.from(this.leftContent.children).forEach(el => {
            const ln = el.querySelector('.line-num');
            if (ln && ln.innerText.trim()) lMap.set(`L${ln.innerText.trim()}`, el);
            else if (el.classList.contains('code-spacer')) lMap.set(`S${el.dataset.rowIdx}`, el);
        });
        Array.from(this.rightContent.children).forEach(el => {
            const ln = el.querySelector('.line-num');
            if (ln && ln.innerText.trim()) rMap.set(`L${ln.innerText.trim()}`, el);
            else if (el.classList.contains('code-spacer')) rMap.set(`S${el.dataset.rowIdx}`, el);
        });

        const neededL = [], neededR = [];

        for (let i = start; i < end; i++) {
            const row = rows[i];
            
            // Left side
            let lEl;
            if (row && row.l) {
                lEl = lMap.get(`L${row.l.line_idx}`);
                if (!lEl) {
                    const temp = document.createElement('div');
                    temp.innerHTML = this.renderSideHtml(row.l);
                    lEl = temp.firstElementChild;
                }
                lMap.delete(`L${row.l.line_idx}`);
            } else {
                lEl = lMap.get(`S${i}`);
                if (!lEl) {
                    lEl = document.createElement('div');
                    lEl.className = 'code-spacer';
                    lEl.dataset.rowIdx = i;
                    lEl.setAttribute('contenteditable', 'false');
                }
                lMap.delete(`S${i}`);
            }
            neededL.push(lEl);

            // Right side
            let rEl;
            if (row && row.r) {
                rEl = rMap.get(`L${row.r.line_idx}`);
                if (!rEl) {
                    const temp = document.createElement('div');
                    temp.innerHTML = this.renderSideHtml(row.r);
                    rEl = temp.firstElementChild;
                }
                rMap.delete(`L${row.r.line_idx}`);
            } else {
                rEl = rMap.get(`S${i}`);
                if (!rEl) {
                    rEl = document.createElement('div');
                    rEl.className = 'code-spacer';
                    rEl.dataset.rowIdx = i;
                    rEl.setAttribute('contenteditable', 'false');
                }
                rMap.delete(`S${i}`);
            }
            neededR.push(rEl);
        }

        lMap.forEach(el => el.remove());
        rMap.forEach(el => el.remove());

        for (let i = 0; i < neededL.length; i++) {
            if (this.leftContent.children[i] !== neededL[i]) this.leftContent.insertBefore(neededL[i], this.leftContent.children[i] || null);
            if (this.rightContent.children[i] !== neededR[i]) this.rightContent.insertBefore(neededR[i], this.rightContent.children[i] || null);
        }

        this.applyLocks(this.leftContent);
        this.applyLocks(this.rightContent);
    },

    renderSideHtml(sideData) {
        if (!sideData) return '<div class="code-spacer"></div>';

        const { chunk_class, chunk_id, line_idx, tooltip_text, tokens } = sideData;
        const data_attrs = `data-chunk-id="${chunk_id}"`;

        let content = '';
        for (const token of tokens) {
            const titleAttr = token.called_func_id ? `title="Click to navigate to ${token.target_name || 'called function'}"` : '';
            if (window.renderTokenHtml) {
                content += window.renderTokenHtml(token, token.diff_class || '')
                    .replace('<span ', `<span data-side="${token.side}" ${titleAttr} `);
            }
        }

        return `<div class="code-line ${chunk_class}" ${data_attrs}>` +
            `<div class="gutter" contenteditable="false"><div class="line-num" title="${tooltip_text}">${line_idx}</div></div>` +
            `<div class="line-content">${content}</div></div>`;
    },

    setupKeyboardSelection() {
        this.leftContent.setAttribute('contenteditable', 'true');
        this.leftContent.setAttribute('spellcheck', 'false');
        this.rightContent.setAttribute('contenteditable', 'true');
        this.rightContent.setAttribute('spellcheck', 'false');
        
        const syncCaret = () => {
            const sel = window.getSelection();
            if (!sel.rangeCount) return;
            const node = sel.anchorNode;
            if (!node || !(this.leftContent.contains(node) || this.rightContent.contains(node))) return;

            const rect = sel.getRangeAt(0).getBoundingClientRect();
            if (rect.height === 0 || (rect.top === 0 && rect.left === 0)) return;

            const containerRect = this.scrollEl.getBoundingClientRect();
            const padding = 60;
            let delta = 0;

            if (rect.top < containerRect.top + padding) {
                delta = rect.top - (containerRect.top + padding);
            } else if (rect.bottom > containerRect.bottom - padding) {
                delta = rect.bottom - (containerRect.bottom - padding);
            }

            if (Math.abs(delta) > 1) {
                this.scrollEl.scrollTop += delta;
            }
        };

        const handleKeydown = (e) => {
            const isCmd = e.ctrlKey || e.metaKey;
            const key = e.key;

            if (key === 'Enter') {
                e.preventDefault();
                const token = this.findTokenFromSelection();
                if (token) {
                    const els = this.getHoverElementsForToken(token);
                    if (els.funcCallToken) {
                        const calledFuncId = els.funcCallToken.getAttribute('data-called-func-id');
                        if (calledFuncId) {
                            this.navigateToFunction(calledFuncId, e);
                            return;
                        }
                    }
                    if (els.token && els.token.classList.contains('feature-highlight')) {
                        const hashes = this.getHashesForToken(els.token);
                        if (hashes) this.toggleLock(hashes, els.token);
                    }
                }
                return;
            }

            if (isCmd && key.toLowerCase() === 'a') return;
            if (isCmd && (key.toLowerCase() === 'c' || key.toLowerCase() === 'v')) return;
            
            const allowedKeys = [
                'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight',
                'Home', 'End', 'PageUp', 'PageDown', 'Shift', 'Control', 'Alt', 'Meta'
            ];
            
            if (allowedKeys.includes(key)) {
                requestAnimationFrame(syncCaret);
                return;
            }
            
            if (!isCmd) e.preventDefault();
        };

        const preventInput = (e) => e.preventDefault();

        this.leftContent.addEventListener('keydown', handleKeydown);
        this.leftContent.addEventListener('beforeinput', preventInput);
        this.rightContent.addEventListener('keydown', handleKeydown);
        this.rightContent.addEventListener('beforeinput', preventInput);

        const forceZeroScroll = (e) => { e.currentTarget.scrollTop = 0; };
        this.leftContent.addEventListener('scroll', forceZeroScroll);
        this.rightContent.addEventListener('scroll', forceZeroScroll);
        const lp = this.leftContent.closest('.diff-pane');
        const rp = this.rightContent.closest('.diff-pane');
        if (lp) lp.addEventListener('scroll', forceZeroScroll);
        if (rp) rp.addEventListener('scroll', forceZeroScroll);

        let selTimeout = null;
        document.addEventListener('selectionchange', () => {
            if (selTimeout) clearTimeout(selTimeout);
            selTimeout = setTimeout(syncCaret, 20);
        });
    },

    ensureTooltip() {
        if (!this.tooltipEl) this.tooltipEl = document.getElementById("bsim-tooltip");
        return this.tooltipEl;
    },

    showTooltip(html, x, y) {
        const el = this.ensureTooltip();
        if (!el) return;
        el.innerHTML = html;
        el.style.display = "block";
        el.style.left = (x + 12) + "px";
        el.style.top = (y + 12) + "px";
    },

    hideTooltip() {
        const el = this.ensureTooltip();
        if (el) el.style.display = "none";
    },

    moveTooltip(x, y) {
        const el = this.ensureTooltip();
        if (!el || el.style.display === "none") return;
        el.style.left = (x + 12) + "px";
        el.style.top = (y + 12) + "px";
    },

    getHashesForToken(token) {
        if (!token) return "";
        const side = token.dataset.side;
        const idx = token.dataset.idx;
        if (!side || !idx) return "";
        const data = side === 'l' ? this.tokenTipsL?.[idx] : this.tokenTipsR?.[idx];
        if (data && data[2]) {
            return data[2].map(f => f[0]).join(' ');
        }
        return "";
    },

    getHtmlForTooltip(token) {
        if (!token) return "";
        const side = token.dataset.side;
        const idx = token.dataset.idx;
        if (!side || !idx) return "";
        const data = side === 'l' ? this.tokenTipsL?.[idx] : this.tokenTipsR?.[idx];
        if (data) {
            let html = `<b>Type:</b> ${data[0]}<br><b>Seq:</b> ${data[1]}<br><b>Idx:</b> ${idx}`;
            for (const f of data[2]) {
                html += `<hr><b style='color:${f[8]}'>${f[0]}</b><br>Op: ${f[1]}<br>OpFull: ${f[2]}<br>Type: ${f[3]}<br>Seq: ${f[4]}<br>Addr: ${f[5]}<br>Line idx: ${f[6]}<br><b>TF:</b> ${f[7]}`;
            }
            return html;
        }
        return "";
    },

    findInPath(event, selector) {
        const path = event.composedPath ? event.composedPath() : [];
        for (const el of path) {
            if (el instanceof Element && el.matches(selector)) {
                return el;
            }
        }
        return null;
    },

    findTokenFromSelection() {
        const sel = window.getSelection();
        if (!sel || !sel.rangeCount) return null;
        let node = sel.focusNode;
        while (node && node !== this.leftContent && node !== this.rightContent) {
            if (node instanceof Element && node.classList.contains('token')) return node;
            node = node.parentNode;
        }
        return null;
    },

    getHoverElementsForToken(t) {
        if (!t) return { token: null, tooltipTarget: null, funcCallToken: null };
        return {
            token: t.closest('.feature-highlight'),
            tooltipTarget: t.closest('[data-side]'),
            funcCallToken: t.closest('[data-called-func-id]')
        };
    },

    clearAllLocks() {
        document.querySelectorAll('.feature-locked, .bsim-group-active-match, .bsim-group-active-unique').forEach(el => {
            el.classList.remove('feature-locked', 'bsim-group-active-match', 'bsim-group-active-unique');
        });
        this.lockedHashes.clear();
    },

    toggleLock(hashString, target) {
        if (!hashString || !target) return;
        const hashes = hashString.trim().split(/\s+/);
        const isAlreadyLocked = hashes.some(h => this.lockedHashes.has(h));
        this.clearAllLocks();
        if (!isAlreadyLocked) {
            hashes.forEach(h => {
                this.lockedHashes.add(h);
                document.querySelectorAll('.feat-' + h).forEach(el => {
                    el.classList.add('feature-locked');
                    const activeClass = el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique';
                    el.classList.add(activeClass);
                });
            });
        }
    },

    setHighlight(hashString, state, target) {
        if (!hashString || !target) return;
        const hashes = hashString.trim().split(/\s+/);
        hashes.forEach(h => {
            if (!state && this.lockedHashes.has(h)) return;
            document.querySelectorAll('.feat-' + h).forEach(el => {
                const activeClass = el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique';
                el.classList.toggle(activeClass, state);
            });
        });
    },

    setChunkHighlight(chunkId, state, target) {
        if (chunkId === undefined || !target) return;
        document.querySelectorAll('.chunk-' + chunkId).forEach(el => {
            el.classList.toggle('chunk-hover-active', state);
        });
    },

    applyLocks(container) {
        if (!this.lockedHashes || !this.lockedHashes.size) return;
        this.lockedHashes.forEach(h => {
            container.querySelectorAll('.feat-' + h).forEach(el => {
                el.classList.add('feature-locked');
                el.classList.add(el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique');
            });
        });
    },

    handleHover(event, state) {
        const token = this.findInPath(event, '.feature-highlight');
        const tooltipTarget = this.findInPath(event, '[data-side]');
        const funcCallToken = this.findInPath(event, '[data-called-func-id]');

        this.handleHoverElements(token, tooltipTarget, funcCallToken, state, event);
    },

    handleHoverElements(token, tooltipTarget, funcCallToken, state, event) {
        if (funcCallToken) {
            const calledFuncId = funcCallToken.getAttribute('data-called-func-id');
            const isExternal = funcCallToken.getAttribute('data-is-external') === 'true';
            const targetName = funcCallToken.getAttribute('data-target-name') || calledFuncId.split(':').pop();
            
            let x = event ? event.clientX : undefined;
            let y = event ? event.clientY : undefined;
            if (x === undefined || y === undefined) {
                const rect = funcCallToken.getBoundingClientRect();
                x = rect.left + rect.width / 2;
                y = rect.bottom;
            }

            if (state) {
                if (calledFuncId && isExternal) {
                    const extName = targetName || calledFuncId.replace('ext:', '');
                    this.showTooltip(`<div style="display:flex;align-items:center;gap:6px;">
                        <span style="background:color-mix(in srgb, var(--token-instruction) 20%, transparent);color:#f92672;border:1px solid color-mix(in srgb, var(--token-instruction) 40%, transparent);border-radius:4px;padding:2px 7px;font-size:0.7rem;font-weight:600;letter-spacing:0.04em;">EXTERNAL</span>
                        <span style="color:var(--meta-text-muted);font-family:monospace;font-size:0.8rem;">${extName}</span>
                    </div>`, x, y);
                } else if (calledFuncId && !isExternal) {
                    const fakeEvent = { clientX: x, clientY: y, target: funcCallToken, currentTarget: funcCallToken };
                    if (window.parent && window.parent !== window && typeof window.parent.showCodePreviewFromIframe === 'function') {
                        window.parent.showCodePreviewFromIframe(window.name, calledFuncId, targetName, fakeEvent);
                    } else if (typeof window.showCodePreview === 'function') {
                        window.showCodePreview(calledFuncId, targetName, null, null, null, fakeEvent);
                    }
                }
            } else {
                if (calledFuncId && isExternal) {
                    this.hideTooltip();
                } else if (calledFuncId && !isExternal) {
                    if (window.parent && window.parent !== window && typeof window.parent.hideCodePreview === 'function') {
                        window.parent.hideCodePreview();
                    } else if (typeof window.hideCodePreview === 'function') {
                        window.hideCodePreview();
                    }
                }
            }
        }

        if (token) {
            const hashes = this.getHashesForToken(token);
            if (hashes) this.setHighlight(hashes, state, token);
        }

        if (tooltipTarget) {
            if (state) {
                let x = event ? event.clientX : undefined;
                let y = event ? event.clientY : undefined;
                if (x === undefined || y === undefined) {
                    const rect = tooltipTarget.getBoundingClientRect();
                    x = rect.left + rect.width / 2;
                    y = rect.bottom;
                }
                const html = this.getHtmlForTooltip(tooltipTarget);
                if (html) this.showTooltip(html, x, y);
            } else {
                this.hideTooltip();
            }
        }

        let chunk = null;
        if (event) {
            chunk = this.findInPath(event, '[data-chunk-id]');
        } else if (token) {
            chunk = token.closest('[data-chunk-id]');
        } else if (tooltipTarget) {
            chunk = tooltipTarget.closest('[data-chunk-id]');
        }
        
        if (chunk && chunk.dataset && chunk.dataset.chunkId !== undefined) {
            this.setChunkHighlight(chunk.dataset.chunkId, state, chunk);
        }
    },

    showTokenContextMenu(e) {
        const token = this.findInPath(e, '.feature-highlight');
        if (!token) return;
        e.preventDefault();

        const side = token.dataset.side;
        const idx = token.dataset.idx;
        const data = side === 'l' ? this.tokenTipsL?.[idx] : this.tokenTipsR?.[idx];
        if (!data || !data[2]) return;

        let menu = document.getElementById('token-context-menu');
        if (!menu) {
            menu = document.createElement('div');
            menu.id = 'token-context-menu';
            menu.className = 'context-menu';
            document.body.appendChild(menu);
        }

        const id1 = this.params.id1 || new URLSearchParams(window.location.search).get('id1') || '';
        const collection = id1.split(':')[0] || '';

        let html = `<div class="context-menu-header">Select Feature to Analyze</div>`;
        data[2].forEach(f => {
            const hash = f[0];
            const op = f[1];
            const type = f[3];
            const tf = f[7] || 0;
            const color = f[8] || 'var(--accent)';

            html += `<div class="context-menu-item" data-hash="${hash}" data-col="${collection}">
                <div class="context-menu-icon" style="color:${color}">🔍</div>
                <div style="flex:1">
                    <div style="font-family:monospace; font-weight:bold; color:${color}">${hash}</div>
                    <div style="font-size:0.7rem; color:var(--subtle); margin-top:2px;">
                        ${type} | Op: ${op} | <b style="color:var(--success)">TF: ${tf}</b>
                    </div>
                </div>
            </div>`;
        });

        menu.innerHTML = html;
        menu.style.display = 'block';
        let x = e.clientX, y = e.clientY;
        if (x + 350 > window.innerWidth) x -= 350;
        if (y + (data[2].length * 52 + 40) > window.innerHeight) y -= (data[2].length * 52 + 40);
        menu.style.left = x + 'px';
        menu.style.top = y + 'px';

        const onMenuClick = (me) => {
            const item = me.target.closest('.context-menu-item');
            if (item) {
                const h = item.dataset.hash;
                const c = item.dataset.col;
                const url = `/collections/${encodeURIComponent(c)}/features/${encodeURIComponent(h)}`;
                Nav.openPath(url, me, { title: `Feature Analysis: ${h.substring(0, 12)}...`, type: 'global-feature' });
            }
            closeMenu();
        };

        const closeMenu = () => {
            menu.style.display = 'none';
            menu.removeEventListener('click', onMenuClick);
            document.removeEventListener('mousedown', closeGlobal);
        };

        const closeGlobal = (me) => {
            if (!menu.contains(me.target)) closeMenu();
        };

        setTimeout(() => {
            menu.addEventListener('click', onMenuClick);
            document.addEventListener('mousedown', closeGlobal);
        }, 10);
    },

    navigateToFunction(funcId, e) {
        const parts = funcId.split(':');
        let col = parts[0] || '';
        let md5 = parts[2];
        let addr = parts[3];
        
        let url;
        if (col.startsWith('pool:')) {
            const colParts = col.split(':');
            const poolId = colParts[1];
            if (colParts.length >= 4 && colParts[2] === 'col') {
                const subCol = colParts[3];
                url = `/pools/${encodeURIComponent(poolId)}/collections/${encodeURIComponent(subCol)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(addr)}`;
            } else {
                url = `/pools/${encodeURIComponent(poolId)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(addr)}`;
            }
        } else {
            const routingState = window.getRoutingState ? window.getRoutingState() : {};
            if (routingState.pool) {
                url = `/pools/${encodeURIComponent(routingState.pool)}/collections/${encodeURIComponent(col)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(addr)}`;
            } else {
                url = `/collections/${encodeURIComponent(col)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(addr)}`;
            }
        }
        
        Nav.openPath(url, e, { title: `Code: ${addr}`, type: 'function' });
    },

    copyDiffCode(side, btn) {
        const rows = (this.bsimRows || [])
            .map(row => row[side])
            .filter(sideData => sideData !== null && sideData !== undefined && sideData.line_idx !== undefined);
        
        if (rows.length === 0) return;
        if (window.copyRichText) {
            window.copyRichText(rows, btn, { showDiffs: true });
        }
    },

    formatMetaCard(m, fullId, isLeft) {
        if (!m) return "";
        const swapIcon = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3L4 7l4 4"/><path d="M4 7h16"/><path d="M16 21l4-4-4-4"/><path d="M20 17H4"/></svg>`;
        const swapBtnHtml = isLeft ? `<button class="swap-btn" id="swap-btn" title="Swap Sides" style="background:none; border:none; color:var(--text); cursor:pointer; padding:0; display:flex; align-items:center; justify-content:center;">${swapIcon}</button>` : '';

        if (window.renderFunctionMetadata) {
            const cardHtml = window.renderFunctionMetadata(null, m, fullId, {
                showDiffBtn: true,
                showCodeLink: true,
                side: isLeft ? 'l' : 'r',
                detailId: `meta-more-${isLeft ? 'l' : 'r'}`,
                detailsToggleFn: 'toggleBothDetail()',
                rightHeaderHtml: swapBtnHtml
            });
            return `<div style="flex:1; min-width:0;">${cardHtml}</div>`;
        }
        return "";
    },

    async fetchSimilarity(id1, id2) {
        const bar = document.getElementById('similarity-bar');
        if (!id1 || !id2) {
            if (bar) bar.style.display = 'none';
            return;
        }

        // Parse flat params from id1/id2
        const parseFunc = window.parseFuncIdFromStr;
        if (!parseFunc) {
            if (bar) bar.style.display = 'none';
            return;
        }
        const p1 = parseFunc(id1);
        const p2 = parseFunc(id2);
        if (!p1.md5_a || !p2.md5_b) {
            // Can't parse, use legacy
            if (bar) bar.style.display = 'none';
            return;
        }

        const pool = window.getRoutingState?.()?.pool || null;
        const simUrl = `/api/similarity?collection_a=${encodeURIComponent(p1.collection_a)}&md5_a=${encodeURIComponent(p1.md5_a)}&addr_a=${encodeURIComponent(p1.addr_a)}&collection_b=${encodeURIComponent(p2.collection_b || p1.collection_a)}&md5_b=${encodeURIComponent(p2.md5_b)}&addr_b=${encodeURIComponent(p2.addr_b)}${pool ? '&pool=' + encodeURIComponent(pool) : ''}`;

        try {
            const res = await fetch(simUrl);
            if (!res.ok) throw new Error("Failed to fetch similarity");
            const data = await res.json();
            this.currentScores = data.scores || {};
            this.currentSimId1 = id1;
            this.currentSimId2 = id2;
            this.currentSimTags = { tags: data.tags || [], user_tags: data.user_tags || [] };
            
            if (bar) bar.style.display = 'flex';
            this.updateSimDisplay();
        } catch (e) {
            console.error("Similarity fetch error:", e);
            if (bar) bar.style.display = 'none';
        }
    },

    updateSimDisplay() {
        const algoSelect = document.getElementById('sim-algo-select');
        if (!algoSelect) return;
        const algo = algoSelect.value;
        const scoreVal = document.getElementById('sim-score-val');
        const score = this.currentScores?.[algo];

        if (this.currentSimId1 && this.currentSimId2) {
            const simId = `${this.currentSimId1}|${this.currentSimId2}|${algo}`;
            const tagsData = this.currentSimTags || { tags: [], user_tags: [] };
            const container = document.getElementById('similarity-tags-container');
            if (container && window.EntityRenderer && window.EntityRenderer.renderTag) {
                container.innerHTML = window.EntityRenderer.renderTag('similarity', simId, tagsData.tags, tagsData.user_tags);
            }
        }

        if (scoreVal) {
            if (score === null || score === undefined) {
                scoreVal.innerText = "N/A";
                scoreVal.style.color = "var(--subtle)";
            } else {
                scoreVal.innerText = (score * 100).toFixed(1) + "%";
                if (score > 0.8) scoreVal.style.color = "var(--success)";
                else if (score > 0.5) scoreVal.style.color = "var(--accent)";
                else scoreVal.style.color = "#fd971f";
            }
        }
    },

    toggleBothDetail() {
        const l = document.getElementById('meta-more-l');
        const r = document.getElementById('meta-more-r');
        if (!l || !r) return;
        const target = !l.classList.contains('expanded');
        l.classList.toggle('expanded', target);
        r.classList.toggle('expanded', target);
    },

    async initSelectionTool() {
        try {
            const res = await (await fetch('/api/collection/search?limit=1000')).json();
            const collections = res.collections || (Array.isArray(res) ? res : []);
            ['l', 'r'].forEach(side => {
                const sel = document.getElementById(`select-coll-${side}`);
                if (!sel) return;
                sel.innerHTML = '<option value="">-- Choose Collection --</option>';
                collections.forEach(c => {
                    const opt = document.createElement('option');
                    opt.value = c.name;
                    opt.innerText = c.name;
                    sel.appendChild(opt);
                });
            });
        } catch (e) {
            console.error("Failed to init selection tool:", e);
        }
    },

    async onCollChange(side) {
        const coll = document.getElementById(`select-coll-${side}`).value;
        const fileSel = document.getElementById(`select-file-${side}`);
        const funcSel = document.getElementById(`select-func-${side}`);

        if (!fileSel || !funcSel) return;

        fileSel.innerHTML = '<option value="">-- Loading Files... --</option>';
        fileSel.disabled = true;
        funcSel.innerHTML = '<option value="">-- Choose Function --</option>';
        funcSel.disabled = true;

        if (!coll) return;

        try {
            const res = await (await fetch(`/api/file/search?collection=${encodeURIComponent(coll)}&limit=1000`)).json();
            fileSel.innerHTML = '<option value="">-- Choose File --</option>';
            (res.files || []).forEach(f => {
                const opt = document.createElement('option');
                opt.value = f['file_md5'];
                opt.innerText = f['file_name'] || f['file_md5'];
                fileSel.appendChild(opt);
            });
            fileSel.disabled = false;
        } catch (e) {
            console.error(e);
        }
        this.validateComparison();
    },

    async onFileChange(side) {
        const coll = document.getElementById(`select-coll-${side}`).value;
        const md5 = document.getElementById(`select-file-${side}`).value;
        const funcSel = document.getElementById(`select-func-${side}`);

        if (!funcSel) return;

        funcSel.innerHTML = '<option value="">-- Loading Functions... --</option>';
        funcSel.disabled = true;

        if (!md5) return;

        try {
            const res = await (await fetch(`/api/function/search?collection=${encodeURIComponent(coll)}&file_md5=${encodeURIComponent(md5)}&limit=1000`)).json();
            funcSel.innerHTML = '<option value="">-- Choose Function --</option>';
            (res.functions || []).forEach(f => {
                const opt = document.createElement('option');
                opt.value = f['entrypoint_address'];
                opt.innerText = `${f['function_name']} (${f['entrypoint_address']})`;
                funcSel.appendChild(opt);
            });
            funcSel.disabled = false;
        } catch (e) {
            console.error(e);
        }
        this.validateComparison();
    },

    onFuncChange(side) {
        const coll = document.getElementById(`select-coll-${side}`).value;
        const md5 = document.getElementById(`select-file-${side}`).value;
        const addr = document.getElementById(`select-func-${side}`).value;

        if (coll && md5 && addr) {
            const el = document.getElementById(`manual-id-${side}`);
            if (el) el.value = `${coll}:func:${md5}:${addr}`;
        }
        this.validateComparison();
    },

    onManualInput(side) {
        this.validateComparison();
    },

    validateComparison() {
        const id1El = document.getElementById('manual-id-l');
        const id2El = document.getElementById('manual-id-r');
        if (!id1El || !id2El) return;
        const id1 = id1El.value.trim();
        const id2 = id2El.value.trim();
        const btn = document.getElementById('compare-btn');
        if (btn) btn.disabled = !(id1 && id2);
    },

    startComparison() {
        const id1 = document.getElementById('manual-id-l').value.trim();
        const id2 = document.getElementById('manual-id-r').value.trim();
        if (id1 && id2) {
            const url = buildDiffUrl(id1, id2);
            Nav.openPath(url);
        }
    },

    switchDiffMode(mode) {
        const codeBtn = document.getElementById('btn-diff-mode-code');
        const graphBtn = document.getElementById('btn-diff-mode-graph');
        const scrollEl = document.getElementById('bsim-scroll');
        const graphWrap = document.getElementById('bsim-graph-diff-wrap');

        if (mode === 'graph') {
            if (codeBtn) codeBtn.classList.remove('active');
            if (graphBtn) graphBtn.classList.add('active');
            if (scrollEl) scrollEl.style.display = 'none';
            if (graphWrap) graphWrap.style.display = 'flex';
            this.loadDiffCallGraphs();
        } else {
            if (graphBtn) graphBtn.classList.remove('active');
            if (codeBtn) codeBtn.classList.add('active');
            if (graphWrap) graphWrap.style.display = 'none';
            if (scrollEl) scrollEl.style.display = 'flex';
        }
    },

    async loadDiffCallGraphs() {
        if (this._cgLoaded) return;
        this._cgLoaded = true;

        const p = this._getCurrentP() || this._parsePathUrl();
        if (!p || !p.collection_a || !p.md5_a || !p.addr_a || !p.collection_b || !p.md5_b || !p.addr_b) return;

        const id1 = `${p.collection_a}:func:${p.md5_a}:${p.addr_a}`;
        const id2 = `${p.collection_b}:func:${p.md5_b}:${p.addr_b}`;

        this._renderSingleDiffGraph('left', id1);
        this._renderSingleDiffGraph('right', id2);
    },

    async _renderSingleDiffGraph(side, funcId) {
        const loader = document.getElementById(`diff-cg-${side}-loader`);
        const container = document.getElementById(`diff-cg-${side}-container`);
        const nameEl = document.getElementById(`diff-cg-${side}-name`);
        if (!loader || !container) return;

        loader.style.display = 'block';
        container.style.display = 'none';

        try {
            const res = await fetch(`/api/function/call_graph?id=${encodeURIComponent(funcId)}`);
            if (!res.ok) throw new Error('Call graph unavailable');
            const data = await res.json();

            if (nameEl && data.node) nameEl.innerText = data.node.function_name || funcId;

            const centerId = data.node.id;
            const nodes = [{ id: centerId, data: { raw: data.node, kind: 'self', depth: 0 }, expanded: true }];
            const edges = [];
            const seen = new Set([centerId]);

            for (const c of data.callers || []) {
                if (!seen.has(c.id)) {
                    seen.add(c.id);
                    nodes.push({ id: c.id, data: { raw: c, kind: c.is_external ? 'external' : 'caller', depth: 1 }, expanded: true });
                }
                edges.push({ id: `${c.id}->${centerId}`, from: c.id, to: centerId });
            }
            for (const c of data.callees || []) {
                if (!seen.has(c.id)) {
                    seen.add(c.id);
                    nodes.push({ id: c.id, data: { raw: c, kind: c.is_external ? 'external' : 'callee', depth: 1 }, expanded: true });
                }
                edges.push({ id: `${centerId}->${c.id}`, from: centerId, to: c.id });
            }

            loader.style.display = 'none';
            container.style.display = 'block';
            container.innerHTML = '';

            if (typeof Pivotick !== 'undefined') {
                const pInst = new Pivotick(container, { nodes, edges }, {
                    UI: { mode: 'light', tooltip: { enabled: false } },
                    simulation: { useWorker: false },
                    render: {
                        nodeShape: 'rectangle',
                        renderNode: (node) => {
                            const d = node.getData() || {};
                            if (typeof FunctionView !== 'undefined' && FunctionView.callGraphRenderNode) {
                                return FunctionView.callGraphRenderNode(d.raw, d.kind);
                            }
                            return `<div>${d.raw?.function_name || node.id}</div>`;
                        }
                    },
                    callbacks: {
                        onNodeClick: async (e, node) => {
                            const id = node.id;
                            const d = node.getData() || {};
                            if (d.kind === 'external' || d.raw?.is_external) return;
                            if (id === centerId) return;
                            const depth = d.depth ?? 1;
                            if (depth >= 3) return;
                            try {
                                const r = await fetch(`/api/function/call_graph?id=${encodeURIComponent(id)}`);
                                if (!r.ok) return;
                                const cgData = await r.json();
                                const newDepth = depth + 1;
                                for (const c of cgData.callers || []) {
                                    if (!pInst.getNode(c.id)) {
                                        pInst.addNode({ id: c.id, data: { raw: c, kind: c.is_external ? 'external' : 'caller', depth: newDepth } });
                                    }
                                    const edgeId = `${c.id}->${id}`;
                                    if (!pInst.edges.has(edgeId)) pInst.addEdge({ id: edgeId, from: c.id, to: id });
                                }
                                for (const c of cgData.callees || []) {
                                    if (!pInst.getNode(c.id)) {
                                        pInst.addNode({ id: c.id, data: { raw: c, kind: c.is_external ? 'external' : 'callee', depth: newDepth } });
                                    }
                                    const edgeId = `${id}->${c.id}`;
                                    if (!pInst.edges.has(edgeId)) pInst.addEdge({ id: edgeId, from: id, to: c.id });
                                }
                            } catch (err) {}
                        }
                    }
                });
            }
        } catch (err) {
            loader.innerHTML = `<div style="padding:20px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> ${err.message}</div>`;
        }
    },

    destroy() {
        this.container = null;
        this.params = null;
        
        if (this._selectionChangeListener) {
            document.removeEventListener('selectionchange', this._selectionChangeListener);
            delete this._selectionChangeListener;
        }

        // Restore original global handlers
        window.toggleLock = this._originalToggleLock;
        window.clearAllLocks = this._originalClearAllLocks;
        window.setHighlight = this._originalSetHighlight;
        window.setChunkHighlight = this._originalSetChunkHighlight;
        
        // Clean up global references to prevent memory leak and unexpected behaviors
        delete window.copyDiffCode;
        delete window.updateSimDisplay;
        delete window.onManualInput;
        delete window.onCollChange;
        delete window.onFileChange;
        delete window.onFuncChange;
        delete window.startComparison;
        delete window.toggleBothDetail;
        delete window.navigateToFunction;
    }
};
