/**
 * Function View Module
 * Extracted from function/index.html
 */

window.FunctionView = {
    container: null,
    params: null,
    scrollEl: null,
    vHeightEl: null,
    vContentEl: null,
    tooltipEl: null,
    funcRows: [],
    funcTips: {},
    rowH: 24,
    OVERSCAN: 20,
    id: '',
    neighborsLoaded: false,
    neighborsDebounceTimer: null,
    callGraphLoaded: false,
    graphController: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        const collection = params.collection || '';
        const file_md5 = params.md5 || params.file_md5;
        const address = params.address;

        if (!file_md5 || !address) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: Missing file MD5 or function address.</div>';
            return;
        }

        this.id = `idx:${collection}:func:${file_md5}:${address}`;
        window.currentFuncId = `${collection}:func:${file_md5}:${address}`;
        this.neighborsLoaded = false;
        this.callGraphLoaded = false;
        if (this.graphController) {
            this.graphController.destroy();
            this.graphController = null;
        }

        // Build initial layout
        this.container.innerHTML = `
            <style>
                .bsim-tabbar { display:flex; gap:4px; margin:0 0 10px 0; border-bottom:2px solid var(--border); flex-shrink:0; }
                .bsim-tab {
                    background:none; border:none; border-bottom:3px solid transparent;
                    margin-bottom:-2px; padding:10px 20px; cursor:pointer;
                    color:var(--subtle); font-size:0.9rem; font-weight:600; letter-spacing:0.01em;
                    transition:color 0.15s, border-color 0.15s, background 0.15s;
                }
                .bsim-tab:hover { color:var(--text); background:rgba(255,255,255,0.04); }
                .bsim-tab.active { color:var(--accent); border-bottom-color:var(--accent); }

                .file-func-table { width:100%; border-collapse:collapse; font-size:0.8rem; }
                .file-func-table th { text-align:left; padding:10px; border-bottom:1px solid var(--border); color:var(--subtle); text-transform:uppercase; font-size:0.75rem; letter-spacing:0.05em; }
                .file-func-table td { padding:10px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:middle; }
                .file-func-table tr:hover { background: rgba(255,255,255,0.02); }
            </style>
            <div style="display:flex; flex-direction:column; flex:1; overflow:hidden; height:100%;">
                <div id="function-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                    <i class="fa-solid fa-spinner fa-spin"></i> Loading Function Code...
                </div>
                <div id="function-content" style="display:none; flex:1; flex-direction:column; overflow:hidden; height:100%;">
                    <div id="meta-container"></div>
                    <div class="bsim-tabbar" id="function-view-tabs">
                        <button class="bsim-tab active" id="function-tab-btn-code" onclick="FunctionView.switchTab('code')">Code</button>
                        <button class="bsim-tab" id="function-tab-btn-neighbors" onclick="FunctionView.switchTab('neighbors')">Similar<span id="fn-nbr-count-wrap" style="display:none;"> (<span id="fn-nbr-count">0</span>)</span></button>
                        <button class="bsim-tab" id="function-tab-btn-callgraph" onclick="FunctionView.switchTab('callgraph')">Call Graph</button>
                    </div>

                    <div id="function-panel-code" class="function-view-panel" style="display:flex; flex-direction:column; flex:1; overflow:hidden;">
                        <div id="code-scroll" style="flex: 1; position: relative; overflow-y: auto; background: var(--card-bg);">
                            <div id="v-height" style="position: absolute; width: 1px; top: 0; left: 0; z-index: -1;"></div>
                            <div id="v-content" class="c-code-container" style="position: sticky; top: 0; width: 100%;"></div>
                            <button id="copy-code-btn" class="floating-copy-btn" title="Copy code with colors" onclick="FunctionView.copyFunctionCode(this)">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                    </div>

                    <div id="function-panel-neighbors" class="function-view-panel" style="display:none; flex:1; overflow-y:auto;">
                        <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; display: flex; flex-direction: column; gap: 15px;">
                            <div class="filter-bar" style="gap:20px; padding:0;">
                                <div class="search-input-wrapper">
                                    <input type="text" id="fn-nbr-q" placeholder="Search similar functions by keywords..." oninput="FunctionView.debounceNeighborsSearch()">
                                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="FunctionView.searchNeighbors()" title="Search"></i>
                                </div>
                            </div>
                            <div style="display:flex; gap:20px; flex-wrap:wrap;">
                                <div class="home-card" style="padding:16px; min-width:160px;">
                                    <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Scope</h3>
                                    <input type="hidden" id="fn-nbr-scope" value="collection">
                                    <div id="fn-nbr-scope-pills" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                                </div>
                                <div class="home-card" style="padding:16px; min-width:220px;">
                                    <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Algorithm</h3>
                                    <input type="hidden" id="fn-nbr-algo" value="unweighted_cosine">
                                    <div id="fn-nbr-algo-pills" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                                </div>
                                <div class="home-card" style="padding:16px; min-width:220px;">
                                    <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Cross Binary</h3>
                                    <input type="hidden" id="fn-nbr-cross-binary" value="">
                                    <div id="fn-nbr-cross-binary-pills" style="display:flex; flex-wrap:wrap; gap:8px;"></div>
                                </div>
                                <div class="home-card" style="padding:16px; min-width:100px;">
                                    <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Limit</h3>
                                    <input type="number" id="fn-nbr-limit" value="50" min="1" max="1000" style="width:70px; font-size:0.8rem; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:5px;" oninput="FunctionView.debounceNeighborsSearch()">
                                </div>
                            </div>
                            <div style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
                                <table id="fn-nbr-results-table">
                                    <thead>
                                        <tr>
                                            <th>Score</th>
                                            <th>Function</th>
                                            <th>Addr</th>
                                            <th>Tags</th>
                                            <th>Clusters</th>
                                            <th>Feat</th>
                                            <th>Notes</th>
                                            <th>File</th>
                                            <th>MD5</th>
                                        </tr>
                                        <tr class="filter-row">
                                            <th>
                                                <div style="display:flex; align-items:center; gap:2px;">
                                                    <input type="number" id="fn-nbr-min-score" placeholder="Min..." value="0.9" step="0.05" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <span class="dim" style="font-size:0.6rem">-</span>
                                                    <input type="number" id="fn-nbr-max-score" placeholder="Max..." step="0.05" min="0" max="1" style="font-size:0.65rem; width:48%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                </div>
                                            </th>
                                            <th>
                                                <div style="display:flex; flex-direction:column; gap:2px;">
                                                    <input type="text" id="fn-nbr-name" placeholder="Name..." style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <input type="text" id="fn-nbr-namespace" placeholder="Namespace..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <input type="text" id="fn-nbr-ret-type" placeholder="Return Type..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                </div>
                                            </th>
                                            <th></th>
                                            <th>
                                                <div style="display:flex; flex-direction:column; gap:2px;">
                                                    <input type="text" id="fn-nbr-func-tag" placeholder="Tags..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <input type="text" id="fn-nbr-exclude-func-tag" placeholder="Exclude..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                </div>
                                            </th>
                                            <th>
                                                <div style="display:flex; flex-direction:column; gap:2px;">
                                                    <input type="text" id="fn-nbr-cluster" placeholder="UUID..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <input type="text" id="fn-nbr-cluster-name" placeholder="Name..." style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                    <input type="number" id="fn-nbr-min-cohesion" placeholder="Min Cohesion" step="0.05" min="0" max="1" style="font-size:0.6rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()">
                                                </div>
                                            </th>
                                            <th><input type="number" id="fn-nbr-min-features" placeholder="Min..." min="0" style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()"></th>
                                            <th><input type="text" id="fn-nbr-note-owner" placeholder="Owner..." style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()"></th>
                                            <th><input type="text" id="fn-nbr-file-name" placeholder="File Name..." style="font-size:0.65rem; width:100%; box-sizing:border-box;" oninput="FunctionView.debounceNeighborsSearch()"></th>
                                            <th></th>
                                        </tr>
                                    </thead>
                                    <tbody id="fn-nbr-results-tbody">
                                        <tr><td colspan="9" style="text-align: center; color: var(--dim); padding: 20px;">Loading similar functions...</td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>

                    <div id="function-panel-callgraph" class="function-view-panel" style="display:none; flex:1; overflow:hidden; position:relative;">
                        <div id="fn-cg-toolbar" style="position:absolute; bottom:10px; left:15px; z-index:100; display:flex; align-items:center; gap:14px; background:rgba(0,0,0,0.6); backdrop-filter:blur(4px); padding:6px 12px; border-radius:6px; border:1px solid var(--border); font-size:0.75rem; max-width:calc(100% - 30px); flex-wrap:wrap;">
                            <label style="cursor:pointer; display:flex; align-items:center; gap:5px; color:var(--text); flex-shrink:0;" title="Toggle high-confidence similarity edges">
                                <input type="checkbox" id="fn-cg-sim-toggle" checked onchange="FunctionView.toggleSimilarityEdges(this.checked)">
                                <span>Similarities ⚡</span>
                            </label>
                            <div style="width:1px; align-self:stretch; background:var(--border);"></div>
                            <div id="fn-cg-legend" style="display:flex; align-items:center; gap:10px; color:var(--subtle); flex-wrap:wrap;">${FunctionView.renderLegendHTML()}</div>
                        </div>
                        <div id="fn-cg-loader" style="text-align:center; padding:50px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin"></i> Loading call graph...</div>
                        <div id="fn-cg-container" style="display:none; width:100%; height:100%;"></div>
                    </div>
                </div>
                <div id="bsim-tooltip" class="tooltip" style="display:none; position:fixed; z-index:20000; background:var(--window-bg); padding:10px; border-radius:4px; border:1px solid var(--accent); color:var(--text); font-size:0.8rem; pointer-events:none;"></div>
            </div>
        `;

        this.scrollEl = document.getElementById('code-scroll');
        this.vHeightEl = document.getElementById('v-height');
        this.vContentEl = document.getElementById('v-content');
        this.tooltipEl = document.getElementById('bsim-tooltip');

        try {
            const res = await fetch(`/api/function/code?id=${encodeURIComponent(this.id)}`);
            if (!res.ok) throw new Error("Function not found");
            const data = await res.json();

            this.funcRows = data.rows || [];
            this.funcTips = data.tips || {};
            window.currentFuncName = data.meta?.['function_name'] || 'unknown';
            if (data.meta && data.meta.file_name && file_md5) {
                window.filenameCache = window.filenameCache || {};
                window.filenameCache[file_md5] = data.meta.file_name;
            }

            Breadcrumbs.setFilename(file_md5, data.meta?.file_name || 'File');
            Breadcrumbs.setFuncName(collection, file_md5, address, window.currentFuncName);
            Breadcrumbs.refresh();

            const loader = document.getElementById('function-loader');
            const content = document.getElementById('function-content');
            if (loader) loader.style.display = 'none';
            if (content) content.style.display = 'flex';

            // Render Metadata
            if (typeof window.renderFunctionMetadata === 'function') {
                window.renderFunctionMetadata('meta-container', data.meta, window.currentFuncId, {
                    showFeaturesBtn: true,
                    showDiffBtn: true,
                    diffBtnFullText: false,
                    showSimilarBtn: true
                });
            }

            // Load tags metadata
            if (window.fetchTagMetadata) {
                await window.fetchTagMetadata(collection);
            }

            // Set virtual height
            this.vHeightEl.style.height = (this.funcRows.length * this.rowH) + 'px';

            // Bind Event Listeners
            this.scrollEl.addEventListener('scroll', () => this.onScroll());
            this.scrollEl.addEventListener('pointerover', e => this.handleHoverMove(e, true));
            this.scrollEl.addEventListener('pointerout', e => this.handleHoverMove(e, false));
            this.scrollEl.addEventListener('contextmenu', e => this.showTokenContextMenu(e));
            this.scrollEl.addEventListener('pointermove', e => {
                if (this.tooltipEl.style.display === "block") {
                    this.tooltipEl.style.left = (e.clientX + 15) + "px";
                    this.tooltipEl.style.top = (e.clientY + 15) + "px";
                }
            });

            this.scrollEl.addEventListener('click', e => {
                const token = this.findToken(e);
                if (token) {
                    const calledFuncId = token.getAttribute('data-called-func-id');
                    if (calledFuncId) {
                        const isExternal = token.getAttribute('data-is-external') === 'true';
                        this.navigateToFunction(calledFuncId, isExternal, e);
                        return;
                    }
                    const hashes = token.getAttribute('data-hashes');
                    if (hashes && window.toggleLock) window.toggleLock(hashes, token);
                }
            });

            this.scrollEl.addEventListener('pointermove', e => {
                if (window.parent && window.parent !== window && typeof window.parent.moveCodePreviewFromIframe === 'function') {
                    window.parent.moveCodePreviewFromIframe(window.name, e);
                }
            });

            this.setupKeyboardSelection();
            this.scrollToLine();
            this.onScroll();
            
            // Actually place the cursor in the element so arrow keys work immediately
            this.vContentEl.focus();
            setTimeout(() => {
                const sel = window.getSelection();
                if (!sel.rangeCount || !this.vContentEl.contains(sel.focusNode)) {
                    const range = document.createRange();
                    range.selectNodeContents(this.vContentEl);
                    range.collapse(true);
                    sel.removeAllRanges();
                    sel.addRange(range);
                }
            }, 250);

            // Bind hashchange listener
            this._hashChangeListener = () => this.scrollToLine();
            window.addEventListener('hashchange', this._hashChangeListener);

            // Bind selection tracking for keyboard token preview
            this._selectionChangeListener = () => {
                if (document.activeElement !== this.vContentEl) {
                    if (this._currentKbdToken) {
                        this.handleTokenHover(this._currentKbdToken, false);
                        this._currentKbdToken = null;
                    }
                    return;
                }
                const token = this.findTokenFromSelection();
                if (token !== this._currentKbdToken) {
                    if (this._currentKbdToken) {
                        this.handleTokenHover(this._currentKbdToken, false);
                    }
                    this._currentKbdToken = token;
                    if (this._currentKbdToken) {
                        this.handleTokenHover(this._currentKbdToken, true);
                    }
                }
            };
            document.addEventListener('selectionchange', this._selectionChangeListener);


            // Rich copy support
            if (window.setupRichCopyInterceptor) {
                window.setupRichCopyInterceptor(this.vContentEl, () => this.funcRows);
            }

            // Initialize notes and AI insights side-panel (silent)
            if (typeof window.showNotes === 'function') {
                window.showNotes(window.currentFuncId, false);
            }

        } catch (err) {
            console.error(err);
            const loader = document.getElementById('function-loader');
            if (loader) loader.innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color: #f92672;"></i> ${err.message}`;
        }
    },

    switchTab(tabId) {
        document.querySelectorAll('#function-view-tabs .bsim-tab').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.function-view-panel').forEach(panel => panel.style.display = 'none');

        const btn = document.getElementById(`function-tab-btn-${tabId}`);
        if (btn) btn.classList.add('active');

        const panel = document.getElementById(`function-panel-${tabId}`);
        if (panel) panel.style.display = (tabId === 'code') ? 'flex' : 'block';

        // ponytail: no hash-routing for tabs here -- #L<line> hash is already owned by scrollToLine()
        if (tabId === 'neighbors') this.loadNeighborsPanel();
        if (tabId === 'callgraph') this.loadCallGraphPanel();
    },

    async loadCallGraphPanel() {
        if (this.callGraphLoaded) return;
        this.callGraphLoaded = true;

        const loader = document.getElementById('fn-cg-loader');
        const container = document.getElementById('fn-cg-container');

        try {
            loader.style.display = 'none';
            container.style.display = 'block';

            this.graphController = new PivotickGraphController(container, { collection: this.params.collection });
            await this.graphController.addFunction(this.id, { asCenter: true });
        } catch (err) {
            console.error(err);
            loader.innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
        }
    },

    async toggleSimilarityEdges(show) {
        if (!this.graphController) return;
        await this.graphController.toggleSimilarity(show);
    },

    // Groups caller/callee entries into Pivotick native cluster (parent) nodes
    // by file_md5, one per distinct binary -- 'self' and 'external' entries and
    // anything added later (recursive expansion, similarity hits) stay loose,
    // since Pivotick's children[] shape only exists at node-construction time,
    // not via a documented "add to existing cluster" API.
    // ponytail: only clusters the initial synchronous batch; deeper-expanded
    // nodes render ungrouped. Revisit if that's confusing in practice.
    buildClusteredNodes(entries) {
        const groupable = entries.filter(n => n.data.kind !== 'external' && n.data.raw?.file_md5);
        const binaries = new Set(groupable.map(n => n.data.raw.file_md5));
        if (binaries.size < 2) return entries;

        const byMd5 = new Map();
        const result = [];
        for (const n of entries) {
            const md5 = n.data.raw?.file_md5;
            if (n.data.kind === 'external' || !md5) {
                result.push(n);
                continue;
            }
            if (!byMd5.has(md5)) {
                const cluster = {
                    id: `cluster:${md5}`,
                    data: { kind: 'binary-cluster', raw: { file_md5: md5, file_name: n.data.raw.file_name } },
                    expanded: true,
                    children: [],
                };
                byMd5.set(md5, cluster);
                result.push(cluster);
            }
            byMd5.get(md5).children.push(n);
        }
        return result;
    },

    // Fetches a single function's current BSimVis notes and concatenates them
    // into the markdown content of one Pivotick note bubble. Single source of
    // truth for "what should this function's graph note bubble say right
    // now" -- used both to seed a freshly-built graph and to live-refresh an
    // already-open one when the BSimVis Notes panel changes something.
    async fetchNoteContent(funcId) {
        const apiParamsFn = window.getApiParams || (window.parent && window.parent.getApiParams);
        if (!apiParamsFn) return null;
        const collection = window.getCollectionFromId ? window.getCollectionFromId(funcId) : (this.params.collection || '');
        try {
            const apiParams = apiParamsFn(collection);
            const res = await fetch(`/api/notes/list?${apiParams}&func_id=${encodeURIComponent(funcId)}`);
            const data = await res.json();
            if (data.status !== 'success' || !data.notes || !data.notes.length) return null;
            return data.notes.map(nt => `**${nt.owner}**: ${nt.text}`).join('\n\n---\n\n');
        } catch (e) { return null; }
    },

    // Fetches existing BSimVis notes for each visible node and turns them into
    // Pivotick's native canvas notes (attachedElement links a note bubble to a
    // node) so notes show up right on the graph instead of only in the side panel.
    async fetchGraphNotes(entries) {
        const targets = entries.filter(n => n.data.kind !== 'external' && n.data.kind !== 'binary-cluster');
        const results = await Promise.all(targets.map(async n => {
            const content = await this.fetchNoteContent(n.id);
            if (!content) return null;
            return { id: `bsimnote:${n.id}`, attachedElement: n.id, content, color: '#ffd700' };
        }));
        return results.filter(Boolean);
    },

    // Pivotick's own note bubbles are a first-class canvas feature (drag, edit,
    // markdown render) separate from the BSimVis notes side panel. Forward edits
    // made in the graph back into the same /api/notes/* store so both surfaces
    // read from one source of truth instead of drifting apart.
    wireNoteSync(pInstance) {
        if (!pInstance || typeof pInstance.on !== 'function') return;
        let timer = null;
        const forward = (note) => {
            const funcId = note?.attachedElement;
            if (!funcId || typeof note.content !== 'string') return;
            clearTimeout(timer);
            timer = setTimeout(async () => {
                const collection = window.getCollectionFromId ? window.getCollectionFromId(funcId) : (this.params.collection || '');
                try {
                    await fetch('/api/notes/add', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ collection, func_id: funcId, text: note.content, owner: 'user' }),
                    });
                    if (window.refreshFunctionRow) window.refreshFunctionRow(funcId);
                } catch (e) { console.error('Failed to sync graph note to BSimVis notes:', e); }
            }, 600);
        };
        pInstance.on('noteChange', forward);
        pInstance.on('noteAdd', forward);
    },

    LEGEND_ITEMS: [
        { color: 'var(--accent, #04d9ff)', label: 'Added function(s)' },
        { color: '#a6e22e', label: 'Caller (calls this)' },
        { color: '#f92672', label: 'Callee (called by this)' },
        { color: 'var(--dim, #888)', label: 'External' },
        { color: '#ae81ff', label: 'Similar to — % on edge', dashed: true },
    ],

    renderLegendHTML() {
        return this.LEGEND_ITEMS.map(i => `<span style="display:flex; align-items:center; gap:4px; white-space:nowrap;">
            <span style="width:10px; height:${i.dashed ? '0' : '10px'}; ${i.dashed ? `border-top:2px dashed ${i.color};` : `border-radius:50%; background:${i.color};`}"></span>
            ${escapeHtml(i.label)}
        </span>`).join('');
    },

    renderEdgeLabel(edge) {
        const d = (edge.getData && edge.getData()) || edge.data || {};
        if (d.kind !== 'similarity' || typeof d.score !== 'number') return '';
        // A solid filled pill on every similarity edge reads as visual noise once
        // there's more than a couple of them -- an outlined, low-contrast tag is
        // still readable but doesn't compete with the nodes for attention.
        const div = document.createElement('div');
        div.style.cssText = 'background:rgba(30,30,46,0.75); color:#ae81ff; border:1px solid rgba(174,129,255,0.5); font:9px/1 monospace; padding:1px 5px; border-radius:6px; white-space:nowrap;';
        div.textContent = Math.round(d.score * 100) + '%';
        return div;
    },

    callGraphRenderNode(raw, kind) {
        if (kind === 'binary-cluster') {
            const label = escapeHtml(raw?.file_name || (raw?.file_md5 || '').slice(0, 10) || 'binary');
            const div = document.createElement('div');
            div.style.cssText = 'padding:6px 10px; border-radius:8px; border:2px dashed #66d9ef; background:var(--card-bg, #222); font:11px/1.3 monospace; color:#66d9ef; font-weight:bold; white-space:nowrap; display:flex; align-items:center; gap:5px;';
            div.innerHTML = `<i class="fa-solid fa-file-binary"></i>${label}`;
            return div;
        }

        const name = escapeHtml(raw?.name || (raw?.id || '').split(':').pop() || '?');
        const border = { self: 'var(--accent, #04d9ff)', added: 'var(--accent, #04d9ff)', caller: '#a6e22e', callee: '#f92672', external: 'var(--dim, #888)', similar: '#ae81ff' }[kind] || '#fff';
        const lineCss = 'white-space:nowrap; overflow:hidden; text-overflow:ellipsis;';
        const fileName = raw?.file_name ? escapeHtml(raw.file_name) : '';
        const fileHtml = fileName ? `<div style="${lineCss} color:var(--dim,#888); opacity:0.75; font-size:8.5px; margin-top:1px;"><i class="fa-solid fa-file-binary" style="margin-right:3px;"></i>${fileName}</div>` : '';
        const div = document.createElement('div');
        // Wide enough that a typical name+signature fits without truncating --
        // still fixed-width (not auto) since Pivotick derives its edge-attachment
        // radius from max(width,height)/2, so a wildly variable box width throws
        // off the anchor point on tall/short neighbors.
        div.style.cssText = `width:190px; padding:5px 8px; border-radius:8px; border-left:3px solid ${border}; background:var(--card-bg, #222); font:12px/1.35 monospace; cursor:pointer; box-shadow:0 1px 3px rgba(0,0,0,0.4);`;

        if (!raw || raw.is_external) {
            div.innerHTML = `<div style="${lineCss} color:${border}; font-weight:bold;">${name}</div>${raw?.is_external ? `<div style="${lineCss} color:var(--dim,#888); font-size:9px;">EXT</div>` : ''}`;
            return div;
        }

        const params = (raw.parameters || []).map(p => (typeof p === 'object' && p !== null) ? (p.name || '...') : p);
        const paramHtml = params.map(p => `<span style="color:#ae81ff;">${escapeHtml(p)}</span>`).join('<span style="color:#fff;">, </span>');
        const nsHtml = raw.namespace ? `<span style="color:#fff; opacity:0.8;">${escapeHtml(raw.namespace)}::</span>` : '';
        const retHtml = raw.return_type ? `<span style="color:#ae81ff; opacity:0.85; font-size:9.5px;">${escapeHtml(raw.return_type)}</span>` : '';

        div.innerHTML = `<div style="${lineCss} color:${border}; font-weight:bold; font-size:12px;">${nsHtml}${name}</div>`
            + `<div style="${lineCss} font-size:9.5px;">${retHtml} <span style="color:#fff; opacity:0.7;">(</span>${paramHtml}<span style="color:#fff; opacity:0.7;">)</span></div>`
            + fileHtml;
        return div;
    },

    async loadNeighborsPanel() {
        if (this.neighborsLoaded) return;
        this.neighborsLoaded = true;

        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const scopeEl = document.getElementById('fn-nbr-scope');
        if (scopeEl) scopeEl.value = poolId ? 'pool' : 'collection';
        this.renderAllNeighborPills();

        await this.searchNeighbors();
    },

    debounceNeighborsSearch() {
        if (this.neighborsDebounceTimer) clearTimeout(this.neighborsDebounceTimer);
        this.neighborsDebounceTimer = setTimeout(() => this.searchNeighbors(), 400);
    },

    async searchNeighbors() {
        const tbody = document.getElementById('fn-nbr-results-tbody');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: var(--dim); padding: 20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading similar functions...</td></tr>';

        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const address = this.params.address;
        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const scope = document.getElementById('fn-nbr-scope')?.value || (poolId ? 'pool' : 'collection');

        const qs = new URLSearchParams();
        qs.set('md5', file_md5);
        qs.set('address', address);
        if (scope === 'pool' && poolId) qs.set('pool', poolId);
        else qs.set('collection', collection);

        qs.set('algo', document.getElementById('fn-nbr-algo')?.value || 'unweighted_cosine');
        qs.set('min_score', document.getElementById('fn-nbr-min-score')?.value || '0.9');
        qs.set('min_cohesion', document.getElementById('fn-nbr-min-cohesion')?.value || '0.5');
        qs.set('min_features', document.getElementById('fn-nbr-min-features')?.value || '0');

        const setIfVal = (id, key) => {
            const v = document.getElementById(id)?.value;
            if (v) qs.set(key, v);
        };
        setIfVal('fn-nbr-q', 'q');
        setIfVal('fn-nbr-max-score', 'max_score');
        setIfVal('fn-nbr-cross-binary', 'cross_binary');
        setIfVal('fn-nbr-name', 'name');
        setIfVal('fn-nbr-namespace', 'namespace');
        setIfVal('fn-nbr-ret-type', 'ret_type');
        setIfVal('fn-nbr-cluster', 'cluster_uuid');
        setIfVal('fn-nbr-cluster-name', 'cluster_name');
        setIfVal('fn-nbr-note-owner', 'note_owner');
        setIfVal('fn-nbr-file-name', 'file_name');
        setIfVal('fn-nbr-language', 'language');
        qs.set('limit', document.getElementById('fn-nbr-limit')?.value || '50');

        const tagList = (id) => (document.getElementById(id)?.value || '').split(',').map(s => s.trim()).filter(Boolean);
        tagList('fn-nbr-func-tag').forEach(t => qs.append('func_tag', t));
        tagList('fn-nbr-exclude-func-tag').forEach(t => qs.append('exclude_func_tag', t));

        try {
            const res = await fetch(`/api/similarity/search?${qs.toString()}`);
            if (!res.ok) throw new Error("Neighbors search failed");
            const data = await res.json();
            const items = data.pairs || data.items || data.results || [];
            const html = window.renderTopCorrelations ? window.renderTopCorrelations(items, {}, file_md5, address) : '';
            tbody.innerHTML = html || '<tr><td colspan="9" style="text-align: center; color: var(--dim); padding: 20px;">No similar functions found.</td></tr>';
            const countEl = document.getElementById('fn-nbr-count');
            if (countEl) countEl.innerText = data.total ?? items.length;
            const countWrap = document.getElementById('fn-nbr-count-wrap');
            if (countWrap) countWrap.style.display = 'inline';
            if (window.TableSelection) new window.TableSelection('fn-nbr-results-table');
        } catch (e) {
            console.error(e);
            tbody.innerHTML = `<tr><td colspan="9" style="text-align: center; color:#f92672; padding: 20px;"><i class="fa-solid fa-circle-exclamation"></i> Error loading similar functions: ${e.message}</td></tr>`;
        }
    },

    // Generic pill group: [{v, label, icon, disabled}], one hidden input holds
    // the active value, one container div renders the pills. Shared shape for
    // Scope/Algorithm/Cross Binary/Match Mode -- none of these carry counts
    // (unlike bin-sim's Scoring Metric cards), so no extra fetches here.
    renderNeighborPillGroup(inputId, containerId, options, color) {
        const el = document.getElementById(containerId);
        if (!el || !window.binSimPillStyle) return;
        const active = document.getElementById(inputId)?.value ?? '';
        el.innerHTML = options.map(o => `<span class="bsim-tag-pill" style="${window.binSimPillStyle(o.v === active, color)}${o.disabled ? ' opacity:0.4; cursor:not-allowed;' : ''}" title="${escapeAttr(o.label)}" ${o.disabled ? '' : `onclick="FunctionView.setNeighborPill('${inputId}', '${containerId}', '${o.v}')"`}><i class="${o.icon}"></i>${o.label}</span>`).join('');
    },

    setNeighborPill(inputId, containerId, value) {
        const el = document.getElementById(inputId);
        if (el) el.value = value;
        if (inputId === 'fn-nbr-scope') this.renderScopePills();
        else if (inputId === 'fn-nbr-algo') this.renderNeighborPillGroup(inputId, containerId, this.ALGO_OPTIONS, 'var(--info, #3b82f6)');
        else if (inputId === 'fn-nbr-cross-binary') this.renderNeighborPillGroup(inputId, containerId, this.CROSS_BINARY_OPTIONS, 'var(--warning, #d97706)');
        this.searchNeighbors();
    },

    ALGO_OPTIONS: [
        { v: 'unweighted_cosine', label: 'Cosine', icon: 'fa-solid fa-arrows-left-right' },
        { v: 'jaccard', label: 'Jaccard', icon: 'fa-solid fa-object-group' },
    ],
    CROSS_BINARY_OPTIONS: [
        { v: '', label: 'All Binaries', icon: 'fa-solid fa-globe' },
        { v: 'false', label: 'Same Binary', icon: 'fa-solid fa-file' },
        { v: 'true', label: 'Cross Binary', icon: 'fa-solid fa-shuffle' },
    ],

    renderScopePills() {
        const el = document.getElementById('fn-nbr-scope-pills');
        if (!el || !window.binSimPillStyle) return;
        const poolId = window.getRoutingState ? window.getRoutingState().pool : null;
        const active = document.getElementById('fn-nbr-scope')?.value || (poolId ? 'pool' : 'collection');
        const options = [
            { v: 'collection', label: 'Collection', icon: 'fa-solid fa-database' },
            { v: 'pool', label: 'Pool', icon: 'fa-solid fa-layer-group', disabled: !poolId },
        ];
        el.innerHTML = options.map(o => `<span class="bsim-tag-pill" style="${window.binSimPillStyle(o.v === active, 'var(--success)')}${o.disabled ? ' opacity:0.4; cursor:not-allowed;' : ''}" title="${o.disabled ? 'No pool in this context' : escapeAttr(o.label)}" ${o.disabled ? '' : `onclick="FunctionView.setNeighborPill('fn-nbr-scope', 'fn-nbr-scope-pills', '${o.v}')"`}><i class="${o.icon}"></i>${o.label}</span>`).join('');
    },

    renderAllNeighborPills() {
        this.renderScopePills();
        this.renderNeighborPillGroup('fn-nbr-algo', 'fn-nbr-algo-pills', this.ALGO_OPTIONS, 'var(--info, #3b82f6)');
        this.renderNeighborPillGroup('fn-nbr-cross-binary', 'fn-nbr-cross-binary-pills', this.CROSS_BINARY_OPTIONS, 'var(--warning, #d97706)');
    },

    copyFunctionCode(btn) {
        if (!this.funcRows || this.funcRows.length === 0) return;
        if (typeof window.copyRichText === 'function') {
            window.copyRichText(this.funcRows, btn);
        }
    },

    scrollToLine() {
        const hash = window.location.hash;
        if (hash && hash.startsWith('#L')) {
            const lineParts = hash.substring(2).split(',');
            window.targetLineSet = new Set(lineParts.map(l => parseInt(l)));

            setTimeout(() => {
                const firstLine = parseInt(lineParts[0]);
                const idx = this.funcRows.findIndex(r => r.line_idx === firstLine);
                if (idx !== -1) {
                    const st = Math.max(0, (idx * this.rowH) - (this.scrollEl.clientHeight / 3));
                    this.scrollEl.scrollTop = st;
                    this.onScroll();
                }
            }, 50);
        }
    },

    onScroll() {
        if (!this.scrollEl) return;
        const st = this.scrollEl.scrollTop;
        const vh = this.scrollEl.clientHeight;
        const start = Math.max(0, Math.floor(st / this.rowH) - this.OVERSCAN);
        const end = Math.min(this.funcRows.length, Math.ceil((st + vh) / this.rowH) + this.OVERSCAN);
        this.renderRows(start, end);

        const offset = (start * this.rowH) - st;
        this.vContentEl.style.transform = `translateY(${offset}px)`;
    },

    renderRows(start, end) {
        const currentLines = Array.from(this.vContentEl.children);
        const lineMap = new Map();
        currentLines.forEach(el => {
            const lnEl = el.querySelector('.line-num');
            if (lnEl) lineMap.set(parseInt(lnEl.innerText), el);
        });

        const neededLines = [];
        for (let i = start; i < end; i++) {
            const row = this.funcRows[i];
            let lineEl = lineMap.get(row.line_idx);
            
            if (!lineEl) {
                const isTarget = window.targetLineSet && window.targetLineSet.has(row.line_idx);
                let content = '';
                for (const t of row.tokens) {
                    if (window.renderTokenHtml) {
                        content += window.renderTokenHtml(t, { inlineHoverHandlers: false });
                    }
                }
                lineEl = document.createElement('div');
                lineEl.className = `code-line ${isTarget ? 'target-highlight' : ''}`;
                lineEl.innerHTML = `<div class="gutter" contenteditable="false"><div class="line-num">${row.line_idx}</div></div><div class="line-content">${content}</div>`;
            }
            neededLines.push(lineEl);
            lineMap.delete(row.line_idx);
        }

        lineMap.forEach(el => el.remove());

        for (let i = 0; i < neededLines.length; i++) {
            const line = neededLines[i];
            if (this.vContentEl.children[i] !== line) {
                this.vContentEl.insertBefore(line, this.vContentEl.children[i] || null);
            }
        }
        
        if (window.applyLocks) {
            window.applyLocks(this.vContentEl);
        }
    },

    setupKeyboardSelection() {
        this.vContentEl.setAttribute('contenteditable', 'true');
        this.vContentEl.setAttribute('spellcheck', 'false');
        
        this.vContentEl.addEventListener('keydown', (e) => {
            const isCmd = e.ctrlKey || e.metaKey;
            const key = e.key;

            if (key === 'Enter') {
                e.preventDefault();
                const token = this.findTokenFromSelection();
                if (token) {
                    const calledFuncId = token.getAttribute('data-called-func-id');
                    if (calledFuncId) {
                        const isExternal = token.getAttribute('data-is-external') === 'true';
                        this.navigateToFunction(calledFuncId, isExternal, e);
                        return;
                    }
                    const hashes = token.getAttribute('data-hashes');
                    if (hashes && window.toggleLock) window.toggleLock(hashes, token);
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
                requestAnimationFrame(() => {
                    const sel = window.getSelection();
                    if (!sel.rangeCount) return;
                    const rect = sel.getRangeAt(0).getBoundingClientRect();
                    
                    if (rect.height === 0 || (rect.top === 0 && rect.left === 0)) return;

                    const containerRect = this.scrollEl.getBoundingClientRect();
                    const padding = 60;

                    if (rect.top < containerRect.top + padding) {
                        this.scrollEl.scrollTop -= (containerRect.top + padding - rect.top);
                    } else if (rect.bottom > containerRect.bottom - padding) {
                        this.scrollEl.scrollTop += (rect.bottom - (containerRect.bottom - padding));
                    }
                });
                return;
            }

            if (!isCmd) e.preventDefault();
        });

        this.vContentEl.addEventListener('beforeinput', (e) => {
            e.preventDefault();
        });
    },

    findToken(e) {
        const path = e.composedPath ? e.composedPath() : [];
        for (const el of path) {
            if (el instanceof Element && el.classList.contains('token')) return el;
        }
        return null;
    },

    findTokenFromSelection() {
        const sel = window.getSelection();
        if (!sel || !sel.rangeCount) return null;
        let node = sel.focusNode;
        while (node && node !== this.vContentEl) {
            if (node instanceof Element && node.classList.contains('token')) return node;
            node = node.parentNode;
        }
        return null;
    },

    handleHoverMove(e, state) {
        const token = this.findToken(e);
        if (!token) return;
        this.handleTokenHover(token, state, e);
    },

    handleTokenHover(token, state, e) {
        const hashes = token.getAttribute('data-hashes');
        if (hashes && window.setHighlight) window.setHighlight(hashes, state, token);

        const calledFuncId = token.getAttribute('data-called-func-id');
        const isExternal = token.getAttribute('data-is-external') === 'true';

        if (state) {
            let x = e ? e.clientX : undefined;
            let y = e ? e.clientY : undefined;
            if (x === undefined || y === undefined) {
                const rect = token.getBoundingClientRect();
                x = rect.left + rect.width / 2;
                y = rect.bottom;
            }

            if (calledFuncId && isExternal) {
                const extName = token.getAttribute('data-target-name') || calledFuncId.replace('ext:', '');
                this.tooltipEl.innerHTML = `<div style="display:flex;align-items:center;gap:6px;">
                    <span style="background:color-mix(in srgb, var(--token-instruction) 20%, transparent);color:#f92672;border:1px solid color-mix(in srgb, var(--token-instruction) 40%, transparent);border-radius:4px;padding:2px 7px;font-size:0.7rem;font-weight:600;letter-spacing:0.04em;">EXTERNAL</span>
                    <span style="color:var(--meta-text-muted);font-family:monospace;font-size:0.8rem;">${extName}</span>
                </div>`;
                this.tooltipEl.style.display = 'block';
                this.tooltipEl.style.left = (x + 15) + 'px';
                this.tooltipEl.style.top = (y + 15) + 'px';
            } else if (calledFuncId && !isExternal) {
                this.tooltipEl.style.display = 'none';
                const targetName = token.getAttribute('data-target-name') || calledFuncId.split(':').pop();
                
                const fakeEvent = { clientX: x, clientY: y, target: token, currentTarget: token };

                if (window.parent && window.parent !== window && typeof window.parent.showCodePreviewFromIframe === 'function') {
                    window.parent.showCodePreviewFromIframe(window.name, calledFuncId, targetName, fakeEvent);
                } else if (typeof window.showCodePreview === 'function') {
                    window.showCodePreview(calledFuncId, targetName, null, null, null, fakeEvent);
                }
            } else {
                const idx = token.getAttribute('data-idx');
                const data = this.funcTips[idx];
                if (data) {
                    let h = `<b>Type:</b> ${data[0]}<br><b>Seq:</b> ${data[1]}<hr>`;
                    for (const f of data[2]) {
                        h += `<div style="margin-bottom:8px;"><b style='color:#66d9ef'>${f[0]}</b><br>Type: ${f[3]}<br>Op: ${f[1]}<br>Addr: ${f[5]}<br><b>TF:</b> ${f[7]}</div>`;
                    }
                    this.tooltipEl.innerHTML = h;
                    this.tooltipEl.style.display = 'block';
                    this.tooltipEl.style.left = (x + 15) + 'px';
                    this.tooltipEl.style.top = (y + 15) + 'px';
                }
            }
        } else {
            this.tooltipEl.style.display = 'none';
            if (calledFuncId && !isExternal) {
                if (window.parent && window.parent !== window && typeof window.parent.hideCodePreview === 'function') {
                    window.parent.hideCodePreview();
                } else if (typeof window.hideCodePreview === 'function') {
                    window.hideCodePreview();
                }
            }
        }
    },

    showTokenContextMenu(e) {
        const token = e.target.closest('.feature-highlight');
        if (!token) return;
        e.preventDefault();

        const idx = token.getAttribute('data-idx');
        const data = this.funcTips[idx];
        if (!data || !data[2]) return;

        let menu = document.getElementById('token-context-menu');
        if (!menu) {
            menu = document.createElement('div');
            menu.id = 'token-context-menu';
            menu.className = 'context-menu';
            document.body.appendChild(menu);
        }

        const collection = this.id.split(':')[1] || '';

        let html = `<div class="context-menu-header">Select Feature to Analyze</div>`;
        data[2].forEach(f => {
            const hash = f[0], op = f[1], type = f[3], tf = f[7] || 0, color = f[8] || 'var(--accent)';
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
        const rect = menu.getBoundingClientRect();
        const w = rect.width || 350;
        const h = rect.height || (data[2].length * 52 + 40);

        if (x + w > window.innerWidth) x = window.innerWidth - w - 10;
        if (y + h > window.innerHeight) y = window.innerHeight - h - 10;

        menu.style.left = Math.max(10, x) + 'px';
        menu.style.top = Math.max(10, y) + 'px';

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

    navigateToFunction(funcId, isExternal, e) {
        if (isExternal) return;
        const parts = funcId.split(':');
        const col = parts[0];
        const md5 = parts[2];
        const addr = parts[3];
        const url = `/collections/${encodeURIComponent(col)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(addr)}`;

        Nav.openPath(url, e, { title: funcId.split(':').pop(), type: 'function' });
    },

    destroy() {
        this.container = null;
        this.params = null;
        this.scrollEl = null;
        this.vHeightEl = null;
        this.vContentEl = null;
        this.tooltipEl = null;
        this.funcRows = [];
        this.funcTips = {};
        this.neighborsLoaded = false;
        if (this.neighborsDebounceTimer) clearTimeout(this.neighborsDebounceTimer);
        this.callGraphLoaded = false;
        if (this.graphController) {
            this.graphController.destroy();
            this.graphController = null;
        }

        if (this._hashChangeListener) {
            window.removeEventListener('hashchange', this._hashChangeListener);
            delete this._hashChangeListener;
        }

        if (this._selectionChangeListener) {
            document.removeEventListener('selectionchange', this._selectionChangeListener);
            delete this._selectionChangeListener;
        }
    }
};
