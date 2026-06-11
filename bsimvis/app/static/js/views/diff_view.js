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
                        <button id="compare-btn" class="btn-compare" disabled onclick="startComparison()" style="padding:10px 25px; background:var(--accent); color:#000; border:none; border-radius:4px; font-weight:bold; cursor:pointer; font-size:1rem;">Start Comparison</button>
                    </div>
                </div>

                <div id="bsim-scroll" style="display:none; flex:1; overflow:hidden; position:relative;">
                    <div id="bsim-vheight" style="position:absolute; width:1px; top:0; left:0; z-index:-1;"></div>
                    <div id="bsim-sticky" style="display:flex; height:100%; width:100%;">
                        <div style="flex:1; position:relative; display:flex; flex-direction:column; overflow:hidden;">
                            <div class="diff-pane" style="height:100%; overflow:auto;">
                                <div id="bsim-left-content" class="bsim-vcontent c-code-container" style="position:relative;"></div>
                            </div>
                            <button class="floating-copy-btn" title="Copy left code with colors" onclick="copyDiffCode('l', this)">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                        <div style="flex:1; position:relative; display:flex; flex-direction:column; overflow:hidden; border-left:1px solid #3e3d32;">
                            <div class="diff-pane" style="height:100%; overflow:auto;">
                                <div id="bsim-right-content" class="bsim-vcontent c-code-container" style="position:relative;"></div>
                            </div>
                            <button class="floating-copy-btn" title="Copy right code with colors" onclick="copyDiffCode('r', this)">
                                <i class="fas fa-copy"></i>
                            </button>
                        </div>
                    </div>
                </div>

                <div id="bsim-tooltip" class="tooltip" style="display:none; position:fixed; z-index:20000; background:rgba(0,0,0,0.85); padding:10px; border-radius:4px; border:1px solid var(--accent); color:#fff; font-size:0.8rem; pointer-events:none;"></div>
            </div>
        `;

        this.scrollEl = document.getElementById('bsim-scroll');
        this.vHeightEl = document.getElementById('bsim-vheight');
        this.leftContent = document.getElementById('bsim-left-content');
        this.rightContent = document.getElementById('bsim-right-content');
        this.tooltipEl = document.getElementById('bsim-tooltip');

        document.getElementById('meta-container').addEventListener('click', (e) => {
            if (e.target.closest('#swap-btn')) {
                let id1 = this.params.id1 || new URLSearchParams(window.location.search).get('id1');
                let id2 = this.params.id2 || new URLSearchParams(window.location.search).get('id2');
                
                const path = window.location.pathname;
                const parts = path.split('/').filter(Boolean);
                if (parts[0] === 'collection' && parts[2] === 'function') {
                    if (parts.length >= 9 && parts[5] === 'vs') {
                        const collA = decodeURIComponent(parts[1]);
                        const md5A = decodeURIComponent(parts[3]);
                        const addrA = decodeURIComponent(parts[4]);
                        const collB = decodeURIComponent(parts[6]);
                        const md5B = decodeURIComponent(parts[7]);
                        const addrB = decodeURIComponent(parts[8]);
                        id1 = `${collA}:func:${md5A}:${addrA}`;
                        id2 = `${collB}:func:${md5B}:${addrB}`;
                    }
                }

                if (id1 && id2) {
                    const newUrl = buildDiffUrl(id2, id1);
                    Nav.openPath(newUrl);
                }
            }
        });

        await this.fetchData();
    },

    async fetchData() {
        let id1 = this.params.id1 || new URLSearchParams(window.location.search).get('id1');
        let id2 = this.params.id2 || new URLSearchParams(window.location.search).get('id2');
        
        // Try RESTful path parsing /collection/COLL_A/function/MD5_A/ADDR_A/vs/collection/COLL_B/MD5_B/ADDR_B
        const path = window.location.pathname;
        const parts = path.split('/').filter(Boolean);
        if (parts[0] === 'collection' && parts[2] === 'function') {
            if (parts.length >= 9 && parts[5] === 'vs') {
                const collA = decodeURIComponent(parts[1]);
                const md5A = decodeURIComponent(parts[3]);
                const addrA = decodeURIComponent(parts[4]);
                const collB = decodeURIComponent(parts[6]);
                const md5B = decodeURIComponent(parts[7]);
                const addrB = decodeURIComponent(parts[8]);
                id1 = `${collA}:func:${md5A}:${addrA}`;
                id2 = `${collB}:func:${md5B}:${addrB}`;
            } else if (parts.length >= 5) {
                id1 = id1 || decodeURIComponent(parts[3]);
                id2 = id2 || decodeURIComponent(parts[4]);
            }
        }

        const loader = document.getElementById('diff-view-loader');
        const scrollContainer = document.getElementById('bsim-scroll');
        const metaContainer = document.getElementById('meta-container');
        const selectionTool = document.getElementById('selection-tool');
        const simBar = document.getElementById('similarity-bar');

        if (!id1 || !id2) {
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
            const response = await fetch(`/api/diff?id1=${encodeURIComponent(id1)}&id2=${encodeURIComponent(id2)}`);
            if (!response.ok) throw new Error("API Network error");
            const data = await response.json();

            if (window.fetchTagMetadata) {
                await window.fetchTagMetadata(id1.split(':')[0]);
            }
            
            metaContainer.innerHTML = this.formatMetaCard(data.meta1, id1, true) + this.formatMetaCard(data.meta2, id2, false);
            this.fetchSimilarity(id1, id2);
            if (typeof window.updateDiffQueueUI === 'function') window.updateDiffQueueUI();

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

        if (funcCallToken) {
            const calledFuncId = funcCallToken.getAttribute('data-called-func-id');
            const isExternal = funcCallToken.getAttribute('data-is-external') === 'true';
            const targetName = funcCallToken.getAttribute('data-target-name') || calledFuncId.split(':').pop();
            
            if (state) {
                if (calledFuncId && isExternal) {
                    const extName = targetName || calledFuncId.replace('ext:', '');
                    this.showTooltip(`<div style="display:flex;align-items:center;gap:6px;">
                        <span style="background:rgba(249,38,114,0.2);color:#f92672;border:1px solid rgba(249,38,114,0.4);border-radius:4px;padding:2px 7px;font-size:0.7rem;font-weight:600;letter-spacing:0.04em;">EXTERNAL</span>
                        <span style="color:#ccc;font-family:monospace;font-size:0.8rem;">${extName}</span>
                    </div>`, event.clientX, event.clientY);
                } else if (calledFuncId && !isExternal) {
                    if (window.parent && window.parent !== window && typeof window.parent.showCodePreviewFromIframe === 'function') {
                        window.parent.showCodePreviewFromIframe(window.name, calledFuncId, targetName, event);
                    } else if (typeof window.showCodePreview === 'function') {
                        window.showCodePreview(calledFuncId, targetName, null, null, null, event);
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
                const html = this.getHtmlForTooltip(tooltipTarget);
                if (html) this.showTooltip(html, event.clientX, event.clientY);
            } else {
                this.hideTooltip();
            }
        }

        const chunk = this.findInPath(event, '[data-chunk-id]');
        if (chunk && chunk.dataset.chunkId !== undefined) {
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
        const collection = id1.split(':')[0] || 'main';

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
                const url = `/collection/${encodeURIComponent(c)}/feature/${encodeURIComponent(h)}`;
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
        const col = parts[0] || 'main';
        const md5 = parts[2];
        const addr = parts[3];
        const url = `/collection/${encodeURIComponent(col)}/function/${encodeURIComponent(md5)}/${encodeURIComponent(addr)}`;
        
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
        try {
            const res = await fetch(`/api/similarity?id1=${encodeURIComponent(id1)}&id2=${encodeURIComponent(id2)}`);
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

    destroy() {
        this.container = null;
        this.params = null;
        
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
        delete window.toggleLock;
        delete window.clearAllLocks;
        delete window.setHighlight;
        delete window.setChunkHighlight;
    }
};
