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

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        const collection = params.collection || 'main';
        const file_md5 = params.md5 || params.file_md5;
        const address = params.address;

        if (!file_md5 || !address) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: Missing file MD5 or function address.</div>';
            return;
        }

        this.id = `idx:${collection}:func:${file_md5}:${address}`;
        window.currentFuncId = `${collection}:func:${file_md5}:${address}`;
        
        // Build initial layout
        this.container.innerHTML = `
            <div style="display:flex; flex-direction:column; flex:1; overflow:hidden; height:100%;">
                <div id="function-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                    <i class="fa-solid fa-spinner fa-spin"></i> Loading Function Code...
                </div>
                <div id="function-content" style="display:none; flex:1; flex-direction:column; overflow:hidden; height:100%;">
                    <div id="meta-container"></div>
                    <div id="code-scroll" style="flex: 1; position: relative; overflow-y: auto; background: #272822;">
                        <div id="v-height" style="position: absolute; width: 1px; top: 0; left: 0; z-index: -1;"></div>
                        <div id="v-content" class="c-code-container" style="position: sticky; top: 0; width: 100%;"></div>
                        <button id="copy-code-btn" class="floating-copy-btn" title="Copy code with colors" onclick="FunctionView.copyFunctionCode(this)">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                </div>
                <div id="bsim-tooltip" class="tooltip" style="display:none; position:fixed; z-index:20000; background:rgba(0,0,0,0.85); padding:10px; border-radius:4px; border:1px solid var(--accent); color:#fff; font-size:0.8rem; pointer-events:none;"></div>
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

            const loader = document.getElementById('function-loader');
            const content = document.getElementById('function-content');
            if (loader) loader.style.display = 'none';
            if (content) content.style.display = 'flex';

            // Update breadcrumb with actual function name in the global breadcrumbs container
            const bcCurrent = document.querySelector('#breadcrumbs-container .breadcrumb-item.current');
            if (bcCurrent) bcCurrent.innerHTML = `<i class="fa-solid fa-code"></i><span>${window.currentFuncName}</span>`;

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

    handleHoverMove(e, state) {
        const token = this.findToken(e);
        if (!token) return;

        const hashes = token.getAttribute('data-hashes');
        if (hashes && window.setHighlight) window.setHighlight(hashes, state, token);

        const calledFuncId = token.getAttribute('data-called-func-id');
        const isExternal = token.getAttribute('data-is-external') === 'true';

        if (state) {
            if (calledFuncId && isExternal) {
                const extName = token.getAttribute('data-target-name') || calledFuncId.replace('ext:', '');
                this.tooltipEl.innerHTML = `<div style="display:flex;align-items:center;gap:6px;">
                    <span style="background:rgba(249,38,114,0.2);color:#f92672;border:1px solid rgba(249,38,114,0.4);border-radius:4px;padding:2px 7px;font-size:0.7rem;font-weight:600;letter-spacing:0.04em;">EXTERNAL</span>
                    <span style="color:#ccc;font-family:monospace;font-size:0.8rem;">${extName}</span>
                </div>`;
                this.tooltipEl.style.display = 'block';
                this.tooltipEl.style.left = (e.clientX + 15) + 'px';
                this.tooltipEl.style.top = (e.clientY + 15) + 'px';
            } else if (calledFuncId && !isExternal) {
                this.tooltipEl.style.display = 'none';
                const targetName = token.getAttribute('data-target-name') || calledFuncId.split(':').pop();
                if (window.parent && window.parent !== window && typeof window.parent.showCodePreviewFromIframe === 'function') {
                    window.parent.showCodePreviewFromIframe(window.name, calledFuncId, targetName, e);
                } else if (typeof window.showCodePreview === 'function') {
                    window.showCodePreview(calledFuncId, targetName, null, null, null, e);
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
                    this.tooltipEl.style.left = (e.clientX + 15) + 'px';
                    this.tooltipEl.style.top = (e.clientY + 15) + 'px';
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

        const collection = this.id.split(':')[1] || 'main';

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

    navigateToFunction(funcId, isExternal, e) {
        if (isExternal) return;
        const parts = funcId.split(':');
        const col = parts[0];
        const md5 = parts[2];
        const addr = parts[3];
        const url = `/collection/${encodeURIComponent(col)}/function/${encodeURIComponent(md5)}/${encodeURIComponent(addr)}`;

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
    }
};
