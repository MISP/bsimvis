/**
 * Global Feature View Module
 * Replaces the static feature/index.html and integrates into SPA dashboard
 */

window.FeatureView = {
    container: null,
    params: null,
    currentOffset: 0,
    totalOccurrences: 0,
    sourceCache: new Map(),
    featureHash: null,
    collection: null,
    PAGE_SIZE: 50,
    CONTEXT_WINDOW: 2,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        this.collection = params.collection || '';
        this.featureHash = params.hash || params.hash_val;
        this.currentOffset = 0;
        this.totalOccurrences = 0;
        this.sourceCache.clear();

        if (!this.featureHash) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No feature hash provided.</div>';
            return;
        }

        // Build HTML structure and scoped style
        this.container.innerHTML = `
            <style>
                .feature-table-container {
                    background: var(--card-bg);
                    border: 1px solid var(--border);
                    border-radius: 8px;
                    margin-top: 15px;
                    overflow: auto;
                    flex: 1;
                }

                .feature-table-container table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.85rem;
                    table-layout: fixed;
                }

                .feature-table-container th {
                    background: var(--window-tray);
                    color: var(--accent);
                    padding: 12px;
                    text-align: left;
                    border-bottom: 2px solid var(--border);
                    font-size: 0.7rem;
                    text-transform: uppercase;
                    position: sticky;
                    top: 0;
                    z-index: 2;
                }

                .feature-table-container td {
                    padding: 15px 12px;
                    border-bottom: 1px solid var(--border);
                    vertical-align: top;
                }

                .feature-table-container tr:hover {
                    background: var(--hover);
                }

                .hash-badge {
                    font-family: monospace;
                    color: #ae81ff;
                    background: color-mix(in srgb, var(--token-address) 10%, transparent);
                    padding: 2px 6px;
                    border-radius: 4px;
                    border: 1px solid color-mix(in srgb, var(--token-address) 20%, transparent);
                }

                .op-badge {
                    background: #f92672;
                    color: var(--window-tray);
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-weight: bold;
                    font-size: 0.7rem;
                    font-family: monospace;
                    display: inline-block;
                }

                .feature-primary {
                    border-bottom: 2px solid #a6e22e;
                    background: color-mix(in srgb, var(--token-symbol) 15%, transparent);
                    border-radius: 2px;
                }

                .feature-secondary {
                    border-bottom: 2px solid #f92672;
                    background: color-mix(in srgb, var(--token-instruction) 10%, transparent);
                    border-radius: 2px;
                }

                .bsim-group-active-unique {
                    background: color-mix(in srgb, var(--token-symbol) 25%, transparent) !important;
                    border-bottom: 2px solid #a6e22e !important;
                    border-radius: 2px;
                }

                .origin-id {
                    font-size: 0.65rem;
                    color: var(--subtle);
                    word-break: break-all;
                    max-width: 250px;
                    display: block;
                    margin-top: 5px;
                }

                .btn-code-action {
                    background: color-mix(in srgb, var(--token-register) 10%, transparent);
                    color: var(--accent);
                    border: 1px solid color-mix(in srgb, var(--token-register) 30%, transparent);
                    border-radius: 4px;
                    padding: 3px 8px;
                    font-size: 0.75rem;
                    cursor: pointer;
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    border-style: solid;
                }

                .btn-code-action:hover {
                    background: color-mix(in srgb, var(--token-register) 20%, transparent);
                }

                .btn-sim-action {
                    background: color-mix(in srgb, var(--token-address) 10%, transparent);
                    color: #ae81ff;
                    border: 1px solid color-mix(in srgb, var(--token-address) 30%, transparent);
                    border-radius: 4px;
                    padding: 3px 8px;
                    font-size: 0.75rem;
                    cursor: pointer;
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    border-style: solid;
                }

                .btn-sim-action:hover {
                    background: color-mix(in srgb, var(--token-address) 20%, transparent);
                }

                .btn-diff-action {
                    background: color-mix(in srgb, var(--token-symbol) 10%, transparent);
                    color: #a6e22e;
                    border: 1px solid color-mix(in srgb, var(--token-symbol) 30%, transparent);
                    border-radius: 4px;
                    padding: 3px 8px;
                    font-size: 0.75rem;
                    cursor: pointer;
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    border-style: solid;
                }

                .btn-diff-action:hover {
                    background: color-mix(in srgb, var(--token-symbol) 20%, transparent);
                }
                
                .btn-diff-action.active {
                    background: #a6e22e;
                    color: var(--window-tray);
                }

                .feat-tooltip {
                    display: none;
                    position: fixed;
                    z-index: 20000;
                    background: var(--window-bg);
                    padding: 12px;
                    border-radius: 6px;
                    border: 1px solid var(--accent);
                    color: var(--text);
                    font-size: 0.8rem;
                    pointer-events: none;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.5);
                    max-width: 320px;
                }
            </style>

            <div style="display:flex; flex-direction:column; flex:1; overflow:hidden; height:100%; padding: 0 0 20px 0;">
                <div class="header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:15px; flex-shrink:0;">
                    <div>
                        <h1 id="feat-title-text" style="color:var(--accent); margin:0; font-size:1.4rem;">Global Feature Analysis</h1>
                        <div id="feat-meta-info" style="margin-top:8px; font-size:0.85rem;"></div>
                    </div>
                </div>

                <div class="feature-table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 220px;">Function Origin</th>
                                <th style="width: 110px;">Type & Op</th>
                                <th style="width: 70px;">Sequence</th>
                                <th style="width: 160px;">PCode</th>
                                <th style="width: 180px;">PCode Context</th>
                                <th style="width: 50px; text-align: center;">TF</th>
                                <th>Source Context (Highlighted)</th>
                            </tr>
                        </thead>
                        <tbody id="feat-features-body">
                            <tr>
                                <td colspan="7" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                                    <i class="fa-solid fa-spinner fa-spin"></i> Querying Redis & Mapping Source Context...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div id="feat-load-more-container" style="text-align: center; margin-top: 20px; flex-shrink:0; display: none;">
                    <button id="feat-load-more-btn" style="
                        background: var(--card-bg);
                        color: var(--accent);
                        border: 1px solid var(--border);
                        padding: 10px 24px;
                        border-radius: 6px;
                        cursor: pointer;
                        font-family: inherit;
                        font-weight: bold;
                        transition: all 0.2s;
                    ">Load More Occurrences (<span id="feat-count-remaining">0</span> remaining)</button>
                </div>
            </div>

            <div id="feat-tooltip" class="feat-tooltip"></div>
        `;

        // Bind DOM elements
        this.tooltipEl = this.container.querySelector('#feat-tooltip');
        this.loadMoreBtn = this.container.querySelector('#feat-load-more-btn');
        this.loadMoreContainer = this.container.querySelector('#feat-load-more-container');
        this.featuresBody = this.container.querySelector('#feat-features-body');

        this.loadMoreBtn.addEventListener('click', () => this.fetchPage());
        this.container.addEventListener('mousemove', e => this.handleMouseMove(e));
        this.container.addEventListener('contextmenu', e => this.handleContextMenu(e));

        // Start fetching
        await this.fetchPage();
    },

    async fetchSource(funcId) {
        if (!funcId) return null;
        if (this.sourceCache.has(funcId)) return this.sourceCache.get(funcId);
        try {
            const res = await fetch(`/api/function/code?id=${encodeURIComponent(funcId)}`);
            const data = await res.json();
            this.sourceCache.set(funcId, data);
            return data;
        } catch (e) {
            console.error("Failed to fetch source for", funcId, e);
            return null;
        }
    },

    async fetchPage() {
        if (!this.featureHash || !this.collection) return;

        if (this.loadMoreBtn) {
            this.loadMoreBtn.disabled = true;
            this.loadMoreBtn.innerText = 'Loading...';
        }

        try {
            const res = await fetch(`/api/feature/details/${this.featureHash}?collection=${this.collection}&offset=${this.currentOffset}&limit=${this.PAGE_SIZE}`);
            const data = await res.json();

            this.totalOccurrences = data.total_occurrences || 0;
            const occurrences = data.occurrences || [];

            if (this.currentOffset === 0) {
                this.featuresBody.innerHTML = '';
                const fullId = `${this.collection}:feature:${this.featureHash}`;
                
                const titleTextEl = this.container.querySelector('#feat-title-text');
                const metaInfoEl = this.container.querySelector('#feat-meta-info');

                if (titleTextEl) {
                    titleTextEl.innerHTML = `Feature occurrences: <span class="hash-badge">${this.featureHash}</span> <button class="btn-copy" style="vertical-align:text-bottom" title="Copy Feature ID: ${fullId}" onclick="copyToClipboard('${fullId}', this)"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg></button>`;
                }
                if (metaInfoEl) {
                    metaInfoEl.innerHTML = `<span class="dim">Collection:</span> ${this.collection} | <span class="dim">Total Instances:</span> ${this.totalOccurrences}`;
                }
            }

            for (const occ of occurrences) {
                let funcId = normalizeFuncId(occ['function_id']);
                const sourceData = await this.fetchSource(funcId);
                const tr = document.createElement('tr');

                const parts = funcId.split(':');
                const addr = parts.pop();
                const md5 = parts[2];
                const funcName = (sourceData && sourceData.meta) ? sourceData.meta['function_name'] : addr;

                const originHtml = `
                    <div style="font-weight:bold; color:var(--accent); font-size:1rem; margin-bottom:4px;">${funcName}</div>
                    <div class="dim" style="font-size:0.7rem;">Address: ${addr}</div>
                    <div class="dim" style="font-size:0.7rem;">Binary: ${md5}</div>
                    <div class="dim" style="font-size:0.7rem; margin-top:2px;">Lines: <span style="color:var(--accent);">${(occ['line_idx'] || []).map(l => l + 1).join(', ')}</span></div>
                    <div style="display:flex; align-items:center; gap:5px; margin-top:8px;">
                        <code class="origin-id" style="font-size:0.6rem;">${funcId}</code>
                        <button class="btn-copy" title="Copy Function ID: ${funcId}" onclick="copyToClipboard('${funcId}', this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                    <div style="display:flex; gap:5px; margin-top:5px;">
                        <button class="btn-code-action" onclick="showFunctionCodeById('${funcId}', '${funcName.replace(/'/g, "\\'")}', '', event)">
                            <span>↗</span> Code
                        </button>
                    </div>
                `;

                // Add to Diff / See Similar available via right-click > Actions
                const originEntityData = JSON.stringify({
                    function_id: funcId,
                    function_name: funcName,
                    file_md5: md5,
                    entrypoint_address: addr,
                    collection: this.collection
                }).replace(/'/g, "&apos;");

                const metaHtml = `
                    <div style="font-size:0.75rem; margin-bottom:5px; color:var(--accent); font-weight:bold;">${occ.type}</div>
                    <div style="display:flex; flex-direction:column; gap:2px;">
                        ${occ['previous_pcode_op'] ? `<span class="prev-badge" title="Previous Op">PREV: ${occ['previous_pcode_op']}</span>` : ''}
                        <span class="op-badge" title="${occ['pcode_op_full'] || ''}">${occ['pcode_op']}</span>
                        ${occ['addr'] ? `<span class="addr-badge">${occ['addr']}</span>` : ''}
                    </div>
                `;

                const targetLinesStr = (occ['line_idx'] || []).map(l => l + 1).join(',');
                const lineHash = targetLinesStr ? `#L${targetLinesStr}` : '';

                const pcodeHtml = `
                    <div class="code-card" style="box-shadow: none;">
                        <div class="code-card-line">
                            <div class="code-card-text pcode-text">${occ['pcode_op_full'] || '<span class="dim">N/A</span>'}</div>
                        </div>
                    </div>
                `;

                const pb = occ['pcode_block'] || {};
                const pcodeBlockHtml = (Object.keys(pb).length > 0) ?
                    `<div class="pcode-block-container">
                        ${Object.entries(pb).map(([s, op]) => {
                        const isActive = (s === occ['seq']);
                        const isPrevActive = (s === occ['previous_seq']);
                        const cls = isActive ? 'active' : (isPrevActive ? 'prev-active' : '');
                        const addrPart = s.split(':')[0];
                        return `<div class="pcode-block-line ${cls}"><span class="pcode-block-seq">${addrPart}</span>${op.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</div>`;
                    }).join('')}
                    </div>` :
                    '<div class="dim">---</div>';

                const tfHtml = `
                    <div class="mono" style="font-size: 0.75rem; color: var(--accent); text-align: center;">${occ.tf || '---'}</div>
                `;

                let contextHtml = '<div class="dim" style="padding:10px;">No context</div>';
                if (sourceData && sourceData.rows) {
                    contextHtml = `<div class="code-card clickable" title="Click to jump to lines ${targetLinesStr || ''}"
                         onclick="showFunctionCodeById('${funcId}', '${funcName.replace(/'/g, "\\'")}', '${lineHash}', event)">`;

                    const contextLineSet = new Set();
                    (occ['line_idx'] || []).forEach(l => {
                        const normalizedIdx = l + 1;
                        for (let i = -this.CONTEXT_WINDOW; i <= this.CONTEXT_WINDOW; i++) {
                            contextLineSet.add(normalizedIdx + i);
                        }
                    });

                    const sortedLines = Array.from(contextLineSet).filter(l => l >= 1 && l <= sourceData.rows.length).sort((a, b) => a - b);
                    let lastLineIdx = -1;

                    sortedLines.forEach(lineIdx => {
                        const lineObj = sourceData.rows.find(l => l.line_idx === lineIdx);
                        if (lineObj) {
                            if (lastLineIdx !== -1 && lineIdx > lastLineIdx + 1) {
                                contextHtml += `<div class="code-card-line" style="opacity: 0.2; justify-content: center; padding: 4px 0;"><span style="font-size: 0.6rem; letter-spacing: 3px;">...</span></div>`;
                            }
                            lastLineIdx = lineIdx;
                            const isFeatureLine = (occ['line_idx'] || []).includes(lineIdx - 1);
                            const rowStyle = isFeatureLine ? '' : 'opacity: 0.5;';
                            contextHtml += `
                                 <div class="code-card-line" style="${rowStyle}">
                                     <span class="code-card-ln">${lineObj.line_idx}</span>
                                     <span class="code-card-text">${this.renderTokens(lineObj.tokens, funcId)}</span>
                                 </div>`;
                        }
                    });
                    contextHtml += '</div>';
                }

                tr.innerHTML = `
                    <td class="code-cell" data-entity-data='${originEntityData}' oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">${originHtml}</td>
                    <td class="code-cell">${metaHtml}</td>
                    <td class="code-cell"><span class="mono" style="font-size:0.75rem; border:1px solid rgba(255,255,255,0.05); padding:1px 4px; border-radius:3px; background:rgba(255,255,255,0.02);">${occ['seq'] || 'N/A'}</span></td>
                    <td class="code-cell">${pcodeHtml}</td>
                    <td class="code-cell">${pcodeBlockHtml}</td>
                    <td class="code-cell">${tfHtml}</td>
                    <td class="code-cell" style="padding:0;">${contextHtml}</td>
                `;
                this.featuresBody.appendChild(tr);
            }

            this.currentOffset += occurrences.length;

            if (this.totalOccurrences === 0 && this.currentOffset === 0) {
                this.featuresBody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:50px; color:var(--dim);">No instances found in this collection.</td></tr>';
                this.loadMoreContainer.style.display = 'none';
            } else if (this.currentOffset < this.totalOccurrences) {
                this.loadMoreContainer.style.display = 'block';
                this.loadMoreBtn.disabled = false;
                this.loadMoreBtn.innerHTML = `Load More Occurrences (<span id="feat-count-remaining">${this.totalOccurrences - this.currentOffset}</span> remaining)`;
            } else {
                this.loadMoreContainer.style.display = 'none';
            }
            if (typeof updateDiffQueueUI === 'function') {
                updateDiffQueueUI();
            }

        } catch (err) {
            console.error(err);
            if (this.currentOffset === 0) {
                this.featuresBody.innerHTML = `<tr><td colspan="7" style="text-align:center; padding:50px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> Error: ${err.message}</td></tr>`;
            }
        }
    },

    renderTokens(tokens, funcId) {
        return tokens.map(t => {
            const isPrimary = t.hash_list && t.hash_list.includes(this.featureHash);
            const hasOthers = t.hash_list && t.hash_list.some(h => h !== this.featureHash);
            const highlightClass = isPrimary ? 'feature-primary' : (hasOthers ? 'feature-secondary' : '');

            if (window.renderTokenHtml) {
                return window.renderTokenHtml(t)
                    .replace('class="token ', `class="token ${highlightClass} `)
                    .replace('<span ', `<span data-func-id="${funcId}" onmouseenter="FeatureView.handleHoverMove(event, true); FeatureView.setGlobalHighlight(true)" onmouseleave="FeatureView.handleHoverMove(event, false); FeatureView.setGlobalHighlight(false)" `);
            }
            return '';
        }).join('');
    },

    handleHoverMove(e, show) {
        if (!show) {
            this.tooltipEl.style.display = 'none';
            return;
        }
        const token = e.target.closest('.feature-primary, .feature-secondary');
        if (!token) return;

        const idx = token.dataset.idx;
        const funcId = token.dataset.funcId;
        const cached = this.sourceCache.get(funcId);
        if (!cached || !cached.tips || !cached.tips[idx]) return;

        const data = cached.tips[idx];
        let html = `<div style="font-weight:bold; color:var(--accent); border-bottom:1px solid var(--border); padding-bottom:5px; margin-bottom:5px;">Features (${data[1]})</div>`;

        data[2].forEach(f => {
            const color = f[8] || 'var(--accent)';
            html += `<div style="margin-bottom:8px;">
                <div style="font-family:monospace; color:${color}; font-weight:bold;">${f[0]}</div>
                <div style="font-size:0.7rem; color:var(--subtle);">${f[3]} | Op: ${f[1]} | <b style="color:var(--success)">TF: ${f[7] || 0}</b></div>
            </div>`;
        });

        this.tooltipEl.innerHTML = html;
        this.tooltipEl.style.display = 'block';
        this.tooltipEl.style.left = (e.clientX + 15) + "px";
        this.tooltipEl.style.top = (e.clientY + 15) + "px";

        const rect = this.tooltipEl.getBoundingClientRect();
        if (rect.right > window.innerWidth) this.tooltipEl.style.left = (e.clientX - rect.width - 15) + "px";
        if (rect.bottom > window.innerHeight) this.tooltipEl.style.top = (e.clientY - rect.height - 15) + "px";
    },

    handleMouseMove(e) {
        if (this.tooltipEl && this.tooltipEl.style.display === 'block') {
            const x = e.clientX + 15;
            const y = e.clientY + 15;
            this.tooltipEl.style.left = x + "px";
            this.tooltipEl.style.top = y + "px";

            const rect = this.tooltipEl.getBoundingClientRect();
            if (rect.right > window.innerWidth) this.tooltipEl.style.left = (e.clientX - rect.width - 15) + "px";
            if (rect.bottom > window.innerHeight) this.tooltipEl.style.top = (e.clientY - rect.height - 15) + "px";
        }
    },

    handleContextMenu(e) {
        const token = e.target.closest('.feature-primary, .feature-secondary');
        if (!token) return;
        e.preventDefault();

        const hashesStr = token.dataset.hashes;
        if (!hashesStr) return;
        const hashes = hashesStr.trim().split(/\s+/);
        const idx = token.dataset.idx;
        const funcId = token.dataset.funcId;

        const cached = this.sourceCache.get(funcId);
        const richData = (cached && cached.tips) ? cached.tips[idx] : null;

        let menu = document.getElementById('token-context-menu');
        if (!menu) {
            menu = document.createElement('div');
            menu.id = 'token-context-menu';
            menu.className = 'context-menu';
            document.body.appendChild(menu);
        }

        let html = `<div class="context-menu-header">Select Feature to Analyze</div>`;
        if (richData && richData[2]) {
            richData[2].forEach(f => {
                const hash = f[0], op = f[1], type = f[3], tf = f[7] || 0, color = f[8] || 'var(--accent)';
                html += `<div class="context-menu-item" data-hash="${hash}" data-col="${this.collection}">
                    <div class="context-menu-icon" style="color:${color}">🔍</div>
                    <div style="flex:1">
                        <div style="font-family:monospace; font-weight:bold; color:${color}">${hash}</div>
                        <div style="font-size:0.7rem; color:var(--subtle); margin-top:2px;">${type} | Op: ${op} | <b style="color:var(--success)">TF: ${tf}</b></div>
                    </div>
                </div>`;
            });
        } else {
            hashes.forEach(h => {
                html += `<div class="context-menu-item" data-hash="${h}" data-col="${this.collection}">
                    <div class="context-menu-icon" style="color:var(--accent)">🔍</div>
                    <div style="flex:1">
                        <div style="font-family:monospace; font-weight:bold; color:var(--accent)">${h}</div>
                        <div style="font-size:0.75rem; color:var(--subtle); margin-top:2px;">Analyze this feature</div>
                    </div>
                </div>`;
            });
        }

        menu.innerHTML = html;
        menu.style.display = 'block';
        let x = e.clientX, y = e.clientY;
        if (x + 350 > window.innerWidth) x -= 350;
        const itemCount = (richData && richData[2]) ? richData[2].length : hashes.length;
        if (y + (itemCount * 52 + 50) > window.innerHeight) y -= (itemCount * 52 + 50);
        menu.style.left = x + 'px';
        menu.style.top = y + 'px';

        const onMenuClick = (me) => {
            const item = me.target.closest('.context-menu-item');
            if (item) {
                const h = item.dataset.hash;
                const c = item.dataset.col;
                const url = `/collections/${encodeURIComponent(c)}/features/${encodeURIComponent(h)}`;
                Nav.openPath(url, me);
            }
            closeMenu();
        };
        const closeMenu = () => {
            menu.style.display = 'none';
            menu.removeEventListener('click', onMenuClick);
            document.removeEventListener('mousedown', closeGlobal);
        };
        const closeGlobal = (me) => { if (!menu.contains(me.target)) closeMenu(); };
        setTimeout(() => {
            menu.addEventListener('click', onMenuClick);
            document.addEventListener('mousedown', closeGlobal);
        }, 10);
    },

    setGlobalHighlight(state) {
        this.container.querySelectorAll('.feature-primary, .feature-secondary').forEach(el => {
            el.classList.toggle('bsim-group-active-unique', state);
        });
    },

    destroy() {
        this.container = null;
        this.params = null;
        this.sourceCache.clear();
    }
};
