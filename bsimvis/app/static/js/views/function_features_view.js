/**
 * Function Features View Module
 * Replaces static function/features/index.html and integrates into SPA dashboard
 */

window.FunctionFeaturesView = {
    container: null,
    params: null,
    id: null,
    tooltipEl: null,
    featuresBody: null,
    globalTips: {},
    featureName: '',

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
        this.globalTips = {};
        this.featureName = address;

        // Build HTML layout with style block
        this.container.innerHTML = `
            <style>
                .feat-table-container {
                    background: var(--card-bg);
                    border: 1px solid var(--border);
                    border-radius: 4px;
                    overflow: auto;
                    flex: 1;
                    margin-top: 15px;
                }

                .feat-table-container table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.8rem;
                }

                .feat-table-container th {
                    background: #000;
                    color: var(--accent);
                    text-transform: uppercase;
                    font-size: 0.65rem;
                    letter-spacing: 0.5px;
                    padding: 8px 12px;
                    text-align: left;
                    border-bottom: 2px solid var(--border);
                    position: sticky;
                    top: 0;
                    z-index: 2;
                }

                .feat-table-container td {
                    padding: 8px 12px;
                    border-bottom: 1px solid var(--border);
                    vertical-align: top;
                }

                .feat-table-container tr:hover {
                    background: var(--hover);
                }

                .hash-badge {
                    font-family: 'Consolas', monospace;
                    color: #ae81ff;
                    background: rgba(174, 129, 255, 0.1);
                    padding: 1px 4px;
                    border-radius: 3px;
                    font-size: 0.75rem;
                }

                .hash-badge:hover {
                    background: rgba(174, 129, 255, 0.3);
                    border: 1px solid #ae81ff;
                    color: #fff;
                }

                .type-badge {
                    color: var(--accent);
                    font-weight: bold;
                    font-size: 0.75rem;
                }

                .pcode-badge {
                    background: #f92672;
                    color: #000;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-weight: bold;
                    font-size: 0.7rem;
                    font-family: monospace;
                    display: inline-block;
                }

                .pcode-cell {
                    font-family: 'Consolas', monospace;
                    color: #e6db74;
                }

                .ff-tooltip {
                    display: none;
                    position: fixed;
                    z-index: 20000;
                    background: rgba(0,0,0,0.95);
                    padding: 10px;
                    border-radius: 4px;
                    border: 1px solid var(--accent);
                    color: #fff;
                    font-size: 0.8rem;
                    pointer-events: none;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.5);
                    max-width: 320px;
                }
            </style>

            <div style="display:flex; flex-direction:column; flex:1; overflow:hidden; height:100%; padding: 0 0 20px 0;">
                <div class="header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:15px; flex-shrink:0;">
                    <div>
                        <h1 style="color:var(--accent); margin:0; font-size:1.2rem;">Function Features Browser</h1>
                        <div style="margin-top:8px; font-size:0.85rem;">
                            <span class="dim">Function:</span> <span id="ff-func-id" style="font-family:monospace; color:#ccc;">---</span> 
                            | <span id="ff-feature-count" class="badge" style="background:rgba(102,217,239,0.1); color:var(--accent); margin-left:10px;">0 features</span>
                        </div>
                    </div>
                </div>

                <div class="feat-table-container">
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 220px;">Feature Hash</th>
                                <th style="width: 70px;">Seq</th>
                                <th style="width: 90px;">Type</th>
                                <th style="width: 100px;">Op</th>
                                <th style="width: 220px;">PCode Statement</th>
                                <th style="width: 180px;">Block Context</th>
                                <th style="width: 40px;">TF</th>
                                <th>Source Code Context</th>
                            </tr>
                        </thead>
                        <tbody id="ff-features-body">
                            <tr>
                                <td colspan="8" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                                    <i class="fa-solid fa-spinner fa-spin"></i> Loading Function Features...
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div id="ff-tooltip" class="ff-tooltip"></div>
        `;

        // Bind DOM elements
        this.tooltipEl = this.container.querySelector('#ff-tooltip');
        this.featuresBody = this.container.querySelector('#ff-features-body');
        
        const funcIdEl = this.container.querySelector('#ff-func-id');
        if (funcIdEl) funcIdEl.innerText = this.id;

        this.container.addEventListener('mousemove', e => this.handleMouseMove(e));
        this.container.addEventListener('contextmenu', e => this.handleContextMenu(e));
        this.featuresBody.addEventListener('pointerover', e => this.handleHoverMove(e, true));
        this.featuresBody.addEventListener('pointerout', e => this.handleHoverMove(e, false));

        // Load data
        await this.loadFeatures();
    },

    async loadFeatures() {
        try {
            const res = await fetch(`/api/function/features?id=${encodeURIComponent(this.id)}`);
            if (!res.ok) throw new Error("Failed to load features");
            const data = await res.json();
            
            this.globalTips = data.tips || {};
            this.featureName = this.id.split(':').pop();

            this.renderFeatures(data.features || []);

            const countEl = this.container.querySelector('#ff-feature-count');
            if (countEl) countEl.innerText = `${data.features ? data.features.length : 0} features`;
        } catch (err) {
            console.error(err);
            this.featuresBody.innerHTML = `<tr><td colspan="8" style="text-align:center; padding:50px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> Error: ${err.message}</td></tr>`;
        }
    },

    renderFeatures(features) {
        this.featuresBody.innerHTML = '';
        window.featureMeta = {};

        if (features.length === 0) {
            this.featuresBody.innerHTML = '<tr><td colspan="8" style="text-align:center; padding:50px; color:var(--dim);">No features found.</td></tr>';
            return;
        }

        const currentCollection = this.params.collection || this.id.split(':')[0];

        features.forEach(f => {
            window.featureMeta[f.hash] = {
                type: f.type || 'N/A',
                op: f['pcode_op'] || 'N/A',
                tf: f.tf || 0
            };
            const tr = document.createElement('tr');

            // Code Context
            let codeHtml = '<div class="dim">---</div>';
            if (f.context_lines && f.context_lines.length > 0) {
                codeHtml = '';
                f.context_lines.forEach(row => {
                    let rowHtml = `<div class="code-line" style="height:auto; line-height:1.3; font-size:0.75rem;">
                        <div class="gutter" style="min-width:30px; background:transparent; border:none; opacity:0.3;"><div class="line-num" style="width:20px; font-size:0.65rem; text-align:right;">${row.line_idx}</div></div>
                        <div class="line-content" style="padding-left:8px;">`;

                    row.tokens.forEach(t => {
                        if (window.renderTokenHtml) {
                            rowHtml += window.renderTokenHtml(t);
                        }
                    });

                    rowHtml += `</div></div>`;
                    codeHtml += rowHtml;
                });
            }

            const targetUrl = `/collection/${encodeURIComponent(currentCollection)}/feature/${encodeURIComponent(f.hash)}`;
            const clickHandler = `Nav.openPath('${targetUrl}', event, { title: 'Feature Analysis', type: 'global-feature' });`;

            tr.innerHTML = `
                <td>
                    <div style="display:inline-flex; align-items:center; gap:5px;">
                        <span class="hash-badge" style="cursor: pointer;" onclick="${clickHandler}">${f.hash}</span>
                        <button class="btn-copy" title="Copy Feature ID: ${f['feature_id']}" onclick="copyToClipboard('${f['feature_id']}', this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </td>
                <td><span class="mono" style="font-size:0.75rem; border:1px solid rgba(255,255,255,0.05); padding:1px 4px; border-radius:3px; background:rgba(255,255,255,0.02);">${f['seq'] || 'N/A'}</span></td>
                <td><span class="type-badge">${f.type || 'N/A'}</span></td>
                <td>
                    <div style="display:inline-flex; flex-direction:column; gap:3px;">
                        ${f['previous_pcode_op'] ? `<span class="prev-badge" title="Previous Op">PREV: ${f['previous_pcode_op']}</span>` : ''}
                        <span class="pcode-badge">${f['pcode_op'] || 'N/A'}</span>
                        ${f['addr'] ? `<span class="addr-badge">${f['addr']}</span>` : ''}
                    </div>
                </td>
                <td class="pcode-cell">${f['pcode_op_full'] || 'N/A'}</td>
                <td>
                    ${(f['pcode_block'] && Object.keys(f['pcode_block']).length > 0) ?
                    `<div class="pcode-block-container">
                            ${Object.entries(f['pcode_block']).map(([s, op]) => {
                        const isActive = (s === f['seq']);
                        const isPrevActive = (s === f['previous_seq']);
                        const cls = isActive ? 'active' : (isPrevActive ? 'prev-active' : '');
                        const addrPart = s.split(':')[0];
                        return `<div class="pcode-block-line ${cls}"><span class="pcode-block-seq">${addrPart}</span>${op.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</div>`;
                    }).join('')}
                        </div>` :
                    '<span class="dim">---</span>'
                }
                </td>
                <td class="dim" style="font-size:0.75rem;">${f.tf || 'N/A'}</td>
                <td class="code-cell">${codeHtml}</td>
            `;
            this.featuresBody.appendChild(tr);
        });

        if (typeof updateDiffQueueUI === 'function') {
            updateDiffQueueUI();
        }
    },

    handleHoverMove(e, show) {
        if (!show) {
            this.tooltipEl.style.display = 'none';
            return;
        }
        const token = e.target.closest('.feature-highlight');
        if (!token) return;

        const idx = token.dataset.idx;
        const data = this.globalTips[idx];
        if (!data) return;

        let html = `<div style="font-weight:bold; color:var(--accent); border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:5px;">Features (${data[1]})</div>`;

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
        const token = e.target.closest('.feature-highlight');
        if (!token) return;
        e.preventDefault();

        const idx = token.dataset.idx;
        const data = this.globalTips[idx];
        if (!data || !data[2]) return;

        const collection = this.id.split(':')[0] || 'main';

        let menu = document.getElementById('token-context-menu');
        if (!menu) {
            menu = document.createElement('div');
            menu.id = 'token-context-menu';
            menu.className = 'context-menu';
            document.body.appendChild(menu);
        }

        let html = `<div class="context-menu-header">Select Feature to Analyze</div>`;
        data[2].forEach(f => {
            const hash = f[0], op = f[1], type = f[3], tf = f[7] || 0, color = f[8] || 'var(--accent)';
            html += `<div class="context-menu-item" data-hash="${hash}" data-col="${collection}">
                <div class="context-menu-icon" style="color:${color}">🔍</div>
                <div style="flex:1">
                    <div style="font-family:monospace; font-weight:bold; color:${color}">${hash}</div>
                    <div style="font-size:0.75rem; color:var(--subtle); margin-top:2px;">${type} | Op: ${op} | <b style="color:var(--success)">TF: ${tf}</b></div>
                </div>
            </div>`;
        });

        menu.innerHTML = html;
        menu.style.display = 'block';
        let x = e.clientX, y = e.clientY;
        if (x + 350 > window.innerWidth) x -= 350;
        if (y + (data[2].length * 52 + 50) > window.innerHeight) y -= (data[2].length * 52 + 50);
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
        const closeGlobal = (me) => { if (!menu.contains(me.target)) closeMenu(); };
        setTimeout(() => {
            menu.addEventListener('click', onMenuClick);
            document.addEventListener('mousedown', closeGlobal);
        }, 10);
    },

    destroy() {
        this.container = null;
        this.params = null;
        this.globalTips = {};
    }
};
