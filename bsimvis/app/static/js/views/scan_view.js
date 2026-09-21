/**
 * Scan View
 * Form mode:   /scans
 * Report mode: /scans/{id}
 *
 * Fast scan mode compares a file against existing collections without
 * ingesting it, so there is nothing to list: the form posts the bytes to
 * /api/scan and the report reads the cached result back. Matched-function
 * rows are paged server-side through /api/scan/{id}/diff, same contract the
 * binary similarity diff uses -- never a full-table load.
 *
 * Everything rendered here came off an uploaded sample, so every value goes
 * through escapeHtml / escapeAttr on the way out.
 */

const SCAN_AXES = ['overall', 'code', 'library', 'content'];
const SCAN_ROWS_PAGE = 50;

function scanScoreCell(value) {
    if (value === null || value === undefined) return '<span style="color:var(--dim);">—</span>';
    const score = Number(value);
    const color = score >= 0.9 ? '#10b981' : score >= 0.5 ? '#f59e0b' : 'var(--dim)';
    return `<span style="color:${color}; font-weight:700;">${(score * 100).toFixed(1)}%</span>`;
}

window.ScanView = {
    _pollTimer: null,
    _stopped: false,
    _scanId: null,
    _doc: null,
    _openPair: null, // "collection\u0000md5" of the expanded diff, if any
    _containerId: null,
    _sortCol: 'sim',
    _sortDir: -1,

    setSort(col) {
        if (this._sortCol === col) {
            this._sortDir *= -1;
        } else {
            this._sortCol = col;
            this._sortDir = -1;
        }
        if (this._doc && this._containerId) {
            const container = document.getElementById(this._containerId);
            if (container) container.innerHTML = this._renderReport(this._doc);
        }
    },

    destroy() {
        this._stopped = true;
        if (this._pollTimer) {
            clearTimeout(this._pollTimer);
            this._pollTimer = null;
        }
        this._doc = null;
        this._openPair = null;
    },

    async init(params, containerId) {
        this._containerId = containerId;
        this._stopped = false;
        const container = document.getElementById(containerId);
        if (!container) return;

        window.ScanViewInstance = this;
        this._scanId = params.scan_id || null;
        if (this._scanId) {
            await this._initReport(container, this._scanId);
        } else {
            await this._initForm(container, params.collection);
        }
    },

    // --- form mode --------------------------------------------------------

    async _initForm(container, preselect) {
        let defaults = { modules: [], top_files: 20, max_cached_bytes: 0 };
        try {
            defaults = await (await fetch('/api/scan/defaults')).json();
        } catch (e) {
            /* the form still works on the server's own defaults */
        }
        container.innerHTML = this._renderForm(defaults);
        this._loadCollections(preselect);
        this._loadRecentScans();
    },

    async _loadRecentScans() {
        const listEl = document.getElementById('scan-list-container');
        if (!listEl) return;
        try {
            const res = await fetch('/api/scan');
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'Failed to load scans');
            
            if (!data.scans || !data.scans.length) {
                listEl.innerHTML = '<div style="color:var(--dim); font-size:0.85rem;">No recent scans found.</div>';
                return;
            }
            
            listEl.innerHTML = data.scans.map(s => {
                const date = new Date((s.created_at || 0) * 1000).toLocaleString();
                const statusColor = s.job_status === 'finished' ? '#10b981' : (s.job_status === 'failed' ? '#f87171' : 'var(--accent)');
                const progress = s.progress || '';
                return `
                <a href="/scans/${escapeAttr(s.scan_id)}" onclick="Nav.openPath(this.href, event)" style="text-decoration:none; display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:var(--bg); border:1px solid var(--border); border-radius:6px; transition:border-color 0.15s;">
                    <div style="display:flex; flex-direction:column; gap:4px;">
                        <span style="color:var(--fg); font-weight:600; font-size:0.85rem;">${escapeHtml(s.file_name || 'unknown')}</span>
                        <span style="color:var(--dim); font-size:0.75rem;">${escapeHtml(date)} &bull; <code>${escapeHtml(s.file_md5 || '').slice(0,8)}</code></span>
                    </div>
                    <div style="text-align:right;">
                        <span style="color:${statusColor}; font-size:0.8rem; font-weight:600;">${escapeHtml(s.job_status || s.status || 'queued')}</span>
                        ${progress ? `<div style="color:var(--dim); font-size:0.75rem;">${escapeHtml(progress)}</div>` : ''}
                    </div>
                </a>`;
            }).join('');
        } catch (e) {
            listEl.innerHTML = `<div style="color:#f87171; font-size:0.85rem;">${escapeHtml(e.message)}</div>`;
        }
    },

    _renderForm(defaults) {
        const modules = ['FunctionID', 'boilerplate', 'capa', 'yara', 'rulezet'];
        const lean = new Set(defaults.modules || []);
        const boxes = modules.map(m => `
            <label style="display:inline-flex; align-items:center; gap:6px; font-size:0.8rem; color:var(--dim); cursor:pointer;">
                <input type="checkbox" class="scan-module" value="${escapeAttr(m)}" ${lean.has(m) ? 'checked' : ''}>
                ${escapeHtml(m)}
            </label>`).join('');

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:20px;">
            <div>
                <h1 style="margin:0 0 6px 0; font-size:1.5rem; color:var(--text); display:flex; align-items:center; gap:10px;">
                    <i class="fa-solid fa-microscope" style="color:var(--accent);"></i> Scan
                </h1>
                <div style="color:var(--dim); font-size:0.85rem;">
                    Compare a binary against existing collections without ingesting it.
                    Nothing is written until you commit the result.
                </div>
            </div>

            <div style="display:flex; gap: 30px; flex-wrap: wrap; align-items: flex-start;">
                <div style="flex: 1; min-width: 400px; border:1px solid var(--border); border-radius:8px; background:var(--card-bg); padding:20px; display:flex; flex-direction:column; gap:16px; max-width:760px;">
                    <div>
                        <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;">File</label>
                        <input type="file" id="scan-form-file" style="width:100%; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.85rem;">
                    </div>

                    <div>
                        <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;">
                            Scope <span style="opacity:0.7;">(ctrl-click for several, or scan everything)</span>
                        </label>
                        <select id="scan-form-collections" multiple size="6" style="width:100%; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.85rem;"></select>
                        <label style="display:inline-flex; align-items:center; gap:6px; font-size:0.8rem; color:var(--dim); margin-top:8px; cursor:pointer;">
                            <input type="checkbox" id="scan-form-all"> Scan against every collection
                        </label>
                    </div>

                    <div>
                        <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;">
                            Analysis modules <span style="opacity:0.7;">(a scan runs lean by default; capa alone can cost more than the rest of the job)</span>
                        </label>
                        <div style="display:flex; gap:16px; flex-wrap:wrap;">${boxes}</div>
                    </div>

                    <div style="display:flex; gap:14px; flex-wrap:wrap; align-items:flex-end;">
                        <div>
                            <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;">Top files scored</label>
                            <input type="number" id="scan-form-top-files" value="${Number(defaults.top_files) || 20}" min="1" max="200" style="width:110px; padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.82rem;">
                        </div>
                        <div>
                            <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;" title="Leave blank to use each collection's own locked value">Min score</label>
                            <input type="number" id="scan-form-min-score" step="0.01" min="0" max="1" placeholder="collection" style="width:110px; padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.82rem;">
                        </div>
                        <div>
                            <label style="display:block; font-size:0.78rem; color:var(--dim); margin-bottom:6px;" title="Leave blank to use each collection's own locked value">Min features</label>
                            <input type="number" id="scan-form-min-features" min="0" placeholder="collection" style="width:110px; padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.82rem;">
                        </div>
                        <button id="scan-form-submit" onclick="window.ScanViewInstance.submit()" style="background:var(--accent); border:none; color:var(--bg); padding:9px 22px; border-radius:6px; font-size:0.85rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px;">
                            <i class="fa-solid fa-microscope"></i> Scan
                        </button>
                    </div>
                </div>

                <div style="flex: 1; min-width: 400px; border:1px solid var(--border); border-radius:8px; background:var(--card-bg); padding:20px; display:flex; flex-direction:column; gap:16px;">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <h2 style="margin:0; font-size:1.1rem; color:var(--text);">Recent Scans</h2>
                        <button onclick="window.ScanViewInstance._loadRecentScans()" style="background:none; border:none; color:var(--accent); cursor:pointer; font-size:0.9rem;" title="Refresh"><i class="fa-solid fa-rotate-right"></i></button>
                    </div>
                    <div id="scan-list-container" style="display:flex; flex-direction:column; gap:8px; max-height:500px; overflow-y:auto; padding-right:5px;">
                        <div style="color:var(--dim); font-size:0.85rem;"><i class="fa-solid fa-spinner fa-spin"></i> Loading...</div>
                    </div>
                </div>
            </div>

            <div id="scan-form-error" style="color:#f87171; font-size:0.82rem;"></div>
        </div>`;
    },

    async _loadCollections(preselect) {
        const sel = document.getElementById('scan-form-collections');
        if (!sel) return;
        try {
            const res = await (await fetch('/api/collection/search?limit=1000')).json();
            const collections = res.collections || (Array.isArray(res) ? res : []);
            sel.innerHTML = collections
                .map(c => `<option value="${escapeAttr(c.name)}"${c.name === preselect ? ' selected' : ''}>${escapeHtml(c.name)}</option>`)
                .join('');
        } catch (e) {
            sel.innerHTML = '<option value="">-- Failed to load collections --</option>';
        }
    },

    async submit() {
        const errEl = document.getElementById('scan-form-error');
        const button = document.getElementById('scan-form-submit');
        const input = document.getElementById('scan-form-file');
        const file = input && input.files && input.files[0];
        if (errEl) errEl.textContent = '';
        if (!file) {
            if (errEl) errEl.textContent = 'Pick a file to scan.';
            return;
        }

        const qs = new URLSearchParams();
        qs.set('file_name', file.name);
        const all = document.getElementById('scan-form-all');
        if (all && all.checked) {
            qs.set('all', 'true');
        } else {
            const sel = document.getElementById('scan-form-collections');
            const chosen = Array.from((sel && sel.selectedOptions) || []).map(o => o.value);
            if (!chosen.length) {
                if (errEl) errEl.textContent = 'Pick at least one collection, or tick "every collection".';
                return;
            }
            chosen.forEach(c => qs.append('collection', c));
        }
        // The server starts from `scan.modules`, so send the full delta both
        // ways: a box the config pre-ticked and the analyst unticked has to
        // arrive as `disable`, not as a missing `enable`.
        document.querySelectorAll('.scan-module').forEach(box => {
            qs.append(box.checked ? 'enable' : 'disable', box.value);
        });
        [['scan-form-top-files', 'top_files'], ['scan-form-min-score', 'min_score'], ['scan-form-min-features', 'min_features']]
            .forEach(([id, param]) => {
                const el = document.getElementById(id);
                if (el && el.value !== '') qs.set(param, el.value);
            });

        if (button) {
            button.disabled = true;
            button.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Uploading...';
        }
        try {
            const res = await fetch(`/api/scan?${qs.toString()}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/octet-stream' },
                body: file
            });
            const body = await res.json();
            if (!res.ok || body.error) throw new Error(body.error || `HTTP ${res.status}`);
            Nav.openPath(`/scans/${encodeURIComponent(body.scan_id)}`);
        } catch (e) {
            if (errEl) errEl.textContent = e.message;
            if (button) {
                button.disabled = false;
                button.innerHTML = '<i class="fa-solid fa-microscope"></i> Scan';
            }
        }
    },

    // --- report mode ------------------------------------------------------

    async _initReport(container, scanId) {
        container.innerHTML = `<div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Scan...</div>`;
        await this._refresh(container, scanId);
    },

    async _refresh(container, scanId) {
        if (this._stopped) return;
        let doc;
        try {
            const res = await fetch(`/api/scan/${encodeURIComponent(scanId)}`);
            doc = await res.json();
            if (!res.ok || doc.error) throw new Error(doc.error || `HTTP ${res.status}`);
        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${escapeHtml(e.message)}</div>`;
            return;
        }

        this._doc = doc;
        container.innerHTML = this._renderReport(doc);
        if (doc.status !== 'completed' && doc.status !== 'failed' && doc.job_status !== 'failed') {
            this._pollTimer = setTimeout(() => this._refresh(container, scanId), 2500);
        }
    },

    _renderReport(doc) {
        const running = doc.status !== 'completed' && doc.status !== 'failed' && doc.job_status !== 'failed';
        const warnings = (doc.warnings || []).map(w =>
            `<div style="color:#f59e0b; font-size:0.8rem;"><i class="fa-solid fa-triangle-exclamation"></i> ${escapeHtml(w)}</div>`).join('');
        const present = (doc.already_present || []).length
            ? `<div style="color:#60a5fa; font-size:0.8rem;"><i class="fa-solid fa-circle-info"></i> This md5 is already in: ${escapeHtml((doc.already_present || []).join(', '))}</div>`
            : '';
        const committed = doc.committed
            ? `<div style="color:#10b981; font-size:0.8rem;"><i class="fa-solid fa-check"></i> Committed to ${escapeHtml(doc.committed.collection)} as batch ${escapeHtml(doc.committed.batch_uuid)}${(doc.committed.topup_modules || []).length ? `, tag top-up queued for ${escapeHtml((doc.committed.topup_modules || []).join(', '))}` : ''}</div>`
            : '';

        const body = running
            ? `<div style="padding:30px; color:var(--dim); display:flex; align-items:center; gap:12px;">
                   <i class="fa-solid fa-spinner fa-spin"></i>
                   ${escapeHtml(doc.status || 'queued')} ${Number(doc.progress || 0)}%
               </div>`
            : doc.status === 'failed' || doc.job_status === 'failed'
                ? `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${escapeHtml(doc.error || 'Scan failed')}</div>`
                : (doc.scanned || []).map(scope => this._renderScope(doc, scope)).join('')
                  || '<div style="padding:30px; color:var(--dim);">No collection matched.</div>';

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:20px;">
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <h1 style="margin:0 0 6px 0; font-size:1.3rem; color:var(--text);">
                        <i class="fa-solid fa-microscope" style="color:var(--accent); margin-right:8px;"></i>${escapeHtml(doc.file_name || 'scan')}
                    </h1>
                    <div style="color:var(--dim); font-size:0.82rem; display:flex; align-items:center; gap:14px; flex-wrap:wrap;">
                        <code>${escapeHtml(doc.file_md5 || '')}</code>
                        <span>${Number(doc.size || 0).toLocaleString()} bytes</span>
                        <span>${Number(doc.function_count || 0)} functions</span>
                        <span>scopes: ${escapeHtml((doc.scopes || []).join(', '))}</span>
                    </div>
                </div>
                <div style="display:flex; gap:10px;">
                    <button onclick="window.ScanViewInstance.openCommit()" ${running || doc.committed ? 'disabled' : ''} style="background:rgba(16,185,129,0.12); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; ${running || doc.committed ? 'opacity:0.4; cursor:not-allowed;' : ''}">
                        <i class="fa-solid fa-database"></i> Commit
                    </button>
                    <button onclick="window.ScanViewInstance.discard()" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer;">
                        <i class="fa-solid fa-trash-can"></i> Discard
                    </button>
                </div>
            </div>

            ${warnings}${present}${committed}
            <div id="scan-commit-form"></div>
            ${body}
        </div>`;
    },

    _renderScope(doc, scope) {
        const rows = (scope.files || []).map(row => {
            const pair = `${scope.collection}\u0000${row.file_md5}`;
            const open = this._openPair === pair;
            const fileUrl = `/collections/${encodeURIComponent(scope.collection)}/files/${encodeURIComponent(row.file_md5)}`;
            return `
            <tr style="border-bottom:1px solid var(--border); cursor:pointer;" onclick="window.ScanViewInstance.toggleDiff(${escapeAttr(jsString(scope.collection))}, ${escapeAttr(jsString(row.file_md5))})">
                <td style="padding:8px 12px;"><i class="fa-solid fa-chevron-${open ? 'down' : 'right'}" style="color:var(--dim); font-size:0.7rem;"></i></td>
                <td style="padding:8px 12px;">
                    <a href="${escapeAttr(fileUrl)}" onclick="event.stopPropagation(); Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none; font-weight:600;">${escapeHtml(row.file_name || row.file_md5)}</a>
                    <div style="color:var(--dim); font-size:0.72rem;"><code>${escapeHtml(row.file_md5)}</code></div>
                </td>
                <td style="padding:8px 12px; text-align:right;">${scanScoreCell(row.score)}</td>
                <td style="padding:8px 12px; text-align:right;">${scanScoreCell(row.score_code)}</td>
                <td style="padding:8px 12px; text-align:right;">${scanScoreCell(row.score_library)}</td>
                <td style="padding:8px 12px; text-align:right; color:var(--dim);">${Number(row.matched_functions || 0)}</td>
                <td style="padding:8px 12px; text-align:right; color:var(--dim);">${Number(row.functions_count || 0)}</td>
                <td style="padding:8px 12px; color:var(--dim); font-size:0.75rem;">${escapeHtml(row.architecture || '')}</td>
            </tr>
            <tr id="scan-diff-${escapeAttr(row.file_md5)}" style="display:${open ? 'table-row' : 'none'};">
                <td colspan="8" style="padding:0; background:var(--bg);"></td>
            </tr>`;
        }).join('');

        const axisMeta = {
            'overall': { label: 'Overall', icon: 'fa-solid fa-layer-group', color: 'var(--success)' },
            'code': { label: 'Code', icon: 'fa-solid fa-code', color: 'var(--info, #3b82f6)' },
            'library': { label: 'Library', icon: 'fa-solid fa-cubes', color: 'var(--warning, #d97706)' },
            'content': { label: 'Content', icon: 'fa-solid fa-file-image', color: 'var(--accent, #9333ea)' }
        };

        let clusterRowsData = SCAN_AXES.flatMap(axis => {
            const list = (scope.bin_clusters || {})[axis] || [];
            return list.map(c => {
                const maxSim = Math.max(...(c.via || []).map(v => Number(v.score || 0)));
                const coh = Number(c.cohesion_score || 0);
                return { axis, c, maxSim, coh };
            });
        });

        clusterRowsData.sort((a, b) => {
            let valA, valB;
            switch(window.ScanViewInstance._sortCol) {
                case 'axis': valA = a.axis; valB = b.axis; break;
                case 'cluster': valA = (a.c.cluster_name || a.c.cluster_id || a.c.cluster_uuid).toLowerCase(); valB = (b.c.cluster_name || b.c.cluster_id || b.c.cluster_uuid).toLowerCase(); break;
                case 'coh': valA = a.coh; valB = b.coh; break;
                case 'sim': default: valA = a.maxSim; valB = b.maxSim; break;
            }
            if (valA < valB) return -1 * window.ScanViewInstance._sortDir;
            if (valA > valB) return 1 * window.ScanViewInstance._sortDir;
            return 0;
        });

        const clusterRows = clusterRowsData.map(({ axis, c, maxSim, coh }) => {
            const url = `/collections/${encodeURIComponent(scope.collection)}/files/clusters/${encodeURIComponent(c.cluster_uuid)}?axis=${encodeURIComponent(axis)}`;
            
            const dists = [
                ['Yara', 'fa-solid fa-biohazard', c.yara_distribution],
                ['AV Type', 'fa-solid fa-shield', c.avtype_distribution],
                ['File Type', 'fa-solid fa-file-code', c.filetype_distribution],
                ['CC IP', 'fa-solid fa-network-wired', c.ccip_distribution],
                ['File Name', 'fa-solid fa-file', c.filename_distribution],
                ['MD5', 'fa-solid fa-fingerprint', c.md5_distribution],
            ];
            const cards = dists.map(([title, icon, dist]) => window.renderDist(title, icon, dist)).join('');
            const ax = axisMeta[axis] || { label: axis, icon: 'fa-solid fa-layer-group', color: 'var(--text)' };

            return `
            <tr style="border-bottom:1px solid var(--border);">
                <td style="padding:12px; vertical-align:top; font-weight:bold; color:${ax.color}; white-space:nowrap; width:1px;">
                    <i class="${ax.icon}" style="margin-right:6px;"></i>${escapeHtml(ax.label)}
                </td>
                <td style="padding:12px; vertical-align:top;">
                    <a href="${escapeAttr(url)}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none; font-weight:600; display:inline-block; word-break:break-word; max-width:250px;">${escapeHtml(c.cluster_name || c.cluster_id || c.cluster_uuid)}</a>
                    <div style="font-size:0.72rem; color:var(--dim); margin-top:4px;">${Number(c.member_count || 0)} members · via ${Number((c.via || []).length)}</div>
                </td>
                <td style="padding:12px; vertical-align:top; text-align:right;">
                    ${scanScoreCell(maxSim)}
                </td>
                <td style="padding:12px; vertical-align:top; text-align:right;">
                    <span style="color:var(--success); font-weight:bold;">${(coh * 100).toFixed(1)}%</span>
                </td>
                <td style="padding:12px; vertical-align:top;">
                    ${cards ? `<div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:10px;">${cards}</div>` : '<span class="dim">No metadata distributions</span>'}
                </td>
            </tr>`;
        }).join('');
        
        const thSort = (col, label, title, style) => {
            const active = window.ScanViewInstance._sortCol === col;
            const icon = active ? (window.ScanViewInstance._sortDir === 1 ? 'fa-sort-up' : 'fa-sort-down') : 'fa-sort';
            const color = active ? 'var(--text)' : 'var(--dim)';
            return `<th style="${style || ''}; cursor:pointer; color:${color}; user-select:none;" onclick="window.ScanViewInstance.setSort('${col}')" title="${escapeAttr(title || '')}">
                ${escapeHtml(label)} <i class="fa-solid ${icon}" style="font-size:0.7rem; margin-left:4px;"></i>
            </th>`;
        };

        const clustersHtml = clusterRows ? `
            <div style="margin-top:4px;">
                <div style="font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:8px;">Would join</div>
                <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                    <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.83rem;">
                        <thead>
                            <tr style="border-bottom:1px solid var(--border); background:var(--hover); color:var(--dim);">
                                ${thSort('axis', 'Axis', '', 'padding:8px 12px; width:100px;')}
                                ${thSort('cluster', 'Cluster', '', 'padding:8px 12px; width:220px;')}
                                ${thSort('sim', 'Sim', 'Maximum similarity among matched cluster members', 'padding:8px 12px; width:80px; text-align:right;')}
                                ${thSort('coh', 'Coh', 'Cluster cohesion score', 'padding:8px 12px; width:80px; text-align:right;')}
                                <th style="padding:8px 12px;">Metadata</th>
                            </tr>
                        </thead>
                        <tbody>${clusterRows}</tbody>
                    </table>
                </div>
            </div>` : '';

        return `
        <div style="display:flex; flex-direction:column; gap:10px;">
            <div style="display:flex; align-items:baseline; gap:12px; flex-wrap:wrap;">
                <h2 style="margin:0; font-size:1.05rem; color:var(--text);">${escapeHtml(scope.collection)}</h2>
                <span style="color:var(--dim); font-size:0.78rem;">
                    ${Number(scope.files_scored || 0)} scored of ${Number(scope.files_touched || 0)} touched ·
                    algo ${escapeHtml((scope.params || {}).algo || '')} ·
                    min_score ${Number((scope.params || {}).min_score || 0)} ·
                    min_features ${Number((scope.params || {}).min_features || 0)}
                </span>
            </div>
            ${clustersHtml}
            <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.83rem;">
                    <thead>
                        <tr style="border-bottom:1px solid var(--border); background:var(--hover); color:var(--dim);">
                            <th style="padding:8px 12px; width:24px;"></th>
                            <th style="padding:8px 12px;">File</th>
                            <th style="padding:8px 12px; text-align:right;">Score</th>
                            <th style="padding:8px 12px; text-align:right;">Code</th>
                            <th style="padding:8px 12px; text-align:right;">Library</th>
                            <th style="padding:8px 12px; text-align:right;" title="scan functions with a match in this file">Matched</th>
                            <th style="padding:8px 12px; text-align:right;">Functions</th>
                            <th style="padding:8px 12px;">Arch</th>
                        </tr>
                    </thead>
                    <tbody>${rows || `<tr><td colspan="8" style="padding:25px; text-align:center; color:var(--dim);">No match in this collection.</td></tr>`}</tbody>
                </table>
            </div>
        </div>`;
    },

    // --- matched-function rows -------------------------------------------

    async toggleDiff(collection, md5) {
        const pair = `${collection}\u0000${md5}`;
        const row = document.getElementById(`scan-diff-${md5}`);
        if (!row) return;
        if (this._openPair === pair) {
            this._openPair = null;
            row.style.display = 'none';
            return;
        }
        if (this._openPair) {
            const previous = document.getElementById(`scan-diff-${this._openPair.split('\u0000')[1]}`);
            if (previous) previous.style.display = 'none';
        }
        this._openPair = pair;
        row.style.display = 'table-row';
        row.firstElementChild.innerHTML = '<div style="padding:15px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin"></i> Loading rows...</div>';
        await this.loadRows(collection, md5, 0);
    },

    async loadRows(collection, md5, offset) {
        const row = document.getElementById(`scan-diff-${md5}`);
        if (!row) return;
        const qs = new URLSearchParams({
            collection, md5,
            table: 'matched',
            offset: String(offset),
            limit: String(SCAN_ROWS_PAGE),
            sort_col: 'similarity',
            sort_dir: 'desc'
        });
        try {
            const res = await fetch(`/api/scan/${encodeURIComponent(this._scanId)}/diff?${qs.toString()}`);
            const data = await res.json();
            if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
            row.firstElementChild.innerHTML = this._renderRows(collection, md5, data.matched || {}, offset);
        } catch (e) {
            row.firstElementChild.innerHTML = `<div style="padding:15px; color:#f87171;">${escapeHtml(e.message)}</div>`;
        }
    },

    _renderRows(collection, md5, page, offset) {
        const total = Number(page.total || 0);
        const body = (page.rows || []).map(r => {
            const addr = String(r.func_a || '').split(':').pop();
            const partner = `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(md5)}/functions/${encodeURIComponent(String(r.func_b || '').split(':').pop())}`;
            const cluster = r.cluster_name || r.cluster_id;
            return `
            <tr style="border-bottom:1px solid var(--border);">
                <td style="padding:6px 12px; font-family:monospace; font-size:0.76rem;">${escapeHtml(r.function_name_a || addr)}</td>
                <td style="padding:6px 12px;">
                    <a href="${escapeAttr(partner)}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none; font-family:monospace; font-size:0.76rem;">${escapeHtml(r.function_name_b || String(r.func_b || '').split(':').pop())}</a>
                </td>
                <td style="padding:6px 12px; text-align:right;">${scanScoreCell(r.similarity)}</td>
                <td style="padding:6px 12px; color:var(--dim); font-size:0.76rem;">${cluster ? escapeHtml(String(cluster)) : '—'}</td>
                <td style="padding:6px 12px; text-align:center;">${r.would_join ? '<i class="fa-solid fa-check" style="color:#10b981;"></i>' : '<span style="color:var(--dim);">—</span>'}</td>
            </tr>`;
        }).join('');

        const prev = offset > 0
            ? `<button onclick="window.ScanViewInstance.loadRows(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(md5))}, ${Math.max(0, offset - SCAN_ROWS_PAGE)})" style="background:var(--hover); border:1px solid var(--border); color:var(--fg); padding:5px 12px; border-radius:6px; font-size:0.76rem; cursor:pointer;">Previous</button>`
            : '';
        const next = offset + SCAN_ROWS_PAGE < total
            ? `<button onclick="window.ScanViewInstance.loadRows(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(md5))}, ${offset + SCAN_ROWS_PAGE})" style="background:var(--hover); border:1px solid var(--border); color:var(--fg); padding:5px 12px; border-radius:6px; font-size:0.76rem; cursor:pointer;">Next</button>`
            : '';

        return `
        <div style="padding:12px 16px; display:flex; flex-direction:column; gap:10px;">
            <table style="width:100%; border-collapse:collapse; text-align:left;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); color:var(--dim); font-size:0.72rem; text-transform:uppercase;">
                        <th style="padding:6px 12px;">Scanned function</th>
                        <th style="padding:6px 12px;">Matched function</th>
                        <th style="padding:6px 12px; text-align:right;">Similarity</th>
                        <th style="padding:6px 12px;">Nearest cluster</th>
                        <th style="padding:6px 12px; text-align:center;" title="would the real clustering threshold merge these two">Would join</th>
                    </tr>
                </thead>
                <tbody>${body || '<tr><td colspan="5" style="padding:15px; color:var(--dim);">No matched functions.</td></tr>'}</tbody>
            </table>
            <div style="display:flex; align-items:center; gap:10px; font-size:0.76rem; color:var(--dim);">
                ${prev}${next}
                <span>${offset + 1}-${Math.min(offset + SCAN_ROWS_PAGE, total)} of ${total}</span>
            </div>
        </div>`;
    },

    // --- commit / discard -------------------------------------------------

    openCommit() {
        const host = document.getElementById('scan-commit-form');
        if (!host || !this._doc) return;
        const options = (this._doc.scopes || [])
            .map(c => `<option value="${escapeAttr(c)}">${escapeHtml(c)}</option>`).join('');
        host.innerHTML = `
        <div style="border:1px solid var(--border); border-radius:8px; background:var(--card-bg); padding:16px; display:flex; gap:12px; flex-wrap:wrap; align-items:flex-end;">
            <div>
                <label style="display:block; font-size:0.75rem; color:var(--dim); margin-bottom:5px;">Collection</label>
                <select id="scan-commit-collection" style="padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.82rem;">${options}</select>
            </div>
            <div>
                <label style="display:block; font-size:0.75rem; color:var(--dim); margin-bottom:5px;">Batch name</label>
                <input id="scan-commit-batch" type="text" placeholder="scan ${escapeAttr(this._doc.scan_id || '')}" style="padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.82rem;">
            </div>
            <label style="display:inline-flex; align-items:center; gap:6px; font-size:0.8rem; color:var(--dim); cursor:pointer;" title="A lean scan skipped some tagging modules; Ghidra has to run again to add them">
                <input type="checkbox" id="scan-commit-topup" checked> Tag top-up
            </label>
            <button onclick="window.ScanViewInstance.commit()" style="background:rgba(16,185,129,0.12); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer;">
                <i class="fa-solid fa-database"></i> Ingest, no re-analysis
            </button>
            <div id="scan-commit-error" style="flex-basis:100%; color:#f87171; font-size:0.8rem;"></div>
        </div>`;
    },

    async commit() {
        const errEl = document.getElementById('scan-commit-error');
        const collection = (document.getElementById('scan-commit-collection') || {}).value;
        const batch = (document.getElementById('scan-commit-batch') || {}).value;
        const topup = (document.getElementById('scan-commit-topup') || {}).checked;
        if (errEl) errEl.textContent = '';
        if (!collection) {
            if (errEl) errEl.textContent = 'Pick a collection.';
            return;
        }
        const qs = new URLSearchParams({ collection, topup: String(!!topup) });
        if (batch) qs.set('batch_name', batch);
        try {
            const res = await fetch(`/api/scan/${encodeURIComponent(this._scanId)}/commit?${qs.toString()}`, { method: 'POST' });
            const body = await res.json();
            if (!res.ok || body.error) throw new Error(body.error || `HTTP ${res.status}`);
            const host = document.getElementById('scan-commit-form');
            if (host) host.innerHTML = '';
            await this._refresh(document.getElementById('module-view-container'), this._scanId);
        } catch (e) {
            if (errEl) errEl.textContent = e.message;
        }
    },

    async discard() {
        try {
            await fetch(`/api/scan/${encodeURIComponent(this._scanId)}`, { method: 'DELETE' });
        } catch (e) {
            /* expired already; the navigation below is still the right answer */
        }
        Nav.openPath('/scans');
    }
};
