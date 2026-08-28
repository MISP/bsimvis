/**
 * Search View
 * List mode: /searches
 * Detail mode: /searches/{id}
 *
 * Row selection uses plain checkboxes rather than the TableSelection class
 * (table_selection.js) -- that class only auto-attaches on the page's single
 * DOMContentLoaded event, and this view's results table is injected later by
 * an SPA route change, so it would need to be constructed manually anyway;
 * checkboxes are simpler to get right without a live browser to verify
 * TableSelection's drag/keyboard-nav wiring against a dynamically-inserted
 * table.
 */

const VERDICT_STYLE = {
    yes: { color: '#10b981', bg: 'rgba(16,185,129,0.15)', border: 'rgba(16,185,129,0.35)', icon: 'fa-circle-check' },
    maybe: { color: '#f59e0b', bg: 'rgba(245,158,11,0.15)', border: 'rgba(245,158,11,0.35)', icon: 'fa-circle-question' },
    no: { color: '#9ca3af', bg: 'rgba(156,163,175,0.12)', border: 'rgba(156,163,175,0.3)', icon: 'fa-circle-minus' }
};

const STATUS_STYLE = {
    running: { color: '#60a5fa', icon: 'fa-circle-notch fa-spin' },
    completed: { color: '#10b981', icon: 'fa-check-circle' },
    failed: { color: '#f87171', icon: 'fa-exclamation-circle' },
    cancelled: { color: '#9ca3af', icon: 'fa-ban' }
};

function searchStatusBadge(status) {
    const s = STATUS_STYLE[status] || STATUS_STYLE.cancelled;
    return `<span style="color:${s.color}; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid ${s.icon}"></i> ${status}</span>`;
}

function verdictBadge(verdict) {
    const v = VERDICT_STYLE[verdict] || VERDICT_STYLE.no;
    return `<span style="background:${v.bg}; border:1px solid ${v.border}; color:${v.color}; padding:3px 10px; border-radius:20px; font-size:0.72rem; font-weight:700; display:inline-flex; align-items:center; gap:5px; white-space:nowrap;"><i class="fa-solid ${v.icon}"></i> ${verdict}</span>`;
}

window.SearchView = {
    _pollTimer: null,
    _stopped: false,

    destroy() {
        this._stopped = true;
        if (this._pollTimer) {
            clearTimeout(this._pollTimer);
            this._pollTimer = null;
        }
    },

    async init(params, containerId) {
        this._stopped = false;
        const container = document.getElementById(containerId);
        if (!container) return;

        const searchId = params.search_id;
        if (searchId) {
            await this._initDetail(container, searchId);
        } else {
            await this._initList(container);
        }
    },

    // --- list mode ------------------------------------------------------

    async _initList(container) {
        container.innerHTML = `<div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Searches...</div>`;
        try {
            const res = await fetch('/api/searches?limit=100');
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            container.innerHTML = this._renderList(data.searches || []);
        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${e.message}</div>`;
        }
    },

    _renderList(searches) {
        const rows = searches.map(s => {
            const url = `/searches/${encodeURIComponent(s.id)}`;
            const scopeType = (s.scope && s.scope.type) || '?';
            return `
            <tr style="border-bottom: 1px solid var(--border); cursor:pointer;" onmouseover="this.style.background='var(--hover)'" onmouseout="this.style.background='transparent'" onclick="Nav.openPath(${escapeAttr(jsString(url))})">
                <td style="padding:10px 15px; font-weight:600;">${escapeHtml(s.name || s.query || s.id)}</td>
                <td style="padding:10px 15px; color:var(--dim);"><code style="font-size:0.78rem;">${escapeHtml(scopeType)}</code></td>
                <td style="padding:10px 15px;">${searchStatusBadge(s.status)}</td>
                <td style="padding:10px 15px; text-align:right; color:var(--accent); font-weight:700;">${s.total ?? '—'}</td>
                <td style="padding:10px 15px; color:var(--dim); font-size:0.8rem;">${window.formatDate ? window.formatDate(s.created_at) : s.created_at}</td>
                <td style="padding:10px 15px; text-align:right;">
                    <button onclick="event.stopPropagation(); window.searchViewDelete(${escapeAttr(jsString(s.id))}, this)" title="Delete" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:5px 10px; border-radius:6px; cursor:pointer;"><i class="fa-solid fa-trash-can"></i></button>
                </td>
            </tr>`;
        }).join('');

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:20px;">
            <div style="display:flex; align-items:center; justify-content:space-between;">
                <h1 style="margin:0; font-size:1.5rem; color:var(--text); display:flex; align-items:center; gap:10px;"><i class="fa-solid fa-list-check" style="color:var(--accent);"></i> Searches</h1>
                <button onclick="window.searchViewOpenNewForm(this)" style="background:rgba(59,130,246,0.12); border:1px solid rgba(59,130,246,0.35); color:#60a5fa; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px;"><i class="fa-solid fa-plus"></i> New Search</button>
            </div>
            <div id="search-new-form-container"></div>
            <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                    <thead>
                        <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                            <th style="padding:10px 15px;">Name / Query</th>
                            <th style="padding:10px 15px;">Scope</th>
                            <th style="padding:10px 15px;">Status</th>
                            <th style="padding:10px 15px; text-align:right;">Functions</th>
                            <th style="padding:10px 15px;">Created</th>
                            <th style="padding:10px 15px;"></th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows || `<tr><td colspan="6" style="padding:30px; text-align:center; color:var(--dim);">No searches yet.</td></tr>`}
                    </tbody>
                </table>
            </div>
        </div>`;
    },

    // --- detail mode ------------------------------------------------------

    async _initDetail(container, searchId) {
        container.innerHTML = `<div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Search...</div>`;
        try {
            const res = await fetch(`/api/searches/${encodeURIComponent(searchId)}`);
            if (!res.ok) throw new Error(res.status === 404 ? 'Search not found' : `HTTP ${res.status}`);
            const meta = await res.json();
            container.innerHTML = this._renderDetailShell(meta);
            await this._refreshResults(searchId, meta.collection);
            if (meta.status === 'running' && meta.job_id) {
                this._pollJob(searchId, meta.job_id, meta.collection);
            }
        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${e.message}</div>`;
        }
    },

    _renderDetailShell(meta) {
        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:20px;">
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <h1 style="margin:0 0 6px 0; font-size:1.3rem; color:var(--text);"><i class="fa-solid fa-magnifying-glass" style="color:var(--accent); margin-right:8px;"></i>${escapeHtml(meta.query || meta.name || '')}</h1>
                    <div style="color:var(--dim); font-size:0.82rem; display:flex; align-items:center; gap:14px; flex-wrap:wrap;">
                        <span>Scope: <code>${escapeHtml((meta.scope && meta.scope.type) || '?')}</code></span>
                        <span>Collection: <code>${escapeHtml(meta.collection || '')}</code></span>
                        <span id="search-detail-status">${searchStatusBadge(meta.status)}</span>
                    </div>
                </div>
                <button onclick="window.searchViewDelete(${escapeAttr(jsString(meta.id))}, this, true)" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px;"><i class="fa-solid fa-trash-can"></i> Delete</button>
            </div>

            <div id="search-progress-container"></div>

            <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
                <label style="display:flex; align-items:center; gap:6px; font-size:0.8rem; color:var(--dim);"><input type="checkbox" id="search-select-all" onchange="window.searchViewToggleAll(this)"> Select all</label>
                <input id="search-apply-tag-input" type="text" placeholder="tag to apply, e.g. category:persistence:file" style="flex:1; min-width:220px; padding:7px 10px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-size:0.8rem;">
                <button onclick="window.searchViewApplyTag(${escapeAttr(jsString(meta.id))})" style="background:rgba(16,185,129,0.12); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:7px 14px; border-radius:6px; font-size:0.8rem; font-weight:700; cursor:pointer;"><i class="fa-solid fa-tag"></i> Apply tag to selected</button>
                <button onclick="window.searchViewAnalyzeSelected(${escapeAttr(jsString(meta.id))})" style="background:rgba(168,85,247,0.12); border:1px solid rgba(168,85,247,0.35); color:#c084fc; padding:7px 14px; border-radius:6px; font-size:0.8rem; font-weight:700; cursor:pointer;"><i class="fa-solid fa-wand-magic-sparkles"></i> Send selected to deep analysis</button>
            </div>

            <div id="search-results-container"></div>
        </div>`;
    },

    _pollJob(searchId, jobId, collection) {
        const poll = async () => {
            if (this._stopped) return;
            try {
                const res = await fetch(`/api/jobs/${jobId}`);
                const job = await res.json();
                const statusEl = document.getElementById('search-detail-status');
                if (statusEl) statusEl.innerHTML = searchStatusBadge(job.status === 'running' ? 'running' : job.status);
                const progress = document.getElementById('search-progress-container');
                if (progress && job.status === 'running') {
                    progress.innerHTML = `<div style="font-size:0.78rem; color:var(--dim);">${job.progress || 0}% -- ${job.processed_items || 0}/${job.total_items || '?'} classified</div>`;
                }
                if (['completed', 'failed', 'cancelled'].includes(job.status)) {
                    if (progress) progress.innerHTML = '';
                    await this._refreshResults(searchId, collection);
                    return;
                }
            } catch (e) {
                // transient -- keep polling
            }
            this._pollTimer = setTimeout(poll, 2000);
        };
        poll();
    },

    async _refreshResults(searchId, collection) {
        const container = document.getElementById('search-results-container');
        if (!container) return;
        try {
            const res = await fetch(`/api/searches/${encodeURIComponent(searchId)}/results?limit=500`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            container.innerHTML = this._renderResults(data.results || [], collection);
        } catch (e) {
            container.innerHTML = `<div style="padding:20px; color:#f87171;">${e.message}</div>`;
        }
    },

    _renderResults(results, collection) {
        if (!results.length) {
            return `<div style="color:var(--dim); font-size:0.85rem; padding:30px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">No results yet.</div>`;
        }
        const rows = results.map(r => {
            const parsed = window.parseFuncId ? window.parseFuncId(r.func_id) : null;
            const funcUrl = parsed && parsed.md5
                ? `/collections/${encodeURIComponent(parsed.collection || collection || '')}/functions/${parsed.md5}/${parsed.address}`
                : null;
            const nameOrId = funcUrl
                ? `<a href="${funcUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); text-decoration:none; font-weight:600;">${escapeHtml(r.func_id)}</a>`
                : `<code>${escapeHtml(r.func_id)}</code>`;
            return `
            <tr data-func-id="${escapeAttr(r.func_id)}" style="border-bottom: 1px solid var(--border);">
                <td style="padding:8px 15px;"><input type="checkbox" class="search-result-checkbox"></td>
                <td style="padding:8px 15px; font-family:monospace; font-size:0.78rem;">${nameOrId}</td>
                <td style="padding:8px 15px;">${verdictBadge(r.verdict)}</td>
                <td style="padding:8px 15px; color:var(--dim); font-size:0.82rem;">${escapeHtml(r.evidence || '')}</td>
                <td style="padding:8px 15px;">${r.suggested_tag ? `<code style="font-size:0.75rem; color:var(--accent);">${escapeHtml(r.suggested_tag)}</code>` : ''}</td>
            </tr>`;
        }).join('');

        return `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
            <table id="search-results-table" style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                        <th style="padding:8px 15px; width:34px;"></th>
                        <th style="padding:8px 15px;">Function</th>
                        <th style="padding:8px 15px;">Verdict</th>
                        <th style="padding:8px 15px;">Evidence</th>
                        <th style="padding:8px 15px;">Suggested tag</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>`;
    }
};

// --- action handlers scoped to this view --------------------------------

window.searchViewToggleAll = function(checkbox) {
    document.querySelectorAll('.search-result-checkbox').forEach(cb => { cb.checked = checkbox.checked; });
};

function searchViewSelectedFuncIds() {
    return Array.from(document.querySelectorAll('.search-result-checkbox:checked'))
        .map(cb => cb.closest('tr').getAttribute('data-func-id'))
        .filter(Boolean);
}

window.searchViewApplyTag = async function(searchId) {
    const funcIds = searchViewSelectedFuncIds();
    const tagInput = document.getElementById('search-apply-tag-input');
    const tag = tagInput ? tagInput.value.trim() : '';
    if (!funcIds.length) { alert('Select at least one function first.'); return; }
    if (!tag) { alert('Enter a tag to apply.'); return; }
    try {
        const res = await fetch(`/api/searches/${encodeURIComponent(searchId)}/apply_tag`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ func_ids: funcIds, tag })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        if (typeof showToast === 'function') showToast(`Tagged ${data.applied.length}/${funcIds.length} function(s)`, 'success');
        else alert(`Tagged ${data.applied.length}/${funcIds.length} function(s)`);
    } catch (e) {
        alert(`Failed to apply tag: ${e.message}`);
    }
};

window.searchViewAnalyzeSelected = async function(searchId) {
    const funcIds = searchViewSelectedFuncIds();
    if (!funcIds.length) { alert('Select at least one function first.'); return; }
    try {
        const res = await fetch(`/api/searches/${encodeURIComponent(searchId)}/analyze`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ func_ids: funcIds })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        if (typeof showToast === 'function') showToast(`Deep analysis started -- job ${data.job_id}`, 'success');
        else alert(`Deep analysis started -- job ${data.job_id}`);
    } catch (e) {
        alert(`Failed to start deep analysis: ${e.message}`);
    }
};

window.searchViewDelete = async function(searchId, btn, redirectToList) {
    if (!confirm('Delete this search?')) return;
    if (btn) { btn.disabled = true; }
    try {
        const res = await fetch(`/api/searches/${encodeURIComponent(searchId)}`, { method: 'DELETE' });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        if (redirectToList) {
            Nav.openPath('/searches');
        } else if (typeof refreshData === 'function') {
            refreshData(false, true);
        }
    } catch (e) {
        alert(`Failed to delete search: ${e.message}`);
        if (btn) { btn.disabled = false; }
    }
};

window.searchViewOpenNewForm = function(btn) {
    const container = document.getElementById('search-new-form-container');
    if (!container) return;
    if (container.innerHTML.trim()) { container.innerHTML = ''; return; }
    container.innerHTML = `
        <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:18px; display:flex; flex-direction:column; gap:12px;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <label style="display:flex; flex-direction:column; gap:5px; font-size:0.78rem; color:var(--dim);">Collection
                    <input id="search-form-collection" type="text" placeholder="main" style="padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px;">
                </label>
                <label style="display:flex; flex-direction:column; gap:5px; font-size:0.78rem; color:var(--dim);">Scope
                    <select id="search-form-scope-type" onchange="window.searchViewScopeChanged(this)" style="padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px;">
                        <option value="collection">Whole collection</option>
                        <option value="file">One file</option>
                        <option value="filter">Filter selection</option>
                        <option value="pair">Bin_sim pair</option>
                    </select>
                </label>
            </div>
            <div id="search-form-scope-fields"></div>
            <label style="display:flex; flex-direction:column; gap:5px; font-size:0.78rem; color:var(--dim);">What are you looking for?
                <textarea id="search-form-query" rows="2" placeholder="e.g. the function decrypting a .dat file" style="padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px; font-family:inherit; resize:vertical;"></textarea>
            </label>
            <div style="display:flex; gap:10px; justify-content:flex-end;">
                <button onclick="document.getElementById('search-new-form-container').innerHTML=''" style="background:var(--hover); border:1px solid var(--border); color:var(--text); padding:8px 16px; border-radius:6px; cursor:pointer;">Cancel</button>
                <button onclick="window.searchViewSubmitNew(this)" style="background:rgba(59,130,246,0.12); border:1px solid rgba(59,130,246,0.35); color:#60a5fa; padding:8px 18px; border-radius:6px; font-weight:700; cursor:pointer;"><i class="fa-solid fa-magnifying-glass"></i> Start Search</button>
            </div>
        </div>`;
    window.searchViewScopeChanged(document.getElementById('search-form-scope-type'));
};

window.searchViewScopeChanged = function(select) {
    const fields = document.getElementById('search-form-scope-fields');
    if (!fields) return;
    const type = select.value;
    const inputStyle = 'padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px;';
    const labelStyle = 'display:flex; flex-direction:column; gap:5px; font-size:0.78rem; color:var(--dim);';
    if (type === 'file') {
        fields.innerHTML = `<label style="${labelStyle}">File MD5<input id="search-form-md5" type="text" style="${inputStyle}"></label>`;
    } else if (type === 'filter') {
        fields.innerHTML = `<label style="${labelStyle}">Filter query string (same syntax as function search)<input id="search-form-filters" type="text" placeholder="tag=x&min_features=5" style="${inputStyle}"></label>`;
    } else if (type === 'pair') {
        fields.innerHTML = `
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <label style="${labelStyle}">MD5 A<input id="search-form-md5a" type="text" style="${inputStyle}"></label>
                <label style="${labelStyle}">MD5 B<input id="search-form-md5b" type="text" style="${inputStyle}"></label>
            </div>`;
    } else {
        fields.innerHTML = '';
    }
};

window.searchViewSubmitNew = async function(btn) {
    const collection = (document.getElementById('search-form-collection') || {}).value?.trim();
    const query = (document.getElementById('search-form-query') || {}).value?.trim();
    const type = (document.getElementById('search-form-scope-type') || {}).value;
    if (!collection || !query) { alert('Collection and query are required.'); return; }

    const scope = { type };
    if (type === 'file') {
        scope.md5 = (document.getElementById('search-form-md5') || {}).value?.trim();
        if (!scope.md5) { alert('File MD5 is required.'); return; }
    } else if (type === 'filter') {
        scope.filters = (document.getElementById('search-form-filters') || {}).value?.trim();
        if (!scope.filters) { alert('Filter query string is required.'); return; }
    } else if (type === 'pair') {
        scope.md5_a = (document.getElementById('search-form-md5a') || {}).value?.trim();
        scope.md5_b = (document.getElementById('search-form-md5b') || {}).value?.trim();
        if (!scope.md5_a || !scope.md5_b) { alert('Both MD5 A and MD5 B are required.'); return; }
    }

    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch('/api/searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, query, scope })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        Nav.openPath(`/searches/${encodeURIComponent(data.search_id)}`);
    } catch (e) {
        alert(`Failed to start search: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-magnifying-glass"></i> Start Search'; }
    }
};
