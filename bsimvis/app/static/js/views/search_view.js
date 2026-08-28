/**
 * Search View
 * List mode: /searches
 * Detail mode: /searches/{id}
 *
 * Results table reuses the same row-rendering/selection stack as the file
 * view's function table: EntityRenderer.renderFunction/renderTag for the
 * cells, and `new TableSelection(id)` after each render (the constructor is
 * idempotent per table id, so calling it again after a re-render is safe).
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
                <span style="font-size:0.75rem; color:var(--dim);"><i class="fa-solid fa-arrows-up-down"></i> shift/ctrl-click or drag to select rows, ctrl+A for all, ctrl+C to copy</span>
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
            // TableSelection takes an element id, not an element (constructor is idempotent per table)
            if (window.TableSelection) new window.TableSelection('search-results-table');
        } catch (e) {
            container.innerHTML = `<div style="padding:20px; color:#f87171;">${e.message}</div>`;
        }
    },

    _renderResults(results, collection) {
        if (!results.length) {
            return `<div style="color:var(--dim); font-size:0.85rem; padding:30px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">No results yet.</div>`;
        }
        const rows = results.map(r => {
            const fColl = r.collection || collection || '';
            // Rows carry their own function metadata (routes/searches.py enriches them via
            // fetch_function_data) so EntityRenderer can render the same colored signature,
            // tag editor and right-click menu the file view's function table uses.
            const f = {
                function_id: r.func_id,
                function_name: r.function_name,
                namespace: r.namespace,
                parameters: r.parameters,
                return_type: r.return_type,
                entrypoint_address: r.entrypoint_address,
                file_md5: r.file_md5,
                collection: fColl,
                bsim_features_count: r.bsim_features_count,
                note_owners: r.note_owners,
                tags: r.tags || [],
                user_tags: r.user_tags || []
            };
            const tagsHtml = window.EntityRenderer ? window.EntityRenderer.renderTag('function', r.func_id, f.tags, f.user_tags) : '';
            return `
            <tr data-id="${escapeAttr(r.func_id)}" style="border-bottom: 1px solid var(--border);"
                data-entity-data='${escapeAttr(JSON.stringify(f))}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                <td style="padding:8px 15px; min-width:260px; max-width:420px;">${window.EntityRenderer ? window.EntityRenderer.renderFunction(f) : `<code>${escapeHtml(r.func_id)}</code>`}</td>
                <td style="padding:8px 15px; min-width:140px; max-width:220px; overflow:hidden;">${tagsHtml}</td>
                <td style="padding:8px 15px; white-space:nowrap;">${verdictBadge(r.verdict)}</td>
                <td style="padding:8px 15px; max-width:340px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--dim); font-size:0.82rem;" title="${escapeAttr(r.evidence || '')}">${escapeHtml(r.evidence || '')}</td>
                <td style="padding:8px 15px; white-space:nowrap;">${r.suggested_tag ? `<code style="font-size:0.75rem; color:var(--accent);">${escapeHtml(r.suggested_tag)}</code>` : ''}</td>
            </tr>`;
        }).join('');

        return `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow-x:auto; background:var(--card-bg);">
            <table id="search-results-table" style="width:100%; min-width:900px; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); background: var(--hover); color:var(--dim);">
                        <th style="padding:8px 15px;">Function</th>
                        <th style="padding:8px 15px;">Tags</th>
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

function searchViewSelectedFuncIds() {
    return window.getSelectedTableIds ? window.getSelectedTableIds('function') : [];
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
    // Same "current collection" detection every other view uses (routing state,
    // falling back to the RESTful/hash path) rather than leaving it blank.
    const currentCollection = (window.getRoutingState && window.getRoutingState().collection)
        || (window.getCollectionFromHash && window.getCollectionFromHash())
        || '';
    container.innerHTML = `
        <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:18px; display:flex; flex-direction:column; gap:12px;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <label style="display:flex; flex-direction:column; gap:5px; font-size:0.78rem; color:var(--dim);">Collection
                    <input id="search-form-collection" type="text" value="${escapeAttr(currentCollection)}" placeholder="main" style="padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:6px;">
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
    // MD5 fields reuse the same attachAutocomplete(input, 'file', 'file_md5', ...) dropdown
    // dashboard.js's advanced file search uses, scoped to the current collection.
    const md5Autocomplete = `onfocus="attachAutocomplete(this, 'file', 'file_md5', (val) => { this.value = val; })"`;
    if (type === 'file') {
        fields.innerHTML = `<label style="${labelStyle}">File MD5<input id="search-form-md5" type="text" autocomplete="off" ${md5Autocomplete} style="${inputStyle}"></label>`;
    } else if (type === 'filter') {
        fields.innerHTML = `<label style="${labelStyle}">Filter query string (same syntax as function search)<input id="search-form-filters" type="text" placeholder="tag=x&min_features=5" style="${inputStyle}"></label>`;
    } else if (type === 'pair') {
        fields.innerHTML = `
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                <label style="${labelStyle}">MD5 A<input id="search-form-md5a" type="text" autocomplete="off" ${md5Autocomplete} style="${inputStyle}"></label>
                <label style="${labelStyle}">MD5 B<input id="search-form-md5b" type="text" autocomplete="off" ${md5Autocomplete} style="${inputStyle}"></label>
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
