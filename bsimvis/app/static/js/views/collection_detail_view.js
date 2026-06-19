/**
 * Collection Detail View
 * Loaded when navigating to /collections/{id}
 */

window.CollectionDetailView = {
    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const collName = params.collection;
        if (!collName) {
            container.innerHTML = '<div style="padding:30px; color:#f87171;">Error: No collection name provided.</div>';
            return;
        }

        container.innerHTML = `
            <div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim); font-size:1rem;">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading Collection Details...
            </div>`;

        try {
            const [collRes, poolsRes] = await Promise.all([
                fetch(`/api/collection/search?q=${encodeURIComponent(collName)}`),
                fetch(`/api/pool?collection=${encodeURIComponent(collName)}`)
            ]);

            if (!collRes.ok) throw new Error('Failed to load collections list');
            const data = await collRes.json();
            const collection = (data.collections || []).find(c => c.name === collName);

            let associatedPools = [];
            if (poolsRes.ok) {
                const pd = await poolsRes.json();
                associatedPools = pd.pools || [];
            }

            if (!collection) {
                container.innerHTML = this._renderPage({
                    name: collName,
                    total_files: 0,
                    total_functions: 0,
                    total_batches: 0,
                    last_updated: 0
                }, associatedPools);
            } else {
                container.innerHTML = this._renderPage(collection, associatedPools);
            }
        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${e.message}</div>`;
        }
    },

    _statusBadge(status) {
        if (status === 'current') return `<span style="background:rgba(16,185,129,0.15); border:1px solid rgba(16,185,129,0.35); color:#10b981; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-check"></i> Current</span>`;
        if (status === 'outdated') return `<span style="background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.35); color:#f59e0b; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle-exclamation"></i> Outdated</span>`;
        return `<span style="background:rgba(156,163,175,0.15); border:1px solid rgba(156,163,175,0.3); color:#9ca3af; padding:4px 12px; border-radius:20px; font-size:0.8rem; font-weight:700; display:inline-flex; align-items:center; gap:6px;"><i class="fa-solid fa-circle"></i> ${status || 'created'}</span>`;
    },

    _renderPage(coll, pools) {
        const name = coll.name;
        const files = coll.total_files !== undefined ? coll.total_files : 0;
        const funcs = coll.total_functions !== undefined ? coll.total_functions : 0;
        const batches = coll.total_batches !== undefined ? coll.total_batches : 0;
        const updated = coll.last_updated ? (typeof window.formatDate === 'function' ? formatDate(coll.last_updated * 1000) : new Date(coll.last_updated * 1000).toLocaleString()) : '—';

        const filesUrl = `/collections/${encodeURIComponent(name)}/files`;
        const funcsUrl = `/collections/${encodeURIComponent(name)}/functions`;
        const batchesUrl = `/collections/${encodeURIComponent(name)}/batches`;

        const poolsHtml = `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); background:rgba(255,255,255,0.02); color:var(--dim);">
                        <th style="padding:10px 15px;">Pool</th>
                        <th style="padding:10px 15px;">Status</th>
                        <th style="padding:10px 15px; text-align:right;">Last Built</th>
                    </tr>
                </thead>
                <tbody>
                    ${pools.map(pool => {
                        const poolUrl = `/pools/${encodeURIComponent(pool.id)}`;
                        const lastBuilt = pool.last_built_at && pool.last_built_at != '0' ? (typeof window.formatDate === 'function' ? formatDate(parseInt(pool.last_built_at)) : pool.last_built_at) : '—';
                        return `
                        <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                            <td style="padding:10px 15px;"><a href="${poolUrl}" onclick="Nav.openPath('${poolUrl}', event)" style="font-weight:bold; color:var(--text); text-decoration:none;">${pool.name || pool.id}</a></td>
                            <td style="padding:10px 15px;">${this._statusBadge(pool.sync_status || 'created')}</td>
                            <td style="padding:10px 15px; text-align:right; color:var(--dim);">${lastBuilt}</td>
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
        </div>`;

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:22px;">

            <!-- HEADER -->
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; flex-wrap:wrap;">
                        <i class="fa-solid fa-layer-group" style="font-size:1.4rem; color:var(--accent);"></i>
                        <h1 style="margin:0; font-size:1.5rem; color:var(--text);">${name}</h1>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                        <code style="font-size:0.8rem; color:var(--dim); background:rgba(255,255,255,0.04); padding:3px 10px; border-radius:4px; border:1px solid var(--border);">${name}</code>
                        <button onclick="copyToClipboard('${name}', this)" class="btn-copy" title="Copy Collection Name" style="padding:4px 8px; font-size:0.75rem;">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                    <button onclick="Nav.openPath('/collections/${encodeURIComponent(name)}/upload')" style="background:rgba(59,130,246,0.12); border:1px solid rgba(59,130,246,0.35); color:#60a5fa; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-upload"></i> Upload Binaries</button>
                    <button onclick="window.collectionDetailClean('${name}', this)" style="background:rgba(168,85,247,0.12); border:1px solid rgba(168,85,247,0.35); color:#c084fc; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-broom"></i> Clean</button>
                    <button onclick="window.collectionDetailDelete('${name}', this)" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px; box-sizing:border-box; white-space:nowrap; flex-shrink:0;"><i class="fa-solid fa-trash-can"></i> Delete</button>
                </div>
            </div>

            <!-- META ROW -->
            <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
                <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 18px; font-size:0.8rem; display:flex; align-items:center; gap:8px;">
                    <i class="fa-solid fa-clock-rotate-left" style="color:var(--dim);"></i>
                    <span style="color:var(--dim);">Last Updated</span>
                    <span style="color:var(--text); font-weight:600;">${updated}</span>
                </div>
            </div>

            <!-- MAIN GRID -->
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px; align-items:start;">

                <!-- LEFT COLUMN -->
                <div style="display:flex; flex-direction:column; gap:20px;">
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-circle-info"></i> Collection Metadata
                        </div>
                        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; overflow:hidden; background:var(--card-bg);">
                            <table style="width:100%; border-collapse:collapse; text-align:left; font-size:0.85rem;">
                                <thead>
                                    <tr style="border-bottom:1px solid var(--border); background:rgba(255,255,255,0.02); color:var(--dim);">
                                        <th style="padding:10px 15px;">Entity Type</th>
                                        <th style="padding:10px 15px; text-align:right;">Count / Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-boxes-stacked" style="margin-right:8px; width:16px;"></i>Batches</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${batchesUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${batches}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-file-code" style="margin-right:8px; width:16px;"></i>Files</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${filesUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${files}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code" style="margin-right:8px; width:16px;"></i>Functions</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="${funcsUrl}" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">${funcs}</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-code-compare" style="margin-right:8px; width:16px;"></i>Function Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/functions/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">View Similarities</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>Function Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/functions/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">View Clusters</a></td>
                                    </tr>
                                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-right-left" style="margin-right:8px; width:16px;"></i>File Similarities</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/files/similarities" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">View Similarities</a></td>
                                    </tr>
                                    <tr style="transition:background 0.2s;" onmouseover="this.style.background='rgba(255,255,255,0.01)'" onmouseout="this.style.background='transparent'">
                                        <td style="padding:10px 15px; font-weight:600; color:var(--dim);"><i class="fa-solid fa-bullseye" style="margin-right:8px; width:16px;"></i>File Clusters</td>
                                        <td style="padding:10px 15px; text-align:right;"><a href="/collections/${encodeURIComponent(name)}/files/clusters" onclick="Nav.openPath(this.href, event)" style="color:var(--accent); font-weight:700; text-decoration:none;">View Clusters</a></td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- RIGHT COLUMN -->
                <div style="display:flex; flex-direction:column; gap:20px;">
                    <div>
                        <div style="font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:var(--dim); margin-bottom:12px; display:flex; align-items:center; gap:7px;">
                            <i class="fa-solid fa-diagram-project"></i> Associated Pools
                        </div>
                        ${pools.length > 0 ? poolsHtml : '<div style="color:var(--dim); font-size:0.85rem; padding:20px; text-align:center; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">Not associated with any pools</div>'}
                    </div>
                </div>

            </div>
        </div>`;
    }
};

window.collectionDetailClean = async function(collName, btn) {
    if (!confirm(`Clean up temporary upload keys for collection "${collName}"?`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/collection/clean`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: collName })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Collection clean enqueued! Job ID: ${data.job_id}`);
    } catch(e) {
        alert(`Failed to clean collection: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-broom"></i> Clean'; }
    }
};

window.collectionDetailDelete = async function(collName, btn) {
    if (!confirm(`Delete collection "${collName}"? This cannot be undone.`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    try {
        const res = await fetch(`/api/collection/delete`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: collName })
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.error || `HTTP ${res.status}`); }
        const data = await res.json();
        alert(`Collection deletion enqueued! Job ID: ${data.job_id}`);
        Nav.openPath('/collections');
    } catch(e) {
        alert(`Failed to delete collection: ${e.message}`);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-trash-can"></i> Delete'; }
    }
};
