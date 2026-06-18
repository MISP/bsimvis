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
            const res = await fetch(`/api/collection/search?q=${encodeURIComponent(collName)}`);
            if (!res.ok) throw new Error('Failed to load collections list');
            const data = await res.json();
            const collection = (data.collections || []).find(c => c.name === collName);

            if (!collection) {
                // Return dummy metadata if backend doesn't return info yet
                container.innerHTML = this._renderPage({
                    name: collName,
                    total_files: 0,
                    total_functions: 0,
                    total_batches: 0,
                    last_updated: 0
                });
            } else {
                container.innerHTML = this._renderPage(collection);
            }
        } catch (e) {
            container.innerHTML = `<div style="padding:30px; color:#f87171;"><i class="fa-solid fa-triangle-exclamation"></i> ${e.message}</div>`;
        }
    },

    _renderPage(coll) {
        const name = coll.name;
        const files = coll.total_files !== undefined ? coll.total_files : '—';
        const funcs = coll.total_functions !== undefined ? coll.total_functions : '—';
        const batches = coll.total_batches !== undefined ? coll.total_batches : '—';
        const updated = coll.last_updated ? (typeof window.formatDate === 'function' ? formatDate(coll.last_updated * 1000) : new Date(coll.last_updated * 1000).toLocaleString()) : '—';

        const filesUrl = `/collections/${encodeURIComponent(name)}/files`;
        const funcsUrl = `/collections/${encodeURIComponent(name)}/functions`;
        const batchesUrl = `/collections/${encodeURIComponent(name)}/batches`;

        return `
        <div style="flex:1; overflow-y:auto; padding:25px 30px; display:flex; flex-direction:column; gap:22px;">

            <!-- HEADER -->
            <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap;">
                <div>
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px; flex-wrap:wrap;">
                        <i class="fa-solid fa-layer-group" style="font-size:1.4rem; color:var(--accent);"></i>
                        <h1 style="margin:0; font-size:1.5rem; color:var(--text);">${name}</h1>
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                    <button onclick="window.collectionDetailClean('${name}', this)" style="background:rgba(168,85,247,0.12); border:1px solid rgba(168,85,247,0.35); color:#c084fc; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px;"><i class="fa-solid fa-broom"></i> Clean</button>
                    <button onclick="window.collectionDetailDelete('${name}', this)" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); color:#f87171; padding:8px 18px; border-radius:6px; font-size:0.82rem; font-weight:700; cursor:pointer; display:inline-flex; align-items:center; gap:8px; height:36px;"><i class="fa-solid fa-trash-can"></i> Delete</button>
                </div>
            </div>

            <!-- META ROW -->
            <div style="display:flex; gap:12px; flex-wrap:wrap;">
                <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 18px; font-size:0.8rem; display:flex; align-items:center; gap:8px;">
                    <i class="fa-solid fa-clock" style="color:var(--dim);"></i>
                    <span style="color:var(--dim);">Last Updated</span>
                    <span style="color:var(--text); font-weight:600;">${updated}</span>
                </div>
            </div>

            <!-- QUICK NAV CARDS -->
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap:16px;">
                <a href="${batchesUrl}" onclick="Nav.openPath('${batchesUrl}', event)" style="background:rgba(255,171,46,0.06); border:1px solid rgba(255,171,46,0.2); border-radius:10px; padding:20px; text-decoration:none; transition:background 0.2s, border-color 0.2s;" onmouseover="this.style.background='rgba(255,171,46,0.12)'; this.style.borderColor='rgba(255,171,46,0.4)';" onmouseout="this.style.background='rgba(255,171,46,0.06)'; this.style.borderColor='rgba(255,171,46,0.2)';">
                    <div style="font-size:2rem; font-weight:800; color:var(--accent);">${batches}</div>
                    <div style="font-size:0.8rem; color:var(--dim); margin-top:4px; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;"><i class="fa-solid fa-boxes-stacked"></i> Batches</div>
                </a>
                <a href="${filesUrl}" onclick="Nav.openPath('${filesUrl}', event)" style="background:rgba(96,165,250,0.06); border:1px solid rgba(96,165,250,0.2); border-radius:10px; padding:20px; text-decoration:none; transition:background 0.2s, border-color 0.2s;" onmouseover="this.style.background='rgba(96,165,250,0.12)'; this.style.borderColor='rgba(96,165,250,0.4)';" onmouseout="this.style.background='rgba(96,165,250,0.06)'; this.style.borderColor='rgba(96,165,250,0.2)';">
                    <div style="font-size:2rem; font-weight:800; color:#60a5fa;">${files}</div>
                    <div style="font-size:0.8rem; color:var(--dim); margin-top:4px; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;"><i class="fa-solid fa-file-code"></i> Files</div>
                </a>
                <a href="${funcsUrl}" onclick="Nav.openPath('${funcsUrl}', event)" style="background:rgba(167,139,250,0.06); border:1px solid rgba(167,139,250,0.2); border-radius:10px; padding:20px; text-decoration:none; transition:background 0.2s, border-color 0.2s;" onmouseover="this.style.background='rgba(167,139,250,0.12)'; this.style.borderColor='rgba(167,139,250,0.4)';" onmouseout="this.style.background='rgba(167,139,250,0.06)'; this.style.borderColor='rgba(167,139,250,0.2)';">
                    <div style="font-size:2rem; font-weight:800; color:#a78bfa;">${funcs}</div>
                    <div style="font-size:0.8rem; color:var(--dim); margin-top:4px; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;"><i class="fa-solid fa-code"></i> Functions</div>
                </a>
            </div>

            <!-- EXTRA NAV LINKS -->
            <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:10px;">
                ${[
                    { label: 'Fn Similarities', icon: 'fa-code-compare', path: `/collections/${encodeURIComponent(name)}/functions/similarities` },
                    { label: 'Fn Clusters', icon: 'fa-bullseye', path: `/collections/${encodeURIComponent(name)}/functions/clusters` },
                    { label: 'File Similarities', icon: 'fa-right-left', path: `/collections/${encodeURIComponent(name)}/files/similarities` },
                    { label: 'File Clusters', icon: 'fa-bullseye', path: `/collections/${encodeURIComponent(name)}/files/clusters` },
                    { label: 'Upload Binaries', icon: 'fa-upload', path: `/collections/${encodeURIComponent(name)}/upload` },
                ].map(nav => `
                    <a href="${nav.path}" onclick="Nav.openPath('${nav.path}', event)"
                       style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:8px 16px; text-decoration:none; font-size:0.8rem; font-weight:600; color:var(--text); display:inline-flex; align-items:center; gap:7px; transition: border-color 0.2s, box-shadow 0.2s;"
                       onmouseover="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 1px rgba(255,171,46,0.2)';"
                       onmouseout="this.style.borderColor='var(--border)'; this.style.boxShadow='none';">
                        <i class="fa-solid ${nav.icon}" style="color:var(--dim);"></i> ${nav.label}
                    </a>`).join('')}
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
