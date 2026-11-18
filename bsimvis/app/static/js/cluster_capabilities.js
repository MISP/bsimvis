/**
 * Capabilities of a binary cluster: what its N-way groups do, as MBC / ATT&CK ids the
 * model read from their functions (capability_service.py), plus each group's name.
 * Read from the stored set summary (GET /api/bin_sim/nway/semantics), so nothing is
 * built here. The overview card shows the top ids; the Capabilities tab lists them all,
 * then every group with its tier, name, ids and sentence.
 * Every value off a sample (names, sentences) goes through escapeHtml / escapeAttr.
 */
window.ClusterCapabilities = (function () {
    const TOP = 8;
    const num = v => Number(v || 0).toLocaleString();
    const TIER = { core: ['Core', '#16a34a'], common: ['Common', '#d97706'], rare: ['Rare', '#2563eb'] };

    function qs(v, c) {
        const q = new URLSearchParams({ cluster_uuid: c.cluster_uuid });
        if (v.params && v.params.pool) q.set('pool', v.params.pool); else q.set('collection', v.collection || '');
        return q;
    }

    async function fetchSet(v, c) {
        const res = await fetch(`/api/bin_sim/nway/semantics?${qs(v, c)}`);
        if (res.status === 404) return null;
        const body = await res.json();
        if (!res.ok) throw new Error(body.message || body.error || `HTTP ${res.status}`);
        return body;
    }

    const chip = (x, extra = '') => `<span class="nway-gchip cap" title="${escapeAttr(`${x.id} ${x.name || ''}${extra}`)}">${escapeHtml(x.id)} <span class="dim">${escapeHtml(x.name || '')}</span></span>`;

    // one line per id: its catalog name, a bar for the rows it covers, how many groups
    function idRows(set, limit) {
        const caps = (set.capabilities || []).slice(0, limit);
        const most = Math.max(1, ...caps.map(e => e.rows));
        return caps.map(e => `<div class="cov-row" style="grid-template-columns:auto 1fr auto;">
            <span class="nway-gchip cap" title="${escapeAttr(e.kind === 'mbc' ? 'MBC' : 'ATT&CK')}">${escapeHtml(e.id)}</span>
            <span style="min-width:0;"><span>${escapeHtml(e.name || '')}</span>
                <span class="cov-sbar" style="margin-top:3px;"><span style="flex:${e.rows}; background:var(--accent)"></span><span style="flex:${most - e.rows}; background:transparent"></span></span></span>
            <span class="dim" style="text-align:right" title="Functions in the groups that show it, and how many groups">${num(e.rows)} fn · ${e.groups.length}</span></div>`).join('');
    }

    function empty(set) {
        if (!set) return '<div class="dim" style="font-size:0.78rem;">Not read yet: open this cluster\'s Functions tab (or Build below) and the model names its groups, then their capabilities.</div>';
        const wait = set.capabilities_pending ? ` The model is still reading ${num(set.capabilities_pending)} named group${set.capabilities_pending > 1 ? 's' : ''}.` : '';
        return `<div class="dim" style="font-size:0.78rem;">No capability found in its groups yet.${wait}</div>`;
    }

    function buildBtn(v, c) {
        return `<button class="btn-code-action cov-btn" style="--c:var(--accent)" title="Queue the stored N-way summary, names and capabilities of this cluster (a big cluster is a long build)" onclick="ClusterCapabilities.build(event)"><i class="fa-solid fa-play"></i> Build</button>`;
    }

    // ---- the overview card: the top ids ----
    function card() {
        return `<div class="cov-card" id="cov-caps">
            <div class="cov-title"><button type="button" class="cov-tlink" title="Open Capabilities" onclick="ClusterDetailView.switchTab('capabilities')"><span class="cov-ico" style="--c:var(--danger, #dc2626)"><i class="fa-solid fa-bolt"></i></span><b>Capabilities</b></button>
                <i class="fa-solid fa-circle-info cov-info" title="What this cluster's code does, as MBC and ATT&CK ids the model read from each N-way group's functions (calls, strings, rule findings, code). Ranked by the functions of the groups that show them. Model output: check the functions before you rely on it."></i><span class="cov-big" id="cov-caps-n"></span></div>
            <div id="cov-caps-body" style="display:flex; flex-direction:column; gap:6px;"><div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> Reading the stored summary…</div></div>
            <div class="cov-foot"><button class="btn-code-action cov-btn" style="--c:var(--danger, #dc2626)" onclick="ClusterDetailView.switchTab('capabilities')"><i class="fa-solid fa-bolt"></i> All capabilities</button></div>
        </div>`;
    }

    async function loadCard(el, v, c) {
        const body = el.querySelector('#cov-caps-body');
        if (!body) return;
        let set;
        try { set = await fetchSet(v, c); } catch (e) { body.innerHTML = `<div class="dim">${escapeHtml(e.message)}</div>`; return; }
        if (v.selectedClusterUuid !== c.cluster_uuid || !body.isConnected) return;
        const n = (set && set.capabilities || []).length;
        el.querySelector('#cov-caps-n').textContent = n ? num(n) : '';
        body.innerHTML = n ? idRows(set, TOP) : empty(set);
    }

    // ---- the tab: every id, then every group ----
    function groupRow(g) {
        const t = TIER[g.tier] || null;
        const ids = (g.caps || []).flatMap(cp => [...(cp.mbc || []), ...(cp.attack || [])].map(x => chip(x)));
        const text = (g.caps || []).map(cp => cp.text).filter(Boolean).join(' ');
        return `<tr style="border-bottom:1px solid var(--border);">
            <td style="padding:6px 10px; white-space:nowrap;">${t ? `<span style="color:${t[1]}">${t[0]}</span>` : '<span class="dim">–</span>'}</td>
            <td style="padding:6px 10px;"><b>${escapeHtml(g.name || g.label || '')}</b>${g.name_source === 'user' ? ' <i class="fa-solid fa-pen dim" title="Named by an analyst"></i>' : ''}<div class="dim" style="font-size:0.72rem;">${escapeHtml(g.kind || '')}${(g.tags || []).length ? ` · ${escapeHtml(g.tags.slice(0, 3).join(', '))}` : ''}</div></td>
            <td style="padding:6px 10px; text-align:right; white-space:nowrap;">${num(g.rows)} fn<div class="dim" style="font-size:0.72rem;">${num(g.files)} files</div></td>
            <td style="padding:6px 10px;"><div style="display:flex; flex-wrap:wrap; gap:4px;">${ids.join('') || '<span class="dim">–</span>'}</div>${text ? `<div class="dim" style="font-size:0.75rem; margin-top:4px;">${escapeHtml(text)}</div>` : ''}</td></tr>`;
    }

    async function renderTab(el, v) {
        const c = v.clusterMapByUuid[v.selectedClusterUuid];
        if (!el || !c) return;
        if (window.ClusterOverview) window.ClusterOverview.ensureCss();
        el.innerHTML = '<div class="cov-wait" style="padding:20px;"><i class="fa-solid fa-spinner fa-spin"></i> Reading the stored summary…</div>';
        let set;
        try { set = await fetchSet(v, c); } catch (e) { el.innerHTML = `<div class="dim" style="padding:20px;">${escapeHtml(e.message)}</div>`; return; }
        if (v.selectedClusterUuid !== c.cluster_uuid || !el.isConnected) return;
        const groups = ((set && set.groups) || []).filter(g => g.kind !== 'tier');
        const onlyCaps = (set && set.capabilities || []).length > 0;
        const at = set && set.saved_at ? new Date(set.saved_at).toISOString().slice(0, 16).replace('T', ' ') : '';
        el.innerHTML = `<div class="cov" style="padding:4px 2px 20px;">
            <div class="cov-card">
                <div class="cov-title"><span class="cov-ico" style="--c:var(--danger, #dc2626)"><i class="fa-solid fa-bolt"></i></span><b>Capabilities</b>
                    <i class="fa-solid fa-circle-info cov-info" title="MBC and ATT&CK ids the model read from each N-way group's functions, ranked by the functions of the groups that show them. Big groups get MBC behaviours and ATT&CK techniques, small ones MBC micro-behaviours. Model output: check the functions before you rely on it."></i>
                    <span class="dim" style="margin-left:auto; font-size:0.75rem;">${set ? `${num(set.groups_total || groups.length)} groups${at ? ` · saved ${escapeHtml(at)} UTC` : ''}` : ''}</span></div>
                <div style="display:flex; flex-direction:column; gap:6px;">${onlyCaps ? idRows(set, 1000) : empty(set)}</div>
                <div class="cov-foot">${buildBtn(v, c)}<button class="btn-code-action cov-btn" style="--c:var(--accent)" onclick="ClusterDetailView.switchTab('functions')"><i class="fa-solid fa-table"></i> N-way table</button></div>
            </div>
            ${groups.length ? `<div class="cov-card" style="margin-top:14px;">
                <div class="cov-title"><span class="cov-ico" style="--c:var(--accent)"><i class="fa-solid fa-layer-group"></i></span><b>Groups</b>
                    <i class="fa-solid fa-circle-info cov-info" title="The N-way groups of this cluster (lanes, regions, bands), biggest first, with the name the model or an analyst gave them and their capabilities. Rename a group with the pencil in the N-way table."></i></div>
                <div style="overflow:auto;"><table style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                    <thead><tr style="border-bottom:1px solid var(--border); color:var(--dim); text-align:left;"><th style="padding:6px 10px;">Tier</th><th style="padding:6px 10px;">Group</th><th style="padding:6px 10px; text-align:right;">Size</th><th style="padding:6px 10px;">Capabilities</th></tr></thead>
                    <tbody>${groups.map(groupRow).join('')}</tbody></table></div>
            </div>` : ''}
        </div>`;
    }

    async function build(ev) {
        const v = window.ClusterDetailView;
        const c = v && v.clusterMapByUuid[v.selectedClusterUuid];
        if (!c) return;
        const btnEl = ev && ev.currentTarget;
        if (btnEl) btnEl.disabled = true;
        const body = { cluster_uuids: [c.cluster_uuid], axis: v.axis, force: true };
        if (v.params && v.params.pool) body.pool = v.params.pool; else body.collection = v.collection;
        try {
            const res = await fetch('/api/bin_sim/nway/semantics/build', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            const out = await res.json();
            if (!res.ok) throw new Error(out.message || out.error || `HTTP ${res.status}`);
            if (window.showToast) window.showToast('Queued: names and capabilities appear here when the job is done (see Jobs).', 'success');
        } catch (e) {
            if (window.showToast) window.showToast(`Build failed: ${e.message}`, 'error');
            if (btnEl) btnEl.disabled = false;
        }
    }

    return { card, loadCard, renderTab, build };
})();
