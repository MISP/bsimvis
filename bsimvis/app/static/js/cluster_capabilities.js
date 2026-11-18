/**
 * Capabilities of a binary cluster (experimental, plan G).
 *
 * Tab layout (two panes): left, the cluster's stored N-way groups by tier with
 * checkboxes and a Suggest bar; right, the model's suggestions for them (cached in
 * the queue Redis, /api/capability/suggest) and the capabilities the analyst
 * accepted (/api/capability/list). A suggestion expands in place into the shared
 * editor (CapEditor) and is accepted whole; several are accepted at once with the
 * checkboxes. `suggestPane` is the same right pane, reused by the N-way page's
 * drawer. Every value off a sample goes through escapeHtml / escapeAttr.
 */
window.ClusterCapabilities = (function () {
    const TOP = 8;
    const num = v => Number(v || 0).toLocaleString();
    const TIER = { core: ['Core', '#16a34a'], common: ['Common', '#d97706'], rare: ['Rare', '#2563eb'] };
    const TIER_ORDER = { core: 0, common: 1, rare: 2 };

    function ctxOf(v, c) {
        const pool = v.params && v.params.pool;
        return { collection: pool ? null : (v.collection || ''), pool: pool || null, cluster_uuid: c.cluster_uuid };
    }
    function qs(ctx, extra = {}) {
        const q = new URLSearchParams({ cluster_uuid: ctx.cluster_uuid, ...extra });
        if (ctx.pool) q.set('pool', ctx.pool); else q.set('collection', ctx.collection || '');
        return q;
    }
    async function getJson(url) {
        const res = await fetch(url);
        const body = await res.json().catch(() => ({}));
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(body.message || body.error || `HTTP ${res.status}`);
        return body;
    }
    const fetchSet = ctx => getJson(`/api/bin_sim/nway/semantics?${qs(ctx)}`);
    const fetchAccepted = ctx => getJson(`/api/capability/list?${qs(ctx)}`);
    const fetchSugg = (ctx, groups) => getJson(`/api/capability/suggestions?${qs(ctx, groups && groups.length ? { groups: groups.join(',') } : {})}`);

    async function postSuggest(ctx, groups, refresh) {
        const body = { cluster_uuid: ctx.cluster_uuid, groups, refresh: !!refresh };
        if (ctx.pool) body.pool = ctx.pool; else body.collection = ctx.collection;
        const res = await fetch('/api/capability/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const j = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(j.message || j.error || `HTTP ${res.status}`);
        return j;
    }

    const groupName = g => g.name || g.label || g.kind || 'group';
    const idChips = (c, cls = 'id') => [...(c.mbc || []), ...(c.attack || [])].map(i => `<span class="cap-chip ${cls}" title="${escapeAttr((c.names || {})[i] || (c.quotes || {})[i] || '')}">${escapeHtml(i)}</span>`).join('');

    function buildBtn() {
        return `<button class="btn-code-action cov-btn" style="--c:var(--accent)" title="Queue the stored N-way summary, names and suggestions of this cluster (a big cluster is a long build)" onclick="ClusterCapabilities.build(event)"><i class="fa-solid fa-play"></i> Build</button>`;
    }

    // ---- the overview card: accepted capabilities ----
    function card() {
        return `<div class="cov-card" id="cov-caps">
            <div class="cov-title"><button type="button" class="cov-tlink" title="Open Capabilities" onclick="ClusterDetailView.switchTab('capabilities')"><span class="cov-ico" style="--c:var(--danger, #dc2626)"><i class="fa-solid fa-bolt"></i></span><b>Capabilities</b></button>${CapEditor.EXP}
                <i class="fa-solid fa-circle-info cov-info" title="What this cluster's code does, as capabilities an analyst accepted: a name, MBC / ATT&CK ids and the functions, strings or rule hits that show it. The model only suggests them, in the Capabilities tab."></i><span class="cov-big" id="cov-caps-n"></span></div>
            <div id="cov-caps-body" style="display:flex; flex-direction:column; gap:6px;"><div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> Loading…</div></div>
            <div class="cov-foot"><button class="btn-code-action cov-btn" style="--c:var(--danger, #dc2626)" onclick="ClusterDetailView.switchTab('capabilities')"><i class="fa-solid fa-bolt"></i> All capabilities</button></div>
        </div>`;
    }

    async function loadCard(el, v, c) {
        CapEditor.ensureCss();
        const body = el.querySelector('#cov-caps-body');
        if (!body) return;
        let acc;
        try { acc = await fetchAccepted(ctxOf(v, c)); } catch (e) { body.innerHTML = `<div class="dim">${escapeHtml(e.message)}</div>`; return; }
        if (v.selectedClusterUuid !== c.cluster_uuid || !body.isConnected) return;
        const caps = (acc && acc.capabilities) || [];
        el.querySelector('#cov-caps-n').textContent = caps.length ? num(caps.length) : '';
        body.innerHTML = caps.length
            ? caps.slice(0, TOP).map(cp => `<div style="display:flex; flex-wrap:wrap; gap:4px; align-items:center;"><span class="cap-chip ok">${escapeHtml(cp.name)}</span>${idChips(cp)}<span class="dim" style="font-size:0.72rem; margin-left:auto;">${num(cp.files.length)} files</span></div>`).join('')
            : `<div class="dim" style="font-size:0.78rem;">${acc && acc.no_set ? 'No stored N-way set yet: open the Functions tab or Build it.' : 'None accepted yet. Select groups in the Capabilities tab and ask for suggestions, or build one from any function (right click, Capability).'}</div>`;
    }

    // ---- the right pane: suggestions, then accepted ----
    // ctx: {collection, pool, cluster_uuid}; opts.groups: stored set groups;
    // opts.only: gids to show suggestions for (empty = every cached one).
    function suggestPane(el, ctx, opts = {}) {
        CapEditor.ensureCss();
        const st = { sugg: [], pending: [], accepted: [], open: null, sel: new Set(), dismissed: new Set(), timer: null, asked: false };
        const byKey = {};
        (opts.groups || []).forEach(g => { byKey[g.gid] = g; });

        async function load(keys) {
            try {
                const [s, a] = await Promise.all([fetchSugg(ctx, keys || opts.only), fetchAccepted(ctx)]);
                if (!el.isConnected) return;
                st.accepted = (a && a.capabilities) || [];
                st.pending = (s && s.pending) || [];
                st.sugg = [];
                ((s && s.groups) || []).forEach(g => (g.suggestions || []).forEach((x, i) => {
                    const id = `${g.key}:${i}`;
                    if (st.dismissed.has(id)) return;
                    st.sugg.push({ id, group: g, draft: { ...x, collection: ctx.collection, pool: ctx.pool, names: {} } });
                }));
                draw();
                clearTimeout(st.timer);
                if (st.pending.length) st.timer = setTimeout(() => load(keys), 3000);
            } catch (e) { el.innerHTML = `<div class="dim" style="padding:10px;">${escapeHtml(e.message)}</div>`; }
        }

        function attachName(uid) { const a = st.accepted.find(c => c.uuid === uid); return a ? a.name : 'an accepted capability'; }

        function suggCard(s) {
            const d = s.draft, open = st.open === s.id;
            const title = d.attach_to && !d.name ? `+1 group → <span style="color:var(--success)">${escapeHtml(attachName(d.attach_to))}</span>` : escapeHtml(d.name || 'unnamed');
            return `<div class="cov-card" style="border-style:dashed; border-color:color-mix(in srgb, var(--info) 60%, transparent); padding:8px 10px;" data-sid="${escapeAttr(s.id)}">
                <div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center;">
                    <input type="checkbox" aria-label="Select this suggestion" data-pick ${st.sel.has(s.id) ? 'checked' : ''}>
                    <b style="flex:1; min-width:0;">${title}</b>${open ? '' : idChips(d, 'sug')}
                    <button type="button" class="cap-btn" data-edit>${open ? 'Done' : 'Edit'}</button>
                    <button type="button" class="cap-btn" data-dismiss title="Hide it (a later Suggest may bring it back)">Dismiss</button>
                    <button type="button" class="cap-btn ok" data-accept>Accept</button></div>
                <div class="dim" style="font-size:0.72rem;">from ${escapeHtml(groupName(s.group))} · ${num((d.evidence || []).length)} pointers${d.why ? ` · ${escapeHtml(d.why)}` : ''}${(d.unbacked || []).length ? ` · <span class="cap-warn">no pointer holds: ${escapeHtml(d.unbacked.join(', '))}</span>` : ''}</div>
                ${open ? '<div data-ed style="margin-top:6px;"></div>' : ''}</div>`;
        }

        function accCard(c) {
            return `<div class="cov-card" style="border-color:color-mix(in srgb, var(--success) 55%, transparent); padding:8px 10px;" data-uid="${escapeAttr(c.uuid)}">
                <div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center;"><b style="flex:1; min-width:0;">${escapeHtml(c.name)}</b>${idChips(c)}
                    <button type="button" class="cap-btn" data-tray title="Open it in the tray to edit or add functions">Edit</button>
                    <button type="button" class="cap-btn" data-rm>Remove</button></div>
                <div class="dim" style="font-size:0.72rem;">${num(c.evidence.length)} pointers · ${num(c.files.length)} files${c.description ? ` · ${escapeHtml(c.description)}` : ''}</div></div>`;
        }

        function draw() {
            const pend = st.pending.length ? `<span class="dim"><i class="fa-solid fa-spinner fa-spin"></i> asking ${num(st.pending.length)} group${st.pending.length > 1 ? 's' : ''}…</span>` : '';
            el.innerHTML = `<div style="display:flex; flex-direction:column; gap:8px;">
                <div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center;"><b>Suggested</b> <span class="dim">(${num(st.sugg.length)})</span> ${pend}
                    <span style="margin-left:auto; display:flex; gap:6px;"><button type="button" class="cap-btn" data-all>${st.sel.size === st.sugg.length && st.sugg.length ? 'Select none' : 'Select all'}</button>
                    <button type="button" class="cap-btn ok" data-acceptsel ${st.sel.size ? '' : 'disabled'}>Accept ${num(st.sel.size)} selected</button></span></div>
                ${st.sugg.map(suggCard).join('') || `<div class="dim" style="font-size:0.78rem;">${st.pending.length ? 'The model is reading the selected groups.' : 'No suggestion yet: select groups and press Suggest.'}</div>`}
                <div style="margin-top:6px;"><b>Accepted</b> <span class="dim">(${num(st.accepted.length)})</span></div>
                ${st.accepted.map(accCard).join('') || '<div class="dim" style="font-size:0.78rem;">None accepted in this cluster yet.</div>'}
            </div>`;
            const s = st.sugg.find(x => x.id === st.open);
            const slot = s && el.querySelector(`[data-sid="${CSS.escape(s.id)}"] [data-ed]`);
            if (slot) CapEditor.mount(slot, s.draft);
        }

        el.onclick = async e => {
            const b = e.target.closest('button,[data-pick]');
            if (!b) return;
            const sc = b.closest('[data-sid]'), ac = b.closest('[data-uid]');
            const s = sc && st.sugg.find(x => x.id === sc.dataset.sid);
            if (b.hasAttribute('data-pick') && s) { b.checked ? st.sel.add(s.id) : st.sel.delete(s.id); draw(); return; }
            if (b.hasAttribute('data-all')) { st.sel = st.sel.size === st.sugg.length ? new Set() : new Set(st.sugg.map(x => x.id)); draw(); return; }
            if (b.hasAttribute('data-edit') && s) { st.open = st.open === s.id ? null : s.id; draw(); return; }
            if (b.hasAttribute('data-dismiss') && s) { st.dismissed.add(s.id); st.sel.delete(s.id); st.sugg = st.sugg.filter(x => x !== s); draw(); return; }
            if (b.hasAttribute('data-accept') && s) {
                b.disabled = true;
                try { await CapEditor.accept(s.draft); st.dismissed.add(s.id); if (window.showToast) showToast(`Accepted: ${s.draft.name || attachName(s.draft.attach_to)}`, 'success'); load(); }
                catch (err) { b.disabled = false; if (window.showToast) showToast(`Not accepted: ${err.message}`, 'error'); }
                return;
            }
            if (b.hasAttribute('data-acceptsel')) {
                const list = st.sugg.filter(x => st.sel.has(x.id));
                b.disabled = true;
                try { await CapEditor.acceptMany(list.map(x => x.draft)); list.forEach(x => st.dismissed.add(x.id)); st.sel.clear(); if (window.showToast) showToast(`Accepted ${list.length} capabilities`, 'success'); load(); }
                catch (err) { b.disabled = false; if (window.showToast) showToast(`Not accepted: ${err.message}`, 'error'); }
                return;
            }
            const c = ac && st.accepted.find(x => x.uuid === ac.dataset.uid);
            if (b.hasAttribute('data-tray') && c) {
                CapTray.edit({ uuid: c.uuid, name: c.name, description: c.description || '', mbc: c.mbc, attack: c.attack, names: c.names, evidence: c.evidence }, ctx);
                return;
            }
            if (b.hasAttribute('data-rm') && c) {
                if (b.dataset.sure !== '1') { b.dataset.sure = '1'; b.textContent = 'Remove?'; return; }
                b.disabled = true;
                const q = ctx.pool ? `pool=${encodeURIComponent(ctx.pool)}` : `collection=${encodeURIComponent(ctx.collection)}`;
                const res = await fetch(`/api/capability/${encodeURIComponent(c.uuid)}?${q}`, { method: 'DELETE' });
                if (!res.ok && window.showToast) showToast(`Not removed: HTTP ${res.status}`, 'error');
                load();
            }
        };
        const onChanged = () => { if (!el.isConnected) { document.removeEventListener('capabilities:changed', onChanged); return; } load(); };
        document.addEventListener('capabilities:changed', onChanged);

        el.innerHTML = '<div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> Loading…</div>';
        load();
        return {
            reload: load,
            async suggest(gids, refresh) {
                await postSuggest(ctx, gids, refresh);
                opts.only = gids;
                load(gids);
            },
        };
    }

    // ---- the tab: groups | capabilities ----
    async function renderTab(el, v) {
        const c = v.clusterMapByUuid[v.selectedClusterUuid];
        if (!el || !c) return;
        if (window.ClusterOverview) window.ClusterOverview.ensureCss();
        CapEditor.ensureCss();
        const ctx = ctxOf(v, c);
        el.innerHTML = '<div class="cov-wait" style="padding:20px;"><i class="fa-solid fa-spinner fa-spin"></i> Reading the stored summary…</div>';
        let set;
        try { set = await fetchSet(ctx); } catch (e) { el.innerHTML = `<div class="dim" style="padding:20px;">${escapeHtml(e.message)}</div>`; return; }
        if (v.selectedClusterUuid !== c.cluster_uuid || !el.isConnected) return;
        const groups = ((set && set.groups) || []).filter(g => g.kind !== 'tier' && (g.anchors || []).length)
            .sort((a, b) => (TIER_ORDER[a.tier] ?? 3) - (TIER_ORDER[b.tier] ?? 3) || (b.rows || 0) - (a.rows || 0));
        const sel = new Set();
        el.innerHTML = `<div class="cov" style="padding:4px 2px 20px;">
            <div class="cov-title" style="margin-bottom:8px;"><span class="cov-ico" style="--c:var(--danger, #dc2626)"><i class="fa-solid fa-bolt"></i></span><b>Capabilities</b>${CapEditor.EXP}
                <i class="fa-solid fa-circle-info cov-info" title="Select N-way groups on the left and press Suggest: the model proposes capabilities with MBC / ATT&CK ids and the functions and strings that show them. Edit, then accept; nothing is kept until you accept it. Build a capability by hand from any function or string (right click, Capability)."></i>
                <span style="margin-left:auto; display:flex; gap:6px;">${buildBtn()}<button class="btn-code-action cov-btn" style="--c:var(--accent)" onclick="ClusterDetailView.switchTab('functions')"><i class="fa-solid fa-table"></i> N-way table</button></span></div>
            <div style="display:grid; grid-template-columns:minmax(0, 300px) minmax(0, 1fr); gap:12px;" class="cap-split">
                <div class="cov-card" style="padding:8px; display:flex; flex-direction:column; gap:4px; align-self:start;">
                    <input id="cap-gq" aria-label="Search groups" placeholder="Search ${num(groups.length)} groups" style="background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:4px 7px;">
                    <div id="cap-glist" style="display:flex; flex-direction:column; gap:1px; max-height:60vh; overflow:auto;"></div>
                    <div style="display:flex; gap:6px; align-items:center; position:sticky; bottom:0; padding-top:6px; border-top:1px solid var(--border); background:var(--card-bg);">
                        <span class="dim" id="cap-gn" style="flex:1; font-size:0.75rem;">0 selected</span>
                        <button type="button" class="cap-btn" id="cap-resug" disabled title="Ask the model again for the selected groups">Re-suggest</button>
                        <button type="button" class="cap-btn pri" id="cap-sug" disabled>Suggest</button></div>
                </div>
                <div id="cap-right"></div>
            </div>
            ${set ? '' : '<div class="dim" style="margin-top:8px;">No stored N-way set for this cluster yet: open its Functions tab once, or Build it.</div>'}
        </div>`;
        if (!el.querySelector('#cap-split-css')) el.insertAdjacentHTML('beforeend', '<style id="cap-split-css">@media (max-width: 760px) { .cap-split { grid-template-columns: 1fr !important; } }</style>');
        const list = el.querySelector('#cap-glist');
        const drawGroups = () => {
            const q = el.querySelector('#cap-gq').value.toLowerCase();
            let tier = null;
            list.innerHTML = groups.filter(g => !q || groupName(g).toLowerCase().includes(q)).map(g => {
                const t = TIER[g.tier];
                const head = g.tier !== tier ? `<div class="dim" style="font-size:0.66rem; text-transform:uppercase; letter-spacing:.05em; margin-top:4px; color:${t ? t[1] : 'var(--dim)'}">${t ? t[0] : 'Other'}</div>` : '';
                tier = g.tier;
                return `${head}<label style="display:flex; gap:6px; align-items:center; padding:3px 5px; border-radius:3px; cursor:pointer; box-shadow:inset 3px 0 0 ${t ? t[1] : 'var(--border)'}; ${sel.has(g.gid) ? 'background:color-mix(in srgb, var(--accent) 12%, transparent);' : ''}">
                    <input type="checkbox" data-g="${escapeAttr(g.gid)}" ${sel.has(g.gid) ? 'checked' : ''}>
                    <span style="flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${escapeAttr(groupName(g))}">${escapeHtml(groupName(g))}</span>
                    <span class="dim" style="font-size:0.7rem;">${num(g.rows)}</span></label>`;
            }).join('') || '<div class="dim" style="font-size:0.78rem;">No group.</div>';
            el.querySelector('#cap-gn').textContent = `${num(sel.size)} selected`;
            el.querySelector('#cap-sug').disabled = el.querySelector('#cap-resug').disabled = !sel.size;
        };
        list.onchange = e => { const g = e.target.dataset.g; if (!g) return; e.target.checked ? sel.add(g) : sel.delete(g); drawGroups(); };
        el.querySelector('#cap-gq').oninput = drawGroups;
        drawGroups();
        const pane = set ? suggestPane(el.querySelector('#cap-right'), ctx, { groups }) : null;
        const ask = refresh => async ev => {
            const b = ev.currentTarget;
            b.disabled = true;
            try { await pane.suggest([...sel], refresh); } catch (e) { if (window.showToast) showToast(`Suggest failed: ${e.message}`, 'error'); }
            b.disabled = !sel.size;
        };
        el.querySelector('#cap-sug').onclick = ask(false);
        el.querySelector('#cap-resug').onclick = ask(true);
    }

    async function build(ev) {
        const v = window.ClusterDetailView;
        const c = v && v.clusterMapByUuid[v.selectedClusterUuid];
        if (!c) return;
        const btnEl = ev && ev.currentTarget;
        if (btnEl) btnEl.disabled = true;
        const body = { cluster_uuids: [c.cluster_uuid], axis: v.axis, force: true, big: true };
        if (v.params && v.params.pool) body.pool = v.params.pool; else body.collection = v.collection;
        try {
            const res = await fetch('/api/bin_sim/nway/semantics/build', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            const out = await res.json();
            if (!res.ok) throw new Error(out.message || out.error || `HTTP ${res.status}`);
            if (window.showToast) window.showToast('Queued: the groups appear here when the job is done (see Jobs).', 'success');
        } catch (e) {
            if (window.showToast) window.showToast(`Build failed: ${e.message}`, 'error');
            if (btnEl) btnEl.disabled = false;
        }
    }

    return { card, loadCard, renderTab, build, suggestPane, fetchSet, fetchAccepted, fetchSugg };
})();
