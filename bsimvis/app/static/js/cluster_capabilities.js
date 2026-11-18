/**
 * Capabilities of a binary cluster (experimental, plan G).
 *
 * Tab layout (two panes): left, the cluster's stored N-way groups by tier with
 * checkboxes and a Suggest bar; right, the model's suggestions for them (cached in
 * the queue Redis, /api/capability/suggest) and the capabilities the analyst
 * accepted (/api/capability/list). The right pane lists every cached suggestion on
 * the cluster's files, whichever N-way set or view asked for it (selecting groups
 * never hides any), and the naming / suggesting / Build steps under way. A
 * suggestion an accepted capability already
 * covers (same group, same name or attach target) is not offered again; the same
 * suggestion from several groups is one card. Cards expand in place into the shared
 * editor (CapEditor) and are accepted whole, or several at once with the
 * checkboxes. `suggestPane` is the same right pane, reused by the N-way drawer.
 * Every value off a sample goes through escapeHtml / escapeAttr.
 */
window.ClusterCapabilities = (function () {
    const TOP = 8;
    const num = v => Number(v || 0).toLocaleString();
    const TIER = { core: ['Core', '#16a34a'], common: ['Common', '#d97706'], rare: ['Rare', '#2563eb'] };
    const TIER_ORDER = { core: 0, common: 1, rare: 2 };
    const norm = s => (String(s || '').toLowerCase().match(/[a-z0-9]+/g) || []).join(' ');

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

    // groups: stored-set gids, or whole groups from an N-way view; ask=false only reads the cache
    async function postSuggest(ctx, groups, refresh, ask = true) {
        const body = { cluster_uuid: ctx.cluster_uuid, groups, refresh: !!refresh, ask };
        if (ctx.pool) body.pool = ctx.pool; else body.collection = ctx.collection;
        const res = await fetch('/api/capability/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const j = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(j.message || j.error || `HTTP ${res.status}`);
        return j;
    }

    // One card per suggestion: its attach target, else its name; one whose target was
    // removed and that has no name is its group's own card.
    const suggKey = (x, g) => x.attach_to ? `a:${x.attach_to}` : x.name ? `n:${norm(x.name)}` : x.attach_gone ? `gone:${g.key}` : '';

    // Per group id, and per `n:<name id>` (the key that survives a rebuild): accepted
    // capability names and the suggestions no accepted one covers.
    function groupStats(sugg) {
        const out = {};
        ((sugg && sugg.groups) || []).forEach(g => {
            const open = new Set((g.suggestions || []).filter(s => !s.accepted).map(s => suggKey(s, g)).filter(Boolean));
            const st = { acc: (g.accepted || []).map(c => c.name), sugg: open.size };
            if (!g.earlier) out[g.gid] = st;
            if (!g.name_id) return;
            const o = out[`n:${g.name_id}`];
            out[`n:${g.name_id}`] = o ? { acc: [...new Set([...o.acc, ...st.acc])], sugg: o.sugg + st.sugg } : st;
        });
        return out;
    }

    // What runs in the background for this cluster: Build jobs, group names, suggestions.
    function runLine(run) {
        if (!run || !run.length) return '';
        const by = w => run.filter(r => r.what === w);
        const names = (list, n = 3) => list.slice(0, n).map(r => escapeHtml(r.label || 'group')).join(', ') + (list.length > n ? ` +${list.length - n}` : '');
        const parts = [];
        by('build').forEach(r => parts.push(`<a href="/jobs" title="Job ${escapeAttr(r.job || '')}">${escapeHtml(r.label || 'Build')}</a>`));
        const nm = by('name'), sg = by('suggest');
        if (nm.length) parts.push(`naming ${num(nm.length)} group${nm.length > 1 ? 's' : ''} <span class="dim">(${names(nm)})</span>`);
        if (sg.length) parts.push(`suggesting for ${num(sg.length)} group${sg.length > 1 ? 's' : ''} <span class="dim">(${names(sg)})</span>`);
        return `<div class="cap-sec" role="status" style="font-size:0.75rem; gap:6px; flex-wrap:wrap;"><i class="fa-solid fa-spinner fa-spin"></i><b>In the background:</b> ${parts.join(' · ')}</div>`;
    }

    const groupName = g => g.name || g.label || g.kind || 'group';
    const idChips = (c, cls = 'id') => [...(c.mbc || []), ...(c.attack || [])].map(i => `<span class="cap-chip ${cls}" title="${escapeAttr((c.names || {})[i] || (c.quotes || {})[i] || '')}">${escapeHtml(i)}</span>`).join('');

    // Groups Suggest left out (not in the default build, or stored before its evidence).
    function warnSkipped(j) {
        const sk = (j && j.skipped) || [];
        if (!sk.length || !window.showToast) return;
        const names = sk.slice(0, 3).map(x => `${x.group}: ${x.reason}`).join('; ');
        showToast(`${sk.length} group${sk.length > 1 ? 's' : ''} not asked. ${names}${sk.length > 3 ? ' …' : ''}`, 'warning');
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
            ? caps.slice(0, TOP).map(cp => `<div class="cap-sec"><span class="cap-chip ok" style="white-space:normal;">${escapeHtml(cp.name)}</span><span class="ids" style="display:flex; flex-wrap:wrap; gap:4px;">${idChips(cp)}</span><span class="dim" style="font-size:0.72rem; margin-left:auto;">${num(cp.files.length)} files</span></div>`).join('')
            : `<div class="dim" style="font-size:0.78rem;">${acc && acc.no_set ? 'No stored N-way set yet: open the Capabilities or Functions tab.' : 'None accepted yet. Select groups in the Capabilities tab and ask for suggestions, or build one from any function (right click, Capability).'}</div>`;
    }

    // ---- the right pane: suggestions, then accepted ----
    // ctx: {collection, pool, cluster_uuid}; opts.groups: show only these groups (the
    // N-way drawer: stored gids or whole groups), asked first when opts.ask;
    // opts.onGroups(stats): per-gid {acc, sugg} after each load; opts.onBuilt(): a
    // Build of this cluster just finished.
    function suggestPane(el, ctx, opts = {}) {
        CapEditor.ensureCss();
        const st = { cards: [], pending: [], running: [], accepted: [], open: null, openAcc: null, sel: new Set(), dismissed: new Set(), timer: null, only: null };

        async function load() {
            try {
                const [s, a] = await Promise.all([fetchSugg(ctx), fetchAccepted(ctx)]);
                if (!el.isConnected) return;
                const only = st.only;
                const shown = ((s && s.groups) || []).filter(g => !only || only.has(g.key));
                const accIds = new Set(shown.flatMap(g => (g.accepted || []).map(c => c.uuid)));
                st.accepted = ((a && a.capabilities) || []).filter(c => !only || accIds.has(c.uuid));
                st.pending = ((s && s.pending) || []).filter(k => shown.some(g => g.key === k));
                const built = st.running.some(r => r.what === 'build');
                st.running = (s && s.running) || [];
                if (built && !st.running.some(r => r.what === 'build') && opts.onBuilt) opts.onBuilt();
                // one card per suggestion, merged over the groups that gave it
                const byKey = new Map();
                shown.forEach(g => (g.suggestions || []).forEach(x => {
                    if (x.accepted) return;
                    const key = suggKey(x, g);
                    if (!key || st.dismissed.has(key)) return;
                    let c = byKey.get(key);
                    if (!c) {
                        const keep = st.cards.find(o => o.id === key);
                        // the suggestion's sentence becomes the capability's description
                        c = keep && st.open === key ? keep : { id: key, groups: [], draft: { name: x.name, attach_to: x.attach_to, attach_gone: !!x.attach_gone, description: x.why || '', mbc: [], attack: [], evidence: [], unbacked: [], why: x.why, quotes: {}, collection: ctx.collection, pool: ctx.pool, cluster_uuid: ctx.cluster_uuid, names: {} } };
                        c.groups = [];
                        byKey.set(key, c);
                    }
                    c.groups.push(g);
                    if (c === st.cards.find(o => o.id === key && st.open === key)) return;
                    const d = c.draft;
                    d.mbc = [...new Set([...d.mbc, ...(x.mbc || [])])];
                    d.attack = [...new Set([...d.attack, ...(x.attack || [])])];
                    d.unbacked = [...new Set([...d.unbacked, ...(x.unbacked || [])])];
                    d.quotes = { ...d.quotes, ...(x.quotes || {}) };
                    if (x.why && !d.description.includes(x.why)) d.description = [d.description, x.why].filter(Boolean).join(' ').slice(0, 1000);
                    const seen = new Set(d.evidence.map(e => e.id));
                    (x.evidence || []).forEach(e => { if (!seen.has(e.id)) { d.evidence.push(e); seen.add(e.id); } });
                }));
                st.cards = [...byKey.values()];
                st.sel = new Set([...st.sel].filter(id => byKey.has(id)));
                if (opts.onGroups) opts.onGroups(groupStats(s));
                draw();
                clearTimeout(st.timer);
                // a queued Build can wait long: poll it slowly
                const busy = st.pending.length || st.running.some(r => r.what !== 'build');
                if (busy || st.running.length) st.timer = setTimeout(load, busy ? 3000 : 15000);
            } catch (e) { el.innerHTML = `<div class="dim" style="padding:10px;">${escapeHtml(e.message)}</div>`; }
        }

        function attachName(uid) { const a = st.accepted.find(c => c.uuid === uid); return a ? a.name : 'an accepted capability'; }

        function suggCard(s) {
            const d = s.draft, open = st.open === s.id;
            const title = d.attach_to && !d.name ? `Add to <span style="color:var(--success)">${escapeHtml(attachName(d.attach_to))}</span>` : escapeHtml(d.name || 'unnamed');
            const from = s.groups.map(g => groupName(g) + (g.earlier ? ' (earlier N-way)' : ''));
            return `<div class="cap-card sug" data-sid="${escapeAttr(s.id)}">
                <div class="hd"><input type="checkbox" aria-label="Select this suggestion" data-pick ${st.sel.has(s.id) ? 'checked' : ''}>
                    <span class="ttl">${title}</span>
                    <span class="acts"><button type="button" class="cap-btn" data-edit aria-expanded="${open}">${open ? 'Close' : 'Edit'}</button>
                    <button type="button" class="cap-btn" data-dismiss title="Hide it (it can come back on a later Suggest or reload)">Dismiss</button>
                    <button type="button" class="cap-btn ok" data-accept>Accept</button></span></div>
                ${open ? '' : `<div class="ids">${idChips(d, 'sug')}</div>`}
                <div class="meta">from ${escapeHtml(from.slice(0, 3).join(', '))}${from.length > 3 ? ` +${from.length - 3}` : ''} · ${num(d.evidence.length)} pointers${d.why ? ` · ${escapeHtml(d.why)}` : ''}${d.unbacked.length ? ` · <span class="cap-warn">no pointer holds: ${escapeHtml(d.unbacked.join(', '))}</span>` : ''}${d.attach_gone ? ` · <span class="cap-warn">the capability it added to was removed: ${d.name ? 'accepting creates it again' : 'give it a name (Edit) to create one, or add it to another from the tray'}</span>` : ''}</div>
                ${open ? '<div data-ed></div>' : ''}</div>`;
        }

        function accCard(c) {
            const open = st.openAcc && st.openAcc.uuid === c.uuid;
            return `<div class="cap-card acc" data-uid="${escapeAttr(c.uuid)}">
                <div class="hd"><i class="fa-solid fa-circle-check" style="color:var(--success); margin-top:3px;" title="Verified: accepted by an analyst"></i>
                    <span class="ttl">${escapeHtml(c.name)}</span>
                    <span class="acts">${open ? '<button type="button" class="cap-btn ok" data-save>Save</button>' : ''}
                    <button type="button" class="cap-btn" data-aedit aria-expanded="${!!open}">${open ? 'Close' : 'Edit'}</button>
                    <button type="button" class="cap-btn" data-tray title="Open it in the tray to add functions from other views">Tray</button>
                    <button type="button" class="cap-btn" data-rm>Remove</button></span></div>
                ${open ? '' : `<div class="ids">${idChips(c)}</div>`}
                <div class="meta">${num(c.evidence.length)} pointers · ${num(c.files.length)} files${c.description ? ` · ${escapeHtml(c.description)}` : ''}</div>
                ${open ? '<div data-aed></div>' : ''}</div>`;
        }

        function draw() {
            const pend = st.pending.length ? `<span class="dim"><i class="fa-solid fa-spinner fa-spin"></i> asking ${num(st.pending.length)} group${st.pending.length > 1 ? 's' : ''}…</span>` : '';
            el.innerHTML = `<div style="display:flex; flex-direction:column; gap:8px; min-width:0;">
                ${runLine(st.running)}
                <div class="cap-sec"><b>Suggested</b> <span class="dim">(${num(st.cards.length)})</span> ${pend}
                    <span style="margin-left:auto; display:flex; gap:6px; flex-wrap:wrap;"><button type="button" class="cap-btn" data-all>${st.sel.size === st.cards.length && st.cards.length ? 'Select none' : 'Select all'}</button>
                    <button type="button" class="cap-btn ok" data-acceptsel ${st.sel.size ? '' : 'disabled'}>Accept ${num(st.sel.size)} selected</button></span></div>
                ${st.cards.map(suggCard).join('') || `<div class="dim" style="font-size:0.78rem;">${st.pending.length ? 'The model is reading the selected groups.' : 'No new suggestion: select groups and press Suggest.'}</div>`}
                <div class="cap-sec" style="margin-top:6px;"><b>Verified</b> <span class="dim">(${num(st.accepted.length)})</span></div>
                ${st.accepted.map(accCard).join('') || `<div class="dim" style="font-size:0.78rem;">None accepted ${st.only ? 'for these groups' : 'in this cluster'} yet.</div>`}
            </div>`;
            const s = st.cards.find(x => x.id === st.open);
            const slot = s && el.querySelector(`[data-sid="${CSS.escape(s.id)}"] [data-ed]`);
            if (slot) CapEditor.mount(slot, s.draft);
            const aslot = st.openAcc && el.querySelector(`[data-uid="${CSS.escape(st.openAcc.uuid)}"] [data-aed]`);
            if (aslot) CapEditor.mount(aslot, st.openAcc);
        }

        el.onclick = async e => {
            const b = e.target.closest('button,[data-pick]');
            if (!b || !el.contains(b) || b.closest('[data-ed],[data-aed]')) return;
            const sc = b.closest('[data-sid]'), ac = b.closest('[data-uid]');
            const s = sc && st.cards.find(x => x.id === sc.dataset.sid);
            if (b.hasAttribute('data-pick') && s) { b.checked ? st.sel.add(s.id) : st.sel.delete(s.id); draw(); return; }
            if (b.hasAttribute('data-all')) { st.sel = st.sel.size === st.cards.length ? new Set() : new Set(st.cards.map(x => x.id)); draw(); return; }
            if (b.hasAttribute('data-edit') && s) { st.open = st.open === s.id ? null : s.id; draw(); return; }
            if (b.hasAttribute('data-dismiss') && s) { st.dismissed.add(s.id); st.sel.delete(s.id); st.cards = st.cards.filter(x => x !== s); draw(); return; }
            if (b.hasAttribute('data-accept') && s) {
                b.disabled = true;
                try { await CapEditor.accept(s.draft); if (st.open === s.id) st.open = null; if (window.showToast) showToast(`Accepted: ${s.draft.name || attachName(s.draft.attach_to)}`, 'success'); load(); }
                catch (err) { b.disabled = false; if (window.showToast) showToast(`Not accepted: ${err.message}`, 'error'); }
                return;
            }
            if (b.hasAttribute('data-acceptsel')) {
                const list = st.cards.filter(x => st.sel.has(x.id));
                b.disabled = true;
                try { await CapEditor.acceptMany(list.map(x => x.draft)); st.sel.clear(); st.open = null; if (window.showToast) showToast(`Accepted ${list.length} capabilities`, 'success'); load(); }
                catch (err) { b.disabled = false; if (window.showToast) showToast(`Not accepted: ${err.message}`, 'error'); }
                return;
            }
            const c = ac && st.accepted.find(x => x.uuid === ac.dataset.uid);
            if (!c) return;
            const asDraft = () => ({ uuid: c.uuid, name: c.name, description: c.description || '', mbc: [...(c.mbc || [])], attack: [...(c.attack || [])], names: { ...(c.names || {}) }, evidence: JSON.parse(JSON.stringify(c.evidence || [])), collection: ctx.collection, pool: ctx.pool, cluster_uuid: ctx.cluster_uuid });
            if (b.hasAttribute('data-aedit')) { st.openAcc = st.openAcc && st.openAcc.uuid === c.uuid ? null : asDraft(); draw(); return; }
            if (b.hasAttribute('data-save') && st.openAcc) {
                b.disabled = true;
                try { await CapEditor.accept(st.openAcc, true); st.openAcc = null; if (window.showToast) showToast(`Saved: ${c.name}`, 'success'); load(); }
                catch (err) { b.disabled = false; if (window.showToast) showToast(`Not saved: ${err.message}`, 'error'); }
                return;
            }
            if (b.hasAttribute('data-tray')) { CapTray.edit(asDraft(), ctx); return; }
            if (b.hasAttribute('data-rm')) {
                if (b.dataset.sure !== '1') { b.dataset.sure = '1'; b.textContent = 'Remove?'; return; }
                b.disabled = true;
                const q = ctx.pool ? `pool=${encodeURIComponent(ctx.pool)}` : `collection=${encodeURIComponent(ctx.collection)}`;
                const res = await fetch(`/api/capability/${encodeURIComponent(c.uuid)}?${q}`, { method: 'DELETE' });
                if (!res.ok && window.showToast) showToast(`Not removed: HTTP ${res.status}`, 'error');
                document.dispatchEvent(new CustomEvent('capabilities:changed', { detail: ctx }));
            }
        };
        const onChanged = () => { if (!el.isConnected) { document.removeEventListener('capabilities:changed', onChanged); return; } load(); };
        document.addEventListener('capabilities:changed', onChanged);

        el.innerHTML = '<div class="cov-wait"><i class="fa-solid fa-spinner fa-spin"></i> Loading…</div>';
        if (opts.groups) {
            // the drawer: its groups' cache keys, then only those
            postSuggest(ctx, opts.groups, false, !!opts.ask)
                .then(j => { warnSkipped(j); st.only = new Set(((j && j.groups) || []).map(g => g.key)); load(); })
                .catch(e => { el.innerHTML = `<div class="dim" style="padding:10px;">${escapeHtml(e.message)}</div>`; });
        } else load();
        return {
            reload: load,
            // the tab: ask for these groups; the pane keeps listing everything
            async suggest(gids, refresh) {
                warnSkipped(await postSuggest(ctx, gids, refresh));
                load();
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
        let stats = {};
        el.innerHTML = `<div class="cov" style="padding:4px 2px 20px;">
            <div class="cov-title" style="margin-bottom:8px;"><span class="cov-ico" style="--c:var(--danger, #dc2626)"><i class="fa-solid fa-bolt"></i></span><b>Capabilities</b>${CapEditor.EXP}
                <i class="fa-solid fa-circle-info cov-info" title="Select N-way groups on the left and press Suggest: the model proposes capabilities with MBC / ATT&CK ids and the functions and strings that show it, read from the cluster's default N-way build (the same functions whichever button asked). Edit, then accept; nothing is kept until you accept it. Build a capability by hand from any function or string (right click, Capability)."></i>
                <span class="cov-acts"><button class="btn-code-action cov-btn" style="--c:var(--accent)" onclick="ClusterDetailView.switchTab('functions')"><i class="fa-solid fa-table"></i> N-way table</button></span></div>
            <div id="cap-build-host" style="margin-bottom:8px;"></div>
            <div class="cap-split" style="display:grid; grid-template-columns:minmax(0, 300px) minmax(0, 1fr); gap:12px; align-items:start;">
                <div class="cov-card" style="padding:8px; gap:4px;">
                    <input id="cap-gq" aria-label="Search groups" placeholder="Search ${num(groups.length)} groups" style="background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:4px 7px; min-width:0;">
                    <div id="cap-glist" style="display:flex; flex-direction:column; gap:1px; max-height:60vh; overflow:auto;"></div>
                    <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap; padding-top:6px; border-top:1px solid var(--border);">
                        <span class="dim" id="cap-gn" style="flex:1; font-size:0.75rem; min-width:70px;">0 selected</span>
                        <button type="button" class="cap-btn" id="cap-resug" disabled title="Ask the model again for the selected groups">Re-suggest</button>
                        <button type="button" class="cap-btn pri" id="cap-sug" disabled>Suggest</button></div>
                </div>
                <div id="cap-right" style="min-width:0;"></div>
            </div>
            ${set ? '' : '<div class="dim" style="margin-top:8px;">No stored N-way set for this cluster yet.</div>'}
        </div>`;
        if (!el.querySelector('#cap-split-css')) el.insertAdjacentHTML('beforeend', '<style id="cap-split-css">@media (max-width: 760px) { .cap-split { grid-template-columns: 1fr !important; } } .cap-gi { display:grid; grid-template-columns:auto minmax(0,1fr) auto; gap:6px; align-items:center; padding:3px 5px; border-radius:3px; cursor:pointer; } .cap-gi .nm { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; } .cap-gi .bd { display:flex; gap:3px; align-items:center; font-size:0.68rem; white-space:nowrap; }</style>');
        const list = el.querySelector('#cap-glist');
        const drawGroups = () => {
            const q = el.querySelector('#cap-gq').value.toLowerCase();
            let tier = null;
            list.innerHTML = groups.filter(g => !q || groupName(g).toLowerCase().includes(q)).map(g => {
                const t = TIER[g.tier], s = stats[g.gid] || { acc: [], sugg: 0 };
                const head = g.tier !== tier ? `<div class="dim" style="font-size:0.66rem; text-transform:uppercase; letter-spacing:.05em; margin-top:4px; color:${t ? t[1] : 'var(--dim)'}">${t ? t[0] : 'Other'}</div>` : '';
                tier = g.tier;
                return `${head}<label class="cap-gi" style="box-shadow:inset 3px 0 0 ${t ? t[1] : 'var(--border)'}; ${sel.has(g.gid) ? 'background:color-mix(in srgb, var(--accent) 12%, transparent);' : ''}">
                    <input type="checkbox" data-g="${escapeAttr(g.gid)}" ${sel.has(g.gid) ? 'checked' : ''}>
                    <span class="nm" title="${escapeAttr(groupName(g))}">${escapeHtml(groupName(g))}</span>
                    <span class="bd">${s.acc.length ? `<span class="cap-chip ok" title="${escapeAttr(`Verified: ${s.acc.join(', ')}`)}">✓${s.acc.length}</span>` : ''}${s.sugg ? `<span class="cap-chip sug" title="Suggestions to review">${s.sugg}</span>` : ''}<span class="dim">${num(g.rows)}</span></span></label>`;
            }).join('') || '<div class="dim" style="font-size:0.78rem;">No group.</div>';
            el.querySelector('#cap-gn').textContent = `${num(sel.size)} selected`;
            el.querySelector('#cap-sug').disabled = el.querySelector('#cap-resug').disabled = !sel.size;
        };
        list.onchange = e => { const g = e.target.dataset.g; if (!g) return; e.target.checked ? sel.add(g) : sel.delete(g); drawGroups(); };
        el.querySelector('#cap-gq').oninput = drawGroups;
        drawGroups();
        buildState(el.querySelector('#cap-build-host'), v, c);
        // mounted with no stored set too: it shows a queued Build
        const pane = suggestPane(el.querySelector('#cap-right'), ctx, {
            onGroups: s => { stats = s; if (list.isConnected) drawGroups(); },
            onBuilt: () => { if (el.isConnected && v.selectedClusterUuid === c.cluster_uuid) renderTab(el, v); },
        });
        const ask = refresh => async ev => {
            const b = ev.currentTarget;
            b.disabled = true;
            try { await pane.suggest([...sel], refresh); } catch (e) { if (window.showToast) showToast(`Suggest failed: ${e.message}`, 'error'); }
            b.disabled = !sel.size;
        };
        el.querySelector('#cap-sug').onclick = ask(false);
        el.querySelector('#cap-resug').onclick = ask(true);
    }

    // The cluster's default N-way table, as the N-way page shows it (NwayPanel.buildGate):
    // a large set not built yet offers its depths (Fast / Balanced / Deep); a built
    // one says its depth and offers Rebuild deeper; a small one needs nothing. One
    // NWAY_BUILD job builds at the picked depth, then stores, names and pre-asks its
    // groups. When the table is built but its groups are not stored (or are an older
    // build's), the probe stores them, as opening the table would, and the tab redraws.
    async function buildState(host, v, c) {
        if (!host || !window.NwayPanel || (v.params && v.params.pool)) return;
        const q = new URLSearchParams({ default: '1', collection: v.collection || '', cluster_uuid: c.cluster_uuid });
        if (v.axis) q.set('axis', v.axis);
        const redraw = () => {
            document.dispatchEvent(new CustomEvent('capabilities:changed', { detail: { collection: v.collection } }));
            if (host.isConnected) renderTab(host.closest('.cov').parentElement, v);
        };
        let res, d;
        try {
            res = await fetch(`/api/bin_sim/nway/build?${q}&dry=1`, { method: 'POST' });
            d = await res.json().catch(() => ({}));
        } catch (e) { return; }
        if (!host.isConnected || v.selectedClusterUuid !== c.cluster_uuid) return;
        if (res.status === 428 || d.error === 'needs_build') return NwayPanel.buildGate(host, d, q.toString(), redraw);
        if (!res.ok) return;
        if (d.saved) return redraw();
        if (!d.large) return;
        const cur = (d.large.depths || []).find(x => Number(x.pair_k) === Number(d.large.pair_k));
        host.innerHTML = `<div class="nway-note">Large set built at <b>${escapeHtml(cur ? cur.label : d.large.pair_k ? 'k=' + Number(d.large.pair_k) : 'Deep')}</b> depth. <button type="button" class="btn-action" data-cap-rebuild title="Queue a build at another depth (Fast, Balanced, Deep); picking this depth again names the groups and pre-asks their suggestions"><i class="fa-solid fa-hammer"></i> Rebuild deeper</button><div class="nway-rebuild-host"></div></div>`;
        host.querySelector('[data-cap-rebuild]').onclick = e => {
            e.currentTarget.hidden = true;
            NwayPanel.buildGate(host.querySelector('.nway-rebuild-host'), d.large, q.toString(), redraw);
        };
    }

    return { card, loadCard, renderTab, suggestPane, fetchSet, fetchAccepted, fetchSugg, groupStats };
})();
