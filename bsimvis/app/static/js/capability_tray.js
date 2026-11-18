/**
 * Analyst capabilities (experimental, plan G): one editor and the bottom dock tray.
 *
 * CapEditor renders a capability draft (free-text name, MBC / ATT&CK ids, evidence
 * pointers) and edits it in place; the Capabilities tab's cards (expand in place)
 * and the tray use the same one. CapTray is the dock at the bottom of every page: it
 * holds the capability being built, every "Capability" menu entry adds to it, and
 * Accept saves it (/api/capability/save or /accept). The draft is kept in
 * localStorage so a reload or a view switch keeps it; nothing reaches the server
 * before Accept. Names, strings and rule names come off samples: every value goes
 * through escapeHtml / escapeAttr.
 */
(function () {
    const KEY = 'cap.tray';
    const ICON = { functions: 'fa-code', group: 'fa-layer-group', string: 'fa-quote-right', yara: 'fa-shield-halved', rulezet: 'fa-shield-halved', capa: 'fa-shield-halved' };
    const EXP = '<span class="exp-badge" title="Experimental: results and layout may change"><i class="fa-solid fa-flask"></i> Experimental</span>';

    function ensureCss() {
        if (document.getElementById('cap-css')) return;
        const s = document.createElement('style');
        s.id = 'cap-css';
        s.textContent = `
.cap-ed { display:flex; flex-direction:column; gap:7px; font-size:0.8rem; }
.cap-ed input, .cap-ed textarea { background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:4px 7px; font:inherit; min-width:0; }
.cap-ed .cap-name { font-weight:600; font-size:0.86rem; }
.cap-row { display:flex; flex-wrap:wrap; gap:4px; align-items:center; position:relative; }
.cap-chip { display:inline-flex; gap:4px; align-items:center; padding:0 7px; border-radius:9px; border:1px solid var(--border); font-size:0.7rem; line-height:1.6; white-space:nowrap; }
.cap-chip.id { font-family:Consolas, monospace; color:var(--success); border-color:color-mix(in srgb, var(--success) 55%, transparent); }
.cap-chip.ok { color:var(--success); border-color:color-mix(in srgb, var(--success) 55%, transparent); background:color-mix(in srgb, var(--success) 10%, transparent); }
.cap-chip.sug { color:var(--info); border-style:dashed; border-color:color-mix(in srgb, var(--info) 70%, transparent); }
.cap-chip.todo { color:var(--note-accent, #ffd700); border-color:color-mix(in srgb, var(--note-accent, #ffd700) 55%, transparent); cursor:pointer; }
.cap-x { cursor:pointer; opacity:.65; } .cap-x:hover { opacity:1; color:var(--danger, #f92672); }
.cap-idres { position:absolute; top:100%; left:0; z-index:30; background:var(--card-bg); border:1px solid var(--border); border-radius:4px; max-height:200px; overflow:auto; min-width:260px; }
.cap-idres button { display:block; width:100%; text-align:left; background:none; border:0; color:var(--text); padding:3px 8px; font:inherit; font-size:0.74rem; cursor:pointer; }
.cap-idres button:hover, .cap-idres button:focus-visible { background:var(--hover); }
.cap-evl { display:flex; flex-direction:column; gap:2px; }
.cap-ev { display:grid; grid-template-columns:16px 1fr auto; gap:6px; align-items:start; padding:2px 4px; border-radius:3px; }
.cap-ev:hover { background:var(--hover); }
.cap-ev .w { color:var(--dim); font-size:0.7rem; }
.cap-warn { color:var(--warning, #d97706); font-size:0.72rem; }
.cap-evx { all:unset; cursor:pointer; overflow-wrap:anywhere; }
.cap-evx:hover { color:var(--accent); } .cap-evx:focus-visible { outline:2px solid var(--accent); outline-offset:1px; }
.cap-fns { display:flex; flex-direction:column; margin:4px 0 2px; border-left:2px solid var(--border); max-height:320px; overflow:auto; }
.cap-fn { display:grid; grid-template-columns:minmax(0, 1fr) minmax(0, auto) 14px; gap:8px; align-items:baseline; padding:2px 6px; cursor:default; font-family:Consolas, monospace; font-size:0.72rem; }
.cap-fn:hover { background:var(--hover); }
.cap-fn .nm { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cap-fn .w { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--dim); }
.cap-card { background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:8px 10px; display:flex; flex-direction:column; gap:5px; min-width:0; }
.cap-card.sug { border-style:dashed; border-color:color-mix(in srgb, var(--info) 60%, transparent); }
.cap-card.acc { border-color:color-mix(in srgb, var(--success) 55%, transparent); }
.cap-card .hd { display:grid; grid-template-columns:auto minmax(0, 1fr) auto; gap:8px; align-items:start; }
.cap-card .ttl { font-weight:600; font-size:0.84rem; overflow-wrap:anywhere; min-width:0; }
.cap-card .acts { display:flex; gap:5px; flex-wrap:wrap; justify-content:flex-end; }
.cap-card .meta { color:var(--dim); font-size:0.72rem; overflow-wrap:anywhere; line-height:1.4; }
.cap-card .ids { display:flex; flex-wrap:wrap; gap:4px; }
.cap-sec { display:flex; flex-wrap:wrap; gap:6px; align-items:center; font-size:0.8rem; }
#cap-tray { position:fixed; left:0; right:0; bottom:0; z-index:9000; background:var(--card-bg); border-top:1px solid color-mix(in srgb, var(--success) 60%, transparent); box-shadow:0 -6px 18px rgb(0 0 0 / .3); padding:0 max(12px, env(safe-area-inset-right, 0px)) env(safe-area-inset-bottom, 0px) max(12px, env(safe-area-inset-left, 0px)); }
#cap-tray .bar { display:flex; gap:8px; align-items:center; flex-wrap:wrap; padding:6px 0; font-size:0.8rem; }
#cap-tray .bar b { max-width:40ch; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#cap-tray .body { max-height:55vh; overflow:auto; padding:4px 0 10px; }
.cap-btn { font:inherit; font-size:0.75rem; padding:3px 9px; border-radius:4px; border:1px solid var(--border); background:transparent; color:var(--text); cursor:pointer; white-space:nowrap; }
.cap-btn.ok { background:var(--success); border-color:var(--success); color:#111; }
.cap-btn.pri { background:var(--accent); border-color:var(--accent); color:#111; }
.cap-btn:disabled { opacity:.5; cursor:default; }
`;
        document.head.appendChild(s);
    }

    const shortFid = f => { const [m, a] = String(f).split(':'); return `${(m || '').slice(0, 8)}…:${a || ''}`; };
    const fnsOf = e => e.fids || e.anchors || [];

    function evLabel(e) {
        const n = (e.fids || []).length;
        if (e.type === 'functions') return `${n} function${n === 1 ? '' : 's'}`;
        if (e.type === 'group') return `group ${e.label || ''} · ${(e.anchors || []).length} sample functions`;
        if (e.type === 'string') return `string "${e.value}"${n ? ` · ${n} fn` : ''}`;
        return `${e.type} ${e.rule}${n ? ` · ${n} fn` : ''}`;
    }

    // Function names for the evidence lists, by collection and md5:addr (fetched in batches).
    const NAMES = {};
    async function loadNames(coll, fids) {
        const miss = [...new Set(fids)].filter(f => !NAMES[`${coll}|${f}`]);
        for (let i = 0; i < miss.length; i += 300) {
            const part = miss.slice(i, i + 300);
            part.forEach(f => { NAMES[`${coll}|${f}`] = { pending: true }; });
            try {
                const r = await fetch(`/api/capability/functions?collection=${encodeURIComponent(coll)}&fids=${encodeURIComponent(part.join(','))}`);
                const j = await r.json();
                (j.functions || []).forEach(x => { NAMES[`${coll}|${x.fid}`] = x; });
            } catch (e) { }
            part.forEach(f => { if (NAMES[`${coll}|${f}`].pending) NAMES[`${coll}|${f}`] = { fid: f }; });
        }
    }

    function fnRows(d, e, k) {
        return `<div class="cap-fns">${fnsOf(e).map((f, i) => {
            const n = NAMES[`${d.collection}|${f}`] || {};
            const [, addr] = f.split(':');
            return `<div class="cap-fn" data-pf="${escapeAttr(f)}">
                <span class="nm">${n.pending ? '<span class="w">…</span>' : escapeHtml(n.name || shortFid(f))}</span>
                <span class="w" title="${escapeAttr(f)}">${escapeHtml(n.file_name || f.split(':')[0].slice(0, 8))} · ${escapeHtml(addr || '')}</span>
                ${e.type === 'group' ? '<span></span>' : `<span class="cap-x" data-rmfn="${k}:${i}" title="Remove this function">×</span>`}</div>`;
        }).join('')}</div>`;
    }

    // ---- the editor ----
    function html(d, open = new Set()) {
        const ids = [...(d.mbc || []), ...(d.attack || [])];
        const names = d.names || {};
        return `<div class="cap-ed">
            <input class="cap-name" aria-label="Capability name" placeholder="Capability name, e.g. UDP flood" maxlength="120" value="${escapeAttr(d.name || '')}">
            ${(d.unbacked || []).length ? `<div class="cap-warn"><i class="fa-solid fa-triangle-exclamation"></i> No pointer holds: ${escapeHtml(d.unbacked.join(', '))}</div>` : ''}
            <div class="cap-row">${ids.map(i => `<span class="cap-chip id" title="${escapeAttr(names[i] || '')}">${escapeHtml(i)} <span style="color:var(--dim)">${escapeHtml((names[i] || '').slice(0, 28))}</span><span class="cap-x" data-rmid="${escapeAttr(i)}" title="Remove">×</span></span>`).join('')}
                <input class="cap-idq" aria-label="Add an MBC or ATT&CK id" placeholder="+ MBC / ATT&CK id" style="width:150px;"><div class="cap-idres" hidden></div></div>
            <div class="cap-evl">${(d.evidence || []).map((e, k) => {
                const n = fnsOf(e).length, isOpen = open.has(k);
                return `<div class="cap-ev">
                <i class="fa-solid ${ICON[e.type] || 'fa-circle'}" style="color:var(--dim); margin-top:3px;"></i>
                <span style="min-width:0; overflow-wrap:anywhere;">${n ? `<button type="button" class="cap-evx" data-evx="${k}" aria-expanded="${isOpen}" title="${isOpen ? 'Hide' : 'Show'} its functions"><i class="fa-solid fa-chevron-${isOpen ? 'down' : 'right'}"></i> ${escapeHtml(evLabel(e))}</button>` : escapeHtml(evLabel(e))}${e.src === 'llm' ? ' <span class="w">model</span>' : ''}${e.why ? `<div class="w">${escapeHtml(e.why)}</div>` : ''}${isOpen ? fnRows(d, e, k) : ''}</span>
                <span class="cap-x" data-rmev="${k}" title="Remove this pointer">×</span></div>`;
            }).join('') || '<div class="w" style="color:var(--dim)">No evidence yet: add functions from any view (right click, Capability).</div>'}</div>
            <input class="cap-addfn" aria-label="Add a function by md5:addr" placeholder="+ function md5:addr, Enter">
            <textarea class="cap-desc" rows="2" aria-label="Description" placeholder="Why (optional)" maxlength="1000">${escapeHtml(d.description || '')}</textarea>
        </div>`;
    }

    // Mounts the editor on `root`, editing `d` in place; `onChange()` after each edit.
    function mount(root, d, onChange = () => {}) {
        ensureCss();
        const open = new Set();
        const render = () => { root.innerHTML = html(d, open); };
        let timer = null;
        render();
        root.oninput = e => {
            const t = e.target;
            if (t.classList.contains('cap-name')) { d.name = t.value; onChange(); }
            else if (t.classList.contains('cap-desc')) { d.description = t.value; onChange(); }
            else if (t.classList.contains('cap-idq')) {
                clearTimeout(timer);
                const q = t.value.trim();
                const res = root.querySelector('.cap-idres');
                if (!q) { res.hidden = true; return; }
                timer = setTimeout(async () => {
                    try {
                        const r = await fetch(`/api/capability/catalog?q=${encodeURIComponent(q)}`);
                        const j = await r.json();
                        res.innerHTML = (j.ids || []).map(x => `<button type="button" data-addid="${escapeAttr(x.id)}" data-kind="${escapeAttr(x.kind)}" data-name="${escapeAttr(x.name || '')}"><b>${escapeHtml(x.id)}</b> ${escapeHtml(x.name || '')} <span style="color:var(--dim)">${x.kind === 'mbc' ? 'MBC' : 'ATT&CK'}</span></button>`).join('') || '<div style="padding:4px 8px; color:var(--dim)">No id matches</div>';
                        res.hidden = false;
                    } catch (err) { res.hidden = true; }
                }, 200);
            }
        };
        root.onclick = e => {
            const t = e.target.closest('[data-rmid],[data-rmev],[data-addid],[data-evx],[data-rmfn]');
            if (!t) return;
            if (t.dataset.evx) {
                const k = +t.dataset.evx;
                if (open.has(k)) open.delete(k);
                else {
                    open.add(k);
                    if (d.collection && !d.pool) loadNames(d.collection, fnsOf(d.evidence[k])).then(() => { if (root.isConnected) render(); });
                }
                render();
                return;
            }
            if (t.dataset.rmid) { d.mbc = (d.mbc || []).filter(i => i !== t.dataset.rmid); d.attack = (d.attack || []).filter(i => i !== t.dataset.rmid); }
            else if (t.dataset.rmfn) {
                const [k, i] = t.dataset.rmfn.split(':').map(Number);
                const ev = d.evidence[k];
                ev.fids = ev.fids.filter((_, j) => j !== i);
                if (!ev.fids.length && ev.type === 'functions') { d.evidence.splice(k, 1); open.clear(); }
            }
            else if (t.dataset.rmev) { d.evidence.splice(+t.dataset.rmev, 1); open.clear(); }
            else if (t.dataset.addid) {
                const k = t.dataset.kind === 'mbc' ? 'mbc' : 'attack';
                d[k] = [...new Set([...(d[k] || []), t.dataset.addid])];
                d.names = { ...(d.names || {}), [t.dataset.addid]: t.dataset.name };
            }
            render(); onChange();
        };
        root.onkeydown = e => {
            if (e.key !== 'Enter' || !e.target.classList.contains('cap-addfn')) return;
            e.preventDefault();
            const m = /([0-9a-f]{32}):([^:\s]+)\s*$/i.exec(e.target.value.trim());
            if (!m) { if (window.showToast) showToast('Paste a function as md5:addr', 'error'); return; }
            d.evidence = [...(d.evidence || []), { type: 'functions', fids: [`${m[1].toLowerCase()}:${m[2]}`], src: 'user' }];
            render(); onChange();
        };
        // one code preview per listed function
        root.onmouseover = e => {
            const row = e.target.closest('[data-pf]');
            const f = row && row.dataset.pf;
            if (!f || !window.showCodePreview || !d.collection || d.pool) return;
            const [md5, addr] = f.split(':');
            const n = NAMES[`${d.collection}|${f}`] || {};
            window.showCodePreview(`${d.collection}:func:${md5}:${addr}`, n.name || f, addr, md5, 0, e, 0, n.file_name || '');
        };
        root.onmouseout = e => { if (e.target.closest('[data-pf]') && window.hideCodePreview) window.hideCodePreview(e); };
        return { render, get: () => d };
    }

    // The fields the server takes; a suggestion's extras (quotes, unbacked) stay out.
    function payload(d) {
        const out = { name: (d.name || '').trim(), description: d.description || '', mbc: d.mbc || [], attack: d.attack || [], evidence: (d.evidence || []).map(({ id, ...e }) => e) };
        if (d.uuid) out.uuid = d.uuid;
        return out;
    }

    function nsBody(d) { return d.pool ? { pool: d.pool } : { collection: d.collection }; }

    async function post(url, body) {
        const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(j.message || j.error || `HTTP ${r.status}`);
        return j;
    }

    // Saves a capability draft: the editor's full save for an existing one (or with
    // `replace`), else an accept (create, or attach onto `attach_to`).
    async function accept(d, replace = false) {
        const p = payload(d);
        if (d.attach_to && !d.uuid) { p.uuid = d.attach_to; }
        if (!p.uuid && !p.name) throw new Error('Give the capability a name');
        const j = (replace || d.uuid)
            ? await post('/api/capability/save', { ...nsBody(d), capability: p })
            : await post('/api/capability/accept', { ...nsBody(d), items: [p] });
        document.dispatchEvent(new CustomEvent('capabilities:changed', { detail: { collection: d.collection, pool: d.pool } }));
        return j;
    }

    async function acceptMany(list) {
        if (!list.length) return null;
        const d0 = list[0];
        const items = list.map(d => { const p = payload(d); if (d.attach_to && !d.uuid) p.uuid = d.attach_to; return p; });
        const j = await post('/api/capability/accept', { ...nsBody(d0), items });
        document.dispatchEvent(new CustomEvent('capabilities:changed', { detail: { collection: d0.collection, pool: d0.pool } }));
        return j;
    }

    window.CapEditor = { mount, html, accept, acceptMany, payload, evLabel, ensureCss, EXP };

    // ---- the tray ----
    let draft = null, open = false, ed = null;
    try { draft = JSON.parse(localStorage.getItem(KEY) || 'null'); } catch (e) { draft = null; }
    const keep = () => { try { draft ? localStorage.setItem(KEY, JSON.stringify(draft)) : localStorage.removeItem(KEY); } catch (e) { } };

    function counts(d) {
        const ev = d.evidence || [];
        const fn = ev.filter(e => e.type === 'functions' || e.type === 'group').length;
        const st = ev.filter(e => e.type === 'string').length;
        const ru = ev.length - fn - st;
        const ids = (d.mbc || []).length + (d.attack || []).length;
        return [fn && `${fn} fn`, st && `${st} string${st > 1 ? 's' : ''}`, ru && `${ru} rule${ru > 1 ? 's' : ''}`, ids && `${ids} id${ids > 1 ? 's' : ''}`].filter(Boolean).join(' · ') || 'empty';
    }

    function el() {
        let t = document.getElementById('cap-tray');
        if (!t) { t = document.createElement('div'); t.id = 'cap-tray'; t.setAttribute('role', 'region'); t.setAttribute('aria-label', 'Capability being built'); document.body.appendChild(t); }
        return t;
    }

    function draw() {
        ensureCss();
        const t = el();
        if (!draft) { t.remove(); document.body.style.paddingBottom = ''; document.documentElement.style.removeProperty('--cap-tray-h'); return; }
        t.innerHTML = `<div class="bar">${EXP}
            <span style="min-width:0">${draft.uuid ? 'Editing' : 'Building'} <b>${escapeHtml(draft.name || 'unnamed capability')}</b> <span style="color:var(--dim)">· <span id="cap-tray-n">${escapeHtml(counts(draft))}</span> · ${escapeHtml(draft.collection || draft.pool || '')}</span></span>
            <span style="margin-left:auto; display:flex; gap:6px;">
                <button type="button" class="cap-btn" id="cap-tray-open" aria-expanded="${open}">${open ? 'Close ▾' : 'Open ▴'}</button>
                <button type="button" class="cap-btn" id="cap-tray-clear" title="Throw this draft away">Clear</button>
                <button type="button" class="cap-btn ok" id="cap-tray-accept">Accept</button></span></div>
            <div class="body" id="cap-tray-body" ${open ? '' : 'hidden'}></div>`;
        if (open) ed = mount(t.querySelector('#cap-tray-body'), draft, () => { keep(); const n = t.querySelector('#cap-tray-n'); if (n) n.textContent = counts(draft); });
        t.querySelector('#cap-tray-open').onclick = () => { open = !open; draw(); };
        t.querySelector('#cap-tray-clear').onclick = () => { draft = null; open = false; keep(); draw(); };
        t.querySelector('#cap-tray-accept').onclick = async ev => {
            const b = ev.currentTarget;
            b.disabled = true;
            try {
                await accept(draft);
                if (window.showToast) showToast(`Accepted: ${draft.name || 'capability'}`, 'success');
                draft = null; open = false; keep(); draw();
            } catch (e) { if (window.showToast) showToast(`Not accepted: ${e.message}`, 'error'); b.disabled = false; }
        };
        document.body.style.paddingBottom = `${t.offsetHeight}px`;
        document.documentElement.style.setProperty('--cap-tray-h', `${t.offsetHeight}px`);
    }

    function sameNs(ctx) { return draft && (draft.collection || '') === (ctx.collection || '') && (draft.pool || '') === (ctx.pool || ''); }

    // Starts a new draft; a non-empty draft of another capability is never dropped silently.
    function start(d, ctx) {
        if (draft && (draft.evidence || []).length) {
            if (window.showToast) showToast(`The tray already holds "${draft.name || 'a capability'}": accept or clear it first`, 'error');
            return false;
        }
        draft = { mbc: [], attack: [], evidence: [], names: {}, ...d, collection: ctx.collection || null, pool: ctx.pool || null };
        open = true; keep(); draw();
        return true;
    }

    // Adds one pointer to the draft (starting one when there is none).
    function add(item, ctx = {}) {
        if (!draft) return start({ name: ctx.name || '', evidence: [item] }, ctx);
        if (!sameNs(ctx)) { if (window.showToast) showToast('The tray holds a capability of another collection', 'error'); return false; }
        draft.evidence = [...(draft.evidence || []), item];
        keep(); draw();
        if (window.showToast) showToast(`Added to ${draft.name || 'the capability'}`, 'success');
        return true;
    }

    // Opens a capability (accepted or suggested) in the tray for editing.
    function edit(cap, ctx) {
        if (draft && (draft.evidence || []).length && !(cap.uuid && draft.uuid === cap.uuid)) {
            if (window.showToast) showToast('The tray holds another capability: accept or clear it first', 'error');
            return false;
        }
        draft = null;
        return start(JSON.parse(JSON.stringify(cap)), ctx);
    }

    // The "Capability" entries of the right-click menu (function and string rows).
    function menuHtml(type, norm) {
        if (type !== 'function' && !(type === 'string' && norm.value)) return '';
        const col = type === 'function' ? (norm.id || '').split(':')[0] : (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : '');
        const item = type === 'function'
            ? { type: 'functions', fids: [`${norm.md5}:${norm.addr}`], src: 'user' }
            : { type: 'string', value: norm.value, src: 'user' };
        const arg = escapeAttr(JSON.stringify({ item, ctx: { collection: col } }));
        const to = draft ? `Add to "${(draft.name || 'unnamed').slice(0, 28)}"` : 'New capability from this';
        return `<div class="context-menu-item" data-cap="${arg}" onclick="event.stopPropagation(); window.closeGraphContextMenu(); CapTray.fromMenu(this.dataset.cap)">
            <i class="fa-solid fa-bolt" style="width:16px; text-align:center; opacity:0.8;"></i><span>Capability: ${escapeHtml(to)}</span></div>`;
    }

    function fromMenu(raw) {
        try { const { item, ctx } = JSON.parse(raw); add(item, ctx); } catch (e) { }
    }

    window.CapTray = { add, edit, start, menuHtml, fromMenu, draw, get: () => draft };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', draw); else draw();
})();
