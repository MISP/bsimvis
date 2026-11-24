/**
 * search_palette.js
 * Ctrl+K unified search across batches, files, functions, clusters, tags,
 * features, collections and pools. Fans out server-side via
 * /api/search/unified/stream — no client-side index.
 *
 * Results paint as they arrive: the endpoint emits NDJSON, one group per line,
 * cheap entity types first, so the first hits show up without waiting on the
 * per-collection scans behind them.
 */

window.SearchPalette = (function () {
    const KIND_META = {
        collections: { label: 'Collections', icon: 'fa-layer-group', color: '#ffab2e' },
        pools: { label: 'Pools', icon: 'fa-diagram-project', color: '#a78bfa' },
        batches: { label: 'Batches', icon: 'fa-boxes-stacked', color: '#60a5fa' },
        files: { label: 'Files', icon: 'fa-file-code', color: '#22c55e' },
        functions: { label: 'Functions', icon: 'fa-code', color: '#38bdf8' },
        strings: { label: 'Strings', icon: 'fa-font', color: '#e6db74' },
        function_strings: { label: 'Functions by string', icon: 'fa-font', color: '#e6db74' },
        features: { label: 'Features', icon: 'fa-fingerprint', color: '#f472b6' },
        function_clusters: { label: 'Function Clusters', icon: 'fa-bullseye', color: '#fb923c' },
        binary_clusters: { label: 'Binary Clusters', icon: 'fa-bullseye', color: '#f87171' },
        tags: { label: 'Tags', icon: 'fa-tags', color: '#facc15' }
    };

    const LIMIT = 5;    // first page per group; "See more" fetches LIMIT more of one kind
    let flat = [];      // flattened results, in render order ({more: groupIdx} = See more row)
    let groups = [];    // painted groups: {kind, items, more, loading}
    let lastQ = '';
    let cursor = 0;
    let seq = 0;        // request sequence, guards out-of-order responses
    let debounce = null;
    let inflight = null; // AbortController for the running stream
    let ctxColl = null;  // collection the palette can scope to (page context)
    let scopeColl = null; // active scope; null = whole instance

    function ensureDom() {
        if (document.getElementById('search-palette')) return;
        const el = document.createElement('div');
        el.id = 'search-palette';
        el.style.cssText = 'display:none; position:fixed; inset:0; z-index:20000; background:rgba(0,0,0,0.55); backdrop-filter:blur(2px);';
        el.innerHTML = `
            <div id="palette-box" style="max-width:720px; margin:8vh auto 0; background:var(--card-bg); border:1px solid var(--border); border-radius:10px; overflow:hidden; box-shadow:0 20px 60px rgba(0,0,0,0.5);">
                <div style="display:flex; align-items:center; gap:10px; padding:14px 16px; border-bottom:1px solid var(--border);">
                    <i class="fa-solid fa-magnifying-glass" style="color:var(--accent);"></i>
                    <span id="palette-scope" style="display:none; border:1px solid var(--border); border-radius:6px; overflow:hidden; font-size:0.72rem; white-space:nowrap;" title="Tab switches scope"></span>
                    <input id="palette-input" type="text" autocomplete="off" spellcheck="false"
                        placeholder="Search batches, files, functions, clusters, tags, features, collections, pools…"
                        style="flex:1; background:none; border:none; outline:none; color:var(--text); font-size:1rem;">
                    <span style="font-size:0.7rem; color:var(--dim); border:1px solid var(--border); border-radius:4px; padding:2px 6px;">ESC</span>
                </div>
                <div id="palette-results" style="max-height:60vh; overflow-y:auto;"></div>
                <div style="padding:8px 16px; border-top:1px solid var(--border); font-size:0.7rem; color:var(--dim); display:flex; gap:16px;">
                    <span><b>↑↓</b> navigate</span><span><b>Enter</b> open</span><span><b>Ctrl+Enter</b> new tab</span>
                </div>
            </div>`;
        document.body.appendChild(el);

        el.addEventListener('mousedown', (e) => { if (e.target === el) close(); });
        const input = el.querySelector('#palette-input');
        input.addEventListener('input', () => schedule(input.value));
        input.addEventListener('keydown', onKey);
        el.querySelector('#palette-results').addEventListener('mousedown', (e) => {
            const ex = e.target.closest('.palette-ex'), sx = e.target.closest('.palette-syntax');
            if (!ex && !sx) return;
            e.preventDefault(); // keep focus in the input
            if (sx) { syntaxOpen = !syntaxOpen; e.currentTarget.innerHTML = hint(); return; }
            input.value = ex.dataset.q;
            run(input.value);
        });
        el.querySelector('#palette-scope').addEventListener('mousedown', (e) => {
            const b = e.target.closest('button[data-scope]');
            if (!b) return;
            e.preventDefault();
            setScope(b.dataset.scope === 'coll' ? ctxColl : null);
        });
    }

    function schedule(q) {
        clearTimeout(debounce);
        debounce = setTimeout(() => run(q), 120);
    }

    async function run(q) {
        const box = document.getElementById('palette-results');
        q = (q || '').trim();
        if (inflight) inflight.abort();     // a stale stream costs server work
        inflight = null;
        if (!q) { flat = []; box.innerHTML = hint(); return; }

        const mine = ++seq;
        flat = [];
        groups = [];
        lastQ = q;
        cursor = 0;
        box.innerHTML = `<div id="palette-groups"></div>
            <div id="palette-status" style="padding:14px 16px; color:var(--dim); font-size:0.8rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Searching…</div>`;

        const ctl = new AbortController();
        inflight = ctl;
        try {
            const res = await fetch(
                `/api/search/unified/stream?q=${encodeURIComponent(q)}&limit=${LIMIT}${scopeArg()}`, { signal: ctl.signal });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            for (;;) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                // Everything up to the last newline is a set of complete records.
                const lines = buf.split('\n');
                buf = lines.pop();
                for (const line of lines) {
                    if (!line.trim()) continue;
                    if (mine !== seq) return;   // a newer query already started
                    let msg;
                    try { msg = JSON.parse(line); } catch (_) { continue; }
                    // collections / pools are not scoped server-side
                    if (msg.type === 'group' && !(scopeColl && (msg.kind === 'collections' || msg.kind === 'pools'))) appendGroup(msg);
                }
            }
        } catch (e) {
            if (e.name === 'AbortError' || mine !== seq) return;
            document.getElementById('palette-status').innerHTML =
                `<span style="color:#f92672;">Search failed: ${escapeHtml(e.message)}</span>`;
            return;
        } finally {
            if (inflight === ctl) inflight = null;
        }
        if (mine !== seq) return;
        finish(q);
    }

    function finish(q) {
        const status = document.getElementById('palette-status');
        if (!status) return;
        if (!flat.length) {
            status.innerHTML = `<div style="padding:10px 0; color:var(--dim);">No results for “${escapeHtml(q)}”.</div>`;
        } else {
            status.remove();
        }
    }

    function scopeArg() {
        return scopeColl ? `&collection=${encodeURIComponent(scopeColl)}` : '';
    }

    const ORDER = ['binary_clusters', 'files', 'functions', 'function_clusters', 'tags', 'strings',
        'function_strings', 'batches', 'features', 'collections', 'pools'];
    const rank = (k) => { const i = ORDER.indexOf(k); return i < 0 ? ORDER.length : i; };

    /** Appends one streamed group, keeping the current selection where it is. */
    function appendGroup(g) {
        if (!g.items || !g.items.length) return;
        groups.push({ kind: g.kind, items: g.items, views: g.views || [], more: g.items.length >= LIMIT, loading: false });
        // Display order, whatever order the stream delivers in (cheap kinds arrive first).
        groups.sort((x, y) => rank(x.kind) - rank(y.kind));
        render();
    }

    /** Repaints every group from `groups`; indices shift when a group grows. */
    function render() {
        const box = document.getElementById('palette-groups');
        if (!box) return;
        flat = [];
        box.innerHTML = groups.map((g, gi) => {
            const meta = KIND_META[g.kind] || { label: g.kind, icon: 'fa-circle', color: 'var(--accent)' };
            let html = `<div style="padding:8px 16px 4px; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; color:${meta.color}; display:flex; gap:8px; align-items:baseline;">
                <span>${escapeHtml(meta.label)}</span>${viewLinks(g, meta)}</div>`;
            html += g.items.map(it => { flat.push(it); return rowHtml(it, flat.length - 1, meta); }).join('');
            if (g.more) {
                flat.push({ more: gi });
                html += `<div class="palette-row" data-idx="${flat.length - 1}" onmousedown="event.preventDefault()" onclick="SearchPalette.open(${flat.length - 1}, event)"
                    style="padding:5px 16px 5px 44px; cursor:pointer; font-size:0.75rem; color:var(--dim); border-left:2px solid transparent;">
                    ${g.loading ? '<i class="fa-solid fa-spinner fa-spin"></i> Loading…' : `<i class="fa-solid fa-angle-down"></i> See more ${escapeHtml(meta.label.toLowerCase())}`}</div>`;
            }
            return html;
        }).join('');
        highlight();
    }

    /** Refetches one kind with a bigger limit (each route pages from 0) and keeps the new tail. */
    async function loadMore(gi) {
        const g = groups[gi];
        if (!g || g.loading) return;
        const mine = seq, shown = g.items.length, want = shown + LIMIT;
        g.loading = true;
        render();
        try {
            const res = await fetch(`/api/search/unified?q=${encodeURIComponent(lastQ)}&kind=${encodeURIComponent(g.kind)}&limit=${want}${scopeArg()}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            if (mine !== seq) return;
            const all = ((data.groups || []).find(x => x.kind === g.kind) || {}).items || [];
            g.items = g.items.concat(all.slice(shown));
            const v = ((data.groups || []).find(x => x.kind === g.kind) || {}).views;
            if (v && v.length) g.views = v;
            g.more = all.length >= want;
        } catch (e) {
            if (mine !== seq) return;
            g.more = false;
        }
        g.loading = false;
        render();
    }

    /** "Files in A · B": one link per collection with hits, to that collection's full list view. */
    function viewLinks(g, meta) {
        // Only same-origin collection paths; anything else from the server is dropped.
        const ok = g.views.filter(v => typeof v.url === 'string' && v.url.startsWith('/collections/'));
        if (!ok.length) return '';
        const links = ok.map(v => `<a href="${escapeHtml(v.url)}" onmousedown="event.preventDefault()" onclick="SearchPalette.go(event, this)"
            style="color:var(--dim); text-decoration:underline; text-transform:none;" title="Open the ${escapeHtml(meta.label.toLowerCase())} view">${escapeHtml(v.collection)}</a>`);
        return `<span style="margin-left:auto; text-transform:none; letter-spacing:0; color:var(--dim);">${escapeHtml(meta.label)} in ${links.join(' · ')} →</span>`;
    }

    function go(e, a) {
        if (e.ctrlKey || e.metaKey || e.shiftKey || e.button === 1) return; // browser opens a new tab
        e.preventDefault();
        close();
        const url = a.getAttribute('href');
        if (window.Nav) Nav.openPath(url, null); else window.location.href = url;
    }

    function rowHtml(it, idx, meta) {
        return `<div class="palette-row" data-idx="${idx}" onclick="SearchPalette.open(${idx}, event)"
            style="display:flex; align-items:center; gap:12px; padding:8px 16px; cursor:pointer; border-left:2px solid transparent;">
            <i class="fa-solid ${meta.icon}" style="color:${meta.color}; width:16px; text-align:center;"></i>
            <div style="min-width:0; flex:1;">
                <div style="font-size:0.85rem; color:var(--text); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(it.title || '')}</div>
                <div style="font-size:0.72rem; color:var(--dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(it.subtitle || '')}</div>
            </div>
        </div>`;
    }

    const EXAMPLES = ['mirai', 'tag:mirai', 'name:busybox', 'md5:b7680c69', 'cluster:gafgyt',
        'string:"/bin/busybox"', 'tag:yara:mirai name:arm', '"tag:mirai"'];
    const SYNTAX = [
        ['word', 'Names, MD5s, cluster names'],
        ['tag:x', 'Tag (file or function)'],
        ['name:x', 'File or function name'],
        ['md5:x', 'File MD5'],
        ['cluster:x', 'Cluster name'],
        ['string:x', 'Strings in the binary'],
        ['field:x', 'Any indexed field'],
        ['a b', 'Both (AND)'],
        ['x*', 'Wildcard'],
        ['"a b"', 'Literal phrase'],
        ['"a \\"b\\""', 'Quote inside a phrase'],
    ];
    let syntaxOpen = false;

    function hint() {
        const chip = (q) => `<code class="palette-ex" data-q="${escapeHtml(q)}" style="cursor:pointer; padding:2px 7px; border:1px solid var(--border); border-radius:4px; background:var(--hover);">${escapeHtml(q)}</code>`;
        const rows = SYNTAX.map(([k, v]) => `<tr><td style="padding:3px 24px 3px 0; border:none;"><code>${escapeHtml(k)}</code></td><td style="color:var(--dim); border:none; padding:3px 0;">${escapeHtml(v)}</td></tr>`).join('');
        return `<div style="padding:18px 16px; font-size:0.8rem;">
            <div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center;">${EXAMPLES.map(chip).join('')}
                <button type="button" class="palette-syntax" aria-expanded="${syntaxOpen}" style="margin-left:auto; background:none; border:none; color:var(--accent); cursor:pointer; font-size:0.75rem;">
                    <i class="fa-solid fa-circle-question"></i> Syntax</button></div>
            ${syntaxOpen ? `<table style="margin-top:12px; width:auto; border-collapse:collapse; font-size:0.78rem;">${rows}</table>` : ''}
        </div>`;
    }

    function renderScope() {
        const pill = document.getElementById('palette-scope');
        const input = document.getElementById('palette-input');
        pill.style.display = ctxColl ? 'inline-flex' : 'none';
        input.placeholder = scopeColl
            ? `Search in ${scopeColl}…`
            : 'Search batches, files, functions, clusters, tags, features, collections, pools…';
        if (!ctxColl) { pill.innerHTML = ''; return; }
        const btn = (label, on, val) => `<button type="button" aria-pressed="${on}" data-scope="${val}"
            style="border:none; padding:3px 9px; cursor:pointer; max-width:180px; overflow:hidden; text-overflow:ellipsis;
            background:${on ? 'var(--accent)' : 'transparent'}; color:${on ? '#000' : 'var(--dim)'};">${escapeHtml(label)}</button>`;
        pill.innerHTML = btn(ctxColl, !!scopeColl, 'coll') + btn('All', !scopeColl, 'all');
    }

    function setScope(coll) {
        scopeColl = coll;
        renderScope();
        const input = document.getElementById('palette-input');
        input.focus();
        run(input.value);
    }

    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    }

    function highlight() {
        document.querySelectorAll('#search-palette .palette-row').forEach(row => {
            const on = Number(row.dataset.idx) === cursor;
            row.style.background = on ? 'rgba(255,171,46,0.10)' : 'transparent';
            row.style.borderLeftColor = on ? 'var(--accent)' : 'transparent';
            if (on) row.scrollIntoView({ block: 'nearest' });
        });
    }

    function onKey(e) {
        // Keys the palette handles must not reach the page behind it. The table
        // selection layer listens on window and, seeing an INPUT focused, blurs
        // it and takes the arrow key for itself -- which left the palette input
        // blurred after the first ArrowDown, so every key after that (Enter
        // included) drove the table instead of the palette.
        //
        // Only the handled keys are stopped: stopping everything would take
        // Ctrl+K too, and the palette is closed with the same shortcut.
        const handled = () => { e.preventDefault(); e.stopPropagation(); };

        if (e.key === 'Escape') { handled(); close(); return; }
        if (e.key === 'Tab' && ctxColl) { handled(); setScope(scopeColl ? null : ctxColl); return; }
        if (e.key === 'ArrowDown') { handled(); if (flat.length) { cursor = (cursor + 1) % flat.length; highlight(); } }
        else if (e.key === 'ArrowUp') { handled(); if (flat.length) { cursor = (cursor - 1 + flat.length) % flat.length; highlight(); } }
        else if (e.key === 'Enter') { handled(); open(cursor, e); }
    }

    function open(idx, event) {
        const it = flat[idx];
        if (!it) return;
        if (it.more !== undefined) { loadMore(it.more); return; }
        close();
        if (event && (event.ctrlKey || event.metaKey)) { window.open(it.url, '_blank'); return; }
        if (window.Nav) Nav.openPath(it.url, null);
        else window.location.href = it.url;
    }

    /** opts.collection scopes to that collection; otherwise the page's collection is offered, global by default. */
    function show(initial, opts) {
        ensureDom();
        if (opts && opts.collection) { ctxColl = scopeColl = opts.collection; }
        else {
            const m = location.pathname.match(/^\/collections\/([^/]+)/);
            ctxColl = m ? decodeURIComponent(m[1]) : null;
            scopeColl = null;
        }
        renderScope();
        const el = document.getElementById('search-palette');
        el.style.display = 'block';
        const input = el.querySelector('#palette-input');
        if (initial !== undefined) input.value = initial;
        input.focus();
        input.select();
        if (input.value) run(input.value); else document.getElementById('palette-results').innerHTML = hint();
    }

    function close() {
        const el = document.getElementById('search-palette');
        if (el) el.style.display = 'none';
    }

    function isOpen() {
        const el = document.getElementById('search-palette');
        return !!el && el.style.display === 'block';
    }

    document.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
            e.preventDefault();
            isOpen() ? close() : show();
        }
    });

    return { show, close, open, isOpen, go };
})();
