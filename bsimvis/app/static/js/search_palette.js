/**
 * search_palette.js
 * Ctrl+K unified search across batches, files, functions, clusters, tags,
 * features, collections and pools. Fans out server-side via
 * /api/search/unified — no client-side index.
 */

window.SearchPalette = (function () {
    const KIND_META = {
        collections: { label: 'Collections', icon: 'fa-layer-group', color: '#ffab2e' },
        pools: { label: 'Pools', icon: 'fa-diagram-project', color: '#a78bfa' },
        batches: { label: 'Batches', icon: 'fa-boxes-stacked', color: '#60a5fa' },
        files: { label: 'Files', icon: 'fa-file-code', color: '#22c55e' },
        functions: { label: 'Functions', icon: 'fa-code', color: '#38bdf8' },
        features: { label: 'Features', icon: 'fa-fingerprint', color: '#f472b6' },
        function_clusters: { label: 'Function Clusters', icon: 'fa-bullseye', color: '#fb923c' },
        binary_clusters: { label: 'Binary Clusters', icon: 'fa-bullseye', color: '#f87171' },
        tags: { label: 'Tags', icon: 'fa-tags', color: '#facc15' }
    };

    let flat = [];      // flattened results, in render order
    let cursor = 0;
    let seq = 0;        // request sequence, guards out-of-order responses
    let debounce = null;

    function ensureDom() {
        if (document.getElementById('search-palette')) return;
        const el = document.createElement('div');
        el.id = 'search-palette';
        el.style.cssText = 'display:none; position:fixed; inset:0; z-index:20000; background:rgba(0,0,0,0.55); backdrop-filter:blur(2px);';
        el.innerHTML = `
            <div id="palette-box" style="max-width:720px; margin:8vh auto 0; background:var(--card-bg); border:1px solid var(--border); border-radius:10px; overflow:hidden; box-shadow:0 20px 60px rgba(0,0,0,0.5);">
                <div style="display:flex; align-items:center; gap:10px; padding:14px 16px; border-bottom:1px solid var(--border);">
                    <i class="fa-solid fa-magnifying-glass" style="color:var(--accent);"></i>
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
    }

    function schedule(q) {
        clearTimeout(debounce);
        debounce = setTimeout(() => run(q), 180);
    }

    async function run(q) {
        const box = document.getElementById('palette-results');
        q = (q || '').trim();
        if (!q) { flat = []; box.innerHTML = hint(); return; }
        const mine = ++seq;
        box.innerHTML = `<div style="padding:20px; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin"></i> Searching…</div>`;
        let data;
        try {
            const res = await fetch(`/api/search/unified?q=${encodeURIComponent(q)}&limit=5`);
            data = await res.json();
        } catch (e) {
            if (mine === seq) box.innerHTML = `<div style="padding:20px; color:#f92672;">Search failed: ${e.message}</div>`;
            return;
        }
        if (mine !== seq) return;   // a newer query already landed
        render(data);
    }

    function hint() {
        return `<div style="padding:24px; color:var(--dim); font-size:0.85rem;">
            Type to search across the whole instance — a filename, an MD5, a function name,
            a tag, a cluster name, a collection or a pool.</div>`;
    }

    function render(data) {
        const box = document.getElementById('palette-results');
        const groups = (data && data.groups) || [];
        flat = [];
        if (!groups.length) {
            box.innerHTML = `<div style="padding:24px; color:var(--dim);">No results for “${data.query}”.</div>`;
            return;
        }
        let html = '';
        groups.forEach(g => {
            const meta = KIND_META[g.kind] || { label: g.kind, icon: 'fa-circle', color: 'var(--accent)' };
            html += `<div style="padding:8px 16px 4px; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; color:${meta.color};">${meta.label}</div>`;
            g.items.forEach(it => {
                const idx = flat.length;
                flat.push(it);
                html += `<div class="palette-row" data-idx="${idx}" onclick="SearchPalette.open(${idx}, event)"
                    style="display:flex; align-items:center; gap:12px; padding:8px 16px; cursor:pointer; border-left:2px solid transparent;">
                    <i class="fa-solid ${meta.icon}" style="color:${meta.color}; width:16px; text-align:center;"></i>
                    <div style="min-width:0; flex:1;">
                        <div style="font-size:0.85rem; color:var(--text); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(it.title || '')}</div>
                        <div style="font-size:0.72rem; color:var(--dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(it.subtitle || '')}</div>
                    </div>
                </div>`;
            });
        });
        box.innerHTML = html;
        cursor = 0;
        highlight();
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
        if (e.key === 'Escape') { close(); return; }
        if (e.key === 'ArrowDown') { e.preventDefault(); if (flat.length) { cursor = (cursor + 1) % flat.length; highlight(); } }
        else if (e.key === 'ArrowUp') { e.preventDefault(); if (flat.length) { cursor = (cursor - 1 + flat.length) % flat.length; highlight(); } }
        else if (e.key === 'Enter') { e.preventDefault(); open(cursor, e); }
    }

    function open(idx, event) {
        const it = flat[idx];
        if (!it) return;
        close();
        if (event && (event.ctrlKey || event.metaKey)) { window.open(it.url, '_blank'); return; }
        if (window.Nav) Nav.openPath(it.url, null);
        else window.location.href = it.url;
    }

    function show(initial) {
        ensureDom();
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

    return { show, close, open, isOpen };
})();
