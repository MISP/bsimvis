// External intel for one file: hash links (no API call) and the MalwareBazaar /
// MWDB lookup cards. A lookup sends the hash to a third party, so it runs only on
// an explicit click; opening the page reads the server's cache and nothing else.
window.ExtIntel = (() => {
    const NAMES = { mb: 'MalwareBazaar', mwdb: 'MWDB' };
    let mwdbUrl = 'https://mwdb.cert.pl';

    const sha256Of = (file) =>
        (file.sha256 || (file.file_format || {})['Executable SHA256'] || '').toLowerCase();

    function links(file) {
        const md5 = file.file_md5, sha = sha256Of(file), h = sha || md5;
        return [
            { key: 'mb', label: 'MB', title: 'MalwareBazaar',
              url: sha ? `https://bazaar.abuse.ch/sample/${sha}/`
                       : `https://bazaar.abuse.ch/browse.php?search=md5%3A${md5}` },
            { key: 'mwdb', label: 'MWDB', title: 'MWDB', url: `${mwdbUrl}/file/${h}` },
            { key: 'vt', label: 'VT', title: 'VirusTotal', url: `https://www.virustotal.com/gui/file/${h}` },
        ];
    }

    // no-referrer: the BsimVis URL (collection, md5) must not reach the site.
    function linksHtml(file) {
        return links(file).map(l =>
            `<a class="badge ext-link" href="${escapeAttr(l.url)}" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer"
                title="Open on ${escapeAttr(l.title)} (your browser sends the hash to it)"
                style="font-size:0.7rem; text-decoration:none;">${l.label} <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.6rem;"></i></a>`
        ).join(' ');
    }

    async function copy(text) {
        try { await navigator.clipboard.writeText(text); window.showToast?.('Copied', 'success'); } catch (e) { /* no clipboard */ }
    }

    function hashesHtml(file) {
        const sha = sha256Of(file);
        const one = (label, v) => v
            ? `<span class="ext-hash" title="${label} ${escapeAttr(v)} (click to copy)" data-copy="${escapeAttr(v)}"
                 style="cursor:copy;">${label} ${escapeHtml(v.slice(0, 12))}…</span>` : '';
        return [one('MD5', file.file_md5), one('SHA256', sha)].filter(Boolean).join(' &middot; ');
    }

    function bindCopy(root) {
        root.querySelectorAll('[data-copy]').forEach(el => { el.onclick = () => copy(el.dataset.copy); });
    }

    const when = (ms) => ms ? new Date(ms).toLocaleString() : '';

    function cardHtml(source, card, configured, collection, md5) {
        const name = NAMES[source];
        const btn = configured
            ? `<button class="top-action-btn" data-ext-lookup="${source}"
                 title="Sends this file's hash to ${name}. Do not use on TLP:RED samples.">
                 <i class="fa-solid fa-satellite-dish"></i> ${card ? 'Look up again' : `Look up on ${name}`}</button>`
            : `<span class="dim" style="font-size:0.78rem;">Not configured: set ${source === 'mb' ? 'MB_API_KEY' : 'MWDB_API_KEY'} in the server's .env</span>`;
        let body;
        if (!card) {
            body = `<div class="dim" style="font-size:0.8rem;">Not looked up yet.</div>`;
        } else if (card.error) {
            body = `<div style="color:var(--danger, #f92672); font-size:0.8rem;">${escapeHtml(card.error)}</div>`;
        } else if (!card.found) {
            body = `<div class="dim" style="font-size:0.8rem;">Not found on ${name}.</div>`;
        } else {
            const tags = (card.tags || []).map(t => `<span class="badge" style="font-size:0.7rem;">${escapeHtml(source)}:${escapeHtml(t)}</span>`).join(' ');
            const rows = [
                ['Family', card.family], ['Names', (card.names || []).join(', ')], ['Type', card.file_type],
                ['First seen', card.first_seen], ['Last seen', card.last_seen],
            ].filter(([, v]) => v).map(([k, v]) =>
                `<tr><td class="bin-sim-mc-label">${k}</td><td>${escapeHtml(String(v))}</td></tr>`).join('');
            body = `<table class="bin-sim-mc-table"><tbody>${rows}</tbody></table>
                <div style="display:flex; gap:4px; flex-wrap:wrap; margin:10px 0;">${tags || '<span class="dim">No tags.</span>'}</div>
                <div style="display:flex; gap:8px; align-items:center;">
                    ${card.tags?.length ? `<button class="top-action-btn" data-ext-import="${source}" title="Write these tags on the file as ${source}:&lt;tag&gt;. No network call."><i class="fa-solid fa-tags"></i> Import as tags</button>` : ''}
                    ${card.url ? `<a href="${escapeAttr(card.url)}" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer" style="font-size:0.8rem;">Open on ${name} <i class="fa-solid fa-arrow-up-right-from-square"></i></a>` : ''}
                </div>`;
        }
        return `<div class="card" style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:16px; flex:1; min-width:280px;">
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <b style="color:var(--accent);">${name}</b>
                <span class="dim" style="font-size:0.75rem;">${card ? 'fetched ' + escapeHtml(when(card.fetched_at)) : ''}</span>
                <span style="margin-left:auto;">${btn}</span>
            </div>${body}</div>`;
    }

    async function renderPanel(el, collection, file) {
        const md5 = file.file_md5;
        const api = `/api/file/${encodeURIComponent(md5)}/ext`;
        el.innerHTML = `<div class="dim" style="padding:12px;"><i class="fa-solid fa-spinner fa-spin"></i></div>`;
        let st;
        try {
            const res = await fetch(`${api}?collection=${encodeURIComponent(collection)}`);
            st = await res.json();
            if (!res.ok) throw new Error(st.error || res.status);
        } catch (e) {
            el.innerHTML = `<div style="padding:12px; color:var(--danger, #f92672);">${escapeHtml(String(e.message || e))}</div>`;
            return;
        }
        if (st.mwdb_url) mwdbUrl = st.mwdb_url;
        const draw = () => {
            el.innerHTML = `
                <div style="font-size:0.8rem; color:var(--subtle); margin-bottom:12px; display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                    <span>${hashesHtml(file)}</span><span>${linksHtml(file)}</span>
                </div>
                <div style="font-size:0.78rem; color:var(--subtle); margin-bottom:12px;">
                    <i class="fa-solid fa-triangle-exclamation"></i> A lookup sends this file's hash to the third party. Nothing is sent unless you click.
                </div>
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    ${['mb', 'mwdb'].map(s => cardHtml(s, st.cards[s], st.configured[s], collection, md5)).join('')}
                </div>`;
            bindCopy(el);
            el.querySelectorAll('[data-ext-lookup]').forEach(b => b.onclick = () => post('lookup', b));
            el.querySelectorAll('[data-ext-import]').forEach(b => b.onclick = () => post('import', b));
        };
        const post = async (what, b) => {
            const source = b.dataset.extLookup || b.dataset.extImport;
            b.disabled = true;
            b.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
            try {
                const res = await fetch(`${api}/${what}`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ collection, source }),
                });
                const j = await res.json();
                if (!res.ok) throw new Error(j.error || res.status);
                if (what === 'lookup') st.cards[source] = j.card;
                else window.showToast?.(`${j.added.length} tag(s) imported`, 'success');
            } catch (e) {
                window.showToast?.(String(e.message || e), 'error');
            }
            draw();
        };
        draw();
    }

    // Bulk: an explicit lookup of the files a query matches, as one EXT_LOOKUP job.
    async function bulkLookup(collection) {
        const q = window.prompt('Look up the files matching this file search (e.g. tag:recent, name:x). Empty = every file.', '');
        if (q === null) return;
        const params = new URLSearchParams({ collection, q, limit: 1000, bin_clusters: 'false' });
        const res = await fetch(`/api/file/search?${params}`);
        const md5s = res.ok ? ((await res.json()).files || []).map(f => f.file_md5).filter(Boolean) : [];
        if (!md5s.length) { window.showToast?.('No file matches', 'info'); return; }
        if (!window.confirm(`Send ${md5s.length} hash(es) to MalwareBazaar and MWDB (where configured)? Do not use on TLP:RED samples.`)) return;
        const importTags = window.confirm('Also import the tags found as mb:<tag> / mwdb:<tag>?');
        const r = await fetch('/api/file/ext/lookup_bulk', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, md5s, sources: ['mb', 'mwdb'], import_tags: importTags }),
        });
        const j = await r.json();
        window.showToast?.(r.ok ? `Lookup job queued: ${md5s.length} files on ${j.sources.join(', ')}` : (j.error || 'Failed'), r.ok ? 'success' : 'error');
    }

    return { links, linksHtml, hashesHtml, bindCopy, renderPanel, bulkLookup, sha256Of };
})();
