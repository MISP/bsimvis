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
    const A = (l, cls, style, inner) =>
        `<a class="${cls}" href="${escapeAttr(l.url)}" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer"
            title="Open on ${escapeAttr(l.title)} (your browser sends the hash to it)" style="text-decoration:none; ${style}">${inner}</a>`;

    // Split button like the Filters one: VirusTotal by default, the rest under the chevron.
    function linksHtml(file) {
        const all = links(file), vt = all.find(l => l.key === 'vt');
        const rest = all.filter(l => l.key !== 'vt').map(l =>
            A(l, 'history-item', 'display:flex; align-items:center; gap:8px; font-size:0.75rem; color:var(--text);',
              `<i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.8rem;"></i> Open on ${escapeHtml(l.title)}`)).join('');
        return `<div class="filter-actions-container">
            ${A(vt, 'top-action-btn', 'border-top-right-radius:0; border-bottom-right-radius:0;',
                '<i class="fa-solid fa-arrow-up-right-from-square"></i> Open on VirusTotal')}
            <button class="top-action-btn" onclick="ExtIntel.toggleMenu(event)" title="Other sites"
                style="border-left:none; border-top-left-radius:0; border-bottom-left-radius:0; padding:0 7px;">
                <i class="fa-solid fa-chevron-down" style="font-size:0.6rem;"></i></button>
            <div class="history-dropdown view-history-dropdown ext-open-dd" style="display:none; width:auto; min-width:200px; right:0; left:auto;">
                <div class="history-dropdown-title">Open on</div>${rest}
            </div></div>`;
    }

    function toggleMenu(event) {
        event.stopPropagation();
        const dd = event.currentTarget.parentElement.querySelector('.ext-open-dd');
        const open = dd.style.display === 'none';
        dd.style.display = open ? 'block' : 'none';
        if (open) {
            const close = (e) => { if (!dd.contains(e.target)) { dd.style.display = 'none'; document.removeEventListener('click', close); } };
            setTimeout(() => document.addEventListener('click', close), 0);
        }
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

    // Card dates are "YYYY-MM-DD[ HH:MM:SS]" UTC; ours are epoch ms.
    function toMs(v) {
        if (!v) return 0;
        if (typeof v === 'number' || /^\d+$/.test(v)) return +v < 1e12 ? +v * 1000 : +v;
        let t = String(v).trim().replace(' ', 'T');
        if (/^\d{4}-\d\d-\d\d$/.test(t)) t += 'T00:00:00';
        if (!/(Z|[+-]\d\d:?\d\d)$/.test(t)) t += 'Z';
        const ms = Date.parse(t);
        return isNaN(ms) ? 0 : ms;
    }
    const day = (ms) => ms ? new Date(ms).toISOString().slice(0, 16).replace('T', ' ') : '';
    const span = (d) => {
        d = Math.abs(d) / 1000;
        return d < 3600 ? `${Math.round(d / 60)} min` : d < 86400 ? `${Math.round(d / 3600)} h`
            : d < 86400 * 60 ? `${Math.round(d / 86400)} d` : `${Math.round(d / 86400 / 30.4)} mo`;
    };
    const ago = (ms) => ms ? `${span(Date.now() - ms)} ago` : '';

    const note = (txt) => `<div class="dim" style="font-size:0.8rem; padding:6px 0;">${txt}</div>`;

    // One chip per card tag, in the tag's own colour: imported ones are solid with
    // a check, the rest toggle in or out of the next import.
    function chipHtml(source, t, imported, picked) {
        const tag = `${source}:${t}`;
        const color = window.tagInk ? window.tagInk(window.getTagMetadata(tag).color) : 'var(--accent)';
        const a = (p) => window.tagAlpha ? window.tagAlpha(color, p) : 'transparent';
        if (imported) {
            return `<span class="sim-tag-card" title="Already on this file"
                style="color:${color}; border-color:${a(40)}; background:${a(12)}; padding:2px 8px;">
                <i class="fa-solid fa-check" style="font-size:0.65rem;"></i>${escapeHtml(tag)}</span>`;
        }
        return `<button type="button" class="sim-tag-card" data-ext-pick="${escapeAttr(source)}" data-tag="${escapeAttr(t)}"
            aria-pressed="${picked}" title="${picked ? 'Picked for import (click to drop)' : 'Click to pick for import'}"
            style="cursor:pointer; font:inherit; font-size:0.7rem; padding:2px 8px; color:${color};
                   border:1px ${picked ? 'solid' : 'dashed'} ${a(picked ? 55 : 35)}; background:${picked ? a(14) : 'transparent'};
                   opacity:${picked ? 1 : 0.65};">
            <i class="fa-${picked ? 'solid fa-square-check' : 'regular fa-square'}" style="font-size:0.7rem;"></i>${escapeHtml(tag)}</button>`;
    }

    function cardHtml(source, card, configured, have, picks) {
        const name = NAMES[source];
        const btn = configured
            ? `<button class="top-action-btn" data-ext-lookup="${source}"
                 title="Sends this file's hash to ${name}. Do not use on TLP:RED samples.">
                 <i class="fa-solid fa-satellite-dish"></i> ${card ? 'Look up again' : 'Look up'}</button>`
            : '';
        let body;
        if (!configured && !card) {
            body = note(`<i class="fa-solid fa-plug-circle-xmark"></i> Not configured: set <code>${source === 'mb' ? 'MB_API_KEY' : 'MWDB_API_KEY'}</code> in the server's .env.`);
        } else if (!card) {
            body = note(`Not looked up yet. Nothing has been sent to ${name}.`);
        } else if (card.error) {
            body = `<div style="color:var(--danger, #f92672); font-size:0.8rem;"><i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(card.error)}</div>`;
        } else if (!card.found) {
            body = note(`<i class="fa-solid fa-circle-question"></i> Not found on ${name}.`);
        } else {
            const tags = card.tags || [];
            const todo = tags.filter(t => !have.has(`${source}:${t}`));
            const nPick = todo.filter(t => picks.has(t)).length;
            const rows = [
                ['Names', (card.names || []).join(', ')], ['Type', card.file_type],
            ].filter(([, v]) => v).map(([k, v]) =>
                `<tr><td class="bin-sim-mc-label">${k}</td><td>${escapeHtml(String(v))}</td></tr>`).join('');
            body = `
                ${card.family ? `<div style="margin-bottom:8px;"><span class="badge" style="font-size:0.8rem; padding:3px 10px;" title="Family">
                    <i class="fa-solid fa-dna"></i> ${escapeHtml(card.family)}</span></div>` : ''}
                ${rows ? `<table class="bin-sim-mc-table"><tbody>${rows}</tbody></table>` : ''}
                <div style="display:flex; align-items:center; gap:8px; margin:12px 0 6px;">
                    <span class="history-dropdown-title" style="padding:0;">Tags</span>
                    ${todo.length > 1 ? `<a href="#" data-ext-all="${source}" style="font-size:0.72rem;">${nPick === todo.length ? 'None' : 'All'}</a>` : ''}
                    <span class="dim" style="font-size:0.72rem; margin-left:auto;">${tags.length - todo.length}/${tags.length} on this file</span>
                </div>
                <div style="display:flex; gap:4px; flex-wrap:wrap;">
                    ${tags.map(t => chipHtml(source, t, have.has(`${source}:${t}`), picks.has(t))).join('') || '<span class="dim" style="font-size:0.8rem;">No tags.</span>'}
                </div>
                <div style="display:flex; gap:8px; align-items:center; margin-top:12px;">
                    ${todo.length ? `<button class="top-action-btn" data-ext-import="${source}" ${nPick ? '' : 'disabled'}
                        title="Writes the picked tags on the file as ${source}:&lt;tag&gt;. No network call.">
                        <i class="fa-solid fa-tags"></i> Import ${nPick} tag${nPick === 1 ? '' : 's'}</button>` : ''}
                    ${card.url ? `<a href="${escapeAttr(card.url)}" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer" style="font-size:0.8rem; margin-left:auto;">Open on ${name} <i class="fa-solid fa-arrow-up-right-from-square"></i></a>` : ''}
                </div>`;
        }
        return `<div class="card" style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:16px; flex:1; min-width:280px;">
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <b style="color:var(--accent);">${name}</b>
                ${card ? `<span class="dim" style="font-size:0.75rem;" title="${escapeAttr(new Date(card.fetched_at).toLocaleString())}">fetched ${escapeHtml(ago(card.fetched_at))}</span>` : ''}
                <span style="margin-left:auto;">${btn}</span>
            </div>${body}</div>`;
    }

    const DATES = [['first_seen', 'First seen', Math.min], ['last_seen', 'Last seen', Math.max]];

    // Ours vs each source: the best value (oldest first seen, latest last seen) is
    // starred; any value can be written on the file, or both best ones at once.
    function datesHtml(file, cards) {
        const srcs = ['mb', 'mwdb'].filter(s => cards[s]?.found);
        if (!srcs.some(s => cards[s].first_seen || cards[s].last_seen)) return '';
        const best = {};
        const rows = DATES.map(([f, label, pick]) => {
            const ours = toMs(file[f]);
            const vals = srcs.map(s => toMs(cards[s][f]));
            const all = [ours, ...vals].filter(Boolean);
            best[f] = all.length ? pick(...all) : 0;
            const cell = (v, own) => {
                if (!v) return `<td class="dim">-</td>`;
                const top = v === best[f] && all.length > 1;
                const diff = !own && ours && v !== ours
                    ? `<span class="dim" style="font-size:0.72rem;"> ${span(v - ours)} ${v < ours ? 'earlier' : 'later'}</span>` : '';
                const use = !own && v !== ours
                    ? ` <button class="top-action-btn" data-ext-date="${f}" data-ms="${v}" style="padding:0 6px; height:20px; font-size:0.7rem;" title="Write this ${label.toLowerCase()} on the file">Use</button>` : '';
                return `<td style="white-space:nowrap;${top ? ' color:var(--success); font-weight:600;' : ''}"
                    title="${top ? (f === 'first_seen' ? 'Oldest' : 'Most recent') : ''}">${top ? '<i class="fa-solid fa-star" style="font-size:0.6rem;"></i> ' : ''}${escapeHtml(day(v))}${diff}${use}</td>`;
            };
            return `<tr><td class="bin-sim-mc-label">${label}</td>${cell(ours, true)}${vals.map(v => cell(v, false)).join('')}</tr>`;
        }).join('');
        const todo = DATES.filter(([f]) => best[f] && best[f] !== toMs(file[f]));
        return `<div class="card" style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:16px; margin-bottom:14px;">
            <div style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                <b style="color:var(--accent);">Dates</b>
                <span class="dim" style="font-size:0.75rem;">UTC. <i class="fa-solid fa-star" style="font-size:0.6rem;"></i> = oldest first seen / latest last seen</span>
                ${todo.length ? `<button class="top-action-btn" data-ext-best style="margin-left:auto;"
                    data-ms="${escapeAttr(JSON.stringify(Object.fromEntries(todo.map(([f]) => [f, best[f]]))))}"
                    title="Overwrite ours with the starred ${escapeAttr(todo.map(([, l]) => l.toLowerCase()).join(' and '))}">
                    <i class="fa-solid fa-wand-magic-sparkles"></i> Use best dates</button>` : ''}
            </div>
            <table class="bin-sim-mc-table"><thead><tr><th></th><th>Ours</th>${srcs.map(s => `<th>${NAMES[s]}</th>`).join('')}</tr></thead>
            <tbody>${rows}</tbody></table></div>`;
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
        const have = () => new Set(file.user_tags || []);
        // Every not-yet-imported tag starts picked; a click drops it.
        const picks = {};
        const resetPicks = (s) => { picks[s] = new Set((st.cards[s]?.tags || []).filter(t => !have().has(`${s}:${t}`))); };
        ['mb', 'mwdb'].forEach(resetPicks);
        const draw = () => {
            el.innerHTML = `
                <div style="font-size:0.8rem; color:var(--subtle); margin-bottom:12px; display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
                    <span>${hashesHtml(file)}</span><span>${linksHtml(file)}</span>
                </div>
                <div style="font-size:0.78rem; color:var(--subtle); margin-bottom:12px;">
                    <i class="fa-solid fa-triangle-exclamation"></i> A lookup sends this file's hash to the third party. Nothing is sent unless you click.
                </div>
                ${datesHtml(file, st.cards)}
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    ${['mb', 'mwdb'].map(s => cardHtml(s, st.cards[s], st.configured[s], have(), picks[s])).join('')}
                </div>`;
            bindCopy(el);
            el.querySelectorAll('[data-ext-lookup]').forEach(b => b.onclick = () => post('lookup', b));
            el.querySelectorAll('[data-ext-import]').forEach(b => b.onclick = () => post('import', b));
            el.querySelectorAll('[data-ext-pick]').forEach(b => b.onclick = () => {
                const p = picks[b.dataset.extPick];
                if (p.has(b.dataset.tag)) p.delete(b.dataset.tag); else p.add(b.dataset.tag);
                draw();
            });
            el.querySelectorAll('[data-ext-all]').forEach(a => a.onclick = (e) => {
                e.preventDefault();
                const s = a.dataset.extAll;
                if (a.textContent === 'None') picks[s].clear(); else resetPicks(s);
                draw();
            });
            el.querySelectorAll('[data-ext-date]').forEach(b => b.onclick = () =>
                saveDates(b, { [b.dataset.extDate]: +b.dataset.ms }));
            el.querySelectorAll('[data-ext-best]').forEach(b => b.onclick = () =>
                saveDates(b, JSON.parse(b.dataset.ms)));
        };
        const busy = (b) => { b.disabled = true; b.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; };
        // Same propagation job the metadata CSV uses: file meta, indexes, functions.
        const saveDates = async (b, upd) => {
            busy(b);
            try {
                const res = await fetch('/api/file/metadata/propagate', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ collection, updates: { [md5]: upd } }),
                });
                const j = await res.json();
                if (!res.ok) throw new Error(j.error || res.status);
                Object.assign(file, upd);
                window.showToast?.('Dates saved, propagating to functions', 'success');
            } catch (e) {
                window.showToast?.(String(e.message || e), 'error');
            }
            draw();
        };
        const post = async (what, b) => {
            const source = b.dataset.extLookup || b.dataset.extImport;
            busy(b);
            try {
                const body = { collection, source };
                if (what === 'import') body.tags = [...picks[source]];
                const res = await fetch(`${api}/${what}`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                });
                const j = await res.json();
                if (!res.ok) throw new Error(j.error || res.status);
                if (what === 'lookup') { st.cards[source] = j.card; resetPicks(source); }
                else {
                    file.user_tags = [...new Set([...(file.user_tags || []), ...j.added])];
                    picks[source].clear();
                    window.showToast?.(`${j.added.length} tag(s) imported`, 'success');
                }
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

    return { links, linksHtml, toggleMenu, hashesHtml, bindCopy, renderPanel, bulkLookup, sha256Of };
})();
