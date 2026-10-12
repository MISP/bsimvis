/**
 * Shared string-table rendering: row cells, YARA / tag badges, the
 * "functions sharing this string" pivot dialog and analyst tagging.
 * Used by the file Strings view and the function Strings tab.
 */
window.StringsUI = {
    // Every value below came off an uploaded sample: escape at each interpolation.
    chip(tag, opts = {}) {
        const color = window.TagColor ? TagColor.forTag(tag) : 'var(--accent)';
        const remove = opts.onRemove
            ? ` <span style="cursor:pointer;" onclick="${escapeAttr(opts.onRemove)}" title="Remove tag">&times;</span>`
            : '';
        return `<span class="badge" style="background:color-mix(in srgb, ${color} 18%, transparent); color:${color}; border:1px solid ${color}; margin:0 3px 3px 0; font-size:0.65rem;">${escapeHtml(tag)}${remove}</span>`;
    },

    yaraBadges(s, rules) {
        return (s.rules || []).map(id => {
            const r = (rules || {})[id] || {};
            const tip = [r.name || id, r.author ? `by ${r.author}` : '', r.url || ''].filter(Boolean).join(' | ');
            return `<span class="badge" title="${escapeAttr(tip)}" style="background:#f92672; color:var(--window-tray); font-size:0.65rem; margin:0 3px 3px 0;"><i class="fa-solid fa-shield-halved"></i> ${escapeHtml(r.name || id)}</span>`;
        }).join('');
    },

    // ctx: { collection, pool, md5, viewName }
    tagCell(s, rules, ctx) {
        const analyzer = (s.tags || []).map(t => this.chip(t)).join('');
        const user = (s.user_tags || []).map(t => this.chip(t, {
            onRemove: `${ctx.viewName}.tagString(${jsString(s.addr)}, ${jsString(t)}, 'remove')`,
        })).join('');
        const add = `<span class="dim" style="cursor:pointer; font-size:0.7rem;" title="Add tag" onclick="${escapeAttr(`${ctx.viewName}.promptTag(${jsString(s.addr)})`)}"><i class="fa-solid fa-plus"></i></span>`;
        return this.yaraBadges(s, rules) + analyzer + user + add;
    },

    valueCell(s, ctx) {
        const shown = s.value.length > 200 ? s.value.slice(0, 200) + '…' : s.value;
        const click = `StringsUI.pivot(${jsString(ctx.collection)}, ${jsString(ctx.pool || '')}, ${jsString(s.id)}, ${jsString(s.value)})`;
        return `<span class="mono" style="cursor:pointer; color:#e6db74; white-space:pre-wrap; word-break:break-all;" title="${escapeAttr(s.value)}" onclick="${escapeAttr(click)}">${escapeHtml(shown)}</span>`;
    },

    // Functions (and files) sharing a string, as an overlay.
    async pivot(collection, pool, id, value) {
        document.getElementById('strings-pivot')?.remove();
        const box = document.createElement('div');
        box.id = 'strings-pivot';
        box.style.cssText = 'position:fixed; inset:0; z-index:30000; background:rgba(0,0,0,0.55); display:flex; align-items:center; justify-content:center;';
        box.onclick = e => { if (e.target === box) box.remove(); };
        box.innerHTML = `<div style="background:var(--window-bg); border:1px solid var(--accent); border-radius:6px; width:min(820px,92vw); max-height:80vh; display:flex; flex-direction:column;">
            <div style="padding:12px 16px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; gap:12px;">
                <div><div class="dim" style="font-size:0.7rem; text-transform:uppercase;">Functions using string</div>
                <div class="mono" style="color:#e6db74; word-break:break-all;">${escapeHtml(value.slice(0, 300))}</div></div>
                <span style="cursor:pointer;" onclick="document.getElementById('strings-pivot').remove()">&times;</span>
            </div>
            <div id="strings-pivot-body" style="padding:12px 16px; overflow:auto;"><i class="fa-solid fa-spinner fa-spin"></i> Loading...</div></div>`;
        document.body.appendChild(box);

        const qs = new URLSearchParams({ id, limit: '200' });
        if (pool) qs.set('pool', pool); else qs.set('collection', collection);
        const body = box.querySelector('#strings-pivot-body');
        try {
            const res = await fetch(`/api/strings/functions?${qs}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'Request failed');
            const rows = data.functions.map(f => {
                const url = `/collections/${encodeURIComponent(f.collection)}/files/${encodeURIComponent(f.file_md5)}/functions/${encodeURIComponent(f.entrypoint_address)}`;
                return `<tr><td><a href="${escapeAttr(url)}" onclick="document.getElementById('strings-pivot').remove(); Nav.openPath(this.href, event)">${escapeHtml(f.function_name || f.entrypoint_address)}</a></td>
                    <td class="mono dim">${escapeHtml(f.entrypoint_address)}</td>
                    <td>${escapeHtml(f.file_name || f.file_md5)}</td><td class="dim">${escapeHtml(f.collection)}</td></tr>`;
            }).join('');
            const searchUrl = Nav.buildUIUrl(pool ? '' : collection, ['search', 'functions']) + `?strings=${encodeURIComponent(value.slice(0, 128))}`;
            body.innerHTML = `<div class="dim" style="margin-bottom:8px;">${data.total} function(s) in ${data.files.length} file(s)
                · <a href="${escapeAttr(searchUrl)}" onclick="Nav.openPath(this.href, event)">open in function search</a></div>
                ${rows ? `<table class="file-func-table"><thead><tr><th>Function</th><th>Address</th><th>File</th><th>Collection</th></tr></thead><tbody>${rows}</tbody></table>` : '<div class="dim">No function references this string.</div>'}`;
        } catch (err) {
            body.innerHTML = `<span style="color:#f92672;">${escapeHtml(err.message)}</span>`;
        }
    },

    async postTag(collection, md5, addr, tag, action) {
        const res = await fetch('/api/strings/tag', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, md5, addr, tag, action }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || 'Tagging failed');
        return data.string;
    },
};
