/**
 * File Strings View: a file's Ghidra-extracted strings, filtered / sorted /
 * paged server-side (/api/strings/file), with YARA + tag badges and a click
 * pivot to the functions sharing a string.
 */
window.FileStringsView = {
    container: null,
    params: null,
    state: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        const md5 = params.md5 || params.file_md5;
        if (!md5 || !params.collection) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: Missing collection or file MD5.</div>';
            return;
        }
        const qs = new URLSearchParams(window.location.search);
        this.state = {
            q: qs.get('q') || '', encoding: '', section: '', tag: qs.get('tag') || '',
            yara: qs.get('yara') === 'true', referenced: '', minLength: '',
            sortBy: 'addr', sortOrder: 'asc', offset: 0, limit: 100,
        };
        this.container.innerHTML = `
            <style>
                .fs-table { width:100%; border-collapse:collapse; font-size:0.8rem; }
                .fs-table th { text-align:left; padding:8px 10px; border-bottom:2px solid var(--border); color:var(--accent); font-size:0.7rem; text-transform:uppercase; position:sticky; top:0; background:var(--window-tray); cursor:pointer; white-space:nowrap; }
                .fs-table td { padding:6px 10px; border-bottom:1px solid var(--border); vertical-align:top; }
                .fs-table tr:hover { background:var(--hover); }
                .fs-filters { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-bottom:10px; font-size:0.75rem; }
                .fs-filters input[type=text], .fs-filters input[type=number], .fs-filters select { font-size:0.75rem; padding:4px 6px; }
            </style>
            <div style="display:flex; flex-direction:column; height:100%; overflow:hidden; padding-bottom:20px;">
                <div style="border-bottom:1px solid var(--border); padding-bottom:12px; margin-bottom:12px;">
                    <h1 style="color:var(--accent); margin:0; font-size:1.2rem;">Strings</h1>
                    <div style="margin-top:6px; font-size:0.85rem;" class="dim">File <span class="mono" id="fs-md5"></span>
                        <span id="fs-count" class="badge" style="margin-left:10px;">0 strings</span></div>
                </div>
                <div class="fs-filters">
                    <input type="text" id="fs-q" placeholder="Contains (use * for glob)..." style="min-width:220px;">
                    <select id="fs-encoding"><option value="">any encoding</option><option>ascii</option><option>utf-8</option><option>utf-16</option><option>utf-32</option></select>
                    <input type="text" id="fs-section" placeholder="Section..." style="width:90px;">
                    <input type="text" id="fs-tag" placeholder="Tag..." style="width:120px;">
                    <input type="number" id="fs-minlen" placeholder="Min len" min="0" style="width:80px;">
                    <select id="fs-referenced"><option value="">referenced or not</option><option value="true">referenced by a function</option><option value="false">unreferenced</option></select>
                    <label><input type="checkbox" id="fs-yara"> YARA hits only</label>
                </div>
                <div style="flex:1; overflow:auto; background:var(--card-bg); border:1px solid var(--border); border-radius:4px;">
                    <table class="fs-table"><thead><tr>
                        <th data-sort="addr">Address</th><th data-sort="value">Value</th><th data-sort="encoding">Enc</th>
                        <th data-sort="section">Section</th><th data-sort="length">Len</th><th data-sort="refs">Refs</th><th>YARA / Tags</th>
                    </tr></thead><tbody id="fs-body"></tbody></table>
                </div>
                <div style="display:flex; gap:10px; align-items:center; margin-top:10px; font-size:0.8rem;">
                    <button class="btn" id="fs-prev">&laquo; Prev</button>
                    <span id="fs-page" class="dim"></span>
                    <button class="btn" id="fs-next">Next &raquo;</button>
                </div>
            </div>`;
        const $ = id => this.container.querySelector(id);
        $('#fs-md5').textContent = md5;
        $('#fs-q').value = this.state.q;
        $('#fs-tag').value = this.state.tag;
        $('#fs-yara').checked = this.state.yara;

        let timer = null;
        const refilter = () => {
            clearTimeout(timer);
            timer = setTimeout(() => {
                Object.assign(this.state, {
                    q: $('#fs-q').value.trim(), encoding: $('#fs-encoding').value, section: $('#fs-section').value.trim(),
                    tag: $('#fs-tag').value.trim(), minLength: $('#fs-minlen').value, referenced: $('#fs-referenced').value,
                    yara: $('#fs-yara').checked, offset: 0,
                });
                this.load();
            }, 250);
        };
        this.container.querySelectorAll('.fs-filters input, .fs-filters select').forEach(el => {
            el.addEventListener('input', refilter);
            el.addEventListener('change', refilter);
        });
        this.container.querySelectorAll('th[data-sort]').forEach(th => th.addEventListener('click', () => {
            const key = th.dataset.sort;
            this.state.sortOrder = (this.state.sortBy === key && this.state.sortOrder === 'asc') ? 'desc' : 'asc';
            this.state.sortBy = key;
            this.state.offset = 0;
            this.load();
        }));
        $('#fs-prev').addEventListener('click', () => { this.state.offset = Math.max(0, this.state.offset - this.state.limit); this.load(); });
        $('#fs-next').addEventListener('click', () => { this.state.offset += this.state.limit; this.load(); });

        try {
            const res = await fetch(`/api/file/search?collection=${encodeURIComponent(params.collection)}&file_md5=${encodeURIComponent(md5)}&limit=1`);
            const f = ((await res.json()).files || [])[0];
            if (f && window.Breadcrumbs) { Breadcrumbs.setFilename(md5, f.file_name || md5); Breadcrumbs.refresh(); }
        } catch (e) { /* breadcrumb label only */ }
        await this.load();
    },

    async load() {
        const s = this.state, md5 = this.params.md5 || this.params.file_md5;
        const qs = new URLSearchParams({
            collection: this.params.collection, md5, sort_by: s.sortBy, sort_order: s.sortOrder,
            offset: s.offset, limit: s.limit,
        });
        if (s.q) qs.set('q', s.q);
        if (s.encoding) qs.set('encoding', s.encoding);
        if (s.section) qs.set('section', s.section);
        if (s.tag) qs.set('tag', s.tag);
        if (s.minLength) qs.set('min_length', s.minLength);
        if (s.referenced) qs.set('referenced', s.referenced);
        if (s.yara) qs.set('yara', 'true');
        const body = this.container.querySelector('#fs-body');
        try {
            const res = await fetch(`/api/strings/file?${qs}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'Failed to load strings');
            this.rules = data.rules || {};
            this.render(data);
        } catch (err) {
            body.innerHTML = `<tr><td colspan="7" style="text-align:center; padding:40px; color:#f92672;">${escapeHtml(err.message)}</td></tr>`;
        }
    },

    ctx() {
        return { collection: this.params.collection, pool: this.params.pool, md5: this.params.md5 || this.params.file_md5, viewName: 'FileStringsView' };
    },

    render(data) {
        const body = this.container.querySelector('#fs-body');
        const ctx = this.ctx();
        this.container.querySelector('#fs-count').textContent = `${data.total} strings`;
        const last = Math.min(data.offset + data.limit, data.total);
        this.container.querySelector('#fs-page').textContent = data.total ? `${data.offset + 1}-${last} of ${data.total}` : '0';
        this.container.querySelector('#fs-prev').disabled = data.offset === 0;
        this.container.querySelector('#fs-next').disabled = last >= data.total;
        if (!data.strings.length) {
            body.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:40px;" class="dim">No strings. Only analyses run after the strings feature store them.</td></tr>';
            return;
        }
        body.innerHTML = data.strings.map(s => `<tr>
            <td class="mono dim">${escapeHtml(s.addr)}</td>
            <td>${StringsUI.valueCell(s, ctx)}</td>
            <td><span class="badge">${escapeHtml(s.encoding)}</span></td>
            <td class="dim">${escapeHtml(s.section || '')}</td>
            <td>${escapeHtml(s.length)}</td>
            <td>${escapeHtml((s.funcs || []).length)}</td>
            <td>${StringsUI.tagCell(s, this.rules, ctx)}</td></tr>`).join('');
    },

    async tagString(addr, tag, action) {
        try {
            await StringsUI.postTag(this.params.collection, this.params.md5 || this.params.file_md5, addr, tag, action);
            await this.load();
        } catch (err) { alert(err.message); }
    },

    promptTag(addr) {
        const tag = prompt('Tag for this string:');
        if (tag && tag.trim()) this.tagString(addr, tag.trim(), 'add');
    },
};
