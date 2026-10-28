/**
 * YARA workbench: /yara
 *
 * Left: the rule editor (rulezet-editor when /vendor/rulezet-editor is fetched,
 * a plain textarea otherwise). Right: the target and negative file sets and the
 * live result. A pause in typing posts the rule and the sets to /api/yara/test,
 * which scans the stored bytes and answers with precision / recall, per-group
 * and per-string hit counts, and where the hits land.
 *
 * A set row is `label` + a search (`tag=family:mirai&bin_cluster_uuid=...`, the
 * same params as the file search) or `files=<coll>:<md5>,...`; a negative row
 * can also be "everything else". Everything shown came off an uploaded sample,
 * so every value goes through escapeHtml / escapeAttr.
 */

const YARA_DEBOUNCE_MS = 800;
const YARA_DEFAULT_RULE = `rule family_name
{
    meta:
        description = ""
    strings:
        $s1 = "" ascii wide
    condition:
        any of them
}
`;

window.YaraView = {
    _editor: null,
    _textarea: null,
    _timer: null,
    _seq: 0,
    _uuid: null,
    _run: null,
    _runTimer: null,
    _sets: { targets: [], negatives: [] },
    _last: null,

    destroy() {
        clearTimeout(this._timer);
        clearTimeout(this._runTimer);
        if (this._editor && this._editor.destroy) this._editor.destroy();
        this._editor = this._textarea = null;
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;
        this._params = params;
        this._uuid = params.rule || null;
        this._last = null;
        this._loadState(params);
        container.innerHTML = this._shell();
        await this._mountEditor(params);
        await this._loadCollections(params.collection);
        this._renderSets();
        this._loadRules();
        if (params.seed_md5 && params.seed_addr) this.insertFunction(params.seed_collection || params.collection, params.seed_md5, params.seed_addr);
        this.schedule(0);
    },

    // --- state ------------------------------------------------------------

    _loadState(params) {
        let saved = {};
        try { saved = JSON.parse(localStorage.getItem('bsimvis.yara') || '{}'); } catch (e) { /* storage is a convenience */ }
        const fromUrl = k => (params[k] ? [{ label: k === 'target' ? 'Target' : 'Negative', query: params[k] }] : null);
        this._sets.targets = fromUrl('target') || saved.targets || [{ label: 'Family', query: '' }];
        this._sets.negatives = fromUrl('negative') || saved.negatives || [{ label: 'Everything else', rest: true }];
        // from an N-way table: its saved column set, one group per side
        if (params.col_set) {
            const grp = (g, label) => ({ label, query: `col_set=${params.col_set}&group=${g}` });
            this._sets.targets = [grp(params.target_group || 0, 'Target group')];
            this._sets.negatives = params.negative_group !== undefined
                ? [grp(params.negative_group, 'Negative group')]
                : [{ label: 'Everything else', rest: true }];
        }
        this._savedRule = saved.rule;
    },

    _persist() {
        try {
            localStorage.setItem('bsimvis.yara', JSON.stringify({ ...this._sets, rule: this.getRule() }));
        } catch (e) { /* ignore */ }
    },

    getRule() {
        if (this._editor) return this._editor.getValue();
        return this._textarea ? this._textarea.value : '';
    },

    setRule(text) {
        if (this._editor) this._editor.setValue(text);
        else if (this._textarea) this._textarea.value = text;
        this.schedule(0);
    },

    insertText(text) {
        if (this._editor && this._editor.insertText) this._editor.insertText(text);
        else if (this._textarea) this._textarea.value += `\n${text}`;
        this.schedule(0);
    },

    // --- layout -----------------------------------------------------------

    _shell() {
        const card = 'background:var(--hover); border:1px solid var(--border); border-radius:8px;';
        return `
        <div style="flex:1; display:grid; grid-template-columns:minmax(380px, 1fr) minmax(420px, 1.1fr); gap:14px; padding:14px 18px; min-height:0; color:var(--text);">
            <div style="display:flex; flex-direction:column; gap:10px; min-height:0;">
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                    <h2 style="margin:0; color:var(--accent); font-size:1.2rem;"><i class="fa-solid fa-shield-halved"></i> YARA workbench</h2>
                    <span id="yara-status" style="color:var(--subtle); font-size:0.78rem;"></span>
                    <span style="flex:1;"></span>
                    <select id="yara-rules" onchange="YaraView.openRule(this.value)" style="background:var(--bg); border:1px solid var(--border); color:var(--text); border-radius:4px; padding:4px; max-width:200px;"><option value="">Saved rules...</option></select>
                </div>
                <div id="yara-editor" style="flex:1; min-height:300px; border:1px solid var(--border); border-radius:6px; overflow:hidden;"></div>
                <div style="display:flex; gap:6px; flex-wrap:wrap; font-size:0.8rem;">
                    <button class="btn" onclick="YaraView.save()"><i class="fa-solid fa-floppy-disk"></i> Save version</button>
                    <button class="btn" onclick="YaraView.exportYar()"><i class="fa-solid fa-download"></i> .yar</button>
                    <button class="btn" onclick="YaraView.toVendored()" title="Write to data/yara_rules/custom for review"><i class="fa-solid fa-folder-plus"></i> To ruleset</button>
                    <button class="btn" onclick="YaraView.startRun('sweep')" title="Scan every file of the collection: exact numbers"><i class="fa-solid fa-magnifying-glass-chart"></i> Full sweep</button>
                    <button class="btn" onclick="YaraView.startRun('apply')" title="Tag matching files yara:custom:<rule> (needs a saved rule)"><i class="fa-solid fa-tags"></i> Apply tags</button>
                    <button class="btn" onclick="YaraView.suggest()"><i class="fa-solid fa-wand-magic-sparkles"></i> Suggest strings</button>
                    <button class="btn" onclick="YaraView.draft()"><i class="fa-solid fa-robot"></i> LLM draft</button>
                </div>
                <div id="yara-run" style="font-size:0.8rem; color:var(--subtle);"></div>
                <div id="yara-seed" style="${card} padding:8px 10px; font-size:0.78rem; display:flex; gap:6px; align-items:center; flex-wrap:wrap;">
                    <span style="color:var(--subtle);">Function bytes</span>
                    <input id="yara-seed-md5" placeholder="file md5" style="width:250px; background:var(--bg); border:1px solid var(--border); color:var(--text); padding:3px 6px; border-radius:4px;">
                    <input id="yara-seed-addr" placeholder="entry (hex)" style="width:100px; background:var(--bg); border:1px solid var(--border); color:var(--text); padding:3px 6px; border-radius:4px;">
                    <button class="btn" onclick="YaraView.seedFromInputs()">Insert</button>
                </div>
            </div>
            <div style="display:flex; flex-direction:column; gap:10px; overflow-y:auto; min-height:0;">
                <div style="${card} padding:10px 12px;">
                    <div style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
                        <span style="color:var(--subtle); font-size:0.78rem;">Collection</span>
                        <select id="yara-collection" onchange="YaraView.schedule(0)" style="background:var(--bg); border:1px solid var(--border); color:var(--text); border-radius:4px; padding:4px;"></select>
                    </div>
                    <div id="yara-sets"></div>
                </div>
                <div id="yara-result" style="display:flex; flex-direction:column; gap:10px;"></div>
            </div>
        </div>`;
    },

    async _mountEditor(params) {
        const host = document.getElementById('yara-editor');
        const initial = params.rule ? '' : (this._savedRule || YARA_DEFAULT_RULE);
        const lib = await this._loadLib();
        if (lib) {
            const adapter = new (class extends lib.HostAdapter {
                name = 'BSimVis';
                async validate(language, text) {
                    const res = await (await fetch('/api/yara/validate', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rule: text }),
                    })).json();
                    return (res.errors || []).map(e => ({ severity: 'error', message: e.message, line: e.line }));
                }
            })();
            this._editor = new lib.RulezetEditor(host, { language: 'yara', value: initial, host: adapter });
            this._editor.on('change', () => this.schedule());
        } else {
            host.innerHTML = '<textarea id="yara-textarea" spellcheck="false" style="width:100%; height:100%; box-sizing:border-box; background:var(--bg); color:var(--text); border:0; padding:10px; font-family:monospace; font-size:0.85rem; resize:none;"></textarea>';
            this._textarea = document.getElementById('yara-textarea');
            this._textarea.value = initial;
            this._textarea.addEventListener('input', () => this.schedule());
            this._status('plain editor (run scripts/fetch_vendor_assets.sh for rulezet-editor)');
        }
        if (params.rule) await this.openRule(params.rule);
    },

    // The editor is fetched, not vendored in git (AGPL): a missing file is fine.
    _loadLib() {
        if (window.RulezetEditorLib) return Promise.resolve(window.RulezetEditorLib);
        return new Promise(resolve => {
            const css = document.createElement('link');
            css.rel = 'stylesheet';
            css.href = '/vendor/rulezet-editor/rulezet-editor.css';
            document.head.appendChild(css);
            const js = document.createElement('script');
            js.src = '/vendor/rulezet-editor/rulezet-editor.iife.js';
            js.onload = () => resolve(window.RulezetEditorLib || null);
            js.onerror = () => resolve(null);
            document.head.appendChild(js);
        });
    },

    async _loadCollections(preselect) {
        const sel = document.getElementById('yara-collection');
        let cols = [];
        try {
            const res = await (await fetch('/api/collection/search?limit=1000')).json();
            cols = (res.collections || (Array.isArray(res) ? res : [])).map(c => c.name).filter(Boolean);
        } catch (e) { /* empty list shows the problem */ }
        let want = preselect;
        try { want = want || localStorage.getItem('bsimvis.yara.collection'); } catch (e) { /* ignore */ }
        sel.innerHTML = cols.map(c => `<option value="${escapeAttr(c)}" ${c === want ? 'selected' : ''}>${escapeHtml(c)}</option>`).join('');
    },

    // --- sets -------------------------------------------------------------

    _renderSets() {
        const box = document.getElementById('yara-sets');
        const block = (key, title, color) => `
            <div style="margin-bottom:8px;">
                <div style="color:${color}; font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:1px; margin:4px 0;">${title}
                    <a href="#" onclick="YaraView.addSet('${key}'); return false;" style="margin-left:8px; font-weight:400;">+ add</a></div>
                ${this._sets[key].map((s, i) => `
                <div style="display:flex; gap:6px; margin-bottom:4px; align-items:center;">
                    <input value="${escapeAttr(s.label || '')}" onchange="YaraView.editSet('${key}', ${i}, 'label', this.value)" style="width:110px; background:var(--bg); border:1px solid var(--border); color:var(--text); padding:3px 6px; border-radius:4px; font-size:0.78rem;">
                    ${s.rest
                        ? '<span style="flex:1; color:var(--subtle); font-size:0.78rem;">every file of the collection not in a target</span>'
                        : `<input value="${escapeAttr(s.query || '')}" placeholder="tag=family:mirai&bin_cluster_uuid=...  |  files=coll:md5,coll:md5" onchange="YaraView.editSet('${key}', ${i}, 'query', this.value)" style="flex:1; background:var(--bg); border:1px solid var(--border); color:var(--text); padding:3px 6px; border-radius:4px; font-size:0.78rem; font-family:monospace;">`}
                    <a href="#" title="remove" onclick="YaraView.removeSet('${key}', ${i}); return false;" style="color:var(--subtle);"><i class="fa-solid fa-xmark"></i></a>
                </div>`).join('')}
            </div>`;
        box.innerHTML = block('targets', 'Must match', '#10b981') + block('negatives', 'Must not match', '#ef4444');
    },

    addSet(key) {
        this._sets[key].push({ label: `${key === 'targets' ? 'Target' : 'Negative'} ${this._sets[key].length + 1}`, query: '' });
        this._renderSets();
    },

    removeSet(key, i) {
        this._sets[key].splice(i, 1);
        this._renderSets();
        this.schedule(0);
    },

    editSet(key, i, field, value) {
        this._sets[key][i][field] = value;
        this.schedule(0);
    },

    // `a=1&b=2&tag=x&tag=y` -> {a:'1', b:'2', tag:['x','y']}; `files=` is a list of ids
    _spec(set) {
        if (set.rest) return { label: set.label, rest: true };
        const q = {};
        for (const [k, v] of new URLSearchParams(set.query || '')) {
            if (k === 'files') return { label: set.label, files: v.split(',').map(x => x.trim()).filter(Boolean) };
            q[k] = k in q ? [].concat(q[k], v) : v;
        }
        if (q.col_set) return { label: set.label, col_set: q.col_set, group: q.group };
        return { label: set.label, queries: [q] };
    },

    _body() {
        const spec = list => list.filter(s => s.rest || (s.query || '').trim()).map(s => this._spec(s));
        return {
            rule: this.getRule(),
            collection: document.getElementById('yara-collection').value,
            targets: spec(this._sets.targets),
            negatives: spec(this._sets.negatives),
        };
    },

    // --- live test --------------------------------------------------------

    schedule(delay = YARA_DEBOUNCE_MS) {
        clearTimeout(this._timer);
        this._timer = setTimeout(() => this.test(), delay);
    },

    _status(text) {
        const el = document.getElementById('yara-status');
        if (el) el.textContent = text;
    },

    async test() {
        const body = this._body();
        this._persist();
        try { localStorage.setItem('bsimvis.yara.collection', body.collection); } catch (e) { /* ignore */ }
        if (!body.collection || !body.targets.length) {
            this._renderResult(null, 'Give at least one target set a search (e.g. tag=family:mirai).');
            return;
        }
        const seq = ++this._seq;
        this._status('scanning...');
        let res;
        try {
            const r = await fetch('/api/yara/test', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            res = await r.json();
            if (!r.ok) res = { ok: false, errors: [], error: res.message || res.error || `HTTP ${r.status}` };
        } catch (e) {
            res = { ok: false, errors: [], error: String(e) };
        }
        if (seq !== this._seq) return; // a newer keystroke owns the panel
        this._last = res;
        this._status(res.ok ? `${res.ms} ms` : '');
        this._renderResult(res);
    },

    _pct(v) {
        return v === null || v === undefined ? '&mdash;' : `${(v * 100).toFixed(1)}%`;
    },

    _fileLink(f) {
        const [coll, md5] = [f.id.slice(0, f.id.lastIndexOf(':')), f.id.slice(f.id.lastIndexOf(':') + 1)];
        return `<a href="/collections/${encodeURIComponent(coll)}/files/${md5}" onclick="Nav.openPath(this.href, event)">${escapeHtml(f.name)}</a>`;
    },

    _renderResult(res, hint) {
        const el = document.getElementById('yara-result');
        if (!res) { el.innerHTML = `<div style="color:var(--subtle); font-size:0.85rem; padding:10px;">${escapeHtml(hint || '')}</div>`; return; }
        if (res.error) { el.innerHTML = `<div style="color:#ef4444; padding:10px;">${escapeHtml(res.error)}</div>`; return; }
        if (!res.ok) {
            el.innerHTML = `<div style="color:#ef4444; font-family:monospace; font-size:0.8rem; padding:10px;">${(res.errors || []).map(e => `line ${e.line}: ${escapeHtml(e.message)}`).join('<br>')}</div>`;
            return;
        }
        const m = res.metrics, s = res.sample;
        const card = (label, value, color) => `<div style="flex:1; min-width:78px; background:var(--hover); border:1px solid var(--border); border-radius:8px; padding:8px 10px; text-align:center;">
            <div style="color:var(--subtle); font-size:0.68rem; text-transform:uppercase;">${label}</div>
            <div style="font-size:1.25rem; font-weight:700; color:${color || 'var(--text)'};">${value}</div></div>`;
        const th = 'text-align:left; color:var(--subtle); font-weight:600; padding:3px 6px; font-size:0.72rem;';
        const td = 'padding:3px 6px; font-size:0.78rem;';
        const tbl = (head, rows) => `<table style="width:100%; border-collapse:collapse;"><thead><tr>${head.map(h => `<th style="${th}">${h}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table>`;
        const sec = (title, body) => `<div style="background:var(--hover); border:1px solid var(--border); border-radius:8px; padding:8px 10px;"><div style="color:var(--accent); font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">${title}</div>${body}</div>`;

        const groups = tbl(['Group', 'Role', 'Files', 'Hits', 'Rate'], res.groups.map(g => `<tr>
            <td style="${td}">${escapeHtml(g.label)}</td>
            <td style="${td} color:${g.role === 'target' ? '#10b981' : '#ef4444'};">${escapeHtml(g.role)}</td>
            <td style="${td}">${g.files}</td><td style="${td}">${g.hits}</td><td style="${td}">${this._pct(g.hit_rate)}</td></tr>`));

        const land = res.landing || {};
        const strings = res.strings.length ? tbl(['String', 'Targets', 'Others', 'Lands in'], res.strings.map(x => {
            const l = land[`${x.rule}:${x.id}`] || { functions: [], strings: [] };
            const where = [
                ...l.functions.map(f => `<a href="/collections/${encodeURIComponent(f.example.slice(0, f.example.lastIndexOf(':')))}/files/${f.example.slice(f.example.lastIndexOf(':') + 1)}/functions/${escapeAttr(f.addr)}" onclick="Nav.openPath(this.href, event)">${escapeHtml(f.name)}</a> <span style="color:var(--subtle);">&times;${f.files}</span>`),
                ...l.strings.map(t => `str &ldquo;${escapeHtml(t.value)}&rdquo; <span style="color:var(--subtle);">&times;${t.files}</span>`),
            ];
            const dead = x.pos === 0 && x.neg === 0;
            const noisy = x.neg > 0;
            return `<tr><td style="${td} font-family:monospace;">${escapeHtml(res.rules.length > 1 ? `${x.rule} ` : '')}${escapeHtml(x.id)}${dead ? ' <span style="color:#f59e0b;">dead</span>' : ''}</td>
                <td style="${td}">${x.pos}/${s.targets}</td><td style="${td} color:${noisy ? '#ef4444' : 'inherit'};">${x.neg}</td>
                <td style="${td}">${where.join('<br>') || '<span style="color:var(--subtle);">&mdash;</span>'}${l.unmapped ? ` <span style="color:var(--subtle);">(${l.unmapped} unmapped)</span>` : ''}</td></tr>`;
        })) : '<div style="color:var(--subtle); font-size:0.78rem;">no strings</div>';

        const list = (rows, extra) => rows.length
            ? `<ul style="margin:0; padding-left:18px; font-size:0.78rem;">${rows.map(f => `<li>${this._fileLink(f)}${extra ? extra(f) : ''}</li>`).join('')}</ul>`
            : '<div style="color:var(--subtle); font-size:0.78rem;">none</div>';

        el.innerHTML = `
            <div style="display:flex; gap:8px; flex-wrap:wrap;">
                ${card('Precision', this._pct(m.precision), m.precision === 1 ? '#10b981' : '')}
                ${card('Recall', this._pct(m.recall), m.recall === 1 ? '#10b981' : '')}
                ${card('F1', this._pct(m.f1))}
                ${card('TP / FN', `${m.tp} / ${m.fn}`)}
                ${card('FP / TN', `${m.fp} / ${m.tn}`, m.fp ? '#ef4444' : '')}
            </div>
            <div style="color:var(--subtle); font-size:0.74rem;">
                ${s.sampled ? `<b style="color:#f59e0b;">sampled</b>: ${s.negatives_scanned} of ${s.negatives_available} negatives &mdash; use Full sweep for exact numbers.` : `${s.negatives_scanned} negatives scanned.`}
                ${s.targets_truncated ? ' Targets truncated.' : ''}${s.unreadable ? ` ${s.unreadable} files have no stored bytes.` : ''}
            </div>
            ${sec('Groups', groups)}
            ${sec('Strings', strings)}
            ${sec(`Missed targets (${m.fn})`, list(res.missed))}
            ${sec(`False hits (${m.fp})`, list(res.false_hits, f => ` <span style="color:var(--subtle);">${escapeHtml(f.rules.join(', '))}</span>`))}`;
    },

    // --- seeds ------------------------------------------------------------

    async insertFunction(collection, md5, addr) {
        const q = new URLSearchParams({ collection, md5, addr });
        const r = await fetch(`/api/yara/seed/function?${q}`);
        const res = await r.json();
        if (!r.ok) { this._status(res.message || res.error || 'seed failed'); return; }
        const note = res.approximate ? ' // approximate length: no CFG stored' : '';
        this.insertText(`// $fn_${addr} from ${md5.slice(0, 8)}${note}\n// $fn_${addr} = ${res.hex}`);
    },

    seedFromInputs() {
        const md5 = document.getElementById('yara-seed-md5').value.trim();
        const addr = document.getElementById('yara-seed-addr').value.trim().replace(/^0x/, '');
        const coll = document.getElementById('yara-collection').value;
        if (md5 && addr) this.insertFunction(coll, md5, addr);
    },

    async suggest() {
        const body = { ...this._body(), min_frac: 0.8 };
        this._status('ranking strings...');
        const r = await fetch('/api/yara/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const res = await r.json();
        this._status('');
        const el = document.getElementById('yara-result');
        const rows = (res.strings || []).map((x, i) => `<tr>
            <td style="padding:3px 6px; font-family:monospace; font-size:0.78rem;">${escapeHtml(x.value)}</td>
            <td style="padding:3px 6px;">${x.pos}/${x.of}</td><td style="padding:3px 6px; color:${x.neg ? '#ef4444' : 'inherit'};">${x.neg}</td>
            <td><a href="#" onclick="YaraView.addString(${i}); return false;">insert</a></td></tr>`).join('');
        this._suggested = res.strings || [];
        el.insertAdjacentHTML('afterbegin', `<div style="background:var(--hover); border:1px solid var(--border); border-radius:8px; padding:8px 10px;">
            <div style="color:var(--accent); font-size:0.72rem; font-weight:700; text-transform:uppercase;">Suggested strings (in &ge;80% of targets)</div>
            ${rows ? `<table style="width:100%;">${rows}</table>` : '<div style="color:var(--subtle); font-size:0.8rem;">nothing shared by 80% of the target files</div>'}</div>`);
    },

    addString(i) {
        const v = this._suggested[i].value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        this.insertText(`// $s = "${v}" ascii wide`);
    },

    async draft() {
        this._status('asking the model...');
        const r = await fetch('/api/yara/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...this._body(), name: (this._sets.targets[0] || {}).label || 'family' }) });
        const res = await r.json();
        this._status('');
        if (!r.ok) { this._status(res.message || res.error || 'draft failed'); return; }
        this.setRule(res.rule);
    },

    // --- registry ---------------------------------------------------------

    async _loadRules() {
        const sel = document.getElementById('yara-rules');
        try {
            const res = await (await fetch('/api/yara/rules')).json();
            sel.innerHTML = '<option value="">Saved rules...</option>' + (res.rules || []).map(r =>
                `<option value="${escapeAttr(r.uuid)}" ${r.uuid === this._uuid ? 'selected' : ''}>${escapeHtml(r.name)} v${r.version}</option>`).join('');
        } catch (e) { /* the list is a convenience */ }
    },

    async openRule(uuid) {
        if (!uuid) return;
        const r = await fetch(`/api/yara/rules/${encodeURIComponent(uuid)}`);
        if (!r.ok) return;
        const doc = await r.json();
        this._uuid = doc.uuid;
        if (doc.scope && doc.scope.targets) {
            this._sets = { targets: doc.scope.targets, negatives: doc.scope.negatives };
            this._renderSets();
        }
        this.setRule(doc.text);
        this._status(`${doc.name} v${doc.version}`);
    },

    async save() {
        const body = { rule: this.getRule(), uuid: this._uuid, scope: { ...this._sets }, note: '' };
        const r = await fetch('/api/yara/rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const res = await r.json();
        if (!r.ok) { this._status(res.message || res.error || 'save failed'); return; }
        this._uuid = res.uuid;
        this._status(`saved ${res.name} v${res.version} (${res.uuid.slice(0, 8)})`);
        this._loadRules();
    },

    async exportYar() {
        if (!this._uuid) await this.save();
        if (!this._uuid) return;
        const res = await (await fetch(`/api/yara/rules/${this._uuid}/export`)).json();
        const a = document.createElement('a');
        a.href = URL.createObjectURL(new Blob([res.content], { type: 'text/plain' }));
        a.download = res.filename;
        a.click();
        URL.revokeObjectURL(a.href);
    },

    async toVendored() {
        if (!this._uuid) await this.save();
        if (!this._uuid) return;
        const r = await fetch(`/api/yara/rules/${this._uuid}/export`, { method: 'POST' });
        const res = await r.json();
        this._status(r.ok ? `written: ${res.path}` : (res.message || 'export failed'));
    },

    // --- sweep / apply jobs -----------------------------------------------

    async startRun(mode) {
        if (mode === 'apply') {
            await this.save(); // apply tags what is saved, so save first
            if (!this._uuid) return;
        }
        const body = { ...this._body(), uuid: this._uuid };
        const r = await fetch(`/api/yara/${mode}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const res = await r.json();
        if (!r.ok) { this._status(res.message || res.error || `${mode} failed`); return; }
        this._run = res.run_id;
        this._pollRun();
    },

    async _pollRun() {
        const el = document.getElementById('yara-run');
        if (!this._run || !el) return;
        const res = await (await fetch(`/api/yara/run/${this._run}`)).json();
        if (res.done) {
            el.innerHTML = `${escapeHtml(res.mode)} done: ${res.seen} files, ${res.hits} hit` +
                (res.mode === 'apply' ? `, ${res.tagged} retagged` : '') +
                (res.precision !== undefined ? ` &mdash; <b>exact</b> precision ${this._pct(res.precision)}, recall ${this._pct(res.recall)} (FP ${res.fp}, FN ${res.fn})` : '');
            return;
        }
        el.textContent = `${res.mode || 'run'}: ${res.seen || 0} files scanned, ${res.hits || 0} hit...`;
        this._runTimer = setTimeout(() => this._pollRun(), 2000);
    },
};
