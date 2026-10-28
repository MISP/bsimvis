/**
 * YARA workbench: /yara
 *
 * Left: the rule editor (rulezet-editor when /vendor/rulezet-editor is fetched,
 * a plain textarea otherwise). Right: the target and negative file sets and the
 * live result. A pause in typing posts the rule and the sets to /api/yara/test,
 * which scans the stored bytes and answers with precision / recall, per-group
 * and per-string hit counts, and where the hits land.
 *
 * The file sets are the N-way Compare form (nway_compare.js): the first group
 * must match, the others must not; an empty negative group is everything else.
 * Everything shown came off an uploaded sample, so every value goes through
 * escapeHtml / escapeAttr.
 */

const YARA_DEBOUNCE_MS = 800;
const YARA_DEFAULT_RULE = `rule family_name
{
    meta:
        description = ""
    strings:
        $s1 = "CHANGE_ME" ascii wide
    condition:
        any of them
}
`;

window.YaraView = {
    _editor: null,
    _textarea: null,
    _timer: null,
    _seq: 0,
    _tab: 'strings', // result tab
    _dtab: 'groups', // rule data tab
    _suggested: null,
    _suggKey: '',
    _res: null,
    _hint: '',
    _view: 'live', // 'live' sample test | 'sweep' full-collection run
    _sweep: null,
    _prog: null,
    _uuid: null,
    _run: null,
    _runTimer: null,
    _cmp: null,
    _last: null,

    destroy() {
        clearTimeout(this._timer);
        clearTimeout(this._runTimer);
        this._busy(false);
        if (this._editor && this._editor.destroy) this._editor.destroy();
        this._editor = this._textarea = null;
        document.removeEventListener('click', this._docClick);
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;
        this._params = params;
        this._uuid = params.rule || null;
        this._last = null;
        this._loadState(params);
        container.innerHTML = this._shell();
        this._docClick = e => { const m = document.getElementById('yara-menu'); if (m && !m.contains(e.target)) m.open = false; };
        document.addEventListener('click', this._docClick);
        await this._mountEditor(params);
        await this._loadCollections(params.collection);
        this._mountSets();
        this._loadRules();
        this.schedule(0);
    },

    // --- state ------------------------------------------------------------

    // The sets are a Compare form (nway_compare.js): group A must match, every other group must not.
    // An empty negative group means everything else in the collection.
    _loadState(params) {
        let saved = {};
        try { saved = JSON.parse(localStorage.getItem('bsimvis.yara') || '{}'); } catch (e) { /* storage is a convenience */ }
        let cmp = saved.cmp || null;
        try { if (params.cmp) cmp = JSON.parse(params.cmp); } catch (e) { /* bad link: use the saved sets */ }
        if (params.target) cmp = { groups: [{ label: 'Must match', queries: [params.target] }, { label: 'Must not match', queries: params.negative ? [params.negative] : [] }] };
        this._savedCmp = cmp;
        this._savedRule = saved.rule;
        this._rest = saved.rest !== false;
    },

    _persist() {
        try {
            localStorage.setItem('bsimvis.yara', JSON.stringify({ cmp: this._cmp.cmp(), rule: this.getRule(), rest: this._rest }));
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
        return `
        <div class="yw">
            <div class="yw-bar">
                <h2 class="yw-title"><i class="fa-solid fa-shield-halved"></i> YARA workbench</h2>
                <select id="yara-rules" aria-label="Saved rules" onchange="YaraView.openRule(this.value)" style="max-width:220px;"><option value="">Saved rules...</option></select>
                <span class="yw-status" id="yara-status"></span>
                <span class="yw-sp"></span>
                <button class="yw-btn primary" onclick="YaraView.save()"><i class="fa-solid fa-floppy-disk"></i> Save version</button>
                <details class="yw-menu" id="yara-menu">
                    <summary class="yw-btn icon" aria-label="More actions"><i class="fa-solid fa-ellipsis"></i></summary>
                    <div class="yw-pop">
                        <button onclick="YaraView.exportYar()"><i class="fa-solid fa-download"></i> Export .yar</button>
                        <button onclick="YaraView.toVendored()" title="Write to data/yara_rules/custom for review"><i class="fa-solid fa-folder-plus"></i> Add to ruleset</button>
                        <button onclick="YaraView.startRun('apply')" title="Tag matching files yara:custom:<rule> (saves first)"><i class="fa-solid fa-tags"></i> Apply tags to matches</button>
                    </div>
                </details>
            </div>
            <details class="yw-sets" id="yara-setbar" open>
                <summary><span class="yw-lbl">Test sets</span><span class="yw-sum" id="yara-setsum"></span></summary>
                <div class="yw-sets-body">
                    <div class="yw-sets-row"><span class="yw-lbl">Collection</span><select id="yara-collection" aria-label="Collection" onchange="YaraView._setsChanged()"></select>
                        <span class="yw-sp"></span>
                        <label class="yw-check" title="Without a Must not match filter, test the rule against every other file of the collection (a sample). Off: only recall is measured."><input type="checkbox" id="yara-rest" onchange="YaraView._restChanged(this.checked)"> Test against every other file</label></div>
                    <div class="nway-focus-body nway-match-body nway-cmp-body" id="yara-sets" style="position:static; box-shadow:none; border:0; background:transparent; padding:0; width:100%; max-width:none; max-height:none; min-width:0; align-items:stretch;"></div>
                </div>
            </details>
            <div class="yw-main">
                <div class="yw-left">
                    <div class="yw-editor" id="yara-editor"></div>
                    <div class="yw-tools">
                        <button class="yw-btn" onclick="YaraView.draft()" title="Ask the model to draft a rule from the Must match files"><i class="fa-solid fa-robot"></i> Draft with LLM</button>
                    </div>
                </div>
                <div class="yw-right" id="yara-result"></div>
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

    _mountSets() {
        const box = document.getElementById('yara-sets');
        this._cmp = new window.NwayCompare({
            scope: () => ({ collection: document.getElementById('yara-collection').value }),
            saved: () => this._savedCmp,
            labels: ['Must match', 'Must not match'],
            runLabel: 'Test',
            onPreview: counts => { this._sum(counts); this.schedule(); },
            onRun: () => this.schedule(0),
        });
        box.innerHTML = this._cmp.html();
        this._cmp.bind(box);
        document.getElementById('yara-rest').checked = this._rest;
        this._cmp._previewSoon();
        const hasTarget = this._savedCmp && this._savedCmp.groups[0] && this._savedCmp.groups[0].queries.length;
        if (hasTarget) document.getElementById('yara-setbar').open = false;
        this._sum(null);
    },

    // One line for the collapsed bar: each group's filters and file count.
    _sum(counts) {
        const el = document.getElementById('yara-setsum');
        if (!el) return;
        const groups = this._cmp.cmp().groups;
        el.innerHTML = groups.map((g, i) => {
            const n = counts && counts.groups[i] && g.queries.length ? ` (${Number(counts.groups[i].files)})` : '';
            const what = g.queries.length ? g.queries.map(q => escapeHtml(q)).join(' or ') : (i ? 'everything else' : 'no filter yet');
            return `<b>${escapeHtml(g.label)}</b> ${what}${n}`;
        }).join(' &middot; ');
    },

    _setsChanged() {
        this._cmp.counts = null;
        this._cmp.refresh();
        this._cmp._previewSoon();
        this.schedule(0);
    },

    _restChanged(on) {
        this._rest = on;
        this.schedule(0);
    },

    // Must match: its filters. Must not match: its filters, else every other file (if ticked), else nothing.
    _body() {
        const cmp = this._cmp.cmp();
        const spec = g => ({ label: g.label, queries: g.queries.map(q => window.NwayCompare.parseQuery(q)) });
        const targets = cmp.groups[0].queries.length ? [spec(cmp.groups[0])] : [];
        const negatives = cmp.groups.slice(1).map(g => g.queries.length ? spec(g) : (this._rest && targets.length ? { label: g.label, rest: true } : null)).filter(Boolean);
        return { rule: this.getRule(), collection: document.getElementById('yara-collection').value, targets, negatives };
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

    // The app's progress card (bottom right, as in the N-way view). `fraction` null = unknown.
    _busy(on, label, fraction, right) {
        let b = document.getElementById('yara-busy');
        if (!on) { if (b) b.remove(); return; }
        if (!b) {
            b = document.createElement('div');
            b.id = 'yara-busy';
            b.className = 'nway-busy';
            b.innerHTML = '<div class="nway-busy-head"><i class="fa-solid fa-spinner fa-spin"></i> <span class="yb-l"></span></div><div class="nway-bar-track"><div class="nway-bar-fill"></div></div><div class="nway-busy-foot"><span></span><span class="nway-busy-r yb-r"></span></div>';
            document.body.appendChild(b);
        }
        b.querySelector('.yb-l').textContent = label;
        b.querySelector('.yb-r').textContent = right || '';
        const fill = b.querySelector('.nway-bar-fill');
        fill.classList.toggle('indet', !fraction);
        fill.style.width = fraction ? `${Math.max(2, Math.round(fraction * 100))}%` : '';
    },

    async test() {
        const body = this._body();
        this._persist();
        try { localStorage.setItem('bsimvis.yara.collection', body.collection); } catch (e) { /* ignore */ }
        const seq = ++this._seq;
        if (!body.collection) { this._renderResult(null, 'Pick a collection.'); return; }
        if (!body.targets.length) {
            // nothing to measure against: still show compile errors
            const v = await (await fetch('/api/yara/validate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rule: body.rule }) })).json().catch(() => ({}));
            if (seq !== this._seq) return;
            this._renderResult(v.errors && v.errors.length ? { ok: false, errors: v.errors } : null,
                'No "Must match" set yet. Add a filter to measure precision and recall, or open Full sweep to see every file this rule hits.');
            this._suggested = [];
            this._suggKey = '';
            this._syncSweep();
            return;
        }
        this._status('scanning...');
        this._loadSuggest(body);
        clearTimeout(this._busyT);
        this._busyT = setTimeout(() => { if (!this._run) this._busy(true, 'Testing the rule'); }, 500);
        let res;
        try {
            const r = await fetch('/api/yara/test', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            res = await r.json();
            if (!r.ok) res = { ok: false, errors: [], error: res.message || res.error || `HTTP ${r.status}` };
        } catch (e) {
            res = { ok: false, errors: [], error: String(e) };
        }
        if (seq !== this._seq) return; // a newer keystroke owns the panel
        clearTimeout(this._busyT);
        if (!this._run) this._busy(false);
        this._last = res;
        this._status(res.ok ? `${res.ms} ms` : '');
        this._renderResult(res);
        this._syncSweep();
    },

    _pct(v) {
        return v === null || v === undefined ? '&mdash;' : `${(v * 100).toFixed(1)}%`;
    },

    _renderResult(res, hint) {
        this._res = res;
        this._hint = hint || '';
        this._paint();
    },

    _errHtml(res) {
        if (!res || res.ok) return '';
        if (res.error) return `<div class="yw-err">${escapeHtml(res.error)}</div>`;
        return `<div class="yw-err">${(res.errors || []).map(e => `line ${e.line}: ${escapeHtml(e.message)}`).join('<br>')}</div>`;
    },

    // Two cards: the data (groups, suggested strings), then the results of a Sample test or a Full sweep.
    // A rule that does not compile shows its error and "no matching results"; the layout stays.
    _paint() {
        const el = document.getElementById('yara-result');
        if (!el) return;
        const seg = `<div class="yw-seg" role="group" aria-label="Result source">
            <button aria-pressed="${this._view === 'live'}" onclick="YaraView.setView('live')">Sample test</button>
            <button aria-pressed="${this._view === 'sweep'}" onclick="YaraView.setView('sweep')">Full sweep</button></div>`;
        const live = this._res && this._res.ok ? this._res : null;
        const dtabs = [['groups', 'Groups', live ? live.groups.length : ''], ['suggest', 'Suggested', this._suggested ? this._suggested.length : '']];
        const data = `<section class="yw-card"><h3 class="yw-h">Data</h3>${this._tabsHtml(dtabs, this._dtab, true)}<div class="yw-pane">${this._dataPane(live)}</div></section>`;
        const none = `<div class="yw-hint">${escapeHtml(this._hint || 'No matching results')}</div>`;
        let body;
        if (this._view === 'sweep') body = this._run ? this._progressHtml() : (this._sweep ? this._resultHtml(this._sweep) : none);
        else body = live ? this._resultHtml(live) : none;
        el.innerHTML = this._errHtml(this._res) + data + `<section class="yw-card"><h3 class="yw-h">Results ${seg}</h3>${body}</section>`;
    },

    _tabsHtml(tabs, cur, data) {
        return `<div class="yw-tabs" role="tablist">${tabs.map(([k, l, c, red]) => `<button class="yw-tab" role="tab" aria-selected="${cur === k}" onclick="YaraView.setTab('${k}', ${!!data})">${l}${c !== '' ? `<em class="${red && c ? 'red' : ''}">${c}</em>` : ''}</button>`).join('')}</div>`;
    },

    setView(v) {
        this._view = v;
        this._paint();
        this._syncSweep();
    },

    // While Full sweep is selected, keep it current: rerun when the rule or the sets changed.
    _syncSweep() {
        if (this._view !== 'sweep' || this._run || (this._res && !this._res.ok)) return;
        const body = this._body();
        if (body.collection && JSON.stringify(body) !== this._sweepKey) this.startRun('sweep');
    },

    _progressHtml() {
        const p = this._prog || {};
        const f = p.total ? Math.min(1, p.seen / p.total) : 0;
        return `<div class="yw-prog"><div class="nway-busy-head"><i class="fa-solid fa-spinner fa-spin"></i> ${p.mode === 'apply' ? 'Tagging files' : 'Sweeping the collection'}</div>
            <div class="nway-bar-track"><div class="nway-bar-fill${f ? '' : ' indet'}" style="${f ? `width:${Math.max(2, Math.round(f * 100))}%` : ''}"></div></div>
            <div class="yw-dim" style="margin-top:6px;">${Number(p.seen || 0)}${p.total ? ` of ${Number(p.total)}` : ''} files scanned &middot; ${Number(p.hits || 0)} hit</div></div>`;
    },

    // Metrics, note, tabs and pane for one result: a sample test or a finished sweep (res.exact).
    _resultHtml(res) {
        const err = this._errHtml(res);
        if (err) return err;
        const m = res.metrics, s = res.sample;
        const good = v => (v === 1 ? ' good' : '');
        let head, note, tabs;
        if (res.exact) {
            head = m ? `<div class="yw-m"><small>Precision</small><strong class="${good(m.precision)}">${this._pct(m.precision)}</strong></div>
                <div class="yw-m"><small>Recall</small><strong class="${good(m.recall)}">${this._pct(m.recall)}</strong></div>
                <div class="yw-m"><small>F1</small><strong>${this._pct(m.f1)}</strong></div>` :
                `<div class="yw-m"><small>Matches</small><strong>${res.hits}</strong></div>
                <div class="yw-m"><small>Scanned</small><strong>${res.seen}</strong></div>
                <div class="yw-m"><small>Rate</small><strong>${this._pct(res.seen ? res.hits / res.seen : null)}</strong></div>`;
            note = `<b>Exact:</b> all ${res.seen} files scanned${res.mode === 'apply' ? `, ${res.tagged} retagged` : ''}.${m ? '' : ' No Must match set, so no precision or recall.'}`;
            tabs = m ? [['missed', 'Missed', m.fn], ['false', 'False hits', m.fp, true]] : [['hits', 'Matches', res.hits]];
        } else {
            head = `<div class="yw-m"><small>Precision</small><strong class="${good(m.precision)}">${this._pct(m.precision)}</strong></div>
                <div class="yw-m"><small>Recall</small><strong class="${good(m.recall)}">${this._pct(m.recall)}</strong></div>
                <div class="yw-m"><small>F1</small><strong>${this._pct(m.f1)}</strong></div>`;
            note = (s.negatives_scanned ? (s.sampled ? `<b>Sampled:</b> ${s.negatives_scanned} of ${s.negatives_available} negatives. Full sweep gives exact numbers.` : `${s.negatives_scanned} negatives scanned.`) : 'No negatives: only recall is measured.') +
                (s.targets_truncated ? ' Targets truncated.' : '') + (s.unreadable ? ` ${s.unreadable} files have no stored bytes.` : '');
            tabs = [['strings', 'Strings', res.strings.length], ['missed', 'Missed', m.fn], ['false', 'False hits', m.fp, true]];
        }
        if (!tabs.some(t => t[0] === this._tab)) this._tab = tabs[0][0];
        const conf = m ? `<div class="yw-conf"><span>TP <b>${m.tp}</b></span><span>FN <b>${m.fn}</b></span><span class="${m.fp ? 'bad' : ''}">FP <b>${m.fp}</b></span><span>TN <b>${m.tn}</b></span></div>` : '';
        return `<div class="yw-metrics">${head}${conf}</div>
            <div class="yw-note">${note}</div>
            ${this._tabsHtml(tabs, this._tab)}
            <div class="yw-pane">${this._resultPane(res)}</div>`;
    },

    setTab(k, data) {
        if (data) this._dtab = k;
        else this._tab = k;
        this._paint();
    },

    _fnUrl(f) {
        const i = f.example.lastIndexOf(':');
        return `/collections/${encodeURIComponent(f.example.slice(0, i))}/files/${f.example.slice(i + 1)}/functions/${escapeAttr(f.addr)}`;
    },

    // Missed / false hits / matches: one table, same look as the data tables.
    _resultPane(res) {
        if (this._tab === 'strings') return this._stringsPane(res);
        const m = res.metrics || {};
        const [rows, total, withRules] = this._tab === 'hits' ? [res.hit_files || [], res.hits] : this._tab === 'false' ? [res.false_hits, m.fp, true] : [res.missed, m.fn];
        if (!rows.length) return '<div class="yw-hint">none</div>';
        const body = rows.map(f => {
            const i = f.id.lastIndexOf(':'), coll = f.id.slice(0, i), md5 = f.id.slice(i + 1);
            const rules = (f.rules || []).map(r => `<span class="yw-badge noisy">${escapeHtml(r)}</span>`).join('');
            return `<tr><td>${EntityRenderer.renderFileName(f.name, md5, coll)}</td><td>${EntityRenderer.renderMd5(md5, { collection: coll })}</td>${withRules ? `<td>${rules}</td>` : ''}</tr>`;
        }).join('');
        return `<table><thead><tr><th>File</th><th>md5</th>${withRules ? '<th>Rules</th>' : ''}</tr></thead><tbody>${body}</tbody></table>
            ${total > rows.length ? `<div class="yw-dim" style="padding-top:6px;">Showing the first ${rows.length} of ${total}.</div>` : ''}`;
    },

    _dataPane(res) {
        if (this._dtab === 'suggest') return this._suggestPane();
        if (!res) return '<div class="yw-hint">No matching results</div>';
        return `<table><thead><tr><th>Group</th><th>Role</th><th>Files</th><th>Hits</th><th>Rate</th></tr></thead><tbody>${res.groups.map(g => `<tr>
            <td>${escapeHtml(g.label)}</td><td class="${g.role === 'target' ? 'yw-t' : 'yw-n'}">${escapeHtml(g.role)}</td>
            <td class="yw-mono">${g.files}</td><td class="yw-mono">${g.hits}</td><td class="yw-mono">${this._pct(g.hit_rate)}</td></tr>`).join('')}</tbody></table>`;
    },

    _stringsPane(res) {
        const s = res.sample || {}, land = res.landing || {};
        if (!res.strings.length) return '<div class="yw-hint">The rule has no strings.</div>';
        const rows = res.strings.map(x => {
            const l = land[`${x.rule}:${x.id}`] || { functions: [], strings: [] };
            const where = [
                ...l.functions.map(f => `<a href="${this._fnUrl(f)}" onclick="Nav.openPath(this.href, event)">${escapeHtml(f.name)}</a> <span class="yw-dim">&times;${f.files}</span>`),
                ...l.strings.map(t => `str &ldquo;${escapeHtml(t.value)}&rdquo; <span class="yw-dim">&times;${t.files}</span>`),
            ];
            const more = where.length > 1 ? ` <span class="yw-dim" title="${escapeAttr(`${where.length - 1} more places`)}">+${where.length - 1}</span>` : '';
            const w = Math.round(x.pos / Math.max(1, s.targets) * 84);
            const dead = x.pos === 0 && x.neg === 0;
            return `<tr><td class="yw-mono">${escapeHtml(res.rules.length > 1 ? `${x.rule} ` : '')}${escapeHtml(x.id)}${dead ? '<span class="yw-badge dead">DEAD</span>' : ''}${x.neg > 0 ? '<span class="yw-badge noisy">NOISY</span>' : ''}</td>
                <td><div class="yw-bar2"><div class="yw-track"><i style="width:${w}px"></i><i class="n" style="width:${x.neg ? Math.min(30, 3 + x.neg) : 0}px"></i></div>${x.pos}/${s.targets} &middot; <span style="color:${x.neg ? '#ef4444' : 'var(--subtle)'};">${x.neg}</span></div></td>
                <td>${where[0] || '<span class="yw-dim">no hit</span>'}${more}${l.unmapped ? ` <span class="yw-dim">(${l.unmapped} unmapped)</span>` : ''}</td></tr>`;
        }).join('');
        return `<table><thead><tr><th>String</th><th>Hits (target &middot; other)</th><th>Lands in</th></tr></thead><tbody>${rows}</tbody></table>`;
    },

    _suggestPane() {
        if (!this._suggested) return '<div class="yw-hint">Ranking strings...</div>';
        const rows = this._suggested.map((x, i) => `<tr>
            <td class="yw-mono">${escapeHtml(x.value)}</td><td class="yw-mono">${x.pos}/${x.of}</td>
            <td class="yw-mono" style="color:${x.neg ? '#ef4444' : 'inherit'};">${x.neg}</td>
            <td><button class="yw-btn ghost" onclick="YaraView.addString(${i})">Insert</button></td></tr>`).join('');
        return rows
            ? `<table><thead><tr><th>String (in &ge;80% of targets)</th><th>Targets</th><th>Others</th><th></th></tr></thead><tbody>${rows}</tbody></table>`
            : '<div class="yw-hint">Nothing is shared by 80% of the target files.</div>';
    },

    // Suggestions depend on the sets only, so refetch when those change, not per keystroke.
    async _loadSuggest(body) {
        const key = JSON.stringify([body.collection, body.targets, body.negatives]);
        if (key === this._suggKey) return;
        this._suggKey = key;
        this._suggested = null;
        let res = {};
        try {
            const r = await fetch('/api/yara/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, min_frac: 0.8 }) });
            if (r.ok) res = await r.json();
        } catch (e) { /* an empty tab shows the problem */ }
        if (key !== this._suggKey) return;
        this._suggested = res.strings || [];
        this._paint();
    },

    addString(i) {
        const v = this._suggested[i].value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        this.insertText(`// $s = "${v}" ascii wide`);
    },

    async draft() {
        this._status('asking the model...');
        const r = await fetch('/api/yara/draft', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...this._body(), name: (this._body().targets[0] || {}).label || 'family' }) });
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
        if (doc.scope && doc.scope.cmp) this._cmp.load(doc.scope.cmp);
        this.setRule(doc.text);
        this._status(`${doc.name} v${doc.version}`);
    },

    async save() {
        const body = { rule: this.getRule(), uuid: this._uuid, scope: { cmp: this._cmp.cmp() }, note: '' };
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
        if (!body.collection) return;
        this._sweepKey = JSON.stringify(this._body());
        const r = await fetch(`/api/yara/${mode}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const res = await r.json();
        if (!r.ok) { this._status(res.message || res.error || `${mode} failed`); return; }
        this._run = res.run_id;
        this._prog = { mode, seen: 0, hits: 0, total: 0 };
        this._view = 'sweep';
        this._paint();
        this._pollRun(mode);
    },

    async _pollRun(mode) {
        const id = this._run;
        if (!id) return;
        let res;
        try {
            const r = await fetch(`/api/yara/run/${id}`);
            res = await r.json();
            if (!r.ok) throw new Error(res.message || `HTTP ${r.status}`);
        } catch (e) {
            this._run = null;
            this._busy(false);
            this._status(String(e.message || e));
            this._paint();
            return;
        }
        if (id !== this._run) return; // another run started
        if (!res.done) {
            this._prog = { mode, ...res };
            const f = res.total ? Math.min(1, res.seen / res.total) : 0;
            this._busy(true, mode === 'apply' ? 'Tagging files' : 'Sweeping the collection', f, `${res.seen}${res.total ? ` / ${res.total}` : ''} files · ${res.hits} hit`);
            this._paint();
            this._runTimer = setTimeout(() => this._pollRun(mode), 1500);
            return;
        }
        this._run = null;
        this._busy(false);
        this._sweep = { ok: true, exact: true, ...res };
        this._status(`${mode === 'apply' ? 'applied' : 'swept'}: ${res.seen} files, ${res.hits} hit`);
        this._paint();
        this._syncSweep();
    },
};
