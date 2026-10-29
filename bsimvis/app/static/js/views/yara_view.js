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

// Link into the workbench with a set of files as "Must match": one search per query string.
window.yaraUrl = function (collection, queries) {
    const cmp = { groups: [{ label: 'Must match', queries }, { label: 'Must not match', queries: [] }] };
    return `/yara?collection=${encodeURIComponent(collection)}&cmp=${encodeURIComponent(JSON.stringify(cmp))}`;
};

window.YaraView = {
    _editor: null,
    _textarea: null,
    _timer: null,
    _seq: 0,
    _tab: 'strings', // result tab
    _axis: 'code', // file cluster axis of the Scope tab
    _sel: new Set(), // picked suggestion rows
    _fold: new Set(), // files folded in the hit tables
    _rowIds: [],
    _suggested: null,
    _suggKey: '',
    _res: null,
    _good: null, // last result of a rule that compiled; shown again while the rule is broken
    _hint: '',
    _view: 'live', // 'live' sample test | 'sweep' full-collection run
    _sweep: null,
    _prog: null,
    _uuid: null,
    _run: null,
    _runTimer: null,
    _cmp: null,
    _last: null,
    _pins: {}, // "coll:md5" -> {role: target | negative | drop, name}
    _vers: [], // stored results of each saved version: {n, rule, live, sweep}
    _cur: -1, // version tab on show; -1 = the rule being edited
    _sort: { sug: ['score', -1], str: ['score', -1] }, // [key, direction] per strings table

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
        this._applySize(this._size.w, this._size.h);
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
    _applySize(w, h) {
        const main = document.querySelector('.yw-main');
        if (!main) return;
        if (w) main.style.setProperty('--yw-left', `${w}px`); else main.style.removeProperty('--yw-left');
        if (h) main.style.setProperty('--yw-data-h', `${h}px`); else main.style.removeProperty('--yw-data-h');
    },

    // Drag a gutter: 'col' sizes the editor, 'row' sizes the Data panel. Saved with the rule state.
    _drag(e, axis) {
        e.preventDefault();
        const main = document.querySelector('.yw-main'), data = document.getElementById('yara-data');
        const gut = e.currentTarget;
        gut.setPointerCapture(e.pointerId);
        const move = ev => {
            if (axis === 'col') {
                const r = main.getBoundingClientRect();
                this._size.w = Math.max(300, Math.min(r.width - 300, ev.clientX - r.left));
            } else {
                const r = data.getBoundingClientRect(), right = document.getElementById('yara-right').getBoundingClientRect();
                this._size.h = Math.max(90, Math.min(right.bottom - r.top - 140, ev.clientY - r.top));
            }
            this._applySize(this._size.w, this._size.h);
        };
        const up = () => {
            gut.removeEventListener('pointermove', move);
            gut.removeEventListener('pointerup', up);
            this._persist();
        };
        gut.addEventListener('pointermove', move);
        gut.addEventListener('pointerup', up);
    },

    _resetSize() {
        this._size = {};
        this._applySize();
        this._persist();
    },

    _loadState(params) {
        let saved = {};
        try { saved = JSON.parse(localStorage.getItem('bsimvis.yara') || '{}'); } catch (e) { /* storage is a convenience */ }
        let cmp = saved.cmp || null;
        try { if (params.cmp) cmp = JSON.parse(params.cmp); } catch (e) { /* bad link: use the saved sets */ }
        if (params.target) cmp = { groups: [{ label: 'Must match', queries: [params.target] }, { label: 'Must not match', queries: params.negative ? [params.negative] : [] }] };
        this._savedCmp = cmp;
        this._savedRule = saved.rule;
        this._rest = saved.rest !== false;
        this._size = saved.size || {};
        this._pins = saved.pins || {};
        this._nway = null;
        if (params.family) {
            // Opened from an N-way family: its files are the targets, its contrast the negatives.
            const pin = (ids, role) => Object.fromEntries(String(ids || '').split(',').filter(Boolean).map(id => [id, { role, name: id.split(':').pop().slice(0, 8) }]));
            this._pins = { ...pin(params.family, 'target'), ...pin(params.contrast, 'negative') };
            this._rest = false;
            this._savedCmp = null;
            try { this._nway = params.nway ? Object.fromEntries(new URLSearchParams(params.nway)) : null; } catch (e) { /* no function seeds */ }
        }
    },

    _persist() {
        try {
            localStorage.setItem('bsimvis.yara', JSON.stringify({ cmp: this._cmp.cmp(), rule: this.getRule(), rest: this._rest, size: this._size, pins: this._pins }));
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
            <div class="yw-pins" id="yara-pins"></div>
            <div class="yw-main">
                <div class="yw-left">
                    <div class="yw-editor" id="yara-editor"></div>
                    <div class="yw-tools">
                        <button class="yw-btn" onclick="YaraView.draft()" title="Ask the model to draft a rule from the Must match files"><i class="fa-solid fa-robot"></i> Draft with LLM</button>
                    </div>
                </div>
                <div class="yw-gut col" id="yw-gut-c" role="separator" aria-orientation="vertical" title="Drag to resize, double-click to reset" onpointerdown="YaraView._drag(event, 'col')" ondblclick="YaraView._resetSize()"></div>
                <div class="yw-right" id="yara-right">
                    <div id="yara-err"></div>
                    <section class="yw-card yw-panel" id="yara-data"></section>
                    <div class="yw-gut row" id="yw-gut-r" role="separator" aria-orientation="horizontal" title="Drag to resize, double-click to reset" onpointerdown="YaraView._drag(event, 'row')" ondblclick="YaraView._resetSize()"></div>
                    <section class="yw-card yw-panel" id="yara-res"></section>
                </div>
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
                async externalChecks(language, text) {
                    const res = await fetch('/api/yara/external-checks', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ format: language, text }),
                    });
                    return res.ok ? (await res.json()).results || {} : {};
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
        this._pinsBar();
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
    // Pinned files override both (the server applies them to every set).
    _body() {
        const cmp = this._cmp.cmp();
        const spec = g => ({ label: g.label, queries: g.queries.map(q => window.NwayCompare.parseQuery(q)) });
        const targets = cmp.groups[0].queries.length ? [spec(cmp.groups[0])] : [];
        const pins = Object.fromEntries(Object.entries(this._pins).map(([id, p]) => [id, p.role]));
        const hasT = targets.length || Object.values(pins).includes('target');
        const negatives = cmp.groups.slice(1).map(g => g.queries.length ? spec(g) : (this._rest && hasT ? { label: g.label, rest: true } : null)).filter(Boolean);
        return { rule: this.getRule(), collection: document.getElementById('yara-collection').value, targets, negatives, pins, hasT };
    },

    // --- pinned files -----------------------------------------------------

    pin(id, name, role) {
        if (this._pins[id] && this._pins[id].role === role) return;
        this._pins = { ...this._pins, [id]: { role, name } };
        this._pinsChanged();
    },

    unpin(id) {
        const { [id]: _, ...rest } = this._pins;
        this._pins = id === '*' ? {} : rest;
        this._pinsChanged();
    },

    _pinsChanged() {
        this._persist();
        this._pinsBar();
        this.schedule(0);
    },

    _pinsBar() {
        const el = document.getElementById('yara-pins');
        if (!el) return;
        const all = Object.entries(this._pins);
        const role = { target: ['Must match', 'yw-t', 'fa-circle-check'], negative: ['Must not match', 'yw-n', 'fa-circle-xmark'], drop: ['Dropped', 'yw-dim', 'fa-ban'] };
        el.innerHTML = all.length ? `<span class="yw-lbl">Pinned files</span>${all.map(([id, p]) => `<span class="yw-pin" title="${escapeAttr(`${role[p.role][0]}: ${id}`)}">
            <i class="fa-solid ${role[p.role][2]} ${role[p.role][1]}"></i>${escapeHtml(p.name || id)}
            <button aria-label="Unpin" onclick="YaraView.unpin(${escapeAttr(jsString(id))})"><i class="fa-solid fa-xmark"></i></button></span>`).join('')}
            <button class="yw-btn ghost" onclick="YaraView.unpin('*')">Clear</button>` : '';
    },

    // Quick actions on a file row: move it to the other set, or drop it from the test.
    // Names of the clusters a file or function belongs to.
    _clBadges(cl, isBinary = true) {
        return EntityRenderer.renderClusterCard(cl, isBinary);
    },

    _pinBtns(f, role) {
        const acts = [['target', 'fa-circle-check', 'Move to Must match'], ['negative', 'fa-circle-xmark', 'Move to Must not match'], ['drop', 'fa-ban', 'Drop from the test sets']];
        const args = `${escapeAttr(jsString(f.id))}, ${escapeAttr(jsString(f.name || ''))}`;
        const pinned = this._pins[f.id];
        return `<span class="yw-acts">${acts.filter(a => a[0] !== role).map(([v, i, t]) =>
            `<button class="yw-btn ghost icon" title="${t}" aria-label="${t}" onclick="event.stopPropagation(); YaraView.pin(${args}, '${v}')"><i class="fa-solid ${i}"></i></button>`).join('')}${pinned ? `<button class="yw-btn ghost icon" title="Unpin" aria-label="Unpin" onclick="event.stopPropagation(); YaraView.unpin(${escapeAttr(jsString(f.id))})"><i class="fa-solid fa-thumbtack-slash"></i></button>` : ''}</span>`;
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
        if (!body.hasT) {
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
        res._rule = body.rule;
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
        if (res && res.ok) this._good = res;
        else if (!res) this._good = null;
        this._paint();
    },

    _errHtml(res, note) {
        if (!res || res.ok) return '';
        const lines = res.error ? [escapeHtml(res.error)] : (res.errors || []).map(e => `<span class="yw-dim">line ${Number(e.line)}</span> ${escapeHtml(e.message)}`);
        return `<div class="yw-errbox" role="alert"><div class="yw-errhead"><i class="fa-solid fa-circle-exclamation"></i> ${res.error ? 'Test failed' : 'Rule does not compile'}</div>
            <div class="yw-errlines">${lines.join('<br>')}</div>${note ? `<div class="yw-errnote">${note}</div>` : ''}</div>`;
    },

    // Two cards: the data (suggested strings), then the results of a Sample test or a Full sweep.
    // A rule that does not compile shows its error and "no matching results"; the layout stays.
    _paint() {
        if (!document.getElementById('yara-res')) return;
        const seg = `<div class="yw-seg" role="group" aria-label="Result source">
            <button aria-pressed="${this._view === 'live'}" onclick="YaraView.setView('live')">Sample test</button>
            <button aria-pressed="${this._view === 'sweep'}" onclick="YaraView.setView('sweep')">Full sweep</button></div>`;
        const live = this._good;
        const data = this._suggestHtml();
        const snap = this._cur >= 0 ? this._vers[this._cur] : null;
        const none = `<div class="yw-hint">${escapeHtml(snap ? `No ${this._view === 'sweep' ? 'sweep' : 'sample test'} was stored with this version.` : (this._hint || 'No matching results'))}</div>`;
        let body;
        if (snap) { const r = this._view === 'sweep' ? snap.sweep : snap.live; body = r ? this._resultHtml(r) : none; }
        else if (this._view === 'sweep') body = this._run ? this._progressHtml() : (this._sweep ? this._resultHtml(this._sweep) : none);
        else body = live ? this._resultHtml(live) : none;
        const err = snap ? '' : this._errHtml(this._res, live ? 'Showing the last valid result below.' : '');
        const put = (id, html) => {
            const n = document.getElementById(id), top = (n.querySelector('.yw-pane') || {}).scrollTop || 0;
            n.innerHTML = html;
            const q = n.querySelector('.yw-pane');
            if (q) q.scrollTop = top;
        };
        put('yara-err', err);
        put('yara-data', data);
        put('yara-res', `<h3 class="yw-h">Results ${seg}</h3>${this._verBar()}${body}`);
    },

    // One tab per saved version plus the rule being edited; each keeps the results it had when saved.
    _verBar() {
        if (!this._vers.length) return '';
        const chip = (i, label, x) => `<span class="yw-vchip" aria-pressed="${this._cur === i}"><button onclick="YaraView.showVer(${i})">${label}</button>${x}</span>`;
        const clear = (i, n) => `<button aria-label="Clear the results of ${n}" title="Clear the stored results of ${n}" onclick="YaraView.clearVer(${i})"><i class="fa-solid fa-xmark"></i></button>`;
        return `<div class="yw-vers">${chip(-1, 'Editing', '')}${this._vers.map((v, i) => chip(i, `v${Number(v.n)}`, clear(i, `v${Number(v.n)}`))).join('')}<span class="yw-sp"></span>
            ${this._cur >= 0 ? `<button class="yw-btn ghost" onclick="YaraView.loadVer(${this._cur})">Load rule into editor</button>` : ''}
            <button class="yw-btn ghost" onclick="YaraView.clearVer(-1)">Clear all</button></div>`;
    },

    showVer(i) {
        this._cur = i;
        this._paint();
    },

    loadVer(i) {
        this.setRule(this._vers[i].rule);
        this._cur = -1;
        this._paint();
    },

    // i < 0 clears every stored version.
    clearVer(i) {
        if (i < 0) { this._vers = []; this._cur = -1; }
        else {
            this._vers.splice(i, 1);
            this._cur = this._cur === i ? -1 : this._cur > i ? this._cur - 1 : this._cur;
        }
        this._paint();
    },

    _tabsHtml(tabs, cur) {
        return `<div class="yw-tabs" role="tablist">${tabs.map(([k, l, c, red]) => `<button class="yw-tab" role="tab" aria-selected="${cur === k}" onclick="YaraView.setTab('${k}')">${l}${c !== '' ? `<em class="${red && c ? 'red' : ''}">${c}</em>` : ''}</button>`).join('')}</div>`;
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
            note = `<b>Exact:</b> all ${res.seen} files scanned${res.mode === 'apply' ? `, ${res.tagged} retagged` : ''}.${res.strings_sampled ? ' String counts come from a sample of the hit files.' : ''}${m ? '' : ' No Must match set, so no precision or recall.'}`;
            const pre = [['strings', 'Strings', res.strings.length], ['groups', 'Groups', res.groups.length]];
            tabs = m ? [...pre, ['true', 'True hits', m.tp], ['missed', 'Missed', m.fn], ['false', 'False hits', m.fp, true], ['scope', 'Scope', '']] : [...pre, ['hits', 'Matches', res.hits], ['scope', 'Scope', ''], ['clusters', 'Clusters', '']];
        } else {
            head = `<div class="yw-m"><small>Precision</small><strong class="${good(m.precision)}">${this._pct(m.precision)}</strong></div>
                <div class="yw-m"><small>Recall</small><strong class="${good(m.recall)}">${this._pct(m.recall)}</strong></div>
                <div class="yw-m"><small>F1</small><strong>${this._pct(m.f1)}</strong></div>`;
            note = (s.negatives_scanned ? (s.sampled ? `<b>Sampled:</b> ${s.negatives_scanned} of ${s.negatives_available} negatives. Full sweep gives exact numbers.` : `${s.negatives_scanned} negatives scanned.`) : 'No negatives: only recall is measured.') +
                (s.targets_truncated ? ' Targets truncated.' : '') + (s.unreadable ? ` ${s.unreadable} files have no stored bytes.` : '');
            tabs = [['strings', 'Strings', res.strings.length], ['groups', 'Groups', res.groups.length], ['true', 'True hits', m.tp], ['missed', 'Missed', m.fn], ['false', 'False hits', m.fp, true], ['scope', 'Scope', ''], ['clusters', 'Clusters', '']];
        }
        if (!tabs.some(t => t[0] === this._tab)) this._tab = tabs[0][0];
        const conf = m ? `<div class="yw-conf"><span>TP <b>${m.tp}</b></span><span>FN <b>${m.fn}</b></span><span class="${m.fp ? 'bad' : ''}">FP <b>${m.fp}</b></span><span>TN <b>${m.tn}</b></span></div>` : '';
        return `<div class="yw-metrics">${head}${conf}</div>
            <div class="yw-note">${note}</div>
            ${this._tabsHtml(tabs, this._tab)}
            <div class="yw-pane">${this._resultPane(res)}</div>`;
    },

    setTab(k) {
        this._tab = k;
        this._paint();
        if (k === 'clusters') this._loadHitmap();
        if (k === 'scope') this._loadScope();
    },

    // Viewing a full sweep that had no Must match set: the sweep's hit files are the set to map.
    _mapBody() {
        const sw = this._view === 'sweep' && this._sweep && this._sweep.exact && !this._sweep.metrics ? this._sweep : null;
        return sw ? { run_id: sw.run_id, collection: document.getElementById('yara-collection').value } : this._body();
    },

    // File clusters to focus on: pure clusters that hold the targets, and the ones hiding the misses.
    async _loadScope() {
        const body = { ...this._mapBody(), axis: this._axis }, key = JSON.stringify(body);
        if (this._sc && this._sc.key === key) return;
        this._sc = { key, loading: true };
        this._paint();
        try {
            const r = await fetch('/api/yara/scope', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            this._sc = { key, data: await r.json() };
        } catch (e) { this._sc = { key, error: String(e) }; }
        this._paint();
    },

    // One table row per cluster, like the other result tabs; the pill is the shared cluster card.
    _scopeRows(rows, gainLabel) {
        if (!rows.length) return '<div class="yw-hint">none</div>';
        const btn = (u, mode, icon, tip) => `<button class="yw-btn ghost icon" title="${tip}" aria-label="${tip}" onclick="YaraView.refocus(${escapeAttr(jsString(u))}, '${mode}')"><i class="fa-solid ${icon}"></i></button>`;
        return `<table class="yw-fixed"><colgroup><col><col style="width:150px"><col style="width:56px"><col style="width:56px"><col style="width:56px"><col style="width:56px"><col style="width:60px"><col style="width:80px"><col style="width:116px"></colgroup>
            <thead><tr><th>Cluster</th><th>UUID</th><th title="Files in the cluster">Files</th><th title="Targets the rule hits">Hit</th><th title="Targets the rule misses (a sweep without targets: cluster-mates it does not match)">Missed</th><th title="Negative files, over the whole sets">Neg</th><th>Pure</th><th title="Newly covered, running total">${gainLabel || 'Covered'}</th><th></th></tr></thead><tbody>${rows.map(c => `<tr>
            <td><div class="yw-cut" title="${escapeAttr(c.name)}">${escapeHtml(c.name)}</div></td>
            <td>${EntityRenderer.renderClusterCard([c.card], true, null, { showUuid: true })}</td>
            <td class="yw-mono">${c.size}</td><td class="yw-mono">${c.hit}</td>
            <td class="yw-mono" style="color:${c.missed ? '#f59e0b' : 'var(--subtle)'};">${c.missed}</td>
            <td class="yw-mono" style="color:${c.negatives ? '#ef4444' : 'var(--subtle)'};">${c.negatives}</td>
            <td class="yw-mono">${Math.round(c.purity * 100)}%</td>
            <td class="yw-mono">${c.gain !== undefined ? `+${c.gain} (${c.covered})` : ''}</td>
            <td>${c.uuid ? btn(c.uuid, 'target', 'fa-crosshairs', 'Test only this cluster: it becomes Must match') + btn(c.uuid, 'add', 'fa-plus', 'Add to Must match') + btn(c.uuid, 'neg', 'fa-ban', 'Add to Must not match') : ''}</td></tr>`).join('')}</tbody></table>`;
    },

    setAxis(a) {
        this._axis = a;
        this._loadScope();
    },

    _axisChips() {
        const names = { code: 'Code', library: 'Library', content: 'Content', overall: 'Overall' };
        return `<span class="yw-dim">Cluster axis</span> ${Object.entries(names).map(([k, l]) => `<button class="yw-btn${this._axis === k ? '' : ' ghost'}" onclick="YaraView.setAxis('${k}')">${l}</button>`).join(' ')}`;
    },

    _scopePane() {
        const h = this._sc, d = h && h.data;
        const axes = `<div style="padding:6px 0;display:flex;gap:6px;align-items:center;">${this._axisChips()}</div>`;
        if (!h || h.loading) return `${axes}<div class="yw-hint">clustering the files&hellip;</div>`;
        if (h.error || !d || !d.ok) return `${axes}<div class="yw-hint">${escapeHtml(h.error || (d && (d.message || 'no scope')) || 'no scope')}</div>`;
        const hits = d.mode === 'hits';
        const cov = d.scope.length ? d.scope[d.scope.length - 1].covered : 0, pc = n => Math.round(100 * n / Math.max(1, d.targets));
        const all = d.scope.filter(c => c.uuid).map(c => c.uuid);
        return `${axes}<div class="yw-metrics"><div class="yw-m"><small>${hits ? 'Hit files' : 'Targets'}</small><strong>${d.targets}</strong></div>
            <div class="yw-m"><small>${hits ? 'Unmatched mates' : 'Missed'}</small><strong>${d.missed}</strong></div>
            <div class="yw-m"><small>Scope covers</small><strong>${pc(cov)}%</strong></div>
            <div class="yw-m"><small>No cluster</small><strong>${d.unclustered}</strong></div>
            <span class="yw-sp"></span>${all.length ? `<button class="yw-btn primary" title="Must match becomes the union of the proposed clusters" onclick='YaraView.refocus(${escapeAttr(JSON.stringify(all))}, "target")'><i class="fa-solid fa-crosshairs"></i> Use proposed scope</button>` : ''}</div>
            <div class="yw-dim" style="padding:6px 0;">${hits ? `No Must match set: the rule's hits stand in for the targets. Pure = the rule matches at least ${Math.round(d.min_purity * 100)}% of the cluster.${d.truncated ? ' Hit list truncated.' : ''}` : `Pure = at least ${Math.round(d.min_purity * 100)}% targets, counted over all negatives, not the scan sample.`} </div>
            <div class="yw-dim" style="padding:6px 0;">${hits ? `<b>Look for misses</b> &middot; ${d.missed} files sit in clusters the rule only partly matches; the unmatched mates, by cluster` : `<b>Focus on the misses</b> &middot; ${d.missed} targets missed; clusters holding most of them`}</div>${this._scopeRows(d.focus, 'misses')}
            <div class="yw-dim" style="padding:10px 0 6px;"><b>Proposed scope</b> &middot; ${cov} of ${d.targets} ${hits ? 'hit files' : 'targets'} in ${d.scope.length} ${hits ? 'clusters the rule matches' : 'pure clusters'}</div>${this._scopeRows(d.scope, 'targets')}
            ${d.impure.length ? `<div class="yw-dim" style="padding:10px 0 6px;"><b>Too impure to scope to</b> &middot; targets mixed with negatives</div>${this._scopeRows(d.impure, '')}` : ''}`;
    },

    // Point the test sets at file clusters (uuid or list): target = replace Must match, add = append, neg = Must not match.
    refocus(uuids, mode) {
        const axis = (this._sc && this._sc.data && this._sc.data.axis) || 'overall';
        const qs = [].concat(uuids).map(u => `cluster_uuid=${encodeURIComponent(u)}${axis === 'overall' ? '' : `&axis=${axis}`}`);
        const cmp = this._cmp.cmp();
        const g = cmp.groups;
        const merge = (list, add) => [...new Set([...list, ...add])];
        if (mode === 'target') g[0].queries = qs;
        else if (mode === 'add') g[0].queries = merge(g[0].queries, qs);
        else if (g[1]) g[1].queries = merge(g[1].queries, qs);
        this._cmp.load(cmp);
        this._sum(null);
        this._persist();
        this.schedule(0);
    },


    // Function clusters the rule's hits land in: one cluster in many groups is a shared capability, in one group a family marker.
    async _loadHitmap() {
        const body = { ...this._mapBody(), cap: 60 }, key = JSON.stringify(body);
        if (this._hm && this._hm.key === key) return;
        this._hm = { key, loading: true };
        this._paint();
        try {
            const r = await fetch('/api/yara/hitmap', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            this._hm = { key, data: await r.json() };
        } catch (e) { this._hm = { key, error: String(e) }; }
        this._paint();
    },

    _clustersPane() {
        const h = this._hm;
        if (!h || h.loading) return '<div class="yw-hint">mapping hits to function clusters&hellip;</div>';
        if (h.error || !h.data || !h.data.ok) return `<div class="yw-hint">${escapeHtml(h.error || (h.data && (h.data.message || 'no map')) || 'no map')}</div>`;
        const rows = h.data.clusters.map(c => `<tr>
            <td>${EntityRenderer.renderClusterCard([c.card], false)}</td>
            <td class="yw-mono">${c.hit_files}/${c.span_files}</td>
            <td class="yw-mono">${escapeHtml(Object.entries(c.groups).map(([g, n]) => `${g} ${n}`).join(', '))}</td>
            <td class="yw-mono">${escapeHtml(c.strings.join(' '))}</td></tr>`).join('');
        return `<div class="yw-dim" style="padding:6px 0;">Hit files / files the cluster spans, per group. ${h.data.unclustered_functions} hit functions have no cluster.</div>
            <table class="yw-fixed"><colgroup><col><col style="width:80px"><col style="width:34%"><col style="width:20%"></colgroup>
            <thead><tr><th>Cluster</th><th>Files</th><th>Groups</th><th>Strings</th></tr></thead><tbody>${rows}</tbody></table>`;
    },

    // Missed / matches: file rows. True / false hits: the evidence, one row per match, folded by file.
    _resultPane(res) {
        if (this._tab === 'strings') return this._stringsPane(res);
        if (this._tab === 'groups') return this._groupsPane(res);
        if (this._tab === 'clusters') return this._clustersPane();
        if (this._tab === 'scope') return this._scopePane();
        const m = res.metrics || {};
        const [rows, total, detail] = this._tab === 'hits' ? [res.hit_files || [], res.hits] : this._tab === 'false' ? [res.false_hits, m.fp, true] : this._tab === 'true' ? [res.true_hits || [], m.tp, true] : [res.missed, m.fn];
        if (!rows.length) return '<div class="yw-hint">none</div>';
        const lead = this._tab === 'missed' && this._cur < 0 ? this._missedSugg(rows) : '';
        const more = total > rows.length ? `<div class="yw-dim" style="padding-top:6px;">Showing the first ${rows.length} of ${total}.</div>` : '';
        const role = this._tab === 'false' ? 'negative' : this._tab === 'hits' ? '' : 'target';
        if (detail) return this._hitsPane(rows, role, res.rules.length > 1) + more;
        const body = rows.map(f => {
            const i = f.id.lastIndexOf(':'), coll = f.id.slice(0, i), md5 = f.id.slice(i + 1);
            return `<tr><td><div class="yw-cut">${EntityRenderer.renderFileName(f.name, md5, coll)}</div></td><td><div class="yw-cut">${EntityRenderer.renderMd5(md5, { collection: coll })}</div></td><td>${this._clBadges(f.clusters)}</td><td>${this._pinBtns(f, role)}</td></tr>`;
        }).join('');
        return `${lead}<table class="yw-fixed"><colgroup><col><col style="width:110px"><col style="width:34%"><col style="width:96px"></colgroup><thead><tr><th>File</th><th>md5</th><th>Clusters</th><th></th></tr></thead><tbody>${body}</tbody></table>${more}`;
    },

    // Suggested strings the rule lacks that sit in the missed files: adding one may bring them back.
    _missedSugg(missed) {
        const ids = new Set(missed.map(f => f.id)), inRule = this._defMap();
        const rows = (this._suggested || []).map((x, i) => ({ ...x, i, hit: (x.files || []).filter(f => ids.has(f)).length }))
            .filter(x => x.hit && !inRule.has(this._vkey(x.value)))
            .sort((a, b) => b.hit - a.hit || b.score - a.score).slice(0, 80);
        if (!rows.length) return '';
        return `<div class="yw-dim" style="padding:6px 0;">Suggested strings not in the rule that match missed files</div>
            <table class="yw-fixed yw-sug"><colgroup><col><col style="width:90px"><col style="width:90px"><col style="width:60px"><col style="width:60px"><col style="width:36px"></colgroup>
            <thead><tr><th>String</th><th>Missed</th><th>Targets</th><th>Others</th><th>Score</th><th></th></tr></thead><tbody>${rows.map(x => `<tr>
            <td class="yw-sv">${this._sugLabel(x)}</td><td class="yw-mono">${x.hit}/${ids.size}</td><td class="yw-mono">${x.pos}/${x.of}</td>
            <td class="yw-mono" style="color:${x.neg ? '#ef4444' : 'var(--subtle)'};">${x.neg}</td><td>${this._scoreHtml(x.score)}</td>
            <td><button class="yw-btn ghost icon" title="Add to the rule" aria-label="Add to the rule" onclick="YaraView.addSug(${x.i})"><i class="fa-solid fa-plus"></i></button></td></tr>`).join('')}</tbody></table>`;
    },

    toggleFile(id, e) {
        if (e && e.target.closest('.entity-filename, .entity-md5, a')) return;
        if (!this._fold.delete(id)) this._fold.add(id);
        this._paint();
    },

    foldAll(on) {
        this._fold = new Set(on ? this._rowIds : []);
        this._paint();
    },

    // One row per (string, place) in each file: repeat instances fold into a count.
    _hitsPane(rows, role, manyRules) {
        this._rowIds = rows.map(f => f.id);
        const fnHtml = f => `<div class="yw-fn">${EntityRenderer.renderFunction({ ...f }, { showActions: false })}${this._clBadges(f.clusters, false)}</div>`;
        const body = rows.map(f => {
            const i = f.id.lastIndexOf(':'), coll = f.id.slice(0, i), md5 = f.id.slice(i + 1);
            const ms = f.matches || [];
            const open = !this._fold.has(f.id);
            const rules = manyRules ? (f.rules || []).map(r => `<span class="yw-badge noisy">${escapeHtml(r)}</span>`).join('') : '';
            const groups = new Map();
            for (const x of ms) {
                const k = [x.rule, x.string, x.function ? x.function.entrypoint_address : x.str || ''].join('|');
                if (groups.has(k)) groups.get(k).n++; else groups.set(k, { x, n: 1 });
            }
            const head = `<tr class="yw-fh" onclick="YaraView.toggleFile(${escapeAttr(jsString(f.id))}, event)"><td colspan="4"><div class="yw-fhb">
                <i class="fa-solid fa-chevron-${open ? 'down' : 'right'} yw-chev"></i>
                <span class="yw-fname">${EntityRenderer.renderFileName(f.name, md5, coll)}</span> ${EntityRenderer.renderMd5(md5, { collection: coll })}
                <span class="yw-dim">${ms.length ? `${ms.length} match${ms.length > 1 ? 'es' : ''}` : 'no details (list limit)'}</span>${rules}${this._clBadges(f.clusters)}
                <span class="yw-sp"></span>${this._pinBtns(f, role)}</div></td></tr>`;
            if (!open) return head;
            return head + [...groups.values()].map(({ x, n }) => {
                if (x.string === undefined) return `<tr class="yw-mr"><td colspan="4" class="yw-dim">${escapeHtml(x.rule)}: matched on the condition alone, no string</td></tr>`;
                const at = x.address ? x.address : `@0x${Number(x.offset).toString(16)}`;
                // printable bytes read better as text, binary ones as hex
                const shown = (x.text.match(/[^.]/g) || []).length >= x.text.length * 0.6 ? x.text : x.hex;
                const xr = x.xrefs || [];
                const where = x.function ? fnHtml(x.function)
                    : x.str ? `<div class="yw-cut" title="${escapeAttr(x.str)}"><span class="yw-dim">str</span> &ldquo;${escapeHtml(x.str)}&rdquo;</div>`
                        + (xr.length ? `<div class="yw-xr">${xr.slice(0, 2).map(fnHtml).join('')}${x.xref_total > 2 ? `<span class="yw-dim">+${x.xref_total - 2} more references</span>` : ''}</div>` : '<div class="yw-dim">no function references it</div>')
                        : '<span class="yw-dim">not in a function or string</span>';
                return `<tr class="yw-mr">
                    <td class="yw-mono" title="${escapeAttr(`${x.rule} ${x.string}`)}">${escapeHtml(manyRules ? `${x.rule} ` : '')}${escapeHtml(x.string)}${n > 1 ? ` <span class="yw-dim">&times;${n}</span>` : ''}</td>
                    <td><code title="${escapeAttr(`${x.hex}\n"${x.text}"`)}">${escapeHtml(shown)}</code></td>
                    <td>${where}</td>
                    <td class="yw-mono yw-dim" title="${escapeAttr(`file offset 0x${Number(x.offset).toString(16)}`)}">${escapeHtml(at)}</td></tr>`;
            }).join('');
        }).join('');
        return `<div class="yw-foldbar"><button class="yw-btn ghost" onclick="YaraView.foldAll(true)">Collapse all</button><button class="yw-btn ghost" onclick="YaraView.foldAll(false)">Expand all</button></div>
            <table class="yw-fixed yw-hits"><colgroup><col style="width:110px"><col style="width:24%"><col><col style="width:84px"></colgroup>
            <thead><tr><th>String</th><th>Matched</th><th>Lands in</th><th>Address</th></tr></thead><tbody>${body}</tbody></table>`;
    },

    _groupsPane(res) {
        return `<table class="yw-fixed"><colgroup><col><col style="width:90px"><col style="width:70px"><col style="width:70px"><col style="width:70px"></colgroup>
            <thead><tr><th>Group</th><th>Role</th><th>Files</th><th>Hits</th><th>Rate</th></tr></thead><tbody>${res.groups.map(g => `<tr>
            <td>${escapeHtml(g.label)}</td><td class="${g.role === 'target' ? 'yw-t' : g.role === 'negative' ? 'yw-n' : 'yw-dim'}">${escapeHtml(g.role)}</td>
            <td class="yw-mono">${g.files}</td><td class="yw-mono">${g.hits}</td><td class="yw-mono">${this._pct(g.hit_rate)}</td></tr>`).join('')}</tbody></table>`;
    },

    _stringsPane(res) {
        const s = res.sample || {}, land = res.landing || {};
        if (!res.strings.length) return '<div class="yw-hint">The rule has no strings.</div>';
        const defs = this._ruleStrings(res._rule || '');
        const rows = this._sorted('str', res.strings, { id: x => x.id, pos: x => x.pos, score: x => x.score }).map(x => {
            const l = land[`${x.rule}:${x.id}`] || { functions: [], strings: [] };
            const files = n => `<span class="yw-dim" title="${escapeAttr(`in ${n} file${n > 1 ? 's' : ''}`)}">&times;${n}</span>`;
            const where = [
                ...l.functions.map(f => `<div class="yw-land"><div class="yw-fn">${EntityRenderer.renderFunction({ ...f.doc }, { showActions: false })}</div>${f.xref ? '<span class="yw-dim" title="References the matched string">xref</span>' : ''}${files(f.files)}</div>`),
                ...l.strings.map(t => `<div class="yw-land"><div class="yw-cut" title="${escapeAttr(t.value)}"><span class="yw-dim">str</span> &ldquo;${escapeHtml(t.value)}&rdquo;</div>${files(t.files)}</div>`),
            ];
            const more = where.length > 2 ? `<span class="yw-dim">+${where.length - 2} more</span>` : '';
            const def = defs.find(d => d.rule === x.rule && d.id === x.id);
            const rm = x.id === '$' || this._cur >= 0 ? '' : `<button class="yw-btn ghost icon" title="Remove ${escapeAttr(x.id)} from the rule" aria-label="Remove ${escapeAttr(x.id)} from the rule" onclick="YaraView.removeString(${escapeAttr(jsString(x.rule))}, ${escapeAttr(jsString(x.id))})"><i class="fa-solid fa-trash-can"></i></button>`;
            const w = Math.round(x.pos / Math.max(1, s.targets) * 84);
            const dead = x.pos === 0 && x.neg === 0;
            return `<tr><td class="yw-mono">${escapeHtml(res.rules.length > 1 ? `${x.rule} ` : '')}${escapeHtml(x.id)}${dead ? '<span class="yw-badge dead">DEAD</span>' : ''}${x.neg > 0 ? '<span class="yw-badge noisy">NOISY</span>' : ''}${def ? `<div class="yw-cut yw-dim" title="${escapeAttr(def.raw)}">${escapeHtml(def.raw)}</div>` : ''}</td>
                <td><div class="yw-bar2"><div class="yw-track"><i style="width:${w}px"></i><i class="n" style="width:${x.neg ? Math.min(30, 3 + x.neg) : 0}px"></i></div>${x.pos}/${s.targets} &middot; <span style="color:${x.neg ? '#ef4444' : 'var(--subtle)'};">${x.neg}</span></div></td>
                <td>${this._scoreHtml(x.score)}</td>
                <td>${where.slice(0, 2).join('') || '<span class="yw-dim">no hit</span>'}${more}${l.unmapped ? ` <span class="yw-dim">(${l.unmapped} unmapped)</span>` : ''}</td><td>${rm}</td></tr>`;
        }).join('');
        const th = (k, label) => this._th('str', k, label);
        return `<table class="yw-fixed"><colgroup><col style="width:30%"><col style="width:160px"><col style="width:64px"><col><col style="width:36px"></colgroup>
            <thead><tr>${th('id', 'String')}${th('pos', 'Hits (target &middot; other)')}${th('score', 'Score')}<th>Lands in</th><th></th></tr></thead><tbody>${rows}</tbody></table>`;
    },

    // --- sorting / score ----------------------------------------------------

    // `list` ordered by the table's current sort; `keys` maps a sort key to a value getter.
    _sorted(table, list, keys) {
        const [k, dir] = this._sort[table], get = keys[k] || keys.score;
        const val = x => { const v = get(x); return v === null || v === undefined ? -Infinity : v; };
        return [...list].sort((a, b) => {
            const va = val(a), vb = val(b);
            return dir * (typeof va === 'string' ? va.localeCompare(vb) : va - vb);
        });
    },

    _th(table, k, label) {
        const [cur, dir] = this._sort[table];
        const icon = cur === k ? `<i class="fa-solid fa-sort-${dir > 0 ? 'up' : 'down'}"></i>` : '';
        const tip = k === 'score' ? ' title="Share of target files that hold the string x share of other files that do not. 1.0 = in every target and no other; 0 = in every other file"' : '';
        return `<th class="yw-sortable" aria-sort="${cur === k ? (dir > 0 ? 'ascending' : 'descending') : 'none'}"${tip} onclick="YaraView.sortBy('${table}', '${k}')">${label} ${icon}</th>`;
    },

    sortBy(table, k) {
        const [cur, dir] = this._sort[table];
        this._sort = { ...this._sort, [table]: [k, cur === k ? -dir : (k === 'id' || k === 'value' ? 1 : -1)] };
        this._paint();
    },

    _scoreHtml(v) {
        if (v === null || v === undefined) return '<span class="yw-dim">&mdash;</span>';
        const c = v >= 0.8 ? '#10b981' : v >= 0.5 ? '#f59e0b' : 'var(--subtle)';
        return `<span class="yw-mono" style="color:${c}; font-weight:600;">${v.toFixed(2)}</span>`;
    },

    // The Data card: every string of the target files, best coverage first, with a multi-select insert.
    // The strings a rule defines, read off its source: [{rule, id, raw, key}]; `key` is the _vkey of the value.
    _ruleStrings(src) {
        const out = [];
        let rule = '';
        const re = /^[ \t]*(?:(?:private|global)\s+)*rule\s+(\w+)|^[ \t]*(\$\w*)[ \t]*=[ \t]*(?:"((?:[^"\\\n]|\\.)*)"|\{([^}]*)\})/gm;
        for (const m of src.matchAll(re)) {
            if (m[1]) { rule = m[1]; continue; }
            if (m[3] !== undefined) {
                const val = m[3].replace(/\\(x[0-9a-fA-F]{2}|.)/g, (_, c) => c.length === 3 ? String.fromCharCode(parseInt(c.slice(1), 16)) : ({ n: '\n', r: '\r', t: '\t' }[c] ?? c));
                out.push({ rule, id: m[2], raw: `"${m[3]}"`, key: `"${val}` });
            } else {
                const hex = `{ ${m[4].trim().toLowerCase().split(/\s+/).join(' ')} }`;
                out.push({ rule, id: m[2], raw: hex, key: hex });
            }
        }
        return out;
    },

    // A suggestion's value in the same form as the `_ruleStrings` keys.
    _vkey(v) {
        return /^\{ [0-9a-f?]{2}( [0-9a-f?]{2})* \}$/.test(v) ? v : `"${v}`;
    },

    // key -> [{rule, id}] for the rule in the editor.
    _defMap() {
        const m = new Map();
        for (const d of this._ruleStrings(this.getRule())) m.set(d.key, [...(m.get(d.key) || []), d]);
        return m;
    },

    _sugLabel(x) {
        return x.kind ? `<span class="yw-dim">${escapeHtml(x.kind)}</span> ${escapeHtml(x.label)} <span class="yw-mono yw-dim">${escapeHtml(x.value)}</span>` : escapeHtml(x.value);
    },

    _suggestHtml() {
        const list = this._suggested;
        const inRule = this._defMap();
        const has = i => inRule.has(this._vkey(list[i].value));
        const sel = list ? [...this._sel].filter(i => list[i]) : [];
        const nAdd = sel.filter(i => !has(i)).length, nDel = sel.length - nAdd;
        const n = this._sel.size;
        const head = `<h3 class="yw-h">Suggested strings, constants and functions${list ? `<em class="yw-count">${list.length}</em>` : ''}</h3>`;
        if (!list) return `${head}<div class="yw-hint">Ranking strings...</div>`;
        if (!list.length) return `${head}<div class="yw-hint">No stored strings for the target files.</div>`;
        const order = this._sorted('sug', list.map((x, i) => ({ ...x, i })), { value: x => x.value, pos: x => x.pos, neg: x => x.neg, score: x => x.score });
        const rows = order.map(x => {
            const i = x.i, pct = Math.round(100 * x.pos / Math.max(1, x.of)), on = has(i);
            return `<tr class="${this._sel.has(i) ? 'on' : ''}" onclick="YaraView.toggleSug(${i})">
                <td><input type="checkbox" tabindex="-1" ${this._sel.has(i) ? 'checked' : ''}></td>
                <td class="yw-sv">${on ? '<span class="yw-badge in">IN RULE</span> ' : ''}${this._sugLabel(x)}</td>
                <td><div class="yw-bar2"><div class="yw-track"><i style="width:${pct}%"></i></div><span>${x.pos}/${x.of}</span></div></td>
                <td class="yw-mono" style="color:${x.neg ? '#ef4444' : 'var(--subtle)'};">${x.neg}</td>
                <td>${this._scoreHtml(x.score)}</td>
                <td><button class="yw-btn ghost icon" title="${on ? 'Remove from the rule' : 'Add to the rule'}" aria-label="${on ? 'Remove from the rule' : 'Add to the rule'}" onclick="event.stopPropagation(); YaraView.${on ? 'removeSug' : 'addSug'}(${i})"><i class="fa-solid ${on ? 'fa-minus' : 'fa-plus'}"></i></button></td></tr>`;
        }).join('');
        const th = (k, label) => this._th('sug', k, label);
        return `${head}<div class="yw-sugbar">
            <label class="yw-check"><input type="checkbox" ${n && n === list.length ? 'checked' : ''} onchange="YaraView.selAll(this.checked)"> All</label>
            <span class="yw-dim">${n} selected</span><span class="yw-sp"></span>
            <button class="yw-btn primary" ${nAdd ? '' : 'disabled'} onclick="YaraView.addSelected()"><i class="fa-solid fa-plus"></i> Add${nAdd ? ` ${nAdd}` : ''}</button>
            <button class="yw-btn" ${nDel ? '' : 'disabled'} onclick="YaraView.removeSelected()"><i class="fa-solid fa-minus"></i> Remove${nDel ? ` ${nDel}` : ''}</button></div>
            <div class="yw-pane"><table class="yw-fixed yw-sug"><colgroup><col style="width:30px"><col><col style="width:140px"><col style="width:64px"><col style="width:64px"><col style="width:36px"></colgroup>
            <thead><tr><th></th>${th('value', 'String')}${th('pos', 'Targets')}${th('neg', 'Others')}${th('score', 'Score')}<th></th></tr></thead><tbody>${rows}</tbody></table></div>`;
    },

    // Suggestions depend on the sets only, so refetch when those change, not per keystroke.
    async _loadSuggest(body) {
        const key = JSON.stringify([body.collection, body.targets, body.negatives, body.pins, this._nway]);
        if (key === this._suggKey) return;
        this._suggKey = key;
        this._suggested = null;
        let res = {};
        try {
            const r = await fetch('/api/yara/suggest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, min_frac: 0 }) });
            if (r.ok) res = await r.json();
        } catch (e) { /* an empty tab shows the problem */ }
        if (key !== this._suggKey) return;
        this._suggested = [...(res.strings || []), ...(res.consts || [])];
        this._sel = new Set();
        this._paint();
        if (!this._nway) return;
        // The function seeds read raw bytes of many files: they join the table when ready.
        try {
            const f = await fetch('/api/yara/suggest/functions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, nway: this._nway }) });
            if (f.ok && key === this._suggKey) { this._suggested = [...this._suggested, ...((await f.json()).functions || [])]; this._paint(); }
        } catch (e) { /* strings and constants stay */ }
    },

    toggleSug(i) {
        if (!this._sel.delete(i)) this._sel.add(i);
        this._paint();
    },

    selAll(on) {
        this._sel = new Set(on ? this._suggested.map((_, i) => i) : []);
        this._paint();
    },

    addSug(i) {
        this.addStrings([this._suggested[i].value]);
        this._paint();
    },

    removeSug(i) {
        for (const d of this._defMap().get(this._vkey(this._suggested[i].value)) || []) this.removeString(d.rule, d.id);
        this._paint();
    },

    addSelected() {
        const inRule = this._defMap();
        const vals = [...this._sel].sort((a, b) => a - b).map(i => this._suggested[i].value).filter(v => !inRule.has(this._vkey(v)));
        if (vals.length) this.addStrings(vals);
        this._sel = new Set();
        this._paint();
    },

    removeSelected() {
        for (const i of [...this._sel]) this.removeSug(i);
        this._sel = new Set();
        this._paint();
    },

    // Delete one string from a rule: its definition (a hex string may span lines until `}`),
    // its `and` / `or` / `,` uses in the condition, and an emptied strings: section.
    removeString(rule, id) {
        const text = this.getRule(), name = id.slice(1);
        const heads = [...text.matchAll(/^[ \t]*(?:(?:private|global)\s+)*rule\s+(\w+)/gm)];
        const h = heads.findIndex(m => m[1] === rule);
        const start = h < 0 ? 0 : heads[h].index, end = h >= 0 && heads[h + 1] ? heads[h + 1].index : text.length;
        let block = text.slice(start, end);
        const before = block;
        block = block.replace(new RegExp(`^[ \\t]*\\$${name}[ \\t]*=[ \\t]*(?:\\{[^}]*\\}|.*)[^\\n]*\\n?`, 'm'), '');
        if (block === before) { this._status(`${id} not found in ${rule}`); return; }
        const ci = Math.max(0, block.search(/^[ \t]*condition[ \t]*:/m));
        const ref = `\\$${name}\\b(?!\\*)`;
        const cond = block.slice(ci).replace(new RegExp(`\\s+(?:and|or)\\s+${ref}|${ref}\\s+(?:and|or)\\s+|\\s*,\\s*${ref}|${ref}\\s*,\\s*`, 'g'), '');
        // an emptied strings: section does not compile
        block = block.slice(0, ci).replace(/^[ \t]*strings[ \t]*:[ \t]*\n(?:[ \t]*\n)*(?![ \t]*[$/])/m, '') + cond;
        this.setRule(text.slice(0, start) + block + text.slice(end));
        this._status(new RegExp(`[$#@!]${name}\\b(?!\\*)`).test(cond) ? `removed ${id}; the condition still uses it` : `removed ${id}`);
    },

    // Add `$sN = "..." ascii wide` lines to the first rule's strings: section (created if missing),
    // numbered after the highest $sN already in the rule.
    addStrings(values) {
        const text = this.getRule();
        let n = Math.max(0, ...[...text.matchAll(/\$s(\d+)\b/g)].map(m => +m[1]));
        const hex = v => /^\{ [0-9a-f?]{2}( [0-9a-f?]{2})* \}$/.test(v);
        const def = (v, n) => `$s${n} = ${hex(v) ? v : `"${esc(v)}" ascii wide`}`;
        const esc = v => v.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t');
        const cond = /^([ \t]*)condition[ \t]*:/m.exec(text);
        if (!cond) { this.insertText(values.map(v => def(v, ++n)).join('\n')); return; }
        const kw = cond[1], item = `${kw}    `;
        const lines = values.map(v => `${item}${def(v, ++n)}\n`).join('');
        const before = text.slice(0, cond.index);
        const ruleAt = Math.max(0, ...[...before.matchAll(/\brule\s+\w+/g)].map(m => m.index));
        const has = /^[ \t]*strings[ \t]*:/m.test(before.slice(ruleAt));
        // sit directly under the last string, before any blank line ahead of condition
        const body = before.replace(/\s*$/, '\n');
        this.setRule(`${body}${has ? '' : `${kw}strings:\n`}${lines}${text.slice(cond.index)}`);
        this._status(`inserted ${values.length} string${values.length > 1 ? 's' : ''}`);
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
        if (this._uuid !== doc.uuid) { this._vers = []; this._cur = -1; }
        this._uuid = doc.uuid;
        if (doc.scope && doc.scope.cmp) this._cmp.load(doc.scope.cmp);
        if (doc.scope) { this._pins = doc.scope.pins || {}; this._pinsBar(); }
        this.setRule(doc.text);
        this._status(`${doc.name} v${doc.version}`);
    },

    async save() {
        clearTimeout(this._timer);
        await this.test(); // so the stored results are of the text being saved
        const body = { rule: this.getRule(), uuid: this._uuid, scope: { cmp: this._cmp.cmp(), pins: this._pins }, note: '' };
        const r = await fetch('/api/yara/rules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const res = await r.json();
        if (!r.ok) { this._status(res.message || res.error || 'save failed'); return; }
        if (this._uuid !== res.uuid) { this._vers = []; this._cur = -1; }
        this._uuid = res.uuid;
        const fresh = r => (r && r._rule === body.rule ? r : null);
        this._vers.push({ n: res.version, rule: body.rule, live: fresh(this._good), sweep: this._sweepKey === JSON.stringify(this._body()) ? fresh(this._sweep) : null });
        this._paint();
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
        this._sweepRule = body.rule;
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
        this._sweep = { ok: true, exact: true, run_id: id, ...res, _rule: this._sweepRule };
        this._status(`${mode === 'apply' ? 'applied' : 'swept'}: ${res.seen} files, ${res.hits} hit`);
        this._paint();
        this._syncSweep();
    },
};
