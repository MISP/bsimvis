/**
 * Compare builder: the file-set form of the N-way diff, reusable by any view.
 *
 * Groups of file searches (filters as chips, with type-ahead from the collection's own
 * values), a live preview of the files each group holds, and an overlap bar. The N-way
 * panel and the YARA workbench both host one; the host decides what "run" does.
 *
 *   new NwayCompare({
 *     scope: () => ({ collection, pool }),   // where the searches run
 *     within: () => ['coll:md5', ...],       // optional: "only the files open" toggle
 *     saved: () => ({ groups, within }),     // optional: initial groups
 *     on: () => bool,                        // a compare is active (shows "clear")
 *     extra: () => html,                     // host controls under the form
 *     onRun: (cmp, out) => {},               // after the server saved the groups
 *     groups: 2, minGroups: 2, runLabel: 'Compare', labels: ['A', 'B'],
 *   })
 *
 * `html()` returns the form; `bind(node)` wires typing, focus and toggles on a stable
 * ancestor; `click(target)` / `input(event)` return true when they handled the event.
 * Everything shown came off uploaded samples: values go through escapeHtml / escapeAttr.
 */
(function () {
    // Compare groups filter files with the Files page parameters (/api/file/search).
    const FILE_FIELDS = [['q', 'Search', 'keywords'], ['file_tag', 'Tag', 'family:mirai, lib:*'], ['file_name', 'File name', 'bot*'], ['batch_uuid', 'Batch', 'uuid'], ['language_id', 'Language', 'x86:LE:64']];
    const FILE_MORE = [['filetype', 'File type'], ['status', 'Status'], ['inferred_tags', 'Inferred tags'], ['bin_cluster_uuid', 'Cluster'], ['min_function_count', 'Min functions'], ['max_function_count', 'Max functions'], ['min_entry_date', 'From date'], ['max_entry_date', 'To date']];
    const FILE_MULTI = new Set(['file_tag']);
    const FILE_LABELS = Object.fromEntries([...FILE_FIELDS, ...FILE_MORE].map(f => [f[0], f[1]]));
    const FILE_KNOWN = new Set([...FILE_FIELDS, ...FILE_MORE].map(f => f[0]));
    const GROUP_MAX = 8;
    const letter = i => String.fromCharCode(65 + i);
    const fileCount = n => `${Number(n)} ${Number(n) === 1 ? 'file' : 'files'}`;

    class NwayCompare {
        constructor(opts) {
            this.o = { endpoint: '/api/bin_sim/nway/columns', runLabel: 'Compare', ...opts };
            this.draft = null;
            this.counts = null;
            this.info = null;
            this.show = 'all';
            this.err = '';
            this.open = false;
            this.sug = null;
        }

        scope() {
            return (this.o.scope && this.o.scope()) || {};
        }

        letter(i) {
            return (this.o.labels && this.o.labels[i]) || letter(i);
        }

        // "tag=family:mirai&file_name=bot" -> search_files params; text without '=' is a plain search.
        static parseQuery(text) {
            const t = text.trim(), out = {};
            if (!t) return out;
            if (!t.includes('=')) return { q: t };
            for (const [k, v] of new URLSearchParams(t)) (out[k] ||= []).push(v);
            for (const k of Object.keys(out)) if (out[k].length === 1) out[k] = out[k][0];
            return out;
        }

        // Save the groups on the server: {id, files, overlap, groups}; `preview` only counts.
        async post(cmp, preview = false) {
            const body = { groups: cmp.groups.map(g => ({ label: g.label, queries: g.queries.map(q => NwayCompare.parseQuery(q)) })) };
            const sc = this.scope();
            if (sc.pool) body.pool = sc.pool; else body.collection = sc.collection || '';
            const within = (this.o.within && this.o.within()) || [];
            if (cmp.within && within.length) body.within = within;
            if (preview) body.preview = true;
            const res = await fetch(this.o.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            const out = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(out.message || out.error || `HTTP ${res.status}`);
            return out;
        }

        _qsToObj(str) {
            const o = {};
            for (const [k, v] of new URLSearchParams(str)) o[k] = o[k] ? `${o[k]},${v}` : v;
            return o;
        }

        _objToQs(o) {
            const p = new URLSearchParams();
            for (const [k, v] of Object.entries(o)) {
                const val = String(v ?? '').trim();
                if (!val) continue;
                if (FILE_MULTI.has(k)) val.split(',').map(x => x.trim()).filter(Boolean).forEach(x => p.append(k, x));
                else p.set(k, val);
            }
            return p.toString();
        }

        // The editable form: one object of filters per search, as typed.
        _draftInit() {
            const saved = this.o.saved && this.o.saved();
            const n = this.o.groups || 2;
            const blank = Array.from({ length: n }, (_, i) => ({ label: this.letter(i), queries: [] }));
            const groups = (saved && saved.groups.length ? saved.groups : blank)
                .map(g => ({ label: g.label, queries: (g.queries.length ? g.queries : ['']).map(q => this._qsToObj(q)) }));
            return { groups, within: saved && saved.within ? 1 : 0 };
        }

        // Replace the draft, e.g. when a saved rule brings its own groups.
        load(cmp) {
            this.draft = null;
            this.o.saved = () => cmp;
            this.draft = this._draftInit();
            this.counts = null;
            this.refresh();
            this._previewSoon();
        }

        // The draft in its URL form: {groups: [{label, queries: ["k=v&..."]}], within}.
        cmp() {
            const d = this._d();
            return { groups: d.groups.map((g, i) => ({ label: g.label.trim() || this.letter(i), queries: g.queries.map(q => this._objToQs(q)).filter(Boolean) })), within: d.within };
        }

        _d() {
            return this.draft || (this.draft = this._draftInit());
        }

        _cfSet(path, value) {
            const [gi, qi, key] = path.split('.'), g = this._d().groups[Number(gi)];
            if (qi === 'label') { g.label = value; return; }
            const q = g.queries[Number(qi)];
            if (key === '_raw') {
                for (const k of Object.keys(q)) if (!FILE_KNOWN.has(k)) delete q[k];
                Object.assign(q, this._qsToObj(value));
            } else q[key] = value;
        }

        // Counts under each group while typing: a preview call, nothing saved.
        _previewSoon() {
            clearTimeout(this._timer);
            this._timer = setTimeout(async () => {
                const msg = document.querySelector('[data-cmp-msg]');
                try {
                    this.counts = await this.post(this.cmp(), true);
                    this._refreshResult();
                    if (msg) msg.hidden = true;
                    if (this.o.onPreview) this.o.onPreview(this.counts, this.cmp());
                } catch (err) {
                    if (msg) { msg.textContent = String(err.message || err); msg.hidden = false; }
                }
            }, 450);
        }

        async run() {
            const cmp = this.cmp();
            this.err = '';
            const empty = cmp.groups.findIndex(g => !g.queries.length);
            if (empty >= 0) {
                this.err = `Give group ${this.letter(empty)} at least one filter.`;
                return this.refresh();
            }
            try {
                const out = await this.post(cmp);
                this.info = out.groups;
                this.o.onRun(cmp, out);
            } catch (err) {
                this.err = String(err.message || err);
                this.refresh();
            }
        }

        // The form. The host wraps it (a popover in the N-way panel, a card in YARA).
        html() {
            const s = this.o, on = !!(s.on && s.on()), dr = this._d();
            const counts = this.counts || (this.info ? { groups: this.info } : null);
            const min = s.minGroups || 2;
            const groups = dr.groups.map((g, gi) => {
                const qs = g.queries.map((q, qi) => {
                    const key = `${gi}.${qi}`;
                    const chips = this._qChips(q).map(c => `<span class="nway-tok"><b>${escapeHtml(FILE_LABELS[c.k] || c.k)}</b> ${escapeHtml(c.v)}<button type="button" data-tok-rm="${key}" data-k="${escapeAttr(c.k)}" data-v="${escapeAttr(c.v)}" aria-label="Remove filter">&times;</button></span>`).join('');
                    return `<div class="nway-cq">${qi ? '<span class="dim nway-cor">or</span>' : ''}<div class="nway-toks">${chips}<div class="nway-tokin"><input type="text" data-tok="${key}" placeholder="${chips ? 'add another filter' : 'tag, file name, batch, language... or key=value'}" autocomplete="off" spellcheck="false"><div class="nway-sug" hidden onmousedown="event.preventDefault()"></div></div></div>
                        ${g.queries.length > 1 ? `<button type="button" class="nway-blink" data-cmp-rmq="${key}" title="Remove this search">&times; remove search</button>` : ''}</div>`;
                }).join('');
                const L = this.letter(gi);
                return `<div class="nway-cg nway-cg-${gi % 2 ? 'b' : 'a'}"><div class="nway-cgh"><b class="nway-cgn">${escapeHtml(L)}</b><input type="text" class="nway-cmp-label" data-cf="${gi}.label" value="${escapeAttr(g.label)}" maxlength="30" aria-label="Name of group ${escapeAttr(L)}"><span class="dim" data-cnt="${gi}">${this._count(counts, gi, g)}</span>${dr.groups.length > min ? `<button type="button" class="nway-blink" data-cmp-rmg="${gi}" title="Remove group ${escapeAttr(L)}">&times; remove group</button>` : ''}</div>${qs}<button type="button" class="nway-blink" data-cmp-addq="${gi}" title="Files matching this search or the ones above count in group ${escapeAttr(L)}">+ or another search</button></div>`;
            }).join('');
            const ids = (s.within && s.within()) || [];
            const scope = ids.length ? `<div class="nway-cmp-scope"><span class="nway-lbl">Search in</span><span class="view-toggle"><button type="button" class="view-btn${dr.within ? '' : ' active'}" data-cmp-within="0">the whole collection</button><button type="button" class="view-btn${dr.within ? ' active' : ''}" data-cmp-within="1">only the ${ids.length} files open</button></span></div>` : '';
            const ctl = (s.extra && s.extra()) || '';
            return `${scope}<div data-cmp-bar>${this._bar()}</div>
                <div class="nway-cmp-split"><div class="nway-cmp-left">${groups}</div><div class="nway-cmp-right" data-cmp-right>${this._right()}</div></div>
                <div class="nway-chips">${dr.groups.length < GROUP_MAX ? '<button type="button" class="nway-chip" data-cmp-addg>+ add group</button>' : ''}<button type="button" class="nway-chip on" data-cmp-run>${escapeHtml(s.runLabel)}${counts && counts.files ? ` ${Number(counts.files)} files` : ''}</button>${on ? '<button type="button" class="nway-chip" data-cmp-clear>&times; clear</button>' : ''}</div>
                <div class="nway-note warn" data-cmp-msg${this.err ? '' : ' hidden'}>${escapeHtml(this.err || '')}</div>${ctl}`;
        }

        _count(counts, gi, g) {
            if (counts && counts.groups[gi] && g.queries.some(q => this._qChips(q).length)) return fileCount(counts.groups[gi].files);
            return '';
        }

        // Overlap bar for two groups: A only, both, B only.
        _bar() {
            const c = this.counts;
            if (!c || c.groups.length !== 2) return '';
            const [a, b] = c.groups.map(g => Number(g.files)), both = Number(c.overlap) || 0, tot = Math.max(1, a + b - both);
            const w = n => (n / tot * 100).toFixed(1);
            const warn = both && both >= tot / 2 ? `<span class="nway-cmp-warn">${both} files match both groups: the filters barely differ.</span>` : '';
            return `<div class="nway-cmp-bar" title="A only / in both / B only"><i class="ga" style="width:${w(a - both)}%"></i><i class="gab" style="width:${w(both)}%"></i><i class="gb" style="width:${w(b - both)}%"></i></div>
                <div class="nway-cmp-leg"><span class="ga">${escapeHtml(this.letter(0))} only ${a - both}</span><span class="gab">both ${both}</span><span class="gb">${escapeHtml(this.letter(1))} only ${b - both}</span>${warn}</div>`;
        }

        // The files the filters match now, tagged with the groups that hold each.
        _right() {
            const c = this.counts, dr = this._d();
            if (!c || !c.sample) return '<div class="dim nway-cmp-empty">Add a filter to a group to see the files it matches.</div>';
            const show = this.show;
            const opts = [['all', 'All']].concat(dr.groups.map((g, i) => [String(i), this.letter(i)]), dr.groups.length > 1 ? [['both', 'In several']] : []);
            const rows = c.sample.filter(f => show === 'all' || (show === 'both' ? f.groups.length > 1 : f.groups.includes(Number(show))));
            const mk = f => f.groups.map(i => `<b class="nway-mk nway-mk-${i % 2 ? 'b' : 'a'}">${escapeHtml(this.letter(i))}</b>`).join('');
            const body = rows.map(f => `<tr><td>${mk(f)}</td><td class="nway-cmp-fn">${escapeHtml(f.name)}</td><td class="dim">${escapeHtml(f.language)}</td><td>${f.tags.slice(0, 3).map(t => `<span class="nway-cmp-tag">${escapeHtml(t)}</span>`).join(' ')}</td></tr>`).join('');
            const cut = c.files > c.sample.length ? `<div class="dim">Showing ${c.sample.length} of ${Number(c.files)} files.</div>` : '';
            return `<div class="nway-cmp-tabs">${opts.map(([v, t]) => `<button type="button" class="nway-chip${show === v ? ' on' : ''}" data-cmp-show="${v}">${escapeHtml(t)}</button>`).join('')}</div>
                <div class="nway-cmp-files">${rows.length ? `<table><tr><th></th><th>File</th><th>Language</th><th>Tags</th></tr>${body}</table>` : '<div class="dim nway-cmp-empty">No file in this view.</div>'}</div>${cut}`;
        }

        // Re-draw the form in place; the popover stays open and filters keep their state.
        refresh() {
            const body = document.querySelector('.nway-cmp-body');
            if (body) body.innerHTML = this.html();
        }

        // Counts, bar and file list only: the filter boxes keep focus while the preview answers.
        _refreshResult() {
            const bar = document.querySelector('[data-cmp-bar]'), right = document.querySelector('[data-cmp-right]');
            if (bar) bar.innerHTML = this._bar();
            if (right) right.innerHTML = this._right();
            const counts = this.counts;
            this._d().groups.forEach((g, i) => { const el = document.querySelector(`[data-cnt="${i}"]`); if (el) el.textContent = this._count(counts, i, g); });
            const run = document.querySelector('[data-cmp-run]');
            if (run && counts) run.textContent = `${this.o.runLabel}${counts.files ? ` ${Number(counts.files)} files` : ''}`;
        }

        // A search's filters as chips: one per tag, one per other value.
        _qChips(q) {
            const out = [];
            for (const [k, v] of Object.entries(q)) {
                const val = String(v ?? '').trim();
                if (!val) continue;
                if (FILE_MULTI.has(k)) val.split(',').map(x => x.trim()).filter(Boolean).forEach(x => out.push({ k, v: x }));
                else out.push({ k, v: val });
            }
            return out;
        }

        _tokAdd(path, k, v) {
            const [gi, qi] = path.split('.').map(Number), q = this._d().groups[gi].queries[qi];
            if (FILE_MULTI.has(k)) {
                const list = (q[k] || '').split(',').map(x => x.trim()).filter(Boolean);
                if (!list.includes(v)) list.push(v);
                q[k] = list.join(',');
            } else q[k] = v;
            this.sug = null;
            this.refresh();
            const inp = document.querySelector(`[data-tok="${path}"]`);
            if (inp) inp.focus();
            this._previewSoon();
        }

        _tokRemove(path, k, v) {
            const [gi, qi] = path.split('.').map(Number), q = this._d().groups[gi].queries[qi];
            if (FILE_MULTI.has(k)) {
                const list = (q[k] || '').split(',').map(x => x.trim()).filter(x => x && x !== v);
                if (list.length) q[k] = list.join(','); else delete q[k];
            } else delete q[k];
            this.refresh();
            const inp = document.querySelector(`[data-tok="${path}"]`);
            if (inp) inp.focus();
            this._previewSoon();
        }

        // Suggestions for the text typed in a filter box: the matching values of each file field, with file counts.
        async _tokSuggest(input) {
            const path = input.dataset.tok, text = input.value.trim(), box = input.parentElement.querySelector('.nway-sug');
            const tag = this._sugTag = (this._sugTag || 0) + 1;
            const items = [];
            const eq = text.indexOf('=');
            if (eq > 0) items.push({ k: text.slice(0, eq).trim(), v: text.slice(eq + 1).trim(), hint: 'raw parameter' });
            else if (text) {
                items.push({ k: 'q', v: text, hint: 'keyword search' });
                if (text.includes('*')) FILE_FIELDS.filter(f => f[0] === 'file_tag' || f[0] === 'file_name').forEach(f => items.push({ k: f[0], v: text, hint: 'pattern' }));
            }
            const sc = this.scope(), coll = sc.collection || (this.o.collection && this.o.collection()) || '';
            if (coll && eq < 0) {
                const fields = [['file_tag', 'tag'], ['file_name', 'file_name'], ['batch_uuid', 'batch_uuid'], ['language_id', 'language_id'], ['filetype', 'filetype']];
                const got = await Promise.all(fields.map(async ([k, f]) => {
                    if (!text && f === 'file_name') return [];
                    try {
                        const res = await fetch(`/api/bin_sim/nway/values?collection=${encodeURIComponent(coll)}&field=${f}&q=${encodeURIComponent(text)}&limit=${text ? 5 : 4}`);
                        const out = res.ok ? await res.json() : { values: [] };
                        return out.values.map(x => ({ k, v: x.value, n: x.files }));
                    } catch (e) { return []; }
                }));
                if (tag !== this._sugTag) return;
                got.forEach(g => items.push(...g));
            }
            this.sug = { path, items, idx: 0 };
            this._sugDraw(box);
        }

        _sugDraw(box) {
            const sug = this.sug;
            if (!box) return;
            box.hidden = !sug || !sug.items.length;
            if (box.hidden) return;
            box.innerHTML = sug.items.map((x, i) => `<div class="nway-sug-i${i === sug.idx ? ' on' : ''}" data-sug-i="${i}"><b>${escapeHtml(FILE_LABELS[x.k] || x.k)}</b> <span class="nway-sug-v">${escapeHtml(x.v)}</span><small>${x.n != null ? fileCount(x.n) : escapeHtml(x.hint || '')}</small></div>`).join('');
        }

        _tokKey(e) {
            const t = e.target;
            if (!t.dataset || !t.dataset.tok) return false;
            const sug = this.sug && this.sug.path === t.dataset.tok ? this.sug : null, box = t.parentElement.querySelector('.nway-sug');
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                if (!sug) return this._tokSuggest(t), true;
                sug.idx = (sug.idx + (e.key === 'ArrowDown' ? 1 : sug.items.length - 1)) % sug.items.length;
                this._sugDraw(box);
                e.preventDefault();
            } else if (e.key === 'Enter') {
                const x = sug && sug.items[sug.idx];
                if (x) this._tokAdd(t.dataset.tok, x.k, x.v);
                else if (!t.value.trim()) this.run();
                e.preventDefault();
            } else if (e.key === 'Escape') {
                this.sug = null;
                if (box) box.hidden = true;
            } else if (e.key === 'Backspace' && !t.value) {
                const [gi, qi] = t.dataset.tok.split('.').map(Number), chips = this._qChips(this._d().groups[gi].queries[qi]);
                if (chips.length) this._tokRemove(t.dataset.tok, chips[chips.length - 1].k, chips[chips.length - 1].v);
            } else return false;
            return true;
        }

        // keydown / focus / toggle do not bubble through the host's own delegation: bind them on a stable ancestor.
        bind(node) {
            node.addEventListener('keydown', e => { if (!this._tokKey(e) && e.key === 'Enter' && e.target.dataset && e.target.dataset.cf) this.run(); });
            node.addEventListener('focusin', e => { if (e.target.dataset && e.target.dataset.tok) this._tokSuggest(e.target); });
            node.addEventListener('focusout', e => { if (e.target.dataset && e.target.dataset.tok) { const b = e.target.parentElement.querySelector('.nway-sug'); this.sug = null; if (b) b.hidden = true; } });
            node.addEventListener('toggle', e => {
                if (e.target.matches('details.nway-compare')) {
                    this.open = e.target.open;
                    if (e.target.open && !this.counts) this._previewSoon();
                }
            }, true);
            node.addEventListener('input', e => this.input(e));
            node.addEventListener('click', e => { const t = e.target.closest('[data-cmp-run],[data-tok-rm],[data-sug-i],[data-cmp-show],[data-cmp-addq],[data-cmp-rmq],[data-cmp-addg],[data-cmp-rmg],[data-cmp-within]'); if (t) this.click(t); });
        }

        input(e) {
            const t = e.target;
            if (t.dataset.cf) {
                this._cfSet(t.dataset.cf, t.value);
                this._previewSoon();
                return true;
            }
            if (t.dataset.tok) {
                clearTimeout(this._sugTimer);
                this._sugTimer = setTimeout(() => this._tokSuggest(t), 150);
                return true;
            }
            return false;
        }

        click(t) {
            const d = t.dataset, dr = this._d();
            if (d.cmpRun !== undefined) return this.run(), true;
            if (d.tokRm !== undefined) return this._tokRemove(d.tokRm, d.k, d.v), true;
            if (d.sugI !== undefined && this.sug) { const x = this.sug.items[Number(d.sugI)]; if (x) this._tokAdd(this.sug.path, x.k, x.v); return true; }
            if (d.cmpShow !== undefined) return this.show = d.cmpShow, this._refreshResult(), true;
            if (d.cmpAddq !== undefined) return dr.groups[Number(d.cmpAddq)].queries.push({}), this.refresh(), true;
            if (d.cmpRmq !== undefined) {
                const [gi, qi] = d.cmpRmq.split('.').map(Number);
                dr.groups[gi].queries.splice(qi, 1);
                this.refresh();
                return this._previewSoon(), true;
            }
            if (d.cmpAddg !== undefined) { if (dr.groups.length < GROUP_MAX) dr.groups.push({ label: this.letter(dr.groups.length), queries: [{}] }); return this.refresh(), true; }
            if (d.cmpRmg !== undefined) {
                if (dr.groups.length > (this.o.minGroups || 2)) dr.groups.splice(Number(d.cmpRmg), 1);
                this.refresh();
                return this._previewSoon(), true;
            }
            if (d.cmpWithin !== undefined) return dr.within = Number(d.cmpWithin), this.refresh(), this._previewSoon(), true;
            return false;
        }
    }

    window.NwayCompare = NwayCompare;
})();
