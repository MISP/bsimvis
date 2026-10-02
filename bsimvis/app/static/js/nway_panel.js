/**
 * N-way diff panel: functions shared across N files (GET /api/bin_sim/nway).
 *
 *   const panel = new NwayPanel(container, {
 *       source: { md5s: 'coll:md5,coll:md5', pool: null },  // any API member params
 *       state: { tab: 'core' },                              // initial controls
 *       onState: s => ...,                                   // fired on every control change
 *   });
 *   panel.load(); ... panel.destroy();
 *
 * `source` is passed to the API untouched, so a cluster entry only supplies
 * `{ cluster_uuid, collection | pool }`; `state.columns` (auto | files | children)
 * and `state.child_presence` are the cluster-only controls. `onOpenCluster(uuid)`
 * is called when a child cluster link in the too-many-members state is clicked.
 * `state.column` keeps the rows with a function in one file; `state.focus` (file
 * ids) splits the files into Focus vs Rest, both set from the file header's
 * right-click menu.
 * Rows are paged
 * server-side (Prev/Next, or appended on scroll -- see paging.js); nothing here
 * loads a full table. Function and file names come off
 * uploaded samples, so every value goes through escapeHtml / escapeAttr.
 */
(function () {
    const TABS = [['all', 'All'], ['core', 'Core'], ['partial', 'Partial'], ['unique', 'Unique']];
    const PAGE = 50;
    const SORTS = [['name', 'Function'], ['span', 'Span'], ['weight', 'Features'], ['support', 'Support'], ['cohesion', 'Cohesion']];
    const DEFAULTS = {
        tab: 'core', scope: 'code', k: 2, min_edge: '', mode: 'stored',
        q: '', tags: '', sort_col: '', sort_dir: 'desc', offset: 0,
        columns: 'auto', child_presence: 0.5,
        column: '', focus: '', focus_rule: 'any', side: '',
    };
    // State keys that go to the API as query params (empty ones are dropped).
    const API_KEYS = ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset', 'columns', 'child_presence', 'column', 'focus', 'focus_rule', 'side'];
    const LOW_SUPPORT = 0.5;
    const DOC_KEYS = ['mode', 'min_edge', 'columns', 'child_presence', 'focus', 'focus_rule'];

    function fnParts(fid) {
        // `{coll}:func:{md5}:{addr}` -- the collection can contain colons.
        const i = fid.indexOf(':func:');
        const rest = i < 0 ? '' : fid.slice(i + 6);
        const j = rest.indexOf(':');
        return { md5: rest.slice(0, j), addr: rest.slice(j + 1) };
    }

    function fnUrl(col, fid, pool) {
        const { addr } = fnParts(fid);
        const base = `${pool ? `/pools/${encodeURIComponent(pool)}` : ''}/collections/${encodeURIComponent(col.collection)}`;
        return `${base}/files/${encodeURIComponent(col.md5)}/functions/${encodeURIComponent(addr)}`;
    }

    function pct(v) {
        return `${(Number(v || 0) * 100).toFixed(0)}%`;
    }

    class NwayPanel {
        constructor(container, opts = {}) {
            this.el = container;
            this.source = opts.source || {};
            this.state = { ...DEFAULTS, ...(opts.state || {}) };
            this.onState = opts.onState || null;
            this.onOpenCluster = opts.onOpenCluster || null;
            this.data = null;
            this.error = null;
            this._gen = 0;
            this._debounce = null;
            this._io = null;
            this._loading = false;
            if (window.Paging.mode() === 'scroll') this.state.offset = 0;
            this.el.addEventListener('click', e => this._onClick(e));
            this.el.addEventListener('change', e => this._onChange(e));
            this.el.addEventListener('input', e => this._onInput(e));
            window.Paging.bind(this.el, () => ({ offset: Number(this.state.offset) || 0, total: Number(this.data && this.data.total) || 0, size: PAGE }), off => this._set({ offset: off }));
            this.el.addEventListener('toggle', e => {
                if (e.target.matches('details.nway-focus')) this._focusOpen = e.target.open;
            }, true);
            this.el.addEventListener('contextmenu', e => {
                const th = e.target.closest('th[data-col]');
                if (th) this._menu(e, th.dataset.col);
            }, true);
            this.el.addEventListener('keydown', e => {
                if (e.key === 'Enter' && e.target.dataset.text) this._set({ [e.target.dataset.text]: e.target.value.trim(), offset: 0 });
            });
        }

        destroy() {
            this._gen++;
            clearTimeout(this._debounce);
            if (this._io) this._io.disconnect();
            this._cgClose();
        }

        _set(patch) {
            Object.assign(this.state, patch);
            const s = { ...this.state };
            // Scroll mode keeps the offset out of the URL (and out of the state).
            if (window.Paging.mode() === 'scroll') { this.state.offset = 0; delete s.offset; }
            if (this.onState) this.onState(s);
            this.load();
        }

        // `append` (scroll mode): fetch the next batch after the rows already shown.
        async load(append = false) {
            const scroll = window.Paging.mode() === 'scroll';
            if (append && (this._loading || !this.data)) return;
            const gen = ++this._gen;
            this._loading = append;
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of API_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            qs.set('limit', PAGE);
            if (scroll) qs.set('offset', append ? this.data.items.length : 0);
            if (!this.data) this.el.innerHTML = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Matching functions...</div>';
            try {
                const res = await fetch(`/api/bin_sim/nway?${qs.toString()}`);
                const body = await res.json().catch(() => ({}));
                if (gen !== this._gen) return;
                this._loading = false;
                if (append && res.ok) return this._append(body);
                if (append) return;
                if (!res.ok) {
                    this.error = { status: res.status, ...body };
                    this.data = null;
                } else {
                    this.error = null;
                    this.data = body;
                }
            } catch (e) {
                if (gen !== this._gen) return;
                this._loading = false;
                if (append) return;
                this.error = { status: 0, message: String(e) };
                this.data = null;
            }
            this.render();
        }

        _renderError() {
            const e = this.error;
            if (e.status === 413) {
                const kids = (e.children || []).map(c => {
                    const uuid = c.cluster_uuid || c.uuid || '';
                    const name = escapeHtml(c.cluster_name || c.name || uuid);
                    const link = uuid && this.onOpenCluster ? `<a href="#" class="btn-action" data-open-cluster="${escapeAttr(uuid)}">${name}</a>` : name;
                    return `<li>${link}${c.member_count ? ` <span class="dim">${Number(c.member_count)} files</span>` : ''}</li>`;
                }).join('');
                return `<div class="nway-note warn"><b>Too many members</b> (${Number(e.members) || '?'}): pick fewer files${kids ? ' or open a child cluster:' : '.'}${kids ? `<ul>${kids}</ul>` : ''}</div>`;
            }
            const msg = e.missing ? `Unknown files: ${e.missing.join(', ')}` : (e.message || e.error || `HTTP ${e.status}`);
            return `<div class="nway-note err">${escapeHtml(msg)}</div>`;
        }

        _count(tab) {
            const c = (this.data.counts || {})[tab] || { code: 0, library: 0 };
            return `${c.code} / ${c.library}`;
        }

        // The shared (i) hover hint (`.home-tip`, as on the home page).
        _tip(text) {
            return text ? `<span class="home-tip" tabindex="0" data-tip="${escapeAttr(text)}"><i class="fa-solid fa-circle-info"></i></span>` : '';
        }

        // One segmented toggle (the shared .view-toggle); `data-set="key:value"`.
        _seg(label, key, options, tip) {
            const cur = String(this.state[key]);
            const btns = options.map(([v, text, t]) =>
                `<button class="view-btn${cur === v ? ' active' : ''}" data-set="${escapeAttr(`${key}:${v}`)}"${t ? ` title="${escapeAttr(t)}"` : ''}>${escapeHtml(text)}</button>`
            ).join('');
            return `<span class="nway-ctlwrap"><div class="view-toggle"><span class="nway-lbl">${escapeHtml(label)}</span>${btns}</div>${this._tip(tip)}</span>`;
        }

        // Coloured icon pills, the same look as the other Code / Library / Overall
        // score-axis pickers (`binSimPillStyle`); `data-set` goes through _onClick.
        _pills(label, key, options, tip) {
            const cur = String(this.state[key]);
            const pills = options.map(([v, text, icon, color]) =>
                `<span class="bsim-tag-pill" data-set="${escapeAttr(`${key}:${v}`)}" style="${window.binSimPillStyle(cur === v, color)} padding:4px 10px; font-size:0.78rem;"><i class="${escapeAttr(icon)}"></i>${escapeHtml(text)}</span>`
            ).join('');
            return `<span class="nway-ctlwrap"><div class="nway-pills"><span class="nway-lbl">${escapeHtml(label)}</span>${pills}</div>${this._tip(tip)}</span>`;
        }

        _slider(label, key, value, min, max, step, tip) {
            const shown = key === 'k' ? String(value) : Number(value).toFixed(2);
            return `<span class="nway-ctlwrap"><label class="nway-ctl"><span class="nway-lbl">${escapeHtml(label)}</span><b>${shown}</b><input type="range" min="${min}" max="${max}"${step ? ` step="${step}"` : ''} value="${Number(value)}" data-range="${key}"></label>${this._tip(tip)}</span>`;
        }

        _group(label, inner) {
            return `<div class="nway-group"><span class="nway-gl">${escapeHtml(label)}</span>${inner}</div>`;
        }

        _controls() {
            const s = this.state, d = this.data;
            const n = d.columns.length;
            const focus = d.columns_mode === 'focus';
            const isNode = !!this.source.cluster_uuid;
            const tabs = TABS.filter(([key]) => !(focus && key === 'partial')).map(([key, label]) =>
                `<button class="bsim-tab${s.tab === key ? ' active' : ''}" data-tab="${key}">${label}<span class="nway-count" title="code / library">${this._count(key)}</span></button>`
            ).join('');
            const tabTip = focus
                ? 'All: every function. Core: on both the focus and the rest side. Unique: on one side only. Counts are code / library.'
                : `All: every function group. Core: present in all ${n} columns. Partial: present in at least k columns. Unique: present in exactly one file. Counts are code / library.`;
            const filter = [
                `<input type="text" class="nway-input" data-text="q" placeholder="function name / address" value="${escapeAttr(s.q)}">`,
                `<input type="text" class="nway-input" data-text="tags" placeholder="tags" value="${escapeAttr(s.tags)}">`,
                this._pills('Show', 'scope', [['code', 'Code', 'fa-solid fa-code', 'var(--info, #3b82f6)'], ['library', 'Library', 'fa-solid fa-cubes', 'var(--warning, #d97706)'], ['all', 'All', 'fa-solid fa-layer-group', 'var(--success)']], 'Code: functions of the program itself. Library: functions recognised as library code (stdlib, statically linked). All: both.'),
                this._columnChip(),
            ].join('');
            const match = [
                this._slider('Min edge', 'min_edge', Number(d.min_edge), 0, 1, 0.05, 'Ignore function matches below this similarity. Higher is stricter: fewer functions get grouped together.'),
                this._seg('Mode', 'mode', [['stored', 'Stored', 'reads built pair docs'], ['virtual', 'Virtual', 'recomputes from vectors']], 'Stored reads the similarity pairs already built. Virtual recomputes the matches from the function vectors, as a fresh collection of just these files would.'),
                s.tab === 'partial' && !focus ? this._slider(`In at least (of ${n})`, 'k', s.k, 2, Math.max(2, n), 1, 'Partial tab: keep functions present in at least this many columns.') : '',
                isNode ? this._seg('Columns', 'columns', [['auto', 'Auto'], ['files', 'Files'], ['children', 'Children']], "Files: one column per file of the node. Children: one column per child cluster. Auto switches to Children above 12 files.") : '',
                isNode && d.columns_mode === 'children'
                    ? this._slider('Child presence', 'child_presence', s.child_presence, 0.05, 1, 0.05, "Share of a child cluster's files that must hold a function for the child to count as having it.") : '',
            ].join('');
            return `<div class="nway-tabrow"><div class="bsim-tabbar">${tabs}</div>${this._tip(tabTip)}</div>
                <div class="nway-bar">${this._group('Filter', filter)}${this._group('Match', match)}</div>
                ${this._focusBox()}`;
        }

        _files() {
            return this.data.file_columns || this.data.columns.filter(c => !c.kind);
        }

        _focusIds() {
            return String(this.state.focus || '').split(',').filter(Boolean);
        }

        _columnChip() {
            const only = this.state.column && this._files().find(f => f.id === this.state.column);
            return only ? `<button class="nway-chip on" data-clear="column" title="drop the file filter">Only ${escapeHtml(only.file_name || only.md5)} &times;</button>` : '';
        }

        // Focus: pick files to compare against all the others. A collapsible box so
        // 30 files do not push the table down; opens itself while a focus is set.
        _focusBox() {
            const s = this.state, files = this._files(), ids = this._focusIds();
            const on = this.data.columns_mode === 'focus';
            const chips = files.map(f => `<button class="nway-chip${ids.includes(f.id) ? ' on' : ''}" data-focus-toggle="${escapeAttr(f.id)}" title="${escapeAttr(f.collection)}">${escapeHtml(f.file_name || f.md5)}</button>`).join('');
            const ctl = on ? [
                this._seg('Focus holds', 'focus_rule', [['any', 'Any'], ['all', 'All']], 'Any: a function is on the focus side when at least one focus file has it. All: every focus file must have it.'),
                this._slider('Rest presence', 'child_presence', s.child_presence, 0.05, 1, 0.05, 'Share of the rest files that must hold a function for it to count on the rest side. Keep it low (a single rest file is enough) for a strict "unique to focus".'),
                s.tab === 'unique' ? this._seg('Unique to', 'side', [['', 'Both'], ['focus', 'Focus'], ['rest', 'Rest']], 'Unique tab only. Focus: functions in the focus files and in none of the rest. Rest: the reverse. Both: either.') : '',
                `<button class="nway-chip" data-clear="focus" title="back to the plain N-way">&times; clear focus</button>`,
            ].join('') : '';
            const open = on || this._focusOpen ? ' open' : '';
            const tip = 'Compare the chosen files against all the others. Columns collapse to Focus and Rest: Core is on both sides, Unique on one side. Right-click a file header for the same action.';
            return `<details class="nway-focus"${open}><summary><i class="fa-solid fa-bullseye"></i> Focus: ${on ? `${ids.length} of ${files.length} files` : 'off'}${this._tip(tip)}</summary>
                <div class="nway-focus-body"><div class="nway-chips">${chips}</div>${ctl ? `<div class="nway-bar">${ctl}</div>` : ''}</div></details>`;
        }

        // Header right-click: the shared file menu, plus this panel's focus / filter
        // actions (rendered by context_menu.js from `__nway`).
        _menu(e, id) {
            e.preventDefault();
            e.stopPropagation();
            const f = this._files().find(x => x.id === id);
            if (!f || !window.showGraphContextMenu) return;
            NwayPanel._active = this;
            const focus = this._focusIds();
            window.showGraphContextMenu(e, 'file', {
                md5: f.md5, file_name: f.file_name, collection: f.collection,
                fileId: `${f.collection}:file:${f.md5}`,
                __nway: { id, focused: focus.includes(id), anyFocus: focus.length > 0, filtered: this.state.column === id },
            });
        }

        _act(act, id) {
            const focus = this._focusIds();
            if (act === 'column') return this._set({ column: this.state.column === id ? '' : id, offset: 0 });
            this._setFocus(act === 'add' ? [...focus, id] : focus.filter(f => f !== id), this._files().length);
        }

        // Focus keeps at least one file on each side; the tab resets because
        // Partial does not exist in focus mode.
        _setFocus(ids, n) {
            if (ids.length >= n) return;
            this._set({ focus: ids.join(','), side: '', offset: 0, sort_col: '', tab: ids.length && this.state.tab === 'partial' ? 'core' : this.state.tab });
        }

        _notes() {
            const d = this.data;
            let html = '';
            if (d.mode !== this.state.mode) html += `<div class="nway-note">Ran in <b>${escapeHtml(d.mode)}</b> mode: files span several collections.</div>`;
            if (d.fallback_pairs) html += `<div class="nway-note">${Number(d.fallback_pairs)} pairs computed on the fly (no stored pair doc).</div>`;
            if ((d.warnings || []).length) {
                html += `<div class="nway-note warn">${d.warnings.map(w => `<div>${escapeHtml(w)}</div>`).join('')}</div>`;
            }
            return html;
        }

        // The compact function entity: the same data, hover preview, click and
        // right-click menu EntityRenderer.renderFunction gives, with the address
        // as the label (the row already carries the name).
        _fnData(fid, col) {
            const m = this.data.functions_metadata[fid] || {};
            const addr = fnParts(fid).addr;
            return {
                function_id: fid, function_name: m.name || addr, namespace: m.namespace || '',
                parameters: m.parameters || [], return_type: m.return_type || '',
                entrypoint_address: m.entrypoint_address || addr, file_md5: col.md5,
                collection: col.collection, bsim_features_count: m.bsim_features_count || 0,
                tags: m.tags || [], user_tags: m.user_tags || [],
            };
        }

        _fnEntity(fid, col, rowName) {
            const f = this._fnData(fid, col);
            // Default names (FUN_, sub_, thunk_) add nothing next to the address.
            const note = /^(FUN_|sub_|thunk_)/i.test(f.function_name) ? ''
                : f.function_name === rowName
                    ? ' <span class="dim" title="same name as the function column">(*)</span>'
                    : ` <span class="dim">(${escapeHtml(f.function_name)})</span>`;
            const sig = typeof formatSigComponent === 'function'
                ? formatSigComponent(f.namespace, f.return_type, f.function_name, f.parameters).fullSig : f.function_name;
            return `<span class="entity-function" title="${escapeAttr(sig)}" data-etype="function" data-eid="${escapeAttr(fid)}"
                    data-entity-data='${escapeAttr(JSON.stringify(f))}'
                    oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>
                <b class="entity-name nway-addr"
                   onmouseenter="typeof showCodePreview === 'function' && showCodePreview(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, ${escapeAttr(jsString(f.entrypoint_address))}, ${escapeAttr(jsString(col.md5))}, ${Number(f.bsim_features_count) || 0}, event)"
                   onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)"
                   onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"
                   onclick="typeof showFunctionCodeById === 'function' && showFunctionCodeById(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, '', event)">@ ${escapeHtml(f.entrypoint_address)}</b>${note}</span>`;
        }

        // A child/direct column: "m/n files", plus links to the functions of the
        // files in that group (a few; the rest as a count).
        _groupCell(row, col) {
            const cell = row.cells[col.id];
            const pool = this.source.pool || null;
            const links = [];
            for (const f of this.data.file_columns || []) {
                const fid = col.members.includes(f.id) && (row.files || {})[f.id];
                if (!fid) continue;
                const data = this._fnData(fid, f);
                const url = fnUrl(f, fid, pool);
                links.push(`<a data-nav="${escapeAttr(url)}" data-title="${escapeAttr(f.file_name)}" title="${escapeAttr(f.file_name)}"
                    data-etype="function" data-eid="${escapeAttr(fid)}" data-entity-data='${escapeAttr(JSON.stringify(data))}'
                    oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>${escapeHtml(data.entrypoint_address)}</a>`);
            }
            const more = links.length > 3 ? `<span class="dim">+${links.length - 3}</span>` : '';
            return `<td class="col${cell.on ? '' : ' dim'}"><b>${Number(cell.present)}/${Number(cell.total)}</b> files${links.length ? `<div class="nway-group-links">${links.slice(0, 3).join('')}${more}</div>` : ''}</td>`;
        }

        _cell(row, col, ri) {
            if (col.kind) return this._groupCell(row, col);
            const fid = row.cells[col.id];
            if (!fid) return '<td class="col gap">&mdash;</td>';
            return `<td class="col"><div class="nway-fn">${this._fnEntity(fid, col, row.name)}</div></td>`;
        }

        // Support / cohesion use the same red-to-green ramp as the other score cells.
        _score(v) {
            let color = '';
            try { color = typeof window.scoreColor === 'function' ? window.scoreColor(v) : ''; } catch (e) { color = ''; }
            return `<span${color ? ` style="color:${escapeAttr(color)}"` : ''}>${pct(v)}</span>`;
        }

        // The row's guessed signature: the most common return type and parameter
        // list over its functions. The best function carries that signature (ties
        // broken by feature count) and feeds the hover preview.
        _rowSig(row) {
            const src = row.files || row.cells;
            const sigOf = m => formatSigComponent('', m.return_type, '', m.parameters);
            const items = [];
            for (const col of this.data.file_columns || this.data.columns.filter(c => !c.kind)) {
                const fid = src[col.id];
                const m = typeof fid === 'string' && this.data.functions_metadata[fid];
                if (m) items.push({ fid, col, m, ret: sigOf(m).ret, params: JSON.stringify(sigOf(m).params) });
            }
            if (!items.length) return null;
            const mode = key => {
                const n = {};
                items.forEach(i => { n[i[key]] = (n[i[key]] || 0) + 1; });
                return Object.entries(n).sort((x, y) => y[1] - x[1])[0][0];
            };
            const ret = mode('ret'), params = mode('params');
            const fits = items.filter(i => i.ret === ret && i.params === params);
            const best = (fits.length ? fits : items).sort((x, y) => (y.m.bsim_features_count || 0) - (x.m.bsim_features_count || 0))[0];
            return { ret, params: JSON.parse(params), best };
        }

        _nameSig(row, opts = {}) {
            const sig = this._rowSig(row);
            const name = escapeHtml(row.name);
            if (!sig) return name;
            const f = this._fnData(sig.best.fid, sig.best.col);
            // Same renderer as the function search table, fed the guessed signature.
            const fn = window.EntityRenderer.renderFunction({ ...f, function_name: row.name, return_type: sig.ret, parameters: sig.params }, { hideNote: true, showActions: false, ...opts });
            // Tags land on the best candidate, the function the preview shows.
            const tags = window.EntityRenderer.renderTag('function', f.function_id, f.tags, f.user_tags);
            return `${fn}<div class="nway-tags" title="tags apply to the best candidate: ${escapeAttr(f.entrypoint_address)}">${tags}</div>`;
        }

        // ---- Call-graph drawer: callers / callees of one row, as a list or a graph ----
        _cgFiles() {
            return this.data.file_columns || this.data.columns.filter(c => !c.kind);
        }

        _cgFocus() {
            return this._cg.stack[this._cg.stack.length - 1];
        }

        _cgOpen(row) {
            const cells = {};
            for (const [k, v] of Object.entries(row.files || row.cells)) if (typeof v === 'string') cells[k] = v;
            this._cg = { stack: [{ name: row.name, cells }], view: (this._cg && this._cg.view) || 'graph', scope: (this._cg && this._cg.scope) || this.state.scope || 'code', nb: null };
            if (!this._dw) {
                this._dw = document.createElement('aside');
                this._dw.className = 'nway-drawer';
                this._dw.addEventListener('click', e => this._onDrawerClick(e));
                document.body.appendChild(this._dw);
                this._escKey = e => { if (e.key === 'Escape') this._cgClose(); };
                document.addEventListener('keydown', this._escKey);
            }
            this._cgShow();
        }

        _cgClose() {
            if (this._dw) this._dw.remove();
            this._dw = null;
            if (this._escKey) document.removeEventListener('keydown', this._escKey);
            this._cg = null;
        }

        async _cgFetch(focus, role) {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of DOC_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            qs.set('fids', Object.values(focus.cells).join(','));
            qs.set('role', role);
            const res = await fetch(`/api/bin_sim/nway/neighbors?${qs.toString()}`);
            const body = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(body.message || body.error || `HTTP ${res.status}`);
            return body;
        }

        async _cgShow() {
            const focus = this._cgFocus();
            const tok = this._cgTok = (this._cgTok || 0) + 1;
            this._cg.nb = null;
            this._cgDraw();
            try {
                const [callers, callees] = await Promise.all([this._cgFetch(focus, 'callers'), this._cgFetch(focus, 'callees')]);
                if (tok !== this._cgTok || !this._cg) return;
                Object.assign(this.data.functions_metadata, callers.functions_metadata, callees.functions_metadata);
                this._cg.nb = { callers, callees };
                this._cgDraw();
            } catch (err) {
                if (tok !== this._cgTok || !this._cg) return;
                this._cgDraw(String(err.message || err));
            }
        }

        // One square per file: lit when that file's function has the edge.
        _cgStrip(on, files) {
            const n = on.filter(Boolean).length;
            const cls = n === files.length ? 'r-hi' : n * 2 >= files.length ? 'r-mid' : 'r-low';
            const sq = files.map((c, i) => `<i class="${on[i] ? 'on' : ''}" title="${escapeAttr(c.file_name || c.md5 || '')}"></i>`).join('');
            return { html: `<span class="nway-strip ${cls}">${sq}</span>`, n, cls };
        }

        _cgName(it) {
            if (it.kind === 'row') return this._nameSig({ name: it.name, cells: it.cells }, { noClick: true });
            if (it.kind === 'ext') return `<span class="nway-ext" title="imported symbol, matched by name">${escapeHtml(it.name)}<span class="nway-badge">ext</span></span>`;
            return `<span class="dim" title="not part of any row in this view">${escapeHtml(it.name || '?')} (no row)</span>`;
        }

        _cgReach(it, focus, files) {
            const on = files.map(c => { const f = focus.cells[c.id]; return !!(f && it.sources[f]); });
            const st = this._cgStrip(on, files);
            return `${st.html}<span class="nway-reach ${st.cls}" title="files whose function has this edge">${Number(it.support)}/${Number(it.of)}</span>`;
        }

        // Neighbors of one role under the Code / Library / All scope, keeping their index in the full list.
        _cgShown(nb, role) {
            const scope = this._cg.scope;
            return nb[role].items.map((it, i) => ({ it, i, out: !(scope === 'all' || (scope === 'library') === !!it.library) }));
        }

        _cgList(nb, focus, files) {
            const box = (role, label, icon) => {
                const shown = this._cgShown(nb, role).filter(x => !x.out);
                const rows = shown.map(({ it, i }) => `<div class="nway-cg-item${it.kind === 'row' ? '' : ' static'}" ${it.kind === 'row' ? `tabindex="0" data-cg-go="${role}:${i}"` : ''}>
                    <span class="nway-cg-l">${this._cgName(it)}</span><span class="nway-cg-r">${this._cgReach(it, focus, files)}</span></div>`).join('');
                return `<section class="nway-cg-box"><header><i class="fa-solid ${icon}"></i> ${label} <span class="dim">(${shown.length})</span></header>${rows || '<div class="dim nway-cg-none">none in this scope</div>'}</section>`;
            };
            return box('callers', 'Callers', 'fa-right-to-bracket') + box('callees', 'Callees', 'fa-right-from-bracket');
        }

        _cgGraph(nb, focus, files) {
            const W = 800, BW = 230, PAD = 0, NW = BW, NH = 104, GAP = 12, CAP = 12, T = 0, CW = 230;
            // Out-of-scope neighbors stay in the graph, faded and after the in-scope ones.
            const lists = { callers: this._cgShown(nb, 'callers').sort((a, b) => a.out - b.out), callees: this._cgShown(nb, 'callees').sort((a, b) => a.out - b.out) };
            const shown = { callers: lists.callers.slice(0, CAP), callees: lists.callees.slice(0, CAP) };
            const H = T + Math.max(shown.callers.length, shown.callees.length, 1) * (NH + GAP) + GAP;
            const cx = (W - CW) / 2, cy = (H - NH) / 2;
            const nodes = [], edges = [];
            for (const [role, bx] of [['callers', 0], ['callees', W - BW]]) {
                const list = shown[role];
                const x = bx + PAD;
                const y0 = T + (H - T - (list.length * (NH + GAP) - GAP)) / 2;
                list.forEach(({ it, i, out }, k) => {
                    const y = y0 + k * (NH + GAP);
                    const on = files.map(c => { const f = focus.cells[c.id]; return !!(f && it.sources[f]); });
                    const st = this._cgStrip(on, files);
                    const [x1, x2] = role === 'callers' ? [x + NW, cx] : [cx + CW, x];
                    const [y1, y2] = role === 'callers' ? [y + NH / 2, cy + NH / 2] : [cy + NH / 2, y + NH / 2];
                    const mx = (x1 + x2) / 2;
                    edges.push(`<path class="nway-edge ${st.cls}${out ? ' out' : ''}" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" style="stroke-width:${(1 + st.n * 0.8).toFixed(1)}"></path>`);
                    nodes.push(`<div class="nway-node${it.kind === 'row' ? '' : ' static'}${out ? ' out' : ''}" ${it.kind === 'row' ? `tabindex="0" data-cg-go="${role}:${i}"` : ''} style="left:${x}px; top:${y}px; width:${NW}px; height:${NH}px">
                        <div class="nway-node-name">${this._cgName(it)}</div><div class="nway-node-sub">${this._cgReach(it, focus, files)}</div></div>`);
                });
            }
            const more = ['callers', 'callees'].map(r => lists[r].length > CAP ? `${lists[r].length - CAP} more ${r} (see the list view)` : '').filter(Boolean).join('; ');
            return `<div class="dim" style="margin-bottom:6px">Edge width is the number of files that share the edge. Click a node to recenter.</div>
                <div class="nway-graph" style="height:${H}px; width:${W}px"><svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${edges.join('')}</svg>${nodes.join('')}
                <div class="nway-node center" style="left:${cx}px; top:${cy}px; width:${CW}px; height:${NH}px"><div class="nway-node-name">${this._nameSig({ name: focus.name, cells: focus.cells }, { noClick: true })}</div><div class="nway-node-sub dim">focus</div></div></div>
                ${more ? `<div class="dim" style="margin-top:6px">${escapeHtml(more)}</div>` : ''}`;
        }

        _cgDraw(err) {
            if (!this._dw || !this._cg) return;
            const st = this._cg, focus = this._cgFocus(), files = this._cgFiles();
            const crumbs = st.stack.map((f, i) => `<button class="nway-crumb" data-cg-crumb="${i}">${escapeHtml(f.name)}</button>`).join('<span class="dim">/</span>');
            const scopeToggle = [['code', 'Code', 'fa-solid fa-code', 'var(--info, #3b82f6)'], ['library', 'Library', 'fa-solid fa-cubes', 'var(--warning, #d97706)'], ['all', 'All', 'fa-solid fa-layer-group', 'var(--success)']].map(([v, text, icon, color]) => `<span class="bsim-tag-pill" data-cg-scope="${v}" style="${window.binSimPillStyle(st.scope === v, color)} padding:4px 10px; font-size:0.78rem;"><i class="${icon}"></i>${text}</span>`).join('');
            const toggle = ['list', 'graph'].map(v => `<button class="view-btn${st.view === v ? ' active' : ''}" data-cg-view="${v}">${v === 'list' ? 'List' : 'Graph'}</button>`).join('');
            const on = files.map(c => !!focus.cells[c.id]);
            const fs = this._cgStrip(on, files);
            let body;
            if (err) body = `<div style="color:#f92672;">${escapeHtml(err)}</div>`;
            else if (!st.nb) body = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Reading call graph...</div>';
            else body = st.view === 'graph' ? this._cgGraph(st.nb, focus, files) : this._cgList(st.nb, focus, files);
            this._dw.innerHTML = `<div class="nway-cg-head"><div class="nway-cg-crumbs">${crumbs}</div><div class="nway-pills" title="Code: the program's own functions. Library: library code and imports. All: both.">${scopeToggle}</div><div class="view-toggle" style="margin:0">${toggle}</div><button class="nway-chip" data-cg-close title="close (Esc)">&times;</button></div>
                <div class="nway-cg-focus">${this._nameSig({ name: focus.name, cells: focus.cells }, { noClick: true })}<div>${fs.html} <span class="dim">present in ${fs.n}/${files.length} files</span></div></div>
                <div class="nway-cg-body">${body}</div>`;
        }

        _onDrawerClick(e) {
            const t = e.target.closest('[data-cg-go],[data-cg-view],[data-cg-scope],[data-cg-crumb],[data-cg-close]');
            if (!t || !this._cg) return;
            if (t.dataset.cgClose !== undefined) return this._cgClose();
            if (t.dataset.cgView) { this._cg.view = t.dataset.cgView; return this._cgDraw(); }
            if (t.dataset.cgScope) { this._cg.scope = t.dataset.cgScope; return this._cgDraw(); }
            if (t.dataset.cgCrumb) {
                this._cg.stack = this._cg.stack.slice(0, Number(t.dataset.cgCrumb) + 1);
                return this._cgShow();
            }
            const [role, i] = t.dataset.cgGo.split(':');
            const it = this._cg.nb && this._cg.nb[role].items[Number(i)];
            if (!it || it.kind !== 'row') return;
            const cells = {};
            for (const [k, v] of Object.entries(it.cells)) if (typeof v === 'string') cells[k] = v;
            this._cg.stack.push({ name: it.name, cells });
            this._cgShow();
        }

        _row(row, ri) {
            const cols = this.data.columns;
            const extra = Array.isArray(row.names) ? row.names.length - 1 : Number(row.names || 0) - 1;
            const low = row.support < LOW_SUPPORT && (row.file_span || row.span) > 2
                ? `<i class="fa-solid fa-triangle-exclamation nway-warn" title="low support: only ${pct(row.support)} of the possible pairs in this row are matched; it may be a chain of transitive matches"></i>` : '';
            return `<tr>
                <td class="nway-open" data-cg-open="${Number(ri)}" title="Click for the call graph of this row" style="min-width:260px; max-width:420px;">${this._nameSig(row, { noClick: true })}<span class="nway-name">${extra > 0 ? `<span class="dim">+${extra} names</span>` : ''}${low}</span></td>
                <td class="num">${Number(row.span)}</td>
                <td class="num">${Number(row.weight).toFixed(0)}</td>
                <td class="num">${this._score(row.support)}</td>
                <td class="num">${this._score(row.cohesion)}</td>
                <td class="cluster-cards-cell">${window.EntityRenderer.renderClusterCard((row.clusters || []).map(u => this.data.clusters[u]).filter(Boolean))}</td>
                ${cols.map(c => this._cell(row, c, ri)).join('')}
            </tr>`;
        }

        _head() {
            const s = this.state;
            const th = ([key, label]) => {
                const arrow = s.sort_col === key ? (s.sort_dir === 'asc' ? '▲' : '▼') : '↕';
                return `<th class="sortable${key === 'name' ? '' : ' num'}" data-sort="${key}">${label} <span class="dim">${arrow}</span></th>`;
            };
            const fileHead = c => {
                const named = c.file_name && c.file_name !== c.md5;
                const name = named ? window.EntityRenderer.renderFileName(c.file_name, c.md5, c.collection) : '';
                const coverage = c.coverage == null ? '' : ` &middot; <span title="share of the shared (span >= 2) weight this file holds">${(Number(c.coverage) * 100).toFixed(0)}% shared</span>`;
                return `<th class="col" data-col="${escapeAttr(c.id)}" title="${escapeAttr(`${c.file_name} (${c.collection}) - right-click: focus / filter`)}"><div class="nway-colhead">${name}${window.EntityRenderer.renderMd5(c.md5, { collection: c.collection })}<span class="nway-sub">${Number(c.functions)} fn${coverage}</span></div></th>`;
            };
            const names = c => (this.data.file_columns || []).filter(f => c.members.includes(f.id)).map(f => f.file_name || f.md5).join(', ');
            const colHead = c => c.kind
                ? `<th class="col" title="${escapeAttr(c.kind === 'focus' || c.kind === 'rest' ? names(c) : c.kind === 'child' ? 'child cluster' : 'files at this node, in no child cluster')}"><div class="nway-colhead"><span class="nway-colname">${escapeHtml(c.label)}</span><span class="nway-sub">${Number(c.member_count)} files</span></div></th>`
                : fileHead(c);
            return `<tr>${SORTS.map(th).join('')}<th>Cluster</th>${this.data.columns.map(colHead).join('')}</tr>`;
        }

        render() {
            if (this.error) {
                this.el.innerHTML = `<div class="nway-panel">${this._renderError()}</div>`;
                return;
            }
            const d = this.data;
            const rows = d.items.map((r, i) => this._row(r, i)).join('');
            const off = Number(d.offset), total = Number(d.total);
            const scroll = window.Paging.mode() === 'scroll';
            this.el.innerHTML = `<div class="nway-panel">${this._controls()}${this._notes()}
                <div class="nway-card table-scope">
                    <div class="nway-scroll">
                        <table id="nway-table" class="nway-table">
                            <thead>${this._head()}</thead>
                            <tbody>${rows || `<tr><td colspan="${d.columns.length + 7}" class="gap">No rows in this tab.</td></tr>`}</tbody>
                            ${scroll ? `<tfoot><tr><td class="nway-sentinel" colspan="${d.columns.length + 7}"></td></tr></tfoot>` : ''}
                        </table>
                    </div>
                    <div class="table-footer">
                        <div class="table-footer-left">${window.Paging.footer({ offset: off, total, size: PAGE, shown: d.items.length })}</div>
                        <div class="table-footer-right">
                            <span class="table-footer-sel selection-stats" style="display:none;"></span>
                        </div>
                    </div>
                </div></div>`;
            // The shared grid selection: it reads the function entities in the
            // selected cells, so the bulk actions of the function menu apply.
            if (window.TableSelection) new window.TableSelection('nway-table');
            this._watch();
        }

        // Scroll mode: load the next batch when the sentinel nears the viewport.
        _watch() {
            if (this._io) this._io.disconnect();
            const s = this.el.querySelector('.nway-sentinel');
            if (!s) return;
            if (this.data.items.length >= Number(this.data.total)) return s.remove();
            this._io = window.Paging.observe(s, () => this.load(true));
        }

        _append(body) {
            const d = this.data;
            Object.assign(d.functions_metadata, body.functions_metadata);
            Object.assign(d.clusters, body.clusters);
            const at = d.items.length;
            d.items.push(...body.items);
            d.total = body.total;
            this.el.querySelector('#nway-table tbody').insertAdjacentHTML('beforeend', body.items.map((r, i) => this._row(r, at + i)).join(''));
            this.el.querySelector('.table-footer-left').innerHTML = window.Paging.footer({ offset: 0, total: Number(d.total), size: PAGE, shown: d.items.length });
            this._watch();
        }

        _onClick(e) {
            const t = e.target.closest('[data-set],[data-tab],[data-sort],[data-nav],[data-open-cluster],[data-focus-toggle],[data-clear],[data-cg-open]');
            if (!t) return;
            if (t.dataset.cgOpen !== undefined) {
                if (e.target.closest('button, a, input, .bsim-tag-pill, .entity-actions')) return;
                return this._cgOpen(this.data.items[Number(t.dataset.cgOpen)]);
            }
            if (t.dataset.openCluster) {
                e.preventDefault();
                return this.onOpenCluster(t.dataset.openCluster);
            }
            const pool = this.source.pool || null;
            if (t.dataset.clear) return this._set({ [t.dataset.clear]: '', side: '', offset: 0 });
            if (t.dataset.focusToggle) {
                const id = t.dataset.focusToggle, focus = this._focusIds();
                const next = focus.includes(id) ? focus.filter(f => f !== id) : [...focus, id];
                return next.length ? this._setFocus(next, this._files().length) : this._set({ focus: '', side: '', offset: 0 });
            }
            if (t.dataset.set) {
                const i = t.dataset.set.indexOf(':');
                return this._set({ [t.dataset.set.slice(0, i)]: t.dataset.set.slice(i + 1), offset: 0 });
            }
            if (t.dataset.tab) return this._set({ tab: t.dataset.tab, offset: 0, sort_col: '' });
            if (t.dataset.sort) {
                const same = this.state.sort_col === t.dataset.sort;
                return this._set({ sort_col: t.dataset.sort, sort_dir: same && this.state.sort_dir === 'desc' ? 'asc' : 'desc', offset: 0 });
            }
            if (t.dataset.nav && window.Nav) {
                e.preventDefault();
                window.Nav.openPath(t.dataset.nav, e, { title: t.dataset.title || 'Function', type: 'function' });
            }
        }

        _onChange(e) {
            const t = e.target;
            if (t.dataset.select) this._set({ [t.dataset.select]: t.value, offset: 0 });
            else if (t.dataset.range) this._set({ [t.dataset.range]: t.value, offset: 0 });
            else if (t.dataset.text) this._set({ [t.dataset.text]: t.value.trim(), offset: 0 });
        }

        // Sliders recompute on release (change), not on every drag step.
        _onInput(e) {
            const t = e.target;
            const label = t.dataset.range && t.closest('label') && t.closest('label').querySelector('b');
            if (label) label.textContent = t.dataset.range === 'k' ? t.value : Number(t.value).toFixed(2);
        }
    }

    NwayPanel.act = (act, id) => NwayPanel._active && NwayPanel._active._act(act, id);

    // Focus / filter items for a file header's right-click menu (see `_menu`).
    NwayPanel.menuHtml = function (n) {
        const item = (act, icon, text) =>
            `<div class="context-menu-item" onclick="${escapeAttr(`event.stopPropagation(); window.closeGraphContextMenu(); NwayPanel.act(${jsString(act)}, ${jsString(n.id)})`)}"><i class="fa-solid ${icon}" style="width: 16px; text-align: center; opacity: 0.8; color: #fd971f;"></i><span>${escapeHtml(text)}</span></div>`;
        return (n.focused
            ? item('remove', 'fa-eye-slash', 'Remove from focus')
            : item('add', 'fa-bullseye', n.anyFocus ? 'Add to focus' : 'Focus on this file'))
            + item('column', 'fa-filter', n.filtered ? 'Show all files' : 'Only rows with this file');
    };

    // Selected file ids are `{coll}:file:{md5}`; the page's pool (if any) rides along.
    NwayPanel.urlFor = function (ids, fallbackCollection) {
        const md5s = ids.map(id => `${id.split(':')[0] || fallbackCollection}:${id.split(':').pop()}`).join(',');
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        return `/diff/nway?md5s=${encodeURIComponent(md5s)}${pool ? `&pool=${encodeURIComponent(pool)}` : ''}`;
    };

    window.NwayPanel = NwayPanel;
})();
