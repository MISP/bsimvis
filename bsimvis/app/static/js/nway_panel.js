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
 * `opts.cg` ({name, cells, view, scope, outside, depth}) makes the panel a standalone
 * call graph on `container` (no table); `onCg(snapshot)` fires on every graph change.
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
    // Same icon + color as the Code / Library / All pills in the table's Show control.
    const AXIS = { code: { icon: 'fa-solid fa-code', color: 'var(--info, #3b82f6)' }, library: { icon: 'fa-solid fa-cubes', color: 'var(--warning, #d97706)' } };
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

    // The visible table's rows holding any of these function ids, as `{fids, name}`.
    window.nwayRowsForFids = ids => {
        const cur = NwayPanel.current, want = new Set(ids), out = [];
        if (!cur || !cur.data) return out;
        for (const row of cur.data.items || []) {
            const fids = Object.values(row.files || row.cells || {}).filter(v => typeof v === 'string');
            if (fids.some(f => want.has(f))) out.push({ fids, name: row.name });
        }
        return out;
    };
    window.nwayRenameSelected = (ids, auto) => {
        const rows = window.nwayRowsForFids(ids);
        if (rows.length) FunctionRename.openBatch(rows, auto);
    };

    document.addEventListener('click', e => {
        const b = e.target.closest('[data-rename-fids]');
        if (!b) return;
        e.preventDefault();
        e.stopPropagation();
        FunctionRename.open(b.dataset.renameFids.split(','), b.dataset.renameName || '');
    }, true);

    class NwayPanel {
        constructor(container, opts = {}) {
            this.el = container;
            this._applyHue();
            this.source = opts.source || {};
            this.state = { ...DEFAULTS, ...(opts.state || {}) };
            this.onState = opts.onState || null;
            this.onOpenCluster = opts.onOpenCluster || null;
            this.cgInit = opts.cg || null;
            this.onCg = opts.onCg || null;
            this.data = null;
            this.error = null;
            this._gen = 0;
            this._debounce = null;
            this._io = null;
            this._loading = false;
            if (window.Paging.mode() === 'scroll') this.state.offset = 0;
            NwayPanel.current = this;
            this._onKey = e => this._cgKey(e);
            document.addEventListener('keydown', this._onKey);
            // Keep open graph focus names in step, then re-read the table / graph names.
            this._onRenamed = e => {
                const hit = ((e.detail || {}).renames || []);
                if (this._cg) for (const f of this._cg.stack) for (const r of hit) if (Object.values(f.cells).some(x => r.fids.includes(x))) f.name = r.name;
                if (!this.cgInit) this.load();
                if (this._cg) this._cgShow();
            };
            document.addEventListener('function-renamed', this._onRenamed);
            this.el.addEventListener('click', e => this._onClick(e));
            this.el.addEventListener('change', e => this._onChange(e));
            this.el.addEventListener('input', e => this._onInput(e));
            window.Paging.bind(this.el, () => ({ offset: Number(this.state.offset) || 0, total: Number(this.data && this.data.total) || 0, size: PAGE }), off => this._set({ offset: off }));
            this.el.addEventListener('toggle', e => {
                if (e.target.matches('details.nway-focus')) this._focusOpen = e.target.open;
                if (e.target.matches('details.nway-match')) this._matchOpen = e.target.open;
            }, true);
            this.el.addEventListener('contextmenu', e => {
                const file = e.target.closest('[data-nway-file]');
                if (file) return this._menu(e, file.dataset.nwayFile);
                const th = e.target.closest('th[data-col]');
                if (th) return this._menu(e, th.dataset.col);
                // A right-click on a function cell's empty space opens the function menu, not the plain cell one.
                const fn = !e.target.closest('[data-entity-data], button, a') && e.target.closest('td.col')?.querySelector('[data-etype="function"]');
                if (fn) window.EntityRenderer.handleContextMenu(e, 'function', fn);
            }, true);
            this.el.addEventListener('keydown', e => {
                if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-nway-file]')) this._onClick(e);
                else if (e.key === 'Enter' && e.target.dataset.text) this._set({ [e.target.dataset.text]: e.target.value.trim(), offset: 0 });
            });
        }

        destroy() {
            this._gen++;
            document.removeEventListener('function-renamed', this._onRenamed);
            if (NwayPanel.current === this) NwayPanel.current = null;
            document.removeEventListener('keydown', this._onKey);
            clearTimeout(this._debounce);
            if (this._io) this._io.disconnect();
            if (this._dw && !this.cgInit) window.clearCallsPanel();
            this._cg = null;
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
            qs.set('limit', this.cgInit ? 1 : PAGE);
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
            if (this.cgInit) {
                if (this.error) this.el.innerHTML = this._renderError();
                else this._cgStart(this.cgInit.name, this.cgInit.cells, this.cgInit, this.el);
                return;
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
            const search = `<div class="search-input-wrapper nway-search"><i class="fa-solid fa-magnifying-glass"></i><input type="text" data-text="q" placeholder="Search name, address, tags, cluster..." value="${escapeAttr(s.q)}"></div>`;
            const filter = [
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
            return `<div class="nway-tabrow"><div class="bsim-tabbar">${tabs}</div>${this._tip(tabTip)}${search}</div>
                <div class="nway-bar">${filter}<span class="nway-pops">${this._matchMenu(match)}${this._focusBox()}</span></div>`;
        }

        // Match settings live in a popover; the summary shows what is set, so the bar stays one line.
        _matchMenu(body) {
            const d = this.data, s = this.state;
            const sum = [`edge ${Number(d.min_edge).toFixed(2)}`, d.mode, d.columns_mode === 'children' ? 'children' : '', s.tab === 'partial' && d.columns_mode !== 'focus' ? `k ${s.k}` : ''].filter(Boolean).join(' · ');
            return `<details class="nway-match nway-pop"${this._matchOpen ? ' open' : ''}><summary class="nway-chip"><i class="fa-solid fa-sliders"></i> Match <span class="dim">${escapeHtml(sum)}</span></summary><div class="nway-match-body">${body}</div></details>`;
        }

        _files() {
            return this.data.file_columns || this.data.columns.filter(c => !c.kind);
        }

        _focusIds() {
            return String(this.state.focus || '').split(',').filter(Boolean);
        }

        _fileNameLink(f) {
            return `<span class="nway-file-link" data-nway-file="${escapeAttr(f.id)}" role="link" tabindex="0" title="${escapeAttr(`Open ${f.file_name || f.md5}`)}">${escapeHtml(f.file_name || f.md5)}</span>`;
        }

        _columnChip() {
            const only = this.state.column && this._files().find(f => f.id === this.state.column);
            return only ? `<button class="nway-chip on" data-clear="column" title="drop the file filter">Only ${this._fileNameLink(only)} &times;</button>` : '';
        }

        // Focus: pick files to compare against all the others. A collapsible box so
        // 30 files do not push the table down; opens itself while a focus is set.
        _focusBox() {
            const s = this.state, files = this._files(), ids = this._focusIds();
            const on = this.data.columns_mode === 'focus';
            const chips = files.map(f => `<button class="nway-chip${ids.includes(f.id) ? ' on' : ''}" data-focus-toggle="${escapeAttr(f.id)}" title="${escapeAttr(f.collection)}">${this._fileNameLink(f)}</button>`).join('');
            const ctl = on ? [
                this._seg('Focus holds', 'focus_rule', [['any', 'Any'], ['all', 'All']], 'Any: a function is on the focus side when at least one focus file has it. All: every focus file must have it.'),
                this._slider('Rest presence', 'child_presence', s.child_presence, 0.05, 1, 0.05, 'Share of the rest files that must hold a function for it to count on the rest side. Keep it low (a single rest file is enough) for a strict "unique to focus".'),
                s.tab === 'unique' ? this._seg('Unique to', 'side', [['', 'Both'], ['focus', 'Focus'], ['rest', 'Rest']], 'Unique tab only. Focus: functions in the focus files and in none of the rest. Rest: the reverse. Both: either.') : '',
                `<button class="nway-chip" data-clear="focus" title="back to the plain N-way">&times; clear focus</button>`,
            ].join('') : '';
            const open = this._focusOpen ? ' open' : '';
            const tip = 'Compare the chosen files against all the others. Columns collapse to Focus and Rest: Core is on both sides, Unique on one side. Right-click a file header for the same action.';
            return `<details class="nway-focus nway-pop"${open}><summary class="nway-chip${on ? ' on' : ''}" title="${escapeAttr(tip)}"><i class="fa-solid fa-bullseye"></i> Focus <span class="dim">${on ? `${ids.length}/${files.length} files` : 'off'}</span></summary>
                <div class="nway-focus-body nway-match-body"><div class="nway-chips">${chips}</div>${ctl}</div></details>`;
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
            // Ghidra's undefined/undefined4 return types are placeholders: vote among real ones when any exist.
            const real = items.filter(i => !/^undefined/.test(i.ret));
            const mode = (key, pool) => {
                const n = {};
                pool.forEach(i => { n[i[key]] = (n[i[key]] || 0) + 1; });
                return Object.entries(n).sort((x, y) => y[1] - x[1])[0][0];
            };
            const ret = mode('ret', real.length ? real : items);
            const params = mode('params', items.filter(i => i.ret === ret));
            const fits = items.filter(i => i.ret === ret && i.params === params);
            const best = (fits.length ? fits : items).sort((x, y) => (y.m.bsim_features_count || 0) - (x.m.bsim_features_count || 0))[0];
            return { ret, params: JSON.parse(params), best };
        }

        // Pencil beside a row's name: opens the rename dialog for every function of the row.
        _renameBtn(row) {
            const fids = Object.values(row.files || row.cells || {}).filter(v => typeof v === 'string');
            if (!fids.length) return '';
            // Graph cards and drill-down focuses only carry name + cells: read the source off the function metadata.
            let by = row.user_named;
            if (!by) {
                const meta = this.data.functions_metadata || {};
                const hit = fids.map(f => meta[f]).find(m => m && m.user_name && m.user_name === row.name);
                by = hit && (hit.user_name_source || 'user');
            }
            const alias = row.aliases && row.aliases.length ? `; also named: ${row.aliases.join(', ')}` : '';
            const auto = !by && row.name_from;
            const mark = auto ? `<span class="nway-by auto" title="Name recovered from a ${row.name_from} by automated analysis"><i class="fa-solid ${row.name_from === 'tag' ? 'fa-tag' : 'fa-diagram-project'}"></i></span>` : by ? `<span class="nway-by ${by === 'llm' ? 'llm' : 'user'}" title="${escapeAttr((by === 'llm' ? 'Named by the LLM' : 'Named by an analyst') + alias)}"><i class="fa-solid ${by === 'llm' ? 'fa-robot' : 'fa-user-pen'}"></i></span>` : '';
            return `${mark}<button class="nway-rename" data-rename-fids="${escapeAttr(fids.join(','))}" data-rename-name="${escapeAttr(row.name)}" title="Rename this function"><i class="fa-solid fa-pen"></i></button>`;
        }

        _nameSig(row, opts = {}) {
            const sig = this._rowSig(row);
            const name = escapeHtml(row.name);
            if (!sig) return `<div class="nway-namerow"><span>${name}</span>${this._renameBtn(row)}</div>`;
            const f = this._fnData(sig.best.fid, sig.best.col);
            // Same renderer as the function search table, fed the guessed signature.
            const fn = window.EntityRenderer.renderFunction({ ...f, user_name: '', function_name: row.name, return_type: sig.ret, parameters: sig.params }, { hideNote: true, showActions: false, ...opts });
            // Tags land on the best candidate, the function the preview shows.
            const tags = window.EntityRenderer.renderTag('function', f.function_id, row.tags || f.tags, row.tags ? [] : f.user_tags);
            return `<div class="nway-namerow">${fn}${this._renameBtn(row)}</div><div class="nway-tags" title="${row.tags ? 'tags pooled over the row' : 'tags of the best candidate'}: ${escapeAttr(f.entrypoint_address)}">${tags}</div>`;
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
            this._cgStart(row.name, cells, { name_from: row.name_from }, window.openCallsPanel());
        }

        // Draws into `host`: the side panel's body, or the standalone page's container.
        _cgStart(name, cells, init, host) {
            const prev = this._cg || {};
            this._cg = {
                stack: [{ name, cells, pieces: init.pieces, name_from: init.name_from }],
                view: init.view || prev.view || 'graph',
                scope: init.scope || prev.scope || this.state.scope || 'code',
                outside: init.outside || prev.outside || 'merge',
                depth: Number(init.depth || prev.depth || 1),
                more: {},
                place: {},
                co: init.co === '1' || init.co === true || (init.co === undefined && !!prev.co),
                hints: init.hints === '1' || init.hints === true || (init.hints === undefined && !!prev.hints),
                suggest: prev.suggest === undefined ? 'comp' : prev.suggest,
                nb: null,
            };
            this._dw = host;
            this._applyHue();
            host.onclick = e => this._onDrawerClick(e);
            host.onchange = e => {
                const t = e.target.closest('[data-cg-depth]');
                if (t && this._cg) { this._cg.depth = Number(t.value); this._cgShow(); }
            };
            host.onpointerdown = e => this._cgPan(e);
            host.onmouseover = e => { const l = e.target.closest('.nway-lane-lbl'); if (l) this._cgLaneTip(l); };
            host.onmouseout = e => { if (e.target.closest('.nway-lane-lbl')) this._cgTipLater(); };
            this._cgShow();
        }

        _cgTipLater() {
            clearTimeout(this._tipT);
            this._tipT = setTimeout(() => { if (this._laneTipEl) this._laneTipEl.style.display = 'none'; }, 250);
        }

        // Hover a lane label: what the lane's files have in common (the bar's tick is the share across all files).
        _cgLaneTip(lbl) {
            clearTimeout(this._tipT);
            const files = this._cgFiles(), mask = (this._cgLaneFiles || [])[Number(lbl.dataset.lane)];
            if (!mask) return;
            const lane = files.filter((_, k) => mask[k]);
            if (!this._laneTipEl) {
                const t = this._laneTipEl = document.createElement('div');
                t.className = 'nway-lane-tip';
                t.onmouseenter = () => clearTimeout(this._tipT);
                t.onmouseleave = () => this._cgTipLater();
                t.onclick = e => {
                    const b = e.target.closest('[data-tip-copy]');
                    if (!b) return;
                    const f = this._tipFiles, k = b.dataset.tipCopy;
                    const text = f.map(c => k === 'md5' ? c.md5 : k === 'name' ? c.file_name : `${c.collection}:${c.md5}`).join('\n');
                    const done = () => { b.textContent = 'copied'; setTimeout(() => { b.textContent = b.dataset.tipLabel; }, 1200); };
                    if (navigator.clipboard) navigator.clipboard.writeText(text).then(done, () => {});
                };
                document.body.appendChild(t);
            }
            this._tipFiles = lane;
            const size = c => { const n = Number(c.file_size); if (!n) return null; return n < 16384 ? '<16 KB' : n < 65536 ? '16-64 KB' : n < 262144 ? '64-256 KB' : n < 1048576 ? '256 KB-1 MB' : n < 4194304 ? '1-4 MB' : '>4 MB'; };
            const dims = [['Architecture', c => c.language_id], ['Format', c => c.format], ['Size', size], ['Batch', c => c.batch_name], ['Collection', c => c.collection], ['Tags', c => c.tags, true]];
            const count = (list, get, multi) => {
                const m = new Map();
                for (const c of list) for (const v of [].concat(multi ? get(c) || [] : [get(c)])) if (v !== null && v !== undefined && v !== '') m.set(String(v), (m.get(String(v)) || 0) + 1);
                return m;
            };
            const sections = dims.map(([title, get, multi]) => {
                const here = count(lane, get, multi), all = count(files, get, multi);
                if (!here.size) return '';
                const top = [...here].sort((a, b) => b[1] - a[1]).slice(0, 5);
                const uniform = here.size === 1 && all.size > 1 && top[0][1] === lane.length;
                const rows = top.map(([v, n]) => `<div class="nway-tip-row" title="${escapeAttr(v)}"><span class="nway-tip-v">${escapeHtml(v)}</span><span class="nway-tip-bar"><i style="width:${(100 * n / lane.length).toFixed(0)}%"></i><b style="left:${(100 * (all.get(v) || 0) / files.length).toFixed(0)}%"></b></span><span class="dim">${n}</span></div>`).join('');
                return `<div class="nway-tip-h">${escapeHtml(title)}${uniform ? ' <span class="nway-tip-flag" title="Every file of this lane shares it, the other files do not all">only this</span>' : ''}${here.size > top.length ? ` <span class="dim">+${here.size - top.length} more</span>` : ''}</div>${rows}`;
            }).join('');
            this._laneTipEl.innerHTML = `<div class="nway-tip-top"><b>${lane.length} of ${files.length} files</b><span class="nway-tip-act"><button data-tip-copy="md5" data-tip-label="md5s">md5s</button><button data-tip-copy="name" data-tip-label="names">names</button><button data-tip-copy="id" data-tip-label="coll:md5">coll:md5</button></span></div><div class="dim nway-tip-legend">bar: share of the lane's files &middot; tick: share of all files</div>${sections}`;
            const r = lbl.getBoundingClientRect(), t = this._laneTipEl;
            t.style.display = 'block';
            t.style.left = `${Math.min(r.left, window.innerWidth - t.offsetWidth - 8)}px`;
            t.style.top = `${Math.min(r.bottom + 4, window.innerHeight - t.offsetHeight - 8)}px`;
        }

        // Drag the graph background to pan; cards, buttons and inputs keep their clicks.
        _cgPan(e) {
            const body = e.target.closest('.nway-cg-body');
            if (!body || e.button !== 0 || e.target.closest('.nway-node, .nway-cg-item, button, a, input, .bsim-tag-pill, .nway-strip')) return;
            if (this._cg && this._cg.selMode) return this._cgLasso(e, body);
            const x = e.clientX, y = e.clientY, l = body.scrollLeft, t = body.scrollTop;
            body.classList.add('panning');
            const move = ev => { body.scrollLeft = l - (ev.clientX - x); body.scrollTop = t - (ev.clientY - y); };
            const up = () => {
                body.classList.remove('panning');
                window.removeEventListener('pointermove', move);
                window.removeEventListener('pointerup', up);
            };
            window.addEventListener('pointermove', move);
            window.addEventListener('pointerup', up);
        }

        // Box-select: cards the rectangle touches replace the selection (Shift: join it).
        _cgLasso(e, body) {
            const x0 = e.clientX, y0 = e.clientY, add = e.shiftKey;
            e.preventDefault();
            window.getSelection().removeAllRanges();
            const box = document.createElement('div');
            box.className = 'nway-sel-rect';
            document.body.appendChild(box);
            let moved = false;
            const rect = ev => ({ l: Math.min(x0, ev.clientX), t: Math.min(y0, ev.clientY), r: Math.max(x0, ev.clientX), b: Math.max(y0, ev.clientY) });
            const move = ev => {
                const q = rect(ev);
                moved = moved || q.r - q.l > 3 || q.b - q.t > 3;
                Object.assign(box.style, { left: `${q.l}px`, top: `${q.t}px`, width: `${q.r - q.l}px`, height: `${q.b - q.t}px` });
            };
            const up = ev => {
                window.removeEventListener('pointermove', move);
                window.removeEventListener('pointerup', up);
                box.remove();
                const q = rect(ev), sel = this._cg && (this._cg.sel || (this._cg.sel = new Set()));
                if (!sel) return;
                if (!add) sel.clear();
                if (moved) {
                    this._cgSwallow = true;
                    setTimeout(() => { this._cgSwallow = false; }, 0);
                    body.querySelectorAll('.nway-node[data-sel]').forEach(n => {
                        const r = n.getBoundingClientRect();
                        if (r.right >= q.l && r.left <= q.r && r.bottom >= q.t && r.top <= q.b) sel.add(n.dataset.sel);
                    });
                }
                this._cgSelSync();
            };
            window.addEventListener('pointermove', move);
            window.addEventListener('pointerup', up);
        }

        // Selected cards as `{name, cells}` (cells: file column id -> function id).
        _cgSelItems() {
            const sel = (this._cg && this._cg.sel) || new Set(), focus = this._cgFocus();
            return [...sel].map(k => (k === 'focus' ? { name: focus.name, cells: focus.cells } : this._cgItems && this._cgItems[k])).filter(Boolean).map(it => ({
                name: it.name,
                fids: Object.values(it.cells || {}).filter(v => typeof v === 'string'),
            })).filter(it => it.fids.length);
        }

        // Re-mark the selected cards after a draw and refresh the action bar.
        _cgSelSync() {
            if (!this._dw || !this._cg) return;
            const sel = this._cg.sel || (this._cg.sel = new Set());
            const live = new Set();
            this._dw.querySelectorAll('.nway-node[data-sel]').forEach(n => {
                live.add(n.dataset.sel);
                n.classList.toggle('sel', sel.has(n.dataset.sel));
            });
            // A card gone after a re-centre is no longer selected.
            for (const k of [...sel]) if (!live.has(k)) sel.delete(k);
            const bar = this._dw.querySelector('.nway-cg-selbar');
            if (!bar) return;
            const items = this._cgSelItems();
            bar.hidden = !items.length && !this._cg.selMode;
            const b = (a, icon, t, title) => `<button class="nway-chip" data-cg-selact="${a}" title="${title}"><i class="${icon}"></i> ${t}</button>`;
            const pick = b('all', 'fa-solid fa-square-check', 'All', 'Select every card (Ctrl+A)') + b('invert', 'fa-solid fa-right-left', 'Invert', 'Invert the selection (Ctrl+I)');
            if (!items.length) { bar.innerHTML = `<b>0 selected</b>${pick}`; return; }
            bar.innerHTML = `<b>${items.length} selected</b>${pick}`
                + b('diff', 'fa-solid fa-plus-minus', 'Add to diff', 'Queue two functions for the function diff: two cards give one function each, one card gives its first two files')
                + b('copy', 'fa-solid fa-copy', 'Copy names', 'Copy the names, one per line (Ctrl+C)')
                + b('rename', 'fa-solid fa-pen', 'Rename', 'Rename the selected cards')
                + b('ai', 'fa-solid fa-robot', 'AI rename', 'Ask the LLM to name the selected cards')
                + b('clear', 'fa-solid fa-xmark', 'Clear', 'Clear the selection (Esc)');
        }

        // Select every selectable card, or flip the selection.
        _cgSelAll(invert) {
            const sel = this._cg.sel || (this._cg.sel = new Set());
            this._dw.querySelectorAll('.nway-node[data-sel]').forEach(n => {
                const k = n.dataset.sel;
                if (!invert) sel.add(k);
                else if (!sel.delete(k)) sel.add(k);
            });
            this._cgSelSync();
        }

        // Ctrl+A / Ctrl+I / Ctrl+C / Esc for the graph; only while the pointer or focus is in the drawer.
        _cgKey(e) {
            if (!this._cg || !this._dw || this._cg.view !== 'graph' || e.target.closest('input, textarea, select, [contenteditable]')) return;
            if (!this._dw.contains(e.target) && !this._dw.matches(':hover')) return;
            const k = e.key.toLowerCase(), mod = e.ctrlKey || e.metaKey;
            if (mod && (k === 'a' || k === 'i')) { e.preventDefault(); this._cgSelAll(k === 'i'); }
            else if (mod && k === 'c' && this._cg.sel && this._cg.sel.size && !String(window.getSelection())) { e.preventDefault(); this._cgSelAct('copy'); }
            else if (k === 'escape' && this._cg.sel && this._cg.sel.size) { e.preventDefault(); this._cg.sel.clear(); this._cgSelSync(); }
        }

        _cgSelAct(act) {
            if (act === 'all' || act === 'invert') return this._cgSelAll(act === 'invert');
            const items = this._cgSelItems();
            if (act === 'clear') { this._cg.sel.clear(); return this._cgSelSync(); }
            if (!items.length) return;
            if (act === 'copy') return window.copyMetadata(items.map(i => i.name).join('\n'), 'names');
            if (act === 'rename' || act === 'ai') return FunctionRename.openBatch(items.map(i => ({ fids: i.fids, name: i.name })), act === 'ai');
            if (act === 'diff') {
                const pick = items.length >= 2 ? [items[0].fids[0], items[1].fids[0]] : items[0].fids.slice(0, 2);
                if (pick.length < 2) return window.showToast && window.showToast('Select two cards, or one present in two files', 'error');
                const name = f => (items.find(i => i.fids.includes(f)) || {}).name || '';
                pick.forEach(f => window.addToDiff(f, name(f)));
            }
        }

        // The standalone page's URL: the member source, doc controls, and the focused graph.
        _cgUrl() {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of DOC_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            const st = this._cg, f = this._cgFocus();
            qs.set('cg_name', f.name);
            qs.set('cg_cells', JSON.stringify(f.cells));
            if (f.pieces) qs.set('cg_pieces', JSON.stringify(f.pieces));
            for (const k of ['view', 'scope', 'outside', 'depth']) qs.set(`cg_${k}`, st[k]);
            if (st.co) qs.set('cg_co', '1');
            if (st.hints) qs.set('cg_hints', '1');
            return `/diff/nway/graph?${qs.toString()}`;
        }

        // Member source + the controls that change the doc: the query every per-row call repeats.
        _docQs() {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of DOC_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            return qs;
        }

        // The function-family page of one row (nway_function.js): the table's own
        // source and doc controls, the row's functions, and this page as the way back.
        _fnUrl(row) {
            const fids = Object.values(row.files || row.cells || {}).filter(v => typeof v === 'string');
            if (!fids.length) return '';
            const qs = this._docQs();
            qs.set('fx_fids', fids.join(','));
            qs.set('fx_name', row.name);
            qs.set('fx_back', window.location.pathname + window.location.search);
            return `/diff/nway/function?${qs.toString()}`;
        }

        _scrollBoxes() {
            return [this.el.querySelector('.nway-scroll'), this.el.closest('.nway-view'), this.el.closest('#cluster-tab-functions')].filter(Boolean);
        }

        // Scroll offsets ride along in sessionStorage so Back (or the breadcrumb) lands where the row was.
        _scrollSave() {
            try {
                sessionStorage.setItem(`nway:scroll:${window.location.pathname}${window.location.search}`, JSON.stringify(this._scrollBoxes().map(b => b.scrollTop)));
            } catch (e) { /* storage blocked: the table just opens at the top */ }
        }

        _scrollRestore() {
            try {
                const key = `nway:scroll:${window.location.pathname}${window.location.search}`;
                const saved = JSON.parse(sessionStorage.getItem(key) || 'null');
                sessionStorage.removeItem(key);
                if (saved) this._scrollBoxes().forEach((b, i) => { b.scrollTop = saved[i] || 0; });
            } catch (e) { /* nothing to restore */ }
        }

        async _cgFetch(focus, role) {
            const qs = this._docQs();
            qs.set('fids', Object.values(focus.cells).join(','));
            qs.set('role', role);
            qs.set('depth', this._cg.depth);
            qs.set('scope', this._cg.scope);
            const res = await fetch(`/api/bin_sim/nway/neighbors?${qs.toString()}`);
            const body = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(body.message || body.error || `HTTP ${res.status}`);
            return body;
        }

        async _cgShow() {
            const focus = this._cgFocus();
            const tok = this._cgTok = (this._cgTok || 0) + 1;
            this._cg.nb = null;
            this._cg.place = {};
            this._cgDraw();
            try {
                const [callers, callees, cocallers] = await Promise.all([this._cgFetch(focus, 'callers'), this._cgFetch(focus, 'callees'), this._cg.co ? this._cgFetch(focus, 'cocallers') : null]);
                if (tok !== this._cgTok || !this._cg) return;
                Object.assign(this.data.functions_metadata, callers.functions_metadata, callees.functions_metadata, cocallers ? cocallers.functions_metadata : {});
                this._cg.nb = { callers, callees };
                if (cocallers) this._cg.nb.cocallers = cocallers;
                this._cgDraw();
                this._cgPlace(tok);
            } catch (err) {
                if (tok !== this._cgTok || !this._cg) return;
                this._cgDraw(String(err.message || err));
            }
        }

        async _cgPair(a, b) {
            const cache = this._pairCache || (this._pairCache = new Map());
            const key = `${a}|${b}`;
            if (!cache.has(key)) {
                cache.set(key, fetch(`/api/similarity?${new URLSearchParams({ id1: a, id2: b, algo: 'binary_cosine' })}`)
                    .then(r => r.json()).then(d => Number(d.scores && d.scores.binary_cosine) || 0).catch(() => 0));
            }
            return cache.get(key);
        }

        // Placeholder for the BSim score of card `b` against card `a` (up to 3 functions each),
        // filled once the drawer is in the DOM by _cgFillSims. Hover shows the function diff of
        // the best pair, click opens it.
        _cgSim(a, b, na, nb) {
            if (!a || !a.length || !b || !b.length) return '';
            return `<span class="nway-sim dim" data-sim-a="${escapeAttr(a.join(','))}" data-sim-b="${escapeAttr(b.join(','))}" data-sim-na="${escapeAttr(na || '')}" data-sim-nb="${escapeAttr(nb || '')}"></span>`;
        }

        _cgFillSims() {
            this._dw.querySelectorAll('.nway-sim[data-sim-a]').forEach(async el => {
                const a = el.dataset.simA.split(','), b = el.dataset.simB.split(',');
                const pairs = await Promise.all(a.flatMap(x => b.map(async y => ({ x, y, s: await this._cgPair(x, y) }))));
                const best = pairs.reduce((m, p) => (p.s > m.s ? p : m), pairs[0]);
                el.textContent = `${Math.round(best.s * 100)}%`;
                el.style.color = window.scoreColor(best.s);
                el.classList.remove('dim');
                el.style.cursor = 'pointer';
                const na = el.dataset.simNa || fnParts(best.x).addr, nb = el.dataset.simNb || fnParts(best.y).addr;
                el.onmouseenter = e => window.showDiffPreview && window.showDiffPreview(best.x, na, best.y, nb, best.s, e);
                el.onmousemove = e => window.moveDiffPreview && window.moveDiffPreview(e);
                el.onmouseleave = e => window.hideDiffPreview && window.hideDiffPreview(e);
                el.onclick = e => {
                    e.stopPropagation();
                    if (window.hideDiffPreview) window.hideDiffPreview(e);
                    window.Nav.openPath(window.buildDiffUrl(best.x, best.y), e, { title: `Diff: ${na} vs ${nb}`, type: 'diff' });
                };
            });
        }

        // Pieces present in one file that several groups could take: score each against
        // the groups' functions (BSim, cross-file, so it can tell what the greedy match
        // dropped) and give each group's free file slot to the best-scoring piece.
        // ponytail: binary cosine over up to 4 functions per group; a BSim-weighted score or a call-set term if this misplaces pieces.
        async _cgPlace(tok) {
            const st = this._cg, nb = st.nb, files = this._cgFiles(), focus = this._cgFocus();
            if (st.suggest !== 'comp') return;
            const place = {};
            // One top-down pass (a level's groups are the parents of the next). `support`, set on
            // the second pass, adds a bonus when a piece's child joined a group whose other
            // members hang under the candidate group: the call graph agreeing with the score.
            const pass = async support => {
                const seen = {}, canonCallees = {};
                for (const role of ['callers', 'callees', 'cocallers']) {
                    if (!nb[role]) continue;
                    let canon = role === 'cocallers' ? canonCallees.map : null;
                    seen[role] = [];
                    for (let lv = 0; lv < nb[role].levels.length; lv++) {
                        const entries = nb[role].levels[lv].map((it, i) => ({ it, i, out: !(st.scope === 'all' || (st.scope === 'library') === !!it.library) })).filter(x => !x.out);
                        const prefix = `${role}${lv}|`;
                        let gs = this._cgGroups(prefix, entries, files, focus, place, canon);
                        const jobs = [];
                        for (const p of gs.pending) {
                            for (const c of p.cands) jobs.push({ key: p.key, idx: p.idx, fids: p.fids, anchor: c.anchor, gf: c.fids });
                        }
                        if (jobs.length) {
                            await Promise.all(jobs.map(async j => { j.s = Math.max(0, ...(await Promise.all(j.fids.flatMap(a => j.gf.map(b => this._cgPair(a, b)))))); }));
                            if (tok !== this._cgTok || this._cg !== st) return null;
                            if (support) jobs.forEach(j => { j.s += support(role, lv, j); });
                            // Best score first; a group takes pieces whose files do not overlap.
                            const used = new Set(), taken = new Map();
                            for (const j of jobs.sort((x, y) => y.s - x.s)) {
                                const t = taken.get(j.anchor) || new Set();
                                if (j.s < 0.3 || used.has(j.key) || j.idx.some(i => t.has(i))) continue;
                                place[j.key] = j.anchor;
                                used.add(j.key);
                                j.idx.forEach(i => t.add(i));
                                taken.set(j.anchor, t);
                            }
                            gs = this._cgGroups(prefix, entries, files, focus, place, canon);
                        }
                        seen[role].push({ gs, entries });
                        canon = gs.canon;
                        if (role === 'callees' && lv === 0) canonCallees.map = canon;
                    }
                }
                return seen;
            };
            const first = await pass(null);
            if (!first) return;
            Object.keys(place).forEach(k => delete place[k]);
            await pass((role, lv, j) => {
                const nxt = first[role][lv + 1];
                if (!nxt) return 0;
                const kids = nxt.entries.filter(e => (e.it.parents || []).includes(j.key));
                return kids.some(kid => {
                    const h = nxt.gs.find(g => g.m.some(r => r.x.it.key === kid.it.key));
                    return h && h.m.some(r => r.x.it.key !== kid.it.key && r.par.has(j.anchor));
                }) ? 0.25 : 0;
            });
            if (tok !== this._cgTok || this._cg !== st || !Object.keys(place).length) return;
            st.place = place;
            this._cgDraw();
        }

        // One square per file: lit when that file's function has the edge.
        // Each square is a file. Lit (colored by how many files) = the file has the function;
        // a dot inside = the focus function in that file calls it. Hover shows name and md5,
        // click opens the file details, right-click gives the usual file menu.
        // Optional "color by file" mode (strips + column swatches); neutral ratio colors stay the default.
        _applyHue() {
            let on = false;
            try { on = localStorage.getItem('nway.hue') === '1'; } catch (e) { /* storage blocked */ }
            this._hueOn = on;
            for (const el of [this.el, this._dw]) if (el) el.classList.toggle('nway-hue', on);
        }

        _fileHue(i) {
            return Math.round((i * 137.508 + 215) % 360);
        }

        _cgStrip(on, files, edge) {
            const n = on.filter(Boolean).length;
            const cls = n === files.length ? 'r-hi' : n * 2 >= files.length ? 'r-mid' : 'r-low';
            const total = files.length;
            if (total > 120) {
                return { html: `<span class="nway-pbar ${cls}" title="${n} of ${total} files"><b style="width:${(100 * n / total).toFixed(0)}%"></b></span>`, n, cls };
            }
            const sz = total <= 16 ? 11 : total <= 64 ? 6 : 4;
            const sq = files.map((c, i) => {
                const coll = c.collection || '';
                const data = { md5: c.md5, fileId: `${coll}:file:${c.md5}`, name: c.file_name || c.md5, file_name: c.file_name || '', collection: coll };
                const tip = [c.file_name, c.md5, coll, on[i] ? 'function present' : 'function absent', edge && edge[i] ? 'called from the focus function here' : ''].filter(Boolean).join('\n');
                return `<i class="${on[i] ? 'on' : ''}${edge && edge[i] ? ' edge' : ''}"${total <= 16 ? ` style="--h:${this._fileHue(i)}"` : ''} title="${escapeAttr(tip)}" data-entity-data='${escapeAttr(JSON.stringify(data))}'
                    onclick="event.stopPropagation(); openFileDetails(${escapeAttr(jsString(coll))}, ${escapeAttr(jsString(c.md5))}, ${escapeAttr(jsString(c.file_name || c.md5))}, event)"
                    oncontextmenu='event.stopPropagation(); EntityRenderer.handleContextMenu(event, "file", this)'></i>`;
            }).join('');
            return { html: `<span class="nway-strip ${cls}${total <= 16 ? ' hued' : ''}" style="--sq:${sz}px">${sq}</span>`, n, cls };
        }

        // Presence: the neighbor's function exists in that file. Edge: the focus function there calls it.
        _cgMeasures(it, focus, files) {
            // Sources are the parent row's functions (the focus for the first hop), so match them to files by md5.
            const srcs = Object.keys(it.sources);
            const edge = files.map(c => srcs.some(f => f.includes(`:func:${c.md5}:`)));
            const present = it.kind === 'row' ? files.map(c => !!it.cells[c.id]) : edge;
            return { present, edge, np: present.filter(Boolean).length, ne: edge.filter(Boolean).length };
        }

        _cgChips(m, st, it, files) {
            const pres = it.kind === 'row' ? `<span class="nway-reach ${st.cls}" title="the function exists in ${m.np} of ${files.length} files"><i class="fa-solid fa-file-code"></i> ${m.np}/${files.length}</span>` : '';
            return `${pres}<span class="nway-edgecnt" title="called from ${m.ne} of ${Number(it.of)} focus functions"><i class="fa-solid fa-link"></i> ${m.ne}/${Number(it.of)}</span>`;
        }

        _cgName(it) {
            if (it.kind === 'row') return this._nameSig({ name: it.name, cells: it.cells, name_from: it.name_from }, { noClick: true });
            if (it.kind === 'ext') return `<span class="nway-ext" title="imported symbol, matched by name">${escapeHtml(it.name)}<span class="nway-badge">ext</span></span>`;
            return `<span class="dim" title="not part of any row in this view">${escapeHtml(it.name || '?')} (no row)</span>`;
        }

        _cgReach(it, focus, files) {
            const m = this._cgMeasures(it, focus, files);
            const st = this._cgStrip(m.present, files, m.edge);
            return st.html + this._cgChips(m, st, it, files);
        }

        // Neighbors of one role under the Code / Library / All scope, keeping their index in the full list.
        _cgShown(nb, role, lv = 0) {
            const scope = this._cg.scope;
            return (lv ? nb[role].levels[lv] || [] : nb[role].items).map((it, i) => ({ it, i, out: !(scope === 'all' || (scope === 'library') === !!it.library) }));
        }

        _cgList(nb, focus, files) {
            const box = (role, label, icon) => {
                const shown = this._cgShown(nb, role).filter(x => !x.out);
                const rows = shown.map(({ it, i }) => `<div class="nway-cg-item${it.kind === 'row' ? '' : ' static'}" ${it.kind === 'row' ? `tabindex="0" data-cg-go="${role}:0:${i}"` : ''}>
                    <span class="nway-cg-l">${this._cgName(it)}</span><span class="nway-cg-r">${this._cgReach(it, focus, files)}</span></div>`).join('');
                return `<section class="nway-cg-box"><header><i class="fa-solid ${icon}"></i> ${label} <span class="dim">(${shown.length})</span></header>${rows || '<div class="dim nway-cg-none">none in this scope</div>'}</section>`;
            };
            return box('callers', 'Callers', 'fa-right-to-bracket') + box('callees', 'Callees', 'fa-right-from-bracket');
        }

        // Suggested merges: cards on one side/hop whose file sets barely overlap (so they never
        // co-occur) and that share a parent -- likely one function the greedy match split.
        // A row joins a group when exactly one group can take it (or _cgPlace picked its group by
        // score); a row several groups could take comes back in `pending`, never first-fit.
        // Seeds are rows present in 2+ files.
        _cgGroups(prefix, entries, files, focus, place = {}, canon = null) {
            const rows = entries.filter(x => x.it.kind === 'row').map(x => {
                const m = this._cgMeasures(x.it, focus, files);
                // Parents count by their group (`canon`: previous hop's member -> anchor), so pieces of one split parent are one parent.
                return { x, m, par: new Set((x.it.parents || ['focus']).map(k => (canon && canon.get(k)) || k)) };
            }).filter(r => r.m.np >= 1).sort((a, b) => b.m.np - a.m.np);
            const groups = [], pending = [];
            const cellFids = r => Object.values(r.x.it.cells).filter(v => typeof v === 'string');
            for (const r of rows) {
                const k1 = r.m.present.indexOf(true);
                // A piece whose parents are all ungrouped (a loose piece one hop up) has no group to share a parent with: let it try any group.
                const orphan = canon && [...r.par].every(p => !canon.has(p));
                const cands = groups.filter(g => (orphan || [...r.par].some(p => g.par.has(p))) && r.m.present.filter((v, k) => v && g.u[k]).length <= Math.floor(r.m.np * 0.1));
                const placed = cands.find(g => g.m[0].x.it.key === place[r.x.it.key]);
                const alone = r.m.np > 1 || rows.filter(o => o.m.np === 1 && o.m.present[k1]).length === 1;
                const g = placed || (cands.length === 1 && alone ? cands[0] : null);
                if (g) {
                    g.m.push(r);
                    r.m.present.forEach((v, k) => { if (v) g.u[k] = true; });
                    r.par.forEach(k => g.par.add(k));
                } else if (cands.length) {
                    pending.push({ key: r.x.it.key, idx: r.m.present.map((v, k) => v ? k : -1).filter(k => k >= 0), fids: cellFids(r).slice(0, 3),
                        cands: cands.map(g => ({ anchor: g.m[0].x.it.key, fids: g.m.flatMap(m => cellFids(m)).slice(0, 4) })) });
                } else if (r.m.np >= 2) {
                    groups.push({ m: [r], u: r.m.present.slice(), par: new Set(r.par) });
                }
            }
            const out = groups.filter(g => g.m.length > 1).map((g, n) => ({
                ...g, n, label: `G${n + 1}`,
                cov: g.u.filter(Boolean).length,
                id: prefix + g.m.map(r => r.x.it.key).sort().join('+'),
            }));
            out.pending = pending;
            out.canon = new Map(out.flatMap(g => g.m.map(r => [r.x.it.key, g.m[0].x.it.key])));
            return out;
        }

        // Cards present in exactly the same files (two or more of them): one change or fork touched them all.
        // Same shape as _cgGroups, but members stay separate functions (no merge, no focus).
        _cgGroupsSame(prefix, entries, files, focus) {
            const by = new Map();
            for (const x of entries) {
                if (x.it.kind !== 'row') continue;
                const m = this._cgMeasures(x.it, focus, files);
                if (m.np < 2) continue;
                const sig = m.present.map(v => (v ? 1 : 0)).join('');
                if (!by.has(sig)) by.set(sig, []);
                by.get(sig).push({ x, m, par: new Set(x.it.parents || ['focus']) });
            }
            const out = [...by.values()].filter(ms => ms.length > 1).map((ms, n) => ({
                m: ms, u: ms[0].m.present.slice(), par: new Set(ms.flatMap(r => [...r.par])), same: true, n, label: `S${n + 1}`,
                cov: ms[0].m.np,
                id: prefix + 'same:' + ms.map(r => r.x.it.key).sort().join('+'),
            }));
            out.pending = [];
            out.canon = new Map(out.flatMap(g => g.m.map(r => [r.x.it.key, g.m[0].x.it.key])));
            return out;
        }

        // A merged focus lists the cards it was made of, each with the files it covers.
        _cgPieces(focus, files) {
            if (!focus.pieces || focus.pieces.length < 2) return '';
            const rows = focus.pieces.map((p, i) => {
                const st = this._cgStrip(files.map(c => p.files.includes(c.id)), files);
                const sim = i ? this._cgSim(focus.pieces[0].fids, p.fids, focus.pieces[0].name, p.name) : '';
                return `<div class="nway-piece"><span class="nway-piece-name">${escapeHtml(String(p.name || '?').replace(/\(.*$/, ''))}</span>${st.html}<span class="dim">${st.n}/${files.length}</span>${sim}</div>`;
            }).join('');
            return `<div class="nway-pieces" title="Merged focus: these cards cover different files and were joined into one function"><div class="dim">&asymp; merged from ${focus.pieces.length} cards</div>${rows}</div>`;
        }

        _cgGraph(nb, focus, files) {
            const CAP = 12, COCAP = 6;
            const st = this._cg, depth = st.depth;
            const node = (role, lv, { it, i, out }) => {
                const m = this._cgMeasures(it, focus, files);
                let twin = '';
                if (role === 'cocallers' && lv === 0 && it.kind === 'row') {
                    // Its files barely overlap the focus's (10% test as _cgGroups): likely the focus split by the match.
                    const both = files.filter((c, k) => m.present[k] && typeof focus.cells[c.id] === 'string').length;
                    if (both <= Math.floor(m.np * 0.1)) {
                        this._cgSib[it.key] = it;
                        twin = `<button class="nway-chip" data-cg-mergefocus="${escapeAttr(it.key)}" title="Same callees, other files: merge into the focus (one function in more files)">&asymp; merge with focus</button>`;
                    }
                }
                const beyond = it.beyond && (role === 'callers' || role === 'callees') ? ` data-beyond="${Number(it.beyond)}"${lv === depth - 1 ? ' data-last="1"' : ''}` : '';
                const other = it.other && (role === 'callers' || role === 'callees') ? ` data-other="${Number(it.other)}"` : '';
                const back = it.back && it.back.length && (role === 'callers' || role === 'callees') ? ` data-back="${escapeAttr(it.back.join(' '))}"` : '';
                const hub = role === 'callees' && nb.cocallers && nb.cocallers.hub_keys[it.key];
                if (hub) twin += `<span class="nway-hub" title="Called by ${hub} functions: too widely called to say anything about similarity, so its other callers are not listed"><i class="fa-solid fa-triangle-exclamation"></i> hub &middot; ${hub} callers not shown</span>`;
                if (it.kind === 'row') this._cgItems[`${role}|${it.key}`] = it;
                return `<div class="nway-node${it.kind === 'row' ? '' : ' static'}${out ? ' out' : ''}${lv ? ' deep' : ''}" ${it.kind === 'row' ? `tabindex="0" data-sel="${escapeAttr(role + '|' + it.key)}" data-cg-go="${role}:${lv}:${i}"` : ''} data-side="${role}"${beyond}${other}${back} data-key="${escapeAttr(it.key)}" data-parents="${escapeAttr((it.parents || ['focus']).join(' '))}" data-n="${m.ne}">
                    <div class="nway-node-name">${this._cgName(it)}</div><div class="nway-node-sub">${this._cgReach(it, focus, files)}</div>${twin}</div>`;
            };
            // Groups form over all `entries`, then `cap` counts a group as one card; the
            // entries past it come back in `rest` for the "+N more" card.
            const col0 = (role, lv, entries, cap = Infinity, canon = null) => {
                const gs = st.suggest === 'lanes' ? [] : st.suggest === 'same' ? this._cgGroupsSame(`${role}${lv}|`, entries, files, focus) : st.suggest ? this._cgGroups(`${role}${lv}|`, entries, files, focus, st.place, canon) : [];
                const by = new Map(), done = new Set(), pos = new Map(), units = [];
                gs.forEach(g => g.m.forEach(r => by.set(r.x, g)));
                for (const x of entries) {
                    const g = by.get(x);
                    if (!g) units.push({ xs: [x], html: () => node(role, lv, x) });
                    else if (!done.has(g)) {
                        done.add(g);
                        units.push({ xs: g.m.map(r => r.x), html: () => {
                            const chip = g.same ? `<span class="nway-grp" title="${g.m.length} cards in exactly the same files: likely one change or fork.">&sube; ${g.label} &middot; ${g.cov}/${files.length} files</span>` : `<span class="nway-grp" title="Suggested merge: ${g.m.length} cards with complementary files and the same parent.">&asymp; ${g.label} &middot; ${g.cov}/${files.length}</span>`;
                            const cells = {};
                            g.m.forEach(m => Object.entries(m.x.it.cells).forEach(([k, v]) => { if (typeof v === 'string') cells[k] = v; }));
                            const pieces = g.m.map(m => ({ name: m.x.it.name, files: Object.keys(m.x.it.cells).filter(k => typeof m.x.it.cells[k] === 'string'), fids: Object.values(m.x.it.cells).filter(v => typeof v === 'string').slice(0, 3) }));
                            if (!g.same) this._cgGrp[g.id] = { name: g.m[0].x.it.name, cells, pieces };
                            const focusBtn = g.same ? '' : `<button class="nway-chip" data-cg-focusgrp="${escapeAttr(g.id)}" title="Make this merged group the focus: its functions in every file">&#9678; focus</button>`;
                            return `<div class="nway-grpbox${lv ? ' deep' : ''}"><div class="nway-grpbox-h">${chip}${focusBtn}</div>${g.m.map((r, i) => node(role, lv, r.x) + (i && !g.same ? this._cgSim(pieces[0].fids, pieces[i].fids, pieces[0].name, pieces[i].name) : '')).join('')}</div>`;
                        } });
                    }
                }
                const shown = units.slice(0, cap);
                shown.forEach((u, n) => u.xs.forEach(x => pos.set(x.it.key, n)));
                return { html: shown.map(u => u.html()).join(''), pos, canon: gs.canon, n: shown.length, rest: units.slice(cap).flatMap(u => u.xs), kept: shown.flatMap(u => u.xs) };
            };
            // Every out-of-scope neighbor of a side as one faded card.
            const merged = (role, list, more) => {
                const ms = list.map(({ it }) => this._cgMeasures(it, focus, files));
                const present = files.map((_, k) => ms.some(m => m.present[k])), edge = files.map((_, k) => ms.some(m => m.edge[k]));
                const r = this._cgStrip(present, files, edge);
                const ne = edge.filter(Boolean).length;
                const names = list.map(({ it }) => it.name || '?').join(', ');
                const axis = st.scope === 'library' ? 'code' : 'library';
                const ax = more ? { color: 'var(--accent)', icon: 'fa-solid fa-ellipsis' } : AXIS[axis];
                const label = more ? `+${list.length} more` : `${list.length} ${axis}`;
                return `<div class="nway-node ${more ? '' : 'out '}merged" tabindex="0" ${more ? `data-cg-more="${role}"` : 'data-cg-expand="1"'} data-side="${role}" data-key="${more ? 'more' : 'merged'}" data-parents="${role === 'cocallers' ? '' : 'focus'}" data-n="${ne}" style="border-color:${ax.color}" title="${escapeAttr(names)}&#10;click to show each one">
                    <div class="nway-node-name"><b style="color:${ax.color}"><i class="${ax.icon}"></i> ${label}</b></div><div class="nway-node-sub">${r.html}<span class="nway-edgecnt" title="called from ${ne} of ${files.length} files"><i class="fa-solid fa-link"></i> ${ne}/${files.length}</span></div></div>`;
            };
            // columns[role][lv]: the hop-1 column keeps the in-scope / out-of-scope handling,
            // later hops only list rows whose parent is on screen.
            // A side cut at the server's node cap says so with a card, not a footnote.
            const capped = role => nb[role] && nb[role].truncated ? `<div class="nway-node static capped" title="The server returns at most 60 nodes per side: the rest of this side is not loaded"><div class="nway-node-name"><b><i class="fa-solid fa-scissors"></i> capped at 60</b></div><div class="nway-node-sub dim">more not loaded</div></div>` : '';
            const columns = { callers: [], callees: [] };
            const keysL = { callers: [], callees: [] }, posL = { callees: [] }, canonL = { callees: [] };
            this._cgGrp = {};
            this._cgSib = {};
            this._cgItems = {};
            for (const role of ['callers', 'callees']) {
                const all = this._cgShown(nb, role);
                const inn = all.filter(x => !x.out), out = all.filter(x => x.out);
                let first, c0;
                if (st.outside === 'merge') {
                    c0 = col0(role, 0, inn, st.more[role] ? Infinity : CAP);
                    first = c0.html + (c0.rest.length ? merged(role, c0.rest, true) : '') + (out.length ? merged(role, out) : '');
                    var prev = new Set(c0.kept.map(x => x.it.key).concat(out.length ? ['merged'] : []));
                } else {
                    const list = inn.concat(out);
                    c0 = col0(role, 0, list, st.more[role] ? Infinity : CAP);
                    first = c0.html + (c0.rest.length ? merged(role, c0.rest, true) : '');
                    var prev = new Set(c0.kept.map(x => x.it.key));
                }
                columns[role].push(first + capped(role));
                keysL[role].push(prev);
                // Later hops are ordered by where their parents sit, so a path stays beside its parent.
                let prevPos = c0.pos, canon = c0.canon;
                prevPos.set('more', c0.n);
                prevPos.set('merged', c0.n);
                if (role === 'callees') { posL.callees.push(prevPos); canonL.callees.push(canon); }
                for (let lv = 1; lv < depth; lv++) {
                    const level = (nb[role].levels[lv] || []).map((it, i) => ({ it, i, out: !(st.scope === 'all' || (st.scope === 'library') === !!it.library) }))
                        .filter(x => !x.out && x.it.parents.some(k => prev.has(k)));
                    const bary = x => {
                        const ps = x.it.parents.map(k => prevPos.get(k)).filter(v => v !== undefined);
                        return ps.length ? ps.reduce((a, b) => a + b, 0) / ps.length : Infinity;
                    };
                    level.sort((a, b) => bary(a) - bary(b));
                    const c = col0(role, lv, level, Infinity, canon);
                    columns[role].push(c.html);
                    prevPos = c.pos;
                    canon = c.canon;
                    prev = new Set(level.map(x => x.it.key));
                    keysL[role].push(prev);
                    if (role === 'callees') { posL.callees.push(prevPos); canonL.callees.push(canon); }
                }
            }
            // The other callers of the callees at hop s + 1 sit in the column left of them (the focus column for
            // hop 1), so every edge still runs caller -> callee. Few callees: any sharer; else 2+ shared.
            let coHtml = '';
            const sibs = sl => {
                const minShare = (nb.cocallers.counts[sl] || 0) >= 3 ? 2 : 1;
                const bary = x => {
                    const ps = x.it.parents.map(k => posL.callees[sl].get(k)).filter(v => v !== undefined);
                    return ps.length ? ps.reduce((a, b) => a + b, 0) / ps.length : Infinity;
                };
                const inScope = this._cgShown(nb, 'cocallers', sl).filter(x => !x.out && x.it.parents.some(k => keysL.callees[sl].has(k)));
                const all = inScope.filter(x => st.more.cocallers || x.it.shared >= minShare).sort((a, b) => bary(a) - bary(b));
                const weak = inScope.length - all.length;
                const cc = col0('cocallers', sl, all, st.more.cocallers ? Infinity : COCAP, canonL.callees[sl]);
                const html = cc.html + (cc.rest.length ? merged('cocallers', cc.rest, true) : '') + (weak ? `<div class="nway-node merged" tabindex="0" data-cg-more="cocallers" title="Callers sharing fewer than ${minShare} callees: click to show them"><div class="nway-node-name"><b style="color:var(--accent)"><i class="fa-solid fa-ellipsis"></i> +${weak} weaker</b></div><div class="nway-node-sub dim">share &lt; ${minShare} callees</div></div>` : '') + (sl ? '' : capped('cocallers'));
                return html ? `<div class="nway-cg-sibs"><div class="dim nway-cg-sibh">also call these callees</div>${html}</div>` : '';
            };
            if (nb.cocallers) {
                coHtml = sibs(0);
                for (let sl = 1; sl < depth && posL.callees[sl]; sl++) columns.callees[sl - 1] += sibs(sl);
            }
            const widths = lv => (lv ? '170px' : '230px');
            const cols = [...Array(depth).keys()].reverse().map(widths).concat(['230px'], [...Array(depth).keys()].map(widths));
            const col = html => `<div class="nway-gcol">${html}</div>`;
            if (st.suggest === 'lanes') {
                // One horizontal band per file slice (same files); a card sits in the band of its files, in its hop's column.
                const inScope = it => st.scope === 'all' || (st.scope === 'library') === !!it.library;
                const placed = [], shownKeys = { callers: [], callees: [] };
                let outMerged = {};
                for (const role of ['callers', 'callees']) {
                    for (let lv = 0; lv < depth; lv++) {
                        let list = this._cgShown(nb, role, lv).filter(x => lv ? inScope(x.it) && x.it.parents.some(k => shownKeys[role][lv - 1].has(k)) : st.outside === 'merge' ? !x.out : true);
                        if (lv === 0 && st.outside === 'merge') outMerged[role] = this._cgShown(nb, role).filter(x => x.out);
                        shownKeys[role].push(new Set(list.map(x => x.it.key)));
                        list.forEach(x => placed.push({ role, lv, x, m: this._cgMeasures(x.it, focus, files) }));
                    }
                }
                const bySig = new Map();
                for (const c of placed) {
                    c.sig = c.m.present.map(v => (v ? 1 : 0)).join('');
                    if (c.m.np >= 2) bySig.set(c.sig, (bySig.get(c.sig) || 0) + 1);
                }
                const lanes = [...bySig].filter(([, n]) => n >= 2).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([sig]) => ({ sig, present: [...sig].map(v => v === '1'), n: 0 }));
                const laneOf = c => {
                    let i = lanes.findIndex(l => l.sig === c.sig);
                    if (i < 0 && c.m.np >= 2) {
                        // a subset of a slice's files joins the smallest slice that holds it
                        const sup = lanes.map((l, j) => ({ j, size: l.present.filter(Boolean).length })).filter(({ j }) => c.m.present.every((v, k) => !v || lanes[j].present[k])).sort((a, b) => a.size - b.size);
                        if (sup.length) i = sup[0].j;
                    }
                    return i < 0 ? lanes.length : i;
                };
                const cells = new Map();
                const put = (r, col, html) => { const k = `${r}|${col}`; cells.set(k, (cells.get(k) || '') + html); };
                const colOf = (role, lv) => role === 'callers' ? depth - lv : depth + 2 + lv;
                const rowCount = new Array(lanes.length + 1).fill(0);
                for (const c of placed) { c.lane = laneOf(c); rowCount[c.lane]++; put(2 * c.lane + 2, colOf(c.role, c.lv), node(c.role, c.lv, c.x)); }
                for (const role of ['callers', 'callees']) {
                    const out = outMerged[role] || [];
                    if (out.length) { rowCount[lanes.length]++; put(2 * lanes.length + 2, colOf(role, 0), merged(role, out)); }
                }
                const heads = [];
                this._cgLaneFiles = [];
                [...lanes, { other: true }].forEach((l, i) => {
                    if (!rowCount[i]) return;
                    const label = l.other ? '<span class="dim">other cards</span>' : this._cgStrip(l.present, files).html;
                    this._cgLaneFiles[i] = l.other ? files.map((_, k) => placed.some(c => c.lane === i && c.m.present[k])) : l.present;
                    heads.push(`<div class="nway-lane-h" style="grid-row:${2 * i + 1}"><span class="nway-lane-lbl" data-lane="${i}">${label}<span class="dim">${rowCount[i]} card${rowCount[i] > 1 ? 's' : ''}</span></span></div>`);
                });
                const rows = 2 * (lanes.length + 1);
                const body = [...cells].map(([k, html]) => { const [r, c] = k.split('|'); return `<div class="nway-gcol" style="grid-row:${r};grid-column:${c}">${html}</div>`; }).join('');
                const fc = `<div class="nway-gcol nway-lane-focus" style="grid-row:1 / span ${rows};grid-column:${depth + 1}"><div class="nway-node center${focus.pieces ? ' merged-focus' : ''}" data-sel="focus"><div class="nway-node-name">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}</div><div class="nway-node-sub dim">focus</div>${this._cgPieces(focus, files)}</div>${coHtml}</div>`;
                return `<div class="nway-graph" data-mid="${depth}" data-lanes="1" style="grid-template-columns:${cols.join(' ')}; column-gap:56px; width:max-content"><svg></svg>${heads.join('')}${body}${fc}</div>`;
            }
            const grid = depth > 1 ? ` style="grid-template-columns:${cols.join(' ')}; column-gap:56px; width:max-content"` : '';
            const left = [...columns.callers].reverse().map(col).join(''), right = columns.callees.map(col).join('');
            return `<div class="nway-graph" data-mid="${depth}"${grid}><svg></svg>${left}
                <div class="nway-gcol"><div class="nway-node center${focus.pieces ? ' merged-focus' : ''}" data-sel="focus"><div class="nway-node-name">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}</div><div class="nway-node-sub dim">focus</div>${this._cgPieces(focus, files)}</div>${coHtml}</div>
                ${right}</div>`;
        }

        // Cards have the height their tags need, so the edges are drawn from where they landed.
        _cgEdges() {
            const g = this._dw && this._dw.querySelector('.nway-graph');
            if (!g) return;
            const gr = g.getBoundingClientRect();
            const center = g.querySelector('.nway-node.center');
            const byKey = {};
            const nodes = [...g.querySelectorAll('[data-key]')];
            nodes.forEach(n => { for (const k of (n.dataset.alias || n.dataset.key).split(' ')) byKey[`${n.dataset.side}|${k}`] = n; });
            // Columns are centered, so slide each hop beyond the first toward its parents
            // (nearest the focus first), within the room the graph has.
            const cols = [...g.children].filter(e => e.classList.contains('nway-gcol'));
            const mid = Number(g.dataset.mid);
            const midY = r => r.top + r.height / 2;
            const slide = [];
            for (let k = 1; k < mid && !g.dataset.lanes; k++) slide.push(cols[mid - 1 - k], cols[mid + 1 + k]);
            {
                for (const col of slide) {
                    const ns = [...col.querySelectorAll('[data-key]')].filter(n => n.dataset.side !== 'cocallers');
                    const ys = [], py = [];
                    for (const n of ns) {
                        ys.push(midY(n.getBoundingClientRect()));
                        for (const pk of n.dataset.parents.split(' ')) {
                            const par = byKey[`${n.dataset.side === 'cocallers' ? 'callees' : n.dataset.side}|${pk}`];
                            if (par) py.push(midY(par.getBoundingClientRect()));
                        }
                    }
                    if (!ys.length || !py.length) continue;
                    const avg = a => a.reduce((x, y) => x + y, 0) / a.length;
                    const cr = col.getBoundingClientRect();
                    col.style.transform = `translateY(${Math.max(gr.top - cr.top, Math.min(gr.bottom - cr.bottom, avg(py) - avg(ys))).toFixed(1)}px)`;
                }
            }
            const svg = g.querySelector(':scope > svg');
            svg.setAttribute('viewBox', `0 0 ${gr.width} ${gr.height}`);
            svg.setAttribute('width', gr.width);
            svg.setAttribute('height', gr.height);
            const paths = [], meta = [];
            for (const n of nodes) {
                for (const pk of n.dataset.parents.split(' ')) {
                    const parent = pk === 'focus' ? center : byKey[`${n.dataset.side === 'cocallers' ? 'callees' : n.dataset.side}|${pk}`];
                    if (!parent) continue;
                    // a callee is called by its parent; a caller calls its parent
                    const [from, to] = n.dataset.side === 'callees' ? [parent, n] : [n, parent];
                    const a = from.getBoundingClientRect(), b = to.getBoundingClientRect();
                    const x1 = a.right - gr.left, x2 = b.left - gr.left;
                    const y1 = a.top + a.height / 2 - gr.top, y2 = b.top + b.height / 2 - gr.top;
                    const mx = (x1 + x2) / 2;
                    meta.push([n, parent]);
                    paths.push(`<path data-i="${meta.length}" class="nway-edge${n.classList.contains('out') ? ' out' : ''}${n.dataset.side === 'cocallers' ? ' co' : ''}" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" style="stroke-width:${Math.min(1 + Number(n.dataset.n) * 0.8, 8).toFixed(1)}"></path>`);
                }
            }
            // A neighbor past the depth limit: a line that fades away from the card's outer side.
            const stubs = [];
            nodes.filter(n => n.dataset.beyond).forEach((n, i) => {
                const a = n.getBoundingClientRect(), left = n.dataset.side === 'callers';
                const x1 = (left ? a.left : a.right) - gr.left, x2 = x1 + (left ? -46 : 46), y = a.top + a.height / 2 - gr.top;
                const deeper = n.dataset.last && Number(g.dataset.mid) < 5, id = `cgfade${i}`;
                stubs.push(`<linearGradient id="${id}" gradientUnits="userSpaceOnUse" x1="${x1}" x2="${x2}" y1="0" y2="0"><stop offset="0" style="stop-color:var(--accent)" stop-opacity="0.8"/><stop offset="1" style="stop-color:var(--accent)" stop-opacity="0"/></linearGradient>
                    <g class="nway-stub" data-node="${escapeAttr(n.dataset.side + '|' + n.dataset.key)}"${deeper ? ' data-cg-deeper="1"' : ''}><title>${Number(n.dataset.beyond)} more ${left ? 'callers' : 'callees'} ${n.dataset.last ? 'past this depth' : 'not shown (capped)'}${deeper ? ': click to go one hop deeper' : ''}</title><line x1="${x1}" x2="${x2}" y1="${y}" y2="${y}" stroke="url(#${id})" stroke-width="3" stroke-dasharray="6 4"/><text x="${x2 + (left ? 4 : -4)}" y="${y - 5}" text-anchor="${left ? 'end' : 'start'}">+${Number(n.dataset.beyond)}</text><rect x="${Math.min(x1, x2) - 4}" y="${y - 12}" width="54" height="24" fill="transparent"/></g>`);
            });
            // Calls back to a card shown in an earlier hop: a dashed edge, lit on hover, plus a count badge.
            const backs = [], backPaths = [];
            const hints = !!this._cg.hints;
            nodes.filter(n => hints && n.dataset.back).forEach(n => {
                const callees = n.dataset.side === 'callees';
                const targets = n.dataset.back.split(' ').map(k => byKey[`${n.dataset.side}|${k}`]).filter(Boolean);
                if (!targets.length) return;
                const a = n.getBoundingClientRect(), id = escapeAttr(n.dataset.side + '|' + n.dataset.key);
                // Calls always read left to right: out of the caller's right side, into the callee's left side.
                const edge = (from, to) => {
                    const f = from.getBoundingClientRect(), g2 = to.getBoundingClientRect();
                    const x1 = f.right - gr.left, y1 = f.top + f.height / 2 - gr.top, x2 = g2.left - gr.left, y2 = g2.top + g2.height / 2 - gr.top;
                    // a straight run out of the caller and into the callee, so the curve never meets a card at an angle
                    const m = 28, c = Math.max(30, Math.abs(x1 - x2) * 0.4);
                    return `<path class="nway-edge back" d="M${x1},${y1} L${x1 + m},${y1} C${x1 + m + c},${y1} ${x2 - m - c},${y2} ${x2 - m},${y2} L${x2},${y2}"></path>`;
                };
                targets.forEach(t => {
                    backs.push([n, t]);
                    backPaths.push(callees ? edge(n, t) : edge(t, n));
                });
                const k = targets.length;
                stubs.push(`<g class="nway-stub back" data-node="${id}"><title>${k} call${k > 1 ? 's' : ''} ${callees ? 'to' : 'from'} cards shown in an earlier hop</title><text x="${callees ? a.right - gr.left + 4 : a.left - gr.left - 4}" y="${a.top - gr.top + 11}" text-anchor="${callees ? 'start' : 'end'}">${callees ? k + ' &rarr;' : '&rarr; ' + k}</text></g>`);
            });
            // What a caller also calls (right of it) or a callee is also called by (left of it): a short angled stub.
            nodes.filter(n => hints && n.dataset.other).forEach(n => {
                const a = n.getBoundingClientRect(), left = n.dataset.side === 'callees', k = Number(n.dataset.other);
                const d = left ? -1 : 1, x1 = (left ? a.left : a.right) - gr.left, y1 = a.top + a.height / 2 - gr.top + 6;
                stubs.push(`<g class="nway-stub other" data-node="${escapeAttr(n.dataset.side + '|' + n.dataset.key)}"><title>${k} other ${left ? 'callers' : 'callees'} (besides the focus): click the card to recenter</title><path d="M${x1},${y1} C${x1 + d * 22},${y1} ${x1 + d * 30},${y1 + 6} ${x1 + d * 30},${y1 + 18}"/><text x="${x1 + d * 30}" y="${y1 + 29}" text-anchor="middle">${k}</text></g>`);
            });
            svg.innerHTML = '<defs><marker id="cgarrow" viewBox="0 0 10 10" refX="8" refY="5" markerUnits="userSpaceOnUse" markerWidth="9" markerHeight="9" orient="auto"><path d="M1,1 L8,5 L1,9" fill="none" stroke="context-stroke" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>' + paths.join('') + backPaths.join('') + stubs.join('');
            this._cgHover(g, center, meta, backs);
        }

        // Hovering a card lights it, everything it calls further out, and the path back to the focus.
        _cgHover(g, center, meta, backs = []) {
            const kids = new Map(), ups = new Map();
            for (const [n, par] of meta) {
                if (!kids.has(par)) kids.set(par, []);
                kids.get(par).push(n);
                if (!ups.has(n)) ups.set(n, []);
                ups.get(n).push(par);
            }
            const walk = (n, map, on) => { for (const m of map.get(n) || []) if (!on.has(m)) { on.add(m); walk(m, map, on); } };
            const paths = [...g.querySelectorAll(':scope > svg > path.nway-edge:not(.back)')];
            const clear = () => {
                g.classList.remove('hl');
                g.querySelectorAll('.hl-on').forEach(e => e.classList.remove('hl-on'));
            };
            g.onmouseover = e => {
                const n = e.target.closest('.nway-node');
                if (!n) return clear();
                const on = new Set([n]);
                walk(n, kids, on);
                walk(n, ups, on);
                const backOn = backs.map(([o, t], i) => (o === n || t === n ? i : -1)).filter(i => i >= 0);
                backOn.forEach(i => { on.add(backs[i][0]); on.add(backs[i][1]); });
                clear();
                g.classList.add('hl');
                on.forEach(x => x.classList.add('hl-on'));
                meta.forEach(([c, par], i) => { if (on.has(c) && on.has(par)) paths[i].classList.add('hl-on'); });
                const bp = g.querySelectorAll(':scope > svg > path.back');
                backOn.forEach(i => bp[i].classList.add('hl-on'));
                const lit = new Set([...on].map(x => `${x.dataset.side}|${x.dataset.key}`));
                g.querySelectorAll('.nway-stub').forEach(x => { if (lit.has(x.dataset.node)) x.classList.add('hl-on'); });
            };
            g.onmouseleave = clear;
        }

        _cgDraw(err) {
            if (!this._dw || !this._cg) return;
            if (this.onCg) this.onCg(this._cgUrl());
            const st = this._cg, focus = this._cgFocus(), files = this._cgFiles();
            const crumbs = st.stack.map((f, i) => `<button class="nway-crumb" data-cg-crumb="${i}">${escapeHtml(f.name)}</button>`).join('<span class="dim">/</span>');
            const outsideToggle = st.view === 'graph' && st.scope !== 'all' ? `<span class="nway-lbl" title="Neighbors outside the selected scope: grouped into one card per side, or each listed (faded)">Outside scope</span><div class="view-toggle" style="margin:0">${[['merge', 'Grouped'], ['each', 'Each']].map(([v, l]) => `<button class="view-btn${st.outside === v ? ' active' : ''}" data-cg-outside="${v}">${l}</button>`).join('')}</div>` : '';
            const scopeToggle = [['code', 'Code', AXIS.code.icon, AXIS.code.color], ['library', 'Library', AXIS.library.icon, AXIS.library.color], ['all', 'All', 'fa-solid fa-layer-group', 'var(--success)']].map(([v, text, icon, color]) => `<span class="bsim-tag-pill" data-cg-scope="${v}" style="${window.binSimPillStyle(st.scope === v, color)} padding:4px 10px; font-size:0.78rem;"><i class="${icon}"></i>${text}</span>`).join('');
            const depthCtl = st.view === 'graph' ? `<label class="nway-ctl" title="How many hops to walk. Beyond the first, only rows in the selected scope are expanded (8 per row, 60 nodes)."><span class="nway-lbl">Depth</span><b>${st.depth}</b><input type="range" min="1" max="5" value="${st.depth}" data-cg-depth></label>` : '';
            const suggestCtl = st.view === 'graph' ? `<span class="nway-lbl" title="Group cards on one side. Complementary: files that barely overlap and share a parent, probably one function split across files. Same files: cards present in exactly the same files, probably one change or fork. Lanes: one horizontal band per set of files.">Group</span><div class="view-toggle" style="margin:0">${[['off', 'Off'], ['comp', 'Complementary'], ['same', 'Same files'], ['lanes', 'Lanes']].map(([v, t]) => `<button class="view-btn${(st.suggest || 'off') === v ? ' active' : ''}" data-cg-suggest="${v}">${t}</button>`).join('')}</div>` : '';
            const coCtl = st.view === 'graph' ? `<button class="view-btn${st.co ? ' active' : ''}" data-cg-co title="Also show the other functions that call what the focus calls, under the focus (the ones sharing most callees), to find look-alikes and unmatched siblings. Callees called by more than 25 functions are skipped. Off by default: it can add a lot."><i class="fa-solid fa-share-nodes"></i> Also called by</button>` : '';
            const hintCtl = st.view === 'graph' ? `<button class="view-btn${st.hints ? ' active' : ''}" data-cg-hints title="Off by default. Per card, on hover: the other callers / callees it has beyond the focus (angled counts), and calls back to cards already shown in an earlier hop (dashed arrows)."><i class="fa-solid fa-diagram-project"></i> Hints</button>` : '';
            const hueCtl = `<button class="view-btn${this._hueOn ? ' active' : ''}" data-cg-hue title="Color each file's slot and column header, to match slots to columns. Off by default: the slots then show how many files have the function."><i class="fa-solid fa-palette"></i> Color files</button>`;
            const selCtl = st.view === 'graph' ? `<button class="view-btn${st.selMode ? ' active' : ''}" data-cg-select title="Select cards: click to toggle, drag the background to box-select (Shift adds). Ctrl-click works without this mode."><i class="fa-solid fa-object-group"></i> Select</button>` : '';
            const toggle = ['list', 'graph'].map(v => `<button class="view-btn${st.view === v ? ' active' : ''}" data-cg-view="${v}">${v === 'list' ? 'List' : 'Graph'}</button>`).join('');
            const on = files.map(c => !!focus.cells[c.id]);
            const fs = this._cgStrip(on, files);
            let body;
            if (err) body = `<div style="color:#f92672;">${escapeHtml(err)}</div>`;
            else if (!st.nb) body = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Reading call graph...</div>';
            else body = `<div class="dim nway-cg-legend"><i class="fa-solid fa-file-code"></i> files that have the function (color: how many) &middot; dot: the focus function there calls it &middot; <i class="fa-solid fa-link"></i> calls from the focus${st.view === 'graph' ? ' (edge width)' : ''}. Click to recenter.${st.co ? ' Dashed lines: other callers of the same callee.' : ''}${st.depth > 1 ? (st.view === 'graph' ? ' Further hops show in-scope rows only.' : ' The list shows the first hop; depth applies to the graph.') : ''}</div>` + (st.view === 'graph' ? this._cgGraph(st.nb, focus, files) : this._cgList(st.nb, focus, files));
            this._dw.innerHTML = `<div class="nway-cg-head"><div class="nway-cg-crumbs">${crumbs}</div><div class="nway-pills" title="Code: the program's own functions. Library: library code and imports. All: both.">${scopeToggle}</div>${outsideToggle}${depthCtl}${suggestCtl}${coCtl}${hintCtl}${hueCtl}${selCtl}<div class="view-toggle" style="margin:0">${toggle}</div>${this.cgInit ? '' : '<button class="nway-chip" data-cg-pop title="Open in its own page (keeps history)"><i class="fa-solid fa-up-right-from-square"></i></button><button class="nway-chip" data-cg-close title="Hide the panel (the N-WAY CALLS handle reopens it)">&times;</button>'}</div>
                <div class="nway-cg-focus">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}<div>${fs.html} <span class="dim">present in ${fs.n}/${files.length} files</span></div></div>
                <div class="nway-cg-selbar" hidden></div>
                <div class="nway-cg-body${st.selMode ? ' selmode' : ''}">${body}</div>`;
            this._cgFillSims();
            this._cgSelSync();
            if (!err && st.nb && st.view === 'graph') {
                this._cgEdges();
                const body = this._dw.querySelector('.nway-cg-body'), c = body.querySelector('.nway-node.center');
                const cr = c.getBoundingClientRect(), br = body.getBoundingClientRect();
                body.scrollLeft = Math.max(0, cr.left - br.left + body.scrollLeft + cr.width / 2 - body.clientWidth / 2);
                body.scrollTop = Math.max(0, cr.top - br.top + body.scrollTop + cr.height / 2 - body.clientHeight / 2);
            }
        }

        _onDrawerClick(e) {
            if (!this._cg) return;
            if (this._cgSwallow) { this._cgSwallow = false; return; }
            const act = e.target.closest('[data-cg-selact]');
            if (act) return this._cgSelAct(act.dataset.cgSelact);
            if (e.target.closest('[data-cg-select]')) {
                this._cg.selMode = !this._cg.selMode;
                return this._cgDraw();
            }
            const card = e.target.closest('.nway-node[data-sel]');
            if (card && (this._cg.selMode || e.ctrlKey || e.metaKey) && !e.target.closest('button, a, input')) {
                e.preventDefault();
                window.getSelection().removeAllRanges();
                const sel = this._cg.sel || (this._cg.sel = new Set()), k = card.dataset.sel;
                if (!sel.delete(k)) sel.add(k);
                return this._cgSelSync();
            }
            const t = e.target.closest('[data-cg-go],[data-cg-view],[data-cg-scope],[data-cg-outside],[data-cg-expand],[data-cg-crumb],[data-cg-close],[data-cg-pop],[data-cg-more],[data-cg-suggest],[data-cg-co],[data-cg-hints],[data-cg-hue],[data-cg-focusgrp],[data-cg-mergefocus],[data-cg-deeper]');
            if (!t || !this._cg) return;
            if (t.dataset.cgClose !== undefined) return window.closeCallsPanel();
            if (t.dataset.cgPop !== undefined) {
                const url = this._cgUrl();
                window.closeCallsPanel();
                return window.Nav.openPath(url, e);
            }
            if (t.dataset.cgView) { this._cg.view = t.dataset.cgView; return this._cgDraw(); }
            if (t.dataset.cgOutside) { this._cg.outside = t.dataset.cgOutside; return this._cgDraw(); }
            if (t.dataset.cgCo !== undefined) { this._cg.co = !this._cg.co; return this._cgShow(); }
            if (t.dataset.cgHue !== undefined) {
                try { localStorage.setItem('nway.hue', this._hueOn ? '0' : '1'); } catch (err) { /* storage blocked */ }
                this._applyHue();
                return this._cgDraw();
            }
            if (t.dataset.cgHints !== undefined) { this._cg.hints = !this._cg.hints; return this._cgDraw(); }
            if (t.dataset.cgFocusgrp) {
                const g = this._cgGrp && this._cgGrp[t.dataset.cgFocusgrp];
                if (!g) return;
                this._cg.stack.push({ name: g.name, cells: g.cells, pieces: g.pieces });
                this._cg.more = {};
                return this._cgShow();
            }
            if (t.dataset.cgDeeper !== undefined && this._cg.depth < 5) {
                this._cg.depth++;
                return this._cgShow();
            }
            if (t.dataset.cgMergefocus) {
                const it = this._cgSib && this._cgSib[t.dataset.cgMergefocus];
                if (!it) return;
                const f = this._cg.stack[this._cg.stack.length - 1];
                const own = o => Object.keys(o).filter(k => typeof o[k] === 'string');
                const cells = { ...f.cells };
                for (const k of own(it.cells)) if (typeof cells[k] !== 'string') cells[k] = it.cells[k];
                const fidsOf = o => Object.values(o).filter(v => typeof v === 'string').slice(0, 3);
                const pieces = (f.pieces || [{ name: f.name, files: own(f.cells), fids: fidsOf(f.cells) }]).concat([{ name: it.name, files: own(it.cells), fids: fidsOf(it.cells) }]);
                this._cg.stack.push({ name: f.name, cells, pieces });
                this._cg.more = {};
                return this._cgShow();
            }
            if (t.dataset.cgSuggest) {
                this._cg.suggest = t.dataset.cgSuggest === 'off' ? false : t.dataset.cgSuggest;
                this._cgDraw();
                return this._cg.nb ? this._cgPlace(this._cgTok) : undefined;
            }
            if (t.dataset.cgMore) { this._cg.more[t.dataset.cgMore] = true; return this._cgDraw(); }
            if (t.dataset.cgExpand) { this._cg.outside = 'each'; return this._cgDraw(); }
            if (t.dataset.cgScope) { this._cg.scope = t.dataset.cgScope; return this._cg.depth > 1 || this._cg.co ? this._cgShow() : this._cgDraw(); }
            if (t.dataset.cgCrumb) {
                this._cg.stack = this._cg.stack.slice(0, Number(t.dataset.cgCrumb) + 1);
                this._cg.more = {};
                return this._cgShow();
            }
            const [role, lv, i] = t.dataset.cgGo.split(':');
            const it = this._cg.nb && this._cg.nb[role].levels[Number(lv)][Number(i)];
            if (!it || it.kind !== 'row') return;
            const cells = {};
            for (const [k, v] of Object.entries(it.cells)) if (typeof v === 'string') cells[k] = v;
            this._cg.stack.push({ name: it.name, cells, name_from: it.name_from });
            this._cg.more = {};
            this._cgShow();
        }

        _row(row, ri) {
            const cols = this.data.columns;
            const extra = Array.isArray(row.names) ? row.names.length - 1 : Number(row.names || 0) - 1;
            const low = row.support < LOW_SUPPORT && (row.file_span || row.span) > 2
                ? `<i class="fa-solid fa-triangle-exclamation nway-warn" title="low support: only ${pct(row.support)} of the possible pairs in this row are matched; it may be a chain of transitive matches"></i>` : '';
            return `<tr>
                <td class="nway-open" data-fn-open="${Number(ri)}" title="Compare this row's functions: strings, APIs, callees, constants and code" style="min-width:260px; max-width:420px;">${this._nameSig(row, { noClick: true })}<span class="nway-name">${extra > 0 ? `<span class="dim">+${extra} names</span>` : ''}${low}</span></td>
                <td class="num">${Number(row.file_span || row.span)}</td>
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
                const fi = this._cgFiles().findIndex(x => x.id === c.id);
                const sw = fi >= 0 && this._cgFiles().length <= 16 ? `<i class="nway-sw" style="--h:${this._fileHue(fi)}"></i>` : '';
                return `<th class="col" data-col="${escapeAttr(c.id)}" title="${escapeAttr(`${c.file_name} (${c.collection}) - right-click: focus / filter`)}"><div class="nway-colhead">${sw}${name}${window.EntityRenderer.renderMd5(c.md5, { collection: c.collection })}<span class="nway-sub">${Number(c.functions)} fn${coverage}</span></div></th>`;
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
            this._scrollRestore();
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
            const t = e.target.closest('[data-nway-file],[data-set],[data-tab],[data-sort],[data-nav],[data-open-cluster],[data-focus-toggle],[data-clear],[data-fn-open]');
            if (!t) return;
            if (t.dataset.nwayFile) {
                e.preventDefault(); e.stopPropagation();
                const f = this._files().find(x => x.id === t.dataset.nwayFile);
                if (!f) return;
                const base = this.source.pool ? `/pools/${encodeURIComponent(this.source.pool)}` : '';
                return window.Nav.openPath(`${base}/collections/${encodeURIComponent(f.collection)}/files/${encodeURIComponent(f.md5)}`, e, { title: f.file_name || f.md5, type: 'file' });
            }
            if (t.dataset.fnOpen !== undefined) {
                if (e.target.closest('button, a, input, .bsim-tag-pill, .entity-actions')) return;
                const url = this._fnUrl(this.data.items[Number(t.dataset.fnOpen)]);
                if (!url) return;
                e.preventDefault();
                this._scrollSave();
                return window.Nav.openPath(url, e);
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

    // Function page URL from a function id (the inline diff's header cards).
    NwayPanel.fnUrl = (fid, pool) => {
        const { md5 } = fnParts(fid);
        return fnUrl({ collection: fid.slice(0, fid.indexOf(':func:')), md5 }, fid, pool);
    };

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

    // The N-way diff a graph URL (/diff/nway/graph?...) came from: the cluster's N-way tab, or /diff/nway for a file set.
    NwayPanel.backUrl = function (q) {
        const uuid = q.get('cluster_uuid');
        if (!uuid) {
            const keep = new URLSearchParams();
            for (const [k, v] of q) if (!k.startsWith('cg_') && !k.startsWith('fx_')) keep.set(k, v);
            return `/diff/nway?${keep.toString()}`;
        }
        const pool = q.get('pool');
        const base = pool ? `/pools/${encodeURIComponent(pool)}/files/clusters/${encodeURIComponent(uuid)}` : window.Nav.buildUIUrl(q.get('collection'), ['files', 'clusters', uuid]);
        const out = new URLSearchParams({ tab: 'functions' });
        for (const k of ['axis', 'scope', 'min_edge', 'mode', 'columns', 'child_presence']) if (q.has(k)) out.set(k, q.get(k));
        return `${base}?${out.toString()}`;
    };

    // Selected file ids are `{coll}:file:{md5}`; the page's pool (if any) rides along.
    NwayPanel.urlFor = function (ids, fallbackCollection) {
        const md5s = ids.map(id => `${id.split(':')[0] || fallbackCollection}:${id.split(':').pop()}`).join(',');
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        const anchor = fallbackCollection || (ids[0] || '').split(':')[0];
        return `/diff/nway?md5s=${encodeURIComponent(md5s)}${pool ? `&pool=${encodeURIComponent(pool)}` : anchor ? `&collection=${encodeURIComponent(anchor)}` : ''}`;
    };

    // Family view of any set of functions (`coll:func:md5:addr` ids), whatever their files or collections.
    NwayPanel.urlForFunctions = function (fids, name) {
        const qs = new URLSearchParams({ scope: 'functions', fx_fids: [...new Set(fids)].join(','), fx_name: name || `${fids.length} functions`, fx_tab: 'facets' });
        return `/diff/nway/function?${qs.toString()}`;
    };

    window.NwayPanel = NwayPanel;
})();
