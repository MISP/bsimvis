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
 * `opts.cg` ({name, cells, view, scope, depth}) makes the panel a standalone
 * call graph on `container` (no table); `onCg(snapshot)` fires on every graph change.
 * Rows are paged
 * server-side (Prev/Next, or appended on scroll -- see paging.js); nothing here
 * loads a full table. Function and file names come off
 * uploaded samples, so every value goes through escapeHtml / escapeAttr.
 */
(function () {
    const TABS = [['all', 'All'], ['core', 'Core'], ['partial', 'Partial'], ['unique', 'Unique']];
    const PAGE = 50;
    const NWAY_TIMEOUT_MS = 180000;
    const SORTS = [['name', 'Function'], ['span', 'Span'], ['weight', 'Features'], ['support', 'Support'], ['cohesion', 'Cohesion']];
    // Sliders whose value is a whole number.
    const INT_RANGES = ['k', 'twin_depth', 'group_lane_depth', 'group_lane_fold'];
    const DEFAULTS = {
        tab: 'all', scope: 'code', k: 2, min_edge: '', mode: 'stored',
        q: '', tags: '', sort_col: '', sort_dir: 'desc', offset: 0,
        columns: 'twins', child_presence: 0.5, twin_depth: 3, twin_link: 0.5,
        column: '', focus: '', focus_rule: 'any', side: '',
        col_by: '', col_by_multi: '', col_set: '', col_presence: '', cmp: '', col_pick: '', col_rest: '',
        group: 'lane', group_axis: '', group_axis_b: '', group_merge: 'name,call', group_min_link: '', group_sort: 'lanes', group_lane_min: '', group_lane_fit: '', group_lane_depth: 3, group_lane_link: 0.5, group_lane_tier: 0.5, group_lane_fold: 3, lane_names: 'on', group_prefix_min: '', col_open: '',
    };
    // State keys that go to the API as query params (empty ones are dropped).
    const API_KEYS = ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset', 'columns', 'child_presence', 'twin_depth', 'twin_link', 'column', 'focus', 'focus_rule', 'side', 'group', 'group_axis', 'group_axis_b', 'group_merge', 'group_min_link', 'group_sort', 'group_lane_min', 'group_lane_fit', 'group_lane_depth', 'group_lane_link', 'group_lane_tier', 'group_lane_fold', 'group_prefix_min', 'col_open', 'col_by', 'col_by_multi', 'col_set', 'col_presence', 'col_pick', 'col_rest'];
    // Group by: rows bundled by file set, nested under one tag axis, or joined as one probable function.
    const GROUPS = [
        ['lane', 'Lane', 'Bundles of rows that appear in exactly the same files, biggest bundle first. With the merge on, a merged function is bundled by all the files it covers together. A set of files only one function has gets no lane and stays a plain row beside the lanes, unless it is a bundle minus a file or two (90% or more of its files), which joins it with a chip.'],
        ['prefix', 'Prefix', 'Group by the first word of the function name (attack_*, killer_*...). Functions with no prefix, or a prefix only one function has, carry on to the next level beside the prefix groups.'],
        ['tag', 'Tag', 'Nest under the tags of one axis (origin, capa, YARA...). Something with several tags on the axis is listed under each.'],
    ];
    const TAG_AXES = [['origin', 'Origin'], ['severity', 'Severity'], ['category', 'Behavior'], ['user', 'User'], ['capa', 'Capa'], ['mitre', 'ATT&CK'], ['mbc', 'MBC'], ['yara', 'YARA'], ['ruleset', 'Ruleset'], ['family', 'Family'], ['vuln', 'Vulnerability']];
    // Widths in px: Function, Span, Features, Support, Cohesion, Cluster, then one square column per file.
    const DATA_COLS = [380, 64, 90, 90, 90, 210];
    const SQ_COL = 28;
    // Names a lane focus in the call graph: not one function, so no signature and no rename.
    const LANE_MARK = '\u29c9 ';
    const BAND_H = 18;
    const GROUP_SORTS = [['files', 'File coverage'], ['lanes', 'Lanes first'], ['weight', 'Features'], ['rows', 'Rows']];
    const LOW_SUPPORT = 0.5;
    // Same icon + color as the Code / Library / All pills in the table's Show control.
    const AXIS = { code: { icon: 'fa-solid fa-code', color: 'var(--info, #3b82f6)' }, library: { icon: 'fa-solid fa-cubes', color: 'var(--warning, #d97706)' } };
    // Match presets set the two thresholds together; '' is the server default edge.
    // ponytail: placeholder values, calibrate on real collections.
    const PRESETS = [
        ['strict', 'Strict', { min_edge: 0.8, child_presence: 0.8 }, 'Only close matches count; a child needs most of its files to hold a function.'],
        ['balanced', 'Balanced', { min_edge: '', child_presence: 0.5 }, 'The collection default edge; half of a child cluster must hold a function.'],
        ['loose', 'Loose', { min_edge: 0.3, child_presence: 0.3 }, 'Weaker matches group together; a third of a child cluster is enough.'],
    ];
    const DOC_KEYS = ['mode', 'min_edge', 'columns', 'child_presence', 'twin_depth', 'twin_link', 'focus', 'focus_rule', 'col_by', 'col_by_multi', 'col_set', 'col_presence', 'col_pick', 'col_rest'];
    // Keys the metadata column merge offers when the server cannot count values.
    const BY_KEYS = ['collection', 'batch_uuid', 'architecture', 'executable_format'];
    // Compare groups filter files with the Files page parameters (/api/file/search).
    const FILE_FIELDS = [['q', 'Search', 'keywords'], ['file_tag', 'Tag', 'family:mirai, lib:*'], ['file_name', 'File name', 'bot*'], ['batch_uuid', 'Batch', 'uuid'], ['language_id', 'Language', 'x86:LE:64']];
    const FILE_MORE = [['filetype', 'File type'], ['status', 'Status'], ['inferred_tags', 'Inferred tags'], ['bin_cluster_uuid', 'Cluster'], ['min_function_count', 'Min functions'], ['max_function_count', 'Max functions'], ['min_entry_date', 'From date'], ['max_entry_date', 'To date']];
    const FILE_MULTI = new Set(['file_tag']);
    const FILE_LABELS = Object.fromEntries([...FILE_FIELDS, ...FILE_MORE].map(f => [f[0], f[1]]));
    const FILE_KNOWN = new Set([...FILE_FIELDS, ...FILE_MORE].map(f => f[0]));
    const GROUP_MAX = 8;
    // Merge files options: icon, and the colour the dropdown form uses.
    const FILE_ICONS = { files: 'fa-regular fa-square', children: 'fa-solid fa-sitemap', twins: 'fa-solid fa-clone', by: 'fa-solid fa-tags' };
    const letter = i => String.fromCharCode(65 + i);
    const fileCount = n => `${Number(n)} ${Number(n) === 1 ? 'file' : 'files'}`;

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
            this.onSource = opts.onSource;
            this.state = { ...DEFAULTS, ...(opts.state || {}) };
            this.onState = opts.onState || null;
            this.onOpenCluster = opts.onOpenCluster || null;
            this.cgInit = opts.cg || null;
            this.onCg = opts.onCg || null;
            this.onData = opts.onData || null;
            this.onSlot = opts.onSlot || null;
            this.data = null;
            this.error = null;
            this._gen = 0;
            this._debounce = null;
            this._io = null;
            this._loading = false;
            // Groups start closed. `_gflip` holds the ids that differ from the `_gdef` default; the server drops closed groups' rows.
            this._gdef = 'none';
            this._gflip = new Set();
            // Auto depth: groups open when they fit the window, columns open as deep as the width allows.
            this._rowAuto = true;
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
            this._ddClose = e => document.querySelectorAll('details.nway-ddl[open]').forEach(d => { if (!d.contains(e.target)) d.removeAttribute('open'); });
            document.addEventListener('click', this._ddClose);
            this.el.addEventListener('mouseover', e => this._gTip(e, true));
            this.el.addEventListener('mouseout', e => this._gTip(e, false));
            this.el.addEventListener('pointerdown', e => { if (e.target.closest('.nway-colgrip')) this._fnGrip(e); });
            // the grip sits in the sortable header: its click must not sort
            this.el.addEventListener('click', e => { if (e.target.closest('.nway-colgrip')) e.stopPropagation(); }, true);
            this.el.addEventListener('dblclick', e => {
                if (!e.target.closest('.nway-colgrip')) return;
                this._fw = 0;
                try { localStorage.removeItem('nway.fnw'); } catch (err) { /* not saved */ }
                this.render();
            });
            this.el.addEventListener('change', e => this._onChange(e));
            this.el.addEventListener('input', e => this._onInput(e));
            this._bindCmp(this.el);
            window.Paging.bind(this.el, () => ({ offset: Number(this.state.offset) || 0, total: Number(this.data && this.data.total) || 0, size: PAGE }), off => this._set({ offset: off }));
            this.el.addEventListener('toggle', e => {
                if (e.target.matches('details.nway-focus')) this._focusOpen = e.target.open;
                if (e.target.matches('details.nway-match')) this._matchOpen = e.target.open;
                if (e.target.matches('details.nway-adv')) this._advOpen = e.target.open;
            }, true);
            this.el.addEventListener('contextmenu', e => {
                const file = e.target.closest('[data-nway-file]');
                if (file) return this._menu(e, file.dataset.nwayFile);
                const th = e.target.closest('th[data-col]');
                if (th) return this._menu(e, th.dataset.col);
                const grp = e.target.closest('[data-gid], [data-colx]');
                if (grp) {
                    const gid = grp.dataset.gid || grp.dataset.colx, col = this.data.columns.find(c => c.id === gid);
                    const file = col && this._colFile(col);
                    return file ? this._menu(e, file) : this._groupMenu(e, gid);
                }
                // A right-click on a function cell's empty space opens the function menu, not the plain cell one.
                const fn = !e.target.closest('[data-entity-data], button, a') && e.target.closest('td.col, td[data-fn-open]')?.querySelector('[data-entity-data]');
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
            document.removeEventListener('click', this._ddClose);
            clearTimeout(this._debounce);
            if (this._io) this._io.disconnect();
            if (this._dw && !this.cgInit) window.clearCallsPanel();
            this._cg = null;
        }

        _set(patch) {
            if (Object.keys(patch).some(k => k.startsWith('group'))) { this._gdef = 'none'; this._gflip.clear(); this._rowAuto = true; }
            Object.assign(this.state, patch);
            const s = { ...this.state };
            // Scroll mode keeps the offset out of the URL (and out of the state).
            if (window.Paging.mode() === 'scroll') { this.state.offset = 0; delete s.offset; }
            if (this.onState) this.onState(s);
            this.load();
        }

        // The table request's params, without offset / limit.
        _listQs() {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of API_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            if (!this.state.col_open) qs.set('col_open', JSON.stringify([`*fit:${Math.max(1, Math.floor((window.innerWidth - 120 - (this.data ? this._lead() : DATA_COLS).reduce((a, b) => a + b, 0)) / SQ_COL))}`]));
            if (this.state.group || this.state.group_merge) {
                qs.set('group_open', this._rowAuto ? 'all' : this._gdef);
                if (this._gflip.size) qs.set('group_flip', JSON.stringify([...this._gflip]));
            }
            return qs;
        }

        // `append` (scroll mode): fetch the next batch after the rows already shown.
        async load(append = false) {
            const scroll = window.Paging.mode() === 'scroll';
            if (append && (this._loading || !this.data)) return;
            const gen = ++this._gen;
            this._loading = append;
            // The compare lives in the URL as `cmp`; its saved set (col_set) only caches it.
            if (!append && this.state.cmp && !this.state.col_set && !(await this._resolveCmp())) return;
            if (gen !== this._gen) return;
            const qs = this._listQs();
            // After opening or closing a group, reload as many rows as were on screen.
            qs.set('limit', this.cgInit ? 1 : (this._keep || PAGE));
            this._keep = 0;
            if (scroll) qs.set('offset', append ? this.data.items.length : 0);
            if (!this.data) this.el.innerHTML = '<div class="nway-state"></div>';
            const t0 = Date.now(), usual = NwayPanel._lastMs;
            const pid = Math.random().toString(36).slice(2, 12) + t0.toString(36);
            qs.set('progress_id', pid);
            let prog = null, polling = false;
            const stop = new AbortController();
            // Progress card: server fraction when reported, else elapsed time against the last run's duration.
            const tick = () => {
                let b = this.el.querySelector('.nway-busy');
                if (!b) {
                    b = document.createElement('div');
                    b.innerHTML = '<div class="nway-busy-head"><i class="fa-solid fa-spinner fa-spin"></i> Matching functions<button type="button" class="nway-busy-cancel" title="Stop this computation"><i class="fa-solid fa-xmark"></i> Cancel</button></div><div class="nway-bar-track"><div class="nway-bar-fill"></div></div><div class="nway-busy-foot"><span class="nway-busy-l"></span><span class="nway-busy-r"></span></div>';
                    (this.data ? this.el : this.el.querySelector('.nway-state') || this.el).appendChild(b);
                    b.querySelector('.nway-busy-cancel').onclick = () => {
                        fetch(`/api/bin_sim/nway/cancel?id=${pid}`, { method: 'POST' }).catch(() => { });
                        stop.abort();
                    };
                }
                b.className = 'nway-busy' + (this.data ? '' : ' inline');
                const el = (Date.now() - t0) / 1000, f = prog && prog.fraction > 0 ? prog.fraction : 0;
                const fill = b.querySelector('.nway-bar-fill');
                fill.classList.toggle('indet', !f);
                fill.style.width = f ? `${Math.max(2, Math.round(f * 100))}%` : '';
                const right = [f ? `${Math.round(f * 100)}%` : '', `${Math.round(el)}s`];
                if (f > 0.1 && el > 2) right.push(`~${Math.max(1, Math.round(el * (1 - f) / f))}s left`);
                else if (!f && usual) right.push(`usually ~${Math.max(1, Math.round(usual / 1000))}s`);
                b.querySelector('.nway-busy-l').textContent = f ? prog.label : 'Starting...';
                b.querySelector('.nway-busy-r').textContent = right.filter(Boolean).join(' · ') + ` · times out at ${NWAY_TIMEOUT_MS / 1000}s`;
                if (!polling) {
                    polling = true;
                    fetch(`/api/bin_sim/nway/progress?id=${pid}`).then(r => r.json()).then(p => { if (p.label) prog = p; }).catch(() => { }).finally(() => { polling = false; });
                }
            };
            tick();
            const timer = setInterval(tick, 500);
            const done = () => { clearInterval(timer); this.el.querySelector('.nway-busy')?.remove(); };
            try {
                const res = await fetch(`/api/bin_sim/nway?${qs.toString()}`, { signal: AbortSignal.any([stop.signal, AbortSignal.timeout(NWAY_TIMEOUT_MS)]) });
                const body = await res.json().catch(() => ({}));
                done();
                if (gen !== this._gen) return;
                this._loading = false; this._drawn = gen;
                if (res.ok) NwayPanel._lastMs = Date.now() - t0;
                if (append && res.ok) return this._append(body);
                if (append) return;
                if (!res.ok && body.error === 'col_set_expired' && this.state.cmp && !this._retried) {
                    this._retried = true;
                    this.state.col_set = '';
                    return this.load();
                }
                if (!res.ok) {
                    this.error = { status: res.status, ...body };
                    this.data = null;
                } else {
                    const axes = TAG_AXES.filter(([key]) => Number(body.tag_axis_counts?.[key]) > 0).map(([key]) => key);
                    if ((this.state.group || '').split(',').includes('tag') && axes.length) {
                        const primary = axes.includes(this.state.group_axis || 'origin') ? (this.state.group_axis || 'origin') : axes[0];
                        const secondary = axes.includes(this.state.group_axis_b) && this.state.group_axis_b !== primary ? this.state.group_axis_b : '';
                        if (primary !== this.state.group_axis || secondary !== this.state.group_axis_b) {
                            this._set({ group_axis: primary, group_axis_b: secondary, offset: 0 });
                            return;
                        }
                    }
                    if (this._rowAuto && (this.state.group || this.state.group_merge)) {
                        this._rowAuto = false;
                        // Everything open must fit about one screen of rows, else keep groups closed.
                        if (body.total > Math.floor(window.innerHeight / 30)) { this._gdef = 'none'; return this.load(); }
                        this._gdef = 'all';
                    }
                    this.error = null;
                    this._retried = false;
                    this.data = body;
                    if (this.onData) this.onData(body);
                }
            } catch (e) {
                done();
                if (gen !== this._gen) return;
                this._loading = false; this._drawn = gen;
                if (append) return;
                const timedOut = e.name === 'TimeoutError';
                // Cancelled with a table on screen: keep it.
                if (stop.signal.aborted && this.data) return this.render();
                this.error = { status: 0, message: stop.signal.aborted ? 'Cancelled. Change a setting or reload the page to run it again.' : timedOut ? `Timed out after ${NWAY_TIMEOUT_MS / 1000}s matching functions. Try fewer files, a higher Min edge, or a narrower search.` : String(e) };
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
                const cap = e.caps || {}, overFn = e.functions != null && cap.functions != null && Number(e.functions) > Number(cap.functions);
                const what = overFn ? `Too many functions</b> (${Number(e.functions)}, cap ${Number(cap.functions)} in nway.max_functions)` : `Too many members</b> (${Number(e.members) || '?'}${cap.members != null ? `, cap ${Number(cap.members)} in nway.max_members` : ''})`;
                return `<div class="nway-note warn"><b>${what}: pick fewer files${kids ? ' or open a child cluster:' : '.'}${kids ? `<ul>${kids}</ul>` : ''}</div>`;
            }
            const msg = e.missing ? `Unknown files: ${e.missing.join(', ')}` : (e.message || e.error || `HTTP ${e.status}`);
            return `<div class="nway-note err">${escapeHtml(msg)}</div>`;
        }

        _count(tab) {
            const c = (this.data.counts || {})[tab] || { code: 0, library: 0 };
            const sc = this.state.scope;
            return sc === 'library' ? c.library : sc === 'all' ? c.code + c.library : c.code;
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

        // A selection list in the pill look: the button shows the current choice as an icon pill, the
        // menu lists every choice as one. `options` = [[value, text, icon, color, count?, tip?]]; `attr(v)`
        // is the data attribute an item carries, which _onClick already routes.
        _dd(label, options, cur, tip, attr) {
            const pill = (o, active, extra = '') => `<span class="bsim-tag-pill" ${extra} style="${window.binSimPillStyle(active, o[3])} padding:4px 10px; font-size:0.78rem;"><i class="${escapeAttr(o[2])}"></i>${escapeHtml(o[1])}${o[4] !== undefined ? ` <span class="nway-count">${escapeHtml(String(o[4]))}</span>` : ''}</span>`;
            const now = options.find(o => String(o[0]) === String(cur)) || options[0];
            const items = options.map(o => pill(o, String(o[0]) === String(now[0]), `${attr(o[0])}${o[5] ? ` title="${escapeAttr(o[5])}"` : ''}`)).join('');
            return `<span class="nway-ctlwrap"><span class="nway-lbl">${escapeHtml(label)}</span><details class="nway-ddl"><summary>${pill(now, true)}<i class="fa-solid fa-chevron-down nway-ddc"></i></summary><div class="nway-ddl-menu">${items}</div></details>${this._tip(tip)}</span>`;
        }

        _slider(label, key, value, min, max, step, tip) {
            const shown = INT_RANGES.includes(key) ? String(value) : Number(value).toFixed(2);
            return `<span class="nway-ctlwrap"><label class="nway-ctl"><span class="nway-lbl">${escapeHtml(label)}</span><b>${shown}</b><input type="range" min="${min}" max="${max}"${step ? ` step="${step}"` : ''} value="${Number(value)}" data-range="${key}"></label>${this._tip(tip)}</span>`;
        }

        _group(label, inner) {
            return `<div class="nway-group"><span class="nway-gl">${escapeHtml(label)}</span>${inner}</div>`;
        }

        _controls() {
            const s = this.state, d = this.data;
            const n = d.columns_mode === 'twins' ? (d.file_columns || []).length : d.columns.length;
            const focus = d.columns_mode === 'focus' || this._ab();
            const isNode = !!this.source.cluster_uuid;
            const tabTip = this._ab()
                ? 'All: every function. Core: in both groups. Unique: in one group only. Counts follow the code axis.'
                : focus
                ? 'All: every function. Core: on both the focus and the rest side. Unique: on one side only. Counts follow the code axis.'
                : `All: every function group. Core: present in all ${n} columns. Partial: present in at least k columns. Unique: present in exactly one file. Counts follow the code axis.`;
            const search = `<div class="search-input-wrapper nway-search"><i class="fa-solid fa-magnifying-glass"></i><input type="text" data-text="q" placeholder="Search name, address, tags, cluster..." value="${escapeAttr(s.q)}"></div>`;
            const axisDd = this._dd('Code axis', [['code', 'Code', 'fa-solid fa-code', 'var(--info, #3b82f6)'], ['library', 'Library', 'fa-solid fa-cubes', 'var(--warning, #d97706)'], ['all', 'All', 'fa-solid fa-layer-group', 'var(--success)']], s.scope, 'Code: functions of the program itself. Library: functions recognised as library code (stdlib, statically linked). All: both.', v => `data-set="scope:${v}"`);
            const tabIcons = { all: ['fa-solid fa-layer-group', 'var(--info, #3b82f6)'], core: ['fa-solid fa-bullseye', 'var(--success)'], partial: ['fa-solid fa-circle-half-stroke', 'var(--warning, #d97706)'], unique: ['fa-solid fa-fingerprint', 'var(--token-instruction, #e11d48)'] };
            const tabDd = this._dd('Functions', TABS.filter(([key]) => !(focus && key === 'partial')).map(([key, label]) => [key, label, tabIcons[key][0], tabIcons[key][1], this._count(key)]), s.tab, tabTip, v => `data-tab="${v}"`);
            const cur = PRESETS.find(([, , v]) => String(v.min_edge) === String(s.min_edge) && Number(v.child_presence) === Number(s.child_presence));
            const preset = `<span class="nway-ctlwrap"><div class="view-toggle"><span class="nway-lbl">Match</span>${PRESETS.map(([k, text, , t]) => `<button class="view-btn${cur && cur[0] === k ? ' active' : ''}" data-preset="${k}" title="${escapeAttr(t)}">${text}</button>`).join('')}${cur ? '' : '<span class="dim" style="margin-left:6px">Custom</span>'}</div>${this._tip('Strict, Balanced or Loose set Min edge and Child presence together. Open Advanced to set them by hand.')}</span>`;
            const advanced = `<details class="nway-adv"${this._advOpen ? ' open' : ''}><summary class="nway-lbl">Advanced</summary>${[
                this._slider('Min edge', 'min_edge', Number(d.min_edge), 0, 1, 0.05, 'Ignore function matches below this similarity. Higher is stricter: fewer functions get grouped together.'),
                (isNode || d.shared_cluster) && d.columns_mode === 'children' ? this._slider('Child presence', 'child_presence', s.child_presence, 0.05, 1, 0.05, "Share of a child cluster's files that must hold a function for the child to count as having it.") : '',
            ].join('')}</details>`;
            const match = [
                preset,
                this._seg('Mode', 'mode', [['stored', 'Stored', 'reads built pair docs'], ['virtual', 'Virtual', 'recomputes from vectors']], 'Stored reads the similarity pairs already built. Virtual recomputes the matches from the function vectors, as a fresh collection of just these files would.'),
                d.columns_mode === 'twins' || (s.columns === 'twins' && !focus && d.columns_mode !== 'by') ? this._slider('Column levels', 'twin_depth', Number(s.twin_depth), 1, 6, 1, 'Same code columns: 1 groups only files with identical shared rows. Higher nests them under looser groups of files whose shared rows overlap.') : '',
                (d.columns_mode === 'twins' || (s.columns === 'twins' && d.columns_mode !== 'by')) && Number(s.twin_depth) > 1 && !focus ? this._slider('Column overlap', 'twin_link', Number(s.twin_link), 0, 1, 0.05, 'Outermost column group: files must share at least this much of their rows (average Jaccard) to be grouped.') : '',
                s.tab === 'partial' && !focus ? this._slider(`In at least (of ${n})`, 'k', s.k, 2, Math.max(2, n), 1, 'Partial tab: keep functions present in at least this many columns.') : '',
                advanced,
            ].join('');
            const sc = d.shared_cluster;
            const shared = sc && sc.cluster_uuid ? `<a href="#" class="nway-chip" data-open-shared="${escapeAttr(sc.cluster_uuid)}" title="The smallest stored cluster holding all of these files. Open it as an N-way to keep its cluster context."><i class="fa-solid fa-bullseye"></i> Shared cluster <span class="dim">${escapeHtml(sc.cluster_name || sc.cluster_uuid)} &middot; ${Number(sc.member_count)} files</span></a>` : '';
            const parts = this._groupParts();
            const lv = (s.group || '').split(',').filter(Boolean);
            const moreSum = [`edge ${Number(d.min_edge).toFixed(2)}`, d.mode, lv.length ? lv.join(' > ') : '', d.columns_mode === 'focus' ? 'focus' : ''].filter(Boolean).join(' · ');
            const stats = `<button type="button" class="nway-chip${this._statsOff() ? ' on' : ''}" data-stats-toggle title="Hide or show the Span, Features, Support, Cohesion and Cluster columns, to focus on the matrix."><i class="fa-solid fa-table-columns"></i> Stats <span class="dim">${this._statsOff() ? 'hidden' : 'shown'}</span></button>`;
            const lineage = `<button type="button" class="nway-chip" data-lineage title="Open the lineage of these files as a full page: who inherited what from whom, and code reused outside the tree path."><i class="fa-solid fa-sitemap"></i> Lineage</button>`;
            const sec = (title, body) => `<div class="nway-more-sec"><span class="nway-sech">${title}</span><div class="nway-more-row">${body}</div></div>`;
            const moreBtn = `<button type="button" class="nway-chip${this._moreOpen ? ' on' : ''}" data-more-toggle title="Matching, row groups, focus, stats and the file set"><i class="fa-solid fa-sliders"></i> More options <span class="dim">${escapeHtml(moreSum)}</span> <i class="fa-solid fa-chevron-${this._moreOpen ? 'up' : 'down'}"></i></button>`;
            const more = !this._moreOpen ? '' : `<div class="nway-more-body">
                ${sec('Matching', match)}${sec('Row groups', parts.rows)}${sec('View', `${this._focusBox()}${shared}${stats}`)}${this.onSlot ? sec('File set', '<div data-more-slot></div>') : ''}</div>`;
            return `<div class="nway-tabrow">${search}${this._columnChip()}${moreBtn}</div>
                <div class="nway-bar">${axisDd}${tabDd}${parts.files}<span class="nway-pops" data-actions>${this._compareBox()}${lineage}</span></div>${parts.byRow}${more}`;
        }

        // The group controls in three parts: merge files (always shown), the metadata controls
        // under it, and the row grouping (group by, merge functions, order), which lives under More.
        _groupParts() {
            const d = this.data, s = this.state, lv = (s.group || '').split(',').filter(Boolean);
            const tagAxes = TAG_AXES.filter(([key]) => Number(d.tag_axis_counts?.[key]) > 0).map(([key, label]) => [key, `${label} (${d.tag_axis_counts[key]})`]);
            const axis = tagAxes.some(([key]) => key === s.group_axis) ? s.group_axis : tagAxes[0]?.[0] || '';
            const merge = new Set((s.group_merge || '').split(',').filter(Boolean)), active = merge.size > 0 || lv.length > 0;
            const sel = (key, cur, opts, none) => `<select class="view-btn" data-select="${key}">${none ? '<option value="">none</option>' : ''}${opts.map(([v, t]) => `<option value="${escapeAttr(v)}"${v === cur ? ' selected' : ''}>${escapeHtml(t)}</option>`).join('')}</select>`;
            const box = (k, text, tip) => `<label class="nway-gtog${merge.has(k) ? ' on' : ''}" title="${escapeAttr(tip)}"><input type="checkbox" data-ev="${k}"${merge.has(k) ? ' checked' : ''}> ${text}</label>`;
            const lbl = (text, tip) => `<span class="nway-blbl" title="${escapeAttr(tip)}">${text}</span>`;
            const sep = '<span class="nway-bsep"></span>';
            const twin = s.columns === 'twins', files = (d.file_columns || d.columns).length;
            const by = d.columns_mode === 'by';
            const fileMode = by ? 'by' : twin || d.columns_mode === 'twins' ? 'twins' : d.columns_mode === 'children' ? 'children' : 'files';
            if (by && !this._axes) this._loadAxes().then(() => { if (this.data) { this._scrollSave(); this.render(); } });
            const twinNote = fileMode !== 'twins' ? '' : d.columns_mode === 'twins' ? `${files} → ${d.columns.length}` : 'no twins';
            const picks = GROUPS.map(([v, t, tip], j) => {
                const i = lv.indexOf(v);
                return `${j ? '<span class="nway-bchev">›</span>' : ''}<button class="nway-gpick${i >= 0 ? ' on' : ''}" data-gpick="${v}" aria-pressed="${i >= 0}" title="${escapeAttr(tip)}">${i >= 0 ? `<b>${i + 1}</b>` : ''}${escapeHtml(t)}</button>`;
            }).join('');
            const laneDepth = lv.includes('lane') ? this._slider('Lane levels', 'group_lane_depth', Number(s.group_lane_depth), 1, 6, 1, 'Lanes nest by file set: a lane sits under the smallest lane that holds all its files. 1 keeps every lane flat; higher allows that many nested levels.') : '';
            const laneLink = lv.includes('lane') && Number(s.group_lane_depth) > 1 ? this._slider('Lane overlap', 'group_lane_link', Number(s.group_lane_link), 0, 1, 0.05, 'Sibling lanes whose files overlap by at least this share (Jaccard) get a parent covering both, marked with a chip. 0 turns it off.') : '';
            const laneTier = lv.includes('lane') ? this._slider('Lane tiers', 'group_lane_tier', Number(s.group_lane_tier), 0, 1, 0.05, 'Bands by file count: a lane covering this share of the files is Core, half that share Common, fewer Rare. A sub-lane in a lower band than its parent heads its own band, with a chip naming the parent. 0 turns the bands off.') : '';
            const laneFold = lv.includes('lane') ? this._slider('Fold small lanes', 'group_lane_fold', Number(s.group_lane_fold), 0, 10, 1, 'Sibling lanes holding fewer rows than this are folded into one closed group, so a lane of two functions does not cost a header each. 0 turns the fold off; it needs two such lanes.') : '';
            const laneNames = lv.includes('lane') && d.llm_names !== false ? `<span class="nway-ctlwrap"><label class="nway-gtog${s.lane_names !== 'off' ? ' on' : ''}" title="Ask the language model to name each lane by what its functions have in common (needs the LLM set in the config; names are cached)."><input type="checkbox" data-lnames${s.lane_names !== 'off' ? ' checked' : ''}> Lane names</label></span>` : '';
            const axes = lv.includes('tag') ? tagAxes.length ? `${sel('group_axis', axis, tagAxes)}<span class="nway-lbl">&times;</span>${sel('group_axis_b', s.group_axis_b, tagAxes.filter(([k]) => k !== axis), true)}` : '<span class="nway-lbl">No tags in these functions</span>' : '';
            const minLink = merge.has('call') ? this._slider('Min link', 'group_min_link', s.group_min_link === '' ? 0.3 : Number(s.group_min_link), 0, 0.9, 0.05, 'Call position only joins rows when an edge between them scores at least this. 0 joins on a shared caller alone. Edges below Min edge only exist when Min edge is above the pair floor.') : '';
            // Merge files by: None = one column per file, Cluster = child clusters (a cluster node only), Same code = twins.
            const fileOpts = [['files', 'None', 'One column per file.'], ...(this.source.cluster_uuid || d.shared_cluster ? [['children', 'Cluster', 'One column per child cluster; open a column to show its own children, down to files.']] : []), ['twins', 'Same code', "Files that hold a function in exactly the same shared rows become one column. Click a column's count to show its files; a function only some of them hold shows as a partly filled square."], ['by', 'Metadata', 'One column per value of a file field or tag namespace (collection, batch, architecture, a tag family...). Click a column to show its files.']];
            const fileColors = { files: 'var(--dim, #888)', children: 'var(--info, #3b82f6)', twins: 'var(--success)', by: 'var(--warning, #d97706)' };
            const fileDd = this._dd('Merge files', fileOpts.map(([v, text, tip]) => [v, text, FILE_ICONS[v], fileColors[v], undefined, tip]), fileMode, 'How files share a column.', v => (v === 'by' ? 'data-pick-by' : `data-set="columns:${v}"`));
            const axisOpts = (this._axes || []).map(a => [a.key, a.values ? `${a.key} (${a.values})` : a.key]);
            if (s.col_by && !axisOpts.some(([k]) => k === s.col_by)) axisOpts.push([s.col_by, s.col_by]);
            const entry = (this._axes || []).find(a => a.key === s.col_by), picked = this._pickList();
            const valueChips = !by || s.col_set || !entry || !entry.all ? '' : `<span class="nway-bvals" title="Click values to make them the only columns, in click order">${entry.all.map(([v, n]) => `<button type="button" class="nway-chip${picked.includes(v) ? ' on' : ''}" data-colpick="${escapeAttr(v)}">${escapeHtml(v)} <span class="dim">${Number(n)}</span></button>`).join('')}${entry.values > entry.all.length ? `<span class="dim">+${entry.values - entry.all.length} more</span>` : ''}</span>`;
            const pickCtl = picked.length ? `${this._seg('Others', 'col_rest', [['', 'Merged'], ['drop', 'Hidden']], 'The files whose value was not picked: merged into one "other" column, or left out of the table.')}<button class="nway-blink" data-colpick-clear>clear picks</button>` : '';
            const abCtl = by && !s.col_set && this._ab() && s.tab === 'unique' ? this._seg('Unique to', 'side', [['', 'Both'], ['a', d.columns[0].label], ['b', d.columns[1].label]], 'Unique tab only: functions in one column and not the other.') : '';
            const byCtl = !by ? '' : s.col_set ? '<span class="dim nway-bnote">compared queries</span><button class="nway-blink" data-cmp-clear>clear</button>' : `${sel('col_by', s.col_by, axisOpts)}${s.col_by.startsWith('tag:') ? sel('col_by_multi', s.col_by_multi || 'first', [['first', 'Most shared tag'], ['all', 'Every tag']]) : ''}${this._seg('Holds when', 'col_presence', [['', 'Half'], ['any', 'Any'], ['all', 'All']], 'A column holds a function when this share of its files do: any one, half (default) or all.')}${pickCtl}${abCtl}`;
            return {
                files: `${fileDd}${twinNote ? `<span class="dim nway-bnote">${twinNote}</span>` : ''}`,
                byRow: by || this._byErr ? `<div class="nway-byrow">${byCtl}${valueChips}${this._byErr ? `<span class="dim nway-bnote">${escapeHtml(this._byErr)}</span>` : ''}</div>` : '',
                rows: `<div class="nway-gcard${active ? ' active' : ''}">
                <span class="nway-bgrp">${lbl('Group by', 'Click order is nesting order: each group is split again by the next level. The column sort still orders the rows inside each group.')}${picks}${axes}${laneDepth}${laneLink}${laneTier}${laneFold}${laneNames}</span>${sep}
                <span class="nway-bgrp">${lbl('Merge functions', 'Join rows that are probably one function: rows the matcher kept apart (another decompilation, architecture or version) show as one, with their rows under it.')}${box('name', 'Name', 'Rows with the same function name. Auto names (FUN_, sub_) and compiler suffixes (.part.0) are ignored.')}${box('call', 'Call position', 'Rows under the same caller, in different files, with a weak edge between them. Needs the callers of every function.')}${minLink}</span>${sep}
                <span class="nway-bgrp nway-bend">${lbl('Order', 'How groups are ordered.')}${sel('group_sort', s.group_sort || 'files', GROUP_SORTS)}${active ? '<button class="nway-blink" data-gall="open">open all</button><button class="nway-blink" data-gall="close">close all</button>' : ''}</span>
            </div>`,
            };
        }

        // Depth dots over the column outline: 0 folds everything, n opens n levels, the last one opens all.
        _colDepth() {
            const all = this._colOpen().has('*');
            const cur = all ? Infinity : Math.max(0, ...this.data.columns.map(c => (c.path || []).length));
            // Dots follow the tree: up to its depth, rounded up to a step of 5; "+5" adds another step (and opens it).
            const lim = this._dlim = Math.max(this._dlim || 5, 5 * Math.ceil((Number.isFinite(cur) ? cur : 0) / 5));
            const dot = n => `<button type="button" class="nway-ddot${n <= cur ? ' on' : ''}" data-coldepth="${n}" aria-label="${n ? `Open ${n} level${n === 1 ? '' : 's'}` : 'Fold all'}" title="${n ? `Open ${n} level${n === 1 ? '' : 's'}` : 'Fold all'}"></button>`;
            return `<div class="nway-depth"><span>Depth</span>${[...Array(lim + 1).keys()].map(dot).join('<i></i>')}<button type="button" class="nway-dall" data-coldepth="${lim + 5}" title="Open ${lim + 5} levels">+5</button><button type="button" class="nway-dall${all ? ' on' : ''}" data-coldepth="all" aria-label="Open all levels" title="Open all levels">&infin;</button></div>`;
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

        // Two top-level merged columns compare like Focus and Rest: Core = both, Unique = one group.
        _ab() {
            const d = this.data;
            return !!d && d.columns_mode === 'by' && d.columns.length === 2 && !d.columns.some(c => (c.path || []).length);
        }

        // Keys the files can be merged on, counted by the server (once per file set).
        _loadAxes() {
            const ids = this._files().map(f => f.id), key = ids.join(',');
            if (this._axesFor !== key) {
                this._axesFor = key;
                const fallback = () => BY_KEYS.map(k => ({ key: k }));
                this._axesP = fetch('/api/bin_sim/nway/column_axes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ md5s: ids }) })
                    .then(r => (r && r.ok ? r.json() : null)).then(b => { this._axes = b && b.axes ? b.axes : fallback(); }).catch(() => { this._axes = fallback(); });
            }
            return this._axesP;
        }

        async _pickBy() {
            await this._loadAxes();
            const first = (this._axes || [])[0];
            this._byErr = first ? '' : 'No field splits these files into groups: each value is either shared by all or unique to one.';
            if (!first) return this.render();
            this._pin();
            this._set({ columns: 'by', col_by: first.key, col_set: '', col_open: '', offset: 0 });
        }

        // "tag=family:mirai&file_name=bot" -> search_files params; text without '=' is a plain search.
        _parseQuery(text) {
            const t = text.trim(), out = {};
            if (!t) return out;
            if (!t.includes('=')) return { q: t };
            for (const [k, v] of new URLSearchParams(t)) (out[k] ||= []).push(v);
            for (const k of Object.keys(out)) if (out[k].length === 1) out[k] = out[k][0];
            return out;
        }

        // A compare's files only live in its saved col_set (1 h); before leaving it, make them the source set so any grouping can follow.
        _pin() {
            if (!this.state.col_set || !this.data) return;
            const ids = this._files().map(f => f.id).join(',');
            delete this.source.batch_uuid; delete this.source.cluster_uuid;
            this.source.md5s = ids;
            if (this.onSource) this.onSource(ids);
        }

        _pickList() {
            try {
                const l = JSON.parse(this.state.col_pick || '[]');
                return Array.isArray(l) ? l.map(String) : [];
            } catch (e) { return []; }
        }

        _cmpState() {
            try {
                const c = JSON.parse(this.state.cmp || 'null');
                return c && Array.isArray(c.groups) ? c : null;
            } catch (e) { return null; }
        }

        // The files the panel was opened on, as `<collection>:<md5>`.
        _sourceIds() {
            return String(this.source.md5s || '').split(',').map(t => t.trim()).filter(Boolean)
                .map(t => (t.includes(':') ? t : `${this.source.collection || ''}:${t}`));
        }

        // Save the compare's groups on the server: {id, files, overlap, groups}; `preview` only counts.
        async _postCmp(cmp, preview = false) {
            const body = { groups: cmp.groups.map(g => ({ label: g.label, queries: g.queries.map(q => this._parseQuery(q)) })) };
            if (this.source.pool) body.pool = this.source.pool;
            else body.collection = this.source.collection || ((this._sourceIds()[0] || '').split(':')[0]) || (this.data ? (this._files()[0] || {}).collection : '');
            const within = this._sourceIds();
            if (cmp.within && within.length) body.within = within;
            if (preview) body.preview = true;
            const res = await fetch('/api/bin_sim/nway/columns', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            const out = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(out.message || out.error || `HTTP ${res.status}`);
            return out;
        }

        async _resolveCmp() {
            try {
                const out = await this._postCmp(this._cmpState());
                this._cmpInfo = out.groups;
                this.state.col_set = out.id;
                if (this.onState) this.onState({ ...this.state });
                return true;
            } catch (err) {
                this.error = { status: 400, message: String(err.message || err) };
                this.data = null;
                this.render();
                return false;
            }
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

        // The editable form of the compare: one object of filters per search, as typed.
        _draftInit() {
            const saved = this._cmpState();
            const groups = (saved && saved.groups.length ? saved.groups : [{ label: 'A', queries: [] }, { label: 'B', queries: [] }])
                .map(g => ({ label: g.label, queries: (g.queries.length ? g.queries : ['']).map(q => this._qsToObj(q)) }));
            return { groups, within: saved && saved.within ? 1 : 0 };
        }

        // The draft in its URL form: {groups: [{label, queries: ["k=v&..."]}], within}.
        _draftCmp() {
            const d = this._draft;
            return { groups: d.groups.map((g, i) => ({ label: g.label.trim() || letter(i), queries: g.queries.map(q => this._objToQs(q)).filter(Boolean) })), within: d.within };
        }

        _cfSet(path, value) {
            const [gi, qi, key] = path.split('.'), g = this._draft.groups[Number(gi)];
            if (qi === 'label') { g.label = value; return; }
            const q = g.queries[Number(qi)];
            if (key === '_raw') {
                for (const k of Object.keys(q)) if (!FILE_KNOWN.has(k)) delete q[k];
                Object.assign(q, this._qsToObj(value));
            } else q[key] = value;
        }

        // Counts under each group while typing: a preview call, nothing saved.
        _cmpPreviewSoon() {
            clearTimeout(this._cmpTimer);
            this._cmpTimer = setTimeout(async () => {
                const cmp = this._draftCmp(), msg = document.querySelector('[data-cmp-msg]');
                try {
                    const out = await this._postCmp({ ...cmp, groups: cmp.groups.map(g => ({ ...g, queries: g.queries })) }, true);
                    this._cmpCounts = out;
                    this._cmpRefreshResult();
                    if (msg) msg.hidden = true;
                } catch (err) {
                    if (msg) { msg.textContent = String(err.message || err); msg.hidden = false; }
                }
            }, 450);
        }

        async _runCompare() {
            const cmp = this._draftCmp();
            this._cmpErr = '';
            const empty = cmp.groups.findIndex(g => !g.queries.length);
            if (empty >= 0) {
                this._cmpErr = `Give group ${letter(empty)} at least one filter.`;
                return this.render();
            }
            try {
                const out = await this._postCmp(cmp);
                this._cmpInfo = out.groups;
                this._scrollSave();
                this._set({ cmp: JSON.stringify(cmp), col_set: out.id, col_by: '', col_open: '', side: '', sort_col: '', tab: this.state.tab === 'partial' ? 'core' : this.state.tab, offset: 0 });
            } catch (err) {
                this._cmpErr = String(err.message || err);
                this.render();
            }
        }

        // Compare: file searches become columns; with two groups Core = in both, Unique = in one.
        _compareBox() {
            const s = this.state, on = !!s.col_set;
            this._draft = this._draft || this._draftInit();
            const tip = 'Compare file searches. Each group takes the same filters as the Files page; Core is in every group, Unique in one.';
            return `<details class="nway-compare nway-pop"${this._cmpOpen ? ' open' : ''}><summary class="nway-chip${on ? ' on' : ''}" title="${escapeAttr(tip)}"><i class="fa-solid fa-code-compare"></i> Compare <span class="dim">${on ? `${(this._cmpState() || { groups: [] }).groups.length || 2} groups` : 'off'}</span></summary>
                <div class="nway-focus-body nway-match-body nway-cmp-body">${this._cmpBodyHtml()}</div></details>`;
        }

        _cmpBodyHtml() {
            const s = this.state, on = !!s.col_set, ab = this._ab(), dr = this._draft;
            const counts = this._cmpCounts || (this._cmpInfo ? { groups: this._cmpInfo } : null);
            const groups = dr.groups.map((g, gi) => {
                const qs = g.queries.map((q, qi) => {
                    const key = `${gi}.${qi}`;
                    const chips = this._qChips(q).map(c => `<span class="nway-tok"><b>${escapeHtml(FILE_LABELS[c.k] || c.k)}</b> ${escapeHtml(c.v)}<button type="button" data-tok-rm="${key}" data-k="${escapeAttr(c.k)}" data-v="${escapeAttr(c.v)}" aria-label="Remove filter">&times;</button></span>`).join('');
                    return `<div class="nway-cq">${qi ? '<span class="dim nway-cor">or</span>' : ''}<div class="nway-toks">${chips}<div class="nway-tokin"><input type="text" data-tok="${key}" placeholder="${chips ? 'add another filter' : 'tag, file name, batch, language... or key=value'}" autocomplete="off" spellcheck="false"><div class="nway-sug" hidden onmousedown="event.preventDefault()"></div></div></div>
                        ${g.queries.length > 1 ? `<button type="button" class="nway-blink" data-cmp-rmq="${key}" title="Remove this search">&times; remove search</button>` : ''}</div>`;
                }).join('');
                return `<div class="nway-cg nway-cg-${gi % 2 ? 'b' : 'a'}"><div class="nway-cgh"><b class="nway-cgn">${letter(gi)}</b><input type="text" class="nway-cmp-label" data-cf="${gi}.label" value="${escapeAttr(g.label)}" maxlength="30" aria-label="Name of group ${letter(gi)}"><span class="dim" data-cnt="${gi}">${this._cmpCount(counts, gi, g)}</span>${dr.groups.length > 2 ? `<button type="button" class="nway-blink" data-cmp-rmg="${gi}" title="Remove group ${letter(gi)}">&times; remove group</button>` : ''}</div>${qs}<button type="button" class="nway-blink" data-cmp-addq="${gi}" title="Files matching this search or the ones above count in group ${letter(gi)}">+ or another search</button></div>`;
            }).join('');
            const ids = this._sourceIds();
            const scope = ids.length ? `<div class="nway-cmp-scope"><span class="nway-lbl">Search in</span><span class="view-toggle"><button type="button" class="view-btn${dr.within ? '' : ' active'}" data-cmp-within="0">the whole collection</button><button type="button" class="view-btn${dr.within ? ' active' : ''}" data-cmp-within="1">only the ${ids.length} files open</button></span></div>` : '';
            const ctl = ab ? [
                this._seg('Holds when', 'col_presence', [['', 'Half'], ['any', 'Any'], ['all', 'All']], 'A group holds a function when this share of its files do: any one, half (default) or all.'),
                s.tab === 'unique' ? this._seg('Unique to', 'side', [['', 'Both'], ['a', dr.groups[0].label || 'A'], ['b', dr.groups[1].label || 'B']], 'Unique tab only: functions in one group and not the other.') : '',
            ].join('') : '';
            return `${scope}<div data-cmp-bar>${this._cmpBar()}</div>
                <div class="nway-cmp-split"><div class="nway-cmp-left">${groups}</div><div class="nway-cmp-right" data-cmp-right>${this._cmpRight()}</div></div>
                <div class="nway-chips">${dr.groups.length < GROUP_MAX ? '<button type="button" class="nway-chip" data-cmp-addg>+ add group</button>' : ''}<button type="button" class="nway-chip on" data-cmp-run>Compare${counts && counts.files ? ` ${Number(counts.files)} files` : ''}</button>${on ? '<button type="button" class="nway-chip" data-cmp-clear>&times; clear</button>' : ''}</div>
                <div class="nway-note warn" data-cmp-msg${this._cmpErr ? '' : ' hidden'}>${escapeHtml(this._cmpErr || '')}</div>${ctl}`;
        }

        _cmpCount(counts, gi, g) {
            if (counts && counts.groups[gi] && g.queries.some(q => this._qChips(q).length)) return fileCount(counts.groups[gi].files);
            return '';
        }

        // Overlap bar for two groups: A only, both, B only.
        _cmpBar() {
            const c = this._cmpCounts;
            if (!c || c.groups.length !== 2) return '';
            const [a, b] = c.groups.map(g => Number(g.files)), both = Number(c.overlap) || 0, tot = Math.max(1, a + b - both);
            const w = n => (n / tot * 100).toFixed(1);
            const warn = both && both >= tot / 2 ? `<span class="nway-cmp-warn">${both} files match both groups: the filters barely differ.</span>` : '';
            return `<div class="nway-cmp-bar" title="A only / in both / B only"><i class="ga" style="width:${w(a - both)}%"></i><i class="gab" style="width:${w(both)}%"></i><i class="gb" style="width:${w(b - both)}%"></i></div>
                <div class="nway-cmp-leg"><span class="ga">${letter(0)} only ${a - both}</span><span class="gab">both ${both}</span><span class="gb">${letter(1)} only ${b - both}</span>${warn}</div>`;
        }

        // The files the filters match now, tagged with the groups that hold each.
        _cmpRight() {
            const c = this._cmpCounts, dr = this._draft;
            if (!c || !c.sample) return '<div class="dim nway-cmp-empty">Add a filter to a group to see the files it matches.</div>';
            const show = this._cmpShow || 'all';
            const opts = [['all', 'All']].concat(dr.groups.map((g, i) => [String(i), letter(i)]), dr.groups.length > 1 ? [['both', 'In several']] : []);
            const rows = c.sample.filter(f => show === 'all' || (show === 'both' ? f.groups.length > 1 : f.groups.includes(Number(show))));
            const mk = f => f.groups.map(i => `<b class="nway-mk nway-mk-${i % 2 ? 'b' : 'a'}">${letter(i)}</b>`).join('');
            const body = rows.map(f => `<tr><td>${mk(f)}</td><td class="nway-cmp-fn">${escapeHtml(f.name)}</td><td class="dim">${escapeHtml(f.language)}</td><td>${f.tags.slice(0, 3).map(t => `<span class="nway-cmp-tag">${escapeHtml(t)}</span>`).join(' ')}</td></tr>`).join('');
            const cut = c.files > c.sample.length ? `<div class="dim">Showing ${c.sample.length} of ${Number(c.files)} files.</div>` : '';
            return `<div class="nway-cmp-tabs">${opts.map(([v, t]) => `<button type="button" class="nway-chip${show === v ? ' on' : ''}" data-cmp-show="${v}">${escapeHtml(t)}</button>`).join('')}</div>
                <div class="nway-cmp-files">${rows.length ? `<table><tr><th></th><th>File</th><th>Language</th><th>Tags</th></tr>${body}</table>` : '<div class="dim nway-cmp-empty">No file in this view.</div>'}</div>${cut}`;
        }

        // Re-draw the compare form in place; the popover stays open and filters keep their state.
        _cmpRefresh() {
            const body = document.querySelector('.nway-cmp-body');
            if (body) body.innerHTML = this._cmpBodyHtml();
        }

        // Counts, bar and file list only: the filter boxes keep focus while the preview answers.
        _cmpRefreshResult() {
            const bar = document.querySelector('[data-cmp-bar]'), right = document.querySelector('[data-cmp-right]');
            if (bar) bar.innerHTML = this._cmpBar();
            if (right) right.innerHTML = this._cmpRight();
            const counts = this._cmpCounts;
            this._draft.groups.forEach((g, i) => { const el = document.querySelector(`[data-cnt="${i}"]`); if (el) el.textContent = this._cmpCount(counts, i, g); });
            const run = document.querySelector('[data-cmp-run]');
            if (run && counts) run.textContent = `Compare${counts.files ? ` ${Number(counts.files)} files` : ''}`;
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
            const [gi, qi] = path.split('.').map(Number), q = this._draft.groups[gi].queries[qi];
            if (FILE_MULTI.has(k)) {
                const list = (q[k] || '').split(',').map(x => x.trim()).filter(Boolean);
                if (!list.includes(v)) list.push(v);
                q[k] = list.join(',');
            } else q[k] = v;
            this._sug = null;
            this._cmpRefresh();
            const inp = document.querySelector(`[data-tok="${path}"]`);
            if (inp) inp.focus();
            this._cmpPreviewSoon();
        }

        _tokRemove(path, k, v) {
            const [gi, qi] = path.split('.').map(Number), q = this._draft.groups[gi].queries[qi];
            if (FILE_MULTI.has(k)) {
                const list = (q[k] || '').split(',').map(x => x.trim()).filter(x => x && x !== v);
                if (list.length) q[k] = list.join(','); else delete q[k];
            } else delete q[k];
            this._cmpRefresh();
            const inp = document.querySelector(`[data-tok="${path}"]`);
            if (inp) inp.focus();
            this._cmpPreviewSoon();
        }

        _cmpCollection() {
            return this.source.collection || ((this._sourceIds()[0] || '').split(':')[0]) || (this.data ? (this._files()[0] || {}).collection : '');
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
            const coll = this._cmpCollection();
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
            this._sug = { path, items, idx: 0 };
            this._tokSugDraw(box);
        }

        _tokSugDraw(box) {
            const sug = this._sug;
            if (!box) return;
            box.hidden = !sug || !sug.items.length;
            if (box.hidden) return;
            box.innerHTML = sug.items.map((x, i) => `<div class="nway-sug-i${i === sug.idx ? ' on' : ''}" data-sug-i="${i}"><b>${escapeHtml(FILE_LABELS[x.k] || x.k)}</b> <span class="nway-sug-v">${escapeHtml(x.v)}</span><small>${x.n != null ? fileCount(x.n) : escapeHtml(x.hint || '')}</small></div>`).join('');
        }

        _tokKey(e) {
            const t = e.target;
            if (!t.dataset || !t.dataset.tok) return false;
            const sug = this._sug && this._sug.path === t.dataset.tok ? this._sug : null, box = t.parentElement.querySelector('.nway-sug');
            if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                if (!sug) return this._tokSuggest(t), true;
                sug.idx = (sug.idx + (e.key === 'ArrowDown' ? 1 : sug.items.length - 1)) % sug.items.length;
                this._tokSugDraw(box);
                e.preventDefault();
            } else if (e.key === 'Enter') {
                const x = sug && sug.items[sug.idx];
                if (x) this._tokAdd(t.dataset.tok, x.k, x.v);
                else if (!t.value.trim()) this._runCompare();
                e.preventDefault();
            } else if (e.key === 'Escape') {
                this._sug = null;
                if (box) box.hidden = true;
            } else if (e.key === 'Backspace' && !t.value) {
                const [gi, qi] = t.dataset.tok.split('.').map(Number), chips = this._qChips(this._draft.groups[gi].queries[qi]);
                if (chips.length) this._tokRemove(t.dataset.tok, chips[chips.length - 1].k, chips[chips.length - 1].v);
            } else return false;
            return true;
        }

        // keydown / focus / toggle do not reach the actions once the host moves them out of this.el
        _bindCmp(node) {
            node.addEventListener('keydown', e => { if (!this._tokKey(e) && e.key === 'Enter' && e.target.dataset && e.target.dataset.cf) this._runCompare(); });
            node.addEventListener('focusin', e => { if (e.target.dataset && e.target.dataset.tok) this._tokSuggest(e.target); });
            node.addEventListener('focusout', e => { if (e.target.dataset && e.target.dataset.tok) { const b = e.target.parentElement.querySelector('.nway-sug'); this._sug = null; if (b) b.hidden = true; } });
            node.addEventListener('toggle', e => {
                if (e.target.matches('details.nway-focus')) this._focusOpen = e.target.open;
                if (e.target.matches('details.nway-compare')) {
                    this._cmpOpen = e.target.open;
                    if (e.target.open && !this._cmpCounts) this._cmpPreviewSoon();
                }
            }, true);
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

        // Right-click on a column group (band, outline row or numbered group head): copy / fold / focus its files.
        // The file a column stands for: itself, or the single file of an expanded twin / cluster leaf.
        _colFile(c) {
            return c.kind === 'file' ? c.members[0] : c.kind ? '' : c.id;
        }

        // The N-way tab of a stored cluster, for this panel's pool or collection.
        _clusterUrl(uuid) {
            const pool = this.source.pool, out = new URLSearchParams({ tab: 'functions' });
            for (const k of ['axis', 'scope']) if (this.state[k]) out.set(k, this.state[k]);
            const base = pool ? `/pools/${encodeURIComponent(pool)}/files/clusters/${encodeURIComponent(uuid)}` : window.Nav.buildUIUrl((this._files()[0] || {}).collection, ['files', 'clusters', uuid]);
            return `${base}?${out.toString()}`;
        }

        _groupIds(id) {
            const ids = new Set();
            let label = id;
            for (const c of this.data.columns) {
                const g = (c.path || []).find(x => x.id === id);
                if (c.id === id && c.kind) label = c.label;
                if (g) label = g.label;
                if (g || c.id === id) (c.kind ? c.members || [] : [c.id]).forEach(m => ids.add(m));
            }
            return { ids, label };
        }

        _groupMenu(e, id) {
            e.preventDefault();
            e.stopPropagation();
            const { ids, label } = this._groupIds(id);
            const files = this._files().filter(f => ids.has(f.id));
            const n = this._tree().byId.get(id);
            const items = [
                ['fa-copy', 'Copy name', () => copyToClipboard(label)],
                ['fa-copy', `Copy ${files.length} md5s`, () => copyToClipboard(files.map(f => f.md5).join('\n'))],
                ['fa-copy', 'Copy file names', () => copyToClipboard(files.map(f => f.file_name || f.md5).join('\n'))],
                ...(n && (n.kids.length || n.expandable) ? [[n.kids.length ? 'fa-compress' : 'fa-expand', n.kids.length ? 'Fold' : 'Show files', () => this._colToggle(id)]] : []),
                ['fa-bullseye', 'Focus on these files', () => this._setFocus(files.map(f => f.id), this._files().length)],
                ['fa-arrow-up-right-from-square', 'Open as its own N-way', () => {
                    // A child cluster keeps its cluster context; any other group is a plain file set.
                    const col = this.data.columns.find(c => c.id === id && c.cluster_uuid);
                    window.Nav.openPath(col ? this._clusterUrl(col.cluster_uuid) : NwayPanel.urlFor(files.map(f => f.id)), e, { title: label, type: 'nway' });
                }],
            ].filter(it => files.length || /^(Copy name|Fold|Show files)$/.test(it[1]));
            document.getElementById('nway-gmenu')?.remove();
            const m = document.createElement('div');
            m.id = 'nway-gmenu';
            m.className = 'context-menu';
            m.style.cssText = `display:block;position:fixed;left:${Math.min(e.clientX, innerWidth - 220)}px;top:${Math.min(e.clientY, innerHeight - 40 * items.length)}px;z-index:20010`;
            m.innerHTML = items.map((it, i) => `<div class="context-menu-item" data-i="${i}"><i class="fa-solid ${it[0]}" style="width:16px;text-align:center;opacity:.8"></i><span>${escapeHtml(it[1])}</span></div>`).join('');
            const close = () => { m.remove(); document.removeEventListener('click', close, true); };
            m.addEventListener('click', ev => { const i = ev.target.closest('[data-i]'); close(); if (i) items[i.dataset.i][2](); });
            document.body.appendChild(m);
            setTimeout(() => document.addEventListener('click', close, true));
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

        // Presence squares: one colour per file column, in the header, the rows and the group headers.
        _sqStyle(i) {
            const t = this._tree();
            if (t.grouped) { const c = t.colors[i]; return `--h:${c.h};--l:${c.l}%;--ll:${c.l - 8}%`; }
            return this.data.columns.length <= 16 ? `--h:${this._fileHue(i)}` : '';
        }

        // "Not in: ..." for a unit that joined a lane while missing a file or two of it (`miss` is a 0/1 string over the columns).
        _missChip(sub, miss) {
            if (!sub) return '';
            const names = [...miss].map((b, i) => (b === '1' ? this._colLabel(this.data.columns[i]) : '')).filter(Boolean).join(', ');
            return `<span class="nway-gchip sub" title="${escapeAttr(`Not in: ${names}. Its other files are all in this bundle, so it joined it.`)}">&minus;${Number(sub)} file${sub > 1 ? 's' : ''}</span>`;
        }

        _colLabel(c) {
            return c ? c.file_name || c.label || c.md5 || '' : '';
        }

        // A square standing for one function: the same data, hover preview, click and right-click menu
        // as the function entity. The address and signature are in the hover.
        _fnEntity(fid, col, ci) {
            const f = this._fnData(fid, col);
            const sig = typeof formatSigComponent === 'function'
                ? formatSigComponent(f.namespace, f.return_type, f.function_name, f.parameters).fullSig : f.function_name;
            const tip = `@ ${f.entrypoint_address}\n${sig}\n${this._colLabel(col)}`;
            return `<span class="entity-function" data-etype="function" data-eid="${escapeAttr(fid)}"
                    data-entity-data='${escapeAttr(JSON.stringify(f))}'
                    oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>
                <i class="nway-sq on" style="${this._sqStyle(ci)}" title="${escapeAttr(tip)}"
                   onmouseenter="typeof showCodePreview === 'function' && showCodePreview(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, ${escapeAttr(jsString(f.entrypoint_address))}, ${escapeAttr(jsString(col.md5))}, ${Number(f.bsim_features_count) || 0}, event)"
                   onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)"
                   onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"
                   onclick="typeof showFunctionCodeById === 'function' && showFunctionCodeById(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, '', event)"></i></span>`;
        }

        // A child/direct column: a square that shows how many of the group's files hold the function.
        // It opens the first of those functions; the hover lists the addresses.
        _groupCell(row, col, ci) {
            const cell = row.cells[col.id];
            const pool = this.source.pool || null;
            const hits = [];
            for (const f of this.data.file_columns || []) {
                const fid = col.members.includes(f.id) && (row.files || {})[f.id];
                if (fid) hits.push({ f, fid, data: this._fnData(fid, f) });
            }
            // Filled when every file of the column holds the function, tinted with the count when only some do.
            // Hovering shows the function preview of the first one, and the others in its "other functions" card.
            const list = hits.map(h => ({ fid: h.fid, name: h.data.function_name, addr: h.data.entrypoint_address, md5: h.f.md5, feat: h.data.bsim_features_count, file: h.f.file_name || '' }));
            const hover = hits.length ? ` data-gfns='${escapeAttr(JSON.stringify(list))}' onmouseenter="NwayPanel.hoverGroup(this, event)" onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)" onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"` : ` title="${escapeAttr(`${col.label || ''}: no function in this row`)}"`;
            const sq = `<i class="nway-sq${cell.present ? (cell.present === cell.total ? ' on' : ' part') : ''}" style="${this._sqStyle(ci)}"${hover}>${cell.total > 1 && cell.present ? Number(cell.present) : ''}</i>`;
            if (!hits.length) return `<td class="col sq">${sq}</td>`;
            const { f, fid, data } = hits[0];
            return `<td class="col sq"><a data-nav="${escapeAttr(fnUrl(f, fid, pool))}" data-title="${escapeAttr(f.file_name)}"
                data-etype="function" data-eid="${escapeAttr(fid)}" data-entity-data='${escapeAttr(JSON.stringify(data))}'
                oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>${sq}</a></td>`;
        }

        _cell(row, col, ci) {
            if (col.kind) return this._groupCell(row, col, ci);
            const fid = row.cells[col.id];
            if (!fid) return `<td class="col sq"><i class="nway-sq" title="${escapeAttr(`${this._colLabel(col)}: no function in this row`)}"></i></td>`;
            return `<td class="col sq">${this._fnEntity(fid, col, ci)}</td>`;
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
            if (String(row.name).startsWith(LANE_MARK)) return null;
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
            if (String(row.name).startsWith(LANE_MARK)) return '';
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

        // The coloured return type + name, drawn by the same renderer as the function search table.
        _fnSig(row, opts = {}) {
            const sig = this._rowSig(row);
            if (!sig) return escapeHtml(row.name);
            const f = this._fnData(sig.best.fid, sig.best.col);
            return window.EntityRenderer.renderFunction({ ...f, user_name: '', function_name: row.name, return_type: sig.ret, parameters: sig.params }, { hideNote: true, showActions: false, ...opts });
        }

        _nameSig(row, opts = {}) {
            const sig = this._rowSig(row);
            const name = escapeHtml(row.name);
            if (!sig) return `<div class="nway-namerow"><span>${name}</span>${this._renameBtn(row)}</div>`;
            const f = this._fnData(sig.best.fid, sig.best.col);
            const fn = this._fnSig(row, opts);
            // Tags land on the best candidate, the function the preview shows.
            // Pooled row tags carry bare user tags as `user:<tag>` (merge_tag_fields); unwrap them so bookmark/ignore still light up.
            const bareUser = t => /^user:[^:]+$/.test(t);
            const tags = window.EntityRenderer.renderTag('function', f.function_id,
                row.tags ? row.tags.filter(t => !bareUser(t)) : f.tags,
                row.tags ? row.tags.filter(bareUser).map(t => t.slice(5)) : f.user_tags,
                { maxTags: 4 });
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
            this._dockRow = { row, cells };
            this._dockFid = null;
            if (!this._dockEl) {
                this._dockEl = document.createElement('aside');
                this._dockEl.className = 'nway-dock';
                this._dockEl.addEventListener('click', e => this._onDockClick(e));
                // drag the left edge to resize, like the columns tree
                this._dockEl.addEventListener('pointerdown', e => {
                    if (!e.target.closest('.nway-dock-grip')) return;
                    e.preventDefault();
                    const el = this._dockEl, x0 = e.clientX, w0 = el.offsetWidth;
                    const move = ev => { el.style.width = `${Math.max(280, w0 + x0 - ev.clientX)}px`; };
                    const up = () => { document.removeEventListener('pointermove', move); document.removeEventListener('pointerup', up); };
                    document.addEventListener('pointermove', move);
                    document.addEventListener('pointerup', up);
                });
            }
            this._dockTab = this._dockTab || 'graph';
            this.el.querySelector('.nway-body')?.appendChild(this._dockEl);
            this._dockDraw();
        }

        // The call graph of a whole lane: the dock's graph with the lane's functions as one focus.
        // The server resolves the lane's functions (`lane` + the table's params); each lane function keeps its own callers and callees,
        // so functions that never call each other still show what they share.
        async _laneGraph(id) {
            const g = (this.data.groups || {})[id];
            if (!g) return;
            const qs = this._listQs();
            qs.set('lane', id);
            let body = null;
            try {
                const res = await fetch(`/api/bin_sim/nway/lane_fids?${qs.toString()}`);
                body = res.ok ? await res.json() : null;
            } catch (e) { body = null; }
            if (!body || !body.rows.length) return;
            const laneRow = { n: body.rows.length };
            body.rows.forEach((fids, i) => fids.forEach(f => { laneRow[f] = i; }));
            // one function per file stands for the lane in the file strip
            const cells = {};
            for (const c of this._cgFiles()) { const f = body.fids.find(x => x.includes(`:func:${c.md5}:`)); if (f) cells[c.id] = f; }
            const name = LANE_MARK + (this._shownName((this._laneNames || {})[id]) || `${g.mask.split('1').length - 1}-file lane`);
            this._dockTab = 'graph';
            this._cgOpen({ name, cells, lane: id, laneRow });
        }

        // Right-hand dock in the table's own panel: simple function preview or a lean call graph.
        _dockDraw() {
            const { row, cells } = this._dockRow, tab = this._dockTab;
            this._dockEl.innerHTML = `<div class="nway-dock-grip" title="Drag to resize"></div><div class="nway-dock-head"><b class="nway-dock-name" title="${escapeAttr(row.name)}">${this._fnSig(row, { noClick: true })}</b>
                <div class="view-toggle" style="margin:0">${[['preview', 'Function'], ['graph', 'Call graph']].map(([v, t]) => `<button class="view-btn${tab === v ? ' active' : ''}" data-dock-tab="${v}">${t}</button>`).join('')}</div>
                <button class="nway-chip" data-dock-full title="Open the detailed N-way function view"><i class="fa-solid fa-up-right-from-square"></i></button>
                <button class="nway-chip" data-dock-close title="Close">&times;</button></div>
                <div class="nway-dock-body"></div>`;
            const body = this._dockEl.querySelector('.nway-dock-body');
            if (tab === 'graph') {
                // the graph follows the table's code axis
                this._cgStart(row.name, cells, { name_from: row.name_from, scope: this.state.scope, lean: true, lane: row.lane, laneRow: row.laneRow }, body);
                return;
            }
            this._cg = null;
            this._dw = null;
            const files = this._cgFiles().filter(c => cells[c.id]);
            if (!this._dockFid || !Object.values(cells).includes(this._dockFid)) this._dockFid = files.length ? cells[files[0].id] : Object.values(cells)[0];
            const chips = files.map(c => `<button class="nway-chip${cells[c.id] === this._dockFid ? ' on' : ''}" data-dock-fid="${escapeAttr(cells[c.id])}">${escapeHtml(c.file_name || c.label || c.md5 || c.id)}</button>`).join('');
            body.innerHTML = `<div class="nway-dock-files">${chips}</div><div class="c-code-container nway-dock-code"><div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i></div></div>`;
            const fid = this._dockFid;
            fetch(`/api/function/code?id=${encodeURIComponent(fid)}`).then(r => { if (!r.ok) throw new Error(`Function not found (${r.status})`); return r.json(); }).then(d => {
                const box = this._dockEl && this._dockFid === fid && this._dockEl.querySelector('.nway-dock-code');
                if (!box) return;
                box.innerHTML = (d.rows || []).map(r => `<div class="code-line"><div class="gutter"><div class="line-num">${Number(r.line_idx)}</div></div><div class="line-content">${(r.tokens || []).map(t => `<span class="token token-${escapeAttr(t.type)}">${escapeHtml(t.text)}</span>`).join('')}</div></div>`).join('') || 'No code available.';
            }).catch(err => {
                const box = this._dockEl && this._dockEl.querySelector('.nway-dock-code');
                if (box) box.textContent = err.message;
            });
        }

        _onDockClick(e) {
            const t = e.target.closest('[data-dock-tab],[data-dock-fid],[data-dock-full],[data-dock-close]');
            if (!t) return;
            if (t.dataset.dockTab) { this._dockTab = t.dataset.dockTab; return this._dockDraw(); }
            if (t.dataset.dockFid) { this._dockFid = t.dataset.dockFid; return this._dockDraw(); }
            if (t.dataset.dockClose !== undefined) {
                this._dockEl.remove();
                this._dockRow = this._dockEl = this._cg = this._dw = null;
                return;
            }
            const url = this._fnUrl(this._dockRow.row);
            if (url) { this._scrollSave(); window.Nav.openPath(url, e); }
        }

        // Draws into `host`: the side panel's body, or the standalone page's container.
        _cgStart(name, cells, init, host) {
            const prev = this._cg || {};
            this._cg = {
                stack: [{ name, cells, pieces: init.pieces, name_from: init.name_from, row: init.row, lane: init.lane, laneRow: init.laneRow }],
                view: init.view || prev.view || 'graph',
                scope: init.scope || prev.scope || this.state.scope || 'code',
                outside: init.outside || prev.outside || 'merge',
                depth: Number(init.depth || prev.depth || 1),
                more: {},
                place: {},
                co: init.co === '1' || init.co === true || (init.co === undefined && !!prev.co),
                hints: init.co === '1' || init.hints === '1' || init.co === true || (init.co === undefined && init.hints === undefined && !!prev.hints),
                suggest: prev.suggest === undefined ? 'comp' : prev.suggest,
                nb: null,
                lean: !!init.lean,
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
        // `preset`: the files of the lane as booleans, for a table group header (the graph lanes look theirs up).
        _cgLaneTip(lbl, preset) {
            clearTimeout(this._tipT);
            const files = this._cgFiles(), mask = preset || (this._cgLaneFiles || [])[Number(lbl.dataset.lane)];
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
            if (f.row !== undefined) qs.set('cg_row', f.row);
            for (const k of ['view', 'scope', 'depth']) qs.set(`cg_${k}`, st[k]);
            if (st.co) qs.set('cg_co', '1');
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
            qs.set('fx_ref', `nway:${(this.data.file_columns || this.data.columns).length}:${(Number(this.state.offset) || 0) + this.data.items.indexOf(row)}`);
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
            if (focus.lane) {
                // the lane's functions are resolved on the server from the table's own params
                for (const [k, v] of this._listQs()) qs.set(k, v);
                qs.set('lane', focus.lane);
                qs.set('lane_scope', this.state.scope);
            } else qs.set('fids', Object.values(focus.cells).join(','));
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
            // the dock has no colour toggle: its squares follow the table's (per file / cluster, or by coverage)
            if (this._cg && this._cg.lean && this._dw) this._dw.classList.toggle('nway-hue', !this._sqLane());
        }

        // i is a column index (data.columns), not an index into file_columns.
        _fileHue(i) {
            const t = this._tree(), c = t.grouped && t.colors[i];
            return c ? c.h : Math.round((i * 137.508 + 215) % 360);
        }

        // The column tree rebuilt from the columns' paths: each top-level group or column gets a hue, and what is
        // inside it shifts that hue and its lightness a little, so a sub-cluster reads as part of its parent.
        _tree() {
            const d = this.data;
            if (d._tree) return d._tree;
            const cols = d.columns, grouped = cols.some(c => c.kind || (c.path || []).length);
            const root = { kids: [] }, byId = new Map(), colors = [];
            const node = (parent, id, o) => {
                let n = byId.get(id);
                if (!n) { n = { id, kids: [], lo: Infinity, hi: -1, ...o }; parent.kids.push(n); byId.set(id, n); }
                return n;
            };
            cols.forEach((c, i) => {
                let p = root;
                const chain = [];
                for (const g of c.path || []) { p = node(p, g.id, { label: g.label, count: Number(g.member_count) || 0 }); chain.push(p); }
                const label = c.kind ? c.label : (c.file_name && c.file_name !== c.md5 ? c.file_name : String(c.md5 || c.id).slice(0, 8));
                const leaf = node(p, c.id, { label, count: c.kind ? Number(c.member_count) || 0 : 1, col: i, expandable: !!c.expandable });
                for (const n of [...chain, leaf]) { n.lo = Math.min(n.lo, i); n.hi = Math.max(n.hi, i); }
            });
            const paint = (n, h, l, depth) => {
                n.h = h; n.l = l;
                if (n.col != null) colors[n.col] = { h, l };
                const m = n.kids.length;
                n.kids.forEach((k, j) => paint(k, depth === 0 ? Math.round((j * 137.508 + 215) % 360) : (h + Math.round((j - (m - 1) / 2) * 12) + 360) % 360, depth === 0 ? 52 : 46 + (j % 3) * 6, depth + 1));
            };
            if (grouped) paint(root, 0, 52, 0);
            return (d._tree = { grouped, root, colors, byId });
        }

        // A line left of each column that starts a new group; the shallower the split, the heavier the line.
        _mark(table) {
            const t = this._tree();
            if (!t.grouped) return;
            let prev = [];
            const lvl = this.data.columns.map(c => {
                const chain = [...(c.path || []).map(g => g.id), c.id];
                let k = 0;
                while (k < prev.length && prev[k] === chain[k]) k++;
                prev = chain;
                return k;
            });
            for (const tr of table.rows) {
                const cells = tr.querySelectorAll('.col');
                cells.forEach((el, i) => { if (i && lvl[i] != null) el.classList.add(`cs${lvl[i]}`); });
            }
        }

        // The focus card's presence squares: which files have this function.
        _cgFocusSub(focus, files) {
            const fs = this._cgStrip(files.map(c => !!focus.cells[c.id]), files);
            return `<div class="nway-node-sub dim">${fs.html} <span>${fs.n}/${files.length} files</span></div>`;
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
                return `<i class="${on[i] ? 'on' : ''}${edge && edge[i] ? ' edge' : ''}"${total <= 16 ? ` style="--h:${this._fileHue(this.data.columns.findIndex(x => x.id === c.id))}"` : ''} title="${escapeAttr(tip)}" data-entity-data='${escapeAttr(JSON.stringify(data))}'
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
            const lr = this._cgFocus().laneRow;
            if (lr) {
                const k = new Set(Object.keys(it.sources || {}).map(f => lr[f]).filter(v => v !== undefined)).size;
                return `${pres}<span class="nway-edgecnt" title="linked with ${k} of ${lr.n} lane functions (in ${m.ne} files)"><i class="fa-solid fa-link"></i> ${k}/${lr.n} fn</span>`;
            }
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
                const gs = st.suggest === 'lanes' ? [] : st.suggest ? this._cgGroups(`${role}${lv}|`, entries, files, focus, st.place, canon) : [];
                const by = new Map(), done = new Set(), pos = new Map(), units = [];
                gs.forEach(g => g.m.forEach(r => by.set(r.x, g)));
                for (const x of entries) {
                    const g = by.get(x);
                    if (!g) units.push({ xs: [x], html: () => node(role, lv, x) });
                    else if (!done.has(g)) {
                        done.add(g);
                        units.push({ xs: g.m.map(r => r.x), html: () => {
                            const chip = `<span class="nway-grp" title="Suggested merge: ${g.m.length} cards with complementary files and the same parent.">&asymp; ${g.label} &middot; ${g.cov}/${files.length}</span>`;
                            const cells = {};
                            g.m.forEach(m => Object.entries(m.x.it.cells).forEach(([k, v]) => { if (typeof v === 'string') cells[k] = v; }));
                            const pieces = g.m.map(m => ({ name: m.x.it.name, files: Object.keys(m.x.it.cells).filter(k => typeof m.x.it.cells[k] === 'string'), fids: Object.values(m.x.it.cells).filter(v => typeof v === 'string').slice(0, 3) }));
                            this._cgGrp[g.id] = { name: g.m[0].x.it.name, cells, pieces };
                            const focusBtn = `<button class="nway-chip" data-cg-focusgrp="${escapeAttr(g.id)}" title="Make this merged group the focus: its functions in every file">&#9678; focus</button>`;
                            return `<div class="nway-grpbox${lv ? ' deep' : ''}"><div class="nway-grpbox-h">${chip}${focusBtn}</div>${g.m.map((r, i) => node(role, lv, r.x) + (i ? this._cgSim(pieces[0].fids, pieces[i].fids, pieces[0].name, pieces[i].name) : '')).join('')}</div>`;
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
                const fc = `<div class="nway-gcol nway-lane-focus" style="grid-row:1 / span ${rows};grid-column:${depth + 1}"><div class="nway-node center${focus.pieces ? ' merged-focus' : ''}" data-sel="focus"><div class="nway-node-name">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}</div>${this._cgFocusSub(focus, files)}${this._cgPieces(focus, files)}</div>${coHtml}</div>`;
                return `<div class="nway-graph" data-mid="${depth}" data-lanes="1" style="grid-template-columns:${cols.join(' ')}; column-gap:56px; width:max-content"><svg></svg>${heads.join('')}${body}${fc}</div>`;
            }
            const band = focus.lane && nb.callees && nb.callees.lane_rows ? this._cgLaneBand(nb.callees, files) : '';
            if (band) cols[depth] = 'max-content';
            const grid = depth > 1 || band ? ` style="grid-template-columns:${cols.join(' ')}; column-gap:56px; width:max-content"` : '';
            const left = [...columns.callers].reverse().map(col).join(''), right = columns.callees.map(col).join('');
            const mid = band || `<div class="nway-node center${focus.pieces ? ' merged-focus' : ''}" data-sel="focus"><div class="nway-node-name">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}</div>${this._cgFocusSub(focus, files)}${this._cgPieces(focus, files)}</div>`;
            return `<div class="nway-graph" data-mid="${depth}"${grid}><svg></svg>${left}
                <div class="nway-gcol">${mid}${coHtml}</div>
                ${right}</div>`;
        }

        // The members of a lane as separate cards, in tiers left to right by who calls whom (a cycle shares a tier);
        // the calls between them are drawn by _cgEdges from `data-lane-edges`. Capped at 12 cards until expanded.
        _cgLaneBand(cal, files) {
            const rows = cal.lane_rows, CAP = 12, all = this._cg.more.lane;
            const shown = all ? rows.length : Math.min(CAP, rows.length);
            const edges = (cal.lane_edges || []).filter(([a, b]) => a < shown && b < shown);
            const tier = this._cgLaneTiers(shown, edges);
            const tiers = [];
            for (let i = 0; i < shown; i++) (tiers[tier[i]] = tiers[tier[i]] || []).push(i);
            const card = i => `<div class="nway-member" tabindex="0" data-member="${i}" data-cg-member="${i}" title="Click to centre the graph on this function"><div class="nway-node-name">${this._nameSig({ name: rows[i].name, cells: rows[i].cells, name_from: rows[i].name_from }, { noClick: true })}</div>${this._cgFocusSub(rows[i], files)}</div>`;
            const more = shown < rows.length ? `<div class="nway-member more" tabindex="0" data-cg-more="lane" title="Show every member of the lane"><b><i class="fa-solid fa-ellipsis"></i> +${rows.length - shown} more</b></div>` : '';
            return `<div class="nway-node center band" data-sel="focus" data-lane-edges="${escapeAttr(JSON.stringify(edges))}">${tiers.filter(Boolean).map(t => `<div class="nway-tier">${t.map(card).join('')}</div>`).join('')}${more ? `<div class="nway-tier">${more}</div>` : ''}</div>`;
        }

        // Tier of each of n members: the longest chain of callers above it. A cycle (strongly connected set) counts once.
        _cgLaneTiers(n, edges) {
            const out = Array.from({ length: n }, () => []);
            edges.forEach(([a, b]) => out[a].push(b));
            // Tarjan
            let idx = 0;
            const num = new Array(n).fill(-1), low = [], on = [], stack = [], comp = new Array(n).fill(-1);
            let nc = 0;
            const dfs = v => {
                num[v] = low[v] = idx++;
                stack.push(v);
                on[v] = true;
                for (const w of out[v]) {
                    if (num[w] < 0) { dfs(w); low[v] = Math.min(low[v], low[w]); } else if (on[w]) low[v] = Math.min(low[v], num[w]);
                }
                if (low[v] === num[v]) {
                    let w;
                    do { w = stack.pop(); on[w] = false; comp[w] = nc; } while (w !== v);
                    nc++;
                }
            };
            for (let v = 0; v < n; v++) if (num[v] < 0) dfs(v);
            // Tarjan numbers components callees-first, so walking them downwards visits callers before callees
            const ct = new Array(nc).fill(0);
            for (let c = nc - 1; c >= 0; c--) {
                for (let v = 0; v < n; v++) {
                    if (comp[v] !== c) continue;
                    for (const w of out[v]) if (comp[w] !== c) ct[comp[w]] = Math.max(ct[comp[w]], ct[c] + 1);
                }
            }
            return comp.map(c => ct[c]);
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
            // Calls between the lane's own members, left to right between tiers (a cycle inside one tier loops over the top).
            if (center && center.dataset.laneEdges) {
                const mem = {};
                center.querySelectorAll('[data-member]').forEach(m => { mem[m.dataset.member] = m; });
                for (const [a, b, k] of JSON.parse(center.dataset.laneEdges)) {
                    if (!mem[a] || !mem[b]) continue;
                    const ra = mem[a].getBoundingClientRect(), rb = mem[b].getBoundingClientRect();
                    const y1 = ra.top + ra.height / 2 - gr.top, y2 = rb.top + rb.height / 2 - gr.top;
                    const w = `stroke-width:${Math.min(1 + k * 0.8, 8).toFixed(1)}`;
                    meta.push([mem[b], mem[a]]);
                    if (Math.abs(ra.left - rb.left) < 4) {
                        const x = ra.right - gr.left;
                        paths.push(`<path class="nway-edge lane" d="M${x},${y1} C${x + 26},${y1} ${x + 26},${y2} ${x},${y2}" style="${w}"></path>`);
                        continue;
                    }
                    const x1 = ra.right - gr.left, x2 = rb.left - gr.left, mx = (x1 + x2) / 2;
                    paths.push(`<path class="nway-edge lane" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" style="${w}"></path>`);
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
                const n = e.target.closest('.nway-member, .nway-node');
                if (!n) return clear();
                const on = new Set([n]);
                if (n.classList.contains('band')) n.querySelectorAll('.nway-member').forEach(m => on.add(m));
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
            // The path walked so far: the panel keeps the last two steps and folds the rest into a menu, the full view lists every step.
            const last = st.stack.length - 1;
            const crumbBtn = i => `<button class="nway-crumb${i === last ? ' cur' : ''}" data-cg-crumb="${i}" title="${escapeAttr(st.stack[i].name)}">${escapeHtml(st.stack[i].name)}</button>`;
            const keep = st.lean ? 2 : st.stack.length, early = Math.max(0, st.stack.length - keep);
            const crumbs = st.stack.length < 2 ? '' : (early ? `<details class="nway-crumb-more"><summary title="Earlier steps">\u2026${early}</summary><div class="nway-crumb-menu">${st.stack.slice(0, early).map((_, i) => crumbBtn(i)).join('')}</div></details>` : '')
                + st.stack.slice(early).map((_, i) => crumbBtn(early + i)).join('<span class="dim">\u203a</span>');
            // Graph view: the focus function in the header, return type and name coloured as in the table (the panel's own header already has the row's).
            const title = st.view === 'graph' && (!st.lean || st.stack.length > 1) ? `<div class="nway-cg-title">${this._fnSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}</div>` : '';
            const scopeToggle = [['code', 'Code', AXIS.code.icon, AXIS.code.color], ['library', 'Library', AXIS.library.icon, AXIS.library.color], ['all', 'All', 'fa-solid fa-layer-group', 'var(--success)']].map(([v, text, icon, color]) => `<span class="bsim-tag-pill" data-cg-scope="${v}" style="${window.binSimPillStyle(st.scope === v, color)} padding:4px 10px; font-size:0.78rem;"><i class="${icon}"></i>${text}</span>`).join('');
            const depthCtl = st.view === 'graph' ? `<label class="nway-ctl" title="How many hops to walk. Beyond the first, only rows in the selected scope are expanded (8 per row, 60 nodes)."><span class="nway-lbl">Depth</span><b>${st.depth}</b><input type="range" min="1" max="5" value="${st.depth}" data-cg-depth></label>` : '';
            const suggestCtl = st.view === 'graph' && st.suggest !== 'lanes' ? `<span class="nway-lbl" title="Auto: suggest merging cards on one side whose files barely overlap and that share a parent, probably one function split across files.">Group</span><div class="view-toggle" style="margin:0">${[['off', 'Off'], ['comp', 'Auto']].map(([v, t]) => `<button class="view-btn${(st.suggest || 'off') === v ? ' active' : ''}" data-cg-suggest="${v}">${t}</button>`).join('')}</div>` : '';
            const detailsCtl = st.view === 'graph' ? `<button class="view-btn${st.co ? ' active' : ''}" data-cg-details title="Also show the other functions that call what the focus calls (look-alikes and unmatched siblings; callees with more than 25 callers are skipped), and per card on hover its other callers / callees and calls back to earlier hops. Off by default: it can add a lot."><i class="fa-solid fa-diagram-project"></i> Details</button>` : '';
            const hueCtl = `<button class="view-btn${this._hueOn ? ' active' : ''}" data-cg-hue title="Cluster colors: each file's slot takes the colour of its file or cluster column in the table. Off: the slots show how many files have the function."><i class="fa-solid fa-palette"></i> Cluster colors</button>`;
            const selCtl = st.view === 'graph' ? `<button class="view-btn${st.selMode ? ' active' : ''}" data-cg-select title="Select cards: click to toggle, drag the background to box-select (Shift adds). Ctrl-click works without this mode."><i class="fa-solid fa-object-group"></i> Select</button>` : '';
            const lanes = st.view === 'graph' && st.suggest === 'lanes';
            const toggle = [['list', 'List'], ['graph', 'Graph'], ['lanes', 'Lanes']].map(([v, t]) => `<button class="view-btn${(v === 'lanes' ? lanes : v === 'graph' ? st.view === 'graph' && !lanes : st.view === v) ? ' active' : ''}" data-cg-view="${v}">${t}</button>`).join('');
            const on = files.map(c => !!focus.cells[c.id]);
            const fs = this._cgStrip(on, files);
            let body;
            if (err) body = `<div style="color:#f92672;">${escapeHtml(err)}</div>`;
            else if (!st.nb) body = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Reading call graph...</div>';
            else body = `<div class="dim nway-cg-legend"><i class="fa-solid fa-file-code"></i> files that have the function (color: how many) &middot; dot: the focus function there calls it &middot; <i class="fa-solid fa-link"></i> calls from the focus${st.view === 'graph' ? ' (edge width)' : ''}. Click to recenter.${st.co ? ' Dashed lines: other callers of the same callee.' : ''}${st.depth > 1 ? (st.view === 'graph' ? ' Further hops show in-scope rows only.' : ' The list shows the first hop; depth applies to the graph.') : ''}</div>` + (st.view === 'graph' ? this._cgGraph(st.nb, focus, files) : this._cgList(st.nb, focus, files));
            // The dock keeps the essentials: the axis is the table's, the rest lives in the full view.
            const head = st.lean
                ? `<div class="nway-cg-crumbs${st.lean ? ' lean' : ''}">${crumbs}</div>${depthCtl}<div class="view-toggle" style="margin:0">${toggle.replace(/<button[^>]*data-cg-view="lanes"[^>]*>.*?<\/button>/, '')}</div>`
                : `<div class="nway-cg-crumbs${st.lean ? ' lean' : ''}">${crumbs}</div><div class="nway-pills" title="Code: the program's own functions. Library: library code and imports. All: both.">${scopeToggle}</div>${depthCtl}${suggestCtl}${detailsCtl}${hueCtl}${selCtl}<div class="view-toggle" style="margin:0">${toggle}</div>`;
            this._dw.innerHTML = `<div class="nway-cg-head">${title}${head}${this.cgInit || st.lean ? '' : '<button class="nway-chip" data-cg-pop title="Open in its own page (keeps history)"><i class="fa-solid fa-up-right-from-square"></i></button><button class="nway-chip" data-cg-close title="Hide the panel (the N-WAY CALLS handle reopens it)">&times;</button>'}</div>
                ${st.view === 'list' ? `<div class="nway-cg-focus">${this._nameSig({ name: focus.name, cells: focus.cells, name_from: focus.name_from }, { noClick: true })}<div>${fs.html} <span class="dim">present in ${fs.n}/${files.length} files</span></div></div>` : ''}
                <div class="nway-cg-selbar" hidden></div>
                <div class="nway-cg-body${st.selMode ? ' selmode' : ''}">${body}</div>`;
            const cr = this._dw.querySelector('.nway-cg-crumbs');
            if (cr) cr.scrollLeft = cr.scrollWidth;
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
            const t = e.target.closest('[data-cg-go],[data-cg-view],[data-cg-scope],[data-cg-expand],[data-cg-crumb],[data-cg-close],[data-cg-pop],[data-cg-more],[data-cg-member],[data-cg-suggest],[data-cg-details],[data-cg-hue],[data-cg-focusgrp],[data-cg-mergefocus],[data-cg-deeper]');
            if (!t || !this._cg) return;
            if (t.dataset.cgClose !== undefined) return window.closeCallsPanel();
            if (t.dataset.cgPop !== undefined) {
                const url = this._cgUrl();
                window.closeCallsPanel();
                return window.Nav.openPath(url, e);
            }
            if (t.dataset.cgView) {
                const v = t.dataset.cgView, was = this._cg.suggest === 'lanes';
                this._cg.view = v === 'lanes' ? 'graph' : v;
                if (v === 'lanes') this._cg.suggest = 'lanes';
                else if (was) this._cg.suggest = 'comp';
                this._cgDraw();
                return this._cg.nb && this._cg.view === 'graph' ? this._cgPlace(this._cgTok) : undefined;
            }
            if (t.dataset.cgDetails !== undefined) { this._cg.co = this._cg.hints = !this._cg.co; return this._cgShow(); }
            if (t.dataset.cgHue !== undefined) {
                try { localStorage.setItem('nway.hue', this._hueOn ? '0' : '1'); } catch (err) { /* storage blocked */ }
                this._applyHue();
                return this._cgDraw();
            }
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
            if (t.dataset.cgMember !== undefined) {
                const m = this._cg.nb.callees.lane_rows[Number(t.dataset.cgMember)];
                this._cg.stack.push({ name: m.name, cells: m.cells, name_from: m.name_from });
                this._cg.more = {};
                return this._cgShow();
            }
            const [role, lv, i] = t.dataset.cgGo.split(':');
            const it = this._cg.nb && this._cg.nb[role].levels[Number(lv)][Number(i)];
            if (!it || it.kind !== 'row') return;
            const cells = {};
            for (const [k, v] of Object.entries(it.cells)) if (typeof v === 'string') cells[k] = v;
            this._cg.stack.push({ name: it.name, cells, name_from: it.name_from, row: String(it.key || '').startsWith('row:') ? it.key.slice(4) : undefined });
            this._cg.more = {};
            this._cgShow();
        }

        // ---- Group by: header rows between the table rows, collapsible ----
        _gOpen(id) {
            // a tier starts open and a fold closed, whatever the default
            const kind = ((this.data.groups || {})[id] || {}).kind;
            const start = kind === 'tier' ? true : kind === 'fold' ? false : this._gdef === 'all';
            return start !== this._gflip.has(id);
        }

        // Open or close groups; the server answers with the new list of headers and rows.
        _gChange(apply) {
            apply();
            this._scrollSave();
            this.state.offset = 0;
            this._keep = window.Paging.mode() === 'scroll' ? Math.max(PAGE, this.data.items.length) : 0;
            this.load();
        }

        // Open or close one group in place: only its rows are fetched and drawn, the rest of the table stays.
        // Redrawing the whole table cost ~200ms at 100 files; this costs the group's own rows.
        async _gSplice(id) {
            const d = this.data, items = d.items, groups = d.groups || {};
            const flip = () => { if (!this._gflip.delete(id)) this._gflip.add(id); };
            const i = items.findIndex(x => x.header && x.g === id);
            const tbody = this.el.querySelector('#nway-table tbody');
            const head = i < 0 || !groups[id] ? null : tbody.querySelector(`tr.nway-grow[data-gtoggle="${CSS.escape(id)}"]`);
            // A load or another splice still in flight: fall back to the full reload.
            if (!head || this._drawn !== this._gen) return this._gChange(flip);
            flip();
            const gen = ++this._gen, open = this._gOpen(id), path = groups[id].path;
            const under = g => !!g && !!groups[g] && groups[g].path.includes(id);
            const qs = this._listQs();
            let body;
            if (!open) {
                // Closing drops the rows now; the request brings the new total and the rows that refill the page.
                let n = 0;
                while (i + 1 + n < items.length && under(items[i + 1 + n].g)) n++;
                items.splice(i + 1, n);
                while (head.nextElementSibling && under(head.nextElementSibling.dataset.g || head.nextElementSibling.dataset.gtoggle)) head.nextElementSibling.remove();
            }
            qs.set('offset', (Number(d.offset) || 0) + (open ? i + 1 : items.length));
            qs.set('limit', open ? PAGE : Math.max(1, PAGE - items.length));
            try {
                const res = await fetch(`/api/bin_sim/nway?${qs.toString()}`, { signal: AbortSignal.timeout(NWAY_TIMEOUT_MS) });
                body = res.ok ? await res.json() : null;
            } catch (e) { body = null; }
            if (gen !== this._gen) return;
            if (!body) return this.load();
            if (open) {
                Object.assign(d.functions_metadata, body.functions_metadata);
                Object.assign(d.clusters, body.clusters);
                Object.assign(groups, body.groups);
                // The group's entries are the ones the open added; a group longer than the batch ends the loaded list.
                const added = Number(body.total) - Number(d.total), rows = body.items.slice(0, Math.max(0, added));
                const whole = added <= body.items.length;
                items.splice(i + 1, whole ? 0 : items.length, ...rows);
                if (!whole) while (head.nextElementSibling) head.nextElementSibling.remove();
                this._gprint = path;
                head.insertAdjacentHTML('afterend', this._rows(rows, i + 1));
            } else if (items.length < PAGE && body.items.length) {
                Object.assign(d.functions_metadata, body.functions_metadata);
                Object.assign(d.clusters, body.clusters);
                Object.assign(groups, body.groups);
                const last = items[items.length - 1], at = items.length;
                this._gprint = (last && last.g && groups[last.g] && groups[last.g].path) || [];
                items.push(...body.items);
                tbody.insertAdjacentHTML('beforeend', this._rows(body.items, at));
            }
            d.total = body.total;
            head.insertAdjacentHTML('afterend', this._groupHead(groups[id], path.length - 1));
            head.remove();
            this._mark(this.el.querySelector('#nway-table'));
            this._nameLanes();
            this.el.querySelector('.table-footer-left').innerHTML = window.Paging.footer({ offset: Number(d.offset) || 0, total: Number(d.total), size: PAGE, shown: items.length });
            this._drawn = gen;
            this._watch();
        }

        // Hover a group header: the lane tooltip of the files the group covers. A child-cluster column stands for its member files.
        _gTip(e, show) {
            const t = e.target.closest('[data-gmask], [data-gid], [data-colx], th[data-col]');
            if (!t || t.contains(e.relatedTarget)) return;
            if (!show) return this._cgTipLater();
            const gid = t.dataset.gid || t.dataset.colx || t.dataset.col;
            if (gid) {
                const { ids } = this._groupIds(gid);
                return ids.size && this._cgLaneTip(t, this._cgFiles().map(f => ids.has(f.id)));
            }
            const cols = this.data.columns, mask = t.dataset.gmask;
            this._cgLaneTip(t, this._cgFiles().map(f => [...mask].some((b, i) => b === '1' && cols[i] && (cols[i].kind ? cols[i].members.includes(f.id) : cols[i].id === f.id))));
        }

        // How many of a child/twin column's files the group holds: [held, total]; [0, 0] for a file column.
        _cover(g, col) {
            if (!col || !col.kind || !g.fcover) return [0, 0];
            const d = this.data;
            if (!d._fidx) d._fidx = new Map((this.data.file_columns || []).map((f, i) => [f.id, i]));
            return [col.members.filter(m => g.fcover[d._fidx.get(m)] === '1').length, col.members.length];
        }

        // The lane's name once the model has answered; a placeholder while it is still working.
        _laneName(g) {
            const name = this._shownName((this._laneNames || {})[g.id]);
            const wait = this._lnBusy && !name && ((g.sample || []).length || g.hint || (g.tags || []).length || (g.fids || []).length);
            const off = this.state.lane_names === 'off' || this.data.llm_names === false;
            const fb = !name && !wait && !off ? this._laneFallback(g) : '';
            return `<span class="nway-lname${wait ? ' wait' : ''}${off ? ' off' : ''}${fb ? ' fb' : ''}" data-lname="${escapeAttr(g.id)}"${fb ? ' title="The model found nothing to name this lane by; this is what it is made of."' : ''}>${escapeHtml(name || fb)}</span>`;
        }

        // A lane the model could not name: its main library calls, else its first function and how many more.
        _laneFallback(g) {
            const calls = (((this._laneEvid || {})[g.id] || {}).calls || []).slice(0, 3);
            if (calls.length) return `calls ${calls.join(', ')}`;
            const first = (g.fids || [])[0];
            const rows = Number(g.rows) || 0;
            return first ? `fn ${first.split(':').pop()}${rows > 1 ? ` +${rows - 1}` : ''}` : '';
        }

        // The nearest lane above this one: a sub-lane holds only files that are also in it.
        _laneParent(g) {
            const groups = this.data.groups || {};
            if (g.under_id) return groups[g.under_id] || null;
            const path = g.path || [];
            for (let i = path.length - 2; i >= 0; i--) if ((groups[path[i]] || {}).kind === 'lane') return groups[path[i]];
            return null;
        }

        // "⊂ network scanning": which lane this one is a part of, by name once known.
        _parentChip(g) {
            const par = this._laneParent(g);
            const files = par ? par.mask.split('1').length - 1 : Number(g.under) || 0;
            const label = (par && this._shownName((this._laneNames || {})[par.id])) || (files ? `${files}-file lane` : '');
            if (!label) return '';
            return `<span class="nway-gchip" data-lpar="${escapeAttr(g.id)}" title="${escapeAttr(`Sub-lane of ${label}: its files are all in that lane.`)}">\u2282 ${escapeHtml(label)}</span>`;
        }

        // The lane's capa / yara findings (group meta plus what the naming call saw), without the rule-detail tail.
        _laneTags(g) {
            const ev = (this._laneEvid || {})[g.id] || {};
            return [...new Set([...(g.tags || []), ...(ev.tags || [])].map(t => t.split('#')[0]))].slice(0, 4);
        }

        // A finding drawn like a tag is everywhere else: its own colour, on a tinted badge.
        _tagBadge(t) {
            const color = window.tagInk(window.getTagMetadata(t).color);
            return `<span class="analysis-tag-badge nway-ltag" style="border-color:${tagAlpha(color, 40)}; color:${color}; background:${tagAlpha(color, 7)};" title="${escapeAttr(t)}: a finding on this lane's functions">${escapeHtml(t)}</span>`;
        }

        // The colour of the lane's first finding: a stripe at the lane's left edge.
        _laneColor(g) {
            const t = this._laneTags(g)[0];
            return t ? window.tagInk(window.getTagMetadata(t).color) : '';
        }

        // What backs the name: the shared prefix, findings and a few strings the lane's functions use.
        _laneEv(g) {
            const ev = (this._laneEvid || {})[g.id] || {};
            const graphBtn = `<button type="button" class="nway-gchip nway-lanegraph" data-lane-graph="${escapeAttr(g.id)}" title="Open the call graph of this lane's functions together: who calls them, what they call, and whether they are called together"><i class="fa-solid fa-diagram-project"></i> call graph</button>`;
            const chip = (text, title, cls) => `<span class="nway-gchip${cls ? ` ${cls}` : ''}" title="${escapeAttr(title)}">${escapeHtml(text)}</span>`;
            return graphBtn + (g.hint ? chip(g.hint, "Most of this lane's functions share this name prefix") : '')
                + this._laneTags(g).map(t => this._tagBadge(t)).join('')
                + (ev.strings || []).map(t => chip(`"${t}"`, 'A string the lane\'s functions use', 'str')).join('');
        }

        _shownName(name) { return !name || /^unnamed/i.test(name) ? '' : name; }

        // Names for the lane groups on screen: asked in the background, cached by the server, shown when ready.
        async _nameLanes() {
            const d = this.data, groups = d && this.state.lane_names !== 'off' ? Object.values(d.groups || {}) : [];
            const lanes = groups.filter(g => g.kind === 'lane' && ((g.sample || []).length || g.hint || (g.tags || []).length || (g.fids || []).length)).slice(0, 40)
                .map(g => ({ id: g.id, files: g.mask.split('1').length - 1, total: g.mask.length, rows: g.rows, sample: g.sample || [], hint: g.hint || '', tags: g.tags || [], fids: g.fids || [] }));
            const sig = lanes.map(l => `${l.id}:${l.rows}`).join('|');
            if (sig === this._lnSig) return;
            this._lnSig = sig;
            if (!lanes.length) { this._laneNames = {}; return; }
            this._lnBusy = true;
            this.el.querySelectorAll('.nway-lname:empty').forEach(el => el.classList.add('wait'));
            for (let i = 0; i < 120 && this._lnSig === sig && this.el.isConnected; i++) {
                let body = null;
                try {
                    const res = await fetch('/api/bin_sim/nway/lane_names', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lanes }) });
                    body = res.ok ? await res.json() : null;
                } catch (e) { body = null; }
                if (this._lnSig !== sig) return;
                if (body && body.names) {
                    this._laneNames = { ...this._laneNames, ...body.names };
                    this._laneEvid = { ...this._laneEvid, ...body.evidence };
                    const groups = this.data.groups || {};
                    this.el.querySelectorAll('[data-lname]').forEach(el => {
                        const g = groups[el.dataset.lname], name = this._shownName(this._laneNames[el.dataset.lname]);
                        el.classList.remove('wait');
                        el.classList.toggle('fb', !name && !!g);
                        el.textContent = name || (g ? this._laneFallback(g) : '');
                    });
                    this.el.querySelectorAll('[data-lpar]').forEach(el => {
                        const g = groups[el.dataset.lpar];
                        if (g) el.outerHTML = this._parentChip(g);
                    });
                    this.el.querySelectorAll('[data-lev]').forEach(el => {
                        const g = groups[el.dataset.lev];
                        if (!g) return;
                        el.innerHTML = this._laneEv(g);
                        el.closest('tr')?.style.setProperty('--lane-c', this._laneColor(g) || 'var(--border)');
                    });
                }
                if (body && body.status === 'pending') { await new Promise(r => setTimeout(r, 1000)); continue; }
                if (body && body.status === 'off') this.data.llm_names = false;
                break;
            }
            if (this._lnSig === sig) {
                this._lnBusy = false;
                this.el.querySelectorAll('.nway-lname.wait').forEach(el => el.classList.remove('wait'));
            }
        }

        _groupHead(g, depth) {
            const n = g.union.length, closed = !this._gOpen(g.id);
            const chip = (cls, text, tip) => `<span class="nway-gchip ${cls}" title="${escapeAttr(tip)}">${escapeHtml(text)}</span>`;
            let title, note = '';
            if (g.kind === 'prefix') {
                title = `<span class="nway-gname">${escapeHtml(g.label)}</span>`;
            } else if (g.kind === 'tag') {
                title = `<span class="nway-gtag" style="color:${escapeAttr(window.TagColor.forTag(g.label))}">${escapeHtml(g.label)}</span>`;
            } else if (g.kind === 'lane') {
                const cov = g.fcover || g.mask;
                const mix = g.core ? chip('also', `${Number(g.lanes)} lanes · ${g.core.split('1').length - 1} shared`, 'Not a real set of files: related lanes grouped under the files they cover together. The count is the files every lane in it holds.') : '';
                const under = this._parentChip(g);
                const have = cov.split('1').length - 1;
                title = `${this._laneName(g)}<span class="nway-lcount"><b>${have}</b> of ${cov.length} files</span>${mix}${under}`;
            } else if (g.kind === 'tier') {
                const hi = Number(g.max), lo = Number(g.min);
                title = `<b>${escapeHtml(g.label)}</b> <span class="dim">${lo ? (hi > lo ? `${lo}\u2013${hi}` : `${lo}`) : `up to ${hi}`} files</span>`;
            } else if (g.kind === 'fold') {
                title = `<b>${Number(g.lanes)} small lanes</b> <span class="dim">${Number(this.state.group_lane_fold) > 1 ? `under ${Number(this.state.group_lane_fold)} rows each` : ''}</span>`;
            } else if (g.kind === 'same') {
                const ev = (g.evidence || []).map(k => k === 'name' ? chip('ev-n', 'name', 'The rows share a function name') : chip('ev-c', 'call position', 'The rows share a caller and a weak edge links them')).join('');
                const more = `${this._missChip(g.sub, g.miss)}${g.also > 1 ? chip('also', `\u00d7${Number(g.also)}`, 'Listed under several tags on this axis.') : ''}`;
                title = `${(g.names || []).map(x => `<span class="nway-gname">${escapeHtml(x)}</span>`).join(' + ')}${ev}${more}`;
                note = `${g.files} of ${n} files together${g.link ? ` &middot; best link ${Number(g.link).toFixed(2)}, under the edge` : ''}`;
            }
            // The files the group covers, one square per file column: the same grid as the rows below.
            const files = g.kind === 'lane' ? g.mask : g.union, cols = this.data.columns;
            const sq = [...files].map((b, i) => {
                const [have, all] = this._cover(g, cols[i]);
                const full = all ? have === all : b === '1', some = all ? have > 0 : b === '1';
                return `<td class="col sq"><i class="nway-sq${some ? (full ? ' on' : ' part') : ''}" style="${this._sqStyle(i)}" title="${escapeAttr(this._colLabel(cols[i]) + (some ? (all > 1 ? `: ${have} of ${all} files` : '') : ': not in this group'))}">${all > 1 && some && !full ? have : ''}</i></td>`;
            }).join('');
            const diff = g.kind === 'same' && g.fids?.length >= 2
                ? `<button class="nway-gchip ev-n nway-merge-diff" data-merge-diff="${escapeAttr(g.id)}" title="N-way diff of all ${g.fids.length} functions in this merge" aria-label="N-way diff of all ${g.fids.length} functions in this merge, ${pct(g.cohesion)} cohesion">${pct(g.cohesion)}</button>` : '';
            return `<tr class="nway-grow d${depth} k-${escapeAttr(g.kind || 'tag')} ${g.kind === 'tier' || g.kind === 'fold' ? '' : this._ratioCls((g.fcover || files).split('1').length - 1)}" style="--lane-c:${g.kind === 'lane' ? escapeAttr(this._laneColor(g) || 'var(--border)') : 'var(--border)'}" data-gtoggle="${escapeAttr(g.id)}"><td colspan="${this._lead().length}"><div class="nway-gh" style="padding-left:${10 + depth * 20}px"><button class="nway-gchev" data-gtoggle="${escapeAttr(g.id)}" aria-label="${closed ? 'Expand' : 'Collapse'} group" aria-expanded="${!closed}">${closed ? '&#9656;' : '&#9662;'}</button><span class="nway-gt" data-gmask="${escapeAttr(files)}">${title}${diff}</span><span class="nway-gst">${Number(g.rows)} row${g.rows === 1 ? '' : 's'} &middot; ${Number(g.weight).toFixed(0)} feat</span><span class="nway-gnote">${note}</span>${g.kind === 'lane' ? `<div class="nway-lev" data-lev="${escapeAttr(g.id)}">${this._laneEv(g)}</div>` : ''}</div></td>${sq}</tr>`;
        }

        // Rows, with a header printed whenever a row's group path leaves the one printed last.
        _rows(items, at) {
            if (!at) this._gprint = [];
            const groups = this.data.groups || {};
            return items.map(row => {
                const path = (row.g && groups[row.g] && groups[row.g].path) || [];
                let k = 0, head = '';
                while (k < path.length && this._gprint[k] === path[k]) k++;
                for (let d = k; d < path.length; d++) head += this._groupHead(groups[path[d]], d);
                this._gprint = path;
                return head + (row.header ? '' : this._row(row));
            }).join('');
        }

        _row(row) {
            const cols = this.data.columns;
            const extra = Array.isArray(row.names) ? row.names.length - 1 : Number(row.names || 0) - 1;
            const low = row.support < LOW_SUPPORT && (row.file_span || row.span) > 2
                ? `<i class="fa-solid fa-triangle-exclamation nway-warn" title="low support: only ${pct(row.support)} of the possible pairs in this row are matched; it may be a chain of transitive matches"></i>` : '';
            const also = row.also > 1 ? `<span class="nway-gchip also" title="This row has ${Number(row.also)} tags on this axis, so it is listed under each of them">&times;${Number(row.also)}</span>` : '';
            // Rows are indented one step past the header of the group they sit in, so a row with no partner lines up with the merged functions beside it.
            const g = row.g && (this.data.groups || {})[row.g], depth = g ? g.path.length : 0;
            const rc = this._ratioCls(Number(row.file_span || row.span) || 0);
            return `<tr class="${depth ? 'nway-in ' : ''}${rc}"${depth ? ` style="--gd:${depth}" data-g="${escapeAttr(row.g)}"` : ''}>
                <td class="nway-open" data-fn-open title="Show this row's call graph (Ctrl-click: compare its functions in a full page)" style="min-width:260px; max-width:420px;">${this._nameSig(row, { noClick: true })}<span class="nway-name">${extra > 0 ? `<span class="dim">+${extra} names</span>` : ''}${low}${also}${this._missChip(row.sub, row.miss)}</span></td>
                ${this._statsOff() ? '' : `<td class="num">${Number(row.file_span || row.span)}</td>
                <td class="num">${Number(row.weight).toFixed(0)}</td>
                <td class="num">${this._score(row.support)}</td>
                <td class="num">${this._score(row.cohesion)}</td>
                <td class="cluster-cards-cell">${window.EntityRenderer.renderClusterCard((row.clusters || []).map(u => this.data.clusters[u]).filter(Boolean))}</td>`}
                ${cols.map((c, ci) => this._cell(row, c, ci)).join('')}
            </tr>`;
        }

        // Fixed column widths, so opening or closing a group never reflows the columns.
        // The stats columns (Span .. Cluster) can be hidden to leave room for the matrix.
        _statsOff() {
            if (this._so == null) {
                try { this._so = localStorage.getItem('nway.stats') === '0'; } catch (e) { this._so = false; }
            }
            return this._so;
        }

        // File squares coloured by how many files hold the row (as in the graph view) instead of one colour per file or cluster.
        _sqLane() {
            if (this._sl == null) {
                try { this._sl = localStorage.getItem('nway.sqlane') === '1'; } catch (e) { this._sl = false; }
            }
            return this._sl;
        }

        // r-hi: every file, r-mid: half or more, r-low: fewer; the same classes the graph view colours its slots with.
        _ratioCls(n) {
            // files, not columns: twin / cluster columns stand for several files each
            const total = (this.data.file_columns || this.data.columns.filter(c => !c.kind)).length;
            return n >= total ? 'r-hi' : n * 2 >= total ? 'r-mid' : 'r-low';
        }

        // The Function column is the one the user can drag wider or narrower; the width is remembered.
        _fnW() {
            if (this._fw == null) {
                try { this._fw = Number(localStorage.getItem('nway.fnw')) || 0; } catch (e) { this._fw = 0; }
            }
            return this._fw || DATA_COLS[0];
        }

        _lead() {
            const cols = [this._fnW(), ...DATA_COLS.slice(1)];
            return this._statsOff() ? cols.slice(0, 1) : cols;
        }

        _fnGrip(e) {
            const table = this.el.querySelector('#nway-table'), col = table && table.querySelector('colgroup col');
            if (!col) return;
            e.preventDefault();
            const x0 = e.clientX, w0 = this._fnW(), t0 = table.offsetWidth;
            const move = ev => {
                const w = Math.max(220, Math.min(1100, Math.round(w0 + ev.clientX - x0)));
                this._fw = w;
                col.style.width = `${w}px`;
                table.style.width = `${t0 + w - w0}px`;
            };
            const up = () => {
                document.removeEventListener('pointermove', move);
                document.removeEventListener('pointerup', up);
                try { localStorage.setItem('nway.fnw', String(this._fw)); } catch (err) { /* not saved */ }
            };
            document.addEventListener('pointermove', move);
            document.addEventListener('pointerup', up);
        }

        _tableWidth() {
            return this._lead().reduce((a, b) => a + b, 0) + SQ_COL * this.data.columns.length;
        }

        _colgroup() {
            return `<colgroup>${this._lead().map(w => `<col style="width:${w}px">`).join('')}${`<col style="width:${SQ_COL}px">`.repeat(this.data.columns.length)}</colgroup>`;
        }

        // The columns the user has expanded (ids of column groups); the server answers with their children.
        _colOpen() {
            try { return new Set(JSON.parse(this.state.col_open || '[]')); } catch (e) { return new Set(); }
        }

        _colToggle(id, openOnly) {
            let open = this._colOpen();
            // Folding one node after "expand all": spell the wildcard out as the nodes now open.
            if ([...open].some(i => i[0] === '*')) open = new Set([...this._tree().byId.values()].filter(n => n.kids.length).map(n => n.id));
            if (openOnly) open.add(id); else if (!open.delete(id)) open.add(id);
            this._scrollSave();
            this._set({ col_open: JSON.stringify([...open]), offset: 0 });
        }

        _head() {
            const s = this.state;
            const th = ([key, label]) => {
                const arrow = s.sort_col === key ? (s.sort_dir === 'asc' ? '▲' : '▼') : '↕';
                return `<th class="sortable${key === 'name' ? '' : ' num'}" data-sort="${key}">${label} <span class="dim">${arrow}</span></th>`;
            };
            // A narrow column per file: its colour square on top, the name running up the column. The counts live in the hover.
            const fileHead = (c, ci) => {
                const named = c.file_name && c.file_name !== c.md5;
                const name = named ? window.EntityRenderer.renderFileName(c.file_name, c.md5, c.collection) : escapeHtml(c.md5.slice(0, 8));
                const coverage = c.coverage == null ? '' : ` · ${(Number(c.coverage) * 100).toFixed(0)}% of the shared weight`;
                return `<th class="col sq" data-col="${escapeAttr(c.id)}" title="${escapeAttr(`${c.file_name} (${c.collection})\n${c.md5}\n${Number(c.functions)} fn${coverage}\nright-click: focus / filter`)}"><i class="nway-sq on" style="${this._sqStyle(ci)}"></i><div class="nway-vname">${name}</div></th>`;
            };
            // Fold or unfold every row group, from the head of the column they sit in.
            const foldBtns = s.group || s.group_merge
                ? '<span class="nway-fold"><button type="button" data-gall="open" title="Expand every group"><i class="fa-solid fa-angles-down"></i></button><button type="button" data-gall="close" title="Collapse every group"><i class="fa-solid fa-angles-up"></i></button></span>'
                : '';
            const hdrBtns = `<span class="nway-fold"><button type="button" class="${this._statsOff() ? '' : 'on'}" data-stats-toggle title="${this._statsOff() ? 'Show' : 'Hide'} the Span, Features, Support, Cohesion and Cluster columns"><i class="fa-solid fa-table-columns"></i></button><button type="button" class="${this._sqLane() ? 'on' : ''}" data-sqmode title="${this._sqLane() ? 'Squares coloured by how many files have the function. Click for one colour per file or cluster.' : 'Squares coloured per file or cluster. Click to colour by how many files have the function.'}"><i class="fa-solid fa-palette"></i></button></span>`;
            const th0 = c => th(c).replace('</th>', `${hdrBtns}${foldBtns}<span class="nway-colgrip" title="Drag to resize the Function column (double-click to reset)"></span></th>`);
            const lead = this._statsOff() ? th0(SORTS[0]) : th0(SORTS[0]) + SORTS.slice(1).map(th).join('') + '<th>Cluster</th>';
            if (this._tree().grouped) {
                const bands = this._bands();
                return `${bands.html}<tr style="--bt:${bands.n * BAND_H}px">${lead}${this.data.columns.map((c, ci) => this._numHead(c, ci)).join('')}</tr>`;
            }
            return `<tr>${lead}${this.data.columns.map(fileHead).join('')}</tr>`;
        }

        // One header row per nesting level: a band over the columns of each opened cluster or twin, with a button that folds it.
        _bands() {
            const cols = this.data.columns, byId = this._tree().byId;
            const n = Math.max(0, ...cols.map(c => (c.path || []).length));
            let html = '', shown = 0;
            // Deep trees: keep the top 2 and last 2 levels until the user expands.
            const cut = n > 4 && !this._bandsAll;
            for (let k = 0; k < n; k++) {
                if (cut && k >= 2 && k < n - 2) continue;
                const lead = this._lead().length;
                let cells = `<th></th>${lead > 1 ? `<th colspan="${lead - 1}"></th>` : ''}`;
                for (let i = 0; i < cols.length;) {
                    const g = (cols[i].path || [])[k];
                    let j = i + 1;
                    while (j < cols.length && ((cols[j].path || [])[k] || {}).id === (g || {}).id) j++;
                    if (!g) cells += `<th colspan="${j - i}"></th>`;
                    else {
                        const nd = byId.get(g.id) || {};
                        const tip = `${g.label} (${Number(g.member_count)} files)\nclick to fold`;
                        cells += `<th class="nway-band" colspan="${j - i}" data-colx="${escapeAttr(g.id)}" title="${escapeAttr(tip)}" style="--h:${nd.h};--l:${nd.l}%"></th>`;
                    }
                    i = j;
                }
                html += `<tr class="nway-bandrow" style="--bt:${shown++ * BAND_H}px">${cells}</tr>`;
                // Marker row between the top 2 and the rest; it carries the expand / collapse button.
                if (n > 4 && k === 1) {
                    const label = cut ? `<i class="fa-solid fa-ellipsis"></i> ${n - 4}` : '<i class="fa-solid fa-angles-up"></i>';
                    html += `<tr class="nway-bandrow nway-bandmore" style="--bt:${shown++ * BAND_H}px"><th colspan="${this._lead().length + cols.length}"><button type="button" data-bandall title="${cut ? `Show the ${n - 4} hidden levels` : 'Hide the middle levels'}">${label}</button></th></tr>`;
                }
            }
            return { html, n: shown };
        }

        // With grouped columns the header is a numbered dot in the cluster's colour; names live in the legend.
        _numHead(c, ci) {
            const fname = new Map((this.data.file_columns || []).map(f => [f.id, f.file_name || f.md5]));
            const kindNote = { child: 'child cluster', direct: 'files at this node, in no child cluster', twin: 'files that hold the same shared functions', meta: 'files that share this value', query: 'files the query matched' };
            let tip;
            if (c.kind) {
                const members = (c.members || []).map(m => fname.get(m) || m);
                tip = [c.label, `${Number(c.member_count)} files`, kindNote[c.kind] || '', ...members.slice(0, 12), members.length > 12 ? `+${members.length - 12} more` : '', c.expandable ? 'click to show the files' : ''].filter(Boolean).join('\n');
            } else {
                tip = `${c.file_name} (${c.collection})\n${c.md5}\n${Number(c.functions)} fn\nright-click: focus / filter`;
            }
            return `<th class="col sq nway-numcol${c.expandable ? ' expandable' : ''}" data-gid="${escapeAttr(c.id)}"${c.kind ? (this._colFile(c) ? ` data-lfile="${escapeAttr(this._colFile(c))}"` : '') : ` data-col="${escapeAttr(c.id)}"`}${c.expandable ? ` data-colx="${escapeAttr(c.id)}"` : ''} title="${escapeAttr(tip)}"><i class="nway-num${Number(c.member_count) > 1 ? ' grp' : ''}" style="${this._sqStyle(ci)}">${ci + 1}</i></th>`;
        }

        // The column outline: every group and column with its number range, and the button that opens or folds it.
        _legend() {
            const t = this._tree();
            if (!t.grouped) return '';
            const row = (n, dep) => {
                const open = n.kids.length > 0, toggle = open || n.expandable;
                const range = n.lo === n.hi ? `${n.lo + 1}` : `${n.lo + 1}–${n.hi + 1}`;
                const tip = `${n.label}${n.count > 1 ? ` (${n.count} files)` : ''}${toggle ? (open ? '\nclick to fold' : '\nclick to show the files') : ''}`;
                return `<div class="nway-ol${toggle ? ' expandable' : ''}" data-gid="${escapeAttr(n.id)}" style="--dep:${dep}"${toggle ? ` data-colx="${escapeAttr(n.id)}"` : ''}${n.col !== undefined && this._colFile(this.data.columns[n.col]) ? ` data-lfile="${escapeAttr(this._colFile(this.data.columns[n.col]))}"` : ''} title="${escapeAttr(tip)}">`
                    + (toggle ? `<button type="button" class="nway-tg" data-colx="${escapeAttr(n.id)}" aria-label="${open ? 'Fold' : 'Open'} ${escapeAttr(n.label)}">${open ? '&#9662;' : '&#9656;'}</button>` : '<span class="nway-tg"></span>')
                    + `<i class="nway-sq on${n.count > 1 ? ' grp' : ''}" style="--h:${n.h};--l:${n.l}%;--ll:${n.l - 8}%"></i><span class="nway-olb">${escapeHtml(n.label)}</span>`
                    + (n.count > 1 ? `<em>${n.count}</em>` : '') + `<b class="nway-rg">${range}</b></div>`
                    + n.kids.map(k => row(k, dep + 1)).join('');
            };
            return `<aside class="nway-legend${this._legOff ? ' off' : ''}"><details${this._legOff ? '' : ' open'} ontoggle="NwayPanel.legend(this)"><summary>Columns <em>${this.data.columns.length}</em><span class="nway-fold"><button type="button" data-coldepth="all" title="Expand every column group"><i class="fa-solid fa-angles-down"></i></button><button type="button" data-coldepth="0" title="Collapse every column group"><i class="fa-solid fa-angles-up"></i></button></span></summary>${this._colDepth()}${t.root.kids.map(k => row(k, 0)).join('')}</details></aside>`;
        }

        // The host (the N-way page) can park its own controls in the More section.
        render() {
            this._renderPanel();
            this._nameLanes();
            if (this.onSlot) {
                const actions = this.el.querySelector('[data-actions]');
                this.onSlot(this.el.querySelector('[data-more-slot]'), actions);
                // the host may move the actions out of this.el, where the delegated listeners no longer reach
                if (actions && !this.el.contains(actions)) {
                    actions.addEventListener('click', e => this._onClick(e));
                    actions.addEventListener('change', e => this._onChange(e));
                    actions.addEventListener('input', e => this._onInput(e));
                    this._bindCmp(actions);
                }
            }
        }

        _renderPanel() {
            if (this.error) {
                this.el.innerHTML = `<div class="nway-panel">${this._renderError()}</div>`;
                return;
            }
            const d = this.data;
            const rows = this._rows(d.items, 0);
            const off = Number(d.offset), total = Number(d.total);
            const scroll = window.Paging.mode() === 'scroll';
            this.el.innerHTML = `<div class="nway-panel">${this._controls()}${this._notes()}${d.group_note ? `<div class="nway-note warn">${escapeHtml(d.group_note)}</div>` : ''}
                <div class="nway-card table-scope">
                    <div class="nway-body">${this._legend()}
                    <div class="nway-scroll">
                        <table id="nway-table" class="nway-table${this._sqLane() ? ' nway-sqlane' : ''}" style="width:${this._tableWidth()}px">${this._colgroup()}
                            <thead>${this._head()}</thead>
                            <tbody>${rows || `<tr><td colspan="${d.columns.length + 7}" class="gap">No rows in this tab.</td></tr>`}</tbody>
                            ${scroll ? `<tfoot><tr><td class="nway-sentinel" colspan="${d.columns.length + 7}"></td></tr></tfoot>` : ''}
                        </table>
                    </div></div>
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
            if (this._dockEl) {
                this.el.querySelector('.nway-body').appendChild(this._dockEl);
                // the code axis changed in the table: the dock graph follows it
                if (this._cg && this._cg.lean && this._cg.scope !== this.state.scope) { this._cg.scope = this.state.scope; this._cgShow(); }
            }
            this._mark(this.el.querySelector('#nway-table'));
            this._watch();
            this._scrollRestore();
            // Header and notes fold away once the table scrolls; they return at the top.
            const sc = this.el.querySelector('.nway-scroll'), view = this.el.closest('.nway-view');
            if (sc && view) sc.addEventListener('scroll', () => {
                if (sc.scrollTop > 30) view.classList.add('nway-compact');
                else view.classList.remove('nway-compact');
            }, { passive: true });
        }

        // Scroll mode: load the next batch when the sentinel nears the viewport.
        _watch() {
            if (this._io) this._io.disconnect();
            const s = this.el.querySelector('.nway-sentinel');
            if (!s) return;
            // Keep the sentinel: a group opened later adds rows and needs it again.
            if (this.data.items.length >= Number(this.data.total)) return;
            this._io = window.Paging.observe(s, () => this.load(true));
        }

        _append(body) {
            const d = this.data;
            Object.assign(d.functions_metadata, body.functions_metadata);
            Object.assign(d.clusters, body.clusters);
            Object.assign(d.groups, body.groups);
            const at = d.items.length;
            d.items.push(...body.items);
            d.total = body.total;
            this.el.querySelector('#nway-table tbody').insertAdjacentHTML('beforeend', this._rows(body.items, at));
            this._nameLanes();
            this.el.querySelector('.table-footer-left').innerHTML = window.Paging.footer({ offset: 0, total: Number(d.total), size: PAGE, shown: d.items.length });
            this._mark(this.el.querySelector('#nway-table'));
            this._watch();
        }

        _onClick(e) {
            const t = e.target.closest('[data-nway-file],[data-set],[data-preset],[data-tab],[data-sort],[data-nav],[data-open-cluster],[data-focus-toggle],[data-clear],[data-fn-open],[data-gtoggle],[data-merge-diff],[data-gall],[data-gpick],[data-colx],[data-coldepth],[data-bandall],[data-stats-toggle],[data-sqmode],[data-lane-graph],[data-open-shared],[data-lineage],[data-pick-by],[data-cmp-run],[data-cmp-clear],[data-cmp-addq],[data-cmp-rmq],[data-cmp-addg],[data-cmp-rmg],[data-cmp-within],[data-tok-rm],[data-sug-i],[data-cmp-show],[data-colpick],[data-colpick-clear],[data-more-toggle]');
            if (!t) return;
            if (t.dataset.lineage !== undefined) {
                this._scrollSave();
                return window.Nav.openPath(`/diff/nway/lineage?${this._docQs().toString()}`, e, { title: 'Lineage', type: 'bin_sim' });
            }
            if (t.dataset.mergeDiff) {
                const group = this.data.groups[t.dataset.mergeDiff];
                if (!group) return;
                e.preventDefault();
                this._scrollSave();
                return window.Nav.openPath(NwayPanel.urlForFunctions(group.fids), e, { title: 'N-way diff', type: 'bin_sim' });
            }
            if (t.dataset.bandall !== undefined) {
                this._bandsAll = !this._bandsAll;
                return this.render();
            }
            if (t.dataset.laneGraph) return this._laneGraph(t.dataset.laneGraph);
            if (t.dataset.sqmode !== undefined) {
                this._sl = !this._sqLane();
                try { localStorage.setItem('nway.sqlane', this._sl ? '1' : '0'); } catch (err) { /* storage blocked */ }
                this._applyHue();
                this._scrollSave();
                return this.render();
            }
            if (t.dataset.statsToggle !== undefined) {
                this._so = !this._statsOff();
                try { localStorage.setItem('nway.stats', this._so ? '0' : '1'); } catch (err) { /* storage blocked */ }
                this._scrollSave();
                return this.render();
            }
            if (t.dataset.openShared) {
                e.preventDefault();
                return window.Nav.openPath(this._clusterUrl(t.dataset.openShared), e, { title: 'Cluster', type: 'cluster' });
            }
            if (t.dataset.coldepth) {
                e.preventDefault();
                const n = t.dataset.coldepth;
                if (Number(n) > (this._dlim || 5)) this._dlim = Number(n);
                this._scrollSave();
                return this._set({ col_open: n === 'all' ? '["*"]' : n === '0' ? '[]' : JSON.stringify([`*${n}`]), offset: 0 });
            }
            // A header dot only opens its column; the outline folds it again.
            if (t.dataset.colx) return this._colToggle(t.dataset.colx, !!t.closest('th') && !t.closest('.nway-band'));
            if (t.dataset.gpick) {
                const lv = (this.state.group || '').split(',').filter(Boolean), i = lv.indexOf(t.dataset.gpick);
                if (i >= 0) lv.splice(i, 1); else lv.push(t.dataset.gpick);
                return this._set({ group: lv.join(','), offset: 0 });
            }
            if (t.dataset.gtoggle !== undefined && !t.dataset.gall) return this._gSplice(t.dataset.gtoggle);
            if (t.dataset.gall) {
                return this._gChange(() => { this._rowAuto = false; this._gdef = t.dataset.gall === 'open' ? 'all' : 'none'; this._gflip.clear(); });
            }
            if (t.dataset.nwayFile) {
                e.preventDefault(); e.stopPropagation();
                const f = this._files().find(x => x.id === t.dataset.nwayFile);
                if (!f) return;
                const base = this.source.pool ? `/pools/${encodeURIComponent(this.source.pool)}` : '';
                return window.Nav.openPath(`${base}/collections/${encodeURIComponent(f.collection)}/files/${encodeURIComponent(f.md5)}`, e, { title: f.file_name || f.md5, type: 'file' });
            }
            if (t.dataset.fnOpen !== undefined) {
                if (e.target.closest('button, a, input, .bsim-tag-pill, .entity-actions')) return;
                // The n-th row cell is the n-th non-header item (group splices shift indexes, so none is stored).
                const at = [...this.el.querySelectorAll('#nway-table td[data-fn-open]')].indexOf(t);
                const row = this.data.items.filter(x => !x.header)[at];
                // Plain click: the call-graph side panel. Ctrl / Shift / middle click: the full function-family page.
                if (!e.ctrlKey && !e.metaKey && !e.shiftKey && row && !this.cgInit) return this._cgOpen(row);
                const url = this._fnUrl(row);
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
            if (t.dataset.preset) return this._set({ ...PRESETS.find(p => p[0] === t.dataset.preset)[2], offset: 0 });
            if (t.dataset.pickBy !== undefined) return this._pickBy();
            if (t.dataset.moreToggle !== undefined) { this._moreOpen = !this._moreOpen; this._scrollSave(); return this.render(); }
            if (t.dataset.colpick !== undefined) {
                const list = this._pickList(), v = t.dataset.colpick, i = list.indexOf(v);
                if (i >= 0) list.splice(i, 1); else list.push(v);
                return this._set({ col_pick: list.length ? JSON.stringify(list) : '', col_rest: list.length ? this.state.col_rest : '', side: '', col_open: '', offset: 0 });
            }
            if (t.dataset.colpickClear !== undefined) return this._set({ col_pick: '', col_rest: '', side: '', offset: 0 });
            if (t.dataset.cmpRun !== undefined) return this._runCompare();
            if (t.dataset.tokRm !== undefined) return this._tokRemove(t.dataset.tokRm, t.dataset.k, t.dataset.v);
            if (t.dataset.sugI !== undefined && this._sug) { const x = this._sug.items[Number(t.dataset.sugI)]; return x && this._tokAdd(this._sug.path, x.k, x.v); }
            if (t.dataset.cmpShow !== undefined) { this._cmpShow = t.dataset.cmpShow; return this._cmpRefreshResult(); }
            if (t.dataset.cmpAddq !== undefined) { this._draft.groups[Number(t.dataset.cmpAddq)].queries.push({}); return this._cmpRefresh(); }
            if (t.dataset.cmpRmq !== undefined) {
                const [gi, qi] = t.dataset.cmpRmq.split('.').map(Number);
                this._draft.groups[gi].queries.splice(qi, 1);
                this._cmpRefresh();
                return this._cmpPreviewSoon();
            }
            if (t.dataset.cmpAddg !== undefined) { if (this._draft.groups.length < GROUP_MAX) this._draft.groups.push({ label: letter(this._draft.groups.length), queries: [{}] }); return this._cmpRefresh(); }
            if (t.dataset.cmpRmg !== undefined) {
                if (this._draft.groups.length > 2) this._draft.groups.splice(Number(t.dataset.cmpRmg), 1);
                this._cmpRefresh();
                return this._cmpPreviewSoon();
            }
            if (t.dataset.cmpWithin !== undefined) { this._draft.within = Number(t.dataset.cmpWithin); this._cmpRefresh(); return this._cmpPreviewSoon(); }
            if (t.dataset.cmpClear !== undefined) { this._pin(); return this._set({ cmp: '', col_set: '', col_by: '', side: '', col_open: '', offset: 0 }); }
            if (t.dataset.set) {
                const i = t.dataset.set.indexOf(':'), key = t.dataset.set.slice(0, i);
                // leaving the metadata merge: the server keeps columns=by while col_by / col_set are set
                if (key === 'columns') this._pin();
                return this._set({ [key]: t.dataset.set.slice(i + 1), ...(key === 'columns' ? { col_by: '', col_set: '', cmp: '', col_pick: '', col_rest: '' } : {}), offset: 0 });
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
            if (t.dataset.lnames !== undefined) {
                this.state.lane_names = t.checked ? 'on' : 'off';
                this._lnSig = null;
                if (!t.checked) { this._laneNames = {}; this._laneEvid = {}; }
                if (this.onState) this.onState({ ...this.state });
                this.render();
            } else if (t.dataset.ev) {
                const ev = new Set((this.state.group_merge || '').split(',').filter(Boolean));
                if (t.checked) ev.add(t.dataset.ev); else ev.delete(t.dataset.ev);
                this._set({ group_merge: [...ev].join(','), offset: 0 });
            } else if (t.dataset.select) this._set({ [t.dataset.select]: t.value, ...(t.dataset.select === 'col_by' ? { col_pick: '', col_rest: '', side: '' } : {}), offset: 0 });
            else if (t.dataset.range) this._set({ [t.dataset.range]: t.value, offset: 0 });
            else if (t.dataset.text) this._set({ [t.dataset.text]: t.value.trim(), offset: 0 });
        }

        // Sliders recompute on release (change), not on every drag step.
        _onInput(e) {
            const t = e.target;
            if (t.dataset.cf) {
                this._cfSet(t.dataset.cf, t.value);
                this._cmpPreviewSoon();
                return;
            }
            if (t.dataset.tok) {
                clearTimeout(this._sugTimer);
                this._sugTimer = setTimeout(() => this._tokSuggest(t), 150);
                return;
            }
            const label = t.dataset.range && t.closest('label') && t.closest('label').querySelector('b');
            if (label) label.textContent = INT_RANGES.includes(t.dataset.range) ? t.value : Number(t.value).toFixed(2);
        }
    }

    NwayPanel.DEFAULTS = DEFAULTS;

    // Function page URL from a function id (the inline diff's header cards).
    NwayPanel.fnUrl = (fid, pool) => {
        const { md5 } = fnParts(fid);
        return fnUrl({ collection: fid.slice(0, fid.indexOf(':func:')), md5 }, fid, pool);
    };

    // Hover a grouped column's cell: the preview of its first function, and the rest in the "other functions" card.
    NwayPanel.hoverGroup = (el, e) => {
        let list = [];
        try { list = JSON.parse(el.dataset.gfns || '[]'); } catch (err) { /* no preview */ }
        if (!list.length || typeof window.showCodePreview !== 'function') return;
        const [a, ...rest] = list;
        window.showCodePreview(a.fid, a.name, a.addr, a.md5, a.feat, e, rest.length, a.file, rest);
    };

    NwayPanel.legend = el => {
        const p = NwayPanel._active;
        if (p) p._legOff = !el.open;
        el.parentNode.classList.toggle('off', !el.open);
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
        for (const k of ['axis', 'scope', 'min_edge', 'mode', 'columns', 'child_presence', 'twin_depth', 'twin_link']) if (q.has(k)) out.set(k, q.get(k));
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
    NwayPanel.urlForFunctions = function (fids, ref) {
        const qs = new URLSearchParams({ scope: 'functions' });
        if (ref) qs.set('fx_ref', ref); else qs.set('fx_fids', [...new Set(fids)].join(','));
        return `/diff/nway/function?${qs.toString()}`;
    };

    window.NwayPanel = NwayPanel;
})();
