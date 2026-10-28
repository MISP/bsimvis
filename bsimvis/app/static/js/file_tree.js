/**
 * file_tree.js -- the file search, drawn by its own browser instead of the
 * dashboard table. The dashboard still fetches, pages and keeps the URL; this
 * module owns everything drawn for the `files` view:
 *
 *  - a query bar (#search-area): free text plus `key:value` filter chips
 *  - a filter panel on the left, hidden with the header's Filters button
 *    (body.filters-collapsed, the same switch the table filter row used)
 *  - three layouts over one row model: cards, list + detail pane, lean table
 *
 * Grouping, picked by the `group` URL param:
 *  - default, by archive: every result sits under the archives (and packed
 *    binaries) it came out of. Each upload behind the page is loaded whole in
 *    two requests (root_md5 list, exact_md5 list), so a contained file is the
 *    same document as a search hit. While a filter is active, the files of an
 *    archive that did not match fold into one "N other files" line.
 *  - group=cluster: the loaded files grouped under their best cluster on one
 *    axis (group_axis, default code); group_nodes=container groups archives.
 *
 * `fold_tag` (comma separated, `lib:*` prefix globs) folds files carrying
 * those tags into one line per archive or cluster.
 *
 * Collapsing is client-side: every drawn item lists the bands, groups and
 * folds above it in data-ft-anc and is hidden while any of them is closed.
 * Bands and groups start open, folds start closed.
 */
window.FileTree = {
    SUBTREE_LIMIT: 2000,
    AXES: ['overall', 'code', 'library', 'content'],
    LAYOUTS: [
        ['cards', 'fa-solid fa-table-cells-large', 'Cards: one card per archive'],
        ['list', 'fa-solid fa-table-columns', 'List with a detail pane'],
        ['table', 'fa-solid fa-table-list', 'Compact table'],
    ],

    _items: [],
    _clusters: {},
    _uploads: new Map(),     // root md5 -> its docs, root included
    _nodes: new Map(),       // md5 -> doc, everything drawn
    _truncated: false,
    _rows: [],
    _params: new URLSearchParams(),
    _collection: '',
    _open: new Map(),        // id -> open, for what the user toggled
    _shut: new Set(),        // bands that start closed (nothing inside matched)
    _sel: null,              // md5 shown in the list layout's detail pane

    // --- filter fields ---------------------------------------------------
    // One table drives the query bar chips, the `key:value` syntax and the
    // side panel. `alias` is what a user types before the colon.

    FIELDS: [
        { param: 'file_name', label: 'Name', alias: ['name'], group: 'File', ac: 'file_name' },
        { param: 'language_id', label: 'Arch', alias: ['arch', 'lang'], group: 'File', ac: 'language_id' },
        { param: 'status', label: 'Status', alias: ['status'], group: 'File', options: ['pending', 'analyzing', 'failed', 'analyzed'] },
        { param: 'inferred_tags', label: 'Inferred tag', alias: ['inferred', 'inf'], group: 'File' },
        { param: 'strings', label: 'Has string', alias: ['string', 'str'], group: 'File' },
        { param: 'file_md5', label: 'MD5', alias: ['md5'], group: 'File', ac: 'file_md5' },
        { param: 'batch_uuid', label: 'Batch', alias: ['batch'], group: 'File', ac: 'batch_uuid' },
        { param: 'min_function_count', label: 'Funcs ≥', alias: ['funcs>=', 'funcs>'], group: 'Size', number: true },
        { param: 'max_function_count', label: 'Funcs ≤', alias: ['funcs<=', 'funcs<'], group: 'Size', number: true },
        { param: 'bin_cluster_name', label: 'Cluster', alias: ['cluster'], group: 'Clusters', ac: 'bin_cluster_name' },
        { param: 'bin_cluster_uuid', label: 'Cluster UUID', alias: ['cluster_uuid', 'uuid'], group: 'Clusters' },
        { param: 'min_cohesion', label: 'Cohesion ≥', alias: ['coh>=', 'coh>'], group: 'Clusters', number: true, step: 0.05, implicit: '0.5' },
        { param: 'max_cohesion', label: 'Cohesion ≤', alias: ['coh<=', 'coh<'], group: 'Clusters', number: true, step: 0.05 },
        { param: 'tag', label: 'Tag', alias: ['tag'], group: 'Tags', multi: true, tag: true },
        { param: 'exclude_tag', label: 'Not tag', alias: ['-tag', 'nottag'], group: 'Tags', multi: true, tag: true },
        { param: 'note_owner', label: 'Note by', alias: ['note'], group: 'Notes', ac: 'note_owners' },
        { param: 'min_entry_date', label: 'Added after', alias: ['added>=', 'added>', 'after'], group: 'Added', date: true },
        { param: 'max_entry_date', label: 'Added before', alias: ['added<=', 'added<', 'before'], group: 'Added', date: true },
        { param: 'fold_tag', label: 'Fold', alias: ['fold'], group: 'Display', multi: true, csv: true, tag: true },
    ],

    // Older links carry these spellings of the tag filters; read them as tags.
    _TAG_ALIASES: { file_tag: 'tag', exclude_file_tag: 'exclude_tag' },

    // --- preferences (per viewer) ----------------------------------------

    _pref(key, fallback) {
        try { return localStorage.getItem(key) || fallback; } catch (e) { return fallback; }
    },
    _setPref(key, value) {
        try { localStorage.setItem(key, value); } catch (e) { /* per-viewer only */ }
    },
    layout() {
        const l = this._pref('fileLayout', 'cards');
        return this.LAYOUTS.some(x => x[0] === l) ? l : 'cards';
    },
    setLayout(l) {
        this._setPref('fileLayout', l);
        this._paintToolbar();
        this._paint();
    },
    extraCols() {
        return this._pref('fileTreeExtraCols', '0') === '1';
    },
    setExtraCols(on) {
        this._setPref('fileTreeExtraCols', on ? '1' : '0');
        this._paintToolbar();
        this._paint();
    },

    // --- URL params ------------------------------------------------------

    _navigate(mutate) {
        const { viewKey, params } = getRoutingState();
        mutate(params);
        currentOffset = 0;
        isEndOfResults = false;
        navigate(viewKey, params);
    },

    setParam(key, value) {
        this._navigate(p => { if (value) p.set(key, value); else p.delete(key); });
    },

    _field(param) {
        return this.FIELDS.find(f => f.param === param);
    },

    /** Values of a field as the user sees them (unquoted, dates as YYYY-MM-DD). */
    _values(params, field) {
        let raw = params.getAll(field.param);
        Object.entries(this._TAG_ALIASES).forEach(([old, to]) => { if (to === field.param) raw = raw.concat(params.getAll(old)); });
        if (field.csv) raw = raw.flatMap(v => v.split(','));
        return raw.map(v => String(v).trim()).filter(Boolean).map(v =>
            field.tag ? unquoteFilterValue(v).value : field.date ? this._day(v) : v);
    },

    _encode(field, value) {
        value = String(value).trim();
        if (field.date) {
            const ms = Date.parse(value);
            return isNaN(ms) ? value : String(Math.floor(ms / 1000));
        }
        if (field.tag && !field.csv) return quoteFilterValue(value, !value.includes('*'));
        return value;
    },

    addValue(param, value) {
        const field = this._field(param);
        if (!field || !String(value || '').trim()) return;
        this._navigate(p => {
            if (field.csv) {
                const cur = this._values(p, field);
                if (!cur.includes(value.trim())) p.set(param, [...cur, value.trim()].join(','));
            } else if (field.multi) {
                p.append(param, this._encode(field, value));
            } else {
                p.set(param, this._encode(field, value));
            }
        });
    },

    removeValue(param, value) {
        const field = this._field(param);
        if (!field) return;
        this._navigate(p => {
            if (!field.multi) { p.delete(param); return; }
            const keep = this._values(p, field).filter(v => v !== value);
            [param, ...Object.keys(this._TAG_ALIASES).filter(k => this._TAG_ALIASES[k] === param)].forEach(k => p.delete(k));
            if (field.csv) { if (keep.length) p.set(param, keep.join(',')); }
            else keep.forEach(v => p.append(param, this._encode(field, v)));
        });
    },

    // --- query bar -------------------------------------------------------

    queryBarHtml(params) {
        return `<div class="fb-qbar" id="fb-qbar">${this._qbarInner(params)}</div>`;
    },

    _qbarInner(params) {
        const chips = [];
        for (const field of this.FIELDS) {
            for (const v of this._values(params, field)) {
                if (field.implicit && v === field.implicit) continue;
                chips.push(`<span class="fb-chip${field.param === 'fold_tag' ? ' fb-chip-fold' : ''}${field.param === 'exclude_tag' ? ' fb-chip-not' : ''}">
                    ${field.param === 'fold_tag' ? '<i class="fa-solid fa-compress"></i>' : ''}<span class="fb-chip-k">${escapeHtml(field.label)}</span>
                    <span class="fb-chip-v">${escapeHtml(v)}</span>
                    <button title="Remove" onclick="FileTree.removeValue(${escapeAttr(jsString(field.param))}, ${escapeAttr(jsString(v))})"><i class="fa-solid fa-xmark"></i></button></span>`);
            }
        }
        const menu = this.FIELDS.map(f => `<button onclick="FileTree._startField(${escapeAttr(jsString(f.alias[0]))})">
            <span>${escapeHtml(f.label)}</span><code>${escapeHtml(f.alias[0])}${/[<>=]$/.test(f.alias[0]) ? '' : ':'}</code></button>`).join('');
        return `
            <i class="fa-solid fa-magnifying-glass fb-qbar-icon"></i>
            ${chips.join('')}
            <input type="text" id="file-search-input" class="fb-qbar-input" autocomplete="off"
                placeholder="${chips.length ? 'Refine…' : 'Search files…  or  tag:mirai  funcs>=100  arch:ARM'}"
                value="${escapeAttr(params.get('q') || '')}" onkeydown="FileTree._queryKey(event, this)">
            <div class="fb-addfilter">
                <button class="fb-addfilter-btn" onclick="this.parentElement.classList.toggle('open')" title="Add a filter">
                    <i class="fa-solid fa-plus"></i> Filter</button>
                <div class="fb-addfilter-menu">${menu}</div>
            </div>`;
    },

    _startField(alias) {
        document.querySelector('.fb-addfilter')?.classList.remove('open');
        const input = document.getElementById('file-search-input');
        if (!input) return;
        input.value = /[<>=]$/.test(alias) ? alias : `${alias}:`;
        input.focus();
    },

    /** Enter: `key:value` (or funcs>=100) becomes a filter, anything else the free-text search. */
    _queryKey(event, input) {
        if (event.key === 'Backspace' && !input.value) {
            const last = [...document.querySelectorAll('#fb-qbar .fb-chip button')].pop();
            if (last) last.click();
            return;
        }
        if (event.key !== 'Enter') return;
        event.preventDefault();
        const text = input.value.trim();
        const m = text.match(/^(-?[a-z_]+)\s*(>=|<=|>|<|:)\s*(.+)$/i);
        if (m) {
            const key = (m[2] === ':' ? m[1] : m[1] + m[2]).toLowerCase();
            const field = this.FIELDS.find(f => f.alias.includes(key));
            if (field) {
                const { params } = getRoutingState();
                this._navigate(p => {
                    if (p.get('q') === params.get('q')) p.delete('q');
                    if (field.csv) {
                        const cur = this._values(p, field);
                        if (!cur.includes(m[3].trim())) p.set(field.param, [...cur, m[3].trim()].join(','));
                    } else if (field.multi) p.append(field.param, this._encode(field, m[3]));
                    else p.set(field.param, this._encode(field, m[3]));
                });
                return;
            }
        }
        this.setParam('q', text);
    },

    // --- side panel ------------------------------------------------------

    _panelHtml(params) {
        const groups = [...new Set(this.FIELDS.map(f => f.group))];
        const input = f => {
            const val = this._values(params, f)[0] || '';
            const common = `data-param="${f.param}" onchange="FileTree._panelChange(this)" onkeydown="if (event.key === 'Enter') this.blur()"`;
            if (f.options) {
                return `<select ${common}><option value="">Any</option>${f.options.map(o =>
                    `<option value="${o}"${o === val ? ' selected' : ''}>${o}</option>`).join('')}</select>`;
            }
            if (f.date) return `<input type="date" ${common} value="${escapeAttr(val)}">`;
            if (f.number) return `<input type="number" ${common} step="${f.step || 1}" min="0" value="${escapeAttr(val)}" placeholder="${f.implicit || ''}">`;
            const ac = f.ac ? `onfocus="attachAutocomplete(this, 'file', '${f.ac}', (v) => { this.value = v; FileTree._panelChange(this); })"` : '';
            return `<input type="text" ${common} ${ac} value="${escapeAttr(val)}" placeholder="${escapeAttr(f.label)}…" autocomplete="off">`;
        };
        const multi = f => {
            const vals = this._values(params, f);
            return `<div class="fb-multi">
                ${vals.map(v => `<span class="fb-chip${f.param === 'exclude_tag' ? ' fb-chip-not' : ''}"><span class="fb-chip-v">${escapeHtml(v)}</span>
                    <button onclick="FileTree.removeValue(${escapeAttr(jsString(f.param))}, ${escapeAttr(jsString(v))})"><i class="fa-solid fa-xmark"></i></button></span>`).join('')}
                <input type="text" placeholder="+ ${escapeAttr(f.label.toLowerCase())}" autocomplete="off"
                    onkeydown="if (event.key === 'Enter') { FileTree.addValue('${f.param}', this.value); }"
                    onfocus="attachTagAutocomplete(this, (v) => FileTree.addValue('${f.param}', v))">
            </div>`;
        };
        const axes = () => {
            const on = new Set((params.get('bin_cluster_axes') || '').split(',').filter(Boolean));
            return `<div class="fb-axes">${this.AXES.map(a => {
                const meta = (window.BinSimScoreTypes || {})[a === 'overall' ? 'score' : `score_${a}`] || {};
                return `<button class="${on.has(a) ? 'active' : ''}" style="--ft-axis:${meta.color || 'var(--accent)'}" title="Show ${a} clusters only"
                    onclick="toggleBinClusterAxisFilter('${a}')"><i class="${meta.icon || 'fa-solid fa-layer-group'}"></i>${meta.label || a}</button>`;
            }).join('')}</div>`;
        };
        const body = groups.map(g => {
            const fields = this.FIELDS.filter(f => f.group === g);
            const rows = fields.map(f => `<label class="fb-field"><span>${escapeHtml(f.label)}</span>${f.multi ? multi(f) : input(f)}</label>`).join('');
            return `<section class="fb-sec"><h4>${escapeHtml(g)}</h4>${rows}${g === 'Clusters' ? `<label class="fb-field"><span>Axes</span>${axes()}</label>` : ''}</section>`;
        }).join('');
        return `<div class="fb-panel-head"><span><i class="fa-solid fa-sliders"></i> Filters</span>
                <button onclick="clearFilters()" title="Clear every filter">Clear</button></div>${body}`;
    },

    _panelChange(el) {
        const field = this._field(el.dataset.param);
        if (!field) return;
        const v = el.value.trim();
        this.setParam(field.param, v ? this._encode(field, v) : '');
    },

    // --- toolbar ---------------------------------------------------------

    toolbarHtml(params) {
        this._params = new URLSearchParams(params);
        return `<div class="ft-toolbar" id="ft-toolbar">${this._toolbarInner(params)}</div>`;
    },

    _paintToolbar() {
        const el = document.getElementById('ft-toolbar');
        if (el) el.innerHTML = this._toolbarInner(this._params);
    },

    _toolbarInner(params) {
        const mode = params.get('group') === 'cluster' ? 'cluster' : 'archive';
        const axis = params.get('group_axis') || 'code';
        const nodes = params.get('group_nodes') || 'file';
        const layout = this.layout();
        const types = window.BinSimScoreTypes || {};
        const seg = items => `<div class="ft-seg">${items.join('')}</div>`;
        const btn = (on, onclick, icon, label, title, style = '') =>
            `<button class="ft-seg-btn${on ? ' active' : ''}" onclick="${onclick}" title="${escapeAttr(title)}"${style ? ` style="${style}"` : ''}>
                <i class="${icon}"></i>${label ? `<span>${label}</span>` : ''}</button>`;

        const layouts = seg(this.LAYOUTS.map(([k, icon, title]) => btn(layout === k, `FileTree.setLayout('${k}')`, icon, '', title)));
        const group = seg([
            btn(mode === 'archive', "FileTree.setParam('group', '')", 'fa-solid fa-box-archive', 'Archives',
                'Nest every result under the archives and packed binaries it came out of'),
            btn(mode === 'cluster', "FileTree.setParam('group', 'cluster')", 'fa-solid fa-circle-nodes', 'Clusters',
                'Group the loaded files by their best cluster'),
        ]);
        let modeControls;
        if (mode === 'cluster') {
            modeControls = seg(this.AXES.map(a => {
                const meta = types[a === 'overall' ? 'score' : `score_${a}`] || {};
                return btn(axis === a, `FileTree.setParam('group_axis', '${a}')`, meta.icon || 'fa-solid fa-layer-group', '',
                    `Group by ${meta.label || a} clusters`, `--ft-axis:${meta.color || 'var(--accent)'}`);
            })) + seg([
                btn(nodes === 'file', "FileTree.setParam('group_nodes', '')", 'fa-regular fa-file', '', 'Group files by their file clusters'),
                btn(nodes === 'container', "FileTree.setParam('group_nodes', 'container')", 'fa-solid fa-box-archive', '', 'Group archives by their archive clusters'),
            ]);
        } else {
            modeControls = seg([
                btn(false, 'FileTree.setAllOpen(true)', 'fa-solid fa-angles-down', '', 'Expand every archive'),
                btn(false, 'FileTree.setAllOpen(false)', 'fa-solid fa-angles-up', '', 'Collapse every archive'),
            ]);
        }
        const sortBy = params.get('sort_by') || 'file_name';
        const order = params.get('sort_order') || 'asc';
        const sort = `<div class="ft-seg ft-sort" title="Sort">
            <select onchange="FileTree.setParam('sort_by', this.value)">
                ${[['file_name', 'Name'], ['function_count', 'Functions'], ['entry_date', 'Added']].map(([v, l]) =>
                    `<option value="${v}"${v === sortBy ? ' selected' : ''}>${l}</option>`).join('')}
            </select>
            ${btn(false, `FileTree.setParam('sort_order', '${order === 'asc' ? 'desc' : 'asc'}')`,
                `fa-solid fa-arrow-${order === 'asc' ? 'up-short-wide' : 'down-wide-short'}`, '', order === 'asc' ? 'Ascending' : 'Descending')}
        </div>`;
        const extra = layout === 'table' ? seg([btn(this.extraCols(), `FileTree.setExtraCols(${!this.extraCols()})`, 'fa-solid fa-hashtag', 'MD5',
            this.extraCols() ? 'Hide the MD5 and batch columns' : 'Show the MD5 and batch columns')]) : '';
        return `${layouts}${group}${modeControls}${sort}${extra}`;
    },

    /** Open or close every archive band / cluster group at once. Folds stay as they are. */
    setAllOpen(open) {
        this._rows.forEach(r => { if (r.t === 'band' || r.t === 'group') this._open.set(r.id, open); });
        this.applyVisibility();
    },

    // --- mount -----------------------------------------------------------

    /** Swap the dashboard table for the browser (files view) and back. */
    mount(on) {
        const wrap = document.getElementById('table-body-wrap');
        if (!wrap) return;
        let host = document.getElementById('file-browser');
        if (on && !host) {
            host = document.createElement('div');
            host.id = 'file-browser';
            host.innerHTML = `<aside id="fb-panel" class="fb-panel"></aside><main id="fb-results" class="fb-results"></main>`;
            wrap.insertBefore(host, document.getElementById('data-table'));
        }
        if (host) host.style.display = on ? '' : 'none';
        const head = document.getElementById('table-header-wrap');
        const table = document.getElementById('data-table');
        if (head) head.style.display = on ? 'none' : '';
        if (table) table.style.display = on ? 'none' : '';
        wrap.classList.toggle('fb-mounted', on);
    },

    // --- entry point -----------------------------------------------------

    /**
     * Everything loaded so far. Appending a page re-draws the whole tree, so
     * a file that arrives on page 2 joins its archive from page 1 instead of
     * starting a second copy of it. `stale()` says a newer request took over.
     */
    async render(items, clustersMap, { append, params, collection, pool, stale }) {
        if (!append) {
            this._items = [];
            this._clusters = {};
            this._uploads = new Map();
            this._truncated = false;
        }
        this._shut = new Set();
        this._items.push(...items);
        Object.assign(this._clusters, clustersMap || {});
        this._params = new URLSearchParams(params);
        this._collection = collection;

        const ctx = {
            params,
            folds: this._values(params, this._field('fold_tag')).map(t => t.toLowerCase()),
            filtered: this._isFiltered(params),
        };
        const rows = params.get('group') === 'cluster'
            ? this._byCluster(ctx)
            : await this._byArchive(ctx, collection, !!pool);
        if (stale && stale()) return;
        this._rows = rows;
        const qbar = document.getElementById('fb-qbar');
        if (qbar) qbar.innerHTML = this._qbarInner(params);
        const panel = document.getElementById('fb-panel');
        if (panel) panel.innerHTML = this._panelHtml(params);
        this._paint();
    },

    // --- tree model ------------------------------------------------------
    // Rows: { t: 'band' | 'file' | 'fold' | 'note' | 'group', depth, anc, ... }

    _LAYOUT_PARAMS: new Set(['collection', 'pool', 'limit', 'offset', 'sort_by', 'sort_order', 'view',
        'group', 'group_axis', 'group_nodes', 'fold_tag', 'min_cohesion', 'bin_cluster_axes', 'algo']),

    _isFiltered(params) {
        for (const [k, v] of params.entries()) {
            if (v && !this._LAYOUT_PARAMS.has(k)) return true;
        }
        return false;
    },

    _tags(f) {
        return [...(f.tags || []), ...(f.user_tags || [])].map(t => String(t).toLowerCase());
    },

    _matchFold(t, folds) {
        return folds.some(p => p.endsWith('*') ? t.startsWith(p.slice(0, -1)) : t === p);
    },

    _folded(f, folds) {
        return folds.length > 0 && this._tags(f).some(t => this._matchFold(t, folds));
    },

    _foldLabel(files, folds) {
        const tags = [...new Set(files.flatMap(f => this._tags(f).filter(t => this._matchFold(t, folds))))];
        const funcs = files.reduce((s, f) => s + (Number(f.function_count) || 0), 0);
        return `<b>${files.length} file${files.length === 1 ? '' : 's'} tagged ${escapeHtml(tags.slice(0, 4).join(', '))}${tags.length > 4 ? '…' : ''}</b>
            <span>· ${funcs.toLocaleString()} functions</span>`;
    },

    _split(path) {
        const parts = String(path || '').split('/');
        const base = parts.pop();
        return { dir: parts.length ? parts.join('/') + '/' : '', base };
    },

    _fetchFiles(params, extra) {
        const q = new URLSearchParams(extra);
        for (const k of ['collection', 'min_cohesion', 'algo']) if (params.get(k)) q.set(k, params.get(k));
        return fetch(`/api/file/search?${q}`).then(r => r.ok ? r.json() : { files: [] }).catch(e => {
            console.error(e);
            return { files: [] };
        });
    },

    /**
     * Every upload behind the page, whole: each root that is not on the page
     * plus every file under it. Two requests however many uploads there are;
     * an upload loaded for an earlier page is not asked for again.
     */
    async _loadUploads(params, collection, roots, hits) {
        const missing = roots.filter(r => !this._uploads.has(r));
        if (missing.length) {
            const absent = missing.filter(r => !hits.has(r));
            const [sub, self] = await Promise.all([
                this._fetchFiles(params, { collection, root_md5: missing.join(','), limit: this.SUBTREE_LIMIT, sort_by: 'file_name', sort_order: 'asc' }),
                absent.length ? this._fetchFiles(params, { collection, exact_md5: absent.join(','), limit: absent.length }) : null,
            ]);
            Object.assign(this._clusters, sub.bin_cluster_map || {}, (self && self.bin_cluster_map) || {});
            missing.forEach(r => this._uploads.set(r, []));
            for (const d of [...((self && self.files) || []), ...(sub.files || [])]) {
                const r = d.root_md5 || d.file_md5;
                if (this._uploads.has(r)) this._uploads.get(r).push(d);
            }
            if ((sub.total || 0) > (sub.files || []).length) this._truncated = true;
        }
        return roots.flatMap(r => this._uploads.get(r) || []);
    },

    async _byArchive(ctx, collection, pool) {
        const hits = new Map(this._items.map(f => [f.file_md5, f]));
        const roots = [];
        for (const f of this._items) {
            const root = f.root_md5 || (f.child_count ? f.file_md5 : null);
            if (root && !roots.includes(root)) roots.push(root);
        }
        // ponytail: root_md5 is indexed per collection; a pool search nests
        // only what is on the page until pools get the same lookup.
        const docs = pool ? [] : await this._loadUploads(ctx.params, collection, roots, hits);

        // Page hits win over the subtree copies: same doc, but they are what matched.
        const nodes = new Map();
        docs.forEach(d => { if (!nodes.has(d.file_md5)) nodes.set(d.file_md5, d); });
        hits.forEach((f, md5) => nodes.set(md5, f));

        const kids = new Map();
        for (const n of [...nodes.values()]) {
            const p = n.parent_md5;
            if (!p || p === n.file_md5) continue;
            if (!nodes.has(p)) {
                // Declared as a container but never uploaded: a band with a name only.
                nodes.set(p, { file_md5: p, file_name: n.parent_file_name || p, exists: false, is_container: true, collection: n.collection || collection });
            }
            if (!kids.has(p)) kids.set(p, []);
            kids.get(p).push(n);
        }
        this._nodes = nodes;
        const byPath = (a, b) => (a.path_in_parent || a.file_name || '').localeCompare(b.path_in_parent || b.file_name || '');
        kids.forEach(list => list.sort((a, b) => (kids.has(a.file_md5) - kids.has(b.file_md5)) || byPath(a, b)));

        const hitBelow = new Map();
        const hasHit = n => {
            if (!hitBelow.has(n.file_md5)) {
                hitBelow.set(n.file_md5, false);  // cycle guard
                hitBelow.set(n.file_md5, hits.has(n.file_md5) || (kids.get(n.file_md5) || []).some(hasHit));
            }
            return hitBelow.get(n.file_md5);
        };
        const drawn = new Set();
        const rows = this._truncated ? [{ t: 'note', depth: 0, anc: '',
            html: `Archive contents are capped at ${this.SUBTREE_LIMIT.toLocaleString()} files per load, so some archives are partial. Narrow the search to see them whole.` }] : [];

        // `filtered` drops to false inside an "other files" fold: context the
        // analyst opened on purpose is shown whole, not folded again.
        const draw = (n, depth, anc, parent, filtered = ctx.filtered) => {
            if (drawn.has(n.file_md5)) return;
            drawn.add(n.file_md5);
            const path = this._split(parent ? (n.path_in_parent || n.file_name) : n.file_name);
            const row = { n, depth, anc: anc.join(' '), prefix: parent ? path.dir : '', base: parent ? path.base : (n.file_name || n.file_md5),
                unpacked: !!parent && !parent.is_container, parentName: parent ? parent.file_name : '' };
            const children = kids.get(n.file_md5);
            if (!children) {
                if (n.exists !== false) rows.push({ t: 'file', ...row });
                return;
            }
            const id = `b:${n.file_md5}`;
            const files = this._descendants(n, kids);
            const inner = [...anc, id];
            const shown = [], tagged = [], other = [];
            for (const c of children) {
                if (filtered && !hasHit(c)) other.push(c);
                else if (!kids.has(c.file_md5) && this._folded(c, ctx.folds) && !(filtered && hits.has(c.file_md5))) tagged.push(c);
                else shown.push(c);
            }
            // Nothing inside matched: the band itself is the fold. It starts
            // closed, instead of an open band around a closed "N other files" line.
            const noneMatched = filtered && other.length === children.length;
            if (noneMatched) this._shut.add(id);
            rows.push({ t: 'band', ...row, id, count: files.length, noneMatched, context: filtered && !hits.has(n.file_md5) });
            if (noneMatched) {
                other.forEach(c => draw(c, depth + 1, inner, n, false));
                return;
            }
            shown.forEach(c => draw(c, depth + 1, inner, n, filtered));
            if (tagged.length) {
                const fid = `f:${n.file_md5}:tag`;
                rows.push({ t: 'fold', id: fid, depth: depth + 1, anc: inner.join(' '), html: this._foldLabel(tagged, ctx.folds) });
                tagged.forEach(c => draw(c, depth + 1, [...inner, fid], n, filtered));
            }
            if (other.length) {
                const fid = `f:${n.file_md5}:other`;
                const count = other.reduce((s, c) => s + (kids.has(c.file_md5) ? this._descendants(c, kids).length + 1 : 1), 0);
                rows.push({ t: 'fold', id: fid, depth: depth + 1, anc: inner.join(' '),
                    html: `<b>${count} other file${count === 1 ? '' : 's'}</b> <span>in this ${n.is_container ? 'archive' : 'binary'} did not match</span>` });
                other.forEach(c => draw(c, depth + 1, [...inner, fid], n, false));
            }
        };

        for (const f of this._items) {
            let top = f;
            const seen = new Set();
            while (top.parent_md5 && nodes.has(top.parent_md5) && !seen.has(top.file_md5)) {
                seen.add(top.file_md5);
                top = nodes.get(top.parent_md5);
            }
            draw(top, 0, [], null);
        }
        return rows;
    },

    _descendants(n, kids) {
        const out = [];
        const walk = m => (kids.get(m.file_md5) || []).forEach(c => {
            if (out.includes(c)) return;
            if (!c.is_container && c.exists !== false) out.push(c);
            walk(c);
        });
        walk(n);
        return out;
    },

    _byCluster(ctx) {
        const axis = ctx.params.get('group_axis') || 'code';
        const wantContainers = ctx.params.get('group_nodes') === 'container';
        const items = this._items.filter(f => !!f.is_container === wantContainers);
        this._nodes = new Map(this._items.map(f => [f.file_md5, f]));

        const groups = new Map();
        const none = [];
        for (const f of items) {
            const ids = Array.isArray(f.bin_clusters) ? f.bin_clusters : [];
            const best = ids.map(u => this._clusters[u])
                .filter(c => c && (c.axis || 'overall') === axis)
                .sort((a, b) => (b.cohesion_score || 0) - (a.cohesion_score || 0))[0];
            if (!best) { none.push(f); continue; }
            if (!groups.has(best.cluster_uuid)) groups.set(best.cluster_uuid, []);
            groups.get(best.cluster_uuid).push(f);
        }
        const order = [...groups.entries()].sort((a, b) =>
            (b[1].length - a[1].length) || ((this._clusters[b[0]].cohesion_score || 0) - (this._clusters[a[0]].cohesion_score || 0)));
        if (none.length) order.push([null, none]);

        const rows = [];
        for (const [uuid, files] of order) {
            const id = `g:${uuid || 'none'}`;
            const c = uuid ? this._clusters[uuid] : null;
            const funcs = files.reduce((s, f) => s + (Number(f.function_count) || 0), 0);
            rows.push({ t: 'group', id, depth: 0, anc: '', cluster: c, uuid, axis, count: files.length, funcs });
            const shown = [], tagged = [];
            files.forEach(f => (this._folded(f, ctx.folds) ? tagged : shown).push(f));
            const fileRow = (f, anc) => {
                const p = this._split(f.parent_md5 ? (f.path_in_parent || f.file_name) : f.file_name);
                return { t: 'file', n: f, depth: 1, anc, prefix: f.parent_md5 ? p.dir : '', base: f.parent_md5 ? p.base : f.file_name,
                    parentName: f.parent_md5 ? (f.parent_file_name || f.parent_md5) : '' };
            };
            shown.forEach(f => rows.push(fileRow(f, id)));
            if (tagged.length) {
                const fid = `f:${id}:tag`;
                rows.push({ t: 'fold', id: fid, depth: 1, anc: id, html: this._foldLabel(tagged, ctx.folds) });
                tagged.forEach(f => rows.push(fileRow(f, `${id} ${fid}`)));
            }
        }
        if (rows.length) {
            // ponytail: groups come from the loaded page; a cluster bigger than
            // a page splits. Page by cluster via the bin-cluster members route
            // if that gets in the way.
            rows.push({ t: 'note', depth: 0, anc: '', html: `Grouped from the ${this._items.length.toLocaleString()} result${this._items.length === 1 ? '' : 's'} loaded so far.` });
        }
        return rows;
    },

    // --- shared cell pieces ----------------------------------------------

    _col(n) {
        return n.collection || (n.file_id || '').split(':')[0] || this._collection;
    },

    _entityAttrs(n) {
        const col = this._col(n);
        const fileId = n.file_id || `${col}:file:${n.file_md5}`;
        return `data-id="${escapeAttr(fileId)}" data-md5="${escapeAttr(n.file_md5)}"
            data-entity-data='${escapeAttr(JSON.stringify({ md5: n.file_md5, file_name: n.file_name, id: fileId, collection: col }))}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)"`;
    },

    _day(v) {
        const d = new Date(toEpochMs(v));
        return isNaN(d) ? String(v) : d.toISOString().slice(0, 10);
    },

    _funcs(n) {
        return Number(n.function_count || 0).toLocaleString();
    },

    _clustersOf(n) {
        return (Array.isArray(n.bin_clusters) ? n.bin_clusters : []).map(u => this._clusters[u]).filter(Boolean);
    },

    _clusterCards(n) {
        const axes = (this._params.get('bin_cluster_axes') || '').split(',').filter(Boolean);
        return EntityRenderer.renderClusterCard(this._clustersOf(n), true, axes.length ? new Set(axes) : null);
    },

    _tagEditor(n, max) {
        const col = this._col(n);
        return EntityRenderer.renderTag('file', n.file_id || `${col}:file:${n.file_md5}`, n.tags || [], n.user_tags || [], { maxTags: max });
    },

    _noteBtn(n) {
        const col = this._col(n);
        return EntityRenderer.renderFileNoteButton(n.file_id || `${col}:file:${n.file_md5}`, n.note_owners, { isTable: true, raw_data: n });
    },

    _icon(r) {
        if (r.t !== 'band') return r.unpacked
            ? '<i class="fa-solid fa-box-open fb-ic fb-ic-unp" title="Unpacked from the packed binary above"></i>'
            : '<i class="fa-regular fa-file fb-ic"></i>';
        return r.n.is_container
            ? '<i class="fa-solid fa-box-archive fb-ic fb-ic-arc" title="Archive: holds code but is not code itself"></i>'
            : '<i class="fa-solid fa-cube fb-ic fb-ic-packed" title="Packed binary: analysed itself, and unpacked into the files below"></i>';
    },

    _kind(r) {
        if (r.t !== 'band') return r.unpacked ? '<span class="fb-kind fb-kind-unp">unpacked</span>' : '';
        return `<span class="fb-kind${r.n.is_container ? '' : ' fb-kind-packed'}">${escapeHtml(r.n.is_container ? (r.n.filetype || 'archive') : 'packed')}</span>`;
    },

    _name(r, { withParent = false } = {}) {
        const n = r.n;
        const parent = withParent && r.parentName ? `<span class="fb-prefix">${escapeHtml(r.parentName)} › </span>` : '';
        const prefix = r.prefix ? `<span class="fb-prefix">${escapeHtml(r.prefix)}</span>` : '';
        if (n.exists === false) return `<span class="fb-name">${parent}${Lineage.nodeName(n, this._col(n), { max: 60 })}</span>`;
        return `<span class="fb-name" title="${escapeAttr(n.file_name || '')}">${parent}${prefix}<a class="fb-link"
            onclick="event.stopPropagation(); openFileDetails(${escapeAttr(jsString(this._col(n)))}, ${escapeAttr(jsString(n.file_md5))}, ${escapeAttr(jsString(n.file_name || ''))}, event)">${escapeHtml(r.base || n.file_name || n.file_md5)}</a></span>`;
    },

    /** type · arch · first seen · status (only while not simply analyzed). */
    _meta(r, { inside = true } = {}) {
        const n = r.n;
        const ft = Array.isArray(n.filetype) ? n.filetype.join(', ') : (n.filetype || '');
        const seen = [].concat(n.first_seen || []).filter(Boolean).map(v => this._day(v)).join(', ');
        const busy = (n.status && n.status !== 'analyzed') || (window.activeJobsByTarget && window.activeJobsByTarget[n.file_md5]);
        const parts = [
            inside && r.t === 'band' ? `<span class="fb-meta-strong">${r.count} file${r.count === 1 ? '' : 's'} inside${r.noneMatched ? ' · none matched' : r.context ? ' · context' : ''}</span>` : '',
            ft && !(r.t === 'band' && n.is_container) ? `<span class="fb-meta-type" title="${escapeAttr(ft)}">${escapeHtml(ft)}</span>` : '',
            n.language_id ? `<span class="fb-mono">${escapeHtml(n.language_id)}</span>` : '',
            seen ? `<span title="First seen">seen ${escapeHtml(seen)}</span>` : '',
            n.entry_date ? `<span title="Added ${escapeAttr(formatDate(n.entry_date))}">added ${escapeHtml(this._day(n.entry_date))}</span>` : '',
            busy ? fileStatusBadgeHtml(n) : '',
        ].filter(Boolean);
        return parts.length ? `<div class="fb-meta">${parts.join('<i class="fb-dot"></i>')}</div>` : '';
    },

    _chev(id, open = true) {
        return `<button class="fb-chev" data-ft-toggle="${escapeAttr(id)}" onclick="FileTree.toggle(event, ${escapeAttr(jsString(id))})" title="Collapse or expand">
            <i class="fa-solid fa-chevron-${open ? 'down' : 'right'}"></i></button>`;
    },

    _groupHead(r) {
        const c = r.cluster;
        if (!c) return `<div class="fb-group-title"><b>No ${escapeHtml(r.axis)} cluster</b></div>
            <div class="fb-meta"><span>${r.count} on this page</span><i class="fb-dot"></i><span>${r.funcs.toLocaleString()} functions</span></div>`;
        return `<div class="fb-group-title">${renderClusterCards([c], true)}
                <a class="fb-link" onclick="event.stopPropagation(); openCluster(${escapeAttr(jsString(r.uuid))}, true, event, ${escapeAttr(jsString(r.axis))})">${escapeHtml(c.cluster_name || `Cluster ${c.cluster_id}`)}</a></div>
            <div class="fb-meta"><span class="fb-meta-strong">${r.count} on this page</span><i class="fb-dot"></i><span>${Number(c.member_count || 0)} members</span>
                <i class="fb-dot"></i><span>cohesion ${(c.cohesion_score || 0).toFixed(2)}</span><i class="fb-dot"></i><span>${r.funcs.toLocaleString()} functions</span></div>`;
    },

    // --- painting --------------------------------------------------------

    _paint() {
        const host = document.getElementById('fb-results');
        if (!host) return;
        const layout = this.layout();
        host.dataset.layout = layout;
        if (!this._rows.length) {
            host.innerHTML = `<div class="fb-empty"><i class="fa-regular fa-folder-open"></i><p>No files match.</p>
                <button onclick="clearFilters()">Clear filters</button></div>`;
            return;
        }
        host.innerHTML = layout === 'table' ? this._paintTable() : layout === 'list' ? this._paintList() : this._paintCards();
        this.applyVisibility();
        if (layout === 'list') this._paintDetail();
    },

    applyVisibility() {
        const host = document.getElementById('fb-results');
        if (!host) return;
        const closed = id => !this._isOpen(id);
        host.querySelectorAll('[data-ft-anc]').forEach(el => {
            const anc = el.dataset.ftAnc;
            el.style.display = anc && anc.split(' ').some(closed) ? 'none' : '';
        });
        host.querySelectorAll('[data-ft-toggle]').forEach(el => {
            const icon = el.querySelector('i');
            if (icon) icon.className = `fa-solid fa-chevron-${closed(el.dataset.ftToggle) ? 'right' : 'down'}`;
        });
        host.querySelectorAll('[data-ft-card]').forEach(el => el.classList.toggle('fb-collapsed', closed(el.dataset.ftCard)));
    },

    /** Folds and the bands in _shut start closed; everything else starts open. */
    _isOpen(id) {
        if (this._open.has(id)) return this._open.get(id);
        return !id.startsWith('f:') && !this._shut.has(id);
    },

    toggle(event, id) {
        event.stopPropagation();
        this._open.set(id, !this._isOpen(id));
        this.applyVisibility();
    },

    _foldLine(r, indent) {
        return `<div class="fb-fold" data-ft-anc="${escapeAttr(r.anc)}" style="--indent:${indent}" onclick="FileTree.toggle(event, ${escapeAttr(jsString(r.id))})">
            <span class="fb-chev" data-ft-toggle="${escapeAttr(r.id)}"><i class="fa-solid fa-chevron-right"></i></span>${r.html}</div>`;
    },

    _noteLine(r) {
        return `<div class="fb-note" data-ft-anc="${escapeAttr(r.anc)}"><i class="fa-solid fa-circle-info"></i> ${r.html}</div>`;
    },

    // Cards: an archive (or a cluster group) per card, its files inside.
    _paintCards() {
        const blocks = [];
        let card = null;
        const close = () => { if (card) blocks.push(card); card = null; };
        for (const r of this._rows) {
            if (r.t === 'note') { close(); blocks.push({ note: r }); continue; }
            if (r.depth === 0) {
                close();
                if (r.t === 'file') { blocks.push({ loose: r }); continue; }
                card = { head: r, body: [] };
                continue;
            }
            if (card) card.body.push(r);
        }
        close();

        const line = r => {
            const indent = Math.max(0, r.depth - 1);
            if (r.t === 'fold') return this._foldLine(r, indent);
            if (r.t === 'note') return this._noteLine(r);
            const n = r.n;
            const head = r.t === 'band';
            return `<div class="fb-line${head ? ' fb-line-band' : ''}" data-ft-anc="${escapeAttr(r.anc)}" style="--indent:${indent}" ${this._entityAttrs(n)}>
                <div class="fb-line-main">
                    ${head ? this._chev(r.id) : ''}${this._icon(r)}${this._name(r, { withParent: this._params.get('group') === 'cluster' })}${this._kind(r)}
                    <span class="fb-grow"></span>
                    <span class="fb-funcs" title="Functions">${this._funcs(n)}</span>
                    <span class="fb-cards">${this._clusterCards(n)}</span>
                </div>
                <div class="fb-line-sub">${this._meta(r)}<div class="fb-line-tags">${this._tagEditor(n, 3)}</div></div>
            </div>`;
        };

        const html = blocks.map(b => {
            if (b.note) return `<div class="fb-card-note">${this._noteLine(b.note)}</div>`;
            if (b.loose) {
                const r = b.loose, n = r.n;
                return `<article class="fb-card fb-card-loose" ${this._entityAttrs(n)}>
                    <header>${this._icon(r)}${this._name(r)}<span class="fb-grow"></span>${this._noteBtn(n)}</header>
                    ${this._meta(r)}
                    <div class="fb-card-stats"><span class="fb-stat"><b>${this._funcs(n)}</b> functions</span><span class="fb-cards">${this._clusterCards(n)}</span></div>
                    <div class="fb-card-tags">${this._tagEditor(n, 4)}</div>
                </article>`;
            }
            const r = b.head;
            if (r.t === 'group') {
                return `<article class="fb-card" data-ft-card="${escapeAttr(r.id)}">
                    <header>${this._chev(r.id)}<div class="fb-card-title">${this._groupHead(r)}</div></header>
                    <div class="fb-card-body">${b.body.map(line).join('')}</div></article>`;
            }
            const n = r.n;
            // Archives carry no code of their own: only packed binaries get a stats row.
            const stats = n.exists === false || n.is_container ? '' : `
                <div class="fb-card-stats"><span class="fb-stat"><b>${this._funcs(n)}</b> functions</span><span class="fb-cards">${this._clusterCards(n)}</span></div>
                <div class="fb-card-tags">${this._tagEditor(n, 3)}</div>`;
            const archiveClusters = n.is_container && this._clustersOf(n).length
                ? `<div class="fb-card-stats"><span class="fb-cards">${this._clusterCards(n)}</span></div>` : '';
            return `<article class="fb-card" data-ft-card="${escapeAttr(r.id)}" ${this._entityAttrs(n)}>
                <header>${this._chev(r.id)}${this._icon(r)}<div class="fb-card-title">
                        <div class="fb-card-name">${this._name(r)}${this._kind(r)}</div>${this._meta(r)}${stats}${archiveClusters}</div>
                    <span class="fb-grow"></span>${n.exists === false ? '' : this._noteBtn(n)}
                </header>
                <div class="fb-card-body">${b.body.map(line).join('')}</div>
            </article>`;
        }).join('');
        return `<div class="fb-card-grid">${html}</div>`;
    },

    // List: narrow rows, the selected file in a pane on the right.
    _paintList() {
        const rows = this._rows.map(r => {
            if (r.t === 'fold') return this._foldLine(r, r.depth);
            if (r.t === 'note') return this._noteLine(r);
            if (r.t === 'group') {
                return `<div class="fb-li fb-li-group" data-ft-anc="">${this._chev(r.id)}<div class="fb-li-text">${this._groupHead(r)}</div></div>`;
            }
            const n = r.n;
            const tags = (n.tags || []).length + (n.user_tags || []).length;
            return `<div class="fb-li${r.t === 'band' ? ' fb-li-band' : ''}${this._sel === n.file_md5 ? ' selected' : ''}" data-ft-anc="${escapeAttr(r.anc)}"
                    style="--indent:${r.depth}" ${this._entityAttrs(n)} onclick="FileTree.select(${escapeAttr(jsString(n.file_md5))})">
                ${r.t === 'band' ? this._chev(r.id) : '<span class="fb-chev-space"></span>'}${this._icon(r)}
                <div class="fb-li-text">
                    <div class="fb-li-name">${this._name(r, { withParent: this._params.get('group') === 'cluster' })}${this._kind(r)}</div>
                    ${this._meta(r)}
                </div>
                <span class="fb-funcs" title="Functions">${this._funcs(n)}</span>
                <span class="fb-cards">${this._clusterCards(n)}</span>
                <span class="fb-tagcount" title="${tags} tag${tags === 1 ? '' : 's'}">${tags ? `<i class="fa-solid fa-tag"></i>${tags}` : ''}</span>
            </div>`;
        }).join('');
        return `<div class="fb-split"><div class="fb-list">${rows}</div><aside class="fb-detail" id="fb-detail"></aside></div>`;
    },

    _li(md5) {
        return [...document.querySelectorAll('#fb-results .fb-li')].find(el => el.dataset.md5 === md5);
    },

    select(md5) {
        this._sel = md5;
        document.querySelectorAll('#fb-results .fb-li.selected').forEach(el => el.classList.remove('selected'));
        this._li(md5)?.classList.add('selected');
        this._paintDetail();
    },

    _paintDetail() {
        const el = document.getElementById('fb-detail');
        if (!el) return;
        const files = this._rows.filter(r => r.t === 'file' || r.t === 'band');
        let r = files.find(x => x.n.file_md5 === this._sel);
        if (!r) { r = files[0]; this._sel = r && r.n.file_md5; }
        if (!r) { el.innerHTML = ''; return; }
        this._li(r.n.file_md5)?.classList.add('selected');
        const n = r.n;
        const chain = [];
        for (let p = n, guard = 0; p && p.parent_md5 && guard < 10; guard++) {
            const parent = this._nodes.get(p.parent_md5);
            chain.unshift(parent ? parent.file_name : (p.parent_file_name || p.parent_md5));
            p = parent;
        }
        const clusters = this._clustersOf(n).sort((a, b) => (b.cohesion_score || 0) - (a.cohesion_score || 0));
        const ft = Array.isArray(n.filetype) ? n.filetype.join(', ') : (n.filetype || '');
        const title = this._name({ ...r, prefix: '', base: r.base || n.file_name });
        const facts = [
            ['Functions', this._funcs(n)],
            ['Type', ft ? escapeHtml(ft) : ''],
            ['Arch', n.language_id ? `<span class="fb-mono">${escapeHtml(n.language_id)}</span>` : ''],
            ['Status', fileStatusBadgeHtml(n)],
            ['First seen', [].concat(n.first_seen || []).filter(Boolean).map(v => escapeHtml(this._day(v))).join(', ')],
            ['Added', n.entry_date ? escapeHtml(formatDate(n.entry_date)) : ''],
            ['MD5', `<span class="fb-mono fb-select">${escapeHtml(n.file_md5)}</span>${copyFullButton(n.file_md5, 'MD5')}`],
            ['Batch', n.batch_uuid ? `<span class="fb-mono fb-select">${escapeHtml(n.batch_uuid)}</span>${copyFullButton(n.batch_uuid, 'batch UUID')}` : ''],
        ].filter(([, v]) => v);
        el.innerHTML = `
            <div class="fb-detail-head">
                ${chain.length ? `<div class="fb-crumbs">${chain.map(c => `<span>${escapeHtml(c)}</span>`).join('<i class="fa-solid fa-chevron-right"></i>')}</div>` : ''}
                <div class="fb-detail-title">${this._icon(r)}${title}${this._kind(r)}</div>
                ${r.prefix ? `<div class="fb-prefix">${escapeHtml(r.prefix)}</div>` : ''}
                <div class="fb-detail-actions">
                    ${n.exists === false ? '' : `<button class="fb-btn" onclick="openFileDetails(${escapeAttr(jsString(this._col(n)))}, ${escapeAttr(jsString(n.file_md5))}, ${escapeAttr(jsString(n.file_name || ''))}, event)"><i class="fa-solid fa-up-right-from-square"></i> Open file</button>
                    ${this._noteBtn(n)}`}
                </div>
            </div>
            <dl class="fb-facts">${facts.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl>
            <h4>Clusters <span>${clusters.length}</span></h4>
            <div class="fb-detail-clusters">${clusters.length ? clusters.map(c => `<div class="fb-detail-cluster">${renderClusterCards([c], true)}
                <a class="fb-link" onclick="openCluster(${escapeAttr(jsString(c.cluster_uuid))}, true, event, ${escapeAttr(jsString(c.axis || 'overall'))})">${escapeHtml(c.cluster_name || `Cluster ${c.cluster_id}`)}</a>
                <span class="fb-mono">${(c.cohesion_score || 0).toFixed(2)}</span></div>`).join('') : '<p class="fb-muted">No cluster at this cohesion.</p>'}</div>
            <h4>Tags</h4>
            <div class="fb-detail-tags">${n.exists === false ? '' : this._tagEditor(n, 60)}</div>`;
    },

    // Table: one compact row per item, one card per archive, cluster group
    // or standalone file. The cards are separate tables sharing one colgroup,
    // so their columns line up under the single sticky header.
    _paintTable() {
        const extra = this.extraCols();
        const sortBy = this._params.get('sort_by') || 'file_name';
        const order = this._params.get('sort_order') || 'asc';
        const th = (label, key, cls = '') => key
            ? `<th class="${cls} fb-sortable${sortBy === key ? ' sorted' : ''}" onclick="FileTree._sortBy('${key}')">${label}
                ${sortBy === key ? `<i class="fa-solid fa-caret-${order === 'asc' ? 'up' : 'down'}"></i>` : ''}</th>`
            : `<th class="${cls}">${label}</th>`;
        const span = 5 + (extra ? 2 : 0);
        const cols = `<colgroup><col>${extra ? '<col class="fb-col-hash"><col class="fb-col-hash">' : ''}
            <col class="fb-col-num"><col class="fb-col-clusters"><col class="fb-col-tags"><col class="fb-col-note"></colgroup>`;

        const row = r => {
            if (r.t === 'fold') return `<tr data-ft-anc="${escapeAttr(r.anc)}" class="fb-tr-line"><td colspan="${span}">${this._foldLine({ ...r, anc: '' }, r.depth)}</td></tr>`;
            if (r.t === 'group') return `<tr class="fb-tr-group"><td colspan="${span}"><div class="fb-tr-grouphead">${this._chev(r.id)}<div>${this._groupHead(r)}</div></div></td></tr>`;
            const n = r.n;
            return `<tr class="${r.t === 'band' ? 'fb-tr-band' : ''}" data-ft-anc="${escapeAttr(r.anc)}" ${this._entityAttrs(n)}>
                <td><div class="fb-td-file" style="--indent:${r.depth}">
                    ${r.t === 'band' ? this._chev(r.id) : '<span class="fb-chev-space"></span>'}${this._icon(r)}
                    <div class="fb-li-text"><div class="fb-li-name">${this._name(r, { withParent: this._params.get('group') === 'cluster' })}${this._kind(r)}</div>${this._meta(r)}</div>
                </div></td>
                ${extra ? `<td class="fb-td-hash"><div class="ft-clip-cell"><span class="ft-clip">${escapeHtml(n.file_md5)}</span>${copyFullButton(n.file_md5, 'MD5')}</div></td>
                <td class="fb-td-hash">${n.batch_uuid ? `<div class="ft-clip-cell"><span class="ft-clip">${escapeHtml(n.batch_uuid)}</span>${copyFullButton(n.batch_uuid, 'batch UUID')}</div>` : ''}</td>` : ''}
                <td class="fb-td-num">${this._funcs(n)}</td>
                <td>${this._clusterCards(n)}</td>
                <td class="fb-td-tags">${n.exists === false ? '' : this._tagEditor(n, 3)}</td>
                <td class="fb-td-note">${n.exists === false ? '' : this._noteBtn(n)}</td>
            </tr>`;
        };

        // A card starts at every top-level row; notes sit between cards.
        const parts = [];
        let card = null;
        for (const r of this._rows) {
            if (r.t === 'note') { card = null; parts.push(this._noteLine(r)); continue; }
            if (r.depth === 0 || !card) { card = []; parts.push(card); }
            card.push(r);
        }
        const body = parts.map(p => typeof p === 'string' ? p
            : `<table class="fb-table fb-table-card${p[0].t === 'file' ? ' fb-table-loose' : ''}">${cols}<tbody>${p.map(row).join('')}</tbody></table>`).join('');

        return `<div class="fb-table-stack">
            <table class="fb-table fb-table-head">${cols}<thead><tr>
                ${th('File', 'file_name')}${extra ? th('MD5', '') + th('Batch', '') : ''}
                ${th('Funcs', 'function_count', 'fb-th-num')}${th('Clusters', '')}${th('Tags', '')}${th('', '')}
            </tr></thead></table>
            ${body}
        </div>`;
    },

    _sortBy(key) {
        const sortBy = this._params.get('sort_by') || 'file_name';
        const order = this._params.get('sort_order') || 'asc';
        this._navigate(p => {
            p.set('sort_by', key);
            p.set('sort_order', sortBy === key && order === 'asc' ? 'desc' : (sortBy === key ? 'asc' : (key === 'file_name' ? 'asc' : 'desc')));
        });
    },
};
