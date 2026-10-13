/**
 * N-way diff view:
 * /diff/nway?md5s=coll:md5,coll:md5[&batch_uuid=U][&collection=C][&pool=P][&tab=..&min_edge=..]
 *
 * The set (md5s + batch) is edited in the "Edit set" panel; control state is
 * read from the URL and written back so the page is deep-linkable.
 */
window.NwayView = {
    _panel: null,
    _functionPanel: null,
    _set: { md5s: [], batch: '', collection: '', pool: '', cluster_uuid: '', axis: '' },
    _state: {},
    _collections: null,

    destroy() {
        if (this._panel) this._panel.destroy();
        this._panel = null;
        if (this._functionPanel) this._functionPanel.destroy();
        this._functionPanel = null;
    },

    async init(params, containerId) {
        this._container = document.getElementById(containerId);
        if (!this._container) return;
        if (window.location.pathname.endsWith('/nway/graph')) return this._initFunction();
        if (window.location.pathname.endsWith('/nway/function')) return this._initFunction();
        const q = new URLSearchParams(window.location.search);
        this._set = {
            md5s: (q.get('md5s') || '').split(',').map(t => t.trim()).filter(Boolean),
            batch: q.get('batch_uuid') || '',
            collection: q.get('collection') || '',
            pool: q.get('pool') || '',
            cluster_uuid: q.get('cluster_uuid') || '',
            axis: q.get('axis') || '',
        };
        this._state = {};
        for (const k of ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset', 'column', 'focus', 'focus_rule', 'side', 'columns', 'child_presence']) {
            if (q.has(k)) this._state[k] = q.get(k);
        }
        this._collections = null;
        this._render();
    },

    // Graph and comparison tabs share the focused family and URL state.
    _initFunction() {
        const q = new URLSearchParams(window.location.search);
        const CTL = ['mode', 'min_edge', 'columns', 'child_presence', 'focus', 'focus_rule'];
        const source = {}, doc = {}, fx = {}, cg = {};
        for (const [k, v] of q) {
            if (k.startsWith('fx_')) fx[k.slice(3)] = v;
            else if (k.startsWith('cg_')) cg[k.slice(3)] = v;
            else if (CTL.includes(k)) doc[k] = v;
            else source[k] = v;
        }
        let cells = {};
        try { cells = JSON.parse(cg.cells || '{}'); } catch (e) { /* fall back to function ids */ }
        try { cg.pieces = cg.pieces ? JSON.parse(cg.pieces) : undefined; } catch (e) { cg.pieces = undefined; }
        if (!Object.keys(cells).length) {
            for (const fid of (fx.fids || '').split(',').filter(Boolean)) {
                const i = fid.indexOf(':func:');
                if (i < 0) continue;
                const md5 = fid.slice(i + 6).split(':')[0];
                cells[`${fid.slice(0, i)}:${md5}`] = fid;
            }
        }
        this.destroy();
        this._familyFids = source.scope === 'functions'
            ? [...new Set((fx.fids || '').split(',').filter(Boolean))]
            : Object.values(cells).filter(v => typeof v === 'string');
        if (!this._familyFids.length) {
            this._container.innerHTML = '<div class="nway-state">This link has no function to graph.</div>';
            return;
        }
        this._familyName = cg.name || fx.name || '';
        this._familySource = source;
        this._familyDoc = doc;
        this._fnScope = source.scope === 'functions';
        this._familyState = { tab: fx.tab || (this._fnScope ? 'facets' : 'graph'), cv: fx.cv || 'blocks', bm: ['hybrid', 'cfg', 'text'].includes(fx.bm) ? fx.bm : 'hybrid', only: fx.only === '1', single: fx.single === '1', cols: Math.min(5, Math.max(2, Number(fx.cols) || 2)) };
        if (!['graph', 'simple', 'facets', 'code', 'pair'].includes(this._familyState.tab) || (this._fnScope && this._familyState.tab === 'graph')) this._familyState.tab = this._fnScope ? 'facets' : 'graph';
        if (fx.fn !== undefined && fx.fn !== '') this._familyState.fn = Number(fx.fn);
        this._container.innerHTML = `<div class="nway-view nway-function-view">
            <div class="nway-head"><h2 id="nway-family-name"></h2></div>
            <div class="xp-bar"><div id="nway-family-tabs" class="bsim-tabbar" role="tablist" style="margin:0">
                ${[['graph', 'Graph'], ['simple', 'Simple'], ['facets', 'Facets'], ['code', 'Code blocks'], ['pair', 'Pair diff']].filter(([k]) => !(this._fnScope && k === 'graph')).map(([k, label]) => `<button class="bsim-tab" role="tab" data-family-tab="${k}">${label}</button>`).join('')}
            </div></div>
            <div id="nway-graph-host" class="nway-cgp" style="height:calc(100vh - 240px);min-height:400px;"></div>
            <div id="nway-fn-host" hidden></div>
        </div>`;
        this._container.querySelector('#nway-family-tabs').onclick = e => {
            const t = e.target.closest('[data-family-tab]');
            if (!t || t.disabled) return;
            const data = this._functionPanel && this._functionPanel.data;
            if (t.dataset.familyTab === 'code' && data && !data.consensus) {
                showToast(`Code blocks are unavailable for ${data.functions.length} functions. Compare at most ${data.limits.code} functions to view them.`, 'warning');
                return;
            }
            this._familyState.tab = t.dataset.familyTab;
            this._writeFamilyState();
            this._showFamilyTab();
        };
        let graphReady = false;
        if (this._fnScope) {
            // A free function set has no file-level graph: show the comparison directly.
            this._familyName = this._familyName || `${this._familyFids.length} functions`;
            this._writeFamilyState();
            this._showFamilyTab();
            return;
        }
        this._panel = new NwayPanel(document.getElementById('nway-graph-host'), {
            source, state: doc, cg: { ...cg, name: this._familyName, cells },
            onCg: url => {
                const graph = new URLSearchParams(url.split('?')[1]);
                const fids = Object.values(JSON.parse(graph.get('cg_cells'))).filter(v => typeof v === 'string');
                if (fids.join(',') !== this._familyFids.join(',')) {
                    this._familyFids = fids;
                    this._familyState.fn = null;
                    if (this._functionPanel) this._functionPanel.destroy();
                    this._functionPanel = null;
                    document.getElementById('nway-fn-host').innerHTML = '';
                }
                this._familyName = graph.get('cg_name') || '';
                const next = new URLSearchParams(window.location.search);
                for (const k of [...next.keys()]) if (k.startsWith('cg_')) next.delete(k);
                for (const [k, v] of graph) if (k.startsWith('cg_')) next.set(k, v);
                this._writeFamilyState(next, graphReady);
                graphReady = true;
                this._syncFamilyTabs();
            },
        });
        this._showFamilyTab();
        this._panel.load();
    },

    _writeFamilyState(next = new URLSearchParams(window.location.search), push = false) {
        const st = this._familyState;
        next.set('fx_fids', this._familyFids.join(','));
        next.set('fx_name', this._familyName);
        next.set('fx_tab', st.tab);
        next.set('fx_cv', st.cv);
        if (st.bm && st.bm !== 'hybrid') next.set('fx_bm', st.bm); else next.delete('fx_bm');
        next.set('fx_only', st.only ? '1' : '0');
        if (st.single) next.set('fx_single', '1'); else next.delete('fx_single');
        if (st.cols > 2) next.set('fx_cols', st.cols); else next.delete('fx_cols');
        if (st.fn !== null && st.fn !== undefined) next.set('fx_fn', st.fn); else next.delete('fx_fn');
        const url = `/diff/nway/function?${next.toString()}`;
        if (url === window.location.pathname + window.location.search) return;
        history[push ? 'pushState' : 'replaceState'](null, '', url);
        if (window.Breadcrumbs && window.getRoutingState) {
            const r = window.getRoutingState();
            window.Breadcrumbs.render(window.Breadcrumbs.generate({ viewKey: r.viewKey, collection: r.collection, pool: r.pool, params: r.params }, null));
        }
    },

    _syncFamilyTabs() {
        document.getElementById('nway-family-name').textContent = this._familyName || 'Function family';
        const data = this._functionPanel && this._functionPanel.data;
        this._container.querySelectorAll('[data-family-tab]').forEach(t => {
            const tab = t.dataset.familyTab;
            t.classList.toggle('active', tab === this._familyState.tab);
            t.setAttribute('aria-selected', String(tab === this._familyState.tab));
            t.disabled = tab !== 'graph' && this._familyFids.length < 2;
            t.title = this._familyFids.length < 2 && tab !== 'graph' ? 'Comparison needs functions in at least two files' : tab === 'code' && data && !data.consensus && !data.blocks_error ? `Code blocks need at most ${data.limits.code} functions` : '';
        });
    },

    _showFamilyTab() {
        const graph = this._familyState.tab === 'graph';
        const host = document.getElementById('nway-fn-host');
        document.getElementById('nway-graph-host').hidden = !graph;
        host.hidden = graph;
        if (graph) {
            if (this._panel && this._panel._cg) this._panel._cgDraw();
        } else if (this._familyFids.length < 2) {
            host.innerHTML = '<div class="nway-state">Comparison needs functions in at least two files. Use Graph to explore this function.</div>';
        } else if (!this._functionPanel) {
            this._functionPanel = new NwayFunction(host, {
                source: this._familySource, doc: this._familyDoc, fids: this._familyFids,
                name: this._familyName, state: this._familyState, embedded: true,
                onState: st => { this._familyState = st; this._writeFamilyState(); this._syncFamilyTabs(); },
                onLoad: () => {
                    if (this._familyState.tab === 'code' && !this._functionPanel.data.consensus && !this._functionPanel.data.blocks_error) {
                        const data = this._functionPanel.data;
                        showToast(`Code blocks are unavailable for ${data.functions.length} functions. Compare at most ${data.limits.code} functions to view them.`, 'warning');
                    }
                    if (this._familyState.tab !== 'graph') this._familyState.tab = this._functionPanel.s.tab;
                    this._writeFamilyState();
                    this._syncFamilyTabs();
                },
            });
            this._functionPanel.load();
        } else {
            this._functionPanel.s.tab = this._familyState.tab;
            if (this._functionPanel.data) this._functionPanel.render();
        }
        this._syncFamilyTabs();
    },

    _source() {
        const s = this._set;
        const source = {};
        if (s.md5s.length) source.md5s = s.md5s.join(',');
        if (s.batch) source.batch_uuid = s.batch;
        if (s.collection) source.collection = s.collection;
        if (s.pool) source.pool = s.pool;
        if (s.cluster_uuid) source.cluster_uuid = s.cluster_uuid;
        if (s.axis) source.axis = s.axis;
        return source;
    },

    _writeUrl(extra) {
        const next = new URLSearchParams(this._source());
        for (const [k, v] of Object.entries(extra || {})) if (v !== '' && v !== null && v !== undefined) next.set(k, v);
        history.replaceState(null, '', `${window.location.pathname}?${next.toString()}`);
    },

    _hasSet() {
        return this._set.cluster_uuid || this._set.batch || this._set.md5s.length >= 2;
    },

    _render() {
        this.destroy();
        this._container.innerHTML = `
            <div class="nway-view">
                <div class="nway-head">
                    <h2><i class="fa-solid fa-table-columns"></i> N-way File Diff <span class="exp-badge" title="Experimental: results and layout may change"><i class="fa-solid fa-flask"></i> Experimental</span></h2>
                    <p>Functions shared across several files, and the ones unique to a single file.</p>
                </div>
                <details id="nway-edit" class="nway-edit" ${this._set.cluster_uuid ? 'hidden' : this._hasSet() ? '' : 'open'}>
                    <summary>Edit set (${this._set.md5s.length} files${this._set.batch ? ' + 1 batch' : ''})</summary>
                    <div id="nway-set"></div>
                </details>
                <div id="nway-panel"></div>
            </div>`;
        this._renderSet();
        this._mount();
    },

    _mount() {
        const host = document.getElementById('nway-panel');
        if (!host) return;
        if (!this._hasSet()) {
            host.innerHTML = '<div class="nway-state">Add two or more files (or a batch) above, or pick rows in a file table and use "Add to compare set".</div>';
            return;
        }
        this._panel = new NwayPanel(host, {
            source: this._source(),
            state: this._state,
            onState: s => {
                this._state = { ...s };
                this._writeUrl(s);
            },
        });
        this._panel.load();
    },

    // `coll:md5` (or a bare md5) -> its collection and md5.
    _split(token) {
        const i = token.lastIndexOf(':');
        return { coll: i > 0 ? token.slice(0, i) : this._set.collection, md5: token.slice(i + 1) };
    },

    async _loadCollections() {
        if (this._collections) return this._collections;
        try {
            const res = await fetch('/api/collection/search?limit=10000');
            const data = await res.json();
            this._collections = (data.collections || []).map(c => c.name);
        } catch (e) {
            this._collections = [];
        }
        return this._collections;
    },

    async _renderSet() {
        const el = document.getElementById('nway-set');
        if (!el) return;
        const colls = await this._loadCollections();
        const summary = document.querySelector('#nway-edit summary');
        if (summary) summary.textContent = `Edit set (${this._set.md5s.length} files${this._set.batch ? ' + 1 batch' : ''})`;
        const pick = this._set.collection || (window.getRoutingState ? window.getRoutingState().collection : '') || '';
        const opts = sel => colls.map(c => `<option value="${escapeAttr(c)}" ${c === sel ? 'selected' : ''}>${escapeHtml(c)}</option>`).join('');
        const chip = (inner, title, i, kind) => `<span class="nway-chip" title="${escapeAttr(title)}">${inner}<button data-rm="${kind}" data-i="${i}" title="Remove">&times;</button></span>`;
        const members = this._set.md5s.map((t, i) => {
            const { coll, md5 } = this._split(t);
            return chip(`${window.EntityRenderer.renderMd5(md5, { collection: coll })}${coll ? `<span>${escapeHtml(coll)}</span>` : ''}`, t, i, 'md5');
        }).join('')
            + (this._set.batch ? chip(`<span>batch ${escapeHtml(this._set.batch.slice(0, 8))}</span>`, this._set.batch, 0, 'batch') : '');
        const basket = window.NwayBasket ? window.NwayBasket.items() : [];
        el.innerHTML = `
            <div class="nway-chips">${members || '<span class="dim">No files yet.</span>'}</div>
            <div class="nway-edit-grid">
                <div class="nway-edit-col">
                    <span class="nway-lbl">Paste md5s (<code>collection:md5</code>, or a bare md5)</span>
                    <textarea id="nway-paste" class="nway-input" rows="3"></textarea>
                    <div class="nway-edit-row">
                        <select id="nway-paste-coll" class="nway-input" title="Collection for bare md5s">${opts(pick)}</select>
                        <button id="nway-add-md5" class="top-action-btn">Add files</button>
                    </div>
                </div>
                <div class="nway-edit-col">
                    <span class="nway-lbl">Batch UUID (all its files)</span>
                    <input id="nway-batch" type="text" class="nway-input" value="${escapeAttr(this._set.batch)}">
                    <div class="nway-edit-row">
                        <select id="nway-batch-coll" class="nway-input" title="Collection of the batch"><option value="">any collection</option>${opts(this._set.collection)}</select>
                        <button id="nway-add-batch" class="top-action-btn">Use batch</button>
                    </div>
                </div>
                ${basket.length ? `<div class="nway-edit-col"><span class="nway-lbl">Compare set</span><button id="nway-use-basket" class="top-action-btn">Add ${basket.length} from compare set</button></div>` : ''}
            </div>
            <div id="nway-set-msg" class="nway-msg"></div>`;

        const msg = t => { el.querySelector('#nway-set-msg').textContent = t; };
        const apply = () => { this._renderSet(); this._writeUrl(this._state); this._mount(); };
        const addTokens = tokens => {
            this._set.md5s = [...new Set([...this._set.md5s, ...tokens])];
            apply();
        };

        el.querySelectorAll('[data-rm]').forEach(btn => btn.addEventListener('click', () => {
            if (btn.dataset.rm === 'batch') this._set.batch = '';
            else this._set.md5s.splice(Number(btn.dataset.i), 1);
            this.destroy();
            apply();
        }));
        el.querySelector('#nway-add-md5').addEventListener('click', () => {
            const coll = el.querySelector('#nway-paste-coll').value;
            const out = [];
            for (const t of el.querySelector('#nway-paste').value.split(/[\s,]+/).filter(Boolean)) {
                if (!/^([^\s:]+:)?[0-9a-f]{32}$/i.test(t)) return msg(`Not an md5 or collection:md5: ${t.slice(0, 60)}`);
                if (!t.includes(':') && !coll) return msg('Pick a collection for bare md5s.');
                out.push(t.includes(':') ? t.toLowerCase() : `${coll}:${t.toLowerCase()}`);
            }
            if (!out.length) return msg('Nothing to add.');
            this.destroy();
            addTokens(out);
        });
        el.querySelector('#nway-add-batch').addEventListener('click', () => {
            const uuid = el.querySelector('#nway-batch').value.trim();
            if (!/^[0-9a-f-]{8,40}$/i.test(uuid)) return msg('Not a batch UUID.');
            this._set.batch = uuid;
            this._set.collection = el.querySelector('#nway-batch-coll').value;
            this.destroy();
            apply();
        });
        const useBasket = el.querySelector('#nway-use-basket');
        if (useBasket) useBasket.addEventListener('click', () => { this.destroy(); addTokens(basket); });
    },
};
