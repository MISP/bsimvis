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
        if (this._renamedFn) document.removeEventListener('function-renamed', this._renamedFn);
        this._renamedFn = null;
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
    async _initFunction() {
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
        const urlCells = Object.keys(cells).length > 0;
        const tok = this._fnTok = (this._fnTok || 0) + 1;
        this.destroy();
        this._meta = null;
        this._ref = this._ref0 = fx.ref || '';
        this._sample = Number(fx.sample) || 0;
        this._setId = fx.set || '';
        this._urlFids = (fx.fids || '').split(',').filter(Boolean);
        this._fnScope = source.scope === 'functions';
        if (window.location.pathname.endsWith('/nway/function') && (this._ref || this._setId || this._urlFids.length)) {
            this._container.innerHTML = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Loading function set...</div>';
            const meta = await this._fetchMeta();
            if (tok !== this._fnTok) return;
            if (meta.error) {
                this._container.innerHTML = '<div class="nway-state"></div>';
                this._container.firstChild.textContent = meta.error;
                return;
            }
            this._meta = meta;
            fx.fids = meta.fids.join(',');
        }
        this._familyFids = (this._fnScope || (this._meta && !urlCells))
            ? [...new Set((fx.fids || '').split(',').filter(Boolean))]
            : Object.values(cells).filter(v => typeof v === 'string');
        if (!urlCells) {
            for (const fid of this._familyFids) {
                const i = fid.indexOf(':func:');
                if (i >= 0) cells[`${fid.slice(0, i)}:${fid.slice(i + 6).split(':')[0]}`] = fid;
            }
        }
        this._cellFids = Object.values(cells);
        this._fids0 = this._cellFids.join(',');
        if (!this._familyFids.length) {
            this._container.innerHTML = '<div class="nway-state">This link has no function to graph.</div>';
            return;
        }
        this._familyName = this._meta ? this._meta.name : cg.name || '';
        this._familySource = source;
        this._familyDoc = doc;
        this._familyState = { tab: (fx.tab === 'code' && fx.cv === 'full' ? 'full' : fx.tab) || 'graph', cv: fx.cv || 'blocks', bm: ['hybrid', 'cfg', 'text'].includes(fx.bm) ? fx.bm : 'hybrid', only: fx.only === '1', single: fx.single === '1', cols: Math.min(5, Math.max(2, Number(fx.cols) || 2)) };
        if (!['graph', 'simple', 'facets', 'code', 'full', 'unified', 'pair'].includes(this._familyState.tab)) this._familyState.tab = 'graph';
        if (fx.fn !== undefined && fx.fn !== '') this._familyState.fn = Number(fx.fn);
        this._container.innerHTML = `<div class="nway-view nway-function-view">
            ${this._meta ? '<div id="nway-hero" class="nway-hero"></div>' : '<div class="nway-head"><h2 id="nway-family-name"></h2></div>'}
            <div class="xp-bar"><div id="nway-family-tabs" class="bsim-tabbar" role="tablist" style="margin:0">
                ${[['graph', 'Graph'], ['simple', 'Reference'], ['facets', 'Facets'], ['code', 'Code blocks'], ['full', 'Full function'], ['unified', 'Unified'], ['pair', 'Pair diff']].map(([k, label]) => `<button class="bsim-tab" role="tab" data-family-tab="${k}">${label}</button>`).join('')}
            </div></div>
            <div id="nway-graph-host" class="nway-cgp" style="height:calc(100vh - 240px);min-height:400px;"></div>
            <div id="nway-fn-host" hidden></div>
        </div>`;
        const bar = this._container.querySelector('.xp-bar');
        new ResizeObserver(() => bar.parentElement.style.setProperty('--xp-bar-h', bar.getBoundingClientRect().height + 'px')).observe(bar);
        this._container.querySelector('#nway-family-tabs').onclick = e => {
            const t = e.target.closest('[data-family-tab]');
            if (!t || t.disabled) return;
            const data = this._functionPanel && this._functionPanel.data;
            if (['code', 'full', 'unified'].includes(t.dataset.familyTab) && data && !data.consensus) {
                showToast(`Code blocks are unavailable for ${data.functions.length} functions. Compare at most ${data.limits.code} functions to view them.`, 'warning');
                return;
            }
            this._familyState.tab = t.dataset.familyTab;
            this._writeFamilyState();
            this._showFamilyTab();
        };
        let graphReady = false;
        this._renamedFn = e => this._onRenamed(e);
        document.addEventListener('function-renamed', this._renamedFn);
        this._renderHero();
        // A free function set has no file diff of its own: graph it over the diff of its parent files.
        const files = [...new Set(this._familyFids.map(f => { const i = f.indexOf(':func:'); return `${f.slice(0, i)}:${f.slice(i + 6).split(':')[0]}`; }))];
        const graphSource = this._fnScope ? { md5s: files.join(','), ...(this._meta && this._meta.links.nway.pool ? { pool: this._meta.links.nway.pool } : {}), ...(files.length < 2 ? { solo: '1' } : {}) } : source;
        this._graphSource = graphSource;
        if (this._fnScope) this._parentN = files.length;
        this._panel = new NwayPanel(document.getElementById('nway-graph-host'), {
            source: graphSource, state: doc, cg: { ...cg, name: this._familyName, cells },
            onCg: url => {
                const graph = new URLSearchParams(url.split('?')[1]);
                const fids = Object.values(JSON.parse(graph.get('cg_cells'))).filter(v => typeof v === 'string');
                if (fids.join(',') !== this._cellFids.join(',')) {
                    this._familyFids = this._cellFids = fids;
                    // A card is a row of the parent diff: stay an N-way row. Back on the starting set, restore its ref.
                    const row = graph.get('cg_row');
                    this._ref = row !== null ? `nway:${files.length}:${row}` : fids.join(',') === this._fids0 ? this._ref0 : '';
                    this._urlFids = fids;
                    this._setId = '';
                    if (this._meta) this._fetchMeta().then(m => { if (!m.error) { this._meta = m; this._familyName = m.name; this._renderHero(); this._crumbs(); } });
                    this._familyState.fn = null;
                    if (this._functionPanel) this._functionPanel.destroy();
                    this._functionPanel = null;
                    document.getElementById('nway-fn-host').innerHTML = '';
                }
                if (!this._meta) this._familyName = graph.get('cg_name') || '';
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
        // A stored cluster names its own members; any other set is written as the list it was opened with.
        next.delete('fx_name');
        const own = /^(cluster|pool):/.test(this._ref);
        const urlFids = this._meta ? this._urlFids : this._familyFids;
        if (this._ref) next.set('fx_ref', this._ref); else next.delete('fx_ref');
        if (this._sample) next.set('fx_sample', this._sample); else next.delete('fx_sample');
        next.delete('fx_fids');
        next.delete('fx_set');
        if (own) { /* the ref is the set */ }
        else if (urlFids.length > 20) {
            if (this._setId) next.set('fx_set', this._setId);
            else { next.set('fx_fids', urlFids.join(',')); this._ensureSet(urlFids); }
        } else next.set('fx_fids', urlFids.join(','));
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
        this._crumbs();
    },

    _crumbs() {
        if (window.Breadcrumbs && window.getRoutingState) {
            const r = window.getRoutingState();
            window.Breadcrumbs.render(window.Breadcrumbs.generate({ viewKey: r.viewKey, collection: r.collection, pool: r.pool, params: r.params }, null));
        }
    },

    _heroLinks(m) {
        const L = m.links, nw = L.nway, out = [];
        const pool = nw.pool ? `&pool=${encodeURIComponent(nw.pool)}` : '';
        if (nw.files >= 2) {
            // A row link also searches the diff for its name, so the row is the first thing in view.
            const focus = L.kind === 'nway' && m.name_source !== 'fallback' ? `&tab=all&q=${encodeURIComponent(m.name)}` : '';
            const warn = m.sample.shown < m.sample.total ? ' (sample)' : '';
            // Files of the diff this view came from: a row ref's count, else what the graph spans (a merged focus has no row).
            const nwayK = r => (/^nway:/.test(r || '') ? Number(r.split(':')[1]) : 0);
            const total = nwayK(this._ref) || nwayK(this._ref0) || this._parentN || 0;
            // A set from a cluster has no file source of its own: the files its graph spans are the parent.
            const { solo, ...src } = this._graphSource || this._familySource || {};
            // From a row of a bigger diff: the whole diff is the parent, these files are a narrower one.
            if (total > nw.files && (src.md5s || src.batch_uuid || src.cluster_uuid)) {
                const qs = new URLSearchParams({ ...src, ...this._familyDoc });
                out.push({ primary: true, icon: 'fa-solid fa-table-columns', href: `/diff/nway?${qs}${focus}`, text: `Open parent N-way diff (${total} files)`, title: 'Parent N-way diff' });
                out.push({ icon: 'fa-solid fa-table-columns', href: `/diff/nway?md5s=${encodeURIComponent(nw.md5s.join(','))}${pool}${focus}`, text: `Only these ${nw.files} files`, title: 'N-way diff of the files holding this row' });
            } else out.push({ primary: true, icon: 'fa-solid fa-table-columns', href: `/diff/nway?md5s=${encodeURIComponent(nw.md5s.join(','))}${pool}${focus}`, text: `Open parent N-way diff (${nw.files} files)${warn}`, title: 'N-way diff' });
        } else if (nw.md5s.length) {
            const i = nw.md5s[0].lastIndexOf(':');
            const coll = nw.md5s[0].slice(0, i), md5 = nw.md5s[0].slice(i + 1);
            out.push({ primary: true, icon: 'fa-solid fa-file', href: `${nw.pool ? `/pools/${encodeURIComponent(nw.pool)}` : ''}/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(md5)}`, text: 'Open file', title: md5 });
        }
        const c = L.cluster;
        if (c && c.cluster_uuid) {
            const href = c.pool ? `/pools/${encodeURIComponent(c.pool)}/functions/clusters/${encodeURIComponent(c.cluster_uuid)}` : window.Nav.buildUIUrl(c.collection, ['functions', 'clusters', c.cluster_uuid]);
            out.push({ icon: 'fa-solid fa-bullseye', href, text: `Open cluster ${c.cluster_uuid.slice(0, 8)}`, title: 'Cluster' });
        }
        return out;
    },

    // The hero: what this set is, in the server's words. Every server string goes through escapeHtml.
    _renderHero() {
        const el = document.getElementById('nway-hero');
        const m = this._meta;
        if (!el || !m) return;
        const c = m.counts, coh = m.cohesion, p = m.prototype, kind = m.links.kind;
        const badge = kind === 'nway' ? 'N-way cluster' : kind === 'cluster' || kind === 'pool' ? 'Function cluster' : 'Function set';
        const row = (m.ref || '').split(':')[2];
        const where = kind === 'nway' ? `row ${Number(row) + 1 || ''} of a ${c.files}-file diff` : kind === 'set' ? (c.files === 1 ? `${c.functions} functions, all in one file` : 'your selection, not an N-way cluster') : `stored in ${(m.links.cluster && (m.links.cluster.pool ? `pool ${m.links.cluster.pool}` : m.links.cluster.collection)) || 'a collection'}`;
        const from = { analyst: 'name set by an analyst', majority: 'name from majority of members', tag: 'name from a tag', fallback: 'name of the heaviest function' }[m.name_source] || '';
        const types = {};
        for (const v of p.variants) types[v.return_type] = (types[v.return_type] || 0) + v.count;
        const typeText = Object.entries(types).map(([t, n]) => `${t} ${n}`).join(' · ');
        const ty = t => `<span style="color:var(--token-address)">${escapeHtml(t)}</span>`;
        const proto = `${ty(p.return_type)} ${escapeHtml(m.name)}(${p.parameters.map(ty).join(', ')})`;
        const plural = (n, w) => `${Number(n)} ${w}${n === 1 ? '' : 's'}`;
        const parts = [plural(c.functions, 'function'), plural(c.files, 'file'), plural(c.collections, 'collection'), c.arch.join(' · ')].filter(Boolean);
        if (c.clusters > 1) parts.push(plural(c.clusters, 'cluster'));
        let cohHtml = '';
        if (coh && coh.error) cohHtml = `<span>cohesion unavailable</span>`;
        else if (coh) {
            const gap = coh.total_pairs - coh.scored_pairs;
            const color = coh.median >= 0.7 ? 'var(--success)' : coh.median >= 0.4 ? 'var(--token-warning)' : 'var(--token-instruction)';
            const pc = v => (v * 100).toFixed(0);
            const tip = escapeAttr(`${Number(coh.scored_pairs)} of ${Number(coh.total_pairs)} pairs scored; ${gap} unscored count as 0`);
            cohHtml = `<span style="display:inline-flex;align-items:center;gap:8px" title="${tip}">cohesion<span class="nway-hbar"><i style="left:${pc(coh.min)}%;right:${pc(1 - coh.max)}%;background:${color}"></i><u style="left:${pc(coh.median)}%"></u></span>min ${coh.min.toFixed(2)} · <b style="color:${color}">median ${coh.median.toFixed(2)}</b> · max ${coh.max.toFixed(2)}</span>`;
        }
        // Pills only for what is off; a healthy set shows none.
        const warns = [];
        if (coh && !coh.error && coh.median < 0.4) warns.push('weak: probably not one function');
        if (c.functions > 1 && p.return_agree !== c.functions) warns.push(`return type ${typeText}`);
        if (m.sample.shown < m.sample.total) warns.push(`showing ${Number(m.sample.shown)} of ${Number(m.sample.total)}`);
        const btn = (icon, label, tooltip, attr) => UI.Button.render({ className: 'btn-code-action', icon, label, tooltip, attr });
        const links = this._heroLinks(m).map((l, i) => btn(l.icon, l.text, l.title, { 'data-hero-link': i })).join('')
            + (m.sample.shown < m.sample.total ? btn('fa-solid fa-sliders', 'Change sample', 'How many functions the comparison tabs take', { 'data-hero-sample': '' }) : '');
        el.innerHTML = `<div class="nway-hero-top">
                <span class="nway-hkind ${kind === 'set' ? 'set' : ''}" title="${escapeAttr(`${where}; ${from}`)}">${badge}</span>
                <h2 class="nway-hero-name">${escapeHtml(m.name)} <button class="nway-rename" data-rename-fids="${escapeAttr(m.fids.join(','))}" data-rename-name="${escapeAttr(m.name)}" title="Rename this function set"><i class="fa-solid fa-pen"></i></button></h2>
                <span class="nway-hero-actions">${links}</span>
            </div>
            <div class="nway-hero-proto">${proto}${p.variants.length > 1 ? ` <span class="nway-hvar">· ${p.variants.length} variants</span>` : ''}</div>
            <div class="nway-hero-meta dim">${[...parts.map(escapeHtml), cohHtml].join(' · ')}${warns.map(w => `<span class="nway-hpill warn">${escapeHtml(w)}</span>`).join('')}</div>`;
        const hrefs = this._heroLinks(m);
        el.onclick = e => {
            const a = e.target.closest('[data-hero-link]');
            if (a) {
                e.preventDefault();
                const l = hrefs[Number(a.dataset.heroLink)];
                return window.Nav.openPath(l.href, e, { title: l.title });
            }
            if (e.target.closest('[data-hero-sample]')) {
                const n = Number(window.prompt(`Functions to compare (2 to ${m.sample.cap}):`, String(m.sample.shown)));
                if (n >= 2) { this._sample = Math.min(n, m.sample.cap); this._writeFamilyState(); this._initFunction(); }
            }
        };
    },

    // A rename from the hero: a stored cluster takes the new name too.
    async _onRenamed(e) {
        if (!this._meta) return;
        const name = ((e.detail || {}).renames || [])[0]?.name;
        const c = this._meta.links.cluster;
        if (name && c && /^(cluster|pool):/.test(this._ref)) {
            await fetch('/api/cluster/meta', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ collection: c.collection, pool: c.pool || undefined, cluster_id: c.cluster_id, cluster_name: name }) }).catch(() => {});
        }
        const m = await this._fetchMeta();
        if (m.error) return;
        this._meta = m;
        this._familyName = m.name;
        this._renderHero();
        this._crumbs();
    },

    // A long list rides the URL as a short Redis id (one day), never as thousands of characters.
    async _ensureSet(fids) {
        if (this._setting) return;
        this._setting = true;
        try {
            const res = await fetch('/api/nway/function/set', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ fids }) });
            const body = await res.json().catch(() => ({}));
            if (res.ok && body.id) { this._setId = body.id; this._writeFamilyState(); }
        } catch (e) { /* the long fx_fids URL keeps working */ }
        this._setting = false;
    },

    async _fetchMeta() {
        const mq = new URLSearchParams();
        if (this._ref) mq.set('ref', this._ref);
        if (this._setId) mq.set('set', this._setId);
        else if (!/^(cluster|pool):/.test(this._ref)) mq.set('fids', this._urlFids.join(','));
        if (this._sample) mq.set('sample', this._sample);
        try {
            const res = await fetch(`/api/nway/function/meta?${mq.toString()}`);
            const body = await res.json().catch(() => ({}));
            if (!res.ok) return { error: body.error === 'set_expired' ? 'This function set has expired. Open it again from its table.' : (body.message || body.error || `HTTP ${res.status}`) };
            return body;
        } catch (e) {
            return { error: String(e) };
        }
    },

    _syncFamilyTabs() {
        const title = document.getElementById('nway-family-name');
        if (title) title.textContent = this._familyName || 'Function family';
        const data = this._functionPanel && this._functionPanel.data;
        this._container.querySelectorAll('[data-family-tab]').forEach(t => {
            const tab = t.dataset.familyTab;
            t.classList.toggle('active', tab === this._familyState.tab);
            t.setAttribute('aria-selected', String(tab === this._familyState.tab));
            t.disabled = tab !== 'graph' && this._familyFids.length < 2;
            t.title = this._familyFids.length < 2 && tab !== 'graph' ? 'Comparison needs functions in at least two files' : ['code', 'full', 'unified'].includes(tab) && data && !data.consensus && !data.blocks_error ? `Code blocks need at most ${data.limits.code} functions` : '';
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
                    if (['code', 'full', 'unified'].includes(this._familyState.tab) && !this._functionPanel.data.consensus && !this._functionPanel.data.blocks_error) {
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
