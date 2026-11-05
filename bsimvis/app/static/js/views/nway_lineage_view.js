/**
 * N-way lineage view (full page):
 * /diff/nway/lineage?<same set params as /diff/nway>[&layout=tree|chain|star][&root=<file id>][&lview=tree|tracks|split|ring]
 *
 * Routing, URL state and fetching only; the chart is nway_lineage.js
 * (NwayLineage.mount). The page reads and writes the N-way table's own params
 * (q, scope, tab, group = the lens preset, columns = files | twins, track, column,
 * order=lineage) plus its own layout, root and lview, so both pages hand every
 * filter to each other. A filter or a lens refetches (rows.match and the lens groups change, the tree
 * stays); a track does not. `lview`: the tree, the group tracks (a group
 * list beside it), or split: a compact tree over a real NwayPanel on the same URL
 * state (a row hover outlines its nodes, a node click sets the table's column,
 * tracking in either pane lights the other).
 */
window.NwayLineageView = {
    _inst: null,
    _panel: null,
    _tok: 0,

    destroy() {
        this._tok++;
        clearTimeout(this._typing);
        if (this._panel) this._panel.destroy();
        this._panel = null;
        if (this._inst) this._inst.destroy();
        this._inst = null;
    },

    async init(params, containerId) {
        this.destroy();
        const host = document.getElementById(containerId);
        if (!host) return;
        const tok = this._tok;
        const NL = window.NwayLineage;
        NL.ensureCss();
        // the table's defaults: a value equal to one stays out of the URL, as the table does
        const D = { q: '', scope: 'code', tab: 'all', group: 'region,role', columns: 'twins', track: '', ...(window.NwayPanel ? window.NwayPanel.DEFAULTS : {}), lview: 'tree', rnest: '1', rarc: window.NwayRing ? window.NwayRing.RING.ARC_DEFAULT : 'group', rmin: '0.5', rlist: '', rtier: '', rbundle: '' };
        // rnest, rarc, rmin, rlist, rtier, rbundle: the Ring view's settings (lineage-only keys, like lview)
        const KEYS = ['q', 'scope', 'tab', 'group', 'columns', 'track', 'lview', 'rnest', 'rarc', 'rmin', 'rlist', 'rtier', 'rbundle'];
        const q = new URLSearchParams(window.location.search);
        // `order` is the table's column order and passes through; an old link's order=tree|chain|star is the layout
        const legacy = ['tree', 'chain', 'star'].includes(q.get('order')) ? q.get('order') : '';
        const st = { layout: q.get('layout') || legacy || 'tree', root: q.get('root') || '', column: q.get('column') || '' };
        for (const k of KEYS) st[k] = q.has(k) ? q.get(k) : String(D[k] ?? '');
        // the lens a group string stands for: its preset, else the lens of its first level
        const lensOf = g => {
            const first = (g || '').split(',')[0];
            const l = NL.LENSES.find(x => x[2] === g) || NL.LENSES.find(x => x[2].split(',')[0] === first);
            return l ? l[0] : 'modules';
        };
        let twinsOk = true;
        const cols = () => (st.columns === 'twins' && twinsOk ? 'twins' : 'files');
        const apiQs = extra => {
            const out = new URLSearchParams(window.location.search);
            for (const k of ['order', 'layout', 'lview', 'rhold', 'rnest', 'rfocus', 'rarc', 'rmin', 'rlist', 'rtier', 'rbundle', 'root', 'file', 'offset', 'track', 'lane_names', 'lane_graph']) out.delete(k);
            for (const k of ['q', 'scope', 'tab', 'group']) if (st[k]) out.set(k, st[k]); else out.delete(k);
            // match, columns and groups must be the table's: the server's own defaults differ (tab=core, no twin tree, no merge)
            for (const k of Object.keys(D)) if ((['k', 'mode', 'twin_depth', 'twin_link', 'child_presence', 'sort_dir'].includes(k) || k.startsWith('group_')) && !out.has(k) && D[k] !== undefined && D[k] !== '') out.set(k, String(D[k]));
            out.set('columns', cols());
            out.set('order', st.layout);
            if (st.root) out.set('root', st.root);
            for (const [k, v] of Object.entries(extra || {})) out.set(k, v);
            return out;
        };
        const get = async extra => {
            const res = await fetch(`/api/bin_sim/nway/lineage?${apiQs(extra).toString()}`, { signal: AbortSignal.timeout(180000) });
            const doc = await res.json();
            // a server without Same code lineage answers 400 "needs file columns": fall back to files
            if (res.status === 400 && cols() === 'twins' && /columns/.test(doc.message || doc.error || '')) { twinsOk = false; if (this._panel) { this._panel.fileModes = ['files']; this._panel.render(); } return get(extra); }
            if (!res.ok) throw new Error(doc.message || doc.error || `HTTP ${res.status}`);
            return doc;
        };

        const mk = (tag, attrs = {}, text) => {
            const e = document.createElement(tag);
            for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
            if (text !== undefined) e.textContent = text;
            return e;
        };
        const layoutSel = mk('select', { 'aria-label': 'Lineage layout' });
        for (const [v, t] of [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']]) {
            const o = mk('option', { value: v }, t);
            o.selected = v === st.layout;
            layoutSel.append(o);
        }
        const rootSel = mk('select', { 'aria-label': 'Lineage root file' });
        const backHref = () => {
            const bq = NL.tableQs(window.location.search);
            return window.NwayPanel ? window.NwayPanel.backUrl(bq) : `/diff/nway?${bq.toString()}`;
        };

        const writeUrl = () => {
            const u = new URLSearchParams(window.location.search);
            if (st.layout !== 'tree') u.set('layout', st.layout); else u.delete('layout');
            if (u.get('order') !== 'lineage') u.delete('order');
            if (st.root) u.set('root', st.root); else u.delete('root');
            for (const k of KEYS) if (st[k] !== String(D[k] ?? '')) u.set(k, st[k]); else u.delete(k);
            u.delete('rhold'); u.delete('rfocus');
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
        };

        // ---- Tree | Tracks | Split ----
        const views = mk('div', { class: 'lin-seg', role: 'group', 'aria-label': 'View' });
        const viewBtns = [['tree', 'Tree', 'fa-sitemap', 'The lineage tree'], ['tracks', 'Tracks', 'fa-route', 'Follow one group of the lens through the tree, or six at once'], ['split', 'Split', 'fa-table-columns', 'A compact tree over the N-way table, linked both ways'], ['ring', 'Ring', 'fa-circle-nodes', 'Clusters of files as circles, the modules they hold as rings, shared modules as ribbons']].map(([v, t, icon, title]) => {
            const b = mk('button', { type: 'button', title });
            b.append(mk('i', { class: `fa-solid ${icon}`, style: 'margin-right:4px' }), document.createTextNode(t));
            b.addEventListener('click', () => { if (st.lview === v) return; st.lview = v; writeUrl(); applyView(); });
            views.append(b);
            return { v, b };
        });

        // ---- the table's own controls (NwayPanel, lean): search, filters, code axis, tabs, merge files, lenses.
        // The panel owns the filter state; the chart sits in its stage slot, under the controls. ----
        const status = mk('span', { class: 'lin-dim' });
        host.replaceChildren();
        const panelHost = mk('div', { class: 'nway-view nway-fill' });
        panelHost.style.cssText = 'flex:1;min-height:0;padding:0 12px';
        const wrap = mk('div');
        wrap.style.cssText = 'flex:1;min-height:0;width:100%;position:relative;display:flex;flex-direction:column;overflow:hidden';
        const chart = mk('div');
        chart.style.cssText = 'display:flex;flex-direction:column;flex:1;min-height:0';
        const msg = mk('div', {}, 'Loading lineage...');
        msg.style.cssText = 'padding:20px;color:var(--dim)';
        host.append(panelHost);
        wrap.append(chart);
        chart.append(msg);

        const setTrack = t => {
            st.track = t;
            writeUrl();
            if (this._inst) this._inst.setTrack(t);
            if (this._panel) this._panel.setTrack(t);
        };
        const split = () => st.lview === 'split';
        const chartView = () => (st.lview === 'tracks' || st.lview === 'ring' ? st.lview : 'tree');
        let started = false;
        // the panel's state is the page's (URL), read the way its Lineage tab hands it over (with the
        // columns it auto-opened); a filter or column change refetches the tree
        const fromTable = () => {
            const p = this._panel, s = p.state, u = p._lineageQs(), cur = new URLSearchParams(window.location.search);
            for (const k of ['layout', 'lview', 'rnest', 'rarc', 'rmin', 'rlist', 'rtier', 'rbundle', 'root']) if (cur.get(k)) u.set(k, cur.get(k));
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
            for (const k of ['q', 'scope', 'tab', 'group', 'columns', 'track']) st[k] = String(s[k] ?? D[k] ?? '');
            st.column = s.column || '';
            if (!started) return;
            const stale = apiQs().toString() !== loadedQs;
            if (this._inst) { if (!stale) this._inst.setLens(lensOf(st.group)); this._inst.setTrack(st.track); this._inst.select(split() ? st.column : null); }
            if (stale) load();
        };
        let ready;
        const stageReady = new Promise(r => { ready = r; });
        const mountPanel = () => {
            const u = new URLSearchParams(window.location.search), source = {}, state = {};
            for (const k of ['md5s', 'batch_uuid', 'collection', 'pool', 'cluster_uuid', 'axis']) if (u.get(k)) source[k] = u.get(k);
            for (const k of Object.keys(window.NwayPanel.DEFAULTS)) if (u.has(k)) state[k] = u.get(k);
            for (const k of KEYS) if (u.has(k) && !['lview', 'rnest', 'rarc', 'rmin', 'rlist', 'rtier', 'rbundle'].includes(k)) state[k] = u.get(k);
            this._panel = new window.NwayPanel(panelHost, {
                source, state,
                lean: true,
                noTable: !split(),
                fileModes: ['files', 'twins'],
                backHref,
                // the chart is re-parented into the slot after every render of the controls
                onStage: slot => { (slot || panelHost).append(wrap); ready(); },
                onState: fromTable,
                onData: fromTable,
                onTrack: t => { if (t !== st.track) setTrack(t); },
                onRowHover: row => { if (this._inst) this._inst.hover(row); },
            });
            this._panel.load();
        };
        const applyView = () => {
            for (const x of viewBtns) x.b.setAttribute('aria-pressed', String(st.lview === x.v));
            wrap.classList.toggle('lin-split', split());
            if (this._panel) this._panel.setTable(split());
            if (this._inst) { this._inst.setView(chartView()); this._inst.select(split() ? st.column : null); }
        };

        let seq = 0, loadedQs = '';
        // The Ring opens merged (twin) files of a cluster: the server opens the column groups listed in col_open, so send the ones
        // already open (the group ids on the nodes' paths, as the table spells out its `*` wildcard) plus these, keep the answer's
        // state in the URL and the panel (no navigation), and hand the doc back for the Ring to merge into what it shows.
        const openNodes = async ids => {
            const doc0 = this._inst.state.doc, open = new Set();
            for (const n of doc0.nodes) for (const g of n.path || []) open.add(g.id);
            for (const id of ids) open.add(id);
            const colOpen = JSON.stringify([...open]);
            const doc = await get({ col_open: colOpen });
            doc.scope = st.scope;
            const u = new URLSearchParams(window.location.search);
            u.set('col_open', colOpen);
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
            if (this._panel) this._panel.state.col_open = colOpen;
            loadedQs = apiQs().toString();
            return doc;
        };
        const load = async () => {
            const my = ++seq;
            loadedQs = apiQs().toString();
            status.textContent = 'Loading…';
            let doc;
            try {
                doc = await get();
            } catch (err) {
                if (tok !== this._tok || my !== seq) return;
                status.textContent = '';
                if (this._inst) { this._inst.destroy(); this._inst = null; }
                chart.replaceChildren(views);
                const m = mk('div', {}, `${err.message || err}${cols() === 'twins' ? ' (Same code merged the files: try Merge files: None.)' : ''}`);
                m.style.cssText = 'padding:20px;color:#f92672';
                chart.append(m);
                return;
            }
            if (tok !== this._tok || my !== seq) return;
            status.textContent = '';
            rootSel.replaceChildren(mk('option', { value: '' }, 'root: largest file'));
            for (const n of doc.nodes) {
                const o = mk('option', { value: n.id }, `${n.file_name || n.id}${(n.members || []).length > 1 ? ` ×${n.members.length}` : ''}`);
                o.selected = n.id === st.root;
                rootSel.append(o);
            }
            if (!this._inst) {
                chart.replaceChildren();
                this._inst = NL.mount(chart, {
                    toolbar: [views, layoutSel, rootSel, status],
                    fetchFile: id => get({ file: id }),
                    lens: lensOf(st.group),
                    track: st.track,
                    view: chartView(),
                    ring: { nest: st.rnest !== '0', arc: st.rarc, min: Number(st.rmin), list: st.rlist === '1', tier: st.rtier, bundle: st.rbundle !== '0', openNodes },
                    onRing: o => { st.rnest = o.nest ? '1' : '0'; st.rarc = o.arc; st.rmin = String(o.min); st.rlist = o.list ? '1' : ''; st.rtier = o.tier === 'hide' ? 'hide' : ''; st.rbundle = o.bundle ? '' : '0'; writeUrl(); },
                    laneNames: q.get('lane_names') !== 'off',
                    // split: a group's call graph or its table opens in the table below, not on a new page
                    onGroup: (what, tk) => {
                        const p = split() ? this._panel : null;
                        if (!p) return false;
                        if (what === 'graph') p.laneGraph(tk);
                        else setTrack(tk);
                        return true;
                    },
                    onTrack: t => { st.track = t; writeUrl(); if (this._panel) this._panel.setTrack(t); },
                    // split: a node is the table's column filter (a second click clears it), not the dock
                    onNode: n => {
                        if (!split()) return false;
                        this._panel._set({ column: st.column === n.id ? '' : n.id, offset: 0 });
                        return true;
                    },
                });
            }
            for (const w of doc.warnings || []) console.warn('lineage:', w);
            doc.scope = st.scope;
            this._inst.setDoc(doc, lensOf(st.group));
            if (split()) this._inst.select(st.column);
        };
        layoutSel.addEventListener('change', () => { st.layout = layoutSel.value; writeUrl(); load(); });
        rootSel.addEventListener('change', () => { st.root = rootSel.value; writeUrl(); load(); });
        mountPanel();
        applyView();
        await stageReady;
        started = true;
        await load();
    },
};
