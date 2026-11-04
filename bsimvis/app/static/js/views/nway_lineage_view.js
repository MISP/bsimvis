/**
 * N-way lineage view (full page):
 * /diff/nway/lineage?<same set params as /diff/nway>[&layout=tree|chain|star][&root=<file id>][&lview=tree|tracks|split]
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
        const D = { q: '', scope: 'code', tab: 'all', group: 'region,role', columns: 'twins', track: '', ...(window.NwayPanel ? window.NwayPanel.DEFAULTS : {}), lview: 'tree' };
        const KEYS = ['q', 'scope', 'tab', 'group', 'columns', 'track', 'lview'];
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
            for (const k of ['order', 'layout', 'lview', 'root', 'file', 'offset', 'track', 'lane_names', 'lane_graph']) out.delete(k);
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
            if (res.status === 400 && cols() === 'twins' && /columns/.test(doc.message || doc.error || '')) { twinsOk = false; renderBar(null); return get(extra); }
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
        const back = mk('a', { class: 'lin-btn', title: 'Back to the N-way diff table, with the same filters, lens and track' }, '← N-way table');
        back.style.textDecoration = 'none';
        const backHref = () => {
            const bq = NL.tableQs(window.location.search);
            return window.NwayPanel ? window.NwayPanel.backUrl(bq) : `/diff/nway?${bq.toString()}`;
        };
        back.href = backHref();
        back.addEventListener('click', e => { if (window.Nav) window.Nav.openPath(back.getAttribute('href'), e); });

        const writeUrl = () => {
            const u = new URLSearchParams(window.location.search);
            if (st.layout !== 'tree') u.set('layout', st.layout); else u.delete('layout');
            if (u.get('order') !== 'lineage') u.delete('order');
            if (st.root) u.set('root', st.root); else u.delete('root');
            for (const k of KEYS) if (st[k] !== String(D[k] ?? '')) u.set(k, st[k]); else u.delete(k);
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
            back.href = backHref();
        };

        // ---- Tree | Tracks | Split ----
        const views = mk('div', { class: 'lin-seg', role: 'group', 'aria-label': 'View' });
        const viewBtns = [['tree', 'Tree', 'fa-sitemap', 'The lineage tree'], ['tracks', 'Tracks', 'fa-route', 'Follow one group of the lens through the tree, or six at once'], ['split', 'Split', 'fa-table-columns', 'A compact tree over the N-way table, linked both ways']].map(([v, t, icon, title]) => {
            const b = mk('button', { type: 'button', title });
            b.append(mk('i', { class: `fa-solid ${icon}`, style: 'margin-right:4px' }), document.createTextNode(t));
            b.addEventListener('click', () => { if (st.lview === v) return; st.lview = v; writeUrl(); applyView(); });
            views.append(b);
            return { v, b };
        });

        // ---- the table's filter row: search, code axis, row tabs with counts, merge files, lens tabs ----
        const bar = mk('div', { class: 'lin-vars lin-tb lin-filters' });
        const sbox = mk('div', { class: 'search-input-wrapper nway-search' });
        const search = mk('input', { type: 'text', placeholder: 'Search functions', 'aria-label': 'Search functions' });
        search.value = st.q;
        sbox.append(mk('i', { class: 'fa-solid fa-magnifying-glass' }), search);
        const seg = (label, opts, key) => {
            const g = mk('div', { class: 'lin-seg', role: 'group', 'aria-label': label });
            const btns = opts.map(([v, t, icon]) => {
                const b = mk('button', { type: 'button' });
                if (icon) b.append(mk('i', { class: `fa-solid ${icon}`, style: 'margin-right:4px' }));
                b.append(document.createTextNode(t));
                const n = mk('span', { class: 'lin-n' });
                b.append(n);
                b.addEventListener('click', () => { if (st[key] === v) return; st[key] = v; writeUrl(); renderBar(null); load(); });
                g.append(b);
                return { v, b, n };
            });
            return { g, btns };
        };
        const scope = seg('Code axis', [['code', 'Code', 'fa-code'], ['library', 'Library', 'fa-cubes'], ['all', 'All', 'fa-layer-group']], 'scope');
        const tabs = seg('Functions', [['all', 'All'], ['core', 'Core'], ['partial', 'Partial'], ['unique', 'Unique']], 'tab');
        const merge = mk('select', { 'aria-label': 'Merge files' });
        for (const [v, t] of [['files', 'None'], ['twins', 'Same code']]) merge.append(mk('option', { value: v }, t));
        merge.addEventListener('change', () => { st.columns = merge.value; st.root = ''; writeUrl(); load(); });
        const lenses = mk('div', { class: 'bsim-tabbar nway-lenses', role: 'tablist', 'aria-label': 'Lens' });
        const status = mk('span', { class: 'lin-dim' });
        bar.append(sbox, mk('span', { class: 'lin-dim' }, 'Show'), scope.g, tabs.g, mk('span', { class: 'lin-dim' }, 'Merge files'), merge, lenses, status);
        search.addEventListener('input', () => {
            clearTimeout(this._typing);
            this._typing = setTimeout(() => { st.q = search.value.trim(); writeUrl(); load(); }, 350);
        });
        let lastDoc = null;
        const renderBar = doc => {
            if (doc) lastDoc = doc;
            const c = (lastDoc && lastDoc.counts) || null;
            for (const x of scope.btns) x.b.setAttribute('aria-pressed', String(st.scope === x.v));
            for (const x of tabs.btns) {
                x.b.setAttribute('aria-pressed', String(st.tab === x.v));
                const n = c && c[x.v] ? (st.scope === 'library' ? c[x.v].library : st.scope === 'all' ? c[x.v].code + c[x.v].library : c[x.v].code) : null;
                x.n.textContent = n == null ? '' : Number(n).toLocaleString();
            }
            // the table's search (split) shows up here, but never under the user's typing
            if (document.activeElement !== search) search.value = st.q;
            merge.value = cols();
            merge.options[1].disabled = !twinsOk;
            merge.options[1].title = twinsOk ? '' : 'This server builds lineage over files only';
            // without the row list only Deltas (the lineage's own colouring) is offered
            const rich = !!(lastDoc && lastDoc.rows);
            const cur = rich ? lensOf(st.group) : 'deltas';
            lenses.replaceChildren();
            for (const [k, label, group, icon] of NL.LENSES) {
                if (!rich && k !== 'deltas') continue;
                const b = mk('button', { type: 'button', role: 'tab', class: `bsim-tab${k === cur ? ' active' : ''}`, 'aria-selected': String(k === cur) });
                b.append(mk('i', { class: `fa-solid ${icon}` }), document.createTextNode(` ${label}`));
                // a lens is the table's groups for its preset: refetched, the tree stays
                b.addEventListener('click', () => { st.group = group; writeUrl(); renderBar(null); load(); });
                lenses.append(b);
            }
        };
        renderBar(null);

        host.replaceChildren();
        const wrap = mk('div');
        wrap.style.cssText = 'flex:1;min-height:0;width:100%;position:relative;display:flex;flex-direction:column;overflow:hidden';
        const chart = mk('div');
        // split: the real N-way table under the tree (nway-view / nway-fill: the table page's own layout rules)
        const table = mk('div', { class: 'nway-view nway-fill' });
        table.style.cssText = 'flex:1;min-height:0;padding:6px 12px;border-top:1px solid var(--border)';
        const msg = mk('div', {}, 'Loading lineage...');
        msg.style.cssText = 'padding:20px;color:var(--dim)';
        host.append(bar, wrap);
        wrap.append(chart, table);
        chart.append(msg);

        const setTrack = t => {
            st.track = t;
            writeUrl();
            if (this._inst) this._inst.setTrack(t);
            if (this._panel) this._panel.setTrack(t);
        };
        // split: the table's state is the page's (URL), read the way its Lineage tab hands it over (with the
        // columns it auto-opened); a filter or column change refetches the tree
        const fromTable = () => {
            const p = this._panel, s = p.state, u = p._lineageQs(), cur = new URLSearchParams(window.location.search);
            for (const k of ['layout', 'lview', 'root']) if (cur.get(k)) u.set(k, cur.get(k));
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
            for (const k of ['q', 'scope', 'tab', 'group', 'columns', 'track']) st[k] = String(s[k] ?? D[k] ?? '');
            st.column = s.column || '';
            back.href = backHref();
            renderBar(null);
            const stale = apiQs().toString() !== loadedQs;
            if (this._inst) { if (!stale) this._inst.setLens(lensOf(st.group)); this._inst.setTrack(st.track); this._inst.select(st.column); }
            if (stale) load();
        };
        const mountTable = () => {
            const u = new URLSearchParams(window.location.search), source = {}, state = {};
            for (const k of ['md5s', 'batch_uuid', 'collection', 'pool', 'cluster_uuid', 'axis']) if (u.get(k)) source[k] = u.get(k);
            for (const k of Object.keys(window.NwayPanel.DEFAULTS)) if (u.has(k)) state[k] = u.get(k);
            const inner = mk('div');
            inner.style.cssText = 'flex:1;min-height:0;display:flex;flex-direction:column';
            table.replaceChildren(inner);
            this._panel = new window.NwayPanel(inner, {
                source, state,
                onState: fromTable,
                onData: fromTable,
                onTrack: t => setTrack(t),
                onRowHover: row => { if (this._inst) this._inst.hover(row); },
            });
            this._panel.load();
        };
        const applyView = () => {
            for (const x of viewBtns) x.b.setAttribute('aria-pressed', String(st.lview === x.v));
            const split = st.lview === 'split' && !!window.NwayPanel;
            // the table brings its own filter row
            bar.style.display = split ? 'none' : '';
            chart.style.cssText = `display:flex;flex-direction:column;${split ? 'flex:none;height:42%;min-height:360px' : 'flex:1;min-height:0'}`;
            table.style.display = split ? '' : 'none';
            if (split && !this._panel) mountTable();
            if (!split && this._panel) { this._panel.destroy(); this._panel = null; table.replaceChildren(); }
            if (this._inst) { this._inst.setView(st.lview === 'tracks' ? 'tracks' : 'tree'); this._inst.select(split ? st.column : null); }
        };

        let seq = 0, loadedQs = '';
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
                chart.replaceChildren(back, views);
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
            renderBar(doc);
            if (!this._inst) {
                chart.replaceChildren();
                this._inst = NL.mount(chart, {
                    toolbar: [back, views, layoutSel, rootSel],
                    fetchFile: id => get({ file: id }),
                    lens: lensOf(st.group),
                    track: st.track,
                    view: st.lview === 'tracks' ? 'tracks' : 'tree',
                    laneNames: q.get('lane_names') !== 'off',
                    // split: a group's call graph or its table opens in the table below, not on a new page
                    onGroup: (what, tk) => {
                        const p = this._panel;
                        if (!p) return false;
                        if (what === 'graph') p.laneGraph(tk);
                        else setTrack(tk);
                        return true;
                    },
                    onTrack: t => { st.track = t; writeUrl(); if (this._panel) this._panel.setTrack(t); },
                    // split: a node is the table's column filter (a second click clears it), not the dock
                    onNode: n => {
                        if (!this._panel) return false;
                        this._panel._set({ column: st.column === n.id ? '' : n.id, offset: 0 });
                        return true;
                    },
                });
            }
            for (const w of doc.warnings || []) console.warn('lineage:', w);
            this._inst.setDoc(doc, lensOf(st.group));
            if (this._panel) this._inst.select(st.column);
        };
        layoutSel.addEventListener('change', () => { st.layout = layoutSel.value; writeUrl(); load(); });
        rootSel.addEventListener('change', () => { st.root = rootSel.value; writeUrl(); load(); });
        applyView();
        await load();
    },
};
