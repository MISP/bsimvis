/**
 * N-way lineage view (full page):
 * /diff/nway/lineage?<same set params as /diff/nway>[&order=tree|chain|star][&root=<file id>]
 *
 * Routing, URL state and fetching only; the chart is nway_lineage.js
 * (NwayLineage.mount). order and root are written back to the URL.
 */
window.NwayLineageView = {
    _inst: null,
    _tok: 0,

    destroy() {
        this._tok++;
        if (this._inst) this._inst.destroy();
        this._inst = null;
    },

    async init(params, containerId) {
        this.destroy();
        const host = document.getElementById(containerId);
        if (!host) return;
        const tok = this._tok;
        const q = new URLSearchParams(window.location.search);
        const st = { order: q.get('order') || 'tree', root: q.get('root') || '' };
        const apiQs = extra => {
            const out = new URLSearchParams(window.location.search);
            for (const k of ['order', 'root', 'file', 'tab', 'offset']) out.delete(k);
            out.set('columns', 'files');
            out.set('order', st.order);
            if (st.root) out.set('root', st.root);
            for (const [k, v] of Object.entries(extra || {})) out.set(k, v);
            return out;
        };
        const get = async extra => {
            const res = await fetch(`/api/bin_sim/nway/lineage?${apiQs(extra).toString()}`, { signal: AbortSignal.timeout(180000) });
            const doc = await res.json();
            if (!res.ok) throw new Error(doc.message || doc.error || `HTTP ${res.status}`);
            return doc;
        };

        const order = document.createElement('select');
        order.setAttribute('aria-label', 'Lineage order');
        for (const [v, t] of [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']]) {
            const o = document.createElement('option');
            o.value = v; o.textContent = t; o.selected = v === st.order;
            order.append(o);
        }
        const rootSel = document.createElement('select');
        rootSel.setAttribute('aria-label', 'Lineage root file');
        const back = document.createElement('a');
        back.className = 'lin-btn';
        back.style.textDecoration = 'none';
        back.textContent = '← N-way table';
        back.title = 'Back to the N-way diff table';
        const bq = new URLSearchParams(window.location.search);
        bq.delete('order'); bq.delete('root');
        back.href = window.NwayPanel ? window.NwayPanel.backUrl(bq) : `/diff/nway?${bq.toString()}`;
        back.addEventListener('click', e => { if (window.Nav) window.Nav.openPath(back.getAttribute('href'), e); });

        host.replaceChildren();
        const wrap = document.createElement('div');
        wrap.style.cssText = 'flex:1;min-height:0;width:100%;position:relative;display:flex;flex-direction:column;overflow:hidden';
        const msg = document.createElement('div');
        msg.style.cssText = 'padding:20px;color:var(--dim)';
        msg.textContent = 'Loading lineage...';
        host.append(wrap);
        wrap.append(msg);

        const writeUrl = () => {
            const u = new URLSearchParams(window.location.search);
            u.set('order', st.order);
            if (st.root) u.set('root', st.root); else u.delete('root');
            history.replaceState(history.state, '', `${window.location.pathname}?${u.toString()}`);
        };
        let first = true;
        const load = async () => {
            let doc;
            try {
                doc = await get();
            } catch (err) {
                if (tok !== this._tok) return;
                if (this._inst) { this._inst.destroy(); this._inst = null; }
                wrap.replaceChildren();
                wrap.append(back);
                const m = document.createElement('div');
                m.style.cssText = 'padding:20px;color:#f92672';
                m.textContent = String(err.message || err);
                wrap.append(m);
                return;
            }
            if (tok !== this._tok) return;
            if (first) {
                rootSel.replaceChildren();
                const o0 = document.createElement('option');
                o0.value = ''; o0.textContent = 'root: largest file';
                rootSel.append(o0);
                for (const n of doc.nodes) {
                    const o = document.createElement('option');
                    o.value = n.id; o.textContent = n.file_name || n.id; o.selected = n.id === st.root;
                    rootSel.append(o);
                }
                wrap.replaceChildren();
                this._inst = window.NwayLineage.mount(wrap, {
                    toolbar: [back, order, rootSel],
                    fetchFile: id => get({ file: id }),
                });
                first = false;
            }
            for (const w of doc.warnings || []) console.warn('lineage:', w);
            this._inst.setDoc(doc);
        };
        order.addEventListener('change', () => { st.order = order.value; writeUrl(); load(); });
        rootSel.addEventListener('change', () => { st.root = rootSel.value; writeUrl(); load(); });
        await load();
    },
};
