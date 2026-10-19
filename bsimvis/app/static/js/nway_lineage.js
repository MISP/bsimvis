/**
 * Lineage tree of the N-way panel's files (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(panel) is called after every panel render: it adds a
 * "Lineage" toggle and, when on, a static SVG tree above the table. Nodes are
 * circles sized by function count, edges run parent -> child, width by shared
 * functions, labelled "+gained -lost" and the recall_suspect count (orange when
 * most of the change looks like a matcher miss). Click a node or edge for numbers.
 * Everything from a sample (file names, md5s) is set with textContent / SVG
 * attributes, never innerHTML.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const ORDERS = [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']];
    const COL_W = 210, ROW_H = 90, PAD = 50;
    const R_MIN = 8, R_MAX = 30;
    const RECALL_HIGH = 0.3; // recall_suspect / (gained + lost) at which an edge turns orange
    const ORANGE = '#fd971f', GREY = '#8a8f98';

    function el(tag, attrs = {}, text) {
        const e = document.createElement(tag);
        for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
        if (text !== undefined) e.textContent = text;
        return e;
    }
    function svg(tag, attrs = {}, text) {
        const e = document.createElementNS(NS, tag);
        for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
        if (text !== undefined) e.textContent = text;
        return e;
    }

    // Depth = column, leaves stacked top to bottom, a parent centred on its children.
    function layout(doc) {
        const kids = new Map(doc.nodes.map(n => [n.id, []]));
        for (const e of doc.edges) kids.get(e.parent).push(e.child);
        const pos = new Map();
        let slot = 0;
        const walk = (id, depth) => {
            const cs = kids.get(id);
            let y;
            if (!cs.length) y = slot++;
            else { const ys = cs.map(c => walk(c, depth + 1)); y = (ys[0] + ys[ys.length - 1]) / 2; }
            pos.set(id, { x: PAD + depth * COL_W, y: PAD + y * ROW_H });
            return y;
        };
        walk(doc.root, 0);
        return pos;
    }

    function draw(doc, onPick) {
        const pos = layout(doc);
        const w = Math.max(...[...pos.values()].map(p => p.x)) + PAD + 140;
        const h = Math.max(...[...pos.values()].map(p => p.y)) + PAD;
        const s = svg('svg', { width: w, height: h, viewBox: `0 0 ${w} ${h}` });
        const maxF = Math.max(1, ...doc.nodes.map(n => n.functions));
        const maxS = Math.max(1, ...doc.edges.map(e => e.shared));
        const radius = n => Math.max(R_MIN, Math.min(R_MAX, R_MIN + (R_MAX - R_MIN) * Math.sqrt(n.functions / maxF)));
        const byId = new Map(doc.nodes.map(n => [n.id, n]));
        for (const e of doc.edges) {
            const a = pos.get(e.parent), b = pos.get(e.child);
            const change = e.gained + e.lost;
            const hot = change > 0 && e.recall_suspect / change >= RECALL_HIGH;
            const g = svg('g', { style: 'cursor:pointer' });
            const line = svg('line', {
                x1: a.x, y1: a.y, x2: b.x, y2: b.y, stroke: hot ? ORANGE : GREY,
                'stroke-width': (1 + 7 * Math.sqrt(e.shared / maxS)).toFixed(1), 'stroke-opacity': 0.7,
            });
            const hit = svg('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, stroke: 'transparent', 'stroke-width': 14 });
            const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
            g.append(line, hit,
                svg('text', { x: mx, y: my - 8, 'text-anchor': 'middle', 'font-size': 11, fill: 'currentColor' }, `+${e.gained} -${e.lost}`),
                svg('text', { x: mx, y: my + 14, 'text-anchor': 'middle', 'font-size': 10, fill: hot ? ORANGE : GREY }, `${e.recall_suspect} suspect`));
            g.addEventListener('click', () => onPick({ edge: e }));
            s.append(g);
        }
        for (const n of doc.nodes) {
            const p = pos.get(n.id);
            const g = svg('g', { style: 'cursor:pointer' });
            g.append(
                svg('circle', { cx: p.x, cy: p.y, r: radius(n), fill: n.root ? '#3b82f6' : '#6b7280', 'fill-opacity': 0.85, stroke: 'currentColor' }),
                svg('text', { x: p.x, y: p.y + radius(n) + 14, 'text-anchor': 'middle', 'font-size': 11, fill: 'currentColor' }, (n.file_name || n.id).slice(0, 24)),
                svg('text', { x: p.x, y: p.y + 4, 'text-anchor': 'middle', 'font-size': 10, fill: '#fff' }, String(n.functions)));
            g.addEventListener('click', () => onPick({ node: n }));
            s.append(g);
        }
        return { svg: s, byId };
    }

    function detail(box, pick, byId) {
        box.replaceChildren();
        const line = t => box.append(el('div', {}, t));
        if (pick.node) {
            box.append(el('b', {}, pick.node.file_name || pick.node.id));
            line(`${pick.node.functions} functions${pick.node.root ? ' (root)' : ''}`);
            line(pick.node.id);
        } else if (pick.edge) {
            const e = pick.edge, nm = id => (byId.get(id) || {}).file_name || id;
            box.append(el('b', {}, `${nm(e.parent)} → ${nm(e.child)}`));
            line(`similarity ${e.similarity}`);
            line(`shared ${e.shared}, gained ${e.gained}, lost ${e.lost}`);
            line(`recall suspect ${e.recall_suspect}`);
            const ev = e.recall_evidence || {};
            line(`  below the floor: ${ev.edge_below_floor}`);
            line(`  refused by one-per-file: ${ev.edge_refused}`);
            line(`  same name: ${ev.name}`);
        }
    }

    async function render(panel, body) {
        const st = panel._lineage;
        body.replaceChildren(el('div', { class: 'dim' }, 'Loading lineage...'));
        const qs = panel._listQs();
        qs.delete('offset');
        qs.set('columns', 'files');
        qs.set('order', st.order);
        if (st.root) qs.set('root', st.root);
        let doc;
        try {
            const res = await fetch(`/api/bin_sim/nway/lineage?${qs.toString()}`, { signal: AbortSignal.timeout(180000) });
            doc = await res.json();
            if (!res.ok) throw new Error(doc.message || doc.error || `HTTP ${res.status}`);
        } catch (err) {
            body.replaceChildren(el('div', { class: 'nway-note err' }, String(err.message || err)));
            return;
        }
        body.replaceChildren();
        for (const w of doc.warnings || []) body.append(el('div', { class: 'nway-note warn' }, w));
        const side = el('div', { style: 'min-width:220px;max-width:300px;font-size:0.85rem;padding:0 12px' }, 'Click a file or an edge.');
        const { svg: tree, byId } = draw(doc, pick => detail(side, pick, byId));
        const scroller = el('div', { style: 'overflow:auto;flex:1' });
        scroller.append(tree);
        const wrap = el('div', { style: 'display:flex' });
        wrap.append(scroller, side);
        body.append(wrap);
    }

    function mount(panel) {
        const st = panel._lineage || (panel._lineage = { on: false, order: 'tree', root: '' });
        const host = panel.el.querySelector('.nway-panel');
        if (!host) return;
        const bar = el('div', { class: 'nway-note', style: 'display:flex;gap:8px;align-items:center' });
        const toggle = el('button', { class: `view-btn${st.on ? ' active' : ''}` }, 'Lineage');
        bar.append(toggle);
        const body = el('div', { class: 'nway-card' });
        body.style.display = st.on ? '' : 'none';
        const card = host.querySelector('.nway-card');
        const draw_ = () => render(panel, body);
        if (st.on) {
            const order = el('select', { class: 'view-btn' });
            for (const [v, t] of ORDERS) {
                const o = el('option', { value: v }, t);
                if (v === st.order) o.selected = true;
                order.append(o);
            }
            order.addEventListener('change', () => { st.order = order.value; draw_(); });
            const root = el('select', { class: 'view-btn' });
            root.append(el('option', { value: '' }, 'root: largest file'));
            const cols = (panel.data && (panel.data.file_columns || panel.data.columns)) || [];
            for (const c of cols) {
                if (!c.md5) continue;
                const o = el('option', { value: c.id }, c.file_name || c.md5);
                if (c.id === st.root) o.selected = true;
                root.append(o);
            }
            root.addEventListener('change', () => { st.root = root.value; draw_(); });
            bar.append(order, root);
            if (card) card.style.display = 'none';
        }
        toggle.addEventListener('click', () => { st.on = !st.on; panel.render(); });
        host.prepend(body);
        host.prepend(bar);
        if (st.on) draw_();
    }

    window.NwayLineage = { mount };
})();
