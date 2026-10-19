/**
 * Lineage Sankey of the N-way panel's files (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(panel) is called after every panel render: it adds a
 * "Lineage" toggle and, when on, a Sankey above the table. One node per file
 * (height = function count); per parent -> child edge three separate bands:
 * shared (grey, between the nodes), born (blue, from the top lane into the
 * child) and died (orange, from the parent into the bottom lane). The part of
 * born/died that looks like a matcher miss (recall_suspect) is a hatched
 * sub-band of the same colour. d3-sankey places the nodes (plain stacking if
 * it is missing); the ribbons are hand-built SVG. Click an edge or node for numbers.
 * Everything from a sample (file names, md5s) is set with textContent / SVG
 * attributes, never innerHTML.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const ORDERS = [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']];
    const NODE_W = 110, PAD = 30, LANE_H = 56, PADDING = 36;
    // colour-blind-safe pair (Okabe-Ito); lane position carries the meaning as well
    const BLUE = '#0072B2', ORANGE = '#D55E00', GREY = '#8a8f98';
    let uid = 0;

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

    // Node boxes: d3.sankey sets x0/x1/y0/y1 (height = function count through
    // fixedValue); without d3-sankey a plain column stacker gives the same fields.
    function placeNodes(doc, W, H) {
        const nodes = doc.nodes.map(n => ({ ...n }));
        const links = doc.edges.map(e => ({ source: e.parent, target: e.child, value: e.shared, edge: e }));
        const ext = [[PAD, LANE_H + 20], [W - PAD, H - LANE_H - 20]];
        if (window.d3 && d3.sankey) {
            nodes.forEach(n => { n.fixedValue = Math.max(1, n.functions); });
            d3.sankey().nodeId(n => n.id).nodeWidth(NODE_W).nodePadding(PADDING).nodeAlign(d3.sankeyLeft)
                .nodeSort(null).extent(ext).iterations(0)({ nodes, links });
        } else {
            const byId = new Map(nodes.map(n => [n.id, n]));
            const dep = depths(doc);
            const cols = new Map();
            nodes.forEach(n => { n.depth = dep.get(n.id); (cols.get(n.depth) || cols.set(n.depth, []).get(n.depth)).push(n); });
            const nc = Math.max(...cols.keys()) + 1;
            let ky = Infinity;
            for (const c of cols.values()) ky = Math.min(ky, (ext[1][1] - ext[0][1] - PADDING * (c.length - 1)) / c.reduce((a, n) => a + Math.max(1, n.functions), 0));
            for (const [d, c] of cols) {
                let y = ext[0][1];
                for (const n of c) {
                    n.x0 = ext[0][0] + (nc > 1 ? d * (ext[1][0] - ext[0][0] - NODE_W) / (nc - 1) : 0);
                    n.x1 = n.x0 + NODE_W; n.y0 = y; n.y1 = y + Math.max(1, n.functions) * ky; y = n.y1 + PADDING;
                }
            }
            links.forEach(l => { l.source = byId.get(l.source); l.target = byId.get(l.target); });
        }
        return { nodes, links };
    }

    function depths(doc) {
        const d = new Map([[doc.root, 0]]);
        for (const e of doc.edges) d.set(e.child, d.get(e.parent) + 1);
        return d;
    }

    // Slots along a node's top or bottom edge, one per edge: widths are shares of
    // the node width (flow / the node's functions), squeezed when they overflow.
    function slots(items, node, key) {
        const f = items.map(it => it.edge[key] / Math.max(1, node.functions));
        const q = Math.min(1, 1 / Math.max(f.reduce((a, b) => a + b, 0), 1e-9));
        let at = 0;
        return items.map((it, i) => { const s = [node.x0 + at * q * NODE_W, f[i] * q * NODE_W]; at += f[i]; return s; });
    }

    function draw(doc, onPick, width) {
        const dep = depths(doc);
        const sizes = new Map();
        for (const v of dep.values()) sizes.set(v, (sizes.get(v) || 0) + 1);
        const W = Math.max(width || 600, (Math.max(...dep.values()) + 1) * (NODE_W + 150) + 2 * PAD);
        const H = Math.max(440, 140 * Math.max(...sizes.values()) + 2 * LANE_H + 60);
        const { nodes, links } = placeNodes(doc, W, H);
        const name = new Map(nodes.map(n => [n.id, n.file_name || n.id]));
        uid++;
        const s = svg('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': 'Lineage of the files: shared, born and died code' });
        const defs = svg('defs');
        for (const [id, col] of [['hatchBorn', BLUE], ['hatchDied', ORANGE]]) {
            const p = svg('pattern', { id: `${id}${uid}`, width: 6, height: 6, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)' });
            p.append(svg('rect', { width: 6, height: 6, fill: col, 'fill-opacity': 0.2 }), svg('rect', { width: 3, height: 6, fill: col }));
            defs.append(p);
        }
        s.append(defs);
        const total = k => doc.edges.reduce((a, e) => a + e[k], 0);
        const lane = (y, label, col, n) => s.append(
            svg('rect', { x: 0, y, width: W, height: LANE_H, fill: col, 'fill-opacity': 0.1 }),
            svg('text', { x: 8, y: y + 20, 'font-size': 12, 'font-weight': 600, fill: 'currentColor' }, `${label}: ${n} functions`));
        lane(0, 'BORN (gained by the child)', BLUE, total('gained'));
        lane(H - LANE_H, 'DIED (lost from the parent)', ORANGE, total('lost'));

        const ky = Math.min(...nodes.map(n => (n.y1 - n.y0) / Math.max(1, n.functions)));
        const outs = new Map(nodes.map(n => [n, []])), ins = new Map(nodes.map(n => [n, []]));
        links.forEach(l => { outs.get(l.source).push(l); ins.get(l.target).push(l); });
        const diedAt = new Map(), bornAt = new Map();
        for (const n of nodes) {
            const ls = outs.get(n).sort((a, b) => a.target.y0 - b.target.y0);
            const ws = ls.map(l => Math.max(1, l.value * ky));
            const sq = Math.min(1, (n.y1 - n.y0) / Math.max(ws.reduce((a, b) => a + b, 0), 1e-9));
            let cum = 0;
            ls.forEach((l, i) => { l.w = Math.max(1, ws[i] * sq); l.ys = n.y0 + (cum + ws[i] / 2) * sq; cum += ws[i]; l.yt = l.target.y0 + Math.max(1, l.value * ky) / 2; });
            slots(ls, n, 'lost').forEach((sl, i) => diedAt.set(ls[i].edge, sl));
            const is = ins.get(n);
            slots(is, n, 'gained').forEach((sl, i) => bornAt.set(is[i].edge, sl));
        }

        const tip = el('div', { style: 'position:fixed;pointer-events:none;display:none;z-index:50;padding:6px 8px;border-radius:4px;background:#1f2328;color:#fff;font-size:12px;line-height:1.4;white-space:pre' });
        document.body.append(tip);
        s._cleanup = () => tip.remove();
        const band = (x, y, w, h, kind, hatch) => {
            const col = kind === 'born' ? BLUE : ORANGE;
            return svg('rect', {
                x, y, width: Math.max(1, w), height: Math.max(0, h), 'fill-opacity': hatch ? 1 : 0.8,
                fill: hatch ? `url(#${kind === 'born' ? 'hatchBorn' : 'hatchDied'}${uid})` : col,
                stroke: hatch ? col : 'none', 'stroke-width': 0.5,
            });
        };
        const edgeGs = [];
        for (const l of links) {
            const e = l.edge, g = svg('g', { style: 'cursor:pointer;transition:opacity .15s' });
            const midX = (l.source.x1 + l.target.x0) / 2;
            const grey = svg('path', {
                d: `M${l.source.x1},${l.ys} C${midX},${l.ys} ${midX},${l.yt} ${l.target.x0},${l.yt}`,
                fill: 'none', stroke: GREY, 'stroke-width': l.w, 'stroke-opacity': 0.45,
            });
            g.append(grey);
            // the suspect part of a lane is a hatched sub-band of the same colour
            if (e.gained > 0) {
                const [x, w] = bornAt.get(e), hs = w * Math.min(e.recall_suspect, e.gained) / e.gained;
                g.append(band(x, LANE_H, w - hs, l.target.y0 - LANE_H, 'born'), band(x + w - hs, LANE_H, hs, l.target.y0 - LANE_H, 'born', true));
            }
            if (e.lost > 0) {
                const [x, w] = diedAt.get(e), hs = w * Math.min(e.recall_suspect, e.lost) / e.lost;
                const y = l.source.y1, h = H - LANE_H - y;
                g.append(band(x, y, w - hs, h, 'died'), band(x + w - hs, y, hs, h, 'died', true));
            }
            const text = `${name.get(e.parent)} → ${name.get(e.child)}\nborn ${e.gained}   died ${e.lost}   shared ${e.shared}\nrecall suspect ${e.recall_suspect}`;
            g.addEventListener('mouseenter', () => {
                edgeGs.forEach(o => { o.style.opacity = o === g ? 1 : 0.12; });
                grey.setAttribute('stroke-opacity', 0.85);
                tip.textContent = text; tip.style.display = 'block';
            });
            g.addEventListener('mousemove', ev => { tip.style.left = `${ev.clientX + 12}px`; tip.style.top = `${ev.clientY + 12}px`; });
            g.addEventListener('mouseleave', () => {
                edgeGs.forEach(o => { o.style.opacity = 1; });
                grey.setAttribute('stroke-opacity', 0.45);
                tip.style.display = 'none';
            });
            g.addEventListener('click', () => onPick({ edge: e }));
            edgeGs.push(g);
            s.append(g);
        }
        for (const n of nodes) {
            const g = svg('g', { style: 'cursor:pointer' });
            const full = name.get(n.id);
            const label = full.length > 16 ? `${full.slice(0, 15)}…` : full;
            g.append(svg('title', {}, `${full} (${n.functions} functions)`),
                svg('rect', { x: n.x0, y: n.y0, width: NODE_W, height: Math.max(2, n.y1 - n.y0), fill: n.root ? '#1d4ed8' : '#4b5563', rx: 2 }));
            if (n.y1 - n.y0 >= 30) {
                g.append(svg('text', { x: n.x0 + 6, y: n.y0 + 15, 'font-size': 11, fill: '#fff' }, label),
                    svg('text', { x: n.x0 + 6, y: n.y0 + 28, 'font-size': 10, fill: '#fff' }, `${n.functions} fn`));
            } else {
                g.append(svg('text', { x: n.x0, y: n.y0 - 4, 'font-size': 10, fill: 'currentColor' }, `${label} (${n.functions})`));
            }
            g.addEventListener('click', () => onPick({ node: n }));
            s.append(g);
        }
        return { svg: s, byId: new Map(nodes.map(n => [n.id, n])) };
    }

    function legend() {
        const box = el('div', { style: 'display:flex;flex-wrap:wrap;gap:14px;font-size:0.8rem;padding:6px 0;align-items:center' });
        const item = (fill, text, hatch) => {
            const w = el('span', { style: 'display:inline-flex;align-items:center;gap:5px' });
            const bg = hatch ? `background:repeating-linear-gradient(45deg,${fill} 0 2px,transparent 2px 4px);` : `background:${fill};`;
            w.append(el('span', { style: `display:inline-block;width:16px;height:10px;${bg}border:1px solid ${fill}` }), el('span', {}, text));
            return w;
        };
        box.append(item(GREY, 'shared (parent to child)'), item(BLUE, 'born (top lane)'), item(ORANGE, 'died (bottom lane)'),
            item(BLUE, 'hatched: likely a matcher miss', true), el('span', { class: 'dim' }, 'height = functions (feature weight is not returned by the API)'));
        return box;
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
        if (st.cleanup) st.cleanup();
        st.cleanup = null;
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
        const { svg: tree, byId } = draw(doc, pick => detail(side, pick, byId), body.clientWidth - 260);
        st.cleanup = tree._cleanup;
        const scroller = el('div', { style: 'overflow:auto;flex:1' });
        scroller.append(tree);
        const wrap = el('div', { style: 'display:flex' });
        wrap.append(scroller, side);
        body.append(legend(), wrap);
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
