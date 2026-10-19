/**
 * Lineage Sankey of the N-way panel's files (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(panel) is called after every panel render: it adds a
 * "Lineage" toggle and, when on, a Sankey above the table. One node per file
 * (height = function count); per parent -> child edge three separate bands:
 * shared (grey, between the nodes), born (blue, from the top lane into the
 * child) and died (orange, from the parent into the bottom lane). The part of
 * born/died that looks like a matcher miss (recall_suspect) is a hatched
 * sub-band of the same colour. d3.sankey builds the graph (born sources and
 * died sinks are real nodes), d3.sankeyLinkHorizontal draws the ribbons. Click an edge or node for numbers.
 * Everything from a sample (file names, md5s) is set with textContent / SVG
 * attributes, never innerHTML.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const ORDERS = [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']];
    const NODE_W = 110, BAND_W = 12, GAP = 230, PAD = 30, PADDING = 28, BAND_PAD = 6;
    const LANE_PAD = 34, BAND_H = 200, FILE_H = 420, KY_MAX = 1.5;
    // colour-blind-safe pair (Okabe-Ito): blue is only born, orange only died
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

    // File depth from the root; a file's layer is 2 * depth, the layers between
    // hold the BORN sources and DIED sinks of the edges that cross that gap.
    function depths(doc) {
        const d = new Map([[doc.root, 0]]);
        for (const e of doc.edges) d.set(e.child, d.get(e.parent) + 1);
        return d;
    }

    // d3.sankey builds the graph (node values, link lists, link breadths); we
    // fix the geometry afterwards so every flow shares one px-per-function scale
    // and the born / died nodes sit in their own bands.
    function layout(doc, width) {
        const dep = depths(doc);
        const maxd = Math.max(...dep.values());
        const files = doc.nodes.map(n => ({ ...n, kind: 'file', sid: `f:${n.id}`, d: dep.get(n.id) }));
        const fileOf = new Map(files.map(n => [n.id, n]));
        const nodes = [...files], links = [];
        doc.edges.forEach((e, i) => {
            const p = fileOf.get(e.parent), c = fileOf.get(e.child);
            const sus = e.recall_suspect;
            links.push({ source: p.sid, target: c.sid, value: Math.max(e.shared, 0), kind: 'shared', edge: e });
            if (e.gained > 0) {
                const b = { kind: 'born', sid: `b:${i}`, d: c.d, edge: e };
                nodes.push(b);
                const s = Math.min(sus, e.gained);
                links.push({ source: b.sid, target: c.sid, value: e.gained - s, kind: 'born', edge: e });
                if (s > 0) links.push({ source: b.sid, target: c.sid, value: s, kind: 'born', hatch: true, edge: e });
            }
            if (e.lost > 0) {
                const x = { kind: 'died', sid: `d:${i}`, d: p.d, edge: e };
                nodes.push(x);
                const s = Math.min(sus, e.lost);
                links.push({ source: p.sid, target: x.sid, value: e.lost - s, kind: 'died', edge: e });
                if (s > 0) links.push({ source: p.sid, target: x.sid, value: s, kind: 'died', hatch: true, edge: e });
            }
        });
        const graph = { nodes, links: links.filter(l => l.value > 0 || l.kind === 'shared') };
        const sankey = d3.sankey().nodeId(n => n.sid).nodeWidth(NODE_W).nodePadding(PADDING)
            .extent([[0, 0], [100, 100]]).iterations(0);
        sankey(graph);

        // node height in functions: max(functions, flow in, flow out); never squeezed
        const units = n => n.kind === 'file' ? Math.max(n.functions, n.value) : n.value;
        const cols = new Map();
        for (const n of files) (cols.get(n.d) || cols.set(n.d, []).get(n.d)).push(n);
        const stack = (kind, k) => {
            const m = new Map();
            for (const n of nodes) if (n.kind === kind) m.set(n.d, (m.get(n.d) || 0) + units(n) + BAND_PAD * 0);
            return Math.max(1, ...m.values(), k);
        };
        let ky = KY_MAX;
        for (const c of cols.values()) ky = Math.min(ky, (FILE_H - PADDING * (c.length - 1)) / c.reduce((a, n) => a + units(n), 0));
        ky = Math.min(ky, BAND_H / stack('born', 1), BAND_H / stack('died', 1));
        ky = Math.max(ky, 0.02);

        const colW = NODE_W + GAP;
        const W = Math.max(width || 600, 2 * PAD + (maxd + 1) * colW);
        const fileTop = LANE_PAD + BAND_H + 70;
        // files: left to right, each below its parent's top where there is room
        const order = [...files].sort((a, b) => a.d - b.d || doc.nodes.findIndex(n => n.id === a.id) - doc.nodes.findIndex(n => n.id === b.id));
        const parentOf = new Map(doc.edges.map(e => [e.child, e.parent]));
        const bottoms = new Map();
        let filesBottom = fileTop;
        for (const n of order) {
            n.x0 = PAD + n.d * colW; n.x1 = n.x0 + NODE_W;
            const par = fileOf.get(parentOf.get(n.id));
            n.y0 = Math.max((bottoms.get(n.d) ?? fileTop - PADDING) + PADDING, par ? par.y0 : fileTop);
            n.y1 = n.y0 + Math.max(2, units(n) * ky);
            bottoms.set(n.d, n.y1);
            filesBottom = Math.max(filesBottom, n.y1);
        }
        const diedTop = filesBottom + 70;
        const H = diedTop + BAND_H + LANE_PAD;
        // born sources: top band, left part of the gap before the child; died sinks: bottom band, right part of the gap after the parent
        const place = (kind, base, x0, byY) => {
            const per = new Map();
            nodes.filter(n => n.kind === kind).sort((a, b) => byY(a) - byY(b)).forEach(n => {
                const used = per.get(n.d) || 0;
                n.y0 = base + used; n.y1 = n.y0 + Math.max(2, units(n) * ky);
                per.set(n.d, n.y1 - base + BAND_PAD);
                n.x0 = x0(n); n.x1 = n.x0 + BAND_W;
            });
        };
        place('born', LANE_PAD, n => PAD + (n.d - 1) * colW + NODE_W + 30, n => fileOf.get(n.edge.child).y0);
        place('died', diedTop, n => PAD + n.d * colW + NODE_W + GAP - 30 - BAND_W, n => fileOf.get(n.edge.parent).y0);
        // links: one scale for all
        for (const l of graph.links) l.width = Math.max(1, l.value * ky);
        sankey.update(graph);
        return { graph, files, W, H, ky, filesBottom, diedTop, fileTop };
    }

    function draw(doc, onPick, width) {
        uid++;
        const L = layout(doc, width);
        const { graph, W, H } = L;
        const name = new Map(L.files.map(n => [n.id, n.file_name || n.id]));
        const s = d3.create('svg').attr('width', W).attr('height', H).attr('viewBox', `0 0 ${W} ${H}`)
            .attr('role', 'img').attr('aria-label', 'Lineage of the files: shared, born and died code');
        const defs = s.append('defs');
        for (const [kind, col] of [['born', BLUE], ['died', ORANGE]]) {
            const p = defs.append('pattern').attr('id', `hatch-${kind}-${uid}`).attr('width', 6).attr('height', 6)
                .attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            p.append('rect').attr('width', 6).attr('height', 6).attr('fill', col).attr('fill-opacity', 0.25);
            p.append('rect').attr('width', 3).attr('height', 6).attr('fill', col);
        }
        const bandTotal = k => doc.edges.reduce((a, e) => a + e[k], 0);
        const lane = (y, h, col, label) => {
            s.append('rect').attr('x', 0).attr('y', y).attr('width', W).attr('height', h).attr('fill', col).attr('fill-opacity', 0.08);
            s.append('text').attr('x', 8).attr('y', y + 16).attr('font-size', 12).attr('font-weight', 600).attr('fill', 'currentColor').text(label);
        };
        lane(0, LANE_PAD + BAND_H + 30, BLUE, `BORN: ${bandTotal('gained')} functions`);
        lane(L.diedTop - 30, H - L.diedTop + 30, ORANGE, `DIED: ${bandTotal('lost')} functions`);
        s.append('text').attr('x', 8).attr('y', L.fileTop - 14).attr('font-size', 12).attr('font-weight', 600).attr('fill', 'currentColor')
            .text('SHARED (kept from parent to child)');

        const tip = d3.select(document.body).append('div')
            .style('position', 'fixed').style('pointer-events', 'none').style('display', 'none').style('z-index', 50)
            .style('padding', '6px 8px').style('border-radius', '4px').style('background', '#1f2328').style('color', '#fff')
            .style('font-size', '12px').style('line-height', 1.4).style('white-space', 'pre');
        s.node()._cleanup = () => tip.remove();

        // grey ribbons fade parent -> child
        graph.links.filter(l => l.kind === 'shared').forEach((l, i) => {
            const g = defs.append('linearGradient').attr('id', `sh-${uid}-${i}`).attr('gradientUnits', 'userSpaceOnUse')
                .attr('x1', l.source.x1).attr('x2', l.target.x0);
            g.append('stop').attr('offset', 0).attr('stop-color', GREY).attr('stop-opacity', 0.65);
            g.append('stop').attr('offset', 1).attr('stop-color', GREY).attr('stop-opacity', 0.3);
            l.grad = `url(#sh-${uid}-${i})`;
        });
        const stroke = l => l.kind === 'shared' ? l.grad
            : l.hatch ? `url(#hatch-${l.kind}-${uid})` : l.kind === 'born' ? BLUE : ORANGE;
        const ribbons = s.append('g').selectAll('path').data(graph.links).join('path')
            .attr('d', d3.sankeyLinkHorizontal()).attr('fill', 'none').attr('stroke', stroke)
            .attr('stroke-width', l => l.width).attr('stroke-opacity', l => l.kind === 'shared' ? 1 : 0.85)
            .style('cursor', 'pointer');
        const text = e => `${name.get(e.parent)} → ${name.get(e.child)}\nborn ${e.gained}   died ${e.lost}   shared ${e.shared}\nrecall suspect ${e.recall_suspect}`;
        ribbons.on('mouseenter', (ev, l) => {
            ribbons.transition().duration(120).style('opacity', d => d.edge === l.edge ? 1 : 0.1);
            ribbons.filter(d => d.edge === l.edge).raise();
            tip.text(text(l.edge)).style('display', 'block');
        }).on('mousemove', ev => tip.style('left', `${ev.clientX + 12}px`).style('top', `${ev.clientY + 12}px`))
            .on('mouseleave', () => {
                ribbons.transition().duration(120).style('opacity', 1);
                tip.style('display', 'none');
            }).on('click', (ev, l) => onPick({ edge: l.edge }));

        const NODE_FILL = '#475569';
        const nodes = s.append('g').selectAll('g').data(graph.nodes).join('g').style('cursor', 'pointer');
        nodes.append('rect').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', n => n.x1 - n.x0)
            .attr('height', n => Math.max(2, n.y1 - n.y0)).attr('rx', 2)
            .attr('fill', n => n.kind === 'born' ? BLUE : n.kind === 'died' ? ORANGE : NODE_FILL);
        nodes.append('title').text(n => n.kind === 'file' ? `${name.get(n.id)} (${n.functions} functions)`
            : `${n.kind === 'born' ? 'born in' : 'died from'} ${name.get(n.kind === 'born' ? n.edge.child : n.edge.parent)}: ${n.value}`);
        const files = nodes.filter(n => n.kind === 'file');
        const trunc = n => { const f = name.get(n.id); return f.length > 16 ? `${f.slice(0, 15)}…` : f; };
        // a file too short for its label gets it above the box, in the page's text colour
        const tall = n => n.y1 - n.y0 >= 32;
        files.append('text').attr('x', n => n.x0 + (tall(n) ? 6 : 0)).attr('y', n => tall(n) ? n.y0 + 15 : n.y0 - 5)
            .attr('font-size', 11).attr('fill', n => tall(n) ? '#fff' : 'currentColor')
            .text(n => tall(n) ? trunc(n) : `${trunc(n)} (${n.functions} fn)`);
        files.filter(tall).append('text').attr('x', n => n.x0 + 6).attr('y', n => n.y0 + 28).attr('font-size', 10).attr('fill', '#fff')
            .text(n => `${n.functions} fn`);
        files.on('click', (ev, n) => onPick({ node: n }));
        return { svg: s.node(), byId: new Map(L.files.map(n => [n.id, n])) };
    }

    function legend() {
        const box = el('div', { style: 'display:flex;flex-wrap:wrap;gap:14px;font-size:0.8rem;padding:6px 0;align-items:center' });
        const item = (fill, text, hatch) => {
            const w = el('span', { style: 'display:inline-flex;align-items:center;gap:5px' });
            const bg = hatch ? `background:repeating-linear-gradient(45deg,${fill} 0 2px,transparent 2px 4px);` : `background:${fill};`;
            w.append(el('span', { style: `display:inline-block;width:16px;height:10px;${bg}border:1px solid ${fill}` }), el('span', {}, text));
            return w;
        };
        box.append(item(GREY, 'shared'), item(BLUE, 'born'), item(ORANGE, 'died'),
            item(GREY, 'hatched: likely a matcher miss (blue in born, orange in died)', true),
            el('span', { class: 'dim' }, 'One scale: ribbon width and node height are in functions. A file is as tall as max(its functions, flow in, flow out). Feature weight is not returned by the API.'));
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
        if (!(window.d3 && d3.sankey)) {
            body.replaceChildren(el('div', { class: 'nway-note err' }, 'd3 / d3-sankey are not loaded.'));
            return;
        }
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
        // scroll inside a bounded box; zoom rescales the SVG (its viewBox keeps the drawing)
        const scroller = el('div', { style: 'overflow:auto;flex:1;max-height:75vh;min-width:0' });
        scroller.append(tree);
        const w0 = Number(tree.getAttribute('width')), h0 = Number(tree.getAttribute('height'));
        const zoom = z => {
            st.zoom = Math.max(0.1, Math.min(4, z));
            tree.setAttribute('width', w0 * st.zoom);
            tree.setAttribute('height', h0 * st.zoom);
        };
        const fit = () => zoom(Math.min(1, (scroller.clientWidth || w0) / w0));
        const bar = el('div', { style: 'display:flex;gap:6px;align-items:center;padding:4px 0' });
        for (const [t, f] of [['\u2212', () => zoom((st.zoom || 1) / 1.25)], ['+', () => zoom((st.zoom || 1) * 1.25)], ['Fit', fit], ['100%', () => zoom(1)]]) {
            const b = el('button', { class: 'view-btn', type: 'button' }, t);
            b.addEventListener('click', f);
            bar.append(b);
        }
        scroller.addEventListener('wheel', ev => {
            if (!ev.ctrlKey && !ev.metaKey) return;
            ev.preventDefault();
            zoom((st.zoom || 1) * (ev.deltaY < 0 ? 1.1 : 1 / 1.1));
        }, { passive: false });
        const wrap = el('div', { style: 'display:flex' });
        wrap.append(scroller, side);
        zoom(st.zoom || 1);
        body.append(bar);
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
