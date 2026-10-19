/**
 * Lineage Sankey of the N-way panel's files (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(panel) is called after every panel render: it adds a
 * "Lineage" toggle and, when on, a Sankey above the table. One node per file,
 * drawn at exactly its function count and cut into segments by which children
 * keep the code; flows are born (blue, from the top lane), shared (grey) and
 * died (orange, into the bottom lane), all on one px-per-function scale. The
 * part of born/died that looks like a matcher miss (recall_suspect) is a hatched
 * sub-band of the same colour. Ribbons are filled polygons offset from one
 * centre curve per bundle, so their width is constant and neighbours never
 * overlap. Hover a file to follow its code through the whole chart.
 * Everything from a sample (file names, md5s) is set with textContent / SVG
 * attributes, never innerHTML. Colours come from the app's theme variables.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const ORDERS = [['tree', 'Tree'], ['chain', 'Chain'], ['star', 'Star']];
    const NODE_W = 110, BAND_W = 12, GAP = 230, PAD = 30, PADDING = 36, BAND_PAD = 6;
    const LANE_PAD = 44, BAND_H = 200, FILE_H = 420, KY_MAX = 1.5;
    const V = { born: 'var(--lin-born)', died: 'var(--lin-died)', shared: 'var(--lin-grey)', fork: 'var(--lin-grey-d)', slate: 'var(--lin-slate)', slateHi: 'var(--lin-slate-hi)' };
    let uid = 0;
    const fmt = n => Number(n).toLocaleString();

    const CSS = `
.lin{--lin-slate:#3d4a5f;--lin-slate-hi:#52617a;--lin-edge:rgba(255,255,255,.16);--lin-grey:#8b95a5;--lin-grey-d:#5d6a7e;--lin-born:#3d9be0;--lin-died:#f08a3c;--lin-card:var(--card-bg,#1e1e1e);--lin-line:var(--border,#3e3d32);--lin-halo:var(--bg,#121212);font:12.5px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-variant-numeric:tabular-nums;color:var(--text,#f8f8f2)}
html.light-theme .lin{--lin-slate:#4a5870;--lin-slate-hi:#66758f;--lin-edge:rgba(0,0,0,.2);--lin-grey:#8892a3;--lin-grey-d:#5f6b7f;--lin-born:#0072B2;--lin-died:#D55E00}
.lin-bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:4px 0 8px}
.lin-seg{display:inline-flex;border:1px solid var(--lin-line);border-radius:8px;overflow:hidden;background:var(--lin-card)}
.lin-seg button{background:transparent;border:0;border-left:1px solid var(--lin-line);color:inherit;padding:5px 9px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;font:inherit}
.lin-seg button:first-child{border-left:0}
.lin-seg button:hover{background:var(--hover,#2d2d2d)}
.lin-chip{display:inline-flex;align-items:center;gap:6px;padding:2px 10px;border:1px solid var(--lin-line);border-radius:999px;background:var(--lin-card);font-size:12px}
.lin-sw{width:12px;height:8px;border-radius:2px;display:inline-block}
.lin-main{display:flex;gap:12px;align-items:flex-start}
.lin-scroll{overflow:auto;flex:1;max-height:75vh;min-width:0;border:1px solid var(--lin-line);border-radius:12px;background:var(--lin-card)}
.lin-side{width:250px;flex:none;border:1px solid var(--lin-line);border-radius:12px;background:var(--lin-card);padding:12px 14px}
.lin-side h4{margin:0 0 8px;font-size:13px;word-break:break-all}
.lin-kv{display:grid;grid-template-columns:1fr auto;gap:2px 14px}
.lin-kv .k{opacity:.7}
.lin-note{opacity:.72;font-size:12px}
.lin-tip{position:fixed;pointer-events:none;display:none;z-index:50;min-width:200px;max-width:340px;padding:9px 11px;border-radius:10px;background:var(--lin-card);color:var(--text,#f8f8f2);border:1px solid var(--lin-line);box-shadow:0 8px 24px rgba(0,0,0,.35)}
.lin-tip:before{content:"";position:absolute;left:-5px;top:12px;width:8px;height:8px;background:var(--lin-card);border-left:1px solid var(--lin-line);border-bottom:1px solid var(--lin-line);transform:rotate(45deg)}
.lin-tip .t{display:flex;align-items:center;gap:7px;font-weight:600;margin-bottom:5px}
.lin-dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex:none}
.lin svg text{font-family:inherit}
.lin-halo{paint-order:stroke;stroke:var(--lin-halo);stroke-width:3.5px;stroke-linejoin:round}
.lin button:focus-visible,.lin select:focus-visible{outline:2px solid var(--accent,#66d9ef);outline-offset:2px}
`;

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
    function icon(d) {
        const s = svg('svg', { width: 14, height: 14, viewBox: '0 0 16 16', fill: 'none', stroke: 'currentColor', 'stroke-width': 1.6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' });
        s.append(svg('path', { d }));
        return s;
    }

    // File depth from the root; a file's layer is 2 * depth, the gaps between
    // hold the BORN sources and DIED sinks of the edges that cross that gap.
    function depths(doc) {
        const d = new Map([[doc.root, 0]]);
        for (const e of doc.edges) d.set(e.child, d.get(e.parent) + 1);
        return d;
    }

    // Every file is drawn at exactly its function count. A parent is cut into
    // segments, one per partition of its kept code (the exact set of children
    // that keep it) plus a "kept by no child" segment, all on one px-per-function
    // scale. A segment held by several children forks: one ribbon per child, each
    // starting on the same segment (an overlap ribbon is drawn once per child).
    function layout(doc, width) {
        const dep = depths(doc);
        const maxd = Math.max(...dep.values());
        const files = doc.nodes.map((n, idx) => ({ ...n, kind: 'file', idx, d: dep.get(n.id) }));
        const fileOf = new Map(files.map(n => [n.id, n]));
        const edgeOf = new Map(doc.edges.map(e => [`${e.parent}|${e.child}`, e]));
        const parentOf = new Map(doc.edges.map(e => [e.child, e.parent]));
        const partsOf = new Map();
        for (const p of doc.partitions || []) (partsOf.get(p.parent) || partsOf.set(p.parent, []).get(p.parent)).push(p);
        const fn = n => Math.max(1, n.functions);

        const colSum = new Map(), colCnt = new Map(), bornSum = new Map(), diedSum = new Map();
        for (const n of files) {
            colSum.set(n.d, (colSum.get(n.d) || 0) + fn(n));
            colCnt.set(n.d, (colCnt.get(n.d) || 0) + 1);
            if (n.died_all > 0) diedSum.set(n.d, (diedSum.get(n.d) || 0) + n.died_all);
        }
        for (const e of doc.edges) if (e.gained > 0) { const d = dep.get(e.child); bornSum.set(d, (bornSum.get(d) || 0) + e.gained); }
        let ky = KY_MAX;
        for (const [d, sum] of colSum) ky = Math.min(ky, (FILE_H - PADDING * (colCnt.get(d) - 1)) / sum);
        ky = Math.min(ky, BAND_H / Math.max(1, ...bornSum.values()), BAND_H / Math.max(1, ...diedSum.values()));
        ky = Math.max(ky, 0.02);

        const colW = NODE_W + GAP;
        const W = Math.max(width || 600, 2 * PAD + (maxd + 1) * colW);
        const fileTop = LANE_PAD + BAND_H + 70;
        let filesBottom = fileTop;
        for (let d = 0; d <= maxd; d++) {
            const col = files.filter(n => n.d === d)
                .sort((a, b) => (fileOf.get(parentOf.get(a.id))?.y0 ?? 0) - (fileOf.get(parentOf.get(b.id))?.y0 ?? 0) || a.idx - b.idx);
            let bottom = fileTop - PADDING;
            for (const n of col) {
                n.x0 = PAD + d * colW; n.x1 = n.x0 + NODE_W;
                n.y0 = Math.max(bottom + PADDING, fileOf.get(parentOf.get(n.id))?.y0 ?? fileTop);
                n.y1 = n.y0 + Math.max(2, n.functions * ky);
                bottom = n.y1;
                filesBottom = Math.max(filesBottom, n.y1);
            }
        }
        const diedTop = filesBottom + 70;
        const H = diedTop + BAND_H + LANE_PAD;

        // parent segments, top to bottom: ordered by their children's positions so forks cross little
        const segs = [];
        for (const n of files) {
            const ps = partsOf.get(n.id);
            if (!ps) continue;
            const pos = c => fileOf.get(c).idx;
            const key = p => p.children.map(pos);
            const cmp = (a, b) => {
                const x = key(a), y = key(b);
                for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i];
                return x.length - y.length;
            };
            let y = n.y0;
            const mine = [...ps].sort((a, b) => (a.folded ? 1 : 0) - (b.folded ? 1 : 0) || cmp(a, b));
            for (const p of mine) { segs.push({ n, part: p, y0: y, y1: y + p.count * ky }); y += p.count * ky; }
            if (n.died_all > 0) segs.push({ n, died: true, count: n.died_all, y0: y, y1: y + n.died_all * ky });
        }

        const nodes = [...files];
        const stackBand = (list, base, x0) => {
            const used = new Map();
            for (const b of list) {
                const u = used.get(b.d) || 0;
                b.y0 = base + u; b.y1 = b.y0 + Math.max(2, b.value * ky);
                used.set(b.d, u + (b.y1 - b.y0) + BAND_PAD);
                b.x0 = x0(b); b.x1 = b.x0 + BAND_W;
            }
            nodes.push(...list);
        };
        const borns = doc.edges.filter(e => e.gained > 0).map(e => ({ kind: 'born', edge: e, value: e.gained, d: dep.get(e.child), child: fileOf.get(e.child) }))
            .sort((a, b) => a.child.y0 - b.child.y0);
        stackBand(borns, LANE_PAD, b => PAD + (b.d - 1) * colW + NODE_W + 30);
        const dieds = files.filter(n => n.died_all > 0).map(n => ({ kind: 'died', parent: n, value: n.died_all, d: n.d }))
            .sort((a, b) => a.parent.y0 - b.parent.y0);
        stackBand(dieds, diedTop, b => PAD + b.d * colW + NODE_W + GAP - 30 - BAND_W);

        const links = [];
        const split = (mkSrc, tgt, total, sus, fields, srcY, tgtY) => {
            const s = Math.min(sus, total), cert = total - s;
            let a = srcY, b = tgtY, before = 0;
            for (const [v, hatch] of [[cert, false], [s, true]]) {
                if (v <= 0) continue;
                const w = Math.max(1, v * ky);
                links.push({ ...fields, hatch, value: v, before, width: w, source: mkSrc, target: tgt, y0: a + w / 2, y1: b + w / 2 });
                a += v * ky; b += v * ky; before += v;
            }
        };
        for (const b of borns)
            split(b, b.child, b.value, b.edge.recall_suspect, { kind: 'born', edge: b.edge, bf: b.edge.born_by_file }, b.y0, b.child.y0);
        const cursor = new Map(files.map(n => [n.id, n.y0 + (doc.edges.find(e => e.child === n.id)?.gained || 0) * ky]));
        for (const sg of segs) {
            if (sg.died) continue;
            for (const c of sg.part.children) {
                const v = sg.part.per_child ? sg.part.per_child[c] : sg.part.count;
                const w = Math.max(1, v * ky), t = fileOf.get(c);
                links.push({
                    kind: 'shared', edge: edgeOf.get(`${sg.n.id}|${c}`), part: sg.part, value: v, before: 0, width: w, bf: sg.part.by_file,
                    source: sg.n, target: t, y0: sg.y0 + w / 2, y1: cursor.get(c) + w / 2, overlap: sg.part.children.length > 1,
                });
                cursor.set(c, cursor.get(c) + v * ky);
            }
        }
        for (const dn of dieds) {
            const sg = segs.find(x => x.died && x.n === dn.parent);
            const kidEdges = doc.edges.filter(e => e.parent === dn.parent.id);
            const sus = kidEdges.length ? Math.min(...kidEdges.map(e => e.recall_suspect)) : 0;
            split(dn.parent, dn, dn.value, sus, { kind: 'died', pnode: dn.parent, bf: dn.parent.died_by_file }, sg.y0, dn.y0);
        }
        return { nodes, files, links, segs, W, H, ky, filesBottom, diedTop, fileTop, edgeOf };
    }

    // ---- ribbon geometry ----
    // One centre cubic per bundle (horizontal tangents at both nodes), sampled with
    // unit normals. A piece is the strip between normal offsets o and o + w, so its
    // perpendicular width is w along the whole curve and neighbours never overlap.
    function sampleCurve(x0, x1, yc0, yc1) {
        const n = Math.max(36, Math.min(96, Math.round((x1 - x0 + Math.abs(yc1 - yc0)) / 12)));
        const m = (x0 + x1) / 2, h = x1 - m, dy = yc1 - yc0;
        const pts = [];
        let kmax = 0;
        for (let i = 0; i <= n; i++) {
            const t = i / n, u = 1 - t;
            const x = u * u * u * x0 + 3 * u * u * t * m + 3 * u * t * t * m + t * t * t * x1;
            const y = u * u * u * yc0 + 3 * u * u * t * yc0 + 3 * u * t * t * yc1 + t * t * t * yc1;
            const vx = 3 * (u * u * (m - x0) + t * t * (x1 - m)), vy = 6 * u * t * dy;
            const ax = 6 * h * (t - u), ay = 6 * dy * (u - t);
            const len = Math.hypot(vx, vy) || 1;
            kmax = Math.max(kmax, Math.abs(vx * ay - vy * ax) / (len * len * len));
            pts.push([x, y, -vy / len, vx / len]);
        }
        return { pts, kmax };
    }
    function strip(pts, o, w) {
        const a = pts.map(([x, y, nx, ny]) => `${(x + nx * o).toFixed(1)},${(y + ny * o).toFixed(1)}`);
        const b = pts.map(([x, y, nx, ny]) => `${(x + nx * (o + w)).toFixed(1)},${(y + ny * (o + w)).toFixed(1)}`).reverse();
        return `M${a.join('L')}L${b.join('L')}Z`;
    }
    // previous shape: constant vertical thickness; used where a bundle cannot be offset
    function band(x0, x1, y0, y1, w) {
        const m = (x0 + x1) / 2, a = y0 - w / 2, b = y1 - w / 2;
        return `M${x0},${a}C${m},${a} ${m},${b} ${x1},${b}L${x1},${b + w}C${m},${b + w} ${m},${a + w} ${x0},${a + w}Z`;
    }
    // Group links by (source, target); a bundle is offset-able when its pieces touch at both ends.
    function bundle(links) {
        const groups = new Map();
        for (const l of links) {
            const k = groups.get(l.source) || groups.set(l.source, new Map()).get(l.source);
            (k.get(l.target) || k.set(l.target, []).get(l.target)).push(l);
        }
        for (const byTarget of groups.values()) for (const ls of byTarget.values()) {
            ls.sort((a, b) => a.y0 - b.y0);
            const W = ls.reduce((a, l) => a + l.width, 0);
            let ok = true;
            for (let i = 1; i < ls.length; i++) {
                const p = ls[i - 1], c = ls[i];
                if (Math.abs((c.y0 - c.width / 2) - (p.y0 + p.width / 2)) > 1.5 || Math.abs((c.y1 - c.width / 2) - (p.y1 + p.width / 2)) > 1.5) ok = false;
            }
            const x0 = ls[0].source.x1, x1 = ls[0].target.x0;
            const yc0 = ls[0].y0 - ls[0].width / 2 + W / 2, yc1 = ls[0].y1 - ls[0].width / 2 + W / 2;
            const curve = ok ? sampleCurve(x0, x1, yc0, yc1) : null;
            ok = ok && curve.kmax * (W / 2) < 0.85;
            let cum = 0;
            for (const l of ls) {
                l.pts = ok ? curve.pts : null;
                l.o = cum - W / 2;
                l.d = ok ? strip(curve.pts, l.o, l.width) : band(x0, x1, l.y0, l.y1, l.width);
                cum += l.width;
            }
        }
    }

    function draw(doc, onPick, width) {
        uid++;
        const L = layout(doc, width);
        const { links, segs, W, H, ky } = L;
        bundle(links);
        const nFiles = doc.nodes.length;
        const name = new Map(L.files.map(n => [n.id, n.file_name || n.id]));
        const names = ids => ids.map(i => name.get(i)).join(', ');
        const s = d3.create('svg').attr('width', W).attr('height', H).attr('viewBox', `0 0 ${W} ${H}`)
            .attr('role', 'img').attr('aria-label', 'Lineage of the files: shared, born and died code').style('opacity', 0);
        s.transition().duration(250).ease(d3.easeCubicOut).style('opacity', 1);
        const defs = s.append('defs');
        for (const kind of ['born', 'died']) {
            const p = defs.append('pattern').attr('id', `hatch-${kind}-${uid}`).attr('width', 6).attr('height', 6)
                .attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            p.append('rect').attr('width', 6).attr('height', 6).style('fill', V[kind]).attr('fill-opacity', 0.22);
            p.append('rect').attr('width', 3).attr('height', 6).style('fill', V[kind]);
        }
        const stripe = defs.append('pattern').attr('id', `ovl-${uid}`).attr('width', 6).attr('height', 6)
            .attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
        stripe.append('rect').attr('width', 6).attr('height', 6).style('fill', V.slateHi);
        stripe.append('rect').attr('width', 2).attr('height', 6).attr('fill', '#fff').attr('fill-opacity', 0.28);
        const sh = defs.append('filter').attr('id', `shadow-${uid}`).attr('x', '-10%').attr('y', '-10%').attr('width', '120%').attr('height', '130%');
        sh.append('feDropShadow').attr('dx', 0).attr('dy', 1.5).attr('stdDeviation', 2.5).attr('flood-color', '#000').attr('flood-opacity', 0.3);
        for (const kind of ['born', 'died']) {
            const g = defs.append('linearGradient').attr('id', `lane-${kind}-${uid}`).attr('x1', 0).attr('x2', 0)
                .attr('y1', kind === 'born' ? 0 : 1).attr('y2', kind === 'born' ? 1 : 0);
            g.append('stop').attr('offset', 0).style('stop-color', V[kind]).attr('stop-opacity', 0.2);
            g.append('stop').attr('offset', 1).style('stop-color', V[kind]).attr('stop-opacity', 0.03);
        }
        const lane = (y, h, kind, label, total) => {
            s.append('rect').attr('x', 0).attr('y', y).attr('width', W).attr('height', h).attr('fill', `url(#lane-${kind}-${uid})`);
            const txt = `${label} ${fmt(total)} fn`, pw = 7.2 * txt.length + 26, py = kind === 'born' ? y + 10 : y + h - 32;
            s.append('rect').attr('x', 12).attr('y', py).attr('width', pw).attr('height', 22).attr('rx', 11)
                .style('fill', 'var(--lin-card)').style('stroke', V[kind]).attr('stroke-opacity', 0.6);
            s.append('circle').attr('cx', 24).attr('cy', py + 11).attr('r', 4).style('fill', V[kind]);
            s.append('text').attr('x', 34).attr('y', py + 15).attr('font-size', 12).attr('font-weight', 600).style('fill', 'var(--text)').text(txt);
        };
        const bornTotal = doc.edges.reduce((a, e) => a + e.gained, 0);
        const diedTotal = L.files.reduce((a, n) => a + (n.died_all || 0), 0);
        lane(0, LANE_PAD + BAND_H + 30, 'born', 'Born', bornTotal);
        lane(L.diedTop - 30, H - L.diedTop + 30, 'died', 'Died (kept by no child)', diedTotal);
        s.append('text').attr('x', 14).attr('y', L.fileTop - 34).attr('font-size', 11.5).attr('fill-opacity', 0.7).style('fill', 'var(--text)').text('Shared: kept from parent to child');

        const tip = d3.select(document.body).append('div').attr('class', 'lin lin-tip');
        s.node()._cleanup = () => tip.remove();
        const showTip = (ev, title, dot, rows) => {
            tip.selectAll('*').remove();
            const t = tip.append('div').attr('class', 't');
            t.append('span').attr('class', 'lin-dot').style('background', dot);
            t.append('span').text(title);
            const kv = tip.append('div').attr('class', 'lin-kv');
            for (const [k, v] of rows) { kv.append('span').attr('class', 'k').text(k); kv.append('span').text(v); }
            tip.style('display', 'block').style('left', `${ev.clientX + 14}px`).style('top', `${ev.clientY + 10}px`);
        };
        const moveTip = ev => tip.style('left', `${ev.clientX + 14}px`).style('top', `${ev.clientY + 10}px`);
        const hideTip = () => tip.style('display', 'none');

        links.filter(l => l.kind === 'shared').concat(links.filter(l => l.kind !== 'shared')).forEach((l, i) => {
            const col = l.kind === 'shared' ? (l.overlap ? V.fork : V.shared) : V[l.kind];
            const g = defs.append('linearGradient').attr('id', `g-${uid}-${i}`).attr('gradientUnits', 'userSpaceOnUse')
                .attr('x1', l.source.x1).attr('x2', l.target.x0);
            g.append('stop').attr('offset', 0).style('stop-color', col).attr('stop-opacity', l.kind === 'shared' ? 0.8 : 0.95);
            g.append('stop').attr('offset', 1).style('stop-color', col).attr('stop-opacity', l.kind === 'shared' ? 0.45 : 0.75);
            l.fill = l.hatch ? `url(#hatch-${l.kind}-${uid})` : `url(#g-${uid}-${i})`;
            l.col = col;
        });
        const ribbons = s.append('g').selectAll('path').data(links).join('path').attr('class', 'lin-rib')
            .attr('d', l => l.d).style('fill', l => l.fill).attr('fill-opacity', 0.9).attr('stroke', 'none').attr('stroke-linejoin', 'round').style('cursor', 'pointer');
        const ov = s.append('g').style('pointer-events', 'none');

        // ---- nodes: rounded, shadowed, segments clipped to the box ----
        const nodeG = s.append('g');
        const clipId = n => `clip-${uid}-${n.idx}`;
        for (const n of L.files) defs.append('clipPath').attr('id', clipId(n)).append('rect').attr('x', n.x0).attr('y', n.y0).attr('width', NODE_W).attr('height', Math.max(2, n.y1 - n.y0)).attr('rx', 6);
        nodeG.selectAll('rect.base').data(L.files).join('rect').attr('class', 'base')
            .attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => Math.max(2, n.y1 - n.y0)).attr('rx', 6)
            .style('fill', V.slate).attr('filter', `url(#shadow-${uid})`);
        const clipped = nodeG.selectAll('g.clip').data(L.files).join('g').attr('class', 'clip').attr('clip-path', n => `url(#${clipId(n)})`);
        const segRect = clipped.selectAll('rect.seg').data(n => segs.filter(g => g.n === n)).join('rect').attr('class', 'seg')
            .attr('x', d => d.n.x0).attr('y', d => d.y0).attr('width', NODE_W).attr('height', d => Math.max(1, d.y1 - d.y0))
            .style('fill', d => d.died ? V.died : d.part.children.length > 1 ? V.slateHi : V.slate)
            .style('stroke', 'var(--lin-edge)').attr('stroke-width', 0.75).style('cursor', 'pointer');
        clipped.selectAll('rect.stripe').data(n => segs.filter(g => g.n === n && !g.died && g.part.children.length > 1)).join('rect').attr('class', 'stripe')
            .attr('x', d => d.n.x0).attr('y', d => d.y0).attr('width', NODE_W).attr('height', d => Math.max(1, d.y1 - d.y0))
            .attr('fill', `url(#ovl-${uid})`).style('pointer-events', 'none');
        const isCore = d => !d.died && !doc.by_file_skipped && d.part.by_file && Object.keys(d.part.by_file).length === nFiles
            && Object.values(d.part.by_file).every(v => v === d.part.count);
        clipped.selectAll('rect.core').data(n => segs.filter(g => g.n === n && isCore(g))).join('rect').attr('class', 'core')
            .attr('x', d => d.n.x0 + 3).attr('y', d => d.y0 + 1.5).attr('width', NODE_W - 6).attr('height', d => Math.max(1, d.y1 - d.y0 - 3))
            .attr('fill', 'none').attr('stroke', '#fff').attr('stroke-opacity', 0.7).attr('stroke-dasharray', '3 2').style('pointer-events', 'none');
        const rim = nodeG.selectAll('rect.rim').data(L.files).join('rect').attr('class', 'rim')
            .attr('x', n => n.x0 + 0.5).attr('y', n => n.y0 + 0.5).attr('width', NODE_W - 1).attr('height', n => Math.max(1, n.y1 - n.y0 - 1)).attr('rx', 6)
            .attr('fill', 'none').style('stroke', 'var(--lin-edge)').style('pointer-events', 'none');
        const leafBox = clipped.filter(n => !segs.some(g => g.n === n));
        nodeG.selectAll('rect.band').data(L.nodes.filter(n => n.kind !== 'file')).join('rect').attr('class', 'band')
            .attr('x', n => n.x0).attr('y', n => n.y0).attr('width', BAND_W).attr('height', n => Math.max(2, n.y1 - n.y0)).attr('rx', 3)
            .style('fill', n => n.kind === 'born' ? V.born : V.died);
        // labels sit above the box with a halo, never on a stripe
        const fileHit = nodeG.selectAll('g.file').data(L.files).join('g').attr('class', 'file').style('cursor', 'pointer')
            .attr('tabindex', 0).attr('role', 'button').attr('aria-label', n => `${name.get(n.id)}, ${n.functions} functions`);
        fileHit.append('rect').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', 24).attr('height', n => Math.max(2, n.y1 - n.y0)).attr('fill', 'transparent');
        const lab = fileHit.append('text').attr('class', 'lin-halo').attr('x', n => n.x0).attr('y', n => n.y0 - 9).attr('font-size', 12.5).attr('font-weight', 600)
            .style('fill', 'var(--text)').style('pointer-events', 'none');
        lab.append('tspan').text(n => { const f = name.get(n.id); return f.length > 18 ? `${f.slice(0, 17)}…` : f; });
        lab.append('tspan').attr('dx', 6).attr('font-weight', 400).attr('font-size', 11.5).attr('fill-opacity', 0.7).style('fill', 'var(--text)').text(n => `${fmt(n.functions)} fn`);
        fileHit.append('title').text(n => `${name.get(n.id)} (${fmt(n.functions)} functions)`);

        // ---- interaction ----
        const kindDot = l => l.kind === 'shared' ? V.shared : V[l.kind];
        const partRows = (p) => [['Kept by', p.folded ? 'several children (folded)' : names(p.children)], ['Functions', fmt(p.count)]];
        const edgeRows = e => [['Born', fmt(e.gained)], ['Died', fmt(e.lost)], ['Shared', fmt(e.shared)], ['Recall suspect', fmt(e.recall_suspect)]];
        let sel = null;
        const applySel = () => {
            ribbons.style('stroke', d => sel && sel.edge && d.edge === sel.edge ? 'var(--accent)' : 'none').attr('stroke-width', 1.2);
            rim.style('stroke', n => sel && sel.node === n ? 'var(--accent)' : 'var(--lin-edge)').attr('stroke-width', n => sel && sel.node === n ? 2.5 : 1);
        };
        const pick = (p) => { sel = p; applySel(); onPick(p); };
        const dim = pred => {
            ribbons.transition().duration(180).ease(d3.easeCubicOut).style('opacity', d => pred(d) ? 1 : 0.1);
            ribbons.filter(pred).raise();
        };
        const undim = () => {
            ribbons.transition().duration(180).ease(d3.easeCubicOut).style('opacity', 1);
            segRect.transition().duration(180).style('opacity', 1);
            ov.selectAll('*').interrupt().remove();
        };
        ribbons.on('mouseenter', (ev, l) => {
            dim(d => l.kind === 'died' ? d.kind === 'died' && d.pnode === l.pnode : d.edge === l.edge);
            if (l.kind === 'died') showTip(ev, `${name.get(l.pnode.id)}: died`, V.died, [['Kept by no child', fmt(l.pnode.died_all)]]);
            else showTip(ev, `${name.get(l.edge.parent)} → ${name.get(l.edge.child)}`, kindDot(l), [...edgeRows(l.edge), ...(l.part ? partRows(l.part) : [])]);
        }).on('mousemove', moveTip).on('mouseleave', () => { undim(); hideTip(); })
            .on('click', (ev, l) => pick(l.kind === 'died' ? { node: l.pnode } : { edge: l.edge, part: l.part }));
        segRect.on('mouseenter', (ev, d) => {
            dim(l => d.died ? l.kind === 'died' && l.pnode === d.n : l.part === d.part);
            if (d.died) showTip(ev, `${name.get(d.n.id)}: died`, V.died, [['Kept by no child', fmt(d.count)]]);
            else showTip(ev, `${name.get(d.n.id)}: kept by`, d.part.children.length > 1 ? V.fork : V.shared, partRows(d.part));
        }).on('mousemove', moveTip).on('mouseleave', () => { undim(); hideTip(); })
            .on('click', (ev, d) => pick(d.died ? { node: d.n } : { part: d.part, parent: d.n.id }));

        // file hover: that file's functions through the whole chart, same scale, anchored at each ribbon's top edge
        const fileHover = (ev, F) => {
            const rows = [['Functions', fmt(F.functions)]];
            if (doc.by_file_skipped) {
                dim(l => (l.edge && (l.edge.parent === F.id || l.edge.child === F.id)) || l.pnode === F);
                rows.push(['Note', 'per-file highlight is off (more than 60 files)']);
            } else {
                ribbons.transition().duration(180).style('opacity', 0.1);
                segRect.transition().duration(180).style('opacity', 0.3);
                for (const l of links) {
                    const n = Math.max(0, Math.min(l.value, ((l.bf || {})[F.id] || 0) - l.before));
                    if (!n) continue;
                    const w = Math.max(1, n * ky);
                    ov.append('path').attr('d', l.pts ? strip(l.pts, l.o, w) : band(l.source.x1, l.target.x0, l.y0 - l.width / 2 + w / 2, l.y1 - l.width / 2 + w / 2, w))
                        .style('fill', l.col).attr('fill-opacity', 0).transition().duration(160).attr('fill-opacity', 1);
                }
                for (const sg of segs) {
                    const n = sg.died ? (F.id === sg.n.id ? sg.count : (sg.n.died_by_file || {})[F.id] || 0) : (sg.part.by_file || {})[F.id] || 0;
                    if (!n) continue;
                    ov.append('path').attr('clip-path', `url(#${clipId(sg.n)})`)
                        .attr('d', `M${sg.n.x0},${sg.y0}h${NODE_W}v${Math.max(1, Math.min(n, sg.died ? sg.count : sg.part.count) * ky)}h${-NODE_W}Z`)
                        .style('fill', sg.died ? V.died : sg.part.children.length > 1 ? V.slateHi : V.slate).style('stroke', 'var(--lin-edge)').attr('stroke-width', 0.75);
                }
                rows.push(['In every file (core)', fmt(doc.core)], ['Only in this file', fmt(F.only_here)]);
                for (const [g, c] of F.top_shared || []) rows.push([`Shared with ${name.get(g)}`, fmt(c)]);
            }
            showTip(ev, name.get(F.id), V.slate, rows);
        };
        fileHit.on('mouseenter', fileHover).on('mousemove', moveTip).on('mouseleave', () => { undim(); hideTip(); })
            .on('click', (ev, n) => pick({ node: n }))
            .on('keydown', (ev, n) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); pick({ node: n }); } });
        void leafBox;
        return { svg: s.node(), name };
    }

    function chip(color, text, hatch) {
        const c = el('span', { class: 'lin-chip' });
        const sw = el('span', { class: 'lin-sw' });
        sw.style.background = hatch ? `repeating-linear-gradient(45deg,${color} 0 2px,transparent 2px 4px)` : color;
        c.append(sw, el('span', {}, text));
        return c;
    }
    function legend(doc) {
        const box = el('div', { style: 'display:flex;flex-wrap:wrap;gap:8px;align-items:center' });
        box.append(chip(V.shared, 'shared'), chip(V.fork, 'kept by several children (forks)'), chip(V.born, 'born'), chip(V.died, 'died: kept by no child'),
            chip(V.born, 'hatched: likely a matcher miss', true));
        if (doc.by_file_skipped) box.append(el('span', { class: 'lin-note' }, 'More than 60 files: per-file highlight is off.'));
        return box;
    }

    function detail(box, pick, names) {
        box.replaceChildren();
        const nm = id => names.get(id) || id;
        const kv = rows => {
            const g = el('div', { class: 'lin-kv' });
            for (const [k, v] of rows) { g.append(el('span', { class: 'k' }, k), el('span', {}, String(v))); }
            box.append(g);
        };
        if (pick.part) {
            const p = pick.part;
            box.append(el('h4', {}, `${nm(p.parent)}: kept by ${p.folded ? 'several children (folded)' : p.children.map(nm).join(' + ')}`));
            kv([['Functions', fmt(p.count)], ...Object.entries(p.per_child || {}).map(([c, n]) => [nm(c), fmt(n)])]);
        }
        if (pick.node) {
            const n = pick.node;
            box.append(el('h4', {}, n.file_name || n.id));
            kv([['Functions', fmt(n.functions) + (n.root ? ' (root)' : '')], ...(n.died_all != null ? [['Kept by no child', fmt(n.died_all)]] : []), ['Only in this file', fmt(n.only_here)]]);
            box.append(el('div', { class: 'lin-note', style: 'margin-top:8px;word-break:break-all' }, n.id));
        } else if (pick.edge) {
            const e = pick.edge, ev = e.recall_evidence || {};
            if (pick.part) box.append(el('div', { style: 'height:10px' }));
            box.append(el('h4', {}, `${nm(e.parent)} → ${nm(e.child)}`));
            kv([['Similarity', e.similarity], ['Shared', fmt(e.shared)], ['Born', fmt(e.gained)], ['Lost', fmt(e.lost)], ['Recall suspect', fmt(e.recall_suspect)],
                ['  below the floor', fmt(ev.edge_below_floor)], ['  refused by one-per-file', fmt(ev.edge_refused)], ['  same name', fmt(ev.name)]]);
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
        body.replaceChildren(el('div', { class: 'lin-note' }, 'Loading lineage...'));
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
        const side = el('div', { class: 'lin-side' });
        side.append(el('div', { class: 'lin-note' }, 'Click a file, a ribbon or a segment.'));
        const { svg: tree, name } = draw(doc, pick => detail(side, pick, name), body.clientWidth - 290);
        st.cleanup = tree._cleanup;
        const scroller = el('div', { class: 'lin-scroll' });
        scroller.append(tree);
        const w0 = Number(tree.getAttribute('width')), h0 = Number(tree.getAttribute('height'));
        const zoom = z => {
            st.zoom = Math.max(0.1, Math.min(4, z));
            tree.setAttribute('width', w0 * st.zoom);
            tree.setAttribute('height', h0 * st.zoom);
        };
        const fit = () => zoom(Math.min(1, (scroller.clientWidth || w0) / w0));
        const seg = el('div', { class: 'lin-seg', role: 'group', 'aria-label': 'Zoom' });
        const btns = [
            ['Zoom out', 'M3 8h10', () => zoom((st.zoom || 1) / 1.25)],
            ['Zoom in', 'M3 8h10M8 3v10', () => zoom((st.zoom || 1) * 1.25)],
            ['Fit to width', 'M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4', fit],
            ['Actual size (100%)', 'M4 3v10M4 3L2.5 4.5M9 3h5v10H9Z', () => zoom(1)],
        ];
        for (const [label, d, f] of btns) {
            const b = el('button', { type: 'button', 'aria-label': label, title: label });
            b.append(icon(d));
            b.addEventListener('click', f);
            seg.append(b);
        }
        scroller.addEventListener('wheel', ev => {
            if (!ev.ctrlKey && !ev.metaKey) return;
            ev.preventDefault();
            zoom((st.zoom || 1) * (ev.deltaY < 0 ? 1.1 : 1 / 1.1));
        }, { passive: false });
        const bar = el('div', { class: 'lin-bar' });
        bar.append(seg, legend(doc));
        const main = el('div', { class: 'lin-main' });
        main.append(scroller, side);
        zoom(st.zoom || 1);
        body.append(bar, main);
    }

    function mount(panel) {
        if (!document.getElementById('nway-lineage-css')) {
            const css = el('style', { id: 'nway-lineage-css' });
            css.textContent = CSS;
            document.head.append(css);
        }
        const st = panel._lineage || (panel._lineage = { on: false, order: 'tree', root: '' });
        const host = panel.el.querySelector('.nway-panel');
        if (!host) return;
        const bar = el('div', { class: 'nway-note', style: 'display:flex;gap:8px;align-items:center' });
        const toggle = el('button', { class: `view-btn${st.on ? ' active' : ''}`, 'aria-pressed': String(!!st.on) }, 'Lineage');
        bar.append(toggle);
        const body = el('div', { class: 'nway-card lin' });
        body.style.display = st.on ? '' : 'none';
        const card = host.querySelector('.nway-card');
        const draw_ = () => render(panel, body);
        if (st.on) {
            const order = el('select', { class: 'view-btn', 'aria-label': 'Lineage order' });
            for (const [v, t] of ORDERS) {
                const o = el('option', { value: v }, t);
                if (v === st.order) o.selected = true;
                order.append(o);
            }
            order.addEventListener('change', () => { st.order = order.value; draw_(); });
            const root = el('select', { class: 'view-btn', 'aria-label': 'Lineage root file' });
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
