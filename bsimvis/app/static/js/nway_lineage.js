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
    const SLATE = '#475569', SLATE_LIGHT = '#64748b', GREY_DARK = '#5b6472';
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

    function draw(doc, onPick, width) {
        uid++;
        const L = layout(doc, width);
        const { links, segs, W, H, ky } = L;
        const nFiles = doc.nodes.length;
        const name = new Map(L.files.map(n => [n.id, n.file_name || n.id]));
        const names = ids => ids.map(i => name.get(i)).join(', ');
        const s = d3.create('svg').attr('width', W).attr('height', H).attr('viewBox', `0 0 ${W} ${H}`)
            .attr('role', 'img').attr('aria-label', 'Lineage of the files: shared, born and died code');
        const defs = s.append('defs');
        for (const [kind, col] of [['born', BLUE], ['died', ORANGE]]) {
            const p = defs.append('pattern').attr('id', `hatch-${kind}-${uid}`).attr('width', 6).attr('height', 6)
                .attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            p.append('rect').attr('width', 6).attr('height', 6).attr('fill', col).attr('fill-opacity', 0.25);
            p.append('rect').attr('width', 3).attr('height', 6).attr('fill', col);
        }
        const stripe = defs.append('pattern').attr('id', `ovl-${uid}`).attr('width', 6).attr('height', 6)
            .attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
        stripe.append('rect').attr('width', 6).attr('height', 6).attr('fill', SLATE_LIGHT);
        stripe.append('rect').attr('width', 2).attr('height', 6).attr('fill', '#cbd5e1');
        const bandTotal = k => doc.edges.reduce((a, e) => a + e[k], 0);
        const lane = (y, h, col, label) => {
            s.append('rect').attr('x', 0).attr('y', y).attr('width', W).attr('height', h).attr('fill', col).attr('fill-opacity', 0.08);
            s.append('text').attr('x', 8).attr('y', y + 16).attr('font-size', 12).attr('font-weight', 600).attr('fill', 'currentColor').text(label);
        };
        lane(0, LANE_PAD + BAND_H + 30, BLUE, `BORN: ${bandTotal('gained')} functions`);
        lane(L.diedTop - 30, H - L.diedTop + 30, ORANGE, `DIED (kept by no child): ${L.files.reduce((a, n) => a + (n.died_all || 0), 0)} functions`);
        s.append('text').attr('x', 8).attr('y', L.fileTop - 14).attr('font-size', 12).attr('font-weight', 600).attr('fill', 'currentColor')
            .text('SHARED (kept from parent to child)');

        const tip = d3.select(document.body).append('div')
            .style('position', 'fixed').style('pointer-events', 'none').style('display', 'none').style('z-index', 50)
            .style('padding', '6px 8px').style('border-radius', '4px').style('background', '#1f2328').style('color', '#fff')
            .style('font-size', '12px').style('line-height', 1.4).style('white-space', 'pre');
        s.node()._cleanup = () => tip.remove();
        const showTip = (ev, t) => tip.text(t).style('display', 'block').style('left', `${ev.clientX + 12}px`).style('top', `${ev.clientY + 12}px`);
        const hideTip = () => tip.style('display', 'none');

        links.filter(l => l.kind === 'shared').forEach((l, i) => {
            const g = defs.append('linearGradient').attr('id', `sh-${uid}-${i}`).attr('gradientUnits', 'userSpaceOnUse')
                .attr('x1', l.source.x1).attr('x2', l.target.x0);
            const col = l.overlap ? GREY_DARK : GREY;
            g.append('stop').attr('offset', 0).attr('stop-color', col).attr('stop-opacity', 0.7);
            g.append('stop').attr('offset', 1).attr('stop-color', col).attr('stop-opacity', 0.3);
            l.grad = `url(#sh-${uid}-${i})`;
        });
        const stroke = l => l.kind === 'shared' ? l.grad : l.hatch ? `url(#hatch-${l.kind}-${uid})` : l.kind === 'born' ? BLUE : ORANGE;
        const full = l => l.kind === 'shared' ? (l.overlap ? GREY_DARK : GREY) : stroke(l);
        // filled band with a vertical thickness equal to its width at both ends, so
        // ribbons stacked on a node edge sit side by side on the curve instead of overlapping
        const ribbon = (x0, x1, y0, y1, w) => {
            const m = (x0 + x1) / 2, a = y0 - w / 2, b = y1 - w / 2;
            return `M${x0},${a}C${m},${a} ${m},${b} ${x1},${b}L${x1},${b + w}C${m},${b + w} ${m},${a + w} ${x0},${a + w}Z`;
        };
        const ribbons = s.append('g').selectAll('path').data(links).join('path')
            .attr('d', l => ribbon(l.source.x1, l.target.x0, l.y0, l.y1, l.width)).attr('fill', stroke).attr('stroke', 'none')
            .attr('fill-opacity', l => l.kind === 'shared' ? 1 : 0.85).style('cursor', 'pointer');
        const ov = s.append('g').style('pointer-events', 'none');

        // parent segments drawn on the node; leaves are plain boxes
        const nodeG = s.append('g');
        const segRect = nodeG.selectAll('rect.seg').data(segs).join('rect').attr('class', 'seg')
            .attr('x', d => d.n.x0).attr('y', d => d.y0).attr('width', NODE_W).attr('height', d => Math.max(1, d.y1 - d.y0))
            .attr('fill', d => d.died ? ORANGE : d.part.children.length > 1 ? SLATE_LIGHT : SLATE)
            .attr('stroke', '#fff').attr('stroke-width', 0.75).style('cursor', 'pointer');
        // overlap segments carry a stripe; a core segment (in every file) gets a dashed inset outline
        nodeG.selectAll('rect.stripe').data(segs.filter(d => !d.died && d.part.children.length > 1)).join('rect').attr('class', 'stripe')
            .attr('x', d => d.n.x0).attr('y', d => d.y0).attr('width', NODE_W).attr('height', d => Math.max(1, d.y1 - d.y0))
            .attr('fill', `url(#ovl-${uid})`).style('pointer-events', 'none');
        const isCore = d => !d.died && !doc.by_file_skipped && d.part.by_file && Object.keys(d.part.by_file).length === nFiles
            && Object.values(d.part.by_file).every(v => v === d.part.count);
        nodeG.selectAll('rect.core').data(segs.filter(isCore)).join('rect').attr('class', 'core')
            .attr('x', d => d.n.x0 + 2).attr('y', d => d.y0 + 1).attr('width', NODE_W - 4).attr('height', d => Math.max(1, d.y1 - d.y0 - 2))
            .attr('fill', 'none').attr('stroke', '#fff').attr('stroke-dasharray', '3 2').style('pointer-events', 'none');
        const leaf = L.files.filter(n => !segs.some(g => g.n === n));
        const fileBox = nodeG.selectAll('rect.leaf').data(leaf).join('rect').attr('class', 'leaf')
            .attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => Math.max(2, n.y1 - n.y0))
            .attr('fill', SLATE).attr('rx', 2);
        const bandNodes = L.nodes.filter(n => n.kind !== 'file');
        nodeG.selectAll('rect.band').data(bandNodes).join('rect').attr('class', 'band')
            .attr('x', n => n.x0).attr('y', n => n.y0).attr('width', BAND_W).attr('height', n => Math.max(2, n.y1 - n.y0))
            .attr('fill', n => n.kind === 'born' ? BLUE : ORANGE);
        const label = n => { const f = name.get(n.id); return f.length > 16 ? `${f.slice(0, 15)}…` : f; };
        const tall = n => n.y1 - n.y0 >= 32;
        const fileHit = nodeG.selectAll('g.file').data(L.files).join('g').attr('class', 'file').style('cursor', 'pointer');
        fileHit.append('rect').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', 22).attr('height', n => Math.max(2, n.y1 - n.y0)).attr('fill', 'transparent');
        fileHit.append('title').text(n => `${name.get(n.id)} (${n.functions} functions)`);
        fileHit.append('text').attr('x', n => n.x0 + (tall(n) ? 6 : 0)).attr('y', n => tall(n) ? n.y0 + 15 : n.y0 - 5)
            .attr('font-size', 11).attr('fill', n => tall(n) ? '#fff' : 'currentColor').style('pointer-events', 'none')
            .text(n => tall(n) ? label(n) : `${label(n)} (${n.functions} fn)`);
        fileHit.filter(tall).append('text').attr('x', n => n.x0 + 6).attr('y', n => n.y0 + 28).attr('font-size', 10)
            .attr('fill', '#fff').style('pointer-events', 'none').text(n => `${n.functions} fn`);
        nodeG.selectAll('text.seg').data(segs.filter(d => !d.died && d.part.children.length > 1 && d.y1 - d.y0 >= 14 && d.y0 >= d.n.y0 + 32)).join('text')
            .attr('class', 'seg').attr('x', d => d.n.x0 + 6).attr('y', d => d.y0 + 11).attr('font-size', 10).attr('fill', '#fff')
            .style('pointer-events', 'none').text(d => `kept by ${d.part.children.map(c => name.get(c)).join(' + ')}: ${d.part.count}`.slice(0, 24));

        // ---- hover ----
        const edgeText = e => `${name.get(e.parent)} → ${name.get(e.child)}\nborn ${e.gained}   died ${e.lost}   shared ${e.shared}\nrecall suspect ${e.recall_suspect}`;
        const partText = (p, par) => `${name.get(par)}: kept by ${p.folded ? 'several children (folded)' : names(p.children)}: ${p.count}`;
        const dim = pred => {
            ribbons.transition().duration(120).style('opacity', d => pred(d) ? 1 : 0.1);
            ribbons.filter(pred).raise();
        };
        const undim = () => {
            ribbons.transition().duration(120).style('opacity', 1);
            segRect.transition().duration(120).style('opacity', 1);
            fileBox.transition().duration(120).style('opacity', 1);
            ov.selectAll('*').remove();
        };
        ribbons.on('mouseenter', (ev, l) => {
            dim(d => l.kind === 'died' ? d.kind === 'died' && d.pnode === l.pnode : d.edge === l.edge);
            showTip(ev, l.kind === 'died' ? `kept by no child of ${name.get(l.pnode.id)}: ${l.pnode.died_all}` : edgeText(l.edge) + (l.part ? `\n${partText(l.part, l.part.parent)}` : ''));
        }).on('mousemove', ev => tip.style('left', `${ev.clientX + 12}px`).style('top', `${ev.clientY + 12}px`))
            .on('mouseleave', () => { undim(); hideTip(); })
            .on('click', (ev, l) => onPick(l.kind === 'died' ? { node: l.pnode } : { edge: l.edge, part: l.part }));
        segRect.on('mouseenter', (ev, d) => {
            dim(l => d.died ? l.kind === 'died' && l.pnode === d.n : l.part === d.part);
            showTip(ev, d.died ? `${name.get(d.n.id)}: kept by no child: ${d.count}` : partText(d.part, d.n.id));
        }).on('mousemove', ev => tip.style('left', `${ev.clientX + 12}px`).style('top', `${ev.clientY + 12}px`))
            .on('mouseleave', () => { undim(); hideTip(); })
            .on('click', (ev, d) => onPick(d.died ? { node: d.n } : { part: d.part, parent: d.n.id }));

        // file hover: that file's functions through the whole chart, same scale, anchored at each ribbon's top edge
        const fileHover = (ev, F) => {
            const lines = [`${name.get(F.id)}: ${F.functions} functions`];
            if (doc.by_file_skipped) {
                dim(l => l.edge && (l.edge.parent === F.id || l.edge.child === F.id) || (l.pnode === F));
                lines.push('(per-file highlight skipped: more than 60 files)');
            } else {
                ribbons.transition().duration(120).style('opacity', 0.1);
                segRect.transition().duration(120).style('opacity', 0.3);
                for (const l of links) {
                    const n = Math.max(0, Math.min(l.value, ((l.bf || {})[F.id] || 0) - l.before));
                    if (!n) continue;
                    const w = Math.max(1, n * ky), top = l.y0 - l.width / 2;
                    ov.append('path').attr('d', ribbon(l.source.x1, l.target.x0, top + w / 2, l.y1 - l.width / 2 + w / 2, w))
                        .attr('fill', full(l)).attr('fill-opacity', l.kind === 'shared' ? 0.95 : 1);
                }
                for (const sg of segs) {
                    const n = sg.died ? (F.id === sg.n.id ? sg.count : (sg.n.died_by_file || {})[F.id] || 0) : (sg.part.by_file || {})[F.id] || 0;
                    if (!n) continue;
                    ov.append('rect').attr('x', sg.n.x0).attr('y', sg.y0).attr('width', NODE_W).attr('height', Math.max(1, Math.min(n, sg.died ? sg.count : sg.part.count) * ky))
                        .attr('fill', sg.died ? ORANGE : sg.part.children.length > 1 ? SLATE_LIGHT : SLATE).attr('stroke', '#fff').attr('stroke-width', 0.75);
                }
                lines.push(`in every file (core): ${doc.core}`, `only in this file: ${F.only_here}`);
                if ((F.top_shared || []).length) lines.push('most shared with: ' + F.top_shared.map(([g, c]) => `${name.get(g)} ${c}`).join(', '));
            }
            showTip(ev, lines.join('\n'));
        };
        fileHit.on('mouseenter', fileHover).on('mousemove', ev => tip.style('left', `${ev.clientX + 12}px`).style('top', `${ev.clientY + 12}px`))
            .on('mouseleave', () => { undim(); hideTip(); }).on('click', (ev, n) => onPick({ node: n }));
        return { svg: s.node(), byId: new Map(L.files.map(n => [n.id, n])), name };
    }

    function legend(doc) {
        const box = el('div', { style: 'display:flex;flex-wrap:wrap;gap:14px;font-size:0.8rem;padding:6px 0;align-items:center' });
        const item = (fill, text, hatch) => {
            const w = el('span', { style: 'display:inline-flex;align-items:center;gap:5px' });
            const bg = hatch ? `background:repeating-linear-gradient(45deg,${fill} 0 2px,transparent 2px 4px);` : `background:${fill};`;
            w.append(el('span', { style: `display:inline-block;width:16px;height:10px;${bg}border:1px solid ${fill}` }), el('span', {}, text));
            return w;
        };
        box.append(item(GREY, 'shared'), item(GREY_DARK, 'kept by several children (forks)'), item(BLUE, 'born'), item(ORANGE, 'died: kept by no child'),
            item(GREY, 'hatched: likely a matcher miss', true),
            el('span', { class: 'dim' }, 'One scale: every file is exactly its function count, split into the parts its children keep. Dashed outline = in every file. Hover a file to follow its code through the whole chart.'));
        if (doc.by_file_skipped) box.append(el('span', { class: 'nway-note warn' }, 'More than 60 files: per-file highlight is off, hover shows the file’s own edges only.'));
        return box;
    }

    function detail(box, pick, names) {
        box.replaceChildren();
        const line = t => box.append(el('div', {}, t));
        const nm = id => names.get(id) || id;
        if (pick.part) {
            const p = pick.part;
            box.append(el('b', {}, `${nm(p.parent)}: kept by ${p.folded ? 'several children (folded)' : p.children.map(nm).join(' + ')}`));
            line(`${p.count} functions`);
            if (p.per_child) for (const [c, n] of Object.entries(p.per_child)) line(`  ${nm(c)}: ${n}`);
        }
        if (pick.node) {
            const n = pick.node;
            box.append(el('b', {}, n.file_name || n.id));
            line(`${n.functions} functions${n.root ? ' (root)' : ''}`);
            if (n.died_all != null) line(`kept by no child: ${n.died_all}`);
            line(`only in this file: ${n.only_here}`);
            line(n.id);
        } else if (pick.edge) {
            const e = pick.edge;
            if (pick.part) box.append(el('hr'));
            box.append(el('b', {}, `${nm(e.parent)} → ${nm(e.child)}`));
            line(`similarity ${e.similarity}`);
            line(`shared ${e.shared}, born ${e.gained}, lost ${e.lost}`);
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
        const { svg: tree, name } = draw(doc, pick => detail(side, pick, name), body.clientWidth - 260);
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
        body.append(legend(doc), wrap);
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
