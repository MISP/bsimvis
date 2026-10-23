/**
 * Lineage chart of the files of an N-way set (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(container, opts) -> { setDoc(doc), destroy() }.
 * One node per file at a fixed px-per-function scale, columns by tree depth. A
 * node body is cut by which children keep the code (grey), plus "kept by no
 * child" (orange); a strip on its left says what was born here (blue, hatched =
 * likely matcher miss) and a strip on its right marks the reuse classes it takes
 * part in (violet). Ribbons between nodes are the shared code. Reuse arcs join
 * the files of a class that the tree does not connect: they leave the right edge
 * into the gap, run in a lane above the columns and drop into the gap of the
 * other file, so they never cross a box.
 *
 * d3.zoom pans and zooms; detail is semantic (labels, segments, hatching appear
 * with the zoom level and only for what is on screen). Subtrees can be collapsed
 * into one aggregate node. Everything from a sample (file names, md5s) is set
 * with textContent / attributes, never innerHTML.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const NODE_W = 120, ORIG_W = 14, REUSE_W = 8, GAP = 230, PAD = 40, NPAD = 28;
    const KY_MAX = 1.2, KY_MIN = 0.12, TARGET_H = 1600, REUSE_MAX = 14;
    const ARC_MAX_PIECES = 6, AUTO_FILES = 40, AUTO_SHOW = 60, FAN_FADE = 8, K_MIN = 0.05, K_MAX = 40;
    const DETAIL_K = 0.3, LABEL_K = 0.55, HATCH_K = 1.5;
    const V = { born: 'var(--lin-born)', died: 'var(--lin-died)', shared: 'var(--lin-grey)', fork: 'var(--lin-grey-d)', slate: 'var(--lin-slate)', slateHi: 'var(--lin-slate-hi)', reuse: 'var(--lin-reuse)' };
    const CAVEAT = 'reuse, independent birth or matcher miss';
    const fmt = n => Number(n).toLocaleString();
    let uid = 0;

    const CSS = `
.lin-root,.lin-vars{--lin-slate:#3d4a5f;--lin-slate-hi:#52617a;--lin-edge:rgba(255,255,255,.16);--lin-grey:#8b95a5;--lin-grey-d:#5d6a7e;--lin-born:#3d9be0;--lin-died:#f08a3c;--lin-reuse:#8b6fd6;--lin-card:var(--card-bg,#1e1e1e);--lin-line:var(--border,#3e3d32);--lin-halo:var(--bg,#121212);display:flex;flex-direction:column;height:100%;min-height:0;font:12.5px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-variant-numeric:tabular-nums;color:var(--text,#f8f8f2);color-scheme:dark}
html.light-theme .lin-root,html.light-theme .lin-vars{--lin-slate:#4a5870;--lin-slate-hi:#66758f;--lin-edge:rgba(0,0,0,.2);--lin-grey:#8892a3;--lin-grey-d:#5f6b7f;--lin-born:#0072B2;--lin-died:#D55E00;--lin-reuse:#6a4fbf;color-scheme:light}
.lin-tb{display:flex;gap:8px 10px;align-items:center;flex-wrap:wrap;padding:6px 10px;border-bottom:1px solid var(--lin-line);background:var(--lin-card)}
.lin-tb select,.lin-tb input{background:var(--lin-card);color:inherit;border:1px solid var(--lin-line);border-radius:8px;padding:4px 8px;font:inherit}
.lin-tb input{width:150px}
.lin-seg{display:inline-flex;border:1px solid var(--lin-line);border-radius:8px;overflow:hidden}
.lin-seg button,.lin-btn{background:transparent;border:0;color:inherit;padding:4px 9px;cursor:pointer;font:inherit}
.lin-btn{border:1px solid var(--lin-line);border-radius:8px}
.lin-seg button{border-left:1px solid var(--lin-line)}
.lin-seg button:first-child{border-left:0}
.lin-seg button:hover,.lin-btn:hover{background:var(--hover,#2d2d2d)}
.lin-btn[aria-pressed=true]{border-color:var(--lin-reuse);color:var(--lin-reuse)}
.lin-chip{display:inline-flex;align-items:center;gap:6px;padding:1px 9px;border:1px solid var(--lin-line);border-radius:999px;font-size:11.5px;white-space:nowrap}
.lin-sw{width:12px;height:8px;border-radius:2px;display:inline-block}
.lin-canvas{position:relative;flex:1;min-height:0;overflow:hidden;background:var(--lin-card)}
.lin-canvas>svg{position:absolute;inset:0;display:block;cursor:grab}
.lin-canvas>svg:active{cursor:grabbing}
.lin-mini{position:absolute;right:12px;bottom:12px;width:190px;height:130px;border:1px solid var(--lin-line);border-radius:8px;background:var(--lin-card);opacity:.92;box-shadow:0 4px 14px rgba(0,0,0,.3)}
.lin-mini svg{width:100%;height:100%;display:block;cursor:pointer;touch-action:none}
.lin-note{position:absolute;left:12px;bottom:12px;max-width:60%;padding:4px 10px;border:1px solid var(--lin-line);border-radius:8px;background:var(--lin-card);font-size:12px;opacity:.95}
.lin-note button{background:none;border:0;color:var(--accent,#66d9ef);cursor:pointer;font:inherit;text-decoration:underline}
.lin-drawer{position:absolute;top:0;right:0;bottom:0;width:290px;max-width:85%;transform:translateX(102%);transition:transform .2s ease;background:var(--lin-card);border-left:1px solid var(--lin-line);padding:12px 14px;overflow:auto;z-index:5;box-shadow:-6px 0 18px rgba(0,0,0,.25)}
.lin-drawer.open{transform:none}
.lin-drawer h4{margin:0 0 8px;font-size:13px;word-break:break-all;padding-right:22px}
.lin-x{position:absolute;top:6px;right:8px;background:none;border:0;color:inherit;font-size:18px;cursor:pointer}
.lin-kv{display:grid;grid-template-columns:1fr auto;gap:2px 14px}
.lin-kv .k{opacity:.7}
.lin-dim{opacity:.72;font-size:12px}
.lin-tip{position:fixed;pointer-events:none;display:none;z-index:1000;min-width:200px;max-width:340px;padding:9px 11px;border-radius:10px;background:var(--lin-card);color:var(--text,#f8f8f2);border:1px solid var(--lin-line);box-shadow:0 8px 24px rgba(0,0,0,.35);font:12.5px/1.45 system-ui,sans-serif}
.lin-tip .t{display:flex;align-items:center;gap:7px;font-weight:600;margin-bottom:5px;word-break:break-all}
.lin-dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex:none}
.lin-canvas svg text{font-family:inherit}
.lin-halo{paint-order:stroke;stroke:var(--lin-halo);stroke-width:3.5px;stroke-linejoin:round}
.lin-canvas ::-webkit-scrollbar{width:10px;height:10px}
.lin-drawer{scrollbar-width:thin;scrollbar-color:var(--lin-grey-d) transparent}
.lin-root button:focus-visible,.lin-root select:focus-visible,.lin-root input:focus-visible{outline:2px solid var(--accent,#66d9ef);outline-offset:2px}
`;

    function el(tag, attrs = {}, text) {
        const e = document.createElement(tag);
        for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
        if (text !== undefined) e.textContent = text;
        return e;
    }

    // ---------------------------------------------------------------- model
    // Visible nodes (files, or one aggregate per collapsed subtree) in DFS order.
    function buildModel(doc, collapsed) {
        const byId = new Map(doc.nodes.map((n, idx) => [n.id, { ...n, idx }]));
        const kids = new Map(), inE = new Map();
        for (const e of doc.edges) { (kids.get(e.parent) || kids.set(e.parent, []).get(e.parent)).push(e.child); inE.set(e.child, e); }
        const size = new Map();
        const sz = id => { if (!size.has(id)) size.set(id, 1 + (kids.get(id) || []).reduce((a, c) => a + sz(c), 0)); return size.get(id); };
        sz(doc.root);
        for (const l of kids.values()) l.sort((a, b) => size.get(b) - size.get(a) || (inE.get(b).shared - inE.get(a).shared));
        const rep = new Map(), vis = [];
        const members = (id, out = []) => { out.push(id); for (const c of kids.get(id) || []) members(c, out); return out; };
        const walk = (id, d) => {
            const n = byId.get(id);
            if (collapsed.has(id) && (kids.get(id) || []).length) {
                const ms = members(id), nodes = ms.map(m => byId.get(m));
                const sum = f => nodes.reduce((a, x) => a + (f(x) || 0), 0);
                const a = {
                    id: `agg:${id}`, agg: true, top: id, members: ms, d, file_name: `${ms.length} files`, idx: n.idx,
                    functions: sum(x => x.functions), died_all: sum(x => x.died_all), only_here: sum(x => x.only_here), reuse_in: sum(x => x.reuse_in),
                    born: ms.reduce((s, m) => s + (inE.get(m)?.gained || 0), 0), suspect: ms.reduce((s, m) => s + (inE.get(m)?.recall_suspect || 0), 0),
                };
                for (const m of ms) rep.set(m, a.id);
                vis.push(a);
                return;
            }
            n.d = d; n.kind = 'file';
            rep.set(id, id);
            vis.push(n);
            for (const c of kids.get(id) || []) walk(c, d + 1);
        };
        walk(doc.root, 0);
        return { byId, kids, inE, vis, rep, size };
    }

    // Above AUTO_FILES files: group every subtree below the deepest level that keeps about AUTO_SHOW nodes visible.
    function autoCollapse(doc) {
        const out = new Set();
        if (doc.nodes.length <= AUTO_FILES) return out;
        const kids = new Map(), depth = new Map([[doc.root, 0]]);
        for (const e of doc.edges) { (kids.get(e.parent) || kids.set(e.parent, []).get(e.parent)).push(e.child); depth.set(e.child, depth.get(e.parent) + 1); }
        const perDepth = new Map();
        for (const d of depth.values()) perDepth.set(d, (perDepth.get(d) || 0) + 1);
        let D = 1, seen = 0;
        for (let d = 0; d <= Math.max(...perDepth.keys()); d++) { seen += perDepth.get(d) || 0; if (seen > AUTO_SHOW) break; D = d; }
        D = Math.max(D, 1);
        for (const [id, d] of depth) if (d === D && (kids.get(id) || []).length) out.add(id);
        return out;
    }

    // ---------------------------------------------------------------- layout
    function layout(doc, M) {
        const { vis, rep, kids, inE } = M;
        const fileOf = new Map(vis.map(n => [n.id, n]));
        const repOf = id => fileOf.get(rep.get(id));
        const maxd = Math.max(...vis.map(n => n.d));
        const colSum = new Map();
        for (const n of vis) colSum.set(n.d, (colSum.get(n.d) || 0) + (n.agg ? 40 / 0.5 : Math.max(1, n.functions)));
        const ky = Math.max(KY_MIN, Math.min(KY_MAX, TARGET_H / Math.max(...colSum.values())));
        for (const n of vis) n.h = n.agg ? Math.max(30, Math.min(260, n.functions * ky)) : Math.max(3, n.functions * ky);
        const x0Of = d => PAD + d * (NODE_W + GAP);
        const parentOf = new Map();
        for (const n of vis) if (!n.agg || true) { const e = inE.get(n.agg ? n.top : n.id); if (e) parentOf.set(n.id, rep.get(e.parent)); }

        // reuse classes mapped onto the visible nodes
        const classes = [];
        if (!doc.reuse_skipped) {
            for (const r of (doc.reuse || []).filter(x => !x.folded)) {
                const comps = [];
                for (const c of r.components) {
                    const s = [...new Set(c.map(i => rep.get(i)))];
                    const hit = comps.filter(o => o.some(x => s.includes(x)));
                    if (hit.length) { for (const o of hit) { comps.splice(comps.indexOf(o), 1); s.push(...o); } }
                    comps.push([...new Set(s)]);
                }
                if (comps.length < 2) continue;
                classes.push({ r, comps, files: [...new Set(comps.flat())] });
            }
        }
        const maxC = Math.max(1, ...classes.map(c => c.r.count));
        const kr = Math.min(ky, REUSE_MAX / maxC);
        classes.forEach((c, i) => { c.rank = i; c.th = Math.max(2, c.r.count * kr); });
        const laneH = Math.max(4, ...classes.map(c => c.th)) + 7;

        const place = top => {
            const bottom = new Map();
            let fb = top;
            for (const n of vis) {
                n.x0 = x0Of(n.d); n.x1 = n.x0 + NODE_W;
                const pb = bottom.has(n.d) ? bottom.get(n.d) + NPAD : top;
                n.y0 = Math.max(pb, fileOf.get(parentOf.get(n.id))?.y0 ?? top);
                n.y1 = n.y0 + n.h;
                bottom.set(n.d, n.y1);
                fb = Math.max(fb, n.y1);
            }
            return fb;
        };
        let top = 46, filesBottom = place(top);
        const stripes = new Map(vis.map(n => [n.id, []]));
        for (const c of classes) for (const id of c.files) stripes.get(id).push({ c, n: fileOf.get(id) });
        const restack = () => {
            for (const list of stripes.values()) {
                let cum = 0;
                for (const s of list) { s.y0 = s.n.y0 + cum; s.y1 = s.y0 + s.c.th; cum += s.c.th; }
            }
        };
        restack();
        const stripeOf = (c, id) => stripes.get(id).find(s => s.c === c);

        const arcs = [], slotN = new Map(), used = new Map();
        for (const c of classes) {
            if (c.comps.length > ARC_MAX_PIECES) continue; // broadly shared code: stripes only
            const anchors = c.comps.map(cp => cp.map(id => fileOf.get(id)).sort((a, b) => a.d - b.d || a.idx - b.idx)[0]).sort((a, b) => a.d - b.d || a.y0 - b.y0);
            for (let i = 1; i < anchors.length; i++) {
                const a = anchors[i - 1], b = anchors[i];
                arcs.push({ c, r: c.r, a, b, same: a.d === b.d });
                slotN.set(a.d, (slotN.get(a.d) || 0) + 1);
                if (a.d !== b.d) slotN.set(b.d, (slotN.get(b.d) || 0) + 1);
            }
        }
        const take = d => {
            const n = used.get(d) || 0;
            used.set(d, n + 1);
            return 22 + n * Math.min(laneH, (GAP - 70) / Math.max(1, slotN.get(d)));
        };
        for (const A of arcs) { A.sa = take(A.a.d); A.sb = A.same ? A.sa : take(A.b.d); }
        const laneEnd = [];
        const cross = arcs.filter(A => !A.same).sort((a, b) => a.a.x1 + a.sa - (b.a.x1 + b.sa));
        for (const A of cross) {
            const l0 = A.a.x1 + A.sa - 8;
            let k = laneEnd.findIndex(e => e < l0);
            if (k < 0) k = laneEnd.length;
            laneEnd[k] = A.b.x1 + A.sb + 8;
            A.lane = k;
        }
        if (laneEnd.length) { top = 46 + laneEnd.length * laneH + 18; filesBottom = place(top); restack(); }
        for (const A of arcs) {
            A.ya = stripeOf(A.c, A.a.id).y0 + A.c.th / 2;
            A.yb = stripeOf(A.c, A.b.id).y0 + A.c.th / 2;
            A.yT = top - 44 - (A.lane || 0) * laneH;
            A.d = arcPath(A);
        }

        // body segments of real parents; aggregates get one kept + one died segment
        const partsOf = new Map();
        for (const p of doc.partitions || []) (partsOf.get(p.parent) || partsOf.set(p.parent, []).get(p.parent)).push(p);
        const segs = [];
        for (const n of vis) {
            if (n.agg) {
                const dh = n.functions ? n.h * n.died_all / n.functions : 0;
                segs.push({ n, agg: true, y0: n.y0, y1: n.y1 - dh });
                if (dh > 0) segs.push({ n, died: true, count: n.died_all, y0: n.y1 - dh, y1: n.y1 });
                continue;
            }
            const ps = partsOf.get(n.id);
            if (!ps) continue;
            const pos = c => fileOf.get(rep.get(c)).idx;
            const cmp = (a, b) => {
                const x = a.children.map(pos), y = b.children.map(pos);
                for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i];
                return x.length - y.length;
            };
            let y = n.y0;
            for (const p of [...ps].sort((a, b) => (a.folded ? 1 : 0) - (b.folded ? 1 : 0) || cmp(a, b))) { segs.push({ n, part: p, y0: y, y1: y + p.count * ky }); y += p.count * ky; }
            if (n.died_all > 0) segs.push({ n, died: true, count: n.died_all, y0: y, y1: y + n.died_all * ky });
        }

        // origin strip: born (blue, hatched = suspect) over inherited
        const origs = [];
        for (const n of vis) {
            const e = n.agg ? null : inE.get(n.id);
            const born = n.agg ? n.born : e ? e.gained : 0, sus = Math.min(born, n.agg ? n.suspect : e ? e.recall_suspect : 0);
            const bh = n.agg ? Math.min(n.h, n.h * born / Math.max(1, n.functions)) : Math.min(n.h, born * ky);
            const sh = born ? bh * sus / born : 0;
            if (born > 0) {
                origs.push({ n, kind: 'born', edge: e, count: born - sus, y0: n.y0, y1: n.y0 + bh - sh });
                if (sus > 0) origs.push({ n, kind: 'born', hatch: true, edge: e, count: sus, y0: n.y0 + bh - sh, y1: n.y0 + bh });
            }
            if (n.y0 + bh < n.y1) origs.push({ n, kind: 'inh', edge: e, count: n.functions - born, y0: n.y0 + bh, y1: n.y1 });
        }

        // shared ribbons
        const links = [], cursor = new Map(vis.map(n => [n.id, n.y0 + (n.agg ? 0 : (inE.get(n.id)?.gained || 0) * ky)]));
        const edgeOf = new Map(doc.edges.map(e => [`${e.parent}|${e.child}`, e]));
        for (const sg of segs) {
            if (!sg.part) continue;
            for (const cid of sg.part.children) {
                const t = repOf(cid);
                const v = sg.part.per_child ? sg.part.per_child[cid] : sg.part.count;
                const w = Math.min(Math.max(1, v * ky), t.agg ? t.h : 1e9);
                links.push({
                    kind: 'shared', edge: edgeOf.get(`${sg.n.id}|${cid}`), part: sg.part, value: v, before: 0, width: w, bf: sg.part.by_file,
                    source: sg.n, target: t, y0: sg.y0 + w / 2, y1: cursor.get(t.id) + w / 2, overlap: sg.part.children.length > 1,
                    fade: (kids.get(sg.n.id) || []).length > FAN_FADE,
                });
                cursor.set(t.id, cursor.get(t.id) + (t.agg ? 0 : v * ky));
            }
        }
        const W = x0Of(maxd) + NODE_W + 90, H = filesBottom + 30;
        return { vis, fileOf, links, segs, origs, arcs, classes, stripes, W, H, ky, top };
    }

    function arcPath(A) {
        const xa = A.a.x1, xb = A.b.x1, rr = 12;
        if (A.same) {
            const dir = Math.sign(A.yb - A.ya) || 1, r = Math.max(1, Math.min(rr, Math.abs(A.yb - A.ya) / 2));
            const x = xa + A.sa;
            return `M${xa},${A.ya}H${x - r}Q${x},${A.ya} ${x},${A.ya + dir * r}V${A.yb - dir * r}Q${x},${A.yb} ${x - r},${A.yb}H${xa}`;
        }
        const x1 = xa + A.sa, x2 = xb + A.sb, t = A.yT;
        return `M${xa},${A.ya}H${x1 - rr}Q${x1},${A.ya} ${x1},${A.ya - rr}V${t + rr}Q${x1},${t} ${x1 + rr},${t}H${x2 - rr}Q${x2},${t} ${x2},${t + rr}V${A.yb - rr}Q${x2},${A.yb} ${x2 - rr},${A.yb}H${xb}`;
    }

    // ---- ribbon geometry: one centre cubic per bundle, offset strips of constant width ----
    function sampleCurve(x0, x1, yc0, yc1) {
        const n = Math.max(24, Math.min(72, Math.round((x1 - x0 + Math.abs(yc1 - yc0)) / 16)));
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
    function band(x0, x1, y0, y1, w) {
        const m = (x0 + x1) / 2, a = y0 - w / 2, b = y1 - w / 2;
        return `M${x0},${a}C${m},${a} ${m},${b} ${x1},${b}L${x1},${b + w}C${m},${b + w} ${m},${a + w} ${x0},${a + w}Z`;
    }
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
                l.bb = [x0, Math.min(l.y0, l.y1) - W, x1, Math.max(l.y0, l.y1) + W];
            }
        }
    }

    // dev guard: boxes of one column never intersect; warn once with the two names
    let warned = false;
    function warnOverlap(L) {
        if (warned) return;
        const cols = new Map();
        for (const n of L.vis) (cols.get(n.d) || cols.set(n.d, []).get(n.d)).push(n);
        for (const list of cols.values()) {
            list.sort((a, b) => a.y0 - b.y0);
            for (let i = 1; i < list.length; i++) {
                if (list[i].y0 < list[i - 1].y1) {
                    warned = true;
                    console.warn('lineage: overlapping boxes', list[i - 1].file_name || list[i - 1].id, list[i].file_name || list[i].id);
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------- mount
    function mount(container, opts = {}) {
        if (!document.getElementById('nway-lineage-css')) {
            const css = el('style', { id: 'nway-lineage-css' });
            css.textContent = CSS;
            document.head.append(css);
        }
        uid++;
        const id = uid;
        const root = el('div', { class: 'lin-root' });
        const tb = el('div', { class: 'lin-tb' });
        const canvas = el('div', { class: 'lin-canvas' });
        const drawer = el('div', { class: 'lin-drawer' });
        const noteBox = el('div', { class: 'lin-note', style: 'display:none' });
        const mini = el('div', { class: 'lin-mini' });
        root.append(tb, canvas);
        container.replaceChildren(root);

        const st = { doc: null, M: null, L: null, collapsed: new Set(), reuseOn: true, sel: null, last: null, t: d3.zoomIdentity, hasView: false, hl: null, hover: 0, raf: 0, bfCache: new Map(), dead: false };
        const s = d3.select(canvas).append('svg').attr('role', 'img').attr('aria-label', 'Lineage of the files: shared, born, died and reused code');
        const defs = s.append('defs');
        const world = s.append('g');
        const gRib = world.append('g'), gArc = world.append('g'), gNode = world.append('g'), gDet = world.append('g'), gStr = world.append('g'), gOv = world.append('g').style('pointer-events', 'none'), gLab = world.append('g').style('pointer-events', 'none');
        const zoom = d3.zoom().scaleExtent([K_MIN, K_MAX]).on('zoom', ev => { st.t = ev.transform; world.attr('transform', st.t); schedule(); });
        s.call(zoom);
        const tip = d3.select(document.body).append('div').attr('class', 'lin-vars lin-tip');
        canvas.append(noteBox, mini, drawer);

        // ---- toolbar ----
        for (const e of opts.toolbar || []) tb.append(e);
        const search = el('input', { type: 'search', placeholder: 'Find file or md5', 'aria-label': 'Find a file' });
        const hits = el('span', { class: 'lin-dim' });
        const btn = (txt, title, f, cls = 'lin-btn') => { const b = el('button', { type: 'button', class: cls, title }, txt); b.addEventListener('click', f); return b; };
        const reuseBtn = btn('Reuse layer', 'Show or hide the reuse arcs', () => { st.reuseOn = !st.reuseOn; reuseBtn.setAttribute('aria-pressed', String(st.reuseOn)); applyReuse(); });
        reuseBtn.setAttribute('aria-pressed', 'true');
        const zgrp = el('div', { class: 'lin-seg', role: 'group', 'aria-label': 'Zoom' });
        const zbtn = (txt, title, f) => { const b = el('button', { type: 'button', title, 'aria-label': title }, txt); b.addEventListener('click', f); zgrp.append(b); };
        zbtn('−', 'Zoom out', () => s.transition().duration(200).call(zoom.scaleBy, 1 / 1.5));
        zbtn('+', 'Zoom in', () => s.transition().duration(200).call(zoom.scaleBy, 1.5));
        zbtn('Fit', 'Fit the whole chart', () => fit(true));
        zbtn('100%', 'Actual size', () => s.transition().duration(250).call(zoom.scaleTo, 1));
        zbtn('Selection', 'Zoom to the selected or last hovered file, edge or reuse arc', () => zoomToSel());
        tb.append(search, hits, btn('Collapse deep', 'Group deep subtrees into one node each', () => { st.collapsed = autoCollapse(st.doc); if (!st.collapsed.size) st.collapsed = deepAll(); rebuild(); }),
            btn('Expand all', 'Show every file', () => { st.collapsed = new Set(); rebuild(); }), reuseBtn, zgrp);
        const legendBox = el('span', { style: 'display:contents' });
        tb.append(legendBox);
        const deepAll = () => {
            const out = new Set();
            for (const n of st.M.vis) if (!n.agg && n.d >= 2 && (st.M.kids.get(n.id) || []).length && !out.has(st.M.rep.get(n.id))) out.add(n.id);
            return out;
        };

        // ---- tooltip / drawer ----
        const showTip = (ev, title, dot, rows) => {
            tip.selectAll('*').remove();
            const t = tip.append('div').attr('class', 't');
            t.append('span').attr('class', 'lin-dot').style('background', dot);
            t.append('span').text(title);
            const kv = tip.append('div').attr('class', 'lin-kv');
            for (const [k, v] of rows) { kv.append('span').attr('class', 'k').text(k); kv.append('span').text(v); }
            tip.style('display', 'block');
            moveTip(ev);
        };
        const moveTip = ev => {
            const w = tip.node().offsetWidth || 240;
            tip.style('left', `${Math.min(ev.clientX + 14, window.innerWidth - w - 8)}px`).style('top', `${ev.clientY + 10}px`);
        };
        const hideTip = () => tip.style('display', 'none');
        const name = id => st.names.get(id) || id;
        const names = ids => ids.slice(0, 8).map(name).join(', ') + (ids.length > 8 ? ` +${ids.length - 8} more` : '');

        const openDrawer = pick => { drawer.classList.add('open'); detail(drawer, pick, name, () => drawer.classList.remove('open')); };

        // ---- build ----
        function rebuild() {
            const M = st.M = buildModel(st.doc, st.collapsed);
            const L = st.L = layout(st.doc, M);
            bundle(L.links);
            st.names = new Map(st.doc.nodes.map(n => [n.id, n.file_name || n.id]));
            for (const n of L.vis) if (n.agg) st.names.set(n.id, n.file_name);
            warnOverlap(L);
            drawStatic();
            drawMini();
            note();
            legend();
            if (!st.hasView) fit(false);
            schedule(true);
        }
        function note() {
            noteBox.replaceChildren();
            const n = st.collapsed.size;
            noteBox.style.display = n ? '' : 'none';
            if (!n) return;
            noteBox.append(document.createTextNode(`${n} subtree${n > 1 ? 's' : ''} grouped (${st.doc.nodes.length} files in all). Click a grouped node to expand it. `));
            noteBox.append(btn('Expand all', 'Show every file', () => { st.collapsed = new Set(); rebuild(); }, ''));
        }
        function legend() {
            const doc = st.doc;
            legendBox.replaceChildren();
            const chip = (color, text, mode) => {
                const c = el('span', { class: 'lin-chip' }), sw = el('span', { class: 'lin-sw' });
                sw.style.background = mode === 'hatch' ? `repeating-linear-gradient(45deg,${color} 0 2px,transparent 2px 4px)` : mode === 'dash' ? `repeating-linear-gradient(90deg,${color} 0 4px,transparent 4px 7px)` : color;
                c.append(sw, el('span', {}, text));
                legendBox.append(c);
            };
            const born = doc.edges.reduce((a, e) => a + e.gained, 0), died = doc.nodes.reduce((a, n) => a + (n.died_all || 0), 0);
            chip(V.shared, 'shared'); chip(V.fork, 'kept by several children');
            chip(V.born, `born ${fmt(born)} fn`); chip(V.born, 'hatched: likely matcher miss', 'hatch');
            chip(V.died, `died ${fmt(died)} fn (kept by no child)`);
            if (doc.reuse_skipped) { chip(V.reuse, 'reuse off: too many files'); return; }
            chip(V.reuse, `reused outside the tree path ${fmt(doc.reuse_total || 0)} fn`);
            chip(V.reuse, 'same level'); chip(V.reuse, 'across depth', 'dash');
            const f = (doc.reuse || []).find(r => r.folded);
            if (f) chip(V.reuse, `+${f.classes} more classes, ${fmt(f.count)} fn`);
        }

        let ribs, arcSel, nodeBase, rim, strSel;
        function drawStatic() {
            const L = st.L;
            // a relayout starts from an empty scene: joins would keep the old boxes and arcs
            for (const g of [defs, gRib, gArc, gNode, gStr, gDet, gOv, gLab]) g.selectAll('*').remove();
            for (const [k, c] of [['born', V.born]]) {
                const p = defs.append('pattern').attr('id', `hatch-${k}-${id}`).attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
                p.append('rect').attr('width', 6).attr('height', 6).style('fill', c).attr('fill-opacity', 0.22);
                p.append('rect').attr('width', 3).attr('height', 6).style('fill', c);
            }
            const ov = defs.append('pattern').attr('id', `ovl-${id}`).attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            ov.append('rect').attr('width', 6).attr('height', 6).style('fill', V.slateHi);
            ov.append('rect').attr('width', 2).attr('height', 6).attr('fill', '#fff').attr('fill-opacity', 0.28);
            for (const n of L.vis) defs.append('clipPath').attr('id', `clip-${id}-${n.idx}-${n.agg ? 'a' : 'f'}`).append('rect').attr('x', n.x0).attr('y', n.y0).attr('width', NODE_W).attr('height', n.h).attr('rx', 6);

            L.links.forEach((l, i) => {
                const col = l.overlap ? V.fork : V.shared;
                const g = defs.append('linearGradient').attr('id', `g-${id}-${i}`).attr('gradientUnits', 'userSpaceOnUse').attr('x1', l.source.x1).attr('x2', l.target.x0);
                g.append('stop').attr('offset', 0).style('stop-color', col).attr('stop-opacity', 0.8);
                g.append('stop').attr('offset', 1).style('stop-color', col).attr('stop-opacity', 0.45);
                l.fill = `url(#g-${id}-${i})`; l.col = col;
            });
            ribs = gRib.selectAll('path').data(L.links).join('path').attr('d', l => l.d).style('fill', l => l.fill).style('opacity', l => l.fade ? 0.28 : 1).style('cursor', 'pointer')
                .on('mouseenter', (ev, l) => hoverRib(ev, l)).on('mousemove', moveTip).on('mouseleave', leave)
                .on('click', (ev, l) => pick({ edge: l.edge, part: l.part }));
            arcSel = gArc.selectAll('g').data(L.arcs).join('g').style('cursor', 'pointer');
            arcSel.append('path').attr('d', A => A.d).attr('fill', 'none').attr('stroke', 'transparent').attr('stroke-width', A => Math.max(12, A.c.th));
            arcSel.append('path').attr('class', 'vis').attr('d', A => A.d).attr('fill', 'none').attr('stroke', V.reuse).attr('stroke-opacity', 0.8)
                .attr('stroke-width', A => A.c.th).attr('stroke-dasharray', A => A.r.same_level ? null : '9 5');
            arcSel.on('mouseenter', (ev, A) => hoverReuse(ev, A.r)).on('mousemove', moveTip).on('mouseleave', leave).on('click', (ev, A) => pick({ reuse: A.r }));

            nodeBase = gNode.selectAll('g').data(L.vis, n => n.id).join('g').attr('tabindex', 0).attr('role', 'button').style('cursor', 'pointer')
                .attr('aria-label', n => `${name(n.id)}, ${n.functions} functions`);
            nodeBase.append('rect').attr('class', 'base').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => n.h).attr('rx', 6).style('fill', V.slate);
            rim = nodeBase.append('rect').attr('class', 'rim').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => n.h).attr('rx', 6)
                .attr('fill', 'none').style('stroke', 'var(--lin-edge)').attr('vector-effect', 'non-scaling-stroke').style('pointer-events', 'none');
            nodeBase.on('mouseenter', (ev, n) => hoverFile(ev, n)).on('mousemove', moveTip).on('mouseleave', leave)
                .on('click', (ev, n) => { if (n.agg) { st.collapsed.delete(n.top); rebuild(); } else pick({ node: n }); })
                .on('keydown', (ev, n) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); if (n.agg) { st.collapsed.delete(n.top); rebuild(); } else pick({ node: n }); } });
            nodeBase.append('title').text(n => n.agg ? `${n.file_name}: ${fmt(n.functions)} functions in all` : `${name(n.id)} (${fmt(n.functions)} functions)`);
            // reuse stripes
            const sdata = [...L.stripes.values()].flat();
            strSel = gStr.selectAll('rect').data(sdata).join('rect').attr('x', d => d.n.x1 - REUSE_W).attr('y', d => d.y0).attr('width', REUSE_W).attr('height', d => d.c.th)
                .style('fill', V.reuse).attr('clip-path', d => `url(#clip-${id}-${d.n.idx}-${d.n.agg ? 'a' : 'f'})`).style('cursor', 'pointer')
                .on('mouseenter', (ev, d) => hoverReuse(ev, d.c.r)).on('mousemove', moveTip).on('mouseleave', leave).on('click', (ev, d) => pick({ reuse: d.c.r }));
            applyReuse();
            applySel();
            detKey.clear();
            gDet.selectAll('*').remove();
        }
        function applyReuse() {
            const d = st.reuseOn ? null : 'none';
            gArc.style('display', d); gStr.style('display', d);
        }
        function applySel() {
            const sel = st.sel;
            const on = n => sel && ((sel.node && sel.node.id === n.id) || (sel.parent && sel.parent === n.id) || (sel.reuse && sel.reuse.files.includes(n.id)) || (st.hl === n.id));
            if (!rim) return;
            rim.style('stroke', n => on(n) ? 'var(--accent,#66d9ef)' : 'var(--lin-edge)').attr('stroke-width', n => on(n) ? 3 : 1);
            if (ribs) ribs.style('stroke', l => sel && sel.edge && l.edge === sel.edge ? 'var(--accent,#66d9ef)' : 'none').attr('stroke-width', 1.5).attr('vector-effect', 'non-scaling-stroke');
        }
        function pick(p) { st.sel = p; st.last = p; applySel(); openDrawer(p); }

        // ---- minimap ----
        let miniVp, miniS = 1;
        function drawMini() {
            mini.replaceChildren();
            const { W, H, vis } = st.L;
            const ms = d3.select(mini).append('svg').attr('viewBox', '0 0 190 130');
            miniS = Math.min(186 / W, 126 / H);
            const g = ms.append('g').attr('transform', `translate(2,2) scale(${miniS})`);
            g.selectAll('rect').data(vis).join('rect').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => Math.max(n.h, 2 / miniS)).style('fill', n => n.agg ? V.slateHi : V.slate);
            if (!st.doc.reuse_skipped) g.append('g').selectAll('path').data(st.L.arcs).join('path').attr('d', A => A.d).attr('fill', 'none').attr('stroke', V.reuse).attr('stroke-width', 1 / miniS).attr('stroke-opacity', 0.6);
            miniVp = ms.append('rect').attr('fill', 'var(--accent,#66d9ef)').attr('fill-opacity', 0.12).attr('stroke', 'var(--accent,#66d9ef)').attr('stroke-width', 1.5);
            const go = ev => {
                const r = ms.node().getBoundingClientRect(), k = r.width / 190;
                const wx = ((ev.clientX - r.left) / k - 2) / miniS, wy = ((ev.clientY - r.top) / k - 2) / miniS;
                s.call(zoom.translateTo, wx, wy);
            };
            ms.on('pointerdown', ev => { ms.node().setPointerCapture(ev.pointerId); go(ev); ms.on('pointermove', go); })
                .on('pointerup pointercancel', () => ms.on('pointermove', null));
            miniVp.style('pointer-events', 'none');
        }

        // ---- semantic zoom update ----
        const detKey = new Map();
        const size = () => ({ w: canvas.clientWidth || 800, h: canvas.clientHeight || 500 });
        function schedule(force) {
            if (force) detKey.clear();
            if (st.raf || st.dead) return;
            st.raf = requestAnimationFrame(() => { st.raf = 0; update(); });
        }
        function update() {
            if (st.dead || !st.L) return;
            const { w, h } = size(), t = st.t, k = t.k;
            const m = 120 / k;
            const vx0 = -t.x / k - m, vy0 = -t.y / k - m, vx1 = (w - t.x) / k + m, vy1 = (h - t.y) / k + m;
            const inV = (x0, y0, x1, y1) => x1 >= vx0 && x0 <= vx1 && y1 >= vy0 && y0 <= vy1;
            const L = st.L;
            ribs.attr('display', l => inV(...l.bb) ? null : 'none');
            if (miniVp) miniVp.attr('x', 2 + vx0 * miniS + m * miniS).attr('y', 2 + vy0 * miniS + m * miniS).attr('width', Math.max(2, (w / k) * miniS)).attr('height', Math.max(2, (h / k) * miniS));
            // detail layer: only nodes on screen, and only past a zoom threshold
            const lvl = k >= HATCH_K ? 2 : 1;
            const want = k >= DETAIL_K ? L.vis.filter(n => inV(n.x0, n.y0, n.x1, n.y1)) : [];
            const keep = new Set(want.map(n => `${n.id}|${lvl}`));
            for (const key of [...detKey.keys()]) if (!keep.has(key)) { detKey.get(key).remove(); detKey.delete(key); }
            for (const n of want) {
                const key = `${n.id}|${lvl}`;
                if (detKey.has(key)) continue;
                detKey.set(key, drawDetail(n, lvl));
            }
            // labels: largest first, no overlap, by zoom and node size
            gLab.selectAll('*').remove();
            if (k >= LABEL_K) {
                const boxes = [];
                const cand = L.vis.filter(n => inV(n.x0, n.y0, n.x1, n.y1) && (n.h * k >= 10 || k >= 1.2)).sort((a, b) => b.h - a.h);
                for (const n of cand) {
                    const f = name(n.id), txt = f.length > 22 ? `${f.slice(0, 21)}…` : f;
                    const lw = txt.length * 7 + 56, bx = [n.x0, n.y0 - 22, n.x0 + lw, n.y0 - 4];
                    if (boxes.some(b => bx[0] < b[2] && bx[2] > b[0] && bx[1] < b[3] && bx[3] > b[1])) continue;
                    boxes.push(bx);
                    const tx = gLab.append('text').attr('class', 'lin-halo').attr('x', n.x0).attr('y', n.y0 - 9).attr('font-size', 12.5).attr('font-weight', 600).style('fill', 'var(--text)');
                    tx.append('tspan').text(txt);
                    tx.append('tspan').attr('dx', 6).attr('font-weight', 400).attr('font-size', 11.5).attr('fill-opacity', 0.7).text(`${fmt(n.functions)} fn`);
                    if (n.agg) tx.append('tspan').attr('dx', 6).attr('font-size', 11).text('(click to expand)');
                }
            }
            hits.textContent = hits._m ? hits._m : '';
        }
        function drawDetail(n, lvl) {
            const L = st.L, g = gDet.append('g').attr('clip-path', `url(#clip-${id}-${n.idx}-${n.agg ? 'a' : 'f'})`);
            for (const o of L.origs.filter(o => o.n === n)) {
                g.append('rect').attr('x', n.x0).attr('y', o.y0).attr('width', ORIG_W).attr('height', Math.max(0.5, o.y1 - o.y0))
                    .style('fill', o.kind === 'inh' ? V.slate : o.hatch && lvl === 2 ? `url(#hatch-born-${id})` : V.born).attr('fill-opacity', o.hatch && lvl === 1 ? 0.65 : 1)
                    .style('cursor', 'pointer').on('mouseenter', ev => hoverOrig(ev, o)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', () => { if (o.edge) pick({ edge: o.edge }); });
            }
            g.append('rect').attr('x', n.x0 + ORIG_W - 0.5).attr('y', n.y0).attr('width', 1).attr('height', n.h).style('fill', 'var(--lin-edge)');
            for (const sg of L.segs.filter(x => x.n === n)) {
                const fork = sg.part && sg.part.children.length > 1;
                g.append('rect').attr('x', n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(0.5, sg.y1 - sg.y0))
                    .style('fill', sg.died ? V.died : fork ? V.slateHi : V.slate).style('stroke', lvl === 2 ? 'var(--lin-edge)' : 'none').attr('stroke-width', 0.75).attr('vector-effect', 'non-scaling-stroke')
                    .style('cursor', 'pointer').on('mouseenter', ev => hoverSeg(ev, sg)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', () => pick(sg.died || sg.agg ? { node: n.agg ? null : n } : { part: sg.part, parent: n.id }));
                if (fork && lvl === 2) g.append('rect').attr('x', n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(0.5, sg.y1 - sg.y0)).attr('fill', `url(#ovl-${id})`).style('pointer-events', 'none');
            }
            return g;
        }

        // ---- hover ----
        function dimRibs(pred) {
            ribs.style('opacity', l => pred(l) ? 1 : 0.08);
            arcSel.style('opacity', 0.25);
            strSel.style('opacity', 0.4);
        }
        function leave() {
            st.hover++;
            ribs.style('opacity', l => l.fade ? 0.28 : 1);
            arcSel.style('opacity', 1); strSel.style('opacity', 1);
            gOv.selectAll('*').remove();
            gDet.style('opacity', 1);
            nodeBase.style('opacity', 1);
            hideTip();
        }
        const partRows = p => [['Kept by', p.folded ? 'several children (folded)' : names(p.children)], ['Functions', fmt(p.count)]];
        const edgeRows = e => [['Born', fmt(e.gained)], ['Died', fmt(e.lost)], ['Shared', fmt(e.shared)], ['Recall suspect', fmt(e.recall_suspect)]];
        function hoverRib(ev, l) {
            st.last = { edge: l.edge };
            dimRibs(d => d.edge === l.edge);
            showTip(ev, `${name(l.edge.parent)} → ${name(l.edge.child)}`, V.shared, [...edgeRows(l.edge), ...partRows(l.part)]);
        }
        function hoverSeg(ev, sg) {
            const n = sg.n;
            if (sg.agg) { showTip(ev, n.file_name, V.slate, [['Functions (summed)', fmt(n.functions)], ['Born inside', fmt(n.born)], ['Kept by no child', fmt(n.died_all)], ['In reuse', fmt(n.reuse_in)]]); return; }
            st.last = { node: n };
            dimRibs(l => sg.part && l.part === sg.part);
            if (sg.died) showTip(ev, `${name(n.id)}: died`, V.died, [['Kept by no child', fmt(sg.count)]]);
            else showTip(ev, `${name(n.id)}: kept by`, sg.part.children.length > 1 ? V.fork : V.shared, partRows(sg.part));
        }
        function hoverOrig(ev, o) {
            const n = o.n;
            if (o.kind === 'inh') showTip(ev, `${name(n.id)}: inherited`, V.slate, [['Functions', fmt(o.count)]]);
            else showTip(ev, `${name(n.id)}: born here`, V.born, [['Functions', fmt(o.count)], ['', o.hatch ? 'likely a matcher miss' : 'not in the parent'], ...(o.edge ? edgeRows(o.edge) : [])]);
        }
        function hoverReuse(ev, r) {
            st.last = { reuse: r };
            ribs.style('opacity', 0.08);
            arcSel.style('opacity', A => A.r === r ? 1 : 0.15);
            strSel.style('opacity', d => d.c.r === r ? 1 : 0.2);
            nodeBase.style('opacity', n => r.files.some(f => st.M.rep.get(f) === n.id) ? 1 : 0.45);
            showTip(ev, 'Code outside the tree path', V.reuse, [['Files', names(r.files)], ['Functions', fmt(r.count)], ['Level', r.same_level ? 'same level' : `across depth (span ${r.depth_span})`], ['Tree pieces', r.components.length], ['Note', CAVEAT]]);
        }
        async function hoverFile(ev, F) {
            const tok = ++st.hover;
            st.last = F.agg ? null : { node: F };
            if (F.agg) {
                showTip(ev, F.file_name, V.slate, [['Functions (summed)', fmt(F.functions)], ['Born inside', fmt(F.born)], ['Kept by no child', fmt(F.died_all)], ['In reuse', fmt(F.reuse_in)], ['', 'click to expand']]);
                return;
            }
            const rows = [['Functions', fmt(F.functions)], ['In reuse classes', F.reuse_in == null ? 'n/a' : fmt(F.reuse_in)]];
            // the reuse arcs that touch this file light up at once; the per-file overlay follows
            arcSel.style('opacity', A => A.r.files.includes(F.id) ? 1 : 0.12);
            strSel.style('opacity', d => d.c.r.files.includes(F.id) ? 1 : 0.25);
            ribs.style('opacity', 0.1);
            showTip(ev, name(F.id), V.slate, rows);
            const bfDoc = await bfFor(F.id);
            if (tok !== st.hover || !bfDoc) return;
            const L = st.L, ni = F.idx;
            const pbf = new Map(), ebf = new Map();
            (st.doc.partitions || []).forEach((p, i) => pbf.set(p, ((bfDoc.partitions[i] || {}).by_file || {})[F.id] || 0));
            st.doc.edges.forEach((e, i) => ebf.set(e, ((bfDoc.edges[i] || {}).born_by_file || {})[F.id] || 0));
            for (const l of L.links) {
                const n = Math.min(l.value, pbf.get(l.part) || 0);
                if (!n) continue;
                const w = Math.max(1, n * L.ky);
                gOv.append('path').attr('d', l.pts ? strip(l.pts, l.o, w) : band(l.source.x1, l.target.x0, l.y0 - l.width / 2 + w / 2, l.y1 - l.width / 2 + w / 2, w)).style('fill', l.col);
            }
            for (const sg of L.segs) {
                if (!sg.part && !sg.died) continue;
                const n = sg.died ? (F.id === sg.n.id ? sg.count : (bfDoc.nodes[sg.n.idx].died_by_file || {})[F.id] || 0) : pbf.get(sg.part) || 0;
                if (!n) continue;
                gOv.append('rect').attr('x', sg.n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(1, Math.min(n * L.ky, sg.y1 - sg.y0)))
                    .style('fill', sg.died ? V.died : sg.part.children.length > 1 ? V.slateHi : V.slate).style('stroke', 'var(--accent,#66d9ef)').attr('stroke-width', 1).attr('vector-effect', 'non-scaling-stroke');
            }
            gDet.style('opacity', 0.35);
            const nn = bfDoc.nodes[ni];
            showTip(ev, name(F.id), V.slate, [...rows, ['In every file (core)', fmt(bfDoc.core)], ['Only in this file', fmt(nn.only_here)], ...(nn.top_shared || []).map(([g, c]) => [`Shared with ${name(g)}`, fmt(c)])]);
            void ebf;
        }
        async function bfFor(fid) {
            if (!st.doc.by_file_skipped) return st.doc;
            if (st.bfCache.has(fid)) return st.bfCache.get(fid);
            const d = opts.fetchFile ? await opts.fetchFile(fid).catch(() => null) : null;
            if (d) st.bfCache.set(fid, d);
            return d;
        }

        // ---- zoom helpers ----
        function fit(animate) {
            const { w, h } = size(), { W, H } = st.L;
            const kf = Math.min(w / W, h / H, 1);
            // the first view keeps a readable scale: the minimap and Fit cover the rest
            const k = Math.max(K_MIN, animate ? kf : Math.max(kf, 0.3));
            const tr = d3.zoomIdentity.translate(k > kf ? 20 : (w - W * k) / 2, k > kf ? 10 : Math.max(8, (h - H * k) / 2)).scale(k);
            st.hasView = true;
            (animate ? s.transition().duration(400) : s).call(zoom.transform, tr);
        }
        function zoomBox(bb) {
            const { w, h } = size();
            const bw = Math.max(40, bb[2] - bb[0]), bh = Math.max(40, bb[3] - bb[1]);
            const k = Math.max(K_MIN, Math.min(K_MAX, 0.85 * Math.min(w / bw, h / bh)));
            const tr = d3.zoomIdentity.translate(w / 2 - k * (bb[0] + bb[2]) / 2, h / 2 - k * (bb[1] + bb[3]) / 2).scale(k);
            s.transition().duration(500).call(zoom.transform, tr);
        }
        function boxOf(nodes) {
            return [Math.min(...nodes.map(n => n.x0)), Math.min(...nodes.map(n => n.y0)), Math.max(...nodes.map(n => n.x1 + 60)), Math.max(...nodes.map(n => n.y1))];
        }
        function zoomToSel() {
            const p = st.sel || st.last;
            if (!p) return;
            const L = st.L, vis = id => L.fileOf.get(st.M.rep.get(id));
            let nodes = [];
            if (p.node) nodes = [vis(p.node.id)];
            else if (p.edge) nodes = [vis(p.edge.parent), vis(p.edge.child)];
            else if (p.reuse) nodes = p.reuse.files.map(vis);
            else if (p.parent) nodes = [vis(p.parent)];
            nodes = nodes.filter(Boolean);
            if (nodes.length) zoomBox(boxOf(nodes));
        }

        // ---- search ----
        let hitList = [], hitAt = 0;
        function find(next) {
            const q = search.value.trim().toLowerCase();
            st.hl = null;
            if (!q) { hits._m = ''; hits.textContent = ''; applySel(); return; }
            if (!next) {
                hitList = st.doc.nodes.filter(n => (n.file_name || '').toLowerCase().includes(q) || n.id.toLowerCase().includes(q)).map(n => n.id);
                hitAt = 0;
            } else hitAt = (hitAt + 1) % Math.max(1, hitList.length);
            hits._m = hitList.length ? `${hitAt + 1}/${hitList.length}` : 'no match';
            hits.textContent = hits._m;
            if (!hitList.length) return;
            const id0 = hitList[hitAt];
            const top = st.M.rep.get(id0);
            if (top && top.startsWith('agg:')) { st.collapsed.delete(top.slice(4)); rebuild(); }
            st.hl = id0;
            applySel();
            zoomBox(boxOf([st.L.fileOf.get(st.M.rep.get(id0))]));
        }
        search.addEventListener('input', () => find(false));
        search.addEventListener('keydown', ev => { if (ev.key === 'Enter') { ev.preventDefault(); find(true); } });

        // ---- size ----
        const ro = new ResizeObserver(() => { const { w, h } = size(); s.attr('width', w).attr('height', h); schedule(); });
        ro.observe(canvas);
        const dblClose = ev => { if (ev.key === 'Escape') drawer.classList.remove('open'); };
        document.addEventListener('keydown', dblClose);

        return {
            setDoc(doc) {
                st.doc = doc;
                st.bfCache.clear();
                st.sel = null;
                drawer.classList.remove('open');
                st.names = new Map(doc.nodes.map(n => [n.id, n.file_name || n.id]));
                st.collapsed = autoCollapse(doc);
                const { w, h } = size();
                s.attr('width', w).attr('height', h);
                rebuild();
            },
            zoomTo(wx, wy, k) {
                const { w, h } = size();
                s.call(zoom.transform, d3.zoomIdentity.translate(w / 2 - k * wx, h / 2 - k * wy).scale(k));
            },
            state: st,
            destroy() {
                st.dead = true;
                cancelAnimationFrame(st.raf);
                ro.disconnect();
                document.removeEventListener('keydown', dblClose);
                s.on('.zoom', null);
                tip.remove();
                container.replaceChildren();
            },
        };
    }

    function detail(box, pick, nm, close) {
        box.replaceChildren();
        const x = el('button', { class: 'lin-x', type: 'button', 'aria-label': 'Close' }, '×');
        x.addEventListener('click', close);
        box.append(x);
        const kv = rows => {
            const g = el('div', { class: 'lin-kv' });
            for (const [k, v] of rows) g.append(el('span', { class: 'k' }, k), el('span', {}, String(v)));
            box.append(g);
        };
        if (pick.reuse) {
            const r = pick.reuse;
            box.append(el('h4', {}, `Reused outside the tree path: ${r.files.map(nm).join(' + ')}`));
            kv([['Functions', fmt(r.count)], ['Level', r.same_level ? 'same level' : `across depth (span ${r.depth_span})`], ['Tree pieces', r.components.length]]);
            r.components.forEach((c, i) => kv([[`Piece ${i + 1}`, c.map(nm).join(', ')]]));
            box.append(el('div', { class: 'lin-dim', style: 'margin-top:8px' }, `The tree has no path between these pieces: ${CAVEAT}.`));
            return;
        }
        if (pick.part) {
            const p = pick.part;
            box.append(el('h4', {}, `${nm(p.parent)}: kept by ${p.folded ? 'several children (folded)' : p.children.map(nm).join(' + ')}`));
            kv([['Functions', fmt(p.count)], ...Object.entries(p.per_child || {}).map(([c, n]) => [nm(c), fmt(n)])]);
        }
        if (pick.node) {
            const n = pick.node;
            box.append(el('h4', {}, n.file_name || n.id));
            kv([['Functions', fmt(n.functions) + (n.root ? ' (root)' : '')], ...(n.died_all != null ? [['Kept by no child', fmt(n.died_all)]] : []), ['Only in this file', fmt(n.only_here)], ['In reuse classes', n.reuse_in == null ? 'n/a' : fmt(n.reuse_in)]]);
            box.append(el('div', { class: 'lin-dim', style: 'margin-top:8px;word-break:break-all' }, n.id));
            if (!n.agg && window.Nav) {
                // pivots: file page, its functions, its similar functions, the N-way table filtered to it
                const [col, , md5] = n.id.split(':'), m = encodeURIComponent(md5);
                const q = new URLSearchParams(location.search);
                q.delete('order'); q.delete('root'); q.set('column', n.id);
                const go = (txt, url, type) => {
                    const b = el('button', { type: 'button', class: 'lin-btn', style: 'margin:6px 6px 0 0' }, txt);
                    b.addEventListener('click', ev => window.Nav.openPath(url, ev, { title: txt, type }));
                    box.append(b);
                };
                go('File', window.Nav.buildUIUrl(col, ['files', md5]), 'file');
                go('Functions', `${window.Nav.buildUIUrl(col, ['functions'])}?file_md5=${m}`, 'functions');
                go('Similar functions', `${window.Nav.buildUIUrl(col, ['functions', 'similarities'])}?md5=${m}`, 'functions');
                go('N-way: only this file', `/diff/nway?${q}`, 'bin_sim');
            }
        } else if (pick.edge) {
            const e = pick.edge, ev = e.recall_evidence || {};
            if (pick.part) box.append(el('div', { style: 'height:10px' }));
            box.append(el('h4', {}, `${nm(e.parent)} → ${nm(e.child)}`));
            kv([['Similarity', e.similarity], ['Shared', fmt(e.shared)], ['Born', fmt(e.gained)], ['Lost', fmt(e.lost)], ['Recall suspect', fmt(e.recall_suspect)],
                ['  below the floor', fmt(ev.edge_below_floor)], ['  refused by one-per-file', fmt(ev.edge_refused)], ['  same name', fmt(ev.name)]]);
        }
    }

    window.NwayLineage = { mount, _layout: { buildModel, layout, autoCollapse } }; // _layout: for the layout check
})();
