/**
 * Births view of the lineage page (lview=births): what code first appears when, and how long it lasts.
 *
 * Needs the time-ordered doc (order=time: nodes carry t and rank) and its row list. A row is born in its
 * holder with the lowest date rank (undated nodes rank last, so they give birth only to rows no dated
 * node holds); rows failing the page filters (rows.match) are left out. A birth (cohort) is the rows
 * born in one file, week or month. Each Timeline lane (layout()'s n.lane, a chain of files sharing code)
 * is drawn as a stream: its height is the file's matching functions, its layers are the cohorts those
 * functions were born in, oldest at the bottom; a fork is fed by ribbons from its parent's layers (inherited code); coloured by the lens's groups (each birth split into its groups, so
 * every group is visible at every date) or by birth date (one layer per birth, old to new). A strip under the lanes counts the rows born in each
 * file. Selecting a cohort greys the others (born before / after) and splits it by the lens's groups;
 * the side pane says where it was born, how much of it later files keep, per lane, and its functions.
 *
 * NwayBirths.model(doc, R, bucket) and share(M, c) are DOM-free (checked in scripts/test_nway_lineage.js).
 * NwayBirths.mount(ctx) -> { draw(), box(), dotAt(i), hoverTrack(test), setTrack(), state(), clear(k), destroy() }; ctx is what nway_lineage.js hands over
 * (g, controls, getDoc, getR, getL, getCats, getTrack, setTrack, showTip, hideTip, rowEl, el,
 * segSwitch, fmt, name, reuseOn, width, fit, opts {bucket, axis, sel, color}, onOpt). Every sample value is set with
 * textContent or d3 .text(), never innerHTML.
 */
(function () {
    const DAY = 864e5;
    const MON = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' ');
    const MONL = 'January February March April May June July August September October November December'.split(' ');
    const iso = t => new Date(t).toISOString().slice(0, 10);
    const pct = v => `${Math.round(v * 100)}%`;
    const short = (s, n = 22) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);
    // ponytail: fixed geometry; TARGET_H is the lanes' height at k=1 before the KY clamps; RIB_MAX caps the reuse ribbons
    // (the biggest kept), raise it or bundle them if real sets lose ones that matter
    const B = { RIB_MAX: 400, TOP: 58, PAD: 16, LABEL_W: 170, MR: 70, STEP_MIN: 26, CAL_PX: 18, LANE_GAP: 30, TARGET_H: 640, KY_MAX: 1.2, KY_MIN: 0.03, STRIP_H: 70 };
    const BUCKETS = ['file', 'week', 'month'];

    // ---------------------------------------------------------------- model (no DOM)
    function bucketOf(n, bucket) {
        const nm = n.file_name || n.id;
        if (bucket === 'file') return { key: n.id, sort: n.rank ?? 0, label: nm, long: `Born in ${nm}` };
        if (n.t == null) return { key: 'undated', sort: Infinity, label: 'Undated', long: 'Born in an undated file' };
        if (bucket === 'week') {
            const ws = n.t - (n.t % DAY) - ((new Date(n.t).getUTCDay() + 6) % 7) * DAY;
            return { key: iso(ws), sort: ws, label: `Week of ${iso(ws)}`, long: `Born in the week of ${iso(ws)}` };
        }
        const d = new Date(n.t), y = d.getUTCFullYear(), m = d.getUTCMonth();
        return { key: `${y}-${String(m + 1).padStart(2, '0')}`, sort: Date.UTC(y, m, 1), label: `${MON[m]} ${y}`, long: `Born in ${MONL[m]} ${y}` };
    }

    // birth[r] = node index the row is born in (-1: filtered out), cohorts in time order (list), per node
    // the matching rows it holds (heldM) and the rows born in it (bornIn), per row its holders by date rank (holders)
    function model(doc, R, bucket) {
        const nodes = doc.nodes, N = nodes.length;
        const pos = new Int32Array(N);
        nodes.map((n, i) => i).sort((a, b) => (nodes[a].rank ?? a) - (nodes[b].rank ?? b)).forEach((i, k) => { pos[i] = k; });
        const birth = new Int32Array(R.n).fill(-1), heldM = nodes.map(() => []), bornIn = nodes.map(() => []);
        for (let i = 0; i < N; i++) for (const r of R.held[i]) if (R.match(r)) { heldM[i].push(r); if (birth[r] < 0 || pos[i] < pos[birth[r]]) birth[r] = i; }
        const keyOf = [], byKey = new Map();
        nodes.forEach((n, i) => { const b = bucketOf(n, bucket); keyOf[i] = b.key; if (!byKey.has(b.key)) byKey.set(b.key, { ...b, nodes: [], rows: [] }); });
        for (let r = 0; r < R.n; r++) if (birth[r] >= 0) { bornIn[birth[r]].push(r); byKey.get(keyOf[birth[r]]).rows.push(r); }
        for (const i of [...Array(N).keys()].sort((a, b) => pos[a] - pos[b])) if (bornIn[i].length) byKey.get(keyOf[i]).nodes.push(i);
        const list = [...byKey.values()].filter(c => c.rows.length).sort((a, b) => a.sort - b.sort);
        list.forEach((c, k) => { c.idx = k; });
        const holders = Array.from({ length: R.n }, () => []);
        for (const i of [...Array(N).keys()].sort((a, b) => pos[a] - pos[b])) for (const r of heldM[i]) holders[r].push(i);
        return { doc, R, bucket, pos, birth, keyOf, byKey, list, heldM, bornIn, holders, cohortOf: r => byKey.get(keyOf[birth[r]]) };
    }

    // Code a node holds that its parent does not and that it was not born in: from its nearest earlier holder
    // (reuse, independent birth or matcher miss). [{ src, dst, rows }] (node indices), only across chains (chainOf).
    function reuse(M, parentIdx, chainOf) {
        const out = new Map();
        M.heldM.forEach((rows, v) => {
            const p = parentIdx(v);
            for (const r of rows) {
                if (M.birth[r] === v || (p >= 0 && M.R.has(r, p))) continue;
                let src = -1;
                for (const i of M.holders[r]) { if (M.pos[i] >= M.pos[v]) break; src = i; }
                if (src < 0 || chainOf(src) === chainOf(v)) continue;
                const k = `${src}>${v}`;
                (out.get(k) || out.set(k, { src, dst: v, rows: [] }).get(k)).rows.push(r);
            }
        });
        return [...out.values()];
    }

    // per node: the share of cohort c it holds
    function share(M, c) {
        const mark = new Uint8Array(M.birth.length);
        for (const r of c.rows) mark[r] = 1;
        return M.heldM.map(h => { let k = 0; for (const r of h) k += mark[r]; return k / c.rows.length; });
    }

    // ---------------------------------------------------------------- renderer
    function mount(ctx) {
        const el = ctx.el, fmt = ctx.fmt, g = d3.select(ctx.g);
        const o = { bucket: BUCKETS.includes(ctx.opts.bucket) ? ctx.opts.bucket : 'month', axis: ctx.opts.axis === 'cal' ? 'cal' : 'rank', sel: ctx.opts.sel || '', color: ctx.opts.color === 'birth' ? 'birth' : 'lens' };
        let M = null, frame = null, lanes = [], paneHover = null;
        const dotPos = new Map(); // node index -> its file dot, for the page's highlight of a table row or column
        const emit = () => ctx.onOpt({ ...o });
        const getModel = () => {
            const doc = ctx.getDoc(), R = ctx.getR();
            if (!M || M.doc !== doc || M.R !== R || M.bucket !== o.bucket) M = model(doc, R, o.bucket);
            return M;
        };
        const current = () => { const c = M && M.byKey.get(o.sel); return c && c.rows.length ? c : null; };
        const select = c => { o.sel = c ? c.key : ''; emit(); draw(); };
        const cohortColor = c => (c.key === 'undated' ? 'var(--lin-grey-d)' : `color-mix(in oklab, var(--lin-born) ${Math.round(12 + 88 * c.idx / Math.max(1, M.list.length - 1))}%, var(--lin-slate))`);
        // the lens's groups of a cohort, biggest first
        const modulesOf = (c, C) => {
            const n = new Map();
            for (const r of c.rows) n.set(C.of(r), (n.get(C.of(r)) || 0) + 1);
            const cat = new Map(C.cats.map(x => [x.key, x]));
            return [...n].map(([key, k]) => ({ key, n: k, cat: cat.get(key) || { key, label: 'Other functions', color: 'var(--lin-grey-d)' } })).sort((a, b) => b.n - a.n);
        };

        // ---- toolbar: grouping, axis, the selected birth with a stepper ----
        const colB = ctx.segSwitch('Colour', 'Layers coloured by the lens\'s groups, or by when they were born', [['lens', 'Lens groups'], ['birth', 'Birth date']], () => o.color, v => { o.color = v; emit(); draw(); });
        const byB = ctx.segSwitch('Births by', 'Group what is born by file, week or month', [['file', 'File'], ['week', 'Week'], ['month', 'Month']], () => o.bucket, v => {
            const c = current(), anchor = c ? c.nodes[0] : -1;
            o.bucket = v;
            getModel();
            o.sel = anchor >= 0 ? M.keyOf[anchor] : '';
            emit(); draw();
        });
        const axB = ctx.segSwitch('X axis', 'Files evenly spaced in date order, or on a calendar', [['rank', 'File order'], ['cal', 'Calendar']], () => o.axis, v => { o.axis = v; emit(); draw(); ctx.fit(); });
        const pick = el('select', { 'aria-label': 'Selected birth', title: 'The birth shown in the side pane' });
        const stepBtn = (txt, lab, d) => {
            const b = el('button', { type: 'button', class: 'lin-btn', 'aria-label': lab, title: lab }, txt);
            b.addEventListener('click', () => { const c = current(), i = c ? c.idx + d : d > 0 ? 0 : M.list.length - 1; if (M.list[i]) select(M.list[i]); });
            return b;
        };
        const prevB = stepBtn('‹', 'Previous birth', -1), nextB = stepBtn('›', 'Next birth', 1);
        pick.addEventListener('change', () => select(M.byKey.get(pick.value) || null));
        const stepper = el('span', { style: 'display:inline-flex;gap:4px;align-items:center' });
        stepper.append(prevB, pick, nextB);
        ctx.controls.append(colB.el, byB.el, axB.el, stepper);
        const syncControls = () => {
            colB.paint(); byB.paint(); axB.paint();
            pick.replaceChildren(el('option', { value: '' }, 'No birth selected'));
            for (const c of M.list) pick.append(el('option', { value: c.key }, `${short(c.label, 28)} · ${fmt(c.rows.length)}`));
            const c = current();
            pick.value = c ? c.key : '';
            prevB.disabled = !!c && c.idx === 0;
            nextB.disabled = !!c && c.idx === M.list.length - 1;
        };

        function draw() {
            g.selectAll('*').remove();
            const doc = ctx.getDoc(), R = ctx.getR(), L = ctx.getL();
            if (!doc || !L) return;
            if (!R || doc.order !== 'time') {
                M = null;
                g.append('text').attr('x', 20).attr('y', 40).style('fill', 'var(--text)').text('Births needs the dated lineage (order=time) and its row list.');
                frame = { x0: 0, y0: 0, x1: 520, y1: 80 };
                return;
            }
            getModel();
            const C = ctx.getCats(), fc = current();
            const vis = L.vis.filter(v => !v.agg).sort((a, b) => a.d - b.d), n = vis.length;
            const tot = v => M.heldM[v.idx].length;

            // x: one evenly spaced column per file, or a calendar (undated files after its end)
            const W0 = Math.max(700, ctx.width() - 20), x0 = B.PAD + B.LABEL_W;
            const ticks = [];
            let X, xEnd, hw, step = B.STEP_MIN;
            if (o.axis === 'rank') {
                step = Math.max(B.STEP_MIN, (W0 - x0 - B.MR) / Math.max(1, n - 1));
                X = v => x0 + v.d * step; xEnd = x0 + (n - 1) * step; hw = Math.min(9, step * 0.3);
                let lastM = null, lastX = -1e9;
                for (const v of vis) {
                    const m = v.t == null ? 'undated' : iso(v.t).slice(0, 7);
                    if (m !== lastM && X(v) - lastX > 60) { ticks.push({ x: X(v), label: m === 'undated' ? m : `${MON[+m.slice(5) - 1]} ’${m.slice(2, 4)}` }); lastX = X(v); }
                    lastM = m;
                }
            } else {
                const dated = vis.filter(v => v.t != null), und = vis.filter(v => v.t == null);
                const t0 = dated.length ? dated[0].t : 0, t1 = dated.length ? dated[dated.length - 1].t : DAY, w = Math.max(W0 - x0 - B.MR, n * B.CAL_PX);
                const sc = d3.scaleUtc().domain([t0 - 5 * DAY, t1 + 5 * DAY]).range([x0, x0 + w]), ux = new Map(und.map((v, k) => [v.id, x0 + w + 50 + k * step]));
                X = v => (v.t == null ? ux.get(v.id) : sc(v.t)); xEnd = und.length ? ux.get(und[und.length - 1].id) : x0 + w; hw = 5;
                for (const t of sc.ticks(Math.max(2, Math.round(w / 110)))) ticks.push({ x: sc(t), label: d3.utcFormat('%b %Y')(t) });
                if (und.length) ticks.push({ x: x0 + w + 50, label: 'undated' });
            }

            // lanes: the Timeline's, each split into its chains (a lane is reused once a chain ends)
            const nL = 1 + d3.max(vis, v => v.lane ?? 0);
            lanes = Array.from({ length: nL }, (_, k) => ({ k, vs: [], chains: [] }));
            for (const v of vis) {
                const ln = lanes[v.lane ?? 0], last = ln.vs[ln.vs.length - 1];
                if (!last || L.parentOf.get(v.id) !== last.id) ln.chains.push([]);
                ln.chains[ln.chains.length - 1].push(v);
                ln.vs.push(v);
            }
            for (const ln of lanes) ln.max = d3.max(ln.vs, tot) || 0;
            const ky = Math.max(B.KY_MIN, Math.min(B.KY_MAX, B.TARGET_H / Math.max(1, d3.sum(lanes, l => l.max))));
            let y = B.TOP;
            for (const ln of lanes) { ln.top = y; ln.bot = y + Math.max(12, ln.max * ky); y = ln.bot + B.LANE_GAP; }
            const laneOf = v => lanes[v.lane ?? 0];
            const sTop = y + 4, sBase = sTop + B.STRIP_H;

            // what each file holds, keyed by birth (by birth and lens group when coloured by lens), or with a selection:
            // '<' born before, its lens groups, '>' born after
            const lensMode = !fc && o.color === 'lens', SEP = '\t';
            const catBy = new Map(C.cats.map(x => [x.key, x])), catOf = gk => catBy.get(gk) || { key: gk, label: 'Other functions', color: 'var(--lin-grey-d)' };
            const mods = fc ? modulesOf(fc, C) : [];
            const keyOf = r => { const c = M.cohortOf(r); return !fc ? (lensMode ? `${c.key}${SEP}${C.of(r)}` : c.key) : c === fc ? `m:${C.of(r)}` : c.idx < fc.idx ? '<' : '>'; };
            const order = fc ? ['<', ...mods.map(m => `m:${m.key}`), '>'] : lensMode ? M.list.flatMap(c => modulesOf(c, C).map(m => `${c.key}${SEP}${m.key}`)) : M.list.map(c => c.key);
            const groupOf = k => (lensMode ? k.slice(k.indexOf(SEP) + 1) : fc && k.startsWith('m:') ? k.slice(2) : null);
            const birthOf = k => (lensMode ? M.byKey.get(k.slice(0, k.indexOf(SEP))) : !fc ? M.byKey.get(k) : null);
            const paint = k => { const gk = groupOf(k); return gk != null ? catOf(gk).color : !fc ? cohortColor(M.byKey.get(k)) : 'var(--lin-slate)'; };
            const counts = new Map(vis.map(v => { const c = {}; for (const r of M.heldM[v.idx]) { const k = keyOf(r); c[k] = (c[k] || 0) + 1; } return [v.id, c]; }));
            const titleOf = k => { const gk = groupOf(k); return gk != null ? catOf(gk).label : !fc ? M.byKey.get(k).long : k === '<' ? `Born before ${fc.label}` : `Born after ${fc.label}`; };
            // a tracked lens group (or a parent of groups) keeps its layers lit, the rest fade
            const tk = ctx.getTrack(), lit = new Set();
            if (tk && !tk.startsWith('fn:')) {
                for (const x of C.cats) if (x.tk === tk) lit.add(x.key);
                const mark = C.track ? C.track(tk) : null;
                if (mark) for (let r = 0; r < R.n; r++) if (mark[r]) lit.add(C.of(r));
            }
            const fade = k => { if (!lit.size) return 1; const gk = groupOf(k); return gk != null && lit.has(gk) ? 1 : 0.2; };

            const gAx = g.append('g').style('pointer-events', 'none');
            for (const k of ticks) {
                gAx.append('line').attr('x1', k.x).attr('x2', k.x).attr('y1', B.TOP - 16).attr('y2', sBase).style('stroke', 'var(--lin-line)').attr('vector-effect', 'non-scaling-stroke');
                gAx.append('text').attr('class', 'lin-halo').attr('x', k.x).attr('y', B.TOP - 22).attr('text-anchor', 'middle').attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.75).text(k.label);
            }
            gAx.append('text').attr('x', B.PAD).attr('y', B.TOP - 22).attr('font-size', 11).attr('font-weight', 600).style('fill', 'var(--text)').style('opacity', 0.75).text(o.axis === 'rank' ? 'Date rank' : 'Date');

            const area = ln => d3.area().curve(d3.curveBumpX).x(d => d.data.x).y0(d => ln.bot - d[0] * ky).y1(d => ln.bot - d[1] * ky);
            // hover lights one group (a birth when coloured by birth) in every lane and its reuse ribbons; bands: each layer's y at each file
            const hid = k => { const gk = groupOf(k); return gk != null ? `g:${gk}` : k; };
            const baseOp = k => (k === '>' ? 0.4 : 1) * fade(k), ribOp = k => 0.35 * fade(k), forkOp = k => 0.5 * fade(k);
            // the page's pane (or a hover elsewhere) lights every layer, fork and ribbon of the groups test(cat) accepts
            const gcat = k => { const gk = groupOf(k); return gk != null ? catOf(gk) : null; };
            paneHover = test => {
                const on = k => { const c = gcat(k); return !!c && test(c); };
                g.selectAll('path.bl-layer').attr('fill-opacity', s => (!test ? baseOp(s.key) : on(s.key) ? 1 : 0.15));
                g.selectAll('path.bl-fork').attr('fill-opacity', d => (!test ? forkOp(d.key) : on(d.key) ? 0.85 : 0.04));
                g.selectAll('path.bl-rib').attr('fill-opacity', d => (!test ? ribOp(d.key) : on(d.key) ? 0.8 : 0.04));
            };
            const hover = id => {
                g.selectAll('path.bl-layer').attr('fill-opacity', s => (!id ? baseOp(s.key) : hid(s.key) === id ? 1 : 0.15));
                g.selectAll('path.bl-fork').attr('fill-opacity', d => (!id ? forkOp(d.key) : hid(d.key) === id ? 0.85 : 0.04));
                g.selectAll('path.bl-rib').attr('fill-opacity', d => (!id ? ribOp(d.key) : hid(d.key) === id ? 0.8 : 0.04));
            };
            const band = new Map(), chainOf = new Map(), forks = [];
            let chainN = 0;
            for (const ln of lanes) {
                for (const ch of ln.chains) {
                    for (const v of ch) chainOf.set(v.idx, chainN);
                    chainN++;
                    // a chain that forks off another lane is fed by fork ribbons from its parent (drawn below)
                    const pts = [], par = L.fileOf.get(L.parentOf.get(ch[0].id));
                    if (par) forks.push({ par, v: ch[0] });
                    for (const v of ch) { const c = counts.get(v.id); pts.push({ x: X(v) - hw, c, v }, { x: X(v) + hw, c, v }); }
                    const keys = order.filter(k => ch.some(v => counts.get(v.id)[k]));
                    const S = d3.stack().keys(keys).value((d, k) => d.c[k] || 0)(pts);
                    for (const sr of S) sr.forEach((d, i) => { if (pts[i].v && d[1] > d[0]) band.set(`${pts[i].v.idx}\n${sr.key}`, [ln.bot - d[1] * ky, ln.bot - d[0] * ky]); });
                    g.append('g').selectAll('path').data(S).join('path').attr('class', 'bl-layer')
                        .attr('d', area(ln)).style('fill', s => paint(s.key)).attr('fill-opacity', s => baseOp(s.key))
                        .style('stroke', 'var(--lin-card)').attr('stroke-width', 0.6).attr('vector-effect', 'non-scaling-stroke').style('cursor', 'pointer')
                        .on('mousemove', (ev, s) => {
                            const [mx] = d3.pointer(ev, ctx.g);
                            let v = ch[0];
                            for (const w of ch) if (Math.abs(X(w) - mx) < Math.abs(X(v) - mx)) v = w;
                            const k = counts.get(v.id)[s.key] || 0, cat = gcat(s.key), tt = cat && ctx.tiers ? ctx.tiers.tip(cat) : null, dot = tt ? tt.badge : paint(s.key);
                            ctx.showTip(ev, titleOf(s.key), dot, [...(lensMode ? [['Born', birthOf(s.key).label]] : []), ['File', `${ctx.name(v.id)} · ${v.t == null ? 'undated' : iso(v.t)}`], ['Functions', `${fmt(k)} of ${fmt(tot(v))}`],
                                ['Click', !fc ? 'select this birth' : s.key.startsWith('m:') ? 'track this group' : 'show every birth']], (tt ? tt.html : '') + (cat && ctx.catMeta ? ctx.catMeta(cat) : ''));
                        })
                        .on('mouseenter', (ev, s) => { hover(hid(s.key)); if (ctx.onHover) ctx.onHover(gcat(s.key)); })
                        .on('mouseleave', () => { ctx.hideTip(); hover(null); if (ctx.onHover) ctx.onHover(null); })
                        .on('click', (ev, s) => {
                            ctx.hideTip();
                            if (!fc) return select(birthOf(s.key));
                            if (!s.key.startsWith('m:')) return select(null);
                            const gt = catOf(groupOf(s.key)).tk;
                            if (gt) ctx.setTrack(ctx.getTrack() === gt ? '' : gt);
                        });
                }
                const first = ln.vs[0];
                g.append('text').attr('class', 'lin-halo').attr('x', B.PAD).attr('y', ln.top + Math.min(14, (ln.bot - ln.top) / 2 + 4)).attr('font-size', 12).attr('font-weight', 600).style('fill', 'var(--text)').text(short(ctx.name(first.id)));
                g.append('text').attr('class', 'lin-halo').attr('x', B.PAD).attr('y', ln.top + Math.min(14, (ln.bot - ln.top) / 2 + 4) + 14).attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.7).text(`${ln.vs.length} file${ln.vs.length === 1 ? '' : 's'}${ln.chains.length > 1 ? `, ${ln.chains.length} chains` : ''}`);
            }

            // reuse across chains: a ribbon from the nearest earlier holder's band to the file that picks the code up
            // off its parent, one per (source, file, layer), stacked from the top of each band
            const visByIdx = new Map(vis.map(v => [v.idx, v])), ribs = [];
            const parentIdx = i => { const v = visByIdx.get(i), p = v && L.fileOf.get(L.parentOf.get(v.id)); return p ? p.idx : -1; };
            for (const x of reuse(M, parentIdx, i => chainOf.get(i))) {
                const by = new Map();
                for (const r of x.rows) by.set(keyOf(r), (by.get(keyOf(r)) || 0) + 1);
                for (const [key, n] of by) ribs.push({ src: visByIdx.get(x.src), dst: visByIdx.get(x.dst), key, n });
            }
            ribs.sort((a, b) => b.n - a.n);
            const offOut = new Map(), offIn = new Map(), shown = ribs.slice(0, B.RIB_MAX).filter(d => d.src && d.dst);
            // a ribbon from the source's band of a layer to the same layer's band in the target, stacked from the top of each band
            const flow = d => {
                const ka = `${d.src.idx}\n${d.key}`, kb = `${d.dst.idx}\n${d.key}`, a = band.get(ka), b = band.get(kb);
                if (!a || !b) return null;
                const oa = offOut.get(ka) || 0, ob = offIn.get(kb) || 0;
                const ha = Math.min(d.n * ky, a[1] - a[0] - oa), hb = Math.min(d.n * ky, b[1] - b[0] - ob);
                offOut.set(ka, oa + ha); offIn.set(kb, ob + hb);
                // the ends leave and enter flat even between neighbour columns, so a ribbon across lanes keeps its thickness
                const x0 = X(d.src) + hw, x1 = X(d.dst) - hw, y0 = a[0] + oa, y1 = b[0] + ob, c = Math.max((x1 - x0) / 2, 70), ea = y0 + Math.max(0.5, ha), eb = y1 + Math.max(0.5, hb);
                return `M${x0},${y0}C${x0 + c},${y0} ${x1 - c},${y1} ${x1},${y1}L${x1},${eb}C${x1 - c},${eb} ${x0 + c},${ea} ${x0},${ea}Z`;
            };
            // fork ribbons: what a fork's first file keeps from its parent, layer by layer (the code it inherits), always drawn
            const fr = [];
            for (const { par, v } of forks) {
                const by = new Map();
                for (const r of M.heldM[v.idx]) if (R.has(r, par.idx)) by.set(keyOf(r), (by.get(keyOf(r)) || 0) + 1);
                for (const k of order) if (by.has(k)) fr.push({ src: par, dst: v, key: k, n: by.get(k) });
            }
            for (const d of fr) d.path = flow(d);
            g.append('g').selectAll('path').data(fr.filter(d => d.path)).join('path').attr('class', 'bl-fork').attr('d', d => d.path)
                .style('fill', d => paint(d.key)).attr('fill-opacity', d => forkOp(d.key)).style('cursor', 'pointer')
                .on('mouseenter', (ev, d) => hover(hid(d.key)))
                .on('mousemove', (ev, d) => ctx.showTip(ev, `Inherited: ${titleOf(d.key)}`, paint(d.key), [...(lensMode ? [['Born', birthOf(d.key).label]] : []),
                    ['From parent', `${ctx.name(d.src.id)} \u00b7 ${d.src.t == null ? 'undated' : iso(d.src.t)}`], ['Into fork', `${ctx.name(d.dst.id)} \u00b7 ${d.dst.t == null ? 'undated' : iso(d.dst.t)}`], ['Functions', fmt(d.n)]]))
                .on('mouseleave', () => { ctx.hideTip(); hover(null); });
            for (const d of shown) d.path = flow(d);
            // the lineage's Reuse layer switch shows or hides this group (applyReuse)
            const gR = g.append('g').attr('class', 'bl-ribs').style('display', ctx.reuseOn && !ctx.reuseOn() ? 'none' : null);
            gR.selectAll('path').data(shown.filter(d => d.path)).join('path').attr('class', 'bl-rib').attr('d', d => d.path)
                .style('fill', d => paint(d.key)).attr('fill-opacity', d => ribOp(d.key)).style('stroke', 'var(--lin-reuse)').attr('stroke-opacity', 0.5).attr('stroke-dasharray', '3 2').attr('stroke-width', 0.8).attr('vector-effect', 'non-scaling-stroke').style('cursor', 'pointer')
                .on('mouseenter', (ev, d) => hover(hid(d.key)))
                .on('mousemove', (ev, d) => ctx.showTip(ev, `Reused: ${titleOf(d.key)}`, 'var(--lin-reuse)', [...(lensMode ? [['Born', birthOf(d.key).label]] : []),
                    ['From', `${ctx.name(d.src.id)} \u00b7 ${d.src.t == null ? 'undated' : iso(d.src.t)}`], ['To', `${ctx.name(d.dst.id)} \u00b7 ${d.dst.t == null ? 'undated' : iso(d.dst.t)}`], ['Functions', fmt(d.n)], ['Note', 'not in its parent: reuse, independent birth or matcher miss']]))
                .on('mouseleave', () => { ctx.hideTip(); hover(null); });
            if (ribs.length > shown.length) gR.append('text').attr('x', B.PAD).attr('y', sBase + 24).attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.7).text(`${fmt(ribs.length - shown.length)} smaller reuse ribbons not drawn`);

            // file dots under each lane; a click selects what was born in it
            dotPos.clear();
            for (const v of vis) {
                dotPos.set(v.idx, [X(v), laneOf(v).bot + 7]);
                const ln = laneOf(v), isBirth = !!fc && fc.nodes.includes(v.idx), par = L.fileOf.get(L.parentOf.get(v.id));
                g.append('circle').attr('cx', X(v)).attr('cy', ln.bot + 7).attr('r', isBirth ? 4.5 : 3).style('fill', isBirth ? 'var(--lin-born)' : 'var(--lin-grey)').style('cursor', 'pointer')
                    .on('mousemove', ev => ctx.showTip(ev, ctx.name(v.id), 'var(--lin-grey)', [['Date', v.t == null ? 'undated' : iso(v.t)], ['Parent', par ? ctx.name(par.id) : 'none (a root)'], ['Functions', fmt(tot(v))], ['Born here', fmt(M.bornIn[v.idx].length)],
                        ...(v.t_swapped ? [['⇄ Swapped', `date of ${ctx.name(v.t_swapped)}, in this view only`]] : []), ...(v.time_inversion ? [['⚠ Date', 'a later file is closer than its parent: this date, and its births, may be late (the ⚠ ⇄ button under the dot swaps it with the closer file)']] : []), ['Click', 'select what was born here']]))
                    .on('mouseleave', ctx.hideTip)
                    .on('click', () => { ctx.hideTip(); const c = M.byKey.get(M.keyOf[v.idx]); if (c && c.rows.length) select(c); });
            }

            // the selection: "+N" where it is born, "+N from X" where it shows up off its parent (reuse, independent birth or matcher miss)
            if (fc) {
                const inFc = new Uint8Array(R.n);
                for (const r of fc.rows) inFc[r] = 1;
                const mark = (v, txt, col) => {
                    const top = laneOf(v).bot - tot(v) * ky, x = X(v);
                    g.append('path').attr('d', `M${x - 4},${top - 9}L${x + 4},${top - 9}L${x},${top - 3}Z`).style('fill', col);
                    g.append('text').attr('class', 'lin-halo').attr('x', x).attr('y', top - 13).attr('text-anchor', 'middle').attr('font-size', 11.5).attr('font-weight', 650).style('fill', col).text(txt);
                };
                for (const v of vis) {
                    if (fc.nodes.includes(v.idx)) { mark(v, `+${fmt(M.bornIn[v.idx].length)}`, 'var(--lin-born)'); continue; }
                    const par = L.fileOf.get(L.parentOf.get(v.id));
                    let k = 0, from = -1;
                    for (const r of M.heldM[v.idx]) if (inFc[r] && !(par && R.has(r, par.idx))) { k++; from = M.birth[r]; }
                    if (k) mark(v, `+${fmt(k)} from ${short(ctx.name(ctx.getDoc().nodes[from].id), 16)}`, 'var(--lin-reuse)');
                }
            }

            // the birth strip: rows born in each file; the biggest is cut so it does not flatten the rest
            const born = vis.map(v => M.bornIn[v.idx].length), srt = [...born].sort((a, b) => b - a);
            const cap = Math.max(5, (srt[1] || srt[0] || 1) * 1.15), hOf = v => (Math.min(v, cap) / cap) * B.STRIP_H;
            const bw = o.axis === 'rank' ? Math.min(12, Math.max(3, step * 0.5)) : 5;
            g.append('text').attr('x', B.PAD).attr('y', sTop + 12).attr('font-size', 12).attr('font-weight', 600).style('fill', 'var(--text)').text('Births');
            g.append('text').attr('x', B.PAD).attr('y', sTop + 26).attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.7).text('functions first seen');
            g.append('text').attr('x', B.PAD).attr('y', sTop + 39).attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.7).text('in each file');
            if (fc) {
                const xs = fc.nodes.map(i => X(L.fileOf.get(doc.nodes[i].id)));
                g.append('rect').attr('x', Math.min(...xs) - bw).attr('y', sTop - 8).attr('width', Math.max(...xs) - Math.min(...xs) + 2 * bw).attr('height', B.STRIP_H + 16).attr('rx', 4)
                    .attr('fill', 'none').style('stroke', 'var(--lin-born)').attr('stroke-dasharray', '3 3').attr('vector-effect', 'non-scaling-stroke');
            }
            vis.forEach((v, i) => {
                const k = born[i], x = X(v), inF = !!fc && M.keyOf[v.idx] === fc.key, c = M.byKey.get(M.keyOf[v.idx]);
                const bg = g.append('g').style('cursor', 'pointer')
                    .on('mousemove', ev => {
                        const top = modulesOf({ rows: M.bornIn[v.idx] }, C).slice(0, 4).map(m => `${m.cat.label} ${fmt(m.n)}`).join(', ');
                        ctx.showTip(ev, `Born in ${ctx.name(v.id)}`, 'var(--lin-born)', [['Date', v.t == null ? 'undated' : iso(v.t)], ['Functions', fmt(k)], ...(top ? [['Mostly', top]] : []), ['Click', 'select this birth']]);
                    })
                    .on('mouseleave', ctx.hideTip)
                    .on('click', () => { ctx.hideTip(); if (k) select(c); });
                bg.append('rect').attr('x', x - bw / 2 - 3).attr('y', sTop).attr('width', bw + 6).attr('height', B.STRIP_H).attr('fill', 'transparent');
                if (!k) return;
                if (inF || lensMode) {
                    let yy = sBase;
                    const sc = Math.min(1, cap / k);
                    for (const m of modulesOf({ rows: M.bornIn[v.idx] }, C)) {
                        const h = hOf(m.n) * sc;
                        bg.append('rect').attr('x', x - bw / 2).attr('y', yy - h).attr('width', bw).attr('height', h).style('fill', m.cat.color);
                        yy -= h;
                    }
                } else bg.append('rect').attr('x', x - bw / 2).attr('y', sBase - hOf(k)).attr('width', bw).attr('height', hOf(k)).style('fill', fc ? 'var(--lin-slate)' : cohortColor(c)).attr('fill-opacity', fc ? 0.6 : 1);
                if (k > cap) for (const dy of [9, 13]) bg.append('line').attr('x1', x - bw / 2 - 2).attr('x2', x + bw / 2 + 2).attr('y1', sBase - B.STRIP_H + dy).attr('y2', sBase - B.STRIP_H + dy - 3).style('stroke', 'var(--lin-card)').attr('stroke-width', 2);
                if (inF || (k >= 5 && k >= cap * 0.25)) bg.append('text').attr('class', 'lin-halo').attr('x', x).attr('y', sBase - hOf(k) - 4).attr('text-anchor', 'middle').attr('font-size', 11).attr('font-weight', inF ? 650 : 400).style('fill', 'var(--text)').text(fmt(k));
            });
            g.append('line').attr('x1', x0 - 8).attr('x2', xEnd + 8).attr('y1', sBase).attr('y2', sBase).style('stroke', 'var(--lin-grey)').attr('vector-effect', 'non-scaling-stroke');
            // the timeline-break buttons last, over every ribbon and mark
            const gBtn = g.append('g').node();
            if (ctx.swapBtn) for (const v of vis) ctx.swapBtn(gBtn, v, X(v) - 17, laneOf(v).bot + 12);
            if (ctx.swapArrows) ctx.swapArrows(gBtn, id => { const v = vis.find(x => x.id === id); return v ? [X(v), laneOf(v).bot + 4] : null; });
            frame = { x0: 0, y0: 0, x1: xEnd + B.MR, y1: sBase + 30, vis, X };
            syncControls();
        }

        return {
            draw,
            box: () => frame,
            dotAt: i => dotPos.get(i) || null,
            hoverTrack(test) { if (paneHover) paneHover(test); },
            // the page's track changed: the tracked group's layers stay lit (drawn again, the fade is computed in draw)
            setTrack() { if (M) draw(); },
            // for the page's state bar: the selected birth, the colouring, and closing either
            state: () => ({ sel: current() ? current().label : '', color: o.color }),
            clear(k) { if (k === 'color') { o.color = 'lens'; emit(); draw(); } else select(null); },
            destroy() { ctx.controls.replaceChildren(); g.selectAll('*').remove(); },
        };
    }

    window.NwayBirths = { model, share, reuse, mount, bucketOf };
})();
