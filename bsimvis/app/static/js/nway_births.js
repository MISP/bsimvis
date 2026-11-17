/**
 * Births view of the lineage page (lview=births): what code first appears when, and how long it lasts.
 *
 * Needs the time-ordered doc (order=time: nodes carry t and rank) and its row list. A row is born in its
 * holder with the lowest date rank (undated nodes rank last, so they give birth only to rows no dated
 * node holds); rows failing the page filters (rows.match) are left out. A birth (cohort) is the rows
 * born in one file, week or month. Each Timeline lane (layout()'s n.lane, a chain of files sharing code)
 * is drawn as a stream: its height is the file's matching functions, its layers are the cohorts those
 * functions were born in, oldest at the bottom. A strip under the lanes counts the rows born in each
 * file. Selecting a cohort greys the others (born before / after) and splits it by the lens's groups;
 * the side pane says where it was born, how much of it later files keep, per lane, and its functions.
 *
 * NwayBirths.model(doc, R, bucket) and share(M, c) are DOM-free (checked in scripts/test_nway_lineage.js).
 * NwayBirths.mount(ctx) -> { draw(), panel(), box(), destroy() }; ctx is what nway_lineage.js hands over
 * (g, side, controls, getDoc, getR, getL, getCats, getTrack, setTrack, showTip, hideTip, rowEl, el,
 * segSwitch, fmt, name, width, fit, opts {bucket, axis, sel}, onOpt). Every sample value is set with
 * textContent or d3 .text(), never innerHTML.
 */
(function () {
    const DAY = 864e5;
    const MON = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' ');
    const MONL = 'January February March April May June July August September October November December'.split(' ');
    const iso = t => new Date(t).toISOString().slice(0, 10);
    const pct = v => `${Math.round(v * 100)}%`;
    const short = (s, n = 22) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);
    // ponytail: fixed geometry; TARGET_H is the lanes' height at k=1 before the KY clamps
    const B = { TOP: 58, PAD: 16, LABEL_W: 170, MR: 70, STEP_MIN: 26, CAL_PX: 18, LANE_GAP: 30, TARGET_H: 640, KY_MAX: 1.2, KY_MIN: 0.03, STRIP_H: 70, ROWS: 30, SURV_LANES: 6 };
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
    // the matching rows it holds (heldM) and the rows born in it (bornIn)
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
        return { doc, R, bucket, pos, birth, keyOf, byKey, list, heldM, bornIn, cohortOf: r => byKey.get(keyOf[birth[r]]) };
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
        const o = { bucket: BUCKETS.includes(ctx.opts.bucket) ? ctx.opts.bucket : 'month', axis: ctx.opts.axis === 'cal' ? 'cal' : 'rank', sel: ctx.opts.sel || '' };
        // ctx.tiers: the Lanes lens tiers (TIERS, KEYS, rank(cat), fill(defs, cache, color, tier), css(color, tier), tip(cat))
        const T = ctx.tiers;
        let M = null, frame = null, lanes = [], rareOpen = false;
        const emit = () => ctx.onOpt({ ...o });
        const getModel = () => {
            const doc = ctx.getDoc(), R = ctx.getR();
            if (!M || M.doc !== doc || M.R !== R || M.bucket !== o.bucket) M = model(doc, R, o.bucket);
            return M;
        };
        const current = () => { const c = M && M.byKey.get(o.sel); return c && c.rows.length ? c : null; };
        const select = c => { o.sel = c ? c.key : ''; emit(); draw(); };
        const cohortColor = c => (c.key === 'undated' ? 'var(--lin-grey-d)' : `color-mix(in oklab, var(--lin-born) ${Math.round(12 + 88 * c.idx / Math.max(1, M.list.length - 1))}%, var(--lin-slate))`);
        // the lens's groups of a cohort: Core, Common, Rare (a tiered lens), biggest first inside a tier
        const modulesOf = (c, C) => {
            const n = new Map();
            for (const r of c.rows) n.set(C.of(r), (n.get(C.of(r)) || 0) + 1);
            const cat = new Map(C.cats.map(x => [x.key, x]));
            return [...n].map(([key, k]) => ({ key, n: k, cat: cat.get(key) || { key, label: 'Other functions', color: 'var(--lin-grey-d)' } })).sort((a, b) => T.rank(a.cat) - T.rank(b.cat) || b.n - a.n);
        };

        // ---- toolbar: grouping, axis, the selected birth with a stepper ----
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
        ctx.controls.append(byB.el, axB.el, stepper);
        const syncControls = () => {
            byB.paint(); axB.paint();
            pick.replaceChildren(el('option', { value: '' }, 'No birth selected'));
            for (const c of M.list) pick.append(el('option', { value: c.key }, `${short(c.label, 28)} · ${fmt(c.rows.length)}`));
            const c = current();
            pick.value = c ? c.key : '';
            prevB.disabled = !!c && c.idx === 0;
            nextB.disabled = !!c && c.idx === M.list.length - 1;
        };

        function draw() {
            g.selectAll('*').remove();
            const defs = g.append('defs'), zc = new Map(), fillOf = cat => T.fill(defs, zc, cat.color, cat.tier);
            const doc = ctx.getDoc(), R = ctx.getR(), L = ctx.getL();
            if (!doc || !L) return;
            if (!R || doc.order !== 'time') {
                M = null;
                g.append('text').attr('x', 20).attr('y', 40).style('fill', 'var(--text)').text('Births needs the dated lineage (order=time) and its row list.');
                frame = { x0: 0, y0: 0, x1: 520, y1: 80 };
                panel();
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

            // what each file holds, keyed by cohort, or with a selection: '<' born before, its lens groups, '>' born after
            // more than one Rare lane: one folded Rare layer until it is opened (a click on it, or the pane)
            const all = fc ? modulesOf(fc, C) : [], rares = all.filter(m => m.cat.tier === 'rare'), fold = !rareOpen && rares.length > 1;
            const folded = new Set(fold ? rares.map(m => m.key) : []), fk = k => (folded.has(k) ? '^rare' : k);
            const mods = fold ? [...all.filter(m => !folded.has(m.key)), { key: '^rare', n: d3.sum(rares, m => m.n), cat: { key: '^rare', label: `Rare lanes (${rares.length})`, color: T.TIERS.rare[2], tier: 'rare', folded: true } }].sort((a, b) => T.rank(a.cat) - T.rank(b.cat) || b.n - a.n) : all;
            const modBy = new Map(mods.map(m => [m.key, m]));
            const keyOf = r => { const c = M.cohortOf(r); return !fc ? c.key : c === fc ? `m:${fk(C.of(r))}` : c.idx < fc.idx ? '<' : '>'; };
            const order = fc ? ['<', ...mods.map(m => `m:${m.key}`), '>'] : M.list.map(c => c.key);
            const paint = k => (!fc ? cohortColor(M.byKey.get(k)) : k === '<' || k === '>' ? 'var(--lin-slate)' : fillOf(modBy.get(k.slice(2)).cat));
            const catOf = k => (fc && k.startsWith('m:') ? modBy.get(k.slice(2)).cat : null);
            const counts = new Map(vis.map(v => { const c = {}; for (const r of M.heldM[v.idx]) { const k = keyOf(r); c[k] = (c[k] || 0) + 1; } return [v.id, c]; }));
            const titleOf = k => (!fc ? M.byKey.get(k).long : k === '<' ? `Born before ${fc.label}` : k === '>' ? `Born after ${fc.label}` : modBy.get(k.slice(2)).cat.label);

            const gAx = g.append('g').style('pointer-events', 'none');
            for (const k of ticks) {
                gAx.append('line').attr('x1', k.x).attr('x2', k.x).attr('y1', B.TOP - 16).attr('y2', sBase).style('stroke', 'var(--lin-line)').attr('vector-effect', 'non-scaling-stroke');
                gAx.append('text').attr('class', 'lin-halo').attr('x', k.x).attr('y', B.TOP - 22).attr('text-anchor', 'middle').attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.75).text(k.label);
            }
            gAx.append('text').attr('x', B.PAD).attr('y', B.TOP - 22).attr('font-size', 11).attr('font-weight', 600).style('fill', 'var(--text)').style('opacity', 0.75).text(o.axis === 'rank' ? 'Date rank' : 'Date');

            const area = ln => d3.area().curve(d3.curveBumpX).x(d => d.data.x).y0(d => ln.bot - d[0] * ky).y1(d => ln.bot - d[1] * ky);
            for (const ln of lanes) {
                for (const ch of ln.chains) {
                    // a chain grows out of its parent's date, from a dashed link to the parent's lane
                    const pts = [], par = L.fileOf.get(L.parentOf.get(ch[0].id));
                    if (par) {
                        pts.push({ x: X(par) + hw, c: {} });
                        const pl = laneOf(par);
                        g.append('line').attr('x1', X(par)).attr('x2', X(par)).attr('y1', pl.bot + 12).attr('y2', ln.bot).style('stroke', 'var(--lin-grey)').attr('stroke-dasharray', '3 4').attr('vector-effect', 'non-scaling-stroke');
                    }
                    for (const v of ch) { const c = counts.get(v.id); pts.push({ x: X(v) - hw, c, v }, { x: X(v) + hw, c, v }); }
                    const keys = order.filter(k => ch.some(v => counts.get(v.id)[k]));
                    const series = d3.stack().keys(keys).value((d, k) => d.c[k] || 0)(pts);
                    g.append('g').selectAll('path').data(series).join('path')
                        .attr('d', area(ln)).style('fill', s => paint(s.key)).attr('fill-opacity', s => (s.key === '>' ? 0.4 : 1))
                        .style('stroke', 'var(--lin-card)').attr('stroke-width', 0.6).attr('vector-effect', 'non-scaling-stroke').style('cursor', 'pointer')
                        .on('mousemove', (ev, s) => {
                            const [mx] = d3.pointer(ev, ctx.g);
                            let v = ch[0];
                            for (const w of ch) if (Math.abs(X(w) - mx) < Math.abs(X(v) - mx)) v = w;
                            const k = counts.get(v.id)[s.key] || 0, cat = catOf(s.key), tt = cat && !cat.folded ? T.tip(cat) : null;
                            // a lane says its tier (badge, its files, the cuts); the folded Rare layer its tier only
                            const dot = tt ? tt.badge : cat && cat.tier ? { glyph: T.TIERS[cat.tier][0], tag: T.TIERS[cat.tier][1], color: T.TIERS[cat.tier][2] } : cat ? cat.color : paint(s.key);
                            ctx.showTip(ev, titleOf(s.key), dot, [['File', `${ctx.name(v.id)} · ${v.t == null ? 'undated' : iso(v.t)}`], ['Functions', `${fmt(k)} of ${fmt(tot(v))}`],
                                ['Click', !fc ? 'select this birth' : cat && cat.folded ? 'open the Rare lanes' : s.key.startsWith('m:') ? 'track this group' : 'show every birth']], (tt ? tt.html : '') + (cat && !cat.folded ? ctx.catMeta(cat) : ''));
                        })
                        .on('mouseleave', ctx.hideTip)
                        .on('click', (ev, s) => {
                            ctx.hideTip();
                            if (!fc) return select(M.byKey.get(s.key));
                            if (!s.key.startsWith('m:')) return select(null);
                            if (s.key === 'm:^rare') { rareOpen = true; return draw(); }
                            const tk = modBy.get(s.key.slice(2)).cat.tk;
                            if (tk) ctx.setTrack(ctx.getTrack() === tk ? '' : tk);
                        });
                    // a line over each tier's band (Core at the bottom), its name at the chain's end when the band is tall enough
                    if (fc) for (const t of T.KEYS) {
                        const mine = series.filter(x => (catOf(x.key) || {}).tier === t);
                        if (!mine.length) continue;
                        const lo = mine[0], hi = mine[mine.length - 1], e = pts.length - 1;
                        g.append('path').attr('d', d3.line().curve(d3.curveBumpX).x(d => d.data.x).y(d => ln.bot - d[1] * ky)(hi)).attr('fill', 'none').style('stroke', 'var(--text)')
                            .attr('stroke-opacity', 0.65).attr('stroke-width', 1.2).attr('stroke-dasharray', t === 'rare' ? '4 3' : null).attr('vector-effect', 'non-scaling-stroke').style('pointer-events', 'none');
                        if ((hi[e][1] - lo[e][0]) * ky >= 9) g.append('text').attr('class', 'lin-halo').attr('x', pts[e].x + 6).attr('y', ln.bot - ((lo[e][0] + hi[e][1]) / 2) * ky + 4)
                            .attr('font-size', 11).attr('font-weight', 600).style('fill', T.TIERS[t][2]).style('pointer-events', 'none').text(`${T.TIERS[t][0]} ${T.TIERS[t][1]}`);
                    }
                }
                const first = ln.vs[0];
                g.append('text').attr('class', 'lin-halo').attr('x', B.PAD).attr('y', ln.top + Math.min(14, (ln.bot - ln.top) / 2 + 4)).attr('font-size', 12).attr('font-weight', 600).style('fill', 'var(--text)').text(short(ctx.name(first.id)));
                g.append('text').attr('class', 'lin-halo').attr('x', B.PAD).attr('y', ln.top + Math.min(14, (ln.bot - ln.top) / 2 + 4) + 14).attr('font-size', 11).style('fill', 'var(--text)').style('opacity', 0.7).text(`${ln.vs.length} file${ln.vs.length === 1 ? '' : 's'}${ln.chains.length > 1 ? `, ${ln.chains.length} chains` : ''}`);
            }

            // tier rail: beside each file, its functions by tier (the Functions dropdown's Core / Common / Rare), whatever the layers are
            if (R.tier) for (const v of vis) {
                const c = { core: 0, common: 0, rare: 0 };
                for (const r of M.heldM[v.idx]) c[R.tier[r] || 'rare']++;
                let y = laneOf(v).bot;
                for (const t of T.KEYS) {
                    const h = c[t] * ky;
                    if (h >= 0.5) g.append('rect').attr('x', X(v) - hw - 6).attr('y', y - h).attr('width', 3.5).attr('height', Math.max(0.5, h - 0.5)).style('fill', T.TIERS[t][2]).append('title').text(`${T.TIERS[t][1]}: ${fmt(c[t])} functions`);
                    y -= h;
                }
            }

            // file dots under each lane; a click selects what was born in it
            for (const v of vis) {
                const ln = laneOf(v), isBirth = !!fc && fc.nodes.includes(v.idx), par = L.fileOf.get(L.parentOf.get(v.id));
                g.append('circle').attr('cx', X(v)).attr('cy', ln.bot + 7).attr('r', isBirth ? 4.5 : 3).style('fill', isBirth ? 'var(--lin-born)' : 'var(--lin-grey)').style('cursor', 'pointer')
                    .on('mousemove', ev => ctx.showTip(ev, ctx.name(v.id), 'var(--lin-grey)', [['Date', v.t == null ? 'undated' : iso(v.t)], ['Parent', par ? ctx.name(par.id) : 'none (a root)'], ['Functions', fmt(tot(v))], ['Born here', fmt(M.bornIn[v.idx].length)],
                        ...(v.time_inversion ? [['⚠ Date', 'a later file is closer than its parent: this date, and its births, may be late']] : []), ['Click', 'select what was born here']], ctx.meta([v.idx], 'this node')))
                    .on('mouseleave', ctx.hideTip)
                    .on('click', () => { ctx.hideTip(); const c = M.byKey.get(M.keyOf[v.idx]); if (c && c.rows.length) select(c); });
                if (v.time_inversion) g.append('text').attr('x', X(v) + 6).attr('y', ln.bot + 11).attr('font-size', 11).style('fill', 'var(--lin-died)').text('⚠');
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
                if (inF) {
                    let yy = sBase;
                    const sc = Math.min(1, cap / k);
                    for (const m of modulesOf({ rows: M.bornIn[v.idx] }, C)) {
                        const h = hOf(m.n) * sc;
                        bg.append('rect').attr('x', x - bw / 2).attr('y', yy - h).attr('width', bw).attr('height', h).style('fill', fillOf(m.cat));
                        yy -= h;
                    }
                } else bg.append('rect').attr('x', x - bw / 2).attr('y', sBase - hOf(k)).attr('width', bw).attr('height', hOf(k)).style('fill', fc ? 'var(--lin-slate)' : cohortColor(c)).attr('fill-opacity', fc ? 0.6 : 1);
                if (k > cap) for (const dy of [9, 13]) bg.append('line').attr('x1', x - bw / 2 - 2).attr('x2', x + bw / 2 + 2).attr('y1', sBase - B.STRIP_H + dy).attr('y2', sBase - B.STRIP_H + dy - 3).style('stroke', 'var(--lin-card)').attr('stroke-width', 2);
                if (inF || (k >= 5 && k >= cap * 0.25)) bg.append('text').attr('class', 'lin-halo').attr('x', x).attr('y', sBase - hOf(k) - 4).attr('text-anchor', 'middle').attr('font-size', 11).attr('font-weight', inF ? 650 : 400).style('fill', 'var(--text)').text(fmt(k));
            });
            g.append('line').attr('x1', x0 - 8).attr('x2', xEnd + 8).attr('y1', sBase).attr('y2', sBase).style('stroke', 'var(--lin-grey)').attr('vector-effect', 'non-scaling-stroke');
            frame = { x0: 0, y0: 0, x1: xEnd + B.MR, y1: sBase + 30, vis, X };
            syncControls();
            panel();
        }

        // ---- side pane: the biggest births, or the selected one ----
        function panel() {
            const side = ctx.side;
            side.replaceChildren();
            if (!M) { side.append(el('div', { class: 'lin-dim' }, 'Births needs dated files and the row list.')); return; }
            const C = ctx.getCats(), fc = current(), doc = M.doc, R = M.R;
            const head = (txt, color) => { const h = el('div', { class: 'lin-gh' }), sw = el('span', { class: 'lin-sw' }); sw.style.background = color; h.append(sw, el('span', {}, txt)); return h; };
            if (!fc) {
                side.append(el('div', { class: 'lin-gh' }, 'Biggest births'), el('div', { class: 'lin-dim', style: 'margin-bottom:8px' }, 'Pick one to see what it was and where it went, or click a layer, a file dot or a bar.'));
                for (const c of [...M.list].sort((a, b) => b.rows.length - a.rows.length).slice(0, 10)) {
                    const b = el('button', { type: 'button', class: 'lin-gi' }), t = el('span', { class: 't' }), sw = el('span', { class: 'lin-sw' });
                    sw.style.background = cohortColor(c);
                    t.append(sw, el('b', {}, c.label), el('span', { class: 'lin-dim' }, fmt(c.rows.length)));
                    b.append(t, el('span', { class: 'lin-dim' }, modulesOf(c, C).slice(0, 3).map(m => m.cat.label).join(', ')));
                    b.addEventListener('click', () => select(c));
                    side.append(b);
                }
                return;
            }
            const sh = share(M, fc), vis = frame.vis, birthNodes = new Set(fc.nodes), first = M.pos[fc.nodes[0]];
            const later = vis.filter(v => M.pos[v.idx] > first && !birthNodes.has(v.idx)), dated = vis.filter(v => v.t != null);
            const newest = dated[dated.length - 1], seen = dated.filter(v => sh[v.idx] > 0);
            const reached = lanes.filter(ln => ln.vs.some(v => sh[v.idx] > 0));
            const card = el('section', { class: 'lin-card', 'aria-label': 'Selected birth' });
            card.append(head(fc.long, 'var(--lin-born)'));
            const where = fc.nodes.slice(0, 4).map(i => { const nd = doc.nodes[i]; return `${ctx.name(nd.id)} (${nd.t == null ? 'undated' : iso(nd.t)}, +${fmt(M.bornIn[i].length)})`; }).join(', ');
            card.append(el('div', { class: 'lin-dim' }, `${fmt(fc.rows.length)} functions first seen in ${fc.nodes.length === 1 ? 'one file' : `${fc.nodes.length} files`}: ${where}${fc.nodes.length > 4 ? ` +${fc.nodes.length - 4} more` : ''}.`));
            if (fc.nodes.some(i => doc.nodes[i].time_inversion)) card.append(el('div', { style: 'color:var(--lin-died)' }, '⚠ A birth file has a likely late date: some of this code may be older.'));
            const kv = el('div', { class: 'lin-kv' });
            for (const [k, v] of [['Born', fmt(fc.rows.length)], ['Later files holding it', `${fmt(later.filter(v => sh[v.idx] > 0).length)} of ${fmt(later.length)}`],
                ...(newest ? [['In the newest file', `${pct(sh[newest.idx])} (${short(ctx.name(newest.id), 18)})`]] : []), ...(seen.length ? [['Last seen', iso(seen[seen.length - 1].t)]] : []), ['Lanes reached', `${reached.length} of ${lanes.length}`]]) kv.append(el('span', { class: 'k' }, k), el('span', {}, v));
            card.append(kv);

            // what was born: the lens's groups; a click tracks one (the Tree and the table see it too)
            card.append(el('div', { class: 'lin-dim', style: 'margin-top:6px' }, 'What was born'));
            const chips = el('div', { class: 'lin-names' });
            for (const m of modulesOf(fc, C)) {
                const on = !!m.cat.tk && ctx.getTrack() === m.cat.tk;
                const b = el('button', { type: 'button', class: `lin-chip${on ? ' on' : ''}`, 'aria-pressed': String(on), title: m.cat.tk ? 'Track this group' : '' }), sw = el('span', { class: 'lin-sw' });
                sw.style.background = T.css(m.cat.color, m.cat.tier);
                b.append(sw, ...(m.cat.tier ? [el('span', { title: T.TIERS[m.cat.tier][1] }, T.TIERS[m.cat.tier][0])] : []), el('span', { class: 'lin-gn' }, m.cat.label), el('span', { class: 'lin-dim' }, fmt(m.n)));
                if (m.cat.tk) b.addEventListener('click', () => ctx.setTrack(on ? '' : m.cat.tk));
                chips.append(b);
            }
            card.append(chips);
            const nRare = modulesOf(fc, C).filter(m => m.cat.tier === 'rare').length;
            if (nRare > 1) {
                const fb = el('button', { type: 'button', class: 'lin-btn', style: 'margin-top:4px' }, rareOpen ? `Fold the ${nRare} Rare lanes into one layer` : `Open the ${nRare} Rare lanes`);
                fb.addEventListener('click', () => { rareOpen = !rareOpen; draw(); });
                card.append(fb);
            }

            // where it went: the share each lane's files keep, over time
            card.append(el('div', { class: 'lin-dim', style: 'margin-top:6px' }, 'Where it went: share held, per lane'));
            const t0 = Math.min(...fc.nodes.map(i => doc.nodes[i].t ?? Infinity));
            const top = reached.map(ln => ({ ln, pts: ln.vs.filter(v => v.t != null && v.t >= t0).map(v => ({ v, s: sh[v.idx] })) })).filter(x => x.pts.length)
                .sort((a, b) => d3.max(b.pts, p => p.s) - d3.max(a.pts, p => p.s)).slice(0, B.SURV_LANES);
            if (top.length && Number.isFinite(t0) && newest) {
                const W = Math.max(220, side.clientWidth - 40), H = 110, ml = 34, mr = 8, mt = 8, mb = 20;
                const x = d3.scaleUtc().domain([t0, Math.max(t0 + DAY, newest.t)]).range([ml, W - mr]), yy = d3.scaleLinear().domain([0, 1]).range([H - mb, mt]);
                const svg = d3.create('svg').attr('width', W).attr('height', H).attr('role', 'img').attr('aria-label', 'Share of this birth each lane keeps over time');
                for (const v of [0, 0.5, 1]) {
                    svg.append('line').attr('x1', ml).attr('x2', W - mr).attr('y1', yy(v)).attr('y2', yy(v)).style('stroke', 'var(--lin-line)');
                    svg.append('text').attr('x', ml - 4).attr('y', yy(v) + 4).attr('text-anchor', 'end').attr('font-size', 10.5).style('fill', 'var(--text)').style('opacity', 0.7).text(pct(v));
                }
                for (const t of x.ticks(3)) svg.append('text').attr('x', x(t)).attr('y', H - 5).attr('text-anchor', 'middle').attr('font-size', 10.5).style('fill', 'var(--text)').style('opacity', 0.7).text(d3.utcFormat('%b %Y')(t));
                for (const { pts } of top) {
                    const home = pts.some(p => birthNodes.has(p.v.idx));
                    svg.append('path').attr('d', d3.line().curve(d3.curveStepAfter).x(p => x(p.v.t)).y(p => yy(p.s))(pts)).attr('fill', 'none').style('stroke', 'var(--text)').attr('stroke-width', home ? 1.8 : 1.2).attr('stroke-opacity', home ? 0.9 : 0.45);
                    for (const p of pts) svg.append('circle').attr('cx', x(p.v.t)).attr('cy', yy(p.s)).attr('r', 2.2).style('fill', birthNodes.has(p.v.idx) ? 'var(--lin-born)' : 'var(--text)').attr('fill-opacity', home ? 0.9 : 0.5);
                }
                card.append(svg.node());
                const lk = el('div', { class: 'lin-kv' });
                for (const { ln, pts } of top) { const e = pts[pts.length - 1]; lk.append(el('span', { class: 'k' }, short(ctx.name(ln.vs[0].id), 20)), el('span', {}, `${pct(e.s)} in ${short(ctx.name(e.v.id), 16)}`)); }
                if (reached.length > top.length) lk.append(el('span', { class: 'k' }, `+${reached.length - top.length} more lanes`), el('span', {}, ''));
                card.append(lk);
            }

            // its functions, most widely held first; a click tracks one
            const rows = [...fc.rows].sort((a, b) => R.pop[b] - R.pop[a]);
            card.append(el('div', { class: 'lin-dim', style: 'margin-top:6px' }, 'Functions, most widely held first'));
            for (const r of rows.slice(0, B.ROWS)) card.append(ctx.rowEl(r));
            if (rows.length > B.ROWS) card.append(el('div', { class: 'lin-dim' }, `+${fmt(rows.length - B.ROWS)} more`));
            side.append(card);
        }

        return {
            draw,
            panel,
            box: () => frame,
            destroy() { ctx.controls.replaceChildren(); g.selectAll('*').remove(); },
        };
    }

    window.NwayBirths = { model, share, mount, bucketOf };
})();
