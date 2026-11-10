/**
 * Ring view of the lineage page (lview=ring): clusters of files as circles, the modules they hold as a
 * ring of segments around each circle, the modules shared between clusters as ribbons.
 *
 * NwayRing.mount(ctx) -> { draw(), setTrack(t), reset(), box(), destroy() }; ctx is documented at mount().
 *
 * The first half is DOM-free (the layout check runs it in Node, scripts/test_nway_lineage.js):
 *   clusterTree  node.path -> nested clusters (no path: a cut of the lineage tree at weak edges),
 *   modulesOf    the groups of the current lens -> modules (a file holds one with at least one of its functions),
 *   buildModel   both + sizes + family homes,
 *   plan         which segment goes on which family ring, one px-per-function scale for all,
 *   geometry     every shape of one frame as a plain list of elements (ring segments, outer ribbons,
 *                lanes, inner ribbons), given the dragged offsets kept by `newSim`.
 * The second half draws that list and runs the soft physics. Every sample-derived string goes through
 * textContent or an attribute, never innerHTML.
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const TAU = 2 * Math.PI;
    // every tuned number of the view
    const RING = {
        DEPTH: 3, DEPTH_BIG: 2, BIG_FILES: 120, // cluster levels drawn; fewer above BIG_FILES files
        LEAF_MAX: 60, // a leaf cluster with more files is one circle with a count until clicked
        FAM_MAX: 24, // families beyond this many merge into "Other files"
        CUT: [0.3, 0.6, 0.85], // fallback: lineage edges below this similarity split family, cluster, sub-cluster
        R_BASE: [24, 13], R_LEAF: [16, 11], PACK_GAP: 24, PACK_PAD: 20, AVOID_MARGIN: 6, AVOID_BEND: 0.25, OWN_MARGIN: 0.1, LANE_EASE: 0.35, // LANE_EASE: share of the remaining angle a lane covers per frame
         // OWN_MARGIN: radians kept free between the lanes of shared modules and those held inside one cluster
         // bands bend around unrelated circles: clearance, and the most a control point moves (share of the band length)
         // circle radius a + b*sqrt(files)
        RI: 6, RM: 14, RO: 22, // ring radii beyond the circle: inner, centre, outer
        SEG_GAP: 0.03, FILL: 0.9, SEG_PAD: 0.012, FLOOR: 2, SEG_STROKE: 12, // ring segments; no segment is ever dropped, the thinnest are drawn FLOOR px wide (1 when a ring is full)
        ARM_MIN: 40, ARM_W: 0.9, ARM_D: 0.36, // outer ribbon arm length: at least this, this share of its width, this share of the distance
        PX_MAX: 2.5, // ceiling of the px-per-function scale, so a small set does not get ring-wide ribbons
        OUTWARD_KEEP: 0.4, // share of a ring kept free on its outward side (away from the other clusters); cross-family arcs use the rest
        BUNDLE_DEFAULT: true, // ribbons between two clusters drawn as one bundle of stripes
        AUTO: { FAMILIES: 3, FILES: 30, FILES_FAMILIES: 2 }, // the page opens in the Ring with at least FAMILIES families, or FILES files in at least FILES_FAMILIES families (the "Other files" family does not count)
        BUCKET_FADE: 0.45, // opacity of the tier buckets next to real lanes
        ARC_DEFAULT: 'group', GROUP_DEG: 40, ORDER_SWEEPS: 6, // arcs per module: partner | group | one; partners closer than GROUP_DEG share an arc in group mode; sweeps of the crossing-minimising order (0 = by direction only)
        MIN_PCT: 0.5, MIN_OPTS: [0, 0.25, 0.5, 1, 2, 5], // gate: modules with fewer functions than this percent of all functions of the set are hidden (0 = off)
        RIBBON_SAFE: 5000, // safety limit of outer ribbons, with a note when it triggers
        OWN_OFF: 10, // below a family's own ring, lanes of modules held in one cluster only sit this far inside the shared lanes
        ROOT_LANE_OFF: -6, // lane ring of a family's own circle, just inside its edge (the outer ring band is r+6..r+22)
        CORE_MIN: 0.9, COMMON_MIN: 0.5, GHOST: 0.28, HATCH: 6, HATCH_DUTY: [0.25, 0.75], // a lane held by fewer than CORE_MIN of its cluster's files is a faint full lane under a 45 degree hatch whose coloured share is the coverage (kept within HATCH_DUTY)
        LANE_OFF: 8, LANE_W: 0.12, LANE_FIT: 0.85, LANE_GAP: 0.05, LANE_STROKE: 7,
        FAM_GAP: 40, SEP: 110, MARGIN: 50, // family spacing when dragged, spacing at home, canvas margin
        FOLLOW: 0.3, DAMP: 0.8, SETTLE: 0.2, DRAG_PX: 4,
        FILE_R: [3.5, 0.2], FILE_FONT: 8, HIDE_FILES: 90, // file dot radius a + b*sqrt(functions); labels hidden above this many drawn files
        BUDGET: [6000, 12000], // estimated elements above which the drawing gets lighter: no parent-to-lane bands, then clusters, rings and family ribbons only
        FRAMES_MAX: 240, MS_MAX: 4000, FRAME_MS: 33, // a redraw loop stops after this many frames or ms; at most 30 frames a second
        MIN_FILL: [0.09, 0.12, 0.16],
    };

    // how widely a module is held inside a cluster: Core (CORE_MIN or more of its files), Common (COMMON_MIN or more), Rare. The glyph is
    // as solid as the lane: filled, half, empty.
    const TIER = { core: ['●', 'Core'], common: ['◐', 'Common'], rare: ['○', 'Rare'] };
    // the 45 degree hatch of a partly held module: one pattern per colour and coverage
    const hatchOf = (b, cov) => { const duty = Math.min(RING.HATCH_DUTY[1], Math.max(RING.HATCH_DUTY[0], cov)); return { id: `rh${b.color.replace(/[^a-z0-9]/gi, '')}${Math.round(duty * 100)}`, col: b.color, duty }; };
    const covTier = (n, t) => { const c = t ? n / t : 1; return c >= RING.CORE_MIN ? 'core' : c >= RING.COMMON_MIN ? 'common' : 'rare'; };
    const tierText = (n, t) => { const k = covTier(n, t); return `${TIER[k][0]} ${TIER[k][1]}`; };
    const norm = a => ((a + 3 * Math.PI) % TAU) - Math.PI;
    const angTo = (a, b) => Math.atan2(b[1] - a[1], b[0] - a[0]);
    const ptAt = (c, r, a) => [c[0] + r * Math.cos(a), c[1] + r * Math.sin(a)];
    const sum = (a, f = x => x) => a.reduce((s, x) => s + f(x), 0);
    const f1 = v => (Math.round(v * 10) / 10).toString();
    const P = p => `${f1(p[0])},${f1(p[1])}`;
    const polyD = pts => `M${pts.map(P).join(' L')}Z`;

    // ---------------------------------------------------------------- bands (ribbons that taper along a curve)
    const bez = (p0, c0, c1, p1, t) => { const m = 1 - t; return [m * m * m * p0[0] + 3 * m * m * t * c0[0] + 3 * m * t * t * c1[0] + t * t * t * p1[0], m * m * m * p0[1] + 3 * m * m * t * c0[1] + 3 * m * t * t * c1[1] + t * t * t * p1[1]]; };
    const bezT = (p0, c0, c1, p1, t) => { const m = 1 - t; return [3 * m * m * (c0[0] - p0[0]) + 6 * m * t * (c1[0] - c0[0]) + 3 * t * t * (p1[0] - c1[0]), 3 * m * m * (c0[1] - p0[1]) + 6 * m * t * (c1[1] - c0[1]) + 3 * t * t * (p1[1] - c1[1])]; };
    function band(p0, c0, c1, p1, w0, w1, n = 16) {
        const L = [], Rr = [];
        for (let k = 0; k <= n; k++) {
            const t = k / n, c = bez(p0, c0, c1, p1, t), tg = bezT(p0, c0, c1, p1, t), len = Math.hypot(tg[0], tg[1]) || 1, nx = -tg[1] / len, ny = tg[0] / len, w = w0 + (w1 - w0) * t;
            L.push([c[0] + (nx * w) / 2, c[1] + (ny * w) / 2]); Rr.push([c[0] - (nx * w) / 2, c[1] - (ny * w) / 2]);
        }
        return { L, R: Rr };
    }

    // ---------------------------------------------------------------- clusters
    // Nested clusters of the nodes. node.path = the table's column groups ({id, label}); a file without one
    // goes to "Other files". With no path anywhere, the lineage tree is cut at weak edges (RING.CUT).
    // -> { roots, byId, famOf: Int array node -> index in roots, hasPath }
    function clusterTree(doc) {
        const nodes = doc.nodes, N = nodes.length, byId = new Map(), roots = [];
        const lvlMax = N > RING.BIG_FILES ? RING.DEPTH_BIG : RING.DEPTH;
        const mk = (id, label, parent, extra) => {
            const c = { id, label, parent, kids: [], files: [], level: 0, fam: null, r: 0, off: [0, 0], ...extra };
            byId.set(id, c);
            (parent ? parent.kids : roots).push(c);
            return c;
        };
        const hasPath = nodes.some(n => (n.path || []).length);
        if (hasPath) {
            nodes.forEach((n, i) => {
                let p = null;
                const path = (n.path || []).slice(0, lvlMax);
                for (const g of path) {
                    const id = `${p ? p.id : ''}/${g.id}`;
                    p = byId.get(id) || mk(id, g.label || String(g.id), p, { gid: g.id });
                    p.files.push(i);
                }
                if (!p) { p = byId.get('/other') || mk('/other', 'Other files', null, { other: true }); p.files.push(i); }
            });
        } else {
            const idx = new Map(nodes.map((n, i) => [n.id, i])), all = nodes.map((_, i) => i);
            const edges = doc.edges.map(e => [idx.get(e.parent), idx.get(e.child), e.similarity ?? 0]);
            const comps = (ids, thr) => {
                const set = new Set(ids), uf = new Map(ids.map(i => [i, i]));
                const find = x => { while (uf.get(x) !== x) { uf.set(x, uf.get(uf.get(x))); x = uf.get(x); } return x; };
                for (const [u, v, s] of edges) if (s >= thr && set.has(u) && set.has(v)) uf.set(find(u), find(v));
                const g = new Map();
                for (const i of ids) { const r = find(i); (g.get(r) || g.set(r, []).get(r)).push(i); }
                return [...g.values()].sort((a, b) => b.length - a.length || a[0] - b[0]);
            };
            // the first threshold from `from` that splits ids into pieces, at least one of them more than a single file
            const cut = (ids, from) => {
                for (let t = from; t < RING.CUT.length; t++) { const cs = comps(ids, RING.CUT[t]); if (cs.length > 1 && cs.some(c => c.length > 1)) return { cs, t }; }
                return null;
            };
            const label = ids => { const b = ids.reduce((x, y) => ((nodes[y].functions || 0) > (nodes[x].functions || 0) ? y : x)), t = String(nodes[b].file_name || nodes[b].id); return t.length > 16 ? `${t.slice(0, 15)}\u2026` : t; };
            const grow = (f, ids, from, depth) => {
                if (depth >= lvlMax) return;
                const r = cut(ids, from);
                if (!r) return;
                r.cs.filter(c => c.length > 1).forEach((c, k) => { const kid = mk(`${f.id}/${k}`, label(c), f, {}); kid.files.push(...c); grow(kid, c, r.t + 1, depth + 1); });
            };
            const top = cut(all, 0);
            if (!top) { const f = mk('/all', 'All files', null, {}); f.files.push(...all); }
            else {
                const real = top.cs.filter(c => c.length > 1), singles = top.cs.filter(c => c.length === 1).flat();
                const keep = real.length > RING.FAM_MAX || singles.length ? RING.FAM_MAX - 1 : RING.FAM_MAX;
                real.slice(0, keep).forEach((c, k) => { const f = mk(`/c${k}`, label(c), null, {}); f.files.push(...c); grow(f, c, top.t + 1, 1); });
                const rest = [...real.slice(keep).flat(), ...singles].sort((x, y) => x - y);
                if (rest.length) { const f = mk('/other', 'Other files', null, { other: true }); f.files.push(...rest); }
            }
        }
        // a cluster whose only child holds the same files adds a ring, not information: absorb it
        const squash = c => { while (c.kids.length === 1 && c.kids[0].files.length === c.files.length) { c.kids = c.kids[0].kids; c.kids.forEach(k => (k.parent = c)); } c.kids.forEach(squash); };
        roots.forEach(squash);
        // files that sit in a cluster but in none of its sub-clusters form an unlabeled sub-cluster of their own
        const loose = c => {
            if (c.kids.length) {
                const inKid = new Set(c.kids.flatMap(k => k.files)), own = c.files.filter(i => !inKid.has(i));
                if (own.length) { const k = { id: `${c.id}#own`, label: '', parent: c, kids: [], files: own, level: 0, fam: null, r: 0, off: [0, 0], loose: true }; c.kids.push(k); byId.set(k.id, k); }
                c.kids.forEach(loose);
            }
        };
        roots.forEach(loose);
        const famOf = new Int32Array(N);
        // biggest families first: the colour order and the placement stay stable
        roots.sort((a, b) => (a.other ? 1 : 0) - (b.other ? 1 : 0) || b.files.length - a.files.length);
        roots.forEach((f, fi) => {
            const lv = (c, d) => { c.level = d; c.fam = f.id; c.famIdx = fi; c.kids.forEach(k => lv(k, d + 1)); };
            lv(f, 0);
            f.files.forEach(i => (famOf[i] = fi));
        });
        return { roots, byId, famOf, hasPath };
    }

    // circle sizes follow the member count; sub-clusters are packed inside their parent
    // lanes(n) = modules held by the cluster: sibling circles are kept apart by their lane rings plus room that grows with the lanes
    function sizeNode(n, lanes = () => 0) {
        n.kids.forEach(k => sizeNode(k, lanes));
        const cnt = n.files.length, k = n.kids, base = RING.R_BASE[0] + RING.R_BASE[1] * Math.sqrt(cnt);
        if (!k.length) { n.r = Math.max(base, RING.R_LEAF[0] + RING.R_LEAF[1] * Math.sqrt(cnt)); return; }
        const g = Math.max(RING.PACK_GAP, 2 * (RING.LANE_OFF + RING.LANE_STROKE)) + Math.min(40, 2 * Math.sqrt(Math.max(...k.map(lanes)))) + (n.level === 0 ? 10 : 0), pad = RING.PACK_PAD;
        let req;
        if (k.length === 1) { req = k[0].r + 2 * pad; k[0].off = [0, 0]; }
        else if (k.length === 2) { const w = 2 * k[0].r + 2 * k[1].r + g; k[0].off = [-w / 2 + k[0].r, 0]; k[1].off = [w / 2 - k[1].r, 0]; req = w / 2 + pad; }
        else {
            const rm = Math.max(...k.map(x => x.r)), rho = (2 * rm + g) / (2 * Math.sin(Math.PI / k.length));
            k.forEach((x, i) => { const a = (TAU * i) / k.length - Math.PI / 2; x.off = [rho * Math.cos(a), rho * Math.sin(a)]; });
            req = rho + rm + pad;
        }
        n.r = Math.max(req, base);
    }

    // ---------------------------------------------------------------- modules
    // The groups of the lens (C = lensCats) that have rows. A file holds a module when it has at least one of its functions.
    // One pass over the row masks gives, per module: size (rows), rowsIn[f] (rows held by a file of family f), pair[x|y]
    // (rows held by a file of x and a file of y) and the files of x holding such a row (hold[x|y]).
    function modulesOf(R, C, N, famOf, F, o = {}) {
        const K = C.cats.length, at = new Map(C.cats.map((c, i) => [c.key, i]));
        const size = new Uint32Array(K), has = new Uint8Array(K * N), cnt = new Uint32Array(K * N), rowsIn = new Uint32Array(K * F), pair = C.cats.map(() => new Map()), hold = C.cats.map(() => new Map());
        const inRow = [], fams = [];
        for (let r = 0; r < R.n; r++) {
            if (!R.match(r)) continue;
            const g = at.get(C.of(r));
            if (g == null) continue;
            size[g]++;
            inRow.length = 0;
            const h = R.mask[r] || '';
            for (let j = h.length - 1, i0 = 0; j >= 0; j--, i0 += 4) {
                const v = parseInt(h[j], 16) || 0;
                if (!v) continue;
                for (let b = 0; b < 4; b++) if ((v >> b) & 1 && i0 + b < N) { inRow.push(i0 + b); has[g * N + i0 + b] = 1; cnt[g * N + i0 + b]++; }
            }
            fams.length = 0;
            for (const i of inRow) if (!fams.includes(famOf[i])) fams.push(famOf[i]);
            for (const f of fams) rowsIn[g * F + f]++;
            for (const x of fams) for (const y of fams) {
                if (x === y) continue;
                const k = `${x}|${y}`;
                if (x < y) pair[g].set(k, (pair[g].get(k) || 0) + 1);
                const set = hold[g].get(k) || hold[g].set(k, new Set()).get(k);
                for (const i of inRow) if (famOf[i] === x) set.add(i);
            }
        }
        const mods = [], gate = ((o.minPct || 0) / 100) * R.n;
        let hidden = 0, buckets = 0, bucketsHidden = 0, bucketFuncs = 0;
        C.cats.forEach((c, g) => {
            if (!size[g]) return;
            const mem = [];
            for (let i = 0; i < N; i++) if (has[g * N + i]) mem.push(i);
            // the gate: a module's own rows against all functions of the set, before any split between families
            if (mem.length && size[g] < gate) { hidden++; return; }
            // the tier / fold groups of the Lanes lens that hold rows of their own: remainders, not lanes
            const bucket = !!(c.g && (c.g.kind === 'tier' || c.g.kind === 'fold'));
            if (mem.length && bucket) buckets++;
            if (mem.length && bucket && o.tier === 'hide') { bucketsHidden++; bucketFuncs += size[g]; return; }
            if (mem.length) mods.push({ cat: c, tk: c.tk, size: size[g], mem, cnt: cnt.subarray(g * N, g * N + N), rowsIn: Array.from(rowsIn.subarray(g * F, g * F + F)), pair: pair[g], hold: hold[g], grey: !!o.grey && c.color === o.grey, bucket, get label() { return this.bucket ? (c.g.kind === 'fold' ? 'slim lanes folded' : `${c.g.label || this.cat.label}: rows in no lane`) : this.cat.label; } });
        });
        mods.hidden = hidden; mods.buckets = buckets; mods.bucketsHidden = bucketsHidden; mods.bucketFuncs = bucketFuncs;
        return mods;
    }

    // ---------------------------------------------------------------- the model
    // o: { minPct (gate, percent of all functions), tier ('hide' drops the tier buckets), oneArc, hsl(h, l) -> css colour, hue(i), tree (NwayColors.tree of the nodes), nodeColor(i), grey }
    function buildModel(doc, R, C, o = {}) {
        const N = doc.nodes.length, T = clusterTree(doc), roots = T.roots, hsl = o.hsl || ((h, l) => `hsl(${h},62%,${l}%)`);
        // colours: a cluster has the hue its columns have in the table; without paths, one hue per family
        const colour = (c, fi) => {
            const t = T.hasPath && o.tree && o.tree.byId.get(c.gid);
            return t && t.h != null ? hsl(t.h, t.l) : hsl(o.hue ? o.hue(fi) : (fi * 137) % 360, 52 - 4 * Math.min(c.level, 2));
        };
        roots.forEach((f, fi) => { const walk = c => { c.col = colour(c, fi); c.kids.forEach(walk); }; walk(f); f.famCol = f.other ? 'var(--lin-grey)' : f.col; });
        const mods = R && C ? modulesOf(R, C, N, T.famOf, roots.length, o) : [];
        const laneCount = new Map();
        const lanes = n => { if (!laneCount.has(n.id)) { const set = new Set(n.files); laneCount.set(n.id, mods.filter(m => m.mem.some(i => set.has(i))).length); } return laneCount.get(n.id); };
        roots.forEach(f => sizeNode(f, lanes));
        mods.sort((a, b) => b.size - a.size);
        mods.forEach((b, k) => {
            b.k = k;
            b.fams = [...new Set(b.mem.map(i => T.famOf[i]))].sort((x, y) => x - y);
            b.pairRows = (x, y) => b.pair.get(`${Math.min(x, y)}|${Math.max(x, y)}`) || 0;
            b.fam = b.fams.length === 1 ? b.fams[0] : null;
            // exactly the colour the Tree gives the group (grey stays grey where the lens says so); a golden-angle hue only when the lens gives none
            b.color = b.bucket ? o.grey || b.cat.color : b.cat.color || hsl(Math.round((k * 137.508 + 40) % 360), 52);
        });
        const M = { doc, N, T, roots, modeOf, arcMode: o.arc || (o.oneArc ? 'one' : RING.ARC_DEFAULT), hidden: mods.hidden || 0, buckets: mods.buckets || 0, bucketsHidden: mods.bucketsHidden || 0, bucketFuncs: mods.bucketFuncs || 0, nFuncs: R ? R.n : 0, minPct: o.minPct || 0, byId: T.byId, famOf: T.famOf, mods, famFiles: roots.map(f => f.files.length), nodeColor: o.nodeColor || (() => '#888') };
        M.homes = placeFamilies(M);
        M.plan = plan(M);
        M.lanePx = lanePx(M);
        return M;
    }

    // estimated elements of a full drawing, and the lighter mode that keeps it under RING.BUDGET:
    // 0 everything, 1 no parent-to-lane bands and file labels, 2 clusters, rings and family ribbons only
    function modeOf(nested) {
        const M = this, nCl = nested ? M.byId.size : M.roots.length;
        const all = [...M.plan.segs.flat(), ...M.plan.layers.flat()], segs = all.length, fans = sum(all, x => x.hold.size), pairs = M.plan.ribbons;
        const est = 2 * nCl + segs + pairs + M.N * 2 + (nested ? 4 * segs : 0) + fans;
        return { est, mode: est > RING.BUDGET[1] ? 2 : est > RING.BUDGET[0] ? 1 : 0 };
    }

    // the radii of a family's rings
    const rg = f => ({ ri: f.r + RING.RI, rm: f.r + RING.RM, ro: f.r + RING.RO });

    // The segments of every family ring, and the one px-per-function scale of all of them. Nothing is dropped: a layer module has
    // one segment (size = its rows held in the family), a cross module one per partner family (size = the rows held by a file
    // of each of the two, so both ends of a ribbon are equal). A segment thinner than the scale gives is drawn FLOOR px wide
    // (less when a ring is genuinely full); the scale shrinks until every ring fits.
    // runs of partner families seen from family c's home: a gap over RING.GROUP_DEG degrees between neighbours starts a new run
    function groupRuns(H, c, ps) {
        if (ps.length < 2) return ps.map(p => [p]);
        const thr = (RING.GROUP_DEG * Math.PI) / 180, a = new Map(ps.map(p => [p, angTo(H[c], H[p])])), s = ps.slice().sort((x, y) => a.get(x) - a.get(y) || x - y);
        const gap = i => { const d = a.get(s[(i + 1) % s.length]) - a.get(s[i]); return i === s.length - 1 ? d + TAU : d; };
        const start = s.findIndex((_, i) => gap(i) > thr);
        if (start < 0) return [s];
        const rot = [...s.slice(start + 1), ...s.slice(0, start + 1)], runs = [[rot[0]]];
        for (let i = 1; i < rot.length; i++) {
            const d = ((a.get(rot[i]) - a.get(rot[i - 1])) % TAU + TAU) % TAU;
            if (d > thr) runs.push([rot[i]]); else runs[runs.length - 1].push(rot[i]);
        }
        return runs;
    }

    // pixel width of a segment: each of its slots (or the segment itself) at least `floor` wide
    const wOf = (s, q, floor) => (s.slots ? sum(s.slots, t => Math.max(t.size * q, floor)) : Math.max(s.size * q, floor));
    function plan(M) {
        const { roots, mods } = M;
        const all = [];
        for (const b of mods) if (b.fam == null) for (let u = 0; u < b.fams.length; u++) for (let v = u + 1; v < b.fams.length; v++) all.push([b, b.fams[u], b.fams[v], b.pairRows(b.fams[u], b.fams[v])]);
        // the safety limit of outer ribbons: the smallest are left out, on both ends
        const skip = new Set();
        let truncated = 0;
        if (all.length > RING.RIBBON_SAFE) {
            all.slice().sort((a, b2) => b2[3] - a[3]).slice(RING.RIBBON_SAFE).forEach(([b, x, y]) => { skip.add(`${b.k}|${x}|${y}`); truncated++; });
        }
        const layers = roots.map(() => []);
        const segs = roots.map((f, c) => {
            const l = [];
            for (const b of mods) {
                if (!b.fams.includes(c)) continue;
                if (b.fam != null) { layers[c].push({ b, size: b.rowsIn[c], hold: new Set(b.mem.filter(i => M.famOf[i] === c)), key: '' }); continue; }
                const ps = b.fams.filter(p => p !== c && !skip.has(`${b.k}|${Math.min(c, p)}|${Math.max(c, p)}`));
                const arc = (run, key) => ({ b, slots: run.map(p => ({ p, size: b.pairRows(c, p) })), size: sum(run, p => b.pairRows(c, p)), hold: new Set(run.flatMap(p => [...(b.hold.get(`${c}|${p}`) || [])])), key, ...(run.length === 1 ? { partner: run[0] } : {}) });
                if (!ps.length) continue;
                // one arc per partner, per run of nearby partners, or one for all of them
                if (M.arcMode === 'one') l.push(arc(ps, ''));
                else for (const run of M.arcMode === 'group' ? groupRuns(M.homes, c, ps) : ps.map(p => [p])) l.push(arc(run, run.length === 1 ? String(run[0]) : `g${run.join('_')}`));
            }
            l.sort((a, b2) => b2.size - a.size);
            return l;
        });
        const avail = segs.map((l, c) => RING.FILL * (1 - RING.OUTWARD_KEEP) * TAU * rg(roots[c]).ro);
        // gap between segments: the usual one, less on a crowded ring
        const gaps = segs.map((l, c) => (l.length ? Math.min(RING.SEG_GAP * rg(roots[c]).ro, (0.2 * avail[c]) / l.length) : 0));
        let floor = RING.FLOOR;
        segs.forEach((l, c) => { const room = avail[c] - gaps[c] * l.length; if (l.length * floor > room) floor = Math.min(floor, Math.max(0.2, (0.9 * room) / l.length)); });
        let px = RING.PX_MAX;
        segs.forEach((l, c) => {
            const room = avail[c] - gaps[c] * l.length, used = q => sum(l, s => wOf(s, q, floor));
            if (used(px) <= room) return;
            let lo = 0, hi = px;
            for (let it = 0; it < 40; it++) { const mid = (lo + hi) / 2; if (used(mid) <= room) lo = mid; else hi = mid; }
            px = lo;
        });
        const floorHits = sum(segs, l => sum(l, s => (s.slots ? s.slots.filter(t => t.size * px < floor).length : s.size * px < floor ? 1 : 0)));
        return { segs, px, floor, w: s => wOf(s, px, floor), gaps: gaps.map((g, c) => g / rg(roots[c]).ro), floorHits, ribbons: all.length - truncated, truncated, layers };
    }

    // the lowest cluster node of family root f that contains all of the files (f itself when not nested)
    function lcaNode(f, files, nested) {
        let n = f;
        while (nested) {
            const k = n.kids.find(K => { const set = K._set || (K._set = new Set(K.files)); return [...files].every(i => set.has(i)); });
            if (!k) break;
            n = k;
        }
        return n;
    }

    // One px-per-function for every lane: RING.LANE_W, or less when the lanes of some cluster would not fit its ring.
    function lanePx(M) {
        // start from the outer ribbons' px-per-function, so a lane is as thick as the ribbon it continues; the fit below only shrinks it
        let px = Math.max(RING.LANE_W, M.plan.px);
        const cross = M.plan.segs.flat(), lay = M.plan.layers.flatMap((l, c) => l.map(x => ({ x, lca: lcaNode(M.roots[c], x.hold, true) })));
        for (const n of M.byId.values()) {
            const set = new Set(n.files), has = x => [...x.hold].some(i => set.has(i));
            const rp = n.r + (n.level === 0 ? RING.ROOT_LANE_OFF : RING.LANE_OFF);
            // the lanes of this ring: cross arcs coming down to it, and the single-cluster modules whose chain passes through it (root: only those starting there)
            const fed = n.level === 0 ? lay.filter(o => o.lca === n).map(o => o.x) : [...cross.filter(has), ...lay.filter(o => has(o.x) && n.files.every(i => (o.lca._set || (o.lca._set = new Set(o.lca.files))).has(i))).map(o => o.x)];
            if (!fed.length) continue;
            // a family's own ring holds only the modules of that cluster; the shared arcs of its outer ring already use part of the one turn, so these lanes get the rest
            const pl = M.plan, sh = n.level === 0 ? sum(pl.segs[n.famIdx], s => pl.w(s)) / rg(n).ro + (pl.gaps[n.famIdx] || 0) * pl.segs[n.famIdx].length : 0;
            const turn = Math.max(0.15 * TAU, RING.LANE_FIT * TAU - sh);
            px = Math.min(px, (turn * rp - fed.length * RING.LANE_GAP * rp) / sum(fed, x => Math.max(x.size, 1)));
        }
        return Math.max(px, 1e-4);
    }

    // Home of each family: a short stress relaxation, pairs that share more sit closer.
    function placeFamilies(M) {
        const { roots, mods } = M, n = roots.length, ro = roots.map(f => rg(f).ro);
        if (n === 1) return [[0, 0]];
        const W = Array.from({ length: n }, () => new Array(n).fill(0));
        for (const b of mods) if (b.fam == null) for (const x of b.fams) for (const y of b.fams) if (x < y) W[x][y] = W[y][x] = W[x][y] + b.size / (b.fams.length - 1);
        const wmax = Math.max(1, ...W.flat());
        const rest = (u, v) => ro[u] + ro[v] + RING.SEP + (1 - W[u][v] / wmax) * 260, min = (u, v) => ro[u] + ro[v] + RING.SEP * 0.6;
        const R0 = Math.max(120, sum(ro) / Math.PI);
        const p = roots.map((_, i) => { const a = (TAU * i) / n; return [R0 * Math.cos(a), R0 * Math.sin(a)]; });
        for (let it = 0; it < 400; it++) {
            const step = 0.12 * (1 - it / 450);
            for (let u = 0; u < n; u++) for (let v = u + 1; v < n; v++) {
                const dx = p[v][0] - p[u][0], dy = p[v][1] - p[u][1], d = Math.hypot(dx, dy) || 0.01, want = d < min(u, v) ? min(u, v) : rest(u, v), s = ((d - want) / d) * (d < min(u, v) ? 0.5 : step), mx = dx * s * 0.5, my = dy * s * 0.5;
                p[u][0] += mx; p[u][1] += my; p[v][0] -= mx; p[v][1] -= my;
            }
        }
        const cx = sum(p, q => q[0]) / n, cy = sum(p, q => q[1]) / n;
        return p.map(q => [q[0] - cx, q[1] - cy]);
    }

    // files inside a leaf cluster: an inner ring (small clusters), else a sunflower
    function placeFiles(files, c, rb, pos) {
        files.forEach((i, k) => {
            if (files.length <= 8) { const a = -Math.PI / 2 + (TAU * k) / files.length, rho = files.length === 1 ? 0 : rb * 0.55; pos[i] = [c[0] + rho * Math.cos(a), c[1] + rho * Math.sin(a)]; }
            else { const rho = rb * 0.8 * Math.sqrt((k + 0.5) / files.length), a = k * 2.399963; pos[i] = [c[0] + rho * Math.cos(a), c[1] + rho * Math.sin(a)]; }
        });
    }

    // ---------------------------------------------------------------- soft physics
    // Every draggable thing is a point stored as a displacement from its computed home, so a lens or filter change
    // keeps what was moved. geometry() registers the points of each frame (d), who follows whom, and the constraint.
    function newSim() {
        const sim = {
            pts: {}, dir: {}, orient: {}, lane: {}, laneRes: 0, homes: {}, fhomes: {}, links: [], cons: null, drag: null, moved: false,
            begin() { this.links = []; this.cons = null; this.laneRes = 0; },
            d(key, hx, hy) { const p = this.pts[key] || (this.pts[key] = { dx: 0, dy: 0, vx: 0, vy: 0, ldx: 0, ldy: 0, qx: 0, qy: 0 }); p.hx = hx; p.hy = hy; return [p.dx, p.dy]; },
            follow(child, parent, rate) { this.links.push([child, parent, rate]); },
            // one step; returns the remaining motion
            tick() {
                const ks = Object.keys(this.pts), dr = this.drag;
                let e = this.laneRes || 0;
                for (const k of ks) {
                    const p = this.pts[k];
                    if (dr && dr.key === k) { p.dx = dr.px - p.hx - dr.gx; p.dy = dr.py - p.hy - dr.gy; p.vx = p.vy = 0; continue; }
                    p.vx *= RING.DAMP; p.vy *= RING.DAMP; p.dx += p.vx; p.dy += p.vy;
                    e += Math.abs(p.vx) + Math.abs(p.vy);
                }
                if (this.cons) {
                    const before = ks.map(k => [this.pts[k].dx, this.pts[k].dy]);
                    this.cons(this.pts, dr && dr.key);
                    ks.forEach((k, n) => { e += Math.abs(this.pts[k].dx - before[n][0]) + Math.abs(this.pts[k].dy - before[n][1]); });
                }
                // a follower keeps its offset to the parent and lags behind the parent's movement
                for (const [c, par, rate] of this.links) {
                    const pc = this.pts[c], pp = this.pts[par];
                    if (!pc || !pp) continue;
                    pc.qx += pp.dx - pp.ldx; pc.qy += pp.dy - pp.ldy;
                    if (dr && dr.key === c) { pc.qx = pc.qy = 0; continue; }
                    const mx = pc.qx * rate, my = pc.qy * rate;
                    pc.dx += mx; pc.dy += my; pc.qx -= mx; pc.qy -= my;
                    e += Math.abs(pc.qx) + Math.abs(pc.qy);
                }
                for (const k of ks) { const p = this.pts[k]; p.ldx = p.dx; p.ldy = p.dy; }
                return e;
            },
            reset() { this.pts = {}; this.dir = {}; this.orient = {}; this.homes = {}; this.fhomes = {}; },
        };
        return sim;
    }

    // ---------------------------------------------------------------- geometry
    // One frame as a flat element list [{id, tag, a (attributes), s (style), m (module k), h (modules a file holds), drag, ...}]
    // in drawing order. S: { sim, nested, open: Set of leaf cluster ids drawn open, pin (module k) }, o: { light }.
    function geometry(M, S, o = {}) {
        const { roots, mods } = M, sim = S.sim, nested = S.nested !== false, open = S.open || new Set();
        const mode = o.mode ?? M.modeOf(nested).mode, light = mode >= 1, lean = mode >= 2;
        const nF = roots.length, pl = M.plan, px = pl.px;
        const els = [];
        sim.begin();
        const add = (id, tag, a, extra = {}) => { els.push({ id, tag, a, ...extra }); return els[els.length - 1]; };
        const key = n => `f:${n.id}`;
        const fade = b => (b.bucket ? RING.BUCKET_FADE : 1);
        const lset = new Map();
        const lcov = (b, X) => { let st = lset.get(X.id); if (!st) lset.set(X.id, (st = new Set(X.files))); let n = 0; for (const i of b.mem) if (st.has(i)) n++; return { n, t: X.files.length, name: X.label }; };
        const holdsOf = new Map();
        for (const b of mods) for (const i of b.mem) (holdsOf.get(i) || holdsOf.set(i, []).get(i)).push(b.k);
        const out = { els, nc: {}, pos: {}, cen: {}, stats: { ribbons: [], lanes: [], laneFit: 1 } };

        // static homes: families at their spots, sub-clusters rotated to sit side by side across the line to the map centre
        const ctr = [sum(M.homes, h => h[0]) / (nF || 1), sum(M.homes, h => h[1]) / (nF || 1)], NH = {};
        const homeNode = (n, c0, rot) => { const c = sim.homes[n.id] || c0; NH[n.id] = c; n.kids.forEach(k => homeNode(k, [c[0] + k.off[0] * Math.cos(rot) - k.off[1] * Math.sin(rot), c[1] + k.off[0] * Math.sin(rot) + k.off[1] * Math.cos(rot)], rot)); };
        roots.forEach((f, c) => homeNode(f, M.homes[c], nF > 1 ? Math.atan2(M.homes[c][1] - ctr[1], M.homes[c][0] - ctr[0]) + Math.PI / 2 : 0));
        let bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
        roots.forEach((f, c) => { const e = rg(f).ro + 80, h = NH[f.id]; bx0 = Math.min(bx0, h[0] - e); by0 = Math.min(by0, h[1] - e); bx1 = Math.max(bx1, h[0] + e); by1 = Math.max(by1, h[1] + e); });
        const bounds = { x0: bx0 - RING.MARGIN * 4, y0: by0 - RING.MARGIN * 4, x1: bx1 + RING.MARGIN * 4, y1: by1 + RING.MARGIN * 4 };
        out.bounds = bounds;
        out.fit = { x0: bx0 + 20, y0: by0 + 20, x1: bx1 - 20, y1: by1 - 20 };

        // cluster centres are physics points; a sub-cluster follows its parent
        const nc = out.nc, cen = out.cen;
        const place = n => { const h = NH[n.id], d = sim.d(key(n), h[0], h[1]); nc[n.id] = [h[0] + d[0], h[1] + d[1]]; if (n.parent) sim.follow(key(n), key(n.parent), RING.FOLLOW); n.kids.forEach(place); };
        roots.forEach((f, c) => { place(f); cen[c] = nc[f.id]; });
        const agg = n => (!nested || !n.kids.length) && n.files.length > RING.LEAF_MAX && !open.has(n.id);
        // files sit on the inner ring of their leaf cluster; the files of a collapsed circle sit at its centre
        const pos = out.pos, hpos = {}, owner = {}, aggOf = {};
        const leafPlace = n => {
            if (!nested || !n.kids.length) {
                if (agg(n)) n.files.forEach(i => { hpos[i] = NH[n.id]; aggOf[i] = n; }); else placeFiles(n.files, NH[n.id], n.r, hpos);
                n.files.forEach(i => (owner[i] = key(n)));
            } else n.kids.forEach(leafPlace);
        };
        roots.forEach(leafPlace);
        // physics points of files are keyed by node id, so they survive a doc whose node order changed
        const nk = i => `n:${M.doc.nodes[i].id}`;
        for (let i = 0; i < M.N; i++) {
            if (!hpos[i]) hpos[i] = NH[roots[M.famOf[i]].id];
            if (aggOf[i]) { pos[i] = nc[aggOf[i].id]; continue; }
            const nid = M.doc.nodes[i].id;
            if (sim.fhomes[nid]) hpos[i] = sim.fhomes[nid];
            sim.fhomes[nid] = hpos[i];
            const d = sim.d(nk(i), hpos[i][0], hpos[i][1]);
            pos[i] = [hpos[i][0] + d[0], hpos[i][1] + d[1]];
            sim.follow(nk(i), owner[i] || key(roots[M.famOf[i]]), RING.FOLLOW);
        }
        for (const id of Object.keys(NH)) sim.homes[id] = NH[id];
        // families keep apart and inside the canvas
        sim.cons = (Pt, dragged) => {
            const k = roots.map(f => Pt[`f:${f.id}`]);
            for (let it = 0; it < 10; it++) {
                for (let u = 0; u < nF; u++) for (let v = u + 1; v < nF; v++) {
                    const A = k[u], B = k[v], dx = B.hx + B.dx - A.hx - A.dx, dy = B.hy + B.dy - A.hy - A.dy, dist = Math.hypot(dx, dy) || 1, min = rg(roots[u]).ro + rg(roots[v]).ro + RING.FAM_GAP;
                    if (dist >= min) continue;
                    const push = min - dist, ux = dist === 1 && !dx && !dy ? 1 : dx / dist, uy = dist === 1 && !dx && !dy ? 0 : dy / dist, aFix = dragged === `f:${roots[u].id}`, bFix = dragged === `f:${roots[v].id}`, sa = bFix ? 0.85 : aFix ? 0.15 : 0.5, sb = 1 - sa;
                    A.dx -= ux * push * sa; A.dy -= uy * push * sa; B.dx += ux * push * sb; B.dy += uy * push * sb;
                }
                k.forEach((p, n) => { const m = rg(roots[n]).ro + RING.MARGIN; p.dx = Math.min(bounds.x1 - m - p.hx, Math.max(bounds.x0 + m - p.hx, p.dx)); p.dy = Math.min(bounds.y1 - m - p.hy, Math.max(bounds.y0 + m - p.hy, p.dy)); });
            }
        };

        // circles
        const drawNode = n => {
            const C = nc[n.id], lvl = Math.min(n.level, 2);
            if (n.level && !nested) return;
            add(`c:${n.id}`, 'circle', { cx: f1(C[0]), cy: f1(C[1]), r: f1(n.r) }, { s: { fill: n.col, stroke: n.col }, sa: { opacity: RING.MIN_FILL[lvl], 'stroke-width': n.level ? 1 : 1.2, 'stroke-dasharray': n.level ? '3 3' : null }, drag: key(n), cl: n });
            const label = n.loose ? '' : n.label;
            if (n.level && label) add(`cl:${n.id}`, 'text', { x: f1(C[0]), y: f1(C[1] - n.r + 13), 'text-anchor': 'middle', 'font-size': n.level === 1 ? 11 : 9 }, { text: label, cls: 'ring-sub', s: { fill: n.col }, lab: { id: n.id, r: n.r, lvl: n.level } });
            if (agg(n)) {
                const br = Math.min(13, n.r * 0.4);
                add(`cb:${n.id}`, 'circle', { cx: f1(C[0]), cy: f1(C[1] - 5), r: f1(br) }, { cls: 'ring-plus', s: { fill: n.col }, cl: n, drag: key(n) });
                add(`cp:${n.id}`, 'text', { x: f1(C[0]), y: f1(C[1] - 5 + br * 0.42), 'text-anchor': 'middle', 'font-size': f1(br * 1.4) }, { text: '+', cls: 'ring-fl', pe: true });
                add(`ca:${n.id}`, 'text', { x: f1(C[0]), y: f1(C[1] + br + 10), 'text-anchor': 'middle', 'font-size': 12 }, { text: `${n.files.length}`, cls: 'ring-sub', cl: n, drag: key(n) });
            }
            n.kids.forEach(drawNode);
        };
        roots.forEach(drawNode);

        // ring segments: layers on the outward side, shared modules facing their partner family
        const segs = [], tos = [];
        roots.forEach((f, c) => {
            const th = roots.map((_, x) => x).filter(x => x !== c).map(x => angTo(cen[c], cen[x])), prev = sim.dir[c];
            let best = prev ?? -Math.PI / 2, bs = -9;
            if (th.length) for (let k = 0; k < 72; k++) {
                const cand = (k / 72) * TAU - Math.PI, sc = Math.min(...th.map(a => Math.abs(norm(cand - a)))) - (prev == null ? 0 : 0.12 * Math.abs(norm(cand - prev)));
                if (sc > bs) { bs = sc; best = cand; }
            }
            sim.dir[c] = prev == null ? best : prev + Math.max(-0.35, Math.min(0.35, norm(best - prev)));
            tos[c] = sim.dir[c];
            segs[c] = pl.segs[c].map(s => {
                if (s.slots && s.partner == null) {
                    // one arc: faces the circular mean of the directions to its partners
                    const ds = s.slots.map(t => [angTo(cen[c], cen[t.p]), t.size + 1]), mean = Math.atan2(sum(ds, d => d[1] * Math.sin(d[0])), sum(ds, d => d[1] * Math.cos(d[0])));
                    return { ...s, want: norm(mean - (tos[c] + Math.PI)) };
                }
                return { ...s, want: s.partner == null ? 0 : norm(angTo(cen[c], cen[s.partner]) - (tos[c] + Math.PI)) };
            });
        });
        const placed = [];
        roots.forEach((f, c) => {
            const { ro } = rg(f), GAP = pl.gaps[c], A = x => pl.w(x) / ro;
            const hs = x => [...x.hold];
            const side = x => { const h = hs(x); if (!h.length) return 0; const vx = sum(h, i => Math.cos(angTo(cen[c], pos[i]))), vy = sum(h, i => Math.sin(angTo(cen[c], pos[i]))); return Math.hypot(vx, vy) / h.length < 0.25 ? 0 : norm(Math.atan2(vy, vx) - tos[c]); };
            const inner_ = segs[c].filter(x => x.b.fam != null).map(x => ({ x, s: side(x) })).sort((p, q) => p.s - q.s || q.x.size - p.x.size).map(q => q.x), cross = segs[c].filter(x => x.b.fam == null);
            const span = Math.max(sum(inner_, x => A(x)) + GAP * Math.max(0, inner_.length - 1), nF > 1 ? RING.OUTWARD_KEEP * TAU : 0);
            let a = tos[c] - span / 2;
            inner_.forEach(x => { x.a0 = a; x.a1 = a + A(x); a = x.a1 + GAP; });
            const hw = Math.PI - span / 2 - GAP, mid0 = tos[c] + Math.PI;
            cross.forEach(x => { x.w = A(x); x.c = Math.max(-hw + x.w / 2, Math.min(hw - x.w / 2, x.want)); });
            const flip = x => (c < (x.partner ?? 0) ? 1 : -1) * x.b.k;
            cross.sort((p, q) => p.c - q.c || flip(p) - flip(q));
            placed[c] = { inner_, cross, hw, mid0, GAP };
        });
        // spread the arcs of a ring along its free part, in their order, without overlap
        const relax = r => {
            const { cross, hw, GAP } = r;
            for (let it = 0; it < 80; it++) {
                for (let k = 1; k < cross.length; k++) {
                    const p = cross[k - 1], q = cross[k], gap = q.c - q.w / 2 - (p.c + p.w / 2) - GAP;
                    if (gap < 0) { p.c += gap / 2; q.c -= gap / 2; }
                }
                cross.forEach(x => (x.c = Math.max(-hw + x.w / 2, Math.min(hw - x.w / 2, x.c))));
            }
            // a last sweep each way guarantees no overlap when the segments fit the ring
            if (cross.length) cross[0].c = Math.max(cross[0].c, -hw + cross[0].w / 2);
            for (let k = 1; k < cross.length; k++) cross[k].c = Math.max(cross[k].c, cross[k - 1].c + cross[k - 1].w / 2 + GAP + cross[k].w / 2);
            if (cross.length) cross[cross.length - 1].c = Math.min(cross[cross.length - 1].c, hw - cross[cross.length - 1].w / 2);
            for (let k = cross.length - 2; k >= 0; k--) cross[k].c = Math.min(cross[k].c, cross[k + 1].c - cross[k + 1].w / 2 - GAP - cross[k].w / 2);
            cross.forEach(x => { x.a0 = r.mid0 + x.c - x.w / 2; x.a1 = r.mid0 + x.c + x.w / 2; });
        };
        placed.forEach(relax);
        // a proxy for the crossings of the outer ribbons (their centre curves), to keep the better of the by-direction order and the sweeps
        const at = new Map();
        placed.forEach((r, c) => r.cross.forEach(x => x.slots.forEach(t => at.set(`${x.b.k}|${c}|${t.p}`, x))));
        const crossCount = () => {
            const curves = [];
            placed.forEach((r, c) => r.cross.forEach(x => x.slots.forEach(t => {
                if (c > t.p) return;
                const y = at.get(`${x.b.k}|${t.p}|${c}`);
                if (!y) return;
                const ma = (x.a0 + x.a1) / 2, mb = (y.a0 + y.a1) / 2, p0 = ptAt(cen[c], rg(roots[c]).ro, ma), p1 = ptAt(cen[t.p], rg(roots[t.p]).ro, mb);
                const h = Math.max(RING.ARM_MIN, (x.a1 - x.a0) * rg(roots[c]).ro * RING.ARM_W, Math.hypot(p0[0] - p1[0], p0[1] - p1[1]) * RING.ARM_D);
                const c0 = [p0[0] + Math.cos(ma) * h, p0[1] + Math.sin(ma) * h], c1 = [p1[0] + Math.cos(mb) * h, p1[1] + Math.sin(mb) * h], pts = [];
                for (let i = 0; i <= 8; i++) pts.push(bez(p0, c0, c1, p1, i / 8));
                curves.push(pts);
            })));
            if (curves.length > 800) return 0;
            const ccw = (a, b, c) => (c[1] - a[1]) * (b[0] - a[0]) > (b[1] - a[1]) * (c[0] - a[0]);
            let n = 0;
            for (let i = 0; i < curves.length; i++) for (let j = i + 1; j < curves.length; j++) {
                let hit = false;
                for (let u = 0; u < 8 && !hit; u++) for (let v = 0; v < 8; v++) {
                    const a = curves[i][u], b = curves[i][u + 1], c = curves[j][v], d = curves[j][v + 1];
                    if (ccw(a, c, d) !== ccw(b, c, d) && ccw(a, b, c) !== ccw(a, b, d)) { hit = true; break; }
                }
                if (hit) n++;
            }
            return n;
        };
        const snapshot = () => placed.map(r => ({ order: r.cross.slice(), cs: r.cross.map(x => x.c) }));
        const restore = sn => placed.forEach((r, i) => { r.cross = sn[i].order; r.cross.forEach((x, k) => { x.c = sn[i].cs[k]; x.a0 = r.mid0 + x.c - x.w / 2; x.a1 = r.mid0 + x.c + x.w / 2; }); });
        // crossing-minimising order: an arc goes where the arcs it connects to lie, seen from its ring's centre (barycentre sweeps over all rings)
        if (RING.ORDER_SWEEPS > 0 && nF > 1) {
            let best = { n: crossCount(), sn: snapshot() };
            for (let sweep = 0; sweep < RING.ORDER_SWEEPS; sweep++) {
                let changed = false;
                placed.forEach((r, c) => {
                    if (r.cross.length < 2) return;
                    const keyOf = x => {
                        let vx = 0, vy = 0;
                        for (const t of x.slots) {
                            const y = at.get(`${x.b.k}|${t.p}|${c}`);
                            if (!y) continue;
                            const q = ptAt(cen[t.p], rg(roots[t.p]).ro, (y.a0 + y.a1) / 2), an = angTo(cen[c], q), w = t.size + 1;
                            vx += w * Math.cos(an); vy += w * Math.sin(an);
                        }
                        return norm(Math.atan2(vy, vx) - (tos[c] + Math.PI));
                    };
                    const order = r.cross.map((x, i) => [x, keyOf(x), i]).sort((u, v) => u[1] - v[1] || u[2] - v[2]).map(o => o[0]);
                    if (order.every((x, i) => x === r.cross[i])) return;
                    changed = true;
                    const cs = r.cross.map(x => x.c).sort((u, v) => u - v);
                    order.forEach((x, i) => (x.c = cs[i]));
                    r.cross = order;
                    relax(r);
                });
                if (!changed) break;
                const n = crossCount();
                if (n < best.n) best = { n, sn: snapshot() };
            }
            restore(best.sn);
        }
        placed.forEach((r, c) => (segs[c] = [...r.inner_, ...r.cross]));
        out.segs = segs;

        // outer ribbons between families (fixed orientation, every slot faces its partner)
        const slots = {};
        segs.forEach((l, c) => l.filter(x => x.b.fam == null).forEach(x => {
            if (x.partner != null) { slots[`${x.b.k}|${c}|${x.partner}`] = [x.a0, x.a1]; return; }
            // one arc: consecutive slots, ordered by the angle of the partner
            const ro = rg(roots[c]).ro, mid = (x.a0 + x.a1) / 2;
            let a = x.a0;
            for (const t of x.slots.slice().sort((u, v) => norm(angTo(cen[c], cen[u.p]) - mid) - norm(angTo(cen[c], cen[v.p]) - mid))) {
                const w = Math.max(t.size * px, pl.floor) / ro;
                slots[`${x.b.k}|${c}|${t.p}`] = [a, a + w];
                a += w;
            }
        }));
        const entries = [];
        for (const b of mods) {
            if (b.fam != null) continue;
            for (let u = 0; u < b.fams.length; u++) for (let v = u + 1; v < b.fams.length; v++) {
                const [X, Y] = [b.fams[u], b.fams[v]], sx = slots[`${b.k}|${X}|${Y}`], sy = slots[`${b.k}|${Y}|${X}`];
                if (sx && sy) entries.push({ b, X, Y, sx, sy });
            }
        }
        const plainRibbon = ({ b, X, Y, sx, sy }) => {
                const roX = rg(roots[X]).ro, roY = rg(roots[Y]).ro;
                const x0 = ptAt(cen[X], roX, sx[0]), x1 = ptAt(cen[X], roX, sx[1]), y0 = ptAt(cen[Y], roY, sy[0]), y1 = ptAt(cen[Y], roY, sy[1]), mx = (sx[0] + sx[1]) / 2, my = (sy[0] + sy[1]) / 2, wpx = (sx[1] - sx[0]) * roX;
                const hk = `r:${b.k}:${X}:${Y}`, hm = sim.d(hk, (x0[0] + y0[0]) / 2, (x0[1] + y0[1]) / 2);
                const h = Math.max(RING.ARM_MIN, wpx * RING.ARM_W, Math.hypot(x0[0] - y0[0], x0[1] - y0[1]) * RING.ARM_D), cb = (p, an) => [p[0] + Math.cos(an) * h + hm[0], p[1] + Math.sin(an) * h + hm[1]];
                const dd = (u, v) => Math.hypot(u[0] - v[0], u[1] - v[1]);
                const mxp = ptAt(cen[X], roX, mx), myp = ptAt(cen[Y], roY, my), ccl = [mxp, cb(mxp, mx), cb(myp, my), myp];
                // one centre curve whose width runs from the slot at X to the slot at Y and never reaches 0: the two edges
                // cannot cross, so the body cannot twist whatever the drag does to the circles
                const { L, R: Rr } = band(...ccl, Math.max(dd(x0, x1), 1), Math.max(dd(y0, y1), 1));
                const last = L.length - 1;
                // each edge starts at the slot end nearest to it, and ends at the nearest end of the other slot
                const lx0 = dd(L[0], x0) <= dd(L[0], x1), ly0 = dd(L[last], y0) <= dd(L[last], y1);
                L[0] = lx0 ? x0 : x1; Rr[0] = lx0 ? x1 : x0; L[last] = ly0 ? y0 : y1; Rr[last] = ly0 ? y1 : y0;
                const A = lx0 ? L : Rr, B = lx0 ? Rr : L, arcSweep = B[last] === y0 ? 1 : 0;
                const d = `M${P(x0)} A${f1(roX)},${f1(roX)} 0 0 1 ${P(x1)} ${B.map(q => `L${P(q)}`).join(' ')} A${f1(roY)},${f1(roY)} 0 0 ${arcSweep} ${P(A[last])} ${A.slice(0, last).reverse().map(q => `L${P(q)}`).join(' ')}Z`;
                const minW = Math.min(...L.map((q, k) => dd(q, Rr[k])));
                out.stats.ribbons.push({ cl: ccl, k: b.k, X, Y, wX: dd(x0, x1), wY: dd(y0, y1), arcX: (sx[1] - sx[0]) * roX, arcY: (sy[1] - sy[0]) * roY, minW });
                add(`o:${b.k}:${X}:${Y}`, 'path', { d }, { s: { fill: b.color, stroke: 'var(--lin-card)' }, sa: { opacity: 0.5 * fade(b), 'stroke-width': 0.6 }, m: b.k, bucket: b.bucket, drag: hk, pair: { X, Y, n: b.pairRows(X, Y) } });
        };
        // a bundle: the ribbons of one cluster pair leave their slots, run side by side along the axis between the two centres and come back apart
        const bundle = list => {
            const { X, Y } = list[0], roX = rg(roots[X]).ro, roY = rg(roots[Y]).ro, cx = cen[X], cy = cen[Y];
            const dx = cy[0] - cx[0], dy = cy[1] - cx[1], dist = Math.hypot(dx, dy) || 1, u = [dx / dist, dy / dist], nrm = [-u[1], u[0]], d = Math.min(0.25 * dist, 80);
            if (dist < roX + roY + 2 * d + 10) return false;
            const hk = `rb:${X}:${Y}`, hm = sim.d(hk, (cx[0] + cy[0]) / 2, (cx[1] + cy[1]) / 2);
            const EX = [cx[0] + u[0] * (roX + d) + hm[0], cx[1] + u[1] * (roX + d) + hm[1]], EY = [cy[0] - u[0] * (roY + d) + hm[0], cy[1] - u[1] * (roY + d) + hm[1]];
            const dot = (a, b2) => a[0] * b2[0] + a[1] * b2[1];
            const st = list.map(e => {
                const mx = (e.sx[0] + e.sx[1]) / 2, my = (e.sy[0] + e.sy[1]) / 2, PX = ptAt(cx, roX, mx), PY = ptAt(cy, roY, my);
                return { ...e, mx, my, PX, PY, w: (e.sx[1] - e.sx[0]) * roX, lat: dot([PX[0] - cx[0], PX[1] - cx[1]], nrm) + dot([PY[0] - cy[0], PY[1] - cy[1]], nrm) };
            }).sort((a, b2) => a.lat - b2.lat || a.b.k - b2.b.k);
            const total = sum(st, q => q.w) + (st.length - 1);
            const shared = sum(st, q => q.b.pairRows(X, Y));
            let off = -total / 2;
            for (const q of st) {
                const c = off + q.w / 2;
                off += q.w + 1;
                const Ein = [EX[0] + nrm[0] * c, EX[1] + nrm[1] * c], Eout = [EY[0] + nrm[0] * c, EY[1] + nrm[1] * c];
                const h1 = 0.4 * Math.hypot(Ein[0] - q.PX[0], Ein[1] - q.PX[1]), h2 = 0.4 * Math.hypot(q.PY[0] - Eout[0], q.PY[1] - Eout[1]);
                const a1 = [q.PX, [q.PX[0] + Math.cos(q.mx) * h1, q.PX[1] + Math.sin(q.mx) * h1], [Ein[0] - u[0] * h1, Ein[1] - u[1] * h1], Ein];
                const a3 = [Eout, [Eout[0] + u[0] * h2, Eout[1] + u[1] * h2], [q.PY[0] + Math.cos(q.my) * h2, q.PY[1] + Math.sin(q.my) * h2], q.PY];
                const pts = [];
                for (let i = 0; i <= 8; i++) pts.push(bez(...a1, i / 8));
                for (let i = 0; i <= 8; i++) pts.push(bez(...a3, i / 8));
                const L = [], Rr = [];
                pts.forEach((p, i) => {
                    const a = pts[Math.max(0, i - 1)], b2 = pts[Math.min(pts.length - 1, i + 1)], tx = b2[0] - a[0], ty = b2[1] - a[1], tl = Math.hypot(tx, ty) || 1, nx = -ty / tl, ny = tx / tl;
                    L.push([p[0] + (nx * q.w) / 2, p[1] + (ny * q.w) / 2]); Rr.push([p[0] - (nx * q.w) / 2, p[1] - (ny * q.w) / 2]);
                });
                const x0 = ptAt(cx, roX, q.sx[0]), x1 = ptAt(cx, roX, q.sx[1]), y0 = ptAt(cy, roY, q.sy[0]), y1 = ptAt(cy, roY, q.sy[1]), last = pts.length - 1;
                const dd = (a, b2) => Math.hypot(a[0] - b2[0], a[1] - b2[1]);
                // each edge starts at the slot end nearest to it, and ends at the nearest end of the other slot
                const lx0 = dd(L[0], x0) <= dd(L[0], x1), ly0 = dd(L[last], y0) <= dd(L[last], y1);
                L[0] = lx0 ? x0 : x1; Rr[0] = lx0 ? x1 : x0; L[last] = ly0 ? y0 : y1; Rr[last] = ly0 ? y1 : y0;
                const A = lx0 ? L : Rr, B = lx0 ? Rr : L, bEndIsY0 = (lx0 ? !ly0 : ly0) ? true : false;
                // A starts at x0, B at x1; B ends at y1' = the Y end it reaches, A at the other
                const bEnd = B[last], arcSweep = bEnd === y0 ? 1 : 0;
                const dstr = `M${P(x0)} A${f1(roX)},${f1(roX)} 0 0 1 ${P(x1)} ${B.map(p => `L${P(p)}`).join(' ')} A${f1(roY)},${f1(roY)} 0 0 ${arcSweep} ${P(A[last])} ${A.slice(0, last).reverse().map(p => `L${P(p)}`).join(' ')}Z`;
                void bEndIsY0;
                out.stats.ribbons.push({ cl: [pts[0], pts[0], pts[0], pts[0]], pl: pts, k: q.b.k, X, Y, wX: dd(x0, x1), wY: dd(y0, y1), arcX: (q.sx[1] - q.sx[0]) * roX, arcY: (q.sy[1] - q.sy[0]) * roY, minW: Math.min(...L.map((p, i) => dd(p, Rr[i]))), bundle: `${X}|${Y}` });
                add(`o:${q.b.k}:${X}:${Y}`, 'path', { d: dstr }, { s: { fill: q.b.color, stroke: 'var(--lin-card)' }, sa: { opacity: 0.5 * fade(q.b), 'stroke-width': 0.6 }, m: q.b.k, bucket: q.b.bucket, drag: hk, pair: { X, Y, n: q.b.pairRows(X, Y) }, bundle: { X, Y, n: st.length, m: shared } });
            }
            return true;
        };
        const byPair = new Map();
        for (const e of entries) (byPair.get(`${e.X}|${e.Y}`) || byPair.set(`${e.X}|${e.Y}`, []).get(`${e.X}|${e.Y}`)).push(e);
        for (const list of byPair.values()) if (!((S.bundle ?? RING.BUNDLE_DEFAULT) && bundle(list))) list.forEach(plainRibbon);

        // ribbons inside clusters: a segment fans to the files of a leaf cluster, sized by slot, tapering to the file
        const inner = (center, rr, a0, a1, files, x, dragKey, famN) => {
            const b = x.b, sk = `${b.k}.${x.key}`;
            if (!files.length) return;
            const mA = (a0 + a1) / 2, fs = files.slice().sort((p, q) => norm(angTo(center, pos[p]) - mA) - norm(angTo(center, pos[q]) - mA));
            // slot of each file on the arc is proportional to the module rows it holds (floored so a thin fan stays visible)
            const cn = i => (b.cnt ? b.cnt[i] : 0) || 1, tot = sum(fs, cn), wt = fs.map(i => Math.max(cn(i), tot * 0.03)), wsum = sum(wt, v => v);
            let acc = a0;
            fs.forEach((i, n) => {
                const cov = Math.min(1, cn(i) / Math.max(1, b.size)), s0 = acc, s1 = acc + ((a1 - a0) * wt[n]) / wsum, p0 = ptAt(center, rr, s0), p1 = ptAt(center, rr, s1), t = pos[i], sm = ptAt(center, rr, (s0 + s1) / 2);
                acc = s1;
                // the fan keeps the width of its slot on the lane (no fixed landing width), so thickness follows the file's rows
                const dotD = 2 * (RING.FILE_R[0] + Math.sqrt(M.doc.nodes[i].functions || 0) * RING.FILE_R[1] * (nested ? 1 : 1.4)), we = Math.min(dotD, Math.max(1.2, Math.hypot(p1[0] - p0[0], p1[1] - p0[1])));
                const dxt = t[0] - sm[0], dyt = t[1] - sm[1], dl = Math.hypot(dxt, dyt) || 1, ud = [dxt / dl, dyt / dl], nr = [-ud[1], ud[0]], uin = [(center[0] - sm[0]) / rr, (center[1] - sm[1]) / rr], hh = Math.max(14, dl * 0.4);
                const sd = (p0[0] - sm[0]) * nr[0] + (p0[1] - sm[1]) * nr[1] > 0 ? 1 : -1, fa = [t[0] + nr[0] * we / 2 * sd, t[1] + nr[1] * we / 2 * sd], fb = [t[0] - nr[0] * we / 2 * sd, t[1] - nr[1] * we / 2 * sd];
                const ar = (p, u) => [p[0] + u[0] * hh, p[1] + u[1] * hh], br = f => [f[0] - ud[0] * hh, f[1] - ud[1] * hh];
                add(`i:${sk}:${dragKey}:${i}`, 'path', { d: `M${P(p0)} A${f1(rr)},${f1(rr)} 0 0 1 ${P(p1)} C${P(ar(p1, uin))} ${P(br(fb))} ${P(fb)} L${P(fa)} C${P(br(fa))} ${P(ar(p0, uin))} ${P(p0)}Z` }, { s: { fill: b.color }, sa: { opacity: ((0.15 + 0.5 * cov) * fade(b)).toFixed(3) }, m: b.k, bucket: b.bucket, drag: dragKey, f: i, fcov: { i, n: cn(i) } });
            });
        };
        // routing: family ring -> lane on each cluster level -> files; a cluster without sub-clusters fans straight to its files
        const holdersIn = (n, x) => n.files.filter(i => x.hold.has(i));
        // a sub-cluster's share of a module: wt = rows summed over its holder files (splits the parent slot), cov = the largest holder's
        // share of the module (a lower bound of what the sub-cluster holds; sizes its lane)
        const kidShare = (K, x) => {
            const c = x.b.cnt, h = holdersIn(K, x), v = h.map(i => (c ? c[i] : 0) || 1);
            return { wt: Math.max(1, sum(v, t => t)), cov: Math.max(0.08, Math.min(1, Math.max(...v) / Math.max(1, x.b.size))) };
        };
        const splitKids = (here, x, span) => {
            const sh = here.map(K => kidShare(K, x)), tot = sum(sh, q => q.wt);
            let acc = 0;
            return sh.map(q => { const w = (span * q.wt) / tot, r = { off: acc + w / 2, w, cov: q.cov }; acc += w; return r; });
        };
        const kidsOf = X => (nested ? X.kids : []);
        let FM = new Map();
        const feedsOf = X => FM.get(X.id) || FM.set(X.id, []).get(X.id);
        // the lane ring of a cluster: one lane per feed, then down to the lanes of the sub-clusters that hold it, or to the files
        // a band bends away from the circles of the clusters it is not related to (siblings of its target and of the target's ancestors)
        const avoid = (X, P0, c0, c1, P1) => {
            const obs = [];
            for (let A = X; A.parent; A = A.parent) for (const K of A.parent.kids) if (K !== A && nc[K.id]) obs.push({ c: nc[K.id], r: K.r + RING.AVOID_MARGIN });
            if (!obs.length) return [c0, c1];
            const o0 = c0, o1 = c1, cap = RING.AVOID_BEND * Math.hypot(P1[0] - P0[0], P1[1] - P0[1]);
            c0 = c0.slice(); c1 = c1.slice();
            for (let it = 0; it < 4; it++) {
                let moved = false;
                for (let k = 3; k <= 9; k++) {
                    const t = k / 12, pt = bez(P0, c0, c1, P1, t), b0 = 3 * t * (1 - t) * (1 - t), b1 = 3 * t * t * (1 - t), nn = b0 * b0 + b1 * b1;
                    for (const o of obs) {
                        const dx = pt[0] - o.c[0], dy = pt[1] - o.c[1], d = Math.hypot(dx, dy) || 1, pen = o.r - d;
                        if (pen <= 0) continue;
                        c0[0] += ((dx / d) * pen * b0) / nn; c0[1] += ((dy / d) * pen * b0) / nn; c1[0] += ((dx / d) * pen * b1) / nn; c1[1] += ((dy / d) * pen * b1) / nn;
                        moved = true;
                    }
                }
                if (!moved) break;
            }
            // a gentle bend only: never move a control point further than AVOID_BEND of the band's length
            const lim = (c, o) => { const dx = c[0] - o[0], dy = c[1] - o[1], m = Math.hypot(dx, dy); return m > cap ? [o[0] + (dx * cap) / m, o[1] + (dy * cap) / m] : c; };
            return [lim(c0, o0), lim(c1, o1)];
        };
        // the stems keep their inward / outward lean, blended with the chord so the curve stays an S without overshooting the child
        const softStems = (P0, P1, u0, u1, h) => {
            const dx = P1[0] - P0[0], dy = P1[1] - P0[1], dl = Math.hypot(dx, dy) || 1, d = [dx / dl, dy / dl], k = Math.min(h, dl / 3);
            const mix = (u, v) => { const m = [u[0] + v[0], u[1] + v[1]], ml = Math.hypot(m[0], m[1]) || 1; return [m[0] / ml, m[1] / ml]; };
            const a = mix(u0, d), b = mix(u1, [-d[0], -d[1]]);
            return [[P0[0] + a[0] * k, P0[1] + a[1] * k], [P1[0] + b[0] * k, P1[1] + b[1] * k]];
        };
        // L / R sides of a band, their two ends moved onto the start arc (parent ring, centred on arc.m, width w0) and the end arc (lane, a0..a1), with the arcs as caps
        const arcEnds = (bd, arc, w0, C1, r1, a0, a1) => {
            const L = bd.L.map(q => q.slice()), R = bd.R.map(q => q.slice()), n = L.length - 1, cap = (c, r, f, t) => [1, 2, 3].map(i => ptAt(c, r, f + ((t - f) * i) / 4));
            const near = (A, B, S0, S1) => Math.hypot(A[0] - S0[0], A[1] - S0[1]) + Math.hypot(B[0] - S1[0], B[1] - S1[1]);
            const shift = (A, B, ang0, ang1, c, r, atEnd) => {
                let e0 = ptAt(c, r, ang0), e1 = ptAt(c, r, ang1), aA = ang0, aB = ang1;
                if (near(A[atEnd ? n : 0], B[atEnd ? n : 0], e0, e1) > near(A[atEnd ? n : 0], B[atEnd ? n : 0], e1, e0)) { [e0, e1] = [e1, e0]; [aA, aB] = [aB, aA]; }
                const i0 = atEnd ? n : 0, dA = [e0[0] - A[i0][0], e0[1] - A[i0][1]], dB = [e1[0] - B[i0][0], e1[1] - B[i0][1]];
                for (let k = 0; k <= n; k++) { const t = atEnd ? k / n : 1 - k / n, wt = t * t * t; A[k][0] += dA[0] * wt; A[k][1] += dA[1] * wt; B[k][0] += dB[0] * wt; B[k][1] += dB[1] * wt; }
                return [aA, aB];
            };
            const [eA, eB] = shift(L, R, a0, a1, C1, r1, true);
            const endCap = cap(C1, r1, eA, eB);
            let startCap = [];
            if (arc) { const hw = w0 / 2 / arc.r, [sA, sB] = shift(L, R, arc.m - hw, arc.m + hw, arc.c, arc.r, false); startCap = cap(arc.c, arc.r, sB, sA); }
            return L.concat(endCap, R.reverse(), startCap);
        };
        const laneRing = (X, feeds) => {
            if (!feeds.length) return;
            const C = nc[X.id], rp = X.r + (X.level === 0 ? RING.ROOT_LANE_OFF : RING.LANE_OFF), ports = feeds.map(fd => ({ ...fd, wp: Math.max(fd.x.size, 1) * M.lanePx * (fd.cov ?? 1) }));
            const fsc = Math.min(1, (RING.LANE_FIT * TAU * rp - ports.length * RING.LANE_GAP * rp) / sum(ports, p => p.wp));
            out.stats.laneFit = Math.min(out.stats.laneFit, fsc);
            ports.forEach(p => { p.wp *= fsc; p.w = p.wp / rp; p.c = ((angTo(C, p.from) % TAU) + TAU) % TAU; });
            const lk = q => `${q.x.b.k}.${q.x.key}:${X.id}`, arrange = list => list.sort((a, b) => a.c - b.c);
            // lanes of shared modules (fed from a ring above) stay together on the side facing their source; lanes of modules held inside this cluster only
            // (noBand) are moved out of that sector, so the two kinds of link do not run in the same direction
            const relax = (list, fixed) => {
                for (let it = 0; it < 80 && list.length > 1; it++) {
                    for (let k = 0; k < list.length; k++) {
                        const j = (k + 1) % list.length, A = list[k], B = list[j], dj = B.c - A.c + (j === 0 ? TAU : 0), gap = dj - (A.w + B.w) / 2 - RING.LANE_GAP;
                        if (gap >= 0) continue;
                        const fa = fixed && fixed(A), fb = fixed && fixed(B);
                        if (fa && fb) continue;
                        if (fa) B.c -= gap; else if (fb) A.c += gap; else { A.c += gap / 2; B.c -= gap / 2; }
                    }
                }
            };
            const shared = ports.filter(q => !q.noBand), own = ports.filter(q => q.noBand);
            // on a family's own ring the cross-family segments of the outer ring are blocked too: their fans come down to the files in that direction
            const outer = X.level === 0 ? (segs[X.famIdx] || []).filter(x => x.a1 > x.a0) : [];
            if ((shared.length || outer.length) && own.length) {
                arrange(shared);
                relax(shared);
                // The shared lanes block their angles (each with a margin). Lone lanes whose natural angle falls in a blocked stretch go to the biggest free arc of
                // the ring (the nearest point of it), so they leave toward where the room is, not toward the nearest edge of a possibly wide, mixed sector.
                const blocked = [], blockers = shared.map(q => [q.c, q.w / 2]).concat(outer.map(x => [(x.a0 + x.a1) / 2, (x.a1 - x.a0) / 2]));
                for (const [bc, bw] of blockers) { const h = bw + RING.LANE_GAP + RING.OWN_MARGIN, c0 = ((bc % TAU) + TAU) % TAU; blocked.push([c0 - h, c0 + h]); if (c0 - h < 0) blocked.push([c0 - h + TAU, c0 + h + TAU]); if (c0 + h > TAU) blocked.push([c0 - h - TAU, c0 + h - TAU]); }
                blocked.sort((u, v) => u[0] - v[0]);
                const merged = [];
                for (const iv of blocked) { const last = merged[merged.length - 1]; if (last && iv[0] <= last[1]) last[1] = Math.max(last[1], iv[1]); else merged.push(iv.slice()); }
                // free arcs between the blocked stretches, taken over one turn [0, TAU) (the stretches already cover the wrap through the copies above)
                const free = [];
                let at = 0;
                for (const iv of merged) { if (iv[1] <= 0) continue; if (iv[0] > at) free.push([at, Math.min(iv[0], TAU)]); at = Math.max(at, iv[1]); if (at >= TAU) break; }
                if (at < TAU) free.push([at, TAU]);
                if (free.length > 1 && free[0][0] === 0 && free[free.length - 1][1] === TAU) { const f = free.pop(); free[0] = [f[0] - TAU, free[0][1]]; }
                for (const q of own) {
                    const h = q.w / 2, fit = free.filter(f => f[1] - f[0] >= 2 * h);
                    if (!fit.length) continue;
                    const inFree = fit.some(f => [0, TAU, -TAU].some(o => q.c + o >= f[0] + h && q.c + o <= f[1] - h));
                    if (inFree) continue;
                    const big = fit.reduce((m, f) => (f[1] - f[0] > m[1] - m[0] ? f : m)), lo = big[0] + h, hi = big[1] - h;
                    q.c = [0, TAU, -TAU].map(o => Math.min(hi, Math.max(lo, q.c + o))).reduce((m, v) => (Math.abs(norm(v - q.c)) < Math.abs(norm(m - q.c)) ? v : m));
                }
                ports.forEach(q => { q.c = ((q.c % TAU) + TAU) % TAU; });
                arrange(ports);
                relax(ports, q => !q.noBand);
            } else { arrange(ports); relax(ports); }
            ports.forEach(q => { q.c = ((q.c % TAU) + TAU) % TAU; });
            // a lane eases to its angle over a few frames (sim.lane), so a swap of neighbours or a change of side while dragging glides instead of jumping;
            // the remaining distance is reported to the redraw loop (sim.laneRes) so it keeps running until the lanes arrive
            for (const q of ports) {
                const k = lk(q), prev = sim.lane[k];
                if (prev != null) {
                    const diff = norm(q.c - prev), mv = Math.abs(diff) < 0.004 ? diff : diff * RING.LANE_EASE;
                    q.c = prev + mv;
                    sim.laneRes += Math.abs(diff - mv) * rp;
                }
                sim.lane[k] = q.c;
            }
            const kids = kidsOf(X);
            ports.forEach(p => {
                // below a family's own ring the modules held only in this cluster get a ring of their own, just inside the shared lanes
                const b = p.x.b, sk = `${b.k}.${p.x.key}`, a0 = p.c - p.w / 2, a1 = p.c + p.w / 2, rl = p.noBand && X.level > 0 ? rp - RING.OWN_OFF : rp;
                if (!light && !p.noBand) {
                    const P1 = ptAt(C, rp + 4, p.c), P0 = p.from, dist = Math.hypot(P1[0] - P0[0], P1[1] - P0[1]), h = Math.max(20, dist * 0.4), cl = Math.hypot(p.centre[0] - P0[0], p.centre[1] - P0[1]) || 1, u0 = [(p.centre[0] - P0[0]) / cl, (p.centre[1] - P0[1]) / cl], u1 = [Math.cos(p.c), Math.sin(p.c)];
                    const w0 = Math.min(Math.max(p.slot, p.wp * 0.8), p.wp * 2, p.slot > 0 ? p.slot : Infinity, RING.LANE_FIT * TAU * rp), [k0, k1] = avoid(X, P0, ...softStems(P0, P1, u0, u1, h), P1), bd = band(P0, k0, k1, P1, w0, p.wp, 16);
                    // both ends follow the round arcs: the start sits on the parent's ring, the end on this lane, instead of a straight cut
                    const poly = arcEnds(bd, p.arc, w0, C, rp + 4, a0, a1);
                    add(`u:${sk}:${X.id}:${p.srcId}`, 'path', { d: polyD(poly) }, { s: { fill: b.color }, sa: { opacity: 0.34 * fade(b) }, m: b.k, bucket: b.bucket, drag: p.srcDrag, lane: X.id, src: p.srcId, sk });
                }
                const cv = lcov(b, X), cov = cv.t ? cv.n / cv.t : 1, part = cv.t && cov < RING.CORE_MIN, hatch = part ? hatchOf(b, cov) : null;
                if (part) add(`lg:${sk}:${X.id}`, 'path', { d: `M${P(ptAt(C, rl, a0))} A${f1(rl)},${f1(rl)} 0 0 1 ${P(ptAt(C, rl, a1))}` }, { s: { stroke: b.color }, sa: { fill: 'none', 'stroke-width': RING.LANE_STROKE, 'stroke-linecap': 'round', opacity: RING.GHOST * fade(b) }, m: b.k, pe: true });
                add(`l:${sk}:${X.id}`, 'path', { d: `M${P(ptAt(C, rl, a0))} A${f1(rl)},${f1(rl)} 0 0 1 ${P(ptAt(C, rl, a1))}` }, { s: { stroke: b.color }, hatch, sa: { fill: 'none', 'stroke-width': b.bucket ? RING.LANE_STROKE * 0.55 : RING.LANE_STROKE, 'stroke-linecap': 'round', opacity: fade(b) }, m: b.k, bucket: b.bucket, drag: key(X), lane: X.id, lc: b.bucket ? undefined : cv, sk, own: p.noBand, pair: p.x.partner != null ? { X: Math.min(X.famIdx, p.x.partner), Y: Math.max(X.famIdx, p.x.partner), n: p.x.size } : undefined, pairs: p.x.partner == null && p.x.slots ? p.x.slots.map(t => ({ X: Math.min(X.famIdx, t.p), Y: Math.max(X.famIdx, t.p), n: t.size })) : undefined });
                out.stats.lanes.push({ k: b.k, node: X.id, w: p.w, wp: p.wp, rp: rl, own: p.noBand });
                if (kids.length) {
                    const here = kids.filter(K => holdersIn(K, p.x).length).sort((u, v) => norm(angTo(C, nc[u.id]) - p.c) - norm(angTo(C, nc[v.id]) - p.c));
                    const sp = splitKids(here, p.x, p.w);
                    here.forEach((K, n) => feedsOf(K).push({ x: p.x, from: ptAt(C, rl - 3.5, a0 + sp[n].off), slot: sp[n].w * (rl - 3.5), cov: sp[n].cov, srcDrag: key(X), srcId: `${X.id}`, centre: C, arc: { c: C, r: rl - 3.5, m: a0 + sp[n].off } }));
                } else if (!agg(X)) inner(C, rl - 3.5, a0, a1, holdersIn(X, p.x), p.x, key(X), M.famFiles[X.famIdx]);
            });
        };
        const walk = X => { laneRing(X, feedsOf(X)); kidsOf(X).forEach(walk); };
        if (!lean) roots.forEach((f, c) => {
            FM = new Map();
            const { ri } = rg(f);
            // single-cluster modules start at the lowest cluster that holds them all, with no band from the outer ring
            for (const x of pl.layers[c]) {
                const X = lcaNode(f, x.hold, nested), hs = [...x.hold];
                feedsOf(X).push({ x, from: [sum(hs, i => pos[i][0]) / hs.length, sum(hs, i => pos[i][1]) / hs.length], slot: 0, srcDrag: key(X), srcId: X.id, centre: nc[X.id], noBand: true });
            }
            // cross-family arcs of the outer ring: straight to the files, or down through the sub-clusters
            if (!nested || !f.kids.length) { if (!agg(f)) segs[c].forEach(x => inner(cen[c], ri, x.a0, x.a1, [...x.hold], x, `f:${f.id}`, M.famFiles[c])); }
            else segs[c].forEach(x => {
                const mA = (x.a0 + x.a1) / 2, here = f.kids.filter(K => holdersIn(K, x).length).sort((u, v) => norm(angTo(cen[c], nc[u.id]) - mA) - norm(angTo(cen[c], nc[v.id]) - mA));
                const sp = splitKids(here, x, x.a1 - x.a0);
                here.forEach((K, n) => feedsOf(K).push({ x, from: ptAt(cen[c], ri, x.a0 + sp[n].off), slot: sp[n].w * ri, cov: sp[n].cov, srcDrag: `f:${f.id}`, srcId: `${f.id}`, centre: cen[c], arc: { c: cen[c], r: ri, m: x.a0 + sp[n].off } }));
            });
            walk(f);
        });

        // files
        if (!lean) for (let i = 0; i < M.N; i++) {
            if (aggOf[i]) continue;
            const rr = RING.FILE_R[0] + Math.sqrt(M.doc.nodes[i].functions || 0) * RING.FILE_R[1] * (nested ? 1 : 1.4);
            add(`n:${i}`, 'circle', { cx: f1(pos[i][0]), cy: f1(pos[i][1]), r: f1(rr) }, { s: { fill: M.nodeColor(i), stroke: 'var(--lin-card)' }, sa: { 'stroke-width': 1.5 }, cls: 'hold', h: (holdsOf.get(i) || []).join(' '), drag: nk(i), file: i });
            if (!light && (M.N <= RING.HIDE_FILES || (S.focusSet && S.focusSet.has(i)))) add(`nl:${i}`, 'text', { x: f1(pos[i][0]), y: f1(pos[i][1] + 3), 'text-anchor': 'middle', 'font-size': RING.FILE_FONT }, { text: (M.doc.nodes[i].file_name || M.doc.nodes[i].id).slice(0, 4), cls: 'ring-fl', lab: { file: i, r: rr } });
        }

        // family rings (rounded arcs) and labels
        roots.forEach((f, c) => {
            const { rm, ro } = rg(f);
            segs[c].forEach(x => {
                const pd = Math.min(RING.SEG_PAD, (x.a1 - x.a0) * 0.2), sw = x.b.bucket ? RING.SEG_STROKE * 0.5 : RING.SEG_STROKE, cap = sw / 2 / rm, mdl = (x.a0 + x.a1) / 2, a0 = Math.min(x.a0 + pd + cap, mdl - 0.002), a1 = Math.max(x.a1 - pd - cap, mdl + 0.002);
                const cv = lcov(x.b, f), cov = cv.t ? cv.n / cv.t : 1, part = cv.t && cov < RING.CORE_MIN;
                if (part) add(`sg:${c}:${x.b.k}:${x.key}`, 'path', { d: `M${P(ptAt(cen[c], rm, a0))} A${f1(rm)},${f1(rm)} 0 ${a1 - a0 > Math.PI ? 1 : 0} 1 ${P(ptAt(cen[c], rm, a1))}` }, { s: { stroke: x.b.color }, sa: { fill: 'none', 'stroke-width': sw, 'stroke-linecap': 'round', opacity: RING.GHOST * fade(x.b) }, m: x.b.k, pe: true });
                add(`s:${c}:${x.b.k}:${x.key}`, 'path', { d: `M${P(ptAt(cen[c], rm, a0))} A${f1(rm)},${f1(rm)} 0 ${a1 - a0 > Math.PI ? 1 : 0} 1 ${P(ptAt(cen[c], rm, a1))}` }, { s: { stroke: x.b.color }, hatch: part ? hatchOf(x.b, cov) : null, sa: { fill: 'none', 'stroke-width': sw, 'stroke-linecap': 'round', opacity: fade(x.b) }, m: x.b.k, bucket: x.b.bucket, drag: `f:${f.id}`, seg: c, lc: x.b.bucket ? undefined : cv, pair: x.partner != null ? { X: Math.min(c, x.partner), Y: Math.max(c, x.partner), n: x.size } : undefined, pairs: x.partner == null && x.slots ? x.slots.map(t => ({ X: Math.min(c, t.p), Y: Math.max(c, t.p), n: t.size })) : undefined });
            });
            const [tx, ty] = ptAt(cen[c], ro + 40, nF > 1 ? angTo(ctr, cen[c]) : -Math.PI / 2);
            const fname = f.label.length > 24 ? `${f.label.slice(0, 23)}\u2026` : f.label;
            add(`fl:${c}`, 'text', { x: f1(tx), y: f1(ty), 'text-anchor': 'middle', 'font-size': 14, 'font-weight': 600 }, { text: fname, cls: 'ring-fam', s: { fill: f.famCol }, drag: `f:${f.id}` });
            add(`fn:${c}`, 'text', { x: f1(tx), y: f1(ty + 14), 'text-anchor': 'middle', 'font-size': 10 }, { text: `${f.files.length} files`, cls: 'ring-fam ring-famn', drag: `f:${f.id}` });
        });
        // which top-level cluster an element belongs to (a ribbon: both of its ends), for the fade of the others when one is focused
        for (const e of els) {
            const k = e.id.slice(0, e.id.indexOf(':'));
            if (e.cl) e.fam = e.cl.famIdx;
            else if (k === 'o') e.fams = [e.pair.X, e.pair.Y];
            else if (e.lane) e.fam = M.byId.get(e.lane).famIdx;
            else if (k === 'i') e.fam = M.famOf[e.f];
            else if (k === 'n') e.fam = M.famOf[e.file];
            else if (k === 'nl') e.fam = M.famOf[Number(e.id.slice(3))];
            else if (k === 's') e.fam = e.seg;
            else if (k === 'fl') e.fam = Number(e.id.slice(3));
        }
        // tier buckets sit behind the real lanes: same kinds of element, buckets first
        for (const kind of ['o:', 'u:', 'l:', 'i:']) {
            const idx = [];
            els.forEach((e, i) => { if (e.id.startsWith(kind)) idx.push(i); });
            const vals = idx.map(i => els[i]);
            if (!vals.some(e => e.bucket)) continue;
            vals.sort((a, b2) => (b2.bucket ? 1 : 0) - (a.bucket ? 1 : 0));
            idx.forEach((i, k) => (els[i] = vals[k]));
        }
        return out;
    }

    // every attribute / style number or string of a frame must be finite
    function badAttrs(els) {
        const bad = [];
        for (const e of els) for (const bag of [e.a, e.s, e.sa]) for (const [k, v] of Object.entries(bag || {})) if (v != null && /NaN|undefined|Infinity/.test(String(v))) bad.push(`${e.id}.${k}=${v}`);
        return bad;
    }

    // The view the page opens in when nobody picked one: Ring for many builds in several clusters, else Tree.
    function autoView(doc) {
        const fams = clusterTree(doc).roots.filter(f => !f.other), files = sum(fams, f => f.files.length), A = RING.AUTO;
        return fams.length >= A.FAMILIES || (fams.length >= A.FILES_FAMILIES && files >= A.FILES) ? 'ring' : 'tree';
    }
    // URL lview wins, then the remembered pick, then the rule above (auto: true, never stored)
    function chooseView(urlView, remembered, doc) {
        const ok = v => ['tree', 'tracks', 'split', 'ring'].includes(v);
        if (ok(urlView)) return { view: urlView, auto: false };
        if (ok(remembered)) return { view: remembered, auto: false };
        return { view: autoView(doc), auto: true };
    }

    // ---------------------------------------------------------------- mount
    const CSS = `
.ring-root.hlm [data-m]:not(.on),.ring-root.hlm [data-h]:not(.on){opacity:.1!important}
.ring-root circle.hold.on{stroke:var(--hc);stroke-width:3.5}
.ring-root .ring-sub{opacity:.85;paint-order:stroke;stroke:var(--lin-card);stroke-width:3px;pointer-events:none}
.ring-root .ring-fl{fill:#fff;paint-order:stroke;stroke:rgba(0,0,0,.4);stroke-width:2px;pointer-events:none}
.ring-root .ring-famn{font-weight:400;opacity:.6;fill:var(--text,#f8f8f2)!important}
.ring-root .ring-fam{fill:var(--text,#f8f8f2);paint-order:stroke;stroke:var(--lin-card);stroke-width:3px;cursor:grab}
.ring-root [data-drag]{cursor:grab}
.ring-root circle[data-drag^="f:"]{cursor:pointer}
.ring-root .ring-hide{display:none}
.ring-root .ring-dim{opacity:.15!important}
.ring-root .on.ring-dim{opacity:1!important}
.ring-root circle.ring-plus{stroke:#fff;stroke-width:1.5;cursor:pointer}
.ring-root circle.ring-plus:hover{stroke-width:3}
.ring-root circle.ring-pending{stroke-dasharray:10 6;stroke-width:5;fill-opacity:.35;animation:ring-march .6s linear infinite,ring-pulse 1s ease-in-out infinite}
@keyframes ring-march{to{stroke-dashoffset:-16}}
@keyframes ring-pulse{50%{stroke-opacity:.35;fill-opacity:.1}}
.ring-note.busy::before{content:"";display:inline-block;width:11px;height:11px;margin-right:7px;vertical-align:-1px;border:2px solid currentColor;border-top-color:transparent;border-radius:50%;animation:ring-spin .7s linear infinite}
@keyframes ring-spin{to{transform:rotate(360deg)}}
.ring-root .ring-new{animation:ring-fade .6s ease-out}
@keyframes ring-fade{from{opacity:0}}
.ring-crumb{position:absolute;left:12px;top:10px;z-index:3;display:flex;align-items:center;gap:4px;flex-wrap:wrap;max-width:60%}
.ring-root [data-m]{cursor:pointer}
.ring-note{position:absolute;left:12px;bottom:12px;max-width:70%;padding:4px 10px;border:1px solid var(--lin-line);border-radius:8px;background:var(--lin-card);font-size:12px;opacity:.95;pointer-events:none}
body.ring-dragging,body.ring-dragging .ring-root [data-drag]{cursor:grabbing}
`;
    function ensureCss() {
        if (document.getElementById('nway-ring-css')) return;
        const s = document.createElement('style');
        s.id = 'nway-ring-css';
        s.textContent = CSS;
        document.head.appendChild(s);
    }

    /**
     * ctx: { g (svg group inside the zoomed world), svg (the svg element), host (the canvas div, for the note),
     *   getDoc(), getR(), getCats() -> lensCats or null, getTrack(), setTrack(tk), onFile(nodeId),
     *   showTip(ev, title, colour, rows), moveTip(ev), hideTip(), fmt, name(id), nodeColor(i), tree(), hsl(h, l), hue(i),
     *   grey, controls (the always-visible controls), controls2 (the controls the page puts behind its "More"),
     *   camera { get() -> the current zoom transform, to(box [x0, y0, x1, y1]) animates to it, restore(transform) },
 *   openNodes(ids) -> Promise of a lineage doc with those merged nodes opened, merge(doc) puts it in (the view state stays),
 *   opts: { nest, arc: 'partner' | 'group' | 'one', min (percent), list }, onOpt({nest, arc, min, list}), fit() }
     */
    function mount(ctx) {
        ensureCss();
        const root = document.createElementNS(NS, 'g');
        root.setAttribute('class', 'ring-root');
        ctx.g.appendChild(root);
        const note = document.createElement('div');
        note.className = 'ring-note';
        note.style.display = 'none';
        ctx.host.appendChild(note);
        const crumb = document.createElement('div');
        crumb.className = 'ring-crumb';
        crumb.style.display = 'none';
        ctx.host.appendChild(crumb);
        const S = { sim: newSim(), nested: ctx.opts.nest !== false, open: new Set(), fc: null, fcTok: 0, focusSet: null, pend: null, err: '', noteBase: '', fadeNew: false, arc: ['partner', 'group', 'one'].includes(ctx.opts.arc) ? ctx.opts.arc : RING.ARC_DEFAULT, min: ctx.opts.min ?? RING.MIN_PCT, list: !!ctx.opts.list, tier: ctx.opts.tier === 'hide' ? 'hide' : 'show', bundle: ctx.opts.bundle ?? RING.BUNDLE_DEFAULT };
        const opt = () => ({ nest: S.nested, arc: S.arc, min: S.min, list: S.list, tier: S.tier, bundle: S.bundle });
        let M = null, key = '', nodes = new Map(), ids = [], raf = 0, dead = false, press = null, frame = null, mods = [], cur = { mode: 0, est: 0 };
        let byMod = new Map(), holdEls = new Map(), lit = [];
        const mkEl = (tag, text) => { const e = document.createElementNS(NS, tag); if (text != null) e.textContent = text; return e; };

        // ---- controls: the lineage page's own segmented switches (.lin-seg, buttons with aria-pressed); the tunables sit behind "More"
        const seg = (label, tip, options, get, set) => {
            const wrap = document.createElement('span'), lab = document.createElement('span'), box = document.createElement('div');
            wrap.setAttribute('style', 'display:inline-flex;align-items:center;gap:6px');
            lab.className = 'lin-dim';
            lab.textContent = label;
            box.className = 'lin-seg';
            box.setAttribute('role', 'group');
            box.setAttribute('aria-label', label);
            box.setAttribute('title', tip);
            const btns = options.map(([v, text]) => {
                const b = document.createElement('button');
                b.setAttribute('type', 'button');
                b.textContent = text;
                b.addEventListener('click', () => { set(v); paintSegs(); });
                box.appendChild(b);
                return [v, b];
            });
            const paint = () => btns.forEach(([v, b]) => b.setAttribute('aria-pressed', String(String(get()) === String(v))));
            wrap.append(lab, box);
            segs.push(paint);
            paint();
            return wrap;
        };
        const segs = [], paintSegs = () => segs.forEach(f => f());
        const onOff = (label, tip, get, set) => seg(label, tip, [[false, 'Off'], [true, 'On']], get, set);
        const applyOpt = rebuild => { ctx.onOpt(opt()); if (rebuild) { M = null; S.sim.reset(); full(); ctx.fit(); } };
        const minSeg = seg('Min size', 'Hide modules and lanes with fewer functions than this share of all functions in the set.', RING.MIN_OPTS.map(v => [v, v ? `${v}%` : 'Off']), () => S.min, v => { S.min = v; applyOpt(true); });
        if (!RING.MIN_OPTS.includes(S.min)) S.min = RING.MIN_PCT;
        const resetBtn = document.createElement('button');
        resetBtn.setAttribute('type', 'button');
        resetBtn.className = 'lin-btn';
        resetBtn.textContent = 'Reset layout';
        resetBtn.title = 'Put every circle and ribbon back where it was computed';
        resetBtn.addEventListener('click', () => { S.sim.reset(); S.open.clear(); full(); ctx.fit(); });
        const arcSeg = seg('Arcs', 'How many ring segments a module gets on a circle: one per partner family, one per group of nearby partners, or one for all of them', [['partner', 'Per partner'], ['group', 'Grouped'], ['one', 'One per module']], () => S.arc, v => { S.arc = v; applyOpt(true); });
        const nestSeg = onOff('Nested clusters', 'Show the sub-clusters of each family as circles inside it, with lanes on each level', () => S.nested, v => { S.nested = v; S.sim.reset(); applyOpt(false); full(); ctx.fit(); });
        const listSeg = onOff('Module list', 'Show the chips of the lens groups above the chart', () => S.list, v => { S.list = v; applyOpt(false); });
        const tierSeg = seg('Tier buckets', 'The Core / Common / Rare remainders of the Lanes lens (rows that are in no lane): shown in grey behind the lanes, or hidden', [['show', 'Show'], ['hide', 'Hide']], () => S.tier, v => { S.tier = v; applyOpt(true); });
        const bundleSeg = seg('Bundle ribbons', 'Draw the ribbons between two clusters as one bundle of parallel stripes, one per module', [[true, 'On'], [false, 'Off']], () => S.bundle, v => { S.bundle = v; applyOpt(false); paint(); });
        ctx.controls.replaceChildren(minSeg, resetBtn);
        ctx.controls2.replaceChildren(arcSeg, nestSeg, bundleSeg, tierSeg, listSeg);
        
        // ---- tooltips
        const modTip = (ev, b, pr, prs, bu, fc, lc) => {
            if (!b) return;
            const doc = M.doc, names = b.mem.slice(0, 6).map(i => ctx.name(doc.nodes[i].id)).join(', ') + (b.mem.length > 6 ? ` +${b.mem.length - 6} more` : '');
            const lab = c => M.roots[c].label;
            ctx.showTip(ev, b.label, b.color, [
                pr ? ['Shared', `${ctx.fmt(pr.n)} functions shared between ${lab(pr.X)} and ${lab(pr.Y)} (of ${ctx.fmt(b.size)} in the module)`] : ['Functions', ctx.fmt(b.size)],
                ...(prs ? prs.slice(0, 8).map(q => ['Shared', `${ctx.fmt(q.n)} between ${lab(q.X)} and ${lab(q.Y)} (of ${ctx.fmt(b.size)} in the module)`]) : []),
                ...(bu ? [['Bundle', `${M.roots[bu.X].label} + ${M.roots[bu.Y].label}: ${bu.n} modules, ${ctx.fmt(bu.m)} shared functions`]] : []),
                ...(fc ? [['This file', `${ctx.name(M.doc.nodes[fc.i].id)} holds ${ctx.fmt(fc.n)} of ${ctx.fmt(b.size)} (${Math.round((100 * fc.n) / Math.max(1, b.size))}%)`]] : []),
                ...(lc && lc.t > 1 ? [['Role', `${tierText(lc.n, lc.t)} in ${lc.name || 'this cluster'}: ${lc.n} of ${lc.t} files (${Math.round((100 * lc.n) / lc.t)}%)`]] : []),
                ...(b.fam != null ? [['Only in cluster', M.roots[b.fam].label]] : []),
                ['Held by', `${b.mem.length} file${b.mem.length === 1 ? '' : 's'}${b.fams.length > 1 ? ` in ${b.fams.length} families` : ''}`], ['Files', names],
                ['', `A file holds a module when it has at least one of its functions. Core: ${Math.round(100 * RING.CORE_MIN)}% or more of a cluster's files, Common: ${Math.round(100 * RING.COMMON_MIN)}% or more, Rare: fewer.`]]);
        };
        const fileTip = (ev, i) => {
            const n = M.doc.nodes[i], held = mods.filter(b => b.mem.includes(i)).sort((a, b) => b.size - a.size);
            // the glyph of each module is its tier inside the file's own (deepest) cluster
            let leaf = M.roots[M.famOf[i]];
            for (let k = leaf.kids.find(x => x.files.includes(i)); k; k = leaf.kids.find(x => x.files.includes(i))) leaf = k;
            const lset = new Set(leaf.files), glyph = b => (leaf.files.length > 1 ? `${tierText(b.mem.filter(j => lset.has(j)).length, leaf.files.length)}  ` : '');
            ctx.showTip(ev, ctx.name(n.id), M.nodeColor(i), [['Functions', ctx.fmt(n.functions)], ...(M.doc.columns_mode === 'children' && n.members && n.members.length > 1 ? [['Files', `${n.members.length} files (cluster)`]] : []), ['Cluster', M.roots[M.famOf[i]].label], ...held.slice(0, 6).map(b => [b.label, `${glyph(b)}${ctx.fmt(b.size)}`]), ...(held.length > 6 ? [['', `+${held.length - 6} more modules`]] : [])]);
        };

        // ---- hover / pin: a module's chain lights, the rest fades. The lists are made once per layout, so a hover touches only its own elements.
        const act = k => {
            root.classList.add('hlm');
            const b = mods.find(x => x.k === k);
            root.style.setProperty('--hc', b ? b.color : 'var(--accent)');
            for (const n of [...(byMod.get(k) || []), ...(holdEls.get(k) || [])]) { n.classList.add('on'); lit.push(n); }
        };
        const clear = () => { root.classList.remove('hlm'); for (const n of lit) n.classList.remove('on'); lit = []; };
        const pinned = () => { const t = ctx.getTrack(); const b = t && mods.find(x => x.tk === t); return b ? b.k : null; };
        const rest = () => { clear(); const p = pinned(); if (p != null) act(p); };
        const index = () => {
            byMod = new Map(); holdEls = new Map();
            for (const e of frame.els) {
                const n = nodes.get(e.id);
                if (e.m !== undefined) (byMod.get(e.m) || byMod.set(e.m, []).get(e.m)).push(n);
                if (e.h) for (const k of e.h.split(' ')) { const kk = Number(k); (holdEls.get(kk) || holdEls.set(kk, []).get(kk)).push(n); }
            }
            lit = [];
        };

        // ---- elements
        function listen(node, e) {
            if (e.m !== undefined) {
                node.dataset.m = String(e.m);
                node.style.cursor = 'pointer';
                node.addEventListener('mouseenter', ev => { if (S.sim.drag) return; clear(); act(e.m); modTip(ev, mods.find(x => x.k === e.m), e.pair, e.pairs, e.bundle, e.fcov, e.lc); });
                node.addEventListener('mousemove', ctx.moveTip);
                node.addEventListener('mouseleave', () => { rest(); ctx.hideTip(); });
                node.addEventListener('click', () => { if (S.sim.moved) return; const b = mods.find(x => x.k === e.m); if (b) ctx.setTrack(ctx.getTrack() === b.tk ? '' : b.tk); });
            } else if (e.file !== undefined) {
                node.addEventListener('mouseenter', ev => { if (S.sim.drag) return; fileTip(ev, e.file); });
                node.addEventListener('mousemove', ctx.moveTip);
                node.addEventListener('mouseleave', ctx.hideTip);
                node.addEventListener('click', () => { if (!S.sim.moved) ctx.onFile(M.doc.nodes[e.file].id); });
            } else if (e.cl) {
                node.addEventListener('mouseenter', ev => {
                    if (S.sim.drag) return;
                    const c = e.cl;
                    ctx.showTip(ev, c.label || 'Files', c.col, [['Files', String(c.files.length)], ...(e.tag === 'circle' ? [['', 'click to focus']] : [])]);
                });
                node.addEventListener('mousemove', ctx.moveTip);
                node.addEventListener('mouseleave', ctx.hideTip);
                if (e.tag === 'circle') node.addEventListener('click', () => { if (!S.sim.moved) focusCluster((M && M.byId.get(e.cl.id)) || e.cl); });
            }
        }
        // 45 degree hatch fills for partial lanes, one pattern per colour and share, kept beside the drawing so a redraw does not drop them
        const defs = document.createElementNS(NS, 'defs'), hatches = new Set();
        ctx.g.appendChild(defs);
        function hatchDef(h) {
            if (hatches.has(h.id)) return;
            hatches.add(h.id);
            const pat = mkEl('pattern'), rect = mkEl('rect');
            for (const [k, v] of Object.entries({ id: h.id, width: RING.HATCH, height: RING.HATCH, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)' })) pat.setAttribute(k, v);
            rect.setAttribute('width', String(RING.HATCH * h.duty)); rect.setAttribute('height', String(RING.HATCH)); rect.style.fill = h.col;
            pat.appendChild(rect); defs.appendChild(pat);
        }
        function draw1(e) {
            const node = mkEl(e.tag, e.text);
            if (e.cls) node.setAttribute('class', e.cls);
            if (e.pe) node.style.pointerEvents = 'none';
            if (e.hatch) hatchDef(e.hatch);
            if (e.h !== undefined) node.dataset.h = e.h;
            if (e.f !== undefined) node.dataset.f = String(e.f);
            if (e.drag) node.dataset.drag = e.drag;
            if (e.lab) node.__lab = e.lab;
            listen(node, e);
            return node;
        }
        function apply(node, e) {
            const prev = node.__v || (node.__v = {});
            for (const [k, v] of Object.entries(e.a)) { const s = String(v); if (prev[k] !== s) { prev[k] = s; node.setAttribute(k, s); } }
            for (const [k, v] of Object.entries(e.sa || {})) { const s = v == null ? '' : String(v); if (prev[`@${k}`] !== s) { prev[`@${k}`] = s; if (v == null) node.removeAttribute(k); else node.setAttribute(k, s); } }
            if (e.text !== undefined && node.textContent !== e.text) node.textContent = e.text;
            for (const [k, v] of Object.entries(e.s || {})) { if (prev[`$${k}`] !== v) { prev[`$${k}`] = v; node.style.setProperty(k, v); } }
            // a hatched lane paints with its pattern; when it stops being hatched the plain colour comes back
            if (e.hatch) { if (prev.hatch !== e.hatch.id) { prev.hatch = e.hatch.id; node.style.setProperty('stroke', `url(#${e.hatch.id})`); } } else if (prev.hatch) { prev.hatch = null; node.style.setProperty('stroke', e.s.stroke); }
        }
        // the DOM is rebuilt only when the element list changes (a new layout, mode or opened circle); a physics frame updates attributes
        function paint() {
            frame = geometry(M, S, { mode: cur.mode });
            const els = frame.els;
            const same = els.length === ids.length && els.every((e, i) => e.id === ids[i]);
            if (!same) {
                const before = new Set(ids);
                root.replaceChildren();
                nodes = new Map();
                ids = els.map(e => e.id);
                for (const e of els) { const n = draw1(e); nodes.set(e.id, n); root.appendChild(n); }
                for (const e of els) apply(nodes.get(e.id), e);
                if (S.fadeNew) for (const e of els) if (!before.has(e.id)) nodes.get(e.id).classList.add('ring-new');
                index();
                rest();
                applyFocus();
                lp();
                return frame;
            }
            for (const e of els) apply(nodes.get(e.id), e);
            return frame;
        }
        function full() {
            if (dead) return;
            build();
            note.style.display = 'none';
            if (!M) return;
            cur = M.modeOf(S.nested);
            paint();
            const gateMsg = M.hidden ? `${M.hidden} module${M.hidden === 1 ? '' : 's'} hidden under ${S.min}% of functions (${ctx.fmt(M.nFuncs)} functions in total); ${mods.length} shown.` : '';
            minSeg.children[1].setAttribute("title", `Hide modules and lanes with fewer functions than this share of all functions in the set.${gateMsg ? ` ${gateMsg}` : ` ${mods.length} shown.`}`);
            const tierMsg = M.bucketsHidden ? `${M.bucketsHidden} tier bucket${M.bucketsHidden === 1 ? '' : 's'} hidden (${ctx.fmt(M.bucketFuncs)} functions).` : '';
            // the Tier buckets switch only means something when the lens has such groups
            for (const b of tierSeg.children[1].children) { if (M.buckets || M.bucketsHidden) b.removeAttribute('disabled'); else b.setAttribute('disabled', ''); }
            tierSeg.children[1].setAttribute('title', M.buckets || M.bucketsHidden ? 'The Core / Common / Rare remainders of the Lanes lens (rows that are in no lane): shown in grey behind the lanes, or hidden' : 'This lens has no tier groups: only the Lanes lens does');
            const msg = !mods.length ? (ctx.getR() ? 'No group of this lens has functions in the files.' : 'This server sends no row list: modules need it, only the clusters are shown.')
                : [cur.mode === 1 ? `Lighter drawing (about ${cur.est} elements): no bands from a ring to its lanes, no file labels.` : cur.mode === 2 ? `Light drawing (about ${cur.est} elements): clusters, rings and family ribbons only.` : '', M.plan.truncated ? `${M.plan.truncated} of the smallest outer ribbons are not drawn (safety limit ${RING.RIBBON_SAFE}).` : '', gateMsg, tierMsg].filter(Boolean).join(' ');
            S.noteBase = msg;
            setNote();
            S.fadeNew = false;
        }
        function setNote() {
            const t = [S.noteBase, S.pend ? `opening ${S.pend.count} files\u2026` : '', S.err].filter(Boolean).join(' ');
            note.textContent = t;
            note.style.display = t ? '' : 'none';
            note.classList.toggle('busy', !!S.pend);
        }
        // ---- focus: a click on a cluster body zooms the camera to it, fades the other clusters, opens what it holds
        const clusterBox = n => { const c = frame.nc[n.id], R = n.r + (n.level === 0 ? RING.RO + 40 : 24); return [c[0] - R, c[1] - R, c[0] + R, c[1] + R]; };
        function applyFocus() {
            if (!frame) return;
            const n = S.fc && M ? M.byId.get(S.fc.id) : null, fam = n ? n.famIdx : null;
            for (const e of frame.els) {
                const el = nodes.get(e.id);
                if (!el) continue;
                const dim = fam != null && !(e.fams ? e.fams.includes(fam) : e.fam === undefined || e.fam === fam);
                el.classList[dim ? 'add' : 'remove']('ring-dim');
                el.classList[S.pend && e.id === `c:${S.pend.id}` ? 'add' : 'remove']('ring-pending');
            }
            lp();
        }
        // labels depend on the zoom: a cluster name needs room on screen, a file name needs a dot big enough to hold it; overlapping names lose to the outer cluster
        let lpRaf = 0;
        const lp = () => { if (!lpRaf && typeof requestAnimationFrame === 'function') lpRaf = requestAnimationFrame(() => { lpRaf = 0; labelPass(); }); };
        function labelPass() {
            if (dead || !frame) return;
            const m = root.getScreenCTM();
            if (!m) return;
            const k = Math.hypot(m.a, m.b), fc = S.fc && M ? M.byId.get(S.fc.id) : null;
            const inFc = id => { if (!fc) return false; for (let c = M.byId.get(id); c; c = c.parent) if (c === fc) return true; return false; };
            const subs = [];
            for (const e of frame.els) {
                const L = e.lab, el = L && nodes.get(e.id);
                if (!el) continue;
                if (L.file !== undefined) { el.classList.toggle('ring-hide', !((S.focusSet && S.focusSet.has(L.file)) || L.r * k >= 7)); continue; }
                const fit = el.textContent.length * (L.lvl === 1 ? 6 : 5) <= 1.9 * L.r, on = fit && (inFc(L.id) || L.r * k >= 34);
                el.classList.toggle('ring-hide', !on);
                if (on) subs.push([L.lvl, el]);
            }
            subs.sort((a, b) => a[0] - b[0]);
            const kept = [];
            for (const [, el] of subs) {
                const r = el.getBoundingClientRect();
                if (kept.some(q => r.left < q.right + 2 && r.right > q.left - 2 && r.top < q.bottom + 2 && r.bottom > q.top - 2)) el.classList.add('ring-hide'); else kept.push(r);
            }
        }
        function drawCrumb() {
            crumb.replaceChildren();
            const n = S.fc && M ? M.byId.get(S.fc.id) : null;
            if (!n) { crumb.style.display = 'none'; return; }
            const btn = (text, f) => { const b = document.createElement('button'); b.setAttribute('type', 'button'); b.className = 'lin-btn'; b.textContent = text; b.addEventListener('click', f); return b; };
            const chain = [];
            for (let c = n; c; c = c.parent) chain.unshift(c);
            crumb.appendChild(btn('All', exitFocus));
            for (const c of chain) {
                const sep = document.createElement('span');
                sep.textContent = '>';
                crumb.append(sep, btn(c.label || 'Files', () => focusCluster(c)));
            }
            crumb.style.display = 'flex';
        }
        function focusCluster(n) {
            if (!M || (S.fc && S.fc.id === n.id)) return;
            // the camera of the first focus is what Esc brings back, however many clusters are focused in between
            if (!S.fc) S.fc = { id: n.id, before: ctx.camera ? ctx.camera.get() : null }; else S.fc.id = n.id;
            S.fcTok++;
            S.pend = null;
            S.err = '';
            S.focusSet = new Set(n.files);
            let opened = false;
            const walk = c => {
                if (!S.nested || !c.kids.length) { if (c.files.length > RING.LEAF_MAX && !S.open.has(c.id)) { S.open.add(c.id); opened = true; } } else c.kids.forEach(walk);
            };
            walk(n);
            if (opened) full(); else paint();
            applyFocus();
            drawCrumb();
            setNote();
            if (ctx.camera && frame) ctx.camera.to(clusterBox(n));
            openMerged(n);
        }
        function exitFocus() {
            if (!S.fc) return;
            const t = S.fc.before;
            S.fc = null; S.focusSet = null; S.fcTok++; S.pend = null; S.err = '';
            paint();
            applyFocus();
            drawCrumb();
            setNote();
            if (ctx.camera && t) ctx.camera.restore(t);
        }
        // the merged (twin) columns inside the cluster: ask the server to open them, then merge the new doc in without touching the view state
        function openMerged(n) {
            const nodesIn = n.files.map(i => M.doc.nodes[i]).filter(x => x.members && x.members.length > 1);
            if (!nodesIn.length || !ctx.openNodes) return;
            const key = nodesIn.map(x => x.id).join('|'), tok = S.fcTok;
            if (S.pend && S.pend.key === key) return;
            S.pend = { id: n.id, key, count: sum(nodesIn, x => x.members.length) };
            setNote();
            applyFocus();
            ctx.openNodes(nodesIn.map(x => x.id)).then(doc => {
                if (dead || tok !== S.fcTok) return;
                S.pend = null;
                S.fadeNew = true;
                ctx.merge(doc);
                applyFocus();
                drawCrumb();
                setNote();
            }).catch(err => {
                if (dead || tok !== S.fcTok) return;
                S.pend = null;
                S.err = `Could not open the files: ${(err && err.message) || err}`;
                setNote();
                applyFocus();
            });
        }
        function build() {
            const doc = ctx.getDoc(), R = ctx.getR(), C = R ? ctx.getCats() : null;
            if (!doc) { M = null; return; }
            let h = 0;
            if (R) for (let r = 0; r < R.n; r++) h = (h * 31 + (R.match(r) ? 1 : 0)) | 0;
            const k = [doc.nodes.map(n => n.id).join('|'), ctx.lens(), S.arc, S.min, S.tier, R ? R.n : 0, C ? C.cats.map(c => c.key).join('|') : '', h].join('#');
            if (M && k === key) return;
            key = k;
            M = buildModel(doc, R, C, { minPct: S.min, tier: S.tier, arc: S.arc, hsl: ctx.hsl, hue: ctx.hue, tree: ctx.tree(), nodeColor: ctx.nodeColor, grey: ctx.grey });
            mods = M.mods;
        }

        // ---- dragging
        const svgPoint = ev => { const p = ctx.svg.createSVGPoint(); p.x = ev.clientX; p.y = ev.clientY; const q = p.matrixTransform(root.getScreenCTM().inverse()); return [q.x, q.y]; };
        const down = ev => {
            const t = ev.target.closest && ev.target.closest('[data-drag]');
            if (!t || !root.contains(t)) return;
            const k = t.dataset.drag, p = S.sim.pts[k];
            if (!p) return;
            const q = svgPoint(ev);
            press = { key: k, sx: ev.clientX, sy: ev.clientY, gx: q[0] - (p.hx + p.dx), gy: q[1] - (p.hy + p.dy), px: q[0], py: q[1], active: false };
        };
        let pointerMoved = false;
        const move = ev => {
            if (!press) return;
            if (!press.active) {
                if (Math.hypot(ev.clientX - press.sx, ev.clientY - press.sy) < RING.DRAG_PX) return;
                press.active = true; S.sim.drag = press; S.sim.moved = true; document.body.classList.add('ring-dragging');
                ctx.hideTip();
            }
            const q = svgPoint(ev);
            press.px = q[0]; press.py = q[1];
            pointerMoved = true;
            run();
        };
        const up = () => {
            if (!press) return;
            const was = press.active;
            press = null; S.sim.drag = null; document.body.classList.remove('ring-dragging');
            if (was) { setTimeout(() => (S.sim.moved = false), 0); run(); }
        };
        // The loop always ends: it stops once the motion settles, after RING.FRAMES_MAX frames or RING.MS_MAX ms of settling,
        // runs at most 30 frames a second, and pauses while the page is hidden.
        let frames = 0, t0 = 0, lastT = 0;
        function run() {
            if (raf || dead || document.hidden) return;
            frames = 0; t0 = performance.now();
            raf = requestAnimationFrame(step);
        }
        function step() {
            raf = 0;
            if (dead || document.hidden) return;
            const now = performance.now();
            if (now - lastT < RING.FRAME_MS) { raf = requestAnimationFrame(step); return; }
            lastT = now;
            const e = S.sim.tick();
            paint();
            frames++;
            const drag = !!S.sim.drag, active = e > RING.SETTLE || (drag && pointerMoved);
            pointerMoved = false;
            if (!active) return;
            if (!drag && (frames >= RING.FRAMES_MAX || now - t0 > RING.MS_MAX)) return;
            raf = requestAnimationFrame(step);
        }
        // Escape clears the pin
        const onKey = ev => { if (ev.key !== 'Escape') return; if (S.fc) exitFocus(); else if (pinned() != null) ctx.setTrack(''); };
        document.addEventListener('keydown', onKey);
        // a click on empty canvas leaves the focus (d3's pan swallows the click that ends a drag)
        const onSvgClick = ev => { if (ev.target === ctx.svg && S.fc) exitFocus(); };
        ctx.svg.addEventListener('click', onSvgClick);
        const onVis = () => { if (!document.hidden && S.sim.drag) run(); };
        ctx.svg.addEventListener('pointerdown', down);
        window.addEventListener('pointermove', move);
        window.addEventListener('pointerup', up);
        document.addEventListener('visibilitychange', onVis);
        // zoom changes the transform of an ancestor group
        const zobs = typeof MutationObserver === 'function' ? new MutationObserver(lp) : null;
        if (zobs) for (let a = ctx.g; a && a !== ctx.svg.parentNode; a = a.parentNode) zobs.observe(a, { attributes: true, attributeFilter: ['transform'] });
        window.addEventListener('pointerup', lp);

        return {
            draw: full,
            // the table's track changed: relight
            setTrack: rest,
            reset() { S.sim.reset(); full(); },
            // bounding box of everything drawn, for Fit
            box() { return frame ? frame.fit : null; },
            show(on) { root.style.display = on ? '' : 'none'; note.style.display = on && note.textContent ? '' : 'none'; },
            state: S,
            frame: () => frame,
            destroy() {
                dead = true;
                cancelAnimationFrame(raf);
                if (zobs) zobs.disconnect();
                window.removeEventListener('pointerup', lp);
                ctx.svg.removeEventListener('pointerdown', down);
                window.removeEventListener('pointermove', move);
                window.removeEventListener('pointerup', up);
                document.removeEventListener('visibilitychange', onVis);
                document.removeEventListener('keydown', onKey);
                ctx.svg.removeEventListener('click', onSvgClick);
                crumb.remove();
                document.body.classList.remove('ring-dragging');
                ctx.controls.replaceChildren();
                ctx.controls2.replaceChildren();
                root.remove(); defs.remove(); note.remove();
                nodes = new Map(); byMod = new Map(); holdEls = new Map(); lit = []; frame = null; M = null;
            },
        };
    }

    window.NwayRing = { mount, autoView, chooseView, RING, _layout: { covTier, tierText, RING, lcaNode, clusterTree, modulesOf, buildModel, plan, placeFamilies, geometry, newSim, badAttrs, rg, band } };
})();
