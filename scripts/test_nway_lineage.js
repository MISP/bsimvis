// Self-check for the N-way lineage chart's model (nway_lineage.js, no DOM):
// the row list (doc.rows) read into per-node rows, lens painting and tracking.
//
// Pinned here: hex masks map bit i to nodes[i] (leading zeros and case do not
// matter), a row is born in the first node whose parent lacks it, node sizes
// never move when only rows.match changes, and track keys use the table's forms.
// Run: node scripts/test_nway_lineage.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/nway_lineage.js', 'utf8');
const window = {};
new Function('window', fs.readFileSync(__dirname + '/../bsimvis/app/static/js/nway_colors.js', 'utf8'))(window);
new Function('window', fs.readFileSync(__dirname + '/../bsimvis/app/static/js/nway_groups.js', 'utf8'))(window);
new Function('window', src)(window);
new Function('window', fs.readFileSync(__dirname + '/../bsimvis/app/static/js/nway_ring.js', 'utf8'))(window);
const { buildModel, layout, rowModel, lensCats, attachRows, lensPaint, trackRows, trackInfo } = window.NwayLineage._layout;

// A (root) > B > D, A > C. Rows: r0 everywhere, r1 A+B, r2 B+D, r3 C, r4 A+C, r5 D.
const node = (id, fn, extra = {}) => ({ id, file_name: `${id}.elf`, functions: fn, died_all: 0, only_here: 0, reuse_in: 0, members: [id], ...extra });
const edge = (parent, child, similarity) => ({ parent, child, similarity, gained: 1, lost: 1, shared: 2, recall_suspect: 0 });
const doc = match => ({
    root: 'c:file:a',
    nodes: [node('c:file:a', 3, { root: true }), node('c:file:b', 3), node('c:file:c', 3), node('c:file:d', 3)],
    edges: [edge('c:file:a', 'c:file:b', 0.9), edge('c:file:a', 'c:file:c', 0.5), edge('c:file:b', 'c:file:d', 0.8)],
    partitions: [], reuse: [], reuse_skipped: true,
    rows: {
        n: 6,
        fid: ['c:func:a:0', 'c:func:a:1', 'c:func:b:2', 'c:func:c:3', 'c:func:a:4', 'c:func:d:5'],
        fids: [['c:func:a:0'], ['c:func:a:1'], ['c:func:b:2', 'c:func:d:2'], ['c:func:c:3'], ['c:func:a:4'], ['c:func:d:5']],
        name: ['main', 'attack_udp', 'attack_syn', 'scan_x', 'FUN_1', 'killer_go'],
        mask: ['F', '3', 'a', '4', '05', '8'],
        role: ['orchestrator', 'logic', 'logic', 'leaf', null, 'bogus'],
        region: [1, 1, 1, 2, null, 2],
        library: [false, false, false, false, true, false],
        weight: [10, 10, 10, 10, 10, 10],
        match,
    },
    regions: { 1: { label: '', kind: '' }, 2: { label: 'Scanner', kind: '' } },
});

const build = (d, lens) => {
    const M = buildModel(d, new Set()), L = layout(d, M), R = rowModel(d, M);
    attachRows(M, R);
    const cache = new Map(), cats = l => cache.get(l) || cache.set(l, lensCats(d, R, l)).get(l);
    return { M, L, R, cats, P: lens ? lensPaint(M, L, R, cats(lens)) : null };
};

// --- rows ---------------------------------------------------------------
const all = build(doc(null));
assert.deepStrictEqual(all.R.held.map(h => h.join()), ['0,1,4', '0,1,2', '0,3,4', '0,2,5'], 'bit i = nodes[i], case and leading zeros ignored');
assert.deepStrictEqual([...all.R.pop], [4, 2, 2, 1, 2, 1]);
assert.deepStrictEqual([...all.R.bornIn], [0, 0, 1, 2, 0, 3], 'born in the first node whose parent lacks the row');
assert.strictEqual(all.R.mask[4], '5', 'mask normalised to the table hex form');

// --- lens groups -------------------------------------------------------------
const core = all.cats('core');
assert.deepStrictEqual([0, 1, 2, 3, 4, 5].map(core.of), ['every', 'few', 'few', 'few', 'few', 'few']);
const mods = all.cats('modules');
assert.deepStrictEqual(mods.cats.map(c => c.label), ['attack_*', 'Scanner', 'No region'], 'unnamed region labelled by its name prefix');
assert.deepStrictEqual(all.cats('triage').cats.map(c => c.key).slice(-1), [''], 'unknown and missing roles share "No role"');
assert.strictEqual(all.cats('triage').of(5), '');
assert.strictEqual(all.cats('lanes').raw(4), '5');
assert.strictEqual(all.cats('deltas').of(2), 'c:file:b');

// --- painting: sizes stay put when only match changes ------------------------
const lit = build(doc(null), 'modules'), some = build(doc([true, false, true, false, true, false]), 'modules');
for (const v of lit.L.vis) {
    const w = some.L.vis.find(x => x.id === v.id);
    assert.deepStrictEqual([w.y0, w.h], [v.y0, v.h], `${v.id} does not move when a filter changes`);
    for (const P of [lit.P, some.P]) {
        const sp = P.segs.get(v.id);
        const sum = [...sp.m.values()].reduce((a, s) => a + s.h, 0) + sp.faint.h;
        assert.ok(Math.abs(sum - v.h) < 1e-6, 'segments fill the node exactly');
    }
}
assert.strictEqual(some.P.segs.get('c:file:b').faint.cnt, 1, 'the non-matching row of B sits faint');
const ab = lit.P.links.filter(l => l.source.id === 'c:file:a' && l.target.id === 'c:file:b');
assert.deepStrictEqual(ab.map(l => [l.cat.label, l.value]), [['attack_*', 2]], 'one ribbon per group, shared rows only');
assert.strictEqual(ab[0].edge.child, 'c:file:b');

// --- tracking --------------------------------------------------------------------
const info = t => { const m = trackRows(all.R, all.cats, t); return m && trackInfo(all.L, all.R, m); };
const fn = info('fn:c:func:d:2');
assert.strictEqual(fn.holders, 2, 'fn: matches any fid of the row');
assert.deepStrictEqual(fn.info.get('c:file:b'), { tk: 1, born: 1, lost: 0, reborn: 0 });
assert.deepStrictEqual(fn.info.get('c:file:d'), { tk: 1, born: 0, lost: 0, reborn: 0 });
assert.deepStrictEqual(info('lanes:3').info.get('c:file:c'), { tk: 0, born: 0, lost: 1, reborn: 0 }, 'lost: in the parent, not here');
assert.strictEqual(info('modules:2').holders, 2);
assert.strictEqual(info('core:every').holders, 4);
assert.strictEqual(info('deltas:c:file:b').holders, 2, 'deltas key is the column id, which has colons');
assert.strictEqual(info('nope:x'), null, 'an unknown lens tracks nothing');
const filtered = trackInfo(some.L, some.R, trackRows(some.R, some.cats, 'fn:c:func:a:1'));
assert.strictEqual(filtered.holders, 0, 'a row the filters drop is not tracked');
assert.strictEqual(fn.max, 1, 'max: the most tracked rows one node holds (the tracks fill bar)');

// --- tracks view: a group born in two places, member ids ---------------------------
const twice = doc(null);
for (const [k, v] of Object.entries({ fid: 'c:func:b:6', fids: ['c:func:b:6', 'c:func:c:6'], name: 'reused', mask: '6', role: 'logic', region: 1, library: false, weight: 10 })) twice.rows[k].push(v);
twice.rows.n = 7;
twice.nodes[1].members = ['c:file:b', 'c:file:b2'];
const tw = build(twice), twInfo = t => trackInfo(tw.L, tw.R, trackRows(tw.R, tw.cats, t));
const re = twInfo('fn:c:func:c:6');
assert.deepStrictEqual(re.info.get('c:file:b'), { tk: 1, born: 1, lost: 0, reborn: 1 }, 'born in B and in C: violet frame on both');
assert.deepStrictEqual(re.info.get('c:file:c'), { tk: 1, born: 1, lost: 0, reborn: 1 });
assert.strictEqual(twInfo('fn:c:func:a:1').info.get('c:file:b').reborn, 0, 'born once, in the root: not reborn');
assert.strictEqual(twInfo('deltas:c:file:b2').holders, twInfo('deltas:c:file:b').holders, 'a deltas key may name a member file of a Same code node');

// --- shared helpers -------------------------------------------------------------------
assert.strictEqual(window.NwayColors.hue(0), 215, 'the table and the lineage share one hue formula');
assert.strictEqual(window.NwayColors.hue(1), Math.round((137.508 + 215) % 360));
// the table's column tree: a group of two plus a lone column; order=lineage moves them (base = default index)
const G = [{ id: 'g', label: 'G', member_count: 2 }];
const def = [{ id: 'a', kind: 'twin', path: G }, { id: 'b', kind: 'twin', path: G }, { id: 'c', kind: 'twin', path: [] }];
const moved = [{ ...def[2], base: 2 }, { ...def[1], base: 1 }, { ...def[0], base: 0 }];
const col = (cols, id) => window.NwayColors.tree(cols).colors[cols.findIndex(c => c.id === id)];
for (const id of ['a', 'b', 'c']) assert.deepStrictEqual(col(moved, id), col(def, id), `${id} keeps its colour when order=lineage moves it`);
assert.notDeepStrictEqual(col(def, 'a'), col(def, 'b'), 'siblings in a group differ a little');
assert.strictEqual(col(def, 'c').h, window.NwayColors.hue(1), 'the second top-level entry takes the second hue');
// a lineage node (path from the server, no kind) gets its column's colour
const nodes = def.map(c => ({ id: c.id, path: c.path }));
for (const id of ['a', 'b', 'c']) assert.deepStrictEqual(col(nodes, id), col(def, id), `lineage node ${id} = its table column`);
const tq = window.NwayLineage.tableQs('md5s=a,b&order=chain&layout=star&lview=split&track=fn:x&root=c:file:a');
assert.strictEqual(tq.toString(), 'md5s=a%2Cb&track=fn%3Ax&root=c%3Afile%3Aa&col_open=%5B%5D', 'lineage-only keys dropped, an old order=chain too; twins closed as here');
assert.strictEqual(window.NwayLineage.tableQs('order=lineage&lview=tracks&col_open=x').toString(), 'order=lineage&col_open=x', "the table's own order and open columns pass through");

// --- the table's groups (doc.groups for doc.group, rows.group = deepest first-level group) ---------
const NG = window.NwayGroups;
assert.strictEqual(NG.trackKey({ kind: 'lane' }, 'tier:core/lane:0110', i => i), 'lanes:6', 'lane bits over the default column order');
assert.strictEqual(NG.trackKey({ kind: 'lane' }, 'lane:0110', i => [3, 2, 1, 0][i]), 'lanes:6', 'order=lineage: bits go back to the base index');
assert.strictEqual(NG.trackKey({ kind: 'region' }, 'region:r2', i => i), 'modules:r2');
assert.strictEqual(NG.trackKey({ kind: 'tier' }, 'tier:core', i => i), 'tier:core', 'a tier keeps its group id');
const grouped = doc([true, true, true, true, true, false]);
Object.assign(grouped, {
    group: 'lane,region',
    groups: [
        { id: 'tier:core', kind: 'tier', label: 'Core', path: ['tier:core'] },
        { id: 'tier:core/lane:1100', kind: 'lane', mask: '1100', path: ['tier:core', 'tier:core/lane:1100'] },
        { id: 'tier:rare', kind: 'tier', label: 'Rare', path: ['tier:rare'] },
    ],
});
grouped.rows.group = [1, 1, 0, 2, null, null];
const gb = build(grouped), lanes = lensCats(grouped, gb.R, 'lanes', g => `name of ${g.id}`);
assert.deepStrictEqual(lanes.cats.map(c => [c.tk, c.label]), [['tier:core', 'name of tier:core'], ['lanes:3', 'name of tier:core/lane:1100'], ['tier:rare', 'name of tier:rare'], ['', 'Other functions']],
    'cats: the groups holding rows themselves, in table order, by their table track key; a matching row with no group goes to Other');
assert.deepStrictEqual(lanes.groups.map(x => x.depth), [0, 1, 0]);
const marked = t => { const m = trackRows(gb.R, l => lensCats(grouped, gb.R, l), t, 'lanes'); return m && [...m].join(''); };
assert.strictEqual(marked('tier:core'), '111000', 'a tier tracks its own rows and its lanes\'');
assert.strictEqual(marked('lanes:3'), '110000');
assert.strictEqual(marked('tier:nope'), null, 'an unknown group id tracks nothing');
assert.strictEqual(marked('modules:2'), '000101', 'another lens keeps its row-derived groups (old track keys)');
assert.ok(!lensCats(grouped, gb.R, 'modules').groups, 'groups are for the lens of doc.group only');
assert.ok(!lensCats(grouped, gb.R, 'deltas').groups, 'Deltas keeps its own rendering');
const gp = build(grouped, 'lanes'), sum = [...gp.P.segs.get('c:file:a').m.values()].reduce((a, x) => a + x.cnt, 0) + gp.P.segs.get('c:file:a').faint.cnt;
assert.strictEqual(sum, 3, 'server cats paint every row of a node');

// --- lane names: one model call per lane content, shared by every page -----------------------------
const lane = NG.payload({ id: 'lane:1100', kind: 'lane', mask: '1100', files: 2, rows: 4, sample: ['attack_udp'] }, 3);
assert.deepStrictEqual([lane.files, lane.total, lane.kind], [2, 3, undefined]);
assert.ok(NG.nameable({ kind: 'region', sample: ['x'] }) && !NG.nameable({ kind: 'region', label: 'Runtime', sample: ['x'] }) && !NG.nameable({ kind: 'lane' }));
let posts = 0;
global.fetch = async () => { posts++; return { ok: true, json: async () => ({ status: 'ready', names: { 'lane:1100': 'udp flood' }, evidence: {} }) }; };
(async () => {
    const got = [];
    assert.strictEqual(await NG.nameLanes([lane], () => true, n => got.push(n['lane:1100'])), 'ready');
    // the lineage page asks for the same lane under another id: answered from the cache
    assert.strictEqual(await NG.nameLanes([{ ...lane, id: 'tier:core/lane:1100' }], () => true, n => got.push(n['tier:core/lane:1100'])), 'ready');
    assert.deepStrictEqual([posts, got], [1, ['udp flood', 'udp flood']], 'a lane named once is not asked again');
    // --- Ring view (nway_ring.js): pure layout ------------------------------------------------------
    {
        const RL = window.NwayRing._layout, RING = window.NwayRing.RING;
        // 12 files in three families (A: two sub-clusters of 3, B: 4 files, C: 2 files); lanes are the modules
        const fam = ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'B', 'C', 'C'], sub = ['a1', 'a1', 'a1', 'a2', 'a2', 'a2', '', '', '', '', '', ''];
        const rdoc = withPath => {
            const nodes = fam.map((f, i) => ({ id: `s:file:${i}`, file_name: `f${i}`, functions: 40, members: [`s:file:${i}`],
                path: withPath ? [{ id: `fam:${f}`, label: `Family ${f}` }, ...(sub[i] ? [{ id: `sub:${sub[i]}`, label: sub[i] }] : [])] : [] }));
            const edges = nodes.slice(1).map((n, i) => ({ parent: nodes[i].id, child: n.id, similarity: fam[i] === fam[i + 1] ? 0.9 : 0.1, gained: 1, lost: 1, shared: 5, recall_suspect: 0 }));
            const masks = [];
            const add = (idx, n) => { let m = 0n; for (const i of idx) m |= 1n << BigInt(i); for (let k = 0; k < n; k++) masks.push(m.toString(16)); };
            add([0, 1, 2, 3, 4, 5], 12); add([0, 1, 2], 8); add([3, 4, 5], 7); add([6, 7, 8, 9], 9); add([10, 11], 6);
            add([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 14); add([3, 4, 5, 10, 11], 10); add([6, 7, 8, 9, 10, 11], 5); add([0], 3);
            return { root: nodes[0].id, nodes, edges, partitions: [], reuse: [], rows: { n: masks.length, mask: masks, fid: masks.map((_, r) => `f${r}`), name: masks.map((_, r) => `fn${r}`), role: masks.map(() => 'logic'), region: masks.map(() => null), library: masks.map(() => false), weight: masks.map(() => 1), match: null }, regions: {} };
        };
        const build = (d, lens, o = {}) => {
            const LM = buildModel(d, new Set()), R = rowModel(d, LM), C = lensCats(d, R, lens);
            return { R, C, M: RL.buildModel(d, R, C, { hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, tree: window.NwayColors.tree(d.nodes.map(n => ({ id: n.id, path: n.path }))), nodeColor: () => '#888', ...o }) };
        };
        const draw = (M, nested = true, sim = RL.newSim(), mode) => ({ sim, g: RL.geometry(M, { sim, nested, open: new Set() }, mode == null ? {} : { mode }) });
        const d = rdoc(true);
        // clusters from node.path: three families, A nested in two sub-clusters
        const { M } = build(d, 'lanes');
        assert.deepStrictEqual(M.roots.map(f => [f.label, f.files.length, f.kids.length]), [['Family A', 6, 2], ['Family B', 4, 0], ['Family C', 2, 0]]);
        // every view and lens: no NaN / undefined / Infinity in any attribute
        for (const lens of ['modules', 'core', 'lanes', 'triage', 'deltas']) for (const nested of [true, false]) {
            const b = build(d, lens), g = draw(b.M, nested).g;
            assert.deepStrictEqual(RL.badAttrs(g.els), [], `${lens} nested=${nested}: finite attributes`);
            assert.ok(g.els.every(e => e.id), 'every element has an id');
        }
        const g = draw(M).g, nodeOf = id => M.byId.get(id) || [...M.byId.values()].find(c => c.id === id);
        // outer ribbons: equal width at both ends, no pinch at the default layout
        assert.ok(g.stats.ribbons.length > 0, 'the cross-family lanes make ribbons');
        for (const r of g.stats.ribbons) {
            assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'both ends have the same arc width');
            assert.ok(Math.abs(r.wX - r.wY) < 0.05 * Math.max(r.wX, r.wY, 1), 'and the same chord');
            assert.ok(r.minW >= 0.75 * Math.max(r.wX, r.wY), `narrowest point ${r.minW.toFixed(1)} of ${r.wX.toFixed(1)}`);
        }
        // lanes: one feeding ribbon each, all lanes of a ring inside 85% of it
        const lanes = g.els.filter(e => e.id.startsWith('l:')), ups = g.els.filter(e => e.id.startsWith('u:'));
        assert.ok(lanes.length > 0);
        for (const l of lanes) assert.strictEqual(ups.filter(u => u.sk === l.sk && u.lane === l.lane).length, 1, `lane ${l.id} has one feeding ribbon`);
        const perNode = new Map();
        for (const l of g.stats.lanes) perNode.set(l.node, (perNode.get(l.node) || 0) + l.w);
        for (const [n, w] of perNode) assert.ok(w <= RING.LANE_FIT * 2 * Math.PI + 1e-6, `lanes of ${n} fit`);
        assert.ok(g.stats.laneFit >= 0.99, 'one lane scale fits every ring without a squeeze');
        // no ribbon skips a level: a lane on a sub-cluster is fed from its parent (a family ring segment, or the parent's lane)
        for (const u of ups) {
            const c = M.byId.get(u.lane), src = u.src;
            assert.ok(c && c.parent && c.parent.id === src, `${u.id} comes from the parent level`);
        }
        const inner = g.els.filter(e => e.id.startsWith('i:'));
        assert.ok(inner.length > 0 && inner.every(e => { const c = M.byId.get(e.drag.slice(2)); return c && (!c.kids.length); }), 'ribbons to files start at a cluster without sub-clusters');
        // nesting off: ring straight to the files, no lanes
        const flat = draw(M, false).g;
        assert.strictEqual(flat.els.filter(e => /^[lu]:/.test(e.id)).length, 0, 'no lanes without nesting');
        assert.ok(flat.els.filter(e => e.id.startsWith('i:')).every(e => e.drag.startsWith('f:/fam:') || e.drag.startsWith('f:/other')), 'fans start at the family circle');
        assert.ok(flat.els.filter(e => e.id.startsWith('c:')).length === 3, 'only the families are circles');
        // drag: sub-clusters and files keep their offsets to the family, families keep apart
        const dr = draw(M), A = M.roots[0], key = `f:${A.id}`, p = dr.sim.pts[key];
        dr.sim.drag = { key, px: p.hx + 220, py: p.hy - 90, gx: 0, gy: 0 };
        for (let i = 0; i < 40; i++) { dr.sim.tick(); RL.geometry(M, { sim: dr.sim, nested: true, open: new Set() }); }
        dr.sim.drag = null;
        let left = 0;
        for (let i = 0; i < 400 && (left = dr.sim.tick()) > RING.SETTLE; i++) RL.geometry(M, { sim: dr.sim, nested: true, open: new Set() });
        assert.ok(left <= RING.SETTLE, 'the motion settles');
        for (const k of A.kids) { const q = dr.sim.pts[`f:${k.id}`]; assert.ok(Math.hypot(q.dx - p.dx, q.dy - p.dy) < 0.5, 'a sub-cluster keeps its offset'); }
        for (const i of A.files) { const q = dr.sim.pts[`n:${i}`]; assert.ok(Math.hypot(q.dx - p.dx, q.dy - p.dy) < 0.5, 'a file keeps its offset'); }
        const after = RL.geometry(M, { sim: dr.sim, nested: true, open: new Set() });
        for (let u = 0; u < 3; u++) for (let v = u + 1; v < 3; v++) {
            const a = after.cen[u], b = after.cen[v];
            assert.ok(Math.hypot(a[0] - b[0], a[1] - b[1]) >= RL.rg(M.roots[u]).ro + RL.rg(M.roots[v]).ro + RING.FAM_GAP - 1, 'families keep their spacing');
        }
        // pushing family A onto B moves B away
        const d2 = draw(M), pa = d2.sim.pts[key], b0 = M.homes[1];
        d2.sim.drag = { key, px: b0[0], py: b0[1], gx: 0, gy: 0 };
        for (let i = 0; i < 30; i++) { d2.sim.tick(); RL.geometry(M, { sim: d2.sim, nested: true, open: new Set() }); }
        const f2 = RL.geometry(M, { sim: d2.sim, nested: true, open: new Set() });
        assert.ok(Math.hypot(f2.cen[0][0] - f2.cen[1][0], f2.cen[0][1] - f2.cen[1][1]) >= RL.rg(M.roots[0]).ro + RL.rg(M.roots[1]).ro + RING.FAM_GAP - 1, 'a dragged family does not sit on another');
        void pa;
        // no node.path: the lineage edges are cut at weak similarity (A, B, C stay apart; singletons join Other files)
        const np = build(rdoc(false), 'lanes').M;
        assert.ok(!np.T.hasPath && np.roots.length >= 2, 'clusters without node.path');
        assert.deepStrictEqual(np.roots.map(f => f.files.length).slice(0, 2), [6, 4]);
        // the holder rule: one pass over the masks; a file holds a module when it has HOLD_MIN of the best holder's rows
        const mc = build(d, 'lanes');
        const mods = RL.modulesOf(mc.R, mc.C, d.nodes.length, { hold: 0.5 });
        assert.strictEqual(mods.reduce((a, m) => a + m.size, 0), d.rows.n, 'every row is in one module');
        assert.deepStrictEqual(mods.find(m => m.size === 12).mem, [0, 1, 2, 3, 4, 5], 'a lane is held by exactly its files');
        // degrade ladder: above the budget, no ribbons to single files; above the second one, clusters and family ribbons only
        const rows = g.els.length, inner0 = inner.length;
        const light = draw(M, true, RL.newSim(), 1).g, lean = draw(M, true, RL.newSim(), 2).g;
        assert.ok(light.els.length < rows && light.els.every(e => !e.id.startsWith('u:') && !e.id.startsWith('nl:')) && light.els.some(e => e.id.startsWith('l:')) && light.els.some(e => e.id.startsWith('i:')), 'mode 1: lanes and fans to files stay, bands and file labels go');
        assert.ok(lean.els.every(e => !/^[iunl]:/.test(e.id)) && lean.els.some(e => e.id.startsWith('o:')), 'mode 2: clusters, rings and family ribbons');
        assert.strictEqual(M.modeOf(true).mode, 0, 'a small set draws everything');
        const saved = RING.BUDGET.slice();
        RING.BUDGET[0] = 10; RING.BUDGET[1] = 100000;
        assert.strictEqual(M.modeOf(true).mode, 1); RING.BUDGET[1] = 20;
        assert.strictEqual(M.modeOf(true).mode, 2);
        RING.BUDGET[0] = saved[0]; RING.BUDGET[1] = saved[1];
        void inner0;
        void nodeOf;

        // nothing is hidden: every module has its segment, every cross-family module its ribbons, equal at both ends, on both lenses
        const rnd = (() => { let x = 12345; return () => (x = (x * 1103515245 + 12345) % 2147483648) / 2147483648; })();
        const yara = (() => {   // the shape of a real set: 35 files in 6 families, 21 call-graph regions, most of them in several families
            const fm = [10, 8, 6, 5, 4, 2], nodes = [];
            fm.forEach((n, f) => { for (let k = 0; k < n; k++) nodes.push({ id: `y:file:${nodes.length}`, file_name: `y${nodes.length}`, functions: 30, members: [], path: [{ id: `fam${f}`, label: `F${f}` }] }); });
            const edges = nodes.slice(1).map((n, i) => ({ parent: nodes[i].id, child: n.id, similarity: 0.9, gained: 1, lost: 1, shared: 1, recall_suspect: 0 }));
            const masks = [], region = [];
            for (let r = 0; r < 600; r++) {
                const reg = r % 21, home = reg % 6; let m = 0n;
                for (let i = 0; i < nodes.length; i++) if (rnd() < (fm.reduce((a, b, f) => a + (f < home ? b : 0), 0) <= i && i < fm.slice(0, home + 1).reduce((a, b) => a + b, 0) ? 0.5 : 0.06)) m |= 1n << BigInt(i);
                masks.push((m || 1n).toString(16)); region.push(`r${reg}`);
            }
            const regions = {}; for (let k = 0; k < 21; k++) regions[`r${k}`] = { label: `Region ${k}`, kind: '' };
            return { root: nodes[0].id, nodes, edges, partitions: [], reuse: [], rows: { n: masks.length, mask: masks, fid: masks.map((_, r) => `y${r}`), name: masks.map((_, r) => `fn${r}`), role: masks.map(() => 'logic'), region, library: masks.map(() => false), weight: masks.map(() => 1), match: null }, regions };
        })();
        for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) {
            const b = build(dd, lens), gg = RL.geometry(b.M, { sim: RL.newSim(), nested: true, open: new Set() });
            const segMods = new Set(b.M.plan.segs.flat().map(x => x.b.k)), cross = b.M.mods.filter(m => m.fam == null);
            assert.strictEqual(segMods.size, b.M.mods.length, `${label}/${lens}: every module has a ring segment`);
            if (label === 'real-shaped' && lens === 'modules') assert.ok(b.M.mods.length === 21 && cross.length >= 10 && b.M.roots.length === 6, `fixture shape: ${b.M.mods.length} modules, ${cross.length} cross, ${b.M.roots.length} families`);
            assert.ok(!b.M.plan.segs.flat().some(x => x.b.other), 'no merged other segment');
            const ribKs = new Set(gg.els.filter(e => e.id.startsWith('o:')).map(e => Number(e.id.split(':')[1])));
            for (const m of cross) assert.ok(ribKs.has(m.k), `${label}/${lens}: cross module ${m.label} has an outer ribbon`);
            for (const m of cross) for (let u = 0; u < m.fams.length; u++) for (let v = u + 1; v < m.fams.length; v++) assert.ok(gg.els.some(e => e.id === `o:${m.k}:${m.fams[u]}:${m.fams[v]}`), 'one ribbon per family pair');
            for (const r of gg.stats.ribbons) assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'both ends of a ribbon have the same width');
            assert.deepStrictEqual(RL.badAttrs(gg.els), [], `${label}/${lens}: finite attributes`);
            assert.ok(b.M.modeOf(true).est < RING.BUDGET[0] && b.M.modeOf(true).mode === 0, `${label}/${lens}: full drawing under the budget`);
            const thin = b.M.plan.segs.flat().filter(x => Math.max(x.size * b.M.plan.px, b.M.plan.floor) < 1).length;
            assert.strictEqual(thin, 0, 'no segment thinner than 1 px');
            assert.ok(gg.stats.laneFit >= 0.99, 'lanes fit');
            if (lens === 'modules') for (const m of b.M.mods) for (const c of m.fams) assert.ok(m.rowsIn[c] > 0, 'rows per family counted');
        }
        // shared functions size the segments: a cross segment counts the rows held by both families, the same at both ends
        {
            const b = build(yara, 'modules'), M2 = b.M, m = M2.mods.find(x => x.fam == null), [x, y] = m.fams;
            const sx = M2.plan.segs[x].find(s => s.b === m && s.partner === y), sy = M2.plan.segs[y].find(s => s.b === m && s.partner === x);
            assert.strictEqual(sx.size, sy.size, 'equal at both ends');
            assert.strictEqual(sx.size, m.pairRows(x, y));
            assert.ok(sx.size <= Math.min(m.rowsIn[x], m.rowsIn[y]) && m.rowsIn[x] <= m.size, 'shared <= held per family <= module');
            assert.strictEqual(M2.plan.truncated, 0, 'the safety limit does not trigger on a real-sized set');
        }
        // the removed holder control and its URL key stay removed; the rule is one function
        const ringSrc = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/nway_ring.js', 'utf8'), viewSrc = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/views/nway_lineage_view.js', 'utf8');
        assert.ok(!/HOLD_|holdSel|Holder threshold|CAP_CROSS|MIN_SEG_PX|OTHER_MAX|MAX_RIBBONS/.test(ringSrc), 'no holder control, no pruning constants');
        assert.ok(!/KEYS = \[[^\]]*rhold/.test(viewSrc) && !/rhold: /.test(viewSrc), 'rhold is not a page key');
        assert.ok(window.NwayRing.RING.BUDGET[0] >= 6000 && !('CAP' in window.NwayRing.RING));
    }
    console.log('nway lineage self-check OK');
})().catch(e => { console.error(e); process.exit(1); });
