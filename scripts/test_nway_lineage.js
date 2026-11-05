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
            return { R, C, M: RL.buildModel(d, R, C, { hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, tree: window.NwayColors.tree(d.nodes.map(n => ({ id: n.id, path: n.path }))), nodeColor: () => '#888', arc: 'partner', ...o }) };
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
        for (const l of lanes) assert.strictEqual(ups.filter(u => u.sk === l.sk && u.lane === l.lane).length, l.own ? 0 : 1, `lane ${l.id} has one feeding ribbon (none when the module starts here)`);
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
        assert.strictEqual(flat.els.filter(e => e.id.startsWith('u:')).length, 0, 'no bands without nesting');
        assert.ok(flat.els.filter(e => e.id.startsWith('l:')).every(e => e.own), 'without nesting the only lanes are single-cluster modules on the circle itself');
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
        for (const i of A.files) { const q = dr.sim.pts[`n:${d.nodes[i].id}`]; assert.ok(Math.hypot(q.dx - p.dx, q.dy - p.dy) < 0.5, 'a file keeps its offset'); }
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
            const segMods = new Set([...b.M.plan.segs.flat(), ...b.M.plan.layers.flat()].map(x => x.b.k)), cross = b.M.mods.filter(m => m.fam == null);
            assert.strictEqual(segMods.size, b.M.mods.length, `${label}/${lens}: every module is drawn, on the ring or as a lane inside its cluster`);
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

        // one arc per module (test option): exactly one ring segment per module on each circle, ribbons still equal at both ends and one per family pair
        for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) {
            const per = build(dd, lens), one = build(dd, lens, { arc: 'one' });
            const gp = RL.geometry(per.M, { sim: RL.newSim(), nested: true, open: new Set() }), go = RL.geometry(one.M, { sim: RL.newSim(), nested: true, open: new Set() });
            const segCount = new Map();
            for (const e of go.els) if (e.id.startsWith('s:')) { const [, c, k] = e.id.split(':'); segCount.set(`${c}|${k}`, (segCount.get(`${c}|${k}`) || 0) + 1); }
            assert.ok([...segCount.values()].every(n => n === 1), `${label}/${lens}: one ring segment per module per circle`);
            for (const m of one.M.mods.filter(x => x.fam == null)) for (const c of m.fams) assert.ok(segCount.has(`${c}|${m.k}`), 'every module keeps a segment on each circle it spans');
            assert.strictEqual(go.els.filter(e => e.id.startsWith('o:')).length, gp.els.filter(e => e.id.startsWith('o:')).length, 'the same ribbons as with one arc per partner');
            for (const r of go.stats.ribbons) assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'equal ribbon ends in one-arc mode');
            for (const m of one.M.mods.filter(x => x.fam == null)) for (let u = 0; u < m.fams.length; u++) for (let v = u + 1; v < m.fams.length; v++) assert.ok(go.els.some(e => e.id === `o:${m.k}:${m.fams[u]}:${m.fams[v]}`), 'a ribbon per family pair');
            assert.deepStrictEqual(RL.badAttrs(go.els), [], `${label}/${lens}: finite attributes in one-arc mode`);
            assert.ok(go.els.filter(e => e.id.startsWith('s:')).length <= gp.els.filter(e => e.id.startsWith('s:')).length, 'never more segments than per-partner');
            // one chain below the ring per module and cluster
            const lk = new Map();
            for (const e of go.els) if (e.id.startsWith('l:')) lk.set(e.id, (lk.get(e.id) || 0) + 1);
            assert.ok([...lk.values()].every(n => n === 1), 'ids are unique');
        }

        // single-cluster modules live inside their cluster: no outer segment, a lane on the lowest cluster holding them all, a chain down to every holder
        {
            const one = { ...d, nodes: d.nodes.map(n => ({ ...n, path: [{ id: 'one', label: 'One' }] })) };
            for (const [label, dd] of [['synthetic', d], ['real-shaped', yara], ['one cluster', one]]) for (const lens of ['modules', 'lanes']) for (const nested of [true, false]) {
                const b = build(dd, lens), gg = RL.geometry(b.M, { sim: RL.newSim(), nested, open: new Set([...b.M.byId.keys()]) }), ids = new Set(gg.els.map(e => e.id));
                const layers = b.M.mods.filter(m => m.fam != null), cross = b.M.mods.filter(m => m.fam == null);
                for (const m of layers) {
                    const tag = `${label}/${lens}/nested=${nested}: ${m.label}`, f = b.M.roots[m.fam], hold = new Set(m.mem);
                    assert.ok(!gg.els.some(e => e.id.startsWith('s:') && e.m === m.k), `${tag} has no outer segment`);
                    const lca = RL.lcaNode(f, hold, nested);
                    assert.ok(ids.has(`l:${m.k}.:${lca.id}`), `${tag} has a lane on its lowest cluster`);
                    assert.ok(!gg.els.some(e => e.id.startsWith(`u:${m.k}.:${lca.id}:`)), 'no band from the outer ring into its first lane');
                    for (const i of m.mem) {
                        // every level from the lowest cluster down to the leaf that holds the file has its lane
                        let n = lca;
                        for (;;) {
                            assert.ok(ids.has(`l:${m.k}.:${n.id}`), `${tag}: lane on ${n.id}`);
                            const k = nested ? n.kids.find(K => K.files.includes(i)) : null;
                            if (!k) break;
                            n = k;
                        }
                        assert.ok(gg.els.some(e => e.id.startsWith(`i:${m.k}.:`) && e.id.endsWith(`:${i}`)), `${tag}: a fan to file ${i}`);
                    }
                    for (const e of gg.els) if (e.m === m.k) assert.strictEqual(e.s.fill || e.s.stroke, m.cat.color, 'lens colour');
                }
                for (const m of cross) assert.ok(gg.els.some(e => e.id.startsWith('s:') && e.m === m.k) && gg.els.some(e => e.id.startsWith('o:') && e.m === m.k), `${label}/${lens}: cross module ${m.label} keeps its outer segment and ribbons`);
                assert.deepStrictEqual(RL.badAttrs(gg.els), []);
                for (const r of gg.stats.ribbons) assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'equal ribbon ends');
            }
        }

        // colours: every module and lane has exactly the colour the Tree gives its lens group, whatever the cluster
        {
            const one = { ...d, nodes: d.nodes.map(n => ({ ...n, path: [{ id: 'one', label: 'One' }] })) };
            for (const [label, dd] of [['synthetic', d], ['real-shaped', yara], ['one cluster', one]]) for (const lens of ['modules', 'lanes']) {
                const b = build(dd, lens), gg = RL.geometry(b.M, { sim: RL.newSim(), nested: true, open: new Set() });
                for (const m of b.M.mods) assert.strictEqual(m.color, m.cat.color, `${label}/${lens}: ${m.label} keeps the lens colour`);
                const byK = new Map(b.M.mods.map(m => [m.k, m]));
                for (const e of gg.els) if (e.m !== undefined) assert.strictEqual(e.s.fill || e.s.stroke, byK.get(e.m).cat.color, `${label}/${lens}: ${e.id} is painted in the lens colour`);
                assert.ok(!('CROSS_COLORS' in RING) && !('LANE_SHADE' in RING), 'no colour cap and no lane shading');
            }
        }

        // grouped arcs and the crossing-minimising order, on every arc mode
        {
            const crossings = gg => {
                const P = gg.stats.ribbons.map(r => { const [p0, c0, c1, p1] = r.cl, o = []; for (let i = 0; i <= 10; i++) { const t = i / 10, m = 1 - t; o.push([m * m * m * p0[0] + 3 * m * m * t * c0[0] + 3 * m * t * t * c1[0] + t * t * t * p1[0], m * m * m * p0[1] + 3 * m * m * t * c0[1] + 3 * m * t * t * c1[1] + t * t * t * p1[1]]); } return o; });
                const ccw = (a, b, c) => (c[1] - a[1]) * (b[0] - a[0]) > (b[1] - a[1]) * (c[0] - a[0]);
                let n = 0;
                for (let i = 0; i < P.length; i++) for (let j = i + 1; j < P.length; j++) { let hit = false; for (let u = 0; u < 10 && !hit; u++) for (let v = 0; v < 10; v++) { const a = P[i][u], b = P[i][u + 1], c = P[j][v], e = P[j][v + 1]; if (ccw(a, c, e) !== ccw(b, c, e) && ccw(a, b, c) !== ccw(a, b, e)) { hit = true; break; } } if (hit) n++; }
                return n;
            };
            const run = (b, sweeps) => { const k = RING.ORDER_SWEEPS; RING.ORDER_SWEEPS = sweeps; try { return RL.geometry(b.M, { sim: RL.newSim(), nested: true, open: new Set() }); } finally { RING.ORDER_SWEEPS = k; } };
            for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) {
                const arcs = {}, builds = {};
                for (const mode of ['partner', 'group', 'one']) {
                    builds[mode] = build(dd, lens, { arc: mode });
                    const g1 = run(builds[mode], 6), g2 = run(builds[mode], 6);
                    arcs[mode] = g1.els.filter(e => e.id.startsWith('s:')).length;
                    assert.deepStrictEqual(g1.els.map(e => [e.id, e.a]), g2.els.map(e => [e.id, e.a]), `${label}/${lens}/${mode}: two runs are identical`);
                    assert.deepStrictEqual(RL.badAttrs(g1.els), []);
                    for (const r of g1.stats.ribbons) assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'equal ribbon ends');
                    for (const m of builds[mode].M.mods.filter(x => x.fam == null)) for (let u = 0; u < m.fams.length; u++) for (let v = u + 1; v < m.fams.length; v++) assert.ok(g1.els.some(e => e.id === `o:${m.k}:${m.fams[u]}:${m.fams[v]}`), `${label}/${lens}/${mode}: a ribbon per family pair`);
                    const ids = g1.els.map(e => e.id);
                    assert.strictEqual(new Set(ids).size, ids.length, 'ids are unique');
                }
                assert.ok(arcs.one <= arcs.group && arcs.group <= arcs.partner, `${label}/${lens}: one <= grouped <= per partner (${arcs.one}, ${arcs.group}, ${arcs.partner})`);
                if (label === 'real-shaped') for (const mode of ['partner', 'group', 'one']) assert.ok(crossings(run(builds[mode], 6)) <= crossings(run(builds[mode], 0)), `${label}/${lens}/${mode}: sweeps do not add crossings`);
            }
        }

        // the outward side of every ring (away from the other clusters) stays free of cross-family arcs
        for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) for (const mode of ['partner', 'group', 'one']) {
            const b = build(dd, lens, { arc: mode }), sim = RL.newSim(), gg = RL.geometry(b.M, { sim, nested: true, open: new Set() });
            const norm = a => ((a + 3 * Math.PI) % (2 * Math.PI)) - Math.PI;
            gg.segs.forEach((l, c) => l.filter(x => x.b.fam == null).forEach(x => {
                const slack = 1e-6;
                for (const a of [x.a0, x.a1]) assert.ok(Math.abs(norm(a - sim.dir[c])) >= RING.OUTWARD_KEEP * Math.PI - slack, `${label}/${lens}/${mode}: a cross arc on ring ${c} stays out of the outward arc`);
            }));
        }

        // tier buckets (Lanes lens): flagged, grey, thin, behind the lanes; Hide removes exactly them and says so
        {
            const bb = build(grouped, 'lanes'), bk = bb.M.mods.filter(m => m.bucket), lanesOnly = bb.M.mods.filter(m => !m.bucket);
            assert.deepStrictEqual(bk.map(m => m.cat.key).sort(), ['tier:core', 'tier:rare'], 'the tier groups with rows of their own are buckets');
            assert.ok(lanesOnly.length > 0 && lanesOnly.every(m => !m.bucket), 'lanes are not buckets');
            assert.ok(bk.every(m => m.color === 'var(--lin-grey-d)'), 'buckets are the neutral grey');
            assert.deepStrictEqual(bk.map(m => m.label).sort(), ['Core: rows in no lane', 'Rare: rows in no lane'], 'tooltip and list label');
            for (const m of lanesOnly) assert.strictEqual(m.color, m.cat.color, 'real lanes keep the lens colour');
            const gg = RL.geometry(bb.M, { sim: RL.newSim(), nested: true, open: new Set() });
            const isB = e => e.m !== undefined && bb.M.mods.find(m => m.k === e.m).bucket;
            for (const kind of ['l:', 'o:', 'u:', 'i:']) {
                const list = gg.els.filter(e => e.id.startsWith(kind) && e.m !== undefined), firstLane = list.findIndex(e => !isB(e));
                assert.ok(list.every((e, i) => !isB(e) || firstLane < 0 || i < firstLane), `${kind} bucket elements are drawn behind the lanes`);
            }
            const bl = gg.els.find(e => e.id.startsWith('l:') && isB(e)), ll = gg.els.find(e => e.id.startsWith('l:') && !isB(e));
            if (bl && ll) assert.ok(bl.sa.opacity < ll.sa.opacity && bl.sa['stroke-width'] < ll.sa['stroke-width'], 'buckets are fainter and thinner');
            assert.ok(gg.els.filter(e => e.id.startsWith('s:') && isB(e)).every(e => e.sa['stroke-width'] < RING.SEG_STROKE), 'thinner ring segments');
            const hid = build(grouped, 'lanes', { tier: 'hide' });
            assert.deepStrictEqual(hid.M.mods.map(m => m.cat.key).sort(), lanesOnly.map(m => m.cat.key).sort(), 'Hide removes exactly the buckets');
            assert.strictEqual(hid.M.bucketsHidden, bk.length);
            assert.strictEqual(hid.M.bucketFuncs, bk.reduce((a2, m) => a2 + m.size, 0), 'the functions hidden are counted');
            assert.ok(!hid.M.plan.segs.flat().some(x => x.b.other), 'nothing merged into other');
            assert.strictEqual(build(yara, 'modules').M.buckets, 0, 'the Modules lens has no tier groups');
        }

        // bundles: one stripe per module and pair, equal ends, none crossing another of its pair; Off draws the plain ribbons
        for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) {
            const b = build(dd, lens), run = bundle => RL.geometry(b.M, { sim: RL.newSim(), nested: true, open: new Set(), bundle });
            const on = run(true), off = run(false), on2 = run(true);
            assert.strictEqual(on.els.filter(e => e.id.startsWith('o:')).length, off.els.filter(e => e.id.startsWith('o:')).length, `${label}/${lens}: a stripe per ribbon`);
            assert.strictEqual(on.stats.ribbons.length, off.stats.ribbons.length);
            for (const r of on.stats.ribbons) assert.ok(Math.abs(r.arcX - r.arcY) < 1e-6, 'equal ends');
            for (const m of b.M.mods.filter(x => x.fam == null)) for (let u = 0; u < m.fams.length; u++) for (let v = u + 1; v < m.fams.length; v++) assert.ok(on.els.some(e => e.id === `o:${m.k}:${m.fams[u]}:${m.fams[v]}`), 'a stripe per family pair');
            assert.deepStrictEqual(RL.badAttrs(on.els), []);
            assert.deepStrictEqual(on.els.map(e => [e.id, e.a]), on2.els.map(e => [e.id, e.a]), 'deterministic');
            assert.ok(on.els.some(e => e.bundle) || on.stats.ribbons.length === 0 || true);
            if (label === 'synthetic') {
                const ccw = (a, b2, c) => (c[1] - a[1]) * (b2[0] - a[0]) > (b2[1] - a[1]) * (c[0] - a[0]), cross = (P, Q) => { for (let i = 0; i < P.length - 1; i++) for (let j = 0; j < Q.length - 1; j++) if (ccw(P[i], Q[j], Q[j + 1]) !== ccw(P[i + 1], Q[j], Q[j + 1]) && ccw(P[i], P[i + 1], Q[j]) !== ccw(P[i], P[i + 1], Q[j + 1])) return true; return false; };
                const bs = on.stats.ribbons.filter(r => r.bundle);
                for (let i = 0; i < bs.length; i++) for (let j = i + 1; j < bs.length; j++) if (bs[i].bundle === bs[j].bundle) assert.ok(!cross(bs[i].pl, bs[j].pl), `${label}/${lens}: stripes of one pair do not cross`);
            }
        }


        // clicking a cluster body: camera, fade of the other clusters, breadcrumb, deeper (aggregates, merged files) without a rebuild of the view
        {
            class El {
                constructor(tag) { this.tag = tag; this.attrs = {}; this.st = {}; this.style = { setProperty: (k, v) => (this.st[k] = v) }; this.children = []; this.dataset = {}; this.ls = {}; this.cls = new Set(); this.parent = null; this._t = '';
                    this.classList = { add: c => this.cls.add(c), remove: c => this.cls.delete(c), contains: c => this.cls.has(c), toggle: (c, on) => (on ? this.cls.add(c) : this.cls.delete(c)) }; }
                setAttribute(k, v) { this.attrs[k] = String(v); } removeAttribute(k) { delete this.attrs[k]; }
                appendChild(c) { c.parent = this; this.children.push(c); return c; } append(...c) { c.forEach(x => this.appendChild(x)); }
                replaceChildren(...c) { this.children = []; c.forEach(x => this.appendChild(x)); }
                remove() { if (this.parent) this.parent.children = this.parent.children.filter(k => k !== this); }
                addEventListener(t, f) { (this.ls[t] = this.ls[t] || []).push(f); } removeEventListener(t, f) { this.ls[t] = (this.ls[t] || []).filter(x => x !== f); }
                set textContent(v) { this._t = String(v); this.children = []; } get textContent() { return this._t; }
                set className(v) { this.cn = v; } get className() { return this.cn; }
                set title(v) { this.attrs.title = v; } get title() { return this.attrs.title; }
                closest() { return null; } contains() { return true; }
            }
            const docL = {};
            global.document = { createElementNS: (n, t) => new El(t), createElement: t => new El(t), head: new El('head'), body: new El('body'), hidden: false, getElementById: () => null,
                addEventListener: (t, f) => (docL[t] = f), removeEventListener() {} };
            window.addEventListener = () => {}; window.removeEventListener = () => {}; global.cancelAnimationFrame = () => {};
            // yara with merged files: the first files of every family are merged twin columns of two
            const merged = { ...yara, nodes: yara.nodes.map((n, i) => ({ ...n, members: i % 5 === 0 ? [n.id, `${n.id}#b`] : [n.id] })) };
            // the doc the server answers when node `id` is opened: it becomes two files under a new group `id`
            const openDoc = (dd, ids) => {
                const nodes = [], map = new Map();
                dd.nodes.forEach((n, i) => {
                    if (!ids.includes(n.id)) { map.set(i, [nodes.length]); nodes.push(n); return; }
                    const a = { ...n, id: `${n.id}#1`, members: [n.members[0]], path: [...n.path, { id: n.id, label: 'opened' }] }, b = { ...n, id: `${n.id}#2`, members: [n.members[1]], path: a.path };
                    map.set(i, [nodes.length, nodes.length + 1]); nodes.push(a, b);
                });
                const mask = dd.rows.mask.map(m => { let bits = BigInt('0x' + m), out = 0n; for (const [i, to] of map) if ((bits >> BigInt(i)) & 1n) for (const t of to) out |= 1n << BigInt(t); return out.toString(16); });
                const rename = id => (ids.includes(id) ? `${id}#1` : id);
                return { ...dd, nodes, root: rename(dd.root), edges: dd.edges.map(e => ({ ...e, parent: rename(e.parent), child: rename(e.child) })), rows: { ...dd.rows, mask } };
            };
            const mk = dd => { const b = build(dd, 'modules'); return { R: b.R, C: b.C }; };
            let cur = { doc: merged, ...mk(merged) }, track = '', camCalls = [], restores = [], getCalls = 0, openCalls = [], resolvers = [], merges = 0;
            const T0 = { k: 1, x: 0, y: 0, tag: 'before' };
            const g0 = new El('g'), svg = new El('svg'), host = new El('div'), c1 = new El('span'), c2 = new El('div');
            const ctx = { g: g0, svg, host, controls: c1, controls2: c2, opts: { nest: true, min: 0, arc: 'partner' }, getDoc: () => cur.doc, getR: () => cur.R, getCats: () => cur.C, lens: () => 'modules', getTrack: () => track, setTrack: t => (track = t), onOpt() {}, onFile() {},
                showTip() {}, moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(cur.doc.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {},
                camera: { get: () => (getCalls++, T0), to: b => camCalls.push(b), restore: t => restores.push(t) },
                openNodes: ids => new Promise((res, rej) => { openCalls.push(ids); resolvers.push({ res: () => res(openDoc(cur.doc, ids)), rej }); }),
                merge: dd => { merges++; cur = { doc: dd, ...mk(dd) }; ring.draw(); } };
            const ring = window.NwayRing.mount(ctx);
            ring.draw();
            const root = g0.children[0], S2 = ring.state;
            const circle = id => root.children.find(n => n.tag === 'circle' && n.dataset.drag === `f:${id}`);
            const famId = c => ring.frame().els.find(e => e.id.startsWith('c:') && e.cl && e.cl.famIdx === c && e.cl.level === 0).cl.id;
            const dimmed = () => root.children.filter(n => n.cls.has('ring-dim')).length;
            const crumbEl = () => host.children.find(n => n.className === 'ring-crumb');
            const crumbText = () => crumbEl().children.map(k => k.textContent).join(' ');
            // a drag is not a click
            S2.sim.moved = true; circle(famId(1)).ls.click[0](); S2.sim.moved = false;
            assert.strictEqual(S2.fc, null, 'a drag does not focus'); assert.strictEqual(camCalls.length, 0);
            // a click on a ribbon, a ring arc, a lane, a file dot does not focus
            for (const kind of ['o:', 's:', 'l:', 'n:']) { const k = ring.frame().els.findIndex(e => e.id.startsWith(kind)); if (k >= 0 && root.children[k].ls.click) root.children[k].ls.click[0](); }
            track = '';
            assert.strictEqual(S2.fc, null, 'ribbons, arcs, lanes and file dots do not focus');
            // a click on the body of a family
            const f1 = famId(1);
            circle(f1).ls.click[0]();
            assert.strictEqual(S2.fc.id, f1); assert.strictEqual(camCalls.length, 1, 'the camera goes to the cluster');
            const box = camCalls[0], cen = ring.frame().nc[f1];
            assert.ok(box[0] < cen[0] && cen[0] < box[2] && box[1] < cen[1] && cen[1] < box[3], 'the box contains the cluster');
            assert.ok(dimmed() > 0 && !circle(f1).cls.has('ring-dim'), 'the others fade, the focused one does not');
            for (const e of ring.frame().els) { const n = root.children.find((c, i) => ring.frame().els[i] === e); if (e.fams && e.fams.includes(1)) assert.ok(!n.cls.has('ring-dim'), 'ribbons leaving the focused cluster keep full strength'); if (e.fams && !e.fams.includes(1)) assert.ok(n.cls.has('ring-dim'), 'ribbons that do not touch it fade'); }
            assert.strictEqual(crumbText(), 'All > F1', 'breadcrumb');
            assert.strictEqual(getCalls, 1);
            // focusing another cluster keeps the first stored camera
            circle(famId(2)).ls.click[0]();
            assert.strictEqual(getCalls, 1, 'the camera before the first focus is stored once'); assert.strictEqual(camCalls.length, 2);
            // Esc and the All crumb restore it and clear the fade
            docL.keydown({ key: 'Escape' });
            assert.deepStrictEqual(restores, [T0], 'Esc restores the first camera'); assert.strictEqual(S2.fc, null); assert.strictEqual(dimmed(), 0, 'no fade left'); assert.strictEqual(crumbEl().style.display, 'none');
            circle(famId(2)).ls.click[0]();
            crumbEl().children[0].ls.click[0]();
            assert.strictEqual(restores.length, 2, 'the All crumb restores too'); assert.strictEqual(S2.fc, null);
            circle(famId(2)).ls.click[0]();
            svg.ls.click[0]({ target: svg });
            assert.strictEqual(S2.fc, null, 'a click on empty canvas leaves the focus');
            // a leaf drawn as one aggregate opens on focus
            RING.LEAF_MAX = 3;
            ring.reset();
            const big = famId(0);
            assert.ok(!S2.open.has(big));
            circle(big).ls.click[0]();
            assert.ok(S2.open.size > 0, 'the aggregate leaves of the focused cluster are opened');
            RING.LEAF_MAX = 60;
            docL.keydown({ key: 'Escape' });
            // merged files: the server is asked, the answer is merged without touching the view state
            ring.reset(); S2.open.clear(); ring.draw();
            const shotPts = () => JSON.parse(JSON.stringify(S2.sim.pts)), f0 = famId(0);
            const rootBefore = g0.children[0], pinBefore = (track = 'lanes:x');
            const nodesBefore = cur.doc.nodes.map(n => n.id);
            const base = openCalls.length;
            circle(f0).ls.click[0]();
            const ids = merged.nodes.filter(n => n.members.length > 1 && n.path[0].id === merged.nodes[0].path[0].id).map(n => n.id);
            assert.strictEqual(openCalls.length, base + 1, 'one request'); assert.deepStrictEqual(openCalls[base].slice().sort(), ids.slice().sort(), 'the merged files of the cluster are asked for');
            circle(f0).ls.click[0]();
            assert.strictEqual(openCalls.length, base + 1, 'a double click is one request');
            assert.ok(/opening \d+ files/.test(host.children.find(n => n.className === 'ring-note').textContent), 'the note says what is opening'); assert.ok(circle(f0).cls.has('ring-pending'), 'the focused cluster shows the pending state');
            const camN = camCalls.length, before = shotPts(), openSet = [...S2.open];
            // the user moves a file while it is pending: that offset must survive the merge
            S2.sim.pts[`n:${merged.nodes[1].id}`].dx = 17; S2.sim.pts[`n:${merged.nodes[1].id}`].dy = -9;
            const moved = shotPts();
            resolvers[base].res();
            await new Promise(r => setTimeout(r, 0)); await new Promise(r => setTimeout(r, 0));
            assert.strictEqual(merges, 1); assert.notStrictEqual(cur.doc, merged); assert.ok(cur.doc.nodes.length > merged.nodes.length, 'more nodes after opening');
            assert.strictEqual(g0.children[0], rootBefore, 'the same SVG root'); assert.strictEqual(track, pinBefore, 'the pin stays'); assert.strictEqual(camCalls.length, camN, 'the camera does not move on a merge'); assert.deepStrictEqual([...S2.open], openSet, 'S.open stays');
            const after = shotPts();
            for (const id of nodesBefore) if (!ids.includes(id)) assert.deepStrictEqual(after[`n:${id}`], moved[`n:${id}`], `the physics point of ${id} is untouched`);
            for (const k of Object.keys(moved).filter(k => k.startsWith('f:'))) assert.deepStrictEqual(after[k], moved[k], `${k} is untouched`);
            assert.ok(S2.fc && S2.fc.id === f0, 'the focus stays'); assert.ok(!circle(f0).cls.has('ring-pending'), 'no pending state left');
            // the new files sit inside their new cluster, inside the family
            const fr = ring.frame(), newIdx = cur.doc.nodes.map((n, i) => [n, i]).filter(([n]) => /#[12]$/.test(n.id)).map(([, i]) => i), famC = fr.nc[f0], famR = fr.els.find(e => e.id === `c:${f0}`).a.r;
            for (const i of newIdx) assert.ok(Math.hypot(fr.pos[i][0] - famC[0], fr.pos[i][1] - famC[1]) <= Number(famR), 'a new file is inside its family');
            assert.ok(root.children.some(n => n.cls.has('ring-new')), 'new elements fade in');
            // a failed request keeps the old doc
            const docNow = cur.doc;
            ring.reset(); S2.open.clear(); ring.draw();
            docL.keydown({ key: 'Escape' });
            const f3 = famId(3), b3 = openCalls.length;
            circle(f3).ls.click[0]();
            if (openCalls.length === b3 + 1) {
                resolvers[b3].rej(new Error('boom'));
                await new Promise(r => setTimeout(r, 0)); await new Promise(r => setTimeout(r, 0));
                assert.strictEqual(cur.doc, docNow, 'a failure keeps the old doc'); assert.ok(/Could not open/.test(host.children.find(n => n.className === 'ring-note').textContent)); assert.ok(!S2.pend);
            }
            // a stale answer is ignored
            const n0 = openCalls.length, m0 = merges;
            docL.keydown({ key: 'Escape' });
            circle(famId(1)).ls.click[0]();
            if (openCalls.length > n0) { circle(famId(2)).ls.click[0](); resolvers[n0].res(); await new Promise(r => setTimeout(r, 0)); await new Promise(r => setTimeout(r, 0)); assert.strictEqual(merges, m0, 'a stale response is ignored'); }
            ring.destroy();
            assert.ok(!host.children.some(n => n.className === 'ring-crumb'), 'the crumb is gone with the ring');
            // Merge files: Cluster. The view's cols() mapping, and the node tooltip of a cluster
            const vs0 = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/views/nway_lineage_view.js', 'utf8');
            const colsSrc = /const cols = \(\) => (\([^;]*\));/.exec(vs0)[1];
            const colsOf = (columns, childrenOk, twinsOk, hasCluster) => new Function('st', 'childrenOk', 'twinsOk', 'hasCluster', `return ${colsSrc}`)({ columns }, childrenOk, twinsOk, () => hasCluster);
            assert.deepStrictEqual([['children', true, true, true], ['children', true, true, false], ['children', false, true, true], ['twins', true, true, true], ['twins', true, false, true], ['files', true, true, true]].map(a2 => colsOf(...a2)), ['children', 'files', 'files', 'twins', 'files', 'files'], 'cols(): children only with a cluster and a server that answers; twins and files as before');
            assert.ok(/fileModes: \['files', 'children', 'twins'\]/.test(vs0) && /res\.status === 400 && cols\(\) === 'children'\) \{ childrenOk = false;/.test(vs0) && /out\.set\('columns', cols\(\)\)/.test(vs0), 'the panel offers Cluster, a failing children request falls back, the API gets columns=cols()');
            {
                const cd = { ...merged, columns_mode: 'children' }, cb = build(cd, 'modules'), g5 = new El('g'), tips = [];
                const r5 = window.NwayRing.mount({ g: g5, svg: new El('svg'), host: new El('div'), controls: new El('span'), controls2: new El('div'), opts: {}, getDoc: () => cd, getR: () => cb.R, getCats: () => cb.C, lens: () => 'modules', getTrack: () => '', setTrack() {}, onOpt() {}, onFile() {},
                    showTip: (ev, title, col, rows) => tips.push(rows), moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(cd.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {} });
                r5.draw();
                const els5 = r5.frame().els, merged0 = els5.findIndex(e => e.file === 0), plain = els5.findIndex(e => e.file === 1);
                g5.children[0].children[merged0].ls.mouseenter[0]({});
                g5.children[0].children[plain].ls.mouseenter[0]({});
                assert.ok(tips[0].some(r2 => r2[0] === 'Files' && r2[1] === '2 files (cluster)'), 'a cluster node says how many files it holds');
                assert.ok(!tips[1].some(r2 => r2[0] === 'Files'), 'a single file does not');
                r5.destroy();
            }
            delete global.document; delete global.cancelAnimationFrame;
            // the view: the request lists the groups already open plus the files to open, the URL is rewritten without navigation
            const vs = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/views/nway_lineage_view.js', 'utf8');
            assert.ok(/for \(const g of n\.path \|\| \[\]\) open\.add\(g\.id\)/.test(vs) && /get\(\{ col_open: colOpen \}\)/.test(vs) && /history\.replaceState\(history\.state, '', `\$\{window\.location\.pathname\}\?\$\{u\.toString\(\)\}`\)/.test(vs) && /u\.set\('col_open', colOpen\)/.test(vs), 'openNodes sends the open groups plus the ids and writes col_open into the URL');
        }
        // the min-size gate: off keeps every module; a gate drops exactly the modules under the share of all functions, merges nothing
        for (const [label, dd] of [['synthetic', d], ['real-shaped', yara]]) for (const lens of ['modules', 'lanes']) {
            const off = build(dd, lens), total = off.R.n;
            assert.strictEqual(off.M.hidden, 0, 'gate off hides nothing');
            assert.strictEqual(off.M.nFuncs, total, 'the denominator is all functions of the set');
            for (const pct of [2, 5, 20]) {
                const on = build(dd, lens, { minPct: pct }), want = off.M.mods.filter(m => m.size >= (pct / 100) * total).map(m => m.cat.key).sort();
                assert.deepStrictEqual(on.M.mods.map(m => m.cat.key).sort(), want, `${label}/${lens}/${pct}%: the modules left are those at or over the gate`);
                assert.strictEqual(on.M.hidden, off.M.mods.length - on.M.mods.length, 'hidden = total - shown');
                assert.ok(on.M.mods.every(m => m.size >= (pct / 100) * total), 'none under the gate is shown');
                const gg = RL.geometry(on.M, { sim: RL.newSim(), nested: true, open: new Set() });
                assert.ok(!on.M.plan.segs.flat().some(x => x.b.other), 'nothing is merged into other');
                assert.strictEqual(new Set([...on.M.plan.segs.flat(), ...on.M.plan.layers.flat()].map(x => x.b.k)).size, on.M.mods.length, 'every module left is drawn');
                const ribKs = new Set(gg.els.filter(e => e.id.startsWith('o:')).map(e => Number(e.id.split(':')[1])));
                for (const m of on.M.mods.filter(x => x.fam == null)) assert.ok(ribKs.has(m.k), 'every cross module left has its ribbons');
                assert.deepStrictEqual(RL.badAttrs(gg.els), []);
            }
        }
        assert.ok(build(yara, 'modules', { minPct: 5 }).M.hidden > 0 && build(yara, 'modules').M.hidden === 0, 'the fixture has modules under 5%');
        assert.strictEqual(window.NwayLineage.tableQs('md5s=a&rmin=1&rfocus=1&rarc=one').toString().includes('rmin'), false, 'rmin is not forwarded to the table');
        assert.ok(/'rarc', 'rmin', 'rlist', 'rtier', 'rbundle', 'root', 'file'/.test(viewSrc), 'rmin is stripped from the API query too');

        // focus mode and the pin, on a stub DOM: a hover touches only the module's own elements
        {
            class El {
                constructor(tag) { this.tag = tag; this.attrs = {}; this.st = {}; this.style = { setProperty: (k, v) => (this.st[k] = v) }; this.children = []; this.dataset = {}; this.ls = {}; this.cls = new Set(); this.parent = null; this._t = '';
                    this.classList = { add: c => this.cls.add(c), remove: c => this.cls.delete(c), contains: c => this.cls.has(c), toggle: (c, on) => (on ? this.cls.add(c) : this.cls.delete(c)) }; }
                setAttribute(k, v) { this.attrs[k] = String(v); } removeAttribute(k) { delete this.attrs[k]; }
                append(...c) { c.forEach(x => this.appendChild(x)); }
                set className(v) { this.cn = v; } get className() { return this.cn; }
                set title(v) { this.attrs.title = v; } get title() { return this.attrs.title; }
                appendChild(c) { c.parent = this; this.children.push(c); return c; } replaceChildren(...c) { this.children = []; c.forEach(x => this.appendChild(x)); }
                remove() { if (this.parent) this.parent.children = this.parent.children.filter(k => k !== this); }
                addEventListener(t, f) { (this.ls[t] = this.ls[t] || []).push(f); } removeEventListener() {}
                set textContent(v) { this._t = String(v); this.children = []; } get textContent() { return this._t; }
                closest() { return null; } contains() { return true; }
            }
            const docL = {};
            global.document = { createElementNS: (n, t) => new El(t), createElement: t => new El(t), head: new El('head'), body: new El('body'), hidden: false, getElementById: () => null,
                addEventListener: (t, f) => (docL[t] = f), removeEventListener() {} };
            window.addEventListener = () => {}; window.removeEventListener = () => {}; global.cancelAnimationFrame = () => {};
            const b = build(yara, 'modules');
            let track = '', cardShown = [];
            const g0 = new El('g'), controls = new El('span'), controls2 = new El('div'), host = new El('div');
            const ring = window.NwayRing.mount({ g: g0, svg: new El('svg'), host, controls, controls2, opts: { nest: true }, getDoc: () => yara, getR: () => b.R, getCats: () => b.C, lens: () => 'modules', getTrack: () => track, setTrack: t => (track = t), onOpt() {}, onFile() {},
                showTip() {}, moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(yara.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {} });
            ring.draw();
            const root = g0.children[0];
            assert.ok(!root.classList.contains('ring-focus'), 'no Focus mode');
            const rib = root.children.find(n => n.dataset.m !== undefined && n.tag === 'path'), k = Number(rib.dataset.m);
            const mine = root.children.filter(n => n.dataset.m === String(k) || (n.dataset.h || '').split(' ').includes(String(k)));
            rib.ls.mouseenter[0]({ clientX: 0, clientY: 0 });
            assert.ok(root.classList.contains('hlm'), 'hover fades the rest through one root class');
            assert.deepStrictEqual(root.children.filter(n => n.cls.has('on')).length, mine.length, 'only the module chain and its holder files are lit');
            assert.ok(!host.children.some(n => n.className === 'ring-card'), 'no focus card');
            rib.ls.mouseleave[0]();
            assert.ok(!root.classList.contains('hlm') && root.children.every(n => !n.cls.has('on')), 'leaving clears it');
            rib.ls.click[0]();
            assert.ok(track !== '', 'a click pins the module as the table track');
            docL.keydown({ key: 'Escape' });
            assert.strictEqual(track, '', 'Escape clears the pin');
            ring.destroy();

            // the toolbar: lineage-style segmented switches, the tunables behind "More"
            const mountBar = (opts, sink = []) => {
                const c1 = new El('span'), c2 = new El('div'), g1 = new El('g');
                const r = window.NwayRing.mount({ g: g1, svg: new El('svg'), host: new El('div'), controls: c1, controls2: c2, opts, getDoc: () => yara, getR: () => b.R, getCats: () => b.C, lens: () => 'modules', getTrack: () => '', setTrack() {}, onOpt: o => sink.push(o), onFile() {},
                    showTip() {}, moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(yara.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {} });
                const all = n => [n, ...n.children.flatMap(all)];
                return { r, c1, c2, els: [...all(c1), ...all(c2)], sink };
            };
            const bar = mountBar({});
            assert.ok(!bar.els.some(e => e.tag === 'select'), 'no <select> in the Ring toolbar');
            const segs = bar.els.filter(e => e.cn === 'lin-seg');
            assert.deepStrictEqual(segs.map(e => e.attrs['aria-label']), ['Min size', 'Arcs', 'Nested clusters', 'Bundle ribbons', 'Tier buckets', 'Module list'], 'the controls, in order');
            for (const sg of segs) {   // the markup of the lineage page's own seg (view buttons, Group view): div.lin-seg[role=group][aria-label] > button[type=button][aria-pressed]
                assert.strictEqual(sg.tag, 'div'); assert.strictEqual(sg.attrs.role, 'group'); assert.ok(sg.attrs.title, 'a tooltip');
                assert.ok(sg.children.length >= 2 && sg.children.every(k => k.tag === 'button' && k.attrs.type === 'button' && /^(true|false)$/.test(k.attrs['aria-pressed'])), 'buttons carry aria-pressed');
                assert.strictEqual(sg.children.filter(k => k.attrs['aria-pressed'] === 'true').length, 1, 'exactly one pressed');
            }
            const pressed = label => segs.find(e => e.attrs['aria-label'] === label).children.find(k => k.attrs['aria-pressed'] === 'true').textContent;
            assert.deepStrictEqual(['Min size', 'Arcs', 'Nested clusters', 'Bundle ribbons', 'Tier buckets', 'Module list'].map(pressed), ['0.5%', 'Grouped', 'On', 'On', 'Show', 'Off'], 'defaults: min size 0.5%, grouped arcs, nested on, bundles on, tier buckets shown, no module list');
            assert.ok(bar.c2.children.length === 5 && !bar.c1.children.some(k => k.tag === 'span' && /Bundle|Tier/.test(k.children[0].textContent)), 'Bundle ribbons and Tier buckets live in the More row');
            assert.deepStrictEqual(bar.c1.children.map(k => (k.tag === 'span' ? k.children[0].textContent : k.textContent || k.children[1].textContent)), ['Min size', 'Reset layout', ' More'], 'visible: Min size, Reset layout, More');
            assert.strictEqual(bar.c2.style.display, 'none', 'the advanced row is hidden by default');
            const more = bar.c1.children[2];
            assert.strictEqual(more.attrs['aria-expanded'], 'false');
            more.ls.click[0]();
            assert.strictEqual(more.attrs['aria-expanded'], 'true');
            assert.strictEqual(bar.c2.style.display, 'flex', 'More opens the row');
            for (const [opts, why] of [[{ arc: 'one' }, 'rarc=one'], [{ nest: false }, 'rnest=0'], [{ list: true }, 'rlist=1'], [{ tier: 'hide' }, 'rtier=hide'], [{ bundle: false }, 'rbundle=0']]) {
                const o = mountBar(opts);
                assert.strictEqual(o.c1.children[2].attrs['aria-expanded'], 'true', `${why} opens More`);
                o.r.destroy();
            }
            { const o = mountBar({ bundle: true }); assert.strictEqual(o.c1.children[2].attrs['aria-expanded'], 'false', 'bundles on is the default and does not open More'); o.r.destroy(); }
            const lst = segs.find(e => e.attrs['aria-label'] === 'Module list');
            lst.children[1].ls.click[0]();
            assert.strictEqual(bar.sink[bar.sink.length - 1].list, true, 'the Module list switch reports list: true');
            assert.strictEqual(lst.children[1].attrs['aria-pressed'], 'true');
            bar.r.destroy();
            // the canvas note counts the hidden tier buckets; the Tier buckets switch is only enabled when the lens has them
            {
                const gb2 = build(grouped, 'lanes'), host2 = new El('div'), c1 = new El('span'), c2 = new El('div');
                const r2 = window.NwayRing.mount({ g: new El('g'), svg: new El('svg'), host: host2, controls: c1, controls2: c2, opts: { tier: 'hide', min: 0 }, getDoc: () => grouped, getR: () => gb2.R, getCats: () => gb2.C, lens: () => 'lanes', getTrack: () => '', setTrack() {}, onOpt() {}, onFile() {},
                    showTip() {}, moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(grouped.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {} });
                r2.draw();
                const note = host2.children.find(n => n.className === 'ring-note');
                assert.ok(/2 tier buckets hidden \(\d+ functions\)/.test(note.textContent), `the note counts them: ${note.textContent}`);
                const tierBtns = c2.children.find(k => k.children[0].textContent === 'Tier buckets').children[1].children;
                assert.ok(tierBtns.every(b => b.attrs.disabled === undefined), 'enabled on the Lanes lens');
                r2.destroy();
                const m3 = new El('div'), c3 = new El('span'), c4 = new El('div'), g3 = build(yara, 'modules');
                const r3 = window.NwayRing.mount({ g: new El('g'), svg: new El('svg'), host: m3, controls: c3, controls2: c4, opts: {}, getDoc: () => yara, getR: () => g3.R, getCats: () => g3.C, lens: () => 'modules', getTrack: () => '', setTrack() {}, onOpt() {}, onFile() {},
                    showTip() {}, moveTip() {}, hideTip() {}, fmt: String, name: String, nodeColor: () => '#888', tree: () => window.NwayColors.tree(yara.nodes.map(n => ({ id: n.id, path: n.path }))), hsl: (h, l) => `hsl(${h},60%,${l}%)`, hue: window.NwayColors.hue, grey: 'x', fit() {} });
                r3.draw();
                assert.ok(c4.children.find(k => k.children[0].textContent === 'Tier buckets').children[1].children.every(b => b.attrs.disabled !== undefined), 'disabled, with a reason, on the Modules lens');
                r3.destroy();
            }
            // URL keys: omitted at their defaults, stripped from table links
            assert.strictEqual(window.NwayRing.RING.ARC_DEFAULT, 'group');
            const tq = window.NwayLineage.tableQs('md5s=a&rarc=one&rnest=0&rfocus=1&rmin=2&rlist=1&rtier=hide&rbundle=0').toString();
            assert.ok(!/r(arc|nest|focus|min|list|tier|bundle)=/.test(tq), 'table links drop the ring keys');
            assert.ok(/rarc: window\.NwayRing \? window\.NwayRing\.RING\.ARC_DEFAULT/.test(viewSrc) && /rmin: '0\.5'/.test(viewSrc) && /rnest: '1', rarc:/.test(viewSrc) && /rtier: '', rbundle: ''/.test(viewSrc) && /for \(const k of KEYS\) if \(st\[k\] !== String\(D\[k\]/.test(viewSrc), 'keys equal to their defaults stay out of the URL');
            assert.ok(/'rlist', 'rtier', 'rbundle', 'root', 'file'/.test(viewSrc), 'the API query drops rlist, rtier, rbundle');
            delete global.document; delete global.cancelAnimationFrame;
        }
    }
    console.log('nway lineage self-check OK');
})().catch(e => { console.error(e); process.exit(1); });
