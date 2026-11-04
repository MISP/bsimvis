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
    console.log('nway lineage self-check OK');
})().catch(e => { console.error(e); process.exit(1); });
