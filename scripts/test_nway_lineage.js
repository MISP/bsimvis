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
assert.deepStrictEqual(fn.info.get('c:file:b'), { tk: 1, born: 1, lost: 0 });
assert.deepStrictEqual(fn.info.get('c:file:d'), { tk: 1, born: 0, lost: 0 });
assert.deepStrictEqual(info('lanes:3').info.get('c:file:c'), { tk: 0, born: 0, lost: 1 }, 'lost: in the parent, not here');
assert.strictEqual(info('modules:2').holders, 2);
assert.strictEqual(info('core:every').holders, 4);
assert.strictEqual(info('deltas:c:file:b').holders, 2, 'deltas key is the column id, which has colons');
assert.strictEqual(info('nope:x'), null, 'an unknown lens tracks nothing');
const filtered = trackInfo(some.L, some.R, trackRows(some.R, some.cats, 'fn:c:func:a:1'));
assert.strictEqual(filtered.holders, 0, 'a row the filters drop is not tracked');

console.log('nway lineage self-check OK');
