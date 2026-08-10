// Self-check for the File sim sankey grouping (tree frontier + mass folding).
// The graph folds where the tree folds: one shared expansion state drives the
// tree, the Summary rollup, the table's groups and this graph.
// Run: node scripts/test_filesim_sankey.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const src = fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', 'binary_similarity.js'),
    'utf8'
);

// The file is browser-global script, not a module: run it with stubs and hand
// back the pure grouping helpers plus the renderer (whose d3 calls are stubbed
// so the built nodes/links can be inspected without a DOM).
const load = new Function('window', 'document', 'd3', `
    ${src}
    return {
        fileSimSankeyGroups,
        fileSimNsPath,
        renderFileSimSankey,
        // Folding is the tree's state now, not a private frontier of the graph.
        setOpen: (ids) => { fileSimTreeOpen = new Set(ids); },
        setScale: (s) => { fileSimScale = s; },
        // The tree is derived from the cached tag summary, so grouping needs it.
        setRows: (rows) => { binSimDataCache = { tags_summary: rows, functions_metadata: {} }; },
    };
`);

const chain = new Proxy(function () {}, {
    get: () => chain,
    apply: () => chain,
});
let captured = null;
const d3Stub = {
    select: () => chain,
    sankey: () => {
        const layout = (graph) => {
            captured = graph;
            graph.nodes.forEach(n => Object.assign(n, { x0: 0, x1: 15, y0: 0, y1: 10 }));
            graph.links.forEach(l => {
                l.source = graph.nodes[l.source];
                l.target = graph.nodes[l.target];
            });
            return graph;
        };
        ['nodeWidth', 'nodePadding', 'nodeAlign', 'nodeSort', 'extent'].forEach(k => {
            layout[k] = () => layout;
        });
        return layout;
    },
};
const container = { innerHTML: '', clientWidth: 900, clientHeight: 500 };
const M = load(
    { addEventListener() {} },
    { getElementById: (id) => (id === 'bin-sim-filesim-sankey' ? container : null) },
    d3Stub
);

// bins: {binIdx: [count_a, weight_a, count_b, weight_b]}
const row = (tag_id, o = {}) => Object.assign({
    tag_id, score: 0.9, matched_weight: 10,
    weight_a: 0, weight_b: 0,
    unique_count_a: 0, unique_count_b: 0, unique_weight_a: 0, unique_weight_b: 0,
    bins: {},
}, o);

const rows = [
    row('lib:libc:2.31', { bins: { '19': [10, 100, 9, 90] }, weight_a: 100, weight_b: 90, unique_count_a: 2, unique_weight_a: 20 }),
    row('lib:libc:2.35', { bins: { '10': [4, 40, 4, 40] }, weight_a: 40, weight_b: 40, unique_count_b: 1, unique_weight_b: 5 }),
    row('lib:ssl:3.0', { bins: { '18': [3, 30, 3, 30] }, weight_a: 30, weight_b: 30 }),
    row('original_code', { bins: { '5': [7, 70, 6, 60] }, weight_a: 70, weight_b: 60, unique_count_a: 5, unique_weight_a: 50 }),
];

const by = (gs) => Object.fromEntries(gs.map(g => [g.key, g]));
M.setRows(rows);

// Libraries open, each library folded: the two libc versions become one node,
// and ssl -- which has a single version -- is already a leaf.
M.setOpen(['libraries']);
let g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['libraries/libc', 'libraries/ssl', 'original']);
assert.strictEqual(g['libraries/libc'].sharedA, 14);      // 10 + 4
assert.strictEqual(g['libraries/libc'].sharedB, 13);      // 9 + 4
assert.strictEqual(g['libraries/libc'].uniqA, 2);
assert.strictEqual(g['libraries/libc'].uniqB, 1);
assert.strictEqual(g['libraries/libc'].expandable, true); // version level below
assert.strictEqual(g['libraries/libc'].label, 'libc');
// Original holds its mass directly, so there is nothing under it to open.
assert.strictEqual(g['original'].expandable, false);
assert.strictEqual(g['original'].label, 'Original');

// Features metric reads the weight_* fields instead of the bin counts.
g = by(M.fileSimSankeyGroups(rows, 'features'));
assert.strictEqual(g['libraries/libc'].sharedA, 140);
assert.strictEqual(g['libraries/libc'].uniqA, 20);
assert.strictEqual(g['original'].uniqA, 50);

// Everything folded: the whole Libraries group draws as one node.
M.setOpen([]);
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['libraries', 'original']);
assert.strictEqual(g['libraries'].sharedA, 17);           // 10 + 4 + 3
assert.strictEqual(g['libraries'].tags, 3);

// Drilling into one library splits only that library; its siblings stay folded.
// A single global depth setting could not express this -- the tree can.
M.setOpen(['libraries', 'libraries/libc']);
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(
    Object.keys(g).sort(),
    ['lib:libc:2.31', 'lib:libc:2.35', 'libraries/ssl', 'original']
);
assert.strictEqual(g['lib:libc:2.31'].label, '2.31');
assert.strictEqual(g['libraries/ssl'].label, 'ssl');

// Sorted by total mass, biggest first, and zero-mass tags dropped.
const withDead = rows.concat([row('lib:dead:1.0')]);
M.setRows(withDead);
M.setOpen(['libraries']);
const sorted = M.fileSimSankeyGroups(withDead, 'count');
assert.strictEqual(sorted[0].key, 'libraries/libc');
assert.ok(!sorted.some(x => x.key === 'libraries/dead'));
M.setRows(rows);

// ---- Graph shape --------------------------------------------------------
// Regression: a side node must carry exactly one category. One node feeding both
// the shared and the unmatched bucket reads as "all of it is shared AND unique".
M.setOpen(['libraries']);
M.setScale('count');
M.renderFileSimSankey({
    tags_summary: rows,
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
assert.ok(captured, 'sankey layout was never called');

const outDeg = new Map(), inDeg = new Map();
captured.links.forEach(l => {
    outDeg.set(l.source.id, (outDeg.get(l.source.id) || 0) + 1);
    inDeg.set(l.target.id, (inDeg.get(l.target.id) || 0) + 1);
});
captured.nodes.forEach(n => {
    if (n.align === 0) {
        assert.strictEqual(outDeg.get(n.id) || 0, 1, `left node ${n.id} must feed exactly one bucket`);
        assert.strictEqual(inDeg.get(n.id) || 0, 0, `left node ${n.id} must have no input`);
    }
    if (n.align === 2) {
        assert.strictEqual(inDeg.get(n.id) || 0, 1, `right node ${n.id} must receive from exactly one bucket`);
        assert.strictEqual(outDeg.get(n.id) || 0, 0, `right node ${n.id} must have no output`);
    }
});

const nodeById = Object.fromEntries(captured.nodes.map(n => [n.id, n]));
const idx = M.fileSimSankeyGroups(rows, 'count').findIndex(x => x.key === 'original');
const linkVal = (from, to) => captured.links.find(l => l.source.id === from && l.target.id === to).value;
// original_code: 7 shared / 5 unique in A, split across two left nodes.
assert.ok(nodeById[`fsk_as_${idx}`] && nodeById[`fsk_au_${idx}`], 'original_code must split shared vs unmatched on the A side');
assert.strictEqual(linkVal(`fsk_as_${idx}`, `fsk_s_${idx}`), 7);
assert.strictEqual(linkVal(`fsk_au_${idx}`, `fsk_ua_${idx}`), 5);
assert.strictEqual(linkVal(`fsk_s_${idx}`, `fsk_bs_${idx}`), 6);
// B has no unmatched original_code, so no such node exists on either side.
assert.ok(!nodeById[`fsk_bu_${idx}`] && !nodeById[`fsk_ub_${idx}`]);
// Shared and unmatched rows of one tag keep the same order in every column.
assert.ok(nodeById[`fsk_as_${idx}`].sort < nodeById[`fsk_au_${idx}`].sort);

// A tag's rows are re-stacked flush against each other; only tag changes get a gap.
[0, 1, 2].forEach(col => {
    const colNodes = captured.nodes.filter(n => n.align === col).sort((a, b) => a.y0 - b.y0);
    colNodes.forEach((n, k) => {
        if (k === 0) return;
        const prev = colNodes[k - 1];
        if (n.tagIdx === prev.tagIdx) {
            assert.strictEqual(n.y0, prev.y1, `${n.id} must sit flush under ${prev.id}`);
        } else {
            assert.ok(n.y0 > prev.y1, `${n.id} must be separated from ${prev.id}`);
        }
    });
});

// ---- Flag stage ---------------------------------------------------------
// The second axis: provenance says whose code matched, flags say what that code
// does. The stage only appears when something is flagged, and it must conserve
// mass -- every unit leaving a provenance node arrives at the shared node.
// Cells: [w_shared_a, w_shared_b, w_uniq_a, w_uniq_b, then the same as counts].
const flagMatrix = {
    'lib:libc:2.31': { 'flag:suspicious': [40, 30, 0, 0, 4, 3, 0, 0] },
};

M.setOpen(['libraries', 'libraries/libc']);
M.setScale('count');
const fgroups = M.fileSimSankeyGroups(rows, 'count', flagMatrix);
const fi = fgroups.findIndex(x => x.key === 'lib:libc:2.31');
assert.strictEqual(fgroups[fi].flagA, 4);
assert.strictEqual(fgroups[fi].flagB, 3);
// A group nobody flagged carries no cells at all.
assert.strictEqual(fgroups.find(x => x.key === 'original').flagA, 0);

captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    flag_matrix: flagMatrix,
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
const fNodes = Object.fromEntries(captured.nodes.map(n => [n.id, n]));
const fLink = (from, to) => captured.links.find(l => l.source.id === from && l.target.id === to).value;

// Five columns now, with the tag columns pushed to the outside.
assert.strictEqual(fNodes[`fsk_as_${fi}`].align, 0);
assert.strictEqual(fNodes[`fsk_s_${fi}`].align, 2);
assert.strictEqual(fNodes[`fsk_bs_${fi}`].align, 4);
assert.strictEqual(fNodes[`fsk_fl_a_${fi}_0`].align, 1);
assert.strictEqual(fNodes[`fsk_fl_b_${fi}_0`].align, 3);

// 4 of libc's 10 shared functions on A are flagged; the other 6 are not, and
// unflagged is a remainder rather than anything the backend stored.
assert.strictEqual(fLink(`fsk_as_${fi}`, `fsk_fl_a_${fi}_0`), 4);
assert.strictEqual(fLink(`fsk_as_${fi}`, `fsk_fl_a_${fi}_rest`), 6);
assert.strictEqual(fLink(`fsk_fl_a_${fi}_0`, `fsk_s_${fi}`), 4);
assert.strictEqual(fLink(`fsk_fl_a_${fi}_rest`, `fsk_s_${fi}`), 6);
assert.strictEqual(fLink(`fsk_fl_b_${fi}_0`, `fsk_bs_${fi}`), 3);
assert.strictEqual(fLink(`fsk_s_${fi}`, `fsk_fl_b_${fi}_rest`), 6);

// An unflagged tag skips the stage instead of drawing an empty column for it.
const oi = fgroups.findIndex(x => x.key === 'original');
assert.strictEqual(fLink(`fsk_as_${oi}`, `fsk_fl_a_${oi}_rest`), 7);
assert.ok(!fNodes[`fsk_fl_a_${oi}_0`], 'no flag node where nothing was flagged');

// Mass in equals mass out at every flag node.
captured.nodes.filter(n => n.id.startsWith('fsk_fl_')).forEach(n => {
    const into = captured.links.filter(l => l.target.id === n.id).reduce((s, l) => s + l.value, 0);
    const outOf = captured.links.filter(l => l.source.id === n.id).reduce((s, l) => s + l.value, 0);
    assert.strictEqual(into, outOf, `${n.id} must conserve mass`);
});

// With no flags anywhere, the chart keeps the three columns it always had.
captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    flag_matrix: {},
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
assert.ok(!captured.nodes.some(n => n.id.startsWith('fsk_fl_')));
assert.deepStrictEqual([...new Set(captured.nodes.map(n => n.align))].sort(), [0, 1, 2]);

console.log('OK: file sim sankey grouping + graph shape + tag blocks + flag stage');
