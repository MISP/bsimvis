// Self-check for the File sim sankey grouping (namespace frontier + mass folding).
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
        fileSimNsOverride,
        renderFileSimSankey,
        setDepth: (d) => { fileSimDepth = d; },
        setScale: (s) => { fileSimScale = s; },
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

// Depth 2 (library): the two libc versions fold into one node, ssl stays alone.
M.setDepth(2);
let g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['lib:libc', 'lib:ssl', 'original_code']);
assert.strictEqual(g['lib:libc'].sharedA, 14);      // 10 + 4
assert.strictEqual(g['lib:libc'].sharedB, 13);      // 9 + 4
assert.strictEqual(g['lib:libc'].uniqA, 2);
assert.strictEqual(g['lib:libc'].uniqB, 1);
assert.strictEqual(g['lib:libc'].expandable, true); // version level still below
assert.strictEqual(g['lib:libc'].label, 'libc');
assert.strictEqual(g['original_code'].expandable, false);
assert.strictEqual(g['original_code'].label, 'Original Code');

// Features metric reads the weight_* fields instead of the bin counts.
g = by(M.fileSimSankeyGroups(rows, 'features'));
assert.strictEqual(g['lib:libc'].sharedA, 140);
assert.strictEqual(g['lib:libc'].uniqA, 20);
assert.strictEqual(g['original_code'].uniqA, 50);

// Depth 1 (namespace): the whole lib namespace becomes one node.
M.setDepth(1);
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['lib', 'original_code']);
assert.strictEqual(g['lib'].sharedA, 17);           // 10 + 4 + 3
assert.strictEqual(g['lib'].tags, 3);

// Per-node override beats the depth setting in both directions.
M.setDepth(1);
M.fileSimNsOverride.set('lib', 'open');
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['lib:libc', 'lib:ssl', 'original_code']);

M.setDepth(3);
M.fileSimNsOverride.clear();
M.fileSimNsOverride.set('lib:libc', 'closed');
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['lib:libc', 'lib:ssl:3.0', 'original_code']);
assert.strictEqual(g['lib:ssl:3.0'].label, 'ssl 3.0');

// Sorted by total mass, biggest first, and zero-mass tags dropped.
M.fileSimNsOverride.clear();
M.setDepth(2);
const sorted = M.fileSimSankeyGroups(rows.concat([row('lib:dead:1.0')]), 'count');
assert.strictEqual(sorted[0].key, 'lib:libc');
assert.ok(!sorted.some(x => x.key === 'lib:dead'));

// ---- Graph shape --------------------------------------------------------
// Regression: a side node must carry exactly one category. One node feeding both
// the shared and the unmatched bucket reads as "all of it is shared AND unique".
M.fileSimNsOverride.clear();
M.setDepth(2);
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
const idx = M.fileSimSankeyGroups(rows, 'count').findIndex(x => x.key === 'original_code');
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

console.log('OK: file sim sankey grouping + graph shape + tag blocks');
