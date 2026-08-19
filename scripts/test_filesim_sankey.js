// Self-check for the File sim sankey grouping (tree frontier + mass folding)
// and for the axis picker over it. The graph folds where the tree folds: one
// shared expansion state drives the tree, the Summary rollup, the table's groups
// and this graph.
//
// The graph can be grouped by any one of four axes, or by a cross of two. Every
// mode is a marginal of the single joint table the backend stores, so switching
// axes is a pure re-render.
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
const load = new Function('window', 'document', 'd3', 'TagColor', `
    ${src}
    return {
        fileSimSankeyGroups,
        fileSimJointMarginal,
        fileSimTree,
        fileSimAvailableAxes,
        fileSimAxisKey,
        renderFileSimSankey,
        // Folding is the tree's state now, not a private frontier of the graph.
        setOpen: (ids) => { fileSimTreeOpen = new Set(ids); },
        setScale: (s) => { fileSimScale = s; },
        // Which axis is on each side; '' on B is a single-axis view.
        setAxis: (a, b) => { fileSimAxisA = a; fileSimAxisB = b; },
        // Every axis has a tree, and each is built from that axis's own summary
        // rows in the cache -- so grouping needs whichever summaries are in play.
        setRows: (rows, extra) => {
            binSimDataCache = Object.assign(
                { tags_summary: rows, functions_metadata: {} }, extra || {});
        },
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
// Colours are a tag's identity now, so the graph asks `TagColor` for them. The
// real derivation is checked in `test_tag_colors.js`; here a legible stub keeps
// the assertions about *which* tag a node was coloured for.
// The split helpers are not stubbed away: the tree's nesting is what the graph
// folds along, so it has to be the real rule. This is the default config the
// browser gets from `/api/tags/colors` -- colon levels, `#` starts the detail.
const tagColorStub = {
    config: () => ({}),
    forTag: (id, o) => ((o && o.gray) ? `gray(${id})` : `color(${id})`),
    groupId: (id) => String(id).split('#')[0],
    levels: (id) => ({ segs: String(id).split('#')[0].split(':').filter(Boolean) }),
    prefixes: (id) => {
        const segs = String(id).split('#')[0].split(':').filter(Boolean);
        return segs.slice(0, -1).map((_, i) => segs.slice(0, i + 1).join(':'));
    },
};
const M = load(
    { addEventListener() {} },
    { getElementById: (id) => (id === 'bin-sim-filesim-sankey' ? container : null) },
    d3Stub,
    tagColorStub
);

// bins: {binIdx: [count_a, weight_a, count_b, weight_b]}
const row = (tag_id, o = {}) => Object.assign({
    tag_id, score: 0.9, matched_weight: 10,
    weight_a: 0, weight_b: 0,
    unique_count_a: 0, unique_count_b: 0, unique_weight_a: 0, unique_weight_b: 0,
    bins: {},
}, o);

const rows = [
    row('origin:lib:libc:2.31', { bins: { '19': [10, 100, 9, 90] }, weight_a: 100, weight_b: 90, unique_count_a: 2, unique_weight_a: 20 }),
    row('origin:lib:libc:2.35', { bins: { '10': [4, 40, 4, 40] }, weight_a: 40, weight_b: 40, unique_count_b: 1, unique_weight_b: 5 }),
    row('origin:lib:ssl:3.0', { bins: { '18': [3, 30, 3, 30] }, weight_a: 30, weight_b: 30 }),
    row('original_code', { bins: { '5': [7, 70, 6, 60] }, weight_a: 70, weight_b: 60, unique_count_a: 5, unique_weight_a: 50 }),
];

const by = (gs) => Object.fromEntries(gs.map(g => [g.key, g]));
M.setRows(rows);
M.setAxis('origin', '');

// Every key here is a tag id, and a real prefix of the rows it folds -- which is
// what makes a node's colour, its index buckets and the scope it sends to the
// backend the same string.
//
// Libraries open, each library folded: the two libc versions become one node.
M.setOpen(['origin', 'origin:lib']);
let g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(
    Object.keys(g).sort(),
    ['origin:lib:libc', 'origin:lib:ssl', 'original_code']
);
assert.strictEqual(g['origin:lib:libc'].sharedA, 14);      // 10 + 4
assert.strictEqual(g['origin:lib:libc'].sharedB, 13);      // 9 + 4
assert.strictEqual(g['origin:lib:libc'].uniqA, 2);
assert.strictEqual(g['origin:lib:libc'].uniqB, 1);
assert.strictEqual(g['origin:lib:libc'].expandable, true); // version level below
assert.strictEqual(g['origin:lib:libc'].label, 'libc');
// An id with no levels holds its mass directly: nothing under it to open.
assert.strictEqual(g['original_code'].expandable, false);
assert.strictEqual(g['original_code'].label, 'original_code');

// Features metric reads the weight_* fields instead of the bin counts.
g = by(M.fileSimSankeyGroups(rows, 'features'));
assert.strictEqual(g['origin:lib:libc'].sharedA, 140);
assert.strictEqual(g['origin:lib:libc'].uniqA, 20);
assert.strictEqual(g['original_code'].uniqA, 50);

// Everything folded: every origin tag draws as one node.
M.setOpen([]);
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(Object.keys(g).sort(), ['origin', 'original_code']);
assert.strictEqual(g['origin'].sharedA, 17);               // 10 + 4 + 3
assert.strictEqual(g['origin'].tags, 3);

// Drilling into one library splits only that library; its siblings stay folded.
// A single global depth setting could not express this -- the tree can.
M.setOpen(['origin', 'origin:lib', 'origin:lib:libc']);
g = by(M.fileSimSankeyGroups(rows, 'count'));
assert.deepStrictEqual(
    Object.keys(g).sort(),
    ['origin:lib:libc:2.31', 'origin:lib:libc:2.35', 'origin:lib:ssl', 'original_code']
);
assert.strictEqual(g['origin:lib:libc:2.31'].label, '2.31');
assert.strictEqual(g['origin:lib:ssl'].label, 'ssl');

// Sorted by total mass, biggest first, and zero-mass tags dropped.
const withDead = rows.concat([row('origin:lib:dead:1.0')]);
M.setRows(withDead);
M.setOpen(['origin', 'origin:lib']);
const sorted = M.fileSimSankeyGroups(withDead, 'count');
assert.strictEqual(sorted[0].key, 'origin:lib:libc');
assert.ok(!sorted.some(x => x.key === 'origin:lib:dead'));
M.setRows(rows);

// ---- Graph shape --------------------------------------------------------
// Regression: a side node must carry exactly one category. One node feeding both
// the shared and the unmatched bucket reads as "all of it is shared AND unique".
M.setOpen(['origin', 'origin:lib']);
M.setScale('count');
M.setAxis('origin', '');
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
// original_code: 7 shared / 5 unique in A, split across two left nodes. With no
// second axis every group has exactly one shared bucket, hence the `_0` suffix.
assert.ok(nodeById[`fsk_as_${idx}`] && nodeById[`fsk_au_${idx}`], 'original_code must split shared vs unmatched on the A side');
assert.strictEqual(linkVal(`fsk_as_${idx}`, `fsk_s_${idx}_0`), 7);
assert.strictEqual(linkVal(`fsk_au_${idx}`, `fsk_ua_${idx}`), 5);
assert.strictEqual(linkVal(`fsk_s_${idx}_0`, `fsk_bs_${idx}`), 6);
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

// ---- The joint table ----------------------------------------------------
// The backend stores one crossing, keyed by origin parent on the outside and by
// the other three axes packed into the inner key. Every view is a marginal.
const SEP = '\u001f';
// One slot per FILESIM_JOINT_INNER axis, in that order. A key of the wrong
// arity is dropped rather than mis-parsed, so this pads to the full width --
// the fixture stopped at three when the axis list grew past it, and every
// assertion below it had been silently reading an empty marginal since.
const JOINT_INNER_WIDTH = 9;   // severity, category, user, capa, mitre, yara, family, vuln, ruleset
const jkey = (sev, cat, usr) => {
    const slots = new Array(JOINT_INNER_WIDTH).fill('');
    [sev, cat, usr].forEach((v, i) => { slots[i] = v; });
    return slots.join(SEP);
};
// Cells: [w_shared_a, w_shared_b, w_uniq_a, w_uniq_b, then the same as counts].
const joint = {
    'origin:lib:libc:2.31': {
        [jkey('severity:high', 'category:network', '')]: [40, 30, 0, 0, 4, 3, 0, 0],
        [jkey('severity:low', 'category:util', '')]: [20, 20, 0, 0, 2, 2, 0, 0],
    },
    'original_code': {
        [jkey('severity:high', 'category:network + category:crypto', '')]: [10, 10, 0, 0, 1, 1, 0, 0],
    },
};

// Origin x Behavior: which part of libc's match is network code.
let mg = M.fileSimJointMarginal(joint, 'origin', 'category');
assert.strictEqual(mg['origin:lib:libc:2.31']['category:network'][4], 4);
assert.strictEqual(mg['origin:lib:libc:2.31']['category:util'][4], 2);

// Severity x Behavior: the cross the axis picker exists for. Origin is summed
// away, so libc's and original_code's high-severity network mass combine.
mg = M.fileSimJointMarginal(joint, 'severity', 'category');
assert.strictEqual(mg['severity:high']['category:network'][4], 4);
assert.strictEqual(mg['severity:high']['category:network + category:crypto'][4], 1);
assert.strictEqual(mg['severity:low']['category:util'][4], 2);

// A single axis is the same call with no B: one bucket per A key.
mg = M.fileSimJointMarginal(joint, 'severity', '');
assert.deepStrictEqual(Object.keys(mg).sort(), ['severity:high', 'severity:low']);
assert.strictEqual(mg['severity:high'][''][4], 5);   // 4 from libc + 1 from original

// The A side expands combos back to single tags so its nodes line up with the
// axis summary rows; overlapping axes therefore double-count, exactly as those
// rows already do.
mg = M.fileSimJointMarginal(joint, 'category', 'severity');
assert.strictEqual(mg['category:network']['severity:high'][4], 5);
assert.strictEqual(mg['category:crypto']['severity:high'][4], 1);

// ---- Crossed stage ------------------------------------------------------
// The second axis: origin says whose code matched, behaviour says what that code
// does. The stage only appears when the crossed axis has mass, and it must
// conserve -- every unit leaving an origin node arrives at the shared node.
M.setOpen(['origin', 'origin:lib', 'origin:lib:libc']);
M.setScale('count');
M.setAxis('origin', 'category');
const crossed = M.fileSimJointMarginal(joint, 'origin', 'category');
const fgroups = M.fileSimSankeyGroups(rows, 'count', crossed);
const fi = fgroups.findIndex(x => x.key === 'origin:lib:libc:2.31');
assert.strictEqual(fgroups[fi].flagA, 6);   // 4 network + 2 util
assert.strictEqual(fgroups[fi].flagB, 5);
// A group the crossed axis says nothing about carries no cells at all.
assert.strictEqual(fgroups.find(x => x.key === 'origin:lib:ssl').flagA, 0);

captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    joint,
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
const fNodes = Object.fromEntries(captured.nodes.map(n => [n.id, n]));
const fLink = (from, to) => {
    const l = captured.links.find(x => x.source.id === from && x.target.id === to);
    assert.ok(l, `expected a link ${from} -> ${to}`);
    return l.value;
};

// Five columns now, with the axis-A columns pushed to the outside.
assert.strictEqual(fNodes[`fsk_as_${fi}_0`].align, 0);
assert.strictEqual(fNodes[`fsk_fl_a_${fi}_0`].align, 1);
assert.strictEqual(fNodes[`fsk_s_${fi}_0`].align, 2);
assert.strictEqual(fNodes[`fsk_fl_b_${fi}_0`].align, 3);
assert.strictEqual(fNodes[`fsk_bs_${fi}_0`].align, 4);
assert.deepStrictEqual([...new Set(captured.nodes.map(n => n.align))].sort(), [0, 1, 2, 3, 4]);

// libc 2.31 has 10 shared functions on A: 4 network, 2 util, and 4 with no
// behaviour at all. The untagged remainder is derived, never stored.
assert.strictEqual(fLink(`fsk_as_${fi}_0`, `fsk_fl_a_${fi}_0`), 4);
assert.strictEqual(fLink(`fsk_fl_a_${fi}_0`, `fsk_s_${fi}_0`), 4);
assert.strictEqual(fLink(`fsk_s_${fi}_0`, `fsk_fl_b_${fi}_0`), 3);
const restK = fgroups[fi].flags.size;   // the remainder bucket is drawn last
assert.strictEqual(fLink(`fsk_as_${fi}_${restK}`, `fsk_fl_a_${fi}_${restK}`), 4);

// Mass in equals mass out at every crossed node.
captured.nodes.filter(n => n.id.startsWith('fsk_fl_')).forEach(n => {
    const into = captured.links.filter(l => l.target.id === n.id).reduce((s, l) => s + l.value, 0);
    const outOf = captured.links.filter(l => l.source.id === n.id).reduce((s, l) => s + l.value, 0);
    assert.strictEqual(into, outOf, `${n.id} must conserve mass`);
});

// ---- Single-axis modes over the other three axes -------------------------
// Every axis has its own tree. A non-origin axis's rows are already at their
// display parent, so each row is a top-level node labelled by its leaf segment,
// with whatever `children` the backend nested under it below.
const sevRows = [
    row('severity:high', { bins: { '19': [5, 50, 4, 40] }, weight_a: 50, weight_b: 40, unique_count_a: 1, unique_weight_a: 10 }),
    row('severity:low', { bins: { '10': [2, 20, 2, 20] }, weight_a: 20, weight_b: 20 }),
];
M.setRows(rows, { severity_summary: sevRows });
M.setAxis('severity', '');
const sg = by(M.fileSimSankeyGroups(sevRows, 'count'));
assert.deepStrictEqual(Object.keys(sg).sort(), ['severity:high', 'severity:low']);
assert.strictEqual(sg['severity:high'].label, 'high');
assert.strictEqual(sg['severity:high'].sharedA, 5);
assert.strictEqual(sg['severity:high'].uniqA, 1);
assert.strictEqual(sg['severity:high'].expandable, false, 'no tree above severity');

captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    severity_summary: sevRows,
    joint,
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
// Reading the severity axis must not fall back to the origin rows.
assert.ok(captured.nodes.some(n => /severity|high/.test(n.name)), 'severity axis must draw severity rows');
assert.ok(!captured.nodes.some(n => /libc/.test(n.name)), 'origin rows must not leak into the severity axis');

// ---- The tree each axis gets --------------------------------------------
// Behaviour rows arrive rolled up to their group with the leaves nested, so the
// tree is that nesting read straight off the rows: `network` over `c2` / `dns`.
const catRows = [
    row('category:network', {
        bins: { '19': [6, 60, 5, 50] }, weight_a: 60, weight_b: 50,
        children: [
            // Leaves sort by composition, the same measure the tree shows.
            row('category:network:c2', { bins: { '19': [4, 40, 4, 40] } }),
            row('category:network:dns', { bins: { '19': [2, 20, 1, 10] } }),
        ],
    }),
    row('category:util', { bins: { '10': [1, 10, 1, 10] } }),
];
const catTree = M.fileSimTree(catRows, 'category');
// Every level sorts by similarity, which is what Origin always did -- the axes
// that used to sort their top level by mass now agree with it. `util` (1 vs 1,
// 100%) leads `network` (mean of c2 100% and dns 50%, so 75%) despite carrying
// a fraction of the mass.
assert.deepStrictEqual(catTree.children.map(n => n.label), ['util', 'network']);
const catNetwork = catTree.children.find(n => n.label === 'network');
assert.deepStrictEqual(catNetwork.children.map(n => n.label), ['c2', 'dns']);
// A node id is the tag id, which is what scopes the table and the flow.
assert.strictEqual(catNetwork.children[0].id, 'category:network:c2');
// The parent row is the merge of its leaves, so the branch rebuilds from them
// rather than adding the row's own mass on top.
assert.strictEqual(catNetwork.a, 6);

// Severity is ordinal: its tree reads worst-first, not biggest-first.
const sevTree = M.fileSimTree([
    row('severity:low', { bins: { '10': [9, 90, 9, 90] } }),
    row('severity:high', { bins: { '19': [1, 10, 1, 10] } }),
], 'severity');
assert.deepStrictEqual(sevTree.children.map(n => n.label), ['high', 'low']);

// An axis this pair carries no tags on is offered nowhere, and selecting one
// anyway falls back to an axis that has rows instead of blanking the view.
M.setRows(rows, { severity_summary: sevRows });
assert.deepStrictEqual(M.fileSimAvailableAxes(), ['origin', 'severity']);
M.setAxis('category', '');
assert.strictEqual(M.fileSimAxisKey(), 'origin', 'an empty axis falls back');
M.setAxis('severity', '');
assert.strictEqual(M.fileSimAxisKey(), 'severity');

// Crossing an axis with itself is meaningless and reads as a single-axis view.
M.setAxis('severity', 'severity');
captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    severity_summary: sevRows,
    joint,
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
assert.deepStrictEqual([...new Set(captured.nodes.map(n => n.align))].sort(), [0, 1, 2]);

// With no crossed mass anywhere, the chart keeps the three columns it always had.
M.setAxis('origin', 'category');
captured = null;
M.renderFileSimSankey({
    tags_summary: rows,
    joint: {},
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
});
assert.ok(!captured.nodes.some(n => n.id.startsWith('fsk_fl_')));
assert.deepStrictEqual([...new Set(captured.nodes.map(n => n.align))].sort(), [0, 1, 2]);

console.log('OK: file sim sankey grouping + graph shape + tag blocks + axis picker + joint marginals');
