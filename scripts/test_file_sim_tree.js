// Self-check for the File sim tree's composition math (binary_similarity.js).
// The tree is the main view's scope selector, so its shape is load-bearing:
// every node is a real tag id, nesting follows the id's own levels, and a
// branch scores as the mean of its children so one absent library still shows.
// Run: node scripts/test_file_sim_tree.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/binary_similarity.js', 'utf8');
const slice = (from, to) => src.slice(src.indexOf(from), src.indexOf(to));
// fileSimTree plus the frontier walk the Sankey uses, so the test can prove the
// graph folds exactly where the tree does.
const body = slice('let fileSimTreeOpen', 'function fileSimTreeRoot')
    + slice('// The chain of tree nodes a tag id belongs to', '// A tag row\'s four masses');

// The browser gets the split rule from /api/tags/colors; here it is the default
// (colon levels, `#` starts the detail tail). `scripts/test_tag_colors.js` is
// what pins this mirror to the Python parser -- this suite only needs the tree.
const TagColor = {
    groupId: (id) => String(id).split('#')[0],
    levels: (id) => ({ segs: TagColor.groupId(id).split(':').filter(Boolean) }),
    prefixes: (id) => {
        const segs = TagColor.groupId(id).split(':').filter(Boolean);
        return segs.slice(0, -1).map((_, i) => segs.slice(0, i + 1).join(':'));
    },
};

const M = new Function('TagColor', body + `
    ; return {
        fileSimTree,
        fileSimFrontier,
        setOpen: (ids) => { fileSimTreeOpen = new Set(ids); },
    };`
)(TagColor);
const fileSimTree = (rows) => M.fileSimTree(rows, 'origin');
const fileSimFrontierNode = (tagId, root) => M.fileSimFrontier(tagId, root).node;
const setOpen = M.setOpen;

// A: 2 libc, 2 openssl, 2 mirai_xor. B: 4 libc, 0 openssl, 2 mirai_xor.
const tag = (type, name, a, b, extra) => Object.assign({
    type, name, tag_id: `${type}:${name}`, children: [],
    unique_count_a: a, unique_count_b: b, bins: {},
}, extra || {});

const tree = fileSimTree([
    tag('lib', 'libc', 2, 4),
    tag('lib', 'openssl', 2, 0),
    tag('bundle', 'mirai_xor', 2, 2),
    tag('bundle', 'other_malware_bundle', 0, 0),
]);

const pct = n => Math.round(n.sim * 100);
const byLabel = (n, label) => n.children.find(c => c.label === label);

// Nodes are named by the tag ids themselves -- `lib`, not a coined "Libraries".
// A node whose id is not a tag id is what let the tree colour a library one way
// and its own card another.
const libs = byLabel(tree, 'lib');
const bundles = byLabel(tree, 'bundle');
assert.strictEqual(libs.id, 'lib');
assert.strictEqual(bundles.id, 'bundle');

// Leaf: min/max of the two side counts.
assert.strictEqual(pct(byLabel(libs, 'libc')), 50);
assert.strictEqual(pct(byLabel(libs, 'openssl')), 0);
// Branch is the mean of its children, so one absent library halves it -- it does
// not read "mostly fine" the way a mass-weighted score would.
assert.strictEqual(pct(libs), 25);
assert.strictEqual(libs.a, 4);
assert.strictEqual(libs.b, 4);

assert.strictEqual(pct(byLabel(bundles, 'mirai_xor')), 100);
// A tag carrying no functions on either side is absence of evidence, not
// evidence of difference: it is dropped rather than scored 0 and averaged in.
assert.strictEqual(byLabel(bundles, 'other_malware_bundle'), undefined);
assert.strictEqual(pct(bundles), 100);

// A lone top-level namespace is dropped: a tree that opens on the one word the
// tab already says wastes its first level. Only one level goes, so a pair of
// nothing but libraries still opens on `lib` rather than on a version number.
const onlyStdlib = fileSimTree([tag('stdlib', 'libstdc++', 1, 1)]);
assert.deepStrictEqual(onlyStdlib.children.map(c => c.id), ['stdlib:libstdc++']);

// An id with no levels at all is its own node rather than vanishing.
const orig = fileSimTree([
    Object.assign(tag('original_code', 'Original Code', 3, 3), { tag_id: 'original_code' }),
]).children[0];
assert.strictEqual(orig.id, 'original_code');
assert.strictEqual(orig.children.length, 0);
assert.strictEqual(orig.prefix, 'original_code');
assert.strictEqual(orig.a, 3);
assert.strictEqual(pct(orig), 100);

// Matched functions live in bins, not unique_count_*, so both must be counted.
const withBins = fileSimTree([{
    type: 'lib', name: 'libc', tag_id: 'lib:libc:2.31', children: [],
    unique_count_a: 1, unique_count_b: 0, bins: { '19': [3, 0, 3, 0] },
}]);
assert.strictEqual(withBins.children[0].a, 4);
assert.strictEqual(withBins.children[0].b, 3);

// A parent row is the merge of its children (bin_sim_tags.summary), so the tree
// takes the leaves and rebuilds the levels above them -- feeding both would
// count the same mass twice.
const rolled = fileSimTree([{
    type: 'lib', name: 'libc', tag_id: 'lib:libc', unique_count_a: 3, unique_count_b: 3, bins: {},
    children: [
        tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31' }),
        tag('lib', 'libc', 1, 1, { tag_id: 'lib:libc:2.35' }),
    ],
}]);
assert.strictEqual(rolled.children[0].a, 3);

// Versions of one library nest under it, and the library's prefix scopes the
// query to all of them at once. The old builder cut this prefix at two segments,
// so selecting one library silently scoped to every library.
const versions = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31', version: '2.31' }),
    tag('lib', 'libc', 1, 0, { tag_id: 'lib:libc:2.35', version: '2.35' }),
]);
const libc = versions.children[0];
assert.strictEqual(libc.prefix, 'lib:libc');
assert.deepStrictEqual(libc.children.map(c => c.label).sort(), ['2.31', '2.35']);
assert.strictEqual(libc.a, 3);

// A detail tail is not a level: the function a library tag matched on must not
// become a node anyone can group or scope by.
const detailed = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31#memcpy' }),
    tag('lib', 'libc', 1, 1, { tag_id: 'lib:libc:2.31#malloc' }),
]);
assert.deepStrictEqual(detailed.children[0].children.map(c => c.id), ['lib:libc:2.31']);
assert.strictEqual(detailed.children[0].children[0].children.length, 0);
assert.strictEqual(detailed.children[0].a, 3);

// Drift rolls up from the versions to the library that drifted.
const drifted = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31', drift: { 'lib:libc:2.35': 8 } }),
]);
assert.deepStrictEqual(drifted.children[0].drift, { 'lib:libc:2.35': 8 });

// The Sankey folds where the tree folds. One expansion state drives the tree,
// the Summary rollup, the table's groups and the graph, so a node that is
// collapsed in one is collapsed in all of them.
const forFrontier = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31', version: '2.31' }),
    tag('lib', 'libc', 1, 1, { tag_id: 'lib:libc:2.35', version: '2.35' }),
    tag('lib', 'zlib', 1, 1, { tag_id: 'lib:zlib:1.2', version: '1.2' }),
]);
const frontier = (tagId) => fileSimFrontierNode(tagId, forFrontier).label;

// Everything folded: the whole namespace draws as one node.
setOpen([]);
assert.strictEqual(frontier('lib:libc:2.31'), 'libc');
assert.strictEqual(frontier('lib:zlib:1.2'), 'zlib');

// Drill into libc only: its versions split, zlib stays folded. This is the case
// that the Sankey's old private depth setting could not express.
setOpen(['lib:libc']);
assert.strictEqual(frontier('lib:libc:2.31'), '2.31');
assert.strictEqual(frontier('lib:libc:2.35'), '2.35');
assert.strictEqual(frontier('lib:zlib:1.2'), 'zlib');

console.log('ok');
