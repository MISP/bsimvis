// Self-check for the File sim tree's composition math (binary_similarity.js).
// The tree is the main view's scope selector, so its shape is load-bearing:
// group -> library -> version, with the group scoring as the mean of its
// children so one absent library still shows.
// Run: node scripts/test_file_sim_tree.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/binary_similarity.js', 'utf8');
const start = src.indexOf('const FILESIM_GROUPS');
const end = src.indexOf('function fileSimTreeRoot');
const { fileSimTree } = new Function(src.slice(start, end) + '; return { fileSimTree };')();

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

const libs = byLabel(tree, 'Libraries');
const bundles = byLabel(tree, 'Bundles');

// Leaf: min/max of the two side counts.
assert.strictEqual(pct(byLabel(libs, 'libc')), 50);
assert.strictEqual(pct(byLabel(libs, 'openssl')), 0);
// Group is the mean of its children, so one absent library halves it -- it does
// not read "mostly fine" the way a mass-weighted score would.
assert.strictEqual(pct(libs), 25);
assert.strictEqual(libs.a, 4);
assert.strictEqual(libs.b, 4);

assert.strictEqual(pct(byLabel(bundles, 'mirai_xor')), 100);
// A tag carrying no functions on either side is absence of evidence, not
// evidence of difference: it is dropped rather than scored 0 and averaged in.
assert.strictEqual(byLabel(bundles, 'other_malware_bundle'), undefined);
assert.strictEqual(pct(bundles), 100);

// Tags land in the group named by their first namespace segment; anything with
// no convention falls through to Other rather than vanishing.
assert.strictEqual(byLabel(fileSimTree([tag('stdlib', 'libstdc++', 1, 1)]), 'Libraries').children.length, 1);
assert.strictEqual(byLabel(fileSimTree([tag('user', 'hand_tagged', 1, 1)]), 'Other').children.length, 1);
const orig = byLabel(fileSimTree([
    Object.assign(tag('original_code', 'Original Code', 3, 3), { tag_id: 'original_code' }),
]), 'Original');
assert.strictEqual(orig.children[0].prefix, 'original_code');

// Matched functions live in bins, not unique_count_*, so both must be counted.
const withBins = fileSimTree([{
    type: 'lib', name: 'libc', tag_id: 'lib:libc:2.31', children: [],
    unique_count_a: 1, unique_count_b: 0, bins: { '19': [3, 0, 3, 0] },
}]);
assert.strictEqual(byLabel(withBins, 'Libraries').a, 4);
assert.strictEqual(byLabel(withBins, 'Libraries').b, 3);

// Versions of one library nest under it, and the library's prefix scopes the
// query to all of them at once.
const versions = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31', version: '2.31' }),
    tag('lib', 'libc', 1, 0, { tag_id: 'lib:libc:2.35', version: '2.35' }),
]);
const libc = byLabel(versions, 'Libraries').children[0];
assert.strictEqual(libc.prefix, 'lib:libc');
assert.deepStrictEqual(libc.children.map(c => c.label).sort(), ['2.31', '2.35']);
assert.strictEqual(libc.a, 3);

// Drift rolls up from the versions to the library that drifted.
const drifted = fileSimTree([
    tag('lib', 'libc', 2, 2, { tag_id: 'lib:libc:2.31', drift: { 'lib:libc:2.35': 8 } }),
]);
assert.deepStrictEqual(byLabel(drifted, 'Libraries').children[0].drift, { 'lib:libc:2.35': 8 });

console.log('ok');
