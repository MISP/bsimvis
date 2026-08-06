// Self-check for the File sim tab's composition math (binary_similarity.js).
// Run: node scripts/test_file_sim_tree.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/binary_similarity.js', 'utf8');
const start = src.indexOf('function tagSideCounts');
const end = src.indexOf('// Paths of expanded subtrees');
const { fileSimTree } = new Function(src.slice(start, end) + '; return { fileSimTree };')();

// A: 2 libc, 2 openssl, 2 mirai_xor, 0 other. B: 4 libc, 0 openssl, 2 mirai_xor, 0 other.
const tag = (type, name, a, b) => ({
    type, name, children: [],
    unique_count_a: a, unique_count_b: b, bins: {},
});
const tree = fileSimTree([
    tag('lib', 'libc', 2, 4),
    tag('lib', 'openssl', 2, 0),
    tag('bundle', 'mirai_xor', 2, 2),
    tag('bundle', 'other_malware_bundle', 0, 0),
]);

const pct = n => Math.round(n.sim * 100);
const byName = (n, name) => n.children.find(c => c.name === name);

const lib = byName(tree, 'lib');
const bundle = byName(tree, 'bundle');
assert.strictEqual(pct(byName(lib, 'libc')), 50);
assert.strictEqual(pct(byName(lib, 'openssl')), 0);
assert.strictEqual(pct(lib), 25);
assert.strictEqual(pct(byName(bundle, 'mirai_xor')), 100);
assert.strictEqual(pct(byName(bundle, 'other_malware_bundle')), 0);
assert.strictEqual(pct(bundle), 50);
assert.strictEqual(lib.a, 4);
assert.strictEqual(lib.b, 4);

// Matched functions live in bins, not unique_count_*, so both must be counted.
const withBins = fileSimTree([{
    type: 'lib', name: 'libc', children: [],
    unique_count_a: 1, unique_count_b: 0, bins: { '19': [3, 0, 3, 0] },
}]);
assert.strictEqual(byName(withBins, 'lib').a, 4);
assert.strictEqual(byName(withBins, 'lib').b, 3);

console.log('ok');
