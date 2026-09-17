// Self-check for the cluster detail route and its local tree.
//
// Two pieces of logic worth pinning:
//   - /functions/clusters/<uuid> and /files/clusters/<uuid> have to resolve to
//     the detail views. A third path segment used to fall through to the md5
//     branch, so a cluster uuid was read as a file hash.
//   - list_clusters returns the whole ancestor chain and the whole descendant
//     subtree (there is no depth parameter), so the "local" tree is pruned here.
// Run: node scripts/test_cluster_detail.js
const fs = require('fs');
const assert = require('assert');

const root = __dirname + '/../bsimvis/app/static/js';

// --- parseRestfulPath --------------------------------------------------------

const utils = fs.readFileSync(root + '/utils.js', 'utf8');
const parseSrc = utils.slice(
    utils.indexOf('function parseRestfulPath'),
    utils.indexOf('window.parseRestfulPath')
);
const makeParse = new Function('window', parseSrc + '; return parseRestfulPath;');

const parseAt = (pathname) => makeParse({
    location: { pathname, search: '', hash: '' },
})();

assert.strictEqual(parseAt('/collections/main/functions/clusters').view, 'clusters',
    'the function cluster list still resolves');
assert.strictEqual(parseAt('/collections/main/files/clusters').view, 'bin-clusters',
    'the binary cluster list still resolves');

const fnDetail = parseAt('/collections/main/functions/clusters/abc-123');
assert.strictEqual(fnDetail.view, 'cluster-detail',
    'a uuid after functions/clusters opens the function cluster view');
assert.strictEqual(fnDetail.cluster_uuid, 'abc-123', 'and carries the uuid');

const binDetail = parseAt('/collections/main/files/clusters/def-456');
assert.strictEqual(binDetail.view, 'bin-cluster-detail',
    'a uuid after files/clusters opens the binary cluster view');
assert.strictEqual(binDetail.cluster_uuid, 'def-456', 'and carries the uuid');

// The regression this replaces: the uuid used to be parsed as an md5.
assert.ok(!fnDetail.md5, 'a cluster uuid is not mistaken for a file hash');
assert.ok(!binDetail.md5, 'nor on the binary side');

// --- localTree ---------------------------------------------------------------

const viewSrc = fs.readFileSync(root + '/views/cluster_detail_view.js', 'utf8');
const treeSrc = viewSrc.slice(
    viewSrc.indexOf('    localTree(self, all) {'),
    viewSrc.indexOf('    render(self, all) {')
).replace(/,\s*$/, '');
const localTree = new Function(`return ({${treeSrc}}).localTree;`)();

const c = (id, parent) => ({ cluster_id: id, parent: parent === undefined ? null : parent, count: 1 });

const root_ = c(1, null);
const grand = c(2, 1);
const parent = c(3, 2);
const self = c(4, 3);
const childA = c(5, 4);
const childB = c(6, 4);
const grandchild = c(7, 5);
const cousin = c(8, 3);

const all = [root_, grand, parent, self, childA, childB, grandchild, cousin];
const tree = localTree.call({}, self, all);

assert.strictEqual(tree.parent, parent, 'the immediate parent is found');
assert.strictEqual(tree.grandparent, grand, 'and one level above it');
assert.deepStrictEqual(tree.children, [childA, childB],
    'only direct children, not the whole subtree');
assert.ok(!tree.children.includes(grandchild), 'a grandchild is pruned away');
assert.ok(!tree.children.includes(cousin), 'a sibling is not mistaken for a child');

// A root cluster has no parent and must not crash on the lookup.
const lonely = c(99, null);
const lonelyTree = localTree.call({}, lonely, [lonely]);
assert.strictEqual(lonelyTree.parent, null, 'a root cluster has no parent');
assert.strictEqual(lonelyTree.grandparent, null, 'nor a grandparent');
assert.deepStrictEqual(lonelyTree.children, [], 'nor children');

console.log('cluster detail route + tree: OK');
