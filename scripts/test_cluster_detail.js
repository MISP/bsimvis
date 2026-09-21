// Self-check for the cluster detail route and its lazily loaded tree.
//
// Two pieces of logic worth pinning:
//   - /functions/clusters/<uuid> and /files/clusters/<uuid> have to resolve to
//     the detail views. A third path segment used to fall through to the md5
//     branch, so a cluster uuid was read as a file hash.
//   - the view no longer pulls the whole cluster list (`limit=20000`, every
//     cluster in the collection) to draw a sidebar. It ingests one level at a
//     time, so the map/child bookkeeping and the "is this a leaf?" test are
//     what keep the tree honest.
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

console.log('ok  routes');

// --- ingest / hasChildren ----------------------------------------------------

// Lift the two pure methods out of the view rather than standing the view up:
// everything around them touches the DOM.
const viewSrc = fs.readFileSync(root + '/views/cluster_detail_view.js', 'utf8');

function method(name) {
    const start = viewSrc.indexOf(`    ${name}(`);
    assert.notStrictEqual(start, -1, `cannot find ${name}`);
    let depth = 0;
    for (let i = viewSrc.indexOf('{', start); i < viewSrc.length; i++) {
        if (viewSrc[i] === '{') depth++;
        else if (viewSrc[i] === '}' && --depth === 0) return viewSrc.slice(start, i + 1);
    }
    throw new Error(`unterminated method: ${name}`);
}

const view = new Function(`return ({
${method('ingest')},
${method('hasChildren')}
});`)();

const fresh = () => Object.assign(Object.create(view), {
    clusterMapById: {},
    clusterMapByUuid: {},
    childrenMap: {},
    childrenLoaded: new Set(),
    rootNodes: [],
});

const c = (id, parent, extra) => Object.assign(
    { cluster_id: id, cluster_uuid: `u${id}`, parent: parent === undefined ? null : parent, count: 1 },
    extra || {}
);

// The opening fetch: the cluster plus its ancestor chain, nothing sideways.
let v = fresh();
v.ingest([c(1, null), c(2, 1), c(3, 2, { has_children: true })]);
assert.deepStrictEqual(v.rootNodes, ['1'], 'only the parentless node is a root');
assert.deepStrictEqual(v.childrenMap['1'], ['2'], 'the chain is linked downwards');
assert.deepStrictEqual(v.childrenMap['2'], ['3'], 'at every level');
assert.strictEqual(v.clusterMapByUuid['u3'].cluster_id, 3, 'uuids resolve to the node');

// has_children is what stops an unexpanded node from rendering as a leaf --
// its children have not been fetched, so childrenMap says nothing about it.
assert.ok(v.hasChildren(v.clusterMapById['3'], '3'), 'an unexpanded node keeps its caret');
assert.ok(!v.hasChildren(c(9, null), '9'), 'a node the server calls childless is a leaf');

// Expanding it: the children arrive in their own call, keyed by the parent.
v.ingest([c(4, 3), c(5, 3)]);
v.childrenLoaded.add('3');
assert.deepStrictEqual(v.childrenMap['3'], ['4', '5'], 'children land under their parent');
assert.deepStrictEqual(v.rootNodes, ['1'], 'a fetched child never becomes a second root');

// An empty expand is final: without childrenLoaded the stale has_children flag
// would spin the "loading…" row forever.
let leaf = fresh();
leaf.ingest([c(7, null, { has_children: true })]);
leaf.childrenLoaded.add('7');
assert.ok(!leaf.hasChildren(leaf.clusterMapById['7'], '7'),
    'a node that was expanded and had nothing is a leaf');

// Re-ingesting the same cluster (selecting it, then loading its stats) merges
// rather than duplicating.
v.ingest([c(4, 3, { function_count_stats: { min: 1, avg: 2, max: 3, files: 2 } })]);
assert.deepStrictEqual(v.childrenMap['3'], ['4', '5'], 'no duplicate child entry');
assert.strictEqual(v.clusterMapById['4'].function_count_stats.max, 3,
    'later fields are merged onto the known cluster');
assert.strictEqual(v.clusterMapByUuid['u4'].function_count_stats.max, 3,
    'and the uuid map sees the same object');

console.log('ok  lazy tree');
console.log('cluster detail route + tree: OK');
