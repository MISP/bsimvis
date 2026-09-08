// Self-check for the containment trees: the panel tree in lineage.js and the
// file-list forest in table_renderers.js.
//
// What is pinned here: a container nested in a container is drawn open one
// indent deeper (renderTree's `childrenOf` recursion), the walk collecting
// those levels terminates on a cycle, and a page of search hits groups into
// trees where every match appears exactly once, under its container.
// Run: node scripts/test_lineage_tree.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/lineage.js', 'utf8');

const stubs = `
    let fetch = null;
    const escapeHtml = v => String(v ?? '');
    const escapeAttr = v => String(v ?? '');
    const jsString = v => JSON.stringify(String(v ?? ''));
    const middleTruncate = v => String(v ?? '');
    const EntityRenderer = { renderTag: (t, id, tags) => '[tags:' + (tags || []).join('+') + ']' };
    const window = { EntityRenderer };
`;
const { Lineage, setFetch } = new Function(
    stubs + src + `
    ; return { Lineage: window.Lineage, setFetch: f => { fetch = f; } };`
)();

const node = (md5, name, path, childCount = 0, extra = {}) => Object.assign({
    file_md5: md5, file_name: name, path_in_parent: path,
    exists: true, child_count: childCount,
}, extra);

// --- renderTree: nesting -----------------------------------------------
const item = (n, hidePath, depth) => Lineage._panelRow(n, 'coll', hidePath, depth);
const depthsOf = html => [...html.matchAll(/data-depth="(-?\d+)"[^>]*>/g)].map(m => Number(m[1]));

const inner = node('aaa', 'inner.zip', 'inner.zip', 1);
const deep = node('bbb', 'deep.so', 'deep.so');
const flat = Lineage.renderTree([inner], 'coll', item);
assert.deepStrictEqual(depthsOf(flat), [0], 'a lone child sits at depth 0');

const nested = Lineage.renderTree([inner], 'coll', item, n => ({ aaa: [deep] })[n.file_md5]);
assert.deepStrictEqual(depthsOf(nested), [0, 1], 'a nested container opens one level deeper');
assert.ok(nested.indexOf('deep.so') > nested.indexOf('inner.zip'), 'contents follow their container');

// Directories still group, and files nested under one keep going deeper.
const inDir = node('ccc', 'libfoo.so', 'lib/arm64-v8a/libfoo.so', 1);
const dirHtml = Lineage.renderTree([inDir], 'coll', item, n => ({ ccc: [deep] })[n.file_md5]);
assert.deepStrictEqual(depthsOf(dirHtml), [0, 1, 2, 3], 'two dir rows, the file, then its contents');
assert.ok(dirHtml.includes('arm64-v8a'), 'directory rows are kept');

// --- _panelRow: tags ---------------------------------------------------
const tagged = Lineage._panelRow(node('ddd', 'x.so', 'x.so', 0, { tags: ['lib:libc'], user_tags: ['mine'] }), 'coll');
assert.ok(tagged.includes('[tags:lib:libc]'), 'a panel row shows its tags');
const dangling = Lineage._panelRow({ file_md5: 'eee', file_name: 'gone', exists: false, tags: ['x'] }, 'coll');
assert.ok(!dangling.includes('[tags:'), 'a node with no document has no tags to show');

// --- fetchSubtrees -----------------------------------------------------
let requests = 0;
const serve = graph => setFetch(async url => {
    requests++;
    const md5 = decodeURIComponent(url.split('/api/file/')[1].split('/lineage')[0]);
    return { ok: true, json: async () => ({ children: graph[md5] || [] }) };
});

(async () => {
    serve({ aaa: [node('bbb', 'mid.zip', 'mid.zip', 1)], bbb: [node('ccc', 'leaf.so', 'leaf.so')] });
    const map = await Lineage.fetchSubtrees('coll', [inner]);
    assert.deepStrictEqual(Object.keys(map).sort(), ['aaa', 'bbb'], 'walks past the first nested level');
    assert.strictEqual(map.bbb[0].file_md5, 'ccc');
    assert.strictEqual(requests, 2, 'one request per expandable node, no leaf lookups');

    // A hand-declared cycle must not spin.
    requests = 0;
    serve({ aaa: [node('bbb', 'b', 'b', 1)], bbb: [node('aaa', 'a', 'a', 1)] });
    const cyc = await Lineage.fetchSubtrees('coll', [inner]);
    assert.deepStrictEqual(Object.keys(cyc).sort(), ['aaa', 'bbb'], 'a cycle is visited once');

    // Nothing expandable, nothing fetched.
    requests = 0;
    assert.deepStrictEqual(await Lineage.fetchSubtrees('coll', [node('zzz', 'leaf', 'leaf')]), {});
    assert.strictEqual(requests, 0);

    console.log('lineage tree self-check OK');
})();

// --- the file list forest ----------------------------------------------
// Grouping only. _fileRows is stubbed to "md5@depth<parent" so the assertions
// read as the row order and nesting the page ends up with.
const trFile = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/table_renderers.js', 'utf8');
// Everything up to the window.* aliases at the end; those assume the object
// is a browser global, which it is not inside a function scope.
const trSrc = trFile.slice(0, trFile.indexOf('window.renderCollections'));
const TableRenderers = new Function(`
    // window.TableRenderers = {...} makes a bare global in a browser; the
    // renderers call each other through it, so mirror that here.
    let TableRenderers;
    const window = {
        set TableRenderers(v) { TableRenderers = v; },
        get TableRenderers() { return TableRenderers; },
    };
    const getRoutingState = () => ({ collection: 'coll', pool: null });
    const escapeHtml = v => String(v ?? '');
    const escapeAttr = v => String(v ?? '');
    const jsString = v => JSON.stringify(String(v ?? ''));
    const middleTruncate = v => String(v ?? '');
    const formatDate = () => '';
    const getRowTagColor = () => '';
    const EntityRenderer = { renderMd5: m => m, renderTag: () => '', renderFileNoteButton: () => '', renderClusterCard: () => '' };
    const Lineage = { toggleButton: () => '', pathLabel: () => '', nodeName: n => n.file_name };
    ${trSrc}
    ; return window.TableRenderers;`
)();

TableRenderers._fileRows = (rows, map, depth, parentMd5) =>
    rows.map(f => `${f.file_md5}@${depth}<${parentMd5 || ''};`).join('');
TableRenderers._contextRow = group => `ctx:${group.file_md5}(${group.rows.length});`;

const row = (md5, parent) => ({ file_md5: md5, file_name: md5, parent_md5: parent, parent_file_name: parent });
const forest = data => TableRenderers._fileForest(data, {}).split(';').filter(Boolean);

// The container matched too: its children hang off it, and only off it.
assert.deepStrictEqual(
    forest([row('apk'), row('dex', 'apk'), row('so', 'apk')]),
    ['apk@0<', 'dex@1<apk', 'so@1<apk'],
    'matches nest under an on-page container'
);

// The container did not match: one context row stands in, shared by both hits.
assert.deepStrictEqual(
    forest([row('dex', 'apk'), row('so', 'apk')]),
    ['ctx:apk(2)', 'dex@1<apk', 'so@1<apk'],
    'a container that is off the page still gets drawn, once'
);

// Two levels: apk > inner.zip > leaf.so, all three on the page.
assert.deepStrictEqual(
    forest([row('apk'), row('inner', 'apk'), row('leaf', 'inner')]),
    ['apk@0<', 'inner@1<apk', 'leaf@2<inner'],
    'nesting follows the whole chain'
);

// Server order decides where a group lands, and unrelated roots keep theirs.
assert.deepStrictEqual(
    forest([row('lone'), row('so', 'apk'), row('other')]),
    ['lone@0<', 'ctx:apk(1)', 'so@1<apk', 'other@0<'],
    'a group is drawn where its first match sorted'
);

// Every row appears exactly once, whatever the shape.
const messy = [row('apk'), row('dex', 'apk'), row('x', 'gone'), row('y', 'gone'), row('root')];
const drawn = forest(messy).filter(s => !s.startsWith('ctx:')).map(s => s.split('@')[0]);
assert.deepStrictEqual(drawn.slice().sort(), ['apk', 'dex', 'root', 'x', 'y'], 'nothing is dropped');
assert.strictEqual(new Set(drawn).size, drawn.length, 'nothing is drawn twice');

// A parent_md5 cycle must still render both rows, once each.
const cycle = forest([{ ...row('a', 'b') }, { ...row('b', 'a') }]);
assert.deepStrictEqual(cycle.map(s => s.split('@')[0]).sort(), ['a', 'b'], 'a cycle still renders');

// A row that claims itself as its own parent is a root, not a loop.
assert.deepStrictEqual(forest([row('self', 'self')]), ['self@0<self'], 'a self-parent is ignored');

console.log('file forest self-check OK');
