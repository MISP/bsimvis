// Self-check for the containment tree in lineage.js.
//
// The two things worth pinning: a container nested in a container is drawn
// open, one indent level deeper than its parent (renderTree's `childrenOf`
// recursion), and the walk that collects those nested levels terminates on a
// cycle instead of spinning.
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
