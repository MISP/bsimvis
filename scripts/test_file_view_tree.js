// Self-check for the single-file view's function tag tree (views/file_view.js).
// It must be the Bin Sim tree with counts instead of masses: grouped by the same
// axis map, nested by the tag ids' own levels, node id == tag id.
// Run: node scripts/test_file_view_tree.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const src = fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', 'views', 'file_view.js'),
    'utf8'
);

// The axis map the browser gets from /api/tags/colors, and the same level split
// tag_taxonomy applies. Parity with the Python rule is test_tag_colors.js's job.
const AXES = {
    fid: 'origin', bsim: 'origin', malware: 'origin', original: 'origin',
    category: 'category', severity: 'severity', yara: 'yara', user: 'user',
    // The synthetic buckets are whole ids, and a namespace lookup on an id with
    // no colon returns the id itself -- so the shipped map answers for them.
    original_code: 'origin', tag_mismatch: 'origin',
};
const TagColor = {
    groupId: (id) => String(id).split('#')[0],
    levels: (id) => ({ segs: TagColor.groupId(id).split(':').filter(Boolean) }),
    prefixes: (id) => {
        const segs = TagColor.groupId(id).split(':').filter(Boolean);
        return segs.slice(0, -1).map((_, i) => segs.slice(0, i + 1).join(':'));
    },
    chain: (id) => TagColor.prefixes(id).concat([TagColor.groupId(id)]),
    axisOf: (id) => AXES[String(id).split(':')[0]] || 'user',
};

// file_view.js is a browser global object literal; lift the two pure methods out
// rather than standing up the whole view.
const slice = (from, to) => src.slice(src.indexOf(from), src.indexOf(to));
const body = slice('    fvAvailableAxes()', '    fvRenderAxisPicker()');
const View = new Function('TagColor', `
    const V = { ${body} };
    return V;
`)(TagColor);

const withTags = (counts) => Object.assign(Object.create(View), {
    fvTagCounts: () => counts,
});

const counts = {
    'fid:uclibc:0.9.30.1#xdrmem_getint32': 3,
    'fid:uclibc:0.9.30.1#memcpy': 2,
    'fid:musl:1.2.4#strcpy': 1,
    'original_code': 7,
    'category:network:c2': 4,
};

// Axes are the ones Bin Sim names, derived from the shipped namespace map --
// not the raw first segment, which is what made every namespace look like an
// axis of its own.
const v = withTags(counts);
assert.deepStrictEqual(v.fvAvailableAxes(), ['category', 'origin']);

// Origin: `fid` and `original_code` are two top nodes, so neither collapses.
v.fvAxis = 'origin';
const origin = v.fvTree();
assert.deepStrictEqual(origin.map(n => n.id).sort(), ['fid', 'original_code']);

const fid = origin.find(n => n.id === 'fid');
assert.strictEqual(fid.count, 6, 'a branch carries the sum beneath it');
assert.deepStrictEqual(fid.children.map(n => n.id), ['fid:uclibc', 'fid:musl']);

const uclibc = fid.children[0];
assert.strictEqual(uclibc.label, 'uclibc');
assert.strictEqual(uclibc.count, 5);
// The version is a level; the symbol it was matched on is not. A tree that made
// `#memcpy` a node would offer a category per function.
assert.deepStrictEqual(uclibc.children.map(n => n.id), ['fid:uclibc:0.9.30.1']);
assert.strictEqual(uclibc.children[0].children.length, 0);
assert.strictEqual(uclibc.children[0].count, 5);

// Every node id is a real tag id and a literal prefix of what it holds, which is
// what makes its colour and its filter the same string.
const walk = (n, out = []) => { out.push(n); n.children.forEach(c => walk(c, out)); return out; };
for (const node of walk(fid)) {
    assert.strictEqual(node.prefix, node.id);
    assert.ok(!node.id.includes('#'), `detail tail leaked into node id ${node.id}`);
}

// A lone top-level namespace is dropped: the picker already says "category".
v.fvAxis = 'category';
assert.deepStrictEqual(v.fvTree().map(n => n.id), ['category:network']);

// Sorted by count, biggest first, at every level.
const sorted = withTags({ 'fid:a:1': 1, 'fid:b:1': 9 });
sorted.fvAxis = 'origin';
assert.deepStrictEqual(sorted.fvTree().map(n => n.id), ['fid:b', 'fid:a']);

console.log('ok');
