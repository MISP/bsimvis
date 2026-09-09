// Self-check for the File-sim tree's per-tag similarity (binary_similarity.js).
//
// The `Sim` column used to be min(count_a, count_b) / max(...) -- a count
// balance, not a similarity. On a cross-arch pair with zero matched functions
// it still read "Original Code 22%" (A: 224 untagged funcs, B: 50), sitting
// next to a 0% overall score. It now reads the backend's own per-tag `score`,
// which is matched cohesion over matched + unmatched mass.
//
// Load-bearing here:
//   - a leaf takes its score from the row, never from the counts;
//   - several tag ids folding onto one node merge weighted by `score_weight`,
//     the way `TagSplit._row` merges children;
//   - a branch is the unweighted mean of its children, so one absent library
//     still shows instead of being buried by a big sibling.
// Run: node scripts/test_filesim_tree_score.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/binary_similarity.js', 'utf8');
const body = src.slice(src.indexOf('function tagSideCounts('), src.indexOf('// One tree per axis'));

// TagColor stubbed the way it really behaves for these ids: a tag's levels are
// its colon segments, and a detail tail past the version is not a level of its
// own -- `origin:lib:libc:2.31:memcpy` groups under `origin:lib:libc:2.31`.
const DEPTH = 4;
const segs = (id) => String(id).split(':');
const group = (id) => segs(id).slice(0, DEPTH).join(':');
const TagColor = {
    levels: (id) => ({ segs: segs(group(id)) }),
    groupId: group,
    prefixes: (id) => {
        const s = segs(group(id));
        return s.slice(0, -1).map((_, i) => s.slice(0, i + 1).join(':'));
    },
};

const M = new Function('TagColor', body + `
return { fileSimNestedNodes, tagSideCounts };`)(TagColor);

const find = (nodes, id) => {
    for (const n of nodes) {
        if (n.id === id) return n;
        const hit = find(n.children || [], id);
        if (hit) return hit;
    }
    return null;
};
const near = (got, want, what) =>
    assert.ok(Math.abs(got - want) < 1e-9, `${what}: got ${got}, want ${want}`);

// --- the pair that started this --------------------------------------------
// ARM vs SuperH: BSim cannot match across architectures, so every score is 0.
// Verbatim from the stored doc, minus the fields the tree does not read.
const CROSS_ARCH = [
    {
        tag_id: 'original_code', score: 0.0, score_weight: 27252.0,
        unique_count_a: 224.0, unique_count_b: 50.0, bins: {}, drift: {},
    },
    {
        tag_id: 'origin:lib:uclibc:0.9.30.1', score: 0.0, score_weight: 5731.0,
        unique_count_a: 0.0, unique_count_b: 138.0, bins: {}, drift: {},
    },
];

const crossArch = M.fileSimNestedNodes(CROSS_ARCH);
const orig = find(crossArch, 'original_code');
near(orig.sim, 0, 'original_code with no matched function scores 0');
assert.ok(Math.abs(orig.sim - 50 / 224) > 0.2, 'not the old count ratio');
// The counts either side of the score are untouched -- they are what the two
// columns next to it show, and they are still 224 vs 50.
near(orig.a, 224, 'A count');
near(orig.b, 50, 'B count');
near(find(crossArch, 'origin:lib:uclibc:0.9.30.1').sim, 0, 'uclibc scores 0');
// Every score is 0 here, so the ordering has to fall back to mass or the tree
// comes out in whatever order the summary arrived in: 274 functions before 138.
assert.strictEqual(crossArch[0].id, 'original_code', 'tied scores order by mass');

// --- a leaf scores on its match, not on how evenly the counts sit -----------
// Lopsided counts (2 vs 4) that matched perfectly: the old rule said 50%.
const matched = M.fileSimNestedNodes([{
    tag_id: 'origin:lib:libc:2.31', score: 1.0, score_weight: 400.0,
    unique_count_a: 0, unique_count_b: 2, bins: { '19': [2, 200, 2, 200] }, drift: {},
}]);
const libc = find(matched, 'origin:lib:libc:2.31');
near(libc.sim, 1.0, 'a fully matched tag reads 100% despite uneven counts');
near(libc.a, 2, 'matched bins count toward side A');
near(libc.b, 4, 'matched bins plus uniques count toward side B');

// --- several ids folding onto one node merge weighted, not averaged ---------
// A 900-feature tag at 0.1 and a 100-feature one at 0.9 land on the same leaf.
// Weighted: 0.18. An unweighted mean would say 0.5.
const folded = M.fileSimNestedNodes([{
    tag_id: 'origin:lib:libc:2.31', score: 0, score_weight: 0,
    unique_count_a: 0, unique_count_b: 0, bins: {}, drift: {},
    children: [
        {
            tag_id: 'origin:lib:libc:2.31:memcpy', score: 0.9, score_weight: 100.0,
            unique_count_a: 1, unique_count_b: 1, bins: {}, drift: {},
        },
        {
            tag_id: 'origin:lib:libc:2.31:strlen', score: 0.1, score_weight: 900.0,
            unique_count_a: 1, unique_count_b: 1, bins: {}, drift: {},
        },
    ],
}]);
near(find(folded, 'origin:lib:libc:2.31').sim, 0.18, 'folded ids merge by weight');

// --- a branch is the mean of its children ----------------------------------
// One library matched perfectly, one is absent: the group reads 50%, not the
// 99% the perfect one's mass alone would give.
const branch = M.fileSimNestedNodes([
    {
        tag_id: 'origin:lib:libc:2.31', score: 1.0, score_weight: 10000.0,
        unique_count_a: 0, unique_count_b: 0, bins: { '19': [50, 5000, 50, 5000] }, drift: {},
    },
    {
        tag_id: 'origin:lib:zlib:1.2.11', score: 0.0, score_weight: 100.0,
        unique_count_a: 3, unique_count_b: 0, bins: {}, drift: {},
    },
]);
near(find(branch, 'origin:lib').sim, 0.5, 'a group is the mean of its children');

// --- no mass, no NaN -------------------------------------------------------
// A doc written before the split carried `score_weight` has neither field; the
// node must read 0 rather than poisoning every ancestor's mean with NaN.
const legacy = M.fileSimNestedNodes([{
    tag_id: 'original_code',
    unique_count_a: 7, unique_count_b: 7, bins: {}, drift: {},
}]);
near(find(legacy, 'original_code').sim, 0, 'a row with no score reads 0, not NaN');

console.log('ok');
