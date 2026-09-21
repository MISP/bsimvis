// Self-check for the cluster tooltip's metadata block (bin_cluster_views.js).
//
// The block renders values that came off an upload -- a yara rule name, an AV
// label -- straight through innerHTML, so it has to escape. It also has three
// states that look alike if the flags are read wrong: still fetching, fetched
// and empty, and fetched with numbers. `meta_loaded` is set when the fetch
// STARTS, so it cannot be what distinguishes the first two.
// Run: node scripts/test_bin_cluster_tooltip_meta.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const read = (...p) => fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', ...p),
    'utf8'
);

function block(text, from) {
    const start = text.indexOf(from);
    assert.notStrictEqual(start, -1, `cannot find ${from}`);
    let depth = 0;
    for (let i = text.indexOf('{', start); i < text.length; i++) {
        if (text[i] === '{') depth++;
        else if (text[i] === '}' && --depth === 0) return text.slice(start, i + 1);
    }
    throw new Error(`unterminated block: ${from}`);
}

const utils = read('utils.js');
const esc = new Function(
    `${block(utils, 'function escapeHtml(value) {')}
    ${block(utils, 'function escapeAttr(value) {')}
    return { escapeHtml, escapeAttr };`
)();

const src = read('bin_cluster_views.js');
const render = new Function('escapeHtml', 'escapeAttr',
    `${block(src, 'function renderBinClusterMetaBlock(data) {')}
    return renderBinClusterMetaBlock;`
)(esc.escapeHtml, esc.escapeAttr);

// ---- the three states ---------------------------------------------------

const loading = render({ uuid: 'abc123', meta_loaded: true });
assert.ok(loading.includes('loading'), 'a fetch in flight says so');
assert.ok(!loading.includes('not computed'), 'and does not claim there is none');

const empty = render({ uuid: 'abc123', meta_loaded: true, function_count_stats: {} });
assert.ok(empty.includes('not computed'), 'a cluster with no spread says so');
assert.ok(!empty.includes('loading'), 'and does not look like it is still fetching');

const filled = render({
    uuid: 'abc123',
    meta_loaded: true,
    function_count_stats: { min: 12, avg: 340.5, max: 41000, files: 9 },
});
assert.ok(filled.includes('12') && filled.includes('min'), 'the minimum is shown');
assert.ok(filled.includes('max'), 'and the maximum');
// Thousands separators are locale-dependent; the digits are not.
assert.ok(/41[,. ]?000/.test(filled), 'a large maximum stays readable');

// The synthetic root of the hierarchy view is not a cluster.
assert.strictEqual(render({ uuid: 'root' }), '', 'the root node gets no block');
assert.strictEqual(render({}), '', 'nor does a node with no uuid');

// ---- distributions ------------------------------------------------------

const PAYLOAD = '"><img src=x onerror=alert(1)>';
const xss = render({
    uuid: 'abc123',
    function_count_stats: {},
    yara_distribution: [{ value: PAYLOAD, percent: 50 }, { value: 'rule_b', percent: 25 }],
});
assert.ok(!xss.includes('<img'), 'a yara rule name renders as text');
// Once in the title attribute, once in the label.
assert.strictEqual((xss.match(/&quot;&gt;&lt;img/g) || []).length, 2, 'attribute and text');
assert.ok(xss.includes('50%'), 'the top value carries its share');
assert.ok(xss.includes('+1'), 'and says how many values it is hiding');

const single = render({
    uuid: 'abc123',
    function_count_stats: {},
    avtype_distribution: [{ value: 'Emotet', percent: 100 }],
});
assert.ok(!single.includes('+'), 'a lone value has nothing to hide');
assert.ok(!single.includes('Yara'), 'a distribution with no values draws no row');

console.log('cluster tooltip metadata block: OK');
