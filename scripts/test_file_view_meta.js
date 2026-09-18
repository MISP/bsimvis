// Self-check for the file detail view's two metadata sinks (views/file_view.js):
// the metadata table and the cluster distribution legend. Both go through
// innerHTML with values that came off an upload -- `file_name` is a query
// parameter -- so both must escape.
// Run: node scripts/test_file_view_meta.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const read = (...p) => fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', ...p),
    'utf8'
);
const src = read('views', 'file_view.js');
const utils = read('utils.js');

// Lifts one declaration out of a source file, from its opening brace to the
// matching close. This used to slice up to whatever came next in the file,
// which tied the test to an unrelated comment: `renderDist` is followed by
// `// Render Clusters` on main and by something else on dev, so the slice ran
// off the end and the file no longer parsed. Brace matching has nothing to do
// with the neighbours.
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

// The real escapers, not stubs: the escaping is what is under test.
const esc = new Function(
    `${block(utils, 'function escapeHtml(value) {')}
    ${block(utils, 'function escapeAttr(value) {')}
    return { escapeHtml, escapeAttr };`
)();

// Both sinks live inside init(); lift them out rather than standing up the view.
const fmt = new Function('escapeHtml', `${block(src, 'const fmt = (v) => {')}
    return fmt;`)(esc.escapeHtml);

const PAYLOAD = '"><img src=x onerror=alert(1)>';

// ---- metadata table -----------------------------------------------------
assert.ok(!fmt(PAYLOAD).includes('<img'), 'a stored file name renders as text');
assert.ok(fmt(PAYLOAD).includes('&lt;img'), 'and is still readable');
const names = fmt([PAYLOAD, 'b.elf']);
assert.ok(!names.includes('<img'), 'other names are escaped too');
assert.ok(names.includes(', b.elf'), 'the list still joins');
assert.strictEqual(fmt('a.elf'), 'a.elf', 'an ordinary value is untouched');
// The placeholder is markup this view owns, so it stays markup.
for (const empty of ['', null, undefined, []]) {
    assert.ok(fmt(empty).includes('<span'), 'empty values keep the placeholder');
}
console.log('ok  metadata table');

// ---- cluster distribution legend ----------------------------------------
const sel = {};
['attr', 'style', 'append', 'selectAll', 'data', 'join', 'text'].forEach(k => { sel[k] = () => sel; });
sel.node = () => ({ outerHTML: '<svg></svg>' });
const d3 = {
    create: () => sel,
    pie: () => {
        const p = (rows) => rows.map(d => ({ data: d, value: d.value }));
        p.value = () => p;
        p.sort = () => p;
        return p;
    },
    arc: () => {
        const a = () => '';
        a.innerRadius = () => a;
        a.outerRadius = () => a;
        return a;
    },
};

const renderDist = new Function('d3', 'escapeHtml', 'escapeAttr',
    `${block(src, 'function renderDist(title, icon, dist) {')}
    return renderDist;`
)(d3, esc.escapeHtml, esc.escapeAttr);

const legend = renderDist('AV Type', 'fa-solid fa-shield', [{ value: PAYLOAD, percent: 50 }]);
assert.ok(!legend.includes('<img'), 'a distribution value renders as text');
// Once in the tooltip attribute, once in the label.
assert.strictEqual((legend.match(/&quot;&gt;&lt;img/g) || []).length, 2, 'attribute and text');
assert.strictEqual(renderDist('AV Type', 'fa-solid fa-shield', []), '', 'no data, no panel');
console.log('ok  distribution legend');

console.log('ok');
