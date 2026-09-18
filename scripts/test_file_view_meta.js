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
const slice = (text, from, to) => text.slice(text.indexOf(from), text.indexOf(to));

// The real escapers, not stubs: the escaping is what is under test.
const esc = new Function(
    `${slice(read('utils.js'), 'function escapeHtml(value) {', '// Filter values:')}
    return { escapeHtml, escapeAttr };`
)();

// Both sinks live inside init(); lift them out rather than standing up the view.
const fmt = new Function('escapeHtml', `${slice(src, 'const fmt = (v) => {', 'const fmtDate =')}
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
    `${slice(src, 'function renderDist(title, icon, dist) {', '// Render Clusters')}
    return renderDist;`
)(d3, esc.escapeHtml, esc.escapeAttr);

const legend = renderDist('AV Type', 'fa-solid fa-shield', [{ value: PAYLOAD, percent: 50 }]);
assert.ok(!legend.includes('<img'), 'a distribution value renders as text');
// Once in the tooltip attribute, once in the label.
assert.strictEqual((legend.match(/&quot;&gt;&lt;img/g) || []).length, 2, 'attribute and text');
assert.strictEqual(renderDist('AV Type', 'fa-solid fa-shield', []), '', 'no data, no panel');
console.log('ok  distribution legend');

console.log('ok');
