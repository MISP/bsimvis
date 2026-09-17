// Self-check for EntityRenderer.clickedCell (entity_renderer.js).
//
// The context menu is built from the row object, so every "Copy X" item is the
// same wherever in the row you right-click -- right-clicking a function row's
// File Name cell offered "Copy Name" and handed back the *function's* name.
// clickedCell resolves the column actually under the pointer so the menu can
// offer that cell's own value first.
// Run: node scripts/test_context_cell.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/entity_renderer.js', 'utf8');
const body = src.slice(
    src.indexOf('    clickedCell: function(e) {'),
    src.indexOf('    handleContextMenu: function(e, type, el) {')
).replace(/,\s*$/, '');

const clickedCell = new Function(`return ({${body}}).clickedCell;`)();

// --- a DOM just big enough --------------------------------------------------

const th = (label, text) => ({ dataset: label ? { label } : {}, textContent: text });

const makeTable = (headers) => {
    const table = {
        querySelector(sel) {
            return sel === 'thead tr' ? { children: headers } : null;
        },
    };
    return table;
};

const td = (table, cellIndex, text) => {
    const cell = {
        nodeType: 1,
        cellIndex,
        innerText: text,
        closest(sel) { return sel === 'td' ? cell : (sel === 'table' ? table : null); },
    };
    return cell;
};

const table = makeTable([
    th('Function', 'Function ↕'),
    th('File Name', 'File Name ↕'),
    th(null, 'Evidence ↕'),
]);

// --- cases ------------------------------------------------------------------

const fileNameCell = td(table, 1, '  busybox  ');
assert.deepStrictEqual(
    clickedCell({ target: fileNameCell }),
    { label: 'File Name', value: 'busybox' },
    'the column under the pointer names itself and yields its own text');

const evidenceCell = td(table, 2, 'matched 3 rules');
assert.deepStrictEqual(
    clickedCell({ target: evidenceCell }),
    { label: 'Evidence', value: 'matched 3 rules' },
    'a header with no data-label falls back to its text, sort arrow stripped');

// Right-clicking the text inside a cell, not the cell itself.
const textNode = { nodeType: 3, parentElement: fileNameCell };
assert.deepStrictEqual(
    clickedCell({ target: textNode }),
    { label: 'File Name', value: 'busybox' },
    'a text node target resolves to its cell');

assert.strictEqual(
    clickedCell({ target: td(table, 1, '   ') }), null,
    'an empty cell offers nothing to copy');

assert.strictEqual(
    clickedCell({ target: { nodeType: 1, closest: () => null } }), null,
    'a right-click outside any cell offers nothing');

assert.strictEqual(clickedCell(null), null, 'no event, no cell');

// A cell past the end of the header row (a colspan row) still copies its text.
assert.deepStrictEqual(
    clickedCell({ target: td(table, 9, 'group header') }),
    { label: 'Cell', value: 'group header' },
    'a cell with no matching header still offers its value');

console.log('context menu clicked cell: OK');
