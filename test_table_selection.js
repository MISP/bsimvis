/**
 * Self-check for cell-aware table selection.
 *
 * Bin-diff matched rows carry no data-id on the <tr> and hold three entities
 * side by side (function A, function B, the similarity pair), so the selection
 * has to resolve ids from the tag editors inside the selected cells.
 * Run: node test_table_selection.js
 */

const fs = require('fs');
const vm = require('vm');

// --- minimal DOM stub: only what TableSelection touches -----------------------
const allEditors = [];

function cell(entities = [], link = null) {
    const editors = entities.map(([etype, eid]) => {
        const e = { dataset: { etype, eid } };
        allEditors.push(e);
        return e;
    });
    return {
        dataset: {}, children: [], querySelectorAll: () => editors,
        querySelector: () => link, classList: { remove() {}, add() {} },
    };
}

function row(cells, id, opts = {}) {
    const dataset = {};
    if (id) dataset.id = id;
    if (opts.rowkey) dataset.rowkey = opts.rowkey;
    return {
        dataset, children: cells, classList: { remove() {}, add() {} },
        getAttribute: name => (name === 'onclick' ? opts.onclick || null : null),
        querySelector: () => opts.rowLink || null,
    };
}

/** A row that spans the grid: one wide cell, the way a group header renders. */
function wideRow(rowkey, opts = {}) {
    return row([cell()], null, { rowkey, ...opts });
}

function table(rows) {
    const tbody = { children: rows, querySelectorAll: () => [] };
    return { querySelector: () => tbody, closest: () => null, parentElement: null };
}

const sandbox = {
    console,
    CSS: { escape: s => s },
    MutationObserver: class { observe() {} },
    document: {
        addEventListener() {},
        getElementById: id => sandbox._tables[id] || null,
        querySelectorAll: () => [],
        // Only used for the row-id path: does the page render this id as `etype`?
        querySelector: sel => {
            const m = /\[data-etype="([^"]+)"\]\[data-eid="([^"]+)"\]/.exec(sel);
            if (!m) return null;
            return allEditors.find(e => e.dataset.etype === m[1] && e.dataset.eid === m[2]) || null;
        },
    },
    _tables: {},
};
sandbox.window = sandbox;
sandbox.window.addEventListener = () => {};

// A bin-diff matched row: no data-id, [similarity | func A | func B].
sandbox._tables['bindiff'] = table([
    row([
        cell([['similarity', 'a|b|unweighted_cosine']]),
        cell([['function', 'col:func:md5a:0x1']]),
        cell([['function', 'col:func:md5b:0x2']]),
    ]),
    row([
        cell([['similarity', 'c|d|unweighted_cosine']]),
        cell([['function', 'col:func:md5a:0x3']]),
        cell([['function', 'col:func:md5b:0x4']]),
    ]),
]);

vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(__dirname + '/bsimvis/app/static/js/table_selection.js', 'utf8'), sandbox);

const assert = require('assert');
// vm-context arrays have a foreign prototype, so compare by value.
const eq = (got, want) => assert.deepStrictEqual([...got].sort(), want.sort());
const ts = new sandbox.TableSelection('bindiff');

// Both function columns selected across both rows -> all four functions batch.
ts.setSelection(0, 1, 1, 2);
eq(sandbox.getSelectedTableIds('function'), [
    'col:func:md5a:0x1', 'col:func:md5a:0x3', 'col:func:md5b:0x2', 'col:func:md5b:0x4',
]);
eq(sandbox.getSelectedTableIds('similarity'), []);

// Similarity column only -> the pairs, no functions.
ts.setSelection(0, 0, 1, 0);
eq(sandbox.getSelectedTableIds('similarity'), [
    'a|b|unweighted_cosine', 'c|d|unweighted_cosine',
]);
eq(sandbox.getSelectedTableIds('function'), []);

// Whole row -> each kind still resolves to its own ids.
ts.setSelection(0, 0, 0, 2);
eq(sandbox.getSelectedTableIds('function'), ['col:func:md5a:0x1', 'col:func:md5b:0x2']);
eq(sandbox.getSelectedTableIds('similarity'), ['a|b|unweighted_cosine']);
assert.strictEqual(sandbox.getSelectedTableIds().length, 3); // untyped: everything selected

// Rows that do carry a data-id keep working, and stay typed.
allEditors.length = 0;
sandbox._tables['classic'] = table([
    row([cell([['function', 'col:func:md5a:0x9']]), cell()], 'col:func:md5a:0x9'),
]);
sandbox.tableSelections.length = 0;
const ts2 = new sandbox.TableSelection('classic');
ts2.setSelection(0, 1, 0, 1); // a cell with no editor, but the row has an id
eq(sandbox.getSelectedTableIds('function'), ['col:func:md5a:0x9']);
eq(sandbox.getSelectedTableIds('file'), []);

sandbox.tableSelections.forEach(t => t.clearSelection());
eq(sandbox.getSelectedTableIds(), []);

// --- grouped tables: rows that span the grid, and keyboard expand -------------
// The bin-sim table interleaves group headers (one wide cell, expand on the <tr>)
// with function rows, and rebuilds every row whenever a group is opened.
allEditors.length = 0;
const groupClicks = [];
const cellLink = { click: () => groupClicks.push('cell') };
const rowLink = { click: () => groupClicks.push('row') };

const header = wideRow('lib:libc', { onclick: 'toggleFileSimNode()' });
header.click = () => groupClicks.push('header');
const dataRow = row([cell([], cellLink), cell(), cell()], 'main:sim:uc:b::a', { rowLink });
const plainRow = row([cell(), cell(), cell()], 'plain', { rowLink });

sandbox._tables['grouped'] = table([header, dataRow, plainRow]);
sandbox.tableSelections.length = 0;
const ts3 = new sandbox.TableSelection('grouped');

// The widest row sets the grid width; a narrow row resolves past its end to its
// one cell, so the focused column survives arrowing through a group header.
assert.strictEqual(ts3.colCount(), 3);
assert.strictEqual(ts3.cellAt(0, 2), header.children[0]);
assert.strictEqual(ts3.cellAt(1, 2), dataRow.children[2]);

// Enter: a handler in the cell wins, then one anywhere in the row, then the
// row's own -- which is the only thing a group header has.
ts3.activationTarget(1, 0).click();
ts3.activationTarget(1, 1).click();
ts3.activationTarget(0, 0).click();
assert.deepStrictEqual([...groupClicks], ['cell', 'row', 'header']);

// Expanding a group rebuilds every row. The focused header must be found again,
// or keyboard-only traversal stops dead at the group it just opened.
ts3.anchorCell = { r: 0, c: 2 };
ts3.focusCell = { r: 0, c: 2 };
ts3.setSelection(0, 2, 0, 2);
ts3.updateVisuals();
assert.strictEqual(ts3.focusKey, 'lib:libc');

const reopened = wideRow('lib:libc', { onclick: 'toggleFileSimNode()' });
sandbox._tables['grouped'].querySelector().children = [plainRow, reopened, dataRow];
ts3.refreshAfterRender();
assert.strictEqual(ts3.focusCell.r, 1, 'header re-found at its new index');
assert.strictEqual(ts3.focusCell.c, 2, 'and on the same column');

// A row that identifies nothing clears, as it always did.
ts3.focusKey = null;
ts3.refreshAfterRender();
assert.strictEqual(ts3.focusCell, null);

console.log('OK');
