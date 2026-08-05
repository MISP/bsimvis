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

function cell(entities = []) {
    const editors = entities.map(([etype, eid]) => {
        const e = { dataset: { etype, eid } };
        allEditors.push(e);
        return e;
    });
    return { dataset: {}, children: [], querySelectorAll: () => editors, classList: { remove() {}, add() {} } };
}

function row(cells, id) {
    const tr = { dataset: id ? { id } : {}, children: cells, classList: { remove() {}, add() {} } };
    return tr;
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

console.log('OK');
