// Self-check for whole-cell activation (table_selection.js).
//
// A click on a cell's dead space is turned into a click on that cell's link, so
// the whole cell is a hit target. Two things about that are load-bearing:
//   - a pair row stacks both sides in one cell, so the link picked has to be the
//     one the pointer was next to, not whichever came first in the DOM;
//   - the native click still arrives after the synthetic one, so it has to be
//     swallowed or the row's own onclick fires a second navigation.
// Run: node scripts/test_table_selection.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/table_selection.js', 'utf8');
const slice = (from, to) => src.slice(src.indexOf(from), src.indexOf(to));

const M = new Function('window', `return ({
${slice('    activationTarget(', '    /**\n     * What identifies a row')},
${slice('    handleMouseUp(e) {', '    handleKeyDown(e) {')}
});`);

// --- a DOM just big enough for the two methods under test -------------------

let clicked = [];
const el = (opts = {}) => ({
    _y: opts.y || 0,
    _onclick: opts.onclick || null,
    tag: opts.tag || 'b',
    getBoundingClientRect() { return { top: this._y, height: 20 }; },
    getAttribute(n) { return n === 'onclick' ? this._onclick : null; },
    contains() { return false; },
    click() { clicked.push(this.tag); },
});

// querySelector as well as querySelectorAll, so the pre-fix code this guards
// against runs far enough to fail on an assertion rather than a TypeError.
const row = (cells, onclick = null) => ({
    tag: 'tr',
    _onclick: onclick,
    _cells: cells,
    getAttribute(n) { return n === 'onclick' ? this._onclick : null; },
    querySelector() { return cells.flatMap(c => c._links)[0] || null; },
    contains() { return false; },
    click() { clicked.push('tr'); },
});

const cell = (links) => ({
    _links: links,
    querySelector() { return links[0] || null; },
    querySelectorAll() { return links; },
});

const inst = (rows) => Object.assign(Object.create(M({ getSelection: () => '' })), {
    tbody: { children: rows },
    cellAt(r, c) { return rows[r]._cells[c]; },
});

// --- activationTarget -------------------------------------------------------

const upper = el({ y: 0, tag: 'file-a' });
const lower = el({ y: 40, tag: 'file-b' });
const pairRow = row([cell([upper, lower])]);
const t = inst([pairRow]);

assert.strictEqual(t.activationTarget(0, 0, { clientY: 45 }), lower,
    'a click beside the lower half of a pair cell activates the lower link');
assert.strictEqual(t.activationTarget(0, 0, { clientY: 5 }), upper,
    'a click beside the upper half activates the upper link');
assert.strictEqual(t.activationTarget(0, 0, null), upper,
    'keyboard activation has no pointer and takes the first link');

// An empty cell stands in for the row's own action, and for nothing else: the
// row here holds a Delete button, which a click on unrelated whitespace must
// never reach.
const deleteBtn = el({ y: 0, onclick: 'deleteSearch()', tag: 'delete' });
const navRow = row([cell([]), cell([deleteBtn])], 'openRow()');
const t2 = inst([navRow]);
assert.strictEqual(t2.activationTarget(0, 0, { clientY: 5 }), navRow,
    'an empty cell in a navigating row activates the row');

const plainRow = row([cell([]), cell([deleteBtn])]);
const t3 = inst([plainRow]);
assert.strictEqual(t3.activationTarget(0, 0, { clientY: 5 }), null,
    'an empty cell in a row with no action of its own activates nothing');

// --- handleMouseUp: the synthetic click must swallow the native one ---------

const t4 = inst([pairRow]);
Object.assign(t4, {
    isDragging: true, cellModeActive: false, startedOnBlocking: false,
    tempFocus: { r: 0, c: 0 }, startPos: { x: 10, y: 50 }, wasSelecting: false,
    clearSelection() {}, setSelection() {}, updateVisuals() {},
});
clicked = [];
t4.handleMouseUp({ clientX: 10, clientY: 45, target: el({ tag: 'td' }) });

assert.deepStrictEqual(clicked, ['file-b'],
    'the click lands on the link nearest the pointer');
assert.strictEqual(t4.wasSelecting, true,
    'the native click that follows is swallowed, so the row does not navigate too');

console.log('table_selection activation: OK');
