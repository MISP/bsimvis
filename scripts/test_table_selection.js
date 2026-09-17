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

const M = new Function('window', 'TableSelection', `return ({
${slice('    isVisible() {', '    /**\n     * Which table the keyboard drives')},
${slice('    isKeyboardOwner() {', '    /** True when the event happened inside')},
${slice('    activationTarget(', '    /**\n     * What identifies a row')},
${slice('    handleMouseUp(e) {', '    handleKeyDown(e) {')}
});`);

// The swallow flag is shared across instances rather than per-instance: several
// tables on one page each hear the same mouseup, and a per-instance flag let one
// eat the click another had armed. Stand in for the real static here.
const Statics = { CLICK_SLOP: 3, swallow: false, armSwallow() { Statics.swallow = true; } };

// --- a DOM just big enough for the two methods under test -------------------

let clicked = [];
const el = (opts = {}) => ({
    _y: opts.y || 0,
    _x: opts.x || 0,
    _w: opts.w === undefined ? 100 : opts.w,
    _class: opts.cls || '',
    _onclick: opts.onclick || null,
    tag: opts.tag || 'b',
    getBoundingClientRect() {
        return {
            top: this._y, height: 20, bottom: this._y + 20,
            left: this._x, right: this._x + this._w,
        };
    },
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
    // Honour the :not(.cls) exclusions the real selector carries, so the
    // blacklist of row-level controls is actually exercised here.
    querySelectorAll(sel) {
        const excluded = [...String(sel).matchAll(/:not\(\.([\w-]+)\)/g)].map(m => m[1]);
        return links.filter(l => !excluded.includes(l._class));
    },
});

const inst = (rows) => Object.assign(Object.create(M({ getSelection: () => '' }, Statics)), {
    tbody: { children: rows },
    cellAt(r, c) { return rows[r]._cells[c]; },
});

// --- activationTarget -------------------------------------------------------

const upper = el({ y: 0, tag: 'file-a' });
const lower = el({ y: 40, tag: 'file-b' });
const pairRow = row([cell([upper, lower])]);
const t = inst([pairRow]);

assert.strictEqual(t.activationTarget(0, 0, { clientX: 10, clientY: 45 }), lower,
    'a click beside the lower half of a pair cell activates the lower link');
assert.strictEqual(t.activationTarget(0, 0, { clientX: 10, clientY: 5 }), upper,
    'a click beside the upper half activates the upper link');
assert.strictEqual(t.activationTarget(0, 0, null), upper,
    'keyboard activation has no pointer and takes the first link');

// An empty cell stands in for the row's own action, and for nothing else: the
// row here holds a Delete button, which a click on unrelated whitespace must
// never reach.
const deleteBtn = el({ y: 0, onclick: 'deleteSearch()', tag: 'delete' });
const navRow = row([cell([]), cell([deleteBtn])], 'openRow()');
const t2 = inst([navRow]);
assert.strictEqual(t2.activationTarget(0, 0, { clientX: 10, clientY: 5 }), navRow,
    'an empty cell in a navigating row activates the row');

const plainRow = row([cell([]), cell([deleteBtn])]);
const t3 = inst([plainRow]);
assert.strictEqual(t3.activationTarget(0, 0, { clientX: 10, clientY: 5 }), null,
    'an empty cell in a row with no action of its own activates nothing');

// A Tags cell is a single flex line: bookmark, ignore, then the tag links. Both
// the row-level controls and the pointer's horizontal position matter here --
// before, every control on the line tied on clientY and DOM order won, so dead
// space in a Tags cell toggled the bookmark.
const bookmark = el({ x: 0, w: 20, y: 0, onclick: 'toggleEntityBookmark()', cls: 'bookmark-btn', tag: 'bookmark' });
const ignore = el({ x: 24, w: 20, y: 0, onclick: 'toggleIgnore()', cls: 'ignore-btn', tag: 'ignore' });
const tagLink = el({ x: 60, w: 40, y: 0, onclick: 'openTag()', tag: 'tag' });
const tagsRow = row([cell([bookmark, ignore, tagLink])]);
const t5 = inst([tagsRow]);

assert.strictEqual(t5.activationTarget(0, 0, { clientX: 5, clientY: 10 }), tagLink,
    'dead space in a Tags cell opens the tag, never the bookmark button');
assert.strictEqual(t5.activationTarget(0, 0, null), tagLink,
    'and Enter on a Tags cell does not bookmark the row either');

// With the row-level controls gone, the remaining links still have to be told
// apart by where the pointer was -- on one line that is the X axis alone.
const leftLink = el({ x: 0, w: 40, y: 0, tag: 'left' });
const rightLink = el({ x: 200, w: 40, y: 0, tag: 'right' });
const t6 = inst([row([cell([leftLink, rightLink])])]);

assert.strictEqual(t6.activationTarget(0, 0, { clientX: 210, clientY: 10 }), rightLink,
    'two links on one line are told apart by horizontal position');
assert.strictEqual(t6.activationTarget(0, 0, { clientX: 10, clientY: 10 }), leftLink,
    'and the left one wins on the left');

// --- handleMouseUp: the synthetic click must swallow the native one ---------

const t4 = inst([pairRow]);
Object.assign(t4, {
    isDragging: true, cellModeActive: false, startedOnBlocking: false,
    tempFocus: { r: 0, c: 0 }, startPos: { x: 10, y: 45 },
    clearSelection() {}, setSelection() {}, updateVisuals() {},
});
clicked = [];
Statics.swallow = false;
t4.handleMouseUp({ clientX: 10, clientY: 45, target: el({ tag: 'td' }) });

assert.deepStrictEqual(clicked, ['file-b'],
    'the click lands on the link nearest the pointer');
assert.strictEqual(Statics.swallow, true,
    'the native click that follows is swallowed, so the row does not navigate too');

// A slightly shaky click -- past the drag threshold, but with nothing selected.
// The swallow and the activation used to disagree about where that threshold
// was, so this armed the swallow, activated, and ate its own synthetic click.
const shaky = inst([pairRow]);
Object.assign(shaky, {
    isDragging: true, cellModeActive: false, startedOnBlocking: false,
    tempFocus: { r: 0, c: 0 }, startPos: { x: 10, y: 50 },
    clearSelection() {}, setSelection() {}, updateVisuals() {},
});
clicked = [];
Statics.swallow = false;
shaky.handleMouseUp({ clientX: 14, clientY: 50, target: el({ tag: 'td' }) });

assert.deepStrictEqual(clicked, [],
    'a 4px drag selects rather than activating -- it does not half-do both');
assert.strictEqual(Statics.swallow, true,
    'and the click it produced is swallowed');

// --- only one table may act on a keypress -----------------------------------
//
// The file view mounts four tables, each with its own window keydown listener.
// Before this, one Enter activated a row in every one of them.

// isKeyboardOwner compares instance identity, so the instances have to be the
// same objects the registry holds -- build them once and only move `active`.
const Env = { tableSelections: [] };
const Kb = Object.assign(Object.create(Statics), { active: null });
const kbProto = M(Env, Kb);
const kb = (visible) => Object.assign(Object.create(kbProto), {
    tbody: { isConnected: visible },
    table: { offsetParent: visible ? {} : null },
});

const hidden = kb(false);
const first = kb(true);
const second = kb(true);
Env.tableSelections.push(hidden, first, second);

Kb.active = second;
assert.strictEqual(first.isKeyboardOwner(), false,
    'a table that is not the one last clicked ignores the keypress');
assert.strictEqual(second.isKeyboardOwner(), true,
    'the table last clicked handles the keypress');

Kb.active = null;
assert.strictEqual(first.isKeyboardOwner(), true,
    'with nothing clicked yet, the first visible table still takes arrow keys');
assert.strictEqual(second.isKeyboardOwner(), false,
    'and only that one -- not every visible table');

Kb.active = hidden;
assert.strictEqual(first.isKeyboardOwner(), true,
    'a hidden active table hands the keyboard back to the visible one');

console.log('table_selection activation: OK');
