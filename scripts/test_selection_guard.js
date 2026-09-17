// Self-check for selectionBlocksClick (utils.js).
//
// A drag that selected text inside a link should not also follow it. A
// selection left somewhere else on the page should not block anything -- the
// bare getSelection() check this replaces blocked every function link until the
// user clicked somewhere to clear it.
// Run: node scripts/test_selection_guard.js
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../bsimvis/app/static/js/utils.js', 'utf8');
const body = src.slice(src.indexOf('window.selectionBlocksClick = function'));

const win = {};
new Function('window', body)(win);
const blocks = win.selectionBlocksClick;

// --- a DOM just big enough --------------------------------------------------

const node = (name, children = []) => ({
    nodeType: 1,
    name,
    contains(n) { return n === this || children.includes(n); },
});

const link = node('link');
const elsewhere = node('elsewhere');
link.contains = (n) => n === link || n === linkText;
const linkText = { nodeType: 3, parentElement: link };

const sel = (text, anchorNode) => ({
    isCollapsed: !text,
    toString() { return text; },
    anchorNode,
    containsNode(el) { return el === anchorNode || (anchorNode && anchorNode.parentElement === el); },
});

const withSelection = (s, fn) => { win.getSelection = () => s; return fn(); };

// --- cases ------------------------------------------------------------------

assert.strictEqual(
    withSelection(sel('', null), () => blocks({ target: link })), false,
    'no selection never blocks');

assert.strictEqual(
    withSelection(sel('   ', linkText), () => blocks({ target: link })), false,
    'a whitespace-only selection never blocks');

assert.strictEqual(
    withSelection(sel('some name', linkText), () => blocks({ target: link })), true,
    'text selected inside the clicked element blocks the navigation');

assert.strictEqual(
    withSelection(sel('some name', elsewhere), () => blocks({ target: link })), false,
    'a selection elsewhere on the page does not block the navigation');

assert.strictEqual(
    withSelection(sel('some name', elsewhere), () => blocks(null)), false,
    'keyboard or synthetic activation has no event and is never blocked');

assert.strictEqual(
    withSelection(sel('some name', linkText), () => blocks({ target: linkText })), true,
    'a text node target resolves to its element');

console.log('selection guard: OK');
