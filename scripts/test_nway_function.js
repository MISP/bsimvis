// Run: node scripts/test_nway_function.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const vm = require('vm');

const jsDir = path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js');
const context = vm.createContext({ window: {} });
const utils = fs.readFileSync(path.join(jsDir, 'utils.js'), 'utf8');
vm.runInContext(utils.match(/function escapeHtml[\s\S]*?\n}/)[0] + '\n' + utils.match(/function escapeAttr[\s\S]*?\n}/)[0], context);
vm.runInContext(fs.readFileSync(path.join(jsDir, 'nway_function.js'), 'utf8'), context);
const view = new context.window.NwayFunction({ addEventListener() {} }, {
    fids: [], state: { fn: 4, cv: 'full' },
});
const block = (id, st) => ({
    id, title: 'branch', ref_text: 'original();', ref_syntax: [['original', 'func_call'], ['();', 'syntax']],
    cells: {
        1: { st: 'same', sim: 1, segs: [0] },
        2: { st, sim: 0.8, segs: st === 'miss' ? [] : [0] },
        3: { st: 'miss', sim: 0, segs: [] },
        4: { st, sim: 0.8, segs: st === 'miss' ? [] : [0] },
    },
    groups: [
        { funcs: [2, 4], st, sims: { 2: 0.8, 4: 0.8 }, ref_segs: [['original();', true]] },
        { funcs: [3], st: 'miss', sims: { 3: 0 } },
    ],
});
view.data = {
    functions: ['A', 'B', 'C', 'D', 'E'].map(letter => ({ letter, file_name: `sample-${letter}.exe` })),
    consensus: {
        reference: 0,
        // the backend's whole-function keys (build noise already removed); 3 has no code
        keys: { 0: 'original ( ) ;', 1: 'original ( ) ;', 2: 'changed ( ) ;', 4: 'changed ( ) ;' },
        blocks: [block('b6', 'mod'), block('b7', 'mod')],
        funcs: {
            1: [{ refs: [{ b: 'b6', st: 'same', sim: 1 }], text: [['original();', false]], syntax: [['original', 'func_call'], ['();', 'syntax']] }],
            2: [{ refs: [{ b: 'b6', st: 'mod', sim: 0.8 }], text: [['changed();', true]], syntax: [['changed', 'func_call'], ['();', 'syntax']] }],
            4: [{ refs: [{ b: 'b6', st: 'mod', sim: 0.8 }], text: [['changed();', true]], syntax: [['changed', 'func_call'], ['();', 'syntax']] }],
        }, extras: [],
    },
};
view.data.consensus.blocks[1].cells[2].into = 'b6';
view.data.consensus.blocks[1].cells[4].into = 'b6';

let html = view._fullView();
const chips = html.match(/<button class="xp-chip"[^>]*>.*?<\/button>/g);
const grouped = chips.filter(chip => chip.includes('>C</span>') && chip.includes('>E</span>') && chip.includes('~ modified'));
assert.strictEqual(grouped.length, 2, 'C and E share one variant selector per block');
assert.ok(grouped.every(chip => chip.includes('~ modified') && chip.includes('aria-pressed="true"')));
assert.ok(grouped.every(chip => chip.includes('data-xfn="4"')), 'clicking the current group preserves selected E');
assert.ok(html.includes('↑ merged into b6 above'), 'merged code is not repeated');
assert.ok(html.includes('<b class="chg"><span class="token-func_call"'), 'changed code keeps syntax color and diff markup');

const showChips = () => view._fullView().split('<label class="xp-chk">')[0].match(/<button class="xp-chip"[^>]*>.*?<\/button>/g);
let show = showChips();
assert.strictEqual(show.length, 3, 'Show groups identical C/E into one choice alongside B and D');
assert.ok(show.some(chip => chip.includes('>C</span>') && chip.includes('>E</span>') && chip.includes('data-xfn="4"') && chip.includes('aria-pressed="true"')));
view.data.consensus.funcs[4].push({ refs: [], text: [['extra();', false]], syntax: [['extra', 'func_call'], ['();', 'syntax']] });
view.data.consensus.keys[4] = 'changed ( ) ; extra ( ) ;';
assert.strictEqual(showChips().length, 4, 'extra code splits otherwise identical functions');
view.data.consensus.funcs[4].pop();
view.data.consensus.keys[4] = 'changed ( ) ;';
view.data.consensus.funcs[2][0].text = [['changed();', true]];
view.data.consensus.funcs[4][0].text = [['changed();', true]];
view.data.consensus.truncated = true;
assert.strictEqual(showChips().length, 4, 'partial functions are not claimed to have identical full code');
view.data.consensus.truncated = false;

let rebody = 0;
view._rebody = () => { rebody++; };
view._click({ target: { closest: () => ({ dataset: { xfn: '3' } }) } });
assert.strictEqual(rebody, 1);
assert.strictEqual(view.s.fn, 3);
assert.ok(view._fullView().includes('(no matching code)'), 'missing variant remains readable');

view.s.fn = 1;
view.s.only = true;
html = view._fullView();
assert.ok(!html.includes('xp-full-groups'), 'identical blocks respect Differences only');
assert.ok(html.includes('2 blocks identical to <span') && html.includes('>A</span> hidden'));
view.s.only = false;
view.s.single = true;
assert.ok(!view._fullView().includes('xp-full-groups'), 'own-order mode remains a single function');
view.data.consensus.funcs[1].push({ refs: [{ b: 'b7', st: 'same', sim: 1 }], text: [['original();', false]], syntax: [['original', 'func_call'], ['();', 'syntax']] });
view.s.fn = 0;
show = showChips();
assert.ok(show.some(chip => chip.includes('>A</span>') && chip.includes('>B</span>') && chip.includes('data-xfn="0"')), 'own-order Show also groups identical reference code');

view.s.single = false;
view.s.fn = 4;
view.data.functions[4].file_name = '"><img src=x onerror=alert(1)>';
assert.ok(!view._fullView().includes('<img'), 'full-function filenames are escaped');
assert.ok(!view._ribbon(view.data.consensus.blocks.map(b => ({ b }))).includes('<img'), 'matrix filenames and tooltips are escaped');
assert.strictEqual(view._codeHtml([['<img>', 'variable']], null, ''), '<span class="token-variable">&lt;img&gt;</span>', 'syntax text is escaped');
view.fids = ['demo:func:abc:1000'];
const call = view._codeHtml([['callee', 'func_call', '2000', 'callee', false]], null, null, 0);
assert.ok(call.includes('data-xcall="demo:func:abc:2000"'), 'resolved calls carry a preview target');
assert.ok(call.includes('data-xcall-name="callee"'));
assert.ok(view._codeHtml([['puts', 'func_call', '3000', 'puts', true]], null, null, 0).includes('title="External call: puts"'), 'external calls get a label without a local preview');
assert.ok(view._codeHtml([['<bad>', 'func_call', null, '"><img>', false]], null, null, 0).includes('&lt;bad&gt;'), 'call text is escaped');
assert.ok(!view._codeHtml([['<bad>', 'func_call', null, '"><img>', false]], null, null, 0).includes('<img>'), 'call titles are escaped');
let preview = null, hidden = false;
context.window.showCodePreview = (...args) => { preview = args; };
context.window.hideCodePreview = () => { hidden = true; };
view.el.contains = () => true;
const callEl = { dataset: { xcall: 'demo:func:abc:2000', xcallName: 'callee' }, contains: () => false };
const hover = { target: { closest: () => callEl }, relatedTarget: null, clientX: 10, clientY: 20 };
view._callHover(hover, true);
assert.strictEqual(preview[0], 'demo:func:abc:2000');
assert.strictEqual(preview[5].currentTarget, callEl, 'preview stays anchored to the call token');
view._callHover(hover, false);
assert.ok(hidden, 'leaving the call hides the preview');
view.fids = view.data.functions.map((f, j) => `demo:func:${String(j).padStart(32, '0')}:${j + 1000}`);
view.data.functions.forEach((f, j) => { f.id = view.fids[j]; f.addr = String(j + 1000); f.name = `function-${j}`; });
context.window.NwayPanel = { fnUrl: fid => `/function/${fid}` };
let opened = null, menu = null;
context.window.Nav = { openPath: (url, event) => { opened = url; } };
context.window.showGraphContextMenu = (event, type, data) => { menu = { type, data }; };
assert.ok(view._card(view.data.functions[1], 1).includes('data-xfnref="1"'), 'card letter opens its function');
assert.ok(view._card(view.data.functions[1], 1).includes('data-xfile="1"'), 'card filename and MD5 open their file');
const eventFor = dataset => ({ target: { closest: () => ({ dataset }) }, preventDefault() {}, stopPropagation() {} });
view._click(eventFor({ xfnref: '1' }));
assert.strictEqual(opened, `/function/${view.fids[1]}`, 'letter navigates to its function');
view._click(eventFor({ xfile: '1' }));
assert.ok(opened.endsWith('/collections/demo/files/00000000000000000000000000000001'), 'filename navigates to its file');
view._click(eventFor({ xcall: 'demo:func:abc:2000', xcallName: 'callee' }));
assert.strictEqual(opened, '/function/demo:func:abc:2000', 'call token navigates to its target');
view._entityMenu(eventFor({ xfnref: '1' }));
assert.strictEqual(menu.data.function_id, view.fids[1], 'letter uses the shared function menu');
view._entityMenu(eventFor({ xfile: '1' }));
assert.strictEqual(menu.data.md5, '00000000000000000000000000000001', 'filename uses the shared file menu');
view._entityMenu(eventFor({ xcall: 'demo:func:abc:2000', xcallName: 'callee' }));
assert.strictEqual(menu.data.function_id, 'demo:func:abc:2000', 'call uses the shared function menu');

context.document = { addEventListener() {} };
vm.runInContext(fs.readFileSync(path.join(jsDir, 'nway_panel.js'), 'utf8'), context);
const panel = Object.create(context.window.NwayPanel.prototype);
const panelFile = { id: 'demo:file:abc', collection: 'demo', md5: 'abc', file_name: 'sample.exe' };
panel.data = { file_columns: [panelFile], columns_mode: 'files' };
panel.state = { column: panelFile.id, focus: '' };
panel.source = {};
assert.ok(panel._columnChip().includes('data-nway-file="demo:file:abc"'), 'filter chip filename is linked');
assert.ok(panel._focusBox().includes('data-nway-file="demo:file:abc"'), 'focus chip filename is linked');
panel._onClick(eventFor({ nwayFile: panelFile.id }));
assert.strictEqual(opened, '/collections/demo/files/abc', 'chip filename navigates to its file');
panel._menu(eventFor({}), panelFile.id);
assert.strictEqual(menu.data.fileId, 'demo:file:abc', 'chip filename uses the shared file menu');

view.data.consensus.funcs[4] = [
    { refs: [{ b: 'b6', st: 'mod', sim: 0.8 }], text: [['changed();', true]] },
    { refs: [], x: 'x1', text: [['extra();', false]] },
    { refs: [{ b: 'b7', st: 'mod', sim: 0.8 }], text: [['changed();', true]] },
];
view.data.consensus.extras = [{ id: 'x1', funcs: [4], title: 'extra branch', text: 'extra();' }];
view.s.single = false;
const blockIds = html => [...html.matchAll(/<span class="xp-bid">(b6|b7|x1)<\/span>/g)].map(m => m[1]);
assert.deepStrictEqual(blockIds(view._blocksView()), ['b6', 'x1', 'b7'], 'only-here block follows its source position in Blocks');
view.s.open = 'x1';
assert.ok(view._blocksView().includes('<pre>extra();</pre>'), 'extra code survives without syntax tokens');
view.s.open = null;
assert.deepStrictEqual(blockIds(view._fullView()), ['b6', 'x1', 'b7'], 'only-here block follows its source position in Full function');
assert.ok(view._blocksView().includes('<th>b6') && view._blocksView().indexOf('<th>x1') < view._blocksView().indexOf('<th>b7'), 'ribbon uses the same order');
view.s.only = true;
view.data.consensus.blocks[0].cells[4].st = 'same';
assert.deepStrictEqual(blockIds(view._fullView()), ['x1', 'b7'], 'extra remains visible when its anchor is hidden');
view.s.only = false;
view.data.consensus.blocks[0].cells[4].st = 'mod';
console.log('N-way full-function grouping, source order, syntax, and navigation: OK');
