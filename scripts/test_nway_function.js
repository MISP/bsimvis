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
    id, title: 'branch', ref_text: 'original();',
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
        blocks: [block('b6', 'mod'), block('b7', 'mod')],
        funcs: {
            1: [{ refs: [{ b: 'b6', st: 'same', sim: 1 }], text: [['original();', false]] }],
            2: [{ refs: [{ b: 'b6', st: 'mod', sim: 0.8 }], text: [['changed();', true]] }],
            4: [{ refs: [{ b: 'b6', st: 'mod', sim: 0.8 }], text: [['changed();', true]] }],
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
assert.ok(html.includes('<b class="chg">changed();</b>'), 'changed tokens stay highlighted');

const showChips = () => view._fullView().split('<label class="xp-chk">')[0].match(/<button class="xp-chip"[^>]*>.*?<\/button>/g);
let show = showChips();
assert.strictEqual(show.length, 3, 'Show groups identical C/E into one choice alongside B and D');
assert.ok(show.some(chip => chip.includes('>C</span>') && chip.includes('>E</span>') && chip.includes('data-xfn="4"') && chip.includes('aria-pressed="true"')));
view.data.consensus.funcs[2][0].text = [['FUN_001234();', false]];
view.data.consensus.funcs[4][0].text = [['  FUN_005678();  ', true]];
assert.strictEqual(showChips().length, 3, 'generated addresses, whitespace and diff highlights do not split equal code');
view.data.consensus.funcs[4].push({ refs: [], text: [['extra();', false]] });
assert.strictEqual(showChips().length, 4, 'extra code splits otherwise identical functions');
view.data.consensus.funcs[4].pop();
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
assert.ok(html.includes('2 blocks identical to A hidden'));
view.s.only = false;
view.s.single = true;
assert.ok(!view._fullView().includes('xp-full-groups'), 'own-order mode remains a single function');
view.data.consensus.funcs[1].push({ refs: [{ b: 'b7', st: 'same', sim: 1 }], text: [['original();', false]] });
view.s.fn = 0;
show = showChips();
assert.ok(show.some(chip => chip.includes('>A</span>') && chip.includes('>B</span>') && chip.includes('data-xfn="0"')), 'own-order Show also groups identical reference code');

view.s.single = false;
view.s.fn = 4;
view.data.functions[4].file_name = '"><img src=x onerror=alert(1)>';
assert.ok(!view._fullView().includes('<img'), 'full-function filenames are escaped');
assert.ok(!view._ribbon(view.data.consensus.blocks, []).includes('<img'), 'matrix filenames and tooltips are escaped');
console.log('N-way full-function variant grouping: OK');
