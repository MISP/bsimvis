// Run: node scripts/test_nway_string_links.js
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const jsDir = path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js');
const context = vm.createContext({ window: {} });
const utils = fs.readFileSync(path.join(jsDir, 'utils.js'), 'utf8');
for (const name of ['escapeHtml', 'escapeAttr', 'quoteFilterValue', 'unquoteFilterValue']) {
    vm.runInContext(utils.match(new RegExp(`function ${name}[\\s\\S]*?\\n}`))[0], context);
}
vm.runInContext(fs.readFileSync(path.join(jsDir, 'nway_function.js'), 'utf8'), context);
const view = Object.create(context.window.NwayFunction.prototype);
view.fids = ['test:func:aa:00405a80'];
for (const value of [
    '[od_zygate] cve-2023-28771 fired at %s:%d\n',
    'a\tb\rc\n"quoted"',
    'C:\\temp\\new\\file',
    'literal * and [brackets]',
    'x'.repeat(200),
    '"><img src=x onerror=alert(1)>',
]) {
    const html = view._label({ key: 'strings' }, { label: value });
    const href = html.match(/href="([^"]*)"/)[1].replace(/&amp;/g, '&').replace(/&quot;/g, '"');
    const query = new URL(href, 'http://localhost').searchParams.get('strings');
    assert.strictEqual(query, context.quoteFilterValue(value.slice(0, 128)));
    assert.strictEqual(context.unquoteFilterValue(query).value, value.slice(0, 128));
    assert.ok(!html.includes('<img'), 'sample markup must stay escaped');
}
assert.strictEqual(view._label({ key: 'apis' }, { label: 'puts' }), 'puts');
console.log('N-way string links: ok');
