// The notes panel and the notes API must agree on which entity an id names.
// The rule lives twice -- `entityKindFromId` in `notes.js` picks the endpoint,
// `_bin_sim_collection` in `routes/notes.py` derives the pair's collection --
// and a pool pair is where they used to part company: its sid is
// `global:pool:<id>:bin_sim:<algo>:<md5a>::<md5b>`, so reading segment 1 gave
// "pool" and the panel silently posted pair notes to the function endpoint.
// Run: node scripts/test_pair_notes.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const vm = require('vm');

const root = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(root, 'bsimvis', 'app', 'static', 'js', ...p), 'utf8');

// Both files are browser globals: run each in a sandbox with just enough of a
// window to hang their exports off, and pull the two rules back out.
const noop = () => {};
const sandbox = {
    window: { addEventListener: noop },
    document: {
        addEventListener: noop,
        getElementById: () => null,
        querySelector: () => null,
        querySelectorAll: () => [],
        createElement: () => ({ style: {}, classList: { add: noop, remove: noop }, appendChild: noop }),
        documentElement: { classList: { add: noop, remove: noop, contains: () => false } },
        body: { appendChild: noop },
    },
    localStorage: { getItem: () => null, setItem: noop },
    console,
};
sandbox.window.window = sandbox.window;
vm.createContext(sandbox);
vm.runInContext(read('diff_queue.js'), sandbox);   // stripPoolPrefix
vm.runInContext(read('utils.js'), sandbox);
vm.runInContext(read('notes.js'), sandbox);

const { entityKindFromId, getCollectionFromId } = sandbox.window;

const FUNC = 'main:func:d41d8cd98f00b204e9800998ecf8427e:00401000';
const FILE = 'main:file:d41d8cd98f00b204e9800998ecf8427e';
const PAIR = 'main:bin_sim:unweighted_cosine:aaaa::bbbb';
const POOL_PAIR = 'global:pool:p1:bin_sim:unweighted_cosine:aaaa::bbbb';

assert.strictEqual(entityKindFromId(FUNC), 'func');
assert.strictEqual(entityKindFromId(FILE), 'file');
assert.strictEqual(entityKindFromId(PAIR), 'bin_sim');
// The regression: segment 1 of a pool pair sid is "pool", not "bin_sim".
assert.strictEqual(entityKindFromId(POOL_PAIR), 'bin_sim');
assert.strictEqual(entityKindFromId(undefined), 'func');

// The collection a pair note is filed under is the sid's own prefix -- pool
// scope included, since a pool pair is its own document, not a reference back
// to a collection's. This is what `_bin_sim_collection` derives server-side.
assert.strictEqual(getCollectionFromId(PAIR), 'main');
assert.strictEqual(getCollectionFromId(POOL_PAIR), 'global:pool:p1');
assert.strictEqual(getCollectionFromId(FILE), 'main');
assert.strictEqual(getCollectionFromId(FUNC), 'main');

console.log('pair-note id routing: OK');
