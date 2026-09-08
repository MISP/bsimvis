// Self-check for the File sim main view's render paths: the tag tree, the
// Summary rollup, and the grouped function table.
//
// These three plus the Sankey read one shared expansion state, so the thing
// most worth pinning is that they agree: folding in one folds in all of them,
// and Expand all reaches the leaves that actually hold the function rows.
//
// Run: node scripts/test_filesim_view.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const src = fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', 'binary_similarity.js'),
    'utf8'
);

// The file is a browser-global script, not a module: run it against stubs and
// hand back the render entry points.
const els = {};
function el(id) {
    if (!els[id]) {
        els[id] = {
            id, innerHTML: '', textContent: '', style: {}, dataset: {}, value: '',
            clientWidth: 900, clientHeight: 500,
            classList: { toggle() {}, add() {}, remove() {} },
            querySelectorAll: () => [],
            addEventListener() {},
            previousElementSibling: null,
            closest: () => null,
        };
    }
    return els[id];
}
el('bin-sim-table-matched').previousElementSibling = el('bin-sim-thead');

const chain = new Proxy(function () {}, { get: () => chain, apply: () => chain });
let captured = null;
const d3Stub = {
    select: () => chain,
    sankey: () => {
        const layout = (g) => {
            g.nodes.forEach(n => Object.assign(n, { x0: 0, x1: 15, y0: 0, y1: 10 }));
            g.links.forEach(l => { l.source = g.nodes[l.source]; l.target = g.nodes[l.target]; });
            captured = g;
            return g;
        };
        ['nodeWidth', 'nodePadding', 'nodeAlign', 'nodeSort', 'extent'].forEach(k => { layout[k] = () => layout; });
        return layout;
    },
};

const fetchCalls = [];
const fetchStub = async (url) => {
    fetchCalls.push(url);
    return {
        ok: true,
        json: async () => ({
            // One folded name with copies, one plain row, and a total well past
            // the page size so the paging control has to appear.
            items: [
                { func_a: 'fa1', func_b: 'fb1', similarity: 0.98, state: 'matched', n_copies: 3, fold_name: 'memcpy' },
                { func_id: 'fa5', state: 'uniq_a', n_copies: 1 },
            ],
            total: 250,
            functions_metadata: {},
        }),
    };
};

const load = new Function(
    'window', 'document', 'd3', 'fetch', 'escapeHtml', 'escapeAttr', 'jsString', 'EntityRenderer',
    'location', 'history',
    `${src}
    return {
        setCache: (c) => { binSimDataCache = c; binSimCtx = { collection: 'c', md5a: 'a', md5b: 'b' }; },
        setTab: (t) => { fileSimTab = t; },
        renderFileSimTree, renderFileSimSummary, renderFileSimTable,
        switchTab: window.switchBinSimTab,
        setView: window.setFileSimView,
        expandAll: window.expandAllFileSimNodes,
        collapseAll: window.collapseAllFileSimNodes,
        selectNode: window.selectFileSimNode,
    };`
);

const M = load(
    { addEventListener() {}, location: { hash: '', pathname: '/x', search: '' }, history: { pushState() {} } },
    { getElementById: (id) => els[id] || el(id), querySelectorAll: () => [], activeElement: null },
    d3Stub,
    fetchStub,
    (s) => String(s),
    (s) => `"${s}"`,
    (s) => `'${s}'`,
    new Proxy({}, { get: () => (() => '') }),
    { hash: '', pathname: '/x', search: '' },
    { pushState() {} }
);

const tag = (type, name, ver, a, b, extra) => Object.assign({
    tag_id: `${type}:${name}:${ver}`, type, name, version: ver,
    unique_count_a: a, unique_count_b: b, bins: {}, children: [],
}, extra || {});

M.setCache({
    score: 0.68,
    counts: { matched: 100, unique_to_a: 20, unique_to_b: 10 },
    file_metadata_a: { file_name: 'a.elf' },
    file_metadata_b: { file_name: 'b.elf' },
    functions_metadata: {},
    tags_summary: [
        tag('lib', 'libc', '2.31', 80, 78, { drift: { 'origin:lib:libc:2.35': 12 } }),
        tag('lib', 'libc', '2.35', 5, 0),
        tag('lib', 'zlib', '1.2', 12, 12),
        tag('bundle', 'mirai', 'v1', 30, 28),
        {
            tag_id: 'original_code', type: 'original_code', name: 'Original Code',
            unique_count_a: 140, unique_count_b: 132, bins: {}, children: [],
        },
    ],
});

// ---- tree ---------------------------------------------------------------
M.renderFileSimTree();
const tree = els['bsim-tree'].innerHTML;
assert.ok(tree.includes('All'), 'root node');
assert.ok(tree.includes('Libraries') && tree.includes('Bundles'), 'groups');
// Original holds its mass directly: an "Original > Original code" nesting is a
// level that says nothing, and it read as a bug on screen.
assert.ok(!/Original code/.test(tree), 'Original is not nested under itself');
// Every node carries its own function counts, not just a percentage.
assert.ok(tree.includes('140/132'), 'nodes carry A/B counts');
console.log('ok  tree');

// ---- summary ------------------------------------------------------------
M.setTab('summary');
M.renderFileSimSummary();
assert.ok(els['bsim-summary-head'].innerHTML.includes('a.elf'), 'summary head names the pair');
assert.ok(els['bsim-summary-rollup'].innerHTML.includes('Libraries'), 'rollup lists groups');

// The rollup is not a one-level table: expanding reaches library-version depth.
M.expandAll();
const deep = els['bsim-summary-rollup'].innerHTML;
assert.ok(deep.includes('libc'), 'rollup reaches library');
assert.ok(deep.includes('2.31') && deep.includes('2.35'), 'rollup reaches version');

// ...and folds back with the tree, because they read the same state.
M.collapseAll();
assert.ok(!els['bsim-summary-rollup'].innerHTML.includes('2.31'), 'collapse all folds the rollup too');
console.log('ok  summary rollup folds with the tree, down to version');

// ---- table --------------------------------------------------------------
M.setTab('all');
M.expandAll();
M.renderFileSimTable();
const grouped = els['bin-sim-table-matched'].innerHTML;
assert.ok(grouped.includes('Libraries'), 'table shows group headers');
assert.ok(grouped.includes('libc'), 'table nests to library');

// Rows arrive asynchronously; give the stubbed fetches a turn.
setTimeout(() => {
    M.renderFileSimTable();
    const rows = els['bin-sim-table-matched'].innerHTML;

    // Expand all has to reach the leaves: a leaf has nothing to unfold in the
    // tree, but in the table its open state is what loads the function rows.
    assert.ok(rows.includes('more copies of memcpy'), 'duplicate fold pill');
    // The page size is 100, so a 250-name tag must offer the rest rather than
    // silently truncating.
    assert.ok(/Load\s+\d+\s+more/.test(rows), 'paging control');
    assert.ok(rows.includes('of 250 names'), 'paging states the total');

    assert.ok(fetchCalls.every(u => u.includes('collapse=name')), 'rows are requested folded by name');
    assert.ok(fetchCalls.some(u => u.includes('tags=')), 'rows are requested tag-scoped');
    console.log('ok  table groups, fold pill, paging');

    // ---- switching tab reloads the rows ---------------------------------
    // Pages are cached per tree node; the tab is what sets the `state` filter,
    // so reusing a cached page across tabs showed the previous tab's rows.
    const beforeSwitch = fetchCalls.length;
    M.switchTab('matched');
    setTimeout(() => {
        const refetch = fetchCalls.slice(beforeSwitch);
        assert.ok(refetch.length, 'switching tab refetches instead of reusing the cache');
        assert.ok(refetch.every(u => u.includes('state=matched')), 'refetch carries the new tab state');
        console.log('ok  tab switch invalidates the cached pages');

        // ---- the graph is the same rows, function to function ------------
        M.setView('graph');
        setTimeout(() => {
            assert.ok(captured, 'graph view builds a sankey');
            const ids = captured.nodes.map(n => n.id);
            assert.ok(ids.every(id => /^(a_|b_|none_)/.test(id)), `only function / no-match nodes: ${ids}`);
            assert.ok(!ids.some(id => /cluster/i.test(id)), 'no cluster column');
            // A match links one side straight to the other.
            assert.ok(
                captured.links.some(l => l.source.id === 'a_fa1' && l.target.id === 'b_fb1'),
                'matched row links func A directly to func B'
            );
            console.log('ok  graph view: direct function-to-function flow');
            console.log('OK: file sim view render paths');
        }, 50);
    }, 50);
}, 50);
