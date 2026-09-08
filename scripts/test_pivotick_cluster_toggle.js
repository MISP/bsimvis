// Two Pivotick cluster regressions, both invisible to `node --check`:
//
// 1. "Cluster by binary" off never went back to flat. render()'s incremental
//    path only ever *adds* nodes, so the cluster parents built by the
//    previous clustered render survived the toggle-off.
// 2. Pivotick sizes an expanded cluster from its children's getCircleRadius()
//    (sqrt(n) * 2 * avg(r + 16) + 50). Our rectangular cards never get one at
//    construction time, so children stayed at Node's 10px default and the
//    cluster circle came out far too small -- worse the more functions a
//    binary contributes.
//
// Run: node scripts/test_pivotick_cluster_toggle.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const vm = require('vm');

const root = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(root, 'bsimvis', 'app', 'static', 'js', 'pivotick_graph.js'), 'utf8');

// Minimal stand-in for the live Pivotick instance: records the node ids of
// every setData() and addNode(), and hands back mutable nodes whose
// children carry Pivotick's real 10px default radius.
class FakePivotick {
    constructor(container, data) { this.setDataCalls = 0; this.added = []; this._apply(data.nodes); }
    _apply(nodes) {
        this.nodes = nodes.map(n => ({
            id: n.id,
            expanded: !!n.expanded,
            hasChildren: () => !!(n.children || []).length,
            children: (n.children || []).map(c => ({
                id: c.id,
                _r: 10,
                getCircleRadius() { return this._r; },
                setCircleRadius(r) { this._r = r; },
            })),
        }));
        this.edges = new Map();
    }
    setData(nodes) { this.setDataCalls++; this._apply(nodes); }
    getNode(id) { return this.nodes.find(n => n.id === id); }
    getMutableNodes() { return this.nodes; }
    addNode(n) { this.added.push(n.id); this.nodes.push({ id: n.id, hasChildren: () => false, children: [] }); }
    addEdge() {}
    onChange() {}
    destroy() {}
}

const noop = () => {};
const sandbox = {
    window: { addEventListener: noop, removeEventListener: noop },
    document: { addEventListener: noop },
    setTimeout: noop,          // keep _fixNodeBoxSizes (pure DOM) out of the test
    console,
    Pivotick: FakePivotick,
    FunctionView: {
        fetchGraphNotes: async () => [],
        wireNoteSync: noop,
        callGraphRenderNode: () => '',
        renderEdgeLabel: () => '',
    },
};
sandbox.window.window = sandbox.window;
vm.createContext(sandbox);
vm.runInContext(src, sandbox);

const fn = (id, md5) => [id, { raw: { id, name: id, file_md5: md5, file_name: md5 }, kind: 'similar' }];
const makeController = () => {
    const c = new sandbox.window.PivotickGraphController({}, { collection: 'main' });
    c.nodes = new Map([fn('a', 'm1'), fn('b', 'm1'), fn('c', 'm1'), fn('d', 'm2'), fn('e', 'm2')]);
    return c;
};

(async () => {
    const c = makeController();
    await c.render();
    // Node ids come back through the vm realm, so compare joined strings --
    // cross-realm arrays never satisfy deepStrictEqual.
    const ids = () => c.pInstance.getMutableNodes().map(n => n.id).sort().join(',');
    assert.strictEqual(ids(), 'a,b,c,d,e', 'flat render must produce flat nodes');

    await c.toggleClustering(true);
    assert.strictEqual(ids(), 'cluster:m1,cluster:m2', 'clustering must nest into one node per binary');

    // 2: children must be sized like the cards they are, not 10px dots.
    const want = sandbox.window.PivotickGraphController.NODE_WIDTH / 2;
    const radii = c.pInstance.getMutableNodes().flatMap(n => n.children.map(ch => ch.getCircleRadius()));
    assert.strictEqual(radii.length, 5, 'every function must end up inside a cluster here');
    assert.ok(radii.every(r => r === want),
        `cluster children must get a card-sized circleRadius (${want}), got ${radii.join(',')}`);

    // 1: back off again is a full rebuild, not an incremental add.
    const before = c.pInstance.setDataCalls;
    await c.toggleClustering(false);
    assert.strictEqual(c.pInstance.setDataCalls, before + 1, 'un-clustering must rebuild, not add');
    assert.strictEqual(ids(), 'a,b,c,d,e', 'un-clustering must leave no cluster parents behind');
    assert.strictEqual(c.pInstance.added.length, 0, 'un-clustering must not go down the incremental path');

    // And a plain flat re-render after that stays incremental (no rebuild).
    const after = c.pInstance.setDataCalls;
    await c.render();
    assert.strictEqual(c.pInstance.setDataCalls, after, 'flat re-render must not rebuild');

    console.log('PASS: pivotick cluster toggle + child sizing');
})();
