// Render check for the homepage view. No DOM library: HomeView only touches
// getElementById().innerHTML, localStorage and fetch, so faking those is enough
// to prove every panel renders and to assert what the markup must contain
// (status classes, progress bars, tooltips, the upload button, escaping).
// Run: node scripts/test_home_view.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const vm = require('vm');

const src = fs.readFileSync(
    path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js', 'views', 'home_view.js'), 'utf8');

const els = {};
const el = (id) => (els[id] = els[id] || { id, innerHTML: '' });

const STATS = {
    totals: { collections: 3, pools: 1, batches: 4, files: 120, functions: 98000 },
    jobs: { processing: 1, pending: 2, completed: 7, failed: 1 },
    recent_jobs: [
        { id: 'j1', type: 'ingest', status: 'running', progress: 42, collection: 'main', created_at: Date.now() - 5000, updated_at: Date.now() },
        { id: 'j2', type: 'build_sim', status: 'failed', progress: 100, collection: '<script>', created_at: 1, updated_at: 2 },
        { id: 'j3', type: 'cluster', status: 'pending', collection: null, created_at: 1, updated_at: 1 }
    ],
    recent_collections: [{ name: 'main', total_files: 120, total_functions: 98000, last_updated: Date.now() }]
};
const INSIGHTS = {
    tags: { top: [{ tag: 'capa/host-interaction', count: 40 }, { tag: 'lib:openssl', count: 10 }], namespaces: [{ namespace: 'capa', count: 40 }] },
    biggest_clusters: [{ collection: 'main', cluster_id: 2, cluster_name: 'memcpy', count: 31 }],
    recent_batches: [{ collection: 'main', batch_uuid: 'b-1', batch_name: 'nightly', last_updated: Date.now() }]
};

const ctx = {
    console,
    localStorage: { getItem: () => 'main' },
    document: { getElementById: el },
    fetch: async (url) => ({ json: async () => (url.includes('insights') ? INSIGHTS : STATS) }),
    formatDuration: () => '3s',
    formatRelativeTime: () => '1m ago'
};
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(src, ctx);

(async () => {
    await ctx.window.HomeView.init({}, 'root');
    const root = els['root'].innerHTML;
    const all = Object.values(els).map(e => e.innerHTML).join('\n');

    // Every panel rendered, none swallowed by the error branch.
    for (const id of ['home-stats', 'home-jobs', 'home-recent', 'home-capabilities', 'home-tags', 'home-clusters', 'home-batches']) {
        assert.ok(els[id] && els[id].innerHTML.length > 50, `${id} did not render`);
    }
    assert.ok(!/Insights failed|Could not load/.test(all), 'a panel fell into its error branch');

    // The asks: obvious upload button, big logo/title, no arbitrary icon colours.
    assert.ok(/home-btn-primary[^>]*>\s*<i class="fa-solid fa-cloud-arrow-up"><\/i> Upload binary/.test(root), 'no Upload binary button');
    assert.ok(root.includes("Nav.openPath('/collections/main/upload')"), 'upload button ignores last collection');
    assert.match(root, /logo\.svg" alt="" style="height:64px/, 'logo not enlarged');
    assert.ok(!/<i class="fa-solid [^"]*" style="color:#/.test(all), 'an icon still has a hard-coded colour');

    // Tables with coloured status + progress, like the collection view.
    assert.ok(all.includes('class="home-table"'), 'panels are not tables');
    assert.ok(all.includes('job-status-badge status-running') && all.includes('job-status-badge status-failed'),
        'job status is not colour-coded');
    assert.ok(all.includes('job-progress-fill progress-running') && all.includes('width:42%'), 'no job progress bar');

    // Hover (i) on each of the five counter cards.
    assert.strictEqual((els['home-stats'].innerHTML.match(/class="home-tip"/g) || []).length, 5,
        'expected one (i) tooltip per counter card');
    assert.ok(/data-tip="[^"]{20,}"/.test(els['home-stats'].innerHTML), 'tooltips have no text');

    // Untrusted values stay escaped.
    assert.ok(!all.includes('<script>') && all.includes('&lt;script&gt;'), 'collection name not escaped');

    console.log('home_view render check: PASS');
})().catch(e => { console.error(e); process.exit(1); });
