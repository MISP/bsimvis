/**
 * Lineage chart of the files of an N-way set (GET /api/bin_sim/nway/lineage).
 *
 * NwayLineage.mount(container, opts) -> { setDoc(doc, lens), setLens(lens), setTrack(t), setView(v), hover(row), select(id), destroy() }.
 * opts: toolbar (elements), fetchFile(id), lens, track, onTrack(track), view ('tree' | 'tracks':
 * a group list beside the tree, or six mini trees | 'ring': nway_ring.js draws the clusters and modules in the same canvas,
 * ring {nest, arc, min, list, tier, bundle}, onRing(state)), onNode(node) (true when it took the click: no dock),
 * laneNames (false: no model names), onGroup(what, track, lens) ('graph' | 'table'; true when it took it).
 * One node per file at a fixed px-per-function scale, columns by tree depth. A
 * node body is cut by which children keep the code (grey), plus "kept by no
 * child" (orange); a strip on its left says what was born here (blue, hatched =
 * likely matcher miss) and a strip on its right marks the reuse classes it takes
 * part in (violet). Ribbons between nodes are the shared code. Reuse arcs join
 * the files of a class that the tree does not connect: they leave the right edge
 * into the gap, run in a lane above the columns and drop into the gap of the
 * other file, so they never cross a box.
 *
 * d3.zoom pans and zooms; detail is semantic (labels, segments, hatching appear
 * with the zoom level and only for what is on screen). Subtrees can be collapsed
 * into one aggregate node. Everything from a sample (file names, md5s) is set
 * with textContent / attributes, never innerHTML.
 *
 * With the doc's compact row list (`rows`, `regions`) the other lenses paint the
 * bodies and ribbons by region, presence band, lane or role instead (Deltas is
 * the view above). Rows failing the page filters (`rows.match`) sit faint at the
 * bottom of each node, so filters never move the tree. `track` (fn:<fid> or
 * <lens>:<key>) lights the nodes holding one row or group. A clicked node opens
 * the dock: its rows by lens, its diff against the parent, a function preview.
 * When the doc carries the table's groups for the lens (`groups`, `rows.group`),
 * they are the lens's groups: same ids, names (model names via NwayGroups) and
 * header pieces as the table, and a tracked one gets a card with its heaviest
 * functions and pivots to the table (its call graph, or the group tracked).
 */
(function () {
    const NS = 'http://www.w3.org/2000/svg';
    const NODE_W = 120, ORIG_W = 14, REUSE_W = 8, GAP = 230, PAD = 40, NPAD = 28;
    const KY_MAX = 1.2, KY_MIN = 0.12, TARGET_H = 1600, REUSE_MAX = 14;
    const ARC_MAX_PIECES = 6, AUTO_FILES = 40, AUTO_SHOW = 60, FAN_FADE = 8, K_MIN = 0.05, K_MAX = 40;
    const DETAIL_K = 0.3, LABEL_K = 0.55, HATCH_K = 1.5;
    const V = { born: 'var(--lin-born)', died: 'var(--lin-died)', shared: 'var(--lin-grey)', fork: 'var(--lin-grey-d)', slate: 'var(--lin-slate)', slateHi: 'var(--lin-slate-hi)', reuse: 'var(--lin-reuse)' };
    const CAVEAT = 'reuse, independent birth or matcher miss';
    // same as nway_panel.js LENSES (key, label, group preset) and its tab icons
    const LENSES = [['modules', 'Modules', 'region,role', 'fa-diagram-project'], ['core', 'Core', 'presence,region', 'fa-bullseye'], ['deltas', 'Deltas', 'delta,region', 'fa-code-compare'], ['lanes', 'Lanes', 'lane,region', 'fa-layer-group'], ['triage', 'Triage', 'role,region', 'fa-list-ol']];
    // same as nway_panel.js ROLES; colours as dashboard.css .r-*
    const ROLES = [['orchestrator', 'Orchestrator', 'var(--token-warning)'], ['entry', 'Entry point', 'var(--token-instruction)'], ['logic', 'Logic', 'var(--accent)'], ['compute', 'Compute', 'var(--note-accent)'], ['leaf', 'Leaf helper', 'var(--success)'], ['plumbing', 'Plumbing', 'var(--subtle)'], ['runtime', 'Runtime', 'var(--border)'], ['library', 'Library', 'var(--info)']];
    const BANDS = [['every', 'In every file', 'var(--success)'], ['most', 'Most files (75%+)', 'var(--accent)'], ['some', 'Some files', 'var(--warning, #d97706)'], ['few', 'One or two files', 'var(--token-instruction, #e11d48)']];
    // nway_region_service.SPECIAL
    const SPECIAL = { small: 'Small regions', none: 'Outside the call graph', runtime: 'Runtime', library: 'Library' };
    const GREY = 'var(--lin-grey-d)', LANE_MAX = 12, LEGEND_MAX = 14, DOCK_ROWS = 5;
    const hueOf = i => window.NwayColors.hue(i);
    // --lin-hl is the table's top-level lightness: 52%, 44% on the light theme (.nway-sq.on)
    const hsl = (h, l = 52) => `hsl(${h} 62% calc(var(--lin-hl) + ${l - 52}%))`;
    // A node's colour is its column's in the table (nway_colors.js): doc.nodes are in the default
    // column order, `path` groups them as the table's column tree does.
    const nodeColor = (doc, i) => {
        const T = doc._colors || (doc._colors = window.NwayColors.tree(doc.nodes.map(n => ({ id: n.id, path: n.path }))));
        const c = T.grouped && T.colors[i];
        return c ? hsl(c.h, c.l) : hsl(hueOf(i));
    };
    const fmt = n => Number(n).toLocaleString();
    let uid = 0;

    const CSS = `
.lin-root,.lin-vars{--lin-hl:52%;--lin-slate:#3d4a5f;--lin-slate-hi:#52617a;--lin-edge:rgba(255,255,255,.16);--lin-grey:#8b95a5;--lin-grey-d:#5d6a7e;--lin-born:#3d9be0;--lin-died:#f08a3c;--lin-reuse:#8b6fd6;--lin-card:var(--card-bg,#1e1e1e);--lin-line:var(--border,#3e3d32);--lin-halo:var(--bg,#121212);font:12.5px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-variant-numeric:tabular-nums;color:var(--text,#f8f8f2);color-scheme:dark}
html.light-theme .lin-root,html.light-theme .lin-vars{--lin-slate:#4a5870;--lin-slate-hi:#66758f;--lin-edge:rgba(0,0,0,.2);--lin-grey:#8892a3;--lin-grey-d:#5f6b7f;--lin-born:#0072B2;--lin-died:#D55E00;--lin-reuse:#6a4fbf;--lin-hl:44%;color-scheme:light}
.lin-tb{display:flex;gap:8px 10px;align-items:center;flex-wrap:wrap;padding:6px 10px;border-bottom:1px solid var(--lin-line);background:var(--lin-card)}
.lin-tb select,.lin-tb input{background:var(--lin-card);color:inherit;border:1px solid var(--lin-line);border-radius:8px;padding:4px 8px;font:inherit}
.lin-tb input{width:150px}
.lin-seg{display:inline-flex;border:1px solid var(--lin-line);border-radius:8px;overflow:hidden}
.lin-seg button,.lin-btn{background:transparent;border:0;color:inherit;padding:4px 9px;cursor:pointer;font:inherit}
.lin-btn{border:1px solid var(--lin-line);border-radius:8px}
.lin-seg button{border-left:1px solid var(--lin-line)}
.lin-seg button:first-child{border-left:0}
.lin-seg button:hover,.lin-btn:hover{background:var(--hover,#2d2d2d)}
.lin-btn[aria-pressed=true]{border-color:var(--lin-reuse);color:var(--lin-reuse)}
.lin-chip{display:inline-flex;align-items:center;gap:6px;padding:1px 9px;border:1px solid var(--lin-line);border-radius:999px;font-size:11.5px;white-space:nowrap}
.lin-sw{width:12px;height:8px;border-radius:2px;display:inline-block}
.lin-canvas{position:relative;flex:1;min-height:0;overflow:hidden;background:var(--lin-card)}
.lin-canvas>svg{position:absolute;inset:0;display:block;cursor:grab}
.lin-canvas>svg:active{cursor:grabbing}
.lin-mini{position:absolute;right:12px;bottom:12px;width:190px;height:130px;border:1px solid var(--lin-line);border-radius:8px;background:var(--lin-card);opacity:.92;box-shadow:0 4px 14px rgba(0,0,0,.3)}
.lin-mini svg{width:100%;height:100%;display:block;cursor:pointer;touch-action:none}
.lin-note{position:absolute;left:12px;bottom:12px;max-width:60%;padding:4px 10px;border:1px solid var(--lin-line);border-radius:8px;background:var(--lin-card);font-size:12px;opacity:.95}
.lin-note button{background:none;border:0;color:var(--accent,#66d9ef);cursor:pointer;font:inherit;text-decoration:underline}
.lin-drawer{position:absolute;top:0;right:0;bottom:0;width:360px;max-width:85%;transform:translateX(102%);transition:transform .2s ease;background:var(--lin-card);border-left:1px solid var(--lin-line);padding:12px 14px;overflow:auto;z-index:5;box-shadow:-6px 0 18px rgba(0,0,0,.25)}
.lin-drawer.open{transform:none}
.lin-drawer h4{margin:0 0 8px;font-size:13px;word-break:break-all;padding-right:22px}
.lin-x{position:absolute;top:6px;right:8px;background:none;border:0;color:inherit;font-size:18px;cursor:pointer}
.lin-kv{display:grid;grid-template-columns:1fr auto;gap:2px 14px}
.lin-kv .k{opacity:.7}
.lin-dim{opacity:.72;font-size:12px}
.lin-tip{position:fixed;pointer-events:none;display:none;z-index:1000;min-width:200px;max-width:340px;padding:9px 11px;border-radius:10px;background:var(--lin-card);color:var(--text,#f8f8f2);border:1px solid var(--lin-line);box-shadow:0 8px 24px rgba(0,0,0,.35);font:12.5px/1.45 system-ui,sans-serif}
.lin-tip .t{display:flex;align-items:center;gap:7px;font-weight:600;margin-bottom:5px;word-break:break-all}
.lin-dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex:none}
.lin-canvas svg text{font-family:inherit}
.lin-halo{paint-order:stroke;stroke:var(--lin-halo);stroke-width:3.5px;stroke-linejoin:round}
.lin-canvas ::-webkit-scrollbar{width:10px;height:10px}
.lin-drawer{scrollbar-width:thin;scrollbar-color:var(--lin-grey-d) transparent}
.lin-root{display:flex;flex-direction:column;height:100%;min-height:0}
.lin-filters{gap:8px 12px;flex:none}
.lin-filters .nway-search{flex:1 1 240px;max-width:420px}
.lin-filters .nway-search input{width:100%;border:0;padding:0}
.lin-seg button[aria-pressed=true]{background:var(--hover,#2d2d2d);color:var(--accent,#66d9ef)}
.lin-n{opacity:.65;margin-left:4px;font-size:11px}
.lin-filters .nway-lenses{margin:0;border:0}
button.lin-chip{background:transparent;color:inherit;cursor:pointer;font-family:inherit}
.lin-chip.on{border-color:var(--accent,#66d9ef);color:var(--accent,#66d9ef)}
.lin-chip .lin-x2{background:none;border:0;color:inherit;cursor:pointer;padding:0 2px;font-size:14px;line-height:1}
.lin-tabs{display:flex;gap:2px;border-bottom:1px solid var(--lin-line);margin:10px 0 6px}
.lin-tabs button{background:none;border:0;border-bottom:2px solid transparent;color:inherit;padding:4px 8px;cursor:pointer;font:inherit;opacity:.75}
.lin-tabs button[aria-selected=true]{border-bottom-color:var(--accent,#66d9ef);color:var(--accent,#66d9ef);opacity:1}
.lin-gh{display:flex;align-items:center;gap:6px;margin:8px 0 2px;font-weight:600}
.lin-gh .lin-dim{font-weight:400}
.lin-ic{background:none;border:0;color:inherit;cursor:pointer;opacity:.55;padding:0 4px;margin-left:auto}
.lin-ic:hover,.lin-ic.on{opacity:1;color:var(--accent,#66d9ef)}
.lin-row{display:grid;grid-template-columns:minmax(0,1fr) auto 96px;gap:6px;align-items:center;width:100%;background:none;border:0;border-radius:6px;color:inherit;text-align:left;padding:2px 4px;cursor:pointer;font:inherit}
.lin-row:hover{background:var(--hover,#2d2d2d)}
.lin-row.on{outline:1px dashed var(--accent,#66d9ef)}
.lin-nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--font-mono,monospace);font-size:12px}
.lin-nm .fa-solid{margin-right:5px;font-size:10px;opacity:.7}
.lin-pres{display:flex;gap:1px;height:9px;overflow:hidden}
.lin-pres i{flex:1 1 0;min-width:1px;max-width:9px;border-radius:1px;box-shadow:inset 0 0 0 1px var(--lin-line)}
.lin-more{background:none;border:0;color:var(--accent,#66d9ef);cursor:pointer;font:inherit;padding:2px 4px}
.lin-names{display:flex;flex-wrap:wrap;gap:4px;margin:2px 0 6px}
.lin-names button{background:none;border:1px solid var(--lin-line);border-radius:6px;color:inherit;cursor:pointer;font:11.5px var(--font-mono,monospace);padding:1px 6px;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lin-plus{color:var(--lin-born);font-weight:600}
.lin-minus{color:var(--lin-died);font-weight:600}
.lin-code{max-height:55vh;overflow:auto;margin-top:6px}
.lin-root button:focus-visible,.lin-root select:focus-visible,.lin-root input:focus-visible{outline:2px solid var(--accent,#66d9ef);outline-offset:2px}
.lin-tb{flex:none}
.lin-zoom{position:absolute;right:12px;top:10px;z-index:3;background:var(--lin-card)}
.lin-root>.lin-tb{padding:4px 0}
.nway-lean{flex:1;min-height:0;display:flex;flex-direction:column}
.nway-stage{flex:1;min-height:0;display:flex;flex-direction:column;border-top:1px solid var(--border)}
.nway-lean.has-table .nway-stage{flex:none;height:42%;min-height:360px}
.lin-body{flex:1;min-height:0;display:flex}
.lin-body>.lin-canvas{min-width:0}
.lin-glist{flex:none;width:300px;max-width:45%;overflow:auto;border-right:1px solid var(--lin-line);padding:6px 8px;background:var(--lin-card)}
.lin-glist .lin-seg{margin:2px 0 8px}
.lin-gi{display:flex;flex-direction:column;gap:4px;width:100%;margin-bottom:2px;text-align:left;background:none;border:1px solid transparent;border-radius:6px;color:inherit;padding:5px 7px;cursor:pointer;font:inherit}
.lin-gi:hover{background:var(--hover,#2d2d2d)}
.lin-gi.on{border-color:var(--accent,#66d9ef)}
.lin-gi .t,.lin-mt .t{display:flex;gap:6px;align-items:center;min-width:0}
.lin-gi .t b,.lin-mt .t span:nth-child(2){overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600}
.lin-gi .t .lin-dim{margin-left:auto}
.lin-multi{flex:1;min-width:0;overflow:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:8px;padding:10px;align-content:start;background:var(--lin-card)}
.lin-mt{border:1px solid var(--lin-line);border-radius:6px;background:none;color:inherit;padding:6px 8px;cursor:pointer;text-align:left;font:inherit;display:flex;flex-direction:column;gap:4px;min-width:0}
.lin-mt:hover{background:var(--hover,#2d2d2d)}
.lin-mt.on{border-color:var(--accent,#66d9ef)}
.lin-mt svg{width:100%;height:150px;display:block}
.lin-glist[hidden],.lin-multi[hidden]{display:none}
.lin-gn{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lin-gi .t .lin-gn{flex:1 1 auto}
.lin-chip .lin-gn{max-width:280px}
.lin-gn .nway-bicon,.lin-gn .nway-ricon{margin-right:4px}
.lin-gev{display:flex;flex-wrap:wrap;gap:4px}
.lin-gev:empty{display:none}
.lin-gev .nway-gchip{margin:0}
.lin-gst{display:flex;flex-wrap:wrap;gap:4px 10px;align-items:center;font-size:11.5px;opacity:.85}
.lin-card{border:1px solid var(--lin-line);border-radius:8px;padding:8px 10px;margin:4px 0 10px;display:flex;flex-direction:column;gap:5px}
.lin-card .lin-gh{margin:0}
.lin-acts{display:flex;gap:6px;flex-wrap:wrap}
.lin-sum{font-size:11.5px;opacity:.75;margin:-3px 0 4px 22px}
.lin-sum:empty{display:none}
@media (max-width:700px){.lin-mini{display:none}.lin-body{flex-direction:column}.lin-glist{width:auto;max-width:none;max-height:35%;border-right:0;border-bottom:1px solid var(--lin-line)}}
`;

    function el(tag, attrs = {}, text) {
        const e = document.createElement(tag);
        for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
        if (text !== undefined) e.textContent = text;
        return e;
    }

    // ---------------------------------------------------------------- model
    // Visible nodes (files, or one aggregate per collapsed subtree) in DFS order.
    function buildModel(doc, collapsed) {
        const byId = new Map(doc.nodes.map((n, idx) => [n.id, { ...n, idx }]));
        const kids = new Map(), inE = new Map();
        for (const e of doc.edges) { (kids.get(e.parent) || kids.set(e.parent, []).get(e.parent)).push(e.child); inE.set(e.child, e); }
        const size = new Map();
        const sz = id => { if (!size.has(id)) size.set(id, 1 + (kids.get(id) || []).reduce((a, c) => a + sz(c), 0)); return size.get(id); };
        sz(doc.root);
        // first child continues the parent's lane: most similar build first, divergent ones below
        for (const l of kids.values()) l.sort((a, b) => (inE.get(b).similarity ?? 0) - (inE.get(a).similarity ?? 0) || size.get(b) - size.get(a) || (inE.get(b).shared - inE.get(a).shared));
        const rep = new Map(), vis = [];
        const members = (id, out = []) => { out.push(id); for (const c of kids.get(id) || []) members(c, out); return out; };
        const walk = (id, d) => {
            const n = byId.get(id);
            if (collapsed.has(id) && (kids.get(id) || []).length) {
                const ms = members(id), nodes = ms.map(m => byId.get(m));
                const sum = f => nodes.reduce((a, x) => a + (f(x) || 0), 0);
                const a = {
                    id: `agg:${id}`, agg: true, top: id, members: ms, d, file_name: `${ms.length} files`, idx: n.idx,
                    functions: sum(x => x.functions), died_all: sum(x => x.died_all), only_here: sum(x => x.only_here), reuse_in: sum(x => x.reuse_in),
                    born: ms.reduce((s, m) => s + (inE.get(m)?.gained || 0), 0), suspect: ms.reduce((s, m) => s + (inE.get(m)?.recall_suspect || 0), 0),
                };
                for (const m of ms) rep.set(m, a.id);
                vis.push(a);
                return;
            }
            n.d = d; n.kind = 'file';
            rep.set(id, id);
            vis.push(n);
            for (const c of kids.get(id) || []) walk(c, d + 1);
        };
        walk(doc.root, 0);
        return { byId, kids, inE, vis, rep, size };
    }

    // Above AUTO_FILES files: group every subtree below the deepest level that keeps about AUTO_SHOW nodes visible.
    function autoCollapse(doc) {
        const out = new Set();
        if (doc.nodes.length <= AUTO_FILES) return out;
        const kids = new Map(), depth = new Map([[doc.root, 0]]);
        for (const e of doc.edges) { (kids.get(e.parent) || kids.set(e.parent, []).get(e.parent)).push(e.child); depth.set(e.child, depth.get(e.parent) + 1); }
        const perDepth = new Map();
        for (const d of depth.values()) perDepth.set(d, (perDepth.get(d) || 0) + 1);
        let D = 1, seen = 0;
        for (let d = 0; d <= Math.max(...perDepth.keys()); d++) { seen += perDepth.get(d) || 0; if (seen > AUTO_SHOW) break; D = d; }
        D = Math.max(D, 1);
        for (const [id, d] of depth) if (d === D && (kids.get(id) || []).length) out.add(id);
        return out;
    }

    // ---------------------------------------------------------------- rows (doc.rows, column-oriented)
    // Per node row lists and a row x node bitset from the hex masks (bit i = nodes[i] holds the row).
    function rowModel(doc, M) {
        const R = doc.rows;
        if (!R || !R.mask) return null;
        const N = doc.nodes.length, B = Math.ceil(N / 8) || 1, n = R.n ?? R.mask.length;
        const bits = new Uint8Array(n * B), pop = new Uint16Array(n), held = doc.nodes.map(() => []);
        // lowercase, no leading zeros: the form the table's lanes:<hex> track key uses
        const mask = Array.from(R.mask, m => String(m || '0').toLowerCase().replace(/^0+(?=.)/, ''));
        for (let r = 0; r < n; r++) {
            const h = mask[r] || '';
            for (let j = 0; j < h.length; j++) {
                const v = parseInt(h[h.length - 1 - j], 16) || 0;
                for (let b = 0; b < 4; b++) {
                    const i = j * 4 + b;
                    if (!((v >> b) & 1) || i >= N) continue;
                    bits[r * B + (i >> 3)] |= 1 << (i & 7);
                    held[i].push(r);
                    pop[r]++;
                }
            }
        }
        const has = (r, i) => (bits[r * B + (i >> 3)] >> (i & 7)) & 1;
        // where a row is born: the first node in DFS order that holds it while its parent does not
        const bornIn = new Int32Array(n).fill(-1);
        const walk = id => {
            const i = M.byId.get(id).idx, e = M.inE.get(id), p = e ? M.byId.get(e.parent).idx : -1;
            for (const r of held[i]) if (bornIn[r] < 0 && (p < 0 || !has(r, p))) bornIn[r] = i;
            for (const c of M.kids.get(id) || []) walk(c);
        };
        walk(doc.root);
        const col = k => R[k] || [];
        // a member file id -> its node id: a deltas key or a hovered table row may name files, not nodes
        const nodeOf = new Map();
        for (const x of doc.nodes) for (const m of [x.id, ...(x.members || [])]) nodeOf.set(m, x.id);
        return { n, N, pop, nodeOf, held, has, bornIn, fid: col('fid'), fids: R.fids || null, name: col('name'), mask, role: col('role'), region: col('region'), library: col('library'), weight: col('weight'), group: R.group || null, match: r => !R.match || !!R.match[r] };
    }

    const presenceBand = (c, N) => (c >= N ? 'every' : c >= 0.75 * N ? 'most' : c > 2 ? 'some' : 'few');

    // The table's groups (doc.groups, built for the lens preset doc.group over the filtered rows): cats are
    // the groups holding rows themselves, `groups` every one in table order with its track key (tk);
    // track(t) marks the rows of the group(s) with that key and of the groups under them.
    function serverCats(doc, R, lens, label) {
        const G = doc.groups, idx = new Map(G.map((g, i) => [g.id, i])), direct = new Uint32Array(G.length);
        const anc = G.map(g => (g.path || [g.id]).map(p => idx.get(p)).filter(i => i != null));
        for (let r = 0; r < R.n; r++) if (R.group[r] != null) direct[R.group[r]]++;
        let hues = 0;
        const color = (g, i) => {
            if (g.kind === 'role') return (ROLES.find(x => x[0] === g.role) || [0, 0, GREY])[2];
            if (g.kind === 'presence') return (BANDS.find(x => `presence:${x[0]}` === g.id) || [0, 0, GREY])[2];
            if (g.kind === 'region' && !g.label) return hsl(hueOf(hues++));
            if (g.kind === 'lane' && direct[i] && hues < LANE_MAX) return hsl(hueOf(hues++ * 3 + 1));
            return GREY;
        };
        const groups = G.map((g, i) => {
            const tk = window.NwayGroups.trackKey(g, g.id, ci => ci);
            return { i, g, tk, key: tk, depth: anc[i].length - 1, direct: direct[i], color: color(g, i), get label() { return label(g); } };
        });
        const of = r => (R.group[r] == null ? '' : groups[R.group[r]].key);
        const cats = groups.filter(x => x.direct);
        for (let r = 0; r < R.n; r++) if (R.group[r] == null && R.match(r)) { cats.push({ key: '', tk: '', label: 'Other functions', color: GREY }); break; }
        // every row of each group, its sub-groups' included
        const members = G.map(() => []);
        for (let r = 0; r < R.n; r++) if (R.group[r] != null) for (const a of anc[R.group[r]]) members[a].push(r);
        const track = t => {
            const hit = groups.filter(x => x.tk === t);
            if (!hit.length) return null;
            const mark = new Uint8Array(R.n);
            for (const x of hit) for (const r of members[x.i]) mark[r] = 1;
            return mark;
        };
        return { lens, cats, groups, of, raw: of, track, members };
    }

    // The groups of one lens: cats in display order, of(r) = the row's group key, raw(r) = the key `track` uses,
    // tk = the track value. The server's groups when the doc carries them for this lens (not Deltas).
    function lensCats(doc, R, lens, label = g => g.label || g.id) {
        const L = LENSES.find(l => l[0] === lens);
        if (lens !== 'deltas' && L && doc.groups && doc.group === L[2] && R.group) return serverCats(doc, R, lens, label);
        const cat = (key, label, color) => ({ key: String(key), tk: `${lens}:${key}`, label, color });
        const done = (cats, of, raw = of) => ({ lens, cats, of, raw });
        if (lens === 'core') return done(BANDS.map(([k, l, c]) => cat(k, l, c)), r => presenceBand(R.pop[r], R.N));
        if (lens === 'triage') {
            const known = new Set(ROLES.map(x => x[0]));
            return done([...ROLES.map(([k, l, c]) => cat(k, l, c)), cat('', 'No role', GREY)], r => (known.has(R.role[r]) ? R.role[r] : ''));
        }
        if (lens === 'modules') {
            const regs = doc.regions || {}, keys = Object.keys(regs), seen = new Set(keys), pre = new Map();
            for (let r = 0; r < R.n; r++) {
                const g = R.region[r] == null ? '' : String(R.region[r]);
                if (!seen.has(g)) { seen.add(g); keys.push(g); }
                // the server leaves call-graph regions unnamed: label them by their commonest name prefix
                const m = /^([A-Za-z][A-Za-z0-9]*)_/.exec(R.name[r] || '');
                if (m && !/^(FUN|sub|thunk|LAB|DAT)$/.test(m[1])) { const c = pre.get(g) || pre.set(g, new Map()).get(g); c.set(m[1], (c.get(m[1]) || 0) + 1); }
            }
            const prefix = k => { const c = pre.get(k); if (!c) return ''; const [p, n] = [...c.entries()].sort((a, b) => b[1] - a[1])[0]; return n > 1 ? `${p}_*` : ''; };
            const grey = k => k === '' || k in SPECIAL || ['runtime', 'library'].includes((regs[k] || {}).kind);
            return done(keys.map((k, i) => cat(k, (regs[k] || {}).label || SPECIAL[k] || (k ? prefix(k) || `Region ${i + 1}` : 'No region'), grey(k) ? GREY : hsl(hueOf(i)))),
                r => (R.region[r] == null ? '' : String(R.region[r])));
        }
        if (lens === 'lanes') {
            // one lane per exact file set; the biggest LANE_MAX get a colour, the rest share one
            const cnt = new Map();
            for (let r = 0; r < R.n; r++) cnt.set(R.mask[r], (cnt.get(R.mask[r]) || 0) + 1);
            const top = [...cnt.entries()].sort((a, b) => b[1] - a[1]).slice(0, LANE_MAX).map(e => e[0]);
            const first = new Map();
            for (let r = 0; r < R.n; r++) if (!first.has(R.mask[r])) first.set(R.mask[r], r);
            const label = m => {
                const r = first.get(m), files = doc.nodes.filter((_, i) => R.has(r, i)).map(x => x.file_name || x.id);
                return `${files.length} file${files.length === 1 ? '' : 's'}: ${files.slice(0, 2).join(', ')}${files.length > 2 ? ` +${files.length - 2}` : ''}`;
            };
            const keep = new Set(top);
            return done([...top.map((m, i) => cat(m, label(m), hsl(hueOf(i * 3 + 1)))), ...(cnt.size > top.length ? [cat('other', `${cnt.size - top.length} smaller lanes`, GREY)] : [])],
                r => (keep.has(R.mask[r]) ? String(R.mask[r]) : 'other'), r => String(R.mask[r]));
        }
        // deltas: the node a row is born in
        return done(doc.nodes.map((x, i) => cat(x.id, `${x.root ? 'In the root, ' : 'Born in '}${x.file_name || x.id}`, nodeColor(doc, i))), r => (R.bornIn[r] < 0 ? '' : String(doc.nodes[R.bornIn[r]].id)));
    }

    // Rows held by each visible node; an aggregate holds what any of its files holds.
    function attachRows(M, R) {
        for (const v of M.vis) {
            if (!v.agg) { const i = v.idx; v.rows = R.held[i]; v.has = r => R.has(r, i) === 1; continue; }
            const mark = new Uint8Array(R.n);
            for (const m of v.members) for (const r of R.held[M.byId.get(m).idx]) mark[r] = 1;
            v.rows = [];
            mark.forEach((x, r) => { if (x) v.rows.push(r); });
            v.has = r => mark[r] === 1;
        }
    }

    // Node bodies cut by the lens (matching rows, then the rest faint), ribbons per group from parent to child.
    function lensPaint(M, L, R, C) {
        const pos = new Map(C.cats.map((c, i) => [c.key, i])), K = C.cats.length;
        const segs = new Map(), links = [];
        for (const v of L.vis) {
            const cnt = new Array(K).fill(0);
            let faint = 0;
            for (const r of v.rows) if (R.match(r)) cnt[pos.get(C.of(r))]++; else faint++;
            const unit = v.h / Math.max(1, v.rows.length), m = new Map();
            let y = v.y0;
            cnt.forEach((c, i) => { if (c) { m.set(C.cats[i].key, { cat: C.cats[i], y0: y, h: c * unit, cnt: c }); y += c * unit; } });
            segs.set(v.id, { m, unit, faint: { y0: y, h: faint * unit, cnt: faint }, shown: v.rows.length - faint });
        }
        const kidsOf = new Map();
        for (const v of L.vis) { const p = L.parentOf.get(v.id); if (p) (kidsOf.get(p) || kidsOf.set(p, []).get(p)).push(v); }
        for (const [pid, kids] of kidsOf) {
            const p = L.fileOf.get(pid), sp = segs.get(pid);
            const share = kids.map(c => { const s = new Array(K).fill(0); for (const r of c.rows) if (R.match(r) && p.has(r)) s[pos.get(C.of(r))]++; return s; });
            C.cats.forEach((cat, k) => {
                const seg = sp.m.get(cat.key), tot = share.reduce((a, s) => a + s[k], 0);
                if (!seg || !tot) return;
                // the parent side squeezes when its children together keep more than it shows
                const sc = Math.min(1, seg.cnt / tot);
                let off = seg.y0;
                kids.forEach((c, i) => {
                    const v = share[i][k];
                    if (!v) return;
                    const sc2 = segs.get(c.id), cy = sc2.m.get(cat.key).y0, hc = v * sc2.unit, hp = v * sp.unit * sc;
                    links.push({ cat, value: v, source: p, target: c, edge: M.inE.get(c.agg ? c.top : c.id), d: ribbon(p.x1, off, hp, c.x0, cy, hc), bb: [p.x1, Math.min(off, cy), c.x0, Math.max(off + hp, cy + hc)], fade: kids.length > FAN_FADE });
                    off += hp;
                });
            });
        }
        return { segs, links };
    }
    function ribbon(x0, y0, h0, x1, y1, h1) {
        const m = (x0 + x1) / 2;
        return `M${x0},${y0}C${m},${y0} ${m},${y1} ${x1},${y1}L${x1},${y1 + h1}C${m},${y1 + h1} ${m},${y0 + h0} ${x0},${y0 + h0}Z`;
    }

    // Rows a `track` value names: fn:<fid> (any fid of the row), <lens>:<key>, or a group id of the
    // current `lens`'s server groups (tier:core, fold:..). null when it names nothing.
    function trackRows(R, catsOf, track, lens) {
        if (!R || !track) return null;
        const mark = new Uint8Array(R.n);
        const i = track.indexOf(':'), kind = track.slice(0, i);
        let key = track.slice(i + 1);
        if (kind === 'fn') {
            for (let r = 0; r < R.n; r++) if (R.fid[r] === key || (R.fids && (R.fids[r] || []).includes(key))) mark[r] = 1;
        } else {
            const isLens = LENSES.some(l => l[0] === kind);
            if (!isLens && !lens) return null;
            const C = catsOf(isLens ? kind : lens);
            if (C.track) { const m = C.track(track); if (m || !isLens) return m; }
            if (kind === 'deltas') key = R.nodeOf.get(key) || key;
            // of(r) too: the lanes lens's "other" bucket has no raw key of its own
            for (let r = 0; r < R.n; r++) if (C.raw(r) === key || C.of(r) === key) mark[r] = 1;
        }
        return mark;
    }

    // Per visible node: tracked rows it holds (tk), born here (not in the parent), lost from the parent,
    // and reborn: rows born here that are also born in another node (the root counts as a birth place):
    // reuse or independent birth.
    function trackInfo(L, R, mark) {
        const info = new Map(), births = new Uint16Array(R.n), bornAt = new Map();
        let holders = 0, max = 0;
        for (const v of L.vis) {
            const p = L.fileOf.get(L.parentOf.get(v.id)), here = [];
            let tk = 0, lost = 0;
            for (const r of v.rows) if (mark[r] && R.match(r)) { tk++; if (!p || !p.has(r)) { births[r]++; if (p) here.push(r); } }
            if (p) for (const r of p.rows) if (mark[r] && R.match(r) && !v.has(r)) lost++;
            if (tk) holders++;
            max = Math.max(max, tk);
            bornAt.set(v.id, here);
            info.set(v.id, { tk, born: here.length, lost, reborn: 0 });
        }
        for (const [id, here] of bornAt) info.get(id).reborn = here.filter(r => births[r] > 1).length;
        return { info, holders, max };
    }

    // ---------------------------------------------------------------- layout
    function layout(doc, M) {
        const { vis, rep, kids, inE } = M;
        const fileOf = new Map(vis.map(n => [n.id, n]));
        const visPos = new Map(vis.map((n, i) => [n.id, i]));
        const repOf = id => fileOf.get(rep.get(id));
        const maxd = Math.max(...vis.map(n => n.d));
        const colSum = new Map();
        for (const n of vis) colSum.set(n.d, (colSum.get(n.d) || 0) + (n.agg ? 40 / 0.5 : Math.max(1, n.functions)));
        const ky = Math.max(KY_MIN, Math.min(KY_MAX, TARGET_H / Math.max(...colSum.values())));
        for (const n of vis) n.h = n.agg ? Math.max(30, Math.min(260, n.functions * ky)) : Math.max(3, n.functions * ky);
        const x0Of = d => PAD + d * (NODE_W + GAP);
        const parentOf = new Map();
        for (const n of vis) if (!n.agg || true) { const e = inE.get(n.agg ? n.top : n.id); if (e) parentOf.set(n.id, rep.get(e.parent)); }

        // reuse classes mapped onto the visible nodes
        const classes = [];
        if (!doc.reuse_skipped) {
            for (const r of (doc.reuse || []).filter(x => !x.folded)) {
                const comps = [];
                for (const c of r.components) {
                    const s = [...new Set(c.map(i => rep.get(i)))];
                    const hit = comps.filter(o => o.some(x => s.includes(x)));
                    if (hit.length) { for (const o of hit) { comps.splice(comps.indexOf(o), 1); s.push(...o); } }
                    comps.push([...new Set(s)]);
                }
                if (comps.length < 2) continue;
                classes.push({ r, comps, files: [...new Set(comps.flat())] });
            }
        }
        const maxC = Math.max(1, ...classes.map(c => c.r.count));
        const kr = Math.min(ky, REUSE_MAX / maxC);
        classes.forEach((c, i) => { c.rank = i; c.th = Math.max(2, c.r.count * kr); });
        const laneH = Math.max(4, ...classes.map(c => c.th)) + 7;

        const place = top => {
            const bottom = new Map();
            let fb = top;
            for (const n of vis) {
                n.x0 = x0Of(n.d); n.x1 = n.x0 + NODE_W;
                const pb = bottom.has(n.d) ? bottom.get(n.d) + NPAD : top;
                n.y0 = Math.max(pb, fileOf.get(parentOf.get(n.id))?.y0 ?? top);
                n.y1 = n.y0 + n.h;
                bottom.set(n.d, n.y1);
                fb = Math.max(fb, n.y1);
            }
            return fb;
        };
        let top = 46, filesBottom = place(top);
        const stripes = new Map(vis.map(n => [n.id, []]));
        for (const c of classes) for (const id of c.files) stripes.get(id).push({ c, n: fileOf.get(id) });
        const restack = () => {
            for (const list of stripes.values()) {
                let cum = 0;
                for (const s of list) { s.y0 = s.n.y0 + cum; s.y1 = s.y0 + s.c.th; cum += s.c.th; }
            }
        };
        restack();
        const stripeOf = (c, id) => stripes.get(id).find(s => s.c === c);

        const arcs = [], slotN = new Map(), used = new Map();
        for (const c of classes) {
            if (c.comps.length > ARC_MAX_PIECES) continue; // broadly shared code: stripes only
            const anchors = c.comps.map(cp => cp.map(id => fileOf.get(id)).sort((a, b) => a.d - b.d || a.idx - b.idx)[0]).sort((a, b) => a.d - b.d || a.y0 - b.y0);
            for (let i = 1; i < anchors.length; i++) {
                const a = anchors[i - 1], b = anchors[i];
                arcs.push({ c, r: c.r, a, b, same: a.d === b.d });
                slotN.set(a.d, (slotN.get(a.d) || 0) + 1);
                if (a.d !== b.d) slotN.set(b.d, (slotN.get(b.d) || 0) + 1);
            }
        }
        const take = d => {
            const n = used.get(d) || 0;
            used.set(d, n + 1);
            return 22 + n * Math.min(laneH, (GAP - 70) / Math.max(1, slotN.get(d)));
        };
        for (const A of arcs) { A.sa = take(A.a.d); A.sb = A.same ? A.sa : take(A.b.d); }
        const laneEnd = [];
        const cross = arcs.filter(A => !A.same).sort((a, b) => a.a.x1 + a.sa - (b.a.x1 + b.sa));
        for (const A of cross) {
            const l0 = A.a.x1 + A.sa - 8;
            let k = laneEnd.findIndex(e => e < l0);
            if (k < 0) k = laneEnd.length;
            laneEnd[k] = A.b.x1 + A.sb + 8;
            A.lane = k;
        }
        if (laneEnd.length) { top = 46 + laneEnd.length * laneH + 18; filesBottom = place(top); restack(); }
        for (const A of arcs) {
            A.ya = stripeOf(A.c, A.a.id).y0 + A.c.th / 2;
            A.yb = stripeOf(A.c, A.b.id).y0 + A.c.th / 2;
            A.yT = top - 44 - (A.lane || 0) * laneH;
            A.d = arcPath(A);
        }

        // body segments of real parents; aggregates get one kept + one died segment
        const partsOf = new Map();
        for (const p of doc.partitions || []) (partsOf.get(p.parent) || partsOf.set(p.parent, []).get(p.parent)).push(p);
        const segs = [];
        for (const n of vis) {
            if (n.agg) {
                const dh = n.functions ? n.h * n.died_all / n.functions : 0;
                segs.push({ n, agg: true, y0: n.y0, y1: n.y1 - dh });
                if (dh > 0) segs.push({ n, died: true, count: n.died_all, y0: n.y1 - dh, y1: n.y1 });
                continue;
            }
            const ps = partsOf.get(n.id);
            if (!ps) continue;
            const pos = c => visPos.get(rep.get(c));
            const cmp = (a, b) => {
                const x = a.children.map(pos), y = b.children.map(pos);
                for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i] - y[i];
                return x.length - y.length;
            };
            let y = n.y0;
            for (const p of [...ps].sort((a, b) => (a.folded ? 1 : 0) - (b.folded ? 1 : 0) || cmp(a, b))) { segs.push({ n, part: p, y0: y, y1: y + p.count * ky }); y += p.count * ky; }
            if (n.died_all > 0) segs.push({ n, died: true, count: n.died_all, y0: y, y1: y + n.died_all * ky });
        }

        // origin strip: born (blue, hatched = suspect) over inherited
        const origs = [];
        for (const n of vis) {
            const e = n.agg ? null : inE.get(n.id);
            const born = n.agg ? n.born : e ? e.gained : 0, sus = Math.min(born, n.agg ? n.suspect : e ? e.recall_suspect : 0);
            const bh = n.agg ? Math.min(n.h, n.h * born / Math.max(1, n.functions)) : Math.min(n.h, born * ky);
            const sh = born ? bh * sus / born : 0;
            if (born > 0) {
                origs.push({ n, kind: 'born', edge: e, count: born - sus, y0: n.y0, y1: n.y0 + bh - sh });
                if (sus > 0) origs.push({ n, kind: 'born', hatch: true, edge: e, count: sus, y0: n.y0 + bh - sh, y1: n.y0 + bh });
            }
            if (n.y0 + bh < n.y1) origs.push({ n, kind: 'inh', edge: e, count: n.functions - born, y0: n.y0 + bh, y1: n.y1 });
        }

        // shared ribbons
        const links = [], cursor = new Map(vis.map(n => [n.id, n.y0 + (n.agg ? 0 : (inE.get(n.id)?.gained || 0) * ky)]));
        const edgeOf = new Map(doc.edges.map(e => [`${e.parent}|${e.child}`, e]));
        for (const sg of segs) {
            if (!sg.part) continue;
            for (const cid of sg.part.children) {
                const t = repOf(cid);
                const v = sg.part.per_child ? sg.part.per_child[cid] : sg.part.count;
                const w = Math.min(Math.max(1, v * ky), t.agg ? t.h : 1e9);
                links.push({
                    kind: 'shared', edge: edgeOf.get(`${sg.n.id}|${cid}`), part: sg.part, value: v, before: 0, width: w, bf: sg.part.by_file,
                    source: sg.n, target: t, y0: sg.y0 + w / 2, y1: cursor.get(t.id) + w / 2, overlap: sg.part.children.length > 1,
                    fade: (kids.get(sg.n.id) || []).length > FAN_FADE,
                });
                cursor.set(t.id, cursor.get(t.id) + (t.agg ? 0 : v * ky));
            }
        }
        const W = x0Of(maxd) + NODE_W + 90, H = filesBottom + 30;
        return { vis, fileOf, parentOf, links, segs, origs, arcs, classes, stripes, W, H, ky, top };
    }

    function arcPath(A) {
        const xa = A.a.x1, xb = A.b.x1, rr = 12;
        if (A.same) {
            const dir = Math.sign(A.yb - A.ya) || 1, r = Math.max(1, Math.min(rr, Math.abs(A.yb - A.ya) / 2));
            const x = xa + A.sa;
            return `M${xa},${A.ya}H${x - r}Q${x},${A.ya} ${x},${A.ya + dir * r}V${A.yb - dir * r}Q${x},${A.yb} ${x - r},${A.yb}H${xa}`;
        }
        const x1 = xa + A.sa, x2 = xb + A.sb, t = A.yT;
        return `M${xa},${A.ya}H${x1 - rr}Q${x1},${A.ya} ${x1},${A.ya - rr}V${t + rr}Q${x1},${t} ${x1 + rr},${t}H${x2 - rr}Q${x2},${t} ${x2},${t + rr}V${A.yb - rr}Q${x2},${A.yb} ${x2 - rr},${A.yb}H${xb}`;
    }

    // ---- ribbon geometry: one centre cubic per bundle, offset strips of constant width ----
    function sampleCurve(x0, x1, yc0, yc1) {
        const n = Math.max(24, Math.min(72, Math.round((x1 - x0 + Math.abs(yc1 - yc0)) / 16)));
        const m = (x0 + x1) / 2, h = x1 - m, dy = yc1 - yc0;
        const pts = [];
        let kmax = 0;
        for (let i = 0; i <= n; i++) {
            const t = i / n, u = 1 - t;
            const x = u * u * u * x0 + 3 * u * u * t * m + 3 * u * t * t * m + t * t * t * x1;
            const y = u * u * u * yc0 + 3 * u * u * t * yc0 + 3 * u * t * t * yc1 + t * t * t * yc1;
            const vx = 3 * (u * u * (m - x0) + t * t * (x1 - m)), vy = 6 * u * t * dy;
            const ax = 6 * h * (t - u), ay = 6 * dy * (u - t);
            const len = Math.hypot(vx, vy) || 1;
            kmax = Math.max(kmax, Math.abs(vx * ay - vy * ax) / (len * len * len));
            pts.push([x, y, -vy / len, vx / len]);
        }
        return { pts, kmax };
    }
    function strip(pts, o, w) {
        const a = pts.map(([x, y, nx, ny]) => `${(x + nx * o).toFixed(1)},${(y + ny * o).toFixed(1)}`);
        const b = pts.map(([x, y, nx, ny]) => `${(x + nx * (o + w)).toFixed(1)},${(y + ny * (o + w)).toFixed(1)}`).reverse();
        return `M${a.join('L')}L${b.join('L')}Z`;
    }
    function band(x0, x1, y0, y1, w) {
        const m = (x0 + x1) / 2, a = y0 - w / 2, b = y1 - w / 2;
        return `M${x0},${a}C${m},${a} ${m},${b} ${x1},${b}L${x1},${b + w}C${m},${b + w} ${m},${a + w} ${x0},${a + w}Z`;
    }
    function bundle(links) {
        const groups = new Map();
        for (const l of links) {
            const k = groups.get(l.source) || groups.set(l.source, new Map()).get(l.source);
            (k.get(l.target) || k.set(l.target, []).get(l.target)).push(l);
        }
        for (const byTarget of groups.values()) for (const ls of byTarget.values()) {
            ls.sort((a, b) => a.y0 - b.y0);
            const W = ls.reduce((a, l) => a + l.width, 0);
            let ok = true;
            for (let i = 1; i < ls.length; i++) {
                const p = ls[i - 1], c = ls[i];
                if (Math.abs((c.y0 - c.width / 2) - (p.y0 + p.width / 2)) > 1.5 || Math.abs((c.y1 - c.width / 2) - (p.y1 + p.width / 2)) > 1.5) ok = false;
            }
            const x0 = ls[0].source.x1, x1 = ls[0].target.x0;
            const yc0 = ls[0].y0 - ls[0].width / 2 + W / 2, yc1 = ls[0].y1 - ls[0].width / 2 + W / 2;
            const curve = ok ? sampleCurve(x0, x1, yc0, yc1) : null;
            ok = ok && curve.kmax * (W / 2) < 0.85;
            let cum = 0;
            for (const l of ls) {
                l.pts = ok ? curve.pts : null;
                l.o = cum - W / 2;
                l.d = ok ? strip(curve.pts, l.o, l.width) : band(x0, x1, l.y0, l.y1, l.width);
                cum += l.width;
                l.bb = [x0, Math.min(l.y0, l.y1) - W, x1, Math.max(l.y0, l.y1) + W];
            }
        }
    }

    // dev guard: boxes of one column never intersect; warn once with the two names
    let warned = false;
    function warnOverlap(L) {
        if (warned) return;
        const cols = new Map();
        for (const n of L.vis) (cols.get(n.d) || cols.set(n.d, []).get(n.d)).push(n);
        for (const list of cols.values()) {
            list.sort((a, b) => a.y0 - b.y0);
            for (let i = 1; i < list.length; i++) {
                if (list[i].y0 < list[i - 1].y1) {
                    warned = true;
                    console.warn('lineage: overlapping boxes', list[i - 1].file_name || list[i - 1].id, list[i].file_name || list[i].id);
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------- mount
    function ensureCss() {
        if (document.getElementById('nway-lineage-css')) return;
        const css = el('style', { id: 'nway-lineage-css' });
        css.textContent = CSS;
        document.head.append(css);
    }

    function mount(container, opts = {}) {
        ensureCss();
        uid++;
        const id = uid;
        const root = el('div', { class: 'lin-root' });
        const tb = el('div', { class: 'lin-tb' });
        const canvas = el('div', { class: 'lin-canvas' });
        const drawer = el('div', { class: 'lin-drawer' });
        const noteBox = el('div', { class: 'lin-note', style: 'display:none' });
        const mini = el('div', { class: 'lin-mini' });
        // Tracks view: the lens's groups on the left, the tree (or six mini trees) for the selected one
        const body = el('div', { class: 'lin-body' }), side = el('nav', { class: 'lin-glist', 'aria-label': 'Groups' }), grid = el('div', { class: 'lin-multi' });
        side.hidden = grid.hidden = true;
        body.append(side, canvas, grid);
        root.append(tb, body);
        container.replaceChildren(root);

        const st = { doc: null, M: null, L: null, collapsed: new Set(), reuseOn: true, sel: null, last: null, t: d3.zoomIdentity, hasView: false, hl: null, hover: 0, raf: 0, bfCache: new Map(), dead: false,
            lens: opts.lens || 'modules', track: opts.track || '', R: null, C: new Map(), P: null, T: null, dockTab: 'rows', dockFn: null, dockOpen: new Set(), view: opts.view || 'tree', multi: false, ringList: !!(opts.ring && opts.ring.list),
            // the model's group names (NwayGroups naming state) and one-line function summaries by fid
            S: { names: {}, evid: {}, busy: false, done: false, off: opts.laneNames === false }, lnSig: '', sums: new Map() };
        const s = d3.select(canvas).append('svg').attr('role', 'img').attr('aria-label', 'Lineage of the files: shared, born, died and reused code');
        const defs = s.append('defs');
        const world = s.append('g'), gTree = world.append('g'), gRingG = world.append('g').style('display', 'none');
        const gRib = gTree.append('g'), gArc = gTree.append('g'), gNode = gTree.append('g'), gDet = gTree.append('g'), gStr = gTree.append('g'), gLib = gTree.append('g').style('pointer-events', 'none'), gTrk = gTree.append('g').style('pointer-events', 'none'), gOv = gTree.append('g').style('pointer-events', 'none'), gLab = gTree.append('g').style('pointer-events', 'none'), gHov = gTree.append('g').style('pointer-events', 'none');
        const zoom = d3.zoom().scaleExtent([K_MIN, K_MAX]).on('zoom', ev => { st.t = ev.transform; world.attr('transform', st.t); schedule(); });
        // in the Ring view a press on a draggable shape drags it; the rest still pans
        zoom.filter(ev => (!ev.ctrlKey || ev.type === 'wheel') && !ev.button && !(st.view === 'ring' && ev.type !== 'wheel' && ev.target.closest && ev.target.closest('[data-drag]')));
        s.call(zoom);
        const tip = d3.select(document.body).append('div').attr('class', 'lin-vars lin-tip');
        canvas.append(noteBox, mini, drawer);

        // ---- toolbar ----
        for (const e of opts.toolbar || []) tb.append(e);
        const search = el('input', { type: 'search', placeholder: 'Find file or md5', 'aria-label': 'Find a file' });
        const hits = el('span', { class: 'lin-dim' });
        const btn = (txt, title, f, cls = 'lin-btn') => { const b = el('button', { type: 'button', class: cls, title }, txt); b.addEventListener('click', f); return b; };
        const reuseBtn = btn('Reuse layer', 'Show or hide the reuse arcs', () => { st.reuseOn = !st.reuseOn; reuseBtn.setAttribute('aria-pressed', String(st.reuseOn)); applyReuse(); });
        reuseBtn.setAttribute('aria-pressed', 'true');
        const zgrp = el('div', { class: 'lin-seg', role: 'group', 'aria-label': 'Zoom' });
        const zbtn = (txt, title, f) => { const b = el('button', { type: 'button', title, 'aria-label': title }, txt); b.addEventListener('click', f); zgrp.append(b); };
        zbtn('−', 'Zoom out', () => s.transition().duration(200).call(zoom.scaleBy, 1 / 1.5));
        zbtn('+', 'Zoom in', () => s.transition().duration(200).call(zoom.scaleBy, 1.5));
        zbtn('Fit', 'Fit the whole chart', () => fit(true));
        zbtn('100%', 'Actual size', () => s.transition().duration(250).call(zoom.scaleTo, 1));
        zbtn('Selection', 'Zoom to the selected or last hovered file, edge or reuse arc', () => zoomToSel());
        // chart-only tools: one popover (as the table's Filter), zoom floating over the canvas
        const chartPop = el('details', { class: 'nway-pop' }), chartBody = el('div', { class: 'nway-match-body' });
        const chartSum = el('summary', { class: 'nway-chip', title: 'Collapse or expand the tree, show the reuse layer' });
        chartSum.append(el('i', { class: 'fa-solid fa-diagram-project' }), document.createTextNode(' Chart'));
        chartBody.append(btn('Collapse deep', 'Group deep subtrees into one node each', () => { st.collapsed = autoCollapse(st.doc); if (!st.collapsed.size) st.collapsed = deepAll(); rebuild(); }),
            btn('Expand all', 'Show every file', () => { st.collapsed = new Set(); rebuild(); }), reuseBtn);
        chartPop.append(chartSum, chartBody);
        zgrp.classList.add('lin-zoom');
        canvas.append(zgrp);
        tb.append(search, hits, chartPop);
        const trackBox = el('span', { style: 'display:contents' }), legendBox = el('span', { style: 'display:contents' });
        // the Ring's controls sit in the same flex row as the others; its "More" row wraps below
        const ringBox = el('span', { style: 'display:none' }), ringBox2 = el('div', { style: 'display:none' });
        tb.append(trackBox, legendBox, ringBox, ringBox2);
        const deepAll = () => {
            const out = new Set();
            for (const n of st.M.vis) if (!n.agg && n.d >= 2 && (st.M.kids.get(n.id) || []).length && !out.has(st.M.rep.get(n.id))) out.add(n.id);
            return out;
        };

        // ---- tooltip / drawer ----
        const showTip = (ev, title, dot, rows) => {
            tip.selectAll('*').remove();
            const t = tip.append('div').attr('class', 't');
            t.append('span').attr('class', 'lin-dot').style('background', dot);
            t.append('span').text(title);
            const kv = tip.append('div').attr('class', 'lin-kv');
            for (const [k, v] of rows) { kv.append('span').attr('class', 'k').text(k); kv.append('span').text(v); }
            tip.style('display', 'block');
            moveTip(ev);
        };
        const moveTip = ev => {
            const w = tip.node().offsetWidth || 240;
            tip.style('left', `${Math.min(ev.clientX + 14, window.innerWidth - w - 8)}px`).style('top', `${ev.clientY + 10}px`);
        };
        const hideTip = () => tip.style('display', 'none');
        const name = id => st.names.get(id) || id;
        const names = ids => ids.slice(0, 8).map(name).join(', ') + (ids.length > 8 ? ` +${ids.length - 8} more` : '');

        const closeDock = () => drawer.classList.remove('open');
        const closeBtn = () => { const x = el('button', { class: 'lin-x', type: 'button', 'aria-label': 'Close' }, '\u00d7'); x.addEventListener('click', closeDock); return x; };
        const openDrawer = pick => {
            drawer.classList.add('open');
            if (pick.node && st.R) dock(pick.node);
            else if (pick.group) { drawer.replaceChildren(closeBtn()); const tg = trackedGroup(); if (tg) drawer.append(groupCard(tg.x, tg.C)); }
            else detail(drawer, pick, name, closeDock);
        };
        const lensOn = () => !!st.R && st.lens !== 'deltas' && LENSES.some(l => l[0] === st.lens);
        const cats = lens => st.C.get(lens) || st.C.set(lens, lensCats(st.doc, st.R, lens, gText)).get(lens);
        const NG = window.NwayGroups;
        // a server group as plain text: its label, the model's name, or what it is made of
        function gText(g) {
            if (g.kind === 'fold') return `${g.lanes} small lanes`;
            if (g.label) return g.label;
            const nm = NG.shownName(st.S.names[g.id]);
            if (nm) return nm;
            if (st.S.busy) return 'naming\u2026';
            if (st.S.done && !st.S.off) return NG.laneFallback(g, st.S);
            if (g.kind === 'lane') return `${g.core ? `${g.lanes} lanes, ` : ''}${g.files}-file lane${g.hint ? ` ${g.hint}` : ''}`;
            return g.hint || (g.sample || []).slice(0, 2).join(', ') || 'Region';
        }
        // as the table's header names it: icon, label, the model's name (shimmer while it works) or its fallback
        function gHtml(g) {
            const icon = g.kind === 'role' ? NG.roleIcon(g.role) : NG.bandIcon(g);
            const nm = NG.shownName(st.S.names[g.id]), named = g.kind === 'lane' || (g.kind === 'region' && !g.label) || NG.nameable(g);
            if (!named || g.kind === 'fold') return `${icon}<b>${escapeHtml(gText(g))}</b>`;
            const fb = st.S.off ? (g.label ? '' : gText(g)) : NG.laneFallback(g, st.S);
            const name = nm ? `<span class="nway-lname">${escapeHtml(nm)}</span>` : st.S.busy ? NG.laneName(g, st.S) : fb ? `<span class="nway-lname fb">${escapeHtml(fb)}</span>` : '';
            return g.label ? `${icon}<b>${escapeHtml(g.label)}</b> <span class="nway-gsum">${name}</span>` : icon + name;
        }
        // the header's numbers: functions, files, span and features, support and cohesion (html), then the role mix
        function gStats(g) {
            const x = NG.stats(g), units = Number(g.units ?? g.rows);
            return `<span>${units} function${units === 1 ? '' : 's'} \u00b7 ${Number(g.files)} file${g.files === 1 ? '' : 's'}${x.span ? ` \u00b7 span ${x.span}` : ''} \u00b7 ${x.feat} feat${x.support ? ` \u00b7 sup ${x.support}` : ''}${x.cohesion ? ` \u00b7 coh ${x.cohesion}` : ''}</span>${g.roles ? NG.roleMix(g.roles) : ''}`;
        }
        // the same as text lines, for a tooltip
        function gTip(g) {
            const units = Number(g.units ?? g.rows), ev = (st.S.evid[g.id] || {});
            const roles = ROLES.filter(([k]) => (g.roles || {})[k]).map(([k, l]) => `${l} ${g.roles[k]}`).join(' \u00b7 ');
            const evid = [g.hint, ...NG.laneTags(g, st.S), ...(ev.strings || []).map(x => `"${x}"`)].filter(Boolean).join(' \u00b7 ');
            return [['Group', `${fmt(units)} functions in ${fmt(g.files)} files`], ...(g.span_lo != null ? [['Span', g.span_lo === g.span_hi ? `${g.span_lo} files` : `${g.span_lo}\u2013${g.span_hi} files`]] : []),
                ['Features', fmt(Math.round(g.weight || 0))], ...(roles ? [['Roles', roles]] : []), ...(evid ? [['Evidence', evid]] : [])];
        }
        // the server group a track value names, with its lens cats (null for fn: or an old-style key)
        function trackedGroup(t = st.track) {
            if (!t || t.startsWith('fn:') || !st.R) return null;
            const kind = t.slice(0, t.indexOf(':')), C = cats(LENSES.some(l => l[0] === kind) ? kind : st.lens);
            const x = C.groups && C.groups.find(y => y.tk === t);
            return x ? { C, x } : null;
        }
        const twinsOf = n => (!n.agg && (n.members || []).length > 1 ? n.members : null);
        const md5Of = id => String(id).split(':').pop();
        // st.fk: the lens group whose lineage is lit, a hovered one first, else the tracked one (null: none)
        const syncFocus = () => { const t = st.hoverTk || st.track; st.fk = t && st.P && st.P.links.some(l => l.cat.tk === t) ? t : null; };
        const holds = n => { const sp = st.P && st.P.segs.get(n.id); return !!sp && [...sp.m.values()].some(x => x.cat.tk === st.fk); };
        const nodeOp = n => (st.hoverTk && st.fk ? (holds(n) ? 1 : 0.3) : st.T && !(st.T.info.get(n.id) || {}).tk ? 0.3 : 1);
        const ribOp = l => (st.fk ? (l.cat.tk === st.fk ? 1 : 0.12) : 1) * (l.fade && !st.fk ? 0.28 : 1) * (st.T && !(st.T.info.get(l.target.id) || {}).tk ? 0.3 : 1);
        // the lens group a reuse class mostly is: the group of the rows held by exactly its files (null: grey or none)
        function reuseCat(r) {
            if (!st.P || !st.R || r.folded) return null;
            const m = st.P.rc || (st.P.rc = new Map());
            if (!m.has(r)) {
                const C = cats(st.lens), ix = r.files.map(f => st.M.byId.get(f).idx), cnt = new Map();
                for (const row of st.R.held[ix[0]]) if (st.R.pop[row] === ix.length && st.R.match(row) && ix.every(i => st.R.has(row, i))) cnt.set(C.of(row), (cnt.get(C.of(row)) || 0) + 1);
                const k = [...cnt].sort((a, b) => b[1] - a[1])[0], c = k && C.cats.find(x => x.key === k[0]);
                m.set(r, c && c.color !== GREY ? c : null);
            }
            return m.get(r);
        }
        // light one group's lineage (null clears) through the same opacity helpers tracking uses
        function focusHover(tk) {
            st.hoverTk = tk; syncFocus();
            ribs.style('opacity', ribOp); nodeBase.style('opacity', nodeOp);
            const lit = r => { const c = reuseCat(r); return !tk || (c && c.tk === tk); };
            strSel.style('opacity', d => (lit(d.c.r) ? 1 : 0.3));
            arcSel.style('opacity', A => (lit(A.r) ? 1 : 0.15));
            applySel();
        }

        // ---- build ----
        function rebuild() {
            const M = st.M = buildModel(st.doc, st.collapsed);
            const L = st.L = layout(st.doc, M);
            bundle(L.links);
            if (st.doc.rows && !st.R) st.R = rowModel(st.doc, M);
            if (st.R) attachRows(M, st.R);
            st.P = lensOn() ? lensPaint(M, L, st.R, cats(st.lens)) : null;
            st.names = new Map(st.doc.nodes.map(n => [n.id, n.file_name || n.id]));
            for (const n of L.vis) if (n.agg) st.names.set(n.id, n.file_name);
            warnOverlap(L);
            drawStatic();
            drawMini();
            note();
            applyTrack(false);
            ringDraw();
            if (!st.hasView) fit(false);
            schedule(true);
            // a node pick survives a relayout when the node is still drawn
            const keep = st.sel && st.sel.node && L.fileOf.get(st.sel.node.id);
            if (keep) { st.sel = { node: keep }; if (drawer.classList.contains('open')) openDrawer(st.sel); }
            else if (st.sel && st.sel.node) { st.sel = null; closeDock(); }
            applySel();
        }
        function note() {
            noteBox.replaceChildren();
            const n = st.collapsed.size;
            noteBox.style.display = n ? '' : 'none';
            if (!n) return;
            noteBox.append(document.createTextNode(`${n} subtree${n > 1 ? 's' : ''} grouped (${st.doc.nodes.length} files in all). Click a grouped node to expand it. `));
            noteBox.append(btn('Expand all', 'Show every file', () => { st.collapsed = new Set(); rebuild(); }, ''));
        }
        function legend() {
            const doc = st.doc;
            legendBox.replaceChildren();
            if (st.P) { lensLegend(); return; }
            const chip = (color, text, mode) => {
                const c = el('span', { class: 'lin-chip' }), sw = el('span', { class: 'lin-sw' });
                sw.style.background = mode === 'hatch' ? `repeating-linear-gradient(45deg,${color} 0 2px,transparent 2px 4px)` : mode === 'dash' ? `repeating-linear-gradient(90deg,${color} 0 4px,transparent 4px 7px)` : color;
                c.append(sw, el('span', {}, text));
                legendBox.append(c);
            };
            const born = doc.edges.reduce((a, e) => a + e.gained, 0), died = doc.nodes.reduce((a, n) => a + (n.died_all || 0), 0);
            chip(V.shared, 'shared'); chip(V.fork, 'kept by several children');
            chip(V.born, `born ${fmt(born)} fn`); chip(V.born, 'hatched: likely matcher miss', 'hatch');
            chip(V.died, `died ${fmt(died)} fn (kept by no child)`);
            if (st.R && doc.scope !== 'code' && st.R.library.some(Boolean)) chip(V.shared, 'library code (bottom of a file)', 'hatch');
            if (doc.reuse_skipped) { chip(V.reuse, 'reuse off: too many files'); return; }
            chip(V.reuse, `reused outside the tree path ${fmt(doc.reuse_total || 0)} fn`);
            chip(V.reuse, 'same level'); chip(V.reuse, 'across depth', 'dash');
            const f = (doc.reuse || []).find(r => r.folded);
            if (f) chip(V.reuse, `+${f.classes} more classes, ${fmt(f.count)} fn`);
        }

        // the active lens's groups in lens order; a click tracks one through the tree
        function lensLegend() {
            const C = cats(st.lens), R = st.R, tot = new Map(C.cats.map(c => [c.key, 0]));
            let faint = 0;
            for (let r = 0; r < R.n; r++) if (R.match(r)) tot.set(C.of(r), tot.get(C.of(r)) + 1); else faint++;
            const list = C.cats.filter(c => tot.get(c.key));
            for (const c of list.slice(0, LEGEND_MAX)) {
                const tk = c.tk, b = el('button', { type: 'button', class: `lin-chip${st.track === tk ? ' on' : ''}`, title: c.g ? `${c.label}\n${gTip(c.g).map(([k, v]) => `${k}: ${v}`).join('\n')}\nClick to track it across the tree.` : 'Track this group across the tree', 'aria-pressed': String(st.track === tk) });
                const sw = el('span', { class: 'lin-sw' }), lab = el('span', { class: 'lin-gn' });
                sw.style.background = c.color;
                if (c.g && (c.g.kind === 'tier' || c.g.kind === 'fold')) lab.textContent = c.g.kind === 'fold' ? 'slim lanes folded' : `${c.g.label || c.label}: rows in no lane`;
                else if (c.g) lab.innerHTML = gHtml(c.g); else lab.textContent = c.label;
                b.append(sw, lab, el('span', { class: 'lin-dim' }, fmt(tot.get(c.key))));
                b.addEventListener('click', () => setTrack(st.track === tk ? '' : tk));
                legendBox.append(b);
            }
            if (list.length > LEGEND_MAX) legendBox.append(el('span', { class: 'lin-chip lin-dim' }, `+${list.length - LEGEND_MAX} more groups`));
            if (faint) {
                const c = el('span', { class: 'lin-chip' }), sw = el('span', { class: 'lin-sw' });
                sw.style.cssText = 'background:var(--lin-grey);opacity:.35';
                c.append(sw, el('span', {}, `filtered out ${fmt(faint)}`));
                legendBox.append(c);
            }
        }

        // ---- tracking ----
        function setTrack(t) {
            st.track = t;
            // before the redraw: the Tracks view may pick a group inside it, and that one must win
            if (opts.onTrack) opts.onTrack(t);
            applyTrack(true);
            // a group tracked here gets its card in the dock (the Tracks view shows it beside its list)
            if (trackedGroup() && (st.view === 'tree' || st.view === 'ring') && !(st.sel && st.sel.node && drawer.classList.contains('open'))) { st.sel = { group: true }; openDrawer(st.sel); }
        }
        function trackLabel() {
            const t = st.track, i = t.indexOf(':'), kind = t.slice(0, i), key = t.slice(i + 1);
            if (kind === 'fn') {
                const r = st.R.fid.indexOf(key);
                return r >= 0 ? st.R.name[r] || key : key;
            }
            const tg = trackedGroup(), lens = LENSES.find(l => l[0] === (tg ? tg.C.lens : kind)), c = tg ? tg.x : lens && cats(kind).cats.find(x => x.key === key);
            return `${lens ? lens[1] : kind}: ${c ? c.label : key}`;
        }
        // the colour of what is tracked: its lens group's, the accent for one function
        function trackColor() {
            const t = st.track, i = t.indexOf(':'), kind = t.slice(0, i), key = t.slice(i + 1);
            const tg = trackedGroup(), c = tg ? tg.x : kind !== 'fn' && LENSES.some(l => l[0] === kind) && cats(kind).cats.find(x => x.key === (kind === 'deltas' ? st.R.nodeOf.get(key) || key : key));
            return c ? c.color : 'var(--accent,#66d9ef)';
        }
        // fade the nodes without the tracked rows, outline the holders, a bar for how much each holds,
        // +born badges (violet frame: also born elsewhere = reuse or independent birth), -lost on the edges
        function applyTrack(fromUser) {
            const mark = st.R && st.track ? trackRows(st.R, cats, st.track, st.lens) : null;
            st.T = mark ? trackInfo(st.L, st.R, mark) : null;
            syncFocus();
            gTrk.selectAll('*').remove();
            trackBox.replaceChildren();
            if (st.T) {
                const col = trackColor();
                for (const n of st.L.vis) {
                    const t = st.T.info.get(n.id), p = st.L.fileOf.get(st.L.parentOf.get(n.id));
                    if (t.lost && p) gTrk.append('text').attr('class', 'lin-halo').attr('x', (p.x1 + n.x0) / 2).attr('y', (p.y0 + p.y1 + n.y0 + n.y1) / 4).attr('text-anchor', 'middle').attr('font-size', 11).style('fill', V.died).text(`\u2212${fmt(t.lost)} lost`);
                    if (!t.tk) continue;
                    gTrk.append('rect').attr('x', n.x0 - 3).attr('y', n.y0 - 3).attr('width', NODE_W + 6).attr('height', n.h + 6).attr('rx', 7).attr('fill', 'none')
                        .style('stroke', t.reborn ? V.reuse : 'var(--accent,#66d9ef)').attr('stroke-width', t.reborn ? 2.4 : 1.6).attr('stroke-dasharray', t.reborn ? null : '4 2').attr('vector-effect', 'non-scaling-stroke');
                    gTrk.append('rect').attr('x', n.x0).attr('y', n.y1 + 5).attr('width', NODE_W * t.tk / st.T.max).attr('height', 4).attr('rx', 1.5).style('fill', col);
                    gTrk.append('text').attr('class', 'lin-halo').attr('x', n.x0).attr('y', n.y1 + 19).attr('font-size', 10.5).style('fill', 'var(--text)').text(`${fmt(t.tk)} fn`);
                    if (t.born) gTrk.append('text').attr('class', 'lin-halo').attr('x', n.x1 + 4).attr('y', n.y0 + 10).attr('font-size', 11).style('fill', t.reborn ? V.reuse : V.born).text(`+${fmt(t.born)} born${t.reborn ? `, ${fmt(t.reborn)} also born elsewhere` : ''}`);
                }
                const c = el('span', { class: 'lin-chip on' }), x = el('button', { type: 'button', class: 'lin-x2', 'aria-label': 'Stop tracking', title: 'Stop tracking' }, '\u00d7');
                x.addEventListener('click', () => setTrack(''));
                c.append(el('i', { class: 'fa-solid fa-crosshairs' }), el('span', {}, 'Tracking'), el('b', {}, trackLabel()), el('span', { class: 'lin-dim' }, `\u00b7 in ${st.T.holders}/${st.L.vis.length} nodes`), x);
                trackBox.append(c);
            } else if (st.R) trackBox.append(el('span', { class: 'lin-dim' }, 'Click a legend chip or a row in the dock to track it.'));
            if (nodeBase) { nodeBase.style('opacity', nodeOp); ribs.style('opacity', ribOp); applySel(); }
            if (st.ring) st.ring.setTrack();
            legend();
            if (groupList()) return;
            multiples();
            if (fromUser) {
                schedule(true);
                const open = drawer.classList.contains('open') && st.sel && (st.sel.node || st.sel.group);
                if (open && (st.sel.node || trackedGroup())) openDrawer(st.sel);
                else if (open) { st.sel = null; closeDock(); }
            }
        }

        // ---- tracks view: the lens's groups (matching rows), one presence square per node ----
        // server groups: every group in table order (a tier or a lane holds its sub-groups' rows too)
        function groupRows() {
            const C = cats(st.lens);
            if (C.groups) return C.groups.map(c => ({ c, rows: C.members[c.i].filter(st.R.match), tk: c.tk, leaf: !!c.direct })).filter(x => x.rows.length);
            const by = new Map(C.cats.map(c => [c.key, []]));
            for (let r = 0; r < st.R.n; r++) if (st.R.match(r)) by.get(C.of(r)).push(r);
            return C.cats.filter(c => by.get(c.key).length).map(c => ({ c, rows: by.get(c.key), tk: c.tk, leaf: true }));
        }
        // true when it picked the biggest group itself (that setTrack already redrew everything)
        function groupList() {
            const tracks = st.view === 'tracks';
            // the left pane lists the groups in every view; tree view just does not pick one for you
            side.hidden = !tracks && !st.R;
            side.replaceChildren();
            if (side.hidden) return false;
            if (!st.R) { side.append(el('div', { class: 'lin-dim' }, 'This server sends no row list: groups need it.')); return false; }
            const list = groupRows();
            if (tracks && !st.track && list.length) { setTrack(list.filter(x => x.leaf).sort((a, b) => b.rows.length - a.rows.length)[0].tk); return true; }
            if (tracks) {
                const seg = el('div', { class: 'lin-seg', role: 'group', 'aria-label': 'Group view' });
                for (const [v, t] of [[false, 'One group'], [true, 'Small multiples']]) {
                    const b = el('button', { type: 'button', 'aria-pressed': String(st.multi === v) }, t);
                    b.addEventListener('click', () => { st.multi = v; groupList(); multiples(); });
                    seg.append(b);
                }
                side.append(seg);
            }
            const tg = trackedGroup();
            if (tg) side.append(groupCard(tg.x, tg.C));
            for (const { c, rows, tk } of list) {
                // a div: the evidence line holds the call graph button
                const b = el('div', { role: 'button', tabindex: 0, class: `lin-gi${st.track === tk ? ' on' : ''}`, 'aria-pressed': String(st.track === tk), title: 'Track this group across the tree' });
                const t = el('span', { class: 't' }), sw = el('span', { class: 'lin-sw' }), pres = el('span', { class: 'lin-pres', 'aria-hidden': 'true' });
                sw.style.background = c.color;
                if (c.g) {
                    b.style.marginLeft = `${c.depth * 14}px`;
                    const nm = el('span', { class: 'lin-gn' });
                    nm.innerHTML = gHtml(c.g);
                    t.append(sw, nm, el('span', { class: 'lin-dim' }, fmt(rows.length)));
                    const ev = el('span', { class: 'lin-gev' }), sts = el('span', { class: 'lin-gst' });
                    ev.innerHTML = (c.g.kind === 'lane' || (c.g.kind === 'region' && !c.g.label) ? NG.laneEv(c.g, st.S) : NG.laneEv(c.g, st.S, false)) + (c.g.kind === 'lane' ? NG.parentChip(c.g, groupMap(), st.S) : '');
                    sts.innerHTML = gStats(c.g);
                    b.append(t, ev, sts, pres);
                } else {
                    t.append(sw, el('b', {}, c.label), el('span', { class: 'lin-dim' }, fmt(rows.length)));
                    b.append(t, pres);
                }
                for (const v of st.L.vis) {
                    const h = rows.reduce((a, r) => a + (v.has(r) ? 1 : 0), 0), i = el('i', { title: `${name(v.id)}: ${fmt(h)}` });
                    if (h) { i.style.background = nodeColor(st.doc, v.idx); i.style.opacity = (0.25 + 0.75 * h / rows.length).toFixed(2); i.style.boxShadow = 'none'; }
                    pres.append(i);
                }
                const go = ev => {
                    if (ev.target.closest('[data-lane-graph]')) return groupPivot('graph', c, ev);
                    st.multi = false;
                    setTrack(tk);
                };
                b.addEventListener('click', go);
                b.addEventListener('keydown', ev => { if (ev.target === b && (ev.key === 'Enter' || ev.key === ' ')) { ev.preventDefault(); go(ev); } });
                side.append(b);
            }
            if (!list.length) side.append(el('div', { class: 'lin-dim' }, 'No group matches the filters.'));
            return false;
        }
        const groupMap = () => Object.fromEntries((st.doc.groups || []).map(g => [g.id, g]));
        // A tracked group as its table header reads, with its heaviest functions and their one-line LLM summaries.
        function groupCard(x, C) {
            const g = x.g, card = el('section', { class: 'lin-card', 'aria-label': 'Tracked group' });
            const h = el('div', { class: 'lin-gh' }), sw = el('span', { class: 'lin-sw' }), nm = el('span', { class: 'lin-gn' });
            sw.style.background = x.color;
            nm.innerHTML = gHtml(g);
            h.append(sw, nm);
            const ev = el('div', { class: 'lin-gev' }), sts = el('div', { class: 'lin-gst' });
            ev.innerHTML = NG.laneEv(g, st.S, false) + (g.kind === 'lane' ? NG.parentChip(g, groupMap(), st.S) : '');
            sts.innerHTML = gStats(g);
            const acts = el('div', { class: 'lin-acts' });
            for (const [what, txt, icon, tip] of [['graph', 'Call graph', 'fa-diagram-project', "The N-way table with the call graph of this group's functions together"], ['table', 'Open in table', 'fa-table', 'The N-way table on the same lens, this group tracked']]) {
                const b = el('button', { type: 'button', class: 'lin-btn', title: tip });
                b.append(el('i', { class: `fa-solid ${icon}` }), document.createTextNode(` ${txt}`));
                b.addEventListener('click', e => groupPivot(what, x, e));
                acts.append(b);
            }
            card.append(h);
            if (g.tip) card.append(el('div', { class: 'lin-dim' }, g.tip));
            card.append(ev, sts, acts);
            // its heaviest matching functions, each with the first line of its LLM note when one exists
            const top = C.members[x.i].filter(st.R.match).sort((a, b) => (st.R.weight[b] || 0) - (st.R.weight[a] || 0)).slice(0, DOCK_ROWS + 1);
            if (top.length) card.append(el('div', { class: 'lin-dim', style: 'margin-top:6px' }, 'Heaviest functions'));
            const want = [];
            for (const r of top) {
                const fid = st.R.fid[r], sum = el('div', { class: 'lin-sum' }, st.sums.get(fid) || '');
                card.append(rowEl(r), sum);
                if (!st.sums.has(fid)) want.push([fid, sum]);
            }
            if (want.length) {
                fetch(`/api/bin_sim/nway/facets/summaries?fids=${encodeURIComponent(want.map(w => w[0]).join(','))}`).then(res => (res.ok ? res.json() : { summaries: {} })).then(d => {
                    for (const [fid, box] of want) { const t = (d.summaries || {})[fid] || ''; st.sums.set(fid, t); box.textContent = t; }
                }).catch(() => {});
            }
            return card;
        }
        // "Call graph" / "Open in table": the N-way table on this lens with the group tracked (and its call graph open)
        function groupPivot(what, x, ev) {
            if (opts.onGroup && opts.onGroup(what, x.tk, st.lens)) return;
            const q = tableQs(location.search), lens = LENSES.find(l => l[0] === st.lens);
            if (lens) q.set('group', lens[2]);
            q.set('track', x.tk);
            if (what === 'graph') q.set('lane_graph', x.tk);
            const url = `/diff/nway?${q}`;
            if (window.Nav) window.Nav.openPath(url, ev, { title: what === 'graph' ? 'Call graph' : 'N-way', type: 'bin_sim' });
            else location.href = url;
        }
        // the model's names for the doc's groups: the table's call and cache (NwayGroups), redrawn as they come
        async function askNames() {
            const G = st.doc.groups || [];
            const lanes = st.S.off || st.lens === 'deltas' ? [] : G.filter(NG.nameable).slice(0, 40).map(g => NG.payload(g, st.doc.nfiles, false));
            const sig = `${st.doc.group}#${lanes.map(l => `${l.id}:${l.rows}`).join('|')}`;
            if (sig === st.lnSig) return;
            st.lnSig = sig;
            st.S = { names: {}, evid: {}, busy: lanes.length > 0, done: false, off: st.S.off };
            if (!lanes.length) return;
            const status = await NG.nameLanes(lanes, () => st.lnSig === sig && !st.dead, (names, evidence) => { Object.assign(st.S.names, names); Object.assign(st.S.evid, evidence); relabel(); });
            if (st.lnSig !== sig) return;
            st.S.busy = false;
            st.S.done = true;
            if (status === 'off') st.S.off = true;
            relabel();
        }
        function relabel() {
            if (!st.L || !st.R || st.dead) return;
            applyTrack(false);
            if (st.ring && st.view === 'ring') st.ring.draw();
            if (drawer.classList.contains('open') && st.sel && (st.sel.node || st.sel.group)) openDrawer(st.sel);
        }

        // small multiples: the six biggest groups, each on the same layout (minimap style), fill = share held
        function multiples() {
            const on = st.view === 'tracks' && st.multi && !!st.R, was = canvas.style.display === 'none';
            grid.hidden = !on;
            canvas.style.display = on ? 'none' : '';
            grid.replaceChildren();
            if (!on) { if (was && st.L) fit(true); return; }
            const L = st.L;
            for (const { c, rows, tk } of groupRows().filter(x => x.leaf).sort((a, b) => b.rows.length - a.rows.length).slice(0, 6)) {
                const mark = new Uint8Array(st.R.n);
                for (const r of rows) mark[r] = 1;
                const cnt = new Map(L.vis.map(v => [v.id, v.rows.reduce((a, r) => a + mark[r], 0)])), max = Math.max(1, ...cnt.values());
                const b = el('button', { type: 'button', class: `lin-mt${st.track === tk ? ' on' : ''}`, title: 'Show this group on the tree' });
                const t = el('span', { class: 't' }), sw = el('span', { class: 'lin-sw' });
                sw.style.background = c.color;
                t.append(sw, el('span', {}, c.label), el('span', { class: 'lin-dim' }, fmt(rows.length)));
                const svg = d3.create('svg').attr('viewBox', `0 0 ${L.W} ${L.H}`).attr('role', 'img').attr('aria-label', `${c.label} across the tree`);
                for (const v of L.vis) {
                    const p = L.fileOf.get(L.parentOf.get(v.id));
                    if (!p) continue;
                    const sh = v.rows.reduce((a, r) => a + (mark[r] && p.has(r) ? 1 : 0), 0), ya = (p.y0 + p.y1) / 2, yb = (v.y0 + v.y1) / 2, xm = (p.x1 + v.x0) / 2;
                    svg.append('path').attr('d', `M${p.x1},${ya}C${xm},${ya} ${xm},${yb} ${v.x0},${yb}`).attr('fill', 'none').attr('vector-effect', 'non-scaling-stroke')
                        .style('stroke', sh ? c.color : 'var(--lin-grey-d)').attr('stroke-width', sh ? 1 + 3 * sh / max : 1).attr('stroke-dasharray', sh ? null : '3 3').attr('opacity', sh ? 0.6 : 0.5);
                }
                for (const v of L.vis) {
                    const k = cnt.get(v.id), r = svg.append('rect').attr('x', v.x0).attr('y', v.y0).attr('width', NODE_W).attr('height', Math.max(v.h, 8)).attr('rx', 6).attr('vector-effect', 'non-scaling-stroke');
                    if (k) r.style('fill', c.color).attr('fill-opacity', (0.25 + 0.75 * k / max).toFixed(2));
                    else r.attr('fill', 'none').style('stroke', 'var(--lin-line)').attr('stroke-dasharray', '3 3');
                }
                b.append(t, svg.node());
                b.addEventListener('click', () => { st.multi = false; setTrack(tk); });
                grid.append(b);
            }
        }

        // a hovered table row (cells keyed by column id, files by file id): outline the nodes holding it
        function hoverRow(row) {
            gHov.selectAll('*').remove();
            if (!row || !st.L || !st.R) return;
            const ids = new Set();
            for (const cells of [row.cells, row.files]) for (const [k, v] of Object.entries(cells || {})) if (v && (typeof v === 'string' || v.present)) ids.add(st.R.nodeOf.get(k));
            for (const id of ids) {
                const n = id && st.L.fileOf.get(st.M.rep.get(id));
                if (n) gHov.append('rect').attr('x', n.x0 - 5).attr('y', n.y0 - 5).attr('width', NODE_W + 10).attr('height', n.h + 10).attr('rx', 8).attr('fill', 'none')
                    .style('stroke', 'var(--accent,#66d9ef)').attr('stroke-width', 3).attr('vector-effect', 'non-scaling-stroke');
            }
        }

        let ribs, arcSel, nodeBase, rim, strSel;
        function drawStatic() {
            const L = st.L;
            // a relayout starts from an empty scene: joins would keep the old boxes and arcs
            for (const g of [defs, gRib, gArc, gNode, gStr, gLib, gDet, gOv, gLab, gHov]) g.selectAll('*').remove();
            syncFocus();
            for (const [k, c] of [['born', V.born]]) {
                const p = defs.append('pattern').attr('id', `hatch-${k}-${id}`).attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
                p.append('rect').attr('width', 6).attr('height', 6).style('fill', c).attr('fill-opacity', 0.22);
                p.append('rect').attr('width', 3).attr('height', 6).style('fill', c);
            }
            const ov = defs.append('pattern').attr('id', `ovl-${id}`).attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            ov.append('rect').attr('width', 6).attr('height', 6).style('fill', V.slateHi);
            ov.append('rect').attr('width', 2).attr('height', 6).attr('fill', '#fff').attr('fill-opacity', 0.28);
            for (const n of L.vis) defs.append('clipPath').attr('id', `clip-${id}-${n.idx}-${n.agg ? 'a' : 'f'}`).append('rect').attr('x', n.x0).attr('y', n.y0).attr('width', NODE_W).attr('height', n.h).attr('rx', 6);

            if (!st.P) L.links.forEach((l, i) => {
                const col = l.overlap ? V.fork : V.shared;
                const g = defs.append('linearGradient').attr('id', `g-${id}-${i}`).attr('gradientUnits', 'userSpaceOnUse').attr('x1', l.source.x1).attr('x2', l.target.x0);
                g.append('stop').attr('offset', 0).style('stop-color', col).attr('stop-opacity', 0.8);
                g.append('stop').attr('offset', 1).style('stop-color', col).attr('stop-opacity', 0.45);
                l.fill = `url(#g-${id}-${i})`; l.col = col;
            });
            ribs = st.P
                ? gRib.selectAll('path').data(st.P.links).join('path').attr('d', l => l.d).style('fill', l => l.cat.color).attr('fill-opacity', 0.55).style('opacity', ribOp).style('cursor', 'pointer')
                    .on('mouseenter', (ev, l) => hoverLens(ev, l)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', (ev, l) => pick({ edge: l.edge }))
                : gRib.selectAll('path').data(L.links).join('path').attr('d', l => l.d).style('fill', l => l.fill).style('opacity', ribOp).style('cursor', 'pointer')
                    .on('mouseenter', (ev, l) => hoverRib(ev, l)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', (ev, l) => pick({ edge: l.edge, part: l.part }));
            // in a lens a reuse stripe, and the arc joining stripes, take their group's colour and sit at that group's row in the node
            const sdata = [...L.stripes.values()].flat(), used = new Map(), at = new Map();
            for (const d of sdata) {
                d.cat = reuseCat(d.c.r);
                const sg = d.cat && st.P.segs.get(d.n.id).m.get(d.cat.key), k = d.n.id + '|' + (sg ? d.cat.key : '');
                const o = used.get(k) || 0;
                used.set(k, o + d.c.th);
                d.ys = sg ? sg.y0 + Math.min(o, Math.max(0, sg.h - d.c.th)) : d.y0;
                at.set(d.c.r, (at.get(d.c.r) || new Map()).set(d.n.id, d));
            }
            for (const A of L.arcs) {
                const a = at.get(A.r).get(A.a.id), b = at.get(A.r).get(A.b.id);
                A.cat = a.cat; A.ya = a.ys + A.c.th / 2; A.yb = b.ys + A.c.th / 2; A.d = arcPath(A);
            }
            arcSel = gArc.selectAll('g').data(L.arcs).join('g').style('cursor', 'pointer');
            arcSel.append('path').attr('d', A => A.d).attr('fill', 'none').attr('stroke', 'transparent').attr('stroke-width', A => Math.max(12, A.c.th));
            arcSel.append('path').attr('class', 'vis').attr('d', A => A.d).attr('fill', 'none').style('stroke', A => (A.cat ? A.cat.color : V.reuse)).attr('stroke-opacity', 0.8)
                .attr('stroke-width', A => A.c.th).attr('stroke-dasharray', A => A.r.same_level ? null : '9 5');
            arcSel.on('mouseenter', (ev, A) => hoverReuse(ev, A.r)).on('mousemove', moveTip).on('mouseleave', leave).on('click', (ev, A) => pick({ reuse: A.r }));

            nodeBase = gNode.selectAll('g').data(L.vis, n => n.id).join('g').attr('tabindex', 0).attr('role', 'button').style('cursor', 'pointer')
                .attr('aria-label', n => `${name(n.id)}, ${n.functions} functions`);
            nodeBase.append('rect').attr('class', 'base').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => n.h).attr('rx', 6).style('fill', st.P ? 'var(--lin-card)' : V.slate);
            // the node's column hue, as in the table
            nodeBase.append('rect').attr('x', n => n.x0).attr('y', n => n.y0 - 6).attr('width', NODE_W).attr('height', 4).attr('rx', 1.5).style('fill', n => nodeColor(st.doc, n.idx));
            if (st.P) nodeBase.each(function (n) {
                const sp = st.P.segs.get(n.id), g = d3.select(this).append('g').attr('clip-path', `url(#clip-${id}-${n.idx}-${n.agg ? 'a' : 'f'})`);
                for (const sg of sp.m.values()) {
                    if (sg.h < 0.3) continue;
                    g.append('rect').attr('x', n.x0).attr('y', sg.y0).attr('width', NODE_W).attr('height', sg.h).style('fill', sg.cat.color)
                        .on('mouseenter', ev => hoverLensSeg(ev, n, sg)).on('mousemove', moveTip).on('mouseleave', leave);
                }
                if (sp.faint.h >= 0.3) g.append('rect').attr('x', n.x0).attr('y', sp.faint.y0).attr('width', NODE_W).attr('height', sp.faint.h).style('fill', V.shared).attr('fill-opacity', 0.16)
                    .on('mouseenter', ev => showTip(ev, `${name(n.id)}: filtered out`, V.shared, [['Functions', fmt(sp.faint.cnt)], ['', 'not matching the filters']])).on('mousemove', moveTip).on('mouseleave', leave);
            });
            rim = nodeBase.append('rect').attr('class', 'rim').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => n.h).attr('rx', 6)
                .attr('fill', 'none').style('stroke', 'var(--lin-edge)').attr('vector-effect', 'non-scaling-stroke').style('pointer-events', 'none');
            drawLib();
            nodeBase.on('mouseenter', (ev, n) => hoverFile(ev, n)).on('mousemove', moveTip).on('mouseleave', leave)
                .on('click', (ev, n) => { if (n.agg) { st.collapsed.delete(n.top); rebuild(); } else pickNode(n); })
                .on('keydown', (ev, n) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); if (n.agg) { st.collapsed.delete(n.top); rebuild(); } else pickNode(n); } });
            nodeBase.append('title').text(n => n.agg ? `${n.file_name}: ${fmt(n.functions)} functions in all` : `${name(n.id)} (${fmt(n.functions)} functions)${twinsOf(n) ? `, same code in ${twinsOf(n).length} files` : ''}`);
            // reuse stripes (sdata is built above, with the arcs)
            strSel = gStr.selectAll('rect').data(sdata).join('rect').attr('x', d => d.n.x1 - REUSE_W).attr('y', d => d.ys).attr('width', REUSE_W).attr('height', d => d.c.th)
                .style('fill', d => (d.cat ? d.cat.color : V.reuse)).attr('clip-path', d => `url(#clip-${id}-${d.n.idx}-${d.n.agg ? 'a' : 'f'})`).style('cursor', 'pointer')
                .on('mouseenter', (ev, d) => hoverReuse(ev, d.c.r)).on('mousemove', moveTip).on('mouseleave', leave).on('click', (ev, d) => pick({ reuse: d.c.r }));
            applyReuse();
            applySel();
            detKey.clear();
            gDet.selectAll('*').remove();
        }
        function applyReuse() {
            const d = st.reuseOn ? null : 'none';
            gArc.style('display', d); gStr.style('display', d);
        }
        function applySel() {
            const sel = st.sel;
            const on = n => sel && ((sel.node && sel.node.id === n.id) || (sel.parent && sel.parent === n.id) || (sel.reuse && sel.reuse.files.includes(n.id)) || (st.hl === n.id));
            if (!rim) return;
            rim.style('stroke', n => on(n) ? 'var(--accent,#66d9ef)' : 'var(--lin-edge)').attr('stroke-width', n => on(n) ? 3 : 1);
            if (ribs) {
                ribs.style('stroke', l => (sel && sel.edge && l.edge === sel.edge ? 'var(--accent,#66d9ef)' : 'none')).attr('stroke-width', 1.5).attr('vector-effect', 'non-scaling-stroke');
                // the lit lineage keeps its own colour, fuller; the rest dims through ribOp
                if (st.P) ribs.attr('fill-opacity', l => (st.fk && l.cat.tk === st.fk ? 0.95 : 0.55));
            }
        }
        function pick(p) { st.sel = p; st.last = p; applySel(); openDrawer(p); }
        // a node click opens the dock, or goes to opts.onNode (the split page sets the table's column)
        function pickNode(n) { if (!(opts.onNode && opts.onNode(n))) pick({ node: n }); }

        // ---- dock: a node's rows by lens, its diff against the parent, a function preview ----
        function dock(n) {
            const R = st.R, lens = st.lens, box = drawer;
            if (st.dockNode !== n.id) { st.dockNode = n.id; st.dockFn = null; st.dockOpen = new Map(); }
            box.replaceChildren();
            const x = el('button', { class: 'lin-x', type: 'button', 'aria-label': 'Close' }, '×');
            x.addEventListener('click', closeDock);
            const h = el('h4', {}), sw = el('span', { class: 'lin-sw', style: 'margin-right:6px' }), tw = twinsOf(n);
            sw.style.background = nodeColor(st.doc, n.idx);
            h.append(sw, document.createTextNode(name(n.id)));
            if (tw) h.append(el('span', { style: 'color:var(--success);margin-left:6px' }, `×${tw.length}`));
            box.append(x, h);
            if (tw) box.append(el('div', { class: 'lin-dim', style: 'word-break:break-all;margin-bottom:6px' }, `${st.doc.columns_mode === 'children' ? 'Cluster files' : 'Same code'}: ${tw.map(md5Of).join(', ')}`));
            const p = st.L.fileOf.get(st.L.parentOf.get(n.id));
            const shown = n.rows.filter(R.match);
            const born = p ? shown.filter(r => !p.has(r)) : [], lost = p ? p.rows.filter(r => R.match(r) && !n.has(r)) : [];
            const line = el('div', { class: 'lin-dim' }, `${fmt(shown.length)} of ${fmt(n.rows.length)} functions shown · `);
            line.append(el('span', { class: 'lin-plus' }, `+${fmt(born.length)}`), document.createTextNode(' '), el('span', { class: 'lin-minus' }, `−${fmt(lost.length)}`));
            box.append(line);
            pivots(box, n);
            const tg = trackedGroup();
            if (tg) box.append(groupCard(tg.x, tg.C));
            const tabs = el('div', { class: 'lin-tabs', role: 'tablist' }), body = el('div');
            for (const [v, t, ic] of [['rows', 'Rows', 'fa-table-list'], ['diff', 'Diff vs parent', 'fa-code-compare'], ['fn', 'Function', 'fa-code']]) {
                const b = el('button', { type: 'button', role: 'tab', 'aria-selected': String(st.dockTab === v) });
                b.append(el('i', { class: `fa-solid ${ic}` }), document.createTextNode(` ${t}`));
                b.addEventListener('click', () => { st.dockTab = v; dock(n); });
                tabs.append(b);
            }
            box.append(tabs, body);
            if (st.dockTab === 'rows') {
                // Deltas: inherited / born here / kept by no child, like the node body; born here tracks as deltas:<node>
                const kids = (st.M.kids.get(n.id) || []).map(k => st.M.byId.get(k).idx);
                const C = lens !== 'deltas' ? cats(lens) : {
                    lens, cats: [{ key: 'kept', label: 'Inherited', color: V.shared }, { key: 'born', label: 'Born here', color: V.born }, { key: 'died', label: 'Kept by no child', color: V.died }],
                    of: r => (p && !p.has(r) ? 'born' : kids.length && !kids.some(i => R.has(r, i)) ? 'died' : 'kept'),
                };
                groups(body, n, shown, C, c => (lens !== 'deltas' ? c.tk : c.key === 'born' ? `deltas:${n.id}` : null));
            } else if (st.dockTab === 'diff') {
                if (!p) { body.append(el('div', { class: 'lin-dim' }, 'The root has no parent. Pick another node.')); return; }
                const e = st.M.inE.get(n.id), kv = el('div', { class: 'lin-kv' });
                kv.append(el('span', { class: 'k' }, 'Parent'), el('span', {}, name(p.id)), el('span', { class: 'k' }, 'Similarity'), el('span', {}, String(e ? e.similarity : '')));
                body.append(kv);
                const C = cats(lens === 'deltas' ? 'modules' : lens);
                for (const [list, cls, sign, text] of [[born, 'lin-plus', '+', 'born here'], [lost, 'lin-minus', '−', 'lost from the parent']]) {
                    const b = el('div', { style: 'margin-top:10px' });
                    b.append(el('span', { class: cls }, `${sign}${fmt(list.length)}`), document.createTextNode(` ${text}`));
                    body.append(b);
                    nameGroups(body, n, list, C);
                }
            } else fnPreview(body, n);
        }
        function groups(body, n, rows, C, trackKey) {
            const by = new Map(C.cats.map(c => [c.key, []]));
            for (const r of rows) by.get(C.of(r)).push(r);
            for (const c of C.cats) {
                const list = by.get(c.key);
                if (!list.length) continue;
                const gh = el('div', { class: 'lin-gh' }), sw = el('span', { class: 'lin-sw' }), tk = trackKey(c), lab = el('span', { class: 'lin-gn' });
                sw.style.background = c.color;
                if (c.g) lab.innerHTML = gHtml(c.g); else lab.textContent = c.label;
                gh.append(sw, lab, el('span', { class: 'lin-dim' }, fmt(list.length)));
                if (tk) {
                    const b = el('button', { type: 'button', class: `lin-ic${st.track === tk ? ' on' : ''}`, title: 'Track this group across the tree', 'aria-label': `Track ${c.label}` });
                    b.append(el('i', { class: 'fa-solid fa-crosshairs' }));
                    b.addEventListener('click', () => setTrack(st.track === tk ? '' : tk));
                    gh.append(b);
                }
                body.append(gh);
                const gk = `${C.lens}:${c.key}`, lim = st.dockOpen.get(gk) || DOCK_ROWS;
                for (const r of list.slice(0, lim)) body.append(rowEl(r));
                if (list.length > lim) {
                    const more = el('button', { type: 'button', class: 'lin-more' }, `+ ${fmt(list.length - lim)} more`);
                    more.addEventListener('click', () => { st.dockOpen.set(gk, lim + 50); dock(n); });
                    body.append(more);
                }
            }
            if (!rows.length) body.append(el('div', { class: 'lin-dim' }, 'No function of this file matches the filters.'));
        }
        // one function row, as in the N-way table: name, role, one hue square per node that holds it
        function rowEl(r) {
            const R = st.R, fid = R.fid[r], on = st.track === `fn:${fid}`;
            const b = el('button', { type: 'button', class: `lin-row${on ? ' on' : ''}`, title: 'Track this function across the tree; the Function tab previews it' });
            const nm = el('span', { class: 'lin-nm' });
            nm.append(el('i', { class: `fa-solid ${R.library[r] ? 'fa-cubes' : 'fa-code'}` }), document.createTextNode(R.name[r] || fid));
            b.append(nm, rolePill(R.role[r]), presence(r));
            b.addEventListener('click', () => { st.dockFn = r; setTrack(`fn:${fid}`); });
            return b;
        }
        function rolePill(role) {
            const x = ROLES.find(o => o[0] === role);
            if (!x) return el('span');
            const s = el('span', { class: `nway-rpill r-${x[0]}` });
            s.append(el('span', { class: 'nway-rdot' }), document.createTextNode(x[1]));
            return s;
        }
        function presence(r) {
            const s = el('span', { class: 'lin-pres', 'aria-hidden': 'true' });
            for (const v of st.L.vis) {
                const i = el('i', { title: name(v.id) });
                if (v.has(r)) { i.style.background = nodeColor(st.doc, v.idx); i.style.boxShadow = 'none'; }
                s.append(i);
            }
            return s;
        }
        function nameGroups(body, n, list, C) {
            const by = new Map(C.cats.map(c => [c.key, []]));
            for (const r of list) by.get(C.of(r)).push(r);
            for (const c of C.cats) {
                const rs = by.get(c.key);
                if (!rs.length) continue;
                const gh = el('div', { class: 'lin-gh', style: 'font-weight:400' }), sw = el('span', { class: 'lin-sw' }), box = el('div', { class: 'lin-names' });
                sw.style.background = c.color;
                gh.append(sw, el('span', {}, c.label), el('span', { class: 'lin-dim' }, fmt(rs.length)));
                for (const r of rs.slice(0, 8)) {
                    const b = el('button', { type: 'button', title: 'Track this function and preview it' }, st.R.name[r] || st.R.fid[r]);
                    b.addEventListener('click', () => { st.dockFn = r; st.dockTab = 'fn'; setTrack(`fn:${st.R.fid[r]}`); });
                    box.append(b);
                }
                if (rs.length > 8) box.append(el('span', { class: 'lin-dim' }, `+${fmt(rs.length - 8)}`));
                body.append(gh, box);
            }
        }
        // same preview as the N-way table's dock Function tab (/api/function/code), this file's own function when the row has one
        function fnPreview(body, n) {
            const R = st.R, r = st.dockFn;
            if (r == null || !n.has(r)) { body.append(el('div', { class: 'lin-dim' }, 'Click a row in Rows or a name in Diff to preview it here.')); return; }
            const head = el('div', { class: 'lin-gh' });
            head.append(el('span', { class: 'lin-nm' }, R.name[r] || R.fid[r]), rolePill(R.role[r]));
            const held = el('div', { class: 'lin-dim', style: 'display:flex;gap:8px;align-items:center' }, 'Held by');
            held.append(presence(r));
            const md5s = new Set((twinsOf(n) || [n.id]).map(md5Of));
            const fid = (R.fids && (R.fids[r] || []).find(f => md5s.has(f.split(':')[2]))) || R.fid[r];
            const code = el('div', { class: 'c-code-container nway-dock-code lin-code' }, 'Loading…');
            body.append(head, held, code);
            fetch(`/api/function/code?id=${encodeURIComponent(fid)}`).then(res => { if (!res.ok) throw new Error(`Function not found (${res.status})`); return res.json(); }).then(d => {
                code.replaceChildren();
                for (const ln of d.rows || []) {
                    const row = el('div', { class: 'code-line' }), gut = el('div', { class: 'gutter' }), ct = el('div', { class: 'line-content' });
                    gut.append(el('div', { class: 'line-num' }, String(Number(ln.line_idx))));
                    for (const t of ln.tokens || []) ct.append(el('span', { class: `token token-${t.type}` }, t.text));
                    row.append(gut, ct);
                    code.append(row);
                }
                if (!code.childElementCount) code.textContent = 'No code available.';
            }).catch(err => { code.textContent = err.message; });
        }

        // ---- minimap ----
        let miniVp, miniS = 1;
        function drawMini() {
            mini.replaceChildren();
            const { W, H, vis } = st.L;
            const ms = d3.select(mini).append('svg').attr('viewBox', '0 0 190 130');
            miniS = Math.min(186 / W, 126 / H);
            const g = ms.append('g').attr('transform', `translate(2,2) scale(${miniS})`);
            g.selectAll('rect').data(vis).join('rect').attr('x', n => n.x0).attr('y', n => n.y0).attr('width', NODE_W).attr('height', n => Math.max(n.h, 2 / miniS)).style('fill', n => n.agg ? V.slateHi : V.slate);
            if (!st.doc.reuse_skipped) g.append('g').selectAll('path').data(st.L.arcs).join('path').attr('d', A => A.d).attr('fill', 'none').attr('stroke', V.reuse).attr('stroke-width', 1 / miniS).attr('stroke-opacity', 0.6);
            miniVp = ms.append('rect').attr('fill', 'var(--accent,#66d9ef)').attr('fill-opacity', 0.12).attr('stroke', 'var(--accent,#66d9ef)').attr('stroke-width', 1.5);
            const go = ev => {
                const r = ms.node().getBoundingClientRect(), k = r.width / 190;
                const wx = ((ev.clientX - r.left) / k - 2) / miniS, wy = ((ev.clientY - r.top) / k - 2) / miniS;
                s.call(zoom.translateTo, wx, wy);
            };
            ms.on('pointerdown', ev => { ms.node().setPointerCapture(ev.pointerId); go(ev); ms.on('pointermove', go); })
                .on('pointerup pointercancel', () => ms.on('pointermove', null));
            miniVp.style('pointer-events', 'none');
        }

        // ---- semantic zoom update ----
        const detKey = new Map();
        const size = () => ({ w: canvas.clientWidth || 800, h: canvas.clientHeight || 500 });
        function schedule(force) {
            if (force) detKey.clear();
            if (st.raf || st.dead) return;
            st.raf = requestAnimationFrame(() => { st.raf = 0; update(); });
        }
        function update() {
            if (st.dead || !st.L) return;
            const { w, h } = size(), t = st.t, k = t.k;
            const m = 120 / k;
            const vx0 = -t.x / k - m, vy0 = -t.y / k - m, vx1 = (w - t.x) / k + m, vy1 = (h - t.y) / k + m;
            const inV = (x0, y0, x1, y1) => x1 >= vx0 && x0 <= vx1 && y1 >= vy0 && y0 <= vy1;
            const L = st.L;
            ribs.attr('display', l => inV(...l.bb) ? null : 'none');
            if (miniVp) miniVp.attr('x', 2 + vx0 * miniS + m * miniS).attr('y', 2 + vy0 * miniS + m * miniS).attr('width', Math.max(2, (w / k) * miniS)).attr('height', Math.max(2, (h / k) * miniS));
            // detail layer: only nodes on screen, and only past a zoom threshold
            const lvl = k >= HATCH_K ? 2 : 1;
            const want = k >= DETAIL_K && !st.P ? L.vis.filter(n => inV(n.x0, n.y0, n.x1, n.y1)) : [];
            const keep = new Set(want.map(n => `${n.id}|${lvl}`));
            for (const key of [...detKey.keys()]) if (!keep.has(key)) { detKey.get(key).remove(); detKey.delete(key); }
            for (const n of want) {
                const key = `${n.id}|${lvl}`;
                if (detKey.has(key)) continue;
                detKey.set(key, drawDetail(n, lvl));
            }
            // labels: largest first, no overlap, by zoom and node size
            gLab.selectAll('*').remove();
            if (k >= LABEL_K) {
                const boxes = [];
                const cand = L.vis.filter(n => inV(n.x0, n.y0, n.x1, n.y1) && (n.h * k >= 10 || k >= 1.2)).sort((a, b) => b.h - a.h);
                for (const n of cand) {
                    const f = name(n.id), txt = f.length > 22 ? `${f.slice(0, 21)}…` : f;
                    const tw = twinsOf(n), lw = txt.length * 7 + 56 + (tw ? 26 : 0), bx = [n.x0, n.y0 - 25, n.x0 + lw, n.y0 - 7];
                    if (boxes.some(b => bx[0] < b[2] && bx[2] > b[0] && bx[1] < b[3] && bx[3] > b[1])) continue;
                    boxes.push(bx);
                    const tx = gLab.append('text').attr('class', 'lin-halo').attr('x', n.x0).attr('y', n.y0 - 11).attr('font-size', 12.5).attr('font-weight', 600).style('fill', 'var(--text)');
                    tx.append('tspan').text(txt);
                    if (tw) tx.append('tspan').attr('dx', 4).style('fill', 'var(--success)').text(`\u00d7${tw.length}`);
                    tx.append('tspan').attr('dx', 6).attr('font-weight', 400).attr('font-size', 11.5).attr('fill-opacity', 0.7).text(`${fmt(n.functions)} fn`);
                    if (n.agg) tx.append('tspan').attr('dx', 6).attr('font-size', 11).text('(click to expand)');
                }
            }
            hits.textContent = hits._m ? hits._m : '';
        }
        // Library share of each file: zebra stripes on the bottom band; nothing on the Code axis
        function drawLib() {
            const R = st.R;
            if (!R || !R.library.length || st.doc.scope === 'code') return;
            const z = defs.append('pattern').attr('id', `lib-${id}`).attr('width', 6).attr('height', 6).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)');
            z.append('rect').attr('width', 3).attr('height', 6).attr('fill', '#fff').attr('fill-opacity', 0.38);
            for (const n of st.L.vis) {
                if (n.agg) continue;
                const held = R.held[n.idx], lib = held.reduce((a, r) => a + (R.library[r] ? 1 : 0), 0);
                const h = held.length ? Math.min(n.h, n.h * lib / held.length) : 0;
                if (h < 1) continue;
                gLib.append('rect').attr('x', n.x0).attr('y', n.y1 - h).attr('width', NODE_W).attr('height', h).attr('fill', `url(#lib-${id})`);
                gLib.append('title').text(`${fmt(lib)} library functions`);
            }
        }
        function drawDetail(n, lvl) {
            const L = st.L, g = gDet.append('g').attr('clip-path', `url(#clip-${id}-${n.idx}-${n.agg ? 'a' : 'f'})`).style('opacity', nodeOp(n));
            for (const o of L.origs.filter(o => o.n === n)) {
                g.append('rect').attr('x', n.x0).attr('y', o.y0).attr('width', ORIG_W).attr('height', Math.max(0.5, o.y1 - o.y0))
                    .style('fill', o.kind === 'inh' ? V.slate : o.hatch && lvl === 2 ? `url(#hatch-born-${id})` : V.born).attr('fill-opacity', o.hatch && lvl === 1 ? 0.65 : 1)
                    .style('cursor', 'pointer').on('mouseenter', ev => hoverOrig(ev, o)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', () => { if (o.edge) pick({ edge: o.edge }); });
            }
            g.append('rect').attr('x', n.x0 + ORIG_W - 0.5).attr('y', n.y0).attr('width', 1).attr('height', n.h).style('fill', 'var(--lin-edge)');
            for (const sg of L.segs.filter(x => x.n === n)) {
                const fork = sg.part && sg.part.children.length > 1;
                g.append('rect').attr('x', n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(0.5, sg.y1 - sg.y0))
                    .style('fill', sg.died ? V.died : fork ? V.slateHi : V.slate).style('stroke', lvl === 2 ? 'var(--lin-edge)' : 'none').attr('stroke-width', 0.75).attr('vector-effect', 'non-scaling-stroke')
                    .style('cursor', 'pointer').on('mouseenter', ev => hoverSeg(ev, sg)).on('mousemove', moveTip).on('mouseleave', leave)
                    .on('click', () => pick(sg.died || sg.agg ? { node: n.agg ? null : n } : { part: sg.part, parent: n.id }));
                if (fork && lvl === 2) g.append('rect').attr('x', n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(0.5, sg.y1 - sg.y0)).attr('fill', `url(#ovl-${id})`).style('pointer-events', 'none');
            }
            return g;
        }

        // ---- hover ----
        function dimRibs(pred) {
            ribs.style('opacity', l => pred(l) ? 1 : 0.08);
            arcSel.style('opacity', 0.25);
            strSel.style('opacity', 0.4);
        }
        function leave() {
            st.hover++;
            if (st.hoverTk) { st.hoverTk = null; syncFocus(); applySel(); }
            ribs.style('opacity', ribOp);
            arcSel.style('opacity', 1); strSel.style('opacity', 1);
            gOv.selectAll('*').remove();
            gDet.style('opacity', 1);
            nodeBase.style('opacity', nodeOp);
            hideTip();
        }
        const partRows = p => [['Kept by', p.folded ? 'several children (folded)' : names(p.children)], ['Functions', fmt(p.count)]];
        const edgeRows = e => [['Born', fmt(e.gained)], ['Died', fmt(e.lost)], ['Shared', fmt(e.shared)], ['Recall suspect', fmt(e.recall_suspect)]];
        function hoverRib(ev, l) {
            st.last = { edge: l.edge };
            dimRibs(d => d.edge === l.edge);
            showTip(ev, `${name(l.edge.parent)} → ${name(l.edge.child)}`, V.shared, [...edgeRows(l.edge), ...partRows(l.part)]);
        }
        // one card for a lane/module, on a node row or a link: the group's name, then what is local to
        // the spot hovered, then the group's own numbers (functions, files, span, features, role mix)
        function lensTip(ev, cat, local) {
            focusHover(cat.tk);
            const g = cat.g, nm = g && NG.shownName(st.S.names[g.id]);
            showTip(ev, nm || cat.label, cat.color, [...local, ...(g ? gTip(g) : [])]);
        }
        function hoverLens(ev, l) {
            st.last = { edge: l.edge };
            lensTip(ev, l.cat, [['Link', `${name(l.source.id)} \u2192 ${name(l.target.id)}`], ['Kept', `${fmt(l.value)} fn`], ...(l.edge ? [['Similarity', l.edge.similarity]] : [])]);
        }
        function hoverLensSeg(ev, n, sg) {
            lensTip(ev, sg.cat, [['In', name(n.id)], ['Here', `${fmt(sg.cnt)} fn`]]);
        }
        function hoverSeg(ev, sg) {
            const n = sg.n;
            if (sg.agg) { showTip(ev, n.file_name, V.slate, [['Functions (summed)', fmt(n.functions)], ['Born inside', fmt(n.born)], ['Kept by no child', fmt(n.died_all)], ['In reuse', fmt(n.reuse_in)]]); return; }
            st.last = { node: n };
            dimRibs(l => sg.part && l.part === sg.part);
            if (sg.died) showTip(ev, `${name(n.id)}: died`, V.died, [['Kept by no child', fmt(sg.count)]]);
            else showTip(ev, `${name(n.id)}: kept by`, sg.part.children.length > 1 ? V.fork : V.shared, partRows(sg.part));
        }
        function hoverOrig(ev, o) {
            const n = o.n;
            if (o.kind === 'inh') showTip(ev, `${name(n.id)}: inherited`, V.slate, [['Functions', fmt(o.count)]]);
            else showTip(ev, `${name(n.id)}: born here`, V.born, [['Functions', fmt(o.count)], ['', o.hatch ? 'likely a matcher miss' : 'not in the parent'], ...(o.edge ? edgeRows(o.edge) : [])]);
        }
        function hoverReuse(ev, r) {
            st.last = { reuse: r };
            ribs.style('opacity', 0.08);
            arcSel.style('opacity', A => A.r === r ? 1 : 0.15);
            strSel.style('opacity', d => d.c.r === r ? 1 : 0.2);
            nodeBase.style('opacity', n => r.files.some(f => st.M.rep.get(f) === n.id) ? 1 : 0.45);
            const rc = reuseCat(r);
            if (rc) focusHover(rc.tk);
            showTip(ev, 'Code outside the tree path', V.reuse, [['Files', names(r.files)], ['Functions', fmt(r.count)], ['Level', r.same_level ? 'same level' : `across depth (span ${r.depth_span})`], ['Tree pieces', r.components.length], ['Note', CAVEAT]]);
        }
        async function hoverFile(ev, F) {
            const tok = ++st.hover;
            st.last = F.agg ? null : { node: F };
            if (F.agg) {
                showTip(ev, F.file_name, V.slate, [['Functions (summed)', fmt(F.functions)], ['Born inside', fmt(F.born)], ['Kept by no child', fmt(F.died_all)], ['In reuse', fmt(F.reuse_in)], ['', 'click to expand']]);
                return;
            }
            const tw = twinsOf(F);
            const rows = [['Functions', fmt(F.functions)], ['In reuse classes', F.reuse_in == null ? 'n/a' : fmt(F.reuse_in)], ...(tw ? [['Same code', names(tw.map(md5Of))]] : [])];
            if (st.P) {
                const sp = st.P.segs.get(F.id);
                ribs.style('opacity', l => (l.source === F || l.target === F ? 1 : 0.1));
                showTip(ev, name(F.id), nodeColor(st.doc, F.idx), [...rows, ['Shown', `${fmt(sp.shown)} of ${fmt(F.rows.length)}`], ...[...sp.m.values()].sort((a, b) => b.cnt - a.cnt).slice(0, 6).map(x => [x.cat.label, fmt(x.cnt)])]);
                return;
            }
            // the reuse arcs that touch this file light up at once; the per-file overlay follows
            arcSel.style('opacity', A => A.r.files.includes(F.id) ? 1 : 0.12);
            strSel.style('opacity', d => d.c.r.files.includes(F.id) ? 1 : 0.25);
            ribs.style('opacity', 0.1);
            showTip(ev, name(F.id), V.slate, rows);
            const bfDoc = await bfFor(F.id);
            if (tok !== st.hover || !bfDoc) return;
            const L = st.L, ni = F.idx;
            const pbf = new Map(), ebf = new Map();
            (st.doc.partitions || []).forEach((p, i) => pbf.set(p, ((bfDoc.partitions[i] || {}).by_file || {})[F.id] || 0));
            st.doc.edges.forEach((e, i) => ebf.set(e, ((bfDoc.edges[i] || {}).born_by_file || {})[F.id] || 0));
            for (const l of L.links) {
                const n = Math.min(l.value, pbf.get(l.part) || 0);
                if (!n) continue;
                const w = Math.max(1, n * L.ky);
                gOv.append('path').attr('d', l.pts ? strip(l.pts, l.o, w) : band(l.source.x1, l.target.x0, l.y0 - l.width / 2 + w / 2, l.y1 - l.width / 2 + w / 2, w)).style('fill', l.col);
            }
            for (const sg of L.segs) {
                if (!sg.part && !sg.died) continue;
                const n = sg.died ? (F.id === sg.n.id ? sg.count : (bfDoc.nodes[sg.n.idx].died_by_file || {})[F.id] || 0) : pbf.get(sg.part) || 0;
                if (!n) continue;
                gOv.append('rect').attr('x', sg.n.x0 + ORIG_W).attr('y', sg.y0).attr('width', NODE_W - ORIG_W).attr('height', Math.max(1, Math.min(n * L.ky, sg.y1 - sg.y0)))
                    .style('fill', sg.died ? V.died : sg.part.children.length > 1 ? V.slateHi : V.slate).style('stroke', 'var(--accent,#66d9ef)').attr('stroke-width', 1).attr('vector-effect', 'non-scaling-stroke');
            }
            gDet.style('opacity', 0.35);
            const nn = bfDoc.nodes[ni];
            showTip(ev, name(F.id), V.slate, [...rows, ['In every file (core)', fmt(bfDoc.core)], ['Only in this file', fmt(nn.only_here)], ...(nn.top_shared || []).map(([g, c]) => [`Shared with ${name(g)}`, fmt(c)])]);
            void ebf;
        }
        async function bfFor(fid) {
            if (!st.doc.by_file_skipped) return st.doc;
            if (st.bfCache.has(fid)) return st.bfCache.get(fid);
            const d = opts.fetchFile ? await opts.fetchFile(fid).catch(() => null) : null;
            if (d) st.bfCache.set(fid, d);
            return d;
        }

        // ---- ring view (nway_ring.js): clusters as circles, modules as rings and ribbons ----
        function ringDraw() {
            const on = st.view === 'ring' && !!window.NwayRing;
            gTree.style('display', on ? 'none' : null);
            gRingG.style('display', on ? null : 'none');
            mini.style.display = on ? 'none' : '';
            ringBox.style.display = on ? 'contents' : 'none';
            // the chip legend is the Ring's optional "Module list"
            legendBox.style.display = on && !(opts.ring && st.ringList) ? 'none' : 'contents';
            if (!on) { if (st.ring) st.ring.show(false); return; }
            if (!st.ring) {
                st.ring = window.NwayRing.mount({
                    g: gRingG.node(), svg: s.node(), host: canvas, controls: ringBox, controls2: ringBox2, opts: opts.ring || {},
                    getDoc: () => st.doc, getR: () => st.R, getCats: () => cats(st.lens), lens: () => st.lens, getTrack: () => st.track,
                    setTrack, onOpt: o => { st.ringList = !!o.list; legendBox.style.display = st.ringList ? 'contents' : 'none'; if (opts.onRing) opts.onRing(o); },
                    onFile: id => {
                        const top = st.M.rep.get(st.R ? st.R.nodeOf.get(id) || id : id);
                        if (top && top.startsWith('agg:')) { st.collapsed = new Set(); rebuild(); }
                        const n = st.L.fileOf.get(top && top.startsWith('agg:') ? id : top);
                        if (n) pickNode(n);
                    },
                    showTip, moveTip, hideTip, fmt, name: nameOf, nodeColor: i => nodeColor(st.doc, i),
                    tree: () => st.doc._colors || (st.doc._colors = window.NwayColors.tree(st.doc.nodes.map(n => ({ id: n.id, path: n.path })))),
                    hsl, hue: hueOf, grey: GREY, fit: () => fit(true),
                    camera: { get: () => st.t, to: bb => zoomBox(bb), restore: t => s.transition().duration(450).call(zoom.transform, t) },
                    openNodes: opts.ring && opts.ring.openNodes, merge: doc => api.setDoc(doc, st.lens, { keep: true }),
                });
            }
            st.ring.show(true);
            st.ring.draw();
        }
        const nameOf = id => st.names.get(id) || id;
        // ---- zoom helpers ----
        function fit(animate) {
            if (st.view === 'ring' && st.ring && st.ring.box()) {
                const b = st.ring.box(), { w, h } = size(), bw = b.x1 - b.x0, bh = b.y1 - b.y0, k = Math.max(K_MIN, Math.min(1, 0.95 * Math.min(w / bw, h / bh)));
                st.hasView = true;
                (animate ? s.transition().duration(400) : s).call(zoom.transform, d3.zoomIdentity.translate(w / 2 - (k * (b.x0 + b.x1)) / 2, h / 2 - (k * (b.y0 + b.y1)) / 2).scale(k));
                return;
            }
            const { w, h } = size(), { W, H } = st.L;
            const kf = Math.min(w / W, h / H, 1);
            // the first view keeps a readable scale: the minimap and Fit cover the rest
            const k = Math.max(K_MIN, animate ? kf : Math.max(kf, 0.3));
            const tr = d3.zoomIdentity.translate(k > kf ? 20 : (w - W * k) / 2, k > kf ? 10 : Math.max(8, (h - H * k) / 2)).scale(k);
            st.hasView = true;
            (animate ? s.transition().duration(400) : s).call(zoom.transform, tr);
        }
        function zoomBox(bb) {
            const { w, h } = size();
            const bw = Math.max(40, bb[2] - bb[0]), bh = Math.max(40, bb[3] - bb[1]);
            const k = Math.max(K_MIN, Math.min(K_MAX, 0.85 * Math.min(w / bw, h / bh)));
            const tr = d3.zoomIdentity.translate(w / 2 - k * (bb[0] + bb[2]) / 2, h / 2 - k * (bb[1] + bb[3]) / 2).scale(k);
            s.transition().duration(500).call(zoom.transform, tr);
        }
        function boxOf(nodes) {
            return [Math.min(...nodes.map(n => n.x0)), Math.min(...nodes.map(n => n.y0)), Math.max(...nodes.map(n => n.x1 + 60)), Math.max(...nodes.map(n => n.y1))];
        }
        function zoomToSel() {
            const p = st.sel || st.last;
            if (!p) return;
            const L = st.L, vis = id => L.fileOf.get(st.M.rep.get(id));
            let nodes = [];
            if (p.node) nodes = [vis(p.node.id)];
            else if (p.edge) nodes = [vis(p.edge.parent), vis(p.edge.child)];
            else if (p.reuse) nodes = p.reuse.files.map(vis);
            else if (p.parent) nodes = [vis(p.parent)];
            nodes = nodes.filter(Boolean);
            if (nodes.length) zoomBox(boxOf(nodes));
        }

        // ---- search ----
        let hitList = [], hitAt = 0;
        function find(next) {
            const q = search.value.trim().toLowerCase();
            st.hl = null;
            if (!q) { hits._m = ''; hits.textContent = ''; applySel(); return; }
            if (!next) {
                hitList = st.doc.nodes.filter(n => (n.file_name || '').toLowerCase().includes(q) || [n.id, ...(n.members || [])].some(x => x.toLowerCase().includes(q))).map(n => n.id);
                hitAt = 0;
            } else hitAt = (hitAt + 1) % Math.max(1, hitList.length);
            hits._m = hitList.length ? `${hitAt + 1}/${hitList.length}` : 'no match';
            hits.textContent = hits._m;
            if (!hitList.length) return;
            const id0 = hitList[hitAt];
            const top = st.M.rep.get(id0);
            if (top && top.startsWith('agg:')) { st.collapsed.delete(top.slice(4)); rebuild(); }
            st.hl = id0;
            applySel();
            zoomBox(boxOf([st.L.fileOf.get(st.M.rep.get(id0))]));
        }
        search.addEventListener('input', () => find(false));
        search.addEventListener('keydown', ev => { if (ev.key === 'Enter') { ev.preventDefault(); find(true); } });

        // ---- size ----
        const ro = new ResizeObserver(() => { const { w, h } = size(); s.attr('width', w).attr('height', h); schedule(); });
        ro.observe(canvas);
        const dblClose = ev => { if (ev.key === 'Escape') closeDock(); };
        document.addEventListener('keydown', dblClose);

        const api = {
            // o.keep: a merge (the ring opened some merged files): the view, the pick and the physics stay
            setDoc(doc, lens, o = {}) {
                if (lens) st.lens = lens;
                // same tree (a filter changed only rows.match): keep the collapsed nodes, the view and a node pick
                const sig = d => `${d.nodes.map(n => n.id).join('|')}#${d.edges.map(e => `${e.parent}>${e.child}`).join('|')}`;
                const same = st.doc && sig(st.doc) === sig(doc);
                st.doc = doc;
                st.bfCache.clear();
                st.R = null;
                st.C = new Map();
                if (!o.keep && (!same || !(st.sel && st.sel.node))) { st.sel = null; closeDock(); }
                st.names = new Map(doc.nodes.map(n => [n.id, n.file_name || n.id]));
                if (!same) st.collapsed = o.keep ? new Set([...st.collapsed].filter(id => st.names.has(id))) : autoCollapse(doc);
                const { w, h } = size();
                s.attr('width', w).attr('height', h);
                if (doc.rows) askNames();
                rebuild();
            },
            setLens(lens) {
                if (lens === st.lens) return;
                st.lens = lens;
                if (st.doc) rebuild();
            },
            // from another pane: no onTrack echo
            setTrack(t) {
                if ((t || '') === st.track) return;
                st.track = t || '';
                if (st.L) applyTrack(true);
            },
            // the canvas changes size with the view (group list, split pane): show the whole tree again
            setView(v) {
                st.view = v;
                if (!st.L) return;
                applyTrack(false);
                ringDraw();
                fit(true);
            },
            hover: hoverRow,
            // outline one node as picked (the split page's table column), or none
            select(id) {
                const n = id && st.L && st.L.fileOf.get(st.M.rep.get(st.R ? st.R.nodeOf.get(id) || id : id));
                st.sel = n ? { node: n } : null;
                applySel();
            },
            zoomTo(wx, wy, k) {
                const { w, h } = size();
                s.call(zoom.transform, d3.zoomIdentity.translate(w / 2 - k * wx, h / 2 - k * wy).scale(k));
            },
            state: st,
            destroy() {
                st.dead = true;
                if (st.ring) st.ring.destroy();
                cancelAnimationFrame(st.raf);
                ro.disconnect();
                document.removeEventListener('keydown', dblClose);
                s.on('.zoom', null);
                tip.remove();
                container.replaceChildren();
            },
        };
        return api;
    }

    // Pivots: file page, its functions, its similar functions, and the N-way table on this
    // column with every page param (track and the table's order=lineage included; layout and lview are lineage-only).
    function pivots(box, n) {
        if (n.agg || !window.Nav) return;
        const [col, , md5] = ((n.members || [])[0] || n.id).split(':'), m = encodeURIComponent(md5);
        const q = tableQs(location.search);
        q.set('column', n.id);
        const go = (txt, url, type) => {
            const b = el('button', { type: 'button', class: 'lin-btn', style: 'margin:6px 6px 0 0' }, txt);
            b.addEventListener('click', ev => window.Nav.openPath(url, ev, { title: txt, type }));
            box.append(b);
        };
        go('File', window.Nav.buildUIUrl(col, ['files', md5]), 'file');
        go('Functions', `${window.Nav.buildUIUrl(col, ['functions'])}?file_md5=${m}`, 'functions');
        go('Similar functions', `${window.Nav.buildUIUrl(col, ['functions', 'similarities'])}?md5=${m}`, 'functions');
        go('N-way: only this file', `/diff/nway?${q}`, 'bin_sim');
    }

    function detail(box, pick, nm, close) {
        box.replaceChildren();
        const x = el('button', { class: 'lin-x', type: 'button', 'aria-label': 'Close' }, '×');
        x.addEventListener('click', close);
        box.append(x);
        const kv = rows => {
            const g = el('div', { class: 'lin-kv' });
            for (const [k, v] of rows) g.append(el('span', { class: 'k' }, k), el('span', {}, String(v)));
            box.append(g);
        };
        if (pick.reuse) {
            const r = pick.reuse;
            box.append(el('h4', {}, `Reused outside the tree path: ${r.files.map(nm).join(' + ')}`));
            kv([['Functions', fmt(r.count)], ['Level', r.same_level ? 'same level' : `across depth (span ${r.depth_span})`], ['Tree pieces', r.components.length]]);
            r.components.forEach((c, i) => kv([[`Piece ${i + 1}`, c.map(nm).join(', ')]]));
            box.append(el('div', { class: 'lin-dim', style: 'margin-top:8px' }, `The tree has no path between these pieces: ${CAVEAT}.`));
            return;
        }
        if (pick.part) {
            const p = pick.part;
            box.append(el('h4', {}, `${nm(p.parent)}: kept by ${p.folded ? 'several children (folded)' : p.children.map(nm).join(' + ')}`));
            kv([['Functions', fmt(p.count)], ...Object.entries(p.per_child || {}).map(([c, n]) => [nm(c), fmt(n)])]);
        }
        if (pick.node) {
            const n = pick.node;
            box.append(el('h4', {}, n.file_name || n.id));
            kv([['Functions', fmt(n.functions) + (n.root ? ' (root)' : '')], ...(n.died_all != null ? [['Kept by no child', fmt(n.died_all)]] : []), ['Only in this file', fmt(n.only_here)], ['In reuse classes', n.reuse_in == null ? 'n/a' : fmt(n.reuse_in)]]);
            box.append(el('div', { class: 'lin-dim', style: 'margin-top:8px;word-break:break-all' }, n.id));
            pivots(box, n);
        } else if (pick.edge) {
            const e = pick.edge, ev = e.recall_evidence || {};
            if (pick.part) box.append(el('div', { style: 'height:10px' }));
            box.append(el('h4', {}, `${nm(e.parent)} → ${nm(e.child)}`));
            kv([['Similarity', e.similarity], ['Shared', fmt(e.shared)], ['Born', fmt(e.gained)], ['Lost', fmt(e.lost)], ['Recall suspect', fmt(e.recall_suspect)],
                ['  below the floor', fmt(ev.edge_below_floor)], ['  refused by one-per-file', fmt(ev.edge_refused)], ['  same name', fmt(ev.name)]]);
        }
    }

    // The lineage page's query as the N-way table's: its own keys (layout, lview, an old link's
    // order=tree|chain|star) dropped, the table's order=lineage and the tree root kept. Without col_open
    // this page shows the twin columns closed, so the table gets them closed too ([] = its depth 0, not
    // its window fit): a column id then names the same node on both pages.
    function tableQs(search) {
        const q = new URLSearchParams(search);
        q.delete('layout'); q.delete('lview'); q.delete('rhold'); q.delete('rnest'); q.delete('rfocus'); q.delete('rarc'); q.delete('rmin'); q.delete('rlist'); q.delete('rtier'); q.delete('rbundle');
        if (q.get('order') !== 'lineage') q.delete('order');
        if (!q.has('col_open')) q.set('col_open', '[]');
        return q;
    }

    window.NwayLineage = { mount, ensureCss, LENSES, tableQs, _layout: { buildModel, layout, autoCollapse, rowModel, lensCats, attachRows, lensPaint, trackRows, trackInfo } }; // _layout: for the layout check
})();
