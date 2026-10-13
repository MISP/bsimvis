// Pure CFG layout shared by the N-way strip and the single-function CFG tab.
// R, F: {nodes: [], edges: [[u, v]]}; pairOf maps F node -> R node (empty for one graph).
window.CfgLayout = {
    // One row grid for both graphs: matched blocks share a row, so equal code lines up even with
    // extra blocks between. Back edges (loops) are left out of the layering and drawn as side arcs.
    grid(R, F, pairOf) {
        const fid = k => (pairOf.has(k) ? 'r' + pairOf.get(k) : 'f' + k);
        const ids = [...R.nodes.keys()].map(i => 'r' + i).concat([...F.nodes.keys()].map(fid));
        const adj = new Map(ids.map(id => [id, new Set()]));
        const rE = R.edges.map(([u, v]) => ['r' + u, 'r' + v]), fE = F.edges.map(([u, v]) => [fid(u), fid(v)]);
        // A block outside the CFG (the signature) hangs above the next block in source order.
        [[R, rE, i => 'r' + i], [F, fE, fid]].forEach(([G, E, idf]) => {
            const used = new Set(E.flat());
            G.nodes.forEach((nd, i) => { if (!used.has(idf(i)) && i + 1 < G.nodes.length) E.push([idf(i), idf(i + 1)]); });
        });
        for (const [u, v] of rE.concat(fE)) if (u !== v) adj.get(u).add(v);
        const state = new Map(), back = new Set();
        for (const root of ['r0', ...adj.keys()]) {
            if (state.has(root)) continue;
            const stack = [[root, [...adj.get(root)]]];
            state.set(root, 1);
            while (stack.length) {
                const top = stack[stack.length - 1], v = top[1].shift();
                if (v === undefined) { state.set(top[0], 2); stack.pop(); continue; }
                if (state.get(v) === 1) back.add(top[0] + '|' + v);
                else if (!state.has(v)) { state.set(v, 1); stack.push([v, [...adj.get(v)]]); }
            }
        }
        const indeg = new Map(ids.map(id => [id, 0])), row = new Map(ids.map(id => [id, 0]));
        for (const [u, vs] of adj) for (const v of vs) if (!back.has(u + '|' + v)) indeg.set(v, indeg.get(v) + 1);
        const q = ids.filter(id => !indeg.get(id));
        for (let h = 0; h < q.length; h++) for (const v of adj.get(q[h])) {
            if (back.has(q[h] + '|' + v)) continue;
            row.set(v, Math.max(row.get(v), row.get(q[h]) + 1));
            indeg.set(v, indeg.get(v) - 1);
            if (!indeg.get(v)) q.push(v);
        }
        const NW = 44, mean = xs => xs.reduce((t, v) => t + v, 0) / xs.length;
        // Each graph keeps its own shape. Edges spanning several rows get a dummy per row so they
        // route around blocks; rows are ordered by barycenter sweeps, then packed under their neighbours.
        const place = (members, edges) => {
            const rowOf = new Map(row), dn = new Map(), up = new Map(), chain = new Map(), ends = new Map(), seen = new Set(), ids = [...members];
            const link = (u, v) => { (dn.get(u) || dn.set(u, []).get(u)).push(v); (up.get(v) || up.set(v, []).get(v)).push(u); };
            for (const [u, v] of edges) {
                const key = u + '|' + v;
                if (u === v || back.has(key) || seen.has(key)) continue;
                seen.add(key);
                let prev = u;
                const ds = [];
                for (let r = row.get(u) + 1; r < row.get(v); r++) { const d = `d${key}#${r}`; rowOf.set(d, r); ids.push(d); link(prev, d); prev = d; ds.push(d); }
                link(prev, v);
                chain.set(key, ds);
                ds.forEach((d, i) => ends.set(d, [u, v, (i + 1) / (ds.length + 1)]));
            }
            const byRow = new Map(), pos = new Map();
            ids.forEach(id => { const r = rowOf.get(id); (byRow.get(r) || byRow.set(r, []).get(r)).push(id); });
            const asc = [...byRow.keys()].sort((m, n) => m - n), desc = [...asc].reverse();
            const reindex = r => byRow.get(r).forEach((id, i) => pos.set(id, i));
            asc.forEach(reindex);
            const sweep = (rs, nb) => rs.forEach(r => {
                const arr = byRow.get(r).map(id => { const ns = nb.get(id) || []; return { id, b: ns.length ? mean(ns.map(n => pos.get(n))) : pos.get(id) }; });
                arr.sort((m, n) => m.b - n.b);
                byRow.set(r, arr.map(m => m.id));
                reindex(r);
            });
            for (let it = 0; it < 4; it++) { sweep(asc, up); sweep(desc, dn); }
            const hw = id => (id[0] === 'd' ? 4 : NW / 2), x = new Map();
            const pack = (rs, nbf) => rs.forEach(r => {
                const arr = byRow.get(r);
                let root = 0;
                const want = arr.map(id => {
                    const e = ends.get(id);
                    if (e && x.has(e[0]) && x.has(e[1])) return x.get(e[0]) + (x.get(e[1]) - x.get(e[0])) * e[2];
                    const ns = nbf(id).filter(n => x.has(n));
                    return ns.length ? mean(ns.map(n => x.get(n))) : x.has(id) ? x.get(id) : (root += 60) - 60;
                });
                const at = [];
                want.forEach((w, i) => at.push(i ? Math.max(w, at[i - 1] + hw(arr[i - 1]) + hw(arr[i]) + 14) : w));
                const shift = mean(want.map((w, i) => w - at[i]));
                arr.forEach((id, i) => x.set(id, at[i] + shift));
            });
            // First a top-down pass under the parents, then passes that pull each block towards all its neighbours.
            const par = id => up.get(id) || [], all = id => par(id).concat(dn.get(id) || []);
            pack(asc, par);
            for (let it = 0; it < 8; it++) { pack(desc, all); pack(asc, all); }
            const lo = Math.min(...[...x].map(([id, v]) => v - hw(id)));
            for (const [id, v] of x) x.set(id, v - lo);
            return { x, rowOf, chain, w: Math.max(...[...x].map(([id, v]) => v + hw(id))) };
        };
        // One layout for the union of both graphs: matched blocks share row and column, so equal
        // functions draw the same shape and a difference shows as a change of shape.
        const L = place([...new Set(ids)], rE.concat(fE)), M = L;
        return { fid, row, back, L, M, rows: Math.max(...row.values()) + 1 };
    },

    // Straight pieces with rounded corners through the points (x, y) of one edge.
    path(pts) {
        pts = pts.filter((q, i) => !i || i === pts.length - 1 || Math.abs((q.x - pts[i - 1].x) * (pts[i + 1].y - q.y) - (q.y - pts[i - 1].y) * (pts[i + 1].x - q.x)) > 1);
        if (pts.length === 2) { const m = (pts[0].y + pts[1].y) / 2; return `M${pts[0].x},${pts[0].y} C${pts[0].x},${m} ${pts[1].x},${m} ${pts[1].x},${pts[1].y}`; }
        let d = `M${pts[0].x},${pts[0].y}`;
        for (let i = 1; i < pts.length - 1; i++) {
            const a = pts[i - 1], q = pts[i], b = pts[i + 1], la = Math.hypot(q.x - a.x, q.y - a.y), lb = Math.hypot(b.x - q.x, b.y - q.y), r = Math.min(12, la / 2, lb / 2);
            d += ` L${q.x + (a.x - q.x) * r / la},${q.y + (a.y - q.y) * r / la} Q${q.x},${q.y} ${q.x + (b.x - q.x) * r / lb},${q.y + (b.y - q.y) * r / lb}`;
        }
        return `${d} L${pts[pts.length - 1].x},${pts[pts.length - 1].y}`;
    },
};
