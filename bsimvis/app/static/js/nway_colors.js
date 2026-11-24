/**
 * Column colours shared by the N-way table (nway_panel.js) and the lineage page
 * (nway_lineage.js), so a lineage node has exactly the hue and lightness of its column.
 *
 * NwayColors.hue(i): golden-angle steps; i is a column's index in the default /nway
 * order (a column moved by order=lineage keeps it as `base`).
 * NwayColors.tree(cols): the column tree rebuilt from the columns' paths. Each top-level
 * group or column gets a hue, what is inside it shifts that hue and its lightness a
 * little, so a sub-cluster reads as part of its parent. Siblings are ranked by their
 * first column in the default order, so a reorder never repaints a column.
 * -> { grouped, root, colors: [{h, l}] per column (grouped only), byId }.
 * Lightness l is the dark theme's (52% at the top level, as .nway-sq.on); the light
 * theme uses l - 8.
 */
window.NwayColors = (function () {
    const hue = i => Math.round((i * 137.508 + 215) % 360);

    function tree(cols) {
        const grouped = cols.some(c => c.kind || (c.path || []).length);
        const root = { kids: [] }, byId = new Map(), colors = [];
        const node = (parent, id, o) => {
            let n = byId.get(id);
            if (!n) { n = { id, kids: [], lo: Infinity, hi: -1, b0: Infinity, ...o }; parent.kids.push(n); byId.set(id, n); }
            return n;
        };
        cols.forEach((c, i) => {
            let p = root;
            const chain = [], base = c.base != null ? c.base : i;
            for (const g of c.path || []) { p = node(p, g.id, { label: g.label, count: Number(g.member_count) || 0 }); chain.push(p); }
            const label = c.kind ? c.label : (c.file_name && c.file_name !== c.md5 ? c.file_name : String(c.md5 || c.id).slice(0, 8));
            const leaf = node(p, c.id, { label, count: c.kind ? Number(c.member_count) || 0 : 1, col: i, expandable: !!c.expandable });
            for (const n of [...chain, leaf]) { n.lo = Math.min(n.lo, i); n.hi = Math.max(n.hi, i); n.b0 = Math.min(n.b0, base); }
        });
        const paint = (n, h, l, depth) => {
            n.h = h; n.l = l;
            if (n.col != null) colors[n.col] = { h, l };
            const m = n.kids.length, rank = [...n.kids].sort((a, b) => a.b0 - b.b0);
            for (const k of n.kids) {
                const j = rank.indexOf(k);
                paint(k, depth === 0 ? hue(j) : (h + Math.round((j - (m - 1) / 2) * 12) + 360) % 360, depth === 0 ? 52 : 46 + (j % 3) * 6, depth + 1);
            }
        };
        if (grouped) paint(root, 0, 52, 0);
        return { grouped, root, colors, byId };
    }

    return { hue, tree };
})();
