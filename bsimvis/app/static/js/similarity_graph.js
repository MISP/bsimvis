class SimilarityGraph {
    constructor(containerId) {
        this.containerId = containerId;
        this.mousePos = { clientX: 0, clientY: 0 };
        window.addEventListener('mousemove', e => {
            this.mousePos.clientX = e.clientX;
            this.mousePos.clientY = e.clientY;
        });
        this.abortController = null;
        this.all_pairs = [];
        this.nodes_map = new Map();
        this.unique_nodes = [];
        this.binary_md5s = new Set();
        this.plot = null;
        window.graphInstance = this;

        // 1. Initialize Data Sources
        this.seg_source = new Bokeh.ColumnDataSource({ data: { x0: [], y0: [], x1: [], y1: [], color: [], alpha: [], width: [] } });
        this.hit_source = new Bokeh.ColumnDataSource({ data: { xs: [], ys: [], f1: [], f2: [], b1: [], b2: [], score: [], c1: [], c2: [], s1: [], s2: [], id1: [], id2: [], fn1: [], fn2: [] } });
        this.node_source = new Bokeh.ColumnDataSource({ data: { x: [], y: [], name: [], addr: [], bin: [], color: [], v_size: [], id: [], snippet: [], file_name: [], return_type: [], tags: [] } });
        this.ring_source = new Bokeh.ColumnDataSource({ data: { start: [], end: [], color: [], name: [], md5: [], file_name: [], inner_r: [], outer_r: [], count: [] } });

        this.initPlot();
    }

    initPlot() {
        const container = document.getElementById(this.containerId);
        const width = container.clientWidth || 800;
        const height = container.clientHeight || 500;

        this.plot = Bokeh.Plotting.figure({
            sizing_mode: "stretch_both",
            match_aspect: true, toolbar_location: "above",
            background_fill_color: "#121212", border_fill_color: "#121212",
            active_scroll: "wheel_zoom"
        });
        this.plot.axis.visible = false;
        this.plot.grid.grid_line_color = null;

        // Glyphs
        const r_ring = this.plot.annular_wedge({ x: 0, y: 0, inner_radius: { field: 'inner_r' }, outer_radius: { field: 'outer_r' }, start_angle: { field: 'start' }, end_angle: { field: 'end' }, color: { field: 'color' }, alpha: 0.3, source: this.ring_source });
        this.plot.segment({ x0: { field: 'x0' }, y0: { field: 'y0' }, x1: { field: 'x1' }, y1: { field: 'y1' }, color: { field: 'color' }, line_alpha: { field: 'alpha' }, line_width: { field: 'width' }, source: this.seg_source });
        const r_links = this.plot.multi_line({ xs: { field: 'xs' }, ys: { field: 'ys' }, line_width: 15, line_alpha: 0, hover_line_alpha: 0.6, hover_line_color: "white", source: this.hit_source });
        const r_nodes = this.plot.circle({ x: { field: 'x' }, y: { field: 'y' }, size: 11, color: { field: 'color' }, line_color: "white", source: this.node_source });

        // Hover Tools (No native tooltips)
        const h_bin = new Bokeh.HoverTool({ renderers: [r_ring], tooltips: null });
        const h_links = new Bokeh.HoverTool({ renderers: [r_links], tooltips: null });
        const h_nodes = new Bokeh.HoverTool({ renderers: [r_nodes], tooltips: null });
        h_bin.callback = new Bokeh.CustomJS({
            args: { source: this.ring_source },
            code: `
                const indices = cb_data.index.indices;
                if (indices.length > 0 && !window.graphContextMenuOpen) {
                    const name = source.data.name[indices[0]];
                    const md5 = source.data.md5[indices[0]];
                    const file_name = source.data.file_name[indices[0]];
                    const count = source.data.count[indices[0]];
                    const language = source.data.language[indices[0]];
                    const tags = source.data.tags[indices[0]];
                    const rect = document.getElementById('bk-similarity-plot').getBoundingClientRect();
                    const e = { clientX: cb_data.geometry.vx + rect.left, clientY: cb_data.geometry.vy + rect.top };
                    if (window.showBinaryPreview) window.showBinaryPreview(md5, file_name || name, count, language, tags, e);
                } else {
                    if (window.hideBinaryPreview) window.hideBinaryPreview();
                }
            `
        });

        h_links.callback = new Bokeh.CustomJS({
            args: { source: this.hit_source },
            code: `
                const indices = cb_data.index.indices;
                window.lastGraphLinkIndices = indices;
                if (indices.length > 0 && !window.graphNodeHovered && !window.graphContextMenuOpen) {
                    const id1 = source.data.id1[indices[0]];
                    const id2 = source.data.id2[indices[0]];
                    const n1 = source.data.f1[indices[0]];
                    const n2 = source.data.f2[indices[0]];
                    const score = source.data.score[indices[0]];
                    const extra = indices.length - 1;
                    const rect = document.getElementById('bk-similarity-plot').getBoundingClientRect();
                    const e = { clientX: cb_data.geometry.vx + rect.left, clientY: cb_data.geometry.vy + rect.top };
                    if (window.showDiffPreview) window.showDiffPreview(id1, n1, id2, n2, score, e, extra);
                } else {
                    if (window.hideDiffPreview) window.hideDiffPreview();
                }
            `
        });

        h_nodes.callback = new Bokeh.CustomJS({
            args: { source: this.node_source },
            code: `
                const indices = cb_data.index.indices;
                window.lastGraphNodeIndices = indices;
                if (indices.length > 0 && !window.graphContextMenuOpen) {
                    window.graphNodeHovered = true;
                    const id = source.data.id[indices[0]];
                    const name = source.data.name[indices[0]];
                    const addr = source.data.addr[indices[0]];
                    const bin = source.data.bin[indices[0]];
                    const file_name = source.data.file_name[indices[0]];
                    const v_size = source.data.v_size[indices[0]];
                    const extra = indices.length - 1;
                    const rect = document.getElementById('bk-similarity-plot').getBoundingClientRect();
                    const e = { clientX: cb_data.geometry.vx + rect.left, clientY: cb_data.geometry.vy + rect.top };
                    if (window.showCodePreview) window.showCodePreview(id, name, addr, bin, v_size, e, extra, file_name);
                } else {
                    window.graphNodeHovered = false;
                    if (window.hideCodePreview) window.hideCodePreview();
                }
            `
        });

        this.plot.add_tools(h_bin, h_links, h_nodes, new Bokeh.TapTool({ renderers: [r_links, r_nodes] }));
        this.plot.toolbar.active_inspect = [h_bin, h_links, h_nodes];

        // Tap Logic (direct call)
        const onSelect = () => {
            const node_inds = this.node_source.selected.indices;
            const hit_inds = this.hit_source.selected.indices;

            if (this.node_source.data.id && node_inds.length > 0) {
                const id = this.node_source.data.id[node_inds[0]];
                const name = this.node_source.data.name[node_inds[0]];
                window.showFunctionCodeById(id, name);
                this.node_source.selected.indices = [];
            } else if (hit_inds.length > 0) {
                const id1 = this.hit_source.data.id1[hit_inds[0]]; const id2 = this.hit_source.data.id2[hit_inds[0]];
                const name1 = this.hit_source.data.f1[hit_inds[0]]; const name2 = this.hit_source.data.f2[hit_inds[0]];
                window.openDiffDirectly(id1, name1, id2, name2);
                this.hit_source.selected.indices = [];
            }
        };
        this.node_source.selected.properties.indices.change.connect(onSelect.bind(this));
        this.hit_source.selected.properties.indices.change.connect(onSelect.bind(this));

        Bokeh.Plotting.show(this.plot, `#${this.containerId}`);
    }

    getRingTooltip() {
        return `<div style="padding:12px; background:#1A1A1A; color:#FFF; border:2px solid @color; border-radius:8px; min-width:200px;">
            <div style="font-size:14px; font-weight:bold; margin-bottom:4px; color:@color;">@file_name (BINARY)</div>
            <div style="font-size:11px; color:#AAA; margin-bottom:8px;">MD5: @md5</div>
            <div style="display:flex; justify-content:space-between; font-size:12px; border-top:1px solid #333; padding-top:8px;">
                <span>Functions in Graph:</span>
                <span style="font-weight:bold; color:#ff79c6;">@count</span>
            </div>
        </div>`;
    }

    getLinkTooltip() {
        return `<div style="padding:12px; background:#121212; color:#D4D4D4; border:1px solid #444; border-radius:8px; width:340px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <span style="color:#A6E22E; font-weight:bold; font-size:16px;">@score MATCH</span>
                <span style="font-size:10px; color:#777;">Pair Summary</span>
            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
                <div style="background:#1A1A1A; padding:6px; border-radius:4px; border-left:3px solid @c1;">
                    <div style="font-size:10px; font-weight:bold; color:#AAA; overflow:hidden; white-space:nowrap; text-overflow:ellipsis;" title="@fn1">@f1</div>
                    <div style="font-size:8px; color:#777; overflow:hidden; text-overflow:ellipsis;">@fn1</div>
                </div>
                <div style="background:#1A1A1A; padding:6px; border-radius:4px; border-left:3px solid @c2;">
                    <div style="font-size:10px; font-weight:bold; color:#AAA; overflow:hidden; white-space:nowrap; text-overflow:ellipsis;" title="@fn2">@f2</div>
                    <div style="font-size:8px; color:#777; overflow:hidden; text-overflow:ellipsis;">@fn2</div>
                </div>
            </div>
        </div>`;
    }

    getNodeTooltip() {
        return `<div style="padding:12px; background:#1A1A1A; color:#D4D4D4; border:2px solid @color; border-radius:8px; width:280px;">
            <div style="font-size:13px; font-weight:bold; margin-bottom:2px; color:@color;">@name</div>
            <div style="font-size:11px; color:#AAA; margin-bottom:4px; font-style:italic;">@file_name</div>
            <div style="font-size:10px; color:#AAA; margin-bottom:8px;">Addr: @addr | Features: @v_size | @return_type</div>
        </div>`;
    }

    stop() { if (this.abortController) this.abortController.abort(); }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        this.all_pairs = []; this.nodes_map.clear(); this.unique_nodes = []; this.binary_md5s.clear();

        const overlay = document.getElementById('graph-loading-overlay');
        const streamInfo = document.getElementById('graph-stream-info');
        const loadingText = document.getElementById('graph-loading-text');
        const stopBtn = document.getElementById('graph-stop-btn');
        const DEFAULT_GRAPH_LIMIT = 500;
        const MAX_TOTAL = parseInt(params.get('limit')) || DEFAULT_GRAPH_LIMIT;

        overlay.style.display = 'flex';
        loadingText.innerText = "Building Similarity Map...";
        streamInfo.innerText = "";
        stopBtn.style.display = 'inline-block';

        // Clone and clean params to avoid duplicates in loop (like &offset=0&offset=500)
        const cleanParams = new URLSearchParams(params.toString());
        cleanParams.delete('limit');
        cleanParams.delete('offset');
        const base_url = `/api/similarity/search?${cleanParams.toString()}`;
        let currentOffset = 0;

        // Clear the graph immediately
        this.updateSources(params);

        try {
            while (currentOffset < MAX_TOTAL && !signal.aborted) {
                const BATCH_SIZE = Math.min(MAX_TOTAL - this.all_pairs.length, 500);
                if (BATCH_SIZE <= 0) break;

                const res = await fetch(`${base_url}&limit=${BATCH_SIZE}&offset=${currentOffset}`, { signal });
                const data = await res.json();

                // Handle truncation and limit alerts next to inputs
                const poolIcon = document.getElementById('pool-warn-icon');
                const limitIcon = document.getElementById('limit-warn-icon');
                const poolInput = document.getElementById('sim-pool-limit');
                const limitInput = document.getElementById('sim-limit');
                const totalEl = document.getElementById('view-total');

                if (poolIcon) {
                    if (data.pool_truncated) {
                        poolIcon.style.display = 'inline-block';
                        poolIcon.title = `⚠️ Pool Truncated to first ${data.pool_limit || '---'} matches. Results may be incomplete.`;
                        if (poolInput) poolInput.style.borderColor = '#ffab2e';
                    } else {
                        poolIcon.style.display = 'none';
                        if (poolInput) poolInput.style.borderColor = 'var(--accent)';
                    }
                }

                const pairs = data.pairs || [];
                if (pairs.length === 0) break;

                this.all_pairs = this.all_pairs.concat(pairs);

                if (limitIcon) {
                    if (this.all_pairs.length >= MAX_TOTAL) {
                        limitIcon.style.display = 'inline-block';
                        limitIcon.title = `ℹ️ Result Limit Reached (${MAX_TOTAL.toLocaleString()}). Map display is capped.`;
                        if (limitInput) limitInput.style.borderColor = '#60a5fa';
                    } else {
                        limitIcon.style.display = 'none';
                        if (limitInput) limitInput.style.borderColor = 'var(--accent)';
                    }
                }

                if (totalEl) {
                    totalEl.style.display = 'inline-block';
                    totalEl.innerText = `${this.all_pairs.length.toLocaleString()} / ${(data.total || 0).toLocaleString()}`;
                }
                pairs.forEach(p => {
                    [{ id: p.id1, name: p.name1, meta: p.meta1 }, { id: p.id2, name: p.name2, meta: p.meta2 }].forEach(n => {
                        if (!this.nodes_map.has(n.id)) {
                            const node_obj = {
                                id: n.id, name: n.name, md5: n.meta.file_md5,
                                file_name: n.meta.file_name, return_type: n.meta.return_type,
                                addr: n.id.split(':').pop(), v_size: n.meta.bsim_features_count,
                                language_id: n.meta.language_id || 'N/A',
                                tags: n.meta.tags || []
                            };
                            this.nodes_map.set(n.id, node_obj); this.unique_nodes.push(node_obj); this.binary_md5s.add(n.meta.file_md5);
                        }
                    });
                });

                this.updateSources(params);
                streamInfo.innerText = `Streamed ${this.all_pairs.length} matches...`;
                currentOffset += BATCH_SIZE;
                if (pairs.length < BATCH_SIZE) break;
            }
        } catch (e) { if (e.name !== 'AbortError') console.error(e); }
        finally {
            overlay.style.display = 'none';
            stopBtn.style.display = 'none';
        }
    }

    updateSources(params) {
        if (this.all_pairs.length === 0) {
            this.node_source.data = { x: [], y: [], name: [], addr: [], bin: [], color: [], v_size: [], id: [], file_name: [], return_type: [], tags: [] };
            this.seg_source.data = { x0: [], y0: [], x1: [], y1: [], color: [], alpha: [], width: [] };
            this.hit_source.data = { xs: [], ys: [], f1: [], f2: [], b1: [], b2: [], score: [], c1: [], c2: [], id1: [], id2: [], fn1: [], fn2: [] };
            this.ring_source.data = { start: [], end: [], color: [], name: [], md5: [], file_name: [], inner_r: [], outer_r: [], count: [] };
            return;
        }

        this.unique_nodes.sort((a, b) => a.md5.localeCompare(b.md5) || a.name.localeCompare(b.name));
        const n_active = this.unique_nodes.length;
        const theta_step = (2 * Math.PI) / n_active;
        const derived_r = Math.max(2.5, 0.3 / theta_step);
        const ring_inner = derived_r + 0.3; const ring_outer = derived_r + 1.8;

        const id_to_info = new Map();
        this.unique_nodes.forEach((n, i) => {
            const angle = i * theta_step;
            id_to_info.set(n.id, { ...n, x: derived_r * Math.cos(angle), y: derived_r * Math.sin(angle), angle: angle });
        });

        const palette = ["#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"];
        const bin_list = Array.from(this.binary_md5s).sort();
        const bin_colors = new Map(bin_list.map((md5, i) => [md5, palette[i % 20]]));

        const ns = { x: [], y: [], name: [], addr: [], bin: [], color: [], v_size: [], id: [], file_name: [], return_type: [] };
        this.unique_nodes.forEach(n => {
            const info = id_to_info.get(n.id);
            ns.x.push(info.x); ns.y.push(info.y); ns.name.push(n.name); ns.addr.push(n.addr);
            ns.bin.push(n.md5.slice(0, 8)); ns.color.push(bin_colors.get(n.md5));
            ns.v_size.push(n.v_size); ns.id.push(n.id);
            ns.file_name.push(n.file_name || ''); ns.return_type.push(n.return_type || 'N/A');
        });

        const ss = { x0: [], y0: [], x1: [], y1: [], color: [], alpha: [], width: [] };
        const hs = { xs: [], ys: [], f1: [], f2: [], b1: [], b2: [], score: [], c1: [], c2: [], id1: [], id2: [], fn1: [], fn2: [] };
        const minScoreParam = params.get('min_score');
        const minScore = (minScoreParam !== null && minScoreParam !== "") ? parseFloat(minScoreParam) : 0.95;

        this.all_pairs.forEach(p => {
            const n1 = id_to_info.get(p.id1); const n2 = id_to_info.get(p.id2);
            if (!n1 || !n2) return;
            const t_vals = []; for (let i = 0; i <= 1; i += 1 / 15) t_vals.push(i);
            const curve = t_vals.map(t => {
                const it = 1 - t;
                return { x: it * it * n1.x + 2 * it * t * 0 + t * t * n2.x, y: it * it * n1.y + 2 * it * t * 0 + t * t * n2.y };
            });
            const col1 = bin_colors.get(n1.md5); const col2 = bin_colors.get(n2.md5);
            const norm = (p.score - minScore) / (1.0 - minScore + 0.0001);
            for (let k = 0; k < curve.length - 1; k++) {
                ss.x0.push(curve[k].x); ss.y0.push(curve[k].y); ss.x1.push(curve[k + 1].x); ss.y1.push(curve[k + 1].y);
                ss.color.push(this.blendHex(col1, col2, t_vals[k])); ss.alpha.push(0.1 + (norm * 0.7)); ss.width.push(0.5 + (norm * 6));
            }
            hs.xs.push(curve.map(v => v.x)); hs.ys.push(curve.map(v => v.y));
            hs.f1.push(p.name1); hs.f2.push(p.name2); hs.b1.push(n1.md5.slice(0, 8)); hs.b2.push(n2.md5.slice(0, 8));
            hs.score.push(p.score.toFixed(4)); hs.c1.push(col1); hs.c2.push(col2); hs.id1.push(p.id1); hs.id2.push(p.id2);
            hs.fn1.push(n1.file_name || n1.md5.slice(0, 8)); hs.fn2.push(n2.file_name || n2.md5.slice(0, 8));

        });

        const rs = { start: [], end: [], color: [], name: [], md5: [], file_name: [], inner_r: [], outer_r: [], count: [], language: [], tags: [] };
        bin_list.forEach(md5 => {
            const md5_nodes = this.unique_nodes.map((n, i) => ({ n, i })).filter(item => item.n.md5 === md5);
            if (md5_nodes.length > 0) {
                const start_idx = md5_nodes[0].i; const end_idx = md5_nodes[md5_nodes.length - 1].i;
                const col = bin_colors.get(md5);
                rs.start.push(start_idx * theta_step - theta_step / 2); rs.end.push(end_idx * theta_step + theta_step / 2);
                rs.color.push(col); rs.name.push(md5.slice(0, 8)); rs.md5.push(md5);
                rs.file_name.push(md5_nodes[0].n.file_name || md5.slice(0, 8));
                rs.inner_r.push(ring_inner); rs.outer_r.push(ring_outer);
                rs.count.push(md5_nodes.length);
                rs.language.push(md5_nodes[0].n.language_id);
                const all_tags = new Set();
                md5_nodes.forEach(item => { (item.n.tags || []).forEach(t => all_tags.add(t)); });
                rs.tags.push(Array.from(all_tags).join(', '));
            }
        });

        this.node_source.data = ns; this.seg_source.data = ss; this.hit_source.data = hs; this.ring_source.data = rs;
    }

    blendHex(c1, c2, t) {
        const rgb1 = [parseInt(c1.slice(1, 3), 16), parseInt(c1.slice(3, 5), 16), parseInt(c1.slice(5, 7), 16)];
        const rgb2 = [parseInt(c2.slice(1, 3), 16), parseInt(c2.slice(3, 5), 16), parseInt(c2.slice(5, 7), 16)];
        const res = rgb1.map((v, i) => Math.round(v * (1 - t) + rgb2[i] * t));
        return "#" + res.map(v => v.toString(16).padStart(2, '0')).join('');
    }
}
