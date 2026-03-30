from bokeh.events import DocumentReady
from bokeh.models import ColumnDataSource, HoverTool, LabelSet, CustomJS, TapTool, Button, Column, Div
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html
from flask import Blueprint, request

# --- Blueprint Setup ---
search_similarity_plot_bp = Blueprint('search_similarity_plot', __name__)

# --- Constants & Config ---
DEFAULT_THRESHOLD = 0.95
DEFAULT_MAX_SCORE = 1.0
DEFAULT_MIN_FEATURES = 30
DEFAULT_POOL_LIMIT = 100000
API_BASE = "" # Relative since we are on the same port

@search_similarity_plot_bp.route('/similarity/plot')
def similarity_plot():
    collection = request.args.get('collection', 'main')

    # --- Data Sources (Empty at start) ---
    seg_source = ColumnDataSource(data=dict(x0=[], y0=[], x1=[], y1=[], color=[], alpha=[], width=[]))
    hit_source = ColumnDataSource(data=dict(xs=[], ys=[], f1=[], f2=[], b1=[], b2=[], score=[], c1=[], c2=[], id1=[], id2=[]))
    node_source = ColumnDataSource(data=dict(x=[], y=[], name=[], addr=[], bin=[], color=[], v_size=[], id=[]))
    ring_source = ColumnDataSource(data=dict(start=[], end=[], color=[], name=[], md5=[], inner_r=[], outer_r=[]))
    label_source = ColumnDataSource(data=dict(x=[], y=[], text=[], color=[]))
    
    # --- Unified Trigger logic ---
    
    UPDATE_GRAPH_JS = CustomJS(args=dict(
        seg_source=seg_source, hit_source=hit_source, node_source=node_source,
        ring_source=ring_source, label_source=label_source,
        COLLECTION_VAL=collection,
        api_base=API_BASE
    ), code="""
        console.log("UPDATE_GRAPH_JS: STARTING FETCH SEQUENCE...");
        
        // 1. Show Spinner
        let overlay = document.getElementById('loading-overlay');
        if (!overlay) {
            const style = document.createElement('style');
            style.id = 'spinner-style';
            style.innerHTML = `@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`;
            document.head.appendChild(style);
            overlay = document.createElement('div');
            overlay.id = 'loading-overlay';
            document.body.appendChild(overlay);
        }
        Object.assign(overlay.style, {
            position: 'fixed', top: '0', left: '0', width: '100vw', height: '100vh',
            background: 'rgba(18, 18, 18, 0.95)', zIndex: '9999', display: 'flex',
            justifyContent: 'center', alignItems: 'center', flexDirection: 'column',
            color: '#66d9ef', fontFamily: "'Segoe UI', monospace", fontSize: '18px',
            backdropFilter: 'blur(10px)'
        });
        overlay.innerHTML = `
            <div style="border: 4px solid #333; border-top: 4px solid #66d9ef; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 15px;"></div>
            <div id="loading-text">Fetching Similarity Records...</div>
        `;
        overlay.style.display = 'flex';

        // 2. Mirror parent parameters exactly for Canonical State (Perfect Match)
        const params = new URLSearchParams(window.location.search);
        if (!params.get('collection')) params.set('collection', COLLECTION_VAL);

        const MAX_TOTAL = parseInt(params.get('limit')) || 1000;
        
        // Remove offset/limit for clean base URL construction
        const baseParams = new URLSearchParams(params);
        baseParams.delete('limit'); // Batch logic will add its own limit
        baseParams.delete('offset');
        
        let base_url = `${api_base}/api/similarity/search?${baseParams.toString()}`;
    
        console.log("Base Search URL (Perfect Match): " + base_url + " (Total Cap: " + MAX_TOTAL + ")");
    
        if (window.search_abort) window.search_abort.abort();
        window.search_abort = new AbortController();
        const signal = window.search_abort.signal;
    
        function blendHex(c1, c2, t) {
            const rgb1 = [parseInt(c1.slice(1,3), 16), parseInt(c1.slice(3,5), 16), parseInt(c1.slice(5,7), 16)];
            const rgb2 = [parseInt(c2.slice(1,3), 16), parseInt(c2.slice(3,5), 16), parseInt(c2.slice(5,7), 16)];
            const res = rgb1.map((v, i) => Math.round(v * (1 - t) + rgb2[i] * t));
            return "#" + res.map(v => v.toString(16).padStart(2, '0')).join('');
        }
    
        let all_pairs = [];
        let unique_nodes = [];
        let nodes_map = new Map();
        let binary_md5s = new Set();
    
        function updateSources() {
            if (all_pairs.length === 0) return;
    
            unique_nodes.sort((a, b) => a.md5.localeCompare(b.md5) || a.name.localeCompare(b.name));
            const n_active = unique_nodes.length;
            const theta_step = (2 * Math.PI) / n_active;
            const derived_r = Math.max(2.5, 0.3 / theta_step);
            const ring_inner = derived_r + 0.3;
            const ring_outer = derived_r + 1.8;
            const label_r = derived_r + 3.5;
    
            const id_to_info = new Map();
            unique_nodes.forEach((n, i) => {
                const angle = i * theta_step;
                id_to_info.set(n.id, { ...n, x: derived_r * Math.cos(angle), y: derived_r * Math.sin(angle), angle: angle });
            });
    
            const palette = ["#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"];
            const bin_list = Array.from(binary_md5s).sort();
            const bin_colors = new Map(bin_list.map((md5, i) => [md5, palette[i % 20]]));
    
            const ns = { x: [], y: [], name: [], addr: [], bin: [], color: [], v_size: [], id: [] };
            unique_nodes.forEach(n => {
                const info = id_to_info.get(n.id);
                ns.x.push(info.x); ns.y.push(info.y); ns.name.push(n.name); ns.addr.push(n.addr);
                ns.bin.push(n.md5.slice(0, 8)); ns.color.push(bin_colors.get(n.md5));
                ns.v_size.push(n.v_size); ns.id.push(n.id);
            });
    
            const ss = { x0: [], y0: [], x1: [], y1: [], color: [], alpha: [], width: [] };
            const hs = { xs: [], ys: [], f1: [], f2: [], b1: [], b2: [], score: [], c1: [], c2: [], id1: [], id2: [] };
    
            all_pairs.forEach(p => {
                const n1 = id_to_info.get(p.id1); const n2 = id_to_info.get(p.id2);
                if (!n1 || !n2) return;
                const t_vals = []; for(let i=0; i<=1; i+=1/15) t_vals.push(i);
                const curve = t_vals.map(t => {
                    const it = 1 - t;
                    return { x: it*it * n1.x + 2*it*t * 0 + t*t * n2.x, y: it*it * n1.y + 2*it*t * 0 + t*t * n2.y };
                });
                const col1 = bin_colors.get(n1.md5); const col2 = bin_colors.get(n2.md5);
                const threshold = parseFloat(params.get('threshold')) || 0.95;
                const norm = (p.score - threshold) / (1.0 - threshold + 0.0001);
                for (let k=0; k < curve.length - 1; k++) {
                    ss.x0.push(curve[k].x); ss.y0.push(curve[k].y); ss.x1.push(curve[k+1].x); ss.y1.push(curve[k+1].y);
                    ss.color.push(blendHex(col1, col2, t_vals[k])); ss.alpha.push(0.1 + (norm * 0.7)); ss.width.push(0.5 + (norm * 6));
                }
                hs.xs.push(curve.map(v => v.x)); hs.ys.push(curve.map(v => v.y));
                hs.f1.push(p.name1); hs.f2.push(p.name2); hs.b1.push(n1.md5.slice(0,8)); hs.b2.push(n2.md5.slice(0,8));
                hs.score.push(p.score.toFixed(4)); hs.c1.push(col1); hs.c2.push(col2); hs.id1.push(p.id1); hs.id2.push(p.id2);
            });
    
            const rs = { start: [], end: [], color: [], name: [], md5: [], inner_r: [], outer_r: [] };
            const ls = { x: [], y: [], text: [], color: [] };
            bin_list.forEach(md5 => {
                const md5_nodes = unique_nodes.map((n, i) => ({ n, i })).filter(item => item.n.md5 === md5);
                if (md5_nodes.length > 0) {
                    const start_idx = md5_nodes[0].i; const end_idx = md5_nodes[md5_nodes.length - 1].i;
                    const col = bin_colors.get(md5);
                    rs.start.push(start_idx * theta_step - theta_step/2); rs.end.push(end_idx * theta_step + theta_step/2);
                    rs.color.push(col); rs.name.push(md5.slice(0, 8)); rs.md5.push(md5);
                    rs.inner_r.push(ring_inner); rs.outer_r.push(ring_outer);
                    const mid_angle = (start_idx + end_idx) * theta_step / 2;
                    ls.x.push(label_r * Math.cos(mid_angle)); ls.y.push(label_r * Math.sin(mid_angle));
                    ls.text.push(md5.slice(0, 8)); ls.color.push(col);
                }
            });
    
            node_source.data = ns; node_source.change.emit();
            seg_source.data = ss; seg_source.change.emit();
            hit_source.data = hs; hit_source.change.emit();
            ring_source.data = rs; ring_source.change.emit();
            label_source.data = ls; label_source.change.emit();
        }
    
        function fetchBatch(offset) {
            if (signal.aborted) return;
            const BATCH_SIZE = Math.min(MAX_TOTAL - all_pairs.length, 500);
            if (BATCH_SIZE <= 0) return;
            const batch_url = base_url + "&limit=" + BATCH_SIZE + "&offset=" + offset;
            console.log("Streaming Batch " + offset + " (size=" + BATCH_SIZE + ")...");
            fetch(batch_url, { signal }).then(r => r.json()).then(data => {
                if (signal.aborted) return;
                const pairs = data.pairs || [];
                if (pairs.length === 0 && offset === 0) { 
                    console.warn("No data returned from API for offset 0.");
                    document.getElementById('loading-overlay').innerHTML = `
                         <div style="color:#ff5555; text-align:center;">
                            <div style="font-size:24px; margin-bottom:10px;">⚠️ No Similarities Found</div>
                            <div style="font-size:14px; color:#aaa;">Try lowering the threshold or checking the binary filters.</div>
                         </div>
                    `;
                    setTimeout(() => overlay.style.display = 'none', 4000); return; 
                }
                if (pairs.length === 0) {
                    console.log("Stream complete (empty batch at " + offset + ")");
                    overlay.style.display = 'none'; return;
                }

                all_pairs = all_pairs.concat(pairs);
                const overlay = document.getElementById('loading-overlay');
                if (offset === 0) {
                    Object.assign(overlay.style, { background: 'rgba(18, 18, 18, 0.95)', height: 'auto', width: 'auto', top: '10px', right: '10px', left: 'auto', padding: '12px 20px', borderRadius: '8px', border: '1px solid #333', backdropFilter: 'blur(8px)', zIndex: '9999' });
                    overlay.innerHTML = `
                        <div style="display:flex; align-items:center; gap:12px;">
                            <div style="border: 3px solid #333; border-top: 3px solid #66d9ef; border-radius: 50%; width: 22px; height: 22px; animation: spin 1s linear infinite;"></div>
                            <div id="stream-text" style="color:#66d9ef; font-family:'Segoe UI', monospace; font-size:14px; min-width:140px; font-weight:bold;">Streaming: ${all_pairs.length}...</div>
                            <button id="cancel-load-btn" style="background:rgba(255,85,85,0.15); color:#ff5555; border:1px solid #ff5555; border-radius:4px; padding:3px 10px; font-size:11px; cursor:pointer; font-weight:bold; transition:all 0.2s; text-transform:uppercase; letter-spacing:0.5px;">Stop</button>
                        </div>`;
                    const stop = document.getElementById('cancel-load-btn');
                    if (stop) {
                        stop.onmouseover = () => stop.style.background = 'rgba(255,85,85,0.3)';
                        stop.onmouseout = () => stop.style.background = 'rgba(255,85,85,0.15)';
                        stop.onclick = () => {
                             console.log("STREAMING CANCELLED BY USER");
                             if (window.search_abort) window.search_abort.abort();
                             overlay.style.display = 'none';
                        };
                    }
                } else {
                    const text = document.getElementById('stream-text'); if (text) text.innerText = `Streaming: ${all_pairs.length}...`;
                }

                pairs.forEach(p => {
                    [ {id: p.id1, name: p.name1, meta: p.meta1}, {id: p.id2, name: p.name2, meta: p.meta2} ].forEach(n => {
                        if (!nodes_map.has(n.id)) {
                            const node_obj = { id: n.id, name: n.name, md5: n.meta.file_md5, addr: n.id.split(':').pop(), v_size: n.meta.bsim_features_count };
                            nodes_map.set(n.id, node_obj); unique_nodes.push(node_obj); binary_md5s.add(n.meta.file_md5);
                        }
                    });
                });
                updateSources();
                if (pairs.length === BATCH_SIZE && all_pairs.length < MAX_TOTAL) { setTimeout(() => fetchBatch(offset + BATCH_SIZE), 20); }
                else { 
                    console.log("Streaming finished. Total records: " + all_pairs.length);
                    overlay.style.display = 'none'; 
                }
            }).catch(err => { 
                if (err.name !== 'AbortError') console.error("Fetch Error:", err); 
                overlay.style.display = 'none'; 
            });
        }
        fetchBatch(0);
    """)
    
    LAZY_LOAD_CODE_JS = CustomJS(args=dict(
        node_source=node_source, hit_source=hit_source
    ), code="""
        const node_inds = node_source.selected.indices;
        const hit_inds = hit_source.selected.indices;
        if (node_inds.length > 0) {
            const id = node_source.data.id[node_inds[0]];
            const name = node_source.data.name[node_inds[0]];
            if (window.parent && window.parent.showFunctionCodeById) window.parent.showFunctionCodeById(id, name);
        } else if (hit_inds.length > 0) {
            const id1 = hit_source.data.id1[hit_inds[0]];
            const id2 = hit_source.data.id2[hit_inds[0]];
            const name1 = hit_source.data.f1[hit_inds[0]];
            const name2 = hit_source.data.f2[hit_inds[0]];
            if (window.parent && window.parent.openDiffDirectly) window.parent.openDiffDirectly(id1, name1, id2, name2);
        }
    """)
    
    plot = figure(width=1000, height=1000, match_aspect=True, title=None, toolbar_location="above")
    plot.axis.visible = False; plot.grid.grid_line_color = None
    plot.background_fill_color = "#121212"; plot.border_fill_color = "#121212"
    
    r_ring = plot.annular_wedge(x=0, y=0, inner_radius='inner_r', outer_radius='outer_r', start_angle='start', end_angle='end', color='color', alpha=0.3, source=ring_source)
    plot.segment(x0='x0', y0='y0', x1='x1', y1='y1', color='color', line_alpha='alpha', line_width='width', source=seg_source)
    r_links = plot.multi_line(xs='xs', ys='ys', line_width=15, line_alpha=0, hover_line_alpha=0.6, hover_line_color="white", source=hit_source)
    r_nodes = plot.circle('x', 'y', size=11, color='color', line_color="white", source=node_source)
    plot.add_layout(LabelSet(x='x', y='y', text='text', text_color='color', text_font_style='bold', text_align='center', source=label_source))
    
    plot.add_tools(
        HoverTool(renderers=[r_ring], tooltips='<div style="padding:10px; background:#1A1A1A; color:#FFF; border:1px solid #444;"><b>BINARY: @name</b><br><small>MD5: @md5</small></div>'),
        HoverTool(renderers=[r_links], tooltips='<div style="padding:10px; background:#121212; color:#D4D4D4; border:1px solid #444;"><div style="color:#A6E22E; font-weight:bold;">@score MATCH</div><div>@f1 (@b1)<br>@f2 (@b2)</div></div>'),
        HoverTool(renderers=[r_nodes], tooltips='<div style="padding:10px; background:#1A1A1A; color:#D4D4D4; border:2px solid @color;"><b>@name</b><br>Addr: @addr | Features: @v_size</div>')
    )
    
    plot.add_tools(TapTool(renderers=[r_links, r_nodes]))
    node_source.selected.js_on_change('indices', LAZY_LOAD_CODE_JS)
    hit_source.selected.js_on_change('indices', LAZY_LOAD_CODE_JS)
    
    # Multi-trigger logic with Run-Once guard
    UPDATE_GRAPH_JS.code = "if (window.already_loading) return; window.already_loading = true; " + UPDATE_GRAPH_JS.code
    
    # Visible Load Button (can always be used to FORCE a reload)
    refresh_btn = Button(label="↺ Refresh Map", button_type="success", width=120)
    refresh_btn.js_on_click(CustomJS(code="window.already_loading=false;")) # Reset guard for manual refresh
    refresh_btn.js_on_click(UPDATE_GRAPH_JS)
    
    # Dual automatic triggers
    plot.js_on_event(DocumentReady, UPDATE_GRAPH_JS)
    plot.js_on_change('inner_width', UPDATE_GRAPH_JS) # inner_width changes from 0 when rendered
    
    # Simple foolproof timer as secondary fallback
    plot.js_on_event(DocumentReady, CustomJS(code="setTimeout(() => { if (!window.already_loading) console.log('Timer triggering fallback...'); }, 500);"))
    plot.js_on_event(DocumentReady, CustomJS(code="setTimeout(() => { if (!window.already_loading) { let b=document.querySelector('button'); if(b) b.click(); } }, 1000);"))

    layout = Column(plot, refresh_btn)
    return file_html(layout, CDN, "BSim Similarity Map")
