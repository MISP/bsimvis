// Binary Cluster Hierarchy (Dendrogram) and Packing Visualizations for BSimVis

function getCurrentCollection() {
    // 1. Try parent hash params first (most reliable for views)
    if (window.parent && window.parent.location) {
        const hash = window.parent.location.hash || '';
        const params = new URLSearchParams(hash.split('?')[1] || '');
        if (params.get('collection')) return params.get('collection');
        const id = params.get('id') || params.get('id1') || params.get('id2');
        if (id && id.includes(':')) return id.split(':')[0];
    }

    // 2. Try URL params
    const params = new URLSearchParams(window.location.search);
    if (params.get('collection')) return params.get('collection');

    // 4. Try parsing from function IDs
    const id = params.get('id') || params.get('id1') || params.get('id2') || window.currentFuncId;
    if (id && id.includes(':')) return id.split(':')[0];

    return 'main';
}

function getBinHierarchyTooltip() {
    let el = document.getElementById('bin-hierarchy-tooltip');
    if (!el) {
        el = document.createElement('div');
        el.id = 'bin-hierarchy-tooltip';
        el.style.cssText = "position:fixed; z-index:20003; background:rgba(13,15,20,0.98); border-radius:8px; border:1px solid var(--accent,#66d9ef); display:none; pointer-events:auto; font-size:0.8rem; box-shadow:0 15px 50px rgba(0,0,0,0.9); max-width:none; backdrop-filter:blur(15px); overflow:hidden;";
        document.body.appendChild(el);
        
        el.addEventListener('click', (event) => {
            const item = event.target.closest('.hier-binary-item');
            if (item) {
                const idx = parseInt(item.getAttribute('data-index'));
                const activeInstance = (window.binHierarchyInstance && window.binHierarchyInstance._activeD)
                    ? window.binHierarchyInstance
                    : ((window.binPackingInstance && window.binPackingInstance._activeD) ? window.binPackingInstance : null);
                
                if (activeInstance && activeInstance._activeD) {
                    const d = activeInstance._activeD;
                    const members = d.data.runtime_members || [];
                    const file = members[idx];
                    if (file && file.file_id) {
                        const url = `/static/bin_sim/index.html?collection=${encodeURIComponent(getCurrentCollection())}&md5_a=${encodeURIComponent(file.file_md5)}`;
                        window.open(url, '_blank');
                    }
                }
            }
        });
    }
    return el;
}

function loadBinHierarchyView(params) {
    if (!window.binHierarchyInstance) {
        window.binHierarchyInstance = new BinClusterHierarchy('hierarchy-view-container');
    }
    window.binHierarchyInstance.fetch(params);
}

function loadBinPackingView(params) {
    if (!window.binPackingInstance) {
        window.binPackingInstance = new BinClusterPacking('packing-view-container');
    }
    window.binPackingInstance.fetch(params);
}

class BinClusterHierarchy {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.width = this.container ? this.container.clientWidth : 800;
        this.height = this.container ? (this.container.clientHeight || 700) : 700;
        this.root = null;
        this.svg = null;
        this.g = null;
        this.zoom = null;
        this.params = {
            min_cluster_size: 2,
            max_cluster_size: 0,
            stability_threshold: 0.0,
            cohesion_min: 0.0,
            cohesion_max: 0.0,
            show_parents: false,
            show_children: false,
            path_compression: true,
            q: ''
        };
        this.abortController = null;
    }

    stop() {
        if (this.abortController) this.abortController.abort();
    }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        const collection = params.get('collection') || getCurrentCollection();
        const algo = params.get('algo') || 'unweighted_cosine';

        // Sync from URL params
        this.params.min_cluster_size = params.has('min_count') ? (parseInt(params.get('min_count')) || 0) : 0;
        this.params.max_cluster_size = params.has('max_count') ? (parseInt(params.get('max_count')) || 0) : 0;
        this.params.cohesion_min = params.has('min_cohesion') ? (parseFloat(params.get('min_cohesion')) || 0) : 0;
        this.params.cohesion_max = params.has('max_cohesion') ? (parseFloat(params.get('max_cohesion')) || 0) : 0;
        this.params.stability_threshold = params.has('min_stability') ? (parseFloat(params.get('min_stability')) || 0) : 0;
        this.params.show_parents = params.get('show_parents') === 'true';
        this.params.show_children = params.get('show_children') === 'true';
        this.params.q = params.get('q') || '';

        const hierControls = `
            <div style="position:absolute; top:20px; left:20px; z-index:10; background:rgba(0,0,0,0.85); padding:15px; border-radius:8px; border:1px solid #333; width:240px; backdrop-filter:blur(10px);">
                <div style="font-size:0.7rem; color:var(--accent); text-transform:uppercase; letter-spacing:1px; font-weight:bold; margin-bottom:15px;">Tree Filters</div>
                
                <!-- Search -->
                <div style="margin-bottom:20px;">
                    <div class="search-input-wrapper" style="width:100%; background:rgba(255,255,255,0.05); border:1px solid #444;">
                        <i class="fa-solid fa-search" style="font-size:0.7rem; color:#666;"></i>
                        <input type="text" id="hier-search-input" placeholder="Search clusters..." value="${this.params.q}" style="color:#fff !important; font-size:0.75rem !important; padding:4px 8px !important;">
                    </div>
                </div>

                <!-- Size Range -->
                <div style="margin-bottom:20px;">
                    <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                        <label style="font-size:0.75rem; color:#aaa;">Cluster Size</label>
                        <span style="font-size:0.75rem; color:var(--accent); font-family:monospace; font-weight:bold;">
                            <span id="val-min-size">${this.params.min_cluster_size || 2}</span>-<span id="val-max-size">${this.params.max_cluster_size || '∞'}</span>
                        </span>
                    </div>
                    <div class="range-slider-container">
                        <div id="size-track" class="slider-track"></div>
                        <input type="range" id="input-min-size" min="2" max="100" value="${this.params.min_cluster_size || 2}">
                        <input type="range" id="input-max-size" min="2" max="100" value="${this.params.max_cluster_size || 100}">
                    </div>
                </div>

                <!-- Cohesion Range -->
                <div style="margin-bottom:20px;">
                    <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                        <label style="font-size:0.75rem; color:#aaa;">Cohesion %</label>
                        <span style="font-size:0.75rem; color:var(--success); font-family:monospace; font-weight:bold;">
                            <span id="val-coh-min">${(this.params.cohesion_min * 100).toFixed(0)}</span>-<span id="val-coh-max">${(this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100')}</span>%
                        </span>
                    </div>
                    <div class="range-slider-container">
                        <div id="coh-track" class="slider-track"></div>
                        <input type="range" id="input-coh-min" min="0" max="1" step="0.01" value="${this.params.cohesion_min || 0}">
                        <input type="range" id="input-coh-max" min="0" max="1" step="0.01" value="${this.params.cohesion_max || 1}">
                    </div>
                </div>

                <!-- Stability Range -->
                <div style="margin-bottom:20px;">
                    <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                        <label style="font-size:0.75rem; color:#aaa;">Stability Cut</label>
                        <span style="font-size:0.75rem; color:#66d9ef; font-family:monospace; font-weight:bold;">
                            <span id="val-stab-min">${this.params.stability_threshold.toFixed(1)}</span>+
                        </span>
                    </div>
                    <div class="range-slider-container">
                        <div id="stab-track" class="slider-track" style="width:100%; left:0%;"></div>
                        <input type="range" id="input-stab-min" min="0" max="10" step="0.1" value="${this.params.stability_threshold || 0}" style="z-index: 3;">
                    </div>
                </div>

                <!-- Checkboxes -->
                <div style="margin-bottom:20px; display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" id="input-show-parents" ${this.params.show_parents ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                        <label for="input-show-parents" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show parents</label>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" id="input-show-children" ${this.params.show_children ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                        <label for="input-show-children" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show children</label>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" id="input-path-compression" ${this.params.path_compression !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                        <label for="input-path-compression" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Path compression</label>
                    </div>
                </div>

                <button id="hier-refresh-btn" class="btn-primary" style="padding:10px; font-size:0.75rem; width:100%; margin-top:5px; text-transform:uppercase; letter-spacing:1.5px; font-weight:bold;">Update Visualization</button>
            </div>

            <div id="hierarchy-loader" style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:var(--accent); background:#0d0f14;">
                <div style="text-align:center;">
                    <div class="spinner" style="margin-bottom:15px;"></div>
                    <div style="font-size:0.9rem; letter-spacing:1px;">Rebuilding Dendrogram...</div>
                </div>
            </div>
        `;

        this.container.innerHTML = hierControls;

        const updateTrack = (idPrefix, minVal, maxVal, minLimit, maxLimit) => {
            const minPercent = ((minVal - minLimit) / (maxLimit - minLimit)) * 100;
            const maxPercent = ((maxVal - minLimit) / (maxLimit - minLimit)) * 100;
            const track = document.getElementById(`${idPrefix}-track`);
            if (track) {
                track.style.left = minPercent + "%";
                track.style.width = (maxPercent - minPercent) + "%";
            }
        };

        // Wire up controls
        const sMin = document.getElementById('input-min-size');
        const sMax = document.getElementById('input-max-size');
        const sUpdate = () => {
            if (parseInt(sMin.value) > parseInt(sMax.value)) sMin.value = sMax.value;
            this.params.min_cluster_size = parseInt(sMin.value);
            this.params.max_cluster_size = parseInt(sMax.value) === 100 ? 0 : parseInt(sMax.value);
            document.getElementById('val-min-size').innerText = this.params.min_cluster_size;
            document.getElementById('val-max-size').innerText = this.params.max_cluster_size || '∞';
            updateTrack('size', parseInt(sMin.value), parseInt(sMax.value), 2, 100);
        };
        sMin.oninput = sUpdate;
        sMax.oninput = sUpdate;
        updateTrack('size', this.params.min_cluster_size || 2, this.params.max_cluster_size || 100, 2, 100);

        const cMinE = document.getElementById('input-coh-min');
        const cMaxE = document.getElementById('input-coh-max');
        const cUpdate = () => {
            if (parseFloat(cMinE.value) > parseFloat(cMaxE.value)) cMinE.value = cMaxE.value;
            this.params.cohesion_min = parseFloat(cMinE.value);
            this.params.cohesion_max = parseFloat(cMaxE.value) >= 1 ? 0 : parseFloat(cMaxE.value);
            document.getElementById('val-coh-min').innerText = (this.params.cohesion_min * 100).toFixed(0);
            document.getElementById('val-coh-max').innerText = this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100';
            updateTrack('coh', this.params.cohesion_min, parseFloat(cMaxE.value), 0, 1);
        };
        cMinE.oninput = cUpdate;
        cMaxE.oninput = cUpdate;
        updateTrack('coh', this.params.cohesion_min, this.params.cohesion_max || 1, 0, 1);

        const stMin = document.getElementById('input-stab-min');
        const stUpdate = () => {
            this.params.stability_threshold = parseFloat(stMin.value);
            document.getElementById('val-stab-min').innerText = this.params.stability_threshold.toFixed(1) + '+';
            updateTrack('stab', this.params.stability_threshold, 10, 0, 10);
        };
        stMin.oninput = stUpdate;
        updateTrack('stab', this.params.stability_threshold, 10, 0, 10);

        const spCheck = document.getElementById('input-show-parents');
        spCheck.onchange = () => { this.params.show_parents = spCheck.checked; };
        const scCheck = document.getElementById('input-show-children');
        scCheck.onchange = () => { this.params.show_children = scCheck.checked; };
        const pcCheck = document.getElementById('input-path-compression');
        pcCheck.onchange = () => { this.params.path_compression = pcCheck.checked; };

        document.getElementById('hier-refresh-btn').onclick = () => {
            const hash = window.location.hash;
            const [path, qs] = hash.split('?');
            const p = new URLSearchParams(qs || '');
            const searchVal = document.getElementById('hier-search-input').value.trim();
            
            if (this.params.min_cluster_size > 0) p.set('min_count', this.params.min_cluster_size); else p.delete('min_count');
            if (this.params.max_cluster_size > 0) p.set('max_count', this.params.max_cluster_size); else p.delete('max_count');
            if (this.params.cohesion_min > 0) p.set('min_cohesion', this.params.cohesion_min); else p.delete('min_cohesion');
            if (this.params.cohesion_max > 0) p.set('max_cohesion', this.params.cohesion_max); else p.delete('max_cohesion');
            if (this.params.stability_threshold > 0) p.set('min_stability', this.params.stability_threshold); else p.delete('min_stability');
            if (searchVal) p.set('q', searchVal); else p.delete('q');
            p.set('show_parents', this.params.show_parents);
            p.set('show_children', this.params.show_children);
            
            window.location.hash = `${path}?${p.toString()}`;
        };

        try {
            const queryParams = new URLSearchParams(params.toString());
            queryParams.set('limit', 10000);
            if (this.params.min_cluster_size > 0) queryParams.set('min_count', this.params.min_cluster_size);
            if (this.params.max_cluster_size > 0) queryParams.set('max_count', this.params.max_cluster_size);
            if (this.params.cohesion_min > 0) queryParams.set('min_cohesion', this.params.cohesion_min);
            if (this.params.cohesion_max > 0) queryParams.set('max_cohesion', this.params.cohesion_max);
            if (this.params.stability_threshold > 0) queryParams.set('min_stability', this.params.stability_threshold);
            if (this.params.q) queryParams.set('q', this.params.q);
            queryParams.set('show_parents', this.params.show_parents !== false);
            queryParams.set('show_children', this.params.show_children !== false);

            const url = `/api/bin_cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Cluster data not found");
            const data = await res.json();

            const nodes = (data.results || []).map(m => ({
                id: String(m.cluster_id),
                parent: m.parent ? String(m.parent) : null,
                name: m.cluster_name || `Cluster ${m.cluster_id}`,
                uuid: m.cluster_uuid,
                size: m.count || 0,
                stability: m.avg_stability || 0.0,
                cohesion: m.cohesion_score || 0.0,
                snippet: m.snippet || "",
                members: m.sample_members || []
            }));

            if (!nodes || nodes.length === 0) {
                this.container.innerHTML += `<div style="position:absolute; top:50%; left:50%; transform:translate(-50%, -50%); color:#aaa; text-align:center; width:100%;">No binary clusters match these criteria.</div>`;
                const loader = document.getElementById('hierarchy-loader');
                if (loader) loader.remove();
                return;
            }

            this.render(nodes);
        } catch (e) {
            if (e.name === 'AbortError') return;
            this.container.innerHTML = `<div style="margin:auto; color:var(--error); text-align:center;">Error loading hierarchy: ${e.message}</div>`;
        }
    }

    render(nodes) {
        const self = this;
        const loader = document.getElementById('hierarchy-loader');
        if (loader) loader.remove();

        if (this.params.path_compression !== false) {
            let compressedNodes = JSON.parse(JSON.stringify(nodes));
            let changed = true;
            while (changed) {
                changed = false;
                const childCounts = {};
                compressedNodes.forEach(n => { if (n.parent) childCounts[n.parent] = (childCounts[n.parent] || 0) + 1; });
                for (let i = 0; i < compressedNodes.length; i++) {
                    const node = compressedNodes[i];
                    const count = childCounts[node.id] || 0;
                    if (count === 1 && node.parent !== null) {
                        const child = compressedNodes.find(n => n.parent === node.id);
                        if (child) { child.parent = node.parent; compressedNodes.splice(i, 1); changed = true; break; }
                    }
                }
            }
            nodes = compressedNodes;
        }

        const width = this.container.clientWidth;
        const height = this.container.clientHeight || 700;
        d3.select(this.container).selectAll("svg").remove();

        this.svg = d3.select(this.container).append("svg")
            .attr("viewBox", `0 0 ${width} ${height}`)
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("style", "background:#0d0f14; cursor:grab;");

        this.g = this.svg.append("g");
        this.zoom = d3.zoom().scaleExtent([0.05, 10]).on("zoom", (e) => this.g.attr("transform", e.transform));
        this.svg.call(this.zoom);

        const stratify = d3.stratify().id(d => d.id).parentId(d => d.parent);
        nodes.forEach(n => { if (n.parent && !nodes.find(p => p.id === n.parent)) n.parent = null; });
        const rootNodes = nodes.filter(n => !n.parent || !nodes.find(p => p.id === n.parent));

        if (rootNodes.length === 0) {
            this.root = null;
        } else if (rootNodes.length === 1) {
            this.root = stratify(nodes);
        } else {
            const virtualRootId = "VIRTUAL_ROOT";
            const augmentedNodes = [
                { id: virtualRootId, parent: null, name: "All Clusters", uuid: "root", size: nodes.reduce((acc, n) => (!n.parent || !nodes.find(p => p.id === n.parent)) ? acc + n.size : acc, 0), cohesion: 0, members: [] },
                ...nodes.map(n => { if (!n.parent || !nodes.find(p => p.id === n.parent)) return { ...n, parent: virtualRootId }; return n; })
            ];
            this.root = stratify(augmentedNodes);
        }

        const nodeWidth = 240;
        const nodeHeight = 35;
        const treeLayout = d3.cluster().nodeSize([nodeHeight, nodeWidth]);
        treeLayout(this.root);

        const dNodes = this.root.descendants();
        const dLinks = this.root.links();

        const getCohesionColor = (cohesion) => {
            const hue = Math.max(0, Math.min(120, (cohesion || 0) * 120));
            return `hsl(${hue}, 100%, 65%)`;
        };

        const link = this.g.selectAll("path.link").data(dLinks, d => d.target.data.id);
        link.enter().insert("path", "g").attr("class", "link").attr("fill", "none").attr("stroke", "#333").attr("stroke-width", 1.5)
            .merge(link).attr("d", d3.linkHorizontal().x(d => d.y).y(d => d.x));

        const node = this.g.selectAll("g.node").data(dNodes, d => d.data.id);
        const nodeEnter = node.enter().append("g").attr("class", "node").attr("transform", d => `translate(${d.y},${d.x})`).style("cursor", "pointer")
            .on("click", (e, d) => { if (d.data.uuid && d.data.uuid !== 'root') window.location.hash = `#files?collection=${getCurrentCollection()}&bin_cluster_uuid=${d.data.uuid}`; })
            .on("mouseenter", (e, d) => { d3.select(e.currentTarget).select("circle").attr("r", 14); this.showTooltip(e, d); })
            .on("mouseleave", (e, d) => {
                const rt = e.relatedTarget;
                const tt = getBinHierarchyTooltip();
                if (tt && (tt === rt || tt.contains(rt))) return;
                d3.select(e.currentTarget).select("circle").attr("r", 8); this.hideTooltip();
            });

        nodeEnter.append("circle").attr("r", 8).attr("stroke", d => getCohesionColor(d.data.cohesion)).attr("stroke-width", 2).style("fill", d => getCohesionColor(d.data.cohesion));
        nodeEnter.append("text").attr("dy", ".35em").attr("x", d => d.children ? -15 : 15).attr("text-anchor", d => d.children ? "end" : "start").style("fill", "#fff").style("font-size", "12px").style("pointer-events", "none")
            .text(d => d.data.name).clone(true).lower().attr("stroke", "#000").attr("stroke-width", 3);

        node.merge(nodeEnter).attr("transform", d => `translate(${d.y},${d.x})`);
        
        const initialTransform = d3.zoomIdentity.translate(80, height / 2).scale(0.6);
        this.svg.call(this.zoom.transform, initialTransform);
    }

    async showTooltip(event, d) {
        this._activeD = d;
        this._hoveredNodeEl = event.currentTarget;
        const tooltip = getBinHierarchyTooltip();
        tooltip.style.display = 'block';
        let x = event.clientX + 20;
        let y = event.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = event.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = event.clientY - rect.height - 20;
        tooltip.style.left = x + 'px'; tooltip.style.top = y + 'px';

        tooltip.onmouseleave = (e) => {
            const rt = e.relatedTarget;
            if (this._hoveredNodeEl && (this._hoveredNodeEl === rt || this._hoveredNodeEl.contains(rt))) return;
            this.hideTooltip();
            if (this._hoveredNodeEl) { d3.select(this._hoveredNodeEl).select("circle").attr("r", 8); this._hoveredNodeEl = null; }
        };

        this.renderTooltip(tooltip, d);

        if (!d.data.runtime_members && d.data.uuid && d.data.uuid !== 'root') {
            try {
                const col = getCurrentCollection();
                const res = await fetch(`/api/bin_cluster/files?collection=${col}&cluster_uuid=${d.data.uuid}&limit=100`);
                const data = await res.json();
                d.data.runtime_members = data.files;
                this.renderTooltip(tooltip, d);
            } catch (e) { console.error("Failed to fetch runtime members", e); }
        }
    }

    renderTooltip(tooltip, d) {
        const members = d.data.runtime_members || [];
        const isLoading = !d.data.runtime_members && d.data.uuid !== 'root';
        
        tooltip.innerHTML = `
            <div style="display:flex; flex-direction:row; min-width:450px; height:320px; background:#0d0f14;">
                <div style="flex:1; padding:15px; border-right:1px solid #333; display:flex; flex-direction:column;">
                    <div style="color:var(--accent); font-weight:bold; margin-bottom:4px; font-size:0.95rem;">${d.data.name}</div>
                    <div style="color:#666; font-size:0.65rem; margin-bottom:10px; font-family:monospace; overflow:hidden; text-overflow:ellipsis;">${d.data.uuid}</div>
                    
                    <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                        <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid #555;">
                            <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                            <div style="color:#eee; font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:#666; font-size:0.65rem;">bins</span></div>
                        </div>
                        <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--accent);">
                            <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                            <div style="color:var(--accent); font-weight:bold;">${d.data.stability.toFixed(2)}</div>
                        </div>
                        <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--success);">
                            <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                            <div style="color:var(--success); font-weight:bold;">${(d.data.cohesion * 100).toFixed(1)}%</div>
                        </div>
                    </div>

                    <div style="border-top:1px solid #333; padding-top:10px; flex:1; overflow-y:auto;">
                        <div style="font-size:0.6rem; color:#555; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px;">
                            ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Fetching Live Samples...' : `Samples (${members.length}):`}
                        </div>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            ${members.map((m, i) => `
                                <div class="hier-binary-item" data-index="${i}" style="padding:4px 8px; border-radius:4px; background:rgba(255,255,255,0.02); display:flex; justify-content:space-between; align-items:center; cursor:pointer;">
                                    <span style="color:var(--accent); font-weight:bold; font-size:0.75rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${m.file_name}</span>
                                    <span style="color:#666; font-size:0.65rem; font-family:monospace;">${(m.file_md5 || '').substring(0, 8)}</span>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                </div>
                <div style="width:180px; padding:15px; background:rgba(0,0,0,0.2); display:flex; flex-direction:column; gap:10px;">
                    <div style="font-size:0.6rem; color:#555; text-transform:uppercase;">Quick Stats</div>
                    ${d.data.snippet ? `<div style="font-size:0.7rem; color:#aaa; font-style:italic;">"${d.data.snippet}"</div>` : ''}
                    <div style="font-size:0.7rem; color:#777;">
                        Architecture: <span style="color:#aaa;">${members[0]?.architecture || 'N/A'}</span><br>
                        Language: <span style="color:#aaa;">${members[0]?.language_id || 'N/A'}</span>
                    </div>
                </div>
            </div>
        `;
    }

    hideTooltip() { this._activeD = null; const el = getBinHierarchyTooltip(); if (el) el.style.display = 'none'; }
}

class BinClusterPacking {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.width = this.container ? this.container.clientWidth : 800;
        this.height = this.container ? (this.container.clientHeight || 700) : 700;
        this.root = null;
        this.svg = null;
        this.params = { min_cluster_size: 2, stability_threshold: 0.0, show_parents: true, path_compression: true };
        this.abortController = null;
    }

    stop() { if (this.abortController) this.abortController.abort(); }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        this.params.min_cluster_size = params.has('min_count') ? (parseInt(params.get('min_count')) || 0) : 0;
        this.params.stability_threshold = params.has('min_stability') ? (parseFloat(params.get('min_stability')) || 0) : 0;

        this.container.innerHTML = `
            <div id="packing-loader" style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:var(--accent); background:#0d0f14;">
                <div style="text-align:center;">
                    <div class="spinner" style="margin-bottom:15px;"></div>
                    <div style="font-size:0.9rem; letter-spacing:1px;">Packing Binary Clusters...</div>
                </div>
            </div>
        `;

        try {
            const queryParams = new URLSearchParams(params.toString());
            queryParams.set('limit', 10000);
            if (this.params.min_cluster_size > 0) queryParams.set('min_count', this.params.min_cluster_size);
            if (this.params.stability_threshold > 0) queryParams.set('min_stability', this.params.stability_threshold);

            const url = `/api/bin_cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Cluster data not found");
            const data = await res.json();

            const nodes = (data.results || []).map(m => ({
                id: String(m.cluster_id),
                parent: m.parent ? String(m.parent) : null,
                name: m.cluster_name || `Cluster ${m.cluster_id}`,
                uuid: m.cluster_uuid,
                size: m.count || 0,
                stability: m.avg_stability || 0.0,
                cohesion: m.cohesion_score || 0.0,
                snippet: m.snippet || "",
                members: m.sample_members || []
            }));

            if (!nodes || nodes.length === 0) {
                this.container.innerHTML = `<div style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:#aaa;">No binary clusters match these criteria.</div>`;
                return;
            }

            const stratify = d3.stratify().id(d => d.id).parentId(d => d.parent);
            this.root = stratify(nodes).sum(d => d.size).sort((a, b) => b.value - a.value);
            this.render();
        } catch (err) {
            if (err.name === 'AbortError') return;
            this.container.innerHTML = `<div style="margin:auto; color:var(--error); text-align:center;">Error loading packing: ${err.message}</div>`;
        }
    }

    render() {
        const width = this.container.clientWidth;
        const height = this.container.clientHeight || 700;
        d3.select(this.container).selectAll("svg").remove();
        if (document.getElementById('packing-loader')) document.getElementById('packing-loader').remove();

        const pack = d3.pack().size([width, height]).padding(3);
        pack(this.root);

        const svg = d3.select(this.container).append("svg").attr("width", width).attr("height", height).attr("viewBox", [0, 0, width, height]);
        this.svg = svg;

        const getCohesionColor = (cohesion) => {
            const hue = Math.max(0, Math.min(120, (cohesion || 0) * 120));
            return `hsl(${hue}, 100%, 65%)`;
        };

        const node = svg.append("g").selectAll("circle").data(this.root.descendants()).join("circle")
            .attr("fill", d => d.children ? "#1a1f29" : getCohesionColor(d.data.cohesion))
            .attr("fill-opacity", d => d.children ? 0.3 : 0.8)
            .attr("stroke", d => d.children ? "rgba(255,255,255,0.1)" : "none")
            .attr("cx", d => d.x).attr("cy", d => d.y).attr("r", d => d.r)
            .style("cursor", "pointer")
            .on("click", (e, d) => { if (d.data.uuid) window.location.hash = `#files?collection=${getCurrentCollection()}&bin_cluster_uuid=${d.data.uuid}`; });

        svg.append("g").style("fill", "#fff").attr("pointer-events", "none").attr("text-anchor", "middle")
            .selectAll("text").data(this.root.descendants().filter(d => d.r > 20)).join("text")
            .attr("x", d => d.x).attr("y", d => d.y)
            .style("font-size", d => Math.min(d.r / 3, 12) + "px")
            .style("font-family", "monospace").style("font-weight", "bold")
            .style("display", d => d.parent === this.root || !d.children ? "inline" : "none")
            .text(d => d.data.name);
    }
}
