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

    applyTagUpdate(action, etype, eid, tag) {
        if (!this.root) return;
        const mutate = (arr, t, add) => {
            if (add) { if (!arr.includes(t)) arr.push(t); }
            else { const i = arr.indexOf(t); if (i !== -1) arr.splice(i, 1); }
        };
        const add = (action === 'add');
        let updated = false;

        this.root.each(d => {
            if (d.data && etype === 'file' && d.data.runtime_files) {
                d.data.runtime_files.forEach(m => {
                    if (m.id === eid || m.md5 === eid || (m.id && m.id.endsWith(eid))) {
                        m.file_user_tags = m.file_user_tags || [];
                        mutate(m.file_user_tags, tag, add);
                        updated = true;
                    }
                });
            }
        });

        if (updated && this._activeD) {
            const tooltip = getBinHierarchyTooltip();
            if (tooltip && tooltip.style.display === 'block' && this._renderedNodeUuid === this._activeD.data.uuid) {
                this._renderedNodeUuid = null;
                this.renderTooltip(tooltip, this._activeD);
            }
        }
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
        this.params.show_members = params.get('show_members') === 'true';
        this.params.color_by_md5 = params.get('color_by_md5') === 'true';
        this.params.q = params.get('q') || '';

        const hierControls = `
            <div style="position:absolute; top:20px; left:20px; z-index:10; background:rgba(0,0,0,0.85); padding:15px; border-radius:8px; border:1px solid #333; width:240px; backdrop-filter:blur(10px);">
                <div style="font-size:0.85rem; color:#fff; font-weight:bold; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:5px;">Tree Filters</div>
                
                <!-- Search -->
                <div style="margin-bottom:15px;">
                    <div class="search-input-wrapper" style="width:100%; background:rgba(255,255,255,0.05); border:1px solid #444;">
                        <i class="fa-solid fa-search" style="font-size:0.7rem; color:#666;"></i>
                        <input type="text" id="hier-search-input" placeholder="Search clusters..." value="${this.params.q}" style="color:#fff !important; font-size:0.75rem !important; padding:4px 8px !important;">
                    </div>
                </div>

                <div class="filter-category-header" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Range Filters</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content">
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
                </div>

                <div class="filter-category-header collapsed" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Display Settings</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content collapsed">
                    <!-- Checkboxes -->
                    <div style="display:flex; flex-direction:column; gap:8px;">
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
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-show-members" ${this.params.show_members ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-show-members" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show members</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-color-by-md5" ${this.params.color_by_md5 ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-color-by-md5" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Color by MD5</label>
                        </div>
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
        
        const smCheck = document.getElementById('input-show-members');
        if (smCheck) {
            smCheck.onchange = () => { this.params.show_members = smCheck.checked; };
        }
        const md5Check = document.getElementById('input-color-by-md5');
        if (md5Check) {
            md5Check.onchange = () => { this.params.color_by_md5 = md5Check.checked; };
        }

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
            p.set('show_members', this.params.show_members ? 'true' : 'false');
            p.set('color_by_md5', this.params.color_by_md5 ? 'true' : 'false');
            
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
            queryParams.set('show_members', this.params.show_members === true);

            const url = `/api/bin_cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Cluster data not found");
            const data = await res.json();
            const nameType = params.get('cluster_name_type') || 'file';

            let nodes = (data.results || []).map(m => {
                let displayName = m.cluster_name || `Cluster ${m.cluster_id}`;
                if (nameType === 'yara' && !m.is_custom_name && m.yara_distribution && m.yara_distribution.length > 0) {
                    displayName = m.yara_distribution[0].value;
                }
                return {
                    id: String(m.cluster_id),
                    parent: m.parent ? String(m.parent) : null,
                    name: displayName,
                    uuid: m.cluster_uuid,
                    size: m.count || 0,
                    stability: m.avg_stability || 0.0,
                    cohesion: m.cohesion_score || 0.0,
                    snippet: m.snippet || "",
                    members: m.sample_members || [],
                    direct_members: m.direct_members || []
                };
            });

            if (this.params.show_members) {
                const memberNodes = [];
                nodes.forEach(c => {
                    if (c.direct_members && c.direct_members.length > 0) {
                        c.direct_members.forEach(m => {
                            memberNodes.push({
                                id: m.id,
                                parent: String(c.id),
                                name: m.name,
                                file_md5: m.file_md5,
                                language_id: m.language_id,
                                function_count: m.function_count,
                                tags: m.tags,
                                user_tags: m.user_tags,
                                is_member: true,
                                size: 1,
                                stability: 0,
                                cohesion: 0,
                                members: [],
                                avtype: m.avtype,
                                filetype: m.filetype,
                                yara: m.yara,
                                cc_ip: m.cc_ip
                            });
                        });
                    }
                });
                nodes = nodes.concat(memberNodes);
            }

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
        this.rawNodes = nodes;
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
            .on("click", (e, d) => {
                const col = getCurrentCollection();
                if (d.data.is_member) {
                    if (d.data.file_md5) {
                        window.location.hash = `#files/sim?collection=${col}&md5=${d.data.file_md5}`;
                    } else {
                        window.location.hash = `#files?collection=${col}&q=${encodeURIComponent(d.data.name)}`;
                    }
                    return;
                }
                if (d.data.uuid && d.data.uuid !== 'root') {
                    window.location.hash = `#files?collection=${col}&bin_cluster_uuid=${d.data.uuid}`;
                }
            })
            .on("mouseenter", (e, d) => {
                if (self.isDragging) return;
                self._hoveredNodeEl = e.currentTarget;
                d3.select(e.currentTarget).select("circle").attr("r", d.data.is_member ? 7 : 14);
                this.showTooltip(e, d);
            })
            .on("mousemove", (e) => {
                if (window.moveCodePreview) window.moveCodePreview(e);
            })
            .on("mouseleave", (e, d) => {
                const rt = e.relatedTarget;
                const tt = getBinHierarchyTooltip();
                if (tt && (tt === rt || tt.contains(rt))) return;
                d3.select(e.currentTarget).select("circle").attr("r", d.data.is_member ? 4 : 8);
                this.hideTooltip();
                this._hoveredNodeEl = null;
            })
            .on("contextmenu", (event, d) => {
                if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return;
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'bin_cluster', d.data);
                }
            });

        nodeEnter.append("circle").attr("r", d => d.data.is_member ? 4 : 8)
            .attr("stroke", d => {
                if (d.data.is_member) {
                    if (this.params.color_by_md5 && d.data.file_md5) {
                        return window.getMd5Color(d.data.file_md5);
                    }
                    return "var(--accent)";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("stroke-width", d => d.data.is_member ? 1.5 : 2)
            .style("fill", d => {
                if (d.data.is_member) {
                    if (this.params.color_by_md5 && d.data.file_md5) {
                        return window.getMd5Color(d.data.file_md5);
                    }
                    return "#0d0f14";
                }
                return getCohesionColor(d.data.cohesion);
            });
        nodeEnter.append("text").attr("dy", ".35em").attr("x", d => d.children ? -15 : 15).attr("text-anchor", d => d.children ? "end" : "start")
            .style("fill", d => d.data.is_member ? "#aaa" : "#fff")
            .style("font-size", d => d.data.is_member ? "10px" : "12px")
            .style("font-style", d => d.data.is_member ? "italic" : "normal")
            .style("pointer-events", "none")
            .text(d => d.data.name).clone(true).lower().attr("stroke", "#000").attr("stroke-width", 3);

        const dragDendro = d3.drag()
            .on("drag", function(event, d) {
                const dx = event.dx;
                const dy = event.dy;
                const descendants = d.descendants();
                descendants.forEach(desc => {
                    desc.y += dx;
                    desc.x += dy;
                });
                self.g.selectAll("g.node").filter(n => descendants.includes(n))
                    .attr("transform", n => `translate(${n.y},${n.x})`);
                self.g.selectAll("path.link").filter(l => descendants.includes(l.source) || descendants.includes(l.target))
                    .attr("d", d3.linkHorizontal().x(l => l.y).y(l => l.x));
            });

        nodeEnter.call(dragDendro);

        const nodeUpdate = node.merge(nodeEnter);
        nodeUpdate.attr("transform", d => `translate(${d.y},${d.x})`);
        nodeUpdate.select("circle")
            .style("fill", d => {
                if (d.data.is_member) {
                    if (this.params.color_by_md5 && d.data.file_md5) {
                        return window.getMd5Color(d.data.file_md5);
                    }
                    return "#0d0f14";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("stroke", d => {
                if (d.data.is_member) {
                    if (this.params.color_by_md5 && d.data.file_md5) {
                        return window.getMd5Color(d.data.file_md5);
                    }
                    return "var(--accent)";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("r", d => d.data.is_member ? 4 : 8);
        
        let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        dNodes.forEach(d => {
            if (d.x < minX) minX = d.x;
            if (d.x > maxX) maxX = d.x;
            if (d.y < minY) minY = d.y;
            if (d.y > maxY) maxY = d.y;
        });
        const dx = maxY - minY;
        const dy = maxX - minX;
        const paddingX = 300; 
        const paddingY = 100;
        const s = Math.min(0.8, width / (dx + paddingX || 1), height / (dy + paddingY || 1));
        const cx = (minY + maxY) / 2;
        const cy = (minX + maxX) / 2;
        const initialTransform = d3.zoomIdentity.translate(width / 2 - cx * s, height / 2 - cy * s).scale(s);
        this.svg.call(this.zoom.transform, initialTransform);
    }

    async showTooltip(event, d) {
        this._activeD = d;
        this._hoveredNodeEl = event.currentTarget;

        if (d.data.is_member) {
            const el = getBinHierarchyTooltip();
            if (el) el.style.display = 'none';
            if (window.showBinaryPreview) {
                window.showBinaryPreview(
                    d.data.file_md5,
                    d.data.name,
                    d.data.function_count,
                    d.data.language_id,
                    d.data.tags,
                    event,
                    d.data.tags,
                    d.data.user_tags,
                    {
                        avtype: d.data.avtype,
                        filetype: d.data.filetype,
                        yara: d.data.yara,
                        cc_ip: d.data.cc_ip
                    }
                );
            }
            return;
        }

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

        if (d.data.scrollOffset === undefined) d.data.scrollOffset = 0;

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
        const scrollOffset = d.data.scrollOffset || 0;
        const selectedFile = members[scrollOffset];

        const isSameNode = tooltip.querySelector('.hier-tooltip-container') && this._renderedNodeUuid === d.data.uuid;
        
        if (!isSameNode) {
            this._renderedNodeUuid = d.data.uuid;
            tooltip.innerHTML = `
                <div class="hier-tooltip-container" style="display:flex; flex-direction:row; min-width:450px; height:320px; background:#0d0f14;">
                    <div class="hier-left-col" style="flex:1; padding:15px; border-right:1px solid #333; display:flex; flex-direction:column;">
                        <div style="color:var(--accent); font-weight:bold; margin-bottom:4px; font-size:0.95rem;">${d.data.name}</div>
                        <div style="color:#666; font-size:0.65rem; margin-bottom:10px; font-family:monospace; overflow:hidden; text-overflow:ellipsis;">${d.data.uuid}</div>
                        
                        <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid #555;">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                                <div style="color:#eee; font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:#666; font-size:0.65rem;">bins</span></div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--accent);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                                <div style="color:var(--accent); font-weight:bold;">${(d.data.stability || 0).toFixed(2)}</div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--success);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                                <div style="color:var(--success); font-weight:bold;">${((d.data.cohesion || 0) * 100).toFixed(1)}%</div>
                            </div>
                        </div>

                        <div style="border-top:1px solid #333; padding-top:10px; flex: 1; display: flex; flex-direction: column; overflow: hidden;">
                            <div class="hier-samples-title" style="font-size:0.6rem; color:#555; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px;">
                                ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Fetching Live Samples...' : `Samples (${members.length}):`}
                            </div>
                            <div class="hier-function-list" style="flex:1; position:relative; overflow:hidden;">
                                <div class="hier-function-list-scroll" style="transition: transform 0.1s cubic-bezier(0.17, 0.67, 0.83, 0.67);">
                                    ${members.map((m, i) => `
                                        <div class="hier-binary-item" data-index="${i}" style="padding:4px 8px; border-radius:4px; background:rgba(255,255,255,0.02); display:flex; justify-content:space-between; align-items:center; cursor:pointer;">
                                            <span class="file-name-span" style="color:#eee; font-weight:bold; font-size:0.75rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${m.file_name}</span>
                                            <span style="color:#666; font-size:0.65rem; font-family:monospace;">${(m.file_md5 || '').substring(0, 8)}</span>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                            ${d.data.size > members.length ? `<div style="color:#444; margin-top:6px; font-size:0.65rem;">... and ${d.data.size - members.length} more</div>` : ''}
                        </div>
                    </div>
                    <div class="hier-right-col" id="bin-hier-snippet-container" style="width:200px; padding:15px; background:rgba(0,0,0,0.2); display:flex; flex-direction:column; gap:10px; overflow-y:auto;">
                        <div class="hier-snippet-placeholder" style="padding: 20px; color: #666; text-align: center; font-size: 0.8rem;">
                            ${selectedFile ? '<i class="fas fa-spinner fa-spin"></i> Loading Preview...' : 'Select a file to preview'}
                        </div>
                    </div>
                </div>
            `;
        } else {
            const samplesTitle = tooltip.querySelector('.hier-samples-title');
            if (samplesTitle) {
                samplesTitle.innerHTML = isLoading ? '<i class="fas fa-spinner fa-spin"></i> Fetching Live Samples...' : `Samples (${members.length}):`;
            }

            const listScroll = tooltip.querySelector('.hier-function-list-scroll');
            if (listScroll && listScroll.children.length === 0 && members.length > 0) {
                listScroll.innerHTML = members.map((m, i) => `
                    <div class="hier-binary-item" data-index="${i}" style="padding:4px 8px; border-radius:4px; background:rgba(255,255,255,0.02); display:flex; justify-content:space-between; align-items:center; cursor:pointer;">
                        <span class="file-name-span" style="color:#eee; font-weight:bold; font-size:0.75rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${m.file_name}</span>
                        <span style="color:#666; font-size:0.65rem; font-family:monospace;">${(m.file_md5 || '').substring(0, 8)}</span>
                    </div>
                `).join('');
            }
        }

        const listScroll = tooltip.querySelector('.hier-function-list-scroll');
        if (listScroll) {
            listScroll.style.transform = `translateY(-${scrollOffset * 28}px)`;
            Array.from(listScroll.children).forEach((itemEl, idx) => {
                const isSelected = idx === scrollOffset;
                itemEl.classList.toggle('selected', isSelected);
                const nameSpan = itemEl.querySelector('.file-name-span');
                if (nameSpan) {
                    nameSpan.style.color = isSelected ? 'var(--accent)' : '#eee';
                }
                itemEl.style.background = isSelected ? 'rgba(255,255,255,0.1)' : 'rgba(255,255,255,0.02)';
            });
        }

        if (selectedFile) {
            this.updateSnippet(selectedFile);
        }
    }

    updateSnippet(file) {
        const container = document.getElementById('bin-hier-snippet-container');
        if (!container || !file) return;

        let yaraHtml = '';
        if (file.yara_matches && file.yara_matches.length > 0) {
            yaraHtml = `
                <div style="margin-top: 8px;">
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Yara Matches</div>
                    <div style="display: flex; flex-direction: column; gap: 2px;">
                        ${file.yara_matches.map(y => `<div class="mono" style="font-size: 0.65rem; color: var(--accent); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${y}">${y}</div>`).join('')}
                    </div>
                </div>
            `;
        } else if (file.yara) {
             yaraHtml = `
                <div style="margin-top: 8px;">
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Yara Match</div>
                    <div class="mono" style="font-size: 0.65rem; color: var(--accent);">${file.yara}</div>
                </div>
            `;
        }

        let ipsHtml = '';
        if (file.ips && file.ips.length > 0) {
            ipsHtml = `
                <div style="margin-top: 8px;">
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">CC IPs</div>
                    <div style="display: flex; flex-direction: column; gap: 2px;">
                        ${file.ips.map(ip => `<div class="mono" style="font-size: 0.65rem; color: var(--info);">${ip}</div>`).join('')}
                    </div>
                </div>
            `;
        }

        let tagsHtml = '';
        const allTags = [...(file.tags || []), ...(file.user_tags || [])].filter(t => t && t.trim());
        if (allTags.length > 0) {
            tagsHtml = `
                <div style="margin-top: 8px;">
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Tags</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 4px;">
                        ${allTags.map(tag => {
                            const isBookmark = tag === 'bookmark';
                            const isIgnore = tag === 'ignore';
                            let color = '#66d9ef';
                            if (isBookmark) color = '#66d9ef';
                            else if (isIgnore) color = '#f92672';
                            else if (window.getTagMetadata) color = window.getTagMetadata(tag).color;
                            
                            return `<span class="tag-card" style="border-color:${color}44; color:${color}; background:${color}11; font-size: 0.6rem; padding: 2px 6px; border-radius: 12px; display: inline-flex; align-items: center;">${tag}</span>`;
                        }).join('')}
                    </div>
                </div>
            `;
        }
        
        const formatArray = (arr) => (arr && arr.length > 0) ? arr.join(', ') : 'N/A';
        
        container.innerHTML = `
            <div style="font-size:0.6rem; color:#555; text-transform:uppercase; margin-bottom:5px;">File Metadata</div>
            
            <div style="display: flex; flex-direction: column; gap: 6px;">
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">AV Type</div>
                    <div class="mono" style="font-size: 0.7rem; color: #eee;">${formatArray(file.avtype)}</div>
                </div>
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">File Type</div>
                    <div class="mono" style="font-size: 0.7rem; color: #eee;">${formatArray(file.filetype)}</div>
                </div>
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Dates</div>
                    <div class="mono" style="font-size: 0.7rem; color: #eee;">${formatArray(file.first_seen)}</div>
                </div>
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">MD5</div>
                    <div class="mono" style="font-size: 0.65rem; color: #aaa; word-break: break-all;">${file.file_md5 || file.md5 || 'N/A'}</div>
                </div>
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Language</div>
                    <div class="mono" style="font-size: 0.7rem; color: #eee;">${file.language_id || 'N/A'}</div>
                </div>
                <div>
                    <div style="font-size: 0.6rem; color: #555; text-transform: uppercase;">Functions</div>
                    <div class="mono" style="font-size: 0.7rem; color: var(--success);">${file.function_count || 0}</div>
                </div>
                ${yaraHtml}
                ${ipsHtml}
                ${tagsHtml}
            </div>
        `;
    }

    hideTooltip() {
        this._activeD = null;
        this._renderedNodeUuid = null;
        const el = getBinHierarchyTooltip();
        if (el) el.style.display = 'none';
        if (window.hideBinaryPreview) {
            window.hideBinaryPreview();
        }
    }
}

const binClusterTooltipMockCache = new Map();

function showBinClusterTableTooltip(event, uuid, name, size, stability, cohesion, avg_features, customMembers = null) {
    const isMenuOpen = window.graphContextMenuOpen || (window.top && window.top.graphContextMenuOpen);
    if (isMenuOpen) return;
    if (window.setTrigger) window.setTrigger(event);
    if (!window.binHierarchyInstance) {
        window.binHierarchyInstance = new BinClusterHierarchy('hierarchy-view-container');
    }

    if (!binClusterTooltipMockCache.has(uuid)) {
        binClusterTooltipMockCache.set(uuid, {
            data: { uuid, name, size, stability, cohesion, avg_features, scrollOffset: 0 }
        });
    } else {
        binClusterTooltipMockCache.get(uuid).data.name = name;
    }
    const mockD = binClusterTooltipMockCache.get(uuid);
    if (customMembers) {
        mockD.data.runtime_members = customMembers;
    }
    window.binHierarchyInstance.showTooltip(event, mockD);
}

function hideBinClusterTableTooltip() {
    if (window.hideAllTooltips) window.hideAllTooltips();
    else if (window.binHierarchyInstance) window.binHierarchyInstance.hideTooltip();
}

function moveBinClusterTableTooltip(e) {
    const tooltip = getBinHierarchyTooltip();
    if (tooltip && tooltip.style.display === 'block') {
        let x = e.clientX + 20;
        let y = e.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = e.clientY - rect.height - 20;
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }
}

window.showBinClusterTableTooltip = showBinClusterTableTooltip;
window.hideBinClusterTableTooltip = hideBinClusterTableTooltip;
window.moveBinClusterTableTooltip = moveBinClusterTableTooltip;

class BinClusterPacking {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.width = this.container ? this.container.clientWidth : 800;
        this.height = this.container ? (this.container.clientHeight || 700) : 700;
        this.root = null;
        this.svg = null;
        this.params = {
            min_cluster_size: 2,
            max_cluster_size: 0,
            cohesion_min: 0.0,
            cohesion_max: 0.0,
            stability_threshold: 0.0,
            show_parents: true,
            show_children: true,
            path_compression: true,
            show_members: false,
            q: ''
        };
        this.abortController = null;
    }

    stop() { if (this.abortController) this.abortController.abort(); }

    applyTagUpdate(action, etype, eid, tag) {
        if (!this.root) return;
        const mutate = (arr, t, add) => {
            if (add) { if (!arr.includes(t)) arr.push(t); }
            else { const i = arr.indexOf(t); if (i !== -1) arr.splice(i, 1); }
        };
        const add = (action === 'add');
        let updated = false;

        this.root.each(d => {
            if (d.data && etype === 'file' && d.data.runtime_files) {
                d.data.runtime_files.forEach(m => {
                    if (m.id === eid || m.md5 === eid || (m.id && m.id.endsWith(eid))) {
                        m.file_user_tags = m.file_user_tags || [];
                        mutate(m.file_user_tags, tag, add);
                        updated = true;
                    }
                });
            }
        });

        if (updated && this._activeD) {
            const tooltip = getBinHierarchyTooltip();
            if (tooltip && tooltip.style.display === 'block' && this._renderedNodeUuid === this._activeD.data.uuid) {
                this._renderedNodeUuid = null;
                this.renderTooltip(tooltip, this._activeD);
            }
        }
    }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        const collection = params.get('collection') || getCurrentCollection();
        const algo = params.get('algo') || 'unweighted_cosine';

        this.params.min_cluster_size = params.has('min_count') ? (parseInt(params.get('min_count')) || 0) : 0;
        this.params.max_cluster_size = params.has('max_count') ? (parseInt(params.get('max_count')) || 0) : 0;
        this.params.cohesion_min = params.has('min_cohesion') ? (parseFloat(params.get('min_cohesion')) || 0) : 0;
        this.params.cohesion_max = params.has('max_cohesion') ? (parseFloat(params.get('max_cohesion')) || 0) : 0;
        this.params.stability_threshold = params.has('min_stability') ? (parseFloat(params.get('min_stability')) || 0) : 0;
        this.params.show_parents = params.get('show_parents') !== 'false';
        this.params.show_children = params.get('show_children') !== 'false';
        this.params.show_members = params.get('show_members') === 'true';
        this.params.color_by_md5 = params.get('color_by_md5') === 'true';
        this.params.q = params.get('q') || '';

        const packControls = `
            <div style="position:absolute; top:20px; left:20px; z-index:10; background:rgba(0,0,0,0.85); padding:15px; border-radius:8px; border:1px solid #333; width:240px; backdrop-filter:blur(10px);">
                <div style="font-size:0.85rem; color:#fff; font-weight:bold; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:5px;">Packing Filters</div>

                <!-- Search -->
                <div style="margin-bottom:15px;">
                    <div class="search-input-wrapper" style="width:100%; background:rgba(255,255,255,0.05); border:1px solid #444;">
                        <i class="fa-solid fa-search" style="font-size:0.7rem; color:#666;"></i>
                        <input type="text" id="bin-pack-search-input" placeholder="Search clusters..." value="${this.params.q}" style="color:#fff !important; font-size:0.75rem !important; padding:4px 8px !important;">
                    </div>
                </div>

                <div class="filter-category-header" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Range Filters</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content">
                    <!-- Size Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:#aaa;">Cluster Size</label>
                            <span style="font-size:0.75rem; color:var(--accent); font-family:monospace; font-weight:bold;">
                                <span id="val-bp-min-size">${this.params.min_cluster_size || 2}</span>-<span id="val-bp-max-size">${this.params.max_cluster_size || '∞'}</span>
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="bp-size-track" class="slider-track"></div>
                            <input type="range" id="input-bp-min-size" min="2" max="100" value="${this.params.min_cluster_size || 2}">
                            <input type="range" id="input-bp-max-size" min="2" max="100" value="${this.params.max_cluster_size || 100}">
                        </div>
                    </div>

                    <!-- Cohesion Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:#aaa;">Cohesion %</label>
                            <span style="font-size:0.75rem; color:var(--success); font-family:monospace; font-weight:bold;">
                                <span id="val-bp-coh-min">${(this.params.cohesion_min * 100).toFixed(0)}</span>-<span id="val-bp-coh-max">${(this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100')}</span>%
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="bp-coh-track" class="slider-track"></div>
                            <input type="range" id="input-bp-coh-min" min="0" max="1" step="0.01" value="${this.params.cohesion_min || 0}">
                            <input type="range" id="input-bp-coh-max" min="0" max="1" step="0.01" value="${this.params.cohesion_max || 1}">
                        </div>
                    </div>

                    <!-- Stability -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:#aaa;">Stability Cut</label>
                            <span style="font-size:0.75rem; color:#66d9ef; font-family:monospace; font-weight:bold;">
                                <span id="val-bp-stab-min">${this.params.stability_threshold.toFixed(1)}</span>+
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="bp-stab-track" class="slider-track" style="width:100%; left:0%;"></div>
                            <input type="range" id="input-bp-stab-min" min="0" max="10" step="0.1" value="${this.params.stability_threshold || 0}" style="z-index:3;">
                        </div>
                    </div>
                </div>

                <div class="filter-category-header collapsed" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Display Settings</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content collapsed">
                    <!-- Checkboxes -->
                    <div style="display:flex; flex-direction:column; gap:8px;">
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-bp-show-parents" ${this.params.show_parents ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-bp-show-parents" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show parents</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-bp-show-children" ${this.params.show_children ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-bp-show-children" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show children</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-bp-path-compression" ${this.params.path_compression !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-bp-path-compression" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Path compression</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-bp-show-members" ${this.params.show_members ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-bp-show-members" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Show members</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-bp-color-by-md5" ${this.params.color_by_md5 ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-bp-color-by-md5" style="font-size:0.75rem; color:#ccc; cursor:pointer; user-select:none;">Color by MD5</label>
                        </div>
                    </div>
                </div>

                <button id="bin-pack-refresh-btn" class="btn-primary" style="padding:10px; font-size:0.75rem; width:100%; margin-top:5px; text-transform:uppercase; letter-spacing:1.5px; font-weight:bold;">Update Visualization</button>
            </div>

            <div id="packing-loader" style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:var(--accent); background:#0d0f14;">
                <div style="text-align:center;">
                    <div class="spinner" style="margin-bottom:15px;"></div>
                    <div style="font-size:0.9rem; letter-spacing:1px;">Packing Binary Clusters...</div>
                </div>
            </div>
        `;

        this.container.innerHTML = packControls;

        const updateTrack = (idPrefix, minVal, maxVal, minLimit, maxLimit) => {
            const minPct = ((minVal - minLimit) / (maxLimit - minLimit)) * 100;
            const maxPct = ((maxVal - minLimit) / (maxLimit - minLimit)) * 100;
            const track = document.getElementById(`${idPrefix}-track`);
            if (track) { track.style.left = minPct + '%'; track.style.width = (maxPct - minPct) + '%'; }
        };

        const sMin = document.getElementById('input-bp-min-size');
        const sMax = document.getElementById('input-bp-max-size');
        const sUpdate = () => {
            if (parseInt(sMin.value) > parseInt(sMax.value)) sMin.value = sMax.value;
            this.params.min_cluster_size = parseInt(sMin.value);
            this.params.max_cluster_size = parseInt(sMax.value) === 100 ? 0 : parseInt(sMax.value);
            document.getElementById('val-bp-min-size').innerText = this.params.min_cluster_size;
            document.getElementById('val-bp-max-size').innerText = this.params.max_cluster_size || '∞';
            updateTrack('bp-size', parseInt(sMin.value), parseInt(sMax.value), 2, 100);
        };
        sMin.oninput = sUpdate; sMax.oninput = sUpdate;
        updateTrack('bp-size', this.params.min_cluster_size || 2, this.params.max_cluster_size || 100, 2, 100);

        const cMin = document.getElementById('input-bp-coh-min');
        const cMax = document.getElementById('input-bp-coh-max');
        const cUpdate = () => {
            if (parseFloat(cMin.value) > parseFloat(cMax.value)) cMin.value = cMax.value;
            this.params.cohesion_min = parseFloat(cMin.value);
            this.params.cohesion_max = parseFloat(cMax.value) >= 1 ? 0 : parseFloat(cMax.value);
            document.getElementById('val-bp-coh-min').innerText = (this.params.cohesion_min * 100).toFixed(0);
            document.getElementById('val-bp-coh-max').innerText = this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100';
            updateTrack('bp-coh', this.params.cohesion_min, parseFloat(cMax.value), 0, 1);
        };
        cMin.oninput = cUpdate; cMax.oninput = cUpdate;
        updateTrack('bp-coh', this.params.cohesion_min, this.params.cohesion_max || 1, 0, 1);

        const stMin = document.getElementById('input-bp-stab-min');
        const stUpdate = () => {
            this.params.stability_threshold = parseFloat(stMin.value);
            document.getElementById('val-bp-stab-min').innerText = this.params.stability_threshold.toFixed(1) + '+';
            updateTrack('bp-stab', this.params.stability_threshold, 10, 0, 10);
        };
        stMin.oninput = stUpdate;
        updateTrack('bp-stab', this.params.stability_threshold, 10, 0, 10);

        document.getElementById('input-bp-show-parents').onchange = (e) => { this.params.show_parents = e.target.checked; };
        document.getElementById('input-bp-show-children').onchange = (e) => { this.params.show_children = e.target.checked; };
        document.getElementById('input-bp-path-compression').onchange = (e) => { this.params.path_compression = e.target.checked; };
        document.getElementById('input-bp-show-members').onchange = (e) => { this.params.show_members = e.target.checked; };
        const md5CheckBp = document.getElementById('input-bp-color-by-md5');
        if (md5CheckBp) {
            md5CheckBp.onchange = (e) => { this.params.color_by_md5 = e.target.checked; };
        }

        document.getElementById('bin-pack-refresh-btn').onclick = () => {
            const hash = window.location.hash;
            const [path, qs] = hash.split('?');
            const p = new URLSearchParams(qs || '');
            const searchVal = document.getElementById('bin-pack-search-input').value.trim();
            if (this.params.min_cluster_size > 0) p.set('min_count', this.params.min_cluster_size); else p.delete('min_count');
            if (this.params.max_cluster_size > 0) p.set('max_count', this.params.max_cluster_size); else p.delete('max_count');
            if (this.params.cohesion_min > 0) p.set('min_cohesion', this.params.cohesion_min); else p.delete('min_cohesion');
            if (this.params.cohesion_max > 0) p.set('max_cohesion', this.params.cohesion_max); else p.delete('max_cohesion');
            if (this.params.stability_threshold > 0) p.set('min_stability', this.params.stability_threshold); else p.delete('min_stability');
            if (searchVal) p.set('q', searchVal); else p.delete('q');
            p.set('show_parents', this.params.show_parents);
            p.set('show_children', this.params.show_children);
            p.set('show_members', this.params.show_members ? 'true' : 'false');
            p.set('color_by_md5', this.params.color_by_md5 ? 'true' : 'false');
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
            queryParams.set('show_members', this.params.show_members === true);

            const url = `/api/bin_cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Cluster data not found");
            const data = await res.json();
            const nameType = params.get('cluster_name_type') || 'file';

            let nodes = (data.results || []).map(m => {
                let displayName = m.cluster_name || `Cluster ${m.cluster_id}`;
                if (nameType === 'yara' && !m.is_custom_name && m.yara_distribution && m.yara_distribution.length > 0) {
                    displayName = m.yara_distribution[0].value;
                }
                return {
                    id: String(m.cluster_id),
                    parent: m.parent ? String(m.parent) : null,
                    name: displayName,
                    uuid: m.cluster_uuid,
                    size: m.count || 0,
                    stability: m.avg_stability || 0.0,
                    cohesion: m.cohesion_score || 0.0,
                    snippet: m.snippet || "",
                    members: m.sample_members || [],
                    direct_members: m.direct_members || []
                };
            });

            if (this.params.show_members) {
                const memberNodes = [];
                nodes.forEach(c => {
                    (c.direct_members || []).forEach(m => {
                        memberNodes.push({
                            id: m.id,
                            parent: String(c.id),
                            name: m.name,
                            file_md5: m.file_md5,
                            language_id: m.language_id,
                            function_count: m.function_count,
                            tags: m.tags,
                            user_tags: m.user_tags,
                            is_member: true,
                            size: 1,
                            stability: 0,
                            cohesion: 0,
                            members: [],
                            avtype: m.avtype,
                            filetype: m.filetype,
                            yara: m.yara,
                            cc_ip: m.cc_ip
                        });
                    });
                });
                nodes = nodes.concat(memberNodes);
            }

            if (!nodes || nodes.length === 0) {
                this.container.innerHTML += `<div style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); color:#aaa; text-align:center;">No binary clusters match these criteria.</div>`;
                const loader = document.getElementById('packing-loader');
                if (loader) loader.remove();
                return;
            }

            this.render(nodes);
        } catch (err) {
            if (err.name === 'AbortError') return;
            this.container.innerHTML = `<div style="margin:auto; color:var(--error); text-align:center;">Error loading packing: ${err.message}</div>`;
        }
    }

    render(nodes) {
        this.rawNodes = nodes;
        const self = this;
        const loader = document.getElementById('packing-loader');
        if (loader) loader.remove();

        if (this.params.path_compression !== false) {
            let compressed = JSON.parse(JSON.stringify(nodes));
            let changed = true;
            while (changed) {
                changed = false;
                const childCounts = {};
                compressed.forEach(n => { if (n.parent) childCounts[n.parent] = (childCounts[n.parent] || 0) + 1; });
                for (let i = 0; i < compressed.length; i++) {
                    const node = compressed[i];
                    if ((childCounts[node.id] || 0) === 1 && node.parent !== null) {
                        const child = compressed.find(n => n.parent === node.id);
                        if (child) { child.parent = node.parent; compressed.splice(i, 1); changed = true; break; }
                    }
                }
            }
            nodes = compressed;
        }

        const width = this.container.clientWidth;
        const height = this.container.offsetHeight || 700;
        d3.select(this.container).selectAll("svg").remove();

        this.svg = d3.select(this.container).append("svg")
            .attr("viewBox", `0 0 ${width} ${height}`)
            .attr("width", "100%").attr("height", "100%")
            .attr("style", "background:#0d0f14; cursor:pointer;");

        const stratify = d3.stratify().id(d => d.id).parentId(d => d.parent);
        nodes.forEach(n => { if (n.parent && !nodes.find(p => p.id === n.parent)) n.parent = null; });
        const rootNodes = nodes.filter(n => !n.parent || !nodes.find(p => p.id === n.parent));

        if (rootNodes.length === 0) { this.root = null; }
        else if (rootNodes.length === 1) { this.root = stratify(nodes); }
        else {
            const vRoot = "VIRTUAL_ROOT";
            const aug = [
                { id: vRoot, parent: null, name: "All Clusters", uuid: "root", size: 0, stability: 0, cohesion: 0, members: [] },
                ...nodes.map(n => (!n.parent || !nodes.find(p => p.id === n.parent)) ? { ...n, parent: vRoot } : n)
            ];
            this.root = stratify(aug);
        }

        if (!this.root) return;

        this.root.sum(d => d.children && d.children.length > 0 ? 0 : (d.size || 1)).sort((a, b) => b.value - a.value);
        d3.pack().size([width - 8, height - 8]).padding(4)(this.root);
        this.root.descendants().forEach(d => { d.x += 4; d.y += 4; });

        let focus = this.root;
        const g = this.svg.append("g");

        const getCohesionFill = (d) => {
            if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return "rgba(255,255,255,0.01)";
            if (d.data.is_member) {
                if (this.params.color_by_md5 && d.data.file_md5) {
                    const baseColor = window.getMd5Color(d.data.file_md5);
                    const rgb = baseColor.match(/\d+/g);
                    if (rgb && rgb.length === 3) {
                        return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, 0.15)`;
                    }
                    return baseColor;
                }
                return "rgba(102,217,239,0.15)";
            }
            const hue = Math.max(0, Math.min(120, (d.data.cohesion || 0) * 120));
            return `hsla(${hue}, 80%, 50%, ${d.children ? 0.03 : 0.15})`;
        };
        const getCohesionStroke = (d) => {
            if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return "rgba(255,255,255,0.15)";
            if (d.data.is_member) {
                if (this.params.color_by_md5 && d.data.file_md5) {
                    return window.getMd5Color(d.data.file_md5);
                }
                return "var(--accent, #66d9ef)";
            }
            const hue = Math.max(0, Math.min(120, (d.data.cohesion || 0) * 120));
            return `hsl(${hue}, 85%, 60%)`;
        };

        const node = g.selectAll("circle")
            .data(this.root.descendants(), d => d.data.id)
            .join("circle")
            .attr("cx", d => d.x).attr("cy", d => d.y).attr("r", d => d.data.is_member ? Math.max(1, d.r * 0.5) : d.r)
            .attr("fill", d => getCohesionFill(d))
            .attr("stroke", d => getCohesionStroke(d))
            .attr("stroke-width", d => d.data.is_member ? 1 : (d.children ? 1 : 1.5))
            .on("mouseover", function(event, d) {
                event.stopPropagation();
                if (d.data.id === "VIRTUAL_ROOT") return;
                d3.select(this).attr("stroke-width", 3).style("filter", `drop-shadow(0 0 8px ${getCohesionStroke(d)})`);
                self.showTooltip(event, d);
            })
            .on("mousemove", function(event) { if (window.moveBinaryPreview) window.moveBinaryPreview(event); })
            .on("mouseout", function(event, d) {
                event.stopPropagation();
                if (d.data.id === "VIRTUAL_ROOT") return;
                const tt = getBinHierarchyTooltip();
                if (tt && (tt === event.relatedTarget || tt.contains(event.relatedTarget))) return;
                d3.select(this).attr("stroke-width", d => d.data.is_member ? 1 : (d.children ? 1 : 1.5)).style("filter", "none");
                self.hideTooltip();
            })
            .on("click", (event, d) => {
                if (d.data.is_member) {
                    if (d.data.file_md5) window.location.hash = `#files/sim?collection=${getCurrentCollection()}&md5=${d.data.file_md5}`;
                    event.stopPropagation();
                    return;
                }
                if (focus !== d) { zoom(event, d); event.stopPropagation(); }
            })
            .on("contextmenu", (event, d) => {
                if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return;
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'bin_cluster', d.data);
                }
            });

        const label = g.selectAll("text")
            .data(this.root.descendants())
            .join("text")
            .attr("text-anchor", "middle").attr("dy", ".35em")
            .style("fill", d => d.data.is_member ? "var(--accent)" : "#fff")
            .style("font-family", "sans-serif").style("pointer-events", "none")
            .attr("x", d => d.x).attr("y", d => d.y)
            .text(d => d.data.id === "VIRTUAL_ROOT" ? "" : d.data.name);

        const dragPack = d3.drag()
            .on("start", function(event, d) {
                self.isDragging = true; self.hideTooltip();
                if (event.sourceEvent) event.sourceEvent.stopPropagation();
            })
            .on("drag", function(event, d) {
                const dx = event.dx;
                const dy = event.dy;
                const descendants = d.descendants();
                descendants.forEach(desc => {
                    desc.x += dx;
                    desc.y += dy;
                });
                g.selectAll("circle").filter(n => descendants.includes(n))
                    .attr("cx", n => n.x)
                    .attr("cy", n => n.y);
                g.selectAll("text").filter(n => descendants.includes(n))
                    .attr("x", n => n.x)
                    .attr("y", n => n.y);
            })
            .on("end", function() { self.isDragging = false; });

        node.call(dragPack);

        const zoomBehavior = d3.zoom().scaleExtent([0.1, 100]).on("zoom", (event) => {
            g.attr("transform", event.transform);
            const k = event.transform.k;
            label.style("font-size", d => `${Math.max(8, Math.min(18, d.r * k / 5)) / k}px`);
            label.style("opacity", d => {
                if (d.data.id === "VIRTUAL_ROOT") return 0;
                const rPx = d.r * k;
                if (rPx < 20) return 0;
                if (d.children && d.children.length > 0 && rPx > 150) return 0;
                return 1;
            });
        });

        this.svg.call(zoomBehavior).on("dblclick.zoom", null);
        this.svg.on("click", (event) => zoom(event, this.root));

        const initS = Math.min(width, height) / (this.root.r * 2);
        this.svg.call(zoomBehavior.transform, d3.zoomIdentity.translate(width / 2 - this.root.x * initS, height / 2 - this.root.y * initS).scale(initS));

        function zoom(event, d) {
            focus = d;
            const s = Math.min(width, height) / (d.r * 2);
            self.svg.transition().duration(750).call(zoomBehavior.transform, d3.zoomIdentity.translate(width / 2 - d.x * s, height / 2 - d.y * s).scale(s));
        }
    }

    async showTooltip(event, d) {
        this._activeD = d;
        if (d.data.is_member) {
            const el = getBinHierarchyTooltip();
            if (el) el.style.display = 'none';
            if (window.showBinaryPreview) window.showBinaryPreview(d.data.file_md5, d.data.name, d.data.function_count, d.data.language_id, d.data.tags, event, d.data.tags, d.data.user_tags, {
                avtype: d.data.avtype,
                filetype: d.data.filetype,
                yara: d.data.yara,
                cc_ip: d.data.cc_ip
            });
            return;
        }
        const tooltip = getBinHierarchyTooltip();
        tooltip.style.display = 'block';
        let x = event.clientX + 20, y = event.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = event.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = event.clientY - rect.height - 20;
        tooltip.style.left = x + 'px'; tooltip.style.top = y + 'px';
        tooltip.onmouseleave = () => { this.hideTooltip(); };
        this.renderTooltip(tooltip, d);
        if (!d.data.runtime_members && d.data.uuid && d.data.uuid !== 'root') {
            try {
                const col = getCurrentCollection();
                const res = await fetch(`/api/bin_cluster/files?collection=${col}&cluster_uuid=${d.data.uuid}&limit=100`);
                const data = await res.json();
                d.data.runtime_members = data.files;
                if (this._activeD === d) this.renderTooltip(tooltip, d);
            } catch(e) { console.error("Failed to fetch runtime members", e); }
        }
    }

    renderTooltip(tooltip, d) {
        if (!window.binHierarchyInstance) return;
        window.binHierarchyInstance.renderTooltip.call(this, tooltip, d);
    }

    updateSnippet(file) {
        if (!window.binHierarchyInstance) return;
        window.binHierarchyInstance.updateSnippet.call(this, file);
    }

    hideTooltip() {
        this._activeD = null;
        const el = getBinHierarchyTooltip();
        if (el) el.style.display = 'none';
        if (window.hideBinaryPreview) window.hideBinaryPreview();
    }
}
