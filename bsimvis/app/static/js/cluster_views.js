// Cluster Hierarchy (Dendrogram) and Packing Visualizations for BSimVis

function getCurrentCollection() {
    let restful;
    if (window.parent && window.parent.parseRestfulPath) {
        restful = window.parent.parseRestfulPath();
    } else if (window.parseRestfulPath) {
        restful = parseRestfulPath();
    }
    if (restful) {
        if (restful.collection) return restful.collection;
        if (restful.pool) return 'pool:' + restful.pool;
    }


    // Fallback for old system
    const params = new URLSearchParams(window.location.search);
    if (params.get('collection')) return params.get('collection');

    const hash = window.location.hash || '';
    const hashParams = new URLSearchParams(hash.split('?')[1] || '');
    if (hashParams.get('collection')) return hashParams.get('collection');

    const id = params.get('id') || params.get('id1') || params.get('id2') || window.currentFuncId;
    if (id && id.includes(':')) return id.split(':')[0];

    return '';
}
function getHierarchyTooltip() {
    let el = document.getElementById('hierarchy-tooltip');
    if (!el) {
        el = document.createElement('div');
        el.id = 'hierarchy-tooltip';
        el.style.cssText = "position:fixed; z-index:20003; background:rgba(13,15,20,0.98); border-radius:8px; border:1px solid var(--accent,#66d9ef); display:none; pointer-events:auto; font-size:0.8rem; box-shadow:0 15px 50px var(--window-bg); max-width:calc(100vw - 30px); backdrop-filter:blur(15px); overflow:hidden;";
        document.body.appendChild(el);
        
        el.addEventListener('click', (event) => {
            const item = event.target.closest('.hier-function-item');
            if (item) {
                const idx = parseInt(item.getAttribute('data-index'));
                const activeInstance = (window.hierarchyInstance && window.hierarchyInstance._activeD)
                    ? window.hierarchyInstance
                    : ((window.packingInstance && window.packingInstance._activeD) ? window.packingInstance : null);
                
                if (activeInstance && activeInstance._activeD) {
                    const d = activeInstance._activeD;
                    const members = d.data.runtime_members || [];
                    const func = members[idx];
                    if (func && func.function_id) {
                        const name = func.function_name || 'Unknown';
                        if (typeof showFunctionCodeById === 'function') {
                            showFunctionCodeById(func.function_id, name, '', event);
                        } else {
                            const parts = func.function_id.split(':');
                            const col = parts[0] || '';
                            const md5 = parts[2];
                            const addr = parts[3];
                            const url = Nav.buildUIUrl(col, ['function', md5, addr]);
                            Nav.openPath(url, event, { title: `Code: ${name}`, type: 'code' });
                        }
                    }
                }
            }
        });
    }
    return el;
}

function loadHierarchyView(params, clusterType = 'function') {
    if (!window.hierarchyInstance) {
        window.hierarchyInstance = new ClusterHierarchy('hierarchy-view-container', clusterType);
    }
    window.hierarchyInstance.fetch(params);
}

function loadPackingView(params) {
    if (!window.packingInstance) {
        window.packingInstance = new ClusterPacking('packing-view-container');
    }
    window.packingInstance.fetch(params);
}

class ClusterHierarchy extends D3BaseLayout {
    constructor(containerId, clusterType = 'function') {
        super(containerId);
        this.clusterType = clusterType;
        this.params = {
            min_cluster_size: 2,
            stability_threshold: 0.0,
            show_parents: true,
            path_compression: true
        };
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
            if (d.data && etype === 'function' && d.data.runtime_members) {
                d.data.runtime_members.forEach(m => {
                    const altEid = eid.includes(':func:') ? eid.replace(':func:', ':function:') : eid.replace(':function:', ':func:');
                    if (m.function_id === eid || m.function_id === altEid) {
                        m.user_tags = m.user_tags || [];
                        mutate(m.user_tags, tag, add);
                        updated = true;
                    }
                });
            }
        });

        if (updated && this._activeD) {
            const tooltip = getHierarchyTooltip();
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

        const collection = params.get('collection');
        const algo = params.get('algo') || 'unweighted_cosine';

        // Sync from URL params so global filters reflect in sliders
        this.params.min_cluster_size = params.has('min_count') ? (parseInt(params.get('min_count')) || 0) : 0;
        this.params.max_cluster_size = params.has('max_count') ? (parseInt(params.get('max_count')) || 0) : 0;
        this.params.cohesion_min = params.has('min_cohesion') ? (parseFloat(params.get('min_cohesion')) || 0) : 0;
        this.params.cohesion_max = params.has('max_cohesion') ? (parseFloat(params.get('max_cohesion')) || 0) : 0;
        this.params.min_features = params.has('min_features') ? (parseInt(params.get('min_features')) || 0) : 0;
        this.params.max_features = params.has('max_features') ? (parseInt(params.get('max_features')) || 0) : 0;
        this.params.stability_threshold = params.has('min_stability') ? (parseFloat(params.get('min_stability')) || 0) : 0;
        this.params.show_parents = params.get('show_parents') !== 'false';
        this.params.show_children = params.get('show_children') !== 'false';
        this.params.show_members = params.get('show_members') === 'true';
        this.params.color_by_md5 = params.get('color_by_md5') === 'true';
        this.params.show_binary_sankey = params.get('show_binary_sankey') === 'true';
        if (this.params.show_binary_sankey) {
            this.params.show_members = true;
        }

        // Template for controls
        const hierControls = `
            <div style="position:absolute; top:20px; left:20px; z-index:10; background:var(--window-bg); padding:15px; border-radius:8px; border:1px solid var(--border); width:240px; backdrop-filter:blur(10px);">
                <div style="font-size:0.85rem; color:var(--text); font-weight:bold; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:5px;">Tree Filters</div>
                
                <div class="filter-category-header" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Range Filters</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content">
                    <!-- Size Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Cluster Size</label>
                            <span style="font-size:0.75rem; color:var(--accent); font-family:monospace; font-weight:bold;">
                                <span id="val-min-size">${this.params.min_cluster_size}</span>-<span id="val-max-size">${this.params.max_cluster_size || '∞'}</span>
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="size-track" class="slider-track"></div>
                            <input type="range" id="input-min-size" min="2" max="100" value="${this.params.min_cluster_size}">
                            <input type="range" id="input-max-size" min="2" max="100" value="${this.params.max_cluster_size || 100}">
                        </div>
                    </div>

                    <!-- Cohesion Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Cohesion %</label>
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

                    <!-- Feature Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Avg Features</label>
                            <span style="font-size:0.75rem; color:#ae81ff; font-family:monospace; font-weight:bold;">
                                <span id="val-feat-min">${this.params.min_features || 0}</span>-<span id="val-feat-max">${this.params.max_features || '∞'}</span>
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="feat-track" class="slider-track"></div>
                            <input type="range" id="input-feat-min" min="0" max="1000" step="10" value="${this.params.min_features || 0}">
                            <input type="range" id="input-feat-max" min="0" max="1000" step="10" value="${this.params.max_features || 1000}">
                        </div>
                    </div>

                    <!-- Stability Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Stability</label>
                            <span style="font-size:0.75rem; color:#66d9ef; font-family:monospace; font-weight:bold;">
                                <span id="val-stab-min">${this.params.stability_threshold.toFixed(1)}</span>+
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="stab-track" class="slider-track" style="width:100%; left:0%;"></div>
                            <input type="range" id="input-stab-min" min="0" max="100" step="1" value="${this.params.stability_threshold || 0}" style="z-index: 3;">
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
                            <input type="checkbox" id="input-show-parents" ${this.params.show_parents !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-show-parents" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show parents</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-show-children" ${this.params.show_children !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-show-children" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show children</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-path-compression" ${this.params.path_compression !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-path-compression" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Path compression</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-show-members" ${this.params.show_members ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-show-members" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show members</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-color-by-md5" ${this.params.color_by_md5 ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-color-by-md5" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Color by MD5</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-show-binary-sankey" ${this.params.show_binary_sankey ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-show-binary-sankey" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Binary flow (Sankey)</label>
                        </div>
                    </div>
                </div>

                <button id="hier-refresh-btn" class="btn-primary" style="padding:10px; font-size:0.75rem; width:100%; margin-top:5px; text-transform:uppercase; letter-spacing:1.5px; font-weight:bold;">Update Visualization</button>
            </div>

            <div style="position:absolute; bottom:20px; right:20px; z-index:10; background:var(--window-bg); padding:15px; border-radius:8px; border:1px solid var(--border); max-width:300px; display:none;" id="hierarchy-info-panel">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <h4 id="hier-cluster-name" style="margin:0; color:var(--accent); font-size:0.9rem;">Cluster Info</h4>
                    <button onclick="this.parentElement.parentElement.style.display='none'" style="background:none; border:none; color:var(--subtle); cursor:pointer;">&times;</button>
                </div>
                <div id="hier-stats" style="font-size:0.85rem; color:var(--meta-text-muted);"></div>
                <div style="margin-top:15px; display:flex; gap:10px;">
                    <button id="hier-view-btn" class="btn-action" style="flex:1">Functions</button>
                </div>
            </div>

            <div id="hierarchy-loader" style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:var(--accent); background:var(--window-bg);">
                <div style="text-align:center;">
                    <div class="spinner" style="margin-bottom:15px;"></div>
                    <div style="font-size:0.9rem; letter-spacing:1px;">Rebuilding Dendrogram...</div>
                </div>
            </div>
        `;

        this.container.innerHTML = hierControls;

        // Initialize defaults
        this.params.min_cluster_size = this.params.min_cluster_size || 2;
        this.params.max_cluster_size = this.params.max_cluster_size || 0;
        this.params.cohesion_min = this.params.cohesion_min || 0;
        this.params.cohesion_max = this.params.cohesion_max || 0;
        this.params.min_features = this.params.min_features || 0;
        this.params.max_features = this.params.max_features || 0;
        if (this.params.show_parents === undefined) this.params.show_parents = true;
        if (this.params.path_compression === undefined) this.params.path_compression = true;

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
        updateTrack('size', this.params.min_cluster_size, this.params.max_cluster_size || 100, 2, 100);

        const cUpdate = () => {
            const cMinE = document.getElementById('input-coh-min');
            const cMaxE = document.getElementById('input-coh-max');
            if (parseFloat(cMinE.value) > parseFloat(cMaxE.value)) cMinE.value = cMaxE.value;
            this.params.cohesion_min = parseFloat(cMinE.value);
            this.params.cohesion_max = parseFloat(cMaxE.value) >= 1 ? 0 : parseFloat(cMaxE.value);
            document.getElementById('val-coh-min').innerText = (this.params.cohesion_min * 100).toFixed(0);
            document.getElementById('val-coh-max').innerText = this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100';
            updateTrack('coh', this.params.cohesion_min, parseFloat(cMaxE.value), 0, 1);
        };
        document.getElementById('input-coh-min').oninput = cUpdate;
        document.getElementById('input-coh-max').oninput = cUpdate;
        updateTrack('coh', this.params.cohesion_min, this.params.cohesion_max || 1, 0, 1);

        const fMin = document.getElementById('input-feat-min');
        const fMax = document.getElementById('input-feat-max');
        const fUpdate = () => {
            if (parseInt(fMin.value) > parseInt(fMax.value)) fMin.value = fMax.value;
            this.params.min_features = parseInt(fMin.value);
            this.params.max_features = parseInt(fMax.value) === 1000 ? 0 : parseInt(fMax.value);
            document.getElementById('val-feat-min').innerText = this.params.min_features;
            document.getElementById('val-feat-max').innerText = this.params.max_features || '∞';
            updateTrack('feat', parseInt(fMin.value), parseInt(fMax.value), 0, 1000);
        };
        fMin.oninput = fUpdate;
        fMax.oninput = fUpdate;
        updateTrack('feat', this.params.min_features, this.params.max_features || 1000, 0, 1000);

        const stMin = document.getElementById('input-stab-min');
        const stUpdate = () => {
            this.params.stability_threshold = parseFloat(stMin.value);
            document.getElementById('val-stab-min').innerText = this.params.stability_threshold.toFixed(1) + '+';
            updateTrack('stab', this.params.stability_threshold, 100, 0, 100);
        };
        stMin.oninput = stUpdate;
        updateTrack('stab', this.params.stability_threshold, 100, 0, 100);

        const spCheck = document.getElementById('input-show-parents');
        spCheck.onchange = () => {
            this.params.show_parents = spCheck.checked;
        };

        const scCheck = document.getElementById('input-show-children');
        scCheck.onchange = () => {
            this.params.show_children = scCheck.checked;
        };

        const pcCheck = document.getElementById('input-path-compression');
        pcCheck.onchange = () => {
            this.params.path_compression = pcCheck.checked;
        };

        const smCheck = document.getElementById('input-show-members');
        if (smCheck) {
            smCheck.onchange = () => {
                this.params.show_members = smCheck.checked;
                if (!smCheck.checked) {
                    this.params.show_binary_sankey = false;
                    const sankeyCheck = document.getElementById('input-show-binary-sankey');
                    if (sankeyCheck) sankeyCheck.checked = false;
                }
            };
        }
        const md5Check = document.getElementById('input-color-by-md5');
        if (md5Check) {
            md5Check.onchange = () => {
                this.params.color_by_md5 = md5Check.checked;
            };
        }
        const sankeyCheck = document.getElementById('input-show-binary-sankey');
        if (sankeyCheck) {
            sankeyCheck.onchange = () => {
                this.params.show_binary_sankey = sankeyCheck.checked;
                if (sankeyCheck.checked) {
                    this.params.show_members = true;
                    const smCheck = document.getElementById('input-show-members');
                    if (smCheck) smCheck.checked = true;
                }
            };
        }

        document.getElementById('hier-refresh-btn').onclick = () => {
            const { view } = parseRestfulPath();
            const p = new URLSearchParams(window.location.search);
            
            if (this.params.min_cluster_size > 0) p.set('min_count', this.params.min_cluster_size); else p.delete('min_count');
            if (this.params.max_cluster_size > 0) p.set('max_count', this.params.max_cluster_size); else p.delete('max_count');
            if (this.params.cohesion_min > 0) p.set('min_cohesion', this.params.cohesion_min); else p.delete('min_cohesion');
            if (this.params.cohesion_max > 0) p.set('max_cohesion', this.params.cohesion_max); else p.delete('max_cohesion');
            if (this.params.min_features > 0) p.set('min_features', this.params.min_features); else p.delete('min_features');
            if (this.params.max_features > 0) p.set('max_features', this.params.max_features); else p.delete('max_features');
            if (this.params.stability_threshold > 0) p.set('min_stability', this.params.stability_threshold); else p.delete('min_stability');
            p.set('show_parents', this.params.show_parents);
            p.set('show_children', this.params.show_children);
            p.set('show_members', this.params.show_members ? 'true' : 'false');
            p.set('color_by_md5', this.params.color_by_md5 ? 'true' : 'false');
            p.set('show_binary_sankey', this.params.show_binary_sankey ? 'true' : 'false');
            
            if (typeof navigate === 'function') {
                navigate(view || 'clusters', p);
            } else {
                window.location.search = p.toString();
            }
        };

        try {
            const queryParams = new URLSearchParams(params.toString());
            
            if (this.params.min_cluster_size > 0) queryParams.set('min_count', this.params.min_cluster_size);
            if (this.params.max_cluster_size < 1000 && this.params.max_cluster_size > 0) queryParams.set('max_count', this.params.max_cluster_size);
            if (this.params.cohesion_min > 0) queryParams.set('min_cohesion', this.params.cohesion_min);
            if (this.params.cohesion_max < 1 && this.params.cohesion_max > 0) queryParams.set('max_cohesion', this.params.cohesion_max);
            if (this.params.min_features > 0) queryParams.set('min_features', this.params.min_features);
            if (this.params.max_features < 1000 && this.params.max_features > 0) queryParams.set('max_features', this.params.max_features);
            if (this.params.stability_threshold > 0) queryParams.set('min_stability', this.params.stability_threshold);
            queryParams.set('show_parents', this.params.show_parents !== false);
            queryParams.set('show_children', this.params.show_children !== false);
            queryParams.set('show_members', this.params.show_members === true);

            const url = `/api/cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Cluster data not found");
            const data = await res.json();

            let nodes = (data.results || []).map(m => ({
                id: String(m.cluster_id),
                parent: m.parent ? String(m.parent) : null,
                name: m.cluster_name || `Cluster ${m.cluster_id}`,
                uuid: m.cluster_uuid,
                size: m.count || 0,
                stability: m.avg_stability || 0.0,
                cohesion: m.cohesion_score || 0.0,
                avg_features: m.avg_features || 0.0,
                snippet: m.snippet || "",
                members: m.sample_members || [],
                direct_members: m.direct_members || []
            }));

            if (this.params.show_members) {
                const memberNodes = [];
                nodes.forEach(c => {
                    if (c.direct_members && c.direct_members.length > 0) {
                        c.direct_members.forEach(m => {
                            memberNodes.push({
                                id: m.id,
                                parent: String(c.id),
                                name: m.name,
                                md5: m.file_md5,
                                bin: m.bin,
                                is_member: true,
                                size: 1,
                                stability: 0,
                                cohesion: 0,
                                members: []
                            });
                        });
                    }
                });
                nodes = nodes.concat(memberNodes);
            }

            if (!nodes || nodes.length === 0) {
                this.container.innerHTML += `<div style="position:absolute; top:50%; left:50%; transform:translate(-50%, -50%); color:var(--meta-text-muted); text-align:center; width:100%;">No clusters match these criteria.<br><span style="font-size:0.8rem; color:var(--subtle);">Try lowering the stability cut or minimum size.</span></div>`;
                const loader = document.getElementById('hierarchy-loader');
                if (loader) loader.remove();
                return;
            }

            this.render(nodes);
        } catch (e) {
            if (e.name === 'AbortError') return;
            this.container.innerHTML = `<div style="margin:auto; color:#ff5555; text-align:center;">Error loading hierarchy: ${e.message}</div>`;
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
                compressedNodes.forEach(n => {
                    if (n.parent) {
                        childCounts[n.parent] = (childCounts[n.parent] || 0) + 1;
                    }
                });

                for (let i = 0; i < compressedNodes.length; i++) {
                    const node = compressedNodes[i];
                    const count = childCounts[node.id] || 0;

                    if (count === 1 && node.parent !== null) {
                        const child = compressedNodes.find(n => n.parent === node.id);
                        if (child) {
                            child.parent = node.parent;
                            compressedNodes.splice(i, 1);
                            changed = true;
                            break;
                        }
                    }
                }
            }
            nodes = compressedNodes;
        }

        const width = this.width;
        const height = this.height;

        this.initSvg();

        const stratify = d3.stratify()
            .id(d => d.id)
            .parentId(d => d.parent);

        nodes.forEach(n => {
            if (n.parent && !nodes.find(p => p.id === n.parent)) {
                n.parent = null;
            }
        });

        const rootNodes = nodes.filter(n => !n.parent || !nodes.find(p => p.id === n.parent));

        if (rootNodes.length === 0) {
            this.root = null;
        } else if (rootNodes.length === 1) {
            this.root = stratify(nodes);
        } else {
            const virtualRootId = "VIRTUAL_ROOT";
            const augmentedNodes = [
                {
                    id: virtualRootId,
                    parent: null,
                    name: "All Clusters",
                    uuid: "root",
                    size: nodes.reduce((acc, n) => (!n.parent || !nodes.find(p => p.id === n.parent)) ? acc + n.size : acc, 0),
                    stability: 0,
                    cohesion: 0,
                    members: []
                },
                ...nodes.map(n => {
                    if (!n.parent || !nodes.find(p => p.id === n.parent)) {
                        return { ...n, parent: virtualRootId };
                    }
                    return n;
                })
            ];
            this.root = stratify(augmentedNodes);
        }

        this.root.x0 = height / 2;
        this.root.y0 = 0;

        this.update(this.root);

        let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        this.root.descendants().forEach(d => {
            if (d.x < minX) minX = d.x;
            if (d.x > maxX) maxX = d.x;
            if (d.y < minY) minY = d.y;
            if (d.y > maxY) maxY = d.y;
        });
        let actualMaxY = maxY;
        if (this.params.show_binary_sankey) {
            actualMaxY = maxY + 400;
        }
        const dx = actualMaxY - minY;
        const dy = maxX - minX;
        const paddingX = 300; 
        const paddingY = 100;
        const s = Math.min(0.8, width / (dx + paddingX || 1), height / (dy + paddingY || 1));
        const cx = (minY + actualMaxY) / 2;
        const cy = (minX + maxX) / 2;
        const initialTransform = d3.zoomIdentity.translate(width / 2 - cx * s, height / 2 - cy * s).scale(s);
        this.svg.call(this.zoom.transform, initialTransform);
    }

    update(source) {
        const self = this;
        const nodeWidth = 240;
        const nodeHeight = 35;

        const treeLayout = d3.cluster().nodeSize([nodeHeight, nodeWidth]);
        treeLayout(this.root);

        const nodes = this.root.descendants();
        const links = this.root.links();

        const node = this.g.selectAll("g.node")
            .data(nodes, d => d.data.id);

        const nodeEnter = node.enter().append("g")
            .attr("class", "node")
            .attr("transform", d => `translate(${source.y},${source.x})`)
            .style("cursor", "pointer")
            .on("click", (event, d) => {
                const col = getCurrentCollection();
                if (d.data.is_member) {
                    if (window.showFunctionCodeById) {
                        window.showFunctionCodeById(d.data.id, d.data.name, '', event);
                    } else if (typeof showFunctionCodeById === 'function') {
                        showFunctionCodeById(d.data.id, d.data.name, '', event);
                    } else {
                        const url = Nav.buildUIUrl(col, ['functions']) + '?q=' + encodeURIComponent(d.data.name);
                        Nav.openPath(url, event);
                    }
                    return;
                }
                const uuid = d.data.uuid;
                if (uuid && uuid !== 'root') {
                    const url = Nav.buildUIUrl(col, ['functions']) + '?cluster_uuid=' + encodeURIComponent(uuid);
                    Nav.openPath(url, event);
                }
            })
            .on("mouseover", (e, d) => {
                self._hoveredNodeEl = e.currentTarget;
                d3.select(e.currentTarget).select("circle").attr("r", d.data.is_member ? 7 : 14);
                self.showTooltip(e, d);
            })
            .on("mousemove", (e, d) => {
                if (window.moveCodePreview) window.moveCodePreview(e);
            })
            .on("mouseout", (e, d) => {
                const relatedTarget = e.relatedTarget;
                const tooltip = getHierarchyTooltip();
                if (tooltip && (tooltip === relatedTarget || tooltip.contains(relatedTarget))) {
                    return;
                }
                d3.select(e.currentTarget).select("circle").attr("r", d.data.is_member ? 4 : 8);
                self.hideTooltip();
                self._hoveredNodeEl = null;
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    const type = d.data.is_member ? 'node' : 'cluster';
                    window.showGraphContextMenu(event, type, d.data);
                }
            });

        const getCohesionColor = (cohesion) => {
            const hue = Math.max(0, Math.min(120, (cohesion || 0) * 120));
            return `hsl(${hue}, var(--color-s-high), var(--color-l-high))`;
        };

        nodeEnter.append("circle")
            .attr("r", d => d.data.is_member ? 4 : 8)
            .attr("stroke", d => {
                if (d.data.is_member) {
                    if ((this.params.color_by_md5 || this.params.show_binary_sankey) && d.data.md5) {
                        return window.getMd5Color(d.data.md5);
                    }
                    return "var(--accent)";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("stroke-width", d => d.data.is_member ? 1.5 : 2)
            .style("fill", d => {
                if (d.data.is_member) {
                    if ((this.params.color_by_md5 || this.params.show_binary_sankey) && d.data.md5) {
                        return window.getMd5Color(d.data.md5);
                    }
                    return "var(--window-bg)";
                }
                return getCohesionColor(d.data.cohesion);
            });

        nodeEnter.append("text")
            .attr("dy", ".35em")
            .attr("x", d => d.children || d._children ? -15 : 15)
            .attr("text-anchor", d => d.children || d._children ? "end" : "start")
            .style("fill", d => d.data.is_member ? "var(--meta-text-muted)" : "var(--text)")
            .style("font-size", d => d.data.is_member ? "10px" : "12px")
            .style("font-style", d => d.data.is_member ? "italic" : "normal")
            .style("pointer-events", "none")
            .text(d => d.data.name)
            .clone(true).lower()
            .attr("stroke", "var(--window-tray)")
            .attr("stroke-width", 3);

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
                    .attr("d", l => diagonal(l.source, l.target));
                if (self.params.show_binary_sankey) {
                    self.g.selectAll("path.sankey-flow-path").filter(l => descendants.includes(l.leaf))
                        .attr("d", l => {
                            l.y0 = l.leaf.y;
                            l.x0 = l.leaf.x;
                            return self.drawSankeyLink(l.y0, l.x0, l.h0, l.y1, l.x1, l.h1);
                        });
                }
            });

        nodeEnter.call(dragDendro);

        const nodeUpdate = nodeEnter.merge(node);

        nodeUpdate.transition().duration(400)
            .attr("transform", d => `translate(${d.y},${d.x})`);

        nodeUpdate.select("circle")
            .style("fill", d => {
                if (d.data.is_member) {
                    if ((this.params.color_by_md5 || this.params.show_binary_sankey) && d.data.md5) {
                        return window.getMd5Color(d.data.md5);
                    }
                    return "var(--window-bg)";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("stroke", d => {
                if (d.data.is_member) {
                    if ((this.params.color_by_md5 || this.params.show_binary_sankey) && d.data.md5) {
                        return window.getMd5Color(d.data.md5);
                    }
                    return "var(--accent)";
                }
                return getCohesionColor(d.data.cohesion);
            })
            .attr("r", d => d.data.is_member ? 4 : 8);

        const nodeExit = node.exit().transition().duration(400)
            .attr("transform", d => `translate(${source.y},${source.x})`)
            .remove();

        const link = this.g.selectAll("path.link")
            .data(links, d => d.target.data.id);

        const linkEnter = link.enter().insert("path", "g")
            .attr("class", "link")
            .attr("fill", "none")
            .attr("stroke", "var(--border)")
            .attr("stroke-width", 1.5)
            .attr("d", d => {
                const o = { x: source.x, y: source.y };
                return diagonal(o, o);
            });

        link.merge(linkEnter).transition().duration(400)
            .attr("stroke", "var(--border)")
            .attr("d", d => diagonal(d.source, d.target));

        link.exit().transition().duration(400)
            .attr("d", d => {
                const o = { x: source.x, y: source.y };
                return diagonal(o, o);
            })
            .remove();

        function diagonal(s, t) {
            return d3.linkHorizontal()
                .x(d => d.y)
                .y(d => d.x)({ source: s, target: t });
        }

        // Cleanup existing Sankey elements
        this.g.selectAll("g.sankey-group").remove();

        if (this.params.show_binary_sankey) {
            const sankeyGroup = this.g.append("g").attr("class", "sankey-group");
            
            // 1. Find all leaves
            const leaves = this.root.leaves().filter(d => d.data.bin || d.data.md5);
            if (leaves.length > 0) {
                // 2. Map binaries
                const binariesMap = new Map();
                leaves.forEach(d => {
                    const name = d.data.bin || "Unknown Binary";
                    const md5 = d.data.md5 || "unknown";
                    if (!binariesMap.has(md5)) {
                        binariesMap.set(md5, {
                            md5: md5,
                            name: name,
                            leaves: [],
                            totalWeight: 0
                        });
                    }
                    const binObj = binariesMap.get(md5);
                    binObj.leaves.push(d);
                    binObj.totalWeight += 1;
                });
                
                const binaries = Array.from(binariesMap.values());
                binaries.forEach(b => {
                    b.averageLeafX = d3.mean(b.leaves, d => d.x);
                });
                binaries.sort((a, b) => a.averageLeafX - b.averageLeafX);
                
                // Calculate geometry
                const maxY = d3.max(this.root.descendants(), d => d.y);
                const binaryY = maxY + 240;
                
                const leafXCoords = leaves.map(d => d.x);
                const minLeafX = d3.min(leafXCoords) || 0;
                const maxLeafX = d3.max(leafXCoords) || 0;
                const leafHeight = maxLeafX - minLeafX;
                const H = Math.max(400, leafHeight);
                
                const totalWeight = d3.sum(binaries, b => b.totalWeight);
                
                const gap = 15;
                const minNodeHeight = 12;
                let scale = (H - (binaries.length - 1) * gap) / (totalWeight || 1);
                if (scale * 1 < minNodeHeight) {
                    scale = minNodeHeight;
                }
                
                let currentX = minLeafX;
                binaries.forEach(b => {
                    b.height = Math.max(minNodeHeight, b.totalWeight * scale);
                    b.x = currentX;
                    b.y = binaryY;
                    currentX += b.height + gap;
                });
                
                const totalBinariesHeight = currentX - gap - minLeafX;
                if (totalBinariesHeight < leafHeight) {
                    const offset = (leafHeight - totalBinariesHeight) / 2;
                    binaries.forEach(b => {
                        b.x += offset;
                    });
                }
                
                binaries.forEach(b => {
                    b.leaves.sort((a, b) => a.x - b.x);
                });
                
                // Draw links
                const linksData = [];
                binaries.forEach(b => {
                    const linkHeight = b.height / b.leaves.length;
                    b.leaves.forEach((leaf, idx) => {
                        const linkDestX = b.x + (idx + 0.5) * linkHeight;
                        linksData.push({
                            id: `link-${leaf.data.id}-${b.md5}`,
                            leaf: leaf,
                            binary: b,
                            y0: leaf.y,
                            x0: leaf.x,
                            h0: 3, // Start height at leaf
                            y1: b.y,
                            x1: linkDestX,
                            h1: linkHeight,
                            color: window.getMd5Color ? window.getMd5Color(b.md5) : "var(--accent)"
                        });
                    });
                });
                
                const tooltipEl = document.getElementById('token-tooltip');
                
                const showSankeyTooltip = (e, funcName, binName) => {
                    if (!tooltipEl) return;
                    tooltipEl.innerHTML = `
                        <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid var(--border); padding-bottom:5px; margin-bottom:5px;">Sankey Flow</div>
                        <div style="font-size:0.75rem; margin-bottom:4px;"><span style="color:var(--meta-text-muted);">Function:</span> <b style="color:var(--text);">${funcName}</b></div>
                        <div style="font-size:0.75rem;"><span style="color:var(--meta-text-muted);">Binary:</span> <b style="color:var(--accent);">${binName}</b></div>
                    `;
                    tooltipEl.style.display = 'block';
                    positionTooltip(e);
                };
                
                const showSankeyBinaryTooltip = (e, binName, funcCount) => {
                    if (!tooltipEl) return;
                    tooltipEl.innerHTML = `
                        <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid var(--border); padding-bottom:5px; margin-bottom:5px;">Binary Destination</div>
                        <div style="font-size:0.75rem; margin-bottom:4px;"><span style="color:var(--meta-text-muted);">Binary:</span> <b style="color:var(--text);">${binName}</b></div>
                        <div style="font-size:0.75rem;"><span style="color:var(--meta-text-muted);">Flows from:</span> <b style="color:var(--success);">${funcCount} functions</b></div>
                    `;
                    tooltipEl.style.display = 'block';
                    positionTooltip(e);
                };
                
                const hideSankeyTooltip = () => {
                    if (tooltipEl) tooltipEl.style.display = 'none';
                };
                
                const positionTooltip = (e) => {
                    if (!tooltipEl) return;
                    tooltipEl.style.left = (e.clientX + 15) + 'px';
                    tooltipEl.style.top = (e.clientY + 15) + 'px';
                    const rect = tooltipEl.getBoundingClientRect();
                    if (rect.right > window.innerWidth) tooltipEl.style.left = (e.clientX - rect.width - 15) + 'px';
                    if (rect.bottom > window.innerHeight) tooltipEl.style.top = (e.clientY - rect.height - 15) + 'px';
                };
                
                // Append flows
                const flows = sankeyGroup.append("g")
                    .attr("class", "sankey-flows")
                    .selectAll("path.sankey-flow-path")
                    .data(linksData, d => d.id);
                
                const flowsEnter = flows.enter().append("path")
                    .attr("class", "sankey-flow-path")
                    .attr("d", d => {
                        const startY = source.y0 || source.y;
                        const startX = source.x0 || source.x;
                        return this.drawSankeyLink(startY, startX, d.h0, d.y1, d.x1, d.h1);
                    })
                    .attr("fill", d => d.color)
                    .attr("opacity", 0)
                    .attr("stroke", "none");
                
                const flowsMerge = flows.merge(flowsEnter);
                
                flowsMerge.transition().duration(400)
                    .attr("d", d => this.drawSankeyLink(d.y0, d.x0, d.h0, d.y1, d.x1, d.h1))
                    .attr("opacity", 0.15);
                
                flowsMerge
                    .on("mouseover", function(event, d) {
                        d3.select(this).transition().duration(100).attr("opacity", 0.7);
                        d3.selectAll("g.node").filter(n => n === d.leaf)
                            .select("circle").transition().duration(100).attr("r", 14);
                        showSankeyTooltip(event, d.leaf.data.name, d.binary.name);
                    })
                    .on("mouseout", function(event, d) {
                        d3.select(this).transition().duration(100).attr("opacity", 0.15);
                        d3.selectAll("g.node").filter(n => n === d.leaf)
                            .select("circle").transition().duration(100).attr("r", d.leaf.data.is_member ? 4 : 8);
                        hideSankeyTooltip();
                    });

                flows.exit().transition().duration(400)
                    .attr("d", d => {
                        const endY = source.y;
                        const endX = source.x;
                        return this.drawSankeyLink(endY, endX, d.h0, d.y1, d.x1, d.h1);
                    })
                    .attr("opacity", 0)
                    .remove();
                    
                // Append binary nodes
                const binNodes = sankeyGroup.append("g")
                    .attr("class", "sankey-binary-nodes")
                    .selectAll("g.sankey-binary-node")
                    .data(binaries, d => d.md5);
                
                const binNodesEnter = binNodes.enter().append("g")
                    .attr("class", "sankey-binary-node")
                    .attr("transform", d => `translate(${d.y},${d.x})`)
                    .style("cursor", "pointer")
                    .style("opacity", 0);
                    
                binNodesEnter.append("rect")
                    .attr("width", 10)
                    .attr("height", d => d.height)
                    .attr("fill", d => window.getMd5Color ? window.getMd5Color(d.md5) : "var(--accent)")
                    .attr("rx", 3);
                    
                binNodesEnter.append("text")
                    .attr("x", 18)
                    .attr("y", d => d.height / 2)
                    .attr("dy", ".35em")
                    .attr("text-anchor", "start")
                    .style("fill", "var(--text)")
                    .style("font-size", "11px")
                    .style("font-weight", "bold")
                    .style("pointer-events", "none")
                    .text(d => d.name)
                    .clone(true).lower()
                    .attr("stroke", "var(--window-tray)")
                    .attr("stroke-width", 3);
                
                const binNodesMerge = binNodes.merge(binNodesEnter);
                
                binNodesMerge.transition().duration(400)
                    .attr("transform", d => `translate(${d.y},${d.x})`)
                    .style("opacity", 1);
                
                binNodesMerge
                    .on("click", (event, d) => {
                        const col = getCollectionFromHash();
                        const url = Nav.buildUIUrl(col, ['files']) + '?q=' + encodeURIComponent(d.name);
                        Nav.openPath(url, event, { title: 'Files: ' + d.name, type: 'files' });
                    })
                    .on("mouseover", function(event, d) {
                        binNodesMerge.filter(n => n.md5 === d.md5).select("rect").transition().duration(100).attr("width", 14);
                        flowsMerge.filter(link => link.binary.md5 === d.md5).transition().duration(100).attr("opacity", 0.7);
                        d.leaves.forEach(leaf => {
                            d3.selectAll("g.node").filter(n => n === leaf)
                                .select("circle").transition().duration(100).attr("r", 14);
                        });
                        showSankeyBinaryTooltip(event, d.name, d.totalWeight);
                    })
                    .on("mouseout", function(event, d) {
                        binNodesMerge.filter(n => n.md5 === d.md5).select("rect").transition().duration(100).attr("width", 10);
                        flowsMerge.filter(link => link.binary.md5 === d.md5).transition().duration(100).attr("opacity", 0.15);
                        d.leaves.forEach(leaf => {
                            d3.selectAll("g.node").filter(n => n === leaf)
                                .select("circle").transition().duration(100).attr("r", leaf.data.is_member ? 4 : 8);
                        });
                        hideSankeyTooltip();
                    });
                    
                binNodes.exit().transition().duration(400)
                    .style("opacity", 0)
                    .remove();
            }
        }
    }

    drawSankeyLink(y0, x0, h0, y1, x1, h1) {
        const cp1y = y0 + (y1 - y0) / 2;
        const cp2y = y1 - (y1 - y0) / 2;
        return `
            M ${y0} ${x0 - h0/2}
            C ${cp1y} ${x0 - h0/2}, ${cp2y} ${x1 - h1/2}, ${y1} ${x1 - h1/2}
            L ${y1} ${x1 + h1/2}
            C ${cp2y} ${x1 + h1/2}, ${cp1y} ${x0 + h0/2}, ${y0} ${x0 + h0/2}
            Z
        `;
    }

    formatFunctionInline(f) {
        if (typeof f === 'string') return `<div style="margin-bottom:2px; color:var(--meta-text-muted);">• ${f}</div>`;
        return `
            <div style="margin-bottom:2px; padding: 2px 0;">
                ${EntityRenderer.renderFunction(f, { showActions: false })}
            </div>
        `;
    }

    async showInfo(d) {
        const panel = document.getElementById('hierarchy-info-panel');
        const title = document.getElementById('hier-cluster-name');
        const stats = document.getElementById('hier-stats');
        const btn = document.getElementById('hier-view-btn');

        panel.style.display = 'block';
        title.innerText = d.data.name;

        const updatePanel = () => {
            const members = d.data.runtime_members || (d.data.snippet ? [d.data.snippet] : []);
            const isLoading = !d.data.runtime_members;

            stats.innerHTML = `
                <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--subtle);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                        <div style="color:var(--meta-text); font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:var(--subtle); font-size:0.65rem;">funcs</span></div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--accent);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                        <div style="color:var(--accent); font-weight:bold;">${d.data.stability.toFixed(2)}</div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--success);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                        <div style="color:var(--success); font-weight:bold;">${(d.data.cohesion * 100).toFixed(1)}%</div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid #ae81ff;">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Avg Features</div>
                        <div style="color:#ae81ff; font-weight:bold;">${(d.data.avg_features || 0).toFixed(1)}</div>
                    </div>
                </div>
                <div style="margin-top:15px; border-top:1px solid var(--border); padding-top:10px;">
                    <div style="font-size:0.65rem; color:var(--subtle); margin-bottom:6px; text-transform:uppercase;">
                        ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Loading Members...' : 'Sample Members (Live):'}
                    </div>
                    <div style="color:var(--meta-text); font-family:monospace; font-size:0.75rem; line-height:1.4;">
                        ${members.map(m => this.formatFunctionInline(m)).join('')}
                        ${d.data.size > members.length ? `<div style="color:var(--border); margin-top:4px; font-size:0.7rem;">... and ${d.data.size - members.length} more</div>` : ''}
                    </div>
                </div>
            `;
        };

        updatePanel();

        if (!d.data.runtime_members && d.data.uuid) {
            try {
                const col = getCurrentCollection();
                const res = await fetch(`/api/cluster/functions?collection=${col}&cluster_uuid=${d.data.uuid}&limit=5`);
                const data = await res.json();
                d.data.runtime_members = data.functions;
                updatePanel();
            } catch (e) {
                console.error("Failed to fetch runtime members", e);
            }
        }

        btn.onclick = (event) => {
            const col = getCurrentCollection();
            const uuid = d.data.uuid;
            const url = Nav.buildUIUrl(col, ['functions']) + '?cluster_uuid=' + encodeURIComponent(uuid);
            Nav.openPath(url, event, { title: 'Cluster Functions', type: 'functions' });
        };
    }

    async showTooltip(event, d) {
        this._activeD = d;
        this._hoveredNodeEl = event.currentTarget;
        if (d.data.is_member) {
            const el = getHierarchyTooltip();
            if (el) el.style.display = 'none';
            if (window.showCodePreview) {
                window.showCodePreview(d.data.id, d.data.name, d.data.addr, d.data.bin, d.data.v_size, event);
            }
            return;
        }
        const tooltip = getHierarchyTooltip();
        tooltip.style.display = 'block';
        let x = event.clientX + 20;
        let y = event.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = event.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = event.clientY - rect.height - 20;
        x = Math.max(5, x);
        y = Math.max(5, y);
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';

        tooltip.onmouseleave = (e) => {
            const relatedTarget = e.relatedTarget;
            if (this._hoveredNodeEl && (this._hoveredNodeEl === relatedTarget || this._hoveredNodeEl.contains(relatedTarget))) {
                return;
            }
            this.hideTooltip();
            if (this._hoveredNodeEl) {
                d3.select(this._hoveredNodeEl).select("circle").attr("r", 8);
                this._hoveredNodeEl = null;
            }
        };

        if (d.data.scrollOffset === undefined) d.data.scrollOffset = 0;

        // Wheel listener removed here - now handled globally in previews.js to prevent double-scroll

        this.renderTooltip(tooltip, d);

        if (!d.data.runtime_members && d.data.uuid) {
            try {
                const col = getCurrentCollection();
                const res = await fetch(`/api/cluster/functions?collection=${col}&cluster_uuid=${d.data.uuid}&limit=100`);
                const data = await res.json();
                d.data.runtime_members = data.functions;
                this.renderTooltip(tooltip, d);
            } catch (e) {
                console.error("Failed to fetch runtime members", e);
            }
        }
    }

    renderTooltip(tooltip, d) {
        const members = d.data.runtime_members || [];
        const isLoading = !d.data.runtime_members;
        const scrollOffset = d.data.scrollOffset || 0;
        const selectedFunc = members[scrollOffset];

        const isSameNode = tooltip.querySelector('.hier-tooltip-container') && this._renderedNodeUuid === d.data.uuid;

        if (!isSameNode) {
            this._renderedNodeUuid = d.data.uuid;
            tooltip.innerHTML = `
                <div class="hier-tooltip-container">
                    <div class="hier-left-col" style="padding: 12px;">
                        <div style="color:var(--accent); font-weight:bold; margin-bottom:4px; font-size:0.95rem;">${d.data.name}</div>
                        <div style="color:var(--subtle); font-size:0.65rem; margin-bottom:10px; font-family:monospace; overflow:hidden; text-overflow:ellipsis;">${d.data.uuid}</div>
                        
                        <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--subtle);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                                <div style="color:var(--meta-text); font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:var(--subtle); font-size:0.65rem;">${this.clusterType === 'file' ? 'files' : 'funcs'}</span></div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--accent);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                                <div style="color:var(--accent); font-weight:bold;">${d.data.stability.toFixed(2)}</div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--success);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                                <div style="color:var(--success); font-weight:bold;">${(d.data.cohesion * 100).toFixed(1)}%</div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid #ae81ff;">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Avg Features</div>
                                <div style="color:#ae81ff; font-weight:bold;">${(d.data.avg_features || 0).toFixed(1)}</div>
                            </div>
                        </div>

                        <div style="border-top:1px solid var(--border); padding-top:10px; flex: 1; display: flex; flex-direction: column; overflow: hidden;">
                            <div class="hier-samples-title" style="font-size:0.6rem; color:var(--subtle); margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px;">
                                ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Fetching Live...' : `${this.clusterType === 'file' ? 'Files' : 'Samples'} (${members.length}):`}
                            </div>
                            <div class="hier-function-list">
                                <div class="hier-function-list-scroll" style="transition: transform 0.1s cubic-bezier(0.17, 0.67, 0.83, 0.67);">
                                    ${members.map((m, i) => {
                                        if (this.clusterType === 'file') {
                                            return `
                                                <div style="font-size: 0.7rem; padding: 2px; color: var(--meta-text); font-family: monospace;">
                                                    ${EntityRenderer.renderFileName(m.file_name, m.file_md5, getCurrentCollection())} ${m.avtype ? `| AV: ${m.avtype}` : ''}
                                                </div>
                                            `;
                                        }
                                        const sig = formatSigComponent(m.namespace || '', m.return_type || 'void', m.function_name || 'Unknown', m.parameters || []);
                                        return `
                                            <div class="hier-function-item" data-index="${i}"
                                                 data-entity-data='${JSON.stringify(m).replace(/'/g, "&apos;")}'
                                                 oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                                                <span style="opacity: 0.5; margin-right: 6px; font-size: 0.7rem; font-family: monospace;">${i + 1}.</span>
                                                <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1;">
                                                    ${sig.ret ? `<span style="color:#ae81ff">${sig.ret}</span> ` : ''}${sig.ns ? `<span style="color:white">${sig.ns}::</span>` : ''}<span class="func-name-span" style="font-weight:bold;">${m.function_name}</span><span style="color:white">(</span>${sig.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                                                </span>
                                            </div>
                                        `;
                                    }).join('')}
                                </div>
                            </div>
                            ${d.data.size > members.length ? `<div style="color:var(--border); margin-top:6px; font-size:0.65rem;">... and ${d.data.size - members.length} more</div>` : ''}
                        </div>
                    </div>
                    <div class="hier-right-col" id="hier-snippet-container">
                        <div class="hier-snippet-placeholder" style="padding: 20px; color: var(--subtle); text-align: center; font-size: 0.8rem;">
                            ${selectedFunc ? '<i class="fas fa-spinner fa-spin"></i> Loading Preview...' : 'Select a function to preview'}
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
                listScroll.innerHTML = members.map((m, i) => {
                    const sig = formatSigComponent(m.namespace || '', m.return_type || 'void', m.function_name || 'Unknown', m.parameters || []);
                    return `
                        <div class="hier-function-item" data-index="${i}"
                             data-entity-data='${JSON.stringify(m).replace(/'/g, "&apos;")}'
                             oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                            <span style="opacity: 0.5; margin-right: 6px; font-size: 0.7rem; font-family: monospace;">${i + 1}.</span>
                            <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1;">
                                ${sig.ret ? `<span style="color:#ae81ff">${sig.ret}</span> ` : ''}${sig.ns ? `<span style="color:white">${sig.ns}::</span>` : ''}<span class="func-name-span" style="font-weight:bold;">${m.function_name}</span><span style="color:white">(</span>${sig.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                            </span>
                        </div>
                    `;
                }).join('');
            }
        }

        const listScroll = tooltip.querySelector('.hier-function-list-scroll');
        if (listScroll) {
            listScroll.style.transform = `translateY(-${scrollOffset * 30}px)`;
            Array.from(listScroll.children).forEach((itemEl, idx) => {
                const isSelected = idx === scrollOffset;
                itemEl.classList.toggle('selected', isSelected);
                const nameSpan = itemEl.querySelector('.func-name-span');
                if (nameSpan) {
                    nameSpan.style.color = isSelected ? 'var(--accent)' : 'var(--meta-text)';
                }
            });
        }

        if (selectedFunc) {
            this.updateSnippet(selectedFunc);
        }
    }

    async updateSnippet(func) {
        const container = document.getElementById('hier-snippet-container') || document.getElementById('packing-snippet-container');
        if (!container || !func || !func.function_id) return;

        const funcId = func.function_id;

        if (window.previewCache && window.previewCache.has(funcId)) {
            this.renderSnippet(window.previewCache.get(funcId), func, container);
            return;
        }

        const existingSnippet = container.querySelector('.hier-code-snippet');
        if (existingSnippet) {
            existingSnippet.style.opacity = '0.4';
        } else {
            container.innerHTML = `
                <div class="hier-snippet-placeholder" style="padding: 20px; color: var(--subtle); text-align: center; font-size: 0.8rem;">
                    <i class="fas fa-spinner fa-spin"></i> Loading Preview...
                </div>
            `;
        }

        try {
            const res = await fetch(`/api/function/code?id=${encodeURIComponent(funcId)}`);
            if (!res.ok) throw new Error("Failed");
            const data = await res.json();
            if (window.previewCache) window.previewCache.set(funcId, data);

            if (this._activeD && this._activeD.data.runtime_members) {
                const currentMembers = this._activeD.data.runtime_members;
                const currentScrollOffset = this._activeD.data.scrollOffset || 0;
                const currentSelected = currentMembers[currentScrollOffset];
                if (currentSelected && currentSelected.function_id === funcId) {
                    this.renderSnippet(data, func, container);
                }
            }
        } catch (e) {
            if (this._activeD && this._activeD.data.runtime_members) {
                const currentMembers = this._activeD.data.runtime_members;
                const currentScrollOffset = this._activeD.data.scrollOffset || 0;
                const currentSelected = currentMembers[currentScrollOffset];
                if (currentSelected && currentSelected.function_id === funcId) {
                    container.innerHTML = `<div style="padding: 20px; color: #ff5555; text-align: center;">Preview Error</div>`;
                }
            }
        }
    }

    renderSnippet(data, func, container) {
        const rows = data.rows;
        const m = data.meta || {};

        let html = `
            <div class="hier-code-snippet" style="height: 100%; display: flex; flex-direction: column;">
                <div style="padding: 10px 15px; border-bottom: 1px solid var(--meta-header-bg); background: rgba(0,0,0,0.3); flex-shrink: 0;">
                    <div style="font-size: 0.75rem; color: var(--accent); font-weight: bold; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                        ${m.return_type || ''} ${m.namespace ? m.namespace + '::' : ''}${func.function_name}
                    </div>
                    <div style="font-size: 0.6rem; color: var(--subtle); font-family: monospace; margin-top: 2px;">
                        Addr: ${func.entrypoint_address} | Feat: ${func.bsim_features_count}
                    </div>
                </div>
                <div class="c-code-container" style="overflow-y: auto; flex: 1; padding: 10px;">`;

        rows.forEach(row => {
            let lineHtml = '';
            row.tokens.forEach(t => {
                const featClass = t.has_features ? 'feature-highlight' : '';
                lineHtml += `<span class="token token-${t.type} ${featClass}">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
            });
            html += `<div class="code-line"><div class="gutter"><div class="line-num" style="font-size: 0.6rem;">${row.line_idx}</div></div><div class="line-content">${lineHtml}</div></div>`;
        });

        html += `</div></div>`;
        container.innerHTML = html;

        const codeScrollEl = container.querySelector('.c-code-container');
        if (codeScrollEl && this._codeScrollTop !== undefined) {
            codeScrollEl.scrollTop = this._codeScrollTop;
        }
    }

    hideTooltip() {
        this._activeD = null;
        this._renderedNodeUuid = null;
        const el = getHierarchyTooltip();
        if (el) el.style.display = 'none';
        if (window.hideCodePreview) {
            window.hideCodePreview();
        }
    }
}

class ClusterPacking {
    constructor(containerId, clusterType = 'function') {
        this.container = document.getElementById(containerId);
        this.clusterType = clusterType;
        this.width = this.container ? this.container.clientWidth : 800;
        this.height = this.container ? (this.container.clientHeight || 700) : 700;
        this.root = null;
        this.svg = null;
        this.params = {
            min_cluster_size: 2,
            stability_threshold: 0.0,
            show_parents: true,
            path_compression: true
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
            if (d.data && etype === 'function' && d.data.runtime_members) {
                d.data.runtime_members.forEach(m => {
                    const altEid = eid.includes(':func:') ? eid.replace(':func:', ':function:') : eid.replace(':function:', ':func:');
                    if (m.function_id === eid || m.function_id === altEid) {
                        m.user_tags = m.user_tags || [];
                        mutate(m.user_tags, tag, add);
                        updated = true;
                    }
                });
            }
        });

        if (updated && this._activeD) {
            const tooltip = getHierarchyTooltip();
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

        const collection = params.get('collection');
        const algo = params.get('algo') || 'unweighted_cosine';

        // Sync from URL params so global filters reflect in sliders
        this.params.min_cluster_size = params.has('min_count') ? (parseInt(params.get('min_count')) || 0) : 0;
        this.params.max_cluster_size = params.has('max_count') ? (parseInt(params.get('max_count')) || 0) : 0;
        this.params.cohesion_min = params.has('min_cohesion') ? (parseFloat(params.get('min_cohesion')) || 0) : 0;
        this.params.cohesion_max = params.has('max_cohesion') ? (parseFloat(params.get('max_cohesion')) || 0) : 0;
        this.params.min_features = params.has('min_features') ? (parseInt(params.get('min_features')) || 0) : 0;
        this.params.max_features = params.has('max_features') ? (parseInt(params.get('max_features')) || 0) : 0;
        this.params.stability_threshold = params.has('min_stability') ? (parseFloat(params.get('min_stability')) || 0) : 0;
        this.params.show_parents = params.get('show_parents') !== 'false';
        this.params.show_children = params.get('show_children') !== 'false';
        this.params.show_members = params.get('show_members') === 'true';
        this.params.color_by_md5 = params.get('color_by_md5') === 'true';

        const packControls = `
            <div style="position:absolute; top:20px; left:20px; z-index:10; background:var(--window-bg); padding:15px; border-radius:8px; border:1px solid var(--border); width:240px; backdrop-filter:blur(10px);">
                <div style="font-size:0.85rem; color:var(--text); font-weight:bold; margin-bottom:15px; border-bottom:1px solid var(--border); padding-bottom:5px;">Packing Filters</div>
                
                <div class="filter-category-header" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed');">
                    <span>Range Filters</span>
                    <i class="fa-solid fa-chevron-down toggle-icon"></i>
                </div>
                <div class="filter-category-content">
                    <!-- Size Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Cluster Size</label>
                            <span style="font-size:0.75rem; color:var(--accent); font-family:monospace; font-weight:bold;">
                                <span id="val-pack-min-size">${this.params.min_cluster_size}</span>-<span id="val-pack-max-size">${this.params.max_cluster_size || '∞'}</span>
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="pack-size-track" class="slider-track"></div>
                            <input type="range" id="input-pack-min-size" min="2" max="100" value="${this.params.min_cluster_size}">
                            <input type="range" id="input-pack-max-size" min="2" max="100" value="${this.params.max_cluster_size || 100}">
                        </div>
                    </div>

                    <!-- Cohesion Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Cohesion %</label>
                            <span style="font-size:0.75rem; color:var(--success); font-family:monospace; font-weight:bold;">
                                <span id="val-pack-coh-min">${(this.params.cohesion_min * 100).toFixed(0)}</span>-<span id="val-pack-coh-max">${(this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100')}</span>%
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="pack-coh-track" class="slider-track"></div>
                            <input type="range" id="input-pack-coh-min" min="0" max="1" step="0.01" value="${this.params.cohesion_min || 0}">
                            <input type="range" id="input-pack-coh-max" min="0" max="1" step="0.01" value="${this.params.cohesion_max || 1}">
                        </div>
                    </div>

                    <!-- Feature Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Avg Features</label>
                            <span style="font-size:0.75rem; color:#ae81ff; font-family:monospace; font-weight:bold;">
                                <span id="val-pack-feat-min">${this.params.min_features || 0}</span>-<span id="val-pack-feat-max">${this.params.max_features || '∞'}</span>
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="pack-feat-track" class="slider-track"></div>
                            <input type="range" id="input-pack-feat-min" min="0" max="1000" step="10" value="${this.params.min_features || 0}">
                            <input type="range" id="input-pack-feat-max" min="0" max="1000" step="10" value="${this.params.max_features || 1000}">
                        </div>
                    </div>

                    <!-- Stability Range -->
                    <div style="margin-bottom:20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <label style="font-size:0.75rem; color:var(--meta-text-muted);">Stability</label>
                            <span style="font-size:0.75rem; color:#66d9ef; font-family:monospace; font-weight:bold;">
                                <span id="val-pack-stab-min">${this.params.stability_threshold.toFixed(1)}</span>+
                            </span>
                        </div>
                        <div class="range-slider-container">
                            <div id="pack-stab-track" class="slider-track" style="width:100%; left:0%;"></div>
                            <input type="range" id="input-pack-stab-min" min="0" max="100" step="1" value="${this.params.stability_threshold || 0}" style="z-index: 3;">
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
                            <input type="checkbox" id="input-pack-show-parents" ${this.params.show_parents !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-pack-show-parents" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show parents</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-pack-show-children" ${this.params.show_children !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-pack-show-children" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show children</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-pack-path-compression" ${this.params.path_compression !== false ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-pack-path-compression" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Path compression</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-pack-show-members" ${this.params.show_members ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-pack-show-members" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Show members</label>
                        </div>
                        <div style="display:flex; align-items:center; gap:8px;">
                            <input type="checkbox" id="input-pack-color-by-md5" ${this.params.color_by_md5 ? 'checked' : ''} style="cursor:pointer; accent-color:var(--accent);">
                            <label for="input-pack-color-by-md5" style="font-size:0.75rem; color:var(--meta-text-muted); cursor:pointer; user-select:none;">Color by MD5</label>
                        </div>
                    </div>
                </div>

                <button id="pack-refresh-btn" class="btn-primary" style="padding:10px; font-size:0.75rem; width:100%; margin-top:5px; text-transform:uppercase; letter-spacing:1.5px; font-weight:bold;">Update Visualization</button>
            </div>

            <div style="position:absolute; bottom:20px; right:20px; z-index:10; background:var(--window-bg); padding:15px; border-radius:8px; border:1px solid var(--border); max-width:300px; display:none;" id="pack-info-panel">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <h4 id="pack-cluster-name" style="margin:0; color:var(--accent); font-size:0.9rem;">Cluster Info</h4>
                    <button onclick="this.parentElement.parentElement.style.display='none'" style="background:none; border:none; color:var(--subtle); cursor:pointer;">&times;</button>
                </div>
                <div id="pack-stats" style="font-size:0.85rem; color:var(--meta-text-muted);"></div>
                <div style="margin-top:15px; display:flex; gap:10px;">
                    <button id="pack-view-btn" class="btn-action" style="flex:1">Functions</button>
                </div>
            </div>

            <div id="pack-loader" style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:var(--accent); background:var(--window-bg);">
                <div style="text-align:center;">
                    <div class="spinner" style="margin-bottom:15px;"></div>
                    <div style="font-size:0.9rem; letter-spacing:1px;">Rebuilding Packing Layout...</div>
                </div>
            </div>
        `;

        this.container.innerHTML = packControls;

        // Initialize defaults
        this.params.min_cluster_size = this.params.min_cluster_size || 2;
        this.params.max_cluster_size = this.params.max_cluster_size || 0;
        this.params.cohesion_min = this.params.cohesion_min || 0;
        this.params.cohesion_max = this.params.cohesion_max || 0;
        this.params.min_features = this.params.min_features || 0;
        this.params.max_features = this.params.max_features || 0;
        if (this.params.show_parents === undefined) this.params.show_parents = true;
        if (this.params.path_compression === undefined) this.params.path_compression = true;

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
        const sMin = document.getElementById('input-pack-min-size');
        const sMax = document.getElementById('input-pack-max-size');
        const sUpdate = () => {
            if (parseInt(sMin.value) > parseInt(sMax.value)) sMin.value = sMax.value;
            this.params.min_cluster_size = parseInt(sMin.value);
            this.params.max_cluster_size = parseInt(sMax.value) === 100 ? 0 : parseInt(sMax.value);
            document.getElementById('val-pack-min-size').innerText = this.params.min_cluster_size;
            document.getElementById('val-pack-max-size').innerText = this.params.max_cluster_size || '∞';
            updateTrack('pack-size', parseInt(sMin.value), parseInt(sMax.value), 2, 100);
        };
        sMin.oninput = sUpdate;
        sMax.oninput = sUpdate;
        updateTrack('pack-size', this.params.min_cluster_size, this.params.max_cluster_size || 100, 2, 100);

        const cMin = document.getElementById('input-pack-coh-min');
        const cMax = document.getElementById('input-pack-coh-max');
        const cUpdate = () => {
            if (parseFloat(cMin.value) > parseFloat(cMax.value)) cMin.value = cMax.value;
            this.params.cohesion_min = parseFloat(cMin.value);
            this.params.cohesion_max = parseFloat(cMax.value) >= 1 ? 0 : parseFloat(cMax.value);
            document.getElementById('val-pack-coh-min').innerText = (this.params.cohesion_min * 100).toFixed(0);
            document.getElementById('val-pack-coh-max').innerText = this.params.cohesion_max > 0 ? (this.params.cohesion_max * 100).toFixed(0) : '100';
            updateTrack('pack-coh', this.params.cohesion_min, parseFloat(cMax.value), 0, 1);
        };
        cMin.oninput = cUpdate;
        cMax.oninput = cUpdate;
        updateTrack('pack-coh', this.params.cohesion_min, this.params.cohesion_max || 1, 0, 1);

        const fMin = document.getElementById('input-pack-feat-min');
        const fMax = document.getElementById('input-pack-feat-max');
        const fUpdate = () => {
            if (parseInt(fMin.value) > parseInt(fMax.value)) fMin.value = fMax.value;
            this.params.min_features = parseInt(fMin.value);
            this.params.max_features = parseInt(fMax.value) === 1000 ? 0 : parseInt(fMax.value);
            document.getElementById('val-pack-feat-min').innerText = this.params.min_features;
            document.getElementById('val-pack-feat-max').innerText = this.params.max_features || '∞';
            updateTrack('pack-feat', parseInt(fMin.value), parseInt(fMax.value), 0, 1000);
        };
        fMin.oninput = fUpdate;
        fMax.oninput = fUpdate;
        updateTrack('pack-feat', this.params.min_features, this.params.max_features || 1000, 0, 1000);

        const stMin = document.getElementById('input-pack-stab-min');
        const stUpdate = () => {
            this.params.stability_threshold = parseFloat(stMin.value);
            document.getElementById('val-pack-stab-min').innerText = this.params.stability_threshold.toFixed(1) + '+';
            updateTrack('pack-stab', this.params.stability_threshold, 100, 0, 100);
        };
        stMin.oninput = stUpdate;
        updateTrack('pack-stab', this.params.stability_threshold, 100, 0, 100);

        const spCheck = document.getElementById('input-pack-show-parents');
        spCheck.onchange = () => {
            this.params.show_parents = spCheck.checked;
        };

        const scCheck = document.getElementById('input-pack-show-children');
        scCheck.onchange = () => {
            this.params.show_children = scCheck.checked;
        };

        const pcCheck = document.getElementById('input-pack-path-compression');
        pcCheck.onchange = () => {
            this.params.path_compression = pcCheck.checked;
        };

        const smCheck = document.getElementById('input-pack-show-members');
        if (smCheck) {
            smCheck.onchange = () => { this.params.show_members = smCheck.checked; };
        }
        const md5Check = document.getElementById('input-pack-color-by-md5');
        if (md5Check) {
            md5Check.onchange = () => { this.params.color_by_md5 = md5Check.checked; };
        }

        document.getElementById('pack-refresh-btn').onclick = () => {
            const { view } = parseRestfulPath();
            const p = new URLSearchParams(window.location.search);
            
            if (this.params.min_cluster_size > 0) p.set('min_count', this.params.min_cluster_size); else p.delete('min_count');
            if (this.params.max_cluster_size > 0) p.set('max_count', this.params.max_cluster_size); else p.delete('max_count');
            if (this.params.cohesion_min > 0) p.set('min_cohesion', this.params.cohesion_min); else p.delete('min_cohesion');
            if (this.params.cohesion_max > 0) p.set('max_cohesion', this.params.cohesion_max); else p.delete('max_cohesion');
            if (this.params.min_features > 0) p.set('min_features', this.params.min_features); else p.delete('min_features');
            if (this.params.max_features > 0) p.set('max_features', this.params.max_features); else p.delete('max_features');
            if (this.params.stability_threshold > 0) p.set('min_stability', this.params.stability_threshold); else p.delete('min_stability');
            p.set('show_parents', this.params.show_parents);
            p.set('show_children', this.params.show_children);
            p.set('show_members', this.params.show_members ? 'true' : 'false');
            p.set('color_by_md5', this.params.color_by_md5 ? 'true' : 'false');
            
            if (typeof navigate === 'function') {
                navigate(view || 'clusters', p);
            } else {
                window.location.search = p.toString();
            }
        };

        try {
            const queryParams = new URLSearchParams(params.toString());
            
            if (this.params.min_cluster_size > 0) queryParams.set('min_count', this.params.min_cluster_size);
            if (this.params.max_cluster_size < 1000 && this.params.max_cluster_size > 0) queryParams.set('max_count', this.params.max_cluster_size);
            if (this.params.cohesion_min > 0) queryParams.set('min_cohesion', this.params.cohesion_min);
            if (this.params.cohesion_max < 1 && this.params.cohesion_max > 0) queryParams.set('max_cohesion', this.params.cohesion_max);
            if (this.params.min_features > 0) queryParams.set('min_features', this.params.min_features);
            if (this.params.max_features < 1000 && this.params.max_features > 0) queryParams.set('max_features', this.params.max_features);
            if (this.params.stability_threshold > 0) queryParams.set('min_stability', this.params.stability_threshold);
            queryParams.set('show_parents', this.params.show_parents !== false);
            queryParams.set('show_children', this.params.show_children !== false);
            queryParams.set('show_members', this.params.show_members === true);

            const url = `/api/cluster/list?` + queryParams.toString();
            const res = await fetch(url, { signal });
            if (!res.ok) throw new Error("Data not found");
            const data = await res.json();

            let nodes = (data.results || []).map(m => ({
                id: String(m.cluster_id),
                parent: m.parent ? String(m.parent) : null,
                name: m.cluster_name || `Cluster ${m.cluster_id}`,
                uuid: m.cluster_uuid,
                size: m.count || 0,
                stability: m.avg_stability || 0.0,
                cohesion: m.cohesion_score || 0.0,
                avg_features: m.avg_features || 0.0,
                snippet: m.snippet || "",
                members: m.sample_members || [],
                direct_members: m.direct_members || []
            }));

            if (this.params.show_members) {
                const memberNodes = [];
                nodes.forEach(c => {
                    if (c.direct_members && c.direct_members.length > 0) {
                        c.direct_members.forEach(m => {
                            memberNodes.push({
                                id: m.id,
                                parent: String(c.id),
                                name: m.name,
                                md5: m.file_md5,
                                is_member: true,
                                addr: m.addr,
                                bin: m.bin,
                                v_size: m.v_size,
                                size: 1,
                                stability: 0,
                                cohesion: 0,
                                avg_features: 0,
                                members: []
                            });
                        });
                    }
                });
                nodes = nodes.concat(memberNodes);
            }

            if (!nodes || nodes.length === 0) {
                this.container.innerHTML += `<div style="position:absolute; top:50%; left:50%; transform:translate(-50%, -50%); color:var(--meta-text-muted); text-align:center; width:100%;">No clusters match these criteria.<br><span style="font-size:0.8rem; color:var(--subtle);">Try lowering the stability cut or minimum size.</span></div>`;
                const loader = document.getElementById('pack-loader');
                if (loader) loader.remove();
                return;
            }

            this.render(nodes);
        } catch (e) {
            if (e.name === 'AbortError') return;
            this.container.innerHTML = `<div style="margin:auto; color:#ff5555; text-align:center; padding: 20px;">Error loading packing layout: ${e.message}</div>`;
        }
    }

    render(nodes) {
        this.rawNodes = nodes;
        const self = this;
        const loader = document.getElementById('pack-loader');
        if (loader) loader.remove();

        if (this.params.path_compression !== false) {
            let compressedNodes = JSON.parse(JSON.stringify(nodes));
            let changed = true;
            while (changed) {
                changed = false;
                const childCounts = {};
                compressedNodes.forEach(n => {
                    if (n.parent) {
                        childCounts[n.parent] = (childCounts[n.parent] || 0) + 1;
                    }
                });

                for (let i = 0; i < compressedNodes.length; i++) {
                    const node = compressedNodes[i];
                    const count = childCounts[node.id] || 0;

                    if (count === 1 && node.parent !== null) {
                        const child = compressedNodes.find(n => n.parent === node.id);
                        if (child) {
                            child.parent = node.parent;
                            compressedNodes.splice(i, 1);
                            changed = true;
                            break;
                        }
                    }
                }
            }
            nodes = compressedNodes;
        }

        const width = this.container.clientWidth;
        const height = this.container.offsetHeight || 700;

        d3.select(this.container).selectAll("svg").remove();

        this.svg = d3.select(this.container).append("svg")
            .attr("viewBox", `0 0 ${width} ${height}`)
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("style", "background:var(--window-bg); cursor:pointer;");

        const stratify = d3.stratify()
            .id(d => d.id)
            .parentId(d => d.parent);

        nodes.forEach(n => {
            if (n.parent && !nodes.find(p => p.id === n.parent)) {
                n.parent = null;
            }
        });

        const rootNodes = nodes.filter(n => !n.parent || !nodes.find(p => p.id === n.parent));

        if (rootNodes.length === 0) {
            this.root = null;
        } else if (rootNodes.length === 1) {
            this.root = stratify(nodes);
        } else {
            const virtualRootId = "VIRTUAL_ROOT";
            const augmentedNodes = [
                {
                    id: virtualRootId,
                    parent: null,
                    name: "All Clusters",
                    uuid: "root",
                    size: nodes.reduce((acc, n) => (!n.parent || !nodes.find(p => p.id === n.parent)) ? acc + n.size : acc, 0),
                    stability: 0,
                    cohesion: 0,
                    members: []
                },
                ...nodes.map(n => {
                    if (!n.parent || !nodes.find(p => p.id === n.parent)) {
                        return { ...n, parent: virtualRootId };
                    }
                    return n;
                })
            ];
            this.root = stratify(augmentedNodes);
        }

        if (!this.root) return;

        this.root.sum(d => d.children && d.children.length > 0 ? 0 : (d.size || 1))
            .sort((a, b) => b.value - a.value);

        const packLayout = d3.pack()
            .size([width - 8, height - 8])
            .padding(4);

        packLayout(this.root);

        this.root.descendants().forEach(d => {
            d.x += 4;
            d.y += 4;
        });

        let focus = this.root;
        const g = this.svg.append("g");

        const getCohesionFill = (d) => {
            if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return "rgba(255,255,255,0.01)";
            if (d.data.is_member) {
                if (this.params.color_by_md5 && d.data.md5) {
                    const baseColor = window.getMd5Color(d.data.md5);
                    // Extract RGB from rgb(...) and add opacity
                    const rgb = baseColor.match(/\d+/g);
                    if (rgb && rgb.length === 3) {
                        return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, 0.15)`;
                    }
                    return baseColor;
                }
                return "color-mix(in srgb, var(--token-register) 15%, transparent)";
            }
            const cohesion = d.data.cohesion || 0;
            const hue = Math.max(0, Math.min(120, cohesion * 120));
            const opacity = d.children ? 0.03 : 0.15;
            return `hsla(${hue}, var(--color-s-med), var(--color-l-dim), ${opacity})`;
        };

        const getCohesionStroke = (d) => {
            if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return "rgba(255,255,255,0.15)";
            if (d.data.is_member) {
                if (this.params.color_by_md5 && d.data.md5) {
                    return window.getMd5Color(d.data.md5);
                }
                return "var(--accent, #66d9ef)";
            }
            const cohesion = d.data.cohesion || 0;
            const hue = Math.max(0, Math.min(120, cohesion * 120));
            return `hsl(${hue}, var(--color-s-low), var(--color-l-low))`;
        };

        const node = g.selectAll("circle")
            .data(this.root.descendants(), d => d.data.id)
            .join("circle")
            .attr("cx", d => d.x)
            .attr("cy", d => d.y)
            .attr("r", d => d.data.is_member ? Math.max(1, d.r * 0.5) : d.r)
            .attr("fill", d => getCohesionFill(d))
            .attr("stroke", d => getCohesionStroke(d))
            .attr("stroke-width", d => d.data.is_member ? 1 : (d.children ? 1 : 1.5))
            .on("mouseover", function (event, d) {
                event.stopPropagation();
                if (d.data.id === "VIRTUAL_ROOT") return;
                d3.select(this)
                    .attr("stroke-width", 3)
                    .style("filter", `drop-shadow(0 0 8px ${getCohesionStroke(d)})`);
                self.showTooltip(event, d);
            })
            .on("mousemove", function (event, d) {
                if (window.moveCodePreview) window.moveCodePreview(event);
            })
            .on("mouseout", function (event, d) {
                event.stopPropagation();
                if (d.data.id === "VIRTUAL_ROOT") return;

                const relatedTarget = event.relatedTarget;
                const tooltip = getHierarchyTooltip();
                if (tooltip && (tooltip === relatedTarget || tooltip.contains(relatedTarget))) {
                    return;
                }

                d3.select(this)
                    .attr("stroke-width", d => d.children ? 1 : 1.5)
                    .style("filter", "none");
                self.hideTooltip();
            })
            .on("click", (event, d) => {
                if (d.data.is_member) {
                    if (window.showFunctionCodeById) {
                        window.showFunctionCodeById(d.data.id, d.data.name, '', event);
                    } else if (typeof showFunctionCodeById === 'function') {
                        showFunctionCodeById(d.data.id, d.data.name, '', event);
                    }
                    event.stopPropagation();
                    return;
                }
                if (focus !== d) {
                    zoom(event, d);
                    event.stopPropagation();
                }
            })
            .on("contextmenu", (event, d) => {
                if (d.data.id === "VIRTUAL_ROOT" || d.data.uuid === "root") return;
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    const type = d.data.is_member ? 'node' : 'cluster';
                    window.showGraphContextMenu(event, type, d.data);
                }
            });

        const label = g.selectAll("text")
            .data(this.root.descendants())
            .join("text")
            .attr("text-anchor", "middle")
            .attr("dy", ".35em")
            .style("fill", "var(--text)")
            .style("font-family", "sans-serif")
            .style("pointer-events", "none")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
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

        const zoomBehavior = d3.zoom()
            .scaleExtent([0.1, 100])
            .on("zoom", (event) => {
                g.attr("transform", event.transform);
                const k = event.transform.k;

                label.style("font-size", d => {
                    const rPx = d.r * k;
                    const fontSize = Math.max(8, Math.min(18, rPx / 5));
                    return `${fontSize / k}px`;
                });

                label.style("opacity", d => {
                    if (d.data.id === "VIRTUAL_ROOT") return 0;
                    const rPx = d.r * k;
                    if (rPx < 25) return 0;
                    if (d.children && d.children.length > 0 && rPx > 150) return 0;
                    return 1;
                });

                label.style("display", d => {
                    if (d.data.id === "VIRTUAL_ROOT") return "none";
                    const rPx = d.r * k;
                    if (rPx < 25) return "none";
                    if (d.children && d.children.length > 0 && rPx > 150) return "none";
                    return "inline";
                });
            });

        this.svg.call(zoomBehavior)
            .on("dblclick.zoom", null);

        this.svg.on("click", (event) => zoom(event, this.root));

        const initS = Math.min(width, height) / (this.root.r * 2);
        const initTx = width / 2 - this.root.x * initS;
        const initTy = height / 2 - this.root.y * initS;
        this.svg.call(zoomBehavior.transform, d3.zoomIdentity.translate(initTx, initTy).scale(initS));

        function zoom(event, d) {
            focus = d;

            const s = Math.min(width, height) / (d.r * 2);
            const tx = width / 2 - d.x * s;
            const ty = height / 2 - d.y * s;

            self.svg.transition()
                .duration(750)
                .call(zoomBehavior.transform, d3.zoomIdentity.translate(tx, ty).scale(s));

            if (d.data.id !== "VIRTUAL_ROOT" && d.data.uuid !== "root") {
                self.showInfo(d);
            } else {
                const panel = document.getElementById('pack-info-panel');
                if (panel) panel.style.display = 'none';
            }
        }
    }

    formatFunctionInline(f) {
        if (typeof f === 'string') return `<div style="margin-bottom:2px; color:var(--meta-text-muted);">• ${f}</div>`;
        return `
            <div style="margin-bottom:2px; padding: 2px 0;">
                ${EntityRenderer.renderFunction(f, { showActions: false })}
            </div>
        `;
    }

    async showInfo(d) {
        const panel = document.getElementById('pack-info-panel');
        const title = document.getElementById('pack-cluster-name');
        const stats = document.getElementById('pack-stats');
        const btn = document.getElementById('pack-view-btn');

        if (!panel) return;
        panel.style.display = 'block';
        title.innerText = d.data.name;

        const updatePanel = () => {
            const members = d.data.runtime_members || (d.data.snippet ? [d.data.snippet] : []);
            const isLoading = !d.data.runtime_members;

            stats.innerHTML = `
                <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--subtle);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                        <div style="color:var(--meta-text); font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:var(--subtle); font-size:0.65rem;">funcs</span></div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--accent);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                        <div style="color:var(--accent); font-weight:bold;">${d.data.stability.toFixed(2)}</div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid var(--success);">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                        <div style="color:var(--success); font-weight:bold;">${(d.data.cohesion * 100).toFixed(1)}%</div>
                    </div>
                    <div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:4px; border-left:2px solid #ae81ff;">
                        <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Avg Features</div>
                        <div style="color:#ae81ff; font-weight:bold;">${(d.data.avg_features || 0).toFixed(1)}</div>
                    </div>
                </div>
                <div style="margin-top:15px; border-top:1px solid var(--border); padding-top:10px;">
                    <div style="font-size:0.65rem; color:var(--subtle); margin-bottom:6px; text-transform:uppercase;">
                        ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Loading Members...' : 'Sample Members (Live):'}
                    </div>
                    <div style="color:var(--meta-text); font-family:monospace; font-size:0.75rem; line-height:1.4;">
                        ${members.map(m => this.formatFunctionInline(m)).join('')}
                        ${d.data.size > members.length ? `<div style="color:var(--border); margin-top:4px; font-size:0.7rem;">... and ${d.data.size - members.length} more</div>` : ''}
                    </div>
                </div>
            `;
        };

        updatePanel();

        if (!d.data.runtime_members && d.data.uuid) {
            try {
                const col = getCurrentCollection();
                const res = await fetch(`/api/cluster/functions?collection=${col}&cluster_uuid=${d.data.uuid}&limit=5`);
                const data = await res.json();
                d.data.runtime_members = data.functions;
                updatePanel();
            } catch (e) {
                console.error("Failed to fetch runtime members", e);
            }
        }

        btn.onclick = (event) => {
            const col = getCurrentCollection();
            const uuid = d.data.uuid;
            const url = Nav.buildUIUrl(col, ['functions']) + '?cluster_uuid=' + encodeURIComponent(uuid);
            Nav.openPath(url, event, { title: 'Cluster Functions', type: 'functions' });
        };
    }

    async showTooltip(event, d) {
        this._activeD = d;

        if (d.data.is_member) {
            if (this._tooltipTimeout) clearTimeout(this._tooltipTimeout);
            this._tooltipTimeout = setTimeout(() => {
                if (this._activeD !== d) return;
                const el = getHierarchyTooltip();
                if (el) el.style.display = 'none';
                if (window.showCodePreview) {
                    window.showCodePreview(d.data.id, d.data.name, d.data.addr, d.data.bin, d.data.v_size, event);
                }
            }, 150);
            return;
        }

        if (this._tooltipTimeout) clearTimeout(this._tooltipTimeout);
        this._tooltipTimeout = setTimeout(async () => {
            if (this._activeD !== d) return;

            const tooltip = getHierarchyTooltip();
            tooltip.style.display = 'block';

            tooltip.onmouseleave = (e) => {
                const relatedTarget = e.relatedTarget;
                if (this._hoveredNodeEl && (this._hoveredNodeEl === relatedTarget || this._hoveredNodeEl.contains(relatedTarget))) {
                    return;
                }
                this.hideTooltip();
                if (this._hoveredNodeEl) {
                    d3.select(this._hoveredNodeEl)
                        .attr("stroke-width", d => d.children ? 1 : 1.5)
                        .style("filter", "none");
                    this._hoveredNodeEl = null;
                }
            };

            let x = event.clientX + 20;
            let y = event.clientY + 20;
            const rect = tooltip.getBoundingClientRect();
            if (x + rect.width > window.innerWidth) x = event.clientX - rect.width - 20;
            if (y + rect.height > window.innerHeight) y = event.clientY - rect.height - 20;
            x = Math.max(5, x);
            y = Math.max(5, y);
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';

            if (d.data.scrollOffset === undefined) d.data.scrollOffset = 0;

            // Wheel listener removed here - now handled globally in previews.js to prevent double-scroll

            this.renderTooltip(tooltip, d);

            if (!d.data.runtime_members && d.data.uuid) {
                try {
                    const col = getCurrentCollection();
                    const res = await fetch(`/api/cluster/functions?collection=${col}&cluster_uuid=${d.data.uuid}&limit=100`);
                    if (this._activeD !== d) return;
                    const data = await res.json();
                    if (this._activeD !== d) return;
                    d.data.runtime_members = data.functions;
                    this.renderTooltip(tooltip, d);
                } catch (e) {
                    console.error("Failed to fetch runtime members", e);
                }
            }
        }, 150);
    }

    renderTooltip(tooltip, d) {
        const members = d.data.runtime_members || [];
        const isLoading = !d.data.runtime_members;
        const scrollOffset = d.data.scrollOffset || 0;
        const selectedFunc = members[scrollOffset];

        const isSameNode = tooltip.querySelector('.hier-tooltip-container') && this._renderedNodeUuid === d.data.uuid;

        if (!isSameNode) {
            this._renderedNodeUuid = d.data.uuid;
            tooltip.innerHTML = `
                <div class="hier-tooltip-container">
                    <div class="hier-left-col" style="padding: 12px;">
                        <div style="color:var(--accent); font-weight:bold; margin-bottom:4px; font-size:0.95rem;">${d.data.name}</div>
                        <div style="color:var(--subtle); font-size:0.65rem; margin-bottom:10px; font-family:monospace; overflow:hidden; text-overflow:ellipsis;">${d.data.uuid}</div>
                        
                        <div style="margin-bottom:12px; display:grid; grid-template-columns: 1fr 1fr; gap:8px; font-size:0.75rem;">
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--subtle);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Size</div>
                                <div style="color:var(--meta-text); font-weight:bold;">${d.data.size} <span style="font-weight:normal; color:var(--subtle); font-size:0.65rem;">${this.clusterType === 'file' ? 'files' : 'funcs'}</span></div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--accent);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Stability</div>
                                <div style="color:var(--accent); font-weight:bold;">${d.data.stability.toFixed(2)}</div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid var(--success);">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Cohesion</div>
                                <div style="color:var(--success); font-weight:bold;">${(d.data.cohesion * 100).toFixed(1)}%</div>
                            </div>
                            <div style="background:rgba(255,255,255,0.05); padding:4px 8px; border-radius:4px; border-left:2px solid #ae81ff;">
                                <div class="dim" style="font-size:0.6rem; text-transform:uppercase; margin-bottom:2px;">Avg Features</div>
                                <div style="color:#ae81ff; font-weight:bold;">${(d.data.avg_features || 0).toFixed(1)}</div>
                            </div>
                        </div>

                        <div style="border-top:1px solid var(--border); padding-top:10px; flex: 1; display: flex; flex-direction: column; overflow: hidden;">
                            <div class="hier-samples-title" style="font-size:0.6rem; color:var(--subtle); margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px;">
                                ${isLoading ? '<i class="fas fa-spinner fa-spin"></i> Fetching Live...' : `${this.clusterType === 'file' ? 'Files' : 'Samples'} (${members.length}):`}
                            </div>
                            <div class="hier-function-list">
                                <div class="hier-function-list-scroll" style="transition: transform 0.1s cubic-bezier(0.17, 0.67, 0.83, 0.67);">
                                    ${members.map((m, i) => {
                                        if (this.clusterType === 'file') {
                                            return `
                                                <div style="font-size: 0.7rem; padding: 2px; color: var(--meta-text); font-family: monospace;">
                                                    ${EntityRenderer.renderFileName(m.file_name, m.file_md5, getCurrentCollection())} ${m.avtype ? `| AV: ${m.avtype}` : ''}
                                                </div>
                                            `;
                                        }
                                        const sig = formatSigComponent(m.namespace || '', m.return_type || 'void', m.function_name || 'Unknown', m.parameters || []);
                                        return `
                                            <div class="hier-function-item" data-index="${i}"
                                                 data-entity-data='${JSON.stringify(m).replace(/'/g, "&apos;")}'
                                                 oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                                                <span style="opacity: 0.5; margin-right: 6px; font-size: 0.7rem; font-family: monospace;">${i + 1}.</span>
                                                <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1;">
                                                    ${sig.ret ? `<span style="color:#ae81ff">${sig.ret}</span> ` : ''}${sig.ns ? `<span style="color:white">${sig.ns}::</span>` : ''}<span class="func-name-span" style="font-weight:bold;">${m.function_name}</span><span style="color:white">(</span>${sig.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                                                </span>
                                            </div>
                                        `;
                                    }).join('')}
                                </div>
                            </div>
                            ${d.data.size > members.length ? `<div style="color:var(--border); margin-top:6px; font-size:0.65rem;">... and ${d.data.size - members.length} more</div>` : ''}
                        </div>
                    </div>
                    <div class="hier-right-col" id="hier-snippet-container">
                        <div class="hier-snippet-placeholder" style="padding: 20px; color: var(--subtle); text-align: center; font-size: 0.8rem;">
                            ${selectedFunc ? '<i class="fas fa-spinner fa-spin"></i> Loading Preview...' : 'Select a function to preview'}
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
                listScroll.innerHTML = members.map((m, i) => {
                    const sig = formatSigComponent(m.namespace || '', m.return_type || 'void', m.function_name || 'Unknown', m.parameters || []);
                    return `
                        <div class="hier-function-item" data-index="${i}"
                             data-entity-data='${JSON.stringify(m).replace(/'/g, "&apos;")}'
                             oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                            <span style="opacity: 0.5; margin-right: 6px; font-size: 0.7rem; font-family: monospace;">${i + 1}.</span>
                            <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1;">
                                ${sig.ret ? `<span style="color:#ae81ff">${sig.ret}</span> ` : ''}${sig.ns ? `<span style="color:white">${sig.ns}::</span>` : ''}<span class="func-name-span" style="font-weight:bold;">${m.function_name}</span><span style="color:white">(</span>${sig.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                            </span>
                        </div>
                    `;
                }).join('');
            }
        }

        const listScroll = tooltip.querySelector('.hier-function-list-scroll');
        if (listScroll) {
            listScroll.style.transform = `translateY(-${scrollOffset * 30}px)`;
            Array.from(listScroll.children).forEach((itemEl, idx) => {
                const isSelected = idx === scrollOffset;
                itemEl.classList.toggle('selected', isSelected);
                const nameSpan = itemEl.querySelector('.func-name-span');
                if (nameSpan) {
                    nameSpan.style.color = isSelected ? 'var(--accent)' : 'var(--meta-text)';
                }
            });
        }

        if (selectedFunc) {
            this.updateSnippet(selectedFunc);
        }
    }

    async updateSnippet(func) {
        const container = document.getElementById('hier-snippet-container') || document.getElementById('packing-snippet-container');
        if (!container || !func || !func.function_id) return;

        const funcId = func.function_id;

        if (window.previewCache && window.previewCache.has(funcId)) {
            this.renderSnippet(window.previewCache.get(funcId), func, container);
            return;
        }

        const existingSnippet = container.querySelector('.hier-code-snippet');
        if (existingSnippet) {
            existingSnippet.style.opacity = '0.4';
        } else {
            container.innerHTML = `
                <div class="hier-snippet-placeholder" style="padding: 20px; color: var(--subtle); text-align: center; font-size: 0.8rem;">
                    <i class="fas fa-spinner fa-spin"></i> Loading Preview...
                </div>
            `;
        }

        try {
            const res = await fetch(`/api/function/code?id=${encodeURIComponent(funcId)}`);
            if (!res.ok) throw new Error("Failed");
            const data = await res.json();
            if (window.previewCache) window.previewCache.set(funcId, data);

            if (this._activeD && this._activeD.data.runtime_members) {
                const currentMembers = this._activeD.data.runtime_members;
                const currentScrollOffset = this._activeD.data.scrollOffset || 0;
                const currentSelected = currentMembers[currentScrollOffset];
                if (currentSelected && currentSelected.function_id === funcId) {
                    this.renderSnippet(data, func, container);
                }
            }
        } catch (e) {
            if (this._activeD && this._activeD.data.runtime_members) {
                const currentMembers = this._activeD.data.runtime_members;
                const currentScrollOffset = this._activeD.data.scrollOffset || 0;
                const currentSelected = currentMembers[currentScrollOffset];
                if (currentSelected && currentSelected.function_id === funcId) {
                    container.innerHTML = `<div style="padding: 20px; color: #ff5555; text-align: center;">Preview Error</div>`;
                }
            }
        }
    }

    renderSnippet(data, func, container) {
        const rows = data.rows;
        const m = data.meta || {};

        let html = `
            <div class="hier-code-snippet" style="height: 100%; display: flex; flex-direction: column;">
                <div style="padding: 10px 15px; border-bottom: 1px solid var(--meta-header-bg); background: rgba(0,0,0,0.3); flex-shrink: 0;">
                    <div style="font-size: 0.75rem; color: var(--accent); font-weight: bold; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                        ${m.return_type || ''} ${m.namespace ? m.namespace + '::' : ''}${func.function_name}
                    </div>
                    <div style="font-size: 0.6rem; color: var(--subtle); font-family: monospace; margin-top: 2px;">
                        Addr: ${func.entrypoint_address} | Feat: ${func.bsim_features_count}
                    </div>
                </div>
                <div class="c-code-container" style="overflow-y: auto; flex: 1; padding: 10px;">`;

        rows.forEach(row => {
            let lineHtml = '';
            row.tokens.forEach(t => {
                const featClass = t.has_features ? 'feature-highlight' : '';
                lineHtml += `<span class="token token-${t.type} ${featClass}">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
            });
            html += `<div class="code-line"><div class="gutter"><div class="line-num" style="font-size: 0.6rem;">${row.line_idx}</div></div><div class="line-content">${lineHtml}</div></div>`;
        });

        html += `</div></div>`;
        container.innerHTML = html;

        const codeScrollEl = container.querySelector('.c-code-container');
        if (codeScrollEl && this._codeScrollTop !== undefined) {
            codeScrollEl.scrollTop = this._codeScrollTop;
        }
    }

    hideTooltip() {
        this._activeD = null;
        this._renderedNodeUuid = null;
        if (this._tooltipTimeout) {
            clearTimeout(this._tooltipTimeout);
            this._tooltipTimeout = null;
        }
        const el = getHierarchyTooltip();
        if (el) el.style.display = 'none';
        if (window.hideCodePreview) {
            window.hideCodePreview();
        }
    }
}

const clusterTooltipMockCache = new Map();
window.clusterTooltipMockCache = clusterTooltipMockCache;

function showClusterTableTooltip(event, uuid, name, size, stability, cohesion, avg_features, customMembers = null, clusterType = 'function') {
    const isMenuOpen = window.graphContextMenuOpen || (window.top && window.top.graphContextMenuOpen);
    if (isMenuOpen) return;
    if (window.setTrigger) window.setTrigger(event);
    if (!window.hierarchyInstance) {
        window.hierarchyInstance = new ClusterHierarchy('hierarchy-view-container', clusterType);
    }
    // Update type if changed
    window.hierarchyInstance.clusterType = clusterType;

    if (!clusterTooltipMockCache.has(uuid)) {
        clusterTooltipMockCache.set(uuid, {
            data: { uuid, name, size, stability, cohesion, avg_features, scrollOffset: 0 }
        });
    }
    const mockD = clusterTooltipMockCache.get(uuid);
    if (customMembers) {
        mockD.data.runtime_members = customMembers;
    }
    window.hierarchyInstance.showTooltip(event, mockD);
}

function hideClusterTableTooltip() {
    if (window.hideAllTooltips) window.hideAllTooltips();
    else if (window.hierarchyInstance) window.hierarchyInstance.hideTooltip();
}

function moveClusterTableTooltip(e) {
    const tooltip = getHierarchyTooltip();
    if (tooltip && tooltip.style.display === 'block') {
        let x = e.clientX + 20;
        let y = e.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = e.clientY - rect.height - 20;
        x = Math.max(5, x);
        y = Math.max(5, y);
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }
}

window.showClusterTableTooltip = showClusterTableTooltip;
window.hideClusterTableTooltip = hideClusterTableTooltip;
window.moveClusterTableTooltip = moveClusterTableTooltip;

window.showClusterTableTooltipFromIframe = function (iframeId, uuid, name, size, stability, cohesion, avg_features, e, customMembers = null) {
    const iframe = document.getElementById(iframeId);
    if (!iframe) {
        showClusterTableTooltip(e, uuid, name, size, stability, cohesion, avg_features, customMembers);
        return;
    }
    const rect = iframe.getBoundingClientRect();
    const fakeEvent = { 
        clientX: e.clientX + rect.left, 
        clientY: e.clientY + rect.top,
        target: e.target,
        currentTarget: e.currentTarget,
        preventDefault: () => { if (e.preventDefault) e.preventDefault(); },
        stopPropagation: () => { if (e.stopPropagation) e.stopPropagation(); }
    };
    showClusterTableTooltip(fakeEvent, uuid, name, size, stability, cohesion, avg_features, customMembers);
};

window.moveClusterTableTooltipFromIframe = function (iframeId, e) {
    const iframe = document.getElementById(iframeId);
    if (!iframe) {
        moveClusterTableTooltip(e);
        return;
    }
    const rect = iframe.getBoundingClientRect();
    const fakeEvent = { 
        clientX: e.clientX + rect.left, 
        clientY: e.clientY + rect.top,
        target: e.target,
        currentTarget: e.currentTarget
    };
    moveClusterTableTooltip(fakeEvent);
};

window.hideClusterTableTooltipFromIframe = function () {
    hideClusterTableTooltip();
};
