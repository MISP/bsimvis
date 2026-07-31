/**
 * File View Module
 * Extracted from file/index.html
 */

window.FileView = {
    container: null,
    params: null,
    functions: [],
    clusters: {},
    functionsLoaded: false,
    sortState: { col: 'function_name', dir: 1 },
    funcClusters: {},
    funcPage: { total: null, loading: false, reqId: 0 },
    FUNC_PAGE_SIZE: 100,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        this.functions = [];
        this.clusters = {};
        this.funcClusters = {};
        this.funcPage = { total: null, loading: false, reqId: 0 };
        this.functionsLoaded = false;
        this.sortState = { col: 'function_name', dir: 1 };

        const collection = params.collection || '';
        const file_md5 = params.md5 || params.file_md5;

        if (!file_md5) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No file MD5 provided.</div>';
            return;
        }

        // Build HTML structure
        this.container.innerHTML = `
            <style>
                .bsim-tabbar { display:flex; gap:4px; margin:0 0 16px 0; border-bottom:2px solid var(--border); }
                .bsim-tab {
                    background:none; border:none; border-bottom:3px solid transparent;
                    margin-bottom:-2px; padding:10px 20px; cursor:pointer;
                    color:var(--subtle); font-size:0.9rem; font-weight:600; letter-spacing:0.01em;
                    transition:color 0.15s, border-color 0.15s, background 0.15s;
                }
                .bsim-tab:hover { color:var(--text); background: var(--hover); }
                .bsim-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
                
                .file-func-table { width:100%; border-collapse:collapse; font-size:0.8rem; }
                .file-func-table th { text-align:left; padding:10px; border-bottom:1px solid var(--border); color:var(--subtle); text-transform:uppercase; font-size:0.75rem; letter-spacing:0.05em; }
                .file-func-table td { padding:10px; border-bottom: 1px solid var(--border); vertical-align:middle; }
                .file-func-table tr:hover { background: var(--hover); }
                
                .file-func-table th.sortable { cursor: pointer; user-select: none; }
                .file-func-table th.sortable:hover { color: var(--text); }
                .file-func-table tr.filter-row th { padding: 4px 10px; border-bottom: 1px solid var(--border); background: var(--border); }
                .file-func-table tr.filter-row input { background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 3px; font-size: 0.7rem; box-sizing: border-box; }

                .bin-sim-mc-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
                .bin-sim-mc-table th { text-align:left; padding:6px 12px; color:var(--subtle); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; border-bottom:1px solid var(--border); }
                .bin-sim-mc-table td { padding:6px 12px; border-bottom: 1px solid var(--border); vertical-align:top; font-family:'Consolas',monospace; word-break:break-word; }
                .bin-sim-mc-cat { padding:10px 12px 4px; font-weight:bold; color:var(--accent); font-size:0.78rem; }
                .bin-sim-mc-label { color:var(--subtle); font-family:'Inter',sans-serif; width:160px; }
                
                .bin-sim-strip { border:1px solid var(--border); border-radius:6px; padding:10px 12px; background:var(--card-bg); display:flex; align-items:center; gap:10px; min-height:24px; }
            </style>
            <div id="file-view-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Loading Binary Details...
            </div>
            <div id="file-view-content" style="display: none; flex:1; overflow-y:auto; padding: 0 0 20px 0;">
                <div id="file-title-strip" class="bin-sim-strip" style="margin-bottom: 20px; cursor: context-menu;"
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
                    <span id="file-title-text" style="font-weight:bold; color:var(--accent); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:30%;">unknown</span>
                    <span id="file-md5-text" style="font-family: 'JetBrains Mono', 'Consolas', monospace; color: var(--dim); font-size: 0.8rem; margin-right: 10px;">(MD5: ---)</span>
                    <span id="file-tags-container" style="display: inline-flex; gap: 4px; flex-wrap: wrap; align-items: center; min-width: 0; flex: 1;"></span>
                    <span id="file-note-btn-container" style="margin-left:auto; display: inline-flex; align-items: center;"></span>
                </div>

                <div class="bsim-tabbar" id="file-view-tabs">
                    <button class="bsim-tab active" id="file-tab-btn-metadata" onclick="FileView.switchTab('metadata')">Metadata (<span id="metadata-count">0</span>)</button>
                    <button class="bsim-tab" id="file-tab-btn-functions" onclick="FileView.switchTab('functions')">Functions (<span id="functions-count">0</span>)</button>
                    <button class="bsim-tab" id="file-tab-btn-clusters" onclick="FileView.switchTab('clusters')">Clusters (<span id="cluster-count">0</span>)</button>
                </div>

                <!-- Metadata Tab Panel (Default Active) -->
                <div id="file-panel-metadata" class="file-view-panel" style="display: block;">
                    <div style="display: flex; flex-direction: column; gap: 20px;">
                        <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px var(--border);">
                            <div id="file-meta-container">
                                <!-- Reused comparison table layout here -->
                            </div>
                        </div>

                        <div class="card" id="inferred-meta-card" style="display: none; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px var(--border);">
                            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid var(--border); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                                <i class="fa-solid fa-wand-magic-sparkles"></i> Inferred Metadata
                            </div>
                            <div class="meta-grid" id="inferred-meta" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;"></div>
                        </div>
                    </div>
                </div>

                <!-- Functions Tab Panel -->
                <div id="file-panel-functions" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px var(--border); display: flex; flex-direction: column; gap: 15px;">
                        <div id="file-func-scroll" style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
                            <table class="file-func-table" id="file-func-table">
                                <thead>
                                    <tr>
                                        <th class="sortable" onclick="FileView.toggleSort('function_name')">Function <span id="sort-icon-function_name">↕</span></th>
                                        <th class="sortable" onclick="FileView.toggleSort('entrypoint_address')">Entrypoint <span id="sort-icon-entrypoint_address">↕</span></th>
                                        <th>Tags</th>
                                        <th>Clusters</th>
                                        <th class="sortable" onclick="FileView.toggleSort('bsim_features_count')">Features <span id="sort-icon-bsim_features_count">↕</span></th>
                                        <th>Notes</th>
                                    </tr>
                                    <tr class="filter-row">
                                        <th>
                                            <div style="display:flex; flex-direction:column; gap:4px;">
                                                <input type="text" id="flt-func-name" placeholder="Name..." style="width:100%;" onfocus="FileView.attachFieldAutocomplete(this, 'function_name')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <div style="display:flex; gap:2px;">
                                                    <input type="text" id="flt-func-namespace" placeholder="Namespace..." style="width:50%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'namespace')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                    <input type="text" id="flt-func-ret_type" placeholder="Return type..." style="width:50%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'return_type')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                </div>
                                            </div>
                                        </th>
                                        <th><input type="text" id="flt-func-address" placeholder="Addr..." style="width:100%;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th><input type="text" id="flt-func-tag" placeholder="Tag..." style="width:100%;" onfocus="FileView.attachTagFilterAutocomplete(this)" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th>
                                            <div style="display:flex; flex-direction:column; gap:2px;">
                                                <input type="text" id="flt-func-cluster" placeholder="UUID..." style="width:100%; font-size:0.6rem;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <input type="text" id="flt-func-cluster-name" placeholder="Cluster name..." style="width:100%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'cluster_name')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                                <input type="number" id="flt-func-min-cohesion" placeholder="Min cohesion..." value="0.95" step="0.05" min="0" max="1" title="Min Cluster Cohesion" style="width:100%; font-size:0.6rem;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" />
                                            </div>
                                        </th>
                                        <th><input type="number" id="flt-func-min-features" placeholder="Min" min="0" title="Min Features" style="width:100%;" oninput="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                        <th><input type="text" id="flt-func-note-owner" placeholder="Note owner..." style="width:100%; font-size:0.6rem;" onfocus="FileView.attachFieldAutocomplete(this, 'note_owners')" onchange="FileView.handleFilterChange()" onkeydown="FileView.handleFilterKey(event)" /></th>
                                    </tr>
                                </thead>
                                <tbody id="file-functions-tbody">
                                    <tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading functions...</td></tr>
                                </tbody>
                            </table>
                        </div>
                        <div id="file-func-status" class="dim" style="font-size:0.7rem; text-align:center;"></div>
                    </div>
                </div>

                <!-- Clusters Tab Panel -->
                <div id="file-panel-clusters" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px var(--border);">
                        <div class="cluster-list" id="cluster-list" style="display: flex; flex-direction: column; gap: 10px;"></div>
                    </div>
                </div>
            </div>
        `;

        try {
            if (window.fetchTagMetadata) await window.fetchTagMetadata(collection);

            const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
            const res = await fetch(`/api/file/details/${file_md5}?${apiParams}`);
            if (!res.ok) throw new Error("File not found");
            const data = await res.json();

            if (data.error) throw new Error(data.error);

            document.getElementById('file-view-loader').style.display = 'none';
            document.getElementById('file-view-content').style.display = 'block';

            const file = data.file;
            this.clusters = data.bin_cluster_map || {};
            const inferredMeta = data.inferred_meta || {};

            // Render Hero Header
            const fileName = file.file_name || file.file_names?.[0] || 'Unknown Binary';
            window.filenameCache = window.filenameCache || {};
            window.filenameCache[file.file_md5] = fileName;
            document.getElementById('file-title-text').innerText = fileName;
            document.getElementById('file-md5-text').innerText = `(MD5: ${file.file_md5})`;

            // Pre-populate functions count
            document.getElementById('functions-count').innerText = file.function_count || 0;

            if (typeof Breadcrumbs !== 'undefined') {
                Breadcrumbs.setFilename(file.file_md5, fileName);
                Breadcrumbs.refresh();
            }

            // Bind data-entity-data on the strip for context menu functionality
            const fileId = `${collection}:file:${file_md5}`;
            const strip = document.getElementById('file-title-strip');
            if (strip) {
                const entityData = {
                    id: file.file_id || fileId,
                    name: fileName,
                    md5: file.file_md5,
                    note_owners: file.note_owners || [],
                    user_tags: file.user_tags || [],
                    tags: file.tags || []
                };
                strip.setAttribute('data-entity-data', JSON.stringify(entityData).replace(/'/g, "&apos;"));
            }

            // Render Tags and Notes in Header (Inspired by File Strip in Sim view)
            if (window.renderTagEditor) {
                document.getElementById('file-tags-container').innerHTML = window.renderTagEditor(
                    'file', file.file_id || fileId, file.tags || [], file.user_tags || []
                );
            }
            if (window.EntityRenderer) {
                document.getElementById('file-note-btn-container').innerHTML = window.EntityRenderer.renderFileNoteButton(
                    file.file_id || fileId, file.note_owners || [], { raw_data: file }
                );
            }

            // Render Metadata Table (Reusing comparison table layout and styles)
            const fmt = (v) => {
                if (v === undefined || v === null || v === '') return '<span style="color:var(--subtle); opacity:0.5;">—</span>';
                if (Array.isArray(v)) return v.length ? v.join(', ') : '<span style="color:var(--subtle); opacity:0.5;">—</span>';
                return String(v);
            };
            const fmtDate = (timestamp) => {
                if (!timestamp) return '';
                const d = new Date(Number(timestamp) * 1000);
                return d.toLocaleString();
            };

            const iconMap = {
                'File Name': 'fa-solid fa-file',
                'Other Names': 'fa-solid fa-tags',
                'MD5': 'fa-solid fa-fingerprint',
                'Batch UUID': 'fa-solid fa-box',
                'Language': 'fa-solid fa-microchip',
                'AV Type': 'fa-solid fa-shield',
                'File Type': 'fa-solid fa-file-code',
                'Yara': 'fa-solid fa-biohazard',
                'CC IP': 'fa-solid fa-network-wired',
                'Functions': 'fa-solid fa-list-ol',
                'BSim Features': 'fa-solid fa-dna',
                'First Seen': 'fa-solid fa-clock',
                'Related MD5s': 'fa-solid fa-link'
            };

            const categories = [
                ['Identity', [
                    ['File Name', file.file_name],
                    ['Other Names', file.file_names],
                    ['MD5', file.file_md5],
                    ['Related MD5s', file.related_md5],
                    ['Batch UUID', file.batch_uuid],
                    ['First Seen', file.first_seen ? fmtDate(file.first_seen) : ''],
                ]],
                ['Classification', [
                    ['Language', file.language_id || file.language],
                    ['AV Type', file.avtype],
                    ['File Type', file.filetype],
                    ['Yara', file.yara],
                    ['CC IP', file.cc_ip],
                ]],
                ['Statistics', [
                    ['Functions', file.function_count],
                    ['BSim Features', file.bsim_features_count],
                ]]
            ];

            if (file.file_format && Object.keys(file.file_format).length > 0) {
                const formatFields = Object.entries(file.file_format).map(([k, v]) => [k, v]);
                categories.push(['File Format', formatFields]);
            }

            let rows = '';
            let metaCount = 0;
            for (const [cat, fields] of categories) {
                rows += `<tr><td class="bin-sim-mc-cat" colspan="2">${cat}</td></tr>`;
                for (const [label, val] of fields) {
                    const icon = iconMap[label] || 'fa-solid fa-circle-info';
                    rows += `<tr>
                        <td class="bin-sim-mc-label" style="display: flex; align-items: center; gap: 8px;"><i class="${icon}" style="width: 14px; text-align: center; color: var(--dim); opacity: 0.8;"></i>${label}</td>
                        <td>${fmt(val)}</td>
                    </tr>`;
                    if (val !== undefined && val !== null && val !== '') {
                        metaCount++;
                    }
                }
            }
            document.getElementById('metadata-count').innerText = metaCount;

            document.getElementById('file-meta-container').innerHTML = `
                <table class="bin-sim-mc-table">
                    <thead><tr><th>Field</th><th>Value</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            `;

            // Render Distributions for Clusters
            const self = this;
            function renderDist(title, icon, dist) {
                if (!dist || dist.length === 0) return '';
                
                const colors = ['#66d9ef', '#a6e22e', '#f92672', '#fd971f', '#ae81ff', '#e6db74', '#75715e'];
                
                let legendHtml = '';
                let totalPercent = 0;
                dist.forEach(d => totalPercent += (d.percent || 0));
                
                let pieData = dist.map((d, i) => {
                    const color = colors[i % colors.length];
                    legendHtml += `
                        <div style="display: flex; align-items: center; gap: 6px; font-size: 0.75rem; margin-bottom: 4px;">
                            <div style="width: 10px; height: 10px; background-color: ${color}; border-radius: 2px;"></div>
                            <span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 150px;" title="${d.value}">${d.value}</span>
                            <span style="color: var(--dim); margin-left: auto;">${d.percent || 0}%</span>
                        </div>
                    `;
                    return {...d, color: color, value: d.percent || 0};
                });
                
                if (totalPercent < 100) {
                    pieData.push({value: 100 - totalPercent, color: 'var(--border)', isDummy: true});
                }
                
                const width = 50;
                const height = 50;
                const radius = Math.min(width, height) / 2;
                
                const pie = d3.pie().value(d => d.value).sort(null);
                const arc = d3.arc().innerRadius(0).outerRadius(radius);
                
                const svg = d3.create("svg")
                    .attr("width", width)
                    .attr("height", height)
                    .attr("viewBox", `0 0 ${width} ${height}`)
                    .style("box-shadow", "0 2px 10px var(--border)")
                    .style("border-radius", "50%");
                    
                svg.append("g")
                    .attr("transform", `translate(${width/2},${height/2})`)
                    .selectAll("path")
                    .data(pie(pieData))
                    .join("path")
                    .attr("fill", d => d.data.color)
                    .attr("d", arc)
                    .append("title")
                    .text(d => d.data.isDummy ? "" : `${d.data.value}: ${d.value}%`);
                    
                const svgHtml = svg.node().outerHTML;
                
                return `
                    <div style="margin-top: 15px; padding: 10px; background: var(--border); border: 1px solid var(--border); border-radius: 6px;">
                        <div style="font-size: 0.75rem; color: var(--dim); margin-bottom: 10px; display: flex; align-items: center; gap: 6px;">
                            <i class="${icon}"></i> ${title}
                        </div>
                        <div style="display: flex; gap: 15px; align-items: center;">
                            <div style="flex-shrink: 0;">${svgHtml}</div>
                            <div style="display: flex; flex-direction: column; flex: 1; min-width: 0;">
                                  ${legendHtml}
                            </div>
                        </div>
                    </div>
                `;
            }

            // Render Clusters
            const clusterIds = file.bin_clusters || [];
            document.getElementById('cluster-count').innerText = clusterIds.length;
            let clustersHtml = '';
            
            if (clusterIds.length === 0) {
                clustersHtml = '<div class="dim" style="text-align:center; padding: 20px;">Binary does not belong to any clusters.</div>';
            } else {
                clusterIds.sort((a, b) => {
                    const cmA = this.clusters[a] || {};
                    const cmB = this.clusters[b] || {};
                    return (cmB.cohesion_score || 0) - (cmA.cohesion_score || 0);
                });

                clusterIds.forEach(cid => {
                    const cm = this.clusters[cid];
                    if (!cm) return;
                    
                    const name = cm.cluster_name || `Cluster ${cid}`;
                    const size = cm.size || cm.member_count || cm.members || cm.count || 0;
                    const cohesionScore = cm.cohesion_score || 0;
                    const cohesion = cohesionScore.toFixed(2);
                    const cohesionColor = d3.interpolateRdYlGn(cohesionScore);
                    
                    let distBadges = '';
                    distBadges += renderDist('Yara Distributions', 'fa-solid fa-biohazard', cm.yara_distribution);
                    distBadges += renderDist('AV Type Distributions', 'fa-solid fa-shield', cm.avtype_distribution);
                    distBadges += renderDist('File Type Distributions', 'fa-solid fa-file-code', cm.filetype_distribution);
                    distBadges += renderDist('CC IP Distributions', 'fa-solid fa-network-wired', cm.ccip_distribution);
                    distBadges += renderDist('File Name Distributions', 'fa-solid fa-file', cm.filename_distribution);
                    distBadges += renderDist('MD5 Distributions', 'fa-solid fa-fingerprint', cm.md5_distribution);

                    clustersHtml += `
                        <div class="cluster-item" style="background: var(--border); border: 1px solid var(--border); border-radius:6px; padding:12px; display:flex; flex-direction:column; gap:8px;">
                            <div class="cluster-item-header" style="margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center; font-weight:bold; font-size:0.95rem; color:var(--text);">
                                <span style="color: var(--accent);"><i class="fa-solid fa-bullseye" style="margin-right: 6px;"></i>${name}</span>
                                <a href="#" style="font-size:0.75rem; color:var(--dim); text-decoration:none;" onclick="FileView.openClusterFiles(event, '${cm.cluster_uuid}')">View Binaries <i class="fa-solid fa-arrow-right"></i></a>
                            </div>
                            <div class="cluster-stat-badges" style="margin-bottom: 5px; display:flex; gap:10px; flex-wrap:wrap;">
                                <div class="stat-badge" style="background: var(--hover); border: 1px solid var(--border); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-users" style="color:var(--dim);"></i><span>Members: <span class="val" style="color:var(--accent); font-family: 'JetBrains Mono', 'Consolas', monospace;">${size}</span></span></div>
                                <div class="stat-badge" style="background: var(--hover); border: 1px solid var(--border); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-bullseye" style="color:var(--dim);"></i><span>Cohesion: <span class="val" style="color: ${cohesionColor}; font-family: 'JetBrains Mono', 'Consolas', monospace;">${cohesion}</span></span></div>
                            </div>
                            ${distBadges}
                        </div>
                    `;
                });
            }
            document.getElementById('cluster-list').innerHTML = clustersHtml;

            // Render Inferred Rows
            const renderInferredRow = (icon, label, mapObj) => {
                const keys = Object.keys(mapObj).sort((a,b) => mapObj[b].percent - mapObj[a].percent);
                if (keys.length === 0) return '';
                const badges = keys.map(k => {
                    const confObj = mapObj[k];
                    const confScore = confObj.percent;
                    const confColor = d3.interpolateRdYlGn(confScore / 100);
                    const clusterLink = Nav.buildUIUrl(collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(confObj.cluster_uuid)}`;
                    return `<a href="${clusterLink}" class="stat-badge" style="background: var(--hover); display: inline-flex; margin: 2px 4px 2px 0; text-decoration: none; transition: background 0.2s;" onclick="event.preventDefault(); Nav.openPath('${clusterLink}', event);"><span style="color: var(--meta-text-muted); font-family: 'JetBrains Mono', 'Consolas', monospace;">${k}</span> <span class="val" style="margin-left: 4px; color: ${confColor};">${confScore}%</span></a>`;
                }).join('');
                return `
                    <div class="meta-label" style="align-items: flex-start; margin-top: 4px; color: var(--dim); text-transform: uppercase; font-size: 0.75rem; display: flex; gap: 6px;"><i class="${icon}" style="width:14px; text-align:center;"></i> ${label}</div>
                    <div class="meta-value" style="display: flex; flex-wrap: wrap;">${badges}</div>
                `;
            };

            let inferredHtml = '';
            inferredHtml += renderInferredRow('fa-solid fa-file', 'File Name', inferredMeta.filename || {});
            inferredHtml += renderInferredRow('fa-solid fa-fingerprint', 'MD5', inferredMeta.md5 || {});
            inferredHtml += renderInferredRow('fa-solid fa-shield', 'AV Type', inferredMeta.avtype || {});
            inferredHtml += renderInferredRow('fa-solid fa-file-code', 'File Type', inferredMeta.filetype || {});
            inferredHtml += renderInferredRow('fa-solid fa-biohazard', 'Yara', inferredMeta.yara || {});
            inferredHtml += renderInferredRow('fa-solid fa-network-wired', 'CC IP', inferredMeta.ccip || {});

            if (inferredHtml) {
                document.getElementById('inferred-meta').innerHTML = inferredHtml;
                document.getElementById('inferred-meta-card').style.display = 'block';
            }

            // Unique-value counts appended to the filter placeholders
            if (typeof loadFieldCardinalities === 'function') {
                loadFieldCardinalities(collection, 'func', {
                    'function_name': 'flt-func-name',
                    'namespace': 'flt-func-namespace',
                    'return_type': 'flt-func-ret_type',
                    'cluster_name': 'flt-func-cluster-name',
                    'note_owners': 'flt-func-note-owner'
                });
            }

            // Silently fetch functions so they're ready when switching tabs
            this.loadFunctionsTable();

            // Apply tab from URL hash
            this.applyTabFromHash();

            // Register hashchange listener
            if (!this._hashBound) {
                this._onHashChange = () => this.applyTabFromHash();
                window.addEventListener('hashchange', this._onHashChange);
                this._hashBound = true;
            }

            // Initialize Notes panel silently
            if (typeof window.showFileNotes === 'function') {
                window.showFileNotes(fileId, false);
            }

        } catch (err) {
            console.error(err);
            document.getElementById('file-view-loader').innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
        }
    },

    switchTab(tabId, push = true) {
        document.querySelectorAll('#file-view-tabs .bsim-tab').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.file-view-panel').forEach(panel => panel.style.display = 'none');

        const btn = document.getElementById(`file-tab-btn-${tabId}`);
        if (btn) btn.classList.add('active');

        const panel = document.getElementById(`file-panel-${tabId}`);
        if (panel) panel.style.display = 'block';

        if (tabId === 'functions') {
            this.loadFunctionsTable();
        }

        if (push && location.hash.slice(1) !== tabId) {
            history.pushState(null, '', location.pathname + location.search + '#' + tabId);
        }
    },

    applyTabFromHash() {
        const allowedTabs = ['metadata', 'functions', 'clusters'];
        const tab = location.hash.slice(1);
        this.switchTab(allowedTabs.includes(tab) ? tab : 'metadata', false);
    },

    // Filter inputs -> /api/function/search params. Same names the function
    // search view uses, so the server-side handling is shared.
    FUNC_FILTERS: {
        'flt-func-name': 'function_name',
        'flt-func-namespace': 'namespace',
        'flt-func-ret_type': 'return_type',
        'flt-func-address': 'entrypoint_address',
        'flt-func-tag': 'func_tag',
        'flt-func-cluster': 'cluster_uuid',
        'flt-func-cluster-name': 'cluster_name',
        'flt-func-min-cohesion': 'min_cohesion',
        'flt-func-min-features': 'min_features',
        'flt-func-note-owner': 'note_owner'
    },

    // attachAutocomplete rebinds focus/click/input on the element, so the inline
    // onfocus only ever runs once — same wiring as the function search view.
    attachFieldAutocomplete(input, field) {
        if (typeof attachAutocomplete !== 'function') return;
        attachAutocomplete(input, 'func', field, (val) => {
            input.value = val;
            this.applyFilters();
        });
    },

    attachTagFilterAutocomplete(input) {
        if (typeof attachTagAutocomplete !== 'function') return;
        attachTagAutocomplete(input, (val) => {
            input.value = val;
            this.applyFilters();
        });
    },

    applyFilters() {
        clearTimeout(this._filterTimer);
        this.loadFunctionsTable({ reset: true });
    },

    buildFunctionsQuery(offset) {
        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
        const p = new URLSearchParams(apiParams);
        p.set('file_md5', file_md5);
        p.set('offset', offset);
        p.set('limit', this.FUNC_PAGE_SIZE);
        p.set('sort_by', this.sortState.col);
        p.set('sort_order', this.sortState.dir === 1 ? 'asc' : 'desc');
        for (const [id, param] of Object.entries(this.FUNC_FILTERS)) {
            const v = (document.getElementById(id)?.value || '').trim();
            if (v) p.set(param, v);
        }
        return p.toString();
    },

    async loadFunctionsTable({ reset = false } = {}) {
        if (this.funcPage.loading && !reset) return;
        if (!reset && this.functionsLoaded) return;
        if (!reset && this.funcPage.total !== null && this.functions.length >= this.funcPage.total) return;

        const tbody = document.getElementById('file-functions-tbody');
        if (reset) {
            this.functions = [];
            this.funcPage.total = null;
        }
        this.funcPage.loading = true;
        this.setFunctionsStatus('<i class="fa-solid fa-spinner fa-spin"></i> Loading...');

        // Bump on every request so a slow earlier page can't overwrite a newer filter's result
        const reqId = ++this.funcPage.reqId;

        try {
            const res = await fetch(`/api/function/search?${this.buildFunctionsQuery(this.functions.length)}`);
            if (!res.ok) throw new Error("Functions load failed");
            const data = await res.json();
            if (reqId !== this.funcPage.reqId) return;
            if (data.error) throw new Error(data.error);

            this.functions = this.functions.concat(data.functions || []);
            this.funcPage.total = data.total || 0;
            this.funcClusters = Object.assign(this.funcClusters || {}, data.clusters || {});
            document.getElementById('functions-count').innerText = this.funcPage.total;
            this.renderFunctionsTable();
            this.functionsLoaded = true;
        } catch (e) {
            console.error(e);
            if (reqId !== this.funcPage.reqId) return;
            if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color:#f92672; padding: 20px;"><i class="fa-solid fa-circle-exclamation"></i> Error loading functions: ${e.message}</td></tr>`;
            this.setFunctionsStatus('');
        } finally {
            if (reqId === this.funcPage.reqId) this.funcPage.loading = false;
        }
    },

    setFunctionsStatus(html) {
        const el = document.getElementById('file-func-status');
        if (el) el.innerHTML = html;
    },

    // Loads the next page whenever the table is scrolled near the bottom.
    bindFunctionsScroll() {
        const scroller = document.getElementById('file-func-scroll');
        if (!scroller || scroller._funcScrollBound) return;
        scroller._funcScrollBound = true;
        scroller.addEventListener('scroll', () => {
            if (scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 200) {
                this.loadFunctionsTable();
            }
        });
    },

    toggleSort(col) {
        if (this.sortState.col === col) {
            this.sortState.dir = -this.sortState.dir;
        } else {
            this.sortState.col = col;
            this.sortState.dir = 1;
        }

        ['function_name', 'entrypoint_address', 'bsim_features_count'].forEach(c => {
            const el = document.getElementById(`sort-icon-${c}`);
            if (el) {
                el.innerText = this.sortState.col === c ? (this.sortState.dir === 1 ? '▲' : '▼') : '↕';
            }
        });

        this.loadFunctionsTable({ reset: true });
    },

    handleFilterChange() {
        clearTimeout(this._filterTimer);
        this._filterTimer = setTimeout(() => this.loadFunctionsTable({ reset: true }), 350);
    },

    handleFilterKey(e) {
        if (e.key === 'Enter') this.applyFilters();
    },

    renderFunctionsTable() {
        const tbody = document.getElementById('file-functions-tbody');
        if (!tbody) return;

        // Filtering, sorting and paging all happen server-side; render what we hold.
        if (this.functions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;">No functions found.</td></tr>';
            this.setFunctionsStatus('');
            return;
        }

        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;

        tbody.innerHTML = this.functions.map(f => {
            const entry = f.entrypoint_address || '';
            const funcName = f.function_name || 'unknown';
            const featCount = f.bsim_features_count || 0;
            const fColl = f.collection || collection;
            const funcId = f.function_id || `${fColl}:func:${file_md5}:${entry}`;
            // renderFunction/context menu read these off the object; the search API may omit them
            f.collection = fColl;
            f.file_md5 = f.file_md5 || file_md5;
            f.function_id = funcId;

            // Notes
            const noteBtn = window.EntityRenderer ? window.EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true, raw_data: f }) : '';
            
            // Tags
            const tagsHtml = window.EntityRenderer ? window.EntityRenderer.renderTag('function', funcId, f.tags || [], f.user_tags || []) : '';
            
            // Clusters
            const cls = (f.clusters || []).map(uuid => (this.funcClusters || {})[uuid] || this.clusters[uuid]).filter(Boolean);
            const clusterCardHtml = window.EntityRenderer ? window.EntityRenderer.renderClusterCard(cls) : '';

            // Clickable details URL
            let poolId = null;
            if (window.getRoutingState && window.getRoutingState().pool) {
                poolId = window.getRoutingState().pool;
            }
            let detailUrl = `/collections/${encodeURIComponent(fColl)}/files/${file_md5}/functions/${entry}`;
            if (poolId) {
                detailUrl = `/pools/${encodeURIComponent(poolId)}` + detailUrl;
            }

            return `
                <tr class="sim-row" style="font-size: 0.75rem;" data-id="${funcId}"
                    data-entity-data='${JSON.stringify(f).replace(/'/g, "&apos;")}'
                    oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                    <td class="sim-cell" style="min-width:300px;">
                        ${window.EntityRenderer ? window.EntityRenderer.renderFunction(f, { hideNote: true }) : funcName}
                    </td>
                    <td>
                        <a class="mono" href="${detailUrl}" onclick="event.preventDefault(); Nav.openPath('${detailUrl}', event);" style="color:var(--accent); text-decoration:none;">@ ${entry}</a>
                    </td>
                    <td>${tagsHtml}</td>
                    <td>${clusterCardHtml}</td>
                    <td>
                        <div style="display:inline-flex; align-items:center; gap:6px;">
                            <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                            <button class="btn-icon" onclick="showFeaturePanel('${funcId}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                        </div>
                    </td>
                    <td style="text-align:center;">${noteBtn}</td>
                </tr>
            `;
        }).join('');

        const shown = this.functions.length;
        const total = this.funcPage.total ?? shown;
        this.setFunctionsStatus(shown < total ? `Showing ${shown} of ${total} — scroll for more` : `${total} function${total === 1 ? '' : 's'}`);
        this.bindFunctionsScroll();

        // TableSelection takes an element id, not an element (constructor is idempotent per table)
        if (window.TableSelection) {
            new window.TableSelection('file-func-table');
        }
    },

    openFunctions(e) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['search', 'functions']) + `?file_md5=${encodeURIComponent(this.params.md5)}`;
        Nav.openPath(url, e, { title: 'Functions', type: 'functions' });
    },

    openCallGraph(e) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['call_graph', this.params.md5]);
        Nav.openPath(url, e, { title: `Call Graph: ${this.params.md5.substring(0, 8)}`, type: 'call_graph' });
    },

    openClusterFiles(e, clusterUuid) {
        e.preventDefault();
        const url = Nav.buildUIUrl(this.params.collection, ['search', 'files']) + `?bin_cluster_uuid=${encodeURIComponent(clusterUuid)}`;
        Nav.openPath(url, e, { title: 'Cluster Files', type: 'files' });
    },

    showNotes(e) {
        e.preventDefault();
        if (window.showFileNotes) window.showFileNotes(`${this.params.collection}:file:${this.params.md5}`, true);
    },

    destroy() {
        if (this._hashBound) {
            window.removeEventListener('hashchange', this._onHashChange);
            this._hashBound = false;
        }
        clearTimeout(this._filterTimer);
        this.funcPage.reqId++;   // orphan any request still in flight
        this.container = null;
        this.params = null;
        this.functions = [];
        this.clusters = {};
        this.funcClusters = {};
        this.funcPage = { total: null, loading: false, reqId: 0 };
        this.functionsLoaded = false;
    }
};
