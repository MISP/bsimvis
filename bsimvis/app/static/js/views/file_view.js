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
    filterState: { q: '', featMin: '', featMax: '' },

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        this.functions = [];
        this.clusters = {};
        this.functionsLoaded = false;
        this.sortState = { col: 'function_name', dir: 1 };
        this.filterState = { q: '', featMin: '', featMax: '' };
        
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
                .bsim-tab:hover { color:var(--text); background:rgba(255,255,255,0.04); }
                .bsim-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
                
                .file-func-table { width:100%; border-collapse:collapse; font-size:0.8rem; }
                .file-func-table th { text-align:left; padding:10px; border-bottom:1px solid var(--border); color:var(--subtle); text-transform:uppercase; font-size:0.75rem; letter-spacing:0.05em; }
                .file-func-table td { padding:10px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:middle; }
                .file-func-table tr:hover { background: rgba(255,255,255,0.02); }
                
                .file-func-table th.sortable { cursor: pointer; user-select: none; }
                .file-func-table th.sortable:hover { color: var(--text); }
                .file-func-table tr.filter-row th { padding: 4px 10px; border-bottom: 1px solid var(--border); background: rgba(0,0,0,0.1); }
                .file-func-table tr.filter-row input { background: #000; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 3px; font-size: 0.7rem; box-sizing: border-box; }

                .bin-sim-mc-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
                .bin-sim-mc-table th { text-align:left; padding:6px 12px; color:var(--subtle); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em; border-bottom:1px solid var(--border); }
                .bin-sim-mc-table td { padding:6px 12px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:top; font-family:'Consolas',monospace; word-break:break-word; }
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
                        <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
                            <div id="file-meta-container">
                                <!-- Reused comparison table layout here -->
                            </div>
                        </div>

                        <div class="card" id="inferred-meta-card" style="display: none; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
                            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                                <i class="fa-solid fa-wand-magic-sparkles"></i> Inferred Metadata
                            </div>
                            <div class="meta-grid" id="inferred-meta" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;"></div>
                        </div>
                    </div>
                </div>

                <!-- Functions Tab Panel -->
                <div id="file-panel-functions" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3); display: flex; flex-direction: column; gap: 15px;">
                        <div style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
                            <table class="file-func-table">
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
                                        <th><input type="text" id="flt-q" placeholder="Search name/tag/addr..." style="width:100%;" oninput="FileView.handleFilterChange()" /></th>
                                        <th></th>
                                        <th></th>
                                        <th></th>
                                        <th>
                                            <div style="display:flex; align-items:center; gap:2px;">
                                                <input type="number" id="flt-feat-min" placeholder="Min" style="width:45%;" oninput="FileView.handleFilterChange()" />
                                                <span class="dim" style="font-size:0.6rem">-</span>
                                                <input type="number" id="flt-feat-max" placeholder="Max" style="width:45%;" oninput="FileView.handleFilterChange()" />
                                            </div>
                                        </th>
                                        <th></th>
                                    </tr>
                                </thead>
                                <tbody id="file-functions-tbody">
                                    <tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading functions...</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Clusters Tab Panel -->
                <div id="file-panel-clusters" class="file-view-panel" style="display: none;">
                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
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
                'First Seen': 'fa-solid fa-clock'
            };

            const categories = [
                ['Identity', [
                    ['File Name', file.file_name],
                    ['Other Names', file.file_names],
                    ['MD5', file.file_md5],
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
                            <span style="color: #ccc; font-family: 'JetBrains Mono', 'Consolas', monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 150px;" title="${d.value}">${d.value}</span>
                            <span style="color: var(--dim); margin-left: auto;">${d.percent || 0}%</span>
                        </div>
                    `;
                    return {...d, color: color, value: d.percent || 0};
                });
                
                if (totalPercent < 100) {
                    pieData.push({value: 100 - totalPercent, color: 'rgba(255,255,255,0.05)', isDummy: true});
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
                    .style("box-shadow", "0 2px 10px rgba(0,0,0,0.5)")
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
                    <div style="margin-top: 15px; padding: 10px; background: rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.05); border-radius: 6px;">
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
                        <div class="cluster-item" style="background: rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.05); border-radius:6px; padding:12px; display:flex; flex-direction:column; gap:8px;">
                            <div class="cluster-item-header" style="margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center; font-weight:bold; font-size:0.95rem; color:#fff;">
                                <span style="color: var(--accent);"><i class="fa-solid fa-bullseye" style="margin-right: 6px;"></i>${name}</span>
                                <a href="#" style="font-size:0.75rem; color:var(--dim); text-decoration:none;" onclick="FileView.openClusterFiles(event, '${cm.cluster_uuid}')">View Binaries <i class="fa-solid fa-arrow-right"></i></a>
                            </div>
                            <div class="cluster-stat-badges" style="margin-bottom: 5px; display:flex; gap:10px; flex-wrap:wrap;">
                                <div class="stat-badge" style="background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-users" style="color:var(--dim);"></i><span>Members: <span class="val" style="color:var(--accent); font-family: 'JetBrains Mono', 'Consolas', monospace;">${size}</span></span></div>
                                <div class="stat-badge" style="background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1); padding:4px 8px; border-radius:4px; font-size:0.75rem; display:flex; align-items:center; gap:6px;"><i class="fa-solid fa-bullseye" style="color:var(--dim);"></i><span>Cohesion: <span class="val" style="color: ${cohesionColor}; font-family: 'JetBrains Mono', 'Consolas', monospace;">${cohesion}</span></span></div>
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
                    return `<a href="${clusterLink}" class="stat-badge" style="background: rgba(255,255,255,0.02); display: inline-flex; margin: 2px 4px 2px 0; text-decoration: none; transition: background 0.2s;" onclick="event.preventDefault(); Nav.openPath('${clusterLink}', event);"><span style="color: #ccc; font-family: 'JetBrains Mono', 'Consolas', monospace;">${k}</span> <span class="val" style="margin-left: 4px; color: ${confColor};">${confScore}%</span></a>`;
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

    async loadFunctionsTable() {
        if (this.functionsLoaded) return;
        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;
        const tbody = document.getElementById('file-functions-tbody');
        
        try {
            const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
            const res = await fetch(`/api/function/search?file_md5=${file_md5}&limit=1000&${apiParams}`);
            if (!res.ok) throw new Error("Functions load failed");
            const data = await res.json();
            
            this.functions = data.functions || [];
            document.getElementById('functions-count').innerText = this.functions.length;
            this.renderFunctionsTable();
            this.functionsLoaded = true;
        } catch (e) {
            console.error(e);
            if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color:#f92672; padding: 20px;"><i class="fa-solid fa-circle-exclamation"></i> Error loading functions: ${e.message}</td></tr>`;
        }
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
        
        this.renderFunctionsTable();
    },

    handleFilterChange() {
        this.filterState.q = document.getElementById('flt-q').value;
        this.filterState.featMin = document.getElementById('flt-feat-min').value;
        this.filterState.featMax = document.getElementById('flt-feat-max').value;
        this.renderFunctionsTable();
    },

    renderFunctionsTable() {
        const tbody = document.getElementById('file-functions-tbody');
        if (!tbody) return;

        // Apply filters
        let filtered = this.functions.slice();
        
        const q = this.filterState.q.toLowerCase().trim();
        if (q) {
            filtered = filtered.filter(f => {
                const name = (f.function_name || '').toLowerCase();
                const addr = (f.entrypoint_address || '').toLowerCase();
                const tags = (f.tags || []).join(' ').toLowerCase() + ' ' + (f.user_tags || []).join(' ').toLowerCase();
                return name.includes(q) || addr.includes(q) || tags.includes(q);
            });
        }
        
        const minFeat = parseInt(this.filterState.featMin);
        const maxFeat = parseInt(this.filterState.featMax);
        if (!isNaN(minFeat)) {
            filtered = filtered.filter(f => (f.bsim_features_count || 0) >= minFeat);
        }
        if (!isNaN(maxFeat)) {
            filtered = filtered.filter(f => (f.bsim_features_count || 0) <= maxFeat);
        }

        // Apply sort
        const col = this.sortState.col;
        const dir = this.sortState.dir;
        filtered.sort((a, b) => {
            let valA = a[col];
            let valB = b[col];
            
            if (col === 'bsim_features_count') {
                valA = Number(valA || 0);
                valB = Number(valB || 0);
            } else {
                valA = String(valA || '').toLowerCase();
                valB = String(valB || '').toLowerCase();
            }
            
            if (valA < valB) return -dir;
            if (valA > valB) return dir;
            return 0;
        });

        if (filtered.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--dim); padding: 20px;">No functions found.</td></tr>';
            return;
        }

        const collection = this.params.collection || '';
        const file_md5 = this.params.md5 || this.params.file_md5;

        tbody.innerHTML = filtered.map(f => {
            const entry = f.entrypoint_address || '';
            const funcName = f.function_name || 'unknown';
            const featCount = f.bsim_features_count || 0;
            const fColl = f.collection || collection;
            const funcId = f.function_id || `${fColl}:func:${file_md5}:${entry}`;
            
            // Notes
            const noteBtn = window.EntityRenderer ? window.EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true, raw_data: f }) : '';
            
            // Tags
            const tagsHtml = window.EntityRenderer ? window.EntityRenderer.renderTag('function', funcId, f.tags || [], f.user_tags || []) : '';
            
            // Clusters
            const cls = (f.clusters || []).map(uuid => this.clusters[uuid]).filter(Boolean);
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
                    <td>
                        <a href="${detailUrl}" onclick="event.preventDefault(); Nav.openPath('${detailUrl}', event);" style="color:var(--accent); font-weight:bold; text-decoration:none;">
                            ${funcName}
                        </a>
                    </td>
                    <td class="mono" style="color:var(--accent);">@ ${entry}</td>
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

        if (window.TableSelection) {
            new window.TableSelection(tbody.closest('table'));
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
        this.container = null;
        this.params = null;
        this.functions = [];
        this.clusters = {};
        this.functionsLoaded = false;
    }
};
