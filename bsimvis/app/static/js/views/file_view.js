/**
 * File View Module
 * Extracted from file/index.html
 */

window.FileView = {
    container: null,
    params: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        const collection = params.collection || '';
        const file_md5 = params.md5 || params.file_md5;

        if (!file_md5) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No file MD5 provided.</div>';
            return;
        }

        // Build HTML structure
        this.container.innerHTML = `
            <div id="file-view-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Loading Binary Details...
            </div>
            <div id="file-view-content" style="display: none; flex:1; overflow-y:auto; padding: 0 0 20px 0;">
                <div class="hero-section" style="display: flex; align-items: center; gap: 15px; margin-bottom: 20px; padding-bottom: 20px; border-bottom: 1px solid rgba(255, 255, 255, 0.05);">
                    <div class="hero-details">
                        <h1 id="file-name" style="margin: 0 0 5px 0; font-size: 1.5rem; color: #fff; word-break: break-all;">unknown</h1>
                        <div class="md5" id="file-md5" style="font-family: 'JetBrains Mono', 'Consolas', monospace; color: var(--dim); font-size: 0.9rem;">MD5: ---</div>
                        <div class="quick-actions" id="file-quick-actions" style="display: flex; gap: 10px; margin-top: 15px;"></div>
                    </div>
                </div>

                <div class="dashboard-grid" style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                    <div style="display: flex; flex-direction: column; gap: 20px;">
                        <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
                            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                                <i class="fa-solid fa-info-circle"></i> File Metadata
                            </div>
                            <div class="meta-grid" id="file-meta" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;"></div>
                            <div style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 15px;">
                                <div style="font-size: 0.75rem; color: var(--dim); text-transform: uppercase; margin-bottom: 8px;">Tags</div>
                                <div id="file-tags-container"></div>
                            </div>
                        </div>

                        <div class="card" id="inferred-meta-card" style="display: none; background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
                            <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                                <i class="fa-solid fa-wand-magic-sparkles"></i> Inferred Metadata
                            </div>
                            <div class="meta-grid" id="inferred-meta" style="display: grid; grid-template-columns: auto 1fr; gap: 10px 15px; font-size: 0.85rem;"></div>
                        </div>
                    </div>

                    <div class="card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);">
                        <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 15px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                            <i class="fa-solid fa-bullseye"></i> Binary Clusters (<span id="cluster-count">0</span>)
                        </div>
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
            const clusters = data.bin_cluster_map || {};
            const inferredMeta = data.inferred_meta || {};

            // Render Hero
            const fileName = file.file_name || file.file_names?.[0] || 'Unknown Binary';
            window.filenameCache = window.filenameCache || {};
            window.filenameCache[file.file_md5] = fileName;
            document.getElementById('file-name').innerText = fileName;
            document.getElementById('file-md5').innerText = `MD5: ${file.file_md5}`;

            // Update breadcrumb to show actual filename in the global breadcrumbs container
            const bcCurrent = document.querySelector('#breadcrumbs-container .breadcrumb-item.current');
            if (bcCurrent) bcCurrent.innerHTML = `<i class="fa-solid fa-file-code"></i><span>${fileName}</span>`;
            
            const bc = window.Breadcrumbs || (window.parent && window.parent.Breadcrumbs);
            if (bc && typeof bc.refresh === 'function') {
                bc.refresh();
            }

            document.getElementById('file-quick-actions').innerHTML = `
                <a href="#" class="quick-action-btn" onclick="FileView.openFunctions(event)"><i class="fa-solid fa-code"></i> View Functions</a>
                <a href="#" class="quick-action-btn" onclick="FileView.openCallGraph(event)"><i class="fa-solid fa-sitemap"></i> Call Graph</a>
                <a href="#" class="quick-action-btn" onclick="FileView.showNotes(event)"><i class="fa-solid fa-note-sticky"></i> Notes</a>
            `;

            // Render Meta Row
            const renderRow = (icon, label, value, color) => {
                if (!value) return '';
                const valStr = Array.isArray(value) ? value.join(', ') : String(value);
                return `
                    <div class="meta-label" style="color: var(--dim); text-transform: uppercase; font-size: 0.75rem; display: flex; align-items: center; gap: 6px;">
                        <i class="${icon}" style="width:14px; text-align:center;"></i> ${label}
                    </div>
                    <div class="meta-value" style="color: ${color || '#eee'}; font-family: 'JetBrains Mono', 'Consolas', monospace; word-break: break-all;">${valStr}</div>
                `;
            };

            let metaHtml = '';
            metaHtml += renderRow('fa-solid fa-microchip', 'Architecture', file.language_id || file.language, '#ae81ff');
            metaHtml += renderRow('fa-solid fa-list-ol', 'Functions', file.function_count, '#a6e22e');
            metaHtml += renderRow('fa-solid fa-shield', 'AV Type', file.avtype);
            metaHtml += renderRow('fa-solid fa-file-code', 'File Type', file.filetype);
            metaHtml += renderRow('fa-solid fa-biohazard', 'Yara', file.yara, 'var(--accent)');
            metaHtml += renderRow('fa-solid fa-network-wired', 'CC IP', file.cc_ip, 'var(--info, #60a5fa)');
            metaHtml += renderRow('fa-solid fa-box', 'Batch UUID', file.batch_uuid, 'var(--dim)');
            if (file.first_seen) {
                metaHtml += renderRow('fa-solid fa-clock', 'First Seen', new Date(file.first_seen * 1000).toLocaleString(), '#ccc');
            }
            
            document.getElementById('file-meta').innerHTML = metaHtml || '<div class="dim">No metadata found.</div>';

            // Render Tags
            if (window.renderTagEditor) {
                document.getElementById('file-tags-container').innerHTML = window.renderTagEditor(
                    'file', file.file_id || `${collection}:file:${file_md5}`, file.tags || [], file.user_tags || []
                );
            }

            // Render Distributions for Clusters
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
                    const cmA = clusters[a] || {};
                    const cmB = clusters[b] || {};
                    return (cmB.cohesion_score || 0) - (cmA.cohesion_score || 0);
                });

                clusterIds.forEach(cid => {
                    const cm = clusters[cid];
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
                        <div class="cluster-item">
                            <div class="cluster-item-header" style="margin-bottom: 8px;">
                                <span style="color: var(--accent);"><i class="fa-solid fa-bullseye" style="margin-right: 6px;"></i>${name}</span>
                                <a href="#" style="font-size:0.75rem; color:var(--dim); text-decoration:none;" onclick="FileView.openClusterFiles(event, '${cm.cluster_uuid}')">View Binaries <i class="fa-solid fa-arrow-right"></i></a>
                            </div>
                            <div class="cluster-stat-badges" style="margin-bottom: 5px;">
                                <div class="stat-badge"><i class="fa-solid fa-users"></i><span>Members: <span class="val">${size}</span></span></div>
                                <div class="stat-badge"><i class="fa-solid fa-bullseye"></i><span>Cohesion: <span class="val" style="color: ${cohesionColor};">${cohesion}</span></span></div>
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
                    const clusterLink = `/collections/${encodeURIComponent(collection)}/files?bin_cluster_uuid=${encodeURIComponent(confObj.cluster_uuid)}`;
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

            // Initialize Notes and AI insights handles silently (hidden but constructed)
            const fileId = `${collection}:file:${file_md5}`;
            if (typeof window.showFileNotes === 'function') {
                window.showFileNotes(fileId, false);
            }

        } catch (err) {
            console.error(err);
            document.getElementById('file-view-loader').innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
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
        this.container = null;
        this.params = null;
    }
};
