/**
 * Call Graph View Module
 * Extracted from call_graph/index.html
 */

window.CallGraphView = {
    container: null,
    params: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        const collection = params.collection || 'main';
        const file_md5 = params.md5 || params.file_md5;

        if (!file_md5) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No file MD5 provided for call graph.</div>';
            return;
        }

        this.container.innerHTML = `
            <div id="cg-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Initializing Call Graph...
            </div>
            <div id="cg-container" style="display:none; flex:1; overflow:hidden; position:relative; display:flex; flex-direction:row; width:100%; height:100%;">
                <div id="cg-canvas-container" style="flex:1; height:100%; position:relative;">
                    <div id="cg-controls" style="position:absolute; top:20px; left:20px; display:flex; gap:10px; z-index:10;">
                        <button class="btn-action" onclick="CallGraphView.resetZoom()"><i class="fa-solid fa-compress"></i> Reset Zoom</button>
                        <button class="btn-action" onclick="CallGraphView.toggleLabels()"><i class="fa-solid fa-tag"></i> Toggle Labels</button>
                    </div>
                </div>
                <div id="cg-meta-sidebar" class="card" style="width:320px; border-left:1px solid var(--border); background:var(--card-bg); display:flex; flex-direction:column; padding:20px; overflow-y:auto; gap:15px; box-shadow:-4px 0 15px rgba(0,0,0,0.3); z-index:5;">
                    <div class="card-title" style="font-size: 1rem; font-weight: bold; margin-bottom: 5px; color: var(--accent); display: flex; align-items: center; gap: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); padding-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">
                        <i class="fa-solid fa-info-circle"></i> File Metadata
                    </div>
                    <div id="cg-meta-content" style="display:flex; flex-direction:column; gap:12px;">
                        <div class="dim"><i class="fa-solid fa-spinner fa-spin"></i> Loading metadata...</div>
                    </div>
                </div>
            </div>
        `;

        try {
            const res = await fetch(`/api/file/call_graph?collection=${encodeURIComponent(collection)}&file_md5=${encodeURIComponent(file_md5)}`);
            if (!res.ok) throw new Error("Call graph data not found");
            const data = await res.json();

            document.getElementById('cg-loader').style.display = 'none';
            document.getElementById('cg-container').style.display = 'flex';

            if (window.initCallGraph) {
                window.initCallGraph(data, 'cg-canvas-container');
            } else {
                throw new Error("initCallGraph function not found in call_graph.js");
            }

            // Fetch file details for metadata and breadcrumb updates
            try {
                const detailsRes = await fetch(`/api/file/details/${file_md5}?collection=${encodeURIComponent(collection)}`);
                if (detailsRes.ok) {
                    const detailsData = await detailsRes.json();
                    if (detailsData && detailsData.file) {
                        const file = detailsData.file;
                        const fileName = file.file_name || file.file_names?.[0] || 'Unknown Binary';
                        window.filenameCache = window.filenameCache || {};
                        window.filenameCache[file_md5] = fileName;
                        
                        // Update breadcrumb item span dynamically
                        const items = document.querySelectorAll('#breadcrumbs-container .breadcrumb-item');
                        if (items.length >= 3) {
                            const fileSpan = items[1].querySelector('span');
                            if (fileSpan) {
                                fileSpan.innerText = fileName;
                            }
                        }

                        const renderRow = (icon, label, value, color, clickable = false, clickHandler = null) => {
                            if (!value) return '';
                            const valStr = Array.isArray(value) ? value.join(', ') : String(value);
                            const style = clickable ? `color:${color || 'var(--accent)'}; font-family:'JetBrains Mono', monospace; word-break:break-all; cursor:pointer; font-weight:bold;` : `color:${color || '#eee'}; font-family:'JetBrains Mono', monospace; word-break:break-all;`;
                            const clickAttr = clickHandler ? `onclick="${clickHandler}"` : '';
                            return `
                                <div style="display:flex; flex-direction:column; gap:4px; font-size:0.85rem; border-bottom:1px solid rgba(255,255,255,0.03); padding-bottom:8px;">
                                    <span style="color:var(--dim); font-size:0.72rem; text-transform:uppercase; display:flex; align-items:center; gap:6px;">
                                        <i class="${icon}" style="width:14px; text-align:center;"></i> ${label}
                                    </span>
                                    <span style="${style}" ${clickAttr}>${valStr}</span>
                                </div>
                            `;
                        };
                        
                        let metaHtml = '';
                        metaHtml += renderRow('fa-solid fa-file-signature', 'Filename', fileName, 'var(--accent)', true, `const showPanel = window.showFileDetailsPanel || (window.parent && window.parent.showFileDetailsPanel); if(showPanel) { showPanel('${collection}', '${file_md5}', '${fileName.replace(/'/g, "\\\\'")}', event); }`);
                        metaHtml += renderRow('fa-solid fa-microchip', 'Architecture', file.language_id || file.language, '#ae81ff');
                        metaHtml += renderRow('fa-solid fa-list-ol', 'Functions', file.function_count, '#a6e22e');
                        metaHtml += renderRow('fa-solid fa-shield', 'AV Type', file.avtype);
                        metaHtml += renderRow('fa-solid fa-file-code', 'File Type', file.filetype);
                        metaHtml += renderRow('fa-solid fa-biohazard', 'Yara', file.yara, 'var(--accent)');
                        if (file.first_seen) {
                            metaHtml += renderRow('fa-solid fa-clock', 'First Seen', new Date(file.first_seen * 1000).toLocaleString(), '#ccc');
                        }
                        
                        document.getElementById('cg-meta-content').innerHTML = metaHtml || '<div class="dim">No metadata found.</div>';
                    }
                }
            } catch (metaErr) {
                console.error("Failed to load metadata in call graph", metaErr);
                document.getElementById('cg-meta-content').innerHTML = '<div class="dim" style="color:var(--danger)">Failed to load metadata</div>';
            }

        } catch (err) {
            console.error(err);
            document.getElementById('cg-loader').innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
        }
    },

    resetZoom() {
        if (window.resetCallGraphZoom) window.resetCallGraphZoom();
    },

    toggleLabels() {
        if (window.toggleCallGraphLabels) window.toggleCallGraphLabels();
    },

    destroy() {
        // Any D3 cleanup if necessary
        this.container = null;
        this.params = null;
    }
};
