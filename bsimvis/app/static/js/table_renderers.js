/**
 * table_renderers.js
 * Extracted table rendering logic from dashboard.js.
 */

function createNav(view, collection, params = {}) {
    let pathSegments = [];
    if (view === 'upload') {
        pathSegments = ['upload'];
    } else if (view === 'function-similarity') {
        pathSegments = ['functions', 'similarities'];
    } else if (view === 'binary-similarity') {
        pathSegments = ['files', 'similarities'];
    } else if (view === 'clusters') {
        pathSegments = ['functions', 'clusters'];
    } else if (view === 'bin-clusters') {
        pathSegments = ['files', 'clusters'];
    } else if (view === 'features-global') {
        pathSegments = ['features'];
    } else {
        pathSegments = [view];
    }
    
    // Convert params object to path segments if possible,
    // otherwise append as query parameters (though we want to avoid this).
    // Let's analyze the parameters used: batch_uuid, file_md5.
    
    let url = Nav.buildUIUrl(collection, pathSegments);
    
    // For now, handle known parameters as query parameters.
    if (params.batch_uuid) {
        url += `?batch_uuid=${encodeURIComponent(params.batch_uuid)}`;
    } else if (params.file_md5) {
        url += `?file_md5=${encodeURIComponent(params.file_md5)}`;
    }
    
    // If there are other params, we might have to keep them as query params for now
    // until the backend routes are updated to fully support these as paths.
    // For this step, I'll focus on the identified ones.
    
    return `href="${url}" onclick="Nav.openPath('${url}', event)"`;
}

window.TableRenderers = {
    renderCollections: function(data) {
        if (!data.length) return '<tr><td colspan="6" style="text-align:center">No collections found.</td></tr>';

        return data.map(col => `
            <tr data-id="${col.name}">
                <td><a ${createNav('files', col.name)} class="clickable-count" style="font-weight:bold;">${col.name}</a></td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('batches', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_batches'] || 0}</a>
                        <a ${createNav('batches', col.name)} class="btn-action" title="Batches">
                            <i class="fa-solid fa-boxes-stacked"></i>
                        </a>
                    </div>
                </td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('files', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_files']}</a>
                        <a ${createNav('files', col.name)} class="btn-action" title="Files">
                            <i class="fa-solid fa-file-code"></i>
                        </a>
                    </div>
                </td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('functions', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_functions']}</a>
                        <a ${createNav('functions', col.name)} class="btn-action" title="Functions">
                            <i class="fa-solid fa-code"></i>
                        </a>
                    </div>
                </td>
                <td class="dim">${formatDate(col['last_updated'])}</td>
                <td>
                    <div style="display: flex; gap: 15px;">
                        <a ${createNav('upload', col.name)} class="btn-action" title="Upload" style="color:var(--accent); display: flex; align-items: center; gap: 5px; text-decoration: none;">
                            <i class="fa-solid fa-cloud-arrow-up"></i>
                            <span style="font-size: 0.8rem;">Upload</span>
                        </a>
                    </div>
                </td>
            </tr>
        `).join('');
    },

    renderBatches: function(data) {
        return data.map(b => {
            const col = b.collection || 'unknown';
            return `
            <tr data-id="${b['batch_uuid']}">
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <b>${b.name || 'Unnamed'}</b>
                        <button class="btn-copy" title="Copy Batch ID: ${b['batch_id']}" onclick="copyToClipboard('${b['batch_id']}', this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </td>
                <td class="mono dim" style="font-size:0.7rem">${b['batch_uuid']}</td>
                <td class="mono"><a ${createNav('files', col, { batch_uuid: b['batch_uuid'] })} class="clickable-count">${b['total_files']}</a></td>
                <td class="mono"><a ${createNav('functions', col, { batch_uuid: b['batch_uuid'] })} class="clickable-count">${b['total_functions']}</a></td>
                <td class="dim">${formatDate(b['last_updated'] || b['created_at'])}</td>
                <td>
                    <div style="display: flex; gap: 15px;">
                        <a ${createNav('upload', col, { batch_uuid: b['batch_uuid'] })} class="btn-action" title="Upload to Batch" style="color:var(--accent)">
                            <i class="fa-solid fa-cloud-arrow-up"></i>
                        </a>
                    </div>
                </td>
            </tr>
        `}).join('');
    },
    
    // ... renderFiles (needs update)

    renderFiles: function(data, clustersMap = {}) {
        const { collection } = getRoutingState();
        const col = collection;
        return data.map(f => {
            const fileId = f['file_id'] || `${col}:file:${f['file_md5']}`;
            const tags = f['tags'] || [];
            const user_tags = f['user_tags'] || [];
            const rowStyle = getRowTagColor(tags, user_tags);
            const batchUuid = f['batch_uuid'] || '---';
            const funcCount = f['function_count'] !== undefined ? f['function_count'] : 0;

            const clusters = (Array.isArray(f['bin_clusters']) ? f['bin_clusters'] : []).map(cid => clustersMap[cid]).filter(Boolean);

            return `
            <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${fileId}"
                data-entity-data='${JSON.stringify({
                    md5: f['file_md5'],
                    file_name: f['file_name'],
                    id: fileId,
                    collection: col
                }).replace(/'/g, "&apos;")}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
                <td class="sim-cell">
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <b style="color:var(--accent); cursor:pointer;" onclick="showFileDetailsPanel('${col}', '${f['file_md5']}', '${(f['file_name'] || '').replace(/'/g, "\\'")}', event)">${f['file_name']}</b>
                    </div>
                </td>
                <td class="sim-cell">
                    ${EntityRenderer.renderMd5(f['file_md5'], { full: true })}
                    <div class="dim" style="font-size:0.65rem">${f['language_id']}</div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:2px; font-size:0.65rem;">
                        ${(() => {
                            const fields = [
                                { key: 'yara', label: 'Yara', dist: 'yara_distribution' },
                                { key: 'avtype', label: 'AV', dist: 'avtype_distribution' },
                                { key: 'filetype', label: 'Type', dist: 'filetype_distribution' },
                                { key: 'cc_ip', label: 'IP', dist: 'ccip_distribution' }
                            ];
                            
                            return fields.map(field => {
                                const val = f[field.key];
                                if (val && val.length) {
                                    return `<div class="dim">${field.label}: <span style="color:var(--accent)">${val.join(', ')}</span></div>`;
                                }
                                
                                // Try inference
                                let bestInf = null;
                                clusters.forEach(c => {
                                    const dist = c[field.dist] || [];
                                    if (dist.length > 0) {
                                        const cohesion = c.cohesion_score || 0;
                                        if (!bestInf || cohesion > bestInf.cohesion) {
                                            bestInf = { value: dist[0].value, cohesion: cohesion };
                                        }
                                    }
                                });
                                
                                if (bestInf) {
                                    const hue = Math.max(0, Math.min(120, bestInf.cohesion * 120));
                                    const color = `hsl(${hue}, 80%, 60%)`;
                                    return `<div class="dim" title="Inferred from cluster (cohesion: ${(bestInf.cohesion*100).toFixed(1)}%)">
                                        ${field.label}: <span style="color:${color}; opacity: 0.9; font-style: italic;">${bestInf.value} <small>(${(bestInf.cohesion*100).toFixed(0)}%)</small></span>
                                    </div>`;
                                }
                                return '';
                            }).join('');
                        })()}
                        ${f['first_seen'] && f['first_seen'].length ? `<div class="dim">Seen: <span style="color:var(--accent)">${f['first_seen'].join(', ')}</span></div>` : ''}
                    </div>
                </td>
                <td class="sim-cell mono dim" style="font-size:0.7rem" title="${batchUuid}">
                    ${batchUuid.length > 8 ? batchUuid.substring(0, 8) + '...' : batchUuid}
                </td>
                <td class="sim-cell">
                    <div style="display:inline-flex; align-items:center; justify-content:center; gap:8px; width:100%;">
                        <a ${createNav('functions', col, { file_md5: f['file_md5'] })} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${funcCount}</a>
                        <button class="btn-file-diff-action ${fileDiffSelection.some(item => item.id === fileId) ? 'active' : ''}"
                                data-file-id="${fileId}"
                                onclick="addToFileDiff('${fileId}', '${f['file_name'].replace(/'/g, "'")}', event)"
                                title="Add to File Diff">
                            <span>±</span>
                        </button>
                        <a class="btn-action" onclick="Nav.openPath('${Nav.buildUIUrl(col, ['call_graph', f['file_md5']])}', event, { title: 'Call Graph: ${f['file_md5']}', type: 'call_graph' })" title="Call Graph" style="color: var(--accent); cursor: pointer;">
                            <i class="fa-solid fa-network-wired"></i>
                        </a>
                    </div>
                </td>
                <td class="sim-cell file-note-cell" style="text-align:center;">
                    ${EntityRenderer.renderFileNoteButton(fileId, f.note_owners, { isTable: true, raw_data: f })}
                </td>
                <td class="cluster-cards-cell" data-is-binary="true" data-clusters='${JSON.stringify(clusters).replace(/'/g, "&apos;")}'>
                    ${EntityRenderer.renderClusterCard(clusters, true)}
                </td>
                <td class="sim-cell dim">${formatDate(f['entry_date'])}</td>
                <td>
                    ${EntityRenderer.renderTag('file', fileId, tags, user_tags)}
                </td>
            </tr>
        `;
        }).join('');
    },

    renderFunctions: function(data, clustersMap = {}) {
        return data.map(f => {
            const entry = f['entrypoint_address'] || '';
            const tags = f['tags'] || [];
            const user_tags = f['user_tags'] || [];
            const fileName = f['file_name'] || '';
            const file_md5 = f['file_md5'] || '';
            const language = f['language_id'] || '---';
            const featCount = f['bsim_features_count'] || 0;
            const funcId = f['function_id'] || `${f.collection}:func:${file_md5}:${entry}`;
            const rowStyle = getRowTagColor(tags, user_tags);
            const clusters = (f['clusters'] || []).map(uuid => clustersMap[uuid]).filter(Boolean);

            return `
            <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${funcId}"
                data-entity-data='${JSON.stringify(f).replace(/'/g, "&apos;")}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                <td class="sim-cell" style="min-width: 300px;">
                    ${EntityRenderer.renderFunction(f, { hideNote: true })}
                </td>
                <td class="sim-cell"><span class="mono" style="color:var(--accent);">@ ${entry}</span></td>
                <td>${EntityRenderer.renderTag('function', funcId, tags, user_tags)}</td>
                <td class="cluster-cards-cell" data-clusters='${JSON.stringify(clusters).replace(/'/g, "&apos;")}'>${EntityRenderer.renderClusterCard(clusters)}</td>
                <td class="sim-cell" style="text-align:center;">
                    <div style="display:inline-flex; align-items:center; gap:6px;">
                        <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                        <button class="btn-icon" onclick="showFeaturePanel('${funcId}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                    </div>
                </td>
                <td class="sim-cell" style="text-align:center;">
                    ${EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true, raw_data: f })}
                </td>

                <td class="sim-cell"><div style="max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8;" title="${fileName}"><b style="color:var(--accent); cursor:pointer;" onclick="const showPanel = window.showFileDetailsPanel || (window.parent && window.parent.showFileDetailsPanel); if(showPanel) { showPanel('${f.collection || 'main'}', '${file_md5}', '${fileName.replace(/'/g, "\\'")}', event); }">${fileName}</b></div></td>

                <td class="sim-cell">${EntityRenderer.renderMd5(file_md5)}</td>
                <td>${EntityRenderer.renderTag('file', `${f.collection || 'main'}:file:${file_md5}`, f.file_tags || [], f.file_user_tags || [])}</td>
                <td class="sim-cell"><span class="mono" style="color:var(--accent)">${language}</span></td>
                <td class="sim-cell"><span class="dim" style="font-size:0.7rem;">${formatDate(f['entry_date'] || f['file_date'])}</span></td>
                <td class="sim-cell"></td>
            </tr>
        `}).join('');
    },

    renderGlobalFeatures: function(items) {
        const { collection } = getRoutingState();
        const col = collection;
        if (!items.length) return '<tr><td colspan="7" style="text-align:center">No features found.</td></tr>';

        return items.map(f => {
            const ctx = f.context || {};

            let pcodeHtml = `
                <div class="code-card">
                    <div class="code-card-line">
                        <div class="code-card-text pcode-text">${ctx.pcode_full || 'N/A'}</div>
                    </div>
                </div>`;

            let cCodeHtml = '<span class="dim">N/A</span>';
            if (ctx.c_code) {
                const funcId = ctx.func_id || `${col}:func:${ctx.md5}:${ctx.addr}`;
                const funcName = (ctx.name || ctx.addr);
                const targetLinesStr = (ctx.line_idxs || []).map(l => l + 1).join(',');
                const lineHash = targetLinesStr ? `#L${targetLinesStr}` : '';

                const displayLine = (ctx.line_idxs && ctx.line_idxs.length > 0) ? ctx.line_idxs[0] + 1 : 1;
                
                const fData = {
                    function_id: funcId,
                    function_name: funcName,
                    file_md5: ctx.md5,
                    entrypoint_address: ctx.addr,
                    collection: col
                };

                cCodeHtml = `<div class="code-card clickable" title="Click to jump to lines ${targetLinesStr || ''}"
                        data-entity-data='${JSON.stringify(fData).replace(/'/g, "&apos;")}'
                        oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)"
                        onclick="showFunctionCodeById('${funcId}', '${funcName.replace(/'/g, "'")}', '${lineHash}', event)">
                    <div class="code-card-line">
                        <div class="code-card-ln">${displayLine}</div>
                        <div class="code-card-text">`;
                ctx.c_code.forEach(t => {
                    const colorMap = {
                        'variable': 'tok-variable', 'func_call': 'tok-func_call', 'type': 'tok-type',
                        'keyword': 'tok-keyword', 'comment': 'tok-comment', 'string': 'tok-string', 'number': 'tok-number'
                    };
                    const cls = colorMap[t.type] || 'tok-default';
                    cCodeHtml += `<span class="${cls} feature-highlight" 
                        data-hashes="${f.hash}" 
                        data-type="${ctx.type || ''}" 
                        data-op="${ctx.op || ''}" 
                        data-tf="${Math.round(f.tf_score || 0)}"
                        onmouseenter="showTokenTooltip(event)"
                        onmouseleave="hideTokenTooltip()"
                        onmousemove="moveCodePreview(event)">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
                });
                cCodeHtml += `</div></div></div>`;
            }

            return `
            <tr data-id="${f.hash}">
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <code class="mono" style="color:var(--accent)">${f.hash}</code>
                        <button class="btn-copy" title="Copy Feature ID: ${f['feature_id']}" onclick="copyToClipboard('${f['feature_id']}', this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </td>
                <td>
                    <div class="dim" style="font-size:0.65rem; font-weight:bold; color:var(--accent);">${ctx.type}</div>
                    <span class="badge" style="margin-top:2px; font-size:0.65rem;">${ctx.op}</span>
                </td>
                <td style="max-width:300px;">${pcodeHtml}</td>
                <td style="max-width:350px;">${cCodeHtml}</td>
                <td class="mono" style="color:var(--accent)">${f.tf_score !== undefined ? Math.round(f.tf_score) : '<span class="dim">-</span>'}</td>
                <td class="mono">${f.frequency}</td>
                <td>
                    <button class="btn-action" style="background:none; border:none; padding:0; font-size:0.8rem; text-align:left; color:var(--accent);"
                        onclick="showGlobalFeaturePanel('${f.hash}', '${col}', event)">Analyze</button>
                </td>

            </tr>
        `}).join('');
    }
};

// Map original function names to the new TableRenderers object for backward compatibility
// or just export them to window directly if desired.
window.renderCollections = TableRenderers.renderCollections;
window.renderBatches = TableRenderers.renderBatches;
window.renderFiles = TableRenderers.renderFiles;
window.renderFunctions = TableRenderers.renderFunctions;
window.renderGlobalFeatures = TableRenderers.renderGlobalFeatures;
