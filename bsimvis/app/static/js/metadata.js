// Shared Function Metadata Card Renderer for BSimVis

// Opens the file details panel (or navigates to the file page as a fallback).
// Kept as a named function so callers pass values as JS arguments instead of
// building a long inline handler out of untrusted strings.
window.openFileDetails = function (collection, md5, fileName, event) {
    const showPanel = window.showFileDetailsPanel || (window.parent && window.parent.showFileDetailsPanel);
    if (showPanel) {
        showPanel(collection, md5, fileName, event);
        return;
    }
    const url = `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(md5)}`;
    const Nav = window.Nav || (window.parent && window.parent.Nav);
    if (Nav) Nav.openPath(url, event, { title: `File: ${fileName}`, type: 'file' });
};

function renderFunctionMetadata(container, m, fullId, options = {}) {
    if (!m) return "";
    
    // Core data extraction
    const label = m['function_name'] || 'unknown_func';
    const returnType = m['return_type'] || 'void';
    const namespace = m['namespace'] || '';
    const parameters = m['parameters'] || [];
    const parsed = window.parseEntityId(fullId);
    const addr = m['entrypoint_address'] || parsed.address || 'N/A';
    const fileMd5 = m['file_md5'] || parsed.md5 || 'N/A';

    const collection = parsed.collection || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
    const fileId = m['file_md5'] ? `${collection}:file:${m['file_md5']}` : (fileMd5 !== 'N/A' ? `${collection}:file:${fileMd5}` : null);
    
    // Tags and Clusters
    const fileTagsHtml = EntityRenderer.renderTag('file', fileId, m['file_tags'] || [], m['file_user_tags'] || []);
    const tags = m['tags'] || [];
    const user_tags = m['user_tags'] || [];
    const clusters = m['clusters'] || [];
    const tagsHtml = EntityRenderer.renderTag('function', fullId, tags, user_tags);
    const clustersHtml = EntityRenderer.renderClusterCard(clusters);

    // Specialized relation rendering (Callers/Callees)
    const renderRelationList = (list, title, iconClass) => {
        if (!list || !Array.isArray(list) || list.length === 0) {
            return `<div class="relation-column empty">
                <div class="relation-list none">None</div>
            </div>`;
        }
        
        const itemsHtml = list.map(t => {
            if (typeof t === 'object' && t !== null && t.name) {
                const isExt = t.is_external;
                const fid = t.id || '';
                
                const funcData = {
                    ...t,
                    function_name: t.name,
                    function_id: fid,
                    entrypoint_address: t.entrypoint,
                    collection: collection
                };

                const renderOptions = {
                    showActions: false
                };

                const funcHtml = EntityRenderer.renderFunction(funcData, renderOptions);
                const color = isExt ? '#f92672' : 'var(--accent)';
                
// Robust navigation handler
window.getNavHandler = () => {
    return function(id, name, lineHash = '', e) {
        // Use global showFunctionCodeById if available (most robust)
        const globalNav = window.showFunctionCodeById || (window.parent && window.parent.showFunctionCodeById);
        
        if (globalNav) {
            globalNav(id, name, lineHash, e);
        } else {
            // Fallback for standalone: construct and open with Nav if available, or window.open
            const parsed = window.parseEntityId(id);
            const col = parsed.collection;
            const md5 = parsed.md5;
            const addr = parsed.address;
            const url = `/collection/${encodeURIComponent(col)}/function/${encodeURIComponent(md5)}/${encodeURIComponent(addr)}${lineHash}`;
            
            const Nav = window.Nav || (window.parent && window.parent.Nav);
            if (Nav) {
                Nav.openPath(url, e, { title: `Code: ${name}`, type: 'code' });
            } else {
                window.open(url, '_blank');
            }
        }
    };
};

return `<span class="relation-tag" onclick="event.stopPropagation(); window.getNavHandler()(${escapeAttr(jsString(funcData.function_id))}, ${escapeAttr(jsString(funcData.function_name || ''))}, '', event);" style="border-color:${color}; color:${color}; cursor:pointer;">
    ${funcHtml}
    ${isExt ? '<span class="ext-badge" style="background:${color}; color:white; font-size:0.6rem; padding:1px 3px; border-radius:2px; margin-left:4px;">EXT</span>' : ''}
</span>`;
            }
            return `<span class="relation-tag">${escapeHtml(JSON.stringify(t))}</span>`;
        }).join('');
        
        return `<div class="relation-column">
            <div class="relation-list">${itemsHtml}</div>
        </div>`;
    };

    const callersHtml = renderRelationList(m['callers'], 'Callers', 'fa-right-to-bracket');
    const calleesHtml = renderRelationList(m['callees'], 'Callees', 'fa-right-from-bracket');

    // Action Buttons - Compact versions for header
    let headerActionsHtml = '';
    
    // Add to Diff moved to right-click > Actions menu

    if (options.showCodeLink) {
        headerActionsHtml += `
            <a class="btn-code-compact" href="#" onclick="event.preventDefault(); const f = window.parseFuncId(${escapeAttr(jsString(fullId))}); const url = '/collection/' + encodeURIComponent(f.collection) + '/function/' + encodeURIComponent(f.md5) + '/' + encodeURIComponent(f.address); Nav.openPath(url, event, { title: 'Code: ' + f.address, type: 'function' });" title="Open Code" 
               style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; margin-left:4px; background: color-mix(in srgb, var(--token-register) 10%, transparent); color: var(--accent); border: 1px solid color-mix(in srgb, var(--token-register) 30%, transparent);">
               <i class="fa-solid fa-code"></i>
            </a>`;
    }

    // Grouping Metadata
    const funcMetaKeys = [
        'calling_convention', 'instruction_count', 'instructions_count', 'basic_blocks_count',
        'cyclomatic_complexity', 'size_bytes', 'function_id_hash'
    ];
    const bsimMetaKeys = ['bsim_features_count', 'bsim_unique_features_count', 'score', 'milvus_id'];
    const fileMetaKeys = ['file_name', 'language_id', 'file_size', 'file_md5'];
    
    const renderRow = (key, val, icon = null) => {
        const labelText = key === 'function_id_hash' ? 'function hash' : key.replace(/_/g, ' ');
        const prefix = icon ? `<i class="fa-solid ${icon}" style="margin-right:8px; opacity:0.5; width:14px;"></i>` : '';
        let valueDisplay = (key === 'entrypoint_address') ? `@ ${escapeHtml(val)}` : (key === 'file_md5' ? `# ${escapeHtml(val)}` : escapeHtml(val));
        const valueColor = (key.includes('address') || key.includes('md5') || key.includes('id') || key.includes('count') || key.includes('score')) ? 'var(--accent)' : 'inherit';
        
        if (key === 'bsim_features_count') {
            valueDisplay = `${escapeHtml(val)} <button class="btn-icon" onclick="navigateToFeatures(${escapeAttr(jsString(fullId))}, event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0 2px; font-size: 0.8rem; opacity: 0.7; display:inline-flex; align-items:center; vertical-align:middle; margin-left:4px;">🔍</button>`;
        }
        if (key === 'file_name') {
            valueDisplay = `<b style="color:var(--accent); cursor:pointer;" onclick="openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(fileMd5))}, ${escapeAttr(jsString(val))}, event)">${escapeHtml(val)}</b>`;
        }

        return `<div class="meta-row" title="${labelText}">
            <span class="meta-label">${prefix}${labelText}</span>
            <span class="meta-value mono" style="color:${valueColor}">${valueDisplay}</span>
        </div>`;
    };

    const getIcon = (k) => {
        if (k === 'entrypoint_address') return 'fa-location-dot';
        if (k === 'calling_convention') return 'fa-handshake';
        if (k === 'instructions_count' || k === 'instruction_count') return 'fa-list-ol';
        if (k === 'basic_blocks_count') return 'fa-cubes';
        if (k === 'cyclomatic_complexity') return 'fa-diagram-project';
        if (k === 'size_bytes') return 'fa-weight-hanging';
        if (k === 'function_id_hash') return 'fa-hashtag';
        if (k === 'language_id') return 'fa-globe';
        if (k === 'file_name') return 'fa-file';
        if (k === 'file_size') return 'fa-floppy-disk';
        if (k === 'file_md5') return 'fa-hashtag';
        if (k === 'bsim_features_count') return 'fa-fingerprint';
        if (k === 'bsim_unique_features_count') return 'fa-fingerprint';
        if (k === 'score') return 'fa-percent';
        if (k === 'milvus_id') return 'fa-database';
        return null;
    };

    let funcHtml = '';
    funcMetaKeys.forEach(k => { if (m[k] !== undefined) funcHtml += renderRow(k, m[k], getIcon(k)); });

    let bsimHtml = '';
    bsimMetaKeys.forEach(k => { if (m[k] !== undefined) bsimHtml += renderRow(k, m[k], getIcon(k)); });

    let fileHtml = '';
    fileMetaKeys.forEach(k => { 
        let val = m[k];
        if (k === 'file_md5' && val === undefined) val = fileMd5;
        if (val !== undefined) fileHtml += renderRow(k, val, getIcon(k)); 
    });
    const cachedCollapsed = localStorage.getItem('metadata_more_collapsed');
    const startCollapsed = (cachedCollapsed === null || cachedCollapsed === 'true');
    const collapseClass = startCollapsed ? 'collapsed' : '';
    const chevronClass = startCollapsed ? 'fa-chevron-right' : 'fa-chevron-down';
    const anglesIconClass = startCollapsed ? 'fa-angles-down' : 'fa-angles-up';
    const toggleText = startCollapsed ? 'Show more metadata' : 'Show less metadata';

    const cardHtml = `
    <div class="func-meta-card modern flattened compact" ${options.side ? `data-side="${escapeAttr(options.side)}"` : ''}>
        <div class="meta-header">
            <div class="meta-title-row" style="margin-bottom: 8px;">
                <div class="meta-sig" 
                     data-entity-data='${escapeAttr(JSON.stringify({
                        function_id: fullId,
                        function_name: label,
                        file_md5: fileMd5,
                        entrypoint_address: addr,
                        collection: collection
                     }))}'
                     oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                    <span class="meta-return-type">${escapeHtml(returnType)}</span>
                    ${namespace ? `<span class="meta-namespace">${escapeHtml(namespace)}::</span>` : ''}
                    <span class="meta-func-name">${escapeHtml(label)}</span>
                    <span class="meta-params">(</span>${parameters.map(p => `<span class="meta-param-type">${escapeHtml(typeof p === 'object' && p !== null ? (p.name || JSON.stringify(p)) : p)}</span>`).join('<span class="meta-comma">, </span>')}<span class="meta-params">)</span>
                    
                    <div class="meta-sig-actions" style="margin-left: 10px;">
                        ${EntityRenderer.renderNoteButton(fullId, m.note_owners, { raw_data: m })}
                        ${headerActionsHtml}
                    </div>

                    <div class="meta-header-addr mono" style="margin-left: 10px; color: var(--accent); font-size: 0.8rem; opacity: 0.8;">
                        @ ${escapeHtml(addr)}
                    </div>

                    <div class="meta-header-tags" style="display: inline-flex; gap: 4px; margin-left: 10px;">
                        ${tagsHtml}
                    </div>
                </div>
                <div class="meta-header-actions">
                    ${options.rightHeaderHtml || ''}
                </div>
            </div>
            
            <div class="meta-header-file-row" style="display: flex; justify-content: space-between; align-items: center; font-size: 0.85rem; padding-top: 6px; border-top: 1px solid var(--border); color: #ddd; margin-top: 6px;">
                <div style="display: flex; gap: 12px; align-items: center; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; max-width: 80%;"
                     data-entity-data='${escapeAttr(JSON.stringify({
                        type: "file",
                        file_md5: fileMd5,
                        file_name: m["file_name"] || "N/A",
                        collection: collection
                     }))}'
                     oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
                    <span class="mono" style="color: var(--accent); font-size: 1rem; font-family: 'JetBrains Mono', 'Consolas', monospace;"># ${escapeHtml(fileMd5)}</span>
                    <span><b style="font-size: 1rem; color: var(--accent); font-family: 'Inter', sans-serif; cursor: pointer;" onclick="openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(fileMd5))}, ${escapeAttr(jsString(m['file_name'] || ''))}, event)">${escapeHtml(m['file_name'] || 'N/A')}</b></span>
                    <div style="display: flex; align-items: center; overflow: hidden; margin-left: 5px;">
                        ${fileTagsHtml}
                    </div>
                </div>
            </div>
        </div>

        <div class="meta-content">
            <div class="meta-section" data-section-type="metadata">
                <div class="meta-col-body-wrapper ${collapseClass}">
                    <div class="meta-columns meta-col-body" style="gap: 20px;">
                        <!-- Row 1: Metadata Fields -->
                    <div class="meta-col" style="border-right: 1px solid var(--border); padding-right: 15px;">
                        <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-gears"></i> Function Metadata</div>
                        ${funcHtml}
                    </div>
                    <div class="meta-col">
                        <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-fingerprint"></i> BSim Vector Info</div>
                        ${bsimHtml}
                        ${clustersHtml ? `
                        <div class="meta-row" style="margin-top:4px; border-top: 1px solid var(--border); padding-top:4px;">
                            <span class="meta-label"><i class="fa-solid fa-bullseye" style="margin-right:8px; opacity:0.5; width:14px;"></i>Clusters</span>
                            <span class="meta-value">${clustersHtml}</span>
                        </div>` : ''}
                        
                        <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-top:12px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-file-invoice"></i> File Metadata</div>
                        ${fileHtml}
                    </div>
                    
                    <!-- Row 2: Relations (Callers & Callees aligned) -->
                    <div class="meta-col" style="grid-column: span 2; border-top: 1px solid var(--border); padding-top: 12px; margin-top: 8px;">
                        <div class="meta-columns" style="margin-bottom: 0;">
                            <div class="meta-col" style="border-right: 1px solid var(--border); padding-right: 15px;">
                                <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;">
                                    <i class="fa-solid fa-right-to-bracket"></i> Callers <span class="count" style="font-size:0.7rem; opacity:0.6; font-weight:normal; margin-left:4px;">(${m['callers'] ? m['callers'].length : 0})</span>
                                </div>
                                ${callersHtml}
                            </div>
                            <div class="meta-col">
                                <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;">
                                    <i class="fa-solid fa-right-from-bracket"></i> Callees <span class="count" style="font-size:0.7rem; opacity:0.6; font-weight:normal; margin-left:4px;">(${m['callees'] ? m['callees'].length : 0})</span>
                                </div>
                                ${calleesHtml}
                            </div>
                        </div>
                    </div>
                </div>
                </div>
            </div>
            <div style="display: flex; justify-content: center; padding-top: 4px; padding-bottom: 4px;">
                <button class="btn-more-toggle" onclick="toggleSection(this)" style="display: flex; align-items: center; gap: 6px; background: var(--hover); border: 1px solid var(--border); padding: 4px 12px; border-radius: 4px; font-size: 0.75rem; color: var(--meta-text-muted); cursor: pointer; user-select: none; transition: all 0.2s;">
                    <i class="fa-solid ${anglesIconClass} toggle-icon" style="font-size: 0.75rem; opacity: 0.7;"></i>
                    <span class="toggle-text" style="font-weight: 500;">${toggleText}</span>
                    <i class="fa-solid ${chevronClass} toggle-chevron" style="font-size: 0.7rem; opacity: 0.5;"></i>
                </button>
            </div>
        </div>
    </div>
    `;

    if (container) {
        if (typeof container === 'string') {
            const el = document.getElementById(container);
            if (el) el.innerHTML = cardHtml;
        } else {
            container.innerHTML = cardHtml;
        }
    }
    return cardHtml;
}

function toggleMetaDetail(id, event) {
    const el = document.getElementById(id);
    if (!el) return;
    
    const isExpanded = el.classList.toggle('expanded');
    
    // Find the toggle button that was clicked to rotate the icon
    let btn = event ? event.currentTarget : null;
    if (!btn && event) {
        btn = event.target.closest('.btn-details-toggle');
    }
    
    if (btn) {
        btn.classList.toggle('expanded', isExpanded);
        const icon = btn.querySelector('.toggle-icon');
        if (icon) {
            if (isExpanded) {
                icon.classList.remove('fa-chevron-down');
                icon.classList.add('fa-chevron-up');
            } else {
                icon.classList.remove('fa-chevron-up');
                icon.classList.add('fa-chevron-down');
            }
        }
    }
}

function navigateToFeatures(id, e) {
    const parsed = window.parseEntityId(id);
    const col = parsed.collection;
    const md5 = parsed.md5;
    const addr = parsed.address;
    const url = `/collection/${encodeURIComponent(col)}/function/${encodeURIComponent(md5)}/${encodeURIComponent(addr)}/features`;

    Nav.openPath(url, e, { title: `Features: ${addr}`, type: 'features' });
}

function seeSimilar(fullId, e) {
    // If no ID passed, try to find it from the card context or global
    const id = fullId || window.currentFuncId;
    if (!id) return;

    const parsed = window.parseEntityId(id);
    const col = parsed.collection;
    const md5 = parsed.md5;
    const addr = parsed.address;
    
    const url = `/collection/${encodeURIComponent(col)}/functions/similarities?md5=${encodeURIComponent(md5)}&address=${encodeURIComponent(addr)}&algo=unweighted_cosine`;

    Nav.openPath(url, e, { title: `Similar to: ${addr}`, type: 'function-similarity' });
}

function toggleSection(headerEl) {
    const card = headerEl.closest('.func-meta-card');
    const side = card ? card.getAttribute('data-side') : null;

    const performToggle = (cCard, forceState) => {
        const body = cCard.querySelector('.meta-col-body-wrapper');
        const chevron = cCard.querySelector('.toggle-chevron');
        const textSpan = cCard.querySelector('.toggle-text');
        const toggleIcon = cCard.querySelector('.toggle-icon');
        
        if (body) {
            const isCollapsed = (forceState !== undefined) ? body.classList.toggle('collapsed', forceState) : body.classList.toggle('collapsed');
            if (chevron) {
                if (isCollapsed) {
                    chevron.classList.remove('fa-chevron-down');
                    chevron.classList.add('fa-chevron-right');
                } else {
                    chevron.classList.remove('fa-chevron-right');
                    chevron.classList.add('fa-chevron-down');
                }
            }
            if (textSpan) {
                textSpan.textContent = isCollapsed ? 'Show more metadata' : 'Show less metadata';
            }
            if (toggleIcon) {
                if (isCollapsed) {
                    toggleIcon.classList.remove('fa-angles-up');
                    toggleIcon.classList.add('fa-angles-down');
                } else {
                    toggleIcon.classList.remove('fa-angles-down');
                    toggleIcon.classList.add('fa-angles-up');
                }
            }
            
            // Save state in cache
            localStorage.setItem('metadata_more_collapsed', isCollapsed ? 'true' : 'false');
            
            return isCollapsed;
        }
        return false;
    };

    if (card) {
        const newState = performToggle(card);
        
        // Sync with other side in diff mode
        if (side) {
            const otherSide = side === 'l' ? 'r' : 'l';
            const otherCard = document.querySelector(`.func-meta-card[data-side="${otherSide}"]`);
            if (otherCard) {
                performToggle(otherCard, newState);
            }
        }
    }
}

function renderFileMetadata(container, m, fullId, options = {}) {
    if (!m) return "";
    
    const fileName = m['file_name'] || 'unknown_file';
    const fileMd5 = m['file_md5'] || 'N/A';
    const collection = fullId ? window.getCollectionFromId(fullId) : '';
    const fileId = fullId || (m['file_md5'] ? `${collection}:file:${m['file_md5']}` : null);
    
    const tagsHtml = EntityRenderer.renderTag('file', fileId, m['tags'] || [], m['user_tags'] || []);
    
    // Grouping Metadata
    const fileMetaKeys = ['language_id', 'processor', 'compiler', 'format', 'endian', 'address_size'];
    const statsMetaKeys = ['function_count', 'bsim_features_count', 'cohesion_score', 'entry_date', 'file_date'];
    
    const renderRow = (key, val, icon = null) => {
        const labelText = key.replace(/_/g, ' ');
        const prefix = icon ? `<i class="fa-solid ${icon}" style="margin-right:8px; opacity:0.5; width:14px;"></i>` : '';
        let valueDisplay = escapeHtml(val);
        if (key.includes('date') && val) valueDisplay = escapeHtml(new Date(val).toLocaleString());
        if (key === 'cohesion_score' && val) valueDisplay = (val * 100).toFixed(1) + '%';
        
        const valueColor = (key.includes('md5') || key.includes('count') || key.includes('score')) ? 'var(--accent)' : 'inherit';
        
        return `<div class="meta-row" title="${labelText}">
            <span class="meta-label">${prefix}${labelText}</span>
            <span class="meta-value mono" style="color:${valueColor}">${valueDisplay}</span>
        </div>`;
    };

    const getIcon = (k) => {
        if (k === 'language_id') return 'fa-globe';
        if (k === 'processor') return 'fa-microchip';
        if (k === 'compiler') return 'fa-terminal';
        if (k === 'format') return 'fa-file-code';
        if (k === 'endian') return 'fa-arrow-right-arrow-left';
        if (k === 'address_size') return 'fa-ruler-horizontal';
        if (k === 'function_count') return 'fa-list-ol';
        if (k === 'bsim_features_count') return 'fa-fingerprint';
        if (k === 'cohesion_score') return 'fa-percent';
        if (k.includes('date')) return 'fa-calendar-days';
        return null;
    };

    let fileHtml = '';
    fileMetaKeys.forEach(k => { if (m[k] !== undefined) fileHtml += renderRow(k, m[k], getIcon(k)); });

    let statsHtml = '';
    statsMetaKeys.forEach(k => { if (m[k] !== undefined) statsHtml += renderRow(k, m[k], getIcon(k)); });

    const cardHtml = `
    <div class="func-meta-card modern flattened compact file-meta-card" ${options.side ? `data-side="${escapeAttr(options.side)}"` : ''}>
        <div class="meta-header">
            <div class="meta-title-row" style="margin-bottom: 8px; display: flex; align-items: center; justify-content: space-between;">
                <div class="meta-sig" 
                     data-entity-data='${escapeAttr(JSON.stringify({
                        type: 'file',
                        file_md5: fileMd5,
                        file_name: fileName,
                        collection: collection
                     }))}'
                     oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)"
                     style="display: flex; align-items: center; flex: 1;">
                    <i class="fa-solid fa-file" style="margin-right: 8px; color: var(--accent);"></i>
                    <span class="meta-func-name"><b style="color:var(--accent); cursor:pointer;" onclick="openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(fileMd5))}, ${escapeAttr(jsString(fileName))}, event)">${escapeHtml(fileName)}</b></span>
                    
                    <div class="meta-header-addr mono" style="margin-left: 10px; color: var(--accent); font-size: 0.8rem; opacity: 0.8;">
                        # ${escapeHtml(fileMd5)}
                    </div>

                    <div class="meta-header-tags" style="display: inline-flex; gap: 4px; margin-left: 10px;">
                        ${tagsHtml}
                    </div>
                </div>
            </div>
        </div>

        <div class="meta-content">
            <div class="meta-section">
                <div class="meta-columns meta-col-body" style="gap: 20px; padding: 10px 15px;">
                    <div class="meta-col" style="border-right: 1px solid var(--border); padding-right: 15px;">
                        <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-info-circle"></i> File Info</div>
                        ${fileHtml}
                    </div>
                    <div class="meta-col">
                        <div style="font-weight:bold; color:var(--accent); border-bottom: 1px solid var(--border); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-chart-bar"></i> Statistics</div>
                        ${statsHtml}
                    </div>
                </div>
            </div>
        </div>
    </div>
    `;

    if (container) {
        const el = typeof container === 'string' ? document.getElementById(container) : container;
        if (el) el.innerHTML = cardHtml;
    }
    return cardHtml;
}

window.seeSimilar = seeSimilar;
window.navigateToFeatures = navigateToFeatures;

function scoreColor(value) {
    const t = Math.max(0, Math.min(1, Number(value) || 0));
    const stops = t < 0.5 ? ['#b42318', '#8a4b08', t * 2] : ['#8a4b08', '#16803c', (t - 0.5) * 2];
    return d3.interpolateRgb(stops[0], stops[1])(stops[2]);
}
window.scoreColor = scoreColor;

// One metadata distribution -- a cluster summary's top values with their share
// of its members -- as a pie plus a legend. Shared by the file view and the
// cluster detail view's Metadata tab, which show the same six distributions off
// the same cluster meta; two copies of this drifted apart once already.
function metadataSlug(value) {
    return String(value || 'metadata').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function metadataPercent(item) {
    if (item.coverage !== undefined) return Number(item.coverage || 0) * 100;
    return Number(item.percent || 0);
}

let metadataTableCounter = 0;

function renderMetadataTable(title, dist, valueKey = 'value') {
    const rows = dist.map((item, i) => {
        const value = item[valueKey] || '';
        const percent = metadataPercent(item);
        const color = window.TagColor && valueKey === 'tag_id' ? TagColor.forTag(value) : scoreColor(percent / 100);
        return `<tr>
            <td><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${escapeAttr(color)};margin-right:7px;"></span><span class="mono">${escapeHtml(value)}</span></td>
            <td class="mono">${Number(item.count || 0).toLocaleString()}</td>
            <td class="mono">${percent.toFixed(0)}%</td>
            ${item.score === undefined ? '' : `<td class="mono">${(Number(item.score || 0) * 100).toFixed(0)}%</td>`}
            <td><button class="btn-copy" title="Copy value" onclick="copyToClipboard(${escapeAttr(jsString(value))}, this); event.stopPropagation();"><i class="fa-regular fa-copy"></i></button></td>
        </tr>`;
    }).join('');
    return `<table id="metadata-table-${++metadataTableCounter}" class="bin-sim-mc-table metadata-axis-table"><thead><tr><th>${escapeHtml(title)}</th><th>Count</th><th>Coverage</th>${dist.some(x => x.score !== undefined) ? '<th>Cohesion</th>' : ''}<th></th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderPie(title, icon, dist) {
    const colors = ['#66d9ef', '#a6e22e', '#f92672', '#fd971f', '#ae81ff', '#e6db74', '#75715e'];
    const pieData = dist.map((d, i) => ({ value: metadataPercent(d), color: colors[i % colors.length], label: d.value }));
    const total = pieData.reduce((sum, d) => sum + d.value, 0);
    if (total < 100) pieData.push({ value: 100 - total, color: 'var(--border)', isDummy: true });
    const svg = d3.create('svg').attr('width', 150).attr('height', 150).attr('viewBox', '0 0 150 150');
    const pie = d3.pie().value(d => d.value).sort(null);
    const arc = d3.arc().innerRadius(0).outerRadius(70);
    svg.append('g').attr('transform', 'translate(75,75)').selectAll('path').data(pie(pieData)).join('path')
        .attr('fill', d => d.data.color).attr('d', arc).append('title').text(d => d.data.isDummy ? '' : `${d.data.label}: ${d.data.value.toFixed(0)}%`);
    return `<div class="metadata-axis-chart">${svg.node().outerHTML}</div>`;
}

function renderDist(title, icon, dist) {
    if (!dist || !dist.length) return '';
    const id = `metadata-axis-${metadataSlug(title)}`;
    return `<details class="metadata-axis" id="${escapeAttr(id)}">
        <summary><i class="${icon}"></i> ${escapeHtml(title)} <span class="dim">${dist.length} values</span></summary>
        <div class="metadata-axis-body"><div class="metadata-axis-visual">${renderPie(title, icon, dist)}</div><div class="metadata-axis-table-wrap">${renderMetadataTable('Value', dist)}</div></div>
    </details>`;
}

let tagTreeCounter = 0;
function toggleTagTreeRow(id, event) {
    event.stopPropagation();
    const button = event.currentTarget;
    const open = button.getAttribute('aria-expanded') === 'true';
    button.setAttribute('aria-expanded', String(!open));
    button.innerHTML = `<i class="fa-solid fa-chevron-${open ? 'right' : 'down'}"></i>`;
    document.querySelectorAll(`[data-tag-parent="${CSS.escape(id)}"]`).forEach(row => {
        row.style.display = open ? 'none' : 'table-row';
        if (open) row.querySelectorAll('[data-tag-parent]').forEach(child => child.style.display = 'none');
    });
}

function renderTagDist(title, icon, dist) {
    if (!dist || !dist.length) return '';
    const treeId = `tag-tree-${++tagTreeCounter}`;
    const rows = [];
    const walk = (item, depth, parentId) => {
        const id = `${treeId}-${rows.length}`;
        const children = item.children || [];
        const color = window.TagColor ? TagColor.forTag(item.tag_id) : 'var(--accent)';
        rows.push({ item, id, depth, parentId, color, hasChildren: children.length > 0 });
        children.forEach(child => walk(child, depth + 1, id));
    };
    dist.forEach(item => walk(item, 0, null));
    const body = rows.map(({item, id, depth, parentId, color, hasChildren}) => {
        const coverage = metadataPercent(item);
        return `<tr data-tag-parent="${parentId ? escapeAttr(parentId) : ''}" style="${depth ? 'display:none;' : ''}">
            <td style="padding-left:${10 + depth * 18}px;"><button class="btn-copy" aria-expanded="false" ${hasChildren ? `onclick="toggleTagTreeRow(${escapeAttr(jsString(id))}, event)"` : 'style="visibility:hidden;"'}><i class="fa-solid fa-chevron-${hasChildren ? 'right' : 'minus'}"></i></button><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${escapeAttr(color)};margin:0 7px;"></span><span class="mono">${escapeHtml(item.tag_id)}</span></td>
            <td class="mono">${Number(item.count || 0).toLocaleString()}</td><td class="mono"><div class="metadata-bar"><span style="width:${Math.min(100, coverage)}%;background:${escapeAttr(color)}"></span></div>${coverage.toFixed(0)}%</td><td class="mono">${(Number(item.score || 0) * 100).toFixed(0)}%</td><td><button class="btn-copy" title="Copy tag" onclick="copyToClipboard(${escapeAttr(jsString(item.tag_id))}, this); event.stopPropagation();"><i class="fa-regular fa-copy"></i></button></td>
        </tr>`;
    }).join('');
    const bars = dist.map(item => {
        const coverage = metadataPercent(item);
        const color = window.TagColor ? TagColor.forTag(item.tag_id) : 'var(--accent)';
        return `<div style="font-size:.72rem;" title="${escapeAttr(item.tag_id)}"><div style="display:flex;justify-content:space-between;gap:8px;"><span class="mono" style="overflow:hidden;text-overflow:ellipsis;">${escapeHtml(item.tag_id)}</span><span class="mono">${coverage.toFixed(0)}%</span></div><div class="metadata-bar" style="width:100%;"><span style="width:${Math.min(100, coverage)}%;background:${escapeAttr(color)}"></span></div></div>`;
    }).join('');
    return `<details class="metadata-axis" id="${escapeAttr(treeId)}"><summary><i class="${icon}"></i> ${escapeHtml(title)} <span class="dim">${dist.length} namespaces</span></summary><div class="metadata-axis-body"><div class="metadata-axis-visual metadata-axis-bars"><div class="dim" style="font-size:.75rem;">Raw coverage · overlapping tags may exceed 100%</div>${bars}</div><div class="metadata-axis-table-wrap"><table id="metadata-table-${++metadataTableCounter}" class="bin-sim-mc-table metadata-axis-table"><thead><tr><th>Tag</th><th>Count</th><th>Coverage</th><th>Cohesion</th><th></th></tr></thead><tbody>${body}</tbody></table></div></div></details>`;
}

window.initMetadataTables = function (root) {
    if (!root || !window.TableSelection) return;
    root.querySelectorAll('table.metadata-axis-table').forEach((table, index) => {
        if (!table.id) table.id = `metadata-table-${metadataTableCounter + index + 1}`;
        new window.TableSelection(table.id);
    });
};

window.renderTagDist = renderTagDist;

window.renderDist = renderDist;
