// Shared Function Metadata Card Renderer for BSimVis

function renderFunctionMetadata(container, m, fullId, options = {}) {
    if (!m) return "";
    
    // Core data extraction
    const label = m['function_name'] || 'unknown_func';
    const returnType = m['return_type'] || 'void';
    const namespace = m['namespace'] || '';
    const parameters = m['parameters'] || [];
    const addr = m['entrypoint_address'] || (fullId ? fullId.split(':').pop() : 'N/A');
    const fileMd5 = m['file_md5'] || 'N/A';

    const collection = fullId ? fullId.split(':')[0] : 'main';
    const fileId = m['file_md5'] ? `${collection}:file:${m['file_md5']}` : null;
    
    // Tags and Clusters
    const fileTagsHtml = (fileId && typeof renderTagEditor === 'function') 
        ? renderTagEditor('file', fileId, m['file_tags'] || [], m['file_user_tags'] || []) 
        : '';
    const tags = m['tags'] || [];
    const user_tags = m['user_tags'] || [];
    const clusters = m['clusters'] || [];
    const tagsHtml = typeof renderTagEditor === 'function' ? renderTagEditor('function', fullId, tags, user_tags) : '';
    const clustersHtml = typeof renderClusterCards === 'function' ? renderClusterCards(clusters) : '';

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
                const name = t.name;
                const addr = t.entrypoint || '';
                const ns = t.namespace || '';
                const ret = t.return_type || '';
                const params = t.parameters || [];
                
                let labelHtml = '';
                if (typeof formatSigComponent === 'function') {
                    const sig = formatSigComponent(ns, ret, name, params);
                    labelHtml = `${sig.ret ? `<span class="sig-ret">${sig.ret}</span> ` : ''}${sig.ns ? `<span class="sig-ns">${sig.ns}::</span>` : ''}${name}<span class="sig-paren">(</span>${sig.params.map(p => `<span class="sig-param">${p}</span>`).join(', ')}<span class="sig-paren">)</span>`;
                } else {
                    labelHtml = name;
                }
                
                if (addr) labelHtml += ` <span class="sig-addr">@${addr}</span>`;
                
                const color = isExt ? '#f92672' : 'var(--accent)';
                const cursor = (isExt || !fid) ? 'default' : 'pointer';
                const previewAttrs = (isExt || !fid) ? '' : `
                    onmouseenter="
                        const fid='${fid}'; const n='${name.replace(/'/g, "\\'")}'; const a='${addr}';
                        if(window.showCodePreview) { showCodePreview(fid, n, a, '', 0, event); }
                        else if(window.parent && window.parent.showCodePreview) { window.parent.showCodePreview(fid, n, a, '', 0, event); }
                    "
                    onmousemove="
                        if(window.moveCodePreview) { moveCodePreview(event); }
                        else if(window.parent && window.parent.moveCodePreview) { window.parent.moveCodePreview(event); }
                    "
                    onmouseleave="
                        if(window.hideCodePreview) { hideCodePreview(event); }
                        else if(window.parent && window.parent.hideCodePreview) { window.parent.hideCodePreview(event); }
                    "
                `;
                const clickAttr = (isExt || !fid) ? '' : `onclick="
                    const fid='${fid}'; 
                    const name='${name.replace(/'/g, "\\'")}';
                    if(window.showFunctionCodeById) { 
                        showFunctionCodeById(fid, name, '', event); 
                    } else if(window.parent && window.parent.showFunctionCodeById) { 
                        window.parent.showFunctionCodeById(fid, name, '', event); 
                    } else { 
                        window.location.href='/function/index.html?id='+encodeURIComponent(fid); 
                    }"` ;

                return `<span class="relation-tag" style="border-color:${color}; color:${color}; cursor:${cursor};" ${previewAttrs} ${clickAttr}>
                    ${labelHtml}
                    ${isExt ? '<span class="ext-badge">EXT</span>' : ''}
                </span>`;
            }
            return `<span class="relation-tag">${JSON.stringify(t)}</span>`;
        }).join('');
        
        return `<div class="relation-column">
            <div class="relation-list">${itemsHtml}</div>
        </div>`;
    };

    const callersHtml = renderRelationList(m['callers'], 'Callers', 'fa-right-to-bracket');
    const calleesHtml = renderRelationList(m['callees'], 'Callees', 'fa-right-from-bracket');

    // Action Buttons - Compact versions for header
    let headerActionsHtml = '';
    
    if (options.showDiffBtn) {
        const fullTextAttr = options.diffBtnFullText ? 'data-full-text="true"' : '';
        const onhover = `onmouseenter="if(window.currentFuncId || '${fullId}') onHoverDiffButton(event, '${fullId}', '${label}')" onmouseleave="hideDiffPreview(event)" onmousemove="if(window.moveDiffPreview) moveDiffPreview(event)"`;
        headerActionsHtml += `
            <button id="${options.diffBtnId || 'add-diff-btn'}" class="btn-diff-action ${window.diffSelection && window.diffSelection.some(item => item.id === normalizeFuncId(fullId)) ? 'active' : ''}" 
                data-func-id="${normalizeFuncId(fullId)}" ${fullTextAttr} ${onhover} onclick="addToDiff('${fullId}', '${label}')" 
                title="Add to Diff" style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; width:22px; height:22px; display:inline-flex; align-items:center; justify-content:center;">
                <span>±</span>
            </button>`;
    }

    if (options.showCodeLink) {
        headerActionsHtml += `
            <a class="btn-code-compact" href="/function/index.html?id=${encodeURIComponent(fullId)}" target="_blank" title="Open Code" 
               style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; margin-left:4px; background: rgba(102, 217, 239, 0.1); color: var(--accent); border: 1px solid rgba(102, 217, 239, 0.3);">
               <i class="fa-solid fa-code"></i>
            </a>`;
    }

    // Grouping Metadata
    const funcMetaKeys = [
        'calling_convention', 'instruction_count', 'instructions_count', 'basic_blocks_count', 
        'cyclomatic_complexity', 'size_bytes'
    ];
    const bsimMetaKeys = ['bsim_features_count', 'bsim_unique_features_count', 'score', 'milvus_id'];
    const fileMetaKeys = ['file_name', 'language_id', 'file_size', 'file_md5'];
    
    const renderRow = (key, val, icon = null) => {
        const labelText = key.replace(/_/g, ' ');
        const prefix = icon ? `<i class="fa-solid ${icon}" style="margin-right:8px; opacity:0.5; width:14px;"></i>` : '';
        let valueDisplay = (key === 'entrypoint_address') ? `@ ${val}` : (key === 'file_md5' ? `# ${val}` : val);
        const valueColor = (key.includes('address') || key.includes('md5') || key.includes('id') || key.includes('count') || key.includes('score')) ? 'var(--accent)' : 'inherit';
        
        if (key === 'bsim_features_count') {
            valueDisplay = `${val} <button class="btn-icon" onclick="if (window.parent && typeof window.parent.showFeaturePanel === 'function') { window.parent.showFeaturePanel('${fullId}', event); } else if (typeof window.showFeaturePanel === 'function') { window.showFeaturePanel('${fullId}', event); } else { window.location.href = '/function/features/index.html?id=' + encodeURIComponent('${fullId}'); }" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0 2px; font-size: 0.8rem; opacity: 0.7; display:inline-flex; align-items:center; vertical-align:middle; margin-left:4px;">🔍</button>`;
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
    const toggleText = startCollapsed ? 'Show More' : 'Show Less';

    const cardHtml = `
    <div class="func-meta-card modern flattened compact" ${options.side ? `data-side="${options.side}"` : ''}>
        <div class="meta-header">
            <div class="meta-title-row" style="margin-bottom: 8px;">
                <div class="meta-sig">
                    <span class="meta-return-type">${returnType}</span>
                    ${namespace ? `<span class="meta-namespace">${namespace}::</span>` : ''}
                    <span class="meta-func-name">${label}</span>
                    <span class="meta-params">(</span>${parameters.map(p => `<span class="meta-param-type">${typeof p === 'object' && p !== null ? (p.name || JSON.stringify(p)) : p}</span>`).join('<span class="meta-comma">, </span>')}<span class="meta-params">)</span>
                    
                    <div class="meta-sig-actions" style="margin-left: 10px;">
                        <a class="btn-sim-action" href="javascript:void(0)" onclick="seeSimilar('${fullId}')" 
                           title="See Similar Functions" style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; background: rgba(174, 129, 255, 0.1); color: var(--info); border: 1px solid rgba(174, 129, 255, 0.3);">
                           <i class="fa-solid fa-code-compare"></i>
                        </a>
                        ${headerActionsHtml}
                    </div>

                    <div class="meta-header-addr mono" style="margin-left: 10px; color: var(--accent); font-size: 0.8rem; opacity: 0.8;">
                        @ ${addr}
                    </div>

                    <div class="meta-header-tags" style="display: inline-flex; gap: 4px; margin-left: 10px;">
                        ${tagsHtml}
                    </div>
                </div>
                <div class="meta-header-actions">
                    <button class="btn-copy-small" title="Copy Function ID" onclick="copyToClipboard('${fullId}', this)">
                        <i class="fa-solid fa-copy"></i>
                    </button>
                    ${options.rightHeaderHtml || ''}
                </div>
            </div>
            
            <div class="meta-header-file-row" style="display: flex; justify-content: space-between; align-items: center; font-size: 0.85rem; padding-top: 6px; border-top: 1px solid rgba(255, 255, 255, 0.05); color: #ddd; margin-top: 6px;">
                <div style="display: flex; gap: 12px; align-items: center; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; max-width: 80%;">
                    <span class="mono" style="color: var(--accent); font-size: 1rem; font-family: 'JetBrains Mono', 'Consolas', monospace;"># ${fileMd5}</span>
                    <span><b style="font-size: 1rem; color: #aaa; font-family: 'Inter', sans-serif;">${m['file_name'] || 'N/A'}</b></span>
                    <div style="display: flex; align-items: center; overflow: hidden; margin-left: 5px;">
                        ${fileTagsHtml}
                    </div>
                </div>
                <button class="btn-more-toggle" onclick="toggleSection(this)" style="display: flex; align-items: center; gap: 6px; background: rgba(255, 255, 255, 0.04); border: 1px solid rgba(255, 255, 255, 0.08); padding: 3px 8px; border-radius: 4px; font-size: 0.7rem; color: #aaa; cursor: pointer; user-select: none; transition: all 0.2s;">
                    <i class="fa-solid ${anglesIconClass} toggle-icon" style="font-size: 0.75rem; opacity: 0.7;"></i>
                    <span class="toggle-text" style="font-weight: 500;">${toggleText}</span>
                    <i class="fa-solid ${chevronClass} toggle-chevron" style="font-size: 0.7rem; opacity: 0.5;"></i>
                </button>
            </div>
        </div>

        <div class="meta-content">
            <div class="meta-section" data-section-type="metadata">
                <div class="meta-columns meta-col-body ${collapseClass}" style="gap: 20px;">
                    <!-- Row 1: Metadata Fields -->
                    <div class="meta-col" style="border-right: 1px solid rgba(255, 255, 255, 0.05); padding-right: 15px;">
                        <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-gears"></i> Function Metadata</div>
                        ${funcHtml}
                    </div>
                    <div class="meta-col">
                        <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-fingerprint"></i> BSim Vector Info</div>
                        ${bsimHtml}
                        ${clustersHtml ? `
                        <div class="meta-row" style="margin-top:4px; border-top: 1px solid rgba(255,255,255,0.05); padding-top:4px;">
                            <span class="meta-label"><i class="fa-solid fa-bullseye" style="margin-right:8px; opacity:0.5; width:14px;"></i>Clusters</span>
                            <span class="meta-value">${clustersHtml}</span>
                        </div>` : ''}
                        
                        <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:4px; margin-top:12px; margin-bottom:8px; font-size:0.75rem;"><i class="fa-solid fa-file-invoice"></i> File Metadata</div>
                        ${fileHtml}
                    </div>
                    
                    <!-- Row 2: Relations (Callers & Callees aligned) -->
                    <div class="meta-col" style="grid-column: span 2; border-top: 1px solid rgba(255, 255, 255, 0.05); padding-top: 12px; margin-top: 8px;">
                        <div class="meta-columns" style="margin-bottom: 0;">
                            <div class="meta-col" style="border-right: 1px solid rgba(255, 255, 255, 0.05); padding-right: 15px;">
                                <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;">
                                    <i class="fa-solid fa-right-to-bracket"></i> Callers <span class="count" style="font-size:0.7rem; opacity:0.6; font-weight:normal; margin-left:4px;">(${m['callers'] ? m['callers'].length : 0})</span>
                                </div>
                                ${callersHtml}
                            </div>
                            <div class="meta-col">
                                <div style="font-weight:bold; color:var(--accent); border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:4px; margin-bottom:8px; font-size:0.75rem;">
                                    <i class="fa-solid fa-right-from-bracket"></i> Callees <span class="count" style="font-size:0.7rem; opacity:0.6; font-weight:normal; margin-left:4px;">(${m['callees'] ? m['callees'].length : 0})</span>
                                </div>
                                ${calleesHtml}
                            </div>
                        </div>
                    </div>
                </div>
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
    const url = `/function/features/index.html?id=${encodeURIComponent(id)}`;
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(url, '_blank');
        return;
    }
    if (window.parent && window.parent !== window && typeof window.parent.showFeaturePanel === 'function') {
        window.parent.showFeaturePanel(id, e);
    } else if (typeof window.showFeaturePanel === 'function') {
        window.showFeaturePanel(id, e);
    } else {
        window.location.href = url;
    }
}

function seeSimilar(fullId, e) {
    // If no ID passed, try to find it from the card context or global
    const id = fullId || window.currentFuncId;
    if (!id) return;

    const parts = id.split(':');
    if (parts.length < 4) return;
    const col = parts[0];
    const md5 = parts[2];
    const addr = parts[3];
    const hash = `#function-similarity?collection=${col}&md5=${md5}&address=${addr}&algo=unweighted_cosine`;

    if (e && (e.ctrlKey || e.metaKey)) {
        window.open('/' + hash, '_blank');
        return;
    }

    if (window.parent && window.parent !== window && typeof window.parent.location !== 'undefined') {
        window.parent.location.hash = hash;
        if (typeof window.parent.hideCodePanel === 'function') {
            window.parent.hideCodePanel();
        }
    } else {
        // Standalone view: redirect to main dashboard with hash
        // hash already starts with #, so just append it to /
        window.location.href = '/' + hash;
    }
}

function toggleSection(headerEl) {
    const card = headerEl.closest('.func-meta-card');
    const side = card ? card.getAttribute('data-side') : null;

    const performToggle = (cCard, forceState) => {
        const body = cCard.querySelector('.meta-col-body');
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
                textSpan.textContent = isCollapsed ? 'Show More' : 'Show Less';
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
