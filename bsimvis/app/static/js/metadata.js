// Shared Function Metadata Card Renderer for BSimVis

function renderFunctionMetadata(container, m, fullId, options = {}) {
    if (!m) return "";
    const label = m['function_name'] || 'unknown_func';
    const returnType = m['return_type'] || 'N/A';
    const namespace = m['namespace'] || '';
    const parameters = m['parameters'] || [];
    const addr = m['entrypoint_address'] || (fullId ? fullId.split(':').pop() : 'N/A');
    const fileName = m['file_name'] || 'N/A';
    const lang = m['language_id'] || 'N/A';

    const collection = fullId ? fullId.split(':')[0] : 'main';
    const fileId = m['file_md5'] ? `${collection}:file:${m['file_md5']}` : null;
    const fileTagsHtml = (fileId && typeof renderTagEditor === 'function') 
        ? renderTagEditor('file', fileId, m['file_tags'] || [], m['file_user_tags'] || []) 
        : '';

    const tags = m['tags'] || [];
    const user_tags = m['user_tags'] || [];
    const clusters = m['clusters'] || [];
    const tagsHtml = typeof renderTagEditor === 'function' ? renderTagEditor('function', fullId, tags, user_tags) : '';
    const clustersHtml = typeof renderClusterCards === 'function' ? renderClusterCards(clusters) : '';

    const skipKeys = ['function_name', 'return_type', 'namespace', 'parameters', 'entrypoint_address', 'file_name', 'language_id', 'full_id', 'tags', 'user_tags', 'file_tags', 'file_user_tags', 'clusters', 'function_id'];
    let moreHtml = '';
    Object.keys(m).sort().forEach(k => {
        if (!skipKeys.includes(k)) {
            let val = m[k];
            if (k.endsWith('_date') || k === 'created_at' || k === 'last_updated') {
                if (typeof formatDate === 'function') val = formatDate(val);
            }
            if (Array.isArray(val)) {
                val = val.length > 0 ? val.map(t => `<span class="tag-card">${t}</span>`).join('') : '<span style="color:#75715e">none</span>';
            } else if (typeof val === 'object' && val !== null) {
                val = JSON.stringify(val);
            }
            moreHtml += `<div class="meta-row"><span class="meta-label">${k}</span><span class="meta-value">${val}</span></div>`;
        }
    });

    let actionButtonsHtml = '';
    
    if (options.showFeaturesBtn) {
        actionButtonsHtml += `
            <button class="btn-feat-action" onclick="navigateToFeatures('${fullId}', event)">
                <span>🔍</span> Features
            </button>`;
    }
    
    if (options.showDiffBtn) {
        const fullTextAttr = options.diffBtnFullText ? 'data-full-text="true"' : '';
        const onhover = `onmouseenter="if(window.currentFuncId || '${fullId}') onHoverDiffButton(event, '${fullId}', '${label}')" onmouseleave="hideDiffPreview(event)" onmousemove="if(window.moveDiffPreview) moveDiffPreview(event)"`;
        actionButtonsHtml += `
            <button id="${options.diffBtnId || 'add-diff-btn'}" class="btn-diff-action" data-func-id="${normalizeFuncId(fullId)}" ${fullTextAttr} ${onhover} onclick="addToDiff('${fullId}', '${label}')">
                <span>±</span> ${options.diffBtnFullText ? 'Add to Diff' : ''}
            </button>`;
    }

    if (options.showSimilarBtn) {
        actionButtonsHtml += `
            <button class="btn-sim-action" onclick="seeSimilar()">
                <span>≈</span> See Similar
            </button>`;
    }

    if (options.showCodeLink) {
        actionButtonsHtml += `
            <a href="/function/index.html?id=${encodeURIComponent(fullId)}" target="_blank" class="meta-more-btn" style="text-decoration:none; display:inline-flex; align-items:center;">Code ↗</a>`;
    }

    if (options.customActionButtonsHtml) {
        actionButtonsHtml += options.customActionButtonsHtml;
    }

    const detailsToggleFn = options.detailsToggleFn || 'toggleMetaDetail()';
    const detailId = options.detailId || 'meta-more-inline';

    const cardHtml = `
    <div class="func-meta-card" ${options.side ? `data-side="${options.side}"` : ''}>
        <span class="meta-title">
            <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                <span class="meta-return-type">${returnType}</span>
                ${namespace ? `<span style="color:white; opacity:0.8">${namespace}::</span>` : ''}
                <span style="color:var(--accent)">${label}</span>
                <span style="color:white">(</span>${parameters.map(p => `<span style="color:#ae81ff">${p}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                <button class="btn-copy" title="Copy Function ID: ${fullId}" onclick="copyToClipboard('${fullId}', this)">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                </button>
                <button class="meta-more-btn" onclick="${detailsToggleFn}">Details</button>
                ${actionButtonsHtml}
            </div>
            ${options.rightHeaderHtml || ''}
        </span>
        <div class="meta-row" ${namespace ? '' : 'style="display:none"'}><span class="meta-label">Namespace</span><span class="meta-value">${namespace}</span></div>
        <div class="meta-row"><span class="meta-label">Address</span><span class="meta-value">${addr}</span></div>
        <div class="meta-row"><span class="meta-label">File</span><span class="meta-value" style="display:flex; align-items:center; gap:10px;">${fileName} ${fileTagsHtml}</span></div>
        <div class="meta-row"><span class="meta-label">Language</span><span class="meta-value">${lang}</span></div>
        <div class="meta-row"><span class="meta-label">Tags</span><span class="meta-value" style="white-space:normal">${tagsHtml} ${clustersHtml}</span></div>
        
        <div class="meta-more-container" id="${detailId}">
            ${moreHtml}
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
