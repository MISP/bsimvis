/**
 * tag_manager.js
 * Tag vocabulary page (/collections/<col>/tags) and file-analysis jobs.
 */

function tagApiParams() {
    const { collection, pool } = getRoutingState();
    const params = new URLSearchParams();
    if (collection) params.set('collection', collection);
    if (pool) params.set('pool', pool);
    return params;
}

function tagApiBody(extra = {}) {
    const { collection, pool } = getRoutingState();
    const body = { collection, ...extra };
    if (pool) body.pool = pool;
    return body;
}

async function tagPost(path, body) {
    const res = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    let data = {};
    try { data = await res.json(); } catch (e) { /* empty body */ }
    if (!res.ok) throw new Error(data.error || `${res.status} ${res.statusText}`);
    return data;
}

// --- Tag vocabulary table ---------------------------------------------------

window.renderTagVocabulary = function (items) {
    return items.map(t => {
        const tag = escapeHtml(t.tag);
        // jsString() already returns a quoted JS literal -- it goes into the
        // handler bare, then the whole handler is attribute-escaped.
        const js = escapeAttr(jsString(t.tag));
        // The swatch input needs a hex value, so it keeps the stored colour (or
        // the neutral default) -- but the card shows what the tag actually looks
        // like everywhere else, which for an untouched tag is derived.
        const color = safeCssColor(t.color);
        const ink = window.getTagMetadata ? window.getTagMetadata(t.tag).color : color;
        return `
        <tr data-id="${tag}">
            <td>
                <span class="tag-card" style="background:${tagAlpha(ink, 13)}; border:1px solid ${ink}; color:${ink}; padding:2px 8px; border-radius:10px; font-size:0.75rem;">${tag}</span>
            </td>
            <td>
                <input type="color" value="${color}" title="Tag color"
                    style="width:34px; height:22px; background:none; border:none; cursor:pointer;"
                    onchange="setTagColorValue(${js}, this.value)">
            </td>
            <td>
                <input type="number" value="${t.priority || 0}" title="Priority"
                    style="width:60px; background:var(--card-bg); color:var(--fg); border:1px solid var(--border); border-radius:4px; padding:2px 4px;"
                    onchange="setTagPriorityValue(${js}, this.value)">
            </td>
            <td>
                <input type="checkbox" ${t.llm ? 'checked' : ''} title="Include this tag in the LLM tagging vocabulary"
                    onchange="setTagLLMFlag(${js}, this.checked)">
            </td>
            <td class="mono">${t.function_count}</td>
            <td class="mono">${t.file_count}</td>
            <td class="mono">${t.similarity_count}</td>
            <td>
                <button class="btn-action" style="background:none; border:none; padding:0; color:#ff6b6b; cursor:pointer;"
                    onclick="deleteTagWithConfirm(${js}, ${t.total_count})">Delete</button>
            </td>
        </tr>`;
    }).join('');
};

window.setTagColorValue = async function (tag, color) {
    try {
        await tagPost('/api/tags/color', tagApiBody({ tag, color }));
        showToast(`Color updated for '${tag}'`, 'success');
        if (typeof fetchTagMetadata === 'function') fetchTagMetadata(getRoutingState().collection);
    } catch (e) {
        showToast(`Failed to set color: ${e.message}`, 'error');
    }
};

window.setTagPriorityValue = async function (tag, priority) {
    try {
        await tagPost('/api/tags/priority', tagApiBody({ tag, priority: parseInt(priority) || 0 }));
        showToast(`Priority updated for '${tag}'`, 'success');
    } catch (e) {
        showToast(`Failed to set priority: ${e.message}`, 'error');
    }
};

window.setTagLLMFlag = async function (tag, enabled) {
    try {
        await tagPost('/api/tags/llm', tagApiBody({ tag, llm: !!enabled }));
        showToast(`'${tag}' ${enabled ? 'added to' : 'removed from'} the LLM vocabulary`, 'success');
    } catch (e) {
        showToast(`Failed to update LLM flag: ${e.message}`, 'error');
    }
};

window.deleteTagWithConfirm = async function (tag, totalCount) {
    // Deletion strips the tag from every entity carrying it and cannot be undone.
    const warning = totalCount > 0
        ? `Delete '${tag}' and remove it from ${totalCount} entit${totalCount === 1 ? 'y' : 'ies'}?\n\nThis cannot be undone.`
        : `Delete the unused tag '${tag}'?`;
    if (!confirm(warning)) return;

    try {
        const res = await tagPost('/api/tags/delete', tagApiBody({ tag }));
        const r = res.removed || {};
        showToast(
            `Deleted '${tag}' (functions: ${r.function || 0}, files: ${r.file || 0}, similarities: ${r.similarity || 0})`,
            'success'
        );
        refreshData(false, true);
    } catch (e) {
        showToast(`Failed to delete tag: ${e.message}`, 'error');
    }
};

window.renderTagCreationForm = function () {
    const gridHeader = document.getElementById('grid-header');
    if (!gridHeader) return;
    gridHeader.innerHTML = `
        <div style="display:flex; gap:10px; align-items:center; padding:10px 0; flex-wrap:wrap;">
            <input id="new-tag-name" type="text" placeholder="New tag name"
                style="background:var(--card-bg); color:var(--fg); border:1px solid var(--border); border-radius:4px; padding:5px 8px; font-size:0.8rem;">
            <input id="new-tag-color" type="color" value="#66d9ef" title="Tag color"
                style="width:34px; height:26px; background:none; border:none; cursor:pointer;">
            <label style="font-size:0.8rem; display:flex; align-items:center; gap:5px;">
                <input id="new-tag-llm" type="checkbox" checked> LLM vocabulary
            </label>
            <button class="top-action-btn" onclick="createTagFromForm()" style="font-size:0.75rem; padding:4px 10px;">
                <i class="fa-solid fa-plus"></i> Create Tag
            </button>
        </div>`;
};

window.createTagFromForm = async function () {
    const name = (document.getElementById('new-tag-name')?.value || '').trim();
    if (!name) {
        showToast('Enter a tag name first', 'warning');
        return;
    }
    try {
        await tagPost('/api/tags/create', tagApiBody({
            tag: name,
            color: document.getElementById('new-tag-color')?.value,
            llm: !!document.getElementById('new-tag-llm')?.checked
        }));
        showToast(`Tag '${name}' created`, 'success');
        document.getElementById('new-tag-name').value = '';
        refreshData(false, true);
    } catch (e) {
        showToast(`Failed to create tag: ${e.message}`, 'error');
    }
};

// --- File analysis ----------------------------------------------------------

window.openFileAnalysisModal = function ({ fileMd5 = '', collection = '' } = {}) {
    collection = collection || getRoutingState().collection;
    if (!collection) {
        showToast('No collection selected', 'warning');
        return;
    }

    let modal = document.getElementById('file-analysis-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'file-analysis-modal';
        modal.style.cssText = 'position:fixed; inset:0; z-index:30000; display:flex; align-items:center; justify-content:center; background:rgba(0,0,0,.65); backdrop-filter:blur(4px);';
        document.body.appendChild(modal);
    }
    modal.dataset.collection = collection;
    modal.dataset.fileMd5 = fileMd5;
    const target = fileMd5 ? `file ${fileMd5}` : `collection ${collection}`;
    modal.innerHTML = `
        <form onsubmit="submitFileAnalysis(event)" style="width:500px; max-width:90vw; background:var(--card-bg); border:1px solid var(--border); border-radius:10px; padding:22px; color:var(--fg);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:18px;">
                <h3 style="margin:0; color:#ae81ff;"><i class="fa-solid fa-robot"></i> Analyze ${escapeHtml(target)}</h3>
                <button type="button" onclick="closeFileAnalysisModal()" style="background:none; border:0; color:var(--subtle); cursor:pointer; font-size:1.3rem;">&times;</button>
            </div>
            <label style="display:block; margin-bottom:14px;">Minimum BSim features
                <input id="file-analysis-min" type="number" min="0" value="0" style="display:block; width:100%; box-sizing:border-box; margin-top:5px; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;">
            </label>
            <label style="display:block; margin-bottom:14px;">Prompt
                <textarea id="file-analysis-prompt" placeholder="Leave empty for the configured default" style="display:block; width:100%; min-height:100px; box-sizing:border-box; margin-top:5px; padding:8px; resize:vertical; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;"></textarea>
            </label>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:18px;">
                <label><input id="file-analysis-skip-fid" type="checkbox" checked> Skip FID-tagged functions</label>
                <label><input id="file-analysis-overwrite" type="checkbox"> Replace existing LLM output</label>
                <label><input id="file-analysis-notes" type="checkbox" checked> Write notes</label>
                <label><input id="file-analysis-tags" type="checkbox" checked> Write tags</label>
            </div>
            <div style="display:flex; justify-content:flex-end; gap:10px;">
                <button type="button" onclick="closeFileAnalysisModal()" class="top-action-btn">Cancel</button>
                <button type="submit" class="top-action-btn" style="color:#ae81ff; border-color:#ae81ff;"><i class="fa-solid fa-play"></i> Create job</button>
            </div>
        </form>`;
    modal.onclick = e => { if (e.target === modal) closeFileAnalysisModal(); };
};

window.closeFileAnalysisModal = function () {
    document.getElementById('file-analysis-modal')?.remove();
};

window.submitFileAnalysis = async function (event) {
    event.preventDefault();
    const modal = document.getElementById('file-analysis-modal');
    const actions = [];
    if (document.getElementById('file-analysis-notes').checked) actions.push('notes');
    if (document.getElementById('file-analysis-tags').checked) actions.push('tags');
    if (!actions.length) {
        showToast('Select notes, tags, or both', 'warning');
        return;
    }

    const body = {
        collection: modal.dataset.collection,
        actions,
        min_complexity: Number(document.getElementById('file-analysis-min').value),
        skip_fid_tagged: document.getElementById('file-analysis-skip-fid').checked,
        overwrite: document.getElementById('file-analysis-overwrite').checked
    };
    if (modal.dataset.fileMd5) body.file_md5 = modal.dataset.fileMd5;
    const prompt = document.getElementById('file-analysis-prompt').value.trim();
    if (prompt) body.custom_prompt = prompt;

    try {
        const result = await tagPost('/api/llm/file_analysis', body);
        closeFileAnalysisModal();
        showToast(`Analysis job started for ${result.files} file(s), ${result.total} function(s)`, 'success');
        trackFileAnalysis(result.job_id);
    } catch (e) {
        showToast(`Could not start analysis: ${e.message}`, 'error');
    }
};

function fileAnalysisPanel() {
    let panel = document.getElementById('file-analysis-panel');
    if (!panel) {
        panel = document.createElement('div');
        panel.id = 'file-analysis-panel';
        panel.style.cssText =
            'position:fixed; right:20px; bottom:20px; z-index:30000; display:flex; flex-direction:column; gap:8px;';
        document.body.appendChild(panel);
    }
    return panel;
}

window.trackFileAnalysis = function (jobId) {
    const card = document.createElement('div');
    card.style.cssText =
        'background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 14px; min-width:260px; font-size:0.8rem; color:var(--fg); ';
    card.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
            <span><i class="fa-solid fa-robot"></i> File analysis</span>
            <button title="Cancel" style="background:none; border:none; color:#ff6b6b; cursor:pointer;"
                onclick="cancelFileAnalysis('${jobId}')"><i class="fa-solid fa-stop"></i></button>
        </div>
        <div class="file-analysis-status" style="margin-top:6px; opacity:0.85;">queued</div>`;
    fileAnalysisPanel().appendChild(card);

    const statusEl = card.querySelector('.file-analysis-status');

    const poll = async () => {
        let data;
        try {
            const res = await fetch(`/api/jobs/${jobId}`);
            data = await res.json();
        } catch (e) {
            statusEl.textContent = 'status unavailable';
            return;
        }

        statusEl.textContent = `${data.status} · ${data.progress || 0}%`;
        if (['completed', 'failed', 'cancelled'].includes(data.status)) {
            if (typeof refreshData === 'function') refreshData(false, true);
            setTimeout(() => card.remove(), 12000);
            return;
        }
        setTimeout(poll, 2000);
    };
    poll();
};

window.cancelFileAnalysis = async function (jobId) {
    try {
        await tagPost(`/api/jobs/${jobId}/cancel`, {});
        showToast('File analysis cancelled', 'info');
    } catch (e) {
        showToast(`Could not cancel analysis: ${e.message}`, 'error');
    }
};
