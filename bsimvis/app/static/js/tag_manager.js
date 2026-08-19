/**
 * tag_manager.js
 * Tag vocabulary page (/collections/<col>/tags) and the LLM batch enrichment
 * actions driven from the function list context menu.
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

// --- LLM batch enrichment ---------------------------------------------------

/**
 * Starts an LLM batch job.
 * @param {string[]} actions - ['notes'], ['tags'] or both.
 * @param {Object} [opts]
 * @param {string[]} [opts.funcIds] - explicit ids; defaults to the table selection.
 * @param {string} [opts.filters] - function-search query string (whole result set).
 * @param {boolean} [opts.askPrompt] - prompt for a custom prompt first.
 */
window.startLLMBatch = async function (actions, opts = {}) {
    const body = tagApiBody({ actions });

    if (opts.filters !== undefined) {
        body.filters = opts.filters;
    } else {
        const ids = opts.funcIds || (window.getSelectedTableIds ? window.getSelectedTableIds() : []);
        if (!ids || !ids.length) {
            showToast('Select one or more functions first', 'warning');
            return;
        }
        body.func_ids = ids;
    }

    if (opts.askPrompt) {
        const prompt = window.prompt('Custom prompt (leave empty for the configured default):', '');
        if (prompt === null) return;
        if (prompt.trim()) body.custom_prompt = prompt.trim();
    }

    try {
        const res = await tagPost('/api/llm/batch', body);
        showToast(`LLM batch started on ${res.total} function(s)`, 'success');
        trackLLMBatch(res.job_id, res.total);
    } catch (e) {
        showToast(`Could not start LLM batch: ${e.message}`, 'error');
    }
};

/** Starts a batch over every function currently matched by the page filters. */
window.startLLMBatchForCurrentFilter = function (actions) {
    const params = new URLSearchParams(window.location.search);
    params.delete('limit');
    params.delete('offset');
    startLLMBatch(actions, { filters: params.toString() });
};

/** Starts a batch over all functions of one binary. */
window.startLLMBatchForFile = function (actions, md5) {
    startLLMBatch(actions, { filters: `file_md5=${encodeURIComponent(md5)}` });
};

function llmBatchPanel() {
    let panel = document.getElementById('llm-batch-panel');
    if (!panel) {
        panel = document.createElement('div');
        panel.id = 'llm-batch-panel';
        panel.style.cssText =
            'position:fixed; right:20px; bottom:20px; z-index:30000; display:flex; flex-direction:column; gap:8px;';
        document.body.appendChild(panel);
    }
    return panel;
}

/** Polls a batch job and shows a progress card with a cancel button. */
window.trackLLMBatch = function (jobId, total) {
    const card = document.createElement('div');
    card.style.cssText =
        'background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px 14px; min-width:260px; font-size:0.8rem; color:var(--fg); ';
    card.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
            <span><i class="fa-solid fa-robot"></i> LLM batch</span>
            <button title="Cancel" style="background:none; border:none; color:#ff6b6b; cursor:pointer;"
                onclick="cancelLLMBatch('${jobId}')"><i class="fa-solid fa-stop"></i></button>
        </div>
        <div class="llm-batch-status" style="margin-top:6px; opacity:0.85;">queued · 0/${total}</div>`;
    llmBatchPanel().appendChild(card);

    const statusEl = card.querySelector('.llm-batch-status');

    const poll = async () => {
        let data;
        try {
            const res = await fetch(`/api/llm/batch/${jobId}`);
            data = await res.json();
        } catch (e) {
            statusEl.textContent = 'status unavailable';
            return;
        }

        const c = data.counts || {};
        statusEl.textContent =
            `${data.status} · ${data.processed || 0}/${data.total || total}` +
            ` · ${c.done || 0} done, ${c.skipped || 0} skipped, ${c.failed || 0} failed`;

        if (['completed', 'failed', 'cancelled'].includes(data.status)) {
            if (c.failed) {
                statusEl.innerHTML +=
                    `<div style="margin-top:4px; color:#ff6b6b;">${c.failed} function(s) failed — see job logs</div>`;
            }
            // Refresh the table so new notes and llm: tags show up.
            if (typeof refreshData === 'function' && getRoutingState().viewKey === 'functions') refreshData(false, true);
            setTimeout(() => card.remove(), 12000);
            return;
        }
        setTimeout(poll, 2000);
    };
    poll();
};

window.cancelLLMBatch = async function (jobId) {
    try {
        await tagPost(`/api/llm/batch/${jobId}/cancel`, {});
        showToast('LLM batch cancelled', 'info');
    } catch (e) {
        showToast(`Could not cancel: ${e.message}`, 'error');
    }
};
