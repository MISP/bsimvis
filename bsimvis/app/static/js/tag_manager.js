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
                <input type="checkbox" ${t.llm ? 'checked' : ''} title="Include this tag in the AI tagging vocabulary"
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
        showToast(`'${tag}' ${enabled ? 'added to' : 'removed from'} the AI vocabulary`, 'success');
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
                <input id="new-tag-llm" type="checkbox" checked> AI vocabulary
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

// --- Analyze (AI) -----------------------------------------------------------
// One modal for every scope. `scope` picks the endpoint and the few fields that
// only make sense for a comparison; everything else is shared, so a file, a
// comparison and a whole collection are the same form with a different chip on
// top. Engine knobs live behind Advanced -- nobody should meet a match
// threshold on their first run.

const ANALYZE_SCOPES = {
    file: { endpoint: '/api/llm/file_analysis', icon: 'fa-file-waveform', noun: 'file' },
    collection: { endpoint: '/api/llm/file_analysis', icon: 'fa-layer-group', noun: 'collection' },
    pair: { endpoint: '/api/llm/pair_analysis', icon: 'fa-code-compare', noun: 'comparison' }
};

// Presets are intents, not knob bundles. For a comparison the intent that
// matters is which half of the diff to read: two builds of the same malware
// have nothing unique and no match under the threshold, so a changed-code pass
// selects nothing at all -- on those pairs the shared code is the whole story.
const ANALYZE_PRESETS = {
    quick: { min_complexity: 10, max_functions: 50, include_unique: true, include_unchanged: false },
    shared: { min_complexity: 10, max_functions: 50, include_unique: false, include_unchanged: true },
    full: { min_complexity: 0, max_functions: 0, include_unique: true, include_unchanged: true }
};

/** Presets offered for a scope, in display order, with what each would select. */
function analyzePresetChoices(scope, counts) {
    if (scope !== 'pair') {
        return [
            { key: 'quick', title: 'Quick triage', note: 'Skips trivial functions' },
            { key: 'full', title: 'Full pass', note: 'Every candidate the scope selects' }
        ];
    }
    const unique = (counts.unique_to_a || 0) + (counts.unique_to_b || 0);
    const matched = counts.matched || 0;
    return [
        {
            key: 'quick', title: 'What changed',
            note: `${unique.toLocaleString()} unique + matches below the threshold`
        },
        {
            key: 'shared', title: 'What they share',
            note: `${matched.toLocaleString()} matched function${matched === 1 ? '' : 's'}`
        },
        { key: 'full', title: 'Everything', note: 'Both halves, no cap' }
    ];
}

/** No unique functions means the two binaries are near-identical, and the
 *  changed-code pass has nothing to work with. Open on the pass that does. */
function defaultAnalyzePreset(scope, counts) {
    if (scope !== 'pair') return 'quick';
    const unique = (counts.unique_to_a || 0) + (counts.unique_to_b || 0);
    return unique === 0 ? 'shared' : 'quick';
}

window.openAnalyzeModal = function (opts = {}) {
    const scope = opts.scope || (opts.fileMd5 ? 'file' : 'collection');
    const meta = ANALYZE_SCOPES[scope];
    if (!meta) return;

    const pair = opts.pair ? { ...opts.pair } : null;
    if (scope === 'pair' && !pair) {
        showToast('No comparison loaded', 'warning');
        return;
    }

    const collection = opts.collection || (pair && pair.collection) || getRoutingState().collection;
    if (!collection) {
        showToast('No collection selected', 'warning');
        return;
    }

    let modal = document.getElementById('analyze-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'analyze-modal';
        modal.style.cssText = 'position:fixed; inset:0; z-index:30000; display:flex; align-items:center; justify-content:center; background:rgba(0,0,0,.65); backdrop-filter:blur(4px);';
        document.body.appendChild(modal);
    }
    modal.dataset.scope = scope;
    modal.dataset.collection = collection;
    modal.dataset.fileMd5 = opts.fileMd5 || '';
    modal._pair = pair;

    const target = scope === 'pair'
        ? `${pair.md5a} vs ${pair.md5b}`
        : (opts.fileMd5 ? `file ${opts.fileMd5}` : `collection ${collection}`);
    const isPair = scope === 'pair';
    const counts = (pair && pair.counts) || {};
    const choices = analyzePresetChoices(scope, counts);
    const chosen = defaultAnalyzePreset(scope, counts);
    const preset = ANALYZE_PRESETS[chosen];

    modal.innerHTML = `
        <form onsubmit="submitAnalyze(event)" style="width:540px; max-width:92vw; background:var(--card-bg); border:1px solid var(--border); border-radius:10px; padding:22px; color:var(--fg);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                <h3 style="margin:0; color:#ae81ff;"><i class="fa-solid fa-robot"></i> Analyze ${escapeHtml(meta.noun)}</h3>
                <button type="button" onclick="closeAnalyzeModal()" style="background:none; border:0; color:var(--subtle); cursor:pointer; font-size:1.3rem;">&times;</button>
            </div>
            <div style="font-size:.75rem; color:var(--subtle); margin-bottom:16px; word-break:break-all;">
                <i class="fa-solid ${escapeAttr(meta.icon)}"></i> ${escapeHtml(target)}
            </div>

            <div class="analyze-presets">
                ${choices.map(choice => `
                <label class="analyze-preset"><input type="radio" name="analyze-preset" value="${escapeAttr(choice.key)}"
                    ${choice.key === chosen ? 'checked' : ''}
                    onchange="applyAnalyzePreset('${escapeAttr(choice.key)}')"> <b>${escapeHtml(choice.title)}</b>
                    <small>${escapeHtml(choice.note)}</small></label>`).join('')}
            </div>

            <label style="display:block; margin-bottom:14px;">Prompt <span style="color:var(--subtle); font-weight:normal;">(optional)</span>
                <textarea id="analyze-prompt" placeholder="Analyst focus. Leave empty for the configured default" style="display:block; width:100%; min-height:80px; box-sizing:border-box; margin-top:5px; padding:8px; resize:vertical; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;"></textarea>
            </label>

            <div style="display:grid; grid-template-columns:1fr 1fr; gap:9px; margin-bottom:14px; font-size:.84rem;">
                <label><input id="analyze-notes" type="checkbox" checked> Write notes</label>
                <label><input id="analyze-tags" type="checkbox" checked> Write tags${isPair ? ' + refresh split' : ''}</label>
            </div>

            <details style="margin-bottom:16px;">
                <summary style="cursor:pointer; font-size:.8rem; color:var(--subtle);">Advanced</summary>
                <div style="display:grid; grid-template-columns:${isPair ? '1fr 1fr 1fr' : '1fr'}; gap:12px; margin:12px 0;">
                    <label style="font-size:.8rem;">Minimum BSim features
                        <input id="analyze-min" type="number" min="0" value="${preset.min_complexity}" style="display:block; width:100%; box-sizing:border-box; margin-top:5px; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;">
                    </label>
                    ${isPair ? `
                    <label style="font-size:.8rem;">Changed-match threshold
                        <input id="analyze-threshold" type="number" min="0" max="1" step="0.01" value="0.90" style="display:block; width:100%; box-sizing:border-box; margin-top:5px; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;">
                    </label>
                    <label style="font-size:.8rem;">Maximum functions
                        <input id="analyze-max" type="number" min="0" value="${preset.max_functions}" title="0 = every diff-selected candidate. A number takes a complexity-ranked subset." style="display:block; width:100%; box-sizing:border-box; margin-top:5px; padding:8px; background:var(--bg); color:var(--fg); border:1px solid var(--border); border-radius:4px;">
                    </label>` : ''}
                </div>
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:9px; font-size:.82rem;">
                    <label><input id="analyze-skip-fid" type="checkbox" checked> Skip FID-tagged functions</label>
                    <label><input id="analyze-overwrite" type="checkbox"> Replace existing AI output</label>
                    ${isPair ? `
                    <label><input id="analyze-unique" type="checkbox" ${preset.include_unique ? 'checked' : ''}> Analyze unique functions</label>
                    <label title="Slower: also sends high-similarity matches"><input id="analyze-unchanged" type="checkbox" ${preset.include_unchanged ? 'checked' : ''}> Include unchanged matches</label>` : ''}
                </div>
            </details>

            ${isPair ? `<div style="font-size:.72rem; color:var(--subtle); margin-bottom:16px;">Unique and low-similarity functions are triage candidates, not evidence of maliciousness.</div>` : ''}

            <div style="display:flex; justify-content:flex-end; gap:10px;">
                <button type="button" onclick="closeAnalyzeModal()" class="top-action-btn">Cancel</button>
                <button type="submit" class="top-action-btn" style="color:#ae81ff; border-color:#ae81ff;"><i class="fa-solid fa-play"></i> Analyze</button>
            </div>
        </form>`;
    modal.onclick = e => { if (e.target === modal) closeAnalyzeModal(); };
};

/** Presets only move the Advanced fields, so opening Advanced always shows what
 *  the chosen preset actually does rather than a stale default. */
window.applyAnalyzePreset = function (name) {
    const preset = ANALYZE_PRESETS[name];
    if (!preset) return;
    const set = (id, value) => { const el = document.getElementById(id); if (el) el.value = value; };
    const check = (id, value) => { const el = document.getElementById(id); if (el) el.checked = value; };
    set('analyze-min', preset.min_complexity);
    set('analyze-max', preset.max_functions);
    check('analyze-unique', preset.include_unique);
    check('analyze-unchanged', preset.include_unchanged);
};

window.closeAnalyzeModal = function () {
    document.getElementById('analyze-modal')?.remove();
};

window.submitAnalyze = async function (event) {
    event.preventDefault();
    const modal = document.getElementById('analyze-modal');
    const scope = modal.dataset.scope;
    const meta = ANALYZE_SCOPES[scope];
    // A field only exists for the scopes that use it, so every read carries the
    // value the endpoint would have defaulted to anyway.
    const num = (id, fallback) => {
        const el = document.getElementById(id);
        return el ? Number(el.value) : fallback;
    };
    const on = (id, fallback) => {
        const el = document.getElementById(id);
        return el ? el.checked : fallback;
    };

    const actions = [];
    if (on('analyze-notes', true)) actions.push('notes');
    if (on('analyze-tags', true)) actions.push('tags');
    if (!actions.length) {
        showToast('Select notes, tags, or both', 'warning');
        return;
    }

    const body = {
        collection: modal.dataset.collection,
        actions,
        min_complexity: num('analyze-min', 0),
        skip_fid_tagged: on('analyze-skip-fid', true),
        overwrite: on('analyze-overwrite', false)
    };
    const prompt = document.getElementById('analyze-prompt').value.trim();
    if (prompt) body.custom_prompt = prompt;

    const pair = modal._pair;
    if (scope === 'pair') {
        Object.assign(body, {
            collection: pair.collection,
            coll_b: pair.collB,
            md5_a: pair.md5a,
            md5_b: pair.md5b,
            pool: pair.poolId || undefined,
            threshold: num('analyze-threshold', 0.9),
            max_functions: num('analyze-max', 0),
            include_unique: on('analyze-unique', true),
            include_unchanged: on('analyze-unchanged', false)
        });
    } else if (modal.dataset.fileMd5) {
        body.file_md5 = modal.dataset.fileMd5;
    }

    try {
        const result = await tagPost(meta.endpoint, body);
        closeAnalyzeModal();
        if (scope === 'pair') {
            showToast(`Analysis queued for ${result.total} candidate function(s)`, 'success');
            trackPairAnalysis(result.job_id, pair);
        } else {
            showToast(`Analysis started for ${result.files} file(s), ${result.total} function(s)`, 'success');
            const warnings = result.warnings || (result.warning ? [result.warning] : []);
            warnings.forEach(w => showToast(w, 'warning'));
            trackFileAnalysis(result.job_id);
        }
    } catch (e) {
        showToast(`Could not start analysis: ${e.message}`, 'error');
    }
};

// Call sites that predate the merge.
window.openFileAnalysisModal = function (opts = {}) {
    openAnalyzeModal({ ...opts, scope: opts.fileMd5 ? 'file' : 'collection' });
};
window.closeFileAnalysisModal = window.closeAnalyzeModal;

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

    const unsubscribe = window.JobStatusStore.subscribe({ jobId }, (evt) => {
        const data = evt.data;
        if (!data) return;
        statusEl.textContent = `${data.status || 'queued'} · ${data.progress || 0}%`;
        if (evt.type === 'job:completed' || evt.type === 'job:failed') {
            if (typeof refreshData === 'function') refreshData(false, true);
            setTimeout(() => card.remove(), 12000);
            unsubscribe();
        }
    });
};

window.cancelFileAnalysis = async function (jobId) {
    try {
        await tagPost(`/api/jobs/${jobId}/cancel`, {});
        showToast('File analysis cancelled', 'info');
    } catch (e) {
        showToast(`Could not cancel analysis: ${e.message}`, 'error');
    }
};
