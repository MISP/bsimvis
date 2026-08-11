// upload.js - Handles binary uploads to BSimVis

let selectedFiles = [];

function renderUploadView(params) {
    const container = document.getElementById('upload-view-container');
    const collection = params.get('collection') || '';
    
    // Hide search area for upload
    const searchArea = document.getElementById('search-area');
    if (searchArea) searchArea.style.display = 'none';

    container.innerHTML = `
        <div class="upload-container" style="max-width: 900px; margin: 0 auto; color: var(--text);">
            <div class="upload-header" style="margin-bottom: 30px; border-bottom: 1px solid var(--border); padding-bottom: 15px; display: flex; justify-content: space-between; align-items: flex-end;">
                <div>
                    <h2 style="color: var(--accent); margin: 0 0 5px 0; font-size: 1.5rem;">Upload Binaries</h2>
                    <p style="color: var(--subtle); font-size: 0.9rem; margin: 0;">Target Collection: <b style="color:var(--accent)">${collection}</b></p>
                </div>
                <div style="font-size: 0.8rem; color: var(--subtle);">
                    Using Ghidra analysis pipeline
                </div>
            </div>

            <div class="upload-grid" style="display: grid; grid-template-columns: 1fr 1.2fr; gap: 40px;">
                <div class="upload-settings-panel">
                    <div style="background: var(--hover); border: 1px solid var(--border); border-radius: 8px; padding: 20px;">
                        <h3 style="font-size: 0.9rem; text-transform: uppercase; color: var(--accent); margin: 0 0 20px 0; letter-spacing: 1px;">Pipeline Settings</h3>
                        
                        <div class="form-group" style="margin-bottom: 15px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Collection</label>
                            <select id="upload-collection" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem; cursor: pointer; margin-bottom: 8px;">
                                <option value="${collection}">${collection}</option>
                            </select>
                            <input type="text" id="upload-new-collection" placeholder="New Collection Name..." style="display: none; width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                        </div>

                        <div class="form-group" style="margin-bottom: 15px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Batch Name</label>
                            <input type="text" id="upload-batch-name" placeholder="e.g. Firmware v1.2" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                        </div>

                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px;">
                            <div class="form-group">
                                <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Analysis Profile</label>
                                <select id="upload-profile" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem; cursor: pointer;">
                                    <option value="fast">Fast</option>
                                    <option value="balanced" selected>Balanced</option>
                                    <option value="deep">Deep</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Min Func Len</label>
                                <input type="number" id="upload-min-func-len" value="0" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                            </div>
                        </div>

                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px;">
                            <div class="form-group">
                                <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Processor</label>
                                <div style="position: relative;">
                                    <input type="text" id="upload-processor-search" autocomplete="off" placeholder="Auto-detect — click to browse" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                                    <input type="hidden" id="upload-processor" value="">
                                    <div id="upload-processor-list" style="display: none; position: absolute; z-index: 50; top: 100%; left: 0; right: 0; max-height: 260px; overflow-y: auto; background: var(--window-tray); border: 1px solid var(--border); border-radius: 4px; margin-top: 2px; "></div>
                                </div>
                                <div id="upload-processor-hint" style="font-size: 0.7rem; color: var(--subtle); margin-top: 4px; min-height: 1em;"></div>
                            </div>
                            <div class="form-group">
                                <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Compiler Spec</label>
                                <select id="upload-cspec" disabled style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem; cursor: pointer;">
                                    <option value="">Default</option>
                                </select>
                                <div id="upload-cspec-hint" style="font-size: 0.7rem; color: var(--subtle); margin-top: 4px; min-height: 1em;"></div>
                            </div>
                        </div>

                        <div class="form-group" style="margin-bottom: 20px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Tags (Global)</label>
                            <input type="text" id="upload-tags" placeholder="Malware, Linux, MIPS..." style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                        </div>
                        <div class="form-group" style="margin-bottom: 20px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Archive Password</label>
                            <input type="text" id="upload-archive-password" value="infected" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                            <div style="font-size: 0.7rem; color: var(--subtle); margin-top: 4px;">Zip/tar uploads are unpacked and every member analyzed.</div>
                        </div>
                        <div class="form-group" style="margin-bottom: 20px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Related MD5s</label>
                            <input type="text" id="upload-related-md5" placeholder="Comma-separated MD5s" style="width: 100%; background: var(--window-tray); border: 1px solid var(--border); color: var(--text); padding: 8px; border-radius: 4px; font-size: 0.85rem;">
                        </div>
                        <div class="form-group" style="margin-bottom: 20px;">
                            <label style="display: block; font-size: 0.75rem; color: var(--subtle); margin-bottom: 6px;">Skip Analysis Modules</label>
                            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.85rem; margin-bottom: 6px; cursor: pointer;">
                                <input type="checkbox" id="upload-skip-functionid"> Skip FunctionID tagging (library ID, on by default)
                            </label>
                            <label style="display: flex; align-items: center; gap: 8px; font-size: 0.85rem; cursor: pointer;">
                                <input type="checkbox" id="upload-skip-capa"> Skip capa tagging
                            </label>
                        </div>

                        <div style="padding-top: 15px; border-top: 1px solid var(--border); display: flex; flex-direction: column; gap: 10px;">
                            <button id="start-upload-btn" onclick="startBatchUpload()" class="btn-primary" style="width: 100%; height: 40px; justify-content: center; display: flex; align-items: center; gap: 10px;">
                                <i class="fa-solid fa-play"></i> Start Analysis
                            </button>
                            <button onclick="clearUploadList()" class="top-action-btn danger-btn" style="width: 100%; height: 35px; justify-content: center;">
                                <i class="fa-solid fa-trash"></i> Clear List
                            </button>
                        </div>
                    </div>
                </div>

                <div class="upload-drop-panel" style="display: flex; flex-direction: column; gap: 20px;">
                    <div id="upload-drop-zone" style="border: 2px dashed var(--border); border-radius: 8px; padding: 50px 20px; text-align: center; cursor: pointer; transition: all 0.2s; background: var(--hover);">
                        <i class="fa-solid fa-cloud-arrow-up" style="font-size: 3.5rem; color: var(--accent); margin-bottom: 15px; opacity: 0.5;"></i>
                        <div style="font-weight: bold; font-size: 1.1rem; margin-bottom: 8px; color: var(--text);">Drop Binaries Here</div>
                        <div style="font-size: 0.85rem; color: var(--subtle);">or click to browse files</div>
                        <input type="file" id="upload-file-input" multiple style="display: none;">
                    </div>

                    <div id="upload-file-list-container" style="display: none; flex: 1; min-height: 0; flex-direction: column;">
                        <h4 style="font-size: 0.8rem; text-transform: uppercase; color: var(--subtle); margin: 0 0 10px 0; display: flex; justify-content: space-between;">
                            Selected Files <span id="file-count-badge" class="badge">0</span>
                        </h4>
                        <div id="upload-file-list" style="flex: 1; overflow-y: auto; background: var(--border); border: 1px solid var(--border); border-radius: 4px; padding: 5px;">
                        </div>
                    </div>
                </div>
            </div>

            <div id="upload-progress-container" style="margin-top: 40px; display: none; background: var(--border); border: 1px solid var(--border); border-radius: 8px; padding: 25px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                    <h3 style="color: var(--accent); font-size: 1rem; margin: 0; display: flex; align-items: center; gap: 10px;">
                        <div class="nav-job-spinner" id="global-upload-spinner" style="margin:0"></div>
                        Batch Upload Progress
                    </h3>
                    <div id="global-progress-text" style="font-size: 0.85rem; font-weight: bold; color: var(--accent);">0%</div>
                </div>
                <div class="job-progress-track" style="height: 10px; margin-bottom: 25px; background: var(--hover);">
                    <div id="global-progress-fill" class="job-progress-fill progress-running" style="width: 0%;"></div>
                </div>
                <div id="upload-progress-list" style="display: flex; flex-direction: column; gap: 12px; max-height: 400px; overflow-y: auto; padding-right: 10px;">
                </div>
            </div>
        </div>
    `;

    setupUploadEvents();
    updateFileList();
    populateUploadCollectionDropdown(collection);
    populateUploadLanguageDropdowns();
}

// ~170 languages, so the processor picker is a filterable combobox: the full
// list is visible on click and narrows as you type. Compiler specs are valid
// per-language, so the cspec <select> is rebuilt from the chosen processor.
let uploadLanguages = [];

async function populateUploadLanguageDropdowns() {
    const search = document.getElementById('upload-processor-search');
    const hidden = document.getElementById('upload-processor');
    if (!search || !hidden) return;

    try {
        const res = await fetch('/api/index/languages');
        if (res.ok) uploadLanguages = (await res.json()).languages || [];
    } catch (e) {
        console.warn('Failed to load Ghidra languages', e);
    }

    if (!uploadLanguages.length) {
        search.disabled = true;
        search.placeholder = 'Auto-detect';
        search.title = 'Ghidra install not reachable from the API';
        return;
    }

    search.placeholder = `Auto-detect \u2014 click to browse (${uploadLanguages.length})`;
    search.onfocus = () => renderProcessorOptions();
    search.oninput = () => {
        // Typing invalidates the previous pick until a row is chosen again.
        hidden.value = '';
        refreshUploadCspecs();
        renderProcessorOptions();
    };
    search.onkeydown = (e) => {
        if (e.key === 'Escape') document.getElementById('upload-processor-list').style.display = 'none';
    };

    // Clicking outside closes the panel; mousedown on a row fires first.
    document.addEventListener('click', (e) => {
        const panel = document.getElementById('upload-processor-list');
        if (!panel) return;
        if (e.target !== search && !panel.contains(e.target)) panel.style.display = 'none';
    });

    renderProcessorOptions();
}

function escapeHtmlAttr(s) {
    return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

function renderProcessorOptions() {
    const search = document.getElementById('upload-processor-search');
    const panel = document.getElementById('upload-processor-list');
    if (!search || !panel) return;

    const q = search.value.trim().toLowerCase();
    // Match id and description, so "arm" and "Intel" both find their languages.
    const matches = uploadLanguages.filter(
        l => !q || l.id.toLowerCase().includes(q) || (l.description || '').toLowerCase().includes(q)
    );

    panel.style.display = 'block';
    if (!matches.length) {
        panel.innerHTML = '<div style="padding: 8px; font-size: 0.8rem; color: var(--subtle);">No matching language</div>';
        return;
    }

    panel.innerHTML =
        '<div data-lang-id="" style="padding: 7px 9px; font-size: 0.8rem; color: var(--subtle); cursor: pointer; border-bottom: 1px solid var(--border);">Auto-detect</div>' +
        matches
            .map(
                l => `<div data-lang-id="${escapeHtmlAttr(l.id)}" style="padding: 7px 9px; font-size: 0.8rem; cursor: pointer; border-bottom: 1px solid var(--border);">
                        <div style="color: var(--text);">${escapeHtmlAttr(l.id)}</div>
                        <div style="color: var(--subtle); font-size: 0.7rem;">${escapeHtmlAttr(l.description || '')}</div>
                      </div>`
            )
            .join('');

    for (const row of panel.querySelectorAll('[data-lang-id]')) {
        row.onmouseenter = () => (row.style.background = 'var(--border)');
        row.onmouseleave = () => (row.style.background = 'transparent');
        row.onmousedown = () => {
            selectUploadProcessor(row.getAttribute('data-lang-id'));
            panel.style.display = 'none';
        };
    }
}

function selectUploadProcessor(langId) {
    const search = document.getElementById('upload-processor-search');
    const hidden = document.getElementById('upload-processor');
    hidden.value = langId || '';
    search.value = langId || '';
    refreshUploadCspecs();
}

function refreshUploadCspecs() {
    const hidden = document.getElementById('upload-processor');
    const cspecSelect = document.getElementById('upload-cspec');
    const procHint = document.getElementById('upload-processor-hint');
    const cspecHint = document.getElementById('upload-cspec-hint');
    if (!hidden || !cspecSelect) return;

    const lang = uploadLanguages.find(l => l.id === hidden.value);

    procHint.innerText = lang ? lang.description || '' : '';

    // Changing or clearing the processor invalidates the selected cspec.
    cspecSelect.innerHTML = '<option value="">Default</option>';
    cspecSelect.disabled = !lang;
    if (!lang) {
        cspecHint.innerText = '';
        return;
    }

    for (const c of lang.compilers) {
        const opt = document.createElement('option');
        opt.value = c.id;
        opt.innerText = c.name && c.name !== c.id ? `${c.id} (${c.name})` : c.id;
        cspecSelect.appendChild(opt);
    }
    cspecHint.innerText = lang.compilers.length
        ? `${lang.compilers.length} spec(s) available`
        : 'No compiler specs for this language';
}

function setupUploadEvents() {
    const dropZone = document.getElementById('upload-drop-zone');
    const fileInput = document.getElementById('upload-file-input');

    if (!dropZone || !fileInput) return;

    const collSelect = document.getElementById('upload-collection');
    const newCollInput = document.getElementById('upload-new-collection');
    if (collSelect && newCollInput) {
        collSelect.onchange = () => {
            if (collSelect.value === '__NEW__') {
                newCollInput.style.display = 'block';
                newCollInput.focus();
            } else {
                newCollInput.style.display = 'none';
            }
        };
    }

    dropZone.onclick = () => fileInput.click();

    fileInput.onchange = (e) => {
        handleFiles(e.target.files);
    };

    dropZone.ondragover = (e) => {
        e.preventDefault();
        dropZone.style.borderColor = 'var(--accent)';
        dropZone.style.background = 'rgba(4, 217, 255, 0.05)';
    };

    dropZone.ondragleave = () => {
        dropZone.style.borderColor = 'var(--border)';
        dropZone.style.background = 'var(--border)';
    };

    dropZone.ondrop = (e) => {
        e.preventDefault();
        dropZone.style.borderColor = 'var(--border)';
        dropZone.style.background = 'var(--border)';
        handleFiles(e.dataTransfer.files);
    };
}

function handleFiles(files) {
    for (let file of files) {
        // Prevent duplicates by name and size
        if (!selectedFiles.find(f => f.name === file.name && f.size === file.size)) {
            selectedFiles.push(file);
        }
    }
    updateFileList();
}

function removeFile(index) {
    selectedFiles.splice(index, 1);
    updateFileList();
}

function clearUploadList() {
    selectedFiles = [];
    updateFileList();
    const progContainer = document.getElementById('upload-progress-container');
    if (progContainer) {
        progContainer.style.display = 'none';
        // Remove go-to-collection footer so it doesn't persist into a new session
        const goBtn = document.getElementById('go-to-collection-btn');
        if (goBtn) goBtn.closest('div[style*="border-top"]')?.remove();
    }
    const startBtn = document.getElementById('start-upload-btn');
    if (startBtn) {
        startBtn.disabled = false;
        startBtn.innerHTML = '<i class="fa-solid fa-play"></i> Start Analysis';
    }
}

function updateFileList() {
    const list = document.getElementById('upload-file-list');
    const container = document.getElementById('upload-file-list-container');
    const badge = document.getElementById('file-count-badge');
    
    if (!list || !container) return;

    if (selectedFiles.length === 0) {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'flex';
    badge.innerText = selectedFiles.length;

    list.innerHTML = selectedFiles.map((file, i) => `
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; border-bottom: 1px solid var(--border); font-size: 0.8rem;">
            <div style="display: flex; align-items: center; gap: 10px; overflow: hidden;">
                <i class="fa-solid fa-file-binary" style="color: var(--subtle); flex-shrink: 0;"></i>
                <span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${file.name}">${file.name}</span>
                <span style="color: var(--dim); font-size: 0.7rem; flex-shrink: 0;">(${(file.size / 1024).toFixed(1)} KB)</span>
            </div>
            <button onclick="removeFile(${i})" style="background: none; border: none; color: #ff4d8d; cursor: pointer; padding: 5px; opacity: 0.6;" title="Remove">
                <i class="fa-solid fa-times"></i>
            </button>
        </div>
    `).join('');
}

async function startBatchUpload() {
    if (selectedFiles.length === 0) {
        if (typeof showToast === 'function') showToast('Please select some files first', 'warning');
        return;
    }

    let collection = document.getElementById('upload-collection').value || '';
    if (collection === '__NEW__') {
        collection = document.getElementById('upload-new-collection').value.trim();
    }
    if (!collection) {
        if (typeof showToast === 'function') showToast('Please select or enter a collection name', 'warning');
        return;
    }
    const batchName = document.getElementById('upload-batch-name').value || 'Manual Upload';
    const profile = document.getElementById('upload-profile').value;
    const minFuncLen = document.getElementById('upload-min-func-len').value;
    const processor = document.getElementById('upload-processor').value.trim();
    const cspec = document.getElementById('upload-cspec').value.trim();

    // The API validates the pair too; this just avoids a round-trip per file.
    const lang = uploadLanguages.find(l => l.id === processor);
    if (processor && !lang) {
        if (typeof showToast === 'function') showToast(`Unknown processor '${processor}'`, 'warning');
        return;
    }
    if (cspec && lang && !lang.compilers.some(c => c.id === cspec)) {
        if (typeof showToast === 'function') showToast(`'${cspec}' is not a valid compiler spec for ${processor}`, 'warning');
        return;
    }
    const tags = document.getElementById('upload-tags').value.split(',').map(t => t.trim()).filter(t => t);
    const relatedMd5s = document.getElementById('upload-related-md5').value.split(',').map(m => m.trim()).filter(m => m);
    const archivePassword = document.getElementById('upload-archive-password').value;
    const skipModules = [];
    if (document.getElementById('upload-skip-functionid').checked) skipModules.push('FunctionID');
    if (document.getElementById('upload-skip-capa').checked) skipModules.push('capa');

    let currentBatchUuid = null;

    document.getElementById('upload-progress-container').style.display = 'block';
    document.getElementById('start-upload-btn').disabled = true;
    document.getElementById('start-upload-btn').innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Uploading...';
    
    const progressList = document.getElementById('upload-progress-list');
    progressList.innerHTML = '';

    const results = [];
    let completedCount = 0;

    for (let i = 0; i < selectedFiles.length; i++) {
        const file = selectedFiles[i];
        const itemEl = document.createElement('div');
        itemEl.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;">
                <div style="font-size: 0.8rem; font-weight: bold; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; padding-right: 15px;">${file.name}</div>
                <div id="file-status-${i}" style="font-size: 0.7rem; font-weight: bold; color: var(--subtle); flex-shrink: 0;">PREPARING</div>
            </div>
            <div class="job-progress-track" style="height: 4px; background: var(--hover);">
                <div id="file-progress-${i}" class="job-progress-fill" style="width: 0%;"></div>
            </div>
        `;
        progressList.appendChild(itemEl);

        try {
            const statusEl = document.getElementById(`file-status-${i}`);
            const progressEl = document.getElementById(`file-progress-${i}`);
            
            statusEl.innerText = 'UPLOADING';
            statusEl.style.color = 'var(--accent)';
            
            const url = new URL('/api/file/upload', window.location.origin);
            url.searchParams.set('collection', collection);
            url.searchParams.set('file_name', file.name);
            url.searchParams.set('enqueue', 'false');
            if (currentBatchUuid) {
                url.searchParams.set('batch_uuid', currentBatchUuid);
            }
            url.searchParams.set('batch_name', batchName);
            url.searchParams.set('profile', profile);
            url.searchParams.set('min_func_len', minFuncLen);
            if (processor) url.searchParams.set('processor', processor);
            if (processor && cspec) url.searchParams.set('cspec', cspec);
            if (archivePassword) url.searchParams.set('archive_password', archivePassword);
            tags.forEach(t => url.searchParams.append('tags', t));
            relatedMd5s.forEach(m => url.searchParams.append('related_md5', m));
            skipModules.forEach(m => url.searchParams.append('skip', m));

            const response = await fetch(url, {
                method: 'POST',
                body: file
            });

            if (response.ok) {
                const data = await response.json();
                if (!currentBatchUuid && data.batch_uuid) {
                    currentBatchUuid = data.batch_uuid;
                }
                statusEl.innerText = data.file_count > 1 ? `QUEUED (${data.file_count} in archive)` : 'QUEUED';
                statusEl.style.color = 'var(--success)';
                progressEl.style.width = '100%';
                results.push(data);
            } else {
                const error = await response.json();
                statusEl.innerText = 'FAILED';
                statusEl.style.color = '#ff4d8d';
                console.error(`Upload failed for ${file.name}:`, error);
            }
        } catch (err) {
            console.error(`Error uploading ${file.name}:`, err);
            document.getElementById(`file-status-${i}`).innerText = 'ERROR';
            document.getElementById(`file-status-${i}`).style.color = '#ff4d8d';
        }

        completedCount++;
        const totalProgress = Math.round((completedCount / selectedFiles.length) * 100);
        document.getElementById('global-progress-fill').style.width = `${totalProgress}%`;
        document.getElementById('global-progress-text').innerText = `${totalProgress}%`;
    }

    if (results.length > 0) {
        try {
            document.getElementById('global-progress-text').innerText = 'Finalizing Batch...';
            // An archive upload answers with one pipeline per extracted member.
            const pipelineIds = results.flatMap(r => r.pipeline_ids || [r.pipeline_id]);
            const finalizeRes = await fetch('/api/file/upload/batch_finalize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    pipeline_ids: pipelineIds,
                    batch_uuid: currentBatchUuid,
                    collection: collection,
                    algo: 'unweighted_cosine' // Default or grab from UI if available
                })
            });

            if (finalizeRes.ok) {
                if (typeof showToast === 'function') showToast(`Successfully queued master pipeline for ${pipelineIds.length} binaries`, 'success');
            } else {
                console.error("Failed to finalize batch", await finalizeRes.text());
                if (typeof showToast === 'function') showToast('Binaries uploaded, but master pipeline orchestration failed.', 'warning');
            }
        } catch (e) {
            console.error(e);
        }
    }

    const globalSpinner = document.getElementById('global-upload-spinner');
    if (globalSpinner) globalSpinner.style.display = 'none';
    
    document.getElementById('start-upload-btn').innerHTML = '<i class="fa-solid fa-check"></i> Finished';

    // Update the URL and Navbar to the new collection context
    const uploadUrl = `/collections/${encodeURIComponent(collection)}/upload`;
    history.pushState(null, '', uploadUrl);
    if (typeof updateNavbarLinks === 'function') {
        updateNavbarLinks(collection);
    }
    // Update breadcrumbs
    if (window.Breadcrumbs && typeof getRoutingState === 'function' && typeof routes !== 'undefined') {
        const routingState = getRoutingState();
        const segments = window.Breadcrumbs.generate(routingState, routes['upload']);
        window.Breadcrumbs.render(segments);
    }
    // Sync the context data attribute so refreshData doesn't re-render
    const uploadView = document.getElementById('upload-view-container');
    if (uploadView) uploadView.dataset.context = collection;

    // Show "Go to Collection" button once upload is done
    const collectionUrl = `/collections/${encodeURIComponent(collection)}`;
    const progressContainer = document.getElementById('upload-progress-container');
    if (progressContainer && !document.getElementById('go-to-collection-btn')) {
        const goBtn = document.createElement('div');
        goBtn.style.cssText = 'margin-top: 20px; padding-top: 15px; border-top: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between;';
        goBtn.innerHTML = `
            <span style="font-size: 0.8rem; color: var(--subtle);">
                <i class="fa-solid fa-layer-group" style="margin-right: 6px;"></i>
                Uploaded to <b style="color: var(--text);">${escapeHtml(collection)}</b>
            </span>
            <button id="go-to-collection-btn" onclick="Nav.openPath(${escapeAttr(jsString(collectionUrl))})" class="btn-primary" style="height: 34px; padding: 0 16px; font-size: 0.8rem; display: flex; align-items: center; gap: 8px;">
                <i class="fa-solid fa-arrow-right"></i> Go to Collection
            </button>
        `;
        progressContainer.appendChild(goBtn);
    }
}

async function populateUploadCollectionDropdown(currentCollection) {
    try {
        const res = await fetch('/api/collection/search?limit=10000'); // ponytail: lift limit to get all collections
        if (!res.ok) return;
        const data = await res.json();
        const collections = data.collections || (Array.isArray(data) ? data : []);
        
        const select = document.getElementById('upload-collection');
        if (!select) return;
        
        select.innerHTML = '';
        let foundCurrent = false;
        
        collections.forEach(c => {
            const opt = document.createElement('option');
            opt.value = c.name;
            opt.textContent = c.name;
            if (c.name === currentCollection) {
                opt.selected = true;
                foundCurrent = true;
            }
            select.appendChild(opt);
        });
        
        if (!foundCurrent && currentCollection) {
            const opt = document.createElement('option');
            opt.value = currentCollection;
            opt.textContent = currentCollection;
            opt.selected = true;
            select.insertBefore(opt, select.firstChild);
        }
        
        const newOpt = document.createElement('option');
        newOpt.value = '__NEW__';
        newOpt.textContent = '+ Create New Collection...';
        select.insertBefore(newOpt, select.firstChild);
    } catch (e) {
        console.error("Failed to populate upload collection dropdown", e);
    }
}
