// Universal Diff Queue Manager for BSimVis
let diffSelection = [];
let fileDiffSelection = [];
let diffPreviewTimer = null;
let activeDiffKey = null;
const diffPreviewCache = new Map();

function normalizeFuncId(id) {
    if (!id || typeof id !== 'string') return id;
    if (id.includes(':function:') || id.includes(':func:')) return id;
    const parts = id.split(':');
    if (parts.length >= 4) {
        const addrPart = parts.pop();
        const funcPart = parts.pop();
        const emptyPart = parts.pop();
        const md5Part = parts.pop();
        const colPart = parts.join(':');
        if (addrPart && addrPart.startsWith('@') && md5Part && md5Part.startsWith('#')) {
            const cleanAddr = addrPart.substring(1);
            const cleanMd5 = md5Part.substring(1);
            return `${colPart}:func:${cleanMd5}:${cleanAddr}`;
        }
    }
    return id;
}

function getParentEvent(e) {
    if (window.parent && window.parent !== window && window.frameElement) {
        const rect = window.frameElement.getBoundingClientRect();
        return {
            clientX: e.clientX + rect.left,
            clientY: e.clientY + rect.top,
            relatedTarget: e.relatedTarget
        };
    }
    return e;
}

function saveDiffQueue() {
    try {
        localStorage.setItem('bsim_diff_queue', JSON.stringify(diffSelection));
    } catch(e) {}
}

function loadDiffQueue() {
    try {
        const stored = localStorage.getItem('bsim_diff_queue');
        if (stored) {
            diffSelection = JSON.parse(stored) || [];
            updateDiffQueueUI();
        }
        
        const storedFile = localStorage.getItem('bsim_file_diff_queue');
        if (storedFile) {
            fileDiffSelection = JSON.parse(storedFile) || [];
            updateFileDiffQueueUI();
        }
    } catch(e) {}
}

function saveFileDiffQueue() {
    try {
        localStorage.setItem('bsim_file_diff_queue', JSON.stringify(fileDiffSelection));
    } catch(e) {}
}

function addToDiff(a1, a2) {
    if (window.parent && window.parent !== window && typeof window.parent.addToDiff === 'function') {
        window.parent.addToDiff(a1, a2);
        return;
    }
    const id = normalizeFuncId(a1);
    const name = a2 || id.split(':').pop();

    const existing = diffSelection.findIndex(item => normalizeFuncId(item.id) === id);
    if (existing !== -1) {
        diffSelection.splice(existing, 1);
    } else {
        diffSelection.push({ id, name });
    }

    if (diffSelection.length > 2) {
        diffSelection.shift();
    }

    updateDiffQueueUI();
    saveDiffQueue();

    if (diffSelection.length === 2) {
        if (typeof window.showDiffPanel === 'function') {
            window.showDiffPanel();
        } else if (typeof openStandaloneDiff === 'function') {
            openStandaloneDiff();
        }
    }
}

function clearDiffSelection() {
    if (window.parent && window.parent !== window && typeof window.parent.clearDiffSelection === 'function') {
        window.parent.clearDiffSelection();
        return;
    }
    diffSelection = [];
    updateDiffQueueUI();
    saveDiffQueue();
}

function addToFileDiff(id, name) {
    if (window.parent && window.parent !== window && typeof window.parent.addToFileDiff === 'function') {
        window.parent.addToFileDiff(id, name);
        return;
    }

    const existing = fileDiffSelection.findIndex(item => item.id === id);
    if (existing !== -1) {
        fileDiffSelection.splice(existing, 1);
    } else {
        fileDiffSelection.push({ id, name });
    }

    if (fileDiffSelection.length > 2) {
        fileDiffSelection.shift();
    }

    updateFileDiffQueueUI();
    saveFileDiffQueue();

    if (fileDiffSelection.length === 2) {
        const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
        const collection = params.get('collection') || 'main';
        const md5a = fileDiffSelection[0].id.split(':').pop();
        const md5b = fileDiffSelection[1].id.split(':').pop();
        
        fileDiffSelection = [];
        updateFileDiffQueueUI();
        saveFileDiffQueue();
        
        window.location.hash = `#binary-similarity?collection=${collection}&md5_a=${md5a}&md5_b=${md5b}`;
    }
}

function clearFileDiffSelection() {
    if (window.parent && window.parent !== window && typeof window.parent.clearFileDiffSelection === 'function') {
        window.parent.clearFileDiffSelection();
        return;
    }
    fileDiffSelection = [];
    updateFileDiffQueueUI();
    saveFileDiffQueue();
}

function updateDiffQueueUI() {
    if (window.parent && window.parent !== window && typeof window.parent.updateDiffQueueUI === 'function') {
        // If we are in an iframe, let parent manage the state, but sync our own buttons
    }

    const queue = window.parent && window.parent !== window ? 
        JSON.parse(localStorage.getItem('bsim_diff_queue') || '[]') : diffSelection;

    // 1. Universal Diff Queue Status Card (Dashboard + Sub-views)
    document.querySelectorAll('.diff-queue-status, #diff-queue-status').forEach(status => {
        if (queue.length === 0) {
            status.innerHTML = '';
        } else if (queue.length === 1) {
            status.innerHTML = `
                <span class="badge diff-queue-badge" style="background:#fd971f; color:#000; display:flex; align-items:center; gap:8px; font-weight:bold; box-shadow:0 0 8px rgba(253,151,31,0.4);">
                    <span>±</span> 1/2 Selected: ${queue[0].name}
                    <button onclick="clearDiffSelection()" style="background:none; border:none; cursor:pointer; color:#000; font-weight:bold; font-size:1.1rem; padding:0; line-height:1;" title="Clear Diff Selection">&times;</button>
                </span>`;
        } else {
            const compareBtnHtml = window.parent === window ? `<button onclick="openStandaloneDiff()" style="background:#000; color:var(--success); border:1px solid var(--success); padding:2px 8px; border-radius:4px; font-weight:bold; cursor:pointer; font-size:0.75rem;">Compare ↗</button>` : '';
            status.innerHTML = `
                <span class="badge diff-queue-badge" style="background:var(--success); color:#000; display:flex; align-items:center; gap:8px; font-weight:bold; box-shadow:0 0 8px rgba(166,226,46,0.4);">
                    <span>±</span> 2/2 Ready: ${queue[0].name} vs ${queue[1].name}
                    ${compareBtnHtml}
                    <button onclick="clearDiffSelection()" style="background:none; border:none; cursor:pointer; color:#000; font-weight:bold; font-size:1.1rem; padding:0; line-height:1;" title="Clear Diff Selection">&times;</button>
                </span>`;
        }
    });

    // 2. Standalone Bottom Bar UI (Fallback if any legacy bar remains)
    const standaloneStatus = document.getElementById('standalone-diff-status');
    const standaloneBtn = document.getElementById('standalone-diff-btn');

    if (standaloneStatus) {
        if (queue.length === 0) {
            standaloneStatus.innerHTML = '<span class="dim">Diff Queue is empty. Select functions to compare.</span>';
            if (standaloneBtn) standaloneBtn.style.display = 'none';
        } else if (queue.length === 1) {
            standaloneStatus.innerHTML = `
                <span style="color:#fd971f; display:flex; align-items:center; gap:8px; font-weight:bold;">
                    <span>±</span> 1/2 Selected: <b>${queue[0].name}</b>
                    <button onclick="clearDiffSelection()" style="background:none; border:none; cursor:pointer; color:#fd971f; font-weight:bold; font-size:1.2rem; padding:0; line-height:1; margin-left:4px;" title="Clear Diff Selection">&times;</button>
                </span>`;
            if (standaloneBtn) standaloneBtn.style.display = 'none';
        } else {
            standaloneStatus.innerHTML = `
                <span style="color:var(--success); display:flex; align-items:center; gap:8px; font-weight:bold;">
                    <span>±</span> 2/2 Ready: <b>${queue[0].name}</b> vs <b>${queue[1].name}</b>
                    <button onclick="clearDiffSelection()" style="background:none; border:none; cursor:pointer; color:var(--success); font-weight:bold; font-size:1.2rem; padding:0; line-height:1; margin-left:4px;" title="Clear Diff Selection">&times;</button>
                </span>`;
            if (standaloneBtn) {
                standaloneBtn.style.display = 'block';
                standaloneBtn.style.background = '#fd971f';
                standaloneBtn.style.color = '#000';
            }
        }
    }

    // 3. Sync all .btn-diff-action buttons on the current page
    document.querySelectorAll('.btn-diff-action[data-func-id]').forEach(btn => {
        const id = normalizeFuncId(btn.dataset.funcId);
        const inQueue = queue.some(item => normalizeFuncId(item.id) === id);
        if (inQueue) {
            btn.classList.add('active');
            if (btn.dataset.fullText) {
                btn.innerHTML = '<span>±</span> In Diff Queue';
            }
        } else {
            btn.classList.remove('active');
            if (btn.dataset.fullText) {
                btn.innerHTML = '<span>±</span> Add to Diff';
            }
        }
    });

    // 4. If in parent window, broadcast to child iframes
    if (window.parent === window) {
        ['code-frame', 'feature-frame', 'global-feature-frame', 'diff-frame'].forEach(frameId => {
            const frame = document.getElementById(frameId);
            if (frame && frame.contentWindow && typeof frame.contentWindow.updateDiffQueueUI === 'function') {
                try { frame.contentWindow.updateDiffQueueUI(); } catch (e) {}
            }
        });
    }
}

function updateFileDiffQueueUI() {
    if (window.parent && window.parent !== window && typeof window.parent.updateFileDiffQueueUI === 'function') {
        // let parent manage
    }

    const queue = window.parent && window.parent !== window ? 
        JSON.parse(localStorage.getItem('bsim_file_diff_queue') || '[]') : fileDiffSelection;

    // File Diff Queue Status Card
    document.querySelectorAll('#file-diff-queue-status').forEach(status => {
        if (queue.length === 0) {
            status.innerHTML = '';
        } else if (queue.length === 1) {
            status.innerHTML = `
                <span class="badge diff-queue-badge" style="background:#fd971f; color:#000; display:flex; align-items:center; gap:8px; font-weight:bold; box-shadow:0 0 8px rgba(253,151,31,0.4);">
                    <i class="fa-solid fa-file-code"></i> 1/2 Selected: ${queue[0].name}
                    <button onclick="clearFileDiffSelection()" style="background:none; border:none; cursor:pointer; color:#000; font-weight:bold; font-size:1.1rem; padding:0; line-height:1;" title="Clear File Diff Selection">&times;</button>
                </span>`;
        }
    });

    // Sync file-diff buttons
    document.querySelectorAll('.btn-file-diff-action[data-file-id]').forEach(btn => {
        const id = btn.dataset.fileId;
        const inQueue = queue.some(item => item.id === id);
        if (inQueue) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
}

function openStandaloneDiff() {
    try {
        const queue = JSON.parse(localStorage.getItem('bsim_diff_queue') || '[]');
        if (queue.length < 2) return;
        window.open(`/diff/index.html?id1=${encodeURIComponent(queue[0].id)}&id2=${encodeURIComponent(queue[1].id)}`, '_blank');
        clearDiffSelection();
    } catch(e) {}
}

function getDiffPreviewTooltip() {
    let tooltip = document.getElementById('diff-preview-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.id = 'diff-preview-tooltip';
        tooltip.className = 'diff-preview-tooltip';
        document.body.appendChild(tooltip);
    }
    return tooltip;
}

function moveDiffPreview(e) {
    if (!e) return;
    if (window.parent && window.parent !== window && typeof window.parent.moveDiffPreview === 'function') {
        window.parent.moveDiffPreview(getParentEvent(e));
        return;
    }
    if (typeof window.moveCodePreview === 'function') {
        window.moveCodePreview(e);
        return;
    }
    const el = document.getElementById('diff-preview-tooltip');
    if (el && (el.style.display === 'block' || el.style.display === 'flex' || el.classList.contains('showing'))) {
        const offset = 15;
        let x = e.clientX + offset;
        let y = e.clientY + offset;
        const rect = el.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - offset;
        if (y + rect.height > window.innerHeight) y = e.clientY - rect.height - offset;
        el.style.left = x + 'px';
        el.style.top = y + 'px';
    }
}

function hideDiffPreview(e) {
    if (window.parent && window.parent !== window && typeof window.parent.hideDiffPreview === 'function') {
        window.parent.hideDiffPreview(getParentEvent(e));
        return;
    }
    if (window.hideAllTooltips) {
        // We only want to hide if we aren't moving into the tooltip itself
        const tooltip = document.getElementById('diff-preview-tooltip');
        if (e && e.relatedTarget && tooltip && (tooltip.contains(e.relatedTarget) || e.relatedTarget === tooltip)) return;
        window.hideAllTooltips();
    } else {
        const tooltip = document.getElementById('diff-preview-tooltip');
        if (e && e.relatedTarget && tooltip && (tooltip.contains(e.relatedTarget) || e.relatedTarget === tooltip)) return;
        if (tooltip) {
            tooltip.style.display = 'none';
            tooltip.classList.remove('showing');
            activeDiffKey = null;
        }
    }
}

function onHoverDiffButton(e, id, name, partnerId = null, score = -1) {
    if (window.parent && window.parent !== window && typeof window.parent.onHoverDiffButton === 'function') {
        window.parent.onHoverDiffButton(getParentEvent(e), id, name, partnerId, score);
        return;
    }
    const queue = diffSelection;
    if (queue.length === 1) {
        const s1 = queue[0];
        const normalizedId = normalizeFuncId(id);
        if (normalizeFuncId(s1.id) !== normalizedId) {
            let finalScore = score;
            if (partnerId && normalizeFuncId(partnerId) !== normalizeFuncId(s1.id)) {
                finalScore = -1; // partner doesn't match s1, need to fetch similarity
            }
            showDiffPreview(s1.id, s1.name, normalizedId, name, finalScore, e);
        }
    }
}

async function showDiffPreview(id1, name1, id2, name2, score, e, extra = 0) {
    if (window.parent && window.parent !== window && typeof window.parent.showDiffPreview === 'function') {
        window.parent.showDiffPreview(id1, name1, id2, name2, score, getParentEvent(e), extra);
        return;
    }
    if (window.setTrigger) window.setTrigger(e);
    const cacheKey = `${id1}::${id2}`;
    const tooltip = getDiffPreviewTooltip();
    
    // Update active key and immediately move
    if (cacheKey === activeDiffKey) {
        moveDiffPreview(e);
        return;
    }
    activeDiffKey = cacheKey;
    
    tooltip.style.display = 'block';
    tooltip.classList.add('showing');
    moveDiffPreview(e);

    if (diffPreviewTimer) clearTimeout(diffPreviewTimer);

    // If we have cached data, render immediately
    if (diffPreviewCache.has(cacheKey)) {
        renderDiffPreview(diffPreviewCache.get(cacheKey), name1, name2, score, extra);
        return;
    }

    // Otherwise, render a loading state for the right side but keep the list responsive
    renderDiffPreview(null, name1, name2, score, extra);

    diffPreviewTimer = setTimeout(async () => {
        let finalScore = score;
        if (finalScore < 0) {
            try {
                const simRes = await fetch(`/api/similarity?id1=${encodeURIComponent(id1)}&id2=${encodeURIComponent(id2)}`);
                if (simRes.ok) {
                    const simData = await simRes.json();
                    finalScore = simData.scores && simData.scores['unweighted_cosine'] !== undefined ? simData.scores['unweighted_cosine'] : 0;
                }
            } catch (err) {}
        }

        try {
            const res = await fetch(`/api/diff?id1=${encodeURIComponent(id1)}&id2=${encodeURIComponent(id2)}`);
            if (!res.ok) throw new Error("Diff failed");
            const data = await res.json();
            diffPreviewCache.set(cacheKey, data);
            // Only render if this is still the active diff
            if (activeDiffKey === cacheKey) {
                renderDiffPreview(data, name1, name2, finalScore, extra);
            }
        } catch (err) {
            if (activeDiffKey === cacheKey) {
                const rightCol = tooltip.querySelector('.diff-right-col');
                if (rightCol) {
                    rightCol.innerHTML = `<div style="padding:40px; text-align:center; color:#ff5555;">Error loading diff: ${err.message}</div>`;
                }
            }
        }
    }, 150); // Reduced delay to 150ms for better responsiveness
}

function renderDiffPreview(data, name1, name2, score, extra = 0) {
    const tooltip = getDiffPreviewTooltip();
    if (!tooltip) return;

    const rows = (data && data.rows) || [];
    const diffPairs = window.diffPreviewPairs || [];
    const selectedIdx = window.diffPreviewIndex || 0;
    const showList = diffPairs.length > 1;

    let html = `
        <div class="diff-tooltip-container">
            ${showList ? `
            <div class="diff-left-col">
                <div style="color:var(--accent); font-weight:bold; margin-bottom:4px; font-size:0.95rem;">Similarity Pairs</div>
                <div style="color:#666; font-size:0.65rem; margin-bottom:10px; text-transform:uppercase; letter-spacing:0.5px;">
                    ${diffPairs.length} matches hovered
                </div>
                
                <div class="diff-pair-list">
                    <div class="diff-pair-list-scroll" style="transition: transform 0.1s cubic-bezier(0.17, 0.67, 0.83, 0.67);">
                        ${diffPairs.map((p, idx) => `
                            <div class="diff-pair-item ${idx === selectedIdx ? 'selected' : ''}" data-index="${idx}">
                                <span style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex:1;">
                                    ${p.n1} <span style="opacity:0.5">↔</span> ${p.n2}
                                </span>
                                <span class="score-badge">${p.score >= 0 ? (p.score * 100).toFixed(0) + '%' : '??%'}</span>
                            </div>`).join('')}
                    </div>
                </div>
                <div style="color:#444; margin-top:8px; font-size:0.65rem;">💡 Use scroll wheel to cycle</div>
            </div>` : ''}

            <div class="diff-right-col">
                <div class="diff-preview-header" style="background:rgba(255,255,255,0.03); border-bottom:1px solid rgba(255,255,255,0.05); padding:12px 15px; flex-shrink:0;">
                    <div style="display:flex; flex-direction:column; gap:4px; width:100%;">
                        <div style="display:flex; align-items:center; gap:8px; font-size:0.9rem;">
                            <span style="color:#FFF; font-weight:bold;">${name1}</span>
                            <span style="color:var(--subtle); font-size:0.7rem;">vs</span>
                            <span style="color:#FFF; font-weight:bold;">${name2}</span>
                        </div>
                        <div style="font-size:0.75rem; color:var(--success); display:flex; justify-content:space-between; align-items:center;">
                            <span>Match: <b style="font-size:0.9rem;">${score >= 0 ? (score * 100).toFixed(2) + '%' : '....%'}</b></span>
                            <span style="color:var(--accent); opacity:0.8;">${rows.length} instructions match</span>
                        </div>
                    </div>
                </div>

                <div class="diff-preview-scroll" style="flex:1; overflow-y:auto; overflow-x:hidden; display:flex; align-items:flex-start; background:#0d0f14; font-family:'JetBrains Mono', monospace; font-size:0.7rem; min-height:0;">
                    ${data ? `
                        <div style="flex:1; border-right:1px solid rgba(255,255,255,0.05); border-left:4px solid #fd971f; min-width:0;">
                            ${rows.map(r => renderPreviewSide(r.l, 'l')).join('')}
                        </div>
                        <div style="flex:1; border-left:4px solid var(--success); min-width:0;">
                            ${rows.map(r => renderPreviewSide(r.r, 'r')).join('')}
                        </div>
                    ` : `
                        <div style="flex:1; display:flex; align-items:center; justify-content:center; height:200px; color:#555;">
                            <div style="text-align:center;">
                                <i class="fas fa-spinner fa-spin" style="font-size:1.5rem; margin-bottom:10px;"></i>
                                <div>Loading Diff...</div>
                            </div>
                        </div>
                    `}
                </div>

                ${rows.length > 12 ? `
                <div style="background:rgba(0,0,0,0.2); text-align:center; font-size:0.65rem; color:var(--subtle); padding:6px; border-top:1px solid rgba(255,255,255,0.05); flex-shrink:0;">
                    💡 Use Ctrl+Scroll wheel to scroll code
                </div>` : ''}
            </div>
        </div>
    `;
    tooltip.innerHTML = html;

    // Handle list scrolling
    if (showList) {
        const listScroll = tooltip.querySelector('.diff-pair-list-scroll');
        if (listScroll) {
            const itemHeight = 34;
            listScroll.style.transform = `translateY(-${selectedIdx * itemHeight}px)`;
        }
    }
}

function renderPreviewSide(sideData, side) {
    if (!sideData) return `<div style="height:1.3em; background:rgba(255,255,255,0.02)"></div>`;

    const tokens = sideData.tokens || [];
    let lineHtml = tokens.map(t => {
        const cls = t.diff_class || '';
        const typeClass = t.type ? `token-${t.type}` : '';
        return `<span class="token ${typeClass} ${cls}">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
    }).join('');

    const bgMap = { 'diff-match': 'rgba(166,226,46,0.05)', 'diff-unique': 'rgba(249,38,114,0.05)', 'tag-replace': 'rgba(102,217,239,0.05)' };
    const chunkCls = sideData.chunk_class ? sideData.chunk_class.split(' ')[0] : '';
    const bg = bgMap[chunkCls] || 'transparent';

    return `<div style="white-space:pre; height:1.3em; padding:0 8px; background:${bg}; border-bottom:1px solid rgba(255,255,255,0.02); overflow:hidden;">${lineHtml}</div>`;
}

window.addEventListener('storage', (e) => {
    if (e.key === 'bsim_diff_queue') {
        loadDiffQueue();
    }
    if (e.key === 'bsim_file_diff_queue') {
        loadDiffQueue();
    }
});

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    loadDiffQueue();
});
loadDiffQueue();
