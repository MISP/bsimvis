// Notes are Markdown written by users and the LLM. marked does not sanitize,
// so escape the source first: HTML in a note then shows as text instead of
// running as markup.
function renderNoteMarkdown(text) {
    const escaped = escapeHtml(text);
    return (typeof marked !== 'undefined') ? marked.parse(escaped) : escaped;
}

/**
 * Independent Notes and AI Insight Side Panels for BSimVis
 * Supports both function notes (/api/notes/*) and file notes (/api/notes/file/*).
 */

let currentNotesFuncId = null;
let lastRenderedNotesFuncId = null;
let lastRenderedAIFuncId = null;
let currentEditingNoteId = null;
// AI Insight is one ongoing conversation per collection, not per function --
// the analyst pivots between functions while the agent keeps its memory of
// what it already looked at. Notes and Graph stay per-entity (a note is
// attached to one function/file; a call graph is centered on one function),
// so only these two are keyed by collection.
let chatHistories = {};
let llmAbortController = null;
let chatSessions = {}; // collection -> agent chat session_id (in-memory only)
let lastChatFocusId = {}; // collection -> last funcId the chat was told about, for the "now viewing X" divider

// 'func' for function notes, 'file' for file notes
let entityMode = 'func';

// Panel State
let isNotesOpen = false;
let isAIOpen = false;
let isGraphOpen = false;
let isGraphLocked = false;
let currentGraphFuncId = null;
let sideGraphController = null;

const NOTES_WIDTH = 500;
const AI_WIDTH = 600;
// Pivotick's own light-mode UI chrome (toolbar/rail/header) needs >600px in
// both dimensions or it silently downgrades to the plainer 'viewer' mode --
// keep this comfortably past that floor so the side panel actually gets it.
const GRAPH_WIDTH = 640;

/** Collection the AI Insight chat should use right now: the focused entity's
 * collection when one is focused, otherwise whatever collection/pool the
 * current page is for. Null only when there's truly no collection context
 * (e.g. the bare home/collections-list page). */
function getChatScopeCollection() {
    if (currentNotesFuncId) return window.getCollectionFromId(currentNotesFuncId);
    if (typeof window.getRoutingState !== 'function') return null;
    const routing = window.getRoutingState();
    if (routing.collection) return routing.collection;
    if (routing.pool) return `global:pool:${routing.pool}`;
    return null;
}

async function showNotes(funcId, expand = true) {
    const isNewFunc = funcId !== currentNotesFuncId;
    currentNotesFuncId = funcId;
    // ponytail: the id carries the entity kind, so derive it instead of trusting
    // the sticky flag showFileNotes() sets (stale after navigating file -> function)
    entityMode = String(funcId).split(':')[1] === 'file' ? 'file' : 'func';

    // Ensure panels exist
    createPanelsIfMissing();

    // Expand if requested
    if (expand) {
        openNotesPanel();
        // only refresh AI if it's already open
        if (isAIOpen) openAIPanel();
    } else {
        updateLayout();
        // AI Insight may already be open on a shared, collection-scoped
        // conversation while the analyst clicks through other functions --
        // mark the pivot in that thread even though the panel isn't
        // (re)opening, so a later question doesn't need to restate it.
        if (isAIOpen && isNewFunc) noteChatFocusChange(funcId);
    }

    // Load data if new function or not yet rendered
    if (isNewFunc || lastRenderedNotesFuncId !== funcId) {
        await refreshNotes(funcId);
    }

    // Also update graph panel if open and not locked
    if (isGraphOpen && !isGraphLocked && funcId && entityMode !== 'file') {
        loadSideGraph(funcId);
    }

    // Add key listeners
    setupInputListeners();
}

/** Entry point for file-level notes. entityMode is derived from the id in showNotes. */
async function showFileNotes(fileId, expand = true) {
    await showNotes(fileId, expand);
}

function createPanelsIfMissing() {
    if (document.getElementById('panel-handles-container')) return;

    injectNotesStyles();

    // Container for Handles
    const handleContainer = document.createElement('div');
    handleContainer.id = 'panel-handles-container';
    document.body.appendChild(handleContainer);

    // Pivotick Graph Handle
    const graphHandle = document.createElement('div');
    graphHandle.id = 'pivotick-panel-handle';
    graphHandle.className = 'panel-handle graph';
    graphHandle.innerHTML = '<i class="fa-solid fa-diagram-project"></i><span>GRAPH</span>';
    graphHandle.onclick = toggleGraphPanel;
    handleContainer.appendChild(graphHandle);

    // Notes Handle
    const notesHandle = document.createElement('div');
    notesHandle.id = 'notes-panel-handle';
    notesHandle.className = 'panel-handle user';
    notesHandle.innerHTML = '<i class="fa-solid fa-note-sticky"></i><span>NOTES</span><div class="note-count-badge" id="notes-handle-badge" style="display:none"></div>';
    notesHandle.onclick = toggleNotesPanel;
    handleContainer.appendChild(notesHandle);

    // AI Handle
    const aiHandle = document.createElement('div');
    aiHandle.id = 'ai-panel-handle';
    aiHandle.className = 'panel-handle ai';
    aiHandle.innerHTML = '<i class="fa-solid fa-robot"></i><span>AI INSIGHT</span>';
    aiHandle.onclick = toggleAIPanel;
    handleContainer.appendChild(aiHandle);

    // Pivotick Graph Panel
    const graphPanel = document.createElement('div');
    graphPanel.id = 'pivotick-panel-v2';
    graphPanel.className = 'side-panel-v2';
    graphPanel.style.width = GRAPH_WIDTH + 'px';
    graphPanel.style.right = -(GRAPH_WIDTH + 50) + 'px';
    document.body.appendChild(graphPanel);

    // Notes Panel
    const notesPanel = document.createElement('div');
    notesPanel.id = 'notes-panel-v2';
    notesPanel.className = 'side-panel-v2';
    notesPanel.style.width = NOTES_WIDTH + 'px';
    notesPanel.style.right = -(NOTES_WIDTH + 50) + 'px';
    document.body.appendChild(notesPanel);

    // AI Panel
    const aiPanel = document.createElement('div');
    aiPanel.id = 'ai-panel-v2';
    aiPanel.className = 'side-panel-v2';
    aiPanel.style.width = AI_WIDTH + 'px';
    aiPanel.style.right = -(AI_WIDTH + 50) + 'px';
    document.body.appendChild(aiPanel);

    renderGraphPanelHTML(graphPanel);
    renderNotesPanelHTML(notesPanel);
    renderAIPanelHTML(aiPanel);
}

function renderNotesPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <h3 style="margin: 0; font-size: 0.9rem; color: #ffd700;"><i class="fa-solid fa-comments"></i> Notes</h3>
            <button onclick="closeNotesPanel()" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='var(--text)'" onmouseout="this.style.color='var(--subtle)'"><i class="fa-solid fa-xmark"></i></button>
        </div>
        <div id="notes-column" style="flex: 1; display: flex; flex-direction: column; position: relative; overflow: hidden;">
            <div id="notes-drop-overlay" style="display:none; position:absolute; top:0; left:0; width:100%; height:100%; background: rgba(255, 215, 0, 0.1); border: 2px dashed #ffd700; z-index: 100; pointer-events: none; align-items: center; justify-content: center; flex-direction: column; color: #ffd700; font-weight: bold; font-size: 1.2rem; backdrop-filter: blur(2px);">
                <i class="fa-solid fa-plus-circle" style="font-size: 3rem; margin-bottom: 10px;"></i>
                Drop to Save Note
            </div>
            <div id="notes-list" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; background: var(--bg);">
                <div style="text-align: center; color: var(--subtle); padding: 20px;">Loading notes...</div>
            </div>
            <div style="padding: 16px; background: var(--meta-bg); border-top: 1px solid var(--border);">
                <textarea id="new-note-text" placeholder="Add a new note (Markdown)..." style="width: 100%; min-height: 80px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;"></textarea>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <select id="note-owner-select" style="background: var(--bg); color: var(--meta-text-muted); border: 1px solid var(--border); border-radius: 4px; padding: 4px 8px; font-size: 0.8rem; outline: none;">
                        <option value="user">User</option>
                        <option value="llm">LLM</option>
                    </select>
                    <button onclick="saveNote(currentNotesFuncId)" style="background: var(--accent, #ffd700); color: var(--window-tray); border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Add Note</button>
                </div>
            </div>
        </div>
    `;

    // Re-setup drag/drop for this specific column
    const notesColumn = el.querySelector('#notes-column');
    const dropOverlay = el.querySelector('#notes-drop-overlay');
    notesColumn.addEventListener('dragover', (e) => { e.preventDefault(); dropOverlay.style.display = 'flex'; });
    notesColumn.addEventListener('dragleave', (e) => {
        const rect = notesColumn.getBoundingClientRect();
        if (e.clientX <= rect.left || e.clientX >= rect.right || e.clientY <= rect.top || e.clientY >= rect.bottom) {
            dropOverlay.style.display = 'none';
        }
    });
    notesColumn.addEventListener('drop', async (e) => {
        e.preventDefault();
        dropOverlay.style.display = 'none';
        const text = e.dataTransfer.getData('text');
        if (text && text.trim()) await handleDroppedText(currentNotesFuncId, text.trim());
    });
}

let isPoolScope = false;
let activePoolId = '';

function toggleGraphScope() {
    isPoolScope = !isPoolScope;
    const txt = document.getElementById('pivotick-scope-text');
    if (txt) {
        txt.textContent = isPoolScope ? 'Pool Scope' : 'Collection';
    }
    const btn = document.getElementById('pivotick-scope-btn');
    if (btn) {
        btn.style.borderColor = isPoolScope ? '#ae81ff' : 'var(--border)';
        btn.style.color = isPoolScope ? '#ae81ff' : 'var(--meta-text)';
    }
    if (window.showToast) {
        window.showToast(`Graph traversal scope switched to ${isPoolScope ? 'Pool' : 'Collection'} mode`, 'info');
    }
    if (currentGraphFuncId && sideGraphController) {
        sideGraphController.recenter(currentGraphFuncId);
    }
}
window.toggleGraphScope = toggleGraphScope;

window.toggleSideSimilarityEdges = async function(show) {
    if (!sideGraphController) return;
    await sideGraphController.toggleSimilarity(show);
};

function renderGraphPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <span style="display: flex; align-items: center; gap: 8px; font-size: 0.9rem; color: var(--accent, #60a5fa); font-weight: bold;">
                <i class="fa-solid fa-diagram-project"></i> Call Graph
            </span>
            <div style="display: flex; align-items: center; gap: 6px;">
                <label style="cursor:pointer; display:flex; align-items:center; gap:5px; color:var(--text); font-size:0.75rem; background:var(--meta-bg); border:1px solid var(--border); padding:2px 6px; border-radius:4px;" title="Toggle high-confidence similarity edges">
                    <input type="checkbox" id="fn-side-cg-sim-toggle" checked onchange="window.toggleSideSimilarityEdges && window.toggleSideSimilarityEdges(this.checked)" style="margin:0;">
                    <span>Sims ⚡</span>
                </label>
                <button id="pivotick-scope-btn" onclick="toggleGraphScope()" title="Switch graph traversal between Collection and Pool scope" style="background: var(--meta-bg); border: 1px solid var(--border); color: var(--meta-text); padding: 3px 8px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; display: flex; align-items: center; gap: 5px;">
                    <i class="fa-solid fa-layer-group"></i> <span id="pivotick-scope-text">${isPoolScope ? 'Pool Scope' : 'Collection'}</span>
                </button>
                <button id="pivotick-lock-btn" onclick="toggleGraphLock()" title="Lock graph to current function" style="background: var(--meta-bg); border: 1px solid var(--border); color: var(--meta-text); padding: 3px 8px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; display: flex; align-items: center; gap: 5px;">
                    <i class="fa-solid fa-lock-open"></i> Unlocked
                </button>
                <button onclick="closeGraphPanel()" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center;"><i class="fa-solid fa-xmark"></i></button>
            </div>
        </div>
        <div id="pivotick-side-body" style="flex: 1; display: flex; flex-direction: column; position: relative; overflow: hidden; background: var(--bg);">
            <div id="pivotick-side-loader" style="text-align: center; padding: 40px; color: var(--dim);">
                <i class="fa-solid fa-spinner fa-spin"></i> Loading call graph...
            </div>
            <div id="pivotick-side-container" style="display: none; width: 100%; height: 100%; position: relative;"></div>
            <div id="pivotick-side-legend" style="position:absolute; bottom:8px; left:8px; z-index:100; display:flex; flex-direction:column; gap:3px; background:rgba(0,0,0,0.6); backdrop-filter:blur(4px); padding:6px 10px; border-radius:6px; border:1px solid var(--border); font-size:0.65rem; color:var(--subtle);">
                ${typeof FunctionView !== 'undefined' ? FunctionView.renderLegendHTML() : ''}
            </div>
        </div>
    `;
}

function renderAIPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <span style="display: flex; align-items: center; gap: 8px; font-size: 0.9rem; color: #ae81ff; font-weight: bold;"><i class="fa-solid fa-robot"></i> AI Insight</span>
            <div style="display: flex; align-items: center; gap: 12px;">
                <div id="llm-status" style="font-size: 0.75rem; color: #ae81ff; font-style: italic; font-weight: normal; text-transform: none;"></div>
                <button onclick="closeAIPanel()" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='var(--text)'" onmouseout="this.style.color='var(--subtle)'"><i class="fa-solid fa-xmark"></i></button>
            </div>
        </div>
        <div id="llm-chat-history" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 20px; background: var(--card-bg);"></div>
        <div style="padding: 16px; background: var(--meta-bg); border-top: 1px solid var(--border);">
            <textarea id="llm-input" placeholder="Ask AI about this function..." style="width: 100%; min-height: 80px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;"></textarea>
            <div style="display: flex; justify-content: flex-end; gap: 10px;">
                <button id="llm-stop-btn" onclick="stopLLMGeneration()" style="display:none; background: #f44336; color: var(--text); border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Stop</button>
                <button id="llm-send-btn" onclick="sendLLMChat()" style="background: #ae81ff; color: var(--window-tray); border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Send</button>
            </div>
        </div>
    `;
}

function updateLayout() {
    const notesPanel = document.getElementById('notes-panel-v2');
    const aiPanel = document.getElementById('ai-panel-v2');
    const graphPanel = document.getElementById('pivotick-panel-v2');
    
    let totalOffset = 0;
    
    // AI is on the far right
    if (isAIOpen) {
        aiPanel.style.right = '0';
        totalOffset += AI_WIDTH;
    } else {
        if (aiPanel) aiPanel.style.right = -(AI_WIDTH + 50) + 'px';
    }
    
    // Notes is to the left of AI
    if (isNotesOpen) {
        notesPanel.style.right = (isAIOpen ? AI_WIDTH : 0) + 'px';
        totalOffset += NOTES_WIDTH;
    } else {
        if (notesPanel) notesPanel.style.right = -(NOTES_WIDTH + 50) + 'px';
    }

    // Graph is to the left of Notes
    if (isGraphOpen) {
        if (graphPanel) graphPanel.style.right = totalOffset + 'px';
        totalOffset += GRAPH_WIDTH;
    } else {
        if (graphPanel) graphPanel.style.right = -(GRAPH_WIDTH + 50) + 'px';
    }
    
    document.body.style.paddingRight = totalOffset + 'px';
    
    // Move handles container with panels
    const handleContainer = document.getElementById('panel-handles-container');
    if (handleContainer) {
        handleContainer.style.right = totalOffset + 'px';
        
        // Hide handles if collapsed and there's no collection to talk about at
        // all (e.g. the bare home/collections-list page). Any page inside a
        // collection keeps them available, not just a function/file's own
        // page -- that's what makes AI Insight reachable globally.
        const hasCollectionContext = !!getChatScopeCollection();

        if (!hasCollectionContext && !isNotesOpen && !isAIOpen && !isGraphOpen) {
            handleContainer.style.opacity = '0';
            handleContainer.style.pointerEvents = 'none';
        } else {
            handleContainer.style.opacity = '1';
            handleContainer.style.pointerEvents = 'auto';
        }
    }
    
    // Update handle active states
    const notesHandle = document.getElementById('notes-panel-handle');
    const aiHandle = document.getElementById('ai-panel-handle');
    const graphHandle = document.getElementById('pivotick-panel-handle');
    if (notesHandle) notesHandle.classList.toggle('active', isNotesOpen);
    if (aiHandle) aiHandle.classList.toggle('active', isAIOpen);
    if (graphHandle) {
        graphHandle.classList.toggle('active', isGraphOpen);
        graphHandle.classList.toggle('locked', isGraphLocked);
    }
}

function toggleGraphPanel() { if (isGraphOpen) closeGraphPanel(); else openGraphPanel(currentNotesFuncId); }
function toggleNotesPanel() { if (isNotesOpen) closeNotesPanel(); else openNotesPanel(); }
function toggleAIPanel() { if (isAIOpen) closeAIPanel(); else openAIPanel(); }

function openGraphPanel(funcId) {
    isGraphOpen = true;
    updateLayout();
    const idToLoad = funcId || currentNotesFuncId || currentGraphFuncId;
    if (idToLoad && (!sideGraphController || (!isGraphLocked && currentGraphFuncId !== idToLoad))) {
        loadSideGraph(idToLoad);
    }
}
function closeGraphPanel() { isGraphOpen = false; updateLayout(); }

function toggleGraphLock() {
    isGraphLocked = !isGraphLocked;
    const lockBtn = document.getElementById('pivotick-lock-btn');
    const handle = document.getElementById('pivotick-panel-handle');
    if (lockBtn) {
        lockBtn.innerHTML = isGraphLocked 
            ? '<i class="fa-solid fa-lock" style="color:#f92672;"></i> Locked' 
            : '<i class="fa-solid fa-lock-open"></i> Unlocked';
    }
    if (handle) handle.classList.toggle('locked', isGraphLocked);
}

async function loadSideGraph(funcId) {
    if (!funcId) return;
    currentGraphFuncId = funcId;
    const container = document.getElementById('pivotick-side-container');
    const loader = document.getElementById('pivotick-side-loader');
    if (!container || !loader) return;

    loader.style.display = 'block';
    loader.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Loading call graph...';
    container.style.display = 'none';

    try {
        loader.style.display = 'none';
        container.style.display = 'block';

        if (sideGraphController) {
            await sideGraphController.recenter(funcId);
        } else {
            container.innerHTML = '';
            sideGraphController = new PivotickGraphController(container, { collection: window.getCollectionFromId ? window.getCollectionFromId(funcId) : '' });
            await sideGraphController.addFunction(funcId, { asCenter: true });
            setupGraphDropTarget(document.getElementById('pivotick-side-body'), sideGraphController);
        }
    } catch (err) {
        loader.style.display = 'block';
        loader.innerHTML = `<div style="padding:20px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> ${err.message}</div>`;
    }
}

window.setupGraphDropTarget = function(el, controller) {
    if (!el) return;
    if (controller) el._graphController = controller;
    if (el._dropSetup) return;
    el._dropSetup = true;

    el.addEventListener('dragover', (e) => {
        e.preventDefault();
        if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
    });

    el.addEventListener('drop', async (e) => {
        e.preventDefault();
        let rawData = e.dataTransfer.getData('application/bsimvis-nodes') || e.dataTransfer.getData('text/plain');
        if (!rawData) return;
        let ids = [];
        try {
            ids = JSON.parse(rawData);
            if (!Array.isArray(ids)) ids = [ids];
        } catch (err) {
            ids = [rawData];
        }
        // Drop lands on a specific container -- route to the controller that
        // actually owns it instead of guessing which graph is "active",
        // fixing drops silently going to a hidden tab's graph.
        await addNodesToActiveGraph(ids, el._graphController);
    });
}

// Which graph a table-selection "add to graph" click (no drop-target
// element to anchor on) should target: whichever of the two graph surfaces
// is actually visible right now.
function getVisibleGraphController() {
    const isVisible = (el) => !!el && el.offsetParent !== null;
    const fvController = (typeof FunctionView !== 'undefined') ? FunctionView.graphController : null;
    if (fvController && isVisible(document.getElementById('fn-cg-container'))) return fvController;
    if (sideGraphController && isVisible(document.getElementById('pivotick-side-container'))) return sideGraphController;
    return fvController || sideGraphController;
}
window.getVisibleGraphController = getVisibleGraphController;

async function addNodesToActiveGraph(ids, controller) {
    if (!Array.isArray(ids)) ids = [ids];
    if (!ids.length) return;

    let ctrl = controller || getVisibleGraphController();
    if (!ctrl && !isGraphOpen) openGraphPanel(ids[0]);

    let addedCount = 0;
    for (const funcId of ids) {
        if (typeof funcId !== 'string' || !funcId.includes(':func:')) continue;
        try {
            if (!ctrl) {
                if (!sideGraphController) await loadSideGraph(funcId);
                ctrl = sideGraphController;
                addedCount++;
                continue;
            }
            const before = ctrl.nodes.size;
            await ctrl.addFunction(funcId);
            if (ctrl.nodes.size > before) addedCount++;
        } catch (err) {
            console.warn('Failed to add node to graph', funcId, err);
        }
    }
    if (addedCount > 0 && window.showToast) {
        window.showToast(`Added ${addedCount} node(s) to graph`, 'success');
    }
}
window.addNodesToActiveGraph = addNodesToActiveGraph;
window.addSelectedNodesToActiveGraph = function() {
    const ids = window.getSelectedTableIds ? window.getSelectedTableIds('function') : [];
    if (ids.length) addNodesToActiveGraph(ids);
    else if (window.showToast) window.showToast('No functions selected', 'info');
};

function openNotesPanel() { 
    isNotesOpen = true; 
    updateLayout(); 
    if (currentNotesFuncId && lastRenderedNotesFuncId !== currentNotesFuncId) {
        refreshNotes(currentNotesFuncId);
    }
}
function closeNotesPanel() { isNotesOpen = false; updateLayout(); }

/** Local-only "-- now viewing X --" marker, no LLM call. Lets the analyst
 * see where the conversation's focus moved without spending a summarize
 * call on every function they merely glance at while the panel is open. */
function noteChatFocusChange(funcId) {
    const collection = getChatScopeCollection();
    if (!collection || lastChatFocusId[collection] === funcId) return;
    lastChatFocusId[collection] = funcId;
    if (chatHistories[collection] && chatHistories[collection].length > 0) {
        addChatMessage(collection, "ai", `_— now viewing \`${funcId}\` —_`);
    }
}

function openAIPanel() {
    isAIOpen = true;
    updateLayout();

    const collection = getChatScopeCollection();
    if (!collection) return; // no collection/entity context available at all yet

    if (currentNotesFuncId) lastChatFocusId[collection] = currentNotesFuncId;

    if (!chatHistories[collection] || chatHistories[collection].length === 0) {
        chatHistories[collection] = [];
        // Only worth an automatic summarize call when a specific function or
        // file is actually focused -- opening the dock from a list/dashboard
        // page with nothing selected just shows the empty conversation.
        if (currentNotesFuncId) generateSummary(currentNotesFuncId);
    } else if (lastRenderedAIFuncId !== collection) {
        renderChatHistory(collection);
    }
}
function closeAIPanel() { isAIOpen = false; updateLayout(); }

function setupInputListeners() {
    const llmInput = document.getElementById('llm-input');
    const notesInput = document.getElementById('new-note-text');

    if (llmInput && notesInput) {
        llmInput.onkeydown = (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendLLMChat();
            } else if (e.key === 'Tab') {
                e.preventDefault();
                notesInput.focus();
            }
        };

        notesInput.onkeydown = (e) => {
            if (e.key === 'Tab') {
                e.preventDefault();
                llmInput.focus();
            }
        };
        
        setTimeout(() => llmInput.focus(), 100);
    }
}

function injectNotesStyles() {
    if (document.getElementById('notes-md-styles')) return;
    
    const style = document.createElement('style');
    style.id = 'notes-md-styles';
    style.textContent = `
        body { 
            transition: padding-right 0.3s cubic-bezier(0.4, 0, 0.2, 1); 
            box-sizing: border-box;
            width: 100vw;
            overflow-x: hidden;
        }
        
        #panel-handles-container {
            position: fixed;
            right: 0;
            top: 20%;
            display: flex;
            flex-direction: column;
            gap: 10px;
            z-index: 10005;
            transition: right 0.3s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease;
        }

        .side-panel-v2 {
            position: fixed;
            top: 0;
            height: 100vh;
            background: var(--card-bg);
            border-left: 1px solid var(--border);
            z-index: 10000;
            display: flex;
            flex-direction: column;
            transition: right 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            font-family: 'Inter', sans-serif;
        }

        .panel-v2-header {
            padding: 12px 16px; 
            background: var(--meta-bg); 
            border-bottom: 1px solid var(--border); 
            display: flex; 
            justify-content: space-between; 
            align-items: center;
        }

        .panel-handle {
            background: var(--meta-bg);
            border: 1px solid var(--border);
            border-right: none;
            color: var(--subtle);
            padding: 15px 8px;
            cursor: pointer;
            border-radius: 8px 0 0 8px;
            writing-mode: vertical-rl;
            text-orientation: mixed;
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 0.7rem;
            font-weight: bold;
            letter-spacing: 1px;
            transition: all 0.2s;
        }
        .panel-handle:hover { color: var(--text); background: var(--border); }
        .panel-handle.user.active { color: #ffd700; border-color: #ffd700; background: var(--meta-bg); }
        .panel-handle.ai.active { color: #ae81ff; border-color: #ae81ff; background: var(--meta-bg); }
        .panel-handle i { font-size: 0.9rem; transform: rotate(90deg); }

        .note-markdown-body, .llm-markdown-body { 
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem; 
            line-height: 1.6; 
            color: var(--meta-text); 
        }
        .note-markdown-body p, .llm-markdown-body p { margin-top: 0; margin-bottom: 12px; }
        .note-markdown-body code { background: var(--border); padding: 2px 5px; border-radius: 3px; color: #ffd700; font-weight: bold; }
        .llm-markdown-body code { background: var(--border); padding: 2px 5px; border-radius: 3px; color: #ae81ff; font-weight: bold; }
        .note-markdown-body pre, .llm-markdown-body pre { background: var(--bg); padding: 12px; border-radius: 6px; overflow-x: auto; border: 1px solid var(--border); margin: 12px 0; }
        .note-markdown-body blockquote { border-left: 4px solid #ffd700; margin: 12px 0; padding-left: 15px; color: var(--meta-text-muted); font-style: italic; background: rgba(255, 215, 0, 0.05); }
        .llm-markdown-body blockquote { border-left: 4px solid #ae81ff; margin: 12px 0; padding-left: 15px; color: var(--meta-text-muted); font-style: italic; background: color-mix(in srgb, var(--token-address) 5%, transparent); }
        
        .chat-msg { border-radius: 8px; padding: 12px 16px; max-width: 95%; position: relative; }
        .chat-msg.user { background: var(--meta-bg); align-self: flex-end; border-bottom-right-radius: 0; border: 1px solid var(--border); }
        .chat-msg.ai { background: var(--meta-bg); align-self: flex-start; border-bottom-left-radius: 0; border: 1px solid var(--border); }
        .chat-msg.user::after { content: 'YOU'; position: absolute; top: -18px; right: 0; font-size: 0.6rem; color: #ffd700; }
        .chat-msg.ai::after { content: 'AI INSIGHT'; position: absolute; top: -18px; left: 0; font-size: 0.6rem; color: #ae81ff; }
        
        .collapsible-container { position: relative; }
        .collapsible-content { overflow: hidden; transition: max-height 0.3s ease-out; }
        .collapsible-content.collapsed { max-height: 200px; mask-image: linear-gradient(to bottom, black 70%, transparent 100%); -webkit-mask-image: linear-gradient(to bottom, black 70%, transparent 100%); }
        .toggle-expand-btn { background: none; border: none; color: #ffd700; cursor: pointer; font-size: 0.75rem; font-weight: bold; padding: 5px 0; display: flex; align-items: center; gap: 5px; }
        .chat-msg.ai .toggle-expand-btn { color: #ae81ff; }

        #notes-panel-handle .note-count-badge {
            writing-mode: horizontal-tb;
            text-orientation: initial;
            transform: rotate(-90deg);
        }
    `;
    document.head.appendChild(style);
}

function toggleContentExpand(btn) {
    const container = btn.closest('.collapsible-container');
    const content = container.querySelector('.collapsible-content');
    const isCollapsed = content.classList.contains('collapsed');
    if (isCollapsed) {
        content.classList.remove('collapsed');
        btn.innerHTML = '<i class="fa-solid fa-chevron-up"></i> Show Less';
    } else {
        content.classList.add('collapsed');
        btn.innerHTML = '<i class="fa-solid fa-chevron-down"></i> Show More';
        container.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
}

async function refreshNotes(funcId) {
    const listEl = document.getElementById('notes-list');
    if (!listEl) return;
    const collection = window.getCollectionFromId(funcId);
    const isFile = entityMode === 'file';
    const idParam = isFile ? `file_id=${encodeURIComponent(funcId)}` : `func_id=${encodeURIComponent(funcId)}`;
    const endpoint = isFile ? '/api/notes/file/list' : '/api/notes/list';
    try {
        const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
        const res = await fetch(`${endpoint}?${apiParams}&${idParam}`);
        const data = await res.json();
        if (data.status === 'success') {
            lastRenderedNotesFuncId = funcId;
            const notes = data.notes || [];
            
            // Update handle badge
            const badge = document.getElementById('notes-handle-badge');
            if (badge) {
                if (notes.length > 0) {
                    badge.innerText = `+${notes.length}`;
                    badge.style.display = 'block';
                } else {
                    badge.style.display = 'none';
                }
            }

            if (notes.length === 0) {
                listEl.innerHTML = '<div style="text-align: center; color: var(--subtle); padding: 40px; font-style: italic;">No notes yet.</div>';
            } else {
                listEl.innerHTML = notes.map(note => {
                    const isAI = note.owner === 'llm' || note.owner === 'AI';
                    const isEditing = currentEditingNoteId === note.id;
                    
                    if (isEditing) {
                        return `
                            <div class="note-item editing" style="background: var(--meta-bg); border-radius: 6px; padding: 15px; border-left: 4px solid #ffd700; border: 1px solid #ffd700; border-left-width: 4px;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                    <span style="font-size: 0.7rem; font-weight: bold; color: #ffd700; text-transform: uppercase;">Editing Note</span>
                                </div>
                                <textarea id="edit-note-text-${escapeAttr(note.id)}" 
                                    onkeydown="if(event.key==='Enter' && !event.shiftKey){event.preventDefault(); submitEditNote(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(note.id))});} if(event.key==='Escape'){cancelEditNote(${escapeAttr(jsString(funcId))});}"
                                    style="width: 100%; min-height: 100px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;">${escapeHtml(note.text)}</textarea>
                                <div style="display: flex; justify-content: flex-end; gap: 10px;">
                                    <button onclick="cancelEditNote(${escapeAttr(jsString(funcId))})" style="background: var(--border); color: var(--meta-text-muted); border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.75rem;">Cancel</button>
                                    <button onclick="submitEditNote(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(note.id))})" style="background: #ffd700; color: var(--window-tray); border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.75rem;">Save</button>
                                </div>
                            </div>
                        `;
                    }

                    const renderedText = renderNoteMarkdown(note.text);
                    return `
                        <div class="note-item" style="background: var(--meta-bg); border-radius: 6px; padding: 15px; border-left: 4px solid ${isAI ? '#ae81ff' : '#ffd700'}; border: 1px solid var(--border); border-left-width: 4px;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                <span style="font-size: 0.7rem; font-weight: bold; color: ${isAI ? '#ae81ff' : '#ffd700'}; text-transform: uppercase;">${escapeHtml(note.owner)}</span>
                                <span style="font-size: 0.6rem; color: var(--subtle);">${escapeHtml(new Date(note.timestamp).toLocaleString())}</span>
                            </div>
                            <div class="collapsible-container">
                                <div class="note-text note-markdown-body collapsible-content ${note.text.length > 500 ? 'collapsed' : ''}">${renderedText}</div>
                                ${note.text.length > 500 ? '<button class="toggle-expand-btn" onclick="toggleContentExpand(this)"><i class="fa-solid fa-chevron-down"></i> Show More</button>' : ''}
                            </div>
                            <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 10px;">
                                <button onclick="startEditNote(${escapeAttr(jsString(note.id))}, ${escapeAttr(jsString(funcId))})" title="Edit Note" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 0.85rem;"><i class="fa-solid fa-pen"></i></button>
                                <button onclick="deleteNote(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(note.id))})" style="background: none; border: none; color: var(--subtle); cursor: pointer;"><i class="fa-solid fa-trash"></i></button>
                            </div>
                        </div>
                    `;
                }).join('');
            }
        }
    } catch (e) { console.error(e); }
}

function getActivePool() {
    const getRS = window.getRoutingState || window.parent?.getRoutingState;
    if (typeof getRS === 'function') {
        const rs = getRS();
        return rs.pool || null;
    }
    return null;
}

async function saveNote(funcId) {
    const textEl = document.getElementById('new-note-text');
    const ownerEl = document.getElementById('note-owner-select');
    const text = textEl.value.trim();
    if (!text) return;
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/add' : '/api/notes/add';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [idKey]: funcId, text, owner: ownerEl.value };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            textEl.value = '';
            await refreshNotes(funcId);
            if (!isFile) window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
            if (!isFile && window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
            if (isFile && window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
        }
    } catch (e) { alert(e.message); }
}

function startEditNote(noteId, funcId) {
    currentEditingNoteId = noteId;
    refreshNotes(funcId).then(() => {
        const el = document.getElementById(`edit-note-text-${noteId}`);
        if (el) {
            el.focus();
            el.setSelectionRange(el.value.length, el.value.length);
        }
    });
}

function cancelEditNote(funcId) {
    currentEditingNoteId = null;
    refreshNotes(funcId);
}

async function submitEditNote(funcId, noteId) {
    const textEl = document.getElementById(`edit-note-text-${noteId}`);
    const text = textEl.value.trim();
    if (!text) return;

    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/update' : '/api/notes/update';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const pool = getActivePool();
        const payload = {
            collection: window.getCollectionFromId(funcId),
            [idKey]: funcId,
            note_id: noteId,
            text: text
        };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.status === 'success') {
            currentEditingNoteId = null;
            await refreshNotes(funcId);
            if (!isFile) window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
            if (!isFile && window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
            if (isFile && window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
        } else {
            alert(data.error || 'Failed to update note');
        }
    } catch (e) {
        alert(e.message);
    }
}

async function deleteNote(funcId, note_id) {
    if (!confirm('Delete note?')) return;
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/remove' : '/api/notes/remove';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [idKey]: funcId, note_id };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
            if (!isFile) window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
            if (!isFile && window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
            if (isFile && window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
        }
    } catch (e) { alert(e.message); }
}

async function readStream(response, onChunk) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let fullText = "";
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        fullText += decoder.decode(value, { stream: true });
        onChunk(fullText);
    }
    return fullText;
}

async function generateSummary(funcId) {
    const statusEl = document.getElementById("llm-status");
    if (statusEl) statusEl.innerText = "Summarizing...";
    const sendBtn = document.getElementById("llm-send-btn");
    const stopBtn = document.getElementById("llm-stop-btn");
    if (sendBtn) sendBtn.style.display = "none";
    if (stopBtn) stopBtn.style.display = "block";

    const isFile = entityMode === 'file';
    const endpoint = isFile ? "/api/llm/summarize_file" : "/api/llm/summarize";
    const body = isFile ? { file_id: funcId } : { func_id: funcId };
    // The summary posts into the shared, collection-scoped conversation, not
    // a per-function one -- this function still summarizes one function/file,
    // it just writes the result where the rest of the chat lives.
    const chatKey = getChatScopeCollection();

    llmAbortController = new AbortController();
    try {
        const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
            signal: llmAbortController.signal
        });
        const msgEl = addChatMessage(chatKey, "ai", "");
        const msgIndex = chatHistories[chatKey].length - 1;
        const summary = await readStream(response, (text) => updateChatMessageUI(msgEl, text, msgIndex, chatKey));
    } catch (err) {
        if (err.name !== 'AbortError') addChatMessage(chatKey, "ai", "Error: " + err.message);
    } finally {
        if (statusEl) statusEl.innerText = "";
        if (sendBtn) sendBtn.style.display = "block";
        if (stopBtn) stopBtn.style.display = "none";
    }
}

function renderChatHistory(chatKey) {
    const historyEl = document.getElementById("llm-chat-history");
    if (!historyEl) return;
    historyEl.innerHTML = "";
    (chatHistories[chatKey] || []).forEach((msg, index) => {
        const msgEl = document.createElement("div");
        msgEl.className = `chat-msg ${msg.role === "assistant" ? "ai" : "user"}`;
        updateChatMessageUI(msgEl, msg.content, index, chatKey);
        historyEl.appendChild(msgEl);
    });
    historyEl.scrollTop = historyEl.scrollHeight;
    lastRenderedAIFuncId = chatKey;
}

function addChatMessage(chatKey, role, content) {
    const historyEl = document.getElementById("llm-chat-history");
    if (!historyEl) return;
    const isAtBottom = historyEl.scrollHeight - historyEl.scrollTop <= historyEl.clientHeight + 5;

    if (!chatHistories[chatKey]) chatHistories[chatKey] = [];
    const index = chatHistories[chatKey].length;
    chatHistories[chatKey].push({ role: role === "ai" ? "assistant" : "user", content: content });

    const msgEl = document.createElement("div");
    msgEl.className = `chat-msg ${role}`;
    updateChatMessageUI(msgEl, content, index, chatKey);
    historyEl.appendChild(msgEl);

    if (isAtBottom || role === 'user') historyEl.scrollTop = historyEl.scrollHeight;
    return msgEl;
}

function updateChatMessageUI(msgEl, content, index, chatKey) {
    const historyEl = document.getElementById("llm-chat-history");
    const isAtBottom = historyEl ? (historyEl.scrollHeight - historyEl.scrollTop <= historyEl.clientHeight + 5) : false;

    // Save to history object so re-renders don't lose progress
    if (chatKey && chatHistories[chatKey] && chatHistories[chatKey][index]) {
        chatHistories[chatKey][index].content = content;
    }

    let html = renderNoteMarkdown(content);
    const isLong = content.length > 500;
    msgEl.innerHTML = `
        <div class="collapsible-container">
            <div class="llm-markdown-body collapsible-content">${html}</div>
            ${isLong ? '<button class="toggle-expand-btn" onclick="toggleContentExpand(this)"><i class="fa-solid fa-chevron-up"></i> Show Less</button>' : ''}
        </div>
    `;
    if (msgEl.classList.contains('ai') && content.trim().length > 0) {
        const actionsEl = document.createElement("div");
        actionsEl.style.cssText = "margin-top: 10px; display: flex; justify-content: flex-end; border-top: 1px solid var(--border); padding-top: 8px;";
        actionsEl.innerHTML = `<button onclick="saveMessageAsNote(${escapeAttr(jsString(currentNotesFuncId))}, ${Number(index)}, this)" style="background: #2a2a2a; color: #ffd700; border: 1px solid var(--border); padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; font-weight: bold;"><i class="fa-solid fa-plus"></i> Save Note</button>`;
        msgEl.appendChild(actionsEl);
    }
    if (historyEl && isAtBottom) historyEl.scrollTop = historyEl.scrollHeight;
}

async function ensureChatSession(collection) {
    if (chatSessions[collection]) return chatSessions[collection];
    const res = await fetch("/api/llm/chat/session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ collection })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    chatSessions[collection] = data.session_id;
    return data.session_id;
}

/** What to tell the agent about the analyst's current focus, prepended to
 * every outgoing message (not just at session start) since the same session
 * now spans however many functions/files the analyst pivots through. */
function currentFocusContextLine() {
    if (!currentNotesFuncId) {
        return "(No specific function or file is currently focused -- this is a general question about the collection.)";
    }
    const kind = entityMode === 'file' ? 'file' : 'function';
    return `(Analyst is currently viewing ${kind} ${currentNotesFuncId}. Assume this question refers to it unless stated otherwise.)`;
}

async function sendLLMChat() {
    const inputEl = document.getElementById("llm-input");
    const text = inputEl.value.trim();
    const chatKey = getChatScopeCollection();
    if (!text || !chatKey) return;
    lastChatFocusId[chatKey] = currentNotesFuncId;
    addChatMessage(chatKey, "user", text);
    inputEl.value = "";
    const sendBtn = document.getElementById("llm-send-btn");
    const stopBtn = document.getElementById("llm-stop-btn");
    const statusEl = document.getElementById("llm-status");
    if (sendBtn) sendBtn.style.display = "none";
    if (stopBtn) stopBtn.style.display = "block";
    if (statusEl) statusEl.innerText = "Agent investigating (may look up related functions)...";
    llmAbortController = new AbortController();
    // The agent runs to completion server-side (including every tool call)
    // before responding, so there is nothing to stream -- unlike the old
    // /api/llm/chat this can take several seconds to a minute rather than
    // starting to type back immediately.
    const msgEl = addChatMessage(chatKey, "ai", "_investigating..._");
    const msgIndex = chatHistories[chatKey].length - 1;
    try {
        const sessionId = await ensureChatSession(chatKey);
        // Context travels with the message, not just the session start, so a
        // question two functions later still tells the agent what's in view.
        const apiMessage = `${currentFocusContextLine()}\n\n${text}`;
        const response = await fetch(`/api/llm/chat/session/${sessionId}/message`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: apiMessage }),
            signal: llmAbortController.signal
        });
        const data = await response.json();
        if (data.error) throw new Error(data.error);
        let content = data.reply || "_(no reply)_";
        if (data.tool_calls && data.tool_calls.length) {
            const names = data.tool_calls.map(tc => `\`${tc.name}\``).join(', ');
            content = `> 🔧 looked up: ${names}\n\n${content}`;
        }
        updateChatMessageUI(msgEl, content, msgIndex, chatKey);
    } catch (err) {
        if (err.name !== 'AbortError') updateChatMessageUI(msgEl, "Error: " + err.message, msgIndex, chatKey);
    } finally {
        if (sendBtn) sendBtn.style.display = "block";
        if (stopBtn) stopBtn.style.display = "none";
        if (statusEl) statusEl.innerText = "";
    }
}

function stopLLMGeneration() { if (llmAbortController) llmAbortController.abort(); }

async function saveMessageAsNote(funcId, index, btn) {
    // funcId here is the entity the button's onclick was rendered with
    // (currentNotesFuncId at render time) -- the chat thread itself is keyed
    // by collection, so look the message content up via that.
    if (!funcId) { alert('No function or file is focused to attach this note to.'); return; }
    const collection = window.getCollectionFromId(funcId);
    const history = chatHistories[collection];
    if (!history || !history[index]) return;
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/add' : '/api/notes/add';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const pool = getActivePool();
        const payload = { collection, [idKey]: funcId, text: history[index].content, owner: "llm" };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === "success") {
            await refreshNotes(funcId);
            btn.innerHTML = '<i class="fa-solid fa-check"></i> Saved';
            btn.disabled = true;
            if (!isFile) window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
            if (!isFile && window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
            if (isFile && window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
        }
    } catch (e) { alert(e.message); }
}

async function handleDroppedText(funcId, text) {
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/add' : '/api/notes/add';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [idKey]: funcId, text: text, owner: 'llm' };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
            if (!isFile) window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
            if (!isFile && window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
            if (isFile && window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
        }
    } catch (e) { alert(e.message); }
}

// Global exposure
window.showNotes = showNotes;
window.showFileNotes = showFileNotes;
window.toggleNotesPanel = toggleNotesPanel;
window.toggleAIPanel = toggleAIPanel;
window.toggleGraphPanel = toggleGraphPanel;
window.toggleGraphLock = toggleGraphLock;
window.openGraphPanel = openGraphPanel;
window.closeGraphPanel = closeGraphPanel;
window.loadSideGraph = loadSideGraph;
window.saveNote = saveNote;
window.startEditNote = startEditNote;
window.cancelEditNote = cancelEditNote;
window.submitEditNote = submitEditNote;
window.deleteNote = deleteNote;
window.sendLLMChat = sendLLMChat;
window.stopLLMGeneration = stopLLMGeneration;
window.saveMessageAsNote = saveMessageAsNote;
window.toggleContentExpand = toggleContentExpand;
window.showNotePanel = function(id, e) { if (typeof showNotes === 'function') showNotes(id); };
window.showFileNotePanel = function(id, e) { if (typeof showFileNotes === 'function') showFileNotes(id); };

// Connect layout updates with SPA navigation. Graph/Notes/AI Insight are
// global now: navigating to a different page (functions list, cluster view,
// dashboard) inside the same collection no longer force-closes them -- only
// leaving the collection entirely (handled by getChatScopeCollection/
// updateLayout's visibility check) hides the handle rail.
document.addEventListener('DOMContentLoaded', () => {
    if (typeof window.refreshData === 'function') {
        const origRefresh = window.refreshData;
        window.refreshData = async function(...args) {
            const res = await origRefresh.apply(this, args);
            createPanelsIfMissing();
            updateLayout();
            return res;
        };
    }
    createPanelsIfMissing();
    updateLayout();
});

// --- Hover Tooltip for Notes ---
window.showNoteTooltip = async function(id, isFile, e) {
    const isMenuOpen = window.graphContextMenuOpen || (window.top && window.top.graphContextMenuOpen);
    if (isMenuOpen) return;
    if (!id) return;
    
    let tooltip = document.getElementById('note-preview-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.id = 'note-preview-tooltip';
        Object.assign(tooltip.style, {
            position: 'fixed',
            display: 'none',
            zIndex: '20000',
            pointerEvents: 'none',
            minWidth: '320px',
            maxWidth: '480px',
            maxHeight: '450px',
            overflow: 'hidden',
            flexDirection: 'column',
            gap: '10px'
        });
        document.body.appendChild(tooltip);
    }

    tooltip.style.display = 'flex';
    tooltip.classList.add('showing');
    if (window.moveCodePreview) window.moveCodePreview(e);

    tooltip.innerHTML = `
        <div class="preview-card" style="max-height:450px; display:flex; flex-direction:column;">
            <div class="preview-header">Notes: ${escapeHtml(id.split(':').pop())}</div>
            <div class="note-preview-scroll" style="flex:1; overflow-y:auto; padding: 10px;">
                <div style="text-align: center; color: var(--subtle); font-style: italic; font-size: 0.8rem;">Loading notes...</div>
            </div>
        </div>
    `;

    const collection = (window.getCollectionFromId && window.getCollectionFromId(id)) || id.split(':')[0];
    const idParam = isFile ? `file_id=${encodeURIComponent(id)}` : `func_id=${encodeURIComponent(id)}`;
    const endpoint = isFile ? '/api/notes/file/list' : '/api/notes/list';
    
    try {
        const apiParams = (window.getApiParams || (window.parent && window.parent.getApiParams) || (() => ''))(collection);
        const res = await fetch(`${endpoint}?${apiParams}&${idParam}`);
        const data = await res.json();
        if (data.status === 'success') {
            const notes = data.notes || [];
            const scrollContainer = tooltip.querySelector('.note-preview-scroll');
            if (notes.length === 0) {
                scrollContainer.innerHTML = '<div style="text-align: center; color: var(--subtle); font-style: italic; font-size: 0.8rem;">No notes found.</div>';
            } else {
                scrollContainer.innerHTML = notes.map(note => {
                    const isAI = note.owner === 'llm' || note.owner === 'AI';
                    const renderedText = renderNoteMarkdown(note.text);
                    return `
                        <div style="background: var(--meta-bg); border-radius: 6px; padding: 12px; margin-bottom: 8px; border-left: 4px solid ${isAI ? 'var(--info)' : 'var(--note-accent)'}; border-top: 1px solid var(--border); border-right: 1px solid var(--border); border-bottom: 1px solid var(--border);">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                                <span style="font-size: 0.65rem; font-weight: bold; color: ${isAI ? 'var(--info)' : 'var(--note-accent)'}; text-transform: uppercase;">${escapeHtml(note.owner)}</span>
                                <span style="font-size: 0.55rem; color: var(--subtle);">${escapeHtml(new Date(note.timestamp).toLocaleString())}</span>
                            </div>
                            <div class="note-markdown-body" style="font-size: 0.75rem;">${renderedText}</div>
                        </div>
                    `;
                }).join('');
                if (notes.length > 1) {
                    scrollContainer.innerHTML += `<div style="text-align:center; font-size:0.65rem; color:var(--subtle); margin-top:8px;">💡 Use scroll wheel to read all notes</div>`;
                }
            }
        }
    } catch (err) {
        tooltip.innerHTML = `<div class="preview-header" style="color:#ff5555">Error loading notes</div>`;
    }
};

window.hideNoteTooltip = function() {
    const tooltip = document.getElementById('note-preview-tooltip');
    if (tooltip) {
        tooltip.style.display = 'none';
        tooltip.classList.remove('showing');
    }
};

window.addDiffEdgeBetweenNodes = function(funcId1, funcId2) {
    const ctrl = getVisibleGraphController();
    if (!ctrl || !ctrl.nodes.has(funcId1) || !ctrl.nodes.has(funcId2)) return;

    const before = ctrl.edges.size;
    ctrl.addCompareEdge(funcId1, funcId2);
    if (ctrl.edges.size > before) {
        if (window.showToast) window.showToast('Diff edge added between nodes', 'info');
    }
};
