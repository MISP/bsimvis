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

// 'func' for function notes, 'file' for file notes, 'bin_sim' for pair notes
let entityMode = 'func';

const NOTE_MODE_INFO = {
    func: { idKey: 'func_id', base: '/api/notes' },
    file: { idKey: 'file_id', base: '/api/notes/file' },
    bin_sim: { idKey: 'sid', base: '/api/notes/bin_sim' }
};
function noteMode() { return NOTE_MODE_INFO[entityMode] || NOTE_MODE_INFO.func; }

/** A note attaches to an entity. The panel can be opened from its rail handle
 * with nothing focused, and a null id used to POST straight through as
 * `func_id: null`, which the API answers with "Missing parameters". */
function requireNoteTarget(funcId) {
    if (funcId) return true;
    const msg = 'Open a function, file, or comparison first -- a note attaches to one of those.';
    if (window.showToast) window.showToast(msg, 'warning'); else alert(msg);
    return false;
}

/** Fires the per-mode "a note changed" hooks after a note write. */
function notifyNoteChanged(funcId) {
    if (entityMode === 'func') {
        window.dispatchEvent(new CustomEvent('bsimvis:note-changed', { detail: { funcId } }));
        if (window.parent?.refreshFunctionRow) window.parent.refreshFunctionRow(funcId);
    } else if (entityMode === 'file') {
        if (window.parent?.refreshFileRow) window.parent.refreshFileRow(funcId);
    } else if (entityMode === 'bin_sim') {
        if (window.parent?.refreshBinSimRow) window.parent.refreshBinSimRow(funcId);
    }
}

// Panel State
let isNotesOpen = false;
let isAIOpen = false;
let isGraphOpen = false;
let isGraphLocked = false;
let currentGraphFuncId = null;
let sideGraphController = null;

// Panel widths are user-resizable (drag handle on each panel's left edge)
// and persisted per-browser so a chosen size survives reloads. Pivotick's
// own light-mode UI chrome (toolbar/rail/header) needs >600px in both
// dimensions or it silently downgrades to the plainer 'viewer' mode --
// keep its floor comfortably past that so the side panel actually gets it.
const PANEL_MAX_WIDTH = 1100;
const PANEL_MIN_WIDTHS = { notes: 320, ai: 360, graph: 620 };
const PANEL_DEFAULTS = { notes: 500, ai: 600, graph: 640 };
const panelWidths = { ...PANEL_DEFAULTS };
for (const key of Object.keys(panelWidths)) {
    const stored = parseInt(localStorage.getItem(`bsimvis-panel-width-${key}`), 10);
    if (stored >= PANEL_MIN_WIDTHS[key] && stored <= PANEL_MAX_WIDTH) panelWidths[key] = stored;
}

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

/** Which NOTE_MODE_INFO entry an entity id belongs to. Match on the kind marker,
 * never on segment 1: a pool pair sid is
 * global:pool:<id>:bin_sim:<algo>:<md5a>::<md5b>, whose segment 1 is "pool" --
 * reading that segment sent every pool pair's notes down the function path. */
function entityKindFromId(id) {
    id = String(id || '');
    if (id.includes(':bin_sim:')) return 'bin_sim';
    if (id.includes(':file:')) return 'file';
    return 'func';
}
window.entityKindFromId = entityKindFromId;

async function showNotes(funcId, expand = true) {
    const isNewFunc = funcId !== currentNotesFuncId;
    currentNotesFuncId = funcId;
    // ponytail: the id carries the entity kind, so derive it instead of trusting
    // the sticky flag showFileNotes() sets (stale after navigating file -> function)
    entityMode = entityKindFromId(funcId);

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
    if (isGraphOpen && !isGraphLocked && funcId && entityMode === 'func') {
        loadSideGraph(funcId);
    }

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
    graphPanel.style.width = panelWidths.graph + 'px';
    graphPanel.style.right = -(panelWidths.graph + 50) + 'px';
    document.body.appendChild(graphPanel);

    // Notes Panel
    const notesPanel = document.createElement('div');
    notesPanel.id = 'notes-panel-v2';
    notesPanel.className = 'side-panel-v2';
    notesPanel.style.width = panelWidths.notes + 'px';
    notesPanel.style.right = -(panelWidths.notes + 50) + 'px';
    document.body.appendChild(notesPanel);

    // AI Panel
    const aiPanel = document.createElement('div');
    aiPanel.id = 'ai-panel-v2';
    aiPanel.className = 'side-panel-v2';
    aiPanel.style.width = panelWidths.ai + 'px';
    aiPanel.style.right = -(panelWidths.ai + 50) + 'px';
    document.body.appendChild(aiPanel);

    renderGraphPanelHTML(graphPanel);
    renderNotesPanelHTML(notesPanel);
    renderAIPanelHTML(aiPanel);

    setupPanelResize(graphPanel, 'graph');
    setupPanelResize(notesPanel, 'notes');
    setupPanelResize(aiPanel, 'ai');

    // Enter-to-send only needs wiring once -- these inputs are never
    // recreated -- rather than every time showNotes() happens to run.
    setupInputListeners();
}

/** Drag handle on a panel's left edge. Width is clamped and persisted so a
 * resize survives reloads (localStorage, per panel key). */
function setupPanelResize(panel, key) {
    const handle = document.createElement('div');
    handle.className = 'panel-resize-handle';
    handle.title = 'Drag or use arrow keys to resize';
    handle.tabIndex = 0;
    handle.setAttribute('role', 'separator');
    handle.setAttribute('aria-orientation', 'vertical');
    handle.setAttribute('aria-label', `Resize ${key} panel`);
    panel.appendChild(handle);

    const setWidth = (width) => {
        const viewportMax = Math.max(PANEL_MIN_WIDTHS[key], window.innerWidth - 48);
        panelWidths[key] = Math.min(PANEL_MAX_WIDTH, viewportMax, Math.max(PANEL_MIN_WIDTHS[key], width));
        panel.style.width = panelWidths[key] + 'px';
        updateLayout();
    };

    handle.addEventListener('pointerdown', (e) => {
        e.preventDefault();
        const startX = e.clientX;
        const startWidth = panelWidths[key];
        document.body.classList.add('panel-resizing');
        handle.setPointerCapture(e.pointerId);

        const onMove = (ev) => setWidth(startWidth + startX - ev.clientX);
        const onUp = () => {
            handle.removeEventListener('pointermove', onMove);
            handle.removeEventListener('pointerup', onUp);
            handle.removeEventListener('pointercancel', onUp);
            document.body.classList.remove('panel-resizing');
            localStorage.setItem(`bsimvis-panel-width-${key}`, String(panelWidths[key]));
        };
        handle.addEventListener('pointermove', onMove);
        handle.addEventListener('pointerup', onUp);
        handle.addEventListener('pointercancel', onUp);
    });

    handle.addEventListener('keydown', (e) => {
        if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
        e.preventDefault();
        setWidth(panelWidths[key] + (e.key === 'ArrowLeft' ? 20 : -20));
        localStorage.setItem(`bsimvis-panel-width-${key}`, String(panelWidths[key]));
    });
}

function renderNotesPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <h3 style="margin: 0; font-size: 0.9rem; color: var(--note-accent);"><i class="fa-solid fa-comments"></i> Notes <span id="notes-scope-label" style="font-weight:400; color:var(--subtle); font-size:0.75rem;"></span></h3>
            <button onclick="closeNotesPanel()" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='var(--text)'" onmouseout="this.style.color='var(--subtle)'"><i class="fa-solid fa-xmark"></i></button>
        </div>
        <div id="notes-column" style="flex: 1; display: flex; flex-direction: column; position: relative; overflow: hidden;">
            <div id="notes-drop-overlay" style="display:none; position:absolute; top:0; left:0; width:100%; height:100%; background: color-mix(in srgb, var(--note-accent) 10%, transparent); border: 2px dashed var(--note-accent); z-index: 100; pointer-events: none; align-items: center; justify-content: center; flex-direction: column; color: var(--note-accent); font-weight: bold; font-size: 1.2rem; backdrop-filter: blur(2px);">
                <i class="fa-solid fa-plus-circle" style="font-size: 3rem; margin-bottom: 10px;"></i>
                Drop to Save Note
            </div>
            <div id="notes-list" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; background: var(--bg);">
                <div style="text-align: center; color: var(--subtle); padding: 20px;">Loading notes...</div>
            </div>
            <div style="padding: 16px; background: var(--meta-bg); border-top: 1px solid var(--border);">
                <textarea id="new-note-text" placeholder="Add a new note (Markdown)..." style="width: 100%; min-height: 80px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font: inherit; font-size: 0.85rem; outline: none;"></textarea>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <select id="note-owner-select" style="background: var(--bg); color: var(--meta-text-muted); border: 1px solid var(--border); border-radius: 4px; padding: 4px 8px; font-size: 0.8rem; outline: none;">
                        <option value="user">User</option>
                        <option value="llm">LLM</option>
                    </select>
                    <button onclick="saveNote(currentNotesFuncId)" class="note-primary-btn">Add Note</button>
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
        btn.style.borderColor = isPoolScope ? 'var(--info)' : 'var(--border)';
        btn.style.color = isPoolScope ? 'var(--info)' : 'var(--meta-text)';
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
            <div id="pivotick-side-legend" style="position:absolute; bottom:8px; left:8px; z-index:100; display:flex; flex-direction:column; gap:3px; background:color-mix(in srgb, var(--meta-bg) 92%, transparent); backdrop-filter:blur(4px); padding:6px 10px; border-radius:6px; border:1px solid var(--border); font-size:0.65rem; color:var(--meta-text-muted);">
                ${typeof FunctionView !== 'undefined' ? FunctionView.renderLegendHTML() : ''}
            </div>
        </div>
    `;
}

function renderAIPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <span style="display: flex; align-items: center; gap: 8px; font-size: 0.9rem; color: var(--info); font-weight: bold;"><i class="fa-solid fa-robot"></i> AI Insight</span>
            <div style="display: flex; align-items: center; gap: 12px;">
                <div id="llm-status" aria-live="polite"></div>
                <button onclick="closeAIPanel()" style="background: none; border: none; color: var(--subtle); cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='var(--text)'" onmouseout="this.style.color='var(--subtle)'"><i class="fa-solid fa-xmark"></i></button>
            </div>
        </div>
        <div id="llm-chat-history" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 14px; background: var(--card-bg);"></div>
        <div style="padding: 16px; background: var(--meta-bg); border-top: 1px solid var(--border);">
            <textarea id="llm-input" placeholder="Message AI Insight (Enter to send, Shift+Enter for a new line)" style="width: 100%; min-height: 80px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 6px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font: inherit; font-size: 0.88rem; outline: none;"></textarea>
            <div style="display: flex; justify-content: flex-end; gap: 10px;">
                <button id="llm-stop-btn" onclick="stopLLMGeneration()" class="llm-stop-btn" style="display:none;">Stop</button>
                <button id="llm-send-btn" onclick="sendLLMChat()" class="llm-send-btn">Send</button>
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
        totalOffset += panelWidths.ai;
    } else {
        if (aiPanel) aiPanel.style.right = -(panelWidths.ai + 50) + 'px';
    }

    // Notes is to the left of AI
    if (isNotesOpen) {
        notesPanel.style.right = (isAIOpen ? panelWidths.ai : 0) + 'px';
        totalOffset += panelWidths.notes;
    } else {
        if (notesPanel) notesPanel.style.right = -(panelWidths.notes + 50) + 'px';
    }

    // Graph is to the left of Notes
    if (isGraphOpen) {
        if (graphPanel) graphPanel.style.right = totalOffset + 'px';
        totalOffset += panelWidths.graph;
    } else {
        if (graphPanel) graphPanel.style.right = -(panelWidths.graph + 50) + 'px';
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
    // Opened from the rail handle rather than an entity's note button. The view
    // usually is about something -- a comparison is about its pair -- so adopt
    // that instead of showing an empty panel whose Add Note button 400s.
    if (!currentNotesFuncId) {
        const fallback = (window.defaultNoteEntityId || window.parent?.defaultNoteEntityId)?.();
        // showNotes sets currentNotesFuncId before it calls back in here, so
        // this cannot loop.
        if (fallback) { showNotes(fallback); return; }
    }
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
    setTimeout(() => document.getElementById('llm-input')?.focus(), 100);

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

    if (llmInput) {
        llmInput.onkeydown = (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendLLMChat();
            } else if (e.key === 'Tab' && notesInput) {
                e.preventDefault();
                notesInput.focus();
            }
        };
    }

    if (notesInput && llmInput) {
        notesInput.onkeydown = (e) => {
            if (e.key === 'Tab') {
                e.preventDefault();
                llmInput.focus();
            }
        };
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
        body.panel-resizing, body.panel-resizing .side-panel-v2 { transition: none !important; }

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

        .panel-resize-handle {
            position: absolute;
            top: 0; left: -4px;
            width: 8px; height: 100%;
            cursor: ew-resize;
            z-index: 10001;
            background: transparent;
            touch-action: none;
        }
        .panel-resize-handle:hover, .panel-resize-handle:focus-visible, body.panel-resizing .panel-resize-handle { background: var(--accent); opacity: 0.4; }

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
        .panel-handle.user.active { color: var(--note-accent); border-color: var(--note-accent); background: var(--meta-bg); }
        .panel-handle.ai.active { color: var(--info); border-color: var(--info); background: var(--meta-bg); }
        .panel-handle i { font-size: 0.9rem; transform: rotate(90deg); }

        .note-markdown-body, .llm-markdown-body {
            font-family: inherit;
            font-size: 0.88rem;
            line-height: 1.6;
            color: var(--meta-text);
        }
        .note-markdown-body p, .llm-markdown-body p { margin-top: 0; margin-bottom: 10px; }
        .note-markdown-body p:last-child, .llm-markdown-body p:last-child { margin-bottom: 0; }
        .note-markdown-body code, .llm-markdown-body code { font-family: 'Fira Code', monospace; background: var(--border); padding: 2px 5px; border-radius: 3px; font-size: 0.85em; }
        .note-markdown-body code { color: var(--note-accent); }
        .llm-markdown-body code { color: var(--info); }
        .note-markdown-body pre, .llm-markdown-body pre { font-family: 'Fira Code', monospace; background: var(--bg); padding: 12px; border-radius: 6px; overflow-x: auto; border: 1px solid var(--border); margin: 10px 0; }
        .note-markdown-body pre code, .llm-markdown-body pre code { background: none; padding: 0; color: inherit; }
        .note-markdown-body blockquote { border-left: 3px solid var(--note-accent); margin: 10px 0; padding-left: 12px; color: var(--meta-text-muted); }
        .llm-markdown-body blockquote { border-left: 3px solid var(--info); margin: 10px 0; padding-left: 12px; color: var(--meta-text-muted); }

        .chat-msg { border-radius: 12px; padding: 10px 14px; max-width: 88%; }
        .chat-msg.user { background: color-mix(in srgb, var(--note-accent) 10%, var(--meta-bg)); align-self: flex-end; }
        .chat-msg.ai { background: transparent; align-self: flex-start; border: 0; max-width: 100%; padding-left: 0; padding-right: 0; }
        .chat-msg-role { font-size: 0.65rem; font-weight: bold; letter-spacing: 0.5px; text-transform: uppercase; margin-bottom: 6px; opacity: 0.7; }
        .chat-msg.user .chat-msg-role { color: var(--note-accent); text-align: right; }
        .chat-msg.ai .chat-msg-role { color: var(--info); }

        .collapsible-container { position: relative; }
        .collapsible-content { overflow: hidden; transition: max-height 0.3s ease-out; }
        .collapsible-content.collapsed { max-height: 200px; mask-image: linear-gradient(to bottom, black 70%, transparent 100%); -webkit-mask-image: linear-gradient(to bottom, black 70%, transparent 100%); }
        .toggle-expand-btn { background: none; border: none; color: var(--note-accent); cursor: pointer; font-size: 0.75rem; font-weight: bold; padding: 5px 0; display: flex; align-items: center; gap: 5px; }
        .chat-msg.ai .toggle-expand-btn { color: var(--info); }

        /* Agent "thinking process" trace */
        .llm-trace { margin-bottom: 10px; }
        .llm-trace > summary { cursor: pointer; font-size: 0.75rem; color: var(--dim); padding: 4px 0; user-select: none; }
        .llm-trace > summary:hover { color: var(--meta-text); }
        .llm-trace-steps { margin-top: 4px; display: flex; flex-direction: column; gap: 4px; padding-left: 4px; border-left: 2px solid var(--border); }
        .llm-trace-step { padding: 4px 0 4px 10px; }
        .llm-trace-step > summary { cursor: pointer; font-size: 0.75rem; color: var(--meta-text-muted); list-style: none; }
        .llm-trace-step > summary:hover { color: var(--meta-text); }
        .llm-trace-step > summary::-webkit-details-marker { display: none; }
        .llm-trace-step[open] > summary { color: var(--meta-text); }
        .llm-trace-body { margin-top: 8px; font-size: 0.75rem; display: flex; flex-direction: column; gap: 8px; }
        .llm-trace-label { font-family: 'Inter', sans-serif; font-size: 0.65rem; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; color: var(--subtle); margin-bottom: 3px; }
        .llm-trace-body pre { margin: 0; white-space: pre-wrap; overflow-x: auto; background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 8px; }
        .llm-trace-body pre.result { max-height: 200px; overflow-y: auto; }
        .llm-trace-curl { display: flex; align-items: stretch; gap: 6px; }
        .llm-trace-curl pre { flex: 1; margin: 0; user-select: all; }
        .llm-trace-copy-btn { flex-shrink: 0; background: var(--meta-bg); color: var(--meta-text); border: 1px solid var(--border); border-radius: 4px; padding: 0 10px; cursor: pointer; font-size: 0.72rem; }
        .llm-trace-copy-btn:hover { background: var(--border); }

        #llm-status { max-width: min(50%, 280px); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 0.75rem; color: var(--info); font-variant-numeric: tabular-nums; font-weight: normal; text-transform: none; }
        .note-primary-btn, .llm-send-btn, .llm-stop-btn { border: 0; padding: 6px 16px; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 0.85rem; }
        .note-primary-btn { background: var(--note-accent); color: var(--window-bg); }
        .llm-send-btn { background: var(--info); color: var(--bg); }
        .llm-stop-btn { background: var(--token-instruction); color: var(--bg); }

        .save-note-btn { background: var(--meta-bg); color: var(--meta-text); border: 1px solid var(--border); padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; font-weight: bold; }
        .save-note-btn:hover:not(:disabled) { border-color: var(--note-accent); color: var(--note-accent); }
        .save-note-btn:disabled { opacity: 0.6; cursor: default; }

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

/** What the notes panel is attached to, shown next to its title. A pair sid is
 * unreadable on its own, so the comparison view supplies "A vs B" for it. */
function noteScopeLabel(funcId) {
    const id = String(funcId || '');
    if (!id) return '';
    if (entityMode === 'bin_sim') {
        const names = (window.binSimPairNames || window.parent?.binSimPairNames)?.(id);
        return names ? `\u00b7 ${names}` : '\u00b7 binary comparison';
    }
    if (entityMode === 'file') return `\u00b7 file ${id.split(':file:')[1] || ''}`;
    return '';
}

async function refreshNotes(funcId) {
    const listEl = document.getElementById('notes-list');
    if (!listEl) return;
    const scopeEl = document.getElementById('notes-scope-label');
    if (scopeEl) scopeEl.textContent = noteScopeLabel(funcId);
    const collection = window.getCollectionFromId(funcId);
    const mode = noteMode();
    const idParam = `${mode.idKey}=${encodeURIComponent(funcId)}`;
    const endpoint = `${mode.base}/list`;
    try {
        const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
        const res = await fetch(`${endpoint}?${apiParams}&${idParam}`);
        const data = await res.json();
        if (data.status === 'success') {
            lastRenderedNotesFuncId = funcId;
            const notes = data.notes || [];
            // The comparison view's badge reads a cached diff doc, so hand it the
            // list we just fetched rather than let it show a stale count.
            if (entityMode === 'bin_sim') {
                (window.refreshBinSimRow || window.parent?.refreshBinSimRow)?.(funcId, notes);
            }
            
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
                            <div class="note-item editing" style="background: var(--meta-bg); border-radius: 6px; padding: 15px; border-left: 4px solid var(--note-accent); border: 1px solid var(--note-accent); border-left-width: 4px;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                    <span style="font-size: 0.7rem; font-weight: bold; color: var(--note-accent); text-transform: uppercase;">Editing Note</span>
                                </div>
                                <textarea id="edit-note-text-${escapeAttr(note.id)}" 
                                    onkeydown="if(event.key==='Enter' && !event.shiftKey){event.preventDefault(); submitEditNote(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(note.id))});} if(event.key==='Escape'){cancelEditNote(${escapeAttr(jsString(funcId))});}"
                                    style="width: 100%; min-height: 100px; background: var(--bg); border: 1px solid var(--border); color: var(--meta-text); padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;">${escapeHtml(note.text)}</textarea>
                                <div style="display: flex; justify-content: flex-end; gap: 10px;">
                                    <button onclick="cancelEditNote(${escapeAttr(jsString(funcId))})" style="background: var(--border); color: var(--meta-text-muted); border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.75rem;">Cancel</button>
                                    <button onclick="submitEditNote(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(note.id))})" style="background: var(--note-accent); color: var(--window-bg); border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.75rem;">Save</button>
                                </div>
                            </div>
                        `;
                    }

                    const renderedText = renderNoteMarkdown(note.text);
                    const noteAccent = isAI ? 'var(--info)' : 'var(--note-accent)';
                    return `
                        <div class="note-item" style="background: var(--meta-bg); border-radius: 6px; padding: 15px; border-left: 4px solid ${noteAccent}; border: 1px solid var(--border); border-left-width: 4px;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                <span style="font-size: 0.7rem; font-weight: bold; color: ${noteAccent}; text-transform: uppercase;">${escapeHtml(note.owner)}</span>
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
    if (!requireNoteTarget(funcId)) return;
    const textEl = document.getElementById('new-note-text');
    const ownerEl = document.getElementById('note-owner-select');
    const text = textEl.value.trim();
    if (!text) return;
    const mode = noteMode();
    const endpoint = `${mode.base}/add`;
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [mode.idKey]: funcId, text, owner: ownerEl.value };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            textEl.value = '';
            await refreshNotes(funcId);
            notifyNoteChanged(funcId);
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
    if (!requireNoteTarget(funcId)) return;
    const textEl = document.getElementById(`edit-note-text-${noteId}`);
    const text = textEl.value.trim();
    if (!text) return;

    const mode = noteMode();
    const endpoint = `${mode.base}/update`;
    try {
        const pool = getActivePool();
        const payload = {
            collection: window.getCollectionFromId(funcId),
            [mode.idKey]: funcId,
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
            notifyNoteChanged(funcId);
        } else {
            alert(data.error || 'Failed to update note');
        }
    } catch (e) {
        alert(e.message);
    }
}

async function deleteNote(funcId, note_id) {
    if (!requireNoteTarget(funcId)) return;
    if (!confirm('Delete note?')) return;
    const mode = noteMode();
    const endpoint = `${mode.base}/remove`;
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [mode.idKey]: funcId, note_id };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
            notifyNoteChanged(funcId);
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
    if (entityMode === 'bin_sim') {
        if (statusEl) statusEl.innerText = 'Use "Analyze comparison" on the comparison view to generate a pair report note.';
        return;
    }
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

// Human-readable one-liner per tool, from its arguments -- what the trace
// summary shows collapsed, before the analyst expands it.
const TOOL_CALL_LABELS = {
    get_function: (a) => `Looked up function ${a.func_id || ''}`,
    get_call_graph: (a) => `Checked call graph for ${a.func_id || ''}`,
    get_similar_functions: (a) => `Searched for similar functions (min_score=${a.min_score ?? 0.9})`,
    search_functions: (a) => `Searched for function with "${a.filters_qs || ''}"`,
    search_tags: (a) => `Searched tags for "${a.q || ''}"`,
    get_file_info: (a) => `Looked up file ${a.file_md5 || ''}`,
    get_cluster_info: (a) => `Looked up cluster ${a.cluster_id || ''}`,
};

function toolCallLabel(tc) {
    const fn = TOOL_CALL_LABELS[tc.name];
    try { return fn ? fn(tc.arguments || {}) : tc.name; } catch { return tc.name; }
}

// Ready-to-paste curl for the tool's real HTTP endpoint (backend-supplied
// method/path/query), or null for tools with no single public endpoint.
function buildCurl(apiCall) {
    if (!apiCall) return null;
    const params = new URLSearchParams();
    Object.entries(apiCall.query || {}).forEach(([k, v]) => {
        if (v !== undefined && v !== null) params.set(k, v);
    });
    const qs = params.toString();
    const url = `${window.location.origin}${apiCall.path}${qs ? '?' + qs : ''}`;
    return `curl '${url}'`;
}

function prettyJson(value) {
    try { return JSON.stringify(typeof value === 'string' ? JSON.parse(value) : value, null, 2); }
    catch { return String(value); }
}

/** Collapsed-by-default "thinking process": every tool call the agent made
 * for this reply, each expandable to its arguments, result, and a curl
 * command for the real API endpoint it corresponds to. */
function renderToolTrace(toolCalls) {
    if (!toolCalls || !toolCalls.length) return '';
    const items = toolCalls.map(tc => {
        const curl = buildCurl(tc.api_call);
        return `
            <details class="llm-trace-step">
                <summary><i class="fa-solid fa-wrench"></i> ${escapeHtml(toolCallLabel(tc))}</summary>
                <div class="llm-trace-body">
                    <div><div class="llm-trace-label">Arguments</div><pre>${escapeHtml(prettyJson(tc.arguments || {}))}</pre></div>
                    <div><div class="llm-trace-label">Result</div><pre class="result">${escapeHtml(prettyJson(tc.result_preview || ''))}</pre></div>
                    ${curl ? `<div><div class="llm-trace-label">API call</div>
                        <div class="llm-trace-curl">
                            <pre>${escapeHtml(curl)}</pre>
                            <button onclick="copyToClipboard(${escapeAttr(jsString(curl))}, this)" class="llm-trace-copy-btn" title="Copy curl command" aria-label="Copy curl command"><i class="fa-solid fa-copy"></i></button>
                        </div>
                    </div>` : ''}
                </div>
            </details>`;
    }).join('');
    return `
        <details class="llm-trace">
            <summary>Thinking process · ${toolCalls.length} lookup${toolCalls.length === 1 ? '' : 's'}</summary>
            <div class="llm-trace-steps">${items}</div>
        </details>`;
}

function updateChatMessageUI(msgEl, content, index, chatKey) {
    const historyEl = document.getElementById("llm-chat-history");
    const isAtBottom = historyEl ? (historyEl.scrollHeight - historyEl.scrollTop <= historyEl.clientHeight + 5) : false;

    // Save to history object so re-renders don't lose progress
    const histEntry = chatKey && chatHistories[chatKey] && chatHistories[chatKey][index];
    if (histEntry) histEntry.content = content;

    let html = renderNoteMarkdown(content);
    const isLong = content.length > 500;
    const traceHtml = renderToolTrace(histEntry && histEntry.tool_calls);
    const roleLabel = msgEl.classList.contains('ai') ? 'AI Insight' : 'You';
    msgEl.innerHTML = `
        <div class="chat-msg-role">${roleLabel}</div>
        ${traceHtml}
        <div class="collapsible-container">
            <div class="llm-markdown-body collapsible-content">${html}</div>
            ${isLong ? '<button class="toggle-expand-btn" onclick="toggleContentExpand(this)"><i class="fa-solid fa-chevron-up"></i> Show Less</button>' : ''}
        </div>
    `;
    if (msgEl.classList.contains('ai') && content.trim().length > 0 && content !== "_investigating..._") {
        const actionsEl = document.createElement("div");
        actionsEl.style.cssText = "margin-top: 10px; display: flex; justify-content: flex-end; border-top: 1px solid var(--border); padding-top: 8px;";
        actionsEl.innerHTML = `<button onclick="saveMessageAsNote(${escapeAttr(jsString(currentNotesFuncId))}, ${Number(index)}, this)" class="save-note-btn"><i class="fa-solid fa-plus"></i> Save Note</button>`;
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
    if (entityMode === 'bin_sim') {
        // A raw sid tells the agent nothing; the comparison view knows the two
        // binaries and their scores, so let it describe its own focus.
        const ctx = (window.binSimFocusContext || window.parent?.binSimFocusContext)?.(currentNotesFuncId);
        return `(Analyst is currently viewing the binary comparison ${currentNotesFuncId}.${ctx ? ` ${ctx}` : ''} Assume this question refers to that comparison -- the two binaries and how they differ -- unless stated otherwise.)`;
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
    const startedAt = Date.now();
    let statusText = "Investigating";
    const updateStatus = () => {
        if (statusEl) statusEl.innerText = `${statusText} · ${Math.floor((Date.now() - startedAt) / 1000)}s`;
    };
    if (sendBtn) sendBtn.style.display = "none";
    if (stopBtn) stopBtn.style.display = "block";
    updateStatus();
    const thinkingTimer = setInterval(updateStatus, 1000);
    llmAbortController = new AbortController();
    const msgEl = addChatMessage(chatKey, "ai", "_investigating..._");
    const msgIndex = chatHistories[chatKey].length - 1;
    if (chatHistories[chatKey] && chatHistories[chatKey][msgIndex]) {
        chatHistories[chatKey][msgIndex].tool_calls = [];
    }
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
        if (!response.body) throw new Error(`HTTP ${response.status}`);
        // The server streams one NDJSON event per line -- a "tool_call" as
        // each lookup resolves, then a final "done" -- so the trace below
        // the placeholder grows live instead of appearing all at once after
        // the whole turn (which can take up to a minute) finishes.
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        let finalReply = null;
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            let nl;
            while ((nl = buf.indexOf("\n")) >= 0) {
                const line = buf.slice(0, nl);
                buf = buf.slice(nl + 1);
                if (!line.trim()) continue;
                const event = JSON.parse(line);
                if (event.type === "tool_call") {
                    const { type, ...call } = event;
                    if (chatHistories[chatKey] && chatHistories[chatKey][msgIndex]) {
                        chatHistories[chatKey][msgIndex].tool_calls.push(call);
                    }
                    statusText = toolCallLabel(call);
                    updateStatus();
                    updateChatMessageUI(msgEl, chatHistories[chatKey][msgIndex].content, msgIndex, chatKey);
                } else if (event.type === "error") {
                    throw new Error(event.error);
                } else if (event.type === "done") {
                    finalReply = event.reply || "_(no reply)_";
                }
            }
        }
        updateChatMessageUI(msgEl, finalReply ?? "_(no reply)_", msgIndex, chatKey);
    } catch (err) {
        updateChatMessageUI(msgEl, err.name === 'AbortError' ? "_Stopped._" : "Error: " + err.message, msgIndex, chatKey);
    } finally {
        clearInterval(thinkingTimer);
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
    if (!requireNoteTarget(funcId)) return;
    const collection = window.getCollectionFromId(funcId);
    const history = chatHistories[collection];
    if (!history || !history[index]) return;
    const mode = noteMode();
    const endpoint = `${mode.base}/add`;
    try {
        const pool = getActivePool();
        const payload = { collection, [mode.idKey]: funcId, text: history[index].content, owner: "llm" };
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
            notifyNoteChanged(funcId);
        }
    } catch (e) { alert(e.message); }
}

async function handleDroppedText(funcId, text) {
    if (!requireNoteTarget(funcId)) return;
    const mode = noteMode();
    const endpoint = `${mode.base}/add`;
    try {
        const pool = getActivePool();
        const payload = { collection: window.getCollectionFromId(funcId), [mode.idKey]: funcId, text: text, owner: 'llm' };
        if (pool) payload.pool = pool;
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
            notifyNoteChanged(funcId);
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
window.showBinSimNotePanel = function(id, e) { if (typeof showNotes === 'function') showNotes(id); };

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
/** A pair sid ends in an md5, so the plain tail would name one binary of two. */
function previewNoteLabel(id, mode) {
    if (mode !== 'bin_sim') return id.split(':').pop();
    const names = (window.binSimPairNames || window.parent?.binSimPairNames)?.(id);
    return names || 'binary comparison pair';
}

window.showNoteTooltip = async function(id, modeArg, e) {
    // Back-compat: callers used to pass a boolean (isFile).
    const mode = modeArg === true ? 'file' : modeArg === false ? 'func' : (modeArg || 'func');
    const tooltipModeInfo = NOTE_MODE_INFO[mode] || NOTE_MODE_INFO.func;
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
            <div class="preview-header">Notes: ${escapeHtml(previewNoteLabel(id, mode))}</div>
            <div class="note-preview-scroll" style="flex:1; overflow-y:auto; padding: 10px;">
                <div style="text-align: center; color: var(--subtle); font-style: italic; font-size: 0.8rem;">Loading notes...</div>
            </div>
        </div>
    `;

    const collection = (window.getCollectionFromId && window.getCollectionFromId(id)) || id.split(':')[0];
    const idParam = `${tooltipModeInfo.idKey}=${encodeURIComponent(id)}`;
    const endpoint = `${tooltipModeInfo.base}/list`;
    
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
