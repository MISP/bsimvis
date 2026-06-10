/**
 * Independent Notes and AI Insight Side Panels for BSimVis
 * Supports both function notes (/api/notes/*) and file notes (/api/notes/file/*).
 */

let currentNotesFuncId = null;
let lastRenderedNotesFuncId = null;
let lastRenderedAIFuncId = null;
let currentEditingNoteId = null;
let chatHistories = {}; 
let llmAbortController = null;

// 'func' for function notes, 'file' for file notes
let entityMode = 'func';

// Panel State
let isNotesOpen = false;
let isAIOpen = false;

const NOTES_WIDTH = 500;
const AI_WIDTH = 600;

async function showNotes(funcId, expand = true) {
    const isNewFunc = funcId !== currentNotesFuncId;
    currentNotesFuncId = funcId;
    
    // Ensure panels exist
    createPanelsIfMissing();
    
    // Expand if requested
    if (expand) {
        openNotesPanel();
        // only refresh AI if it's already open
        if (isAIOpen) openAIPanel();
    } else {
        updateLayout();
    }
    
    // Load data if new function or not yet rendered
    if (isNewFunc || lastRenderedNotesFuncId !== funcId) {
        await refreshNotes(funcId);
    }
    
    // Initialize LLM history object if missing
    if (!chatHistories[funcId]) {
        chatHistories[funcId] = [];
    }

    // Add key listeners
    setupInputListeners();
}

/** Entry point for file-level notes. Sets entityMode and delegates to showNotes. */
async function showFileNotes(fileId, expand = true) {
    entityMode = 'file';
    await showNotes(fileId, expand);
}

function createPanelsIfMissing() {
    if (document.getElementById('panel-handles-container')) return;

    injectNotesStyles();

    // Container for Handles
    const handleContainer = document.createElement('div');
    handleContainer.id = 'panel-handles-container';
    document.body.appendChild(handleContainer);

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

    renderNotesPanelHTML(notesPanel);
    renderAIPanelHTML(aiPanel);
}

function renderNotesPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <h3 style="margin: 0; font-size: 0.9rem; color: #ffd700;"><i class="fa-solid fa-comments"></i> Notes</h3>
            <button onclick="closeNotesPanel()" style="background: none; border: none; color: #888; cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#888'"><i class="fa-solid fa-xmark"></i></button>
        </div>
        <div id="notes-column" style="flex: 1; display: flex; flex-direction: column; position: relative; overflow: hidden;">
            <div id="notes-drop-overlay" style="display:none; position:absolute; top:0; left:0; width:100%; height:100%; background: rgba(255, 215, 0, 0.1); border: 2px dashed #ffd700; z-index: 100; pointer-events: none; align-items: center; justify-content: center; flex-direction: column; color: #ffd700; font-weight: bold; font-size: 1.2rem; backdrop-filter: blur(2px);">
                <i class="fa-solid fa-plus-circle" style="font-size: 3rem; margin-bottom: 10px;"></i>
                Drop to Save Note
            </div>
            <div id="notes-list" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; background: #121212;">
                <div style="text-align: center; color: #888; padding: 20px;">Loading notes...</div>
            </div>
            <div style="padding: 16px; background: #1a1a1a; border-top: 1px solid #333;">
                <textarea id="new-note-text" placeholder="Add a new note (Markdown)..." style="width: 100%; min-height: 80px; background: #0a0a0a; border: 1px solid #444; color: #eee; padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;"></textarea>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <select id="note-owner-select" style="background: #0a0a0a; color: #ccc; border: 1px solid #444; border-radius: 4px; padding: 4px 8px; font-size: 0.8rem; outline: none;">
                        <option value="user">User</option>
                        <option value="llm">LLM</option>
                    </select>
                    <button onclick="saveNote(currentNotesFuncId)" style="background: var(--accent, #ffd700); color: #000; border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Add Note</button>
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

function renderAIPanelHTML(el) {
    el.innerHTML = `
        <div class="panel-v2-header">
            <span style="display: flex; align-items: center; gap: 8px; font-size: 0.9rem; color: #ae81ff; font-weight: bold;"><i class="fa-solid fa-robot"></i> AI Insight</span>
            <div style="display: flex; align-items: center; gap: 12px;">
                <div id="llm-status" style="font-size: 0.75rem; color: #ae81ff; font-style: italic; font-weight: normal; text-transform: none;"></div>
                <button onclick="closeAIPanel()" style="background: none; border: none; color: #888; cursor: pointer; font-size: 1.1rem; padding: 4px; display: flex; align-items: center; transition: color 0.2s;" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#888'"><i class="fa-solid fa-xmark"></i></button>
            </div>
        </div>
        <div id="llm-chat-history" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 20px; background: #0f0f0f;"></div>
        <div style="padding: 16px; background: #1a1a1a; border-top: 1px solid #333;">
            <textarea id="llm-input" placeholder="Ask AI about this function..." style="width: 100%; min-height: 80px; background: #0a0a0a; border: 1px solid #444; color: #eee; padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;"></textarea>
            <div style="display: flex; justify-content: flex-end; gap: 10px;">
                <button id="llm-stop-btn" onclick="stopLLMGeneration()" style="display:none; background: #f44336; color: #fff; border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Stop</button>
                <button id="llm-send-btn" onclick="sendLLMChat()" style="background: #ae81ff; color: #000; border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Send</button>
            </div>
        </div>
    `;
}

function updateLayout() {
    const notesPanel = document.getElementById('notes-panel-v2');
    const aiPanel = document.getElementById('ai-panel-v2');
    
    let totalOffset = 0;
    
    // AI is on the far right
    if (isAIOpen) {
        aiPanel.style.right = '0';
        totalOffset += AI_WIDTH;
    } else {
        aiPanel.style.right = -(AI_WIDTH + 50) + 'px';
    }
    
    // Notes is to the left of AI
    if (isNotesOpen) {
        notesPanel.style.right = (isAIOpen ? AI_WIDTH : 0) + 'px';
        totalOffset += NOTES_WIDTH;
    } else {
        notesPanel.style.right = -(NOTES_WIDTH + 50) + 'px';
    }
    
    document.body.style.paddingRight = totalOffset + 'px';
    
    // Move handles container with panels
    const handleContainer = document.getElementById('panel-handles-container');
    if (handleContainer) {
        handleContainer.style.right = totalOffset + 'px';
        
        // Hide handles in dashboard view if both collapsed
        const isDashboard = !!document.getElementById('nav-collections');
        if (isDashboard && !isNotesOpen && !isAIOpen) {
            handleContainer.style.opacity = '0';
            handleContainer.style.pointerEvents = 'none';
        } else {
            handleContainer.style.opacity = '1';
            handleContainer.style.pointerEvents = 'auto';
        }
    }
    
    // Update handle active states
    document.getElementById('notes-panel-handle').classList.toggle('active', isNotesOpen);
    document.getElementById('ai-panel-handle').classList.toggle('active', isAIOpen);
}

function toggleNotesPanel() { if (isNotesOpen) closeNotesPanel(); else openNotesPanel(); }
function toggleAIPanel() { if (isAIOpen) closeAIPanel(); else openAIPanel(); }

function openNotesPanel() { 
    isNotesOpen = true; 
    updateLayout(); 
    if (currentNotesFuncId && lastRenderedNotesFuncId !== currentNotesFuncId) {
        refreshNotes(currentNotesFuncId);
    }
}
function closeNotesPanel() { isNotesOpen = false; updateLayout(); }
function openAIPanel() { 
    isAIOpen = true; 
    updateLayout(); 
    
    // Trigger summary if empty
    if (currentNotesFuncId && (!chatHistories[currentNotesFuncId] || chatHistories[currentNotesFuncId].length === 0)) {
        chatHistories[currentNotesFuncId] = [];
        generateSummary(currentNotesFuncId);
    } else if (currentNotesFuncId && lastRenderedAIFuncId !== currentNotesFuncId) {
        renderChatHistory(currentNotesFuncId);
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
            background: #1e1e1e;
            border-left: 1px solid #333;
            box-shadow: -5px 0 20px rgba(0,0,0,0.4);
            z-index: 10000;
            display: flex;
            flex-direction: column;
            transition: right 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            font-family: 'Inter', sans-serif;
        }

        .panel-v2-header {
            padding: 12px 16px; 
            background: #252525; 
            border-bottom: 1px solid #333; 
            display: flex; 
            justify-content: space-between; 
            align-items: center;
        }

        .panel-handle {
            background: #252525;
            border: 1px solid #444;
            border-right: none;
            color: #888;
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
            box-shadow: -2px 0 10px rgba(0,0,0,0.3);
        }
        .panel-handle:hover { color: #fff; background: #333; }
        .panel-handle.user.active { color: #ffd700; border-color: #ffd700; background: #1a1a1a; }
        .panel-handle.ai.active { color: #ae81ff; border-color: #ae81ff; background: #1a1a1a; }
        .panel-handle i { font-size: 0.9rem; transform: rotate(90deg); }

        .note-markdown-body, .llm-markdown-body { 
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem; 
            line-height: 1.6; 
            color: #eee; 
        }
        .note-markdown-body p, .llm-markdown-body p { margin-top: 0; margin-bottom: 12px; }
        .note-markdown-body code { background: #333; padding: 2px 5px; border-radius: 3px; color: #ffd700; font-weight: bold; }
        .llm-markdown-body code { background: #333; padding: 2px 5px; border-radius: 3px; color: #ae81ff; font-weight: bold; }
        .note-markdown-body pre, .llm-markdown-body pre { background: #0a0a0a; padding: 12px; border-radius: 6px; overflow-x: auto; border: 1px solid #333; margin: 12px 0; }
        .note-markdown-body blockquote { border-left: 4px solid #ffd700; margin: 12px 0; padding-left: 15px; color: #aaa; font-style: italic; background: rgba(255, 215, 0, 0.05); }
        .llm-markdown-body blockquote { border-left: 4px solid #ae81ff; margin: 12px 0; padding-left: 15px; color: #aaa; font-style: italic; background: rgba(174, 129, 255, 0.05); }
        
        .chat-msg { border-radius: 8px; padding: 12px 16px; max-width: 95%; position: relative; }
        .chat-msg.user { background: #252525; align-self: flex-end; border-bottom-right-radius: 0; border: 1px solid #444; }
        .chat-msg.ai { background: #1a1a1a; align-self: flex-start; border-bottom-left-radius: 0; border: 1px solid #333; }
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
    const collection = funcId.split(':')[0];
    const isFile = entityMode === 'file';
    const idParam = isFile ? `file_id=${encodeURIComponent(funcId)}` : `func_id=${encodeURIComponent(funcId)}`;
    const endpoint = isFile ? '/api/notes/file/list' : '/api/notes/list';
    try {
        const res = await fetch(`${endpoint}?collection=${encodeURIComponent(collection)}&${idParam}`);
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
                listEl.innerHTML = '<div style="text-align: center; color: #555; padding: 40px; font-style: italic;">No notes yet.</div>';
            } else {
                listEl.innerHTML = notes.map(note => {
                    const isAI = note.owner === 'llm' || note.owner === 'AI';
                    const isEditing = currentEditingNoteId === note.id;
                    
                    if (isEditing) {
                        return `
                            <div class="note-item editing" style="background: #1a1a1a; border-radius: 6px; padding: 15px; border-left: 4px solid #ffd700; border: 1px solid #ffd700; border-left-width: 4px;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                    <span style="font-size: 0.7rem; font-weight: bold; color: #ffd700; text-transform: uppercase;">Editing Note</span>
                                </div>
                                <textarea id="edit-note-text-${note.id}" 
                                    onkeydown="if(event.key==='Enter' && !event.shiftKey){event.preventDefault(); submitEditNote('${funcId}', '${note.id}');} if(event.key==='Escape'){cancelEditNote('${funcId}');}"
                                    style="width: 100%; min-height: 100px; background: #0a0a0a; border: 1px solid #444; color: #eee; padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', monospace; font-size: 0.85rem; outline: none;">${note.text}</textarea>
                                <div style="display: flex; justify-content: flex-end; gap: 10px;">
                                    <button onclick="cancelEditNote('${funcId}')" style="background: #333; color: #ccc; border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.75rem;">Cancel</button>
                                    <button onclick="submitEditNote('${funcId}', '${note.id}')" style="background: #ffd700; color: #000; border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.75rem;">Save</button>
                                </div>
                            </div>
                        `;
                    }

                    const renderedText = (typeof marked !== 'undefined') ? marked.parse(note.text) : note.text;
                    return `
                        <div class="note-item" style="background: #1a1a1a; border-radius: 6px; padding: 15px; border-left: 4px solid ${isAI ? '#ae81ff' : '#ffd700'}; border: 1px solid #333; border-left-width: 4px;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                <span style="font-size: 0.7rem; font-weight: bold; color: ${isAI ? '#ae81ff' : '#ffd700'}; text-transform: uppercase;">${note.owner}</span>
                                <span style="font-size: 0.6rem; color: #666;">${new Date(note.timestamp).toLocaleString()}</span>
                            </div>
                            <div class="collapsible-container">
                                <div class="note-text note-markdown-body collapsible-content ${note.text.length > 500 ? 'collapsed' : ''}">${renderedText}</div>
                                ${note.text.length > 500 ? '<button class="toggle-expand-btn" onclick="toggleContentExpand(this)"><i class="fa-solid fa-chevron-down"></i> Show More</button>' : ''}
                            </div>
                            <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 10px;">
                                <button onclick="startEditNote('${note.id}', '${funcId}')" title="Edit Note" style="background: none; border: none; color: #555; cursor: pointer; font-size: 0.85rem;"><i class="fa-solid fa-pen"></i></button>
                                <button onclick="deleteNote('${funcId}', '${note.id}')" style="background: none; border: none; color: #555; cursor: pointer;"><i class="fa-solid fa-trash"></i></button>
                            </div>
                        </div>
                    `;
                }).join('');
            }
        }
    } catch (e) { console.error(e); }
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
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: funcId.split(':')[0], [idKey]: funcId, text, owner: ownerEl.value })
        });
        if ((await res.json()).status === 'success') {
            textEl.value = '';
            await refreshNotes(funcId);
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
        const res = await fetch(endpoint, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                collection: funcId.split(':')[0],
                [idKey]: funcId,
                note_id: noteId,
                text: text
            })
        });
        const data = await res.json();
        if (data.status === 'success') {
            currentEditingNoteId = null;
            await refreshNotes(funcId);
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
        const res = await fetch(endpoint, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: funcId.split(':')[0], [idKey]: funcId, note_id })
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
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

    llmAbortController = new AbortController();
    try {
        const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
            signal: llmAbortController.signal
        });
        const msgEl = addChatMessage(funcId, "ai", "");
        const msgIndex = chatHistories[funcId].length - 1;
        const summary = await readStream(response, (text) => updateChatMessageUI(msgEl, text, msgIndex, funcId));
    } catch (err) {
        if (err.name !== 'AbortError') addChatMessage(funcId, "ai", "Error: " + err.message);
    } finally {
        if (statusEl) statusEl.innerText = "";
        if (sendBtn) sendBtn.style.display = "block";
        if (stopBtn) stopBtn.style.display = "none";
    }
}

function renderChatHistory(funcId) {
    const historyEl = document.getElementById("llm-chat-history");
    if (!historyEl) return;
    historyEl.innerHTML = "";
    (chatHistories[funcId] || []).forEach((msg, index) => {
        const msgEl = document.createElement("div");
        msgEl.className = `chat-msg ${msg.role === "assistant" ? "ai" : "user"}`;
        updateChatMessageUI(msgEl, msg.content, index, funcId);
        historyEl.appendChild(msgEl);
    });
    historyEl.scrollTop = historyEl.scrollHeight;
    lastRenderedAIFuncId = funcId;
}

function addChatMessage(funcId, role, content) {
    const historyEl = document.getElementById("llm-chat-history");
    if (!historyEl) return;
    const isAtBottom = historyEl.scrollHeight - historyEl.scrollTop <= historyEl.clientHeight + 5;
    
    if (!chatHistories[funcId]) chatHistories[funcId] = [];
    const index = chatHistories[funcId].length;
    chatHistories[funcId].push({ role: role === "ai" ? "assistant" : "user", content: content });

    const msgEl = document.createElement("div");
    msgEl.className = `chat-msg ${role}`;
    updateChatMessageUI(msgEl, content, index, funcId);
    historyEl.appendChild(msgEl);
    
    if (isAtBottom || role === 'user') historyEl.scrollTop = historyEl.scrollHeight;
    return msgEl;
}

function updateChatMessageUI(msgEl, content, index, funcId) {
    const historyEl = document.getElementById("llm-chat-history");
    const isAtBottom = historyEl ? (historyEl.scrollHeight - historyEl.scrollTop <= historyEl.clientHeight + 5) : false;
    
    // Save to history object so re-renders don't lose progress
    if (funcId && chatHistories[funcId] && chatHistories[funcId][index]) {
        chatHistories[funcId][index].content = content;
    }

    let html = (typeof marked !== "undefined") ? marked.parse(content) : content;
    const isLong = content.length > 500;
    msgEl.innerHTML = `
        <div class="collapsible-container">
            <div class="llm-markdown-body collapsible-content">${html}</div>
            ${isLong ? '<button class="toggle-expand-btn" onclick="toggleContentExpand(this)"><i class="fa-solid fa-chevron-up"></i> Show Less</button>' : ''}
        </div>
    `;
    if (msgEl.classList.contains('ai') && content.trim().length > 0) {
        const actionsEl = document.createElement("div");
        actionsEl.style.cssText = "margin-top: 10px; display: flex; justify-content: flex-end; border-top: 1px solid #333; padding-top: 8px;";
        actionsEl.innerHTML = `<button onclick="saveMessageAsNote('${currentNotesFuncId}', ${index}, this)" style="background: #2a2a2a; color: #ffd700; border: 1px solid #444; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.75rem; font-weight: bold;"><i class="fa-solid fa-plus"></i> Save Note</button>`;
        msgEl.appendChild(actionsEl);
    }
    if (historyEl && isAtBottom) historyEl.scrollTop = historyEl.scrollHeight;
}

async function sendLLMChat() {
    const inputEl = document.getElementById("llm-input");
    const text = inputEl.value.trim();
    if (!text || !currentNotesFuncId) return;
    addChatMessage(currentNotesFuncId, "user", text);
    inputEl.value = "";
    const sendBtn = document.getElementById("llm-send-btn");
    const stopBtn = document.getElementById("llm-stop-btn");
    if (sendBtn) sendBtn.style.display = "none";
    if (stopBtn) stopBtn.style.display = "block";
    llmAbortController = new AbortController();
    try {
        const response = await fetch("/api/llm/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ messages: chatHistories[currentNotesFuncId] }),
            signal: llmAbortController.signal
        });
        const msgEl = addChatMessage(currentNotesFuncId, "ai", "");
        const msgIndex = chatHistories[currentNotesFuncId].length - 1;
        const reply = await readStream(response, (text) => updateChatMessageUI(msgEl, text, msgIndex, currentNotesFuncId));
    } catch (err) {
        if (err.name !== 'AbortError') addChatMessage(currentNotesFuncId, "ai", "Error: " + err.message);
    } finally {
        if (sendBtn) sendBtn.style.display = "block";
        if (stopBtn) stopBtn.style.display = "none";
    }
}

function stopLLMGeneration() { if (llmAbortController) llmAbortController.abort(); }

async function saveMessageAsNote(funcId, index, btn) {
    const history = chatHistories[funcId];
    if (!history || !history[index]) return;
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/add' : '/api/notes/add';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const res = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ collection: funcId.split(":")[0], [idKey]: funcId, text: history[index].content, owner: "llm" })
        });
        if ((await res.json()).status === "success") {
            await refreshNotes(funcId);
            btn.innerHTML = '<i class="fa-solid fa-check"></i> Saved';
            btn.disabled = true;
        }
    } catch (e) { alert(e.message); }
}

async function handleDroppedText(funcId, text) {
    const isFile = entityMode === 'file';
    const endpoint = isFile ? '/api/notes/file/add' : '/api/notes/add';
    const idKey = isFile ? 'file_id' : 'func_id';
    try {
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection: funcId.split(':')[0], [idKey]: funcId, text: text, owner: 'llm' })
        });
        if ((await res.json()).status === 'success') {
            await refreshNotes(funcId);
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
