/**
 * Notes management for BSimVis
 */

async function showNotes(funcId) {
    const collection = funcId.split(':')[0];
    
    // Create or find notes panel
    let panel = document.getElementById('notes-panel');
    if (!panel) {
        panel = document.createElement('div');
        panel.id = 'notes-panel';
        panel.className = 'floating-panel';
        panel.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 500px;
            max-width: 90vw;
            max-height: 80vh;
            background: #1e1e1e;
            border: 1px solid #444;
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            z-index: 10001;
            display: flex;
            flex-direction: column;
            border-radius: 8px;
            overflow: hidden;
            font-family: 'Inter', sans-serif;
        `;
        document.body.appendChild(panel);
        
        // Add backdrop
        const backdrop = document.createElement('div');
        backdrop.id = 'notes-backdrop';
        backdrop.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: rgba(0,0,0,0.6);
            z-index: 10000;
        `;
        backdrop.onclick = closeNotes;
        document.body.appendChild(backdrop);
    }
    
        panel.innerHTML = `
        <div style="padding: 12px 16px; background: #252525; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center;">
            <h3 style="margin: 0; font-size: 1rem; color: #ffd700;"><i class="fa-solid fa-note-sticky"></i> Function Notes</h3>
            <button onclick="closeNotes()" style="background: none; border: none; color: #888; cursor: pointer; font-size: 1.2rem;">&times;</button>
        </div>
        <div id="notes-list" style="flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px;">
            <div style="text-align: center; color: #888; padding: 20px;">Loading notes...</div>
        </div>
        <div style="padding: 16px; background: #252525; border-top: 1px solid #333;">
            <textarea id="new-note-text" placeholder="Add a new note (Markdown supported)..." style="width: 100%; min-height: 100px; background: #121212; border: 1px solid #444; color: #eee; padding: 10px; border-radius: 4px; resize: vertical; margin-bottom: 8px; box-sizing: border-box; font-family: 'Fira Code', 'Cascadia Code', 'Source Code Pro', monospace; font-size: 0.9rem; line-height: 1.5;"></textarea>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <select id="note-owner-select" style="background: #121212; color: #ccc; border: 1px solid #444; border-radius: 4px; padding: 4px 8px; font-size: 0.8rem;">
                    <option value="user">User</option>
                    <option value="llm">LLM</option>
                </select>
                <button onclick="saveNote('${funcId}')" style="background: var(--accent); color: #000; border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 0.85rem;">Add Note</button>
            </div>
        </div>
    `;
    
    panel.style.display = 'flex';
    document.getElementById('notes-backdrop').style.display = 'block';
    
    await refreshNotes(funcId);
}

function closeNotes() {
    const panel = document.getElementById('notes-panel');
    const backdrop = document.getElementById('notes-backdrop');
    if (panel) panel.style.display = 'none';
    if (backdrop) backdrop.style.display = 'none';
}

async function refreshNotes(funcId) {
    const listEl = document.getElementById('notes-list');
    const collection = funcId.split(':')[0];
    
    // Inject markdown styles if not present
    if (!document.getElementById('notes-md-styles')) {
        const style = document.createElement('style');
        style.id = 'notes-md-styles';
        style.textContent = `
            .note-markdown-body { font-family: 'Fira Code', 'Cascadia Code', 'Source Code Pro', monospace; font-size: 0.9rem; line-height: 1.5; color: #eee; }
            .note-markdown-body p { margin-top: 0; margin-bottom: 8px; }
            .note-markdown-body code { background: #333; padding: 2px 4px; border-radius: 3px; }
            .note-markdown-body pre { background: #121212; padding: 10px; border-radius: 4px; overflow-x: auto; border: 1px solid #333; margin: 8px 0; }
            .note-markdown-body pre code { background: none; padding: 0; }
            .note-markdown-body ul, .note-markdown-body ol { margin-top: 0; margin-bottom: 8px; padding-left: 20px; }
            .note-markdown-body h1, .note-markdown-body h2, .note-markdown-body h3 { margin-top: 12px; margin-bottom: 8px; color: #ffd700; border-bottom: 1px solid #333; padding-bottom: 4px; }
            .note-markdown-body blockquote { border-left: 3px solid var(--accent); margin: 8px 0; padding-left: 12px; color: #bbb; font-style: italic; }
        `;
        document.head.appendChild(style);
    }
    
    try {
        const res = await fetch(`/api/notes/list?collection=${encodeURIComponent(collection)}&func_id=${encodeURIComponent(funcId)}`);
        const data = await res.json();
        
        if (data.status === 'success') {
            const notes = data.notes || [];
            if (notes.length === 0) {
                listEl.innerHTML = '<div style="text-align: center; color: #666; padding: 20px;">No notes yet. Be the first to add one!</div>';
            } else {
                listEl.innerHTML = notes.map(note => {
                    const renderedText = (typeof marked !== 'undefined') ? marked.parse(note.text) : note.text;
                    const rawTextEscaped = note.text.replace(/'/g, "&apos;").replace(/"/g, "&quot;");
                    return `
                        <div class="note-item" data-raw-text="${rawTextEscaped}" style="background: #2a2a2a; border-radius: 6px; padding: 12px; border-left: 3px solid ${note.owner === 'llm' ? '#ae81ff' : '#ffd700'};">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                                <span style="font-size: 0.75rem; font-weight: bold; color: ${note.owner === 'llm' ? '#ae81ff' : '#ffd700'}; text-transform: uppercase;">${note.owner}</span>
                                <span style="font-size: 0.7rem; color: #888;">${new Date(note.timestamp).toLocaleString()}</span>
                            </div>
                            <div class="note-text note-markdown-body" style="font-family: inherit;">${renderedText}</div>
                            <div style="display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px; border-top: 1px solid #333; padding-top: 8px;">
                                <button onclick="editNoteUI('${funcId}', '${note.id}', this)" style="background: none; border: none; color: #666; cursor: pointer; font-size: 0.8rem;" title="Edit"><i class="fa-solid fa-pen"></i></button>
                                <button onclick="deleteNote('${funcId}', '${note.id}')" style="background: none; border: none; color: #666; cursor: pointer; font-size: 0.8rem;" title="Delete"><i class="fa-solid fa-trash"></i></button>
                            </div>
                        </div>
                    `;
                }).join('');
            }
        } else {
            listEl.innerHTML = `<div style="color: var(--danger); padding: 20px;">Error: ${data.error || 'Unknown error'}</div>`;
        }
    } catch (e) {
        listEl.innerHTML = `<div style="color: var(--danger); padding: 20px;">Failed to fetch notes: ${e.message}</div>`;
    }
}

async function saveNote(funcId) {
    const textEl = document.getElementById('new-note-text');
    const ownerEl = document.getElementById('note-owner-select');
    const text = textEl.value.trim();
    const owner = ownerEl.value;
    const collection = funcId.split(':')[0];
    
    if (!text) return;
    
    try {
        const res = await fetch('/api/notes/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, func_id: funcId, text, owner })
        });
        
        const data = await res.json();
        if (data.status === 'success') {
            textEl.value = '';
            await refreshNotes(funcId);
            
            // Proactively update UI if we are in search view
            if (window.parent && typeof window.parent.refreshFunctionRow === 'function') {
                window.parent.refreshFunctionRow(funcId);
            }
        } else {
            alert(`Error adding note: ${data.error}`);
        }
    } catch (e) {
        alert(`Failed to save note: ${e.message}`);
    }
}

async function deleteNote(funcId, note_id) {
    if (!confirm('Are you sure you want to delete this note?')) return;
    const collection = funcId.split(':')[0];
    
    try {
        const res = await fetch('/api/notes/remove', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, func_id: funcId, note_id })
        });
        
        const data = await res.json();
        if (data.status === 'success') {
            await refreshNotes(funcId);
            if (window.parent && typeof window.parent.refreshFunctionRow === 'function') {
                window.parent.refreshFunctionRow(funcId);
            }
        } else {
            alert(`Error deleting note: ${data.error}`);
        }
    } catch (e) {
        alert(`Failed to delete note: ${e.message}`);
    }
}

function editNoteUI(funcId, note_id, btn) {
    const item = btn.closest('.note-item');
    const textEl = item.querySelector('.note-text');
    const collection = funcId.split(':')[0];
    
    // We need the raw text to edit, but it might be rendered as HTML.
    // Let's fetch the raw text from the original data or just parse it back if simple.
    // A better way is to store the raw text in a data attribute when rendering.
    // For now, let's just use the textContent which should be close enough for simple text.
    // Or better: update the render loop to include a data-raw-text attribute.
    
    const rawText = item.dataset.rawText || textEl.innerText;
    
    textEl.innerHTML = `
        <textarea style="width: 100%; min-height: 100px; background: #121212; border: 1px solid var(--accent); color: #eee; padding: 10px; border-radius: 4px; font-size: 0.9rem; font-family: 'Fira Code', 'Cascadia Code', 'Source Code Pro', monospace; line-height: 1.5; resize: vertical;">${rawText}</textarea>
        <div style="display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px;">
            <button onclick="refreshNotes('${funcId}')" style="font-size: 0.75rem; background: #444; border: none; color: #eee; padding: 4px 12px; border-radius: 3px; cursor: pointer;">Cancel</button>
            <button onclick="saveEdit('${funcId}', '${note_id}', this)" style="font-size: 0.75rem; background: var(--accent); border: none; color: #000; padding: 4px 12px; border-radius: 3px; cursor: pointer; font-weight: bold;">Save</button>
        </div>
    `;
    // Hide original actions
    btn.parentElement.style.display = 'none';
}

async function saveEdit(funcId, note_id, btn) {
    const text = btn.parentElement.parentElement.querySelector('textarea').value.trim();
    const collection = funcId.split(':')[0];
    
    if (!text) return;
    
    try {
        const res = await fetch('/api/notes/update', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, func_id: funcId, note_id, text })
        });
        
        const data = await res.json();
        if (data.status === 'success') {
            await refreshNotes(funcId);
        } else {
            alert(`Error updating note: ${data.error}`);
        }
    } catch (e) {
        alert(`Failed to update note: ${e.message}`);
    }
}

// Global exposure
window.showNotes = showNotes;
window.closeNotes = closeNotes;
window.saveNote = saveNote;
window.deleteNote = deleteNote;
window.editNoteUI = editNoteUI;
window.saveEdit = saveEdit;

/**
 * Global wrapper for note buttons rendered by EntityRenderer.
 */
window.showNotePanel = function(id, e) {
    if (typeof showNotes === 'function') {
        showNotes(id);
    } else {
        console.error("showNotes function not found");
    }
};
