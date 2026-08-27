/**
 * EntityRenderer Module for BSimVis
 * Standardizes rendering of functions, hashes, tags, and clusters.
 */

window.EntityRenderer = {
    /**
     * Renders a function signature with interactions.
     * @param {Object} f - Function data
     * @param {Object} options - Rendering options (showActions, inline, etc.)
     * @returns {string} HTML string
     */
    renderFunction: function(f, options = {}) {
        const name = f['function_name'] || 'Unknown';
        const namespace = f['namespace'] || '';
        const parameters = f['parameters'] || [];
        const returnType = f['return_type'] || 'void';
        const entry = f['entrypoint_address'] || '';
        const file_md5 = f['file_md5'] || '';
        const featCount = f['bsim_features_count'] || 0;
        
        let collection = stripPoolPrefix(f['collection'] || '') || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
        
        const funcId = f['function_id'] || `${collection}:func:${file_md5}:${entry}`;
        
        // Use global formatSigComponent from utils.js
        const fInfo = (typeof formatSigComponent === 'function') 
            ? formatSigComponent(namespace, returnType, name, parameters)
            : { ns: namespace, ret: returnType, params: parameters, fullSig: name };

        const showActions = options.showActions !== false;
        const hideNote = options.hideNote === true;
        
        let actionsHtml = '';
        if (showActions) {
            // Add to Diff / See Similar moved to right-click > Actions menu
            actionsHtml = `
                <div class="entity-actions" style="display:inline-flex; gap:4px; margin-left: auto; flex-shrink: 0; padding-left: 8px;">
                    ${hideNote ? '' : this.renderNoteButton(funcId, f.note_owners, { ...options, raw_data: f })}
                </div>
            `;
        }

        return `
            <div class="entity-function" style="display:flex; align-items:center; gap:8px; overflow:hidden; width: 100%;" 
                 title="${escapeAttr(fInfo.fullSig)}"
                 data-entity-data='${escapeAttr(JSON.stringify(f))}'
                 draggable="true" ondragstart="typeof onTableRowDragStart === 'function' && onTableRowDragStart(event)"
                 oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>
                <b class="entity-name" style="color:var(--accent); cursor:pointer; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex: 1; min-width: 0;" 
                   onmouseenter="typeof showCodePreview === 'function' && showCodePreview(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(name))}, ${escapeAttr(jsString(entry))}, ${escapeAttr(jsString(file_md5))}, ${Number(featCount) || 0}, event)" 
                   onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)"
                   onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"
                   onclick="typeof showFunctionCodeById === 'function' && showFunctionCodeById(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(name))}, '', event)">
                    ${fInfo.ret ? `<span style="color:var(--token-address)">${escapeHtml(fInfo.ret)}</span> ` : ''}${fInfo.ns ? `<span style="opacity:0.8; color:var(--text)">${escapeHtml(fInfo.ns)}::</span>` : ''}${escapeHtml(name)}<span style="color:var(--text)">(</span>${fInfo.params.map(t => `<span style="color:var(--token-address)">${escapeHtml(t)}</span>`).join('<span style="color:var(--text)">, </span>')}<span style="color:var(--text)">)</span>
                </b>
                ${actionsHtml}
            </div>
        `;
    },

    /**
     * Hover attributes that show the note preview tooltip. Reuses moveCodePreview for positioning.
     */
    notePreviewAttrs: function(id, mode) {
        return {
            onmouseenter: `typeof showNoteTooltip === 'function' && showNoteTooltip(${jsString(id)}, ${jsString(mode)}, event)`,
            onmousemove: `typeof moveCodePreview === 'function' && moveCodePreview(event)`,
            onmouseleave: `typeof hideNoteTooltip === 'function' && hideNoteTooltip()`
        };
    },

    /**
     * Extra class from note ownership: 'ai' (purple), 'both' (stacked yellow+purple), '' (yellow).
     */
    noteOwnerClass: function(noteOwners = []) {
        const isAI = o => ['llm', 'ai'].includes(String(o).toLowerCase());
        const hasAI = noteOwners.some(isAI);
        const hasUser = noteOwners.some(o => !isAI(o));
        if (hasAI && hasUser) return 'notes-both';
        if (hasAI) return 'notes-ai';
        return '';
    },

    /**
     * Renders a note button with consistent styling.
     */
    renderNoteButton: function(id, noteOwners = [], options = {}) {
        const hasNotes = noteOwners && noteOwners.length > 0;
        const isTable = options.isTable === true;
        
        // We look for note_count in the parent data if available
        const f = options.raw_data || {};
        const noteCount = f.note_count || noteOwners.length || 0;
        
        if (isTable && !hasNotes) return '';
        
        return UI.Button.render({
            className: `btn-note-action ${hasNotes ? 'has-notes ' + this.noteOwnerClass(noteOwners) : ''}`,
            icon: hasNotes ? 'fa-solid fa-note-sticky' : 'fa-regular fa-note-sticky',
            tooltip: hasNotes ? `Notes by: ${noteOwners.join(', ')}` : 'Add Note',
            onClick: `event.stopPropagation(); showNotePanel(${jsString(id)}, event)`,
            badge: noteCount > 1 ? `+${noteCount}` : null,
            attr: hasNotes ? this.notePreviewAttrs(id, 'func') : {}
        });
    },

    /**
     * Renders a note button for file entities (calls showFileNotePanel).
     */
    renderFileNoteButton: function(id, noteOwners = [], options = {}) {
        const hasNotes = noteOwners && noteOwners.length > 0;
        const isTable = options.isTable === true;

        const f = options.raw_data || {};
        const noteCount = f.note_count || noteOwners.length || 0;

        if (isTable && !hasNotes) return '';

        return UI.Button.render({
            className: `btn-note-action ${hasNotes ? 'has-notes ' + this.noteOwnerClass(noteOwners) : ''}`,
            icon: hasNotes ? 'fa-solid fa-note-sticky' : 'fa-regular fa-note-sticky',
            tooltip: hasNotes ? `File Notes by: ${noteOwners.join(', ')}` : 'Add File Note',
            onClick: `event.stopPropagation(); showFileNotePanel(${jsString(id)}, event)`,
            badge: noteCount > 1 ? `+${noteCount}` : null,
            attr: hasNotes ? this.notePreviewAttrs(id, 'file') : {}
        });
    },

    /**
     * Renders a note button for bin_sim pair entities (calls showBinSimNotePanel).
     */
    renderBinSimNoteButton: function(sid, noteOwners = [], options = {}) {
        const hasNotes = noteOwners && noteOwners.length > 0;
        const isTable = options.isTable === true;

        const f = options.raw_data || {};
        const noteCount = f.note_count || noteOwners.length || 0;

        if (isTable && !hasNotes) return '';

        return UI.Button.render({
            className: `btn-note-action ${hasNotes ? 'has-notes ' + this.noteOwnerClass(noteOwners) : ''}`,
            icon: hasNotes ? 'fa-solid fa-note-sticky' : 'fa-regular fa-note-sticky',
            tooltip: hasNotes ? `Pair notes by: ${noteOwners.join(', ')}` : 'Add Pair Note',
            onClick: `event.stopPropagation(); showBinSimNotePanel(${jsString(sid)}, event)`,
            badge: noteCount > 1 ? `+${noteCount}` : null,
            attr: hasNotes ? this.notePreviewAttrs(sid, 'bin_sim') : {}
        });
    },

    /**
     * Handles context menu by parsing data attribute.
     */
    handleContextMenu: function(e, type, el) {
        if (e) {
            e.preventDefault();
            e.stopPropagation();
        }
        
        // Prefer the top-most window that has the context menu module loaded
        let menuFn = null;
        let targetWindow = null;
        
        try {
            if (window.top && typeof window.top.showGraphContextMenu === 'function') {
                menuFn = window.top.showGraphContextMenu;
                targetWindow = window.top;
            } else if (window.parent && typeof window.parent.showGraphContextMenu === 'function') {
                menuFn = window.parent.showGraphContextMenu;
                targetWindow = window.parent;
            } else {
                menuFn = window.showGraphContextMenu;
                targetWindow = window;
            }
        } catch (err) {
            // Cross-origin or other error
            menuFn = window.showGraphContextMenu;
            targetWindow = window;
        }

        if (typeof menuFn === 'function') {
            try {
                const dataStr = el.getAttribute('data-entity-data');
                if (dataStr) {
                    const data = JSON.parse(dataStr);
                    
                    if (targetWindow !== window) {
                        // Adjust coordinates for iframe
                        let rect = { left: 0, top: 0 };
                        try {
                            // Try to find this window's iframe element in the target window's document
                            const iframes = targetWindow.document.querySelectorAll('iframe');
                            for (let i = 0; i < iframes.length; i++) {
                                if (iframes[i].contentWindow === window) {
                                    rect = iframes[i].getBoundingClientRect();
                                    break;
                                }
                            }
                        } catch (err) {
                        }

                        const fakeEvent = {
                            clientX: e.clientX + rect.left,
                            clientY: e.clientY + rect.top,
                            preventDefault: () => { if (e.preventDefault) e.preventDefault(); },
                            stopPropagation: () => { if (e.stopPropagation) e.stopPropagation(); }
                        };
                        menuFn(fakeEvent, type, data);
                    } else {
                        menuFn(e, type, data);
                    }
                }
            } catch (err) {
                console.error("Failed to parse entity data for context menu", err);
            }
        }
    },

    /**
     * Renders an MD5 hash with interactions.
     */
    renderMd5: function(md5, options = {}) {
        if (!md5) return '<span class="mono dim">---</span>';
        const actualMd5 = md5.includes(':') ? md5.split(':').pop() : md5;
        const displayMd5 = options.full ? actualMd5 : actualMd5.substring(0, 8);
        const collection = typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main';
        const fileId = `${collection}:file:${actualMd5}`;
        const fileData = { md5: actualMd5, fileId: fileId, name: actualMd5 };
        // ponytail: enable direct file context menu from any rendered md5
        return `
            <span class="entity-md5 mono" style="color:var(--accent); cursor:pointer;" title="${escapeAttr(actualMd5)}"
                  data-entity-data='${escapeAttr(JSON.stringify(fileData))}'
                  oncontextmenu='event.stopPropagation(); typeof EntityRenderer !== "undefined" && EntityRenderer.handleContextMenu(event, "file", this)'># ${escapeHtml(displayMd5)}</span>
        `;
    },

    /**
     * Renders a tag editor/viewer.
     */
    renderTag: function(etype, eid, tags, user_tags, options = {}) {
        if (typeof renderTagEditor === 'function') {
            return renderTagEditor(etype, eid, tags, user_tags, options);
        }
        return '';
    },

    /**
     * Renders cluster cards.
     */
    renderClusterCard: function(clusters, isBinary = false) {
        if (typeof renderClusterCards === 'function') {
            return renderClusterCards(clusters, isBinary);
        }
        return '';
    },

    /**
     * Renders a file name with interactions (click to view details, right click for context menu).
     */
    renderFileName: function(filename, md5, collection = '', options = {}) {
        if (!filename && !md5) return '<span class="dim">Unknown</span>';
        const display = filename || md5 || 'Unknown';
        const actualMd5 = md5 ? (md5.includes(':') ? md5.split(':').pop() : md5) : '';
        const col = collection || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
        const fileId = `${col}:file:${actualMd5}`;
        const fileData = { md5: actualMd5, fileId: fileId, name: display, file_name: display };
        // ponytail: generic component for filename with both click and contextmenu support
        return `
            <b class="entity-filename" style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; cursor:pointer;"
               data-entity-data='${escapeAttr(JSON.stringify(fileData))}'
               onclick="openFileDetails(${escapeAttr(jsString(col))}, ${escapeAttr(jsString(actualMd5))}, ${escapeAttr(jsString(display))}, event)"
               oncontextmenu='event.stopPropagation(); typeof EntityRenderer !== "undefined" && EntityRenderer.handleContextMenu(event, "file", this)'>${escapeHtml(display)}</b>
        `;
    }
};
