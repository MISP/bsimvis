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
        const collection = f['collection'] || 'main';
        const funcId = f['function_id'] || `${collection}:func:${file_md5}:${entry}`;
        
        const safeName = (name || '').replace(/'/g, "\\'");
        
        // Use global formatSigComponent from utils.js
        const fInfo = (typeof formatSigComponent === 'function') 
            ? formatSigComponent(namespace, returnType, name, parameters)
            : { ns: namespace, ret: returnType, params: parameters, fullSig: name };

        const showActions = options.showActions !== false;
        const hideNote = options.hideNote === true;
        
        let actionsHtml = '';
        if (showActions) {
            const isActive = (typeof diffSelection !== 'undefined' && typeof normalizeFuncId === 'function') 
                ? diffSelection.some(item => item.id === normalizeFuncId(funcId)) 
                : false;
            
            const diffButton = UI.Button.render({
                className: `btn-diff-action ${isActive ? 'active' : ''}`,
                label: '±',
                tooltip: 'Add to Diff',
                attr: {
                    'data-func-id': typeof normalizeFuncId === 'function' ? normalizeFuncId(funcId) : funcId,
                    'onmouseenter': `typeof onHoverDiffButton === 'function' && onHoverDiffButton(event, '${funcId}', '${safeName}')`,
                    'onmousemove': `typeof moveCodePreview === 'function' && moveCodePreview(event)`,
                    'onmouseleave': `typeof hideDiffPreview === 'function' && hideDiffPreview(event)`
                },
                onClick: `event.stopPropagation(); typeof addToDiff === 'function' && addToDiff('${funcId}', '${safeName}')`,
                style: 'padding:0; font-size: 0.75rem; border-radius: 3px; width:22px; height:22px; display:inline-flex; align-items:center; justify-content:center;'
            });

            const simButton = UI.Button.render({
                className: 'btn-sim-action',
                icon: 'fa-solid fa-code-compare',
                tooltip: 'See Similar Functions',
                onClick: `event.stopPropagation(); Nav.openPath('/collection/${collection}/search/function-similarity?md5=${file_md5}&address=${entry}&algo=unweighted_cosine', event)`,
                style: 'padding:0; font-size: 0.75rem; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;'
            });

            actionsHtml = `
                <div class="entity-actions" style="display:inline-flex; gap:4px; margin-left: auto; flex-shrink: 0; padding-left: 8px;">
                    ${hideNote ? '' : this.renderNoteButton(funcId, f.note_owners, { ...options, raw_data: f })}
                    ${diffButton}
                    ${simButton}
                </div>
            `;
        }

        return `
            <div class="entity-function" style="display:flex; align-items:center; gap:8px; overflow:hidden; width: 100%;" 
                 title="${fInfo.fullSig}"
                 data-entity-data='${JSON.stringify(f).replace(/'/g, "&apos;")}'
                 oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>
                <b class="entity-name" style="color:var(--accent); cursor:pointer; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex: 1; min-width: 0;" 
                   onmouseenter="typeof showCodePreview === 'function' && showCodePreview('${funcId}', '${safeName}', '${entry}', '${file_md5}', ${featCount}, event)" 
                   onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)"
                   onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"
                   onclick="typeof showFunctionCodeById === 'function' && showFunctionCodeById('${funcId}', '${safeName}', '', event)">
                    ${fInfo.ret ? `<span style="color:#ae81ff">${fInfo.ret}</span> ` : ''}${fInfo.ns ? `<span style="color:white; opacity:0.8">${fInfo.ns}::</span>` : ''}${name}<span style="color:white">(</span>${fInfo.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                </b>
                ${actionsHtml}
            </div>
        `;
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
            className: `btn-note-action ${hasNotes ? 'has-notes' : ''}`,
            icon: hasNotes ? 'fa-solid fa-note-sticky' : 'fa-regular fa-note-sticky',
            tooltip: hasNotes ? `Notes by: ${noteOwners.join(', ')}` : 'Add Note',
            onClick: `event.stopPropagation(); showNotePanel('${id}', event)`,
            badge: noteCount > 0 ? `+${noteCount}` : null
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
            className: `btn-note-action ${hasNotes ? 'has-notes' : ''}`,
            icon: hasNotes ? 'fa-solid fa-note-sticky' : 'fa-regular fa-note-sticky',
            tooltip: hasNotes ? `File Notes by: ${noteOwners.join(', ')}` : 'Add File Note',
            onClick: `event.stopPropagation(); showFileNotePanel('${id}', event)`,
            badge: noteCount > 0 ? `+${noteCount}` : null
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
        return `
            <span class="entity-md5 mono" style="color:var(--accent);" title="${actualMd5}"># ${displayMd5}</span>
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
    }
};
