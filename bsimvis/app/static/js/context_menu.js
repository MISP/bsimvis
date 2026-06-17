// ---------------------------------------------------------------------------
// Unified Graph Context Menu Module
// ---------------------------------------------------------------------------

(function () {
    // Save current context menu state for refreshing
    window.currentContextMenu = null;
    window.graphContextMenuOpen = false;
    window._contextMenuCloseFn = null;

    // Helper function to copy metadata to clipboard and notify
    window.copyMetadata = function (text, desc) {
        if (typeof copyToClipboard === 'function') {
            copyToClipboard(text);
            if (typeof showToast === 'function') {
                showToast(`Copied ${desc}: ${text}`, 'success');
            } else {
                console.log(`Copied ${desc}: ${text}`);
            }
        } else {
            // Fallback copy
            try {
                const textArea = document.createElement("textarea");
                textArea.value = text;
                textArea.style.position = "fixed";
                textArea.style.top = "0";
                textArea.style.left = "0";
                textArea.style.opacity = "0";
                document.body.appendChild(textArea);
                textArea.focus();
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
                if (typeof showToast === 'function') {
                    showToast(`Copied ${desc}: ${text}`, 'success');
                }
            } catch (err) {
                if (typeof showToast === 'function') {
                    showToast(`Failed to copy ${desc}`, 'error');
                }
            }
        }
    };

    // Main entry point for the Unified Graph Context Menu
    window.showGraphContextMenu = function (e, type, data, isRefresh = false) {
        if (window.setTrigger) window.setTrigger(e);

        if (!isRefresh) {
            // Trigger a background refresh of tag metadata if it's a new menu opening
            const col = typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : '';
            if (typeof fetchTagMetadata === 'function') {
                fetchTagMetadata(col).then(() => {
                    if (window.graphContextMenuOpen) {
                        window.refreshContextMenuUI();
                    }
                });
            }
        }

        let menu = document.getElementById('graph-context-menu');
        if (!menu) {
            menu = document.createElement('div');
            menu.id = 'graph-context-menu';
            menu.className = 'context-menu';
            document.body.appendChild(menu);
        }

        window.currentContextMenu = { e, type, data };
        window.graphContextMenuOpen = true;

        // Hide previews/tooltips to avoid overlap, but DON'T close ourselves
        if (window.hideDiffPreview) window.hideDiffPreview(null, true);
        if (window.hideCodePreview) window.hideCodePreview(null, true);
        if (window.hideBinaryPreview) window.hideBinaryPreview(null, true);

        const tooltipIds = [
            'code-preview-tooltip',
            'token-tooltip',
            'diff-preview-tooltip',
            'hierarchy-tooltip',
            'binary-preview-tooltip',
            'chord-tooltip',
            'bin-hierarchy-tooltip',
            'tag-tooltip'
        ];
        tooltipIds.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
        });

        if (window.hierarchyInstance && typeof window.hierarchyInstance.hideTooltip === 'function') {
            window.hierarchyInstance.hideTooltip();
        }
        if (window.packingInstance && typeof window.packingInstance.hideTooltip === 'function') {
            window.packingInstance.hideTooltip();
        }
        if (window.binHierarchyInstance && typeof window.binHierarchyInstance.hideTooltip === 'function') {
            window.binHierarchyInstance.hideTooltip();
        }
        if (window.binPackingInstance && typeof window.binPackingInstance.hideTooltip === 'function') {
            window.binPackingInstance.hideTooltip();
        }

        // 1. Resolve and normalize type & metadata fields
        let resolvedType = type;
        let norm = {};

        // Resolve D3 clusters / members data wrappers
        if (type === 'cluster' || type === 'bin_cluster') {
            if (data.is_member) {
                if (type === 'cluster') {
                    resolvedType = 'function';
                    norm.id = data.id || data.function_id;
                    norm.name = data.name || data.function_name;
                    norm.addr = data.entrypoint || data.entrypoint_address || data.addr;
                    norm.md5 = data.md5 || data.file_md5;
                    if (norm.id && (!norm.addr || !norm.md5)) {
                        const parsed = window.parseFuncId(norm.id);
                        norm.addr = norm.addr || parsed.address;
                        norm.md5 = norm.md5 || parsed.md5;
                    }
                } else {
                    resolvedType = 'file';
                    norm.md5 = data.id || data.md5;
                    norm.name = data.name || data.file_name || norm.md5;
                    norm.id = data.fileId || data.id || `${getCollectionFromHash()}:file:${norm.md5}`;
                }
            } else {
                resolvedType = type;
                norm.id = data.id || data.cluster_id;
                norm.uuid = data.uuid || data.cluster_uuid;
                norm.name = data.name || data.cluster_name || norm.uuid || norm.id;
            }
        } else if (type === 'node' || type === 'function') {
            resolvedType = 'function';
            norm.id = data.id || data.function_id;
            norm.name = data.name || data.function_name;
            norm.addr = data.entrypoint || data.entrypoint_address || data.addr;
            norm.md5 = data.md5 || data.file_md5;
            if (norm.id && (!norm.addr || !norm.md5)) {
                const parsed = window.parseFuncId(norm.id);
                norm.addr = norm.addr || parsed.address;
                norm.md5 = norm.md5 || parsed.md5;
            }
            // Ensure norm.id has a parseable collection:func:md5:addr format
            const col = typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main';
            if (col && norm.md5 && norm.addr) {
                norm.id = col + ':func:' + norm.md5 + ':' + norm.addr;
            }
        } else if (type === 'file') {
            resolvedType = 'file';
            norm.md5 = data.md5 || data.id;
            norm.name = data.file_name || data.name || norm.md5;
            norm.id = data.fileId || data.id || `${getCollectionFromHash()}:file:${norm.md5}`;
        } else if (type === 'link' || type === 'similarity') {
            resolvedType = 'similarity';
            norm.id1 = data.id1;
            norm.id2 = data.id2;
            norm.name1 = data.name1;
            norm.name2 = data.name2;
            norm.score = data.score;
            norm.sid = data.sid || `${norm.id1}|${norm.id2}|${data.algo || 'unweighted_cosine'}`;
        } else if (type === 'bin_similarity') {
            resolvedType = 'bin_similarity';
            norm.md5_a = data.file1 ? (data.file1.md5 || data.file1.id) : data.md5_a;
            norm.md5_b = data.file2 ? (data.file2.md5 || data.file2.id) : data.md5_b;
            norm.name_a = data.file1 ? (data.file1.name || data.file1.file_name || data.file1.file_md5) : data.name_a;
            norm.name_b = data.file2 ? (data.file2.name || data.file2.file_name || data.file2.file_md5) : data.name_b;
            norm.value = data.value;
        }

        // 2. Build HTML Content
        let html = '';

        // -- Header --
        let headerTitle = '';
        if (resolvedType === 'function') headerTitle = `Function: ${norm.name}`;
        else if (resolvedType === 'file') headerTitle = `File: ${norm.name}`;
        else if (resolvedType === 'similarity') headerTitle = `Similarity: ${(parseFloat(norm.score) * 100).toFixed(1)}% Match`;
        else if (resolvedType === 'bin_similarity') headerTitle = `Link: ${norm.name_a.substring(0,10)} ↔ ${norm.name_b.substring(0,10)}`;
        else if (resolvedType === 'cluster') headerTitle = `Cluster: ${norm.name}`;
        else if (resolvedType === 'bin_cluster') headerTitle = `Binary Cluster: ${norm.name}`;

        html += `<div class="context-menu-header">${headerTitle}</div>`;

        // -- Bookmark & Ignore (Pinned at the Top) --
        if (resolvedType === 'function' || resolvedType === 'file' || resolvedType === 'similarity') {
            const etype = resolvedType;
            const eid = (resolvedType === 'function') ? norm.id : ((resolvedType === 'file') ? norm.id : norm.sid);
            const userTags = getEntityUserTags(etype, eid);
            const tags = getEntityStaticTags(etype, eid, data);
            
            const isBookmarked = userTags.includes('bookmark');
            const isIgnored = userTags.includes('ignore');

            html += `
            <div style="display: flex; gap: 8px; padding: 6px 16px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); margin-bottom: 4px;">
                <button class="bookmark-btn ${isBookmarked ? 'active' : ''}" style="flex: 1; padding: 6px; border-radius: 6px; display: flex; align-items: center; justify-content: center; background: ${isBookmarked ? 'rgba(102, 217, 239, 0.1)' : 'none'}; border: 1px solid ${isBookmarked ? '#66d9ef' : 'rgba(255, 255, 255, 0.1)'}; color: ${isBookmarked ? '#66d9ef' : '#75715e'}; cursor: pointer; transition: all 0.2s;" onclick="event.stopPropagation(); window.toggleContextMenuBookmark(event, '${etype}', '${eid}')">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="${isBookmarked ? '#66d9ef' : 'none'}" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right: 6px;"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
                    Bookmark
                </button>
                <button class="ignore-btn ${isIgnored ? 'active' : ''}" style="flex: 1; padding: 6px; border-radius: 6px; display: flex; align-items: center; justify-content: center; background: ${isIgnored ? 'rgba(249, 38, 114, 0.1)' : 'none'}; border: 1px solid ${isIgnored ? '#f92672' : 'rgba(255, 255, 255, 0.1)'}; color: ${isIgnored ? '#f92672' : '#75715e'}; cursor: pointer; transition: all 0.2s;" onclick="event.stopPropagation(); window.toggleContextMenuIgnore(event, '${etype}', '${eid}')">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right: 6px;"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>
                    Ignore
                </button>
            </div>`;

            // -- Tags Dropdown Submenu --
            const tagMeta = window.tagMetadata || (window.parent && window.parent.tagMetadata) || {};
            const allKnownTags = Object.keys(tagMeta).filter(t => t !== 'bookmark' && t !== 'ignore' && t && t.trim());
            let tagsSubmenuHtml = '';
            allKnownTags.forEach(tag => {
                const isActive = userTags.includes(tag);
                const color = window.getTagMetadata ? window.getTagMetadata(tag).color : '#66d9ef';
                const checkboxStyle = `color: ${isActive ? color : 'rgba(255,255,255,0.2)'}; width: 16px; text-align: center; font-size: 0.8rem;`;

                tagsSubmenuHtml += `
                <div class="context-menu-item" onclick="event.stopPropagation(); window.toggleContextMenuTag(event, '${etype}', '${eid}', '${String(tag).replace(/'/g, "\\'")}')">
                    <i class="fa-solid ${isActive ? 'fa-square-check' : 'fa-square'}" style="${checkboxStyle}"></i>
                    <span>${tag}</span>
                </div>`;
            });

            if (tagsSubmenuHtml) {
                tagsSubmenuHtml += `<div style="border-top: 1px solid rgba(255,255,255,0.05); margin: 4px 0;"></div>`;
            }

            tagsSubmenuHtml += `
            <div class="context-menu-item add-custom-tag-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); window.showTagManagementModal('${etype}', '${eid}')">
                <i class="fa-solid fa-plus" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Add custom tag...</span>
            </div>`;

            html += `
            <div class="context-menu-item submenu-trigger" style="position: relative;">
                <i class="fa-solid fa-tags" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Tags</span>
                <i class="fa-solid fa-chevron-right" style="margin-left: auto; font-size: 0.7rem; opacity: 0.5;"></i>
                
                <div class="context-menu submenu" style="position: absolute; left: 100%; top: -6px; display: none; min-width: 185px; max-height: 250px; overflow-y: auto; background: rgba(30, 30, 30, 0.98); border: 1px solid rgba(255, 255, 255, 0.15); z-index: 20005;">
                    ${tagsSubmenuHtml}
                </div>
            </div>`;

            // Active tags inline card list (stays under Tags Trigger row)
            const allTags = [...(userTags || [])].filter(t => t !== 'bookmark' && t !== 'ignore' && t && t.trim());
            if (allTags.length > 0) {
                const tagsHtml = allTags.map(tag => {
                    let color = '#66d9ef';
                    if (window.getTagMetadata) {
                        color = window.getTagMetadata(tag).color;
                    }
                    const removeClick = `event.stopPropagation(); window.removeContextMenuTag(event, '${etype}', '${eid}', '${tag}')`;
                    return `
                    <span class="sim-tag-card" style="border-color:${color}44; color:${color}; background:${color}11; margin: 2px; padding: 1px 6px; font-size: 0.7rem; border-radius: 4px; display: inline-flex; align-items: center;">
                        ${tag}
                        <span onclick="${removeClick}" style="cursor: pointer; margin-left: 4px; opacity: 0.7; font-weight: bold;">×</span>
                    </span>`;
                }).join('');

                html += `
                <div style="padding: 4px 16px 8px 16px; display: flex; flex-wrap: wrap; gap: 2px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); margin-bottom: 4px;">
                    ${tagsHtml}
                </div>`;
            }
        }

        // -- Copy Dropdown Submenu --
        let copySubmenuHtml = '';
        if (resolvedType === 'function') {
            copySubmenuHtml += renderCopyItem('Name', norm.name, 'fa-signature');
            copySubmenuHtml += renderCopyItem('Address', norm.addr, 'fa-location-crosshairs');
            copySubmenuHtml += renderCopyItem('Function ID', norm.id, 'fa-id-badge');
            copySubmenuHtml += renderCopyItem('File MD5', norm.md5, 'fa-fingerprint');
        } else if (resolvedType === 'file') {
            copySubmenuHtml += renderCopyItem('Name', norm.name, 'fa-signature');
            copySubmenuHtml += renderCopyItem('MD5', norm.md5, 'fa-fingerprint');
            copySubmenuHtml += renderCopyItem('File ID', norm.id, 'fa-id-badge');
        } else if (resolvedType === 'similarity') {
            copySubmenuHtml += renderCopyItem('Similarity ID', norm.sid, 'fa-id-badge');
            copySubmenuHtml += renderCopyItem('Match Score', norm.score, 'fa-percent');
            copySubmenuHtml += renderCopyItem('First Function ID', norm.id1, 'fa-id-badge');
            copySubmenuHtml += renderCopyItem('Second Function ID', norm.id2, 'fa-id-badge');
        } else if (resolvedType === 'bin_similarity') {
            copySubmenuHtml += renderCopyItem('Score', norm.value, 'fa-percent');
            copySubmenuHtml += renderCopyItem('First File MD5', norm.md5_a, 'fa-fingerprint');
            copySubmenuHtml += renderCopyItem('Second File MD5', norm.md5_b, 'fa-fingerprint');
        } else if (resolvedType === 'cluster' || resolvedType === 'bin_cluster') {
            copySubmenuHtml += renderCopyItem('Name', norm.name, 'fa-signature');
            copySubmenuHtml += renderCopyItem('UUID', norm.uuid, 'fa-id-badge');
            copySubmenuHtml += renderCopyItem('Cluster ID', norm.id, 'fa-id-badge');
        }

        if (copySubmenuHtml) {
            html += `
            <div class="context-menu-item submenu-trigger" style="position: relative;">
                <i class="fa-solid fa-copy" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Copy</span>
                <i class="fa-solid fa-chevron-right" style="margin-left: auto; font-size: 0.7rem; opacity: 0.5;"></i>
                
                <div class="context-menu submenu" style="position: absolute; left: 100%; top: -6px; display: none; min-width: 185px; background: rgba(30, 30, 30, 0.98); border: 1px solid rgba(255, 255, 255, 0.15); z-index: 20005;">
                    ${copySubmenuHtml}
                </div>
            </div>`;
        }

        // -- Actions Dropdown Submenu --
        let actionsSubmenuHtml = '';
        const col = getCollectionFromHash();
        if (resolvedType === 'function') {
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); addToDiff('${norm.id}', '${String(norm.name || '').replace(/'/g, "\\'")}')">
                <i class="fa-solid fa-plus-minus" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Add to Diff</span>
            </div>
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); seeSimilar('${norm.id}', event)">
                <i class="fa-solid fa-code-compare" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>See Similar</span>
            </div>
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); showFeaturePanel('${norm.id}', event)">
                <i class="fa-solid fa-fingerprint" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Show Features</span>
            </div>
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); showFunctionCodeById('${norm.id}', '${String(norm.name || '').replace(/'/g, "\\'")}', '', event)">
                <i class="fa-solid fa-code" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Show Code</span>
            </div>`;
        } else if (resolvedType === 'file') {
            const cgUrl = Nav.buildUIUrl(col, ['call_graph', norm.md5]);
            const simUrl = Nav.buildUIUrl(col, ['functions', 'similarities']) + '?md5=' + encodeURIComponent(norm.md5);
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="window.closeGraphContextMenu(); Nav.openPath('${cgUrl}', event, { title: 'Call Graph: ${norm.md5}', type: 'call_graph' })">
                <i class="fa-solid fa-sitemap" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Open Call Graph</span>
            </div>
            <div class="context-menu-item" onclick="window.closeGraphContextMenu(); Nav.openPath('${simUrl}', event, { title: 'Function Similarities', type: 'function-similarity' })">
                <i class="fa-solid fa-code-compare" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>View Similarities</span>
            </div>`;
        } else if (resolvedType === 'similarity') {
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); openDiffDirectly('${norm.id1}', '${String(norm.name1 || '').replace(/'/g, "\\'")}', '${norm.id2}', '${String(norm.name2 || '').replace(/'/g, "\\'")}', event)">
                <i class="fa-solid fa-columns" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Show Diff</span>
            </div>`;
        } else if (resolvedType === 'bin_similarity') {
            const diffUrl = (window.buildFileDiffUrl || (window.parent && window.parent.buildFileDiffUrl) || buildFileDiffUrl)(col, norm.md5_a, col, norm.md5_b);
            const safeNameA = String(norm.name_a || '').replace(/'/g, "\\'").replace(/"/g, "&quot;");
            const safeNameB = String(norm.name_b || '').replace(/'/g, "\\'").replace(/"/g, "&quot;");
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="window.closeGraphContextMenu(); Nav.openPath('${diffUrl}', event, { title: 'Bin Diff: ${safeNameA} vs ${safeNameB}', type: 'bin_sim' })">
                <i class="fa-solid fa-columns" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Show Binary Diff</span>
            </div>`;
        } else if (resolvedType === 'cluster') {
            const funcClusterUrl = Nav.buildUIUrl(col, ['functions']) + '?cluster_uuid=' + encodeURIComponent(norm.uuid);
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); renameCluster('${norm.id}', '${String(norm.name || '').replace(/'/g, "\\'")}')">
                <i class="fa-solid fa-pen-to-square" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Rename Cluster</span>
            </div>
            <div class="context-menu-item" onclick="window.closeGraphContextMenu(); Nav.openPath('${funcClusterUrl}', event, { title: 'Cluster Functions', type: 'functions' })">
                <i class="fa-solid fa-code" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>View Functions</span>
            </div>`;
        } else if (resolvedType === 'bin_cluster') {
            const fileClusterUrl = Nav.buildUIUrl(col, ['files']) + '?bin_cluster_uuid=' + encodeURIComponent(norm.uuid);
            actionsSubmenuHtml += `
            <div class="context-menu-item" onclick="event.stopPropagation(); window.closeGraphContextMenu(); renameBinCluster('${norm.id}', '${String(norm.name || '').replace(/'/g, "\\'")}')">
                <i class="fa-solid fa-pen-to-square" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Rename Cluster</span>
            </div>
            <div class="context-menu-item" onclick="window.closeGraphContextMenu(); Nav.openPath('${fileClusterUrl}', event, { title: 'Cluster Files', type: 'files' })">
                <i class="fa-solid fa-folder-open" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>View Files</span>
            </div>`;
        }

        if (actionsSubmenuHtml) {
            html += `
            <div class="context-menu-item submenu-trigger" style="position: relative;">
                <i class="fa-solid fa-bolt" style="width: 16px; text-align: center; opacity: 0.8;"></i>
                <span>Actions</span>
                <i class="fa-solid fa-chevron-right" style="margin-left: auto; font-size: 0.7rem; opacity: 0.5;"></i>
                
                <div class="context-menu submenu" style="position: absolute; left: 100%; top: -6px; display: none; min-width: 185px; background: rgba(30, 30, 30, 0.98); border: 1px solid rgba(255, 255, 255, 0.15); z-index: 20005;">
                    ${actionsSubmenuHtml}
                </div>
            </div>`;
        }

        menu.innerHTML = html;
        menu.style.display = 'block';

        // Add hover flipping listener to prevent submenus from going offscreen
        menu.querySelectorAll('.submenu-trigger').forEach(trigger => {
            trigger.addEventListener('mouseenter', () => {
                const submenu = trigger.querySelector('.submenu');
                if (!submenu) return;
                
                submenu.style.display = 'block';
                submenu.style.left = '100%';
                submenu.style.right = 'auto';
                submenu.style.top = '-6px';
                
                const rect = submenu.getBoundingClientRect();
                if (rect.right > window.innerWidth) {
                    submenu.style.left = 'auto';
                    submenu.style.right = '100%';
                }
                
                if (rect.bottom > window.innerHeight) {
                    const diff = rect.bottom - window.innerHeight;
                    submenu.style.top = `-${diff + 16}px`;
                }
            });
            trigger.addEventListener('mouseleave', () => {
                const submenu = trigger.querySelector('.submenu');
                if (!submenu) return;
                submenu.style.display = 'none';
            });
        });

        // Disable SVG pointer events during menu display to avoid underlying visual hover interference
        const graphSvg = document.querySelector('#bk-similarity-plot svg');
        if (graphSvg) graphSvg.style.pointerEvents = 'none';

        if (!isRefresh) {
            // Position the menu
            let x = e.clientX, y = e.clientY;
            menu.style.left = x + 'px';
            menu.style.top = y + 'px';

            // Boundary collision handling
            const rect = menu.getBoundingClientRect();
            if (x + rect.width > window.innerWidth) x = window.innerWidth - rect.width - 10;
            if (y + rect.height > window.innerHeight) y = window.innerHeight - rect.height - 10;

            menu.style.left = Math.max(5, x) + 'px';
            menu.style.top = Math.max(5, y) + 'px';
        }

        // Click outside handler - clean up any existing listener and re-register
        if (window._contextMenuCloseFn) {
            document.removeEventListener('mousedown', window._contextMenuCloseFn, { capture: true });
            window._contextMenuCloseFn = null;
        }

        const closeGlobal = (me) => {
            if (me.button === 2) return; // Ignore right-click
            const currentMenu = document.getElementById('graph-context-menu');
            if (currentMenu && !currentMenu.contains(me.target)) {
                window.closeGraphContextMenu();
            }
        };

        if (isRefresh) {
            document.addEventListener('mousedown', closeGlobal, { capture: true });
            window._contextMenuCloseFn = closeGlobal;
        } else {
            setTimeout(() => {
                // Only register if the menu hasn't been closed in the 10ms window
                const currentMenu = document.getElementById('graph-context-menu');
                if (currentMenu && currentMenu.style.display === 'block') {
                    document.addEventListener('mousedown', closeGlobal, { capture: true });
                    window._contextMenuCloseFn = closeGlobal;
                }
            }, 10);
        }
    };

    window.closeGraphContextMenu = function () {
        const menu = document.getElementById('graph-context-menu');
        if (menu) menu.style.display = 'none';
        window.graphContextMenuOpen = false;
        window.currentContextMenu = null;

        const graphSvg = document.querySelector('#bk-similarity-plot svg');
        if (graphSvg) graphSvg.style.pointerEvents = 'auto';

        if (window._contextMenuCloseFn) {
            document.removeEventListener('mousedown', window._contextMenuCloseFn, { capture: true });
            window._contextMenuCloseFn = null;
        }
    };

    window.refreshContextMenuUI = function () {
        if (!window.currentContextMenu) return;
        const { e, type, data } = window.currentContextMenu;
        window.showGraphContextMenu(e, type, data, true);
    };

    // Helper functions for tags
    function getEntityUserTags(etype, eid, data = null) {
        if (!data && window.currentContextMenu) {
            data = window.currentContextMenu.data;
        }

        // 1. Try DOM elements first (most accurate for tables and forms)
        const domEditors = document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`);
        if (domEditors && domEditors.length > 0) {
            const editor = domEditors[0];
            const analystCards = editor.querySelectorAll('.sim-tag-card');
            let userTags = Array.from(analystCards).map(c => c.textContent.replace('×', '').trim());

            const bookmarkBtn = editor.querySelector('.bookmark-btn');
            if (bookmarkBtn && bookmarkBtn.classList.contains('active')) {
                if (!userTags.includes('bookmark')) userTags.push('bookmark');
            }
            const ignoreBtn = editor.querySelector('.ignore-btn');
            if (ignoreBtn && ignoreBtn.classList.contains('active')) {
                if (!userTags.includes('ignore')) userTags.push('ignore');
            }
            return userTags;
        }

        // 2. Try graph instance
        const graph = window.graphInstance;
        if (graph) {
            if (etype === 'function') {
                const latest = graph.nodes_map.get(eid);
                if (latest && latest.user_tags) return latest.user_tags;
            } else if (etype === 'file') {
                const md5 = eid.split(':').pop();
                for (const node of graph.nodes_map.values()) {
                    if (node.md5 === md5) {
                        if (node.file_user_tags) return node.file_user_tags;
                    }
                }
            } else if (etype === 'similarity') {
                let latest = graph.all_pairs.find(p => p.sid === eid);
                if (!latest) {
                    const parts = eid.split('|');
                    if (parts.length >= 2) {
                        latest = graph.all_pairs.find(p =>
                            (p.id1 === parts[0] && p.id2 === parts[1]) ||
                            (p.id1 === parts[1] && p.id2 === parts[0])
                        );
                    }
                }
                if (latest && latest.user_tags) return latest.user_tags;
            }
        }

        // 3. Fallback to data object
        if (data) {
            return data.user_tags || data.file_user_tags || [];
        }

        return [];
    }

    function getEntityStaticTags(etype, eid, data = null) {
        if (!data && window.currentContextMenu) {
            data = window.currentContextMenu.data;
        }

        // 1. Try DOM elements first for static tags
        const domEditors = document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`);
        if (domEditors && domEditors.length > 0) {
            const editor = domEditors[0];
            const analysisCards = editor.querySelectorAll('.analysis-tag-badge');
            let analysisTags = Array.from(analysisCards).map(c => c.textContent.trim());
            if (analysisTags.length > 0) {
                return analysisTags;
            }
        }

        // 2. Try graph
        const graph = window.graphInstance;
        if (graph) {
            if (etype === 'function') {
                const latest = graph.nodes_map.get(eid);
                if (latest && latest.tags) return latest.tags;
            } else if (etype === 'file') {
                const md5 = eid.split(':').pop();
                for (const node of graph.nodes_map.values()) {
                    if (node.md5 === md5 && node.file_tags) return node.file_tags;
                }
            } else if (etype === 'similarity') {
                let latest = graph.all_pairs.find(p => p.sid === eid);
                if (!latest) {
                    const parts = eid.split('|');
                    if (parts.length >= 2) {
                        latest = graph.all_pairs.find(p =>
                            (p.id1 === parts[0] && p.id2 === parts[1]) ||
                            (p.id1 === parts[1] && p.id2 === parts[0])
                        );
                    }
                }
                if (latest && latest.tags) return latest.tags;
            }
        }

        // 3. Fallback to data
        if (data) {
            return data.tags || data.file_tags || [];
        }

        return [];
    }

    function renderCopyItem(label, text, icon = 'fa-copy') {
        if (text === null || text === undefined || text === '') return '';
        const strText = String(text);
        const safeText = strText.replace(/'/g, "\\'").replace(/"/g, "&quot;");
        const safeLabel = String(label).replace(/'/g, "\\'").replace(/"/g, "&quot;");
        return `
        <div class="context-menu-item" onclick="event.stopPropagation(); copyMetadata('${safeText}', '${safeLabel}')">
            <i class="fa-solid ${icon}" style="width: 16px; text-align: center; opacity: 0.8;"></i>
            <span>Copy ${label}</span>
        </div>`;
    }

    // Export tag helpers to window
    window.toggleContextMenuBookmark = async function (event, etype, eid) {
        const userTags = getEntityUserTags(etype, eid);
        const isBookmarked = userTags.includes('bookmark');

        if (isBookmarked) {
            await removeTag(null, etype, eid, 'bookmark');
        } else {
            await confirmAddTag(etype, eid, 'bookmark');
        }
    };

    window.toggleContextMenuIgnore = async function (event, etype, eid) {
        const userTags = getEntityUserTags(etype, eid);
        const isIgnored = userTags.includes('ignore');

        if (isIgnored) {
            await removeTag(null, etype, eid, 'ignore');
        } else {
            await confirmAddTag(etype, eid, 'ignore');
        }
    };

    window.toggleContextMenuTag = async function (event, etype, eid, tag) {
        const userTags = getEntityUserTags(etype, eid);
        const hasTag = userTags.includes(tag);
        if (hasTag) {
            await removeTag(null, etype, eid, tag);
        } else {
            await confirmAddTag(etype, eid, tag);
        }
    };

    window.removeContextMenuTag = async function (event, etype, eid, tag) {
        await removeTag(null, etype, eid, tag);
    };

    // Injected styles for Tag Management Modal
    if (!document.getElementById('tag-modal-style')) {
        const style = document.createElement('style');
        style.id = 'tag-modal-style';
        style.textContent = `
            .tag-modal-overlay {
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0, 0, 0, 0.75);
                backdrop-filter: blur(6px);
                z-index: 30000;
                display: none;
                align-items: center;
                justify-content: center;
                opacity: 0;
                transition: opacity 0.2s ease-out;
            }
            .tag-modal-overlay.active {
                opacity: 1;
            }
            .tag-modal-content {
                background: #181818;
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 12px;
                box-shadow: 0 20px 50px rgba(0, 0, 0, 0.7);
                width: 440px;
                max-width: 90vw;
                padding: 24px;
                transform: translateY(-20px);
                transition: transform 0.2s ease-out;
                display: flex;
                flex-direction: column;
                gap: 16px;
                color: #eee;
            }
            .tag-modal-overlay.active .tag-modal-content {
                transform: translateY(0);
            }
            .tag-modal-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                border-bottom: 1px solid rgba(255, 255, 255, 0.08);
                padding-bottom: 12px;
            }
            .tag-modal-header h3 {
                margin: 0;
                font-size: 1.1rem;
                color: var(--accent, #a6e22e);
                font-weight: 600;
            }
            .tag-modal-close {
                background: none;
                border: none;
                color: #888;
                font-size: 1.5rem;
                cursor: pointer;
                line-height: 1;
                padding: 0 4px;
                transition: color 0.15s ease;
            }
            .tag-modal-close:hover {
                color: #fff;
            }
            .tag-modal-target-info {
                font-size: 0.75rem;
                color: #aaa;
                word-break: break-all;
                background: rgba(255, 255, 255, 0.02);
                padding: 10px 14px;
                border-radius: 6px;
                border: 1px solid rgba(255, 255, 255, 0.05);
                font-family: var(--mono, monospace);
                line-height: 1.4;
            }
            .tag-modal-section-title {
                font-size: 0.65rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: #888;
                margin-bottom: 8px;
            }
            .tag-modal-active-tags {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                min-height: 26px;
            }
            .tag-modal-tag-pill {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 4px 10px;
                border-radius: 4px;
                font-size: 0.75rem;
                color: #000;
                font-weight: 600;
            }
            .tag-modal-tag-pill .remove-btn {
                cursor: pointer;
                font-weight: bold;
                opacity: 0.6;
                transition: opacity 0.15s ease;
                font-size: 0.9rem;
                margin-left: 2px;
            }
            .tag-modal-tag-pill .remove-btn:hover {
                opacity: 1;
            }
            .tag-modal-input-wrapper {
                display: flex;
                gap: 8px;
            }
            .tag-modal-input {
                flex: 1;
                background: #121212;
                border: 1px solid rgba(255, 255, 255, 0.15);
                border-radius: 6px;
                color: #fff;
                padding: 8px 12px;
                font-size: 0.85rem;
                outline: none;
                transition: border-color 0.15s ease;
            }
            .tag-modal-input:focus {
                border-color: var(--accent, #a6e22e);
            }
            .tag-modal-btn-add {
                background: var(--accent, #a6e22e);
                color: #000;
                border: none;
                border-radius: 6px;
                padding: 0 16px;
                font-size: 0.85rem;
                font-weight: 600;
                cursor: pointer;
                transition: opacity 0.15s ease;
            }
            .tag-modal-btn-add:hover {
                opacity: 0.9;
            }
            .tag-modal-suggestions {
                max-height: 150px;
                overflow-y: auto;
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 6px;
                background: #121212;
            }
            .tag-modal-suggestion-item {
                padding: 8px 12px;
                display: flex;
                align-items: center;
                gap: 8px;
                cursor: pointer;
                font-size: 0.8rem;
                transition: background 0.15s ease;
                color: #ccc;
            }
            .tag-modal-suggestion-item:hover {
                background: rgba(255, 255, 255, 0.05);
                color: #fff;
            }
            .tag-modal-suggestion-item.active-tag {
                opacity: 0.4;
                cursor: not-allowed;
            }
        `;
        document.head.appendChild(style);
    }

    window.closeTagManagementModal = function () {
        const overlay = document.getElementById('tag-management-modal');
        if (overlay) {
            overlay.classList.remove('active');
            setTimeout(() => {
                overlay.style.display = 'none';
            }, 200);
        }
        if (window._tagModalEscHandler) {
            document.removeEventListener('keydown', window._tagModalEscHandler);
            window._tagModalEscHandler = null;
        }
        window.refreshTagModalUI = null;
    };

    window.handleModalAddTag = async function (etype, eid, tag) {
        if (!tag || !tag.trim()) return;
        const success = await confirmAddTag(etype, eid, tag.trim());
        if (success) {
            const input = document.getElementById('tag-modal-input-field');
            if (input) input.value = '';
            if (window.refreshTagModalUI) window.refreshTagModalUI();
        }
    };

    window.handleModalRemoveTag = async function (etype, eid, tag) {
        await removeTag(null, etype, eid, tag);
        if (window.refreshTagModalUI) window.refreshTagModalUI();
    };

    window.showTagManagementModal = function (etype, eid) {
        let overlay = document.getElementById('tag-management-modal');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'tag-management-modal';
            overlay.className = 'tag-modal-overlay';
            document.body.appendChild(overlay);
        }

        overlay.innerHTML = `
            <div class="tag-modal-content" onclick="event.stopPropagation()">
                <div class="tag-modal-header">
                    <h3>Manage Tags</h3>
                    <button class="tag-modal-close" onclick="window.closeTagManagementModal()">&times;</button>
                </div>
                <div class="tag-modal-target-info">
                    <strong>Entity:</strong> ${etype.toUpperCase()} <br/>
                    <strong>ID:</strong> ${eid}
                </div>
                
                <div>
                    <div class="tag-modal-section-title">Active Tags</div>
                    <div id="tag-modal-active-list" class="tag-modal-active-tags"></div>
                </div>

                <div>
                    <div class="tag-modal-section-title">Add Tag</div>
                    <div class="tag-modal-input-wrapper">
                        <input type="text" id="tag-modal-input-field" class="tag-modal-input" placeholder="Search or type new tag..." autocomplete="off">
                        <button id="tag-modal-btn-submit" class="tag-modal-btn-add">Add</button>
                    </div>
                </div>

                <div>
                    <div class="tag-modal-section-title">Existing Tags / Suggestions</div>
                    <div id="tag-modal-suggestions-list" class="tag-modal-suggestions"></div>
                </div>
            </div>
        `;

        overlay.style.display = 'flex';
        setTimeout(() => overlay.classList.add('active'), 10);

        const input = document.getElementById('tag-modal-input-field');
        const btnSubmit = document.getElementById('tag-modal-btn-submit');

        input.focus();

        const updateModalUI = () => {
            const activeTags = getEntityUserTags(etype, eid).filter(t => t !== 'bookmark' && t !== 'ignore');
            const activeList = document.getElementById('tag-modal-active-list');
            const tagMeta = window.tagMetadata || (window.parent && window.parent.tagMetadata) || {};
            
            if (activeList) {
                if (activeTags.length === 0) {
                    activeList.innerHTML = `<span style="font-style: italic; font-size: 0.75rem; color: var(--dim, #888);">No custom tags applied.</span>`;
                } else {
                    activeList.innerHTML = activeTags.map(tag => {
                        const meta = tagMeta[tag] || { color: '#66d9ef' };
                        return `
                            <span class="tag-modal-tag-pill" style="background: ${meta.color}">
                                ${tag}
                                <span class="remove-btn" onclick="window.handleModalRemoveTag('${etype}', '${eid}', '${String(tag).replace(/'/g, "\\'")}')">&times;</span>
                            </span>
                        `;
                    }).join('');
                }
            }

            const query = input.value.trim().toLowerCase();
            const suggestionsList = document.getElementById('tag-modal-suggestions-list');
            
            if (suggestionsList) {
                const allAvailable = Object.keys(tagMeta).filter(t => t !== 'bookmark' && t !== 'ignore');
                const filtered = allAvailable.filter(t => t.toLowerCase().includes(query));

                if (filtered.length === 0) {
                    if (query) {
                        suggestionsList.innerHTML = `
                            <div class="tag-modal-suggestion-item" onclick="window.handleModalAddTag('${etype}', '${eid}', '${String(query).replace(/'/g, "\\'")}')">
                                <i class="fa-solid fa-plus" style="color: var(--accent, #a6e22e);"></i>
                                <span>Create new tag: <strong>${query}</strong></span>
                            </div>
                        `;
                    } else {
                        suggestionsList.innerHTML = `<div style="padding: 12px; font-size: 0.75rem; font-style: italic; color: var(--dim, #888);">No existing tags found. Type above to create one.</div>`;
                    }
                } else {
                    suggestionsList.innerHTML = filtered.map(tag => {
                        const isApplied = activeTags.includes(tag);
                        const meta = tagMeta[tag] || { color: '#66d9ef' };
                        const clickAction = isApplied ? '' : `onclick="window.handleModalAddTag('${etype}', '${eid}', '${String(tag).replace(/'/g, "\\'")}')"`;
                        const activeClass = isApplied ? 'active-tag' : '';
                        return `
                            <div class="tag-modal-suggestion-item ${activeClass}" ${clickAction}>
                                <span style="display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: ${meta.color}"></span>
                                <span>${tag}</span>
                                ${isApplied ? '<span style="margin-left: auto; font-size: 0.7rem; color: var(--dim, #888);">Applied</span>' : ''}
                            </div>
                        `;
                    }).join('');
                }
            }
        };

        window.refreshTagModalUI = updateModalUI;
        updateModalUI();

        input.oninput = updateModalUI;
        input.onkeydown = async (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                const val = input.value.trim();
                if (val) {
                    await window.handleModalAddTag(etype, eid, val);
                }
            }
        };

        btnSubmit.onclick = async () => {
            const val = input.value.trim();
            if (val) {
                await window.handleModalAddTag(etype, eid, val);
            }
        };

        overlay.onclick = window.closeTagManagementModal;

        const escHandler = (e) => {
            if (e.key === 'Escape') {
                window.closeTagManagementModal();
            }
        };
        document.addEventListener('keydown', escHandler);
        window._tagModalEscHandler = escHandler;
    };

    // Global cross-window message listener to keep modal sync'd
    window.addEventListener('message', (event) => {
        const msg = event.data;
        if (msg && msg.type === 'bsimvis_tag_update') {
            if (window.currentContextMenu && window.currentContextMenu.data) {
                const { action, tag, targets } = msg;
                if (tag && targets) {
                    targets.forEach(t => {
                        const cm = window.currentContextMenu;
                        if (cm.type === 'function' && t.etype === 'function' && 
                            (cm.data.function_id === t.eid || cm.data.id === t.eid)) {
                            cm.data.user_tags = cm.data.user_tags || [];
                            if (action === 'add' && !cm.data.user_tags.includes(tag)) cm.data.user_tags.push(tag);
                            if (action === 'remove') cm.data.user_tags = cm.data.user_tags.filter(x => x !== tag);
                        } else if (cm.type === 'file' && t.etype === 'file' && 
                            (cm.data.md5 === t.eid || cm.data.id === t.eid || (cm.data.id && cm.data.id.endsWith(t.eid)))) {
                            cm.data.file_user_tags = cm.data.file_user_tags || [];
                            if (action === 'add' && !cm.data.file_user_tags.includes(tag)) cm.data.file_user_tags.push(tag);
                            if (action === 'remove') cm.data.file_user_tags = cm.data.file_user_tags.filter(x => x !== tag);
                        } else if ((cm.type === 'similarity' || cm.type === 'link') && t.etype === 'similarity') {
                            cm.data.user_tags = cm.data.user_tags || [];
                            if (action === 'add' && !cm.data.user_tags.includes(tag)) cm.data.user_tags.push(tag);
                            if (action === 'remove') cm.data.user_tags = cm.data.user_tags.filter(x => x !== tag);
                        }
                    });
                }
            }
            if (typeof window.refreshTagModalUI === 'function') {
                window.refreshTagModalUI();
            }
            if (typeof window.refreshContextMenuUI === 'function' && window.graphContextMenuOpen) {
                window.refreshContextMenuUI();
            }
        }
    });
})();
