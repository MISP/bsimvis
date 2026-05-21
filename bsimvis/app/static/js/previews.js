// Shared Preview Tooltips for BSimVis
// Provides: showCodePreview, hideCodePreview, moveCodePreview
//           showClusterTableTooltip, hideClusterTableTooltip, moveClusterTableTooltip
//           showCodePreviewFromIframe, moveCodePreviewFromIframe
// Works standalone (no parent dashboard required).

(function () {
    // --- Internal state ---
    const previewCache = new Map();
    let previewTimer = null;
    let activePreviewId = null;

    window.previewTips = window.previewTips || {};
    window.previewCollection = window.previewCollection || '';

    // --- DOM helpers: lazily create tooltip elements ---
    function ensureEl(id, styles) {
        let el = document.getElementById(id);
        if (!el && document.body) {
            el = document.createElement('div');
            el.id = id;
            Object.assign(el.style, styles);
            document.body.appendChild(el);
        }
        return el;
    }

    function getCodeTooltip() {
        return ensureEl('code-preview-tooltip', {
            position: 'fixed',
            display: 'none',
            zIndex: '20000',
            pointerEvents: 'none',
            maxWidth: '480px',
            maxHeight: '800px',
            overflow: 'visible',
            flexDirection: 'column',
            gap: '10px',
        });
    }

    function getTokenTooltip() {
        return ensureEl('token-tooltip', {
            position: 'fixed',
            pointerEvents: 'none',
            zIndex: '20002',
            background: '#121212',
            color: '#fff',
            border: '1px solid #a6e22e',
            borderRadius: '4px',
            padding: '10px',
            fontSize: '0.8em',
            lineHeight: '1.2',
            maxWidth: '320px',
            boxShadow: '0 4px 10px black',
            display: 'none',
        });
    }

    function getDiffTooltip() {
        return ensureEl('diff-preview-tooltip', {});
    }

    function getClusterTooltip() {
        return ensureEl('hierarchy-tooltip', {
            position: 'fixed',
            zIndex: '20003',
            background: 'rgba(13,15,20,0.98)',
            borderRadius: '8px',
            border: '1px solid var(--accent, #66d9ef)',
            display: 'none',
            pointerEvents: 'auto',
            fontSize: '0.8rem',
            boxShadow: '0 15px 50px rgba(0,0,0,0.9)',
            backdropFilter: 'blur(15px)',
            overflow: 'hidden',
        });
    }

    function getBinaryTooltip() {
        return ensureEl('binary-preview-tooltip', {
            position: 'fixed',
            display: 'none',
            zIndex: '20000',
            pointerEvents: 'none',
            minWidth: '280px',
            maxWidth: '400px',
            flexDirection: 'column',
            gap: '10px',
        });
    }

    // --- Code Preview ---
    window.moveCodePreview = function (e) {
        if (!e) return;
        const offset = 15;
        const els = [
            getCodeTooltip(),
            document.getElementById('token-tooltip'),
            document.getElementById('diff-preview-tooltip'),
            getBinaryTooltip(),
        ];
        els.forEach(el => {
            if (el && (el.style.display === 'block' || el.style.display === 'flex' || el.classList.contains('showing'))) {
                let x = e.clientX + offset;
                let y = e.clientY + offset;
                const rect = el.getBoundingClientRect();
                
                // Keep within viewport
                if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - offset;
                if (y + rect.height > window.innerHeight) y = e.clientY - rect.height - offset;
                
                // Ensure it doesn't go off-screen on the top/left
                x = Math.max(5, x);
                y = Math.max(5, y);
                
                el.style.left = x + 'px';
                el.style.top = y + 'px';
                el.classList.add('showing');
            }
        });
    };

    window.hideCodePreview = function (e) {
        if (previewTimer) clearTimeout(previewTimer);
        const tooltip = getCodeTooltip();
        if (e && e.relatedTarget && (tooltip.contains(e.relatedTarget) || e.relatedTarget === tooltip)) return;
        tooltip.style.display = 'none';
        tooltip.classList.remove('showing');
        activePreviewId = null;
    };

    window.showCodePreview = async function (id, name, addr, bin, v_size, e, extra = 0, file_name = '') {
        if (!id) return;
        const tooltip = getCodeTooltip();
        if (id === activePreviewId) {
            window.moveCodePreview(e);
            return;
        }
        activePreviewId = id;
        const collection = id.split(':')[0];
        if (previewTimer) clearTimeout(previewTimer);

        tooltip.style.display = 'flex';
        tooltip.classList.add('showing');
        window.moveCodePreview(e);

        const extraHtml = extra > 0 ? `<div class="others-count-card"><span>⚡</span> And ${extra} other functions...</div>` : '';

        tooltip.innerHTML = `
            <div class="preview-card">
                <div class="preview-header">Quick Preview: ${name || id.split(':').pop()}</div>
                <div style="font-size:0.65rem; color:var(--accent,#66d9ef); font-family:monospace; padding:0 8px; margin-bottom:5px;">
                    Addr: ${addr || '---'} | Bin: ${file_name || bin || '---'} | Feat: ${v_size || 0}
                </div>
                <div style="font-size:0.55rem; color:var(--subtle,#75715e); padding:0 8px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">ID: ${id}</div>
                <div class="preview-header" style="border:none; margin-top:10px;">Loading Code...</div>
            </div>
            ${extraHtml}
        `;

        if (!tooltip.dataset.hasLeave) {
            tooltip.addEventListener('mouseleave', (me) => { 
                // Only hide if we aren't moving back to the trigger element
                if (me.relatedTarget && me.relatedTarget.classList && me.relatedTarget.classList.contains('func-call-clickable')) return;
                window.hideCodePreview(); 
            });
            tooltip.dataset.hasLeave = 'true';
        }

        previewTimer = setTimeout(async () => {
            if (previewCache.has(id)) {
                const cached = previewCache.get(id);
                window.previewTips = cached.tips;
                window.previewCollection = collection;
                _renderCodePreview(tooltip, cached, name, addr, bin, v_size, extra, file_name);
                // Re-position after content loads to account for new dimensions
                window.moveCodePreview(e);
                return;
            }
            try {
                const res = await fetch(`/api/function/code?id=${encodeURIComponent(id)}`);
                if (!res.ok) throw new Error('Preview failed');
                const data = await res.json();
                window.previewTips = data.tips;
                window.previewCollection = collection;
                previewCache.set(id, data);
                _renderCodePreview(tooltip, data, name, addr, bin, v_size, extra, file_name);
                // Re-position after content loads
                window.moveCodePreview(e);
            } catch (err) {
                tooltip.innerHTML = `<div class="preview-header" style="color:#ff5555">Error loading preview</div>`;
            }
        }, 50);
    };

    function _renderCodePreview(tooltip, data, name, addr, bin, v_size, extra = 0, file_name = '') {
        const rows = data.rows;
        const m = data.meta || {};
        const ns = m.namespace || '';
        const parameters = m.parameters || [];
        const retType = m.return_type || '';

        const displayAddr = addr || m.entrypoint_address || '---';
        const displayBin = file_name || bin || m.file_name || '---';
        const displayFeat = v_size || m.bsim_features_count || 0;
        const displayName = name || m.function_name || 'Function';
        const extraHtml = extra > 0 ? `<div class="others-count-card"><span>⚡</span> And ${extra} other functions...</div>` : '';

        let html = `
            <div class="preview-card" style="max-height:450px; display:flex; flex-direction:column;">
                <div style="flex-shrink:0; border-bottom:1px solid rgba(255,255,255,0.05); padding-bottom:8px; margin-bottom:8px;">
                    <div class="preview-header" style="border:none; margin-bottom:4px; padding:0;">Quick Preview: <span style="color:#ae81ff">${retType}</span> ${ns ? `<span style="color:white">${ns}::</span>` : ''}${displayName}<span style="color:white">(</span>${parameters.map(p => `<span style="color:#ae81ff">${typeof p === 'object' && p !== null ? (p.name || JSON.stringify(p)) : p}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span></div>
                    <div style="font-size:0.65rem; color:var(--accent,#66d9ef); font-family:monospace; padding:0 8px;">
                        Addr: ${displayAddr} | Bin: ${displayBin} | Feat: ${displayFeat}
                    </div>
                </div>
                <div class="c-code-container code-preview-scroll" style="border:none; margin:0; padding:0; background:transparent; overflow-y:auto; overflow-x:auto; flex:1; min-height:0;">`;

        rows.forEach(row => {
            let lineHtml = '';
            row.tokens.forEach(t => {
                const featClass = t.has_features ? 'feature-highlight' : '';
                const hashes = (t.hash_list || []).join(' ');
                // Show func_call tokens with hover-to-preview support
                const calledAttr = t.called_func_id ? `data-called-func-id="${t.called_func_id}" data-is-external="${t.is_external}" data-target-name="${t.target_name || ''}"` : '';
                const clickClass = t.called_func_id ? (t.is_external ? 'func-call-external' : 'func-call-clickable') : '';
                lineHtml += `<span class="token token-${t.type} ${featClass} ${clickClass}" data-idx="${t.global_idx}" data-hashes="${hashes}" ${calledAttr}
                    onmouseenter="window.showTokenTooltip && showTokenTooltip(event)" onmouseleave="window.hideTokenTooltip && hideTokenTooltip()" onmousemove="window.moveCodePreview(event)">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
            });
            html += `<div class="code-line"><div class="gutter" style="background:transparent;"><div class="line-num">${row.line_idx}</div></div><div class="line-content">${lineHtml}</div></div>`;
        });
        html += '</div>';

        if (rows.length > 18) {
            html += `<div style="text-align:center; font-size:0.65rem; color:var(--subtle,#75715e); padding-top:8px; border-top:1px solid rgba(255,255,255,0.05); flex-shrink:0;">💡 Use scroll wheel to view all ${rows.length} lines</div>`;
        }

        html += '</div>';
        html += extraHtml;
        tooltip.innerHTML = html;
    }

    // --- Iframe bridge functions (expose on window) ---
    window.showCodePreviewFromIframe = function (iframeId, id, name, e) {
        const iframe = document.getElementById(iframeId);
        if (!iframe) {
            // Standalone: show directly from coordinates
            window.showCodePreview(id, name, null, null, null, e);
            return;
        }
        const rect = iframe.getBoundingClientRect();
        const fakeEvent = { clientX: e.clientX + rect.left, clientY: e.clientY + rect.top };
        window.showCodePreview(id, name, null, null, null, fakeEvent);
    };

    window.moveCodePreviewFromIframe = function (iframeId, e) {
        const iframe = document.getElementById(iframeId);
        if (!iframe) {
            window.moveCodePreview(e);
            return;
        }
        const rect = iframe.getBoundingClientRect();
        const fakeEvent = { clientX: e.clientX + rect.left, clientY: e.clientY + rect.top };
        window.moveCodePreview(fakeEvent);
    };

    // --- Cluster Preview (standalone, no ClusterHierarchy required) ---


    // --- Token tooltip (shared) ---
    window.showTokenTooltip = function (e) {
        const token = e.target.closest('.feature-highlight');
        if (!token) return;

        const tooltipEl = getTokenTooltip();
        const idx = token.dataset.idx;
        const hashesStr = token.dataset.hashes || '';
        const hashes = hashesStr.trim().split(/\s+/);

        let html = '';
        if (idx !== undefined && window.previewTips && window.previewTips[idx]) {
            const data = window.previewTips[idx];
            html = `<div style="font-weight:bold; color:var(--accent,#66d9ef); border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:5px;">Features (${data[1]})</div>`;
            data[2].forEach(f => {
                const color = f[8] || 'var(--accent,#66d9ef)';
                html += `<div style="margin-bottom:8px;">
                    <div style="font-family:monospace; color:${color}; font-weight:bold;">${f[0]}</div>
                    <div style="font-size:0.7rem; color:var(--subtle,#75715e);">${f[3]} | Op: ${f[1]} | <b style="color:var(--success,#a6e22e)">TF: ${f[7] || 0}</b></div>
                </div>`;
            });
        } else {
            html = `<div style="font-weight:bold; color:var(--accent,#66d9ef); border-bottom:1px solid #333; padding-bottom:5px; margin-bottom:5px;">Features (${hashes.length})</div>`;
            hashes.forEach(h => {
                html += `<div style="margin-bottom:8px;">
                    <div style="font-family:monospace; color:var(--accent,#66d9ef); font-weight:bold;">${h}</div>
                </div>`;
            });
        }

        tooltipEl.innerHTML = html;
        tooltipEl.style.display = 'block';
        tooltipEl.style.left = (e.clientX + 15) + 'px';
        tooltipEl.style.top = (e.clientY + 15) + 'px';

        const rect = tooltipEl.getBoundingClientRect();
        if (rect.right > window.innerWidth) tooltipEl.style.left = (e.clientX - rect.width - 15) + 'px';
        if (rect.bottom > window.innerHeight) tooltipEl.style.top = (e.clientY - rect.height - 15) + 'px';
    };

    window.hideTokenTooltip = function () {
        const el = document.getElementById('token-tooltip');
        if (el) el.style.display = 'none';
    };

    // --- Wheel scroll intercept for preview tooltips ---
    window.addEventListener('wheel', e => {
        const codeTooltip = document.getElementById('code-preview-tooltip');
        const diffTooltip = document.getElementById('diff-preview-tooltip');
        
        const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
        const hierTooltip = targetWindow.document.getElementById('hierarchy-tooltip');

        const isCodeActive = codeTooltip && (codeTooltip.style.display === 'flex' || codeTooltip.classList.contains('showing'));
        const isDiffActive = diffTooltip && (diffTooltip.style.display === 'flex' || diffTooltip.classList.contains('showing'));
        const isHierActive = hierTooltip && hierTooltip.style.display === 'block';

        if (isCodeActive || isDiffActive || isHierActive) {
            e.preventDefault();
            e.stopPropagation();
            
            if (isHierActive) {
                if (e.ctrlKey) {
                    const codeScrollEl = hierTooltip.querySelector('#hier-snippet-container .c-code-container');
                    if (codeScrollEl) {
                        codeScrollEl.scrollTop += e.deltaY;
                        if (targetWindow.hierarchyInstance) {
                            targetWindow.hierarchyInstance._codeScrollTop = codeScrollEl.scrollTop;
                        }
                    }
                    return;
                }

                const activeInstance = (targetWindow.hierarchyInstance && targetWindow.hierarchyInstance._activeD)
                    ? targetWindow.hierarchyInstance
                    : ((targetWindow.packingInstance && targetWindow.packingInstance._activeD) ? targetWindow.packingInstance : null);

                if (activeInstance && activeInstance._activeD) {
                    const d = activeInstance._activeD;
                    const delta = Math.sign(e.deltaY);
                    const members = d.data.runtime_members || [];
                    if (members.length === 0) return;

                    if (d.data.scrollOffset === undefined) d.data.scrollOffset = 0;
                    d.data.scrollOffset = Math.max(0, Math.min(members.length - 1, d.data.scrollOffset + delta));
                    activeInstance.renderTooltip(hierTooltip, d);
                }
                return;
            }

            const activeTooltip = isDiffActive ? diffTooltip : codeTooltip;
            const scrollContainer = activeTooltip.querySelector('.code-preview-scroll, .diff-preview-scroll');
            
            if (isDiffActive) {
                if (e.ctrlKey) {
                    if (scrollContainer) {
                        scrollContainer.scrollTop += e.deltaY;
                        if (e.deltaX) scrollContainer.scrollLeft += e.deltaX;
                    }
                } else {
                    const diffPairs = targetWindow.diffPreviewPairs || window.diffPreviewPairs;
                    if (diffPairs && diffPairs.length > 1) {
                        const delta = Math.sign(e.deltaY);
                        let idx = targetWindow.diffPreviewIndex !== undefined ? targetWindow.diffPreviewIndex : (window.diffPreviewIndex || 0);
                        idx = Math.max(0, Math.min(diffPairs.length - 1, idx + delta));
                        if (targetWindow.diffPreviewIndex !== undefined) targetWindow.diffPreviewIndex = idx;
                        else window.diffPreviewIndex = idx;
                        
                        const p = diffPairs[idx];
                        const extra = diffPairs.length - 1;
                        const showFunc = targetWindow.showDiffPreview || window.showDiffPreview;
                        if (showFunc) showFunc(p.id1, p.n1, p.id2, p.n2, p.score, e, extra);
                    } else if (scrollContainer) {
                        scrollContainer.scrollTop += e.deltaY;
                        if (e.deltaX) scrollContainer.scrollLeft += e.deltaX;
                    }
                }
            } else if (scrollContainer) {
                scrollContainer.scrollTop += e.deltaY;
                if (e.deltaX) scrollContainer.scrollLeft += e.deltaX;
            }
        }
    }, { passive: false, capture: true });

    // Global mouse tracking for tooltip follow
    document.addEventListener('mousemove', window.moveCodePreview);

    // Failsafe: hide tooltips if the window loses focus or mouse leaves the document
    window.addEventListener('blur', () => {
        window.hideCodePreview();
        window.hideBinaryPreview();
    });
    document.addEventListener('mouseleave', (e) => {
        if (!e.relatedTarget) {
            window.hideCodePreview();
            window.hideBinaryPreview();
        }
    });

    window.showBinaryPreview = function (md5, fileName, count, language, tags, e, fileTags = [], fileUserTags = []) {
        const tooltip = getBinaryTooltip();
        
        // Build tags html
        const allTags = [...(fileTags || []), ...(fileUserTags || [])].filter(t => t && t.trim());
        let tagsHtml = '';
        if (allTags.length > 0) {
            const tagBadges = allTags.map(tag => {
                const isBookmark = tag === 'bookmark';
                const isIgnore = tag === 'ignore';
                let color = '#66d9ef';
                if (isBookmark) color = '#66d9ef';
                else if (isIgnore) color = '#f92672';
                else if (window.getTagMetadata) {
                    color = window.getTagMetadata(tag).color;
                }
                
                return `
                <span class="tag-card" style="border-color:${color}44; color:${color}; background:${color}11; font-size: 0.65rem; padding: 2px 6px; border-radius: 12px; margin: 2px 4px 2px 0; display: inline-flex; align-items: center; gap: 4px;">
                    ${isBookmark ? '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="2"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>' : ''}
                    ${isIgnore ? '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>' : ''}
                    ${tag}
                </span>`;
            }).join('');
            
            tagsHtml = `
            <div style="margin-top: 10px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 8px;">
                <div style="font-size: 0.65rem; color: #777; text-transform: uppercase; margin-bottom: 4px;"><i class="fa-solid fa-tags" style="margin-right: 6px; opacity: 0.5;"></i>Tags</div>
                <div style="display: flex; flex-wrap: wrap;">${tagBadges}</div>
            </div>`;
        }

        tooltip.innerHTML = `
        <div class="func-meta-card modern" style="border: 1px solid var(--accent, #66d9ef); box-shadow: 0 15px 50px rgba(0,0,0,0.9); background: rgba(13,15,20,0.98); backdrop-filter: blur(15px); padding: 12px; border-radius: 8px; margin-bottom: 0;">
            <div style="font-weight: bold; font-size: 0.95rem; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 6px; margin-bottom: 8px; color: #fff; display: flex; align-items: center; gap: 8px; font-family: 'Inter', sans-serif;">
                <i class="fa-solid fa-file" style="color: var(--accent);"></i>
                <span>${fileName}</span>
            </div>
            
            <div style="display: flex; flex-direction: column; gap: 4px; font-family: 'Inter', sans-serif;">
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; border-bottom: 1px solid rgba(255,255,255,0.03); padding: 2px 0;">
                    <span style="color: #777; text-transform: uppercase;"><i class="fa-solid fa-hashtag" style="margin-right: 6px; opacity: 0.5; width: 14px;"></i>MD5</span>
                    <span class="mono" style="color: var(--accent); font-weight: bold; font-family: 'JetBrains Mono', 'Consolas', monospace;">${md5}</span>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; border-bottom: 1px solid rgba(255,255,255,0.03); padding: 2px 0;">
                    <span style="color: #777; text-transform: uppercase;"><i class="fa-solid fa-list-ol" style="margin-right: 6px; opacity: 0.5; width: 14px;"></i>Functions</span>
                    <span class="mono" style="color: #a6e22e; font-family: 'JetBrains Mono', 'Consolas', monospace;">${count}</span>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; border-bottom: 1px solid rgba(255,255,255,0.03); padding: 2px 0;">
                    <span style="color: #777; text-transform: uppercase;"><i class="fa-solid fa-globe" style="margin-right: 6px; opacity: 0.5; width: 14px;"></i>Language</span>
                    <span class="mono" style="color: #ae81ff; font-family: 'JetBrains Mono', 'Consolas', monospace;">${language}</span>
                </div>
            </div>
            ${tagsHtml}
        </div>`;
        
        tooltip.style.display = 'flex';
        window.moveCodePreview(e);
    };

    window.hideBinaryPreview = function () {
        const tooltip = getBinaryTooltip();
        tooltip.style.display = 'none';
        tooltip.classList.remove('showing');
    };

})();
