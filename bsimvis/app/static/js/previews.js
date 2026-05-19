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
        if (!el) {
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
            maxWidth: '600px',
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

    // --- Code Preview ---
    window.moveCodePreview = function (e) {
        if (!e) return;
        const offset = 15;
        const els = [
            getCodeTooltip(),
            document.getElementById('token-tooltip'),
            document.getElementById('diff-preview-tooltip'),
            document.getElementById('binary-preview-tooltip'),
        ];
        els.forEach(el => {
            if (el && (el.style.display === 'block' || el.style.display === 'flex' || el.classList.contains('showing'))) {
                let x = e.clientX + offset;
                let y = e.clientY + offset;
                const rect = el.getBoundingClientRect();
                if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - offset;
                if (y + rect.height > window.innerHeight) y = e.clientY - rect.height - offset;
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
            tooltip.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
            tooltip.dataset.hasLeave = 'true';
        }

        previewTimer = setTimeout(async () => {
            if (previewCache.has(id)) {
                const cached = previewCache.get(id);
                window.previewTips = cached.tips;
                window.previewCollection = collection;
                _renderCodePreview(tooltip, cached, name, addr, bin, v_size, extra, file_name);
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
                    <div class="preview-header" style="border:none; margin-bottom:4px; padding:0;">Quick Preview: <span style="color:#ae81ff">${retType}</span> ${ns ? `<span style="color:white">${ns}::</span>` : ''}${displayName}<span style="color:white">(</span>${parameters.map(p => `<span style="color:#ae81ff">${p}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span></div>
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
    const _clusterCache = new Map();

    window.showClusterTableTooltip = function (event, uuid, name, size, stability, cohesion, avg_features) {
        const tooltip = getClusterTooltip();

        const hue = Math.max(0, Math.min(120, (cohesion || 0) * 120));
        const cohColor = `hsl(${hue}, 100%, 65%)`;
        const stabColor = `hsl(${Math.max(0, Math.min(120, (stability || 0) * 120))}, 100%, 65%)`;

        const cohPct = ((cohesion || 0) * 100).toFixed(1);
        const stabFmt = (stability || 0).toFixed(2);
        const featFmt = (avg_features || 0).toFixed(1);
        const shortUuid = (uuid || '').substring(0, 8);

        tooltip.innerHTML = `
            <div style="padding:14px 18px; min-width:240px; max-width:320px;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <div style="font-weight:bold; font-size:0.95rem; color:#fff; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:180px;" title="${name}">${name || 'Cluster'}</div>
                    <span style="font-family:monospace; font-size:0.65rem; color:var(--subtle,#75715e); background:rgba(255,255,255,0.05); padding:2px 6px; border-radius:4px;">${shortUuid}</span>
                </div>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="color:var(--subtle,#75715e); font-size:0.7rem; width:70px; flex-shrink:0;">Members</span>
                        <span style="font-weight:bold; color:#fff; font-size:0.85rem;">${size || 0}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="color:var(--subtle,#75715e); font-size:0.7rem; width:70px; flex-shrink:0;">Cohesion</span>
                        <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden;">
                            <div style="height:100%; background:${cohColor}; width:${cohPct}%;"></div>
                        </div>
                        <span style="font-family:monospace; font-size:0.75rem; color:${cohColor}; min-width:38px; text-align:right;">${cohPct}%</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="color:var(--subtle,#75715e); font-size:0.7rem; width:70px; flex-shrink:0;">Stability</span>
                        <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden;">
                            <div style="height:100%; background:${stabColor}; width:${Math.min(100,(stability||0)*100).toFixed(0)}%;"></div>
                        </div>
                        <span style="font-family:monospace; font-size:0.75rem; color:${stabColor}; min-width:38px; text-align:right;">${stabFmt}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="color:var(--subtle,#75715e); font-size:0.7rem; width:70px; flex-shrink:0;">Avg Feat</span>
                        <span style="font-family:monospace; font-size:0.75rem; color:var(--info,#ae81ff);">${featFmt}</span>
                    </div>
                </div>
            </div>`;

        tooltip.style.display = 'block';
        _positionClusterTooltip(tooltip, event);
    };

    window.hideClusterTableTooltip = function () {
        const tooltip = document.getElementById('hierarchy-tooltip');
        if (tooltip) {
            tooltip.style.display = 'none';
        }
    };

    window.moveClusterTableTooltip = function (e) {
        const tooltip = document.getElementById('hierarchy-tooltip');
        if (tooltip && tooltip.style.display === 'block') {
            _positionClusterTooltip(tooltip, e);
        }
    };

    function _positionClusterTooltip(tooltip, e) {
        let x = e.clientX + 20;
        let y = e.clientY + 20;
        const rect = tooltip.getBoundingClientRect();
        if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - 20;
        if (y + rect.height > window.innerHeight) y = Math.max(10, e.clientY - rect.height - 20);
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }

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

        const isCodeActive = codeTooltip && (codeTooltip.style.display === 'flex' || codeTooltip.classList.contains('showing'));
        const isDiffActive = diffTooltip && (diffTooltip.style.display === 'flex' || diffTooltip.classList.contains('showing'));

        if (isCodeActive || isDiffActive) {
            e.preventDefault();
            e.stopPropagation();
            const activeTooltip = isDiffActive ? diffTooltip : codeTooltip;
            const scrollContainer = activeTooltip.querySelector('.code-preview-scroll, .diff-preview-scroll');
            if (scrollContainer) {
                scrollContainer.scrollTop += e.deltaY;
                if (e.deltaX) scrollContainer.scrollLeft += e.deltaX;
            }
        }
    }, { passive: false, capture: true });

    // Global mouse tracking for tooltip follow
    document.addEventListener('mousemove', window.moveCodePreview);

})();
