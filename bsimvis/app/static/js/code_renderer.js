// Shared Code Renderer and Highlight Lock Manager for BSimVis

window.lockedHashes = new Set();

window.clearAllLocks = function () {
    document.querySelectorAll('.feature-locked, .bsim-group-active-match, .bsim-group-active-unique').forEach(el => {
        el.classList.remove('feature-locked', 'bsim-group-active-match', 'bsim-group-active-unique');
    });
    window.lockedHashes.clear();
};

window.setHighlight = function (hashString, state, target) {
    if (!hashString) return;
    const hashes = hashString.trim().split(/\s+/);
    hashes.forEach(h => {
        if (!state && window.lockedHashes.has(h)) return;
        document.querySelectorAll('.feat-' + h).forEach(el => {
            const activeClass = el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique';
            el.classList.toggle(activeClass, state);
        });
    });
};

window.toggleLock = function (hashString, target) {
    if (!hashString || !target) return;
    const hashes = hashString.trim().split(/\s+/);
    const isAlreadyLocked = hashes.some(h => window.lockedHashes.has(h));
    window.clearAllLocks();
    if (!isAlreadyLocked) {
        hashes.forEach(h => {
            window.lockedHashes.add(h);
            document.querySelectorAll('.feat-' + h).forEach(el => {
                el.classList.add('feature-locked');
                const activeClass = el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique';
                el.classList.add(activeClass);
            });
        });
    }
};

window.applyLocks = function (container) {
    if (!window.lockedHashes || !window.lockedHashes.size) return;
    const root = container || document;
    window.lockedHashes.forEach(h => {
        root.querySelectorAll('.feat-' + h).forEach(el => {
            el.classList.add('feature-locked');
            const activeClass = el.classList.contains('diff-match') ? 'bsim-group-active-match' : 'bsim-group-active-unique';
            el.classList.add(activeClass);
        });
    });
};

window.renderTokenHtml = function (t, options = {}) {
    if (!t) return '';
    const featClass = t.has_features ? 'feature-highlight' : '';
    const hashes = (t.hash_list || []).join(' ');
    const featClasses = (t.hash_list || []).map(h => `feat-${h}`).join(' ');

    const calledAttr = t.called_func_id ? `data-called-func-id="${t.called_func_id}" data-is-external="${t.is_external || false}" data-target-name="${t.target_name || ''}"` : '';
    const clickClass = t.called_func_id ? (t.is_external ? 'func-call-external' : 'func-call-clickable') : '';
    const titleAttr = t.called_func_id ? `title="Click to navigate to ${t.target_name || 'called function'}"` : '';

    const diffClass = t.diff_class || '';
    const sideAttr = options.side ? `data-side="${options.side}"` : '';

    const escapedText = t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const hoverHandlers = options.inlineHoverHandlers ? `onmouseenter="handleHoverMove(event, true)" onmouseleave="handleHoverMove(event, false)"` : '';

    return `<span class="token token-${t.type} ${featClass} ${clickClass} ${featClasses} ${diffClass}" data-idx="${t.global_idx}" data-hashes="${hashes}" ${calledAttr} ${titleAttr} ${sideAttr} ${hoverHandlers}>${escapedText}</span>`;
};

window.TOKEN_COLORS = {
    'variable': '#f92672',
    'func_call': '#66d9ef',
    'type': '#ae81ff',
    'keyword': '#f92672',
    'comment': '#75715e',
    'string': '#e6db74',
    'number': '#ae81ff',
    'instruction': '#f92672',
    'register': '#ae81ff',
    'address': '#75715e',
    'constant': '#ae81ff',
    'symbol': '#66d9ef',
    'label': '#a6e22e',
    'operand': '#f8f8f2',
    'default': '#f8f8f2'
};

window.renderRichHtml = function (rows, options = {}) {
    const noBg = options.noBg === true;
    const bg = noBg ? 'transparent' : (options.bg || '#272822');
    const fg = noBg ? 'var(--meta-header-bg)' : (options.fg || '#f8f8f2');
    const font = options.font || "Consolas, 'Courier New', monospace";
    const showGutter = options.showGutter === true; // Default to false

    // Wrapping in a container div
    let style = `color: ${fg}; font-family: ${font}; font-size: 13px; line-height: 1.4; padding: 15px; border-radius: 4px;`;
    if (!noBg) style += ` background-color: ${bg};`;

    let html = `<div style="${style}">`;

    for (const row of rows) {
        // Use a div for each line with white-space: pre to ensure line breaks are preserved in all editors
        let lineHtml = `<div style="white-space: pre; min-height: 1.4em;">`;
        
        // Optional Gutter
        if (showGutter) {
            lineHtml += `<span style="display: inline-block; width: 45px; text-align: right; padding-right: 15px; color: #75715e; border-right: 1px solid #3e3d32; margin-right: 15px; user-select: none;">${row.line_idx}</span>`;
        }

        // Content
        for (const t of row.tokens) {
            let color = window.TOKEN_COLORS[t.type] || window.TOKEN_COLORS['default'];
            
            // If no background, make the default color darker for better contrast on light docs
            if (noBg && (t.type === 'default' || !window.TOKEN_COLORS[t.type])) {
                color = 'var(--meta-header-bg)';
            }
            
            let tStyle = `color: ${color};`;
            
            // Highlight diffs if requested
            if (options.showDiffs) {
                if (t.diff_class === 'diff-match') tStyle += " background-color: rgba(166, 226, 46, 0.15); border-bottom: 1px solid #a6e22e;";
                else if (t.diff_class === 'diff-unique') tStyle += " background-color: rgba(249, 38, 114, 0.15); border-bottom: 1px solid #f92672;";
            }

            const escapedText = t.text.replace(/&/g, '&amp;')
                                     .replace(/</g, '&lt;')
                                     .replace(/>/g, '&gt;')
                                     .replace(/ /g, '&nbsp;')
                                     .replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
            lineHtml += `<span style="${tStyle}">${escapedText}</span>`;
        }
        lineHtml += `</div>`;
        html += lineHtml;
    }

    html += `</div>`;
    return html;
};

window.copyRichText = async function (rows, btn, options = {}) {
    // Default to no background as requested by user
    if (options.noBg === undefined) options.noBg = true;
    
    const html = window.renderRichHtml(rows, options);
    const showGutter = options.showGutter === true;

    const plainText = rows.map(r => {
        const lineNum = showGutter ? r.line_idx.toString().padStart(4) + '  ' : '';
        return lineNum + r.tokens.map(t => t.text).join('');
    }).join('\n');

    let success = false;

    // Check if ClipboardItem and navigator.clipboard.write are available (secure context / modern browsers)
    if (typeof ClipboardItem !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.write === 'function') {
        try {
            const blob = new Blob([html], { type: 'text/html' });
            const textBlob = new Blob([plainText], { type: 'text/plain' });
            const item = new ClipboardItem({
                'text/html': blob,
                'text/plain': textBlob
            });
            await navigator.clipboard.write([item]);
            success = true;
        } catch (err) {
            console.warn("Modern navigator.clipboard.write failed, trying fallback...", err);
        }
    }

    if (!success) {
        // Fallback 1: Try document.execCommand('copy') with custom copy listener for rich text (works in HTTP context)
        try {
            const listener = function(e) {
                e.clipboardData.setData('text/html', html);
                e.clipboardData.setData('text/plain', plainText);
                e.preventDefault();
            };
            document.addEventListener('copy', listener);
            success = document.execCommand('copy');
            document.removeEventListener('copy', listener);
        } catch (err) {
            console.warn("Fallback rich copy failed, trying plain text copy...", err);
        }
    }

    if (!success) {
        // Fallback 2: Try document.execCommand('copy') with a temporary textarea (plain text only)
        try {
            const textArea = document.createElement("textarea");
            textArea.value = plainText;
            textArea.style.position = "fixed";
            textArea.style.top = "0";
            textArea.style.left = "0";
            textArea.style.opacity = "0";
            document.body.appendChild(textArea);
            textArea.focus();
            textArea.select();
            success = document.execCommand('copy');
            document.body.removeChild(textArea);
        } catch (err) {
            console.error("All copy methods failed", err);
        }
    }

    if (success && btn) {
        const originalHtml = btn.innerHTML;
        btn.innerHTML = '<i class="fas fa-check" style="color:#a6e22e"></i>';
        setTimeout(() => { btn.innerHTML = originalHtml; }, 2000);
    }
};

window.setupRichCopyInterceptor = function (container, getRowsFn, options = {}) {
    container.addEventListener('copy', (e) => {
        const selection = window.getSelection();
        if (selection.isCollapsed) return;

        // Verify selection is within our container
        if (!container.contains(selection.anchorNode) && selection.anchorNode !== container) return;

        const rows = getRowsFn();
        if (!rows || rows.length === 0) return;

        let startLine = -1, endLine = -1;
        
        const getLineIdx = (node) => {
            if (!node) return -1;
            const lineEl = node.nodeType === 1 ? node.closest('.code-line') : node.parentElement.closest('.code-line');
            if (lineEl) {
                const ln = lineEl.querySelector('.line-num');
                return ln ? parseInt(ln.innerText) : -1;
            }
            return -1;
        };

        let startIdx = getLineIdx(selection.anchorNode);
        let endIdx = getLineIdx(selection.focusNode);
        
        // If Ctrl+A was used, anchorNode might be the container itself
        if (startIdx === -1 || endIdx === -1) {
            // Check if selection covers the whole container or starts/ends at boundaries
            const range = selection.getRangeAt(0);
            if (range.intersectsNode(container)) {
                // If we can't find specific lines, assume all currently rendered lines in the container
                const renderedLines = Array.from(container.querySelectorAll('.line-num')).map(el => parseInt(el.innerText));
                if (renderedLines.length > 0) {
                    startLine = Math.min(...renderedLines);
                    endLine = Math.max(...renderedLines);
                }
            }
        } else {
            startLine = Math.min(startIdx, endIdx);
            endLine = Math.max(startIdx, endIdx);
        }

        if (startLine !== -1 && endLine !== -1) {
            const selectedRows = rows.filter(r => r.line_idx >= startLine && r.line_idx <= endLine);
            if (selectedRows.length > 0) {
                e.preventDefault();
                const richOptions = { ...options, noBg: true };
                const html = window.renderRichHtml(selectedRows, richOptions);
                const plainText = selectedRows.map(r => r.tokens.map(t => t.text).join('')).join('\n');
                
                e.clipboardData.setData('text/html', html);
                e.clipboardData.setData('text/plain', plainText);
            }
        }
    });
};

