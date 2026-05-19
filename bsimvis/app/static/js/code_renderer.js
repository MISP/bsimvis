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
