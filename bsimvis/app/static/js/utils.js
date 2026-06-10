// Shared utilities for BSimVis

function formatDate(iso) {
    if (!iso || iso === 'N/A') return '---';
    if (typeof iso === 'string' && /^\d+$/.test(iso)) {
        iso = parseInt(iso, 10);
    }
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleString();
}

function copyToClipboard(text, btn) {
    let success = false;
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        navigator.clipboard.writeText(text).then(() => {
            if (btn) {
                const originalHtml = btn.innerHTML;
                btn.innerHTML = '<span style="color:var(--success)">✓</span>';
                setTimeout(() => { btn.innerHTML = originalHtml; }, 1500);
            }
        }).catch(err => {
            console.warn('navigator.clipboard.writeText failed, using fallback', err);
            fallbackCopyToClipboard(text, btn);
        });
    } else {
        fallbackCopyToClipboard(text, btn);
    }
}

function fallbackCopyToClipboard(text, btn) {
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
        const success = document.execCommand('copy');
        document.body.removeChild(textArea);
        if (success && btn) {
            const originalHtml = btn.innerHTML;
            btn.innerHTML = '<span style="color:var(--success)">✓</span>';
            setTimeout(() => { btn.innerHTML = originalHtml; }, 1500);
        }
    } catch (err) {
        console.error('Fallback copy failed', err);
    }
}

function formatSigComponent(ns, ret, name, params) {
    let truncatedNs = ns;
    if (ns) {
        const parts = ns.split('::');
        if (parts.length > 3) {
            truncatedNs = `${parts[0]}::${parts[1]}...${parts[parts.length - 1]}`;
        } else if (ns.length > 20) {
            truncatedNs = ns.substring(0, 8) + "..." + ns.substring(ns.length - 8);
        }
    }

    let truncatedRet = ret;
    if (ret && ret.length > 12) {
        truncatedRet = ret.substring(0, 5) + "..." + ret.substring(ret.length - 4);
    }

    let truncatedParams = params || [];
    if (params && params.length > 2) {
        truncatedParams = [...params.slice(0, 2), "...", params[params.length - 1]];
    }

    const paramList = (params || []).map(p => (typeof p === 'object' && p !== null) ? (p.name || JSON.stringify(p)) : p);
    const fullSig = `${ret ? ret + ' ' : ''}${ns ? ns + '::' : ''}${name}(${paramList.join(', ')})`;

    return {
        ns: truncatedNs,
        ret: truncatedRet,
        params: truncatedParams.map(p => (typeof p === 'object' && p !== null) ? (p.name || JSON.stringify(p)) : p),
        fullSig: fullSig
    };
}

function showToast(message, type = 'info') {
    const container = document.getElementById('notification-container');
    if (!container) {
        console.warn('Notification container not found, falling back to console:', message);
        return;
    }

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    let icon = 'fa-info-circle';
    if (type === 'success') icon = 'fa-check-circle';
    if (type === 'error') icon = 'fa-exclamation-triangle';
    if (type === 'warning') icon = 'fa-exclamation-circle';

    toast.innerHTML = `
        <i class="fa-solid ${icon}"></i>
        <div class="toast-message">${message}</div>
    `;

    container.appendChild(toast);

    // Auto-remove after 5 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(20px)';
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

function showConfirm(message, onConfirm) {
    const container = document.getElementById('notification-container');
    if (!container) {
        if (confirm(message)) onConfirm();
        return;
    }

    const toast = document.createElement('div');
    toast.className = 'toast toast-confirm';
    toast.style.flexDirection = 'column';
    toast.style.alignItems = 'flex-start';
    toast.style.gap = '10px';

    toast.innerHTML = `
        <div style="display:flex; gap:10px; align-items:center;">
            <i class="fa-solid fa-question-circle" style="color:var(--accent)"></i>
            <div class="toast-message">${message}</div>
        </div>
        <div style="display:flex; gap:10px; width:100%; justify-content: flex-end;">
            <button class="toast-btn toast-btn-cancel">Cancel</button>
            <button class="toast-btn toast-btn-ok">Confirm</button>
        </div>
    `;

    container.appendChild(toast);

    toast.querySelector('.toast-btn-cancel').onclick = () => {
        toast.remove();
    };

    toast.querySelector('.toast-btn-ok').onclick = () => {
        onConfirm();
        toast.remove();
    };
}

/**
 * Generates a deterministic color for an MD5 hash.
 * Ensures the color is bright enough for a dark theme.
 */
function getMd5Color(md5) {
    if (!md5) return "#888888";
    let hash = 0;
    for (let i = 0; i < md5.length; i++) {
        hash = md5.charCodeAt(i) + ((hash << 5) - hash);
    }
    const c = (hash & 0x00FFFFFF).toString(16).toUpperCase();
    const hex = "00000".substring(0, 6 - c.length) + c;

    let r = parseInt(hex.substring(0, 2), 16);
    let g = parseInt(hex.substring(2, 4), 16);
    let b = parseInt(hex.substring(4, 6), 16);

    const brightness = (r * 299 + g * 587 + b * 114) / 1000;
    if (brightness < 60) {
        r = Math.min(255, r + 80);
        g = Math.min(255, g + 80);
        b = Math.min(255, b + 80);
    }

    return `rgb(${r}, ${g}, ${b})`;
}
window.getMd5Color = getMd5Color;

/**
 * Parses the current RESTful URL path to extract context like collection, entity type, and IDs.
 * Supports /collection/{collection}/file/{md5}, /collection/{collection}/search/{view}, etc.
 */
function parseRestfulPath() {
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    const params = {
        collection: null,
        entityType: null,
        id: null,
        md5: null,
        address: null,
        hash: null,
        view: null,
        subview: null
    };

    // Standard pattern: /collection/{collection}/...
    if (parts[0] === 'collection' && parts.length >= 2) {
        params.collection = parts[1];
        
        if (parts[2] === 'search' && parts[3]) {
            params.view = parts[3];
        } else if (parts[2] && !['file', 'function', 'bin_sim', 'diff', 'feature', 'call_graph'].includes(parts[2])) {
            // It's likely a view like 'upload', 'files', 'functions', etc.
            params.view = parts[2];
        } else if (parts[2] === 'file' && parts[3]) {
            params.entityType = 'file';
            params.id = parts[3]; // often md5 for files
            params.md5 = parts[3];
            params.view = 'file';
            if (parts[4] === 'call_graph') params.view = 'call_graph';
        } else if (parts[2] === 'call_graph' && parts[3]) {
            params.entityType = 'call_graph';
            params.id = parts[3];
            params.md5 = parts[3];
            params.view = 'call_graph';
        } else if (parts[2] === 'function' && parts[3] && parts[4]) {
            params.entityType = 'function';
            params.md5 = parts[3];
            params.address = parts[4];
            params.view = 'function';
            // Construct standard func ID if needed: collection:file:md5:address
            params.id = `${params.collection}:file:${params.md5}:${params.address}`;
            if (parts[5] === 'features') params.subview = 'features';
            if (parts[5] === 'similarity') params.subview = 'similarity';
        } else if (parts[2] === 'bin_sim' && parts[3]) {
            params.entityType = 'bin_sim';
            params.id = parts[3];
            params.md5 = parts[3];
            params.view = 'bin_sim';
        } else if (parts[2] === 'diff') {
            params.view = 'diff';
        } else if (parts[2] === 'feature' && parts[3]) {
            params.entityType = 'feature';
            params.hash = parts[3];
            params.view = 'feature';
        }
    } else if (parts[0] === 'jobs') {
        params.view = 'jobs';
    } else if (parts[0] === 'collections' || parts.length === 0) {
        params.view = 'collections';
    }

    return params;
}
window.parseRestfulPath = parseRestfulPath;

/**
 * Gets the current routing state from the URL.
 */
function getRoutingState() {
    const restful = parseRestfulPath();
    const params = new URLSearchParams(window.location.search);
    const viewKey = restful.view || params.get('view') || (window.location.hash ? window.location.hash.substring(1).split('?')[0] : 'collections');
    const collection = restful.collection || params.get('collection') || 'main';
    return { viewKey, collection, params };
}
window.getRoutingState = getRoutingState;

function getCollectionFromHash() {
    // 1. Try RESTful path first
    const pathParams = parseRestfulPath();
    if (pathParams.collection) return pathParams.collection;

    // 2. Try URL Search Params (modern fallback)
    const searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has('collection')) return searchParams.get('collection');

    // 3. Legacy: Try Hash
    const [hashPath, queryString] = (window.location.hash || '').split('?');
    const params = new URLSearchParams(queryString);
    const col = params.get('collection');
    if (col) return col;
    
    // Fallback: check window.parent if in iframe
    if (window.parent && window.parent !== window) {
        try {
            // Check parent pathname
            const pPath = window.parent.location.pathname;
            const pParts = pPath.split('/').filter(Boolean);
            if (pParts[0] === 'collection' && pParts[1]) return pParts[1];

            // Check parent search
            const pSearch = new URLSearchParams(window.parent.location.search);
            if (pSearch.has('collection')) return pSearch.get('collection');

            // Check parent hash
            if (window.parent.location.hash) {
                const [pHashPath, pQueryString] = (window.parent.location.hash || '').split('?');
                const pParams = new URLSearchParams(pQueryString);
                const pCol = pParams.get('collection');
                if (pCol) return pCol;
            }
        } catch (e) {
            // CORS might block access if same-origin is not met
        }
    }
    
    return 'main';
}
window.getCollectionFromHash = getCollectionFromHash;
