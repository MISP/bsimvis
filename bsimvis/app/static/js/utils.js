// Shared utilities for BSimVis

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function escapeAttr(value) {
    return escapeHtml(value);
}

function jsString(value) {
    return JSON.stringify(String(value ?? ''))
        .replace(/</g, '\\u003C')
        .replace(/>/g, '\\u003E')
        .replace(/&/g, '\\u0026')
        .replace(/\u2028/g, '\\u2028')
        .replace(/\u2029/g, '\\u2029');
}

function safeCssClassPart(value) {
    return String(value ?? '').replace(/[^a-zA-Z0-9_-]/g, '_');
}

function safeCssColor(value, fallback = '#66d9ef') {
    const color = String(value ?? '').trim();
    if (/^#[0-9a-fA-F]{3,8}$/.test(color)) return color;
    if (/^rgba?\(\s*[0-9.]+%?\s*,\s*[0-9.]+%?\s*,\s*[0-9.]+%?(\s*,\s*(0|1|0?\.[0-9]+))?\s*\)$/.test(color)) return color;
    if (/^hsla?\(\s*[0-9.]+(?:deg)?\s*,\s*[0-9.]+%\s*,\s*[0-9.]+%(\s*,\s*(0|1|0?\.[0-9]+))?\s*\)$/.test(color)) return color;
    return fallback;
}

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
    navigator.clipboard.writeText(text).then(() => {
        const originalHtml = btn.innerHTML;
        btn.innerHTML = '<span style="color:var(--success)">✓</span>';
        setTimeout(() => { btn.innerHTML = originalHtml; }, 1500);
    }).catch(err => console.error('Failed to copy', err));
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
