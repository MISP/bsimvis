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
