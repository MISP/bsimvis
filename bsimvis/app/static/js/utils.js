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

    const fullSig = `${ret ? ret + ' ' : ''}${ns ? ns + '::' : ''}${name}(${(params || []).join(', ')})`;

    return {
        ns: truncatedNs,
        ret: truncatedRet,
        params: truncatedParams,
        fullSig: fullSig
    };
}
