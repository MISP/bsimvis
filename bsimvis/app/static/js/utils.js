// Shared utilities for BSimVis

function formatDate(iso) {
    if (!iso || iso === 'N/A') return '---';
    if (typeof iso === 'string' && /^\d+$/.test(iso)) {
        iso = parseInt(iso, 10);
    }
    const d = new Date(iso);
    return d.toLocaleString();
}
window.formatDate = formatDate;

function copyToClipboard(text, btn) {
    navigator.clipboard.writeText(text).then(() => {
        if (btn) {
            const originalIcon = btn.innerHTML;
            btn.innerHTML = '<i class="fa-solid fa-check"></i>';
            btn.classList.add('success');
            setTimeout(() => {
                btn.innerHTML = originalIcon;
                btn.classList.remove('success');
            }, 2000);
        }
    });
}
window.copyToClipboard = copyToClipboard;

function getMd5Color(md5) {
    if (!md5) return 'var(--accent)';
    // Generate a consistent color from MD5
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
 */
function parseRestfulPath() {
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    const params = {
        collection: 'main',
        view: 'dashboard',
        md5: null,
        address: null,
        hash: null
    };

    if (parts[0] === 'collection' && parts.length >= 2) {
        params.collection = parts[1];

        if (parts[2] === 'search' && parts[3]) {
            params.view = parts[3];
        } else if (parts[2] === 'file' && parts[3] && parts[4] === 'vs') {
            // File diff: /collection/{coll}/file/{md5}/vs/{coll}/{md5}
            params.view = 'bin_sim';
            params.md5 = parts[3];
        } else if (parts[2] === 'file' && parts[3]) {
            params.view = 'file';
            params.md5 = parts[3];
        } else if (parts[2] === 'call_graph' && parts[3]) {
            params.view = 'call_graph';
            params.md5 = parts[3];
        } else if (parts[2] === 'function' && parts[3] && parts[4] && parts[5] === 'vs') {
            // Function diff: /collection/{coll}/function/{md5}/{addr}/vs/{coll}/{md5}/{addr}
            params.view = 'diff';
        } else if (parts[2] === 'function' && parts[3]) {
            params.view = 'function';
            params.md5 = parts[3];
            params.address = parts[4];
            if (parts[5] === 'features') params.view = 'function_features';
        } else if (parts[2] === 'bin_sim' && parts[3]) {
            params.view = 'bin_sim';
            params.md5 = parts[3];
        } else if (parts[2] === 'diff') {
            params.view = 'diff';
        } else if (parts[2] === 'feature' && parts[3]) {
            params.view = 'feature';
            params.hash = parts[3];
        } else if (parts[2]) {
            params.view = parts[2];
        }
    } else if (parts[0] === 'jobs') {
        params.view = 'jobs';
    } else if (parts[0] === 'upload') {
        params.view = 'upload';
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
    const viewKey = restful.view || params.get('view') || (window.location.hash ? window.location.hash.substring(1).split('?')[0] : 'dashboard');
    const collection = restful.collection || params.get('collection') || 'main';

    // Bridge restful params to search params for backward compatibility
    if (restful.md5 && !params.has('md5')) params.set('md5', restful.md5);
    if (restful.md5 && !params.has('file_md5')) params.set('file_md5', restful.md5);
    if (restful.address && !params.has('address')) params.set('address', restful.address);
    if (restful.hash && !params.has('hash_val')) params.set('hash_val', restful.hash);

    return { viewKey, collection, params, ...restful };
}
window.getRoutingState = getRoutingState;

function getCollectionFromHash() {
    const pathParams = parseRestfulPath();
    if (pathParams.collection) return pathParams.collection;

    const searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has('collection')) return searchParams.get('collection');

    const [hashPath, queryString] = (window.location.hash || '').split('?');
    const params = new URLSearchParams(queryString);
    if (params.has('collection')) return params.get('collection');
    
    if (window.opener) {
        try {
            if (window.opener.getCollectionFromHash) {
                const pCol = window.opener.getCollectionFromHash();
                if (pCol) return pCol;
            }
        } catch (e) {
            // CORS might block access if same-origin is not met
        }
    }
    
    return 'main';
}
window.getCollectionFromHash = getCollectionFromHash;

/**
 * Formats a function signature into its components (namespace, return type, parameter types, and full string).
 */
function formatSigComponent(namespace, returnType, name, parameters = []) {
    const ns = namespace || '';
    const ret = returnType || 'void';
    const params = (parameters || []).map(p => {
        if (!p) return '...';
        if (typeof p === 'object') return p.type || p.name || '...';
        return p;
    });
    
    const nsPrefix = ns ? `${ns}::` : '';
    const paramsStr = params.join(', ');
    const fullSig = `${ret} ${nsPrefix}${name}(${paramsStr})`;
    
    return { ns, ret, params, fullSig };
}
window.formatSigComponent = formatSigComponent;
