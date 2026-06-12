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
        hash: null,
        coll_b: null,
        md5_b: null,
        addr_b: null,
        id1: null,
        id2: null
    };

    const hasCol = parts[0] === 'collection' || parts[0] === 'collections';
    const isPoolPath = parts[0] === 'pools' || parts[0] === 'pool';

    if ((hasCol || isPoolPath) && parts.length >= 2) {
        if (isPoolPath) {
            params.collection = 'pool:' + parts[1];
        } else {
            params.collection = parts[1];
        }

        const p2 = parts[2];
        if (!p2) {
            params.view = 'files';
        } else if (p2 === 'batches') {
            params.view = 'batches';
        } else if (p2 === 'files' || p2 === 'file') {
            if (parts.length === 3) {
                params.view = 'files';
            } else if (parts[3] === 'similarities') {
                params.view = 'binary-similarity';
            } else if (parts[3] === 'clusters') {
                params.view = 'bin-clusters';
            } else if (parts.length >= 4) {
                if (parts[4] === 'vs') {
                    // File diff: /collections/{coll}/files/{md5}/vs/{coll_b}/{md5_b}
                    params.view = 'bin_sim';
                    params.md5 = parts[3];
                    params.coll_b = parts[5];
                    params.md5_b = parts[6];
                } else {
                    params.md5 = parts[3];
                    if (parts.length === 4) {
                        params.view = 'file';
                    } else if (parts[4] === 'functions' || parts[4] === 'function') {
                        if (parts.length === 5) {
                            params.view = 'call_graph';
                        } else if (parts.length >= 6) {
                            params.address = parts[5];
                            if (parts.length === 6) {
                                params.view = 'function';
                            } else if (parts[6] === 'vs') {
                                // Function diff: /collections/{coll}/files/{md5}/functions/{addr}/vs/{coll_b}/{md5_b}/{addr_b}
                                params.view = 'diff';
                                params.coll_b = parts[7];
                                params.md5_b = parts[8];
                                params.addr_b = parts[9];
                                params.id1 = `${params.collection}:func:${params.md5}:${params.address}`;
                                params.id2 = `${params.coll_b}:func:${params.md5_b}:${params.addr_b}`;
                            } else if (parts[6] === 'features') {
                                params.view = 'function_features';
                            }
                        }
                    } else if (parts[4] === 'vs') {
                        params.view = 'bin_sim';
                        params.coll_b = parts[5];
                        params.md5_b = parts[6];
                    }
                }
            }
        } else if (p2 === 'functions' || p2 === 'function') {
            if (parts.length === 3) {
                params.view = 'functions';
            } else if (parts[3] === 'similarities') {
                params.view = 'function-similarity';
            } else if (parts[3] === 'clusters') {
                params.view = 'clusters';
            } else if (parts.length >= 4) {
                if (parts[4] && parts[5] === 'vs') {
                    params.view = 'diff';
                    if (parts.length >= 9) {
                        params.id1 = `${params.collection}:func:${parts[3]}:${parts[4]}`;
                        params.id2 = `${parts[6]}:func:${parts[7]}:${parts[8]}`;
                    }
                } else if (parts[3] && parts[4]) {
                    params.view = 'function';
                    params.md5 = parts[3];
                    params.address = parts[4];
                    if (parts[5] === 'features') params.view = 'function_features';
                } else if (parts[3]) {
                    params.view = 'function';
                    params.md5 = parts[3];
                }
            }
        } else if (p2 === 'features' || p2 === 'feature') {
            if (parts.length === 3) {
                params.view = 'features-global';
            } else if (parts.length >= 4) {
                params.view = 'feature';
                params.hash = parts[3];
            }
        } else if (p2 === 'bin_sim' && parts[3]) {
            params.view = 'bin_sim';
            params.md5 = parts[3];
        } else if (p2 === 'diff') {
            params.view = 'diff';
        } else if (p2) {
            params.view = p2;
        }
    } else if (parts[0] === 'jobs') {
        params.view = 'jobs';
    } else if (parts[0] === 'upload') {
        params.view = 'upload';
    } else if (parts[0] === 'pools') {
        params.view = 'pools';
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
    
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    const hasColInPath = parts[0] === 'collection' || parts[0] === 'collections';
    const collection = (hasColInPath ? restful.collection : null) || params.get('collection') || 'main';


    // Bridge restful params to search params for backward compatibility
    if (restful.md5 && !params.has('md5')) params.set('md5', restful.md5);
    if (restful.md5 && !params.has('file_md5')) params.set('file_md5', restful.md5);
    if (restful.md5 && !params.has('md5_a')) params.set('md5_a', restful.md5);
    if (restful.md5_b && !params.has('md5_b')) params.set('md5_b', restful.md5_b);
    if (restful.address && !params.has('address')) params.set('address', restful.address);
    if (restful.hash && !params.has('hash_val')) params.set('hash_val', restful.hash);
    if (restful.id1 && !params.has('id1')) params.set('id1', restful.id1);
    if (restful.id2 && !params.has('id2')) params.set('id2', restful.id2);

    return { viewKey, collection, params, ...restful };
}
window.getRoutingState = getRoutingState;

function getCollectionFromHash() {
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    const hasColInPath = parts[0] === 'collection' || parts[0] === 'collections';

    const pathParams = parseRestfulPath();
    if (hasColInPath && pathParams.collection) return pathParams.collection;

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

// Global fetch interceptor to transparently migrate pool API collection parameter to pool query/body parameter
(function() {
    const originalFetch = window.fetch;
    window.fetch = function(input, init) {
        let fetchInput = input;
        let fetchInit = init;

        if (typeof fetchInput === 'string') {
            try {
                const url = new URL(fetchInput, window.location.origin);
                if (url.searchParams.has('collection')) {
                    const col = url.searchParams.get('collection');
                    if (col && col.startsWith('pool:')) {
                        const poolId = col.substring(5);
                        url.searchParams.delete('collection');
                        url.searchParams.set('pool', poolId);
                        fetchInput = url.pathname + url.search;
                    }
                }
            } catch (e) {}
        }

        if (fetchInit && fetchInit.body && typeof fetchInit.body === 'string') {
            try {
                const data = JSON.parse(fetchInit.body);
                if (data && data.collection && data.collection.startsWith('pool:')) {
                    data.pool = data.collection.substring(5);
                    delete data.collection;
                    fetchInit = { ...fetchInit, body: JSON.stringify(data) };
                }
            } catch (e) {}
        }

        return originalFetch(fetchInput, fetchInit);
    };
})();
