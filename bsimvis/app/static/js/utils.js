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

function formatDuration(createdAt, updatedAt, status) {
    if (!createdAt) return '<span class="dim">-</span>';
    let end = updatedAt;
    if (status === 'running' || status === 'pending') {
        end = Date.now();
    }
    const diffMs = end - createdAt;
    if (diffMs < 0) return '0s';
    const totalSecs = Math.floor(diffMs / 1000);
    if (totalSecs < 1) return '< 1s';

    const hours = Math.floor(totalSecs / 3600);
    const minutes = Math.floor((totalSecs % 3600) / 60);
    const seconds = totalSecs % 60;

    let parts = [];
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);

    return parts.join(' ');
}
window.formatDuration = formatDuration;


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
        pool: null,
        collection: null,
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

    if (parts.length === 0) {
        params.view = 'collections';
        return params;
    }

    let pIdx = 0;

    if (parts[pIdx] === 'pool' || parts[pIdx] === 'pools') {
        params.pool = decodeURIComponent(parts[pIdx + 1] || '');
        pIdx += 2;
        if (parts[pIdx] === 'collections' || parts[pIdx] === 'collection') {
            params.collection = decodeURIComponent(parts[pIdx + 1] || '');
            pIdx += 2;
        } else {
            params.collection = null;
        }
    } else if (parts[pIdx] === 'collections' || parts[pIdx] === 'collection') {
        params.collection = decodeURIComponent(parts[pIdx + 1] || '');
        pIdx += 2;
    } else if (parts[pIdx] === 'jobs') {
        params.view = 'jobs';
        return params;
    } else if (parts[pIdx] === 'upload') {
        params.view = 'upload';
        return params;
    }

    const p2 = parts[pIdx];
    const isPoolPath = parts[0] === 'pool' || parts[0] === 'pools';
    const isCollPath = parts[0] === 'collection' || parts[0] === 'collections';

    if (!p2) {
        if (isPoolPath && params.pool) {
            params.view = 'pool-detail';
        } else if (isCollPath && params.collection) {
            params.view = 'collection-detail';
        } else if (params.pool && !params.collection) {
            params.view = 'files';
        } else {
            params.view = 'collections';
        }
    } else if (p2 === 'batches') {
        params.view = 'batches';
    } else if (p2 === 'files' || p2 === 'file') {
        pIdx++;
        if (parts.length === pIdx) {
            params.view = 'files';
        } else if (parts[pIdx] === 'similarities') {
            params.view = 'binary-similarity';
        } else if (parts[pIdx] === 'clusters') {
            params.view = 'bin-clusters';
        } else {
            params.md5 = parts[pIdx];
            pIdx++;
            if (parts.length === pIdx) {
                params.view = 'file';
            } else if (parts[pIdx] === 'vs') {
                params.view = 'bin_sim';
                params.coll_b = decodeURIComponent(parts[pIdx + 1] || '');
                params.md5_b = parts[pIdx + 2];
            } else if (parts[pIdx] === 'functions' || parts[pIdx] === 'function') {
                pIdx++;
                if (parts.length === pIdx) {
                    params.view = 'call_graph';
                } else {
                    params.address = parts[pIdx];
                    pIdx++;
                    if (parts.length === pIdx) {
                        params.view = 'function';
                    } else if (parts[pIdx] === 'features') {
                        params.view = 'function_features';
                    } else if (parts[pIdx] === 'vs') {
                        params.view = 'diff';
                        params.coll_b = decodeURIComponent(parts[pIdx + 1] || '');
                        params.md5_b = parts[pIdx + 2];
                        params.addr_b = parts[pIdx + 3];
                        params.id1 = `${stripPoolPrefix(params.collection || '')}:func:${params.md5}:${params.address}`;
                        params.id2 = `${stripPoolPrefix(params.coll_b || '')}:func:${params.md5_b}:${params.addr_b}`;
                    }
                }
            }
        }
    } else if (p2 === 'functions' || p2 === 'function') {
        pIdx++;
        if (parts.length === pIdx) {
            params.view = 'functions';
        } else if (parts[pIdx] === 'similarities') {
            params.view = 'function-similarity';
        } else if (parts[pIdx] === 'clusters') {
            params.view = 'clusters';
        } else {
            params.md5 = parts[pIdx];
            pIdx++;
            if (parts.length === pIdx) {
                params.view = 'function';
            } else {
                params.address = parts[pIdx];
                pIdx++;
                if (parts.length === pIdx) {
                    params.view = 'function';
                } else if (parts[pIdx] === 'features') {
                    params.view = 'function_features';
                } else if (parts[pIdx] === 'vs') {
                    params.view = 'diff';
                    params.id1 = `${stripPoolPrefix(params.collection || '')}:func:${params.md5}:${params.address}`;
                    params.coll_b = stripPoolPrefix(decodeURIComponent(parts[pIdx + 1] || '')) || '';
                    params.md5_b = parts[pIdx + 2];
                    params.addr_b = parts[pIdx + 3];
                    params.id2 = `${stripPoolPrefix(params.coll_b || '')}:func:${params.md5_b}:${params.addr_b}`;
                }
            }
        }
    } else if (p2 === 'features' || p2 === 'feature') {
        pIdx++;
        if (parts.length === pIdx) {
            params.view = 'features-global';
        } else {
            params.view = 'feature';
            params.hash = parts[pIdx];
        }
    } else if (p2 === 'bin_sim') {
        params.view = 'bin_sim';
        params.md5 = parts[pIdx + 1];
    } else if (p2 === 'diff') {
        params.view = 'diff';
    } else {
        params.view = p2;
    }

    if (parts[0] === 'pools' && parts.length === 1) {
        params.view = 'pools';
    }

    return params;
}
window.parseRestfulPath = parseRestfulPath;

window.getCollectionFromId = function (id) {
    if (!id || typeof id !== 'string') return '';
    if (id.includes(':func:')) return stripPoolPrefix(id.split(':func:')[0]) || '';
    if (id.includes(':function:')) return stripPoolPrefix(id.split(':function:')[0]) || '';
    if (id.includes(':file:')) return stripPoolPrefix(id.split(':file:')[0]) || '';

    const parts = id.split(':');
    if (parts.length >= 4 && (parts[parts.length - 1].startsWith('00') || parts[parts.length - 1].length < 10)) {
        return stripPoolPrefix(parts.slice(0, parts.length - 2).join(':')) || '';
    }
    if (parts.length >= 2) {
        return stripPoolPrefix(parts.slice(0, parts.length - 1).join(':')) || '';
    }
    return '';
};

window.parseFuncId = function (id) {
    if (!id || typeof id !== 'string') return { collection: '', md5: '', address: '' };
    const delimiter = id.includes(':func:') ? ':func:' : (id.includes(':function:') ? ':function:' : null);
    if (delimiter) {
        const parts = id.split(delimiter);
        const col = stripPoolPrefix(parts[0]) || '';
        const rest = parts[1].split(':');
        return {
            collection: col,
            md5: rest[0],
            address: rest[1] || ''
        };
    }
    const parts = id.split(':');
    if (parts.length >= 4) {
        return {
            collection: stripPoolPrefix(parts[0]) || '',
            md5: parts[2],
            address: parts[3] || ''
        };
    }
    return { collection: '', md5: parts[2] || '', address: parts[3] || parts[parts.length - 1] || '' };
};

window.parseFileId = function (id) {
    if (!id || typeof id !== 'string') return { collection: '', md5: '' };
    if (id.includes(':file:')) {
        const parts = id.split(':file:');
        return {
            collection: parts[0] || '',
            md5: parts[1]
        };
    }
    const parts = id.split(':');
    if (parts.length >= 2) {
        const md5 = parts[parts.length - 1];
        const collection = parts.slice(0, parts.length - 1).join(':') || '';
        return { collection, md5 };
    }
    return { collection: '', md5: id };
};

window.getApiParams = function (collection) {
    if (!collection) {
        throw new Error("getApiParams: collection is required and cannot be null/empty.");
    }
    let params = `collection=${encodeURIComponent(collection)}`;
    const routingState = window.getRoutingState ? window.getRoutingState() : {};
    if (routingState.pool) {
        params += `&pool=${encodeURIComponent(routingState.pool)}`;
    }
    return params;
};


/**
 * Gets the current routing state from the URL.
 */
function getRoutingState() {
    const restful = parseRestfulPath();
    const params = new URLSearchParams(window.location.search);
    const viewKey = restful.view || params.get('view') || (window.location.hash ? window.location.hash.substring(1).split('?')[0] : 'dashboard');

    let collection = restful.collection || params.get('collection') || null;
    if (collection === 'null' || collection === 'undefined') {
        collection = null;
    }
    const pool = restful.pool || params.get('pool') || null;

    if (collection && viewKey !== 'jobs') {
        localStorage.setItem('lastCollectionContext', collection);
    }
    if (pool && viewKey !== 'jobs') {
        localStorage.setItem('lastPoolContext', pool);
    }

    // Bridge restful params to search params for backward compatibility
    if (restful.md5 && !params.has('md5')) params.set('md5', restful.md5);
    if (restful.md5 && !params.has('file_md5')) params.set('file_md5', restful.md5);
    if (restful.md5 && !params.has('md5_a')) params.set('md5_a', restful.md5);
    if (restful.md5_b && !params.has('md5_b')) params.set('md5_b', restful.md5_b);
    if (restful.address && !params.has('address')) params.set('address', restful.address);
    if (restful.hash && !params.has('hash_val')) params.set('hash_val', restful.hash);
    if (restful.id1 && !params.has('id1')) params.set('id1', restful.id1);
    if (restful.id2 && !params.has('id2')) params.set('id2', restful.id2);
    if (restful.pool && !params.has('pool')) params.set('pool', restful.pool);

    return { viewKey, collection, pool, params, ...restful };
}
window.getRoutingState = getRoutingState;

function getCollectionFromHash() {
    const pathParams = parseRestfulPath();
    if (pathParams.collection) return pathParams.collection;
    if (pathParams.pool) return 'pool:' + pathParams.pool;

    const searchParams = new URLSearchParams(window.location.search);
    if (searchParams.has('collection')) return searchParams.get('collection');
    if (searchParams.has('pool')) return 'pool:' + searchParams.get('pool');

    const [hashPath, queryString] = (window.location.hash || '').split('?');
    const params = new URLSearchParams(queryString);
    if (params.has('collection')) return params.get('collection');
    if (params.has('pool')) return 'pool:' + params.get('pool');

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

    throw new Error("Navigation error: collection context could not be resolved.");
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

// Global fetch interceptor removed.

/**
 * Parses an entity ID (function_id or file_id) robustly, supporting collections/pools containing colons.
 */
function parseEntityId(id) {
    if (!id || typeof id !== 'string') {
        return { collection: '', type: null, md5: '', address: '' };
    }
    const funcIdx = id.indexOf(':func:');
    const fileIdx = id.indexOf(':file:');

    if (funcIdx !== -1) {
        const collection = id.substring(0, funcIdx);
        const rest = id.substring(funcIdx + 6);
        const parts = rest.split(':');
        return {
            collection: collection || '',
            type: 'func',
            md5: parts[0] || '',
            address: parts[1] || ''
        };
    } else if (fileIdx !== -1) {
        const collection = id.substring(0, fileIdx);
        const rest = id.substring(fileIdx + 6);
        const parts = rest.split(':');
        return {
            collection: collection || '',
            type: 'file',
            md5: parts[0] || '',
            address: ''
        };
    } else {
        const parts = id.split(':');
        if (parts.length >= 4) {
            const col = parts.slice(0, parts.length - 2).join(':');
            return {
                collection: col || '',
                type: 'func',
                md5: parts[parts.length - 2],
                address: parts[parts.length - 1]
            };
        } else {
            return {
                collection: parts[0] || '',
                type: null,
                md5: parts[2] || parts[1] || '',
                address: parts[3] || parts[2] || ''
            };
        }
    }
}
window.parseEntityId = parseEntityId;

function assertValidCollection(ctx) {
    if (!ctx.collection || ctx.collection === 'null' || ctx.collection === 'undefined' || ctx.collection === 'main') {
        let msg = `Navigation error: Invalid collection context: ${ctx.collection || 'null'}`;
        if (ctx.pool) msg += ` (within pool "${ctx.pool}", view "${ctx.viewKey || ''}")`;
        showNullContextWarning(ctx.collection, ctx.pool, ctx.viewKey);
        throw new Error(msg);
    }
    return ctx.collection;
}
window.assertValidCollection = assertValidCollection;

function showNullContextWarning(collection, pool, viewKey) {
    let banner = document.getElementById('null-context-warning');
    if (banner) {
        banner.remove();
    }
    banner = document.createElement('div');
    banner.id = 'null-context-warning';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:9999;background:#f92672;color:var(--text);text-align:center;padding:8px;font-size:0.85rem;font-weight:bold;box-shadow:0 2px 8px color-mix(in srgb, var(--token-instruction) 40%, transparent);cursor:pointer;';
    banner.textContent = `⚠️ Navigation error: Invalid collection or pool context. Please navigate directly to a valid collection or pool.`;
    banner.onclick = () => banner.remove();
    document.body.prepend(banner);
    console.error(banner.textContent);
    setTimeout(() => banner.remove(), 10000); // dismiss after 10s
}
window.showNullContextWarning = showNullContextWarning;

function showToast(message, type = 'info') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.position = 'fixed';
        container.style.bottom = '20px';
        container.style.right = '20px';
        container.style.zIndex = '9999';
        container.style.display = 'flex';
        container.style.flexDirection = 'column';
        container.style.gap = '10px';
        document.body.appendChild(container);
    }

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    let iconClass = 'fa-info-circle';
    if (type === 'success') iconClass = 'fa-check-circle';
    else if (type === 'error') iconClass = 'fa-times-circle';
    else if (type === 'warning') iconClass = 'fa-exclamation-triangle';

    toast.innerHTML = `
        <div class="toast-message">
            <i class="fa-solid ${iconClass}"></i>
            <span>${escapeHtml(message)}</span>
        </div>
    `;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(20px)';
        toast.style.transition = 'all 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}
window.showToast = showToast;

// Apply theme across all pages that include utils.js (like iframes)
if (localStorage.getItem('lightTheme') === 'true') {
    document.documentElement.classList.add('light-theme');
} else {
    document.documentElement.classList.remove('light-theme');
}

// Inject floating UI Settings button for standalone views
document.addEventListener('DOMContentLoaded', () => {
    // Only inject if the page doesn't have the main dashboard settings button
    if (!document.getElementById('header-settings-btn') && !document.getElementById('floating-settings-btn')) {
        const btn = document.createElement('button');
        btn.id = 'floating-settings-btn';
        btn.innerHTML = '<i class="fa-solid fa-sliders"></i>';
        btn.title = "UI Settings";
        btn.style.cssText = "position:fixed; bottom:20px; left:20px; z-index:9999; background:var(--card-bg); color:var(--accent); border:1px solid var(--border); border-radius:50%; width:45px; height:45px; cursor:pointer; box-shadow:0 4px 12px rgba(0,0,0,0.3); display:flex; align-items:center; justify-content:center; font-size:1.2rem; transition: all 0.2s;";
        
        btn.onmouseover = () => btn.style.transform = "scale(1.1)";
        btn.onmouseout = () => btn.style.transform = "scale(1)";
        
        btn.onclick = () => {
            let panel = document.getElementById('floating-ui-settings');
            if (!panel) {
                panel = document.createElement('div');
                panel.id = 'floating-ui-settings';
                panel.style.cssText = "position:fixed; bottom:75px; left:20px; z-index:9999; background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:20px; width:280px; box-shadow:0 4px 15px rgba(0,0,0,0.5); color:var(--text); display:block; font-family: 'Inter', sans-serif;";
                
                const isLight = document.documentElement.classList.contains('light-theme');
                const useFloating = localStorage.getItem('useFloatingWindows') === 'true';
                const includeHeaders = localStorage.getItem('includeHeaders') === 'true';
                
                panel.innerHTML = `
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
                        <h3 style="margin:0; font-size:1rem; color:var(--accent);"><i class="fa-solid fa-sliders"></i> UI Settings</h3>
                        <button onclick="document.getElementById('floating-ui-settings').style.display='none'" style="background:none; border:none; color:var(--dim); cursor:pointer;"><i class="fa-solid fa-times"></i></button>
                    </div>
                    <label style="display:flex; align-items:center; gap:10px; margin-bottom: 20px; cursor:pointer; padding: 4px 0;">
                        <input type="checkbox" id="floating-param-light-theme" ${isLight ? 'checked' : ''} onchange="
                            if(this.checked) {
                                document.documentElement.classList.add('light-theme');
                                localStorage.setItem('lightTheme', 'true');
                            } else {
                                document.documentElement.classList.remove('light-theme');
                                localStorage.setItem('lightTheme', 'false');
                            }
                        " style="cursor:pointer;">
                        <span style="font-size:0.9rem; flex:1; display:flex; align-items:center; gap:10px;">
                            <i class="fa-solid fa-sun" style="color:var(--accent); width:16px; text-align:center;"></i> Light Theme
                        </span>
                    </label>
                `;
                document.body.appendChild(panel);
            } else {
                panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
            }
        };
        document.body.appendChild(btn);
    }
});
