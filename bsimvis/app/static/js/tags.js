if (typeof window.getCurrentCollection !== 'function') {
    window.getCurrentCollection = function() {
        if (typeof getCollectionFromHash === 'function') {
            return getCollectionFromHash();
        }
        return '';
    };
}

let tagMetadata = {};
window.tagMetadata = tagMetadata;

document.addEventListener('mousedown', (e) => {
    if (e.target.closest('.tag-overflow-chip')) return;
    document.querySelectorAll('.tag-overflow-dropdown.open').forEach(d => d.classList.remove('open'));
});

if (typeof escapeHtml === 'undefined') {
    window.escapeHtml = function (value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    };
}
if (typeof escapeAttr === 'undefined') window.escapeAttr = window.escapeHtml;
if (typeof jsString === 'undefined') {
    window.jsString = function (value) {
        return JSON.stringify(String(value ?? ''))
            .replace(/</g, '\\u003C')
            .replace(/>/g, '\\u003E')
            .replace(/&/g, '\\u0026')
            .replace(/\u2028/g, '\\u2028')
            .replace(/\u2029/g, '\\u2029');
    };
}
if (typeof safeCssColor === 'undefined') {
    window.safeCssColor = function (value, fallback = '#66d9ef') {
        const color = String(value ?? '').trim();
        if (/^#[0-9a-fA-F]{3,8}$/.test(color)) return color;
        if (/^rgba?\(\s*[0-9.]+%?\s*,\s*[0-9.]+%?\s*,\s*[0-9.]+%?(\s*,\s*(0|1|0?\.[0-9]+))?\s*\)$/.test(color)) return color;
        if (/^hsla?\(\s*[0-9.]+(?:deg)?\s*,\s*[0-9.]+%\s*,\s*[0-9.]+%(\s*,\s*(0|1|0?\.[0-9]+))?\s*\)$/.test(color)) return color;
        return fallback;
    };
}

// Tag colors are picked for a dark background; on the light theme the pale ones
// (yellow, light green) vanish. Darken them, keeping the hue so tags stay
// recognizable. ponytail: pure function, callers re-render on theme toggle.
window.tagInk = function (color) {
    if (!document.documentElement.classList.contains('light-theme')) return color;
    let hex = String(color || '').trim();
    if (!/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(hex)) return color;
    if (hex.length === 4) hex = '#' + [...hex.slice(1)].map(c => c + c).join('');
    const rgb = [1, 3, 5].map(i => parseInt(hex.slice(i, i + 2), 16));
    // Relative luminance; anything brighter than this washes out on white.
    const lum = (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255;
    if (lum <= 0.40) return hex;
    const k = 0.40 / lum; // uniform scale keeps the hue, just dims it
    return '#' + rgb.map(c => Math.round(c * k).toString(16).padStart(2, '0')).join('');
};

// A tag colour at partial opacity, for the card borders and fills that used to
// be written as `${color}44`. A derived colour is an `hsl()` with CSS variables
// in it, not a hex string, so appending alpha digits to it produces nothing at
// all -- `color-mix` takes any colour, including those.
window.tagAlpha = function (color, pct) {
    return `color-mix(in srgb, ${color} ${pct}%, transparent)`;
};

let isFetchingTagMetadata = false;
let tagFetchPromise = null;

async function fetchTagMetadata(collection) {
    if (!collection) return;
    if (isFetchingTagMetadata && tagFetchPromise) return tagFetchPromise;

    isFetchingTagMetadata = true;
    tagFetchPromise = (async () => {
        try {
            const apiParams = (window.getApiParams || window.parent.getApiParams)(collection);
            const res = await fetch(`/api/tags/metadata?${apiParams}`);
            if (res.ok) {
                tagMetadata = await res.json();
                window.tagMetadata = tagMetadata;
            }
            // Ensure bookmark and ignore have a default look if not set on server
            if (!tagMetadata['bookmark']) {
                tagMetadata['bookmark'] = { color: '#66d9ef', priority: 1000, count: 0 };
            }
            if (!tagMetadata['ignore']) {
                tagMetadata['ignore'] = { color: '#f92672', priority: 900, count: 0 };
            }
            window.tagMetadata = tagMetadata;
        } catch (err) {
            console.error("Failed to fetch tag metadata", err);
        } finally {
            isFetchingTagMetadata = false;
            tagFetchPromise = null;
        }
    })();
    return tagFetchPromise;
}

function getRawTagColor(analysisTags, userTags = []) {
    const allTags = [...(analysisTags || []), ...(userTags || [])].filter(t => t && t.trim());
    if (allTags.length === 0) return null;

    let bestColor = null;
    let maxPrio = -1;
    allTags.forEach(t => {
        let meta = tagMetadata[t];
        if (t === 'bookmark') meta = { color: '#66d9ef', priority: 1000 };
        if (t === 'ignore') meta = { color: '#f92672', priority: 900 };
        const color = (meta && meta.color)
            ? safeCssColor(meta.color)
            : TagColor.css(t);
        const priority = (meta && meta.priority !== undefined) ? meta.priority : 0;

        if (priority >= maxPrio) {
            maxPrio = priority;
            bestColor = color;
        }
    });
    return bestColor;
}

function getRowTagColor(analysisTags, userTags = []) {
    const colorEnabled = typeof UIParams !== 'undefined' ? UIParams.colorByTag : (localStorage.getItem('sim-color-by-tag') === 'true');
    if (!colorEnabled) return "";

    const bestColor = getRawTagColor(analysisTags, userTags);
    if (bestColor) {
        return `linear-gradient(90deg, ${bestColor}44 0%, transparent 100%)`;
    }
    return "";
}

function refreshAllRowColors() {
    const rows = document.querySelectorAll('tr.sim-row');
    const view = typeof parseRestfulPath === 'function' ? parseRestfulPath().view : '';
    const hash = window.location.hash || '';
    const isColorEnabled = typeof UIParams !== 'undefined' ? UIParams.colorByTag : (localStorage.getItem('sim-color-by-tag') === 'true');

    rows.forEach(tr => {
        if (!isColorEnabled) {
            tr.style.background = "";
            return;
        }

        // Only collect tags from the PRIMARY editor of the row
        let selector = '.entity-tags-editor[data-etype="function"]';
        if (view === 'bin_sim' || view === 'function-similarity' || hash.includes('function-similarity')) {
            selector = '.sim-tags-editor[data-etype="similarity"]';
        } else if (view === 'files' || hash.includes('files')) {
            selector = '.entity-tags-editor[data-etype="file"]';
        }

        const editor = tr.querySelector(selector);
        if (!editor) {
            tr.style.background = "";
            return;
        }

        const analystCards = editor.querySelectorAll('.sim-tag-card');
        const analysisCards = editor.querySelectorAll('.analysis-tag-badge');

        const userTags = Array.from(analystCards).map(c => c.textContent.replace('×', '').trim());
        const analysisTags = Array.from(analysisCards).map(c => c.textContent.trim());

        const bookmarkBtn = editor.querySelector('.bookmark-btn');
        if (bookmarkBtn && bookmarkBtn.classList.contains('active')) {
            if (!userTags.includes('bookmark')) userTags.push('bookmark');
        }
        const ignoreBtn = editor.querySelector('.ignore-btn');
        if (ignoreBtn && ignoreBtn.classList.contains('active')) {
            if (!userTags.includes('ignore')) userTags.push('ignore');
        }

        const rowColor = getRowTagColor(analysisTags, userTags);
        tr.style.background = rowColor;
    });
}

function updateTagUIElements(tag, color) {
    // Update .sim-tag-card (User Tags)
    document.querySelectorAll('.sim-tag-card').forEach(card => {
        if (card.textContent.replace('×', '').trim() === tag) {
            card.style.borderColor = tagAlpha(color, 40);
            card.style.color = color;
            card.style.background = tagAlpha(color, 7);
        }
    });

    // Update .analysis-tag-badge (Analysis Tags)
    document.querySelectorAll('.analysis-tag-badge').forEach(badge => {
        if (badge.textContent.trim() === tag) {
            badge.style.borderColor = tagAlpha(color, 40);
            badge.style.color = color;
            badge.style.background = tagAlpha(color, 7);
        }
    });

    // Update Cluster Cards if they match the tag name (unlikely but possible)
    document.querySelectorAll('.cluster-card').forEach(card => {
        const nameSpan = card.querySelector('span');
        if (nameSpan && nameSpan.textContent.trim() === tag) {
            card.style.borderColor = tagAlpha(color, 27);
            card.style.color = color;
            card.style.background = tagAlpha(color, 7);
        }
    });
}

// Tag Tooltip
window.showTooltip = (e, tag, coll) => {
    if (window.setTrigger) window.setTrigger(e);
    let el = document.getElementById('tag-tooltip');
    if (!el) {
        el = document.createElement('div');
        el.id = 'tag-tooltip';
        el.style.cssText = "position:fixed; z-index:20005; background:var(--meta-bg); border:1px solid var(--border); padding:12px; border-radius:8px; display:none; pointer-events:none; font-size:0.8rem; color:var(--text); backdrop-filter:blur(10px); min-width:180px;";
        document.body.appendChild(el);
    }

    el.innerHTML = `<div style="color:var(--dim)">Loading stats for <b>${escapeHtml(tag)}</b>...</div>`;
    el.style.display = 'block';

    const apiParams = (window.getApiParams || window.parent.getApiParams)(coll);
    fetch(`/api/tags/stats?${apiParams}&tag=${encodeURIComponent(tag)}`)
        .then(res => res.json())
        .then(stats => {
            const meta = getTagMetadata(tag);
            el.innerHTML = `
                <div style="font-weight:bold; margin-bottom:8px; border-bottom:1px solid var(--border); padding-bottom:5px; display:flex; justify-content:space-between; align-items:center; gap:8px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="width:10px; height:10px; border-radius:50%; background:${meta.color}"></span>
                        ${escapeHtml(tag)}
                    </div>
                    <div style="font-size:0.65rem; background:rgba(255,171,46,0.1); color:var(--accent); padding:2px 6px; border-radius:4px; border:1px solid rgba(255,171,46,0.2);">
                        Prio: ${meta.priority || 0}
                    </div>
                </div>
                <div style="display:grid; grid-template-columns: 1fr auto; gap:5px 15px;">
                    <span style="color:var(--dim)">Functions:</span> <b style="color:var(--accent)">${stats.function}</b>
                    <span style="color:var(--dim)">Files:</span> <b style="color:var(--accent)">${stats.file}</b>
                    <span style="color:var(--dim)">Similarities:</span> <b style="color:var(--accent)">${stats.similarity}</b>
                </div>
                <div style="margin-top:8px; font-size:0.65rem; color:var(--dim); font-style:italic;">Right-click tag to customize</div>
            `;
        });

    const moveTooltip = (ev) => {
        let x = ev.clientX + 15;
        let y = ev.clientY + 15;
        if (x + 200 > window.innerWidth) x -= 220;
        if (y + 120 > window.innerHeight) y -= 140;
        el.style.left = x + 'px';
        el.style.top = y + 'px';
    };
    moveTooltip(e);
};

window.getTagMetadata = (tag) => {
    if (tag === 'bookmark') return { color: window.tagInk('#66d9ef'), priority: 1000 };
    if (tag === 'ignore') return { color: window.tagInk('#f92672'), priority: 900 };
    const m = tagMetadata[tag] || (window.parent && window.parent.tagMetadata && window.parent.tagMetadata[tag]);
    if (m) return { ...m, color: window.tagInk(safeCssColor(m.color)) };
    // No stored colour: derive one from the tag id. A fixed palette indexed by a
    // hash of the whole name gave `category:network` and `category:crypto` two
    // unrelated colours and `network` a third; `TagColor` keeps a namespace's
    // tags in one arc and a leaf a shade of its group, and matches what the
    // graphs paint for the same tag.
    return { color: TagColor.css(tag), priority: 0 };
};

window.hideTooltip = () => {
    if (window.hideAllTooltips) window.hideAllTooltips();
    else {
        const el = document.getElementById('tag-tooltip');
        if (el) el.style.display = 'none';
    }
};

window.handleTagContextMenu = (e, tag) => {
    e.preventDefault();
    const coll = typeof getCurrentCollection === 'function' ? getCurrentCollection() : '';
    const currentMeta = { ...(tagMetadata[tag] || { color: "#66d9ef", priority: 0 }) };
    currentMeta.color = safeCssColor(currentMeta.color);

    let menu = document.getElementById('tag-custom-context-menu');
    if (!menu) {
        menu = document.createElement('div');
        menu.id = 'tag-custom-context-menu';
        menu.style.cssText = "position:fixed; z-index:20010; background:var(--card-bg); border:1px solid var(--border); border-radius:8px; display:none; overflow:hidden; width:220px; font-family:var(--font-main, inherit);";
        document.body.appendChild(menu);
    }

    menu.innerHTML = `
        <div style="padding:12px 15px; font-weight:bold; font-size:0.8rem; color:var(--accent); border-bottom:1px solid var(--border); background: var(--hover); display:flex; justify-content:space-between; align-items:center;">
            <span>Tag: ${escapeHtml(tag)}</span>
            <button onclick="document.getElementById('tag-custom-context-menu').style.display='none'" style="background:none; border:none; color:var(--dim); cursor:pointer;"><i class="fa-solid fa-times"></i></button>
        </div>
        <div style="padding:15px; display:flex; flex-direction:column; gap:15px;">
            <div id="tag-picker-container" style="display:flex; justify-content:center;"></div>

            <div style="display:flex; flex-direction:column; gap:8px;">
                <label style="font-size:0.75rem; color:var(--dim); display:flex; justify-content:space-between;">
                    Priority <span id="tag-prio-val" style="color:var(--accent)">${currentMeta.priority}</span>
                </label>
                <input type="range" id="tag-prio-slider" min="0" max="1000" step="10" value="${currentMeta.priority}" style="width:100%; cursor:pointer;">
            </div>

            <button id="tag-save-btn" style="width:100%; padding:10px; background:var(--accent); color:var(--window-tray); border:none; border-radius:4px; font-weight:bold; cursor:pointer; transition:opacity 0.2s;">
                Apply Changes
            </button>
        </div>
    `;

    // Initialize Color Wheel
    const colorPicker = new iro.ColorPicker("#tag-picker-container", {
        width: 160,
        color: currentMeta.color,
        layout: [
            { component: iro.ui.Wheel },
            { component: iro.ui.Slider, options: { sliderType: 'value' } }
        ]
    });

    const prioSlider = menu.querySelector('#tag-prio-slider');
    const prioVal = menu.querySelector('#tag-prio-val');
    prioSlider.oninput = (ev) => {
        prioVal.innerText = ev.target.value;
    };

    const saveBtn = menu.querySelector('#tag-save-btn');
    saveBtn.onclick = async () => {
        saveBtn.disabled = true;
        saveBtn.innerText = "Saving...";
        const newColor = colorPicker.color.hexString;
        const newPrio = parseInt(prioSlider.value);

        try {
            await Promise.all([
                fetch('/api/tags/color', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({collection: coll, tag: tag, color: newColor})
                }),
                fetch('/api/tags/priority', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({collection: coll, tag: tag, priority: newPrio})
                })
            ]);

            // Force immediate metadata sync
            const updatedMeta = { color: newColor, priority: newPrio };
            tagMetadata[tag] = updatedMeta;
            if (window.parent && window.parent.tagMetadata) {
                window.parent.tagMetadata[tag] = updatedMeta;
            }
            if (window.top && window.top.tagMetadata) {
                window.top.tagMetadata[tag] = updatedMeta;
            }

            // Update all tag badges and cards in the current document
            updateTagUIElements(tag, window.tagInk(newColor));

            // Refresh row colors
            if (typeof refreshAllRowColors === 'function') {
                refreshAllRowColors();
            }

            // If we have a graph instance, refresh its colors too
            if (window.graphInstance && typeof window.graphInstance.refreshColors === 'function') {
                window.graphInstance.refreshColors();
            }

            menu.style.display = 'none';

        } catch (err) {
            console.error("Failed to save tag metadata", err);
            saveBtn.disabled = false;
            saveBtn.innerText = "Error - Retry";
        }
    };

    menu.style.display = 'block';

    // Position handling
    let x = e.clientX;
    let y = e.clientY;
    if (x + 240 > window.innerWidth) x -= 240;
    if (y + 350 > window.innerHeight) y -= 350;

    menu.style.left = x + 'px';
    menu.style.top = y + 'px';

    const closeMenu = (me) => {
        if (!menu.contains(me.target)) {
            menu.style.display = 'none';
            document.removeEventListener('mousedown', closeMenu);
        }
    };
    setTimeout(() => document.addEventListener('mousedown', closeMenu), 10);
};

window.renderTagEditor = (etype, eid, tagsList, userTagsList, options = {}) => {
    const isBookmarked = userTagsList.includes('bookmark');
    const isIgnored = userTagsList.includes('ignore');
    const editorClass = etype === 'similarity' ? 'sim-tags-editor' : 'entity-tags-editor';
    const bookmarkOnClick = etype === 'similarity'
        ? `toggleBookmark(event, ${jsString(eid)})`
        : `toggleEntityBookmark(event, ${jsString(etype)}, ${jsString(eid)})`;

    const ignoreOnClick = etype === 'similarity'
        ? `toggleIgnore(event, ${jsString(eid)})`
        : `toggleEntityIgnore(event, ${jsString(etype)}, ${jsString(eid)})`;

    const addOnClick = `startAddTag(event, ${jsString(etype)}, ${jsString(eid)})`;

    // The entity id rides on the badge so the provenance popup can ask "which
    // rule fired *here*" (match_provenance) before falling back to "which rules
    // can emit this tag at all" (tag_rules).
    // An analysis tag badge carries its tag's colour like every other card. It
    // used to be the one place that did not: a class with a fixed palette, so
    // `category:network` looked the same as `severity:high` in a table and
    // different from the same tag in the graphs.
    const analysisBadge = t => {
        const color = window.tagInk(window.getTagMetadata(t).color);
        return `<span class="analysis-tag-badge" style="cursor:pointer; border-color:${tagAlpha(color, 40)}; color:${color}; background:${tagAlpha(color, 7)};" data-eid="${escapeAttr(eid)}" title="Analysis Tag: ${escapeAttr(t)} (click for source)">${escapeHtml(t)}</span>`;
    };

    const userBadge = t => {
        if (t === 'bookmark' || t === 'ignore') return '';
        const color = window.tagInk(window.getTagMetadata(t).color);
        const removeClick = `removeTag(event, ${jsString(etype)}, ${jsString(eid)}, ${jsString(t)})`;
        const coll = typeof getCurrentCollection === 'function' ? getCurrentCollection() : '';

        return `
        <span class="sim-tag-card"
              style="border-color:${tagAlpha(color, 27)}; color:${color}; background:${tagAlpha(color, 7)}; cursor:pointer;"
              onmouseenter="showTooltip(event, ${escapeAttr(jsString(t))}, ${escapeAttr(jsString(coll))})"
              onmouseleave="hideTooltip()"
              oncontextmenu="handleTagContextMenu(event, ${escapeAttr(jsString(t))})">
            ${escapeHtml(t)}
            <span class="remove-tag-btn" onclick="${escapeAttr(removeClick)}" style="background:${escapeAttr(color)}22">×</span>
        </span>`;
    };

    // Table cells cap visible tags so a file with 30 tags doesn't blow out row
    // height; overflow tags sit behind a "+N" chip instead of being dropped.
    const maxTags = options.maxTags;
    const nonSpecialUserTags = userTagsList.filter(t => t !== 'bookmark' && t !== 'ignore');
    const byPriorityDesc = (a, b) => (window.getTagMetadata(b.t).priority || 0) - (window.getTagMetadata(a.t).priority || 0);
    let visibleHtml, overflowHtml = '';
    if (maxTags && (tagsList.length + nonSpecialUserTags.length) > maxTags) {
        const allTags = [
            ...tagsList.map(t => ({ t, badge: analysisBadge })),
            ...nonSpecialUserTags.map(t => ({ t, badge: userBadge })),
        ].sort(byPriorityDesc);
        const shown = allTags.slice(0, maxTags);
        const hidden = allTags.slice(maxTags);
        visibleHtml = shown.map(x => x.badge(x.t)).join('');
        const hiddenHtml = hidden.map(x => x.badge(x.t)).join('');
        overflowHtml = `
        <span class="tag-overflow-wrap" style="position:relative; display:inline-flex;">
            <span class="tag-overflow-chip" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open');">+${hidden.length}</span>
            <div class="tag-overflow-dropdown" onclick="event.stopPropagation();">${hiddenHtml}</div>
        </span>`;
    } else {
        const allTags = [
            ...tagsList.map(t => ({ t, badge: analysisBadge })),
            ...nonSpecialUserTags.map(t => ({ t, badge: userBadge })),
        ].sort(byPriorityDesc);
        visibleHtml = allTags.map(x => x.badge(x.t)).join('');
    }

    return `
        <div class="${editorClass}" data-etype="${escapeAttr(etype)}" data-eid="${escapeAttr(eid)}" style="display:inline-flex; flex-wrap:wrap; gap:2px; align-items:center; vertical-align:middle; max-width:100%;">
            <button class="bookmark-btn ${isBookmarked ? 'active' : ''}"
                    title="${isBookmarked ? 'Remove Bookmark' : 'Add Bookmark'}"
                    onclick="${escapeAttr(bookmarkOnClick)}">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
            </button>
            <button class="ignore-btn ${isIgnored ? 'active' : ''}"
                    title="${isIgnored ? 'Remove Ignore' : 'Add Ignore'}"
                    onclick="${escapeAttr(ignoreOnClick)}">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>
            </button>
            ${visibleHtml}
            ${overflowHtml}
            <button class="add-tag-btn" onclick="${escapeAttr(addOnClick)}">+</button>
        </div>
    `;
};

window.applyClusterFilter = (uuid, isBinary = false) => {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    const { collection, pool } = targetWindow.getRoutingState ? targetWindow.getRoutingState() : { collection: '', pool: null };
    const col = collection || '';
    // Cluster uuids are scoped to the namespace that computed them, so a pool
    // context must stay on the pool route rather than fall back to /collections.
    const basePath = pool ? `/pools/${encodeURIComponent(pool)}` : `/collections/${encodeURIComponent(col)}`;

    if (isBinary) {
        const inputId = 'flt-file-cluster';
        let input = targetWindow.document.getElementById(inputId);
        if (input) {
            input.value = uuid;
            if (targetWindow.applyAdvancedFileSearch) {
                targetWindow.applyAdvancedFileSearch();
            }
        } else {
            const params = new URLSearchParams();
            params.set('bin_cluster_uuid', uuid);
            if (typeof targetWindow.navigate === 'function') {
                targetWindow.navigate('files', params, col);
            } else {
                targetWindow.location.href = `${basePath}/files?${params.toString()}`;
            }
        }
    } else {
        const isSim = targetWindow.location.pathname.includes('/similarity') || targetWindow.location.pathname.includes('/vs/');
        const viewKey = isSim ? 'function-similarity' : 'functions';
        const inputId = isSim ? 'flt-sim-cluster' : 'flt-func-cluster';
        
        let input = targetWindow.document.getElementById(inputId);
        if (input) {
            input.value = uuid;
            if (isSim) {
                if (targetWindow.applySimSearch) targetWindow.applySimSearch();
            } else {
                if (targetWindow.applyAdvancedFuncSearch) targetWindow.applyAdvancedFuncSearch();
            }
        } else {
            const params = new URLSearchParams();
            params.set('cluster_uuid', uuid);
            if (typeof targetWindow.navigate === 'function') {
                targetWindow.navigate(viewKey, params, col);
            } else {
                const searchPath = isSim ? 'functions/similarities' : 'functions';
                targetWindow.location.href = `${basePath}/${searchPath}?${params.toString()}`;
            }
        }
    }
};

window.showClusterCardTooltip = function(event, uuid, name, size, stability, cohesion, avg_features, clusterType = 'function') {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    let adjustedEvent = event;
    if (targetWindow !== window) {
        const iframe = window.getHostFrame();
        if (iframe) {
            const rect = iframe.getBoundingClientRect();
            adjustedEvent = {
                clientX: event.clientX + rect.left,
                clientY: event.clientY + rect.top,
                target: event.target
            };
        }
    }
    
    if (clusterType === 'file' && typeof targetWindow.showBinClusterTableTooltip === 'function') {
        targetWindow.showBinClusterTableTooltip(adjustedEvent, uuid, name, size, stability, cohesion, avg_features, null);
    } else if (typeof targetWindow.showClusterTableTooltip === 'function') {
        targetWindow.showClusterTableTooltip(adjustedEvent, uuid, name, size, stability, cohesion, avg_features, null, clusterType);
    }
};

window.hideClusterCardTooltip = function(event) {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    const el = targetWindow.document.getElementById('hierarchy-tooltip');
    const binEl = targetWindow.document.getElementById('bin-hierarchy-tooltip');
    
    if (event && event.relatedTarget) {
        if (el && (el === event.relatedTarget || el.contains(event.relatedTarget))) return;
        if (binEl && (binEl === event.relatedTarget || binEl.contains(event.relatedTarget))) return;
    }

    if (typeof targetWindow.hideClusterTableTooltip === 'function') {
        targetWindow.hideClusterTableTooltip(event);
    }
    if (typeof targetWindow.hideBinClusterTableTooltip === 'function') {
        targetWindow.hideBinClusterTableTooltip(event);
    }
    if (el) el.style.display = 'none';
    if (binEl) binEl.style.display = 'none';
};

window.moveClusterCardTooltip = function(e) {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    const tooltip = targetWindow.document.getElementById('hierarchy-tooltip');
    const binTooltip = targetWindow.document.getElementById('bin-hierarchy-tooltip');
    const activeTooltip = (tooltip && tooltip.style.display === 'block') ? tooltip : ((binTooltip && binTooltip.style.display === 'block') ? binTooltip : null);
    
    if (!activeTooltip) return;
    
    if (targetWindow !== window) {
        const iframe = window.getHostFrame();
        if (iframe) {
            const rect = iframe.getBoundingClientRect();
            const adjustedEvent = {
                clientX: e.clientX + rect.left,
                clientY: e.clientY + rect.top
            };
            if (activeTooltip === tooltip && typeof targetWindow.moveClusterTableTooltip === 'function') {
                targetWindow.moveClusterTableTooltip(adjustedEvent);
            } else if (activeTooltip === binTooltip && typeof targetWindow.moveBinClusterTableTooltip === 'function') {
                targetWindow.moveBinClusterTableTooltip(adjustedEvent);
            }
            return;
        }
    }

    const container = e.target.closest('.cluster-cards-container');
    if (container) {
        const overflow = container.querySelector('.cluster-overflow-box');
        const isOverflowVisible = overflow && window.getComputedStyle(overflow).display !== 'none';
        const boxRect = (isOverflowVisible && overflow) ? overflow.getBoundingClientRect() : container.getBoundingClientRect();
        const tooltipRect = activeTooltip.getBoundingClientRect();
        
        let x = boxRect.right + 15;
        let y = boxRect.top;

        if (x + tooltipRect.width > window.innerWidth) {
            x = boxRect.left - tooltipRect.width - 15;
        }
        if (y + tooltipRect.height > window.innerHeight) {
            y = Math.max(10, window.innerHeight - tooltipRect.height - 15);
        }
        
        x = Math.max(5, x);
        y = Math.max(5, y);
        
        activeTooltip.style.left = x + 'px';
        activeTooltip.style.top = y + 'px';
    }
};

window.renderClusterCards = (clusters, isBinary = false) => {
    if (!clusters || clusters.length === 0) return '';
    
    const threshold = typeof UIParams !== 'undefined' ? UIParams.cohesionThreshold : 0.5;
    const validClusters = clusters.filter(c => (c.cohesion_score || 0) >= threshold);
    if (validClusters.length === 0) return '';

    const sorted = [...validClusters].sort((a, b) => (b.cohesion_score || 0) - (a.cohesion_score || 0));
    
    const renderCard = (c, isHidden = false) => {
        const name = c.cluster_name || `Cluster ${c.cluster_id}`;
        const score = (c.cohesion_score || 0).toFixed(2);
        const uuid = c.cluster_uuid;
        const hue = Math.max(0, Math.min(120, (c.cohesion_score || 0) * 120));
        const color = `hsl(${hue}, var(--color-s-high), var(--color-l-high))`;
        
        let displayName = name;
        if (isBinary) {
            let extra = '';
            if (c.yara_distribution && c.yara_distribution.length > 0) {
                extra = `${c.yara_distribution[0].value} (${c.yara_distribution[0].percent}%)`;
            } else if (c.avtype_distribution && c.avtype_distribution.length > 0) {
                extra = `${c.avtype_distribution[0].value} (${c.avtype_distribution[0].percent}%)`;
            }
            if (extra) {
                displayName = `${extra}`;
            }
        }
        
        const maxWidth = isBinary ? '160px' : '80px';
        const cardClass = isHidden ? 'tag-card cluster-card cluster-hidden' : 'tag-card cluster-card';
        const clusterType = isBinary ? 'file' : 'function';

        return `
        <span class="${cardClass}"
              onmouseenter="showClusterCardTooltip(event, ${escapeAttr(jsString(uuid))}, ${escapeAttr(jsString(name))}, ${Number(c.member_count || 0)}, ${Number(c.cluster_stability || 0)}, ${Number(c.cohesion_score || 0)}, ${Number(c.avg_features || 0)}, ${escapeAttr(jsString(clusterType))})"
              onmouseleave="hideClusterCardTooltip(event)"
              onmousemove="moveClusterCardTooltip(event)"
              onclick="applyClusterFilter(${escapeAttr(jsString(uuid))}, ${isBinary})"
              style="border-color:${tagAlpha(color, 27)}; color:${color}; background:${tagAlpha(color, 7)}; align-items:center; gap:4px; padding:2px 6px 2px 8px; font-size:0.65rem; border-radius:12px; margin:2px; cursor:pointer;">
            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
                 stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"></circle>
                <circle cx="12" cy="12" r="4"></circle>
            </svg>
            <span style="max-width:${maxWidth}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${escapeAttr(displayName)}">${escapeHtml(displayName)}</span>
            <span style="opacity:0.8; font-family:monospace; font-size:0.65rem;">${Number(c.member_count || 0)}</span>
        </span>`;
    };

    const hasMore = sorted.length > 1;
    const moreHtml = hasMore ? `
        <span class="analysis-tag-badge cluster-card-more"
              style="cursor:help; margin:2px; font-size:0.65rem; padding: 2px 6px;">
            +${sorted.length - 1}
        </span>` : '';

    const allHtml = sorted.map(c => renderCard(c, false)).join('');
    const overflowBox = `
        <div class="cluster-overflow-box">
            <div style="font-size:0.6rem; color:var(--subtle); margin-bottom:4px; text-transform:uppercase; letter-spacing:1px; padding:0 4px;">Clusters</div>
            ${allHtml}
        </div>`;

    return `<div class="cluster-cards-container" style="position:relative; display:inline-flex; align-items:center; padding:6px; margin:-6px; cursor:default;">
        ${renderCard(sorted[0])}${moreHtml}${overflowBox}
    </div>`;
};

function attachTagAutocomplete(input, onSelect) {
    if (input._acAttached) return;
    input._acAttached = true;
    input.setAttribute('autocomplete', 'off');

    const dropdown = document.createElement('div');
    dropdown.className = 'tag-autocomplete-dropdown';
    document.body.appendChild(dropdown);
    input._autocompleteDropdown = dropdown;

    const positionDropdown = () => {
        const rect = input.getBoundingClientRect();
        dropdown.style.position = 'fixed';
        dropdown.style.left = rect.left + 'px';
        dropdown.style.top = rect.bottom + 'px';
        dropdown.style.width = Math.max(150, rect.width) + 'px';
        dropdown.style.zIndex = '200000';
    };

    let activeIndex = -1;
    let currentSuggestions = [];

    const updateActiveStyle = () => {
        const items = dropdown.querySelectorAll('.tag-suggestion-item');
        items.forEach((item, index) => {
            if (index === activeIndex) {
                item.classList.add('active');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('active');
            }
        });
    };

    const renderSuggestions = (tags) => {
        dropdown.innerHTML = '';
        currentSuggestions = tags;
        activeIndex = -1;
        tags.forEach(t => {
            const item = document.createElement('div');
            item.className = 'tag-suggestion-item';
            // Through `getTagMetadata` so a suggestion's dot is the colour the
            // tag will actually have once applied, derived or hand-picked.
            const meta = { ...(tagMetadata[t] || { count: 0 }), ...window.getTagMetadata(t) };
            const isBookmark = (t === 'bookmark');
            const isIgnore = (t === 'ignore');
            item.innerHTML = `
                <div class="tag-color-dot" style="background:${meta.color}; display:${(isBookmark || isIgnore) ? 'none' : 'block'};"></div>
                ${isBookmark ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="#66d9ef" stroke="#66d9ef" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>' : ''}
                ${isIgnore ? '<svg width="12" height="12" viewBox="0 0 24 24" fill="#f92672" stroke="#f92672" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>' : ''}
                <span style="flex:1">${t}</span>
                <span class="dim" style="font-size:0.6rem; margin-left:10px;">Prio: ${meta.priority || 0}</span>
                <span class="dim" style="font-size:0.6rem; margin-left:10px;">${meta.count || 0}</span>
            `;
            item.onmousedown = (e) => {
                e.preventDefault();
                onSelect(t);
                dropdown.style.display = 'none';
            };
            dropdown.appendChild(item);
        });
        positionDropdown();
        dropdown.style.display = 'block';
    };

    const showSuggestions = (filter = '') => {
        const query = filter.toLowerCase().trim();
        const tags = Object.keys(tagMetadata).filter(t => t.toLowerCase().includes(query));
        if (tags.length > 0) {
            renderSuggestions(tags);
        } else {
            dropdown.style.display = 'none';
            currentSuggestions = [];
            activeIndex = -1;
        }
    };

    input.onfocus = () => showSuggestions(input.value);
    input.onclick = () => showSuggestions(input.value);
    input.oninput = () => showSuggestions(input.value);
    input.onblur = () => {
        setTimeout(() => {
            dropdown.style.display = 'none';
            activeIndex = -1;
        }, 200);
    };

    const originalOnKeyDown = input.onkeydown;
    input.onkeydown = (e) => {
        if (dropdown.style.display === 'block' && currentSuggestions.length > 0) {
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                activeIndex = (activeIndex + 1) % currentSuggestions.length;
                updateActiveStyle();
                return;
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                activeIndex = (activeIndex - 1 + currentSuggestions.length) % currentSuggestions.length;
                updateActiveStyle();
                return;
            } else if (e.key === 'Enter') {
                if (activeIndex >= 0 && activeIndex < currentSuggestions.length) {
                    e.preventDefault();
                    e.stopPropagation();
                    const selectedValue = currentSuggestions[activeIndex];
                    onSelect(selectedValue);
                    dropdown.style.display = 'none';
                    activeIndex = -1;
                    return;
                }
            } else if (e.key === 'Escape') {
                e.preventDefault();
                dropdown.style.display = 'none';
                activeIndex = -1;
                return;
            }
        }
        if (originalOnKeyDown) {
            originalOnKeyDown.call(input, e);
        }
    };

    if (document.activeElement === input) showSuggestions(input.value);
}

function attachAutocomplete(input, level, field, onSelect) {
    if (input._acAttached) return;
    input._acAttached = true;
    input.setAttribute('autocomplete', 'off');

    const dropdown = document.createElement('div');
    dropdown.className = 'tag-autocomplete-dropdown';
    document.body.appendChild(dropdown);
    input._autocompleteDropdown = dropdown;

    const positionDropdown = () => {
        const rect = input.getBoundingClientRect();
        dropdown.style.position = 'fixed';
        dropdown.style.left = rect.left + 'px';
        dropdown.style.top = rect.bottom + 'px';
        dropdown.style.width = Math.max(150, rect.width) + 'px';
        dropdown.style.zIndex = '200000';
    };

    let activeIndex = -1;
    let currentSuggestions = [];

    const updateActiveStyle = () => {
        const items = dropdown.querySelectorAll('.tag-suggestion-item');
        items.forEach((item, index) => {
            if (index === activeIndex) {
                item.classList.add('active');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('active');
            }
        });
    };

    const renderSuggestions = (items) => {
        dropdown.innerHTML = '';
        currentSuggestions = items;
        activeIndex = -1;
        items.forEach(item => {
            const div = document.createElement('div');
            div.className = 'tag-suggestion-item';
            div.style.display = 'flex';
            div.style.justifyContent = 'space-between';
            // On a tag field the suggestion carries the tag's colour, as a dot
            // rather than coloured text: a whole dropdown of coloured rows is
            // unreadable, and the dot is enough to tie the entry to the same
            // tag in the tables and the graphs.
            const dot = /tag/i.test(field)
                ? `<span class="tag-color-dot" style="background:${window.getTagMetadata(item.value).color};"></span>`
                : '';
            div.innerHTML = `${dot}<span style="flex:1">${escapeHtml(item.value)}</span> <span class="dim" style="font-size:0.6rem; margin-left:10px;">${escapeHtml(item.count)}</span>`;
            div.onmousedown = (e) => {
                e.preventDefault();
                onSelect(item.value);
                dropdown.style.display = 'none';
            };
            dropdown.appendChild(div);
        });
        positionDropdown();
        dropdown.style.display = 'block';
    };


    const showSuggestions = async (filter = '') => {
        const col = typeof getCurrentCollection === 'function' ? getCurrentCollection() : '';
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        let url = `/api/search/autocomplete?level=${level}&field=${field}&q=${encodeURIComponent(filter)}&limit=50&collection=${encodeURIComponent(col || '')}`;
        if (pool) {
            url += `&pool=${encodeURIComponent(pool)}`;
        }

        try {
            const res = await fetch(url);
            if (res.ok) {
                const data = await res.json();
                const items = data.results || [];
                if (items.length > 0) {
                    renderSuggestions(items);
                } else {
                    dropdown.style.display = 'none';
                    currentSuggestions = [];
                    activeIndex = -1;
                }
            }
        } catch (err) {
            console.error("Autocomplete fetch failed", err);
        }
    };

    input.onfocus = () => showSuggestions(input.value);
    input.onclick = () => showSuggestions(input.value);
    input.oninput = () => showSuggestions(input.value);
    input.onblur = () => {
        setTimeout(() => {
            dropdown.style.display = 'none';
            activeIndex = -1;
        }, 200);
    };

    const originalOnKeyDown = input.onkeydown;
    input.onkeydown = (e) => {
        if (dropdown.style.display === 'block' && currentSuggestions.length > 0) {
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                activeIndex = (activeIndex + 1) % currentSuggestions.length;
                updateActiveStyle();
                return;
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                activeIndex = (activeIndex - 1 + currentSuggestions.length) % currentSuggestions.length;
                updateActiveStyle();
                return;
            } else if (e.key === 'Enter') {
                if (activeIndex >= 0 && activeIndex < currentSuggestions.length) {
                    e.preventDefault();
                    e.stopPropagation();
                    const selectedValue = currentSuggestions[activeIndex].value;
                    onSelect(selectedValue);
                    dropdown.style.display = 'none';
                    activeIndex = -1;
                    return;
                }
            } else if (e.key === 'Escape') {
                e.preventDefault();
                dropdown.style.display = 'none';
                activeIndex = -1;
                return;
            }
        }
        if (originalOnKeyDown) {
            originalOnKeyDown.call(input, e);
        }
    };

    if (document.activeElement === input) showSuggestions(input.value);
}


// ----------------------------------------------------
// Togglers
// ----------------------------------------------------

async function toggleEntityBookmark(event, etype, eid) {
    event.stopPropagation();
    const btn = event.currentTarget;
    const isBookmarked = btn.classList.contains('active');
    const action = isBookmarked ? 'remove' : 'add';

    if (action === 'remove') {
        await removeTag(null, etype, eid, 'bookmark');
        btn.classList.remove('active');
        btn.title = "Add Bookmark";
    } else {
        await confirmAddTag(etype, eid, 'bookmark');
        btn.classList.add('active');
        btn.title = "Remove Bookmark";
    }
    refreshAllRowColors();
}

async function toggleEntityIgnore(event, etype, eid) {
    event.stopPropagation();
    const btn = event.currentTarget;
    const isIgnored = btn.classList.contains('active');
    const action = isIgnored ? 'remove' : 'add';

    if (action === 'remove') {
        await removeTag(null, etype, eid, 'ignore');
        btn.classList.remove('active');
        btn.title = "Add Ignore";
    } else {
        await confirmAddTag(etype, eid, 'ignore');
        btn.classList.add('active');
        btn.title = "Remove Ignore";
    }
    refreshAllRowColors();
}

// NOTE: id1, id2, algo are optional now if eid is given explicitly, but kept for compatibility.
// If the button passes exactly one parameter `'pairId'`, it lands in id1.
async function toggleBookmark(event, id1, id2, algo) {
    event.stopPropagation();
    const btn = event.currentTarget;
    const isBookmarked = btn.classList.contains('active');
    const pairId = (id2 && algo) ? `${id1}|${id2}|${algo}` : id1;

    if (isBookmarked) {
        await removeTag(null, 'similarity', pairId, 'bookmark');
    } else {
        await confirmAddTag('similarity', pairId, 'bookmark', btn.closest('.sim-tags-editor'));
    }
}

async function toggleIgnore(event, id1, id2, algo) {
    event.stopPropagation();
    const btn = event.currentTarget;
    const isIgnored = btn.classList.contains('active');
    const pairId = (id2 && algo) ? `${id1}|${id2}|${algo}` : id1;

    if (isIgnored) {
        await removeTag(null, 'similarity', pairId, 'ignore');
    } else {
        await confirmAddTag('similarity', pairId, 'ignore', btn.closest('.sim-tags-editor'));
    }
}

async function startAddTag(event, etype, eid) {
    event.stopPropagation();
    const btn = event.target;
    const parent = btn.parentElement;

    const wrapper = document.createElement('div');
    wrapper.style.position = 'relative';
    wrapper.style.display = 'inline-block';

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'tag-input-field';
    input.placeholder = 'Tag...';

    wrapper.appendChild(input);
    parent.replaceChild(wrapper, btn);

    attachTagAutocomplete(input, async (tag) => {
        await confirmAddTag(etype, eid, tag, parent);
        cleanup();
    });
    input.focus();

    let isClosing = false;
    const cleanup = () => {
        if (isClosing) return;
        isClosing = true;
        if (input._autocompleteDropdown) {
            input._autocompleteDropdown.remove();
        }
        if (wrapper.parentNode === parent) parent.replaceChild(btn, wrapper);
    };

    input.onblur = () => {
        setTimeout(cleanup, 200);
    };

    input.onkeyup = async (e) => {
        if (e.key === 'Enter') {
            const tag = input.value.trim();
            if (tag) await confirmAddTag(etype, eid, tag, parent);
            cleanup();
        } else if (e.key === 'Escape') {
            cleanup();
        }
    };
}

async function confirmAddTag(etype, eid, tag, container) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || window.location.search);
    const colStr = params.get('collection');
    const col = colStr || (typeof getCurrentCollection === 'function' ? getCurrentCollection() : window.getCollectionFromId(eid));

    let targets = [{ etype, eid, container }];
    const selectedIds = (typeof getSelectedTableIds === 'function') ? getSelectedTableIds(etype) : [];

    if (selectedIds.includes(eid)) {
        targets = selectedIds.map(id => {
            // Rows without a data-id (bin diff) still have their editor in the DOM.
            const targetContainer = document.querySelector(`[data-etype="${etype}"][data-eid="${CSS.escape(id)}"]`);
            return { etype, eid: id, container: targetContainer };
        });
    }

    let uiTargets = [container];
    if (targets.length > 1) {
        uiTargets = targets.map(t => t.container).filter(c => !!c);
    } else if (etype === 'function' || etype === 'file') {
        const allEditors = document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`);
        if (allEditors.length > 0) uiTargets = Array.from(allEditors);
    }

    let mainSuccess = false;

    if (targets.length > 1) {
        try {
            const res = await fetch('/api/tags/bulk_add', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    collection: col,
                    entity_type: etype,
                    entity_ids: targets.map(t => t.eid),
                    tag
                })
            });
            if (res.ok) {
                mainSuccess = true;
                if (!tagMetadata[tag]) await fetchTagMetadata(col);
                targets.forEach(t => {
                    const editorsToUpdate = (t.etype === 'function' || t.etype === 'file')
                        ? document.querySelectorAll(`[data-etype="${t.etype}"][data-eid="${t.eid}"]`)
                        : (t.container ? [t.container] : document.querySelectorAll(`[data-eid="${t.eid}"][data-etype="${t.etype}"]`));

                    updateUIForTagAdd(editorsToUpdate, tag);
                });
            }
        } catch (err) { console.error(err); }
    } else if (targets.length === 1) {
        const t = targets[0];
        try {
            const res = await fetch('/api/tags/add', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ collection: col, entity_type: t.etype, entity_id: t.eid, tag })
            });

            if (res.ok) {
                mainSuccess = true;
                if (!tagMetadata[tag]) await fetchTagMetadata(col);
                updateUIForTagAdd(uiTargets, tag);
            }
        } catch (err) { console.error(err); }
    }

    refreshAllRowColors();

    // Broadcast tag update to parent dashboard (and siblings) via postMessage
    const msg = {
        type: 'bsimvis_tag_update',
        action: 'add',
        tag,
        targets: targets.map(t => ({ etype: t.etype, eid: t.eid }))
    };
    if (window.parent && window.parent !== window) {
        window.parent.postMessage(msg, '*');
    } else {
        window.postMessage(msg, '*');
    }

    return mainSuccess;
}

function updateUIForTagAdd(editors, tag) {
    const meta = tagMetadata[tag] || { color: '#66d9ef' };
    const color = window.tagInk(safeCssColor(meta.color));
    const isBookmark = (tag === 'bookmark');
    const isIgnore = (tag === 'ignore');
    const coll = typeof getCurrentCollection === 'function' ? getCurrentCollection() : '';

    editors.forEach(editor => {
        if (!editor) return;
        if (isBookmark) {
            const btn = editor.querySelector('.bookmark-btn');
            if (btn) { btn.classList.add('active'); btn.title = "Remove Bookmark"; }
        } else if (isIgnore) {
            const btn = editor.querySelector('.ignore-btn');
            if (btn) { btn.classList.add('active'); btn.title = "Remove Ignore"; }
        } else {
            const existing = Array.from(editor.querySelectorAll('.sim-tag-card')).find(c => c.textContent.trim().startsWith(tag));
            if (!existing) {
                const card = document.createElement('span');
                card.className = 'sim-tag-card';
                card.style.borderColor = tagAlpha(color, 27);
                card.style.color = color;
                card.style.background = tagAlpha(color, 7);
                card.style.cursor = 'pointer';

                // Add event handlers for tooltip and context menu
                card.setAttribute('onmouseenter', `showTooltip(event, ${jsString(tag)}, ${jsString(coll)})`);
                card.setAttribute('onmouseleave', 'hideTooltip()');
                card.setAttribute('oncontextmenu', `handleTagContextMenu(event, ${jsString(tag)})`);

                const removeClick = `removeTag(event, ${jsString(editor.dataset.etype)}, ${jsString(editor.dataset.eid)}, ${jsString(tag)})`;
                card.innerHTML = `${escapeHtml(tag)} <span class="remove-tag-btn" onclick="${escapeAttr(removeClick)}" style="background:${escapeAttr(color)}22">×</span>`;
                const addBtn = editor.querySelector('.add-tag-btn');
                if (addBtn) editor.insertBefore(card, addBtn);
                else editor.appendChild(card);
            }
        }
    });
}

async function removeTag(event, etype, eid, tag) {
    if (event) event.stopPropagation();
    const params = new URLSearchParams(window.location.hash.split('?')[1] || window.location.search);
    const colStr = params.get('collection');
    const col = colStr || (typeof getCurrentCollection === 'function' ? getCurrentCollection() : window.getCollectionFromId(eid));

    let targets = [{ etype, eid }];
    const selectedIds = (typeof getSelectedTableIds === 'function') ? getSelectedTableIds(etype) : [];

    if (selectedIds.includes(eid)) {
        targets = selectedIds.map(id => {
            // Rows without a data-id (bin diff) still have their editor in the DOM.
            const targetContainer = document.querySelector(`[data-etype="${etype}"][data-eid="${CSS.escape(id)}"]`);
            return { etype, eid: id, container: targetContainer };
        });
    }

    if (targets.length > 1) {
        try {
            const res = await fetch('/api/tags/bulk_remove', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    collection: col,
                    entity_type: etype,
                    entity_ids: targets.map(t => t.eid),
                    tag
                })
            });
            if (res.ok) {
                targets.forEach(t => {
                    const editorsToUpdate = (t.etype === 'function' || t.etype === 'file')
                        ? document.querySelectorAll(`[data-etype="${t.etype}"][data-eid="${t.eid}"]`)
                        : (t.container ? [t.container] : document.querySelectorAll(`[data-eid="${t.eid}"][data-etype="${t.etype}"]`));

                    updateUIForTagRemove(editorsToUpdate, tag);
                });
            }
        } catch (err) { console.error(err); }
    } else if (targets.length === 1) {
        const t = targets[0];
        try {
            const res = await fetch('/api/tags/remove', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ collection: col, entity_type: t.etype, entity_id: t.eid, tag })
            });
            if (res.ok) {
                const editorsToUpdate = (t.etype === 'function' || t.etype === 'file')
                    ? document.querySelectorAll(`[data-etype="${t.etype}"][data-eid="${t.eid}"]`)
                    : (t.container ? [t.container] : document.querySelectorAll(`[data-eid="${t.eid}"][data-etype="${t.etype}"]`));

                updateUIForTagRemove(editorsToUpdate, tag);
            }
        } catch (err) { console.error(err); }
    }
    refreshAllRowColors();

    // Broadcast tag update to parent dashboard (and siblings) via postMessage
    const msg = {
        type: 'bsimvis_tag_update',
        action: 'remove',
        tag,
        targets: targets.map(t => ({ etype: t.etype, eid: t.eid }))
    };
    if (window.parent && window.parent !== window) {
        window.parent.postMessage(msg, '*');
    } else {
        window.postMessage(msg, '*');
    }
}

function updateUIForTagRemove(editors, tag) {
    editors.forEach(container => {
        if (!container) return;
        if (tag === 'bookmark') {
            const btn = container.querySelector('.bookmark-btn');
            if (btn) { btn.classList.remove('active'); btn.title = "Add Bookmark"; }
        } else if (tag === 'ignore') {
            const btn = container.querySelector('.ignore-btn');
            if (btn) { btn.classList.remove('active'); btn.title = "Add Ignore"; }
        } else {
            const cards = container.querySelectorAll('.sim-tag-card');
            cards.forEach(c => { if (c.textContent.trim().startsWith(tag)) c.remove(); });
        }
    });
}

async function loadFieldCardinalities(col, level, fieldMap) {
    if (!col) return;
    const fields = Object.keys(fieldMap);
    const query = fields.map(f => `field=${f}`).join('&');
    try {
        const apiParams = (window.getApiParams || window.parent.getApiParams)(col);
        const res = await fetch(`/api/search/fields?${apiParams}&level=${level}&${query}`);
        if (res.ok) {
            const stats = await res.json();
            for (const [field, count] of Object.entries(stats)) {
                const inputId = fieldMap[field];
                const input = document.getElementById(inputId);
                if (input) {
                    const originalPlaceholder = input.getAttribute('data-original-placeholder') || input.placeholder;
                    if (!input.getAttribute('data-original-placeholder')) {
                        input.setAttribute('data-original-placeholder', originalPlaceholder);
                    }
                    if (count > 0) {
                        input.placeholder = `${originalPlaceholder} (${count})`;
                        input.title = `Total unique values for ${field}: ${count}`;
                    }
                }
            }
        }
    } catch (err) {
        console.error("Failed to load cardinalities", err);
    }
}

// ---------------------------------------------------------------------------
// Cross-window tag sync: receive forwarded tag updates from the dashboard
// (works in any iframe: code view, diff view, feature view, etc.)
// ---------------------------------------------------------------------------
window.addEventListener('message', (event) => {
    const msg = event.data;
    if (!msg || msg.type !== 'bsimvis_tag_update') return;

    // Only apply in iframe pages (avoid double-applying in the dashboard itself,
    // which already handles this in dashboard.js)
    if (window.parent === window) return;

    const { action, tag, targets } = msg;
    if (!tag || !targets || !targets.length) return;

    // If we encounter a new tag, refresh metadata to get its color/priority
    if (action === 'add' && !tagMetadata[tag]) {
        if (typeof fetchTagMetadata === 'function') {
            const col = typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : '';
            fetchTagMetadata(col);
        }
    }

    targets.forEach(({ etype, eid }) => {
        const editors = Array.from(document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`));
        if (editors.length === 0) return;

        if (action === 'add' && typeof updateUIForTagAdd === 'function') {
            updateUIForTagAdd(editors, tag);
        } else if (action === 'remove' && typeof updateUIForTagRemove === 'function') {
            updateUIForTagRemove(editors, tag);
        }
    });

    if (typeof refreshAllRowColors === 'function') refreshAllRowColors();
});

// --- Tag provenance ---------------------------------------------------------
// Which rule minted an analysis tag, and where that rule lives. Deliberately
// click-only and fetched on demand: the tag lists, the search filters and the
// chips themselves stay flat strings, so nothing on the render path pays for
// this. Delegated off document rather than wired per badge, so every place
// that renders an .analysis-tag-badge gets it without knowing about it.

// `<coll>:file:<md5>` / `<coll>:func:<md5>:<addr>` -> the collection. Pool
// collections are themselves prefixed (`global:pool:x:file:...`), so the split
// is on the entity kind rather than on the first colon.
const provEntityCollection = (eid) => String(eid || '').replace(/:(file|func):.*$/, '');

// Rule text is fetched per rule id, once. A mirror rule body is a few KB and a
// popup gets reopened constantly, so the cache is worth its one line.
const provSourceCache = {};

const provRuleSource = (rid) => {
    if (rid in provSourceCache) return Promise.resolve(provSourceCache[rid]);
    return fetch(`/api/tags/rule_source?id=${encodeURIComponent(rid)}`)
        .then((res) => res.json())
        .then((data) => (provSourceCache[rid] = (data && data.text) || null))
        .catch(() => null);
};

window.showTagProvenance = (e, tag, eid) => {
    let el = document.getElementById('tag-provenance-popup');
    if (!el) {
        el = document.createElement('div');
        el.id = 'tag-provenance-popup';
        el.style.cssText = "position:fixed; z-index:20015; background:var(--card-bg); border:1px solid var(--border); padding:0; border-radius:8px; display:none; font-size:0.78rem; color:var(--text); width:520px; max-width:92vw; box-shadow:0 8px 24px rgba(0,0,0,0.35);";
        document.body.appendChild(el);
        // Hoverable: the pointer leaving the badge must be able to land in the
        // popup without it closing, so both ends cancel the pending close.
        el.addEventListener('mouseenter', () => window.cancelTagProvenanceClose());
        el.addEventListener('mouseleave', () => window.hideTagProvenance(400));
    }

    const row = (label, value) => value
        ? `<div style="display:grid; grid-template-columns:78px 1fr; gap:4px 10px; margin-top:3px;">
               <span style="color:var(--dim)">${label}</span>
               <span style="word-break:break-all;">${escapeHtml(String(value))}</span>
           </div>`
        : '';

    const chips = (tags) => (tags && tags.length)
        ? `<div style="display:flex; flex-wrap:wrap; gap:4px; margin-top:6px;">` +
          tags.map(t => `<span style="background:var(--hover); border:1px solid var(--border); border-radius:10px; padding:1px 7px; font-size:0.7rem;">${escapeHtml(String(t))}</span>`).join('') +
          `</div>`
        : '';

    el.innerHTML = `<div style="padding:12px 14px; color:var(--dim)">Looking up <b>${escapeHtml(tag)}</b>...</div>`;
    el.style.display = 'block';

    let x = e.clientX + 12, y = e.clientY + 12;
    if (x + 540 > window.innerWidth) x = Math.max(10, window.innerWidth - 550);
    if (y + 380 > window.innerHeight) y = Math.max(10, window.innerHeight - 390);
    el.style.left = x + 'px';
    el.style.top = y + 'px';

    const header = (note) => `
        <div style="padding:10px 14px 8px; border-bottom:1px solid var(--border); background:var(--hover); border-radius:8px 8px 0 0;">
            <div style="font-weight:bold; display:flex; justify-content:space-between; gap:10px; align-items:center;">
                <span style="word-break:break-all;">${escapeHtml(tag)}</span>
                <span id="tag-prov-close" style="color:var(--dim); cursor:pointer; font-size:1rem;">&times;</span>
            </div>
            ${note ? `<div style="color:var(--dim); margin-top:4px;">${escapeHtml(note)}</div>` : ''}
        </div>`;

    // One card per rule. The rule text is the expensive half, so it loads for
    // the first card only and the rest load when their toggle is clicked.
    const card = (r, i) => `
        <div style="padding:8px 14px; border-bottom:1px solid var(--border);">
            <div style="color:var(--accent); font-weight:bold; word-break:break-all;">${escapeHtml(r.title || r.name || r.source || r.id || '')}</div>
            ${row('Origin', r.source)}
            ${row('Rule id', r.id)}
            ${row('Rule name', r.title && r.name ? r.name : '')}
            ${row('File', r.path)}
            ${row('Format', r.format)}
            ${row('Author', r.author || (r.authors || []).join(', '))}
            ${row('Rules', (r.rules || []).join(', '))}
            ${row('Scope', r.scopes ? Object.entries(r.scopes).map(([k, v]) => `${k}: ${v}`).join(', ') : '')}
            ${row('ATT&CK', (r.attack || []).join(', '))}
            ${row('MBC', (r.mbc || []).join(', '))}
            ${row('Examples', (r.examples || []).join(', '))}
            ${row('License', r.license)}
            ${row('Upstream', r.upstream)}
            ${chips(r.tags)}
            <div style="margin-top:7px; display:flex; gap:12px;">
                ${r.id ? `<span class="tag-prov-toggle" data-rid="${escapeAttr(r.id)}" data-idx="${i}" style="color:var(--accent); cursor:pointer;">Rule text</span>` : ''}
                ${r.url ? `<a href="${escapeAttr(r.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent);">Open source &nearr;</a>` : ''}
            </div>
            <pre id="tag-prov-src-${i}" style="display:none; margin:7px 0 0; padding:8px; background:var(--hover); border:1px solid var(--border); border-radius:6px; max-height:220px; overflow:auto; white-space:pre; font-size:0.72rem; line-height:1.35;"></pre>
        </div>`;

    const loadInto = (rid, pre) => {
        pre.style.display = 'block';
        pre.textContent = 'Loading rule...';
        provRuleSource(rid).then((text) => {
            pre.textContent = text || 'Rule text not available locally (see Open source).';
        });
    };

    const paint = (records, note) => {
        const body = records.length
            ? records.map(card).join('')
            : `<div style="padding:10px 14px; color:var(--dim); font-style:italic;">No source recorded for this tag.</div>`;
        // Scrolls as one list: many rules stay reachable without the popup
        // growing past the viewport, and each rule body scrolls inside its own
        // box so the outer scroll position does not jump when one expands.
        el.innerHTML = header(note) +
            `<div style="max-height:min(60vh, 520px); overflow-y:auto;">${body}</div>`;

        const close = el.querySelector('#tag-prov-close');
        if (close) close.onclick = () => window.hideTagProvenance(0);

        el.querySelectorAll('.tag-prov-toggle').forEach((t) => {
            t.onclick = () => {
                const pre = el.querySelector(`#tag-prov-src-${t.dataset.idx}`);
                if (!pre) return;
                if (pre.style.display === 'block') { pre.style.display = 'none'; return; }
                loadInto(t.dataset.rid, pre);
            };
        });

        if (records.length && records[0].id) {
            const first = el.querySelector('#tag-prov-src-0');
            if (first) loadInto(records[0].id, first);
        }
    };

    // Endpoint B: the whole ruleset, not this entity. Paged, so the count is
    // half the answer -- without it 50 rows read as "these are the rules".
    const showRuleset = () => fetch(`/api/tags/provenance?tag=${encodeURIComponent(tag)}`)
        .then(res => res.json())
        .then(data => {
            const records = (data && data.provenance && data.provenance[tag]) || [];
            const total = (data && data.counts && data.counts[tag]) || records.length;
            const note = !records.length
                ? ''
                : records.length < total
                    ? `Ruleset-wide: showing ${records.length} of ${total} rules that can emit this tag.`
                    : `Ruleset-wide: ${total} rule${total === 1 ? '' : 's'} can emit this tag.`;
            paint(records, note);
        });

    // Endpoint A: the rules that actually fired on this entity. Recorded for
    // the file and for every function a match resolved into; a function whose
    // tag came from an offset that resolved nowhere has no entry and falls
    // through to B, which is the honest answer for it.
    const collection = provEntityCollection(eid);
    const request = (eid && collection)
        ? fetch('/api/tags/match_provenance', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ collection, entity_ids: [eid] }),
          })
              .then(res => res.json())
              .then(data => {
                  const ids = (((data && data.hits) || {})[eid] || {})[tag] || [];
                  if (!ids.length) return showRuleset();
                  const rules = (data && data.rules) || {};
                  const where = eid.includes(':func:') ? 'function' : 'file';
                  paint(
                      ids.map(id => Object.assign({ id }, rules[id] || {})),
                      `Matched here: ${ids.length} rule${ids.length === 1 ? '' : 's'} fired on this ${where}.`,
                  );
              })
        : showRuleset();

    request.catch(() => {
        el.innerHTML = `<div style="color:var(--dim)">Could not load provenance for ${escapeHtml(tag)}.</div>`;
    });
};

// A popup that is *entered* by the pointer cannot close on the badge's
// mouseleave, so closing is always deferred and any re-entry cancels it.
let provCloseTimer = null;
let provOpenTimer = null;
let provOpenFor = '';

window.cancelTagProvenanceClose = () => {
    if (provCloseTimer) { clearTimeout(provCloseTimer); provCloseTimer = null; }
};

window.hideTagProvenance = (delay) => {
    window.cancelTagProvenanceClose();
    const hide = () => {
        const popup = document.getElementById('tag-provenance-popup');
        if (popup) popup.style.display = 'none';
        provOpenFor = '';
    };
    if (!delay) return hide();
    provCloseTimer = setTimeout(hide, delay);
};

const provKey = (badge) => `${badge.textContent.trim()}|${badge.dataset.eid || ''}`;

document.addEventListener('mouseover', (e) => {
    const badge = e.target.closest && e.target.closest('.analysis-tag-badge');
    if (!badge) return;
    window.cancelTagProvenanceClose();
    if (provKey(badge) === provOpenFor) return;
    // Delayed so sweeping the pointer across a row of chips does not fire a
    // request per chip.
    clearTimeout(provOpenTimer);
    provOpenTimer = setTimeout(() => {
        provOpenFor = provKey(badge);
        showTagProvenance(e, badge.textContent.trim(), badge.dataset.eid || '');
    }, 250);
});

document.addEventListener('mouseout', (e) => {
    const badge = e.target.closest && e.target.closest('.analysis-tag-badge');
    if (!badge) return;
    clearTimeout(provOpenTimer);
    window.hideTagProvenance(400);
});

document.addEventListener('click', (e) => {
    const badge = e.target.closest && e.target.closest('.analysis-tag-badge');
    const popup = document.getElementById('tag-provenance-popup');
    if (badge) {
        e.stopPropagation();
        clearTimeout(provOpenTimer);
        window.cancelTagProvenanceClose();
        provOpenFor = provKey(badge);
        showTagProvenance(e, badge.textContent.trim(), badge.dataset.eid || '');
    } else if (popup && !e.target.closest('#tag-provenance-popup')) {
        window.hideTagProvenance(0);
    }
});

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') window.hideTagProvenance(0);
});
