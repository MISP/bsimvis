let tagMetadata = {};

async function fetchTagMetadata(collection) {
    if (!collection) return;
    try {
        const res = await fetch(`/api/tags/metadata?collection=${collection}`);
        if (res.ok) {
            tagMetadata = await res.json();
        }
        // Ensure bookmark and ignore have a default look if not set on server
        if (!tagMetadata['bookmark']) {
            tagMetadata['bookmark'] = { color: '#66d9ef', priority: 1000, count: 0 };
        }
        if (!tagMetadata['ignore']) {
            tagMetadata['ignore'] = { color: '#f92672', priority: 900, count: 0 };
        }
    } catch (err) {
        console.error("Failed to fetch tag metadata", err);
    }
}

function getRowTagColor(analysisTags, userTags = []) {
    // Only use user-defined tags for row coloring as requested
    const allTags = [...(userTags || [])].filter(t => t && t.trim());
    if (localStorage.getItem('sim-color-by-tag') !== 'true' || allTags.length === 0) return "";
    
    let bestColor = null;
    let maxPrio = -1;
    allTags.forEach(t => {
        let meta = tagMetadata[t];
        if (t === 'bookmark') meta = { color: '#66d9ef', priority: 1000 };
        if (t === 'ignore') meta = { color: '#f92672', priority: 900 };
        const color = (meta && meta.color) ? meta.color : '#66d9ef';
        const priority = (meta && meta.priority !== undefined) ? meta.priority : 0;
        
        if (priority >= maxPrio) {
            maxPrio = priority;
            bestColor = color;
        }
    });
    if (bestColor) {
        return `linear-gradient(90deg, ${bestColor}44 0%, transparent 100%)`;
    }
    return "";
}

function refreshAllRowColors() {
    const rows = document.querySelectorAll('tr.sim-row');
    const [hashPath] = (window.location.hash || '#collections').split('?');
    const isColorEnabled = localStorage.getItem('sim-color-by-tag') === 'true';

    rows.forEach(tr => {
        if (!isColorEnabled) {
            tr.style.background = "";
            return;
        }

        // Only collect tags from the PRIMARY editor of the row
        let selector = '.entity-tags-editor[data-etype="function"]';
        if (hashPath === '#function-similarity') {
            selector = '.sim-tags-editor[data-etype="similarity"]';
        } else if (hashPath === '#files') {
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

const renderTagEditor = (etype, eid, tagsList, userTagsList, options = {}) => {
    const isBookmarked = userTagsList.includes('bookmark');
    const isIgnored = userTagsList.includes('ignore');
    const editorClass = etype === 'similarity' ? 'sim-tags-editor' : 'entity-tags-editor';
    const bookmarkOnClick = etype === 'similarity'
        ? `toggleBookmark(event, '${eid}')`
        : `toggleEntityBookmark(event, '${etype}', '${eid}')`;

    const ignoreOnClick = etype === 'similarity'
        ? `toggleIgnore(event, '${eid}')`
        : `toggleEntityIgnore(event, '${etype}', '${eid}')`;

    const addOnClick = `startAddTag(event, '${etype}', '${eid}')`;

    const analysisHtml = tagsList.map(t => `<span class="analysis-tag-badge" title="Analysis Tag: ${t}">${t}</span>`).join('');
    const userHtml = userTagsList.map(t => {
        if (t === 'bookmark' || t === 'ignore') return '';
        const meta = tagMetadata[t] || { color: '#66d9ef' };
        const color = meta.color;
        const removeClick = `removeTag(event, '${etype}', '${eid}', '${t}')`;
        return `
        <span class="sim-tag-card" style="border-color:${color}44; color:${color}; background:${color}11;">
            ${t} 
            <span class="remove-tag-btn" onclick="${removeClick}" style="background:${color}22">×</span>
        </span>`;
    }).join('');

    return `
        <div class="${editorClass}" data-etype="${etype}" data-eid="${eid}" style="display:inline-flex; flex-wrap:wrap; gap:2px; align-items:center; vertical-align:middle;">
            <button class="bookmark-btn ${isBookmarked ? 'active' : ''}" 
                    title="${isBookmarked ? 'Remove Bookmark' : 'Add Bookmark'}"
                    onclick="${bookmarkOnClick}">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path></svg>
            </button>
            <button class="ignore-btn ${isIgnored ? 'active' : ''}" 
                    title="${isIgnored ? 'Remove Ignore' : 'Add Ignore'}"
                    onclick="${ignoreOnClick}">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>
            </button>
            ${analysisHtml}
            ${userHtml}
            <button class="add-tag-btn" onclick="${addOnClick}">+</button>
        </div>
    `;
};

window.applyClusterFilter = (uuid) => {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    const hash = targetWindow.location.hash || '#collections';
    const isSim = hash.startsWith('#function-similarity');
    
    const targetHashPath = isSim ? '#function-similarity' : '#functions';
    const inputId = isSim ? 'flt-sim-cluster' : 'flt-function-cluster';
    
    let input = targetWindow.document.getElementById(inputId);
    if (!input) {
        const currentHash = targetWindow.location.hash || `#functions`;
        const [path, query] = currentHash.split('?');
        const params = new URLSearchParams(query || '');
        params.set('cluster_uuid', uuid);
        
        const currentParams = new URLSearchParams(targetWindow.location.hash.split('?')[1] || '');
        if (currentParams.has('collection')) {
            params.set('collection', currentParams.get('collection'));
        }
        
        targetWindow.location.hash = `${targetHashPath}?${params.toString()}`;
    } else {
        input.value = uuid;
        if (isSim) {
            if (targetWindow.applySimSearch) targetWindow.applySimSearch();
        } else {
            if (targetWindow.applyAdvancedFuncSearch) targetWindow.applyAdvancedFuncSearch();
        }
    }
};

window.showClusterCardTooltip = function(event, uuid, name, size, stability, cohesion, avg_features) {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    if (typeof targetWindow.showClusterTableTooltip === 'function') {
        let adjustedEvent = event;
        if (targetWindow !== window) {
            let iframeId = 'code-frame';
            if (window.location.pathname.includes('/diff/')) iframeId = 'diff-frame';
            const iframe = targetWindow.document.getElementById(iframeId);
            if (iframe) {
                const rect = iframe.getBoundingClientRect();
                adjustedEvent = {
                    clientX: event.clientX + rect.left,
                    clientY: event.clientY + rect.top,
                    target: event.target
                };
            }
        }
        targetWindow.showClusterTableTooltip(adjustedEvent, uuid, name, size, stability, cohesion, avg_features);
    }
};

window.hideClusterCardTooltip = function(event) {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    if (typeof targetWindow.hideClusterTableTooltip === 'function') {
        targetWindow.hideClusterTableTooltip(event);
    }
    const el = targetWindow.document.getElementById('hierarchy-tooltip');
    if (el) el.style.display = 'none';
};

window.moveClusterCardTooltip = function(e) {
    const targetWindow = (window.parent && window.parent !== window) ? window.parent : window;
    const tooltip = targetWindow.document.getElementById('hierarchy-tooltip');
    if (!tooltip || tooltip.style.display !== 'block') return;
    
    if (targetWindow !== window) {
        let iframeId = 'code-frame';
        if (window.location.pathname.includes('/diff/')) iframeId = 'diff-frame';
        const iframe = targetWindow.document.getElementById(iframeId);
        if (iframe) {
            const rect = iframe.getBoundingClientRect();
            const adjustedEvent = {
                clientX: e.clientX + rect.left,
                clientY: e.clientY + rect.top
            };
            if (typeof targetWindow.moveClusterTableTooltip === 'function') {
                targetWindow.moveClusterTableTooltip(adjustedEvent);
            }
            return;
        }
    }
    
    const container = e.target.closest('.cluster-cards-container');
    if (container) {
        const overflow = container.querySelector('.cluster-overflow-box');
        const isOverflowVisible = overflow && window.getComputedStyle(overflow).display !== 'none';
        const boxRect = (isOverflowVisible && overflow) ? overflow.getBoundingClientRect() : container.getBoundingClientRect();
        const tooltipRect = tooltip.getBoundingClientRect();
        
        let x = boxRect.right + 15;
        let y = boxRect.top;
        
        if (x + tooltipRect.width > window.innerWidth) {
            x = boxRect.left - tooltipRect.width - 15;
        }
        if (y + tooltipRect.height > window.innerHeight) {
            y = Math.max(10, window.innerHeight - tooltipRect.height - 15);
        }
        
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }
};

window.renderClusterCards = (clusters) => {
    if (!clusters || clusters.length === 0) return '';
    
    const validClusters = clusters.filter(c => (c.cohesion_score || 0) >= 0.50);
    if (validClusters.length === 0) return '';
    
    const sorted = [...validClusters].sort((a, b) => (b.cohesion_score || 0) - (a.cohesion_score || 0));
    const renderCard = (c, isHidden = false) => {
        const name = c.cluster_name || `Cluster ${c.cluster_id}`;
        const score = (c.cohesion_score || 0).toFixed(2);
        const uuid = c.cluster_uuid;
        const hue = Math.max(0, Math.min(120, (c.cohesion_score || 0) * 120));
        const color = `hsl(${hue}, 100%, 65%)`;
        
        const cardClass = isHidden ? 'tag-card cluster-card cluster-hidden' : 'tag-card cluster-card';
        
        return `
        <span class="${cardClass}"
              onmouseenter="showClusterCardTooltip(event, '${uuid}', '${name.replace(/'/g, "\\'")}', ${c.member_count || 0}, ${c.cluster_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})"
              onmouseleave="hideClusterCardTooltip(event)"
              onmousemove="moveClusterCardTooltip(event)"
              onclick="applyClusterFilter('${uuid}')"
              style="border-color:${color}44; color:${color}; background:${color}11; align-items:center; gap:4px; padding:2px 6px 2px 8px; font-size:0.65rem; border-radius:12px; margin:2px; cursor:pointer;">
            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" 
                 stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"></circle>
                <circle cx="12" cy="12" r="4"></circle>
            </svg>
            <span style="max-width:80px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${name}</span>
            <span style="opacity:0.8; font-family:monospace; font-size:0.65rem;">${c.member_count || 0}</span>
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
            <div style="font-size:0.6rem; color:#888; margin-bottom:4px; text-transform:uppercase; letter-spacing:1px; padding:0 4px;">Clusters</div>
            ${allHtml}
        </div>`;

    return `<div class="cluster-cards-container" style="position:relative; display:inline-flex; align-items:center; padding:6px; margin:-6px; cursor:default;">
        ${renderCard(sorted[0])}${moreHtml}${overflowBox}
    </div>`;
};

function attachTagAutocomplete(input, onSelect) {
    if (input._acAttached) return;
    input._acAttached = true;

    const parent = input.parentElement;
    const dropdown = document.createElement('div');
    dropdown.className = 'tag-autocomplete-dropdown';
    parent.appendChild(dropdown);

    const renderSuggestions = (tags) => {
        dropdown.innerHTML = '';
        tags.forEach(t => {
            const item = document.createElement('div');
            item.className = 'tag-suggestion-item';
            const meta = tagMetadata[t] || { color: '#66d9ef', count: 0, priority: 0 };
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
        dropdown.style.display = 'block';
    };

    const showSuggestions = (filter = '') => {
        const query = filter.toLowerCase().trim();
        const tags = Object.keys(tagMetadata).filter(t => t.toLowerCase().includes(query));
        if (tags.length > 0) {
            renderSuggestions(tags);
        } else {
            dropdown.style.display = 'none';
        }
    };

    input.onfocus = () => showSuggestions(input.value);
    input.oninput = () => showSuggestions(input.value);
    input.onblur = () => {
        setTimeout(() => { dropdown.style.display = 'none'; }, 200);
    };


    if (document.activeElement === input) showSuggestions(input.value);
}

function attachAutocomplete(input, level, field, onSelect) {
    if (input._acAttached) return;
    input._acAttached = true;

    const parent = input.parentElement;
    const originalPosition = window.getComputedStyle(parent).position;
    if (originalPosition === 'static') parent.style.position = 'relative';

    const dropdown = document.createElement('div');
    dropdown.className = 'tag-autocomplete-dropdown';
    parent.appendChild(dropdown);


    const renderSuggestions = (items) => {
        dropdown.innerHTML = '';
        items.forEach(item => {
            const div = document.createElement('div');
            div.className = 'tag-suggestion-item';
            div.style.display = 'flex';
            div.style.justifyContent = 'space-between';
            div.innerHTML = `<span>${item.value}</span> <span class="dim" style="font-size:0.6rem; margin-left:10px;">${item.count}</span>`;
            div.onmousedown = (e) => {
                e.preventDefault();
                onSelect(item.value);
                dropdown.style.display = 'none';
            };
            dropdown.appendChild(div);
        });
        dropdown.style.display = 'block';
    };


    const showSuggestions = async (filter = '') => {
        const hashParts = window.location.hash.split('?');
        const params = new URLSearchParams(hashParts[1] || "");
        const col = params.get('collection') || 'main';

        try {
            const res = await fetch(`/api/search/autocomplete?collection=${col}&level=${level}&field=${field}&q=${encodeURIComponent(filter)}&limit=50`);
            if (res.ok) {
                const data = await res.json();
                const items = data.results || [];
                if (items.length > 0) {
                    renderSuggestions(items);
                } else {
                    dropdown.style.display = 'none';
                }
            }
        } catch (err) {
            console.error("Autocomplete fetch failed", err);
        }
    };

    input.onfocus = () => showSuggestions(input.value);
    input.oninput = () => showSuggestions(input.value);
    input.onblur = () => {
        setTimeout(() => { dropdown.style.display = 'none'; }, 200);
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
    // If not in query, try parsing from eid (e.g. main:function:md5:addr -> main)
    const colParts = eid ? eid.split(':') : [];
    const col = colStr || (colParts.length > 2 ? colParts[0] : 'main');

    let targets = [{ etype, eid, container }];
    if (etype === 'similarity' && typeof selectedSimilarityPairs !== 'undefined' && selectedSimilarityPairs.has(eid)) {
        targets = Array.from(selectedSimilarityPairs).map(pid => {
            const info = typeof getSimilarityRowInfo === 'function' ? getSimilarityRowInfo(pid) : null;
            return info ? { etype: 'similarity', eid: pid, container: info.container } : { etype: 'similarity', eid: pid, container: null };
        });
    }

    let uiTargets = [container];
    if (etype === 'function' || etype === 'file') {
        const allEditors = document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`);
        if (allEditors.length > 0) uiTargets = Array.from(allEditors);
    } else if (etype === 'similarity' && typeof selectedSimilarityPairs !== 'undefined' && selectedSimilarityPairs.has(eid)) {
        uiTargets = targets.map(t => t.container).filter(c => !!c);
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

    // Broadcast updates to dashboard parent if inside an iframe
    if (window.parent && window.parent !== window && typeof window.parent.updateUIForTagAdd === 'function') {
        targets.forEach(t => {
            let parentEditors = Array.from(window.parent.document.querySelectorAll(`[data-etype="${t.etype}"][data-eid="${t.eid}"]`));

            // Fallback for similarities because dashboard might use canonical sid while diff view uses id1|id2|algo
            if (parentEditors.length === 0 && t.etype === 'similarity') {
                const parts = t.eid.split('|');
                if (parts.length >= 2) {
                    const id1 = parts[0];
                    const id2 = parts[1];
                    const algoPart = parts.length > 2 ? `[data-algo="${parts[2]}"]` : '';
                    const row = window.parent.document.querySelector(`tr[data-id1="${id1}"][data-id2="${id2}"]${algoPart}`);
                    if (row) {
                        const ed = row.querySelector('[data-etype="similarity"]');
                        if (ed) parentEditors.push(ed);
                    }
                }
            }

            if (parentEditors.length > 0) window.parent.updateUIForTagAdd(parentEditors, tag);
        });
        if (typeof window.parent.refreshAllRowColors === 'function') {
            window.parent.refreshAllRowColors();
        }
    }

    return mainSuccess;
}

function updateUIForTagAdd(editors, tag) {
    const meta = tagMetadata[tag] || { color: '#66d9ef' };
    const color = meta.color;
    const isBookmark = (tag === 'bookmark');
    const isIgnore = (tag === 'ignore');

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
                card.style.borderColor = color + '44';
                card.style.color = color;
                card.style.background = color + '11';
                const removeClick = `removeTag(event, '${editor.dataset.etype}', '${editor.dataset.eid}', '${tag}')`;
                card.innerHTML = `${tag} <span class="remove-tag-btn" onclick="${removeClick}" style="background:${color}22">×</span>`;
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
    // If not in query, try parsing from eid (e.g. main:function:md5:addr -> main)
    const colParts = eid ? eid.split(':') : [];
    const col = colStr || (colParts.length > 2 ? colParts[0] : 'main');

    let targets = [{ etype, eid }];
    if (etype === 'similarity' && typeof selectedSimilarityPairs !== 'undefined' && selectedSimilarityPairs.has(eid)) {
        targets = Array.from(selectedSimilarityPairs).map(pid => {
            const info = typeof getSimilarityRowInfo === 'function' ? getSimilarityRowInfo(pid) : null;
            return info ? { etype: 'similarity', eid: pid, container: info.container } : { etype: 'similarity', eid: pid, container: null };
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

    // Broadcast updates to dashboard parent if inside an iframe
    if (window.parent && window.parent !== window && typeof window.parent.updateUIForTagRemove === 'function') {
        targets.forEach(t => {
            let parentEditors = Array.from(window.parent.document.querySelectorAll(`[data-etype="${t.etype}"][data-eid="${t.eid}"]`));

            if (parentEditors.length === 0 && t.etype === 'similarity') {
                const parts = t.eid.split('|');
                if (parts.length >= 2) {
                    const id1 = parts[0];
                    const id2 = parts[1];
                    const algoPart = parts.length > 2 ? `[data-algo="${parts[2]}"]` : '';
                    const row = window.parent.document.querySelector(`tr[data-id1="${id1}"][data-id2="${id2}"]${algoPart}`);
                    if (row) {
                        const ed = row.querySelector('[data-etype="similarity"]');
                        if (ed) parentEditors.push(ed);
                    }
                }
            }

            if (parentEditors.length > 0) window.parent.updateUIForTagRemove(parentEditors, tag);
        });
        if (typeof window.parent.refreshAllRowColors === 'function') {
            window.parent.refreshAllRowColors();
        }
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
        const res = await fetch(`/api/search/fields?collection=${col}&level=${level}&${query}`);
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
