// Main Dashboard Controller for BSimVis

let filterDebounceTimer = null;
function debouncedSearch(searchFn) {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    filterDebounceTimer = setTimeout(() => {
        searchFn();
    }, 350);
}

function handleFilterKey(e, searchFn) {
    if (e.key === 'Enter') {
        e.preventDefault();
        if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
        searchFn();
    }
}

let currentOffset = 0;
const DEFAULT_PAGE_LIMIT = 50;
const DEFAULT_GRAPH_LIMIT = 500;
const PAGE_SIZE = DEFAULT_PAGE_LIMIT;
let isEndOfResults = false;
let lastHashPath = '';
let selectedSimilarityPairs = new Set(); // Global selection state: Set of "id1|id2|algo"
let lastSimilarityQuery = null; // Track filters to detect view switching
let simSearchRequested = false; // Set to true when user explicitly triggers a search

function toggleSidebar() {
    const body = document.body;
    const isCollapsed = body.classList.toggle('sidebar-collapsed');
    localStorage.setItem('sidebarCollapsed', isCollapsed);
    const btn = document.getElementById('sidebar-toggle');
    btn.innerHTML = isCollapsed ? '⟩' : '⟨';

    // Trigger window resize for Bokeh/D3 plots
    setTimeout(() => window.dispatchEvent(new Event('resize')), 300);
}

function toggleHeader() {
    const body = document.body;
    const isCollapsed = body.classList.toggle('header-collapsed');
    localStorage.setItem('headerCollapsed', isCollapsed);

    const btn = document.getElementById('collapse-header-btn');
    if (btn) {
        if (isCollapsed) btn.classList.remove('active');
        else btn.classList.add('active');
    }



    // Trigger window resize for plots
    setTimeout(() => window.dispatchEvent(new Event('resize')), 400);
}

function toggleFilters() {
    const body = document.body;
    const isCollapsed = body.classList.toggle('filters-collapsed');
    localStorage.setItem('filtersCollapsed', isCollapsed);
    const btn = document.getElementById('toggle-filters-btn');
    if (btn) {
        if (isCollapsed) btn.classList.remove('active');
        else btn.classList.add('active');
    }
    setTimeout(() => window.dispatchEvent(new Event('resize')), 200);
}

function getCollectionFromHash() {
    const [hashPath, queryString] = (window.location.hash || '').split('?');
    const params = new URLSearchParams(queryString);
    return params.get('collection') || 'main';
}

function toggleSimilaritySelection(event, id1, id2, algo) {
    const pairId = event.target.dataset.pairId || `${id1}|${id2}|${algo}`;
    if (event.target.checked) {
        selectedSimilarityPairs.add(pairId);
    } else {
        selectedSimilarityPairs.delete(pairId);
        const master = document.getElementById('select-all-sim');
        if (master) master.checked = false;
    }
}

function toggleAllSimilaritySelection(master) {
    const checkboxes = document.querySelectorAll('.row-selector');
    checkboxes.forEach(cb => {
        cb.checked = master.checked;
        const pairId = cb.dataset.pairId;
        if (master.checked) {
            selectedSimilarityPairs.add(pairId);
        } else {
            selectedSimilarityPairs.delete(pairId);
        }
    });
}

function getSimilarityRowInfo(pairId) {
    const row = document.querySelector(`input[data-pair-id="${pairId}"]`)?.closest('tr');
    if (!row) return null;
    return {
        container: row.querySelector('.sim-tags-editor'),
        id1: row.dataset.id1,
        id2: row.dataset.id2,
        algo: row.dataset.algo
    };
}

const routes = {
    '#collections': {
        title: 'Collections',
        api: '/api/collection/search',
        headers: ['Name', 'Files', 'Functions', 'Last Updated', 'Actions'],
        renderer: renderCollections
    },
    '#batches': {
        title: 'Batches',
        api: '/api/batch/search',
        headers: ['Batch Name', 'UUID', 'Files', 'Functions', 'Timestamp', 'Actions'],
        renderer: renderBatches
    },
    '#files': {
        title: 'Files',
        api: '/api/file/search',
        headers: ['Filename', 'MD5 / Arch', 'Entry Date', 'Actions'],
        renderer: renderFiles
    },
    '#functions': {
        title: 'Functions',
        api: '/api/function/search',
        headers: [
            { label: 'Function', width: '20%' },
            { label: 'Address', width: '8%', sort: 'entrypoint_address' },
            { label: 'Function Tags', width: '12%' },
            { label: 'Clusters', width: '10%' },
            { label: 'Feat', width: '5%', sort: 'bsim_features_count' },
            { label: 'File Name', width: '10%', sort: 'file_name' },
            { label: 'MD5', width: '5%', sort: 'file_md5' },
            { label: 'File Tags', width: '12%' },
            { label: 'Language', width: '5%', sort: 'language_id' },
            { label: 'Date', width: '8%', sort: 'entry_date' },
            { label: 'Actions', width: '5%' }
        ],
        renderer: renderFunctions
    },
    '#features-global': {
        title: 'Global Feature Index',
        api: '/api/feature/search',
        headers: ['Feature Hash', 'Type / Op', 'PCode Context', 'C Code Context', 'Total TF', 'Funcs', 'Actions'],
        renderer: renderGlobalFeatures
    },
    '#function-similarity': {
        title: 'Function similarities',
        api: '/api/similarity/search',
        headers: [
            { label: 'Similarity', sort: 'score', width: '8%' },
            { label: 'Function Pair', width: '18%' },
            { label: 'Address', width: '5%' },
            { label: 'Function Tags', width: '10%' },
            { label: 'Clusters', width: '10%' },
            { label: 'Feat', sort: 'feat_count', width: '5%' },
            { label: 'File Name', width: '9%' },
            { label: 'MD5', width: '5%' },
            { label: 'File Tags', width: '10%' },
            { label: 'Language', width: '5%' },
            { label: 'Date', sort: 'entry_date', width: '15%' }
        ],
        renderer: renderTopCorrelations
    },
    '#clusters': {
        title: 'Function Clusters',
        api: '/api/cluster/list',
        headers: [
            { label: 'UUID', sort: 'cluster_uuid', width: '10%' },
            { label: 'Name', sort: 'cluster_name', width: '25%' },
            { label: 'Functions', sort: 'count', width: '8%' },
            { label: 'Stability', sort: 'stability', width: '8%' },
            { label: 'Avg Feat', sort: 'features', width: '8%' },
            { label: 'Cohesion', sort: 'cohesion', width: '8%' },
            { label: 'Created', width: '15%' },
            { label: 'Actions', width: '18%' }
        ],
        renderer: renderClusters
    },
    '#file-call-graph': {
        title: 'File Call Graph',
        api: null,
        headers: [],
        renderer: null
    }
};

function clearFilters() {
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);
    const newParams = new URLSearchParams();

    // Preserved context keys
    const preserved = ['collection', 'batch_uuid', 'file_md5', 'algo', 'view', 'cluster_id', 'cluster_uuid'];
    preserved.forEach(k => {
        if (params.has(k)) newParams.set(k, params.get(k));
    });

    currentOffset = 0;
    isEndOfResults = false;
    const newHash = hashPath + (newParams.toString() ? '?' + newParams.toString() : '');
    window.location.hash = newHash;
    // Re-apply sim view defaults when clearing within the sim view
    if (hashPath === '#function-similarity') {
        const [hp, qs] = (window.location.hash || '').split('?');
        applySimViewDefaults(hp, qs);
    }
}

async function refreshData(appendArg = false, force = false) {
    const append = (appendArg === true);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const full_hash = hashPath + (queryString ? '?' + queryString : '');
    const route = routes[hashPath];
    if (!route) return;

    if (full_hash !== lastHashPath || !append) {
        currentOffset = 0;
        isEndOfResults = false;
        document.getElementById('table-body').innerHTML = '';
        document.getElementById('loader').style.display = 'block';
    }
    lastHashPath = full_hash;

    const params = new URLSearchParams(queryString);
    const collection = params.get('collection') || 'main';

    // Ensure tag metadata is loaded for views that use it (functions and similarities)
    if (hashPath === '#functions' || hashPath === '#function-similarity') {
        await fetchTagMetadata(collection);
    }

    if (hashPath === '#function-similarity') {
        // Caching strategy: Use cache ONLY for 'Load More' (append) or 'Switch View' (same query)
        const queryParams = new URLSearchParams(queryString);
        queryParams.delete('offset');
        queryParams.delete('limit');
        queryParams.delete('view');
        queryParams.delete('use_cache');
        const currentQuery = queryParams.toString();

        if (!force && (append || (lastSimilarityQuery !== null && currentQuery === lastSimilarityQuery))) {
            params.set('use_cache', 'true');
        } else {
            params.set('use_cache', 'false');
        }
        lastSimilarityQuery = currentQuery;
    }

    // Standard pagination for all search routes
    const countLimit = params.get('limit') || (params.get('view') === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT);
    params.set('offset', currentOffset);
    params.set('limit', countLimit);

    let apiUrl = route.api + (params.toString() ? '?' + params.toString() : '');
    updateUI(hashPath, params, route);

    if (params.get('view') === 'graph' && hashPath === '#function-similarity' || !route.api) {
        document.getElementById('loader').style.display = 'none';
        return;
    }

    try {
        const response = await fetch(apiUrl);
        const data = await response.json();

        // Extract the list of items based on the API response structure
        const items = data.items || data.results || data.files || data.functions || data.features || data.pairs || data.collections || data.batches || (Array.isArray(data) ? data : []);
        const total = data.total !== undefined ? data.total : (data.total_estimated !== undefined ? data.total_estimated : (Array.isArray(data) ? data.length : (items.length || 0)));

        const totalEl = document.getElementById('view-total');
        const poolIcon = document.getElementById('pool-warn-icon');
        const limitIcon = document.getElementById('limit-warn-icon');
        const poolInput = document.getElementById('sim-pool-limit');
        const limitInput = document.getElementById('sim-limit');

        if (totalEl) {
            totalEl.style.display = 'inline-block';

            if (poolIcon) {
                if (data.pool_truncated) {
                    document.getElementById('pool-warn-icon').style.display = 'inline-block';
                    document.getElementById('pool-warn-icon').title = `⚠️ Pool Truncated to first ${data.pool_limit || '---'} matches. Results may be incomplete.`;
                    if (poolInput) poolInput.style.borderColor = '#ffab2e';
                } else {
                    poolIcon.style.display = 'none';
                    if (poolInput) poolInput.style.borderColor = 'var(--accent)';
                }
            }

            if (limitIcon) {
                const currentLimit = parseInt(params.get('limit')) || DEFAULT_PAGE_LIMIT;
                if (total >= currentLimit && hashPath === '#function-similarity') {
                    limitIcon.style.display = 'inline-block';
                    limitIcon.title = `ℹ️ Result Limit Reached (${currentLimit.toLocaleString()}). Not all pairs are shown.`;
                    if (limitInput) limitInput.style.borderColor = '#60a5fa';
                } else {
                    limitIcon.style.display = 'none';
                    if (limitInput) limitInput.style.borderColor = 'var(--accent)';
                }
            }
        }

        const tbody = document.getElementById('table-body');
        if (items.length === 0 && !append) {
            tbody.innerHTML = '<tr><td colspan="100" style="text-align:center; padding:40px;">No data found</td></tr>';
        } else {
            tbody.insertAdjacentHTML('beforeend', route.renderer(items));
        }

        currentOffset += (items.length || 0);
        isEndOfResults = currentOffset >= total;

        // Update total display with "Shown / Total" format
        if (totalEl) {
            totalEl.style.display = 'inline-block';
            totalEl.innerText = `${currentOffset.toLocaleString()} / ${total.toLocaleString()}`;
        }

        renderPagination(hashPath);
    } catch (err) {
        console.error(err);
    } finally {
        document.getElementById('loader').style.display = 'none';
    }
}

function updateUI(path, params, route) {
    // Reset all special view containers and stop active processes
    document.getElementById('graph-view-container').style.display = 'none';
    document.getElementById('hierarchy-view-container').style.display = 'none';
    if (document.getElementById('packing-view-container')) document.getElementById('packing-view-container').style.display = 'none';
    if (document.getElementById('call-graph-view-container')) document.getElementById('call-graph-view-container').style.display = 'none';
    document.getElementById('table-body').style.display = 'table-row-group';
    document.getElementById('pagination-container').style.display = 'block';

    if (window.graphInstance) window.graphInstance.stop();
    if (window.hierarchyInstance) window.hierarchyInstance.stop();
    if (window.packingInstance) window.packingInstance.stop();
    if (window.callGraphInstance) window.callGraphInstance.stop();

    const col = params.get('collection');

    // Sidebar
    document.querySelectorAll('nav a').forEach(a => a.classList.remove('active'));
    const navLink = document.getElementById('nav-' + path.substring(1));

    // Clear selection if navigating away from similarity view
    if (path !== lastHashPath.split('?')[0] && path !== '#function-similarity') {
        selectedSimilarityPairs.clear();
    }

    if (navLink) navLink.classList.add('active');

    // Titles
    document.getElementById('view-title').innerText = route.title;
    const badgeEl = document.getElementById('view-collection-badge');
    if (badgeEl) {
        if (col) {
            badgeEl.style.display = 'inline-block';
            badgeEl.innerText = `Collection: ${col}`;
        } else {
            badgeEl.style.display = 'none';
        }
    }

    // Side Collections Info
    const sideSelect = document.getElementById('side-collection-select');
    if (sideSelect) sideSelect.value = col || '';
    updateNavVisibility(col);

    if (col) {
        document.getElementById('nav-batches').href = `#batches?collection=${col}`;
        document.getElementById('nav-files').href = `#files?collection=${col}`;
        document.getElementById('nav-functions').href = `#functions?collection=${col}`;
        document.getElementById('nav-features-global').href = `#features-global?collection=${col}`;
        document.getElementById('nav-function-similarity').href = `#function-similarity?collection=${col}`;
        document.getElementById('nav-clusters').href = `#clusters?collection=${col}`;
        
        const fileMd5 = params.get('file_md5');
        const cgNav = document.getElementById('nav-file-call-graph');
        if (cgNav) {
            let href = `#file-call-graph?collection=${col}`;
            if (fileMd5) href += `&file_md5=${fileMd5}`;
            cgNav.href = href;
        }
    }

    // Table Head
    const thead = document.getElementById('table-head');
    const dataTable = document.getElementById('data-table');
    let headHtml = '<tr>';
    if (path === '#function-similarity') {
        headHtml += `<th style="width:30px; text-align:center;"><input type="checkbox" id="select-all-sim" onchange="toggleAllSimilaritySelection(this)"></th>`;
        if (dataTable) dataTable.style.tableLayout = 'fixed';
    } else {
        if (dataTable) dataTable.style.tableLayout = 'auto';
    }
    route.headers.forEach(h => {
        const label = typeof h === 'string' ? h : h.label;
        const sortKey = typeof h === 'object' ? h.sort : null;
        const width = typeof h === 'object' ? h.width : 'auto';

        let style = width !== 'auto' ? `style="width:${width}"` : '';
        if (sortKey) {
            const currentSort = params.get('sort_by');
            const currentOrder = params.get('sort_order') || 'desc';
            const icon = (currentSort === sortKey) ? (currentOrder === 'desc' ? '▼' : '▲') : '↕';
            headHtml += `<th ${style} class="sortable" onclick="toggleSort('${sortKey}')">${label} <small>${icon}</small></th>`;
        } else {
            headHtml += `<th ${style}>${label}</th>`;
        }
    });
    headHtml += '</tr>';
    thead.innerHTML = headHtml;

    // Reset UI settings and display containers to defaults for all views
    const settingsEl = document.getElementById('search-settings-container');
    settingsEl.style.display = 'none';
    settingsEl.innerHTML = '';
    document.getElementById('table-body').style.display = 'table-row-group';
    document.getElementById('pagination-container').style.display = 'block';
    document.getElementById('graph-view-container').style.display = 'none';
    document.getElementById('hierarchy-view-container').style.display = 'none';
    if (document.getElementById('packing-view-container')) document.getElementById('packing-view-container').style.display = 'none';

    if (path === '#function-similarity') {
        settingsEl.style.display = 'flex';
        const viewMode = params.get('view') || 'table';
        const currentLimit = params.get('pool_limit') || '1000000';
        const countLimit = params.get('limit') || (viewMode === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT);

        settingsEl.innerHTML = `
            <div class="view-toggle">
                <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchSimView('table')">Table</button>
                <button class="view-btn ${viewMode === 'graph' ? 'active' : ''}" onclick="switchSimView('graph')">Graph</button>
            </div>
            <span class="dim" style="font-size:0.65rem; margin-left:15px;">Pool Limit:</span>
            <div style="position:relative; display:inline-flex; align-items:center;">
                <input type="number" id="sim-pool-limit" value="${currentLimit}" step="100000" min="1000" max="1000000" 
                    title="Max candidates to score" 
                    style="width:70px; background:rgba(0,0,0,0.3); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                    onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                <span id="pool-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Pool Truncated: Not all candidates were scored.">⚠️</span>
            </div>
            <span class="dim" style="font-size:0.65rem; margin-left:15px;">Limit:</span>
            <div style="position:relative; display:inline-flex; align-items:center;">
                <input type="number" id="sim-limit" value="${countLimit}" step="10" min="1" max="50000" 
                    title="Max results to display (Output Limit)" 
                    style="width:60px; background:rgba(0,0,0,0.3); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                    onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                <span id="limit-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Output Limit Reached: Results are capped.">ℹ️</span>
            </div>
        `;

        const tbody = document.getElementById('table-body');
        const pag = document.getElementById('pagination-container');
        const gview = document.getElementById('graph-view-container');

        if (viewMode === 'graph') {
            tbody.style.display = 'none';
            pag.style.display = 'none';
            gview.style.display = 'flex';
            console.log("updateUI: Loading Graph...");
            loadGraphView(params);
        } else {
            tbody.style.display = 'table-row-group';
            pag.style.display = 'block';
            gview.style.display = 'none';
        }

        const p = new URLSearchParams(params);
        headHtml += `<tr class="filter-row">
            <th></th>
            <th style="vertical-align: middle;">
                <div style="display:flex; align-items:center; gap:2px;">
                    <input type="number" id="sim-min-score" value="${p.get('min_score') || '0.95'}" step="0.05" min="0" max="1" title="Min Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                    <span class="dim" style="font-size:0.6rem">-</span>
                    <input type="number" id="sim-max-score" value="${p.get('max_score') || '1.0'}" step="0.05" min="0" max="1" title="Max Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                </div>
                <div class="tag-filter-container" id="tag-container-sim">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'sim')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('sim', 'sim_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
            <th>
                <div style="display:flex; flex-direction:column; gap:4px;">
                    <input type="text" id="flt-sim-name" placeholder="Name..." value="${p.get('name') || ''}" onfocus="attachAutocomplete(this, 'func', 'function_name', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    <div style="display:flex; gap:2px;">
                        <input type="text" id="flt-sim-namespace" placeholder="Namespace..." value="${p.get('namespace') || ''}" onfocus="attachAutocomplete(this, 'func', 'namespace', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                        <input type="text" id="flt-sim-ret_type" placeholder="Return Type..." value="${p.get('ret_type') || ''}" onfocus="attachAutocomplete(this, 'func', 'return_type', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                    </div>
                </div>
            </th>
            <th>
                <input type="text" id="flt-sim-address" placeholder="Addr..." value="${p.get('address') || ''}" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
            </th>
            <th style="position:relative">
                <div class="tag-filter-container" id="tag-container-func">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'func')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('func', 'func_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-sim-cluster" placeholder="UUID..." value="${p.get('cluster_uuid') || ''}" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                    <input type="text" id="flt-sim-cluster-name" placeholder="Name..." value="${p.get('cluster_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'cluster_name', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                </div>
            </th>
            <th><input type="number" id="sim-min-features" value="${p.get('min_features') || '0'}" min="0" title="Min Features" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th><input type="text" id="flt-sim-file_name" placeholder="Name..." value="${p.get('file_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_name', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="width: 100%; box-sizing: border-box;"></th>
            <th><input type="text" id="flt-sim-md5" placeholder="MD5..." value="${p.get('md5') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_md5', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="width: 100%; box-sizing: border-box;"></th>
            <th style="position:relative">
                <div class="tag-filter-container" id="tag-container-file">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
            <th><input type="text" id="flt-sim-language" placeholder="Lang..." value="${p.get('language') || ''}" onfocus="attachAutocomplete(this, 'func', 'language_id', (val) => { this.value = val; applySimSearch(); })" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
            <th style="font-size: 0.65rem;">
                <select id="sim-algo" onchange="applySimSearch()" style="width:100%; background:#000; color:var(--text); border:1px solid #333; font-size:0.65rem; border-radius:2px;">
                    <option value="unweighted_cosine" ${p.get('algo') === 'unweighted_cosine' ? 'selected' : ''}>Cosine</option>
                    <option value="jaccard" ${p.get('algo') === 'jaccard' ? 'selected' : ''}>Jaccard</option>
                    <option value="milvus_sparse" ${p.get('algo') === 'milvus_sparse' ? 'selected' : ''}>Milvus Sparse</option>
                </select>
                <div style="margin-top:4px;">
                    <select id="sim-cross-binary" onchange="applySimSearch()" style="width:100%; background:#000; color:var(--text); border:1px solid #333; font-size:0.6rem; border-radius:2px;">
                        <option value="" ${!p.get('cross_binary') ? 'selected' : ''}>All Binaries</option>
                        <option value="false" ${p.get('cross_binary') === 'false' ? 'selected' : ''}>Same Binary Only</option>
                        <option value="true" ${p.get('cross_binary') === 'true' ? 'selected' : ''}>Cross Binary Only</option>
                    </select>
                </div>
            </th>
        </tr>`;
        thead.innerHTML = headHtml;

        // --- Inject Tag Cards from URL Params ---
        const tagFields = [
            { key: 'sim', fields: ['sim_tag', 'sim_static_tag', 'sim_user_tag', 'exclude_sim_tag', 'exclude_sim_static_tag', 'exclude_sim_user_tag'] },
            { key: 'func', fields: ['func_tag', 'func_static_tag', 'func_user_tag', 'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] },
            { key: 'file', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'] }
        ];

        tagFields.forEach(col => {
            col.fields.forEach(f => {
                const values = p.getAll(f);
                const isEx = f.startsWith('exclude_');
                const baseType = isEx ? f.substring(8) : f;
                values.forEach(v => {
                    if (v) createTagCard(col.key, baseType, v, isEx);
                });
            });
        });
        loadFieldCardinalities(col, 'func', {
            'function_name': 'flt-sim-name',
            'namespace': 'flt-sim-namespace',
            'return_type': 'flt-sim-ret_type',
            'file_md5': 'flt-sim-md5',
            'file_name': 'flt-sim-file_name',
            'language_id': 'flt-sim-language'
        });
    } else if (path === '#functions') {
        const p = new URLSearchParams(params);
        if (dataTable) dataTable.style.tableLayout = 'fixed';
        headHtml += `<tr class="filter-row">
            <th>
                <div style="display:flex; flex-direction:column; gap:4px;">
                    <input type="text" id="flt-function-name" placeholder="Name..." value="${p.get('function_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'function_name', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    <div style="display:flex; gap:2px;">
                        <input type="text" id="flt-function-namespace" placeholder="Namespace..." value="${p.get('namespace') || ''}" onfocus="attachAutocomplete(this, 'func', 'namespace', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                        <input type="text" id="flt-function-ret_type" placeholder="Return Type..." value="${p.get('return_type') || ''}" onfocus="attachAutocomplete(this, 'func', 'return_type', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                    </div>
                </div>
            </th>
            <th>
                <input type="text" id="flt-function-address" placeholder="Addr..." value="${p.get('entrypoint_address') || ''}" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
            </th>
            <th style="position:relative">
                <div class="tag-filter-container" id="tag-container-func">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'func')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('func', 'func_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-function-cluster" placeholder="UUID..." value="${p.get('cluster_uuid') || ''}" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                    <input type="text" id="flt-function-cluster-name" placeholder="Name..." value="${p.get('cluster_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'cluster_name', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                </div>
            </th>
            <th><input type="number" id="flt-function-min_features" value="${p.get('min_features') || '0'}" min="0" title="Min Features" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th><input type="text" id="flt-function-file_name" placeholder="Name..." value="${p.get('file_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_name', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="width: 100%; box-sizing: border-box;"></th>
            <th><input type="text" id="flt-function-md5" placeholder="MD5..." value="${p.get('file_md5') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_md5', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="width: 100%; box-sizing: border-box;"></th>
            <th style="position:relative">
                <div class="tag-filter-container" id="tag-container-file">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
            <th><input type="text" id="flt-function-language" placeholder="Lang..." value="${p.get('language_id') || ''}" onfocus="attachAutocomplete(this, 'func', 'language_id', (val) => { this.value = val; applyAdvancedFuncSearch(); })" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
            <th></th>
            <th></th>
        </tr>`;
        thead.innerHTML = headHtml;

        // Re-inject tags
        const tagFields = [
            { key: 'func', fields: ['func_tag', 'func_static_tag', 'func_user_tag', 'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] },
            { key: 'file', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'] }
        ];
        tagFields.forEach(col => {
            col.fields.forEach(f => {
                const values = p.getAll(f);
                const isEx = f.startsWith('exclude_');
                const baseType = isEx ? f.substring(8) : f;
                values.forEach(v => {
                    if (v) createTagCard(col.key, baseType, v, isEx);
                });
            });
        });
        loadFieldCardinalities(col, 'func', {
            'function_name': 'flt-function-name',
            'file_name': 'flt-function-file_name',
            'file_md5': 'flt-function-md5',
            'return_type': 'flt-function-ret_type',
            'language_id': 'flt-function-language',
            'namespace': 'flt-function-namespace'
        });
    } else if (path === '#clusters') {
        const p = new URLSearchParams(params);
        if (dataTable) dataTable.style.tableLayout = 'fixed';
        settingsEl.style.display = 'flex';
        const viewMode = params.get('view') || 'table';
        settingsEl.innerHTML = `
            <div class="view-toggle">
                <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchClusterView('table')">Table</button>
                <button class="view-btn ${viewMode === 'hierarchy' ? 'active' : ''}" onclick="switchClusterView('hierarchy')">Graph</button>
                <button class="view-btn ${viewMode === 'packing' ? 'active' : ''}" onclick="switchClusterView('packing')">Packing</button>
            </div>
        `;
        headHtml += `<tr class="filter-row">
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-cluster-uuid" placeholder="UUID..." value="${p.get('cluster_uuid') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    <input type="text" id="flt-cluster-id" placeholder="ID..." value="${p.get('cluster_id') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                </div>
            </th>
            <th>
                <input type="text" id="flt-cluster-name" placeholder="Name..." value="${p.get('cluster_name') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
            </th>
            <th><input type="number" id="flt-cluster-min-count" value="${p.get('min_count') || '0'}" min="0" title="Min Functions" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th><input type="number" id="flt-cluster-min-stability" value="${p.get('min_stability') || '0'}" step="0.1" min="0" max="1" title="Min Stability" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th><input type="number" id="flt-cluster-min-features" value="${p.get('min_features') || '0'}" min="0" title="Min Features" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th><input type="number" id="flt-cluster-min-cohesion" value="${p.get('min_cohesion') || '0'}" step="0.1" min="0" max="1" title="Min Cohesion" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th></th>
            <th><button onclick="applyClusterSearch()" style="width:100%; padding:2px; font-size:0.65rem; cursor:pointer">Search</button></th>
        </tr>`;
        thead.innerHTML = headHtml;
    }

    if (path === '#clusters') {
        const viewMode = params.get('view') || 'table';
        const tbody = document.getElementById('table-body');
        const hview = document.getElementById('hierarchy-view-container');
        const pview = document.getElementById('packing-view-container');
        const pag = document.getElementById('pagination-container');

        if (viewMode === 'hierarchy') {
            tbody.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'flex';
            if (pview) pview.style.display = 'none';
            loadHierarchyView(params);
        } else if (viewMode === 'packing') {
            tbody.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'flex';
            loadPackingView(params);
        } else {
            tbody.style.display = 'table-row-group';
            pag.style.display = 'block';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'none';
        }
    } else if (path === '#file-call-graph') {
        const tbody = document.getElementById('table-body');
        const pag = document.getElementById('pagination-container');
        const cgview = document.getElementById('call-graph-view-container');

        tbody.style.display = 'none';
        pag.style.display = 'none';
        cgview.style.display = 'flex';
        loadCallGraphView(params);
    }

    // Search Bar for Files
    const searchArea = document.getElementById('search-area');
    if (path === '#files' && !document.getElementById('file-search')) {
        searchArea.innerHTML = `<div class="filter-bar">
            <div class="search-input-wrapper">
                <input type="text" id="file-search" placeholder="Filter by filename..." autofocus onchange="debouncedSearch(applySearch)" onkeydown="handleFilterKey(event, applySearch)">
                <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applySearch()" title="Search"></i>
            </div>
        </div>`;
    } else if (path === '#file-call-graph') {
        const p = new URLSearchParams(params);
        const col = p.get('collection');
        const fileMd5 = p.get('file_md5');
        
        searchArea.innerHTML = `<div class="filter-bar" style="display:flex; align-items:center; gap:10px;">
            <label style="color:var(--accent); font-weight:bold; font-size:0.85rem;">Select File:</label>
            <select id="call-graph-file-select" onchange="window.location.hash='#file-call-graph?collection=${col}&file_md5=' + this.value" style="padding: 5px; background: #111; color: var(--success); border: 1px solid var(--border); border-radius: 4px; font-size: 0.8rem; max-width: 400px; width: 300px;">
                <option value="">-- Loading Files... --</option>
            </select>
        </div>`;

        fetch(`/api/file/search?collection=${col}&limit=1000`)
            .then(res => res.json())
            .then(data => {
                const select = document.getElementById('call-graph-file-select');
                if (select) {
                    select.innerHTML = '<option value="">-- Select File --</option>' + 
                        (data.files || []).map(f => `<option value="${f.file_md5}" ${f.file_md5 === fileMd5 ? 'selected' : ''}>${f.file_name} (${f.file_md5.substring(0,8)})</option>`).join('');
                }
            })
            .catch(e => console.error("Error loading files for dropdown", e));
    } else if (path === '#functions') {
        const p = new URLSearchParams(params);
        const fileMd5 = p.get('file_md5');
        const callGraphBtn = fileMd5 ? `<a class="btn-action" href="#file-call-graph?collection=${p.get('collection')}&file_md5=${fileMd5}" style="color:var(--accent); margin-left:10px; padding: 6px 12px; border:1px solid var(--accent); border-radius:4px; font-size:0.8rem;">View File Call Graph 🕸️</a>` : '';
        
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="func-search-input" placeholder="Search by Keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFuncSearch()" title="Search"></i>
                </div>
                ${callGraphBtn}
            </div>
        </div>`;
    } else if (path === '#features-global' && !document.getElementById('feature-search')) {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="feature-search" placeholder="Search by hash $(prefix)..." autofocus value="${p.get('hash') || ''}" onchange="debouncedSearch(applySearch)" onkeydown="handleFilterKey(event, applySearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applySearch()" title="Search"></i>
                </div>
            </div>
            <div style="display:flex; align-items:center; gap:8px;">
                <input type="checkbox" id="sort-tf" ${p.get('sort') === 'tf' ? 'checked' : ''} onchange="applySearch()">
                <label for="sort-tf" style="font-size:0.85rem; cursor:pointer; color:var(--accent)">Sort by Total TF</label>
            </div>
        </div>`;
    } else if (path === '#function-similarity') {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="sim-search-input" placeholder="Search by Keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applySimSearch()" title="Search"></i>
                </div>
            </div>
        </div>`;
    } else if (path !== '#files' && path !== '#functions' && path !== '#features-global') {
        searchArea.innerHTML = '';
    }
}

function applySearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    if (hashPath === '#files') {
        const val = document.getElementById('file-search').value;
        if (val) params.set('file_name', val);
        else params.delete('file_name');
    } else if (hashPath === '#features-global') {
        const val = document.getElementById('feature-search').value;
        if (val) params.set('hash', val);
        else params.delete('hash');

        const sortTf = document.getElementById('sort-tf').checked;
        if (sortTf) params.set('sort', 'tf');
        else params.delete('sort');
    }

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function toggleSort(key) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const currentSort = params.get('sort_by');
    const currentOrder = params.get('sort_order') || 'desc';

    if (currentSort === key) {
        params.set('sort_order', currentOrder === 'desc' ? 'asc' : 'desc');
    } else {
        params.set('sort_by', key);
        params.set('sort_order', 'desc');
    }
    currentOffset = 0;
    if (window.location.hash.startsWith('#function-similarity')) {
        simSearchRequested = true;
    }
    window.location.hash = `${window.location.hash.split('?')[0]}?${params.toString()}`;
}

function applyAdvancedFuncSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const globalQ = document.getElementById('func-search-input')?.value;
    params.set('q', globalQ || '');

    const nameFlt = document.getElementById('flt-function-name')?.value;
    const addressFlt = document.getElementById('flt-function-address')?.value;
    const nsFlt = document.getElementById('flt-function-namespace')?.value;
    const retTypeFlt = document.getElementById('flt-function-ret_type')?.value;
    const fileNameFlt = document.getElementById('flt-function-file_name')?.value;
    const md5Flt = document.getElementById('flt-function-md5')?.value;
    const langFlt = document.getElementById('flt-function-language')?.value;
    const clusterFlt = document.getElementById('flt-function-cluster')?.value;
    const clusterNameFlt = document.getElementById('flt-function-cluster-name')?.value;
    const minFeatFlt = document.getElementById('flt-function-min_features')?.value;

    if (clusterFlt) params.set('cluster_uuid', clusterFlt); else params.delete('cluster_uuid');
    if (clusterNameFlt) params.set('cluster_name', clusterNameFlt); else params.delete('cluster_name');

    if (nameFlt) params.set('function_name', nameFlt); else params.delete('function_name');
    if (addressFlt) params.set('entrypoint_address', addressFlt); else params.delete('entrypoint_address');
    if (nsFlt) params.set('namespace', nsFlt); else params.delete('namespace');
    if (retTypeFlt) params.set('return_type', retTypeFlt); else params.delete('return_type');
    if (fileNameFlt) params.set('file_name', fileNameFlt); else params.delete('file_name');
    if (md5Flt) params.set('file_md5', md5Flt); else params.delete('file_md5');
    if (langFlt) params.set('language_id', langFlt); else params.delete('language_id');
    if (minFeatFlt) params.set('min_features', minFeatFlt); else params.delete('min_features');

    const tagCols = ['func', 'file'];
    const allPossibleTagKeys = [
        'tag', 'static_tag', 'user_tag', 'func_tag', 'func_static_tag', 'func_user_tag', 'file_tag', 'file_static_tag', 'file_user_tag',
        'exclude_tag', 'exclude_static_tag', 'exclude_user_tag', 'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'
    ];
    allPossibleTagKeys.forEach(k => params.delete(k));

    tagCols.forEach(colId => {
        const container = document.getElementById(`tag-container-${colId}`);
        if (!container) return;
        const cards = container.querySelectorAll('.tag-filter-card');
        cards.forEach(card => {
            const type = card.dataset.type;
            const val = card.dataset.value;
            const isEx = card.dataset.exclude === 'true';
            const key = (isEx ? 'exclude_' : '') + type;
            params.append(key, val);
        });
    });

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function switchSimView(mode) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    params.set('view', mode);
    simSearchRequested = true;
    window.location.hash = `#function-similarity?${params.toString()}`;
}

function loadGraphView(params) {
    document.getElementById('graph-view-container').style.display = 'flex';
    if (!window.graphInstance) {
        window.graphInstance = new SimilarityGraph('bk-similarity-plot');
        document.getElementById('graph-refresh-btn').onclick = () => {
            const p = new URLSearchParams(window.location.hash.split('?')[1] || '');
            p.set('use_cache', 'false'); // Force refresh
            window.graphInstance.fetch(p);
        };
        document.getElementById('graph-stop-btn').onclick = () => window.graphInstance.stop();
    }
    window.graphInstance.fetch(params);
}

function applySimSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const minScore = document.getElementById('sim-min-score')?.value;
    const maxScore = document.getElementById('sim-max-score')?.value;
    const algo = document.getElementById('sim-algo')?.value;
    const minFeatures = document.getElementById('sim-min-features')?.value;
    const crossBinary = document.getElementById('sim-cross-binary')?.value;
    const globalQ = document.getElementById('sim-search-input')?.value;
    const lang = document.getElementById('flt-sim-language')?.value;
    const poolLimit = document.getElementById('sim-pool-limit')?.value;
    const countLimit = document.getElementById('sim-limit')?.value;

    params.set('q', globalQ || '');
    if (lang) params.set('language', lang); else params.delete('language');
    params.set('min_score', minScore || '0.95');
    params.set('max_score', maxScore || '1.0');
    params.set('algo', algo || 'unweighted_cosine');
    params.set('min_features', minFeatures || '0');

    if (crossBinary) params.set('cross_binary', crossBinary);
    else params.delete('cross_binary');
    params.set('pool_limit', poolLimit || '1000000');
    params.set('limit', countLimit || (params.get('view') === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT));

    const nameFlt = document.getElementById('flt-sim-name')?.value;
    const addressFlt = document.getElementById('flt-sim-address')?.value;
    const nsFlt = document.getElementById('flt-sim-namespace')?.value;
    const retTypeFlt = document.getElementById('flt-sim-ret_type')?.value;
    const clusterFlt = document.getElementById('flt-sim-cluster')?.value;
    const clusterNameFlt = document.getElementById('flt-sim-cluster-name')?.value;
    const fileNameFlt = document.getElementById('flt-sim-file_name')?.value;

    if (clusterFlt) params.set('cluster_uuid', clusterFlt); else params.delete('cluster_uuid');
    if (clusterNameFlt) params.set('cluster_name', clusterNameFlt); else params.delete('cluster_name');

    if (nameFlt) params.set('name', nameFlt); else params.delete('name');
    if (addressFlt) params.set('address', addressFlt); else params.delete('address');
    if (nsFlt) params.set('namespace', nsFlt); else params.delete('namespace');
    if (retTypeFlt) params.set('ret_type', retTypeFlt); else params.delete('ret_type');
    if (fileNameFlt) params.set('file_name', fileNameFlt); else params.delete('file_name');

    const tagCols = ['sim', 'func', 'file'];
    const allPossibleTagKeys = [
        'tag', 'static_tag', 'user_tag', 'sim_tag', 'sim_static_tag', 'sim_user_tag',
        'func_tag', 'func_static_tag', 'func_user_tag', 'file_tag', 'file_static_tag', 'file_user_tag',
        'exclude_tag', 'exclude_static_tag', 'exclude_user_tag', 'exclude_sim_tag', 'exclude_sim_static_tag', 'exclude_sim_user_tag',
        'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'
    ];
    allPossibleTagKeys.forEach(k => params.delete(k));

    tagCols.forEach(colId => {
        const container = document.getElementById(`tag-container-${colId}`);
        if (!container) return;
        const cards = container.querySelectorAll('.tag-filter-card');
        cards.forEach(card => {
            const type = card.dataset.type;
            const val = card.dataset.value;
            const isEx = card.dataset.exclude === 'true';
            const key = (isEx ? 'exclude_' : '') + type;
            params.append(key, val);
        });
    });

    const md5Flt = document.getElementById('flt-sim-md5')?.value;
    params.delete('md5');
    if (md5Flt) {
        const md5s = md5Flt.split(/[\s,]+/).filter(x => x.length > 0);
        md5s.forEach(m => params.append('md5', m));
    }

    currentOffset = 0;
    isEndOfResults = false;
    simSearchRequested = true;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function triggerTagSearch() {
    if (window.location.hash.startsWith('#function-similarity')) debouncedSearch(applySimSearch);
    else if (window.location.hash.startsWith('#functions')) debouncedSearch(applyAdvancedFuncSearch);
}

function createTagCard(columnId, type, value, isExclude = false) {
    const container = document.getElementById(`tag-container-${columnId}`);
    if (!container) return;

    const existing = Array.from(container.querySelectorAll('.tag-filter-card')).find(c => c.dataset.value === value && c.dataset.type === type);
    if (existing) return;

    const card = document.createElement('div');
    card.className = `tag-filter-card ${isExclude ? 'exclude' : ''}`;
    card.dataset.value = value;
    card.dataset.type = type;
    card.dataset.exclude = isExclude;

    card.innerHTML = `
        <span class="btn-card-ex" title="Toggle Exclude" onclick="toggleCardExclude(this)">NOT</span>
        <span class="tag-text" title="${value}">${value}</span>
        <span class="btn-card-remove" title="Remove" onclick="this.parentElement.remove(); triggerTagSearch();">×</span>
    `;

    container.insertBefore(card, container.querySelector('.tag-filter-add'));
}

function toggleCardExclude(btn) {
    const card = btn.parentElement;
    const isExclude = card.classList.toggle('exclude');
    card.dataset.exclude = isExclude;
    triggerTagSearch();
}

function handleTagAdd(event, columnId) {
    if (event.key === 'Enter' || event.key === ',') {
        event.preventDefault();
        const val = event.target.value.replace(',', '').trim();
        if (val) {
            let type = (columnId === 'sim' ? 'sim_tag' : (columnId === 'func' ? 'func_tag' : 'file_tag'));
            createTagCard(columnId, type, val);
            event.target.value = '';
            triggerTagSearch();
        }
    }
}

function renderPagination(path) {
    const container = document.getElementById('pagination-container');
    container.innerHTML = (!isEndOfResults) ?
        `<button class="btn-primary" onclick="refreshData(true)">Load More Results</button>` : '';
}

function copyToClipboard(text, btn) {
    navigator.clipboard.writeText(text).then(() => {
        const originalHtml = btn.innerHTML;
        btn.innerHTML = '<span style="color:var(--success)">✓</span>';
        setTimeout(() => { btn.innerHTML = originalHtml; }, 1500);
    }).catch(err => console.error('Failed to copy', err));
}

function renderCollections(data) {
    if (!data.length) return '<tr><td colspan="5" style="text-align:center">No collections found.</td></tr>';

    return data.map(col => `
        <tr>
            <td><b style="color:var(--accent)">${col.name}</b></td>
            <td class="mono">${col['total_files']}</td>
            <td class="mono">${col['total_functions']}</td>
            <td class="dim">${formatDate(col['last_updated'])}</td>
            <td>
                <div style="display: flex; gap: 15px;">
                    <a class="btn-action" href="#batches?collection=${col.name}">
                        Browse Batches
                    </a>
                    <span style="color:var(--border)">|</span>
                    <a class="btn-action" href="#files?collection=${col.name}" style="color:var(--success)">
                        View All Files →
                    </a>
                </div>
            </td>
        </tr>
    `).join('');
}

function renderBatches(data) {
    return data.map(b => {
        const col = b.collection || 'unknown';
        return `
        <tr>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <b>${b.name || 'Unnamed'}</b>
                    <button class="btn-copy" title="Copy Batch ID: ${b['batch_id']}" onclick="copyToClipboard('${b['batch_id']}', this)">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                    </button>
                </div>
            </td>
            <td class="mono dim" style="font-size:0.7rem">${b['batch_uuid']}</td>
            <td class="mono">${b['total_files']}</td>
            <td class="mono">${b['total_functions']}</td>
            <td class="dim">${formatDate(b['last_updated'] || b['created_at'])}</td>
            <td><a class="btn-action" href="#files?collection=${col}&batch_uuid=${b['batch_uuid']}">View Files</a></td>
        </tr>
    `}).join('');
}

function renderFiles(data) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const col = params.get('collection');
    return data.map(f => `
        <tr>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <b style="color:var(--accent)">${f['file_name']}</b>
                    <button class="btn-copy" title="Copy File ID: ${f['file_id']}" onclick="copyToClipboard('${f['file_id']}', this)">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                    </button>
                </div>
            </td>
            <td>
                <div class="mono" style="font-size:0.7rem">${f['file_md5']}</div>
                <div class="dim">${f['language_id']}</div>
            </td>
            <td class="dim">${formatDate(f['entry_date'])}</td>
            <td>
                <div style="display:flex; gap:10px;">
                    <a class="btn-action" href="#functions?collection=${col}&file_md5=${f['file_md5']}">Functions →</a>
                    <a class="btn-action" href="#file-call-graph?collection=${col}&file_md5=${f['file_md5']}" style="color:var(--accent)">Call Graph 🕸️</a>
                </div>
            </td>
        </tr>
    `).join('');
}

function renderFunctions(data) {
    return data.map(f => {
        const name = f['function_name'] || 'Unknown';
        const namespace = f['namespace'] || '';
        const parameters = f['parameters'] || [];
        const returnType = f['return_type'] || 'void';
        const entry = f['entrypoint_address'] || '';
        const tags = f['tags'] || [];
        const user_tags = f['user_tags'] || [];
        const fileName = f['file_name'] || '';
        const file_md5 = f['file_md5'] || '';
        const language = f['language_id'] || '---';
        const featCount = f['bsim_features_count'] || 0;
        const funcId = f['function_id'] || `${f.collection}:func:${file_md5}:${entry}`;

        const safeName = (name || '').replace(/'/g, "\\'");
        const fInfo = formatSigComponent(namespace, returnType, name, parameters);
        const rowStyle = getRowTagColor(tags, user_tags);

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${funcId}">
            <td class="sim-cell">
                <div style="display:flex; align-items:center; gap:8px; overflow:hidden;" title="${fInfo.fullSig}">
                    <b style="color:var(--accent); cursor:pointer; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex: 1; min-width: 0;" 
                       onmouseenter="showCodePreview('${funcId}', '${safeName}', '${entry}', '${file_md5}', ${featCount}, event)" 
                       onmousemove="moveCodePreview(event)"
                       onmouseleave="hideCodePreview(event)"
                       onclick="showFunctionCodeById('${funcId}', '${safeName}')">
                        ${fInfo.ret ? `<span style="color:#ae81ff">${fInfo.ret}</span> ` : ''}${fInfo.ns ? `<span style="color:white; opacity:0.8">${fInfo.ns}::</span>` : ''}${name}<span style="color:white">(</span>${fInfo.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                    </b>
                    <div style="display:inline-flex; gap:4px; margin-left: 8px;">
                        <button class="btn-diff-action ${diffSelection.some(item => item.id === normalizeFuncId(funcId)) ? 'active' : ''}" 
                                data-func-id="${normalizeFuncId(funcId)}" 
                                onmouseenter="onHoverDiffButton(event, '${funcId}', '${safeName}')"
                                onmousemove="moveCodePreview(event)"
                                onmouseleave="hideDiffPreview(event)"
                                onclick="addToDiff('${funcId}', '${safeName}')" title="Add to Diff" style="padding:0 5px; font-size: 0.75rem; border-radius: 3px;"><span>±</span></button>
                        <a class="btn-sim-action" href="#function-similarity?collection=${f.collection || 'main'}&md5=${file_md5}&address=${entry}&algo=unweighted_cosine" title="See Similar Functions" style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;"><i class="fa-solid fa-code-compare"></i></a>
                    </div>
                </div>
            </td>
            <td class="sim-cell"><span class="mono" style="color:var(--accent);">@ ${entry}</span></td>
            <td>${renderTagEditor('function', funcId, tags, user_tags)}</td>
            <td class="cluster-cards-cell" data-clusters='${JSON.stringify(f['clusters'] || []).replace(/'/g, "&apos;")}'>${renderClusterCards(f['clusters'])}</td>
            <td class="sim-cell" style="text-align:center;">

                <div style="display:inline-flex; align-items:center; gap:6px;">
                    <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                    <button class="btn-icon" onclick="showFeaturePanel('${funcId}')" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                </div>
            </td>
            <td class="sim-cell"><div style="color:#aaa; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8;" title="${fileName}">${fileName}</div></td>
            <td class="sim-cell"><span class="mono" style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:80px;" title="${file_md5}"># ${file_md5}</span></td>
            <td>${renderTagEditor('file', `${f.collection || 'main'}:file:${file_md5}`, f.file_tags || [], f.file_user_tags || [])}</td>
            <td class="sim-cell"><span class="mono" style="color:var(--accent)">${language}</span></td>
            <td class="sim-cell"><span class="dim" style="font-size:0.7rem;">${formatDate(f['entry_date'] || f['file_date'])}</span></td>
            <td class="sim-cell"></td>
        </tr>
    `}).join('');
}

function renderGlobalFeatures(items) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const col = params.get('collection');
    if (!items.length) return '<tr><td colspan="7" style="text-align:center">No features found.</td></tr>';

    return items.map(f => {
        const ctx = f.context || {};

        let pcodeHtml = `
            <div class="code-card">
                <div class="code-card-line">
                    <div class="code-card-text pcode-text">${ctx.pcode_full || 'N/A'}</div>
                </div>
            </div>`;

        let cCodeHtml = '<span class="dim">N/A</span>';
        if (ctx.c_code) {
            const funcId = ctx.func_id || `${col}:function:${ctx.md5}:${ctx.addr}`;
            const funcName = (ctx.name || ctx.addr);
            const targetLinesStr = (ctx.line_idxs || []).map(l => l + 1).join(',');
            const lineHash = targetLinesStr ? `#L${targetLinesStr}` : '';

            const displayLine = (ctx.line_idxs && ctx.line_idxs.length > 0) ? ctx.line_idxs[0] + 1 : 1;
            cCodeHtml = `<div class="code-card clickable" title="Click to jump to lines ${targetLinesStr || ''}"
                     onclick="showFunctionCodeById('${funcId}', '${funcName.replace(/'/g, "\\'")}', '${lineHash}')">
                <div class="code-card-line">
                    <div class="code-card-ln">${displayLine}</div>
                    <div class="code-card-text">`;
            ctx.c_code.forEach(t => {
                const colorMap = {
                    'variable': 'tok-variable', 'func_call': 'tok-func_call', 'type': 'tok-type',
                    'keyword': 'tok-keyword', 'comment': 'tok-comment', 'string': 'tok-string', 'number': 'tok-number'
                };
                const cls = colorMap[t.type] || 'tok-default';
                cCodeHtml += `<span class="${cls} feature-highlight" 
                    data-hashes="${f.hash}" 
                    data-type="${ctx.type || ''}" 
                    data-op="${ctx.op || ''}" 
                    data-tf="${Math.round(f.tf_score || 0)}"
                    onmouseenter="showTokenTooltip(event)"
                    onmouseleave="hideTokenTooltip()"
                    onmousemove="moveCodePreview(event)">${t.text.replace(/&/g, '&amp;').replace(/</g, '&lt;')}</span>`;
            });
            cCodeHtml += `</div></div></div>`;
        }

        return `
        <tr>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <code class="mono" style="color:var(--accent)">${f.hash}</code>
                    <button class="btn-copy" title="Copy Feature ID: ${f['feature_id']}" onclick="copyToClipboard('${f['feature_id']}', this)">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                    </button>
                </div>
            </td>
            <td>
                <div class="dim" style="font-size:0.65rem; font-weight:bold; color:var(--accent);">${ctx.type}</div>
                <span class="badge" style="margin-top:2px; font-size:0.65rem;">${ctx.op}</span>
            </td>
            <td style="max-width:300px;">${pcodeHtml}</td>
            <td style="max-width:350px;">${cCodeHtml}</td>
            <td class="mono" style="color:var(--accent)">${f.tf_score !== undefined ? Math.round(f.tf_score) : '<span class="dim">-</span>'}</td>
            <td class="mono">${f.frequency}</td>
            <td>
                <button class="btn-action" style="background:none; border:none; padding:0; font-size:0.8rem; text-align:left; color:var(--accent);" 
                    onclick="showGlobalFeaturePanel('${f.hash}', '${col}')">Analyze →</button>
            </td>
        </tr>
    `}).join('');
}

function renderTopCorrelations(items) {
    if (!items || !items.length) return '<tr><td colspan="11" style="text-align:center; padding:40px;">No similarity pairs found in this collection.</td></tr>';

    return items.map(p => {
        const s1 = p.id1.split(':');
        const s2 = p.id2.split(':');
        const col = s1[0];

        const offset1 = s1[0] === 'idx' ? 1 : 0;
        const offset2 = s2[0] === 'idx' ? 1 : 0;

        const m1 = s1[2 + offset1] || '---';
        const m2 = s2[2 + offset2] || '---';
        const addr1 = s1[3 + offset1] || '---';
        const addr2 = s2[3 + offset2] || '---';

        const name1 = (p.name1 || '---').replace(/'/g, "\\'");
        const name2 = (p.name2 || '---').replace(/'/g, "\\'");

        const f1 = formatSigComponent(p.meta1?.namespace || '', p.meta1?.return_type || '', p.name1 || '---', p.meta1?.parameters || []);
        const f2 = formatSigComponent(p.meta2?.namespace || '', p.meta2?.return_type || '', p.name2 || '---', p.meta2?.parameters || []);

        const tags = p.tags || [];
        const user_tags = p.user_tags || [];
        const rowStyle = getRowTagColor(tags, user_tags);
        const pairId = p.sid || `${p.id1}|${p.id2}|${p.algo}`;

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id1="${p.id1}" data-id2="${p.id2}" data-algo="${p.algo}" data-sid="${p.sid || ''}">
            <td style="text-align:center; vertical-align:middle;">
                <input type="checkbox" class="row-selector" 
                        data-pair-id="${pairId}"
                        ${selectedSimilarityPairs.has(pairId) ? 'checked' : ''} 
                        onchange="toggleSimilaritySelection(event, '${p.id1}', '${p.id2}', '${p.algo}')">
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="font-size:1.1rem; font-weight:bold; color:var(--success);">${(p.score * 100).toFixed(1)}%</div>
                    <button class="btn-diff-action" 
                        onmouseenter="showDiffPreview('${p.id1}', '${name1}', '${p.id2}', '${name2}', ${p.score}, event)" 
                        onmousemove="moveCodePreview(event)"
                        onmouseleave="hideDiffPreview(event)"
                        onclick="openDiffDirectly('${p.id1}', '${p.name1.replace(/'/g, "\\'")}', '${p.id2}', '${p.name2.replace(/'/g, "\\'")}')" 
                        title="Run Aligned Diff" 
                        style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;">
                        <span>±</span>
                    </button>
                </div>
                ${renderTagEditor('similarity', pairId, tags, user_tags)}
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; align-items:center; gap:8px; overflow:hidden; min-height:24px;" title="${f1.fullSig}">
                        <b style="color:var(--accent); cursor:pointer; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex: 1; min-width: 0;" 
                           onmouseenter="showCodePreview('${p.id1}', '${name1}', '${addr1}', '${m1}', ${p.meta1?.bsim_features_count || 0}, event, 0, '${(p.meta1?.file_name || '').replace(/'/g, "\\'")}')" 
                           onmousemove="moveCodePreview(event)"
                           onmouseleave="hideCodePreview(event)"
                           onclick="showFunctionCodeById('${p.id1}', '${name1}')">
                            ${f1.ret ? `<span style="color:#ae81ff">${f1.ret}</span> ` : ''}${f1.ns ? `<span style="color:white; opacity:0.8">${f1.ns}::</span>` : ''}${p.name1 || '---'}<span style="color:white">(</span>${f1.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                        </b>
                        <button class="btn-diff-action ${diffSelection.some(item => item.id === normalizeFuncId(p.id1)) ? 'active' : ''}" 
                                data-func-id="${normalizeFuncId(p.id1)}" 
                                onmouseenter="onHoverDiffButton(event, '${p.id1}', '${name1}', '${p.id2}', ${p.score})"
                                onmousemove="moveCodePreview(event)"
                                onmouseleave="hideDiffPreview(event)"
                                onclick="addToDiff('${p.id1}', '${name1}')" title="Add to Diff" style="padding:0 5px; font-size: 0.75rem; margin-left: 4px; border-radius: 3px;"><span>±</span></button>
                        <a class="btn-sim-action" href="#function-similarity?collection=${col}&md5=${m1}&address=${addr1}&algo=unweighted_cosine" title="See Similar Functions" style="padding:0 5px; font-size: 0.75rem; margin-left: 4px; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;"><i class="fa-solid fa-code-compare"></i></a>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px; overflow:hidden; min-height:24px;" title="${f2.fullSig}">
                        <b style="color:var(--accent); cursor:pointer; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex: 1; min-width: 0;" 
                           onmouseenter="showCodePreview('${p.id2}', '${name2}', '${addr2}', '${m2}', ${p.meta2?.bsim_features_count || 0}, event, 0, '${(p.meta2?.file_name || '').replace(/'/g, "\\'")}')" 
                           onmousemove="moveCodePreview(event)"
                           onmouseleave="hideCodePreview(event)"
                           onclick="showFunctionCodeById('${p.id2}', '${name2}')">
                            ${f2.ret ? `<span style="color:#ae81ff">${f2.ret}</span> ` : ''}${f2.ns ? `<span style="color:white; opacity:0.8">${f2.ns}::</span>` : ''}${p.name2 || '---'}<span style="color:white">(</span>${f2.params.map(t => `<span style="color:#ae81ff">${t}</span>`).join('<span style="color:white">, </span>')}<span style="color:white">)</span>
                        </b>
                        <button class="btn-diff-action ${diffSelection.some(item => item.id === normalizeFuncId(p.id2)) ? 'active' : ''}" 
                                data-func-id="${normalizeFuncId(p.id2)}" 
                                onmouseenter="onHoverDiffButton(event, '${p.id2}', '${name2}', '${p.id1}', ${p.score})"
                                onmousemove="moveCodePreview(event)"
                                onmouseleave="hideDiffPreview(event)"
                                onclick="addToDiff('${p.id2}', '${name2}')" title="Add to Diff" style="padding:0 5px; font-size: 0.75rem; margin-left: 4px; border-radius: 3px;"><span>±</span></button>
                        <a class="btn-sim-action" href="#function-similarity?collection=${col}&md5=${m2}&address=${addr2}&algo=unweighted_cosine" title="See Similar Functions" style="padding:0 5px; font-size: 0.75rem; margin-left: 4px; border-radius: 3px; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;"><i class="fa-solid fa-code-compare"></i></a>
                    </div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent);">@ ${addr1}</span></div>
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent);">@ ${addr2}</span></div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;">${renderTagEditor('function', p.id1, p.meta1?.tags || [], p.meta1?.user_tags || [])}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${renderTagEditor('function', p.id2, p.meta2?.tags || [], p.meta2?.user_tags || [])}</div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;" class="cluster-cards-cell" data-clusters='${JSON.stringify(p.meta1?.clusters || []).replace(/'/g, "&apos;")}'>${renderClusterCards(p.meta1?.clusters)}</div>
                    <div style="min-height:24px; display:flex; align-items:center;" class="cluster-cards-cell" data-clusters='${JSON.stringify(p.meta2?.clusters || []).replace(/'/g, "&apos;")}'>${renderClusterCards(p.meta2?.clusters)}</div>
                </div>
            </td>
            <td class="sim-cell" style="text-align:center; vertical-align:middle;">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta1?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel('${p.id1}')" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta2?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel('${p.id2}')" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="color:#aaa; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8; min-height:24px; display:flex; align-items:center;" title="${p.meta1?.file_name}">${p.meta1?.file_name || ''}</div>
                    <div style="color:#aaa; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8; min-height:24px; display:flex; align-items:center;" title="${p.meta2?.file_name}">${p.meta2?.file_name || ''}</div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:80px;" title="${m1}"># ${m1}</span></div>
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:80px;" title="${m2}"># ${m2}</span></div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;">${renderTagEditor('file', `${col}:file:${p.meta1?.file_md5}`, p.meta1?.file_tags || [], p.meta1?.file_user_tags || [])}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${renderTagEditor('file', `${col}:file:${p.meta2?.file_md5}`, p.meta2?.file_tags || [], p.meta2?.file_user_tags || [])}</div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent)">${p.meta1?.language_id || '---'}</span></div>
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent)">${p.meta2?.language_id || '---'}</span></div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.7rem;">${formatDate(p.meta1?.entry_date)}</span></div>
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="dim" style="font-size:0.7rem;">${formatDate(p.meta2?.entry_date)}</span></div>
                </div>
            </td>
        </tr>
    `}).join('');
}

function showDiffPanel(force = false) {
    if (!force && diffSelection.length < 2) return;

    hideCodePanel();
    hideFeaturePanel();
    hideGlobalFeaturePanel();

    const p = document.getElementById('diff-panel');
    const f = document.getElementById('diff-frame');
    const title = document.getElementById('diff-title');

    let url = '/diff/index.html';
    let label = 'Function Comparison';

    if (diffSelection.length === 2) {
        url = `/diff/index.html?id1=${encodeURIComponent(diffSelection[0].id)}&id2=${encodeURIComponent(diffSelection[1].id)}`;
        label = `Diff: ${diffSelection[0].name} vs ${diffSelection[1].name}`;

        // Reset queue after opening
        diffSelection = [];
        updateDiffQueueUI();
        saveDiffQueue();
    }

    if (p) p.style.display = 'flex';
    if (title) title.innerText = label;
    if (f) f.src = url;
}

function openDiffDirectly(id1, name1, id2, name2) {
    hideCodePanel();
    hideFeaturePanel();
    hideGlobalFeaturePanel();

    const p = document.getElementById('diff-panel');
    const f = document.getElementById('diff-frame');
    const title = document.getElementById('diff-title');

    const url = `/diff/index.html?id1=${encodeURIComponent(normalizeFuncId(id1))}&id2=${encodeURIComponent(normalizeFuncId(id2))}`;

    if (p) p.style.display = 'flex';
    if (title) title.innerText = `Diff: ${name1} vs ${name2}`;
    if (f) f.src = url;
}

function hideDiffPanel() {
    const p = document.getElementById('diff-panel');
    if (p) p.style.display = 'none';
}

function showDiffView() {
    let url = '/diff/index.html';
    if (diffSelection.length === 2) {
        url = `/diff/index.html?id1=${encodeURIComponent(diffSelection[0].id)}&id2=${encodeURIComponent(diffSelection[1].id)}`;

        // Reset queue after opening in new window
        diffSelection = [];
        updateDiffQueueUI();
        saveDiffQueue();
    }
    window.open(url, '_blank');
}

function showFunctionCodeById(id, name, lineHash = '') {
    hideFeaturePanel();
    hideGlobalFeaturePanel();
    const p = document.getElementById('code-panel');
    const f = document.getElementById('code-frame');
    const title = document.getElementById('code-title');

    if (p) p.style.display = 'flex';
    if (title) title.innerText = `Code: ${name}`;
    if (f) f.src = `/function/index.html?id=${encodeURIComponent(id)}${lineHash}`;
}

function seeSimilarFromCode() {
    const frame = document.getElementById('code-frame');
    if (!frame || !frame.src) return;
    const url = new URL(frame.src, window.location.origin);
    const id = url.searchParams.get('id');
    if (!id) return;

    const parts = id.split(':');
    if (parts.length < 4) return;
    const col = parts[0];
    const md5 = parts[2];
    const addr = parts[3];

    window.location.hash = `#function-similarity?collection=${col}&md5=${md5}&address=${addr}&algo=unweighted_cosine`;
    hideCodePanel();
}

function hideCodePanel() {
    const p = document.getElementById('code-panel');
    if (p) p.style.display = 'none';
}

function showFeaturePanel(id) {
    hideCodePanel();
    const p = document.getElementById('feature-panel');
    const f = document.getElementById('feature-frame');
    const title = document.getElementById('feature-title');

    const addr = id.split(':').pop();

    if (p) p.style.display = 'flex';
    if (title) title.innerText = `Features: ${addr}`;
    if (f) f.src = `/function/features/index.html?id=${encodeURIComponent(id)}`;
}

function hideFeaturePanel() {
    const p = document.getElementById('feature-panel');
    if (p) p.style.display = 'none';
}

function showGlobalFeaturePanel(hash, collection) {
    hideCodePanel();
    hideFeaturePanel();
    const p = document.getElementById('global-feature-panel');
    const f = document.getElementById('global-feature-frame');
    const title = document.getElementById('global-feature-title');

    if (p) p.style.display = 'flex';
    if (title) title.innerText = `Feature Analysis: ${hash.substring(0, 12)}...`;
    if (f) f.src = `/feature/index.html?hash=${encodeURIComponent(hash)}&collection=${encodeURIComponent(collection)}`;
}

function hideGlobalFeaturePanel() {
    const p = document.getElementById('global-feature-panel');
    if (p) p.style.display = 'none';
}

function launchExternal(type) {
    let url = '';
    if (type === 'diff') {
        url = document.getElementById('diff-frame').src;
    } else if (type === 'code') {
        url = document.getElementById('code-frame').src;
    } else if (type === 'features') {
        url = document.getElementById('feature-frame').src;
    } else if (type === 'global-feature') {
        url = document.getElementById('global-feature-frame').src;
    }

    if (url) {
        window.open(url, '_blank');
        if (type === 'diff') hideDiffPanel();
        else if (type === 'code') hideCodePanel();
        else if (type === 'features') hideFeaturePanel();
        else if (type === 'global-feature') hideGlobalFeaturePanel();
    }
}

// Resizing Logic
let isResizing = false;
let currentPanel = null;

function initResize(panelId) {
    const panel = document.getElementById(panelId);
    if (!panel) return;
    const handle = panel.querySelector('.resize-handle');
    if (!handle) return;

    handle.addEventListener('mousedown', e => {
        isResizing = true;
        currentPanel = panel;
        document.body.style.cursor = 'ns-resize';
        document.body.style.userSelect = 'none';
    });
}

window.addEventListener('mousemove', e => {
    if (!isResizing || !currentPanel) return;
    const newHeight = window.innerHeight - e.clientY;
    if (newHeight > 100 && newHeight < window.innerHeight * 0.9) {
        currentPanel.style.height = newHeight + 'px';
    }
});

window.addEventListener('mouseup', () => {
    isResizing = false;
    currentPanel = null;
    document.body.style.cursor = 'default';
    document.body.style.userSelect = 'auto';
});

// Apply "NOT ignore" defaults only when first entering the Sim view
function applySimViewDefaults(hashPath, queryString) {
    if (hashPath !== '#function-similarity') return false;
    const pC = new URLSearchParams(queryString);
    let changed = false;

    const hasFunc = pC.has('func_tag') || pC.has('exclude_func_tag') ||
        pC.has('func_static_tag') || pC.has('exclude_func_static_tag') ||
        pC.has('func_user_tag') || pC.has('exclude_func_user_tag') ||
        pC.has('tag') || pC.has('exclude_tag') ||
        pC.has('static_tag') || pC.has('exclude_static_tag') ||
        pC.has('user_tag') || pC.has('exclude_user_tag');
    if (!hasFunc) { pC.set('exclude_func_tag', 'ignore'); changed = true; }

    const hasSim = pC.has('sim_tag') || pC.has('exclude_sim_tag') ||
        pC.has('sim_static_tag') || pC.has('exclude_sim_static_tag') ||
        pC.has('sim_user_tag') || pC.has('exclude_sim_user_tag');
    if (!hasSim) { pC.set('exclude_sim_tag', 'ignore'); changed = true; }

    const hasFile = pC.has('file_tag') || pC.has('exclude_file_tag') ||
        pC.has('file_static_tag') || pC.has('exclude_file_static_tag') ||
        pC.has('file_user_tag') || pC.has('exclude_file_user_tag');
    if (!hasFile) { pC.set('exclude_file_tag', 'ignore'); changed = true; }

    if (changed) {
        window.location.hash = hashPath + '?' + pC.toString();
        return true; // redirected, don't call refreshData yet
    }
    return false;
}

window.addEventListener('hashchange', (e) => {
    // Ensure all tooltips are hidden when navigating/switching views
    [
        document.getElementById('code-preview-tooltip'),
        document.getElementById('token-tooltip'),
        document.getElementById('diff-preview-tooltip'),
        document.getElementById('hierarchy-tooltip'),
        document.getElementById('binary-preview-tooltip')
    ].forEach(el => {
        if (el) {
            el.style.display = 'none';
            el.classList.remove('showing');
        }
    });
    if (window.hierarchyInstance) window.hierarchyInstance.hideTooltip();

    const [newHash] = (window.location.hash || '#collections').split('?');
    const [oldHash] = (e.oldURL ? new URL(e.oldURL).hash : '').split('?');
    // Apply defaults only when entering sim view from a different view
    if (newHash === '#function-similarity' && oldHash !== '#function-similarity') {
        const [hashPath, queryString] = (window.location.hash || '').split('?');
        if (applySimViewDefaults(hashPath, queryString)) return;
    }
    refreshData();
});

// UI Settings
let UIParams = {
    cohesionThreshold: parseFloat(localStorage.getItem('cohesionThreshold')) || 0.5,
    colorByTag: localStorage.getItem('colorByTag') === 'true'
};

function toggleUISettings() {
    const panel = document.getElementById('ui-settings-panel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

function updateUIParams() {
    const prevThreshold = UIParams.cohesionThreshold;
    const prevColorByTag = UIParams.colorByTag;

    UIParams.cohesionThreshold = parseFloat(document.getElementById('param-cohesion').value);
    UIParams.colorByTag = document.getElementById('param-color-tags').checked;

    document.getElementById('val-cohesion').innerText = UIParams.cohesionThreshold.toFixed(2);

    localStorage.setItem('cohesionThreshold', UIParams.cohesionThreshold);
    localStorage.setItem('colorByTag', UIParams.colorByTag);
    
    // Sync with sim-color-by-tag for tags.js compatibility
    localStorage.setItem('sim-color-by-tag', UIParams.colorByTag);

    // Real-time updates
    if (UIParams.colorByTag !== prevColorByTag) {
        if (typeof refreshAllRowColors === 'function') {
            refreshAllRowColors();
        }
        if (window.graphInstance && typeof window.graphInstance.refreshColors === 'function') {
            window.graphInstance.refreshColors();
        }
    }

    if (UIParams.cohesionThreshold !== prevThreshold) {
        const [hashPath] = (window.location.hash || '#collections').split('?');
        if (hashPath === '#clusters') {
            filterClusterRowsByCohesion(UIParams.cohesionThreshold);
        } else {
            refreshClusterCards();
        }
    }
}

function filterClusterRowsByCohesion(threshold) {
    const rows = document.querySelectorAll('#table-body tr[data-cluster-id]');
    rows.forEach(row => {
        // Cohesion is in the 6th column (index 5)
        const cohesionSpan = row.querySelectorAll('td')[5]?.querySelector('span.dim');
        if (cohesionSpan) {
            const score = parseFloat(cohesionSpan.textContent);
            row.style.display = (score < threshold) ? 'none' : '';
        }
    });
}

function refreshClusterCards() {
    document.querySelectorAll('.cluster-cards-cell').forEach(cell => {
        try {
            const clusters = JSON.parse(cell.dataset.clusters || '[]');
            if (typeof renderClusterCards === 'function') {
                cell.innerHTML = renderClusterCards(clusters);
            }
        } catch (e) {
            console.error("Failed to re-render cluster cards", e);
        }
    });
}

function loadUIParams() {
    const elCohesion = document.getElementById('param-cohesion');
    const elColorTags = document.getElementById('param-color-tags');
    if (elCohesion) {
        elCohesion.value = UIParams.cohesionThreshold;
        document.getElementById('val-cohesion').innerText = UIParams.cohesionThreshold.toFixed(2);
    }
    if (elColorTags) elColorTags.checked = UIParams.colorByTag;
}

window.addEventListener('load', () => {
    loadUIParams();
    if (!window.location.hash) window.location.hash = '#collections';
    // Apply defaults on initial page load if landing on sim view
    const [hashPath, queryString] = (window.location.hash || '').split('?');
    if (applySimViewDefaults(hashPath, queryString)) {
        loadDiffQueue();
        return;
    }
    refreshData();
    loadDiffQueue();
});

async function populateCollectionDropdown() {
    try {
        const res = await fetch('/api/collection/search');
        if (!res.ok) return;
        const data = await res.json();
        const collections = data.collections || (Array.isArray(data) ? data : []);
        const select = document.getElementById('side-collection-select');
        if (!select) return;

        select.innerHTML = '<option value="">None Selected</option>';
        collections.forEach(c => {
            const opt = document.createElement('option');
            opt.value = c.name;
            opt.innerText = c.name;
            select.appendChild(opt);
        });

        // Sync with current URL
        const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
        select.value = params.get('collection') || '';
        updateNavVisibility(select.value);

        // Add event listener once populated
        if (!select.dataset.hasListener) {
            select.addEventListener('change', (e) => {
                const val = e.target.value;
                const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
                const p = new URLSearchParams(queryString);
                if (val) p.set('collection', val);
                else p.delete('collection');
                window.location.hash = `${hashPath}?${p.toString()}`;
            });
            select.dataset.hasListener = "true";
        }
    } catch (e) {
        console.error("Failed to populate collections", e);
    }
}

function updateNavVisibility(collection) {
    const navItems = ['nav-batches', 'nav-files', 'nav-functions', 'nav-features-global', 'nav-function-similarity', 'nav-clusters', 'nav-file-call-graph'];
    navItems.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = collection ? 'flex' : 'none';
    });
}

function renderClusters(items) {
    return items.map(c => `
        <tr data-cluster-id="${c.cluster_id}">
            <td class="mono" style="color:var(--accent)">
                ${(c.cluster_uuid || '').substring(0, 8)}
                <div class="dim" style="font-size:0.7rem">ID: ${c.cluster_id}</div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <span id="name-display-${c.cluster_id}">${c.cluster_name}</span>
                    <button class="btn-panel" style="padding: 2px 5px; font-size: 0.6rem;" onclick="renameCluster('${c.cluster_id}', '${c.cluster_name}')">✎</button>
                </div>
            </td>
            <td style="font-weight:bold">${c.count.toLocaleString()}</td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--success); width:${Math.min(100, c.avg_stability).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.avg_stability).toFixed(2)}</span>
                </div>
            </td>
            <td class="mono dim">${(c.avg_features || 0).toFixed(1)}</td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--info); width:${((c.cohesion_score || 0) * 100).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.cohesion_score || 0).toFixed(2)}</span>
                </div>
            </td>
            <td class="dim">${formatDate(c.created_at)}</td>
            <td>
                <div style="display:flex; gap:10px;">
                    <a href="#functions?collection=${document.getElementById('side-collection-select').value}&cluster_uuid=${c.cluster_uuid}" class="btn-action" onmouseenter="showClusterTableTooltip(event, '${c.cluster_uuid}', '${(c.cluster_name || '').replace(/'/g, "\\'")}', ${c.count || 0}, ${c.avg_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})" onmouseleave="hideClusterTableTooltip(event)" onmousemove="moveClusterTableTooltip(event)">View Functions →</a>
                    <a href="#function-similarity?collection=${document.getElementById('side-collection-select').value}&cluster_uuid=${c.cluster_uuid}" class="btn-action" style="color:var(--info)">View similarities →</a>
                </div>
            </td>
        </tr>
    `).join('');
}

function applyClusterSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const cid = document.getElementById('flt-cluster-id')?.value;
    const cuuid = document.getElementById('flt-cluster-uuid')?.value;
    const cname = document.getElementById('flt-cluster-name')?.value;
    const cstab = document.getElementById('flt-cluster-min-stability')?.value;
    const ccount = document.getElementById('flt-cluster-min-count')?.value;
    const cfeat = document.getElementById('flt-cluster-min-features')?.value;
    const ccoh = document.getElementById('flt-cluster-min-cohesion')?.value;

    if (cid) params.set('cluster_id', cid); else params.delete('cluster_id');
    if (cuuid) params.set('cluster_uuid', cuuid); else params.delete('cluster_uuid');
    if (cname) params.set('cluster_name', cname); else params.delete('cluster_name');
    if (cstab) params.set('min_stability', cstab); else params.delete('min_stability');
    if (ccount) params.set('min_count', ccount); else params.delete('min_count');
    if (cfeat) params.set('min_features', cfeat); else params.delete('min_features');
    if (ccoh) params.set('min_cohesion', ccoh); else params.delete('min_cohesion');

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

async function renameCluster(clusterId, currentName) {
    const newName = prompt(`Enter new name for cluster ${clusterId}:`, currentName);
    if (!newName || newName === currentName) return;

    const collection = document.getElementById('side-collection-select').value;
    const algo = new URLSearchParams(window.location.hash.split('?')[1]).get('algo') || 'unweighted_cosine';

    try {
        const res = await fetch('/api/cluster/meta', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                collection,
                algo,
                cluster_id: clusterId,
                cluster_name: newName
            })
        });
        if (res.ok) {
            document.getElementById(`name-display-${clusterId}`).innerText = newName;
        } else {
            alert("Failed to rename cluster");
        }
    } catch (e) {
        console.error(e);
        alert("Error renaming cluster");
    }
}

function switchClusterView(mode) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    params.set('view', mode);
    window.location.hash = `#clusters?${params.toString()}`;
}

function showTokenContextMenu(e) {
    const token = e.target.closest('.feature-highlight');
    if (!token) return;
    e.preventDefault();
    e.stopPropagation();

    const hashesStr = token.dataset.hashes;
    if (!hashesStr) return;
    const hashes = hashesStr.trim().split(/\s+/);
    const idx = token.dataset.idx;

    const richData = (idx !== undefined && window.previewTips) ? window.previewTips[idx] : null;
    const collection = window.previewCollection;

    let menu = document.getElementById('token-context-menu');
    if (!menu) {
        menu = document.createElement('div');
        menu.id = 'token-context-menu';
        menu.className = 'context-menu';
        document.body.appendChild(menu);
    }

    let html = `<div class="context-menu-header">Select Feature to Analyze</div>`;
    if (richData && richData[2]) {
        richData[2].forEach(f => {
            const hash = f[0], op = f[1], type = f[3], tf = f[7] || 0, color = f[8] || 'var(--accent)';
            html += `<div class="context-menu-item feature-analyze-item" data-hash="${hash}" data-col="${collection}">
                <div class="context-menu-icon" style="color:${color}">🔍</div>
                <div style="flex:1">
                    <div style="font-family:monospace; font-weight:bold; color:${color}">${hash}</div>
                    <div style="font-size:0.7rem; color:var(--subtle); margin-top:2px;">${type} | Op: ${op} | <b style="color:var(--success)">TF: ${tf}</b></div>
                </div>
            </div>`;
        });
    } else {
        hashes.forEach(h => {
            html += `<div class="context-menu-item feature-analyze-item" data-hash="${h}" data-col="${collection}">
                <div class="context-menu-icon" style="color:var(--accent)">🔍</div>
                <div style="flex:1">
                    <div style="font-family:monospace; font-weight:bold; color:var(--accent)">${h}</div>
                    <div style="font-size:0.7rem; color:var(--subtle); margin-top:2px;">Feature | Op: --- | <b style="color:var(--success)">TF: ---</b></div>
                </div>
            </div>`;
        });
    }

    menu.innerHTML = html;
    menu.style.display = 'block';
    let x = e.clientX, y = e.clientY;
    if (x + 350 > window.innerWidth) x -= 350;
    const itemCount = richData ? richData[2].length : hashes.length;
    if (y + (itemCount * 52 + 50) > window.innerHeight) y -= (itemCount * 52 + 50);
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';

    const onMenuClick = (me) => {
        const item = me.target.closest('.feature-analyze-item');
        if (item) {
            const h = item.dataset.hash;
            const c = item.dataset.col;
            const url = `/feature/index.html?hash=${encodeURIComponent(h)}&collection=${encodeURIComponent(c)}`;

            if (me.ctrlKey || me.metaKey) {
                window.open(url, '_blank');
            } else {
                showGlobalFeaturePanel(h, c);
            }
        }
        closeMenu();
    };
    const closeMenu = () => {
        menu.style.display = 'none';
        menu.removeEventListener('click', onMenuClick);
        document.removeEventListener('mousedown', closeGlobal);
    };
    const closeGlobal = (me) => { if (!menu.contains(me.target)) closeMenu(); };
    setTimeout(() => {
        menu.addEventListener('click', onMenuClick);
        document.addEventListener('mousedown', closeGlobal);
    }, 10);
}

function showGraphContextMenu(e) {
    const graph = window.graphInstance;
    if (!graph) return;

    const linkIndices = window.lastGraphLinkIndices || [];
    const nodeIndices = window.lastGraphNodeIndices || [];

    if (linkIndices.length === 0 && nodeIndices.length === 0) return;

    e.preventDefault();
    e.stopPropagation();

    hideDiffPreview();
    hideCodePreview();
    hideBinaryPreview();
    window.graphContextMenuOpen = true;

    let menu = document.getElementById('graph-context-menu');
    if (!menu) {
        menu = document.createElement('div');
        menu.id = 'graph-context-menu';
        menu.className = 'context-menu';
        document.body.appendChild(menu);
    }

    let html = '';

    if (linkIndices.length > 0) {
        html += `<div class="context-menu-header">Similarity Comparisons (${linkIndices.length})</div>`;
        linkIndices.forEach(idx => {
            const d = graph.hit_source.data;
            const id1 = d.id1[idx], id2 = d.id2[idx], f1 = d.f1[idx], f2 = d.f2[idx], score = d.score[idx];
            html += `<div class="context-menu-item graph-compare-item" data-id1="${id1}" data-id2="${id2}" data-f1="${f1}" data-f2="${f2}">
                <div class="context-menu-icon" style="color:var(--success)">⇄</div>
                <div style="flex:1">
                    <div style="font-weight:bold; color:#FFF; font-size:0.75rem;">${f1} vs ${f2}</div>
                    <div style="font-size:0.65rem; color:var(--success); margin-top:2px;">Match Score: <b>${(score * 100).toFixed(2)}%</b></div>
                </div>
            </div>`;
        });
    }

    if (nodeIndices.length > 0) {
        html += `<div class="context-menu-header">Function Analysis (${nodeIndices.length})</div>`;
        nodeIndices.forEach(idx => {
            const d = graph.node_source.data;
            const id = d.id[idx], name = d.name[idx], addr = d.addr[idx];
            html += `<div class="context-menu-item graph-node-item" data-id="${id}">
                <div class="context-menu-icon" style="color:var(--accent)">𝑓</div>
                <div style="flex:1">
                    <div style="font-weight:bold; color:var(--accent); font-size:0.75rem;">${name}</div>
                    <div style="font-size:0.65rem; color:var(--subtle); font-family:monospace;">Addr: ${addr}</div>
                </div>
            </div>`;
        });
    }

    menu.innerHTML = html;
    menu.style.display = 'block';
    let x = e.clientX, y = e.clientY;
    if (x + 350 > window.innerWidth) x -= 350;
    const totalItems = linkIndices.length + nodeIndices.length;
    if (y + (totalItems * 52 + 60) > window.innerHeight) y -= (totalItems * 52 + 60);
    menu.style.left = x + 'px';
    menu.style.top = y + 'px';

    const onMenuClick = (me) => {
        const compareItem = me.target.closest('.graph-compare-item');
        const nodeItem = me.target.closest('.graph-node-item');

        if (compareItem) {
            const { id1, id2, f1, f2 } = compareItem.dataset;
            openDiffDirectly(id1, f1, id2, f2);
        } else if (nodeItem) {
            const { id } = nodeItem.dataset;
            showFunctionCodeById(id);
        }
        closeMenu();
    };
    const closeMenu = () => {
        menu.style.display = 'none';
        window.graphContextMenuOpen = false;
        menu.removeEventListener('click', onMenuClick);
        document.removeEventListener('mousedown', closeGlobal);
    };
    const closeGlobal = (me) => { if (!menu.contains(me.target)) closeMenu(); };
    setTimeout(() => {
        menu.addEventListener('click', onMenuClick);
        document.addEventListener('mousedown', closeGlobal);
    }, 10);
}

document.addEventListener('DOMContentLoaded', () => {
    // Attach graph context menu (global to ensure it's not swallowed by Bokeh canvas)
    window.addEventListener('contextmenu', showGraphContextMenu);

    // Intercept wheel events for scrolling code/diff preview tooltips while hovering trigger elements
    // [REMOVED: Now handled in previews.js to prevent double-scroll]

    // Reducible Panels Initialization
    if (localStorage.getItem('sidebarCollapsed') === 'true') {
        document.body.classList.add('sidebar-collapsed');
        const btn = document.getElementById('sidebar-toggle');
        if (btn) btn.innerHTML = '⟩';
    }
    if (localStorage.getItem('headerCollapsed') === 'true') {
        document.body.classList.add('header-collapsed');
        const btn = document.getElementById('collapse-header-btn');
        if (btn) btn.classList.remove('active');

    }
    if (localStorage.getItem('filtersCollapsed') === 'true') {
        document.body.classList.add('filters-collapsed');
        const btn = document.getElementById('toggle-filters-btn');
        if (btn) btn.classList.remove('active');
    }
});

document.addEventListener('contextmenu', e => {
    if (e.target.closest('.feature-highlight')) {
        showTokenContextMenu(e);
    }
});

initResize('code-panel');
initResize('feature-panel');
initResize('global-feature-panel');
initResize('diff-panel');

// Expose dashboard controllers/globals explicitly on window
window.applyAdvancedFuncSearch = applyAdvancedFuncSearch;
window.applySimSearch = applySimSearch;
window.applyClusterSearch = applyClusterSearch;
window.switchClusterView = switchClusterView;
window.renameCluster = renameCluster;
window.refreshData = refreshData;
window.clearFilters = clearFilters;
window.toggleSort = toggleSort;
window.applySearch = applySearch;
window.switchSimView = switchSimView;
window.debouncedSearch = debouncedSearch;
window.handleFilterKey = handleFilterKey;
window.toggleSidebar = toggleSidebar;
window.toggleHeader = toggleHeader;
window.toggleFilters = toggleFilters;
window.toggleSimilaritySelection = toggleSimilaritySelection;
window.toggleAllSimilaritySelection = toggleAllSimilaritySelection;
window.getSimilarityRowInfo = getSimilarityRowInfo;
window.seeSimilarFromCode = seeSimilarFromCode;
window.showFunctionCodeById = showFunctionCodeById;
window.showFeaturePanel = showFeaturePanel;
window.showGlobalFeaturePanel = showGlobalFeaturePanel;
window.openDiffDirectly = openDiffDirectly;
window.hideCodePanel = hideCodePanel;
window.hideFeaturePanel = hideFeaturePanel;
window.hideGlobalFeaturePanel = hideGlobalFeaturePanel;
window.hideDiffPanel = hideDiffPanel;
window.showDiffView = showDiffView;
window.showDiffPanel = showDiffPanel;
window.launchExternal = launchExternal;
window.createTagCard = createTagCard;
window.toggleCardExclude = toggleCardExclude;
window.handleTagAdd = handleTagAdd;
window.triggerTagSearch = triggerTagSearch;
