// Main Dashboard Controller for BSimVis

const windowManager = new WindowManager();
window.windowManager = windowManager;

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

        // Capture focus and selection before searching
        currentFocusId = e.target.id;
        try {
            if (e.target.setSelectionRange) {
                preservedSelection.start = e.target.selectionStart;
                preservedSelection.end = e.target.selectionEnd;
            }
        } catch (err) {
            // Some input types (number, etc.) throw when accessing selection properties
            preservedSelection.start = 0;
            preservedSelection.end = 0;
        }

        searchFn();
    }
}

// --- Column Resizing & Persistence ---
function getSavedColumnWidth(path, label) {
    try {
        const saved = JSON.parse(localStorage.getItem('columnWidths') || '{}');
        return saved[path] ? saved[path][label] : null;
    } catch (e) { return null; }
}

function saveColumnWidth(path, label, width) {
    try {
        const saved = JSON.parse(localStorage.getItem('columnWidths') || '{}');
        if (!saved[path]) saved[path] = {};
        saved[path][label] = width;
        localStorage.setItem('columnWidths', JSON.stringify(saved));
    } catch (e) { }
}

function resetColumnWidths() {
    const [hashPath] = (window.location.hash || '#collections').split('?');
    try {
        const saved = JSON.parse(localStorage.getItem('columnWidths') || '{}');
        delete saved[hashPath];
        localStorage.setItem('columnWidths', JSON.stringify(saved));
    } catch (e) { }
    refreshData(false, true);
}

function initColumnResize(th, path, label) {
    const resizer = th.querySelector('.resizer');
    if (!resizer) return;

    // Find matching <col> in both header and body colgroups by th index
    const colgroupHeader = document.getElementById('table-colgroup-header');
    const colgroupBody = document.getElementById('table-colgroup');
    const thIndex = Array.from(th.parentElement.children).indexOf(th);
    const colHeader = colgroupHeader ? colgroupHeader.children[thIndex] : null;
    const colBody = colgroupBody ? colgroupBody.children[thIndex] : null;

    let startX, startWidth;

    resizer.addEventListener('mousedown', (e) => {
        e.preventDefault();
        e.stopPropagation();

        startX = e.clientX;
        startWidth = th.getBoundingClientRect().width;

        document.body.classList.add('resizing');

        // Disable pointer events on iframes during resize
        document.querySelectorAll('iframe').forEach(ifrm => {
            ifrm.style.pointerEvents = 'none';
        });

        const onMouseMove = (e) => {
            const width = startWidth + (e.clientX - startX);
            if (width > 30) {
                // Sync width to both tables via colgroup
                if (colHeader) colHeader.style.width = width + 'px';
                if (colBody) colBody.style.width = width + 'px';
                th.style.width = width + 'px';
                th.style.minWidth = width + 'px';
            }
        };

        const onMouseUp = () => {
            document.body.classList.remove('resizing');
            document.querySelectorAll('iframe').forEach(ifrm => {
                ifrm.style.pointerEvents = 'auto';
            });
            saveColumnWidth(path, label, th.style.width);
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
        };

        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    });
}
// ------------------------------------

let currentOffset = 0;
const DEFAULT_PAGE_LIMIT = 100;
const DEFAULT_GRAPH_LIMIT = 500;
const PAGE_SIZE = DEFAULT_PAGE_LIMIT;
let isEndOfResults = false;
let lastHashPath = '';
let lastViewPath = '';
let lastSimilarityQuery = null; // Track filters to detect view switching
let simSearchRequested = false; // Set to true when user explicitly triggers a search
let currentFocusId = null;
let preservedSelection = { start: 0, end: 0 };

function toggleSidebar() {
    const body = document.body;
    const isCollapsed = body.classList.toggle('sidebar-collapsed');
    localStorage.setItem('sidebarCollapsed', isCollapsed);
    const btn = document.getElementById('sidebar-toggle');
    btn.innerHTML = isCollapsed ? '⟩' : '⟨';

    // Trigger window resize for D3 plots
    setTimeout(() => window.dispatchEvent(new Event('resize')), 300);

    // Update header buttons visibility immediately
    if (window.updateJobStatusIcon) window.updateJobStatusIcon();
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
        headers: [
            { label: 'Filename', width: '18%' },
            { label: 'MD5 / Arch', width: '12%' },
            { label: 'Metadata', width: '15%' },
            { label: 'Batch UUID', width: '10%' },
            { label: 'Funcs', width: '8%', sort: 'function_count' },
            { label: 'Clusters', width: '13%' },
            { label: 'Entry Date', width: '8%', sort: 'entry_date' },
            { label: 'Tags', width: '16%' }
        ],
        renderer: renderFiles
    },
    '#functions': {
        title: 'Functions',
        api: '/api/function/search',
        headers: [
            { label: 'Function', width: '20%' },
            { label: 'Address', width: '8%', sort: 'entrypoint_address' },
            { label: 'Function Tags', width: '10%' },
            { label: 'Clusters', width: '10%' },
            { label: 'Feat', width: '5%', sort: 'bsim_features_count' },
            { label: 'Notes', width: '3%' },
            { label: 'File Name', width: '10%', sort: 'file_name' },
            { label: 'MD5', width: '5%', sort: 'file_md5' },
            { label: 'File Tags', width: '11%' },
            { label: 'Language', width: '5%', sort: 'language_id' },
            { label: 'Date', width: '8%', sort: 'entry_date' },
            { label: 'Actions', width: '5%' }
        ],
        renderer: renderFunctions
    },
    '#features-global': {
        title: 'Global Feature Index',
        api: '/api/feature/search',
        headers: [
            { label: 'Feature Hash', width: '12%', sort: 'hash' },
            { label: 'Type / Op', width: '10%', sort: 'type' },
            { label: 'PCode Context', width: '22%' },
            { label: 'C Code Context', width: '24%' },
            { label: 'Total TF', width: '6%', sort: 'tf_score' },
            { label: 'Funcs', width: '5%', sort: 'frequency' },
            { label: 'Actions', width: '5%' }
        ],
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
            { label: 'Notes', width: '3%' },
            { label: 'File Name', width: '9%' },
            { label: 'MD5', width: '5%' },
            { label: 'File Tags', width: '10%' },
            { label: 'Language', width: '5%' },
            { label: 'Date', sort: 'entry_date', width: '12%' }
        ],
        renderer: renderTopCorrelations
    },
    '#clusters': {
        title: 'Function Clusters',
        api: '/api/cluster/list',
        headers: [
            { label: 'UUID', sort: 'cluster_uuid', width: '10%' },
            { label: 'Name', sort: 'cluster_name', width: '25%' },
            { label: 'Functions', sort: 'count', width: '12%' },
            { label: 'Stability', sort: 'stability', width: '8%' },
            { label: 'Avg Feat', sort: 'features', width: '8%' },
            { label: 'Cohesion', sort: 'cohesion', width: '8%' },
            { label: 'Created', width: '11%' },
            { label: 'Sample Functions', width: '18%' }
        ],
        renderer: renderClusters
    },
    '#upload': {
        title: 'Upload Binaries',
        api: null,
        headers: [],
        renderer: null
    },
    '#binary-similarity': {
        title: 'Binary Similarity',
        api: '/api/bin_sim/search',
        headers: [
            { label: 'Score', width: '10%', sort: 'score' },
            { label: 'Binary Pair', width: '20%' },
            { label: 'MD5', width: '15%' },
            { label: 'Arch', width: '8%', sort: 'architecture' },
            { label: 'Funcs', width: '8%', sort: 'functions_count' },
            { label: 'Coverage', width: '12%', sort: 'coverage' },
            { label: 'Shared Clusters', width: '7%', sort: 'shared_clusters' },
            { label: 'Tags', width: '20%' },
        ],
        renderer: renderBinSimPairs
    },
    '#bin-clusters': {
        title: 'Binary Clusters',
        api: '/api/bin_cluster/list',
        headers: [
            { label: 'UUID', sort: 'cluster_uuid', width: '10%' },
            { label: 'Name', sort: 'cluster_name', width: '25%' },
            { label: 'Binaries', sort: 'count', width: '12%' },
            { label: 'Stability', sort: 'stability', width: '8%' },
            { label: 'Cohesion', sort: 'cohesion', width: '8%' },
            { label: 'Created', width: '11%' },
            { label: 'Sample Binaries', width: '18%' }
        ],
        renderer: renderBinClusters
    },
    '#file-call-graph': {
        title: 'File Call Graph',
        api: null,
        headers: [],
        renderer: null
    },
    '#jobs': {
        title: 'Background Jobs',
        api: '/api/jobs',
        headers: [
            { label: 'ID', width: '15%' },
            { label: 'Type', width: '12%' },
            { label: 'Collection', width: '10%' },
            { label: 'Target', width: '15%' },
            { label: 'Status', width: '10%' },
            { label: 'Progress', width: '15%' },
            { label: 'Created', width: '12%' },
            { label: 'Actions', width: '11%' }
        ],
        renderer: (data) => window.renderJobs(data)
    }
};

function clearFilters() {
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);
    const newParams = new URLSearchParams();

    // Preserved context keys
    const preserved = ['collection', 'algo', 'view'];
    preserved.forEach(k => {
        if (params.has(k)) newParams.set(k, params.get(k));
    });

    // Set default cohesion threshold
    newParams.set('min_cohesion', '0.95');

    currentOffset = 0;
    isEndOfResults = false;
    const newHash = hashPath + (newParams.toString() ? '?' + newParams.toString() : '');
    window.isClearingFilters = true;
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

    populateCollectionDropdown();

    const isSilent = (full_hash === lastHashPath && !force && !append);

    if (full_hash !== lastHashPath || !append) {
        if (!isSilent) {
            currentOffset = 0;
            isEndOfResults = false;
            document.getElementById('table-body').innerHTML = '';
            document.getElementById('loader').style.display = 'block';
        }
    }
    lastHashPath = full_hash;

    const params = new URLSearchParams(queryString);
    const collection = params.get('collection') || 'main';

    if (hashPath === '#clusters' || hashPath === '#bin-clusters') {
        const viewMode = params.get('view') || 'table';
        if (viewMode !== 'hierarchy' && viewMode !== 'packing') {
            params.delete('show_parents');
            params.delete('show_children');
            params.delete('show_members');
            params.delete('path_compression');
            params.delete('show_binary_sankey');
        }
    }

    // Save search filters state (only if not collections view)
    if (hashPath !== '#collections') {
        localStorage.setItem(`savedFilters:${collection}:${hashPath}`, params.toString() || `collection=${collection}`);
        addToHistory(hashPath, params.toString());
    }

    // Ensure tag metadata is loaded for views that use it (functions, similarities, and files)
    if (hashPath === '#functions' || hashPath === '#function-similarity' || hashPath === '#binary-similarity' || hashPath === '#files') {
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
    const countLimit = parseInt(params.get('limit')) || (params.get('view') === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT);
    if (isSilent && currentOffset > 0) {
        params.set('offset', 0);
        params.set('limit', currentOffset);
    } else {
        params.set('offset', currentOffset);
        params.set('limit', countLimit);
    }

    let apiUrl = route.api + (params.toString() ? '?' + params.toString() : '');
    updateUI(hashPath, params, route);

    const isGraphView = params.get('view') === 'graph' || params.get('view') === 'hierarchy' || params.get('view') === 'packing';
    if ((isGraphView && (hashPath === '#function-similarity' || hashPath === '#binary-similarity' || hashPath === '#clusters')) || !route.api) {
        document.getElementById('loader').style.display = 'none';
        return;
    }

    try {
        const response = await fetch(apiUrl);
        if (!response.ok) {
            const text = await response.text();
            console.error(`API Error for ${apiUrl}: ${response.status} ${response.statusText}\n${text.substring(0, 500)}`);
            throw new Error(`API Error ${response.status}: ${response.statusText}`);
        }
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
                if (data.pool_truncated && (hashPath === '#function-similarity' || hashPath === '#functions')) {
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
                if (total >= currentLimit && (hashPath === '#function-similarity' || hashPath === '#functions')) {
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
        if (!append) tbody.innerHTML = '';

        if (items.length === 0 && !append) {
            tbody.innerHTML = '<tr><td colspan="100" style="text-align:center; padding:40px;">No data found</td></tr>';
        } else {
            // Pass clusters map for views that use it
            let clustersMap = undefined;
            if (hashPath === '#functions' || hashPath === '#function-similarity') {
                clustersMap = data.clusters || {};
            } else if (hashPath === '#files') {
                clustersMap = data.bin_cluster_map || {};
            }

            const html = clustersMap !== undefined ? route.renderer(items, clustersMap) : route.renderer(items);
            if (html) tbody.insertAdjacentHTML('beforeend', html);
        }

        let count = items.length;
        if (hashPath === '#jobs') {
            const jobIds = new Set(items.map(item => item.id));
            count = items.filter(item => !item.parent_id || !jobIds.has(item.parent_id)).length;
        }

        if (append) {
            currentOffset += count;
        } else {
            currentOffset = count;
        }
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
    const pathChanged = (path !== lastViewPath);
    lastViewPath = path;
    const col = params.get('collection');

    // Reset all special view containers and stop active processes
    document.getElementById('graph-view-container').style.display = 'none';
    document.getElementById('hierarchy-view-container').style.display = 'none';
    if (document.getElementById('packing-view-container')) document.getElementById('packing-view-container').style.display = 'none';
    if (document.getElementById('call-graph-view-container')) document.getElementById('call-graph-view-container').style.display = 'none';
    if (document.getElementById('upload-view-container')) document.getElementById('upload-view-container').style.display = 'none';
    if (document.getElementById('binary-similarity-container')) document.getElementById('binary-similarity-container').style.display = 'none';
    if (document.getElementById('chord-view-container')) document.getElementById('chord-view-container').style.display = 'none';
    if (document.getElementById('binary-density-view-container')) document.getElementById('binary-density-view-container').style.display = 'none';

    // Clear all autocomplete dropdowns to prevent leftovers from previous navigation
    document.querySelectorAll('.tag-autocomplete-dropdown').forEach(el => el.remove());

    const tableWrap = document.getElementById('table-wrap');
    const tableBodyWrap = document.getElementById('table-body-wrap');
    tableWrap.style.display = 'flex';
    tableWrap.style.flex = '1';
    if (tableBodyWrap) tableBodyWrap.style.display = '';

    document.getElementById('pagination-container').style.display = 'block';

    if (window.graphInstance) window.graphInstance.stop();
    if (window.hierarchyInstance) window.hierarchyInstance.stop();
    if (window.packingInstance) window.packingInstance.stop();
    if (window.callGraphInstance) window.callGraphInstance.stop();
    if (window.chordGraphInstance) window.chordGraphInstance.stop();


    if (path === '#upload') {
        tableWrap.style.display = 'none';
        if (tableBodyWrap) tableBodyWrap.style.display = 'none';
        document.getElementById('pagination-container').style.display = 'none';
        document.getElementById('upload-view-container').style.display = 'block';
        if (typeof renderUploadView === 'function') renderUploadView(params);
    } else if (path === '#file-call-graph') {
        tableWrap.style.display = 'none';
        if (tableBodyWrap) tableBodyWrap.style.display = 'none';
        document.getElementById('pagination-container').style.display = 'none';
        document.getElementById('call-graph-view-container').style.display = 'block';
        if (!window.callGraphInstance) window.callGraphInstance = new FileCallGraph('call-graph-view-container');
        window.callGraphInstance.fetch(params);
    } else if (path === '#binary-similarity') {
        document.getElementById('header-top-actions').style.display = 'flex';
    } else {
        document.getElementById('header-top-actions').style.display = 'flex';
    }

    // Sidebar
    document.querySelectorAll('nav a').forEach(a => a.classList.remove('active'));
    const navLink = document.getElementById('nav-' + path.substring(1));

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
    const viewHistoryBtnContainer = document.querySelector('.view-history-container');
    if (viewHistoryBtnContainer) {
        viewHistoryBtnContainer.style.display = path === '#collections' ? 'none' : 'block';
    }

    // Toggle download buttons visibility
    const exportContainer = document.getElementById('export-dropdown-container');
    if (exportContainer) {
        if (route.api) {
            exportContainer.style.display = 'inline-block';
        } else {
            exportContainer.style.display = 'none';
            if (typeof closeExportDropdown === 'function') closeExportDropdown();
        }
    }

    if (col) {
        const updateNavLink = (id, hash) => {
            const el = document.getElementById(id);
            if (!el) return;
            const saved = localStorage.getItem(`savedFilters:${col}:${hash}`);
            if (saved) {
                const savedParams = new URLSearchParams(saved);
                savedParams.set('collection', col);
                el.href = `${hash}?${savedParams.toString()}`;
            } else {
                el.href = `${hash}?collection=${col}`;
            }
        };

        updateNavLink('nav-collections', '#collections');
        updateNavLink('nav-batches', '#batches');
        updateNavLink('nav-files', '#files');
        updateNavLink('nav-functions', '#functions');
        updateNavLink('nav-features-global', '#features-global');
        updateNavLink('nav-function-similarity', '#function-similarity');
        updateNavLink('nav-clusters', '#clusters');
        updateNavLink('nav-bin-clusters', '#bin-clusters');
        updateNavLink('nav-binary-similarity', '#binary-similarity');
        updateNavLink('nav-chord-map', '#chord-map');
        updateNavLink('nav-jobs', '#jobs');
        updateNavLink('nav-upload', '#upload');

        const fileMd5 = params.get('file_md5');
        const cgNav = document.getElementById('nav-file-call-graph');
        if (cgNav) {
            const saved = localStorage.getItem(`savedFilters:${col}:#file-call-graph`);
            if (saved) {
                const savedParams = new URLSearchParams(saved);
                savedParams.set('collection', col);
                cgNav.href = `#file-call-graph?${savedParams.toString()}`;
            } else {
                let href = `#file-call-graph?collection=${col}`;
                if (fileMd5) href += `&file_md5=${fileMd5}`;
                cgNav.href = href;
            }
        }
    }

    if (path === '#function-similarity' && params.get('view') === 'graph') {
        restoreGraphSettings();
    }

    // Table Head
    const thead = document.getElementById('table-head');
    const dataTable = document.getElementById('data-table');
    const dataTableHeader = document.getElementById('data-table-header');
    let headHtml = '<tr>';

    const savedForRoute = JSON.parse(localStorage.getItem('columnWidths') || '{}')[path];
    const hasSavedWidths = savedForRoute && Object.keys(savedForRoute).length > 0;
    const hasWidths = route.headers.some(h => typeof h === 'object' && h.width) || hasSavedWidths;

    const tableLayout = hasWidths ? 'fixed' : 'auto';
    if (dataTable) dataTable.style.tableLayout = tableLayout;
    if (dataTableHeader) dataTableHeader.style.tableLayout = tableLayout;
    route.headers.forEach(h => {
        const label = typeof h === 'string' ? h : h.label;
        const sortKey = typeof h === 'object' ? h.sort : null;
        let width = typeof h === 'object' ? h.width : 'auto';

        // Apply saved width if exists
        const savedWidth = getSavedColumnWidth(path, label);
        if (savedWidth) width = savedWidth;

        let style = width !== 'auto' ? `style="width:${width}"` : '';
        const resizerHtml = `<div class="resizer"></div>`;

        if (sortKey) {
            const currentSort = params.get('sort_by');
            const currentOrder = params.get('sort_order') || 'desc';
            const icon = (currentSort === sortKey) ? (currentOrder === 'desc' ? '▼' : '▲') : '↕';
            headHtml += `<th ${style} class="sortable resizable-th" data-label="${label}" onclick="toggleSort('${sortKey}')">${label} <small>${icon}</small>${resizerHtml}</th>`;
        } else {
            headHtml += `<th ${style} class="resizable-th" data-label="${label}">${label}${resizerHtml}</th>`;
        }
    });
    headHtml += '</tr>';
    thead.innerHTML = headHtml;

    // Reset UI settings and display containers to defaults for all views
    const settingsEl = document.getElementById('search-settings-container');
    settingsEl.style.display = 'none';
    settingsEl.innerHTML = '';
    if (path !== '#upload' && path !== '#file-call-graph') {
        tableWrap.style.display = 'flex';
        tableWrap.style.flex = '1';
        if (tableBodyWrap) tableBodyWrap.style.display = '';
        document.getElementById('pagination-container').style.display = 'block';
    }
    document.getElementById('graph-view-container').style.display = 'none';
    document.getElementById('hierarchy-view-container').style.display = 'none';
    if (document.getElementById('packing-view-container')) document.getElementById('packing-view-container').style.display = 'none';

    if (path === '#function-similarity' || path === '#functions' || path === '#files' || path === '#clusters' || path === '#bin-clusters' || path === '#features-global' || path === '#binary-similarity' || path === '#jobs') {
        const applyFn = path === '#function-similarity' ? 'applySimSearch' : (path === '#functions' ? 'applyAdvancedFuncSearch' : (path === '#files' ? 'applyAdvancedFileSearch' : (path === '#features-global' ? 'applyAdvancedFeatureSearch' : (path === '#binary-similarity' ? 'applyBinSimSearch' : (path === '#bin-clusters' ? 'applyBinClusterSearch' : (path === '#clusters' ? 'applyClusterSearch' : 'applyJobSearch'))))));

        let settingsHtml = '';
        if (path === '#function-similarity' || path === '#functions' || path === '#files' || path === '#clusters' || path === '#bin-clusters' || path === '#features-global' || path === '#binary-similarity' || path === '#jobs') {
            settingsEl.style.display = 'flex';

            if (path === '#jobs') {
                settingsHtml += `
                    <div style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" id="job-auto-refresh" ${localStorage.getItem('jobAutoRefresh') !== 'false' ? 'checked' : ''} onchange="localStorage.setItem('jobAutoRefresh', this.checked)" style="cursor:pointer; vertical-align:middle;">
                        <label for="job-auto-refresh" style="font-size:0.75rem; color:var(--text); cursor:pointer; font-weight:bold;">Auto-Refresh</label>
                    </div>`;
            } else {
                const viewMode = params.get('view') || 'table';
                const poolLimit = params.get('pool_limit') || '1000000';
                const countLimit = params.get('limit') || (viewMode === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT);

                if (path === '#function-similarity') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchSimView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'graph' ? 'active' : ''}" onclick="switchSimView('graph')">Graph</button>
                        </div>`;
                } else if (path === '#binary-similarity') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchBinSimView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'graph' ? 'active' : ''}" onclick="switchBinSimView('graph')">Graph</button>
                        </div>`;
                } else if (path === '#clusters') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchClusterView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'hierarchy' ? 'active' : ''}" onclick="switchClusterView('hierarchy')">Graph</button>
                            <button class="view-btn ${viewMode === 'packing' ? 'active' : ''}" onclick="switchClusterView('packing')">Packing</button>
                        </div>`;
                } else if (path === '#bin-clusters') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchBinClusterView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'hierarchy' ? 'active' : ''}" onclick="switchBinClusterView('hierarchy')">Graph</button>
                            <button class="view-btn ${viewMode === 'packing' ? 'active' : ''}" onclick="switchBinClusterView('packing')">Packing</button>
                        </div>`;
                }

                if (path === '#function-similarity' || path === '#functions') {
                    settingsHtml += `
                        <span class="dim" style="font-size:0.65rem; margin-left:15px;">Pool Limit:</span>
                        <div style="position:relative; display:inline-flex; align-items:center;">
                            <input type="number" id="sim-pool-limit" value="${poolLimit}" step="100000" min="1000" max="1000000" 
                                title="Max candidates to score / filter" 
                                style="width:70px; background:rgba(0,0,0,0.3); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                                onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})">
                            <span id="pool-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Pool Truncated: Not all candidates were scored.">⚠️</span>
                        </div>`;
                }

                settingsHtml += `
                    <span class="dim" style="font-size:0.65rem; margin-left:15px;">Limit:</span>
                    <div style="position:relative; display:inline-flex; align-items:center;">
                        <input type="number" id="sim-limit" value="${countLimit}" step="10" min="1" max="50000" 
                            title="Max results to display (Output Limit)" 
                            style="width:60px; background:rgba(0,0,0,0.3); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                            onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})">
                        <span id="limit-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Output Limit Reached: Results are capped.">ℹ️</span>
                    </div>
                `;
            }
            settingsEl.innerHTML = settingsHtml;
        } else {
            settingsEl.style.display = 'none';
        }

        const tableWrap = document.getElementById('table-wrap');
        const tableBodyWrap = document.getElementById('table-body-wrap');
        const tbody = document.getElementById('table-body');
        const pag = document.getElementById('pagination-container');
        const gview = document.getElementById('graph-view-container');

        if (path === '#function-similarity' || path === '#binary-similarity') {
            const viewMode = params.get('view') || 'table';
            if (viewMode === 'graph') {
                tableWrap.style.display = 'flex';
                tableWrap.style.flex = 'none';
                if (tableBodyWrap) tableBodyWrap.style.display = 'none';
                pag.style.display = 'none';

                if (path === '#function-similarity') {
                    gview.style.display = 'flex';
                    loadGraphView(params);
                } else {
                    const chordView = document.getElementById('chord-view-container');
                    if (chordView) chordView.style.display = 'flex';
                    loadChordView(params);
                }
            } else {
                tableWrap.style.display = 'flex';
                tableWrap.style.flex = '1';
                if (tableBodyWrap) tableBodyWrap.style.display = '';
                pag.style.display = 'block';
                gview.style.display = 'none';
                const chordView = document.getElementById('chord-view-container');
                if (chordView) chordView.style.display = 'none';
            }
        } else {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = '1';
            if (tableBodyWrap) tableBodyWrap.style.display = '';
            pag.style.display = 'block';
            if (gview) gview.style.display = 'none';
            const chordView = document.getElementById('chord-view-container');
            if (chordView) chordView.style.display = 'none';
        }

        const p = new URLSearchParams(params);

        if (path === '#files' || path === '#functions' || path === '#function-similarity' || path === '#features-global') {
            headHtml += `<tr class="filter-row">`;

            if (path === '#features-global') {
                headHtml += `
                    <th>
                        <input type="text" id="flt-feat-hash" placeholder="Hash..." value="${p.get('hash') || ''}" onfocus="attachAutocomplete(this, 'feature', 'hash', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            <input type="text" id="flt-feat-type" placeholder="Type..." value="${p.get('type') || ''}" onfocus="attachAutocomplete(this, 'feature', 'type', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                            <input type="text" id="flt-feat-op" placeholder="Op..." value="${p.get('op') || ''}" onfocus="attachAutocomplete(this, 'feature', 'op', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th></th>
                    <th></th>
                    <th>
                        <div style="display:flex; align-items:center; gap:2px;">
                            <input type="number" id="flt-feat-min-tf" placeholder="Min..." value="${p.get('min_tf_score') || ''}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            <span class="dim" style="font-size:0.6rem">-</span>
                            <input type="number" id="flt-feat-max-tf" placeholder="Max..." value="${p.get('max_tf_score') || ''}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th>
                        <div style="display:flex; align-items:center; gap:2px;">
                            <input type="number" id="flt-feat-min-freq" placeholder="Min..." value="${p.get('min_frequency') || ''}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            <span class="dim" style="font-size:0.6rem">-</span>
                            <input type="number" id="flt-feat-max-freq" placeholder="Max..." value="${p.get('max_frequency') || ''}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th></th>
                `;
            } else if (path === '#files') {
                headHtml += `
                    <th>
                        <input type="text" id="flt-file-name" placeholder="Name..." value="${p.get('file_name') || ''}" onfocus="attachAutocomplete(this, 'file', 'file_name', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            <input type="text" id="flt-file-md5" placeholder="MD5..." value="${p.get('file_md5') || ''}" onfocus="attachAutocomplete(this, 'file', 'file_md5', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                            <input type="text" id="flt-file-language" placeholder="Lang..." value="${p.get('language_id') || ''}" onfocus="attachAutocomplete(this, 'file', 'language_id', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            <input type="text" id="flt-file-yara" placeholder="Yara..." value="${p.get('yara') || ''}" onfocus="attachAutocomplete(this, 'file', 'yara', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            <input type="text" id="flt-file-avtype" placeholder="AVType..." value="${p.get('avtype') || ''}" onfocus="attachAutocomplete(this, 'file', 'avtype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            <input type="text" id="flt-file-ccip" placeholder="CC IP..." value="${p.get('cc_ip') || ''}" onfocus="attachAutocomplete(this, 'file', 'cc_ip', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            <hr style="margin: 2px 0; border: none; border-top: 1px solid rgba(255,255,255,0.1);">
                            <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 2px;">
                                <input type="text" id="flt-file-inf-yara" placeholder="Inf.Yara" title="Inferred Yara" value="${p.get('inferred_yara') || ''}" onfocus="attachAutocomplete(this, 'file', 'inferred_yara', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                <input type="text" id="flt-file-inf-avtype" placeholder="Inf.AV" title="Inferred AVType" value="${p.get('inferred_avtype') || ''}" onfocus="attachAutocomplete(this, 'file', 'inferred_avtype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                            </div>
                            <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 2px;">
                                <input type="text" id="flt-file-inf-type" placeholder="Inf.Type" title="Inferred Type" value="${p.get('inferred_filetype') || ''}" onfocus="attachAutocomplete(this, 'file', 'inferred_filetype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                <input type="text" id="flt-file-inf-ccip" placeholder="Inf.IP" title="Inferred CC IP" value="${p.get('inferred_ccip') || ''}" onfocus="attachAutocomplete(this, 'file', 'inferred_ccip', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                            </div>
                        </div>
                    </th>
                    <th>
                        <input type="text" id="flt-file-batch" placeholder="Batch UUID..." value="${p.get('batch_uuid') || ''}" onfocus="attachAutocomplete(this, 'file', 'batch_uuid', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    </th>
                    <th>
                        <div style="display:flex; align-items:center; gap:2px;">
                            <input type="number" id="flt-file-min-funcs" placeholder="Min..." value="${p.get('min_function_count') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            <span class="dim" style="font-size:0.6rem">-</span>
                            <input type="number" id="flt-file-max-funcs" placeholder="Max..." value="${p.get('max_function_count') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:2px;">
                            <input type="text" id="flt-file-cluster" placeholder="UUID..." value="${p.get('bin_cluster_uuid') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                            <input type="text" id="flt-file-cluster-name" placeholder="Name..." value="${p.get('bin_cluster_name') || ''}" onfocus="attachAutocomplete(this, 'file', 'bin_cluster_name', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                            <div style="display:flex; align-items:center; gap:2px;">
                                <input type="number" id="flt-file-min-cohesion" placeholder="Min coh..." value="${p.get('min_cohesion') || ''}" step="0.05" min="0" max="1" title="Min Cluster Cohesion" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                <span class="dim" style="font-size:0.6rem">-</span>
                                <input type="number" id="flt-file-max-cohesion" placeholder="Max coh..." value="${p.get('max_cohesion') || ''}" step="0.05" min="0" max="1" title="Max Cluster Cohesion" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            </div>
                        </div>
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:2px;">
                            <input type="text" id="flt-file-min-date" placeholder="Min Date..." value="${p.get('min_entry_date') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            <input type="text" id="flt-file-max-date" placeholder="Max Date..." value="${p.get('max_entry_date') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                        </div>
                    </th>
                    <th style="position:relative">
                        <div class="tag-filter-container" id="tag-container-file">
                            <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })">
                        </div>
                    </th>`;
            } else if (path === '#function-similarity' || path === '#functions') {
                if (path === '#function-similarity') {
                    headHtml += `
                        <th style="vertical-align: middle;">
                            <div style="display:flex; align-items:center; gap:2px;">
                                <input type="number" id="sim-min-score" value="${p.get('min_score') || '0.95'}" step="0.05" min="0" max="1" title="Min Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                                <span class="dim" style="font-size:0.6rem">-</span>
                                <input type="number" id="sim-max-score" value="${p.get('max_score') || '1.0'}" step="0.05" min="0" max="1" title="Max Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                            </div>
                            <div class="tag-filter-container" id="tag-container-sim">
                                <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'sim')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('sim', 'sim_tag', val); this.value=''; triggerTagSearch(); })">
                            </div>
                        </th>`;
                }

                // Common Fields (Function Name, Namespace, Return Type)
                const nameVal = path === '#function-similarity' ? p.get('name') : p.get('function_name');
                headHtml += `
                    <th>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            <input type="text" id="flt-func-name" placeholder="Name..." value="${nameVal || ''}" onfocus="attachAutocomplete(this, 'func', 'function_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                            <div style="display:flex; gap:2px;">
                                <input type="text" id="flt-func-namespace" placeholder="Namespace..." value="${p.get('namespace') || ''}" onfocus="attachAutocomplete(this, 'func', 'namespace', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                                <input type="text" id="flt-func-ret_type" placeholder="Return Type..." value="${p.get('return_type') || p.get('ret_type') || ''}" onfocus="attachAutocomplete(this, 'func', 'return_type', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                            </div>
                        </div>
                    </th>`;

                // Address
                const addrVal = path === '#function-similarity' ? p.get('address') : p.get('entrypoint_address');
                headHtml += `
                    <th>
                        <input type="text" id="flt-func-address" placeholder="Addr..." value="${addrVal || ''}" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    </th>`;

                // Tags, Clusters, Features, File Info
                headHtml += `
                    <th style="position:relative">
                        <div class="tag-filter-container" id="tag-container-func">
                            <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'func')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('func', 'func_tag', val); this.value=''; triggerTagSearch(); })">
                        </div>
                    </th>
                    <th>
                        <div style="display:flex; flex-direction:column; gap:2px;">
                            <input type="text" id="flt-func-cluster" placeholder="UUID..." value="${p.get('cluster_uuid') || ''}" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                            <input type="text" id="flt-func-cluster-name" placeholder="Name..." value="${p.get('cluster_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'cluster_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                            <input type="number" id="flt-func-min-cohesion" placeholder="Min cohesion..." value="${p.get('min_cohesion') || '0.95'}" step="0.05" min="0" max="1" title="Min Cluster Cohesion" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                        </div>
                    </th>
                    <th><input type="number" id="flt-func-min-features" value="${p.get('min_features') || '0'}" min="0" title="Min Features" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th><input type="text" id="flt-func-note-owner" placeholder="Note Owner..." value="${p.get('note_owner') || ''}" onfocus="attachAutocomplete(this, 'func', 'note_owners', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width:100%; font-size:0.6rem; box-sizing: border-box;"></th>
                    <th><input type="text" id="flt-func-file_name" placeholder="Name..." value="${p.get('file_name') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box;"></th>
                    <th><input type="text" id="flt-func-md5" placeholder="MD5..." value="${p.get('file_md5') || p.get('md5') || ''}" onfocus="attachAutocomplete(this, 'func', 'file_md5', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box;"></th>
                    <th style="position:relative">
                        <div class="tag-filter-container" id="tag-container-file">
                            <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })">
                        </div>
                    </th>
                    <th><input type="text" id="flt-func-language" placeholder="Lang..." value="${p.get('language_id') || p.get('language') || ''}" onfocus="attachAutocomplete(this, 'func', 'language_id', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>`;

                if (path === '#function-similarity') {
                    headHtml += `
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
                                <select id="sim-match-mode" onchange="applySimSearch()" style="width:100%; background:#000; color:var(--text); border:1px solid #333; font-size:0.6rem; border-radius:2px; margin-top:4px;">
                                    <option value="any" ${(p.get('match_mode') || 'any') === 'any' ? 'selected' : ''}>Match Any Function</option>
                                    <option value="both" ${p.get('match_mode') === 'both' ? 'selected' : ''}>Match Both Functions</option>
                                </select>
                            </div>
                        </th>`;
                } else {
                    headHtml += `<th></th><th></th>`;
                }
            }

            headHtml += `</tr>`;
            thead.innerHTML = headHtml;

            if (path === '#files') {
                loadFieldCardinalities(col, 'file', {
                    'file_name': 'flt-file-name',
                    'file_md5': 'flt-file-md5',
                    'language_id': 'flt-file-language',
                    'batch_uuid': 'flt-file-batch',
                    'bin_cluster_name': 'flt-file-cluster-name'
                });
            } else {
                loadFieldCardinalities(col, 'func', {
                    'function_name': 'flt-func-name',
                    'file_name': 'flt-func-file_name',
                    'file_md5': 'flt-func-md5',
                    'return_type': 'flt-func-ret_type',
                    'language_id': 'flt-func-language',
                    'namespace': 'flt-func-namespace'
                });
            }

            if (path === '#features-global') {
                loadFieldCardinalities(col, 'feature', {
                    'hash': 'flt-feat-hash',
                    'type': 'flt-feat-type',
                    'op': 'flt-feat-op'
                });
            }
        }
    }
    if (path === '#clusters') {
        const p = new URLSearchParams(params);
        if (dataTable) dataTable.style.tableLayout = 'fixed';
        if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';
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
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-cluster-func-name" placeholder="Func Name..." value="${p.get('func_name') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                    <input type="text" id="flt-cluster-func-addr" placeholder="Func Addr..." value="${p.get('func_addr') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                    <input type="text" id="flt-cluster-file-name" placeholder="File Name..." value="${p.get('file_name') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                </div>
            </th>
        </tr>`;
        thead.innerHTML = headHtml;
    }
    if (path === '#jobs') {
        const p = new URLSearchParams(params);
        if (dataTable) dataTable.style.tableLayout = 'fixed';
        if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';

        const statuses = ['', 'pending', 'running', 'completed', 'failed', 'cancelled'];
        const statusOptions = statuses.map(s => {
            const label = s ? s.toUpperCase() : 'All Statuses';
            return `<option value="${s}" ${p.get('status') === s ? 'selected' : ''}>${label}</option>`;
        }).join('');

        const types = ['', 'pipeline', 'group', 'file_data_ingest', 'ghidra_analyze', 'idx_meta', 'idx_functions', 'idx_features', 'build_sim', 'cluster_functions', 'cluster_binaries', 'enrich_features'];
        const typeOptions = types.map(t => {
            const label = t ? t.replace(/_/g, ' ').toUpperCase() : 'All Types';
            return `<option value="${t}" ${p.get('type') === t ? 'selected' : ''}>${label}</option>`;
        }).join('');

        headHtml += `<tr class="filter-row">
            <th></th>
            <th>
                <select id="job-type-filter" onchange="applyJobSearch()" style="background:#000; border:1px solid #333; color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">
                    ${typeOptions}
                </select>
            </th>
            <th>
                <select id="job-collection-filter" onchange="applyJobSearch()" style="background:#000; border:1px solid #333; color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">
                    <option value="">All Collections</option>
                </select>
            </th>
            <th></th>
            <th>
                <select id="job-status-filter" onchange="applyJobSearch()" style="background:#000; border:1px solid #333; color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">
                    ${statusOptions}
                </select>
            </th>
            <th></th>
            <th></th>
            <th></th>
        </tr>`;
        thead.innerHTML = headHtml;

        fetch('/api/collection/search')
            .then(res => res.json())
            .then(data => {
                const collections = data.collections || (Array.isArray(data) ? data : []);
                const select = document.getElementById('job-collection-filter');
                if (select) {
                    const currentVal = p.get('collection') || '';
                    select.innerHTML = `<option value="">All Collections</option>` + collections.map(c =>
                        `<option value="${c.name}" ${currentVal === c.name ? 'selected' : ''}>${c.name}</option>`
                    ).join('');
                }
            })
            .catch(err => console.error('Error fetching collections for jobs filter:', err));
    }

    if (path === '#binary-similarity') {
        const p = new URLSearchParams(params);
        headHtml += `<tr class="filter-row">
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <select id="bsim-score-type" onchange="debouncedSearch(applyBinSimSearch)" style="font-size:0.6rem; padding:2px; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:3px;">
                        <option value="score" ${p.get('sort') === 'score' || !p.get('sort') ? 'selected' : ''}>Unweighted</option>
                        <option value="score_sim_weighted" ${p.get('sort') === 'score_sim_weighted' ? 'selected' : ''}>Sim Weighted</option>
                        <option value="score_collection_weighted" ${p.get('sort') === 'score_collection_weighted' ? 'selected' : ''}>Col Weighted</option>
                    </select>
                    <div style="display:flex; align-items:center; gap:2px;">
                        <input type="number" id="bsim-min-score" placeholder="Min..." step="0.05" min="0" max="1" value="${p.get('min_score') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                        <span class="dim" style="font-size:0.6rem">-</span>
                        <input type="number" id="bsim-max-score" placeholder="Max..." step="0.05" min="0" max="1" value="${p.get('max_score') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                    </div>
                </div>
            </th>
            <th>
                <input type="text" id="bsim-file-name" placeholder="File Name..." value="${p.get('file_name') || ''}" onfocus="attachAutocomplete(this, 'file', 'file_name', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;">
            </th>
            <th>
                <input type="text" id="bsim-md5" placeholder="MD5..." value="${p.get('md5') || ''}" onfocus="attachAutocomplete(this, 'file', 'file_md5', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.6rem; width:100%; box-sizing:border-box; font-family:monospace;">
            </th>
            <th>
                <input type="text" id="bsim-arch" placeholder="Arch..." value="${p.get('arch') || ''}" onfocus="attachAutocomplete(this, 'file', 'language_id', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.6rem; width:100%; box-sizing:border-box;">
            </th>
            <th>
                <div style="display:flex; align-items:center; gap:2px;">
                    <input type="number" id="bsim-min-funcs" placeholder="Min..." min="0" value="${p.get('min_funcs') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                    <span class="dim" style="font-size:0.6rem">-</span>
                    <input type="number" id="bsim-max-funcs" placeholder="Max..." min="0" value="${p.get('max_funcs') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                </div>
            </th>
            <th>
                <div style="display:flex; align-items:center; gap:2px;">
                    <input type="number" id="bsim-min-cov" placeholder="Min..." step="0.1" min="0" max="1" value="${p.get('min_coverage') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                    <span class="dim" style="font-size:0.6rem">-</span>
                    <input type="number" id="bsim-max-cov" placeholder="Max..." step="0.1" min="0" max="1" value="${p.get('max_coverage') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                </div>
            </th>
            <th>
                <input type="number" id="bsim-min-shared" placeholder="Min..." min="0" value="${p.get('min_shared') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;">
            </th>
            <th style="position:relative">
                <div class="tag-filter-container" id="tag-container-bin-sim">
                    <input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'bin-sim')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('bin-sim', 'file_tag', val); this.value=''; triggerTagSearch(); })">
                </div>
            </th>
        </tr>`;
        thead.innerHTML = headHtml;
    }

    if (path === '#clusters') {
        const viewMode = params.get('view') || 'table';
        const tableWrap = document.getElementById('table-wrap');
        const tableBodyWrap = document.getElementById('table-body-wrap');
        const hview = document.getElementById('hierarchy-view-container');
        const pview = document.getElementById('packing-view-container');
        const pag = document.getElementById('pagination-container');

        if (viewMode === 'hierarchy') {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = 'none';
            if (tableBodyWrap) tableBodyWrap.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'flex';
            if (pview) pview.style.display = 'none';
            loadHierarchyView(params);
        } else if (viewMode === 'packing') {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = 'none';
            if (tableBodyWrap) tableBodyWrap.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'flex';
            loadPackingView(params);
        } else {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = '1';
            if (tableBodyWrap) tableBodyWrap.style.display = '';
            pag.style.display = 'block';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'none';
        }
    }
    if (path === '#bin-clusters') {
        const p = new URLSearchParams(params);
        const viewMode = p.get('view') || 'table';
        const nameType = p.get('cluster_name_type') || 'file';
        if (dataTable) dataTable.style.tableLayout = 'fixed';
        if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';
        headHtml += `<tr class="filter-row">
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-bin-cluster-uuid" placeholder="UUID..." value="${p.get('cluster_uuid') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    <input type="text" id="flt-bin-cluster-id" placeholder="ID..." value="${p.get('cluster_id') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                </div>
            </th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-bin-cluster-name" placeholder="Name..." value="${p.get('cluster_name') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                    <select id="bin-cluster-name-type" style="background:rgba(0,0,0,0.3); color:var(--accent); border:1px solid var(--accent); font-size:0.6rem; border-radius:4px; padding:2px; width:100%; box-sizing:border-box;" onchange="changeBinClusterNameType(this.value)">
                        <option value="file" ${nameType === 'file' ? 'selected' : ''}>Most Common File Name</option>
                        <option value="yara" ${nameType === 'yara' ? 'selected' : ''}>Most Common Yara</option>
                    </select>
                </div>
            </th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="number" id="flt-bin-cluster-min-count" value="${p.get('min_count') || '0'}" min="0" placeholder="Min" title="Min Binaries" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;">
                    <input type="number" id="flt-bin-cluster-max-count" value="${p.get('max_count') || ''}" min="0" placeholder="Max" title="Max Binaries" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;">
                </div>
            </th>
            <th><input type="number" id="flt-bin-cluster-min-stability" value="${p.get('min_stability') || '0'}" step="0.1" min="0" title="Min Stability" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="number" id="flt-bin-cluster-min-cohesion" value="${p.get('min_cohesion') || '0'}" step="0.1" min="0" max="1" placeholder="Min" title="Min Cohesion" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;">
                    <input type="number" id="flt-bin-cluster-max-cohesion" value="${p.get('max_cohesion') || ''}" step="0.1" min="0" max="1" placeholder="Max" title="Max Cohesion" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;">
                </div>
            </th>
            <th></th>
            <th>
                <div style="display:flex; flex-direction:column; gap:2px;">
                    <input type="text" id="flt-bin-cluster-file-name" placeholder="File Name..." value="${p.get('file_name') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                    <input type="text" id="flt-bin-cluster-file-md5" placeholder="MD5..." value="${p.get('file_md5') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                </div>
            </th>
        </tr>`;
        thead.innerHTML = headHtml;

        const tableWrap = document.getElementById('table-wrap');
        const tableBodyWrap = document.getElementById('table-body-wrap');
        const hview = document.getElementById('hierarchy-view-container');
        const pview = document.getElementById('packing-view-container');
        const pag = document.getElementById('pagination-container');

        if (viewMode === 'hierarchy') {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = 'none';
            if (tableBodyWrap) tableBodyWrap.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'flex';
            if (pview) pview.style.display = 'none';
            loadBinHierarchyView(params);
        } else if (viewMode === 'packing') {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = 'none';
            if (tableBodyWrap) tableBodyWrap.style.display = 'none';
            pag.style.display = 'none';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'flex';
            loadBinPackingView(params);
        } else {
            tableWrap.style.display = 'flex';
            tableWrap.style.flex = '1';
            if (tableBodyWrap) tableBodyWrap.style.display = '';
            pag.style.display = 'block';
            hview.style.display = 'none';
            if (pview) pview.style.display = 'none';
        }
    } else if (path === '#file-call-graph') {
        const tableWrap = document.getElementById('table-wrap');
        const tableBodyWrap = document.getElementById('table-body-wrap');
        const pag = document.getElementById('pagination-container');
        const cgview = document.getElementById('call-graph-view-container');

        tableWrap.style.display = 'none';
        if (tableBodyWrap) tableBodyWrap.style.display = 'none';
        pag.style.display = 'none';
        cgview.style.display = 'flex';
        loadCallGraphView(params);
    }

    // Search Bar for Files
    const searchArea = document.getElementById('search-area');
    if (path === '#files') {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar">
            <div class="search-input-wrapper">
                <input type="text" id="file-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)">
                <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFileSearch()" title="Search"></i>
            </div>
        </div>`;

    } else if (path === '#functions') {
        const p = new URLSearchParams(params);
        const fileMd5 = p.get('file_md5');
        const callGraphBtn = fileMd5 ? `<a class="btn-action" href="#file-call-graph?collection=${p.get('collection')}&file_md5=${fileMd5}" style="color:var(--accent); margin-left:10px; padding: 6px 12px; border:1px solid var(--accent); border-radius:4px; font-size:0.8rem;">View File Call Graph 🕸️</a>` : '';

        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="func-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFuncSearch()" title="Search"></i>
                </div>
                ${callGraphBtn}
            </div>
        </div>`;
    } else if (path === '#features-global' && !document.getElementById('feature-search-input')) {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="feature-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFeatureSearch()" title="Search"></i>
                </div>
            </div>
        </div>`;
    } else if (path === '#function-similarity') {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="sim-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applySimSearch()" title="Search"></i>
                </div>
            </div>
        </div>`;
    } else if (path === '#clusters' && !document.getElementById('cluster-search-input')) {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar">
            <div class="search-input-wrapper">
                <input type="text" id="cluster-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)">
                <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyClusterSearch()" title="Search"></i>
            </div>
        </div>`;
    } else if (path === '#bin-clusters' && !document.getElementById('bin-cluster-search-input')) {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar">
            <div class="search-input-wrapper">
                <input type="text" id="bin-cluster-search-input" placeholder="Search by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)">
                <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyBinClusterSearch()" title="Search"></i>
            </div>
        </div>`;
    } else if (path === '#binary-similarity') {
        const p = new URLSearchParams(params);
        searchArea.innerHTML = `<div class="filter-bar" style="gap:20px">
            <div style="display:flex; gap:10px; align-items:center;">
                <div class="search-input-wrapper">
                    <input type="text" id="bsim-search-input" placeholder="Search similarities by keywords..." autofocus value="${p.get('q') || ''}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)">
                    <i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyBinSimSearch()" title="Search"></i>
                </div>
            </div>
        </div>`;
    } else if (path !== '#files' && path !== '#functions' && path !== '#features-global' && path !== '#clusters' && path !== '#bin-clusters' && path !== '#function-similarity' && path !== '#binary-similarity') {
        searchArea.innerHTML = '';
    }

    // Re-inject tags for all views that support them
    setTimeout(() => {
        const tagSearchParams = new URLSearchParams(params);
        const tagFields = [
            { key: 'sim', fields: ['sim_tag', 'sim_static_tag', 'sim_user_tag', 'exclude_sim_tag', 'exclude_sim_static_tag', 'exclude_sim_user_tag'] },
            { key: 'func', fields: ['func_tag', 'func_static_tag', 'func_user_tag', 'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] },
            { key: 'file', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'] },
            { key: 'bin-sim', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] }
        ];
        tagFields.forEach(col => {
            col.fields.forEach(f => {
                const values = tagSearchParams.getAll(f);
                const isEx = f.startsWith('exclude_');
                const baseType = isEx ? f.substring(8) : f;
                values.forEach(v => {
                    if (v) createTagCard(col.key, baseType, v, isEx);
                });
            });
        });
    }, 0);

    // Sync body colgroup from the header row's actual rendered widths.
    // We use requestAnimationFrame so the header table has laid out first.
    const syncColgroups = () => {
        const headerTable = document.getElementById('data-table-header');
        const bodyColgroup = document.getElementById('table-colgroup');
        if (!headerTable || !bodyColgroup) return;

        const headerRow = thead.querySelector('tr:first-child');
        if (!headerRow) return;
        const ths = headerRow.querySelectorAll('th');

        // Also rebuild the header colgroup
        const headerColgroup = document.getElementById('table-colgroup-header');
        if (headerColgroup) {
            headerColgroup.innerHTML = '';
            ths.forEach(th => {
                const col = document.createElement('col');
                if (th.style.width) col.style.width = th.style.width;
                headerColgroup.appendChild(col);
            });
        }

        // Read actual rendered widths after layout and apply to body colgroup
        requestAnimationFrame(() => {
            bodyColgroup.innerHTML = '';
            ths.forEach(th => {
                const col = document.createElement('col');
                col.style.width = th.getBoundingClientRect().width + 'px';
                bodyColgroup.appendChild(col);
            });
        });
    };
    syncColgroups();

    // Initialize resizers - MUST BE DONE AFTER ALL thead.innerHTML UPDATES
    thead.querySelectorAll('.resizable-th').forEach(th => {
        initColumnResize(th, path, th.dataset.label);
    });

    // Automatically focus the active search input ONLY when switching views
    const searchInput = searchArea.querySelector('input[type="text"]');
    if (pathChanged && searchInput) {
        searchInput.focus();
        // Move cursor to end if there's text
        const val = searchInput.value;
        if (val) {
            searchInput.value = '';
            searchInput.value = val;
        }
    } else if (currentFocusId) {
        // Restore focus and selection after re-render if it was the search bar
        const focusedEl = document.getElementById(currentFocusId);
        if (focusedEl) {
            focusedEl.focus();
            try {
                if (focusedEl.setSelectionRange) {
                    focusedEl.setSelectionRange(preservedSelection.start, preservedSelection.end);
                }
            } catch (err) {
                // Ignore if the element doesn't support selection (number, etc.)
            }
        }
        // Reset after restoration
        currentFocusId = null;
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
        if (currentOrder === 'desc') {
            params.set('sort_order', 'asc');
        } else {
            // Third click: remove sorting
            params.delete('sort_by');
            params.delete('sort_order');
        }
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

    const nameFlt = document.getElementById('flt-func-name')?.value;
    const addressFlt = document.getElementById('flt-func-address')?.value;
    const nsFlt = document.getElementById('flt-func-namespace')?.value;
    const retTypeFlt = document.getElementById('flt-func-ret_type')?.value;
    const fileNameFlt = document.getElementById('flt-func-file_name')?.value;
    const md5Flt = document.getElementById('flt-func-md5')?.value;
    const langFlt = document.getElementById('flt-func-language')?.value;
    const clusterFlt = document.getElementById('flt-func-cluster')?.value;
    const clusterNameFlt = document.getElementById('flt-func-cluster-name')?.value;
    const minCohesionFlt = document.getElementById('flt-func-min-cohesion')?.value;
    const minFeatFlt = document.getElementById('flt-func-min-features')?.value;
    const noteOwnerFlt = document.getElementById('flt-func-note-owner')?.value;

    if (clusterFlt) params.set('cluster_uuid', clusterFlt); else params.delete('cluster_uuid');
    if (clusterNameFlt) params.set('cluster_name', clusterNameFlt); else params.delete('cluster_name');
    params.set('min_cohesion', minCohesionFlt || '0.95');

    if (nameFlt) params.set('function_name', nameFlt); else params.delete('function_name');
    if (addressFlt) params.set('entrypoint_address', addressFlt); else params.delete('entrypoint_address');
    if (nsFlt) params.set('namespace', nsFlt); else params.delete('namespace');
    if (retTypeFlt) params.set('return_type', retTypeFlt); else params.delete('return_type');
    if (fileNameFlt) params.set('file_name', fileNameFlt); else params.delete('file_name');
    if (md5Flt) params.set('file_md5', md5Flt); else params.delete('file_md5');
    if (langFlt) params.set('language_id', langFlt); else params.delete('language_id');
    if (minFeatFlt) params.set('min_features', minFeatFlt); else params.delete('min_features');
    if (noteOwnerFlt) params.set('note_owner', noteOwnerFlt); else params.delete('note_owner');
    const poolLimit = document.getElementById('sim-pool-limit')?.value;
    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('pool_limit', poolLimit || '1000000');
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

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

function switchBinSimView(mode) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    params.set('view', mode);
    window.location.hash = `#binary-similarity?${params.toString()}`;
}

function loadGraphView(params) {
    document.getElementById('graph-view-container').style.display = 'flex';
    if (!window.graphInstance) {
        window.graphInstance = new SimilarityGraph('bk-similarity-plot');
        document.getElementById('graph-stop-btn').onclick = () => window.graphInstance.stop();
    }
    window.graphInstance.fetch(params);
}

function loadChordView(params) {
    document.getElementById('chord-view-container').style.display = 'flex';
    if (!window.chordGraphInstance) {
        window.chordGraphInstance = new ChordGraph('chord-view-container');
    }
    window.chordGraphInstance.fetch(params);
}


function applySimSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const minScore = document.getElementById('sim-min-score')?.value;
    const maxScore = document.getElementById('sim-max-score')?.value;
    const algo = document.getElementById('sim-algo')?.value;
    const minFeatures = document.getElementById('flt-func-min-features')?.value;
    const crossBinary = document.getElementById('sim-cross-binary')?.value;
    const matchMode = document.getElementById('sim-match-mode')?.value;
    const globalQ = document.getElementById('sim-search-input')?.value;
    const lang = document.getElementById('flt-func-language')?.value;
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
    if (matchMode && matchMode !== 'any') params.set('match_mode', matchMode);
    else params.delete('match_mode');
    params.set('pool_limit', poolLimit || '1000000');
    params.set('limit', countLimit || (params.get('view') === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT));

    const nameFlt = document.getElementById('flt-func-name')?.value;
    const addressFlt = document.getElementById('flt-func-address')?.value;
    const nsFlt = document.getElementById('flt-func-namespace')?.value;
    const retTypeFlt = document.getElementById('flt-func-ret_type')?.value;
    const clusterFlt = document.getElementById('flt-func-cluster')?.value;
    const clusterNameFlt = document.getElementById('flt-func-cluster-name')?.value;
    const minCohesionFlt = document.getElementById('flt-func-min-cohesion')?.value;
    const fileNameFlt = document.getElementById('flt-func-file_name')?.value;

    if (clusterFlt) params.set('cluster_uuid', clusterFlt); else params.delete('cluster_uuid');
    if (clusterNameFlt) params.set('cluster_name', clusterNameFlt); else params.delete('cluster_name');
    params.set('min_cohesion', minCohesionFlt || '0.95');

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

    const md5Flt = document.getElementById('flt-func-md5')?.value;
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

function applyAdvancedFileSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const globalQ = document.getElementById('file-search-input')?.value;
    params.set('q', globalQ || '');

    const nameFlt = document.getElementById('flt-file-name')?.value;
    const md5Flt = document.getElementById('flt-file-md5')?.value;
    const langFlt = document.getElementById('flt-file-language')?.value;
    const batchFlt = document.getElementById('flt-file-batch')?.value;
    const minEntryFlt = document.getElementById('flt-file-min-date')?.value;
    const maxEntryFlt = document.getElementById('flt-file-max-date')?.value;
    const minFuncsFlt = document.getElementById('flt-file-min-funcs')?.value;
    const maxFuncsFlt = document.getElementById('flt-file-max-funcs')?.value;
    const clusterFlt = document.getElementById('flt-file-cluster')?.value;
    const clusterNameFlt = document.getElementById('flt-file-cluster-name')?.value;
    const minCohesionFlt = document.getElementById('flt-file-min-cohesion')?.value;
    const maxCohesionFlt = document.getElementById('flt-file-max-cohesion')?.value;
    const yaraFlt = document.getElementById('flt-file-yara')?.value;
    const avtypeFlt = document.getElementById('flt-file-avtype')?.value;
    const ccipFlt = document.getElementById('flt-file-ccip')?.value;
    const infYaraFlt = document.getElementById('flt-file-inf-yara')?.value;
    const infAvtypeFlt = document.getElementById('flt-file-inf-avtype')?.value;
    const infTypeFlt = document.getElementById('flt-file-inf-type')?.value;
    const infCcipFlt = document.getElementById('flt-file-inf-ccip')?.value;

    if (nameFlt) params.set('file_name', nameFlt); else params.delete('file_name');
    if (md5Flt) params.set('file_md5', md5Flt); else params.delete('file_md5');
    if (langFlt) params.set('language_id', langFlt); else params.delete('language_id');
    if (batchFlt) params.set('batch_uuid', batchFlt); else params.delete('batch_uuid');
    if (minEntryFlt) params.set('min_entry_date', minEntryFlt); else params.delete('min_entry_date');
    if (maxEntryFlt) params.set('max_entry_date', maxEntryFlt); else params.delete('max_entry_date');
    if (minFuncsFlt) params.set('min_function_count', minFuncsFlt); else params.delete('min_function_count');
    if (maxFuncsFlt) params.set('max_function_count', maxFuncsFlt); else params.delete('max_function_count');
    if (clusterFlt) params.set('bin_cluster_uuid', clusterFlt); else params.delete('bin_cluster_uuid');
    if (clusterNameFlt) params.set('bin_cluster_name', clusterNameFlt); else params.delete('bin_cluster_name');
    if (minCohesionFlt) params.set('min_cohesion', minCohesionFlt); else params.delete('min_cohesion');
    if (maxCohesionFlt) params.set('max_cohesion', maxCohesionFlt); else params.delete('max_cohesion');
    if (yaraFlt) params.set('yara', yaraFlt); else params.delete('yara');
    if (avtypeFlt) params.set('avtype', avtypeFlt); else params.delete('avtype');
    if (ccipFlt) params.set('cc_ip', ccipFlt); else params.delete('cc_ip');
    if (infYaraFlt) params.set('inferred_yara', infYaraFlt); else params.delete('inferred_yara');
    if (infAvtypeFlt) params.set('inferred_avtype', infAvtypeFlt); else params.delete('inferred_avtype');
    if (infTypeFlt) params.set('inferred_filetype', infTypeFlt); else params.delete('inferred_filetype');
    if (infCcipFlt) params.set('inferred_ccip', infCcipFlt); else params.delete('inferred_ccip');

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    const allPossibleTagKeys = [
        'tag', 'static_tag', 'user_tag', 'file_tag', 'file_static_tag', 'file_user_tag',
        'exclude_tag', 'exclude_static_tag', 'exclude_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'
    ];
    allPossibleTagKeys.forEach(k => params.delete(k));

    const container = document.getElementById(`tag-container-file`);
    if (container) {
        const cards = container.querySelectorAll('.tag-filter-card');
        cards.forEach(card => {
            const type = card.dataset.type;
            const val = card.dataset.value;
            const isEx = card.dataset.exclude === 'true';
            const key = (isEx ? 'exclude_' : '') + type;
            params.append(key, val);
        });
    }

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function applyJobSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const collectionVal = document.getElementById('job-collection-filter')?.value;
    const statusVal = document.getElementById('job-status-filter')?.value;
    const typeVal = document.getElementById('job-type-filter')?.value;

    if (collectionVal) params.set('collection', collectionVal); else params.delete('collection');
    if (statusVal) params.set('status', statusVal); else params.delete('status');
    if (typeVal) params.set('type', typeVal); else params.delete('type');

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}
window.applyJobSearch = applyJobSearch;


function triggerTagSearch() {
    if (window.location.hash.startsWith('#function-similarity')) debouncedSearch(applySimSearch);
    else if (window.location.hash.startsWith('#binary-similarity')) debouncedSearch(applyBinSimSearch);
    else if (window.location.hash.startsWith('#functions')) debouncedSearch(applyAdvancedFuncSearch);
    else if (window.location.hash.startsWith('#files')) debouncedSearch(applyAdvancedFileSearch);
    else if (window.location.hash.startsWith('#features-global')) debouncedSearch(applyAdvancedFeatureSearch);
    else if (window.location.hash.startsWith('#clusters')) debouncedSearch(applyClusterSearch);
}

function applyAdvancedFeatureSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const globalQ = document.getElementById('feature-search-input')?.value;
    params.set('q', globalQ || '');

    const hashFlt = document.getElementById('flt-feat-hash')?.value;
    const typeFlt = document.getElementById('flt-feat-type')?.value;
    const opFlt = document.getElementById('flt-feat-op')?.value;
    const minTf = document.getElementById('flt-feat-min-tf')?.value;
    const maxTf = document.getElementById('flt-feat-max-tf')?.value;
    const minFreq = document.getElementById('flt-feat-min-freq')?.value;
    const maxFreq = document.getElementById('flt-feat-max-freq')?.value;

    if (hashFlt) params.set('hash', hashFlt); else params.delete('hash');
    if (typeFlt) params.set('type', typeFlt); else params.delete('type');
    if (opFlt) params.set('op', opFlt); else params.delete('op');
    if (minTf) params.set('min_tf_score', minTf); else params.delete('min_tf_score');
    if (maxTf) params.set('max_tf_score', maxTf); else params.delete('max_tf_score');
    if (minFreq) params.set('min_frequency', minFreq); else params.delete('min_frequency');
    if (maxFreq) params.set('max_frequency', maxFreq); else params.delete('max_frequency');

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
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

function renderCollections(data) {
    if (!data.length) return '<tr><td colspan="5" style="text-align:center">No collections found.</td></tr>';

    return data.map(col => `
        <tr data-id="${col.name}">
            <td><b style="color:var(--accent)">${col.name}</b></td>
            <td class="mono">${col['total_files']}</td>
            <td class="mono">${col['total_functions']}</td>
            <td class="dim">${formatDate(col['last_updated'])}</td>
            <td>
                <div style="display: flex; gap: 15px;">
                    <a class="btn-action" href="#upload?collection=${col.name}" style="color:var(--accent)">
                        <i class="fa-solid fa-cloud-arrow-up"></i>
                    </a>
                    <span style="color:var(--border)">|</span>
                    <a class="btn-action" href="#batches?collection=${col.name}">
                        Batches
                    </a>
                    <span style="color:var(--border)">|</span>
                    <a class="btn-action" href="#files?collection=${col.name}" style="color:var(--success)">
                        Files
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
        <tr data-id="${b['batch_uuid']}">
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
            <td><a class="btn-action" href="#files?collection=${col}&batch_uuid=${b['batch_uuid']}">Files</a></td>
        </tr>
    `}).join('');
}

function renderFiles(data, clustersMap = {}) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const col = params.get('collection');
    return data.map(f => {
        const fileId = f['file_id'] || `${col}:file:${f['file_md5']}`;
        const tags = f['tags'] || [];
        const user_tags = f['user_tags'] || [];
        const rowStyle = getRowTagColor(tags, user_tags);
        const batchUuid = f['batch_uuid'] || '---';
        const funcCount = f['function_count'] !== undefined ? f['function_count'] : 0;

        const clusters = (Array.isArray(f['bin_clusters']) ? f['bin_clusters'] : []).map(cid => clustersMap[cid]).filter(Boolean);

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${fileId}"
            data-entity-data='${JSON.stringify({
                md5: f['file_md5'],
                file_name: f['file_name'],
                id: fileId,
                collection: col
            }).replace(/'/g, "&apos;")}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
            <td class="sim-cell">
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <b style="color:var(--accent); cursor:pointer;" onclick="showFileDetailsPanel('${col}', '${f['file_md5']}', '${(f['file_name'] || '').replace(/'/g, '\\\'')}', event)">${f['file_name']}</b>
                </div>
            </td>
            <td class="sim-cell">
                ${EntityRenderer.renderMd5(f['file_md5'], { full: true })}
                <div class="dim" style="font-size:0.65rem">${f['language_id']}</div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:2px; font-size:0.65rem;">
                    ${(() => {
                        const fields = [
                            { key: 'yara', label: 'Yara', dist: 'yara_distribution' },
                            { key: 'avtype', label: 'AV', dist: 'avtype_distribution' },
                            { key: 'filetype', label: 'Type', dist: 'filetype_distribution' },
                            { key: 'cc_ip', label: 'IP', dist: 'ccip_distribution' }
                        ];
                        
                        return fields.map(field => {
                            const val = f[field.key];
                            if (val && val.length) {
                                return `<div class="dim">${field.label}: <span style="color:var(--accent)">${val.join(', ')}</span></div>`;
                            }
                            
                            // Try inference
                            let bestInf = null;
                            clusters.forEach(c => {
                                const dist = c[field.dist] || [];
                                if (dist.length > 0) {
                                    const cohesion = c.cohesion_score || 0;
                                    if (!bestInf || cohesion > bestInf.cohesion) {
                                        bestInf = { value: dist[0].value, cohesion: cohesion };
                                    }
                                }
                            });
                            
                            if (bestInf) {
                                const hue = Math.max(0, Math.min(120, bestInf.cohesion * 120));
                                const color = `hsl(${hue}, 80%, 60%)`;
                                return `<div class="dim" title="Inferred from cluster (cohesion: ${(bestInf.cohesion*100).toFixed(1)}%)">
                                    ${field.label}: <span style="color:${color}; opacity: 0.9; font-style: italic;">${bestInf.value} <small>(${(bestInf.cohesion*100).toFixed(0)}%)</small></span>
                                </div>`;
                            }
                            return '';
                        }).join('');
                    })()}
                    ${f['first_seen'] && f['first_seen'].length ? `<div class="dim">Seen: <span style="color:var(--accent)">${f['first_seen'].join(', ')}</span></div>` : ''}
                </div>
            </td>
            <td class="sim-cell mono dim" style="font-size:0.7rem" title="${batchUuid}">
                ${batchUuid.length > 8 ? batchUuid.substring(0, 8) + '...' : batchUuid}
            </td>
            <td class="sim-cell">
                <div style="display:inline-flex; align-items:center; justify-content:center; gap:8px; width:100%;">
                    <span style="font-weight: bold; color: var(--text); min-width: 20px; text-align: right;">${funcCount}</span>
                    <button class="btn-file-diff-action ${fileDiffSelection.some(item => item.id === fileId) ? 'active' : ''}"
                            data-file-id="${fileId}"
                            onclick="addToFileDiff('${fileId}', '${f['file_name'].replace(/'/g, "\\'")}', event)"
                            title="Add to File Diff">
                        <span>±</span>
                    </button>
                    <a class="btn-action" href="#functions?collection=${col}&file_md5=${f['file_md5']}" title="Functions">
                        <i class="fa-solid fa-code"></i>
                    </a>
                    <a class="btn-action" href="#file-call-graph?collection=${col}&file_md5=${f['file_md5']}" title="Call Graph" style="color: var(--accent);">
                        <i class="fa-solid fa-network-wired"></i>
                    </a>
                </div>
            </td>
            <td class="cluster-cards-cell" data-is-binary="true" data-clusters='${JSON.stringify(clusters).replace(/'/g, "&apos;")}'>
                ${EntityRenderer.renderClusterCard(clusters, true)}
            </td>
            <td class="sim-cell dim">${formatDate(f['entry_date'])}</td>
            <td>
                ${EntityRenderer.renderTag('file', fileId, tags, user_tags)}
            </td>
        </tr>
    `;
    }).join('');
}

function renderFunctions(data, clustersMap = {}) {
    return data.map(f => {
        const entry = f['entrypoint_address'] || '';
        const tags = f['tags'] || [];
        const user_tags = f['user_tags'] || [];
        const fileName = f['file_name'] || '';
        const file_md5 = f['file_md5'] || '';
        const language = f['language_id'] || '---';
        const featCount = f['bsim_features_count'] || 0;
        const funcId = f['function_id'] || `${f.collection}:func:${file_md5}:${entry}`;
        const rowStyle = getRowTagColor(tags, user_tags);
        const clusters = (f['clusters'] || []).map(uuid => clustersMap[uuid]).filter(Boolean);

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${funcId}"
            data-entity-data='${JSON.stringify(f).replace(/'/g, "&apos;")}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
            <td class="sim-cell" style="min-width: 300px;">
                ${EntityRenderer.renderFunction(f, { hideNote: true })}
            </td>
            <td class="sim-cell"><span class="mono" style="color:var(--accent);">@ ${entry}</span></td>
            <td>${EntityRenderer.renderTag('function', funcId, tags, user_tags)}</td>
            <td class="cluster-cards-cell" data-clusters='${JSON.stringify(clusters).replace(/'/g, "&apos;")}'>${EntityRenderer.renderClusterCard(clusters)}</td>
            <td class="sim-cell" style="text-align:center;">
                <div style="display:inline-flex; align-items:center; gap:6px;">
                    <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                    <button class="btn-icon" onclick="showFeaturePanel('${funcId}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                </div>
            </td>
            <td class="sim-cell" style="text-align:center;">
                ${EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true })}
            </td>
            <td class="sim-cell"><div style="color:#aaa; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8;" title="${fileName}">${fileName}</div></td>

            <td class="sim-cell">${EntityRenderer.renderMd5(file_md5)}</td>
            <td>${EntityRenderer.renderTag('file', `${f.collection || 'main'}:file:${file_md5}`, f.file_tags || [], f.file_user_tags || [])}</td>
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
            const funcId = ctx.func_id || `${col}:func:${ctx.md5}:${ctx.addr}`;
            const funcName = (ctx.name || ctx.addr);
            const targetLinesStr = (ctx.line_idxs || []).map(l => l + 1).join(',');
            const lineHash = targetLinesStr ? `#L${targetLinesStr}` : '';

            const displayLine = (ctx.line_idxs && ctx.line_idxs.length > 0) ? ctx.line_idxs[0] + 1 : 1;
            
            const fData = {
                function_id: funcId,
                function_name: funcName,
                file_md5: ctx.md5,
                entrypoint_address: ctx.addr,
                collection: col
            };

            cCodeHtml = `<div class="code-card clickable" title="Click to jump to lines ${targetLinesStr || ''}"
                     data-entity-data='${JSON.stringify(fData).replace(/'/g, "&apos;")}'
                     oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)"
                     onclick="showFunctionCodeById('${funcId}', '${funcName.replace(/'/g, "\\'")}', '${lineHash}', event)">
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
        <tr data-id="${f.hash}">
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
                    onclick="showGlobalFeaturePanel('${f.hash}', '${col}', event)">Analyze</button>
            </td>

        </tr>
    `}).join('');
}

function renderTopCorrelations(items, clustersMap = {}) {
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

        const func1Data = {
            ...p.meta1,
            function_name: p.name1 || p.meta1?.function_name,
            function_id: p.id1,
            entrypoint_address: addr1,
            collection: col
        };
        const func2Data = {
            ...p.meta2,
            function_name: p.name2 || p.meta2?.function_name,
            function_id: p.id2,
            entrypoint_address: addr2,
            collection: col
        };

        const clusters1 = (p.meta1?.clusters || []).map(uuid => clustersMap[uuid]).filter(Boolean);
        const clusters2 = (p.meta2?.clusters || []).map(uuid => clustersMap[uuid]).filter(Boolean);

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${pairId}" data-id1="${p.id1}" data-id2="${p.id2}" data-algo="${p.algo}" data-sid="${p.sid || ''}"
            data-entity-data='${JSON.stringify(p).replace(/'/g, "&apos;")}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'similarity', this)">
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="font-size:1.1rem; font-weight:bold; color:var(--success);">${(p.score * 100).toFixed(1)}%</div>
                    <button class="btn-diff-action" 
                        onmouseenter="showDiffPreview('${p.id1}', '${(p.name1 || '').replace(/'/g, "\\'")}', '${p.id2}', '${(p.name2 || '').replace(/'/g, "\\'")}', ${p.score}, event)" 
                        onmousemove="moveCodePreview(event)"
                        onmouseleave="hideDiffPreview(event)"
                        onclick="openDiffDirectly('${p.id1}', '${(p.name1 || '').replace(/'/g, "\\'")}', '${p.id2}', '${(p.name2 || '').replace(/'/g, "\\'")}', event)" 
                        title="Run Aligned Diff" 
                        style="padding:0 5px; font-size: 0.75rem; border-radius: 3px; display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px;">
                        <span>±</span>
                    </button>
                </div>
                ${EntityRenderer.renderTag('similarity', pairId, tags, user_tags)}
            </td>
            <td style="min-width: 300px;">
                <div style="display:flex; flex-direction:column; gap:8px; width: 100%;">
                    <div style="min-height:24px; display:flex; align-items:center; width: 100%;">
                        ${EntityRenderer.renderFunction(func1Data, { hideNote: true })}
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; width: 100%;">
                        ${EntityRenderer.renderFunction(func2Data, { hideNote: true })}
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
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('function', p.id1, p.meta1?.tags || [], p.meta1?.user_tags || [])}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('function', p.id2, p.meta2?.tags || [], p.meta2?.user_tags || [])}</div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;" class="cluster-cards-cell" data-clusters='${JSON.stringify(clusters1).replace(/'/g, "&apos;")}'>${EntityRenderer.renderClusterCard(clusters1)}</div>
                    <div style="min-height:24px; display:flex; align-items:center;" class="cluster-cards-cell" data-clusters='${JSON.stringify(clusters2).replace(/'/g, "&apos;")}'>${EntityRenderer.renderClusterCard(clusters2)}</div>
                </div>
            </td>
            <td class="sim-cell" style="text-align:center; vertical-align:middle;">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta1?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel('${p.id1}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta2?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel('${p.id2}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                </div>
            </td>
            <td class="sim-cell" style="text-align:center; vertical-align:middle;">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        ${EntityRenderer.renderNoteButton(p.id1, p.meta1?.note_owners, { isTable: true })}
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        ${EntityRenderer.renderNoteButton(p.id2, p.meta2?.note_owners, { isTable: true })}
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
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderMd5(m1)}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderMd5(m2)}</div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${col}:file:${p.meta1?.file_md5}`, p.meta1?.file_tags || [], p.meta1?.file_user_tags || [])}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${col}:file:${p.meta2?.file_md5}`, p.meta2?.file_tags || [], p.meta2?.file_user_tags || [])}</div>
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

    windowManager.createWindow(label, url, { type: 'diff' });
}

function openDiffDirectly(id1, name1, id2, name2, e) {
    const url = `/diff/index.html?id1=${encodeURIComponent(normalizeFuncId(id1))}&id2=${encodeURIComponent(normalizeFuncId(id2))}`;
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(url, '_blank');
        return;
    }

    windowManager.createWindow(`Diff: ${name1} vs ${name2}`, url, { type: 'diff' });
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

function showFunctionCodeById(id, name, lineHash = '', e) {
    if (window.getSelection && window.getSelection().toString().trim()) {
        return;
    }
    const url = `/function/index.html?id=${encodeURIComponent(id)}${lineHash}`;
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(url, '_blank');
        return;
    }

    windowManager.createWindow(`Code: ${name}`, url, { type: 'code' });
}

function seeSimilarFromCode() {
    const win = windowManager.activeWindow;
    if (!win || !win.iframe || !win.iframe.src) return;

    let url;
    try {
        url = new URL(win.iframe.contentWindow.location.href);
    } catch (e) {
        url = new URL(win.iframe.src, window.location.origin);
    }

    const id = url.searchParams.get('id');
    if (!id) return;

    const parts = id.split(':');
    if (parts.length < 4) return;
    const col = parts[0];
    const md5 = parts[2];
    const addr = parts[3];

    window.location.hash = `#function-similarity?collection=${col}&md5=${md5}&address=${addr}&algo=unweighted_cosine`;
    windowManager.closeWindow(win);
}


function showFileDetailsPanel(col, md5, name, e) {
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(`/file/index.html?collection=${encodeURIComponent(col)}&md5=${encodeURIComponent(md5)}`, '_blank');
        return;
    }
    const url = `/file/index.html?collection=${encodeURIComponent(col)}&md5=${encodeURIComponent(md5)}`;
    windowManager.createWindow(`File: ${name}`, url, { type: 'file' });
}

function showFeaturePanel(id, e) {
    const url = `/function/features/index.html?id=${encodeURIComponent(id)}`;
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(url, '_blank');
        return;
    }

    const addr = id.split(':').pop();
    windowManager.createWindow(`Features: ${addr}`, url, { type: 'features' });
}

function showNotePanel(id, e) {
    if (typeof showNotes === 'function') {
        showNotes(id);
    } else {
        console.error("showNotes not found");
    }
}

function showGlobalFeaturePanel(hash, collection, e) {
    const url = `/feature/index.html?hash=${encodeURIComponent(hash)}&collection=${encodeURIComponent(collection)}`;
    if (e && (e.ctrlKey || e.metaKey)) {
        window.open(url, '_blank');
        return;
    }

    windowManager.createWindow(`Feature Analysis: ${hash.substring(0, 12)}...`, url, { type: 'global-feature' });
}

// Old panel toggle functions removed as closing is handled by WindowManager
function hideDiffPanel() { }
function hideCodePanel() { }
function hideFeaturePanel() { }
function hideGlobalFeaturePanel() { }

function launchExternal(type) {
    const win = windowManager.activeWindow;
    if (win) windowManager.popout(win);
}

// Old Resizing Logic removed in favor of WindowManager's resize handles

// Apply "NOT ignore" defaults only when first entering the Sim view
function applySimViewDefaults(hashPath, queryString) {
    return false;
}

window.addEventListener('hashchange', (e) => {
    // Ensure all tooltips are hidden when navigating/switching views
    if (window.hideAllTooltips) {
        window.hideAllTooltips();
    } else {
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
    }

    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);
    const col = params.get('collection') || 'main';

    if (window.isClearingFilters) {
        window.isClearingFilters = false;
        localStorage.setItem(`savedFilters:${col}:${hashPath}`, queryString || `collection=${col}`);
    }

    const [newHash] = (window.location.hash || '#collections').split('?');
    const [oldHash] = (e.oldURL ? new URL(e.oldURL).hash : '').split('?');
    // Apply defaults only when entering sim view from a different view
    if (newHash === '#function-similarity' && oldHash !== '#function-similarity') {
        const [hashPathPart, queryStringPart] = (window.location.hash || '').split('?');
        if (applySimViewDefaults(hashPathPart, queryStringPart)) return;
    }

    // Update rebuild buttons state for the new collection/view immediately
    if (window.updateJobStatusIcon) window.updateJobStatusIcon();

    refreshData();
});

// UI Settings
const UIParams = {
    cohesionThreshold: localStorage.getItem('cohesionThreshold') !== null ? parseFloat(localStorage.getItem('cohesionThreshold')) : 0.95,
    colorByTag: localStorage.getItem('colorByTag') === 'true',
    includeHeaders: localStorage.getItem('includeHeaders') === 'true'
};
window.UIParams = UIParams;

function toggleUISettings() {
    const panel = document.getElementById('ui-settings-panel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

function updateUIParams() {
    const prevColorByTag = UIParams.colorByTag;

    UIParams.colorByTag = document.getElementById('param-color-tags').checked;
    UIParams.includeHeaders = document.getElementById('param-include-headers').checked;

    localStorage.setItem('colorByTag', UIParams.colorByTag);
    localStorage.setItem('includeHeaders', UIParams.includeHeaders);

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
            const isBinary = cell.dataset.isBinary === 'true';
            cell.innerHTML = EntityRenderer.renderClusterCard(clusters, isBinary);
        } catch (e) {
            console.error("Failed to re-render cluster cards", e);
        }
    });
}

function loadUIParams() {
    const elColorTags = document.getElementById('param-color-tags');
    const elIncludeHeaders = document.getElementById('param-include-headers');
    if (elColorTags) elColorTags.checked = UIParams.colorByTag;
    if (elIncludeHeaders) elIncludeHeaders.checked = UIParams.includeHeaders;
}

window.addEventListener('load', () => {
    loadUIParams();
    populateCollectionDropdown();
    if (!window.location.hash) window.location.hash = '#collections';

    // Attach graph settings listeners
    const graphSettingIds = [
        'graph-show-label',
        'graph-color-binary',
        'graph-color-function',
        'graph-color-sim',
        'graph-bundle-tension',
        'graph-link-width',
        'graph-scale-width'
    ];
    graphSettingIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', () => { if (typeof saveGraphSettings === 'function') saveGraphSettings(); });
            el.addEventListener('input', () => { if (typeof saveGraphSettings === 'function') saveGraphSettings(); });
        }
    });

    const [hashPath, queryString] = (window.location.hash || '').split('?');

    if (applySimViewDefaults(hashPath, queryString)) {
        loadDiffQueue();
        return;
    }
    refreshData();
    loadDiffQueue();

    // Sidebar History Hover Listeners
    const navHistoryContainer = document.querySelector('.history-dropdown-container');
    if (navHistoryContainer) {
        let historyHideTimeout = null;
        const historyFlyout = document.getElementById('history-flyout');

        const showHistoryPanel = () => {
            if (historyHideTimeout) clearTimeout(historyHideTimeout);
            if (typeof renderHistoryDropdowns === 'function') renderHistoryDropdowns();

            const isCollapsed = document.body.classList.contains('sidebar-collapsed');
            if (isCollapsed && historyFlyout) {
                // Populate flyout with same content as the global dropdown
                const globalDropdown = document.getElementById('history-dropdown');
                if (globalDropdown) historyFlyout.innerHTML = globalDropdown.innerHTML;
                // Position vertically at the icon
                const rect = navHistoryContainer.getBoundingClientRect();
                historyFlyout.style.top = Math.min(rect.top, window.innerHeight - 420) + 'px';
                historyFlyout.style.display = 'flex';
            } else {
                const dropdown = document.getElementById('history-dropdown');
                if (dropdown) {
                    dropdown.style.display = 'block';
                    const chev = document.getElementById('nav-history-chevron');
                    if (chev) chev.style.transform = 'rotate(180deg)';
                }
            }
        };

        const hideHistoryPanel = () => {
            historyHideTimeout = setTimeout(() => {
                const dropdown = document.getElementById('history-dropdown');
                if (dropdown) {
                    dropdown.style.display = 'none';
                    const chev = document.getElementById('nav-history-chevron');
                    if (chev) chev.style.transform = 'rotate(0deg)';
                }
                if (historyFlyout) historyFlyout.style.display = 'none';
            }, 200);
        };

        navHistoryContainer.addEventListener('mouseenter', showHistoryPanel);
        navHistoryContainer.addEventListener('mouseleave', hideHistoryPanel);

        if (historyFlyout) {
            historyFlyout.addEventListener('mouseenter', () => {
                if (historyHideTimeout) clearTimeout(historyHideTimeout);
            });
            historyFlyout.addEventListener('mouseleave', hideHistoryPanel);
            // Allow clicking items inside flyout to work (loadHistoryItemByTimestamp)
            historyFlyout.addEventListener('click', (e) => {
                const item = e.target.closest('.history-item');
                if (item && item.getAttribute('onclick')) {
                    historyFlyout.style.display = 'none';
                }
                const clearBtn = e.target.closest('.history-dropdown-clear-btn');
                if (clearBtn) {
                    // Re-render flyout after clearing
                    setTimeout(() => {
                        if (typeof renderHistoryDropdowns === 'function') renderHistoryDropdowns();
                        const globalDropdown = document.getElementById('history-dropdown');
                        if (globalDropdown) historyFlyout.innerHTML = globalDropdown.innerHTML;
                    }, 50);
                }
            });
        }
    }


    // View-Specific History Hover Listeners
    const viewHistoryContainer = document.querySelector('.view-history-container');
    if (viewHistoryContainer) {
        viewHistoryContainer.addEventListener('mouseenter', () => {
            if (typeof renderHistoryDropdowns === 'function') renderHistoryDropdowns();
            const dropdown = document.getElementById('view-history-dropdown');
            if (dropdown) {
                dropdown.style.display = 'block';
            }
        });
        viewHistoryContainer.addEventListener('mouseleave', () => {
            const dropdown = document.getElementById('view-history-dropdown');
            if (dropdown) {
                dropdown.style.display = 'none';
            }
        });
    }

    // Navbar Job Status Polling
    window.triggerRebuildAll = async function () {
        const [hashPath, queryString] = (window.location.hash || '').split('?');
        const params = new URLSearchParams(queryString);
        const collection = params.get('collection') || 'main';
        const algo = params.get('algo') || 'unweighted_cosine';

        // Select all possible buttons
        const btns = document.querySelectorAll('.nav-rebuild-btn, #header-rebuild-all-btn');
        const icons = document.querySelectorAll('.nav-rebuild-icon, #header-rebuild-all-icon');

        btns.forEach(btn => btn.disabled = true);
        icons.forEach(icon => icon.classList.add('fa-spin'));

        try {
            const resp = await fetch('/api/cluster/rebuild_all', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    collection: collection,
                    algo: algo
                })
            });

            if (resp.ok) {
                if (typeof showToast === 'function') {
                    showToast(`Analysis rebuild pipeline enqueued!`, 'success');
                }
                if (window.refreshData) window.refreshData();
            } else {
                const data = await resp.json();
                if (typeof showToast === 'function') {
                    showToast(`Failed to trigger rebuild: ${data.error || 'Unknown error'}`, 'error');
                }
            }
        } catch (e) {
            console.error(e);
            if (typeof showToast === 'function') {
                showToast('Error triggering rebuild', 'error');
            }
        }
    };

    window.updateJobStatusIcon = async () => {
        try {
            const res = await fetch('/api/jobs/stats');
            if (!res.ok) return;
            const stats = await res.json();
            const loader = document.getElementById('nav-jobs-loader');
            const icon = document.getElementById('nav-jobs-icon');
            const navLink = document.getElementById('nav-jobs');

            const isActive = stats.active_workers > 0 || stats.pending_jobs > 0;

            if (loader && icon && navLink) {
                if (isActive) {
                    loader.style.display = 'block';
                    icon.style.display = 'none';
                    navLink.title = `${stats.active_workers} active, ${stats.pending_jobs} pending jobs`;
                } else {
                    loader.style.display = 'none';
                    icon.style.display = 'inline-block';
                    navLink.title = "Background Jobs";
                }
            }

            // Update rebuild buttons
            const btns = document.querySelectorAll('.nav-rebuild-btn, #header-rebuild-all-btn');
            const icons = document.querySelectorAll('.nav-rebuild-icon, #header-rebuild-all-icon');

            const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
            const currentCollection = params.get('collection');

            // Rebuild animation should only show if a job FOR THIS COLLECTION is active
            const activeCollections = stats.active_collections || [];
            const isCollectionActive = currentCollection && activeCollections.includes(currentCollection);
            const showAnimation = isActive && isCollectionActive;

            btns.forEach(btn => {
                btn.disabled = showAnimation;
                btn.title = showAnimation ? "A job for this collection is already running" : "Rebuild Clusters & Binary Sim";
            });

            icons.forEach(icon => {
                if (showAnimation) icon.classList.add('fa-spin');
                else icon.classList.remove('fa-spin');
            });

            // Handle Header Rebuild Button Visibility (only if sidebar is collapsed AND view is Clusters/BinSim)
            const headerRebuildBtn = document.getElementById('header-rebuild-all-btn');
            if (headerRebuildBtn) {
                const isCollapsed = document.body.classList.contains('sidebar-collapsed');
                const [path] = (window.location.hash || '').split('?');
                const isRelevantView = (path === '#clusters' || path === '#binary-similarity');

                if (isCollapsed && isRelevantView) {
                    headerRebuildBtn.style.display = 'inline-flex';
                } else {
                    headerRebuildBtn.style.display = 'none';
                }
            }
        } catch (e) {
            // Silently fail for navbar polling
        }
    };
    window.updateJobStatusIcon();
    setInterval(window.updateJobStatusIcon, 10000); // Check every 10s
});

async function populateCollectionDropdown() {
    try {
        const res = await fetch('/api/collection/search');
        if (!res.ok) return;
        const data = await res.json();
        const collections = data.collections || (Array.isArray(data) ? data : []);

        const list = document.getElementById('collection-flyout-list');
        const trigger = document.getElementById('nav-collections');
        const flyout = document.getElementById('collection-flyout');
        if (!list || !trigger || !flyout) return;

        const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
        const currentCollection = params.get('collection') || '';

        const nameDisplay = document.getElementById('current-collection-name');
        if (nameDisplay) {
            nameDisplay.innerText = currentCollection || 'Collections';
            nameDisplay.style.color = currentCollection ? 'var(--accent)' : 'inherit';
        }

        list.innerHTML = collections.map(c => `
            <div class="collection-item ${c.name === currentCollection ? 'active' : ''}" onclick="selectCollection('${c.name}')">
                <i class="fa-solid fa-database"></i>
                <span>${c.name}</span>
            </div>
        `).join('') || '<div class="collection-item dim">No collections found</div>';

        updateNavVisibility(currentCollection);

        // Hover Logic
        if (!trigger.dataset.hasListener) {
            let hideTimeout = null;

            const show = () => {
                if (hideTimeout) clearTimeout(hideTimeout);
                const rect = trigger.getBoundingClientRect();
                flyout.style.top = rect.top + 'px';
                flyout.style.display = 'flex';
            };

            const hide = () => {
                hideTimeout = setTimeout(() => {
                    flyout.style.display = 'none';
                }, 300);
            };

            trigger.addEventListener('mouseenter', show);
            trigger.addEventListener('mouseleave', hide);
            flyout.addEventListener('mouseenter', () => { if (hideTimeout) clearTimeout(hideTimeout); });
            flyout.addEventListener('mouseleave', hide);

            trigger.dataset.hasListener = "true";
        }
    } catch (e) {
        console.error("Failed to populate collections", e);
    }
}

function selectCollection(name) {
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const p = new URLSearchParams(queryString);
    if (name) p.set('collection', name);
    else p.delete('collection');
    window.location.hash = `${hashPath}?${p.toString()}`;
    document.getElementById('collection-flyout').style.display = 'none';
}

function updateNavVisibility(collection) {
    const navItems = ['nav-batches', 'nav-files', 'nav-functions', 'nav-features-global', 'nav-function-similarity', 'nav-clusters', 'nav-bin-clusters', 'nav-file-call-graph', 'nav-binary-similarity', 'nav-chord-map'];
    navItems.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = collection ? 'flex' : 'none';
    });
}

function renderClusters(items) {
    return items.map(c => {
        const showDots = (c.sample_members && c.sample_members.length > 0) && (c.count > 3 || c.sample_members.length > 3);
        const remaining = showDots ? Math.max(c.count - 3, c.sample_members.length - 3) : 0;
        
        const sampleMembersHtml = (c.sample_members || []).slice(0, 3).map(m => {
            if (typeof m === 'string') {
                return `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${m}">${m}</div>`;
            }
            return `
                <div style="margin-bottom:2px; width: 100%;">
                    ${EntityRenderer.renderFunction(m, { showActions: false })}
                </div>
            `;
        }).join('') + (showDots ? `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); padding-left:2px; line-height:1;">and ${remaining} others ...</div>` : '') || '<span class="dim">—</span>';

        return `
        <tr data-cluster-id="${c.cluster_id}"
            data-entity-data='${JSON.stringify({
                cluster_id: c.cluster_id,
                cluster_uuid: c.cluster_uuid,
                cluster_name: c.cluster_name
            }).replace(/'/g, "&apos;")}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'cluster', this)">
            <td class="mono cluster-uuid-id-cell" data-uuid="${c.cluster_uuid}" data-id="${c.cluster_id}" style="color:var(--accent)">
                ${(c.cluster_uuid || '').substring(0, 8)}
                <div class="dim" style="font-size:0.7rem">ID: ${c.cluster_id}</div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <span id="name-display-${c.cluster_id}" style="cursor:pointer;">${c.cluster_name}</span>
                    <button class="btn-action" title="Rename" onclick="renameCluster('${c.cluster_id}', '${c.cluster_name}')"><i class="fa-solid fa-pen"></i></button>
                </div>
            </td>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <span style="font-weight:bold; min-width: 25px; text-align: right;">${c.count.toLocaleString()}</span>
                    <a href="#functions?collection=${getCollectionFromHash()}&cluster_uuid=${c.cluster_uuid}" class="btn-action" title="Functions" onmouseenter="showClusterTableTooltip(event, '${c.cluster_uuid}', '${(c.cluster_name || '').replace(/'/g, "\\'")}', ${c.count || 0}, ${c.avg_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})" onmouseleave="hideClusterTableTooltip(event)" onmousemove="moveClusterTableTooltip(event)">
                        <i class="fa-solid fa-code"></i>
                    </a>
                    <a href="#function-similarity?collection=${getCollectionFromHash()}&cluster_uuid=${c.cluster_uuid}" class="btn-action" title="Similarities" style="color:var(--info)">
                        <i class="fa-solid fa-code-compare"></i>
                    </a>
                </div>
            </td>
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
            <td style="min-width: 350px;">
                <div style="display:flex; flex-direction:column; gap:2px; width: 100%;">
                    ${sampleMembersHtml}
                </div>
            </td>
        </tr>
        `;
    }).join('');
}

function applyClusterSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const globalQ = document.getElementById('cluster-search-input')?.value;
    params.set('q', globalQ || '');

    const cid = document.getElementById('flt-cluster-id')?.value;
    const cuuid = document.getElementById('flt-cluster-uuid')?.value;
    const cname = document.getElementById('flt-cluster-name')?.value;
    const cstab = document.getElementById('flt-cluster-min-stability')?.value;
    const ccount = document.getElementById('flt-cluster-min-count')?.value;
    const cfeat = document.getElementById('flt-cluster-min-features')?.value;
    const ccoh = document.getElementById('flt-cluster-min-cohesion')?.value;
    const fFuncName = document.getElementById('flt-cluster-func-name')?.value;
    const fFuncAddr = document.getElementById('flt-cluster-func-addr')?.value;
    const fFileName = document.getElementById('flt-cluster-file-name')?.value;

    if (cid) params.set('cluster_id', cid); else params.delete('cluster_id');
    if (cuuid) params.set('cluster_uuid', cuuid); else params.delete('cluster_uuid');
    if (cname) params.set('cluster_name', cname); else params.delete('cluster_name');
    if (cstab) params.set('min_stability', cstab); else params.delete('min_stability');
    if (ccount) params.set('min_count', ccount); else params.delete('min_count');
    if (cfeat) params.set('min_features', cfeat); else params.delete('min_features');
    if (ccoh) params.set('min_cohesion', ccoh); else params.delete('min_cohesion');
    if (fFuncName) params.set('func_name', fFuncName); else params.delete('func_name');
    if (fFuncAddr) params.set('func_addr', fFuncAddr); else params.delete('func_addr');
    if (fFileName) params.set('file_name', fFileName); else params.delete('file_name');

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function renderBinClusters(items) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    const nameType = params.get('cluster_name_type') || 'file';

    return items.map(c => {
        let displayName = c.cluster_name;
        if (nameType === 'yara' && c.yara_distribution && c.yara_distribution.length > 0) {
            displayName = c.yara_distribution[0].value;
        }

        const showDots = (c.sample_members && c.sample_members.length > 0) && (c.count > 3 || c.sample_members.length > 3);
        const remaining = showDots ? Math.max(c.count - 3, c.sample_members.length - 3) : 0;
        
        const sampleMembersHtml = (c.sample_members || []).slice(0, 3).map(m => {
            if (typeof m === 'string') {
                return `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${m}">${m}</div>`;
            }
            // For binaries, we don't have a standardized renderer yet, but we can wrap them
            return `<div class="mono" style="font-size:0.7rem; color:var(--accent); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${m.name}">${m.name}</div>`;
        }).join('') + (showDots ? `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); padding-left:2px; line-height:1;">and ${remaining} others ...</div>` : '') || '<span class="dim">—</span>';

        return `
        <tr data-cluster-id="${c.cluster_id}"
            data-entity-data='${JSON.stringify({
                cluster_id: c.cluster_id,
                cluster_uuid: c.cluster_uuid,
                cluster_name: displayName
            }).replace(/'/g, "&apos;")}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'bin_cluster', this)">
            <td class="mono cluster-uuid-id-cell" data-uuid="${c.cluster_uuid}" data-id="${c.cluster_id}" style="color:var(--accent)">
                ${(c.cluster_uuid || '').substring(0, 8)}
                <div class="dim" style="font-size:0.7rem">ID: ${c.cluster_id}</div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:4px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span id="name-display-bin-${c.cluster_id}" style="cursor:pointer; font-weight:bold;">${displayName}</span>
                        <button class="btn-action" title="Rename" onclick="renameBinCluster('${c.cluster_id}', '${(c.cluster_name || '').replace(/'/g, "\\'")}')"><i class="fa-solid fa-pen"></i></button>
                    </div>
                    <div class="dim" style="font-size:0.65rem; display:flex; flex-direction:column; gap:2px;">
                        ${c.yara_distribution && c.yara_distribution.length ? `<div>Yara: <span style="color:var(--accent)">${c.yara_distribution[0].value} (${c.yara_distribution[0].percent}%)</span></div>` : ''}
                        ${c.avtype_distribution && c.avtype_distribution.length ? `<div>AV: <span style="color:var(--accent)">${c.avtype_distribution[0].value} (${c.avtype_distribution[0].percent}%)</span></div>` : ''}
                    </div>
                </div>
            </td>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <span style="font-weight:bold; min-width: 25px; text-align: right;">${c.count.toLocaleString()}</span>
                    <a href="#files?collection=${getCollectionFromHash()}&bin_cluster_uuid=${c.cluster_uuid}" class="btn-action" title="Binaries" onmouseenter="showBinClusterTableTooltip(event, '${c.cluster_uuid}', '${(displayName || '').replace(/'/g, "\\'")}', ${c.count || 0}, ${c.avg_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})" onmouseleave="hideBinClusterTableTooltip(event)" onmousemove="moveBinClusterTableTooltip(event)">
                        <i class="fa-solid fa-file-code"></i>
                    </a>
                </div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--success); width:${Math.min(100, c.avg_stability).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.avg_stability).toFixed(2)}</span>
                </div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:#333; border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--info); width:${((c.cohesion_score || 0) * 100).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.cohesion_score || 0).toFixed(2)}</span>
                </div>
            </td>
            <td class="dim">${formatDate(c.created_at)}</td>
            <td style="min-width: 250px;">
                <div style="display:flex; flex-direction:column; gap:2px; width: 100%;">
                    ${sampleMembersHtml}
                </div>
            </td>
        </tr>
        `;
    }).join('');
}

function applyBinClusterSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);

    const globalQ = document.getElementById('bin-cluster-search-input')?.value;
    params.set('q', globalQ || '');

    const cid = document.getElementById('flt-bin-cluster-id')?.value;
    const cuuid = document.getElementById('flt-bin-cluster-uuid')?.value;
    const cname = document.getElementById('flt-bin-cluster-name')?.value;
    const cstab = document.getElementById('flt-bin-cluster-min-stability')?.value;
    const ccount = document.getElementById('flt-bin-cluster-min-count')?.value;
    const ccountMax = document.getElementById('flt-bin-cluster-max-count')?.value;
    const ccoh = document.getElementById('flt-bin-cluster-min-cohesion')?.value;
    const ccohMax = document.getElementById('flt-bin-cluster-max-cohesion')?.value;
    const fFileName = document.getElementById('flt-bin-cluster-file-name')?.value;
    const fFileMd5 = document.getElementById('flt-bin-cluster-file-md5')?.value;

    if (cid) params.set('cluster_id', cid); else params.delete('cluster_id');
    if (cuuid) params.set('cluster_uuid', cuuid); else params.delete('cluster_uuid');
    if (cname) params.set('cluster_name', cname); else params.delete('cluster_name');
    if (cstab) params.set('min_stability', cstab); else params.delete('min_stability');
    if (ccount) params.set('min_count', ccount); else params.delete('min_count');
    if (ccountMax) params.set('max_count', ccountMax); else params.delete('max_count');
    if (ccoh) params.set('min_cohesion', ccoh); else params.delete('min_cohesion');
    if (ccohMax) params.set('max_cohesion', ccohMax); else params.delete('max_cohesion');
    if (fFileName) params.set('file_name', fFileName); else params.delete('file_name');
    if (fFileMd5) params.set('file_md5', fFileMd5); else params.delete('file_md5');

    currentOffset = 0;
    isEndOfResults = false;
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function updateD3ClusterName(cid, newName) {
    [window.hierarchyInstance, window.packingInstance, window.binHierarchyInstance, window.binPackingInstance].forEach(instance => {
        if (instance && instance.rawNodes) {
            const match = instance.rawNodes.find(n => n.id === cid || n.cluster_id === cid || n.cluster_uuid === cid || n.uuid === cid);
            if (match) {
                if (match.cluster_name !== undefined) match.cluster_name = newName;
                if (match.name !== undefined) match.name = newName;
                instance.render(instance.rawNodes);
            }
        }
    });
}

async function renameBinCluster(clusterId, currentName) {
    const newName = prompt(`Enter new name for binary cluster ${clusterId}:`, currentName);
    if (!newName || newName === currentName) return;

    const collection = getCollectionFromHash();
    const algo = new URLSearchParams(window.location.hash.split('?')[1]).get('algo') || 'unweighted_cosine';

    try {
        const res = await fetch('/api/bin_cluster/meta', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, algo, cluster_id: clusterId, cluster_name: newName })
        });
        const data = await res.json();
        if (data.status === 'success') {
            const el = document.getElementById(`name-display-bin-${clusterId}`);
            if (el) el.innerText = newName;
            updateD3ClusterName(clusterId, newName);
            if (typeof showToast === 'function') {
                showToast(`Binary cluster renamed to ${newName}`, 'success');
            }
        } else {
            if (typeof showToast === 'function') {
                showToast(`Rename failed: ${data.error}`, 'error');
            }
        }
    } catch (err) {
        if (typeof showToast === 'function') {
            showToast(`Error renaming binary cluster: ${err}`, 'error');
        }
    }
}

async function renameCluster(clusterId, currentName) {
    const newName = prompt(`Enter new name for cluster ${clusterId}:`, currentName);
    if (!newName || newName === currentName) return;

    const collection = getCollectionFromHash();
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
            const el = document.getElementById(`name-display-${clusterId}`);
            if (el) el.innerText = newName;
            updateD3ClusterName(clusterId, newName);
            if (typeof showToast === 'function') {
                showToast(`Cluster renamed to ${newName}`, 'success');
            }
        } else {
            if (typeof showToast === 'function') {
                showToast("Failed to rename cluster", "error");
            }
        }
    } catch (e) {
        console.error(e);
        if (typeof showToast === 'function') {
            showToast("Error renaming cluster", "error");
        }
    }
}

function switchClusterView(mode) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    params.set('view', mode);
    if (mode !== 'hierarchy' && mode !== 'packing') {
        params.delete('show_parents');
        params.delete('show_children');
        params.delete('show_members');
        params.delete('path_compression');
        params.delete('show_binary_sankey');
    }
    window.location.hash = `#clusters?${params.toString()}`;
}

function switchBinClusterView(mode) {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '');
    params.set('view', mode);
    if (mode !== 'hierarchy' && mode !== 'packing') {
        params.delete('show_parents');
        params.delete('show_children');
        params.delete('show_members');
        params.delete('path_compression');
    }
    window.location.hash = `#bin-clusters?${params.toString()}`;
}

function changeBinClusterNameType(value) {
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const params = new URLSearchParams(queryString);
    params.set('cluster_name_type', value);
    window.location.hash = `${hashPath}?${params.toString()}`;
}

function showTokenContextMenu(e) {
    if (window.setTrigger) window.setTrigger(e);
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

document.addEventListener('DOMContentLoaded', () => {
    if (window.self !== window.top) {
        document.body.classList.add('in-iframe');
    }

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

    document.addEventListener('contextmenu', e => {
        const menu = document.getElementById('graph-context-menu');
        if (menu && menu.style.display === 'block' && !menu.contains(e.target)) {
            window.closeGraphContextMenu();
        }
        if (e.target.closest('.feature-highlight')) {
            showTokenContextMenu(e);
        }
    });
});

// Expose dashboard controllers/globals explicitly on window
window.applyAdvancedFuncSearch = applyAdvancedFuncSearch;
window.applySimSearch = applySimSearch;
window.applyBinSimSearch = applyBinSimSearch;
window.applyClusterSearch = applyClusterSearch;
window.switchClusterView = switchClusterView;
window.renameCluster = renameCluster;
window.refreshData = refreshData;
window.clearFilters = clearFilters;
window.selectCollection = selectCollection;
window.resetColumnWidths = resetColumnWidths;
window.toggleSort = toggleSort;
window.applySearch = applySearch;
window.switchSimView = switchSimView;
window.debouncedSearch = debouncedSearch;
window.handleFilterKey = handleFilterKey;
window.toggleSidebar = toggleSidebar;
window.toggleHeader = toggleHeader;
window.toggleFilters = toggleFilters;
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

// ---------------------------------------------------------------------------
// Cross-window tag broadcast listener
// Receives bsimvis_tag_update from child iframes, updates dashboard UI and
// forwards to all other open managed windows (siblings).
// ---------------------------------------------------------------------------
window.addEventListener('message', (event) => {
    const msg = event.data;
    if (!msg || msg.type !== 'bsimvis_tag_update') return;

    const { action, tag, targets } = msg;
    if (!tag || !targets || !targets.length) return;

    // If we encounter a new tag, refresh metadata to get its color/priority
    if (action === 'add' && (!window.tagMetadata || !window.tagMetadata[tag])) {
        if (typeof fetchTagMetadata === 'function') {
            fetchTagMetadata(getCollectionFromHash());
        }
    }

    // 1. Update dashboard DOM (tables, tag editors)
    targets.forEach(({ etype, eid }) => {
        let editors = Array.from(document.querySelectorAll(`[data-etype="${etype}"][data-eid="${eid}"]`));

        // Similarity fallback: eid may be id1|id2|algo format
        if (editors.length === 0 && etype === 'similarity') {
            const parts = eid.split('|');
            if (parts.length >= 2) {
                const algoPart = parts.length > 2 ? `[data-algo="${parts[2]}"]` : '';
                const row = document.querySelector(`tr[data-id1="${parts[0]}"][data-id2="${parts[1]}"]${algoPart}`);
                if (row) {
                    const ed = row.querySelector('[data-etype="similarity"]');
                    if (ed) editors.push(ed);
                }
            }
        }

        if (editors.length === 0) return;

        if (action === 'add' && typeof updateUIForTagAdd === 'function') {
            updateUIForTagAdd(editors, tag);
        } else if (action === 'remove' && typeof updateUIForTagRemove === 'function') {
            updateUIForTagRemove(editors, tag);
        }
    });

    // 2. Refresh row colors and update sim graph with patched in-memory tag data
    if (typeof refreshAllRowColors === 'function') refreshAllRowColors();
    [window.graphInstance, window.hierarchyInstance, window.packingInstance, window.binHierarchyInstance, window.binPackingInstance].forEach(inst => {
        if (inst && typeof inst.applyTagUpdate === 'function') {
            targets.forEach(({ etype, eid }) => {
                inst.applyTagUpdate(action, etype, eid, tag);
            });
        }
    });

    if (typeof window.refreshContextMenuUI === 'function') {
        window.refreshContextMenuUI();
    }

    // 3. Forward to all other managed iframe windows (siblings of the sender)
    if (window.windowManager) {
        const senderFrame = event.source;
        window.windowManager.windows.forEach(win => {
            try {
                if (win.iframe && win.iframe.contentWindow && win.iframe.contentWindow !== senderFrame) {
                    win.iframe.contentWindow.postMessage(msg, '*');
                }
            } catch (e) { /* cross-origin, skip */ }
        });
    }
});

// --- Saved Filters & History Mechanism ---
const viewMetaData = {
    '#collections': { name: 'Collections', icon: 'fa-layer-group' },
    '#batches': { name: 'Batches', icon: 'fa-boxes-stacked' },
    '#files': { name: 'Files', icon: 'fa-file-code' },
    '#functions': { name: 'Functions', icon: 'fa-code' },
    '#features-global': { name: 'Features', icon: 'fa-fingerprint' },
    '#function-similarity': { name: 'Similarities', icon: 'fa-code-compare' },
    '#clusters': { name: 'Clusters', icon: 'fa-bullseye' },
    '#bin-clusters': { name: 'Bin Clusters', icon: 'fa-bullseye' },
    '#file-call-graph': { name: 'Call Graph', icon: 'fa-sitemap' }
};

function getFilterSummary(path, params) {
    const summary = [];
    const q = params.get('q');
    if (q) summary.push(`q: "${q}"`);

    if (path === '#files') {
        const file_name = params.get('file_name');
        const file_md5 = params.get('file_md5');
        const language_id = params.get('language_id');
        const min_function_count = params.get('min_function_count');
        const max_function_count = params.get('max_function_count');

        if (file_name) summary.push(`Name: "${file_name}"`);
        if (file_md5) summary.push(`MD5: ${file_md5.substring(0, 6)}`);
        if (language_id) summary.push(`Lang: ${language_id}`);
        if (min_function_count || max_function_count) {
            summary.push(`Funcs: ${min_function_count || 0}-${max_function_count || '∞'}`);
        }
        const min_cohesion = params.get('min_cohesion');
        const max_cohesion = params.get('max_cohesion');
        if (min_cohesion || max_cohesion) {
            summary.push(`Cohesion: ${min_cohesion || 0}-${max_cohesion || 1}`);
        }
    } else if (path === '#functions') {
        const function_name = params.get('function_name');
        const file_name = params.get('file_name');
        const file_md5 = params.get('file_md5');
        const min_features = params.get('min_features');
        const cluster_name = params.get('cluster_name');
        const entrypoint_address = params.get('entrypoint_address');

        if (function_name) summary.push(`Func: "${function_name}"`);
        if (file_name) summary.push(`File: "${file_name}"`);
        if (file_md5) summary.push(`MD5: ${file_md5.substring(0, 6)}`);
        if (entrypoint_address) summary.push(`Addr: ${entrypoint_address}`);
        if (min_features && min_features !== '0') summary.push(`Min Feat: ${min_features}`);
        if (cluster_name) summary.push(`Cluster: "${cluster_name}"`);
    } else if (path === '#function-similarity') {
        const name = params.get('name');
        const md5 = params.get('md5');
        const address = params.get('address');
        const min_score = params.get('min_score');
        const max_score = params.get('max_score');
        const algo = params.get('algo');
        const cross_binary = params.get('cross_binary');
        const match_mode = params.get('match_mode');

        if (name) summary.push(`Func: "${name}"`);
        if (md5) summary.push(`MD5: ${md5.substring(0, 6)}`);
        if (address) summary.push(`Addr: ${address}`);
        if (min_score && min_score !== '0.95') summary.push(`Score >= ${min_score}`);
        if (max_score && max_score !== '1.0') summary.push(`Score <= ${max_score}`);
        if (algo && algo !== 'unweighted_cosine') summary.push(`Algo: ${algo}`);
        if (cross_binary) {
            summary.push(cross_binary === 'true' ? 'Cross-Binary' : 'Same-Binary');
        }
        if (match_mode && match_mode !== 'any') summary.push(`Match: ${match_mode}`);
    } else if (path === '#clusters') {
        const cluster_uuid = params.get('cluster_uuid');
        const cluster_name = params.get('cluster_name');
        const min_count = params.get('min_count');
        const min_cohesion = params.get('min_cohesion');

        if (cluster_uuid) summary.push(`UUID: ${cluster_uuid.substring(0, 6)}`);
        if (cluster_name) summary.push(`Name: "${cluster_name}"`);
        if (min_count && min_count !== '0') summary.push(`Min Funcs: ${min_count}`);
        if (min_cohesion && min_cohesion !== '0') summary.push(`Cohesion >= ${min_cohesion}`);
    } else if (path === '#file-call-graph') {
        const file_md5 = params.get('file_md5');
        if (file_md5) summary.push(`File MD5: ${file_md5.substring(0, 8)}`);
    } else if (path === '#features-global') {
        const hash = params.get('hash');
        const type = params.get('type');
        const op = params.get('op');
        const min_tf = params.get('min_tf_score');
        const min_freq = params.get('min_frequency');

        if (hash) summary.push(`Hash: ${hash.substring(0, 8)}`);
        if (type) summary.push(`Type: ${type}`);
        if (op) summary.push(`Op: ${op}`);
        if (min_tf) summary.push(`Min TF: ${min_tf}`);
        if (min_freq) summary.push(`Min Freq: ${min_freq}`);
    }

    const tags = [];
    params.forEach((val, key) => {
        if (key.includes('tag') && !key.startsWith('exclude_') && val) {
            tags.push(val);
        }
    });
    if (tags.length > 0) {
        summary.push(`Tags: ${tags.join(',')}`);
    }

    return summary.join(', ') || 'No filters applied';
}

function getGraphTypeBadge(path, params) {
    const viewMode = params.get('view') || 'table';
    if (path === '#function-similarity') {
        return viewMode === 'graph' ? 'Graph' : 'Table';
    } else if (path === '#clusters') {
        if (viewMode === 'hierarchy') return 'Hierarchy';
        if (viewMode === 'packing') return 'Packing';
        return 'Table';
    } else if (path === '#file-call-graph') {
        return 'Call Graph';
    }
    return 'Table';
}

function formatRelativeTime(timestamp) {
    const diff = Date.now() - timestamp;
    if (diff < 60000) return 'Just now';
    const mins = Math.floor(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days}d ago`;
}

function addToHistory(path, queryString) {
    if (path === '#collections') return;

    const params = new URLSearchParams(queryString);
    const col = params.get('collection') || 'main';
    const view = params.get('view') || 'table';
    const summary = getFilterSummary(path, params);

    // Do not save to history if no filters were applied
    if (!summary || summary === 'No filters applied') {
        return;
    }

    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    const now = Date.now();
    const cleanParamsObj = {};
    params.forEach((val, key) => {
        if (cleanParamsObj[key]) {
            if (Array.isArray(cleanParamsObj[key])) {
                cleanParamsObj[key].push(val);
            } else {
                cleanParamsObj[key] = [cleanParamsObj[key], val];
            }
        } else {
            cleanParamsObj[key] = val;
        }
    });

    const newItem = {
        timestamp: now,
        path: path,
        collection: col,
        params: cleanParamsObj,
        view: view,
        summary: summary
    };

    // Helper to generate a standardized fingerprint ignoring key order
    const getFingerprint = (item) => {
        const sortedParams = {};
        if (item.params) {
            Object.keys(item.params).sort().forEach(k => {
                if (k !== 'collection' && k !== 'view') {
                    sortedParams[k] = item.params[k];
                }
            });
        }
        return `${item.collection || 'main'}:${item.path || ''}:${item.view || 'table'}:${JSON.stringify(sortedParams)}`;
    };

    const newItemFingerprint = getFingerprint(newItem);

    let mergedTyping = false;
    if (history.length > 0) {
        const last = history[0];
        const isSameView = last.path === path && last.collection === col && last.view === view;
        const timeDiff = now - last.timestamp;

        // Typing merge (debounce within 7 seconds)
        if (isSameView && timeDiff < 7000) {
            history[0] = newItem;
            mergedTyping = true;
        }
    }

    if (!mergedTyping) {
        // Full list deduplication: Filter out any existing matching query
        history = history.filter(item => getFingerprint(item) !== newItemFingerprint);
        // Move/unshift new item to top
        history.unshift(newItem);
    }

    if (history.length > 30) {
        history = history.slice(0, 30);
    }

    localStorage.setItem('bsimvis_search_history', JSON.stringify(history));
    renderHistoryDropdowns();
}

function serializeParams(paramsObj) {
    const params = new URLSearchParams();
    for (const [key, val] of Object.entries(paramsObj)) {
        if (Array.isArray(val)) {
            val.forEach(v => params.append(key, v));
        } else {
            params.set(key, val);
        }
    }
    return params.toString();
}

function loadHistoryItemByTimestamp(timestamp) {
    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    const item = history.find(h => h.timestamp === timestamp);
    if (!item) return;

    const qs = serializeParams(item.params);
    window.location.hash = `${item.path}?${qs}`;
    closeAllHistoryDropdowns();
}

function renderHistoryDropdowns() {
    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    const globalDropdown = document.getElementById('history-dropdown');
    const viewDropdown = document.getElementById('view-history-dropdown');
    const [currentPath, currentQueryString] = (window.location.hash || '#collections').split('?');
    const currentParams = new URLSearchParams(currentQueryString);
    const currentCol = currentParams.get('collection') || 'main';

    const esc = (str) => {
        if (!str) return '';
        return str.replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    };

    // 1. Render Global Dropdown
    if (globalDropdown) {
        const colHistory = history.filter(item => item.collection === currentCol);
        if (colHistory.length === 0) {
            globalDropdown.innerHTML = `
                <div class="history-dropdown-title">
                    <span>Search History</span>
                </div>
                <div class="history-empty">
                    <i class="fa-solid fa-clock-rotate-left" style="font-size: 1.5rem; opacity: 0.3;"></i>
                    <span>No search history yet.</span>
                </div>`;
        } else {
            let html = `
                <div class="history-dropdown-title">
                    <span>Search History</span>
                    <button class="history-dropdown-clear-btn" onclick="clearSearchHistory(event)">
                        <i class="fa-solid fa-trash-can"></i> Clear
                    </button>
                </div>`;
            colHistory.forEach(item => {
                const meta = viewMetaData[item.path] || { name: item.path, icon: 'fa-magnifying-glass' };
                const tempParams = new URLSearchParams();
                for (const [k, v] of Object.entries(item.params)) {
                    if (Array.isArray(v)) v.forEach(x => tempParams.append(k, x));
                    else tempParams.set(k, v);
                }
                const graphType = getGraphTypeBadge(item.path, tempParams);
                html += `
                    <div class="history-item" onclick="loadHistoryItemByTimestamp(${item.timestamp})">
                        <div class="history-item-header">
                            <i class="fa-solid ${meta.icon}"></i>
                            <span class="history-item-view-name">${meta.name}</span>
                            <span class="history-item-graph-type">${graphType}</span>
                            <span class="history-item-time" title="${new Date(item.timestamp).toLocaleString()}">${formatRelativeTime(item.timestamp)}</span>
                        </div>
                        <div class="history-item-summary" title="${esc(item.summary)}">${esc(item.summary)}</div>
                    </div>`;
            });
            globalDropdown.innerHTML = html;
        }
    }

    // 2. Render View-Specific Dropdown
    if (viewDropdown) {
        const viewHistory = history.filter(item => item.path === currentPath && item.collection === currentCol);
        if (viewHistory.length === 0) {
            viewDropdown.innerHTML = `
                <div class="history-dropdown-title">
                    <span>View History</span>
                </div>
                <div class="history-empty">
                    <i class="fa-solid fa-clock-rotate-left" style="font-size: 1.5rem; opacity: 0.3;"></i>
                    <span>No history for this view.</span>
                </div>`;
        } else {
            let html = `
                <div class="history-dropdown-title">
                    <span>View History</span>
                    <button class="history-dropdown-clear-btn" onclick="clearViewHistory(event, '${currentPath}')">
                        <i class="fa-solid fa-trash-can"></i> Clear View
                    </button>
                </div>`;
            viewHistory.forEach(item => {
                const meta = viewMetaData[item.path] || { name: item.path, icon: 'fa-magnifying-glass' };
                const tempParams = new URLSearchParams();
                for (const [k, v] of Object.entries(item.params)) {
                    if (Array.isArray(v)) v.forEach(x => tempParams.append(k, x));
                    else tempParams.set(k, v);
                }
                const graphType = getGraphTypeBadge(item.path, tempParams);
                html += `
                    <div class="history-item" onclick="loadHistoryItemByTimestamp(${item.timestamp})">
                        <div class="history-item-header">
                            <i class="fa-solid ${meta.icon}"></i>
                            <span class="history-item-view-name">${meta.name}</span>
                            <span class="history-item-graph-type">${graphType}</span>
                            <span class="history-item-time" title="${new Date(item.timestamp).toLocaleString()}">${formatRelativeTime(item.timestamp)}</span>
                        </div>
                        <div class="history-item-summary" title="${esc(item.summary)}">${esc(item.summary)}</div>
                    </div>`;
            });
            viewDropdown.innerHTML = html;
        }
    }
}

function clearSearchHistory(event) {
    if (event) {
        event.stopPropagation();
        event.preventDefault();
    }
    const [currentPath, currentQueryString] = (window.location.hash || '#collections').split('?');
    const currentParams = new URLSearchParams(currentQueryString);
    const currentCol = currentParams.get('collection') || 'main';

    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    // Keep items from other collections
    const filtered = history.filter(item => item.collection !== currentCol);
    localStorage.setItem('bsimvis_search_history', JSON.stringify(filtered));
    renderHistoryDropdowns();
}

function clearViewHistory(event, path) {
    if (event) {
        event.stopPropagation();
        event.preventDefault();
    }
    const [currentPath, currentQueryString] = (window.location.hash || '#collections').split('?');
    const currentParams = new URLSearchParams(currentQueryString);
    const currentCol = currentParams.get('collection') || 'main';

    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    // Keep items that are not in this view or not in this collection
    const filtered = history.filter(item => !(item.path === path && item.collection === currentCol));
    localStorage.setItem('bsimvis_search_history', JSON.stringify(filtered));
    renderHistoryDropdowns();
}

function toggleHistoryDropdown(event) {
    if (event) {
        event.stopPropagation();
        event.preventDefault();
    }
    const dropdown = document.getElementById('history-dropdown');
    if (!dropdown) return;
    const isVisible = dropdown.style.display === 'block';

    closeAllHistoryDropdowns();

    if (!isVisible) {
        renderHistoryDropdowns();
        dropdown.style.display = 'block';
        const chev = document.getElementById('nav-history-chevron');
        if (chev) chev.style.transform = 'rotate(180deg)';
    }
}

function toggleViewHistoryDropdown(event) {
    if (event) {
        event.stopPropagation();
        event.preventDefault();
    }
    const dropdown = document.getElementById('view-history-dropdown');
    if (!dropdown) return;
    const isVisible = dropdown.style.display === 'block';

    closeAllHistoryDropdowns();

    if (!isVisible) {
        renderHistoryDropdowns();
        dropdown.style.display = 'block';
    }
}

function closeAllHistoryDropdowns() {
    const globalDropdown = document.getElementById('history-dropdown');
    const viewDropdown = document.getElementById('view-history-dropdown');
    const flyout = document.getElementById('history-flyout');

    if (globalDropdown) globalDropdown.style.display = 'none';
    if (viewDropdown) viewDropdown.style.display = 'none';
    if (flyout) flyout.style.display = 'none';

    const chev = document.getElementById('nav-history-chevron');
    if (chev) chev.style.transform = 'rotate(0deg)';
}

// Close dropdowns on outside click
document.addEventListener('click', (e) => {
    if (!e.target.closest('.history-dropdown-container') && !e.target.closest('.view-history-container') && !e.target.closest('#history-flyout')) {
        closeAllHistoryDropdowns();
    }
    if (!e.target.closest('.export-dropdown-container')) {
        closeExportDropdown();
    }
});

// --- Graph Settings Sync ---
function saveGraphSettings() {
    const settings = {
        showLabel: document.getElementById('graph-show-label')?.value || 'none',
        colorBinary: document.getElementById('graph-color-binary')?.value || 'binary',
        colorFunction: document.getElementById('graph-color-function')?.value || 'binary',
        colorSim: document.getElementById('graph-color-sim')?.value || 'gradient',
        bundleTension: document.getElementById('graph-bundle-tension')?.value || '0.85',
        linkWidth: document.getElementById('graph-link-width')?.value || '1.0',
        scaleWidth: document.getElementById('graph-scale-width')?.checked ?? true,
        activeProfile: document.querySelector('#profile-toggle .view-btn.active')?.getAttribute('data-profile') || 'default'
    };
    localStorage.setItem('similarityGraphSettings', JSON.stringify(settings));
}
window.saveGraphSettings = saveGraphSettings;

function restoreGraphSettings() {
    const raw = localStorage.getItem('similarityGraphSettings');
    if (!raw) return;
    try {
        const settings = JSON.parse(raw);
        const setVal = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.value = val;
        };
        if (settings.showLabel !== undefined) setVal('graph-show-label', settings.showLabel);
        if (settings.colorBinary !== undefined) setVal('graph-color-binary', settings.colorBinary);
        if (settings.colorFunction !== undefined) setVal('graph-color-function', settings.colorFunction);
        if (settings.colorSim !== undefined) setVal('graph-color-sim', settings.colorSim);
        if (settings.bundleTension !== undefined) setVal('graph-bundle-tension', settings.bundleTension);
        if (settings.linkWidth !== undefined) setVal('graph-link-width', settings.linkWidth);

        const chk = document.getElementById('graph-scale-width');
        if (chk && settings.scaleWidth !== undefined) chk.checked = settings.scaleWidth;

        // Restore active profile button selection
        if (settings.activeProfile) {
            document.querySelectorAll('#profile-toggle .view-btn').forEach(btn => {
                if (btn.getAttribute('data-profile') === settings.activeProfile) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
            if (window.graphInstance) {
                window.graphInstance.applyProfile(settings.activeProfile);
            }
        }
    } catch (e) {
        console.error("Failed to restore graph settings", e);
    }
}
window.restoreGraphSettings = restoreGraphSettings;

function downloadSearchResults(format) {
    const [hashPath, queryString] = (window.location.hash || '#collections').split('?');
    const route = routes[hashPath];
    if (!route || !route.api) {
        alert("Downloads are not available for this view.");
        return;
    }

    const params = new URLSearchParams(queryString);
    params.set('format', format);
    // For downloads, we want to fetch all matches, so we set limit to a large number
    params.set('limit', '100000');

    const downloadUrl = route.api + '?' + params.toString();

    // Create a temporary anchor element to trigger the browser download
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.setAttribute('download', '');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}
window.downloadSearchResults = downloadSearchResults;

function toggleExportDropdown(e) {
    if (e) e.stopPropagation();
    const dropdown = document.getElementById('export-dropdown');
    if (!dropdown) return;
    if (dropdown.style.display === 'none' || !dropdown.style.display) {
        dropdown.style.display = 'block';
    } else {
        dropdown.style.display = 'none';
    }
}
window.toggleExportDropdown = toggleExportDropdown;

function closeExportDropdown() {
    const dropdown = document.getElementById('export-dropdown');
    if (dropdown) dropdown.style.display = 'none';
}
window.closeExportDropdown = closeExportDropdown;

async function refreshFunctionRow(funcId) {
    const row = document.querySelector(`.sim-row[data-id="${funcId}"]`);
    if (!row) return;
    
    const parts = funcId.split(':');
    const collection = parts[0];
    const md5 = parts[2];
    const addr = parts[3];
    
    try {
        // Use exact ID search
        const res = await fetch(`/api/function/search?collection=${collection}&entrypoint_address=${addr}&file_md5=${md5}`);
        const data = await res.json();
        if (data.functions && data.functions.length > 0) {
            const f = data.functions[0];
            const newHtml = renderFunctions([f], {});
            const temp = document.createElement('tbody');
            temp.innerHTML = newHtml;
            const newRow = temp.firstElementChild;
            row.replaceWith(newRow);
        }
    } catch (e) {
        console.error("Failed to refresh function row:", e);
    }
}

window.refreshFunctionRow = refreshFunctionRow;
