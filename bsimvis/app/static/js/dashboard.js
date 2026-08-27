// Main Dashboard Controller for BSimVis

const windowManager = new WindowManager();
window.windowManager = windowManager;

// Backend config defaults, loaded once at startup (see get_config in index.py).
// Falls back to the same values baked into bsimvis_config.toml.
window.APP_CONFIG = null;
async function loadAppConfig() {
    try {
        const res = await fetch('/api/index/config');
        if (res.ok) window.APP_CONFIG = await res.json();
    } catch (e) {
        console.error("Failed to load app config", e);
    }
}
loadAppConfig();
function defaultMinScore() {
    const v = window.APP_CONFIG?.similarity?.min_score;
    return (v !== undefined && v !== null) ? String(v) : '0.9';
}

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
    const { viewKey } = getRoutingState();
    try {
        const saved = JSON.parse(localStorage.getItem('columnWidths') || '{}');
        delete saved[viewKey];
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
let lastPathName = '';
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

// Views whose pool-scoped results can span several collections.
const COLLECTION_COLUMN_VIEWS = ['files', 'functions', 'function-similarity', 'binary-similarity'];

const routes = {
    'collections': {
        title: 'Collections',
        api: '/api/collection/search',
        headers: [
            { label: 'Name', sort: 'name' },
            { label: 'Batches', sort: 'total_batches' },
            { label: 'Files', sort: 'total_files' },
            { label: 'Functions', sort: 'total_functions' },
            { label: 'Last Updated', sort: 'last_updated' },
            'Status',
            'Actions'
        ],
        renderer: renderCollections
    },
    'pools': {
        title: 'Pools',
        api: '/api/pool',
        headers: [
            { label: 'Pool ID', sort: 'id', width: '15%' },
            { label: 'Name', sort: 'name', width: '15%' },
            { label: 'Collections', width: '20%' },
            { label: 'Files', sort: 'total_files', width: '6%' },
            { label: 'Funcs', sort: 'total_functions', width: '6%' },
            { label: 'Sims', sort: 'total_func_similarities', width: '6%' },
            { label: 'Clusters', sort: 'total_func_clusters', width: '7%' },
            { label: 'Sync Status', sort: 'sync_status', width: '8%' },
            { label: 'Created At', sort: 'created_at', width: '10%' },
            { label: 'Actions', width: '7%' }
        ],
        renderer: renderPools
    },
    'batches': {
        title: 'Batches',
        api: '/api/batch/search',
        headers: ['Batch Name', 'UUID', 'Files', 'Functions', 'Timestamp', 'Status', 'Actions'],
        renderer: renderBatches
    },
    'tags': {
        title: 'Tags',
        api: '/api/tags/list',
        headers: [
            { label: 'Tag', sort: 'tag', width: '30%' },
            { label: 'Color', width: '8%' },
            { label: 'Priority', sort: 'priority', width: '10%' },
            { label: 'LLM', width: '6%' },
            { label: 'Functions', sort: 'function_count', width: '12%' },
            { label: 'Files', sort: 'file_count', width: '12%' },
            { label: 'Similarities', width: '12%' },
            { label: 'Actions', width: '10%' }
        ],
        renderer: renderTagVocabulary
    },
    'files': {
        title: 'Files',
        api: '/api/file/search',
        headers: [
            { label: 'Filename', width: '17%' },
            { label: 'MD5 / Arch', width: '11%' },
            { label: 'Metadata', width: '14%' },
            { label: 'Batch UUID', width: '9%' },
            { label: 'Status', width: '7%' },
            { label: 'Funcs', width: '7%', sort: 'function_count' },
            { label: 'Notes', width: '3%' },
            { label: 'Clusters', width: '12%' },
            { label: 'Entry Date', width: '8%', sort: 'entry_date' },
            { label: 'Tags', width: '16%' }
        ],
        renderer: renderFiles
    },
    'functions': {
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
    'features-global': {
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
    'function-similarity': {
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
    'clusters': {
        title: 'Function Clusters',
        api: '/api/cluster/list',
        headers: [
            { label: 'UUID', sort: 'cluster_uuid', width: '10%' },
            { label: 'Name', sort: 'cluster_name', width: '18%' },
            { label: 'Functions', sort: 'count', width: '12%' },
            { label: 'Stability', sort: 'stability', width: '8%' },
            { label: 'Avg Feat', sort: 'features', width: '8%' },
            { label: 'Cohesion', sort: 'cohesion', width: '8%' },
            { label: 'Created', width: '11%' },
            { label: 'Tags', width: '14%' },
            { label: 'Sample Functions', width: '11%' }
        ],
        renderer: renderClusters
    },
    'upload': {
        title: 'Upload Binaries',
        api: null,
        headers: [],
        renderer: null
    },
    'binary-similarity': {
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
            { label: 'Pair', width: '10%' },
            { label: 'Tags', width: '20%' },
        ],
        renderer: renderBinSimPairs
    },
    'bin-clusters': {
        title: 'Binary Clusters',
        api: '/api/bin_cluster/list',
        headers: [
            { label: 'UUID', sort: 'cluster_uuid', width: '10%' },
            { label: 'Name', sort: 'cluster_name', width: '20%' },
            { label: 'Binaries', sort: 'count', width: '12%' },
            { label: 'Stability', sort: 'stability', width: '8%' },
            { label: 'Cohesion', sort: 'cohesion', width: '8%' },
            { label: 'Created', width: '11%' },
            { label: 'Tags', width: '16%' },
            { label: 'Sample Binaries', width: '15%' }
        ],
        renderer: renderBinClusters
    },
    'jobs': {
        title: 'Background Jobs',
        api: '/api/jobs',
        headers: [
            { label: 'Task / ID', width: '27%' },
            { label: 'Collection', width: '10%' },
            { label: 'Target', width: '12%' },
            { label: 'Status', width: '10%' },
            { label: 'Progress', width: '15%' },
            { label: 'Created', width: '11%' },
            { label: 'Duration', width: '7%' },
            { label: 'Actions', width: '8%' }
        ],
        renderer: (data) => window.renderJobs(data)
    }
};

function clearFilters() {
    const { viewKey, params } = getRoutingState();
    const newParams = new URLSearchParams();

    // Preserved context keys
    const preserved = ['algo', 'view'];
    preserved.forEach(k => {
        if (params.has(k)) newParams.set(k, params.get(k));
    });

    // Set default cohesion threshold. 'clusters'/'bin-clusters' reset to a
    // strict browse default; 'functions'/'files' get their own 0.5 default
    // from navigate() below -- forcing 0.95 here for every view hid clusters
    // between 0.5 and 0.95 cohesion after a Clear Filters click anywhere.
    if (viewKey === 'clusters' || viewKey === 'bin-clusters') {
        newParams.set('min_cohesion', '0.95');
    }

    currentOffset = 0;
    isEndOfResults = false;
    window.isClearingFilters = true;
    navigate(viewKey, newParams);

    // Re-apply sim view defaults when clearing within the sim view
    if (viewKey === 'function-similarity') {
        applySimViewDefaults(viewKey, newParams.toString());
    }
}

window.ModuleLoader = {
    currentModule: null,

    async loadView(viewName, params) {


        const dashboardContainer = document.getElementById('dashboard-view-container');
        const moduleContainer = document.getElementById('module-view-container');

        if (dashboardContainer) dashboardContainer.style.display = 'none';
        if (moduleContainer) {
            moduleContainer.style.display = 'flex';
            moduleContainer.innerHTML = '<div style="display:flex; justify-content:center; align-items:center; height:100%; color:var(--dim);"><i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading View...</div>';
        }

        if (this.currentModule && typeof this.currentModule.destroy === 'function') {
            this.currentModule.destroy();
        }

        const moduleMap = {
            'function': window.FunctionView,
            'file': window.FileView,
            'diff': window.DiffView,
            'call_graph': window.CallGraphView,
            'feature': window.FeatureView,
            'function_features': window.FunctionFeaturesView,
            'home': window.HomeView,
            'pool-detail': window.PoolDetailView,
            'collection-detail': window.CollectionDetailView,
            'bin_sim': {
                init: (p) => {
                    if (window.renderBinarySimilarityView) {
                        const searchParams = new URLSearchParams();
                        for (let k in p) if (p[k] !== undefined) searchParams.set(k, p[k]);
                        moduleContainer.innerHTML = '<div id="binary-similarity-container" style="flex:1; display:flex; flex-direction:column; overflow:hidden;"></div>';
                        window.renderBinarySimilarityView(searchParams, 'binary-similarity-container');
                    }
                }
            }
        };

        const module = moduleMap[viewName];
        if (module && typeof module.init === 'function') {
            this.currentModule = module;
            try {
                await module.init(params, 'module-view-container');
            } catch (err) {
                console.error(`Failed to init module ${viewName}:`, err);
                if (moduleContainer) moduleContainer.innerHTML = `<div style="padding:20px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> Error loading view: ${err.message}</div>`;
            }
        } else {
            console.error(`Module not found or invalid: ${viewName}`);
            if (moduleContainer) moduleContainer.innerHTML = `<div style="padding:20px; color:#f92672;"><i class="fa-solid fa-triangle-exclamation"></i> View module "${viewName}" not found.</div>`;
        }
    },

    showDashboard() {
        if (this.currentModule && typeof this.currentModule.destroy === 'function') {
            this.currentModule.destroy();
        }
        this.currentModule = null;

        const dashboardContainer = document.getElementById('dashboard-view-container');
        const moduleContainer = document.getElementById('module-view-container');

        if (moduleContainer) {
            moduleContainer.style.display = 'none';
            moduleContainer.innerHTML = '';
        }
        if (dashboardContainer) dashboardContainer.style.display = 'flex';
    }
};

function hideDashboardActions() {
    const toHide = [
        document.getElementById('search-settings-container'),
        document.getElementById('header-clear-btn'), // filter-actions-container
        document.getElementById('collapse-header-btn')
    ];
    toHide.forEach(el => {
        if (el) el.style.display = 'none';
    });
    const settingsPanel = document.getElementById('ui-settings-panel');
    if (settingsPanel) settingsPanel.style.display = 'none';
}

function showDashboardActions() {
    const toShow = [
        document.getElementById('header-clear-btn'), // filter-actions-container
        document.getElementById('collapse-header-btn'),
        document.getElementById('header-settings-btn')
    ];
    toShow.forEach(el => {
        if (el) el.style.display = '';
    });
}

function toggleFilterActionsDropdown(event) {
    event.stopPropagation();
    const dd = document.getElementById('filter-actions-dropdown');
    if (!dd) return;
    const isOpen = dd.style.display !== 'none';
    if (isOpen) {
        dd.style.display = 'none';
    } else {
        dd.style.display = 'block';
        // Close on outside click
        const close = (e) => {
            if (!dd.contains(e.target) && e.target !== event.currentTarget) {
                dd.style.display = 'none';
                document.removeEventListener('click', close);
            }
        };
        setTimeout(() => document.addEventListener('click', close), 0);
    }
}

function closeFilterActionsDropdown() {
    const dd = document.getElementById('filter-actions-dropdown');
    if (dd) dd.style.display = 'none';
}

// Add a NOT exclude tag card for 'ignore' to all visible tag filter containers
function addNotIgnoreFilters() {
    const tagContainerIds = ["sim", "func", "file", "bin-sim", "cluster", "bin-cluster"];
    const typeMap = { "sim": "sim_tag", "func": "func_tag", "file": "file_tag", "bin-sim": "file_tag", "cluster": "cluster_tag", "bin-cluster": "cluster_tag" };
    let added = 0;
    tagContainerIds.forEach(key => {
        const container = document.getElementById(`tag-container-${key}`);
        if (!container) return;
        // Only act on visible containers
        if (container.offsetParent === null) return;
        // Check if 'ignore' exclude card already exists
        const already = Array.from(container.querySelectorAll('.tag-filter-card')).find(
            c => c.dataset.value === 'ignore' && c.dataset.exclude === 'true'
        );
        if (already) return;
        createTagCard(key, typeMap[key], 'ignore', true);
        added++;
    });
    if (added > 0) triggerTagSearch();
}

async function refreshData(appendArg = false, force = false, skipHeader = false) {
    if (window.updateJobStatusIcon) window.updateJobStatusIcon();
    const append = (appendArg === true);
    const { viewKey, collection, pool, params } = getRoutingState();


    // Check if we should load a module view
    if (['home', 'function', 'file', 'diff', 'call_graph', 'feature', 'bin_sim', 'function_features', 'pool-detail', 'collection-detail'].includes(viewKey)) {
        const stateParams = Object.fromEntries(params);
        stateParams.collection = collection;
        stateParams.pool = pool;
        stateParams.view = viewKey;
        if (window.Breadcrumbs) {
            const segments = window.Breadcrumbs.generate({ viewKey, collection, pool, params }, null);
            window.Breadcrumbs.render(segments);
        }
        hideDashboardActions();
        if (typeof updateNavbarLinks === 'function') {
            updateNavbarLinks(collection);
        }
        if (typeof UI !== 'undefined' && UI.Sidebar && typeof UI.Sidebar.updateActiveState === 'function') {
            UI.Sidebar.updateActiveState();
        }
        await ModuleLoader.loadView(viewKey, stateParams);
        return;
    }

    ModuleLoader.showDashboard();

    const route = routes[viewKey];
    if (!route) return;

    // Set default parameters for search views if not present
    if (viewKey === 'files') {
        if (!params.has('min_cohesion')) {
            params.set('min_cohesion', '0.5');
        }
    } else if (viewKey === 'functions') {
        if (!params.has('min_cohesion')) {
            params.set('min_cohesion', '0.5');
        }
    } else if (viewKey === 'function-similarity') {
        if (!params.has('min_score')) {
            params.set('min_score', defaultMinScore());
        }
        if (!params.has('max_score')) {
            params.set('max_score', '1.0');
        }
    }

    updateNavVisibility(collection);

    const currentUrlPath = window.location.pathname + window.location.search;
    const isSilent = (currentUrlPath === lastPathName && !force && !append);

    if (currentUrlPath !== lastPathName || !append) {
        if (!isSilent) {
            currentOffset = 0;
            isEndOfResults = false;
            // Only clear if switching view types, otherwise just show loader
            if (viewKey !== lastViewPath) {
                document.getElementById('table-body').innerHTML = '';
            } else if (!append) {
                // Dim the existing content during refresh instead of clearing
                document.getElementById('table-body').style.opacity = '0.5';
            }
            document.getElementById('loader').style.display = 'block';
        }
    }
    lastPathName = currentUrlPath;
    if (viewKey === 'pools') {
        if (!skipHeader) renderPoolCreationForm();
    } else if (viewKey === 'tags') {
        if (!skipHeader) renderTagCreationForm();
    } else if (viewKey === 'jobs') {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) {
            const hasPool = !!(pool || localStorage.getItem('lastPoolContext'));
            const hasCol = !!(collection || localStorage.getItem('lastCollectionContext'));
            const showContextBtn = hasPool || hasCol;

            // Determine if we are currently filtered by context
            const isContextFiltered = !!(pool || collection);

            let buttonText = "Context Jobs";
            if (pool) {
                buttonText = "Pool Jobs";
            } else if (collection) {
                buttonText = "Collection Jobs";
            } else {
                const lastPool = localStorage.getItem('lastPoolContext');
                const lastCol = localStorage.getItem('lastCollectionContext');
                if (lastPool) {
                    buttonText = "Pool Jobs";
                } else if (lastCol) {
                    buttonText = "Collection Jobs";
                }
            }

            gridHeader.innerHTML = `
                <div style="padding: 10px 15px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; background: var(--hover);">
                    <div style="display: flex; gap: 10px; align-items: center;">
                        ${showContextBtn ? `
                            <button class="top-action-btn ${isContextFiltered ? 'active' : ''}" onclick="window.goToContextJobs()" style="${isContextFiltered ? 'background: var(--accent); color: var(--bg);' : ''}">
                                <i class="fa-solid fa-filter"></i> ${buttonText}
                            </button>
                        ` : ''}
                        <button class="top-action-btn ${!isContextFiltered ? 'active' : ''}" onclick="window.goToAllJobs()" style="${!isContextFiltered ? 'background: var(--accent); color: var(--bg);' : ''}">
                            <i class="fa-solid fa-globe"></i> All Jobs
                        </button>
                    </div>
                    <div style="display: flex; gap: 8px; align-items: center;">
                        <span style="font-size: 0.8rem; color: var(--dim);">
                            ${isContextFiltered ? `Viewing jobs for <b>${pool ? 'Pool: ' + pool : 'Collection: ' + collection}</b>` : 'Viewing all jobs in all collections'}
                        </span>
                        <button class="top-action-btn" onclick="window.expandAllPipelines()" title="Expand all pipelines" style="font-size: 0.75rem; padding: 3px 8px;">
                            <i class="fa-solid fa-expand"></i> Expand All
                        </button>
                        <button class="top-action-btn" onclick="window.collapseAllPipelines()" title="Collapse all pipelines" style="font-size: 0.75rem; padding: 3px 8px;">
                            <i class="fa-solid fa-compress"></i> Collapse All
                        </button>
                    </div>
                </div>
            `;
        }
    } else if (viewKey === 'binary-similarity') {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) {
            const p = new URLSearchParams(params);
            gridHeader.innerHTML = `
                <div style="padding: 24px; border-bottom: 1px solid var(--border); background: var(--bg); display: flex; flex-direction: column;">
                    <div id="bsim-hero-text" style="transition: max-height 0.3s ease, opacity 0.3s ease; overflow: hidden; max-height: 200px; opacity: 1;">
                        <p style="margin: 0 0 20px 0; font-size: 0.95rem; color: var(--subtle); max-width: 800px; line-height: 1.5;">
                            Search and compare similarities between binaries and containers based on shared code, libraries, and content. 
                            Use the filters below to refine your view by scoring methodology and artifact type.
                        </p>
                    </div>
                    <div style="display: flex; gap: 24px; flex-wrap: wrap;">
                        <div class="home-card" style="padding: 16px; min-width: 300px;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 12px;">
                                <h3 style="margin: 0; font-size: 0.9rem; color: var(--text);">Scoring Metric</h3>
                                <span class="home-tip" tabindex="0" data-tip="The dimensions of similarity calculated between two binaries. Overall combines multiple factors, while Library, Code, and Content scores isolate specific types of matches."><i class="fa-solid fa-circle-info"></i></span>
                            </div>
                            ${binSimScoreTypeTagsHtml(p)}
                        </div>
                        <div class="home-card" style="padding: 16px; min-width: 300px;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 12px;">
                                <h3 style="margin: 0; font-size: 0.9rem; color: var(--text);">Node Type</h3>
                                <span class="home-tip" tabindex="0" data-tip="A node is the artifact being compared. It can be a single parsed File (e.g. an ELF binary), or a Container holding multiple files (e.g. an APK, MachO, or Zip archive) which aggregates matches from its contents."><i class="fa-solid fa-circle-info"></i></span>
                            </div>
                            ${binSimNodeTypeTagsHtml(p)}
                        </div>
                        <div class="home-card" style="padding: 16px; min-width: 220px;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 12px;">
                                <h3 style="margin: 0; font-size: 0.9rem; color: var(--text);">Packer</h3>
                                <span class="home-tip" tabindex="0" data-tip="A UPX-packed binary is analyzed and compared as real code (packed-vs-unpacked is a normal diff), so it can otherwise dominate a search with packer-stub matches that say nothing about the payload's capabilities. Hide it to see only unpacked code."><i class="fa-solid fa-circle-info"></i></span>
                            </div>
                            ${binSimHidePackedTagHtml(p)}
                        </div>
                    </div>
                </div>
            `;
            
            // Asynchronously fetch counts for the pills
            setTimeout(async () => {
                const fetchCount = async (paramKey, paramVal, elId) => {
                    try {
                        const u = new URLSearchParams(params);
                        u.set('limit', 0); // Only return total count
                        if (paramKey) u.set(paramKey, paramVal);
                        // collection/pool land in `params` later in refreshData (after the
                        // await fetchTagMetadata call below); this timer can fire first on
                        // a slow/remote backend, so set them from local scope directly.
                        if (collection) u.set('collection', collection); else u.delete('collection');
                        if (pool) u.set('pool', pool); else u.delete('pool');

                        const res = await fetch('/api/bin_sim/search?' + u.toString());
                        if (!res.ok) return;
                        const data = await res.json();
                        if (data && data.total !== undefined) {
                            const el = document.getElementById(elId);
                            if (el) el.innerText = '(' + data.total.toLocaleString() + ')';
                        }
                    } catch (e) {}
                };
                
                // Fetch for Score Types
                const types = window.BinSimScoreTypes || { score: {} };
                for (const v of Object.keys(types)) {
                    fetchCount('sort', v, 'bsim-count-score-' + v);
                }
                
                // Fetch for Node Types
                fetchCount('containers', 'none', 'bsim-count-nt-file');
                fetchCount('containers', 'both', 'bsim-count-nt-container');

                // Hide Packed count: fetchCount only sets one key=val pair, but this
                // filter is client-side sugar for exclude_file_tag (see
                // applyBinSimSearch), so append it directly rather than teaching
                // fetchCount a param it can't forward to the backend.
                (async () => {
                    try {
                        const u = new URLSearchParams(params);
                        u.set('limit', 0);
                        if (collection) u.set('collection', collection); else u.delete('collection');
                        if (pool) u.set('pool', pool); else u.delete('pool');
                        u.append('exclude_file_tag', 'packer:upx');
                        const res = await fetch('/api/bin_sim/search?' + u.toString());
                        if (!res.ok) return;
                        const data = await res.json();
                        if (data && data.total !== undefined) {
                            const el = document.getElementById('bsim-count-hide-packed');
                            if (el) el.innerText = '(' + data.total.toLocaleString() + ')';
                        }
                    } catch (e) {}
                })();
            }, 50);
            
            // Collapse hero text on table scroll
            const tableBodyWrap = document.getElementById('table-body-wrap');
            if (tableBodyWrap) {
                if (window.bsimHeroScrollListener) {
                    tableBodyWrap.removeEventListener('scroll', window.bsimHeroScrollListener);
                }
                window.bsimHeroScrollListener = function() {
                    const heroText = document.getElementById('bsim-hero-text');
                    if (!heroText) return;
                    if (tableBodyWrap.scrollTop > 30) {
                        heroText.style.maxHeight = '0';
                        heroText.style.opacity = '0';
                    } else {
                        heroText.style.maxHeight = '200px';
                        heroText.style.opacity = '1';
                    }
                };
                tableBodyWrap.addEventListener('scroll', window.bsimHeroScrollListener, { passive: true });
            }
        }
    } else if (viewKey === 'function-similarity') {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) {
            const p = new URLSearchParams(params);
            gridHeader.innerHTML = `
                <div style="padding: 24px; border-bottom: 1px solid var(--border); background: var(--bg); display: flex; flex-direction: column;">
                    ${simFilterPillsHtml(p)}
                </div>`;
        }
    } else if (viewKey === 'collections') {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) {
            renderHeroHeader(gridHeader, 'collections-hero-text',
                'Collections group uploaded binaries and their analysis results. Upload new files to create or extend a collection, or open one below to explore its files, functions, and similarities.',
                `<a href="/upload" onclick="Nav.openPath(this.href, event)" class="top-action-btn" style="background:var(--accent); color:var(--bg); padding:10px 20px; font-size:0.95rem; display:inline-flex; align-items:center; gap:8px; border-radius:8px; text-decoration:none; font-weight:600; width:fit-content;">
                    <i class="fa-solid fa-cloud-arrow-up"></i> Upload Binaries
                </a>`);
        }
    } else if (viewKey === 'bin-clusters') {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) {
            const p = new URLSearchParams(params);
            const viewMode = p.get('view') || 'table';
            if (viewMode === 'table') {
                const nodeType = p.get('node_type') || 'file';
                const fileActive = nodeType === 'file';
                const containerActive = nodeType === 'container';
                gridHeader.innerHTML = `
                    <div style="padding: 24px; border-bottom: 1px solid var(--border); background: var(--bg); display: flex; flex-direction: column;">
                        <div style="display: flex; gap: 24px; flex-wrap: wrap;">
                            <div class="home-card" style="padding: 16px; min-width: 300px;">
                                <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 12px;">
                                    <h3 style="margin: 0; font-size: 0.9rem; color: var(--text);">Node Type</h3>
                                    <span class="home-tip" tabindex="0" data-tip="Switch between viewing single-file clusters and top-level container clusters."><i class="fa-solid fa-circle-info"></i></span>
                                </div>
                                <div style="display:flex; flex-wrap:wrap; gap:8px;">
                                    <span class="bsim-nt-pill" onclick="changeBinClusterNodeType('file')" style="${binSimPillStyle(fileActive, 'var(--info, #3b82f6)')}" title="View file clusters"><i class="fa-solid fa-file"></i>File</span>
                                    <span class="bsim-nt-pill" onclick="changeBinClusterNodeType('container')" style="${binSimPillStyle(containerActive, 'var(--warning, #d97706)')}" title="View container clusters"><i class="fa-solid fa-box"></i>Container</span>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            } else {
                gridHeader.innerHTML = '';
            }
        }
    } else {
        const gridHeader = document.getElementById('grid-header');
        if (gridHeader) gridHeader.innerHTML = '';
    }


    if (viewKey === 'clusters' || viewKey === 'bin-clusters') {
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
    if (viewKey !== 'collections') {
        localStorage.setItem(`savedFilters:${collection}:${viewKey}`, params.toString() || `collection=${collection}`);
        addToHistory(viewKey, params.toString());
    }

    // Ensure tag metadata is loaded for views that use it (functions, similarities, and files)
    if (['functions', 'function-similarity', 'binary-similarity', 'files'].includes(viewKey)) {
        await fetchTagMetadata(collection);
    }

    if (viewKey === 'function-similarity') {
        // Caching strategy: Use cache ONLY for 'Load More' (append) or 'Switch View' (same query)
        const queryParams = new URLSearchParams(window.location.search);
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

    // Ensure collection and pool are in params for the API call
    if (viewKey !== 'jobs' && viewKey !== 'pools') {
        params.set('collection', collection);
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        if (pool) {
            params.set('pool', pool);
        } else {
            params.delete('pool');
        }
    } else if (viewKey === 'jobs') {
        if (collection) {
            params.set('collection', collection);
        } else {
            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.has('collection')) {
                params.set('collection', urlParams.get('collection'));
            } else {
                params.delete('collection');
            }
        }
        if (pool) {
            params.set('pool', pool);
        } else {
            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.has('pool')) {
                params.set('pool', urlParams.get('pool'));
            } else {
                params.delete('pool');
            }
        }
        params.delete('pool_id');
    } else {
        params.delete('collection');
        params.delete('pool');
        params.delete('pool_id');
    }

    let apiUrl = route.api + (params.toString() ? '?' + params.toString() : '');
    // Neighbours of one file, folded into the containers they came from. The
    // faceted pair search this view normally uses is index-backed and cannot
    // group, so grouping is served by the neighbour endpoint instead.
    if (viewKey === 'binary-similarity' && params.get('group') === 'container' && params.get('md5')) {
        apiUrl = '/api/bin_sim/list?' + params.toString();
    }
    updateUI(viewKey, collection, params, route, force);

    const isGraphView = params.get('view') === 'graph' || params.get('view') === 'hierarchy' || params.get('view') === 'packing';
    if ((isGraphView && (viewKey === 'function-similarity' || viewKey === 'binary-similarity' || viewKey === 'clusters')) || !route.api) {
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
        const items = data.pools || data.items || data.results || data.files || data.functions || data.features || data.pairs || data.collections || data.batches || (Array.isArray(data) ? data : []);
        const total = data.total !== undefined ? data.total : (data.total_estimated !== undefined ? data.total_estimated : (Array.isArray(data) ? data.length : (items.length || 0)));

        const totalEl = document.getElementById('view-total');
        const poolIcon = document.getElementById('pool-warn-icon');
        const limitIcon = document.getElementById('limit-warn-icon');
        const poolInput = document.getElementById('sim-pool-limit');
        const limitInput = document.getElementById('sim-limit');

        if (totalEl) {
            totalEl.style.display = 'inline-block';

            if (poolIcon) {
                if (data.pool_truncated && (viewKey === 'function-similarity' || viewKey === 'functions')) {
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
                if (total >= currentLimit && (viewKey === 'function-similarity' || viewKey === 'functions')) {
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
            if (viewKey === 'functions' || viewKey === 'function-similarity') {
                clustersMap = data.clusters || {};
            } else if (viewKey === 'files') {
                clustersMap = data.bin_cluster_map || {};
            }

            const html = clustersMap !== undefined ? route.renderer(items, clustersMap) : route.renderer(items);
            if (html) tbody.insertAdjacentHTML('beforeend', html);
        }

        let count = items.length;

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

        renderPagination(viewKey);
    } catch (err) {
        console.error(err);
    } finally {
        document.getElementById('loader').style.display = 'none';
        document.getElementById('table-body').style.opacity = '1';
    }
}

function updateNavbarLinks(col) {
    const pool = window.getRoutingState ? window.getRoutingState().pool : null;
    if (!col && !pool) return;
    
    const updateNavLink = (id, targetView) => {
        const el = document.getElementById(id);
        if (!el) return;
        const saved = localStorage.getItem(`savedFilters:${col}:${targetView}`);

        let url;
        if (pool) {
            const poolId = pool;
            url = `/pools/${encodeURIComponent(poolId)}/${targetView}`;
            if (targetView === 'collections') {
                url = `/collections`;
            } else if (targetView === 'jobs') {
                url = col ? `/pools/${encodeURIComponent(poolId)}/collections/${encodeURIComponent(col)}/jobs` : `/pools/${encodeURIComponent(poolId)}/jobs`;
            } else if (targetView === 'files') {
                url = `/pools/${encodeURIComponent(poolId)}/files`;
            } else if (targetView === 'functions') {
                url = `/pools/${encodeURIComponent(poolId)}/functions`;
            } else if (targetView === 'batches') {
                url = `/pools/${encodeURIComponent(poolId)}/batches`;
            } else if (targetView === 'features-global') {
                url = `/pools/${encodeURIComponent(poolId)}/features`;
            } else if (targetView === 'upload') {
                url = `/pools/${encodeURIComponent(poolId)}/upload`;
            } else if (targetView === 'function-similarity') {
                url = `/pools/${encodeURIComponent(poolId)}/functions/similarities`;
            } else if (targetView === 'binary-similarity') {
                url = `/pools/${encodeURIComponent(poolId)}/files/similarities`;
            } else if (targetView === 'clusters') {
                url = `/pools/${encodeURIComponent(poolId)}/functions/clusters`;
            } else if (targetView === 'bin-clusters') {
                url = `/pools/${encodeURIComponent(poolId)}/files/clusters`;
            }
        } else {
            url = `/collections/${encodeURIComponent(col)}/${targetView}`;
            if (targetView === 'collections') {
                url = `/collections`;
            } else if (targetView === 'jobs') {
                url = `/collections/${encodeURIComponent(col)}/jobs`;
            } else if (targetView === 'files') {
                url = `/collections/${encodeURIComponent(col)}/files`;
            } else if (targetView === 'functions') {
                url = `/collections/${encodeURIComponent(col)}/functions`;
            } else if (targetView === 'batches') {
                url = `/collections/${encodeURIComponent(col)}/batches`;
            } else if (targetView === 'features-global') {
                url = `/collections/${encodeURIComponent(col)}/features`;
            } else if (targetView === 'upload') {
                url = `/collections/${encodeURIComponent(col)}/upload`;
            } else if (targetView === 'function-similarity') {
                url = `/collections/${encodeURIComponent(col)}/functions/similarities`;
            } else if (targetView === 'binary-similarity') {
                url = `/collections/${encodeURIComponent(col)}/files/similarities`;
            } else if (targetView === 'clusters') {
                url = `/collections/${encodeURIComponent(col)}/functions/clusters`;
            } else if (targetView === 'bin-clusters') {
                url = `/collections/${encodeURIComponent(col)}/files/clusters`;
            }
        }

        if (saved && targetView !== 'jobs') {
            const savedParams = new URLSearchParams(saved);
            savedParams.delete('collection'); // Already in path
            savedParams.delete('pool'); // Already in path
            if (savedParams.toString()) url += `?${savedParams.toString()}`;
        }
        el.href = url;

        // Intercept click for SPA navigation
        el.onclick = (e) => {
            if (e.ctrlKey || e.metaKey) return;
            e.preventDefault();
            const currentState = getRoutingState();
            if (currentState.viewKey === targetView) {
                clearFilters();
            } else {
                Nav.openPath(url, e);
            }
        };
    };

    updateNavLink('nav-collections', 'collections');
    updateNavLink('nav-batches', 'batches');
    updateNavLink('nav-files', 'files');
    updateNavLink('nav-functions', 'functions');
    updateNavLink('nav-features-global', 'features-global');
    updateNavLink('nav-function-similarity', 'function-similarity');
    updateNavLink('nav-clusters', 'clusters');
    updateNavLink('nav-bin-clusters', 'bin-clusters');
    updateNavLink('nav-binary-similarity', 'binary-similarity');
    updateNavLink('nav-upload', 'upload');
    updateNavLink('nav-jobs', 'jobs');
}
window.updateNavbarLinks = updateNavbarLinks;

// Generic collapsible orientation header for views that otherwise drop the
// user straight into an empty grid: one-line explainer + optional action
// button(s), same collapse-on-scroll behavior as the bin-sim hero text.
function renderHeroHeader(gridHeader, heroId, text, actionsHtml) {
    gridHeader.innerHTML = `
        <div style="padding: 24px; border-bottom: 1px solid var(--border); background: var(--bg); display: flex; flex-direction: column; gap: 16px;">
            <div id="${heroId}" style="transition: max-height 0.3s ease, opacity 0.3s ease; overflow: hidden; max-height: 200px; opacity: 1;">
                <p style="margin: 0; font-size: 0.95rem; color: var(--subtle); max-width: 800px; line-height: 1.5;">${text}</p>
            </div>
            ${actionsHtml || ''}
        </div>
    `;
    const tableBodyWrap = document.getElementById('table-body-wrap');
    if (!tableBodyWrap) return;
    const listenerKey = heroId + 'ScrollListener';
    if (window[listenerKey]) tableBodyWrap.removeEventListener('scroll', window[listenerKey]);
    window[listenerKey] = function() {
        const heroText = document.getElementById(heroId);
        if (!heroText) return;
        if (tableBodyWrap.scrollTop > 30) {
            heroText.style.maxHeight = '0';
            heroText.style.opacity = '0';
        } else {
            heroText.style.maxHeight = '200px';
            heroText.style.opacity = '1';
        }
    };
    tableBodyWrap.addEventListener('scroll', window[listenerKey], { passive: true });
}

// Bin-sim search filters: score type (Overall/Code/Library/Content) and node
// type (File/Container) render as clickable tag pills -- same visual language
// as the score sub-cards in the results view -- instead of <select> dropdowns.
// Both live behind hidden inputs (#bsim-score-type, #bsim-containers) so
// applyBinSimSearch's generic id->param reader in binary_similarity.js needs
// no changes.
function binSimPillStyle(active, color) {
    color = color || 'var(--accent)';
    return `display:inline-flex; align-items:center; gap:6px; padding:6px 12px; border-radius:8px; font-size:0.85rem; font-weight:600; cursor:pointer; white-space:nowrap; border:1px solid ${active ? color : 'var(--border)'}; color:${active ? color : 'var(--subtle)'}; background:${active ? color + '22' : 'var(--window-tray, transparent)'};`;
}

function binSimScoreTypeTagsHtml(p) {
    const active = p.get('sort') || 'score';
    const types = window.BinSimScoreTypes || { score: { label: 'Overall', icon: 'fa-solid fa-layer-group', color: 'var(--success)' } };
    const pills = Object.entries(types).map(([v, meta]) => {
        const on = v === active;
        return `<span class="bsim-tag-pill" data-value="${v}" onclick="setBinSimScoreType('${v}')" style="${binSimPillStyle(on, meta.color)}" title="${escapeAttr(meta.label)}"><i class="${meta.icon}"></i>${meta.label} <span id="bsim-count-score-${v}" style="font-size:0.75rem; opacity:0.8; font-weight:normal;"></span></span>`;
    }).join('');
    return `<input type="hidden" id="bsim-score-type" value="${escapeAttr(active)}"><div id="bsim-score-type-tags" style="display:flex; flex-wrap:wrap; gap:8px;">${pills}</div>`;
}

function binSimNodeTypeTagsHtml(p) {
    const cur = p.has('containers') ? p.get('containers') : 'none';
    const fileActive = cur !== 'both';
    const containerActive = cur !== 'none';
    return `<input type="hidden" id="bsim-containers" value="${escapeAttr(cur)}">
        <div style="display:flex; flex-wrap:wrap; gap:8px;">
            <span id="bsim-nt-file" class="bsim-nt-pill" onclick="toggleBinSimNodeType('file')" style="${binSimPillStyle(fileActive, 'var(--info, #3b82f6)')}" title="Include file ↔ file pairs"><i class="fa-solid fa-file"></i>File <span id="bsim-count-nt-file" style="font-size:0.75rem; opacity:0.8; font-weight:normal;"></span></span>
            <span id="bsim-nt-container" class="bsim-nt-pill" onclick="toggleBinSimNodeType('container')" style="${binSimPillStyle(containerActive, 'var(--warning, #d97706)')}" title="Include container ↔ container pairs"><i class="fa-solid fa-box"></i>Container <span id="bsim-count-nt-container" style="font-size:0.75rem; opacity:0.8; font-weight:normal;"></span></span>
        </div>`;
}

// Independent of Node Type: it hides pairs where either side is UPX-packed,
// regardless of File/Container. Packed binaries are still real files with
// their own similarity document (not containers), so they aren't covered by
// the Node Type partition and need their own switch.
function binSimHidePackedTagHtml(p) {
    const active = p.get('hide_packed') === 'true';
    return `<input type="hidden" id="bsim-hide-packed" value="${active ? 'true' : ''}">
        <span id="bsim-hide-packed-pill" onclick="toggleBinSimHidePacked()" style="${binSimPillStyle(active, 'var(--danger, #dc2626)')}" title="Hide pairs where either binary is UPX-packed -- packer stub matches are nice for reference but drown out the payload's real capabilities"><i class="fa-solid fa-box-archive"></i>Hide Packed <span id="bsim-count-hide-packed" style="font-size:0.75rem; opacity:0.8; font-weight:normal;"></span></span>`;
}

function toggleBinSimHidePacked() {
    const el = document.getElementById('bsim-hide-packed');
    if (!el) return;
    el.value = el.value === 'true' ? '' : 'true';
    if (window.applyBinSimSearch) window.applyBinSimSearch();
}
window.toggleBinSimHidePacked = toggleBinSimHidePacked;

function setBinSimScoreType(v) {
    const el = document.getElementById('bsim-score-type');
    if (el) el.value = v;
    if (window.applyBinSimSearch) window.applyBinSimSearch();
}
window.setBinSimScoreType = setBinSimScoreType;

function toggleBinSimNodeType(which) {
    const el = document.getElementById('bsim-containers');
    if (!el) return;
    let fileActive = el.value !== 'both';
    let containerActive = el.value !== 'none';
    if (which === 'file') fileActive = !fileActive; else containerActive = !containerActive;
    if (!fileActive && !containerActive) { fileActive = true; containerActive = true; } // never both off
    el.value = (fileActive && containerActive) ? 'all' : (fileActive ? 'none' : 'both');
    if (window.applyBinSimSearch) window.applyBinSimSearch();
}
window.toggleBinSimNodeType = toggleBinSimNodeType;

function syncBinSimTags(p) {
    const active = p.get('sort') || 'score';
    const hidden = document.getElementById('bsim-score-type');
    if (hidden) hidden.value = active;
    document.querySelectorAll('#bsim-score-type-tags .bsim-tag-pill').forEach(el => {
        const meta = (window.BinSimScoreTypes || {})[el.dataset.value] || {};
        const on = el.dataset.value === active;
        el.setAttribute('style', binSimPillStyle(on, meta.color));
    });
    const cur = p.has('containers') ? p.get('containers') : 'none';
    const hiddenC = document.getElementById('bsim-containers');
    if (hiddenC) hiddenC.value = cur;
    const fileActive = cur !== 'both';
    const containerActive = cur !== 'none';
    const fileEl = document.getElementById('bsim-nt-file');
    const contEl = document.getElementById('bsim-nt-container');
    if (fileEl) fileEl.setAttribute('style', binSimPillStyle(fileActive, 'var(--info, #3b82f6)'));
    if (contEl) contEl.setAttribute('style', binSimPillStyle(containerActive, 'var(--warning, #d97706)'));

    const hidePacked = p.get('hide_packed') === 'true';
    const hiddenP = document.getElementById('bsim-hide-packed');
    if (hiddenP) hiddenP.value = hidePacked ? 'true' : '';
    const pillP = document.getElementById('bsim-hide-packed-pill');
    if (pillP) pillP.setAttribute('style', binSimPillStyle(hidePacked, 'var(--danger, #dc2626)'));
}

// Same card/pill treatment as the bin-sim hero, applied to the
// function-similarity search page's Algorithm/Cross Binary/Match Mode
// controls -- previously three plain <select>s buried under a mislabeled
// "Date" column header.
const SimAlgoOptions = [
    { v: 'unweighted_cosine', label: 'Cosine', icon: 'fa-solid fa-arrows-left-right' },
    { v: 'jaccard', label: 'Jaccard', icon: 'fa-solid fa-object-group' },
    { v: 'milvus_sparse', label: 'Milvus Sparse', icon: 'fa-solid fa-braille' },
];
const SimCrossBinaryOptions = [
    { v: '', label: 'All Binaries', icon: 'fa-solid fa-globe' },
    { v: 'false', label: 'Same Binary', icon: 'fa-solid fa-file' },
    { v: 'true', label: 'Cross Binary', icon: 'fa-solid fa-shuffle' },
];
const SimMatchModeOptions = [
    { v: 'any', label: 'Match Any', icon: 'fa-solid fa-check' },
    { v: 'both', label: 'Match Both', icon: 'fa-solid fa-check-double' },
];

function simPillGroupHtml(groupClass, options, active, color) {
    return options.map(o => `<span class="${groupClass}" data-value="${escapeAttr(o.v)}" onclick="setSimPill('${groupClass}', '${o.v}')" style="${binSimPillStyle(o.v === active, color)}" title="${escapeAttr(o.label)}"><i class="${o.icon}"></i>${o.label}</span>`).join('');
}

function simFilterPillsHtml(p) {
    const algo = p.get('algo') || 'unweighted_cosine';
    const crossBinary = p.has('cross_binary') ? p.get('cross_binary') : '';
    const matchMode = p.get('match_mode') || 'any';
    return `
        <input type="hidden" id="sim-algo" value="${escapeAttr(algo)}">
        <input type="hidden" id="sim-cross-binary" value="${escapeAttr(crossBinary)}">
        <input type="hidden" id="sim-match-mode" value="${escapeAttr(matchMode)}">
        <div style="display:flex; gap:24px; flex-wrap:wrap;">
            <div class="home-card" style="padding:16px; min-width:220px;">
                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Algorithm</h3>
                <div id="sim-algo-pills" style="display:flex; flex-wrap:wrap; gap:8px;">${simPillGroupHtml('sim-algo-pill', SimAlgoOptions, algo, 'var(--info, #3b82f6)')}</div>
            </div>
            <div class="home-card" style="padding:16px; min-width:220px;">
                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Cross Binary</h3>
                <div id="sim-cross-binary-pills" style="display:flex; flex-wrap:wrap; gap:8px;">${simPillGroupHtml('sim-cb-pill', SimCrossBinaryOptions, crossBinary, 'var(--warning, #d97706)')}</div>
            </div>
            <div class="home-card" style="padding:16px; min-width:180px;">
                <h3 style="margin:0 0 12px 0; font-size:0.9rem; color:var(--text);">Match Mode</h3>
                <div id="sim-match-mode-pills" style="display:flex; flex-wrap:wrap; gap:8px;">${simPillGroupHtml('sim-mm-pill', SimMatchModeOptions, matchMode, 'var(--accent, #9333ea)')}</div>
            </div>
        </div>`;
}

function setSimPill(groupClass, value) {
    const idByClass = { 'sim-algo-pill': 'sim-algo', 'sim-cb-pill': 'sim-cross-binary', 'sim-mm-pill': 'sim-match-mode' };
    const hidden = document.getElementById(idByClass[groupClass]);
    if (hidden) hidden.value = value;
    if (window.applySimSearch) window.applySimSearch();
}
window.setSimPill = setSimPill;

function syncSimFilterPills(p) {
    const groups = [
        ['sim-algo', 'sim-algo-pill', p.get('algo') || 'unweighted_cosine', 'var(--info, #3b82f6)'],
        ['sim-cross-binary', 'sim-cb-pill', p.has('cross_binary') ? p.get('cross_binary') : '', 'var(--warning, #d97706)'],
        ['sim-match-mode', 'sim-mm-pill', p.get('match_mode') || 'any', 'var(--accent, #9333ea)'],
    ];
    groups.forEach(([inputId, pillClass, active, color]) => {
        const hidden = document.getElementById(inputId);
        if (hidden) hidden.value = active;
        document.querySelectorAll(`.${pillClass}`).forEach(el => {
            el.setAttribute('style', binSimPillStyle(el.dataset.value === active, color));
        });
    });
}

function updateUI(viewKey, collection, params, route, force = false) {
    showDashboardActions();
    const routingState = getRoutingState();
    if (window.Breadcrumbs) {
        const segments = window.Breadcrumbs.generate(routingState, route);
        window.Breadcrumbs.render(segments);
    }

    const path = viewKey;
    const pathChanged = (viewKey !== lastViewPath);
    lastViewPath = viewKey;
    const col = collection;

    // Common Elements
    const gview = document.getElementById('graph-view-container');
    const hview = document.getElementById('hierarchy-view-container');
    const pview = document.getElementById('packing-view-container');
    const pag = document.getElementById('pagination-container');
    const settingsEl = document.getElementById('search-settings-container');
    const chordView = document.getElementById('chord-view-container');
    const tableWrap = document.getElementById('table-wrap');
    const tableBodyWrap = document.getElementById('table-body-wrap');
    const searchArea = document.getElementById('search-area');

    // Reset all special view containers and stop active processes
    if (gview) gview.style.display = 'none';
    if (hview) hview.style.display = 'none';
    if (pview) pview.style.display = 'none';
    if (document.getElementById('call-graph-view-container')) document.getElementById('call-graph-view-container').style.display = 'none';
    if (document.getElementById('upload-view-container')) document.getElementById('upload-view-container').style.display = 'none';
    if (document.getElementById('binary-similarity-container')) document.getElementById('binary-similarity-container').style.display = 'none';
    if (chordView) chordView.style.display = 'none';
    if (document.getElementById('binary-density-view-container')) document.getElementById('binary-density-view-container').style.display = 'none';
    if (searchArea) searchArea.style.display = 'flex'; // Default to visible, specific views might hide it

    // Clear all autocomplete dropdowns to prevent leftovers from previous navigation
    document.querySelectorAll('.tag-autocomplete-dropdown').forEach(el => el.remove());

    if (tableWrap) {
        tableWrap.style.display = 'flex';
        tableWrap.style.flex = '1';
    }
    if (tableBodyWrap) tableBodyWrap.style.display = '';

    if (pag) pag.style.display = 'flex';

    if (window.graphInstance) window.graphInstance.stop();
    if (window.hierarchyInstance) window.hierarchyInstance.stop();
    if (window.packingInstance) window.packingInstance.stop();
    if (window.callGraphInstance) window.callGraphInstance.stop();
    if (window.chordGraphInstance) window.chordGraphInstance.stop();


    if (viewKey === 'upload') {
        if (tableWrap) tableWrap.style.display = 'none';
        if (tableBodyWrap) tableBodyWrap.style.display = 'none';
        if (pag) pag.style.display = 'none';
        
        const uploadView = document.getElementById('upload-view-container');
        const isAlreadyVisible = uploadView.style.display === 'block';
        const currentContext = uploadView.dataset.context;
        uploadView.style.display = 'block';

        if ((!isAlreadyVisible || force || currentContext !== collection) && typeof renderUploadView === 'function') {
            uploadView.dataset.context = collection;
            renderUploadView(params);
        }
    }
 else if (viewKey === 'binary-similarity') {
        document.getElementById('header-top-actions').style.display = 'flex';
    } else {
        document.getElementById('header-top-actions').style.display = 'flex';
    }

    // Sidebar
    document.querySelectorAll('nav a').forEach(a => a.classList.remove('active'));
    const navLink = document.getElementById('nav-' + viewKey);

    if (navLink) navLink.classList.add('active');

    // Titles
    document.getElementById('view-title').innerText = route.title;

    // Side Collections Info
    const viewHistoryBtnContainer = document.querySelector('.view-history-container');
    if (viewHistoryBtnContainer) {
        viewHistoryBtnContainer.style.display = viewKey === 'collections' ? 'none' : 'block';
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

    const pool = window.getRoutingState ? window.getRoutingState().pool : null;
    if (col || pool) {
        updateNavbarLinks(col);
        if (typeof UI !== 'undefined' && UI.Sidebar && typeof UI.Sidebar.updateActiveState === 'function') {
            UI.Sidebar.updateActiveState();
        }
    }

    if (viewKey === 'function-similarity' && params.get('view') === 'graph') {
        restoreGraphSettings();
    }

    // Table Head
    const thead = document.getElementById('table-head');
    const dataTable = document.getElementById('data-table');
    const dataTableHeader = document.getElementById('data-table-header');

    if (pathChanged) {
        let headHtml = '<tr>';

        // Pool searches span collections: append a Collection column, matching
        // the trailing cell renderCollectionCell() emits in the row renderers.
        const routeHeaders = (pool && !col && COLLECTION_COLUMN_VIEWS.includes(viewKey))
            ? [...route.headers, { label: 'Collection', width: '8%' }]
            : route.headers;

        const savedForRoute = JSON.parse(localStorage.getItem('columnWidths') || '{}')[viewKey];
        const hasSavedWidths = savedForRoute && Object.keys(savedForRoute).length > 0;
        const hasWidths = routeHeaders.some(h => typeof h === 'object' && h.width) || hasSavedWidths;

        const tableLayout = hasWidths ? 'fixed' : 'auto';
        if (dataTable) dataTable.style.tableLayout = tableLayout;
        if (dataTableHeader) dataTableHeader.style.tableLayout = tableLayout;
        routeHeaders.forEach(h => {
            const label = typeof h === 'string' ? h : h.label;
            const sortKey = typeof h === 'object' ? h.sort : null;
            let width = typeof h === 'object' ? h.width : 'auto';

            // Apply saved width if exists
            const savedWidth = getSavedColumnWidth(viewKey, label);
            if (savedWidth) width = savedWidth;

            let style = width !== 'auto' ? `style="width:${width}"` : '';
            const resizerHtml = `<div class="resizer"></div>`;

            if (sortKey) {
                const currentSort = params.get('sort_by');
                const currentOrder = params.get('sort_order') || 'desc';
                // ponytail: bin-sim "Score" column tracks whichever score type is active
                const effectiveSortKey = (viewKey === 'binary-similarity' && sortKey === 'score')
                    ? (params.get('sort') || 'score') : sortKey;
                const icon = (currentSort === effectiveSortKey) ? (currentOrder === 'desc' ? '▼' : '▲') : '↕';
                headHtml += `<th ${style} class="sortable resizable-th" data-label="${escapeAttr(label)}" data-sort="${escapeAttr(sortKey)}" onclick="toggleSort(${escapeAttr(jsString(sortKey))})">${escapeHtml(label)} <small>${icon}</small>${resizerHtml}</th>`;
            } else {
                headHtml += `<th ${style} class="resizable-th" data-label="${label}">${label}${resizerHtml}</th>`;
            }
        });
        headHtml += '</tr>';
        thead.innerHTML = headHtml;

        // Reset UI settings and display containers to defaults for all views
        if (settingsEl) {
            settingsEl.style.display = 'none';
            settingsEl.innerHTML = '';
        }
        if (viewKey !== 'upload') {
            if (tableWrap) {
                tableWrap.style.display = 'flex';
                tableWrap.style.flex = '1';
            }
            if (tableBodyWrap) tableBodyWrap.style.display = '';
            if (pag) pag.style.display = 'flex';
        }
        if (gview) gview.style.display = 'none';
        if (hview) hview.style.display = 'none';
        if (pview) pview.style.display = 'none';
    } else {
        // Surgical Sort Icon Update
        const currentSort = params.get('sort_by');
        const currentOrder = params.get('sort_order') || 'desc';
        // ponytail: read the key off data-sort, not out of the onclick string —
        // the quoting there is escapeAttr(jsString(...))'s business, not ours.
        thead.querySelectorAll('th.sortable').forEach(th => {
            const sortKey = th.dataset.sort;
            const effectiveSortKey = (viewKey === 'binary-similarity' && sortKey === 'score')
                ? (params.get('sort') || 'score') : sortKey;
            const small = th.querySelector('small');
            if (sortKey && small) {
                small.innerText = (currentSort === effectiveSortKey) ? (currentOrder === 'desc' ? '▼' : '▲') : '↕';
            }
        });
    }

    if (pathChanged) {
        if (path === 'function-similarity' || path === 'functions' || path === 'files' || path === 'clusters' || path === 'bin-clusters' || path === 'features-global' || path === 'binary-similarity' || path === 'jobs' || path === 'collections' || path === 'pools') {
            const applyFn = path === 'function-similarity' ? 'applySimSearch' : (path === 'functions' ? 'applyAdvancedFuncSearch' : (path === 'files' ? 'applyAdvancedFileSearch' : (path === 'features-global' ? 'applyAdvancedFeatureSearch' : (path === 'binary-similarity' ? 'applyBinSimSearch' : (path === 'bin-clusters' ? 'applyBinClusterSearch' : (path === 'clusters' ? 'applyClusterSearch' : (path === 'collections' ? 'applyCollectionSearch' : (path === 'pools' ? 'applyPoolSearch' : 'applyJobSearch'))))))));

            let settingsHtml = '';
            settingsEl.style.display = 'flex';

            if (path === 'jobs') {
                settingsHtml += `
                    <div style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" id="job-auto-refresh" ${localStorage.getItem('jobAutoRefresh') !== 'false' ? 'checked' : ''} onchange="localStorage.setItem('jobAutoRefresh', this.checked)" style="cursor:pointer; vertical-align:middle;">
                        <label for="job-auto-refresh" style="font-size:0.75rem; color:var(--text); cursor:pointer; font-weight:bold;">Auto-Refresh</label>
                    </div>
                    <button id="job-pause-toggle" class="view-btn" onclick="toggleJobPause()" title="Pause/resume all workers (fleet-wide)">…</button>`;
            } else {
                const viewMode = params.get('view') || 'table';
                const poolLimit = params.get('pool_limit') || '1000000';
                const countLimit = params.get('limit') || (viewMode === 'graph' ? DEFAULT_GRAPH_LIMIT : DEFAULT_PAGE_LIMIT);

                if (path === 'function-similarity') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchSimView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'graph' ? 'active' : ''}" onclick="switchSimView('graph')">Graph</button>
                        </div>`;
                } else if (path === 'binary-similarity') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchBinSimView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'graph' ? 'active' : ''}" onclick="switchBinSimView('graph')">Graph</button>
                        </div>`;
                } else if (path === 'clusters') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchClusterView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'hierarchy' ? 'active' : ''}" onclick="switchClusterView('hierarchy')">Graph</button>
                            <button class="view-btn ${viewMode === 'packing' ? 'active' : ''}" onclick="switchClusterView('packing')">Packing</button>
                        </div>`;
                } else if (path === 'bin-clusters') {
                    settingsHtml += `
                        <div class="view-toggle">
                            <button class="view-btn ${viewMode === 'table' ? 'active' : ''}" onclick="switchBinClusterView('table')">Table</button>
                            <button class="view-btn ${viewMode === 'hierarchy' ? 'active' : ''}" onclick="switchBinClusterView('hierarchy')">Graph</button>
                            <button class="view-btn ${viewMode === 'packing' ? 'active' : ''}" onclick="switchBinClusterView('packing')">Packing</button>
                        </div>`;
                }

                if (path === 'function-similarity' || path === 'functions') {
                    settingsHtml += `
                        <span class="dim" style="font-size:0.65rem; margin-left:15px;">Pool Limit:</span>
                        <div style="position:relative; display:inline-flex; align-items:center;">
                            <input type="number" id="sim-pool-limit" value="${escapeAttr(poolLimit)}" step="100000" min="1000" max="1000000" 
                                title="Max candidates to score / filter" 
                                style="width:70px; background:var(--border); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                                onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})">
                            <span id="pool-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Pool Truncated: Not all candidates were scored.">⚠️</span>
                        </div>`;
                }

                settingsHtml += `
                    <span class="dim" style="font-size:0.65rem; margin-left:15px;">Limit:</span>
                    <div style="position:relative; display:inline-flex; align-items:center;">
                        <input type="number" id="sim-limit" value="${escapeAttr(countLimit)}" step="10" min="1" max="50000" 
                            title="Max results to display (Output Limit)" 
                            style="width:60px; background:var(--border); color:var(--accent); border:1px solid var(--accent); font-size:0.65rem; border-radius:4px; padding:2px 5px;" 
                            onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})">
                        <span id="limit-warn-icon" style="display:none; cursor:help; margin-left:4px; font-size:0.8rem;" title="Output Limit Reached: Results are capped.">ℹ️</span>
                    </div>
                `;
            }
            settingsEl.innerHTML = settingsHtml;
            if (path === 'jobs' && window.refreshPauseButton) window.refreshPauseButton();

            const p = new URLSearchParams(params);
            let headHtml = thead.innerHTML; // Start with the <tr> built above

            if (path === 'files' || path === 'functions' || path === 'function-similarity' || path === 'features-global') {
                headHtml += `<tr class="filter-row">`;
                if (path === 'features-global') {
                    headHtml += `
                        <th><input type="text" id="flt-feat-hash" placeholder="Hash..." value="${escapeAttr(p.get('hash') || '')}" onfocus="attachAutocomplete(this, 'feature', 'hash', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:4px;">
                                <input type="text" id="flt-feat-type" placeholder="Type..." value="${escapeAttr(p.get('type') || '')}" onfocus="attachAutocomplete(this, 'feature', 'type', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                                <input type="text" id="flt-feat-op" placeholder="Op..." value="${escapeAttr(p.get('op') || '')}" onfocus="attachAutocomplete(this, 'feature', 'op', (val) => { this.value = val; applyAdvancedFeatureSearch(); })" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th></th><th></th>
                        <th>
                            <div style="display:flex; align-items:center; gap:2px;">
                                <input type="number" id="flt-feat-min-tf" placeholder="Min..." value="${escapeAttr(p.get('min_tf_score') || '')}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                <span class="dim" style="font-size:0.6rem">-</span>
                                <input type="number" id="flt-feat-max-tf" placeholder="Max..." value="${escapeAttr(p.get('max_tf_score') || '')}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th>
                            <div style="display:flex; align-items:center; gap:2px;">
                                <input type="number" id="flt-feat-min-freq" placeholder="Min..." value="${escapeAttr(p.get('min_frequency') || '')}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                <span class="dim" style="font-size:0.6rem">-</span>
                                <input type="number" id="flt-feat-max-freq" placeholder="Max..." value="${escapeAttr(p.get('max_frequency') || '')}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th></th>`;
                } else if (path === 'files') {
                    headHtml += `
                        <th><input type="text" id="flt-file-name" placeholder="Name..." value="${escapeAttr(p.get('file_name') || '')}" onfocus="attachAutocomplete(this, 'file', 'file_name', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:4px;">
                                <input type="text" id="flt-file-md5" placeholder="MD5..." value="${escapeAttr(p.get('file_md5') || '')}" onfocus="attachAutocomplete(this, 'file', 'file_md5', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                                <input type="text" id="flt-file-language" placeholder="Lang..." value="${escapeAttr(p.get('language_id') || '')}" onfocus="attachAutocomplete(this, 'file', 'language_id', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:4px;">
                                <input type="text" id="flt-file-yara" placeholder="Yara..." value="${escapeAttr(p.get('yara') || '')}" onfocus="attachAutocomplete(this, 'file', 'yara', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                                <input type="text" id="flt-file-avtype" placeholder="AVType..." value="${escapeAttr(p.get('avtype') || '')}" onfocus="attachAutocomplete(this, 'file', 'avtype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                                <input type="text" id="flt-file-ccip" placeholder="CC IP..." value="${escapeAttr(p.get('cc_ip') || '')}" onfocus="attachAutocomplete(this, 'file', 'cc_ip', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                                <hr style="margin: 2px 0; border: none; border-top: 1px solid var(--border);">
                                <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 2px;">
                                    <input type="text" id="flt-file-inf-yara" placeholder="Inf.Yara" title="Inferred Yara" value="${escapeAttr(p.get('inferred_yara') || '')}" onfocus="attachAutocomplete(this, 'file', 'inferred_yara', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                    <input type="text" id="flt-file-inf-avtype" placeholder="Inf.AV" title="Inferred AVType" value="${escapeAttr(p.get('inferred_avtype') || '')}" onfocus="attachAutocomplete(this, 'file', 'inferred_avtype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                </div>
                                <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 2px;">
                                    <input type="text" id="flt-file-inf-type" placeholder="Inf.Type" title="Inferred Type" value="${escapeAttr(p.get('inferred_filetype') || '')}" onfocus="attachAutocomplete(this, 'file', 'inferred_filetype', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                    <input type="text" id="flt-file-inf-ccip" placeholder="Inf.IP" title="Inferred CC IP" value="${escapeAttr(p.get('inferred_ccip') || '')}" onfocus="attachAutocomplete(this, 'file', 'inferred_ccip', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.55rem; width: 100%; box-sizing: border-box; background: rgba(0,255,0,0.03);">
                                </div>
                            </div>
                        </th>
                        <th><input type="text" id="flt-file-batch" placeholder="Batch UUID..." value="${escapeAttr(p.get('batch_uuid') || '')}" onfocus="attachAutocomplete(this, 'file', 'batch_uuid', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
                        <th>
                            <select id="flt-file-status" onchange="applyAdvancedFileSearch()" style="background:var(--window-tray); border:1px solid var(--border); color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">
                                ${['', 'pending', 'analyzing', 'failed', 'analyzed'].map(s => `<option value="${escapeAttr(s)}" ${(p.get('status') || '') === s ? 'selected' : ''}>${s ? s.toUpperCase() : 'All Statuses'}</option>`).join('')}
                            </select>
                        </th>
                        <th>
                            <div style="display:flex; align-items:center; gap:2px;">
                                <input type="number" id="flt-file-min-funcs" placeholder="Min..." value="${escapeAttr(p.get('min_function_count') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                <span class="dim" style="font-size:0.6rem">-</span>
                                <input type="number" id="flt-file-max-funcs" placeholder="Max..." value="${escapeAttr(p.get('max_function_count') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th><input type="text" id="flt-file-note-owner" placeholder="Note Owner..." value="${escapeAttr(p.get('note_owner') || '')}" onfocus="attachAutocomplete(this, 'file', 'note_owners', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="width:100%; font-size:0.6rem; box-sizing: border-box;"></th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:2px;">
                                <input type="text" id="flt-file-cluster" placeholder="UUID..." value="${escapeAttr(p.get('bin_cluster_uuid') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                                <input type="text" id="flt-file-cluster-name" placeholder="Cluster Name..." value="${escapeAttr(p.get('bin_cluster_name') || '')}" onfocus="attachAutocomplete(this, 'file', 'bin_cluster_name', (val) => { this.value = val; applyAdvancedFileSearch(); })" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                                <div style="display:flex; align-items:center; gap:2px;">
                                    <input type="number" id="flt-file-min-cohesion" placeholder="Min coh..." value="${escapeAttr(p.get('min_cohesion') || '0.5')}" step="0.05" min="0" max="1" title="Min Cluster Cohesion" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                    <span class="dim" style="font-size:0.6rem">-</span>
                                    <input type="number" id="flt-file-max-cohesion" placeholder="Max coh..." value="${escapeAttr(p.get('max_cohesion') || '')}" step="0.05" min="0" max="1" title="Max Cluster Cohesion" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 45%; box-sizing: border-box;">
                                </div>
                            </div>
                        </th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:2px;">
                                <input type="text" id="flt-file-min-date" placeholder="Min Date..." value="${escapeAttr(p.get('min_entry_date') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                                <input type="text" id="flt-file-max-date" placeholder="Max Date..." value="${escapeAttr(p.get('max_entry_date') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;">
                            </div>
                        </th>
                        <th style="position:relative"><div class="tag-filter-container" id="tag-container-file"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })"></div></th>`;
                } else if (path === 'function-similarity' || path === 'functions') {
                    if (path === 'function-similarity') {
                        headHtml += `
                            <th style="vertical-align: middle;">
                                <div style="display:flex; align-items:center; gap:2px;">
                                    <input type="number" id="sim-min-score" value="${escapeAttr(p.get('min_score') || defaultMinScore())}" step="0.05" min="0" max="1" title="Min Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                                    <span class="dim" style="font-size:0.6rem">-</span>
                                    <input type="number" id="sim-max-score" value="${escapeAttr(p.get('max_score') || '1.0')}" step="0.05" min="0" max="1" title="Max Score" style="width:45%; font-size:0.65rem;" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)">
                                </div>
                                <div class="tag-filter-container" id="tag-container-sim"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'sim')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('sim', 'sim_tag', val); this.value=''; triggerTagSearch(); })"></div>
                            </th>`;
                    }
                    const nameVal = path === 'function-similarity' ? p.get('name') : p.get('function_name');
                    headHtml += `
                        <th>
                            <div style="display:flex; flex-direction:column; gap:4px;">
                                <input type="text" id="flt-func-name" placeholder="Name..." value="${escapeAttr(nameVal || '')}" onfocus="attachAutocomplete(this, 'func', 'function_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;">
                                <div style="display:flex; gap:2px;">
                                    <input type="text" id="flt-func-namespace" placeholder="Namespace..." value="${escapeAttr(p.get('namespace') || '')}" onfocus="attachAutocomplete(this, 'func', 'namespace', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                                    <input type="text" id="flt-func-ret_type" placeholder="Return Type..." value="${escapeAttr(p.get('return_type') || p.get('ret_type') || '')}" onfocus="attachAutocomplete(this, 'func', 'return_type', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.6rem; width: 50%; box-sizing: border-box;">
                                </div>
                            </div>
                        </th>`;
                    const addrVal = path === 'function-similarity' ? p.get('address') : p.get('entrypoint_address');
                    headHtml += `<th><input type="text" id="flt-func-address" placeholder="Addr..." value="${escapeAttr(addrVal || '')}" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
                        <th style="position:relative"><div class="tag-filter-container" id="tag-container-func"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'func')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('func', 'func_tag', val); this.value=''; triggerTagSearch(); })"></div></th>
                        <th>
                            <div style="display:flex; flex-direction:column; gap:2px;">
                                <input type="text" id="flt-func-cluster" placeholder="UUID..." value="${escapeAttr(p.get('cluster_uuid') || '')}" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                                <input type="text" id="flt-func-cluster-name" placeholder="Name..." value="${escapeAttr(p.get('cluster_name') || '')}" onfocus="attachAutocomplete(this, 'func', 'cluster_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                                <input type="number" id="flt-func-min-cohesion" placeholder="Min cohesion..." value="${escapeAttr(p.get('min_cohesion') || '0.5')}" step="0.05" min="0" max="1" title="Min Cluster Cohesion" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box; font-size:0.6rem;">
                            </div>
                        </th>
                        <th><input type="number" id="flt-func-min-features" value="${escapeAttr(p.get('min_features') || '0')}" min="0" title="Min Features" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                        <th><input type="text" id="flt-func-note-owner" placeholder="Note Owner..." value="${escapeAttr(p.get('note_owner') || '')}" onfocus="attachAutocomplete(this, 'func', 'note_owners', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width:100%; font-size:0.6rem; box-sizing: border-box;"></th>
                        <th><input type="text" id="flt-func-file_name" placeholder="Name..." value="${escapeAttr(p.get('file_name') || '')}" onfocus="attachAutocomplete(this, 'func', 'file_name', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box;"></th>
                        <th><input type="text" id="flt-func-md5" placeholder="MD5..." value="${escapeAttr(p.get('file_md5') || p.get('md5') || '')}" onfocus="attachAutocomplete(this, 'func', 'file_md5', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="width: 100%; box-sizing: border-box;"></th>
                        <th style="position:relative"><div class="tag-filter-container" id="tag-container-file"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'file')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('file', 'file_tag', val); this.value=''; triggerTagSearch(); })"></div></th>
                        <th><input type="text" id="flt-func-language" placeholder="Lang..." value="${escapeAttr(p.get('language_id') || p.get('language') || '')}" onfocus="attachAutocomplete(this, 'func', 'language_id', (val) => { this.value = val; ${applyFn}(); })" onchange="debouncedSearch(${applyFn})" onkeydown="handleFilterKey(event, ${applyFn})" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>`;
                    if (path === 'function-similarity') {
                        // Algorithm/Cross Binary/Match Mode moved to the hero pill cards
                        // above the table (simFilterPillsHtml) -- this column, previously
                        // mislabeled "Date", is now genuinely empty.
                        headHtml += `<th></th>`;
                    } else { headHtml += `<th></th><th></th>`; }
                }
                headHtml += `</tr>`;
                thead.innerHTML = headHtml;

                if (path === 'files') {
                    loadFieldCardinalities(col, 'file', { 'file_name': 'flt-file-name', 'file_md5': 'flt-file-md5', 'language_id': 'flt-file-language', 'batch_uuid': 'flt-file-batch', 'bin_cluster_name': 'flt-file-cluster-name' });
                } else {
                    loadFieldCardinalities(col, 'func', { 'function_name': 'flt-func-name', 'file_name': 'flt-func-file_name', 'file_md5': 'flt-func-md5', 'return_type': 'flt-func-ret_type', 'language_id': 'flt-func-language', 'namespace': 'flt-func-namespace' });
                }
                if (path === 'features-global') {
                    loadFieldCardinalities(col, 'feature', { 'hash': 'flt-feat-hash', 'type': 'flt-feat-type', 'op': 'flt-feat-op' });
                }
            } else if (path === 'clusters') {
                if (dataTable) dataTable.style.tableLayout = 'fixed';
                if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';
                headHtml += `<tr class="filter-row">
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="text" id="flt-cluster-uuid" placeholder="UUID..." value="${escapeAttr(p.get('cluster_uuid') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"><input type="text" id="flt-cluster-id" placeholder="ID..." value="${escapeAttr(p.get('cluster_id') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"></div></th>
                    <th><input type="text" id="flt-cluster-name" placeholder="Name..." value="${escapeAttr(p.get('cluster_name') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"></th>
                    <th><input type="number" id="flt-cluster-min-count" value="${escapeAttr(p.get('min_count') || '0')}" min="0" title="Min Functions" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th><input type="number" id="flt-cluster-min-stability" value="${escapeAttr(p.get('min_stability') || '0')}" step="0.1" min="0" max="1" title="Min Stability" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th><input type="number" id="flt-cluster-min-features" value="${escapeAttr(p.get('min_features') || '0')}" min="0" title="Min Features" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th><input type="number" id="flt-cluster-min-cohesion" value="${escapeAttr(p.get('min_cohesion') || '0')}" step="0.1" min="0" max="1" title="Min Cohesion" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th></th>
                    <th style="position:relative"><div class="tag-filter-container" id="tag-container-cluster"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'cluster')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('cluster', 'cluster_tag', val); this.value=''; triggerTagSearch(); })"></div></th>
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="text" id="flt-cluster-func-name" placeholder="Func Name..." value="${escapeAttr(p.get('func_name') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"><input type="text" id="flt-cluster-func-addr" placeholder="Func Addr..." value="${escapeAttr(p.get('func_addr') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"><input type="text" id="flt-cluster-file-name" placeholder="File Name..." value="${escapeAttr(p.get('file_name') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"></div></th>
                </tr>`;
                thead.innerHTML = headHtml;
            } else if (path === 'jobs') {
                if (dataTable) dataTable.style.tableLayout = 'fixed';
                if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';
                const statuses = ['', 'pending', 'running', 'completed', 'failed', 'cancelled'];
                const statusOptions = statuses.map(s => { const label = s ? s.toUpperCase() : 'All Statuses'; return `<option value="${escapeAttr(s)}" ${p.get('status') === s ? 'selected' : ''}>${label}</option>`; }).join('');
                const types = ['', 'pipeline', 'group', 'file_data_ingest', 'ghidra_analyze', 'idx_meta', 'idx_functions', 'idx_features', 'build_sim', 'cluster_functions', 'cluster_binaries', 'enrich_features'];
                const typeOptions = types.map(t => { const label = t ? t.replace(/_/g, ' ').toUpperCase() : 'All Types'; return `<option value="${escapeAttr(t)}" ${p.get('type') === t ? 'selected' : ''}>${label}</option>`; }).join('');
                headHtml += `<tr class="filter-row">
                    <th><select id="job-type-filter" onchange="applyJobSearch()" style="background:var(--window-tray); border:1px solid var(--border); color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">${typeOptions}</select></th>
                    <th></th>
                    <th></th>
                    <th><select id="job-status-filter" onchange="applyJobSearch()" style="background:var(--window-tray); border:1px solid var(--border); color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">${statusOptions}</select></th>
                    <th></th><th></th><th></th><th></th>
                </tr>`;
                thead.innerHTML = headHtml;
            } else if (path === 'binary-similarity') {
                headHtml += `<tr class="filter-row">
                    <th>
                        <div style="display:flex; align-items:center; gap:2px;">
                            <input type="number" id="bsim-min-score" placeholder="Min..." step="0.05" min="0" max="1" value="${escapeAttr(p.get('min_score') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;"><span class="dim" style="font-size:0.6rem">-</span><input type="number" id="bsim-max-score" placeholder="Max..." step="0.05" min="0" max="1" value="${escapeAttr(p.get('max_score') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;">
                        </div>
                    </th>
                    <th>
                        <input type="text" id="bsim-file-name" placeholder="File Name..." value="${escapeAttr(p.get('file_name') || '')}" onfocus="attachAutocomplete(this, 'file', 'file_name', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;">
                    </th>
                    <th><input type="text" id="bsim-md5" placeholder="MD5..." value="${escapeAttr(p.get('md5') || '')}" onfocus="attachAutocomplete(this, 'file', 'file_md5', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.6rem; width:100%; box-sizing:border-box; font-family:monospace;"></th>
                    <th><input type="text" id="bsim-arch" placeholder="Arch..." value="${escapeAttr(p.get('arch') || '')}" onfocus="attachAutocomplete(this, 'file', 'language_id', (val) => { this.value = val; applyBinSimSearch(); })" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.6rem; width:100%; box-sizing:border-box;"></th>
                    <th><div style="display:flex; align-items:center; gap:2px;"><input type="number" id="bsim-min-funcs" placeholder="Min..." min="0" value="${escapeAttr(p.get('min_funcs') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;"><span class="dim" style="font-size:0.6rem">-</span><input type="number" id="bsim-max-funcs" placeholder="Max..." min="0" value="${escapeAttr(p.get('max_funcs') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;"></div></th>
                    <th><div style="display:flex; align-items:center; gap:2px;"><input type="number" id="bsim-min-cov" placeholder="Min..." step="0.1" min="0" max="1" value="${escapeAttr(p.get('min_coverage') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;"><span class="dim" style="font-size:0.6rem">-</span><input type="number" id="bsim-max-cov" placeholder="Max..." step="0.1" min="0" max="1" value="${escapeAttr(p.get('max_coverage') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:48%; box-sizing:border-box;"></div></th>
                    <th><input type="number" id="bsim-min-shared" placeholder="Min..." min="0" value="${escapeAttr(p.get('min_shared') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;"></th>
                    <th style="position:relative"><div class="tag-filter-container" id="tag-container-bin-sim"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'bin-sim')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('bin-sim', 'file_tag', val); this.value=''; triggerTagSearch(); })"></div></th>
                </tr>`;
                thead.innerHTML = headHtml;
            } else if (path === 'bin-clusters') {
                const nameType = p.get('cluster_name_type') || 'file';
                const nodeType = p.get('node_type') || 'file';
                if (dataTable) dataTable.style.tableLayout = 'fixed';
                if (dataTableHeader) dataTableHeader.style.tableLayout = 'fixed';
                headHtml += `<tr class="filter-row">
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="text" id="flt-bin-cluster-uuid" placeholder="UUID..." value="${escapeAttr(p.get('cluster_uuid') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"><input type="text" id="flt-bin-cluster-id" placeholder="ID..." value="${escapeAttr(p.get('cluster_id') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"></div></th>
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="text" id="flt-bin-cluster-name" placeholder="Name..." value="${escapeAttr(p.get('cluster_name') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.65rem; width: 100%; box-sizing: border-box;"><select id="bin-cluster-name-type" style="background:var(--border); color:var(--accent); border:1px solid var(--accent); font-size:0.6rem; border-radius:4px; padding:2px; width:100%; box-sizing:border-box;" onchange="changeBinClusterNameType(this.value)"><option value="file" ${nameType === 'file' ? 'selected' : ''}>Most Common File Name</option><option value="yara" ${nameType === 'yara' ? 'selected' : ''}>Most Common Yara</option></select></div></th>
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="number" id="flt-bin-cluster-min-count" value="${escapeAttr(p.get('min_count') || '0')}" min="0" placeholder="Min" title="Min Binaries" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"><input type="number" id="flt-bin-cluster-max-count" value="${escapeAttr(p.get('max_count') || '')}" min="0" placeholder="Max" title="Max Binaries" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></div></th>
                    <th><input type="number" id="flt-bin-cluster-min-stability" value="${escapeAttr(p.get('min_stability') || '0')}" step="0.1" min="0" title="Min Stability" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></th>
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="number" id="flt-bin-cluster-min-cohesion" value="${escapeAttr(p.get('min_cohesion') || '0')}" step="0.1" min="0" max="1" placeholder="Min" title="Min Cohesion" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"><input type="number" id="flt-bin-cluster-max-cohesion" value="${escapeAttr(p.get('max_cohesion') || '')}" step="0.1" min="0" max="1" placeholder="Max" title="Max Cohesion" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="width:100%; font-size:0.65rem; box-sizing: border-box;"></div></th>
                    <th></th>
                    <th style="position:relative"><div class="tag-filter-container" id="tag-container-bin-cluster"><input type="text" class="tag-filter-add" placeholder="+ Tag" onkeydown="handleTagAdd(event, 'bin-cluster')" onfocus="attachTagAutocomplete(this, (val) => { createTagCard('bin-cluster', 'cluster_tag', val); this.value=''; triggerTagSearch(); })"></div></th>
                    <th><div style="display:flex; flex-direction:column; gap:2px;"><input type="text" id="flt-bin-cluster-file-name" placeholder="File Name..." value="${escapeAttr(p.get('file_name') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"><input type="text" id="flt-bin-cluster-file-md5" placeholder="MD5..." value="${escapeAttr(p.get('file_md5') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)" style="font-size:0.6rem; width: 100%; box-sizing: border-box;"></div></th>
                </tr>`;
                thead.innerHTML = headHtml;
            } else if (path === 'collections') {
                const msToDate = (ms) => ms ? new Date(+ms).toISOString().slice(0, 10) : '';
                const numRange = (minId, maxId, minP, maxP) => `<th><div style="display:flex; align-items:center; gap:2px;"><input type="number" id="${minId}" placeholder="Min..." min="0" value="${escapeAttr(p.get(minP) || '')}" onchange="debouncedSearch(applyCollectionSearch)" onkeydown="handleFilterKey(event, applyCollectionSearch)" style="font-size:0.6rem; width:45%; box-sizing:border-box;"><span class="dim" style="font-size:0.6rem">-</span><input type="number" id="${maxId}" placeholder="Max..." min="0" value="${escapeAttr(p.get(maxP) || '')}" onchange="debouncedSearch(applyCollectionSearch)" onkeydown="handleFilterKey(event, applyCollectionSearch)" style="font-size:0.6rem; width:45%; box-sizing:border-box;"></div></th>`;
                headHtml += `<tr class="filter-row">
                    <th><input type="text" id="flt-coll-name" placeholder="Name..." value="${escapeAttr(p.get('name') || '')}" onchange="debouncedSearch(applyCollectionSearch)" onkeydown="handleFilterKey(event, applyCollectionSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;"></th>
                    ${numRange('flt-coll-min-batches', 'flt-coll-max-batches', 'min_batches', 'max_batches')}
                    ${numRange('flt-coll-min-files', 'flt-coll-max-files', 'min_files', 'max_files')}
                    ${numRange('flt-coll-min-functions', 'flt-coll-max-functions', 'min_functions', 'max_functions')}
                    <th><div style="display:flex; align-items:center; gap:2px;"><input type="date" id="flt-coll-min-date" title="From" value="${escapeAttr(msToDate(p.get('min_last_updated')))}" onchange="debouncedSearch(applyCollectionSearch)" style="font-size:0.6rem; width:48%; box-sizing:border-box;"><input type="date" id="flt-coll-max-date" title="To" value="${escapeAttr(msToDate(p.get('max_last_updated')))}" onchange="debouncedSearch(applyCollectionSearch)" style="font-size:0.6rem; width:48%; box-sizing:border-box;"></div></th>
                    <th></th>
                    <th></th>
                </tr>`;
                thead.innerHTML = headHtml;
            } else if (path === 'pools') {
                const msToDate = (ms) => ms ? new Date(+ms).toISOString().slice(0, 10) : '';
                const status = p.get('sync_status') || '';
                const statusOpts = ['', 'current', 'outdated', 'created'].map(s => `<option value="${escapeAttr(s)}" ${status === s ? 'selected' : ''}>${s ? s.toUpperCase() : 'All'}</option>`).join('');
                headHtml += `<tr class="filter-row">
                    <th><input type="text" id="flt-pool-id" placeholder="ID..." value="${escapeAttr(p.get('id') || '')}" onchange="debouncedSearch(applyPoolSearch)" onkeydown="handleFilterKey(event, applyPoolSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;"></th>
                    <th><input type="text" id="flt-pool-name" placeholder="Name..." value="${escapeAttr(p.get('name') || '')}" onchange="debouncedSearch(applyPoolSearch)" onkeydown="handleFilterKey(event, applyPoolSearch)" style="font-size:0.65rem; width:100%; box-sizing:border-box;"></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th><select id="flt-pool-status" onchange="applyPoolSearch()" style="background:var(--window-tray); border:1px solid var(--border); color:var(--text); padding:2px; font-size:0.65rem; border-radius:2px; width:100%; box-sizing:border-box;">${statusOpts}</select></th>
                    <th><div style="display:flex; align-items:center; gap:2px;"><input type="date" id="flt-pool-min-date" title="From" value="${escapeAttr(msToDate(p.get('min_created_at')))}" onchange="debouncedSearch(applyPoolSearch)" style="font-size:0.6rem; width:48%; box-sizing:border-box;"><input type="date" id="flt-pool-max-date" title="To" value="${escapeAttr(msToDate(p.get('max_created_at')))}" onchange="debouncedSearch(applyPoolSearch)" style="font-size:0.6rem; width:48%; box-sizing:border-box;"></div></th>
                    <th></th>
                </tr>`;
                thead.innerHTML = headHtml;
            }

            // Graph/Hierarchy Container Visibility Logic
            const viewMode = params.get('view') || 'table';

            if ((path === 'function-similarity' || path === 'binary-similarity') && viewMode === 'graph') {
                if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = 'none'; }
                if (tableBodyWrap) tableBodyWrap.style.display = 'none';
                if (pag) pag.style.display = 'none';
                if (path === 'function-similarity') { if (gview) gview.style.display = 'flex'; loadGraphView(params); }
                else { if (chordView) chordView.style.display = 'flex'; loadChordView(params); }
            } else if ((path === 'clusters' || path === 'bin-clusters') && (viewMode === 'hierarchy' || viewMode === 'packing')) {
                if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = 'none'; }
                if (tableBodyWrap) tableBodyWrap.style.display = 'none';
                if (pag) pag.style.display = 'none';
                if (viewMode === 'hierarchy') { if (hview) hview.style.display = 'flex'; if (pview) pview.style.display = 'none'; if (path === 'clusters') loadHierarchyView(params); else loadBinHierarchyView(params); }
                else { if (hview) hview.style.display = 'none'; if (pview) pview.style.display = 'flex'; if (path === 'clusters') loadPackingView(params); else loadBinPackingView(params); }
            } else {
                if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = '1'; }
                if (tableBodyWrap) tableBodyWrap.style.display = '';
                if (pag) pag.style.display = 'flex';
                if (gview) gview.style.display = 'none';
                if (chordView) chordView.style.display = 'none';
                if (hview) hview.style.display = 'none';
                if (pview) pview.style.display = 'none';
            }

            // Search Bar building
            if (searchArea) {
                if (path === 'files') {
                    searchArea.innerHTML = `<div class="filter-bar"><div class="search-input-wrapper"><input type="text" id="file-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyAdvancedFileSearch)" onkeydown="handleFilterKey(event, applyAdvancedFileSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFileSearch()" title="Search"></i></div></div>`;
                } else if (path === 'functions') {
                    const fileMd5 = p.get('file_md5');
                    const callGraphBtn = fileMd5 ? `<a class="btn-action" onclick="Nav.openPath('/collections/${encodeURIComponent(p.get('collection'))}/files/${encodeURIComponent(fileMd5)}/functions', event, { title: 'Call Graph: ${fileMd5}', type: 'call_graph' })" style="color:var(--accent); margin-left:10px; padding: 6px 12px; border:1px solid var(--accent); border-radius:4px; font-size:0.8rem; cursor:pointer;">View File Call Graph 🕸️</a>` : '';
                    searchArea.innerHTML = `<div class="filter-bar" style="gap:20px"><div style="display:flex; gap:10px; align-items:center;"><div class="search-input-wrapper"><input type="text" id="func-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyAdvancedFuncSearch)" onkeydown="handleFilterKey(event, applyAdvancedFuncSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFuncSearch()" title="Search"></i></div>${callGraphBtn}</div></div>`;
                } else if (path === 'features-global') {
                    searchArea.innerHTML = `<div class="filter-bar" style="gap:20px"><div style="display:flex; gap:10px; align-items:center;"><div class="search-input-wrapper"><input type="text" id="feature-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyAdvancedFeatureSearch)" onkeydown="handleFilterKey(event, applyAdvancedFeatureSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyAdvancedFeatureSearch()" title="Search"></i></div></div></div>`;
                } else if (path === 'function-similarity') {
                    searchArea.innerHTML = `<div class="filter-bar" style="gap:20px"><div style="display:flex; gap:10px; align-items:center;"><div class="search-input-wrapper"><input type="text" id="sim-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applySimSearch)" onkeydown="handleFilterKey(event, applySimSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applySimSearch()" title="Search"></i></div></div></div>`;
                } else if (path === 'clusters') {
                    searchArea.innerHTML = `<div class="filter-bar"><div class="search-input-wrapper"><input type="text" id="cluster-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyClusterSearch)" onkeydown="handleFilterKey(event, applyClusterSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyClusterSearch()" title="Search"></i></div></div>`;
                } else if (path === 'bin-clusters') {
                    searchArea.innerHTML = `<div class="filter-bar"><div class="search-input-wrapper"><input type="text" id="bin-cluster-search-input" placeholder="Search by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyBinClusterSearch)" onkeydown="handleFilterKey(event, applyBinClusterSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyBinClusterSearch()" title="Search"></i></div></div>`;
                } else if (path === 'binary-similarity') {
                    searchArea.innerHTML = `<div class="filter-bar" style="gap:20px"><div style="display:flex; gap:10px; align-items:center;"><div class="search-input-wrapper"><input type="text" id="bsim-search-input" placeholder="Search similarities by keywords..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyBinSimSearch)" onkeydown="handleFilterKey(event, applyBinSimSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyBinSimSearch()" title="Search"></i></div></div></div>`;
                } else if (path === 'collections') {
                    searchArea.innerHTML = `<div class="filter-bar"><div class="search-input-wrapper"><input type="text" id="collection-search-input" placeholder="Search collections by name..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyCollectionSearch)" onkeydown="handleFilterKey(event, applyCollectionSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyCollectionSearch()" title="Search"></i></div></div>`;
                } else if (path === 'pools') {
                    searchArea.innerHTML = `<div class="filter-bar"><div class="search-input-wrapper"><input type="text" id="pool-search-input" placeholder="Search pools by name, id, collection..." autofocus value="${escapeAttr(p.get('q') || '')}" onchange="debouncedSearch(applyPoolSearch)" onkeydown="handleFilterKey(event, applyPoolSearch)"><i class="fa-solid fa-magnifying-glass search-icon-btn" onclick="applyPoolSearch()" title="Search"></i></div></div>`;
                } else { searchArea.innerHTML = ''; }
            }
        } else {
            if (settingsEl) settingsEl.style.display = 'none';
        }
    } else {
        // Surgical UI State Syncing (when path haven't changed)
        const p = new URLSearchParams(params);
        const syncInput = (id, paramName) => { const el = document.getElementById(id); if (el) el.value = p.get(paramName) || ''; };
        const syncSelect = (id, paramName, defaultVal = '') => { const el = document.getElementById(id); if (el) el.value = p.get(paramName) || defaultVal; };
 
        // Sync main search bars
        syncInput('file-search-input', 'q');
        syncInput('func-search-input', 'q');
        syncInput('feature-search-input', 'q');
        syncInput('sim-search-input', 'q');
        syncInput('cluster-search-input', 'q');
        syncInput('bin-cluster-search-input', 'q');
        syncInput('bsim-search-input', 'q');
        syncInput('collection-search-input', 'q');
        syncInput('pool-search-input', 'q');

        // Sync view settings
        syncInput('sim-pool-limit', 'pool_limit');
        syncInput('sim-limit', 'limit');
 
        // Sync filter inputs
        if (path === 'files') {
            syncInput('flt-file-name', 'file_name'); syncInput('flt-file-md5', 'file_md5'); syncInput('flt-file-language', 'language_id'); syncInput('flt-file-yara', 'yara'); syncInput('flt-file-avtype', 'avtype'); syncInput('flt-file-ccip', 'cc_ip');
            syncInput('flt-file-inf-yara', 'inferred_yara'); syncInput('flt-file-inf-avtype', 'inferred_avtype'); syncInput('flt-file-inf-type', 'inferred_filetype'); syncInput('flt-file-inf-ccip', 'inferred_ccip');
            syncInput('flt-file-batch', 'batch_uuid'); syncInput('flt-file-min-funcs', 'min_function_count'); syncInput('flt-file-max-funcs', 'max_function_count');
            syncInput('flt-file-note-owner', 'note_owner'); syncInput('flt-file-cluster', 'bin_cluster_uuid'); syncInput('flt-file-cluster-name', 'bin_cluster_name'); syncInput('flt-file-min-cohesion', 'min_cohesion'); syncInput('flt-file-max-cohesion', 'max_cohesion');
            syncInput('flt-file-min-date', 'min_entry_date'); syncInput('flt-file-max-date', 'max_entry_date');
        } else if (path === 'functions' || path === 'function-similarity') {
            const prefix = path === 'function-similarity' ? 'sim-' : 'flt-func-';
            const nameParam = path === 'function-similarity' ? 'name' : 'function_name';
            const addrParam = path === 'function-similarity' ? 'address' : 'entrypoint_address';
            if (path === 'function-similarity') { 
                syncInput('sim-min-score', 'min_score');
                syncInput('sim-max-score', 'max_score');
                syncSimFilterPills(p);
            }
            syncInput('flt-func-name', nameParam); syncInput('flt-func-namespace', 'namespace'); syncInput('flt-func-ret_type', 'return_type'); syncInput('flt-func-address', addrParam);
            syncInput('flt-func-cluster', 'cluster_uuid'); syncInput('flt-func-cluster-name', 'cluster_name'); syncInput('flt-func-min-cohesion', 'min_cohesion');
            syncInput('flt-func-min-features', 'min_features'); syncInput('flt-func-note-owner', 'note_owner'); syncInput('flt-func-file_name', 'file_name');
            const md5Val = p.get('md5') || p.get('file_md5');
            const md5El = document.getElementById('flt-func-md5'); if (md5El) md5El.value = md5Val || '';
        } else if (path === 'features-global') {
            syncInput('flt-feat-hash', 'hash'); syncInput('flt-feat-type', 'type'); syncInput('flt-feat-op', 'op');
            syncInput('flt-feat-min-tf', 'min_tf_score'); syncInput('flt-feat-max-tf', 'max_tf_score'); syncInput('flt-feat-min-freq', 'min_frequency'); syncInput('flt-feat-max-freq', 'max_frequency');
        } else if (path === 'clusters') {
            syncInput('flt-cluster-uuid', 'cluster_uuid'); syncInput('flt-cluster-id', 'cluster_id'); syncInput('flt-cluster-name', 'cluster_name'); syncInput('flt-cluster-min-count', 'min_count'); syncInput('flt-cluster-min-stability', 'min_stability'); syncInput('flt-cluster-min-features', 'min_features'); syncInput('flt-cluster-min-cohesion', 'min_cohesion');
            syncInput('flt-cluster-func-name', 'func_name'); syncInput('flt-cluster-func-addr', 'func_addr'); syncInput('flt-cluster-file-name', 'file_name');
        } else if (path === 'bin-clusters') {
            syncInput('flt-bin-cluster-uuid', 'cluster_uuid'); syncInput('flt-bin-cluster-id', 'cluster_id'); syncInput('flt-bin-cluster-name', 'cluster_name'); syncSelect('bin-cluster-name-type', 'cluster_name_type', 'file');
            syncInput('flt-bin-cluster-min-count', 'min_count'); syncInput('flt-bin-cluster-max-count', 'max_count'); syncInput('flt-bin-cluster-min-stability', 'min_stability'); syncInput('flt-bin-cluster-min-cohesion', 'min_cohesion'); syncInput('flt-bin-cluster-max-cohesion', 'max_cohesion');
            syncInput('flt-bin-cluster-file-name', 'file_name'); syncInput('flt-bin-cluster-file-md5', 'file_md5');
        } else if (path === 'collections') {
            const syncDate = (id, paramName) => { const el = document.getElementById(id); if (el) { const ms = p.get(paramName); el.value = ms ? new Date(+ms).toISOString().slice(0, 10) : ''; } };
            syncInput('flt-coll-name', 'name');
            syncInput('flt-coll-min-batches', 'min_batches'); syncInput('flt-coll-max-batches', 'max_batches');
            syncInput('flt-coll-min-files', 'min_files'); syncInput('flt-coll-max-files', 'max_files');
            syncInput('flt-coll-min-functions', 'min_functions'); syncInput('flt-coll-max-functions', 'max_functions');
            syncDate('flt-coll-min-date', 'min_last_updated'); syncDate('flt-coll-max-date', 'max_last_updated');
        } else if (path === 'pools') {
            const syncDate = (id, paramName) => { const el = document.getElementById(id); if (el) { const ms = p.get(paramName); el.value = ms ? new Date(+ms).toISOString().slice(0, 10) : ''; } };
            syncInput('flt-pool-id', 'id'); syncInput('flt-pool-name', 'name'); syncSelect('flt-pool-status', 'sync_status', '');
            syncDate('flt-pool-min-date', 'min_created_at'); syncDate('flt-pool-max-date', 'max_created_at');
        } else if (path === 'binary-similarity') {
            syncBinSimTags(p); syncInput('bsim-min-score', 'min_score'); syncInput('bsim-max-score', 'max_score'); syncInput('bsim-file-name', 'file_name'); syncInput('bsim-md5', 'md5'); syncInput('bsim-arch', 'arch');
            syncInput('bsim-min-funcs', 'min_funcs'); syncInput('bsim-max-funcs', 'max_funcs'); syncInput('bsim-min-cov', 'min_coverage'); syncInput('bsim-max-cov', 'max_coverage'); syncInput('bsim-min-shared', 'min_shared');
        } else if (path === 'jobs') {
            syncSelect('job-type-filter', 'type'); syncSelect('job-collection-filter', 'collection'); syncSelect('job-status-filter', 'status');
        }

        // Sync view mode buttons active state
        const viewMode = params.get('view') || 'table';
        document.querySelectorAll('.view-btn').forEach(btn => {
            const onclickStr = btn.getAttribute('onclick') || '';
            const btnModeMatch = onclickStr.match(/switch.*View\('([^']+)'\)/);
            if (btnModeMatch) {
                const btnMode = btnModeMatch[1];
                btn.classList.toggle('active', btnMode === viewMode);
            }
        });

        // Trigger graph/hierarchy reload if needed when on same path but mode changed
        if ((path === 'function-similarity' || path === 'binary-similarity') && viewMode === 'graph') {
            if (tableBodyWrap && tableBodyWrap.style.display !== 'none') {
                if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = 'none'; }
                tableBodyWrap.style.display = 'none';
                if (pag) pag.style.display = 'none';
                if (path === 'function-similarity') { if (gview) gview.style.display = 'flex'; loadGraphView(params); }
                else { if (chordView) chordView.style.display = 'flex'; loadChordView(params); }
            }
        } else if ((path === 'clusters' || path === 'bin-clusters') && (viewMode === 'hierarchy' || viewMode === 'packing')) {
            if (tableBodyWrap && tableBodyWrap.style.display !== 'none') {
                if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = 'none'; }
                tableBodyWrap.style.display = 'none';
                if (pag) pag.style.display = 'none';
                if (viewMode === 'hierarchy') { if (hview) hview.style.display = 'flex'; if (pview) pview.style.display = 'none'; if (path === 'clusters') loadHierarchyView(params); else loadBinHierarchyView(params); }
                else { if (hview) hview.style.display = 'none'; if (pview) pview.style.display = 'flex'; if (path === 'clusters') loadPackingView(params); else loadBinPackingView(params); }
            }
        } else if (path !== 'upload' && tableBodyWrap && tableBodyWrap.style.display === 'none') {
            if (tableWrap) { tableWrap.style.display = 'flex'; tableWrap.style.flex = '1'; }
            tableBodyWrap.style.display = '';
            if (pag) pag.style.display = 'flex';
            if (gview) gview.style.display = 'none';
            if (chordView) chordView.style.display = 'none';
            if (hview) hview.style.display = 'none';
            if (pview) pview.style.display = 'none';
        }
    }

    // Re-inject tags for all views that support them
    setTimeout(() => {
        // Always clear existing cards first to avoid duplicates when navigating same-view
        ["sim", "func", "file", "bin-sim", "cluster", "bin-cluster"].forEach(key => {
            const container = document.getElementById(`tag-container-${key}`);
            if (container) container.querySelectorAll('.tag-filter-card').forEach(c => c.remove());
        });
        const tagSearchParams = new URLSearchParams(params);
        const tagFields = [
            { key: 'sim', fields: ['sim_tag', 'sim_static_tag', 'sim_user_tag', 'exclude_sim_tag', 'exclude_sim_static_tag', 'exclude_sim_user_tag'] },
            { key: 'func', fields: ['func_tag', 'func_static_tag', 'func_user_tag', 'exclude_func_tag', 'exclude_func_static_tag', 'exclude_func_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] },
            { key: 'file', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag'] },
            { key: 'bin-sim', fields: ['file_tag', 'file_static_tag', 'file_user_tag', 'exclude_file_tag', 'exclude_file_static_tag', 'exclude_file_user_tag', 'tag', 'static_tag', 'user_tag', 'exclude_tag', 'exclude_static_tag', 'exclude_user_tag'] },
            { key: "cluster", fields: ["cluster_tag", "exclude_cluster_tag"] },
            { key: "bin-cluster", fields: ["cluster_tag", "exclude_cluster_tag"] }
        ];
        tagFields.forEach(col => {
            col.fields.forEach(f => {
                const values = tagSearchParams.getAll(f);
                const isEx = f.startsWith('exclude_');
                const baseType = isEx ? f.substring(8) : f;
                values.forEach(v => {
                    if (!v) return;
                    const parsed = unquoteFilterValue(v);
                    createTagCard(col.key, baseType, parsed.value, isEx, parsed.literal);
                });
            });
        });
    }, 0);

    // Sync body colgroup from the header row's actual rendered widths.
    // We use requestAnimationFrame so the header table has laid out first.
    if (true) {
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
    }

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
    const { viewKey, params } = getRoutingState();

    if (viewKey === 'files') {
        const val = document.getElementById('file-search').value;
        if (val) params.set('file_name', val);
        else params.delete('file_name');
    } else if (viewKey === 'features-global') {
        const val = document.getElementById('feature-search').value;
        if (val) params.set('hash', val);
        else params.delete('hash');

        const sortTf = document.getElementById('sort-tf').checked;
        if (sortTf) params.set('sort', 'tf');
        else params.delete('sort');
    }

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

function toggleSort(key) {
    const { viewKey, params } = getRoutingState();
    // ponytail: bin-sim "Score" column must sort on whichever score type
    // (lib/original/content) is active in the score-type dropdown, not
    // always the overall 'score' field.
    if (viewKey === 'binary-similarity' && key === 'score') {
        key = params.get('sort') || 'score';
    }
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
    if (viewKey === 'function-similarity') {
        simSearchRequested = true;
    }
    navigate(viewKey, params);
}

function applyAdvancedFuncSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

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
    params.set('min_cohesion', minCohesionFlt || '0.5');

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
            // Quote unless the user hand-typed a wildcard (see quoteFilterValue).
            params.append(key, quoteFilterValue(val, card.dataset.literal !== 'false'));
        });
    });

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

function switchSimView(mode) {
    const { viewKey, params } = getRoutingState();
    params.set('view', mode);
    simSearchRequested = true;
    navigate(viewKey, params);
}

function switchBinSimView(mode) {
    const { viewKey, params } = getRoutingState();
    params.set('view', mode);
    navigate(viewKey, params);
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
    const { viewKey, params } = getRoutingState();

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
    params.set('min_score', minScore || defaultMinScore());
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
    params.set('min_cohesion', minCohesionFlt || '0.5');

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
            // Quote unless the user hand-typed a wildcard (see quoteFilterValue).
            params.append(key, quoteFilterValue(val, card.dataset.literal !== 'false'));
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
    navigate(viewKey, params);
}

function applyAdvancedFileSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

    const globalQ = document.getElementById('file-search-input')?.value;
    params.set('q', globalQ || '');

    const nameFlt = document.getElementById('flt-file-name')?.value;
    const md5Flt = document.getElementById('flt-file-md5')?.value;
    const langFlt = document.getElementById('flt-file-language')?.value;
    const batchFlt = document.getElementById('flt-file-batch')?.value;
    const statusFlt = document.getElementById('flt-file-status')?.value;
    const minEntryFlt = document.getElementById('flt-file-min-date')?.value;
    const maxEntryFlt = document.getElementById('flt-file-max-date')?.value;
    const minFuncsFlt = document.getElementById('flt-file-min-funcs')?.value;
    const maxFuncsFlt = document.getElementById('flt-file-max-funcs')?.value;
    const clusterUuidFlt = document.getElementById('flt-file-cluster')?.value;
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
    if (statusFlt) params.set('status', statusFlt); else params.delete('status');
    if (minEntryFlt) params.set('min_entry_date', minEntryFlt); else params.delete('min_entry_date');
    if (maxEntryFlt) params.set('max_entry_date', maxEntryFlt); else params.delete('max_entry_date');
    if (minFuncsFlt) params.set('min_function_count', minFuncsFlt); else params.delete('min_function_count');
    if (maxFuncsFlt) params.set('max_function_count', maxFuncsFlt); else params.delete('max_function_count');
    if (clusterUuidFlt) params.set('bin_cluster_uuid', clusterUuidFlt); else params.delete('bin_cluster_uuid');
    if (clusterNameFlt) params.set('bin_cluster_name', clusterNameFlt); else params.delete('bin_cluster_name');
    params.set('min_cohesion', minCohesionFlt || '0.5');
    if (maxCohesionFlt) params.set('max_cohesion', maxCohesionFlt); else params.delete('max_cohesion');
    if (yaraFlt) params.set('yara', yaraFlt); else params.delete('yara');
    if (avtypeFlt) params.set('avtype', avtypeFlt); else params.delete('avtype');
    if (ccipFlt) params.set('cc_ip', ccipFlt); else params.delete('cc_ip');
    if (infYaraFlt) params.set('inferred_yara', infYaraFlt); else params.delete('inferred_yara');
    if (infAvtypeFlt) params.set('inferred_avtype', infAvtypeFlt); else params.delete('inferred_avtype');
    if (infTypeFlt) params.set('inferred_filetype', infTypeFlt); else params.delete('inferred_filetype');
    if (infCcipFlt) params.set('inferred_ccip', infCcipFlt); else params.delete('inferred_ccip');

    const noteOwnerFlt = document.getElementById('flt-file-note-owner')?.value;
    if (noteOwnerFlt) params.set('note_owner', noteOwnerFlt); else params.delete('note_owner');

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
            // Quote unless the user hand-typed a wildcard (see quoteFilterValue).
            params.append(key, quoteFilterValue(val, card.dataset.literal !== 'false'));
        });
    }

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

function applyJobSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

    const collectionVal = document.getElementById('job-collection-filter')?.value;
    const statusVal = document.getElementById('job-status-filter')?.value;
    const typeVal = document.getElementById('job-type-filter')?.value;

    if (collectionVal) params.set('collection', collectionVal); else params.delete('collection');
    if (statusVal) params.set('status', statusVal); else params.delete('status');
    if (typeVal) params.set('type', typeVal); else params.delete('type');

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}
window.applyJobSearch = applyJobSearch;


function triggerTagSearch() {
    // The bin-sim detail table filters its own rows in place; it must not fall
    // through to the binary-similarity LIST search, which navigates.
    if (document.getElementById('tag-container-bsim-sim')) {
        binSimFilterChange(true);
        return;
    }
    const { viewKey } = getRoutingState();
    if (viewKey === 'function-similarity') debouncedSearch(applySimSearch);
    else if (viewKey === 'binary-similarity') debouncedSearch(applyBinSimSearch);
    else if (viewKey === 'functions') debouncedSearch(applyAdvancedFuncSearch);
    else if (viewKey === 'files') debouncedSearch(applyAdvancedFileSearch);
    else if (viewKey === 'features-global') debouncedSearch(applyAdvancedFeatureSearch);
    else if (viewKey === 'clusters') debouncedSearch(applyClusterSearch);
    else if (viewKey === "bin-clusters") debouncedSearch(applyBinClusterSearch);
}

function applyAdvancedFeatureSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

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
    navigate(viewKey, params);
}

function createTagCard(columnId, type, value, isExclude = false, literal = true) {
    const container = document.getElementById(`tag-container-${columnId}`);
    if (!container) return;

    const existing = Array.from(container.querySelectorAll('.tag-filter-card')).find(c => c.dataset.value === value && c.dataset.type === type);
    if (existing) return;

    const card = document.createElement('div');
    card.className = `tag-filter-card ${isExclude ? 'exclude' : ''}`;
    card.dataset.value = value;
    card.dataset.type = type;
    card.dataset.exclude = isExclude;
    card.dataset.literal = literal;

    card.innerHTML = `
        <span class="btn-card-ex" title="Toggle Exclude" onclick="toggleCardExclude(this)">NOT</span>
        <span class="tag-text" title="${escapeAttr(value)}">${value}</span>
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
            let type = (columnId === "sim" ? "sim_tag" : ((columnId === "cluster" || columnId === "bin-cluster") ? "cluster_tag" : (columnId === "func" ? "func_tag" : "file_tag")));
            createTagCard(columnId, type, val, false, false);
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
    let tableSel = null;
    if (window.tableSelections) {
        tableSel = window.tableSelections.find(ts => ts.selectedCells && ts.selectedCells.size > 0);
    }
    if (tableSel) {
        tableSel.copySelection();
        if (btn) {
            const originalHtml = btn.innerHTML;
            btn.innerHTML = '<span style="color:var(--success)">✓</span>';
            setTimeout(() => { btn.innerHTML = originalHtml; }, 1500);
        }
        return;
    }
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

/** Refreshes a single file row's note badge after a note add/remove from the file detail view. */
function refreshFileRow(fileId) {
    const { params, collection: currentCollection } = getRoutingState();
    const collection = params.get('collection') || currentCollection || fileId.split(':')[0];
    const md5 = fileId.split(':')[2];
    if (!md5) return;
    fetch(`/api/file/search?collection=${encodeURIComponent(collection)}&file_md5=${encodeURIComponent(md5)}&limit=1`)
        .then(r => r.json())
        .then(data => {
            const f = (data.files || data.items || [])[0];
            if (!f) return;
            const row = document.querySelector(`tr[data-id="${fileId}"]`);
            if (!row) return;
            const noteCell = row.querySelector('.file-note-cell');
            if (noteCell) noteCell.innerHTML = EntityRenderer.renderFileNoteButton(fileId, f.note_owners, { isTable: true, raw_data: f });
        })
        .catch(() => { });
}
window.refreshFileRow = refreshFileRow;

function renderTopCorrelations(items, clustersMap = {}, anchorMd5 = null, anchorAddress = null) {
    if (!items || !items.length) return '<tr><td colspan="11" style="text-align:center; padding:40px;">No similarity pairs found in this collection.</td></tr>';

    return items.map(p => {
        const s1 = p.id1.split(':');
        const s2 = p.id2.split(':');
        // Pool pairs can cross collections, so each side keeps its own.
        const col = p.meta1?.collection || s1[0];
        const col2 = p.meta2?.collection || s2[0];

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
            collection: col2
        };

        // Single best-shared cluster for the pair (empty when the two share none).
        const sharedClusters = (p.shared_clusters || []).map(cid => clustersMap[cid]).filter(Boolean);

        // Neighbors mode: render only the side that is NOT the anchor function, as a single row.
        if (anchorMd5 && anchorAddress) {
            const isFunc1Anchor = (m1 === anchorMd5 && addr1 === anchorAddress);
            const otherData = isFunc1Anchor ? func2Data : func1Data;
            const otherId = isFunc1Anchor ? p.id2 : p.id1;
            const otherAddr = isFunc1Anchor ? addr2 : addr1;
            const otherMeta = isFunc1Anchor ? p.meta2 : p.meta1;
            const otherMd5 = isFunc1Anchor ? m2 : m1;

            return `
            <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${pairId}" data-id1="${p.id1}" data-id2="${p.id2}" data-algo="${p.algo}" data-sid="${p.sid || ''}"
                data-entity-data='${JSON.stringify(p).replace(/'/g, "&apos;")}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'similarity', this)">
                <td>
                    <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;"
                        onclick="openDiffDirectly('${p.id1}', '${(p.name1 || '').replace(/'/g, "\\'")}', '${p.id2}', '${(p.name2 || '').replace(/'/g, "\\'")}', event)"
                        title="Run Aligned Diff">${(p.score * 100).toFixed(1)}%</div>
                    ${EntityRenderer.renderTag('similarity', pairId, tags, user_tags)}
                </td>
                <td style="min-width: 300px;">${EntityRenderer.renderFunction(otherData, { hideNote: true })}</td>
                <td class="sim-cell"><span class="mono" style="color:var(--accent);">@ ${otherAddr}</span></td>
                <td>${EntityRenderer.renderTag('function', otherId, otherMeta?.tags || [], otherMeta?.user_tags || [], { maxTags: 4 })}</td>
                <td><div class="cluster-cards-cell" data-clusters='${JSON.stringify(sharedClusters).replace(/'/g, "&apos;")}'>${EntityRenderer.renderClusterCard(sharedClusters)}</div></td>
                <td class="sim-cell" style="text-align:center;">
                    <span class="mono" style="color:var(--accent);">${otherMeta?.bsim_features_count || 0}</span>
                    <button class="btn-icon" onclick="showFeaturePanel('${otherId}', event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                </td>
                <td class="sim-cell" style="text-align:center;">${EntityRenderer.renderNoteButton(otherId, otherMeta?.note_owners, { isTable: true, raw_data: otherMeta })}</td>
                <td class="sim-cell" style="color:#aaa; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${otherMeta?.file_name}">${EntityRenderer.renderFileName(otherMeta?.file_name, otherMd5, col)}</td>
                <td class="sim-cell">${EntityRenderer.renderMd5(otherMd5)}</td>
            </tr>
            `;
        }

        return `
        <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${escapeAttr(pairId)}" data-id1="${escapeAttr(p.id1)}" data-id2="${escapeAttr(p.id2)}" data-algo="${escapeAttr(p.algo)}" data-sid="${escapeAttr(p.sid || '')}"
            data-entity-data='${escapeAttr(JSON.stringify(p))}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'similarity', this)">
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="font-size:1.1rem; font-weight:bold; color:var(--success); cursor:pointer;"
                        onmouseenter="showDiffPreview(${escapeAttr(jsString(p.id1))}, ${escapeAttr(jsString(p.name1 || ''))}, ${escapeAttr(jsString(p.id2))}, ${escapeAttr(jsString(p.name2 || ''))}, ${Number(p.score) || 0}, event)"
                        onmousemove="moveCodePreview(event)"
                        onmouseleave="hideDiffPreview(event)"
                        onclick="openDiffDirectly(${escapeAttr(jsString(p.id1))}, ${escapeAttr(jsString(p.name1 || ''))}, ${escapeAttr(jsString(p.id2))}, ${escapeAttr(jsString(p.name2 || ''))}, event)"
                        title="Run Aligned Diff">${(p.score * 100).toFixed(1)}%</div>
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
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent);">@ ${escapeHtml(addr1)}</span></div>
                    <div style="min-height:24px; display:flex; align-items:center;"><span class="mono" style="color:var(--accent);">@ ${escapeHtml(addr2)}</span></div>
                </div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('function', p.id1, p.meta1?.tags || [], p.meta1?.user_tags || [])}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('function', p.id2, p.meta2?.tags || [], p.meta2?.user_tags || [])}</div>
                </div>
            </td>
            <td>
                <div style="min-height:24px; display:flex; align-items:center;" class="cluster-cards-cell" data-clusters='${escapeAttr(JSON.stringify(sharedClusters))}'>${EntityRenderer.renderClusterCard(sharedClusters)}</div>
            </td>
            <td class="sim-cell" style="text-align:center; vertical-align:middle;">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta1?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel(${escapeAttr(jsString(p.id1))}, event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        <span class="mono" style="color:var(--accent);">${p.meta2?.bsim_features_count || 0}</span>
                        <button class="btn-icon" onclick="showFeaturePanel(${escapeAttr(jsString(p.id2))}, event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7; margin-left: 5px;">🔍</button>
                    </div>
                </div>
            </td>
            <td class="sim-cell" style="text-align:center; vertical-align:middle;">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        ${EntityRenderer.renderNoteButton(p.id1, p.meta1?.note_owners, { isTable: true, raw_data: p.meta1 })}
                    </div>
                    <div style="min-height:24px; display:flex; align-items:center; justify-content:center;">
                        ${EntityRenderer.renderNoteButton(p.id2, p.meta2?.note_owners, { isTable: true, raw_data: p.meta2 })}
                    </div>
                </div>
            </td>
            <td class="sim-cell">
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="color:var(--meta-text-muted); max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8; min-height:24px; display:flex; align-items:center;" title="${escapeAttr(p.meta1?.file_name)}">${EntityRenderer.renderFileName(p.meta1?.file_name, m1, col)}</div>
                    <div style="color:var(--meta-text-muted); max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8; min-height:24px; display:flex; align-items:center;" title="${escapeAttr(p.meta2?.file_name)}">${EntityRenderer.renderFileName(p.meta2?.file_name, m2, col2)}</div>
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
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${col}:file:${p.meta1?.file_md5}`, p.meta1?.file_tags || [], p.meta1?.file_user_tags || [], { maxTags: 4 })}</div>
                    <div style="min-height:24px; display:flex; align-items:center;">${EntityRenderer.renderTag('file', `${col2}:file:${p.meta2?.file_md5}`, p.meta2?.file_tags || [], p.meta2?.file_user_tags || [], { maxTags: 4 })}</div>
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
            ${window.renderCollectionCell ? window.renderCollectionCell(col, col2) : ''}
        </tr>
    `}).join('');
}

function showDiffPanel(force = false) {
    if (!force && diffSelection.length < 2) return;

    let url = '/collections/main/diff';
    let label = 'Function Comparison';

    if (diffSelection.length === 2) {
        const id1 = diffSelection[0].id;
        const id2 = diffSelection[1].id;
        url = buildDiffUrl(id1, id2);
        label = `Diff: ${diffSelection[0].name} vs ${diffSelection[1].name}`;

        // Reset queue after opening
        diffSelection = [];
        updateDiffQueueUI();
        saveDiffQueue();
    }

    Nav.openPath(url, null, { title: label, type: 'diff' });
}

function openDiffDirectly(id1, name1, id2, name2, e) {
    // Build RESTful UI path (not API URL)
    const p1 = window.parseFuncIdFromStr ? window.parseFuncIdFromStr(id1) : null;
    const p2 = window.parseFuncIdFromStr ? window.parseFuncIdFromStr(id2) : null;
    if (!p1 || !p2) {
        // Fallback: try buildDiffUrl
        url = buildDiffUrl(id1, id2);
        Nav.openPath(url, e, { title: `Diff: ${name1} vs ${name2}`, type: 'diff' });
        return;
    }

    const pool = p1.pool || (window.getRoutingState ? window.getRoutingState().pool : null);
    const url = (pool
        ? `/pools/${encodeURIComponent(pool)}/collections/${encodeURIComponent(p1.collection_a)}/files/${encodeURIComponent(p1.md5_a)}/functions/${encodeURIComponent(p1.addr_a)}/vs/${encodeURIComponent(p2.collection_b || p2.collection_a)}/${encodeURIComponent(p2.md5_b)}/${encodeURIComponent(p2.addr_b)}`
        : `/collections/${encodeURIComponent(p1.collection_a)}/files/${encodeURIComponent(p1.md5_a)}/functions/${encodeURIComponent(p1.addr_a)}/vs/${encodeURIComponent(p2.collection_b || p2.collection_a)}/${encodeURIComponent(p2.md5_b)}/${encodeURIComponent(p2.addr_b)}`
    );
    Nav.openPath(url, e, { title: `Diff: ${name1} vs ${name2}`, type: 'diff' });
}

function showDiffView() {
    let url = '/collections/main/diff';
    if (diffSelection.length === 2) {
        const p1 = diffSelection[0], p2 = diffSelection[1];
        const pool = p1.pool || (window.getRoutingState ? window.getRoutingState().pool : null);

        if (p1.md5_a && p1.addr_a && p2.md5_b && p2.addr_b) {
            // Flat params format
            const collA = encodeURIComponent(p1.collection_a || '');
            const collB = encodeURIComponent(p2.collection_b || p1.collection_a || '');
            const md5A = encodeURIComponent(p1.md5_a);
            const md5B = encodeURIComponent(p2.md5_b);
            const addrA = encodeURIComponent(p1.addr_a);
            const addrB = encodeURIComponent(p2.addr_b);
            url = pool
                ? `/pools/${encodeURIComponent(pool)}/collections/${collA}/files/${md5A}/functions/${addrA}/vs/${collB}/${md5B}/${addrB}`
                : `/collections/${collA}/files/${md5A}/functions/${addrA}/vs/${collB}/${md5B}/${addrB}`;
        } else {
            // Back-compat: legacy ID format
            url = buildDiffUrl(p1.id, p2.id);
        }

        // Reset queue after opening in new window
        diffSelection = [];
        updateDiffQueueUI();
        saveDiffQueue();
    }
    Nav.openPath(url, null, { title: 'Function Comparison', type: 'diff' });
}

function showFunctionCodeById(id, name, lineHash = '', e) {
    if (window.getSelection && window.getSelection().toString().trim()) {
        return;
    }
    const f = window.parseFuncId(id);
    const url = Nav.buildUIUrl(f.collection, ['function', f.md5, f.address]) + lineHash;
    Nav.openPath(url, e, { title: `Code: ${name}`, type: 'code' });
}

function seeSimilarFromCode() {
    const win = windowManager.activeWindow;
    if (!win || !win.iframe || !win.iframe.src) return;

    let col, md5, addr;
    try {
        const winContent = win.iframe.contentWindow;
        if (winContent.parseRestfulPath) {
            const restful = winContent.parseRestfulPath();
            col = restful.collection;
            md5 = restful.md5;
            addr = restful.address;
        }

        if (!col || !md5 || !addr) {
            const url = new URL(winContent.location.href);
            const params = url.searchParams;
            const id = params.get('id');
            if (id) {
                const f = window.parseFuncId(id);
                col = f.collection;
                md5 = f.md5;
                addr = f.address;
            } else {
                col = params.get('collection');
                md5 = params.get('md5') || params.get('file_md5');
                addr = params.get('address') || params.get('entrypoint_address');
            }
        }
    } catch (e) {
        const url = new URL(win.iframe.src, window.location.origin);
        const params = url.searchParams;
        const id = params.get('id');
        if (id) {
            const f = window.parseFuncId(id);
            col = f.collection;
            md5 = f.md5;
            addr = f.address;
        }
    }

    if (!col || !md5 || !addr) return;

    const newParams = new URLSearchParams();
    newParams.set('md5', md5);
    newParams.set('address', addr);
    newParams.set('algo', 'unweighted_cosine');

    navigate('function-similarity', newParams, col);
    windowManager.closeWindow(win);
}


function showFileDetailsPanel(col, md5, name, e) {
    const url = Nav.buildUIUrl(col, ['file', md5]);
    Nav.openPath(url, e, { title: `File: ${name}`, type: 'file' });
}

function showFeaturePanel(id, e) {
    const f = window.parseFuncId(id);
    const url = Nav.buildUIUrl(f.collection, ['function_features', f.md5, f.address]);
    Nav.openPath(url, e, { title: `Features: ${f.address}`, type: 'features' });
}

function showGlobalFeaturePanel(hash, collection, e) {
    const url = Nav.buildUIUrl(collection, ['feature', hash]);
    Nav.openPath(url, e, { title: `Feature Analysis: ${hash.substring(0, 12)}...`, type: 'global-feature' });
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

// getRoutingState is now in utils.js

function navigate(viewKey, queryParams = null, collection = null, replace = false) {
    const currentParams = new URLSearchParams(window.location.search);
    const restful = parseRestfulPath();
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    const hasColInPath = parts[0] === 'collection' || parts[0] === 'collections' || parts[0] === 'pool' || parts[0] === 'pools';
    let col = collection || (hasColInPath ? restful.collection : null) || currentParams.get('collection') || null;
    if (col === 'null' || col === 'undefined') {
        col = null;
    }
    const pool = window.getRoutingState ? window.getRoutingState().pool : null;
    const isGlobalView = viewKey === 'pools' || viewKey === 'collections' || viewKey === 'jobs' || parts[0] === 'pools' || parts[0] === 'collections';
    if (!isGlobalView && ((!col && !pool) || col === 'null' || col === 'undefined')) {
        showNullContextWarning(col, pool, viewKey);
    }
    const params = queryParams || currentParams;

    let url;
    if (pool) {
        const poolId = pool;
        url = `/pools/${encodeURIComponent(poolId)}/${viewKey}`;
        if (viewKey === 'pool-detail') {
            url = `/pools/${encodeURIComponent(poolId)}`;
        } else if (viewKey === 'files') {
            url = `/pools/${encodeURIComponent(poolId)}/files`;
        } else if (viewKey === 'functions') {
            url = `/pools/${encodeURIComponent(poolId)}/functions`;
        } else if (viewKey === 'batches') {
            url = `/pools/${encodeURIComponent(poolId)}/batches`;
        } else if (viewKey === 'features-global') {
            url = `/pools/${encodeURIComponent(poolId)}/features`;
        } else if (viewKey === 'clusters') {
            url = `/pools/${encodeURIComponent(poolId)}/functions/clusters`;
        } else if (viewKey === 'bin-clusters') {
            url = `/pools/${encodeURIComponent(poolId)}/files/clusters`;
        } else if (viewKey === 'binary-similarity') {
            url = `/pools/${encodeURIComponent(poolId)}/files/similarities`;
        } else if (viewKey === 'function-similarity') {
            url = `/pools/${encodeURIComponent(poolId)}/functions/similarities`;
        } else if (viewKey === 'upload') {
            url = `/pools/${encodeURIComponent(poolId)}/upload`;
        } else if (viewKey === 'pools') {
            url = `/pools`;
        }
    } else {
        url = `/collections/${col}/${viewKey}`;
        if (viewKey === 'collection-detail') {
            url = `/collections/${col}`;
        } else if (viewKey === 'files') {
            url = `/collections/${col}/files`;
        } else if (viewKey === 'functions') {
            url = `/collections/${col}/functions`;
        } else if (viewKey === 'batches') {
            url = `/collections/${col}/batches`;
        } else if (viewKey === 'features-global') {
            url = `/collections/${col}/features`;
        } else if (viewKey === 'clusters') {
            url = `/collections/${col}/functions/clusters`;
        } else if (viewKey === 'bin-clusters') {
            url = `/collections/${col}/files/clusters`;
        } else if (viewKey === 'binary-similarity') {
            url = `/collections/${col}/files/similarities`;
        } else if (viewKey === 'function-similarity') {
            url = `/collections/${col}/functions/similarities`;
        } else if (viewKey === 'upload') {
            url = `/collections/${col}/upload`;
        }
    }
    if (viewKey === 'collections') {
        url = `/collections`;
    } else if (viewKey === 'jobs') {
        if (pool) {
            url = col ? `/pools/${encodeURIComponent(pool)}/collections/${encodeURIComponent(col)}/jobs` : `/pools/${encodeURIComponent(pool)}/jobs`;
        } else if (col) {
            url = `/collections/${encodeURIComponent(col)}/jobs`;
        } else {
            url = `/jobs`;
        }
    } else if (viewKey === 'pools') {
        url = `/pools`;
    }

    // Clean up params as collection is in the path (except for jobs)
    const cleanParams = new URLSearchParams(params);
    if (viewKey !== 'jobs') {
        cleanParams.delete('collection');
    }

    if (cleanParams.toString()) url += `?${cleanParams.toString()}`;

    if (replace) {
        history.replaceState(null, '', url);
    } else {
        history.pushState(null, '', url);
    }

    // Ensure all tooltips are hidden when navigating/switching views
    if (window.hideAllTooltips) {
        window.hideAllTooltips();
    }

    refreshData();
}

window.addEventListener('popstate', (e) => {
    // Ensure all tooltips are hidden
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

    const { viewKey, collection } = getRoutingState();

    if (window.isClearingFilters) {
        window.isClearingFilters = false;
        const queryString = window.location.search;
        localStorage.setItem(`savedFilters:${collection}:${viewKey}`, queryString || `collection=${collection}`);
    }

    // Update rebuild buttons state immediately
    if (window.updateJobStatusIcon) window.updateJobStatusIcon();

    refreshData();
});

// Deprecated hashchange listener for compatibility during transition
window.addEventListener('hashchange', (e) => {
    // Only handle legacy hash routing if we're on the root path
    if (window.location.pathname !== '/' && window.location.pathname !== '') {
        return;
    }
    
    // If we have a hash, convert it to a restful path and navigate
    if (window.location.hash) {
        const [hashPath, queryString] = window.location.hash.split('?');
        const viewKey = hashPath.substring(1);
        const validViewKeys = [
            'collections', 'pools', 'batches', 'files', 'functions', 'features-global',
            'function-similarity', 'clusters', 'upload', 'binary-similarity', 'bin-clusters', 'jobs',
            'function', 'file', 'diff', 'call_graph', 'feature', 'function_features', 'pool-detail',
            'collection-detail', 'bin_sim'
        ];
        if (validViewKeys.includes(viewKey)) {
            const params = new URLSearchParams(queryString);
            const col = params.get('collection') || null;

            window.location.hash = ''; // Clear hash
            navigate(viewKey, params, col);
        }
    }
});

// UI Settings
const UIParams = {
    colorByTag: localStorage.getItem('colorByTag') === 'true',
    includeHeaders: localStorage.getItem('includeHeaders') === 'true',
    useFloatingWindows: localStorage.getItem('useFloatingWindows') === null ? false : localStorage.getItem('useFloatingWindows') === 'true',
    lightTheme: localStorage.getItem('lightTheme') === 'true'
};
window.UIParams = UIParams;

// Apply theme early on load
// ponytail: light theme initialization
if (UIParams.lightTheme) {
    document.documentElement.classList.add('light-theme');
}

function toggleUISettings() {
    const panel = document.getElementById('ui-settings-panel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

function updateUIParams() {
    const prevColorByTag = UIParams.colorByTag;

    UIParams.colorByTag = document.getElementById('param-color-tags').checked;
    UIParams.includeHeaders = document.getElementById('param-include-headers').checked;
    UIParams.useFloatingWindows = document.getElementById('param-use-floating-windows').checked;
    UIParams.lightTheme = document.getElementById('param-light-theme').checked;

    localStorage.setItem('colorByTag', UIParams.colorByTag);
    localStorage.setItem('includeHeaders', UIParams.includeHeaders);
    localStorage.setItem('useFloatingWindows', UIParams.useFloatingWindows);
    localStorage.setItem('lightTheme', UIParams.lightTheme);

    // ponytail: light theme toggle action
    if (UIParams.lightTheme) {
        document.documentElement.classList.add('light-theme');
    } else {
        document.documentElement.classList.remove('light-theme');
    }

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
    const elFloatingWindows = document.getElementById('param-use-floating-windows');
    const elLightTheme = document.getElementById('param-light-theme');
    if (elColorTags) elColorTags.checked = UIParams.colorByTag;
    if (elIncludeHeaders) elIncludeHeaders.checked = UIParams.includeHeaders;
    if (elFloatingWindows) elFloatingWindows.checked = UIParams.useFloatingWindows;
    if (elLightTheme) elLightTheme.checked = UIParams.lightTheme;
}

window.addEventListener('load', () => {
    loadUIParams();

    const { collection, viewKey, pool } = getRoutingState();
    updateNavVisibility(collection);

    if (window.location.hash && (window.location.pathname === '/' || window.location.pathname === '')) {
        // Migration for users with bookmarks
        const [hashPath, queryString] = window.location.hash.split('?');
        const viewKey = hashPath.substring(1);
        const validViewKeys = [
            'collections', 'pools', 'batches', 'files', 'functions', 'features-global',
            'function-similarity', 'clusters', 'upload', 'binary-similarity', 'bin-clusters', 'jobs',
            'function', 'file', 'diff', 'call_graph', 'feature', 'function_features', 'pool-detail',
            'collection-detail', 'bin_sim'
        ];
        if (validViewKeys.includes(viewKey)) {
            const params = new URLSearchParams(queryString);
            const col = params.get('collection') || null;
            window.location.hash = ''; // Clear hash
            navigate(viewKey, params, col, true);
            return;
        }
    }

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

    refreshData();
    loadDiffQueue();

    function setupSidebarHistoryHover() {
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
    }
    window.setupSidebarHistoryHover = setupSidebarHistoryHover;
    setupSidebarHistoryHover();


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
        const { collection, params } = getRoutingState();
        const algo = params.get('algo') || 'unweighted_cosine';

        // Select all possible buttons
        const btns = document.querySelectorAll('.nav-rebuild-btn');
        const icons = document.querySelectorAll('.nav-rebuild-icon');

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

    let jobStatusInFlight = false;
    window.updateJobStatusIcon = async () => {
        // ponytail: one poll at a time. Without this, a slow /api/jobs/stats lets
        // polls overlap and orphan intervals, which snowballs into more polls.
        if (jobStatusInFlight) return;
        jobStatusInFlight = true;
        try {
            const res = await fetch('/api/jobs/stats');
            if (!res.ok) return;
            const stats = await res.json();
            const loader = document.getElementById('nav-jobs-loader');
            const icon = document.getElementById('nav-jobs-icon');
            const navLink = document.getElementById('nav-jobs');

            // active_workers is now a real worker-process count, so the spinner
            // keys off running jobs instead -- otherwise an idle-but-alive fleet
            // would spin forever.
            const activeJobs = stats.active_jobs_count ?? 0;
            const isActive = activeJobs > 0 || stats.pending_jobs > 0;

            if (isActive && typeof refreshActiveJobsByTarget === 'function') {
                refreshActiveJobsByTarget();
            } else {
                window.activeJobsByTarget = {};
            }

            if (loader && icon && navLink) {
                if (isActive) {
                    loader.style.display = 'block';
                    icon.style.display = 'none';
                    navLink.title = `${activeJobs} active, ${stats.pending_jobs} pending jobs (${stats.active_workers} workers)`;
                } else {
                    loader.style.display = 'none';
                    icon.style.display = 'inline-block';
                    navLink.title = "Background Jobs";
                }
            }

            // Update rebuild buttons
            const btns = document.querySelectorAll('.nav-rebuild-btn');
            const icons = document.querySelectorAll('.nav-rebuild-icon');

            const { collection: currentCollection, pool: currentPool, viewKey: path } = getRoutingState();

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

            // Update view-specific job indicator
            const activeJobList = stats.active_jobs || [];
            const isJobInContext = (job) => {
                if (currentPool) {
                    return job.pool_id === currentPool || job.collection === `pool:${currentPool}` || (currentCollection && job.collection === currentCollection);
                }
                if (currentCollection) {
                    return job.collection === currentCollection;
                }
                return false;
            };

            const isJobRelevant = (job) => {
                const type = job.type;
                if (path === 'batches' || path === 'upload') {
                    return ['file_data_ingest', 'ghidra_analyze'].includes(type);
                }
                if (path === 'files') {
                    return ['file_data_ingest', 'ghidra_analyze', 'idx_meta'].includes(type);
                }
                if (path === 'features-global') {
                    return ['idx_features', 'enrich_features'].includes(type);
                }
                if (path === 'function-similarity') {
                    return ['idx_functions', 'idx_features', 'build_sim', 'build_pool_sim', 'sync_milvus'].includes(type);
                }
                if (path === 'clusters') {
                    return ['cluster_functions', 'cluster_pool'].includes(type);
                }
                if (path === 'binary-similarity') {
                    return ['build_bin_sim', 'build_pool_bin_sim'].includes(type);
                }
                if (path === 'bin-clusters') {
                    return ['cluster_binaries', 'cluster_pool_binaries'].includes(type);
                }
                return false;
            };

            const formatJobType = (type) => {
                if (!type) return 'Job';
                return type.split('_')
                    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
                    .join(' ');
            };

            const matchingJob = activeJobList.find(job => isJobInContext(job) && isJobRelevant(job));
            const statusBadge = document.getElementById('view-job-status');
            if (statusBadge) {
                if (matchingJob) {
                    statusBadge.style.display = 'inline-flex';
                    const iconClass = matchingJob.status === 'running' ? 'fa-circle-notch fa-spin' : 'fa-clock';
                    const progressText = matchingJob.status === 'running' ? ` (${matchingJob.progress}%)` : '';
                    statusBadge.innerHTML = `<i class="fa-solid ${iconClass}"></i> ${formatJobType(matchingJob.type)}${progressText}`;
                    statusBadge.title = `Job ID: ${matchingJob.id} is ${matchingJob.status}`;
                    statusBadge.style.cursor = 'pointer';
                    statusBadge.onclick = () => {
                        if (window.showJobDetails) window.showJobDetails(matchingJob.id);
                    };
                } else {
                    statusBadge.style.display = 'none';
                }
            }

            window.jobsActive = isActive;
        } catch (e) {
            // Silently fail for navbar polling
        } finally {
            jobStatusInFlight = false;
        }
    };
    // ponytail: one fixed interval, no self-rescheduling. Ticks at 3s but only
    // hits the API when jobs are active or every 3rd tick otherwise.
    let jobPollTick = 0;
    window.updateJobStatusIcon();
    window.jobPollInterval = setInterval(() => {
        if (document.visibilityState !== 'visible') return;
        jobPollTick++;
        if (window.jobsActive || jobPollTick % 3 === 0) window.updateJobStatusIcon();
    }, 3000);
});

function updateNavVisibility(collection) {
    const { pool } = getRoutingState();
    const navItems = ['nav-batches', 'nav-files', 'nav-functions', 'nav-features-global', 'nav-function-similarity', 'nav-clusters', 'nav-bin-clusters', 'nav-binary-similarity', 'nav-chord-map'];
    navItems.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = (collection || pool) ? 'flex' : 'none';
    });
}

function renderClusters(items) {
    const { collection } = getRoutingState();
    return items.map(c => {
        const showDots = (c.sample_members && c.sample_members.length > 0) && (c.count > 3 || c.sample_members.length > 3);
        const remaining = showDots ? Math.max(c.count - 3, c.sample_members.length - 3) : 0;

        const sampleMembersHtml = (c.sample_members || []).slice(0, 3).map(m => {
            if (typeof m === 'string') {
                return `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${escapeAttr(m)}">${m}</div>`;
            }
            return `
                <div style="margin-bottom:2px; width: 100%;">
                    ${EntityRenderer.renderFunction(m, { showActions: false })}
                </div>
            `;
        }).join('') + (showDots ? `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); padding-left:2px; line-height:1;">and ${remaining} others ...</div>` : '') || '<span class="dim">—</span>';

        return `
        <tr data-cluster-id="${escapeAttr(c.cluster_id)}"
            data-entity-data='${escapeAttr(JSON.stringify({
            cluster_id: c.cluster_id,
            cluster_uuid: c.cluster_uuid,
            cluster_name: c.cluster_name,
            user_tags: c.user_tags || [],
            tag_id: c.tag_id
        }))}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'cluster', this)">
            <td class="mono cluster-uuid-id-cell" data-uuid="${escapeAttr(c.cluster_uuid)}" data-id="${escapeAttr(c.cluster_id)}">
                <a href="javascript:void(0)" onclick="event.preventDefault(); navigate('functions', new URLSearchParams('cluster_uuid=' + ${escapeAttr(jsString(c.cluster_uuid))}), ${collection ? escapeAttr(jsString(collection)) : 'null'})" style="color:var(--accent); text-decoration:none;">
                    ${(c.cluster_uuid || '').substring(0, 8)}
                </a>
                <div class="dim" style="font-size:0.7rem">ID: ${c.cluster_id}</div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <span id="name-display-${escapeAttr(c.cluster_id)}" style="cursor:pointer;">${escapeHtml(c.cluster_name)}</span>
                    <button class="btn-action" title="Rename" onclick="renameCluster(${escapeAttr(jsString(c.cluster_id))}, ${escapeAttr(jsString(c.cluster_name || ''))})"><i class="fa-solid fa-pen"></i></button>
                </div>
            </td>
            <td>
                <div style="display:inline-flex; align-items:center; gap:8px;">
                    <span style="font-weight:bold; min-width: 25px; text-align: right;">${c.count.toLocaleString()}</span>
                    <a href="javascript:void(0)" onclick="event.preventDefault(); navigate('functions', new URLSearchParams('cluster_uuid=' + ${escapeAttr(jsString(c.cluster_uuid))}), ${collection ? escapeAttr(jsString(collection)) : 'null'})" class="btn-action" title="Functions" onmouseenter="showClusterTableTooltip(event, ${escapeAttr(jsString(c.cluster_uuid))}, ${escapeAttr(jsString(c.cluster_name || ''))}, ${c.count || 0}, ${c.avg_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})" onmouseleave="hideClusterTableTooltip(event)" onmousemove="moveClusterTableTooltip(event)">
                        <i class="fa-solid fa-code"></i>
                    </a>
                    <a href="javascript:void(0)" onclick="event.preventDefault(); navigate('function-similarity', new URLSearchParams('cluster_uuid=' + ${escapeAttr(jsString(c.cluster_uuid))}), ${collection ? escapeAttr(jsString(collection)) : 'null'})" class="btn-action" title="Similarities" style="color:var(--info)">
                        <i class="fa-solid fa-code-compare"></i>
                    </a>
                </div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--success); width:${Math.min(100, c.avg_stability).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.avg_stability).toFixed(2)}</span>
                </div>
            </td>
            <td class="mono dim">${(c.avg_features || 0).toFixed(1)}</td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--info); width:${((c.cohesion_score || 0) * 100).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.cohesion_score || 0).toFixed(2)}</span>
                </div>
            </td>
            <td class="dim">${formatDate(c.created_at)}</td>
            <td>${EntityRenderer.renderTag('cluster', c.tag_id || c.cluster_id, [], c.user_tags || [], { maxTags: 4 })}</td>
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
    const { viewKey, params } = getRoutingState();

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

    params.delete("cluster_tag");
    params.delete("exclude_cluster_tag");
    document.querySelectorAll("#tag-container-cluster .tag-filter-card").forEach(card => {
        const key = (card.dataset.exclude === "true" ? "exclude_" : "") + card.dataset.type;
        params.append(key, quoteFilterValue(card.dataset.value, card.dataset.literal !== "false"));
    });

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

// Set param from an element value, or delete when empty
function _setOrDel(params, key, val) {
    if (val !== undefined && val !== null && val !== '') params.set(key, val);
    else params.delete(key);
}
// Date input (YYYY-MM-DD) -> Unix ms. endOfDay adds a day span for max bounds.
function _dateToMs(val, endOfDay) {
    if (!val) return '';
    const ms = Date.parse(val);
    if (isNaN(ms)) return '';
    return String(endOfDay ? ms + 86399999 : ms);
}

function applyCollectionSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

    _setOrDel(params, 'q', document.getElementById('collection-search-input')?.value);
    _setOrDel(params, 'name', document.getElementById('flt-coll-name')?.value);
    _setOrDel(params, 'min_batches', document.getElementById('flt-coll-min-batches')?.value);
    _setOrDel(params, 'max_batches', document.getElementById('flt-coll-max-batches')?.value);
    _setOrDel(params, 'min_files', document.getElementById('flt-coll-min-files')?.value);
    _setOrDel(params, 'max_files', document.getElementById('flt-coll-max-files')?.value);
    _setOrDel(params, 'min_functions', document.getElementById('flt-coll-min-functions')?.value);
    _setOrDel(params, 'max_functions', document.getElementById('flt-coll-max-functions')?.value);
    _setOrDel(params, 'min_last_updated', _dateToMs(document.getElementById('flt-coll-min-date')?.value, false));
    _setOrDel(params, 'max_last_updated', _dateToMs(document.getElementById('flt-coll-max-date')?.value, true));

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

function applyPoolSearch() {
    if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
    const { viewKey, params } = getRoutingState();

    _setOrDel(params, 'q', document.getElementById('pool-search-input')?.value);
    _setOrDel(params, 'id', document.getElementById('flt-pool-id')?.value);
    _setOrDel(params, 'name', document.getElementById('flt-pool-name')?.value);
    _setOrDel(params, 'sync_status', document.getElementById('flt-pool-status')?.value);
    _setOrDel(params, 'min_created_at', _dateToMs(document.getElementById('flt-pool-min-date')?.value, false));
    _setOrDel(params, 'max_created_at', _dateToMs(document.getElementById('flt-pool-max-date')?.value, true));

    const countLimit = document.getElementById('sim-limit')?.value;
    params.set('limit', countLimit || DEFAULT_PAGE_LIMIT);

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
}

function renderBinClusters(items) {
    const { collection, params } = getRoutingState();
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
                return `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${escapeAttr(m)}">${m}</div>`;
            }
            // For binaries, we don't have a standardized renderer yet, but we can wrap them
            return `<div class="mono" style="font-size:0.7rem; color:var(--accent); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${escapeAttr(m.name)}">${escapeHtml(m.name)}</div>`;
        }).join('') + (showDots ? `<div class="mono" style="font-size:0.7rem; color:var(--text-dim); padding-left:2px; line-height:1;">and ${remaining} others ...</div>` : '') || '<span class="dim">—</span>';

        return `
        <tr data-cluster-id="${escapeAttr(c.cluster_id)}"
            data-entity-data='${escapeAttr(JSON.stringify({
            cluster_id: c.cluster_id,
            cluster_uuid: c.cluster_uuid,
            cluster_name: displayName,
            user_tags: c.user_tags || [],
            tag_id: c.tag_id
        }))}'
            oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'bin_cluster', this)">
            <td class="mono cluster-uuid-id-cell" data-uuid="${escapeAttr(c.cluster_uuid)}" data-id="${escapeAttr(c.cluster_id)}">
                <a href="javascript:void(0)" onclick="event.preventDefault(); navigate('files', new URLSearchParams('bin_cluster_uuid=' + ${escapeAttr(jsString(c.cluster_uuid))}), ${collection ? escapeAttr(jsString(collection)) : 'null'})" style="color:var(--accent); text-decoration:none;">
                    ${(c.cluster_uuid || '').substring(0, 8)}
                </a>
                <div class="dim" style="font-size:0.7rem">ID: ${c.cluster_id}</div>
            </td>
            <td>
                <div style="display:flex; flex-direction:column; gap:4px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span id="name-display-bin-${escapeAttr(c.cluster_id)}" style="cursor:pointer; font-weight:bold;">${escapeHtml(displayName)}</span>
                        <button class="btn-action" title="Rename" onclick="renameBinCluster(${escapeAttr(jsString(c.cluster_id))}, ${escapeAttr(jsString(c.cluster_name || ''))})"><i class="fa-solid fa-pen"></i></button>
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
                    <a href="javascript:void(0)" onclick="event.preventDefault(); navigate('files', new URLSearchParams('bin_cluster_uuid=' + ${escapeAttr(jsString(c.cluster_uuid))}), ${collection ? escapeAttr(jsString(collection)) : 'null'})" class="btn-action" title="Binaries" onmouseenter="showBinClusterTableTooltip(event, ${escapeAttr(jsString(c.cluster_uuid))}, ${escapeAttr(jsString(displayName || ''))}, ${c.count || 0}, ${c.avg_stability || 0}, ${c.cohesion_score || 0}, ${c.avg_features || 0})" onmouseleave="hideBinClusterTableTooltip(event)" onmousemove="moveBinClusterTableTooltip(event)">
                        <i class="fa-solid fa-file-code"></i>
                    </a>
                </div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--success); width:${Math.min(100, c.avg_stability).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.avg_stability).toFixed(2)}</span>
                </div>
            </td>
            <td>
                <div style="display:flex; align-items:center; gap:8px;">
                    <div style="flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; min-width:60px;">
                        <div style="height:100%; background:var(--info); width:${((c.cohesion_score || 0) * 100).toFixed(0)}%"></div>
                    </div>
                    <span class="dim">${(c.cohesion_score || 0).toFixed(2)}</span>
                </div>
            </td>
            <td class="dim">${formatDate(c.created_at)}</td>
            <td>${EntityRenderer.renderTag('bin_cluster', c.tag_id || c.cluster_id, [], c.user_tags || [], { maxTags: 4 })}</td>
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
    const { viewKey, params } = getRoutingState();

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

    params.delete("cluster_tag");
    params.delete("exclude_cluster_tag");
    document.querySelectorAll("#tag-container-bin-cluster .tag-filter-card").forEach(card => {
        const key = (card.dataset.exclude === "true" ? "exclude_" : "") + card.dataset.type;
        params.append(key, quoteFilterValue(card.dataset.value, card.dataset.literal !== "false"));
    });

    currentOffset = 0;
    isEndOfResults = false;
    navigate(viewKey, params);
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

    const { collection, params } = getRoutingState();
    const algo = params.get('algo') || 'unweighted_cosine';
    const nodeType = params.get('node_type') || 'file';

    try {
        const res = await fetch('/api/bin_cluster/meta', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ collection, algo, node_type: nodeType, cluster_id: clusterId, cluster_name: newName })
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

    const { collection, params } = getRoutingState();
    const algo = params.get('algo') || 'unweighted_cosine';

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
    const { viewKey, params } = getRoutingState();
    params.set('view', mode);
    if (mode !== 'hierarchy' && mode !== 'packing') {
        params.delete('show_parents');
        params.delete('show_children');
        params.delete('show_members');
        params.delete('path_compression');
        params.delete('show_binary_sankey');
    }
    navigate(viewKey, params);
}

function switchBinClusterView(mode) {
    const { viewKey, params } = getRoutingState();
    params.set('view', mode);
    if (mode !== 'hierarchy' && mode !== 'packing') {
        params.delete('show_parents');
        params.delete('show_children');
        params.delete('show_members');
        params.delete('path_compression');
    }
    navigate(viewKey, params);
}

function changeBinClusterNameType(value) {
    const { viewKey, params } = getRoutingState();
    params.set('cluster_name_type', value);
    navigate(viewKey, params);
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
            showGlobalFeaturePanel(h, c, me);
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
    if (typeof UI !== 'undefined' && UI.Sidebar) {
        UI.Sidebar.init('sidebar-container');
    }

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

// Forward wheel/scroll events on the main content background to the active scrollable table/container inside it
document.addEventListener('wheel', (e) => {
    if (e.target.closest('#fn-cg-container')) return; // let Pivotick's own zoom-on-wheel handle the call graph
    let element = e.target;
    let isScrollableTarget = false;
    while (element && element !== document.body) {
        const style = window.getComputedStyle(element);
        const overflowY = style.overflowY || style.overflow || '';
        if ((overflowY === 'auto' || overflowY === 'scroll') && element.scrollHeight > element.clientHeight) {
            isScrollableTarget = true;
            break;
        }
        element = element.parentElement;
    }
    
    if (!isScrollableTarget) {
        const mainContent = document.getElementById('main-content');
        if (!mainContent) return;
        
        const scrollables = Array.from(mainContent.querySelectorAll('.table-body-wrap, .bsim-subtab-panel, div')).filter(el => {
            if (el.offsetWidth === 0 || el.offsetHeight === 0) return false;
            const style = window.getComputedStyle(el);
            const overflowY = style.overflowY || style.overflow || '';
            return (overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight;
        });
        
        if (scrollables.length > 0) {
            scrollables[0].scrollTop += e.deltaY;
            e.preventDefault();
        }
    }
}, { passive: false });


// Expose dashboard controllers/globals explicitly on window
window.applyAdvancedFuncSearch = applyAdvancedFuncSearch;
window.applySimSearch = applySimSearch;
window.applyBinSimSearch = applyBinSimSearch;
window.applyClusterSearch = applyClusterSearch;
window.applyCollectionSearch = applyCollectionSearch;
window.applyPoolSearch = applyPoolSearch;
window.switchClusterView = switchClusterView;
window.renameCluster = renameCluster;
window.refreshData = refreshData;
window.clearFilters = clearFilters;
window.resetColumnWidths = resetColumnWidths;
window.toggleFilterActionsDropdown = toggleFilterActionsDropdown;
window.closeFilterActionsDropdown = closeFilterActionsDropdown;
window.addNotIgnoreFilters = addNotIgnoreFilters;
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
    'collections': { name: 'Collections', icon: 'fa-layer-group' },
    'batches': { name: 'Batches', icon: 'fa-boxes-stacked' },
    'files': { name: 'Files', icon: 'fa-file-code' },
    'functions': { name: 'Functions', icon: 'fa-code' },
    'features-global': { name: 'Features', icon: 'fa-fingerprint' },
    'function-similarity': { name: 'Similarities', icon: 'fa-code-compare' },
    'clusters': { name: 'Clusters', icon: 'fa-bullseye' },
    'bin-clusters': { name: 'Bin Clusters', icon: 'fa-bullseye' },
    'tags': { name: 'Tags', icon: 'fa-tags' }
};

function getFilterSummary(path, params) {
    const summary = [];
    const q = params.get('q');
    if (q) summary.push(`q: "${q}"`);

    if (path === 'files') {
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
    } else if (path === 'functions') {
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
    } else if (path === 'function-similarity') {
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
        if (min_score && min_score !== defaultMinScore()) summary.push(`Score >= ${min_score}`);
        if (max_score && max_score !== '1.0') summary.push(`Score <= ${max_score}`);
        if (algo && algo !== 'unweighted_cosine') summary.push(`Algo: ${algo}`);
        if (cross_binary) {
            summary.push(cross_binary === 'true' ? 'Cross-Binary' : 'Same-Binary');
        }
        if (match_mode && match_mode !== 'any') summary.push(`Match: ${match_mode}`);
    } else if (path === 'clusters') {
        const cluster_uuid = params.get('cluster_uuid');
        const cluster_name = params.get('cluster_name');
        const min_count = params.get('min_count');
        const min_cohesion = params.get('min_cohesion');

        if (cluster_uuid) summary.push(`UUID: ${cluster_uuid.substring(0, 6)}`);
        if (cluster_name) summary.push(`Name: "${cluster_name}"`);
        if (min_count && min_count !== '0') summary.push(`Min Funcs: ${min_count}`);
        if (min_cohesion && min_cohesion !== '0') summary.push(`Cohesion >= ${min_cohesion}`);
    } else if (path === 'features-global') {
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
    if (path === 'function-similarity') {
        return viewMode === 'graph' ? 'Graph' : 'Table';
    } else if (path === 'clusters') {
        if (viewMode === 'hierarchy') return 'Hierarchy';
        if (viewMode === 'packing') return 'Packing';
        return 'Table';
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
    if (path === 'collections') return;

    const params = new URLSearchParams(queryString);
    const col = params.get('collection') || '';
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
        return `${item.collection || ''}:${item.path || ''}:${item.view || 'table'}:${JSON.stringify(sortedParams)}`;
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

    let path = item.path;
    if (path.startsWith('#')) path = path.substring(1);

    const params = new URLSearchParams();
    for (const [key, val] of Object.entries(item.params)) {
        if (Array.isArray(val)) {
            val.forEach(v => params.append(key, v));
        } else {
            params.set(key, val);
        }
    }

    const col = params.get('collection');
    navigate(path, params, col);
    closeAllHistoryDropdowns();
}

function renderHistoryDropdowns() {
    let history = [];
    try {
        history = JSON.parse(localStorage.getItem('bsimvis_search_history') || '[]');
    } catch (e) { }

    const globalDropdown = document.getElementById('history-dropdown');
    const viewDropdown = document.getElementById('view-history-dropdown');
    const { viewKey: currentPath, collection: currentCol, params: currentParams } = getRoutingState();

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
                            <span class="history-item-view-name">${escapeHtml(meta.name)}</span>
                            <span class="history-item-graph-type">${graphType}</span>
                            <span class="history-item-time" title="${escapeAttr(new Date(item.timestamp).toLocaleString())}">${formatRelativeTime(item.timestamp)}</span>
                        </div>
                        <div class="history-item-summary" title="${escapeAttr(esc(item.summary))}">${esc(item.summary)}</div>
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
                    <button class="history-dropdown-clear-btn" onclick="clearViewHistory(event, ${escapeAttr(jsString(currentPath))})">
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
                            <span class="history-item-view-name">${escapeHtml(meta.name)}</span>
                            <span class="history-item-graph-type">${graphType}</span>
                            <span class="history-item-time" title="${escapeAttr(new Date(item.timestamp).toLocaleString())}">${formatRelativeTime(item.timestamp)}</span>
                        </div>
                        <div class="history-item-summary" title="${escapeAttr(esc(item.summary))}">${esc(item.summary)}</div>
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
    const { collection: currentCol } = getRoutingState();

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
    const { collection: currentCol } = getRoutingState();

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
    const { viewKey, params } = getRoutingState();
    const route = routes[viewKey];
    if (!route || !route.api) {
        alert("Downloads are not available for this view.");
        return;
    }

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

async function renderPoolCreationForm() {
    const gridHeader = document.getElementById('grid-header');
    if (!gridHeader) return;

    // Fetch existing collections to select from
    let collections = [];
    try {
        const res = await fetch('/api/collection/search?limit=10000'); // ponytail: lift limit to get all collections
        if (res.ok) {
            const data = await res.json();
            collections = data.collections || data || [];
        }
    } catch (e) {
        console.error("Failed to fetch collections for pool creation", e);
    }

    // Fetch default config parameters dynamically
    let config = null;
    try {
        const res = await fetch('/api/index/config');
        if (res.ok) {
            config = await res.json();
        }
    } catch (e) {
        console.error("Failed to fetch default config for pool creation", e);
    }

    const clustering = config?.clustering || {};
    const similarity = config?.similarity || {};

    const funcAlgo = similarity.algo || 'unweighted_cosine';
    const funcTopK = similarity.top_k !== undefined ? similarity.top_k : 1000;
    const funcMinScore = similarity.min_score !== undefined ? similarity.min_score : 0.9;
    const funcMinFeatures = similarity.min_features !== undefined ? similarity.min_features : 0;
    const funcClusterMinSize = clustering.min_cluster_size !== undefined ? clustering.min_cluster_size : 2;
    const funcClusterMinSamples = clustering.min_samples !== undefined ? clustering.min_samples : 1;
    const funcClusterEpsilon = clustering.epsilon !== undefined ? clustering.epsilon : 0.1;
    const funcClusterMethod = clustering.selection_method || 'eom';

    const fileTopK = 100; // default for file-level similarity
    const fileMinScore = 0.5; // default for file-level similarity
    const fileClusterMinSize = clustering.min_cluster_size !== undefined ? clustering.min_cluster_size : 2;
    const fileClusterMinSamples = clustering.min_samples !== undefined ? clustering.min_samples : 1;
    const fileClusterEpsilon = clustering.epsilon !== undefined ? clustering.epsilon : 0.001;
    const fileClusterMethod = clustering.selection_method || 'eom';

    const colCheckboxes = collections.map(col => `
        <label style="display:flex; align-items:center; gap:8px; padding:6px 12px; cursor:pointer; font-size:0.8rem; border-bottom: 1px solid var(--border); transition: background 0.2s;">
            <input type="checkbox" name="pool-collections" value="${escapeAttr(col.name)}" onchange="updateAutoPoolName()">
            <span>${escapeHtml(col.name)} <span style="font-size:0.7rem; color:var(--dim);">(${col.total_files || 0} files)</span></span>
        </label>
    `).join('');

    gridHeader.innerHTML = `
        <div id="create-pool-card" style="background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 25px; overflow:hidden;">
            <!-- COLLAPSIBLE HEADER -->
            <div onclick="togglePoolCreationForm()" style="padding:15px 25px; display:flex; justify-content:space-between; align-items:center; cursor:pointer; background: var(--hover); transition: background 0.2s;" onmouseover="this.style.background='var(--border)'" onmouseout="this.style.background='var(--border)'">
                <h3 style="margin:0; font-size:1.05rem; color:var(--accent); display:flex; align-items:center; gap:12px;">
                    <i class="fa-solid fa-diagram-project"></i> Create New Pool
                </h3>
                <div id="pool-toggle-icon" style="color:var(--dim); font-size:0.9rem; transition: transform 0.3s ease; transform: rotate(180deg);">
                    <i class="fa-solid fa-chevron-up"></i>
                </div>
            </div>

            <div id="pool-creation-content" style="padding:0 25px 25px 25px; display: none;">
                <div id="pool-creation-form-container" style="border-top: 1px solid var(--border); padding-top:20px;">
                    <div style="margin-bottom: 25px;">
                        <label style="display:block; font-size:0.75rem; color:var(--dim); margin-bottom:6px; font-weight:600; text-transform:uppercase;">Pool Name</label>
                        <input type="text" id="new-pool-name" placeholder="e.g. Shared Analysis Pool" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:10px; border-radius:6px; font-size:0.85rem;">
                    </div>

                    <div style="display:grid; grid-template-columns: 320px 1fr; gap:30px;">
                        <!-- LEFT COLUMN: COLLECTIONS -->
                        <div style="display:flex; flex-direction:column; gap:12px;">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <label style="font-size:0.75rem; color:var(--dim); font-weight:600; text-transform:uppercase;">Collections</label>
                                <div style="display:flex; gap:10px;">
                                    <button onclick="event.preventDefault(); document.querySelectorAll('input[name=\\'pool-collections\\']').forEach(cb => cb.checked = true); updateAutoPoolName();" style="background:none; border:none; padding:0; font-size:0.7rem; color:var(--accent); cursor:pointer; font-weight:600;">All</button>
                                    <button onclick="event.preventDefault(); document.querySelectorAll('input[name=\\'pool-collections\\']').forEach(cb => cb.checked = false); updateAutoPoolName();" style="background:none; border:none; padding:0; font-size:0.7rem; color:var(--dim); cursor:pointer; font-weight:600;">None</button>
                                </div>
                            </div>
                            <div style="position:relative;">
                                <i class="fa-solid fa-magnifying-glass" style="position:absolute; left:12px; top:50%; transform:translateY(-50%); font-size:0.8rem; color:var(--dim);"></i>
                                <input type="text" placeholder="Filter collections..." oninput="filterPoolCollections(this.value)" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:8px 10px 8px 35px; border-radius:6px; font-size:0.8rem;">
                            </div>
                            <div id="pool-collections-list" style="background:var(--border); border:1px solid var(--border); border-radius:6px; max-height:430px; overflow-y:auto; scrollbar-width: thin;">
                                ${colCheckboxes.length ? colCheckboxes : '<div style="padding:20px; font-size:0.85rem; color:var(--dim); text-align:center;">No collections found.</div>'}
                            </div>
                        </div>
                        
                        <!-- RIGHT COLUMN: CONFIGURATION -->
                        <div style="display:flex; flex-direction:column; gap:15px;">
                            <div style="display:flex; align-items:center; gap:25px; background:rgba(255,171,46,0.03); border:1px solid rgba(255,171,46,0.15); border-radius:8px; padding:12px 15px;">
                                <div style="display:flex; align-items:center; gap:8px; background:var(--border); padding:6px 12px; border-radius:20px; border:1px solid var(--border); flex-shrink:0;">
                                    <input type="checkbox" id="pool-cross-only" style="cursor:pointer; width:14px; height:14px; accent-color:var(--accent);">
                                    <label for="pool-cross-only" style="font-size:0.75rem; cursor:pointer; font-weight:700; color:var(--accent); display:flex; align-items:center; gap:4px;">
                                        <i class="fa-solid fa-arrow-right-arrow-left"></i> CROSS-ONLY
                                    </label>
                                </div>
                                <div style="flex:1;">
                                    <div style="font-size:0.75rem; color:var(--text); font-weight:700; margin-bottom:2px; display:flex; align-items:center; gap:8px;">
                                        <i class="fa-solid fa-bolt" style="color:var(--accent);"></i> Analysis Scope
                                    </div>
                                    <div style="font-size:0.7rem; color:var(--dim);">Discovery focused on cross-collection pairs only.</div>
                                </div>
                            </div>

                            <div style="display:flex; flex-direction:column; gap:12px; background:var(--border); border:1px solid var(--border); border-radius:8px; padding:15px;">
                                <div style="display:flex; align-items:center; gap:8px; color:var(--accent); font-weight:600; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.03em;">
                                    <i class="fa-solid fa-microchip"></i> Function-Level
                                </div>
                                
                                <div style="display:grid; grid-template-columns: 1fr 1fr; gap:20px;">
                                    <div>
                                        <div style="display:grid; grid-template-columns: 1fr; gap:10px;">
                                            <div>
                                                <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Algorithm</label>
                                                <select id="pool-func-algo" onchange="const d=document.getElementById('pool-file-algo-display'); if(d) d.textContent=this.value;" style="width:100%; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                    <option value="unweighted_cosine" ${funcAlgo === 'unweighted_cosine' ? 'selected' : ''}>Unweighted Cosine</option>
                                                    <option value="jaccard" ${funcAlgo === 'jaccard' ? 'selected' : ''}>Jaccard</option>
                                                </select>
                                            </div>
                                            <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:8px;">
                                                <div>
                                                    <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Top K</label>
                                                    <input type="number" id="pool-func-topk" value="${escapeAttr(funcTopK)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                </div>
                                                <div>
                                                    <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Score</label>
                                                    <input type="number" id="pool-func-minscore" step="0.05" value="${escapeAttr(funcMinScore)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                </div>
                                                <div>
                                                    <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Features</label>
                                                    <input type="number" id="pool-func-minfeatures" value="${escapeAttr(funcMinFeatures)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px;">
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Cluster</label>
                                            <input type="number" id="pool-cluster-min-size" value="${escapeAttr(funcClusterMinSize)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Samples</label>
                                            <input type="number" id="pool-cluster-min-samples" value="${escapeAttr(funcClusterMinSamples)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Epsilon</label>
                                            <input type="number" id="pool-cluster-epsilon" step="0.05" value="${escapeAttr(funcClusterEpsilon)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Method</label>
                                            <select id="pool-cluster-method" style="width:100%; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                <option value="eom" ${funcClusterMethod === 'eom' ? 'selected' : ''}>EOM</option>
                                                <option value="leaf" ${funcClusterMethod === 'leaf' ? 'selected' : ''}>Leaf</option>
                                            </select>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div style="display:flex; flex-direction:column; gap:12px; background:var(--border); border:1px solid var(--border); border-radius:8px; padding:15px;">
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <div style="display:flex; align-items:center; gap:8px; color:var(--accent); font-weight:600; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.03em;">
                                        <i class="fa-solid fa-file-code"></i> File-Level
                                    </div>
                                    <div style="display:flex; align-items:center; gap:10px; background: var(--hover); padding:2px 10px; border-radius:20px; border: 1px solid var(--border);">
                                        <input type="checkbox" id="pool-enable-files" checked onchange="document.getElementById('file-params-grid').style.opacity = this.checked ? '1' : '0.4'; document.getElementById('file-params-grid').style.pointerEvents = this.checked ? 'auto' : 'none';" style="cursor:pointer; width:12px; height:12px; accent-color:var(--accent);">
                                        <label for="pool-enable-files" style="font-size:0.7rem; cursor:pointer; font-weight:600; color:var(--text);">Enabled</label>
                                    </div>
                                </div>
                                
                                <div id="file-params-grid" style="display:grid; grid-template-columns: 1fr 1fr; gap:20px; transition: opacity 0.2s;">
                                    <div>
                                        <div style="display:grid; grid-template-columns: 1fr; gap:10px;">
                                            <div>
                                                <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Algorithm</label>
                                                <div id="pool-file-algo-display" title="Inherited from function similarity: file scores live in the namespace of the function clusters they are built from." style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--dim); padding:6px; border-radius:4px; font-size:0.75rem;">${funcAlgo}</div>
                                            </div>
                                            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
                                                <div>
                                                    <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Top K</label>
                                                    <input type="number" id="pool-file-topk" value="${escapeAttr(fileTopK)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                </div>
                                                <div>
                                                    <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Score</label>
                                                    <input type="number" id="pool-file-minscore" step="0.05" value="${escapeAttr(fileMinScore)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px;">
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Cluster</label>
                                            <input type="number" id="pool-file-cluster-min-size" value="${escapeAttr(fileClusterMinSize)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Min Samples</label>
                                            <input type="number" id="pool-file-cluster-min-samples" value="${escapeAttr(fileClusterMinSamples)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Epsilon</label>
                                            <input type="number" id="pool-file-cluster-epsilon" step="0.05" value="${escapeAttr(fileClusterEpsilon)}" style="width:100%; box-sizing:border-box; background:var(--border); border:1px solid var(--border); color:var(--text); padding:6px; border-radius:4px; font-size:0.75rem;">
                                        </div>
                                        <div>
                                            <label style="display:block; font-size:0.65rem; color:var(--dim); margin-bottom:4px;">Method</label>
                                            <select id="pool-file-cluster-method" style="width:100%; background:var(--border); border:1px solid var(--border); color:var(--text); padding:8px; border-radius:4px; font-size:0.8rem;">
                                                <option value="eom" ${fileClusterMethod === 'eom' ? 'selected' : ''}>EOM</option>
                                                <option value="leaf" ${fileClusterMethod === 'leaf' ? 'selected' : ''}>Leaf</option>
                                            </select>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div style="margin-top:auto; padding-top:15px; display:flex; justify-content:flex-end; gap:12px;">
                                <button onclick="renderPoolCreationForm()" class="btn-secondary" style="padding:10px 20px; border-radius:6px; font-size:0.85rem; font-weight:600; cursor:pointer;">Reset</button>
                                <button onclick="submitCreatePool(this)" class="btn-primary" style="padding:10px 25px; border-radius:6px; font-size:0.85rem; font-weight:600; cursor:pointer; display:flex; align-items:center; gap:8px; ">
                                    <i class="fa-solid fa-plus-circle"></i> Create Pool
                                </button>
                            </div>
                        </div>
                    </div>
                    <div id="pool-create-error" style="color:#ef4444; font-size:0.85rem; margin-top:20px; padding:12px 18px; background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.2); border-radius:8px; display:none;"></div>
                </div>
            </div>
        </div>
    `;

    window.isPoolNameManuallyEdited = false;
    const nameInput = document.getElementById('new-pool-name');
    if (nameInput) {
        nameInput.addEventListener('input', () => {
            window.isPoolNameManuallyEdited = true;
        });
    }
}
window.renderPoolCreationForm = renderPoolCreationForm;

function updateAutoPoolName() {
    if (window.isPoolNameManuallyEdited) return;
    const checked = Array.from(document.querySelectorAll('input[name="pool-collections"]:checked')).map(cb => cb.value);
    const poolNameEl = document.getElementById('new-pool-name');
    if (poolNameEl) {
        if (checked.length === 0) {
            poolNameEl.value = '';
        } else {
            poolNameEl.value = checked.join(', ') + ' Pool';
        }
    }
}
window.updateAutoPoolName = updateAutoPoolName;

function togglePoolCreationForm() {
    const content = document.getElementById('pool-creation-content');
    const icon = document.getElementById('pool-toggle-icon');
    if (!content || !icon) return;
    
    if (content.style.display === 'none') {
        content.style.display = 'block';
        icon.style.transform = 'rotate(0deg)';
    } else {
        content.style.display = 'none';
        icon.style.transform = 'rotate(180deg)';
    }
}
window.togglePoolCreationForm = togglePoolCreationForm;

function filterPoolCollections(val) {
    const list = document.getElementById('pool-collections-list');
    if (!list) return;
    const labels = list.querySelectorAll('label');
    const query = val.toLowerCase();
    labels.forEach(lbl => {
        const text = lbl.innerText.toLowerCase();
        lbl.style.display = text.includes(query) ? 'flex' : 'none';
    });
}
window.filterPoolCollections = filterPoolCollections;

async function submitCreatePool(btn) {
    const poolNameEl = document.getElementById('new-pool-name');
    const errEl = document.getElementById('pool-create-error');
    
    // Function settings
    const crossOnly = document.getElementById('pool-cross-only')?.checked ?? false;
    const funcAlgo = document.getElementById('pool-func-algo')?.value ?? 'unweighted_cosine';
    const funcTopK = parseInt(document.getElementById('pool-func-topk')?.value || '1000');
    const funcMinScore = parseFloat(document.getElementById('pool-func-minscore')?.value || defaultMinScore());
    const funcMinFeatures = parseInt(document.getElementById('pool-func-minfeatures')?.value || '0');
    const funcClusterMinSize = parseInt(document.getElementById('pool-cluster-min-size')?.value || '2');
    const funcClusterMinSamples = parseInt(document.getElementById('pool-cluster-min-samples')?.value || '1');
    const funcClusterEpsilon = parseFloat(document.getElementById('pool-cluster-epsilon')?.value || '0.1');
    const funcClusterMethod = document.getElementById('pool-cluster-method')?.value ?? 'eom';
    
    // File settings
    const enableFiles = document.getElementById('pool-enable-files')?.checked ?? false;
    const fileTopK = parseInt(document.getElementById('pool-file-topk')?.value || '100');
    const fileMinScore = parseFloat(document.getElementById('pool-file-minscore')?.value || '0.5');
    const fileClusterMinSize = parseInt(document.getElementById('pool-file-cluster-min-size')?.value || '2');
    const fileClusterMinSamples = parseInt(document.getElementById('pool-file-cluster-min-samples')?.value || '1');
    const fileClusterEpsilon = parseFloat(document.getElementById('pool-file-cluster-epsilon')?.value || '0.1');
    const fileClusterMethod = document.getElementById('pool-file-cluster-method')?.value ?? 'eom';

    if (errEl) errEl.style.display = 'none';

    const poolName = poolNameEl ? poolNameEl.value.trim() : '';
    
    const checkedBoxes = document.querySelectorAll('input[name="pool-collections"]:checked');
    const collections = Array.from(checkedBoxes).map(cb => cb.value);

    if (!poolName || !collections.length) {
        if (errEl) {
            errEl.innerText = "Error: Pool Name and at least one Collection are required.";
            errEl.style.display = 'block';
        }
        return;
    }

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';
    }

    try {
        const res = await fetch('/api/pool', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: poolName,
                collections: collections,
                config: {
                    only_cross_collection: crossOnly,
                    func_sim_params: {
                        algo: funcAlgo,
                        top_k: funcTopK,
                        min_score: funcMinScore,
                        min_features: funcMinFeatures
                    },
                    func_cluster_params: {
                        min_cluster_size: funcClusterMinSize,
                        min_samples: funcClusterMinSamples,
                        epsilon: funcClusterEpsilon,
                        selection_method: funcClusterMethod
                    },
                    file_sim_params: {
                        enabled: enableFiles,
                        top_k: fileTopK,
                        min_score: fileMinScore
                    },
                    file_cluster_params: { 
                        enabled: enableFiles,
                        min_cluster_size: fileClusterMinSize,
                        min_samples: fileClusterMinSamples,
                        epsilon: fileClusterEpsilon,
                        selection_method: fileClusterMethod
                    }
                }
            })
        });

        if (!res.ok) {
            const errData = await res.json();
            throw new Error(errData.error || `HTTP ${res.status}`);
        }

        const data = await res.json();
        
        // Show success message with "Go to Pool" button
        const formContainer = document.getElementById('pool-creation-form-container');
        if (formContainer) {
            formContainer.innerHTML = `
                <div style="background:rgba(34,197,94,0.1); border:1px solid rgba(34,197,94,0.2); border-radius:8px; padding:30px; text-align:center; display:flex; flex-direction:column; align-items:center; gap:20px;">
                    <div style="width:60px; height:60px; background:rgba(34,197,94,0.2); color:#22c55e; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:1.5rem;">
                        <i class="fa-solid fa-check"></i>
                    </div>
                    <div>
                        <h4 style="margin:0 0 8px 0; color:#22c55e;">Pool Created Successfully!</h4>
                        <p style="margin:0; font-size:0.85rem; color:var(--dim);">Pipeline <b>${data.job_id}</b> has been scheduled to process the pool.</p>
                    </div>
                    <div style="display:flex; gap:12px;">
                        <button onclick="Nav.openPath('/pools/${encodeURIComponent(data.pool_id)}')" class="btn-primary" style="padding:10px 20px; font-weight:600; display:flex; align-items:center; gap:8px;">
                            <i class="fa-solid fa-arrow-right"></i> Go to Pool
                        </button>
                        <button onclick="renderPoolCreationForm()" class="top-action-btn" style="padding:10px 20px; height:auto;">
                            Create Another
                        </button>
                    </div>
                </div>
            `;
        } else {
            alert(`Pool created! scheduled job pipeline ID: ${data.job_id}`);
        }
        
        // Refresh the pools list without re-rendering the header creation form
        refreshData(false, true, true);

    } catch (e) {
        if (errEl) {
            errEl.innerText = `Error: ${e.message}`;
            errEl.style.display = 'block';
        }
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fa-solid fa-plus"></i> Create & Process Pool';
        }
    }
}
window.submitCreatePool = submitCreatePool;

async function deletePool(poolId, btn) {
    if (!confirm(`Are you sure you want to delete pool "${poolId}"?`)) return;

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }

    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}`, {
            method: 'DELETE'
        });

        if (!res.ok) {
            const data = await res.json();
            throw new Error(data.error || `HTTP ${res.status}`);
        }

        refreshData(false, true);
    } catch (e) {
        alert(`Failed to delete pool: ${e.message}`);
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fa-solid fa-trash-can"></i>';
        }
    }
}
window.deletePool = deletePool;

async function renamePool(poolId) {
    const el = document.getElementById(`pool-name-${poolId}`);
    const currentName = el ? el.textContent : '';
    const newName = prompt(`Enter new name for pool "${poolId}":`, currentName);
    if (newName === null || newName.trim() === '' || newName === currentName) return;

    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName.trim() })
        });

        if (!res.ok) {
            const data = await res.json();
            throw new Error(data.error || `HTTP ${res.status}`);
        }

        refreshData(false, true);
    } catch (e) {
        alert(`Failed to rename pool: ${e.message}`);
    }
}
window.renamePool = renamePool;

async function buildPool(poolId, btn) {
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }

    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}/build`, {
            method: 'POST'
        });

        if (!res.ok) {
            const data = await res.json();
            throw new Error(data.error || `HTTP ${res.status}`);
        }

        const data = await res.json();
        alert(`Pool build enqueued! Job ID: ${data.job_id}`);
        refreshData(false, true);
    } catch (e) {
        alert(`Failed to build pool: ${e.message}`);
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fa-solid fa-play"></i> Build';
        }
    }
}
window.buildPool = buildPool;

async function rebuildPool(poolId, btn) {
    if (!confirm(`Are you sure you want to WIPE and rebuild all data for pool "${poolId}"?`)) return;

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }

    try {
        const res = await fetch(`/api/pool/${encodeURIComponent(poolId)}/rebuild`, {
            method: 'POST'
        });

        if (!res.ok) {
            const data = await res.json();
            throw new Error(data.error || `HTTP ${res.status}`);
        }

        const data = await res.json();
        alert(`Pool wipe and rebuild enqueued! Job ID: ${data.job_id}`);
        refreshData(false, true);
    } catch (e) {
        alert(`Failed to rebuild pool: ${e.message}`);
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fa-solid fa-rotate"></i> Rebuild';
        }
    }
}
window.rebuildPool = rebuildPool;

window.goToContextJobs = function() {
    const pool = localStorage.getItem('lastPoolContext');
    const col = localStorage.getItem('lastCollectionContext');
    let url;
    if (pool) {
        url = col ? `/pools/${encodeURIComponent(pool)}/collections/${encodeURIComponent(col)}/jobs` : `/pools/${encodeURIComponent(pool)}/jobs`;
    } else if (col) {
        url = `/collections/${encodeURIComponent(col)}/jobs`;
    } else {
        url = '/jobs';
    }
    Nav.openPath(url);
};

window.goToAllJobs = function() {
    Nav.openPath('/jobs');
};

function applyJobSearch() {
    const p = new URLSearchParams();
    const type = document.getElementById('job-type-filter')?.value;
    const collection = document.getElementById('job-collection-filter')?.value;
    const status = document.getElementById('job-status-filter')?.value;

    if (type) p.set('type', type);
    if (collection) p.set('collection', collection);
    if (status) p.set('status', status);

    // Preserve pool context if present
    const pool = window.getRoutingState ? window.getRoutingState().pool : null;
    if (pool) p.set('pool', pool);

    currentOffset = 0;
    isEndOfResults = false;
    navigate('jobs', p);
}
window.applyJobSearch = applyJobSearch;





window.changeBinClusterNodeType = function(type) {
    const { params } = getRoutingState();
    if (type !== 'file') {
        params.set('node_type', type);
    } else {
        params.delete('node_type');
    }
    const path = parseRestfulPath();
    navigate(path.view || 'bin-clusters', params);
};
