/**
 * ui_components.js
 * Centralized UI components and managers.
 */

window.UI = {
    /**
     * Tooltip Manager
     */
    Tooltip: {
        show: function(el, event, content) {
            if (!el) return;
            if (typeof content === 'function') content = content();
            el.innerHTML = content;
            el.style.display = 'block';
            this.position(el, event);
        },
        position: function(el, event) {
            if (!el || el.style.display === 'none') return;
            const x = event.clientX + 15;
            const y = event.clientY + 15;
            el.style.left = x + 'px';
            el.style.top = y + 'px';
            const rect = el.getBoundingClientRect();
            if (rect.right > window.innerWidth) el.style.left = (event.clientX - rect.width - 15) + 'px';
            if (rect.bottom > window.innerHeight) el.style.top = (event.clientY - rect.height - 15) + 'px';
        },
        hide: function(el) {
            if (el) el.style.display = 'none';
        }
    },

    /**
     * Extensible Button Component
     */
    Button: {
        render: function(options = {}) {
            const classes = ['ui-button', options.className || ''].join(' ').trim();
            const titleAttr = options.tooltip ? `title="${escapeAttr(options.tooltip)}"` : '';
            const styleAttr = options.style ? `style="${escapeAttr(options.style)}"` : '';
            let extraAttrs = '';
            if (options.attr) {
                // Values may be JS handler code; HTML-escaping them keeps the
                // attribute intact and the browser unescapes before parsing.
                for (const [key, val] of Object.entries(options.attr)) extraAttrs += ` ${key}="${escapeAttr(val)}"`;
            }
            let innerHtml = '';
            if (options.icon) innerHtml += `<i class="${escapeAttr(options.icon)}"></i>`;
            if (options.label) innerHtml += `<span>${escapeHtml(options.label)}</span>`;
            if (options.helperText) innerHtml += `<small>${escapeHtml(options.helperText)}</small>`;
            if (options.badge) innerHtml += `<div class="ui-button-badge">${escapeHtml(options.badge)}</div>`;
            if (options.extraHtml) innerHtml += options.extraHtml;
            return `<button class="${escapeAttr(classes)}" onclick="${escapeAttr(options.onClick || '')}" ${titleAttr} ${styleAttr} ${extraAttrs}>${innerHtml}</button>`;
        }
    },

    /**
     * The "⋯" overflow menu of a page header. Items: { label, icon, onClick, danger } or '-' for a rule.
     * Primary and neutral header buttons use UI.Button with className 'hdr-btn' / 'hdr-btn primary'.
     */
    MoreMenu: {
        render: function(items) {
            const rows = items.map(it => it === '-' ? '<hr>' : UI.Button.render({
                className: it.danger ? 'danger' : '',
                icon: it.icon,
                label: it.label,
                tooltip: it.tooltip,
                onClick: it.onClick,
            })).join('');
            return `<details class="more-menu"><summary title="More actions"><i class="fa-solid fa-ellipsis"></i></summary>
                <div class="more-menu-list" onclick="this.closest('details').removeAttribute('open')">${rows}</div></details>`;
        },
    },

    /**
     * Collection tab strip: one header for every collection-scoped page.
     */
    EntityTabs: {
        counts: {},
        tabs: [
            { key: 'overview', label: 'Overview', path: '' },
            { key: 'batches', label: 'Batches', path: '/batches', count: 'total_batches' },
            { key: 'files', label: 'Files', path: '/files', count: 'total_files' },
            { key: 'binary-similarity', count: 'file_sims', label: 'Similarities', path: '/files/similarities' },
            { key: 'bin-clusters', count: 'file_clusters', label: 'Clusters', path: '/files/clusters' },
            { key: 'functions', label: 'Functions', path: '/functions', count: 'total_functions', gap: true },
            { key: 'features-global', label: 'Features', path: '/features', count: 'features' },
            { key: 'function-similarity', count: 'function_sims', label: 'Func sims', path: '/functions/similarities' },
            { key: 'clusters', count: 'function_clusters', label: 'Func clusters', path: '/functions/clusters' },
            { key: 'tags', label: 'Tags', path: '/tags', hide: true },
            { key: 'jobs', label: 'Jobs', path: '/jobs', hide: true },
            { key: 'upload', label: 'Upload', path: '/upload', push: true },
        ],
        // Detail pages light up the tab of the list they belong to.
        parent: {
            'collection-detail': 'overview', 'pool-detail': 'overview', dashboard: 'files', file: 'files', function: 'functions', call_graph: 'functions',
            function_features: 'functions', diff: 'function-similarity', bin_sim: 'binary-similarity',
            feature: 'features-global', 'cluster-detail': 'clusters', 'bin-cluster-detail': 'bin-clusters',
        },

        render: function() {
            const el = document.getElementById('entity-tabs');
            if (!el) return;
            const { viewKey, collection, pool } = getRoutingState();
            const active = this.parent[viewKey] || viewKey;
            const id = pool || collection;
            this.paths = [];
            document.body.classList.remove('has-entity-tabs');
            if (!id || !this.tabs.some(t => t.key === active)) {
                el.innerHTML = '';
                return;
            }
            const key = (pool ? 'pool:' : 'coll:') + id;
            this.load(key, id, !!pool);
            const base = `/${pool ? 'pools' : 'collections'}/${encodeURIComponent(id)}`;
            const c = this.counts[key] || {};
            const link = t => {
                const n = t.count && c[t.count] != null ? `<span class="entity-tab-count">${escapeHtml(Number(c[t.count]).toLocaleString())}</span>` : '';
                return `<a class="entity-tab${t.key === active ? ' current' : ''}${t.gap ? ' gap' : ''}" href="${escapeAttr(base + t.path + this.savedQuery(collection, t.key))}" onclick="UI.EntityTabs.go(this, event, ${escapeAttr(jsString(t.key))})">${escapeHtml(t.label)}${n}</a>`;
            };
            // Crumbs and the page title skip what the strip already shows.
            const shown = this.tabs.filter(t => !t.hide && t.path);
            this.paths = shown.map(t => base + t.path);
            document.body.classList.toggle('has-entity-tabs', shown.some(t => t.key === active));
            // Tabs scroll on narrow screens; the pinned links (Upload) stay in view beside them.
            el.innerHTML = `<div class="entity-tabs-scroll">${this.tabs.filter(t => !t.hide && !t.push).map(link).join('')}</div>${this.tabs.filter(t => t.push).map(link).join('')}`;
        },

        // Same behaviour the sidebar links had: reopen a view with its last filters, and a click on the current tab clears them.
        savedQuery: function(collection, view) {
            if (view === 'jobs') return '';
            let saved = null;
            try { saved = localStorage.getItem(`savedFilters:${collection}:${view}`); } catch (e) { /* storage blocked */ }
            if (!saved) return '';
            const q = new URLSearchParams(saved);
            q.delete('collection');
            q.delete('pool');
            return q.toString() ? `?${q}` : '';
        },

        go: function(a, e, view) {
            if (e.ctrlKey || e.metaKey) return;
            e.preventDefault();
            if (getRoutingState().viewKey === view && typeof clearFilters === 'function') clearFilters();
            else Nav.openPath(a.href, e);
        },

        // Counts are cheap cardinality reads; refetch at most every 15s and repaint.
        load: function(key, id, isPool) {
            const old = this.counts[key];
            if (old && Date.now() - old.__ts < 15000) return;
            this.counts[key] = { ...old, __ts: Date.now() };
            const q = encodeURIComponent(id);
            const get = (url, pick) => fetch(url).then(r => r.ok ? r.json() : {}).then(pick).catch(() => ({}));
            const fetched = isPool
                ? get(`/api/pool/${q}`, d => ({
                    total_files: d.total_files, total_functions: d.total_functions,
                    function_sims: d.total_func_similarities, file_sims: d.total_file_similarities,
                    function_clusters: d.total_func_clusters, file_clusters: d.total_file_clusters,
                }))
                : Promise.all([
                    get(`/api/collection/search?q=${q}`, d => (d.collections || []).find(x => x.name === id) || {}),
                    get(`/api/collection/counts?collection=${q}`, d => d),
                ]).then(([coll, extra]) => ({ ...coll, ...extra }));
            fetched.then(c => {
                this.counts[key] = { ...c, __ts: Date.now() };
                this.render();
            });
        },
    },

    /**
     * Sidebar Component
     */
    Sidebar: {
        render: function() {
            const routing = getRoutingState();
            const { viewKey, pool } = routing;
            // No collection in the URL: fall back to the last one visited rather than breaking the whole navbar.
            let collection = routing.collection;
            if (!collection && !pool) {
                try { collection = localStorage.getItem('lastCollectionContext') || null; } catch (e) { /* storage blocked */ }
            }

            const buildNavUrl = (view) => {
                if (view === 'home') return '/';
                if (view === 'collections') return '/collections';
                if (view === 'pools') return '/pools';
                if (!collection && !pool && !['jobs', 'nway', 'scan'].includes(view)) return '/collections';
                if (view === 'nway' || view === 'scan') {
                    const k = pool ? `pool=${encodeURIComponent(pool)}` : collection ? `collection=${encodeURIComponent(collection)}` : '';
                    return `${view === 'nway' ? '/diff/nway' : '/scans'}${k ? `?${k}` : ''}`;
                }

                const prefix = window.location.pathname.startsWith('/pool/') ? 'pool' : 'pools';
                if (pool) {
                    if (view === 'jobs') {
                        return collection ? `/${prefix}/${encodeURIComponent(pool)}/collections/${encodeURIComponent(collection)}/jobs` : `/${prefix}/${encodeURIComponent(pool)}/jobs`;
                    }
                    if (view === 'upload') return `/${prefix}/${encodeURIComponent(pool)}/upload`;
                    if (view === 'batches') return `/${prefix}/${encodeURIComponent(pool)}/batches`;
                    if (view === 'files') return `/${prefix}/${encodeURIComponent(pool)}/files`;
                    if (view === 'functions') return `/${prefix}/${encodeURIComponent(pool)}/functions`;
                    if (view === 'features-global') return `/${prefix}/${encodeURIComponent(pool)}/features`;
                    if (view === 'function-similarity') return `/${prefix}/${encodeURIComponent(pool)}/functions/similarities`;
                    if (view === 'binary-similarity') return `/${prefix}/${encodeURIComponent(pool)}/files/similarities`;
                    if (view === 'clusters') return `/${prefix}/${encodeURIComponent(pool)}/functions/clusters`;
                    if (view === 'bin-clusters') return `/${prefix}/${encodeURIComponent(pool)}/files/clusters`;
                    return `/${prefix}/${encodeURIComponent(pool)}/${view}`;
                }

                if (view === 'jobs') {
                    return collection ? `/collections/${encodeURIComponent(collection)}/jobs` : `/jobs`;
                }
                if (view === 'upload') return `/collections/${encodeURIComponent(collection)}/upload`;
                if (view === 'batches') return `/collections/${encodeURIComponent(collection)}/batches`;
                if (view === 'files') return `/collections/${encodeURIComponent(collection)}/files`;
                if (view === 'functions') return `/collections/${encodeURIComponent(collection)}/functions`;
                if (view === 'features-global') return `/collections/${encodeURIComponent(collection)}/features`;
                if (view === 'function-similarity') return `/collections/${encodeURIComponent(collection)}/functions/similarities`;
                if (view === 'binary-similarity') return `/collections/${encodeURIComponent(collection)}/files/similarities`;
                if (view === 'clusters') return `/collections/${encodeURIComponent(collection)}/functions/clusters`;
                if (view === 'bin-clusters') return `/collections/${encodeURIComponent(collection)}/files/clusters`;
                return `/collections/${encodeURIComponent(collection)}/${view}`;
            };

            const isActive = (view) => {
                if (viewKey === view) return 'active';
                if (viewKey === 'dashboard' && view === 'files') return 'active';
                
                if (view === 'files' && viewKey === 'file') return 'active';
                if (view === 'functions' && (viewKey === 'function' || viewKey === 'call_graph' || viewKey === 'function_features')) return 'active';
                if (view === 'function-similarity' && viewKey === 'diff') return 'active';
                if (view === 'binary-similarity' && viewKey === 'bin_sim') return 'active';
                if (view === 'features-global' && viewKey === 'feature') return 'active';
                if (view === 'collections' && viewKey === 'collection-detail') return 'active';
                if (view === 'pools' && viewKey === 'pool-detail') return 'active';
                if (view === 'search' && viewKey === 'search-detail') return 'active';
                if (view === 'nway' && viewKey === 'nway') return 'active';
                if (view === 'scan' && viewKey === 'scan-detail') return 'active';

                return '';
            };

            return `
                <button id="sidebar-toggle" onclick="toggleSidebar()" title="Toggle Sidebar">⟨</button>
                <nav class="sidebar-nav">
                    <h2 title="BSimVis">
                        <a href="/" id="brand-link" onclick="Nav.openPath('/', event)">
                            <img src="/logo.svg" alt="Logo">
                            <span>BSimVis</span>
                        </a>
                    </h2>
                    <div class="nav-section-content">
                        <a href="/" id="nav-home" title="Home" class="${isActive('home')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-house"></i> <span>Home</span></a>
                        <a href="#" id="nav-search" title="Search everything (Ctrl+K)" onclick="event.preventDefault(); SearchPalette.show();"><i class="fa-solid fa-magnifying-glass"></i> <span>Search</span></a>
                        <a href="${buildNavUrl('collections')}" id="nav-collections" title="Collections" class="${isActive('collections')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-layer-group"></i> <span>Collections</span></a>
                        <a href="${buildNavUrl('pools')}" id="nav-pools" title="Pools" class="${isActive('pools')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-diagram-project"></i> <span>Pools</span></a>
                    </div>
                    <div class="nav-section-content">
                        <a href="${buildNavUrl('scan')}" id="nav-scans" title="Scan a file without ingesting it" class="${isActive('scan')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-microscope"></i> <span>Scan</span></a>
                        <a href="${buildNavUrl('nway')}" id="nav-nway" title="N-way file diff (experimental)" class="${isActive('nway')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-table-columns"></i> <span>N-way diff</span></a>
                        <a href="/searches" id="nav-searches" title="Saved Searches" class="${isActive('search')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-list-check"></i> <span>Searches</span></a>
                        <a href="${buildNavUrl('jobs')}" id="nav-jobs" title="Jobs" class="${isActive('jobs')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-server"></i> <span>Jobs</span></a>
                    </div>
                    <div class="nav-section-content nav-bottom">
                        <a href="/api/" target="_blank" id="nav-api" title="API"><i class="fa-solid fa-book"></i> <span>API</span></a>
                    </div>
                </nav>
            `;
        },

        init: function(containerId = 'sidebar-container') {
            if (window.parent && window.parent !== window) document.body.classList.add('in-iframe');
            const container = document.getElementById(containerId);
            if (!container) return;
            container.innerHTML = this.render();
        },

        updateActiveState: function() {
            const container = document.getElementById('sidebar-container');
            if (container) container.innerHTML = this.render();
        }
    },

    /**
     * Breadcrumb Component
     * @param {Array<{label: string, icon?: string, href?: string, onClick?: string}>} items
     * Last item is treated as the current (non-clickable) page.
     */
    Breadcrumb: {
        render: function(items = []) {
            if (!items.length) return '';
            const crumbs = items.map((item, i) => {
                const isLast = i === items.length - 1;
                const icon = item.icon ? `<i class="${escapeAttr(item.icon)}"></i>` : '';
                if (isLast) {
                    return `<span class="breadcrumb-item current">${icon}${escapeHtml(item.label)}</span>`;
                }
                const clickAttr = item.onClick ? `onclick="${escapeAttr(item.onClick)}"` : '';
                const hrefAttr = item.href ? `href="${escapeAttr(item.href)}"` : 'href="#"';
                return `<a class="breadcrumb-item" ${hrefAttr} ${clickAttr}>${icon}${escapeHtml(item.label)}</a>`;
            }).join('<span class="breadcrumb-sep"><i class="fa-solid fa-chevron-right"></i></span>');
            return `<nav class="breadcrumb" aria-label="breadcrumb">${crumbs}</nav>`;
        }
    }
};

// Close any open header overflow menu on an outside click.
document.addEventListener("click", e => document.querySelectorAll("details.more-menu[open]").forEach(d => d.contains(e.target) || d.removeAttribute("open")));
