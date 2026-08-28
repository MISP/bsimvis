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
     * Sidebar Component
     */
    Sidebar: {
        render: function() {
            const { viewKey, collection, pool } = getRoutingState();
            if (!collection && !pool && viewKey !== 'home' && viewKey !== 'collections' && viewKey !== 'pools' && viewKey !== 'jobs') {
                throw new Error("Navigation error: collection context is missing.");
            }

            const buildNavUrl = (view) => {
                if (view === 'home') return '/';
                if (view === 'collections') return '/collections';
                if (view === 'pools') return '/pools';

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
                    <div class="nav-section-title"><span>Binaries</span></div>
                    <div class="nav-section-content">
                        <a href="${buildNavUrl('batches')}" id="nav-batches" title="Batches" class="${isActive('batches')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-boxes-stacked"></i> <span>Batches</span></a>
                        <a href="${buildNavUrl('files')}" id="nav-files" title="Files" class="${isActive('files')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-file-code"></i> <span>Files</span></a>
                        <a href="${buildNavUrl('binary-similarity')}" id="nav-binary-similarity" title="Similarities" class="${isActive('binary-similarity')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-code-compare"></i> <span>Similarities</span></a>
                        <a href="${buildNavUrl('bin-clusters')}" id="nav-bin-clusters" title="Clusters" class="${isActive('bin-clusters')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-bullseye"></i> <span>Clusters</span></a>
                    </div>
                    <div class="nav-section-title"><span>Functions</span></div>
                    <div class="nav-section-content">
                        <a href="${buildNavUrl('functions')}" id="nav-functions" title="Functions" class="${isActive('functions')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-code"></i> <span>Functions</span></a>
                        <a href="${buildNavUrl('function-similarity')}" id="nav-function-similarity" title="Function Similarities" class="${isActive('function-similarity')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-code-compare"></i> <span>Similarities</span></a>
                        <a href="${buildNavUrl('clusters')}" id="nav-clusters" title="Clusters" class="${isActive('clusters')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-bullseye"></i> <span>Clusters</span></a>
                        <a href="${buildNavUrl('features-global')}" id="nav-features-global" title="Features" class="${isActive('features-global')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-fingerprint"></i> <span>Features</span></a>
                    </div>
                    <div style="margin-top:auto; padding-top:20px; border-top: 1px solid var(--border);">
                        ${collection ? `<a href="${buildNavUrl('tags')}" id="nav-tags" title="Tags" class="${isActive('tags')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-tags"></i> <span>Tags</span></a>` : ''}
                        <a href="${buildNavUrl('upload')}" id="nav-upload" title="Upload" class="${isActive('upload')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-cloud-arrow-up"></i> <span>Upload</span></a>
                        <a href="${buildNavUrl('jobs')}" id="nav-jobs" title="Jobs" class="${isActive('jobs')}" onclick="Nav.openPath(this.href, event)"><i class="fa-solid fa-server"></i> <span>Jobs</span></a>
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
