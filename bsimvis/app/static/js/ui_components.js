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
            const titleAttr = options.tooltip ? `title="${options.tooltip}"` : '';
            const styleAttr = options.style ? `style="${options.style}"` : '';
            let extraAttrs = '';
            if (options.attr) {
                for (const [key, val] of Object.entries(options.attr)) extraAttrs += ` ${key}="${val}"`;
            }
            let innerHtml = '';
            if (options.icon) innerHtml += `<i class="${options.icon}"></i>`;
            if (options.label) innerHtml += `<span>${options.label}</span>`;
            if (options.helperText) innerHtml += `<small>${options.helperText}</small>`;
            if (options.badge) innerHtml += `<div class="ui-button-badge">${options.badge}</div>`;
            if (options.extraHtml) innerHtml += options.extraHtml;
            return `<button class="${classes}" onclick="${options.onClick || ''}" ${titleAttr} ${styleAttr} ${extraAttrs}>${innerHtml}</button>`;
        }
    },

    /**
     * Sidebar Component
     */
    Sidebar: {
        render: function() {
            const { viewKey, collection } = getRoutingState();

            const buildNavUrl = (view) => {
                if (view === 'collections') return '/collections';
                if (view === 'jobs') return `/jobs?collection=${encodeURIComponent(collection)}`;
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
                const icon = item.icon ? `<i class="${item.icon}"></i>` : '';
                if (isLast) {
                    return `<span class="breadcrumb-item current">${icon}${item.label}</span>`;
                }
                const clickAttr = item.onClick ? `onclick="${item.onClick}"` : '';
                const hrefAttr = item.href ? `href="${item.href}"` : 'href="#"';
                return `<a class="breadcrumb-item" ${hrefAttr} ${clickAttr}>${icon}${item.label}</a>`;
            }).join('<span class="breadcrumb-sep"><i class="fa-solid fa-chevron-right"></i></span>');
            return `<nav class="breadcrumb" aria-label="breadcrumb">${crumbs}</nav>`;
        }
    }
};
