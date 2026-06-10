/**
 * ui_components.js
 * Centralized UI components and managers.
 */

window.UI = {
    /**
     * Tooltip Manager
     */
    Tooltip: {
        /**
         * Standardizes tooltip showing logic.
         * @param {HTMLElement} el - The tooltip element.
         * @param {MouseEvent} event - The mouse event.
         * @param {string|Function} content - HTML content or a function that returns it.
         */
        show: function(el, event, content) {
            if (!el) return;
            
            if (typeof content === 'function') {
                content = content();
            }
            
            el.innerHTML = content;
            el.style.display = 'block';
            this.position(el, event);
        },

        /**
         * Positions a tooltip relative to the mouse, with boundary checking.
         */
        position: function(el, event) {
            if (!el || el.style.display === 'none') return;

            const x = event.clientX + 15;
            const y = event.clientY + 15;

            el.style.left = x + 'px';
            el.style.top = y + 'px';

            const rect = el.getBoundingClientRect();
            if (rect.right > window.innerWidth) {
                el.style.left = (event.clientX - rect.width - 15) + 'px';
            }
            if (rect.bottom > window.innerHeight) {
                el.style.top = (event.clientY - rect.height - 15) + 'px';
            }
        },

        hide: function(el) {
            if (el) el.style.display = 'none';
        }
    },

    /**
     * Extensible Button Component
     */
    Button: {
        /**
         * Creates an HTML string for a button.
         * @param {Object} options - Button options.
         * @param {string} [options.label] - Text label.
         * @param {string} [options.icon] - FontAwesome class (e.g., 'fa-solid fa-code').
         * @param {string} [options.helperText] - Small text next to icon/label.
         * @param {string} [options.onClick] - Inline onClick handler string.
         * @param {string} [options.tooltip] - Title attribute for tooltip.
         * @param {string} [options.className] - Additional CSS classes.
         * @param {string} [options.style] - Inline style string.
         * @param {Object} [options.attr] - Additional attributes.
         */
        render: function(options = {}) {
            const classes = ['ui-button', options.className || ''].join(' ').trim();
            const titleAttr = options.tooltip ? `title="${options.tooltip}"` : '';
            const styleAttr = options.style ? `style="${options.style}"` : '';
            
            let extraAttrs = '';
            if (options.attr) {
                for (const [key, val] of Object.entries(options.attr)) {
                    extraAttrs += ` ${key}="${val}"`;
                }
            }

            let innerHtml = '';
            if (options.icon) {
                innerHtml += `<i class="${options.icon}"></i>`;
            }
            if (options.label) {
                innerHtml += `<span>${options.label}</span>`;
            }
            if (options.helperText) {
                innerHtml += `<small>${options.helperText}</small>`;
            }
            if (options.badge) {
                innerHtml += `<div class="ui-button-badge">${options.badge}</div>`;
            }
            if (options.extraHtml) {
                innerHtml += options.extraHtml;
            }

            return `<button class="${classes}" onclick="${options.onClick || ''}" ${titleAttr} ${styleAttr} ${extraAttrs}>${innerHtml}</button>`;
        }
    },

    /**
     * Sidebar Component
     */
    Sidebar: {
        render: function() {
            // Get current path to determine active state
            const currentPath = window.location.pathname;
            const pathParts = currentPath.split('/').filter(Boolean);
            
            // Helper to build links
            const buildNavUrl = (view) => {
                if (view === 'collections') return '/collections';
                if (view === 'jobs') return '/jobs';

                // Get current collection from routing state
                const currentCollection = (window.getRoutingState && window.getRoutingState().collection) || 'main';

                // If we have a collection context that is not 'main', use it
                if (currentCollection && currentCollection !== 'main') {
                    if (view === 'upload') return `/collection/${encodeURIComponent(currentCollection)}/upload`;
                    return Nav.buildUIUrl(currentCollection, ['search', view]);
                }

                if (view === 'upload') return '/upload';
                return `/${view}`; // Fallback to RESTful path instead of hash
            };
            
            const isActive = (view) => {
                if (view === 'collections' && (currentPath === '/collections' || currentPath === '/')) return 'active';
                if (view === 'jobs' && currentPath === '/jobs') return 'active';
                if (view === 'upload' && (currentPath === '/upload' || (pathParts[0] === 'collection' && pathParts[2] === 'upload'))) return 'active';
                
                if (pathParts[0] === 'collection' && pathParts[1]) {
                    if (currentPath.includes(`/search/${view}`)) return 'active';
                    if (pathParts[2] === view) return 'active';
                }
                return '';
            };

            return `
                <button id="sidebar-toggle" onclick="toggleSidebar()" title="Toggle Sidebar">⟨</button>
                <nav>
                    <h2 title="BSimVis">
                        <a href="/" id="brand-link">
                            <img src="/logo.svg" alt="Logo">
                            <span>BSimVis</span>
                        </a>
                    </h2>
                    <div class="nav-section-title">
                        <span>Binaries</span>
                    </div>
                    <div class="nav-section-content">
                        <a href="${buildNavUrl('batches')}" id="nav-batches" title="Batches" class="${isActive('batches')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none"><i
                                class="fa-solid fa-boxes-stacked"></i> <span>Batches</span></a>
                        <a href="${buildNavUrl('files')}" id="nav-files" title="Files" class="${isActive('files')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none"><i class="fa-solid fa-file-code"></i>
                            <span>Files</span></a>
                        <a href="${buildNavUrl('binary-similarity')}" id="nav-binary-similarity" title="Similarities" class="${isActive('binary-similarity')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none">
                            <i class="fa-solid fa-code-compare"></i>
                            <span>Similarities</span>
                            <button class="nav-rebuild-btn"
                                onclick="event.preventDefault(); event.stopPropagation(); triggerRebuildAll();"
                                title="Rebuild Analysis">
                                <i class="fa-solid fa-arrows-rotate nav-rebuild-icon"></i>
                            </button>
                        </a>
                        <a href="${buildNavUrl('bin-clusters')}" id="nav-bin-clusters" title="Clusters" class="${isActive('bin-clusters')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none">
                            <i class="fa-solid fa-bullseye"></i>
                            <span>Clusters</span>
                        </a>
                    </div>

                    <div class="nav-section-title">
                        <span>Functions</span>
                    </div>
                    <div class="nav-section-content">
                        <a href="${buildNavUrl('functions')}" id="nav-functions" title="Functions" class="${isActive('functions')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none"><i
                                class="fa-solid fa-code"></i>
                            <span>Functions</span></a>
                        <a href="${buildNavUrl('function-similarity')}" id="nav-function-similarity" title="Function Similarities" class="${isActive('function-similarity')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none"><i class="fa-solid fa-code-compare"></i> <span>Similarities</span></a>
                        <a href="${buildNavUrl('clusters')}" id="nav-clusters" title="Clusters" class="${isActive('clusters')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none">
                            <i class="fa-solid fa-bullseye"></i>
                            <span>Clusters</span>
                            <button class="nav-rebuild-btn"
                                onclick="event.preventDefault(); event.stopPropagation(); triggerRebuildAll();"
                                title="Rebuild Analysis">
                                <i class="fa-solid fa-arrows-rotate nav-rebuild-icon"></i>
                            </button>
                        </a>
                        <a href="${buildNavUrl('features-global')}" id="nav-features-global" title="Features" class="${isActive('features-global')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="display:none"><i
                                class="fa-solid fa-fingerprint"></i> <span>Features</span></a>
                    </div>

                    <div style="margin-top:auto; padding-top:20px; border-top: 1px solid var(--border);">
                        <a href="${buildNavUrl('upload')}" id="nav-upload" title="Upload Binaries" class="${isActive('upload')}"
                            onclick="Nav.openPath(this.href, event); return false;"><i class="fa-solid fa-cloud-arrow-up"></i>
                            <span>Upload</span></a>
                        <a href="${buildNavUrl('jobs')}" id="nav-jobs" title="Background Jobs" class="${isActive('jobs')}"
                            onclick="Nav.openPath(this.href, event); return false;"
                            style="color:var(--text);"><i id="nav-jobs-icon"
                                class="fa-solid fa-server" style="color: var(--subtle);"></i>
                            <div id="nav-jobs-loader" class="nav-job-spinner" style="display:none;"></div> <span>Jobs</span>
                        </a>
                        <!-- History and API links kept as is for now -->
                        <a href="/api/" target="_blank" id="nav-api" title="API Documentation" style="color:var(--text);"><i
                                class="fa-solid fa-book" style="color: var(--subtle);"></i> <span>API</span></a>
                    </div>
                </nav>
            `;
        },

        init: function(containerId = 'sidebar-container') {
            // Detect if running inside an iframe (e.g. WindowManager panel)
            if (window.parent && window.parent !== window) {
                document.body.classList.add('in-iframe');
            }

            const container = document.getElementById(containerId);
            if (!container) return;

            container.innerHTML = this.render();

            // Set collapsed state from localStorage
            if (localStorage.getItem('sidebarCollapsed') === 'true') {
                document.body.classList.add('sidebar-collapsed');
                const btn = document.getElementById('sidebar-toggle');
                if (btn) btn.innerHTML = '⟩';
            }

            // Re-bind toggle function if it exists globally
            window.toggleSidebar = window.toggleSidebar || function() {
                const body = document.body;
                const isCollapsed = body.classList.toggle('sidebar-collapsed');
                localStorage.setItem('sidebarCollapsed', isCollapsed);
                const btn = document.getElementById('sidebar-toggle');
                if (btn) btn.innerHTML = isCollapsed ? '⟩' : '⟨';
                setTimeout(() => window.dispatchEvent(new Event('resize')), 300);
            };

            // Setup history hover if in dashboard context
            if (typeof setupSidebarHistoryHover === 'function') {
                setupSidebarHistoryHover();
            }
        }
    }
};
