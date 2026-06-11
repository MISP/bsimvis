/**
 * navigation.js
 * Centralized navigation and window management.
 */

window.Nav = {
    /**
     * Constructs a RESTful UI URL.
     * @param {string} collection - The collection name.
     * @param {string[]} pathSegments - Array of path segments.
     * @returns {string} The constructed URL.
     */
    buildUIUrl: function(collection, pathSegments = []) {
        let mapped = [...pathSegments];
        if (mapped[0] === 'search' && mapped[1] === 'files') {
            mapped = ['files'];
        } else if (mapped[0] === 'search' && mapped[1] === 'functions') {
            mapped = ['functions'];
        } else if (mapped[0] === 'search' && mapped[1] === 'features-global') {
            mapped = ['features'];
        } else if (mapped[0] === 'search' && mapped[1] === 'batches') {
            mapped = ['batches'];
        } else if (mapped[0] === 'call_graph' && mapped[1]) {
            mapped = ['files', mapped[1], 'functions'];
        } else if (mapped[0] === 'file' && mapped[1]) {
            mapped = ['files', mapped[1]];
        } else if (mapped[0] === 'function' && mapped[1] && mapped[2]) {
            mapped = ['files', mapped[1], 'functions', mapped[2]];
        } else if (mapped[0] === 'feature' && mapped[1]) {
            mapped = ['features', mapped[1]];
        }

        let url = `/collections/${encodeURIComponent(collection)}`;
        if (mapped.length > 0) {
            url += '/' + mapped.map(encodeURIComponent).join('/');
        }
        return url;
    },

    /**
     * Opens a path (URL or hash) based on the user's interaction (e.g., Ctrl+click).
     * @param {string} path - The URL or hash to open.
     * @param {MouseEvent|KeyboardEvent} [event] - The event that triggered the navigation.
     * @param {Object} [options] - Additional options.
     * @param {string} [options.title] - Window title for windowManager.
     * @param {string} [options.type] - Window type for windowManager (e.g., 'diff').
     */
    openPath: function(path, event, options = {}) {
        if (window.parent && window.parent !== window && window.parent.Nav && window.parent.Nav.openPath) {
            window.parent.Nav.openPath(path, event, options);
            return;
        }

        if (event) {
            if (typeof event.stopPropagation === 'function') {
                event.stopPropagation();
            }
        }

        // Standard logic for Ctrl/Meta + Click: Open in new tab
        const ctrlKey = event && (event.ctrlKey || event.metaKey);
        if (ctrlKey) {
            window.open(path, '_blank');
            return;
        }

        // Normalize URL on the fly to support the new plural hierarchical RESTful structure
        let targetPath = path;
        if (typeof path === 'string' && (path.startsWith('/collection/') || path.startsWith('/collections/'))) {
            const pathParts = path.split('?')[0].split('/').filter(Boolean);
            const query = path.split('?')[1] ? ('?' + path.split('?')[1]) : '';
            const coll = pathParts[1];
            const p2 = pathParts[2];
            
            if (p2 === 'files' || p2 === 'file') {
                if (pathParts[4] === 'functions' || pathParts[4] === 'function') {
                    if (pathParts[6] === 'vs') {
                        // Function diff: /collections/{coll}/files/{md5}/functions/{addr}/vs/{coll_b}/{md5_b}/{addr_b}
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}/vs/${encodeURIComponent(pathParts[7])}/${encodeURIComponent(pathParts[8])}/${encodeURIComponent(pathParts[9])}${query}`;
                    } else if (pathParts[5]) {
                        if (pathParts[6] === 'features') {
                            // Function features: /collections/{coll}/files/{md5}/functions/{addr}/features
                            targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}/features${query}`;
                        } else {
                            // Function detail: /collections/{coll}/files/{md5}/functions/{addr}
                            targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}${query}`;
                        }
                    } else {
                        // Call graph: /collections/{coll}/files/{md5}/functions
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${query}`;
                    }
                } else if (pathParts[4] === 'vs') {
                    // File diff: /collections/{coll}/files/{md5}/vs/{coll_b}/{md5_b}
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/vs/${encodeURIComponent(pathParts[5])}/${encodeURIComponent(pathParts[6])}${query}`;
                } else if (pathParts[3]) {
                    // File details: /collections/{coll}/files/{md5}
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}${query}`;
                } else {
                    // Files search: /collections/{coll}/files
                    targetPath = `/collections/${encodeURIComponent(coll)}/files${query}`;
                }
            } else if (p2 === 'function' || p2 === 'functions') {
                if (pathParts[5] === 'vs') {
                    // /collection/{coll}/function/{md5_a}/{addr_a}/vs/{coll_b}/{md5_b}/{addr_b}
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}/vs/${encodeURIComponent(pathParts[6])}/${encodeURIComponent(pathParts[7])}/${encodeURIComponent(pathParts[8])}${query}`;
                } else if (pathParts[3] && pathParts[4]) {
                    if (pathParts[5] === 'features') {
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}/features${query}`;
                    } else {
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}${query}`;
                    }
                } else if (pathParts[3]) {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${query}`;
                } else {
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions${query}`;
                }
            } else if (p2 === 'call_graph' && pathParts[3]) {
                targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${query}`;
            } else if (p2 === 'feature' || p2 === 'features') {
                if (pathParts[3]) {
                    targetPath = `/collections/${encodeURIComponent(coll)}/features/${encodeURIComponent(pathParts[3])}${query}`;
                } else {
                    targetPath = `/collections/${encodeURIComponent(coll)}/features${query}`;
                }
            } else if (p2 === 'search') {
                if (pathParts[3] === 'files') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files${query}`;
                } else if (pathParts[3] === 'functions') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions${query}`;
                } else if (pathParts[3] === 'features-global') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/features${query}`;
                } else if (pathParts[3] === 'batches') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/batches${query}`;
                } else if (pathParts[3] === 'clusters') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/search/clusters${query}`;
                } else if (pathParts[3] === 'bin-clusters') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/search/bin-clusters${query}`;
                } else if (pathParts[3] === 'binary-similarity') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/search/binary-similarity${query}`;
                } else if (pathParts[3] === 'function-similarity') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/search/function-similarity${query}`;
                }
            } else if (p2) {
                targetPath = `/collections/${encodeURIComponent(coll)}/${p2}${query}`;
            } else if (pathParts[0] === 'collection') {
                targetPath = `/collections/${encodeURIComponent(coll)}${query}`;
            }
        }

        // Window Manager integration (only if enabled in UI settings)
        const uiParams = window.UIParams || (window.parent && window.parent.UIParams) || {};
        const useFloating = uiParams.useFloatingWindows !== false; // Default to true

        if (useFloating) {
            if (typeof windowManager !== 'undefined' && options.title) {
                windowManager.createWindow(options.title, targetPath, options);
                if (event && typeof event.preventDefault === 'function') {
                    event.preventDefault();
                }
                return;
            }
        }

        // Default SPA navigation
        if (event && typeof event.preventDefault === 'function') {
            event.preventDefault();
        }

        // Handle path navigation
        if (targetPath.startsWith('#')) {
            window.location.hash = targetPath;
        } else {
            const isInternal = targetPath.startsWith('/') || targetPath.startsWith(window.location.origin);
            
            if (isInternal && typeof window.refreshData === 'function') {
                history.pushState(null, '', targetPath);
                window.refreshData();
            } else {
                window.location.href = targetPath;
            }
        }
    },
};

// Handle browser back/forward buttons
window.addEventListener('popstate', () => {
    if (typeof window.refreshData === 'function') {
        window.refreshData();
    }
});
