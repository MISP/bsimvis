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
        } else if (mapped[0] === 'search' && mapped[1] === 'function-similarity') {
            mapped = ['functions', 'similarities'];
        } else if (mapped[0] === 'search' && mapped[1] === 'binary-similarity') {
            mapped = ['files', 'similarities'];
        } else if (mapped[0] === 'search' && mapped[1] === 'clusters') {
            mapped = ['functions', 'clusters'];
        } else if (mapped[0] === 'search' && mapped[1] === 'bin-clusters') {
            mapped = ['files', 'clusters'];
        } else if (mapped[0] === 'search') {
            mapped = [mapped[1]];
        }
 else if (mapped[0] === 'call_graph' && mapped[1]) {
            mapped = ['files', mapped[1], 'functions'];
        } else if (mapped[0] === 'file' && mapped[1]) {
            mapped = ['files', mapped[1]];
        } else if (mapped[0] === 'function' && mapped[1] && mapped[2]) {
            mapped = ['files', mapped[1], 'functions', mapped[2]];
        } else if (mapped[0] === 'feature' && mapped[1]) {
            mapped = ['features', mapped[1]];
        }

        let url;
        if (collection && collection.startsWith('pool:')) {
            url = `/pools/${encodeURIComponent(collection.substring(5))}`;
        } else {
            url = `/collections/${encodeURIComponent(collection)}`;
        }
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
        const isCollectionPath = typeof path === 'string' && (path.startsWith('/collection/') || path.startsWith('/collections/'));
        const isPoolPath = typeof path === 'string' && (path.startsWith('/pool/') || path.startsWith('/pools/'));
        if (isCollectionPath || isPoolPath) {
            // Strip fragment (#...) before splitting so # is not encoded as %23 in path segments
            const hashIdx = path.indexOf('#');
            const fragment = hashIdx !== -1 ? path.slice(hashIdx) : '';
            const pathNoFrag = hashIdx !== -1 ? path.slice(0, hashIdx) : path;
            const pathParts = pathNoFrag.split('?')[0].split('/').filter(Boolean);
            const query = pathNoFrag.split('?')[1] ? ('?' + pathNoFrag.split('?')[1]) : '';
            let coll = pathParts[1];
            if (isPoolPath && coll && !coll.startsWith('pool:')) {
                coll = 'pool:' + coll;
            }
            const p2 = pathParts[2];
            
            if (p2 === 'files' || p2 === 'file') {
                if (pathParts[3] === 'similarities') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/similarities${query}`;
                } else if (pathParts[3] === 'clusters') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/clusters${query}`;
                } else if (pathParts[4] === 'functions' || pathParts[4] === 'function') {
                    if (pathParts[6] === 'vs') {
                        // Function diff: /collections/{coll}/files/{md5}/functions/{addr}/vs/{coll_b}/{md5_b}/{addr_b}
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}/vs/${encodeURIComponent(pathParts[7])}/${encodeURIComponent(pathParts[8])}/${encodeURIComponent(pathParts[9])}${query}`;
                    } else if (pathParts[5]) {
                        if (pathParts[6] === 'features') {
                            // Function features: /collections/{coll}/files/{md5}/functions/{addr}/features
                            targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}/features${query}${fragment}`;
                        } else {
                            // Function detail: /collections/{coll}/files/{md5}/functions/{addr}
                            targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[5])}${query}${fragment}`;
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
                if (pathParts[3] === 'similarities') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions/similarities${query}`;
                } else if (pathParts[3] === 'clusters') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions/clusters${query}`;
                } else if (pathParts[5] === 'vs') {
                    // /collection/{coll}/function/{md5_a}/{addr_a}/vs/{coll_b}/{md5_b}/{addr_b}
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}/vs/${encodeURIComponent(pathParts[6])}/${encodeURIComponent(pathParts[7])}/${encodeURIComponent(pathParts[8])}${query}`;
                } else if (pathParts[3] && pathParts[4]) {
                    if (pathParts[5] === 'features') {
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}/features${query}${fragment}`;
                    } else {
                        targetPath = `/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(pathParts[3])}/functions/${encodeURIComponent(pathParts[4])}${query}${fragment}`;
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
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions/clusters${query}`;
                } else if (pathParts[3] === 'bin-clusters') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/clusters${query}`;
                } else if (pathParts[3] === 'binary-similarity') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/files/similarities${query}`;
                } else if (pathParts[3] === 'function-similarity') {
                    targetPath = `/collections/${encodeURIComponent(coll)}/functions/similarities${query}`;
                }
            } else if (p2) {
                targetPath = `/collections/${encodeURIComponent(coll)}/${p2}${query}`;
            } else if (pathParts[0] === 'collection') {
                targetPath = `/collections/${encodeURIComponent(coll)}${query}`;
            }
        }

        if (typeof targetPath === 'string') {
            targetPath = targetPath.replace(/\/collections\/pool(:|%3A)/g, '/pools/');
        }

        // Window Manager integration (only if enabled in UI settings)
        const uiParams = window.UIParams || (window.parent && window.parent.UIParams) || {};
        const useFloating = uiParams.useFloatingWindows !== false; // Default to false

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
