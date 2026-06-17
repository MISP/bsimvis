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
        } else if (mapped[0] === 'function_features' && mapped[1] && mapped[2]) {
            mapped = ['files', mapped[1], 'functions', mapped[2], 'features'];
        } else if (mapped[0] === 'feature' && mapped[1]) {
            mapped = ['features', mapped[1]];
        }

        let url;
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        const prefix = window.location.pathname.startsWith('/pool/') ? 'pool' : 'pools';
        const rawCollection = stripPoolPrefix(collection);

        if (pool) {
            const isTopLevelPoolView = 
                (mapped.length === 1 && (mapped[0] === 'files' || mapped[0] === 'functions')) ||
                (mapped.length === 2 && (mapped[0] === 'files' || mapped[0] === 'functions') && mapped[1] === 'similarities');
            if (rawCollection && !isTopLevelPoolView) {
                url = `/${prefix}/${encodeURIComponent(pool)}/collections/${encodeURIComponent(rawCollection)}`;
            } else {
                url = `/${prefix}/${encodeURIComponent(pool)}`;
            }
        } else {
            url = `/collections/${encodeURIComponent(rawCollection)}`;
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

        const targetPath = path;

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
    }
};

// Handle browser back/forward buttons
window.addEventListener('popstate', () => {
    if (typeof window.refreshData === 'function') {
        window.refreshData();
    }
});
