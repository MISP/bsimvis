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
        let url = `/collection/${encodeURIComponent(collection)}`;
        if (pathSegments.length > 0) {
            url += '/' + pathSegments.map(encodeURIComponent).join('/');
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

        const ctrlKey = event && (event.ctrlKey || event.metaKey);

        // Standard logic for Ctrl/Meta + Click: Open in new tab
        if (ctrlKey) {
            window.open(path, '_blank');
            return;
        }

        // Window Manager integration (only if enabled in UI settings)
        const uiParams = window.UIParams || (window.parent && window.parent.UIParams) || {};
        const useFloating = uiParams.useFloatingWindows !== false; // Default to true

        if (useFloating) {
            if (typeof windowManager !== 'undefined' && options.title) {
                // If it's a relative path starting with /static/, we can often strip that
                // Or if it's a SPA path, we might want to load it in the iframe
                windowManager.createWindow(options.title, path, options);
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
        if (path.startsWith('#')) {
            // Legacy hash navigation
            window.location.hash = path;
        } else {
            // Check if it's an internal path (relative or same origin)
            const isInternal = path.startsWith('/') || path.startsWith(window.location.origin);
            
            if (isInternal && typeof window.refreshData === 'function') {
                history.pushState(null, '', path);
                window.refreshData();
            } else {
                window.location.href = path;
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
