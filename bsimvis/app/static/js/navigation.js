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
                windowManager.createWindow(options.title, path, { type: options.type || 'default' });
                return;
            }

            // Fallback for parent window manager (if in iframe)
            if (window.parent && window.parent.windowManager && options.title) {
                window.parent.windowManager.createWindow(options.title, path, { type: options.type || 'default' });
                return;
            }
        }

        // Default navigation
        if (path.startsWith('#')) {
            // Legacy hash support
            window.location.hash = path;
        } else {
            // RESTful path: Check if we are in the main dashboard and should do SPA navigation
            const parts = path.split('?')[0].split('/').filter(Boolean);
            const isCollectionDashboard = parts[0] === 'collection' && (parts.length === 2 || parts.length === 3 || (parts[2] === 'search' && parts.length === 4));
            const isGlobalDashboard = path === '/' || path === '/collections' || path === '/jobs' || path === '/upload';
            
            const isDashboardPath = isGlobalDashboard || isCollectionDashboard;

            if (isDashboardPath && typeof window.refreshData === 'function') {
                history.pushState(null, '', path);
                window.refreshData();
            } else {
                window.location.href = path;
            }
        }
    },
};
