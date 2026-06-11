/**
 * Call Graph View Module
 * Extracted from call_graph/index.html
 */

window.CallGraphView = {
    container: null,
    params: null,

    async init(params, containerId) {
        this.params = params;
        this.container = document.getElementById(containerId);
        
        const collection = params.collection || 'main';
        const file_md5 = params.md5 || params.file_md5;

        if (!file_md5) {
            this.container.innerHTML = '<div style="padding:20px; color:#f92672;">Error: No file MD5 provided for call graph.</div>';
            return;
        }

        this.container.innerHTML = `
            <div id="cg-loader" style="text-align:center; padding:50px; color:var(--dim); font-size:1.2rem;">
                <i class="fa-solid fa-spinner fa-spin"></i> Initializing Call Graph...
            </div>
            <div id="cg-container" style="display:none; flex:1; overflow:hidden; position:relative;">
                <div id="cg-canvas-container" style="width:100%; height:100%;"></div>
                <div id="cg-controls" style="position:absolute; top:20px; left:20px; display:flex; gap:10px; z-index:10;">
                    <button class="btn-action" onclick="CallGraphView.resetZoom()"><i class="fa-solid fa-compress"></i> Reset Zoom</button>
                    <button class="btn-action" onclick="CallGraphView.toggleLabels()"><i class="fa-solid fa-tag"></i> Toggle Labels</button>
                </div>
            </div>
        `;

        try {
            const res = await fetch(`/api/file/call_graph?collection=${encodeURIComponent(collection)}&file_md5=${encodeURIComponent(file_md5)}`);
            if (!res.ok) throw new Error("Call graph data not found");
            const data = await res.json();

            document.getElementById('cg-loader').style.display = 'none';
            document.getElementById('cg-container').style.display = 'flex';

            if (window.initCallGraph) {
                window.initCallGraph(data, 'cg-canvas-container');
            } else {
                throw new Error("initCallGraph function not found in call_graph.js");
            }

        } catch (err) {
            console.error(err);
            document.getElementById('cg-loader').innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#f92672;"></i> ${err.message}`;
        }
    },

    resetZoom() {
        if (window.resetCallGraphZoom) window.resetCallGraphZoom();
    },

    toggleLabels() {
        if (window.toggleCallGraphLabels) window.toggleCallGraphLabels();
    },

    destroy() {
        // Any D3 cleanup if necessary
        this.container = null;
        this.params = null;
    }
};
