/**
 * d3_layouts.js
 * Base classes and shared logic for D3 visualizations.
 */

class D3BaseLayout {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.width = this.container ? this.container.clientWidth : 800;
        this.height = this.container ? (this.container.clientHeight || 700) : 700;
        this.root = null;
        this.svg = null;
        this.g = null;
        this.zoom = null;
        this.abortController = null;
    }

    stop() {
        if (this.abortController) this.abortController.abort();
    }

    initSvg(background = "var(--window-bg)") {
        if (!this.container) return;
        d3.select(this.container).selectAll("svg").remove();
        this.svg = d3.select(this.container).append("svg")
            .attr("viewBox", `0 0 ${this.width} ${this.height}`)
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("style", `background:${background}; cursor:grab;`);
        this.g = this.svg.append("g");
        this.zoom = d3.zoom()
            .scaleExtent([0.05, 10])
            .on("zoom", (event) => {
                this.g.attr("transform", event.transform);
            });
        this.svg.call(this.zoom);
    }
}

window.D3BaseLayout = D3BaseLayout;
