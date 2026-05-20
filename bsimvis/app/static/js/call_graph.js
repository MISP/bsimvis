function loadCallGraphView(params) {
    if (!window.callGraphInstance) {
        window.callGraphInstance = new FileCallGraph('call-graph-view-container');
    }
    window.callGraphInstance.fetch(params);
}

class FileCallGraph {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = document.getElementById(containerId);
        this.svg = null;
        this.simulation = null;
        this.nodes = [];
        this.links = [];
        this.width = this.container.clientWidth;
        this.height = this.container.clientHeight || 600;
        this.abortController = null;
    }

    stop() {
        if (this.abortController) this.abortController.abort();
        if (this.simulation) this.simulation.stop();
    }

    async fetch(params) {
        this.stop();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        const col = params.get('collection');
        const file_md5 = params.get('file_md5');
        if (!col || !file_md5) {
            this.container.innerHTML = '<div style="display:flex; justify-content:center; align-items:center; height:100%; color:var(--subtle); font-size:1.2rem;">Please select a file from the dropdown above to view its call graph.</div>';
            return;
        }

        try {
            const res = await fetch(`/api/file/call_graph?collection=${col}&file_md5=${file_md5}`, { signal });
            const data = await res.json();
            this.nodes = data.nodes;
            this.links = data.edges;
            this.render();
        } catch (e) {
            if (e.name !== 'AbortError') console.error("Call Graph Fetch Error:", e);
        }
    }

    render() {
        // Calculate degrees for orphan detection and outEdges for hierarchy
        const degrees = {};
        const outEdges = {};
        this.nodes.forEach(n => { 
            degrees[n.id] = 0; 
            n.depth = 0; 
            n.inDegree = 0; 
            outEdges[n.id] = []; 
        });

        this.links.forEach(l => {
            const sourceId = typeof l.source === 'object' ? l.source.id : l.source;
            const targetId = typeof l.target === 'object' ? l.target.id : l.target;
            degrees[sourceId]++;
            degrees[targetId]++;
            outEdges[sourceId].push(targetId);
            const targetNode = this.nodes.find(n => n.id === targetId);
            if (targetNode) targetNode.inDegree++;
        });

        // BFS for topological depth
        let queue = this.nodes.filter(n => n.inDegree === 0);
        if (queue.length === 0 && this.nodes.length > 0) queue = [this.nodes[0]];
        const visited = new Set();
        while (queue.length > 0) {
            const curr = queue.shift();
            if (!visited.has(curr.id)) visited.add(curr.id);
            outEdges[curr.id].forEach(targetId => {
                const targetNode = this.nodes.find(n => n.id === targetId);
                if (targetNode && targetNode.depth < curr.depth + 1) {
                    targetNode.depth = curr.depth + 1;
                    if (!visited.has(targetNode.id)) queue.push(targetNode);
                }
            });
        }

        // Clear previous
        this.container.innerHTML = `
            <div style="position:absolute; top:15px; left:15px; z-index:10; background:rgba(0,0,0,0.85); padding:12px; border-radius:6px; border:1px solid #333; pointer-events:none; font-size:0.75rem; color:#eee; backdrop-filter:blur(5px);">
                <div style="font-weight:bold; color:var(--accent); margin-bottom:10px; border-bottom:1px solid #444; padding-bottom:4px; display:flex; justify-content:space-between; align-items:center;">
                    <span>Call Graph Legend</span>
                    <span style="font-size:0.6rem; opacity:0.6; font-weight:normal;">${this.nodes.length} nodes</span>
                </div>
                <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; align-items:center; gap:10px;">
                        <div style="width:12px; height:12px; border-radius:50%; background:#66d9ef; border:1px solid #fff;"></div>
                        <span>Internal (In File)</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:10px;">
                        <div style="width:12px; height:12px; border-radius:50%; background:#fd971f; border:1px solid #fff;"></div>
                        <span>Unindexed / Filtered</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:10px;">
                        <div style="width:12px; height:12px; border-radius:50%; background:#f92672; border:1px solid #fff;"></div>
                        <span>External Function</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:10px; opacity:0.8;">
                        <div style="width:12px; height:12px; border-radius:50%; background:none; border:2px dashed #fff;"></div>
                        <span>Orphan (No Calls)</span>
                    </div>
                </div>
                <div style="margin-top:12px; border-top:1px solid #444; padding-top:8px; color:var(--subtle); font-size:0.65rem; line-height:1.4;">
                    • <b>Drag</b> nodes to move<br>
                    • <b>Hover</b> for code preview<br>
                    • <b>Click</b> to open full code<br>
                    • <b>Scroll</b> to zoom/pan
                </div>
            </div>
            <div style="position:absolute; bottom:15px; right:15px; z-index:10; display:flex; gap:10px;">
                <button id="${this.containerId}-btn-size" title="Toggle Size by Operations" style="background:#1e1e1e; border:1px solid #444; color:var(--accent); padding:6px 12px; border-radius:4px; cursor:pointer; font-size:0.8rem;">Size: Uniform</button>
                <button id="${this.containerId}-btn-physics" title="Toggle Physics Simulation" style="background:#1e1e1e; border:1px solid #444; color:var(--success); padding:6px 12px; border-radius:4px; cursor:pointer; font-size:0.8rem; font-weight:bold;">Physics: ON</button>
                <button id="${this.containerId}-btn-hierarchy" title="Arrange Topologically" style="background:#1e1e1e; border:1px solid #444; color:var(--accent); padding:6px 12px; border-radius:4px; cursor:pointer; font-size:0.8rem;">Hierarchical Layout</button>
            </div>
            <div id="${this.containerId}-svg-root" style="width:100%; height:100%;"></div>
        `;

        this.width = this.container.clientWidth;
        this.height = this.container.clientHeight || 600;

        const svg = d3.select(`#${this.containerId}-svg-root`)
            .append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewBox", [0, 0, this.width, this.height]);

        const g = svg.append("g");

        // Zoom support
        const zoomBehavior = d3.zoom()
            .extent([[0, 0], [this.width, this.height]])
            .scaleExtent([0.1, 8])
            .on("zoom", ({transform}) => {
                g.attr("transform", transform);
            });
        svg.call(zoomBehavior);

        // Arrows
        svg.append("defs").selectAll("marker")
            .data(["end"])
            .join("marker")
            .attr("id", "arrow-end")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 25)
            .attr("refY", 0)
            .attr("markerWidth", 6)
            .attr("markerHeight", 6)
            .attr("orient", "auto")
            .append("path")
            .attr("fill", "#666")
            .attr("d", "M0,-5L10,0L0,5");

        const simulation = d3.forceSimulation(this.nodes)
            .force("link", d3.forceLink(this.links).id(d => d.id).distance(120))
            .force("charge", d3.forceManyBody().strength(-400))
            .force("center", d3.forceCenter(this.width / 2, this.height / 2))
            .force("collision", d3.forceCollide().radius(50));

        this.simulation = simulation;
        this.physicsEnabled = true;

        const link = g.append("g")
            .attr("stroke", "#555")
            .attr("stroke-opacity", 0.6)
            .selectAll("line")
            .data(this.links)
            .join("line")
            .attr("stroke-width", 1.5)
            .attr("marker-end", "url(#arrow-end)");

        this.linkSelection = link;

        const node = g.append("g")
            .selectAll("g")
            .data(this.nodes)
            .join("g")
            .attr("cursor", d => d.is_external ? "default" : "pointer")
            .call(this.drag(simulation));

        this.nodeSelection = node;

        node.append("circle")
            .attr("r", d => d.is_external ? 8 : (d.is_unindexed ? 10 : 13))
            .attr("fill", d => {
                if (d.is_external) return "#f92672";
                if (d.is_unindexed) return "#fd971f";
                return "#66d9ef";
            })
            .attr("stroke", "#fff")
            .attr("stroke-width", d => (degrees[d.id] === 0) ? 0 : 1.5)
            .attr("stroke-dasharray", d => (degrees[d.id] === 0) ? "4,2" : "none")
            .style("stroke", d => (degrees[d.id] === 0) ? "#fff" : "#fff")
            .style("stroke-width", d => (degrees[d.id] === 0) ? 3 : 1.5);

        // For orphans, we add a dashed outer ring
        node.filter(d => degrees[d.id] === 0)
            .append("circle")
            .attr("r", 17)
            .attr("fill", "none")
            .attr("stroke", "#fff")
            .attr("stroke-width", 1.5)
            .attr("stroke-dasharray", "4,2")
            .attr("opacity", 0.6);

        // Color coded syntax highlighting for the signature
        const textNode = node.append("text")
            .attr("x", 15)
            .attr("y", 4)
            .attr("font-size", "10px")
            .attr("stroke", "none");

        textNode.each(function(d) {
            const el = d3.select(this);
            if (d.is_external || !d.return_type) {
                el.append("tspan").attr("fill", "var(--accent)").text(d.name);
                return;
            }
            
            const params = (d.parameters || []).map(p => typeof p === 'object' ? (p.name || '...') : p);
            
            if (d.return_type) {
                el.append("tspan").attr("fill", "#ae81ff").text(d.return_type + " ");
            }
            if (d.namespace) {
                el.append("tspan").attr("fill", "white").attr("opacity", 0.8).text(d.namespace + "::");
            }
            el.append("tspan").attr("fill", "var(--accent)").attr("font-weight", "bold").text(d.name);
            el.append("tspan").attr("fill", "white").text("(");
            
            const slicedParams = params.slice(0, 2);
            slicedParams.forEach((p, i) => {
                el.append("tspan").attr("fill", "#ae81ff").text(p);
                if (i < slicedParams.length - 1) {
                    el.append("tspan").attr("fill", "white").text(", ");
                }
            });
            
            if (params.length > 2) {
                if (slicedParams.length > 0) el.append("tspan").attr("fill", "white").text(", ");
                el.append("tspan").attr("fill", "#ae81ff").text("...");
            }
            el.append("tspan").attr("fill", "white").text(")");
        });

        // Interaction
        node.on("mouseenter", (event, d) => {
            if (d.is_external) return;
            const fid = d.id;
            const name = d.name;
            const addr = d.entrypoint;
            if (window.showCodePreview) {
                window.showCodePreview(fid, name, addr, '', 0, event);
            }
        }).on("mousemove", (event) => {
            if (window.moveCodePreview) window.moveCodePreview(event);
        }).on("mouseleave", (event) => {
            if (window.hideCodePreview) window.hideCodePreview(event);
        }).on("click", (event, d) => {
            if (d.is_external) return;
            if (window.showFunctionCodeById) {
                window.showFunctionCodeById(d.id, d.name, '', event);
            }
        });

        simulation.on("tick", () => {
            link
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node
                .attr("transform", d => `translate(${d.x},${d.y})`);
        });

        // Controls
        this.sizeByOps = false;
        const btnSize = document.getElementById(`${this.containerId}-btn-size`);
        
        const updateRadii = () => {
            const maxF = Math.max(...this.nodes.map(n => n.features_count || 0), 1);
            node.selectAll("circle").transition().duration(500)
                .attr("r", function(d) {
                    const isOrphanRing = d3.select(this).attr("fill") === "none";
                    if (isOrphanRing) {
                        return this.parentNode._sizeByOps ? 17 + Math.min(20, (d.features_count / maxF) * 20) : 17;
                    }
                    
                    const base = d.is_external ? 8 : (d.is_unindexed ? 10 : 13);
                    if (!this.parentNode._sizeByOps || d.is_external || !d.features_count) return base;
                    
                    return base + Math.min(20, (d.features_count / maxF) * 20);
                });
        };

        btnSize.onclick = () => {
            this.sizeByOps = !this.sizeByOps;
            if (this.sizeByOps) {
                btnSize.textContent = "Size: Ops Count";
                btnSize.style.color = "var(--success)";
            } else {
                btnSize.textContent = "Size: Uniform";
                btnSize.style.color = "var(--accent)";
            }
            
            node.each(function(d) { this._sizeByOps = btnSize.textContent === "Size: Ops Count"; });
            updateRadii();
        };

        const btnPhysics = document.getElementById(`${this.containerId}-btn-physics`);
        btnPhysics.onclick = () => {
            this.physicsEnabled = !this.physicsEnabled;
            if (this.physicsEnabled) {
                btnPhysics.textContent = "Physics: ON";
                btnPhysics.style.color = "var(--success)";
                this.nodes.forEach(n => { n.fx = null; n.fy = null; });
                simulation.alphaTarget(0.3).restart();
            } else {
                btnPhysics.textContent = "Physics: OFF";
                btnPhysics.style.color = "var(--subtle)";
                simulation.stop();
                // Fix positions
                this.nodes.forEach(n => { n.fx = n.x; n.fy = n.y; });
            }
        };

        const btnHierarchy = document.getElementById(`${this.containerId}-btn-hierarchy`);
        btnHierarchy.onclick = () => {
            // Force physics off
            this.physicsEnabled = false;
            btnPhysics.textContent = "Physics: OFF";
            btnPhysics.style.color = "var(--subtle)";
            simulation.stop();
            
            // Organize hierarchically by depth
            const layerNodes = {};
            let maxDepth = 0;
            this.nodes.forEach(n => {
                if (!layerNodes[n.depth]) layerNodes[n.depth] = [];
                layerNodes[n.depth].push(n);
                if (n.depth > maxDepth) maxDepth = n.depth;
            });
            
            const layerHeight = 150;
            const startY = 100;
            
            Object.keys(layerNodes).forEach(depth => {
                const layer = layerNodes[depth];
                const widthPerNode = Math.max(200, this.width / (layer.length + 1));
                
                // Sort layer by name or degree for nicer layout
                layer.sort((a, b) => a.name.localeCompare(b.name));
                
                layer.forEach((n, i) => {
                    n.fx = widthPerNode * (i + 1);
                    n.fy = startY + parseInt(depth) * layerHeight;
                    n.x = n.fx;
                    n.y = n.fy;
                });
            });

            // Re-center view around the hierarchy
            const totalHeight = startY + maxDepth * layerHeight + 100;
            const totalWidth = Math.max(...Object.values(layerNodes).map(l => l.length)) * 200;
            const scale = Math.min(1, this.width / totalWidth, this.height / totalHeight);
            
            svg.transition().duration(750).call(
                zoomBehavior.transform, 
                d3.zoomIdentity.translate(this.width/2 - (totalWidth*scale)/2, 50).scale(scale)
            );

            // Update DOM positions
            link.transition().duration(750)
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node.transition().duration(750)
                .attr("transform", d => `translate(${d.x},${d.y})`);
        };
    }

    drag(simulation) {
        const self = this;
        function dragstarted(event) {
            if (self.physicsEnabled && !event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }

        function dragged(event) {
            event.subject.fx = event.x;
            event.subject.fy = event.y;
            
            if (!self.physicsEnabled) {
                event.subject.x = event.x;
                event.subject.y = event.y;
                d3.select(this).attr("transform", `translate(${event.x},${event.y})`);
                if (self.linkSelection) {
                    self.linkSelection
                        .attr("x1", d => d.source.x)
                        .attr("y1", d => d.source.y)
                        .attr("x2", d => d.target.x)
                        .attr("y2", d => d.target.y);
                }
            }
        }

        function dragended(event) {
            if (self.physicsEnabled) {
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null;
                event.subject.fy = null;
            } else {
                // Keep fx, fy to lock the node in its new dragged position
                event.subject.fx = event.x;
                event.subject.fy = event.y;
            }
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    }
}
