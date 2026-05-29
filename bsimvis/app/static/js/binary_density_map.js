class BinaryDensityMap {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = d3.select(`#${containerId}`);
        this.width = 0;
        this.height = 0;
        this.svg = null;
        this.g = null;
        this.defs = null;
        this.abortController = null;
        this.nodes = [];
        this.links = [];
        this.tension = 0.95; 
        this.localGravity = false; // By default, don't enable k-means local gravity
        this.clusterGranularity = 0.5; // Default granularity (0.5 * nodes.length / 2)
        this.showLabels = true;
        this.isolatedMode = 'gray'; // 'all', 'gray', 'hide'
        this.linkColorScheme = 'gradient'; // 'gradient', 'score'
        
        window.binaryDensityMapInstance = this;
        this.initSVG();
        window.addEventListener('resize', () => this.handleResize());
    }

    shortenName(name, length = 16) {
        if (!name) return "";
        if (name.length <= length) return name;
        return name.substring(0, length - 3) + "...";
    }

    toggleLabels(show) {
        this.showLabels = show;
        this.render();
    }

    setIsolatedMode(mode) {
        this.isolatedMode = mode;
        this.render();
        this.updateIsolatedUI();
    }

    updateIsolatedUI() {
        const container = document.getElementById('isolated-toggle');
        if (!container) return;
        container.querySelectorAll('.view-btn').forEach(btn => {
            btn.classList.toggle('active', btn.getAttribute('data-mode') === this.isolatedMode);
        });
    }

    setLinkColorScheme(scheme) {
        this.linkColorScheme = scheme;
        this.render();
        this.updateLinkColorUI();
    }

    updateLinkColorUI() {
        const container = document.getElementById('link-color-toggle');
        if (!container) return;
        container.querySelectorAll('.view-btn').forEach(btn => {
            btn.classList.toggle('active', btn.getAttribute('data-scheme') === this.linkColorScheme);
        });
    }

    toggleGravity(enabled) {
        this.localGravity = enabled;
        this.render();
    }

    setTension(value) {
        this.tension = parseFloat(value);
        if (this.localGravity) this.render();
    }

    setClusterGranularity(value) {
        this.clusterGranularity = parseFloat(value);
        if (this.localGravity) this.render();
    }

    initSVG() {
        this.container.selectAll("svg").remove();
        const rect = this.container.node().getBoundingClientRect();
        this.width = rect.width || 800;
        this.height = rect.height || 600;

        this.svg = this.container.append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewBox", [0, 0, this.width, this.height])
            .style("background-color", "#0d0f14")
            .style("user-select", "none");

        this.g = this.svg.append("g");
        this.defs = this.svg.append("defs");
        
        const zoom = d3.zoom()
            .scaleExtent([0.1, 40])
            .on("zoom", (event) => {
                this.g.attr("transform", event.transform);
            });
        this.svg.call(zoom);
    }

    handleResize() {
        const rect = this.container.node().getBoundingClientRect();
        if (!rect.width) return;
        this.width = rect.width;
        this.height = rect.height;
        this.svg.attr("viewBox", [0, 0, this.width, this.height]);
    }

    stop() {
        if (this.abortController) this.abortController.abort();
    }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        const overlay = document.getElementById('graph-loading-overlay');
        const loadingText = document.getElementById('graph-loading-text');
        if (overlay) overlay.style.display = 'flex';
        if (loadingText) loadingText.innerText = "Building Density Map...";

        try {
            const url = `/api/bin_sim/umap?${params.toString()}`;
            const res = await fetch(url, { signal });
            const data = await res.json();
            
            this.nodes = data.nodes || [];
            this.links = data.links || [];
            this.render();
        } catch (e) {
            if (e.name !== 'AbortError') console.error("Binary Density Map Error:", e);
        } finally {
            if (overlay) overlay.style.display = 'none';
        }
    }

    // Simple K-Means to find local cluster centers
    calculateClusters(nodes, k = 5) {
        if (nodes.length <= k) {
            nodes.forEach((n, i) => n.clusterIdx = i);
            return nodes.map(n => ({x: n.x, y: n.y}));
        }
        
        // Initialize centroids randomly from nodes
        let centroids = nodes.slice(0, k).map(n => ({x: n.x, y: n.y}));
        
        for (let iter = 0; iter < 10; iter++) {
            const groups = Array.from({length: k}, () => []);
            nodes.forEach(n => {
                let minDist = Infinity;
                let bestIdx = 0;
                centroids.forEach((c, i) => {
                    const d = Math.hypot(n.x - c.x, n.y - c.y);
                    if (d < minDist) {
                        minDist = d;
                        bestIdx = i;
                    }
                });
                groups[bestIdx].push(n);
                n.clusterIdx = bestIdx; // Store cluster assignment on node
            });
            
            centroids = groups.map((group, i) => {
                if (group.length === 0) return centroids[i];
                return {
                    x: d3.mean(group, n => n.x),
                    y: d3.mean(group, n => n.y)
                };
            });
        }
        return centroids;
    }

    render() {
        this.g.selectAll("*").remove();
        this.defs.selectAll("*").remove();

        if (!this.nodes.length) {
            this.g.append("text")
                .attr("x", this.width / 2)
                .attr("y", this.height / 2)
                .attr("text-anchor", "middle")
                .attr("fill", "#888")
                .text("No UMAP density data found. Try rebuilding binary similarities.");
            return;
        }

        // 1. Identify Isolated Nodes
        const linkedNodes = new Set();
        this.links.forEach(l => {
            linkedNodes.add(l.source);
            linkedNodes.add(l.target);
        });

        const visibleNodes = this.isolatedMode === 'hide' 
            ? this.nodes.filter(n => linkedNodes.has(n.id))
            : this.nodes;

        if (!visibleNodes.length) {
            this.g.append("text")
                .attr("x", this.width / 2)
                .attr("y", this.height / 2)
                .attr("text-anchor", "middle")
                .attr("fill", "#888")
                .text(this.isolatedMode === 'hide' ? "No binaries with similarities found." : "No UMAP density data found.");
            return;
        }

        // 2. Setup Scales
        const xExtent = d3.extent(visibleNodes, d => d.x);
        const yExtent = d3.extent(visibleNodes, d => d.y);
        
        const xPadding = (xExtent[1] - xExtent[0]) * 0.1 || 1;
        const yPadding = (yExtent[1] - yExtent[0]) * 0.1 || 1;

        const xScale = d3.scaleLinear()
            .domain([xExtent[0] - xPadding, xExtent[1] + xPadding])
            .range([0, this.width]);
            
        const yScale = d3.scaleLinear()
            .domain([yExtent[0] - yPadding, yExtent[1] + yPadding])
            .range([this.height, 0]);

        const nodeMap = new Map(this.nodes.map(n => [n.id, n]));
        const linkGroup = this.g.append("g").attr("class", "links");

        // 3. Identify Local Cluster Centers & Setup Line Generator
        let screenCentroids = [];
        let lineGenerator;

        if (this.localGravity) {
            // Gravity Path (Slower, requires K-Means)
            const numClusters = Math.max(2, Math.floor(visibleNodes.length * this.clusterGranularity));
            const centroids = this.calculateClusters(visibleNodes, numClusters);
            screenCentroids = centroids.map(c => [xScale(c.x), yScale(c.y)]);

            lineGenerator = d3.line()
                .curve(d3.curveBundle.beta(0.98))
                .x(d => d[0])
                .y(d => d[1]);
        } else {
            // Fast Path (Straight lines)
            lineGenerator = d3.line()
                .curve(d3.curveLinear)
                .x(d => d[0])
                .y(d => d[1]);
        }

        // 4. Draw Links
        linkGroup.selectAll("path")
            .data(this.links)
            .join("path")
            .attr("class", "similarity-link")
            .each((d, i) => {
                const n1 = nodeMap.get(d.source);
                const n2 = nodeMap.get(d.target);
                
                const p1 = [xScale(n1.x), yScale(n1.y)];
                const p2 = [xScale(n2.x), yScale(n2.y)];
                
                if (this.localGravity) {
                    const c1 = screenCentroids[n1.clusterIdx];
                    const c2 = screenCentroids[n2.clusterIdx];
                    const targetCP = [(c1[0] + c2[0]) / 2, (c1[1] + c2[1]) / 2];
                    
                    const mid = [(p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2];
                    const cp = [
                        mid[0] + (targetCP[0] - mid[0]) * this.tension,
                        mid[1] + (targetCP[1] - mid[1]) * this.tension
                    ];
                    d.points = [p1, cp, p2];
                } else {
                    d.points = [p1, p2];
                }
                
                const color1 = getMd5Color(d.source);
                const color2 = getMd5Color(d.target);
                
                if (this.linkColorScheme === 'score') {
                    // Determine domain based on filters if available, otherwise data extent
                    let minScore = parseFloat(document.getElementById('bsim-min-score')?.value) || 0;
                    let maxScore = parseFloat(document.getElementById('bsim-max-score')?.value) || 1;
                    
                    // Fallback to data extent if no filters are set
                    if (!document.getElementById('bsim-min-score')?.value && !document.getElementById('bsim-max-score')?.value) {
                         const scores = this.links.map(l => l.value);
                         if (scores.length) {
                             minScore = Math.min(...scores);
                             maxScore = Math.max(...scores);
                         }
                    }
                    
                    // If all scores are the same, ensure a valid range
                    if (minScore === maxScore) {
                        minScore = 0;
                        maxScore = 1;
                    }

                    // Use the same color from sankey graph cohesion (hsl(score * 120, 70%, 55%))
                    const scoreColorScale = d3.scaleSequential(t => `hsl(${t * 120}, 70%, 55%)`)
                        .domain([minScore, maxScore]);
                        
                    d.linkColor = scoreColorScale(d.value);
                    d.gradientId = null;
                } else {
                    const gradId = `link_grad_${i}`;
                    const grad = this.defs.append("linearGradient")
                        .attr("id", gradId)
                        .attr("gradientUnits", "userSpaceOnUse")
                        .attr("x1", p1[0])
                        .attr("y1", p1[1])
                        .attr("x2", p2[0])
                        .attr("y2", p2[1]);
                    
                    grad.append("stop").attr("offset", "0%").attr("stop-color", color1);
                    grad.append("stop").attr("offset", "100%").attr("stop-color", color2);
                    
                    d.gradientId = gradId;
                    d.linkColor = null;
                }
            })
            .attr("d", d => lineGenerator(d.points))
            .attr("fill", "none")
            .attr("stroke", d => d.gradientId ? `url(#${d.gradientId})` : d.linkColor)
            .attr("stroke-opacity", 0.4)
            .attr("stroke-width", d => Math.max(1, d.value * 4))
            .style("cursor", "pointer")
            .on("mouseover", (event, d) => {
                d3.select(event.currentTarget)
                    .attr("stroke-opacity", 1)
                    .attr("stroke-width", d => Math.max(2, d.value * 7));
                this.showLinkTooltip(event, nodeMap.get(d.source), nodeMap.get(d.target), d.value);
            })
            .on("mouseout", (event, d) => {
                d3.select(event.currentTarget)
                    .attr("stroke-opacity", 0.4)
                    .attr("stroke-width", d => Math.max(1, d.value * 4));
                this.hideTooltip();
            })
            .on("click", (event, d) => {
                this.openDiff(nodeMap.get(d.source), nodeMap.get(d.target), event);
            });

        // 5. Draw Nodes (Scatter Plot)
        const node = this.g.append("g")
            .attr("class", "nodes")
            .selectAll("g")
            .data(visibleNodes)
            .join("g")
            .attr("class", "node-group")
            .attr("transform", d => `translate(${xScale(d.x)},${yScale(d.y)})`);

        node.append("circle")
            .attr("r", 5)
            .attr("fill", d => {
                if (this.isolatedMode === 'gray' && !linkedNodes.has(d.id)) return "#333";
                return getMd5Color(d.id);
            })
            .attr("stroke", "#fff")
            .attr("stroke-width", 1.0)
            .style("cursor", "pointer")
            .on("mouseover", (event, d) => this.showTooltip(event, d))
            .on("mouseout", () => this.hideTooltip())
            .on("click", (event, d) => {
                if (typeof addToFileDiff === 'function') {
                    addToFileDiff(d.id, d.file_name, event);
                }
            });

        node.append("text")
            .attr("dy", 12)
            .attr("text-anchor", "middle")
            .attr("fill", d => {
                if (this.isolatedMode === 'gray' && !linkedNodes.has(d.id)) return "#666";
                return getMd5Color(d.id);
            })
            .style("font-size", "8px")
            .style("pointer-events", "none")
            .style("text-shadow", "1px 1px 1px #000")
            .style("display", this.showLabels ? "block" : "none")
            .text(d => this.shortenName(d.file_name || d.id.substring(0, 8)));

        this.updateSelection();
    }

    updateSelection() {
        if (!this.svg) return;
        
        let defs = this.svg.select("defs");
        if (defs.empty()) {
            defs = this.svg.append("defs");
            this.defs = defs;
        }

        if (defs.select("#selection-glow").empty()) {
            const filter = defs.append("filter")
                .attr("id", "selection-glow")
                .attr("x", "-50%")
                .attr("y", "-50%")
                .attr("width", "200%")
                .attr("height", "200%");
            
            filter.append("feGaussianBlur")
                .attr("stdDeviation", "2")
                .attr("result", "coloredBlur");
            
            const feMerge = filter.append("feMerge");
            feMerge.append("feMergeNode").attr("in", "coloredBlur");
            feMerge.append("feMergeNode").attr("in", "SourceGraphic");
        }
        
        let queue = [];
        try {
            queue = JSON.parse(localStorage.getItem('bsim_file_diff_queue') || '[]');
        } catch (e) { console.error("Error reading diff queue", e); }
            
        const selectedMd5s = new Set(queue.map(item => {
            if (!item.id) return null;
            return item.id.includes(':') ? item.id.split(':').pop() : item.id;
        }).filter(Boolean));

        const orange = "#fd971f";

        this.g.selectAll("g.node-group")
            .each(function(d) {
                const g = d3.select(this);
                const isSelected = selectedMd5s.has(d.id);
                
                g.select("circle")
                    .attr("stroke", isSelected ? orange : "#fff")
                    .attr("stroke-width", isSelected ? 3 : 1.0)
                    .attr("stroke-dasharray", isSelected ? "2,1" : "none")
                    .style("filter", isSelected ? "url(#selection-glow)" : "none");

                g.select("text")
                    .attr("fill", isSelected ? orange : getMd5Color(d.id))
                    .style("font-weight", isSelected ? "bold" : "normal")
                    .style("font-size", isSelected ? "10px" : "8px");
            });
    }

    showTooltip(event, node) {
        let tip = document.getElementById('chord-tooltip');
        if (!tip) {
            tip = document.createElement('div');
            tip.id = 'chord-tooltip';
            tip.className = 'graph-tooltip';
            document.body.appendChild(tip);
        }

        const renderRow = (icon, label, value) => `
            <div style="display:flex; justify-content:space-between; gap:20px; font-size:0.75rem; margin-top:4px;">
                <span class="dim"><i class="fa-solid ${icon}" style="width:14px; margin-right:5px; opacity:0.6;"></i>${label}</span>
                <span class="mono" style="color:var(--accent)">${value || 'Unknown'}</span>
            </div>
        `;

        const tags = [...(node.tags || []), ...(node.user_tags || [])];

        tip.innerHTML = `
            <div style="border-bottom:1px solid rgba(255,255,255,0.1); padding-bottom:8px; margin-bottom:8px; min-width:250px;">
                <div style="font-weight:bold; color:var(--accent); font-size:0.95rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${node.file_name}</div>
                <div class="dim mono" style="font-size:0.7rem; margin-top:2px; opacity:0.7;"># ${node.id}</div>
            </div>
            
            ${renderRow('fa-globe', 'Architecture', node.architecture)}
            ${renderRow('fa-gears', 'Compiler', node.compiler)}
            ${renderRow('fa-list-ol', 'Functions', node.functions_count)}
            ${renderRow('fa-calendar-day', 'Entry Date', typeof formatDate === 'function' ? formatDate(node.entry_date) : node.entry_date)}
            
            <div style="margin-top:10px; display:flex; flex-wrap:wrap; gap:4px; border-top:1px solid rgba(255,255,255,0.05); padding-top:8px;">
                ${tags.length > 0 ? tags.map(t => `<span class="tag-pill" style="font-size:0.65rem; padding:1px 6px;">${t}</span>`).join('') : '<span class="dim" style="font-size:0.7rem;">No tags</span>'}
            </div>
            <div class="dim" style="font-size:0.65rem; margin-top:8px; text-align:center; border-top:1px solid rgba(255,255,255,0.05); padding-top:5px;">
                Click to add to comparison
            </div>
        `;
        tip.style.display = 'block';
        tip.style.left = (event.clientX + 15) + 'px';
        tip.style.top = (event.clientY + 15) + 'px';
    }

    showLinkTooltip(event, n1, n2, score) {
        let tip = document.getElementById('chord-tooltip');
        if (!tip) {
            tip = document.createElement('div');
            tip.id = 'chord-tooltip';
            tip.className = 'graph-tooltip';
            document.body.appendChild(tip);
        }
        tip.innerHTML = `
            <div style="font-weight:bold; color:var(--success);">Binary Similarity</div>
            <div style="margin-top:5px; font-size:0.8rem;">
                <b>${n1.file_name}</b> ↔ <b>${n2.file_name}</b>
            </div>
            <div style="margin-top:8px; font-size:1.1rem; color:var(--success); font-weight:bold;">
                Score: ${(score * 100).toFixed(1)}%
            </div>
            <div class="dim" style="font-size:0.7rem; margin-top:5px;">Click to view detailed diff</div>
        `;
        tip.style.display = 'block';
        tip.style.left = (event.clientX + 10) + 'px';
        tip.style.top = (event.clientY + 10) + 'px';
    }

    openDiff(n1, n2, event) {
        const collection = new URLSearchParams(window.location.hash.split('?')[1]).get('collection') || 'main';
        const diffUrl = `/static/bin_sim/index.html?collection=${collection}&md5_a=${n1.id}&md5_b=${n2.id}`;
        const safeNameA = (n1.file_name || n1.id.substring(0,8)).replace(/'/g, "\\'").replace(/"/g, "&quot;");
        const safeNameB = (n2.file_name || n2.id.substring(0,8)).replace(/'/g, "\\'").replace(/"/g, "&quot;");
        
        if (event.ctrlKey || event.metaKey) {
            window.open(diffUrl, '_blank');
        } else if (typeof windowManager !== 'undefined') {
            windowManager.createWindow(`Bin Diff: ${safeNameA} vs ${safeNameB}`, diffUrl, { type: 'diff' });
        } else if (window.parent && window.parent.windowManager) {
            window.parent.windowManager.createWindow(`Bin Diff: ${safeNameA} vs ${safeNameB}`, diffUrl, { type: 'diff' });
        } else {
            window.open(diffUrl, '_blank');
        }
    }

    hideTooltip() {
        const tip = document.getElementById('chord-tooltip');
        if (tip) tip.style.display = 'none';
    }
}
