class ChordGraph {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = d3.select(`#${containerId}`);
        this.width = 0;
        this.height = 0;
        this.svg = null;
        this.g = null;
        this.abortController = null;
        this.data = [];
        this.nodes = [];
        
        window.chordGraphInstance = this;
        this.initSVG();
        window.addEventListener('resize', () => this.handleResize());
    }

    initSVG() {
        this.container.selectAll("*").remove();
        const rect = this.container.node().getBoundingClientRect();
        this.width = rect.width || 800;
        this.height = rect.height || 600;

        this.svg = this.container.append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewBox", [-this.width / 2, -this.height / 2, this.width, this.height])
            .style("background-color", "#0d0f14")
            .style("user-select", "none");

        this.g = this.svg.append("g");
        this.defs = this.svg.append("defs");
        
        // Add zoom support
        const zoom = d3.zoom()
            .scaleExtent([0.5, 10])
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
        this.svg.attr("viewBox", [-this.width / 2, -this.height / 2, this.width, this.height]);
    }

    stop() {
        if (this.abortController) this.abortController.abort();
    }

    fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        const overlay = document.getElementById('graph-loading-overlay');
        const loadingText = document.getElementById('graph-loading-text');
        if (overlay) overlay.style.display = 'flex';
        if (loadingText) loadingText.innerText = "Building Chord Map...";

        try {
            // Use all parameters passed from the URL
            const url = `/api/bin_sim/search?${params.toString()}`;
            return fetch(url, { signal })
                .then(res => res.json())
                .then(data => {
                    this.data = data.results || [];
                    this.render();
                });
        } catch (e) {
            if (e.name !== 'AbortError') console.error("Chord Graph Error:", e);
        } finally {
            if (overlay) overlay.style.display = 'none';
        }
    }

    render() {
        this.g.selectAll("*").remove();
        if (!this.data.length) {
            this.g.append("text")
                .attr("text-anchor", "middle")
                .attr("fill", "#888")
                .text("No binary similarity data found for this collection.");
            return;
        }

        // 1. Identify unique binaries and build an index
        const binaryMap = new Map();
        this.data.forEach(d => {
            if (!binaryMap.has(d.md5_a)) {
                binaryMap.set(d.md5_a, { 
                    md5: d.md5_a, 
                    name: d.file_name_a, 
                    arch: d.architecture_a,
                    compiler: d.compiler_a,
                    functions_count: d.functions_count_a,
                    entry_date: d.entry_date_a,
                    tags: d.file_tags_a,
                    user_tags: d.file_user_tags_a
                });
            }
            if (!binaryMap.has(d.md5_b)) {
                binaryMap.set(d.md5_b, { 
                    md5: d.md5_b, 
                    name: d.file_name_b, 
                    arch: d.architecture_b,
                    compiler: d.compiler_b,
                    functions_count: d.functions_count_b,
                    entry_date: d.entry_date_b,
                    tags: d.file_tags_b,
                    user_tags: d.file_user_tags_b
                });
            }
        });

        const nodes = Array.from(binaryMap.values());
        const md5ToIndex = new Map(nodes.map((n, i) => [n.md5, i]));
        const n = nodes.length;

        // 2. Build the adjacency matrix
        const matrix = Array.from({ length: n }, () => new Float64Array(n));
        this.data.forEach(d => {
            const i = md5ToIndex.get(d.md5_a);
            const j = md5ToIndex.get(d.md5_b);
            // We use score as the weight
            const score = d.score_collection_weighted || d.score || 0;
            matrix[i][j] = score;
            matrix[j][i] = score; // Symmetrical
        });

        // 3. Setup D3 Chord layout
        const outerRadius = Math.min(this.width, this.height) * 0.5 - 120;
        const innerRadius = outerRadius - 20;

        const chord = d3.chord()
            .padAngle(0.05)
            .sortSubgroups(d3.descending);

        const arc = d3.arc()
            .innerRadius(innerRadius)
            .outerRadius(outerRadius);

        const ribbon = d3.ribbon()
            .radius(innerRadius);

        const chords = chord(matrix);

        // 4. Draw Arcs (Binaries)
        const group = this.g.append("g")
            .selectAll("g")
            .data(chords.groups)
            .join("g");

        group.append("path")
            .attr("fill", d => getMd5Color(nodes[d.index].md5))
            .attr("stroke", "#fff")
            .attr("stroke-width", 0.5)
            .attr("d", arc)
            .style("cursor", "pointer")
            .on("mouseover", (event, d) => {
                this.highlightChords(d.index, true);
                this.showTooltip(event, nodes[d.index]);
            })
            .on("mouseout", () => {
                this.highlightChords(null, false);
                this.hideTooltip();
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'file', nodes[d.index]);
                }
            });

        // 5. Draw Labels
        group.append("text")
            .each(d => { d.angle = (d.startAngle + d.endAngle) / 2; })
            .attr("dy", ".35em")
            .attr("transform", d => `
                rotate(${(d.angle * 180 / Math.PI - 90)})
                translate(${outerRadius + 10})
                ${d.angle > Math.PI ? "rotate(180)" : ""}
            `)
            .attr("text-anchor", d => d.angle > Math.PI ? "end" : "start")
            .attr("fill", "#fff")
            .style("font-size", "10px")
            .style("font-weight", "bold")
            .text(d => nodes[d.index].name || nodes[d.index].md5.substring(0, 8));

        // 6. Draw Ribbons (Similarities)
        this.defs.selectAll("*").remove();

        this.g.append("g")
            .attr("fill-opacity", 0.67)
            .selectAll("path")
            .data(chords)
            .join("path")
            .attr("class", d => `chord chord-${d.source.index} chord-${d.target.index}`)
            .each((d, i) => {
                const color1 = getMd5Color(nodes[d.source.index].md5);
                const color2 = getMd5Color(nodes[d.target.index].md5);
                
                if (color1 === color2) {
                    d.fillColor = color1;
                    return;
                }
                
                const gradId = `chord_grad_${i}`;
                const grad = this.defs.append("linearGradient")
                    .attr("id", gradId)
                    .attr("gradientUnits", "userSpaceOnUse");
                
                const angle1 = (d.source.startAngle + d.source.endAngle) / 2;
                const angle2 = (d.target.startAngle + d.target.endAngle) / 2;
                
                grad.attr("x1", innerRadius * Math.cos(angle1 - Math.PI / 2))
                    .attr("y1", innerRadius * Math.sin(angle1 - Math.PI / 2))
                    .attr("x2", innerRadius * Math.cos(angle2 - Math.PI / 2))
                    .attr("y2", innerRadius * Math.sin(angle2 - Math.PI / 2));
                
                grad.append("stop").attr("offset", "0%").attr("stop-color", color1);
                grad.append("stop").attr("offset", "100%").attr("stop-color", color2);
                
                d.fillColor = `url(#${gradId})`;
            })
            .attr("d", ribbon)
            .attr("fill", d => d.fillColor)
            .attr("stroke", "none")
            .style("mix-blend-mode", "screen")
            .on("mouseover", (event, d) => {
                d3.selectAll(".chord").style("opacity", 0.1);
                d3.select(event.currentTarget).style("opacity", 1);
                this.showChordTooltip(event, nodes[d.source.index], nodes[d.target.index], d.source.value);
            })
            .on("mouseout", () => {
                d3.selectAll(".chord").style("opacity", 1);
                this.hideTooltip();
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'bin_similarity', {
                        file1: nodes[d.source.index],
                        file2: nodes[d.target.index],
                        value: d.source.value
                    });
                }
            })
            .on("click", (event, d) => {
                const n1 = nodes[d.source.index];
                const n2 = nodes[d.target.index];
                // Navigate to binary similarity diff view
                const { collection } = getRoutingState();
                const diffUrl = Nav.buildUIUrl(collection, ['diff', `${n1.md5}/${n2.md5}`]);
                const safeNameA = (n1.name || n1.md5.substring(0,8)).replace(/'/g, "\\'").replace(/"/g, "&quot;");
                const safeNameB = (n2.name || n2.md5.substring(0,8)).replace(/'/g, "\\'").replace(/"/g, "&quot;");
                
                if (event.ctrlKey || event.metaKey) {
                    window.open(diffUrl, '_blank');
                } else if (typeof windowManager !== 'undefined') {
                    windowManager.createWindow(`Bin Diff: ${safeNameA} vs ${safeNameB}`, diffUrl, { type: 'diff' });
                } else if (window.parent && window.parent.windowManager) {
                    window.parent.windowManager.createWindow(`Bin Diff: ${safeNameA} vs ${safeNameB}`, diffUrl, { type: 'diff' });
                } else {
                    window.location.href = diffUrl;
                }
            });
    }

    highlightChords(index, active) {
        if (!active) {
            d3.selectAll(".chord").style("opacity", 1);
            return;
        }
        d3.selectAll(".chord").style("opacity", 0.05);
        d3.selectAll(`.chord-${index}`).style("opacity", 1);
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
                <div style="font-weight:bold; color:var(--accent); font-size:0.95rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${node.name}</div>
                <div class="dim mono" style="font-size:0.7rem; margin-top:2px; opacity:0.7;"># ${node.md5}</div>
            </div>
            
            ${renderRow('fa-globe', 'Architecture', node.arch)}
            ${renderRow('fa-gears', 'Compiler', node.compiler)}
            ${renderRow('fa-list-ol', 'Functions', node.functions_count)}
            ${renderRow('fa-calendar-day', 'Entry Date', typeof formatDate === 'function' ? formatDate(node.entry_date) : node.entry_date)}
            
            <div style="margin-top:10px; display:flex; flex-wrap:wrap; gap:4px; border-top:1px solid rgba(255,255,255,0.05); padding-top:8px;">
                ${tags.length > 0 ? tags.map(t => `<span class="tag-pill" style="font-size:0.65rem; padding:1px 6px;">${t}</span>`).join('') : '<span class="dim" style="font-size:0.7rem;">No tags</span>'}
            </div>
        `;
        tip.style.display = 'block';
        tip.style.left = (event.clientX + 10) + 'px';
        tip.style.top = (event.clientY + 10) + 'px';
    }

    showChordTooltip(event, n1, n2, score) {
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
                <b>${n1.name}</b> ↔ <b>${n2.name}</b>
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

    hideTooltip() {
        const tip = document.getElementById('chord-tooltip');
        if (tip) tip.style.display = 'none';
    }
}
