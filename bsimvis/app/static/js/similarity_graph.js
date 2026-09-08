class SimilarityGraph {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = d3.select(`#${containerId}`);
        this.width = 0;
        this.height = 0;
        this.svg = null;
        this.g = null;
        this.linkGroup = null;
        this.nodeGroup = null;
        this.ringGroup = null;
        this.zoom = null;

        this.all_pairs = [];
        this.nodes_map = new Map();
        this.unique_nodes = [];
        this.binary_md5s = new Set();
        this.abortController = null;

        this.mousePos = { clientX: 0, clientY: 0 };
        window.addEventListener('mousemove', e => {
            this.mousePos.clientX = e.clientX;
            this.mousePos.clientY = e.clientY;
        });

        window.graphInstance = this;
        this.initSVG();

        window.addEventListener('resize', () => this.handleResize());
    }

    initSVG() {
        this.container.selectAll("*").remove();
        const rect = this.container.node().getBoundingClientRect();
        this.width = rect.width || 800;
        this.height = rect.height || 500;

        this.svg = this.container.append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewBox", [-this.width / 2, -this.height / 2, this.width, this.height])
            .style("background-color", "var(--bg)")
            .style("user-select", "none");

        this.g = this.svg.append("g");
        this.defs = this.svg.append("defs");

        this.zoom = d3.zoom()
            .scaleExtent([0.1, 20])
            .on("zoom", (event) => {
                this.g.attr("transform", event.transform);
            });

        this.svg.call(this.zoom);

        // Background click to clear selection/hide previews
        this.svg.on("click", (e) => {
            if (e.target === this.svg.node()) {
                if (window.hideBinaryPreview) window.hideBinaryPreview();
                if (window.hideDiffPreview) window.hideDiffPreview();
                if (window.hideCodePreview) window.hideCodePreview();
                this.nodeGroup.selectAll("circle").attr("stroke", "white").attr("stroke-width", 1);
                this.linkGroup.selectAll("path").style("opacity", null).attr("stroke-width", d => d.width);
            }
        });

        this.linkGroup = this.g.append("g").attr("class", "links");
        this.ringGroup = this.g.append("g").attr("class", "rings");
        this.nodeGroup = this.g.append("g").attr("class", "nodes");
        this.binaryLabelGroup = this.g.append("g").attr("class", "binary-labels");
    }

    handleResize() {
        const rect = this.container.node().getBoundingClientRect();
        this.width = rect.width || 800;
        this.height = rect.height || 500;
        this.svg.attr("viewBox", [-this.width / 2, -this.height / 2, this.width, this.height]);
    }

    stop() {
        if (this.abortController) this.abortController.abort();
    }

    async fetch(params) {
        if (this.abortController) this.abortController.abort();
        this.abortController = new AbortController();
        const signal = this.abortController.signal;

        this.all_pairs = [];
        this.nodes_map.clear();
        this.unique_nodes = [];
        this.binary_md5s.clear();

        const overlay = document.getElementById('graph-loading-overlay');
        const streamInfo = document.getElementById('graph-stream-info');
        const loadingText = document.getElementById('graph-loading-text');
        const stopBtn = document.getElementById('graph-stop-btn');
        const DEFAULT_GRAPH_LIMIT = 500;
        const MAX_TOTAL = parseInt(params.get('limit')) || DEFAULT_GRAPH_LIMIT;

        if (overlay) overlay.style.display = 'flex';
        if (loadingText) loadingText.innerText = "Building Similarity Map...";
        if (streamInfo) streamInfo.innerText = "";
        if (stopBtn) stopBtn.style.display = 'inline-block';

        const cleanParams = new URLSearchParams(params.toString());
        cleanParams.delete('limit');
        cleanParams.delete('offset');
        const base_url = `/api/similarity/search?${cleanParams.toString()}`;
        let currentOffset = 0;

        this.updateSources(params);

        try {
            while (currentOffset < MAX_TOTAL && !signal.aborted) {
                const BATCH_SIZE = Math.min(MAX_TOTAL - this.all_pairs.length, 500);
                if (BATCH_SIZE <= 0) break;

                const res = await fetch(`${base_url}&limit=${BATCH_SIZE}&offset=${currentOffset}`, { signal });
                const data = await res.json();

                // UI Warnings (truncation)
                const poolIcon = document.getElementById('pool-warn-icon');
                const limitIcon = document.getElementById('limit-warn-icon');
                const poolInput = document.getElementById('sim-pool-limit');
                const limitInput = document.getElementById('sim-limit');
                const totalEl = document.getElementById('view-total');

                if (poolIcon) {
                    if (data.pool_truncated) {
                        poolIcon.style.display = 'inline-block';
                        poolIcon.title = `⚠️ Pool Truncated to first ${data.pool_limit || '---'} matches. Results may be incomplete.`;
                        if (poolInput) poolInput.style.borderColor = '#ffab2e';
                    } else {
                        poolIcon.style.display = 'none';
                        if (poolInput) poolInput.style.borderColor = 'var(--accent)';
                    }
                }

                const pairs = data.pairs || [];
                if (pairs.length === 0) break;

                this.all_pairs = this.all_pairs.concat(pairs);

                if (limitIcon) {
                    if (this.all_pairs.length >= MAX_TOTAL) {
                        limitIcon.style.display = 'inline-block';
                        limitIcon.title = `ℹ️ Result Limit Reached (${MAX_TOTAL.toLocaleString()}). Map display is capped.`;
                        if (limitInput) limitInput.style.borderColor = '#60a5fa';
                    } else {
                        limitIcon.style.display = 'none';
                        if (limitInput) limitInput.style.borderColor = 'var(--accent)';
                    }
                }

                if (totalEl) {
                    totalEl.style.display = 'inline-block';
                    totalEl.innerText = `${this.all_pairs.length.toLocaleString()} / ${(data.total || 0).toLocaleString()}`;
                }

                pairs.forEach(p => {
                    [{ id: p.id1, name: p.name1, meta: p.meta1 }, { id: p.id2, name: p.name2, meta: p.meta2 }].forEach(n => {
                        if (!this.nodes_map.has(n.id)) {
                            const node_obj = {
                                id: n.id, name: n.name, md5: n.meta.file_md5,
                                file_name: n.meta.file_name, return_type: n.meta.return_type,
                                addr: n.id.split(':').pop(), v_size: n.meta.bsim_features_count,
                                language_id: n.meta.language_id || 'N/A',
                                tags: n.meta.tags || [],
                                user_tags: n.meta.user_tags || [],
                                file_tags: n.meta.file_tags || [],
                                file_user_tags: n.meta.file_user_tags || [],
                                yara: n.meta.yara || n.meta.yara_matches,
                                avtype: n.meta.avtype,
                                filetype: n.meta.filetype,
                                cc_ip: n.meta.cc_ip || n.meta.ips
                            };
                            this.nodes_map.set(n.id, node_obj);
                            this.unique_nodes.push(node_obj);
                            this.binary_md5s.add(n.meta.file_md5);
                        }
                    });
                });

                this.updateSources(params);
                if (streamInfo) streamInfo.innerText = `Streamed ${this.all_pairs.length} matches...`;
                currentOffset += BATCH_SIZE;
                if (pairs.length < BATCH_SIZE) break;
            }
        } catch (e) {
            if (e.name !== 'AbortError') console.error(e);
        } finally {
            if (overlay) overlay.style.display = 'none';
            if (stopBtn) stopBtn.style.display = 'none';
        }
    }

    updateSources(params) {
        if (this.all_pairs.length === 0) {
            this.linkGroup.selectAll("*").remove();
            this.nodeGroup.selectAll("*").remove();
            this.ringGroup.selectAll("*").remove();
            return;
        }

        // 1. Sort nodes to ensure consistent circular order (by binary then name)
        this.unique_nodes.sort((a, b) => a.md5.localeCompare(b.md5) || a.name.localeCompare(b.name));

        const radius = Math.min(this.width, this.height) / 2 - 120;
        const innerRadius = radius;
        const ringWidth = 20;

        // 2. Build Hierarchy for Bundling
        const binGroups = d3.group(this.unique_nodes, d => d.md5);
        const hierarchyData = {
            name: "root",
            children: Array.from(binGroups, ([md5, nodes]) => ({
                name: md5,
                children: nodes
            }))
        };

        const root = d3.hierarchy(hierarchyData)
            .sort((a, b) => d3.ascending(a.data.name, b.data.name));

        const cluster = d3.cluster()
            .size([2 * Math.PI, innerRadius]);

        cluster(root);

        const idToNode = new Map(root.leaves().map(d => [d.data.id, d]));

        // 3. Prepare Links
        const minScoreParam = params.get('min_score');
        const minScore = (minScoreParam !== null && minScoreParam !== "") ? parseFloat(minScoreParam) : parseFloat(defaultMinScore());

        const colorBinaryBy = document.getElementById('graph-color-binary')?.value || 'binary';
        const colorFunctionBy = document.getElementById('graph-color-function')?.value || 'binary';
        const colorSimBy = document.getElementById('graph-color-sim')?.value || 'gradient';
        const linkWidthFactor = parseFloat(document.getElementById('graph-link-width')?.value || 1.0);
        const shouldScaleWidth = document.getElementById('graph-scale-width')?.checked ?? true;

        const getNodeColor = (n, mode) => {
            const defaultColor = getMd5Color(n.md5);
            if (typeof getRawTagColor === 'function') {
                if (mode === 'func_tag') {
                    const tagColor = getRawTagColor(n.tags, n.user_tags);
                    return tagColor || "var(--border)";
                } else if (mode === 'file_tag') {
                    const tagColor = getRawTagColor(n.file_tags, n.file_user_tags);
                    return tagColor || "var(--border)";
                }
            }
            return defaultColor;
        };

        const links = this.all_pairs.map(p => {
            const source = idToNode.get(p.id1);
            const target = idToNode.get(p.id2);
            if (!source || !target) return null;

            const norm = (p.score - minScore) / (1.0 - minScore + 0.0001);

            let color1, color2;
            let linkColorOverride = null;

            if (colorSimBy === 'gradient') {
                color1 = getMd5Color(source.data.md5);
                color2 = getMd5Color(target.data.md5);
            } else if (colorSimBy === 'sim_tag') {
                if (typeof getRawTagColor === 'function') {
                    linkColorOverride = getRawTagColor(p.tags, p.user_tags) || "var(--border)";
                } else {
                    linkColorOverride = "var(--border)";
                }
            } else if (colorSimBy === 'func_tag') {
                if (typeof getRawTagColor === 'function') {
                    color1 = getRawTagColor(source.data.tags, source.data.user_tags) || "var(--border)";
                    color2 = getRawTagColor(target.data.tags, target.data.user_tags) || "var(--border)";
                } else {
                    color1 = color2 = "var(--border)";
                }
            } else if (colorSimBy === 'file_tag') {
                if (typeof getRawTagColor === 'function') {
                    color1 = getRawTagColor(source.data.file_tags, source.data.file_user_tags) || "var(--border)";
                    color2 = getRawTagColor(target.data.file_tags, target.data.file_user_tags) || "var(--border)";
                } else {
                    color1 = color2 = "var(--border)";
                }
            }

            const baseWidth = shouldScaleWidth ? (0.5 + norm * 6) : 2.5;
            const finalWidth = baseWidth * linkWidthFactor;

            return {
                source, target, path: source.path(target), score: p.score,
                width: finalWidth, alpha: 0.1 + (norm * 0.7),
                color1, color2, linkColorOverride,
                id1: p.id1, id2: p.id2, name1: p.name1, name2: p.name2,
                sid: p.sid, algo: p.algo || 'unweighted_cosine',
                tags: p.tags || [], user_tags: p.user_tags || []
            };
        }).filter(l => l !== null);

        // 4. Draw Links
        const tension = document.getElementById('graph-bundle-tension')?.value || 0.85;
        const line = d3.lineRadial()
            .curve(d3.curveBundle.beta(tension))
            .radius(d => d.y)
            .angle(d => d.x);

        // For gradients, we need to define them in <defs>
        this.defs.selectAll("*").remove();

        const linkPaths = this.linkGroup.selectAll("path")
            .data(links)
            .join("path")
            .each((d, i, nodes) => {
                if (d.linkColorOverride) {
                    d.strokeColor = d.linkColorOverride;
                    return;
                }
                if (d.color1 === d.color2) {
                    d.strokeColor = d.color1;
                    return;
                }
                const gradId = `grad_${i}`;
                const grad = this.defs.append("linearGradient")
                    .attr("id", gradId)
                    .attr("gradientUnits", "userSpaceOnUse")
                    .attr("x1", d.source.y * Math.cos(d.source.x - Math.PI / 2))
                    .attr("y1", d.source.y * Math.sin(d.source.x - Math.PI / 2))
                    .attr("x2", d.target.y * Math.cos(d.target.x - Math.PI / 2))
                    .attr("y2", d.target.y * Math.sin(d.target.x - Math.PI / 2));

                grad.append("stop").attr("offset", "0%").attr("stop-color", d.color1);
                grad.append("stop").attr("offset", "100%").attr("stop-color", d.color2);
                d.gradientId = gradId;
                d.strokeColor = `url(#${gradId})`;
            })
            .attr("d", d => line(d.path))
            .attr("fill", "none")
            .attr("stroke", d => d.strokeColor)
            .attr("stroke-width", d => d.width)
            .attr("stroke-opacity", d => d.alpha)
            .style("cursor", "pointer")
            .on("mouseover", (event, d) => {
                if (window.graphContextMenuOpen || window.graphNodeHovered) return;
                const rect = this.container.node().getBoundingClientRect();
                const e = { clientX: event.clientX, clientY: event.clientY };

                // Set up preview cycle similar to previous implementation
                window.diffPreviewPairs = [{
                    id1: d.id1, id2: d.id2, n1: d.name1, n2: d.name2, score: d.score.toFixed(4),
                    sid: d.sid, algo: d.algo, tags: d.tags, user_tags: d.user_tags
                }];
                window.diffPreviewIndex = 0;

                if (window.showDiffPreview) window.showDiffPreview(d.id1, d.name1, d.id2, d.name2, d.score.toFixed(4), e, 0);

                d3.select(event.currentTarget)
                    .attr("stroke", "white")
                    .attr("stroke-opacity", 1)
                    .attr("stroke-width", d.width + 2);
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'link', d);
                }
            })
            .on("mouseout", (event, d) => {
                if (window.graphContextMenuOpen) return;
                if (window.hideDiffPreview) window.hideDiffPreview();
                d3.select(event.currentTarget)
                    .attr("stroke", d.strokeColor)
                    .attr("stroke-opacity", d.alpha)
                    .attr("stroke-width", d.width);
            })
            .on("click", (event, d) => {
                event.stopPropagation();
                // Open the pair currently selected in the preview (user may have scrolled)
                const pairs = window.diffPreviewPairs;
                const idx = window.diffPreviewIndex || 0;
                const sel = (pairs && pairs.length > 0) ? pairs[idx] : null;
                if (sel) {
                    window.openDiffDirectly(sel.id1, sel.n1, sel.id2, sel.n2, event);
                } else {
                    window.openDiffDirectly(d.id1, d.name1, d.id2, d.name2, event);
                }
            });

        // 5. Draw Binary Rings
        const arc = d3.arc()
            .innerRadius(radius + 5)
            .outerRadius(radius + ringWidth + 5)
            .startAngle(d => d.start)
            .endAngle(d => d.end);

        const ringsData = Array.from(binGroups, ([md5, nodes]) => {
            const nodeInfos = nodes.map(n => idToNode.get(n.id));
            const start = d3.min(nodeInfos, d => d.x);
            const end = d3.max(nodeInfos, d => d.x);
            // Add half step padding
            const step = (2 * Math.PI) / this.unique_nodes.length;

            let color = getMd5Color(md5);
            if (colorBinaryBy === 'file_tag') {
                color = "var(--border)";
                if (typeof getRawTagColor === 'function') {
                    const tagColor = getRawTagColor(nodes[0].file_tags, nodes[0].file_user_tags);
                    if (tagColor) color = tagColor;
                }
            }

            const collection = nodes[0].id.split(':')[0];
            return {
                md5,
                collection,
                fileId: `${collection}:file:${md5}`,
                start: start - step / 2,
                end: end + step / 2,
                color: color,
                name: md5.slice(0, 8),
                file_name: nodes[0].file_name || md5.slice(0, 8),
                count: nodes.length,
                language: nodes[0].language_id,
                tags: Array.from(new Set(nodes.flatMap(n => n.tags || []))).join(', '),
                file_tags: nodes[0].file_tags || [],
                file_user_tags: nodes[0].file_user_tags || [],
                extraMeta: {
                    yara: nodes[0].yara,
                    avtype: nodes[0].avtype,
                    filetype: nodes[0].filetype,
                    cc_ip: nodes[0].cc_ip
                }
            };
        });

        this.ringGroup.selectAll("path")
            .data(ringsData)
            .join("path")
            .attr("d", arc)
            .attr("fill", d => d.color)
            .attr("fill-opacity", 0.3)
            .style("cursor", "help")
            .on("mouseover", (event, d) => {
                if (window.graphContextMenuOpen) return;
                const e = { clientX: event.clientX, clientY: event.clientY };
                if (window.showBinaryPreview) window.showBinaryPreview(d.md5, d.file_name, d.count, d.language, d.tags, e, d.file_tags, d.file_user_tags, d.extraMeta);
                d3.select(event.currentTarget).attr("fill-opacity", 0.6);
            })
            .on("mouseout", (event, d) => {
                if (window.hideBinaryPreview) window.hideBinaryPreview();
                d3.select(event.currentTarget).attr("fill-opacity", 0.3);
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'file', d);
                }
            });

        // 6. Draw Nodes
        const nodes = this.nodeGroup.selectAll("g")
            .data(root.leaves())
            .join("g")
            .attr("transform", d => `rotate(${d.x * 180 / Math.PI - 90}) translate(${d.y},0)`);

        nodes.selectAll("circle")
            .data(d => [d])
            .join("circle")
            .attr("r", 5.5)
            .attr("fill", d => getNodeColor(d.data, colorFunctionBy))
            .attr("stroke", "white")
            .attr("stroke-width", 1)
            .style("cursor", "pointer")
            .on("mouseover", (event, d) => {
                if (window.graphContextMenuOpen) return;
                window.graphNodeHovered = true;
                const e = { clientX: event.clientX, clientY: event.clientY };
                const n = d.data;

                // Find all pairs for this node to show in diff preview
                const relatedPairs = this.all_pairs.filter(p => p.id1 === n.id || p.id2 === n.id);
                if (relatedPairs.length > 0) {
                    window.diffPreviewPairs = relatedPairs.map(p => ({
                        id1: p.id1, id2: p.id2, n1: p.name1, n2: p.name2, score: p.score.toFixed(4),
                        sid: p.sid, algo: p.algo || 'unweighted_cosine',
                        tags: p.tags || [], user_tags: p.user_tags || []
                    }));
                    window.diffPreviewIndex = 0;
                    const p = window.diffPreviewPairs[0];
                    if (window.showDiffPreview) window.showDiffPreview(p.id1, p.n1, p.id2, p.n2, p.score, e, window.diffPreviewPairs.length - 1);
                }

                d3.select(event.currentTarget).attr("stroke-width", 3).attr("stroke", "var(--accent)");

                // Highlight connected links
                this.linkGroup.selectAll("path")
                    .style("opacity", l => (l.id1 === n.id || l.id2 === n.id) ? 1 : 0.05)
                    .attr("stroke-width", l => (l.id1 === n.id || l.id2 === n.id) ? l.width + 2 : l.width);
            })
            .on("contextmenu", (event, d) => {
                event.preventDefault();
                event.stopPropagation();
                if (window.showGraphContextMenu) {
                    window.showGraphContextMenu(event, 'node', d.data);
                }
            })
            .on("mouseout", (event, d) => {
                if (window.graphContextMenuOpen) return;
                window.graphNodeHovered = false;
                if (window.hideDiffPreview) window.hideDiffPreview();
                d3.select(event.currentTarget).attr("stroke-width", 1).attr("stroke", "white");

                this.linkGroup.selectAll("path")
                    .style("opacity", null)
                    .attr("stroke-width", l => l.width);
            })
            .on("click", (event, d) => {
                event.stopPropagation();
                if (event.ctrlKey || event.metaKey) {
                    // Ctrl+click: open code view for this specific function
                    window.showFunctionCodeById(d.data.id, d.data.name, '', event);
                } else {
                    // Plain click: open the pair currently selected in the preview
                    const pairs = window.diffPreviewPairs;
                    const idx = window.diffPreviewIndex || 0;
                    const sel = (pairs && pairs.length > 0) ? pairs[idx] : null;
                    if (sel) {
                        window.openDiffDirectly(sel.id1, sel.n1, sel.id2, sel.n2, event);
                    } else {
                        window.showFunctionCodeById(d.data.id, d.data.name, '', event);
                    }
                }
            });

        // 7. Add labels
        const showLabelMode = document.getElementById('graph-show-label')?.value || 'func_name';

        // Clear existing labels
        this.binaryLabelGroup.selectAll("*").remove();
        nodes.selectAll("text").remove();

        if (showLabelMode === 'none') return;

        if (showLabelMode === 'file_name' || showLabelMode === 'file_tag') {
            const r = radius + ringWidth + 15;
            const texts = this.binaryLabelGroup.selectAll("text")
                .data(ringsData)
                .join("text")
                .attr("dy", "0.31em")
                .attr("transform", d => {
                    const angle = (d.start + d.end) / 2;
                    const deg = angle * 180 / Math.PI - 90;
                    return `rotate(${deg}) translate(${r},0) ${angle >= Math.PI ? "rotate(180)" : ""}`;
                })
                .attr("text-anchor", d => {
                    const angle = (d.start + d.end) / 2;
                    return angle >= Math.PI ? "end" : "start";
                })
                .attr("x", d => {
                    const angle = (d.start + d.end) / 2;
                    return angle >= Math.PI ? -5 : 5;
                })
                .style("font-size", "10px")
                .style("font-weight", "bold")
                .style("pointer-events", "none");

            texts.each(function(d) {
                const el = d3.select(this);
                el.selectAll("*").remove();
                if (showLabelMode === 'file_name') {
                    el.text(d.file_name).style("fill", d.color);
                } else {
                    const all = [...(d.file_tags || []), ...(d.file_user_tags || [])].filter(t => t && t.trim());
                    if (all.length === 0) {
                        el.append("tspan").text("---").style("fill", "#75715e");
                        return;
                    }
                    all.forEach((t, i) => {
                        if (i > 0) {
                            el.append("tspan").text(", ").style("fill", "#75715e");
                        }
                        // Through `getTagMetadata`, so a tag with no stored
                        // colour gets its derived one instead of the same cyan
                        // every other tag here was drawn in.
                        const color = window.getTagMetadata
                            ? window.getTagMetadata(t).color
                            : '#66d9ef';
                        el.append("tspan")
                            .text(t)
                            .style("fill", color)
                            .style("font-weight", "bold");
                    });
                }
            });
        } else if (this.unique_nodes.length < 500) {
            const labels = nodes.selectAll("text")
                .data(d => [d])
                .join("text")
                .attr("dy", "0.31em")
                .attr("x", d => d.x < Math.PI ? 10 : -10)
                .attr("text-anchor", d => d.x < Math.PI ? "start" : "end")
                .attr("transform", d => d.x >= Math.PI ? "rotate(180)" : null)
                .style("font-size", "8px")
                .style("pointer-events", "none");

            labels.each(function(d) {
                const el = d3.select(this);
                el.selectAll("*").remove();
                if (showLabelMode === 'func_name') {
                    el.append("tspan")
                        .text(d.data.return_type && d.data.return_type !== 'N/A' ? `${d.data.return_type} ` : "")
                        .style("fill", "#ae81ff") // Purple
                        .style("font-weight", "bold");

                    el.append("tspan")
                        .text(d.data.name)
                        .style("fill", "#66d9ef"); // Cyan
                } else if (showLabelMode === 'func_tag') {
                    const tags = d.data.tags || [];
                    const userTags = d.data.user_tags || [];
                    const all = [...tags, ...userTags].filter(t => t && t.trim());
                    if (all.length === 0) {
                        el.append("tspan").text("---").style("fill", "#75715e");
                        return;
                    }
                    all.forEach((t, i) => {
                        if (i > 0) {
                            el.append("tspan").text(", ").style("fill", "#75715e");
                        }
                        // Through `getTagMetadata`, so a tag with no stored
                        // colour gets its derived one instead of the same cyan
                        // every other tag here was drawn in.
                        const color = window.getTagMetadata
                            ? window.getTagMetadata(t).color
                            : '#66d9ef';
                        el.append("tspan")
                            .text(t)
                            .style("fill", color)
                            .style("font-weight", "bold");
                    });
                }
            });
        }
    }

    // Patch in-memory node/pair tag data after a cross-window tag update,
    // then refresh graph colors so the visual matches the new tag state.
    applyTagUpdate(action, etype, eid, tag) {
        const mutate = (arr, t, add) => {
            if (add) { if (!arr.includes(t)) arr.push(t); }
            else { const i = arr.indexOf(t); if (i !== -1) arr.splice(i, 1); }
        };
        const add = (action === 'add');

        if (etype === 'function') {
            // Update matching function node (allowing fallback for :func: vs :function: differences)
            let node = this.nodes_map.get(eid);
            if (!node) {
                const alternativeEid = eid.includes(':function:') 
                    ? eid.replace(':function:', ':func:') 
                    : eid.replace(':func:', ':function:');
                node = this.nodes_map.get(alternativeEid);
            }
            if (node) mutate(node.user_tags, tag, add);
        } else if (etype === 'file') {
            // eid format: collection:file:md5 — update all nodes with that md5
            const md5 = eid.split(':').pop();
            this.nodes_map.forEach(node => {
                if (node.md5 === md5) mutate(node.file_user_tags, tag, add);
            });
        } else if (etype === 'similarity') {
            // eid may be id1|id2|algo or a sid — match against all_pairs
            this.all_pairs.forEach(p => {
                let match = false;
                if (p.sid === eid) {
                    match = true;
                } else {
                    const parts = eid.split('|');
                    if (parts.length >= 2) {
                        const id1 = parts[0].replace(':function:', ':func:');
                        const id2 = parts[1].replace(':function:', ':func:');
                        const pId1 = p.id1.replace(':function:', ':func:');
                        const pId2 = p.id2.replace(':function:', ':func:');
                        if ((pId1 === id1 && pId2 === id2) || (pId1 === id2 && pId2 === id1)) {
                            match = true;
                        }
                    }
                }
                if (match) {
                    p.user_tags = p.user_tags || [];
                    mutate(p.user_tags, tag, add);
                }
            });
        }

        this.refreshColors();
    }

    refreshColors() {
        if (!this.unique_nodes.length) return;

        // Re-run the updateSources logic but focused on color updates if possible.
        // For simplicity and since D3 is fast enough for 500 nodes, we just call updateSources with current params.
        const params = (typeof getRoutingState === 'function') ? getRoutingState().params : new URLSearchParams(window.location.search);
        this.updateSources(params);

        if (typeof window.saveGraphSettings === 'function') {
            window.saveGraphSettings();
        }
    }

    applyProfile(profile) {
        const colorBinary = document.getElementById('graph-color-binary');
        const colorFunc = document.getElementById('graph-color-function');
        const colorSim = document.getElementById('graph-color-sim');

        if (!colorBinary || !colorFunc || !colorSim) return;

        // Update button states
        const toggle = document.getElementById('profile-toggle');
        if (toggle) {
            toggle.querySelectorAll('.view-btn').forEach(btn => {
                btn.classList.toggle('active', btn.getAttribute('data-profile') === profile);
            });
        }

        if (profile === 'default') {
            colorBinary.value = 'binary';
            colorFunc.value = 'binary';
            colorSim.value = 'gradient';
        } else if (profile === 'func_tags') {
            colorBinary.value = 'binary';
            colorFunc.value = 'func_tag';
            colorSim.value = 'func_tag';
        } else if (profile === 'sim_tags') {
            colorBinary.value = 'binary';
            colorFunc.value = 'binary';
            colorSim.value = 'sim_tag';
        }

        this.refreshColors();
    }

    blendHex(c1, c2, t) {
        if (!c1 || !c2) return "var(--subtle)";
        const rgb1 = [parseInt(c1.slice(1, 3), 16), parseInt(c1.slice(3, 5), 16), parseInt(c1.slice(5, 7), 16)];
        const rgb2 = [parseInt(c2.slice(1, 3), 16), parseInt(c2.slice(3, 5), 16), parseInt(c2.slice(5, 7), 16)];
        const res = rgb1.map((v, i) => Math.round(v * (1 - t) + rgb2[i] * t));
        return "#" + res.map(v => v.toString(16).padStart(2, '0')).join('');
    }
}
