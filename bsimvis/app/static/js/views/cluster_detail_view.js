/**
 * Cluster Detail View
 * Loaded when navigating to /collections/{col}/functions/clusters/{uuid}
 *                       or /collections/{col}/files/clusters/{uuid}
 *
 * The tree is loaded one level at a time. This view used to open with
 * `list?limit=20000`, which on a function-clustered collection is every
 * cluster in the collection -- megabytes of summaries to draw a sidebar that
 * shows a dozen rows. Now it asks for the cluster itself (with its ancestor
 * chain, so the path to the root can be drawn) and for one node's direct
 * children at a time, via the `parent` filter.
 */

window.ClusterDetailView = {
    params: null,
    isBinary: false,
    collection: '',
    algo: '',
    axis: 'overall',
    nodeType: '',

    clusterMapById: {},
    clusterMapByUuid: {},
    childrenMap: {}, // parent_id -> [child_id1, child_id2, ...]
    childrenLoaded: new Set(), // cluster_id whose children have been fetched
    rootNodes: [],

    selectedClusterUuid: null,
    expandedGroups: new Set(),
    memberCache: {}, // cluster_uuid -> direct_members array
    groupBy: 'cluster',
    treeExpanded: new Set(), // cluster_id
    tab: 'members',

    destroy() {
        this.params = null;
        this.clusterMapById = {};
        this.clusterMapByUuid = {};
        this.childrenMap = {};
        this.childrenLoaded.clear();
        this.rootNodes = [];
        this.memberCache = {};
        this.expandedGroups.clear();
        this.treeExpanded.clear();
        this.tab = 'members';
    },

    api(path) {
        return this.isBinary ? `/api/bin_cluster/${path}` : `/api/cluster/${path}`;
    },

    /** One listing call, with this view's collection/axis context applied. */
    async fetchClusters(extra) {
        const qs = new URLSearchParams();
        if (this.params && this.params.pool) qs.set('pool', this.params.pool);
        if (this.collection) qs.set('collection', this.collection);
        if (this.isBinary) {
            qs.set('axis', this.axis);
            if (this.nodeType) qs.set('node_type', this.nodeType);
        }
        Object.entries(extra || {}).forEach(([k, v]) => qs.set(k, v));

        const res = await fetch(`${this.api('list')}?${qs.toString()}`);
        if (!res.ok) throw new Error(`Cluster lookup failed (${res.status})`);
        const data = await res.json();
        return data.results || [];
    },

    /** Merge fetched clusters into the local maps, newest fields winning. */
    ingest(list) {
        (list || []).forEach(c => {
            const cid = String(c.cluster_id);
            this.clusterMapById[cid] = { ...(this.clusterMapById[cid] || {}), ...c };
            if (c.cluster_uuid) this.clusterMapByUuid[c.cluster_uuid] = this.clusterMapById[cid];

            const pid = c.parent ? String(c.parent) : null;
            if (pid) {
                if (!this.childrenMap[pid]) this.childrenMap[pid] = [];
                if (!this.childrenMap[pid].includes(cid)) this.childrenMap[pid].push(cid);
            } else if (!this.rootNodes.includes(cid)) {
                this.rootNodes.push(cid);
            }
        });
    },

    /** Direct children of one cluster, fetched once. */
    async loadChildren(clusterId) {
        const cid = String(clusterId);
        if (this.childrenLoaded.has(cid)) return;
        this.childrenLoaded.add(cid);
        try {
            const kids = await this.fetchClusters({ parent: cid, limit: '2000' });
            if (!this.childrenMap[cid]) this.childrenMap[cid] = [];
            this.ingest(kids);
        } catch (e) {
            console.error('Failed to fetch children of', cid, e);
            this.childrenLoaded.delete(cid);
        }
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        this.destroy();
        this.params = params;
        this.isBinary = params.view === 'bin-cluster-detail';
        this.collection = params.collection || '';
        this.algo = params.algo || 'unweighted_cosine';

        // Fix for binary clusters: parse axis from URL params
        const urlParams = new URLSearchParams(window.location.search);
        const explicitAxis = params.axis || urlParams.get('axis');
        this.axis = explicitAxis || window.BINSIM_DEFAULT_AXIS || 'overall';
        // Containers and files cluster in separate namespaces; carry whichever
        // one the caller opened, or the listing reads the wrong graph.
        this.nodeType = params.node_type || urlParams.get('node_type') || '';

        const uuid = params.cluster_uuid;
        if (!uuid) {
            container.innerHTML = '<div style="padding:30px; color:#f87171;">Error: No cluster UUID provided.</div>';
            return;
        }

        container.innerHTML = `
            <style>
                #cluster-main { display:flex; flex:1; min-height:0; align-items:stretch; gap:16px; padding: 20px 24px; }
                #cluster-sidebar {
                    width:320px; flex-shrink:0; display:flex; flex-direction:column;
                    border:1px solid var(--border); border-radius:8px; background:var(--card-bg);
                    overflow:auto; padding:10px 0;
                }
                .bsim-side-title {
                    font-size:0.68rem; text-transform:uppercase; letter-spacing:0.07em;
                    color:var(--subtle); font-weight:bold; padding:4px 12px 8px;
                    display:flex; align-items:baseline; justify-content:space-between; gap:8px;
                }
                .bsim-side-actions { display:flex; gap:8px; text-transform:none; letter-spacing:0; font-weight:normal; }
                .bsim-side-actions span { cursor:pointer; color:var(--dim); }
                .bsim-side-actions span:hover { color:var(--accent); }
                .bsim-tree { flex:0 0 auto; }
                .bsim-node {
                    display:flex; align-items:center; gap:6px; padding:4px 12px; cursor:pointer;
                    font-size:0.8rem; font-family:'Inter',sans-serif; color:var(--text);
                    border-left:3px solid transparent; white-space:nowrap;
                }
                .bsim-node:hover { background:var(--hover); }
                .bsim-node.selected { background:var(--hover); border-left-color:var(--accent); }
                .bsim-node .bsim-caret { width:12px; color:var(--subtle); flex-shrink:0; user-select:none; }
                .bsim-node .bsim-node-label { flex:1; overflow:hidden; text-overflow:ellipsis; }
                .bsim-node .bsim-node-count { font-size:0.68rem; color:var(--dim); font-family:'Consolas',monospace; }

                .bsim-grp-row td {
                    background:var(--bg-alt); border-top:1px solid var(--border);
                    border-bottom:1px solid var(--border); padding:7px 10px;
                    font-family:'Inter',sans-serif; font-size:0.78rem; cursor:pointer;
                }
                .bsim-grp-row:hover td { background:var(--hover); }
                .bsim-caret-btn { cursor:pointer; user-select:none; color:var(--subtle); display:inline-block; width:14px; text-align:center; }

                #cluster-members-table td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
                #cluster-members-table tr:hover:not(.bsim-grp-row) { background: var(--hover); }
            </style>

            <div id="cluster-loader" style="display:flex; justify-content:center; align-items:center; height:100%; color:var(--dim);">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading cluster...
            </div>

            <div id="cluster-main" style="display:none;">
                <div id="cluster-sidebar">
                    <div class="bsim-side-title">
                        Cluster Hierarchy
                        <span class="bsim-side-actions">
                            <span onclick="ClusterDetailView.collapseTreeAll()" title="Collapse back to this cluster">collapse</span>
                        </span>
                    </div>
                    <div id="cluster-tree" class="bsim-tree"></div>
                </div>

                <div id="cluster-detail" style="flex:1; min-width:0; display:flex; flex-direction:column; min-height:0;">
                    <div id="cluster-header"></div>

                    <div class="bsim-tabbar" id="cluster-view-tabs" style="margin:20px 0 16px; flex-shrink:0;">
                        <button class="bsim-tab active" id="cluster-tab-btn-members" onclick="ClusterDetailView.switchTab('members')">Members</button>
                        <button class="bsim-tab" id="cluster-tab-btn-metadata" onclick="ClusterDetailView.switchTab('metadata')">Metadata</button>
                    </div>

                    <div id="cluster-tab-members" style="display:flex; flex-direction:column; flex:1; min-height:0;">
                        <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap; flex-shrink:0;">
                            <div class="view-toggle" style="margin:0; display:flex; align-items:center;">
                                <span class="bsim-ctl-label">Group by:</span>
                                <button class="view-btn active" id="cluster-group-btn-cluster" onclick="ClusterDetailView.setGroupBy('cluster')" title="Group by child cluster">Cluster</button>
                                <button class="view-btn" id="cluster-group-btn-none" onclick="ClusterDetailView.setGroupBy('none')" title="Flat list of direct members">None</button>
                            </div>
                        </div>

                        <div class="resizable-card" style="border:1px solid var(--border); border-radius:8px; display:flex; flex-direction:column; flex:1; min-height:200px; overflow:hidden; margin-top: 10px; background: var(--card-bg);">
                            <div style="flex:1; overflow:auto;" id="cluster-table-scroll">
                                <table id="cluster-members-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                                    <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                                        <tr style="border-bottom:1px solid var(--border); color:var(--dim);">
                                            <th style="padding:8px 12px; text-align:left;">${this.isBinary ? 'File Name' : 'Function'}</th>
                                            <th style="padding:8px 12px; text-align:left;">MD5</th>
                                            <th style="padding:8px 12px; text-align:left;">${this.isBinary ? 'Arch' : 'File Name'}</th>
                                        </tr>
                                    </thead>
                                    <tbody id="cluster-members-tbody"></tbody>
                                </table>
                                <div id="cluster-table-loader" style="display:none; text-align:center; padding:20px; color:var(--dim);">
                                    <i class="fa-solid fa-spinner fa-spin"></i> Loading members...
                                </div>
                            </div>
                        </div>
                    </div>

                    <div id="cluster-tab-metadata" style="display:none; flex:1; min-height:0; overflow:auto;"></div>
                </div>
            </div>
        `;

        try {
            // The cluster itself plus its ancestors: enough to draw the path to
            // the root. Everything else in the tree arrives on expand.
            const lookup = { cluster_uuid: uuid, show_parents: 'true', limit: '1' };
            let chain = await this.fetchClusters(lookup);
            // Each score axis clusters into its own namespace, but a uuid is
            // unique across them: a link that carries no axis probes the
            // others rather than reporting the cluster missing.
            if (this.isBinary && !explicitAxis && !chain.length) {
                for (const ax of ['overall', 'code', 'library', 'content']) {
                    if (ax === this.axis) continue;
                    this.axis = ax;
                    chain = await this.fetchClusters(lookup);
                    if (chain.length) break;
                }
            }
            this.ingest(chain);

            // uuid matching is a substring test server-side; pin the exact one.
            const self = chain.find(c => String(c.cluster_uuid) === String(uuid))
                || chain.find(c => String(c.cluster_uuid || '').startsWith(String(uuid)));

            if (!self) {
                container.innerHTML = `<div style="padding:30px; color:var(--dim);">No cluster matching <code>${escapeHtml(uuid)}</code>.</div>`;
                return;
            }

            this.selectedClusterUuid = self.cluster_uuid;
            this.ensureTreeExpandedTo(self.cluster_id);

            document.getElementById('cluster-loader').style.display = 'none';
            document.getElementById('cluster-main').style.display = 'flex';

            this.renderTree();
            await this.selectNode(this.selectedClusterUuid);

            if (window.TableSelection) new window.TableSelection('cluster-members-table');
        } catch (e) {
            console.error(e);
            container.innerHTML = `<div style="padding:30px; color:#f92672;">
                <i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(e.message)}</div>`;
        }
    },

    ensureTreeExpandedTo(clusterId) {
        let curr = this.clusterMapById[String(clusterId)];
        while (curr && curr.parent) {
            this.treeExpanded.add(String(curr.parent));
            curr = this.clusterMapById[String(curr.parent)];
        }
    },

    collapseTreeAll() {
        this.treeExpanded.clear();
        this.ensureTreeExpandedTo(this.clusterMapByUuid[this.selectedClusterUuid]?.cluster_id);
        this.renderTree();
    },

    async toggleTreeNode(clusterId, event) {
        event.stopPropagation();
        const id = String(clusterId);
        if (this.treeExpanded.has(id)) {
            this.treeExpanded.delete(id);
            this.renderTree();
            return;
        }
        this.treeExpanded.add(id);
        this.renderTree();
        if (!this.childrenLoaded.has(id)) {
            await this.loadChildren(id);
            this.renderTree();
        }
    },

    /** Does this node have children, loaded or not? */
    hasChildren(c, cid) {
        if ((this.childrenMap[cid] || []).length > 0) return true;
        // has_children comes off the listing, so a node that has never been
        // expanded still draws a caret instead of looking like a leaf.
        return !this.childrenLoaded.has(cid) && !!(c && c.has_children);
    },

    renderTree() {
        const treeContainer = document.getElementById('cluster-tree');
        if (!treeContainer) return;

        let html = '';
        const buildHtml = (cid, depth) => {
            const c = this.clusterMapById[cid];
            if (!c) return '';

            const isSelected = c.cluster_uuid === this.selectedClusterUuid;
            const children = this.childrenMap[cid] || [];
            const hasChildren = this.hasChildren(c, cid);
            const isExpanded = this.treeExpanded.has(cid);

            let icon = '';
            if (hasChildren) {
                icon = `<i class="fa-solid fa-caret-${isExpanded ? 'down' : 'right'} bsim-caret" onclick="ClusterDetailView.toggleTreeNode('${cid}', event)"></i>`;
            } else {
                icon = `<div class="bsim-caret"></div>`;
            }

            let rowHtml = `
                <div class="bsim-node ${isSelected ? 'selected' : ''}" style="padding-left: ${12 + depth * 16}px;" onclick="ClusterDetailView.selectNode('${c.cluster_uuid}')">
                    ${icon}
                    <i class="fa-solid fa-bullseye" style="color:var(--accent); font-size:0.75rem;"></i>
                    <span class="bsim-node-label" title="${escapeAttr(c.cluster_name || `Cluster #${c.cluster_id}`)}">
                        ${escapeHtml(c.cluster_name || `Cluster #${c.cluster_id}`)}
                    </span>
                    <span class="bsim-node-count">${Number(c.count || 0).toLocaleString()}</span>
                </div>
            `;

            if (hasChildren && isExpanded) {
                if (children.length === 0) {
                    rowHtml += `<div class="bsim-node" style="padding-left:${28 + depth * 16}px; color:var(--dim); cursor:default;">
                        <i class="fa-solid fa-spinner fa-spin" style="font-size:0.7rem;"></i> loading…</div>`;
                } else {
                    // Sort children by count desc
                    const sorted = [...children].sort((a, b) => (this.clusterMapById[b]?.count || 0) - (this.clusterMapById[a]?.count || 0));
                    for (const childId of sorted) {
                        rowHtml += buildHtml(childId, depth + 1);
                    }
                }
            }

            return rowHtml;
        };

        // Only the loaded slice of the hierarchy is drawn: the ancestor chain
        // of the selected cluster, and whatever has been expanded under it.
        const sortedRoots = [...this.rootNodes].sort((a, b) => (this.clusterMapById[b]?.count || 0) - (this.clusterMapById[a]?.count || 0));
        for (const rootId of sortedRoots) {
            html += buildHtml(rootId, 0);
        }

        treeContainer.innerHTML = html;
    },

    setBreadcrumb(self) {
        if (typeof Breadcrumbs === 'undefined' || !Breadcrumbs.refresh) return;
        Breadcrumbs.setClusterName(self.cluster_uuid, self.cluster_name || `#${self.cluster_id}`);
        Breadcrumbs.refresh();
    },

    async selectNode(uuid) {
        this.selectedClusterUuid = uuid;
        let c = this.clusterMapByUuid[uuid];
        if (!c) {
            this.ingest(await this.fetchClusters({ cluster_uuid: uuid, show_parents: 'true', limit: '1' }));
            c = this.clusterMapByUuid[uuid];
        }
        if (!c) return;

        this.ensureTreeExpandedTo(c.cluster_id);
        this.treeExpanded.add(String(c.cluster_id));
        this.renderTree();
        this.setBreadcrumb(c);

        // Update URL to reflect selected cluster without reloading
        const urlParams = new URLSearchParams(window.location.search);
        urlParams.set(this.isBinary ? 'bin_cluster_uuid' : 'cluster_uuid', uuid);
        if (this.isBinary) urlParams.set('axis', this.axis);
        const newUrl = window.location.pathname + '?' + urlParams.toString();
        window.history.replaceState({path: newUrl}, '', newUrl);

        // Render header immediately
        document.getElementById('cluster-header').innerHTML = this.renderHeader(c);

        // The selected node's own children, one level deep
        await this.loadChildren(c.cluster_id);
        this.renderTree();

        // Fetch members if not cached
        if (!this.memberCache[uuid]) {
            await this.fetchMembers(uuid);
        }

        this.renderTable();
        if (this.tab === 'metadata') this.renderMetadataTab();
    },

    async fetchMembers(uuid) {
        try {
            document.getElementById('cluster-table-loader').style.display = 'block';

            const all = await this.fetchClusters({
                cluster_uuid: uuid,
                show_members: 'true',
                limit: '1', // we only need the exact cluster's members
            });
            const exact = all.find(c => String(c.cluster_uuid) === String(uuid));
            this.memberCache[uuid] = (exact && exact.direct_members) || [];
        } catch (e) {
            console.error('Failed to fetch members for', uuid, e);
            this.memberCache[uuid] = [];
        } finally {
            const loader = document.getElementById('cluster-table-loader');
            if (loader) loader.style.display = 'none';
        }
    },

    setGroupBy(mode) {
        this.groupBy = mode;
        document.getElementById('cluster-group-btn-cluster').classList.toggle('active', mode === 'cluster');
        document.getElementById('cluster-group-btn-none').classList.toggle('active', mode === 'none');
        this.renderTable();
    },

    switchTab(tab) {
        this.tab = tab;
        ['members', 'metadata'].forEach(t => {
            document.getElementById(`cluster-tab-btn-${t}`).classList.toggle('active', t === tab);
            document.getElementById(`cluster-tab-${t}`).style.display =
                t === tab ? (t === 'members' ? 'flex' : 'block') : 'none';
        });
        if (tab === 'metadata') this.renderMetadataTab();
    },

    renderHeader(self) {
        const stat = (label, value) => `
            <div style="min-width:110px;">
                <div class="dim" style="font-size:0.65rem; text-transform:uppercase; letter-spacing:1px;">${label}</div>
                <div style="font-size:1.1rem; font-weight:bold;">${value}</div>
            </div>`;

        const pct = v => (v === null || v === undefined) ? '---' : (Number(v) * 100).toFixed(1) + '%';

        return `
        <div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:16px 20px;">
            <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                <i class="fa-solid fa-bullseye" style="color:var(--accent);"></i>
                <span style="font-size:1.2rem; font-weight:bold;">${escapeHtml(self.cluster_name || `Cluster #${self.cluster_id}`)}</span>
                <span class="badge">${this.isBinary ? (this.axis === 'overall' ? 'binary' : `binary (${this.axis})`) : 'function'}</span>
                ${EntityRenderer.renderTag(this.isBinary ? 'bin_cluster' : 'cluster', self.tag_id || self.cluster_id, [], self.user_tags || [])}
            </div>
            <div class="mono dim" style="font-size:0.72rem; margin-top:6px;">
                ${escapeHtml(self.cluster_uuid || '')}
                <button class="btn-copy" title="Copy UUID" onclick="copyToClipboard(${escapeAttr(jsString(self.cluster_uuid || ''))}, this)"><i class="fa-regular fa-copy"></i></button>
            </div>
            <div style="display:flex; gap:26px; flex-wrap:wrap; margin-top:14px;">
                ${stat('Members', Number(self.count || 0).toLocaleString())}
                ${stat('Cohesion', pct(self.cohesion_score))}
                ${stat('Stability', Number(self.avg_stability || 0).toFixed(2))}
                ${stat('Avg features', Number(self.avg_features || 0).toFixed(0))}
                ${stat('Cluster ID', escapeHtml(String(self.cluster_id)))}
            </div>
            <div style="margin-top:14px;">
                ${this.renderMemberListLink(self)}
            </div>
        </div>`;
    },

    /** The distributions and the function-count spread of the selected cluster. */
    async renderMetadataTab() {
        const panel = document.getElementById('cluster-tab-metadata');
        if (!panel) return;
        const uuid = this.selectedClusterUuid;
        let c = this.clusterMapByUuid[uuid];
        if (!c) return;

        // The function-count spread is stored on the cluster summary, but
        // clusters built before it existed have none; with_stats makes the
        // server walk that one cluster's members for it.
        if (this.isBinary && !c.function_count_stats) {
            panel.innerHTML = `<div class="dim" style="padding:20px;"><i class="fa-solid fa-spinner fa-spin"></i> Loading cluster metadata…</div>`;
            try {
                this.ingest(await this.fetchClusters({ cluster_uuid: uuid, with_stats: 'true', limit: '1' }));
                c = this.clusterMapByUuid[uuid];
            } catch (e) {
                console.error('Failed to load cluster stats', e);
            }
            if (this.tab !== 'metadata' || this.selectedClusterUuid !== uuid) return;
        }

        const dists = [
            ['File Type', 'fa-solid fa-file-code', c.filetype_distribution],
            ['Architecture', 'fa-solid fa-microchip', c.architecture_distribution],
            ['Executable Format', 'fa-solid fa-file-code', c.executable_format_distribution],
            ['Batch UUID', 'fa-solid fa-box', c.batch_uuid_distribution],
        ];
        Object.entries(c.tag_distribution || {}).forEach(([axis, dist]) => {
            dists.push(['Tags · ' + axis, 'fa-solid fa-tags', dist, true]);
        });
        const cards = dists.map(([title, icon, dist, isTags]) => (isTags ? window.renderTagDist(title, icon, dist) : window.renderDist(title, icon, dist))).join('');

        panel.innerHTML = `
            <div style="display:flex; flex-direction:column; gap:14px;">
                ${this.renderFunctionCountCard(c)}
                ${cards
                    ? `<div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:14px; align-items:start;">${cards}</div>`
                    : `<div class="dim" style="padding:20px; text-align:center;">No metadata distributions for this cluster.${this.isBinary ? ' A cluster below the configured min_cohesion reports none.' : ''}</div>`}
            </div>`;
    },

    renderFunctionCountCard(c) {
        const s = c.function_count_stats || {};
        const num = v => (v === null || v === undefined) ? '---' : Number(v).toLocaleString();
        const cell = (label, value) => `
            <div style="flex:1; min-width:90px;">
                <div class="dim" style="font-size:0.62rem; text-transform:uppercase; letter-spacing:1px;">${label}</div>
                <div style="font-size:1.05rem; font-weight:bold;">${value}</div>
            </div>`;

        const body = (s.min === undefined)
            ? `<div class="dim" style="font-size:0.75rem;">Not computed for this cluster${this.isBinary ? '' : ' (function clusters group functions, not files)'}.</div>`
            : `<div style="display:flex; gap:20px; flex-wrap:wrap;">
                    ${cell('Min', num(s.min))}
                    ${cell('Average', num(s.avg))}
                    ${cell('Max', num(s.max))}
                    ${cell('Files counted', num(s.files))}
               </div>`;

        return `
            <div style="padding:12px 14px; background:var(--card-bg); border:1px solid var(--border); border-radius:6px;">
                <div style="font-size:0.75rem; color:var(--dim); margin-bottom:10px; display:flex; align-items:center; gap:6px;">
                    <i class="fa-solid fa-list-ol"></i> Functions per member
                </div>
                ${body}
            </div>`;
    },

    /** The old destination, kept as an explicit action rather than a surprise. */
    renderMemberListLink(self) {
        const col = this.collection || '';
        const segs = this.isBinary ? ['files'] : ['functions'];
        const key = this.isBinary ? 'bin_cluster_uuid' : 'cluster_uuid';
        const url = `${Nav.buildUIUrl(col, segs)}?${key}=${encodeURIComponent(self.cluster_uuid)}`;
        return `<a href="${escapeAttr(url)}" class="ui-button" onclick="Nav.openPath(this.href, event)">
            <i class="fa-solid fa-list"></i> Open in ${this.isBinary ? 'file' : 'function'} search
        </a>`;
    },

    async toggleGroup(uuid) {
        if (this.expandedGroups.has(uuid)) {
            this.expandedGroups.delete(uuid);
        } else {
            this.expandedGroups.add(uuid);
            const c = this.clusterMapByUuid[uuid];
            if (c) await this.loadChildren(c.cluster_id);
            if (!this.memberCache[uuid]) {
                await this.fetchMembers(uuid);
            }
        }
        this.renderTable();
    },

    renderTable() {
        const tbody = document.getElementById('cluster-members-tbody');
        tbody.innerHTML = '';

        if (!this.selectedClusterUuid) return;

        if (this.groupBy === 'none') {
            this.renderFlatMembers(tbody);
        } else {
            this.renderHierarchicalGroups(tbody, this.selectedClusterUuid, 0);
        }
    },

    async renderFlatMembers(tbody) {
        tbody.innerHTML = `<tr><td colspan="3" class="dim" style="padding:20px; text-align:center;"><i class="fa-solid fa-spinner fa-spin"></i> Loading full membership...</td></tr>`;
        try {
            const qs = new URLSearchParams();
            if (this.params.pool) qs.set('pool', this.params.pool);
            if (this.collection) qs.set('collection', this.collection);
            qs.set(this.isBinary ? 'bin_cluster_uuid' : 'cluster_uuid', this.selectedClusterUuid);
            qs.set('limit', '1000');

            const endpoint = this.isBinary ? '/api/file/search' : '/api/function/search';
            const res = await fetch(`${endpoint}?${qs.toString()}`);
            if (!res.ok) throw new Error('Search API failed');
            const data = await res.json();

            tbody.innerHTML = '';
            const items = this.isBinary ? data.files : data.functions;
            if (!items || items.length === 0) {
                tbody.innerHTML = `<tr><td colspan="3" class="dim" style="padding:20px; text-align:center;">No members found in search index.</td></tr>`;
                return;
            }

            const members = items.map(m => {
                if (this.isBinary) {
                    return {
                        id: m.id || m.md5 || m.file_md5,
                        name: m.file_name || m.name,
                        file_md5: m.md5 || m.file_md5,
                        language_id: m.language_id,
                        bin: m.file_name || m.name
                    };
                } else {
                    return {
                        id: m.function_id || m.id,
                        name: m.function_name || m.name,
                        addr: m.entrypoint_address || m.addr,
                        file_md5: m.file_md5 || m.md5,
                        bin: m.file_name || m.bin,
                        v_size: m.bsim_features_count || m.v_size
                    };
                }
            });

            this.renderMembersList(tbody, members, 0);

            if (data.total > 1000) {
                const tr = document.createElement('tr');
                tr.innerHTML = `<td colspan="3" class="dim" style="padding:15px; text-align:center; font-style:italic;">Showing first 1000 members out of ${data.total}. ${this.renderMemberListLink(this.clusterMapByUuid[this.selectedClusterUuid])}</td>`;
                tbody.appendChild(tr);
            }

        } catch (e) {
            console.error(e);
            tbody.innerHTML = `<tr><td colspan="3" style="padding:20px; text-align:center; color:var(--error);"><i class="fa-solid fa-circle-exclamation"></i> Error loading members: ${e.message}</td></tr>`;
        }
    },

    renderHierarchicalGroups(tbody, uuid, depth) {
        const c = this.clusterMapByUuid[uuid];
        if (!c) return;

        // 1. If this is not the root of the current selection, we render it as a group header
        if (uuid !== this.selectedClusterUuid) {
            const isExpanded = this.expandedGroups.has(uuid);
            const tr = document.createElement('tr');
            tr.className = 'bsim-grp-row';
            tr.onclick = () => this.toggleGroup(uuid);
            tr.innerHTML = `
                <td colspan="3" style="padding-left: ${12 + depth * 20}px;">
                    <i class="fa-solid fa-chevron-${isExpanded ? 'down' : 'right'} bsim-caret-btn"></i>
                    <i class="fa-solid fa-bullseye" style="color:var(--accent); margin:0 6px;"></i>
                    <b>${escapeHtml(c.cluster_name || `Cluster #${c.cluster_id}`)}</b>
                    <span class="dim" style="font-size:0.72rem; margin-left:8px;">(${Number(c.count || 0).toLocaleString()} members in subtree)</span>
                </td>
            `;
            tbody.appendChild(tr);

            if (!isExpanded) return; // Stop here if collapsed
        }

        // 2. If we are expanded (or it's the root of the selection), render its direct members and children

        // 2a. Direct Members
        if (uuid === this.selectedClusterUuid && !this.memberCache[uuid]) {
            return; // Waiting for fetch
        }

        const members = this.memberCache[uuid] || [];
        const children = this.childrenMap[c.cluster_id] || [];
        const targetDepth = uuid === this.selectedClusterUuid ? depth : depth + 1;

        if (members.length > 0) {
            if (children.length > 0) {
                 const dmId = uuid + '_direct';
                 const isDmExpanded = this.expandedGroups.has(dmId);
                 const tr = document.createElement('tr');
                 tr.className = 'bsim-grp-row';
                 tr.onclick = () => {
                     if (isDmExpanded) this.expandedGroups.delete(dmId);
                     else this.expandedGroups.add(dmId);
                     this.renderTable();
                 };
                 tr.innerHTML = `
                    <td colspan="3" style="padding-left: ${12 + targetDepth * 20}px; opacity: 0.9;">
                        <i class="fa-solid fa-chevron-${isDmExpanded ? 'down' : 'right'} bsim-caret-btn"></i>
                        <i class="fa-solid fa-users" style="color:var(--dim); margin:0 6px;"></i>
                        <b>Direct Members</b>
                        <span class="dim" style="font-size:0.72rem; margin-left:8px;">(${members.length})</span>
                    </td>
                 `;
                 tbody.appendChild(tr);

                 if (isDmExpanded) {
                     this.renderMembersList(tbody, members, targetDepth + 1);
                 }
            } else {
                 this.renderMembersList(tbody, members, targetDepth);
            }
        } else if (members.length === 0 && children.length === 0) {
            // Empty leaf
            const tr = document.createElement('tr');
            tr.innerHTML = `<td colspan="3" class="dim" style="padding-left: ${12 + targetDepth * 20}px; font-style:italic;">No direct members</td>`;
            tbody.appendChild(tr);
        }

        // 2b. Children
        // Sort children by count descending
        const sortedChildren = [...children].sort((a, b) => (this.clusterMapById[b]?.count || 0) - (this.clusterMapById[a]?.count || 0));

        for (const childId of sortedChildren) {
            const child = this.clusterMapById[childId];
            if (child) {
                this.renderHierarchicalGroups(tbody, child.cluster_uuid, targetDepth);
            }
        }
    },

    renderMembersList(tbody, members, depth) {
        const col = this.collection || '';

        members.forEach(m => {
            const memberCol = String(m.id || '').split(':')[0] || col;
            const md5 = m.file_md5 || '';
            const tr = document.createElement('tr');
            tr.setAttribute('data-id', escapeAttr(m.id || md5));

            let c1, c2, c3;
            if (this.isBinary) {
                c1 = EntityRenderer.renderFileName(m.name || '', md5, memberCol);
                c2 = EntityRenderer.renderMd5(md5, { collection: memberCol });
                c3 = `<span class="dim">${escapeHtml(m.language_id || '---')}</span>`;
            } else {
                const f = {
                    function_id: m.id,
                    function_name: m.name,
                    name: m.name,
                    entrypoint_address: m.addr,
                    file_md5: md5,
                    file_name: m.bin,
                    bsim_features_count: m.v_size,
                    collection: memberCol,
                };
                c1 = EntityRenderer.renderFunction(f);
                c2 = EntityRenderer.renderMd5(md5, { collection: memberCol });
                c3 = `<span class="dim">${escapeHtml(m.bin || '---')}</span>`;
            }

            tr.innerHTML = `
                <td style="padding-left: ${12 + depth * 20}px;">${c1}</td>
                <td>${c2}</td>
                <td>${c3}</td>
            `;
            tbody.appendChild(tr);
        });
    }
};
