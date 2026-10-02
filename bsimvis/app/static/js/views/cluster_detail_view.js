/**
 * Cluster Detail View
 * Loaded when navigating to /collections/{col}/functions/clusters/{uuid}
 *                       or /collections/{col}/files/clusters/{uuid}
 *
 * The sidebar shows a bounded WINDOW around the selected cluster, not the
 * whole hierarchy: `treeUp` ancestors above, `treeDown` levels below, at most
 * `treeWidth` children per node -- fetched in one `slice` call server-side
 * (cluster_utils.cluster_tree_slice), which never reads more cluster metas
 * than the window needs. "N more above" / "+N more" rows page past the caps
 * via the `parent` fast path. Selecting another node re-centers the window.
 */

const CLUSTER_TREE_DEFAULTS = { up: 3, down: 3, width: 10 };

window.ClusterDetailView = {
    params: null,
    isBinary: false,
    collection: '',
    algo: '',
    axis: 'overall',
    nodeType: '',

    clusterMapById: {},
    clusterMapByUuid: {},
    childrenMap: {}, // parent_id -> [child_id1, child_id2, ...] (shown so far)
    childTotal: {}, // parent_id -> total child count (for "+N more")
    childrenLoaded: new Set(), // cluster_id whose children have been fetched at least once
    treeAncestors: [], // nearest-first cluster_ids above the selected node, currently shown
    hiddenAbove: 0, // ancestors that exist but aren't in treeAncestors yet

    treeUp: CLUSTER_TREE_DEFAULTS.up,
    treeDown: CLUSTER_TREE_DEFAULTS.down,
    treeWidth: CLUSTER_TREE_DEFAULTS.width,

    selectedClusterUuid: null,
    expandedGroups: new Set(),
    memberCache: {}, // cluster_uuid -> direct_members array
    centralitySort: false, // members ranked by centrality, highest first
    groupBy: 'cluster',
    treeExpanded: new Set(), // cluster_id
    tab: 'members',

    destroy() {
        this.params = null;
        this.clusterMapById = {};
        this.clusterMapByUuid = {};
        this.childrenMap = {};
        this.childTotal = {};
        this.childrenLoaded.clear();
        this.treeAncestors = [];
        this.hiddenAbove = 0;
        this.memberCache = {};
        this.expandedGroups.clear();
        this.treeExpanded.clear();
        this._sliceCenteredOn = null;
        this.heroCompact = false;
        this.heroManual = false;
        this.tab = 'members';
        if (this._nway) this._nway.destroy();
        this._nway = null;
    },

    /** Function clusters open on their medoid's code, file clusters on the member list. */
    defaultTab() {
        return this.isBinary ? 'members' : 'medoid';
    },

    /** The Functions tab (N-way diff of the node's files) exists for binary file clusters only. */
    hasFunctionsTab() {
        return this.isBinary && this.nodeType !== 'container';
    },

    /** up/down/width, remembered per browser -- a per-viewer convenience, never load-bearing. */
    loadTreeSettings() {
        try {
            const raw = localStorage.getItem('clusterTreeSettings');
            const saved = raw ? JSON.parse(raw) : {};
            this.treeUp = Number(saved.up) || CLUSTER_TREE_DEFAULTS.up;
            this.treeDown = Number(saved.down) || CLUSTER_TREE_DEFAULTS.down;
            this.treeWidth = Number(saved.width) || CLUSTER_TREE_DEFAULTS.width;
        } catch (e) {
            this.treeUp = CLUSTER_TREE_DEFAULTS.up;
            this.treeDown = CLUSTER_TREE_DEFAULTS.down;
            this.treeWidth = CLUSTER_TREE_DEFAULTS.width;
        }
    },

    saveTreeSettings() {
        try {
            localStorage.setItem('clusterTreeSettings', JSON.stringify({
                up: this.treeUp, down: this.treeDown, width: this.treeWidth,
            }));
        } catch (e) { /* private window or blocked storage: setting just won't persist */ }
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

    /** Same call, whole response (slice needs center_cluster_id / hidden_above too). */
    async fetchClustersRaw(extra) {
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
        return res.json();
    },

    /** Re-center the tree window on `uuid`: `treeUp` ancestors, `treeDown` levels
     * below, `treeWidth` children per node -- one cheap server call
     * (cluster_utils.cluster_tree_slice), not a full-collection scan. */
    async fetchSlice(uuid) {
        this.childrenMap = {};
        this.childTotal = {};
        this.childrenLoaded.clear();
        this.treeExpanded.clear();
        this.treeAncestors = [];
        this.hiddenAbove = 0;

        const data = await this.fetchClustersRaw({
            slice: uuid, up: String(this.treeUp), down: String(this.treeDown), width: String(this.treeWidth),
        });
        this.ingest(data.results || []);
        this.hiddenAbove = data.hidden_above || 0;

        const centerId = data.center_cluster_id ? String(data.center_cluster_id) : null;
        if (!centerId) return null;

        // Walk the now-loaded ancestor chain nearest-first, and mark every
        // loaded node (path + auto-fetched descendants) expanded by default.
        let curr = this.clusterMapById[centerId];
        while (curr && curr.parent && this.clusterMapById[String(curr.parent)]) {
            this.treeAncestors.push(String(curr.parent));
            this.treeExpanded.add(String(curr.parent));
            curr = this.clusterMapById[String(curr.parent)];
        }
        this.treeExpanded.add(centerId);
        Object.keys(this.childrenMap).forEach(pid => {
            this.childrenLoaded.add(pid);
            this.treeExpanded.add(pid);
        });
        this._sliceCenteredOn = uuid;
        return centerId;
    },

    /** Merge fetched clusters into the local maps, newest fields winning. */
    ingest(list) {
        (list || []).forEach(c => {
            const cid = String(c.cluster_id);
            this.clusterMapById[cid] = { ...(this.clusterMapById[cid] || {}), ...c };
            if (c.cluster_uuid) this.clusterMapByUuid[c.cluster_uuid] = this.clusterMapById[cid];
            if (c.child_total !== undefined) this.childTotal[cid] = c.child_total;

            const pid = c.parent ? String(c.parent) : null;
            if (pid) {
                if (!this.childrenMap[pid]) this.childrenMap[pid] = [];
                if (!this.childrenMap[pid].includes(cid)) this.childrenMap[pid].push(cid);
            }
        });
    },

    /** How many of a node's children are loaded/shown vs. its real total. */
    hiddenChildCount(cid) {
        const shown = (this.childrenMap[cid] || []).length;
        const total = this.childTotal[cid] || 0;
        return Math.max(0, total - shown);
    },

    /** Next page of one node's children (top-`treeWidth`, offset = shown so far). */
    async loadMoreChildren(clusterId) {
        const cid = String(clusterId);
        const offset = (this.childrenMap[cid] || []).length;
        try {
            const kids = await this.fetchClusters({ parent: cid, width: String(this.treeWidth), offset: String(offset) });
            this.childrenLoaded.add(cid);
            this.ingest(kids);
            this.treeExpanded.add(cid);
        } catch (e) {
            console.error('Failed to fetch more children of', cid, e);
        }
        this.renderTree();
        this.renderTable();
    },

    /** Reveal one more batch of ancestors above the current window. */
    async loadMoreAbove() {
        this.treeUp += CLUSTER_TREE_DEFAULTS.up;
        await this.fetchSlice(this.selectedClusterUuid);
        this.renderTree();
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        this.destroy();
        this.params = params;
        this.isBinary = params.view === 'bin-cluster-detail';
        this.tab = this.defaultTab();
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
        this.loadTreeSettings();
        try {
            this.sidebarCollapsed = localStorage.getItem('clusterSidebarCollapsed') === '1';
        } catch (e) {
            this.sidebarCollapsed = false;
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
                .bsim-tree-settings {
                    display:flex; gap:12px; padding:0 12px 8px; color:var(--dim); font-size:0.72rem;
                }
                .bsim-tree-settings span { display:flex; align-items:center; gap:4px; }
                .bsim-tree-settings input {
                    width:34px; padding:1px 3px; font-size:0.72rem; text-align:center;
                    background:var(--bg-alt); color:var(--text); border:1px solid var(--border); border-radius:4px;
                }
                #cluster-side-wrap { position:relative; display:flex; flex-shrink:0; margin-right:2px; }
                #cluster-sidebar { transition:width 0.28s ease; }
                /* Slim: the tree stays, names fold into the row tooltip. */
                #cluster-sidebar.collapsed { width:96px; }
                #cluster-sidebar.collapsed .bsim-tree-settings,
                #cluster-sidebar.collapsed .bsim-node-label,
                #cluster-sidebar.collapsed .bsim-side-collapse { display:none; }
                #cluster-sidebar.collapsed .bsim-side-title { padding:4px 8px 8px; }
                #cluster-sidebar.collapsed .bsim-node { padding-right:6px; gap:4px; }
                #cluster-sidebar.collapsed .bsim-node-count { margin-left:auto; }
                .cluster-side-handle {
                    position:absolute; left:100%; top:50%; transform:translateY(-50%);
                    width:16px; height:60px; z-index:5; cursor:pointer;
                    display:flex; align-items:center; justify-content:center;
                    background:var(--card-bg); color:var(--accent); font-size:0.75rem;
                    border:1px solid var(--border); border-left:none; border-radius:0 8px 8px 0;
                    transition:background 0.2s, color 0.2s;
                }
                .cluster-side-handle:hover { background:var(--accent); color:var(--window-tray); }
                .cluster-side-handle i { transition:transform 0.28s ease; }
                #cluster-sidebar.collapsed + .cluster-side-handle i { transform:rotate(180deg); }
                .hero-handle i { transition:transform 0.25s ease; }
                .hero-handle { background:none; border:none; color:var(--dim); cursor:pointer; padding:2px 6px; }
                .hero-handle:hover { color:var(--accent); }
                .cluster-hero { transition:padding 0.25s ease; }
                .hero-fold { display:grid; grid-template-rows:1fr; opacity:1; transition:grid-template-rows 0.25s ease, opacity 0.2s ease; }
                .hero-fold-in { overflow:hidden; min-height:0; }
                .hero-mini { display:none; gap:12px; color:var(--dim); font-size:0.78rem; opacity:0; transition:opacity 0.25s ease; }
                .cluster-hero.compact .hero-handle i { transform:rotate(180deg); }
                .cluster-hero.compact .hero-fold { grid-template-rows:0fr; opacity:0; }
                .cluster-hero.compact .hero-mini { display:inline-flex; opacity:1; }
                .cluster-hero.compact { padding-top:8px !important; padding-bottom:8px !important; }
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

                #cluster-members-table { table-layout:fixed; }
                #cluster-members-table td { padding: 8px 12px; border-bottom: 1px solid var(--border); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
                #cluster-members-table td > * { max-width:100%; }
                #cluster-members-table tr:hover:not(.bsim-grp-row) { background: var(--hover); }
            </style>

            <div id="cluster-loader" style="display:flex; justify-content:center; align-items:center; height:100%; color:var(--dim);">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading cluster...
            </div>

            <div id="cluster-main" style="display:none;">
                <div id="cluster-side-wrap">
                <div id="cluster-sidebar" class="${this.sidebarCollapsed ? 'collapsed' : ''}">
                    <div class="bsim-side-title">
                        <span class="bsim-side-label">Cluster Hierarchy</span>
                        <span class="bsim-side-actions">
                            <span class="bsim-side-collapse" onclick="ClusterDetailView.collapseTreeAll()" title="Collapse back to this cluster">collapse</span>
                        </span>
                    </div>
                    <div class="bsim-tree-settings" title="Ancestors shown / levels below / children per node">
                        <span><i class="fa-solid fa-angle-up"></i> <input type="number" min="1" max="50" value="${this.treeUp}" onchange="ClusterDetailView.updateTreeSetting('up', this.value)"></span>
                        <span><i class="fa-solid fa-angle-down"></i> <input type="number" min="1" max="50" value="${this.treeDown}" onchange="ClusterDetailView.updateTreeSetting('down', this.value)"></span>
                        <span><i class="fa-solid fa-arrows-left-right"></i> <input type="number" min="1" max="50" value="${this.treeWidth}" onchange="ClusterDetailView.updateTreeSetting('width', this.value)"></span>
                    </div>
                    <div id="cluster-tree" class="bsim-tree"></div>
                </div>
                <div class="cluster-side-handle" onclick="ClusterDetailView.toggleSidebar()" title="Slim down / widen the cluster hierarchy"><i class="fa-solid fa-chevron-left"></i></div>
                </div>

                <div id="cluster-detail" style="flex:1; min-width:0; display:flex; flex-direction:column; min-height:0;">
                    <div id="cluster-header"></div>

                    <div class="bsim-tabbar" id="cluster-view-tabs" style="margin:20px 0 16px; flex-shrink:0;">
                        ${this.isBinary ? '' : `<button class="bsim-tab active" id="cluster-tab-btn-medoid" onclick="ClusterDetailView.switchTab('medoid')" title="Most central function of this cluster">★ Medoid</button>`}
                        <button class="bsim-tab${this.isBinary ? ' active' : ''}" id="cluster-tab-btn-members" onclick="ClusterDetailView.switchTab('members')">Members</button>
                        <button class="bsim-tab" id="cluster-tab-btn-metadata" onclick="ClusterDetailView.switchTab('metadata')">Metadata</button>
                        ${this.hasFunctionsTab() ? `<button class="bsim-tab" id="cluster-tab-btn-functions" onclick="ClusterDetailView.switchTab('functions')">Functions</button>` : ''}
                    </div>

                    <div id="cluster-tab-members" style="display:${this.isBinary ? 'flex' : 'none'}; flex-direction:column; flex:1; min-height:0;">
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
                                    <colgroup>
                                        <col style="width:${this.isBinary ? '40%' : '50%'};"><col style="width:270px;"><col><col style="width:${this.isBinary ? '100px' : '0'};">
                                    </colgroup>
                                    <thead style="position:sticky; top:0; background:var(--card-bg); z-index:10;">
                                        <tr style="border-bottom:1px solid var(--border); color:var(--dim);">
                                            <th style="padding:8px 12px; text-align:left;">${this.isBinary ? 'File Name' : 'Function'}</th>
                                            <th style="padding:8px 12px; text-align:left;">MD5</th>
                                            <th style="padding:8px 12px; text-align:left;">${this.isBinary ? 'Arch' : 'File Name'}</th>
                                            ${this.isBinary ? `<th style="padding:8px 12px; text-align:right; cursor:pointer; user-select:none;" id="cluster-centrality-th" onclick="ClusterDetailView.toggleCentralitySort()" title="Mean similarity to the other files of this cluster">Centrality</th>` : ''}
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
                    <div id="cluster-tab-functions" style="display:none; flex:1; min-height:0; overflow:auto;"></div>
                    <div id="cluster-tab-medoid" style="display:${this.isBinary ? 'none' : 'flex'}; flex:1; min-height:0; flex-direction:column;"></div>
                </div>
            </div>
        `;

        try {
            let centerId = await this.fetchSlice(uuid);
            // Each score axis clusters into its own namespace, but a uuid is
            // unique across them: a link that carries no axis probes the
            // others rather than reporting the cluster missing.
            if (this.isBinary && !explicitAxis && !centerId) {
                for (const ax of ['overall', 'code', 'library', 'content']) {
                    if (ax === this.axis) continue;
                    this.axis = ax;
                    centerId = await this.fetchSlice(uuid);
                    if (centerId) break;
                }
            }

            const self = this.clusterMapById[centerId];
            if (!self) {
                container.innerHTML = `<div style="padding:30px; color:var(--dim);">No cluster matching <code>${escapeHtml(uuid)}</code>.</div>`;
                return;
            }

            this.selectedClusterUuid = self.cluster_uuid;
            this._sliceCenteredOn = self.cluster_uuid;

            document.getElementById('cluster-loader').style.display = 'none';
            document.getElementById('cluster-main').style.display = 'flex';
            document.getElementById('cluster-detail').addEventListener('scroll', e => this.onDetailScroll(e), true);

            this.renderTree();
            await this.selectNode(this.selectedClusterUuid);

            if (window.TableSelection) new window.TableSelection('cluster-members-table');
            const urlTab = urlParams.get('tab');
            if (urlTab && urlTab !== this.tab && document.getElementById(`cluster-tab-btn-${urlTab}`)) this.switchTab(urlTab);
        } catch (e) {
            console.error(e);
            container.innerHTML = `<div style="padding:30px; color:#f92672;">
                <i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(e.message)}</div>`;
        }
    },

    toggleSidebar() {
        this.sidebarCollapsed = !this.sidebarCollapsed;
        try {
            localStorage.setItem('clusterSidebarCollapsed', this.sidebarCollapsed ? '1' : '0');
        } catch (e) {}
        const el = document.getElementById('cluster-sidebar');
        if (el) el.classList.toggle('collapsed', this.sidebarCollapsed);
    },

    setHero(compact) {
        this.heroCompact = compact;
        const el = document.getElementById('cluster-hero');
        if (el) el.classList.toggle('compact', compact);
    },

    /** The handle: a manual choice wins over the scroll rule. */
    toggleHero() {
        this.heroManual = true;
        this.setHero(!this.heroCompact);
    },

    /** Scrolling a table down folds the hero card to its title row; back at the
     * top it unfolds. Only vertical movement counts (a sideways scroll keeps
     * scrollTop), and the 40px gap keeps the layout shift from re-triggering. */
    onDetailScroll(e) {
        const t = e.target;
        if (this.heroManual || !t || t.id === 'cluster-detail' || typeof t.scrollTop !== 'number') return;
        this._lastTop = this._lastTop || new WeakMap();
        const prev = this._lastTop.get(t) || 0;
        this._lastTop.set(t, t.scrollTop);
        if (t.scrollTop === prev) return;
        if (t.scrollTop > 40 && !this.heroCompact) this.setHero(true);
        else if (t.scrollTop === 0 && this.heroCompact) this.setHero(false);
    },

    collapseTreeAll() {
        this.fetchSlice(this.selectedClusterUuid).then(() => this.renderTree());
    },

    updateTreeSetting(key, value) {
        const n = Math.max(1, Math.min(50, parseInt(value, 10) || CLUSTER_TREE_DEFAULTS[key]));
        this[key === 'up' ? 'treeUp' : key === 'down' ? 'treeDown' : 'treeWidth'] = n;
        this.saveTreeSettings();
        this.fetchSlice(this.selectedClusterUuid).then(() => this.renderTree());
    },

    /** Ancestors + the selected node itself: always expanded, never collapse --
     * their caret instead pages in more of their children (siblings of the path). */
    isTreePathNode(id) {
        const centerId = this.clusterMapByUuid[this.selectedClusterUuid]?.cluster_id;
        return this.treeAncestors.includes(id) || id === String(centerId);
    },

    async toggleTreeNode(clusterId, event) {
        event.stopPropagation();
        const id = String(clusterId);
        if (this.isTreePathNode(id)) {
            await this.loadMoreChildren(id);
            return;
        }
        if (this.treeExpanded.has(id)) {
            this.treeExpanded.delete(id);
            this.renderTree();
            return;
        }
        this.treeExpanded.add(id);
        this.renderTree();
        if (!this.childrenLoaded.has(id)) {
            await this.loadMoreChildren(id);
        }
    },

    /** Does this node have children, loaded or not? */
    hasChildren(c, cid) {
        if ((this.childrenMap[cid] || []).length > 0) return true;
        return !this.childrenLoaded.has(cid) && !!(c && c.has_children);
    },

    renderTree() {
        const treeContainer = document.getElementById('cluster-tree');
        if (!treeContainer) return;

        let html = '';
        if (this.hiddenAbove > 0) {
            html += `<div class="bsim-node" style="color:var(--accent); font-size:0.72rem;" onclick="ClusterDetailView.loadMoreAbove()">
                <i class="fa-solid fa-angles-up" style="width:12px;"></i> ${this.hiddenAbove} more above…</div>`;
        }

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
                <div class="bsim-node ${isSelected ? 'selected' : ''}" title="${escapeAttr(c.cluster_name || `Cluster #${c.cluster_id}`)}" style="padding-left: ${12 + depth * 16}px;" onclick="ClusterDetailView.selectNode('${c.cluster_uuid}')">
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
                    for (const childId of children) {
                        rowHtml += buildHtml(childId, depth + 1);
                    }
                    const hidden = this.hiddenChildCount(cid);
                    if (hidden > 0) {
                        rowHtml += `<div class="bsim-node" style="padding-left:${28 + depth * 16}px; color:var(--accent); font-size:0.72rem;" onclick="ClusterDetailView.loadMoreChildren('${cid}')">
                            + ${hidden} more…</div>`;
                    }
                }
            }

            return rowHtml;
        };

        // buildHtml already recurses through every loaded/expanded child, so
        // one call on the topmost visible node draws the whole path down
        // through the selected cluster to its descendants -- calling it again
        // per ancestor (or once more for the center) would draw each level's
        // subtree twice.
        const path = [...this.treeAncestors].reverse(); // furthest-first
        const centerId = this.clusterMapByUuid[this.selectedClusterUuid]?.cluster_id;
        const topId = path.length ? path[0] : (centerId !== undefined ? String(centerId) : null);
        if (topId !== null) html += buildHtml(topId, 0);

        treeContainer.innerHTML = html;
    },

    setBreadcrumb(self) {
        if (typeof Breadcrumbs === 'undefined' || !Breadcrumbs.refresh) return;
        Breadcrumbs.setClusterName(self.cluster_uuid, self.cluster_name || `#${self.cluster_id}`);
        Breadcrumbs.refresh();
    },

    async selectNode(uuid) {
        // Re-center the tree window on the newly selected node -- unless the
        // caller (init) already loaded a slice centered here.
        if (this._sliceCenteredOn !== uuid) {
            await this.fetchSlice(uuid);
            this._sliceCenteredOn = uuid;
        }
        this.selectedClusterUuid = uuid;
        const c = this.clusterMapByUuid[uuid];
        if (!c) return;

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
        this.renderMedoidCtl();

        // Fetch members if not cached
        if (!this.memberCache[uuid]) {
            await this.fetchMembers(uuid);
        }

        this.renderTable();
        if (this.tab === 'metadata') this.renderMetadataTab();
        if (this.tab === 'functions') this.renderFunctionsTab();
        if (this.tab === 'medoid') this.renderMedoidTab();
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
        ['members', 'metadata', 'functions', 'medoid'].forEach(t => {
            const btn = document.getElementById(`cluster-tab-btn-${t}`);
            if (!btn) return;
            btn.classList.toggle('active', t === tab);
            document.getElementById(`cluster-tab-${t}`).style.display =
                t === tab ? (t === 'members' || t === 'medoid' ? 'flex' : 'block') : 'none';
        });
        const url = new URL(window.location.href);
        if (tab !== this.defaultTab()) url.searchParams.set('tab', tab);
        else url.searchParams.delete('tab');
        window.history.replaceState({ path: url.pathname + url.search }, '', url.pathname + url.search);
        if (tab === 'metadata') this.renderMetadataTab();
        if (tab === 'functions') this.renderFunctionsTab();
        if (tab === 'medoid') this.renderMedoidTab();
    },

    /** Function clusters: the medoid's code view, embedded as-is (in-iframe mode hides the app chrome). */
    async renderMedoidTab() {
        const el = document.getElementById('cluster-tab-medoid');
        const uuid = this.selectedClusterUuid;
        if (!el || !uuid) return;
        let c = this.clusterMapByUuid[uuid];
        if (c && c.medoid === undefined) {
            el.innerHTML = `<div class="dim" style="padding:20px;"><i class="fa-solid fa-spinner fa-spin"></i> Finding the most central function…</div>`;
            try {
                this.ingest(await this.fetchClusters({ cluster_uuid: uuid, with_medoid: 'true', limit: '1' }));
            } catch (e) {
                console.error('Failed to load medoid', e);
            }
            if (this.tab !== 'medoid' || this.selectedClusterUuid !== uuid) return;
            c = this.clusterMapByUuid[uuid];
        }
        if (!c || !c.medoid) {
            el.innerHTML = `<div class="dim" style="padding:20px;">No medoid for this cluster.</div>`;
            return;
        }
        const f = window.parseFuncId(c.medoid);
        let data;
        try {
            const res = await fetch(`/api/function/code?id=${encodeURIComponent(`idx:${f.collection}:func:${f.md5}:${f.address}`)}`);
            if (!res.ok) throw new Error(`Function not found (${res.status})`);
            data = await res.json();
        } catch (e) {
            el.innerHTML = `<div style="padding:20px; color:var(--error);">${escapeHtml(e.message)}</div>`;
            return;
        }
        if (this.tab !== 'medoid' || this.selectedClusterUuid !== uuid) return;

        // Same metadata card and line markup as the code view (FunctionView.renderRows),
        // ponytail: not virtualized -- fine for one function, port FunctionView's scroller if huge ones crawl.
        const cen = c.medoid_centrality == null ? '---' : Number(c.medoid_centrality).toFixed(3);
        const lines = (data.rows || []).map(row =>
            `<div class="code-line"><div class="gutter" contenteditable="false"><div class="line-num">${Number(row.line_idx)}</div></div><div class="line-content">${row.tokens.map(t => window.renderTokenHtml(t)).join('')}</div></div>`
        ).join('');
        el.innerHTML = `
            <div class="dim" style="font-size:0.78rem; margin-bottom:6px; flex-shrink:0;" title="Mean stored similarity to the other members (a pair BSim never kept counts as 0)">★ Most central function · centrality ${escapeHtml(cen)}</div>
            <div id="cluster-medoid-meta" style="flex-shrink:0;"></div>
            <div id="cluster-medoid-code" style="flex:1; min-height:300px; overflow:auto; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">
                <div class="c-code-container">${lines}</div>
            </div>`;
        if (window.renderFunctionMetadata) {
            window.renderFunctionMetadata('cluster-medoid-meta', data.meta, c.medoid, { showFeaturesBtn: true, showDiffBtn: true, diffBtnFullText: false, showSimilarBtn: true });
        }
        const code = document.getElementById('cluster-medoid-code');
        if (window.applyLocks) window.applyLocks(code);
        code.onclick = e => {
            const token = e.target.closest('.token');
            if (!token) return;
            const called = token.getAttribute('data-called-func-id');
            if (called && token.getAttribute('data-is-external') !== 'true') showFunctionCodeById(called, token.getAttribute('data-target-name') || '', '', e);
            else if (token.getAttribute('data-hashes') && window.toggleLock) window.toggleLock(token.getAttribute('data-hashes'), token);
        };
    },

    /**
     * N-way diff of the selected node (nway_panel.js). Remounted whenever the
     * node changes; its controls live in the URL (`ntab` is the panel's own tab,
     * `tab=functions` is this view's).
     */
    renderFunctionsTab() {
        const el = document.getElementById('cluster-tab-functions');
        if (!el || !this.selectedClusterUuid || !window.NwayPanel) return;
        if (this._nway) this._nway.destroy();
        const url = new URLSearchParams(window.location.search);
        const keys = ['scope', 'k', 'min_edge', 'mode', 'columns', 'child_presence', 'q', 'tags', 'offset'];
        const state = {};
        keys.forEach(k => { if (url.get(k)) state[k] = url.get(k); });
        if (url.get('ntab')) state.tab = url.get('ntab');
        const source = { cluster_uuid: this.selectedClusterUuid, axis: this.axis };
        if (this.params && this.params.pool) source.pool = this.params.pool;
        else source.collection = this.collection;
        this._nway = new window.NwayPanel(el, {
            source,
            state,
            onState: s => {
                const u = new URL(window.location.href);
                u.searchParams.set('tab', 'functions');
                u.searchParams.set('ntab', s.tab);
                keys.forEach(k => (s[k] === '' || s[k] == null ? u.searchParams.delete(k) : u.searchParams.set(k, s[k])));
                window.history.replaceState({ path: u.pathname + u.search }, '', u.pathname + u.search);
            },
            onOpenCluster: uuid => this.selectNode(uuid),
        });
        this._nway.load();
    },

    /** Secondary-axis cohesion, outside the main card so its tint doesn't clash. */
    renderSecondaryAxes(self) {
        const types = window.BinSimScoreTypes || {};
        const chips = Object.entries(self.cohesion_axes || {})
            .filter(([ax]) => types[ax === 'overall' ? 'score' : `score_${ax}`])
            .sort((a, b) => b[1] - a[1])
            .map(([ax, v]) => {
                const t = types[ax === 'overall' ? 'score' : `score_${ax}`];
                return `<div title="${escapeAttr(t.label)} cohesion" style="display:flex; align-items:center; gap:6px; font-size:0.75rem; font-weight:600; color:${t.color};">
                    <i class="${t.icon}"></i><span>${escapeHtml(t.label)}</span><span>${(v * 100).toFixed(0)}%</span></div>`;
            }).join('');
        return chips ? `<div style="display:flex; flex-direction:column; gap:3px; margin-top:16px;">${chips}</div>` : '';
    },

    renderHeader(self) {
        const stat = (label, value) => `
            <div style="min-width:110px;">
                <div class="dim" style="font-size:0.65rem; text-transform:uppercase; letter-spacing:1px;">${label}</div>
                <div style="font-size:1.1rem; font-weight:bold;">${value}</div>
            </div>`;

        const axisKey = { overall: 'score', code: 'score_code', library: 'score_library', content: 'score_content' }[this.axis] || 'score';
        const scoreType = (window.BinSimScoreTypes && window.BinSimScoreTypes[axisKey]) || { color: 'var(--accent)' };
        return `
        <style>
            .cluster-axis-score-card { width:fit-content; min-width:250px; margin-top:16px; padding:12px 16px; border:1px solid color-mix(in srgb, var(--cluster-score-color) 45%, var(--border)); border-left:4px solid var(--cluster-score-color); border-radius:7px; background:color-mix(in srgb, var(--cluster-score-color) 10%, var(--card-bg)); }
            .cluster-axis-score-card > div > div:first-child { gap:10px !important; }
            .cluster-axis-score-card > div > div:first-child span:first-of-type { font-size:0.9rem !important; }
            .cluster-axis-score-card > div > div:first-child span:last-of-type { font-size:2rem !important; transition:font-size 0.25s ease; }
            .cluster-axis-score-card { transition:margin 0.25s ease, padding 0.25s ease; }
            /* Folded: the main score stays, just smaller. */
            .cluster-hero.compact .cluster-axis-score-card { margin-top:8px; padding:5px 14px; min-width:0; }
            .cluster-hero.compact .cluster-axis-score-card > div > div:first-child span:last-of-type { font-size:1.3rem !important; }
        </style>
        <div id="cluster-hero" class="cluster-hero${this.heroCompact ? ' compact' : ''}" style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:16px 20px;">
            <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                <i class="fa-solid fa-bullseye" style="color:var(--accent);"></i>
                <span style="font-size:1.2rem; font-weight:bold;">${escapeHtml(self.cluster_name || `Cluster #${self.cluster_id}`)}</span>
                <span class="badge">${this.isBinary ? this.axis : 'function'}</span>
                <span class="hero-mini"><span>${Number(self.count || 0).toLocaleString()} members</span></span>
                ${EntityRenderer.renderTag(this.isBinary ? 'bin_cluster' : 'cluster', self.tag_id || self.cluster_id, [], self.user_tags || [])}
                <span style="margin-left:auto; display:flex; gap:6px; align-items:center;">${this.renderMemberListLink(self)}${this.renderSimilaritiesLink(self)}<button class="hero-handle" onclick="ClusterDetailView.toggleHero()" title="Fold / unfold the details (folds by itself while you scroll a table)"><i class="fa-solid fa-chevron-up"></i></button></span>
            </div>
            <div class="hero-fold"><div class="hero-fold-in">
            <div class="mono dim" style="font-size:0.72rem; margin-top:6px;">
                ${escapeHtml(self.cluster_uuid || '')}
                <button class="btn-copy" title="Copy UUID" onclick="copyToClipboard(${escapeAttr(jsString(self.cluster_uuid || ''))}, this)"><i class="fa-regular fa-copy"></i></button>
            </div>
            <div style="display:flex; gap:26px; flex-wrap:wrap; margin-top:14px;">
                ${stat('Members', Number(self.count || 0).toLocaleString())}
                ${stat('Stability', Number(self.avg_stability || 0).toFixed(2))}
                ${stat('Avg features', Number(self.avg_features || 0).toFixed(0))}
                ${stat('Cluster ID', escapeHtml(String(self.cluster_id)))}
            </div>
            ${this.isBinary ? `<div id="cluster-medoid-ctl" style="font-size:0.8rem; margin-top:12px;"></div>` : ''}
            </div></div>
            ${this.isBinary ? `<div style="display:flex; align-items:center; gap:18px; flex-wrap:wrap;">
                <div class="cluster-axis-score-card" style="--cluster-score-color:${scoreType.color};">${binSimScoreCards({ [axisKey]: self.cohesion_score }, axisKey)}</div>
                <div class="hero-fold"><div class="hero-fold-in">${this.renderSecondaryAxes(self)}</div></div>
            </div>` : ''}
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
                    ? `<div class="metadata-axis-list">${cards}</div>`
                    : `<div class="dim" style="padding:20px; text-align:center;">No metadata distributions for this cluster.${this.isBinary ? ' A cluster below the configured min_cohesion reports none.' : ''}</div>`}
            </div>`;
        if (window.initMetadataTables) window.initMetadataTables(panel);
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
        return UI.Button.render({
            className: 'btn-code-action',
            icon: 'fa-solid fa-magnifying-glass',
            label: `Open in ${this.isBinary ? 'file' : 'function'} search`,
            onClick: `Nav.openPath(${jsString(url)}, event)`,
        });
    },

    renderSimilaritiesLink(self) {
        const segs = this.isBinary ? ['files', 'similarities'] : ['functions', 'similarities'];
        const key = this.isBinary ? 'bin_cluster_uuid' : 'cluster_uuid';
        const url = `${Nav.buildUIUrl(this.collection || '', segs)}?${key}=${encodeURIComponent(self.cluster_uuid)}`;
        return UI.Button.render({
            className: 'btn-code-action',
            icon: 'fa-solid fa-diagram-project',
            label: `Open ${this.isBinary ? 'file' : 'function'} similarities`,
            onClick: `Nav.openPath(${jsString(url)}, event)`,
        });
    },

    async toggleGroup(uuid) {
        if (this.expandedGroups.has(uuid)) {
            this.expandedGroups.delete(uuid);
        } else {
            this.expandedGroups.add(uuid);
            const c = this.clusterMapByUuid[uuid];
            if (c && !this.childrenLoaded.has(String(c.cluster_id))) await this.loadMoreChildren(c.cluster_id);
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

    toggleCentralitySort() {
        this.centralitySort = !this.centralitySort;
        const th = document.getElementById('cluster-centrality-th');
        if (th) th.textContent = this.centralitySort ? 'Centrality ▼' : 'Centrality';
        this.renderTable();
    },

    /** Main file (medoid): a link plus its essentials; the hint shows for clusters built before centrality. */
    async renderMedoidCtl() {
        const el = document.getElementById('cluster-medoid-ctl');
        if (!el || !this.isBinary) return;
        const uuid = this.selectedClusterUuid;
        const medoid = (this.clusterMapByUuid[uuid] || {}).medoid;
        if (!medoid) {
            el.innerHTML = `<span class="dim">rebuild clusters to compute centrality</span>`;
            return;
        }
        const parts = String(medoid).split(':file:');
        const col = parts.length > 1 ? parts[0] : this.collection || '';
        const md5 = parts[parts.length - 1];
        this._medoidFiles = this._medoidFiles || {};
        if (!this._medoidFiles[medoid]) {
            try {
                const qs = new URLSearchParams({ collection: col });
                if (this.params && this.params.pool) qs.set('pool', this.params.pool);
                const res = await fetch(`/api/file/details/${encodeURIComponent(md5)}?${qs.toString()}`);
                this._medoidFiles[medoid] = (res.ok && (await res.json()).file) || {};
            } catch (e) {
                this._medoidFiles[medoid] = {};
            }
            if (this.selectedClusterUuid !== uuid) return;
        }
        const f = this._medoidFiles[medoid];
        const bits = [f.language_id, f.executable_format, f.function_count != null ? `${Number(f.function_count).toLocaleString()} functions` : null]
            .filter(Boolean).map(b => `<span>${escapeHtml(String(b))}</span>`).join(' · ');
        el.innerHTML = `<span title="Most central file of this cluster" style="display:inline-flex; align-items:center; gap:8px;">
            <span style="color:var(--accent);">★ Main file</span>
            ${EntityRenderer.renderFileName(f.file_name || md5, md5, col)}
            ${EntityRenderer.renderMd5(md5, { collection: col })}
            <span class="dim">${bits}</span></span>`;
    },

    async renderFlatMembers(tbody) {
        tbody.innerHTML = `<tr><td colspan="4" class="dim" style="padding:20px; text-align:center;"><i class="fa-solid fa-spinner fa-spin"></i> Loading full membership...</td></tr>`;
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
                tbody.innerHTML = `<tr><td colspan="4" class="dim" style="padding:20px; text-align:center;">No members found in search index.</td></tr>`;
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
                tr.innerHTML = `<td colspan="4" class="dim" style="padding:15px; text-align:center; font-style:italic;">Showing first 1000 members out of ${data.total}. ${this.renderMemberListLink(this.clusterMapByUuid[this.selectedClusterUuid])}</td>`;
                tbody.appendChild(tr);
            }

        } catch (e) {
            console.error(e);
            tbody.innerHTML = `<tr><td colspan="4" style="padding:20px; text-align:center; color:var(--error);"><i class="fa-solid fa-circle-exclamation"></i> Error loading members: ${e.message}</td></tr>`;
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
                <td colspan="4" style="padding-left: ${12 + depth * 20}px;">
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
                    <td colspan="4" style="padding-left: ${12 + targetDepth * 20}px; opacity: 0.9;">
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
            tr.innerHTML = `<td colspan="4" class="dim" style="padding-left: ${12 + targetDepth * 20}px; font-style:italic;">No direct members</td>`;
            tbody.appendChild(tr);
        }

        // 2b. Children -- already sorted count-desc and capped by the server (see cluster_utils.cluster_children_page)
        for (const childId of children) {
            const child = this.clusterMapById[childId];
            if (child) {
                this.renderHierarchicalGroups(tbody, child.cluster_uuid, targetDepth);
            }
        }
        const hiddenChildren = this.hiddenChildCount(String(c.cluster_id));
        if (hiddenChildren > 0) {
            const tr = document.createElement('tr');
            tr.className = 'bsim-grp-row';
            tr.onclick = () => this.loadMoreChildren(c.cluster_id);
            tr.innerHTML = `<td colspan="4" style="padding-left: ${12 + targetDepth * 20}px; color:var(--accent);">+ ${hiddenChildren} more groups…</td>`;
            tbody.appendChild(tr);
        }
    },

    renderMembersList(tbody, members, depth) {
        const col = this.collection || '';
        const medoid = (this.clusterMapByUuid[this.selectedClusterUuid] || {}).medoid;
        if (this.centralitySort) {
            members = [...members].sort((a, b) => (b.centrality ?? -1) - (a.centrality ?? -1));
        }

        members.forEach(m => {
            // A bare md5 id (flat file search) has no collection to split out.
            const qualified = String(m.id || '').includes(':');
            const memberCol = (qualified && String(m.id).split(':')[0]) || col;
            const md5 = m.file_md5 || '';
            const tr = document.createElement('tr');
            tr.setAttribute('data-id', escapeAttr(qualified ? m.id : this.isBinary ? `${memberCol}:file:${md5}` : m.id || md5));

            let c1, c2, c3;
            if (this.isBinary) {
                c1 = EntityRenderer.renderFileName(m.name || '', md5, memberCol);
                if (medoid && m.id === medoid) c1 = `<span title="Main file (medoid)" style="color:var(--accent);">★</span> ${c1}`;
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
                ${this.isBinary ? `<td style="text-align:right;" class="dim">${m.centrality == null ? '---' : escapeHtml(Number(m.centrality).toFixed(3))}</td>` : ''}
            `;
            tbody.appendChild(tr);
        });
    }
};
