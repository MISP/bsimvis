/**
 * Shared controller behind every Pivotick call-graph surface (function view's
 * Call Graph tab, the notes side panel, diff view's left/right graphs) so
 * they all behave the same way: same UI mode, same clustering, same
 * expand/collapse, same notes sync, same "no duplicates, detect every
 * connection already in the graph" behavior.
 *
 * Keeps its own canonical {nodes, edges} model (plain Maps) as the source of
 * truth instead of reading it back out of the live Pivotick instance --
 * once nodes are grouped into a Pivotick cluster/parent node, their children
 * live in a nested sub-graph internal to Pivotick and are no longer visible
 * to top-level queries, so the model has to be kept separately to stay
 * rebuildable.
 *
 * Pure per-node helpers (clustering shape, notes fetch, legend, edge labels,
 * node rendering) still live on FunctionView (bsimvis/app/static/js/views/
 * function_view.js) and are reused here, not duplicated.
 */
class PivotickGraphController {
    constructor(container, opts = {}) {
        this.container = container;
        this.opts = opts; // { collection?: string }
        this.nodes = new Map();   // id -> { raw, kind, depth, expandedFrom? }
        this.edges = new Map();   // id -> { from, to, kind: 'call'|'similarity', score? }
        this.centerId = null;
        this.pInstance = null;
        this._simEdgesEnabled = true;
        this._clusteringActive = false;

        this._noteListener = (e) => this._onNoteChanged(e.detail);
        window.addEventListener('bsimvis:note-changed', this._noteListener);
    }

    getCollection() {
        return this.opts.collection || (window.getRoutingState ? window.getRoutingState().collection : '') || '';
    }

    distinctBinaryCount() {
        const md5s = new Set();
        for (const n of this.nodes.values()) {
            if (n.kind !== 'external' && n.raw?.file_md5) md5s.add(n.raw.file_md5);
        }
        return md5s.size;
    }

    // ---- model mutation -----------------------------------------------

    // Fetches one function's own call graph (itself + its depth-1 callers/
    // callees) and folds it into the model. Every function added this way --
    // whether it's the graph's original center or a function dropped in
    // later -- gets the same one-hop treatment, which is the "functions
    // treated equally" behavior.
    async addFunction(id, { asCenter = false } = {}) {
        if (this.nodes.has(id) && !asCenter) return;

        const res = await fetch(`/api/function/call_graph?id=${encodeURIComponent(id)}`);
        if (!res.ok) throw new Error('Call graph not found');
        const data = await res.json();
        const nodeId = data.node.id;

        if (asCenter || !this.centerId) this.centerId = nodeId;

        const newIds = [];
        const upsert = (fid, raw, kind, extra = {}) => {
            if (this.nodes.has(fid)) return false;
            this.nodes.set(fid, { raw, kind, depth: extra.depth ?? 1, expandedFrom: extra.expandedFrom });
            newIds.push(fid);
            return true;
        };

        if (!this.nodes.has(nodeId)) {
            upsert(nodeId, data.node, nodeId === this.centerId ? 'self' : 'added', { depth: 0 });
        } else if (nodeId === this.centerId) {
            this.nodes.get(nodeId).kind = 'self';
        }

        for (const c of data.callers || []) {
            upsert(c.id, c, c.is_external ? 'external' : 'caller', { expandedFrom: nodeId });
            this._addEdge(c.id, nodeId, 'call');
        }
        for (const c of data.callees || []) {
            upsert(c.id, c, c.is_external ? 'external' : 'callee', { expandedFrom: nodeId });
            this._addEdge(nodeId, c.id, 'call');
        }

        await this.refreshRelations(newIds.length ? newIds : [nodeId]);
        await this.render();
        if (this._simEdgesEnabled) this._discoverSimilar([nodeId, ...newIds]);
    }

    async expandFunction(id) {
        const entry = this.nodes.get(id);
        if (!entry || entry.kind === 'external') return;
        const depth = entry.depth ?? 1;

        const res = await fetch(`/api/function/call_graph?id=${encodeURIComponent(id)}`);
        if (!res.ok) return;
        const data = await res.json();
        const newDepth = depth + 1;
        const newIds = [];

        for (const c of data.callers || []) {
            if (!this.nodes.has(c.id)) {
                this.nodes.set(c.id, { raw: c, kind: c.is_external ? 'external' : 'caller', depth: newDepth, expandedFrom: id });
                newIds.push(c.id);
            }
            this._addEdge(c.id, id, 'call');
        }
        for (const c of data.callees || []) {
            if (!this.nodes.has(c.id)) {
                this.nodes.set(c.id, { raw: c, kind: c.is_external ? 'external' : 'callee', depth: newDepth, expandedFrom: id });
                newIds.push(c.id);
            }
            this._addEdge(id, c.id, 'call');
        }
        entry.expanded = true;

        await this.refreshRelations(newIds.length ? newIds : [id]);
        await this.render();
        if (this._simEdgesEnabled && newIds.length) this._discoverSimilar(newIds);
    }

    collapseFunction(id) {
        const entry = this.nodes.get(id);
        if (!entry) return;
        const currentDepth = entry.depth ?? 1;
        const toRemove = [];
        for (const [nid, n] of this.nodes) {
            if (n.expandedFrom === id || (n.depth > currentDepth && this._isConnectedTo(nid, id))) {
                toRemove.push(nid);
            }
        }
        for (const rid of toRemove) this.removeFunction(rid, { skipRender: true });
        entry.expanded = false;
        this.render();
    }

    removeFunction(id, { skipRender = false } = {}) {
        this.nodes.delete(id);
        for (const [eid, e] of this.edges) {
            if (e.from === id || e.to === id) this.edges.delete(eid);
        }
        if (!skipRender) this.render();
    }

    // "Compare" context-menu action: an orange dashed edge marking two nodes
    // the user wants to diff, independent of any real call/similarity
    // relationship. Tracked in the model like any other edge so it survives
    // a clusters-changed rebuild instead of disappearing.
    addCompareEdge(id1, id2) {
        if (!this.nodes.has(id1) || !this.nodes.has(id2)) return;
        const id = `diff:${[id1, id2].sort().join('<->')}`;
        if (this.edges.has(id)) return;
        this.edges.set(id, { from: id1, to: id2, kind: 'diff' });
        this.render();
    }

    _isConnectedTo(nid, otherId) {
        for (const e of this.edges.values()) {
            if ((e.from === nid && e.to === otherId) || (e.from === otherId && e.to === nid)) return true;
        }
        return false;
    }

    _addEdge(from, to, kind, extra = {}) {
        const id = kind === 'call' ? `${from}->${to}` : `sim:${[from, to].sort().join('<->')}`;
        if (this.edges.has(id)) return false;
        this.edges.set(id, { from, to, kind, ...extra });
        return true;
    }

    // Asks the backend which of the ids already in the model are actually
    // connected (call or similarity) to anything else already present --
    // this is what gives "no duplicates, detect every connection" instead of
    // only ever looking one hop out from a single center.
    async refreshRelations(newIds) {
        if (this.nodes.size < 2) return;
        const collection = this.getCollection();
        if (!collection) return;
        const ids = [...this.nodes.keys()].filter(id => !id.startsWith('ext:'));
        if (ids.length < 2) return;

        const qs = new URLSearchParams();
        qs.set('ids', ids.join(','));
        qs.set('collection', collection);
        if (newIds && newIds.length && newIds.length < ids.length) qs.set('new_ids', newIds.join(','));

        try {
            const res = await fetch(`/api/function/relations?${qs.toString()}`);
            if (!res.ok) return;
            const data = await res.json();
            for (const e of data.call_edges || []) {
                if (this.nodes.has(e.from) && this.nodes.has(e.to)) this._addEdge(e.from, e.to, 'call');
            }
            for (const e of data.sim_edges || []) {
                if (this.nodes.has(e.id1) && this.nodes.has(e.id2)) {
                    this._addEdge(e.id1, e.id2, 'similarity', { score: e.score });
                }
            }
        } catch (err) {
            console.error('Failed to refresh graph relations:', err);
        }
    }

    // Opt-in "find more functions like this one" -- discovers new nodes
    // (unlike refreshRelations, which only wires up what's already present)
    // and always funnels them through addFunction so they dedup normally.
    //
    // In dense corpora (e.g. many near-identical malware samples) every node
    // can have a dozen 90%+ matches -- firing this for center + every
    // caller/callee on a single load flooded graphs with 100+ loose nodes,
    // one per near-duplicate binary, which is what actually produced the
    // messy clusters (force-laid-out internals with way too many members),
    // not a lack of layout options. Capped per-node and per-batch so a
    // normal load stays small; MAX_AUTO_SIMILAR_PER_BATCH is a blunt but
    // simple backstop -- revisit if users want more auto-pulled in by default.
    static MAX_SIMILAR_PER_NODE = 3;
    static MAX_AUTO_SIMILAR_PER_BATCH = 15;

    async _discoverSimilar(ids) {
        let added = 0;
        for (const id of ids) {
            if (added >= PivotickGraphController.MAX_AUTO_SIMILAR_PER_BATCH) {
                if (window.showToast) window.showToast('More similar functions exist -- use the Similar tab to explore them all', 'info');
                break;
            }
            const entry = this.nodes.get(id);
            if (!entry || entry.kind === 'external') continue;
            const collection = this.getCollection();
            const md5 = entry.raw.file_md5;
            const address = entry.raw.entrypoint || entry.raw.address;
            if (!collection || !md5 || !address) continue;
            try {
                const res = await fetch(`/api/similarity/search?md5=${encodeURIComponent(md5)}&address=${encodeURIComponent(address)}&collection=${encodeURIComponent(collection)}&min_score=0.90&limit=${PivotickGraphController.MAX_SIMILAR_PER_NODE}`);
                if (!res.ok) continue;
                const data = await res.json();
                for (const pair of data.pairs || []) {
                    if (added >= PivotickGraphController.MAX_AUTO_SIMILAR_PER_BATCH) break;
                    const isSelf1 = pair.id1 === id;
                    const otherId = isSelf1 ? pair.id2 : pair.id1;
                    const otherMeta = isSelf1 ? pair.meta2 : pair.meta1;
                    const otherName = isSelf1 ? pair.name2 : pair.name1;
                    if (!otherId || !otherMeta || otherId === id || this.nodes.has(otherId)) continue;
                    this.nodes.set(otherId, {
                        raw: {
                            id: otherId, name: otherName, namespace: otherMeta.namespace,
                            return_type: otherMeta.return_type, parameters: otherMeta.parameters,
                            file_md5: otherMeta.file_md5, entrypoint: otherMeta.entrypoint_address,
                            is_external: false,
                        },
                        kind: 'similar', depth: (entry.depth ?? 1) + 1, expandedFrom: id,
                    });
                    this._addEdge(id, otherId, 'similarity', { score: pair.score });
                    added++;
                }
            } catch (err) { console.error('Error discovering similar functions:', err); }
        }
        await this.render();
    }

    async toggleSimilarity(show) {
        this._simEdgesEnabled = show;
        if (!show) {
            for (const [eid, e] of this.edges) {
                if (e.kind === 'similarity') this.edges.delete(eid);
            }
            await this.render();
            return;
        }
        await this._discoverSimilar([...this.nodes.keys()]);
    }

    // Clears the model and re-centers on a different function -- what the
    // side panel's lock toggle gates (locked = never call this on navigation).
    async recenter(newCenterId) {
        this.nodes.clear();
        this.edges.clear();
        this.centerId = null;
        this._clusteringActive = false;
        if (window.getCollectionFromId) {
            const col = window.getCollectionFromId(newCenterId);
            if (col) this.opts.collection = col;
        }
        await this.addFunction(newCenterId, { asCenter: true });
    }

    // ---- rendering ------------------------------------------------------

    _flatEntries() {
        return [...this.nodes.entries()].map(([id, n]) => ({ id, data: { raw: n.raw, kind: n.kind, depth: n.depth, expandedFrom: n.expandedFrom, expanded: n.expanded } }));
    }

    _flatEdges() {
        const STYLE = {
            similarity: { edge: { strokeColor: '#ae81ff', dashed: true, strokeWidth: 2 } },
            diff: { edge: { strokeColor: '#fd971f', dashed: true, strokeWidth: 3 } },
        };
        return [...this.edges.entries()].map(([id, e]) => ({
            id, from: e.from, to: e.to,
            directed: e.kind === 'call',
            data: { kind: e.kind, score: e.score },
            style: STYLE[e.kind],
        }));
    }

    async render() {
        const willCluster = this.distinctBinaryCount() >= 2;
        const needsRebuild = !this.pInstance || willCluster || this._clusteringActive;
        this._clusteringActive = willCluster;

        const flatEntries = this._flatEntries();
        const nodes = FunctionView.buildClusteredNodes(flatEntries);
        const edges = this._flatEdges();

        if (needsRebuild) {
            const positions = new Map();
            if (this.pInstance) {
                for (const n of this.pInstance.getNodes()) {
                    if (typeof n.x === 'number' && typeof n.y === 'number') positions.set(n.id, { x: n.x, y: n.y });
                }
            }
            for (const n of nodes) {
                const pos = positions.get(n.id);
                if (pos) { n.fx = pos.x; n.fy = pos.y; }
            }

            const notes = await FunctionView.fetchGraphNotes(flatEntries);

            if (!this.pInstance) {
                this.pInstance = new Pivotick(this.container, { nodes, edges, notes }, this._pivotickOptions());
                // Outbound (graph note edit -> BSimVis notes store) sync. Wired once
                // at instance creation: setData() clears/repopulates the same Graph
                // object, so this listener survives every later rebuild.
                FunctionView.wireNoteSync(this.pInstance);
                if (typeof window.setupGraphDropTarget === 'function') window.setupGraphDropTarget(this.container, this);
            } else {
                this.pInstance.setData(nodes, edges, notes);
            }
            return;
        }

        // Cluster-preserving path: just add what's new, no recenter/flicker.
        for (const n of nodes) {
            if (!this.pInstance.getNode(n.id)) {
                try { this.pInstance.addNode(n); } catch (e) {}
            }
        }
        for (const e of edges) {
            if (!this.pInstance.edges.has(e.id)) {
                try { this.pInstance.addEdge(e); } catch (e2) {}
            }
        }
        this.pInstance.onChange();
    }

    _pivotickOptions() {
        const self = this;
        return {
            UI: { mode: 'light', tooltip: { enabled: false } },
            // Call graphs are a hierarchy radiating out from the center function,
            // not a free-floating network -- the force layout's default produced
            // a hairball of crossing arrows even on small graphs. Tree-Radial is
            // one of Pivotick's own built-in layouts (View panel > Layout); users
            // can still switch back to Force from there if they prefer it.
            layout: { type: 'tree', horizontal: false },
            simulation: { useWorker: false },
            render: {
                nodeShape: 'rectangle',
                renderNode: (node) => {
                    const d = node.getData() || {};
                    return FunctionView.callGraphRenderNode(d.raw, d.kind);
                },
                renderLabel: (edge) => FunctionView.renderEdgeLabel(edge),
            },
            callbacks: {
                onNodeClick: async (e, node) => {
                    const id = node.id;
                    const d = node.getData() || {};
                    if (d.kind === 'external' || d.raw?.is_external) {
                        if (window.showToast) window.showToast('External node cannot be expanded', 'info');
                        return;
                    }
                    if (id === self.centerId) return;
                    if (d.expanded) self.collapseFunction(id);
                    else self.expandFunction(id);
                },
                onNodeHoverIn: (e, node) => {
                    const raw = node.getData()?.raw;
                    if (!raw || raw.is_external) return;
                    if (window.showCodePreview) window.showCodePreview(raw.id, raw.name, raw.entrypoint, '', 0, e);
                },
                onNodeHoverOut: (e) => { if (window.hideCodePreview) window.hideCodePreview(e); },
                onCanvasMousemove: (e) => { if (window.moveCodePreview) window.moveCodePreview(e); },
                onEdgeClick: (e, edge) => {
                    const d = edge.getData() || {};
                    const fromId = edge.from?.id || edge.source?.id;
                    const toId = edge.to?.id || edge.target?.id;
                    if (fromId && toId && (d.kind === 'similarity' || d.kind === 'diff')) {
                        if (typeof window.openDiffDirectly === 'function') window.openDiffDirectly(fromId, '', toId, '', e);
                        else window.location.hash = `#/diff/${encodeURIComponent(fromId)}/${encodeURIComponent(toId)}`;
                    }
                },
            },
        };
    }

    // ---- notes live sync --------------------------------------------------

    async _onNoteChanged({ funcId }) {
        if (!this.pInstance || !this.nodes.has(funcId)) return;
        const content = await FunctionView.fetchNoteContent(funcId);
        const noteId = `bsimnote:${funcId}`;
        const existing = this.pInstance.noteManager?.getNote(noteId);
        if (existing) {
            if (content) { existing.setContent(content); this.pInstance.onChange(); }
            else { this.pInstance.noteManager.removeNote(existing); this.pInstance.onChange(); }
        } else if (content) {
            this.pInstance.noteManager?.addNote({ id: noteId, attachedElement: funcId, content, color: '#ffd700' }, true);
            this.pInstance.onChange();
        }
    }

    destroy() {
        window.removeEventListener('bsimvis:note-changed', this._noteListener);
        if (this.pInstance) { this.pInstance.destroy(); this.pInstance = null; }
        this.nodes.clear();
        this.edges.clear();
    }
}

window.PivotickGraphController = PivotickGraphController;
