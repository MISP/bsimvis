/**
 * Shared controller behind every Pivotick call-graph surface (function view's
 * Call Graph tab, the notes side panel, diff view's left/right graphs) so
 * they all behave the same way: same UI mode, same expand/collapse, same
 * notes sync, same "no duplicates, detect every connection already in the
 * graph" behavior.
 *
 * The whole model reduces to two ideas:
 *  - Every node is either "expanded" (in expandedIds -- the user put it here:
 *    the center, something they clicked to pull in its own callers/callees,
 *    or something they explicitly added) or merely "inferred" (it's on the
 *    graph only because expanding or similarity-searching some other node
 *    pulled it in). Each inferred node remembers exactly one thing about how
 *    it got here: `parentId`, the node whose expand/search discovered it
 *    first (refreshRelations's extra cross-edges between already-present
 *    nodes never change this -- real call graphs get densely
 *    cross-connected fast, and reachability-via-any-edge stopped meaning
 *    "belongs to this branch" the moment that happens). A node stays on the
 *    graph iff expandedIds itself, or climbing parentId pointers reaches an
 *    expandedIds member without the chain breaking -- see _computeKeepSet.
 *    Collapsing a node just drops it from expandedIds; anything whose only
 *    path back to an anchor ran through it (directly or transitively) falls
 *    out in the same pass, cascading through the whole discovered subtree
 *    (_pruneToKeepSet). Hiding similarities is the same rule with one extra
 *    condition: a chain that passes through a similarity-discovered hop
 *    doesn't count unless similarities are shown.
 *  - Clustering (toggleClustering) is a purely visual grouping by file_md5,
 *    applied uniformly across expanded and inferred nodes alike (see
 *    NON_CLUSTERABLE_KINDS/_buildClusteredEntries) -- it doesn't change
 *    what's in the model, only how same-binary nodes are drawn. `kind`
 *    (self/caller/callee/external/similar/added) is display-only now --
 *    coloring, click behavior for external nodes, and cluster eligibility --
 *    never structural visibility.
 *
 * Nodes are flat top-level Pivotick nodes unless clustering is on, in which
 * case same-md5 nodes become real Pivotick parent/children cluster nodes.
 * That nesting is only accepted by Pivotick at setData()/construction time,
 * so a clustered graph does a full rebuild on every structural change; flat
 * mode instead uses incremental addNode/removeNode so an expand, collapse,
 * or similarity toggle never disturbs the rest of the graph's layout.
 *
 * Keeps its own canonical {nodes, edges} model (plain Maps) as the source of
 * truth instead of reading it back out of the live Pivotick instance, so a
 * fresh render() can always be reconstructed from scratch.
 *
 * Pure per-node helpers (notes fetch, legend, edge labels, node rendering)
 * still live on FunctionView (bsimvis/app/static/js/views/function_view.js)
 * and are reused here, not duplicated.
 */
class PivotickGraphController {
    constructor(container, opts = {}) {
        this.container = container;
        this.opts = opts; // { collection?: string }
        this.nodes = new Map();   // id -> { raw, kind, parentId }
        this.edges = new Map();   // id -> { from, to, kind: 'call'|'similarity', score? }
        this.centerId = null;
        this.pInstance = null;
        this._simEdgesEnabled = true;
        this._clusterByBinary = false;
        // The one and only "why is this node here" bookkeeping: every
        // function is either in this set (the user put it here -- the
        // center, something they clicked to expand, or something they
        // explicitly added) or it's merely inferred, on the graph only
        // because a call or similarity edge connects it to something that
        // is. addFunction always adds its own id here, including the
        // center's, so this alone is the anchor set _computeKeepSet walks
        // from -- no separate depth/expandedFrom/kind-based tracking.
        this.expandedIds = new Set();

        this._noteListener = (e) => this._onNoteChanged(e.detail);
        window.addEventListener('bsimvis:note-changed', this._noteListener);
    }

    getCollection() {
        return this.opts.collection || (window.getRoutingState ? window.getRoutingState().collection : '') || '';
    }

    _notifyExpandedChange() {
        if (typeof this.opts.onExpandedChange !== 'function') return;
        // The center is always in expandedIds too (see addFunction) so
        // _computeKeepSet has one anchor set to walk, but it's not
        // removable -- leave it off the side panel's list.
        const list = [...this.expandedIds]
            .filter(id => id !== this.centerId)
            .map(id => ({ id, name: this.nodes.get(id)?.raw?.name || id.split(':').pop() }));
        this.opts.onExpandedChange(list);
    }

    // ---- model mutation -----------------------------------------------

    // A function with hundreds of callers (a shared utility in a
    // statically-linked binary) would otherwise dump hundreds of nodes into
    // the force simulation from one click -- both a real performance cliff
    // and an unreadable graph. Capped per side (callers/callees each), with
    // the backend reporting the true counts so callGraphRenderNode can show
    // "+N more" instead of silently hiding them (see _applyCallGraphTruncation).
    static MAX_CALL_CHILDREN_PER_SIDE = 40;

    _callGraphUrl(id) {
        return `/api/function/call_graph?id=${encodeURIComponent(id)}&limit=${PivotickGraphController.MAX_CALL_CHILDREN_PER_SIDE}`;
    }

    // Stashes how many callers/callees the backend truncated for the node
    // that was just fetched (entry is that node's own model entry, not the
    // newly-discovered children's) -- 0 when nothing was cut off.
    _applyCallGraphTruncation(entry, data) {
        if (!entry) return;
        const shownCallers = (data.callers || []).length;
        const shownCallees = (data.callees || []).length;
        entry.moreCallers = Math.max(0, (data.callers_total ?? shownCallers) - shownCallers);
        entry.moreCallees = Math.max(0, (data.callees_total ?? shownCallees) - shownCallees);
    }

    // Fetches one function's own call graph (itself + its depth-1 callers/
    // callees) and folds it into the model. Every function added this way --
    // whether it's the graph's original center or a function dropped in
    // later -- gets the same one-hop treatment, which is the "functions
    // treated equally" behavior.
    async addFunction(id, { asCenter = false } = {}) {
        if (this.nodes.has(id) && !asCenter) return;

        const res = await fetch(this._callGraphUrl(id));
        if (!res.ok) throw new Error('Call graph not found');
        const data = await res.json();
        const nodeId = data.node.id;

        if (asCenter || !this.centerId) this.centerId = nodeId;

        const newIds = [];
        const upsert = (fid, raw, kind, parentId) => {
            if (this.nodes.has(fid)) return false;
            this.nodes.set(fid, { raw, kind, parentId });
            newIds.push(fid);
            return true;
        };

        if (!this.nodes.has(nodeId)) {
            // A root: nothing discovered it, it's either the center or a
            // direct add -- both go straight into expandedIds below.
            upsert(nodeId, data.node, nodeId === this.centerId ? 'self' : 'added', null);
        } else if (nodeId === this.centerId) {
            this.nodes.get(nodeId).kind = 'self';
        }
        this._applyCallGraphTruncation(this.nodes.get(nodeId), data);

        for (const c of data.callers || []) {
            upsert(c.id, c, c.is_external ? 'external' : 'caller', nodeId);
            this._addEdge(c.id, nodeId, 'call');
        }
        for (const c of data.callees || []) {
            upsert(c.id, c, c.is_external ? 'external' : 'callee', nodeId);
            this._addEdge(nodeId, c.id, 'call');
        }

        // nodeId itself was just explicitly put on the graph (as the center,
        // or as a direct add) -- an anchor like any manually-expanded node,
        // never merely inferred. Its own callers/callees above stay inferred
        // until someone expands them too.
        this.expandedIds.add(nodeId);
        this._notifyExpandedChange();

        await this.refreshRelations(newIds.length ? newIds : [nodeId]);
        await this.render();
        if (this._simEdgesEnabled) this._discoverSimilar([nodeId, ...newIds]);
    }

    async expandFunction(id) {
        const entry = this.nodes.get(id);
        if (!entry || entry.kind === 'external') return;

        const res = await fetch(this._callGraphUrl(id));
        if (!res.ok) return;
        const data = await res.json();
        this._applyCallGraphTruncation(entry, data);
        const newIds = [];

        for (const c of data.callers || []) {
            if (!this.nodes.has(c.id)) {
                this.nodes.set(c.id, { raw: c, kind: c.is_external ? 'external' : 'caller', parentId: id });
                newIds.push(c.id);
            }
            this._addEdge(c.id, id, 'call');
        }
        for (const c of data.callees || []) {
            if (!this.nodes.has(c.id)) {
                this.nodes.set(c.id, { raw: c, kind: c.is_external ? 'external' : 'callee', parentId: id });
                newIds.push(c.id);
            }
            this._addEdge(id, c.id, 'call');
        }
        this.expandedIds.add(id);
        this._notifyExpandedChange();

        await this.refreshRelations(newIds.length ? newIds : [id]);
        await this.render();
        if (this._simEdgesEnabled && newIds.length) this._discoverSimilar(newIds);
    }

    // Collapsing is just "this is no longer something the user put here" --
    // pull it out of expandedIds and let whatever stops being reachable from
    // an anchor as a result fall out too, via the same rule
    // toggleSimilarity(false) uses (_computeKeepSet). The center is always
    // in expandedIds (see addFunction) but isn't collapsible -- it's the
    // graph's root, not a node the user expanded into it.
    collapseFunction(id) {
        if (id === this.centerId || !this.expandedIds.has(id)) return;
        this.expandedIds.delete(id);
        this._notifyExpandedChange();
        this._pruneToKeepSet(this._computeKeepSet(this._simEdgesEnabled));
    }

    // Side-panel "x" button: same as clicking an already-expanded node again.
    removeExpanded(id) {
        this.collapseFunction(id);
    }

    // Model-only removal (maps, not the live Pivotick instance) -- the
    // building block _pruneToKeepSet uses to compute what's left, and to
    // apply that removal to the live graph without a full rebuild.
    removeFunction(id) {
        this.nodes.delete(id);
        for (const [eid, e] of this.edges) {
            if (e.from === id || e.to === id) this.edges.delete(eid);
        }
    }

    // A node stays in the graph iff it's an anchor (expandedIds), was
    // marked for comparison (a 'diff' edge -- an explicit user action, same
    // as expanding), or its provenance chain reaches one of those. Two
    // different kinds of chain link, deliberately not treated the same:
    //
    //  - A caller/callee/external/self/added node exists only because
    //    expanding parentId specifically pulled it in -- it survives
    //    exactly as long as parentId is a *currently live* anchor
    //    (expandedIds.has(parentId)), full stop. It does NOT survive just
    //    because parentId itself remains visible for some unrelated reason
    //    (e.g. parentId is also a direct callee of the center) -- that
    //    would let a plain neighbor's own fallback visibility resurrect
    //    every child of an expand that was just undone. No further
    //    climbing past parentId: an expand's discovered nodes are always
    //    exactly one hop from the expand that found them.
    //  - A similar-kind node rides along with whatever found it, however
    //    far that chain climbs (similarity discovery isn't a reversible
    //    per-node action the way expand is), so it recurses through
    //    resolve(parentId) instead -- and is excluded outright when
    //    includeSimilarity is false.
    //
    // Deliberately NOT graph reachability over every edge, either:
    // refreshRelations wires up real call edges between any two nodes that
    // happen to already both be on the graph, and a handful of expands is
    // enough for that to connect almost everything in a small, densely
    // cross-called binary -- at which point "keep if reachable via any
    // edge" keeps everything and collapsing stops doing anything visible.
    _computeKeepSet(includeSimilarity) {
        const diffAnchors = new Set();
        for (const e of this.edges.values()) {
            if (e.kind === 'diff') { diffAnchors.add(e.from); diffAnchors.add(e.to); }
        }
        const isAnchor = (id) => this.expandedIds.has(id) || diffAnchors.has(id);

        const memo = new Map();
        const resolve = (id, visiting) => {
            if (isAnchor(id)) return true;
            if (memo.has(id)) return memo.get(id);
            if (visiting.has(id)) return false; // cycle guard
            visiting.add(id);
            const entry = this.nodes.get(id);
            let ok = false;
            if (entry?.kind === 'similar') {
                ok = includeSimilarity && !!entry.parentId && this.nodes.has(entry.parentId)
                    && resolve(entry.parentId, visiting);
            } else if (entry) {
                ok = !!entry.parentId && this.expandedIds.has(entry.parentId);
            }
            memo.set(id, ok);
            return ok;
        };

        const keep = new Set();
        for (const id of this.nodes.keys()) {
            if (resolve(id, new Set())) keep.add(id);
        }
        return keep;
    }

    // Drops whatever isn't in `keep` from the model (plus any extra edges
    // the caller already knows should go, e.g. similarity edges between two
    // surviving nodes), then applies that to the live graph. Plain
    // removeNode/removeEdge calls when the graph is flat -- no rebuild, so
    // everything else keeps its position. Clustering nests real
    // parent/children structure that Pivotick only accepts at setData()
    // time (see _buildClusteredEntries), so a clustered graph still needs
    // the full rebuild render() already does.
    _pruneToKeepSet(keep, extraEdgeIds = []) {
        const removedIds = [...this.nodes.keys()].filter(id => !keep.has(id));
        for (const id of removedIds) this.removeFunction(id);
        for (const eid of extraEdgeIds) this.edges.delete(eid);
        if (!removedIds.length && !extraEdgeIds.length) return;

        if (this._clusterByBinary || !this.pInstance) {
            this.render();
            return;
        }
        for (const id of removedIds) {
            try { this.pInstance.removeNode(id); } catch (err) {}
        }
        for (const eid of extraEdgeIds) {
            try { this.pInstance.removeEdge(eid); } catch (err) {}
        }
        this.pInstance.onChange();
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
            // Similarity edges the backend reports between nodes already in
            // the model -- gated the same as _discoverSimilar, otherwise
            // expanding a node while similarities are toggled off silently
            // wires new similarity edges back in behind the toggle's back.
            if (this._simEdgesEnabled) {
                for (const e of data.sim_edges || []) {
                    if (this.nodes.has(e.id1) && this.nodes.has(e.id2)) {
                        this._addEdge(e.id1, e.id2, 'similarity', { score: e.score });
                    }
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
        const newIds = [];
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
                const shown = (data.pairs || []).length;
                entry.moreSimilar = Math.max(0, (data.total ?? shown) - shown);
                for (const pair of data.pairs || []) {
                    const isSelf1 = pair.id1 === id;
                    const otherId = isSelf1 ? pair.id2 : pair.id1;
                    const otherMeta = isSelf1 ? pair.meta2 : pair.meta1;
                    const otherName = isSelf1 ? pair.name2 : pair.name1;
                    if (!otherId || !otherMeta || otherId === id) continue;
                    // A function already in the graph (as a caller/callee/
                    // whatever) still gets this similarity edge drawn -- it's
                    // the same node regardless of which path found it first,
                    // and the relationship is real either way. Only actually
                    // adding a new node counts against the flood-control cap.
                    if (!this.nodes.has(otherId)) {
                        if (added >= PivotickGraphController.MAX_AUTO_SIMILAR_PER_BATCH) continue;
                        this.nodes.set(otherId, {
                            raw: {
                                id: otherId, name: otherName, namespace: otherMeta.namespace,
                                return_type: otherMeta.return_type, parameters: otherMeta.parameters,
                                file_md5: otherMeta.file_md5, entrypoint: otherMeta.entrypoint_address,
                                is_external: false,
                            },
                            kind: 'similar', parentId: id,
                        });
                        newIds.push(otherId);
                        added++;
                    }
                    this._addEdge(id, otherId, 'similarity', { score: pair.score });
                }
            } catch (err) { console.error('Error discovering similar functions:', err); }
        }
        // Newly added similar-function nodes only got the one edge that
        // pulled them in -- refreshRelations wires up whatever else they
        // connect to (calls, other similarities) among what's already here.
        if (newIds.length) await this.refreshRelations(newIds);
        await this.render();
    }

    async toggleSimilarity(show) {
        this._simEdgesEnabled = show;
        if (!show) {
            // Same rule as collapsing a node (_computeKeepSet), just with
            // similarity edges excluded from reachability: whatever's left
            // reachable only through a similarity chain -- never expanded,
            // never on a call path from an anchor -- goes. Similarity edges
            // between two nodes that both survive (e.g. two manually-expanded
            // functions that happen to match) aren't touched by that node
            // removal, so they're passed along as extra edges to drop too.
            const simEdgeIds = [...this.edges].filter(([, e]) => e.kind === 'similarity').map(([eid]) => eid);
            this._pruneToKeepSet(this._computeKeepSet(false), simEdgeIds);
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
        this.expandedIds.clear();
        this._notifyExpandedChange();
        if (window.getCollectionFromId) {
            const col = window.getCollectionFromId(newCenterId);
            if (col) this.opts.collection = col;
        }
        await this.addFunction(newCenterId, { asCenter: true });
    }

    // ---- rendering ------------------------------------------------------

    // Pivotick's built-in search box (toolbar "Search" button / Shift+J)
    // works generically off whatever top-level keys a node's data object
    // has -- it string-matches every field and, on pick, selects that node.
    // Our own fields all live nested under `raw`, which the search only
    // sees as "[object Object]", so the fields worth searching (name,
    // address, return type, containing binary) are duplicated flat here.
    // `label` is Pivotick's own convention for a node's display name --
    // giving it one is what makes search results (and other native chrome
    // that reads it) show the function name instead of a placeholder.
    static _searchableFields(raw) {
        if (!raw) return {};
        return {
            label: raw.name,
            name: raw.name,
            address: raw.entrypoint ?? raw.address,
            namespace: raw.namespace,
            return_type: raw.return_type,
            file_md5: raw.file_md5,
            file_name: raw.file_name,
        };
    }

    _flatEntries() {
        return [...this.nodes.entries()].map(([id, n]) => ({
            id,
            data: {
                ...PivotickGraphController._searchableFields(n.raw),
                raw: n.raw, kind: n.kind,
                more: { callers: n.moreCallers || 0, callees: n.moreCallees || 0, similar: n.moreSimilar || 0 },
            },
        }));
    }

    _flatEdges() {
        // Calls are the graph's real structure; similarity is a secondary,
        // looser relationship -- keep it visually subordinate (thinner) so
        // it doesn't compete with call edges for attention.
        const STYLE = {
            call: { edge: { strokeWidth: 2.5 } },
            similarity: { edge: { strokeColor: '#ae81ff', dashed: true, strokeWidth: 1.5 } },
            diff: { edge: { strokeColor: '#fd971f', dashed: true, strokeWidth: 3 } },
        };
        return [...this.edges.entries()].map(([id, e]) => ({
            id, from: e.from, to: e.to,
            directed: e.kind === 'call',
            data: { kind: e.kind, score: e.score },
            style: STYLE[e.kind],
        }));
    }

    // Grouping same-binary functions into a real Pivotick parent/children
    // cluster node is opt-in (toggleClustering) -- when it's off every
    // function is a normal flat top-level node, which is the only shape
    // that survived the tree-radial drag saga (see class docstring). Any
    // 2+ nodes sharing a file_md5 get grouped, regardless of kind -- an
    // earlier version only grouped 'similar'/'added' matches on the theory
    // that the center's own direct callers/callees are the actual subject
    // of the graph, but that made clustering look random ("this file's
    // functions group, that file's don't") whenever the center's own
    // neighbors happened to share its binary, which is the common case for
    // a statically-linked sample. Only the center itself (always exactly
    // one node, nothing to group) and externals (no real file identity)
    // are excluded.
    static NON_CLUSTERABLE_KINDS = new Set(['self', 'external']);

    _buildClusteredEntries(flatEntries) {
        const byMd5 = new Map();
        for (const n of flatEntries) {
            const md5 = n.data.raw?.file_md5;
            if (PivotickGraphController.NON_CLUSTERABLE_KINDS.has(n.data.kind) || !md5) continue;
            if (!byMd5.has(md5)) byMd5.set(md5, []);
            byMd5.get(md5).push(n);
        }
        const clusterMd5s = new Set([...byMd5].filter(([, ns]) => ns.length >= 2).map(([md5]) => md5));

        const result = [];
        const clusters = new Map();
        for (const n of flatEntries) {
            const md5 = n.data.raw?.file_md5;
            const clusterable = !PivotickGraphController.NON_CLUSTERABLE_KINDS.has(n.data.kind) && clusterMd5s.has(md5);
            if (!clusterable) { result.push(n); continue; }
            if (!clusters.has(md5)) {
                const cluster = {
                    id: `cluster:${md5}`,
                    data: {
                        label: n.data.raw.file_name || md5,
                        file_md5: md5, file_name: n.data.raw.file_name,
                        kind: 'binary-cluster', raw: { file_md5: md5, file_name: n.data.raw.file_name },
                    },
                    expanded: false,
                    children: [],
                };
                clusters.set(md5, cluster);
                result.push(cluster);
            }
            clusters.get(md5).children.push(n);
        }
        return result;
    }

    // Clustering toggle changes node *structure* (real children[] nesting),
    // which Pivotick only accepts at setData()/construction time -- unlike
    // the flat path, every render() has to fully rebuild while it's on.
    async toggleClustering(enabled) {
        this._clusterByBinary = enabled;
        await this.render();
    }

    async render() {
        const flatEntries = this._flatEntries();
        const nodes = this._clusterByBinary ? this._buildClusteredEntries(flatEntries) : flatEntries;
        const edges = this._flatEdges();
        const needsRebuild = !this.pInstance || this._clusterByBinary;

        if (needsRebuild) {
            const notes = await FunctionView.fetchGraphNotes(flatEntries);
            if (!this.pInstance) {
                this.pInstance = new Pivotick(this.container, { nodes, edges, notes }, this._pivotickOptions());
                // Outbound (graph note edit -> BSimVis notes store) sync. Wired
                // once at instance creation: setData() clears/repopulates the
                // same Graph object, so this listener survives every rebuild.
                FunctionView.wireNoteSync(this.pInstance);
                if (typeof window.setupGraphDropTarget === 'function') window.setupGraphDropTarget(this.container, this);
            } else {
                this.pInstance.setData(nodes, edges, notes);
            }
            this._tuneForceLinks();
            this._positionFreshNotes(notes);
            this.pInstance.onChange();
            this._scheduleFixNodeBoxSizes();
            return;
        }

        // Flat and already built: adding more never needs a full rebuild,
        // just add whatever's new.
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
        this._scheduleFixNodeBoxSizes();
    }

    // Pivotick's own foreignObject auto-sizer (measure the rendered content
    // on a rAF, call node.setBoxSize()) already can't be trusted to shrink a
    // box (see the header-sizing comment this file used to carry for
    // clusters) -- in Firefox it undershoots growing one too, and Firefox
    // clips foreignObject content strictly to whatever box was last set, so
    // a card's bottom or right edge gets cropped instead of just overflowing
    // visibly the way it does in Chrome. Measure the real rendered content
    // ourselves and set the box explicitly, both dimensions -- callGraphRenderNode
    // fixes flat function cards at 190px (so scrollWidth there just confirms
    // that), but the binary-cluster card is intentionally auto-width
    // (nowrap label, no CSS width), and hardcoding it to 190 here used to
    // clip anything longer than that.
    static NODE_WIDTH = 190;

    _fixNodeBoxSizes() {
        if (!this.pInstance) return;
        try {
            let touched = false;
            for (const node of this.pInstance.getMutableNodes()) {
                const el = typeof node.getGraphElement === 'function' ? node.getGraphElement() : null;
                const fo = el?.querySelector(':scope > foreignObject');
                const content = fo?.firstElementChild;
                if (!fo || !content) continue;
                // Belt-and-suspenders against the Firefox-only crop: SVG's
                // default foreignObject overflow is UA-dependent, and
                // Firefox's is strict enough to clip content to whatever
                // box was last set even a pixel early -- explicit
                // overflow:visible on both the foreignObject and its
                // content div means even a stale/undersized box (a resize
                // caught mid-flight, a font metrics change after this ran)
                // spills visibly instead of getting clipped, matching what
                // Chrome already does with no box-size math needed for it.
                fo.style.overflow = 'visible';
                content.style.overflow = 'visible';
                const w = content.scrollWidth || PivotickGraphController.NODE_WIDTH;
                const h = content.scrollHeight;
                if (!h) continue;
                if (fo.getAttribute('width') === String(w) && fo.getAttribute('height') === String(h)) continue;
                fo.setAttribute('width', w);
                fo.setAttribute('height', h);
                fo.setAttribute('x', -w / 2);
                fo.setAttribute('y', -h / 2);
                if (typeof node.setBoxSize === 'function') node.setBoxSize(w, h);
                touched = true;
            }
            if (touched) this.pInstance.onChange();
        } catch (err) {
            console.error('Failed to fix node box sizes:', err);
        }
    }

    // requestAnimationFrame doesn't fire at all in a backgrounded/inactive
    // tab (browsers pause rAF entirely when nothing's being painted), so a
    // measurement scheduled on one can be stuck waiting indefinitely --
    // confirmed live: a cluster header right after expanding stayed at its
    // stale placeholder size no matter how long a queued rAF was given to
    // run. setTimeout still fires (just possibly throttled) regardless of
    // visibility, and by the time it runs Pivotick's own synchronous DOM
    // update for the change is already done -- reading scrollWidth/Height
    // forces a layout flush at query time either way, so there's nothing an
    // rAF was buying here.
    _scheduleFixNodeBoxSizes() {
        setTimeout(() => this._fixNodeBoxSizes(), 0);
    }

    // Force layout's link force has no per-edge distance/strength hook in
    // Pivotick's public options (only a single graph-wide d3LinkDistance
    // knob) -- reach into the live d3-force instance directly and hand it
    // per-edge functions instead. Calls pull hard and short, so the graph's
    // shape is driven by actual call structure; similarity edges pull weakly
    // and sit longer, so a similarity match drifts nearby without distorting
    // that shape. d3-force calls these every tick for whatever edges are
    // currently in the simulation, so this only needs wiring once -- it
    // keeps applying to edges added later too.
    static LINK_DISTANCE = { call: 90, similarity: 220, diff: 150 };
    static LINK_STRENGTH = { call: 1, similarity: 0.15, diff: 0.3 };

    _tuneForceLinks() {
        const link = this.pInstance?.simulation?.simulationForces?.link;
        if (!link || typeof link.distance !== 'function') return;
        const kindOf = (e) => e?.getData?.()?.kind ?? e?.data?.kind;
        link.distance(e => PivotickGraphController.LINK_DISTANCE[kindOf(e)] ?? PivotickGraphController.LINK_DISTANCE.call);
        link.strength(e => PivotickGraphController.LINK_STRENGTH[kindOf(e)] ?? PivotickGraphController.LINK_STRENGTH.call);
    }

    // Notes default to (0,0) and Pivotick's note-edge connector renders
    // nothing when note and node coincide -- place a freshly-attached note
    // just outside its node's circle instead of on top of it.
    static NOTE_MARGIN = 24;

    _positionFreshNotes(notes) {
        if (!this.pInstance?.noteManager) return;
        for (const n of notes) {
            const note = this.pInstance.noteManager.getNote(n.id);
            let node = this.pInstance.getMutableNode?.(n.attachedElement?.id);
            while (node && typeof node.x !== 'number' && node.parentNode) node = node.parentNode;
            if (!note || !node || typeof node.x !== 'number' || typeof node.y !== 'number') continue;
            const radius = (typeof node.getCircleRadius === 'function' && node.getCircleRadius()) || 0;
            const offset = radius + PivotickGraphController.NOTE_MARGIN;
            note.setPosition(node.x + offset, node.y - offset);
        }
    }

    _pivotickOptions() {
        const self = this;
        return {
            UI: { mode: 'light', tooltip: { enabled: false } },
            // Tree-Radial fought manual dragging (its own settle force kept
            // pulling every node back to its hierarchy-computed slot -- see
            // git history) and forced every node into a strict hierarchy
            // when a graph is really calls *and* similarity matches, not a
            // clean tree. Force lets both relationships pull nodes into a
            // shape and holds a manually-dragged node right where it's put
            // (see _tuneForceLinks for making call edges pull harder than
            // similarity edges).
            layout: { type: 'force' },
            simulation: { useWorker: false },
            render: {
                nodeShape: 'rectangle',
                renderNode: (node) => {
                    const d = node.getData() || {};
                    return FunctionView.callGraphRenderNode(d.raw, d.kind, d.more);
                },
                renderLabel: (edge) => FunctionView.renderEdgeLabel(edge),
                // Pivotick's curved-edge arc radius is literally the raw
                // node-to-node distance (linkArc() does Math.hypot(dx,dy)),
                // not anything shape-aware -- bows into huge arcs whenever
                // two nodes end up far apart, which force layout does
                // constantly. Only the straight-line renderer consults
                // getNodeBorderRadius(), which is what actually anchors an
                // arrowhead to a rectangular card's edge instead of its center.
                defaultEdgeStyle: { curveStyle: 'straight' },
            },
            callbacks: {
                onNodeClick: async (e, node) => {
                    const d = node.getData() || {};
                    if (d.kind === 'binary-cluster') {
                        self.pInstance.toggleExpandNode(node);
                        // toggleExpandNode is a native Pivotick action, not
                        // one of our render() calls -- it re-renders the
                        // cluster header itself (collapsed <-> expanded
                        // pill), so it needs the same box-size fix-up or the
                        // label can end up in a stale/zero-size foreignObject.
                        self._scheduleFixNodeBoxSizes();
                        return;
                    }
                    const id = node.id;
                    if (d.kind === 'external' || d.raw?.is_external) {
                        if (window.showToast) window.showToast('External node cannot be expanded', 'info');
                        return;
                    }
                    if (id === self.centerId) return;
                    // expandedIds is the single source of truth for this --
                    // not a flag baked into the node's own (possibly stale)
                    // rendered data.
                    if (self.expandedIds.has(id)) self.collapseFunction(id);
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
            const attachedElement = { type: 'node', id: funcId };
            this.pInstance.noteManager?.addNote({ id: noteId, attachedElement, content, color: '#ffd700' }, true);
            this._positionFreshNotes([{ id: noteId, attachedElement }]);
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
