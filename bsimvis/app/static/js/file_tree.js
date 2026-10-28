/**
 * file_tree.js -- the file search table, drawn as a tree.
 *
 * Two modes, picked by the `group` URL param:
 *
 *  - default, by archive: every result sits under the archives (and packed
 *    binaries) it came out of. Each upload behind the page is fetched once,
 *    whole, through /api/file/search?root_md5=..., so a contained file is the
 *    same row as a search hit -- same fields, same cluster cards. Archives are
 *    header bands, open by default: the unpacked files are what an analyst is
 *    after. While a filter is active, the files of an archive that did not
 *    match fold into one "N other files" line.
 *  - group=cluster: the page's files grouped under their best cluster on one
 *    axis (group_axis, default code), each with its archive as a path prefix.
 *
 * `fold_tag` (comma separated, `lib:*` style prefix globs) folds the files
 * carrying those tags into one line per archive or cluster.
 *
 * Collapsing is client-side: every row lists the bands, groups and folds above
 * it in data-ft-anc and is hidden while any of them is closed. Bands and groups
 * start open, folds start closed.
 */
window.FileTree = {
    SUBTREE_LIMIT: 2000,
    AXES: ['code', 'overall', 'library', 'content'],

    _items: [],
    _clusters: {},
    _uploads: new Map(),     // root md5 -> its docs, root included
    _truncated: false,
    _collapsed: new Set(),   // bands / groups the user closed
    _opened: new Set(),      // folds the user opened

    // --- toolbar ---------------------------------------------------------

    extraCols() {
        try { return localStorage.getItem('fileTreeExtraCols') === '1'; } catch (e) { return false; }
    },

    setExtraCols(on) {
        try { localStorage.setItem('fileTreeExtraCols', on ? '1' : '0'); } catch (e) { /* per-viewer only */ }
        // The columns are left out of the markup, not hidden, so the colgroup
        // sizing measures real cells: rebuild header and rows.
        lastViewPath = '';
        refreshData(false, true);
    },

    setParam(key, value) {
        const { viewKey, params } = getRoutingState();
        if (value) params.set(key, value); else params.delete(key);
        currentOffset = 0;
        isEndOfResults = false;
        navigate(viewKey, params);
    },

    toolbarHtml(params) {
        const mode = params.get('group') === 'cluster' ? 'cluster' : 'archive';
        const axis = params.get('group_axis') || 'code';
        const nodes = params.get('group_nodes') || 'file';
        const sel = (id, key, cur, opts) => `<select id="${id}" class="ft-select" onchange="FileTree.setParam('${key}', this.value)">
            ${opts.map(([v, l]) => `<option value="${v}"${v === cur ? ' selected' : ''}>${l}</option>`).join('')}</select>`;
        return `
            <div class="view-toggle" title="How nested files are grouped">
                <button class="view-btn ${mode === 'archive' ? 'active' : ''}" onclick="FileTree.setParam('group', '')">By archive</button>
                <button class="view-btn ${mode === 'cluster' ? 'active' : ''}" onclick="FileTree.setParam('group', 'cluster')">By cluster</button>
            </div>
            ${mode === 'cluster' ? sel('ft-axis', 'group_axis', axis, this.AXES.map(a => [a, `${a[0].toUpperCase()}${a.slice(1)} clusters`]))
                + sel('ft-nodes', 'group_nodes', nodes, [['file', 'Files'], ['container', 'Archives']]) : ''}
            <input type="text" id="ft-fold-tag" class="ft-fold-input" placeholder="Fold tags: lib:*, stdlib" title="Fold the files carrying these tags into one line. A trailing * matches a prefix."
                value="${escapeAttr(params.get('fold_tag') || '')}" onchange="FileTree.setParam('fold_tag', this.value.trim())">
            <label class="ft-check" title="Show the MD5 and batch columns"><input type="checkbox" id="ft-extra-cols" ${this.extraCols() ? 'checked' : ''}
                onchange="FileTree.setExtraCols(this.checked)"> MD5 &amp; batch</label>`;
    },

    // --- entry point -----------------------------------------------------

    /**
     * Rows for everything loaded so far. Appending a page re-draws the whole
     * tree, so a file that arrives on page 2 joins its archive from page 1
     * instead of starting a second copy of it.
     */
    async html(items, clustersMap, { append, params, collection, pool }) {
        if (!append) {
            this._items = [];
            this._clusters = {};
            this._uploads = new Map();
            this._truncated = false;
        }
        this._items.push(...items);
        Object.assign(this._clusters, clustersMap || {});
        // Full-width rows span the real column count: an oversized colspan
        // adds phantom columns that a fixed-layout table shares its width with.
        this._span = (typeof document.querySelectorAll === 'function'
            && document.querySelectorAll('#header-table thead tr:first-child th').length) || 10;

        const ctx = {
            params,
            folds: (params.get('fold_tag') || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean),
            filtered: this._isFiltered(params),
        };
        if (params.get('group') === 'cluster') return this._byCluster(ctx);
        // ponytail: root_md5 is indexed per collection; a pool search keeps
        // the page-only forest until pools get the same lookup.
        if (pool) return TableRenderers._fileForest(this._items, this._clusters);
        return this._byArchive(ctx, collection);
    },

    /** Hide every row under a closed band, group or fold. */
    applyVisibility(tbody) {
        const closed = id => id.startsWith('f:') ? !this._opened.has(id) : this._collapsed.has(id);
        tbody.querySelectorAll('tr[data-ft-anc]').forEach(tr => {
            const anc = tr.dataset.ftAnc;
            tr.style.display = anc && anc.split(' ').some(closed) ? 'none' : '';
        });
        tbody.querySelectorAll('[data-ft-toggle]').forEach(el => {
            const open = !closed(el.dataset.ftToggle);
            const icon = el.querySelector('i');
            if (icon) icon.className = `fa-solid fa-chevron-${open ? 'down' : 'right'}`;
        });
    },

    toggle(event, id) {
        event.stopPropagation();
        const set = id.startsWith('f:') ? this._opened : this._collapsed;
        set.has(id) ? set.delete(id) : set.add(id);
        this.applyVisibility(document.getElementById('table-body'));
    },

    // --- shared bits -----------------------------------------------------

    // Params that change the layout, not the result set.
    _LAYOUT_PARAMS: new Set(['collection', 'pool', 'limit', 'offset', 'sort_by', 'sort_order', 'view',
        'group', 'group_axis', 'group_nodes', 'fold_tag', 'min_cohesion', 'bin_cluster_axes', 'algo']),

    _isFiltered(params) {
        for (const [k, v] of params.entries()) {
            if (v && !this._LAYOUT_PARAMS.has(k)) return true;
        }
        return false;
    },

    _tags(f) {
        return [...(f.tags || []), ...(f.user_tags || [])].map(t => String(t).toLowerCase());
    },

    _folded(f, folds) {
        if (!folds.length) return false;
        return this._tags(f).some(t => folds.some(p => p.endsWith('*') ? t.startsWith(p.slice(0, -1)) : t === p));
    },

    _foldedTags(files, folds) {
        const out = new Set();
        files.forEach(f => this._tags(f).forEach(t => {
            if (folds.some(p => p.endsWith('*') ? t.startsWith(p.slice(0, -1)) : t === p)) out.add(t);
        }));
        return [...out];
    },

    _split(path) {
        const parts = String(path || '').split('/');
        const base = parts.pop();
        return { dir: parts.length ? parts.join('/') + '/' : '', base };
    },

    _row(f, depth, tree) {
        return TableRenderers._fileRows([f], this._clusters, depth, f.parent_md5, false, tree);
    },

    _lineRow(id, anc, depth, html) {
        return `<tr class="ft-fold" data-ft-anc="${escapeAttr(anc)}" onclick="FileTree.toggle(event, ${escapeAttr(jsString(id))})">
            <td colspan="${this._span}" style="padding-left:${12 + depth * 18}px;">
                <span class="ft-chev" data-ft-toggle="${escapeAttr(id)}"><i class="fa-solid fa-chevron-right"></i></span>${html}
            </td></tr>`;
    },

    _noteRow(anc, depth, html) {
        return `<tr class="ft-note" data-ft-anc="${escapeAttr(anc)}"><td colspan="${this._span}" style="padding-left:${30 + depth * 18}px;">${html}</td></tr>`;
    },

    _foldLabel(files, folds) {
        const tags = this._foldedTags(files, folds);
        const funcs = files.reduce((s, f) => s + (Number(f.function_count) || 0), 0);
        return `<b>${files.length} file${files.length === 1 ? '' : 's'} tagged ${escapeHtml(tags.slice(0, 4).join(', '))}${tags.length > 4 ? '…' : ''}</b>
            <span class="dim">· ${funcs.toLocaleString()} functions</span>`;
    },

    /** Name cell of a tree row; everything right of it is the normal row. */
    nameCell(f, collection, depth, tree, nameHtml) {
        const chev = tree.band
            ? `<span class="ft-chev" data-ft-toggle="${escapeAttr(tree.id)}" onclick="FileTree.toggle(event, ${escapeAttr(jsString(tree.id))})"><i class="fa-solid fa-chevron-down"></i></span>`
            : '<span class="ft-chev"></span>';
        const icon = tree.band
            ? (f.is_container
                ? '<i class="fa-solid fa-box-archive ft-ic-arc" title="Archive: holds code but is not code itself"></i>'
                : '<i class="fa-solid fa-cube ft-ic-packed" title="Packed binary: analysed itself, and unpacked into the files below"></i>')
            : '';
        const kind = tree.band
            ? `<span class="ft-kind${f.is_container ? '' : ' ft-kind-packed'}">${escapeHtml(f.is_container ? (f.filetype || 'archive') : 'packed')}</span>`
            : '';
        const name = (f.exists === false)
            ? nameHtml
            : `<b class="lineage-link" title="${escapeAttr(f.file_name || '')}" onclick="event.stopPropagation(); openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(f.file_md5))}, ${escapeAttr(jsString(f.file_name || ''))}, event)">${escapeHtml(tree.base || f.file_name || f.file_md5)}</b>`;
        return `
            <div class="lineage-cell ft-name" style="padding-left:${depth * 18}px;">
                ${chev}${icon}${kind}
                <span class="ft-name-text">${tree.prefix ? `<span class="ft-prefix" title="${escapeAttr(tree.prefix)}">${escapeHtml(tree.prefix)}</span>` : ''}${name}</span>
                ${tree.unpacked ? '<span class="ft-badge">unpacked</span>' : ''}
            </div>
            ${tree.sub ? `<div class="ft-sub" style="padding-left:${depth * 18 + 22}px;">${tree.sub}</div>` : ''}`;
    },

    // --- by archive ------------------------------------------------------

    _fetchFiles(params, extra) {
        const q = new URLSearchParams(extra);
        for (const k of ['collection', 'min_cohesion', 'algo']) if (params.get(k)) q.set(k, params.get(k));
        return fetch(`/api/file/search?${q}`).then(r => r.ok ? r.json() : { files: [] }).catch(e => {
            console.error(e);
            return { files: [] };
        });
    },

    /**
     * Every upload behind the page, whole: each root that is not on the page
     * plus every file under it. Two requests however many uploads there are;
     * an upload loaded for an earlier page is not asked for again.
     */
    async _loadUploads(params, collection, roots, hits) {
        const missing = roots.filter(r => !this._uploads.has(r));
        if (missing.length) {
            const absent = missing.filter(r => !hits.has(r));
            const [sub, self] = await Promise.all([
                this._fetchFiles(params, { collection, root_md5: missing.join(','), limit: this.SUBTREE_LIMIT, sort_by: 'file_name', sort_order: 'asc' }),
                absent.length ? this._fetchFiles(params, { collection, exact_md5: absent.join(','), limit: absent.length }) : null,
            ]);
            Object.assign(this._clusters, sub.bin_cluster_map || {}, (self && self.bin_cluster_map) || {});
            missing.forEach(r => this._uploads.set(r, []));
            for (const d of [...((self && self.files) || []), ...(sub.files || [])]) {
                const r = d.root_md5 || d.file_md5;
                if (this._uploads.has(r)) this._uploads.get(r).push(d);
            }
            if ((sub.total || 0) > (sub.files || []).length) this._truncated = true;
        }
        return roots.flatMap(r => this._uploads.get(r) || []);
    },

    async _byArchive(ctx, collection) {
        const hits = new Map(this._items.map(f => [f.file_md5, f]));
        const roots = [];
        for (const f of this._items) {
            const root = f.root_md5 || (f.child_count ? f.file_md5 : null);
            if (root && !roots.includes(root)) roots.push(root);
        }
        const docs = await this._loadUploads(ctx.params, collection, roots, hits);

        // Page hits win over the subtree copies: same doc, but they are what matched.
        const nodes = new Map();
        docs.forEach(d => { if (!nodes.has(d.file_md5)) nodes.set(d.file_md5, d); });
        hits.forEach((f, md5) => nodes.set(md5, f));

        const kids = new Map();
        for (const n of [...nodes.values()]) {
            const p = n.parent_md5;
            if (!p || p === n.file_md5) continue;
            if (!nodes.has(p)) {
                // Declared as a container but never uploaded: a band with a name only.
                nodes.set(p, { file_md5: p, file_name: n.parent_file_name || p, exists: false, is_container: true, collection });
            }
            if (!kids.has(p)) kids.set(p, []);
            kids.get(p).push(n);
        }
        const byPath = (a, b) => (a.path_in_parent || a.file_name || '').localeCompare(b.path_in_parent || b.file_name || '');
        kids.forEach(list => list.sort((a, b) => (kids.has(a.file_md5) - kids.has(b.file_md5)) || byPath(a, b)));

        const hitBelow = new Map();
        const hasHit = n => {
            if (!hitBelow.has(n.file_md5)) {
                hitBelow.set(n.file_md5, false);  // cycle guard
                hitBelow.set(n.file_md5, hits.has(n.file_md5) || (kids.get(n.file_md5) || []).some(hasHit));
            }
            return hitBelow.get(n.file_md5);
        };
        const drawn = new Set();
        const out = this._truncated ? [this._noteRow('', 0,
            `Archive contents are capped at ${this.SUBTREE_LIMIT.toLocaleString()} files per load, so some archives below are partial. Narrow the search to see them whole.`)] : [];

        // `filtered` drops to false inside an "other files" fold: context the
        // analyst opened on purpose is shown whole, not folded again.
        const draw = (n, depth, anc, parent, filtered = ctx.filtered) => {
            if (drawn.has(n.file_md5)) return;
            drawn.add(n.file_md5);
            const path = this._split(parent ? (n.path_in_parent || n.file_name) : n.file_name);
            const tree = { anc: anc.join(' '), prefix: parent ? path.dir : '', base: parent ? path.base : null, unpacked: parent && !parent.is_container };
            const children = kids.get(n.file_md5);
            if (!children) {
                out.push(n.exists === false ? '' : this._row(n, depth, tree));
                return;
            }

            const id = `b:${n.file_md5}`;
            const files = this._descendants(n, kids);
            tree.band = true;
            tree.id = id;
            tree.sub = `${files.length} file${files.length === 1 ? '' : 's'} inside${filtered && !hits.has(n.file_md5) ? ' · shown for context' : ''}`;
            out.push(n.exists === false ? this._stubBand(n, depth, tree) : this._row(n, depth, tree));

            const inner = [...anc, id];
            const shown = [], tagged = [], other = [];
            for (const c of children) {
                if (filtered && !hasHit(c)) other.push(c);
                else if (!kids.has(c.file_md5) && this._folded(c, ctx.folds) && !(filtered && hits.has(c.file_md5))) tagged.push(c);
                else shown.push(c);
            }
            shown.forEach(c => draw(c, depth + 1, inner, n, filtered));
            if (tagged.length) {
                const fid = `f:${n.file_md5}:tag`;
                out.push(this._lineRow(fid, inner.join(' '), depth + 1, this._foldLabel(tagged, ctx.folds)));
                tagged.forEach(c => draw(c, depth + 1, [...inner, fid], n, filtered));
            }
            if (other.length) {
                const fid = `f:${n.file_md5}:other`;
                const count = other.reduce((s, c) => s + (kids.has(c.file_md5) ? this._descendants(c, kids).length + 1 : 1), 0);
                out.push(this._lineRow(fid, inner.join(' '), depth + 1,
                    `<b>${count} other file${count === 1 ? '' : 's'}</b> <span class="dim">in this archive did not match</span>`));
                other.forEach(c => draw(c, depth + 1, [...inner, fid], n, false));
            }
        };

        for (const f of this._items) {
            let top = f;
            const seen = new Set();
            while (top.parent_md5 && nodes.has(top.parent_md5) && !seen.has(top.file_md5)) {
                seen.add(top.file_md5);
                top = nodes.get(top.parent_md5);
            }
            draw(top, 0, [], null);
        }
        return out.join('');
    },

    _descendants(n, kids) {
        const out = [];
        const walk = m => (kids.get(m.file_md5) || []).forEach(c => {
            if (out.includes(c)) return;
            if (!c.is_container && c.exists !== false) out.push(c);
            walk(c);
        });
        walk(n);
        return out;
    },

    _stubBand(n, depth, tree) {
        return `<tr class="sim-row ft-band" data-ft-anc="${escapeAttr(tree.anc)}" style="font-size:0.75rem;">
            <td class="sim-cell">${this.nameCell(n, n.collection, depth, tree, Lineage.nodeName(n, n.collection, { max: 40 }))}</td>
            <td class="sim-cell" colspan="${this._span - 1}"><span class="dim">Declared as a container but never uploaded</span></td></tr>`;
    },

    // --- by cluster ------------------------------------------------------

    _byCluster(ctx) {
        const axis = ctx.params.get('group_axis') || 'code';
        const wantContainers = ctx.params.get('group_nodes') === 'container';
        const items = this._items.filter(f => !!f.is_container === wantContainers);

        const groups = new Map();
        const none = [];
        for (const f of items) {
            const ids = Array.isArray(f.bin_clusters) ? f.bin_clusters : [];
            const best = ids.map(u => this._clusters[u])
                .filter(c => c && (c.axis || 'overall') === axis)
                .sort((a, b) => (b.cohesion_score || 0) - (a.cohesion_score || 0))[0];
            if (!best) { none.push(f); continue; }
            if (!groups.has(best.cluster_uuid)) groups.set(best.cluster_uuid, []);
            groups.get(best.cluster_uuid).push(f);
        }
        const order = [...groups.entries()].sort((a, b) =>
            (b[1].length - a[1].length) || ((this._clusters[b[0]].cohesion_score || 0) - (this._clusters[a[0]].cohesion_score || 0)));
        if (none.length) order.push([null, none]);

        const out = [];
        for (const [uuid, files] of order) {
            const id = `g:${uuid || 'none'}`;
            const c = uuid ? this._clusters[uuid] : null;
            const funcs = files.reduce((s, f) => s + (Number(f.function_count) || 0), 0);
            const head = c
                ? `${renderClusterCards([c], true)}
                   <b class="lineage-link" onclick="event.stopPropagation(); openCluster(${escapeAttr(jsString(uuid))}, true, event, ${escapeAttr(jsString(axis))})">${escapeHtml(c.cluster_name || `Cluster ${c.cluster_id}`)}</b>
                   <span class="dim">${files.length} on this page · ${Number(c.member_count || 0)} members · cohesion ${(c.cohesion_score || 0).toFixed(2)} · ${funcs.toLocaleString()} functions</span>`
                : `<b>No ${escapeHtml(axis)} cluster</b> <span class="dim">${files.length} on this page · ${funcs.toLocaleString()} functions</span>`;
            out.push(`<tr class="ft-group" data-ft-anc="">
                <td colspan="${this._span}"><div class="ft-group-head">
                    <span class="ft-chev" data-ft-toggle="${escapeAttr(id)}" onclick="FileTree.toggle(event, ${escapeAttr(jsString(id))})"><i class="fa-solid fa-chevron-down"></i></span>${head}
                </div></td></tr>`);

            const shown = [], tagged = [];
            files.forEach(f => (this._folded(f, ctx.folds) ? tagged : shown).push(f));
            const ctxOf = f => {
                if (!f.parent_md5) return {};
                const p = this._split(f.path_in_parent || f.file_name);
                return { prefix: `${f.parent_file_name || f.parent_md5} › ${p.dir}`, base: p.base };
            };
            shown.forEach(f => out.push(this._row(f, 1, { anc: id, ...ctxOf(f) })));
            if (tagged.length) {
                const fid = `f:${id}:tag`;
                out.push(this._lineRow(fid, id, 1, this._foldLabel(tagged, ctx.folds)));
                tagged.forEach(f => out.push(this._row(f, 1, { anc: `${id} ${fid}`, ...ctxOf(f) })));
            }
        }
        if (!out.length) {
            out.push(`<tr><td colspan="${this._span}" style="text-align:center; padding:30px;" class="dim">No ${wantContainers ? 'archives' : 'files'} on this page.</td></tr>`);
        } else {
            // ponytail: groups are built from the loaded page; a cluster bigger
            // than a page splits. Page by cluster via the bin-cluster members
            // route if that gets in the way.
            out.push(`<tr class="ft-note"><td colspan="${this._span}">Grouped from the ${this._items.length.toLocaleString()} result${this._items.length === 1 ? '' : 's'} loaded so far. Load more to fill the groups.</td></tr>`);
        }
        return out.join('');
    },
};
