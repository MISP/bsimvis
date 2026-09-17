/**
 * Cluster Detail View
 * Loaded when navigating to /collections/{col}/functions/clusters/{uuid}
 *                       or /collections/{col}/files/clusters/{uuid}
 *
 * A cluster had no page of its own: every cluster link dropped the user onto a
 * filtered member list, which answers "what is in it" and nothing else. This
 * view answers "what is it, what is in it, and where does it sit" -- the
 * cluster's own metadata, its members, and the branch of the hierarchy around
 * it.
 *
 * No new backend. list_clusters / list_bin_clusters already accept
 * show_parents / show_children / show_members and resolve parent-child edges
 * from {coll}:cluster:tree_links:{algo}, which is exactly a local tree. The
 * server walks all the way to the root and over the whole subtree with no depth
 * parameter, so the pruning to one level each way happens here.
 */

window.ClusterDetailView = {
    params: null,
    isBinary: false,

    destroy() {
        this.params = null;
    },

    /** Which API family this cluster belongs to. */
    api(path) {
        return this.isBinary ? `/api/bin_cluster/${path}` : `/api/cluster/${path}`;
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        this.params = params;
        this.isBinary = params.view === 'bin-cluster-detail';

        const uuid = params.cluster_uuid;
        if (!uuid) {
            container.innerHTML = '<div style="padding:30px; color:#f87171;">Error: No cluster UUID provided.</div>';
            return;
        }

        container.innerHTML = `
            <div style="display:flex; justify-content:center; align-items:center; height:200px; color:var(--dim);">
                <i class="fa-solid fa-spinner fa-spin" style="margin-right:10px;"></i> Loading cluster...
            </div>`;

        try {
            const qs = new URLSearchParams();
            if (params.pool) qs.set('pool', params.pool);
            if (params.collection) qs.set('collection', params.collection);
            qs.set('cluster_uuid', uuid);
            qs.set('show_parents', 'true');
            qs.set('show_children', 'true');
            qs.set('show_members', 'true');
            // The expansion returns the ancestor chain and the whole subtree
            // alongside the match, so the page size has to cover them, not just
            // the one cluster asked for.
            qs.set('limit', '500');

            const res = await fetch(`${this.api('list')}?${qs.toString()}`);
            if (!res.ok) throw new Error(`Cluster lookup failed (${res.status})`);
            const data = await res.json();

            const all = data.results || [];
            // uuid matching is a substring test server-side; pin the exact one.
            const self = all.find(c => String(c.cluster_uuid) === String(uuid))
                || all.find(c => String(c.cluster_uuid || '').startsWith(String(uuid)));

            if (!self) {
                container.innerHTML = `<div style="padding:30px; color:var(--dim);">No cluster matching <code>${escapeHtml(uuid)}</code>.</div>`;
                return;
            }

            container.innerHTML = this.render(self, all);
            this.setBreadcrumb(self);
            if (window.TableSelection) new window.TableSelection('cluster-members-table');
        } catch (e) {
            console.error(e);
            container.innerHTML = `<div style="padding:30px; color:#f92672;">
                <i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(e.message)}</div>`;
        }
    },

    setBreadcrumb(self) {
        if (typeof Breadcrumbs === 'undefined' || !Breadcrumbs.refresh) return;
        Breadcrumbs.setClusterName(self.cluster_uuid, self.cluster_name || `#${self.cluster_id}`);
        Breadcrumbs.refresh();
    },

    /**
     * The branch around this cluster: its immediate parent, itself, and its
     * direct children. The response carries the whole ancestor chain and
     * subtree, so anything further out is dropped here rather than asked for.
     */
    localTree(self, all) {
        const byId = {};
        all.forEach(c => { byId[String(c.cluster_id)] = c; });

        const parent = self.parent ? byId[String(self.parent)] : null;
        const grandparent = parent && parent.parent ? byId[String(parent.parent)] : null;
        const children = all.filter(c => String(c.parent) === String(self.cluster_id));

        return { grandparent, parent, self, children };
    },

    render(self, all) {
        const { grandparent, parent, children } = this.localTree(self, all);
        const memberWord = this.isBinary ? 'Files' : 'Functions';

        return `
        <div style="flex:1; overflow-y:auto; padding:20px 24px;">
            ${this.renderHeader(self)}

            <div style="display:flex; gap:20px; flex-wrap:wrap; align-items:flex-start; margin-top:20px;">
                <div style="flex:1 1 320px; min-width:300px;">
                    <h3 style="font-size:0.8rem; text-transform:uppercase; letter-spacing:1px; color:var(--accent); margin:0 0 10px;">Hierarchy</h3>
                    ${this.renderTree(grandparent, parent, self, children)}
                </div>
                <div style="flex:2 1 460px; min-width:340px;">
                    <h3 style="font-size:0.8rem; text-transform:uppercase; letter-spacing:1px; color:var(--accent); margin:0 0 10px;">
                        ${memberWord} <span class="dim">(${Number(self.count || 0).toLocaleString()})</span>
                    </h3>
                    ${this.renderMembers(self)}
                </div>
            </div>
        </div>`;
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
                <span class="badge">${this.isBinary ? 'binary' : 'function'}</span>
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

    /** The old destination, kept as an explicit action rather than a surprise. */
    renderMemberListLink(self) {
        const col = this.params.collection || '';
        const segs = this.isBinary ? ['files'] : ['functions'];
        const key = this.isBinary ? 'bin_cluster_uuid' : 'cluster_uuid';
        const url = `${Nav.buildUIUrl(col, segs)}?${key}=${encodeURIComponent(self.cluster_uuid)}`;
        return `<a href="${escapeAttr(url)}" class="ui-button" onclick="Nav.openPath(this.href, event)">
            <i class="fa-solid fa-list"></i> Open in ${this.isBinary ? 'file' : 'function'} search
        </a>`;
    },

    clusterUrl(c) {
        const col = this.params.collection || '';
        const segs = this.isBinary ? ['files', 'clusters'] : ['functions', 'clusters'];
        return Nav.buildUIUrl(col, segs.concat([c.cluster_uuid]));
    },

    renderTree(grandparent, parent, self, children) {
        const node = (c, opts = {}) => {
            const label = escapeHtml(c.cluster_name || `Cluster #${c.cluster_id}`);
            const count = Number(c.count || 0).toLocaleString();
            const body = `${label} <span class="dim" style="font-size:0.72rem;">(${count})</span>`;
            if (opts.current) {
                return `<div style="padding:6px 10px; border-radius:6px; background:var(--hover); border:1px solid var(--accent);">
                    <i class="fa-solid fa-bullseye" style="color:var(--accent); width:14px;"></i> <b>${body}</b>
                </div>`;
            }
            return `<div style="padding:5px 10px;">
                <a href="${escapeAttr(this.clusterUrl(c))}" style="color:var(--accent);"
                   onclick="Nav.openPath(this.href, event)">
                   <i class="fa-solid ${opts.up ? 'fa-turn-up' : 'fa-turn-down'}" style="width:14px; opacity:0.7;"></i> ${body}
                </a>
            </div>`;
        };

        const indent = (html, depth) => `<div style="margin-left:${depth * 18}px;">${html}</div>`;

        let depth = 0;
        let html = '';
        if (grandparent) html += indent(node(grandparent, { up: true }), depth++);
        if (parent) html += indent(node(parent, { up: true }), depth++);
        html += indent(node(self, { current: true }), depth++);

        if (children.length) {
            html += children.map(c => indent(node(c), depth)).join('');
        } else {
            html += indent('<div class="dim" style="padding:5px 10px; font-size:0.78rem;">No child clusters.</div>', depth);
        }

        if (!parent && !children.length) {
            html += `<div class="dim" style="font-size:0.75rem; margin-top:8px;">
                This cluster stands alone in the hierarchy.</div>`;
        }

        return `<div style="background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:12px;">${html}</div>`;
    },

    renderMembers(self) {
        const members = self.direct_members || self.sample_members || [];
        if (!members.length) {
            return `<div class="dim" style="padding:14px; background:var(--card-bg); border:1px solid var(--border); border-radius:8px;">
                No members returned for this cluster.</div>`;
        }

        const col = this.params.collection || '';
        // direct_members is a compact projection -- {id, name, addr, bin,
        // file_md5} for functions, {id, name, file_md5, language_id, tags} for
        // files -- not the row shape the entity renderers take, so map it.
        const rows = members.map(m => {
            const memberCol = String(m.id || '').split(':')[0] || col;
            const md5 = m.file_md5 || '';

            if (this.isBinary) {
                return `<tr data-id="${escapeAttr(m.id || md5)}">
                    <td>${EntityRenderer.renderFileName(m.name || '', md5, memberCol)}</td>
                    <td>${EntityRenderer.renderMd5(md5, { collection: memberCol })}</td>
                    <td class="dim">${escapeHtml(m.language_id || '---')}</td>
                </tr>`;
            }

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
            return `<tr data-id="${escapeAttr(m.id || '')}">
                <td>${EntityRenderer.renderFunction(f)}</td>
                <td>${EntityRenderer.renderMd5(md5, { collection: memberCol })}</td>
                <td class="dim">${escapeHtml(m.bin || '---')}</td>
            </tr>`;
        }).join('');

        const total = Number(self.count || members.length);
        const shown = members.length;
        const note = shown < total
            ? `<div class="dim" style="padding:8px 12px; font-size:0.75rem;">
                 Showing ${shown.toLocaleString()} of ${total.toLocaleString()} -- open the search above for the full list.</div>`
            : '';

        return `
        <div class="table-container" style="border:1px solid var(--border); border-radius:8px; background:var(--card-bg);">
            <table id="cluster-members-table" style="width:100%; border-collapse:collapse; font-size:0.8rem;">
                <thead>
                    <tr style="border-bottom:1px solid var(--border); color:var(--dim);">
                        <th data-label="${this.isBinary ? 'File Name' : 'Function'}" style="padding:8px 12px; text-align:left;">${this.isBinary ? 'File Name' : 'Function'}</th>
                        <th data-label="MD5" style="padding:8px 12px; text-align:left;">MD5</th>
                        <th data-label="${this.isBinary ? 'Arch' : 'File Name'}" style="padding:8px 12px; text-align:left;">${this.isBinary ? 'Arch' : 'File Name'}</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
            ${note}
        </div>`;
    },
};
