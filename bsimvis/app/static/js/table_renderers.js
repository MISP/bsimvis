/**
 * table_renderers.js
 * Extracted table rendering logic from dashboard.js.
 */

/**
 * Trailing "Collection" cell for pool-scoped search tables. Renders nothing
 * outside a pool (or when the pool view is already pinned to one collection),
 * so it stays in lockstep with the header injected in dashboard.js.
 * Pass a second collection for pair rows (similarities).
 */
function renderCollectionCell(colA, colB) {
    const { pool, collection } = getRoutingState();
    if (!pool || collection) return '';
    const link = c => c
        ? `<a href="/collections/${encodeURIComponent(c)}" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="color:var(--accent);">${escapeHtml(c)}</a>`
        : '<span class="dim">---</span>';
    const cols = (colB === undefined || colB === colA) ? [colA] : [colA, colB];
    return `<td class="sim-cell" style="font-size:0.7rem;">
        <div style="display:flex; flex-direction:column; gap:8px;">
            ${cols.map(c => `<div style="min-height:24px; display:flex; align-items:center;">${link(c)}</div>`).join('')}
        </div>
    </td>`;
}
window.renderCollectionCell = renderCollectionCell;

function createNav(view, collection, params = {}) {
    let pathSegments = [];
    if (view === 'upload') {
        pathSegments = ['upload'];
    } else if (view === 'function-similarity') {
        pathSegments = ['functions', 'similarities'];
    } else if (view === 'binary-similarity') {
        pathSegments = ['files', 'similarities'];
    } else if (view === 'clusters') {
        pathSegments = ['functions', 'clusters'];
    } else if (view === 'bin-clusters') {
        pathSegments = ['files', 'clusters'];
    } else if (view === 'features-global') {
        pathSegments = ['features'];
    } else {
        pathSegments = [view];
    }
    
    // Convert params object to path segments if possible,
    // otherwise append as query parameters (though we want to avoid this).
    // Let's analyze the parameters used: batch_uuid, file_md5.
    
    let url = Nav.buildUIUrl(collection, pathSegments);
    
    // For now, handle known parameters as query parameters.
    if (params.batch_uuid) {
        url += `?batch_uuid=${encodeURIComponent(params.batch_uuid)}`;
    } else if (params.file_md5) {
        url += `?file_md5=${encodeURIComponent(params.file_md5)}`;
    }
    
    // If there are other params, we might have to keep them as query params for now
    // until the backend routes are updated to fully support these as paths.
    // For this step, I'll focus on the identified ones.
    
    return `href="${escapeAttr(url)}" onclick="Nav.openPath(${escapeAttr(jsString(url))}, event)"`;
}

// Shared "is this row being worked on right now" badge for the batches/files
// tables. Reads window.activeJobsByTarget (jobs.js:refreshActiveJobsByTarget),
// which is kept warm by the existing 3s nav-job-status poll.
function jobStatusBadgeHtml(target) {
    const job = target && window.activeJobsByTarget && window.activeJobsByTarget[target];
    if (!job) return '<span class="dim" style="font-size:0.7rem;">—</span>';
    return `
        <span class="row-job-status-badge" onclick="event.stopPropagation(); showJobDetails('${job.id}')" title="${job.type || 'job'} — click to view log" style="display:inline-flex; align-items:center; gap:6px; cursor:pointer; font-size:0.7rem; color:var(--accent); background:rgba(4,217,255,0.08); border:1px solid rgba(4,217,255,0.25); border-radius:4px; padding:3px 8px;">
            <span class="nav-job-spinner" style="width:10px; height:10px; margin:0;"></span> ${job.type || 'running'}
        </span>
    `;
}

function jobStatusCell(target) {
    return `<span class="job-status-cell" data-job-target="${target}">${jobStatusBadgeHtml(target)}</span>`;
}

// File rows carry a real lifecycle (data-service-set on upload/analysis, see
// index_service.update_file_status), not just "is a job running right now" --
// so they get their own badge instead of jobStatusCell's generic one.
function fileStatusBadgeHtml(f) {
    // Files indexed before this field existed have no `status` -- they are,
    // by definition, already fully analyzed.
    const status = f.status || 'analyzed';
    const cssStatus = status === 'analyzing' ? 'running' : (status === 'analyzed' ? 'completed' : status);
    const icons = {
        pending: 'fa-clock',
        analyzing: 'fa-circle-notch fa-spin',
        failed: 'fa-exclamation-circle',
        analyzed: 'fa-check-circle',
    };
    const target = f['file_md5'];
    const job = target && window.activeJobsByTarget && window.activeJobsByTarget[target];
    const clickAttr = job
        ? `onclick="event.stopPropagation(); showJobDetails(${escapeAttr(jsString(job.id))})" style="cursor:pointer;" title="${escapeHtml(job.type || 'job')} — click to view log"`
        : '';
    return `<span class="job-status-badge status-${cssStatus}" ${clickAttr}><i class="fa-solid ${icons[status] || icons.analyzed}"></i> ${escapeHtml(status.toUpperCase())}</span>`;
}

window.refreshVisibleStatusBadges = function () {
    document.querySelectorAll('.job-status-cell[data-job-target]').forEach(el => {
        el.outerHTML = jobStatusCell(el.dataset.jobTarget);
    });
};

window.TableRenderers = {
    renderPools: function(data) {
        if (!data || !data.length) return '<tr><td colspan="6" style="text-align:center">No pools found.</td></tr>';

        return data.map(pool => {
            Breadcrumbs.setPoolName(pool.id, pool.name || 'Unnamed Pool');
            const collectionsList = (pool.collections || []).map(c => `
                <a href="/collections/${encodeURIComponent(c)}" onclick="Nav.openPath(this.href, event)" style="font-size:0.75rem; background:rgba(96,165,250,0.1); border:1px solid rgba(96,165,250,0.3); color:#60a5fa; padding:2px 8px; border-radius:4px; margin-right:4px; text-decoration:none; cursor:pointer;" class="clickable-count">${escapeHtml(c)}</a>
            `).join('');

            const crossCollectionOnlyIndicator = pool.only_cross_collection ? `
                <span style="font-size:0.75rem; background:rgba(245, 158, 11, 0.15); border:1px solid rgba(245, 158, 11, 0.3); color:#f59e0b; padding:2px 8px; border-radius:4px; margin-right:4px; display:inline-flex; align-items:center; gap:4px;" title="Cross-Collection Only">
                    <i class="fa-solid fa-arrow-right-arrow-left"></i> Cross-Only
                </span>
            ` : '';

            let syncStatusBadge = '';
            if (pool.sync_status === 'current') {
                syncStatusBadge = `<span style="font-size:0.75rem; background:rgba(16, 185, 129, 0.15); border:1px solid rgba(16, 185, 129, 0.3); color:#10b981; padding:2px 8px; border-radius:4px; font-weight:bold;">Up to date</span>`;
            } else if (pool.sync_status === 'outdated') {
                syncStatusBadge = `<span style="font-size:0.75rem; background:rgba(245, 158, 11, 0.15); border:1px solid rgba(245, 158, 11, 0.3); color:#f59e0b; padding:2px 8px; border-radius:4px; font-weight:bold;">Outdated</span>`;
            } else {
                syncStatusBadge = `<span style="font-size:0.75rem; background:rgba(156, 163, 175, 0.15); border:1px solid rgba(156, 163, 175, 0.3); color:#9ca3af; padding:2px 8px; border-radius:4px; font-weight:bold;">${escapeHtml(pool.sync_status || 'created')}</span>`;
            }

            const buildBtn = pool.sync_status === 'outdated' ? `
                <button onclick="buildPool(${escapeAttr(jsString(pool.id))}, this)" class="btn-action" title="Build/Sync Pool" style="background:rgba(59,130,246,0.1); border:1px solid rgba(59,130,246,0.3); border-radius:4px; color:#60a5fa; cursor:pointer; padding:3px 8px !important; font-size:0.75rem; display:inline-flex; align-items:center; gap:4px; margin-left:6px; font-weight:bold; height:24px; box-sizing:border-box; white-space:nowrap; width:auto !important; min-width:max-content !important;">
                    <i class="fa-solid fa-play" style="font-size:0.7rem;"></i> Build
                </button>
            ` : '';

            const rebuildBtn = `
                <button onclick="rebuildPool(${escapeAttr(jsString(pool.id))}, this)" class="btn-action" title="Wipe & Rebuild Pool" style="background:rgba(168,85,247,0.15); border:1px solid rgba(168,85,247,0.3); border-radius:4px; color:#c084fc; cursor:pointer; padding:3px 8px !important; font-size:0.75rem; display:inline-flex; align-items:center; gap:4px; font-weight:bold; height:24px; box-sizing:border-box; white-space:nowrap; width:auto !important; min-width:max-content !important;">
                    <i class="fa-solid fa-rotate" style="font-size:0.7rem;"></i> Rebuild
                </button>
            `;

            const poolUrl = '/pools/' + encodeURIComponent(pool.id);
            return `
            <tr data-id="${escapeAttr(pool.id)}">
                <td><a href="${escapeAttr(poolUrl)}" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="font-weight:bold;">${escapeHtml(pool.id)}</a></td>
                <td>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <a href="${escapeAttr(poolUrl)}" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="font-weight:bold;" id="pool-name-${escapeAttr(pool.id)}">${escapeHtml(pool.name || 'Unnamed')}</a>
                        <button class="btn-action" title="Rename" onclick="event.stopPropagation(); renamePool(${escapeAttr(jsString(pool.id))})"><i class="fa-solid fa-pen"></i></button>
                    </div>
                </td>
                <td><div style="display:flex; flex-wrap:wrap; gap:4px; align-items:center;">${collectionsList}${crossCollectionOnlyIndicator}</div></td>
                <td><a href="/pools/${encodeURIComponent(pool.id)}/files" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="color:#60a5fa; font-weight:700; text-decoration:none;">${pool.total_files || 0}</a></td>
                <td><a href="/pools/${encodeURIComponent(pool.id)}/functions" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="color:#a78bfa; font-weight:700; text-decoration:none;">${pool.total_functions || 0}</a></td>
                <td><a href="/pools/${encodeURIComponent(pool.id)}/functions/similarities" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="color:var(--info); font-weight:700; text-decoration:none;">${pool.total_func_similarities || 0}</a></td>
                <td><a href="/pools/${encodeURIComponent(pool.id)}/functions/clusters" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="color:#f59e0b; font-weight:700; text-decoration:none;">${pool.total_func_clusters || 0}</a></td>
                <td>
                    <div style="display:inline-flex; align-items:center;">
                        ${syncStatusBadge}
                        ${buildBtn}
                    </div>
                </td>
                <td class="dim">${formatDate(pool.created_at)}</td>
                <td>
                    <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
                        ${rebuildBtn}
                        <button onclick="deletePool(${escapeAttr(jsString(pool.id))}, this)" class="btn-action" title="Delete Pool" style="background:rgba(239,68,68,0.1); border:1px solid rgba(239,68,68,0.3); border-radius:4px; color:#f87171; cursor:pointer; padding:3px 8px !important; font-size:0.75rem; display:inline-flex; align-items:center; gap:4px; font-weight:bold; height:24px; box-sizing:border-box; white-space:nowrap; width:auto !important; min-width:max-content !important;">
                            <i class="fa-solid fa-trash-can" style="font-size:0.7rem;"></i> Delete
                        </button>
                    </div>
                </td>
            </tr>
            `;
        }).join('');
    },

    renderCollections: function(data) {
        if (!data.length) return '<tr><td colspan="6" style="text-align:center">No collections found.</td></tr>';

        return data.map(col => `
            <tr data-id="${escapeAttr(col.name)}">
                <td><a href="/collections/${encodeURIComponent(col.name)}" onclick="Nav.openPath(this.href, event)" class="clickable-count" style="font-weight:bold;">${escapeHtml(col.name)}</a></td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('batches', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_batches'] || 0}</a>
                        <a ${createNav('batches', col.name)} class="btn-action" title="Batches">
                            <i class="fa-solid fa-boxes-stacked"></i>
                        </a>
                    </div>
                </td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('files', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_files']}</a>
                        <a ${createNav('files', col.name)} class="btn-action" title="Files">
                            <i class="fa-solid fa-file-code"></i>
                        </a>
                    </div>
                </td>
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <a ${createNav('functions', col.name)} class="clickable-count" style="font-weight: bold; min-width: 20px; text-align: right;">${col['total_functions']}</a>
                        <a ${createNav('functions', col.name)} class="btn-action" title="Functions">
                            <i class="fa-solid fa-code"></i>
                        </a>
                    </div>
                </td>
                <td class="dim">${formatDate(col['last_updated'])}</td>
                <td>${jobStatusCell(col.name)}</td>
                <td>
                    <div style="display: flex; gap: 15px;">
                        <a ${createNav('upload', col.name)} class="btn-action" title="Upload" style="color:var(--accent); display: flex; align-items: center; gap: 5px; text-decoration: none;">
                            <i class="fa-solid fa-cloud-arrow-up"></i>
                            <span style="font-size: 0.8rem;">Upload</span>
                        </a>
                    </div>
                </td>
            </tr>
        `).join('');
    },

    renderBatches: function(data) {
        return data.map(b => {
            const col = b.collection || 'unknown';
            return `
            <tr data-id="${escapeAttr(b['batch_uuid'])}">
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <b>${escapeHtml(b.name || 'Unnamed')}</b>
                        <button class="btn-copy" title="Copy Batch ID: ${escapeAttr(b['batch_id'])}" onclick="copyToClipboard(${escapeAttr(jsString(b['batch_id']))}, this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </td>
                <td class="mono dim" style="font-size:0.7rem">${escapeHtml(b['batch_uuid'])}</td>
                <td class="mono"><a ${createNav('files', col, { batch_uuid: b['batch_uuid'] })} class="clickable-count">${b['total_files']}</a></td>
                <td class="mono"><a ${createNav('functions', col, { batch_uuid: b['batch_uuid'] })} class="clickable-count">${b['total_functions']}</a></td>
                <td class="dim">${formatDate(b['last_updated'] || b['created_at'])}</td>
                <td>${jobStatusCell(b['batch_uuid'])}</td>
                <td>
                    <div style="display: flex; gap: 15px;">
                        <a ${createNav('upload', col, { batch_uuid: b['batch_uuid'] })} class="btn-action" title="Upload to Batch" style="color:var(--accent)">
                            <i class="fa-solid fa-cloud-arrow-up"></i>
                        </a>
                    </div>
                </td>
            </tr>
        `}).join('');
    },
    
    // ... renderFiles (needs update)

    /**
     * A page of file rows, drawn as containment trees.
     *
     * Every match is rendered exactly once, underneath the container it came
     * out of. Whether the container matched the filter too makes no difference
     * to where the child sits: if the container is on the page it becomes the
     * child's parent row, and if it is not, a context row standing for it is
     * drawn instead. So a filtered search never yields a file floating free of
     * its archive, and never yields the same file in two places.
     *
     * `opts.injected` renders the given rows flat at `opts.depth` instead --
     * that is the path Lineage.toggleRow() uses for rows it splices in by hand.
     */
    renderFiles: function(data, clustersMap = {}, opts = {}) {
        if (opts.injected) {
            return TableRenderers._fileRows(data, clustersMap, opts.depth || 0, opts.parentMd5, true);
        }
        return TableRenderers._fileForest(data, clustersMap);
    },

    /**
     * Groups a page into trees, then walks them in the order the server sent.
     * A parent that is itself on the page anchors its children; a parent that
     * is not gets one context row, shared by all of its matches.
     */
    _fileForest: function(data, clustersMap) {
        const onPage = new Map(data.map(f => [f['file_md5'], f]));
        const kids = new Map();      // on-page parent md5 -> its matching children
        const absent = new Map();    // off-page parent md5 -> context row + children

        for (const f of data) {
            const p = f['parent_md5'];
            if (!p || p === f['file_md5']) continue;
            if (onPage.has(p)) {
                if (!kids.has(p)) kids.set(p, []);
                kids.get(p).push(f);
            } else {
                if (!absent.has(p)) {
                    absent.set(p, { file_md5: p, file_name: f['parent_file_name'] || p, rows: [] });
                }
                absent.get(p).rows.push(f);
            }
        }

        const drawn = new Set();
        const subtree = (f, depth) => {
            if (drawn.has(f['file_md5'])) return '';
            drawn.add(f['file_md5']);
            let out = TableRenderers._fileRows([f], clustersMap, depth, f['parent_md5']);
            for (const c of (kids.get(f['file_md5']) || [])) out += subtree(c, depth + 1);
            return out;
        };

        let html = '';
        for (const f of data) {
            if (drawn.has(f['file_md5'])) continue;
            const p = f['parent_md5'];
            if (p && onPage.has(p)) continue;        // drawn under its parent instead
            if (p) {
                const group = absent.get(p);
                if (group.drawn) continue;
                group.drawn = true;
                html += TableRenderers._contextRow(group, f);
                for (const c of group.rows) html += subtree(c, 1);
            } else {
                html += subtree(f, 0);
            }
        }
        // A parent_md5 cycle would leave rows unreachable from any root.
        for (const f of data) {
            if (!drawn.has(f['file_md5'])) html += subtree(f, 0);
        }
        return html;
    },

    /**
     * Stands in for a container that holds matches but did not match itself.
     * Identity only -- it is context, not a result -- and it expands like any
     * other container row.
     */
    _contextRow: function(group, child) {
        const { collection } = getRoutingState();
        const col = child['collection'] || (child['file_id'] || '').split(':')[0] || collection;
        const name = escapeHtml(middleTruncate(group.file_name, 46));
        return `
            <tr class="sim-row lineage-context-row" style="font-size:0.75rem; opacity:0.8;"
                data-lineage-depth="0" data-lineage-md5="${escapeAttr(group.file_md5)}"
                data-lineage-col="${escapeAttr(col)}" data-lineage-open="0">
                <td class="sim-cell">
                    <div class="lineage-cell">
                        ${Lineage.toggleButton(group.rows.length)}
                        <i class="fa-solid fa-box-archive dim" title="Container of the matches below" style="font-size:0.7rem;"></i>
                        <b class="lineage-link" title="${escapeAttr(group.file_name)}"
                            onclick="openFileDetails(${escapeAttr(jsString(col))}, ${escapeAttr(jsString(group.file_md5))}, ${escapeAttr(jsString(group.file_name))}, event)">${name}</b>
                        <span class="dim" style="font-size:0.65rem;">contains ${group.rows.length} match${group.rows.length === 1 ? '' : 'es'}</span>
                    </div>
                </td>
                <td class="sim-cell">${EntityRenderer.renderMd5(group.file_md5, { full: true })}</td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                <td class="sim-cell"></td>
                ${renderCollectionCell(col)}
            </tr>`;
    },

    /** The file rows themselves, all at one depth under one parent. */
    _fileRows: function(rows, clustersMap, depth, parentMd5, injected) {
        const { collection } = getRoutingState();
        return rows.map(f => {
            // Base collection: pool searches span collections, so fall back to the id prefix.
            const col = f.collection || (f['file_id'] || '').split(':')[0] || collection;
            const fileId = f['file_id'] || `${col}:file:${f['file_md5']}`;
            let targetCol = col;
            const tags = f['tags'] || [];
            const user_tags = f['user_tags'] || [];
            const rowStyle = getRowTagColor(tags, user_tags);
            const batchUuid = f['batch_uuid'] || '---';
            const funcCount = f['function_count'] !== undefined ? f['function_count'] : 0;

            const clusters = (Array.isArray(f['bin_clusters']) ? f['bin_clusters'] : []).map(cid => clustersMap[cid]).filter(Boolean);

            // A node the lineage edges declared but whose bytes were never
            // uploaded has no name of its own to link to.
            const nameHtml = (f['exists'] === false)
                ? Lineage.nodeName(f, targetCol, { max: 40 })
                : `<b style="color:var(--accent); cursor:pointer;" onclick="openFileDetails(${escapeAttr(jsString(targetCol))}, ${escapeAttr(jsString(f['file_md5']))}, ${escapeAttr(jsString(f['file_name'] || ''))}, event)">${escapeHtml(f['file_name'])}</b>`;

            // The row above already names the container, so all that is left to
            // say is where inside it the file sat. Only lineage rows carry that.
            const contextHtml = depth > 0 ? Lineage.pathLabel(f, 38) : '';

            // Depth 0 anchors the lineage tree: nested rows carry a higher
            // depth, which is how collapsing knows where to stop. data-lineage-
            // parent is what stops an expand from re-adding a child already
            // drawn under this same container.
            return `
            <tr class="sim-row${depth > 0 ? ' lineage-row' : ''}" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${escapeAttr(fileId)}"
                data-lineage-depth="${depth}" data-lineage-md5="${escapeAttr(f['file_md5'])}"
                data-lineage-parent="${escapeAttr(parentMd5 || f['parent_md5'] || '')}"
                ${injected ? 'data-lineage-injected="1"' : ''}
                data-lineage-col="${escapeAttr(targetCol)}" data-lineage-open="0"
                data-entity-data='${escapeAttr(JSON.stringify({
                    md5: f['file_md5'],
                    file_name: f['file_name'],
                    id: fileId,
                    collection: col
                }))}'
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'file', this)">
                <td class="sim-cell">
                    <div class="lineage-cell" style="padding-left:${depth * 18}px;">
                        ${depth > 0 ? '<span class="lineage-guide">└</span>' : ''}
                        ${Lineage.toggleButton(f['child_count'])}
                        ${f['is_container'] ? '<i class="fa-solid fa-box-archive dim" title="Container: holds code but is not code itself" style="font-size:0.7rem;"></i>' : ''}
                        ${nameHtml}
                    </div>
                    ${contextHtml}
                </td>
                <td class="sim-cell">
                    ${EntityRenderer.renderMd5(f['file_md5'], { full: true })}
                    <div class="dim" style="font-size:0.65rem">${escapeHtml(f['language_id'])}</div>
                </td>
                <td class="sim-cell">
                    <div style="display:flex; flex-direction:column; gap:2px; font-size:0.65rem;">
                        ${(() => {
                            const fields = [
                                { key: 'yara', label: 'Yara', dist: 'yara_distribution' },
                                { key: 'avtype', label: 'AV', dist: 'avtype_distribution' },
                                { key: 'filetype', label: 'Type', dist: 'filetype_distribution' },
                                { key: 'cc_ip', label: 'IP', dist: 'ccip_distribution' }
                            ];
                            
                            return fields.map(field => {
                                const val = f[field.key];
                                if (val && val.length) {
                                    return `<div class="dim">${field.label}: <span style="color:var(--accent)">${escapeHtml(Array.isArray(val) ? val.join(', ') : val)}</span></div>`;
                                }
                                
                                // Try inference
                                let bestInf = null;
                                clusters.forEach(c => {
                                    const dist = c[field.dist] || [];
                                    if (dist.length > 0) {
                                        const cohesion = c.cohesion_score || 0;
                                        if (!bestInf || cohesion > bestInf.cohesion) {
                                            bestInf = { value: dist[0].value, cohesion: cohesion };
                                        }
                                    }
                                });
                                
                                if (bestInf) {
                                    const hue = Math.max(0, Math.min(120, bestInf.cohesion * 120));
                                    const color = `hsl(${hue}, var(--color-s-med), var(--color-l-med))`;
                                    return `<div class="dim" title="Inferred from cluster (cohesion: ${(bestInf.cohesion*100).toFixed(1)}%)">
                                        ${field.label}: <span style="color:${color}; opacity: 0.9; font-style: italic;">${escapeHtml(bestInf.value)} <small>(${(bestInf.cohesion*100).toFixed(0)}%)</small></span>
                                    </div>`;
                                }
                                return '';
                            }).join('');
                        })()}
                        ${f['first_seen'] && f['first_seen'].length ? `<div class="dim">Seen: <span style="color:var(--accent)">${escapeHtml(Array.isArray(f['first_seen']) ? f['first_seen'].join(', ') : f['first_seen'])}</span></div>` : ''}
                    </div>
                </td>
                <td class="sim-cell mono dim" style="font-size:0.7rem" title="${escapeAttr(batchUuid)}">
                    ${escapeHtml(batchUuid.length > 8 ? batchUuid.substring(0, 8) + '...' : batchUuid)}
                </td>
                <td class="sim-cell">${fileStatusBadgeHtml(f)}</td>
                <td class="sim-cell">
                    <div style="display:inline-flex; align-items:center; justify-content:center; gap:8px; width:100%;">
                        <span style="font-weight: bold; min-width: 20px; text-align: right;">${funcCount}</span>
                    </div>
                </td>
                <td class="sim-cell file-note-cell" style="text-align:center;">
                    ${EntityRenderer.renderFileNoteButton(fileId, f.note_owners, { isTable: true, raw_data: f })}
                </td>
                <td class="cluster-cards-cell" data-is-binary="true" data-clusters='${escapeAttr(JSON.stringify(clusters))}'>
                    ${EntityRenderer.renderClusterCard(clusters, true)}
                </td>
                <td class="sim-cell dim">${formatDate(f['entry_date'])}</td>
                <td>
                    ${EntityRenderer.renderTag('file', fileId, tags, user_tags, { maxTags: 4 })}
                </td>
                ${renderCollectionCell(col)}
            </tr>
        `;
        }).join('');
    },

    renderFunctions: function(data, clustersMap = {}) {
        const { collection } = getRoutingState();
        return data.map(f => {
            const entry = f['entrypoint_address'] || '';
            const tags = f['tags'] || [];
            const user_tags = f['user_tags'] || [];
            const fileName = f['file_name'] || '';
            const file_md5 = f['file_md5'] || '';
            const language = f['language_id'] || '---';
            const featCount = f['bsim_features_count'] || 0;
            const fColl = f.collection || (f['function_id'] || '').split(':')[0] || collection;
            const funcId = f['function_id'] || `${fColl}:func:${file_md5}:${entry}`;
            let targetCol = fColl;
            const rowStyle = getRowTagColor(tags, user_tags);
            const clusters = (f['clusters'] || []).map(uuid => clustersMap[uuid]).filter(Boolean);

            return `
            <tr class="sim-row" style="background: ${rowStyle}; font-size: 0.75rem;" data-id="${escapeAttr(funcId)}"
                data-entity-data='${escapeAttr(JSON.stringify(f))}'
                draggable="true" ondragstart="onTableRowDragStart(event)"
                oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)">
                <td class="sim-cell" style="min-width: 300px;">
                    ${EntityRenderer.renderFunction(f, { hideNote: true })}
                </td>
                <td class="sim-cell"><span class="mono" style="color:var(--accent);">@ ${escapeHtml(entry)}</span></td>
                <td>${EntityRenderer.renderTag('function', funcId, tags, user_tags, { maxTags: 4 })}</td>
                <td class="cluster-cards-cell" data-clusters='${escapeAttr(JSON.stringify(clusters))}'>${EntityRenderer.renderClusterCard(clusters)}</td>
                <td class="sim-cell" style="text-align:center;">
                    <div style="display:inline-flex; align-items:center; gap:6px;">
                        <span class="mono" style="color:var(--accent); font-weight:bold;">${featCount}</span>
                        <button class="btn-icon" onclick="showFeaturePanel(${escapeAttr(jsString(funcId))}, event)" title="Show Features" style="background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font-size: 0.8rem; opacity: 0.7;">🔍</button>
                    </div>
                </td>
                <td class="sim-cell" style="text-align:center;">
                    ${EntityRenderer.renderNoteButton(funcId, f.note_owners, { isTable: true, raw_data: f })}
                </td>

                <td class="sim-cell"><div style="max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; opacity:0.8;" title="${escapeAttr(fileName)}"><b style="color:var(--accent); cursor:pointer;" onclick="openFileDetails(${escapeAttr(jsString(targetCol))}, ${escapeAttr(jsString(file_md5))}, ${escapeAttr(jsString(fileName))}, event)">${escapeHtml(fileName)}</b></div></td>

                <td class="sim-cell">${EntityRenderer.renderMd5(file_md5)}</td>
                <td>${EntityRenderer.renderTag('file', `${fColl}:file:${file_md5}`, f.file_tags || [], f.file_user_tags || [], { maxTags: 1 })}</td>
                <td class="sim-cell"><span class="mono" style="color:var(--accent)">${escapeHtml(language)}</span></td>
                <td class="sim-cell"><span class="dim" style="font-size:0.7rem;">${formatDate(f['entry_date'] || f['file_date'])}</span></td>
                <td class="sim-cell"></td>
                ${renderCollectionCell(fColl)}
            </tr>
        `}).join('');
    },

    renderGlobalFeatures: function(items) {
        const { collection } = getRoutingState();
        const col = collection;
        if (!items.length) return '<tr><td colspan="7" style="text-align:center">No features found.</td></tr>';

        return items.map(f => {
            const ctx = f.context || {};

            let pcodeHtml = `
                <div class="code-card">
                    <div class="code-card-line">
                        <div class="code-card-text pcode-text">${escapeHtml(ctx.pcode_full || 'N/A')}</div>
                    </div>
                </div>`;

            let cCodeHtml = '<span class="dim">N/A</span>';
            if (ctx.c_code) {
                const funcId = ctx.func_id || `${col}:func:${ctx.md5}:${ctx.addr}`;
                const funcName = (ctx.name || ctx.addr);
                const targetLinesStr = (ctx.line_idxs || []).map(l => l + 1).join(',');
                const lineHash = targetLinesStr ? `#L${targetLinesStr}` : '';

                const displayLine = (ctx.line_idxs && ctx.line_idxs.length > 0) ? ctx.line_idxs[0] + 1 : 1;
                
                const fData = {
                    function_id: funcId,
                    function_name: funcName,
                    file_md5: ctx.md5,
                    entrypoint_address: ctx.addr,
                    collection: col
                };

                cCodeHtml = `<div class="code-card clickable" title="Click to jump to lines ${escapeAttr(targetLinesStr || '')}"
                        data-entity-data='${escapeAttr(JSON.stringify(fData))}'
                        oncontextmenu="typeof EntityRenderer !== 'undefined' && EntityRenderer.handleContextMenu(event, 'function', this)"
                        onclick="showFunctionCodeById(${escapeAttr(jsString(funcId))}, ${escapeAttr(jsString(funcName))}, ${escapeAttr(jsString(lineHash))}, event)">
                    <div class="code-card-line">
                        <div class="code-card-ln">${displayLine}</div>
                        <div class="code-card-text">`;
                ctx.c_code.forEach(t => {
                    const colorMap = {
                        'variable': 'tok-variable', 'func_call': 'tok-func_call', 'type': 'tok-type',
                        'keyword': 'tok-keyword', 'comment': 'tok-comment', 'string': 'tok-string', 'number': 'tok-number'
                    };
                    const cls = colorMap[t.type] || 'tok-default';
                    cCodeHtml += `<span class="${cls} feature-highlight" 
                        data-hashes="${escapeAttr(f.hash)}" 
                        data-type="${escapeAttr(ctx.type || '')}" 
                        data-op="${escapeAttr(ctx.op || '')}" 
                        data-tf="${Math.round(f.tf_score || 0)}"
                        onmouseenter="showTokenTooltip(event)"
                        onmouseleave="hideTokenTooltip()"
                        onmousemove="moveCodePreview(event)">${escapeHtml(t.text)}</span>`;
                });
                cCodeHtml += `</div></div></div>`;
            }

            return `
            <tr data-id="${escapeAttr(f.hash)}">
                <td>
                    <div style="display:inline-flex; align-items:center; gap:8px;">
                        <code class="mono" style="color:var(--accent)">${escapeHtml(f.hash)}</code>
                        <button class="btn-copy" title="Copy Feature ID: ${escapeAttr(f['feature_id'])}" onclick="copyToClipboard(${escapeAttr(jsString(f['feature_id']))}, this)">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                        </button>
                    </div>
                </td>
                <td>
                    <div class="dim" style="font-size:0.65rem; font-weight:bold; color:var(--accent);">${escapeHtml(ctx.type)}</div>
                    <span class="badge" style="margin-top:2px; font-size:0.65rem;">${escapeHtml(ctx.op)}</span>
                </td>
                <td style="max-width:300px;">${pcodeHtml}</td>
                <td style="max-width:350px;">${cCodeHtml}</td>
                <td class="mono" style="color:var(--accent)">${f.tf_score !== undefined ? Math.round(f.tf_score) : '<span class="dim">-</span>'}</td>
                <td class="mono">${escapeHtml(f.frequency)}</td>
                <td>
                    <button class="btn-action" style="background:none; border:none; padding:0; font-size:0.8rem; text-align:left; color:var(--accent);"
                        onclick="showGlobalFeaturePanel(${escapeAttr(jsString(f.hash))}, ${escapeAttr(jsString(col))}, event)">Analyze</button>
                </td>

            </tr>
        `}).join('');
    }
};

// Map original function names to the new TableRenderers object for backward compatibility
// or just export them to window directly if desired.
window.renderCollections = TableRenderers.renderCollections;
window.renderPools = TableRenderers.renderPools;
window.renderBatches = TableRenderers.renderBatches;
window.renderFiles = TableRenderers.renderFiles;
window.renderFunctions = TableRenderers.renderFunctions;
window.renderGlobalFeatures = TableRenderers.renderGlobalFeatures;

window.onTableRowDragStart = function(e) {
    let id = null;
    const tr = e.target.closest('tr');
    if (tr) id = tr.dataset.id || tr.getAttribute('data-id');
    if (!id) {
        const ef = e.target.closest('.entity-function');
        if (ef) {
            const dataStr = ef.getAttribute('data-entity-data');
            if (dataStr) {
                try {
                    const data = JSON.parse(dataStr);
                    id = data.id || data.function_id;
                } catch(err){}
            }
        }
    }
    if (!id) return;

    let selectedIds = [];
    if (typeof window.getSelectedTableIds === 'function') {
        selectedIds = window.getSelectedTableIds('function');
    }
    if (!selectedIds.includes(id)) {
        selectedIds = [id];
    }
    e.dataTransfer.setData('text/plain', JSON.stringify(selectedIds));
    e.dataTransfer.setData('application/bsimvis-nodes', JSON.stringify(selectedIds));
    e.dataTransfer.effectAllowed = 'copy';
};
