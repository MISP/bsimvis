/**
 * lineage.js -- containment lineage UI (issue MISP/bsimvis#32 section 5).
 *
 * Two surfaces share the node rendering here: the indented tree in the file
 * list, and the breadcrumb + "Extracted from / Contains" panel in the file
 * view.
 *
 * Pagination and sorting: an expanded row's children are read from
 * /api/file/<md5>/lineage, never gathered from the rows already on the page.
 * The paginated result set is therefore untouched -- every file still appears
 * exactly once, where its sort order puts it, counted once in "shown / total"
 * -- and a container still lists all of its children even when they sorted
 * onto a different page, or onto no loaded page at all. Injected child rows
 * are a client-side overlay: they do not move the offset and they disappear on
 * collapse. A child that also matched the search on its own is shown twice, in
 * two different roles; the indent and the "in <path>" label say which is which.
 */

window.Lineage = {

    async fetch(collection, md5) {
        const res = await fetch(
            `/api/file/${encodeURIComponent(md5)}/lineage?collection=${encodeURIComponent(collection)}`
        );
        if (!res.ok) throw new Error(`Lineage lookup failed (${res.status})`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        return data;
    },

    /**
     * A node's name as a link, or as a visibly unresolved stand-in when the
     * container was declared by an edge but its bytes were never uploaded.
     */
    nodeName(node, collection, { max = 40 } = {}) {
        const name = node.file_name || node.file_md5;
        const label = escapeHtml(middleTruncate(name, max));
        if (!node.exists) {
            return `<span class="lineage-missing" title="${escapeAttr(name)} — declared as a container but never uploaded">
                <i class="fa-solid fa-link-slash"></i> ${label}</span>`;
        }
        return `<b class="lineage-link" title="${escapeAttr(name)}"
            onclick="event.stopPropagation(); openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(node.file_md5))}, ${escapeAttr(jsString(name))}, event)">${label}</b>`;
    },

    /** "in lib/arm64-v8a/libfoo.so", middle-truncated, full value on hover. */
    pathLabel(node, max = 46) {
        if (!node.path_in_parent) return '';
        return `<span class="lineage-path" title="${escapeAttr(node.path_in_parent)}">in ${escapeHtml(middleTruncate(node.path_in_parent, max))}</span>`;
    },

    // -----------------------------------------------------------------
    // File list: indented tree rows
    // -----------------------------------------------------------------

    /** Chevron for a row that has children. Nothing at all for a leaf. */
    toggleButton(childCount) {
        if (!childCount) return '<span class="lineage-toggle-spacer"></span>';
        return `<button class="lineage-toggle" title="${childCount} contained file${childCount === 1 ? '' : 's'}"
            onclick="Lineage.toggleRow(event, this)"><i class="fa-solid fa-chevron-right"></i></button>`;
    },

    async toggleRow(event, btn) {
        event.stopPropagation();
        event.preventDefault();
        const row = btn.closest('tr');
        if (!row || row.dataset.lineageBusy === '1') return;

        if (row.dataset.lineageOpen === '1') {
            this._collapse(row);
            return;
        }

        const icon = btn.querySelector('i');
        row.dataset.lineageBusy = '1';
        if (icon) icon.className = 'fa-solid fa-spinner fa-spin';
        try {
            const data = await this.fetch(row.dataset.lineageCol, row.dataset.lineageMd5);
            const depth = Number(row.dataset.lineageDepth || 0) + 1;
            const html = (data.children || [])
                .map(node => this._childRow(node, row.dataset.lineageCol, depth))
                .join('') || this._noteRow(depth, 'No contained files.');
            row.insertAdjacentHTML('afterend', html);
            row.dataset.lineageOpen = '1';
            if (icon) icon.className = 'fa-solid fa-chevron-down';
        } catch (e) {
            console.error(e);
            row.insertAdjacentHTML('afterend',
                this._noteRow(Number(row.dataset.lineageDepth || 0) + 1, `Could not load lineage: ${escapeHtml(e.message)}`));
            row.dataset.lineageOpen = '1';
            if (icon) icon.className = 'fa-solid fa-chevron-down';
        } finally {
            row.dataset.lineageBusy = '0';
        }
    },

    /** Drops the injected subtree: every following row deeper than this one. */
    _collapse(row) {
        const depth = Number(row.dataset.lineageDepth || 0);
        let next = row.nextElementSibling;
        while (next && Number(next.dataset.lineageDepth || 0) > depth) {
            const after = next.nextElementSibling;
            next.remove();
            next = after;
        }
        row.dataset.lineageOpen = '0';
        const icon = row.querySelector('.lineage-toggle i');
        if (icon) icon.className = 'fa-solid fa-chevron-right';
    },

    /** Same column count as renderFiles(), so the table stays aligned. */
    _childRow(node, collection, depth) {
        const fileId = `${collection}:file:${node.file_md5}`;
        const md5Cell = node.exists
            ? EntityRenderer.renderMd5(node.file_md5, { full: true })
            : `<span class="mono dim" style="font-size:0.7rem;">${escapeHtml(node.file_md5)}</span>`;
        const tagsCell = node.exists
            ? EntityRenderer.renderTag('file', fileId, node.tags || [], [])
            : '';

        return `
        <tr class="sim-row lineage-row" style="font-size:0.75rem;"
            data-lineage-depth="${depth}" data-lineage-md5="${escapeAttr(node.file_md5)}"
            data-lineage-col="${escapeAttr(collection)}" data-lineage-open="0">
            <td class="sim-cell">
                <div class="lineage-cell" style="padding-left:${depth * 18}px;">
                    <span class="lineage-guide">└</span>
                    ${this.toggleButton(node.child_count)}
                    ${this.nodeName(node, collection, { max: 34 })}
                    ${this.pathLabel(node, 38)}
                </div>
            </td>
            <td class="sim-cell">${md5Cell}</td>
            <td class="sim-cell dim" style="font-size:0.65rem;">${escapeHtml(node.filetype || '')}</td>
            <td class="sim-cell"></td>
            <td class="sim-cell" style="text-align:center;">
                <span style="font-weight:bold;">${Number(node.function_count) || 0}</span>
            </td>
            <td class="sim-cell"></td>
            <td class="sim-cell"></td>
            <td class="sim-cell"></td>
            <td>${tagsCell}</td>
            ${renderCollectionCell(collection)}
        </tr>`;
    },

    _noteRow(depth, text) {
        return `<tr class="lineage-row" data-lineage-depth="${depth}">
            <td class="sim-cell dim" colspan="100" style="padding-left:${depth * 18 + 24}px; font-size:0.7rem;">${text}</td>
        </tr>`;
    },

    // -----------------------------------------------------------------
    // File view: breadcrumb + panel
    // -----------------------------------------------------------------

    /**
     * app.apk › lib/arm64-v8a/libfoo.so. Built from the full ancestor chain
     * reversed, so a file two levels down shows both hops, not just its parent.
     */
    renderBreadcrumb(lin, collection) {
        const chain = (lin.ancestors || []).slice().reverse();
        if (!chain.length) return '';
        const self = lin.file || {};
        const crumbs = chain.map(node =>
            `<span class="lineage-crumb">${this.nodeName(node, collection, { max: 32 })}</span>`
        );
        crumbs.push(`<span class="lineage-crumb lineage-crumb-current" title="${escapeAttr(self.file_name || '')}">${escapeHtml(middleTruncate(self.file_name || self.file_md5, 32))}</span>`);
        return `<div class="lineage-breadcrumb"><i class="fa-solid fa-box-open"></i>${crumbs.join('<span class="lineage-sep">›</span>')}</div>`;
    },

    renderTree(nodes, collection, itemFn) {
        if (!nodes || !nodes.length) return '';
        
        const root = { files: [], dirs: {} };
        for (const node of nodes) {
            const pathStr = node.path_in_parent || '';
            const pathParts = pathStr.split('/').filter(p => p);
            if (pathParts.length > 0) {
                pathParts.pop();
            }
            
            let current = root;
            for (const part of pathParts) {
                if (!current.dirs[part]) current.dirs[part] = { files: [], dirs: {} };
                current = current.dirs[part];
            }
            current.files.push(node);
        }

        const renderDir = (dir, name, depth) => {
            let html = [];
            
            if (name) {
                html.push(`
                <tr class="bsim-grp-row" data-depth="${depth}" data-rowkey="dir-${escapeAttr(name)}" onclick="Lineage.toggleTreeRow(this)">
                    <td colspan="2" style="padding-left: ${12 + depth * 18}px; border-bottom: 1px solid var(--border);">
                        <span class="bsim-caret-btn" style="display:inline-block; width:14px; color:var(--subtle);">▼</span>
                        <i class="fa-regular fa-folder" style="color:var(--accent);"></i> <span style="font-weight:600; margin-left:4px;">${escapeHtml(name)}</span>
                    </td>
                </tr>`);
            }
            
            const dirNames = Object.keys(dir.dirs).sort((a, b) => a.localeCompare(b));
            for (const d of dirNames) {
                html = html.concat(renderDir(dir.dirs[d], d, depth + 1));
            }
            
            dir.files.sort((a, b) => (a.file_name || a.file_md5).localeCompare(b.file_name || b.file_md5));
            for (const f of dir.files) {
                html.push(itemFn(f, true, depth + 1));
            }
            
            return html;
        };
        
        return renderDir(root, '', -1).join('');
    },

    toggleTreeRow(tr) {
        const depth = parseInt(tr.dataset.depth || 0);
        const caret = tr.querySelector('.bsim-caret-btn');
        if (!caret) return;
        const isClosing = caret.textContent === '▼';
        caret.textContent = isClosing ? '▶' : '▼';
        tr.dataset.closed = isClosing ? '1' : '0';

        let next = tr.nextElementSibling;
        while (next) {
            const nextDepth = parseInt(next.dataset.depth || 0);
            if (nextDepth <= depth) break;
            
            if (isClosing) {
                next.style.display = 'none';
            } else {
                let visible = true;
                let p = next.previousElementSibling;
                let currentDepth = nextDepth;
                while (p && parseInt(p.dataset.depth || 0) >= 0) {
                    const pDepth = parseInt(p.dataset.depth || 0);
                    if (pDepth < currentDepth) {
                        if (p.dataset.closed === '1') {
                            visible = false;
                            break;
                        }
                        currentDepth = pDepth;
                        if (currentDepth <= depth) break;
                    }
                    p = p.previousElementSibling;
                }
                next.style.display = visible ? '' : 'none';
            }
            next = next.nextElementSibling;
        }
    },

    renderParents(lin, collection, siblingsByParent = {}) {
        const parents = lin.parents || [];
        if (!parents.length) return '';

        const item = (node, extra = '', hidePath = false, depth = -1) => {
            const fileId = `${collection}:file:${node.file_md5}`;
            return `
            <tr class="sim-row" data-id="${escapeAttr(fileId)}" data-depth="${depth}" onclick="openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(node.file_md5))}, ${escapeAttr(jsString(node.file_name || ''))}, event)">
                <td style="padding-left: ${12 + Math.max(0, depth) * 18}px; border-bottom: 1px solid var(--border); padding-top:6px; padding-bottom:6px; font-size:0.85rem;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        ${this.nodeName(node, collection, { max: 48 })}
                        ${hidePath ? '' : this.pathLabel(node)}
                    </div>
                </td>
                <td style="border-bottom: 1px solid var(--border); text-align: right; padding-right:12px; font-size:0.85rem;">
                    ${extra}
                </td>
            </tr>`;
        };
        const counts = node => {
            const bits = [];
            if (node.child_count) bits.push(`${node.child_count} contained`);
            if (node.function_count) bits.push(`${node.function_count} func`);
            return bits.length ? `<span class="lineage-count">${bits.join(' · ')}</span>` : '';
        };

        const itemWithCounts = (node, hidePath = false, depth = -1) => item(node, counts(node), hidePath, depth);

        let html = `<div class="card lineage-panel" style="padding:0; overflow:hidden;"><table class="data-table" id="lineage-tree-parents-table" style="width: 100%;"><tbody>`;
        html += `<tr class="bsim-grp-row" data-depth="-1"><td colspan="2"><div class="lineage-section-title" style="padding: 10px 15px; margin: 0; border: none; background: transparent;"><i class="fa-solid fa-box-open"></i> Extracted from</div></td></tr>`;
        html += parents.map(p => item(p, counts(p))).join('');

        const sibsHtml = parents.map((p, i) => {
            const sibs = (siblingsByParent[p.file_md5] || [])
                .filter(s => s.file_md5 !== lin.file.file_md5);
            if (!sibs.length) return '';
            const heading = parents.length > 1
                ? ` <span class="lineage-section-sub">in ${escapeHtml(middleTruncate(p.file_name || p.file_md5, 40))}</span>`
                : '';
            return `
                <tr style="height: 20px; background: var(--bg);"><td colspan="2" style="border: none; padding: 0;"></td></tr>
                <tr class="bsim-grp-row" data-depth="-1"><td colspan="2"><div class="lineage-section-title" style="padding: 10px 15px; margin: 0; border: none; background: transparent;"><i class="fa-solid fa-folder-tree"></i> Alongside${heading}</div></td></tr>
                ${this.renderTree(sibs, collection, itemWithCounts)}
            `;
        }).join('');

        html += sibsHtml + `</tbody></table></div>`;
        return html;
    },

    renderChildren(lin, collection) {
        const children = lin.children || [];
        if (!children.length) return '';

        const item = (node, extra = '', hidePath = false, depth = -1) => {
            const fileId = `${collection}:file:${node.file_md5}`;
            return `
            <tr class="sim-row" data-id="${escapeAttr(fileId)}" data-depth="${depth}" onclick="openFileDetails(${escapeAttr(jsString(collection))}, ${escapeAttr(jsString(node.file_md5))}, ${escapeAttr(jsString(node.file_name || ''))}, event)">
                <td style="padding-left: ${12 + Math.max(0, depth) * 18}px; border-bottom: 1px solid var(--border); padding-top:6px; padding-bottom:6px; font-size:0.85rem;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        ${this.nodeName(node, collection, { max: 48 })}
                        ${hidePath ? '' : this.pathLabel(node)}
                    </div>
                </td>
                <td style="border-bottom: 1px solid var(--border); text-align: right; padding-right:12px; font-size:0.85rem;">
                    ${extra}
                </td>
            </tr>`;
        };
        const counts = node => {
            const bits = [];
            if (node.child_count) bits.push(`${node.child_count} contained`);
            if (node.function_count) bits.push(`${node.function_count} func`);
            return bits.length ? `<span class="lineage-count">${bits.join(' · ')}</span>` : '';
        };

        let html = `<div class="card lineage-panel" style="padding:0; overflow:hidden;"><table class="data-table" id="lineage-tree-children-table" style="width: 100%;"><tbody>`;
        const deeper = (lin.descendant_count || 0) - children.length;
        html += `<tr class="bsim-grp-row" data-depth="-1"><td colspan="2"><div class="lineage-section-title" style="padding: 10px 15px; margin: 0; border: none; background: transparent;"><i class="fa-solid fa-diagram-project"></i> Contains
            <span class="lineage-count">${lin.child_count} direct${deeper > 0 ? ` · ${deeper} deeper` : ''}</span></div></td></tr>`;
        
        const itemWithCounts = (node, hidePath = false, depth = -1) => item(node, counts(node), hidePath, depth);
        html += this.renderTree(children, collection, itemWithCounts);
        html += `</tbody></table></div>`;

        return html;
    }
};
