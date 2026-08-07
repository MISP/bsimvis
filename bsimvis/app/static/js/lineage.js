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

    /**
     * "Extracted from / Contains". `siblingsByParent` maps a parent md5 to that
     * parent's children, so a file held by two containers lists the neighbours
     * of each separately instead of merging two unrelated archives.
     */
    renderPanel(lin, collection, siblingsByParent = {}) {
        const parents = lin.parents || [];
        const children = lin.children || [];
        if (!parents.length && !children.length) return '';

        const item = (node, extra = '') => `
            <div class="lineage-item">
                ${this.nodeName(node, collection, { max: 48 })}
                ${this.pathLabel(node)}
                ${extra}
            </div>`;
        const counts = node => {
            const bits = [];
            if (node.child_count) bits.push(`${node.child_count} contained`);
            if (node.function_count) bits.push(`${node.function_count} func`);
            return bits.length ? `<span class="lineage-count">${bits.join(' · ')}</span>` : '';
        };

        let html = '';

        if (parents.length) {
            html += `<div class="lineage-section-title"><i class="fa-solid fa-box-open"></i> Extracted from</div>`;
            html += parents.map(p => item(p, counts(p))).join('');

            const sibSections = parents.map(p => {
                const sibs = (siblingsByParent[p.file_md5] || [])
                    .filter(s => s.file_md5 !== lin.file.file_md5);
                if (!sibs.length) return '';
                const heading = parents.length > 1
                    ? `<div class="lineage-section-sub">alongside, in ${escapeHtml(middleTruncate(p.file_name || p.file_md5, 40))}</div>`
                    : `<div class="lineage-section-sub">alongside</div>`;
                return heading + sibs.map(s => item(s, counts(s))).join('');
            }).join('');
            html += sibSections;
        }

        if (children.length) {
            const deeper = (lin.descendant_count || 0) - children.length;
            html += `<div class="lineage-section-title"><i class="fa-solid fa-diagram-project"></i> Contains
                <span class="lineage-count">${lin.child_count} direct${deeper > 0 ? ` · ${deeper} deeper` : ''}</span></div>`;
            html += children.map(c => item(c, counts(c))).join('');
        }

        return `<div class="card lineage-panel">${html}</div>`;
    }
};
