/**
 * Compare draft: file searches sent from the Files page into the N-way compare.
 *
 * "Set" makes the current Files filters the whole of group A or B; "Add" appends them as another
 * search of that group (the group is the union of its searches). The draft is the panel's own
 * `cmp` value ({groups: [{label, queries: ["k=v&..."]}], within}) plus the scope, kept in
 * sessionStorage like nway_basket.js; every access is wrapped so a blocked storage only loses it.
 */
(function () {
    const KEY = 'bsim_nway_compare';
    // The /api/file/search parameters a Files filter can set; paging, sort and view state are left out.
    const SEARCH_PARAMS = new Set([
        'q', 'language_id', 'language', 'status', 'batch_uuid', 'bin_cluster_uuid', 'bin_cluster_name', 'first_seen', 'last_seen',
        'filetype', 'avtype', 'yara', 'cc_ip', 'file_names', 'inferred_yara', 'inferred_avtype', 'inferred_ccip', 'inferred_filename',
        'inferred_md5', 'inferred_tags', 'note_owner', 'note_owners', 'root_md5', 'strings', 'md5', 'file_md5', 'file_name',
        'min_function_count', 'max_function_count', 'min_bsim_features', 'max_bsim_features', 'min_cohesion', 'max_cohesion',
        'min_entry_date', 'max_entry_date', 'min_first_seen', 'max_first_seen', 'min_last_seen', 'max_last_seen',
        'tag', 'file_tag', 'static_tag', 'file_static_tag', 'user_tag', 'file_user_tag',
        'exclude_tag', 'exclude_file_tag', 'exclude_static_tag', 'exclude_file_static_tag', 'exclude_user_tag', 'exclude_file_user_tag',
    ]);
    let fallback = null;

    function read() {
        try {
            return JSON.parse(sessionStorage.getItem(KEY)) || fallback;
        } catch (e) {
            return fallback;
        }
    }

    function write(draft) {
        fallback = draft;
        try {
            if (draft) sessionStorage.setItem(KEY, JSON.stringify(draft)); else sessionStorage.removeItem(KEY);
        } catch (e) { /* storage blocked */ }
    }

    // The Files page's current filters as one query string.
    function currentQuery(params) {
        const out = new URLSearchParams();
        for (const [k, v] of params) if (SEARCH_PARAMS.has(k) && v !== '') out.append(k, v);
        return out.toString();
    }

    function summary(g) {
        const n = (g && g.queries.length) || 0;
        return n ? `${n} ${n === 1 ? 'search' : 'searches'}` : 'not set';
    }

    function render() {
        // Only the top window owns the chip; iframes ask it to redraw.
        try {
            if (window.top !== window && window.top.NwayCompareDraft) return window.top.NwayCompareDraft.render();
        } catch (e) { /* cross-origin parent */ }
        const d = read(), el = document.getElementById('nway-compare-status');
        if (!el) return;
        const ready = d && d.groups.every(g => g.queries.length);
        el.innerHTML = d
            ? `<span class="table-footer-badge nway-basket" title="${escapeAttr(d.groups.map(g => `${g.label}: ${g.queries.join('  |  ')}`).join('\n'))}">
                <i class="fa-solid fa-code-compare"></i> Compare: A ${escapeHtml(summary(d.groups[0]))} &middot; B ${escapeHtml(summary(d.groups[1]))}
                ${ready ? '<button class="nway-basket-open" onclick="NwayCompareDraft.open(event)">Open Compare &#8599;</button>' : ''}
                <button onclick="NwayCompareDraft.clear()" title="Clear the compare draft">&times;</button>
            </span>`
            : '';
    }

    window.NwayCompareDraft = {
        // Files filter bar: send the current filters to group A or B.
        buttons() {
            const b = (slot, mode, text, tip) => `<button type="button" class="nway-chip" onclick="NwayCompareDraft.send('${slot}', '${mode}')" title="${escapeAttr(tip)}">${text}</button>`;
            return `<span class="nway-cmp-send"><i class="fa-solid fa-code-compare dim" title="Compare the files these filters find in the N-way diff"></i>`
                + b('A', 'set', 'Set as A', 'Group A becomes these filters')
                + b('A', 'add', 'Add to A', 'Group A also takes the files these filters find')
                + b('B', 'set', 'Set as B', 'Group B becomes these filters')
                + b('B', 'add', 'Add to B', 'Group B also takes the files these filters find')
                + `</span>`;
        },
        send(slot, mode) {
            const { collection, pool, params } = getRoutingState();
            const query = currentQuery(params);
            if (!query) return showToast('Filter the files first: an empty search would take the whole collection.', 'warning');
            let d = read();
            if (!d || d.collection !== (collection || null) || d.pool !== (pool || null)) {
                d = { collection: collection || null, pool: pool || null, groups: [{ label: 'A', queries: [] }, { label: 'B', queries: [] }] };
            }
            const g = d.groups[slot === 'A' ? 0 : 1];
            g.queries = mode === 'add' ? [...new Set([...g.queries, query])] : [query];
            write(d);
            render();
            showToast(`${mode === 'add' ? 'Added to' : 'Set'} group ${slot}`, 'success');
        },
        open(event) {
            const d = read();
            if (!d) return;
            const qs = new URLSearchParams();
            if (d.pool) qs.set('pool', d.pool); else if (d.collection) qs.set('collection', d.collection);
            qs.set('cmp', JSON.stringify({ groups: d.groups, within: 0 }));
            Nav.openPath(`/diff/nway?${qs.toString()}`, event, { title: 'N-way diff', type: 'bin_sim' });
        },
        clear() {
            write(null);
            render();
        },
        render,
    };

    document.addEventListener('DOMContentLoaded', render);
})();
