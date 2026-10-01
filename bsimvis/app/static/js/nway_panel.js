/**
 * N-way diff panel: functions shared across N files (GET /api/bin_sim/nway).
 *
 *   const panel = new NwayPanel(container, {
 *       source: { md5s: 'coll:md5,coll:md5', pool: null },  // any API member params
 *       state: { tab: 'core' },                              // initial controls
 *       onState: s => ...,                                   // fired on every control change
 *   });
 *   panel.load(); ... panel.destroy();
 *
 * `source` is passed to the API untouched, so a cluster entry only supplies
 * `{ cluster_uuid, collection | pool }`; `state.columns` (auto | files | children)
 * and `state.child_presence` are the cluster-only controls. `onOpenCluster(uuid)`
 * is called when a child cluster link in the too-many-members state is clicked.
 * Rows are paged
 * server-side; nothing here loads a full table. Function and file names come off
 * uploaded samples, so every value goes through escapeHtml / escapeAttr.
 */
(function () {
    const TABS = [['core', 'Core'], ['partial', 'Partial'], ['unique', 'Unique']];
    const PAGE = 50;
    const SORTS = [['name', 'Function'], ['span', 'Span'], ['weight', 'Weight'], ['support', 'Support'], ['cohesion', 'Cohesion']];
    const DEFAULTS = {
        tab: 'core', scope: 'code', k: 2, min_edge: '', mode: 'stored',
        q: '', tags: '', sort_col: '', sort_dir: 'desc', offset: 0,
        columns: 'auto', child_presence: 0.5,
    };
    // State keys that go to the API as query params (empty ones are dropped).
    const API_KEYS = ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset', 'columns', 'child_presence'];
    const LOW_SUPPORT = 0.5;

    function fnParts(fid) {
        // `{coll}:func:{md5}:{addr}` -- the collection can contain colons.
        const i = fid.indexOf(':func:');
        const rest = i < 0 ? '' : fid.slice(i + 6);
        const j = rest.indexOf(':');
        return { md5: rest.slice(0, j), addr: rest.slice(j + 1) };
    }

    function fnUrl(col, fid, pool) {
        const { addr } = fnParts(fid);
        const base = `${pool ? `/pools/${encodeURIComponent(pool)}` : ''}/collections/${encodeURIComponent(col.collection)}`;
        return `${base}/files/${encodeURIComponent(col.md5)}/functions/${encodeURIComponent(addr)}`;
    }

    function diffUrl(colA, fidA, colB, fidB, pool) {
        return `${fnUrl(colA, fidA, pool)}/vs/${encodeURIComponent(colB.collection)}/${encodeURIComponent(colB.md5)}/${encodeURIComponent(fnParts(fidB).addr)}`;
    }

    function pct(v) {
        return `${(Number(v || 0) * 100).toFixed(0)}%`;
    }

    class NwayPanel {
        constructor(container, opts = {}) {
            this.el = container;
            this.source = opts.source || {};
            this.state = { ...DEFAULTS, ...(opts.state || {}) };
            this.onState = opts.onState || null;
            this.onOpenCluster = opts.onOpenCluster || null;
            this.data = null;
            this.error = null;
            this.picks = {};
            this._gen = 0;
            this._debounce = null;
            this.el.addEventListener('click', e => this._onClick(e));
            this.el.addEventListener('change', e => this._onChange(e));
            this.el.addEventListener('input', e => this._onInput(e));
            this.el.addEventListener('keydown', e => {
                if (e.key === 'Enter' && e.target.dataset.text) this._set({ [e.target.dataset.text]: e.target.value.trim(), offset: 0 });
            });
        }

        destroy() {
            this._gen++;
            clearTimeout(this._debounce);
        }

        _set(patch) {
            Object.assign(this.state, patch);
            if (this.onState) this.onState({ ...this.state });
            this.load();
        }

        async load() {
            const gen = ++this._gen;
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries(this.source)) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            for (const k of API_KEYS) {
                const v = this.state[k];
                if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            }
            qs.set('limit', PAGE);
            this.picks = {};
            if (!this.data) this.el.innerHTML = '<div class="dim" style="padding:20px;"><i class="fa-solid fa-spinner fa-spin"></i> Matching functions...</div>';
            try {
                const res = await fetch(`/api/bin_sim/nway?${qs.toString()}`);
                const body = await res.json().catch(() => ({}));
                if (gen !== this._gen) return;
                if (!res.ok) {
                    this.error = { status: res.status, ...body };
                    this.data = null;
                } else {
                    this.error = null;
                    this.data = body;
                }
            } catch (e) {
                if (gen !== this._gen) return;
                this.error = { status: 0, message: String(e) };
                this.data = null;
            }
            this.render();
        }

        _renderError() {
            const e = this.error;
            if (e.status === 413) {
                const kids = (e.children || []).map(c => {
                    const uuid = c.cluster_uuid || c.uuid || '';
                    const name = escapeHtml(c.cluster_name || c.name || uuid);
                    const link = uuid && this.onOpenCluster ? `<a href="#" data-open-cluster="${escapeAttr(uuid)}">${name}</a>` : name;
                    return `<li>${link}${c.member_count ? ` <span class="dim">${Number(c.member_count)} files</span>` : ''}</li>`;
                }).join('');
                return `<div class="dim" style="padding:20px;"><b>Too many members</b> (${Number(e.members) || '?'}): pick fewer files${kids ? ' or open a child cluster:' : '.'}${kids ? `<ul>${kids}</ul>` : ''}</div>`;
            }
            const msg = e.missing ? `Unknown files: ${e.missing.join(', ')}` : (e.message || e.error || `HTTP ${e.status}`);
            return `<div class="dim" style="padding:20px; color:var(--danger, #ef4444);">${escapeHtml(msg)}</div>`;
        }

        _count(tab) {
            const c = (this.data.counts || {})[tab] || { code: 0, library: 0 };
            return `${c.code} / ${c.library}`;
        }

        _controls() {
            const s = this.state, d = this.data;
            const n = d.columns.length;
            const tabs = TABS.map(([key, label]) =>
                `<button class="btn${s.tab === key ? ' active' : ''}" data-tab="${key}">${label} <span class="dim" title="code / library">${this._count(key)}</span></button>`
            ).join('');
            const opt = (v, cur, label) => `<option value="${escapeAttr(v)}"${v === cur ? ' selected' : ''}>${escapeHtml(label)}</option>`;
            const k = s.tab === 'partial'
                ? `<label>in at least <b>${Number(s.k)}</b> of ${n} <input type="range" min="2" max="${n}" value="${Number(s.k)}" data-range="k"></label>` : '';
            const edge = Number(d.min_edge);
            const isNode = !!this.source.cluster_uuid;
            const cols = isNode
                ? `<select data-select="columns" title="columns: the node's files, or one per child cluster">${opt('auto', s.columns, 'Columns: auto')}${opt('files', s.columns, 'Columns: files')}${opt('children', s.columns, 'Columns: children')}</select>` : '';
            const presence = isNode && d.columns_mode === 'children'
                ? `<label title="share of a child's files that must hold the function for the child to count as present">child presence <b>${Number(s.child_presence).toFixed(2)}</b> <input type="range" min="0.05" max="1" step="0.05" value="${Number(s.child_presence)}" data-range="child_presence"></label>` : '';
            return `<div style="display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:10px;">
                <div style="display:flex; gap:4px;">${tabs}</div>
                <select data-select="scope">${opt('code', s.scope, 'Code')}${opt('library', s.scope, 'Library')}${opt('all', s.scope, 'All')}</select>
                ${cols}${presence}
                ${k}
                <label>min edge <b>${edge.toFixed(2)}</b> <input type="range" min="0" max="1" step="0.05" value="${edge}" data-range="min_edge"></label>
                <select data-select="mode" title="stored reads built pair docs; virtual recomputes from vectors">${opt('stored', s.mode, 'Stored')}${opt('virtual', s.mode, 'Virtual')}</select>
                <input type="text" data-text="q" placeholder="function name / address" value="${escapeAttr(s.q)}" style="width:190px;">
                <input type="text" data-text="tags" placeholder="tags" value="${escapeAttr(s.tags)}" style="width:130px;">
            </div>`;
        }

        _notes() {
            const d = this.data;
            let html = '';
            if (d.mode !== this.state.mode) html += `<div class="dim">Ran in <b>${escapeHtml(d.mode)}</b> mode: files span several collections.</div>`;
            if (d.fallback_pairs) html += `<div class="dim">${Number(d.fallback_pairs)} pairs computed on the fly (no stored pair doc).</div>`;
            if ((d.warnings || []).length) {
                html += `<div style="border-left:3px solid #f59e0b; padding:6px 10px; margin-bottom:8px;">${d.warnings.map(w => `<div>${escapeHtml(w)}</div>`).join('')}</div>`;
            }
            return html;
        }

        // A child/direct column: "m/n files", plus links to the functions of the
        // files in that group (a few; the rest as a count).
        _groupCell(row, col) {
            const cell = row.cells[col.id];
            const style = cell.on ? '' : ' class="dim"';
            const pool = this.source.pool || null;
            const links = [];
            for (const f of this.data.file_columns || []) {
                const fid = col.members.includes(f.id) && (row.files || {})[f.id];
                if (fid) links.push(`<a href="${escapeAttr(fnUrl(f, fid, pool))}" data-nav="${escapeAttr(fnUrl(f, fid, pool))}" data-title="${escapeAttr(f.file_name)}" title="${escapeAttr(f.file_name)}"><code>${escapeHtml((this.data.functions_metadata[fid] || {}).entrypoint_address || fnParts(fid).addr)}</code></a>`);
            }
            const shown = links.slice(0, 3).join(' ');
            const more = links.length > 3 ? ` <span class="dim">+${links.length - 3}</span>` : '';
            return `<td${style}><b>${Number(cell.present)}/${Number(cell.total)}</b> files${shown ? `<div>${shown}${more}</div>` : ''}</td>`;
        }

        _cell(row, col, ri) {
            if (col.kind) return this._groupCell(row, col);
            const fid = row.cells[col.id];
            if (!fid) return '<td class="dim" style="text-align:center;">&mdash;</td>';
            const meta = this.data.functions_metadata[fid] || {};
            const label = meta.entrypoint_address || fnParts(fid).addr;
            const picked = (this.picks[ri] || []).includes(col.id);
            const pool = this.source.pool || null;
            return `<td><a href="${escapeAttr(fnUrl(col, fid, pool))}" data-nav="${escapeAttr(fnUrl(col, fid, pool))}" data-title="${escapeAttr(meta.name || label)}"><code>${escapeHtml(label)}</code></a>
                <button class="btn" data-pick="${ri}" data-col="${escapeAttr(col.id)}" title="pick for function diff" style="padding:0 5px;${picked ? ' background:var(--accent); color:#fff;' : ''}"><i class="fa-solid fa-code-compare"></i></button></td>`;
        }

        _row(row, ri) {
            const cols = this.data.columns;
            const extra = Array.isArray(row.names) ? row.names.length - 1 : Number(row.names || 0) - 1;
            const low = row.support < LOW_SUPPORT && (row.file_span || row.span) > 2
                ? ` <i class="fa-solid fa-triangle-exclamation" style="color:#f59e0b;" title="low support: only ${pct(row.support)} of the possible pairs in this row are matched; it may be a chain of transitive matches"></i>` : '';
            const picks = this.picks[ri] || [];
            const cmp = picks.length === 2 && !this.data.columns[0].kind ? `<button class="btn" data-compare="${ri}">Compare</button>` : '';
            return `<tr>
                <td>${escapeHtml(row.name)}${extra > 0 ? ` <span class="dim">+${extra} names</span>` : ''}${row.library ? ' <span class="dim">lib</span>' : ''}${low}</td>
                <td style="text-align:right;">${Number(row.span)}</td>
                <td style="text-align:right;">${Number(row.weight).toFixed(0)}</td>
                <td style="text-align:right;">${pct(row.support)}</td>
                <td style="text-align:right;">${pct(row.cohesion)}</td>
                ${cols.map(c => this._cell(row, c, ri)).join('')}
                <td>${cmp}</td>
            </tr>`;
        }

        _head() {
            const s = this.state;
            const th = ([key, label]) => {
                const arrow = s.sort_col === key ? (s.sort_dir === 'asc' ? ' &#9650;' : ' &#9660;') : '';
                return `<th data-sort="${key}" style="cursor:pointer; ${key === 'name' ? '' : 'text-align:right;'}">${label}${arrow}</th>`;
            };
            const colHead = c => c.kind
                ? `<th title="${escapeAttr(c.kind === 'child' ? 'child cluster' : 'files at this node, in no child cluster')}">${escapeHtml(c.label)}<div class="dim" style="font-weight:400;">${Number(c.member_count)} files</div></th>`
                : `<th title="${escapeAttr(`${c.file_name} (${c.collection})`)}">${escapeHtml(c.file_name)}<div class="dim" style="font-weight:400;">${Number(c.functions)} fn</div>${c.coverage == null ? '' : `<div title="share of the shared (span >= 2) weight this file holds" style="font-weight:400; color:var(--accent);">${(Number(c.coverage) * 100).toFixed(0)}% shared</div>`}</th>`;
            return `<tr>${SORTS.map(th).join('')}${this.data.columns.map(colHead).join('')}<th></th></tr>`;
        }

        render() {
            if (this.error) {
                this.el.innerHTML = this._renderError();
                return;
            }
            const d = this.data;
            const rows = d.items.map((r, i) => this._row(r, i)).join('');
            const off = Number(d.offset), total = Number(d.total);
            this.el.innerHTML = `${this._controls()}${this._notes()}
                <div style="overflow:auto;"><table class="data-table" style="width:100%;">
                    <thead>${this._head()}</thead>
                    <tbody>${rows || `<tr><td colspan="${d.columns.length + 6}" class="dim" style="padding:16px;">No rows in this tab.</td></tr>`}</tbody>
                </table></div>
                <div style="display:flex; gap:8px; align-items:center; margin-top:8px;">
                    <button class="btn" data-page="-1"${off <= 0 ? ' disabled' : ''}>Prev</button>
                    <span class="dim">${total ? off + 1 : 0}&ndash;${Math.min(off + PAGE, total)} of ${total}</span>
                    <button class="btn" data-page="1"${off + PAGE >= total ? ' disabled' : ''}>Next</button>
                </div>`;
        }

        _onClick(e) {
            const t = e.target.closest('[data-tab],[data-sort],[data-page],[data-pick],[data-compare],[data-nav],[data-open-cluster]');
            if (!t) return;
            if (t.dataset.openCluster) {
                e.preventDefault();
                return this.onOpenCluster(t.dataset.openCluster);
            }
            const pool = this.source.pool || null;
            if (t.dataset.tab) return this._set({ tab: t.dataset.tab, offset: 0, sort_col: '' });
            if (t.dataset.sort) {
                const same = this.state.sort_col === t.dataset.sort;
                return this._set({ sort_col: t.dataset.sort, sort_dir: same && this.state.sort_dir === 'desc' ? 'asc' : 'desc', offset: 0 });
            }
            if (t.dataset.page) return this._set({ offset: Math.max(0, Number(this.state.offset) + Number(t.dataset.page) * PAGE) });
            if (t.dataset.pick) {
                const ri = t.dataset.pick, id = t.dataset.col;
                const cur = this.picks[ri] || [];
                this.picks[ri] = cur.includes(id) ? cur.filter(x => x !== id) : [...cur, id].slice(-2);
                return this.render();
            }
            if (t.dataset.compare) {
                const row = this.data.items[Number(t.dataset.compare)];
                const [a, b] = this.picks[t.dataset.compare].map(id => this.data.columns.find(c => c.id === id));
                return window.Nav.openPath(diffUrl(a, row.cells[a.id], b, row.cells[b.id], pool), e, { title: `Diff: ${row.name}`, type: 'diff' });
            }
            if (t.dataset.nav && window.Nav) {
                e.preventDefault();
                window.Nav.openPath(t.dataset.nav, e, { title: t.dataset.title || 'Function', type: 'function' });
            }
        }

        _onChange(e) {
            const t = e.target;
            if (t.dataset.select) this._set({ [t.dataset.select]: t.value, offset: 0 });
            else if (t.dataset.range) this._set({ [t.dataset.range]: t.value, offset: 0 });
            else if (t.dataset.text) this._set({ [t.dataset.text]: t.value.trim(), offset: 0 });
        }

        // Sliders recompute on release (change), not on every drag step.
        _onInput(e) {
            const t = e.target;
            const label = t.dataset.range && t.closest('label') && t.closest('label').querySelector('b');
            if (label) label.textContent = t.dataset.range === 'k' ? t.value : Number(t.value).toFixed(2);
        }
    }

    // Selected file ids are `{coll}:file:{md5}`; the page's pool (if any) rides along.
    NwayPanel.urlFor = function (ids, fallbackCollection) {
        const md5s = ids.map(id => `${id.split(':')[0] || fallbackCollection}:${id.split(':').pop()}`).join(',');
        const pool = window.getRoutingState ? window.getRoutingState().pool : null;
        return `/diff/nway?md5s=${encodeURIComponent(md5s)}${pool ? `&pool=${encodeURIComponent(pool)}` : ''}`;
    };

    window.NwayPanel = NwayPanel;
})();
