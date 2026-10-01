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
            if (!this.data) this.el.innerHTML = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Matching functions...</div>';
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
                    const link = uuid && this.onOpenCluster ? `<a href="#" class="btn-action" data-open-cluster="${escapeAttr(uuid)}">${name}</a>` : name;
                    return `<li>${link}${c.member_count ? ` <span class="dim">${Number(c.member_count)} files</span>` : ''}</li>`;
                }).join('');
                return `<div class="nway-note warn"><b>Too many members</b> (${Number(e.members) || '?'}): pick fewer files${kids ? ' or open a child cluster:' : '.'}${kids ? `<ul>${kids}</ul>` : ''}</div>`;
            }
            const msg = e.missing ? `Unknown files: ${e.missing.join(', ')}` : (e.message || e.error || `HTTP ${e.status}`);
            return `<div class="nway-note err">${escapeHtml(msg)}</div>`;
        }

        _count(tab) {
            const c = (this.data.counts || {})[tab] || { code: 0, library: 0 };
            return `${c.code} / ${c.library}`;
        }

        // One segmented toggle (the shared .view-toggle); `data-set="key:value"`.
        _seg(label, key, options, title) {
            const cur = String(this.state[key]);
            const btns = options.map(([v, text, tip]) =>
                `<button class="view-btn${cur === v ? ' active' : ''}" data-set="${escapeAttr(`${key}:${v}`)}"${tip ? ` title="${escapeAttr(tip)}"` : ''}>${escapeHtml(text)}</button>`
            ).join('');
            return `<div class="view-toggle"${title ? ` title="${escapeAttr(title)}"` : ''}><span class="nway-lbl">${escapeHtml(label)}</span>${btns}</div>`;
        }

        _slider(label, key, value, min, max, step, title) {
            const shown = key === 'k' ? String(value) : Number(value).toFixed(2);
            return `<label class="nway-ctl"${title ? ` title="${escapeAttr(title)}"` : ''}><span class="nway-lbl">${escapeHtml(label)}</span><b>${shown}</b><input type="range" min="${min}" max="${max}"${step ? ` step="${step}"` : ''} value="${Number(value)}" data-range="${key}"></label>`;
        }

        _controls() {
            const s = this.state, d = this.data;
            const n = d.columns.length;
            const tabs = TABS.map(([key, label]) =>
                `<button class="bsim-tab${s.tab === key ? ' active' : ''}" data-tab="${key}">${label}<span class="nway-count" title="code / library">${this._count(key)}</span></button>`
            ).join('');
            const isNode = !!this.source.cluster_uuid;
            const ctl = [
                this._seg('Show', 'scope', [['code', 'Code'], ['library', 'Library'], ['all', 'All']]),
                isNode ? this._seg('Columns', 'columns', [['auto', 'Auto'], ['files', 'Files'], ['children', 'Children']], "the node's files, or one column per child cluster") : '',
                isNode && d.columns_mode === 'children'
                    ? this._slider('Child presence', 'child_presence', s.child_presence, 0.05, 1, 0.05, "share of a child's files that must hold the function for the child to count as present") : '',
                s.tab === 'partial' ? this._slider(`In at least (of ${n})`, 'k', s.k, 2, Math.max(2, n), 1) : '',
                this._slider('Min edge', 'min_edge', Number(d.min_edge), 0, 1, 0.05),
                this._seg('Mode', 'mode', [['stored', 'Stored', 'reads built pair docs'], ['virtual', 'Virtual', 'recomputes from vectors']]),
                `<input type="text" class="nway-input" data-text="q" placeholder="function name / address" value="${escapeAttr(s.q)}">`,
                `<input type="text" class="nway-input" data-text="tags" placeholder="tags" value="${escapeAttr(s.tags)}">`,
            ].join('');
            return `<div class="bsim-tabbar">${tabs}</div><div class="nway-controls">${ctl}</div>`;
        }

        _notes() {
            const d = this.data;
            let html = '';
            if (d.mode !== this.state.mode) html += `<div class="nway-note">Ran in <b>${escapeHtml(d.mode)}</b> mode: files span several collections.</div>`;
            if (d.fallback_pairs) html += `<div class="nway-note">${Number(d.fallback_pairs)} pairs computed on the fly (no stored pair doc).</div>`;
            if ((d.warnings || []).length) {
                html += `<div class="nway-note warn">${d.warnings.map(w => `<div>${escapeHtml(w)}</div>`).join('')}</div>`;
            }
            return html;
        }

        // The compact function entity: the same data, hover preview, click and
        // right-click menu EntityRenderer.renderFunction gives, with the address
        // as the label (the row already carries the name).
        _fnData(fid, col) {
            const m = this.data.functions_metadata[fid] || {};
            const addr = fnParts(fid).addr;
            return {
                function_id: fid, function_name: m.name || addr, namespace: m.namespace || '',
                parameters: m.parameters || [], return_type: m.return_type || '',
                entrypoint_address: m.entrypoint_address || addr, file_md5: col.md5,
                collection: col.collection, bsim_features_count: m.bsim_features_count || 0,
                tags: m.tags || [], user_tags: m.user_tags || [],
            };
        }

        _fnEntity(fid, col, rowName) {
            const f = this._fnData(fid, col);
            // Default names (FUN_, sub_, thunk_) add nothing next to the address.
            const note = /^(FUN_|sub_|thunk_)/i.test(f.function_name) ? ''
                : f.function_name === rowName
                    ? ' <span class="dim" title="same name as the function column">(*)</span>'
                    : ` <span class="dim">(${escapeHtml(f.function_name)})</span>`;
            const sig = typeof formatSigComponent === 'function'
                ? formatSigComponent(f.namespace, f.return_type, f.function_name, f.parameters).fullSig : f.function_name;
            return `<span class="entity-function" title="${escapeAttr(sig)}" data-etype="function" data-eid="${escapeAttr(fid)}"
                    data-entity-data='${escapeAttr(JSON.stringify(f))}'
                    oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>
                <b class="entity-name nway-addr"
                   onmouseenter="typeof showCodePreview === 'function' && showCodePreview(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, ${escapeAttr(jsString(f.entrypoint_address))}, ${escapeAttr(jsString(col.md5))}, ${Number(f.bsim_features_count) || 0}, event)"
                   onmousemove="typeof moveCodePreview === 'function' && moveCodePreview(event)"
                   onmouseleave="typeof hideCodePreview === 'function' && hideCodePreview(event)"
                   onclick="typeof showFunctionCodeById === 'function' && showFunctionCodeById(${escapeAttr(jsString(fid))}, ${escapeAttr(jsString(f.function_name))}, '', event)">@ ${escapeHtml(f.entrypoint_address)}</b>${note}</span>`;
        }

        // A child/direct column: "m/n files", plus links to the functions of the
        // files in that group (a few; the rest as a count).
        _groupCell(row, col) {
            const cell = row.cells[col.id];
            const pool = this.source.pool || null;
            const links = [];
            for (const f of this.data.file_columns || []) {
                const fid = col.members.includes(f.id) && (row.files || {})[f.id];
                if (!fid) continue;
                const data = this._fnData(fid, f);
                const url = fnUrl(f, fid, pool);
                links.push(`<a data-nav="${escapeAttr(url)}" data-title="${escapeAttr(f.file_name)}" title="${escapeAttr(f.file_name)}"
                    data-etype="function" data-eid="${escapeAttr(fid)}" data-entity-data='${escapeAttr(JSON.stringify(data))}'
                    oncontextmenu='EntityRenderer.handleContextMenu(event, "function", this)'>${escapeHtml(data.entrypoint_address)}</a>`);
            }
            const more = links.length > 3 ? `<span class="dim">+${links.length - 3}</span>` : '';
            return `<td class="col${cell.on ? '' : ' dim'}"><b>${Number(cell.present)}/${Number(cell.total)}</b> files${links.length ? `<div class="nway-group-links">${links.slice(0, 3).join('')}${more}</div>` : ''}</td>`;
        }

        _cell(row, col, ri) {
            if (col.kind) return this._groupCell(row, col);
            const fid = row.cells[col.id];
            if (!fid) return '<td class="col gap">&mdash;</td>';
            return `<td class="col"><div class="nway-fn">${this._fnEntity(fid, col, row.name)}</div></td>`;
        }

        // Support / cohesion use the same red-to-green ramp as the other score cells.
        _score(v) {
            let color = '';
            try { color = typeof window.scoreColor === 'function' ? window.scoreColor(v) : ''; } catch (e) { color = ''; }
            return `<span${color ? ` style="color:${escapeAttr(color)}"` : ''}>${pct(v)}</span>`;
        }

        // The row's guessed signature: the most common return type and parameter
        // list over its functions. The best function carries that signature (ties
        // broken by feature count) and feeds the hover preview.
        _rowSig(row) {
            const src = row.files || row.cells;
            const sigOf = m => formatSigComponent('', m.return_type, '', m.parameters);
            const items = [];
            for (const col of this.data.file_columns || this.data.columns.filter(c => !c.kind)) {
                const fid = src[col.id];
                const m = typeof fid === 'string' && this.data.functions_metadata[fid];
                if (m) items.push({ fid, col, m, ret: sigOf(m).ret, params: JSON.stringify(sigOf(m).params) });
            }
            if (!items.length) return null;
            const mode = key => {
                const n = {};
                items.forEach(i => { n[i[key]] = (n[i[key]] || 0) + 1; });
                return Object.entries(n).sort((x, y) => y[1] - x[1])[0][0];
            };
            const ret = mode('ret'), params = mode('params');
            const fits = items.filter(i => i.ret === ret && i.params === params);
            const best = fits.sort((x, y) => (y.m.bsim_features_count || 0) - (x.m.bsim_features_count || 0))[0];
            return { ret, params: JSON.parse(params), best };
        }

        _nameSig(row) {
            const sig = this._rowSig(row);
            const name = escapeHtml(row.name);
            if (!sig) return name;
            const f = this._fnData(sig.best.fid, sig.best.col);
            // Same renderer as the function search table, fed the guessed signature.
            return window.EntityRenderer.renderFunction({ ...f, function_name: row.name, return_type: sig.ret, parameters: sig.params }, { hideNote: true, showActions: false });
        }

        _row(row, ri) {
            const cols = this.data.columns;
            const extra = Array.isArray(row.names) ? row.names.length - 1 : Number(row.names || 0) - 1;
            const low = row.support < LOW_SUPPORT && (row.file_span || row.span) > 2
                ? `<i class="fa-solid fa-triangle-exclamation nway-warn" title="low support: only ${pct(row.support)} of the possible pairs in this row are matched; it may be a chain of transitive matches"></i>` : '';
            return `<tr>
                <td style="min-width:260px; max-width:420px;">${this._nameSig(row)}<span class="nway-name">${extra > 0 ? `<span class="dim">+${extra} names</span>` : ''}${row.library ? '<span class="badge">lib</span>' : ''}${low}</span></td>
                <td class="num">${Number(row.span)}</td>
                <td class="num">${Number(row.weight).toFixed(0)}</td>
                <td class="num">${this._score(row.support)}</td>
                <td class="num">${this._score(row.cohesion)}</td>
                ${cols.map(c => this._cell(row, c, ri)).join('')}
            </tr>`;
        }

        _head() {
            const s = this.state;
            const th = ([key, label]) => {
                const arrow = s.sort_col === key ? (s.sort_dir === 'asc' ? '▲' : '▼') : '↕';
                return `<th class="sortable${key === 'name' ? '' : ' num'}" data-sort="${key}">${label} <span class="dim">${arrow}</span></th>`;
            };
            const fileHead = c => {
                const named = c.file_name && c.file_name !== c.md5;
                const name = named ? window.EntityRenderer.renderFileName(c.file_name, c.md5, c.collection) : '';
                const coverage = c.coverage == null ? '' : ` &middot; <span title="share of the shared (span >= 2) weight this file holds">${(Number(c.coverage) * 100).toFixed(0)}% shared</span>`;
                return `<th class="col" title="${escapeAttr(`${c.file_name} (${c.collection})`)}"><div class="nway-colhead">${name}${window.EntityRenderer.renderMd5(c.md5, { collection: c.collection })}<span class="nway-sub">${Number(c.functions)} fn${coverage}</span></div></th>`;
            };
            const colHead = c => c.kind
                ? `<th class="col" title="${escapeAttr(c.kind === 'child' ? 'child cluster' : 'files at this node, in no child cluster')}"><div class="nway-colhead"><span class="nway-colname">${escapeHtml(c.label)}</span><span class="nway-sub">${Number(c.member_count)} files</span></div></th>`
                : fileHead(c);
            return `<tr>${SORTS.map(th).join('')}${this.data.columns.map(colHead).join('')}</tr>`;
        }

        render() {
            if (this.error) {
                this.el.innerHTML = `<div class="nway-panel">${this._renderError()}</div>`;
                return;
            }
            const d = this.data;
            const rows = d.items.map((r, i) => this._row(r, i)).join('');
            const off = Number(d.offset), total = Number(d.total);
            this.el.innerHTML = `<div class="nway-panel">${this._controls()}${this._notes()}
                <div class="nway-card table-scope">
                    <div class="nway-scroll">
                        <table id="nway-table" class="nway-table">
                            <thead>${this._head()}</thead>
                            <tbody>${rows || `<tr><td colspan="${d.columns.length + 6}" class="gap">No rows in this tab.</td></tr>`}</tbody>
                        </table>
                    </div>
                    <div class="table-footer">
                        <div class="table-footer-left">
                            <button class="top-action-btn" data-page="-1"${off <= 0 ? ' disabled' : ''}>Prev</button>
                            <span class="table-footer-badge">${total ? off + 1 : 0}&ndash;${Math.min(off + PAGE, total)} of ${total}</span>
                            <button class="top-action-btn" data-page="1"${off + PAGE >= total ? ' disabled' : ''}>Next</button>
                        </div>
                        <div class="table-footer-right">
                            <span class="table-footer-sel selection-stats" style="display:none;"></span>
                        </div>
                    </div>
                </div></div>`;
            // The shared grid selection: it reads the function entities in the
            // selected cells, so the bulk actions of the function menu apply.
            if (window.TableSelection) new window.TableSelection('nway-table');
        }

        _onClick(e) {
            const t = e.target.closest('[data-set],[data-tab],[data-sort],[data-page],[data-nav],[data-open-cluster]');
            if (!t) return;
            if (t.dataset.openCluster) {
                e.preventDefault();
                return this.onOpenCluster(t.dataset.openCluster);
            }
            const pool = this.source.pool || null;
            if (t.dataset.set) {
                const i = t.dataset.set.indexOf(':');
                return this._set({ [t.dataset.set.slice(0, i)]: t.dataset.set.slice(i + 1), offset: 0 });
            }
            if (t.dataset.tab) return this._set({ tab: t.dataset.tab, offset: 0, sort_col: '' });
            if (t.dataset.sort) {
                const same = this.state.sort_col === t.dataset.sort;
                return this._set({ sort_col: t.dataset.sort, sort_dir: same && this.state.sort_dir === 'desc' ? 'asc' : 'desc', offset: 0 });
            }
            if (t.dataset.page) return this._set({ offset: Math.max(0, Number(this.state.offset) + Number(t.dataset.page) * PAGE) });
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
