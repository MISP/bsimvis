/**
 * N-way diff view:
 * /diff/nway?md5s=coll:md5,coll:md5[&batch_uuid=U][&collection=C][&pool=P][&tab=..&min_edge=..]
 *
 * The set (md5s + batch) is edited in the "Edit set" panel; control state is
 * read from the URL and written back so the page is deep-linkable.
 */
window.NwayView = {
    _panel: null,
    _set: { md5s: [], batch: '', collection: '', pool: '' },
    _state: {},
    _collections: null,

    destroy() {
        if (this._panel) this._panel.destroy();
        this._panel = null;
    },

    async init(params, containerId) {
        this._container = document.getElementById(containerId);
        if (!this._container) return;
        const q = new URLSearchParams(window.location.search);
        this._set = {
            md5s: (q.get('md5s') || '').split(',').map(t => t.trim()).filter(Boolean),
            batch: q.get('batch_uuid') || '',
            collection: q.get('collection') || '',
            pool: q.get('pool') || '',
        };
        this._state = {};
        for (const k of ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset', 'column', 'focus', 'focus_rule', 'side']) {
            if (q.has(k)) this._state[k] = q.get(k);
        }
        this._collections = null;
        this._render();
    },

    _source() {
        const s = this._set;
        const source = {};
        if (s.md5s.length) source.md5s = s.md5s.join(',');
        if (s.batch) source.batch_uuid = s.batch;
        if (s.collection) source.collection = s.collection;
        if (s.pool) source.pool = s.pool;
        return source;
    },

    _writeUrl(extra) {
        const next = new URLSearchParams(this._source());
        for (const [k, v] of Object.entries(extra || {})) if (v !== '' && v !== null && v !== undefined) next.set(k, v);
        history.replaceState(null, '', `${window.location.pathname}?${next.toString()}`);
    },

    _hasSet() {
        return this._set.batch || this._set.md5s.length >= 2;
    },

    _render() {
        this.destroy();
        this._container.innerHTML = `
            <div class="nway-view">
                <div class="nway-head">
                    <h2><i class="fa-solid fa-table-columns"></i> N-way File Diff</h2>
                    <p>Functions shared across several files, and the ones unique to a single file.</p>
                </div>
                <details id="nway-edit" class="nway-edit" ${this._hasSet() ? '' : 'open'}>
                    <summary>Edit set (${this._set.md5s.length} files${this._set.batch ? ' + 1 batch' : ''})</summary>
                    <div id="nway-set"></div>
                </details>
                <div id="nway-panel"></div>
            </div>`;
        this._renderSet();
        this._mount();
    },

    _mount() {
        const host = document.getElementById('nway-panel');
        if (!host) return;
        if (!this._hasSet()) {
            host.innerHTML = '<div class="nway-state">Add two or more files (or a batch) above, or pick rows in a file table and use "Add to compare set".</div>';
            return;
        }
        this._panel = new NwayPanel(host, {
            source: this._source(),
            state: this._state,
            onState: s => {
                this._state = { ...s };
                this._writeUrl(s);
            },
        });
        this._panel.load();
    },

    // `coll:md5` (or a bare md5) -> its collection and md5.
    _split(token) {
        const i = token.lastIndexOf(':');
        return { coll: i > 0 ? token.slice(0, i) : this._set.collection, md5: token.slice(i + 1) };
    },

    async _loadCollections() {
        if (this._collections) return this._collections;
        try {
            const res = await fetch('/api/collection/search?limit=10000');
            const data = await res.json();
            this._collections = (data.collections || []).map(c => c.name);
        } catch (e) {
            this._collections = [];
        }
        return this._collections;
    },

    async _renderSet() {
        const el = document.getElementById('nway-set');
        if (!el) return;
        const colls = await this._loadCollections();
        const summary = document.querySelector('#nway-edit summary');
        if (summary) summary.textContent = `Edit set (${this._set.md5s.length} files${this._set.batch ? ' + 1 batch' : ''})`;
        const pick = this._set.collection || (window.getRoutingState ? window.getRoutingState().collection : '') || '';
        const opts = sel => colls.map(c => `<option value="${escapeAttr(c)}" ${c === sel ? 'selected' : ''}>${escapeHtml(c)}</option>`).join('');
        const chip = (inner, title, i, kind) => `<span class="nway-chip" title="${escapeAttr(title)}">${inner}<button data-rm="${kind}" data-i="${i}" title="Remove">&times;</button></span>`;
        const members = this._set.md5s.map((t, i) => {
            const { coll, md5 } = this._split(t);
            return chip(`${window.EntityRenderer.renderMd5(md5, { collection: coll })}${coll ? `<span>${escapeHtml(coll)}</span>` : ''}`, t, i, 'md5');
        }).join('')
            + (this._set.batch ? chip(`<span>batch ${escapeHtml(this._set.batch.slice(0, 8))}</span>`, this._set.batch, 0, 'batch') : '');
        const basket = window.NwayBasket ? window.NwayBasket.items() : [];
        el.innerHTML = `
            <div class="nway-chips">${members || '<span class="dim">No files yet.</span>'}</div>
            <div class="nway-edit-grid">
                <div class="nway-edit-col">
                    <span class="nway-lbl">Paste md5s (<code>collection:md5</code>, or a bare md5)</span>
                    <textarea id="nway-paste" class="nway-input" rows="3"></textarea>
                    <div class="nway-edit-row">
                        <select id="nway-paste-coll" class="nway-input" title="Collection for bare md5s">${opts(pick)}</select>
                        <button id="nway-add-md5" class="top-action-btn">Add files</button>
                    </div>
                </div>
                <div class="nway-edit-col">
                    <span class="nway-lbl">Batch UUID (all its files)</span>
                    <input id="nway-batch" type="text" class="nway-input" value="${escapeAttr(this._set.batch)}">
                    <div class="nway-edit-row">
                        <select id="nway-batch-coll" class="nway-input" title="Collection of the batch"><option value="">any collection</option>${opts(this._set.collection)}</select>
                        <button id="nway-add-batch" class="top-action-btn">Use batch</button>
                    </div>
                </div>
                ${basket.length ? `<div class="nway-edit-col"><span class="nway-lbl">Compare set</span><button id="nway-use-basket" class="top-action-btn">Add ${basket.length} from compare set</button></div>` : ''}
            </div>
            <div id="nway-set-msg" class="nway-msg"></div>`;

        const msg = t => { el.querySelector('#nway-set-msg').textContent = t; };
        const apply = () => { this._renderSet(); this._writeUrl(this._state); this._mount(); };
        const addTokens = tokens => {
            this._set.md5s = [...new Set([...this._set.md5s, ...tokens])];
            apply();
        };

        el.querySelectorAll('[data-rm]').forEach(btn => btn.addEventListener('click', () => {
            if (btn.dataset.rm === 'batch') this._set.batch = '';
            else this._set.md5s.splice(Number(btn.dataset.i), 1);
            this.destroy();
            apply();
        }));
        el.querySelector('#nway-add-md5').addEventListener('click', () => {
            const coll = el.querySelector('#nway-paste-coll').value;
            const out = [];
            for (const t of el.querySelector('#nway-paste').value.split(/[\s,]+/).filter(Boolean)) {
                if (!/^([^\s:]+:)?[0-9a-f]{32}$/i.test(t)) return msg(`Not an md5 or collection:md5: ${t.slice(0, 60)}`);
                if (!t.includes(':') && !coll) return msg('Pick a collection for bare md5s.');
                out.push(t.includes(':') ? t.toLowerCase() : `${coll}:${t.toLowerCase()}`);
            }
            if (!out.length) return msg('Nothing to add.');
            this.destroy();
            addTokens(out);
        });
        el.querySelector('#nway-add-batch').addEventListener('click', () => {
            const uuid = el.querySelector('#nway-batch').value.trim();
            if (!/^[0-9a-f-]{8,40}$/i.test(uuid)) return msg('Not a batch UUID.');
            this._set.batch = uuid;
            this._set.collection = el.querySelector('#nway-batch-coll').value;
            this.destroy();
            apply();
        });
        const useBasket = el.querySelector('#nway-use-basket');
        if (useBasket) useBasket.addEventListener('click', () => { this.destroy(); addTokens(basket); });
    },
};
