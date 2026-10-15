/**
 * N-way function view: one function family of the N-way table on its own page
 * (/diff/nway/function?<source>&<controls>&fx_*), backed by
 * GET /api/bin_sim/nway/facets. Header cards + similarity matrix, then
 * Simple / Facets / Code blocks / Pair diff tabs.
 *
 *   new NwayFunction(container, { source, doc, fids, state, onState }).load();
 *
 * `source` and `doc` are the member params and doc controls of the table the
 * row came from; `state` ({tab, cv, fn, only, single}) is the view state the URL
 * carries and `onState(state)` fires on every change. Every value comes off an
 * uploaded sample: it all goes through escapeHtml / escapeAttr.
 */
(function () {
    const PAIR_ROWS = 3000;
    const hue = j => Math.round((j * 137.508 + 215) % 360);
    const cc = j => `style="--cc:hsl(${hue(j)} 62% var(--xp-l))"`;
    const parts = fid => {
        const [coll, rest = ''] = fid.split(':func:');
        const i = rest.indexOf(':');
        return { coll, md5: rest.slice(0, i), addr: rest.slice(i + 1) };
    };

    // ---- What a same-code group ignores. Mirrors `_keys` in nway_facets_service.py. ----
    const REL = { eq: 'identical text', noise: 'names only', const: 'constants differ', chg: 'code differs' };
    const REL_TIP = {
        eq: 'Every member has the same text.',
        noise: 'Members differ only in decompiler-chosen names, types and casts.',
        const: 'Members differ in constants, which the matcher ignores.',
        chg: 'Members differ in more than names and constants.',
    };
    const RANK = { eq: 0, noise: 1, const: 2, chg: 3 };
    const T_VAR = /^([a-z]{0,3}Var\d+|local_\w+|[a-z]?Stack_\w+|in_\w+|extraout_\w+)$/;
    const T_TYPE = /^(undefined\d*|[ui]?int(\d+_t)?|u?(long|short|char)|u?longlong|byte|bool|code|[su]?size_t|unsigned|signed|[dq]?word)$/;
    const T_ADDR = /\b(FUN|DAT|LAB|PTR|SUB|UNK|switchD|caseD)_(?:[a-z]+_)?[0-9a-fA-F]+/;
    const T_NUM = /^(0[xX][0-9a-fA-F]+|\d+)[uUlL]*$/;
    const T_CAST = /^\(\s*(?:[A-Za-z_]\w*\s+)*[A-Za-z_]\w*\s*\*+\s*\)$|^\(\s*(?:[A-Za-z_]\w*\s+)*[A-Za-z_]\w*\s*\)$/;
    const tkey = t => (T_VAR.test(t) ? 'V' : T_TYPE.test(t) ? 'T' : t.replace(T_ADDR, '$1_'));
    const LCS_MAX = 1e6;

    // Token-level diff of two code texts: `ma` / `mb` are [text, class] runs ('' | noise | const | chg).
    function tokDiff(x, y) {
        const none = { rel: 'eq', ma: null, mb: null, counts: { noise: 0, const: 0, chg: 0 } };
        if (x === y) return none;
        const ta = x.match(/\w+|\s+|[^\w\s]/g) || [], tb = y.match(/\w+|\s+|[^\w\s]/g) || [];
        const ia = [], ib = [];
        ta.forEach((t, i) => { if (!/^\s/.test(t)) ia.push(i); });
        tb.forEach((t, i) => { if (!/^\s/.test(t)) ib.push(i); });
        const n = ia.length, m = ib.length, ca = new Array(n).fill(''), cb = new Array(m).fill('');
        const match = [];
        if ((n + 1) * (m + 1) > LCS_MAX) { ca.fill('chg'); cb.fill('chg'); } else {
            const A = ia.map(i => tkey(ta[i])), B = ib.map(i => tkey(tb[i])), w = m + 1, dp = new Int32Array((n + 1) * w);
            for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) dp[i * w + j] = A[i] === B[j] ? dp[(i + 1) * w + j + 1] + 1 : Math.max(dp[(i + 1) * w + j], dp[i * w + j + 1]);
            for (let i = 0, j = 0; i < n && j < m;) {
                if (A[i] === B[j]) { match.push([i, j]); i++; j++; } else if (dp[(i + 1) * w + j] >= dp[i * w + j + 1]) i++; else j++;
            }
            const side = (toks, idx, c, from, to) => {
                const s = idx.slice(from, to).map(i => toks[i]).join('');
                for (let k = from; k < to; k++) c[k] = T_CAST.test(s) ? 'noise' : 'chg';
            };
            let pi = 0, pj = 0;
            for (const [i, j] of [...match, [n, m]]) {
                if (i - pi === j - pj) {
                    for (let k = 0; k < i - pi; k++) {
                        const a = ta[ia[pi + k]], b = tb[ib[pj + k]], c = T_NUM.test(a) && T_NUM.test(b) ? 'const' : 'chg';
                        ca[pi + k] = cb[pj + k] = c;
                    }
                } else { side(ta, ia, ca, pi, i); side(tb, ib, cb, pj, j); }
                if (i < n && ta[ia[i]] !== tb[ib[j]]) ca[i] = cb[j] = 'noise';
                pi = i + 1; pj = j + 1;
            }
        }
        const runs = (toks, idx, c) => {
            const at = new Map(idx.map((t, k) => [t, c[k]]));
            return toks.map((t, i) => [t, at.get(i) || '']);
        };
        const counts = { noise: 0, const: 0, chg: 0 };
        for (const k of Object.keys(counts)) counts[k] = Math.max(ca.filter(c => c === k).length, cb.filter(c => c === k).length);
        const rel = counts.chg ? 'chg' : counts.const ? 'const' : counts.noise ? 'noise' : 'eq';
        // layout alone (whitespace) is not worth a mark
        if (rel === 'eq') return none;
        return { rel, ma: runs(ta, ia, ca), mb: runs(tb, ib, cb), counts };
    }

    class NwayFunction {
        constructor(el, o) {
            this.el = el;
            this.source = o.source || {};
            this.doc = o.doc || {};
            this.fids = o.fids;
            this.name = o.name || '';
            this.onState = o.onState || null;
            this.embedded = !!o.embedded;
            this.cfgOnly = !!o.cfgOnly;
            this.onLoad = o.onLoad || null;
            this.s = { tab: 'facets', only: false, pair: null, cv: 'blocks', bm: 'hybrid', open: {}, fn: null, single: false, cols: 2, pick: {}, uoff: {}, uopen: {}, ...(o.state || {}) };
            this._pc = new Map();
            if (this.s.tab === 'code' && this.s.cv === 'full') this.s.tab = 'full';
            this.titles = {};
            this.expl = {};
            this.sums = {};
            this.data = null;
            this.simpleCode = null;
            this._dead = false;
            el.addEventListener('click', e => this._click(e));
            el.addEventListener('change', e => this._change(e));
            el.addEventListener('mouseover', e => this._callHover(e, true));
            el.addEventListener('mouseout', e => this._callHover(e, false));
            el.addEventListener('mouseover', e => this._cfgHover(e, true));
            el.addEventListener('mouseout', e => this._cfgHover(e, false));
            // Feature tokens light up their twins in both panes; a click locks them (same helpers as the function diff).
            el.addEventListener('mouseover', e => this._featHover(e, true));
            el.addEventListener('mouseout', e => this._featHover(e, false));
            el.addEventListener('click', e => { const t = e.target.closest('.feature-highlight'); if (t && !e.target.closest('[data-xcall]') && this.el.contains(t)) window.toggleLock?.(t.dataset.hashes, t); });
            el.addEventListener('contextmenu', e => this._entityMenu(e));
            this._railFn = () => this._railSync();
            addEventListener('scroll', this._railFn, true);
            addEventListener('resize', this._railFn);
            el.addEventListener('pointerdown', e => this._railDrag(e));
            // Side-by-side code panes scroll together (scroll does not bubble, so capture it).
            el.addEventListener('scroll', e => {
                const pre = e.target;
                if (!pre.matches || !pre.matches('pre') || this._syncing) return;
                const box = pre.closest('.xp-panes, .xp-frow');
                if (!box) return;
                this._syncing = true;
                box.querySelectorAll('pre').forEach(o => { if (o !== pre) { o.scrollTop = pre.scrollTop; if (o.scrollLeft !== pre.scrollLeft) o.scrollLeft = pre.scrollLeft; } });
                requestAnimationFrame(() => { this._syncing = false; });
            }, true);
            el.addEventListener('keydown', e => {
                if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-xfnref],[data-xfile],[data-xcall]')) this._click(e);
            });
        }

        destroy() {
            this._dead = true;
            removeEventListener('scroll', this._railFn, true);
            removeEventListener('resize', this._railFn);
        }

        // The rail is the scrollbar of the block grid: its thumb is the part of the grid in view, drag it to scroll.
        _railSync() {
            const wrap = this.el.querySelector('.xp-gwrap'), rail = wrap && wrap.querySelector('.xp-rail'), th = rail && rail.querySelector('.xp-thumb');
            if (!th) return;
            const r = wrap.getBoundingClientRect(), H = r.height, rh = rail.clientHeight;
            const a = Math.max(0, -r.top), b = Math.min(H, innerHeight - r.top);
            th.style.display = b > a ? '' : 'none';
            th.style.top = (a / H) * rh + 'px';
            th.style.height = Math.max(8, ((b - a) / H) * rh) + 'px';
        }

        _railDrag(e) {
            const th = e.target.closest('.xp-thumb');
            if (!th) return;
            e.preventDefault();
            const wrap = th.closest('.xp-gwrap'), k = wrap.getBoundingClientRect().height / th.parentElement.clientHeight, y0 = e.clientY, s0 = scrollY;
            const mv = m => scrollTo(0, s0 + (m.clientY - y0) * k);
            const up = () => { removeEventListener('pointermove', mv); removeEventListener('pointerup', up); };
            addEventListener('pointermove', mv);
            addEventListener('pointerup', up);
        }

        _qs() {
            const qs = new URLSearchParams();
            for (const [k, v] of Object.entries({ ...this.source, ...this.doc })) if (v !== null && v !== undefined && v !== '') qs.set(k, v);
            qs.set('fids', this.fids.join(','));
            qs.set('blocks', this.s.bm || 'hybrid');
            return qs;
        }

        _persist() {
            if (!this.onState) return;
            const { tab, cv, fn, only, single, bm, cols } = this.s;
            this.onState({ tab, cv, fn, only, single, bm, cols });
        }

        async load() {
            this.el.innerHTML = `<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i> Comparing ${this.fids.length} functions...</div>`;
            try {
                const res = await fetch(`/api/bin_sim/nway/facets?${this._qs().toString()}`);
                const body = await res.json().catch(() => ({}));
                if (!res.ok) throw new Error(body.message || body.error || body.detail || `HTTP ${res.status}`);
                if (this._dead) return;
                this.data = body;
                this._pc = new Map();
            } catch (e) {
                if (!this._dead) this.el.innerHTML = `<div class="nway-state">Could not compare these functions: ${escapeHtml(String(e.message || e))}</div>`;
                return;
            }
            this.render();
            if (this.onLoad) this.onLoad();
        }

        render() {
            const d = this.data, s = this.s;
            const code = d.consensus;
            if (this.cfgOnly) { this.el.innerHTML = this._cfgOnly(); return; }
            if (['code', 'full', 'unified'].includes(s.tab) && !code && !d.blocks_error) s.tab = 'facets';
            const tab = (k, label, why) => `<button class="bsim-tab${s.tab === k ? ' active' : ''}" role="tab" data-xtab="${k}"${why ? ` title="${escapeAttr(why)}"` : ''}>${label}</button>`;
            const nf = d.functions.length;
            this.el.innerHTML = `
                ${this.embedded ? '' : `<div class="nway-head"><h2><i class="fa-solid fa-table-columns"></i> ${escapeHtml(this.name || 'Function family')} <span class="exp-badge" title="Experimental: results and layout may change"><i class="fa-solid fa-flask"></i> Experimental</span></h2>
                    <p>${nf} functions, one per file. Strings, calls, constants and code compared across them.</p></div>`}
                <div class="nway-card xp-head"><div class="xp-cols">${d.functions.map((f, j) => this._card(f, j)).join('')}</div>
                    <div><div class="xp-mx" style="--n:${nf}">${this._matrix()}</div>
                    <div class="xp-cap">${this._info('Function similarity from the matcher; a dash means no match was found. Click a cell to open that pair in the 2-way diff.')}</div></div></div>
                <div class="xp-bar"><div class="bsim-tabbar" role="tablist" style="margin:0">
                    ${this.embedded ? '' : tab('simple', 'Reference')}
                    ${this.embedded ? '' : tab('facets', 'Facets')}
                    ${this.embedded ? '' : tab('code', 'Code blocks', !code ? `Code blocks need at most ${d.limits.code} functions (this row has ${nf})` : '')}
                    ${this.embedded ? '' : tab('full', 'Full function', !code ? `Code blocks need at most ${d.limits.code} functions (this row has ${nf})` : '')}
                    ${this.embedded ? '' : tab('unified', 'Unified', !code ? `Code blocks need at most ${d.limits.code} functions (this row has ${nf})` : '')}
                    ${this.embedded ? '' : tab('pair', 'Pair diff')}</div><span class="xp-sp"></span>
                    ${s.tab === 'pair' || s.tab === 'simple' ? '' : `<label class="xp-chk"><input type="checkbox" data-xonly ${s.only ? 'checked' : ''}> Differences only</label>`}</div>
                <div class="xp-body">${this._body()}</div>`;
            const bar = this.el.querySelector('.xp-bar');
            new ResizeObserver(() => this.el.style.setProperty('--xp-bar2-h', bar.getBoundingClientRect().height + 'px')).observe(bar);
            this._loadSummaries();
            if (s.tab === 'pair') this._loadPair();
            if (['code', 'full', 'unified'].includes(s.tab)) this._loadTitles();
            if (s.tab === 'simple') this._loadSimple();
            this._railSync();
        }

        // Diff-view tab: just the reference-vs-function CFG pairs, no cards or tabs.
        _cfgOnly() {
            const d = this.data;
            if (!d.consensus) {
                const miss = d.blocks_error ? d.blocks_error.missing || [] : [];
                return `<div class="xp-cbar">${this._bmToggle()}</div><div class="xp-none">${miss.length ? `Ghidra blocks are not stored for ${miss.map(j => this._funcRef(j)).join(' ')}: analysed before the CFG was stored. Re-analyse them, or use Hybrid or Text blocks.` : 'No block comparison available for these functions.'}</div>`;
            }
            return `<div class="xp-cbar">${this._bmToggle()}</div>` + (this._cfgStrip() || '<div class="xp-none">No stored control-flow graph for these functions. Re-analyse the files.</div>');
        }

        _body() {
            const t = this.s.tab;
            return t === 'simple' ? this._simple() : t === 'facets' ? this._facets() : t === 'code' || t === 'full' || t === 'unified' ? this._code() : this._pair();
        }

        // Server-tokenized code token; a resolved call becomes a data-xcall link like the block views use.
        _tok(t) {
            const h = window.renderTokenHtml(t);
            return t.called_func_id && !t.is_external
                ? h.replace('<span ', `<span data-xcall="${escapeAttr(t.called_func_id)}" data-xcall-name="${escapeAttr(t.target_name || '')}" `) : h;
        }

        _line(row, cls = '') {
            return `<div class="code-line ${escapeAttr(cls)}"><div class="gutter" contenteditable="false"><div class="line-num"${row.tooltip_text ? ` title="${escapeAttr(row.tooltip_text)}"` : ''}>${Number(row.line_idx)}</div></div><div class="line-content">${(row.tokens || []).map(t => this._tok(t)).join('')}</div></div>`;
        }

        _simple() {
            const f = this.data.functions[this.data.reference];
            const url = window.NwayPanel.fnUrl(f.id, this.source.pool || null);
            return `<p class="xp-hint">★ Most central function: ${escapeHtml(f.file_name || '')} @ ${escapeHtml(f.addr)} · <a href="${escapeAttr(url)}" data-xnav="${escapeAttr(url)}" data-title="${escapeAttr(f.name || f.addr)}">Open function</a></p>
                <div class="c-code-container" style="overflow:auto">${this.simpleCode || 'Loading code...'}</div>`;
        }

        async _loadSimple() {
            if (this.simpleCode || this.simpleLoading) return;
            this.simpleLoading = true;
            const id = this.data.functions[this.data.reference].id;
            try {
                const res = await fetch(`/api/function/code?id=${encodeURIComponent(id)}`);
                if (!res.ok) throw new Error(`Function not found (${res.status})`);
                const data = await res.json();
                this.simpleCode = (data.rows || []).map(row => this._line(row)).join('') || 'No code available.';
            } catch (e) {
                this.simpleCode = `<div class="xp-none">${escapeHtml(e.message)}</div>`;
            }
            this.simpleLoading = false;
            if (!this._dead && this.s.tab === 'simple') this._rebody();
        }

        // `focus`: block id to scroll to once the body is redrawn (a matrix click opens it and jumps there).
        _rebody(focus = null) {
            this.el.querySelector('.xp-body').innerHTML = this._body();
            const hd = focus && this.el.querySelector(`.xp-bh[data-xb="${CSS.escape(focus)}"]`);
            if (hd) hd.closest('.xp-blk').scrollIntoView({ block: 'start', behavior: 'smooth' });
            if (this.s.tab === 'pair') this._loadPair();
            this._railSync();
        }

        // The (i) tooltip every explanatory text lives in; `text` is plain, escaped here.
        _info(text) {
            return `<span class="xp-info" tabindex="0" role="img" aria-label="${escapeAttr(text)}"><i>i</i><span class="xp-tipbox" role="tooltip">${escapeHtml(text)}</span></span>`;
        }

        _funcRef(j, cls = 'xp-badge', link = true) {
            const f = this.data.functions[j];
            return `<span class="${cls}${link ? ' xp-entity-link' : ''}" ${cc(j)}${link ? ` data-xfnref="${j}" role="link" tabindex="0" title="${escapeAttr(`Open function ${f.name || f.addr}`)}"` : ''}>${escapeHtml(f.letter)}</span>`;
        }

        _fileRef(j, cls = 'xp-fn', link = true) {
            const f = this.data.functions[j], { coll, md5 } = parts(this.fids[j] || '');
            // Functions picked from several collections: say which one each column is from.
            const label = new Set(this.data.functions.map(x => x.collection)).size > 1 ? `${coll} / ${f.file_name || md5}` : (f.file_name || md5);
            return `<span class="${cls}${link ? ' xp-entity-link' : ''}"${link ? ` data-xfile="${j}" role="link" tabindex="0" title="${escapeAttr(`${label} (${md5})`)}"` : ''}>${escapeHtml(label)}</span>`;
        }

        _md5Ref(j) {
            const { md5 } = parts(this.fids[j] || '');
            return md5 ? `<span class="xp-md5 xp-entity-link" data-xfile="${j}" role="link" tabindex="0" title="${escapeAttr(md5)}"># ${escapeHtml(md5.slice(0, 8))}</span>` : '';
        }

        _card(f, j) {
            const url = window.NwayPanel.fnUrl(f.id, this.source.pool || null);
            const ref = j === this.data.reference ? '<span class="xp-ref" title="Reference of the consensus code: the function most similar to the others">ref</span>' : '';
            return `<div class="xp-col" ${cc(j)}>
                <div class="xp-l">${this._funcRef(j)}${this._fileRef(j)}${ref}</div>
                <div class="xp-f"><a data-xnav="${escapeAttr(url)}" data-xfnref="${j}" data-title="${escapeAttr(f.name || f.addr)}" href="${escapeAttr(url)}">@ ${escapeHtml(f.addr)}</a>${this._md5Ref(j)}${f.instr != null ? ` &middot; ${Number(f.instr)} instr` : ''}</div>
                <div class="xp-t">${(f.tags || []).slice(0, 3).map(t => `<span class="xp-tag">${escapeHtml(t)}</span>`).join('')}</div>${this._sumHtml(j)}</div>`;
        }

        _matrix() {
            const fs = this.data.functions, sim = this.data.similarity;
            let h = '<span></span>' + fs.map((_, j) => this._funcRef(j, 'xp-h')).join('');
            sim.forEach((row, r) => {
                h += this._funcRef(r, 'xp-h');
                row.forEach((v, c) => {
                    if (r === c) { h += '<span class="xp-v self">&middot;</span>'; return; }
                    const bg = v == null ? '' : ` style="background:color-mix(in srgb, var(--accent) ${Math.round(Math.max(0, (v - 0.5) / 0.5) * 70 + 6)}%, var(--card-bg))"`;
                    h += `<button class="xp-v" data-xpair="${r},${c}"${bg} title="${escapeAttr(`${fs[r].letter} vs ${fs[c].letter}: ${v == null ? 'no match' : v.toFixed(2)}`)}">${v == null ? '&ndash;' : escapeHtml(v.toFixed(2).replace(/^0/, ''))}</button>`;
                });
            });
            return h;
        }

        // ---- Facets ----
        _dots(mask) {
            return [...mask].map((x, j) => `<td class="xp-c" ${cc(j)}><span class="xp-dot${x === '1' ? '' : ' no'}"></span></td>`).join('');
        }

        _thead() {
            return `<tr><th></th>${this.data.functions.map((f, j) => `<th ${cc(j)}>${this._funcRef(j)}</th>`).join('')}</tr>`;
        }

        // A string item opens the normal functions table filtered by that string.
        _label(facet, item) {
            const text = escapeHtml(item.label);
            if (facet.key !== 'strings') return text;
            const coll = parts(this.fids[0]).coll;
            const path = `/collections/${encodeURIComponent(coll)}/functions?strings=${encodeURIComponent(quoteFilterValue(item.label.slice(0, 128)))}`;
            return `<a href="${escapeAttr(path)}" data-xnav="${escapeAttr(path)}" data-title="Functions using this string" title="Functions using this string">${text}</a>`;
        }

        _facet(f) {
            const n = this.data.functions.length;
            if (f.skipped) return `<div class="xp-grp"><h3>${escapeHtml(f.title)}</h3><div class="xp-none">${escapeHtml(f.skipped)}</div></div>`;
            const items = this.s.only ? f.items.filter(i => i.count < n) : f.items;
            const head = `<h3>${escapeHtml(f.title)}<span class="n">${Number(f.total)} total, ${Number(f.in_all)} in all ${n}${f.truncated ? `, ${Number(f.truncated)} not listed` : ''}</span></h3>`;
            if (!f.total) {
                const why = f.key === 'strings' ? 'No strings extracted for these collections.' : 'None in these functions.';
                return `<div class="xp-grp">${head}<div class="xp-none">${why}</div></div>`;
            }
            if (!items.length) return `<div class="xp-grp">${head}<div class="xp-none">All ${Number(f.total)} are in every function.</div></div>`;
            return `<div class="xp-grp">${head}<div class="xp-fx"><table>${this._thead()}${items.map(i => `<tr class="${i.count < n ? 'diff' : ''}"><td class="xp-i">${this._label(f, i)}<span class="xp-cnt${i.count === n ? ' all' : ''}">${i.count === n ? 'all' : `${Number(i.count)}/${n}`}</span></td>${this._dots(i.mask)}</tr>`).join('')}</table></div></div>`;
        }

        _facets() {
            const d = this.data;
            const shape = d.shape.map(r => `<tr><td class="xp-i xp-plain">${escapeHtml(r.label)}</td>${r.values.map((v, j) => `<td class="xp-c${r.off[j] ? ' out' : ''}">${v == null ? '&ndash;' : Number(v)}</td>`).join('')}</tr>`).join('');
            return `<div class="xp-cbar" style="margin-bottom:6px"><span class="xp-hint" style="margin:0">Shared and missing items</span>${this._info(`Every string, API call, callee and constant found in the ${d.functions.length} functions, one list per kind. A dot means the function has it; items missing from some functions are marked in amber.`)}</div>
                ${d.facets.map(f => this._facet(f)).join('')}
                <div class="xp-grp"><h3>Shape<span class="n">${this._info('Amber = more than 20% from the median.')}</span></h3><div class="xp-fx"><table>${this._thead()}${shape}</table></div></div>`;
        }

        // ---- Code blocks ----
        static GLYPH = { same: '=', mod: '~', moved: '⇄', restr: '◧', miss: '✕', extra: '+' };
        static SNAME = { same: 'same code', mod: 'modified', moved: 'moved', restr: 'restructured', miss: 'missing', extra: 'only here' };

        _bt(b) { return this.titles[b.id] || b.title; }

        // Title span the LLM titles are swapped into in place; the deterministic label stays as tooltip.
        _tspan(b) {
            const t = this.titles[b.id];
            return `<span class="bt" data-btitle="${escapeAttr(b.id)}" title="${escapeAttr(t ? b.title : '')}">${escapeHtml(t || b.title)}</span>`;
        }

        _runs(runs, cls) {
            return (runs || []).map(([t, ch]) => (ch ? `<b class="${cls}">${escapeHtml(t)}</b>` : escapeHtml(t))).join('');
        }

        // BSim feature hashes of a syntax list; the other pane's set decides shared (green) or unique (red).
        _hashSet(syntax) {
            return new Set((syntax || []).flatMap(e => e[5] || []));
        }

        _codeHtml(syntax, runs, cls, j, other) {
            if (!syntax) return this._runs(runs, cls);
            const plain = syntax.map(([text]) => text).join('');
            if (runs && plain !== runs.map(([text]) => text).join('')) return this._runs(runs, cls);
            const ends = [];
            let end = 0;
            for (const [text, changed] of (runs || [])) ends.push({ end: end += text.length, changed });
            let pos = 0, ri = 0, html = '';
            const origin = j != null && this.fids[j] ? parts(this.fids[j]) : null;
            for (const [token, type, addr, name, external, hashes] of syntax) {
                const callId = type === 'func_call' && addr && !external && origin
                    ? `${origin.coll}:func:${origin.md5}:${addr}` : null;
                const callAttrs = type === 'func_call' ? (callId
                    ? ` class="func-call-clickable" data-xcall="${escapeAttr(callId)}" data-xcall-name="${escapeAttr(name || token)}" role="link" tabindex="0"`
                    : ` title="${escapeAttr(`${external ? 'External' : 'Function'} call: ${name || token}`)}"`) : '';
                let offset = 0;
                while (offset < token.length) {
                    while (ends[ri] && pos >= ends[ri].end) ri++;
                    const length = Math.min(token.length - offset, ends[ri] ? ends[ri].end - pos : token.length);
                    const part = escapeHtml(token.slice(offset, offset + length));
                    let colored = ['variable', 'func_call', 'type', 'op', 'field'].includes(type)
                        ? `<span class="token-${type}"${callAttrs}>${part}</span>` : part;
                    if (hashes && !/^\s*$/.test(part)) {
                        const shared = other ? hashes.some(h => other.has(h)) : null;
                        const hs = hashes.map(safeCssClassPart);
                        colored = `<span class="token feature-highlight ${shared === null ? '' : shared ? 'diff-match' : 'diff-unique'} ${hs.map(h => 'feat-' + h).join(' ')}" data-hashes="${hs.join(' ')}" title="${hashes.length} BSim feature${hashes.length > 1 ? 's' : ''}${shared === null ? '' : shared ? ', also in the other function' : ', not in the other function'}">${colored}</span>`;
                    }
                    html += ends[ri]?.changed ? `<b class="${cls}">${colored}</b>` : colored;
                    offset += length;
                    pos += length;
                }
            }
            return html;
        }

        _cell(b, j) { return b.cells[String(j)] || { st: 'same', sim: 1, segs: [] }; }

        _fseg(j, k) { return (this.data.consensus.funcs[String(j)] || [])[k]; }

        // The text function j holds for reference block b, as token runs.
        _cellRuns(b, j) {
            const c = this._cell(b, j);
            return c.segs.flatMap((k, i) => {
                const seg = this._fseg(j, k);
                return seg ? (i ? [['\n', false], ...seg.text] : seg.text) : [];
            });
        }

        // Leading spaces shared by every non-blank line of `texts`.
        _indent(texts) {
            let n = Infinity;
            for (const t of texts) for (const l of t.split('\n')) if (l.trim()) n = Math.min(n, l.length - l.trimStart().length);
            return n === Infinity ? 0 : n;
        }

        // `list` (syntax tokens or diff runs) with `n` leading spaces removed from every line.
        _trim(list, n) {
            if (!n || !list) return list;
            let bol = true, k = 0;
            return list.map(it => {
                let t = '';
                for (const ch of it[0]) {
                    if (bol && ch === ' ' && k < n) { k++; continue; }
                    t += ch; bol = ch === '\n'; k = 0;
                }
                return [t, ...it.slice(1)];
            });
        }

        _cellSyntax(b, j) {
            return this._cell(b, j).segs.flatMap((k, i) => {
                const seg = this._fseg(j, k);
                return seg ? (i ? [['\n', null], ...(seg.syntax || [])] : (seg.syntax || [])) : [];
            });
        }

        _group(b, j) { return b.groups.find(g => g.funcs.includes(j)); }

        _glyph(st, sim, miss = true) {
            const G = NwayFunction.GLYPH;
            return `${G[st]}${st === 'same' || (st === 'miss' && miss) ? '' : ' ' + Math.round(sim * 100)}`;
        }

        _others() { return this.data.functions.map((f, j) => j).filter(j => j !== this.data.consensus.reference); }

        // The function standing for group `g` in the grid: the selected one, else the member picked from its header.
        _rep(g) {
            return g.includes(this.s.fn) ? this.s.fn : (g.includes(this.s.pick[g[0]]) ? this.s.pick[g[0]] : g[0]);
        }

        // The text function j holds for block b, as plain code.
        _cellText(b, j) {
            const syn = this._cellSyntax(b, j);
            return (syn.length ? syn : this._cellRuns(b, j)).map(e => e[0]).join('');
        }

        // How j differs from x in block b (cached per load).
        _pd(b, x, j) {
            const k = `${b.id}|${x}|${j}`;
            if (!this._pc.has(k)) this._pc.set(k, tokDiff(this._cellText(b, x), this._cellText(b, j)));
            return this._pc.get(k);
        }

        // The widest difference between a same-code group's first function and the others, over every block.
        _groupRel(g) {
            if (g.length < 2) return null;
            const k = 'g|' + g.join(',');
            if (!this._pc.has(k)) {
                let w = 'eq';
                for (const j of g.slice(1)) for (const b of this.data.consensus.blocks) { const r = this._pd(b, g[0], j).rel; if (RANK[r] > RANK[w]) w = r; }
                this._pc.set(k, w);
            }
            return this._pc.get(k);
        }

        _relTag(rel) {
            return rel ? `<span class="xp-rel xp-r-${rel}" title="${escapeAttr(REL_TIP[rel])}">${REL[rel]}</span>` : '';
        }

        // The lines of a marked text that carry a mark, underlined.
        _markLines(marks) {
            const out = [];
            let cur = '', hit = false;
            for (const [t, c] of marks || []) for (const p of t.split(/(\n)/)) {
                if (p === '\n') { if (hit) out.push(cur); cur = ''; hit = false; } else if (p) { cur += c ? `<u class="xp-u-${c}">${escapeHtml(p)}</u>` : escapeHtml(p); hit = hit || !!c; }
            }
            if (hit) out.push(cur);
            return out.join('\n');
        }

        // Per same-code group: what its members differ in, block by block.
        _audit(fgroups) {
            const d = this.data, bl = d.consensus.blocks, fn = d.functions;
            const cards = fgroups.filter(g => g.length > 1).map(g => {
                const body = g.slice(1).map(j => {
                    const diffs = bl.map(b => ({ b, r: this._pd(b, g[0], j) })).filter(x => x.r.rel !== 'eq');
                    const c = { noise: 0, const: 0, chg: 0 };
                    diffs.forEach(({ r }) => { for (const k in c) c[k] += r.counts[k]; });
                    const sum = diffs.length ? `${c.noise} name token${c.noise === 1 ? '' : 's'}, ${c.const} constant${c.const === 1 ? '' : 's'}, ${c.chg} other, in ${diffs.length} block${diffs.length === 1 ? '' : 's'}` : 'text identical';
                    const rows = diffs.slice(0, 30).map(({ b, r }) => `<div class="xp-adr"><span class="xp-aat">${escapeHtml(b.id)}</span><pre class="xp-fcode">${this._markLines(r.ma)}</pre><pre class="xp-fcode">${this._markLines(r.mb)}</pre></div>`).join('');
                    return `<div class="xp-asum">${this._funcRef(j)} against ${this._funcRef(g[0])}: ${sum}.</div>${rows}${diffs.length > 30 ? `<p class="xp-hint">+ ${diffs.length - 30} more blocks</p>` : ''}`;
                }).join('');
                const open = ['const', 'chg'].includes(this._groupRel(g));
                return `<details class="xp-acard"${open ? ' open' : ''}><summary>${g.map(j => this._funcRef(j, 'xp-badge', false)).join('')} ${this._relTag(this._groupRel(g))}<span class="dim">${g.map(j => escapeHtml(fn[j].file_name || fn[j].addr)).join(', ')}</span></summary><div class="xp-abody"><p class="xp-hint" style="margin:0">Each member is compared with ${escapeHtml(fn[g[0]].letter)}, the first of the group. Left: ${escapeHtml(fn[g[0]].letter)}; right: the member. Only lines with a difference are listed.</p>${body}</div></details>`;
            });
            return `<div class="xp-audit">${cards.join('')}</div>`;
        }

        // The same-code audit control: a pill and its panel; empty when no functions share code.
        _auditBar(fgroups) {
            const groups = fgroups.filter(g => g.length > 1);
            if (!groups.length) return { pill: '', panel: '' };
            const look = groups.filter(g => ['const', 'chg'].includes(this._groupRel(g))).length;
            const pill = `<button class="xp-pill${look ? ' look' : ''}" data-xaudit aria-expanded="${!!this.audit}" title="Same code means the same after ignoring names and constants. This shows what each group ignores, member by member">${groups.length} same-code group${groups.length === 1 ? '' : 's'} &middot; ${look ? `${look} need${look === 1 ? 's' : ''} a look` : 'names only or identical'}</button>`;
            return { pill, panel: this.audit ? this._audit(fgroups) : '' };
        }

        _isSame(b) { return Object.values(b.cells).every(c => c.st === 'same'); }

        // Where the blocks come from: Ghidra basic blocks where stored with text splitting elsewhere (hybrid), Ghidra only, or text only.
        _bmToggle() {
            const d = this.data, bm = this.s.bm || 'hybrid', have = (d.cfg_funcs || []).filter(Boolean).length, all = (d.cfg_funcs || []).length;
            const b = (k, label, why) => `<button class="view-btn${bm === k ? ' active' : ''}" data-xbm="${k}" title="${escapeAttr(why)}">${label}</button>`;
            return `<div class="view-toggle" role="group" aria-label="Block source">${b('hybrid', 'Hybrid', 'Ghidra basic blocks where stored, text splitting for the rest')}${b('cfg', 'Ghidra blocks', 'Ghidra basic blocks only')}${b('text', 'Text', 'Blocks cut from the decompiled text only')}</div>`
                + (all && bm === 'hybrid' && have < all ? `<span class="xp-tag" title="Functions analysed before the CFG was stored use text blocks">${have} of ${all} with Ghidra blocks</span>` : '');
        }

        _code() {
            const s = this.s;
            if (!this.data.consensus && this.data.blocks_error) {
                const miss = this.data.blocks_error.missing || [];
                return `<div class="xp-cbar">${this._bmToggle()}</div><div class="xp-none">Ghidra blocks are not stored for ${miss.map(j => this._funcRef(j)).join(' ')}: analysed before the CFG was stored. Re-analyse them, or use Hybrid or Text blocks.</div>`;
            }
            const audit = s.tab === 'code' ? this._auditBar(this._functionGroups(false)) : { pill: '', panel: '' };
            const sw = `<div class="xp-cbar">${this._bmToggle()}
${this._info(s.tab === 'code' ? 'Overview of what changed, block by block.' : s.tab === 'unified' ? 'The reference function once. A block that differs in the selected functions expands to its variants.' : 'Read the whole function, aligned to the reference by block.')}
                <button class="xp-aibtn" data-xask title="Open AI Insight and ask it about this whole row"><i class="fa-solid fa-robot"></i> Ask AI about this row</button>
                <span class="xp-naming" ${this._naming ? '' : 'hidden'}><i class="fa-solid fa-spinner fa-spin"></i> naming blocks&hellip;</span>${audit.pill}</div>${audit.panel}`;
            return sw + (s.tab === 'code' ? this._blocksView() : s.tab === 'unified' ? this._unifiedView() : this._fullView());
        }

        _extraSlots(functions, unique = false) {
            const c = this.data.consensus, slots = new Map(), seen = new Set();
            // A shared extra uses its first owner's position in the overview.
            for (const f of functions) {
                let after = null;
                for (const seg of c.funcs[String(f)] || []) {
                    if (seg.refs.length) after = seg.refs[seg.refs.length - 1].b;
                    else if (seg.x && (!unique || !seen.has(seg.x))) {
                        seen.add(seg.x);
                        if (!slots.has(after)) slots.set(after, []);
                        slots.get(after).push({ seg, f, x: c.extras.find(e => e.id === seg.x) });
                    }
                }
            }
            return slots;
        }

        _ribbon(items) {
            const d = this.data, c = d.consensus;
            let h = '<div class="xp-rib"><table><tr><th class="rowh">Block in ' + this._funcRef(c.reference) + '</th>'
                + items.map(({ b, x }) => b
                    ? `<th>${escapeHtml(b.id)}<small data-btitle="${escapeAttr(b.id)}" title="${escapeAttr(this._bt(b))}">${escapeHtml(this._bt(b))}</small></th>`
                    : `<th>${escapeHtml(x.id)}<small title="${escapeAttr(x.title)}">${escapeHtml(x.title)}</small></th>`).join('') + '</tr>';
            d.functions.forEach((f, j) => {
                h += `<tr><td class="rowh" ${cc(j)}>${this._funcRef(j)} ${this._fileRef(j, 'xp-rfile')}</td>`;
                for (const { b, x } of items) {
                    if (x) {
                        h += x.funcs.includes(j) ? `<td><button class="xp-cell st-extra" data-xx="${escapeAttr(x.id)}" title="only in this function">+</button></td>` : '<td><span class="st-none">&ndash;</span></td>';
                        continue;
                    }
                    if (j === c.reference) { h += '<td><span class="st-none">ref</span></td>'; continue; }
                    const cell = this._cell(b, j);
                    const gsel = b.groups[this.s.open[b.id]];
                    const sel = !!gsel && gsel.funcs.includes(j);
                    h += `<td><button class="xp-cell st-${cell.st}${sel ? ' sel' : ''}" data-xb="${escapeAttr(b.id)}" data-xc="${j}" title="${NwayFunction.SNAME[cell.st]}">${this._glyph(cell.st, cell.sim)}</button></td>`;
                }
                h += '</tr>';
            });
            return h + '</table></div>';
        }

        // Ghidra basic blocks of a block, e.g. "BB 1 + 2"; absent for collections analysed before the CFG was stored.
        _bbTag(bb, hashes) {
            const n = hashes ? ` · ${Number(hashes)} hashes` : '';
            return bb && bb.length ? `<span class="xp-tag" title="Ghidra basic blocks inside this block${hashes ? ' · distinct BSim feature hashes' : ''}">BB ${bb.map(Number).join(' + ')}${n}</span>` : '';
        }

        // Reference CFG left, one function's CFG right; matched blocks are linked and coloured by state.
        _cfgPair(j, c) {
            const R = c.cfg[String(c.reference)], F = c.cfg[String(j)];
            if (R.nodes.length > 300 || F.nodes.length > 300) return '';
            const entries = c.funcs[String(j)] || [], pairOf = new Map(), taken = new Set();
            F.nodes.forEach((nd, k) => {
                const ref = entries[k]?.refs?.[0], i = ref ? c.blocks.findIndex(b => b.id === ref.b) : -1;
                if (i >= 0 && !taken.has(i)) { taken.add(i); pairOf.set(k, i); }
            });
            const g = CfgLayout.grid(R, F, pairOf), NW = 44, NH = 22, ROW = 46, gap = 50;
            const off = g.L.w + gap, W = off + g.M.w, H = g.rows * ROW;
            // Shrink to the panel width so both shapes are visible together; the hover preview carries the detail.
            const sc = Math.max(0.4, Math.min(1, ((this.el.clientWidth || 1200) - 40) / W));
            const cx = (side, id) => (side ? off + g.M.x.get(id) : g.L.x.get(id));
            const at = (side, id) => ({ x: cx(side, id) - NW / 2, y: g.row.get(id) * ROW });
            const edge = (side, u, v) => {
                const key = u + '|' + v, G = side ? g.M : g.L;
                if (g.back.has(key)) { const a = at(side, u), b = at(side, v), s = 26; return `<path class="e back" d="M${a.x + NW},${a.y + NH / 2} C${a.x + NW + s},${a.y + NH / 2} ${b.x + NW + s},${b.y + NH / 2} ${b.x + NW},${b.y + NH / 2}"/>`; }
                const pts = [{ x: cx(side, u), y: g.row.get(u) * ROW + NH }]
                    .concat((G.chain.get(key) || []).map(d => ({ x: (side ? off : 0) + G.x.get(d), y: G.rowOf.get(d) * ROW + NH / 2 })))
                    .concat([{ x: cx(side, v), y: g.row.get(v) * ROW }]);
                return `<path class="e" d="${CfgLayout.path(pts)}"/>`;
            };
            let svg = '';
            for (const [u, v] of R.edges) svg += edge(0, 'r' + u, 'r' + v);
            for (const [u, v] of F.edges) svg += edge(1, g.fid(u), g.fid(v));
            for (const [k, i] of pairOf) {
                const a = at(0, 'r' + i), b = at(1, 'r' + i);
                svg += `<path class="m" d="M${a.x + NW},${a.y + NH / 2} L${b.x},${b.y + NH / 2}"/>`;
            }
            // Every reference block each function block holds code of (merged and split cases), primary pair first.
            const relF = new Map(), relR = new Map();
            entries.forEach((ent, k) => (ent.refs || []).forEach(r => {
                const i = c.blocks.findIndex(b => b.id === r.b);
                if (i < 0 || k >= F.nodes.length) return;
                const front = (m, a, v) => { const l = m.get(a) || m.set(a, []).get(a); if (pairOf.get(k) === i) l.unshift(v); else l.push(v); };
                front(relF, k, `r:${i}`);
                front(relR, i, `f:${j}:${k}`);
                if (pairOf.get(k) !== i) {
                    const a = at(0, 'r' + i), b = at(1, g.fid(k)), h = (b.x - a.x - NW) / 2;
                    svg += `<path class="m x" d="M${a.x + NW},${a.y + NH / 2} C${a.x + NW + h},${a.y + NH / 2} ${b.x - h},${b.y + NH / 2} ${b.x},${b.y + NH / 2}"/>`;
                }
            }));
            let nodes = '';
            R.nodes.forEach((nd, i) => {
                const b = c.blocks[i], st = this._cell(b, j).st, p = at(0, 'r' + i);
                nodes += `<button class="xp-cell xp-cfn st-${st}" style="left:${p.x}px;top:${p.y}px" data-xb="${escapeAttr(b.id)}" data-xc="${j}" data-cfn="r:${i}"${relR.has(i) ? ` data-cfp="${relR.get(i).join(' ')}"` : ''}>${escapeHtml(b.id)}</button>`;
            });
            F.nodes.forEach((nd, k) => {
                const ent = entries[k], ref = ent?.refs?.[0], p = at(1, g.fid(k)), st = ref ? ref.st : 'extra';
                const attrs = ref ? `data-xb="${escapeAttr(ref.b)}" data-xc="${j}"` : ent?.x ? `data-xx="${escapeAttr(ent.x)}"` : 'disabled';
                nodes += `<button class="xp-cell xp-cfn st-${st}" style="left:${p.x}px;top:${p.y}px" ${attrs} data-cfn="f:${j}:${k}"${relF.has(k) ? ` data-cfp="${relF.get(k).join(' ')}"` : ''}>${ref ? escapeHtml(ref.b) : '+'}</button>`;
            });
            return `<div class="xp-cfgp"><div class="xp-ph">${this._funcRef(c.reference)} reference (${R.nodes.length} blocks) &harr; ${this._funcRef(j)} ${escapeHtml(this.data.functions[j].file_name || '')} (${F.nodes.length} blocks)</div>
                <div class="xp-cfgw" style="width:${Math.round(W * sc)}px;height:${Math.round(H * sc)}px"><div class="xp-cfg" style="width:${W}px;height:${H}px;transform:scale(${sc})"><svg width="${W}" height="${H}">${svg}</svg>${nodes}</div></div></div>`;
        }

        // Code of a graph node, for the hover preview: [heading, html].
        _cfgSnip(key) {
            const c = this.data.consensus, [side, a, b] = key.split(':');
            if (side === 'r') {
                const blk = c.blocks[Number(a)];
                return [`${blk.id} reference`, escapeHtml(blk.ref_text)];
            }
            const ent = (c.funcs[a] || [])[Number(b)], ref = ent?.refs?.[0];
            if (!ent) return null;
            return [ref ? `${ref.b} · ${NwayFunction.SNAME[ref.st]} ${Math.round(ref.sim * 100)}%` : 'no match in the reference', this._runs(ent.text, 'chg')];
        }

        _cfgHover(e, on) {
            const n = e.target.closest('[data-cfn]');
            if (!n || !this.el.contains(n) || n.contains(e.relatedTarget)) return;
            this.el.querySelectorAll('.xp-cfpair').forEach(x => x.classList.remove('xp-cfpair'));
            document.querySelectorAll('.xp-cfgtip').forEach(x => x.remove());
            if (!on) return;
            const key = n.dataset.cfn, rel = (n.dataset.cfp || '').split(' ').filter(Boolean), other = rel[0] || null, side = key[0];
            const mine = this._cfgSnip(key), theirs = other && this._cfgSnip(other);
            if (!mine) return;
            n.classList.add('xp-cfpair');
            rel.forEach(o => n.closest('.xp-cfg').querySelector(`[data-cfn="${o}"]`)?.classList.add('xp-cfpair'));
            const pane = ([h, code]) => `<div class="xp-cfgpane"><div class="xp-ph">${escapeHtml(h)}</div><pre>${code}</pre></div>`;
            const tip = document.createElement('div');
            tip.className = 'xp-cfgtip';
            tip.innerHTML = (side === 'r' || !theirs ? pane(mine) + (theirs ? pane(theirs) : '') : pane(theirs) + pane(mine));
            document.body.appendChild(tip);
            const r = tip.getBoundingClientRect();
            tip.style.left = Math.max(4, Math.min(e.clientX + 14, innerWidth - r.width - 8)) + 'px';
            tip.style.top = Math.max(4, Math.min(e.clientY + 14, innerHeight - r.height - 8)) + 'px';
        }

        _cfgStrip() {
            const c = this.data.consensus, cfg = c.cfg || {};
            if (!cfg[String(c.reference)]) return '';
            const js = this._others().filter(j => cfg[String(j)]);
            if (!js.length) return '';
            return `<div class="xp-cfgs"><div class="xp-cbar" style="margin-bottom:6px"><span class="xp-hint" style="margin:0">Control-flow graphs</span>${this._info('Basic blocks from Ghidra, merged along single edges. Matched blocks are linked and coloured like the table below; + marks a block with no counterpart. Click a block to jump to its code.')}</div>${js.map(j => this._cfgPair(j, c)).join('')}</div>`;
        }

        _blockBody(b) {
            const d = this.data, c = d.consensus;
            if (!b.groups.length) return `<div class="xp-bd"><p class="xp-hint" style="margin:0">Same code in all ${d.functions.length} functions.</p><div class="xp-panes"><div class="xp-pane"><div class="xp-ph">${this._funcRef(c.reference)} all functions</div><pre>${this._codeHtml(b.ref_syntax, null, null, c.reference)}</pre></div></div></div>`;
            const gi = b.groups[this.s.open[b.id]] ? this.s.open[b.id] : 0, g = b.groups[gi], G = NwayFunction.GLYPH;
            const badges = (fs, link = true) => fs.map(j => this._funcRef(j, 'xp-badge', link)).join('');
            const chips = b.groups.map((x, i) => `<button class="xp-chip" aria-pressed="${i === gi}" data-xb="${escapeAttr(b.id)}" data-xg="${i}">${badges(x.funcs, false)} ${G[x.st]} ${NwayFunction.SNAME[x.st]}</button>`).join('');
            const same = this._others().filter(j => this._cell(b, j).st === 'same');
            const j0 = g.funcs[0], cell = this._cell(b, j0);
            const runs = this._cellRuns(b, j0);
            const sim = g.funcs.map(j => Math.round(g.sims[String(j)] * 100)).join('/');
            return `<div class="xp-bd"><div class="xp-vchips"><span class="lab">Compare ${this._funcRef(c.reference)} with</span>${chips}${same.length ? `<span class="lab">${same.map(j => this._funcRef(j)).join(', ')}: same as ${this._funcRef(c.reference)}</span>` : ''}</div>
                <div class="xp-panes">
                  <div class="xp-pane"><div class="xp-ph">${this._funcRef(c.reference)} reference ${this._bbTag(b.bb, b.hashes)}</div><pre data-xfns="${c.reference},${j0}">${this._codeHtml(b.ref_syntax, g.ref_segs, 'del', c.reference, this._hashSet(this._cellSyntax(b, j0)))}</pre></div>
                  <div class="xp-pane"><div class="xp-ph">${badges(g.funcs)} <span class="xp-cell st-${g.st}" style="cursor:default">${G[g.st]} ${NwayFunction.SNAME[g.st]}${g.st === 'miss' ? '' : ' ' + sim + '%'}</span>${cell.into ? `<span class="xp-tag hot">merged into ${escapeHtml(cell.into)}</span>` : ''}${this._bbTag(cell.segs.flatMap(k => this._fseg(j0, k)?.bb || []))}${this._expl(b.id, j0)}</div>
                    <pre data-xfns="${j0},${c.reference}">${runs.length ? this._codeHtml(this._cellSyntax(b, j0), runs, 'chg', j0, this._hashSet(b.ref_syntax)) : '(no matching code)'}</pre>${g.note ? `<div class="xp-pnote">${escapeHtml(g.note)}</div>` : ''}</div>
                </div></div>`;
        }

        _blocksView() {
            const c = this.data.consensus, only = this.s.only;
            const slots = this._extraSlots(this._others(), true), ordered = [];
            const addExtras = after => (slots.get(after) || []).forEach(({ x }) => { if (x) ordered.push({ x }); });
            addExtras(null);
            for (const b of c.blocks) {
                if (!only || !this._isSame(b)) ordered.push({ b });
                addExtras(b.id);
            }
            const G = NwayFunction.GLYPH;
            const head = '';
            const legend = `<div class="xp-legend">${Object.keys(NwayFunction.SNAME).map(k => `<span><span class="xp-cell st-${k}" style="min-width:0;height:18px;cursor:default">${G[k]}</span>${NwayFunction.SNAME[k]}</span>`).join('')}${this._info('When decompiled code is laid out differently, lines do not match one to one. Functions are cut into blocks (loops, branches, straight runs) and each block is matched by its content, in any order. Each cell shows how that block looks in the other function: how similar it is, or that it moved, was merged into or split from another block, or is gone. Click a cell or a block to compare it with the reference. "Same code" means the same after ignoring names and constants; the same-code pill shows what that ignores.')}${c.truncated ? this._info(`Large functions are compared on their first ${Number(this.data.limits.lines)} lines and ${Number(this.data.limits.blocks)} blocks.`) : ''}</div>`;
            const accordion = ordered.map(({ b, x }) => {
                if (x) {
                    const open = x.id in this.s.open;
                    return `<div class="xp-blk wst-extra"><button class="xp-bh" data-xb="${escapeAttr(x.id)}" aria-expanded="${open}"><span class="xp-bid">${escapeHtml(x.id)}</span><span class="bt">${escapeHtml(x.title)}</span><span class="xp-sum st-extra">+ ${x.funcs.length} of ${this.data.functions.length} only</span></button>${open ? `<div class="xp-bd"><div class="xp-panes"><div class="xp-pane"><div class="xp-ph">${x.funcs.map(j => `${this._funcRef(j)}`).join('')} <span class="xp-tag">no counterpart in the reference</span></div><pre>${x.syntax ? this._codeHtml(x.syntax, null, null, x.funcs[0]) : escapeHtml(x.text)}</pre></div></div></div>` : ''}</div>`;
                }
                const open = b.id in this.s.open;
                const sts = this._others().map(j => this._cell(b, j)), G2 = NwayFunction.GLYPH;
                const worst = ['miss', 'restr', 'mod', 'moved'].find(k => sts.some(x => x.st === k)) || 'same';
                const nSame = sts.filter(x => x.st === 'same').length;
                const sims = sts.filter(x => x.st === 'mod' || x.st === 'moved').map(x => x.sim);
                const verdict = `<span class="xp-sum st-${worst}" title="${escapeAttr(Object.keys(NwayFunction.SNAME).filter(k => k !== 'same' && sts.some(x => x.st === k)).map(k => `${sts.filter(x => x.st === k).length} ${NwayFunction.SNAME[k]}`).join(', ') || 'same code in all')}">${G2[worst]} ${worst === 'same' ? `same in all ${sts.length}` : `${sts.length - nSame} of ${sts.length} differ`}</span>${sims.length ? `<span class="xp-min" title="lowest similarity of the modified or moved functions">${Math.round(Math.min(...sims) * 100)}%<small> min</small></span>` : ''}`;
                return `<div class="xp-blk wst-${worst}"><button class="xp-bh" data-xb="${escapeAttr(b.id)}" aria-expanded="${open}"><span class="xp-bid">${escapeHtml(b.id)}</span>${this._tspan(b)}${verdict}</button>${open ? this._blockBody(b) : ''}</div>`;
            }).join('');
            const hidden = only ? c.blocks.length - ordered.filter(item => item.b).length : 0;
            const cfg = this._cfgStrip();
            return head + this._ribbon(ordered) + (cfg ? `<details class="xp-cfgd"><summary>Control-flow graphs</summary>${cfg}</details>` : '') + legend + accordion
                + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} with the same code in all ${this.data.functions.length} functions hidden.</p>` : '');
        }

        _band(id, st, sim, title, extra = '', tail = '') {
            return `<div class="xp-fband"><span class="xp-bid">${escapeHtml(id)}</span><span class="xp-cell st-${st}" style="cursor:default">${NwayFunction.GLYPH[st]} ${NwayFunction.SNAME[st]}${st === 'miss' || st === 'same' || st === 'extra' ? '' : ' ' + Math.round(sim * 100) + '%'}</span>${extra}<span class="dim">${escapeHtml(title)}</span><i></i>${tail}</div>`;
        }

        // The groups of functions sharing a variant of block `b`; the one holding `f` is pressed.
        _fullGroups(b, f) {
            const same = this._others().filter(j => this._cell(b, j).st === 'same');
            const groups = [...(same.length ? [{ funcs: same, st: 'same' }] : []), ...b.groups];
            return { groups, html: `<div class="xp-vchips"><span class="lab">Variants</span>${groups.map(g => `<button class="xp-chip" aria-pressed="${g.funcs.includes(f)}" data-xfn="${g.funcs.includes(f) ? f : g.funcs[0]}">${g.funcs.map(j => this._funcRef(j, 'xp-badge', false)).join('')} ${NwayFunction.GLYPH[g.st]} ${NwayFunction.SNAME[g.st]}</button>`).join('')}</div>` };
        }

        _functionGroups(single) {
            const c = this.data.consensus, groups = new Map();
            this.data.functions.forEach((fn, j) => {
                if (j === c.reference && !single) return;
                // The matcher's own key: build noise (locals, types, generated names) is already removed.
                const key = c.keys && c.keys[j] && !c.truncated ? c.keys[j] : j;
                if (!groups.has(key)) groups.set(key, []);
                groups.get(key).push(j);
            });
            return [...groups.values()];
        }

        // Most severe state of block `b` over the functions `fs`.
        _worst(b, fs) {
            return ['miss', 'restr', 'mod', 'moved'].find(k => fs.some(j => this._cell(b, j).st === k)) || 'same';
        }

        // Block metadata column: id, state, per-column dots; variants, title and Explain open on hover or focus.
        _gutter(b, fs, trimmed = 0) {
            const G = NwayFunction.GLYPH, S = NwayFunction.SNAME, w = this._worst(b, fs), fnames = this.data.functions;
            const sts = fs.map(j => this._cell(b, j));
            const marks = fs.length > 1
                ? `<span class="xp-dots">${fs.map((j, i) => `<i class="st-${sts[i].st}" title="${escapeAttr(`${fnames[j].letter}: ${S[sts[i].st]}`)}"></i>`).join('')}</span>`
                : (sts[0].st === 'mod' || sts[0].st === 'moved' ? `<span>${Math.round(sts[0].sim * 100)}%</span>` : '');
            const { groups, html } = this._fullGroups(b, fs[0]);
            const expl = fs.map(j => ({ j, e: this._expl(b.id, j) })).filter(x => x.e);
            const more = `<div class="xp-more"><b>${this._tspan(b)}</b>${groups.length > 1 ? html : ''}${expl.map(({ j, e }) => `<div class="xp-vchips">${this._funcRef(j)} ${e}</div>`).join('')}</div>`;
            return `<div class="xp-gut" tabindex="0"><span class="xp-gid">${escapeHtml(b.id)}</span><span class="xp-gl st-${w}">${G[w]}</span>${marks}${trimmed ? `<span class="xp-gvar" title="${trimmed} leading spaces shared by every column of this block are hidden">&#8676;${trimmed}</span>` : ''}${groups.length > 1 ? `<span class="xp-gvar">${groups.length} var.</span>` : ''}${more}</div>`;
        }

        _fullView() {
            const d = this.data, c = d.consensus, s = this.s, single = s.single, nf = d.functions.length;
            if (s.fn == null || (!single && s.fn === c.reference)) s.fn = this._others()[0];
            const f = s.fn, ref = c.reference, rl = d.functions[ref].letter;
            const fgroups = this._functionGroups(single);
            const chips = fgroups.map(g => `<button class="xp-chip" aria-pressed="${g.includes(f)}" data-xfn="${this._rep(g)}" title="${escapeAttr(g.map(j => `${d.functions[j].letter}: ${d.functions[j].file_name || ''}`).join('\n'))}">${g.map(j => this._funcRef(j, 'xp-badge', false)).join('')}${g.length > 1 ? 'same code' : this._fileRef(g[0], 'xp-rfile', false)}</button>`).join('');
            const ncols = Math.min(5, nf, single ? nf : fgroups.length + 1);
            const colsCtl = !single && ncols > 2 ? `<div class="view-toggle" role="group" aria-label="Columns">${Array.from({ length: ncols - 1 }, (_, i) => i + 2).map(n => `<button class="view-btn${Math.min(s.cols, ncols) === n ? ' active' : ''}" data-xcols="${n}" title="${n} columns: ${escapeAttr(rl)} and ${n - 1} variant${n > 2 ? 's' : ''}">${n}</button>`).join('')}</div>` : '';
            const why = single ? `Whole function ${d.functions[f].letter}, blocks labelled with their match in the reference.`
                : `Rows are blocks of ${rl}, the first column. The other columns are the selected variants, side by side. Functions sharing a block variant are grouped together; hover a block's gutter to see its variants and pick one to show first. Blocks that moved or merged stay on the row of the block they match. Under a column header, the functions listed as the same code can be shown in that column instead.`;
            const { pill, panel } = single ? { pill: '', panel: '' } : this._auditBar(fgroups);
            const out = `<div class="xp-cbar"><span class="xp-hint" style="margin:0">Show</span>${chips}${colsCtl}<label class="xp-chk"><input type="checkbox" data-xsingle ${single ? 'checked' : ''}> Function alone, in its own order</label>${single ? '' : `<label class="xp-chk" title="Hide the indentation every column of a block has in common, so deep code needs less sideways scrolling"><input type="checkbox" data-xdedent ${s.dedent ? 'checked' : ''}> Trim shared indent</label>`}${pill}${this._info(why)}</div>${panel}`;
            if (single) {
                if (f === ref) return out + c.blocks.map(b => `<div class="xp-frow one">${this._band(b.id, 'same', 1, this._bt(b))}<pre class="xp-fcode">${this._codeHtml(b.ref_syntax, null, null, ref)}</pre></div>`).join('');
                const segs = c.funcs[String(f)] || [];
                return out + segs.map(seg => {
                    const r = seg.refs[0], b = r && c.blocks.find(x => x.id === r.b);
                    const band = r ? this._band(seg.refs.map(x => x.b).join(' + '), r.st, r.sim, this._bt(b)) : seg.x ? this._band(seg.x, 'extra', 1, (c.extras.find(x => x.id === seg.x) || {}).title || '') : this._band('-', 'extra', 1, 'unmatched');
                    return `<div class="xp-frow one">${band}<pre class="xp-fcode st-${r ? r.st : 'extra'}">${this._codeHtml(seg.syntax, seg.text, 'chg', f)}</pre></div>`;
                }).join('');
            }
            // One representative per group of same-code functions; the selected one stands for its group.
            const reps = fgroups.map(g => this._rep(g));
            const fs = [f, ...reps.filter(j => j !== f)].slice(0, Math.min(s.cols, ncols) - 1);
            const G = NwayFunction.GLYPH, S = NwayFunction.SNAME, n = fs.length;
            this.unfold = this.unfold || new Set();
            const rail = [];
            const gap = text => `<pre class="xp-fcode gap">${text}</pre>`;
            const row = b => {
                const w = this._worst(b, fs), g = this._group(b, fs[0]);
                rail.push({ id: b.id, w, n: (b.ref_text || '').split('\n').length });
                const cs = fs.map(j => ({ j, cell: this._cell(b, j), syn: this._cellSyntax(b, j), runs: this._cellRuns(b, j) }));
                const txt = (syn, runs) => (syn || runs || []).map(e => e[0]).join('');
                // Indent shared by the whole row is hidden, so deep blocks need no sideways scroll.
                const ind = !s.dedent ? 0 : this._indent([txt(b.ref_syntax, g && g.ref_segs), ...cs.filter(x => x.cell.st !== 'miss' && !x.cell.into).map(x => txt(x.syn, x.runs))]);
                const cells = cs.map(({ j, cell, syn, runs }) => {
                    if (cell.st === 'miss') return gap('(no matching code)');
                    if (cell.into) return gap(`↑ merged into ${escapeHtml(cell.into)} above`);
                    const tip = `${d.functions[j].letter}: ${S[cell.st]}${cell.st === 'same' ? '' : ' ' + Math.round(cell.sim * 100) + '%'}`;
                    return `<pre class="xp-fcode st-${cell.st}" title="${escapeAttr(tip)}" data-xfns="${j},${ref}">${this._codeHtml(this._trim(syn, ind), this._trim(runs, ind), 'chg', j, this._hashSet(b.ref_syntax))}</pre>`;
                }).join('');
                return `<div class="xp-frow xp-g wst-${w}" data-xrow="${escapeAttr(b.id)}" style="--n:${n}">${this._gutter(b, fs, ind)}<pre class="xp-fcode" data-xfns="${ref},${fs[0]}">${this._codeHtml(this._trim(b.ref_syntax, ind), this._trim(g && g.ref_segs, ind), 'del', ref, this._hashSet(cs[0].syn))}</pre>${cells}</div>`;
            };
            const slots = this._extraSlots(fs);
            const extraRows = after => (slots.get(after) || []).map(({ seg, x, f: owner }) => {
                rail.push({ id: seg.x, w: 'extra', n: 2 });
                return `<div class="xp-frow xp-g wst-extra" data-xrow="${escapeAttr(seg.x)}" style="--n:${n}"><div class="xp-gut"><span class="xp-gid">${escapeHtml(seg.x)}</span><span class="xp-gl st-extra">${G.extra}</span><div class="xp-more"><b>${escapeHtml(x ? x.title : '')}</b></div></div>${gap(`(no counterpart in ${escapeHtml(rl)})`)}${fs.map(j => j === owner ? `<pre class="xp-fcode st-extra">${this._codeHtml(seg.syntax, seg.text, 'chg', j)}</pre>` : gap('&ndash;')).join('')}</div>`;
            }).join('');
            const head = `<div class="xp-frow xp-g xp-ghead" style="--n:${n}"><div></div><div class="xp-fband">${this._funcRef(ref)} ${this._fileRef(ref, 'xp-rfile')} (reference)</div>${fs.map(j => {
                const g = fgroups.find(x => x.includes(j)) || [], mates = g.filter(x => x !== j);
                const pick = x => (g.includes(f) ? `data-xfn="${x}"` : `data-xtw="${g[0]},${x}"`);
                const btns = mates.map(x => `<button class="xp-twin" ${pick(x)} title="${escapeAttr(`Show ${d.functions[x].letter} in this column instead of ${d.functions[j].letter}`)}">${this._funcRef(x, 'xp-badge', false)}${this._fileRef(x, 'xp-rfile', false)}<i>show</i></button>`).join('');
                return `<div class="xp-fband">${this._funcRef(j)} ${this._fileRef(j, 'xp-rfile')}${mates.length ? ` ${this._relTag(this._groupRel(g))}<div class="xp-twins"><span class="dim">same code as</span>${btns}</div>` : ''}</div>`;
            }).join('')}</div>`;
            let body = extraRows(null), run = [];
            const flush = () => {
                if (!run.length) return;
                const key = run[0].id;
                if (this.unfold.has(key)) body += run.map(row).join('');
                else {
                    rail.push({ fold: key, w: 'same', n: 1 });
                    body += `<div class="xp-fold"><button data-xfold="${escapeAttr(key)}">&hellip; ${run.length} same-code block${run.length === 1 ? '' : 's'} (${run.map(b => escapeHtml(b.id)).join(', ')}) &middot; show</button></div>`;
                }
                run = [];
            };
            for (const b of c.blocks) {
                if (s.only && this._worst(b, fs) === 'same') run.push(b);
                else { flush(); body += row(b); }
                const ex = extraRows(b.id);
                if (ex) { flush(); body += ex; }
            }
            flush();
            const railHtml = `<div class="xp-rail">${rail.map(r => `<a class="st-${r.w}" ${r.fold ? `data-xfold="${escapeAttr(r.fold)}"` : `data-xjump="${escapeAttr(r.id)}"`} style="flex:${r.n}" title="${escapeAttr(r.fold ? 'same-code blocks, click to show' : `${r.id}: ${S[r.w]}`)}"></a>`).join('')}<div class="xp-thumb"></div></div>`;
            return out + `<div class="xp-gwrap"><div class="xp-gscroll"><div class="xp-ginner" style="--n:${n}">${head}${body}</div></div>${railHtml}</div>`;
        }

        // One function (the reference), once. A block that differs in the focus group expands to its variants.
        _unifiedView() {
            const d = this.data, c = d.consensus, s = this.s, ref = c.reference, rl = d.functions[ref].letter;
            const fgroups = this._functionGroups(false), on = g => !s.uoff[g[0]], audit = this._auditBar(fgroups);
            const focus = fgroups.filter(on).flat();
            const chips = fgroups.map(g => `<button class="xp-chip" aria-pressed="${on(g)}" data-xug="${g[0]}" title="${escapeAttr(g.map(j => `${d.functions[j].letter}: ${d.functions[j].file_name || ''}`).join('\n'))}">${g.map(j => this._funcRef(j, 'xp-badge', false)).join('')}${g.length > 1 ? 'same code' : this._fileRef(g[0], 'xp-rfile', false)}</button>`).join('');
            const bar = `<div class="xp-cbar"><span class="xp-hint" style="margin:0">Compare ${this._funcRef(ref)} with</span>${chips}${audit.pill}${this._info(`Toggle which functions are compared with ${rl}. Variants and the same-code block count follow the selection.`)}</div>${audit.panel}`;
            const variants = b => b.groups.map(g => ({ ...g, funcs: g.funcs.filter(j => focus.includes(j)) })).filter(g => g.funcs.length);
            const badges = fs => fs.map(j => this._funcRef(j, 'xp-badge', false)).join('');
            const variant = (b, g) => {
                const j0 = g.funcs[0], cell = this._cell(b, j0);
                const sim = g.funcs.map(j => Math.round(g.sims[String(j)] * 100)).join('/');
                const tag = `<span class="xp-cell st-${g.st}" style="cursor:default">${NwayFunction.GLYPH[g.st]} ${NwayFunction.SNAME[g.st]}${g.st === 'miss' ? '' : ' ' + sim + '%'}</span>`;
                const code = g.st === 'miss' ? '(no matching code)' : cell.into ? `↑ merged into ${escapeHtml(cell.into)}` : this._codeHtml(this._cellSyntax(b, j0), this._cellRuns(b, j0), 'chg', j0, this._hashSet(b.ref_syntax));
                return `<div class="xp-fband">${badges(g.funcs)} ${tag}${this._expl(b.id, j0)}</div><pre class="xp-fcode st-${g.st}" data-xfns="${j0},${ref}">${code}</pre>`;
            };
            const rows = [], slots = this._extraSlots(focus, true);
            let hidden = 0;
            const extraRows = after => (slots.get(after) || []).forEach(({ seg, x }) => {
                const own = x ? x.funcs.filter(j => focus.includes(j)) : [];
                rows.push(`<div class="xp-frow one wst-extra" data-xrow="${escapeAttr(seg.x)}">${this._band(seg.x, 'extra', 1, x ? x.title : '', badges(own))}<pre class="xp-fcode st-extra">${this._codeHtml(seg.syntax, seg.text, 'chg', own[0])}</pre></div>`);
            });
            extraRows(null);
            for (const b of c.blocks) {
                const vs = variants(b), w = this._worst(b, focus);
                if (s.only && !vs.length) hidden++;
                else {
                    const sims = focus.map(j => this._cell(b, j)).filter(x => x.st === 'mod' || x.st === 'moved').map(x => x.sim);
                    const same = focus.filter(j => this._cell(b, j).st === 'same');
                    const open = vs.length && s.uopen[b.id];
                    const tail = vs.length ? `<button class="xp-chip" data-xu="${escapeAttr(b.id)}" aria-expanded="${!!open}" title="Show how the selected functions differ in this block">${vs.length} variant${vs.length === 1 ? '' : 's'} ${open ? '▴' : '▾'}</button>` : '';
                    const body = open ? `<div class="xp-uvars">${vs.map(g => variant(b, g)).join('')}${same.length ? `<div class="xp-fband">${badges(same)} same code as ${escapeHtml(rl)}</div>` : ''}</div>` : '';
                    rows.push(`<div class="xp-frow one wst-${w}" data-xrow="${escapeAttr(b.id)}">${this._band(b.id, w, sims.length ? Math.min(...sims) : 1, this._bt(b), '', tail)}<pre class="xp-fcode st-${w}" ${focus.length ? `data-xfns="${ref},${focus[0]}"` : ''}>${this._codeHtml(b.ref_syntax, null, null, ref)}</pre>${body}</div>`);
                }
                extraRows(b.id);
            }
            return bar + `<div class="xp-ubody">${rows.join('')}</div>` + (hidden ? `<p class="xp-foot">${hidden} block${hidden === 1 ? '' : 's'} with the same code in the selected functions hidden.</p>` : '');
        }

        // LLM titles: asked once when the code tab first opens, never blocking the render.
        async _loadTitles() {
            if (this._titlesAsked) return;
            this._titlesAsked = true;
            const qs = this._qs();
            for (let i = 0; i < 40 && !this._dead; i++) {
                let body = null;
                try {
                    const res = await fetch(`/api/bin_sim/nway/facets/titles?${qs.toString()}`);
                    body = res.ok ? await res.json() : null;
                } catch (e) { body = null; }
                if (this._dead) return;
                if (body && body.status === 'pending') {
                    this._setNaming(true);
                    await new Promise(r => setTimeout(r, 2500));
                    continue;
                }
                if (body && body.status === 'ready') {
                    this.titles = body.titles || {};
                    this.el.querySelectorAll('[data-btitle]').forEach(el => {
                        const b = this.data.consensus.blocks.find(x => x.id === el.dataset.btitle);
                        if (!b || !this.titles[b.id]) return;
                        el.textContent = this.titles[b.id];
                        el.title = b.title;
                    });
                }
                break;
            }
            this._setNaming(false);
        }

        // ---- AI insight: stored one-liners, and per-block explanations asked on click ----
        _sumHtml(j) {
            const t = this.sums[this.fids[j]];
            return `<div class="xp-aisum" data-xsum="${j}"${t ? '' : ' hidden'}>${t ? `<i class="fa-solid fa-robot" title="From the LLM note on this function"></i> ${escapeHtml(t)}` : ''}</div>`;
        }

        async _loadSummaries() {
            if (this._sumsAsked) return;
            this._sumsAsked = true;
            try {
                const res = await fetch(`/api/bin_sim/nway/facets/summaries?fids=${encodeURIComponent(this.fids.join(','))}`);
                this.sums = res.ok ? (await res.json()).summaries || {} : {};
            } catch (e) { return; }
            if (this._dead) return;
            this.el.querySelectorAll('[data-xsum]').forEach(el => { el.outerHTML = this._sumHtml(Number(el.dataset.xsum)); });
        }

        // The Explain control of block `bid` in function `j`; only where the block differs from the reference.
        _expl(bid, j) {
            const b = this.data.consensus.blocks.find(x => x.id === bid), st = this._cell(b, j).st;
            if (st === 'same' || st === 'miss') return '';
            const key = `${bid}:${j}`, e = this.expl[key] || {}, k = escapeAttr(key);
            let inner;
            if (e.status === 'pending') inner = '<i class="fa-solid fa-spinner fa-spin"></i> asking the LLM&hellip;';
            else if (e.status === 'ready') inner = `<i class="fa-solid fa-robot"></i> <span class="xp-tag ${e.verdict === 'semantic' ? 'hot' : ''}">${escapeHtml(e.verdict)}</span> ${escapeHtml(e.summary)} <button class="xp-aibtn" data-xsave="${k}" ${e.saved ? 'disabled' : ''} title="Save this explanation as a note on the reference function">${e.saved ? 'saved' : 'Save as note'}</button>`;
            else inner = `<button class="xp-aibtn" data-xexp="${k}" title="Ask the LLM how this block differs from the reference"><i class="fa-solid fa-robot"></i> ${e.status === 'failed' ? 'Failed, retry' : e.status === 'off' ? 'LLM not configured' : 'Explain'}</button>`;
            return `<span class="xp-ai${e.status === 'ready' ? ' done' : ''}" data-xexpo="${k}">${inner}</span>`;
        }

        // Row-level questions go to the AI Insight agent: it has tools and takes follow-ups.
        _askAI() {
            const host = window.askAIAbout ? window : window.parent;
            if (!host || !host.askAIAbout) return showToast('AI Insight is not available here', 'warning');
            const d = this.data, c = d.consensus, others = this._others();
            const fns = d.functions.map((f, j) => `${f.letter}${j === c.reference ? ' (reference)' : ''}: ${this.fids[j]} (${f.file_name || '?'})`).join('\n');
            const table = c.blocks.map(b => `${b.id} ${b.title}: ${others.map(j => `${d.functions[j].letter}=${this._cell(b, j).st}`).join(', ')}`).join('\n');
            host.askAIAbout(this.fids[c.reference], `Analyze this N-way row: ${d.functions.length} functions matched as variants of one another.\n${fns}\n\nHow each block of the reference looks in the others (same, mod = modified, moved, restr = restructured, miss = missing):\n${table}\n\nIn at most five sentences: what does this function do, what do all variants share, and how do they diverge? Separate changes that alter behaviour from cosmetic ones (renames, reordering, compiler layout), cite block ids, and fetch code or callees with your tools where the table is not enough.`);
        }

        _explPaint(key) {
            const [bid, j] = key.split(':');
            this.el.querySelectorAll(`[data-xexpo="${CSS.escape(key)}"]`).forEach(el => { el.outerHTML = this._expl(bid, Number(j)); });
        }

        async _explain(key) {
            const [bid, j] = key.split(':');
            const qs = this._qs();
            qs.set('block', bid);
            qs.set('fn', j);
            this.expl[key] = { status: 'pending' };
            this._explPaint(key);
            for (let i = 0; i < 80 && !this._dead; i++) {
                let body = null;
                try {
                    const res = await fetch(`/api/bin_sim/nway/facets/explain?${qs.toString()}`);
                    body = res.ok ? await res.json() : null;
                } catch (e) { body = null; }
                if (this._dead) return;
                if (body && body.status === 'pending') { await new Promise(r => setTimeout(r, 2500)); continue; }
                this.expl[key] = body && body.status === 'ready' ? body : { status: body ? body.status : 'failed' };
                return this._explPaint(key);
            }
            this.expl[key] = { status: 'failed' };
            this._explPaint(key);
        }

        async _saveExplain(key) {
            const e = this.expl[key], [bid, j] = key.split(':'), c = this.data.consensus;
            const b = c.blocks.find(x => x.id === bid), f = this.data.functions[Number(j)], ref = this.fids[c.reference];
            const text = `[AI] N-way block ${bid} (${this._bt(b)}), ${this.data.functions[c.reference].letter} vs ${f.letter} (${f.file_name || f.addr}): ${e.verdict}. ${e.summary}`;
            try {
                const res = await fetch('/api/notes/add', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ collection: parts(ref).coll, func_id: ref, text, owner: 'user' }) });
                if (!res.ok) throw new Error(res.status);
                e.saved = true;
                showToast('Saved as a note on the reference function', 'success');
            } catch (err) { showToast('Could not save the note', 'warning'); }
            this._explPaint(key);
        }

        _setNaming(on) {
            this._naming = on;
            const el = this.el && this.el.querySelector('.xp-naming');
            if (el) el.hidden = !on;
        }

        // ---- Pair diff ----
        _pair() {
            const d = this.data, s = this.s, n = d.functions.length;
            if (!s.pair) {
                const ref = d.reference;
                const far = d.similarity[ref].map((v, j) => [j === ref ? 2 : (v == null ? -1 : v), j]).sort((a, b) => a[0] - b[0])[0][1];
                s.pair = [ref, far];
            }
            const opt = sel => d.functions.map((f, j) => `<option value="${j}"${j === sel ? ' selected' : ''}>${escapeHtml(f.letter)} - ${escapeHtml(f.file_name || '')} @ ${escapeHtml(f.addr)}</option>`).join('');
            const [a, b] = s.pair;
            return `<div class="xp-pairbar">${this._info(`Pick any two of the ${n} functions. The existing 2-way diff is loaded here, so you do not leave the table.`)} Compare <select data-xsel="0">${opt(a)}</select> with <select data-xsel="1">${opt(b)}</select>
                <span class="xp-tag" id="xp-pair-sim"></span><button class="btn-action" data-xopen>Open full diff</button></div>
                <div class="xp-pair" id="xp-pair-body"></div>`;
        }

        async _loadPair() {
            const [a, b] = this.s.pair || [];
            const host = this.el.querySelector('#xp-pair-body');
            if (!host || a === undefined) return;
            const sim = this.data.similarity[a][b];
            this.el.querySelector('#xp-pair-sim').textContent = a === b ? 'same function' : sim == null ? 'no match' : `similarity ${sim.toFixed(2)}`;
            if (a === b) { host.innerHTML = '<div class="xp-none">Pick two different functions.</div>'; return; }
            const tok = (this._pairTok = (this._pairTok || 0) + 1);
            host.innerHTML = '<div class="nway-state"><i class="fa-solid fa-spinner fa-spin"></i></div>';
            const A = parts(this.fids[a]), B = parts(this.fids[b]);
            const qs = new URLSearchParams({ collection_a: A.coll, md5_a: A.md5, addr_a: A.addr, collection_b: B.coll, md5_b: B.md5, addr_b: B.addr });
            if (this.source.pool) qs.set('pool', this.source.pool);
            let body = null;
            try {
                const res = await fetch(`/api/diff?${qs.toString()}`);
                body = await res.json();
                if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
            } catch (e) {
                if (tok === this._pairTok) host.innerHTML = `<div class="xp-none">Could not load the diff: ${escapeHtml(String(e.message || e))}</div>`;
                return;
            }
            if (tok !== this._pairTok) return;
            const side = r => (r ? this._line(r, r.chunk_class) : '<div class="code-spacer"></div>');
            const rows = body.rows || [];
            host.innerHTML = `<div class="xp-dpair" data-xfns="${a},${b}">${rows.slice(0, PAIR_ROWS).map(r => `<div class="xp-drow">${side(r.l)}${side(r.r)}</div>`).join('')}</div>
                ${rows.length > PAIR_ROWS ? `<p class="xp-foot">First ${PAIR_ROWS} of ${rows.length} lines. Open the full diff for the rest.</p>` : ''}`;
        }

        // ---- Events ----
        _featHover(e, on) {
            const t = e.target.closest('.feature-highlight');
            if (t && this.el.contains(t) && !t.contains(e.relatedTarget)) window.setHighlight?.(t.dataset.hashes, on, t);
        }

        _callHover(e, on) {
            const call = e.target.closest('[data-xcall]');
            if (!call || !this.el.contains(call) || call.contains(e.relatedTarget)) return;
            const pos = { clientX: e.clientX, clientY: e.clientY, target: call, currentTarget: call };
            const tok = (this._hoverTok = (this._hoverTok || 0) + 1);
            if (!on) {
                this.el.querySelectorAll('.xp-callpair').forEach(x => x.classList.remove('xp-callpair'));
                window.hideDiffPreview?.(e);
                return window.hideCodePreview?.(e);
            }
            const fallback = () => window.showCodePreview?.(call.dataset.xcall, call.dataset.xcallName, null, null, null, pos);
            const fns = call.closest('[data-xfns]')?.dataset.xfns.split(',').map(Number);
            if (!fns) return fallback();
            this._calledHit(fns[0], fns[1], call.dataset.xcall).then(hit => {
                if (tok !== this._hoverTok) return;
                if (!hit) return fallback();
                this._markPair(call, hit);
                window.showDiffPreview(hit.func_a.id, hit.func_a.name || '', hit.func_b.id, hit.func_b.name || '', Number(hit.similarity) || 0, pos);
            });
        }

        // Light the hovered call and its partner call in the neighbouring pane.
        _markPair(call, hit) {
            const other = hit.func_a.id === call.dataset.xcall ? hit.func_b.id : hit.func_a.id;
            const box = call.closest('.xp-panes, .xp-frow, .xp-dpair');
            call.classList.add('xp-callpair');
            box?.querySelectorAll('[data-xcall]').forEach(x => { if (x.dataset.xcall === other) x.classList.add('xp-callpair'); });
        }

        // Greedy partner row of a called function in the pair (i, j), null when unmatched.
        async _calledHit(i, j, id) {
            const key = `${i}|${j}`;
            this._callSim = this._callSim || {};
            if (!(key in this._callSim)) {
                const A = parts(this.fids[i]), B = parts(this.fids[j]);
                const run = source => {
                    const q = new URLSearchParams({ collection_a: A.coll, collection_b: B.coll, md5_a: A.md5, md5_b: B.md5, addr_a: A.addr, addr_b: B.addr, min_score: 0.5, source });
                    if (this.source.pool) q.set('pool', this.source.pool);
                    return fetch(`/api/function/call_graph_similarity?${q}`).then(r => (r.ok ? r.json() : null)).catch(() => null);
                };
                // Stored file-pair match first, then the functions' own neighbours.
                this._callSim[key] = run('stored').then(d => d || run('local'));
            }
            const data = await this._callSim[key];
            if (!data) return null;
            return ['callers', 'callees'].flatMap(r => data[r].matched).find(m => m.func_a.id === id || m.func_b.id === id) || null;
        }

        _entityMenu(e) {
            const target = e.target.closest('[data-xcall],[data-xfnref],[data-xfile]');
            if (!target || !this.el.contains(target) || !window.showGraphContextMenu) return;
            e.preventDefault(); e.stopPropagation();
            const d = target.dataset;
            if (d.xcall) return window.showGraphContextMenu(e, 'function', { function_id: d.xcall, function_name: d.xcallName });
            if (d.xfnref !== undefined) {
                const j = Number(d.xfnref), f = this.data.functions[j];
                return window.showGraphContextMenu(e, 'function', { function_id: this.fids[j], function_name: f.name || f.addr });
            }
            const j = Number(d.xfile), f = this.data.functions[j], { coll, md5 } = parts(this.fids[j]);
            return window.showGraphContextMenu(e, 'file', { fileId: `${coll}:file:${md5}`, collection: coll, md5, file_name: f.file_name });
        }

        _openDiff(a, b, e) {
            const fs = this.data.functions;
            window.Nav.openPath(window.buildDiffUrl(this.fids[a], this.fids[b]), e, { title: `Diff: ${fs[a].addr} vs ${fs[b].addr}`, type: 'diff' });
        }

        async _click(e) {
            const t = e.target.closest('[data-xcall],[data-xfnref],[data-xfile],[data-xtab],[data-xbm],[data-xfn],[data-xtw],[data-xg],[data-xc],[data-xx],[data-xb],[data-xexp],[data-xsave],[data-xask],[data-xpair],[data-xnav],[data-xopen],[data-xcols],[data-xjump],[data-xfold],[data-xug],[data-xu]');
            if (!t) return;
            const d = t.dataset;
            if (this.cfgOnly && (d.xb !== undefined || d.xx !== undefined)) return;
            if (d.xask !== undefined) {
                e.preventDefault(); e.stopPropagation();
                return this._askAI();
            }
            if (d.xexp || d.xsave) {
                e.preventDefault(); e.stopPropagation();
                return d.xexp ? this._explain(d.xexp) : this._saveExplain(d.xsave);
            }
            if (d.xcall) {
                e.preventDefault(); e.stopPropagation();
                const fns = t.closest('[data-xfns]')?.dataset.xfns.split(',').map(Number);
                const hit = fns ? await this._calledHit(fns[0], fns[1], d.xcall) : null;
                if (hit && window.buildDiffUrl) return window.Nav.openPath(window.buildDiffUrl(hit.func_a.id, hit.func_b.id), e, { title: `Diff: ${hit.func_a.name || hit.func_a.id} vs ${hit.func_b.name || hit.func_b.id}`, type: 'diff' });
                return window.Nav.openPath(window.NwayPanel.fnUrl(d.xcall, this.source.pool || null), e, { title: d.xcallName || 'Function', type: 'function' });
            }
            if (d.xfnref !== undefined) {
                e.preventDefault(); e.stopPropagation();
                const j = Number(d.xfnref), f = this.data.functions[j];
                return window.Nav.openPath(window.NwayPanel.fnUrl(this.fids[j], this.source.pool || null), e, { title: f.name || f.addr, type: 'function' });
            }
            if (d.xfile !== undefined) {
                e.preventDefault(); e.stopPropagation();
                const j = Number(d.xfile), f = this.data.functions[j], { coll, md5 } = parts(this.fids[j]);
                const base = this.source.pool ? `/pools/${encodeURIComponent(this.source.pool)}` : '';
                const url = `${base}/collections/${encodeURIComponent(coll)}/files/${encodeURIComponent(md5)}`;
                return window.Nav.openPath(url, e, { title: f.file_name || md5, type: 'file' });
            }
            if (d.xtab) {
                if (['code', 'full', 'unified'].includes(d.xtab) && !this.data.consensus) {
                    showToast(`Code blocks are unavailable for ${this.data.functions.length} functions. Compare at most ${this.data.limits.code} functions to view them.`, 'warning');
                    return;
                }
                this.s.tab = d.xtab; this._persist(); return this.render();
            }
            if (d.xpair) { const [a, b] = d.xpair.split(',').map(Number); return this._openDiff(a, b, e); }
            if (d.xopen !== undefined) return this._openDiff(...this.s.pair, e);
            if (d.xnav) {
                e.preventDefault();
                return window.Nav.openPath(d.xnav, e, { title: d.title || 'Function', type: 'function' });
            }
            if (d.xcols) { this.s.cols = Number(d.xcols); this._persist(); return this._rebody(); }
            if (d.xfold) { this.unfold.add(d.xfold); return this._rebody(); }
            if (d.xjump) return this.el.querySelector(`[data-xrow="${CSS.escape(d.xjump)}"]`)?.scrollIntoView({ block: 'start', behavior: 'smooth' });
            if (d.xbm) { this.s.bm = d.xbm; this._persist(); return this.load(); }
            if (d.xug) { this.s.uoff[d.xug] = !this.s.uoff[d.xug]; return this._rebody(); }
            if (d.xu) { this.s.uopen[d.xu] = !this.s.uopen[d.xu]; return this._rebody(); }
            if (d.xaudit !== undefined) { this.audit = !this.audit; return this._rebody(); }
            if (d.xfn !== undefined || d.xtw !== undefined) {
                // Keep the block the chip sits in at the same screen position across the redraw.
                // The sticky header's buttons sit in no row: anchor on the first row visible under it.
                const hb = t.closest('.xp-ghead')?.getBoundingClientRect().bottom;
                const row = t.closest('[data-xrow]') || (hb != null && [...this.el.querySelectorAll('[data-xrow]')].find(r => r.getBoundingClientRect().bottom > hb));
                const id = row && row.dataset.xrow, y = row && row.getBoundingClientRect().top;
                if (d.xtw !== undefined) { const [k, m] = d.xtw.split(',').map(Number); this.s.pick[k] = m; } else this.s.fn = Number(d.xfn);
                this._persist(); this._rebody();
                const now = id && this.el.querySelector(`[data-xrow="${CSS.escape(id)}"]`);
                if (now) {
                    // The page may scroll in a container rather than the window.
                    let sc = this.el;
                    while (sc && !(/(auto|scroll)/.test(getComputedStyle(sc).overflowY) && sc.scrollHeight > sc.clientHeight)) sc = sc.parentElement;
                    (sc || window).scrollBy(0, now.getBoundingClientRect().top - y);
                    now.classList.add('xp-flash');
                    now.addEventListener('animationend', () => now.classList.remove('xp-flash'), { once: true });
                }
                return;
            }
            if (d.xx) { this.s.open[d.xx] = 0; return this._rebody(d.xx); }
            if (d.xb !== undefined) {
                const b = this.data.consensus.blocks.find(x => x.id === d.xb);
                let focus = null;
                if (d.xg !== undefined) this.s.open[d.xb] = Number(d.xg);
                else if (d.xc !== undefined && b) {
                    this.s.open[d.xb] = Math.max(0, b.groups.findIndex(g => g.funcs.includes(Number(d.xc))));
                    focus = d.xb;
                } else if (d.xb in this.s.open) delete this.s.open[d.xb];
                else this.s.open[d.xb] = 0;
                this._rebody(focus);
            }
        }

        _change(e) {
            const t = e.target;
            if (t.dataset.xsingle !== undefined) { this.s.single = t.checked; this._persist(); return this._rebody(); }
            if (t.dataset.xdedent !== undefined) { this.s.dedent = t.checked; return this._rebody(); }
            if (t.dataset.xonly !== undefined) { this.s.only = t.checked; this._persist(); return this._rebody(); }
            if (t.dataset.xsel !== undefined) {
                this.s.pair[Number(t.dataset.xsel)] = Number(t.value);
                this._loadPair();
            }
        }
    }

    window.NwayFunction = NwayFunction;
})();
